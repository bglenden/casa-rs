// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rust implementation of casacore AipsIO persistent-object I/O.
//!
//! This crate provides two complementary layers:
//!
//! - **Primitive codec** — the private codec core encodes and decodes the
//!   casacore scalar/array wire format (big-endian by default, matching C++
//!   `CanonicalIO`). Narrow [`encode_value`] and [`decode_value`] operations
//!   expose the raw interoperability surface.
//!
//! - **Object framing** — the [`aipsio`] module exposes [`aipsio::AipsIo`],
//!   which adds type-checked object headers (`putstart`/`putend`,
//!   `getstart`/`getend`), nesting, and version numbers, matching the C++
//!   `AipsIO` class.
//!
//! # Quick start
//!
//! For read/write interoperability with casacore `.table` or `.image` files,
//! use [`aipsio::AipsIo`]:
//!
//! ```rust
//! use casa_aipsio::AipsIo;
//! use std::io::Cursor;
//!
//! let mut io = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
//! io.putstart("MyObject", 1).unwrap();
//! io.put_i32(42).unwrap();
//! io.putend().unwrap();
//! ```
//!
//! For lower-level interoperability without object headers, use
//! [`encode_value`] and [`decode_value`].
//!
//! Detailed behavior and C++ mapping notes are in the [`aipsio`] module-level
//! rustdoc.

use std::io::{Read, Write};

pub use casa_values::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue,
    TypeTag, Value, ValueKind, ValueRank,
};
use thiserror::Error;

pub mod aipsio;
mod buffer;

pub use aipsio::{AipsIo, AipsIoObjectError, AipsIoObjectResult, AipsIoStream, AipsOpenOption};

/// Cross-crate storage integration used by `casa-tables`.
///
/// This is not an application-developer codec facade; normal callers should
/// use [`AipsIo`] or the narrow raw [`encode_value`] and [`decode_value`]
/// operations.
#[doc(hidden)]
pub mod internal {
    pub use super::buffer::{AipsIoBufferWriter, AipsIoSliceReader, detect_aipsio_byte_order};
}

pub type AipsIoResult<T> = Result<T, AipsIoError>;

/// Byte order used when encoding or decoding multi-byte numeric values.
///
/// The casacore canonical format is big-endian (`CanonicalIO` in C++). Little-
/// endian support (`LECanonicalIO`) exists for files written on little-endian
/// systems. Storage formats that lack external byte-order metadata use the
/// crate's hidden structural frame validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ByteOrder {
    /// Big-endian byte order — the canonical casacore wire format. This is the
    /// default.
    #[default]
    BigEndian,
    /// Little-endian byte order, used by casacore `LECanonicalIO`.
    LittleEndian,
}

pub(crate) mod primitive_codec {
    use super::ByteOrder;

    macro_rules! endian_codec {
        ($encode:ident, $decode:ident, $type:ty, $width:literal) => {
            pub(crate) fn $encode(value: $type, order: ByteOrder) -> [u8; $width] {
                match order {
                    ByteOrder::BigEndian => value.to_be_bytes(),
                    ByteOrder::LittleEndian => value.to_le_bytes(),
                }
            }

            pub(crate) fn $decode(bytes: [u8; $width], order: ByteOrder) -> $type {
                match order {
                    ByteOrder::BigEndian => <$type>::from_be_bytes(bytes),
                    ByteOrder::LittleEndian => <$type>::from_le_bytes(bytes),
                }
            }
        };
    }

    endian_codec!(encode_i16, decode_i16, i16, 2);
    endian_codec!(encode_u16, decode_u16, u16, 2);
    endian_codec!(encode_i32, decode_i32, i32, 4);
    endian_codec!(encode_u32, decode_u32, u32, 4);
    endian_codec!(encode_i64, decode_i64, i64, 8);
    endian_codec!(encode_u64, decode_u64, u64, 8);
}

/// Errors from primitive codec operations.
///
/// These are the lower-level errors that do not involve object framing. For
/// object-framing errors see [`aipsio::AipsIoObjectError`].
#[derive(Debug, Error)]
pub enum AipsIoError {
    /// A low-level I/O error from the underlying stream.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// The stream contained bytes that are not valid UTF-8 when decoding a
    /// string field.
    #[error("utf-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// A boolean field contained a byte value other than `0` or `1`.
    #[error("invalid boolean value {0}; expected 0 or 1")]
    InvalidBoolean(u8),
    /// A string or array length exceeded the `u32` limit used in the wire
    /// format.
    #[error("length {0} exceeds maximum supported length")]
    LengthTooLarge(usize),
    /// An array of rank != 1 was supplied to the primitive codec, which only
    /// supports rank-1 (linear) arrays.
    #[error("unsupported array rank {0}; primitive AipsIO currently supports rank-1 arrays only")]
    UnsupportedArrayRank(usize),
    /// A [`Value`] variant that the primitive codec does not support was
    /// supplied (currently `Record` or `TableRef`).
    #[error("unsupported value kind for primitive AipsIO codec: {0:?}")]
    UnsupportedValueKind(ValueKind),
    /// A catch-all error for miscellaneous string-described failures.
    #[error("{0}")]
    Other(String),
}

/// Streaming writer for casacore primitive values in canonical wire format.
///
/// `AipsWriter` is a lower-level building block used by [`aipsio::AipsIo`].
/// It encodes individual scalars and arrays without any object-framing
/// headers; callers that need type-checked, versioned object persistence
/// should use [`aipsio::AipsIo`] instead.
///
/// All multi-byte integers and floats are encoded in the byte order chosen at
/// construction time. The casacore canonical format is big-endian, which is
/// the default (see [`ByteOrder::BigEndian`]).
pub(crate) struct AipsWriter<W> {
    inner: W,
    byte_order: ByteOrder,
}

impl<W: Write> AipsWriter<W> {
    /// Create a writer that uses big-endian byte order.
    #[cfg(test)]
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            byte_order: ByteOrder::default(),
        }
    }

    /// Create a writer with an explicit byte order.
    pub fn with_byte_order(inner: W, byte_order: ByteOrder) -> Self {
        Self { inner, byte_order }
    }

    /// Write a boolean value as a single byte (`0` for `false`, `1` for `true`).
    ///
    /// All other `write_*` scalar methods follow the same pattern: encode the
    /// value in the configured byte order and write it to the stream.
    pub fn write_bool(&mut self, value: bool) -> AipsIoResult<()> {
        self.inner.write_all(&[u8::from(value)])?;
        Ok(())
    }

    /// Write an unsigned 8-bit integer (1 byte). See [`write_bool`](Self::write_bool).
    pub fn write_u8(&mut self, value: u8) -> AipsIoResult<()> {
        self.inner.write_all(&[value])?;
        Ok(())
    }

    /// Write an unsigned 16-bit integer in canonical byte order (2 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_u16(&mut self, value: u16) -> AipsIoResult<()> {
        let bytes = primitive_codec::encode_u16(value, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write an unsigned 32-bit integer in canonical byte order (4 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_u32(&mut self, value: u32) -> AipsIoResult<()> {
        let bytes = primitive_codec::encode_u32(value, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write a signed 16-bit integer in canonical byte order (2 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_i16(&mut self, value: i16) -> AipsIoResult<()> {
        let bytes = primitive_codec::encode_i16(value, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write a signed 32-bit integer in canonical byte order (4 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_i32(&mut self, value: i32) -> AipsIoResult<()> {
        let bytes = primitive_codec::encode_i32(value, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write a signed 64-bit integer in canonical byte order (8 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_i64(&mut self, value: i64) -> AipsIoResult<()> {
        let bytes = primitive_codec::encode_i64(value, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write a 32-bit float as its IEEE 754 bit pattern (4 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_f32(&mut self, value: f32) -> AipsIoResult<()> {
        let bits = value.to_bits();
        let bytes = primitive_codec::encode_u32(bits, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write a 64-bit float as its IEEE 754 bit pattern (8 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_f64(&mut self, value: f64) -> AipsIoResult<()> {
        let bits = value.to_bits();
        let bytes = primitive_codec::encode_u64(bits, self.byte_order);
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    /// Write a 32-bit complex number as two consecutive `f32` values (8 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_complex32(&mut self, value: Complex32) -> AipsIoResult<()> {
        self.write_f32(value.re)?;
        self.write_f32(value.im)?;
        Ok(())
    }

    /// Write a 64-bit complex number as two consecutive `f64` values (16 bytes). See [`write_bool`](Self::write_bool).
    pub fn write_complex64(&mut self, value: Complex64) -> AipsIoResult<()> {
        self.write_f64(value.re)?;
        self.write_f64(value.im)?;
        Ok(())
    }

    /// Write a UTF-8 string as a `u32` byte length followed by the UTF-8 bytes. See [`write_bool`](Self::write_bool).
    pub fn write_string(&mut self, value: &str) -> AipsIoResult<()> {
        let len_u32 =
            u32::try_from(value.len()).map_err(|_| AipsIoError::LengthTooLarge(value.len()))?;
        self.write_u32(len_u32)?;
        self.inner.write_all(value.as_bytes())?;
        Ok(())
    }

    /// Write a dynamically-typed scalar value by dispatching on its variant.
    pub fn write_scalar(&mut self, value: &ScalarValue) -> AipsIoResult<()> {
        match value {
            ScalarValue::Bool(v) => self.write_bool(*v),
            ScalarValue::UInt8(v) => self.write_u8(*v),
            ScalarValue::UInt16(v) => self.write_u16(*v),
            ScalarValue::UInt32(v) => self.write_u32(*v),
            ScalarValue::Int16(v) => self.write_i16(*v),
            ScalarValue::Int32(v) => self.write_i32(*v),
            ScalarValue::Int64(v) => self.write_i64(*v),
            ScalarValue::Float32(v) => self.write_f32(*v),
            ScalarValue::Float64(v) => self.write_f64(*v),
            ScalarValue::Complex32(v) => self.write_complex32(*v),
            ScalarValue::Complex64(v) => self.write_complex64(*v),
            ScalarValue::String(v) => self.write_string(v),
        }
    }

    /// Write an array value (rank-1 only in this primitive codec).
    ///
    /// The element count is written first as `u32`.
    pub fn write_array(&mut self, value: &ArrayValue) -> AipsIoResult<()> {
        let ndim = value.ndim();
        if ndim != 1 {
            return Err(AipsIoError::UnsupportedArrayRank(ndim));
        }

        let len = value.len();
        let len_u32 = u32::try_from(len).map_err(|_| AipsIoError::LengthTooLarge(len))?;
        self.write_u32(len_u32)?;

        match value {
            ArrayValue::Bool(values) => values.iter().try_for_each(|v| self.write_bool(*v)),
            ArrayValue::UInt8(values) => values.iter().try_for_each(|v| self.write_u8(*v)),
            ArrayValue::UInt16(values) => values.iter().try_for_each(|v| self.write_u16(*v)),
            ArrayValue::UInt32(values) => values.iter().try_for_each(|v| self.write_u32(*v)),
            ArrayValue::Int16(values) => values.iter().try_for_each(|v| self.write_i16(*v)),
            ArrayValue::Int32(values) => values.iter().try_for_each(|v| self.write_i32(*v)),
            ArrayValue::Int64(values) => values.iter().try_for_each(|v| self.write_i64(*v)),
            ArrayValue::Float32(values) => values.iter().try_for_each(|v| self.write_f32(*v)),
            ArrayValue::Float64(values) => values.iter().try_for_each(|v| self.write_f64(*v)),
            ArrayValue::Complex32(values) => {
                values.iter().try_for_each(|v| self.write_complex32(*v))
            }
            ArrayValue::Complex64(values) => {
                values.iter().try_for_each(|v| self.write_complex64(*v))
            }
            ArrayValue::String(values) => values.iter().try_for_each(|v| self.write_string(v)),
        }
    }

    /// Write a dynamically-typed value (scalar or rank-1 array).
    ///
    /// Returns [`AipsIoError::UnsupportedValueKind`] if `value` is a
    /// `Value::Record` or `Value::TableRef`.
    pub fn write_value(&mut self, value: &Value) -> AipsIoResult<()> {
        match value {
            Value::Scalar(v) => self.write_scalar(v),
            Value::Array(v) => self.write_array(v),
            Value::Record(_) => Err(AipsIoError::UnsupportedValueKind(ValueKind::Record)),
            Value::TableRef(_) => Err(AipsIoError::UnsupportedValueKind(ValueKind::TableRef)),
        }
    }
}

/// Streaming reader for casacore primitive values in canonical wire format.
///
/// `AipsReader` is the read-side counterpart to [`AipsWriter`]. It decodes
/// individual scalars and arrays without any object-framing interpretation;
/// callers that need type-checked, versioned object persistence should use
/// [`aipsio::AipsIo`] instead.
///
/// The byte order must match the byte order used by the writer that produced
/// the stream. Use [`ByteOrder::BigEndian`] (the default) for files written
/// by standard casacore tools.
pub(crate) struct AipsReader<R> {
    inner: R,
    byte_order: ByteOrder,
}

impl<R: Read> AipsReader<R> {
    /// Create a reader that uses big-endian byte order.
    #[cfg(test)]
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            byte_order: ByteOrder::default(),
        }
    }

    /// Create a reader with an explicit byte order.
    pub fn with_byte_order(inner: R, byte_order: ByteOrder) -> Self {
        Self { inner, byte_order }
    }

    /// Read a boolean value from a single byte (`0` → `false`, `1` → `true`).
    ///
    /// Returns [`AipsIoError::InvalidBoolean`] for any byte value other than
    /// `0` or `1`. All other `read_*` scalar methods follow the same pattern:
    /// read the encoded bytes and decode them using the configured byte order.
    pub fn read_bool(&mut self) -> AipsIoResult<bool> {
        let mut buf = [0_u8; 1];
        self.inner.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(AipsIoError::InvalidBoolean(value)),
        }
    }

    /// Read an unsigned 8-bit integer (1 byte). See [`read_bool`](Self::read_bool).
    pub fn read_u8(&mut self) -> AipsIoResult<u8> {
        let mut buf = [0_u8; 1];
        self.inner.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Read an unsigned 16-bit integer in canonical byte order (2 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_u16(&mut self) -> AipsIoResult<u16> {
        let bytes = self.read_exact_array::<2>()?;
        Ok(primitive_codec::decode_u16(bytes, self.byte_order))
    }

    /// Read an unsigned 32-bit integer in canonical byte order (4 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_u32(&mut self) -> AipsIoResult<u32> {
        let bytes = self.read_exact_array::<4>()?;
        Ok(primitive_codec::decode_u32(bytes, self.byte_order))
    }

    /// Read a signed 16-bit integer in canonical byte order (2 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_i16(&mut self) -> AipsIoResult<i16> {
        let bytes = self.read_exact_array::<2>()?;
        Ok(primitive_codec::decode_i16(bytes, self.byte_order))
    }

    /// Read a signed 32-bit integer in canonical byte order (4 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_i32(&mut self) -> AipsIoResult<i32> {
        let bytes = self.read_exact_array::<4>()?;
        Ok(primitive_codec::decode_i32(bytes, self.byte_order))
    }

    /// Read a signed 64-bit integer in canonical byte order (8 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_i64(&mut self) -> AipsIoResult<i64> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(primitive_codec::decode_i64(bytes, self.byte_order))
    }

    /// Read a 32-bit float from its IEEE 754 bit pattern (4 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_f32(&mut self) -> AipsIoResult<f32> {
        let bits = self.read_u32()?;
        Ok(f32::from_bits(bits))
    }

    /// Read a 64-bit float from its IEEE 754 bit pattern (8 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_f64(&mut self) -> AipsIoResult<f64> {
        let bits = self.read_u64()?;
        Ok(f64::from_bits(bits))
    }

    /// Read a 32-bit complex number as two consecutive `f32` values (8 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_complex32(&mut self) -> AipsIoResult<Complex32> {
        let re = self.read_f32()?;
        let im = self.read_f32()?;
        Ok(Complex32 { re, im })
    }

    /// Read a 64-bit complex number as two consecutive `f64` values (16 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_complex64(&mut self) -> AipsIoResult<Complex64> {
        let re = self.read_f64()?;
        let im = self.read_f64()?;
        Ok(Complex64 { re, im })
    }

    /// Read a length-prefixed UTF-8 string (4-byte `u32` length then UTF-8 bytes). See [`read_bool`](Self::read_bool).
    pub fn read_string(&mut self) -> AipsIoResult<String> {
        let len = self.read_u32()? as usize;
        let mut bytes = vec![0_u8; len];
        self.inner.read_exact(&mut bytes)?;
        Ok(String::from_utf8(bytes)?)
    }

    /// Read a scalar value for the given primitive type by dispatching on the variant.
    pub fn read_scalar(&mut self, primitive: PrimitiveType) -> AipsIoResult<ScalarValue> {
        match primitive {
            PrimitiveType::Bool => Ok(ScalarValue::Bool(self.read_bool()?)),
            PrimitiveType::UInt8 => Ok(ScalarValue::UInt8(self.read_u8()?)),
            PrimitiveType::UInt16 => Ok(ScalarValue::UInt16(self.read_u16()?)),
            PrimitiveType::UInt32 => Ok(ScalarValue::UInt32(self.read_u32()?)),
            PrimitiveType::Int16 => Ok(ScalarValue::Int16(self.read_i16()?)),
            PrimitiveType::Int32 => Ok(ScalarValue::Int32(self.read_i32()?)),
            PrimitiveType::Int64 => Ok(ScalarValue::Int64(self.read_i64()?)),
            PrimitiveType::Float32 => Ok(ScalarValue::Float32(self.read_f32()?)),
            PrimitiveType::Float64 => Ok(ScalarValue::Float64(self.read_f64()?)),
            PrimitiveType::Complex32 => Ok(ScalarValue::Complex32(self.read_complex32()?)),
            PrimitiveType::Complex64 => Ok(ScalarValue::Complex64(self.read_complex64()?)),
            PrimitiveType::String => Ok(ScalarValue::String(self.read_string()?)),
        }
    }

    /// Read an array for the given primitive type.
    ///
    /// The function first reads a `u32` element count.
    pub fn read_array(&mut self, primitive: PrimitiveType) -> AipsIoResult<ArrayValue> {
        let len = self.read_u32()? as usize;
        match primitive {
            PrimitiveType::Bool => (0..len)
                .map(|_| self.read_bool())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_bool_vec),
            PrimitiveType::UInt8 => (0..len)
                .map(|_| self.read_u8())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_u8_vec),
            PrimitiveType::UInt16 => (0..len)
                .map(|_| self.read_u16())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_u16_vec),
            PrimitiveType::UInt32 => (0..len)
                .map(|_| self.read_u32())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_u32_vec),
            PrimitiveType::Int16 => (0..len)
                .map(|_| self.read_i16())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_i16_vec),
            PrimitiveType::Int32 => (0..len)
                .map(|_| self.read_i32())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_i32_vec),
            PrimitiveType::Int64 => (0..len)
                .map(|_| self.read_i64())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_i64_vec),
            PrimitiveType::Float32 => (0..len)
                .map(|_| self.read_f32())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_f32_vec),
            PrimitiveType::Float64 => (0..len)
                .map(|_| self.read_f64())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_f64_vec),
            PrimitiveType::Complex32 => (0..len)
                .map(|_| self.read_complex32())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_complex32_vec),
            PrimitiveType::Complex64 => (0..len)
                .map(|_| self.read_complex64())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_complex64_vec),
            PrimitiveType::String => (0..len)
                .map(|_| self.read_string())
                .collect::<AipsIoResult<Vec<_>>>()
                .map(ArrayValue::from_string_vec),
        }
    }

    /// Read a dynamically-typed value (scalar or rank-1 array) using the supplied [`TypeTag`].
    pub fn read_value(&mut self, type_tag: TypeTag) -> AipsIoResult<Value> {
        match type_tag.rank {
            ValueRank::Scalar => self.read_scalar(type_tag.primitive).map(Value::Scalar),
            ValueRank::Array => self.read_array(type_tag.primitive).map(Value::Array),
        }
    }

    pub(crate) fn read_u64(&mut self) -> AipsIoResult<u64> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(primitive_codec::decode_u64(bytes, self.byte_order))
    }

    fn read_exact_array<const N: usize>(&mut self) -> AipsIoResult<[u8; N]> {
        let mut bytes = [0_u8; N];
        self.inner.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

/// Encode one scalar or rank-1 array value without object framing.
pub fn encode_value(value: &Value, byte_order: ByteOrder) -> AipsIoResult<Vec<u8>> {
    let mut bytes = Vec::new();
    AipsWriter::with_byte_order(&mut bytes, byte_order).write_value(value)?;
    Ok(bytes)
}

/// Decode one scalar or rank-1 array value without object framing.
pub fn decode_value(bytes: &[u8], type_tag: TypeTag, byte_order: ByteOrder) -> AipsIoResult<Value> {
    AipsReader::with_byte_order(bytes, byte_order).read_value(type_tag)
}

#[cfg(test)]
mod tests {
    use super::internal::{AipsIoBufferWriter, detect_aipsio_byte_order};
    use super::{
        AipsIoError, AipsReader, AipsWriter, ArrayValue, ByteOrder, Complex32, Complex64,
        PrimitiveType, RecordField, RecordValue, ScalarValue, TypeTag, Value, ValueRank,
    };

    #[test]
    fn scalar_type_tag_is_derived_from_variant() {
        let value = ScalarValue::Float64(3.5);
        let tag = value.type_tag();
        assert_eq!(tag.primitive, PrimitiveType::Float64);
    }

    #[test]
    fn big_endian_scalar_round_trip() {
        let mut buf = Vec::new();
        {
            let mut writer = AipsWriter::new(&mut buf);
            writer
                .write_scalar(&ScalarValue::Int32(-42))
                .expect("write");
            writer
                .write_scalar(&ScalarValue::Complex64(Complex64 { re: 1.25, im: -0.5 }))
                .expect("write");
        }

        let mut reader = AipsReader::new(buf.as_slice());
        let value = reader
            .read_scalar(PrimitiveType::Int32)
            .expect("read int32");
        assert_eq!(value, ScalarValue::Int32(-42));

        let complex = reader
            .read_scalar(PrimitiveType::Complex64)
            .expect("read complex");
        assert_eq!(
            complex,
            ScalarValue::Complex64(Complex64 { re: 1.25, im: -0.5 })
        );
    }

    #[test]
    fn little_endian_array_round_trip() {
        let mut buf = Vec::new();
        {
            let mut writer = AipsWriter::with_byte_order(&mut buf, ByteOrder::LittleEndian);
            writer
                .write_array(&ArrayValue::from_i16_vec(vec![1, -2, 3]))
                .expect("write");
            writer
                .write_array(&ArrayValue::from_string_vec(vec![
                    "a".to_string(),
                    "bc".to_string(),
                ]))
                .expect("write");
        }

        let mut reader = AipsReader::with_byte_order(buf.as_slice(), ByteOrder::LittleEndian);
        let ints = reader.read_array(PrimitiveType::Int16).expect("read array");
        assert_eq!(ints, ArrayValue::from_i16_vec(vec![1, -2, 3]));

        let strings = reader
            .read_array(PrimitiveType::String)
            .expect("read array");
        assert_eq!(
            strings,
            ArrayValue::from_string_vec(vec!["a".to_string(), "bc".to_string()])
        );
    }

    #[test]
    fn byte_order_detection_accepts_canonical_and_little_endian_headers() {
        let mut be_writer = AipsIoBufferWriter::new(ByteOrder::BigEndian);
        be_writer.putstart("Object", 1).unwrap();
        be_writer.putend().unwrap();
        let be = be_writer.into_bytes();
        assert_eq!(
            detect_aipsio_byte_order(&be).expect("big-endian header"),
            ByteOrder::BigEndian
        );

        let mut le_writer = AipsIoBufferWriter::new(ByteOrder::LittleEndian);
        le_writer.putstart("Object", 1).unwrap();
        le_writer.putend().unwrap();
        let le = le_writer.into_bytes();
        assert_eq!(
            detect_aipsio_byte_order(&le).expect("little-endian header"),
            ByteOrder::LittleEndian
        );

        assert!(detect_aipsio_byte_order(&[1, 2, 3]).is_err());
        assert!(detect_aipsio_byte_order(&[0, 0, 0, 0, 0, 0, 0, 0]).is_err());

        let mut unreasonable = Vec::new();
        unreasonable.extend_from_slice(&0xBEBE_BEBE_u32.to_be_bytes());
        unreasonable.extend_from_slice(&0_u32.to_be_bytes());
        assert!(detect_aipsio_byte_order(&unreasonable).is_err());
    }

    #[test]
    fn primitive_codec_round_trips_every_scalar_family() {
        let scalars = [
            ScalarValue::Bool(true),
            ScalarValue::UInt8(3),
            ScalarValue::UInt16(513),
            ScalarValue::UInt32(65_537),
            ScalarValue::Int16(-123),
            ScalarValue::Int32(-65_537),
            ScalarValue::Int64(-9_000_000_000),
            ScalarValue::Float32(1.25),
            ScalarValue::Float64(-2.5),
            ScalarValue::Complex32(Complex32 { re: 3.0, im: -4.0 }),
            ScalarValue::Complex64(Complex64 { re: 5.0, im: -6.0 }),
            ScalarValue::String("ngc5921".to_string()),
        ];

        for scalar in scalars {
            let mut buf = Vec::new();
            AipsWriter::new(&mut buf)
                .write_scalar(&scalar)
                .expect("write scalar");
            let mut reader = AipsReader::new(buf.as_slice());
            let decoded = reader
                .read_scalar(scalar.type_tag().primitive)
                .expect("read scalar");
            assert_eq!(decoded, scalar);
        }
    }

    #[test]
    fn primitive_codec_round_trips_every_array_family() {
        let arrays = [
            ArrayValue::from_bool_vec(vec![true, false]),
            ArrayValue::from_u8_vec(vec![1, 2]),
            ArrayValue::from_u16_vec(vec![3, 4]),
            ArrayValue::from_u32_vec(vec![5, 6]),
            ArrayValue::from_i16_vec(vec![-1, 2]),
            ArrayValue::from_i32_vec(vec![-3, 4]),
            ArrayValue::from_i64_vec(vec![-5, 6]),
            ArrayValue::from_f32_vec(vec![1.5, 2.5]),
            ArrayValue::from_f64_vec(vec![3.5, 4.5]),
            ArrayValue::from_complex32_vec(vec![Complex32 { re: 1.0, im: 2.0 }]),
            ArrayValue::from_complex64_vec(vec![Complex64 { re: 3.0, im: 4.0 }]),
            ArrayValue::from_string_vec(vec!["rr".to_string(), "ll".to_string()]),
        ];

        for array in arrays {
            let mut buf = Vec::new();
            AipsWriter::new(&mut buf)
                .write_value(&Value::Array(array.clone()))
                .expect("write array");
            let tag = array.type_tag();
            let mut reader = AipsReader::new(buf.as_slice());
            assert_eq!(
                reader.read_value(tag).expect("read array"),
                Value::Array(array)
            );
        }
    }

    #[test]
    fn primitive_codec_reports_invalid_wire_values_and_unsupported_values() {
        let mut invalid_bool = AipsReader::new([7_u8].as_slice());
        assert!(matches!(
            invalid_bool.read_bool().expect_err("invalid bool byte"),
            AipsIoError::InvalidBoolean(7)
        ));

        let mut invalid_utf8 = Vec::new();
        AipsWriter::new(&mut invalid_utf8)
            .write_u32(1)
            .expect("length");
        invalid_utf8.push(0xff);
        let mut reader = AipsReader::new(invalid_utf8.as_slice());
        assert!(matches!(
            reader.read_string().expect_err("invalid utf8"),
            AipsIoError::Utf8(_)
        ));

        let mut writer = AipsWriter::new(Vec::new());
        assert!(matches!(
            writer
                .write_value(&Value::TableRef("table/path".to_string()))
                .expect_err("table refs are unsupported"),
            AipsIoError::UnsupportedValueKind(super::ValueKind::TableRef)
        ));

        let mut reader = AipsReader::new([0_u8].as_slice());
        let tag = TypeTag {
            primitive: PrimitiveType::UInt32,
            rank: ValueRank::Scalar,
        };
        assert!(matches!(
            reader.read_value(tag).expect_err("short read"),
            AipsIoError::Io(_)
        ));
    }

    #[test]
    fn value_tag_matches_container_kind() {
        let scalar = Value::Scalar(ScalarValue::Bool(true));
        assert_eq!(
            scalar.type_tag().expect("scalar has a primitive tag").rank,
            super::ValueRank::Scalar
        );

        let array = Value::Array(ArrayValue::from_bool_vec(vec![true, false]));
        assert_eq!(
            array.type_tag().expect("array has a primitive tag").rank,
            super::ValueRank::Array
        );
    }

    #[test]
    fn write_value_rejects_record_values() {
        let mut buf = Vec::new();
        let mut writer = AipsWriter::new(&mut buf);
        let record = RecordValue::new(vec![RecordField::new(
            "field",
            Value::Scalar(ScalarValue::Int32(7)),
        )]);

        let error = writer
            .write_value(&Value::Record(record))
            .expect_err("record values are not yet wire-encoded");
        assert!(matches!(
            error,
            AipsIoError::UnsupportedValueKind(super::ValueKind::Record)
        ));
    }
}
