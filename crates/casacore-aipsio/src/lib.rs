//! Rust AipsIO support.
//!
//! This crate exposes two layers:
//! - `AipsWriter` / `AipsReader`: primitive scalar/array codec utilities.
//! - [`aipsio`]: full AipsIO object framing API (`putstart/getstart/...`).
//!
//! For object-persistent interoperability similar to casacore C++ `AipsIO`,
//! prefer the [`aipsio::AipsIo`] API.
//! Detailed `AipsIo` behavior and C++ mapping notes are in the
//! [`aipsio`] module-level rustdoc.

use std::io::{Read, Write};

pub use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue,
    TypeTag, Value, ValueKind, ValueRank,
};
use thiserror::Error;

pub mod aipsio;
pub mod demo;

pub use aipsio::{AipsIo, AipsIoObjectError, AipsIoObjectResult, AipsIoStream, AipsOpenOption};

pub type AipsIoResult<T> = Result<T, AipsIoError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ByteOrder {
    #[default]
    BigEndian,
    LittleEndian,
}

/// Detect byte order from a raw AipsIO header buffer.
///
/// Examines the first 8 bytes after the magic to determine whether the
/// object-length field makes sense in big-endian or little-endian.
/// This mirrors the logic used in C++ casacore SSM to pick CanonicalIO vs
/// LECanonicalIO.
pub fn detect_aipsio_byte_order(data: &[u8]) -> Result<ByteOrder, AipsIoError> {
    const MAGIC: u32 = 0xBEBE_BEBE;
    if data.len() < 8 {
        return Err(AipsIoError::Other(
            "buffer too short for AipsIO header".into(),
        ));
    }
    let magic = u32::from_be_bytes(data[0..4].try_into().unwrap());
    if magic != MAGIC {
        let magic_le = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic_le != MAGIC {
            return Err(AipsIoError::Other(format!(
                "AipsIO magic not found: got {magic:#010x}"
            )));
        }
    }
    // The object-length field follows the magic. In a valid file it should be
    // a reasonable value (< 1 GiB). Try both byte orders.
    let obj_len_be = u32::from_be_bytes(data[4..8].try_into().unwrap());
    let obj_len_le = u32::from_le_bytes(data[4..8].try_into().unwrap());
    const MAX_REASONABLE_LEN: u32 = 1 << 30; // 1 GiB
    let be_ok = obj_len_be > 0 && obj_len_be < MAX_REASONABLE_LEN;
    let le_ok = obj_len_le > 0 && obj_len_le < MAX_REASONABLE_LEN;
    match (be_ok, le_ok) {
        (true, false) => Ok(ByteOrder::BigEndian),
        (false, true) => Ok(ByteOrder::LittleEndian),
        // Both look reasonable — default to BE (canonical).
        (true, true) => Ok(ByteOrder::BigEndian),
        (false, false) => Err(AipsIoError::Other(format!(
            "cannot determine byte order: obj_len BE={obj_len_be}, LE={obj_len_le}"
        ))),
    }
}

#[derive(Debug, Error)]
pub enum AipsIoError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("utf-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid boolean value {0}; expected 0 or 1")]
    InvalidBoolean(u8),
    #[error("length {0} exceeds maximum supported length")]
    LengthTooLarge(usize),
    #[error("unsupported array rank {0}; primitive AipsIO currently supports rank-1 arrays only")]
    UnsupportedArrayRank(usize),
    #[error("unsupported value kind for primitive AipsIO codec: {0:?}")]
    UnsupportedValueKind(ValueKind),
    #[error("{0}")]
    Other(String),
}

pub struct AipsWriter<W> {
    inner: W,
    byte_order: ByteOrder,
}

impl<W: Write> AipsWriter<W> {
    /// Create a writer that uses big-endian byte order.
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

    pub fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Write one boolean value (1 byte).
    ///
    /// Other scalar write methods follow the same pattern.
    pub fn write_bool(&mut self, value: bool) -> AipsIoResult<()> {
        self.inner.write_all(&[u8::from(value)])?;
        Ok(())
    }

    pub fn write_u8(&mut self, value: u8) -> AipsIoResult<()> {
        self.inner.write_all(&[value])?;
        Ok(())
    }

    pub fn write_u16(&mut self, value: u16) -> AipsIoResult<()> {
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => value.to_be_bytes(),
            ByteOrder::LittleEndian => value.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_u32(&mut self, value: u32) -> AipsIoResult<()> {
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => value.to_be_bytes(),
            ByteOrder::LittleEndian => value.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_i16(&mut self, value: i16) -> AipsIoResult<()> {
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => value.to_be_bytes(),
            ByteOrder::LittleEndian => value.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_i32(&mut self, value: i32) -> AipsIoResult<()> {
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => value.to_be_bytes(),
            ByteOrder::LittleEndian => value.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_i64(&mut self, value: i64) -> AipsIoResult<()> {
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => value.to_be_bytes(),
            ByteOrder::LittleEndian => value.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_f32(&mut self, value: f32) -> AipsIoResult<()> {
        let bits = value.to_bits();
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => bits.to_be_bytes(),
            ByteOrder::LittleEndian => bits.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_f64(&mut self, value: f64) -> AipsIoResult<()> {
        let bits = value.to_bits();
        let bytes = match self.byte_order {
            ByteOrder::BigEndian => bits.to_be_bytes(),
            ByteOrder::LittleEndian => bits.to_le_bytes(),
        };
        self.inner.write_all(&bytes)?;
        Ok(())
    }

    pub fn write_complex32(&mut self, value: Complex32) -> AipsIoResult<()> {
        self.write_f32(value.re)?;
        self.write_f32(value.im)?;
        Ok(())
    }

    pub fn write_complex64(&mut self, value: Complex64) -> AipsIoResult<()> {
        self.write_f64(value.re)?;
        self.write_f64(value.im)?;
        Ok(())
    }

    pub fn write_string(&mut self, value: &str) -> AipsIoResult<()> {
        let len_u32 =
            u32::try_from(value.len()).map_err(|_| AipsIoError::LengthTooLarge(value.len()))?;
        self.write_u32(len_u32)?;
        self.inner.write_all(value.as_bytes())?;
        Ok(())
    }

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

    pub fn write_value(&mut self, value: &Value) -> AipsIoResult<()> {
        match value {
            Value::Scalar(v) => self.write_scalar(v),
            Value::Array(v) => self.write_array(v),
            Value::Record(_) => Err(AipsIoError::UnsupportedValueKind(ValueKind::Record)),
        }
    }
}

pub struct AipsReader<R> {
    inner: R,
    byte_order: ByteOrder,
}

impl<R: Read> AipsReader<R> {
    /// Create a reader that uses big-endian byte order.
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

    pub fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Read one boolean value.
    ///
    /// Other scalar read methods follow the same pattern.
    pub fn read_bool(&mut self) -> AipsIoResult<bool> {
        let mut buf = [0_u8; 1];
        self.inner.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(AipsIoError::InvalidBoolean(value)),
        }
    }

    pub fn read_u8(&mut self) -> AipsIoResult<u8> {
        let mut buf = [0_u8; 1];
        self.inner.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    pub fn read_u16(&mut self) -> AipsIoResult<u16> {
        let bytes = self.read_exact_array::<2>()?;
        Ok(match self.byte_order {
            ByteOrder::BigEndian => u16::from_be_bytes(bytes),
            ByteOrder::LittleEndian => u16::from_le_bytes(bytes),
        })
    }

    pub fn read_u32(&mut self) -> AipsIoResult<u32> {
        let bytes = self.read_exact_array::<4>()?;
        Ok(match self.byte_order {
            ByteOrder::BigEndian => u32::from_be_bytes(bytes),
            ByteOrder::LittleEndian => u32::from_le_bytes(bytes),
        })
    }

    pub fn read_i16(&mut self) -> AipsIoResult<i16> {
        let bytes = self.read_exact_array::<2>()?;
        Ok(match self.byte_order {
            ByteOrder::BigEndian => i16::from_be_bytes(bytes),
            ByteOrder::LittleEndian => i16::from_le_bytes(bytes),
        })
    }

    pub fn read_i32(&mut self) -> AipsIoResult<i32> {
        let bytes = self.read_exact_array::<4>()?;
        Ok(match self.byte_order {
            ByteOrder::BigEndian => i32::from_be_bytes(bytes),
            ByteOrder::LittleEndian => i32::from_le_bytes(bytes),
        })
    }

    pub fn read_i64(&mut self) -> AipsIoResult<i64> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(match self.byte_order {
            ByteOrder::BigEndian => i64::from_be_bytes(bytes),
            ByteOrder::LittleEndian => i64::from_le_bytes(bytes),
        })
    }

    pub fn read_f32(&mut self) -> AipsIoResult<f32> {
        let bits = self.read_u32()?;
        Ok(f32::from_bits(bits))
    }

    pub fn read_f64(&mut self) -> AipsIoResult<f64> {
        let bits = self.read_u64()?;
        Ok(f64::from_bits(bits))
    }

    pub fn read_complex32(&mut self) -> AipsIoResult<Complex32> {
        let re = self.read_f32()?;
        let im = self.read_f32()?;
        Ok(Complex32 { re, im })
    }

    pub fn read_complex64(&mut self) -> AipsIoResult<Complex64> {
        let re = self.read_f64()?;
        let im = self.read_f64()?;
        Ok(Complex64 { re, im })
    }

    pub fn read_string(&mut self) -> AipsIoResult<String> {
        let len = self.read_u32()? as usize;
        let mut bytes = vec![0_u8; len];
        self.inner.read_exact(&mut bytes)?;
        Ok(String::from_utf8(bytes)?)
    }

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

    pub fn read_value(&mut self, type_tag: TypeTag) -> AipsIoResult<Value> {
        match type_tag.rank {
            ValueRank::Scalar => self.read_scalar(type_tag.primitive).map(Value::Scalar),
            ValueRank::Array => self.read_array(type_tag.primitive).map(Value::Array),
        }
    }

    fn read_u64(&mut self) -> AipsIoResult<u64> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(match self.byte_order {
            ByteOrder::BigEndian => u64::from_be_bytes(bytes),
            ByteOrder::LittleEndian => u64::from_le_bytes(bytes),
        })
    }

    fn read_exact_array<const N: usize>(&mut self) -> AipsIoResult<[u8; N]> {
        let mut bytes = [0_u8; N];
        self.inner.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AipsIoError, AipsReader, AipsWriter, ArrayValue, ByteOrder, Complex64, PrimitiveType,
        RecordField, RecordValue, ScalarValue, Value,
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
