// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared reader/writer for the `StManArrayFile` binary format.
//!
//! Both `StManAipsIO` and `StandardStMan` use this format for variable-shape
//! (indirect) array data.  The file sits alongside the main data file with an
//! `i` suffix (e.g. `table.f0i`).
//!
//! ## On-disk layout
//!
//! | Offset | Field              | Bytes |
//! |--------|--------------------|-------|
//! |  0     | `version: u32`     | 4     |
//! |  4     | `file_length: i64` | 8     |
//! | 12     | padding `i32(0)`   | 4     |
//!
//! Each array record starts at an **8-byte aligned** offset and contains:
//!
//! ```text
//! [if version > 0]  ref_count : u32
//! ndim              : u32
//! shape[0..ndim]    : i32 × ndim
//! data              : canonical-encoded flat array (Fortran order)
//! ```
//!
//! Booleans are bit-packed (1 bit per element, LSB-first).
//! Strings use an indirection layer: the data area stores `u32` file offsets;
//! each offset points to `u32(length) + raw chars` elsewhere in the file.
//!
//! ## C++ reference
//!
//! - `casacore::StManArrayFile`  (`StArrayFile.h / StArrayFile.cc`)
//! - `casacore::StIndArray`      (`StIndArray.h / StIndArray.cc`)

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use casacore_types::{ArrayValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::data_type::CasacoreDataType;

// ---------------------------------------------------------------------------
// Byte-order helpers (local to this module)
// ---------------------------------------------------------------------------

/// Byte order for canonical encoding in the array file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArrayFileByteOrder {
    Big,
    Little,
}

fn read_u32(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<u32, StorageError> {
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => u32::from_be_bytes(buf),
        ArrayFileByteOrder::Little => u32::from_le_bytes(buf),
    })
}

fn read_i32(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<i32, StorageError> {
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => i32::from_be_bytes(buf),
        ArrayFileByteOrder::Little => i32::from_le_bytes(buf),
    })
}

fn read_i64(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<i64, StorageError> {
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => i64::from_be_bytes(buf),
        ArrayFileByteOrder::Little => i64::from_le_bytes(buf),
    })
}

fn write_u32(
    f: &mut (impl Write + ?Sized),
    val: u32,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

fn write_i32(
    f: &mut (impl Write + ?Sized),
    val: i32,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

fn write_i64(
    f: &mut (impl Write + ?Sized),
    val: i64,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

fn write_f32(
    f: &mut (impl Write + ?Sized),
    val: f32,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

fn write_f64(
    f: &mut (impl Write + ?Sized),
    val: f64,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

fn read_f32(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<f32, StorageError> {
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => f32::from_be_bytes(buf),
        ArrayFileByteOrder::Little => f32::from_le_bytes(buf),
    })
}

fn read_f64(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<f64, StorageError> {
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => f64::from_be_bytes(buf),
        ArrayFileByteOrder::Little => f64::from_le_bytes(buf),
    })
}

fn read_i16(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<i16, StorageError> {
    let mut buf = [0u8; 2];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => i16::from_be_bytes(buf),
        ArrayFileByteOrder::Little => i16::from_le_bytes(buf),
    })
}

fn read_u16(f: &mut (impl Read + ?Sized), bo: ArrayFileByteOrder) -> Result<u16, StorageError> {
    let mut buf = [0u8; 2];
    f.read_exact(&mut buf)?;
    Ok(match bo {
        ArrayFileByteOrder::Big => u16::from_be_bytes(buf),
        ArrayFileByteOrder::Little => u16::from_le_bytes(buf),
    })
}

fn write_i16(
    f: &mut (impl Write + ?Sized),
    val: i16,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

fn write_u16(
    f: &mut (impl Write + ?Sized),
    val: u16,
    bo: ArrayFileByteOrder,
) -> Result<(), StorageError> {
    let bytes = match bo {
        ArrayFileByteOrder::Big => val.to_be_bytes(),
        ArrayFileByteOrder::Little => val.to_le_bytes(),
    };
    f.write_all(&bytes)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Canonical element size for the array file (bytes per element)
// ---------------------------------------------------------------------------

/// Returns the canonical byte size per element for the given data type.
/// For Bool returns 0 (bit-packed). For String returns 4 (u32 offset).
fn canonical_elem_bytes(dt: CasacoreDataType) -> f64 {
    match dt {
        CasacoreDataType::TpBool => 0.125, // 1 bit
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => 1.0,
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => 2.0,
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => 4.0,
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => 8.0,
        CasacoreDataType::TpDComplex => 16.0,
        CasacoreDataType::TpString => 4.0, // u32 file offset per string
        _ => 0.0,
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reader for the `StManArrayFile` binary format.
///
/// Corresponds to C++ `casacore::StManArrayFile` in read mode.
pub(crate) struct StManArrayFileReader {
    file: BufReader<File>,
    bo: ArrayFileByteOrder,
    version: u32,
    #[allow(dead_code)]
    file_length: i64,
    /// Tracked logical position to avoid unnecessary seeks.
    pos: u64,
}

impl StManArrayFileReader {
    /// Open an existing array file.
    pub(crate) fn open(path: &Path, big_endian: bool) -> Result<Self, StorageError> {
        let mut file = BufReader::new(File::open(path)?);
        let bo = if big_endian {
            ArrayFileByteOrder::Big
        } else {
            ArrayFileByteOrder::Little
        };

        // Read 16-byte header.
        let version = read_u32(&mut file, bo)?;
        let file_length = read_i64(&mut file, bo)?;
        // Skip 4-byte padding.
        let mut _pad = [0u8; 4];
        file.read_exact(&mut _pad)?;

        Ok(Self {
            file,
            bo,
            version,
            file_length,
            pos: 16, // after 16-byte header
        })
    }

    /// Read an array at the given file offset.
    ///
    /// Returns `None` if `offset == 0` (undefined cell).
    /// Returns `Some((shape, Value::Array(...)))` on success.
    pub(crate) fn read_array_at(
        &mut self,
        offset: i64,
        dt: CasacoreDataType,
    ) -> Result<Option<Value>, StorageError> {
        if offset == 0 {
            return Ok(None);
        }

        let target = offset as u64;
        if self.pos != target {
            self.file.seek(SeekFrom::Start(target))?;
            self.pos = target;
        }

        // Skip refCount for version > 0.
        if self.version > 0 {
            let _ref_count = read_u32(&mut self.file, self.bo)?;
        }

        // Read shape.
        let ndim = read_u32(&mut self.file, self.bo)? as usize;
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            shape.push(read_i32(&mut self.file, self.bo)? as usize);
        }

        let total: usize = shape.iter().product();
        if total == 0 {
            let header_bytes = if self.version > 0 { 4u64 } else { 0 } + 4 + (ndim as u64 * 4);
            self.pos = target + header_bytes;
            return Ok(Some(make_empty_array(dt, &shape)));
        }

        // Read flat data in Fortran order, then construct ndarray.
        let array_value = self.read_typed_data(dt, total, &shape)?;

        // Update tracked position: header + shape + data bytes.
        // For strings, internal seeks invalidate the position.
        if dt == CasacoreDataType::TpString {
            self.pos = u64::MAX;
        } else {
            let header_bytes = if self.version > 0 { 4u64 } else { 0 } + 4 + (ndim as u64 * 4);
            let data_bytes = (total as f64 * canonical_elem_bytes(dt) + 0.95) as u64;
            self.pos = target + header_bytes + data_bytes;
        }

        Ok(Some(Value::Array(array_value)))
    }

    fn read_typed_data(
        &mut self,
        dt: CasacoreDataType,
        count: usize,
        shape: &[usize],
    ) -> Result<ArrayValue, StorageError> {
        match dt {
            CasacoreDataType::TpBool => {
                let byte_count = count.div_ceil(8);
                let mut buf = vec![0u8; byte_count];
                self.file.read_exact(&mut buf)?;
                let mut vals = Vec::with_capacity(count);
                for i in 0..count {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    vals.push((buf[byte_idx] >> bit_idx) & 1 != 0);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Bool(arr))
            }
            CasacoreDataType::TpUChar => {
                let mut vals = vec![0u8; count];
                self.file.read_exact(&mut vals)?;
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::UInt8(arr))
            }
            CasacoreDataType::TpShort => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_i16(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Int16(arr))
            }
            CasacoreDataType::TpUShort => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_u16(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::UInt16(arr))
            }
            CasacoreDataType::TpInt => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_i32(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Int32(arr))
            }
            CasacoreDataType::TpUInt => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_u32(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::UInt32(arr))
            }
            CasacoreDataType::TpInt64 => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_i64(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Int64(arr))
            }
            CasacoreDataType::TpFloat => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_f32(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Float32(arr))
            }
            CasacoreDataType::TpDouble => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(read_f64(&mut self.file, self.bo)?);
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Float64(arr))
            }
            CasacoreDataType::TpComplex => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    let re = read_f32(&mut self.file, self.bo)?;
                    let im = read_f32(&mut self.file, self.bo)?;
                    vals.push(casacore_types::Complex32::new(re, im));
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Complex32(arr))
            }
            CasacoreDataType::TpDComplex => {
                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    let re = read_f64(&mut self.file, self.bo)?;
                    let im = read_f64(&mut self.file, self.bo)?;
                    vals.push(casacore_types::Complex64::new(re, im));
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::Complex64(arr))
            }
            CasacoreDataType::TpString => {
                // Read u32 file offsets, then resolve each to a string.
                let mut offsets = Vec::with_capacity(count);
                for _ in 0..count {
                    offsets.push(read_u32(&mut self.file, self.bo)?);
                }
                let mut strings = Vec::with_capacity(count);
                for &off in &offsets {
                    if off == 0 {
                        strings.push(String::new());
                    } else {
                        self.file.seek(SeekFrom::Start(off as u64))?;
                        let len = read_u32(&mut self.file, self.bo)? as usize;
                        let mut buf = vec![0u8; len];
                        self.file.read_exact(&mut buf)?;
                        strings.push(String::from_utf8(buf).map_err(|e| {
                            StorageError::FormatMismatch(format!("invalid UTF-8: {e}"))
                        })?);
                    }
                }
                let arr = ArrayD::from_shape_vec(IxDyn(shape).f(), strings)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
                Ok(ArrayValue::String(arr))
            }
            _ => Err(StorageError::FormatMismatch(format!(
                "unsupported array file data type: {dt:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Writer for the `StManArrayFile` binary format.
///
/// Corresponds to C++ `casacore::StManArrayFile` in write mode.
pub(crate) struct StManArrayFileWriter {
    file: BufWriter<File>,
    bo: ArrayFileByteOrder,
    version: u32,
    file_length: i64,
}

impl StManArrayFileWriter {
    /// Create a new array file with a 16-byte header.
    ///
    /// `version`: 0 = no refcount, 1 = with refcount.
    pub(crate) fn create(
        path: &Path,
        big_endian: bool,
        version: u32,
    ) -> Result<Self, StorageError> {
        let mut file = BufWriter::new(File::create(path)?);
        let bo = if big_endian {
            ArrayFileByteOrder::Big
        } else {
            ArrayFileByteOrder::Little
        };

        let file_length: i64 = 16;
        write_u32(&mut file, version, bo)?;
        write_i64(&mut file, file_length, bo)?;
        write_i32(&mut file, 0, bo)?; // padding

        Ok(Self {
            file,
            bo,
            version,
            file_length,
        })
    }

    /// Write an array and return the file offset where it was written.
    ///
    /// The caller stores this offset in the metadata (AipsIO extents or SSM
    /// bucket data). The array data is written in Fortran (column-major) order.
    pub(crate) fn write_array(
        &mut self,
        value: &Value,
        dt: CasacoreDataType,
    ) -> Result<i64, StorageError> {
        let (shape, total) = match value {
            Value::Array(av) => {
                let s: Vec<usize> = av.shape().to_vec();
                let t: usize = s.iter().product();
                (s, t)
            }
            _ => {
                return Err(StorageError::FormatMismatch(
                    "write_array expects Value::Array".to_string(),
                ));
            }
        };

        // Align to 8 bytes.
        self.file_length = (self.file_length + 7) / 8 * 8;
        let offset = self.file_length;
        self.file.seek(SeekFrom::Start(offset as u64))?;

        // Write refcount for version > 0.
        let mut header_bytes: i64 = 0;
        if self.version > 0 {
            write_u32(&mut self.file, 1, self.bo)?; // refCount = 1
            header_bytes += 4;
        }

        // Write ndim + shape.
        let ndim = shape.len() as u32;
        write_u32(&mut self.file, ndim, self.bo)?;
        header_bytes += 4;
        for &dim in &shape {
            write_i32(&mut self.file, dim as i32, self.bo)?;
            header_bytes += 4;
        }

        // Calculate data size and reserve space.
        let elem_bytes = canonical_elem_bytes(dt);
        let data_bytes = (total as f64 * elem_bytes + 0.95) as i64;
        self.file_length = offset + header_bytes + data_bytes;

        // Write the actual array data.
        self.write_typed_data(value, dt)?;

        Ok(offset)
    }

    fn write_typed_data(
        &mut self,
        value: &Value,
        dt: CasacoreDataType,
    ) -> Result<(), StorageError> {
        match (value, dt) {
            (Value::Array(ArrayValue::Bool(arr)), CasacoreDataType::TpBool) => {
                let flat = fortran_flat_iter(arr);
                let byte_count = flat.len().div_ceil(8);
                let mut buf = vec![0u8; byte_count];
                for (i, &val) in flat.iter().enumerate() {
                    if val {
                        buf[i / 8] |= 1 << (i % 8);
                    }
                }
                self.file.write_all(&buf)?;
            }
            (Value::Array(ArrayValue::UInt8(arr)), CasacoreDataType::TpUChar) => {
                let flat = fortran_flat_iter(arr);
                self.file.write_all(&flat)?;
            }
            (Value::Array(ArrayValue::Int16(arr)), CasacoreDataType::TpShort) => {
                for v in fortran_flat_iter(arr) {
                    write_i16(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::UInt16(arr)), CasacoreDataType::TpUShort) => {
                for v in fortran_flat_iter(arr) {
                    write_u16(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::Int32(arr)), CasacoreDataType::TpInt) => {
                for v in fortran_flat_iter(arr) {
                    write_i32(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::UInt32(arr)), CasacoreDataType::TpUInt) => {
                for v in fortran_flat_iter(arr) {
                    write_u32(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::Int64(arr)), CasacoreDataType::TpInt64) => {
                for v in fortran_flat_iter(arr) {
                    write_i64(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::Float32(arr)), CasacoreDataType::TpFloat) => {
                for v in fortran_flat_iter(arr) {
                    write_f32(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::Float64(arr)), CasacoreDataType::TpDouble) => {
                for v in fortran_flat_iter(arr) {
                    write_f64(&mut self.file, v, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::Complex32(arr)), CasacoreDataType::TpComplex) => {
                for v in fortran_flat_iter(arr) {
                    write_f32(&mut self.file, v.re, self.bo)?;
                    write_f32(&mut self.file, v.im, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::Complex64(arr)), CasacoreDataType::TpDComplex) => {
                for v in fortran_flat_iter(arr) {
                    write_f64(&mut self.file, v.re, self.bo)?;
                    write_f64(&mut self.file, v.im, self.bo)?;
                }
            }
            (Value::Array(ArrayValue::String(arr)), CasacoreDataType::TpString) => {
                // Write u32 offset placeholders, then append strings.
                let flat = fortran_flat_iter(arr);
                let offsets_pos = self.file.stream_position()?;
                // Write zero placeholders.
                for _ in 0..flat.len() {
                    write_u32(&mut self.file, 0, self.bo)?;
                }
                // Now append each string and record its offset.
                let mut string_offsets = Vec::with_capacity(flat.len());
                for s in &flat {
                    if s.is_empty() {
                        string_offsets.push(0u32);
                    } else {
                        let pos = self.file_length;
                        self.file.seek(SeekFrom::Start(pos as u64))?;
                        write_u32(&mut self.file, s.len() as u32, self.bo)?;
                        self.file.write_all(s.as_bytes())?;
                        self.file_length = pos + 4 + s.len() as i64;
                        string_offsets.push(pos as u32);
                    }
                }
                // Go back and fill in the offsets.
                self.file.seek(SeekFrom::Start(offsets_pos))?;
                for off in &string_offsets {
                    write_u32(&mut self.file, *off, self.bo)?;
                }
            }
            _ => {
                return Err(StorageError::FormatMismatch(format!(
                    "type mismatch in array file write: {dt:?}"
                )));
            }
        }
        Ok(())
    }

    fn flush_header(&mut self) -> Result<(), StorageError> {
        self.file.seek(SeekFrom::Start(0))?;
        write_u32(&mut self.file, self.version, self.bo)?;
        write_i64(&mut self.file, self.file_length, self.bo)?;
        Ok(())
    }

    /// Flush the file header and buffered data to disk.
    ///
    /// Must be called after all arrays have been written.
    pub(crate) fn finish(&mut self) -> Result<(), StorageError> {
        self.flush_header()?;
        self.file.flush()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create an empty array Value for the given data type and shape.
fn make_empty_array(dt: CasacoreDataType, shape: &[usize]) -> Value {
    let s = IxDyn(shape);
    let av = match dt {
        CasacoreDataType::TpBool => ArrayValue::Bool(ArrayD::default(s)),
        CasacoreDataType::TpUChar => ArrayValue::UInt8(ArrayD::default(s)),
        CasacoreDataType::TpShort => ArrayValue::Int16(ArrayD::default(s)),
        CasacoreDataType::TpUShort => ArrayValue::UInt16(ArrayD::default(s)),
        CasacoreDataType::TpInt => ArrayValue::Int32(ArrayD::default(s)),
        CasacoreDataType::TpUInt => ArrayValue::UInt32(ArrayD::default(s)),
        CasacoreDataType::TpInt64 => ArrayValue::Int64(ArrayD::default(s)),
        CasacoreDataType::TpFloat => ArrayValue::Float32(ArrayD::default(s)),
        CasacoreDataType::TpDouble => ArrayValue::Float64(ArrayD::default(s)),
        CasacoreDataType::TpComplex => ArrayValue::Complex32(ArrayD::from_elem(
            s,
            casacore_types::Complex32::new(0.0, 0.0),
        )),
        CasacoreDataType::TpDComplex => ArrayValue::Complex64(ArrayD::from_elem(
            s,
            casacore_types::Complex64::new(0.0, 0.0),
        )),
        CasacoreDataType::TpString => ArrayValue::String(ArrayD::from_elem(s, String::new())),
        _ => ArrayValue::Float32(ArrayD::default(s)),
    };
    Value::Array(av)
}

/// Return array data as a flat Vec in Fortran (column-major) order.
///
/// Fast path: if the array is contiguous in Fortran order, copies directly
/// from memory via `as_slice_memory_order()` (avoids per-element index math).
/// Fallback: manual index iteration for non-contiguous or C-order arrays.
fn fortran_flat_iter<T: Clone>(arr: &ArrayD<T>) -> Vec<T> {
    if arr.is_empty() {
        return vec![];
    }
    // Fast path: contiguous Fortran-order array — bulk copy from memory.
    if let Some(slice) = arr.as_slice_memory_order() {
        let is_fortran = arr.ndim() <= 1 || arr.strides().first().is_some_and(|&s| s == 1);
        if is_fortran {
            return slice.to_vec();
        }
    }
    // Fallback: manual iteration for non-contiguous or C-order arrays.
    let shape = arr.shape();
    let total: usize = shape.iter().product();
    let ndim = shape.len();
    let mut result = Vec::with_capacity(total);
    let mut indices = vec![0usize; ndim];

    for _ in 0..total {
        result.push(arr[IxDyn(&indices)].clone());
        for d in 0..ndim {
            indices[d] += 1;
            if indices[d] < shape[d] {
                break;
            }
            indices[d] = 0;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;
    use tempfile::TempDir;

    #[test]
    fn round_trip_float32_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        // Write.
        let arr = ArrayD::from_shape_vec(IxDyn(&[2, 3]).f(), vec![1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0])
            .unwrap();
        let val = Value::Array(ArrayValue::Float32(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let off = writer.write_array(&val, CasacoreDataType::TpFloat).unwrap();
            writer.finish().unwrap();
            off
        };

        // Read.
        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpFloat)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Float32(read_arr)) => {
                assert_eq!(read_arr, arr);
            }
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_bool_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(IxDyn(&[3]).f(), vec![true, false, true]).unwrap();
        let val = Value::Array(ArrayValue::Bool(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 0).unwrap();
            let off = writer.write_array(&val, CasacoreDataType::TpBool).unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpBool)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Bool(read_arr)) => {
                assert_eq!(read_arr, arr);
            }
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_string_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(
            IxDyn(&[2]).f(),
            vec!["hello".to_string(), "world".to_string()],
        )
        .unwrap();
        let val = Value::Array(ArrayValue::String(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let off = writer
                .write_array(&val, CasacoreDataType::TpString)
                .unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpString)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::String(read_arr)) => {
                assert_eq!(read_arr, arr);
            }
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_complex32_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(
            IxDyn(&[2]).f(),
            vec![
                casacore_types::Complex32::new(1.0, 2.0),
                casacore_types::Complex32::new(3.0, 4.0),
            ],
        )
        .unwrap();
        let val = Value::Array(ArrayValue::Complex32(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let off = writer
                .write_array(&val, CasacoreDataType::TpComplex)
                .unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpComplex)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Complex32(read_arr)) => {
                assert_eq!(read_arr, arr);
            }
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn multiple_arrays_in_one_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr1 = Value::Array(ArrayValue::Int32(array![1i32, 2, 3].into_dyn()));
        let arr2 = Value::Array(ArrayValue::Int32(array![10i32, 20].into_dyn()));

        let (off1, off2) = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let o1 = writer.write_array(&arr1, CasacoreDataType::TpInt).unwrap();
            let o2 = writer.write_array(&arr2, CasacoreDataType::TpInt).unwrap();
            writer.finish().unwrap();
            (o1, o2)
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();

        let r1 = reader
            .read_array_at(off1, CasacoreDataType::TpInt)
            .unwrap()
            .unwrap();
        let r2 = reader
            .read_array_at(off2, CasacoreDataType::TpInt)
            .unwrap()
            .unwrap();

        assert_eq!(r1, arr1);
        assert_eq!(r2, arr2);
    }

    #[test]
    fn offset_zero_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            writer.finish().unwrap();
        }

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        assert!(
            reader
                .read_array_at(0, CasacoreDataType::TpFloat)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn round_trip_int32_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(IxDyn(&[4]), vec![10i32, -20, 30, -40]).unwrap();
        let val = Value::Array(ArrayValue::Int32(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let off = writer.write_array(&val, CasacoreDataType::TpInt).unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpInt)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Int32(read_arr)) => assert_eq!(read_arr, arr),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_int16_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(IxDyn(&[3]), vec![100i16, -200, 300]).unwrap();
        let val = Value::Array(ArrayValue::Int16(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 0).unwrap();
            let off = writer.write_array(&val, CasacoreDataType::TpShort).unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpShort)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Int16(read_arr)) => assert_eq!(read_arr, arr),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_int64_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(IxDyn(&[2]), vec![i64::MAX, i64::MIN]).unwrap();
        let val = Value::Array(ArrayValue::Int64(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let off = writer.write_array(&val, CasacoreDataType::TpInt64).unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpInt64)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Int64(read_arr)) => assert_eq!(read_arr, arr),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_u8_array() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(IxDyn(&[3]), vec![0u8, 128, 255]).unwrap();
        let val = Value::Array(ArrayValue::UInt8(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 0).unwrap();
            let off = writer.write_array(&val, CasacoreDataType::TpUChar).unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpUChar)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::UInt8(read_arr)) => assert_eq!(read_arr, arr),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn round_trip_complex64_array() {
        use casacore_types::Complex64;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = ArrayD::from_shape_vec(
            IxDyn(&[2]),
            vec![Complex64::new(1.0, -2.0), Complex64::new(3.0, -4.5)],
        )
        .unwrap();
        let val = Value::Array(ArrayValue::Complex64(arr.clone()));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, true, 1).unwrap();
            let off = writer
                .write_array(&val, CasacoreDataType::TpDComplex)
                .unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, true).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpDComplex)
            .unwrap()
            .unwrap();

        match result {
            Value::Array(ArrayValue::Complex64(read_arr)) => assert_eq!(read_arr, arr),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn little_endian_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.arr");

        let arr = Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        ));

        let offset = {
            let mut writer = StManArrayFileWriter::create(&path, false, 1).unwrap();
            let off = writer
                .write_array(&arr, CasacoreDataType::TpDouble)
                .unwrap();
            writer.finish().unwrap();
            off
        };

        let mut reader = StManArrayFileReader::open(&path, false).unwrap();
        let result = reader
            .read_array_at(offset, CasacoreDataType::TpDouble)
            .unwrap()
            .unwrap();

        assert_eq!(result, arr);
    }
}
