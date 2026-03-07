// SPDX-License-Identifier: LGPL-3.0-or-later
//! IncrementalStMan (ISM) delta-compression storage manager reader/writer.
//!
//! The ISM stores column values only when they change from the previous row,
//! making it space-efficient for slowly-changing columns (e.g. `ANTENNA1`,
//! `FEED_ID`, `SCAN_NUMBER` in a MeasurementSet).
//!
//! On-disk format: `table.fN` file with 512-byte AipsIO header at offset 0,
//! followed by fixed-size buckets, followed by an ISMIndex AipsIO object.
//!
//! The ISM file header, bucket data, and index all use the table's endian
//! setting (BE for v4 tables, LE for v5). The DM blob in `table.dat` always
//! uses big-endian AipsIO (file-based).
//!
//! C++ equivalent: `casacore/tables/DataMan/ISMBase`, `ISMBucket`, `ISMIndex`,
//! `ISMColumn`.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use casacore_aipsio::{AipsIo, ByteOrder};

use super::StorageError;
use super::canonical::{
    read_f32_be, read_f32_le, read_f64_be, read_f64_le, read_i16_be, read_i16_le, read_i32_be,
    read_i32_le, read_i64_be, read_i64_le, read_u16_be, read_u16_le, read_u32_be, read_u32_le,
    write_f32_be, write_f32_le, write_f64_be, write_f64_le, write_i16_be, write_i16_le,
    write_i32_be, write_i32_le, write_i64_be, write_i64_le, write_u16_be, write_u16_le,
    write_u32_be, write_u32_le,
};
use super::data_type::CasacoreDataType;
use super::stman_aipsio::ColumnRawData;
use super::table_control::ColumnDescContents;

const ISM_HEADER_SIZE: u64 = 512;
const AIPSIO_MAGIC: u32 = 0xbebebebe;

// ---------------------------------------------------------------------------
// In-memory AipsIO helpers
// ---------------------------------------------------------------------------

/// Minimal reader for AipsIO-framed data in either byte order.
struct AipsIoBuf<'a> {
    data: &'a [u8],
    pos: usize,
    order: ByteOrder,
    level: usize,
}

impl<'a> AipsIoBuf<'a> {
    fn new(data: &'a [u8], order: ByteOrder) -> Self {
        Self {
            data,
            pos: 0,
            order,
            level: 0,
        }
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], StorageError> {
        if self.pos + n > self.data.len() {
            return Err(StorageError::FormatMismatch(
                "ISM AipsIO buffer underrun".to_string(),
            ));
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u32(&mut self) -> Result<u32, StorageError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            ByteOrder::BigEndian => u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            ByteOrder::LittleEndian => u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    fn read_i32(&mut self) -> Result<i32, StorageError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            ByteOrder::BigEndian => i32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            ByteOrder::LittleEndian => i32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    fn read_u64(&mut self) -> Result<u64, StorageError> {
        let b = self.read_bytes(8)?;
        Ok(match self.order {
            ByteOrder::BigEndian => {
                u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
            ByteOrder::LittleEndian => {
                u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
        })
    }

    fn read_bool(&mut self) -> Result<bool, StorageError> {
        let b = self.read_bytes(1)?;
        Ok(b[0] != 0)
    }

    fn read_string(&mut self) -> Result<String, StorageError> {
        let len = self.read_u32()? as usize;
        let b = self.read_bytes(len)?;
        String::from_utf8(b.to_vec())
            .map_err(|e| StorageError::FormatMismatch(format!("invalid UTF-8 in ISM: {e}")))
    }

    fn getstart(&mut self, expected_type: &str) -> Result<u32, StorageError> {
        if self.level == 0 {
            let magic = self.read_u32()?;
            if magic != AIPSIO_MAGIC {
                return Err(StorageError::FormatMismatch(format!(
                    "ISM AipsIO magic mismatch: expected 0x{AIPSIO_MAGIC:08x}, got 0x{magic:08x}"
                )));
            }
        }
        self.level += 1;
        let _obj_len = self.read_u32()?;
        let type_name = self.read_string()?;
        if type_name != expected_type {
            return Err(StorageError::FormatMismatch(format!(
                "ISM AipsIO type mismatch: expected '{expected_type}', got '{type_name}'"
            )));
        }
        self.read_u32()
    }

    fn getend(&mut self) {
        if self.level > 0 {
            self.level -= 1;
        }
    }

    fn read_block_u32(&mut self) -> Result<Vec<u32>, StorageError> {
        let _version = self.getstart("Block")?;
        let count = self.read_u32()?;
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            values.push(self.read_u32()?);
        }
        self.getend();
        Ok(values)
    }

    fn read_block_u64(&mut self) -> Result<Vec<u64>, StorageError> {
        let _version = self.getstart("Block")?;
        let count = self.read_u32()?;
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            values.push(self.read_u64()?);
        }
        self.getend();
        Ok(values)
    }
}

/// Detect the byte order of in-memory AipsIO data.
fn detect_aipsio_byte_order(data: &[u8]) -> Result<ByteOrder, StorageError> {
    if data.len() < 8 {
        return Err(StorageError::FormatMismatch(
            "ISM data too short for byte order detection".to_string(),
        ));
    }
    let be_len = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let le_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

    let be_ok = be_len > 0 && be_len < 4096;
    let le_ok = le_len > 0 && le_len < 4096;

    match (be_ok, le_ok) {
        (true, false) => Ok(ByteOrder::BigEndian),
        (false, true) => Ok(ByteOrder::LittleEndian),
        (true, true) => Ok(ByteOrder::BigEndian),
        (false, false) => Err(StorageError::FormatMismatch(format!(
            "ISM: cannot detect byte order (be_len={be_len}, le_len={le_len})"
        ))),
    }
}

/// Minimal writer for in-memory AipsIO in the given byte order.
struct AipsIoWriteBuf {
    data: Vec<u8>,
    order: ByteOrder,
    len_positions: Vec<usize>,
    level: usize,
}

impl AipsIoWriteBuf {
    fn new(order: ByteOrder) -> Self {
        Self {
            data: Vec::new(),
            order,
            len_positions: Vec::new(),
            level: 0,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    fn put_u8(&mut self, val: u8) {
        self.data.push(val);
    }

    fn put_u32(&mut self, val: u32) {
        match self.order {
            ByteOrder::BigEndian => self.data.extend_from_slice(&val.to_be_bytes()),
            ByteOrder::LittleEndian => self.data.extend_from_slice(&val.to_le_bytes()),
        }
    }

    fn put_i32(&mut self, val: i32) {
        match self.order {
            ByteOrder::BigEndian => self.data.extend_from_slice(&val.to_be_bytes()),
            ByteOrder::LittleEndian => self.data.extend_from_slice(&val.to_le_bytes()),
        }
    }

    fn put_u64(&mut self, val: u64) {
        match self.order {
            ByteOrder::BigEndian => self.data.extend_from_slice(&val.to_be_bytes()),
            ByteOrder::LittleEndian => self.data.extend_from_slice(&val.to_le_bytes()),
        }
    }

    fn put_bool(&mut self, val: bool) {
        self.put_u8(if val { 1 } else { 0 });
    }

    fn put_string(&mut self, s: &str) {
        self.put_u32(s.len() as u32);
        self.data.extend_from_slice(s.as_bytes());
    }

    fn putstart(&mut self, type_name: &str, version: u32) {
        self.put_u32(AIPSIO_MAGIC);
        let pos = self.data.len();
        self.put_u32(0);
        self.len_positions.push(pos);
        self.put_string(type_name);
        self.put_u32(version);
        self.level += 1;
    }

    fn putstart_nested(&mut self, type_name: &str, version: u32) {
        let pos = self.data.len();
        self.put_u32(0);
        self.len_positions.push(pos);
        self.put_string(type_name);
        self.put_u32(version);
        self.level += 1;
    }

    fn putend(&mut self) {
        if let Some(pos) = self.len_positions.pop() {
            let obj_len = (self.data.len() - pos) as u32;
            let bytes = match self.order {
                ByteOrder::BigEndian => obj_len.to_be_bytes(),
                ByteOrder::LittleEndian => obj_len.to_le_bytes(),
            };
            self.data[pos..pos + 4].copy_from_slice(&bytes);
        }
        if self.level > 0 {
            self.level -= 1;
        }
    }

    fn putend_nested(&mut self) {
        self.putend();
    }
}

// ---------------------------------------------------------------------------
// Parsed types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct IsmHeader {
    bucket_size: u32,
    nr_buckets: u32,
    big_endian: bool,
    io_order: ByteOrder,
}

/// Row-to-bucket mapping. `rows[i]` is the start row of bucket interval `i`.
/// `rows[nused]` is the total row count sentinel.
#[derive(Debug, Clone)]
struct IsmIndex {
    rows: Vec<u64>,
    bucket_nrs: Vec<u32>,
}

/// Per-column sparse index within a bucket.
#[derive(Debug, Clone)]
struct IsmBucketColIndex {
    n_values: u32,
    row_nrs: Vec<u32>,
    offsets: Vec<u32>,
}

/// A parsed ISM bucket.
#[derive(Debug, Clone)]
struct IsmBucket {
    data: Vec<u8>,
    col_indices: Vec<IsmBucketColIndex>,
}

// ---------------------------------------------------------------------------
// ISM-specific canonical element size (Bool = 1 byte, not bit-packed)
// ---------------------------------------------------------------------------

/// Returns the canonical byte size for one element in the ISM data area.
///
/// Unlike SSM (which bit-packs bools), ISM stores each Bool as 1 byte.
/// Returns 0 for variable-length types (String).
fn ism_element_size(dt: CasacoreDataType) -> usize {
    match dt {
        CasacoreDataType::TpBool => 1,
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => 1,
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => 2,
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => 4,
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => 8,
        CasacoreDataType::TpDComplex => 16,
        CasacoreDataType::TpString => 0, // variable-length
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

fn parse_ism_header(file: &mut (impl Read + Seek)) -> Result<IsmHeader, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = vec![0u8; ISM_HEADER_SIZE as usize];
    file.read_exact(&mut header_buf)?;

    let io_order = detect_aipsio_byte_order(&header_buf)?;
    let mut buf = AipsIoBuf::new(&header_buf, io_order);
    let version = buf.getstart("IncrementalStMan")?;

    let big_endian = if version >= 5 {
        buf.read_bool()?
    } else {
        true // v4 is always big-endian
    };

    let bucket_size = buf.read_u32()?;
    let nr_buckets = buf.read_u32()?;
    let _pers_cache_size = buf.read_u32()?;
    let _uniq_nr = buf.read_u32()?;
    let _nr_free_buckets = buf.read_u32()?;
    let _first_free_bucket = buf.read_i32()?;

    Ok(IsmHeader {
        bucket_size,
        nr_buckets,
        big_endian,
        io_order,
    })
}

// ---------------------------------------------------------------------------
// DM blob parsing ("ISM" v3 from table.dat)
// ---------------------------------------------------------------------------

fn parse_ism_dm_blob(data: &[u8]) -> Result<String, StorageError> {
    let cursor = std::io::Cursor::new(data.to_vec());
    let mut io = AipsIo::new_read_only(cursor);
    let _version = io.getstart("ISM")?;
    let name = io.get_string()?;
    io.getend()?;
    Ok(name)
}

// ---------------------------------------------------------------------------
// Index parsing (at end of file, after all buckets)
// ---------------------------------------------------------------------------

fn parse_ism_index(file: &mut File, header: &IsmHeader) -> Result<IsmIndex, StorageError> {
    let index_offset = ISM_HEADER_SIZE + (header.nr_buckets as u64) * (header.bucket_size as u64);
    file.seek(SeekFrom::Start(index_offset))?;

    let mut index_data = Vec::new();
    file.read_to_end(&mut index_data)?;

    if index_data.is_empty() {
        return Ok(IsmIndex {
            rows: vec![0],
            bucket_nrs: vec![],
        });
    }

    let mut buf = AipsIoBuf::new(&index_data, header.io_order);
    let version = buf.getstart("ISMIndex")?;

    let _nused = buf.read_u32()?;

    let rows = if version == 1 {
        buf.read_block_u32()?
            .into_iter()
            .map(|v| v as u64)
            .collect()
    } else {
        buf.read_block_u64()?
    };

    let bucket_nrs = buf.read_block_u32()?;
    buf.getend();

    Ok(IsmIndex { rows, bucket_nrs })
}

// ---------------------------------------------------------------------------
// Bucket parsing
// ---------------------------------------------------------------------------

fn read_ism_bucket(
    file: &mut File,
    header: &IsmHeader,
    bucket_nr: u32,
) -> Result<Vec<u8>, StorageError> {
    let offset = ISM_HEADER_SIZE + (bucket_nr as u64) * (header.bucket_size as u64);
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; header.bucket_size as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn parse_ism_bucket(
    raw: &[u8],
    n_cols: usize,
    big_endian: bool,
) -> Result<IsmBucket, StorageError> {
    if raw.len() < 4 {
        return Err(StorageError::FormatMismatch(
            "ISM bucket too small".to_string(),
        ));
    }

    // Read index_offset from byte 0..3 in table byte order
    let raw_offset = if big_endian {
        read_u32_be(raw)
    } else {
        read_u32_le(raw)
    };

    // Bit 31 = 64-bit row numbers flag
    if raw_offset & 0x80000000 != 0 {
        return Err(StorageError::FormatMismatch(
            "ISM 64-bit row numbers not supported".to_string(),
        ));
    }

    let index_offset = (raw_offset & 0x7fffffff) as usize;

    // Data area: bytes 4..index_offset
    let data_end = index_offset.min(raw.len());
    let data = raw[4..data_end].to_vec();

    // Per-column index: bytes index_offset..end
    let mut pos = index_offset;
    let mut col_indices = Vec::with_capacity(n_cols);

    for _ in 0..n_cols {
        if pos + 4 > raw.len() {
            break;
        }
        let n_values = if big_endian {
            read_u32_be(&raw[pos..])
        } else {
            read_u32_le(&raw[pos..])
        };
        pos += 4;

        let n = n_values as usize;

        // Row numbers (u32 each)
        let mut row_nrs = Vec::with_capacity(n);
        for _ in 0..n {
            if pos + 4 > raw.len() {
                return Err(StorageError::FormatMismatch(
                    "ISM bucket index overrun reading row_nrs".to_string(),
                ));
            }
            row_nrs.push(if big_endian {
                read_u32_be(&raw[pos..])
            } else {
                read_u32_le(&raw[pos..])
            });
            pos += 4;
        }

        // Offsets (u32 each)
        let mut offsets = Vec::with_capacity(n);
        for _ in 0..n {
            if pos + 4 > raw.len() {
                return Err(StorageError::FormatMismatch(
                    "ISM bucket index overrun reading offsets".to_string(),
                ));
            }
            offsets.push(if big_endian {
                read_u32_be(&raw[pos..])
            } else {
                read_u32_le(&raw[pos..])
            });
            pos += 4;
        }

        col_indices.push(IsmBucketColIndex {
            n_values,
            row_nrs,
            offsets,
        });
    }

    Ok(IsmBucket { data, col_indices })
}

// ---------------------------------------------------------------------------
// Value lookup within a bucket
// ---------------------------------------------------------------------------

/// Find the interval index for `rel_row` in the per-column index.
///
/// Returns the index `k` such that `row_nrs[k] <= rel_row < row_nrs[k+1]`
/// (or k is the last entry). Row 0 always has an entry, so the search
/// always succeeds for valid relative row numbers.
fn get_interval(col_index: &IsmBucketColIndex, rel_row: u32) -> usize {
    // Binary search for the largest row_nr <= rel_row
    let pos = col_index.row_nrs.partition_point(|&r| r <= rel_row);
    if pos == 0 { 0 } else { pos - 1 }
}

/// Read a single scalar value from the ISM data area at the given offset.
fn read_scalar_at(
    data: &[u8],
    offset: usize,
    dt: CasacoreDataType,
    big_endian: bool,
) -> Result<casacore_types::Value, StorageError> {
    use casacore_types::{ScalarValue, Value};

    let d = &data[offset..];
    let val = match dt {
        CasacoreDataType::TpBool => Value::Scalar(ScalarValue::Bool(d[0] != 0)),
        CasacoreDataType::TpUChar => Value::Scalar(ScalarValue::UInt8(d[0])),
        CasacoreDataType::TpShort => Value::Scalar(ScalarValue::Int16(if big_endian {
            read_i16_be(d)
        } else {
            read_i16_le(d)
        })),
        CasacoreDataType::TpUShort => Value::Scalar(ScalarValue::UInt16(if big_endian {
            read_u16_be(d)
        } else {
            read_u16_le(d)
        })),
        CasacoreDataType::TpInt => Value::Scalar(ScalarValue::Int32(if big_endian {
            read_i32_be(d)
        } else {
            read_i32_le(d)
        })),
        CasacoreDataType::TpUInt => Value::Scalar(ScalarValue::UInt32(if big_endian {
            read_u32_be(d)
        } else {
            read_u32_le(d)
        })),
        CasacoreDataType::TpFloat => Value::Scalar(ScalarValue::Float32(if big_endian {
            read_f32_be(d)
        } else {
            read_f32_le(d)
        })),
        CasacoreDataType::TpDouble => Value::Scalar(ScalarValue::Float64(if big_endian {
            read_f64_be(d)
        } else {
            read_f64_le(d)
        })),
        CasacoreDataType::TpInt64 => Value::Scalar(ScalarValue::Int64(if big_endian {
            read_i64_be(d)
        } else {
            read_i64_le(d)
        })),
        CasacoreDataType::TpComplex => {
            let (re, im) = if big_endian {
                (read_f32_be(d), read_f32_be(&d[4..]))
            } else {
                (read_f32_le(d), read_f32_le(&d[4..]))
            };
            Value::Scalar(ScalarValue::Complex32(casacore_types::Complex32::new(
                re, im,
            )))
        }
        CasacoreDataType::TpDComplex => {
            let (re, im) = if big_endian {
                (read_f64_be(d), read_f64_be(&d[8..]))
            } else {
                (read_f64_le(d), read_f64_le(&d[8..]))
            };
            Value::Scalar(ScalarValue::Complex64(casacore_types::Complex64::new(
                re, im,
            )))
        }
        CasacoreDataType::TpString => {
            let s = read_ism_string(d, 1, big_endian)?;
            Value::Scalar(ScalarValue::String(
                s.into_iter().next().unwrap_or_default(),
            ))
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported ISM data type: {dt:?}"
            )));
        }
    };
    Ok(val)
}

/// Read ISM-encoded strings from the data area.
///
/// ISM string format (C++ `ISMColumn::fromString` / `toString`):
///
/// For scalar strings (nvalues == 1):
///   `[u32: total_length] [string_bytes]`
///   where `total_length` = 4 + string_length (includes the u32 itself).
///
/// For string arrays (nvalues > 1):
///   `[u32: total_length] [per-string: [u32: str_len] [str_data]] ...`
///   where `total_length` includes itself.
fn read_ism_string(
    data: &[u8],
    nvalues: usize,
    big_endian: bool,
) -> Result<Vec<String>, StorageError> {
    let total_length = if big_endian {
        read_u32_be(data)
    } else {
        read_u32_le(data)
    } as usize;

    let mut pos = 4usize; // skip total_length field
    let mut strings = Vec::with_capacity(nvalues);

    if nvalues == 1 {
        // Scalar: total_length includes itself, remaining bytes are the string.
        let str_len = total_length.saturating_sub(4);
        let s = String::from_utf8(data[pos..pos + str_len].to_vec())
            .map_err(|e| StorageError::FormatMismatch(format!("ISM string not UTF-8: {e}")))?;
        strings.push(s);
    } else {
        // Array: each string has its own u32 length prefix.
        for _ in 0..nvalues {
            if pos + 4 > total_length {
                break;
            }
            let str_len = if big_endian {
                read_u32_be(&data[pos..])
            } else {
                read_u32_le(&data[pos..])
            } as usize;
            pos += 4;

            let s = String::from_utf8(data[pos..pos + str_len].to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("ISM string not UTF-8: {e}")))?;
            pos += str_len;
            strings.push(s);
        }
    }

    Ok(strings)
}

/// Read a fixed-shape array value from the ISM data area at the given offset.
fn read_array_at(
    data: &[u8],
    offset: usize,
    dt: CasacoreDataType,
    nrelem: usize,
    shape: &[i32],
    big_endian: bool,
) -> Result<casacore_types::Value, StorageError> {
    use casacore_types::{ArrayValue, Value};
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let d = &data[offset..];
    let shape_usize: Vec<usize> = shape.iter().map(|&s| s as usize).collect();

    let arr_val = match dt {
        CasacoreDataType::TpBool => {
            // C++ ISM stores bools bit-packed (1 bit per element).
            let v: Vec<bool> = (0..nrelem)
                .map(|i| {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    (d[byte_idx] >> bit_idx) & 1 != 0
                })
                .collect();
            ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUChar => {
            let v: Vec<u8> = d[..nrelem].to_vec();
            ArrayValue::UInt8(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpShort => {
            let v: Vec<i16> = (0..nrelem)
                .map(|i| {
                    if big_endian {
                        read_i16_be(&d[i * 2..])
                    } else {
                        read_i16_le(&d[i * 2..])
                    }
                })
                .collect();
            ArrayValue::Int16(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpInt => {
            let v: Vec<i32> = (0..nrelem)
                .map(|i| {
                    if big_endian {
                        read_i32_be(&d[i * 4..])
                    } else {
                        read_i32_le(&d[i * 4..])
                    }
                })
                .collect();
            ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpFloat => {
            let v: Vec<f32> = (0..nrelem)
                .map(|i| {
                    if big_endian {
                        read_f32_be(&d[i * 4..])
                    } else {
                        read_f32_le(&d[i * 4..])
                    }
                })
                .collect();
            ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpDouble => {
            let v: Vec<f64> = (0..nrelem)
                .map(|i| {
                    if big_endian {
                        read_f64_be(&d[i * 8..])
                    } else {
                        read_f64_le(&d[i * 8..])
                    }
                })
                .collect();
            ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpInt64 => {
            let v: Vec<i64> = (0..nrelem)
                .map(|i| {
                    if big_endian {
                        read_i64_be(&d[i * 8..])
                    } else {
                        read_i64_le(&d[i * 8..])
                    }
                })
                .collect();
            ArrayValue::Int64(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpComplex => {
            let v: Vec<casacore_types::Complex32> = (0..nrelem)
                .map(|i| {
                    let (re, im) = if big_endian {
                        (read_f32_be(&d[i * 8..]), read_f32_be(&d[i * 8 + 4..]))
                    } else {
                        (read_f32_le(&d[i * 8..]), read_f32_le(&d[i * 8 + 4..]))
                    };
                    casacore_types::Complex32::new(re, im)
                })
                .collect();
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpDComplex => {
            let v: Vec<casacore_types::Complex64> = (0..nrelem)
                .map(|i| {
                    let (re, im) = if big_endian {
                        (read_f64_be(&d[i * 16..]), read_f64_be(&d[i * 16 + 8..]))
                    } else {
                        (read_f64_le(&d[i * 16..]), read_f64_le(&d[i * 16 + 8..]))
                    };
                    casacore_types::Complex64::new(re, im)
                })
                .collect();
            ArrayValue::Complex64(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), v)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpString => {
            let strings = read_ism_string(d, nrelem, big_endian)?;
            ArrayValue::String(
                ArrayD::from_shape_vec(IxDyn(&shape_usize).f(), strings)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported ISM array type: {dt:?}"
            )));
        }
    };
    Ok(Value::Array(arr_val))
}

// ---------------------------------------------------------------------------
// Read entry point
// ---------------------------------------------------------------------------

pub(crate) fn read_ism_file(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    nrrow: usize,
) -> Result<Vec<(String, ColumnRawData)>, StorageError> {
    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    let index = parse_ism_index(&mut file, &header)?;
    let be = header.big_endian;

    // Pre-compute column info
    let col_info: Vec<(CasacoreDataType, usize, bool)> = col_descs
        .iter()
        .map(|c| {
            let scalar_dt =
                CasacoreDataType::from_primitive_type(c.require_primitive_type()?, false);
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                1
            };
            Ok((
                scalar_dt,
                nrelem,
                c.is_array && c.nrdim > 0 && !c.shape.is_empty(),
            ))
        })
        .collect::<Result<_, StorageError>>()?;

    // Initialize column-major result vectors
    let ncol = col_descs.len();
    let mut columns: Vec<Vec<casacore_types::Value>> =
        (0..ncol).map(|_| Vec::with_capacity(nrrow)).collect();

    // Iterate over bucket intervals from the ISMIndex
    let n_intervals = index.bucket_nrs.len();
    let mut last_bucket_nr: Option<u32> = None;
    let mut cached_bucket: Option<IsmBucket> = None;

    for interval in 0..n_intervals {
        let bucket_start = index.rows[interval] as usize;
        let bucket_end = index.rows[interval + 1] as usize;
        let rows_in_bucket = bucket_end - bucket_start;
        let bucket_nr = index.bucket_nrs[interval];

        // Load bucket (cache for repeated access)
        if last_bucket_nr != Some(bucket_nr) {
            let raw = read_ism_bucket(&mut file, &header, bucket_nr)?;
            cached_bucket = Some(parse_ism_bucket(&raw, ncol, be)?);
            last_bucket_nr = Some(bucket_nr);
        }
        let bucket = cached_bucket.as_ref().unwrap();

        // For each column, expand the interval values
        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            let (dt, nrelem, is_fixed_array) = col_info[col_idx];

            if col_idx >= bucket.col_indices.len() {
                // Column not in this bucket — fill with defaults
                for _ in 0..rows_in_bucket {
                    columns[col_idx].push(default_value(dt, is_fixed_array, col_desc));
                }
                continue;
            }

            let col_index = &bucket.col_indices[col_idx];

            // Expand each row in this bucket's interval
            for rel_row in 0..rows_in_bucket {
                let k = get_interval(col_index, rel_row as u32);
                let data_offset = col_index.offsets[k] as usize;

                let val = if is_fixed_array {
                    read_array_at(&bucket.data, data_offset, dt, nrelem, &col_desc.shape, be)?
                } else {
                    read_scalar_at(&bucket.data, data_offset, dt, be)?
                };
                columns[col_idx].push(val);
            }
        }
    }

    // Convert Vec<Value> columns to ColumnRawData
    let mut result = Vec::with_capacity(ncol);
    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        let (dt, _nrelem, is_fixed_array) = col_info[col_idx];
        let values = &columns[col_idx];
        let raw = if is_fixed_array {
            values_to_column_raw_array(values, dt, col_desc)?
        } else {
            values_to_column_raw_scalar(values, dt)?
        };
        result.push((col_desc.col_name.clone(), raw));
    }

    Ok(result)
}

fn default_value(
    dt: CasacoreDataType,
    _is_array: bool,
    _col_desc: &ColumnDescContents,
) -> casacore_types::Value {
    use casacore_types::{ScalarValue, Value};
    match dt {
        CasacoreDataType::TpBool => Value::Scalar(ScalarValue::Bool(false)),
        CasacoreDataType::TpUChar => Value::Scalar(ScalarValue::UInt8(0)),
        CasacoreDataType::TpShort => Value::Scalar(ScalarValue::Int16(0)),
        CasacoreDataType::TpUShort => Value::Scalar(ScalarValue::UInt16(0)),
        CasacoreDataType::TpInt => Value::Scalar(ScalarValue::Int32(0)),
        CasacoreDataType::TpUInt => Value::Scalar(ScalarValue::UInt32(0)),
        CasacoreDataType::TpFloat => Value::Scalar(ScalarValue::Float32(0.0)),
        CasacoreDataType::TpDouble => Value::Scalar(ScalarValue::Float64(0.0)),
        CasacoreDataType::TpInt64 => Value::Scalar(ScalarValue::Int64(0)),
        CasacoreDataType::TpComplex => Value::Scalar(ScalarValue::Complex32(
            casacore_types::Complex32::new(0.0, 0.0),
        )),
        CasacoreDataType::TpDComplex => Value::Scalar(ScalarValue::Complex64(
            casacore_types::Complex64::new(0.0, 0.0),
        )),
        CasacoreDataType::TpString => Value::Scalar(ScalarValue::String(String::new())),
        _ => Value::Scalar(ScalarValue::Int32(0)),
    }
}

/// Convert scalar values into column-major ColumnRawData.
fn values_to_column_raw_scalar(
    values: &[casacore_types::Value],
    dt: CasacoreDataType,
) -> Result<ColumnRawData, StorageError> {
    use casacore_types::{ScalarValue, Value};

    match dt {
        CasacoreDataType::TpBool => {
            let v: Vec<bool> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Bool(b)) => *b,
                    _ => false,
                })
                .collect();
            Ok(ColumnRawData::Bool(v))
        }
        CasacoreDataType::TpUChar => {
            let v: Vec<u8> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::UInt8(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::UInt8(v))
        }
        CasacoreDataType::TpShort => {
            let v: Vec<i16> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Int16(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::Int16(v))
        }
        CasacoreDataType::TpUShort => {
            let v: Vec<u16> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::UInt16(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::UInt16(v))
        }
        CasacoreDataType::TpInt => {
            let v: Vec<i32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Int32(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::Int32(v))
        }
        CasacoreDataType::TpUInt => {
            let v: Vec<u32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::UInt32(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::UInt32(v))
        }
        CasacoreDataType::TpFloat => {
            let v: Vec<f32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Float32(b)) => *b,
                    _ => 0.0,
                })
                .collect();
            Ok(ColumnRawData::Float32(v))
        }
        CasacoreDataType::TpDouble => {
            let v: Vec<f64> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Float64(b)) => *b,
                    _ => 0.0,
                })
                .collect();
            Ok(ColumnRawData::Float64(v))
        }
        CasacoreDataType::TpInt64 => {
            let v: Vec<i64> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Int64(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::Int64(v))
        }
        CasacoreDataType::TpComplex => {
            let v: Vec<casacore_types::Complex32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Complex32(c)) => *c,
                    _ => casacore_types::Complex32::new(0.0, 0.0),
                })
                .collect();
            Ok(ColumnRawData::Complex32(v))
        }
        CasacoreDataType::TpDComplex => {
            let v: Vec<casacore_types::Complex64> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Complex64(c)) => *c,
                    _ => casacore_types::Complex64::new(0.0, 0.0),
                })
                .collect();
            Ok(ColumnRawData::Complex64(v))
        }
        CasacoreDataType::TpString => {
            let v: Vec<String> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::String(s)) => s.clone(),
                    _ => String::new(),
                })
                .collect();
            Ok(ColumnRawData::String(v))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported ISM scalar type: {dt:?}"
        ))),
    }
}

/// Convert array values into column-major ColumnRawData (flattened in Fortran order).
fn values_to_column_raw_array(
    values: &[casacore_types::Value],
    dt: CasacoreDataType,
    col_desc: &ColumnDescContents,
) -> Result<ColumnRawData, StorageError> {
    use casacore_types::{ArrayValue, Value};

    let nrelem: usize = col_desc.shape.iter().map(|&s| s as usize).product();

    match dt {
        CasacoreDataType::TpFloat => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Float32(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(0.0f32, nrelem));
                }
            }
            Ok(ColumnRawData::Float32(flat))
        }
        CasacoreDataType::TpDouble => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Float64(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(0.0f64, nrelem));
                }
            }
            Ok(ColumnRawData::Float64(flat))
        }
        CasacoreDataType::TpInt => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Int32(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(0i32, nrelem));
                }
            }
            Ok(ColumnRawData::Int32(flat))
        }
        CasacoreDataType::TpComplex => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Complex32(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(
                        casacore_types::Complex32::new(0.0, 0.0),
                        nrelem,
                    ));
                }
            }
            Ok(ColumnRawData::Complex32(flat))
        }
        CasacoreDataType::TpDComplex => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Complex64(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(
                        casacore_types::Complex64::new(0.0, 0.0),
                        nrelem,
                    ));
                }
            }
            Ok(ColumnRawData::Complex64(flat))
        }
        CasacoreDataType::TpBool => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Bool(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(false, nrelem));
                }
            }
            Ok(ColumnRawData::Bool(flat))
        }
        CasacoreDataType::TpUChar => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::UInt8(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(0u8, nrelem));
                }
            }
            Ok(ColumnRawData::UInt8(flat))
        }
        CasacoreDataType::TpShort => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Int16(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(0i16, nrelem));
                }
            }
            Ok(ColumnRawData::Int16(flat))
        }
        CasacoreDataType::TpInt64 => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::Int64(arr)) = v {
                    flat.extend(arr.t().iter());
                } else {
                    flat.extend(std::iter::repeat_n(0i64, nrelem));
                }
            }
            Ok(ColumnRawData::Int64(flat))
        }
        CasacoreDataType::TpString => {
            let mut flat = Vec::with_capacity(values.len() * nrelem);
            for v in values {
                if let Value::Array(ArrayValue::String(arr)) = v {
                    flat.extend(arr.t().iter().cloned());
                } else {
                    flat.extend(std::iter::repeat_n(String::new(), nrelem));
                }
            }
            Ok(ColumnRawData::String(flat))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported ISM array type: {dt:?}"
        ))),
    }
}

// ===========================================================================
// ISM Write Path
// ===========================================================================

/// Encode a single scalar or array value into ISM canonical bytes.
fn encode_value_bytes(
    value: Option<&casacore_types::Value>,
    dt: CasacoreDataType,
    nrelem: usize,
    big_endian: bool,
) -> Vec<u8> {
    use casacore_types::{ArrayValue, ScalarValue, Value};

    if dt == CasacoreDataType::TpString {
        return encode_string_value(value, nrelem, big_endian);
    }

    // Bool is bit-packed (1 bit per element), matching C++ canonical conversion.
    if dt == CasacoreDataType::TpBool {
        let total = nrelem.div_ceil(8);
        let mut buf = vec![0u8; total];
        if nrelem == 1 {
            let v = match value {
                Some(Value::Scalar(ScalarValue::Bool(b))) => *b,
                _ => false,
            };
            if v {
                buf[0] = 1;
            }
        } else if let Some(Value::Array(ArrayValue::Bool(arr))) = value {
            for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                if v {
                    buf[i / 8] |= 1 << (i % 8);
                }
            }
        }
        return buf;
    }

    let elem_size = ism_element_size(dt);
    let total = elem_size * nrelem;
    let mut buf = vec![0u8; total];

    match dt {
        CasacoreDataType::TpUChar => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::UInt8(v))) => *v,
                _ => 0,
            };
            buf[0] = v;
        }
        CasacoreDataType::TpShort => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::Int16(v))) => *v,
                _ => 0,
            };
            if big_endian {
                write_i16_be(&mut buf, v);
            } else {
                write_i16_le(&mut buf, v);
            }
        }
        CasacoreDataType::TpUShort => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::UInt16(v))) => *v,
                _ => 0,
            };
            if big_endian {
                write_u16_be(&mut buf, v);
            } else {
                write_u16_le(&mut buf, v);
            }
        }
        CasacoreDataType::TpInt => {
            if nrelem == 1 {
                let v = match value {
                    Some(Value::Scalar(ScalarValue::Int32(v))) => *v,
                    _ => 0,
                };
                if big_endian {
                    write_i32_be(&mut buf, v);
                } else {
                    write_i32_le(&mut buf, v);
                }
            } else if let Some(Value::Array(ArrayValue::Int32(arr))) = value {
                for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                    if big_endian {
                        write_i32_be(&mut buf[i * 4..], v);
                    } else {
                        write_i32_le(&mut buf[i * 4..], v);
                    }
                }
            }
        }
        CasacoreDataType::TpUInt => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::UInt32(v))) => *v,
                _ => 0,
            };
            if big_endian {
                write_u32_be(&mut buf, v);
            } else {
                write_u32_le(&mut buf, v);
            }
        }
        CasacoreDataType::TpFloat => {
            if nrelem == 1 {
                let v = match value {
                    Some(Value::Scalar(ScalarValue::Float32(v))) => *v,
                    _ => 0.0,
                };
                if big_endian {
                    write_f32_be(&mut buf, v);
                } else {
                    write_f32_le(&mut buf, v);
                }
            } else if let Some(Value::Array(ArrayValue::Float32(arr))) = value {
                for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                    if big_endian {
                        write_f32_be(&mut buf[i * 4..], v);
                    } else {
                        write_f32_le(&mut buf[i * 4..], v);
                    }
                }
            }
        }
        CasacoreDataType::TpDouble => {
            if nrelem == 1 {
                let v = match value {
                    Some(Value::Scalar(ScalarValue::Float64(v))) => *v,
                    _ => 0.0,
                };
                if big_endian {
                    write_f64_be(&mut buf, v);
                } else {
                    write_f64_le(&mut buf, v);
                }
            } else if let Some(Value::Array(ArrayValue::Float64(arr))) = value {
                for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                    if big_endian {
                        write_f64_be(&mut buf[i * 8..], v);
                    } else {
                        write_f64_le(&mut buf[i * 8..], v);
                    }
                }
            }
        }
        CasacoreDataType::TpInt64 => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::Int64(v))) => *v,
                _ => 0,
            };
            if big_endian {
                write_i64_be(&mut buf, v);
            } else {
                write_i64_le(&mut buf, v);
            }
        }
        CasacoreDataType::TpComplex => {
            if nrelem == 1 {
                let (re, im) = match value {
                    Some(Value::Scalar(ScalarValue::Complex32(c))) => (c.re, c.im),
                    _ => (0.0, 0.0),
                };
                if big_endian {
                    write_f32_be(&mut buf, re);
                    write_f32_be(&mut buf[4..], im);
                } else {
                    write_f32_le(&mut buf, re);
                    write_f32_le(&mut buf[4..], im);
                }
            } else if let Some(Value::Array(ArrayValue::Complex32(arr))) = value {
                for (i, c) in arr.t().iter().enumerate().take(nrelem) {
                    if big_endian {
                        write_f32_be(&mut buf[i * 8..], c.re);
                        write_f32_be(&mut buf[i * 8 + 4..], c.im);
                    } else {
                        write_f32_le(&mut buf[i * 8..], c.re);
                        write_f32_le(&mut buf[i * 8 + 4..], c.im);
                    }
                }
            }
        }
        CasacoreDataType::TpDComplex => {
            if nrelem == 1 {
                let (re, im) = match value {
                    Some(Value::Scalar(ScalarValue::Complex64(c))) => (c.re, c.im),
                    _ => (0.0, 0.0),
                };
                if big_endian {
                    write_f64_be(&mut buf, re);
                    write_f64_be(&mut buf[8..], im);
                } else {
                    write_f64_le(&mut buf, re);
                    write_f64_le(&mut buf[8..], im);
                }
            } else if let Some(Value::Array(ArrayValue::Complex64(arr))) = value {
                for (i, c) in arr.t().iter().enumerate().take(nrelem) {
                    if big_endian {
                        write_f64_be(&mut buf[i * 16..], c.re);
                        write_f64_be(&mut buf[i * 16 + 8..], c.im);
                    } else {
                        write_f64_le(&mut buf[i * 16..], c.re);
                        write_f64_le(&mut buf[i * 16 + 8..], c.im);
                    }
                }
            }
        }
        _ => {}
    }
    buf
}

/// Encode string value(s) in ISM format (C++ `ISMColumn::fromString`):
///
/// Scalar (nrelem == 1): `[u32: total_length] [string_bytes]`
///   where total_length = 4 + string.len().
///
/// Array (nrelem > 1): `[u32: total_length] [per-string: [u32: str_len] [str_data]] ...`
///   where total_length includes itself.
fn encode_string_value(
    value: Option<&casacore_types::Value>,
    nrelem: usize,
    big_endian: bool,
) -> Vec<u8> {
    use casacore_types::{ArrayValue, ScalarValue, Value};

    let strings: Vec<String> = if nrelem == 1 {
        match value {
            Some(Value::Scalar(ScalarValue::String(s))) => vec![s.clone()],
            _ => vec![String::new()],
        }
    } else {
        match value {
            Some(Value::Array(ArrayValue::String(arr))) => arr.iter().cloned().collect(),
            _ => vec![String::new(); nrelem],
        }
    };

    if nrelem == 1 {
        // Scalar: total_length = 4 + string.len() (no per-string length prefix)
        let s = &strings[0];
        let total_length = (4 + s.len()) as u32;
        let mut buf = Vec::with_capacity(total_length as usize);
        if big_endian {
            buf.extend_from_slice(&total_length.to_be_bytes());
        } else {
            buf.extend_from_slice(&total_length.to_le_bytes());
        }
        buf.extend_from_slice(s.as_bytes());
        buf
    } else {
        // Array: total_length + per-string [length] [data]
        let total_length: usize = 4 + strings.iter().map(|s| 4 + s.len()).sum::<usize>();
        let mut buf = Vec::with_capacity(total_length);
        if big_endian {
            buf.extend_from_slice(&(total_length as u32).to_be_bytes());
        } else {
            buf.extend_from_slice(&(total_length as u32).to_le_bytes());
        }
        for s in &strings {
            let len = s.len() as u32;
            if big_endian {
                buf.extend_from_slice(&len.to_be_bytes());
            } else {
                buf.extend_from_slice(&len.to_le_bytes());
            }
            buf.extend_from_slice(s.as_bytes());
        }
        buf
    }
}

/// Write an ISM data file and return the DM data blob for table.dat.
pub(crate) fn write_ism_file(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casacore_types::RecordValue],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let nrrow = rows.len();
    let ncol = col_descs.len();

    // Pre-compute column info
    let col_info: Vec<(CasacoreDataType, usize)> = col_descs
        .iter()
        .map(|c| {
            let dt = CasacoreDataType::from_primitive_type(c.require_primitive_type()?, false);
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                1
            };
            Ok((dt, nrelem))
        })
        .collect::<Result<_, StorageError>>()?;

    // Compute fixed bytes per row for bucket sizing
    let fixed_bytes_per_row: usize = col_info
        .iter()
        .map(|&(dt, nrelem)| {
            if dt == CasacoreDataType::TpBool {
                nrelem.div_ceil(8) // bit-packed
            } else {
                let elem = ism_element_size(dt);
                if elem > 0 {
                    elem * nrelem
                } else {
                    // String: estimate 12 bytes per string value
                    nrelem * 12
                }
            }
        })
        .sum();

    // Target ~32 rows per bucket, plus overhead for per-column index
    // Index overhead per row: ncol * (4 + 4) bytes worst case (row_nr + offset per entry)
    let index_overhead_per_row = ncol * 8;
    let bytes_per_row = fixed_bytes_per_row + index_overhead_per_row;
    let bucket_size = if bytes_per_row == 0 {
        128u32
    } else {
        let target = (bytes_per_row * 32 + 4) as u32; // +4 for index_offset field
        target.clamp(128, 327680)
    };

    // Build buckets with delta compression
    struct BucketBuilder {
        data: Vec<u8>,
        col_indices: Vec<(Vec<u32>, Vec<u32>)>, // (row_nrs, offsets) per column
    }

    let mut buckets: Vec<BucketBuilder> = Vec::new();
    let mut bucket_start_rows: Vec<usize> = Vec::new();
    let mut last_values: Vec<Vec<u8>> = vec![Vec::new(); ncol];

    if nrrow > 0 {
        // Start first bucket
        let mut current = BucketBuilder {
            data: Vec::new(),
            col_indices: (0..ncol).map(|_| (Vec::new(), Vec::new())).collect(),
        };
        bucket_start_rows.push(0);

        for (row_idx, row) in rows.iter().enumerate().take(nrrow) {
            let rel_row = (row_idx - *bucket_start_rows.last().unwrap()) as u32;

            // Check if adding this row would overflow the bucket
            // Estimate: current data size + worst-case new data + index size
            let mut new_data_estimate = 0usize;
            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                let (dt, nrelem) = col_info[col_idx];
                let value = row
                    .fields()
                    .iter()
                    .find(|f| f.name == col_desc.col_name)
                    .map(|f| &f.value);
                let encoded = encode_value_bytes(value, dt, nrelem, big_endian);
                if rel_row == 0 || encoded != last_values[col_idx] {
                    new_data_estimate += encoded.len();
                }
            }

            let current_index_size: usize = current
                .col_indices
                .iter()
                .map(|(rn, _)| 4 + (rn.len() + 1) * 4 + (rn.len() + 1) * 4)
                .sum();
            let total_estimate =
                4 + current.data.len() + new_data_estimate + current_index_size + ncol * 12;

            if rel_row > 0 && total_estimate > bucket_size as usize {
                // Finalize current bucket and start a new one
                buckets.push(current);
                current = BucketBuilder {
                    data: Vec::new(),
                    col_indices: (0..ncol).map(|_| (Vec::new(), Vec::new())).collect(),
                };
                bucket_start_rows.push(row_idx);
                // Carry over last values as first entries in new bucket
                for (col_idx, col_desc) in col_descs.iter().enumerate() {
                    let (dt, nrelem) = col_info[col_idx];
                    let bytes = if last_values[col_idx].is_empty() {
                        let value = row
                            .fields()
                            .iter()
                            .find(|f| f.name == col_desc.col_name)
                            .map(|f| &f.value);
                        encode_value_bytes(value, dt, nrelem, big_endian)
                    } else {
                        last_values[col_idx].clone()
                    };
                    let offset = current.data.len() as u32;
                    current.data.extend_from_slice(&bytes);
                    current.col_indices[col_idx].0.push(0); // row 0
                    current.col_indices[col_idx].1.push(offset);
                }
                // Now process the current row as rel_row 0
                // but we already wrote carried-over values, so check if current
                // row differs from carried-over
                for (col_idx, col_desc) in col_descs.iter().enumerate() {
                    let (dt, nrelem) = col_info[col_idx];
                    let value = row
                        .fields()
                        .iter()
                        .find(|f| f.name == col_desc.col_name)
                        .map(|f| &f.value);
                    let encoded = encode_value_bytes(value, dt, nrelem, big_endian);
                    // The carried-over value was already written at row 0.
                    // If current row differs from carried-over, we need to update.
                    if encoded != last_values[col_idx] {
                        let offset = current.data.len() as u32;
                        current.data.extend_from_slice(&encoded);
                        // Row 0 already has an entry from carry-over.
                        // Replace it since the actual row 0 value is different.
                        let idx = &mut current.col_indices[col_idx];
                        idx.0[0] = 0;
                        idx.1[0] = offset;
                        last_values[col_idx] = encoded;
                    }
                }
                continue;
            }

            // Add this row's data to current bucket
            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                let (dt, nrelem) = col_info[col_idx];
                let value = row
                    .fields()
                    .iter()
                    .find(|f| f.name == col_desc.col_name)
                    .map(|f| &f.value);
                let encoded = encode_value_bytes(value, dt, nrelem, big_endian);

                if rel_row == 0 || encoded != last_values[col_idx] {
                    let offset = current.data.len() as u32;
                    current.data.extend_from_slice(&encoded);
                    current.col_indices[col_idx].0.push(rel_row);
                    current.col_indices[col_idx].1.push(offset);
                    last_values[col_idx] = encoded;
                }
            }
        }
        // Finalize last bucket
        buckets.push(current);
    }

    // Serialize buckets
    let nr_buckets = buckets.len();
    let mut raw_buckets: Vec<Vec<u8>> = Vec::with_capacity(nr_buckets);

    for bucket in &buckets {
        let mut raw = vec![0u8; bucket_size as usize];

        // Write data area starting at byte 4
        let data_len = bucket.data.len().min(bucket_size as usize - 4);
        raw[4..4 + data_len].copy_from_slice(&bucket.data[..data_len]);

        // Build and write per-column index
        let mut index_area = Vec::new();
        for (row_nrs, offsets) in &bucket.col_indices {
            let n = row_nrs.len() as u32;
            if big_endian {
                index_area.extend_from_slice(&n.to_be_bytes());
                for &r in row_nrs {
                    index_area.extend_from_slice(&r.to_be_bytes());
                }
                for &o in offsets {
                    index_area.extend_from_slice(&o.to_be_bytes());
                }
            } else {
                index_area.extend_from_slice(&n.to_le_bytes());
                for &r in row_nrs {
                    index_area.extend_from_slice(&r.to_le_bytes());
                }
                for &o in offsets {
                    index_area.extend_from_slice(&o.to_le_bytes());
                }
            }
        }

        // index_offset = position where per-column index starts
        let index_start = bucket_size as usize - index_area.len();
        let index_offset = index_start as u32;

        // Write index_offset at byte 0
        if big_endian {
            raw[0..4].copy_from_slice(&index_offset.to_be_bytes());
        } else {
            raw[0..4].copy_from_slice(&index_offset.to_le_bytes());
        }

        // Write per-column index at the end of bucket
        if index_start + index_area.len() <= raw.len() {
            raw[index_start..index_start + index_area.len()].copy_from_slice(&index_area);
        }

        raw_buckets.push(raw);
    }

    // Build ISMIndex
    let mut ism_rows: Vec<u64> = Vec::with_capacity(nr_buckets + 1);
    let mut ism_bucket_nrs: Vec<u32> = Vec::with_capacity(nr_buckets);
    for (i, &start) in bucket_start_rows.iter().enumerate() {
        ism_rows.push(start as u64);
        ism_bucket_nrs.push(i as u32);
    }
    ism_rows.push(nrrow as u64); // sentinel

    let index_data = serialize_ism_index(&ism_rows, &ism_bucket_nrs, big_endian);

    // Write header
    let header_buf = serialize_ism_header(bucket_size, nr_buckets as u32, big_endian);

    // Assemble the file
    let mut file = File::create(file_path)?;
    file.write_all(&header_buf)?;
    for raw in &raw_buckets {
        file.write_all(raw)?;
    }
    file.write_all(&index_data)?;

    // Generate the DM data blob
    let dm_blob = serialize_ism_dm_blob("ISM")?;

    Ok(dm_blob)
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

fn serialize_ism_header(bucket_size: u32, nr_buckets: u32, big_endian: bool) -> Vec<u8> {
    let io_order = if big_endian {
        ByteOrder::BigEndian
    } else {
        ByteOrder::LittleEndian
    };
    let mut buf = AipsIoWriteBuf::new(io_order);

    // v5 for LE tables (includes endian flag), v4 for BE tables
    let version = if big_endian { 4 } else { 5 };
    buf.putstart("IncrementalStMan", version);

    if version >= 5 {
        buf.put_bool(big_endian);
    }

    buf.put_u32(bucket_size); // bucketSize
    buf.put_u32(nr_buckets); // nbucketInit
    buf.put_u32(1); // persCacheSize
    buf.put_u32(0); // uniqnr
    buf.put_u32(0); // nFreeBucket
    buf.put_i32(-1); // firstFreeBucket
    buf.putend();

    let mut header = buf.into_bytes();
    header.resize(ISM_HEADER_SIZE as usize, 0);
    header
}

fn serialize_ism_index(rows: &[u64], bucket_nrs: &[u32], big_endian: bool) -> Vec<u8> {
    let io_order = if big_endian {
        ByteOrder::BigEndian
    } else {
        ByteOrder::LittleEndian
    };
    let mut buf = AipsIoWriteBuf::new(io_order);
    let nused = bucket_nrs.len() as u32;

    // Use version 2 (64-bit row numbers) for safety
    buf.putstart("ISMIndex", 2);
    buf.put_u32(nused);

    // rows: Block<Int64> (version 2)
    buf.putstart_nested("Block", 1);
    buf.put_u32(rows.len() as u32);
    for &r in rows {
        buf.put_u64(r);
    }
    buf.putend_nested();

    // bucketNr: Block<uInt>
    buf.putstart_nested("Block", 1);
    buf.put_u32(bucket_nrs.len() as u32);
    for &b in bucket_nrs {
        buf.put_u32(b);
    }
    buf.putend_nested();

    buf.putend();
    buf.into_bytes()
}

fn serialize_ism_dm_blob(name: &str) -> Result<Vec<u8>, StorageError> {
    let mut io = AipsIo::new_write_only(std::io::Cursor::new(Vec::new()));
    io.putstart("ISM", 3)?;
    io.put_string(name)?;
    io.putend()?;
    let cursor: std::io::Cursor<Vec<u8>> = io.into_inner_typed()?;
    Ok(cursor.into_inner())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ism_element_sizes() {
        assert_eq!(ism_element_size(CasacoreDataType::TpBool), 1);
        assert_eq!(ism_element_size(CasacoreDataType::TpUChar), 1);
        assert_eq!(ism_element_size(CasacoreDataType::TpShort), 2);
        assert_eq!(ism_element_size(CasacoreDataType::TpInt), 4);
        assert_eq!(ism_element_size(CasacoreDataType::TpFloat), 4);
        assert_eq!(ism_element_size(CasacoreDataType::TpDouble), 8);
        assert_eq!(ism_element_size(CasacoreDataType::TpInt64), 8);
        assert_eq!(ism_element_size(CasacoreDataType::TpComplex), 8);
        assert_eq!(ism_element_size(CasacoreDataType::TpDComplex), 16);
        assert_eq!(ism_element_size(CasacoreDataType::TpString), 0);
    }

    #[test]
    fn get_interval_basic() {
        let col_idx = IsmBucketColIndex {
            n_values: 3,
            row_nrs: vec![0, 3, 7],
            offsets: vec![0, 4, 8],
        };
        // Row 0 → interval 0
        assert_eq!(get_interval(&col_idx, 0), 0);
        // Row 2 → interval 0 (still in [0, 3))
        assert_eq!(get_interval(&col_idx, 2), 0);
        // Row 3 → interval 1 (at boundary)
        assert_eq!(get_interval(&col_idx, 3), 1);
        // Row 5 → interval 1 (in [3, 7))
        assert_eq!(get_interval(&col_idx, 5), 1);
        // Row 7 → interval 2 (at boundary)
        assert_eq!(get_interval(&col_idx, 7), 2);
        // Row 10 → interval 2 (past last, stays at last)
        assert_eq!(get_interval(&col_idx, 10), 2);
    }

    #[test]
    fn get_interval_single_entry() {
        let col_idx = IsmBucketColIndex {
            n_values: 1,
            row_nrs: vec![0],
            offsets: vec![0],
        };
        assert_eq!(get_interval(&col_idx, 0), 0);
        assert_eq!(get_interval(&col_idx, 5), 0);
    }

    #[test]
    fn parse_header_be() {
        // Synthesize a BE header
        let io_order = ByteOrder::BigEndian;
        let mut buf = AipsIoWriteBuf::new(io_order);
        buf.putstart("IncrementalStMan", 4);
        buf.put_u32(32768); // bucket_size
        buf.put_u32(10); // nr_buckets
        buf.put_u32(1); // persCacheSize
        buf.put_u32(0); // uniqnr
        buf.put_u32(0); // nFreeBucket
        buf.put_i32(-1); // firstFreeBucket
        buf.putend();
        let mut header = buf.into_bytes();
        header.resize(512, 0);

        // Parse it
        let mut cursor = std::io::Cursor::new(header);
        let hdr = parse_ism_header(&mut cursor).unwrap();
        assert_eq!(hdr.bucket_size, 32768);
        assert_eq!(hdr.nr_buckets, 10);
        assert!(hdr.big_endian);
    }

    #[test]
    fn parse_header_le() {
        let io_order = ByteOrder::LittleEndian;
        let mut buf = AipsIoWriteBuf::new(io_order);
        buf.putstart("IncrementalStMan", 5);
        buf.put_bool(false); // LE
        buf.put_u32(16384); // bucket_size
        buf.put_u32(5); // nr_buckets
        buf.put_u32(1); // persCacheSize
        buf.put_u32(0); // uniqnr
        buf.put_u32(0); // nFreeBucket
        buf.put_i32(-1); // firstFreeBucket
        buf.putend();
        let mut header = buf.into_bytes();
        header.resize(512, 0);

        let mut cursor = std::io::Cursor::new(header);
        let hdr = parse_ism_header(&mut cursor).unwrap();
        assert_eq!(hdr.bucket_size, 16384);
        assert_eq!(hdr.nr_buckets, 5);
        assert!(!hdr.big_endian);
    }

    #[test]
    fn string_encoding_round_trip() {
        for big_endian in [true, false] {
            let val = casacore_types::Value::Scalar(casacore_types::ScalarValue::String(
                "hello".to_string(),
            ));
            let encoded = encode_string_value(Some(&val), 1, big_endian);
            let decoded = read_ism_string(&encoded, 1, big_endian).unwrap();
            assert_eq!(decoded, vec!["hello"]);
        }
    }

    #[test]
    fn string_encoding_empty() {
        for big_endian in [true, false] {
            let val =
                casacore_types::Value::Scalar(casacore_types::ScalarValue::String(String::new()));
            let encoded = encode_string_value(Some(&val), 1, big_endian);
            let decoded = read_ism_string(&encoded, 1, big_endian).unwrap();
            assert_eq!(decoded, vec![""]);
        }
    }

    #[test]
    fn bool_encoding_round_trip() {
        for big_endian in [true, false] {
            let val_true = casacore_types::Value::Scalar(casacore_types::ScalarValue::Bool(true));
            let val_false = casacore_types::Value::Scalar(casacore_types::ScalarValue::Bool(false));
            let enc_true =
                encode_value_bytes(Some(&val_true), CasacoreDataType::TpBool, 1, big_endian);
            let enc_false =
                encode_value_bytes(Some(&val_false), CasacoreDataType::TpBool, 1, big_endian);
            assert_eq!(enc_true.len(), 1);
            assert_eq!(enc_false.len(), 1);
            assert_eq!(enc_true[0], 1);
            assert_eq!(enc_false[0], 0);
        }
    }

    #[test]
    fn bucket_round_trip() {
        // Build a bucket for 5 rows of i32 column: [1, 1, 1, 2, 2]
        let big_endian = true;
        let bucket_size = 256u32;
        let ncol = 1;

        // Data area: value 1 at offset 0, value 2 at offset 4
        let mut data_area = vec![0u8; 8];
        write_i32_be(&mut data_area[0..], 1);
        write_i32_be(&mut data_area[4..], 2);

        // Per-column index: 2 entries
        // row_nrs = [0, 3], offsets = [0, 4]
        let n_values = 2u32;
        let mut index_area = Vec::new();
        index_area.extend_from_slice(&n_values.to_be_bytes());
        index_area.extend_from_slice(&0u32.to_be_bytes()); // row_nr 0
        index_area.extend_from_slice(&3u32.to_be_bytes()); // row_nr 3
        index_area.extend_from_slice(&0u32.to_be_bytes()); // offset 0
        index_area.extend_from_slice(&4u32.to_be_bytes()); // offset 4

        let mut raw = vec![0u8; bucket_size as usize];
        let index_start = bucket_size as usize - index_area.len();

        // Write index_offset at byte 0
        raw[0..4].copy_from_slice(&(index_start as u32).to_be_bytes());
        // Write data area at byte 4
        raw[4..4 + data_area.len()].copy_from_slice(&data_area);
        // Write index at end
        raw[index_start..index_start + index_area.len()].copy_from_slice(&index_area);

        // Parse the bucket
        let bucket = parse_ism_bucket(&raw, ncol, big_endian).unwrap();
        assert_eq!(bucket.col_indices.len(), 1);
        assert_eq!(bucket.col_indices[0].n_values, 2);
        assert_eq!(bucket.col_indices[0].row_nrs, vec![0, 3]);
        assert_eq!(bucket.col_indices[0].offsets, vec![0, 4]);

        // Verify all 5 rows read back correctly
        for rel_row in 0..5u32 {
            let k = get_interval(&bucket.col_indices[0], rel_row);
            let offset = bucket.col_indices[0].offsets[k] as usize;
            let val =
                read_scalar_at(&bucket.data, offset, CasacoreDataType::TpInt, big_endian).unwrap();
            let expected = if rel_row < 3 { 1 } else { 2 };
            assert_eq!(
                val,
                casacore_types::Value::Scalar(casacore_types::ScalarValue::Int32(expected)),
                "row {rel_row}"
            );
        }
    }
}
