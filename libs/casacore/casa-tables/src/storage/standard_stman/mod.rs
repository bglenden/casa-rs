// SPDX-License-Identifier: LGPL-3.0-or-later
//! StandardStMan (SSM) bucket-based storage manager reader/writer.
//!
//! On-disk format: `table.fN` file with 512-byte AipsIO header at offset 0,
//! followed by fixed-size buckets at offset 512+.
//!
//! The SSM file header and index data use in-memory AipsIO whose byte order
//! matches the table's endian setting (BE for BE tables, LE for LE tables).
//! The DM data blob from `table.dat` always uses big-endian AipsIO
//! (file-based). Bucket column data byte order is indicated by the
//! `big_endian` flag in the SSM header. String bucket metadata (freeLink,
//! usedLength, nDeleted, nextBucket) and index bucket chain pointers always
//! use canonical (big-endian) encoding regardless of the table's endian
//! setting.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use casa_aipsio::internal::{AipsIoBufferWriter, AipsIoSliceReader, detect_aipsio_byte_order};
use casa_aipsio::{AipsIo, ByteOrder};

use casa_core::{ArrayValue, ScalarValue, Value};

use super::canonical::{
    canonical_element_size, read_bool_bits, read_f32_be, read_f32_le, read_f32_slice_be,
    read_f32_slice_le, read_f64_be, read_f64_le, read_f64_slice_be, read_f64_slice_le,
    read_i16_slice_be, read_i16_slice_le, read_i32_be, read_i32_slice_be, read_i32_slice_le,
    read_i64_slice_be, read_i64_slice_le, read_u16_slice_be, read_u16_slice_le, read_u32_slice_be,
    read_u32_slice_le, write_bool_bits, write_i32_be, write_i32_le, write_i64_be, write_i64_le,
};
use super::cell_elements::{cell_bools, encode_fixed_cell};
use super::data_file::{DataFile, DataRead};
use super::data_type::CasacoreDataType;
use super::scalar_sources::SCALAR_BLOCK_CELLS;
use super::stman_aipsio::{ColumnRawData, extract_row_value};
use super::stman_array_file::{StManArrayFileReader, StManArrayFileWriter, stored_shape};
use super::table_control::ColumnDescContents;
use super::{ColumnSources, RequiredScalarColumnData, StorageError};

mod array_updates;
mod bucket_reader;
use bucket_reader::{LastBucket, ReadBuckets};
mod append;
mod column_writer;
pub(crate) use append::{StagedRecords, stage_records};
pub(crate) use array_updates::save_ssm_array_rows_in_place;
mod index;
mod replace_ints;
pub(in crate::storage) use replace_ints::{replace_int32_columns, stage_int32_replacement};
mod string_arrays;
mod string_buckets;
mod string_read;

use string_buckets::StringBuckets;
use string_read::{
    read_fixed_ssm_string, read_ssm_string, read_ssm_string_array_value, read_ssm_string_bytes,
};

pub(super) const SSM_HEADER_SIZE: u64 = 512;
pub(super) const DEFAULT_SSM_BUCKET_SIZE: u32 = 32768;

/// File offset of bucket `bucket` of a file of `bucket_bytes`-byte buckets.
fn bucket_offset(bucket: usize, bucket_bytes: usize) -> u64 {
    SSM_HEADER_SIZE + (bucket * bucket_bytes) as u64
}

type SparseScalarCell = (usize, Option<ScalarValue>);
type SparseScalarColumn = Option<Vec<SparseScalarCell>>;

fn packed_ssm_column_bytes(col_sizes_bits: &[usize], rows: usize) -> usize {
    col_sizes_bits
        .iter()
        .map(|&bits| (rows * bits).div_ceil(8))
        .sum()
}

mod read;
mod write;

pub(crate) use read::read_ssm_file_columns;
pub(crate) use read::read_ssm_required_scalar_columns_rows;
pub(crate) use read::read_ssm_required_scalar_columns_rows_reusing;
pub(crate) use read::read_ssm_scalar_column_rows;
use read::*;
pub(crate) use write::rewrite_ssm_columns;
pub(crate) use write::write_ssm_file_column_sources;
pub(crate) use write::write_ssm_file_indexed;
pub(crate) use write::write_ssm_file_scalar_columns;
pub(super) use write::write_ssm_with_bucket;
use write::*;

/// Choose casacore-compatible SSM geometry for a newly created table.
///
/// Casacore's zero-argument `StandardStMan` deliberately defaults to 32 rows
/// per bucket and documents that choice as suitable only for small tables. A
/// positive bucket-size argument instead fills that bucket with as many rows
/// as fit. casa-rs knows the creation row count, so large new tables use the
/// latter policy while small tables retain the casacore default geometry.
pub(super) fn new_ssm_bucket_layout_with_policy(
    col_sizes_bits: &[usize],
    row_count: usize,
    casacore_default_small: bool,
) -> (u32, u32) {
    if casacore_default_small {
        let data_size = packed_ssm_column_bytes(col_sizes_bits, 32);
        return (32, data_size.max(128) as u32);
    }
    if col_sizes_bits.iter().all(|bits| *bits == 0) {
        return (32, DEFAULT_SSM_BUCKET_SIZE);
    }
    if row_count <= 32 {
        let data_size = packed_ssm_column_bytes(col_sizes_bits, 32);
        return (32, (data_size.max(128) as u32).max(DEFAULT_SSM_BUCKET_SIZE));
    }

    let bucket_size = DEFAULT_SSM_BUCKET_SIZE as usize;
    let full_bytes_per_row = col_sizes_bits.iter().map(|bits| bits / 8).sum::<usize>();
    let mut rows = bucket_size
        .checked_div(full_bytes_per_row)
        .unwrap_or_else(|| bucket_size * 8 / col_sizes_bits.iter().sum::<usize>().max(1))
        .max(1);
    while rows > 1 && packed_ssm_column_bytes(col_sizes_bits, rows) > bucket_size {
        rows -= 1;
    }
    while packed_ssm_column_bytes(col_sizes_bits, rows + 1) <= bucket_size {
        rows += 1;
    }
    let data_size = packed_ssm_column_bytes(col_sizes_bits, rows);
    if rows == 1 && data_size > bucket_size {
        (1, data_size.max(128) as u32)
    } else {
        (rows as u32, DEFAULT_SSM_BUCKET_SIZE)
    }
}

fn new_ssm_bucket_layout(col_sizes_bits: &[usize], row_count: usize) -> (u32, u32) {
    new_ssm_bucket_layout_with_policy(col_sizes_bits, row_count, false)
}

#[cfg(test)]
mod bucket_layout_tests {
    use super::*;

    #[test]
    fn small_tables_keep_casacore_default_rows_per_bucket() {
        assert_eq!(new_ssm_bucket_layout(&[32, 32, 32, 1], 32), (32, 32_768));
    }

    #[test]
    fn large_scalar_tables_fill_the_standard_bucket() {
        let (rows, bytes) = new_ssm_bucket_layout(&[32, 32, 32, 1], 1_000_000);
        assert_eq!(bytes, 32_768);
        assert!(rows > 2_500, "rows={rows}");
        assert!(packed_ssm_column_bytes(&[32, 32, 32, 1], rows as usize) <= bytes as usize);
        assert!(packed_ssm_column_bytes(&[32, 32, 32, 1], rows as usize + 1) > bytes as usize);
    }
}

/// SSM column data — either flat (scalar/fixed-shape) or indirect (variable-shape).
#[derive(Debug)]
pub(crate) enum SsmColumnResult {
    Flat(ColumnRawData),
    Indirect(Vec<Option<Value>>),
}

fn is_ssm_variable_string(col_desc: &ColumnDescContents) -> bool {
    col_desc.data_type == CasacoreDataType::TpString && col_desc.max_length == 0
}

fn is_ssm_fixed_string(col_desc: &ColumnDescContents) -> bool {
    col_desc.data_type == CasacoreDataType::TpString && col_desc.max_length > 0
}

fn is_ssm_array_file_indirect(col_desc: &ColumnDescContents) -> bool {
    col_desc.is_array && (col_desc.option & 1) == 0 && !is_ssm_variable_string(col_desc)
}

fn is_ssm_indirect(col_desc: &ColumnDescContents) -> bool {
    col_desc.is_record() || is_ssm_array_file_indirect(col_desc)
}

fn ssm_cell_element_count(col_desc: &ColumnDescContents) -> usize {
    if col_desc.is_array && !col_desc.shape.is_empty() {
        col_desc.shape.iter().map(|&s| s as usize).product()
    } else {
        1
    }
}

// ---------------------------------------------------------------------------
// Byte-order-aware buffer reader for in-memory AipsIO parsing
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Parsed types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SsmHeader {
    bucket_size: u32,
    nr_buckets: u32,
    pers_cache_size: u32,
    #[allow(dead_code)]
    nr_free_buckets: u32,
    #[allow(dead_code)]
    first_free_bucket: i32,
    nr_idx_buckets: u32,
    first_idx_bucket: i32,
    idx_bucket_offset: u32,
    #[allow(dead_code)]
    last_string_bucket: i32,
    index_length: u32,
    nr_indices: u32,
    big_endian: bool,
    /// Byte order used by in-memory AipsIO (header + indices).
    io_order: ByteOrder,
}

#[derive(Debug, Clone)]
struct SsmDmInfo {
    name: String,
    column_offsets: Vec<u32>,
    col_index_map: Vec<u32>,
}

#[derive(Debug, Clone)]
struct SsmIndex {
    #[allow(dead_code)]
    n_used: u32,
    rows_per_bucket: u32,
    last_row: Vec<u64>,
    bucket_number: Vec<u32>,
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

fn parse_ssm_header(file: &mut impl DataRead) -> Result<SsmHeader, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = vec![0u8; SSM_HEADER_SIZE as usize];
    file.read_exact(&mut header_buf)?;

    let io_order = detect_aipsio_byte_order(&header_buf)?;
    let mut buf = AipsIoSliceReader::new(&header_buf, io_order)?;
    let version = buf.getstart("StandardStMan")?;

    let big_endian = if version >= 3 {
        buf.read_bool()?
    } else {
        true // versions 1-2 are always big-endian
    };

    let bucket_size = buf.read_u32()?;
    let nr_buckets = buf.read_u32()?;
    let pers_cache_size = buf.read_u32()?;
    let nr_free_buckets = buf.read_u32()?;
    let first_free_bucket = buf.read_i32()?;
    let nr_idx_buckets = buf.read_u32()?;
    let first_idx_bucket = buf.read_i32()?;

    let idx_bucket_offset = if version >= 2 { buf.read_u32()? } else { 0 };

    let last_string_bucket = buf.read_i32()?;
    let index_length = buf.read_u32()?;
    let nr_indices = buf.read_u32()?;
    buf.getend()?;

    Ok(SsmHeader {
        bucket_size,
        nr_buckets,
        pers_cache_size,
        nr_free_buckets,
        first_free_bucket,
        nr_idx_buckets,
        first_idx_bucket,
        idx_bucket_offset,
        last_string_bucket,
        index_length,
        nr_indices,
        big_endian,
        io_order,
    })
}

// ---------------------------------------------------------------------------
// DM blob parsing ("SSM" v2 from table.dat ColumnSet section)
// ---------------------------------------------------------------------------

/// Parse the DM data blob from table.dat. This blob is written within the
/// file-based AipsIO stream (always big-endian canonical encoding).
///
/// C++ casacore serialises the arrays via `putBlock`/`getBlock` which wraps
/// each array in a nested AipsIO `"Block"` object.
fn parse_ssm_dm_blob(data: &[u8]) -> Result<SsmDmInfo, StorageError> {
    let cursor = std::io::Cursor::new(data.to_vec());
    let mut io = AipsIo::new_read_only(cursor);
    let _version = io.getstart("SSM")?;

    let name = io.get_string()?;

    let column_offsets = read_block_u32(&mut io)?;
    let col_index_map = read_block_u32(&mut io)?;

    io.getend()?;

    Ok(SsmDmInfo {
        name,
        column_offsets,
        col_index_map,
    })
}

/// The manager's name (`DataManager::dataManagerName()`), from its blob in
/// `table.dat`; `None` for a blob that does not parse.
pub(crate) fn dm_name_from_blob(data: &[u8]) -> Option<String> {
    parse_ssm_dm_blob(data).ok().map(|info| info.name)
}

/// Whether an existing on-disk `StandardStMan` file uses smaller buckets than
/// casa-rs's 32 KiB default (such as casacore's default 32-row layout).
pub(crate) fn ssm_has_small_buckets(data_path: &Path) -> Result<bool, StorageError> {
    let mut file = DataFile::open(data_path)?;
    let header = parse_ssm_header(&mut file)?;
    Ok(header.bucket_size < DEFAULT_SSM_BUCKET_SIZE)
}

/// The `SPEC` record of a `StandardStMan` manager (`SSMBase::dataManagerSpec`).
pub(crate) fn ssm_spec(
    table_path: &Path,
    dm_seq_nr: u32,
) -> Result<casa_core::RecordValue, StorageError> {
    let data_path = table_path.join(format!("{}{dm_seq_nr}", super::TABLE_DATA_FILE_PREFIX));
    let mut file = DataFile::open(&data_path)?;
    let header = parse_ssm_header(&mut file)?;
    let bucket_size = i32::try_from(header.bucket_size)
        .map_err(|_| StorageError::FormatMismatch("SSM bucket size exceeds i32".into()))?;
    let index_length = i32::try_from(header.index_length)
        .map_err(|_| StorageError::FormatMismatch("SSM index length exceeds i32".into()))?;
    let pers_cache_size = i32::try_from(header.pers_cache_size)
        .map_err(|_| StorageError::FormatMismatch("SSM cache size exceeds i32".into()))?;
    Ok(casa_core::RecordValue::new(vec![
        casa_core::RecordField::new("BUCKETSIZE", Value::Scalar(ScalarValue::Int32(bucket_size))),
        casa_core::RecordField::new(
            "IndexLength",
            Value::Scalar(ScalarValue::Int32(index_length)),
        ),
        casa_core::RecordField::new(
            "MaxCacheSize",
            Value::Scalar(ScalarValue::Int32(pers_cache_size)),
        ),
        casa_core::RecordField::new(
            "PERSCACHESIZE",
            Value::Scalar(ScalarValue::Int32(pers_cache_size)),
        ),
    ]))
}

/// Read a `Block<uInt>` as serialised by C++ casacore's `putBlock`.
fn read_block_u32(io: &mut AipsIo) -> Result<Vec<u32>, StorageError> {
    let _version = io.getstart("Block")?;
    let count = io.get_u32()?;
    let mut values = vec![0u32; count as usize];
    for v in &mut values {
        *v = io.get_u32()?;
    }
    io.getend()?;
    Ok(values)
}

// ---------------------------------------------------------------------------
// Index parsing
// ---------------------------------------------------------------------------

fn read_bucket(
    file: &mut impl DataRead,
    header: &SsmHeader,
    bucket_nr: u32,
) -> Result<Vec<u8>, StorageError> {
    let offset = SSM_HEADER_SIZE + (bucket_nr as u64) * (header.bucket_size as u64);
    let file_len = file.data_len()?;
    if offset + header.bucket_size as u64 > file_len {
        return Err(StorageError::FormatMismatch(format!(
            "SSM bucket {bucket_nr} out of range: offset={offset}, bucket_size={}, file_len={file_len}",
            header.bucket_size
        )));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; header.bucket_size as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn parse_ssm_indices(
    file: &mut impl DataRead,
    header: &SsmHeader,
) -> Result<Vec<SsmIndex>, StorageError> {
    if header.nr_indices == 0 || header.first_idx_bucket < 0 {
        return Ok(Vec::new());
    }

    // Assemble index data from index bucket(s)
    let mut index_data = Vec::with_capacity(header.index_length as usize);

    if header.nr_idx_buckets <= 1 && header.idx_bucket_offset > 0 {
        // Index fits in part of one bucket
        let bucket = read_bucket(file, header, header.first_idx_bucket as u32)?;
        let start = header.idx_bucket_offset as usize;
        let end = start + header.index_length as usize;
        if end <= bucket.len() {
            index_data.extend_from_slice(&bucket[start..end]);
        } else {
            return Err(StorageError::FormatMismatch(
                "SSM index data exceeds bucket bounds".to_string(),
            ));
        }
    } else {
        // Index spans one or more full buckets.
        // Each index bucket has an 8-byte header: 2 × canonical Int
        //   bytes 0-3: check number (canonical/big-endian)
        //   bytes 4-7: next bucket number (canonical/big-endian)
        // Note: chain pointers use CanonicalConversion (always big-endian),
        // independent of the SSM's big_endian flag.
        let chain_overhead = 8usize;
        let data_per_bucket = header.bucket_size as usize - chain_overhead;
        let mut bucket_nr = header.first_idx_bucket as u32;
        let mut remaining = header.index_length as usize;

        for _ in 0..header.nr_idx_buckets {
            if remaining == 0 {
                break;
            }
            let bucket = read_bucket(file, header, bucket_nr)?;
            let chunk = remaining.min(data_per_bucket);
            index_data.extend_from_slice(&bucket[chain_overhead..chain_overhead + chunk]);
            remaining = remaining.saturating_sub(data_per_bucket);

            if remaining > 0 {
                // Next bucket from chain pointer (always canonical/big-endian)
                bucket_nr = read_i32_be(&bucket[4..8]) as u32;
            }
        }

        if remaining > 0 {
            return Err(StorageError::FormatMismatch(
                "SSM index data incomplete after following bucket chain".to_string(),
            ));
        }
    }

    // Parse index data using the same byte order as the header (in-memory AipsIO).
    // Each SSMIndex is a top-level AipsIO object (level 0 → prefixed with magic).
    let mut buf = AipsIoSliceReader::new(&index_data, header.io_order)?;

    let mut indices = Vec::with_capacity(header.nr_indices as usize);
    for _ in 0..header.nr_indices {
        let version = buf.getstart("SSMIndex")?;

        let n_used = buf.read_u32()?;
        let rows_per_bucket = buf.read_u32()?;
        let _nr_columns = buf.read_i32()?;

        // Free space map: nested "SimpleOrderedMap" AipsIO object
        {
            let _map_version = buf.getstart("SimpleOrderedMap")?;
            let _default_val = buf.read_i32()?;
            let nr = buf.read_u32()?;
            let _incr = buf.read_u32()?;
            for _ in 0..nr {
                let _key = buf.read_i32()?;
                let _val = buf.read_i32()?;
            }
            buf.getend()?;
        }

        // Last row numbers: Block<uInt> (v1) or Block<Int64> (v2)
        let last_row = if version == 1 {
            buf.read_block_u32()?
                .into_iter()
                .map(|v| v as u64)
                .collect()
        } else {
            buf.read_block_u64()?
        };

        // Bucket numbers: Block<uInt>
        let bucket_number = buf.read_block_u32()?;

        buf.getend()?;

        indices.push(SsmIndex {
            n_used,
            rows_per_bucket,
            last_row,
            bucket_number,
        });
    }

    Ok(indices)
}

impl SsmIndex {
    /// Find the bucket containing `row_nr`.
    /// Returns (bucket_nr, start_row, end_row) where the data bucket contains
    /// rows [start_row..=end_row].
    fn find_bucket(&self, row_nr: u64) -> Option<(u32, u64, u64)> {
        let pos = self.last_row.partition_point(|&lr| lr < row_nr);
        if pos >= self.last_row.len() {
            return None;
        }
        let bucket_nr = self.bucket_number[pos];
        let start_row = if pos == 0 {
            0
        } else {
            self.last_row[pos - 1] + 1
        };
        let end_row = self.last_row[pos];
        Some((bucket_nr, start_row, end_row))
    }
}

// ---------------------------------------------------------------------------
// Canonical byte reading for bucket data (respects big_endian flag)
// ---------------------------------------------------------------------------

fn read_i32_canonical(src: &[u8], big_endian: bool) -> i32 {
    if big_endian {
        read_i32_be(src)
    } else {
        i32::from_le_bytes([src[0], src[1], src[2], src[3]])
    }
}

fn extract_scalar_from_ssm_bytes(
    bytes: &[u8],
    data_type: CasacoreDataType,
    big_endian: bool,
) -> Result<Value, StorageError> {
    let value = match data_type {
        CasacoreDataType::TpUChar => Value::Scalar(ScalarValue::UInt8(bytes[0])),
        CasacoreDataType::TpShort => Value::Scalar(ScalarValue::Int16(if big_endian {
            i16::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM i16 scalar width".to_string())
            })?)
        } else {
            i16::from_le_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM i16 scalar width".to_string())
            })?)
        })),
        CasacoreDataType::TpUShort => Value::Scalar(ScalarValue::UInt16(if big_endian {
            u16::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM u16 scalar width".to_string())
            })?)
        } else {
            u16::from_le_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM u16 scalar width".to_string())
            })?)
        })),
        CasacoreDataType::TpInt => Value::Scalar(ScalarValue::Int32(if big_endian {
            read_i32_be(bytes)
        } else {
            i32::from_le_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM i32 scalar width".to_string())
            })?)
        })),
        CasacoreDataType::TpUInt => Value::Scalar(ScalarValue::UInt32(if big_endian {
            u32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM u32 scalar width".to_string())
            })?)
        } else {
            u32::from_le_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM u32 scalar width".to_string())
            })?)
        })),
        CasacoreDataType::TpFloat => Value::Scalar(ScalarValue::Float32(if big_endian {
            read_f32_be(bytes)
        } else {
            read_f32_le(bytes)
        })),
        CasacoreDataType::TpDouble => Value::Scalar(ScalarValue::Float64(if big_endian {
            read_f64_be(bytes)
        } else {
            read_f64_le(bytes)
        })),
        CasacoreDataType::TpInt64 => Value::Scalar(ScalarValue::Int64(if big_endian {
            i64::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM i64 scalar width".to_string())
            })?)
        } else {
            i64::from_le_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid SSM i64 scalar width".to_string())
            })?)
        })),
        CasacoreDataType::TpComplex => {
            if bytes.len() != 8 {
                return Err(StorageError::FormatMismatch(
                    "invalid SSM complex32 scalar width".to_string(),
                ));
            }
            let re = if big_endian {
                read_f32_be(&bytes[..4])
            } else {
                read_f32_le(&bytes[..4])
            };
            let im = if big_endian {
                read_f32_be(&bytes[4..8])
            } else {
                read_f32_le(&bytes[4..8])
            };
            Value::Scalar(ScalarValue::Complex32(casa_core::Complex32::new(re, im)))
        }
        CasacoreDataType::TpDComplex => {
            if bytes.len() != 16 {
                return Err(StorageError::FormatMismatch(
                    "invalid SSM complex64 scalar width".to_string(),
                ));
            }
            let re = if big_endian {
                read_f64_be(&bytes[..8])
            } else {
                read_f64_le(&bytes[..8])
            };
            let im = if big_endian {
                read_f64_be(&bytes[8..16])
            } else {
                read_f64_le(&bytes[8..16])
            };
            Value::Scalar(ScalarValue::Complex64(casa_core::Complex64::new(re, im)))
        }
        other => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported SSM scalar type {other:?}"
            )));
        }
    };
    Ok(value)
}

// ---------------------------------------------------------------------------
// Column data reader
// ---------------------------------------------------------------------------

pub(crate) fn read_ssm_file(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    nrrow: usize,
) -> Result<Vec<(String, SsmColumnResult)>, StorageError> {
    let columns = col_descs.iter().copied().enumerate().collect::<Vec<_>>();
    read_ssm_file_columns(file_path, dm_blob, &columns, nrrow)
}

fn scalar_slice_as_bytes_mut<T>(values: &mut [T]) -> &mut [u8] {
    unsafe {
        std::slice::from_raw_parts_mut(
            values.as_mut_ptr().cast::<u8>(),
            std::mem::size_of_val(values),
        )
    }
}

/// Patch fixed-width scalar cells without rebuilding the StandardStMan group.
///
/// The persisted SSM header, index, bucket geometry, and data-manager blob are
/// authoritative. Only data buckets containing requested cells are read,
/// modified, and written back, so a bounded selected-row mutation never
/// materializes or rewrites the complete scalar group.
pub(crate) fn save_ssm_file_scalar_columns_sparse_rows_in_place(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[ColumnDescContents],
    sparse_columns: &[SparseScalarColumn],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    if sparse_columns.len() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "SSM sparse scalar column count mismatch: expected {}, got {}",
            col_descs.len(),
            sparse_columns.len()
        )));
    }
    if changed_rows.is_empty()
        || sparse_columns.iter().all(Option::is_none)
        || col_descs
            .iter()
            .zip(sparse_columns)
            .any(|(column, patches)| {
                patches.is_some()
                    && (column.is_array
                        || column.is_record()
                        || column.data_type == CasacoreDataType::TpString)
            })
    {
        return Ok(false);
    }

    let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
    let header = parse_ssm_header(&mut file)?;
    if header.big_endian != big_endian {
        return Err(StorageError::FormatMismatch(format!(
            "SSM endian mismatch for sparse save: file={}, requested={}",
            header.big_endian, big_endian
        )));
    }
    let dm_info = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;
    if dm_info.column_offsets.len() < col_descs.len()
        || dm_info.col_index_map.len() < col_descs.len()
    {
        return Err(StorageError::FormatMismatch(
            "SSM sparse scalar save is missing column metadata".to_string(),
        ));
    }

    type BucketMutation = (usize, usize, Option<ScalarValue>);
    let mut mutations_by_bucket: BTreeMap<u32, Vec<BucketMutation>> = BTreeMap::new();
    for (col_idx, values) in sparse_columns.iter().enumerate() {
        let Some(values) = values else {
            continue;
        };
        let index_nr = dm_info.col_index_map[col_idx] as usize;
        let index = indices.get(index_nr).ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "SSM column '{}' references index {index_nr} but only {} indices exist",
                col_descs[col_idx].col_name,
                indices.len()
            ))
        })?;
        for (row_index, value) in values {
            let (bucket_nr, start_row, _) =
                index.find_bucket(*row_index as u64).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "SSM index has no bucket for row {row_index}"
                    ))
                })?;
            let row_in_bucket = (*row_index as u64 - start_row) as usize;
            mutations_by_bucket.entry(bucket_nr).or_default().push((
                col_idx,
                row_in_bucket,
                value.clone(),
            ));
        }
    }
    if mutations_by_bucket.is_empty() {
        return Ok(false);
    }

    for (bucket_nr, mutations) in mutations_by_bucket {
        let mut bucket = read_bucket(&mut file, &header, bucket_nr)?;
        for (col_idx, row_in_bucket, scalar) in mutations {
            let value = scalar.map(Value::Scalar);
            write_cell_to_bucket(
                &mut bucket,
                dm_info.column_offsets[col_idx] as usize,
                row_in_bucket,
                &col_descs[col_idx],
                col_descs[col_idx].data_type,
                1,
                value.as_ref(),
                &mut no_string_buckets,
                header.nr_buckets as usize,
                big_endian,
            )?;
        }
        let offset = SSM_HEADER_SIZE + u64::from(bucket_nr) * u64::from(header.bucket_size);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&bucket)?;
    }
    Ok(true)
}

mod array_rows;
mod fixed_arrays;
pub(crate) use array_rows::{read_ssm_array_column_rows, read_ssm_array_column_shapes};
pub(crate) use fixed_arrays::read_fixed_2d_cells;

// ===========================================================================
// SSM Write Path
// ===========================================================================

/// Write a StandardStMan data file and return the DM data blob for table.dat.
///
/// The file uses little-endian byte order (SSM v3) matching modern machines.
/// Returns the "SSM" AipsIO data blob to be stored in the DataManagerEntry.
pub(crate) fn write_ssm_file(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    write_ssm_file_impl(file_path, col_descs, rows, None, big_endian, false, "SSM")
}

/// Like [`write_ssm_file`], with explicit casacore small-bucket policy and manager name.
pub(crate) fn write_ssm_file_with_options(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    big_endian: bool,
    casacore_default_small: bool,
    dm_name: &str,
) -> Result<Vec<u8>, StorageError> {
    write_ssm_file_impl(
        file_path,
        col_descs,
        rows,
        None,
        big_endian,
        casacore_default_small,
        dm_name,
    )
}

/// Rebuild a single manager's rows using the original bucket and cache sizes.
pub(crate) fn rewrite_ssm_rows(
    original: &Path,
    destination: &Path,
    columns: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    name: &str,
) -> Result<Vec<u8>, StorageError> {
    let header = parse_ssm_header(&mut File::open(original)?)?;
    column_writer::write_ssm_file_columns_with_layout(
        destination,
        columns,
        &column_writer::RowCells::new(rows, None, columns),
        header.big_endian,
        false,
        name,
        Some((Some(header.bucket_size), header.pers_cache_size)),
    )
}

/// Serialize the DM data blob ("SSM" v2) using file-based AipsIO (always BE).
fn serialize_ssm_dm_blob(
    name: &str,
    column_offsets: &[u32],
    ncol: usize,
) -> Result<Vec<u8>, StorageError> {
    serialize_ssm_dm_blob_mapped(name, column_offsets, &vec![0u32; ncol])
}

fn serialize_ssm_dm_blob_mapped(
    name: &str,
    column_offsets: &[u32],
    column_indexes: &[u32],
) -> Result<Vec<u8>, StorageError> {
    let mut io = AipsIo::new_write_only(std::io::Cursor::new(Vec::new()));
    io.putstart("SSM", 2)?;
    io.put_string(name)?;
    // putBlock for column_offsets
    write_block_u32(&mut io, column_offsets)?;
    write_block_u32(&mut io, column_indexes)?;
    io.putend()?;
    let cursor: std::io::Cursor<Vec<u8>> = io.into_inner_typed()?;
    Ok(cursor.into_inner())
}

/// Write a Block<uInt> as a nested AipsIO "Block" object.
fn write_block_u32(io: &mut AipsIo, values: &[u32]) -> Result<(), StorageError> {
    io.putstart("Block", 1)?;
    io.put_u32(values.len() as u32)?;
    for &v in values {
        io.put_u32(v)?;
    }
    io.putend()?;
    Ok(())
}
