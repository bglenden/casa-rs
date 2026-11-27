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

mod array_rows;
pub(crate) use array_rows::{read_ism_array_column_rows, read_ism_array_column_shapes};

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_aipsio::internal::{AipsIoBufferWriter, AipsIoSliceReader, detect_aipsio_byte_order};
use casa_aipsio::{AipsIo, ByteOrder};
use casa_core::ScalarValue;

use super::canonical::{
    read_f32_be, read_f32_le, read_f64_be, read_f64_le, read_i16_be, read_i16_le, read_i32_be,
    read_i32_le, read_i64_be, read_i64_le, read_u16_be, read_u16_le, read_u32_be, read_u32_le,
};
use super::data_file::DataFile;
use super::data_type::CasacoreDataType;
use super::scalar_sources::{ColumnSource, RowFields, SCALAR_BLOCK_CELLS};
use super::stman_aipsio::ColumnRawData;
use super::stman_aipsio::scalar_value_is_default;
use super::stman_array_file::StManArrayFileReader;
use super::table_control::ColumnDescContents;
use super::{ColumnSources, EncodedCells, RequiredScalarColumnData, StorageError};
use crate::table::GeneratedScalarValueRun;

mod append_records;
mod cells;
mod check_layout;
mod replace_ints;
mod selected_scalars_cache;
mod write_columns;
pub(super) use append_records::stage_scalar_append;
pub(super) use replace_ints::stage_int32_replacement;
pub(crate) use write_columns::write_ism_file_column_sources;

pub use check_layout::IncrementalCorruption;
pub(crate) use check_layout::check_incremental_layout;

use cells::{decode_array_cell, encode_cell, flatten_array_cells};

const ISM_HEADER_SIZE: u64 = 512;

/// Rows of values a scalar bucket is sized for.
const ISM_BUCKET_TARGET_ROWS: usize = 32;

/// The most a fixed-width scalar column adds to a bucket's size per row:
/// an 8-byte value and its 8-byte index entry.
const ISM_MAX_SCALAR_ROW_BYTES: usize = 16;

/// Heap the scalar source writer holds per column while it assembles a
/// bucket: the bucket it writes, beside the data and index it is building,
/// which grow by doubling to at most twice their bytes.
pub(super) const ISM_SCALAR_BUCKET_BYTES_PER_COLUMN: usize =
    4 * ISM_BUCKET_TARGET_ROWS * ISM_MAX_SCALAR_ROW_BYTES;

type SparseScalarRowValues = Vec<(usize, Option<casa_core::ScalarValue>)>;

// ---------------------------------------------------------------------------
// In-memory AipsIO helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Parsed types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct IsmHeader {
    version: u32,
    bucket_size: u32,
    nr_buckets: u32,
    pers_cache_size: u32,
    uniq_nr: u32,
    nr_free_buckets: u32,
    first_free_bucket: i32,
    big_endian: bool,
    io_order: ByteOrder,
}

/// Row-to-bucket mapping. `rows[i]` is the start row of bucket interval `i`.
/// `rows[nused]` is the total row count sentinel.
#[derive(Debug, Clone)]
struct IsmIndex {
    used: usize,
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

mod read;
mod write;

pub(crate) use read::fill_ism_f64_array_rows;
pub(crate) use read::read_ism_file_columns;
pub(crate) use read::read_ism_required_scalar_columns_rows;
pub(crate) use read::read_ism_required_scalar_columns_rows_reusing;
pub(crate) use read::read_ism_scalar_column;
pub(crate) use read::read_ism_scalar_column_rows;
use read::*;
pub(crate) use write::refuse_ism_indirect_fixed_arrays;
pub(crate) use write::save_ism_file_scalar_columns_rows_in_place;
pub(crate) use write::save_ism_file_scalar_columns_sparse_rows_in_place;
pub(crate) use write::write_ism_file_indexed;
pub(crate) use write::write_ism_file_scalar_columns;
pub(super) use write::write_ism_scalar_columns_with_layout;
pub(super) use write::write_ism_with_layout;
use write::*;

fn parse_ism_header(file: &mut (impl Read + Seek)) -> Result<IsmHeader, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = vec![0u8; ISM_HEADER_SIZE as usize];
    file.read_exact(&mut header_buf)?;

    let io_order = detect_aipsio_byte_order(&header_buf)?;
    let mut buf = AipsIoSliceReader::new(&header_buf, io_order)?;
    let version = buf.getstart("IncrementalStMan")?;

    let big_endian = if version >= 5 {
        buf.read_bool()?
    } else {
        true // v4 is always big-endian
    };

    let bucket_size = buf.read_u32()?;
    let nr_buckets = buf.read_u32()?;
    let pers_cache_size = buf.read_u32()?;
    let uniq_nr = buf.read_u32()?;
    let nr_free_buckets = buf.read_u32()?;
    let first_free_bucket = buf.read_i32()?;
    buf.getend()?;

    Ok(IsmHeader {
        version,
        bucket_size,
        nr_buckets,
        pers_cache_size,
        uniq_nr,
        nr_free_buckets,
        first_free_bucket,
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

/// The manager's name (`DataManager::dataManagerName()`), from its blob in
/// `table.dat`; `None` for a blob that does not parse.
pub(crate) fn dm_name_from_blob(data: &[u8]) -> Option<String> {
    parse_ism_dm_blob(data).ok()
}

/// The `SPEC` record of an `IncrementalStMan` manager (`ISMBase::dataManagerSpec`).
pub(crate) fn ism_spec(
    table_path: &Path,
    dm_seq_nr: u32,
) -> Result<casa_core::RecordValue, StorageError> {
    let data_path = table_path.join(format!("{}{dm_seq_nr}", super::TABLE_DATA_FILE_PREFIX));
    let mut file = DataFile::open(&data_path)?;
    let header = parse_ism_header(&mut file)?;
    let bucket_size = i32::try_from(header.bucket_size)
        .map_err(|_| StorageError::FormatMismatch("ISM bucket size exceeds i32".into()))?;
    let pers_cache_size = i32::try_from(header.pers_cache_size)
        .map_err(|_| StorageError::FormatMismatch("ISM cache size exceeds i32".into()))?;
    Ok(casa_core::RecordValue::new(vec![
        casa_core::RecordField::new(
            "BUCKETSIZE",
            casa_core::Value::Scalar(ScalarValue::Int32(bucket_size)),
        ),
        casa_core::RecordField::new(
            "MaxCacheSize",
            casa_core::Value::Scalar(ScalarValue::Int32(pers_cache_size)),
        ),
        casa_core::RecordField::new(
            "PERSCACHESIZE",
            casa_core::Value::Scalar(ScalarValue::Int32(pers_cache_size)),
        ),
    ]))
}

// ---------------------------------------------------------------------------
// Index parsing (at end of file, after all buckets)
// ---------------------------------------------------------------------------

fn parse_ism_index(
    file: &mut (impl Read + Seek),
    header: &IsmHeader,
) -> Result<IsmIndex, StorageError> {
    let index_offset = ISM_HEADER_SIZE + (header.nr_buckets as u64) * (header.bucket_size as u64);
    file.seek(SeekFrom::Start(index_offset))?;

    let mut index_data = Vec::new();
    file.read_to_end(&mut index_data)?;

    if index_data.is_empty() {
        return Ok(IsmIndex {
            used: 0,
            rows: vec![0],
            bucket_nrs: vec![],
        });
    }

    let mut buf = AipsIoSliceReader::new(&index_data, header.io_order)?;
    let version = buf.getstart("ISMIndex")?;

    let used = buf.read_u32()? as usize;

    let rows = if version == 1 {
        buf.read_block_u32()?
            .into_iter()
            .map(|v| v as u64)
            .collect()
    } else {
        buf.read_block_u64()?
    };

    let bucket_nrs = buf.read_block_u32()?;
    buf.getend()?;

    Ok(IsmIndex {
        used,
        rows,
        bucket_nrs,
    })
}

/// Append inherited values without touching the buckets or the indirect array file.
pub(super) fn appended_ism_index(
    path: &Path,
    old_rows: u64,
    new_rows: u64,
) -> Result<(u64, Vec<u8>), StorageError> {
    let mut file = File::open(path)?;
    let header = parse_ism_header(&mut file)?;
    let mut index = parse_ism_index(&mut file, &header)?;
    if index.used == 0
        || index.rows.get(index.used).copied() != Some(old_rows)
        || index.bucket_nrs.len() < index.used
    {
        return Err(StorageError::FormatMismatch(format!(
            "IncrementalStMan {} index does not cover {old_rows} rows",
            path.display()
        )));
    }
    index.rows[index.used] = new_rows;
    let bytes = serialize_ism_index(
        &index.rows[..=index.used],
        &index.bucket_nrs[..index.used],
        header.big_endian,
    )?;
    let offset = ISM_HEADER_SIZE + u64::from(header.nr_buckets) * u64::from(header.bucket_size);
    Ok((offset, bytes))
}

// ---------------------------------------------------------------------------
// Bucket parsing
// ---------------------------------------------------------------------------

fn read_ism_bucket(
    file: &mut (impl Read + Seek),
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
) -> Result<casa_core::Value, StorageError> {
    use casa_core::{ScalarValue, Value};

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
            Value::Scalar(ScalarValue::Complex32(casa_core::Complex32::new(re, im)))
        }
        CasacoreDataType::TpDComplex => {
            let (re, im) = if big_endian {
                (read_f64_be(d), read_f64_be(&d[8..]))
            } else {
                (read_f64_le(d), read_f64_le(&d[8..]))
            };
            Value::Scalar(ScalarValue::Complex64(casa_core::Complex64::new(re, im)))
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

// ---------------------------------------------------------------------------
// Read entry point
// ---------------------------------------------------------------------------

pub(crate) enum IsmColumnResult {
    Flat(ColumnRawData),
    Indirect(Vec<Option<casa_core::Value>>),
}

/// Whether casacore stores the array column `desc` as an `ISMIndColumn`:
/// every array column without the Direct option, of fixed shape or not
/// (`ArrayColumnData::createDataManagerColumn`). Its bucket values are the
/// offsets of its cells in the `table.fNi` array file, 0 for no array.
fn is_ism_indirect_array(desc: &ColumnDescContents) -> bool {
    desc.is_array && desc.option & 1 == 0
}

pub(crate) fn read_ism_file(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    nrrow: usize,
) -> Result<Vec<(String, IsmColumnResult)>, StorageError> {
    let columns = col_descs.iter().copied().enumerate().collect::<Vec<_>>();
    read_ism_file_columns(file_path, dm_blob, &columns, col_descs.len(), nrrow)
}

// ===========================================================================
// ISM Write Path
// ===========================================================================

/// Write an ISM data file and return the DM data blob for table.dat.
pub(crate) fn write_ism_file(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    write_ism_file_impl(file_path, col_descs, rows, None, big_endian)
}

pub(super) fn ism_layout(path: &Path) -> Result<(bool, u32, u32), StorageError> {
    let header = parse_ism_header(&mut DataFile::open(path)?)?;
    Ok((
        header.big_endian,
        header.bucket_size,
        header.pers_cache_size,
    ))
}

pub(super) fn serialize_ism_dm_blob(name: &str) -> Result<Vec<u8>, StorageError> {
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
mod tests;
