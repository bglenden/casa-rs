//! StandardStMan (SSM) bucket-based storage manager reader/writer.
//!
//! On-disk format: `table.fN` file with 512-byte AipsIO header at offset 0,
//! followed by fixed-size buckets at offset 512+.
//!
//! The SSM file header and index data use in-memory AipsIO which may use
//! native byte order (little-endian on modern machines). The DM data blob
//! from `table.dat` always uses big-endian AipsIO (file-based). Bucket
//! data byte order is indicated by the `big_endian` flag in the SSM header.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use casacore_aipsio::AipsIo;

use super::StorageError;
use super::canonical::{
    canonical_element_size, read_bool_bits, read_f32_be, read_f32_le, read_f32_slice_be,
    read_f32_slice_le, read_f64_be, read_f64_le, read_f64_slice_be, read_f64_slice_le,
    read_i16_slice_be, read_i16_slice_le, read_i32_be, read_i32_slice_be, read_i32_slice_le,
    read_i64_slice_be, read_i64_slice_le, read_u16_slice_be, read_u16_slice_le, read_u32_slice_be,
    read_u32_slice_le, write_bool_bits, write_f32_be, write_f64_be, write_i16_be, write_i32_be,
    write_i64_be, write_u16_be, write_u32_be,
};
use super::data_type::CasacoreDataType;
use super::stman_aipsio::ColumnRawData;
use super::table_control::ColumnDescContents;

const SSM_HEADER_SIZE: u64 = 512;
const AIPSIO_MAGIC: u32 = 0xbebebebe;

// ---------------------------------------------------------------------------
// Byte-order-aware buffer reader for in-memory AipsIO parsing
// ---------------------------------------------------------------------------

/// Whether in-memory AipsIO data uses big- or little-endian encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoByteOrder {
    Big,
    Little,
}

/// Minimal reader for AipsIO-framed data in either byte order.
struct AipsIoBuf<'a> {
    data: &'a [u8],
    pos: usize,
    order: IoByteOrder,
    level: usize,
}

impl<'a> AipsIoBuf<'a> {
    fn new(data: &'a [u8], order: IoByteOrder) -> Self {
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
                "SSM AipsIO buffer underrun".to_string(),
            ));
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u32(&mut self) -> Result<u32, StorageError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            IoByteOrder::Big => u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            IoByteOrder::Little => u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    fn read_i32(&mut self) -> Result<i32, StorageError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            IoByteOrder::Big => i32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            IoByteOrder::Little => i32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    fn read_u64(&mut self) -> Result<u64, StorageError> {
        let b = self.read_bytes(8)?;
        Ok(match self.order {
            IoByteOrder::Big => {
                u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
            IoByteOrder::Little => {
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
            .map_err(|e| StorageError::FormatMismatch(format!("invalid UTF-8 in SSM: {e}")))
    }

    /// Read AipsIO getstart: magic at level 0, then obj_len + type + version.
    fn getstart(&mut self, expected_type: &str) -> Result<u32, StorageError> {
        if self.level == 0 {
            let magic = self.read_u32()?;
            if magic != AIPSIO_MAGIC {
                return Err(StorageError::FormatMismatch(format!(
                    "SSM AipsIO magic mismatch: expected 0x{AIPSIO_MAGIC:08x}, got 0x{magic:08x}"
                )));
            }
        }
        self.level += 1;
        let _obj_len = self.read_u32()?;
        let type_name = self.read_string()?;
        if type_name != expected_type {
            return Err(StorageError::FormatMismatch(format!(
                "SSM AipsIO type mismatch: expected '{expected_type}', got '{type_name}'"
            )));
        }
        self.read_u32()
    }

    /// Finish reading an object (decrements level, no validation).
    fn getend(&mut self) {
        if self.level > 0 {
            self.level -= 1;
        }
    }

    /// Read a Block<uInt> (nested AipsIO "Block" object containing u32 values).
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

    /// Read a Block<Int64> (nested AipsIO "Block" object containing u64 values).
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
/// Returns the byte order by checking the object length field after the magic.
fn detect_aipsio_byte_order(data: &[u8]) -> Result<IoByteOrder, StorageError> {
    if data.len() < 8 {
        return Err(StorageError::FormatMismatch(
            "SSM data too short for byte order detection".to_string(),
        ));
    }
    // Bytes 0-3: magic (same in both byte orders for 0xbebebebe)
    // Bytes 4-7: object length
    let be_len = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let le_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

    // A valid object length is small (< 4096 for any reasonable SSM header/index)
    let be_ok = be_len > 0 && be_len < 4096;
    let le_ok = le_len > 0 && le_len < 4096;

    match (be_ok, le_ok) {
        (true, false) => Ok(IoByteOrder::Big),
        (false, true) => Ok(IoByteOrder::Little),
        (true, true) => {
            // Both look valid — prefer native (little-endian on modern machines)
            Ok(IoByteOrder::Little)
        }
        (false, false) => Err(StorageError::FormatMismatch(format!(
            "SSM: cannot detect byte order (be_len={be_len}, le_len={le_len})"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Parsed types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SsmHeader {
    bucket_size: u32,
    nr_buckets: u32,
    #[allow(dead_code)]
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
    io_order: IoByteOrder,
}

#[derive(Debug, Clone)]
struct SsmDmInfo {
    #[allow(dead_code)]
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

fn parse_ssm_header(file: &mut File) -> Result<SsmHeader, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = vec![0u8; SSM_HEADER_SIZE as usize];
    file.read_exact(&mut header_buf)?;

    let io_order = detect_aipsio_byte_order(&header_buf)?;
    let mut buf = AipsIoBuf::new(&header_buf, io_order);
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
    file: &mut File,
    header: &SsmHeader,
    bucket_nr: u32,
) -> Result<Vec<u8>, StorageError> {
    let offset = SSM_HEADER_SIZE + (bucket_nr as u64) * (header.bucket_size as u64);
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; header.bucket_size as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn parse_ssm_indices(file: &mut File, header: &SsmHeader) -> Result<Vec<SsmIndex>, StorageError> {
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
    let mut buf = AipsIoBuf::new(&index_data, header.io_order);

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
            buf.getend();
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

        buf.getend();

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

// ---------------------------------------------------------------------------
// String bucket reader
// ---------------------------------------------------------------------------

const STRING_BUCKET_HEADER: usize = 16;

fn read_ssm_string(
    file: &mut File,
    header: &SsmHeader,
    bucket_nr: i32,
    offset: i32,
    length: i32,
) -> Result<String, StorageError> {
    if length == 0 {
        return Ok(String::new());
    }

    let mut result = Vec::with_capacity(length as usize);
    let mut remaining = length as usize;
    let mut cur_bucket = bucket_nr;
    let mut cur_offset = offset as usize;

    while remaining > 0 {
        let bucket = read_bucket(file, header, cur_bucket as u32)?;
        let data_start = STRING_BUCKET_HEADER + cur_offset;
        let available = bucket.len() - data_start;
        let chunk = remaining.min(available);
        result.extend_from_slice(&bucket[data_start..data_start + chunk]);
        remaining -= chunk;

        if remaining > 0 {
            let next_bucket = read_i32_canonical(&bucket[12..16], header.big_endian);
            if next_bucket < 0 {
                return Err(StorageError::FormatMismatch(
                    "SSM string chain ended prematurely".to_string(),
                ));
            }
            cur_bucket = next_bucket;
            cur_offset = 0;
        }
    }

    String::from_utf8(result)
        .map_err(|e| StorageError::FormatMismatch(format!("SSM string is not valid UTF-8: {e}")))
}

// ---------------------------------------------------------------------------
// Column data reader
// ---------------------------------------------------------------------------

pub(crate) fn read_ssm_file(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    nrrow: usize,
) -> Result<Vec<(String, ColumnRawData)>, StorageError> {
    let mut file = File::open(file_path)?;
    let header = parse_ssm_header(&mut file)?;
    let dm_info = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;

    let mut result = Vec::with_capacity(col_descs.len());

    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        if col_idx >= dm_info.column_offsets.len() {
            return Err(StorageError::FormatMismatch(format!(
                "SSM column index {col_idx} out of range for columnOffsets"
            )));
        }

        let column_offset = dm_info.column_offsets[col_idx] as usize;
        let index_nr = dm_info.col_index_map[col_idx] as usize;

        if index_nr >= indices.len() {
            return Err(StorageError::FormatMismatch(format!(
                "SSM column {} references index {index_nr} but only {} indices exist",
                col_desc.col_name,
                indices.len()
            )));
        }
        let index = &indices[index_nr];

        let nrelem = if col_desc.is_array && !col_desc.shape.is_empty() {
            col_desc.shape.iter().map(|&s| s as usize).product()
        } else {
            1usize
        };

        let raw = read_column_from_buckets(
            &mut file,
            &header,
            index,
            column_offset,
            col_desc.data_type,
            nrelem,
            nrrow,
        )?;

        result.push((col_desc.col_name.clone(), raw));
    }

    Ok(result)
}

fn read_column_from_buckets(
    file: &mut File,
    header: &SsmHeader,
    index: &SsmIndex,
    column_offset: usize,
    data_type: CasacoreDataType,
    nrelem: usize,
    nrrow: usize,
) -> Result<ColumnRawData, StorageError> {
    let (elem_bytes, _) = canonical_element_size(data_type);
    let be = header.big_endian;

    match data_type {
        CasacoreDataType::TpBool => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            let mut row = 0usize;
            while row < nrrow {
                let (bucket_nr, start_row, end_row) =
                    index.find_bucket(row as u64).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM index has no bucket for row {row}"
                        ))
                    })?;
                let bucket = read_bucket(file, header, bucket_nr)?;
                let row_in_bucket = (row as u64 - start_row) as usize;
                let rows_in_chunk =
                    ((end_row - start_row + 1) as usize - row_in_bucket).min(nrrow - row);
                let bit_offset = row_in_bucket * nrelem;
                let byte_offset = column_offset + bit_offset / 8;
                let sub_bit = bit_offset % 8;
                let bools = read_bool_bits(&bucket[byte_offset..], sub_bit, rows_in_chunk * nrelem);
                values.extend_from_slice(&bools);
                row += rows_in_chunk;
            }
            Ok(ColumnRawData::Bool(values))
        }
        CasacoreDataType::TpUChar => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend_from_slice(&data[..count]);
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::UInt8(values))
        }
        CasacoreDataType::TpShort => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_i16_slice_be(data, count)
                    } else {
                        read_i16_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Int16(values))
        }
        CasacoreDataType::TpUShort => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_u16_slice_be(data, count)
                    } else {
                        read_u16_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::UInt16(values))
        }
        CasacoreDataType::TpInt => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_i32_slice_be(data, count)
                    } else {
                        read_i32_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Int32(values))
        }
        CasacoreDataType::TpUInt => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_u32_slice_be(data, count)
                    } else {
                        read_u32_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::UInt32(values))
        }
        CasacoreDataType::TpFloat => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_f32_slice_be(data, count)
                    } else {
                        read_f32_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Float32(values))
        }
        CasacoreDataType::TpDouble => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_f64_slice_be(data, count)
                    } else {
                        read_f64_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Float64(values))
        }
        CasacoreDataType::TpInt64 => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_i64_slice_be(data, count)
                    } else {
                        read_i64_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Int64(values))
        }
        CasacoreDataType::TpComplex => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    for i in 0..count {
                        let (re, im) = if be {
                            (read_f32_be(&data[i * 8..]), read_f32_be(&data[i * 8 + 4..]))
                        } else {
                            (read_f32_le(&data[i * 8..]), read_f32_le(&data[i * 8 + 4..]))
                        };
                        values.push(casacore_types::Complex32::new(re, im));
                    }
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Complex32(values))
        }
        CasacoreDataType::TpDComplex => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    for i in 0..count {
                        let (re, im) = if be {
                            (
                                read_f64_be(&data[i * 16..]),
                                read_f64_be(&data[i * 16 + 8..]),
                            )
                        } else {
                            (
                                read_f64_le(&data[i * 16..]),
                                read_f64_le(&data[i * 16 + 8..]),
                            )
                        };
                        values.push(casacore_types::Complex64::new(re, im));
                    }
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Complex64(values))
        }
        CasacoreDataType::TpString => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            for row in 0..nrrow {
                let (bucket_nr, start_row, _end_row) =
                    index.find_bucket(row as u64).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM index has no bucket for row {row}"
                        ))
                    })?;
                let bucket = read_bucket(file, header, bucket_nr)?;
                let row_in_bucket = (row as u64 - start_row) as usize;
                let ref_offset = column_offset + row_in_bucket * 12;
                let str_bucket = read_i32_canonical(&bucket[ref_offset..], be);
                let str_offset = read_i32_canonical(&bucket[ref_offset + 4..], be);
                let str_length = read_i32_canonical(&bucket[ref_offset + 8..], be);

                let s = if str_length <= 8 {
                    let inline_start = ref_offset;
                    String::from_utf8(
                        bucket[inline_start..inline_start + str_length as usize].to_vec(),
                    )
                    .map_err(|e| StorageError::FormatMismatch(format!("invalid UTF-8: {e}")))?
                } else {
                    read_ssm_string(file, header, str_bucket, str_offset, str_length)?
                };

                for _ in 0..nrelem {
                    values.push(s.clone());
                }
            }
            Ok(ColumnRawData::String(values))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported SSM data type: {data_type:?}"
        ))),
    }
}

/// Iterate over data buckets and process column data for non-Bool, non-String types.
#[allow(clippy::too_many_arguments)]
fn read_typed_column<F>(
    file: &mut File,
    header: &SsmHeader,
    index: &SsmIndex,
    column_offset: usize,
    nrrow: usize,
    nrelem: usize,
    elem_bytes: usize,
    mut process: F,
) -> Result<(), StorageError>
where
    F: FnMut(&[u8], usize) -> Result<(), StorageError>,
{
    let bytes_per_row = elem_bytes * nrelem;
    let mut row = 0usize;

    while row < nrrow {
        let (bucket_nr, start_row, end_row) = index.find_bucket(row as u64).ok_or_else(|| {
            StorageError::FormatMismatch(format!("SSM index has no bucket for row {row}"))
        })?;
        let bucket = read_bucket(file, header, bucket_nr)?;
        let row_in_bucket = (row as u64 - start_row) as usize;
        let rows_in_chunk = ((end_row - start_row + 1) as usize - row_in_bucket).min(nrrow - row);
        let data_start = column_offset + row_in_bucket * bytes_per_row;
        process(&bucket[data_start..], rows_in_chunk * nrelem)?;
        row += rows_in_chunk;
    }

    Ok(())
}

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
    rows: &[casacore_types::RecordValue],
) -> Result<Vec<u8>, StorageError> {
    let nrrow = rows.len();
    let ncol = col_descs.len();

    // 1. Compute column sizes in bits (for Bool bit-packing)
    let col_sizes_bits: Vec<usize> = col_descs
        .iter()
        .map(|c| {
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                1
            };
            let (_, bits) = canonical_element_size(c.data_type);
            nrelem * bits
        })
        .collect();

    // 2. Compute rows_per_bucket and bucket_size
    let total_bits_per_row: usize = col_sizes_bits.iter().sum();
    let total_bytes_per_row = total_bits_per_row.div_ceil(8);
    let rows_per_bucket = if total_bytes_per_row == 0 {
        32u32
    } else {
        // Start with 32 rows/bucket (SSM default), adjust to fit
        let mut rpb = 32u32;
        loop {
            let size: usize = col_sizes_bits
                .iter()
                .map(|&bits| (rpb as usize * bits).div_ceil(8))
                .sum();
            let next_size: usize = col_sizes_bits
                .iter()
                .map(|&bits| ((rpb as usize + 1) * bits).div_ceil(8))
                .sum();
            if next_size > 128.max(size) {
                break;
            }
            rpb += 1;
        }
        rpb
    };

    let bucket_size: u32 = {
        let data_size: usize = col_sizes_bits
            .iter()
            .map(|&bits| (rows_per_bucket as usize * bits).div_ceil(8))
            .sum();
        data_size.max(128) as u32
    };

    // 3. Compute column offsets within a data bucket
    let mut column_offsets = vec![0u32; ncol];
    let mut offset = 0u32;
    for (i, &bits) in col_sizes_bits.iter().enumerate() {
        column_offsets[i] = offset;
        offset += (rows_per_bucket as usize * bits).div_ceil(8) as u32;
    }

    // 4. Build data buckets
    let nr_data_buckets = if nrrow == 0 {
        0
    } else {
        nrrow.div_ceil(rows_per_bucket as usize)
    };

    let mut buckets: Vec<Vec<u8>> = (0..nr_data_buckets)
        .map(|_| vec![0u8; bucket_size as usize])
        .collect();

    // String handling: collect long strings into string buckets
    let mut string_buckets: Vec<Vec<u8>> = Vec::new();

    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        let nrelem = if col_desc.is_array && !col_desc.shape.is_empty() {
            col_desc.shape.iter().map(|&s| s as usize).product()
        } else {
            1usize
        };
        let col_off = column_offsets[col_idx] as usize;

        for (row, row_record) in rows.iter().enumerate() {
            let bucket_idx = row / rows_per_bucket as usize;
            let row_in_bucket = row % rows_per_bucket as usize;
            let bucket = &mut buckets[bucket_idx];

            let value = row_record
                .fields()
                .iter()
                .find(|f| f.name == col_desc.col_name)
                .map(|f| &f.value);

            write_cell_to_bucket(
                bucket,
                col_off,
                row_in_bucket,
                col_desc.data_type,
                nrelem,
                value,
                &mut string_buckets,
                bucket_size as usize,
            );
        }
    }

    // 5. Build the SSMIndex
    let mut last_row = Vec::with_capacity(nr_data_buckets);
    let mut bucket_number = Vec::with_capacity(nr_data_buckets);
    for i in 0..nr_data_buckets {
        let end_row = ((i + 1) * rows_per_bucket as usize - 1).min(nrrow - 1);
        last_row.push(end_row as u64);
        bucket_number.push(i as u32);
    }

    // 6. Serialize the SSMIndex to bytes (LE AipsIO)
    let index_data = serialize_ssm_index(
        nr_data_buckets as u32,
        rows_per_bucket,
        ncol as i32,
        &last_row,
        &bucket_number,
    );

    // 7. Write the index into index buckets
    let chain_overhead = 8usize; // check number + next bucket pointer
    let data_per_index_bucket = bucket_size as usize - chain_overhead;
    let nr_idx_buckets = if index_data.is_empty() {
        0
    } else {
        index_data.len().div_ceil(data_per_index_bucket)
    };

    // String bucket count (each goes after data buckets, before index buckets)
    let nr_string_buckets = string_buckets.len();
    let first_string_bucket = if nr_string_buckets > 0 {
        nr_data_buckets as i32
    } else {
        -1i32
    };

    let first_idx_bucket = (nr_data_buckets + nr_string_buckets) as i32;
    let nr_buckets = nr_data_buckets + nr_string_buckets + nr_idx_buckets;

    // 8. Write the 512-byte header (BE AipsIO framing, v3 with LE bucket data)
    let header_buf = serialize_ssm_header(
        bucket_size,
        nr_buckets as u32,
        0,  // nr_free_buckets
        -1, // first_free_bucket
        nr_idx_buckets as u32,
        first_idx_bucket,
        0, // idx_bucket_offset
        first_string_bucket,
        index_data.len() as u32,
        1, // nr_indices (single index for all columns)
    );

    // 9. Assemble the file
    let mut file = File::create(file_path)?;
    file.write_all(&header_buf)?;

    // Data buckets
    for bucket in &buckets {
        file.write_all(bucket)?;
    }

    // String buckets
    for sb in &string_buckets {
        file.write_all(sb)?;
    }

    // Index buckets
    let mut idx_remaining = &index_data[..];
    for i in 0..nr_idx_buckets {
        let mut idx_bucket = vec![0u8; bucket_size as usize];
        // Check number at offset 0 (canonical/big-endian)
        idx_bucket[0..4].copy_from_slice(&0i32.to_be_bytes());
        // Next bucket pointer at offset 4 (canonical/big-endian)
        let next = if i + 1 < nr_idx_buckets {
            (first_idx_bucket as usize + i + 1) as i32
        } else {
            -1i32
        };
        idx_bucket[4..8].copy_from_slice(&next.to_be_bytes());
        // Copy index data
        let chunk = idx_remaining.len().min(data_per_index_bucket);
        idx_bucket[chain_overhead..chain_overhead + chunk].copy_from_slice(&idx_remaining[..chunk]);
        idx_remaining = &idx_remaining[chunk..];
        file.write_all(&idx_bucket)?;
    }

    // 10. Generate the DM data blob (always big-endian, file-based AipsIO)
    let dm_blob = serialize_ssm_dm_blob("SSM", &column_offsets, ncol)?;

    Ok(dm_blob)
}

/// Write a single cell value into a data bucket using canonical (BE) format.
#[allow(clippy::too_many_arguments)]
fn write_cell_to_bucket(
    bucket: &mut [u8],
    col_offset: usize,
    row_in_bucket: usize,
    data_type: CasacoreDataType,
    nrelem: usize,
    value: Option<&casacore_types::Value>,
    string_buckets: &mut Vec<Vec<u8>>,
    bucket_size: usize,
) {
    use casacore_types::{ScalarValue, Value};

    match data_type {
        CasacoreDataType::TpBool => {
            let bit_offset = row_in_bucket * nrelem;
            let byte_offset = col_offset + bit_offset / 8;
            let sub_bit = bit_offset % 8;
            let bools: Vec<bool> = match value {
                Some(Value::Scalar(ScalarValue::Bool(b))) => vec![*b],
                _ => vec![false; nrelem],
            };
            write_bool_bits(&mut bucket[byte_offset..], sub_bit, &bools);
        }
        CasacoreDataType::TpString => {
            // String reference: 3 canonical (BE) ints (bucket_nr, offset, length)
            // Short strings (<=8 bytes) stored inline
            let ref_offset = col_offset + row_in_bucket * 12;
            let s = match value {
                Some(Value::Scalar(ScalarValue::String(s))) => s.as_str(),
                _ => "",
            };
            let len = s.len() as i32;
            if len <= 8 {
                // Inline: write string bytes at ref_offset, then zeros, then length at +8
                bucket[ref_offset..ref_offset + s.len()].copy_from_slice(s.as_bytes());
                // Zero remaining bytes in the 12-byte slot (already zeroed)
                write_i32_be(&mut bucket[ref_offset + 8..], len);
            } else {
                // Store in string bucket
                let (sb_nr, sb_offset) =
                    allocate_string_in_bucket(string_buckets, s.as_bytes(), bucket_size);
                write_i32_be(&mut bucket[ref_offset..], sb_nr as i32);
                write_i32_be(&mut bucket[ref_offset + 4..], sb_offset as i32);
                write_i32_be(&mut bucket[ref_offset + 8..], len);
            }
        }
        _ => {
            // Fixed-size types
            let (elem_bytes, _) = canonical_element_size(data_type);
            let bytes_per_row = elem_bytes * nrelem;
            let data_start = col_offset + row_in_bucket * bytes_per_row;
            write_value_canonical(&mut bucket[data_start..], data_type, nrelem, value);
        }
    }
}

/// Write a scalar or array value in canonical (big-endian) format.
fn write_value_canonical(
    dst: &mut [u8],
    data_type: CasacoreDataType,
    nrelem: usize,
    value: Option<&casacore_types::Value>,
) {
    use casacore_types::{ArrayValue, ScalarValue, Value};

    match data_type {
        CasacoreDataType::TpUChar => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::UInt8(v))) => *v,
                _ => 0,
            };
            dst[0] = v;
        }
        CasacoreDataType::TpShort => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::Int16(v))) => *v,
                _ => 0,
            };
            write_i16_be(dst, v);
        }
        CasacoreDataType::TpUShort => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::UInt16(v))) => *v,
                _ => 0,
            };
            write_u16_be(dst, v);
        }
        CasacoreDataType::TpInt => {
            if nrelem == 1 {
                let v = match value {
                    Some(Value::Scalar(ScalarValue::Int32(v))) => *v,
                    _ => 0,
                };
                write_i32_be(dst, v);
            } else if let Some(Value::Array(ArrayValue::Int32(arr))) = value {
                for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                    write_i32_be(&mut dst[i * 4..], v);
                }
            }
        }
        CasacoreDataType::TpUInt => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::UInt32(v))) => *v,
                _ => 0,
            };
            write_u32_be(dst, v);
        }
        CasacoreDataType::TpFloat => {
            if nrelem == 1 {
                let v = match value {
                    Some(Value::Scalar(ScalarValue::Float32(v))) => *v,
                    _ => 0.0,
                };
                write_f32_be(dst, v);
            } else if let Some(Value::Array(ArrayValue::Float32(arr))) = value {
                for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                    write_f32_be(&mut dst[i * 4..], v);
                }
            }
        }
        CasacoreDataType::TpDouble => {
            if nrelem == 1 {
                let v = match value {
                    Some(Value::Scalar(ScalarValue::Float64(v))) => *v,
                    _ => 0.0,
                };
                write_f64_be(dst, v);
            } else if let Some(Value::Array(ArrayValue::Float64(arr))) = value {
                for (i, &v) in arr.t().iter().enumerate().take(nrelem) {
                    write_f64_be(&mut dst[i * 8..], v);
                }
            }
        }
        CasacoreDataType::TpInt64 => {
            let v = match value {
                Some(Value::Scalar(ScalarValue::Int64(v))) => *v,
                _ => 0,
            };
            write_i64_be(dst, v);
        }
        CasacoreDataType::TpComplex => {
            if nrelem == 1 {
                let (re, im) = match value {
                    Some(Value::Scalar(ScalarValue::Complex32(c))) => (c.re, c.im),
                    _ => (0.0, 0.0),
                };
                write_f32_be(dst, re);
                write_f32_be(&mut dst[4..], im);
            } else if let Some(Value::Array(ArrayValue::Complex32(arr))) = value {
                for (i, c) in arr.t().iter().enumerate().take(nrelem) {
                    write_f32_be(&mut dst[i * 8..], c.re);
                    write_f32_be(&mut dst[i * 8 + 4..], c.im);
                }
            }
        }
        CasacoreDataType::TpDComplex => {
            if nrelem == 1 {
                let (re, im) = match value {
                    Some(Value::Scalar(ScalarValue::Complex64(c))) => (c.re, c.im),
                    _ => (0.0, 0.0),
                };
                write_f64_be(dst, re);
                write_f64_be(&mut dst[8..], im);
            } else if let Some(Value::Array(ArrayValue::Complex64(arr))) = value {
                for (i, c) in arr.t().iter().enumerate().take(nrelem) {
                    write_f64_be(&mut dst[i * 16..], c.re);
                    write_f64_be(&mut dst[i * 16 + 8..], c.im);
                }
            }
        }
        _ => {}
    }
}

/// Allocate space for a string in string buckets. Returns (bucket_nr, offset).
fn allocate_string_in_bucket(
    string_buckets: &mut Vec<Vec<u8>>,
    data: &[u8],
    bucket_size: usize,
) -> (usize, usize) {
    // String bucket layout: 4 canonical (BE) ints (freeLink, usedLength, nDeleted, nextBucket) + data
    let header_size = 16;
    let data_capacity = bucket_size - header_size;

    // Try to fit in the last bucket
    if let Some(last) = string_buckets.last_mut() {
        let used_len = i32::from_be_bytes([last[4], last[5], last[6], last[7]]) as usize;
        if used_len + data.len() <= data_capacity {
            let offset = used_len;
            last[header_size + offset..header_size + offset + data.len()].copy_from_slice(data);
            let new_used = (used_len + data.len()) as i32;
            last[4..8].copy_from_slice(&new_used.to_be_bytes());
            return (string_buckets.len() - 1, offset);
        }
    }

    // Allocate a new string bucket
    let mut bucket = vec![0u8; bucket_size];
    // freeLink = -1
    bucket[0..4].copy_from_slice(&(-1i32).to_be_bytes());
    // usedLength = data.len()
    bucket[4..8].copy_from_slice(&(data.len() as i32).to_be_bytes());
    // nDeleted = 0
    bucket[8..12].copy_from_slice(&0i32.to_be_bytes());
    // nextBucket = -1 (will be updated if chaining needed)
    bucket[12..16].copy_from_slice(&(-1i32).to_be_bytes());
    // Write data
    bucket[header_size..header_size + data.len()].copy_from_slice(data);

    let bucket_nr = string_buckets.len();
    string_buckets.push(bucket);
    (bucket_nr, 0)
}

/// Serialize the SSM header into a 512-byte buffer.
///
/// The AipsIO framing uses big-endian (canonical) byte order because C++
/// casacore reads SSM files through `CanonicalIO`. The `asBigEndian = false`
/// flag inside the header controls only bucket data byte order.
#[allow(clippy::too_many_arguments)]
fn serialize_ssm_header(
    bucket_size: u32,
    nr_buckets: u32,
    nr_free_buckets: u32,
    first_free_bucket: i32,
    nr_idx_buckets: u32,
    first_idx_bucket: i32,
    idx_bucket_offset: u32,
    last_string_bucket: i32,
    index_length: u32,
    nr_indices: u32,
) -> Vec<u8> {
    // AipsIO framing is always canonical (big-endian) for file-based I/O
    let mut buf = AipsIoWriteBuf::new(IoByteOrder::Big);
    buf.putstart("StandardStMan", 3);
    buf.put_bool(true); // asBigEndian = true (canonical/BE bucket data)
    buf.put_u32(bucket_size);
    buf.put_u32(nr_buckets);
    buf.put_u32(0); // persCacheSize
    buf.put_u32(nr_free_buckets);
    buf.put_i32(first_free_bucket);
    buf.put_u32(nr_idx_buckets);
    buf.put_i32(first_idx_bucket);
    buf.put_u32(idx_bucket_offset);
    buf.put_i32(last_string_bucket);
    buf.put_u32(index_length);
    buf.put_u32(nr_indices);
    buf.putend();

    // Pad to 512 bytes
    let mut header = buf.into_bytes();
    header.resize(SSM_HEADER_SIZE as usize, 0);
    header
}

/// Serialize SSMIndex to bytes using canonical (big-endian) AipsIO.
fn serialize_ssm_index(
    n_used: u32,
    rows_per_bucket: u32,
    nr_columns: i32,
    last_row: &[u64],
    bucket_number: &[u32],
) -> Vec<u8> {
    let mut buf = AipsIoWriteBuf::new(IoByteOrder::Big);
    // SSMIndex is a top-level object (level 0 → has magic)
    buf.putstart("SSMIndex", 2);
    buf.put_u32(n_used);
    buf.put_u32(rows_per_bucket);
    buf.put_i32(nr_columns);
    // freeSpace map: empty SimpleOrderedMap
    buf.putstart_nested("SimpleOrderedMap", 1);
    buf.put_i32(0); // default_value
    buf.put_u32(0); // nr entries
    buf.put_u32(0); // incr
    buf.putend_nested();
    // lastRow: Block<Int64> (version 2)
    buf.putstart_nested("Block", 1);
    buf.put_u32(last_row.len() as u32);
    for &v in last_row {
        buf.put_u64(v);
    }
    buf.putend_nested();
    // bucketNumber: Block<uInt>
    buf.putstart_nested("Block", 1);
    buf.put_u32(bucket_number.len() as u32);
    for &v in bucket_number {
        buf.put_u32(v);
    }
    buf.putend_nested();
    buf.putend();
    buf.into_bytes()
}

/// Serialize the DM data blob ("SSM" v2) using file-based AipsIO (always BE).
fn serialize_ssm_dm_blob(
    name: &str,
    column_offsets: &[u32],
    ncol: usize,
) -> Result<Vec<u8>, StorageError> {
    let mut io = AipsIo::new_write_only(std::io::Cursor::new(Vec::new()));
    io.putstart("SSM", 2)?;
    io.put_string(name)?;
    // putBlock for column_offsets
    write_block_u32(&mut io, column_offsets)?;
    // putBlock for col_index_map (all zeros → single index for all columns)
    let col_index_map = vec![0u32; ncol];
    write_block_u32(&mut io, &col_index_map)?;
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

// ---------------------------------------------------------------------------
// In-memory AipsIO writer (for SSM header and index data)
// ---------------------------------------------------------------------------

/// Minimal writer for in-memory AipsIO in the given byte order.
struct AipsIoWriteBuf {
    data: Vec<u8>,
    order: IoByteOrder,
    /// Stack of obj_len placeholder positions for putend backpatching.
    len_positions: Vec<usize>,
    level: usize,
}

impl AipsIoWriteBuf {
    fn new(order: IoByteOrder) -> Self {
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
            IoByteOrder::Big => self.data.extend_from_slice(&val.to_be_bytes()),
            IoByteOrder::Little => self.data.extend_from_slice(&val.to_le_bytes()),
        }
    }

    fn put_i32(&mut self, val: i32) {
        match self.order {
            IoByteOrder::Big => self.data.extend_from_slice(&val.to_be_bytes()),
            IoByteOrder::Little => self.data.extend_from_slice(&val.to_le_bytes()),
        }
    }

    fn put_u64(&mut self, val: u64) {
        match self.order {
            IoByteOrder::Big => self.data.extend_from_slice(&val.to_be_bytes()),
            IoByteOrder::Little => self.data.extend_from_slice(&val.to_le_bytes()),
        }
    }

    fn put_bool(&mut self, val: bool) {
        self.put_u8(if val { 1 } else { 0 });
    }

    fn put_string(&mut self, s: &str) {
        self.put_u32(s.len() as u32);
        self.data.extend_from_slice(s.as_bytes());
    }

    /// Begin a top-level AipsIO object (writes magic at level 0).
    fn putstart(&mut self, type_name: &str, version: u32) {
        // Magic only at level 0
        self.put_u32(AIPSIO_MAGIC);
        // Placeholder for obj_len (will be backpatched in putend)
        let pos = self.data.len();
        self.put_u32(0); // placeholder
        self.len_positions.push(pos);
        self.put_string(type_name);
        self.put_u32(version);
        self.level += 1;
    }

    /// Begin a nested AipsIO object (no magic).
    fn putstart_nested(&mut self, type_name: &str, version: u32) {
        let pos = self.data.len();
        self.put_u32(0); // placeholder for obj_len
        self.len_positions.push(pos);
        self.put_string(type_name);
        self.put_u32(version);
        self.level += 1;
    }

    /// End the current object, backpatch obj_len.
    fn putend(&mut self) {
        if let Some(pos) = self.len_positions.pop() {
            // obj_len includes everything from the obj_len field to here
            let obj_len = (self.data.len() - pos) as u32;
            let bytes = match self.order {
                IoByteOrder::Big => obj_len.to_be_bytes(),
                IoByteOrder::Little => obj_len.to_le_bytes(),
            };
            self.data[pos..pos + 4].copy_from_slice(&bytes);
        }
        if self.level > 0 {
            self.level -= 1;
        }
    }

    /// End a nested object (same as putend, alias for clarity).
    fn putend_nested(&mut self) {
        self.putend();
    }
}
