// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::aipsio_buf::{AipsIoBuf, ByteOrder};
use super::data_type::CasacoreDataType;
use super::stman_array_file::StManArrayFileReader;
use super::table_control::ColumnDesc;
use super::{ColumnData, TableReadError};

const ISM_HEADER_SIZE: u64 = 512;
const AIPSIO_MAGIC: u32 = 0xbebebebe;

pub fn read_incremental_stman_file(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDesc],
    nrrow: usize,
) -> Result<Vec<(String, ColumnData)>, TableReadError> {
    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    let index = parse_ism_index(&mut file, &header)?;
    let big_endian = header.big_endian;

    let mut builders = col_descs
        .iter()
        .map(|desc| ColumnBuilder::new(desc))
        .collect::<Result<Vec<_>, _>>()?;
    let mut indirect_arrays = if col_descs.iter().any(|desc| desc.is_indirect_array()) {
        Some(StManArrayFileReader::open(
            &append_path_suffix(file_path, "i"),
            big_endian,
        )?)
    } else {
        None
    };

    let n_intervals = index.bucket_nrs.len();
    let mut last_bucket_nr = None;
    let mut cached_bucket = None;

    for interval in 0..n_intervals {
        let bucket_start = index.rows[interval] as usize;
        let bucket_end = index.rows[interval + 1] as usize;
        let rows_in_bucket = bucket_end - bucket_start;
        let bucket_nr = index.bucket_nrs[interval];

        if last_bucket_nr != Some(bucket_nr) {
            let raw = read_ism_bucket(&mut file, &header, bucket_nr)?;
            cached_bucket = Some(parse_ism_bucket(&raw, col_descs.len(), big_endian)?);
            last_bucket_nr = Some(bucket_nr);
        }
        let bucket = cached_bucket.as_ref().expect("cached bucket");

        for (col_idx, desc) in col_descs.iter().enumerate() {
            if col_idx >= bucket.col_indices.len() {
                for _ in 0..rows_in_bucket {
                    builders[col_idx].push_default()?;
                }
                continue;
            }

            let col_index = &bucket.col_indices[col_idx];
            for rel_row in 0..rows_in_bucket {
                let k = get_interval(col_index, rel_row as u32);
                let data_offset = col_index.offsets[k] as usize;
                builders[col_idx].push_value(
                    &bucket.data,
                    data_offset,
                    big_endian,
                    desc,
                    indirect_arrays.as_mut(),
                )?;
            }
        }
    }

    let mut result = Vec::with_capacity(col_descs.len());
    for (desc, builder) in col_descs.iter().zip(builders) {
        if builder.len() != nrrow {
            return Err(TableReadError::Format(format!(
                "column {:?} decoded {} rows but expected {}",
                desc.col_name,
                builder.len(),
                nrrow
            )));
        }
        result.push((desc.col_name.clone(), builder.finish()?));
    }
    Ok(result)
}

enum ColumnBuilder {
    Float64(Vec<f64>),
    String(Vec<String>),
    ArrayFloat64 {
        values: Vec<f64>,
        shape: Option<Vec<i32>>,
        rows: usize,
    },
}

impl ColumnBuilder {
    fn new(desc: &ColumnDesc) -> Result<Self, TableReadError> {
        match (desc.data_type, desc.is_array) {
            (CasacoreDataType::TpDouble, false) => Ok(Self::Float64(Vec::new())),
            (CasacoreDataType::TpString, false) => Ok(Self::String(Vec::new())),
            (CasacoreDataType::TpDouble, true) => Ok(Self::ArrayFloat64 {
                values: Vec::new(),
                shape: (!desc.shape.is_empty()).then(|| desc.shape.clone()),
                rows: 0,
            }),
            _ => Err(TableReadError::UnsupportedColumn(format!(
                "unsupported IncrementalStMan column {:?} type {:?} array={}",
                desc.col_name, desc.data_type, desc.is_array
            ))),
        }
    }

    fn push_default(&mut self) -> Result<(), TableReadError> {
        match self {
            Self::Float64(values) => values.push(0.0),
            Self::String(values) => values.push(String::new()),
            Self::ArrayFloat64 {
                values,
                shape,
                rows,
            } => {
                let nrelem = shape.as_deref().map(shape_nrelem).transpose()?.unwrap_or(0);
                values.extend(std::iter::repeat_n(0.0, nrelem));
                *rows += 1;
            }
        }
        Ok(())
    }

    fn push_value(
        &mut self,
        data: &[u8],
        offset: usize,
        big_endian: bool,
        desc: &ColumnDesc,
        indirect_arrays: Option<&mut StManArrayFileReader>,
    ) -> Result<(), TableReadError> {
        match self {
            Self::Float64(values) => values.push(read_f64_at(data, offset, big_endian)?),
            Self::String(values) => values.push(read_string_at(data, offset, 1, big_endian)?),
            Self::ArrayFloat64 {
                values,
                shape,
                rows,
            } => {
                let cell = if desc.is_indirect_array() {
                    let offset = read_i64_at(data, offset, big_endian)?;
                    let reader = indirect_arrays.ok_or_else(|| {
                        TableReadError::Format(format!(
                            "indirect array column {:?} missing array-file reader",
                            desc.col_name
                        ))
                    })?;
                    let (cell_shape, cell_values) =
                        reader.read_f64_array_at(offset)?.ok_or_else(|| {
                            TableReadError::Format(format!(
                                "indirect array column {:?} has undefined cell",
                                desc.col_name
                            ))
                        })?;
                    match shape {
                        Some(existing) if existing != &cell_shape => {
                            return Err(TableReadError::Format(format!(
                                "array column {:?} shape mismatch: {:?} vs {:?}",
                                desc.col_name, existing, cell_shape
                            )));
                        }
                        Some(_) => {}
                        None => *shape = Some(cell_shape),
                    }
                    cell_values
                } else {
                    let known_shape = shape.as_deref().ok_or_else(|| {
                        TableReadError::Format(format!(
                            "direct array column {:?} is missing shape metadata",
                            desc.col_name
                        ))
                    })?;
                    let nrelem = shape_nrelem(known_shape)?;
                    let cell = read_array_f64_at(data, offset, nrelem, big_endian)?;
                    if cell.len() != nrelem {
                        return Err(TableReadError::Format(format!(
                            "array column {:?} expected {nrelem} elements, found {}",
                            desc.col_name,
                            cell.len()
                        )));
                    }
                    cell
                };

                if let Some(known_shape) = shape.as_deref() {
                    let nrelem = shape_nrelem(known_shape)?;
                    if cell.len() != nrelem {
                        return Err(TableReadError::Format(format!(
                            "array column {:?} expected {nrelem} elements, found {}",
                            desc.col_name,
                            cell.len()
                        )));
                    }
                }
                values.extend(cell);
                *rows += 1;
            }
        }
        Ok(())
    }

    fn len(&self) -> usize {
        match self {
            Self::Float64(values) => values.len(),
            Self::String(values) => values.len(),
            Self::ArrayFloat64 { rows, .. } => *rows,
        }
    }

    fn finish(self) -> Result<ColumnData, TableReadError> {
        match self {
            Self::Float64(values) => Ok(ColumnData::Float64(values)),
            Self::String(values) => Ok(ColumnData::String(values)),
            Self::ArrayFloat64 { values, shape, .. } => {
                let shape = shape.ok_or_else(|| {
                    TableReadError::Format("array column is missing shape metadata".to_string())
                })?;
                Ok(ColumnData::ArrayFloat64 { values, shape })
            }
        }
    }
}

#[derive(Debug, Clone)]
struct IsmHeader {
    bucket_size: u32,
    nr_buckets: u32,
    big_endian: bool,
    io_order: ByteOrder,
}

#[derive(Debug, Clone)]
struct IsmIndex {
    rows: Vec<u64>,
    bucket_nrs: Vec<u32>,
}

#[derive(Debug, Clone)]
struct IsmBucketColIndex {
    row_nrs: Vec<u32>,
    offsets: Vec<u32>,
}

#[derive(Debug, Clone)]
struct IsmBucket {
    data: Vec<u8>,
    col_indices: Vec<IsmBucketColIndex>,
}

trait BlockReaderExt {
    fn read_block_u32(&mut self) -> Result<Vec<u32>, TableReadError>;
    fn read_block_u64(&mut self) -> Result<Vec<u64>, TableReadError>;
}

struct IsmAipsIoBuf<'a> {
    data: &'a [u8],
    pos: usize,
    order: ByteOrder,
    level: usize,
}

impl<'a> IsmAipsIoBuf<'a> {
    fn new(data: &'a [u8], order: ByteOrder) -> Self {
        Self {
            data,
            pos: 0,
            order,
            level: 0,
        }
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], TableReadError> {
        if self.pos + n > self.data.len() {
            return Err(TableReadError::Format(
                "ISM AipsIO buffer underrun".to_string(),
            ));
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_bool(&mut self) -> Result<bool, TableReadError> {
        Ok(self.read_bytes(1)?[0] != 0)
    }

    fn read_u32(&mut self) -> Result<u32, TableReadError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            ByteOrder::BigEndian => u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            ByteOrder::LittleEndian => u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    fn read_i32(&mut self) -> Result<i32, TableReadError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            ByteOrder::BigEndian => i32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            ByteOrder::LittleEndian => i32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    fn read_u64(&mut self) -> Result<u64, TableReadError> {
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

    fn read_string(&mut self) -> Result<String, TableReadError> {
        let len = self.read_u32()? as usize;
        let b = self.read_bytes(len)?;
        String::from_utf8(b.to_vec())
            .map_err(|error| TableReadError::Format(format!("invalid UTF-8 in ISM: {error}")))
    }

    fn getstart(&mut self, expected_type: &str) -> Result<u32, TableReadError> {
        if self.level == 0 {
            let magic = self.read_u32()?;
            if magic != AIPSIO_MAGIC {
                return Err(TableReadError::Format(format!(
                    "ISM AipsIO magic mismatch: expected 0x{AIPSIO_MAGIC:08x}, got 0x{magic:08x}"
                )));
            }
        }
        self.level += 1;
        let _obj_len = self.read_u32()?;
        let type_name = self.read_string()?;
        if type_name != expected_type {
            return Err(TableReadError::Format(format!(
                "ISM AipsIO type mismatch: expected {expected_type:?}, got {type_name:?}"
            )));
        }
        self.read_u32()
    }

    fn getend(&mut self) {
        if self.level > 0 {
            self.level -= 1;
        }
    }
}

impl BlockReaderExt for IsmAipsIoBuf<'_> {
    fn read_block_u32(&mut self) -> Result<Vec<u32>, TableReadError> {
        let _version = self.getstart("Block")?;
        let count = self.read_u32()?;
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            values.push(self.read_u32()?);
        }
        self.getend();
        Ok(values)
    }

    fn read_block_u64(&mut self) -> Result<Vec<u64>, TableReadError> {
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

fn detect_ism_aipsio_byte_order(data: &[u8]) -> Result<ByteOrder, TableReadError> {
    if data.len() < 8 {
        return Err(TableReadError::Format(
            "ISM data too short for byte-order detection".to_string(),
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
        (false, false) => Err(TableReadError::Format(format!(
            "ISM: cannot detect byte order (be_len={be_len}, le_len={le_len})"
        ))),
    }
}

fn parse_ism_header(file: &mut (impl Read + Seek)) -> Result<IsmHeader, TableReadError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = vec![0u8; ISM_HEADER_SIZE as usize];
    file.read_exact(&mut header_buf)?;

    let io_order = detect_ism_aipsio_byte_order(&header_buf)?;
    let mut buf = IsmAipsIoBuf::new(&header_buf, io_order);
    let version = buf.getstart("IncrementalStMan")?;
    let big_endian = if version >= 5 { buf.read_bool()? } else { true };
    let bucket_size = buf.read_u32()?;
    let nr_buckets = buf.read_u32()?;
    let _pers_cache_size = buf.read_u32()?;
    let _uniq_nr = buf.read_u32()?;
    let _nr_free_buckets = buf.read_u32()?;
    let _first_free_bucket = buf.read_i32()?;
    buf.getend();
    Ok(IsmHeader {
        bucket_size,
        nr_buckets,
        big_endian,
        io_order,
    })
}

fn parse_ism_dm_blob(data: &[u8]) -> Result<String, TableReadError> {
    let mut io = AipsIoBuf::with_detected_order(data)?;
    let _version = io.getstart("ISM")?;
    let name = io.read_string()?;
    io.getend()?;
    Ok(name)
}

fn parse_ism_index(file: &mut File, header: &IsmHeader) -> Result<IsmIndex, TableReadError> {
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

    let mut buf = IsmAipsIoBuf::new(&index_data, header.io_order);
    let version = buf.getstart("ISMIndex")?;
    let _nused = buf.read_u32()?;
    let rows = if version == 1 {
        buf.read_block_u32()?.into_iter().map(u64::from).collect()
    } else {
        buf.read_block_u64()?
    };
    let bucket_nrs = buf.read_block_u32()?;
    buf.getend();
    Ok(IsmIndex { rows, bucket_nrs })
}

fn read_ism_bucket(
    file: &mut File,
    header: &IsmHeader,
    bucket_nr: u32,
) -> Result<Vec<u8>, TableReadError> {
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
) -> Result<IsmBucket, TableReadError> {
    if raw.len() < 4 {
        return Err(TableReadError::Format("ISM bucket too small".to_string()));
    }
    let raw_offset = if big_endian {
        u32::from_be_bytes(raw[0..4].try_into().expect("u32"))
    } else {
        u32::from_le_bytes(raw[0..4].try_into().expect("u32"))
    };
    if raw_offset & 0x80000000 != 0 {
        return Err(TableReadError::Format(
            "ISM 64-bit row numbers are not supported".to_string(),
        ));
    }
    let index_offset = (raw_offset & 0x7fffffff) as usize;
    let data = raw[4..index_offset.min(raw.len())].to_vec();

    let mut pos = index_offset;
    let mut col_indices = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        if pos + 4 > raw.len() {
            break;
        }
        let n_values = read_u32(raw, pos, big_endian)? as usize;
        pos += 4;

        let mut row_nrs = Vec::with_capacity(n_values);
        for _ in 0..n_values {
            row_nrs.push(read_u32(raw, pos, big_endian)?);
            pos += 4;
        }

        let mut offsets = Vec::with_capacity(n_values);
        for _ in 0..n_values {
            offsets.push(read_u32(raw, pos, big_endian)?);
            pos += 4;
        }

        col_indices.push(IsmBucketColIndex { row_nrs, offsets });
    }

    Ok(IsmBucket { data, col_indices })
}

fn get_interval(col_index: &IsmBucketColIndex, rel_row: u32) -> usize {
    let pos = col_index.row_nrs.partition_point(|&r| r <= rel_row);
    if pos == 0 { 0 } else { pos - 1 }
}

fn append_path_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(suffix);
    PathBuf::from(value)
}

fn shape_nrelem(shape: &[i32]) -> Result<usize, TableReadError> {
    shape.iter().try_fold(1usize, |acc, &dim| {
        let dim = usize::try_from(dim)
            .map_err(|_| TableReadError::Format(format!("negative array dimension {dim}")))?;
        acc.checked_mul(dim)
            .ok_or_else(|| TableReadError::Format("array size overflow".to_string()))
    })
}

fn read_f64_at(data: &[u8], offset: usize, big_endian: bool) -> Result<f64, TableReadError> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| TableReadError::Format("ISM scalar overrun".to_string()))?;
    Ok(if big_endian {
        f64::from_be_bytes(bytes.try_into().expect("f64"))
    } else {
        f64::from_le_bytes(bytes.try_into().expect("f64"))
    })
}

fn read_i64_at(data: &[u8], offset: usize, big_endian: bool) -> Result<i64, TableReadError> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| TableReadError::Format("ISM i64 overrun".to_string()))?;
    Ok(if big_endian {
        i64::from_be_bytes(bytes.try_into().expect("i64"))
    } else {
        i64::from_le_bytes(bytes.try_into().expect("i64"))
    })
}

fn read_string_at(
    data: &[u8],
    offset: usize,
    nvalues: usize,
    big_endian: bool,
) -> Result<String, TableReadError> {
    let strings = read_ism_string(
        data.get(offset..)
            .ok_or_else(|| TableReadError::Format("ISM string overrun".to_string()))?,
        nvalues,
        big_endian,
    )?;
    Ok(strings.into_iter().next().unwrap_or_default())
}

fn read_array_f64_at(
    data: &[u8],
    offset: usize,
    nrelem: usize,
    big_endian: bool,
) -> Result<Vec<f64>, TableReadError> {
    let bytes = data
        .get(offset..offset + (nrelem * 8))
        .ok_or_else(|| TableReadError::Format("ISM array overrun".to_string()))?;
    let mut values = Vec::with_capacity(nrelem);
    for chunk in bytes.chunks_exact(8) {
        values.push(if big_endian {
            f64::from_be_bytes(chunk.try_into().expect("f64"))
        } else {
            f64::from_le_bytes(chunk.try_into().expect("f64"))
        });
    }
    Ok(values)
}

fn read_ism_string(
    data: &[u8],
    nvalues: usize,
    big_endian: bool,
) -> Result<Vec<String>, TableReadError> {
    let total_length = read_u32(data, 0, big_endian)? as usize;
    let mut pos = 4usize;
    let mut strings = Vec::with_capacity(nvalues);

    if nvalues == 1 {
        let str_len = total_length.saturating_sub(4);
        let bytes = data
            .get(pos..pos + str_len)
            .ok_or_else(|| TableReadError::Format("ISM string overrun".to_string()))?;
        strings.push(String::from_utf8(bytes.to_vec()).map_err(|error| {
            TableReadError::Format(format!("ISM string is not UTF-8: {error}"))
        })?);
        return Ok(strings);
    }

    for _ in 0..nvalues {
        let str_len = read_u32(data, pos, big_endian)? as usize;
        pos += 4;
        let bytes = data
            .get(pos..pos + str_len)
            .ok_or_else(|| TableReadError::Format("ISM string array overrun".to_string()))?;
        strings.push(String::from_utf8(bytes.to_vec()).map_err(|error| {
            TableReadError::Format(format!("ISM string is not UTF-8: {error}"))
        })?);
        pos += str_len;
    }
    Ok(strings)
}

fn read_u32(data: &[u8], offset: usize, big_endian: bool) -> Result<u32, TableReadError> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| TableReadError::Format("ISM u32 overrun".to_string()))?;
    Ok(if big_endian {
        u32::from_be_bytes(bytes.try_into().expect("u32"))
    } else {
        u32::from_le_bytes(bytes.try_into().expect("u32"))
    })
}
