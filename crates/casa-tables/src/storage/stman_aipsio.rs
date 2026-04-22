// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_aipsio::{AipsIo, AipsOpenOption, ByteOrder};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::data_type::CasacoreDataType;
use super::stman_array_file::{StManArrayFileReader, StManArrayFileWriter};
use super::table_control::{self, ColumnDescContents};

// ---------------------------------------------------------------------------
// Parsed column data from table.f<N>
// ---------------------------------------------------------------------------

/// Parsed data from a StManAipsIO data file (`table.fN`).
#[derive(Debug)]
pub(crate) struct StManAipsIOFile {
    pub name: String,
    pub seq_nr: u32,
    pub uniq_nr: u32,
    pub nrrow: u32,
    pub ncol: u32,
    pub data_types: Vec<CasacoreDataType>,
    pub columns: Vec<StManColumnData>,
}

/// Column data from StManAipsIO — either flat (scalar/fixed-shape) or
/// indirect (variable-shape, per-row).
#[derive(Debug)]
pub(crate) enum StManColumnData {
    /// Scalar or fixed-shape array column: flat column-major data.
    Flat(ColumnRawData),
    /// Variable-shape (indirect) array column: per-row `Option<Value>`.
    /// `None` entries represent undefined cells (file offset = 0).
    Indirect(Vec<Option<Value>>),
}

/// Raw column data in column-major order (one entry per row for scalars,
/// or flattened arrays for fixed-shape array columns).
#[derive(Debug, Clone)]
pub(crate) enum ColumnRawData {
    Bool(Vec<bool>),
    UInt8(Vec<u8>),
    Int16(Vec<i16>),
    UInt16(Vec<u16>),
    Int32(Vec<i32>),
    UInt32(Vec<u32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Complex32(Vec<casa_types::Complex32>),
    Complex64(Vec<casa_types::Complex64>),
    String(Vec<String>),
}

#[derive(Debug, Clone)]
struct ExtentLayout {
    row_start: usize,
    row_count: usize,
    values_start: u64,
    row_width_bytes: usize,
}

#[derive(Debug, Clone)]
struct BitExtentLayout {
    row_start: usize,
    row_count: usize,
    values_start: u64,
}

#[derive(Debug, Clone)]
enum SparseColumnLayout {
    ScalarBoolPacked {
        extents: Vec<BitExtentLayout>,
    },
    ScalarFixed {
        data_type: CasacoreDataType,
        extents: Vec<ExtentLayout>,
    },
    DirectArrayFixed {
        data_type: CasacoreDataType,
        extents: Vec<ExtentLayout>,
    },
    IndirectArrayOffsetsU32 {
        data_type: CasacoreDataType,
        extents: Vec<ExtentLayout>,
    },
}

#[derive(Debug, Clone)]
struct ObjectHeader {
    len: u32,
    type_name: String,
    version: u32,
    payload_start: u64,
    object_end: u64,
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

/// Column shape info needed to correctly decode array columns in StManAipsIO.
#[derive(Debug, Clone)]
pub(crate) struct StManColumnInfo {
    pub is_array: bool,
    /// Number of elements per row (product of shape dimensions). 0 for scalars.
    pub nrelem: usize,
}

pub(crate) fn read_stman_file(
    path: &Path,
    col_info: &[StManColumnInfo],
    byte_order: ByteOrder,
) -> Result<StManAipsIOFile, StorageError> {
    let mut io = AipsIo::open_with_order(path, AipsOpenOption::Old, byte_order)?;

    let version = io.getstart("StManAipsIO")?;

    let name = if version >= 2 {
        io.get_string()?
    } else {
        String::new()
    };

    let seq_nr = io.get_u32()?;
    let uniq_nr = io.get_u32()?;
    let nrrow = io.get_u32()?;
    let ncol = io.get_u32()?;

    // Read per-column data types
    let mut data_types = Vec::with_capacity(ncol as usize);
    for _ in 0..ncol {
        let dt_i32 = io.get_i32()?;
        let dt = CasacoreDataType::from_i32(dt_i32).ok_or_else(|| {
            StorageError::FormatMismatch(format!("unknown StManAipsIO column data type: {dt_i32}"))
        })?;
        data_types.push(dt);
    }

    // Read per-column data blocks, using get_next_type() to dispatch.
    let mut columns = Vec::with_capacity(ncol as usize);
    let big_endian = byte_order == ByteOrder::BigEndian;
    for (i, dt) in data_types.iter().enumerate() {
        let info = col_info.get(i).ok_or_else(|| {
            StorageError::FormatMismatch(format!("missing column info for StManAipsIO col {i}"))
        })?;

        // Peek at the next AipsIO object type to determine column kind.
        let next_type = io.get_next_type()?;
        let col_data = match next_type.as_str() {
            "StManColumnIndArrayAipsIO" => {
                read_stman_indirect_column(&mut io, *dt, path, big_endian)?
            }
            "StManColumnArrayAipsIO" => {
                StManColumnData::Flat(read_stman_array_column(&mut io, *dt, info.nrelem)?)
            }
            "StManColumnAipsIO" => {
                if *dt == CasacoreDataType::TpRecord {
                    read_stman_record_column(&mut io)?
                } else {
                    StManColumnData::Flat(read_stman_column(&mut io, *dt)?)
                }
            }
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "unexpected StManAipsIO column type: {other}"
                )));
            }
        };
        columns.push(col_data);
    }

    io.getend()?;
    io.close()?;

    Ok(StManAipsIOFile {
        name,
        seq_nr,
        uniq_nr,
        nrrow,
        ncol,
        data_types,
        columns,
    })
}

fn read_stman_column(io: &mut AipsIo, dt: CasacoreDataType) -> Result<ColumnRawData, StorageError> {
    let _version = io.getstart("StManColumnAipsIO")?;
    let nrval = io.get_u32()?;

    let data = if nrval > 0 {
        // C++ StManColumnAipsIO organises data into extent blocks.
        // Each extent has: u32(extent_count) + ios.put(extent_count, data).
        // ios.put writes a count prefix + typed data, which getnew reads.
        read_extent_data(io, dt, nrval)?
    } else {
        empty_column_data(dt)
    };

    io.getend()?;
    Ok(data)
}

/// Read a record column from StManColumnAipsIO.
///
/// C++ `StManColumnAipsIO::getData` for `TpRecord` reads individual
/// `TableRecord` objects (no count prefix, unlike typed columns). Each
/// extent contains `extent_count` serialized `Record` objects.
///
/// Corresponds to C++ `ScalarRecordColumnData` with `StManColumnAipsIO`.
fn read_stman_record_column(io: &mut AipsIo) -> Result<StManColumnData, StorageError> {
    let _version = io.getstart("StManColumnAipsIO")?;
    let nrval = io.get_u32()?;

    let mut records = Vec::with_capacity(nrval as usize);
    if nrval > 0 {
        let mut nrd = 0u32;
        while nrd < nrval {
            let mut extent_count = io.get_u32()?;
            if extent_count == 0 {
                extent_count = nrval - nrd;
            }
            for _ in 0..extent_count {
                let record = table_control::read_record(io)?;
                records.push(Some(Value::Record(record)));
            }
            nrd += extent_count;
        }
    }

    io.getend()?;
    Ok(StManColumnData::Indirect(records))
}

/// Read an array column wrapped in StManColumnArrayAipsIO > StManColumnAipsIO.
fn read_stman_array_column(
    io: &mut AipsIo,
    dt: CasacoreDataType,
    nrelem: usize,
) -> Result<ColumnRawData, StorageError> {
    let _arr_version = io.getstart("StManColumnArrayAipsIO")?;

    let _inner_version = io.getstart("StManColumnAipsIO")?;
    let nrval = io.get_u32()?;

    let data = if nrval > 0 {
        read_array_extent_data(io, dt, nrval, nrelem)?
    } else {
        empty_column_data(dt)
    };

    io.getend()?; // StManColumnAipsIO
    io.getend()?; // StManColumnArrayAipsIO
    Ok(data)
}

/// Marker value for file offsets > 2GB in indirect AipsIO columns.
const LARGE_OFFSET_MARKER: u32 = 2u32 * 1024 * 1024 * 1024 + 1;
const AIPSIO_TOP_LEVEL_MAGIC: u32 = 0xbebebebe;

/// Read a variable-shape (indirect) array column.
///
/// Corresponds to C++ `StManColumnIndArrayAipsIO::getFile`.
/// The AipsIO stream contains:
/// ```text
/// StManColumnIndArrayAipsIO v2 {
///     dtype: i32,
///     seqnr: i32,
///     StManColumnAipsIO {
///         nrval,
///         extents containing file offsets (not data)
///     }
/// }
/// ```
fn read_stman_indirect_column(
    io: &mut AipsIo,
    dt: CasacoreDataType,
    data_path: &Path,
    big_endian: bool,
) -> Result<StManColumnData, StorageError> {
    let ind_version = io.getstart("StManColumnIndArrayAipsIO")?;
    let _dtype_compat = io.get_i32()?; // backward-compat dtype
    let _seqnr = io.get_i32()?;

    // Read inner StManColumnAipsIO containing file offsets.
    let _inner_version = io.getstart("StManColumnAipsIO")?;
    let nrval = io.get_u32()?;

    let mut offsets: Vec<i64> = Vec::with_capacity(nrval as usize);
    if nrval > 0 {
        // Read extent blocks. Each extent: u32(extent_count) + getnew(count_prefix + u32 offsets).
        // But the indirect column stores offsets via putData, which writes directly without
        // count prefix — it uses the same extent structure as scalar columns.
        let mut nrd: u32 = 0;
        while nrd < nrval {
            let mut extent_count = io.get_u32()?;
            if extent_count == 0 {
                extent_count = nrval - nrd;
            }

            // Read file offsets for this extent.
            // The C++ putData writes u32 offsets (or marker+i64 for >2GB).
            // The C++ getData reads them via ios >> off pattern.
            // Since this is inside a scalar-column extent, it uses getnew (count-prefixed).
            // Actually, looking at the C++ code more carefully:
            // StManColumnAipsIO::putFile calls putData per extent, and putData/getData
            // are virtual — for indirect columns they write/read file offsets.
            // The getData reads raw u32 values from the AipsIO stream.
            let extent_offsets = read_indirect_extent_offsets(io, extent_count as usize)?;
            offsets.extend(extent_offsets);

            nrd += extent_count;
        }
    }

    io.getend()?; // StManColumnAipsIO
    io.getend()?; // StManColumnIndArrayAipsIO

    // Determine the array file path.
    // Version >= 2: shared file = data_path + "i"
    // Version <= 1: per-column file = data_path + "i" + seqnr (not commonly seen)
    let array_file_path = if ind_version >= 2 {
        let mut p = data_path.as_os_str().to_os_string();
        p.push("i");
        std::path::PathBuf::from(p)
    } else {
        let mut p = data_path.as_os_str().to_os_string();
        p.push(format!("i{}", _seqnr));
        std::path::PathBuf::from(p)
    };

    // Open the array file and read each row's data.
    let mut reader = StManArrayFileReader::open(&array_file_path, big_endian)?;
    let mut values = Vec::with_capacity(offsets.len());
    for &offset in &offsets {
        values.push(reader.read_array_at(offset, dt)?);
    }

    Ok(StManColumnData::Indirect(values))
}

/// Read file offsets from an indirect column extent.
///
/// C++ `StManColumnIndArrayAipsIO::getData` reads individual `ios >> uInt` per
/// row (no count prefix). For offsets > 2GB a marker u32 is followed by an i64.
fn read_indirect_extent_offsets(io: &mut AipsIo, count: usize) -> Result<Vec<i64>, StorageError> {
    let mut offsets = Vec::with_capacity(count);
    for _ in 0..count {
        let off = io.get_u32()?;
        if off == LARGE_OFFSET_MARKER {
            let big = io.get_i64()?;
            offsets.push(big);
        } else {
            offsets.push(off as i64);
        }
    }
    Ok(offsets)
}

/// Read array column data organized in extent blocks.
/// Each extent has: u32(extent_count) + u32(total_elements) + per-row raw data.
fn read_array_extent_data(
    io: &mut AipsIo,
    dt: CasacoreDataType,
    nrval: u32,
    nrelem: usize,
) -> Result<ColumnRawData, StorageError> {
    let mut nrd: u32 = 0;
    let mut result = empty_column_data(dt);

    while nrd < nrval {
        let mut extent_count = io.get_u32()?;
        if extent_count == 0 {
            extent_count = nrval - nrd;
        }
        if extent_count + nrd > nrval {
            return Err(StorageError::FormatMismatch(format!(
                "StManColumnArrayAipsIO: extent overrun ({extent_count}+{nrd} > {nrval})"
            )));
        }

        // Read u32(total_elements) — sanity check, then per-row data
        let _total_elements = io.get_u32()?;
        let total_values = extent_count as usize * nrelem;

        let extent = read_raw_typed_values(io, dt, total_values)?;
        append_column_data(&mut result, extent);
        nrd += extent_count;
    }

    Ok(result)
}

/// Read typed values without a count prefix (used for array column data).
fn read_raw_typed_values(
    io: &mut AipsIo,
    dt: CasacoreDataType,
    count: usize,
) -> Result<ColumnRawData, StorageError> {
    match dt {
        CasacoreDataType::TpBool => {
            let mut v = vec![false; count];
            io.get_bool_into(&mut v)?;
            Ok(ColumnRawData::Bool(v))
        }
        CasacoreDataType::TpUChar => {
            let mut v = vec![0u8; count];
            io.get_u8_into(&mut v)?;
            Ok(ColumnRawData::UInt8(v))
        }
        CasacoreDataType::TpShort => {
            let mut v = vec![0i16; count];
            io.get_i16_into(&mut v)?;
            Ok(ColumnRawData::Int16(v))
        }
        CasacoreDataType::TpUShort => {
            let mut v = vec![0u16; count];
            io.get_u16_into(&mut v)?;
            Ok(ColumnRawData::UInt16(v))
        }
        CasacoreDataType::TpInt => {
            let mut v = vec![0i32; count];
            io.get_i32_into(&mut v)?;
            Ok(ColumnRawData::Int32(v))
        }
        CasacoreDataType::TpUInt => {
            let mut v = vec![0u32; count];
            io.get_u32_into(&mut v)?;
            Ok(ColumnRawData::UInt32(v))
        }
        CasacoreDataType::TpFloat => {
            let mut v = vec![0f32; count];
            io.get_f32_into(&mut v)?;
            Ok(ColumnRawData::Float32(v))
        }
        CasacoreDataType::TpDouble => {
            let mut v = vec![0f64; count];
            io.get_f64_into(&mut v)?;
            Ok(ColumnRawData::Float64(v))
        }
        CasacoreDataType::TpComplex => {
            let mut v = vec![casa_types::Complex32::new(0.0, 0.0); count];
            io.get_complex32_into(&mut v)?;
            Ok(ColumnRawData::Complex32(v))
        }
        CasacoreDataType::TpDComplex => {
            let mut v = vec![casa_types::Complex64::new(0.0, 0.0); count];
            io.get_complex64_into(&mut v)?;
            Ok(ColumnRawData::Complex64(v))
        }
        CasacoreDataType::TpString => {
            let mut v = Vec::with_capacity(count);
            for _ in 0..count {
                v.push(io.get_string()?);
            }
            Ok(ColumnRawData::String(v))
        }
        CasacoreDataType::TpInt64 => {
            let mut v = vec![0i64; count];
            io.get_i64_into(&mut v)?;
            Ok(ColumnRawData::Int64(v))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported StManAipsIO array column type: {dt:?}"
        ))),
    }
}

/// Read column data organized in extent blocks as written by C++ StManColumnAipsIO.
/// Format per extent: u32(extent_count) + getnew(count_prefix + typed_data).
fn read_extent_data(
    io: &mut AipsIo,
    dt: CasacoreDataType,
    nrval: u32,
) -> Result<ColumnRawData, StorageError> {
    let mut nrd: u32 = 0;
    let mut result = empty_column_data(dt);

    while nrd < nrval {
        let mut extent_count = io.get_u32()?;
        if extent_count == 0 {
            extent_count = nrval - nrd;
        }
        if extent_count + nrd > nrval {
            return Err(StorageError::FormatMismatch(format!(
                "StManColumnAipsIO: extent overrun ({extent_count}+{nrd} > {nrval})"
            )));
        }

        // getnew reads the count prefix (written by ios.put) + typed data
        let extent = read_typed_extent(io, dt)?;
        append_column_data(&mut result, extent);
        nrd += extent_count;
    }

    Ok(result)
}

/// Read a single extent's typed data via getnew (reads count prefix + data).
fn read_typed_extent(io: &mut AipsIo, dt: CasacoreDataType) -> Result<ColumnRawData, StorageError> {
    match dt {
        CasacoreDataType::TpBool => Ok(ColumnRawData::Bool(io.getnew_bool()?)),
        CasacoreDataType::TpUChar => Ok(ColumnRawData::UInt8(io.getnew_u8()?)),
        CasacoreDataType::TpShort => Ok(ColumnRawData::Int16(io.getnew_i16()?)),
        CasacoreDataType::TpUShort => Ok(ColumnRawData::UInt16(io.getnew_u16()?)),
        CasacoreDataType::TpInt => Ok(ColumnRawData::Int32(io.getnew_i32()?)),
        CasacoreDataType::TpUInt => Ok(ColumnRawData::UInt32(io.getnew_u32()?)),
        CasacoreDataType::TpFloat => Ok(ColumnRawData::Float32(io.getnew_f32()?)),
        CasacoreDataType::TpDouble => Ok(ColumnRawData::Float64(io.getnew_f64()?)),
        CasacoreDataType::TpComplex => Ok(ColumnRawData::Complex32(io.getnew_complex32()?)),
        CasacoreDataType::TpDComplex => Ok(ColumnRawData::Complex64(io.getnew_complex64()?)),
        CasacoreDataType::TpString => Ok(ColumnRawData::String(io.getnew_string()?)),
        CasacoreDataType::TpInt64 => Ok(ColumnRawData::Int64(io.getnew_i64()?)),
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported StManAipsIO column type: {dt:?}"
        ))),
    }
}

/// Append extent data to the accumulated column data.
fn append_column_data(target: &mut ColumnRawData, source: ColumnRawData) {
    match (target, source) {
        (ColumnRawData::Bool(t), ColumnRawData::Bool(s)) => t.extend(s),
        (ColumnRawData::UInt8(t), ColumnRawData::UInt8(s)) => t.extend(s),
        (ColumnRawData::Int16(t), ColumnRawData::Int16(s)) => t.extend(s),
        (ColumnRawData::UInt16(t), ColumnRawData::UInt16(s)) => t.extend(s),
        (ColumnRawData::Int32(t), ColumnRawData::Int32(s)) => t.extend(s),
        (ColumnRawData::UInt32(t), ColumnRawData::UInt32(s)) => t.extend(s),
        (ColumnRawData::Int64(t), ColumnRawData::Int64(s)) => t.extend(s),
        (ColumnRawData::Float32(t), ColumnRawData::Float32(s)) => t.extend(s),
        (ColumnRawData::Float64(t), ColumnRawData::Float64(s)) => t.extend(s),
        (ColumnRawData::Complex32(t), ColumnRawData::Complex32(s)) => t.extend(s),
        (ColumnRawData::Complex64(t), ColumnRawData::Complex64(s)) => t.extend(s),
        (ColumnRawData::String(t), ColumnRawData::String(s)) => t.extend(s),
        _ => {} // type mismatch — should not happen
    }
}

fn empty_column_data(dt: CasacoreDataType) -> ColumnRawData {
    match dt {
        CasacoreDataType::TpBool => ColumnRawData::Bool(vec![]),
        CasacoreDataType::TpUChar => ColumnRawData::UInt8(vec![]),
        CasacoreDataType::TpShort => ColumnRawData::Int16(vec![]),
        CasacoreDataType::TpUShort => ColumnRawData::UInt16(vec![]),
        CasacoreDataType::TpInt => ColumnRawData::Int32(vec![]),
        CasacoreDataType::TpUInt => ColumnRawData::UInt32(vec![]),
        CasacoreDataType::TpFloat => ColumnRawData::Float32(vec![]),
        CasacoreDataType::TpDouble => ColumnRawData::Float64(vec![]),
        CasacoreDataType::TpComplex => ColumnRawData::Complex32(vec![]),
        CasacoreDataType::TpDComplex => ColumnRawData::Complex64(vec![]),
        CasacoreDataType::TpString => ColumnRawData::String(vec![]),
        CasacoreDataType::TpInt64 => ColumnRawData::Int64(vec![]),
        _ => ColumnRawData::Int32(vec![]),
    }
}

// ---------------------------------------------------------------------------
// Sparse patch path
// ---------------------------------------------------------------------------

pub(crate) fn save_stman_file_rows_in_place(
    path: &Path,
    columns: &[ColumnDescContents],
    changed_values: &HashMap<&str, Vec<(usize, Option<Value>)>>,
    byte_order: ByteOrder,
) -> Result<bool, StorageError> {
    if changed_values.is_empty() {
        return Ok(true);
    }

    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    if layouts.len() != columns.len() {
        return Ok(false);
    }

    let mut main_file = OpenOptions::new().read(true).write(true).open(path)?;
    let array_file_path = indirect_array_file_path(path);
    let array_file_len = if array_file_path.exists() {
        Some(std::fs::metadata(&array_file_path)?.len())
    } else {
        None
    };
    let mut array_writer: Option<StManArrayFileWriter> = None;

    for (col_index, col_desc) in columns.iter().enumerate() {
        let Some(row_values) = changed_values.get(col_desc.col_name.as_str()) else {
            continue;
        };
        let Some(layout) = layouts[col_index].as_ref() else {
            return Ok(false);
        };
        match layout {
            SparseColumnLayout::ScalarBoolPacked { extents } => {
                for &(row_index, ref value) in row_values {
                    let Some(Value::Scalar(ScalarValue::Bool(flag))) = value else {
                        return Ok(false);
                    };
                    let (byte_pos, bit_index) = locate_bit_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(byte_pos))?;
                    let mut current = [0u8; 1];
                    main_file.read_exact(&mut current)?;
                    if *flag {
                        current[0] |= 1 << bit_index;
                    } else {
                        current[0] &= !(1 << bit_index);
                    }
                    main_file.seek(SeekFrom::Start(byte_pos))?;
                    main_file.write_all(&current)?;
                }
            }
            SparseColumnLayout::IndirectArrayOffsetsU32 { data_type, extents } => {
                let existing_len = array_file_len.unwrap_or(0);
                if existing_len >= LARGE_OFFSET_MARKER as u64 {
                    return Ok(false);
                }
                let writer = match array_writer.as_mut() {
                    Some(writer) => writer,
                    None => {
                        array_writer = Some(StManArrayFileWriter::open_append(
                            &array_file_path,
                            byte_order == ByteOrder::BigEndian,
                        )?);
                        array_writer.as_mut().expect("writer just created")
                    }
                };
                for &(row_index, ref value) in row_values {
                    let offset = match value {
                        Some(value @ Value::Array(_)) => writer.write_array(value, *data_type)?,
                        None => 0,
                        _ => return Ok(false),
                    };
                    let offset_u32 = u32::try_from(offset).map_err(|_| {
                        StorageError::FormatMismatch(format!(
                            "sparse StManAipsIO offset exceeds u32 for column {} row {}",
                            col_desc.col_name, row_index
                        ))
                    })?;
                    let slot = locate_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(slot))?;
                    main_file.write_all(&offset_u32.to_be_bytes())?;
                }
            }
            SparseColumnLayout::ScalarFixed { data_type, extents } => {
                for &(row_index, ref value) in row_values {
                    let Some(Value::Scalar(scalar)) = value else {
                        return Ok(false);
                    };
                    let encoded = encode_fixed_scalar_value(*data_type, scalar)?;
                    let slot = locate_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(slot))?;
                    main_file.write_all(&encoded)?;
                }
            }
            SparseColumnLayout::DirectArrayFixed { data_type, extents } => {
                for &(row_index, ref value) in row_values {
                    let Some(Value::Array(array)) = value else {
                        return Ok(false);
                    };
                    let encoded = encode_fixed_array_row(*data_type, array)?;
                    let slot = locate_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(slot))?;
                    main_file.write_all(&encoded)?;
                }
            }
        }
    }

    if let Some(writer) = array_writer.as_mut() {
        writer.finish()?;
    }
    main_file.flush()?;
    Ok(true)
}

pub(crate) fn read_stman_array_column_rows(
    path: &Path,
    columns: &[ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<ArrayValue>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(SparseColumnLayout::IndirectArrayOffsetsU32 { data_type, extents }) = layouts
        .get(target_col_idx)
        .and_then(|layout| layout.as_ref())
    else {
        return Ok(None);
    };

    let array_file_path = indirect_array_file_path(path);
    if std::fs::metadata(&array_file_path)
        .map(|meta| meta.len())
        .unwrap_or(0)
        >= LARGE_OFFSET_MARKER as u64
    {
        return Ok(None);
    }

    let mut main_file = std::fs::File::open(path)?;
    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);

    let mut offsets = vec![0u32; selected_rows.len()];
    for (row_index, out_idx) in requests {
        let slot = locate_extent_slot(extents, row_index)?;
        main_file.seek(SeekFrom::Start(slot))?;
        offsets[out_idx] = read_be_u32(&mut main_file)?;
    }

    let mut reader =
        StManArrayFileReader::open(&array_file_path, byte_order == ByteOrder::BigEndian)?;
    let mut values = Vec::with_capacity(offsets.len());
    for offset in offsets {
        let value = reader
            .read_array_at(offset as i64, *data_type)?
            .map(|value| match value {
                Value::Array(array) => Ok(array),
                other => Err(StorageError::FormatMismatch(format!(
                    "expected array value in indirect StManAipsIO column, found {:?}",
                    other.kind()
                ))),
            })
            .transpose()?;
        values.push(value);
    }

    Ok(Some(values))
}

pub(crate) fn read_stman_scalar_column_rows(
    path: &Path,
    columns: &[ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(layout) = layouts
        .get(target_col_idx)
        .and_then(|layout| layout.as_ref())
    else {
        return Ok(None);
    };
    let column = columns.get(target_col_idx).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "target StManAipsIO scalar column index {target_col_idx} is out of range"
        ))
    })?;

    let mut main_file = std::fs::File::open(path)?;
    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);

    let mut values = vec![None; selected_rows.len()];
    match layout {
        SparseColumnLayout::ScalarBoolPacked { extents } => {
            for (row_index, out_idx) in requests {
                let (slot, bit_offset) = locate_bit_extent_slot(extents, row_index)?;
                main_file.seek(SeekFrom::Start(slot))?;
                let mut byte = [0u8; 1];
                main_file.read_exact(&mut byte)?;
                let scalar = ScalarValue::Bool(((byte[0] >> bit_offset) & 1) != 0);
                values[out_idx] = if (column.option & 2) != 0
                    && scalar_value_is_default(
                        &Value::Scalar(scalar.clone()),
                        column.require_primitive_type()?,
                    ) {
                    None
                } else {
                    Some(scalar)
                };
            }
        }
        SparseColumnLayout::ScalarFixed { data_type, extents } => {
            let row_width = scalar_fixed_width_bytes(*data_type).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "unsupported direct StManAipsIO scalar type {data_type:?}"
                ))
            })?;
            let mut raw = vec![0u8; row_width];
            for (row_index, out_idx) in requests {
                let slot = locate_extent_slot(extents, row_index)?;
                main_file.seek(SeekFrom::Start(slot))?;
                main_file.read_exact(&mut raw)?;
                let scalar = decode_fixed_scalar_value(*data_type, &raw)?;
                values[out_idx] = if (column.option & 2) != 0
                    && scalar_value_is_default(
                        &Value::Scalar(scalar.clone()),
                        column.require_primitive_type()?,
                    ) {
                    None
                } else {
                    Some(scalar)
                };
            }
        }
        _ => return Ok(None),
    }

    Ok(Some(values))
}

pub(crate) fn read_stman_scalar_column(
    path: &Path,
    columns: &[ColumnDescContents],
    target_col_idx: usize,
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(layout) = layouts
        .get(target_col_idx)
        .and_then(|layout| layout.as_ref())
    else {
        return Ok(None);
    };
    let column = columns.get(target_col_idx).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "target StManAipsIO scalar column index {target_col_idx} is out of range"
        ))
    })?;

    let mut file = std::fs::File::open(path)?;
    let mut values = Vec::new();
    match layout {
        SparseColumnLayout::ScalarBoolPacked { extents } => {
            for extent in extents {
                file.seek(SeekFrom::Start(extent.values_start))?;
                let mut packed = vec![0u8; extent.row_count.div_ceil(8)];
                file.read_exact(&mut packed)?;
                for row_offset in 0..extent.row_count {
                    let byte = packed[row_offset / 8];
                    let flag = ((byte >> (row_offset % 8)) & 1) != 0;
                    let scalar = ScalarValue::Bool(flag);
                    if (column.option & 2) != 0
                        && scalar_value_is_default(
                            &Value::Scalar(scalar.clone()),
                            column.require_primitive_type()?,
                        )
                    {
                        values.push(None);
                    } else {
                        values.push(Some(scalar));
                    }
                }
            }
        }
        SparseColumnLayout::ScalarFixed { data_type, extents } => {
            for extent in extents {
                file.seek(SeekFrom::Start(extent.values_start))?;
                let mut raw = vec![0u8; extent.row_count * extent.row_width_bytes];
                file.read_exact(&mut raw)?;
                for chunk in raw.chunks_exact(extent.row_width_bytes) {
                    let scalar = decode_fixed_scalar_value(*data_type, chunk)?;
                    if (column.option & 2) != 0
                        && scalar_value_is_default(
                            &Value::Scalar(scalar.clone()),
                            column.require_primitive_type()?,
                        )
                    {
                        values.push(None);
                    } else {
                        values.push(Some(scalar));
                    }
                }
            }
        }
        _ => return Ok(None),
    }
    Ok(Some(values))
}

fn parse_sparse_column_layouts(
    path: &Path,
    columns: &[ColumnDescContents],
    byte_order: ByteOrder,
) -> Result<Vec<Option<SparseColumnLayout>>, StorageError> {
    if byte_order != ByteOrder::BigEndian {
        return Ok(vec![None; columns.len()]);
    }

    let mut file = std::fs::File::open(path)?;
    let magic = read_be_u32(&mut file)?;
    if magic != AIPSIO_TOP_LEVEL_MAGIC {
        return Err(StorageError::FormatMismatch(format!(
            "StManAipsIO file missing top-level magic: {magic:#x}"
        )));
    }
    let top = read_object_header(&mut file)?;
    if top.type_name != "StManAipsIO" {
        return Err(StorageError::FormatMismatch(format!(
            "expected StManAipsIO top-level object, found {}",
            top.type_name
        )));
    }

    let _name = read_be_string(&mut file)?;
    let _seq_nr = read_be_u32(&mut file)?;
    let _uniq_nr = read_be_u32(&mut file)?;
    let nrrow = read_be_u32(&mut file)? as usize;
    let ncol = read_be_u32(&mut file)? as usize;
    if ncol != columns.len() {
        return Err(StorageError::FormatMismatch(format!(
            "StManAipsIO column count mismatch: file has {ncol}, expected {}",
            columns.len()
        )));
    }

    let mut data_types = Vec::with_capacity(ncol);
    for _ in 0..ncol {
        let dt_i32 = read_be_i32(&mut file)?;
        let dt = CasacoreDataType::from_i32(dt_i32).ok_or_else(|| {
            StorageError::FormatMismatch(format!("unknown StManAipsIO dtype tag: {dt_i32}"))
        })?;
        data_types.push(dt);
    }

    let mut layouts = Vec::with_capacity(ncol);
    for (col_desc, data_type) in columns.iter().zip(data_types) {
        let column_header = read_object_header(&mut file)?;
        let layout = match column_header.type_name.as_str() {
            "StManColumnIndArrayAipsIO" => {
                let _compat_dtype = read_be_i32(&mut file)?;
                let _seqnr = read_be_i32(&mut file)?;
                let inner = read_object_header(&mut file)?;
                if inner.type_name != "StManColumnAipsIO" {
                    None
                } else {
                    let stored_nrrow = read_be_u32(&mut file)? as usize;
                    if stored_nrrow != nrrow {
                        None
                    } else {
                        parse_indirect_offsets_layout(&mut file, stored_nrrow, data_type)?
                    }
                }
            }
            "StManColumnAipsIO" => {
                let stored_nrrow = read_be_u32(&mut file)? as usize;
                if stored_nrrow != nrrow {
                    None
                } else {
                    parse_scalar_layout(&mut file, stored_nrrow, data_type)?
                }
            }
            "StManColumnArrayAipsIO" => {
                let inner = read_object_header(&mut file)?;
                if inner.type_name != "StManColumnAipsIO" {
                    None
                } else {
                    let stored_nrrow = read_be_u32(&mut file)? as usize;
                    if stored_nrrow != nrrow {
                        None
                    } else {
                        parse_direct_array_layout(&mut file, stored_nrrow, col_desc, data_type)?
                    }
                }
            }
            _ => None,
        };
        file.seek(SeekFrom::Start(column_header.object_end))?;
        layouts.push(layout);
    }

    file.seek(SeekFrom::Start(top.object_end))?;
    Ok(layouts)
}

fn parse_indirect_offsets_layout(
    file: &mut std::fs::File,
    nrrow: usize,
    data_type: CasacoreDataType,
) -> Result<Option<SparseColumnLayout>, StorageError> {
    let mut extents = Vec::new();
    let mut row_start = 0usize;
    while row_start < nrrow {
        let raw_count = read_be_u32(file)? as usize;
        let row_count = if raw_count == 0 {
            nrrow - row_start
        } else {
            raw_count
        };
        let values_start = file.stream_position()?;
        file.seek(SeekFrom::Current(
            (row_count * std::mem::size_of::<u32>()) as i64,
        ))?;
        extents.push(ExtentLayout {
            row_start,
            row_count,
            values_start,
            row_width_bytes: std::mem::size_of::<u32>(),
        });
        row_start += row_count;
    }
    Ok(Some(SparseColumnLayout::IndirectArrayOffsetsU32 {
        data_type,
        extents,
    }))
}

fn parse_scalar_layout(
    file: &mut std::fs::File,
    nrrow: usize,
    data_type: CasacoreDataType,
) -> Result<Option<SparseColumnLayout>, StorageError> {
    if data_type == CasacoreDataType::TpBool {
        let mut extents = Vec::new();
        let mut row_start = 0usize;
        while row_start < nrrow {
            let raw_count = read_be_u32(file)? as usize;
            let row_count = if raw_count == 0 {
                nrrow - row_start
            } else {
                raw_count
            };
            let stored_count = read_be_u32(file)? as usize;
            if stored_count != row_count {
                return Ok(None);
            }
            let values_start = file.stream_position()?;
            let packed_bytes = row_count.div_ceil(8);
            file.seek(SeekFrom::Current(packed_bytes as i64))?;
            extents.push(BitExtentLayout {
                row_start,
                row_count,
                values_start,
            });
            row_start += row_count;
        }
        return Ok(Some(SparseColumnLayout::ScalarBoolPacked { extents }));
    }

    let Some(row_width_bytes) = scalar_fixed_width_bytes(data_type) else {
        return Ok(None);
    };
    let mut extents = Vec::new();
    let mut row_start = 0usize;
    while row_start < nrrow {
        let raw_count = read_be_u32(file)? as usize;
        let row_count = if raw_count == 0 {
            nrrow - row_start
        } else {
            raw_count
        };
        let stored_count = read_be_u32(file)? as usize;
        if stored_count != row_count {
            return Ok(None);
        }
        let values_start = file.stream_position()?;
        file.seek(SeekFrom::Current((row_count * row_width_bytes) as i64))?;
        extents.push(ExtentLayout {
            row_start,
            row_count,
            values_start,
            row_width_bytes,
        });
        row_start += row_count;
    }
    Ok(Some(SparseColumnLayout::ScalarFixed { data_type, extents }))
}

fn parse_direct_array_layout(
    file: &mut std::fs::File,
    nrrow: usize,
    col_desc: &ColumnDescContents,
    data_type: CasacoreDataType,
) -> Result<Option<SparseColumnLayout>, StorageError> {
    let Some(elem_width) = scalar_fixed_width_bytes(data_type) else {
        return Ok(None);
    };
    let elements_per_row: usize = col_desc.shape.iter().map(|&dim| dim as usize).product();
    if elements_per_row == 0 {
        return Ok(None);
    }
    let row_width_bytes = elements_per_row.checked_mul(elem_width).ok_or_else(|| {
        StorageError::FormatMismatch("direct-array row width overflow".to_string())
    })?;
    let mut extents = Vec::new();
    let mut row_start = 0usize;
    while row_start < nrrow {
        let raw_count = read_be_u32(file)? as usize;
        let row_count = if raw_count == 0 {
            nrrow - row_start
        } else {
            raw_count
        };
        let total_elements = read_be_u32(file)? as usize;
        if total_elements != row_count * elements_per_row {
            return Ok(None);
        }
        let values_start = file.stream_position()?;
        file.seek(SeekFrom::Current((row_count * row_width_bytes) as i64))?;
        extents.push(ExtentLayout {
            row_start,
            row_count,
            values_start,
            row_width_bytes,
        });
        row_start += row_count;
    }
    Ok(Some(SparseColumnLayout::DirectArrayFixed {
        data_type,
        extents,
    }))
}

fn read_object_header(file: &mut std::fs::File) -> Result<ObjectHeader, StorageError> {
    let len_pos = file.stream_position()?;
    let len = read_be_u32(file)?;
    let type_name = read_be_string(file)?;
    let version = read_be_u32(file)?;
    let payload_start = file.stream_position()?;
    let object_end = len_pos + len as u64;
    Ok(ObjectHeader {
        len,
        type_name,
        version,
        payload_start,
        object_end,
    })
}

fn locate_extent_slot(extents: &[ExtentLayout], row_index: usize) -> Result<u64, StorageError> {
    for extent in extents {
        if row_index >= extent.row_start && row_index < extent.row_start + extent.row_count {
            return Ok(extent.values_start
                + ((row_index - extent.row_start) * extent.row_width_bytes) as u64);
        }
    }
    Err(StorageError::FormatMismatch(format!(
        "row {row_index} is outside sparse-patch extents"
    )))
}

fn locate_bit_extent_slot(
    extents: &[BitExtentLayout],
    row_index: usize,
) -> Result<(u64, usize), StorageError> {
    for extent in extents {
        if row_index >= extent.row_start && row_index < extent.row_start + extent.row_count {
            let row_offset = row_index - extent.row_start;
            return Ok((
                extent.values_start + (row_offset / 8) as u64,
                row_offset % 8,
            ));
        }
    }
    Err(StorageError::FormatMismatch(format!(
        "row {row_index} is outside sparse-patch bit extents"
    )))
}

fn scalar_fixed_width_bytes(data_type: CasacoreDataType) -> Option<usize> {
    match data_type {
        CasacoreDataType::TpBool | CasacoreDataType::TpUChar | CasacoreDataType::TpChar => Some(1),
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => Some(2),
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => Some(4),
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => {
            Some(8)
        }
        CasacoreDataType::TpDComplex => Some(16),
        _ => None,
    }
}

fn decode_fixed_scalar_value(
    data_type: CasacoreDataType,
    bytes: &[u8],
) -> Result<ScalarValue, StorageError> {
    let scalar = match data_type {
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => {
            ScalarValue::UInt8(*bytes.first().ok_or_else(|| {
                StorageError::FormatMismatch("missing u8 scalar bytes".to_string())
            })?)
        }
        CasacoreDataType::TpShort => {
            ScalarValue::Int16(i16::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid i16 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpUShort => {
            ScalarValue::UInt16(u16::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid u16 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpInt => {
            ScalarValue::Int32(i32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid i32 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpUInt => {
            ScalarValue::UInt32(u32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid u32 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpInt64 => {
            ScalarValue::Int64(i64::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid i64 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpFloat => {
            ScalarValue::Float32(f32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid f32 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpDouble => {
            ScalarValue::Float64(f64::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid f64 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpComplex => {
            if bytes.len() != 8 {
                return Err(StorageError::FormatMismatch(
                    "invalid complex32 scalar width".to_string(),
                ));
            }
            ScalarValue::Complex32(casa_types::Complex32 {
                re: f32::from_be_bytes(bytes[0..4].try_into().expect("slice width checked")),
                im: f32::from_be_bytes(bytes[4..8].try_into().expect("slice width checked")),
            })
        }
        CasacoreDataType::TpDComplex => {
            if bytes.len() != 16 {
                return Err(StorageError::FormatMismatch(
                    "invalid complex64 scalar width".to_string(),
                ));
            }
            ScalarValue::Complex64(casa_types::Complex64 {
                re: f64::from_be_bytes(bytes[0..8].try_into().expect("slice width checked")),
                im: f64::from_be_bytes(bytes[8..16].try_into().expect("slice width checked")),
            })
        }
        other => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported direct StManAipsIO scalar decode for {other:?}"
            )));
        }
    };
    Ok(scalar)
}

fn encode_fixed_scalar_value(
    data_type: CasacoreDataType,
    value: &ScalarValue,
) -> Result<Vec<u8>, StorageError> {
    let bytes = match (data_type, value) {
        (CasacoreDataType::TpBool, ScalarValue::Bool(v)) => vec![u8::from(*v)],
        (CasacoreDataType::TpUChar, ScalarValue::UInt8(v))
        | (CasacoreDataType::TpChar, ScalarValue::UInt8(v)) => vec![*v],
        (CasacoreDataType::TpShort, ScalarValue::Int16(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpUShort, ScalarValue::UInt16(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpInt, ScalarValue::Int32(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpUInt, ScalarValue::UInt32(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpInt64, ScalarValue::Int64(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpFloat, ScalarValue::Float32(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpDouble, ScalarValue::Float64(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpComplex, ScalarValue::Complex32(v)) => {
            let mut bytes = Vec::with_capacity(8);
            bytes.extend_from_slice(&v.re.to_be_bytes());
            bytes.extend_from_slice(&v.im.to_be_bytes());
            bytes
        }
        (CasacoreDataType::TpDComplex, ScalarValue::Complex64(v)) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&v.re.to_be_bytes());
            bytes.extend_from_slice(&v.im.to_be_bytes());
            bytes
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported sparse scalar patch for type {data_type:?} value {value:?}"
            )));
        }
    };
    Ok(bytes)
}

fn encode_fixed_array_row(
    data_type: CasacoreDataType,
    value: &ArrayValue,
) -> Result<Vec<u8>, StorageError> {
    let mut encoded = Vec::new();
    match (data_type, value) {
        (CasacoreDataType::TpBool, _) | (_, ArrayValue::String(_)) => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported sparse direct-array patch for {data_type:?}"
            )));
        }
        (CasacoreDataType::TpUChar, ArrayValue::UInt8(arr))
        | (CasacoreDataType::TpChar, ArrayValue::UInt8(arr)) => {
            encoded.extend(fortran_flat_iter(arr))
        }
        (CasacoreDataType::TpShort, ArrayValue::Int16(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpUShort, ArrayValue::UInt16(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpInt, ArrayValue::Int32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpUInt, ArrayValue::UInt32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpInt64, ArrayValue::Int64(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpFloat, ArrayValue::Float32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpDouble, ArrayValue::Float64(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpComplex, ArrayValue::Complex32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.re.to_be_bytes());
                encoded.extend_from_slice(&value.im.to_be_bytes());
            }
        }
        (CasacoreDataType::TpDComplex, ArrayValue::Complex64(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.re.to_be_bytes());
                encoded.extend_from_slice(&value.im.to_be_bytes());
            }
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported sparse direct-array patch for type {data_type:?}"
            )));
        }
    }
    Ok(encoded)
}

fn indirect_array_file_path(path: &Path) -> PathBuf {
    let mut array_path = path.as_os_str().to_os_string();
    array_path.push("i");
    PathBuf::from(array_path)
}

fn read_be_u32(file: &mut std::fs::File) -> Result<u32, StorageError> {
    let mut bytes = [0u8; 4];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_be_i32(file: &mut std::fs::File) -> Result<i32, StorageError> {
    let mut bytes = [0u8; 4];
    file.read_exact(&mut bytes)?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_be_string(file: &mut std::fs::File) -> Result<String, StorageError> {
    let length = read_be_u32(file)? as usize;
    let mut bytes = vec![0u8; length];
    file.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|error| {
        StorageError::FormatMismatch(format!("invalid UTF-8 in AipsIO string: {error}"))
    })
}

// ---------------------------------------------------------------------------
// Column-major -> row-major conversion
// ---------------------------------------------------------------------------

/// Extract the Value for a single row from column-major raw data.
/// For scalar columns: one value per row.
/// For fixed-shape array columns: product(shape) values per row, in Fortran order.
pub(crate) fn extract_row_value(
    raw: &ColumnRawData,
    col_desc: &ColumnDescContents,
    row: usize,
    nrrow: usize,
) -> Result<Value, StorageError> {
    if col_desc.is_array && col_desc.nrdim > 0 && !col_desc.shape.is_empty() {
        extract_fixed_array_value(raw, col_desc, row, nrrow)
    } else {
        extract_scalar_value(raw, row)
    }
}

fn extract_scalar_value(raw: &ColumnRawData, row: usize) -> Result<Value, StorageError> {
    match raw {
        ColumnRawData::Bool(v) => Ok(Value::Scalar(ScalarValue::Bool(v[row]))),
        ColumnRawData::UInt8(v) => Ok(Value::Scalar(ScalarValue::UInt8(v[row]))),
        ColumnRawData::Int16(v) => Ok(Value::Scalar(ScalarValue::Int16(v[row]))),
        ColumnRawData::UInt16(v) => Ok(Value::Scalar(ScalarValue::UInt16(v[row]))),
        ColumnRawData::Int32(v) => Ok(Value::Scalar(ScalarValue::Int32(v[row]))),
        ColumnRawData::UInt32(v) => Ok(Value::Scalar(ScalarValue::UInt32(v[row]))),
        ColumnRawData::Int64(v) => Ok(Value::Scalar(ScalarValue::Int64(v[row]))),
        ColumnRawData::Float32(v) => Ok(Value::Scalar(ScalarValue::Float32(v[row]))),
        ColumnRawData::Float64(v) => Ok(Value::Scalar(ScalarValue::Float64(v[row]))),
        ColumnRawData::Complex32(v) => Ok(Value::Scalar(ScalarValue::Complex32(v[row]))),
        ColumnRawData::Complex64(v) => Ok(Value::Scalar(ScalarValue::Complex64(v[row]))),
        ColumnRawData::String(v) => Ok(Value::Scalar(ScalarValue::String(v[row].clone()))),
    }
}

fn extract_fixed_array_value(
    raw: &ColumnRawData,
    col_desc: &ColumnDescContents,
    row: usize,
    _nrrow: usize,
) -> Result<Value, StorageError> {
    let shape: Vec<usize> = col_desc.shape.iter().map(|&s| s as usize).collect();
    let elements_per_row: usize = shape.iter().product();
    let offset = row * elements_per_row;

    // Data is in Fortran (column-major) order. Create array with Fortran layout.
    let array_value = match raw {
        ColumnRawData::Bool(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Bool(arr)
        }
        ColumnRawData::UInt8(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::UInt8(arr)
        }
        ColumnRawData::UInt16(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::UInt16(arr)
        }
        ColumnRawData::Int16(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Int16(arr)
        }
        ColumnRawData::UInt32(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::UInt32(arr)
        }
        ColumnRawData::Int32(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Int32(arr)
        }
        ColumnRawData::Int64(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Int64(arr)
        }
        ColumnRawData::Float32(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Float32(arr)
        }
        ColumnRawData::Float64(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Float64(arr)
        }
        ColumnRawData::Complex32(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Complex32(arr)
        }
        ColumnRawData::Complex64(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Complex64(arr)
        }
        ColumnRawData::String(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::String(arr)
        }
    };

    Ok(Value::Array(array_value))
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

pub(crate) fn write_stman_file(
    path: &Path,
    columns: &[ColumnDescContents],
    rows: &[casa_types::RecordValue],
    byte_order: ByteOrder,
) -> Result<(), StorageError> {
    let nrrow = rows.len() as u32;
    let ncol = columns.len() as u32;
    let big_endian = byte_order == ByteOrder::BigEndian;

    // Check if any columns are stored indirectly or are record columns.
    // Record columns use indirect Vector<uChar> storage (like C++ ScaRecordColData).
    let has_indirect = columns
        .iter()
        .any(|c| c.is_record() || (c.is_array && (c.option & 1) == 0));

    // Create shared array file for indirect columns (version 2 format: path + "i").
    let mut array_writer = if has_indirect {
        let mut arr_path = path.as_os_str().to_os_string();
        arr_path.push("i");
        // StManAipsIO uses version 1 (with refcount) by default.
        Some(StManArrayFileWriter::create(
            &std::path::PathBuf::from(arr_path),
            big_endian,
            1,
        )?)
    } else {
        None
    };

    let mut io = AipsIo::open_with_order(path, AipsOpenOption::New, byte_order)?;
    io.putstart("StManAipsIO", 2)?;

    // C++ casacore writes an empty stmanName for StManAipsIO v2
    io.put_string("")?; // stmanName
    io.put_u32(0)?; // seqNr
    io.put_u32(0)?; // uniqNr
    io.put_u32(nrrow)?;
    io.put_u32(ncol)?;

    // Per-column data types.
    // C++ stores the DM-level data type: TpUChar for record columns
    // (since ScaRecordColData uses createIndArrColumn(name, TpUChar, "")).
    for col in columns {
        let scalar_dt = if col.is_record() {
            CasacoreDataType::TpUChar
        } else {
            CasacoreDataType::from_primitive_type(col.require_primitive_type()?, false)
        };
        io.put_i32(scalar_dt as i32)?;
    }

    // Per-column data blocks
    for (col_idx, col) in columns.iter().enumerate() {
        write_stman_column(&mut io, col, rows, col_idx, &mut array_writer)?;
    }

    // Flush the array file header and buffer if we wrote indirect arrays.
    if let Some(ref mut w) = array_writer {
        w.finish()?;
    }

    io.putend()?;
    io.close()?;
    Ok(())
}

fn write_stman_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_types::RecordValue],
    col_idx: usize,
    array_writer: &mut Option<StManArrayFileWriter>,
) -> Result<(), StorageError> {
    let col_name = &col_desc.col_name;
    let is_direct_array = col_desc.is_array && (col_desc.option & 1) != 0;
    let is_indirect_array = col_desc.is_array && !is_direct_array;

    if col_desc.is_record() {
        // Record column: serialize as indirect Vector<uChar>, matching C++ ScaRecordColData.
        write_stman_record_as_indirect(io, rows, col_name, array_writer)?;
    } else if is_indirect_array {
        // Non-direct array column: StManColumnIndArrayAipsIO. This includes
        // both variable-shape arrays and fixed-shape arrays without the Direct
        // option bit.
        write_stman_indirect_column(io, col_desc, rows, col_name, array_writer)?;
    } else if is_direct_array {
        // Fixed-shape array: StManColumnArrayAipsIO wrapping StManColumnAipsIO
        let nrrow = rows.len() as u32;
        let elements_per_row: usize = col_desc.shape.iter().map(|&s| s as usize).product();

        io.putstart("StManColumnArrayAipsIO", 2)?;
        io.putstart("StManColumnAipsIO", 2)?;

        io.put_u32(nrrow)?; // nrval = number of rows

        if nrrow > 0 {
            // Single extent: extent_count + total_elements + per-row data (no count prefix)
            io.put_u32(nrrow)?; // extent count
            io.put_u32(nrrow * elements_per_row as u32)?; // total elements
            write_flat_array_column_raw(io, col_desc, rows, col_name)?;
        }

        io.putend()?; // StManColumnAipsIO
        io.putend()?; // StManColumnArrayAipsIO
    } else {
        // Scalar columns: StManColumnAipsIO only
        let nrval = rows.len() as u32;

        io.putstart("StManColumnAipsIO", 2)?;
        io.put_u32(nrval)?;

        if nrval > 0 {
            // Single extent: extent_count + data (with count prefix)
            io.put_u32(nrval)?;
            write_scalar_column(io, col_desc, rows, col_name, col_idx)?;
        }

        io.putend()?;
    }

    Ok(())
}

/// Write a variable-shape (indirect) column.
///
/// Corresponds to C++ `StManColumnIndArrayAipsIO::putFile`.
fn write_stman_indirect_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_types::RecordValue],
    col_name: &str,
    array_writer: &mut Option<StManArrayFileWriter>,
) -> Result<(), StorageError> {
    let nrrow = rows.len() as u32;
    let dt = CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false);

    let writer = array_writer.as_mut().ok_or_else(|| {
        StorageError::FormatMismatch("no array file writer for indirect column".to_string())
    })?;

    // Write each row's array to the array file, collecting offsets.
    let mut offsets = Vec::with_capacity(nrrow as usize);
    for row in rows {
        match row.get(col_name) {
            Some(val @ Value::Array(_)) => {
                let offset = writer.write_array(val, dt)?;
                offsets.push(offset);
            }
            _ => {
                offsets.push(0i64); // undefined cell
            }
        }
    }

    // Write AipsIO envelope.
    io.putstart("StManColumnIndArrayAipsIO", 2)?;
    io.put_i32(dt as i32)?; // dtype for backward compatibility
    io.put_i32(0)?; // seqnr (not significant for v2)

    // Inner StManColumnAipsIO with file offsets as scalar data.
    io.putstart("StManColumnAipsIO", 2)?;
    io.put_u32(nrrow)?; // nrval

    if nrrow > 0 {
        // Single extent: extent_count + individual u32 offsets (no count prefix).
        // C++ StManColumnIndArrayAipsIO::putData writes `ios << uInt(off)` per row.
        io.put_u32(nrrow)?; // extent_count

        for &off in &offsets {
            if off == 0 {
                io.put_u32(0)?;
            } else if off <= 2u64.pow(31) as i64 {
                io.put_u32(off as u32)?;
            } else {
                io.put_u32(LARGE_OFFSET_MARKER)?;
                io.put_i64(off)?;
            }
        }
    }

    io.putend()?; // StManColumnAipsIO
    io.putend()?; // StManColumnIndArrayAipsIO

    Ok(())
}

/// Write a record column as `StManColumnIndArrayAipsIO` with `TpUChar` data.
///
/// Matches C++ `ScalarRecordColumnData` which calls `createIndArrColumn(name, TpUChar, "")`,
/// storing each record as an AipsIO-serialized `Vector<uChar>` in the indirect array file.
fn write_stman_record_as_indirect(
    io: &mut AipsIo,
    rows: &[casa_types::RecordValue],
    col_name: &str,
    array_writer: &mut Option<StManArrayFileWriter>,
) -> Result<(), StorageError> {
    let nrrow = rows.len() as u32;
    let dt = CasacoreDataType::TpUChar;

    // Serialize each record to uChar bytes and write to the array file.
    let mut offsets = Vec::with_capacity(nrrow as usize);
    for row in rows {
        let record = match row.get(col_name) {
            Some(Value::Record(r)) => r.clone(),
            _ => RecordValue::default(),
        };
        let bytes = table_control::serialize_record_to_uchar(&record)?;
        // Write as 1-D Array<uChar> to the array file.
        let arr = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[bytes.len()]), bytes)
            .map_err(|e| StorageError::FormatMismatch(format!("record serialize shape: {e}")))?;
        let value = Value::Array(casa_types::ArrayValue::UInt8(arr));
        let writer = array_writer.as_mut().ok_or_else(|| {
            StorageError::FormatMismatch(
                "record column requires indirect array writer but none was created".into(),
            )
        })?;
        let offset = writer.write_array(&value, dt)?;
        offsets.push(offset);
    }

    // Write AipsIO envelope: StManColumnIndArrayAipsIO.
    io.putstart("StManColumnIndArrayAipsIO", 2)?;
    io.put_i32(dt as i32)?; // dtype for backward compatibility
    io.put_i32(0)?; // seqnr

    // Inner StManColumnAipsIO with file offsets.
    io.putstart("StManColumnAipsIO", 2)?;
    io.put_u32(nrrow)?;

    if nrrow > 0 {
        io.put_u32(nrrow)?; // extent_count
        for &off in &offsets {
            if off == 0 {
                io.put_u32(0)?;
            } else if off <= 2u64.pow(31) as i64 {
                io.put_u32(off as u32)?;
            } else {
                io.put_u32(LARGE_OFFSET_MARKER)?;
                io.put_i64(off)?;
            }
        }
    }

    io.putend()?; // StManColumnAipsIO
    io.putend()?; // StManColumnIndArrayAipsIO

    Ok(())
}

fn write_scalar_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_types::RecordValue],
    col_name: &str,
    _col_idx: usize,
) -> Result<(), StorageError> {
    // Collect all scalar values into a typed vec and write as array
    let pt = col_desc.require_primitive_type()?;
    let allow_undefined = (col_desc.option & 2) != 0;
    match pt {
        PrimitiveType::Bool => {
            let values: Vec<bool> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Bool(v))) => Ok(*v),
                    None if allow_undefined => Ok(false),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Bool for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_bool_slice(&values, true)?;
        }
        PrimitiveType::Int32 => {
            let values: Vec<i32> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Int32(v))) => Ok(*v),
                    None if allow_undefined => Ok(0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Int32 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_i32_slice(&values, true)?;
        }
        PrimitiveType::Float64 => {
            let values: Vec<f64> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Float64(v))) => Ok(*v),
                    None if allow_undefined => Ok(0.0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Float64 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_f64_slice(&values, true)?;
        }
        PrimitiveType::String => {
            let values: Vec<String> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::String(v))) => Ok(v.clone()),
                    None if allow_undefined => Ok(String::new()),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected String for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_string_slice(&values, true)?;
        }
        PrimitiveType::Float32 => {
            let values: Vec<f32> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Float32(v))) => Ok(*v),
                    None if allow_undefined => Ok(0.0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Float32 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_f32_slice(&values, true)?;
        }
        PrimitiveType::UInt8 => {
            let values: Vec<u8> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::UInt8(v))) => Ok(*v),
                    None if allow_undefined => Ok(0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected UInt8 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_u8_slice(&values, true)?;
        }
        PrimitiveType::Int16 => {
            let values: Vec<i16> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Int16(v))) => Ok(*v),
                    None if allow_undefined => Ok(0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Int16 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_i16_slice(&values, true)?;
        }
        PrimitiveType::UInt16 => {
            let values: Vec<u16> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::UInt16(v))) => Ok(*v),
                    None if allow_undefined => Ok(0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected UInt16 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_u16_slice(&values, true)?;
        }
        PrimitiveType::UInt32 => {
            let values: Vec<u32> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::UInt32(v))) => Ok(*v),
                    None if allow_undefined => Ok(0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected UInt32 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_u32_slice(&values, true)?;
        }
        PrimitiveType::Int64 => {
            let values: Vec<i64> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Int64(v))) => Ok(*v),
                    None if allow_undefined => Ok(0),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Int64 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_i64_slice(&values, true)?;
        }
        PrimitiveType::Complex32 => {
            let values: Vec<casa_types::Complex32> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Complex32(v))) => Ok(*v),
                    None if allow_undefined => Ok(casa_types::Complex32::new(0.0, 0.0)),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Complex32 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_complex32_slice(&values, true)?;
        }
        PrimitiveType::Complex64 => {
            let values: Vec<casa_types::Complex64> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Complex64(v))) => Ok(*v),
                    None if allow_undefined => Ok(casa_types::Complex64::new(0.0, 0.0)),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Complex64 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_complex64_slice(&values, true)?;
        }
    }
    Ok(())
}

pub(crate) fn scalar_value_is_default(value: &Value, pt: PrimitiveType) -> bool {
    match (pt, value) {
        (PrimitiveType::Bool, Value::Scalar(ScalarValue::Bool(v))) => !*v,
        (PrimitiveType::UInt8, Value::Scalar(ScalarValue::UInt8(v))) => *v == 0,
        (PrimitiveType::Int16, Value::Scalar(ScalarValue::Int16(v))) => *v == 0,
        (PrimitiveType::UInt16, Value::Scalar(ScalarValue::UInt16(v))) => *v == 0,
        (PrimitiveType::Int32, Value::Scalar(ScalarValue::Int32(v))) => *v == 0,
        (PrimitiveType::UInt32, Value::Scalar(ScalarValue::UInt32(v))) => *v == 0,
        (PrimitiveType::Float32, Value::Scalar(ScalarValue::Float32(v))) => *v == 0.0,
        (PrimitiveType::Float64, Value::Scalar(ScalarValue::Float64(v))) => *v == 0.0,
        (PrimitiveType::Int64, Value::Scalar(ScalarValue::Int64(v))) => *v == 0,
        (PrimitiveType::Complex32, Value::Scalar(ScalarValue::Complex32(v))) => {
            v.re == 0.0 && v.im == 0.0
        }
        (PrimitiveType::Complex64, Value::Scalar(ScalarValue::Complex64(v))) => {
            v.re == 0.0 && v.im == 0.0
        }
        (PrimitiveType::String, Value::Scalar(ScalarValue::String(v))) => v.is_empty(),
        _ => false,
    }
}

/// Write array column data per-row without count prefix (putNR=false).
/// C++ StManColumnArrayAipsIO writes each row's data with ios.put(nrelem, data, False).
fn write_flat_array_column_raw(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_types::RecordValue],
    col_name: &str,
) -> Result<(), StorageError> {
    let shape: Vec<usize> = col_desc.shape.iter().map(|&s| s as usize).collect();
    let elements_per_row: usize = shape.iter().product();

    // Collect all values in row order (Fortran layout within each row), then write without count prefix
    match col_desc.require_primitive_type()? {
        PrimitiveType::Float32 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Float32(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Float32 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_f32_slice(&flat, false)?;
        }
        PrimitiveType::Float64 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Float64(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Float64 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_f64_slice(&flat, false)?;
        }
        PrimitiveType::Int16 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Int16(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Int16 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_i16_slice(&flat, false)?;
        }
        PrimitiveType::Int32 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Int32(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Int32 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_i32_slice(&flat, false)?;
        }
        PrimitiveType::Complex32 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Complex32(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Complex32 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_complex32_slice(&flat, false)?;
        }
        PrimitiveType::Bool => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Bool(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Bool array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_bool_slice(&flat, false)?;
        }
        PrimitiveType::UInt8 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::UInt8(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected UInt8 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_u8_slice(&flat, false)?;
        }
        PrimitiveType::UInt16 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::UInt16(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected UInt16 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_u16_slice(&flat, false)?;
        }
        PrimitiveType::UInt32 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::UInt32(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected UInt32 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_u32_slice(&flat, false)?;
        }
        PrimitiveType::Int64 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Int64(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Int64 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_i64_slice(&flat, false)?;
        }
        PrimitiveType::Complex64 => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::Complex64(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected Complex64 array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_complex64_slice(&flat, false)?;
        }
        PrimitiveType::String => {
            let mut flat = Vec::with_capacity(rows.len() * elements_per_row);
            for row in rows {
                match row.get(col_name) {
                    Some(Value::Array(ArrayValue::String(arr))) => {
                        flat.extend(fortran_flat_iter(arr));
                    }
                    _ => {
                        return Err(StorageError::FormatMismatch(format!(
                            "expected String array for column {col_name}"
                        )));
                    }
                }
            }
            io.put_string_slice(&flat, false)?;
        }
    }
    Ok(())
}

/// Iterate over an ndarray in Fortran (column-major) order.
fn fortran_flat_iter<T: Clone>(arr: &ArrayD<T>) -> Vec<T> {
    let shape = arr.shape();
    if shape.is_empty() {
        return vec![];
    }
    let total: usize = shape.iter().product();
    let ndim = shape.len();
    let mut result = Vec::with_capacity(total);
    let mut indices = vec![0usize; ndim];

    for _ in 0..total {
        result.push(arr[IxDyn(&indices)].clone());
        // Increment indices in Fortran order (first dimension changes fastest)
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
