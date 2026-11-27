// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

mod array_shapes;
use array_shapes::parse_indirect_offsets_layout;
pub(crate) use array_shapes::read_stman_array_column_shapes;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_aipsio::{AipsIo, AipsOpenOption, ByteOrder};
use casa_core::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
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
    Complex32(Vec<casa_core::Complex32>),
    Complex64(Vec<casa_core::Complex64>),
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
    IndirectArrayOffsets {
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

/// The manager's name (`DataManager::dataManagerName()`), from the header of
/// its file; `None` when the file is missing, does not parse, or is of the
/// version before the header held a name. The file is read in whichever byte
/// order its header parses in.
pub(crate) fn name_from_file(path: &Path) -> Option<String> {
    [ByteOrder::BigEndian, ByteOrder::LittleEndian]
        .into_iter()
        .find_map(|byte_order| {
            let mut io = AipsIo::open_with_order(path, AipsOpenOption::Old, byte_order).ok()?;
            let version = io.getstart("StManAipsIO").ok()?;
            if version < 2 {
                return None;
            }
            io.get_string().ok()
        })
}

mod sparse;
mod write;

pub(crate) use sparse::read_stman_array_column_rows;
pub(crate) use sparse::read_stman_scalar_column;
pub(crate) use sparse::read_stman_scalar_column_rows;
pub(crate) use sparse::save_stman_file_rows_in_place;
use sparse::*;
pub(crate) use write::scalar_value_is_default;
pub(super) use write::write_named_stman_file;
use write::*;

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
            let mut v = vec![casa_core::Complex32::new(0.0, 0.0); count];
            io.get_complex32_into(&mut v)?;
            Ok(ColumnRawData::Complex32(v))
        }
        CasacoreDataType::TpDComplex => {
            let mut v = vec![casa_core::Complex64::new(0.0, 0.0); count];
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

/// Write a StManAipsIO data file holding `columns` of `rows`.
///
/// `seq_nr` is the manager's sequence number in `table.dat`: casacore's
/// `StManAipsIO::open` refuses a file whose sequence number or column count
/// differs from the table's, so a StManAipsIO beside other managers must
/// carry its own number (#698).
pub(crate) fn write_stman_file(
    path: &Path,
    seq_nr: u32,
    columns: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    byte_order: ByteOrder,
) -> Result<(), StorageError> {
    write_named_stman_file(path, seq_nr, columns, rows, byte_order, "")
}
