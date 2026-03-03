// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::path::Path;

use casacore_aipsio::{AipsIo, AipsOpenOption};
use casacore_types::{ArrayValue, PrimitiveType, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::data_type::CasacoreDataType;
use super::table_control::ColumnDescContents;

// ---------------------------------------------------------------------------
// Parsed column data from table.f<N>
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct StManAipsIOFile {
    pub name: String,
    pub seq_nr: u32,
    pub uniq_nr: u32,
    pub nrrow: u32,
    pub ncol: u32,
    pub data_types: Vec<CasacoreDataType>,
    pub columns: Vec<ColumnRawData>,
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
    Complex32(Vec<casacore_types::Complex32>),
    Complex64(Vec<casacore_types::Complex64>),
    String(Vec<String>),
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
) -> Result<StManAipsIOFile, StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::Old)?;

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

    // Read per-column data blocks
    let mut columns = Vec::with_capacity(ncol as usize);
    for (i, dt) in data_types.iter().enumerate() {
        let info = col_info.get(i).ok_or_else(|| {
            StorageError::FormatMismatch(format!("missing column info for StManAipsIO col {i}"))
        })?;
        let col_data = if info.is_array {
            read_stman_array_column(&mut io, *dt, info.nrelem)?
        } else {
            read_stman_column(&mut io, *dt)?
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
            let mut v = vec![casacore_types::Complex32::new(0.0, 0.0); count];
            io.get_complex32_into(&mut v)?;
            Ok(ColumnRawData::Complex32(v))
        }
        CasacoreDataType::TpDComplex => {
            let mut v = vec![casacore_types::Complex64::new(0.0, 0.0); count];
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
        ColumnRawData::Int16(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Int16(arr)
        }
        ColumnRawData::Int32(v) => {
            let slice = &v[offset..offset + elements_per_row];
            let arr = ArrayD::from_shape_vec(IxDyn(&shape).f(), slice.to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
            ArrayValue::Int32(arr)
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
        _ => {
            return Err(StorageError::FormatMismatch(
                "unsupported array element type".to_string(),
            ));
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
    rows: &[casacore_types::RecordValue],
) -> Result<(), StorageError> {
    let nrrow = rows.len() as u32;
    let ncol = columns.len() as u32;

    let mut io = AipsIo::open(path, AipsOpenOption::New)?;
    io.putstart("StManAipsIO", 2)?;

    // C++ casacore writes an empty stmanName for StManAipsIO v2
    io.put_string("")?; // stmanName
    io.put_u32(0)?; // seqNr
    io.put_u32(0)?; // uniqNr
    io.put_u32(nrrow)?;
    io.put_u32(ncol)?;

    // Per-column data types (always the scalar element type)
    for col in columns {
        let scalar_dt = CasacoreDataType::from_primitive_type(col.primitive_type, false);
        io.put_i32(scalar_dt as i32)?;
    }

    // Per-column data blocks
    for (col_idx, col) in columns.iter().enumerate() {
        write_stman_column(&mut io, col, rows, col_idx)?;
    }

    io.putend()?;
    io.close()?;
    Ok(())
}

fn write_stman_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casacore_types::RecordValue],
    col_idx: usize,
) -> Result<(), StorageError> {
    let col_name = &col_desc.col_name;
    let is_fixed_array = col_desc.is_array && col_desc.nrdim > 0 && !col_desc.shape.is_empty();

    if is_fixed_array {
        // Array columns: StManColumnArrayAipsIO wrapping StManColumnAipsIO
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

fn write_scalar_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casacore_types::RecordValue],
    col_name: &str,
    _col_idx: usize,
) -> Result<(), StorageError> {
    // Collect all scalar values into a typed vec and write as array
    let pt = col_desc.primitive_type;
    match pt {
        PrimitiveType::Bool => {
            let values: Vec<bool> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Bool(v))) => Ok(*v),
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
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Int64 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_i64_slice(&values, true)?;
        }
        PrimitiveType::Complex32 => {
            let values: Vec<casacore_types::Complex32> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Complex32(v))) => Ok(*v),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Complex32 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_complex32_slice(&values, true)?;
        }
        PrimitiveType::Complex64 => {
            let values: Vec<casacore_types::Complex64> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Complex64(v))) => Ok(*v),
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

/// Write array column data per-row without count prefix (putNR=false).
/// C++ StManColumnArrayAipsIO writes each row's data with ios.put(nrelem, data, False).
fn write_flat_array_column_raw(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casacore_types::RecordValue],
    col_name: &str,
) -> Result<(), StorageError> {
    let shape: Vec<usize> = col_desc.shape.iter().map(|&s| s as usize).collect();
    let elements_per_row: usize = shape.iter().product();

    // Collect all values in row order (Fortran layout within each row), then write without count prefix
    match col_desc.primitive_type {
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
        other => {
            return Err(StorageError::FormatMismatch(format!(
                "fixed array write not yet implemented for {other:?}"
            )));
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
