// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn write_named_stman_file(
    path: &Path,
    seq_nr: u32,
    columns: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    byte_order: ByteOrder,
    manager_name: &str,
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

    io.put_string(manager_name)?; // stmanName (introduced by version 2)
    io.put_u32(seq_nr)?;
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

pub(super) fn write_stman_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_core::RecordValue],
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
pub(super) fn write_stman_indirect_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_core::RecordValue],
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
pub(super) fn write_stman_record_as_indirect(
    io: &mut AipsIo,
    rows: &[casa_core::RecordValue],
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
        let value = Value::Array(casa_core::ArrayValue::UInt8(arr));
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

pub(super) fn write_scalar_column(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_core::RecordValue],
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
            let values: Vec<casa_core::Complex32> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Complex32(v))) => Ok(*v),
                    None if allow_undefined => Ok(casa_core::Complex32::new(0.0, 0.0)),
                    _ => Err(StorageError::FormatMismatch(format!(
                        "expected Complex32 for column {col_name}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            io.put_complex32_slice(&values, true)?;
        }
        PrimitiveType::Complex64 => {
            let values: Vec<casa_core::Complex64> = rows
                .iter()
                .map(|r| match r.get(col_name) {
                    Some(Value::Scalar(ScalarValue::Complex64(v))) => Ok(*v),
                    None if allow_undefined => Ok(casa_core::Complex64::new(0.0, 0.0)),
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
pub(super) fn write_flat_array_column_raw(
    io: &mut AipsIo,
    col_desc: &ColumnDescContents,
    rows: &[casa_core::RecordValue],
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
pub(super) fn fortran_flat_iter<T: Clone>(arr: &ArrayD<T>) -> Vec<T> {
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
