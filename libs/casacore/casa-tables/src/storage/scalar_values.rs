// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn scalar_columns_from_snapshot(snapshot: &StorageSnapshot) -> ScalarColumnSnapshot {
    let mut columns: HashMap<String, Vec<Option<ScalarValue>>> = HashMap::new();
    for (row_index, row) in snapshot.rows.iter().enumerate() {
        for (name, values) in &mut columns {
            values.push(match row.get(name) {
                Some(Value::Scalar(value)) => Some(value.clone()),
                _ => None,
            });
        }
        for field in row.fields() {
            if let Value::Scalar(value) = &field.value
                && !columns.contains_key(&field.name)
            {
                let mut values = vec![None; row_index];
                values.push(Some(value.clone()));
                columns.insert(field.name.clone(), values);
            }
        }
    }
    for values in columns.values_mut() {
        if values.len() < snapshot.row_count {
            values.resize(snapshot.row_count, None);
        }
    }
    ScalarColumnSnapshot {
        row_count: snapshot.row_count,
        columns,
    }
}

pub(super) fn array_column_from_snapshot(
    snapshot: &StorageSnapshot,
    column: &str,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    snapshot
        .rows
        .iter()
        .map(|row| match row.get(column) {
            Some(Value::Array(value)) => Ok(Some(value.clone())),
            Some(other) => Err(StorageError::FormatMismatch(format!(
                "column '{column}' expected array value, found {:?}",
                other.kind()
            ))),
            None => Ok(None),
        })
        .collect()
}

pub(super) fn scalar_column_from_snapshot(
    snapshot: &StorageSnapshot,
    column: &str,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    snapshot
        .rows
        .iter()
        .map(|row| match row.get(column) {
            Some(Value::Scalar(value)) => Ok(Some(value.clone())),
            Some(other) => Err(StorageError::FormatMismatch(format!(
                "column '{column}' expected scalar value, found {:?}",
                other.kind()
            ))),
            None => Ok(None),
        })
        .collect()
}

pub(super) fn select_array_rows(
    values: &[Option<ArrayValue>],
    selected_rows: &[usize],
) -> Vec<Option<ArrayValue>> {
    selected_rows
        .iter()
        .map(|&row_idx| values.get(row_idx).cloned().unwrap_or(None))
        .collect()
}

pub(super) fn select_scalar_rows(
    values: &[Option<ScalarValue>],
    selected_rows: &[usize],
) -> Vec<Option<ScalarValue>> {
    selected_rows
        .iter()
        .map(|&row_idx| values.get(row_idx).cloned().unwrap_or(None))
        .collect()
}

pub(super) fn collect_stman_scalar_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: Option<&HashSet<&str>>,
    nrrow: usize,
    byte_order: ByteOrder,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let col_info: Vec<StManColumnInfo> = bound_cols
        .iter()
        .map(|(desc_idx, _)| {
            let c = &all_col_descs[*desc_idx];
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                0
            };
            StManColumnInfo {
                is_array: c.is_array,
                nrelem,
            }
        })
        .collect();
    let stman_data = read_stman_file(data_path, &col_info, byte_order)?;
    for (stman_col_idx, (desc_idx, _)) in bound_cols.iter().enumerate() {
        if stman_col_idx >= stman_data.columns.len() {
            break;
        }
        let col_desc = &all_col_descs[*desc_idx];
        if requested_columns
            .is_some_and(|requested| !requested.contains(col_desc.col_name.as_str()))
        {
            continue;
        }
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values =
            scalar_values_from_stman_data(&stman_data.columns[stman_col_idx], col_desc, nrrow)?;
        columns.insert(col_desc.col_name.clone(), values);
    }
    Ok(())
}

pub(super) fn collect_stman_required_scalar_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: &HashSet<&str>,
    nrrow: usize,
    byte_order: ByteOrder,
    columns: &mut HashMap<String, RequiredScalarColumnData>,
) -> Result<(), StorageError> {
    let col_info: Vec<StManColumnInfo> = bound_cols
        .iter()
        .map(|(desc_idx, _)| {
            let c = &all_col_descs[*desc_idx];
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                0
            };
            StManColumnInfo {
                is_array: c.is_array,
                nrelem,
            }
        })
        .collect();
    let stman_data = read_stman_file(data_path, &col_info, byte_order)?;
    for (stman_col_idx, (desc_idx, _)) in bound_cols.iter().enumerate() {
        if stman_col_idx >= stman_data.columns.len() {
            break;
        }
        let col_desc = &all_col_descs[*desc_idx];
        if !requested_columns.contains(col_desc.col_name.as_str())
            || col_desc.is_array
            || col_desc.is_record()
        {
            continue;
        }
        let values = match &stman_data.columns[stman_col_idx] {
            StManColumnData::Flat(raw) => required_scalar_column_from_raw(raw, col_desc, nrrow)?,
            StManColumnData::Indirect(per_row) => {
                required_scalar_column_from_optional_values(per_row, &col_desc.col_name)?
            }
        };
        columns.insert(col_desc.col_name.clone(), values);
    }
    Ok(())
}

pub(super) fn collect_ssm_scalar_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: Option<&HashSet<&str>>,
    nrrow: usize,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let col_descs: Vec<(usize, &table_control::ColumnDescContents)> = bound_cols
        .iter()
        .enumerate()
        .filter_map(|(dm_col_idx, (desc_idx, plain_col))| {
            requested_columns
                .is_none_or(|requested| requested.contains(plain_col.original_name.as_str()))
                .then_some((dm_col_idx, &all_col_descs[*desc_idx]))
        })
        .collect();
    let ssm_columns = read_ssm_file_columns(data_path, dm_blob, &col_descs, nrrow)?;
    for (col_name, col_result) in &ssm_columns {
        let col_desc = col_descs
            .iter()
            .map(|(_, col_desc)| *col_desc)
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM returned column '{col_name}' not in descriptor"
                ))
            })?;
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values = scalar_values_from_ssm_data(col_result, col_desc, nrrow)?;
        columns.insert(col_name.clone(), values);
    }
    Ok(())
}

pub(super) fn collect_ssm_required_scalar_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: &HashSet<&str>,
    nrrow: usize,
    columns: &mut HashMap<String, RequiredScalarColumnData>,
) -> Result<(), StorageError> {
    let col_descs: Vec<(usize, &table_control::ColumnDescContents)> = bound_cols
        .iter()
        .enumerate()
        .filter_map(|(dm_col_idx, (desc_idx, plain_col))| {
            requested_columns
                .contains(plain_col.original_name.as_str())
                .then_some((dm_col_idx, &all_col_descs[*desc_idx]))
        })
        .collect();
    let ssm_columns = read_ssm_file_columns(data_path, dm_blob, &col_descs, nrrow)?;
    for (col_name, col_result) in &ssm_columns {
        let col_desc = col_descs
            .iter()
            .map(|(_, col_desc)| *col_desc)
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM returned column '{col_name}' not in descriptor"
                ))
            })?;
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values = match col_result {
            standard_stman::SsmColumnResult::Flat(raw) => {
                required_scalar_column_from_raw(raw, col_desc, nrrow)?
            }
            standard_stman::SsmColumnResult::Indirect(per_row) => {
                required_scalar_column_from_optional_values(per_row, &col_desc.col_name)?
            }
        };
        columns.insert(col_name.clone(), values);
    }
    Ok(())
}

pub(super) fn collect_ism_scalar_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: Option<&HashSet<&str>>,
    nrrow: usize,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let col_descs: Vec<(usize, &table_control::ColumnDescContents)> = bound_cols
        .iter()
        .enumerate()
        .filter_map(|(dm_col_idx, (desc_idx, plain_col))| {
            requested_columns
                .is_none_or(|requested| requested.contains(plain_col.original_name.as_str()))
                .then_some((dm_col_idx, &all_col_descs[*desc_idx]))
        })
        .collect();
    let ism_columns =
        read_ism_file_columns(data_path, dm_blob, &col_descs, bound_cols.len(), nrrow)?;
    for (col_name, col_result) in &ism_columns {
        let col_desc = col_descs
            .iter()
            .map(|(_, col_desc)| *col_desc)
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM returned column '{col_name}' not in descriptor"
                ))
            })?;
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values = scalar_values_from_ism_data(col_result, col_desc, nrrow)?;
        columns.insert(col_name.clone(), values);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_ism_required_scalar_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: &HashSet<&str>,
    total_column_count: usize,
    nrrow: usize,
    columns: &mut HashMap<String, RequiredScalarColumnData>,
) -> Result<(), StorageError> {
    let col_descs: Vec<(usize, &table_control::ColumnDescContents)> = bound_cols
        .iter()
        .enumerate()
        .filter_map(|(dm_col_idx, (desc_idx, plain_col))| {
            requested_columns
                .contains(plain_col.original_name.as_str())
                .then_some((dm_col_idx, &all_col_descs[*desc_idx]))
        })
        .collect();
    let ism_columns =
        read_ism_file_columns(data_path, dm_blob, &col_descs, total_column_count, nrrow)?;
    for (col_name, col_result) in &ism_columns {
        let col_desc = col_descs
            .iter()
            .map(|(_, col_desc)| *col_desc)
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM returned column '{col_name}' not in descriptor"
                ))
            })?;
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values = match col_result {
            IsmColumnResult::Flat(raw) => required_scalar_column_from_raw(raw, col_desc, nrrow)?,
            IsmColumnResult::Indirect(per_row) => {
                required_scalar_column_from_optional_values(per_row, &col_desc.col_name)?
            }
        };
        columns.insert(col_name.clone(), values);
    }
    Ok(())
}

pub(super) fn required_scalar_columns_from_optional_snapshot(
    snapshot: &ScalarColumnSnapshot,
) -> Result<RequiredScalarColumnSnapshot, StorageError> {
    let mut columns = HashMap::with_capacity(snapshot.columns.len());
    for (column_name, values) in &snapshot.columns {
        columns.insert(
            column_name.clone(),
            required_scalar_column_from_optional_scalars(values, column_name)?,
        );
    }
    Ok(RequiredScalarColumnSnapshot {
        row_count: snapshot.row_count,
        columns,
    })
}

pub(super) fn required_scalar_column_from_raw(
    raw: &ColumnRawData,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<RequiredScalarColumnData, StorageError> {
    let primitive = col_desc.require_primitive_type()?;
    match (primitive, raw) {
        (casa_core::PrimitiveType::Bool, ColumnRawData::Bool(values)) => Ok(
            RequiredScalarColumnData::Bool(values.get(..nrrow).unwrap_or(values).to_vec()),
        ),
        (casa_core::PrimitiveType::Int32, ColumnRawData::Int32(values)) => Ok(
            RequiredScalarColumnData::Int32(values.get(..nrrow).unwrap_or(values).to_vec()),
        ),
        (casa_core::PrimitiveType::Float32, ColumnRawData::Float32(values)) => Ok(
            RequiredScalarColumnData::Float32(values.get(..nrrow).unwrap_or(values).to_vec()),
        ),
        (casa_core::PrimitiveType::Float64, ColumnRawData::Float64(values)) => Ok(
            RequiredScalarColumnData::Float64(values.get(..nrrow).unwrap_or(values).to_vec()),
        ),
        (expected, found) => Err(StorageError::FormatMismatch(format!(
            "required scalar column '{}' expected {:?}, found {:?}",
            col_desc.col_name,
            expected,
            column_raw_data_primitive(found)
        ))),
    }
}

pub(super) fn required_scalar_column_from_optional_values(
    values: &[Option<Value>],
    column_name: &str,
) -> Result<RequiredScalarColumnData, StorageError> {
    let mut scalar_values = Vec::with_capacity(values.len());
    for (row, value) in values.iter().enumerate() {
        let Some(Value::Scalar(scalar)) = value else {
            return Err(StorageError::FormatMismatch(format!(
                "required scalar column '{column_name}' row {row} is missing"
            )));
        };
        scalar_values.push(Some(scalar.clone()));
    }
    required_scalar_column_from_optional_scalars(&scalar_values, column_name)
}

pub(super) fn required_scalar_column_from_optional_scalars(
    values: &[Option<ScalarValue>],
    column_name: &str,
) -> Result<RequiredScalarColumnData, StorageError> {
    let first = values
        .iter()
        .find_map(|value| value.as_ref())
        .ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "required scalar column '{column_name}' has no values"
            ))
        })?;
    match first {
        ScalarValue::Bool(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Bool(value)) => Ok(*value),
                _ => Err(StorageError::FormatMismatch(format!(
                    "required scalar column '{column_name}' row {row} is not Bool"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Bool),
        ScalarValue::Int32(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Int32(value)) => Ok(*value),
                _ => Err(StorageError::FormatMismatch(format!(
                    "required scalar column '{column_name}' row {row} is not Int32"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Int32),
        ScalarValue::Float32(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float32(value)) => Ok(*value),
                _ => Err(StorageError::FormatMismatch(format!(
                    "required scalar column '{column_name}' row {row} is not Float32"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Float32),
        ScalarValue::Float64(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float64(value)) => Ok(*value),
                _ => Err(StorageError::FormatMismatch(format!(
                    "required scalar column '{column_name}' row {row} is not Float64"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnData::Float64),
        other => Err(StorageError::FormatMismatch(format!(
            "required scalar column '{column_name}' has unsupported type {:?}",
            other.primitive_type()
        ))),
    }
}

pub(super) fn column_raw_data_primitive(raw: &ColumnRawData) -> casa_core::PrimitiveType {
    match raw {
        ColumnRawData::Bool(_) => casa_core::PrimitiveType::Bool,
        ColumnRawData::UInt8(_) => casa_core::PrimitiveType::UInt8,
        ColumnRawData::Int16(_) => casa_core::PrimitiveType::Int16,
        ColumnRawData::UInt16(_) => casa_core::PrimitiveType::UInt16,
        ColumnRawData::Int32(_) => casa_core::PrimitiveType::Int32,
        ColumnRawData::UInt32(_) => casa_core::PrimitiveType::UInt32,
        ColumnRawData::Int64(_) => casa_core::PrimitiveType::Int64,
        ColumnRawData::Float32(_) => casa_core::PrimitiveType::Float32,
        ColumnRawData::Float64(_) => casa_core::PrimitiveType::Float64,
        ColumnRawData::Complex32(_) => casa_core::PrimitiveType::Complex32,
        ColumnRawData::Complex64(_) => casa_core::PrimitiveType::Complex64,
        ColumnRawData::String(_) => casa_core::PrimitiveType::String,
    }
}

pub(super) fn scalar_values_from_stman_data(
    data: &StManColumnData,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    match data {
        StManColumnData::Flat(raw) => {
            let mut values = Vec::with_capacity(nrrow);
            for row_idx in 0..nrrow {
                let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                let scalar = scalar_value_from_value(value, &col_desc.col_name)?;
                if (col_desc.option & 2) != 0
                    && scalar_value_is_default(
                        &Value::Scalar(scalar.clone()),
                        col_desc.require_primitive_type()?,
                    )
                {
                    values.push(None);
                } else {
                    values.push(Some(scalar));
                }
            }
            Ok(values)
        }
        StManColumnData::Indirect(per_row) => {
            scalar_values_from_indirect(per_row, &col_desc.col_name)
        }
    }
}

pub(super) fn scalar_values_from_ssm_data(
    data: &standard_stman::SsmColumnResult,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    match data {
        standard_stman::SsmColumnResult::Flat(raw) => {
            let mut values = Vec::with_capacity(nrrow);
            for row_idx in 0..nrrow {
                let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                values.push(Some(scalar_value_from_value(value, &col_desc.col_name)?));
            }
            Ok(values)
        }
        standard_stman::SsmColumnResult::Indirect(per_row) => {
            scalar_values_from_indirect(per_row, &col_desc.col_name)
        }
    }
}

pub(super) fn scalar_values_from_ism_data(
    data: &IsmColumnResult,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    match data {
        IsmColumnResult::Flat(raw) => {
            let mut values = Vec::with_capacity(nrrow);
            for row_idx in 0..nrrow {
                let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                values.push(Some(scalar_value_from_value(value, &col_desc.col_name)?));
            }
            Ok(values)
        }
        IsmColumnResult::Indirect(per_row) => {
            scalar_values_from_indirect(per_row, &col_desc.col_name)
        }
    }
}

pub(super) fn scalar_values_from_indirect(
    per_row: &[Option<Value>],
    column: &str,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    per_row
        .iter()
        .map(|value| {
            value
                .clone()
                .map(|value| scalar_value_from_value(value, column))
                .transpose()
        })
        .collect()
}

pub(super) fn scalar_value_from_value(
    value: Value,
    column: &str,
) -> Result<ScalarValue, StorageError> {
    match value {
        Value::Scalar(scalar) => Ok(scalar),
        other => Err(StorageError::FormatMismatch(format!(
            "column '{column}' expected scalar value, found {:?}",
            other.kind()
        ))),
    }
}
