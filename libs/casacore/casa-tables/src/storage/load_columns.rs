// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Load columns from a StManAipsIO data file into row records.
pub(super) fn load_stman_aipsio_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
    byte_order: ByteOrder,
) -> Result<(), StorageError> {
    // Build column info for the StManAipsIO reader using the bound columns
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
        match &stman_data.columns[stman_col_idx] {
            StManColumnData::Flat(raw) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                    if !col_desc.is_array
                        && (col_desc.option & 2) != 0
                        && scalar_value_is_default(&value, col_desc.require_primitive_type()?)
                    {
                        undefined_cells[row_idx].insert(col_desc.col_name.clone());
                        continue;
                    }
                    if matches!(&value, Value::Array(_)) {
                        undefined_cells[row_idx].insert(col_desc.col_name.clone());
                    }
                    row.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
            StManColumnData::Indirect(per_row) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = match per_row.get(row_idx) {
                        Some(Some(v)) => {
                            if col_desc.is_record() {
                                // C++ stores records as indirect Vector<uChar>.
                                // Deserialize the byte array back to a RecordValue.
                                match v {
                                    Value::Array(casa_core::ArrayValue::UInt8(arr)) => {
                                        let bytes = arr.as_slice().ok_or_else(|| {
                                            StorageError::FormatMismatch(format!(
                                                "record column '{}': non-contiguous u8 array",
                                                col_desc.col_name
                                            ))
                                        })?;
                                        Value::Record(table_control::deserialize_record_from_uchar(
                                            bytes,
                                        )?)
                                    }
                                    Value::Record(_) => v.clone(),
                                    _ => {
                                        return Err(StorageError::FormatMismatch(format!(
                                            "record column '{}' has unexpected value type",
                                            col_desc.col_name
                                        )));
                                    }
                                }
                            } else {
                                v.clone()
                            }
                        }
                        Some(None) | None => {
                            match undefined_indirect_cell(col_desc, &mut undefined_cells[row_idx]) {
                                Some(value) => value,
                                None => continue,
                            }
                        }
                    };
                    row.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

/// Load columns from a StandardStMan data file into row records.
pub(super) fn load_ssm_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();

    let ssm_columns = read_ssm_file(data_path, dm_blob, &col_descs, nrrow)?;

    for (col_name, col_result) in &ssm_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM returned column '{col_name}' not in descriptor"
                ))
            })?;

        match col_result {
            standard_stman::SsmColumnResult::Flat(raw_data) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = extract_row_value(raw_data, col_desc, row_idx, nrrow)?;
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
            standard_stman::SsmColumnResult::Indirect(per_row) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = match per_row.get(row_idx) {
                        Some(Some(v)) => v.clone(),
                        Some(None) | None => {
                            match undefined_indirect_cell(col_desc, &mut undefined_cells[row_idx]) {
                                Some(value) => value,
                                None => continue,
                            }
                        }
                    };
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

/// Load columns from an IncrementalStMan data file into row records.
pub(super) fn load_ism_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();

    let ism_columns = read_ism_file(data_path, dm_blob, &col_descs, nrrow)?;

    for (col_name, col_result) in &ism_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM returned column '{col_name}' not in descriptor"
                ))
            })?;

        match col_result {
            IsmColumnResult::Flat(raw_data) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = extract_row_value(raw_data, col_desc, row_idx, nrrow)?;
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
            IsmColumnResult::Indirect(per_row) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = match per_row.get(row_idx) {
                        Some(Some(value)) => value.clone(),
                        Some(None) | None => {
                            match undefined_indirect_cell(col_desc, &mut undefined_cells[row_idx]) {
                                Some(value) => value,
                                None => continue,
                            }
                        }
                    };
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

/// What a row record holds for an undefined indirect cell of `col_desc`: an
/// empty record for a record column, and nothing for an array column, its
/// cell marked in `undefined`.
///
/// casacore has no array to give for such a cell: `ArrayColumn::get`
/// throws "no array in row" (`SSMIndColumn::getShape` and its kin). A row
/// record without the field is casa-rs's undefined cell, as in a table
/// built from rows: a cell read returns `None` (or a missing-cell error)
/// and `is_cell_defined` `false`, never a placeholder array (#852,
/// undefined indirect cells).
pub(crate) fn undefined_indirect_cell(
    col_desc: &table_control::ColumnDescContents,
    undefined: &mut HashSet<String>,
) -> Option<Value> {
    if col_desc.is_record() {
        return Some(Value::Record(RecordValue::default()));
    }
    undefined.insert(col_desc.col_name.clone());
    None
}

pub(super) fn apply_physical_dm_descriptor_group(
    table_dat: &mut TableDatContents,
    dm_type_name: &str,
    dm_group_name: &str,
    virtual_columns: &HashSet<String>,
) {
    for desc in &mut table_dat.table_desc.columns {
        if virtual_columns.contains(&desc.col_name) {
            continue;
        }
        desc.data_manager_type = dm_type_name.to_string();
        desc.data_manager_group = dm_group_name.to_string();
    }
}
