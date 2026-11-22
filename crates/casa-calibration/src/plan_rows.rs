// SPDX-License-Identifier: LGPL-3.0-or-later
//! Project selection metadata from MeasurementSet MAIN into apply rows.

use super::*;

pub(super) fn build_selected_rows(
    ms: &MeasurementSet,
    row_indices: &[usize],
) -> Result<Vec<ApplyRowPlan>, ApplyPlanError> {
    let dd = ms
        .data_description()
        .map_err(|source| ApplyPlanError::Selection { source })?;
    let table = ms.main_table();
    let data_desc_ids = load_i32_column_rows(table, "DATA_DESC_ID", row_indices)?;
    let field_ids = load_i32_column_rows(table, "FIELD_ID", row_indices)?;
    let observation_ids = load_i32_column_rows(table, "OBSERVATION_ID", row_indices)?;
    let scan_numbers = load_i32_column_rows(table, "SCAN_NUMBER", row_indices)?;
    let antenna1s = load_i32_column_rows(table, "ANTENNA1", row_indices)?;
    let antenna2s = load_i32_column_rows(table, "ANTENNA2", row_indices)?;
    let feed1s = load_i32_column_rows(table, "FEED1", row_indices)?;
    let feed2s = load_i32_column_rows(table, "FEED2", row_indices)?;
    let times = load_f64_column_rows(table, "TIME", row_indices)?;
    let intervals = load_f64_column_rows(table, "INTERVAL", row_indices)?;

    let mut rows = row_indices
        .iter()
        .enumerate()
        .map(|(selected_idx, &row_index)| {
            let data_desc_id = value_at_i32(&data_desc_ids, selected_idx, "DATA_DESC_ID")?;
            let ddid_index = usize::try_from(data_desc_id).map_err(|_| {
                ApplyPlanError::MissingDataDescription {
                    row_index,
                    data_desc_id,
                }
            })?;
            if ddid_index >= dd.row_count() {
                return Err(ApplyPlanError::MissingDataDescription {
                    row_index,
                    data_desc_id,
                });
            }

            Ok(ApplyRowPlan {
                row_index,
                field_id: value_at_i32(&field_ids, selected_idx, "FIELD_ID")?,
                observation_id: value_at_i32(&observation_ids, selected_idx, "OBSERVATION_ID")?,
                calibration_observation_id: value_at_i32(
                    &observation_ids,
                    selected_idx,
                    "OBSERVATION_ID",
                )?,
                scan_number: value_at_i32(&scan_numbers, selected_idx, "SCAN_NUMBER")?,
                data_desc_id,
                data_spw_id: dd
                    .spectral_window_id(ddid_index)
                    .map_err(|source| ApplyPlanError::Selection { source })?,
                polarization_id: dd
                    .polarization_id(ddid_index)
                    .map_err(|source| ApplyPlanError::Selection { source })?,
                antenna1: value_at_i32(&antenna1s, selected_idx, "ANTENNA1")?,
                antenna2: value_at_i32(&antenna2s, selected_idx, "ANTENNA2")?,
                feed1: value_at_i32(&feed1s, selected_idx, "FEED1")?,
                feed2: value_at_i32(&feed2s, selected_idx, "FEED2")?,
                time_seconds: value_at_f64(&times, selected_idx, "TIME")?,
                interval_seconds: value_at_f64(&intervals, selected_idx, "INTERVAL")?,
            })
        })
        .collect::<Result<Vec<_>, ApplyPlanError>>()?;
    library::buffer_observations(ms, &mut rows)?;
    Ok(rows)
}

pub(super) fn load_i32_column_rows(
    table: &Table,
    column: &str,
    row_indices: &[usize],
) -> Result<Vec<Option<i32>>, ApplyPlanError> {
    table
        .column_accessor(column)
        .and_then(|column| column.scalar_cells_owned_for_rows(row_indices))
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })?
        .into_iter()
        .enumerate()
        .map(|(selected_idx, value)| match value {
            Some(casa_types::ScalarValue::Int32(v)) => Ok(Some(v)),
            Some(other) => Err(ApplyPlanError::ScalarTypeMismatch {
                context: format!("{column} selected row {selected_idx}"),
                expected: "Int32",
                found: scalar_kind(&other),
            }),
            None => Ok(None),
        })
        .collect()
}

fn load_f64_column_rows(
    table: &Table,
    column: &str,
    row_indices: &[usize],
) -> Result<Vec<Option<f64>>, ApplyPlanError> {
    table
        .column_accessor(column)
        .and_then(|column| column.scalar_cells_owned_for_rows(row_indices))
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })?
        .into_iter()
        .enumerate()
        .map(|(selected_idx, value)| match value {
            Some(casa_types::ScalarValue::Float64(v)) => Ok(Some(v)),
            Some(other) => Err(ApplyPlanError::ScalarTypeMismatch {
                context: format!("{column} selected row {selected_idx}"),
                expected: "Float64",
                found: scalar_kind(&other),
            }),
            None => Ok(None),
        })
        .collect()
}

pub(super) fn value_at_i32(
    values: &[Option<i32>],
    row_index: usize,
    column: &str,
) -> Result<i32, ApplyPlanError> {
    values
        .get(row_index)
        .and_then(|value| *value)
        .ok_or_else(|| ApplyPlanError::Selection {
            source: MsError::MissingColumn {
                column: column.to_string(),
                table: format!("MAIN row {row_index}"),
            },
        })
}

fn value_at_f64(
    values: &[Option<f64>],
    row_index: usize,
    column: &str,
) -> Result<f64, ApplyPlanError> {
    values
        .get(row_index)
        .and_then(|value| *value)
        .ok_or_else(|| ApplyPlanError::Selection {
            source: MsError::MissingColumn {
                column: column.to_string(),
                table: format!("MAIN row {row_index}"),
            },
        })
}
