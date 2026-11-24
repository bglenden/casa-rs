// SPDX-License-Identifier: LGPL-3.0-or-later
//! Project selection metadata from MeasurementSet MAIN into apply rows.

use casa_tables::{ColumnType, RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};

use super::*;

const INTEGER_COLUMNS: [&str; 8] = [
    "DATA_DESC_ID",
    "FIELD_ID",
    "OBSERVATION_ID",
    "SCAN_NUMBER",
    "ANTENNA1",
    "ANTENNA2",
    "FEED1",
    "FEED2",
];
const TIME_COLUMNS: [&str; 2] = ["TIME", "INTERVAL"];
const ARRAY_COLUMN: &str = "ARRAY_ID";

enum ScalarRows<T> {
    Defined(Vec<T>),
    Optional(Vec<Option<T>>),
}

impl<T: Copy> ScalarRows<T> {
    fn required(&self, row: usize, column: &str) -> Result<T, ApplyPlanError> {
        let value = match self {
            Self::Defined(values) => values.get(row).copied(),
            Self::Optional(values) => values.get(row).copied().flatten(),
        };
        required_scalar(value, row, column)
    }
}

type ApplyScalarRows = (
    [ScalarRows<i32>; INTEGER_COLUMNS.len()],
    [ScalarRows<f64>; TIME_COLUMNS.len()],
    Option<Vec<i32>>,
);

pub(super) fn build_selected_rows(
    ms: &MeasurementSet,
    row_indices: &[usize],
) -> Result<Vec<ApplyRowPlan>, ApplyPlanError> {
    let dd = ms
        .data_description()
        .map_err(|source| ApplyPlanError::Selection { source })?;
    let table = ms.main_table();
    let (
        [
            data_desc_ids,
            field_ids,
            observation_ids,
            scan_numbers,
            antenna1s,
            antenna2s,
            feed1s,
            feed2s,
        ],
        [times, intervals],
        array_ids,
    ) = read_apply_scalars(table, row_indices)?;

    let mut rows = row_indices
        .iter()
        .enumerate()
        .map(|(selected_idx, &row_index)| {
            let data_desc_id = data_desc_ids.required(selected_idx, "DATA_DESC_ID")?;
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
            let field_id = field_ids.required(selected_idx, "FIELD_ID")?;
            let observation_id = observation_ids.required(selected_idx, "OBSERVATION_ID")?;

            Ok(ApplyRowPlan {
                row_index,
                field_id,
                observation_id,
                calibration_observation_id: observation_id,
                scan_number: scan_numbers.required(selected_idx, "SCAN_NUMBER")?,
                data_desc_id,
                data_spw_id: dd
                    .spectral_window_id(ddid_index)
                    .map_err(|source| ApplyPlanError::Selection { source })?,
                polarization_id: dd
                    .polarization_id(ddid_index)
                    .map_err(|source| ApplyPlanError::Selection { source })?,
                antenna1: antenna1s.required(selected_idx, "ANTENNA1")?,
                antenna2: antenna2s.required(selected_idx, "ANTENNA2")?,
                feed1: feed1s.required(selected_idx, "FEED1")?,
                feed2: feed2s.required(selected_idx, "FEED2")?,
                time_seconds: times.required(selected_idx, "TIME")?,
                interval_seconds: intervals.required(selected_idx, "INTERVAL")?,
            })
        })
        .collect::<Result<Vec<_>, ApplyPlanError>>()?;
    let arrays = match array_ids {
        Some(values) => ScalarRows::Defined(values),
        None => ScalarRows::Optional(load_i32_column_rows(table, ARRAY_COLUMN, row_indices)?),
    };
    library::buffer_observations(&mut rows, |index| arrays.required(index, ARRAY_COLUMN))?;
    Ok(rows)
}

fn read_apply_scalars(table: &Table, rows: &[usize]) -> Result<ApplyScalarRows, ApplyPlanError> {
    if let Some(columns) = read_defined_apply_scalars(table, rows) {
        return Ok(columns);
    }
    // Keep the original row-specific diagnostics when a typed read cannot be used.
    let integer = |name| load_i32_column_rows(table, name, rows).map(ScalarRows::Optional);
    let time = |name| load_f64_column_rows(table, name, rows).map(ScalarRows::Optional);
    Ok((
        [
            integer("DATA_DESC_ID")?,
            integer("FIELD_ID")?,
            integer("OBSERVATION_ID")?,
            integer("SCAN_NUMBER")?,
            integer("ANTENNA1")?,
            integer("ANTENNA2")?,
            integer("FEED1")?,
            integer("FEED2")?,
        ],
        [time("TIME")?, time("INTERVAL")?],
        None,
    ))
}

fn read_defined_apply_scalars(table: &Table, rows: &[usize]) -> Option<ApplyScalarRows> {
    use RequiredScalarColumnValuesMut::{Float64, Int32};

    let schema = table.schema()?;
    if !INTEGER_COLUMNS
        .into_iter()
        .chain(TIME_COLUMNS)
        .chain([ARRAY_COLUMN])
        .all(|name| {
            schema.column(name).is_some_and(|column| {
                matches!(column.column_type(), ColumnType::Scalar) && !column.options().undefined
            })
        })
    {
        return None;
    }
    let mut integer = std::array::from_fn(|_| Vec::new());
    let mut time = std::array::from_fn(|_| Vec::new());
    let mut array_ids = Vec::new();
    let mut destinations: Vec<_> = INTEGER_COLUMNS
        .into_iter()
        .zip(integer.iter_mut().map(Int32))
        .chain(TIME_COLUMNS.into_iter().zip(time.iter_mut().map(Float64)))
        .chain([(ARRAY_COLUMN, Int32(&mut array_ids))])
        .map(|(name, values)| RequiredScalarColumnDestination::new(name, values))
        .collect();
    table
        .required_scalar_columns_for_rows_into(rows, &mut destinations)
        .ok()?;
    Some((
        integer.map(ScalarRows::Defined),
        time.map(ScalarRows::Defined),
        Some(array_ids),
    ))
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
    required_scalar(values.get(row_index).copied().flatten(), row_index, column)
}

fn required_scalar<T>(
    value: Option<T>,
    row_index: usize,
    column: &str,
) -> Result<T, ApplyPlanError> {
    value.ok_or_else(|| ApplyPlanError::Selection {
        source: MsError::MissingColumn {
            column: column.to_string(),
            table: format!("MAIN row {row_index}"),
        },
    })
}
