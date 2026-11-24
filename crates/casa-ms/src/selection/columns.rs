// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected MAIN-table column reads used by the selection planner.

use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCells, Table,
};
use casa_types::ArrayValue;

use crate::error::{MsError, MsResult};

#[derive(Default)]
pub(super) struct SelectedRowColumns {
    pub(super) field_ids: Vec<i32>,
    pub(super) data_desc_ids: Vec<i32>,
    pub(super) antenna1_ids: Vec<i32>,
    pub(super) antenna2_ids: Vec<i32>,
    pub(super) flag_rows: Vec<bool>,
    pub(super) times: Vec<f64>,
}

impl SelectedRowColumns {
    pub(super) fn read(table: &Table, rows: &[usize]) -> MsResult<Self> {
        use RequiredScalarColumnValuesMut::{Bool, Float64, Int32};

        let mut columns = Self::default();
        let mut destinations = [
            RequiredScalarColumnDestination::new("FIELD_ID", Int32(&mut columns.field_ids)),
            RequiredScalarColumnDestination::new("DATA_DESC_ID", Int32(&mut columns.data_desc_ids)),
            RequiredScalarColumnDestination::new("ANTENNA1", Int32(&mut columns.antenna1_ids)),
            RequiredScalarColumnDestination::new("ANTENNA2", Int32(&mut columns.antenna2_ids)),
            RequiredScalarColumnDestination::new("FLAG_ROW", Bool(&mut columns.flag_rows)),
            RequiredScalarColumnDestination::new("TIME", Float64(&mut columns.times)),
        ];
        if table
            .required_scalar_columns_for_rows_into(rows, &mut destinations)
            .is_ok()
        {
            return Ok(columns);
        }

        // On a projection error, preserve the row-specific selection diagnostics.
        Ok(Self {
            field_ids: load_i32_column(table, "FIELD_ID", rows)?,
            data_desc_ids: load_i32_column(table, "DATA_DESC_ID", rows)?,
            antenna1_ids: load_i32_column(table, "ANTENNA1", rows)?,
            antenna2_ids: load_i32_column(table, "ANTENNA2", rows)?,
            flag_rows: load_bool_column(table, "FLAG_ROW", rows)?,
            times: load_f64_column(table, "TIME", rows)?,
        })
    }
}

pub(super) fn load_field_ids(table: &Table, rows: &[usize]) -> MsResult<Vec<i32>> {
    let mut field_ids = Vec::new();
    if table
        .required_scalar_columns_for_rows_into(
            rows,
            &mut [RequiredScalarColumnDestination::new(
                "FIELD_ID",
                RequiredScalarColumnValuesMut::Int32(&mut field_ids),
            )],
        )
        .is_ok()
    {
        return Ok(field_ids);
    }
    load_i32_column(table, "FIELD_ID", rows)
}

pub(crate) fn load_uvw_column(table: &Table, rows: &[usize]) -> MsResult<Vec<[f64; 3]>> {
    let accessor = table.column_accessor("UVW")?;
    if let Ok(cells) = accessor.array_cells_1d_typed_uncached(rows) {
        let SelectedArray1DCells::Float64(values) = cells else {
            return Err(MsError::ColumnTypeMismatch {
                column: "UVW".to_string(),
                table: "MAIN".to_string(),
                expected: "Float64 array".to_string(),
                found: format!("{:?}", cells.primitive_type()),
            });
        };
        if values.axis0_count() != 3 {
            return Err(MsError::ColumnTypeMismatch {
                column: "UVW".to_string(),
                table: "MAIN".to_string(),
                expected: "f64[3]".to_string(),
                found: format!("f64[{}]", values.axis0_count()),
            });
        }
        return Ok(values.into_values().as_chunks::<3>().0.to_vec());
    }

    let values = accessor.array_cells_owned_uncached(rows)?;
    values
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ArrayValue::Float64(values)) if values.len() == 3 => {
                let values = values
                    .as_slice()
                    .ok_or_else(|| MsError::ColumnTypeMismatch {
                        column: format!("UVW[row={row}]"),
                        table: "MAIN".to_string(),
                        expected: "contiguous f64[3]".to_string(),
                        found: "non-contiguous Float64 array".to_string(),
                    })?;
                Ok([values[0], values[1], values[2]])
            }
            Some(ArrayValue::Float64(values)) => Err(MsError::ColumnTypeMismatch {
                column: format!("UVW[row={row}]"),
                table: "MAIN".to_string(),
                expected: "f64[3]".to_string(),
                found: format!("f64[{}]", values.len()),
            }),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: format!("UVW[row={row}]"),
                table: "MAIN".to_string(),
                expected: "Float64 array".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("UVW[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

pub(super) fn load_i32_column(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<i32>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_types::ScalarValue::Int32(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Int32".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

pub(super) fn load_f64_column(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<f64>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_types::ScalarValue::Float64(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Float64".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

fn load_bool_column(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<bool>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_types::ScalarValue::Bool(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Bool".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}
