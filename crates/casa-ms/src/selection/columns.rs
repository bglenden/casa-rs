// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected MAIN-table column reads used by the selection planner.

use casa_core::ArrayValue;
use casa_tables::{
    ColumnType, RequiredScalarColumnDestination, RequiredScalarColumnValuesMut,
    SelectedArray1DCells, Table,
};

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
        if project_required_scalars(table, rows, &mut destinations) {
            return Ok(columns);
        }
        drop(columns);

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
    let mut destinations = [RequiredScalarColumnDestination::new(
        "FIELD_ID",
        RequiredScalarColumnValuesMut::Int32(&mut field_ids),
    )];
    if project_required_scalars(table, rows, &mut destinations) {
        return Ok(field_ids);
    }
    drop(field_ids);
    load_i32_column(table, "FIELD_ID", rows)
}

fn project_required_scalars(
    table: &Table,
    rows: &[usize],
    destinations: &mut [RequiredScalarColumnDestination<'_>],
) -> bool {
    table.schema().is_some_and(|schema| {
        destinations.iter().all(|destination| {
            schema.column(destination.column()).is_some_and(|column| {
                matches!(column.column_type(), ColumnType::Scalar) && !column.options().undefined
            })
        })
    }) && table
        .required_scalar_columns_for_rows_into(rows, destinations)
        .is_ok()
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
            Some(casa_core::ScalarValue::Int32(v)) => Ok(v),
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
    let mut values = Vec::new();
    let mut destinations = [RequiredScalarColumnDestination::new(
        column,
        RequiredScalarColumnValuesMut::Float64(&mut values),
    )];
    if project_required_scalars(table, rows, &mut destinations) {
        return Ok(values);
    }
    drop(values);
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_core::ScalarValue::Float64(v)) => Ok(v),
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
            Some(casa_core::ScalarValue::Bool(v)) => Ok(v),
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use casa_tables::{ColumnOptions, ColumnSchema, DataManagerKind, TableOptions, TableSchema};

    use super::*;

    fn reopen_row(
        path: &Path,
        schema: Vec<ColumnSchema>,
        fields: Vec<RecordField>,
        manager: DataManagerKind,
    ) -> Table {
        let mut table = Table::with_schema(TableSchema::new(schema).unwrap());
        table.add_row(RecordValue::new(fields)).unwrap();
        table
            .save(TableOptions::new(path).with_data_manager(manager))
            .unwrap();
        Table::open(TableOptions::new(path)).unwrap()
    }

    #[test]
    fn array_field_id_uses_the_scalar_diagnostic() {
        let directory = tempfile::tempdir().unwrap();
        let table = reopen_row(
            &directory.path().join("array-field"),
            vec![ColumnSchema::array_fixed(
                "FIELD_ID",
                PrimitiveType::Int32,
                vec![1],
            )],
            vec![RecordField::new(
                "FIELD_ID",
                Value::Array(ArrayValue::from_i32_vec(vec![0])),
            )],
            DataManagerKind::StandardStMan,
        );
        let error = load_field_ids(&table, &[0]).unwrap_err();
        assert!(error.to_string().contains("scalar"), "{error}");
    }

    #[test]
    fn undefined_flag_retains_the_missing_cell_diagnostic() {
        let directory = tempfile::tempdir().unwrap();
        let ids = ["FIELD_ID", "DATA_DESC_ID", "ANTENNA1", "ANTENNA2"];
        let schema = ids
            .iter()
            .map(|&name| ColumnSchema::scalar(name, PrimitiveType::Int32))
            .chain([
                ColumnSchema::scalar("FLAG_ROW", PrimitiveType::Bool)
                    .with_options(ColumnOptions {
                        undefined: true,
                        ..ColumnOptions::default()
                    })
                    .unwrap(),
                ColumnSchema::scalar("TIME", PrimitiveType::Float64),
            ])
            .collect();
        let fields = ids
            .into_iter()
            .map(|name| RecordField::new(name, Value::Scalar(ScalarValue::Int32(0))))
            .chain([
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(1.0))),
            ])
            .collect();
        let table = reopen_row(
            &directory.path().join("undefined-flag"),
            schema,
            fields,
            DataManagerKind::IncrementalStMan,
        );
        let error = SelectedRowColumns::read(&table, &[0]).err().unwrap();
        assert!(
            matches!(error, MsError::MissingColumn { column, .. } if column == "FLAG_ROW[row=0]")
        );
    }
}
