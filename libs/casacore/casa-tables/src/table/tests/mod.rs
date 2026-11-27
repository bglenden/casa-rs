// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use casa_core::{
    Array2, ArrayD, ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue,
    Value,
};
use ndarray::ShapeBuilder;

use crate::schema::{ColumnSchema, TableSchema};

use super::{
    ColumnBinding, ColumnOverrides, DataManagerKind, EndianFormat, GeneratedScalarColumn,
    GeneratedScalarValueRun, RequiredScalarColumnValues, RowRange, SelectedArray2DCells,
    SelectedArray2DCellsMut, SortOrder, StreamedScalarType, StreamingScalarColumnWriter, Table,
    TableError, TableOptions,
};

mod bindings;
#[cfg(unix)]
mod copy;
mod disk_reads;
mod prepared_rows;
#[cfg(unix)]
mod publish;
mod stale_rows;
mod structure;
use bindings::{
    build_direct_endian_test_table, build_endian_test_table, unique_test_dir,
    verify_endian_test_table,
};
mod data_managers;
mod lifecycle;
#[cfg(unix)]
mod lock_tests;
mod memory;
mod mutation;
mod slicing;
mod virtual_columns;

#[test]
fn uncached_shape_lookup_does_not_buffer_indirect_array_cells() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("shapes");
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "lag",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for lags in [8, 12] {
        let values = Array2::from_elem((2, lags), Complex32::new(1.0, -1.0));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "lag",
                Value::Array(ArrayValue::Complex32(values.into_dyn())),
            )]))
            .unwrap();
    }
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(
        reopened.array_shape_uncached(0, "lag").unwrap(),
        Some(vec![2, 8])
    );
    assert_eq!(
        reopened.array_shape_uncached(1, "lag").unwrap(),
        Some(vec![2, 12])
    );
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_buffered_array_column("lag"));
    assert_eq!(reopened.array_shape(1, "lag").unwrap(), Some(vec![2, 12]));
    assert!(reopened.inner.has_buffered_array_column("lag"));
}

/// `schema` as `manager` stores it: IncrementalStMan stores Direct
/// fixed-shape arrays only, so its fixed-shape arrays become Direct.
fn schema_for_manager(schema: &TableSchema, manager: DataManagerKind) -> TableSchema {
    use crate::schema::{ArrayShapeContract, ColumnOptions, ColumnType};

    if manager != DataManagerKind::IncrementalStMan {
        return schema.clone();
    }
    let columns = schema
        .columns()
        .iter()
        .map(|column| match column.column_type() {
            ColumnType::Array(ArrayShapeContract::Fixed { .. }) => column
                .clone()
                .with_options(ColumnOptions {
                    direct: true,
                    undefined: column.options().undefined,
                })
                .expect("direct fixed-shape array"),
            _ => column.clone(),
        });
    TableSchema::new(columns.collect()).expect("schema")
}

fn row_with_fixed_arrays(id: i32, data: &[i32], other: &[i32]) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
        RecordField::new(
            "data",
            Value::Array(ArrayValue::from_i32_vec(data.to_vec())),
        ),
        RecordField::new(
            "other",
            Value::Array(ArrayValue::from_i32_vec(other.to_vec())),
        ),
    ])
}

fn table_row(table: &Table, row_index: usize) -> Result<&RecordValue, TableError> {
    table.row_accessor().row(row_index)
}

fn table_cell<'a>(
    table: &'a Table,
    row_index: usize,
    column: &str,
) -> Result<Option<&'a Value>, TableError> {
    Ok(table.row_accessor().row(row_index)?.get(column))
}

fn table_scalar<'a>(
    table: &'a Table,
    row_index: usize,
    column: &str,
) -> Result<&'a ScalarValue, TableError> {
    table.column_accessor(column)?.scalar_cell(row_index)
}

fn table_array<'a>(
    table: &'a Table,
    row_index: usize,
    column: &str,
) -> Result<&'a ArrayValue, TableError> {
    table.column_accessor(column)?.array_cell(row_index)
}

fn table_column_cells<'a>(
    table: &'a Table,
    column: &str,
) -> Result<Vec<Option<&'a Value>>, TableError> {
    table.column_accessor(column)?.cells()
}

fn table_column_range<'a>(
    table: &'a Table,
    column: &str,
    row_range: RowRange,
) -> Result<super::ColumnCellIter<'a>, TableError> {
    table.column_accessor(column)?.iter_range(row_range)
}

fn table_record_column<'a>(
    table: &'a Table,
    column: &str,
) -> Result<super::RecordColumnIter<'a>, TableError> {
    table.column_accessor(column)?.record_iter()
}

fn table_record_column_range<'a>(
    table: &'a Table,
    column: &str,
    row_range: RowRange,
) -> Result<super::RecordColumnIter<'a>, TableError> {
    table.column_accessor(column)?.record_iter_range(row_range)
}

fn table_iter_column_chunks<'a>(
    table: &'a Table,
    column: &str,
    row_range: RowRange,
    chunk_size: usize,
) -> Result<super::ColumnChunkIter<'a>, TableError> {
    table.column_accessor(column)?.chunks(row_range, chunk_size)
}

fn table_set_cell(
    table: &mut Table,
    row_index: usize,
    column: &str,
    value: Value,
) -> Result<(), TableError> {
    table.cell_accessor_mut(row_index, column)?.set(value)
}

fn table_set_record_cell(
    table: &mut Table,
    row_index: usize,
    column: &str,
    value: RecordValue,
) -> Result<(), TableError> {
    table
        .cell_accessor_mut(row_index, column)?
        .set_record(value)
}

fn table_put_column_range<I>(
    table: &mut Table,
    column: &str,
    row_range: RowRange,
    values: I,
) -> Result<usize, TableError>
where
    I: IntoIterator<Item = Value>,
{
    table
        .column_accessor_mut(column)?
        .put_range(row_range, values)
}

fn table_write_scalar_bound(
    table: &mut Table,
    row_index: usize,
    column: &str,
    value: ScalarValue,
) -> Result<(), TableError> {
    let mut row = table.row_accessor_mut().prepare(&[column])?;
    row.seek(row_index)?;
    row.set_value_at(0, Value::Scalar(value))?;
    row.flush()
}

fn table_write_array_bound(
    table: &mut Table,
    row_index: usize,
    column: &str,
    value: ArrayValue,
) -> Result<(), TableError> {
    let mut row = table.row_accessor_mut().prepare(&[column])?;
    row.seek(row_index)?;
    row.set_value_at(0, Value::Array(value))?;
    row.flush()
}

fn table_scalar_cells_owned(
    table: &Table,
    column: &str,
) -> Result<Vec<Option<ScalarValue>>, TableError> {
    table.column_accessor(column)?.scalar_cells_owned()
}

fn table_scalar_columns_owned(
    table: &Table,
    columns: &[&str],
) -> Result<HashMap<String, Vec<Option<ScalarValue>>>, TableError> {
    table.scalar_columns_owned(columns)
}

fn table_required_scalar_columns_owned(
    table: &Table,
    columns: &[&str],
) -> Result<HashMap<String, RequiredScalarColumnValues>, TableError> {
    table.required_scalar_columns_owned(columns)
}

fn table_array_cells_owned(
    table: &Table,
    column: &str,
    row_indices: &[usize],
) -> Result<Vec<Option<ArrayValue>>, TableError> {
    table
        .column_accessor(column)?
        .array_cells_owned(row_indices)
}

#[test]
fn table_keeps_rows_in_order() {
    let first = RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(1)),
    )]);
    let second = RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(2)),
    )]);

    let table = Table::from_rows(vec![first.clone(), second.clone()]);
    assert_eq!(table.row_count(), 2);
    assert_eq!(table.rows().unwrap(), &[first, second]);
}

#[test]
fn table_exposes_row_and_column_cell_access() {
    let first = RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new("name", Value::Scalar(ScalarValue::String("a".to_string()))),
    ]);
    let second = RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
        RecordField::new("name", Value::Scalar(ScalarValue::String("b".to_string()))),
    ]);
    let mut table = Table::from_rows(vec![first.clone(), second.clone()]);

    assert_eq!(table_row(&table, 0).unwrap(), &first);
    assert_eq!(
        table_cell(&table, 1, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(2))))
    );

    table_set_cell(
        &mut table,
        1,
        "name",
        Value::Scalar(ScalarValue::String("beta".to_string())),
    )
    .expect("set cell");
    assert_eq!(
        table_cell(&table, 1, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String(
            "beta".to_string()
        ))))
    );

    let id_cells = table_column_cells(&table, "id").unwrap();
    assert_eq!(
        id_cells,
        vec![
            Some(&Value::Scalar(ScalarValue::Int32(1))),
            Some(&Value::Scalar(ScalarValue::Int32(2))),
        ]
    );
}

#[test]
fn canonical_accessors_read_rows_columns_and_cells() {
    let meta = RecordValue::new(vec![RecordField::new(
        "label",
        Value::Scalar(ScalarValue::String("alpha".to_string())),
    )]);
    let data = ArrayValue::from_i32_vec(vec![3, 5, 8]);
    let table = Table::from_rows(vec![RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(7))),
        RecordField::new("data", Value::Array(data.clone())),
        RecordField::new("meta", Value::Record(meta.clone())),
    ])]);

    let rows = table.row_accessor();
    let row = rows.row(0).expect("row 0");
    assert_eq!(row.get("id"), Some(&Value::Scalar(ScalarValue::Int32(7))));

    let column = table.column_accessor("data").expect("column accessor");
    assert_eq!(column.array_cell(0).expect("array cell"), &data);
    let iterated: Vec<_> = column
        .iter()
        .expect("column iter")
        .map(|cell| (cell.row_index, cell.value.cloned()))
        .collect();
    assert_eq!(iterated, vec![(0, Some(Value::Array(data.clone())))]);

    let cell = table.cell_accessor(0, "meta").expect("cell accessor");
    assert_eq!(cell.row_index(), 0);
    assert_eq!(cell.column(), "meta");
    assert!(cell.is_defined().expect("defined"));
    assert_eq!(cell.record().expect("record cell"), meta);
}

#[test]
fn mutable_accessors_update_rows_columns_and_cells() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ColumnSchema::record("meta"),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![2, 3]))),
            RecordField::new(
                "meta",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "label",
                    Value::Scalar(ScalarValue::String("old".to_string())),
                )])),
            ),
        ]))
        .expect("add row");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![5, 8]))),
            RecordField::new(
                "meta",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "label",
                    Value::Scalar(ScalarValue::String("keep".to_string())),
                )])),
            ),
        ]))
        .expect("add second row");

    table
        .cell_accessor_mut(1, "id")
        .expect("scalar cell accessor")
        .set(Value::Scalar(ScalarValue::Int32(11)))
        .expect("set scalar");

    table
        .column_accessor_mut("data")
        .expect("column accessor mut")
        .set(1, Value::Array(ArrayValue::from_i32_vec(vec![13, 21])))
        .expect("set array");

    table
        .row_accessor_mut()
        .set_record_cell(
            1,
            "meta",
            RecordValue::new(vec![RecordField::new(
                "label",
                Value::Scalar(ScalarValue::String("new".to_string())),
            )]),
        )
        .expect("set record");

    {
        let mut rows = table.row_accessor_mut();
        let row = rows.row_mut(1).expect("mutable row accessor");
        row.upsert("extra", Value::Scalar(ScalarValue::Bool(true)));
    }

    assert_eq!(
        table_scalar(&table, 0, "id").expect("first id"),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        table_array(&table, 0, "data").expect("first data"),
        &ArrayValue::from_i32_vec(vec![2, 3])
    );
    assert_eq!(
        table.record_cell(0, "meta").expect("first meta"),
        RecordValue::new(vec![RecordField::new(
            "label",
            Value::Scalar(ScalarValue::String("old".to_string())),
        )])
    );
    assert_eq!(table_cell(&table, 0, "extra"), Ok(None));
    assert_eq!(
        table_scalar(&table, 1, "id").expect("id"),
        &ScalarValue::Int32(11)
    );
    assert_eq!(
        table_array(&table, 1, "data").expect("data"),
        &ArrayValue::from_i32_vec(vec![13, 21])
    );
    assert_eq!(
        table.record_cell(1, "meta").expect("meta"),
        RecordValue::new(vec![RecordField::new(
            "label",
            Value::Scalar(ScalarValue::String("new".to_string())),
        )])
    );
    assert_eq!(
        table_cell(&table, 1, "extra"),
        Ok(Some(&Value::Scalar(ScalarValue::Bool(true))))
    );
}

#[test]
fn cell_accessors_reject_unknown_columns() {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .expect("add row");

    assert!(matches!(
        table.cell_accessor(0, "missing"),
        Err(TableError::SchemaColumnUnknown { column }) if column == "missing"
    ));
    assert!(matches!(
        table.cell_accessor_mut(0, "missing"),
        Err(TableError::SchemaColumnUnknown { column }) if column == "missing"
    ));
}

#[test]
fn column_range_iteration_supports_stride() {
    let rows = (0..6)
        .map(|value| {
            RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(value)),
            )])
        })
        .collect();
    let table = Table::from_rows(rows);

    let cells: Vec<(usize, Option<Value>)> =
        table_column_range(&table, "id", RowRange::with_stride(1, 6, 2))
            .expect("get strided range")
            .map(|cell| (cell.row_index, cell.value.cloned()))
            .collect();
    assert_eq!(
        cells,
        vec![
            (1, Some(Value::Scalar(ScalarValue::Int32(1)))),
            (3, Some(Value::Scalar(ScalarValue::Int32(3)))),
            (5, Some(Value::Scalar(ScalarValue::Int32(5)))),
        ]
    );
}

#[test]
fn column_range_rejects_invalid_ranges() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(1)),
    )])]);

    let bad_stride = table_column_range(&table, "id", RowRange::with_stride(0, 1, 0));
    assert!(matches!(
        bad_stride,
        Err(TableError::InvalidRowStride { stride: 0 })
    ));

    let bad_end = table_column_range(&table, "id", RowRange::new(0, 2));
    assert!(matches!(
        bad_end,
        Err(TableError::InvalidRowRange {
            start: 0,
            end: 2,
            row_count: 1,
        })
    ));
}

#[test]
fn query_result_reports_view_and_materialized_results() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for (id, name) in [(0, "alpha"), (1, "beta"), (2, "gamma")] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(name.to_string()))),
            ]))
            .unwrap();
    }

    let view = table.query_result("SELECT id, name WHERE id > 0").unwrap();
    assert!(format!("{view:?}").contains("QueryResult::View"));
    assert_eq!(view.row_count(), 2);
    assert_eq!(
        view.column_names(),
        vec!["id".to_string(), "name".to_string()]
    );
    assert_eq!(
        view.row(0).unwrap().get("name"),
        Some(&Value::Scalar(ScalarValue::String("beta".to_string())))
    );

    let materialized = table
        .query_result("SELECT id AS source_id, name AS label WHERE id > 0")
        .unwrap();
    assert!(format!("{materialized:?}").contains("QueryResult::Materialized"));
    assert_eq!(materialized.row_count(), 2);
    assert_eq!(
        materialized.column_names(),
        vec!["source_id".to_string(), "label".to_string()]
    );
    assert_eq!(
        materialized.row(0).unwrap().get("label"),
        Some(&Value::Scalar(ScalarValue::String("beta".to_string())))
    );

    let err = table.query_result("COUNT SELECT *").unwrap_err();
    assert!(
        matches!(err, TableError::Taql(msg) if msg.contains("only supports SELECT statements"))
    );
}

#[test]
fn mutable_selection_sort_and_query_variants_write_through() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for (id, name) in [(0, "alpha"), (1, "beta"), (2, "gamma")] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(name.to_string()))),
            ]))
            .unwrap();
    }

    {
        let mut projected = table.select_columns_mut(&["name"]).unwrap();
        assert_eq!(projected.column_names(), &["name"]);
        assert_eq!(projected.schema().unwrap().columns().len(), 1);
        assert_eq!(
            projected.row(0).unwrap().get("id"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert!(projected.parent_path().is_none());
        assert_eq!(projected.as_ref().column_names(), &["name"]);
        projected
            .set_cell(
                1,
                "name",
                Value::Scalar(ScalarValue::String("projected".into())),
            )
            .unwrap();
    }
    assert_eq!(
        table_cell(&table, 1, "name").unwrap(),
        Some(&Value::Scalar(ScalarValue::String("projected".to_string())))
    );

    {
        let mut selected = table
            .select_mut(|row| {
                matches!(
                    row.get("id"),
                    Some(Value::Scalar(ScalarValue::Int32(id))) if *id >= 1
                )
            })
            .unwrap();
        assert_eq!(selected.row_numbers(), &[1, 2]);
        selected
            .set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("selected".into())),
            )
            .unwrap();
    }
    assert_eq!(
        table_cell(&table, 1, "name").unwrap(),
        Some(&Value::Scalar(ScalarValue::String("selected".to_string())))
    );

    {
        let mut sorted = table.sort_mut(&[("id", SortOrder::Descending)]).unwrap();
        assert_eq!(sorted.row_numbers(), &[2, 1, 0]);
        sorted
            .set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("sorted".into())),
            )
            .unwrap();
    }
    assert_eq!(
        table_cell(&table, 2, "name").unwrap(),
        Some(&Value::Scalar(ScalarValue::String("sorted".to_string())))
    );

    {
        let mut sorted = table
            .sort_by_mut("id", |a, b| match (a, b) {
                (Value::Scalar(ScalarValue::Int32(a)), Value::Scalar(ScalarValue::Int32(b))) => {
                    b.cmp(a)
                }
                _ => std::cmp::Ordering::Equal,
            })
            .unwrap();
        assert_eq!(sorted.row_numbers(), &[2, 1, 0]);
        sorted
            .set_cell(
                1,
                "name",
                Value::Scalar(ScalarValue::String("custom".into())),
            )
            .unwrap();
    }
    assert_eq!(
        table_cell(&table, 1, "name").unwrap(),
        Some(&Value::Scalar(ScalarValue::String("custom".to_string())))
    );

    {
        let mut queried = table.query_mut("SELECT * WHERE id = 0").unwrap();
        queried
            .set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("queried".into())),
            )
            .unwrap();
    }
    assert_eq!(
        table_cell(&table, 0, "name").unwrap(),
        Some(&Value::Scalar(ScalarValue::String("queried".to_string())))
    );

    {
        let mut queried = table.query_mut("SELECT name WHERE id = 2").unwrap();
        assert_eq!(queried.column_names(), &["name"]);
        queried
            .set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("projected_query".into())),
            )
            .unwrap();
    }
    assert_eq!(
        table_cell(&table, 2, "name").unwrap(),
        Some(&Value::Scalar(ScalarValue::String(
            "projected_query".to_string()
        )))
    );

    let readonly = table.query("SELECT name WHERE id >= 1").unwrap();
    assert_eq!(readonly.column_names(), &["name"]);
    assert_eq!(readonly.row_count(), 2);

    let query_err = match table.query("COUNT SELECT *") {
        Ok(_) => panic!("COUNT SELECT should not produce a RefTable view"),
        Err(err) => err,
    };
    assert!(
        matches!(query_err, TableError::Taql(msg) if msg.contains("only supports SELECT statements"))
    );

    let query_mut_err = match table.query_mut("COUNT SELECT *") {
        Ok(_) => panic!("COUNT SELECT should not produce a mutable RefTable view"),
        Err(err) => err,
    };
    assert!(
        matches!(query_mut_err, TableError::Taql(msg) if msg.contains("only supports SELECT statements"))
    );
}

#[test]
fn lazy_materialization_failures_return_storage_errors() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("persisted".to_string())),
            ),
        ]))
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lazy_error.tbl");
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();

    let mut removed = 0usize;
    for entry in std::fs::read_dir(&path).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("table.f") {
            std::fs::remove_file(entry.path()).unwrap();
            removed += 1;
        }
    }
    assert!(removed > 0, "expected row-storage files to remove");

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.row_count(), 1);
    assert!(matches!(
        table_row(&reopened, 0),
        Err(TableError::Storage(msg)) if msg.contains("failed to materialize rows")
    ));
    assert!(matches!(
        table_cell(&reopened, 0, "id"),
        Err(TableError::Storage(msg)) if msg.contains("failed to materialize rows")
    ));

    let selected = reopened.select_rows(&[0]).unwrap();
    assert!(matches!(
        selected.row(0),
        Err(TableError::Storage(msg)) if msg.contains("failed to materialize rows")
    ));
}

#[test]
fn sort_by_reports_missing_columns_and_orders_defined_before_undefined() {
    use crate::schema::ColumnOptions;

    let mut table = Table::with_schema(
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap(),
    );
    for id in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(id)),
            )]))
            .unwrap();
    }
    table
        .add_column(
            ColumnSchema::scalar("opt", PrimitiveType::Int32)
                .with_options(ColumnOptions {
                    direct: false,
                    undefined: true,
                })
                .unwrap(),
            None,
        )
        .unwrap();
    table_set_cell(&mut table, 0, "opt", Value::Scalar(ScalarValue::Int32(10))).unwrap();
    table_set_cell(&mut table, 2, "opt", Value::Scalar(ScalarValue::Int32(5))).unwrap();

    let sorted = table
        .sort_by("opt", |a, b| match (a, b) {
            (Value::Scalar(ScalarValue::Int32(a)), Value::Scalar(ScalarValue::Int32(b))) => {
                a.cmp(b)
            }
            other => panic!("unexpected comparator inputs: {other:?}"),
        })
        .unwrap();
    assert_eq!(sorted.row_numbers(), &[2, 0, 1, 3]);

    let mut persisted = Table::with_schema(
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap(),
    );
    for id in 0..64 {
        persisted
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(id)),
            )]))
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sort_lazy_error.tbl");
    persisted
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();
    for entry in std::fs::read_dir(&path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("table.f") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    let err = match reopened.sort_by("id", |_, _| std::cmp::Ordering::Equal) {
        Ok(_) => panic!("sorting should fail when row materialization fails"),
        Err(err) => err,
    };
    assert!(
        matches!(err, TableError::Storage(message) if message.contains("failed to materialize rows"))
    );
}

#[test]
fn schema_record_cell_defaults_to_empty_record_when_missing() {
    let schema =
        TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![]))
        .expect("missing record cell should be valid");

    assert_eq!(table.record_cell(0, "meta"), Ok(RecordValue::default()));
    assert_eq!(table.is_cell_defined(0, "meta"), Ok(true));
}

#[test]
fn record_cell_requires_present_value_without_schema() {
    let table = Table::from_rows(vec![RecordValue::new(vec![])]);
    assert_eq!(
        table.record_cell(0, "meta"),
        Err(TableError::ColumnNotFound {
            row_index: 0,
            column: "meta".to_string(),
        })
    );
}

#[test]
fn record_cell_rejects_non_record_schema_column() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)])
        .expect("create scalar schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .expect("push schema-compliant row");

    assert_eq!(
        table.record_cell(0, "id"),
        Err(TableError::SchemaColumnNotRecord {
            column: "id".to_string(),
        })
    );
}

#[test]
fn record_column_range_defaults_missing_cells_for_record_schema() {
    let schema =
        TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");

    let first = RecordValue::new(vec![RecordField::new(
        "flag",
        Value::Scalar(ScalarValue::Bool(true)),
    )]);
    let second = RecordValue::new(vec![RecordField::new(
        "flag",
        Value::Scalar(ScalarValue::Bool(false)),
    )]);
    let rows = vec![
        RecordValue::new(vec![RecordField::new("meta", Value::Record(first.clone()))]),
        RecordValue::new(vec![]),
        RecordValue::new(vec![RecordField::new(
            "meta",
            Value::Record(second.clone()),
        )]),
    ];
    let table = Table::from_rows_with_schema(rows, schema).expect("schema-valid rows");

    let cells: Vec<(usize, RecordValue)> =
        table_record_column_range(&table, "meta", RowRange::new(0, 3))
            .expect("iterate record column")
            .map(|cell| (cell.row_index, cell.value))
            .collect();

    assert_eq!(
        cells,
        vec![(0, first), (1, RecordValue::default()), (2, second),]
    );
}

#[test]
fn record_column_range_without_schema_requires_all_rows_present() {
    let record = RecordValue::new(vec![RecordField::new(
        "meta",
        Value::Record(RecordValue::default()),
    )]);
    let table = Table::from_rows(vec![record, RecordValue::new(vec![])]);

    assert_eq!(
        table_record_column(&table, "meta").map(|iter| iter.count()),
        Err(TableError::ColumnNotFound {
            row_index: 1,
            column: "meta".to_string(),
        })
    );
}

#[test]
fn record_column_range_rejects_non_record_cells() {
    let table = Table::from_rows(vec![
        RecordValue::new(vec![RecordField::new(
            "meta",
            Value::Record(RecordValue::default()),
        )]),
        RecordValue::new(vec![RecordField::new(
            "meta",
            Value::Scalar(ScalarValue::Int32(9)),
        )]),
    ]);

    assert_eq!(
        table_record_column(&table, "meta").map(|iter| iter.count()),
        Err(TableError::ColumnTypeMismatch {
            row_index: 1,
            column: "meta".to_string(),
            expected: "record",
            found: casa_core::ValueKind::Scalar,
        })
    );
}

#[test]
fn set_record_cell_updates_row() {
    let schema =
        TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![]))
        .expect("push schema-compliant row");
    let payload = RecordValue::new(vec![RecordField::new(
        "code",
        Value::Scalar(ScalarValue::Int32(42)),
    )]);

    table_set_record_cell(&mut table, 0, "meta", payload.clone()).expect("set record cell");
    assert_eq!(table.record_cell(0, "meta"), Ok(payload));
}

#[test]
fn standard_stman_round_trips_scalar_record_columns() {
    let schema =
        TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");
    let payload = RecordValue::new(vec![
        RecordField::new("code", Value::Scalar(ScalarValue::Int32(42))),
        RecordField::new(
            "nested",
            Value::Record(RecordValue::new(vec![RecordField::new(
                "label",
                Value::Scalar(ScalarValue::String("source-model".into())),
            )])),
        ),
    ]);

    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "meta",
            Value::Record(payload.clone()),
        )]))
        .expect("push defined record row");
    table
        .add_row(RecordValue::new(vec![]))
        .expect("push undefined record row");

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("record_ssm.tbl");
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save StandardStMan record table");

    let reopened = Table::open(TableOptions::new(&path)).expect("reopen StandardStMan table");
    assert_eq!(reopened.record_cell(0, "meta"), Ok(payload));
    assert_eq!(reopened.record_cell(1, "meta"), Ok(RecordValue::default()));
}

// -------------------------------------------------------------------
// Wave 24 — Data manager introspection
// -------------------------------------------------------------------
