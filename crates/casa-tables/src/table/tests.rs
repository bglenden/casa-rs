// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use casa_types::{
    Array2, ArrayD, ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ShapeBuilder;

use crate::schema::{ColumnSchema, TableSchema};

use super::{
    ColumnBinding, ColumnOverrides, DataManagerKind, EndianFormat, GeneratedScalarColumn,
    GeneratedScalarValueRun, RequiredScalarColumnValues, RowRange, SelectedArray2DCells, SortOrder,
    Table, TableError, TableOptions,
};

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

fn table_set_scalar_assuming_valid(
    table: &mut Table,
    row_index: usize,
    column: &str,
    value: ScalarValue,
) -> Result<(), TableError> {
    table
        .column_accessor_mut(column)?
        .set_scalar_assuming_valid(row_index, value)
}

fn table_set_array_assuming_valid(
    table: &mut Table,
    row_index: usize,
    column: &str,
    value: ArrayValue,
) -> Result<(), TableError> {
    table
        .column_accessor_mut(column)?
        .set_array_assuming_valid(row_index, value)
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
        .set_scalar_assuming_valid(ScalarValue::Int32(11))
        .expect("set scalar");

    table
        .column_accessor_mut("data")
        .expect("column accessor mut")
        .set_array_assuming_valid(1, ArrayValue::from_i32_vec(vec![13, 21]))
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
            found: casa_types::ValueKind::Scalar,
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

#[test]
fn put_column_range_streams_values_without_column_vecs() {
    let mut table = Table::from_rows(vec![
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("a".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("b".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("c".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("d".to_string())),
        )]),
    ]);

    let written = table_put_column_range(
        &mut table,
        "name",
        RowRange::with_stride(0, 4, 2),
        ["x", "y"]
            .into_iter()
            .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
    )
    .expect("put strided range");
    assert_eq!(written, 2);
    assert_eq!(
        table_cell(&table, 0, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String("x".to_string()))))
    );
    assert_eq!(
        table_cell(&table, 1, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String("b".to_string()))))
    );
    assert_eq!(
        table_cell(&table, 2, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String("y".to_string()))))
    );
    assert_eq!(
        table_cell(&table, 3, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String("d".to_string()))))
    );
}

#[test]
fn put_column_range_checks_value_count() {
    let mut table = Table::from_rows(vec![
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("a".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("b".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("c".to_string())),
        )]),
    ]);

    let too_few = table_put_column_range(
        &mut table,
        "name",
        RowRange::new(0, 3),
        ["x", "y"]
            .into_iter()
            .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
    );
    assert_eq!(
        too_few,
        Err(TableError::ColumnWriteTooFewValues {
            expected: 3,
            provided: 2,
        })
    );

    let mut table = Table::from_rows(vec![
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("a".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("b".to_string())),
        )]),
        RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("c".to_string())),
        )]),
    ]);
    let too_many = table_put_column_range(
        &mut table,
        "name",
        RowRange::new(0, 3),
        ["x", "y", "z", "w"]
            .into_iter()
            .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
    );
    assert_eq!(
        too_many,
        Err(TableError::ColumnWriteTooManyValues { expected: 3 })
    );
}

#[test]
fn fixed_array_schema_enforces_defined_shape() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Int32,
        vec![2],
    )])
    .expect("create schema");
    let mut table = Table::with_schema(schema);

    let missing = table.add_row(RecordValue::new(vec![]));
    assert_eq!(
        missing,
        Err(TableError::SchemaColumnMissing {
            row_index: 0,
            column: "data".to_string(),
        })
    );

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_i32_vec(vec![1, 2])),
        )]))
        .expect("push valid fixed-shape row");

    let wrong_shape = table.add_row(RecordValue::new(vec![RecordField::new(
        "data",
        Value::Array(ArrayValue::from_i32_vec(vec![3])),
    )]));
    assert_eq!(
        wrong_shape,
        Err(TableError::ArrayShapeMismatch {
            row_index: 1,
            column: "data".to_string(),
            expected: vec![2],
            found: vec![1],
        })
    );
}

#[test]
fn variable_array_schema_allows_undefined_and_checks_ndim() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "payload",
        PrimitiveType::Int32,
        Some(1),
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);

    table
        .add_row(RecordValue::new(vec![]))
        .expect("undefined variable-shape cell should be allowed");

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "payload",
            Value::Array(ArrayValue::from_i32_vec(vec![1, 2, 3])),
        )]))
        .expect("1d array should satisfy ndim=1");

    let two_d = Array2::from_shape_vec((1, 2), vec![4, 5])
        .expect("shape")
        .into_dyn();
    let error = table_set_cell(
        &mut table,
        0,
        "payload",
        Value::Array(ArrayValue::Int32(two_d)),
    );
    assert_eq!(
        error,
        Err(TableError::ArrayNdimMismatch {
            row_index: 0,
            column: "payload".to_string(),
            expected: 1,
            found: 2,
        })
    );
}

#[test]
fn table_schema_round_trips_through_disk_storage() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema.clone());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
        ]))
        .expect("push schema-compliant row");
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("rust-test".to_string())),
    ));

    let root = unique_test_dir("table_schema_round_trip");
    std::fs::create_dir_all(&root).expect("create test dir");

    table
        .save(TableOptions::new(&root))
        .expect("save disk-backed table");
    let reopened = Table::open(TableOptions::new(&root)).expect("open disk-backed table");

    assert_eq!(reopened.row_count(), 1);
    assert_eq!(reopened.schema(), Some(&schema));
    assert_eq!(
        table_cell(&reopened, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(42))))
    );
    assert_eq!(
        reopened.keywords().get("observer"),
        Some(&Value::Scalar(ScalarValue::String("rust-test".to_string())))
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn table_keywords_round_trip_through_disk_storage() {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::from_rows_with_schema(
        vec![RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(42)),
        )])],
        schema,
    )
    .expect("create table");
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("rust-test".to_string())),
    ));

    let root = unique_test_dir("table_keywords_round_trip");
    std::fs::create_dir_all(&root).expect("create test dir");

    table
        .save(TableOptions::new(&root))
        .expect("save disk-backed table");
    let reopened = Table::open(TableOptions::new(&root)).expect("open disk-backed table");

    assert_eq!(reopened.row_count(), 1);
    assert_eq!(
        table_cell(&reopened, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(42))))
    );
    assert_eq!(
        reopened.keywords().get("observer"),
        Some(&Value::Scalar(ScalarValue::String("rust-test".to_string())))
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn metadata_only_open_loads_schema_and_keywords_without_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema.clone());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
        ]))
        .expect("push schema-compliant row");
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("rust-test".to_string())),
    ));

    let root = unique_test_dir("table_metadata_only_open");
    std::fs::create_dir_all(&root).expect("create test dir");

    table
        .save(TableOptions::new(&root))
        .expect("save disk-backed table");
    let reopened = Table::open_metadata_only(TableOptions::new(&root)).expect("metadata-only");

    assert_eq!(reopened.row_count(), 0);
    assert_eq!(reopened.schema(), Some(&schema));
    assert_eq!(
        reopened.keywords().get("observer"),
        Some(&Value::Scalar(ScalarValue::String("rust-test".to_string())))
    );
    assert_eq!(reopened.data_manager_info().len(), 1);

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn metadata_only_save_updates_keywords_without_rewriting_row_storage() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema.clone());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
        ]))
        .expect("push schema-compliant row");
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("before".to_string())),
    ));

    let root = unique_test_dir("table_metadata_only_save");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root))
        .expect("save disk-backed table");

    let mut metadata_only =
        Table::open_metadata_only(TableOptions::new(&root)).expect("metadata-only");
    metadata_only.keywords_mut().upsert(
        "observer",
        Value::Scalar(ScalarValue::String("after".to_string())),
    );
    metadata_only
        .save_metadata_only(TableOptions::new(&root))
        .expect("save metadata only");

    let reopened = Table::open(TableOptions::new(&root)).expect("full reopen");
    assert_eq!(reopened.row_count(), 1);
    assert_eq!(
        reopened.keywords().get("observer"),
        Some(&Value::Scalar(ScalarValue::String("after".to_string())))
    );
    assert_eq!(
        table_cell(&reopened, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(42))))
    );
    assert_eq!(
        table_cell(&reopened, 0, "data"),
        Ok(Some(&Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))))
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn iter_column_chunks_batches_rows() {
    let rows: Vec<RecordValue> = (0..7)
        .map(|v| {
            RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(v)),
            )])
        })
        .collect();
    let table = Table::from_rows(rows);

    let chunks: Vec<Vec<(usize, i32)>> =
        table_iter_column_chunks(&table, "id", RowRange::new(0, 7), 3)
            .expect("chunk iter")
            .map(|chunk| {
                chunk
                    .into_iter()
                    .map(|cell| {
                        let v = match cell.value {
                            Some(Value::Scalar(ScalarValue::Int32(n))) => n,
                            _ => panic!("expected i32"),
                        };
                        (cell.row_index, *v)
                    })
                    .collect()
            })
            .collect();

    assert_eq!(
        chunks,
        vec![
            vec![(0, 0), (1, 1), (2, 2)],
            vec![(3, 3), (4, 4), (5, 5)],
            vec![(6, 6)],
        ]
    );
}

#[test]
fn iter_column_chunks_with_stride() {
    let rows: Vec<RecordValue> = (0..6)
        .map(|v| {
            RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(v)),
            )])
        })
        .collect();
    let table = Table::from_rows(rows);

    let chunks: Vec<Vec<usize>> =
        table_iter_column_chunks(&table, "id", RowRange::with_stride(0, 6, 2), 2)
            .expect("chunk iter")
            .map(|chunk| chunk.into_iter().map(|cell| cell.row_index).collect())
            .collect();

    assert_eq!(chunks, vec![vec![0, 2], vec![4]]);
}

#[test]
fn get_array_cell_returns_borrow() {
    let array = ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0]);
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "data",
        Value::Array(array.clone()),
    )])]);

    let borrowed = table_array(&table, 0, "data").expect("get array cell");
    assert_eq!(borrowed, &array);
}

#[test]
fn get_array_cell_rejects_non_array() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(42)),
    )])]);

    assert!(matches!(
        table_array(&table, 0, "id"),
        Err(TableError::ColumnTypeMismatch { .. })
    ));
}

#[test]
fn get_array_cell_rejects_missing() {
    let table = Table::from_rows(vec![RecordValue::new(vec![])]);

    assert!(matches!(
        table_array(&table, 0, "data"),
        Err(TableError::ColumnNotFound { .. })
    ));
}

#[test]
fn get_scalar_cell_returns_borrow() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(42)),
    )])]);

    let borrowed = table_scalar(&table, 0, "id").expect("get scalar cell");
    assert_eq!(borrowed, &ScalarValue::Int32(42));
}

#[test]
fn lazy_disk_open_reads_cells_without_materializing_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(42))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push row");

        let root = unique_test_dir(&format!("lazy_scalar_open_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());
        assert_eq!(
            table_scalar(&reopened, 0, "id").expect("scalar access"),
            &ScalarValue::Int32(42)
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "scalar access should not force row materialization for {dm:?}"
        );

        let array = table_array(&reopened, 0, "data").expect("array access");
        assert_eq!(array, &ArrayValue::from_i32_vec(vec![7, 9]));
        assert!(
            !reopened.inner.has_loaded_rows(),
            "array access should not force row materialization for {dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn prepared_row_reader_reuses_buffer_without_materializing_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("other", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(row_with_fixed_arrays(1, &[7, 9], &[100, 200]))
            .expect("push row 0");
        table
            .add_row(row_with_fixed_arrays(2, &[11, 13], &[300, 400]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("prepared_row_reader_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        let mut prepared = reopened
            .row_accessor()
            .prepare(&["id", "data"])
            .expect("prepare row reader");
        let id_index = prepared.column_index("id").expect("id index");
        let data_index = prepared.column_index("data").expect("data index");

        prepared.load(0).expect("load first row");
        assert_eq!(
            prepared.scalar_at(id_index).expect("id slot"),
            &ScalarValue::Int32(1)
        );
        assert_eq!(
            prepared.array_at(data_index).expect("data slot"),
            &ArrayValue::from_i32_vec(vec![7, 9])
        );
        let first_buffer_ptr = prepared
            .row()
            .expect("materialized first row")
            .fields()
            .as_ptr();
        assert!(!reopened.inner.has_loaded_rows());
        assert!(!reopened.inner.has_loaded_scalar_column("id"));
        assert!(!reopened.inner.has_loaded_array_column("data"));
        assert!(!reopened.inner.has_loaded_array_column("other"));

        prepared.load(1).expect("load second row");
        assert_eq!(
            prepared.scalar_at(id_index).expect("id slot"),
            &ScalarValue::Int32(2)
        );
        assert_eq!(
            prepared.array_at(data_index).expect("data slot"),
            &ArrayValue::from_i32_vec(vec![11, 13])
        );
        assert_eq!(
            prepared
                .row()
                .expect("materialized second row")
                .fields()
                .as_ptr(),
            first_buffer_ptr
        );
        assert!(!reopened.inner.has_loaded_rows());
        assert!(!reopened.inner.has_loaded_scalar_column("id"));
        assert!(!reopened.inner.has_loaded_array_column("data"));
        assert!(!reopened.inner.has_loaded_array_column("other"));

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn prepared_row_writer_reuses_buffer_and_keeps_sparse_updates() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("other", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(row_with_fixed_arrays(1, &[7, 9], &[100, 200]))
            .expect("push row 0");
        table
            .add_row(row_with_fixed_arrays(2, &[11, 13], &[300, 400]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("prepared_row_writer_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        let mut prepared = reopened
            .row_accessor_mut()
            .prepare(&["id", "data"])
            .expect("prepare row writer");
        let id_index = prepared.column_index("id").expect("id index");
        let data_index = prepared.column_index("data").expect("data index");

        prepared.load(0).expect("load first row");
        let first_buffer_ptr = prepared.row().expect("row loaded").fields().as_ptr();
        {
            let row = prepared.row_mut().expect("mutable row");
            row.fields_mut()[id_index].value = Value::Scalar(ScalarValue::Int32(10));
            row.fields_mut()[data_index].value =
                Value::Array(ArrayValue::from_i32_vec(vec![70, 90]));
        }

        prepared.load(1).expect("load second row");
        assert_eq!(
            prepared.row().expect("row loaded").fields().as_ptr(),
            first_buffer_ptr
        );
        {
            let row = prepared.row_mut().expect("mutable row");
            row.fields_mut()[id_index].value = Value::Scalar(ScalarValue::Int32(20));
            row.fields_mut()[data_index].value =
                Value::Array(ArrayValue::from_i32_vec(vec![110, 130]));
        }
        prepared.flush().expect("flush prepared rows");

        assert!(
            !reopened.inner.has_loaded_rows(),
            "prepared row writes should keep lazy row state for {dm:?}"
        );
        assert!(
            reopened.inner.has_loaded_array_column("data")
                || reopened.inner.has_pending_array_cells("data"),
            "prepared row writes should keep updates isolated to the prepared data column for {dm:?}"
        );
        assert!(
            !reopened.inner.has_loaded_array_column("other"),
            "prepared row writes should not touch unrelated array columns for {dm:?}"
        );
        assert_eq!(
            table_scalar(&reopened, 0, "id").expect("buffered scalar row 0"),
            &ScalarValue::Int32(10)
        );
        assert_eq!(
            table_scalar(&reopened, 1, "id").expect("buffered scalar row 1"),
            &ScalarValue::Int32(20)
        );

        reopened
            .save_selected_rows_in_place_assuming_valid(&["id", "data"], &[0, 1])
            .expect("save prepared row updates");

        let verify = Table::open(TableOptions::new(&root)).expect("reopen after prepared writes");
        assert_eq!(
            table_scalar(&verify, 0, "id").expect("id row 0"),
            &ScalarValue::Int32(10)
        );
        assert_eq!(
            table_scalar(&verify, 1, "id").expect("id row 1"),
            &ScalarValue::Int32(20)
        );
        assert_eq!(
            table_array(&verify, 0, "data").expect("data row 0"),
            &ArrayValue::from_i32_vec(vec![70, 90])
        );
        assert_eq!(
            table_array(&verify, 1, "data").expect("data row 1"),
            &ArrayValue::from_i32_vec(vec![110, 130])
        );
        assert_eq!(
            table_array(&verify, 0, "other").expect("other row 0"),
            &ArrayValue::from_i32_vec(vec![100, 200])
        );
        assert_eq!(
            table_array(&verify, 1, "other").expect("other row 1"),
            &ArrayValue::from_i32_vec(vec![300, 400])
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn prepared_row_writer_seek_keeps_buffer_unmaterialized_and_direct_writes_coherent() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("other", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(row_with_fixed_arrays(1, &[1, 2], &[100, 200]))
            .expect("push row 0");
        table
            .add_row(row_with_fixed_arrays(2, &[3, 4], &[300, 400]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("prepared_row_writer_seek_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        let mut prepared = reopened
            .row_accessor_mut()
            .prepare(&["id", "data"])
            .expect("prepare row writer");
        let id_index = prepared.column_index("id").expect("id index");
        let data_index = prepared.column_index("data").expect("data index");

        prepared.seek(0).expect("seek first row");
        assert!(
            prepared.row().is_none(),
            "seek should not materialize the row buffer for {dm:?}"
        );
        assert!(
            prepared.row_mut().is_none(),
            "row_mut should stay unavailable until load materializes the buffer for {dm:?}"
        );
        prepared
            .set_value_at(id_index, Value::Scalar(ScalarValue::Int32(10)))
            .expect("direct scalar write");
        prepared
            .set_value_at(
                data_index,
                Value::Array(ArrayValue::from_i32_vec(vec![70, 90])),
            )
            .expect("direct array write");
        assert!(
            prepared.row().is_none(),
            "direct writes through seek should not invent a row buffer for {dm:?}"
        );

        prepared.load(1).expect("load second row");
        prepared
            .set_value_at(id_index, Value::Scalar(ScalarValue::Int32(20)))
            .expect("direct write-through keeps loaded buffer coherent");
        assert_eq!(
            prepared.row().expect("loaded row").fields()[id_index].value,
            Value::Scalar(ScalarValue::Int32(20))
        );

        reopened
            .save_selected_rows_in_place_assuming_valid(&["id", "data"], &[0, 1])
            .expect("save prepared row updates");

        let verify = Table::open(TableOptions::new(&root)).expect("reopen after prepared writes");
        assert_eq!(
            table_scalar(&verify, 0, "id").expect("id row 0"),
            &ScalarValue::Int32(10)
        );
        assert_eq!(
            table_scalar(&verify, 1, "id").expect("id row 1"),
            &ScalarValue::Int32(20)
        );
        assert_eq!(
            table_array(&verify, 0, "data").expect("data row 0"),
            &ArrayValue::from_i32_vec(vec![70, 90])
        );
        assert_eq!(
            table_array(&verify, 1, "data").expect("data row 1"),
            &ArrayValue::from_i32_vec(vec![3, 4])
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn prepared_row_reader_sees_pending_and_cached_lazy_updates() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("other", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(row_with_fixed_arrays(1, &[7, 9], &[100, 200]))
            .expect("push row 0");
        table
            .add_row(row_with_fixed_arrays(2, &[11, 13], &[300, 400]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("prepared_row_reader_pending_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        table_set_scalar_assuming_valid(&mut reopened, 0, "id", ScalarValue::Int32(10))
            .expect("pending scalar update");
        table_set_array_assuming_valid(
            &mut reopened,
            0,
            "data",
            ArrayValue::from_i32_vec(vec![70, 90]),
        )
        .expect("pending array update");

        table_scalar(&reopened, 1, "id").expect("prime scalar cache");
        table_array(&reopened, 1, "data").expect("prime array cache");
        table_set_scalar_assuming_valid(&mut reopened, 1, "id", ScalarValue::Int32(20))
            .expect("cached scalar update");
        table_set_array_assuming_valid(
            &mut reopened,
            1,
            "data",
            ArrayValue::from_i32_vec(vec![110, 130]),
        )
        .expect("cached array update");

        let mut prepared = reopened
            .row_accessor()
            .prepare(&["id", "data"])
            .expect("prepare reader");
        let id_index = prepared.column_index("id").expect("id index");
        let data_index = prepared.column_index("data").expect("data index");

        prepared.load(0).expect("load first row");
        assert_eq!(
            prepared.scalar_at(id_index).expect("row 0 id"),
            &ScalarValue::Int32(10)
        );
        assert_eq!(
            prepared.array_at(data_index).expect("row 0 data"),
            &ArrayValue::from_i32_vec(vec![70, 90])
        );

        prepared.load(1).expect("load second row");
        assert_eq!(
            prepared.scalar_at(id_index).expect("row 1 id"),
            &ScalarValue::Int32(20)
        );
        assert_eq!(
            prepared.array_at(data_index).expect("row 1 data"),
            &ArrayValue::from_i32_vec(vec![110, 130])
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn prepared_row_cached_fast_path_rejects_invalid_slot_index() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push row");

        let root = unique_test_dir(&format!("prepared_row_invalid_slot_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        let mut prepared = reopened
            .row_accessor()
            .prepare(&["id", "data"])
            .expect("prepare reader");
        prepared.load(0).expect("load row");

        assert!(matches!(
            prepared.scalar_at(99),
            Err(TableError::SchemaColumnUnknown { column }) if column == "#99"
        ));
        assert!(matches!(
            prepared.array_at(99),
            Err(TableError::SchemaColumnUnknown { column }) if column == "#99"
        ));

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn prepared_row_access_rejects_record_columns() {
    let schema = TableSchema::new(vec![ColumnSchema::record("meta")]).expect("schema");
    let table = Table::with_schema(schema);

    let err = table
        .row_accessor()
        .prepare(&["meta"])
        .expect_err("record column should be rejected");

    assert!(matches!(
        err,
        TableError::PreparedRowRecordColumnUnsupported { column } if column == "meta"
    ));
}

#[test]
fn lazy_disk_open_reads_scalar_column_owned_without_materializing_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(10))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push row 0");
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(20))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![11, 13]))),
            ]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("lazy_scalar_column_owned_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());

        let values = table_scalar_cells_owned(&reopened, "scan").expect("read scalar column");
        assert_eq!(
            values,
            vec![Some(ScalarValue::Int32(10)), Some(ScalarValue::Int32(20))]
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "owned scalar column reads should not force row materialization for {dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn lazy_disk_open_reads_scalar_columns_owned_without_materializing_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
        ColumnSchema::scalar("time", PrimitiveType::Float64),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(10))),
                RecordField::new("time", Value::Scalar(ScalarValue::Float64(1.5))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push row 0");
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(20))),
                RecordField::new("time", Value::Scalar(ScalarValue::Float64(2.5))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![11, 13]))),
            ]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("lazy_scalar_columns_owned_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());

        let values =
            table_scalar_columns_owned(&reopened, &["scan", "time"]).expect("read scalar columns");
        assert_eq!(
            values.get("scan"),
            Some(&vec![
                Some(ScalarValue::Int32(10)),
                Some(ScalarValue::Int32(20))
            ])
        );
        assert_eq!(
            values.get("time"),
            Some(&vec![
                Some(ScalarValue::Float64(1.5)),
                Some(ScalarValue::Float64(2.5))
            ])
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "owned scalar column reads should not force row materialization for {dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn lazy_disk_open_reads_required_scalar_columns_owned_without_materializing_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
        ColumnSchema::scalar("time", PrimitiveType::Float64),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(10))),
                RecordField::new("time", Value::Scalar(ScalarValue::Float64(1.5))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push row 0");
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(20))),
                RecordField::new("time", Value::Scalar(ScalarValue::Float64(2.5))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![11, 13]))),
            ]))
            .expect("push row 1");

        let root = unique_test_dir(&format!("lazy_required_scalar_columns_owned_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());

        let values = table_required_scalar_columns_owned(&reopened, &["scan", "time"])
            .expect("read required scalar columns");
        assert_eq!(
            values.get("scan"),
            Some(&RequiredScalarColumnValues::Int32(vec![10, 20]))
        );
        assert_eq!(
            values.get("time"),
            Some(&RequiredScalarColumnValues::Float64(vec![1.5, 2.5]))
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "required scalar column reads should not force row materialization for {dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn lazy_disk_open_mutates_and_partially_saves_without_materializing_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(10))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push row");
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("scan", Value::Scalar(ScalarValue::Int32(20))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![11, 13]))),
            ]))
            .expect("push row");

        let root = unique_test_dir(&format!("lazy_partial_save_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());
        assert_eq!(
            table_scalar(&reopened, 0, "id").expect("prefetch id row 0"),
            &ScalarValue::Int32(1)
        );
        assert_eq!(
            table_scalar(&reopened, 1, "scan").expect("prefetch scan row 1"),
            &ScalarValue::Int32(20)
        );

        table_set_scalar_assuming_valid(&mut reopened, 1, "id", ScalarValue::Int32(22))
            .expect("set scalar cell lazily");
        table_set_array_assuming_valid(
            &mut reopened,
            0,
            "data",
            ArrayValue::from_i32_vec(vec![70, 90]),
        )
        .expect("set array cell lazily");
        assert!(
            !reopened.inner.has_loaded_rows(),
            "lazy mutation should not force row materialization for {dm:?}"
        );
        assert!(
            !reopened.inner.has_loaded_array_column("data"),
            "lazy array mutation should not force full array-column loads for {dm:?}"
        );
        assert!(
            reopened.inner.has_pending_array_cells("data"),
            "lazy array mutation should keep pending sparse cells for {dm:?}"
        );

        reopened
            .save_selected_columns_in_place_assuming_valid(&["id", "data"])
            .expect("partial save");
        assert!(
            !reopened.inner.has_loaded_rows(),
            "partial save should not force row materialization for {dm:?}"
        );

        let verify = Table::open(TableOptions::new(&root)).expect("reopen after partial save");
        assert!(!verify.inner.has_loaded_rows());
        assert_eq!(
            table_scalar(&verify, 0, "id").expect("id row 0"),
            &ScalarValue::Int32(1)
        );
        assert_eq!(
            table_scalar(&verify, 1, "id").expect("id row 1"),
            &ScalarValue::Int32(22)
        );
        assert_eq!(
            table_scalar(&verify, 0, "scan").expect("scan row 0"),
            &ScalarValue::Int32(10)
        );
        assert_eq!(
            table_scalar(&verify, 1, "scan").expect("scan row 1"),
            &ScalarValue::Int32(20)
        );
        assert_eq!(
            table_array(&verify, 0, "data").expect("data row 0"),
            &ArrayValue::from_i32_vec(vec![70, 90])
        );
        assert_eq!(
            table_array(&verify, 1, "data").expect("data row 1"),
            &ArrayValue::from_i32_vec(vec![11, 13])
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn lazy_disk_open_reads_selected_array_cells_without_loading_full_tiled_column() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 2],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..6 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("lazy_selected_array_rows_tiled_shape");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_loaded_array_column("data"));

    let selected =
        table_array_cells_owned(&reopened, "data", &[5, 2, 4]).expect("read selected array cells");
    assert_eq!(
        selected,
        vec![
            Some(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![5.0, 15.0, 25.0, 35.0]).unwrap()
            )),
            Some(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![2.0, 12.0, 22.0, 32.0]).unwrap()
            )),
            Some(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![4.0, 14.0, 24.0, 34.0]).unwrap()
            )),
        ]
    );
    assert!(
        !reopened.inner.has_loaded_rows(),
        "selected array reads should not force row materialization"
    );
    assert!(
        !reopened.inner.has_loaded_array_column("data"),
        "selected array reads should not populate the full array-column cache"
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn lazy_disk_open_reads_selected_tiled_array_channel_ranges_without_full_column() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 6],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..8 {
        let mut values = Vec::new();
        for channel in 0..6 {
            for corr in 0..2 {
                values.push(row_idx as f32 * 100.0 + channel as f32 * 10.0 + corr as f32);
            }
        }
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 6]).f(), values)
                        .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("lazy_selected_tiled_channel_ranges");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_loaded_array_column("data"));

    let typed = reopened
        .column_accessor("data")
        .expect("data accessor")
        .array_cells_2d_channel_range_typed_uncached(&[7, 2], 1, 3)
        .expect("typed selected channel ranges");
    let SelectedArray2DCells::Float32(typed) = typed else {
        panic!("expected Float32 typed selected cells");
    };
    assert_eq!(typed.row_count(), 2);
    assert_eq!(typed.axis0_count(), 2);
    assert_eq!(typed.channel_count(), 3);
    assert_eq!(
        typed.values(),
        &[
            710.0, 711.0, 210.0, 211.0, 720.0, 721.0, 220.0, 221.0, 730.0, 731.0, 230.0, 231.0,
        ]
    );
    assert!(
        !reopened.inner.has_loaded_rows(),
        "selected channel-range reads should not force row materialization"
    );
    assert!(
        !reopened.inner.has_loaded_array_column("data"),
        "selected channel-range reads should not populate the full array-column cache"
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn lazy_disk_open_reads_selected_fixed_array_cells_without_loading_full_column() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "uvw",
        PrimitiveType::Float64,
        vec![3],
    )])
    .expect("schema");

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let mut table = Table::with_schema(schema.clone());
        for row_idx in 0..6 {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "uvw",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![3],
                            vec![row_idx as f64, row_idx as f64 + 10.0, row_idx as f64 + 20.0],
                        )
                        .expect("shape uvw"),
                    )),
                )]))
                .expect("push row");
        }

        let root = unique_test_dir(&format!("lazy_selected_fixed_array_rows_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());
        assert!(!reopened.inner.has_loaded_array_column("uvw"));

        let selected =
            table_array_cells_owned(&reopened, "uvw", &[5, 2, 4]).expect("read selected uvw rows");
        assert_eq!(
            selected,
            vec![
                Some(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], vec![5.0, 15.0, 25.0]).unwrap()
                )),
                Some(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], vec![2.0, 12.0, 22.0]).unwrap()
                )),
                Some(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], vec![4.0, 14.0, 24.0]).unwrap()
                )),
            ],
            "dm={dm:?}"
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "selected fixed-array reads should not force row materialization for {dm:?}"
        );
        assert!(
            !reopened.inner.has_loaded_array_column("uvw"),
            "selected fixed-array reads should not populate the full array-column cache for {dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn lazy_disk_open_reads_array_cells_without_loading_full_tiled_column() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 2],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..6 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("lazy_array_cells_tiled_shape");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_loaded_array_column("data"));

    let row_4 = reopened.get_array_cell(4, "data").expect("row 4 array");
    assert_eq!(
        row_4,
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![4.0, 14.0, 24.0, 34.0]).unwrap()
        )
    );
    let row_1 = reopened.get_array_cell(1, "data").expect("row 1 array");
    assert_eq!(
        row_1,
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 11.0, 21.0, 31.0]).unwrap()
        )
    );
    assert!(
        !reopened.inner.has_loaded_rows(),
        "single array-cell reads should not force row materialization"
    );
    assert!(
        !reopened.inner.has_loaded_array_column("data"),
        "single array-cell reads should stay on the buffered tiled path"
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn opened_table_supports_multi_thread_shared_reads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Table>();

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 2],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..8 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("shared_multithread_reads_tiled_shape");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let shared = Arc::new(Table::open(TableOptions::new(&root)).expect("open lazy table"));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                shared
                    .get_array_cells_owned("data", &[7, 3, 5])
                    .expect("selected rows")
            })
        })
        .collect();

    for rows in handles
        .into_iter()
        .map(|handle| handle.join().expect("thread join"))
    {
        assert_eq!(
            rows,
            vec![
                Some(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 2], vec![7.0, 17.0, 27.0, 37.0]).unwrap()
                )),
                Some(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 2], vec![3.0, 13.0, 23.0, 33.0]).unwrap()
                )),
                Some(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 2], vec![5.0, 15.0, 25.0, 35.0]).unwrap()
                )),
            ]
        );
    }
    assert!(
        !shared.inner.has_loaded_rows(),
        "shared reads should stay on the lazy read path"
    );
    assert!(
        !shared.inner.has_loaded_array_column("data"),
        "shared selected-row reads should not populate the full array-column cache"
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn tiled_shared_read_cache_is_invalidated_after_in_place_save() {
    crate::storage::tiled_stman::reset_table_cache_budget_for_tests();

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 2],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("tiled_cache_invalidated_after_partial_save");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    reopened
        .get_array_cell(2, "data")
        .expect("prime shared cache");
    assert!(
        crate::storage::tiled_stman::shared_tile_cache_entry_count() > 0,
        "read should populate the shared tiled cache"
    );

    reopened
        .set_array_cell_assuming_valid(
            2,
            "data",
            ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![90.0, 91.0, 92.0, 93.0]).unwrap(),
            ),
        )
        .expect("mutate cached row");
    reopened
        .save_selected_rows_in_place_assuming_valid(&["data"], &[2])
        .expect("partial save");

    let verify = Table::open(TableOptions::new(&root)).expect("reopen after partial save");
    assert_eq!(
        verify.get_array_cell(2, "data").expect("updated row"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![90.0, 91.0, 92.0, 93.0]).unwrap()
        )
    );

    crate::storage::tiled_stman::reset_table_cache_budget_for_tests();
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn lazy_disk_open_reads_scalar_cells_with_full_scalar_column_cache() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
    ])
    .expect("schema");

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema.clone());
        for (id, scan) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                    RecordField::new("scan", Value::Scalar(ScalarValue::Int32(scan))),
                ]))
                .expect("push row");
        }

        let root = unique_test_dir(&format!("lazy_scalar_cell_buffered_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert!(!reopened.inner.has_loaded_rows());
        assert!(!reopened.inner.has_loaded_scalar_column("id"));
        assert!(!reopened.inner.has_loaded_scalar_column("scan"));

        assert_eq!(
            table_scalar(&reopened, 3, "id").expect("id row 3"),
            &ScalarValue::Int32(4)
        );
        assert_eq!(
            table_scalar(&reopened, 1, "scan").expect("scan row 1"),
            &ScalarValue::Int32(20)
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "scalar cell reads should not force row materialization for {dm:?}"
        );
        assert!(
            reopened.inner.has_loaded_scalar_column("id"),
            "scalar cell reads should populate the full scalar-column cache for {dm:?}"
        );
        assert!(
            reopened.inner.has_loaded_scalar_column("scan"),
            "scalar cell reads should populate the full scalar-column cache for {dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

#[test]
fn partial_save_with_changed_rows_patches_stman_aipsio_indirect_arrays() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("flag_row", PrimitiveType::Bool),
        ColumnSchema::array_variable("data", PrimitiveType::Float32, Some(2)),
    ])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("flag_row", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array2::from_shape_vec((2, 2), vec![1.0, 2.0, 3.0, 4.0])
                        .expect("row 0 shape")
                        .into_dyn(),
                )),
            ),
        ]))
        .expect("add row 0");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("flag_row", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array2::from_shape_vec((1, 3), vec![5.0, 6.0, 7.0])
                        .expect("row 1 shape")
                        .into_dyn(),
                )),
            ),
        ]))
        .expect("add row 1");

    let root = unique_test_dir("partial_save_changed_rows_stman_aipsio_indirect");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO))
        .expect("save disk-backed table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    table_set_scalar_assuming_valid(&mut reopened, 1, "flag_row", ScalarValue::Bool(true))
        .expect("set flag_row");
    table_set_array_assuming_valid(
        &mut reopened,
        1,
        "data",
        ArrayValue::Float32(
            ndarray::Array2::from_shape_vec((1, 3), vec![50.0, 60.0, 70.0])
                .expect("updated row 1 shape")
                .into_dyn(),
        ),
    )
    .expect("set indirect data");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(reopened.inner.has_pending_array_cells("data"));

    reopened
        .save_selected_rows_in_place_assuming_valid(&["flag_row", "data"], &[1])
        .expect("partial sparse save");

    let verify = Table::open(TableOptions::new(&root)).expect("reopen after partial save");
    assert_eq!(
        table_scalar(&verify, 0, "flag_row").expect("row 0 flag"),
        &ScalarValue::Bool(false)
    );
    assert_eq!(
        table_scalar(&verify, 1, "flag_row").expect("row 1 flag"),
        &ScalarValue::Bool(true)
    );
    assert_eq!(
        table_array(&verify, 0, "data").expect("row 0 data"),
        &ArrayValue::Float32(
            ndarray::Array2::from_shape_vec((2, 2), vec![1.0, 2.0, 3.0, 4.0])
                .expect("verify row 0 shape")
                .into_dyn(),
        )
    );
    assert_eq!(
        table_array(&verify, 1, "data").expect("row 1 data"),
        &ArrayValue::Float32(
            ndarray::Array2::from_shape_vec((1, 3), vec![50.0, 60.0, 70.0])
                .expect("verify row 1 shape")
                .into_dyn(),
        )
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn lazy_disk_open_reads_selected_indirect_array_cells_without_loading_full_stman_column() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row in [
        ndarray::Array2::from_shape_vec((2, 2), vec![1.0, 2.0, 3.0, 4.0]).expect("row 0 shape"),
        ndarray::Array2::from_shape_vec((1, 3), vec![5.0, 6.0, 7.0]).expect("row 1 shape"),
        ndarray::Array2::from_shape_vec((1, 2), vec![8.0, 9.0]).expect("row 2 shape"),
    ] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(row.into_dyn())),
            )]))
            .expect("add row");
    }

    let root = unique_test_dir("lazy_selected_rows_stman_indirect");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO))
        .expect("save disk-backed table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    let values =
        table_array_cells_owned(&reopened, "data", &[2, 1]).expect("selected indirect array rows");

    assert_eq!(
        values,
        vec![
            Some(ArrayValue::Float32(
                ndarray::Array2::from_shape_vec((1, 2), vec![8.0, 9.0])
                    .expect("verify row 2 shape")
                    .into_dyn(),
            )),
            Some(ArrayValue::Float32(
                ndarray::Array2::from_shape_vec((1, 3), vec![5.0, 6.0, 7.0])
                    .expect("verify row 1 shape")
                    .into_dyn(),
            )),
        ]
    );
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_loaded_array_column("data"));

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn partial_save_with_changed_rows_patches_only_touched_tiled_rows() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![4, 1],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![4, 1],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("partial_save_changed_rows_tiled_shape");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open tiled-shape table");
    table_set_array_assuming_valid(
        &mut reopened,
        2,
        "data",
        ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![4, 1], vec![200.0, 210.0, 220.0, 230.0])
                .expect("shape updated data"),
        ),
    )
    .expect("set array cell lazily");
    reopened
        .save_selected_rows_in_place_assuming_valid(&["data"], &[2])
        .expect("partial save with row hint");

    let verify = Table::open(TableOptions::new(&root)).expect("reopen after sparse partial save");
    assert_eq!(
        table_array(&verify, 0, "data").expect("data row 0"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![4, 1], vec![0.0, 10.0, 20.0, 30.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 1, "data").expect("data row 1"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![4, 1], vec![1.0, 11.0, 21.0, 31.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 2, "data").expect("data row 2"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![4, 1], vec![200.0, 210.0, 220.0, 230.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 3, "data").expect("data row 3"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![4, 1], vec![3.0, 13.0, 23.0, 33.0]).unwrap()
        )
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn partial_save_with_changed_rows_patches_only_touched_multi_column_tiled_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("DATA", PrimitiveType::Float32, vec![2, 2]),
        ColumnSchema::array_fixed("WEIGHT", PrimitiveType::Float32, vec![2, 2]),
    ])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..4 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                row_idx as f32,
                                row_idx as f32 + 10.0,
                                row_idx as f32 + 20.0,
                                row_idx as f32 + 30.0,
                            ],
                        )
                        .expect("shape DATA"),
                    )),
                ),
                RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                row_idx as f32 + 100.0,
                                row_idx as f32 + 110.0,
                                row_idx as f32 + 120.0,
                                row_idx as f32 + 130.0,
                            ],
                        )
                        .expect("shape WEIGHT"),
                    )),
                ),
            ]))
            .expect("push row");
    }

    let root = unique_test_dir("partial_save_changed_rows_tiled_shape_multi_column");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled-shape table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open tiled-shape table");
    let tiled_groups: Vec<_> = reopened
        .data_manager_info()
        .iter()
        .filter(|dm| dm.dm_type == "TiledShapeStMan")
        .collect();
    assert_eq!(tiled_groups.len(), 1);
    assert_eq!(tiled_groups[0].columns.len(), 2);
    assert!(
        tiled_groups[0]
            .columns
            .iter()
            .any(|column| column == "DATA")
    );
    assert!(
        tiled_groups[0]
            .columns
            .iter()
            .any(|column| column == "WEIGHT")
    );

    table_set_array_assuming_valid(
        &mut reopened,
        1,
        "DATA",
        ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![201.0, 211.0, 221.0, 231.0])
                .expect("shape updated DATA"),
        ),
    )
    .expect("set DATA lazily");
    table_set_array_assuming_valid(
        &mut reopened,
        2,
        "WEIGHT",
        ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![302.0, 312.0, 322.0, 332.0])
                .expect("shape updated WEIGHT"),
        ),
    )
    .expect("set WEIGHT lazily");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(reopened.inner.has_pending_array_cells("DATA"));
    assert!(reopened.inner.has_pending_array_cells("WEIGHT"));
    assert!(!reopened.inner.has_loaded_array_column("DATA"));
    assert!(!reopened.inner.has_loaded_array_column("WEIGHT"));

    reopened
        .save_selected_rows_in_place_assuming_valid(&["DATA", "WEIGHT"], &[1, 2])
        .expect("partial save with tiled group row hints");
    assert!(
        !reopened.inner.has_loaded_rows(),
        "multi-column tiled sparse save should not force row materialization"
    );
    assert!(
        !reopened.inner.has_loaded_array_column("DATA"),
        "multi-column tiled sparse save should not materialize the DATA column"
    );
    assert!(
        !reopened.inner.has_loaded_array_column("WEIGHT"),
        "multi-column tiled sparse save should not materialize the WEIGHT column"
    );

    let verify = Table::open(TableOptions::new(&root)).expect("reopen after sparse partial save");
    assert_eq!(
        table_array(&verify, 0, "DATA").expect("DATA row 0"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![0.0, 10.0, 20.0, 30.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 1, "DATA").expect("DATA row 1"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![201.0, 211.0, 221.0, 231.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 2, "DATA").expect("DATA row 2"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![2.0, 12.0, 22.0, 32.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 1, "WEIGHT").expect("WEIGHT row 1"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![101.0, 111.0, 121.0, 131.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 2, "WEIGHT").expect("WEIGHT row 2"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![302.0, 312.0, 322.0, 332.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 3, "WEIGHT").expect("WEIGHT row 3"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![103.0, 113.0, 123.0, 133.0]).unwrap()
        )
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn partial_save_with_changed_rows_patches_only_touched_tiled_cell_rows() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 2],
    )])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let root = unique_test_dir("partial_save_changed_rows_tiled_cell");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledCellStMan))
        .expect("save tiled-cell table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open tiled-cell table");
    table_set_array_assuming_valid(
        &mut reopened,
        2,
        "data",
        ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![202.0, 212.0, 222.0, 232.0])
                .expect("shape updated data"),
        ),
    )
    .expect("set array cell lazily");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(reopened.inner.has_pending_array_cells("data"));
    assert!(!reopened.inner.has_loaded_array_column("data"));

    reopened
        .save_selected_rows_in_place_assuming_valid(&["data"], &[2])
        .expect("partial save with tiled-cell row hint");
    assert!(
        !reopened.inner.has_loaded_rows(),
        "tiled-cell sparse save should not force row materialization"
    );
    assert!(
        !reopened.inner.has_loaded_array_column("data"),
        "tiled-cell sparse save should not materialize the array column"
    );

    let verify = Table::open(TableOptions::new(&root)).expect("reopen after sparse partial save");
    assert_eq!(
        table_array(&verify, 0, "data").expect("data row 0"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![0.0, 10.0, 20.0, 30.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 2, "data").expect("data row 2"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![202.0, 212.0, 222.0, 232.0]).unwrap()
        )
    );
    assert_eq!(
        table_array(&verify, 3, "data").expect("data row 3"),
        &ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![3.0, 13.0, 23.0, 33.0]).unwrap()
        )
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn partial_save_with_changed_rows_patches_only_touched_incremental_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
        ColumnSchema::scalar("state", PrimitiveType::Int32),
    ])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..80 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(row_idx))),
                RecordField::new(
                    "scan",
                    Value::Scalar(ScalarValue::Int32((row_idx / 8) * 10)),
                ),
                RecordField::new("state", Value::Scalar(ScalarValue::Int32(row_idx % 3))),
            ]))
            .expect("push row");
    }

    let root = unique_test_dir("partial_save_changed_rows_incremental");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::IncrementalStMan))
        .expect("save incremental table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open incremental table");
    assert!(!reopened.inner.has_loaded_rows());
    table_set_scalar_assuming_valid(&mut reopened, 2, "id", ScalarValue::Int32(2002))
        .expect("set row 2 id");
    table_set_scalar_assuming_valid(&mut reopened, 41, "scan", ScalarValue::Int32(9041))
        .expect("set row 41 scan");
    reopened
        .save_selected_rows_in_place_assuming_valid(&["id", "scan"], &[2, 41])
        .expect("sparse incremental partial save");
    assert!(
        !reopened.inner.has_loaded_rows(),
        "sparse incremental save should not force row materialization"
    );
    assert!(
        !reopened.inner.has_loaded_scalar_column("id"),
        "sparse incremental save should not materialize the id column"
    );
    assert!(
        !reopened.inner.has_loaded_scalar_column("scan"),
        "sparse incremental save should not materialize the scan column"
    );

    let verify =
        Table::open(TableOptions::new(&root)).expect("reopen after sparse incremental save");
    assert_eq!(
        table_scalar(&verify, 1, "id").expect("id row 1"),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        table_scalar(&verify, 2, "id").expect("id row 2"),
        &ScalarValue::Int32(2002)
    );
    assert_eq!(
        table_scalar(&verify, 40, "scan").expect("scan row 40"),
        &ScalarValue::Int32(50)
    );
    assert_eq!(
        table_scalar(&verify, 41, "scan").expect("scan row 41"),
        &ScalarValue::Int32(9041)
    );
    assert_eq!(
        table_scalar(&verify, 42, "scan").expect("scan row 42"),
        &ScalarValue::Int32(50)
    );
    assert_eq!(
        table_scalar(&verify, 41, "state").expect("state row 41"),
        &ScalarValue::Int32(2)
    );

    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn get_scalar_cell_rejects_non_scalar() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "data",
        Value::Array(ArrayValue::from_i32_vec(vec![1, 2])),
    )])]);

    assert!(matches!(
        table_scalar(&table, 0, "data"),
        Err(TableError::ColumnTypeMismatch { .. })
    ));
}

fn unique_test_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("casa_tables_{prefix}_{nanos}"))
}

/// Build a small multi-type table for endian round-trip tests.
fn build_endian_test_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("i32_col", PrimitiveType::Int32),
        ColumnSchema::scalar("f64_col", PrimitiveType::Float64),
        ColumnSchema::scalar("str_col", PrimitiveType::String),
        ColumnSchema::array_fixed("arr_col", PrimitiveType::Float32, vec![3]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("i32_col", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new("f64_col", Value::Scalar(ScalarValue::Float64(2.78))),
            RecordField::new(
                "str_col",
                Value::Scalar(ScalarValue::String("hello".into())),
            ),
            RecordField::new(
                "arr_col",
                Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0, 3.0])),
            ),
        ]))
        .expect("row 0");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("i32_col", Value::Scalar(ScalarValue::Int32(-7))),
            RecordField::new("f64_col", Value::Scalar(ScalarValue::Float64(-0.5))),
            RecordField::new(
                "str_col",
                Value::Scalar(ScalarValue::String("world".into())),
            ),
            RecordField::new(
                "arr_col",
                Value::Array(ArrayValue::from_f32_vec(vec![4.0, 5.0, 6.0])),
            ),
        ]))
        .expect("row 1");
    table
}

/// Verify a reopened table matches the endian test fixture.
fn verify_endian_test_table(t: &Table) {
    assert_eq!(t.row_count(), 2);
    assert_eq!(
        table_cell(t, 0, "i32_col"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(42))))
    );
    assert_eq!(
        table_cell(t, 0, "f64_col"),
        Ok(Some(&Value::Scalar(ScalarValue::Float64(2.78))))
    );
    assert_eq!(
        table_cell(t, 0, "str_col"),
        Ok(Some(&Value::Scalar(ScalarValue::String("hello".into()))))
    );
    assert_eq!(
        table_cell(t, 1, "i32_col"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(-7))))
    );
}

#[test]
fn stmanaipsio_le_round_trip() {
    let table = build_endian_test_table();
    let root = unique_test_dir("aipsio_le_rt");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::StManAipsIO)
                .with_endian_format(EndianFormat::LittleEndian),
        )
        .expect("save LE");
    let reopened = Table::open(TableOptions::new(&root)).expect("open LE");
    verify_endian_test_table(&reopened);
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn stmanaipsio_be_round_trip() {
    let table = build_endian_test_table();
    let root = unique_test_dir("aipsio_be_rt");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::StManAipsIO)
                .with_endian_format(EndianFormat::BigEndian),
        )
        .expect("save BE");
    let reopened = Table::open(TableOptions::new(&root)).expect("open BE");
    verify_endian_test_table(&reopened);
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn ssm_le_round_trip() {
    let table = build_endian_test_table();
    let root = unique_test_dir("ssm_le_rt");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::StandardStMan)
                .with_endian_format(EndianFormat::LittleEndian),
        )
        .expect("save LE");
    let reopened = Table::open(TableOptions::new(&root)).expect("open LE");
    verify_endian_test_table(&reopened);
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn ssm_be_round_trip() {
    let table = build_endian_test_table();
    let root = unique_test_dir("ssm_be_rt");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::StandardStMan)
                .with_endian_format(EndianFormat::BigEndian),
        )
        .expect("save BE");
    let reopened = Table::open(TableOptions::new(&root)).expect("open BE");
    verify_endian_test_table(&reopened);
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn ssm_fixed_length_string_round_trip() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("priority", PrimitiveType::String).with_max_length(9),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "priority",
            Value::Scalar(ScalarValue::String("INFO".into())),
        )]))
        .expect("row 0");
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "priority",
            Value::Scalar(ScalarValue::String("DEBUGGING".into())),
        )]))
        .expect("row 1");

    let root = unique_test_dir("ssm_fixed_string");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save");
    let reopened = Table::open(TableOptions::new(&root)).expect("open");
    assert_eq!(
        table_scalar(&reopened, 0, "priority"),
        Ok(&ScalarValue::String("INFO".into()))
    );
    assert_eq!(
        table_scalar(&reopened, 1, "priority"),
        Ok(&ScalarValue::String("DEBUGGING".into()))
    );
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn save_with_bindings_keeps_different_tiled_array_dims_in_separate_groups() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_variable("FLAG", PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable("FLAG_CATEGORY", PrimitiveType::Bool, Some(3)),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "FLAG",
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(vec![4, 1], vec![false; 4]).expect("shape FLAG"),
                )),
            ),
            RecordField::new(
                "FLAG_CATEGORY",
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(vec![4, 1, 6], vec![false; 24])
                        .expect("shape FLAG_CATEGORY"),
                )),
            ),
        ]))
        .expect("add row");

    let root = unique_test_dir("save_with_bindings_tiled_dims");
    std::fs::create_dir_all(&root).expect("mkdir");

    let mut bindings = HashMap::new();
    for column in ["FLAG", "FLAG_CATEGORY"] {
        bindings.insert(
            column.to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::TiledShapeStMan,
                tile_shape: None,
            },
        );
    }

    table
        .save_with_bindings(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("save with tiled bindings");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    let tiled_groups: Vec<_> = reopened
        .data_manager_info()
        .iter()
        .filter(|dm| dm.dm_type == "TiledShapeStMan")
        .collect();
    assert_eq!(tiled_groups.len(), 2);
    assert!(tiled_groups.iter().all(|dm| dm.columns.len() == 1));
    assert!(
        tiled_groups
            .iter()
            .any(|dm| dm.columns.iter().any(|column| column == "FLAG"))
    );
    assert!(
        tiled_groups
            .iter()
            .any(|dm| dm.columns.iter().any(|column| column == "FLAG_CATEGORY"))
    );

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn save_with_bindings_column_overrides_write_single_tiled_array_column() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ROW_ID", PrimitiveType::Int32),
        ColumnSchema::array_variable("DATA", PrimitiveType::Int32, Some(2)),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row_id in 0..3 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "ROW_ID",
                Value::Scalar(ScalarValue::Int32(row_id)),
            )]))
            .expect("add scalar-only row");
    }

    let root = unique_test_dir("save_with_bindings_column_overrides");
    std::fs::create_dir_all(&root).expect("mkdir");

    let bindings = HashMap::from([(
        "DATA".to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::TiledShapeStMan,
            tile_shape: None,
        },
    )]);
    let mut overrides = ColumnOverrides::for_row_count(3);
    overrides.insert_values(
        "DATA",
        vec![
            Some(Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).expect("row 0 shape"),
            ))),
            Some(Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![2, 2], vec![5, 6, 7, 8]).expect("row 1 shape"),
            ))),
            Some(Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![2, 2], vec![9, 10, 11, 12]).expect("row 2 shape"),
            ))),
        ],
    );

    table
        .save_with_bindings_and_column_overrides_assuming_valid(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
            &overrides,
        )
        .expect("save with column overrides");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    assert_eq!(
        table_scalar(&reopened, 1, "ROW_ID").expect("row id"),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        table_array(&reopened, 2, "DATA").expect("data row 2"),
        &ArrayValue::Int32(
            ArrayD::from_shape_vec(vec![2, 2], vec![9, 10, 11, 12]).expect("expected shape")
        )
    );

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn save_with_bindings_generated_scalar_overrides_define_row_count() {
    let row_count = 4096usize;
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ROW_ID", PrimitiveType::Int32),
        ColumnSchema::scalar("SCAN", PrimitiveType::Int32),
        ColumnSchema::scalar("FLAG_ROW", PrimitiveType::Bool),
    ])
    .expect("schema");
    let table = Table::with_schema(schema);

    let root = unique_test_dir("save_with_bindings_generated_scalar_overrides");
    std::fs::create_dir_all(&root).expect("mkdir");

    let bindings = HashMap::from([
        (
            "SCAN".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::IncrementalStMan,
                tile_shape: None,
            },
        ),
        (
            "FLAG_ROW".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::StandardStMan,
                tile_shape: None,
            },
        ),
    ]);
    let mut overrides = ColumnOverrides::for_row_count(row_count);
    overrides.insert_generated_scalar(
        "ROW_ID",
        GeneratedScalarColumn::new(row_count, |row| Some(ScalarValue::Int32(row as i32))),
    );
    overrides.insert_generated_scalar(
        "SCAN",
        GeneratedScalarColumn::new(row_count, |row| {
            Some(ScalarValue::Int32((row / 128) as i32))
        }),
    );
    overrides.insert_generated_scalar(
        "FLAG_ROW",
        GeneratedScalarColumn::new(row_count, |row| Some(ScalarValue::Bool(row % 17 == 0))),
    );

    table
        .save_with_bindings_and_column_overrides_assuming_valid(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
            &overrides,
        )
        .expect("save generated overrides");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    assert_eq!(reopened.row_count(), row_count);
    assert_eq!(
        table_scalar(&reopened, 0, "ROW_ID").expect("row 0 id"),
        &ScalarValue::Int32(0)
    );
    assert_eq!(
        table_scalar(&reopened, 1025, "ROW_ID").expect("row 1025 id"),
        &ScalarValue::Int32(1025)
    );
    assert_eq!(
        table_scalar(&reopened, 4095, "SCAN").expect("last scan"),
        &ScalarValue::Int32(31)
    );
    assert_eq!(
        table_scalar(&reopened, 34, "FLAG_ROW").expect("flag row"),
        &ScalarValue::Bool(true)
    );
    assert_eq!(
        table_scalar(&reopened, 35, "FLAG_ROW").expect("flag row"),
        &ScalarValue::Bool(false)
    );

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn generated_scalar_column_runs_validate_and_resolve_values() {
    let column = GeneratedScalarColumn::from_scalar_runs(
        10,
        vec![
            GeneratedScalarValueRun::new(0, Some(ScalarValue::Int32(1))),
            GeneratedScalarValueRun::new(4, Some(ScalarValue::Int32(2))),
            GeneratedScalarValueRun::new(8, None),
        ],
    )
    .expect("valid runs");

    assert_eq!(column.value(0), Some(ScalarValue::Int32(1)));
    assert_eq!(column.value(3), Some(ScalarValue::Int32(1)));
    assert_eq!(column.value(4), Some(ScalarValue::Int32(2)));
    assert_eq!(column.value(7), Some(ScalarValue::Int32(2)));
    assert_eq!(column.value(8), None);
    assert_eq!(column.value(10), None);

    assert!(matches!(
        GeneratedScalarColumn::from_scalar_runs(
            2,
            vec![GeneratedScalarValueRun::new(1, Some(ScalarValue::Int32(1)))],
        ),
        Err(TableError::InvalidGeneratedScalarRuns { .. })
    ));
    assert!(matches!(
        GeneratedScalarColumn::from_scalar_runs(
            2,
            vec![
                GeneratedScalarValueRun::new(0, Some(ScalarValue::Int32(1))),
                GeneratedScalarValueRun::new(0, Some(ScalarValue::Int32(2))),
            ],
        ),
        Err(TableError::InvalidGeneratedScalarRuns { .. })
    ));
    assert!(matches!(
        GeneratedScalarColumn::from_scalar_runs(
            2,
            vec![
                GeneratedScalarValueRun::new(0, Some(ScalarValue::Int32(1))),
                GeneratedScalarValueRun::new(2, Some(ScalarValue::Int32(2))),
            ],
        ),
        Err(TableError::InvalidGeneratedScalarRuns { .. })
    ));
}

#[test]
fn save_with_bindings_generated_scalar_run_overrides_round_trip_ism() {
    let row_count = 10_000usize;
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("CONST_ID", PrimitiveType::Int32),
        ColumnSchema::scalar("SCAN", PrimitiveType::Int32),
        ColumnSchema::scalar("TIME", PrimitiveType::Float64),
    ])
    .expect("schema");
    let table = Table::with_schema(schema);

    let root = unique_test_dir("save_with_bindings_generated_scalar_run_overrides");
    std::fs::create_dir_all(&root).expect("mkdir");

    let bindings = HashMap::from([
        (
            "CONST_ID".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::IncrementalStMan,
                tile_shape: None,
            },
        ),
        (
            "SCAN".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::IncrementalStMan,
                tile_shape: None,
            },
        ),
        (
            "TIME".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::IncrementalStMan,
                tile_shape: None,
            },
        ),
    ]);
    let mut overrides = ColumnOverrides::for_row_count(row_count);
    overrides.insert_generated_scalar(
        "CONST_ID",
        GeneratedScalarColumn::constant(row_count, Some(ScalarValue::Int32(7))),
    );
    overrides.insert_generated_scalar(
        "SCAN",
        GeneratedScalarColumn::from_scalar_runs(
            row_count,
            vec![
                GeneratedScalarValueRun::new(0, Some(ScalarValue::Int32(1))),
                GeneratedScalarValueRun::new(128, Some(ScalarValue::Int32(2))),
                GeneratedScalarValueRun::new(8192, Some(ScalarValue::Int32(5))),
            ],
        )
        .expect("scan runs"),
    );
    overrides.insert_generated_scalar(
        "TIME",
        GeneratedScalarColumn::from_scalar_runs(
            row_count,
            vec![
                GeneratedScalarValueRun::new(0, Some(ScalarValue::Float64(10.0))),
                GeneratedScalarValueRun::new(351, Some(ScalarValue::Float64(12.0))),
                GeneratedScalarValueRun::new(702, Some(ScalarValue::Float64(14.0))),
                GeneratedScalarValueRun::new(9000, Some(ScalarValue::Float64(16.0))),
            ],
        )
        .expect("time runs"),
    );

    table
        .save_with_bindings_and_column_overrides_assuming_valid(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
            &overrides,
        )
        .expect("save run overrides");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    assert_eq!(reopened.row_count(), row_count);
    for row in [0, 127, 128, 8191, 8192, 9999] {
        assert_eq!(
            table_scalar(&reopened, row, "CONST_ID").expect("const id"),
            &ScalarValue::Int32(7)
        );
    }
    assert_eq!(
        table_scalar(&reopened, 127, "SCAN").expect("scan before change"),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        table_scalar(&reopened, 128, "SCAN").expect("scan at change"),
        &ScalarValue::Int32(2)
    );
    assert_eq!(
        table_scalar(&reopened, 8192, "SCAN").expect("scan later change"),
        &ScalarValue::Int32(5)
    );
    assert_eq!(
        table_scalar(&reopened, 350, "TIME").expect("time before change"),
        &ScalarValue::Float64(10.0)
    );
    assert_eq!(
        table_scalar(&reopened, 351, "TIME").expect("time at change"),
        &ScalarValue::Float64(12.0)
    );
    assert_eq!(
        table_scalar(&reopened, 8999, "TIME").expect("time before final change"),
        &ScalarValue::Float64(14.0)
    );
    assert_eq!(
        table_scalar(&reopened, 9000, "TIME").expect("time final change"),
        &ScalarValue::Float64(16.0)
    );

    std::fs::remove_dir_all(&root).expect("cleanup");
}

fn assert_save_with_bindings_preserves_scalar_values_when_row_field_order_varies(
    dm_kind: DataManagerKind,
    label: &str,
) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("A", PrimitiveType::Int32),
        ColumnSchema::scalar("B", PrimitiveType::Int32),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("A", Value::Scalar(ScalarValue::Int32(10))),
            RecordField::new("B", Value::Scalar(ScalarValue::Int32(20))),
        ]))
        .expect("add row 0");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("B", Value::Scalar(ScalarValue::Int32(40))),
            RecordField::new("A", Value::Scalar(ScalarValue::Int32(30))),
        ]))
        .expect("add row 1");

    let root = unique_test_dir(label);
    std::fs::create_dir_all(&root).expect("mkdir");

    let bindings = HashMap::from([
        (
            "A".to_string(),
            ColumnBinding {
                data_manager: dm_kind,
                tile_shape: None,
            },
        ),
        (
            "B".to_string(),
            ColumnBinding {
                data_manager: dm_kind,
                tile_shape: None,
            },
        ),
    ]);

    table
        .save_with_bindings(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("save with bindings");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    let row0 = table_row(&reopened, 0).expect("row 0");
    let row1 = table_row(&reopened, 1).expect("row 1");
    assert_eq!(row0.get("A"), Some(&Value::Scalar(ScalarValue::Int32(10))));
    assert_eq!(row0.get("B"), Some(&Value::Scalar(ScalarValue::Int32(20))));
    assert_eq!(row1.get("A"), Some(&Value::Scalar(ScalarValue::Int32(30))));
    assert_eq!(row1.get("B"), Some(&Value::Scalar(ScalarValue::Int32(40))));

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn save_with_bindings_ssm_preserves_scalar_values_when_row_field_order_varies() {
    assert_save_with_bindings_preserves_scalar_values_when_row_field_order_varies(
        DataManagerKind::StandardStMan,
        "save_with_bindings_reordered_rows_ssm",
    );
}

#[test]
fn save_with_bindings_ism_preserves_scalar_values_when_row_field_order_varies() {
    assert_save_with_bindings_preserves_scalar_values_when_row_field_order_varies(
        DataManagerKind::IncrementalStMan,
        "save_with_bindings_reordered_rows_ism",
    );
}

#[test]
fn fixed_shape_indirect_array_round_trips_through_disk() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![3],
    )])
    .expect("schema");
    let rows = vec![
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0])),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f64_vec(vec![4.0, 5.0, 6.0])),
        )]),
    ];
    let table = Table::from_rows_with_schema(rows, schema).expect("table");

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("fixed_indirect_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        match table_cell(&reopened, 0, "data")
            .expect("row0 data")
            .expect("row0 data defined")
        {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a.as_slice().unwrap(), &[1.0, 2.0, 3.0])
            }
            other => panic!("unexpected row0 value: {other:?}"),
        }
        match table_cell(&reopened, 1, "data")
            .expect("row1 data")
            .expect("row1 data defined")
        {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a.as_slice().unwrap(), &[4.0, 5.0, 6.0])
            }
            other => panic!("unexpected row1 value: {other:?}"),
        }
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn ism_le_round_trip() {
    let table = build_endian_test_table();
    let root = unique_test_dir("ism_le_rt");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::IncrementalStMan)
                .with_endian_format(EndianFormat::LittleEndian),
        )
        .expect("save LE");
    let reopened = Table::open(TableOptions::new(&root)).expect("open LE");
    verify_endian_test_table(&reopened);
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn ism_be_round_trip() {
    let table = build_endian_test_table();
    let root = unique_test_dir("ism_be_rt");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::IncrementalStMan)
                .with_endian_format(EndianFormat::BigEndian),
        )
        .expect("save BE");
    let reopened = Table::open(TableOptions::new(&root)).expect("open BE");
    verify_endian_test_table(&reopened);
    std::fs::remove_dir_all(&root).expect("cleanup");
}

/// Test ISM delta compression: values that repeat across consecutive rows.
#[test]
fn ism_slowly_changing() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("SCAN_NUMBER", PrimitiveType::Int32),
        ColumnSchema::scalar("FLAG", PrimitiveType::Bool),
    ])
    .unwrap();

    let scans = [0, 0, 0, 1, 1, 1, 1, 2, 2, 2];
    let flags = [
        true, true, true, true, true, false, false, false, true, true,
    ];

    let rows: Vec<RecordValue> = scans
        .iter()
        .zip(flags.iter())
        .map(|(&s, &f)| {
            RecordValue::new(vec![
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(s))),
                RecordField::new("FLAG", Value::Scalar(ScalarValue::Bool(f))),
            ])
        })
        .collect();

    let table = Table::from_rows_with_schema(rows, schema).unwrap();
    let root = unique_test_dir("ism_slowly_changing");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::IncrementalStMan))
        .expect("save ISM");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen");
    assert_eq!(reopened.row_count(), 10);
    for (i, (&expected_scan, &expected_flag)) in scans.iter().zip(flags.iter()).enumerate() {
        let scan = table_scalar(&reopened, i, "SCAN_NUMBER").unwrap();
        assert_eq!(
            *scan,
            ScalarValue::Int32(expected_scan),
            "row {i} SCAN_NUMBER"
        );
        let flag = table_scalar(&reopened, i, "FLAG").unwrap();
        assert_eq!(*flag, ScalarValue::Bool(expected_flag), "row {i} FLAG");
    }
    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn default_endian_matches_host() {
    let table = build_endian_test_table();
    let root = unique_test_dir("default_endian");
    std::fs::create_dir_all(&root).expect("mkdir");
    table.save(TableOptions::new(&root)).expect("save default");

    // Read table.dat and check the endian marker
    let dat_path = root.join("table.dat");
    let reopened = Table::open(TableOptions::new(&root)).expect("open");
    verify_endian_test_table(&reopened);

    // Verify the table.dat file exists and the table round-trips
    assert!(dat_path.exists());
    std::fs::remove_dir_all(&root).expect("cleanup");
}

// ---- Wave 2: Schema mutation & row operations tests ----

/// Build a 3-row table with an "id" (Int32) and "name" (String) column.
fn build_mutation_test_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for i in 0..3 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("row{i}"))),
                ),
            ]))
            .expect("add row");
    }
    table
}

#[test]
fn add_column_populates_existing_rows() {
    let mut table = build_mutation_test_table();
    table
        .add_column(
            ColumnSchema::scalar("score", PrimitiveType::Float64),
            Some(Value::Scalar(ScalarValue::Float64(0.0))),
        )
        .expect("add column");

    assert_eq!(table.schema().unwrap().columns().len(), 3);
    for i in 0..3 {
        assert_eq!(
            table_cell(&table, i, "score"),
            Ok(Some(&Value::Scalar(ScalarValue::Float64(0.0))))
        );
    }
}

#[test]
fn add_column_round_trips_through_disk() {
    let mut table = build_mutation_test_table();
    table
        .add_column(
            ColumnSchema::scalar("score", PrimitiveType::Float64),
            Some(Value::Scalar(ScalarValue::Float64(99.5))),
        )
        .expect("add column");

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("add_col_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        assert_eq!(reopened.schema().unwrap().columns().len(), 3);
        for i in 0..3 {
            assert_eq!(
                table_cell(&reopened, i, "score"),
                Ok(Some(&Value::Scalar(ScalarValue::Float64(99.5))))
            );
        }
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn add_column_none_default_with_undefined() {
    use crate::schema::ColumnOptions;

    let mut table = build_mutation_test_table();
    table
        .add_column(
            ColumnSchema::scalar("opt", PrimitiveType::Int32)
                .with_options(ColumnOptions {
                    direct: false,
                    undefined: true,
                })
                .expect("options"),
            None,
        )
        .expect("add column with None default");

    assert_eq!(table.schema().unwrap().columns().len(), 3);
    // Rows should not have the new field.
    for i in 0..3 {
        assert_eq!(table_cell(&table, i, "opt"), Ok(None));
    }
}

#[test]
fn add_variable_shape_tiled_column_in_place_persists_defined_rows_only() {
    let root = unique_test_dir("add_sparse_tiled_shape_col");
    std::fs::create_dir_all(&root).expect("mkdir");
    build_mutation_test_table()
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save base table");

    let mut table = Table::open(TableOptions::new(&root)).expect("open base table");
    let column = ColumnSchema::array_variable("vis", PrimitiveType::Float32, Some(2));
    table.add_column(column, None).expect("add column");
    table_set_cell(
        &mut table,
        1,
        "vis",
        Value::Array(ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        )),
    )
    .expect("set row 1");
    table_set_cell(
        &mut table,
        2,
        "vis",
        Value::Array(ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![1, 3], vec![5.0, 6.0, 7.0]).unwrap(),
        )),
    )
    .expect("set row 2");
    table
        .save_added_tiled_shape_column_in_place_assuming_valid("vis", &[1, 2], Some(&[2, 2, 8]))
        .expect("save added tiled column");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    match table_cell(&reopened, 0, "vis") {
        Ok(None) => {}
        Ok(Some(Value::Array(ArrayValue::Float32(array)))) => {
            assert_eq!(array.shape(), &[0, 0]);
        }
        other => panic!("unexpected row 0 value: {other:?}"),
    }
    assert_eq!(
        table_array(&reopened, 1, "vis"),
        Ok(&ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).unwrap()
        ))
    );
    assert_eq!(
        table_array(&reopened, 2, "vis"),
        Ok(&ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![1, 3], vec![5.0, 6.0, 7.0]).unwrap()
        ))
    );
    assert!(
        reopened
            .data_manager_info()
            .iter()
            .any(|dm| dm.dm_type == "TiledShapeStMan" && dm.columns == ["vis"])
    );

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn clone_tiled_array_column_in_place_preserves_source_values_and_allows_sparse_patch() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DATA",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row_idx in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            casa_types::Complex32::new(row_idx as f32, 0.0),
                            casa_types::Complex32::new(row_idx as f32, 1.0),
                            casa_types::Complex32::new(row_idx as f32, 2.0),
                            casa_types::Complex32::new(row_idx as f32, 3.0),
                        ],
                    )
                    .expect("shape DATA"),
                )),
            )]))
            .expect("add row");
    }

    let root = unique_test_dir("clone_tiled_array_column");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save source table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open source table");
    reopened
        .add_column(
            ColumnSchema::array_variable("CORRECTED_DATA", PrimitiveType::Complex32, Some(2)),
            None,
        )
        .expect("add corrected column");
    reopened
        .save_added_tiled_column_clone_in_place_assuming_valid(
            "DATA",
            "CORRECTED_DATA",
            "TiledCorrected",
        )
        .expect("clone DATA to CORRECTED_DATA");
    table_set_cell(
        &mut reopened,
        2,
        "CORRECTED_DATA",
        Value::Array(ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2, 2],
                vec![
                    casa_types::Complex32::new(20.0, 0.0),
                    casa_types::Complex32::new(20.0, 1.0),
                    casa_types::Complex32::new(20.0, 2.0),
                    casa_types::Complex32::new(20.0, 3.0),
                ],
            )
            .expect("shape corrected"),
        )),
    )
    .expect("patch row");
    reopened
        .save_selected_rows_in_place_assuming_valid(&["CORRECTED_DATA"], &[2])
        .expect("sparse patch corrected");

    let patched = Table::open(TableOptions::new(&root)).expect("reopen patched table");
    assert_eq!(
        table_array(&patched, 1, "CORRECTED_DATA"),
        table_array(&patched, 1, "DATA")
    );
    assert_ne!(
        table_array(&patched, 2, "CORRECTED_DATA"),
        table_array(&patched, 2, "DATA")
    );
    assert_eq!(
        table_array(&patched, 2, "CORRECTED_DATA"),
        Ok(&ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2, 2],
                vec![
                    casa_types::Complex32::new(20.0, 0.0),
                    casa_types::Complex32::new(20.0, 1.0),
                    casa_types::Complex32::new(20.0, 2.0),
                    casa_types::Complex32::new(20.0, 3.0),
                ],
            )
            .unwrap()
        ))
    );
    assert!(
        patched
            .data_manager_info()
            .iter()
            .any(|dm| dm.dm_type == "TiledShapeStMan" && dm.columns == ["CORRECTED_DATA"])
    );

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn from_rows_with_schema_persists_missing_undefined_scalar_cells() {
    use crate::schema::ColumnOptions;

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("opt", PrimitiveType::Float64)
            .with_options(ColumnOptions {
                direct: false,
                undefined: true,
            })
            .expect("undefined scalar"),
    ])
    .expect("schema");
    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("opt", Value::Scalar(ScalarValue::Float64(2.5))),
        ]),
        RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(2)),
        )]),
    ];

    let table = Table::from_rows_with_schema(rows, schema).expect("table");
    assert_eq!(table_cell(&table, 1, "opt"), Ok(None));
    assert!(
        table.undefined_cells().unwrap()[1].contains("opt"),
        "missing scalar field should be tracked as undefined"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("undefined_scalar_rows.tbl");
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StManAipsIO))
        .expect("save table");

    let reopened = Table::open(TableOptions::new(&path)).expect("reopen table");
    assert_eq!(
        table_cell(&reopened, 0, "opt"),
        Ok(Some(&Value::Scalar(ScalarValue::Float64(2.5))))
    );
    assert_eq!(table_cell(&reopened, 1, "opt"), Ok(None));
    assert!(reopened.undefined_cells().unwrap()[1].contains("opt"));
}

#[test]
fn add_column_none_default_without_undefined_errors() {
    let mut table = build_mutation_test_table();
    let result = table.add_column(
        ColumnSchema::scalar("required_col", PrimitiveType::Int32),
        None,
    );
    assert!(
        result.is_err(),
        "should error when no default and column requires values"
    );
}

#[test]
fn add_column_rejects_duplicate() {
    let mut table = build_mutation_test_table();
    let result = table.add_column(
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        Some(Value::Scalar(ScalarValue::Int32(0))),
    );
    assert!(result.is_err());
}

#[test]
fn remove_column_drops_from_all_rows() {
    let mut table = build_mutation_test_table();
    table.set_column_keywords(
        "name",
        RecordValue::new(vec![RecordField::new(
            "unit",
            Value::Scalar(ScalarValue::String("none".into())),
        )]),
    );

    table.remove_column("name").expect("remove column");

    assert_eq!(table.schema().unwrap().columns().len(), 1);
    assert!(!table.schema().unwrap().contains_column("name"));
    for i in 0..3 {
        assert_eq!(table_cell(&table, i, "name"), Ok(None));
    }
    assert!(table.column_keywords("name").is_none());
}

#[test]
fn remove_column_round_trips_through_disk() {
    let mut table = build_mutation_test_table();
    table.remove_column("name").expect("remove");

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("rm_col_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        assert_eq!(reopened.schema().unwrap().columns().len(), 1);
        assert!(!reopened.schema().unwrap().contains_column("name"));
        assert_eq!(
            table_cell(&reopened, 0, "id"),
            Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
        );
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn remove_column_missing_errors() {
    let mut table = build_mutation_test_table();
    assert!(table.remove_column("nonexistent").is_err());
}

#[test]
fn rename_column_updates_rows_and_keywords() {
    let mut table = build_mutation_test_table();
    table.set_column_keywords(
        "name",
        RecordValue::new(vec![RecordField::new(
            "unit",
            Value::Scalar(ScalarValue::String("text".into())),
        )]),
    );

    table.rename_column("name", "label").expect("rename");

    assert!(table.schema().unwrap().contains_column("label"));
    assert!(!table.schema().unwrap().contains_column("name"));
    for i in 0..3 {
        assert!(table_cell(&table, i, "label").unwrap().is_some());
        assert_eq!(table_cell(&table, i, "name"), Ok(None));
    }
    assert!(table.column_keywords("label").is_some());
    assert!(table.column_keywords("name").is_none());
}

#[test]
fn rename_column_round_trips_through_disk() {
    let mut table = build_mutation_test_table();
    table.rename_column("name", "label").expect("rename");

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("rename_col_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        assert!(reopened.schema().unwrap().contains_column("label"));
        assert!(!reopened.schema().unwrap().contains_column("name"));
        assert_eq!(
            table_cell(&reopened, 0, "label"),
            Ok(Some(&Value::Scalar(ScalarValue::String("row0".into()))))
        );
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn remove_rows_compacts() {
    let mut table = build_mutation_test_table();
    // Add 2 more rows so we have 5 total (ids 0..5)
    for i in 3..5 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("row{i}"))),
                ),
            ]))
            .expect("add row");
    }
    assert_eq!(table.row_count(), 5);

    // Remove rows at indices 1 and 3 (ids 1, 3)
    table.remove_rows(&[1, 3]).expect("remove rows");

    assert_eq!(table.row_count(), 3);
    // Remaining rows should be ids 0, 2, 4
    assert_eq!(
        table_cell(&table, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
    );
    assert_eq!(
        table_cell(&table, 1, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(2))))
    );
    assert_eq!(
        table_cell(&table, 2, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(4))))
    );
}

#[test]
fn remove_rows_round_trips_through_disk() {
    let mut table = build_mutation_test_table();
    table.remove_rows(&[1]).expect("remove row 1");

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("rm_rows_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        assert_eq!(reopened.row_count(), 2);
        assert_eq!(
            table_cell(&reopened, 0, "id"),
            Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
        );
        assert_eq!(
            table_cell(&reopened, 1, "id"),
            Ok(Some(&Value::Scalar(ScalarValue::Int32(2))))
        );
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn remove_rows_rejects_out_of_bounds() {
    let mut table = build_mutation_test_table();
    assert!(matches!(
        table.remove_rows(&[5]),
        Err(TableError::RowOutOfBounds {
            row_index: 5,
            row_count: 3
        })
    ));
}

#[test]
fn remove_rows_rejects_unsorted() {
    let mut table = build_mutation_test_table();
    assert!(table.remove_rows(&[2, 1]).is_err());
}

#[test]
fn insert_row_at_position() {
    let mut table = build_mutation_test_table();
    let new_row = RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
        RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("inserted".into())),
        ),
    ]);

    table.insert_row(1, new_row).expect("insert at 1");

    assert_eq!(table.row_count(), 4);
    assert_eq!(
        table_cell(&table, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
    );
    assert_eq!(
        table_cell(&table, 1, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(99))))
    );
    assert_eq!(
        table_cell(&table, 2, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(1))))
    );
    assert_eq!(
        table_cell(&table, 3, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(2))))
    );
}

#[test]
fn insert_row_at_end() {
    let mut table = build_mutation_test_table();
    let new_row = RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
        RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("appended".into())),
        ),
    ]);
    table.insert_row(3, new_row).expect("insert at end");
    assert_eq!(table.row_count(), 4);
    assert_eq!(
        table_cell(&table, 3, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(99))))
    );
}

#[test]
fn insert_row_rejects_out_of_bounds() {
    let mut table = build_mutation_test_table();
    let new_row = RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
        RecordField::new("name", Value::Scalar(ScalarValue::String("bad".into()))),
    ]);
    assert!(matches!(
        table.insert_row(10, new_row),
        Err(TableError::RowOutOfBounds { .. })
    ));
}

#[test]
fn insert_row_validates_against_schema() {
    let mut table = build_mutation_test_table();
    // Missing required "id" column
    let bad_row = RecordValue::new(vec![RecordField::new(
        "name",
        Value::Scalar(ScalarValue::String("only name".into())),
    )]);
    assert!(table.insert_row(0, bad_row).is_err());
}

// ---- Locking integration tests ----

#[cfg(unix)]
mod lock_tests {
    use super::*;
    use crate::lock::{LockMode, LockOptions, LockType};

    fn build_test_table_on_disk(dir: &std::path::Path, dm: DataManagerKind) -> TableOptions {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("alice".into()))),
            ]))
            .unwrap();
        let opts = TableOptions::new(dir.join("test.tbl")).with_data_manager(dm);
        table.save(opts.clone()).unwrap();
        opts
    }

    #[test]
    fn open_with_permanent_lock_acquires_immediately() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::PermanentLocking);

        let table = Table::open_with_lock(opts, lock_opts).unwrap();
        assert!(table.has_lock(LockType::Write));
        assert!(table.has_lock(LockType::Read));
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn open_with_user_lock_has_no_lock_until_explicit() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::UserLocking);

        let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
        assert!(!table.has_lock(LockType::Write));
        assert!(!table.has_lock(LockType::Read));

        // Acquire write lock explicitly.
        assert!(table.lock(LockType::Write, 1).unwrap());
        assert!(table.has_lock(LockType::Write));
        assert!(table.has_lock(LockType::Read));
    }

    #[test]
    fn lock_unlock_cycle_user_mode() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::UserLocking);

        let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
        assert!(table.lock(LockType::Write, 1).unwrap());
        assert!(table.has_lock(LockType::Write));

        table.unlock().unwrap();
        assert!(!table.has_lock(LockType::Write));
    }

    #[test]
    fn user_locking_write_requires_explicit_write_lock() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::UserLocking);

        let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
        let err = table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
            ]))
            .unwrap_err();
        assert!(matches!(err, TableError::LockFailed { .. }));

        assert!(table.lock(LockType::Write, 1).unwrap());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
            ]))
            .unwrap();
        table.unlock().unwrap();
    }

    #[test]
    fn auto_locking_write_ops_acquire_write_lock_automatically() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::AutoLocking);

        let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
            ]))
            .unwrap();

        assert_eq!(table.row_count(), 2);
        assert!(!table.has_lock(LockType::Write));
        let reopened = Table::open(opts).unwrap();
        assert_eq!(reopened.row_count(), 2);
    }

    #[test]
    fn unlock_flushes_write_to_disk() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::UserLocking);

        let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
        assert!(table.lock(LockType::Write, 1).unwrap());

        // Add a row while holding the write lock.
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
            ]))
            .unwrap();

        // Unlock should flush to disk.
        table.unlock().unwrap();

        // Reopen without locking and verify.
        let reopened = Table::open(opts).unwrap();
        assert_eq!(reopened.row_count(), 2);
    }

    #[test]
    fn lock_reloads_after_external_modification() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::UserLocking);

        // Open table A with locking, acquire and release.
        let mut table_a = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
        assert!(table_a.lock(LockType::Write, 1).unwrap());
        table_a.unlock().unwrap();

        // Simulate another process: open with locking, modify, unlock.
        {
            let mut table_b = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
            assert!(table_b.lock(LockType::Write, 1).unwrap());
            table_b
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
                    RecordField::new(
                        "name",
                        Value::Scalar(ScalarValue::String("external".into())),
                    ),
                ]))
                .unwrap();
            table_b.unlock().unwrap();
            // table_b dropped here, releasing lock file fd.
        }

        // Re-acquire the lock on table_a — should reload.
        assert!(table_a.lock(LockType::Write, 1).unwrap());
        assert_eq!(table_a.row_count(), 2);
    }

    #[test]
    fn no_lock_backward_compat() {
        // Existing open()/save() API works unchanged.
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let table = Table::open(opts).unwrap();
        assert_eq!(table.row_count(), 1);
        // has_lock returns false for non-locked tables.
        assert!(!table.has_lock(LockType::Write));
    }

    #[test]
    fn lock_file_created_on_disk() {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let lock_opts = LockOptions::new(LockMode::PermanentLocking);

        let _table = Table::open_with_lock(opts, lock_opts).unwrap();
        let lock_path = tmp.path().join("test.tbl").join("table.lock");
        assert!(lock_path.exists(), "table.lock should be created");
    }

    #[test]
    fn lock_on_non_locked_table_errors() {
        let table = Table::new();
        let mut table = table;
        let result = table.lock(LockType::Write, 1);
        assert!(matches!(result, Err(TableError::NotLocked { .. })));
    }

    #[test]
    fn permanent_lock_round_trip_both_dms() {
        for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), dm);
            let lock_opts = LockOptions::new(LockMode::PermanentLocking);

            let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
            assert!(table.has_lock(LockType::Write));
            assert_eq!(table.row_count(), 1);

            // Add a row while permanently locked.
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                    RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
                ]))
                .unwrap();

            // Unlock is a no-op in permanent mode; lock stays held.
            table.unlock().unwrap();
            assert!(table.has_lock(LockType::Write));

            // Drop closes and flushes.
            drop(table);

            // Reopen and verify.
            let reopened = Table::open(opts).unwrap();
            assert_eq!(reopened.row_count(), 2);
        }
    }
}

// -------------------------------------------------------------------
// Memory table tests
// -------------------------------------------------------------------

fn memory_schema() -> TableSchema {
    TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap()
}

fn memory_row(id: i32, name: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
        RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
    ])
}

#[test]
fn new_memory_creates_transient_table() {
    let table = Table::new_memory();
    assert!(table.is_memory());
    assert_eq!(table.table_kind(), super::TableKind::Memory);
    assert_eq!(table.row_count(), 0);
    assert!(table.path().is_none());
}

#[test]
fn with_schema_memory_validates_rows() {
    let mut table = Table::with_schema_memory(memory_schema());
    assert!(table.is_memory());
    table.add_row(memory_row(1, "alice")).unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn from_rows_memory_basic() {
    let rows = vec![memory_row(1, "a"), memory_row(2, "b")];
    let table = Table::from_rows_memory(rows);
    assert!(table.is_memory());
    assert_eq!(table.row_count(), 2);
}

#[test]
fn from_rows_with_schema_memory_validates() {
    let rows = vec![memory_row(1, "a")];
    let table = Table::from_rows_with_schema_memory(rows, memory_schema()).unwrap();
    assert!(table.is_memory());
    assert_eq!(table.row_count(), 1);
}

#[test]
fn memory_table_full_crud_cycle() {
    let mut table = Table::with_schema_memory(memory_schema());
    // add_row
    table.add_row(memory_row(1, "alice")).unwrap();
    table.add_row(memory_row(2, "bob")).unwrap();
    assert_eq!(table.row_count(), 2);

    // set_cell
    table_set_cell(
        &mut table,
        0,
        "name",
        Value::Scalar(ScalarValue::String("ALICE".into())),
    )
    .unwrap();
    assert_eq!(
        table_cell(&table, 0, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String("ALICE".into()))))
    );

    // remove_rows
    table.remove_rows(&[1]).unwrap();
    assert_eq!(table.row_count(), 1);

    // add_column
    table
        .add_column(
            ColumnSchema::scalar("score", PrimitiveType::Float64),
            Some(Value::Scalar(ScalarValue::Float64(0.0))),
        )
        .unwrap();
    assert!(table.schema().unwrap().contains_column("score"));

    // remove_column
    table.remove_column("score").unwrap();
    assert!(!table.schema().unwrap().contains_column("score"));
}

#[test]
fn memory_table_save_materializes_to_disk() {
    let mut table = Table::with_schema_memory(memory_schema());
    table.add_row(memory_row(42, "test")).unwrap();
    table.keywords_mut().push(RecordField::new(
        "origin",
        Value::Scalar(ScalarValue::String("memory".into())),
    ));

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("materialized.tbl");
    table.save(TableOptions::new(&path)).unwrap();

    // Reopen as a plain table.
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert!(!reopened.is_memory());
    assert_eq!(reopened.row_count(), 1);
    assert_eq!(
        table_cell(&reopened, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(42))))
    );
    assert_eq!(
        reopened.keywords().get("origin"),
        Some(&Value::Scalar(ScalarValue::String("memory".into())))
    );
}

#[test]
fn memory_table_save_with_both_data_managers() {
    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let mut table = Table::with_schema_memory(memory_schema());
        table.add_row(memory_row(1, "a")).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(format!("test_{dm:?}.tbl"));
        table
            .save(TableOptions::new(&path).with_data_manager(dm))
            .unwrap();

        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert_eq!(reopened.row_count(), 1);
    }
}

#[test]
fn to_memory_copies_all_data() {
    let mut plain = Table::with_schema(memory_schema());
    plain.add_row(memory_row(1, "orig")).unwrap();
    plain.keywords_mut().push(RecordField::new(
        "key",
        Value::Scalar(ScalarValue::Int32(99)),
    ));

    let mem = plain.to_memory().unwrap();
    assert!(mem.is_memory());
    assert!(mem.path().is_none());
    assert_eq!(mem.row_count(), 1);
    assert_eq!(
        table_cell(&mem, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(1))))
    );
    assert_eq!(
        mem.keywords().get("key"),
        Some(&Value::Scalar(ScalarValue::Int32(99)))
    );
    assert!(mem.schema().is_some());
}

#[test]
fn to_memory_from_disk_table() {
    let mut table = Table::with_schema(memory_schema());
    table.add_row(memory_row(5, "disk")).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("source.tbl");
    table.save(TableOptions::new(&path)).unwrap();

    let disk = Table::open(TableOptions::new(&path)).unwrap();
    let mem = disk.to_memory().unwrap();
    assert!(mem.is_memory());
    assert!(mem.path().is_none());
    assert_eq!(mem.row_count(), 1);
}

#[test]
fn memory_table_sort_and_select() {
    let mut table = Table::with_schema_memory(memory_schema());
    for (id, name) in [(3, "c"), (1, "a"), (2, "b")] {
        table.add_row(memory_row(id, name)).unwrap();
    }

    // Sort.
    let sorted = table.sort(&[("id", super::SortOrder::Ascending)]).unwrap();
    assert_eq!(sorted.row_count(), 3);
    assert_eq!(
        sorted.cell(0, "id").unwrap(),
        Some(&Value::Scalar(ScalarValue::Int32(1)))
    );
    drop(sorted);

    // Select by predicate.
    let view = table
        .select(
            |row| matches!(row.get("id"), Some(Value::Scalar(ScalarValue::Int32(i))) if *i >= 2),
        )
        .unwrap();
    assert_eq!(view.row_count(), 2);
}

#[test]
fn memory_table_iter_groups() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("group", PrimitiveType::String),
        ColumnSchema::scalar("val", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema_memory(schema);
    for (g, v) in [("a", 1), ("b", 2), ("a", 3)] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("group", Value::Scalar(ScalarValue::String(g.into()))),
                RecordField::new("val", Value::Scalar(ScalarValue::Int32(v))),
            ]))
            .unwrap();
    }

    let groups: Vec<_> = table
        .iter_groups(&[("group", super::SortOrder::Ascending)])
        .unwrap()
        .collect();
    assert_eq!(groups.len(), 2);
}

#[cfg(unix)]
#[test]
fn memory_table_lock_is_noop() {
    use crate::lock::LockType;

    let table = Table::new_memory();
    assert!(table.has_lock(LockType::Write));
    assert!(table.has_lock(LockType::Read));
    assert!(!table.is_multi_used());
}

#[cfg(unix)]
#[test]
fn memory_table_lock_unlock_succeed() {
    use crate::lock::LockType;

    let mut table = Table::new_memory();
    assert!(table.lock(LockType::Write, 1).unwrap());
    table.unlock().unwrap();
}

#[test]
fn plain_table_kind_is_default() {
    let table = Table::new();
    assert!(!table.is_memory());
    assert_eq!(table.table_kind(), super::TableKind::Plain);
}

// -------------------------------------------------------------------
// Virtual column tests
// -------------------------------------------------------------------

#[test]
fn forward_column_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base_table");
    let fwd_path = dir.path().join("fwd_table");

    // Create and save a base table with some data.
    let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "value",
        casa_types::PrimitiveType::Float64,
    )])
    .unwrap();
    let mut base = Table::with_schema(base_schema);
    for v in [1.5, 2.5, 3.5] {
        base.add_row(RecordValue::new(vec![RecordField::new(
            "value",
            Value::Scalar(ScalarValue::Float64(v)),
        )]))
        .unwrap();
    }
    base.save(TableOptions::new(&base_path)).unwrap();

    // Create a forwarding table that references the base table's "value" column.
    let fwd_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "value",
        casa_types::PrimitiveType::Float64,
    )])
    .unwrap();
    let mut fwd = Table::with_schema(fwd_schema);
    for _ in 0..3 {
        fwd.add_row(RecordValue::new(vec![RecordField::new(
            "value",
            Value::Scalar(ScalarValue::Float64(0.0)),
        )]))
        .unwrap();
    }
    fwd.bind_forward_column("value", &base_path).unwrap();
    fwd.save(TableOptions::new(&fwd_path)).unwrap();

    // Reopen and verify forwarded values.
    let reopened = Table::open(TableOptions::new(&fwd_path)).unwrap();
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("value"));
    for (i, expected) in [1.5, 2.5, 3.5].iter().enumerate() {
        let val = table_cell(&reopened, i, "value").unwrap();
        match val.unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!(
                    (v - expected).abs() < 1e-10,
                    "row {i}: expected {expected}, got {v}"
                );
            }
            other => panic!("row {i}: expected Float64, got {other:?}"),
        }
    }
}

#[test]
fn scaled_array_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let table_path = dir.path().join("scaled_table");

    let scale = 2.5;
    let offset = 10.0;

    // Schema: stored_col (Int32 scalar), virtual_col (Float64 scalar).
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("stored_col", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("virtual_col", casa_types::PrimitiveType::Float64),
    ])
    .unwrap();

    let mut table = Table::with_schema(schema);
    for i in [1i32, 2, 3] {
        // Only stored_col has meaningful data; virtual_col is a placeholder.
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("stored_col", Value::Scalar(ScalarValue::Int32(i))),
                RecordField::new("virtual_col", Value::Scalar(ScalarValue::Float64(0.0))),
            ]))
            .unwrap();
    }
    table
        .bind_scaled_array_column("virtual_col", "stored_col", scale, offset)
        .unwrap();
    table.save(TableOptions::new(&table_path)).unwrap();

    // Reopen and verify: virtual = stored * 2.5 + 10.0
    let reopened = Table::open(TableOptions::new(&table_path)).unwrap();
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("virtual_col"));
    assert!(!reopened.is_virtual_column("stored_col"));

    for (i, stored) in [1i32, 2, 3].iter().enumerate() {
        let expected = (*stored as f64) * scale + offset;
        let val = table_cell(&reopened, i, "virtual_col").unwrap();
        match val.unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!(
                    (v - expected).abs() < 1e-10,
                    "row {i}: expected {expected}, got {v}"
                );
            }
            other => panic!("row {i}: expected Float64, got {other:?}"),
        }
    }
}

#[test]
fn is_virtual_column_empty_for_plain_table() {
    let table = Table::new();
    assert!(!table.is_virtual_column("anything"));
}

#[test]
fn multi_dm_round_trip() {
    // Test that a table with both stored and virtual columns produces
    // multiple DM entries in table.dat after save/reload.
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base");
    let main_path = dir.path().join("main");

    // Base table for forward column.
    let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "fwd_col",
        casa_types::PrimitiveType::Float64,
    )])
    .unwrap();
    let mut base = Table::with_schema(base_schema);
    base.add_row(RecordValue::new(vec![RecordField::new(
        "fwd_col",
        Value::Scalar(ScalarValue::Float64(42.0)),
    )]))
    .unwrap();
    base.save(TableOptions::new(&base_path)).unwrap();

    // Main table with stored + forward + scaled columns.
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("stored_int", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("fwd_col", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("scaled_col", casa_types::PrimitiveType::Float64),
    ])
    .unwrap();

    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("stored_int", Value::Scalar(ScalarValue::Int32(5))),
            RecordField::new("fwd_col", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("scaled_col", Value::Scalar(ScalarValue::Float64(0.0))),
        ]))
        .unwrap();

    table.bind_forward_column("fwd_col", &base_path).unwrap();
    table
        .bind_scaled_array_column("scaled_col", "stored_int", 3.0, 1.0)
        .unwrap();
    table.save(TableOptions::new(&main_path)).unwrap();

    // Reopen and verify all columns.
    let reopened = Table::open(TableOptions::new(&main_path)).unwrap();
    assert_eq!(reopened.row_count(), 1);
    assert!(!reopened.is_virtual_column("stored_int"));
    assert!(reopened.is_virtual_column("fwd_col"));
    assert!(reopened.is_virtual_column("scaled_col"));

    // stored_int should be 5
    match table_cell(&reopened, 0, "stored_int").unwrap().unwrap() {
        Value::Scalar(ScalarValue::Int32(v)) => assert_eq!(*v, 5),
        other => panic!("expected Int32(5), got {other:?}"),
    }

    // fwd_col should be 42.0 (from base table)
    match table_cell(&reopened, 0, "fwd_col").unwrap().unwrap() {
        Value::Scalar(ScalarValue::Float64(v)) => {
            assert!((v - 42.0).abs() < 1e-10, "fwd_col: expected 42.0, got {v}");
        }
        other => panic!("expected Float64(42.0), got {other:?}"),
    }

    // scaled_col should be 5 * 3.0 + 1.0 = 16.0
    match table_cell(&reopened, 0, "scaled_col").unwrap().unwrap() {
        Value::Scalar(ScalarValue::Float64(v)) => {
            assert!(
                (v - 16.0).abs() < 1e-10,
                "scaled_col: expected 16.0, got {v}"
            );
        }
        other => panic!("expected Float64(16.0), got {other:?}"),
    }
}

// -------------------------------------------------------------------
// TableInfo round-trip tests
// -------------------------------------------------------------------

#[test]
fn table_info_default_is_empty() {
    let table = Table::new();
    assert_eq!(table.info().table_type, "");
    assert_eq!(table.info().sub_type, "");
}

#[test]
fn table_info_set_and_get() {
    use crate::storage::TableInfo;
    let mut table = Table::new();
    table.set_info(TableInfo {
        table_type: "MeasurementSet".to_string(),
        sub_type: "UVFITS".to_string(),
        readme: Vec::new(),
    });
    assert_eq!(table.info().table_type, "MeasurementSet");
    assert_eq!(table.info().sub_type, "UVFITS");
}

#[test]
fn table_info_round_trip_disk() {
    use crate::storage::TableInfo;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("info_test.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "MeasurementSet".to_string(),
        sub_type: "UVFITS".to_string(),
        readme: Vec::new(),
    });
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(42)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&path)).unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.info().table_type, "MeasurementSet");
    assert_eq!(reopened.info().sub_type, "UVFITS");
}

#[test]
fn table_info_empty_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_info.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    table.save(TableOptions::new(&path)).unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.info().table_type, "");
    assert_eq!(reopened.info().sub_type, "");
}

#[test]
fn table_info_preserved_by_to_memory() {
    use crate::storage::TableInfo;
    let mut table = Table::new();
    table.set_info(TableInfo {
        table_type: "Catalog".to_string(),
        sub_type: "".to_string(),
        readme: Vec::new(),
    });
    let mem = table.to_memory().unwrap();
    assert_eq!(mem.info().table_type, "Catalog");
}

#[test]
fn table_info_preserved_by_deep_copy() {
    use crate::storage::TableInfo;
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.tbl");
    let dst_path = dir.path().join("dst.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Float64)]).unwrap();
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Sky".to_string(),
        sub_type: "Model".to_string(),
        readme: Vec::new(),
    });
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Float64(1.0)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&src_path)).unwrap();

    let original = Table::open(TableOptions::new(&src_path)).unwrap();
    original.deep_copy(TableOptions::new(&dst_path)).unwrap();

    let copy = Table::open(TableOptions::new(&dst_path)).unwrap();
    assert_eq!(copy.info().table_type, "Sky");
    assert_eq!(copy.info().sub_type, "Model");
}

// -------------------------------------------------------------------
// Lifecycle operation tests
// -------------------------------------------------------------------

#[test]
fn flush_writes_changes_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flush_test.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&path)).unwrap();

    // Reopen, mutate, and flush
    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(2)),
        )]))
        .unwrap();
    table.flush().unwrap();

    // Reopen and verify both rows
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.row_count(), 2);
}

#[test]
fn flush_without_path_fails() {
    let table = Table::new();
    assert!(table.flush().is_err());
}

#[test]
fn resync_discards_in_memory_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resync_test.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&path)).unwrap();

    // Open from disk, add a row in memory (not saved)
    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(2)),
        )]))
        .unwrap();
    assert_eq!(table.row_count(), 2);

    // Resync discards the unsaved row
    table.resync().unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn resync_without_path_fails() {
    let mut table = Table::new();
    assert!(table.resync().is_err());
}

#[test]
fn mark_for_delete_removes_directory_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delete_me.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    table.save(TableOptions::new(&path)).unwrap();
    assert!(path.exists());

    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table.mark_for_delete();
    assert!(table.is_marked_for_delete());

    drop(table);
    assert!(!path.exists(), "table directory should be deleted on drop");
}

#[test]
fn unmark_for_delete_prevents_removal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keep_me.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    table.save(TableOptions::new(&path)).unwrap();

    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table.mark_for_delete();
    table.unmark_for_delete();
    assert!(!table.is_marked_for_delete());

    drop(table);
    assert!(path.exists(), "table directory should still exist");
}

// -------------------------------------------------------------------
// Locking extension tests
// -------------------------------------------------------------------

#[test]
fn lock_mode_resolve() {
    use crate::lock::LockMode;
    assert_eq!(LockMode::DefaultLocking.resolve(), LockMode::AutoLocking);
    assert_eq!(LockMode::AutoLocking.resolve(), LockMode::AutoLocking);
    assert_eq!(LockMode::NoLocking.resolve(), LockMode::NoLocking);
}

#[test]
fn lock_mode_skip_read_lock() {
    use crate::lock::LockMode;
    assert!(LockMode::AutoNoReadLocking.skip_read_lock());
    assert!(LockMode::UserNoReadLocking.skip_read_lock());
    assert!(!LockMode::AutoLocking.skip_read_lock());
    assert!(!LockMode::UserLocking.skip_read_lock());
}

#[test]
fn external_sync_hook_ordering() {
    use crate::lock::ExternalLockSync;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Recorder(Arc<Mutex<Vec<&'static str>>>);
    impl ExternalLockSync for Recorder {
        fn acquire_read(&self) {
            self.0.lock().unwrap().push("acquire_read");
        }
        fn acquire_write(&self) {
            self.0.lock().unwrap().push("acquire_write");
        }
        fn release(&self) {
            self.0.lock().unwrap().push("release");
        }
    }

    let log = Arc::new(Mutex::new(Vec::new()));
    let recorder = Recorder(log.clone());

    // Verify the trait is object-safe and can be boxed
    let _: Box<dyn ExternalLockSync> = Box::new(recorder);

    // Verify the log works
    let recorder2 = Recorder(log.clone());
    recorder2.acquire_read();
    recorder2.acquire_write();
    recorder2.release();

    let events = log.lock().unwrap();
    assert_eq!(&*events, &["acquire_read", "acquire_write", "release"]);
}

// -------------------------------------------------------------------
// Set algebra tests
// -------------------------------------------------------------------

#[test]
fn row_union_merges_and_deduplicates() {
    assert_eq!(
        Table::row_union(&[0, 2, 4], &[1, 2, 3]),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn row_intersection_keeps_common() {
    assert_eq!(
        Table::row_intersection(&[0, 1, 2, 3], &[2, 3, 4]),
        vec![2, 3]
    );
}

#[test]
fn row_difference_removes_second() {
    assert_eq!(Table::row_difference(&[0, 1, 2, 3], &[1, 3]), vec![0, 2]);
}

#[test]
fn row_set_ops_with_empty() {
    assert!(Table::row_intersection(&[0, 1], &[]).is_empty());
    assert_eq!(Table::row_union(&[], &[3, 1]), vec![1, 3]);
    assert_eq!(Table::row_difference(&[5, 3], &[]), vec![3, 5]);
}

#[test]
fn row_set_operations_cover_greater_and_unsorted_paths() {
    assert_eq!(Table::row_intersection(&[2, 4], &[1, 2]), vec![2]);
    assert_eq!(Table::row_difference(&[2, 4], &[1, 2]), vec![4]);
    assert_eq!(
        Table::row_intersection(&[4, 2, 2, 1], &[3, 4, 2]),
        vec![2, 4]
    );
    assert_eq!(Table::row_difference(&[4, 2, 2, 1], &[4, 3]), vec![1, 2]);
}

// -------------------------------------------------------------------
// NoSort iteration tests
// -------------------------------------------------------------------

#[test]
fn iter_groups_nosort_preserves_natural_order() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    // Insert pattern: A, B, A — nosort should yield 3 groups
    for v in [1, 2, 1] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "k",
                Value::Scalar(ScalarValue::Int32(v)),
            )]))
            .unwrap();
    }

    let groups: Vec<_> = table.iter_groups_nosort(&["k"]).unwrap().collect();
    assert_eq!(
        groups.len(),
        3,
        "nosort should not merge non-adjacent duplicates"
    );
    assert_eq!(groups[0].row_indices, vec![0]); // k=1
    assert_eq!(groups[1].row_indices, vec![1]); // k=2
    assert_eq!(groups[2].row_indices, vec![2]); // k=1 again (separate group)
}

#[test]
fn iter_groups_nosort_merges_consecutive_equal() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    for v in [1, 1, 2, 2, 2] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "k",
                Value::Scalar(ScalarValue::Int32(v)),
            )]))
            .unwrap();
    }

    let groups: Vec<_> = table.iter_groups_nosort(&["k"]).unwrap().collect();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].row_indices, vec![0, 1]); // k=1
    assert_eq!(groups[1].row_indices, vec![2, 3, 4]); // k=2
}

// -------------------------------------------------------------------
// Slicer and cell slicing tests
// -------------------------------------------------------------------

#[test]
fn slicer_contiguous_2d() {
    use super::Slicer;
    let s = Slicer::contiguous(vec![0, 1], vec![2, 3]).unwrap();
    assert_eq!(s.ndim(), 2);
    assert_eq!(s.start(), &[0, 1]);
    assert_eq!(s.end(), &[2, 3]);
    assert_eq!(s.stride(), &[1, 1]);
}

#[test]
fn slicer_rejects_zero_stride() {
    use super::Slicer;
    assert!(Slicer::new(vec![0], vec![5], vec![0]).is_err());
}

#[test]
fn slicer_rejects_start_gt_end() {
    use super::Slicer;
    assert!(Slicer::new(vec![5], vec![3], vec![1]).is_err());
}

#[test]
fn get_cell_slice_2d() {
    use super::Slicer;
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![3, 4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // 3x4 array filled with value = row*10 + col
    let arr: ArrayD<f64> =
        ArrayD::from_shape_fn(IxDyn(&[3, 4]), |idx| (idx[0] * 10 + idx[1]) as f64);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float64(arr)),
        )]))
        .unwrap();

    // Slice rows 1..3, cols 2..4
    let slicer = Slicer::contiguous(vec![1, 2], vec![3, 4]).unwrap();
    let sliced = table.get_cell_slice("data", 0, &slicer).unwrap();

    match sliced {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.shape(), &[2, 2]);
            assert_eq!(a[[0, 0]], 12.0); // row=1, col=2
            assert_eq!(a[[0, 1]], 13.0); // row=1, col=3
            assert_eq!(a[[1, 0]], 22.0); // row=2, col=2
            assert_eq!(a[[1, 1]], 23.0); // row=2, col=3
        }
        other => panic!("expected Float64 array, got {other:?}"),
    }
}

#[test]
fn put_cell_slice_2d() {
    use super::Slicer;
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![3, 4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[3, 4]));
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float64(arr)),
        )]))
        .unwrap();

    // Write 99.0 into the [1..3, 0..2] sub-region
    let patch: ArrayD<f64> = ArrayD::from_elem(IxDyn(&[2, 2]), 99.0);
    let slicer = Slicer::contiguous(vec![1, 0], vec![3, 2]).unwrap();
    table
        .put_cell_slice("data", 0, &slicer, &ArrayValue::Float64(patch))
        .unwrap();

    match table_cell(&table, 0, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a[[0, 0]], 0.0); // untouched
            assert_eq!(a[[1, 0]], 99.0); // patched
            assert_eq!(a[[2, 1]], 99.0); // patched
            assert_eq!(a[[0, 3]], 0.0); // untouched
        }
        other => panic!("expected Float64 array, got {other:?}"),
    }
}

#[test]
fn get_cell_slice_with_stride() {
    use super::Slicer;
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Int32,
        vec![6],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let arr: ArrayD<i32> = ArrayD::from_shape_fn(IxDyn(&[6]), |idx| idx[0] as i32);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Int32(arr)),
        )]))
        .unwrap();

    // Every other element: [0, 2, 4]
    let slicer = Slicer::new(vec![0], vec![6], vec![2]).unwrap();
    let sliced = table.get_cell_slice("data", 0, &slicer).unwrap();

    match sliced {
        Value::Array(ArrayValue::Int32(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0, 2, 4]);
        }
        other => panic!("expected Int32 array, got {other:?}"),
    }
}

#[test]
fn get_cell_slice_scalar_cell_fails() {
    use super::Slicer;

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(42)),
        )]))
        .unwrap();

    let slicer = Slicer::contiguous(vec![0], vec![1]).unwrap();
    assert!(table.get_cell_slice("x", 0, &slicer).is_err());
}

#[test]
fn slice_errors_cover_missing_columns_and_invalid_bounds() {
    use super::Slicer;
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![4]),
        ColumnSchema::scalar("x", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "data",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[4]), vec![0, 1, 2, 3]).unwrap(),
                )),
            ),
            RecordField::new("x", Value::Scalar(ScalarValue::Int32(42))),
        ]))
        .unwrap();

    let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();
    let missing_read = table.get_cell_slice("missing", 0, &slicer).unwrap_err();
    assert!(matches!(
        missing_read,
        TableError::ColumnNotFound {
            row_index: 0,
            ref column
        } if column == "missing"
    ));

    let missing_write = table
        .put_cell_slice(
            "missing",
            0,
            &slicer,
            &ArrayValue::Int32(ArrayD::from_shape_vec(IxDyn(&[2]), vec![9, 9]).unwrap()),
        )
        .unwrap_err();
    assert!(matches!(
        missing_write,
        TableError::ColumnNotFound {
            row_index: 0,
            ref column
        } if column == "missing"
    ));

    let scalar_write = table
        .put_cell_slice(
            "x",
            0,
            &slicer,
            &ArrayValue::Int32(ArrayD::from_shape_vec(IxDyn(&[2]), vec![9, 9]).unwrap()),
        )
        .unwrap_err();
    assert!(matches!(
        scalar_write,
        TableError::CellNotArray { row: 0, ref column } if column == "x"
    ));

    let mismatch = table
        .get_cell_slice(
            "data",
            0,
            &Slicer::contiguous(vec![0, 0], vec![1, 1]).unwrap(),
        )
        .unwrap_err();
    assert!(matches!(
        mismatch,
        TableError::SlicerDimensionMismatch {
            start_ndim: 2,
            end_ndim: 1,
            stride_ndim: 2
        }
    ));

    let out_of_bounds = table
        .get_cell_slice("data", 0, &Slicer::contiguous(vec![3], vec![5]).unwrap())
        .unwrap_err();
    assert!(matches!(
        out_of_bounds,
        TableError::SlicerOutOfBounds {
            axis: 0,
            index: 5,
            extent: 4
        }
    ));
}

#[test]
fn get_column_slice_multiple_rows() {
    use super::{RowRange, Slicer};
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Int32,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    for i in 0..3 {
        let arr = ArrayD::from_shape_fn(IxDyn(&[4]), |idx| (i * 10 + idx[0]) as i32);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Int32(arr)),
            )]))
            .unwrap();
    }

    // Slice elements [1..3] from rows 0 and 1
    let slicer = Slicer::contiguous(vec![1], vec![3]).unwrap();
    let results = table
        .get_column_slice("data", RowRange::new(0, 2), &slicer)
        .unwrap();

    assert_eq!(results.len(), 2);
    match &results[0] {
        Value::Array(ArrayValue::Int32(a)) => assert_eq!(a.as_slice().unwrap(), &[1, 2]),
        other => panic!("expected Int32 array, got {other:?}"),
    }
    match &results[1] {
        Value::Array(ArrayValue::Int32(a)) => assert_eq!(a.as_slice().unwrap(), &[11, 12]),
        other => panic!("expected Int32 array, got {other:?}"),
    }
}

#[test]
fn put_column_slice_multiple_rows() {
    use super::{RowRange, Slicer};
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    for _ in 0..3 {
        let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[4]));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();
    }

    // Patch elements [1..3] in rows 0 and 2 (stride=2)
    let slicer = Slicer::contiguous(vec![1], vec![3]).unwrap();
    let patches = vec![
        ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 11.0)),
        ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 22.0)),
    ];
    table
        .put_column_slice("data", RowRange::with_stride(0, 3, 2), &slicer, &patches)
        .unwrap();

    // Row 0: [0, 11, 11, 0]
    match table_cell(&table, 0, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 11.0, 11.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
    // Row 1: untouched
    match table_cell(&table, 1, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 0.0, 0.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
    // Row 2: [0, 22, 22, 0]
    match table_cell(&table, 2, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 22.0, 22.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn put_column_slice_length_mismatch() {
    use super::{RowRange, Slicer};
    use casa_types::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    for _ in 0..2 {
        let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[4]));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();
    }

    let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();
    // 2 rows selected but only 1 data element
    let patches = vec![ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 1.0))];
    let result = table.put_column_slice("data", RowRange::new(0, 2), &slicer, &patches);
    assert!(result.is_err());
}

#[test]
fn slice_helpers_cover_remaining_array_variants() {
    use super::Slicer;
    use casa_types::{ArrayD, Complex32, Complex64};
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("c32", PrimitiveType::Complex32, vec![2]),
        ColumnSchema::array_fixed("c64", PrimitiveType::Complex64, vec![2]),
        ColumnSchema::array_fixed("text", PrimitiveType::String, vec![2]),
        ColumnSchema::array_fixed("ints", PrimitiveType::Int32, vec![2]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "c32",
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]),
                        vec![Complex32::new(1.0, 2.0), Complex32::new(3.0, 4.0)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "c64",
                Value::Array(ArrayValue::Complex64(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]),
                        vec![Complex64::new(5.0, 6.0), Complex64::new(7.0, 8.0)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "text",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]),
                        vec!["alpha".to_string(), "beta".to_string()],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "ints",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[2]), vec![1, 2]).unwrap(),
                )),
            ),
        ]))
        .unwrap();

    let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();

    match table.get_cell_slice("c32", 0, &slicer).unwrap() {
        Value::Array(ArrayValue::Complex32(values)) => {
            assert_eq!(values[[1]], Complex32::new(3.0, 4.0));
        }
        other => panic!("unexpected sliced complex32 value: {other:?}"),
    }

    match table.get_cell_slice("c64", 0, &slicer).unwrap() {
        Value::Array(ArrayValue::Complex64(values)) => {
            assert_eq!(values[[0]], Complex64::new(5.0, 6.0));
        }
        other => panic!("unexpected sliced complex64 value: {other:?}"),
    }

    match table.get_cell_slice("text", 0, &slicer).unwrap() {
        Value::Array(ArrayValue::String(values)) => {
            assert_eq!(values[[1]], "beta");
        }
        other => panic!("unexpected sliced string value: {other:?}"),
    }

    table
        .put_cell_slice(
            "c32",
            0,
            &slicer,
            &ArrayValue::Complex32(
                ArrayD::from_shape_vec(
                    IxDyn(&[2]),
                    vec![Complex32::new(9.0, 1.0), Complex32::new(8.0, 2.0)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    match table_cell(&table, 0, "c32").unwrap().unwrap() {
        Value::Array(ArrayValue::Complex32(values)) => {
            assert_eq!(values[[0]], Complex32::new(9.0, 1.0));
        }
        other => panic!("unexpected put complex32 value: {other:?}"),
    }

    table
        .put_cell_slice(
            "c64",
            0,
            &slicer,
            &ArrayValue::Complex64(
                ArrayD::from_shape_vec(
                    IxDyn(&[2]),
                    vec![Complex64::new(6.0, 5.0), Complex64::new(8.0, 7.0)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    match table_cell(&table, 0, "c64").unwrap().unwrap() {
        Value::Array(ArrayValue::Complex64(values)) => {
            assert_eq!(values[[1]], Complex64::new(8.0, 7.0));
        }
        other => panic!("unexpected put complex64 value: {other:?}"),
    }

    table
        .put_cell_slice(
            "text",
            0,
            &slicer,
            &ArrayValue::String(
                ArrayD::from_shape_vec(IxDyn(&[2]), vec!["delta".to_string(), "gamma".to_string()])
                    .unwrap(),
            ),
        )
        .unwrap();
    match table_cell(&table, 0, "text").unwrap().unwrap() {
        Value::Array(ArrayValue::String(values)) => {
            assert_eq!(values[[0]], "delta");
        }
        other => panic!("unexpected put string value: {other:?}"),
    }

    table
        .put_cell_slice(
            "ints",
            0,
            &slicer,
            &ArrayValue::Float64(ArrayD::from_shape_vec(IxDyn(&[2]), vec![9.0, 9.0]).unwrap()),
        )
        .unwrap();
    match table_cell(&table, 0, "ints").unwrap().unwrap() {
        Value::Array(ArrayValue::Int32(values)) => assert_eq!(values.as_slice().unwrap(), &[1, 2]),
        other => panic!("unexpected mismatch target value: {other:?}"),
    }
}

// ---- Row copy and fill tests ----

#[test]
fn copy_rows_appends_all() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut dst = Table::with_schema(schema.clone());
    dst.add_row(RecordValue::new(vec![RecordField::new(
        "x",
        Value::Scalar(ScalarValue::Int32(0)),
    )]))
    .unwrap();

    let src = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(1)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(2)),
            )]),
        ],
        schema,
    )
    .unwrap();

    dst.copy_rows(&src).unwrap();
    assert_eq!(dst.row_count(), 3);
    assert_eq!(
        table_cell(&dst, 2, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(2))))
    );
}

#[test]
fn copy_rows_schema_mismatch() {
    let s1 = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let s2 = TableSchema::new(vec![ColumnSchema::scalar("y", PrimitiveType::Int32)]).unwrap();
    let mut dst = Table::with_schema(s1);
    let src = Table::with_schema(s2);
    assert!(dst.copy_rows(&src).is_err());
}

#[test]
fn copy_rows_with_mapping_selects_rows() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let src = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(10)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(20)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(30)),
            )]),
        ],
        schema.clone(),
    )
    .unwrap();

    let mut dst = Table::with_schema(schema);
    dst.copy_rows_with_mapping(&src, &[2, 0]).unwrap();
    assert_eq!(dst.row_count(), 2);
    assert_eq!(
        table_cell(&dst, 0, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(30))))
    );
    assert_eq!(
        table_cell(&dst, 1, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(10))))
    );
}

#[test]
fn copy_info_transfers_metadata() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut src = Table::with_schema(schema.clone());
    src.set_info(crate::TableInfo {
        table_type: "MeasurementSet".into(),
        sub_type: "".into(),
        readme: Vec::new(),
    });

    let mut dst = Table::with_schema(schema);
    dst.copy_info(&src);
    assert_eq!(dst.info().table_type, "MeasurementSet");
}

#[test]
fn fill_column_sets_all_cells() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(1)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(2)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(3)),
            )]),
        ],
        schema,
    )
    .unwrap();

    table
        .fill_column("x", Value::Scalar(ScalarValue::Int32(99)))
        .unwrap();
    for i in 0..3 {
        assert_eq!(
            table_cell(&table, i, "x"),
            Ok(Some(&Value::Scalar(ScalarValue::Int32(99))))
        );
    }
}

#[test]
fn fill_column_range_sets_subset() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
        ],
        schema,
    )
    .unwrap();

    // Fill only rows 1 and 3 (stride=2 starting at 1)
    table
        .fill_column_range(
            "x",
            super::RowRange::with_stride(1, 4, 2),
            Value::Scalar(ScalarValue::Int32(77)),
        )
        .unwrap();

    assert_eq!(
        table_cell(&table, 0, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
    );
    assert_eq!(
        table_cell(&table, 1, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(77))))
    );
    assert_eq!(
        table_cell(&table, 2, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
    );
    assert_eq!(
        table_cell(&table, 3, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(77))))
    );
}

// -------------------------------------------------------------------
// Wave 24 — Data manager introspection
// -------------------------------------------------------------------

#[test]
fn data_manager_info_empty_for_memory_table() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    assert!(table.data_manager_info().is_empty());
}

#[test]
fn data_manager_info_populated_after_roundtrip() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("a", PrimitiveType::Int32),
        ColumnSchema::scalar("b", PrimitiveType::Float64),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("a", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("b", Value::Scalar(ScalarValue::Float64(2.0))),
        ]))
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dm_info_test");
    table.save(TableOptions::new(&path)).unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    let info = reopened.data_manager_info();
    assert!(!info.is_empty(), "should have at least one DM");
    // All columns should appear somewhere across the DMs
    let all_cols: Vec<&str> = info
        .iter()
        .flat_map(|dm| dm.columns.iter().map(|s| s.as_str()))
        .collect();
    assert!(all_cols.contains(&"a"));
    assert!(all_cols.contains(&"b"));
}

#[test]
fn show_structure_contains_columns_and_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(1.5))),
        ]))
        .unwrap();

    let output = table.show_structure();
    assert!(output.contains("1 rows"), "should show row count");
    assert!(output.contains("id"), "should list id column");
    assert!(output.contains("flux"), "should list flux column");
    assert!(output.contains("Scalar"), "should show scalar type");
}

#[test]
fn column_keywords_roundtrip_scalar() {
    // Red test: simple scalar column keyword must survive save→open.
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("flux", PrimitiveType::Float64)]).unwrap();
    let mut table = Table::with_schema(schema);
    table.set_column_keywords(
        "flux",
        RecordValue::new(vec![RecordField::new(
            "unit",
            Value::Scalar(ScalarValue::String("Jy".into())),
        )]),
    );
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "flux",
            Value::Scalar(ScalarValue::Float64(1.5)),
        )]))
        .unwrap();

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("col_kw_scalar_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");

        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        let kw = reopened
            .column_keywords("flux")
            .expect("column keywords should survive roundtrip");
        assert_eq!(
            kw.get("unit"),
            Some(&Value::Scalar(ScalarValue::String("Jy".into()))),
            "dm={dm:?}: scalar column keyword 'unit' should roundtrip"
        );
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn column_keywords_roundtrip_nested_record() {
    // Red test: nested sub-record keyword (like MEASINFO) must survive save→open.
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let mut measinfo = RecordValue::default();
    measinfo.upsert("type", Value::Scalar(ScalarValue::String("epoch".into())));
    measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("UTC".into())));
    let mut col_kw = RecordValue::default();
    col_kw.upsert("MEASINFO", Value::Record(measinfo));
    table.set_column_keywords("TIME", col_kw);

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "TIME",
            Value::Array(ArrayValue::from_f64_vec(vec![51544.5])),
        )]))
        .unwrap();

    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("col_kw_nested_{dm:?}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");

        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        let kw = reopened
            .column_keywords("TIME")
            .expect("column keywords should survive roundtrip");
        match kw.get("MEASINFO") {
            Some(Value::Record(mi)) => {
                assert_eq!(
                    mi.get("type"),
                    Some(&Value::Scalar(ScalarValue::String("epoch".into()))),
                    "dm={dm:?}: MEASINFO.type should be 'epoch'"
                );
                assert_eq!(
                    mi.get("Ref"),
                    Some(&Value::Scalar(ScalarValue::String("UTC".into()))),
                    "dm={dm:?}: MEASINFO.Ref should be 'UTC'"
                );
            }
            other => panic!("dm={dm:?}: expected MEASINFO record, got {other:?}"),
        }
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

#[test]
fn column_keywords_visible_in_table_dat_binary() {
    // Verify the on-disk table.dat contains column keywords in the column descriptor,
    // not just in a side channel. This catches C++ compatibility issues.
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let mut measinfo = RecordValue::default();
    measinfo.upsert("type", Value::Scalar(ScalarValue::String("epoch".into())));
    measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("UTC".into())));
    let mut col_kw = RecordValue::default();
    col_kw.upsert("MEASINFO", Value::Record(measinfo));
    table.set_column_keywords("TIME", col_kw);

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "TIME",
            Value::Array(ArrayValue::from_f64_vec(vec![51544.5])),
        )]))
        .unwrap();

    let root = unique_test_dir("col_kw_binary");
    std::fs::create_dir_all(&root).expect("mkdir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO))
        .expect("save");

    // Read table.dat and verify MEASINFO appears in the binary
    let table_dat_bytes = std::fs::read(root.join("table.dat")).expect("read table.dat");
    let as_str = String::from_utf8_lossy(&table_dat_bytes);
    assert!(
        as_str.contains("MEASINFO"),
        "table.dat should contain MEASINFO string in column descriptor keywords"
    );

    // Verify the keywords survive Rust roundtrip
    let reopened = Table::open(TableOptions::new(&root)).expect("open");
    let kw = reopened
        .column_keywords("TIME")
        .expect("should have keywords");
    match kw.get("MEASINFO") {
        Some(Value::Record(mi)) => {
            assert_eq!(
                mi.get("type"),
                Some(&Value::Scalar(ScalarValue::String("epoch".into())))
            );
        }
        other => panic!("expected MEASINFO record, got {other:?}"),
    }

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn show_keywords_includes_table_and_column_keywords() {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("flux", PrimitiveType::Float64)]).unwrap();
    let mut table = Table::with_schema(schema);
    *table.keywords_mut() = RecordValue::new(vec![RecordField::new(
        "telescope",
        Value::Scalar(ScalarValue::String("ALMA".into())),
    )]);
    table.set_column_keywords(
        "flux",
        RecordValue::new(vec![RecordField::new(
            "unit",
            Value::Scalar(ScalarValue::String("Jy".into())),
        )]),
    );

    let output = table.show_keywords();
    assert!(
        output.contains("Table keywords:"),
        "should have table keywords header"
    );
    assert!(
        output.contains("telescope"),
        "should show telescope keyword"
    );
    assert!(
        output.contains("Column \"flux\" keywords:"),
        "should have column keywords header"
    );
    assert!(output.contains("unit"), "should show unit keyword");
}
