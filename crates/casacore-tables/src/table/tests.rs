// SPDX-License-Identifier: LGPL-3.0-or-later
use std::path::PathBuf;

use casacore_types::{
    Array2, ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};

use crate::schema::{ColumnSchema, TableSchema};

use super::{DataManagerKind, EndianFormat, RowRange, Table, TableError, TableOptions};

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
    assert_eq!(table.rows(), &[first, second]);
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

    assert_eq!(table.row(0), Some(&first));
    assert_eq!(
        table.cell(1, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(2)))
    );

    table
        .set_cell(
            1,
            "name",
            Value::Scalar(ScalarValue::String("beta".to_string())),
        )
        .expect("set cell");
    assert_eq!(
        table.cell(1, "name"),
        Some(&Value::Scalar(ScalarValue::String("beta".to_string())))
    );

    let id_cells = table.column_cells("id");
    assert_eq!(
        id_cells,
        vec![
            Some(&Value::Scalar(ScalarValue::Int32(1))),
            Some(&Value::Scalar(ScalarValue::Int32(2))),
        ]
    );
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

    let cells: Vec<(usize, Option<Value>)> = table
        .get_column_range("id", RowRange::with_stride(1, 6, 2))
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

    let bad_stride = table.get_column_range("id", RowRange::with_stride(0, 1, 0));
    assert!(matches!(
        bad_stride,
        Err(TableError::InvalidRowStride { stride: 0 })
    ));

    let bad_end = table.get_column_range("id", RowRange::new(0, 2));
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

    let cells: Vec<(usize, RecordValue)> = table
        .get_record_column_range("meta", RowRange::new(0, 3))
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
        table.get_record_column("meta").map(|iter| iter.count()),
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
        table.get_record_column("meta").map(|iter| iter.count()),
        Err(TableError::ColumnTypeMismatch {
            row_index: 1,
            column: "meta".to_string(),
            expected: "record",
            found: casacore_types::ValueKind::Scalar,
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

    table
        .set_record_cell(0, "meta", payload.clone())
        .expect("set record cell");
    assert_eq!(table.record_cell(0, "meta"), Ok(payload));
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

    let written = table
        .put_column_range(
            "name",
            RowRange::with_stride(0, 4, 2),
            ["x", "y"]
                .into_iter()
                .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
        )
        .expect("put strided range");
    assert_eq!(written, 2);
    assert_eq!(
        table.cell(0, "name"),
        Some(&Value::Scalar(ScalarValue::String("x".to_string())))
    );
    assert_eq!(
        table.cell(1, "name"),
        Some(&Value::Scalar(ScalarValue::String("b".to_string())))
    );
    assert_eq!(
        table.cell(2, "name"),
        Some(&Value::Scalar(ScalarValue::String("y".to_string())))
    );
    assert_eq!(
        table.cell(3, "name"),
        Some(&Value::Scalar(ScalarValue::String("d".to_string())))
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

    let too_few = table.put_column_range(
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
    let too_many = table.put_column_range(
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
    let error = table.set_cell(0, "payload", Value::Array(ArrayValue::Int32(two_d)));
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
        reopened.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(42)))
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
        reopened.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(42)))
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
        reopened.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(42)))
    );
    assert_eq!(
        reopened.cell(0, "data"),
        Some(&Value::Array(ArrayValue::from_i32_vec(vec![7, 9])))
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

    let chunks: Vec<Vec<(usize, i32)>> = table
        .iter_column_chunks("id", RowRange::new(0, 7), 3)
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

    let chunks: Vec<Vec<usize>> = table
        .iter_column_chunks("id", RowRange::with_stride(0, 6, 2), 2)
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

    let borrowed = table.get_array_cell(0, "data").expect("get array cell");
    assert_eq!(borrowed, &array);
}

#[test]
fn get_array_cell_rejects_non_array() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(42)),
    )])]);

    assert!(matches!(
        table.get_array_cell(0, "id"),
        Err(TableError::ColumnTypeMismatch { .. })
    ));
}

#[test]
fn get_array_cell_rejects_missing() {
    let table = Table::from_rows(vec![RecordValue::new(vec![])]);

    assert!(matches!(
        table.get_array_cell(0, "data"),
        Err(TableError::ColumnNotFound { .. })
    ));
}

#[test]
fn get_scalar_cell_returns_borrow() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(42)),
    )])]);

    let borrowed = table.get_scalar_cell(0, "id").expect("get scalar cell");
    assert_eq!(borrowed, &ScalarValue::Int32(42));
}

#[test]
fn get_scalar_cell_rejects_non_scalar() {
    let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
        "data",
        Value::Array(ArrayValue::from_i32_vec(vec![1, 2])),
    )])]);

    assert!(matches!(
        table.get_scalar_cell(0, "data"),
        Err(TableError::ColumnTypeMismatch { .. })
    ));
}

fn unique_test_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("casacore_tables_{prefix}_{nanos}"))
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
        t.cell(0, "i32_col"),
        Some(&Value::Scalar(ScalarValue::Int32(42)))
    );
    assert_eq!(
        t.cell(0, "f64_col"),
        Some(&Value::Scalar(ScalarValue::Float64(2.78)))
    );
    assert_eq!(
        t.cell(0, "str_col"),
        Some(&Value::Scalar(ScalarValue::String("hello".into())))
    );
    assert_eq!(
        t.cell(1, "i32_col"),
        Some(&Value::Scalar(ScalarValue::Int32(-7)))
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
        let scan = reopened.get_scalar_cell(i, "SCAN_NUMBER").unwrap();
        assert_eq!(
            *scan,
            ScalarValue::Int32(expected_scan),
            "row {i} SCAN_NUMBER"
        );
        let flag = reopened.get_scalar_cell(i, "FLAG").unwrap();
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
            table.cell(i, "score"),
            Some(&Value::Scalar(ScalarValue::Float64(0.0)))
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
                reopened.cell(i, "score"),
                Some(&Value::Scalar(ScalarValue::Float64(99.5)))
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
        assert_eq!(table.cell(i, "opt"), None);
    }
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
        assert_eq!(table.cell(i, "name"), None);
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
            reopened.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
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
        assert!(table.cell(i, "label").is_some());
        assert_eq!(table.cell(i, "name"), None);
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
            reopened.cell(0, "label"),
            Some(&Value::Scalar(ScalarValue::String("row0".into())))
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
        table.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(0)))
    );
    assert_eq!(
        table.cell(1, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(2)))
    );
    assert_eq!(
        table.cell(2, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(4)))
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
            reopened.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert_eq!(
            reopened.cell(1, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(2)))
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
        table.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(0)))
    );
    assert_eq!(
        table.cell(1, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(99)))
    );
    assert_eq!(
        table.cell(2, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(1)))
    );
    assert_eq!(
        table.cell(3, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(2)))
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
        table.cell(3, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(99)))
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
    table
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("ALICE".into())),
        )
        .unwrap();
    assert_eq!(
        table.cell(0, "name"),
        Some(&Value::Scalar(ScalarValue::String("ALICE".into())))
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
        reopened.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(42)))
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

    let mem = plain.to_memory();
    assert!(mem.is_memory());
    assert!(mem.path().is_none());
    assert_eq!(mem.row_count(), 1);
    assert_eq!(
        mem.cell(0, "id"),
        Some(&Value::Scalar(ScalarValue::Int32(1)))
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
    let mem = disk.to_memory();
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
        &Value::Scalar(ScalarValue::Int32(1))
    );
    drop(sorted);

    // Select by predicate.
    let view = table.select(
        |row| matches!(row.get("id"), Some(Value::Scalar(ScalarValue::Int32(i))) if *i >= 2),
    );
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
        casacore_types::PrimitiveType::Float64,
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
        casacore_types::PrimitiveType::Float64,
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
        let val = reopened.cell(i, "value").unwrap();
        match val {
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
        ColumnSchema::scalar("stored_col", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("virtual_col", casacore_types::PrimitiveType::Float64),
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
        let val = reopened.cell(i, "virtual_col").unwrap();
        match val {
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
        casacore_types::PrimitiveType::Float64,
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
        ColumnSchema::scalar("stored_int", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("fwd_col", casacore_types::PrimitiveType::Float64),
        ColumnSchema::scalar("scaled_col", casacore_types::PrimitiveType::Float64),
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
    match reopened.cell(0, "stored_int").unwrap() {
        Value::Scalar(ScalarValue::Int32(v)) => assert_eq!(*v, 5),
        other => panic!("expected Int32(5), got {other:?}"),
    }

    // fwd_col should be 42.0 (from base table)
    match reopened.cell(0, "fwd_col").unwrap() {
        Value::Scalar(ScalarValue::Float64(v)) => {
            assert!((v - 42.0).abs() < 1e-10, "fwd_col: expected 42.0, got {v}");
        }
        other => panic!("expected Float64(42.0), got {other:?}"),
    }

    // scaled_col should be 5 * 3.0 + 1.0 = 16.0
    match reopened.cell(0, "scaled_col").unwrap() {
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
    });
    let mem = table.to_memory();
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
    use casacore_types::ArrayD;
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
    use casacore_types::ArrayD;
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

    match table.cell(0, "data").unwrap() {
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
    use casacore_types::ArrayD;
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
fn get_column_slice_multiple_rows() {
    use super::{RowRange, Slicer};
    use casacore_types::ArrayD;
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
    use casacore_types::ArrayD;
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
    match table.cell(0, "data").unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 11.0, 11.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
    // Row 1: untouched
    match table.cell(1, "data").unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 0.0, 0.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
    // Row 2: [0, 22, 22, 0]
    match table.cell(2, "data").unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 22.0, 22.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn put_column_slice_length_mismatch() {
    use super::{RowRange, Slicer};
    use casacore_types::ArrayD;
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
        dst.cell(2, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(2)))
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
        dst.cell(0, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(30)))
    );
    assert_eq!(
        dst.cell(1, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(10)))
    );
}

#[test]
fn copy_info_transfers_metadata() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut src = Table::with_schema(schema.clone());
    src.set_info(crate::TableInfo {
        table_type: "MeasurementSet".into(),
        sub_type: "".into(),
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
            table.cell(i, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(99)))
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
        table.cell(0, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(0)))
    );
    assert_eq!(
        table.cell(1, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(77)))
    );
    assert_eq!(
        table.cell(2, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(0)))
    );
    assert_eq!(
        table.cell(3, "x"),
        Some(&Value::Scalar(ScalarValue::Int32(77)))
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
