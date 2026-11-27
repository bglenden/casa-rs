// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
fn prepared_appender_rejects_invalid_rows_before_mutation() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![2]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    let mut appender = table.prepare_row_appender().expect("prepare appender");

    let missing = appender.append(RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(1)),
    )]));
    assert!(matches!(
        missing,
        Err(TableError::SchemaColumnMissing { .. })
    ));
    assert_eq!(appender.row_count(), 0);

    let wrong_primitive = appender.append(RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Float64(1.0))),
        RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0])),
        ),
    ]));
    assert!(matches!(
        wrong_primitive,
        Err(TableError::PrimitiveTypeMismatch { .. })
    ));
    assert_eq!(appender.row_count(), 0);

    let wrong_shape = appender.append(RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new("data", Value::Array(ArrayValue::from_f32_vec(vec![1.0]))),
    ]));
    assert!(matches!(
        wrong_shape,
        Err(TableError::ArrayShapeMismatch { .. })
    ));
    assert_eq!(appender.row_count(), 0);

    let extra = appender.append(RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0])),
        ),
        RecordField::new("extra", Value::Scalar(ScalarValue::Bool(true))),
    ]));
    assert!(matches!(
        extra,
        Err(TableError::RowContainsUnknownColumn { .. })
    ));
    assert_eq!(appender.row_count(), 0);
}

#[test]
fn prepared_mutable_row_rejects_value_before_sparse_update() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![2]),
        ColumnSchema::array_variable("payload", PrimitiveType::Int32, Some(2)),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new(
                "data",
                Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0])),
            ),
            RecordField::new(
                "payload",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![1, 2], vec![1, 2]).expect("payload shape"),
                )),
            ),
        ]))
        .expect("row");

    let mut row = table
        .row_accessor_mut()
        .prepare(&["id", "data", "payload"])
        .expect("prepare row");
    row.seek(0).expect("seek row");
    assert!(matches!(
        row.set_value_at(0, Value::Scalar(ScalarValue::Float64(7.0))),
        Err(TableError::PrimitiveTypeMismatch { .. })
    ));
    assert!(matches!(
        row.set_value_at(1, Value::Array(ArrayValue::from_f32_vec(vec![3.0]))),
        Err(TableError::ArrayShapeMismatch { .. })
    ));
    assert!(matches!(
        row.set_value_at(2, Value::Array(ArrayValue::from_i32_vec(vec![3, 4]))),
        Err(TableError::ArrayNdimMismatch { .. })
    ));
    assert_eq!(
        table_scalar(&table, 0, "id").expect("unchanged id"),
        &ScalarValue::Int32(7)
    );
}

#[test]
fn write_plan_validates_scope_and_can_retry_after_io_failure() {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .expect("row");

    let base = unique_test_dir("write_plan_retry");
    std::fs::create_dir_all(&base).expect("create base");
    let blocker = base.join("not_a_directory");
    std::fs::write(&blocker, b"block").expect("create blocker");
    let valid = base.join("valid");

    {
        let mut plan = table.prepare_write();
        assert!(matches!(
            plan.save_selected_rows(&["id", "id"], &[0]),
            Err(TableError::Storage(_))
        ));
        assert!(matches!(
            plan.save_selected_rows(&["id"], &[1]),
            Err(TableError::RowOutOfBounds { .. })
        ));
        assert!(plan.save(TableOptions::new(blocker.join("table"))).is_err());
        plan.save(TableOptions::new(&valid))
            .expect("retry same plan");
    }

    let reopened = Table::open(TableOptions::new(&valid)).expect("open retried save");
    assert_eq!(
        table_scalar(&reopened, 0, "id").expect("saved id"),
        &ScalarValue::Int32(7)
    );
}

#[test]
fn write_plan_rejects_invalid_override_before_io() {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .expect("row");
    let mut overrides = ColumnOverrides::for_row_count(1);
    overrides.insert_values("id", vec![Some(Value::Scalar(ScalarValue::Float64(7.0)))]);
    let destination = unique_test_dir("invalid_write_override");

    let result = table.prepare_write().save_with_column_overrides(
        TableOptions::new(&destination),
        &HashMap::new(),
        &overrides,
    );
    assert!(matches!(
        result,
        Err(TableError::PrimitiveTypeMismatch { .. })
    ));
    assert!(!destination.exists());
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
            .persist_selected_rows_in_place(&["id", "data"], &[0, 1])
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
            .persist_selected_rows_in_place(&["id", "data"], &[0, 1])
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

/// A cell written to a table whose rows are not loaded stays written when
/// the rows are later loaded for reading or for a full save, whether it was
/// written before or after another cell of its column was read, and a cell
/// of a later write is not undone by the one before it. The table's other
/// rows stay on disk while only cells are written and read.
#[test]
fn cells_written_to_a_lazy_table_survive_the_loading_of_its_rows() {
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
        for row in 0..4 {
            table
                .add_row(row_with_fixed_arrays(
                    row,
                    &[row, row + 1],
                    &[100 + row, 200 + row],
                ))
                .expect("push row");
        }
        let root = unique_test_dir(&format!("lazy_cell_writes_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save disk-backed table");

        let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        // An array cell is written and its shape asked, and the scalar column
        // read before one of its cells is written: no row is loaded.
        {
            let mut writer = reopened
                .row_accessor_mut()
                .prepare(&["id", "data"])
                .expect("prepare");
            writer.seek(1).expect("seek");
            writer
                .set_value_at(1, Value::Array(ArrayValue::from_i32_vec(vec![70, 90])))
                .expect("array write");
        }
        assert!(
            !reopened.inner.has_loaded_rows(),
            "after the array write {dm:?}"
        );
        assert_eq!(
            reopened.array_shape(1, "data").expect("shape"),
            Some(vec![2])
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "after array_shape {dm:?}"
        );
        assert_eq!(
            table_scalar(&reopened, 2, "id").expect("read caches the column"),
            &ScalarValue::Int32(2)
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "after the scalar read {dm:?}"
        );
        {
            let mut writer = reopened
                .row_accessor_mut()
                .prepare(&["id"])
                .expect("prepare");
            writer.seek(3).expect("seek");
            writer
                .set_value_at(0, Value::Scalar(ScalarValue::Int32(33)))
                .expect("scalar write");
        }
        assert!(
            !reopened.inner.has_loaded_rows(),
            "cell writes and reads load no rows for {dm:?}"
        );

        // The rows loaded for reading carry both writes.
        assert_eq!(
            reopened.row(1).expect("row 1").get("data"),
            Some(&Value::Array(ArrayValue::from_i32_vec(vec![70, 90]))),
            "{dm:?}"
        );
        assert_eq!(
            reopened.row(3).expect("row 3").get("id"),
            Some(&Value::Scalar(ScalarValue::Int32(33))),
            "{dm:?}"
        );

        // A later write is not undone by the earlier one, and a full save
        // writes them all.
        {
            let mut writer = reopened
                .row_accessor_mut()
                .prepare(&["data"])
                .expect("prepare");
            writer.seek(1).expect("seek");
            writer
                .set_value_at(0, Value::Array(ArrayValue::from_i32_vec(vec![71, 91])))
                .expect("later write");
        }
        let copy = unique_test_dir(&format!("lazy_cell_writes_copy_{dm:?}"));
        reopened
            .save(TableOptions::new(&copy).with_data_manager(dm))
            .expect("full save");
        let verify = Table::open(TableOptions::new(&copy)).expect("reopen the copy");
        assert_eq!(
            table_array(&verify, 1, "data").expect("data row 1"),
            &ArrayValue::from_i32_vec(vec![71, 91]),
            "{dm:?}"
        );
        assert_eq!(
            table_scalar(&verify, 3, "id").expect("id row 3"),
            &ScalarValue::Int32(33),
            "{dm:?}"
        );
        assert_eq!(
            table_array(&verify, 2, "data").expect("data row 2"),
            &ArrayValue::from_i32_vec(vec![2, 3]),
            "{dm:?}"
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
        std::fs::remove_dir_all(&copy).expect("cleanup copy");
    }
}

/// A slice of an array cell is read and written without loading the rows of
/// a table read lazily (a MeasurementSet's visibilities would not fit in
/// memory for one cell's slice), and the written slice is what the disk has
/// after an in-place save of the row.
#[test]
fn cell_slices_of_a_lazy_table_load_no_rows() {
    use super::super::Slicer;

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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
        for row in 0..3 {
            table
                .add_row(row_with_fixed_arrays(
                    row,
                    &[10 * row, 10 * row + 1],
                    &[0, 0],
                ))
                .expect("push row");
        }
        let root = unique_test_dir(&format!("lazy_cell_slices_{dm:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .expect("save");

        let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        let second = Slicer::contiguous(vec![1], vec![2]).expect("slicer");
        assert_eq!(
            reopened.get_cell_slice("data", 1, &second).expect("read"),
            Value::Array(ArrayValue::from_i32_vec(vec![11])),
            "{dm:?}"
        );
        reopened
            .put_cell_slice("data", 1, &second, &ArrayValue::from_i32_vec(vec![99]))
            .expect("write");
        assert_eq!(
            reopened.get_cell_slice("data", 1, &second).expect("reread"),
            Value::Array(ArrayValue::from_i32_vec(vec![99])),
            "{dm:?}"
        );
        assert!(
            !reopened.inner.has_loaded_rows(),
            "a slice read and written loads no rows for {dm:?}"
        );
        reopened
            .persist_selected_rows_in_place(&["data"], &[1])
            .expect("save the row");

        let verify = Table::open(TableOptions::new(&root)).expect("reopen");
        assert_eq!(
            table_array(&verify, 1, "data").expect("row 1"),
            &ArrayValue::from_i32_vec(vec![10, 99]),
            "{dm:?}"
        );
        assert_eq!(
            table_array(&verify, 2, "data").expect("row 2"),
            &ArrayValue::from_i32_vec(vec![20, 21]),
            "{dm:?}"
        );
        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}
