// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn save_with_bindings_generated_scalar_overrides_define_row_count() {
    let row_count = 4096usize;
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ROW_ID", PrimitiveType::Int32),
        ColumnSchema::scalar("SCAN", PrimitiveType::Int32),
        ColumnSchema::scalar("FLAG_ROW", PrimitiveType::Bool),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);

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
        .prepare_write()
        .save_with_column_overrides(
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
fn streamed_scalar_overrides_round_trip_standard_and_incremental_managers() {
    let row_count = 10_000usize;
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ANTENNA", PrimitiveType::Int32),
        ColumnSchema::scalar("FLAG_ROW", PrimitiveType::Bool),
        ColumnSchema::scalar("SCAN", PrimitiveType::Int32),
        ColumnSchema::scalar("TIME", PrimitiveType::Float64),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    let root = unique_test_dir("streamed_scalar_overrides_round_trip");
    std::fs::create_dir_all(&root).expect("mkdir");

    let mut antenna = StreamingScalarColumnWriter::create(
        root.join(".antenna.scalar.tmp"),
        row_count,
        StreamedScalarType::Int32,
    )
    .expect("antenna writer");
    let mut flag_row = StreamingScalarColumnWriter::create(
        root.join(".flag.scalar.tmp"),
        row_count,
        StreamedScalarType::Bool,
    )
    .expect("flag writer");
    let mut scan = StreamingScalarColumnWriter::create(
        root.join(".scan.scalar.tmp"),
        row_count,
        StreamedScalarType::Int32,
    )
    .expect("scan writer");
    let mut time = StreamingScalarColumnWriter::create(
        root.join(".time.scalar.tmp"),
        row_count,
        StreamedScalarType::Float64,
    )
    .expect("time writer");
    for row in 0..row_count {
        antenna
            .push(Some(ScalarValue::Int32((row % 27) as i32)))
            .expect("antenna value");
        flag_row
            .push(Some(ScalarValue::Bool(row % 101 == 0)))
            .expect("flag value");
        scan.push(Some(ScalarValue::Int32((row / 351) as i32)))
            .expect("scan value");
        time.push(Some(ScalarValue::Float64(row as f64 * 0.5)))
            .expect("time value");
    }

    let mut overrides = ColumnOverrides::for_row_count(row_count);
    overrides.insert_streamed_scalar("ANTENNA", antenna.finish().expect("finish antenna"));
    overrides.insert_streamed_scalar("FLAG_ROW", flag_row.finish().expect("finish flag"));
    overrides.insert_streamed_scalar("SCAN", scan.finish().expect("finish scan"));
    overrides.insert_streamed_scalar("TIME", time.finish().expect("finish time"));
    let bindings = HashMap::from([
        (
            "ANTENNA".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::StandardStMan,
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
    table
        .prepare_write()
        .save_with_column_overrides(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
            &overrides,
        )
        .expect("save streamed scalar overrides");

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    assert_eq!(reopened.row_count(), row_count);
    for row in [0, 100, 350, 351, 8191, 9999] {
        assert_eq!(
            table_scalar(&reopened, row, "ANTENNA").expect("antenna"),
            &ScalarValue::Int32((row % 27) as i32)
        );
        assert_eq!(
            table_scalar(&reopened, row, "FLAG_ROW").expect("flag row"),
            &ScalarValue::Bool(row % 101 == 0)
        );
        assert_eq!(
            table_scalar(&reopened, row, "SCAN").expect("scan"),
            &ScalarValue::Int32((row / 351) as i32)
        );
        assert_eq!(
            table_scalar(&reopened, row, "TIME").expect("time"),
            &ScalarValue::Float64(row as f64 * 0.5)
        );
    }

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
    let mut table = Table::with_schema(schema);

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
        .prepare_write()
        .save_with_column_overrides(
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
    let table = build_direct_endian_test_table();
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
    let table = build_direct_endian_test_table();
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
    for (row, shape, values) in [
        (1, vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]),
        (2, vec![1, 3], vec![5.0, 6.0, 7.0]),
    ] {
        let mut prepared = table.row_accessor_mut().prepare(&["vis"]).unwrap();
        prepared.seek(row).unwrap();
        prepared
            .set_value_at(
                0,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, values).unwrap(),
                )),
            )
            .unwrap();
    }
    table
        .prepare_write()
        .add_tiled_shape_column("vis", &[1, 2], Some(&[2, 2, 8]))
        .expect("save added tiled column");
    table.discard_persisted_cell_updates(&["vis"], &[1, 2]);
    let same_handle = table.column_accessor("vis").unwrap();
    for (row, expected) in [false, true, true].into_iter().enumerate() {
        assert_eq!(
            same_handle.array_cell_is_defined_uncached(row).unwrap(),
            expected
        );
    }
    assert_eq!(
        same_handle.array_cells_owned_uncached(&[2, 1]).unwrap(),
        vec![
            Some(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![1, 3], vec![5.0, 6.0, 7.0]).unwrap()
            )),
            Some(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).unwrap()
            )),
        ]
    );
    assert!(!table.inner.has_loaded_rows());

    let reopened = Table::open(TableOptions::new(&root)).expect("reopen table");
    let vis = reopened.column_accessor("vis").expect("vis accessor");
    for (row, expected) in [false, true, true].into_iter().enumerate() {
        assert_eq!(
            vis.array_cell_is_defined_uncached(row)
                .expect("metadata-only definedness"),
            expected
        );
    }
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_loaded_array_column("vis"));
    assert_eq!(reopened.inner.cached_tiled_header_count(), 1);

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
fn packed_tiled_cells_preserve_order_and_refuse_shape_changes() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DATA",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for (row, channels) in [3, 4, 3].into_iter().enumerate() {
        let data = ArrayD::from_shape_fn(vec![2, channels], |index| {
            casa_core::Complex32::new(row as f32, (index[0] + 2 * index[1]) as f32)
        });
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Complex32(data)),
            )]))
            .unwrap();
    }
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("packed.tab");
    let options = TableOptions::new(&path).with_data_manager(DataManagerKind::TiledShapeStMan);
    table.save(options).unwrap();
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    let column = reopened.column_accessor("DATA").unwrap();
    let Some(ArrayValue::Complex32(packed)) = column.packed_2d_cells_uncached(&[2, 0, 2]).unwrap()
    else {
        panic!("expected a packed tiled read");
    };
    assert_eq!(packed.shape(), &[2, 3, 3]);
    for (index, row) in [2, 0, 2].into_iter().enumerate() {
        for channel in 0..3 {
            for corr in 0..2 {
                assert_eq!(
                    packed[[corr, channel, index]],
                    casa_core::Complex32::new(row as f32, (corr + 2 * channel) as f32)
                );
            }
        }
    }
    assert!(column.packed_2d_cells_uncached(&[0, 1]).unwrap().is_none());
    let Some(ArrayValue::Complex64(widened)) = column
        .packed_2d_cells_uncached_with_complex_promotion(&[2, 0, 2])
        .unwrap()
    else {
        panic!("expected a packed complex128 read");
    };
    assert_eq!(widened.shape(), &[2, 3, 3]);
    for (offset, row) in [2, 0, 2].into_iter().enumerate() {
        for channel in 0..3 {
            for corr in 0..2 {
                assert_eq!(
                    widened[[corr, channel, offset]],
                    casa_core::Complex64::new(row as f64, (corr + 2 * channel) as f64)
                );
            }
        }
    }
    assert!(
        column
            .packed_2d_cells_uncached_with_complex_promotion(&[0, 1])
            .unwrap()
            .is_none()
    );
    assert!(!reopened.inner.has_loaded_rows());
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
                            casa_core::Complex32::new(row_idx as f32, 0.0),
                            casa_core::Complex32::new(row_idx as f32, 1.0),
                            casa_core::Complex32::new(row_idx as f32, 2.0),
                            casa_core::Complex32::new(row_idx as f32, 3.0),
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
        .prepare_write()
        .add_tiled_column_clone("DATA", "CORRECTED_DATA", "TiledCorrected")
        .expect("clone DATA to CORRECTED_DATA");
    let control_reads = crate::storage::table_control::CONTROL_FILE_READS.get();
    let cloned = reopened
        .column_accessor("CORRECTED_DATA")
        .unwrap()
        .array_cells_owned_uncached(&[3, 0, 3])
        .expect("read cloned column through the same lazy handle");
    let source = reopened
        .column_accessor("DATA")
        .unwrap()
        .array_cells_owned_uncached(&[3, 0, 3])
        .unwrap();
    assert_eq!(cloned, source);
    assert_eq!(
        crate::storage::table_control::CONTROL_FILE_READS.get(),
        control_reads
    );
    assert!(!reopened.inner.has_loaded_rows());
    table_set_cell(
        &mut reopened,
        2,
        "CORRECTED_DATA",
        Value::Array(ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2, 2],
                vec![
                    casa_core::Complex32::new(20.0, 0.0),
                    casa_core::Complex32::new(20.0, 1.0),
                    casa_core::Complex32::new(20.0, 2.0),
                    casa_core::Complex32::new(20.0, 3.0),
                ],
            )
            .expect("shape corrected"),
        )),
    )
    .expect("patch row");
    reopened
        .persist_selected_rows_in_place(&["CORRECTED_DATA"], &[2])
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
                    casa_core::Complex32::new(20.0, 0.0),
                    casa_core::Complex32::new(20.0, 1.0),
                    casa_core::Complex32::new(20.0, 2.0),
                    casa_core::Complex32::new(20.0, 3.0),
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
