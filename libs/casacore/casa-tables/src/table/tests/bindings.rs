// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
        .write_array_cell_bound(
            2,
            "data",
            ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![90.0, 91.0, 92.0, 93.0]).unwrap(),
            ),
        )
        .expect("mutate cached row");
    reopened
        .persist_selected_rows_in_place(&["data"], &[2])
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
    table_write_scalar_bound(&mut reopened, 1, "flag_row", ScalarValue::Bool(true))
        .expect("set flag_row");
    table_write_array_bound(
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
        .persist_selected_rows_in_place(&["flag_row", "data"], &[1])
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
    table_write_array_bound(
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
        .persist_selected_rows_in_place(&["data"], &[2])
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

    table_write_array_bound(
        &mut reopened,
        1,
        "DATA",
        ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 2], vec![201.0, 211.0, 221.0, 231.0])
                .expect("shape updated DATA"),
        ),
    )
    .expect("set DATA lazily");
    table_write_array_bound(
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
        .persist_selected_rows_in_place(&["DATA", "WEIGHT"], &[1, 2])
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
    table_write_array_bound(
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
        .persist_selected_rows_in_place(&["data"], &[2])
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
    table_write_scalar_bound(&mut reopened, 2, "id", ScalarValue::Int32(2002))
        .expect("set row 2 id");
    table_write_scalar_bound(&mut reopened, 41, "scan", ScalarValue::Int32(9041))
        .expect("set row 41 scan");
    reopened
        .persist_selected_rows_in_place(&["id", "scan"], &[2, 41])
        .expect("sparse incremental partial save");
    reopened.discard_persisted_cell_updates(&["id", "scan"], &[2, 41]);
    assert_eq!(
        reopened
            .column_accessor("id")
            .unwrap()
            .scalar_cells_owned_for_rows(&[2, 41, 2])
            .unwrap(),
        vec![
            Some(ScalarValue::Int32(2002)),
            Some(ScalarValue::Int32(41)),
            Some(ScalarValue::Int32(2002))
        ]
    );
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
fn partial_save_with_changed_rows_patches_only_touched_standard_rows() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flag_row", PrimitiveType::Bool),
        ColumnSchema::scalar("time", PrimitiveType::Float64),
    ])
    .expect("schema");

    let mut table = Table::with_schema(schema);
    for row_idx in 0..10_000 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(row_idx))),
                RecordField::new(
                    "flag_row",
                    Value::Scalar(ScalarValue::Bool(row_idx % 17 == 0)),
                ),
                RecordField::new(
                    "time",
                    Value::Scalar(ScalarValue::Float64(row_idx as f64 * 0.5)),
                ),
            ]))
            .expect("push row");
    }

    let root = unique_test_dir("partial_save_changed_rows_standard");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save standard table");
    let data_len = std::fs::metadata(root.join("table.f0"))
        .expect("standard data file")
        .len();

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open standard table");
    table_write_scalar_bound(&mut reopened, 7, "flag_row", ScalarValue::Bool(true))
        .expect("set bit before byte boundary");
    table_write_scalar_bound(&mut reopened, 8, "flag_row", ScalarValue::Bool(true))
        .expect("set bit after byte boundary");
    table_write_scalar_bound(&mut reopened, 9_001, "id", ScalarValue::Int32(42))
        .expect("set distant id");
    reopened
        .persist_selected_rows_in_place(&["flag_row", "id"], &[7, 8, 9_001])
        .expect("sparse standard partial save");
    assert!(!reopened.inner.has_loaded_rows());
    assert!(!reopened.inner.has_loaded_scalar_column("flag_row"));
    assert!(!reopened.inner.has_loaded_scalar_column("id"));
    assert_eq!(
        std::fs::metadata(root.join("table.f0"))
            .expect("standard data file after patch")
            .len(),
        data_len,
        "sparse patches must preserve the existing SSM layout"
    );

    let verify = Table::open(TableOptions::new(&root)).expect("reopen after sparse standard save");
    assert_eq!(
        table_scalar(&verify, 6, "flag_row").expect("neighbor flag"),
        &ScalarValue::Bool(false)
    );
    assert_eq!(
        table_scalar(&verify, 7, "flag_row").expect("row 7 flag"),
        &ScalarValue::Bool(true)
    );
    assert_eq!(
        table_scalar(&verify, 8, "flag_row").expect("row 8 flag"),
        &ScalarValue::Bool(true)
    );
    assert_eq!(
        table_scalar(&verify, 9, "flag_row").expect("neighbor flag"),
        &ScalarValue::Bool(false)
    );
    assert_eq!(
        table_scalar(&verify, 9_000, "id").expect("neighbor id"),
        &ScalarValue::Int32(9_000)
    );
    assert_eq!(
        table_scalar(&verify, 9_001, "id").expect("updated id"),
        &ScalarValue::Int32(42)
    );
    assert_eq!(
        table_scalar(&verify, 9_001, "time").expect("untouched time"),
        &ScalarValue::Float64(4_500.5)
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

pub(super) fn unique_test_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("casa_tables_{prefix}_{nanos}"))
}

/// Build a small multi-type table for endian round-trip tests.
pub(super) fn build_endian_test_table() -> Table {
    endian_test_table(false)
}

/// The endian test table with a Direct array column, which IncrementalStMan
/// stores in its buckets.
pub(super) fn build_direct_endian_test_table() -> Table {
    endian_test_table(true)
}

fn endian_test_table(direct_array: bool) -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("i32_col", PrimitiveType::Int32),
        ColumnSchema::scalar("f64_col", PrimitiveType::Float64),
        ColumnSchema::scalar("str_col", PrimitiveType::String),
        ColumnSchema::array_fixed("arr_col", PrimitiveType::Float32, vec![3])
            .with_options(crate::schema::ColumnOptions {
                direct: direct_array,
                undefined: false,
            })
            .expect("array options"),
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
pub(super) fn verify_endian_test_table(t: &Table) {
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
        .prepare_write()
        .save_with_column_overrides(
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
