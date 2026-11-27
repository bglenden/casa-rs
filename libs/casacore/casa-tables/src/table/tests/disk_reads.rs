// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// The manager's name is what the table on disk says it is
/// (`DataManager::dataManagerName()`): for `StandardStMan` and
/// `IncrementalStMan` the name in the manager's blob of `table.dat`, which
/// casa-rs writes as the manager's type tag (casacore writes the group); for
/// `StManAipsIO` the name in the header of its file, which casa-rs writes
/// empty; for the others the group the columns were saved with.
#[test]
fn a_data_manager_info_names_the_manager_as_the_table_on_disk_does() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
    ])
    .expect("schema");
    for (kind, name) in [
        (DataManagerKind::StManAipsIO, ""),
        (DataManagerKind::StandardStMan, "SSM"),
        (DataManagerKind::IncrementalStMan, "ISM"),
    ] {
        let root = unique_test_dir(&format!("dm_name_{kind:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        let mut table = Table::with_schema(schema_for_manager(&schema, kind));
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool(true))),
            ]))
            .expect("push row");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(kind)
                    .with_data_manager_group("MyGroup"),
            )
            .expect("save");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        let managers = reopened.data_manager_info();
        assert!(!managers.is_empty(), "{kind:?}");
        for manager in managers {
            assert_eq!(manager.name, name, "{kind:?}: {manager:?}");
        }
        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}

/// A table that is one `StManAipsIO` group says how to write it in full as it
/// is laid out (group name and byte order kept); any other layout says
/// nothing, because a full save would change it.
#[test]
fn a_single_stman_aipsio_group_is_written_in_full_as_it_is_laid_out() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![2]),
    ])
    .expect("schema");
    let build = || {
        let mut table =
            Table::with_schema(schema_for_manager(&schema, DataManagerKind::StManAipsIO));
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0])),
                ),
            ]))
            .expect("push row");
        table
    };

    for format in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
        let root = unique_test_dir(&format!("single_group_{format:?}"));
        std::fs::create_dir_all(&root).expect("create test dir");
        build()
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::StManAipsIO)
                    .with_data_manager_group("MyGroup")
                    .with_endian_format(format),
            )
            .expect("save");
        let table = Table::open(TableOptions::new(&root)).expect("open");
        let options = table
            .single_stman_aipsio_options()
            .expect("options")
            .expect("one StManAipsIO group");
        assert_eq!(options.endian_format(), format);

        // Written in full with those options, the table is laid out the same.
        table.save(options).expect("full save");
        let again = Table::open(TableOptions::new(&root)).expect("reopen");
        let options = again
            .single_stman_aipsio_options()
            .expect("options")
            .expect("still one StManAipsIO group");
        assert_eq!(options.endian_format(), format);
        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }

    // Two managers: nothing to write it with.
    let root = unique_test_dir("two_managers");
    std::fs::create_dir_all(&root).expect("create test dir");
    let bindings = HashMap::from([(
        "data".to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::StandardStMan,
            tile_shape: None,
        },
    )]);
    build()
        .save_with_bindings(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO),
            &bindings,
        )
        .expect("save with a binding");
    let table = Table::open(TableOptions::new(&root)).expect("open");
    assert!(
        table
            .single_stman_aipsio_options()
            .expect("options")
            .is_none(),
        "a table of two managers must not be rewritten as one"
    );
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

/// A cell that was undefined on disk is defined once it is written, whether
/// or not the table's rows are loaded.
#[test]
fn a_written_cell_of_a_lazy_table_is_defined() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_variable("data", PrimitiveType::Int32, Some(1)),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema_for_manager(&schema, DataManagerKind::StManAipsIO));
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .expect("a row whose array cell is undefined");
    let root = unique_test_dir("lazy_undefined_cell");
    std::fs::create_dir_all(&root).expect("create test dir");
    table.save(TableOptions::new(&root)).expect("save");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    assert!(!reopened.is_cell_defined(0, "data").expect("before"));
    {
        let mut writer = reopened
            .row_accessor_mut()
            .prepare(&["data"])
            .expect("prepare");
        writer.seek(0).expect("seek");
        writer
            .set_value_at(0, Value::Array(ArrayValue::from_i32_vec(vec![5, 6])))
            .expect("write");
    }
    assert!(reopened.is_cell_defined(0, "data").expect("after"));
    assert_eq!(
        reopened.array_shape(0, "data").expect("shape"),
        Some(vec![2])
    );
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        table_write_scalar_bound(&mut reopened, 0, "id", ScalarValue::Int32(10))
            .expect("pending scalar update");
        table_write_array_bound(
            &mut reopened,
            0,
            "data",
            ArrayValue::from_i32_vec(vec![70, 90]),
        )
        .expect("pending array update");

        table_scalar(&reopened, 1, "id").expect("prime scalar cache");
        table_array(&reopened, 1, "data").expect("prime array cache");
        table_write_scalar_bound(&mut reopened, 1, "id", ScalarValue::Int32(20))
            .expect("cached scalar update");
        table_write_array_bound(
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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

        table_write_scalar_bound(&mut reopened, 1, "id", ScalarValue::Int32(22))
            .expect("set scalar cell lazily");
        table_write_array_bound(
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
            .persist_selected_columns_in_place(&["id", "data"])
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
fn incremental_selected_array_cell_writes_retain_at_most_one_cell_and_no_rows() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "MODEL_DATA",
        PrimitiveType::Complex32,
        vec![2, 2],
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for ordinal in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "MODEL_DATA",
                Value::Array(ArrayValue::Complex32(
                    ndarray::ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![Complex32::new(ordinal as f32, 0.0); 4],
                    )
                    .expect("2x2 cell"),
                )),
            )]))
            .expect("append cell");
    }
    let root = unique_test_dir("bounded_selected_model_cells");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy tiled table");
    for row in 0..4 {
        {
            let mut prepared = reopened
                .row_accessor_mut()
                .prepare(&["MODEL_DATA"])
                .expect("prepare one array column");
            prepared.seek(row).expect("select row without loading it");
            prepared
                .set_value_at(
                    0,
                    Value::Array(ArrayValue::Complex32(
                        ndarray::ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![Complex32::new(10.0 + row as f32, -1.0); 4],
                        )
                        .expect("replacement cell"),
                    )),
                )
                .expect("write one lazy cell");
        }
        assert!(!reopened.inner.has_loaded_rows());
        assert!(!reopened.inner.has_loaded_array_column("MODEL_DATA"));
        assert_eq!(
            reopened
                .inner
                .pending_array_cells("MODEL_DATA")
                .expect("one pending cell")
                .len(),
            1
        );
        reopened
            .prepare_write()
            .save_selected_rows(&["MODEL_DATA"], &[row])
            .expect("persist one row");
        reopened.discard_persisted_cell_updates(&["MODEL_DATA"], &[row]);
        assert!(!reopened.inner.has_pending_array_cells("MODEL_DATA"));
        assert!(!reopened.inner.has_loaded_rows());
    }

    let verify = Table::open(TableOptions::new(&root)).expect("verify bounded writes");
    assert!(!verify.inner.has_loaded_rows());
    for row in 0..4 {
        let ArrayValue::Complex32(values) = table_array(&verify, row, "MODEL_DATA").unwrap() else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(values[[0, 0]], Complex32::new(10.0 + row as f32, -1.0));
    }
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn adding_a_streamed_tiled_shape_rejects_incompatible_type_and_rank() {
    use crate::streamed_tiled::{
        StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType, StreamingTiledShapeWriter,
    };

    let scratch = tempfile::tempdir().expect("temporary directory");
    let root = scratch.path().join("table");
    let schema = TableSchema::new(vec![ColumnSchema::scalar("ID", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "ID",
            Value::Scalar(ScalarValue::Int32(0)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&root)).unwrap();
    let mut table = Table::open(TableOptions::new(&root)).unwrap();
    table
        .add_column(
            ColumnSchema::array_variable("SPECTRUM", PrimitiveType::Float32, Some(2)),
            None,
        )
        .unwrap();
    for (shape, kind) in [
        (vec![2, 2], StreamedTiledShapeValueType::Float64),
        (vec![4], StreamedTiledShapeValueType::Float32),
    ] {
        let mut tile = shape.clone();
        tile.push(1);
        let mut writer = StreamingTiledShapeWriter::create(
            scratch.path().join("stream"),
            kind,
            vec![StreamedTiledShapeCubeLayout::new(shape.clone(), 1, tile)],
            4096,
            true,
        )
        .unwrap();
        match kind {
            StreamedTiledShapeValueType::Float64 => writer.push_f64_row(&shape, &[1.0; 4]).unwrap(),
            _ => writer.push_f32_row(&shape, &[1.0; 4]).unwrap(),
        }
        let result = table
            .prepare_write()
            .add_streamed_tiled_shape_column("SPECTRUM", writer.finish().unwrap());
        assert!(
            matches!(result, Err(TableError::Storage(message)) if message.contains("type or rank"))
        );
        let reopened = Table::open(TableOptions::new(&root)).unwrap();
        assert!(!reopened.schema().unwrap().contains_column("SPECTRUM"));
    }
}

/// A selected-row save whose rows span the hypercubes of a heterogeneous
/// `TiledShapeStMan` patches each row into its own cube.
#[test]
fn a_selected_row_save_across_tiled_shape_cubes_patches_each_row_in_its_cube() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "MODEL_DATA",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .expect("schema");
    let shape = |row: usize| {
        if row.is_multiple_of(2) {
            vec![2, 2]
        } else {
            vec![2, 3]
        }
    };
    let cell = |row: usize, value: f32| {
        let shape = shape(row);
        let values = shape.iter().product();
        Value::Array(ArrayValue::Complex32(
            ndarray::ArrayD::from_shape_vec(shape, vec![Complex32::new(value, 0.0); values])
                .expect("cell"),
        ))
    };
    let mut table = Table::with_schema(schema);
    for row in 0..4 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "MODEL_DATA",
                cell(row, row as f32),
            )]))
            .expect("append cell");
    }
    let root = unique_test_dir("selected_rows_across_tiled_shape_cubes");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(TableOptions::new(&root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save tiled table");

    let mut reopened = Table::open(TableOptions::new(&root)).expect("open lazy tiled table");
    for row in 0..4 {
        let mut prepared = reopened
            .row_accessor_mut()
            .prepare(&["MODEL_DATA"])
            .expect("prepare one array column");
        prepared.seek(row).expect("select row without loading it");
        prepared
            .set_value_at(0, cell(row, 10.0 + row as f32))
            .expect("write one lazy cell");
    }
    assert!(!reopened.inner.has_loaded_rows());
    reopened
        .prepare_write()
        .save_selected_rows(&["MODEL_DATA"], &[0, 1, 2, 3])
        .expect("persist rows of both cubes");

    let verify = Table::open(TableOptions::new(&root)).expect("verify the save");
    for row in 0..4 {
        let ArrayValue::Complex32(values) = table_array(&verify, row, "MODEL_DATA").unwrap() else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(values.shape(), shape(row).as_slice());
        assert!(
            values
                .iter()
                .all(|value| *value == Complex32::new(10.0 + row as f32, 0.0))
        );
    }
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn selected_row_reads_reuse_open_control_metadata_without_retaining_payloads() {
    use crate::storage::table_control::CONTROL_FILE_READS;

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("other", PrimitiveType::Int32, vec![2]),
    ])
    .unwrap();
    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let root = unique_test_dir(&format!("selected_rows_retained_metadata_{dm:?}"));
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
        for row in 0..6 {
            table
                .add_row(row_with_fixed_arrays(row, &[row, row + 10], &[0, 1]))
                .unwrap();
        }
        table
            .save(TableOptions::new(&root).with_data_manager(dm))
            .unwrap();
        let reopened = Table::open(TableOptions::new(&root)).unwrap();
        let metadata_bytes = reopened.retained_read_metadata_bytes().unwrap();
        let initial_reads = CONTROL_FILE_READS.get();
        for rows in [&[5, 2, 5][..], &[0][..], &[][..]] {
            let arrays = reopened
                .column_accessor("data")
                .unwrap()
                .array_cells_owned_uncached(rows)
                .unwrap();
            let scalars = reopened
                .column_accessor("id")
                .unwrap()
                .scalar_cells_owned_for_rows(rows)
                .unwrap();
            assert_eq!(
                arrays,
                rows.iter()
                    .map(|&row| Some(ArrayValue::from_i32_vec(vec![row as i32, row as i32 + 10])))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                scalars,
                rows.iter()
                    .map(|&row| Some(ScalarValue::Int32(row as i32)))
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(CONTROL_FILE_READS.get(), initial_reads);
        assert_eq!(
            reopened.retained_read_metadata_bytes(),
            Some(metadata_bytes)
        );
        std::fs::remove_dir_all(&root).unwrap();
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
fn typed_selected_reads_cover_every_casacore_array_manager() {
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

    for manager in [
        DataManagerKind::TiledColumnStMan,
        DataManagerKind::StandardStMan,
        DataManagerKind::StManAipsIO,
    ] {
        let root = unique_test_dir("typed_selected_reads_every_manager");
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(manager))
            .expect("save table");
        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        let column = reopened.column_accessor("data").expect("data accessor");

        let mut row_channel = Vec::new();
        let shape = column
            .fill_array_cells_2d_channel_range_typed_uncached(
                &[7, 2],
                1,
                3,
                SelectedArray2DCellsMut::RowChannelFloat32(&mut row_channel),
            )
            .unwrap_or_else(|error| panic!("{manager:?} typed 2-D fill: {error}"))
            .expect("defined selected cells");
        assert_eq!(
            (shape.row_count, shape.axis0_count, shape.channel_count),
            (2, 2, 3),
            "{manager:?}"
        );
        assert_eq!(
            row_channel,
            [
                710.0, 711.0, 720.0, 721.0, 730.0, 731.0, 210.0, 211.0, 220.0, 221.0, 230.0, 231.0,
            ],
            "{manager:?}"
        );
        let SelectedArray2DCells::Float32(channel_row) = column
            .array_cells_2d_channel_range_typed_uncached(&[7, 2], 1, 3)
            .expect("typed selected channel ranges")
            .expect("defined selected cells")
        else {
            panic!("{manager:?}: expected Float32 cells");
        };
        assert_eq!(
            channel_row.values(),
            &[
                710.0, 711.0, 210.0, 211.0, 720.0, 721.0, 220.0, 221.0, 730.0, 731.0, 230.0, 231.0,
            ],
            "{manager:?}"
        );
        assert!(
            !reopened.inner.has_loaded_array_column("data"),
            "{manager:?}: selected reads must not populate the full array-column cache"
        );
        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
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
    assert!(reopened.inner.has_cached_control_metadata());
    assert_eq!(reopened.inner.cached_tiled_header_count(), 1);
    let control_metadata_bytes = reopened.inner.cached_control_metadata_heap_bytes();
    let tiled_header_bytes = reopened.inner.cached_tiled_header_heap_bytes();
    assert!(control_metadata_bytes > 0);
    assert!(tiled_header_bytes > 0);
    let retained_metadata_bytes = reopened
        .retained_read_metadata_bytes()
        .expect("fresh lazy table has fully modeled retained metadata");
    assert!(
        retained_metadata_bytes >= control_metadata_bytes + tiled_header_bytes,
        "table metadata charge must include parsed control and tiled-manager metadata"
    );
    let tile_cache_budget = crate::table_cache_budget_bytes();

    let typed = reopened
        .column_accessor("data")
        .expect("data accessor")
        .array_cells_2d_channel_range_typed_uncached(&[7, 2], 1, 3)
        .expect("typed selected channel ranges")
        .expect("defined selected cells");
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
    assert!(reopened.inner.has_cached_control_metadata());
    assert_eq!(reopened.inner.cached_tiled_header_count(), 1);
    let repeated = reopened
        .column_accessor("data")
        .expect("data accessor")
        .array_cells_2d_channel_range_typed_uncached(&[7, 2], 1, 3)
        .expect("repeat typed selected channel ranges")
        .expect("defined selected cells");
    let SelectedArray2DCells::Float32(repeated) = repeated else {
        panic!("expected Float32 typed selected cells");
    };
    assert_eq!(repeated.values(), typed.values());
    assert_eq!(reopened.inner.cached_tiled_header_count(), 1);
    assert_eq!(
        reopened.retained_read_metadata_bytes(),
        Some(retained_metadata_bytes),
        "typed reads must not grow retained table metadata after admission"
    );
    assert_eq!(crate::table_cache_budget_bytes(), tile_cache_budget);
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

    for dm in [
        DataManagerKind::StManAipsIO,
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        let mut table = Table::with_schema(schema_for_manager(&schema, dm));
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
