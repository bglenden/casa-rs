// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
