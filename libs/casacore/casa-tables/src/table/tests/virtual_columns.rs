// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn forward_column_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base_table");
    let fwd_path = dir.path().join("fwd_table");

    // Create and save a base table with some data.
    let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "value",
        casa_core::PrimitiveType::Float64,
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
        casa_core::PrimitiveType::Float64,
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
        ColumnSchema::scalar("stored_col", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("virtual_col", casa_core::PrimitiveType::Float64),
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
        casa_core::PrimitiveType::Float64,
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
        ColumnSchema::scalar("stored_int", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("fwd_col", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("scaled_col", casa_core::PrimitiveType::Float64),
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
