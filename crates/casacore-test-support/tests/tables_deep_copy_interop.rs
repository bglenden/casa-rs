// SPDX-License-Identifier: LGPL-3.0-or-later
//! Deep copy interop tests between Rust and C++ casacore.
//!
//! Tests verify the 2×2 matrix:
//! - CC: C++ writes original + deep copy → C++ reads copy
//! - CR: C++ writes original + deep copy → Rust opens copy
//! - RC: Rust writes original + deep copy → C++ opens copy
//! - RR: Rust writes original + deep copy → Rust opens copy
//!
//! These tests are skipped when `pkg-config casacore` is not available.

use casacore_test_support::{
    CppTableFixture, cpp_backend_available, cpp_table_verify, cpp_table_write,
};

use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Build the same 5-row table as the C++ fixture: (id: Int32, name: String).
fn build_deep_copy_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .expect("valid schema");

    let mut table = Table::with_schema(schema);
    for i in 0..5 {
        let id = (i + 1) * 10;
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("item_{i}"))),
                ),
            ]))
            .expect("add row");
    }
    table
}

/// CC: C++ writes original + deep copy → C++ reads (baseline).
#[test]
fn cc_deep_copy() {
    if !cpp_backend_available() {
        eprintln!("skipping CC deep_copy test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    cpp_table_write(CppTableFixture::DeepCopy, dir.path())
        .expect("C++ write deep copy should succeed");

    cpp_table_verify(CppTableFixture::DeepCopy, dir.path())
        .expect("C++ verify deep copy should succeed");
}

/// CR: C++ writes original + deep copy → Rust opens copy.
#[test]
fn cr_deep_copy() {
    if !cpp_backend_available() {
        eprintln!("skipping CR deep_copy test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    cpp_table_write(CppTableFixture::DeepCopy, dir.path())
        .expect("C++ write deep copy should succeed");

    let copy_path = dir.path().join("copy.tbl");
    let table = Table::open(TableOptions::new(&copy_path)).expect("Rust should open C++ deep copy");

    assert_eq!(table.row_count(), 5, "deep copy should have 5 rows");

    for i in 0..5 {
        let expected_id = ((i as i32) + 1) * 10;
        let row = table.row(i).unwrap_or_else(|| panic!("row {i} exists"));
        assert_eq!(
            row.get("id").expect("id field"),
            &Value::Scalar(ScalarValue::Int32(expected_id)),
            "row {i} id mismatch"
        );
    }
}

/// RC: Rust writes original + deep copy → C++ opens copy.
#[test]
fn rc_deep_copy() {
    if !cpp_backend_available() {
        eprintln!("skipping RC deep_copy test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let original_path = dir.path().join("original.tbl");
    let copy_path = dir.path().join("copy.tbl");

    // Rust writes original with default StManAipsIO.
    let table = build_deep_copy_table();
    table
        .save(TableOptions::new(&original_path))
        .expect("save original");

    // Rust deep copies to StandardStMan.
    let table = Table::open(TableOptions::new(&original_path)).unwrap();
    table
        .deep_copy(TableOptions::new(&copy_path).with_data_manager(DataManagerKind::StandardStMan))
        .expect("deep copy");

    // C++ opens and verifies.
    cpp_table_verify(CppTableFixture::DeepCopy, dir.path())
        .expect("C++ should read Rust-produced deep copy");
}

/// RR: Rust writes original + deep copy → Rust opens copy.
#[test]
fn rr_deep_copy() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let original_path = dir.path().join("original.tbl");
    let copy_path = dir.path().join("copy.tbl");

    // Rust writes original with default StManAipsIO.
    let table = build_deep_copy_table();
    table
        .save(TableOptions::new(&original_path))
        .expect("save original");

    // Rust deep copies to StandardStMan.
    let table = Table::open(TableOptions::new(&original_path)).unwrap();
    table
        .deep_copy(TableOptions::new(&copy_path).with_data_manager(DataManagerKind::StandardStMan))
        .expect("deep copy");
    drop(table);

    // Re-open the deep copy.
    let reopened = Table::open(TableOptions::new(&copy_path)).expect("reopen deep copy");

    assert_eq!(reopened.row_count(), 5, "deep copy should have 5 rows");

    for i in 0..5 {
        let expected_id = ((i as i32) + 1) * 10;
        let row = reopened.row(i).unwrap_or_else(|| panic!("row {i} exists"));
        assert_eq!(
            row.get("id").expect("id field"),
            &Value::Scalar(ScalarValue::Int32(expected_id)),
            "row {i} id mismatch"
        );
        let expected_name = format!("item_{i}");
        assert_eq!(
            row.get("name").expect("name field"),
            &Value::Scalar(ScalarValue::String(expected_name)),
            "row {i} name mismatch"
        );
    }
}
