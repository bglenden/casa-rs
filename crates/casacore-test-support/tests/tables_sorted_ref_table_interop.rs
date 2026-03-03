// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sorted RefTable interop tests between Rust and C++ casacore.
//!
//! Tests verify the 2×2 matrix:
//! - CC: C++ writes parent + sorted RefTable → C++ reads
//! - CR: C++ writes parent + sorted RefTable → Rust opens, verifies order
//! - RC: Rust writes parent + sorted RefTable → C++ opens, verifies order
//! - RR: Rust writes parent + sorted RefTable → Rust opens, verifies order
//!
//! These tests are skipped when `pkg-config casacore` is not available.

use casacore_test_support::{
    CppTableFixture, cpp_backend_available, cpp_table_verify, cpp_table_write,
};

use casacore_tables::{ColumnSchema, SortOrder, Table, TableOptions, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Build the same parent table as the C++ `write_sorted_ref_table_impl` fixture:
/// schema (id: Int32, name: String, value: Float32), 5 rows:
///   row 0: (30, "charlie", 3.0)
///   row 1: (10, "alpha",   1.0)
///   row 2: (50, "echo",    5.0)
///   row 3: (20, "bravo",   2.0)
///   row 4: (40, "delta",   4.0)
fn build_sorted_parent_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("value", PrimitiveType::Float32),
    ])
    .expect("valid schema");

    let mut table = Table::with_schema(schema);
    let data = [
        (30, "charlie", 3.0f32),
        (10, "alpha", 1.0),
        (50, "echo", 5.0),
        (20, "bravo", 2.0),
        (40, "delta", 4.0),
    ];
    for (id, name, value) in data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
                RecordField::new("value", Value::Scalar(ScalarValue::Float32(value))),
            ]))
            .expect("add row");
    }
    table
}

/// CC: C++ writes parent + sorted RefTable → C++ reads sorted RefTable (baseline).
#[test]
fn cc_sorted_ref_table() {
    if !cpp_backend_available() {
        eprintln!("skipping CC sorted_ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    cpp_table_write(CppTableFixture::SortedRefTable, dir.path())
        .expect("C++ write sorted ref table should succeed");

    cpp_table_verify(CppTableFixture::SortedRefTable, dir.path())
        .expect("C++ verify sorted ref table should succeed");
}

/// CR: C++ writes parent + sorted RefTable → Rust opens and verifies order.
#[test]
fn cr_sorted_ref_table() {
    if !cpp_backend_available() {
        eprintln!("skipping CR sorted_ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    // C++ writes parent.tbl and sorted.tbl
    cpp_table_write(CppTableFixture::SortedRefTable, dir.path())
        .expect("C++ write sorted ref table should succeed");

    // Rust opens the sorted RefTable (materializes it as a plain Table)
    let sorted_path = dir.path().join("sorted.tbl");
    let table =
        Table::open(TableOptions::new(&sorted_path)).expect("Rust should open C++ sorted RefTable");

    assert_eq!(table.row_count(), 5, "sorted table should have 5 rows");

    // Should be in descending id order: 50, 40, 30, 20, 10.
    let expected_ids = [50, 40, 30, 20, 10];
    for (i, &expected) in expected_ids.iter().enumerate() {
        let row = table.row(i).unwrap_or_else(|| panic!("row {i} exists"));
        assert_eq!(
            row.get("id").expect("id field"),
            &Value::Scalar(ScalarValue::Int32(expected)),
            "row {i} id mismatch"
        );
    }
}

/// RC: Rust writes parent + sorted RefTable → C++ opens and verifies order.
#[test]
fn rc_sorted_ref_table() {
    if !cpp_backend_available() {
        eprintln!("skipping RC sorted_ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let parent_path = dir.path().join("parent.tbl");
    let sorted_path = dir.path().join("sorted.tbl");

    // Rust writes parent table.
    let mut parent = build_sorted_parent_table();
    parent
        .save(TableOptions::new(&parent_path))
        .expect("save parent");
    parent.set_path(&parent_path);

    // Rust sorts descending by id and saves as RefTable.
    let sorted = parent
        .sort(&[("id", SortOrder::Descending)])
        .expect("sort descending by id");
    sorted
        .save(TableOptions::new(&sorted_path))
        .expect("save sorted ref table");

    // C++ opens and verifies.
    cpp_table_verify(CppTableFixture::SortedRefTable, dir.path())
        .expect("C++ should read Rust-produced sorted RefTable");
}

/// RR: Rust writes parent + sorted RefTable → Rust opens and verifies order.
#[test]
fn rr_sorted_ref_table() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let parent_path = dir.path().join("parent.tbl");
    let sorted_path = dir.path().join("sorted.tbl");

    // Rust writes parent table.
    let mut parent = build_sorted_parent_table();
    parent
        .save(TableOptions::new(&parent_path))
        .expect("save parent");
    parent.set_path(&parent_path);

    // Rust sorts descending by id and saves as RefTable.
    let sorted = parent
        .sort(&[("id", SortOrder::Descending)])
        .expect("sort descending by id");
    sorted
        .save(TableOptions::new(&sorted_path))
        .expect("save sorted ref table");
    drop(sorted);

    // Re-open the sorted RefTable from disk (materializes as plain Table).
    let reopened = Table::open(TableOptions::new(&sorted_path)).expect("reopen sorted ref table");

    assert_eq!(reopened.row_count(), 5, "sorted table should have 5 rows");

    // Should be in descending id order: 50, 40, 30, 20, 10.
    let expected_ids = [50, 40, 30, 20, 10];
    for (i, &expected) in expected_ids.iter().enumerate() {
        let row = reopened.row(i).unwrap_or_else(|| panic!("row {i} exists"));
        assert_eq!(
            row.get("id").expect("id field"),
            &Value::Scalar(ScalarValue::Int32(expected)),
            "row {i} id mismatch"
        );
    }
}
