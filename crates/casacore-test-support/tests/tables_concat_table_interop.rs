// SPDX-License-Identifier: LGPL-3.0-or-later
//! ConcatTable interop tests between Rust and C++ casacore.
//!
//! Tests verify the 2×2 matrix:
//! - CC: C++ writes concat table → C++ reads
//! - CR: C++ writes concat table → Rust opens, verifies data
//! - RC: Rust writes concat table → C++ opens, verifies data
//! - RR: Rust writes concat table → Rust opens, verifies data
//!
//! These tests are skipped when `pkg-config casacore` is not available.

use casacore_test_support::{
    CppTableFixture, cpp_backend_available, cpp_table_verify, cpp_table_write,
};

use casacore_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Build a 3-row table matching the C++ fixture: (id: Int32, name: String).
fn build_part_table(ids: &[(i32, &str)]) -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .expect("valid schema");

    let mut table = Table::with_schema(schema);
    for &(id, name) in ids {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
            ]))
            .expect("add row");
    }
    table
}

/// CC: C++ writes concat → C++ reads (baseline).
#[test]
fn cc_concat_table() {
    if !cpp_backend_available() {
        eprintln!("skipping CC concat_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    cpp_table_write(CppTableFixture::ConcatTable, dir.path())
        .expect("C++ write concat table should succeed");

    cpp_table_verify(CppTableFixture::ConcatTable, dir.path())
        .expect("C++ verify concat table should succeed");
}

/// CR: C++ writes concat → Rust opens and verifies data.
#[test]
fn cr_concat_table() {
    if !cpp_backend_available() {
        eprintln!("skipping CR concat_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    cpp_table_write(CppTableFixture::ConcatTable, dir.path())
        .expect("C++ write concat table should succeed");

    // Rust opens the ConcatTable (materializes as plain Table).
    let concat_path = dir.path().join("concat.tbl");
    let table =
        Table::open(TableOptions::new(&concat_path)).expect("Rust should open C++ ConcatTable");

    assert_eq!(
        table.row_count(),
        6,
        "concatenated table should have 6 rows"
    );

    let expected_ids = [1, 2, 3, 4, 5, 6];
    for (i, &expected) in expected_ids.iter().enumerate() {
        let row = table.row(i).unwrap_or_else(|| panic!("row {i} exists"));
        assert_eq!(
            row.get("id").expect("id field"),
            &Value::Scalar(ScalarValue::Int32(expected)),
            "row {i} id mismatch"
        );
    }
}

/// RC: Rust writes concat → C++ opens and verifies data.
#[test]
fn rc_concat_table() {
    if !cpp_backend_available() {
        eprintln!("skipping RC concat_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let p0 = dir.path().join("part0.tbl");
    let p1 = dir.path().join("part1.tbl");
    let concat_path = dir.path().join("concat.tbl");

    // Rust writes part0 and part1.
    let part0 = build_part_table(&[(1, "alpha"), (2, "bravo"), (3, "charlie")]);
    part0.save(TableOptions::new(&p0)).expect("save part0");
    let mut part0 = Table::open(TableOptions::new(&p0)).unwrap();
    part0.set_path(&p0);

    let part1 = build_part_table(&[(4, "delta"), (5, "echo"), (6, "foxtrot")]);
    part1.save(TableOptions::new(&p1)).expect("save part1");
    let mut part1 = Table::open(TableOptions::new(&p1)).unwrap();
    part1.set_path(&p1);

    // Rust writes ConcatTable.
    let concat = Table::concat(vec![part0, part1]).expect("concat");
    concat
        .save(TableOptions::new(&concat_path))
        .expect("save concat");

    // C++ opens and verifies.
    cpp_table_verify(CppTableFixture::ConcatTable, dir.path())
        .expect("C++ should read Rust-produced ConcatTable");
}

/// RR: Rust writes concat → Rust opens and verifies data.
#[test]
fn rr_concat_table() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let p0 = dir.path().join("part0.tbl");
    let p1 = dir.path().join("part1.tbl");
    let concat_path = dir.path().join("concat.tbl");

    // Rust writes part0 and part1.
    let part0 = build_part_table(&[(1, "alpha"), (2, "bravo"), (3, "charlie")]);
    part0.save(TableOptions::new(&p0)).expect("save part0");
    let mut part0 = Table::open(TableOptions::new(&p0)).unwrap();
    part0.set_path(&p0);

    let part1 = build_part_table(&[(4, "delta"), (5, "echo"), (6, "foxtrot")]);
    part1.save(TableOptions::new(&p1)).expect("save part1");
    let mut part1 = Table::open(TableOptions::new(&p1)).unwrap();
    part1.set_path(&p1);

    // Rust writes ConcatTable.
    let concat = Table::concat(vec![part0, part1]).expect("concat");
    concat
        .save(TableOptions::new(&concat_path))
        .expect("save concat");
    drop(concat);

    // Re-open the ConcatTable from disk (materializes as plain Table).
    let reopened = Table::open(TableOptions::new(&concat_path)).expect("reopen concat table");

    assert_eq!(
        reopened.row_count(),
        6,
        "concatenated table should have 6 rows"
    );

    let expected_ids = [1, 2, 3, 4, 5, 6];
    for (i, &expected) in expected_ids.iter().enumerate() {
        let row = reopened.row(i).unwrap_or_else(|| panic!("row {i} exists"));
        assert_eq!(
            row.get("id").expect("id field"),
            &Value::Scalar(ScalarValue::Int32(expected)),
            "row {i} id mismatch"
        );
    }
}
