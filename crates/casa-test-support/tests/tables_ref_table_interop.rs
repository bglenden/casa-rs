// SPDX-License-Identifier: LGPL-3.0-or-later
//! RefTable interop tests between Rust and C++ casacore.
//!
//! Tests verify the 2×2 matrix:
//! - CC: C++ writes parent + RefTable → C++ reads RefTable
//! - CR: C++ writes parent + RefTable → Rust opens, verifies data
//! - RC: Rust writes parent + RefTable → C++ opens, verifies data
//! - RR: Rust writes parent + RefTable → Rust opens, verifies data
//!
//! These tests are skipped when `pkg-config casacore` is not available.

#![cfg(feature = "cpp-interop-tests")]

use casa_test_support::{CppTableFixture, TableOracle, casacore_oracle_available};

use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Build the same parent table as the C++ `write_ref_table_impl` fixture:
/// schema (id: Int32, name: String), 3 rows:
///   row 0: (10, "alpha")
///   row 1: (20, "beta")
///   row 2: (30, "gamma")
fn build_ref_parent_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .expect("valid schema");

    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(10))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("alpha".into()))),
        ]))
        .expect("row 0");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(20))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("beta".into()))),
        ]))
        .expect("row 1");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(30))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("gamma".into()))),
        ]))
        .expect("row 2");
    table
}

/// CC: C++ writes parent + RefTable → C++ reads RefTable (baseline).
#[test]
fn cc_ref_table() {
    if !casacore_oracle_available() {
        eprintln!("skipping CC ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    TableOracle::table_write(CppTableFixture::RefTable, dir.path())
        .expect("C++ write ref table should succeed");

    TableOracle::table_verify(CppTableFixture::RefTable, dir.path())
        .expect("C++ verify ref table should succeed");
}

/// CR: C++ writes parent + RefTable → Rust opens and verifies data.
#[test]
fn cr_ref_table() {
    if !casacore_oracle_available() {
        eprintln!("skipping CR ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");

    // C++ writes parent.tbl and ref.tbl
    TableOracle::table_write(CppTableFixture::RefTable, dir.path())
        .expect("C++ write ref table should succeed");

    // Rust opens the RefTable (materializes it as a plain Table)
    let ref_path = dir.path().join("ref.tbl");
    let table = Table::open(TableOptions::new(&ref_path)).expect("Rust should open C++ RefTable");

    assert_eq!(table.row_count(), 2, "RefTable should have 2 rows");

    // Row 0 → parent row 0: (10, "alpha")
    let row0 = table.row_accessor().row(0).expect("row 0 exists");
    assert_eq!(
        row0.get("id").expect("id field"),
        &Value::Scalar(ScalarValue::Int32(10))
    );
    assert_eq!(
        row0.get("name").expect("name field"),
        &Value::Scalar(ScalarValue::String("alpha".into()))
    );

    // Row 1 → parent row 2: (30, "gamma")
    let row1 = table.row_accessor().row(1).expect("row 1 exists");
    assert_eq!(
        row1.get("id").expect("id field"),
        &Value::Scalar(ScalarValue::Int32(30))
    );
    assert_eq!(
        row1.get("name").expect("name field"),
        &Value::Scalar(ScalarValue::String("gamma".into()))
    );
}

/// RC: Rust writes parent + RefTable → C++ opens and verifies data.
#[test]
fn rc_ref_table() {
    if !casacore_oracle_available() {
        eprintln!("skipping RC ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let parent_path = dir.path().join("parent.tbl");
    let ref_path = dir.path().join("ref.tbl");

    // Rust writes parent table
    let mut parent = build_ref_parent_table();
    parent
        .save(TableOptions::new(&parent_path))
        .expect("save parent");
    parent.set_path(&parent_path);

    // Rust creates RefTable (rows 0 and 2) and saves
    let ref_table = parent.select_rows(&[0, 2]).expect("select rows 0 and 2");
    ref_table
        .save(TableOptions::new(&ref_path))
        .expect("save ref table");

    // C++ opens and verifies
    TableOracle::table_verify(CppTableFixture::RefTable, dir.path())
        .expect("C++ should read Rust-produced RefTable");
}

/// RC after a move (#715): casa-rs stores a parent deeper under the RefTable's
/// directory as `./sub/parent.tbl` and a parent enclosing the RefTable as
/// `sel.tbl/.`; casacore resolves both once the whole tree has moved.
#[test]
fn rc_ref_tables_beside_and_inside_their_parent_open_after_a_move() {
    if !casacore_oracle_available() {
        eprintln!("skipping RC moved ref_table test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let root = dir.path().join("run");
    let parent_path = root.join("sub").join("parent.tbl");
    std::fs::create_dir_all(parent_path.parent().unwrap()).unwrap();
    let mut parent = build_ref_parent_table();
    parent
        .save(TableOptions::new(&parent_path))
        .expect("save parent");
    parent.set_path(&parent_path);
    let beside = parent.select_rows(&[0, 2]).expect("select rows 0 and 2");
    beside
        .save(TableOptions::new(root.join("beside.tbl")))
        .expect("save the RefTable beside");
    let inside = parent.select_rows(&[1]).expect("select row 1");
    inside
        .save(TableOptions::new(parent_path.join("sel.tbl")))
        .expect("save the RefTable inside");

    let moved = dir.path().join("moved").join("run");
    std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
    std::fs::rename(&root, &moved).unwrap();
    let (rows, _) = TableOracle::lock_read_relock(&moved.join("beside.tbl"))
        .expect("C++ opens the moved RefTable beside its parent");
    assert_eq!(rows, 2);
    let inside = moved.join("sub").join("parent.tbl").join("sel.tbl");
    let (rows, _) = TableOracle::lock_read_relock(&inside)
        .expect("C++ opens the moved RefTable inside its parent");
    assert_eq!(rows, 1);
}

/// RR: Rust writes parent + RefTable → Rust opens and verifies data.
#[test]
fn rr_ref_table() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let parent_path = dir.path().join("parent.tbl");
    let ref_path = dir.path().join("ref.tbl");

    // Rust writes parent table
    let mut parent = build_ref_parent_table();
    parent
        .save(TableOptions::new(&parent_path))
        .expect("save parent");
    parent.set_path(&parent_path);

    // Rust creates RefTable (rows 0 and 2) and saves
    let ref_table = parent.select_rows(&[0, 2]).expect("select rows 0 and 2");
    ref_table
        .save(TableOptions::new(&ref_path))
        .expect("save ref table");

    // Re-open the RefTable from disk (materializes as plain Table)
    let reopened = Table::open(TableOptions::new(&ref_path)).expect("reopen ref table");

    assert_eq!(reopened.row_count(), 2, "RefTable should have 2 rows");

    // Row 0 → parent row 0: (10, "alpha")
    let row0 = reopened.row_accessor().row(0).expect("row 0 exists");
    assert_eq!(
        row0.get("id").expect("id field"),
        &Value::Scalar(ScalarValue::Int32(10))
    );
    assert_eq!(
        row0.get("name").expect("name field"),
        &Value::Scalar(ScalarValue::String("alpha".into()))
    );

    // Row 1 → parent row 2: (30, "gamma")
    let row1 = reopened.row_accessor().row(1).expect("row 1 exists");
    assert_eq!(
        row1.get("id").expect("id field"),
        &Value::Scalar(ScalarValue::Int32(30))
    );
    assert_eq!(
        row1.get("name").expect("name field"),
        &Value::Scalar(ScalarValue::String("gamma".into()))
    );
}
