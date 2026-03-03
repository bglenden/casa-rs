// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lock file format interop tests between Rust and C++ casacore.
//!
//! Tests verify that:
//! - C++ casacore can open a table written by Rust with locking
//!   (the Rust-produced `table.lock` is binary-compatible)
//! - Rust can open a table written by C++ casacore with locking
//!   (the C++-produced `table.lock` is readable)
//!
//! These tests are skipped when `pkg-config casacore` is not available.
#![cfg(unix)]

use casacore_test_support::{
    CppTableFixture, cpp_backend_available, cpp_table_verify, cpp_table_write,
};

use casacore_tables::{
    ColumnSchema, LockMode, LockOptions, LockType, Table, TableOptions, TableSchema,
};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Build the same table as the C++ `write_with_lock_impl` fixture:
/// schema (id: Int, name: String), 1 row: (42, "from_rust").
fn build_lock_test_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .expect("valid schema");

    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("from_rust".into())),
            ),
        ]))
        .expect("schema-compliant row");
    table
}

/// CC: C++ writes with lock → C++ reads with lock.
///
/// Validates that our C++ shim itself works (baseline).
#[test]
fn cc_lock_file() {
    if !cpp_backend_available() {
        eprintln!("skipping CC lock test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("cc_lock_table");

    cpp_table_write(CppTableFixture::LockFile, &table_path)
        .expect("C++ write with lock should succeed");

    // Verify lock file exists.
    assert!(
        table_path.join("table.lock").exists(),
        "C++ should create table.lock"
    );

    cpp_table_verify(CppTableFixture::LockFile, &table_path)
        .expect("C++ verify with lock should succeed");
}

/// CR: C++ writes with lock → Rust reads with lock.
///
/// Validates that Rust can decode the C++-produced `table.lock`
/// (sync data format, request list layout).
#[test]
fn cr_lock_file() {
    if !cpp_backend_available() {
        eprintln!("skipping CR lock test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("cr_lock_table");

    // C++ writes with locking.
    cpp_table_write(CppTableFixture::LockFile, &table_path)
        .expect("C++ write with lock should succeed");

    assert!(
        table_path.join("table.lock").exists(),
        "C++ should create table.lock"
    );

    // Rust opens with locking and reads the C++-produced lock file.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut table = Table::open_with_lock(TableOptions::new(&table_path), lock_opts)
        .expect("Rust should open C++-locked table");

    table
        .lock(LockType::Read, 1)
        .expect("Rust should acquire read lock on C++-produced lock file");

    assert_eq!(table.row_count(), 1, "should see 1 row from C++");

    // Verify the data C++ wrote.
    let row = table.row(0).expect("row 0 exists");
    let id = row.get("id").expect("id field");
    assert_eq!(id, &Value::Scalar(ScalarValue::Int32(42)));
    let name = row.get("name").expect("name field");
    assert_eq!(name, &Value::Scalar(ScalarValue::String("from_cpp".into())));

    table.unlock().expect("unlock");
}

/// RC: Rust writes with lock → C++ reads with lock.
///
/// Validates that the Rust-produced `table.lock` is binary-compatible
/// with C++ casacore's `LockFile` / `TableLockData`.
#[test]
fn rc_lock_file() {
    if !cpp_backend_available() {
        eprintln!("skipping RC lock test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("rc_lock_table");

    // Rust writes the table data.
    let table = build_lock_test_table();
    table
        .save(TableOptions::new(&table_path))
        .expect("save table data");

    // Open with locking, acquire write lock, then unlock to produce
    // a table.lock file with sync data.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut locked =
        Table::open_with_lock(TableOptions::new(&table_path), lock_opts).expect("open with lock");
    locked.lock(LockType::Write, 1).expect("acquire write lock");
    locked.unlock().expect("unlock (flushes sync data)");
    drop(locked);

    assert!(
        table_path.join("table.lock").exists(),
        "Rust should create table.lock"
    );

    // C++ opens with locking and verifies.
    cpp_table_verify(CppTableFixture::LockFile, &table_path)
        .expect("C++ should read Rust-produced table with lock file");
}

/// RR with locking: Rust writes with lock → Rust reads with lock.
///
/// Validates the Rust lock round-trip independently of C++.
#[test]
fn rr_lock_file() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("rr_lock_table");

    // Rust writes.
    let table = build_lock_test_table();
    table
        .save(TableOptions::new(&table_path))
        .expect("save table data");

    // Open with locking, write lock + unlock to produce sync data.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut locked = Table::open_with_lock(TableOptions::new(&table_path), lock_opts.clone())
        .expect("open with lock");
    locked.lock(LockType::Write, 1).expect("acquire write lock");
    locked.unlock().expect("unlock");
    drop(locked);

    // Rust reads with locking.
    let mut reopened =
        Table::open_with_lock(TableOptions::new(&table_path), lock_opts).expect("reopen with lock");
    reopened.lock(LockType::Read, 1).expect("acquire read lock");

    assert_eq!(reopened.row_count(), 1);
    let row = reopened.row(0).expect("row 0 exists");
    let id = row.get("id").expect("id field");
    assert_eq!(id, &Value::Scalar(ScalarValue::Int32(42)));

    reopened.unlock().expect("unlock");
}
