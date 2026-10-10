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
#![cfg(all(feature = "cpp-interop-tests", unix))]

use std::collections::HashMap;

use casa_test_support::table_sync::{persisted_table_shape, published_table_sync};
use casa_test_support::{CppTableFixture, TableOracle, casacore_oracle_available};

use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, LockMode, LockOptions, LockType, Table,
    TableOptions, TableSchema, TableWriteLock,
};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

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
    if !casacore_oracle_available() {
        eprintln!("skipping CC lock test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("cc_lock_table");

    TableOracle::table_write(CppTableFixture::LockFile, &table_path)
        .expect("C++ write with lock should succeed");

    // Verify lock file exists.
    assert!(
        table_path.join("table.lock").exists(),
        "C++ should create table.lock"
    );

    TableOracle::table_verify(CppTableFixture::LockFile, &table_path)
        .expect("C++ verify with lock should succeed");
}

/// CR: C++ writes with lock → Rust reads with lock.
///
/// Validates that Rust can decode the C++-produced `table.lock`
/// (sync data format, request list layout).
#[test]
fn cr_lock_file() {
    if !casacore_oracle_available() {
        eprintln!("skipping CR lock test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("cr_lock_table");

    // C++ writes with locking.
    TableOracle::table_write(CppTableFixture::LockFile, &table_path)
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
    let row = table.row_accessor().row(0).expect("row 0 exists");
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
    if !casacore_oracle_available() {
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

    // Open with locking, change the table under the write lock, then unlock
    // to produce a table.lock file with sync data. A write lock that changes
    // nothing publishes no sync data, as in casacore.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut locked = Table::open_with_lock(TableOptions::new(&table_path), lock_opts.clone())
        .expect("open with lock");
    locked.lock(LockType::Write, 1).expect("acquire write lock");
    locked
        .row_accessor_mut()
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("from_rust".into())),
        )
        .expect("rewrite row 0");
    locked.unlock().expect("unlock (flushes sync data)");
    drop(locked);
    let mut reader =
        Table::open_with_lock(TableOptions::new(&table_path), lock_opts).expect("reopen");
    assert!(reader.lock(LockType::Read, 1).expect("read lock"));
    assert_eq!(
        reader.locked_modify_counter().expect("modify counter"),
        1,
        "Rust published the write in the sync data"
    );
    drop(reader);

    assert!(
        table_path.join("table.lock").exists(),
        "Rust should create table.lock"
    );

    // C++ opens with locking and verifies.
    TableOracle::table_verify(CppTableFixture::LockFile, &table_path)
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
    let row = reopened.row_accessor().row(0).expect("row 0 exists");
    let id = row.get("id").expect("id field");
    assert_eq!(id, &Value::Scalar(ScalarValue::Int32(42)));

    reopened.unlock().expect("unlock");
}

/// The sync data a Rust write publishes must describe the table on disk:
/// C++ casacore reopened with `UserLocking` synchronizes with it on every
/// explicit lock, refusing a different column count and asserting one change
/// counter per data manager (`ColumnSet::resync`).
#[track_caller]
fn assert_cpp_relocks(table_path: &std::path::Path, columns: u32) {
    let persisted = persisted_table_shape(table_path);
    let published = published_table_sync(table_path).expect("published sync data");
    assert_eq!(published.shape, persisted, "published versus persisted");
    let (rows, cpp_columns) =
        TableOracle::lock_read_relock(table_path).expect("C++ locks, unlocks and relocks");
    assert_eq!(rows, persisted.rows);
    assert_eq!(cpp_columns, columns);
}

/// RC with several data managers: writes published by `TableWriteLock` and
/// by `Table::unlock`, and a layout change made under the lock, each leave a
/// table C++ casacore can lock, unlock and relock.
#[test]
fn rc_published_writes_relock_in_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping RC relock test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("rc_relock_table");
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    for id in 0..3 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("n".into()))),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .expect("row");
    }
    let binding = |data_manager| ColumnBinding {
        data_manager,
        tile_shape: None,
    };
    let bindings = HashMap::from([(
        "name".to_string(),
        binding(DataManagerKind::IncrementalStMan),
    )]);
    table
        .prepare_write()
        .save_with_bindings(
            TableOptions::new(&table_path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("save with two data managers");
    assert_eq!(persisted_table_shape(&table_path).data_managers, 2);

    // An in-place writer's publication.
    let mut lock = TableWriteLock::acquire(&table_path, 1).expect("write lock");
    lock.record_write();
    lock.release().expect("release");
    assert_cpp_relocks(&table_path, 3);

    // Table::unlock's publication.
    let mut locked = Table::open_with_lock(
        TableOptions::new(&table_path),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open with lock");
    assert!(locked.lock(LockType::Write, 1).expect("write lock"));
    locked
        .row_accessor_mut()
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("changed".into())),
        )
        .expect("change a cell");
    locked.unlock().expect("unlock");
    drop(locked);
    assert_cpp_relocks(&table_path, 3);

    // A layout change made under the lock: one more column and manager.
    let mut lock = TableWriteLock::acquire(&table_path, 1).expect("write lock");
    let mut plain = Table::open(TableOptions::new(&table_path)).expect("open");
    plain
        .add_column(
            ColumnSchema::array_variable("extra", PrimitiveType::Float32, Some(1)),
            None,
        )
        .expect("add the column");
    lock.record_write();
    plain
        .prepare_write()
        .add_tiled_shape_column("extra", &[], None)
        .expect("install the column");
    lock.release().expect("release");
    drop(plain);
    assert_cpp_relocks(&table_path, 4);
}
