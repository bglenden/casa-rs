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
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use casa_test_support::table_sync::{persisted_table_shape, published_table_sync};
use casa_test_support::taql_interop::TaqlOracle;
use casa_test_support::{
    CppTableFixture, HeldCppTableReader, TableOracle, casacore_oracle_available,
};

use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, LockMode, LockOptions, LockType, Table,
    TableOptions, TableSchema, TableWriteLock,
};

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

// ---- A C++ reader holding the table open across a Rust write ----
//
// casacore re-reads an open table when it takes a lock only if the modify
// counter in `table.lock` changed (`PlainTable::lock`); otherwise it keeps
// what its columns cached. A writer that does not raise the counter leaves
// such a reader with stale data although a fresh open would see the write.
// `fcntl` locks belong to a process, so the reader runs in its own process.

/// A `casacore-held-table-reader` process: C++ casacore holding the table
/// and its column "id" open between lock periods.
struct HeldCppReaderProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl HeldCppReaderProcess {
    fn open(table: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_casacore-held-table-reader"))
            .arg(table)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the C++ reader");
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().expect("reader stdout"));
        let mut reader = Self {
            child,
            stdin,
            stdout,
        };
        let reply = reader.reply();
        assert_eq!(reply, "ready", "the C++ reader did not open the table");
        reader
    }

    fn reply(&mut self) -> String {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("C++ reader reply");
        line.trim_end().to_owned()
    }

    /// Take a read lock, read `id(0)` and release the lock, in C++.
    fn read_id(&mut self) -> i32 {
        self.read_int("read", "id ")
    }

    fn read_keyword(&mut self, name: &str) -> i32 {
        self.read_int(&format!("keyword {name}"), "keyword ")
    }

    fn read_int(&mut self, command: &str, prefix: &str) -> i32 {
        let stdin = self.stdin.as_mut().expect("reader stdin");
        writeln!(stdin, "{command}").expect("send read");
        stdin.flush().expect("flush read");
        let reply = self.reply();
        reply
            .strip_prefix(prefix)
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("C++ reader replied {reply:?}"))
    }
}

impl Drop for HeldCppReaderProcess {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.wait();
    }
}

/// A one-row table whose Int column "id" holds 1, with that write published
/// (modify counter 1). With `second_manager`, the String column "name"
/// ("initial") is stored by StandardStMan and "id" by StManAipsIO;
/// otherwise the table has only "id", stored by StManAipsIO.
fn published_id_table(dir: &Path, second_manager: bool) -> PathBuf {
    let path = dir.join("held_reader_table");
    let mut columns = vec![ColumnSchema::scalar("id", PrimitiveType::Int32)];
    let mut row = vec![RecordField::new("id", Value::Scalar(ScalarValue::Int32(1)))];
    if second_manager {
        columns.push(ColumnSchema::scalar("name", PrimitiveType::String));
        row.push(RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("initial".into())),
        ));
    }
    let mut table = Table::with_schema(TableSchema::new(columns).expect("valid schema"));
    table.add_row(RecordValue::new(row)).expect("row");
    let bindings = if second_manager {
        HashMap::from([(
            "name".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::StandardStMan,
                tile_shape: None,
            },
        )])
    } else {
        HashMap::new()
    };
    table
        .save_with_bindings(TableOptions::new(&path), &bindings)
        .expect("save the table");
    let mut lock = TableWriteLock::acquire(&path, 1).expect("write lock");
    lock.record_write();
    lock.release().expect("publish the table");
    assert_eq!(published_counter(&path), 1);
    path
}

/// The modify counter `table.lock` publishes. Reading it through another
/// descriptor drops this process's `fcntl` locks on the file, so it is read
/// only while this process holds none.
fn published_counter(table: &Path) -> u32 {
    published_table_sync(table)
        .expect("published sync data")
        .modify_counter
}

/// Run `write` on a Rust handle that holds the table's write lock, then
/// release the lock, while C++ casacore holds the table open in another
/// process after reading it. Returns the published modify counters before
/// and after, and the "id" the C++ reader then reads under a new lock.
fn write_under_a_held_cpp_reader(
    table: &Path,
    write: impl FnOnce(&mut Table, &Path),
) -> (u32, u32, i32) {
    let before = published_counter(table);
    let mut reader = HeldCppReaderProcess::open(table);
    assert_eq!(reader.read_id(), 1, "C++ reads the initial value");

    let mut writer = Table::open_with_lock(
        TableOptions::new(table),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open with lock");
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    write(&mut writer, table);
    writer.unlock().expect("unlock");
    drop(writer);

    let after = published_counter(table);
    (before, after, reader.read_id())
}

fn set_id(table: &mut Table, id: i32) {
    table
        .row_accessor_mut()
        .set_cell(0, "id", Value::Scalar(ScalarValue::Int32(id)))
        .expect("set id");
}

fn cell(table: &Table, column: &str) -> ScalarValue {
    table
        .cell_accessor(0, column)
        .and_then(|cell| cell.scalar())
        .expect("row 0 cell")
        .clone()
}

#[test]
fn failed_full_save_leaves_the_previous_table_readable() {
    if !casacore_oracle_available() {
        eprintln!("skipping full-save readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = published_id_table(dir.path(), false);
    let mut writer = Table::open(TableOptions::new(&path)).expect("open initial table");
    set_id(&mut writer, 2);

    // The full writer can build this descriptor, but the tiled
    // serializer rejects the read-only TiledDataStMan format.
    let failed =
        writer.save(TableOptions::new(&path).with_data_manager(DataManagerKind::TiledDataStMan));
    assert!(failed.is_err(), "invalid tiled save succeeded");
    let rust = Table::open(TableOptions::new(&path)).map(|table| cell(&table, "id"));
    let cpp = HeldCppTableReader::open(&path).and_then(|mut table| table.read_id());
    assert!(
        matches!(rust, Ok(ScalarValue::Int32(1))) && matches!(cpp, Ok(1)),
        "the old table must survive a mid-save failure ({failed:?}): Rust {rust:?}, C++ {cpp:?}"
    );

    writer.save(TableOptions::new(&path)).expect("retry save");
    let rust = Table::open(TableOptions::new(&path)).expect("reopen from Rust");
    assert_eq!(cell(&rust, "id"), ScalarValue::Int32(2));
    let mut cpp = HeldCppTableReader::open(&path).expect("reopen from C++");
    assert_eq!(cpp.read_id().expect("read from C++"), 2);
}

#[test]
fn full_save_refuses_rebinding_a_virtual_column_held_by_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    let path = dir.path().join("forward");
    let table_with_id = |id| {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(id)),
            )]))
            .unwrap();
        table
    };
    for (target, id) in [(&a, 11), (&b, 22)] {
        table_with_id(id).save(TableOptions::new(target)).unwrap();
    }
    let mut original = table_with_id(0);
    original.bind_forward_column("id", &a).unwrap();
    let options = TableOptions::new(&path);
    original.save(options.clone()).unwrap();
    original.save(options.clone()).unwrap();
    let before = published_counter(&path);
    let mut held = HeldCppReaderProcess::open(&path);
    assert_eq!(held.read_id(), 11);

    let mut changed = table_with_id(0);
    changed.bind_forward_column("id", &b).unwrap();
    let saved = changed.save(options.clone());
    let held_id = held.read_id();
    let fresh_id = HeldCppReaderProcess::open(&path).read_id();
    assert!(
        saved
            .as_ref()
            .is_err_and(|error| error.to_string().contains("layout of an open table"))
            && held_id == 11
            && fresh_id == 11
            && published_counter(&path) == before,
        "forwarding save returned {saved:?}, held C++ reader {held_id}, fresh C++ reader {fresh_id}"
    );
    assert_eq!(
        cell(&Table::open(options.clone()).unwrap(), "id"),
        ScalarValue::Int32(11)
    );

    drop(held);
    changed.save(options.clone()).unwrap();
    assert_eq!(published_counter(&path), before + 1);
    assert_eq!(HeldCppReaderProcess::open(&path).read_id(), 22);
    assert_eq!(
        cell(&Table::open(options).unwrap(), "id"),
        ScalarValue::Int32(22)
    );
}

#[test]
fn full_save_refuses_read_only_descriptor_without_changing_table() {
    if !casacore_oracle_available() {
        eprintln!("skipping full-save readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = published_id_table(dir.path(), false);
    let options = TableOptions::new(&path);
    let mut writer = Table::open(options.clone()).expect("open initial table");
    set_id(&mut writer, 2);
    let control = path.join("table.dat");
    let metadata = control.metadata().expect("descriptor metadata");
    std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o444))
        .expect("make descriptor read only");
    match std::fs::OpenOptions::new().write(true).open(&control) {
        Ok(_) => {
            eprintln!("skipping permission assertion: process can write a mode 0444 file");
            return;
        }
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied),
    }

    let mut reader = HeldCppReaderProcess::open(&path);
    assert_eq!(reader.read_id(), 1);
    let before = published_counter(&path);
    let saved = writer.save(options.clone());
    assert!(saved.is_err(), "read-only table.dat accepted a full save");
    assert_eq!(published_counter(&path), before);
    assert_eq!(control.metadata().unwrap().ino(), metadata.ino());
    let rust = Table::open(options.clone()).expect("reopen from Rust");
    assert_eq!(cell(&rust, "id"), ScalarValue::Int32(1));
    assert_eq!(reader.read_id(), 1);
    let mut fresh = HeldCppReaderProcess::open(&path);
    assert_eq!(fresh.read_id(), 1);

    std::fs::set_permissions(&control, metadata.permissions()).expect("restore permissions");
    writer.save(options).expect("retry full save");
    assert_eq!(reader.read_id(), 2);
    assert_eq!(fresh.read_id(), 2);
}

#[test]
fn implicit_close_leaves_a_failed_unscoped_write_unpublished() {
    if !casacore_oracle_available() {
        eprintln!("skipping close readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let opts = TableOptions::new(path)
        .with_data_manager(DataManagerKind::StandardStMan)
        .with_data_manager_group("ScalarData");
    build_lock_test_table().save(opts.clone()).unwrap();
    let original = files_and_inodes(path, &["table.dat", "table.f0", "table.info", "table.lock"]);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    let edit = |table: &mut Table| {
        table.rename_column("name", "renamed").unwrap();
        set_id(table, 52);
    };
    edit(&mut writer);
    let control = path.join("table.dat");
    let Some(permissions) = make_read_only(&control) else {
        return;
    };
    assert!(writer.unlock().is_err());
    assert!(writer.has_lock(LockType::Write));
    std::fs::set_permissions(control, permissions).unwrap();
    drop(writer);

    assert_eq!(
        files_and_inodes(path, &["table.dat", "table.f0", "table.info", "table.lock"]),
        original
    );
    let rust = Table::open(opts.clone()).unwrap();
    assert_eq!(cell(&rust, "id"), ScalarValue::Int32(42));
    assert!(rust.schema().unwrap().column("name").is_some());
    assert_eq!(
        TableOracle::column_manager_name(path, "name").unwrap(),
        "ScalarData"
    );
    assert_eq!(
        HeldCppTableReader::open(path).unwrap().read_id().unwrap(),
        42
    );

    let mut writer = Table::open(opts.clone()).unwrap();
    edit(&mut writer);
    writer.flush().unwrap();
    assert_eq!(
        cell(&Table::open(opts).unwrap(), "id"),
        ScalarValue::Int32(52)
    );
    assert_eq!(
        HeldCppTableReader::open(path).unwrap().read_id().unwrap(),
        52
    );
    assert_eq!(
        TableOracle::column_manager_name(path, "renamed").unwrap(),
        "ScalarData"
    );
}

fn make_read_only(path: &Path) -> Option<std::fs::Permissions> {
    let original = std::fs::metadata(path).unwrap().permissions();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444)).unwrap();
    match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(_) => {
            eprintln!("skipping permission assertion: process can write a mode 0444 file");
            std::fs::set_permissions(path, original).unwrap();
            None
        }
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            Some(original)
        }
    }
}

fn files_and_inodes(path: &Path, files: &[&str]) -> Vec<(u64, Vec<u8>)> {
    files
        .iter()
        .map(|name| {
            let file = path.join(name);
            (file.metadata().unwrap().ino(), std::fs::read(file).unwrap())
        })
        .collect()
}

#[test]
fn explicit_flush_keeps_mixed_managers_after_failure() {
    if !casacore_oracle_available() {
        eprintln!("skipping flush readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed");
    let opts = TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan);
    let mut table = build_lock_test_table();
    table
        .add_column(
            ColumnSchema::scalar("flag", PrimitiveType::Bool),
            Some(Value::Scalar(ScalarValue::Bool(false))),
        )
        .unwrap();
    let bindings = HashMap::from([(
        "flag".to_owned(),
        ColumnBinding {
            data_manager: DataManagerKind::StManAipsIO,
            tile_shape: None,
        },
    )]);
    table.save_with_bindings(opts.clone(), &bindings).unwrap();
    let files = [
        "table.dat",
        "table.f0",
        "table.f1",
        "table.info",
        "table.lock",
    ];
    let snapshot = files_and_inodes(&path, &files);
    let mut writer = Table::open(opts.clone()).unwrap();
    let managers = writer.data_manager_info().to_vec();
    assert_eq!(
        TableOracle::column_manager_name(&path, "id").unwrap(),
        "StandardStMan"
    );
    set_id(&mut writer, 52);
    writer
        .row_accessor_mut()
        .set_cell(0, "flag", Value::Scalar(ScalarValue::Bool(true)))
        .unwrap();

    let assert_readback = |id, flag, cpp_flag| {
        let read = Table::open(opts.clone()).unwrap();
        assert_eq!(read.data_manager_info(), managers);
        assert_eq!(cell(&read, "id"), ScalarValue::Int32(id));
        assert_eq!(cell(&read, "flag"), ScalarValue::Bool(flag));
        assert_eq!(cell(&read, "name"), ScalarValue::String("from_rust".into()));
        let cpp =
            TaqlOracle::query(&path, "SELECT id, iif(flag,1,0) AS flag_num, name FROM $1").unwrap();
        assert_eq!(
            cpp.grid.rows,
            [vec![
                id.to_string(),
                String::from(cpp_flag),
                "from_rust".to_owned()
            ]]
        );
    };
    for file in ["table.dat", "table.f1"] {
        let read_only = path.join(file);
        let Some(permissions) = make_read_only(&read_only) else {
            return;
        };
        assert!(writer.flush().is_err(), "flush accepted a read-only {file}");
        assert_eq!(files_and_inodes(&path, &files), snapshot);
        assert_readback(42, false, "0");
        std::fs::set_permissions(read_only, permissions).unwrap();
    }

    let mut held = HeldCppReaderProcess::open(&path);
    assert_eq!(held.read_id(), 42);
    writer.flush().unwrap();
    assert_eq!(held.read_id(), 52);
    assert_readback(52, true, "1");
    for (file, (inode, _)) in files.iter().zip(&snapshot).skip(1) {
        assert_eq!(path.join(file).metadata().unwrap().ino(), *inode, "{file}");
    }
}

#[test]
fn explicit_flush_keeps_forward_engine_bindings_intact() {
    if !casacore_oracle_available() {
        eprintln!("skipping forward-column readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    build_lock_test_table()
        .save(TableOptions::new(&target))
        .unwrap();
    let path = dir.path().join("forward");
    let opts = TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan);
    let mut table = build_lock_test_table();
    table.bind_forward_column("name", &target).unwrap();
    table.save(opts.clone()).unwrap();
    table.save(opts.clone()).unwrap();
    let mut writer = Table::open(opts.clone()).unwrap();
    assert!(writer.is_virtual_column("name"));
    let managers = writer.data_manager_info().to_vec();
    let mut held = HeldCppReaderProcess::open(&path);
    assert_eq!(held.read_id(), 42);
    set_id(&mut writer, 52);
    writer.flush().unwrap();
    assert_eq!(held.read_id(), 52);
    let read_name = || {
        let reopened = Table::open(opts.clone()).unwrap();
        assert_eq!(reopened.data_manager_info(), managers);
        let cpp = TaqlOracle::query(&path, "SELECT name FROM $1").unwrap();
        let name = cpp.grid.rows[0][0].clone();
        assert_eq!(cell(&reopened, "name"), ScalarValue::String(name.clone()));
        name
    };
    assert_eq!(read_name(), "from_rust");

    // A held engine caches its forward target. A flush of the raw binding
    // keyword must honor the same compatibility boundary as a full save.
    let alternate = dir.path().join("alternate");
    let mut replacement = build_lock_test_table();
    replacement
        .row_accessor_mut()
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("alternate".into())),
        )
        .unwrap();
    replacement.save(TableOptions::new(&alternate)).unwrap();
    let mut keywords = writer.column_keywords("name").unwrap().clone();
    keywords.upsert(
        "_ForwardColumn_TableName",
        Value::Scalar(ScalarValue::String(alternate.display().to_string())),
    );
    writer.set_column_keywords("name", keywords);
    let files = ["table.dat", "table.f0", "table.info", "table.lock"];
    let flushed = files_and_inodes(&path, &files);
    assert!(
        writer
            .flush()
            .unwrap_err()
            .to_string()
            .contains("virtual manager keywords")
    );
    assert_eq!(files_and_inodes(&path, &files), flushed);
    assert_eq!(read_name(), "from_rust");
    drop(held);
    assert!(writer.flush().is_err());
    let mut rebound = build_lock_test_table();
    set_id(&mut rebound, 52);
    rebound.bind_forward_column("name", &alternate).unwrap();
    rebound.save(opts.clone()).unwrap();
    assert_eq!(read_name(), "alternate");
}

#[test]
fn explicit_flush_refuses_a_same_size_tiled_shape_change_before_writing() {
    if !casacore_oracle_available() {
        eprintln!("skipping tiled flush readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let opts = TableOptions::new(path).with_data_manager(DataManagerKind::TiledShapeStMan);
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "tiles",
        PrimitiveType::Float32,
        Some(2),
    )])
    .unwrap();
    let array = |shape: &[usize], fill: f32| {
        Value::Array(casa_core::ArrayValue::Float32(ndarray::ArrayD::from_elem(
            ndarray::IxDyn(shape),
            fill,
        )))
    };
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "tiles",
            array(&[2, 3], 1.0),
        )]))
        .unwrap();
    table.save(opts.clone()).unwrap();
    let read_cpp = || {
        TaqlOracle::query(
            path,
            "SELECT shape(tiles) AS extent, sum(tiles) AS total FROM $1",
        )
        .unwrap()
        .grid
        .rows
    };
    let before = read_cpp();
    let mut writer = Table::open(opts.clone()).unwrap();
    writer
        .row_accessor_mut()
        .set_cell(0, "tiles", array(&[3, 2], 3.0))
        .unwrap();
    assert!(
        writer
            .flush()
            .unwrap_err()
            .to_string()
            .contains("cannot change the shape")
    );
    assert_eq!(read_cpp(), before);
    let old = Table::open(opts.clone()).unwrap();
    assert_eq!(
        old.cell_accessor(0, "tiles").unwrap().value().unwrap(),
        Some(&array(&[2, 3], 1.0))
    );
    writer
        .row_accessor_mut()
        .set_cell(0, "tiles", array(&[2, 3], 3.0))
        .unwrap();
    writer.flush().unwrap();
    let after = read_cpp();
    assert_eq!(after[0][0], before[0][0]);
    assert_eq!(after[0][1].parse::<f32>().unwrap(), 18.0);
    let new = Table::open(opts).unwrap();
    assert_eq!(
        new.cell_accessor(0, "tiles").unwrap().value().unwrap(),
        Some(&array(&[2, 3], 3.0))
    );
}

#[test]
fn existing_cpp_standard_manager_name_survives_a_failed_flush_and_row_retry() {
    if !casacore_oracle_available() {
        eprintln!("skipping StandardStMan readback: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing");
    let fixture = CppTableFixture::SsmScalarPrimitives;
    TableOracle::table_write(fixture, &path).unwrap();
    assert_eq!(
        TableOracle::column_manager_name(&path, "col_i32").unwrap(),
        "SSM"
    );
    let opts = TableOptions::new(&path);
    let mut table = Table::open(opts.clone()).unwrap();
    let managers = table.data_manager_info().to_vec();
    assert_eq!(managers[0].name, "SSM");
    let files = ["table.dat", "table.f0", "table.info", "table.lock"];
    let initial = files_and_inodes(&path, &files);
    table.remove_rows(&[1]).unwrap();
    let control = path.join("table.dat");
    let Some(permissions) = make_read_only(&control) else {
        return;
    };
    assert!(table.flush().is_err());
    assert_eq!(files_and_inodes(&path, &files), initial);
    assert_eq!(Table::open(opts.clone()).unwrap().row_count(), 3);
    TableOracle::table_verify(fixture, &path).unwrap();
    std::fs::set_permissions(control, permissions).unwrap();

    table.flush().unwrap();
    let fresh = Table::open(opts).unwrap();
    assert_eq!(fresh.data_manager_info(), managers);
    assert_eq!(fresh.row_count(), 2);
    assert_eq!(
        TableOracle::column_manager_name(&path, "col_i32").unwrap(),
        "SSM"
    );
    let cpp = TaqlOracle::query(&path, "SELECT col_i32 FROM $1").unwrap();
    assert_eq!(cpp.grid.rows, [vec!["42"], vec!["0"]]);
    for (file, (inode, _)) in files.iter().zip(&initial).skip(1) {
        assert_eq!(path.join(file).metadata().unwrap().ino(), *inode, "{file}");
    }
}

#[test]
fn full_save_keeps_live_lock_and_manager_files_for_relocking_readers() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("mixed");
    let options = TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan);
    let bindings = HashMap::from([(
        "name".to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::StManAipsIO,
            tile_shape: None,
        },
    )]);
    build_lock_test_table()
        .save_with_bindings(options.clone(), &bindings)
        .unwrap();
    std::fs::write(path.join("table.f90"), b"auxiliary").unwrap();
    std::fs::write(path.join("table.flagversions"), b"auxiliary").unwrap();
    let identity = || {
        [
            ".",
            "table.lock",
            "table.f0",
            "table.f1",
            "table.f90",
            "table.flagversions",
        ]
        .map(|name| path.join(name).metadata().unwrap().ino())
    };
    let original = identity();
    let mut cpp = HeldCppReaderProcess::open(&path);
    assert_eq!(cpp.read_id(), 42);
    let mut rust =
        Table::open_with_lock(options.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(rust.lock(LockType::Read, 1).unwrap());
    assert_eq!(cell(&rust, "id"), ScalarValue::Int32(42));
    rust.unlock().unwrap();

    let mut save = Table::open(options.clone()).unwrap();
    set_id(&mut save, 52);
    save.row_accessor_mut()
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("after save".into())),
        )
        .unwrap();
    let competing_writer = TableWriteLock::acquire(&path, 1).unwrap();
    assert!(matches!(
        save.save_with_bindings(options.clone(), &bindings),
        Err(casa_tables::TableError::LockFailed { .. })
    ));
    competing_writer.release().unwrap();
    assert_eq!(cpp.read_id(), 42);

    let refused = save.save(TableOptions::new(&path)).unwrap_err();
    assert!(
        refused.to_string().contains("layout of an open table"),
        "{refused}"
    );
    assert_eq!(cpp.read_id(), 42);

    save.save_with_bindings(options.clone(), &bindings).unwrap();
    assert_eq!(identity(), original);
    assert_eq!(
        cpp.read_id(),
        52,
        "the original C++ bucket file must see the saved row"
    );
    assert!(rust.lock(LockType::Read, 1).unwrap());
    assert_eq!(cell(&rust, "id"), ScalarValue::Int32(52));
    assert_eq!(
        cell(&rust, "name"),
        ScalarValue::String("after save".into())
    );
    rust.unlock().unwrap();
    let inspection = Table::open(options).unwrap();
    assert_eq!(inspection.data_manager_info().len(), 2);
    assert_eq!(std::fs::read(path.join("table.f90")).unwrap(), b"auxiliary");
    assert_eq!(
        std::fs::read(path.join("table.flagversions")).unwrap(),
        b"auxiliary"
    );

    drop(cpp);
    drop(rust);
    save.save(TableOptions::new(&path))
        .expect("retry migration after closing reader");
    let mut cpp = HeldCppReaderProcess::open(&path);
    assert_eq!(cpp.read_id(), 52);
    assert_eq!(
        path.join("table.lock").metadata().unwrap().ino(),
        original[1]
    );
    assert!(!path.join("table.f1").exists());
    assert_eq!(std::fs::read(path.join("table.f90")).unwrap(), b"auxiliary");
}

#[test]
fn held_cpp_reader_relocks_and_reads_replaced_table_dat_keywords() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("keyword_table");
    let mut table = build_lock_test_table();
    table
        .keywords_mut()
        .upsert("revision", Value::Scalar(ScalarValue::Int32(1)));
    table.save(TableOptions::new(&path)).expect("initial save");
    let mut lock = TableWriteLock::acquire(&path, 1).expect("write lock");
    lock.record_write();
    lock.release().expect("publish the initial table");

    let mut reader = HeldCppReaderProcess::open(&path);
    assert_eq!(reader.read_keyword("revision"), 1);
    let mut writer = Table::open_with_lock(
        TableOptions::new(&path),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open writer");
    for revision in [2, 3, 4] {
        assert!(writer.lock(LockType::Write, 1).expect("write lock"));
        writer
            .keywords_mut()
            .upsert("revision", Value::Scalar(ScalarValue::Int32(revision)));
        writer.flush().expect("rewrite table.dat");
        writer.unlock().expect("publish the new revision");
        assert_eq!(reader.read_keyword("revision"), revision);
    }
    let mut reopened = HeldCppReaderProcess::open(&path);
    assert_eq!(reopened.read_keyword("revision"), 4);
    assert_eq!(reopened.read_id(), 42);
}

/// (a) A change flushed to disk and followed by a resync is announced when
/// the write lock is released, so the held C++ reader re-reads it.
#[test]
fn held_cpp_reader_sees_a_flushed_write_after_a_resync() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table = published_id_table(dir.path(), false);
    let (before, after, cpp_id) = write_under_a_held_cpp_reader(&table, |writer, _| {
        set_id(writer, 2);
        writer.flush().expect("flush");
        writer.resync().expect("resync");
    });
    assert_eq!(cpp_id, 2, "the held C++ reader kept its stale cache");
    assert_eq!(after, before + 1, "the flushed write was not published");
}

/// (b) Rows persisted in place by a write plan and followed by a resync are
/// announced when the write lock is released.
#[test]
fn held_cpp_reader_sees_selected_rows_saved_before_a_resync() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table = published_id_table(dir.path(), false);
    let (before, after, cpp_id) = write_under_a_held_cpp_reader(&table, |writer, _| {
        set_id(writer, 2);
        writer
            .prepare_write()
            .save_selected_rows(&["id"], &[0])
            .expect("save the selected row");
        writer.resync().expect("resync");
    });
    assert_eq!(cpp_id, 2, "the held C++ reader kept its stale cache");
    assert_eq!(after, before + 1, "the persisted rows were not published");
}

/// (c) A write that fails after persisting part of its change, and is then
/// discarded by a resync, still announces the part that reached the disk.
/// The second data manager's storage file is replaced by a directory, which
/// the in-place save cannot open after it has written the first manager, and
/// is restored before the resync.
#[test]
fn held_cpp_reader_sees_the_persisted_part_of_a_failed_write_after_a_resync() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table = published_id_table(dir.path(), true);
    let (before, after, cpp_id) = write_under_a_held_cpp_reader(&table, |writer, path| {
        set_id(writer, 2);
        writer
            .row_accessor_mut()
            .set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("changed".into())),
            )
            .expect("set name");
        let managers = writer.data_manager_info();
        let holding = |column: &str| {
            managers
                .iter()
                .position(|dm| dm.columns.iter().any(|name| name == column))
                .expect("a manager holds the column")
        };
        assert!(
            holding("id") < holding("name"),
            "the save must reach id's manager first: {managers:?}"
        );
        let storage = path.join(format!("table.f{}", managers[holding("name")].seq_nr));
        let hidden = path.join("name-storage.hidden");
        std::fs::rename(&storage, &hidden).expect("hide name's storage");
        std::fs::create_dir(&storage).expect("block it with a directory");
        std::fs::write(storage.join("blocker"), b"").expect("keep the directory non-empty");
        let saved = writer
            .prepare_write()
            .save_selected_rows(&["id", "name"], &[0]);
        std::fs::remove_dir_all(&storage).expect("remove the blocker");
        std::fs::rename(&hidden, &storage).expect("restore name's storage");
        assert!(saved.is_err(), "the blocked save succeeded");

        writer.resync().expect("resync");
        assert_eq!(
            cell(writer, "id"),
            ScalarValue::Int32(2),
            "id was not persisted"
        );
        assert_eq!(cell(writer, "name"), ScalarValue::String("initial".into()));
    });
    assert_eq!(cpp_id, 2, "the held C++ reader kept its stale cache");
    assert_eq!(after, before + 1, "the persisted part was not published");
}

/// Control: a write lock that changes nothing, and one whose change a resync
/// discards before it reaches the disk, publish nothing, as in casacore, and
/// the held C++ reader keeps the value it read.
#[test]
fn held_cpp_reader_sees_no_change_from_a_write_lock_that_wrote_nothing() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table = published_id_table(dir.path(), false);
    let (before, after, cpp_id) = write_under_a_held_cpp_reader(&table, |_, _| {});
    assert_eq!(after, before, "an unchanged write lock was published");
    assert_eq!(cpp_id, 1);

    let (before, after, cpp_id) = write_under_a_held_cpp_reader(&table, |writer, _| {
        set_id(writer, 2);
        writer.resync().expect("resync");
        assert_eq!(cell(writer, "id"), ScalarValue::Int32(1));
    });
    assert_eq!(after, before, "a discarded change was published");
    assert_eq!(cpp_id, 1);
}

/// casacore marks a table in use for as long as any handle has it open: a
/// read lock on the in-use byte of `table.lock` (`LockFile`'s
/// `itsUseLocker`, at `4*seqnr+1`). `table_holder`, which guards deleting
/// and renaming a flag version (#695), sees that mark while a C++ reader in
/// another process holds the table, and sees it gone once the reader
/// closes.
#[test]
fn a_table_casacore_holds_open_is_multi_used_until_it_closes() {
    if !casacore_oracle_available() {
        eprintln!("skipping held C++ reader test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let table = published_id_table(dir.path(), false);
    let holder = || casa_tables::table_holder(&table).expect("probe the in-use mark");
    let in_use = || holder() == Some(casa_tables::TableHolder::AnotherProcess);
    assert_eq!(holder(), None, "a closed table reads as held");

    let mut reader = HeldCppReaderProcess::open(&table);
    assert!(
        in_use(),
        "the table casacore holds open reads as not in use"
    );
    assert_eq!(reader.read_id(), 1);
    assert!(in_use(), "the mark went with the reader's lock period");

    drop(reader);
    assert_eq!(holder(), None, "the mark outlived the casacore reader");
}
