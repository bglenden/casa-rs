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
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

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
        let stdin = self.stdin.as_mut().expect("reader stdin");
        writeln!(stdin, "read").expect("send read");
        stdin.flush().expect("flush read");
        let reply = self.reply();
        reply
            .strip_prefix("id ")
            .and_then(|id| id.parse().ok())
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
