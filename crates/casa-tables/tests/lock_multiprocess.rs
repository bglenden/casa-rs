// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multi-process lock contention integration tests.
//!
//! These tests use the `lock_helper` helper binary to verify that
//! fcntl-based table locking works correctly across OS processes.
//! Because fcntl locks are per-process (not per-fd), single-process
//! tests cannot exercise true contention.
#![cfg(unix)]
// These tests spawn a helper binary and communicate via signal files.
// The helper is a package bin target (`src/bin/lock_helper.rs`), so Cargo
// builds it for tests and exposes `CARGO_BIN_EXE_lock_helper`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use casa_tables::{
    LockMode, LockOptions, LockType, Table, TableError, TableOptions, TableWriteLock,
};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Locate the test-built lock_helper binary.
fn helper_binary() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_lock_helper"));
    assert!(path.exists(), "lock_helper binary not found at {path:?}");
    path
}

/// Create a test table with one row on disk.
fn create_test_table(dir: &Path) -> TableOptions {
    let schema = casa_tables::TableSchema::new(vec![
        casa_tables::ColumnSchema::scalar("id", PrimitiveType::Int32),
        casa_tables::ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("initial".into()))),
        ]))
        .unwrap();
    let opts = TableOptions::new(dir.join("test.tbl"));
    table.save(opts.clone()).unwrap();
    // Create the lock file so subsequent opens don't need create=true.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let t = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
    drop(t);
    opts
}

/// Wait for a file to appear on disk, with a timeout.
fn wait_for_file(path: &Path, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn write_lock_contention_across_processes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();

    let signal_file = tmp.path().join("locked.signal");
    let wait_file = tmp.path().join("release.signal");

    // Process A: hold write lock.
    let mut proc_a = Command::new(&helper)
        .args([
            table_dir,
            "hold_write_lock",
            signal_file.to_str().unwrap(),
            wait_file.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to spawn process A");

    // Wait for process A to signal that it holds the lock.
    assert!(
        wait_for_file(&signal_file, Duration::from_secs(10)),
        "Process A did not signal lock acquisition"
    );

    // Process B: try to acquire write lock — should fail.
    let output_b = Command::new(&helper)
        .args([table_dir, "try_write_lock"])
        .output()
        .expect("failed to spawn process B");

    assert!(
        !output_b.status.success(),
        "Process B should NOT acquire write lock while A holds it. \
         stderr: {}",
        String::from_utf8_lossy(&output_b.stderr)
    );

    // Tell process A to release.
    fs::write(&wait_file, "release").unwrap();
    let status_a = proc_a.wait().expect("process A wait failed");
    assert!(status_a.success(), "Process A should exit cleanly");

    // Process B retries: should succeed now.
    let output_b2 = Command::new(&helper)
        .args([table_dir, "try_write_lock"])
        .output()
        .expect("failed to spawn process B retry");

    assert!(
        output_b2.status.success(),
        "Process B should acquire write lock after A releases. \
         stderr: {}",
        String::from_utf8_lossy(&output_b2.stderr)
    );
}

/// The big-endian `Int` at `offset` in the lock file's request list.
fn request_list_int(bytes: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

/// The process ids in the request list of `table`'s `table.lock`, read as
/// casacore's `LockFile` reads it: a big-endian count, then `(pid, hostid)`
/// pairs. A casacore holder using AutoLocking releases its lock when the
/// count is not zero.
///
/// Reading through a separate descriptor and closing it drops every `fcntl`
/// lock this process holds on the file, so it is called only while this
/// process waits for the main lock and before it is told to stop waiting, or
/// after it has released the lock again.
fn requesting_pids(table: &Path) -> Vec<i32> {
    let bytes = fs::read(table.join("table.lock")).unwrap_or_default();
    if bytes.len() < 4 {
        return Vec::new();
    }
    let count = request_list_int(&bytes, 0).clamp(0, 32) as usize;
    (0..count)
        .map(|slot| 4 + 8 * slot)
        .take_while(|offset| offset + 4 <= bytes.len())
        .map(|offset| request_list_int(&bytes, offset))
        .collect()
}

/// Wait until this process is in the request list of `table`'s lock file.
fn wait_for_request_from_this_process(table: &Path, timeout: Duration) -> bool {
    let pid = std::process::id() as i32;
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if requesting_pids(table).contains(&pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

/// An auto-locked open waits, as casacore's does, for the read lock while
/// another process holds the write lock, registered in the request list, and
/// reads the table's metadata only once it holds the lock: `table.dat` is
/// missing until the writer releases.
#[test]
fn auto_locked_open_waits_for_the_read_lock_before_loading_metadata() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let locked_file = tmp.path().join("writer-locked.signal");
    let release_file = tmp.path().join("writer-release.signal");

    let mut writer = Command::new(&helper)
        .args([
            table_dir,
            "hold_write_lock",
            locked_file.to_str().unwrap(),
            release_file.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to spawn lock holder");
    assert!(
        wait_for_file(&locked_file, Duration::from_secs(10)),
        "writer did not acquire its lock"
    );

    let table_dat = opts.path().join("table.dat");
    let hidden_table_dat = opts.path().join("table.dat.hidden-by-lock-order-test");
    fs::rename(&table_dat, &hidden_table_dat).unwrap();
    let table_path = opts.path().to_path_buf();
    let releaser = thread::spawn(move || {
        let requested = wait_for_request_from_this_process(&table_path, Duration::from_secs(8));
        fs::rename(&hidden_table_dat, &table_dat).unwrap();
        fs::write(&release_file, "release").unwrap();
        requested
    });
    let result = Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::AutoLocking));

    assert!(
        releaser.join().unwrap(),
        "the waiting open did not register in the request list"
    );
    let status = writer.wait().expect("lock holder wait failed");
    assert!(status.success(), "lock holder should exit cleanly");
    let table = result.expect("the open waits for the writer and then reads the metadata");
    assert_eq!(table.row_count(), 1);
    drop(table);
    assert!(requesting_pids(opts.path()).is_empty());
}

/// A lock another process holds is waited for by an in-place writer, as
/// casacore waits: the waiter's process id is in the request list, where a
/// casacore holder using AutoLocking sees it and releases, and it is removed
/// once the lock is acquired. The holder is an idle reader, as a casacore
/// session holding the table open is.
#[test]
fn table_write_lock_waits_for_another_process_in_the_request_list() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let locked_file = tmp.path().join("holder-locked.signal");
    let release_file = tmp.path().join("holder-release.signal");

    let mut holder = Command::new(&helper)
        .args([
            table_dir,
            "hold_read_lock",
            locked_file.to_str().unwrap(),
            release_file.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to spawn lock holder");
    assert!(
        wait_for_file(&locked_file, Duration::from_secs(10)),
        "holder did not acquire its lock"
    );

    let table_path = opts.path().to_path_buf();
    let waiter = thread::spawn(move || TableWriteLock::acquire(&table_path, 0));
    let requested = wait_for_request_from_this_process(opts.path(), Duration::from_secs(8));
    fs::write(&release_file, "release").unwrap();
    let lock = waiter.join().unwrap();
    let status = holder.wait().expect("lock holder wait failed");

    assert!(status.success(), "lock holder should exit cleanly");
    assert!(
        requested,
        "the waiting writer did not register in the request list"
    );
    let lock = lock.expect("the writer waits for the other process and then takes the lock");
    lock.release().unwrap();
    assert!(
        requesting_pids(opts.path()).is_empty(),
        "the request is removed once the lock is acquired"
    );
}

/// A holder that takes the write lock and changes nothing publishes nothing,
/// as in casacore, so a writer that waited for it is not refused as stale.
#[test]
fn table_write_lock_admits_a_writer_after_a_holder_that_wrote_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let locked_file = tmp.path().join("holder-locked.signal");
    let release_file = tmp.path().join("holder-release.signal");

    let mut holder = Command::new(&helper)
        .args([
            table_dir,
            "hold_write_lock",
            locked_file.to_str().unwrap(),
            release_file.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to spawn lock holder");
    assert!(
        wait_for_file(&locked_file, Duration::from_secs(10)),
        "holder did not acquire its lock"
    );

    let table_path = opts.path().to_path_buf();
    let waiter = thread::spawn(move || TableWriteLock::acquire(&table_path, 0));
    let requested = wait_for_request_from_this_process(opts.path(), Duration::from_secs(8));
    fs::write(&release_file, "release").unwrap();
    let lock = waiter.join().unwrap();
    let status = holder.wait().expect("lock holder wait failed");

    assert!(status.success(), "lock holder should exit cleanly");
    assert!(requested, "the waiting writer did not register");
    lock.expect("a holder that wrote nothing does not make the waiting writer stale")
        .release()
        .unwrap();
}

/// A writer that waited is refused when the holder wrote the table in the
/// meantime: what it read before waiting is stale.
#[test]
fn table_write_lock_refuses_a_table_written_while_it_waited() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let staged_file = tmp.path().join("row-staged.signal");
    let publish_file = tmp.path().join("publish.signal");

    let mut writer = Command::new(&helper)
        .args([
            table_dir,
            "hold_write_with_row",
            "42",
            "published",
            staged_file.to_str().unwrap(),
            publish_file.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to spawn staged writer");
    assert!(
        wait_for_file(&staged_file, Duration::from_secs(10)),
        "writer did not stage the new row while holding its lock"
    );

    let table_path = opts.path().to_path_buf();
    let waiter = thread::spawn(move || TableWriteLock::acquire(&table_path, 0));
    let requested = wait_for_request_from_this_process(opts.path(), Duration::from_secs(8));
    fs::write(&publish_file, "publish").unwrap();
    let result = waiter.join().unwrap();
    let status = writer.wait().expect("staged writer wait failed");

    assert!(status.success(), "staged writer should exit cleanly");
    assert!(requested, "the waiting writer did not register");
    match result {
        Err(TableError::LockFailed { message, .. }) => {
            assert!(message.contains("wrote the table while"), "{message}");
        }
        other => panic!("a table written during the wait must be refused: {other:?}"),
    }
    // The refused writer released the lock.
    assert!(TableWriteLock::acquire(opts.path(), 1).is_ok());
}

/// Wait for `child` until `deadline`; kill it and return `None` after that.
fn output_before(mut child: std::process::Child, deadline: std::time::Instant) -> Option<String> {
    while std::time::Instant::now() < deadline {
        if child.try_wait().expect("poll the helper").is_some() {
            let output = child.wait_with_output().expect("helper output");
            return Some(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

/// Two processes that each hold a read lock and wait to upgrade it to a
/// write lock would wait for each other forever. The waits block in the
/// kernel, as casacore's do, so the kernel refuses the request that closes
/// the cycle (`EDEADLK`); that process gives up and releases its read lock,
/// and the other then acquires the write lock.
#[test]
fn competing_read_lock_upgrades_do_not_deadlock() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let go_file = tmp.path().join("go.signal");

    let upgraders = [2, 3].map(|id| {
        let ready_file = tmp.path().join(format!("ready-{id}.signal"));
        let child = Command::new(&helper)
            .args([
                table_dir,
                "auto_locked_write_row",
                &id.to_string(),
                ready_file.to_str().unwrap(),
                go_file.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("failed to spawn upgrader");
        assert!(
            wait_for_file(&ready_file, Duration::from_secs(10)),
            "upgrader {id} did not take its read lock"
        );
        child
    });
    fs::write(&go_file, "go").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let outputs = upgraders.map(|child| output_before(child, deadline));
    let outputs: Vec<String> = outputs
        .into_iter()
        .map(|output| output.expect("competing upgrades deadlocked"))
        .collect();

    assert_eq!(
        outputs
            .iter()
            .filter(|out| out.contains("acquired"))
            .count(),
        1,
        "{outputs:?}"
    );
    assert!(
        outputs
            .iter()
            .any(|out| out.contains("refused") && out.contains("would deadlock")),
        "{outputs:?}"
    );
    let mut table = Table::open_with_lock(opts, LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(table.lock(LockType::Read, 1).unwrap());
    assert_eq!(table.row_count(), 2, "the winner's row is written");
}

/// Requesting a read lock while holding the write lock keeps the write lock,
/// as casacore's `FileLocker` does: another process still cannot read-lock
/// the table, and the change made under the write lock is written when the
/// table is unlocked.
#[test]
fn a_read_request_keeps_the_write_lock_and_its_changes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let another_process_read_locks = || {
        Command::new(&helper)
            .args([table_dir, "try_read_lock"])
            .status()
            .expect("failed to spawn lock probe")
            .success()
    };

    let mut table =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(table.lock(LockType::Write, 1).unwrap());
    table
        .row_accessor_mut()
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("edited".into())),
        )
        .unwrap();
    assert!(table.lock(LockType::Read, 1).unwrap());
    assert!(
        table.has_lock(LockType::Write),
        "the write lock was dropped"
    );
    assert!(
        !another_process_read_locks(),
        "another process read-locked the table while it was write-locked"
    );

    table.unlock().unwrap();
    drop(table);
    assert!(another_process_read_locks());
    let reopened = Table::open(opts).unwrap();
    assert_eq!(
        reopened
            .cell_accessor(0, "name")
            .and_then(|cell| cell.scalar())
            .unwrap(),
        &ScalarValue::String("edited".into()),
        "the change made under the write lock was lost"
    );
}

#[test]
fn cross_process_write_then_read() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();

    // Process writes a new row.
    let output = Command::new(&helper)
        .args([table_dir, "write_row", "42", "from_child"])
        .output()
        .expect("failed to spawn write process");
    assert!(
        output.status.success(),
        "write_row failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Another process reads row count.
    let output = Command::new(&helper)
        .args([table_dir, "read_row_count"])
        .output()
        .expect("failed to spawn read process");
    assert!(
        output.status.success(),
        "read_row_count failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let count: usize = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap();
    assert_eq!(count, 2, "child should see the row written by sibling");

    // Also verify from Rust in this process.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
    table.lock(LockType::Read, 1).unwrap();
    assert_eq!(table.row_count(), 2);
}

#[test]
fn waiting_locked_open_reads_metadata_published_before_acquisition() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let staged_file = tmp.path().join("row-staged.signal");
    let publish_file = tmp.path().join("publish.signal");

    let mut writer = Command::new(&helper)
        .args([
            table_dir,
            "hold_write_with_row",
            "42",
            "published",
            staged_file.to_str().unwrap(),
            publish_file.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to spawn staged writer");
    assert!(
        wait_for_file(&staged_file, Duration::from_secs(10)),
        "writer did not stage the new row while holding its lock"
    );

    let publish_file_for_thread = publish_file.clone();
    let publisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        fs::write(publish_file_for_thread, "publish").unwrap();
    });
    let table = Table::open_with_lock(opts, LockOptions::new(LockMode::PermanentLockingWait))
        .expect("waiting open should acquire after the writer publishes");

    publisher.join().unwrap();
    let status = writer.wait().expect("staged writer wait failed");
    assert!(status.success(), "staged writer should exit cleanly");
    assert_eq!(
        table.row_count(),
        2,
        "metadata must be loaded after lock acquisition, not retained from before the writer published"
    );
}

#[test]
fn sequential_writes_from_multiple_processes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();

    // Three processes write rows sequentially.
    for i in 2..=4 {
        let output = Command::new(&helper)
            .args([table_dir, "write_row", &i.to_string(), &format!("proc_{i}")])
            .output()
            .expect("failed to spawn write process");
        assert!(
            output.status.success(),
            "write_row {i} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Verify final row count.
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
    table.lock(LockType::Read, 1).unwrap();
    assert_eq!(table.row_count(), 4);
}

/// The modify counter another process would see in the table's sync data.
fn published_modify_counter(opts: &TableOptions) -> u32 {
    let mut table =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(table.lock(LockType::Read, 1).unwrap());
    table.locked_modify_counter().unwrap()
}

/// fcntl locks belong to the process and closing any descriptor on a file
/// drops them all. A write lock held through one handle must nevertheless
/// survive other handles on the same table opening and closing in this
/// process, refuse those handles, and exclude other processes until it is
/// released; releasing it publishes the write in the sync data.
#[test]
fn table_write_lock_survives_other_handles_in_this_process() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = create_test_table(tmp.path());
    let helper = helper_binary();
    let table_dir = opts.path().to_str().unwrap();
    let another_process_takes_the_write_lock = || {
        Command::new(&helper)
            .args([table_dir, "try_write_lock"])
            .output()
            .expect("failed to spawn lock probe")
            .status
            .success()
    };
    let counter_before = published_modify_counter(&opts);

    let mut lock = TableWriteLock::acquire(opts.path(), 1).expect("take the write lock");
    assert!(matches!(
        TableWriteLock::acquire(opts.path(), 1),
        Err(TableError::LockFailed { .. })
    ));
    // A holder in this process is never waited for, even when the request
    // would wait indefinitely for another process.
    let (sender, receiver) = std::sync::mpsc::channel();
    let table_path = opts.path().to_path_buf();
    thread::spawn(move || {
        let _ = sender.send(TableWriteLock::acquire(&table_path, 0).map(drop));
    });
    match receiver.recv_timeout(Duration::from_secs(10)) {
        Ok(Err(TableError::LockFailed { message, .. })) => {
            assert!(message.contains("in this process"), "{message}");
        }
        Ok(other) => panic!("a holder in this process must be refused: {other:?}"),
        Err(_) => panic!("a holder in this process was waited for"),
    }
    drop(Table::open(opts.clone()).unwrap());
    let mut user_locked =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(!user_locked.lock(LockType::Write, 1).unwrap());
    assert!(user_locked.lock(LockType::Read, 1).unwrap());
    drop(user_locked);
    drop(Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::AutoLocking)).unwrap());
    assert!(
        !another_process_takes_the_write_lock(),
        "another process took the write lock while this process held it"
    );

    lock.record_write(&Table::open(opts.clone()).unwrap());
    lock.release().unwrap();
    assert_eq!(published_modify_counter(&opts), counter_before + 1);
    assert!(another_process_takes_the_write_lock());
}
