// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multi-process lock contention integration tests.
//!
//! These tests use the `lock_helper` example binary to verify that
//! fcntl-based table locking works correctly across OS processes.
//! Because fcntl locks are per-process (not per-fd), single-process
//! tests cannot exercise true contention.
#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use casacore_tables::{LockMode, LockOptions, LockType, Table, TableOptions};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// Build the lock_helper example binary and return the path to it.
fn helper_binary() -> PathBuf {
    // Build the example in the same target directory.
    let output = Command::new("cargo")
        .args(["build", "--example", "lock_helper", "-p", "casacore-tables"])
        .output()
        .expect("failed to run cargo build");
    assert!(
        output.status.success(),
        "Failed to build lock_helper: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Find the binary.
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // casacore-tables -> crates
    path.pop(); // crates -> repo root
    path.push("target");
    path.push("debug");
    path.push("examples");
    path.push("lock_helper");
    assert!(path.exists(), "lock_helper binary not found at {path:?}");
    path
}

/// Create a test table with one row on disk.
fn create_test_table(dir: &Path) -> TableOptions {
    let schema = casacore_tables::TableSchema::new(vec![
        casacore_tables::ColumnSchema::scalar("id", PrimitiveType::Int32),
        casacore_tables::ColumnSchema::scalar("name", PrimitiveType::String),
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
