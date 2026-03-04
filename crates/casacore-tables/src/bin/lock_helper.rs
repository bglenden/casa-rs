// SPDX-License-Identifier: LGPL-3.0-or-later
//! Helper binary for multi-process lock contention tests.
//!
//! Usage:
//!   lock_helper <table_dir> <command> [args...]
//!
//! Commands:
//!   hold_write_lock <signal_file> <wait_file>
//!     Opens the table with UserLocking, acquires a write lock,
//!     creates `signal_file` to indicate readiness, waits for `wait_file`
//!     to appear, then unlocks and exits.
//!
//!   try_write_lock
//!     Opens the table with UserLocking, tries to acquire a write lock
//!     (nattempts=1). Exits 0 if acquired, exits 1 if not.
//!
//!   write_row <id> <name>
//!     Opens with UserLocking, acquires write lock, adds a row, unlocks.
//!     Exits 0 on success.
//!
//!   read_row_count
//!     Opens with UserLocking, acquires read lock, prints row count to
//!     stdout, unlocks. Exits 0 on success.
use std::env;
use std::fs;
use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;

use casacore_tables::{LockMode, LockOptions, LockType, Table, TableOptions};
use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: lock_helper <table_dir> <command> [args...]");
        process::exit(2);
    }

    let table_dir = &args[1];
    let command = &args[2];
    let opts = TableOptions::new(table_dir);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    match command.as_str() {
        "hold_write_lock" => {
            if args.len() < 5 {
                eprintln!(
                    "Usage: lock_helper <table_dir> hold_write_lock <signal_file> <wait_file>"
                );
                process::exit(2);
            }
            let signal_file = &args[3];
            let wait_file = &args[4];

            let mut table = Table::open_with_lock(opts, lock_opts).unwrap_or_else(|e| {
                eprintln!("open_with_lock failed: {e}");
                process::exit(3);
            });
            table.lock(LockType::Write, 1).unwrap_or_else(|e| {
                eprintln!("lock failed: {e}");
                process::exit(3);
            });

            // Signal that we hold the lock.
            fs::write(signal_file, "locked").unwrap();

            // Wait for the test to tell us to release.
            for _ in 0..100 {
                if Path::new(wait_file).exists() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }

            table.unlock().unwrap_or_else(|e| {
                eprintln!("unlock failed: {e}");
                process::exit(3);
            });
        }

        "try_write_lock" => {
            let mut table = Table::open_with_lock(opts, lock_opts).unwrap_or_else(|e| {
                eprintln!("open_with_lock failed: {e}");
                process::exit(3);
            });
            let acquired = table.lock(LockType::Write, 1).unwrap_or_else(|e| {
                eprintln!("lock failed: {e}");
                process::exit(3);
            });
            if acquired {
                table.unlock().unwrap();
                process::exit(0);
            } else {
                process::exit(1);
            }
        }

        "write_row" => {
            if args.len() < 5 {
                eprintln!("Usage: lock_helper <table_dir> write_row <id> <name>");
                process::exit(2);
            }
            let id: i32 = args[3].parse().unwrap();
            let name = &args[4];

            let mut table = Table::open_with_lock(opts, lock_opts).unwrap_or_else(|e| {
                eprintln!("open_with_lock failed: {e}");
                process::exit(3);
            });
            table.lock(LockType::Write, 0).unwrap_or_else(|e| {
                eprintln!("lock failed: {e}");
                process::exit(3);
            });
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                    RecordField::new("name", Value::Scalar(ScalarValue::String(name.to_string()))),
                ]))
                .unwrap();
            table.unlock().unwrap_or_else(|e| {
                eprintln!("unlock failed: {e}");
                process::exit(3);
            });
        }

        "read_row_count" => {
            let mut table = Table::open_with_lock(opts, lock_opts).unwrap_or_else(|e| {
                eprintln!("open_with_lock failed: {e}");
                process::exit(3);
            });
            table.lock(LockType::Read, 1).unwrap_or_else(|e| {
                eprintln!("lock failed: {e}");
                process::exit(3);
            });
            println!("{}", table.row_count());
            table.unlock().unwrap_or_else(|e| {
                eprintln!("unlock failed: {e}");
                process::exit(3);
            });
        }

        _ => {
            eprintln!("Unknown command: {command}");
            process::exit(2);
        }
    }
}
