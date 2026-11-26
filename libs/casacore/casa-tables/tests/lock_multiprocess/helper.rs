// SPDX-License-Identifier: LGPL-3.0-or-later
//! Self-spawned child command runner for multi-process lock contention tests.

use std::fs;
use std::io::{BufRead, Write};
use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;

use casa_core::{RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{LockMode, LockOptions, LockType, Table, TableOptions};

pub(super) const REPLY_PREFIX: &str = "LOCK_HELPER:";

pub(super) fn run(args: &[String]) -> ! {
    if args.len() < 2 {
        eprintln!("Usage: lock_helper <table_dir> <command> [args...]");
        process::exit(2);
    }

    let table_dir = &args[0];
    let command = &args[1];
    let opts = TableOptions::new(table_dir);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    match command.as_str() {
        "hold_write_lock" => hold_lock(opts, lock_opts, LockType::Write, &args[2..]),
        "hold_read_lock" => hold_lock(opts, lock_opts, LockType::Read, &args[2..]),
        "try_write_lock" => try_lock(opts, lock_opts, LockType::Write),
        "try_read_lock" => try_lock(opts, lock_opts, LockType::Read),
        "write_row" => write_row(opts, lock_opts, &args[2..]),
        "hold_write_with_row" => hold_write_with_row(opts, lock_opts, &args[2..]),
        "read_row_count" => read_row_count(opts, lock_opts),
        "auto_locked_write_row" => auto_locked_write_row(opts, &args[2..]),
        "held_reader" => held_reader(opts, lock_opts),
        _ => {
            eprintln!("Unknown command: {command}");
            process::exit(2);
        }
    }
    process::exit(0);
}

fn reply(text: impl std::fmt::Display) {
    println!("{REPLY_PREFIX}{text}");
    let _ = std::io::stdout().flush();
}

fn hold_lock(opts: TableOptions, lock_opts: LockOptions, lock_type: LockType, args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: hold_lock <signal_file> <wait_file>");
        process::exit(2);
    }
    let signal_file = &args[0];
    let wait_file = &args[1];

    let mut table = open_or_exit(opts, lock_opts);
    table.lock(lock_type, 1).unwrap_or_else(|e| {
        eprintln!("lock failed: {e}");
        process::exit(3);
    });

    fs::write(signal_file, "locked").unwrap();
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

fn try_lock(opts: TableOptions, lock_opts: LockOptions, lock_type: LockType) -> ! {
    let mut table = open_or_exit(opts, lock_opts);
    let acquired = table.lock(lock_type, 1).unwrap_or_else(|e| {
        eprintln!("lock failed: {e}");
        process::exit(3);
    });
    if acquired {
        table.unlock().unwrap();
        process::exit(0);
    }
    process::exit(1);
}

fn write_row(opts: TableOptions, lock_opts: LockOptions, args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: write_row <id> <name>");
        process::exit(2);
    }
    let id: i32 = args[0].parse().unwrap();
    let name = &args[1];

    let mut table = open_or_exit(opts, lock_opts);
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

fn hold_write_with_row(opts: TableOptions, lock_opts: LockOptions, args: &[String]) {
    if args.len() < 4 {
        eprintln!("Usage: hold_write_with_row <id> <name> <signal_file> <wait_file>");
        process::exit(2);
    }
    let id: i32 = args[0].parse().unwrap();
    let name = &args[1];
    let signal_file = &args[2];
    let wait_file = &args[3];

    let mut table = open_or_exit(opts, lock_opts);
    table.lock(LockType::Write, 1).unwrap_or_else(|e| {
        eprintln!("lock failed: {e}");
        process::exit(3);
    });
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
            RecordField::new("name", Value::Scalar(ScalarValue::String(name.to_string()))),
        ]))
        .unwrap();
    fs::write(signal_file, "row staged").unwrap();

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

fn read_row_count(opts: TableOptions, lock_opts: LockOptions) {
    let mut table = open_or_exit(opts, lock_opts);
    table.lock(LockType::Read, 1).unwrap_or_else(|e| {
        eprintln!("lock failed: {e}");
        process::exit(3);
    });
    reply(table.row_count());
    table.unlock().unwrap_or_else(|e| {
        eprintln!("unlock failed: {e}");
        process::exit(3);
    });
}

fn auto_locked_write_row(opts: TableOptions, args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: auto_locked_write_row <id> <ready_file> <go_file>");
        process::exit(2);
    }
    let id: i32 = args[0].parse().unwrap();
    let ready_file = &args[1];
    let go_file = &args[2];

    let mut table = open_or_exit(opts, LockOptions::new(LockMode::AutoLocking));
    fs::write(ready_file, "read-locked").unwrap();
    for _ in 0..200 {
        if Path::new(go_file).exists() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    match table.add_row(RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
        RecordField::new("name", Value::Scalar(ScalarValue::String("upgrade".into()))),
    ])) {
        Ok(()) => reply("acquired"),
        Err(error) => reply(format!("refused: {error}")),
    }
}

fn held_reader(opts: TableOptions, lock_opts: LockOptions) {
    let mut table = open_or_exit(opts, lock_opts);
    reply("ready");
    serve_held_reader(&mut table);
}

fn open_or_exit(opts: TableOptions, lock_opts: LockOptions) -> Table {
    Table::open_with_lock(opts, lock_opts).unwrap_or_else(|e| {
        eprintln!("open_with_lock failed: {e}");
        process::exit(3);
    })
}

fn serve_held_reader(table: &mut Table) {
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap_or_else(|e| {
            eprintln!("read stdin failed: {e}");
            process::exit(3);
        });
        if line.trim() != "read" {
            eprintln!("unknown held_reader request {line:?}");
            process::exit(2);
        }
        match table.lock(LockType::Read, 1) {
            Ok(true) => {}
            Ok(false) => {
                eprintln!("read lock was not acquired");
                process::exit(3);
            }
            Err(e) => {
                eprintln!("lock failed: {e}");
                process::exit(3);
            }
        }
        let id = match table.cell_accessor(0, "id").and_then(|cell| cell.scalar()) {
            Ok(ScalarValue::Int32(id)) => *id,
            other => {
                eprintln!("row 0 has no Int32 id: {other:?}");
                process::exit(3);
            }
        };
        let counter = table.locked_modify_counter().unwrap_or_else(|e| {
            eprintln!("modify counter failed: {e}");
            process::exit(3);
        });
        table.unlock().unwrap_or_else(|e| {
            eprintln!("unlock failed: {e}");
            process::exit(3);
        });
        reply(format!("id {id} counter {counter}"));
    }
}
