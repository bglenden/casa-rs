// SPDX-License-Identifier: LGPL-3.0-or-later
//! A C++ casacore reader that holds a table open between its lock periods,
//! run in its own process so that a writer under test in another process
//! does not share its `fcntl` locks.
//!
//! Usage: `casacore-held-table-reader <table_dir>`. The table must have an
//! Int column "id". The reader opens the table with `UserLocking` and prints
//! `ready`; then, for each `read` line on stdin, it takes a read lock, reads
//! `id(0)`, releases the lock without closing the table and prints
//! `id <value>`. It closes the table and exits at end of input. A failure is
//! printed as `error <message>` and ends the process with status 1.

use std::io::{BufRead, Write};
use std::process::ExitCode;

use casa_test_support::HeldCppTableReader;

fn fail(message: impl std::fmt::Display) -> ExitCode {
    println!("error {message}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let (Some(table_dir), None) = (args.next(), args.next()) else {
        eprintln!("usage: casacore-held-table-reader <table_dir>");
        return ExitCode::from(2);
    };
    let mut reader = match HeldCppTableReader::open(std::path::Path::new(&table_dir)) {
        Ok(reader) => reader,
        Err(error) => return fail(error),
    };
    println!("ready");
    for line in std::io::stdin().lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => return fail(error),
        };
        match line.trim() {
            "read" => match reader.read_id() {
                Ok(id) => println!("id {id}"),
                Err(error) => return fail(error),
            },
            other => return fail(format!("unknown command {other:?}")),
        }
        if let Err(error) = std::io::stdout().flush() {
            return fail(error);
        }
    }
    ExitCode::SUCCESS
}
