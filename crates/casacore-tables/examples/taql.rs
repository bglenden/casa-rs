// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL command-line executor.
//!
//! A minimal CLI for executing TaQL queries against casacore tables.
//!
//! # Usage
//!
//! ```text
//! # Single command from arguments:
//! cargo run --example taql -- /path/to/table "SELECT col1, col2 WHERE flux > 1.0"
//!
//! # Interactive mode reading from stdin:
//! cargo run --example taql -- /path/to/table
//! ```
//!
//! # C++ equivalent
//!
//! `taql` / `tableCommand()` in casacore.

use std::io::{self, BufRead, Write};

use casacore_tables::Table;
use casacore_tables::taql::{self, format as taql_fmt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: taql <table-path> [query]");
        eprintln!();
        eprintln!("  table-path  Path to a casacore table directory");
        eprintln!("  query       TaQL query string (omit for interactive stdin mode)");
        std::process::exit(1);
    }

    let table_path = &args[1];
    let mut table = Table::open(casacore_tables::TableOptions::new(table_path))?;

    if args.len() >= 3 {
        // Single command mode: execute the query from the argument.
        let query = args[2..].join(" ");
        execute_and_print(&mut table, &query)?;
    } else {
        // Interactive stdin mode: read queries line by line.
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut stdout = stdout.lock();

        write!(stdout, "taql> ")?;
        stdout.flush()?;

        for line in stdin.lock().lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed == "exit" || trimmed == "quit" {
                if trimmed == "exit" || trimmed == "quit" {
                    break;
                }
                write!(stdout, "taql> ")?;
                stdout.flush()?;
                continue;
            }

            if let Err(e) = execute_and_print(&mut table, trimmed) {
                eprintln!("Error: {e}");
            }

            write!(stdout, "taql> ")?;
            stdout.flush()?;
        }
    }

    Ok(())
}

/// Execute a single TaQL query and print the result.
fn execute_and_print(table: &mut Table, query: &str) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = taql::parse(query)?;

    match &stmt {
        taql::Statement::Select(_) => {
            let view = table.query(query)?;
            let columns = view.column_names().to_vec();
            let rows: Vec<_> = (0..view.row_count())
                .filter_map(|i| view.row(i).cloned())
                .collect();
            print!("{}", taql_fmt::format_rows(&columns, &rows));
        }
        _ => {
            let result = taql::execute(&stmt, table)?;
            println!("{}", taql_fmt::format_result(&result));
        }
    }
    Ok(())
}
