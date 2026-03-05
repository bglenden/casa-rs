// SPDX-License-Identifier: LGPL-3.0-or-later
//! CLI utility that prints structure and keyword information for a casacore
//! table on disk.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p casacore-tables --example showtableinfo -- /path/to/table
//! ```
//!
//! # C++ equivalent
//!
//! The C++ casacore distribution ships a `showtableinfo` command-line tool.
//! This example provides equivalent functionality using the Rust API.

use casacore_tables::{Table, TableOptions};
use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: showtableinfo <table-path>");
        process::exit(1);
    }

    let path = &args[1];
    let table = match Table::open(TableOptions::new(path)) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error opening table {path}: {e}");
            process::exit(1);
        }
    };

    print!("{}", table.show_structure());
    println!();
    print!("{}", table.show_keywords());
}
