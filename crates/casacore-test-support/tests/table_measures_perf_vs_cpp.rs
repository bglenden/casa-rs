// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rust-vs-C++ performance comparison for measure column read throughput.
//!
//! Use `cargo test --release` for meaningful ratios.

#![cfg(has_casacore_cpp)]

use casacore_tables::table_measures::*;
use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_test_support::table_measures_interop::*;
use casacore_types::*;
use std::time::Instant;

const NROWS: usize = 10_000;
const ITERATIONS: i32 = 10;

/// Create a table with `NROWS` rows of fixed-ref epoch measures.
fn create_epoch_bench_table(path: &str) {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC")
        .write(&mut table)
        .unwrap();

    for i in 0..NROWS {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "TIME",
                Value::Array(ArrayValue::from_f64_vec(vec![51544.5 + i as f64 * 0.001])),
            )]))
            .unwrap();
    }

    table
        .save(TableOptions::new(path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();
}

/// Create a table with `NROWS` rows of fixed-ref direction measures.
fn create_direction_bench_table(path: &str) {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "DIR",
        PrimitiveType::Float64,
        vec![2],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    TableMeasDesc::new_fixed("DIR", MeasureType::Direction, "J2000")
        .write(&mut table)
        .unwrap();

    for i in 0..NROWS {
        let lon = (i as f64) * 0.001;
        let lat = -0.5 + (i as f64) * 0.0001;
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DIR",
                Value::Array(ArrayValue::from_f64_vec(vec![lon, lat])),
            )]))
            .unwrap();
    }

    table
        .save(TableOptions::new(path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();
}

#[test]
fn epoch_read_throughput_vs_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_meas_epoch");
    let path_str = path.to_str().unwrap();

    create_epoch_bench_table(path_str);

    // ── C++ timing ──
    let cpp_ns =
        cpp_bench_epoch_read(path_str, "TIME", ITERATIONS).expect("C++ bench should succeed");

    // ── Rust timing ──
    let table = Table::open(TableOptions::new(path_str)).unwrap();
    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();

    // Warm up
    for r in 0..table.row_count() {
        std::hint::black_box(col.get_epoch(r).unwrap());
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for r in 0..table.row_count() {
            let e = col.get_epoch(r).unwrap();
            std::hint::black_box(e.value().as_mjd());
        }
    }
    let rust_ns = start.elapsed().as_nanos() as u64;

    let total_ops = (ITERATIONS as u64) * (NROWS as u64);
    let rust_per_op = rust_ns as f64 / total_ops as f64;
    let cpp_per_op = cpp_ns as f64 / total_ops as f64;
    let ratio = rust_per_op / cpp_per_op;

    eprintln!("── epoch measure read throughput ──");
    eprintln!("  rows:       {NROWS}");
    eprintln!("  iterations: {ITERATIONS}");
    eprintln!("  total ops:  {total_ops}");
    eprintln!("  Rust:       {rust_ns:>12} ns  ({rust_per_op:.1} ns/op)");
    eprintln!("  C++:        {cpp_ns:>12} ns  ({cpp_per_op:.1} ns/op)");
    eprintln!("  ratio:      {ratio:.2}x");

    if ratio > 2.0 {
        eprintln!(
            "  WARNING: Rust epoch measure read is {ratio:.1}x slower than C++ (threshold: 2.0x)"
        );
    }
}

#[test]
fn direction_read_throughput_vs_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_meas_direction");
    let path_str = path.to_str().unwrap();

    create_direction_bench_table(path_str);

    // ── C++ timing ──
    let cpp_ns =
        cpp_bench_direction_read(path_str, "DIR", ITERATIONS).expect("C++ bench should succeed");

    // ── Rust timing ──
    let table = Table::open(TableOptions::new(path_str)).unwrap();
    let col = ScalarMeasColumn::new(&table, "DIR").unwrap();

    // Warm up
    for r in 0..table.row_count() {
        std::hint::black_box(col.get_direction(r).unwrap());
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for r in 0..table.row_count() {
            let d = col.get_direction(r).unwrap();
            std::hint::black_box(d.as_angles());
        }
    }
    let rust_ns = start.elapsed().as_nanos() as u64;

    let total_ops = (ITERATIONS as u64) * (NROWS as u64);
    let rust_per_op = rust_ns as f64 / total_ops as f64;
    let cpp_per_op = cpp_ns as f64 / total_ops as f64;
    let ratio = rust_per_op / cpp_per_op;

    eprintln!("── direction measure read throughput ──");
    eprintln!("  rows:       {NROWS}");
    eprintln!("  iterations: {ITERATIONS}");
    eprintln!("  total ops:  {total_ops}");
    eprintln!("  Rust:       {rust_ns:>12} ns  ({rust_per_op:.1} ns/op)");
    eprintln!("  C++:        {cpp_ns:>12} ns  ({cpp_per_op:.1} ns/op)");
    eprintln!("  ratio:      {ratio:.2}x");

    if ratio > 2.0 {
        eprintln!(
            "  WARNING: Rust direction measure read is {ratio:.1}x slower than C++ (threshold: 2.0x)"
        );
    }
}
