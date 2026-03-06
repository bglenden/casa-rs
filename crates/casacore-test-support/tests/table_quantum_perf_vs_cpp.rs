// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rust-vs-C++ performance comparison for quantum column read throughput.
//!
//! Use `cargo test --release` for meaningful ratios.

#![cfg(has_casacore_cpp)]

use casacore_tables::table_quantum::{ArrayQuantColumn, ScalarQuantColumn, TableQuantumDesc};
use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_test_support::table_quantum_interop::{cpp_bench_array_read, cpp_bench_scalar_read};
use casacore_types::*;
use std::time::Instant;

const NROWS: usize = 10_000;
const ITERATIONS: i32 = 10;

/// Create a table with `NROWS` rows of scalar + array quantum columns.
fn create_bench_table(path: &str) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ScaFixed", PrimitiveType::Float64),
        ColumnSchema::array_fixed("ArrFixed", PrimitiveType::Float64, vec![4]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    TableQuantumDesc::with_unit("ScaFixed", "deg")
        .write(&mut table)
        .unwrap();
    TableQuantumDesc::with_unit("ArrFixed", "MHz")
        .write(&mut table)
        .unwrap();

    for i in 0..NROWS {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "ScaFixed",
                    Value::Scalar(ScalarValue::Float64(i as f64 * 0.1)),
                ),
                RecordField::new(
                    "ArrFixed",
                    Value::Array(ArrayValue::from_f64_vec(vec![
                        i as f64,
                        i as f64 + 1.0,
                        i as f64 + 2.0,
                        i as f64 + 3.0,
                    ])),
                ),
            ]))
            .unwrap();
    }

    table
        .save(TableOptions::new(path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();
}

#[test]
fn scalar_read_throughput_vs_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_quantum_scalar");
    let path_str = path.to_str().unwrap();

    create_bench_table(path_str);

    // ── C++ timing ──
    let cpp_ns =
        cpp_bench_scalar_read(path_str, "ScaFixed", ITERATIONS).expect("C++ bench should succeed");

    // ── Rust timing ──
    let table = Table::open(TableOptions::new(path_str)).unwrap();
    let col = ScalarQuantColumn::new(&table, "ScaFixed").unwrap();

    // Warm up
    for r in 0..table.row_count() {
        std::hint::black_box(col.get(r).unwrap());
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for r in 0..table.row_count() {
            let q = col.get(r).unwrap();
            std::hint::black_box(q.value());
        }
    }
    let rust_ns = start.elapsed().as_nanos() as u64;

    let total_ops = (ITERATIONS as u64) * (NROWS as u64);
    let rust_per_op = rust_ns as f64 / total_ops as f64;
    let cpp_per_op = cpp_ns as f64 / total_ops as f64;
    let ratio = rust_per_op / cpp_per_op;

    eprintln!("── scalar quantum read throughput ──");
    eprintln!("  rows:       {NROWS}");
    eprintln!("  iterations: {ITERATIONS}");
    eprintln!("  total ops:  {total_ops}");
    eprintln!("  Rust:       {rust_ns:>12} ns  ({rust_per_op:.1} ns/op)");
    eprintln!("  C++:        {cpp_ns:>12} ns  ({cpp_per_op:.1} ns/op)");
    eprintln!("  ratio:      {ratio:.2}x");

    if ratio > 2.0 {
        eprintln!(
            "  ⚠ WARNING: Rust scalar quantum read is {ratio:.1}x slower than C++ (threshold: 2.0x)"
        );
    }
}

#[test]
fn array_read_throughput_vs_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_quantum_array");
    let path_str = path.to_str().unwrap();

    create_bench_table(path_str);

    // ── C++ timing ──
    let cpp_ns =
        cpp_bench_array_read(path_str, "ArrFixed", ITERATIONS).expect("C++ bench should succeed");

    // ── Rust timing ──
    let table = Table::open(TableOptions::new(path_str)).unwrap();
    let col = ArrayQuantColumn::new(&table, "ArrFixed").unwrap();

    // Warm up
    for r in 0..table.row_count() {
        std::hint::black_box(col.get(r).unwrap());
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for r in 0..table.row_count() {
            let qs = col.get(r).unwrap();
            std::hint::black_box(qs.len());
        }
    }
    let rust_ns = start.elapsed().as_nanos() as u64;

    let total_ops = (ITERATIONS as u64) * (NROWS as u64);
    let rust_per_op = rust_ns as f64 / total_ops as f64;
    let cpp_per_op = cpp_ns as f64 / total_ops as f64;
    let ratio = rust_per_op / cpp_per_op;

    eprintln!("── array quantum read throughput ──");
    eprintln!("  rows:       {NROWS}");
    eprintln!("  iterations: {ITERATIONS}");
    eprintln!("  total ops:  {total_ops}");
    eprintln!("  Rust:       {rust_ns:>12} ns  ({rust_per_op:.1} ns/op)");
    eprintln!("  C++:        {cpp_ns:>12} ns  ({cpp_per_op:.1} ns/op)");
    eprintln!("  ratio:      {ratio:.2}x");

    if ratio > 2.0 {
        eprintln!(
            "  ⚠ WARNING: Rust array quantum read is {ratio:.1}x slower than C++ (threshold: 2.0x)"
        );
    }
}
