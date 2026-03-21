// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL performance threshold tests.
//!
//! Measures Rust query execution time and compares against the C++ baseline
//! (via `cpp_taql_query` which returns wall-clock nanoseconds). Asserts that
//! Rust is within the allowed performance ratio.
//!
//! These tests are only compiled when C++ casacore is available and only run
//! in release mode (debug-mode Rust vs optimized C++ is not a fair comparison).

#![cfg(has_casacore_cpp)]

use std::path::Path;
use std::time::Instant;

use casacore_test_support::taql_interop::*;

/// Only run threshold comparisons in release mode.
fn skip_unless_release() -> bool {
    cfg!(debug_assertions)
}

/// Measure median execution time in nanoseconds over `iterations` runs.
fn median_ns(mut f: impl FnMut(), iterations: usize) -> u64 {
    // Warm up
    f();
    let mut times: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        times.push(start.elapsed().as_nanos() as u64);
    }
    times.sort_unstable();
    times[times.len() / 2]
}

/// Measure median C++ query time via the shim.
fn cpp_median_ns(table_path: &Path, query: &str, iterations: usize) -> u64 {
    // Warm up
    let _ = cpp_taql_query(table_path, query);
    let mut times: Vec<u64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let res = cpp_taql_query(table_path, query).expect("C++ query failed");
        times.push(res.elapsed_ns);
    }
    times.sort_unstable();
    times[times.len() / 2]
}

/// Run a threshold comparison: Rust vs C++ on the same query/table size.
fn run_threshold(
    label: &str,
    rust_fn: &mut dyn FnMut(),
    cpp_query: &str,
    table_path: &Path,
    iterations: usize,
    max_ratio: f64,
) {
    let cpp_ns = cpp_median_ns(table_path, cpp_query, iterations);
    let rust_ns = median_ns(rust_fn, iterations);
    let ratio = rust_ns as f64 / cpp_ns as f64;

    eprintln!(
        "[perf] {label}: Rust={:.2}ms  C++={:.2}ms  ratio={:.2}x  (threshold={:.1}x)",
        rust_ns as f64 / 1e6,
        cpp_ns as f64 / 1e6,
        ratio,
        max_ratio,
    );

    assert!(
        ratio <= max_ratio,
        "{label}: Rust/C++ ratio {ratio:.2}x exceeds threshold {max_ratio:.1}x \
         (Rust={rust_ns}ns, C++={cpp_ns}ns)",
    );
}

/// Save a bench table to disk for C++ to read.
fn save_bench_table(n: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("perf_table");
    let table = build_simple_fixture_n(n);
    table
        .save(casacore_tables::TableOptions::new(&path))
        .expect("save bench table");
    (tmp, path)
}

/// Save a variable-shape array bench table to disk.
fn save_varshape_bench_table(n: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    use casacore_tables::{ColumnSchema, TableSchema};
    use casacore_types::*;
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("varshape_perf_table");

    let schema = TableSchema::new(vec![
        ColumnSchema::array_variable("data", PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("label", PrimitiveType::String),
    ])
    .unwrap();

    let mut table = casacore_tables::Table::with_schema(schema);
    for i in 0..n {
        let len = (i % 20) + 1; // variable lengths 1..20
        let vals: Vec<f64> = (0..len).map(|j| (i * 20 + j) as f64 * 0.1).collect();
        let arr = ArrayD::from_shape_vec(IxDyn(&[len]).f(), vals).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("data", Value::Array(ArrayValue::Float64(arr))),
                RecordField::new("label", Value::Scalar(ScalarValue::String(format!("R{i}")))),
            ]))
            .unwrap();
    }
    table
        .save(casacore_tables::TableOptions::new(&path))
        .expect("save varshape bench table");
    (tmp, path)
}

#[test]
fn perf_filter_sort_project_limit() {
    if skip_unless_release() {
        eprintln!("[perf] skipping in debug mode");
        return;
    }
    let (_tmp, path) = save_bench_table(5000);
    let table = build_simple_fixture_n(5000);

    run_threshold(
        "filter_sort_project_limit",
        &mut || {
            let _ = table
                .query("SELECT id, name, flux WHERE flux > 10.0 ORDER BY flux DESC LIMIT 100")
                .unwrap();
        },
        "SELECT id, name, flux FROM $1 WHERE flux > 10.0 ORDER BY flux DESC LIMIT 100",
        &path,
        50,
        2.0,
    );
}

#[test]
fn perf_groupby_count() {
    if skip_unless_release() {
        eprintln!("[perf] skipping in debug mode");
        return;
    }
    let (_tmp, path) = save_bench_table(5000);
    let mut table = build_simple_fixture_n(5000);

    run_threshold(
        "groupby_count",
        &mut || {
            let _ = table
                .execute_taql("SELECT category, COUNT(*) GROUP BY category")
                .unwrap();
        },
        "SELECT category, gcount() FROM $1 GROUPBY category",
        &path,
        50,
        2.0,
    );
}

#[test]
fn perf_expression_eval() {
    if skip_unless_release() {
        eprintln!("[perf] skipping in debug mode");
        return;
    }
    let (_tmp, path) = save_bench_table(5000);
    let table = build_simple_fixture_n(5000);

    run_threshold(
        "expression_eval",
        &mut || {
            let _ = table.query("SELECT * WHERE sqrt(flux) > 20.0").unwrap();
        },
        "SELECT * FROM $1 WHERE sqrt(flux) > 20.0",
        &path,
        50,
        2.0,
    );
}

#[test]
fn perf_order_by() {
    if skip_unless_release() {
        eprintln!("[perf] skipping in debug mode");
        return;
    }
    let (_tmp, path) = save_bench_table(5000);
    let table = build_simple_fixture_n(5000);

    run_threshold(
        "order_by",
        &mut || {
            let _ = table.query("SELECT * ORDER BY flux DESC").unwrap();
        },
        "SELECT * FROM $1 ORDER BY flux DESC",
        &path,
        50,
        2.0,
    );
}

#[test]
fn perf_varshape_read() {
    if skip_unless_release() {
        eprintln!("[perf] skipping in debug mode");
        return;
    }
    let (_tmp, path) = save_varshape_bench_table(5000);
    let table = casacore_tables::Table::open(casacore_tables::TableOptions::new(&path))
        .expect("open varshape bench table");

    run_threshold(
        "varshape_read",
        &mut || {
            let _ = table.query("SELECT data").unwrap();
        },
        "SELECT data FROM $1",
        &path,
        50,
        2.0,
    );
}
