// SPDX-License-Identifier: LGPL-3.0-or-later
//! Criterion benchmarks for TaQL query execution.
//!
//! Measures Rust query performance on realistic workloads.
//! When C++ casacore is available (via casacore-test-support),
//! the threshold tests in `tests/taql_perf_threshold.rs` compare
//! Rust timings against the C++ baseline.

use criterion::{Criterion, criterion_group, criterion_main};

use casacore_tables::{ColumnSchema, Table, TableSchema};
use casacore_types::*;

/// Build a simple table with `n` rows for benchmarking.
fn bench_simple_table(n: usize) -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("ra", PrimitiveType::Float64),
        ColumnSchema::scalar("dec", PrimitiveType::Float64),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("category", PrimitiveType::String),
    ])
    .unwrap();

    let categories = ["star", "galaxy", "pulsar", "quasar", "nebula"];
    let mut table = Table::with_schema(schema);
    for i in 0..n {
        let cat = categories[i % categories.len()];
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(i as i32))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("SRC_{i:03}"))),
                ),
                RecordField::new("ra", Value::Scalar(ScalarValue::Float64(i as f64 * 7.2))),
                RecordField::new(
                    "dec",
                    Value::Scalar(ScalarValue::Float64(-45.0 + i as f64 * 1.8)),
                ),
                RecordField::new(
                    "flux",
                    Value::Scalar(ScalarValue::Float64(0.1 + i as f64 * 0.5)),
                ),
                RecordField::new(
                    "category",
                    Value::Scalar(ScalarValue::String(cat.to_string())),
                ),
            ]))
            .unwrap();
    }
    table
}

fn bench_filter_sort_project(c: &mut Criterion) {
    let mut group = c.benchmark_group("taql_pipeline");

    // Wave 14: Full pipeline (filter + sort + project + limit)
    group.bench_function("filter_sort_project_limit_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table
                .query("SELECT id, name, flux WHERE flux > 10.0 ORDER BY flux DESC LIMIT 100")
                .unwrap();
        });
    });

    group.bench_function("select_all_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table.query("SELECT *").unwrap();
        });
    });

    group.bench_function("where_filter_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table
                .query("SELECT * WHERE category = 'star' AND flux > 100.0")
                .unwrap();
        });
    });

    group.finish();
}

fn bench_groupby(c: &mut Criterion) {
    let mut group = c.benchmark_group("taql_groupby");

    // Wave 6: GROUP BY + aggregate
    group.bench_function("groupby_count_5k", |b| {
        let mut table = bench_simple_table(5000);
        b.iter(|| {
            let _result = table
                .execute_taql("SELECT category, COUNT(*) GROUP BY category")
                .unwrap();
        });
    });

    group.finish();
}

fn bench_expression_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("taql_expr");

    group.bench_function("sqrt_filter_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table.query("SELECT * WHERE sqrt(flux) > 20.0").unwrap();
        });
    });

    group.bench_function("compound_where_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table
                .query("SELECT * WHERE flux > 100.0 AND category = 'star'")
                .unwrap();
        });
    });

    group.finish();
}

fn bench_order_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("taql_sort");

    group.bench_function("order_by_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table.query("SELECT * ORDER BY flux DESC").unwrap();
        });
    });

    group.bench_function("order_by_multi_5k", |b| {
        let table = bench_simple_table(5000);
        b.iter(|| {
            let _view = table
                .query("SELECT * ORDER BY category ASC, flux DESC")
                .unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_filter_sort_project,
    bench_groupby,
    bench_expression_eval,
    bench_order_by,
);
criterion_main!(benches);
