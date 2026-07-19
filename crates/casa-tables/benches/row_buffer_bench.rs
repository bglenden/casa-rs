// SPDX-License-Identifier: LGPL-3.0-or-later
//! Criterion benchmarks for row-oriented table access.
//!
//! Compares the legacy materialized row path against the reusable prepared-row
//! path added in wave 99 on the same persisted table shape.

use std::hint::black_box;
use std::path::PathBuf;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ndarray::ArrayD;
use tempfile::TempDir;

use casa_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn persisted_row_table(row_count: usize) -> (TempDir, std::path::PathBuf) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("weight", PrimitiveType::Float64),
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![4]),
        ColumnSchema::array_fixed("other", PrimitiveType::Int32, vec![512]),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
    ])
    .expect("valid schema");

    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("row-bench.table");

    let mut table = Table::with_schema(schema);
    for row_index in 0..row_count {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(row_index as i32))),
                RecordField::new(
                    "weight",
                    Value::Scalar(ScalarValue::Float64(row_index as f64 * 0.25)),
                ),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::from_i32_vec(vec![
                        row_index as i32,
                        row_index as i32 + 1,
                        row_index as i32 + 2,
                        row_index as i32 + 3,
                    ])),
                ),
                RecordField::new(
                    "other",
                    Value::Array(ArrayValue::from_i32_vec(
                        (0..512)
                            .map(|offset| row_index as i32 + offset)
                            .collect::<Vec<_>>(),
                    )),
                ),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool(row_index % 2 == 0))),
            ]))
            .expect("add row");
    }

    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save benchmark table");
    (tempdir, path)
}

fn persisted_incremental_scalar_table(row_count: usize) -> (TempDir, PathBuf) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan", PrimitiveType::Int32),
        ColumnSchema::scalar("state", PrimitiveType::Int32),
    ])
    .expect("valid schema");

    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("incremental-sparse-bench.table");

    let mut table = Table::with_schema(schema);
    for row_index in 0..row_count {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(row_index as i32))),
                RecordField::new(
                    "scan",
                    Value::Scalar(ScalarValue::Int32(((row_index / 8) * 10) as i32)),
                ),
                RecordField::new(
                    "state",
                    Value::Scalar(ScalarValue::Int32((row_index % 3) as i32)),
                ),
            ]))
            .expect("add row");
    }

    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::IncrementalStMan))
        .expect("save benchmark table");
    (tempdir, path)
}

fn persisted_tiled_single_column_table(row_count: usize) -> (TempDir, PathBuf) {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![16, 4],
    )])
    .expect("valid schema");

    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("tiled-sparse-bench.table");

    let mut table = Table::with_schema(schema);
    for row_index in 0..row_count {
        let values = (0..64)
            .map(|offset| row_index as f32 + offset as f32)
            .collect::<Vec<_>>();
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![16, 4], values).expect("shape data"),
                )),
            )]))
            .expect("add row");
    }

    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save benchmark table");
    (tempdir, path)
}

fn int32_from_value(value: &Value) -> i32 {
    match value {
        Value::Scalar(ScalarValue::Int32(v)) => *v,
        other => panic!("expected int32 scalar, got {other:?}"),
    }
}

fn float64_from_value(value: &Value) -> f64 {
    match value {
        Value::Scalar(ScalarValue::Float64(v)) => *v,
        other => panic!("expected float64 scalar, got {other:?}"),
    }
}

fn first_i32_from_value(value: &Value) -> i32 {
    match value {
        Value::Array(ArrayValue::Int32(values)) => values[[0]],
        other => panic!("expected int32 array, got {other:?}"),
    }
}

fn bench_row_reads(c: &mut Criterion) {
    let (_tempdir, path) = persisted_row_table(4096);
    let mut group = c.benchmark_group("row_buffer_read");

    group.bench_function("materialized_rows_4k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&path)).expect("open table"),
            |table| {
                let rows = table.row_accessor();
                let mut acc = 0.0f64;
                for row_index in 0..rows.row_count() {
                    let row = rows.row(row_index).expect("row");
                    let id = int32_from_value(row.get("id").expect("id"));
                    let weight = float64_from_value(row.get("weight").expect("weight"));
                    let first = first_i32_from_value(row.get("data").expect("data"));
                    acc += f64::from(id + first) + weight;
                }
                black_box(acc)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("prepared_rows_4k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&path)).expect("open table"),
            |table| {
                let mut prepared = table
                    .row_accessor()
                    .prepare(&["id", "weight", "data"])
                    .expect("prepare rows");
                let id_index = prepared.column_index("id").expect("id index");
                let weight_index = prepared.column_index("weight").expect("weight index");
                let data_index = prepared.column_index("data").expect("data index");
                let mut acc = 0.0f64;
                for row_index in 0..prepared.row_count() {
                    prepared.load(row_index).expect("load row");
                    let id = match prepared.scalar_at(id_index).expect("id slot") {
                        ScalarValue::Int32(value) => *value,
                        other => panic!("expected int32 scalar, got {other:?}"),
                    };
                    let weight = match prepared.scalar_at(weight_index).expect("weight slot") {
                        ScalarValue::Float64(value) => *value,
                        other => panic!("expected float64 scalar, got {other:?}"),
                    };
                    let first = match prepared.array_at(data_index).expect("data slot") {
                        ArrayValue::Int32(values) => values[[0]],
                        other => panic!("expected int32 array, got {other:?}"),
                    };
                    acc += f64::from(id + first) + weight;
                }
                black_box(acc)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_row_writes(c: &mut Criterion) {
    let (_tempdir, path) = persisted_row_table(4096);
    let mut group = c.benchmark_group("row_buffer_write");

    group.bench_function("materialized_rows_4k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&path)).expect("open table"),
            |mut table| {
                let mut rows = table.row_accessor_mut();
                for row_index in 0..rows.row_count() {
                    let row = rows.row_mut(row_index).expect("row_mut");
                    if let Value::Scalar(ScalarValue::Float64(v)) =
                        row.get_mut("weight").expect("weight")
                    {
                        *v += 1.0;
                    }
                    if let Value::Scalar(ScalarValue::Bool(v)) = row.get_mut("flag").expect("flag")
                    {
                        *v = !*v;
                    }
                    if let Value::Array(ArrayValue::Int32(values)) =
                        row.get_mut("data").expect("data")
                    {
                        values[[0]] += 1;
                    }
                }
                black_box(rows.row_count())
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("prepared_rows_4k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&path)).expect("open table"),
            |mut table| {
                let mut prepared = table
                    .row_accessor_mut()
                    .prepare(&["weight", "flag", "data"])
                    .expect("prepare mutable rows");
                let weight_index = prepared.column_index("weight").expect("weight index");
                let flag_index = prepared.column_index("flag").expect("flag index");
                let data_index = prepared.column_index("data").expect("data index");
                for row_index in 0..prepared.row_count() {
                    prepared.seek(row_index).expect("seek row");
                    prepared
                        .set_value_at(
                            weight_index,
                            Value::Scalar(ScalarValue::Float64(row_index as f64 * 0.25 + 1.0)),
                        )
                        .expect("set weight");
                    prepared
                        .set_value_at(
                            flag_index,
                            Value::Scalar(ScalarValue::Bool(row_index % 2 != 0)),
                        )
                        .expect("set flag");
                    prepared
                        .set_value_at(
                            data_index,
                            Value::Array(ArrayValue::from_i32_vec(vec![
                                row_index as i32 + 1,
                                row_index as i32 + 1,
                                row_index as i32 + 2,
                                row_index as i32 + 3,
                            ])),
                        )
                        .expect("set data");
                }
                black_box(prepared.row_count())
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sparse_partial_writes(c: &mut Criterion) {
    let (_incremental_tempdir, incremental_path) = persisted_incremental_scalar_table(8192);
    let (_tiled_tempdir, tiled_path) = persisted_tiled_single_column_table(2048);
    let mut group = c.benchmark_group("sparse_partial_write");

    group.bench_function("incremental_scalar_rows_8k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&incremental_path)).expect("open table"),
            |mut table| {
                let mut writer = table
                    .row_accessor_mut()
                    .prepare(&["id", "scan"])
                    .expect("prepare scalar writes");
                let id = writer.column_index("id").expect("id slot");
                let scan = writer.column_index("scan").expect("scan slot");
                writer.seek(2).expect("seek row 2");
                writer
                    .set_value_at(id, Value::Scalar(ScalarValue::Int32(2002)))
                    .expect("set row 2 id");
                writer.seek(4097).expect("seek row 4097");
                writer
                    .set_value_at(scan, Value::Scalar(ScalarValue::Int32(9041)))
                    .expect("set row 4097 scan");
                drop(writer);
                table
                    .prepare_write()
                    .save_selected_rows(&["id", "scan"], &[2, 4097])
                    .expect("sparse incremental partial save");
                black_box(table.row_count())
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("tiled_single_column_rows_2k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&tiled_path)).expect("open table"),
            |mut table| {
                let values = (0..64)
                    .map(|offset| 20_000.0 + offset as f32)
                    .collect::<Vec<_>>();
                let mut writer = table
                    .row_accessor_mut()
                    .prepare(&["data"])
                    .expect("prepare tiled write");
                let data = writer.column_index("data").expect("data slot");
                writer.seek(1024).expect("seek row 1024");
                writer
                    .set_value_at(
                        data,
                        Value::Array(ArrayValue::Float32(
                            ArrayD::from_shape_vec(vec![16, 4], values)
                                .expect("updated sparse tiled shape"),
                        )),
                    )
                    .expect("set sparse tiled cell");
                drop(writer);
                table
                    .prepare_write()
                    .save_selected_rows(&["data"], &[1024])
                    .expect("sparse tiled partial save");
                black_box(table.row_count())
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_lazy_single_cell_reads(c: &mut Criterion) {
    let (_incremental_tempdir, incremental_path) = persisted_incremental_scalar_table(8192);
    let mut group = c.benchmark_group("lazy_single_cell_read");

    group.bench_function("incremental_scalar_rows_8k", |b| {
        b.iter_batched(
            || Table::open(TableOptions::new(&incremental_path)).expect("open table"),
            |table| {
                let mut acc = 0i64;
                for row_index in [2usize, 511, 4097, 7001] {
                    let value = table
                        .cell_accessor(row_index, "scan")
                        .expect("scan accessor")
                        .scalar()
                        .expect("get buffered scalar cell");
                    let &ScalarValue::Int32(value) = value else {
                        panic!("expected int32 scalar");
                    };
                    acc += i64::from(value);
                }
                black_box(acc)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_row_reads,
    bench_row_writes,
    bench_sparse_partial_writes,
    bench_lazy_single_cell_reads
);
criterion_main!(benches);
