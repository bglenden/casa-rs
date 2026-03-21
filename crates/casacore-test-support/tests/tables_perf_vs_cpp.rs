// SPDX-License-Identifier: LGPL-3.0-or-later
//! Performance comparison tests: Rust vs C++ casacore.
//!
//! Each test runs the same workload with both implementations and reports the
//! ratio. In `cargo test` (debug) the Rust side is unoptimized while C++ is
//! always compiled with optimization, so ratios >2× in debug are expected.
//! Use `cargo test --release` for a meaningful comparison. The 2× threshold
//! triggers a warning (not a hard failure) so CI captures the ratio.

use casacore_tables::{ColumnSchema, Slicer, Table, TableOptions, TableSchema};
use casacore_test_support::{
    CellSliceBenchParams, cpp_backend_available, cpp_bulk_scalar_io_bench, cpp_cell_slice_bench,
    cpp_copy_rows_bench, cpp_deep_copy_bench, cpp_set_algebra_bench,
};
use casacore_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ShapeBuilder;

// ---------------------------------------------------------------------------
// Set algebra: Rust row_union/intersection/difference vs C++ Table::operator|/&/-
// ---------------------------------------------------------------------------

#[test]
fn set_algebra_perf_100k_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping set_algebra_perf_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: u64 = 100_000;
    const SPLIT_A: u64 = 60_000; // set A = [0..60k)
    const SPLIT_B: u64 = 40_000; // set B = [40k..100k)

    // ── C++ timing ──────────────────────────────────────────────────────────
    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_path = dir.path().join("cpp_set_algebra.tbl");

    let cpp = cpp_set_algebra_bench(&cpp_path, NROWS, SPLIT_A, SPLIT_B)
        .expect("C++ set_algebra_bench should succeed");

    assert_eq!(cpp.union_rows, NROWS, "C++ union row count");
    assert_eq!(cpp.intersection_rows, 20_000, "C++ intersection row count");
    assert_eq!(cpp.difference_rows, 40_000, "C++ difference row count");

    // ── Rust timing ─────────────────────────────────────────────────────────
    let a: Vec<usize> = (0..SPLIT_A as usize).collect();
    let b: Vec<usize> = (SPLIT_B as usize..NROWS as usize).collect();

    let t0 = std::time::Instant::now();
    let union = Table::row_union(&a, &b);
    let rust_union_ns = t0.elapsed().as_nanos() as u64;

    let t0 = std::time::Instant::now();
    let intersection = Table::row_intersection(&a, &b);
    let rust_intersection_ns = t0.elapsed().as_nanos() as u64;

    let t0 = std::time::Instant::now();
    let difference = Table::row_difference(&a, &b);
    let rust_difference_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(union.len(), NROWS as usize, "Rust union row count");
    assert_eq!(intersection.len(), 20_000, "Rust intersection row count");
    assert_eq!(difference.len(), 40_000, "Rust difference row count");

    // ── Report ──────────────────────────────────────────────────────────────
    let union_ratio = rust_union_ns as f64 / cpp.union_ns.max(1) as f64;
    let inter_ratio = rust_intersection_ns as f64 / cpp.intersection_ns.max(1) as f64;
    let diff_ratio = rust_difference_ns as f64 / cpp.difference_ns.max(1) as f64;

    eprintln!(
        "Set algebra perf ({NROWS} rows, A=[0..{SPLIT_A}), B=[{SPLIT_B}..{NROWS})):\n  \
         Union:        C++ {:.1} ms, Rust {:.1} ms, ratio {union_ratio:.1}×\n  \
         Intersection: C++ {:.1} ms, Rust {:.1} ms, ratio {inter_ratio:.1}×\n  \
         Difference:   C++ {:.1} ms, Rust {:.1} ms, ratio {diff_ratio:.1}×",
        cpp.union_ns as f64 / 1e6,
        rust_union_ns as f64 / 1e6,
        cpp.intersection_ns as f64 / 1e6,
        rust_intersection_ns as f64 / 1e6,
        cpp.difference_ns as f64 / 1e6,
        rust_difference_ns as f64 / 1e6,
    );

    let max_ratio = union_ratio.max(inter_ratio).max(diff_ratio);
    if max_ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust set algebra {max_ratio:.1}× slower than C++ (threshold 2×). \
             Follow-up recommended."
        );
    }
}

// ---------------------------------------------------------------------------
// Row copy: Rust copy_rows vs C++ TableCopy::copyRows
// ---------------------------------------------------------------------------

#[test]
fn copy_rows_perf_10k_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping copy_rows_perf_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: u64 = 10_000;

    // ── C++ timing ──────────────────────────────────────────────────────────
    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_dir = dir.path().join("cpp_copy");
    std::fs::create_dir_all(&cpp_dir).unwrap();

    let cpp_ns = cpp_copy_rows_bench(&cpp_dir, NROWS).expect("C++ copy_rows_bench should succeed");

    // ── Rust timing ─────────────────────────────────────────────────────────
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
    ])
    .unwrap();

    let rows: Vec<RecordValue> = (0..NROWS as usize)
        .map(|i| {
            RecordValue::new(vec![
                RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(i as i32))),
                RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(i as f64))),
                RecordField::new(
                    "col_str",
                    Value::Scalar(ScalarValue::String(format!("row_{i}"))),
                ),
            ])
        })
        .collect();

    let source = Table::from_rows_with_schema(rows, schema.clone()).unwrap();
    let mut dest = Table::with_schema(schema);

    let t0 = std::time::Instant::now();
    dest.copy_rows(&source).unwrap();
    let rust_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(dest.row_count(), NROWS as usize, "Rust dest row count");

    // ── Report ──────────────────────────────────────────────────────────────
    let ratio = rust_ns as f64 / cpp_ns.max(1) as f64;

    eprintln!(
        "copy_rows perf ({NROWS} rows, 3 scalar columns):\n  \
         C++:  {:.1} ms\n  \
         Rust: {:.1} ms\n  \
         ratio Rust/C++: {ratio:.1}×",
        cpp_ns as f64 / 1e6,
        rust_ns as f64 / 1e6,
    );

    if ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust copy_rows {ratio:.1}× slower than C++ (threshold 2×). \
             Follow-up recommended."
        );
    }
}

// ---------------------------------------------------------------------------
// Cell slicing: Rust get_cell_slice vs C++ ArrayColumn::getSlice
// ---------------------------------------------------------------------------

#[test]
fn cell_slice_perf_10k_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping cell_slice_perf_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: u64 = 10_000;
    const DIM0: i64 = 100;
    const DIM1: i64 = 100;
    const SLICE_START0: i64 = 10;
    const SLICE_START1: i64 = 20;
    const SLICE_END0: i64 = 50; // exclusive
    const SLICE_END1: i64 = 80; // exclusive

    // ── C++ timing ──────────────────────────────────────────────────────────
    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_path = dir.path().join("cpp_slice.tbl");

    let cpp = cpp_cell_slice_bench(
        &cpp_path,
        &CellSliceBenchParams {
            nrows: NROWS,
            dim0: DIM0,
            dim1: DIM1,
            slice_start0: SLICE_START0,
            slice_start1: SLICE_START1,
            slice_end0: SLICE_END0,
            slice_end1: SLICE_END1,
        },
    )
    .expect("C++ cell_slice_bench should succeed");

    // ── Rust timing ─────────────────────────────────────────────────────────
    let shape = vec![DIM0 as usize, DIM1 as usize];
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        shape.clone(),
    )])
    .unwrap();

    let arr = ArrayValue::Float64(
        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape).f(), vec![1.0f64; 10_000]).unwrap(),
    );
    let row = RecordValue::new(vec![RecordField::new("data", Value::Array(arr))]);
    let rows: Vec<RecordValue> = vec![row; NROWS as usize];

    let table = Table::from_rows_with_schema(rows, schema).unwrap();

    let slicer = Slicer::contiguous(
        vec![SLICE_START0 as usize, SLICE_START1 as usize],
        vec![SLICE_END0 as usize, SLICE_END1 as usize],
    )
    .unwrap();

    let t0 = std::time::Instant::now();
    for i in 0..NROWS as usize {
        let slice = table.get_cell_slice("data", i, &slicer).unwrap();
        if i == 0 {
            match &slice {
                Value::Array(ArrayValue::Float64(arr)) => {
                    assert_eq!(arr.shape(), &[40, 60], "slice shape mismatch");
                }
                other => panic!("expected Float64 array, got {:?}", other),
            }
        }
    }
    let rust_slice_ns = t0.elapsed().as_nanos() as u64;

    // ── Report ──────────────────────────────────────────────────────────────
    let ratio = rust_slice_ns as f64 / cpp.slice_ns.max(1) as f64;

    eprintln!(
        "cell_slice perf ({NROWS} rows, [{DIM0}×{DIM1}] arrays, slice [{SLICE_START0}..{SLICE_END0}, {SLICE_START1}..{SLICE_END1}]):\n  \
         C++:  {:.1} ms (slice only, excludes write)\n  \
         Rust: {:.1} ms (in-memory slice)\n  \
         ratio Rust/C++: {ratio:.1}×",
        cpp.slice_ns as f64 / 1e6,
        rust_slice_ns as f64 / 1e6,
    );

    if ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust cell slicing {ratio:.1}× slower than C++ (threshold 2×). \
             Follow-up recommended."
        );
    }
}

// ---------------------------------------------------------------------------
// Bulk scalar I/O: 100k rows with Int32 + Float64 + String columns
// ---------------------------------------------------------------------------

#[test]
fn bulk_scalar_io_100k_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping bulk_scalar_io_100k_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: u64 = 100_000;

    // ── C++ timing ──────────────────────────────────────────────────────────
    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_path = dir.path().join("cpp_bulk.tbl");

    let cpp = cpp_bulk_scalar_io_bench(&cpp_path, NROWS)
        .expect("C++ bulk_scalar_io_bench should succeed");

    // ── Rust write timing ───────────────────────────────────────────────────
    let rust_path = dir.path().join("rust_bulk.tbl");
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
    ])
    .unwrap();

    let t0 = std::time::Instant::now();
    let rows: Vec<RecordValue> = (0..NROWS as usize)
        .map(|i| {
            RecordValue::new(vec![
                RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(i as i32))),
                RecordField::new(
                    "col_f64",
                    Value::Scalar(ScalarValue::Float64(i as f64 * 0.5)),
                ),
                RecordField::new(
                    "col_str",
                    Value::Scalar(ScalarValue::String(format!("row_{i}"))),
                ),
            ])
        })
        .collect();
    let table = Table::from_rows_with_schema(rows, schema).unwrap();
    table.save(TableOptions::new(&rust_path)).unwrap();
    let rust_write_ns = t0.elapsed().as_nanos() as u64;

    // ── Rust read timing ────────────────────────────────────────────────────
    let t0 = std::time::Instant::now();
    let table = Table::open(TableOptions::new(&rust_path)).unwrap();
    let mut sum: i64 = 0;
    for i in 0..table.row_count() {
        if let Ok(Some(Value::Scalar(ScalarValue::Int32(v)))) = table.cell(i, "col_i32") {
            sum += *v as i64;
        }
        if let Ok(Some(Value::Scalar(ScalarValue::Float64(v)))) = table.cell(i, "col_f64") {
            sum += *v as i64;
        }
        if let Ok(Some(Value::Scalar(ScalarValue::String(s)))) = table.cell(i, "col_str") {
            sum += s.len() as i64;
        }
    }
    let rust_read_ns = t0.elapsed().as_nanos() as u64;
    let _ = sum; // prevent optimization

    // ── Report ──────────────────────────────────────────────────────────────
    let write_ratio = rust_write_ns as f64 / cpp.write_ns.max(1) as f64;
    let read_ratio = rust_read_ns as f64 / cpp.read_ns.max(1) as f64;

    eprintln!(
        "Bulk scalar I/O ({NROWS} rows, 3 columns):\n  \
         Write: C++ {:.1} ms, Rust {:.1} ms, ratio {write_ratio:.1}×\n  \
         Read:  C++ {:.1} ms, Rust {:.1} ms, ratio {read_ratio:.1}×",
        cpp.write_ns as f64 / 1e6,
        rust_write_ns as f64 / 1e6,
        cpp.read_ns as f64 / 1e6,
        rust_read_ns as f64 / 1e6,
    );

    if write_ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust bulk scalar write {write_ratio:.1}× slower than C++ (threshold 2×)."
        );
    }
    if read_ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust bulk scalar read {read_ratio:.1}× slower than C++ (threshold 2×)."
        );
    }
}

// ---------------------------------------------------------------------------
// Deep copy: 10k rows with Int32 + Float64 + String columns
// ---------------------------------------------------------------------------

#[test]
fn deep_copy_perf_10k_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping deep_copy_perf_10k_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: u64 = 10_000;

    // ── C++ timing ──────────────────────────────────────────────────────────
    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_dir = dir.path().join("cpp_copy");
    std::fs::create_dir_all(&cpp_dir).unwrap();

    let cpp = cpp_deep_copy_bench(&cpp_dir, NROWS).expect("C++ deep_copy_bench should succeed");

    // ── Rust timing ─────────────────────────────────────────────────────────
    let rust_dir = dir.path().join("rust_copy");
    std::fs::create_dir_all(&rust_dir).unwrap();

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
    ])
    .unwrap();

    // Write source
    let rows: Vec<RecordValue> = (0..NROWS as usize)
        .map(|i| {
            RecordValue::new(vec![
                RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(i as i32))),
                RecordField::new(
                    "col_f64",
                    Value::Scalar(ScalarValue::Float64(i as f64 * 0.5)),
                ),
                RecordField::new(
                    "col_str",
                    Value::Scalar(ScalarValue::String(format!("row_{i}"))),
                ),
            ])
        })
        .collect();
    let source = Table::from_rows_with_schema(rows, schema).unwrap();
    let src_path = rust_dir.join("source.tbl");
    source.save(TableOptions::new(&src_path)).unwrap();

    // Time deep copy
    let source = Table::open(TableOptions::new(&src_path)).unwrap();
    let dst_path = rust_dir.join("copy.tbl");

    let t0 = std::time::Instant::now();
    source.deep_copy(TableOptions::new(&dst_path)).unwrap();
    let rust_copy_ns = t0.elapsed().as_nanos() as u64;

    // Verify
    let copy = Table::open(TableOptions::new(&dst_path)).unwrap();
    assert_eq!(copy.row_count(), NROWS as usize, "deep_copy row count");

    // ── Report ──────────────────────────────────────────────────────────────
    let ratio = rust_copy_ns as f64 / cpp.copy_ns.max(1) as f64;

    eprintln!(
        "Deep copy perf ({NROWS} rows, 3 scalar columns):\n  \
         C++:  {:.1} ms\n  \
         Rust: {:.1} ms\n  \
         ratio Rust/C++: {ratio:.1}×",
        cpp.copy_ns as f64 / 1e6,
        rust_copy_ns as f64 / 1e6,
    );

    if ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust deep_copy {ratio:.1}× slower than C++ (threshold 2×). \
             Follow-up recommended."
        );
    }
}
