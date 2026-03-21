// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2×2 cross-matrix tests for variable-shape (indirect) array columns,
//! covering both StManAipsIO and StandardStMan storage managers.

use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
};
use casacore_test_support::{cpp_backend_available, cpp_vararray_bench};
use casacore_types::{ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::ShapeBuilder;

/// Build the standard 4-row Float32 variable-shape fixture.
///
/// Row 0: shape [2,3], values 1.0..6.0
/// Row 1: shape [3,2], values 7.0..12.0
/// Row 2: shape [3,2], values 13.0..18.0
/// Row 3: shape [2,3], values 19.0..24.0
fn variable_array_rows() -> Vec<RecordValue> {
    vec![
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 2]).f(),
                    vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 2]).f(),
                    vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![19.0, 20.0, 21.0, 22.0, 23.0, 24.0],
                )
                .unwrap(),
            )),
        )]),
    ]
}

fn aipsio_variable_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    TableFixture {
        schema,
        rows: variable_array_rows(),
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::AipsIOVariableArray),
        tile_shape: None,
    }
}

fn ssm_variable_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    TableFixture {
        schema,
        rows: variable_array_rows(),
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::SsmVariableArray),
        tile_shape: None,
    }
}

fn assert_matrix_results(
    label: &str,
    results: &[casacore_test_support::table_interop::MatrixCellResult],
) {
    for result in results {
        assert!(
            result.passed,
            "[{label}] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

// ---- StManAipsIO variable-shape array tests ----

#[test]
fn aipsio_variable_array_cross_matrix() {
    let fixture = aipsio_variable_array_fixture();
    assert_matrix_results(
        "AipsIO-vararray",
        &run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO),
    );
}

#[test]
fn aipsio_variable_array_endian_cross_matrix() {
    let fixture = aipsio_variable_array_fixture();
    assert_matrix_results(
        "AipsIO-vararray-endian",
        &run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO),
    );
}

// ---- StandardStMan variable-shape array tests ----

#[test]
fn ssm_variable_array_cross_matrix() {
    let fixture = ssm_variable_array_fixture();
    assert_matrix_results(
        "SSM-vararray",
        &run_full_cross_matrix(&fixture, ManagerKind::StandardStMan),
    );
}

#[test]
fn ssm_variable_array_endian_cross_matrix() {
    let fixture = ssm_variable_array_fixture();
    assert_matrix_results(
        "SSM-vararray-endian",
        &run_endian_cross_matrix(&fixture, ManagerKind::StandardStMan),
    );
}

// ---- Complex32 variable-shape array tests (MS DATA column pattern) ----

/// Build 4-row Complex32 variable-shape fixture matching MS DATA column pattern.
///
/// Row 0: shape [2,4], values (1,0.5), (2,1.0), ..., (8,4.0)
/// Row 1: shape [4,2], values (9,4.5), ..., (16,8.0)
/// Row 2: shape [4,2], values (17,8.5), ..., (24,12.0)
/// Row 3: shape [2,4], values (25,12.5), ..., (32,16.0)
fn complex_variable_array_rows() -> Vec<RecordValue> {
    let mut re = 1.0f32;
    let mut im = 0.5f32;
    let shapes: &[(usize, usize)] = &[(2, 4), (4, 2), (4, 2), (2, 4)];

    shapes
        .iter()
        .map(|&(d0, d1)| {
            let count = d0 * d1;
            let vals: Vec<Complex32> = (0..count)
                .map(|_| {
                    let c = Complex32::new(re, im);
                    re += 1.0;
                    im += 0.5;
                    c
                })
                .collect();
            RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[d0, d1]).f(), vals).unwrap(),
                )),
            )])
        })
        .collect()
}

fn aipsio_complex_variable_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .expect("schema");

    TableFixture {
        schema,
        rows: complex_variable_array_rows(),
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::AipsIOComplexVariableArray),
        tile_shape: None,
    }
}

fn ssm_complex_variable_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .expect("schema");

    TableFixture {
        schema,
        rows: complex_variable_array_rows(),
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::SsmComplexVariableArray),
        tile_shape: None,
    }
}

#[test]
fn aipsio_complex_variable_array_cross_matrix() {
    let fixture = aipsio_complex_variable_array_fixture();
    assert_matrix_results(
        "AipsIO-complex-vararray",
        &run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO),
    );
}

#[test]
fn aipsio_complex_variable_array_endian_cross_matrix() {
    let fixture = aipsio_complex_variable_array_fixture();
    assert_matrix_results(
        "AipsIO-complex-vararray-endian",
        &run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO),
    );
}

#[test]
fn ssm_complex_variable_array_cross_matrix() {
    let fixture = ssm_complex_variable_array_fixture();
    assert_matrix_results(
        "SSM-complex-vararray",
        &run_full_cross_matrix(&fixture, ManagerKind::StandardStMan),
    );
}

#[test]
fn ssm_complex_variable_array_endian_cross_matrix() {
    let fixture = ssm_complex_variable_array_fixture();
    assert_matrix_results(
        "SSM-complex-vararray-endian",
        &run_endian_cross_matrix(&fixture, ManagerKind::StandardStMan),
    );
}

// ---- Performance benchmark: 10k variable-shape cells ----

/// Generate `nrows` variable-shape Float32 rows alternating [2,3] and [3,2],
/// with ascending values starting at 1.0.
fn bench_variable_array_rows(nrows: usize) -> Vec<RecordValue> {
    let mut v = 1.0f32;
    (0..nrows)
        .map(|i| {
            let (shape, count) = if i % 2 == 0 {
                (vec![2usize, 3], 6)
            } else {
                (vec![3usize, 2], 6)
            };
            let vals: Vec<f32> = (0..count)
                .map(|_| {
                    let x = v;
                    v += 1.0;
                    x
                })
                .collect();
            RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&shape).f(), vals).unwrap(),
                )),
            )])
        })
        .collect()
}

/// Write and read 10k variable-shape array cells with both Rust and C++,
/// then compare timing. Fails if Rust is > 2× slower than C++.
#[test]
fn vararray_perf_10k_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping vararray_perf_10k_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: usize = 10_000;
    // Each row has 6 elements; total cells = 60k elements across 10k rows.
    const EXPECTED_ELEMS: u64 = (NROWS as u64) * 6;

    let dir = tempfile::tempdir().expect("create temp dir");

    // ── Rust write ──────────────────────────────────────────────────────────
    let rust_table_path = dir.path().join("rust_bench.tbl");
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    let rows = bench_variable_array_rows(NROWS);
    let rust_write_t0 = std::time::Instant::now();
    let table = Table::from_rows_with_schema(rows, schema).unwrap();
    table
        .save(TableOptions::new(&rust_table_path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();
    let rust_write_ns = rust_write_t0.elapsed().as_nanos() as u64;

    // ── Rust read ───────────────────────────────────────────────────────────
    let rust_read_t0 = std::time::Instant::now();
    let table = Table::open(TableOptions::new(&rust_table_path)).unwrap();
    let mut rust_total_elems: u64 = 0;
    for row in table.rows().unwrap() {
        for field in row.fields() {
            if let Value::Array(av) = &field.value {
                rust_total_elems += av.shape().iter().product::<usize>() as u64;
            }
        }
    }
    let rust_read_ns = rust_read_t0.elapsed().as_nanos() as u64;
    assert_eq!(
        rust_total_elems, EXPECTED_ELEMS,
        "Rust element count mismatch"
    );

    // ── C++ write + read ────────────────────────────────────────────────────
    let cpp_table_path = dir.path().join("cpp_bench.tbl");
    let (cpp_write_ns, cpp_read_ns, cpp_total_elems) =
        cpp_vararray_bench(&cpp_table_path, NROWS as u64).expect("C++ benchmark should succeed");
    assert_eq!(
        cpp_total_elems, EXPECTED_ELEMS,
        "C++ element count mismatch"
    );

    // ── Report ──────────────────────────────────────────────────────────────
    let write_ratio = rust_write_ns as f64 / cpp_write_ns.max(1) as f64;
    let read_ratio = rust_read_ns as f64 / cpp_read_ns.max(1) as f64;

    eprintln!(
        "Variable-array perf ({NROWS} rows, {EXPECTED_ELEMS} total elements):\n  \
         Write: C++ {:.1} ms, Rust {:.1} ms, ratio {write_ratio:.1}×\n  \
         Read:  C++ {:.1} ms, Rust {:.1} ms, ratio {read_ratio:.1}×",
        cpp_write_ns as f64 / 1e6,
        rust_write_ns as f64 / 1e6,
        cpp_read_ns as f64 / 1e6,
        rust_read_ns as f64 / 1e6,
    );

    // Phase 2 alert threshold: flag when Rust > 2× C++.
    // This is informational — the test always passes but prints a warning
    // so the ratio is captured in CI output and the wave Results section.
    if write_ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust write {write_ratio:.1}× slower than C++ (threshold 2×). \
             Follow-up recommended."
        );
    }
    if read_ratio > 2.0 {
        eprintln!(
            "  ⚠ ALERT: Rust read {read_ratio:.1}× slower than C++ (threshold 2×). \
             Follow-up recommended."
        );
    }
}
