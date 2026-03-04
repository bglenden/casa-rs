// SPDX-License-Identifier: LGPL-3.0-or-later
//! Virtual column engine interop tests between Rust and C++ casacore.
//!
//! Tests cover ForwardColumnEngine and ScaledArrayEngine in the standard
//! 2×2 matrix (CC, CR, RC, RR). These tests are skipped when `pkg-config
//! casacore` is not available.

use casacore_test_support::{
    CppTableFixture, cpp_backend_available, cpp_table_verify, cpp_table_write,
};

use casacore_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn};

// ===================== ForwardColumnEngine =====================

/// CC: C++ writes forward-column fixture → C++ verifies.
#[test]
fn cc_forward_column() {
    if !cpp_backend_available() {
        eprintln!("skipping CC forward_column test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let fwd_path = dir.path().join("fwd.tbl");

    cpp_table_write(CppTableFixture::ForwardColumn, &fwd_path)
        .expect("C++ write forward column should succeed");

    cpp_table_verify(CppTableFixture::ForwardColumn, &fwd_path)
        .expect("C++ verify forward column should succeed");
}

/// CR: C++ writes forward-column fixture → Rust opens and verifies data.
#[test]
fn cr_forward_column() {
    if !cpp_backend_available() {
        eprintln!("skipping CR forward_column test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let fwd_path = dir.path().join("fwd.tbl");

    cpp_table_write(CppTableFixture::ForwardColumn, &fwd_path)
        .expect("C++ write forward column should succeed");

    // Rust opens the forwarding table.
    let table = Table::open(TableOptions::new(&fwd_path))
        .expect("Rust should open C++ ForwardColumn table");

    assert_eq!(table.row_count(), 3, "forwarding table should have 3 rows");
    assert!(
        table.is_virtual_column("col_value"),
        "col_value should be virtual"
    );

    let expected = [1.5, 2.5, 3.5];
    for (i, &exp) in expected.iter().enumerate() {
        match table.cell(i, "col_value").expect("cell exists") {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!((v - exp).abs() < 1e-10, "row {i}: expected {exp}, got {v}");
            }
            other => panic!("row {i}: expected Float64, got {other:?}"),
        }
    }
}

/// RC: Rust writes forward-column → C++ verifies.
#[test]
fn rc_forward_column() {
    if !cpp_backend_available() {
        eprintln!("skipping RC forward_column test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let base_path = dir.path().join("fwd.tbl_base");
    let fwd_path = dir.path().join("fwd.tbl");

    // Rust writes the base table.
    let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "col_value",
        PrimitiveType::Float64,
    )])
    .unwrap();
    let mut base = Table::with_schema(base_schema);
    for v in [1.5, 2.5, 3.5] {
        base.add_row(RecordValue::new(vec![RecordField::new(
            "col_value",
            Value::Scalar(ScalarValue::Float64(v)),
        )]))
        .unwrap();
    }
    base.save(TableOptions::new(&base_path)).unwrap();

    // Rust writes the forwarding table.
    let fwd_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "col_value",
        PrimitiveType::Float64,
    )])
    .unwrap();
    let mut fwd = Table::with_schema(fwd_schema);
    for _ in 0..3 {
        fwd.add_row(RecordValue::new(vec![RecordField::new(
            "col_value",
            Value::Scalar(ScalarValue::Float64(0.0)),
        )]))
        .unwrap();
    }
    fwd.bind_forward_column("col_value", &base_path).unwrap();
    fwd.save(TableOptions::new(&fwd_path)).unwrap();

    // C++ verifies.
    cpp_table_verify(CppTableFixture::ForwardColumn, &fwd_path)
        .expect("C++ should read Rust-produced ForwardColumn table");
}

/// RR: Rust writes forward-column → Rust opens and verifies data.
#[test]
fn rr_forward_column() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let base_path = dir.path().join("base.tbl");
    let fwd_path = dir.path().join("fwd.tbl");

    // Rust writes the base table.
    let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "col_value",
        PrimitiveType::Float64,
    )])
    .unwrap();
    let mut base = Table::with_schema(base_schema);
    for v in [1.5, 2.5, 3.5] {
        base.add_row(RecordValue::new(vec![RecordField::new(
            "col_value",
            Value::Scalar(ScalarValue::Float64(v)),
        )]))
        .unwrap();
    }
    base.save(TableOptions::new(&base_path)).unwrap();

    // Rust writes the forwarding table.
    let fwd_schema = TableSchema::new(vec![ColumnSchema::scalar(
        "col_value",
        PrimitiveType::Float64,
    )])
    .unwrap();
    let mut fwd = Table::with_schema(fwd_schema);
    for _ in 0..3 {
        fwd.add_row(RecordValue::new(vec![RecordField::new(
            "col_value",
            Value::Scalar(ScalarValue::Float64(0.0)),
        )]))
        .unwrap();
    }
    fwd.bind_forward_column("col_value", &base_path).unwrap();
    fwd.save(TableOptions::new(&fwd_path)).unwrap();

    // Rust reopens and verifies.
    let reopened =
        Table::open(TableOptions::new(&fwd_path)).expect("Rust should reopen ForwardColumn table");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("col_value"));

    let expected = [1.5, 2.5, 3.5];
    for (i, &exp) in expected.iter().enumerate() {
        match reopened.cell(i, "col_value").expect("cell exists") {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!((v - exp).abs() < 1e-10, "row {i}: expected {exp}, got {v}");
            }
            other => panic!("row {i}: expected Float64, got {other:?}"),
        }
    }
}

// ===================== ScaledArrayEngine =====================

/// CC: C++ writes scaled-array fixture → C++ verifies.
#[test]
fn cc_scaled_array() {
    if !cpp_backend_available() {
        eprintln!("skipping CC scaled_array test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled.tbl");

    cpp_table_write(CppTableFixture::ScaledArray, &tbl_path)
        .expect("C++ write scaled array should succeed");

    cpp_table_verify(CppTableFixture::ScaledArray, &tbl_path)
        .expect("C++ verify scaled array should succeed");
}

/// CR: C++ writes scaled-array fixture → Rust opens and verifies data.
///
/// C++ fixture: stored_col is Int array (shape [2]), virtual_col is Double
/// array via ScaledArrayEngine(scale=2.5, offset=10.0).
/// Stored: [[1,2],[3,4],[5,6]]  Virtual: [[12.5,15],[17.5,20],[22.5,25]]
#[test]
fn cr_scaled_array() {
    if !cpp_backend_available() {
        eprintln!("skipping CR scaled_array test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled.tbl");

    cpp_table_write(CppTableFixture::ScaledArray, &tbl_path)
        .expect("C++ write scaled array should succeed");

    // Rust opens the table.
    let table =
        Table::open(TableOptions::new(&tbl_path)).expect("Rust should open C++ ScaledArray table");

    assert_eq!(table.row_count(), 3, "table should have 3 rows");
    assert!(
        table.is_virtual_column("virtual_col"),
        "virtual_col should be virtual"
    );
    assert!(
        !table.is_virtual_column("stored_col"),
        "stored_col should NOT be virtual"
    );

    // Verify stored column values (Int32 arrays of shape [2]).
    let expected_stored: [[i32; 2]; 3] = [[1, 2], [3, 4], [5, 6]];
    for (i, exp) in expected_stored.iter().enumerate() {
        match table.cell(i, "stored_col").expect("cell exists") {
            Value::Array(ArrayValue::Int32(arr)) => {
                let flat: Vec<i32> = arr.iter().copied().collect();
                assert_eq!(flat, exp, "stored_col row {i} mismatch");
            }
            other => panic!("stored_col row {i}: expected Int32 array, got {other:?}"),
        }
    }

    // Verify virtual column values: stored * 2.5 + 10.0.
    let expected_virtual: [[f64; 2]; 3] = [[12.5, 15.0], [17.5, 20.0], [22.5, 25.0]];
    for (i, exp) in expected_virtual.iter().enumerate() {
        match table.cell(i, "virtual_col").expect("cell exists") {
            Value::Array(ArrayValue::Float64(arr)) => {
                let flat: Vec<f64> = arr.iter().copied().collect();
                for (j, (&got, &want)) in flat.iter().zip(exp.iter()).enumerate() {
                    assert!(
                        (got - want).abs() < 1e-10,
                        "virtual_col row {i} elem {j}: expected {want}, got {got}"
                    );
                }
            }
            other => panic!("virtual_col row {i}: expected Float64 array, got {other:?}"),
        }
    }
}

/// RC: Rust writes scaled-array → C++ verifies.
///
/// Rust creates a table with array columns matching the C++ fixture format:
/// stored_col: Int32 arrays (shape [2]), virtual_col: Float64 arrays.
#[test]
fn rc_scaled_array() {
    if !cpp_backend_available() {
        eprintln!("skipping RC scaled_array test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled.tbl");

    // Rust creates a table with array stored + virtual columns.
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("stored_col", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("virtual_col", PrimitiveType::Float64, vec![2]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let stored_data: [[i32; 2]; 3] = [[1, 2], [3, 4], [5, 6]];
    for arr in &stored_data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(IxDyn(&[2]), arr.to_vec()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(IxDyn(&[2]), vec![0.0, 0.0]).unwrap(),
                    )),
                ),
            ]))
            .unwrap();
    }
    table
        .bind_scaled_array_column("virtual_col", "stored_col", 2.5, 10.0)
        .unwrap();
    table.save(TableOptions::new(&tbl_path)).unwrap();

    // C++ verifies.
    cpp_table_verify(CppTableFixture::ScaledArray, &tbl_path)
        .expect("C++ should read Rust-produced ScaledArray table");
}

/// RR: Rust writes scaled-array → Rust opens and verifies data.
#[test]
fn rr_scaled_array() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled.tbl");

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("stored_col", PrimitiveType::Int32, vec![2]),
        ColumnSchema::array_fixed("virtual_col", PrimitiveType::Float64, vec![2]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let stored_data: [[i32; 2]; 3] = [[1, 2], [3, 4], [5, 6]];
    for arr in &stored_data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(IxDyn(&[2]), arr.to_vec()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(IxDyn(&[2]), vec![0.0, 0.0]).unwrap(),
                    )),
                ),
            ]))
            .unwrap();
    }
    table
        .bind_scaled_array_column("virtual_col", "stored_col", 2.5, 10.0)
        .unwrap();
    table.save(TableOptions::new(&tbl_path)).unwrap();

    // Rust reopens and verifies.
    let reopened =
        Table::open(TableOptions::new(&tbl_path)).expect("Rust should reopen ScaledArray table");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("virtual_col"));
    assert!(!reopened.is_virtual_column("stored_col"));

    // Verify stored column (Int32 arrays of shape [2]).
    let expected_stored: [[i32; 2]; 3] = [[1, 2], [3, 4], [5, 6]];
    for (i, exp) in expected_stored.iter().enumerate() {
        match reopened.cell(i, "stored_col").expect("cell exists") {
            Value::Array(ArrayValue::Int32(arr)) => {
                let flat: Vec<i32> = arr.iter().copied().collect();
                assert_eq!(flat, exp, "stored_col row {i} mismatch");
            }
            other => panic!("stored_col row {i}: expected Int32 array, got {other:?}"),
        }
    }

    // Verify virtual column: stored * 2.5 + 10.0.
    let expected_virtual: [[f64; 2]; 3] = [[12.5, 15.0], [17.5, 20.0], [22.5, 25.0]];
    for (i, exp) in expected_virtual.iter().enumerate() {
        match reopened.cell(i, "virtual_col").expect("cell exists") {
            Value::Array(ArrayValue::Float64(arr)) => {
                let flat: Vec<f64> = arr.iter().copied().collect();
                for (j, (&got, &want)) in flat.iter().zip(exp.iter()).enumerate() {
                    assert!(
                        (got - want).abs() < 1e-10,
                        "virtual_col row {i} elem {j}: expected {want}, got {got}"
                    );
                }
            }
            other => panic!("virtual_col row {i}: expected Float64 array, got {other:?}"),
        }
    }
}

// ===================== ScaledArrayEngine<Complex,Short> =====================
//
// NOTE: CC, CR, RC tests for ScaledComplex are omitted because casacore 3.7.1's
// ScaledArrayEngine.tcc has a template bug (`if (offset == 0)` compares
// std::complex<float> with int) that prevents compilation with Apple Clang.
// The RR test validates that our Rust implementation correctly handles Complex32
// output from the scale/offset transform.

/// RR: Rust writes scaled-complex → Rust opens and verifies data.
#[test]
fn rr_scaled_complex() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled_cx.tbl");

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("stored_col", PrimitiveType::Int16, vec![2]),
        ColumnSchema::array_fixed("virtual_col", PrimitiveType::Complex32, vec![2]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let stored_data: [[i16; 2]; 3] = [[10, 20], [30, 40], [50, 60]];
    for arr in &stored_data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Int16(
                        ArrayD::from_shape_vec(IxDyn(&[2]), arr.to_vec()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2]),
                            vec![Complex32::new(0.0, 0.0), Complex32::new(0.0, 0.0)],
                        )
                        .unwrap(),
                    )),
                ),
            ]))
            .unwrap();
    }
    table
        .bind_scaled_complex_column(
            "virtual_col",
            "stored_col",
            Complex64::new(0.5, 0.0),
            Complex64::new(1.0, 0.0),
        )
        .unwrap();
    table.save(TableOptions::new(&tbl_path)).unwrap();

    let reopened =
        Table::open(TableOptions::new(&tbl_path)).expect("Rust should reopen ScaledComplex table");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("virtual_col"));
    assert!(!reopened.is_virtual_column("stored_col"));

    // Verify stored column.
    let expected_stored: [[i16; 2]; 3] = [[10, 20], [30, 40], [50, 60]];
    for (i, exp) in expected_stored.iter().enumerate() {
        match reopened.cell(i, "stored_col").expect("cell exists") {
            Value::Array(ArrayValue::Int16(arr)) => {
                let flat: Vec<i16> = arr.iter().copied().collect();
                assert_eq!(flat, exp, "stored_col row {i} mismatch");
            }
            other => panic!("stored_col row {i}: expected Int16 array, got {other:?}"),
        }
    }

    // Verify virtual column: Complex(stored * 0.5 + 1.0, 0).
    let expected_re: [[f32; 2]; 3] = [[6.0, 11.0], [16.0, 21.0], [26.0, 31.0]];
    for (i, exp) in expected_re.iter().enumerate() {
        match reopened.cell(i, "virtual_col").expect("cell exists") {
            Value::Array(ArrayValue::Complex32(arr)) => {
                let flat: Vec<Complex32> = arr.iter().copied().collect();
                for (j, (got, &want_re)) in flat.iter().zip(exp.iter()).enumerate() {
                    assert!(
                        (got.re - want_re).abs() < 1e-5 && got.im.abs() < 1e-5,
                        "virtual_col row {i} elem {j}: expected ({want_re},0), got ({},{})",
                        got.re,
                        got.im
                    );
                }
            }
            other => panic!("virtual_col row {i}: expected Complex32 array, got {other:?}"),
        }
    }
}
