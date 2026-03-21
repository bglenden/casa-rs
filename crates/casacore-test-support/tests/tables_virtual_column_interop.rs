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
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

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
        match table
            .cell(i, "col_value")
            .expect("cell lookup")
            .expect("cell exists")
        {
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
        match reopened
            .cell(i, "col_value")
            .expect("cell lookup")
            .expect("cell exists")
        {
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
        match table
            .cell(i, "stored_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
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
        match table
            .cell(i, "virtual_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
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
        match reopened
            .cell(i, "stored_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
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
        match reopened
            .cell(i, "virtual_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
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

    // ScaledComplexData stores complex values as [2, ...] real arrays where
    // the first axis holds [re, im]. For 2 Complex32 elements per row, the
    // stored column must have shape [2, 2] (4 Int16 values) and the virtual
    // column shape [2] (2 Complex32 values).
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("stored_col", PrimitiveType::Int16, vec![2, 2]),
        ColumnSchema::array_fixed("virtual_col", PrimitiveType::Complex32, vec![2]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    // stored_col rows: [2,2] arrays in Fortran order [re0, im0, re1, im1]
    let stored_data: [[i16; 4]; 3] = [[10, 20, 30, 40], [50, 60, 70, 80], [2, 4, 6, 8]];
    for arr in &stored_data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Int16(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]), arr.to_vec()).unwrap(),
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
            Complex64::new(0.5, 0.25),
            Complex64::new(1.0, 2.0),
        )
        .unwrap();
    table.save(TableOptions::new(&tbl_path)).unwrap();

    let reopened =
        Table::open(TableOptions::new(&tbl_path)).expect("Rust should reopen ScaledComplex table");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("virtual_col"));
    assert!(!reopened.is_virtual_column("stored_col"));

    // Verify stored column shape and values.
    for (i, exp) in stored_data.iter().enumerate() {
        match reopened
            .cell(i, "stored_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Array(ArrayValue::Int16(arr)) => {
                assert_eq!(arr.shape(), &[2, 2], "stored_col row {i} shape");
                let flat: Vec<i16> = arr.iter().copied().collect();
                assert_eq!(flat, exp, "stored_col row {i} mismatch");
            }
            other => panic!("stored_col row {i}: expected Int16 array, got {other:?}"),
        }
    }

    // Verify virtual column with nonzero imaginary scale/offset.
    // scale=(0.5, 0.25), offset=(1.0, 2.0).
    // Stored [2,2] array: axis 0 splits re/im via index_axis.
    //   re_virtual = re_stored * 0.5 + 1.0
    //   im_virtual = im_stored * 0.25 + 2.0
    // Row 0: stored [10,20,30,40] → re=[10,20], im=[30,40]
    //   → [(6.0, 9.5), (11.0, 12.0)]
    // Row 1: stored [50,60,70,80] → re=[50,60], im=[70,80]
    //   → [(26.0, 19.5), (31.0, 22.0)]
    // Row 2: stored [2,4,6,8] → re=[2,4], im=[6,8]
    //   → [(2.0, 3.5), (3.0, 4.0)]
    let expected: [[(f32, f32); 2]; 3] = [
        [(6.0, 9.5), (11.0, 12.0)],
        [(26.0, 19.5), (31.0, 22.0)],
        [(2.0, 3.5), (3.0, 4.0)],
    ];
    for (i, exp_row) in expected.iter().enumerate() {
        match reopened
            .cell(i, "virtual_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Array(ArrayValue::Complex32(arr)) => {
                assert_eq!(arr.shape(), &[2], "virtual_col row {i} shape");
                let flat: Vec<Complex32> = arr.iter().copied().collect();
                for (j, (got, &(want_re, want_im))) in flat.iter().zip(exp_row.iter()).enumerate() {
                    assert!(
                        (got.re - want_re).abs() < 1e-5 && (got.im - want_im).abs() < 1e-5,
                        "virtual_col row {i} elem {j}: expected ({want_re},{want_im}), got ({},{})",
                        got.re,
                        got.im
                    );
                }
            }
            other => panic!("virtual_col row {i}: expected Complex32 array, got {other:?}"),
        }
    }
}

// ===================== Additional RR-only tests =====================

/// RR: Forward column engine with Float32 arrays.
#[test]
fn rr_forward_column_arrays() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let base_path = dir.path().join("base.tbl");
    let fwd_path = dir.path().join("fwd.tbl");

    let base_schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 3],
    )])
    .unwrap();
    let mut base = Table::with_schema(base_schema);
    let arrays: [Vec<f32>; 3] = [
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ];
    for arr in &arrays {
        base.add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&[2, 3]).f(), arr.clone()).unwrap(),
            )),
        )]))
        .unwrap();
    }
    base.save(TableOptions::new(&base_path)).unwrap();

    let fwd_schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 3],
    )])
    .unwrap();
    let mut fwd = Table::with_schema(fwd_schema);
    for _ in 0..3 {
        fwd.add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&[2, 3]).f(), vec![0.0; 6]).unwrap(),
            )),
        )]))
        .unwrap();
    }
    fwd.bind_forward_column("data", &base_path).unwrap();
    fwd.save(TableOptions::new(&fwd_path)).unwrap();

    let reopened = Table::open(TableOptions::new(&fwd_path)).expect("reopen");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("data"));
    for (i, exp) in arrays.iter().enumerate() {
        match reopened
            .cell(i, "data")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Array(ArrayValue::Float32(arr)) => {
                assert_eq!(arr.shape(), &[2, 3], "row {i} shape");
                // Compare in memory order (Fortran) to match how data was created.
                let got: Vec<f32> = arr.as_slice_memory_order().unwrap().to_vec();
                assert_eq!(got, *exp, "row {i} data mismatch");
            }
            other => panic!("row {i}: expected Float32 array, got {other:?}"),
        }
    }
}

/// RR: Forward column engine with multiple scalar types.
#[test]
fn rr_forward_column_multi_type() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let base_path = dir.path().join("base.tbl");
    let fwd_path = dir.path().join("fwd.tbl");

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
    ])
    .unwrap();

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("hello".to_string())),
            ),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(3.125))),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(-7))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("world".to_string())),
            ),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(-99.5))),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("col_str", Value::Scalar(ScalarValue::String(String::new()))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(0.0))),
        ]),
    ];

    let mut base = Table::with_schema(schema.clone());
    for row in &rows {
        base.add_row(row.clone()).unwrap();
    }
    base.save(TableOptions::new(&base_path)).unwrap();

    let mut fwd = Table::with_schema(schema);
    for row in &rows {
        fwd.add_row(row.clone()).unwrap();
    }
    for col in ["col_i32", "col_str", "col_f64"] {
        fwd.bind_forward_column(col, &base_path).unwrap();
    }
    fwd.save(TableOptions::new(&fwd_path)).unwrap();

    let reopened = Table::open(TableOptions::new(&fwd_path)).expect("reopen");
    assert_eq!(reopened.row_count(), 3);
    for col in ["col_i32", "col_str", "col_f64"] {
        assert!(reopened.is_virtual_column(col), "{col} should be virtual");
    }

    // Verify values.
    let expected_i32 = [42, -7, 0];
    let expected_str = ["hello", "world", ""];
    let expected_f64 = [3.125, -99.5, 0.0];
    for i in 0..3 {
        match reopened
            .cell(i, "col_i32")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Scalar(ScalarValue::Int32(v)) => {
                assert_eq!(*v, expected_i32[i], "row {i} i32")
            }
            other => panic!("row {i}: expected Int32, got {other:?}"),
        }
        match reopened
            .cell(i, "col_str")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Scalar(ScalarValue::String(v)) => {
                assert_eq!(v, expected_str[i], "row {i} str")
            }
            other => panic!("row {i}: expected String, got {other:?}"),
        }
        match reopened
            .cell(i, "col_f64")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!((*v - expected_f64[i]).abs() < 1e-10, "row {i} f64")
            }
            other => panic!("row {i}: expected Float64, got {other:?}"),
        }
    }
}

/// RR: ScaledArrayEngine with Float32 stored → Float64 virtual.
#[test]
fn rr_scaled_array_float_to_float() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled_f2f.tbl");

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("stored_col", PrimitiveType::Float32, vec![3]),
        ColumnSchema::array_fixed("virtual_col", PrimitiveType::Float64, vec![3]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let stored_data: [[f32; 3]; 3] = [[1.0, 2.0, 3.0], [10.0, 20.0, 30.0], [0.0, -5.0, 100.0]];
    for arr in &stored_data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[3]), arr.to_vec()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(IxDyn(&[3]), vec![0.0; 3]).unwrap(),
                    )),
                ),
            ]))
            .unwrap();
    }
    table
        .bind_scaled_array_column("virtual_col", "stored_col", 0.1, 5.0)
        .unwrap();
    table.save(TableOptions::new(&tbl_path)).unwrap();

    let reopened = Table::open(TableOptions::new(&tbl_path)).expect("reopen");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("virtual_col"));

    // virtual = stored * 0.1 + 5.0
    let expected: [[f64; 3]; 3] = [[5.1, 5.2, 5.3], [6.0, 7.0, 8.0], [5.0, 4.5, 15.0]];
    for (i, exp) in expected.iter().enumerate() {
        match reopened
            .cell(i, "virtual_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Array(ArrayValue::Float64(arr)) => {
                let flat: Vec<f64> = arr.iter().copied().collect();
                for (j, (&got, &want)) in flat.iter().zip(exp.iter()).enumerate() {
                    assert!(
                        (got - want).abs() < 1e-5,
                        "row {i} elem {j}: expected {want}, got {got}"
                    );
                }
            }
            other => panic!("row {i}: expected Float64 array, got {other:?}"),
        }
    }
}

/// RR: Forward column with Bool scalar type.
#[test]
fn rr_forward_column_bool() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let base_path = dir.path().join("base.tbl");
    let fwd_path = dir.path().join("fwd.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("flag", PrimitiveType::Bool)]).unwrap();

    let mut base = Table::with_schema(schema.clone());
    for v in [true, false, true] {
        base.add_row(RecordValue::new(vec![RecordField::new(
            "flag",
            Value::Scalar(ScalarValue::Bool(v)),
        )]))
        .unwrap();
    }
    base.save(TableOptions::new(&base_path)).unwrap();

    let mut fwd = Table::with_schema(schema);
    for _ in 0..3 {
        fwd.add_row(RecordValue::new(vec![RecordField::new(
            "flag",
            Value::Scalar(ScalarValue::Bool(false)),
        )]))
        .unwrap();
    }
    fwd.bind_forward_column("flag", &base_path).unwrap();
    fwd.save(TableOptions::new(&fwd_path)).unwrap();

    let reopened = Table::open(TableOptions::new(&fwd_path)).expect("reopen");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("flag"));

    let expected = [true, false, true];
    for (i, &exp) in expected.iter().enumerate() {
        match reopened
            .cell(i, "flag")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Scalar(ScalarValue::Bool(v)) => {
                assert_eq!(*v, exp, "row {i}: expected {exp}, got {v}");
            }
            other => panic!("row {i}: expected Bool, got {other:?}"),
        }
    }
}

/// RR: ScaledComplexData with nonzero imaginary scale/offset and 3 virtual elements.
#[test]
fn rr_scaled_complex_nonzero_imag() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let tbl_path = dir.path().join("scaled_cx_imag.tbl");

    // stored_col: Int16 [2,3] → virtual_col: Complex32 [3]
    // scale=(1.0, 0.5), offset=(0.0, 1.0)
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("stored_col", PrimitiveType::Int16, vec![2, 3]),
        ColumnSchema::array_fixed("virtual_col", PrimitiveType::Complex32, vec![3]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // Stored [2,3] arrays: axis 0 = [re, im], axis 1 = elements.
    // Row 0: re=[1,2,3], im=[10,20,30]
    // Row 1: re=[4,5,6], im=[40,50,60]
    // Row 2: re=[0,0,0], im=[0,0,0]
    let stored_data: [[i16; 6]; 3] = [
        [1, 2, 3, 10, 20, 30],
        [4, 5, 6, 40, 50, 60],
        [0, 0, 0, 0, 0, 0],
    ];
    for arr in &stored_data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Int16(
                        ArrayD::from_shape_vec(IxDyn(&[2, 3]), arr.to_vec()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[3]), vec![Complex32::new(0.0, 0.0); 3])
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
            Complex64::new(1.0, 0.5),
            Complex64::new(0.0, 1.0),
        )
        .unwrap();
    table.save(TableOptions::new(&tbl_path)).unwrap();

    let reopened = Table::open(TableOptions::new(&tbl_path)).expect("reopen");
    assert_eq!(reopened.row_count(), 3);
    assert!(reopened.is_virtual_column("virtual_col"));

    // re_virtual = re_stored * 1.0 + 0.0 = re_stored
    // im_virtual = im_stored * 0.5 + 1.0
    // Row 0: re=[1,2,3], im=[10*0.5+1, 20*0.5+1, 30*0.5+1] = [6, 11, 16]
    // Row 1: re=[4,5,6], im=[40*0.5+1, 50*0.5+1, 60*0.5+1] = [21, 26, 31]
    // Row 2: re=[0,0,0], im=[0*0.5+1, 0*0.5+1, 0*0.5+1] = [1, 1, 1]
    let expected: [[(f32, f32); 3]; 3] = [
        [(1.0, 6.0), (2.0, 11.0), (3.0, 16.0)],
        [(4.0, 21.0), (5.0, 26.0), (6.0, 31.0)],
        [(0.0, 1.0), (0.0, 1.0), (0.0, 1.0)],
    ];
    for (i, exp_row) in expected.iter().enumerate() {
        match reopened
            .cell(i, "virtual_col")
            .expect("cell lookup")
            .expect("cell exists")
        {
            Value::Array(ArrayValue::Complex32(arr)) => {
                assert_eq!(arr.shape(), &[3], "virtual_col row {i} shape");
                let flat: Vec<Complex32> = arr.iter().copied().collect();
                for (j, (got, &(want_re, want_im))) in flat.iter().zip(exp_row.iter()).enumerate() {
                    assert!(
                        (got.re - want_re).abs() < 1e-5 && (got.im - want_im).abs() < 1e-5,
                        "row {i} elem {j}: expected ({want_re},{want_im}), got ({},{})",
                        got.re,
                        got.im
                    );
                }
            }
            other => panic!("row {i}: expected Complex32 array, got {other:?}"),
        }
    }
}
