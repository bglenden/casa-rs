// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2×2 interop tests for quantum columns (RR, RC, CR, CC).
//!
//! All tests require C++ casacore to be available (`has_casacore_cpp`).

#![cfg(has_casacore_cpp)]

use casacore_tables::table_quantum::{ArrayQuantColumn, ScalarQuantColumn, TableQuantumDesc};
use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_test_support::table_quantum_interop::{
    cpp_create_quantum_table, cpp_read_quantum_table, cpp_verify_quantum_table,
};
use casacore_types::*;

/// Build a Rust table with the same layout as the C++ shim creates.
fn create_rust_quantum_table(path: &str) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ScaFixedDeg", PrimitiveType::Float64),
        ColumnSchema::scalar("ScaVarUnits", PrimitiveType::Float64),
        ColumnSchema::scalar("ScaUnitCol", PrimitiveType::String),
        ColumnSchema::array_fixed("ArrFixed", PrimitiveType::Float64, vec![4]),
        ColumnSchema::array_fixed("ArrVarPerRow", PrimitiveType::Float64, vec![3]),
        ColumnSchema::scalar("ArrUnitScaCol", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // Attach quantum descriptors.
    TableQuantumDesc::with_unit("ScaFixedDeg", "deg")
        .write(&mut table)
        .unwrap();
    TableQuantumDesc::with_variable_units("ScaVarUnits", "ScaUnitCol")
        .write(&mut table)
        .unwrap();
    TableQuantumDesc::with_unit("ArrFixed", "MHz")
        .write(&mut table)
        .unwrap();
    TableQuantumDesc::with_variable_units("ArrVarPerRow", "ArrUnitScaCol")
        .write(&mut table)
        .unwrap();

    // Row 0
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("ScaFixedDeg", Value::Scalar(ScalarValue::Float64(45.0))),
            RecordField::new("ScaVarUnits", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new(
                "ScaUnitCol",
                Value::Scalar(ScalarValue::String("Jy".to_owned())),
            ),
            RecordField::new(
                "ArrFixed",
                Value::Array(ArrayValue::from_f64_vec(vec![100.0, 200.0, 300.0, 400.0])),
            ),
            RecordField::new(
                "ArrVarPerRow",
                Value::Array(ArrayValue::from_f64_vec(vec![10.0, 20.0, 30.0])),
            ),
            RecordField::new(
                "ArrUnitScaCol",
                Value::Scalar(ScalarValue::String("km".to_owned())),
            ),
        ]))
        .unwrap();

    // Row 1
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("ScaFixedDeg", Value::Scalar(ScalarValue::Float64(90.0))),
            RecordField::new("ScaVarUnits", Value::Scalar(ScalarValue::Float64(2.5))),
            RecordField::new(
                "ScaUnitCol",
                Value::Scalar(ScalarValue::String("mJy".to_owned())),
            ),
            RecordField::new(
                "ArrFixed",
                Value::Array(ArrayValue::from_f64_vec(vec![500.0, 600.0, 700.0, 800.0])),
            ),
            RecordField::new(
                "ArrVarPerRow",
                Value::Array(ArrayValue::from_f64_vec(vec![40.0, 50.0, 60.0])),
            ),
            RecordField::new(
                "ArrUnitScaCol",
                Value::Scalar(ScalarValue::String("m".to_owned())),
            ),
        ]))
        .unwrap();

    // Row 2
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("ScaFixedDeg", Value::Scalar(ScalarValue::Float64(180.0))),
            RecordField::new("ScaVarUnits", Value::Scalar(ScalarValue::Float64(2.71))),
            RecordField::new(
                "ScaUnitCol",
                Value::Scalar(ScalarValue::String("Jy".to_owned())),
            ),
            RecordField::new(
                "ArrFixed",
                Value::Array(ArrayValue::from_f64_vec(vec![
                    900.0, 1000.0, 1100.0, 1200.0,
                ])),
            ),
            RecordField::new(
                "ArrVarPerRow",
                Value::Array(ArrayValue::from_f64_vec(vec![70.0, 80.0, 90.0])),
            ),
            RecordField::new(
                "ArrUnitScaCol",
                Value::Scalar(ScalarValue::String("cm".to_owned())),
            ),
        ]))
        .unwrap();

    table
        .save(TableOptions::new(path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();
}

// ─── RR: Rust write → Rust read ─────────────────────────────────────────────

#[test]
fn rr_scalar_fixed_and_variable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rr_quantum");
    let path_str = path.to_str().unwrap();

    create_rust_quantum_table(path_str);

    let table = Table::open(TableOptions::new(path_str)).unwrap();

    // Fixed-unit scalar.
    let col = ScalarQuantColumn::new(&table, "ScaFixedDeg").unwrap();
    let q0 = col.get(0).unwrap();
    assert!((q0.value() - 45.0).abs() < 1e-12);
    assert_eq!(q0.unit().name(), "deg");

    // Variable-unit scalar.
    let col_var = ScalarQuantColumn::new(&table, "ScaVarUnits").unwrap();
    let qv0 = col_var.get(0).unwrap();
    assert!((qv0.value() - 1.5).abs() < 1e-12);
    assert_eq!(qv0.unit().name(), "Jy");

    let qv1 = col_var.get(1).unwrap();
    assert!((qv1.value() - 2.5).abs() < 1e-12);
    assert_eq!(qv1.unit().name(), "mJy");
}

#[test]
fn rr_array_fixed_and_variable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rr_quantum_arr");
    let path_str = path.to_str().unwrap();

    create_rust_quantum_table(path_str);

    let table = Table::open(TableOptions::new(path_str)).unwrap();

    // Fixed-unit array.
    let col = ArrayQuantColumn::new(&table, "ArrFixed").unwrap();
    let q0 = col.get(0).unwrap();
    assert_eq!(q0.len(), 4);
    assert!((q0[0].value() - 100.0).abs() < 1e-12);
    assert_eq!(q0[0].unit().name(), "MHz");

    // Variable-unit array (per-row).
    let col_var = ArrayQuantColumn::new(&table, "ArrVarPerRow").unwrap();
    let qv0 = col_var.get(0).unwrap();
    assert_eq!(qv0[0].unit().name(), "km");
    let qv1 = col_var.get(1).unwrap();
    assert_eq!(qv1[0].unit().name(), "m");
}

// ─── CR: C++ write → Rust read ──────────────────────────────────────────────

#[test]
fn cr_scalar_fixed_and_variable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_quantum");
    let path_str = path.to_str().unwrap();

    cpp_create_quantum_table(path_str).expect("C++ create should succeed");

    let table = Table::open(TableOptions::new(path_str)).unwrap();

    // Fixed-unit scalar.
    assert!(TableQuantumDesc::has_quanta(&table, "ScaFixedDeg"));
    let col = ScalarQuantColumn::new(&table, "ScaFixedDeg").unwrap();
    let q0 = col.get(0).unwrap();
    assert!((q0.value() - 45.0).abs() < 1e-12);
    assert_eq!(q0.unit().name(), "deg");

    let q1 = col.get(1).unwrap();
    assert!((q1.value() - 90.0).abs() < 1e-12);

    // Variable-unit scalar.
    let col_var = ScalarQuantColumn::new(&table, "ScaVarUnits").unwrap();
    let qv0 = col_var.get(0).unwrap();
    assert!((qv0.value() - 1.5).abs() < 1e-12);
    assert_eq!(qv0.unit().name(), "Jy");

    let qv1 = col_var.get(1).unwrap();
    assert!((qv1.value() - 2.5).abs() < 1e-12);
    assert_eq!(qv1.unit().name(), "mJy");
}

#[test]
fn cr_array_fixed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_quantum_arr");
    let path_str = path.to_str().unwrap();

    cpp_create_quantum_table(path_str).expect("C++ create should succeed");

    let table = Table::open(TableOptions::new(path_str)).unwrap();

    let col = ArrayQuantColumn::new(&table, "ArrFixed").unwrap();
    let q0 = col.get(0).unwrap();
    assert_eq!(q0.len(), 4);
    assert!((q0[0].value() - 100.0).abs() < 1e-12);
    assert_eq!(q0[0].unit().name(), "MHz");
}

#[test]
fn cr_array_variable_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_quantum_arr_var");
    let path_str = path.to_str().unwrap();

    cpp_create_quantum_table(path_str).expect("C++ create should succeed");

    let table = Table::open(TableOptions::new(path_str)).unwrap();

    let col = ArrayQuantColumn::new(&table, "ArrVarPerRow").unwrap();
    assert!(col.is_unit_variable());

    let q0 = col.get(0).unwrap();
    assert_eq!(q0.len(), 3);
    assert!((q0[0].value() - 10.0).abs() < 1e-12);
    assert_eq!(q0[0].unit().name(), "km");

    let q1 = col.get(1).unwrap();
    assert_eq!(q1[0].unit().name(), "m");

    let q2 = col.get(2).unwrap();
    assert_eq!(q2[0].unit().name(), "cm");
}

// ─── RC: Rust write → C++ read ──────────────────────────────────────────────

#[test]
fn rc_scalar_verify_from_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_quantum");
    let path_str = path.to_str().unwrap();

    create_rust_quantum_table(path_str);

    // C++ reads the values.
    let (fixed, var, units) = cpp_read_quantum_table(path_str).expect("C++ read should succeed");

    assert!((fixed[0] - 45.0).abs() < 1e-12);
    assert!((fixed[1] - 90.0).abs() < 1e-12);
    assert!((fixed[2] - 180.0).abs() < 1e-12);

    assert!((var[0] - 1.5).abs() < 1e-12);
    assert!((var[1] - 2.5).abs() < 1e-12);
    assert!((var[2] - 2.71).abs() < 1e-12);

    assert_eq!(units[0], "Jy");
    assert_eq!(units[1], "mJy");
    assert_eq!(units[2], "Jy");
}

#[test]
fn rc_verify_quantum_keywords_from_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_quantum_verify");
    let path_str = path.to_str().unwrap();

    create_rust_quantum_table(path_str);

    let ok = cpp_verify_quantum_table(path_str).expect("C++ verify should succeed");
    assert!(ok, "C++ verification of Rust-written quantum table failed");
}

// ─── CC: C++ write → C++ read ───────────────────────────────────────────────

#[test]
fn cc_cpp_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cc_quantum");
    let path_str = path.to_str().unwrap();

    cpp_create_quantum_table(path_str).expect("C++ create should succeed");

    let (fixed, var, units) = cpp_read_quantum_table(path_str).expect("C++ read should succeed");

    assert!((fixed[0] - 45.0).abs() < 1e-12);
    assert!((fixed[1] - 90.0).abs() < 1e-12);
    assert!((var[0] - 1.5).abs() < 1e-12);
    assert_eq!(units[0], "Jy");
    assert_eq!(units[1], "mJy");
}
