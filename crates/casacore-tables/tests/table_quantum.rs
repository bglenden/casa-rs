// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rust-only unit tests for quantum column support.
//!
//! Tests `TableQuantumDesc`, `ScalarQuantColumn`, `ScalarQuantColumnMut`,
//! `ArrayQuantColumn`, and `ArrayQuantColumnMut`.

use casacore_tables::table_quantum::{
    ArrayQuantColumn, ArrayQuantColumnMut, ScalarQuantColumn, ScalarQuantColumnMut,
    TableQuantumDesc,
};
use casacore_tables::{ColumnSchema, Table, TableSchema};
use casacore_types::quanta::Quantity;
use casacore_types::*;

/// Helper: create a table with scalar Float64 columns.
fn scalar_table(cols: &[&str]) -> Table {
    let schema = TableSchema::new(
        cols.iter()
            .map(|&c| ColumnSchema::scalar(c, PrimitiveType::Float64))
            .collect(),
    )
    .unwrap();
    Table::with_schema(schema)
}

/// Helper: create a table with a Float64 data column and a String units column.
fn scalar_with_units_column(data_col: &str, units_col: &str) -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(data_col, PrimitiveType::Float64),
        ColumnSchema::scalar(units_col, PrimitiveType::String),
    ])
    .unwrap();
    Table::with_schema(schema)
}

// ─── TableQuantumDesc ───────────────────────────────────────────────────────

#[test]
fn desc_fixed_unit_roundtrip() {
    let mut table = scalar_table(&["flux"]);
    let desc = TableQuantumDesc::with_unit("flux", "Jy");
    desc.write(&mut table).unwrap();

    assert!(TableQuantumDesc::has_quanta(&table, "flux"));

    let recovered = TableQuantumDesc::reconstruct(&table, "flux").unwrap();
    assert_eq!(recovered.column_name(), "flux");
    assert_eq!(recovered.units(), &["Jy"]);
    assert!(!recovered.is_unit_variable());
    assert!(recovered.unit_column_name().is_none());
}

#[test]
fn desc_fixed_multiple_units_roundtrip() {
    let mut table = scalar_table(&["freq"]);
    let desc = TableQuantumDesc::with_units("freq", &["MHz", "GHz"]);
    desc.write(&mut table).unwrap();

    let recovered = TableQuantumDesc::reconstruct(&table, "freq").unwrap();
    assert_eq!(recovered.units(), &["MHz", "GHz"]);
}

#[test]
fn desc_variable_units_roundtrip() {
    let mut table = scalar_with_units_column("flux", "flux_unit");
    let desc = TableQuantumDesc::with_variable_units("flux", "flux_unit");
    desc.write(&mut table).unwrap();

    assert!(TableQuantumDesc::has_quanta(&table, "flux"));

    let recovered = TableQuantumDesc::reconstruct(&table, "flux").unwrap();
    assert!(recovered.is_unit_variable());
    assert_eq!(recovered.unit_column_name(), Some("flux_unit"));
    assert!(recovered.units().is_empty());
}

#[test]
fn has_quanta_returns_false_for_plain_column() {
    let table = scalar_table(&["flux"]);
    assert!(!TableQuantumDesc::has_quanta(&table, "flux"));
}

#[test]
fn reconstruct_returns_none_for_plain_column() {
    let table = scalar_table(&["flux"]);
    assert!(TableQuantumDesc::reconstruct(&table, "flux").is_none());
}

#[test]
fn desc_write_overwrites_fixed_with_variable() {
    let mut table = scalar_with_units_column("flux", "flux_unit");

    // Write fixed first.
    TableQuantumDesc::with_unit("flux", "Jy")
        .write(&mut table)
        .unwrap();
    assert_eq!(
        TableQuantumDesc::reconstruct(&table, "flux")
            .unwrap()
            .units(),
        &["Jy"]
    );

    // Overwrite with variable.
    TableQuantumDesc::with_variable_units("flux", "flux_unit")
        .write(&mut table)
        .unwrap();
    let recovered = TableQuantumDesc::reconstruct(&table, "flux").unwrap();
    assert!(recovered.is_unit_variable());
    assert!(recovered.units().is_empty());
}

// ─── ScalarQuantColumn (fixed unit) ─────────────────────────────────────────

#[test]
fn scalar_fixed_unit_read_write() {
    let mut table = scalar_table(&["flux"]);
    TableQuantumDesc::with_unit("flux", "Jy")
        .write(&mut table)
        .unwrap();

    // Add a row.
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "flux",
            Value::Scalar(ScalarValue::Float64(1.5)),
        )]))
        .unwrap();

    let col = ScalarQuantColumn::new(&table, "flux").unwrap();
    let q = col.get(0).unwrap();
    assert!((q.value() - 1.5).abs() < 1e-12);
    assert_eq!(q.unit().name(), "Jy");
}

#[test]
fn scalar_fixed_unit_on_read_conversion() {
    let mut table = scalar_table(&["angle"]);
    TableQuantumDesc::with_unit("angle", "deg")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "angle",
            Value::Scalar(ScalarValue::Float64(180.0)),
        )]))
        .unwrap();

    let col = ScalarQuantColumn::with_unit(&table, "angle", "rad").unwrap();
    let q = col.get(0).unwrap();
    assert!((q.value() - std::f64::consts::PI).abs() < 1e-10);
    assert_eq!(q.unit().name(), "rad");
}

#[test]
fn scalar_fixed_unit_write_with_conversion() {
    let mut table = scalar_table(&["freq"]);
    TableQuantumDesc::with_unit("freq", "Hz")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "freq",
            Value::Scalar(ScalarValue::Float64(0.0)),
        )]))
        .unwrap();

    {
        let mut col = ScalarQuantColumnMut::new(&mut table, "freq").unwrap();
        let q = Quantity::new(1.0, "MHz").unwrap();
        col.put(0, &q).unwrap();
    }

    // Should be stored as 1e6 Hz.
    let col = ScalarQuantColumn::new(&table, "freq").unwrap();
    let q = col.get(0).unwrap();
    assert!((q.value() - 1e6).abs() < 1e-6);
    assert_eq!(q.unit().name(), "Hz");
}

// ─── ScalarQuantColumn (variable unit) ──────────────────────────────────────

#[test]
fn scalar_variable_unit_read_write() {
    let mut table = scalar_with_units_column("flux", "flux_unit");
    TableQuantumDesc::with_variable_units("flux", "flux_unit")
        .write(&mut table)
        .unwrap();

    // Row 0: 1.5 Jy
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new(
                "flux_unit",
                Value::Scalar(ScalarValue::String("Jy".to_owned())),
            ),
        ]))
        .unwrap();

    // Row 1: 100.0 mJy
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(100.0))),
            RecordField::new(
                "flux_unit",
                Value::Scalar(ScalarValue::String("mJy".to_owned())),
            ),
        ]))
        .unwrap();

    let col = ScalarQuantColumn::new(&table, "flux").unwrap();
    assert!(col.is_unit_variable());

    let q0 = col.get(0).unwrap();
    assert!((q0.value() - 1.5).abs() < 1e-12);
    assert_eq!(q0.unit().name(), "Jy");

    let q1 = col.get(1).unwrap();
    assert!((q1.value() - 100.0).abs() < 1e-12);
    assert_eq!(q1.unit().name(), "mJy");
}

#[test]
fn scalar_variable_unit_write_via_mut() {
    let mut table = scalar_with_units_column("flux", "flux_unit");
    TableQuantumDesc::with_variable_units("flux", "flux_unit")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(
                "flux_unit",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
        ]))
        .unwrap();

    {
        let mut col = ScalarQuantColumnMut::new(&mut table, "flux").unwrap();
        let q = Quantity::new(2.71, "Jy").unwrap();
        col.put(0, &q).unwrap();
    }

    let col = ScalarQuantColumn::new(&table, "flux").unwrap();
    let q = col.get(0).unwrap();
    assert!((q.value() - 2.71).abs() < 1e-12);
    assert_eq!(q.unit().name(), "Jy");
}

// ─── ArrayQuantColumn (fixed unit) ──────────────────────────────────────────

fn array_table(data_col: &str) -> Table {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        data_col,
        PrimitiveType::Float64,
        vec![4],
    )])
    .unwrap();
    Table::with_schema(schema)
}

#[test]
fn array_fixed_unit_read_write() {
    let mut table = array_table("data");
    TableQuantumDesc::with_unit("data", "MHz")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f64_vec(vec![100.0, 200.0, 300.0, 400.0])),
        )]))
        .unwrap();

    let col = ArrayQuantColumn::new(&table, "data").unwrap();
    let quanta = col.get(0).unwrap();
    assert_eq!(quanta.len(), 4);
    assert!((quanta[0].value() - 100.0).abs() < 1e-12);
    assert_eq!(quanta[0].unit().name(), "MHz");
    assert!((quanta[3].value() - 400.0).abs() < 1e-12);
}

#[test]
fn array_fixed_unit_on_read_conversion() {
    let mut table = array_table("data");
    TableQuantumDesc::with_unit("data", "km")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0, 4.0])),
        )]))
        .unwrap();

    let col = ArrayQuantColumn::with_unit(&table, "data", "m").unwrap();
    let quanta = col.get(0).unwrap();
    assert!((quanta[0].value() - 1000.0).abs() < 1e-8);
    assert_eq!(quanta[0].unit().name(), "m");
}

#[test]
fn array_fixed_unit_write_with_conversion() {
    let mut table = array_table("data");
    TableQuantumDesc::with_unit("data", "m")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f64_vec(vec![0.0; 4])),
        )]))
        .unwrap();

    {
        let mut col = ArrayQuantColumnMut::new(&mut table, "data").unwrap();
        let quanta: Vec<Quantity> = vec![1.0, 2.0, 3.0, 4.0]
            .into_iter()
            .map(|v| Quantity::new(v, "km").unwrap())
            .collect();
        col.put(0, &quanta).unwrap();
    }

    // Values should be stored as meters.
    let col = ArrayQuantColumn::new(&table, "data").unwrap();
    let quanta = col.get(0).unwrap();
    assert!((quanta[0].value() - 1000.0).abs() < 1e-8);
    assert_eq!(quanta[0].unit().name(), "m");
}

// ─── ArrayQuantColumn (variable unit per-row) ───────────────────────────────

#[test]
fn array_variable_unit_per_row() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("data", PrimitiveType::Float64, vec![3]),
        ColumnSchema::scalar("data_unit", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    TableQuantumDesc::with_variable_units("data", "data_unit")
        .write(&mut table)
        .unwrap();

    // Row 0: units in km
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "data",
                Value::Array(ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0])),
            ),
            RecordField::new(
                "data_unit",
                Value::Scalar(ScalarValue::String("km".to_owned())),
            ),
        ]))
        .unwrap();

    // Row 1: units in m
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "data",
                Value::Array(ArrayValue::from_f64_vec(vec![100.0, 200.0, 300.0])),
            ),
            RecordField::new(
                "data_unit",
                Value::Scalar(ScalarValue::String("m".to_owned())),
            ),
        ]))
        .unwrap();

    let col = ArrayQuantColumn::new(&table, "data").unwrap();
    assert!(col.is_unit_variable());

    let q0 = col.get(0).unwrap();
    assert_eq!(q0[0].unit().name(), "km");
    assert!((q0[0].value() - 1.0).abs() < 1e-12);

    let q1 = col.get(1).unwrap();
    assert_eq!(q1[0].unit().name(), "m");
    assert!((q1[0].value() - 100.0).abs() < 1e-12);
}

// ─── ArrayQuantColumn (variable unit per-element) ───────────────────────────

#[test]
fn array_variable_unit_per_element() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("data", PrimitiveType::Float64, vec![3]),
        ColumnSchema::array_fixed("data_unit", PrimitiveType::String, vec![3]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    TableQuantumDesc::with_variable_units("data", "data_unit")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "data",
                Value::Array(ArrayValue::from_f64_vec(vec![1.0, 100.0, 1000.0])),
            ),
            RecordField::new(
                "data_unit",
                Value::Array(ArrayValue::from_string_vec(vec![
                    "km".to_owned(),
                    "m".to_owned(),
                    "mm".to_owned(),
                ])),
            ),
        ]))
        .unwrap();

    let col = ArrayQuantColumn::new(&table, "data").unwrap();
    let quanta = col.get(0).unwrap();
    assert_eq!(quanta.len(), 3);
    assert_eq!(quanta[0].unit().name(), "km");
    assert_eq!(quanta[1].unit().name(), "m");
    assert_eq!(quanta[2].unit().name(), "mm");
}

// ─── Error cases ────────────────────────────────────────────────────────────

#[test]
fn error_no_quantum_keywords() {
    let table = scalar_table(&["flux"]);
    assert!(ScalarQuantColumn::new(&table, "flux").is_err());
    assert!(ArrayQuantColumn::new(&table, "flux").is_err());
}

#[test]
fn error_nonexistent_column() {
    let table = scalar_table(&["flux"]);
    assert!(!TableQuantumDesc::has_quanta(&table, "nonexistent"));
    assert!(TableQuantumDesc::reconstruct(&table, "nonexistent").is_none());
}

// ─── Edge: empty array ──────────────────────────────────────────────────────

#[test]
fn array_fixed_empty() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float64,
        Some(1),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    TableQuantumDesc::with_unit("data", "Jy")
        .write(&mut table)
        .unwrap();

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_f64_vec(vec![])),
        )]))
        .unwrap();

    let col = ArrayQuantColumn::new(&table, "data").unwrap();
    let quanta = col.get(0).unwrap();
    assert!(quanta.is_empty());
}

// ─── Persistence: save → reopen ─────────────────────────────────────────────

#[test]
fn scalar_quantum_survives_save_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quantum_test");

    // Create and save.
    {
        let mut table = scalar_table(&["flux"]);
        TableQuantumDesc::with_unit("flux", "Jy")
            .write(&mut table)
            .unwrap();

        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "flux",
                Value::Scalar(ScalarValue::Float64(2.5)),
            )]))
            .unwrap();

        table
            .save(casacore_tables::TableOptions::new(path.to_str().unwrap()))
            .unwrap();
    }

    // Reopen and verify.
    let table = Table::open(casacore_tables::TableOptions::new(path.to_str().unwrap())).unwrap();
    assert!(TableQuantumDesc::has_quanta(&table, "flux"));

    let col = ScalarQuantColumn::new(&table, "flux").unwrap();
    let q = col.get(0).unwrap();
    assert!((q.value() - 2.5).abs() < 1e-12);
    assert_eq!(q.unit().name(), "Jy");
}

#[test]
fn variable_quantum_survives_save_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quantum_var_test");

    {
        let mut table = scalar_with_units_column("flux", "flux_unit");
        TableQuantumDesc::with_variable_units("flux", "flux_unit")
            .write(&mut table)
            .unwrap();

        table
            .add_row(RecordValue::new(vec![
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(2.71))),
                RecordField::new(
                    "flux_unit",
                    Value::Scalar(ScalarValue::String("Jy".to_owned())),
                ),
            ]))
            .unwrap();

        table
            .save(casacore_tables::TableOptions::new(path.to_str().unwrap()))
            .unwrap();
    }

    let table = Table::open(casacore_tables::TableOptions::new(path.to_str().unwrap())).unwrap();
    let col = ScalarQuantColumn::new(&table, "flux").unwrap();
    let q = col.get(0).unwrap();
    assert!((q.value() - 2.71).abs() < 1e-12);
    assert_eq!(q.unit().name(), "Jy");
}
