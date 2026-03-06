// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2×2 interop tests for measure columns (RR, RC, CR, CC).
//!
//! All C++-dependent tests require `has_casacore_cpp`.

#![cfg(has_casacore_cpp)]

use casacore_tables::table_measures::*;
use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_test_support::table_measures_interop::*;
use casacore_types::measures::direction::DirectionRef;
use casacore_types::measures::epoch::EpochRef;
use casacore_types::*;

fn tmp_path(name: &str) -> String {
    format!("/private/tmp/claude-501/table_meas_{name}")
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

// ─── CR: C++ writes, Rust reads ─────────────────────────────────────────────

#[test]
fn cr_epoch_fixed() {
    let path = tmp_path("cr_epoch_fixed");
    cleanup(&path);
    cpp_create_epoch_fixed(&path).unwrap();

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();

    let e0 = col.get_epoch(0).unwrap();
    assert_eq!(e0.refer(), EpochRef::UTC);
    assert!((e0.value().as_mjd() - 51544.5).abs() < 1e-9);

    let e1 = col.get_epoch(1).unwrap();
    assert!((e1.value().as_mjd() - 51545.0).abs() < 1e-9);

    let e2 = col.get_epoch(2).unwrap();
    assert!((e2.value().as_mjd() - 51546.5).abs() < 1e-9);

    cleanup(&path);
}

#[test]
fn cr_epoch_var_int() {
    let path = tmp_path("cr_epoch_var_int");
    cleanup(&path);
    cpp_create_epoch_var_int(&path).unwrap();

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();

    let e0 = col.get_epoch(0).unwrap();
    assert_eq!(e0.refer(), EpochRef::UTC);
    assert!((e0.value().as_mjd() - 51544.5).abs() < 1e-9);

    let e1 = col.get_epoch(1).unwrap();
    assert_eq!(e1.refer(), EpochRef::TAI);

    let e2 = col.get_epoch(2).unwrap();
    assert_eq!(e2.refer(), EpochRef::TT); // C++ TDT = Rust TT

    cleanup(&path);
}

#[test]
fn cr_epoch_var_str() {
    let path = tmp_path("cr_epoch_var_str");
    cleanup(&path);
    cpp_create_epoch_var_str(&path).unwrap();

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();

    let e0 = col.get_epoch(0).unwrap();
    assert_eq!(e0.refer(), EpochRef::UTC);

    let e1 = col.get_epoch(1).unwrap();
    assert_eq!(e1.refer(), EpochRef::TAI);

    // C++ writes "TDT" string, Rust should parse it as TT
    let e2 = col.get_epoch(2).unwrap();
    assert_eq!(e2.refer(), EpochRef::TT);

    cleanup(&path);
}

#[test]
fn cr_direction_fixed() {
    let path = tmp_path("cr_direction_fixed");
    cleanup(&path);
    cpp_create_direction_fixed(&path).unwrap();

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let col = ScalarMeasColumn::new(&table, "DIR").unwrap();

    let d0 = col.get_direction(0).unwrap();
    assert_eq!(d0.refer(), DirectionRef::J2000);
    let (lon, lat) = d0.as_angles();
    assert!((lon - 1.0).abs() < 1e-9);
    assert!((lat - 0.5).abs() < 1e-9);

    let d2 = col.get_direction(2).unwrap();
    let (lon, lat) = d2.as_angles();
    assert!((lon - 0.0).abs() < 1e-9);
    assert!((lat - 1.5).abs() < 1e-9);

    cleanup(&path);
}

// ─── RC: Rust writes, C++ reads ─────────────────────────────────────────────

#[test]
fn rc_epoch_fixed() {
    let path = tmp_path("rc_epoch_fixed");
    cleanup(&path);

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC")
        .write(&mut table)
        .unwrap();

    let mjds = [51544.5, 51545.0, 51546.5];
    for &mjd in &mjds {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "TIME",
                Value::Array(ArrayValue::from_f64_vec(vec![mjd])),
            )]))
            .unwrap();
    }
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();

    cpp_verify_epochs(&path, "TIME", &mjds, &["UTC", "UTC", "UTC"]).unwrap();
    cleanup(&path);
}

#[test]
fn rc_epoch_var_int() {
    let path = tmp_path("rc_epoch_var_int");
    cleanup(&path);

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("TimeRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let (types, codes) = default_epoch_ref_map();
    TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
        .unwrap()
        .write(&mut table)
        .unwrap();

    let data: [(f64, EpochRef); 3] = [
        (51544.5, EpochRef::UTC),
        (51545.0, EpochRef::TAI),
        (51546.5, EpochRef::TT),
    ];
    for &(mjd, refer) in &data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![mjd]))),
                RecordField::new(
                    "TimeRef",
                    Value::Scalar(ScalarValue::Int32(refer.casacore_code())),
                ),
            ]))
            .unwrap();
    }
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();

    // C++ reads back — note TT shows as "TDT" in older casacore
    // Use the shim's read function instead of verify for flexibility
    let (mjds, refs) = cpp_read_epochs(&path, "TIME", 3).unwrap();
    assert!((mjds[0] - 51544.5).abs() < 1e-9);
    assert!((mjds[1] - 51545.0).abs() < 1e-9);
    assert!((mjds[2] - 51546.5).abs() < 1e-9);
    assert_eq!(refs[0], "UTC");
    assert_eq!(refs[1], "TAI");
    // C++ may show TDT or TT depending on version
    assert!(refs[2] == "TDT" || refs[2] == "TT", "got: {}", refs[2]);

    cleanup(&path);
}

#[test]
fn rc_direction_fixed() {
    let path = tmp_path("rc_direction_fixed");
    cleanup(&path);

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "DIR",
        PrimitiveType::Float64,
        vec![2],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    TableMeasDesc::new_fixed("DIR", MeasureType::Direction, "J2000")
        .write(&mut table)
        .unwrap();

    let dirs: [(f64, f64); 3] = [(1.0, 0.5), (2.0, -0.3), (0.0, 1.5)];
    for &(lon, lat) in &dirs {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DIR",
                Value::Array(ArrayValue::from_f64_vec(vec![lon, lat])),
            )]))
            .unwrap();
    }
    table
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StManAipsIO))
        .unwrap();

    let expected_vals: Vec<f64> = dirs.iter().flat_map(|&(l, b)| vec![l, b]).collect();
    cpp_verify_directions(&path, "DIR", &expected_vals, &["J2000", "J2000", "J2000"]).unwrap();

    cleanup(&path);
}

// ─── CC: C++ writes + reads (baseline) ──────────────────────────────────────

#[test]
fn cc_epoch_roundtrip() {
    let path = tmp_path("cc_epoch_roundtrip");
    cleanup(&path);

    cpp_create_epoch_fixed(&path).unwrap();
    let (mjds, refs) = cpp_read_epochs(&path, "TIME", 3).unwrap();

    assert!((mjds[0] - 51544.5).abs() < 1e-9);
    assert!((mjds[1] - 51545.0).abs() < 1e-9);
    assert!((mjds[2] - 51546.5).abs() < 1e-9);
    assert_eq!(refs[0], "UTC");
    assert_eq!(refs[1], "UTC");
    assert_eq!(refs[2], "UTC");

    cleanup(&path);
}
