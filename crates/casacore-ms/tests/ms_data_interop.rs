// SPDX-License-Identifier: LGPL-3.0-or-later
//! MS DATA column interop tests for Rust and C++ casacore.
//!
//! The fixture uses a small but non-trivial MeasurementSet with populated
//! ANTENNA, FIELD, POLARIZATION, SPECTRAL_WINDOW, and DATA_DESCRIPTION
//! subtables plus a Complex32 `DATA` column in the main table.

mod common;

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::{OptionalMainColumn, VisibilityDataColumn};
use casacore_test_support::cpp_backend_available;
use casacore_test_support::ms_interop::{cpp_ms_verify_basic_fixture, cpp_ms_write_basic_fixture};
use common::{NUM_CHAN, NUM_CORR, populate_main_rows, populate_subtables, verify_vis_data};
use ndarray::{ArrayD, ShapeBuilder};

use casacore_types::{ArrayValue, Complex32, ScalarValue, Value};

/// Rust write → Rust read: MS DATA column round-trip.
#[test]
fn ms_data_column_rust_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("data_rt.ms");
    let num_rows = 6;

    {
        let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
        let mut ms = MeasurementSet::create(&ms_path, builder).unwrap();
        populate_subtables(&mut ms);
        populate_main_rows(&mut ms, num_rows);
        ms.save().unwrap();
    }

    let ms = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(ms.row_count(), num_rows);
    assert_eq!(ms.main_table().info().table_type, "Measurement Set");
    assert_eq!(ms.antenna().unwrap().row_count(), 2);
    assert_eq!(ms.field().unwrap().name(0).unwrap(), "TEST_FIELD");

    let data_col = ms.data_column(VisibilityDataColumn::Data).unwrap();
    for row in 0..num_rows {
        assert_eq!(data_col.shape(row).unwrap(), vec![NUM_CORR, NUM_CHAN]);
        verify_vis_data(data_col.get(row).unwrap(), row);
    }
}

/// Rust write → Rust read: verify DataColumnMut::put overwrites correctly.
#[test]
fn ms_data_column_mut_put() {
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create_memory(builder).unwrap();
    populate_subtables(&mut ms);

    let zeros = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            ndarray::IxDyn(&[NUM_CORR, NUM_CHAN]).f(),
            vec![Complex32::new(0.0, 0.0); NUM_CORR * NUM_CHAN],
        )
        .unwrap(),
    );
    common::add_main_row(
        &mut ms,
        &[
            ("DATA", Value::Array(zeros)),
            ("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
        ],
    );

    {
        let mut col = ms.data_column_mut(VisibilityDataColumn::Data).unwrap();
        col.put(0, common::make_vis_data(42)).unwrap();
    }

    let col = ms.data_column(VisibilityDataColumn::Data).unwrap();
    verify_vis_data(col.get(0).unwrap(), 42);
}

/// Rust write → C++ verify: casacore must be able to open the Rust-written MS
/// through `casacore::MeasurementSet` and confirm the data fixture contents.
#[test]
fn ms_data_column_rust_to_cpp_round_trip() {
    if !cpp_backend_available() {
        eprintln!("skipping Rust→C++ MS interop test: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("rust_to_cpp.ms");

    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&ms_path, builder).unwrap();
    populate_subtables(&mut ms);
    populate_main_rows(&mut ms, 6);
    ms.save().unwrap();

    cpp_ms_verify_basic_fixture(&ms_path).expect("C++ should verify Rust-written MS");
}

/// C++ write → Rust read: Rust must be able to open a MeasurementSet created
/// with casacore's `MeasurementSet` and read back the same typed structure.
#[test]
fn ms_data_column_cpp_to_rust_round_trip() {
    if !cpp_backend_available() {
        eprintln!("skipping C++→Rust MS interop test: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("cpp_to_rust.ms");

    cpp_ms_write_basic_fixture(&ms_path).expect("C++ should write fixture MS");

    let ms = MeasurementSet::open(&ms_path).unwrap();
    let issues = ms.validate().unwrap();
    assert!(issues.is_empty(), "Validation issues: {issues:?}");

    assert_eq!(ms.main_table().info().table_type, "Measurement Set");
    assert_eq!(ms.row_count(), 6);

    let antenna = ms.antenna().unwrap();
    assert_eq!(antenna.row_count(), 2);
    assert_eq!(antenna.name(0).unwrap(), "ANT0");
    assert_eq!(antenna.name(1).unwrap(), "ANT1");

    let field = ms.field().unwrap();
    assert_eq!(field.row_count(), 1);
    assert_eq!(field.name(0).unwrap(), "TEST_FIELD");

    let pol = ms.polarization().unwrap();
    assert_eq!(pol.row_count(), 1);
    assert_eq!(pol.num_corr(0).unwrap(), NUM_CORR as i32);

    let spw = ms.spectral_window().unwrap();
    assert_eq!(spw.row_count(), 1);
    assert_eq!(spw.num_chan(0).unwrap(), NUM_CHAN as i32);
    assert_eq!(spw.name(0).unwrap(), "SPW0");

    let data_col = ms.data_column(VisibilityDataColumn::Data).unwrap();
    verify_vis_data(data_col.get(0).unwrap(), 0);
    verify_vis_data(data_col.get(5).unwrap(), 5);
}
