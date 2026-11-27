// SPDX-License-Identifier: LGPL-3.0-or-later
//! Performance comparison for MeasurementSet create/open/read workloads.

#![cfg(feature = "performance-tests")]

use std::time::Instant;

use casa_core::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use casa_ms::schema::{self, SubtableId};
use casa_ms::{
    ColumnDef, ColumnKind, MeasurementSet, MeasurementSetBuilder, OptionalMainColumn,
    VisibilityDataColumn,
};
use casa_test_support::casacore_oracle_available;
use casa_test_support::ms_interop::MeasurementSetOracle;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const NUM_CORR: usize = 4;
const NUM_CHAN: usize = 16;
const TIME_BASE_SECONDS: f64 = 59_000.0 * 86_400.0;

fn default_value_for_def(c: &ColumnDef) -> Value {
    match c.column_kind {
        ColumnKind::Scalar => match c.data_type {
            PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
            _ => Value::Scalar(ScalarValue::Float64(0.0)),
        },
        ColumnKind::FixedArray { shape } => {
            let total: usize = shape.iter().product();
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
            ))
        }
        ColumnKind::VariableArray { ndim } => {
            let shape: Vec<usize> = vec![1; ndim];
            let total: usize = shape.iter().product();
            match c.data_type {
                PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
                PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total]).unwrap(),
                )),
                PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(shape, vec![0; total]).unwrap(),
                )),
                PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}

fn make_subtable_row(defs: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields: Vec<RecordField> = defs
        .iter()
        .map(|c| {
            if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == c.name) {
                RecordField::new(c.name, v.clone())
            } else {
                RecordField::new(c.name, default_value_for_def(c))
            }
        })
        .collect();
    RecordValue::new(fields)
}

fn make_vis_data(row: usize) -> ArrayValue {
    let offset = (row * NUM_CORR * NUM_CHAN) as f32;
    let vals: Vec<Complex32> = (0..NUM_CORR * NUM_CHAN)
        .map(|i| Complex32::new(offset + i as f32, -(offset + i as f32) * 0.5))
        .collect();
    ArrayValue::Complex32(ArrayD::from_shape_vec(IxDyn(&[NUM_CORR, NUM_CHAN]).f(), vals).unwrap())
}

fn add_main_row(ms: &mut MeasurementSet, overrides: &[(&str, Value)]) {
    let schema = ms.main_table().schema().unwrap().clone();
    let all_cols: Vec<&ColumnDef> = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .collect();

    let fields: Vec<RecordField> = schema
        .columns()
        .iter()
        .map(|col| {
            if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                return RecordField::new(col.name(), v.clone());
            }
            if let Some(c) = all_cols.iter().find(|c| c.name == col.name()) {
                RecordField::new(col.name(), default_value_for_def(c))
            } else {
                RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(0)))
            }
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

fn populate_subtables(ms: &mut MeasurementSet) {
    {
        let mut ant = ms.antenna_mut().expect("antenna");
        ant.add_antenna(
            "ANT0",
            "STA0",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0, 10.0, 20.0],
            [0.0; 3],
            12.0,
        )
        .unwrap();
        ant.add_antenna(
            "ANT1",
            "STA1",
            "GROUND-BASED",
            "ALT-AZ",
            [100.0, 110.0, 120.0],
            [0.0; 3],
            13.0,
        )
        .unwrap();
    }

    {
        let field_table = ms.subtable_mut(SubtableId::Field).expect("field");
        let direction =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![1.0, 0.5]).unwrap());
        let row = make_subtable_row(
            schema::field::REQUIRED_COLUMNS,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("TEST_FIELD".to_string())),
                ),
                ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                ("DELAY_DIR", Value::Array(direction.clone())),
                ("PHASE_DIR", Value::Array(direction.clone())),
                ("REFERENCE_DIR", Value::Array(direction)),
                ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                (
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS)),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        field_table.add_row(row).unwrap();
    }

    {
        let pol_table = ms.subtable_mut(SubtableId::Polarization).expect("pol");
        let corr_type =
            ArrayValue::Int32(ArrayD::from_shape_vec(vec![4], vec![5, 6, 7, 8]).unwrap());
        let corr_product = ArrayValue::Int32(
            ArrayD::from_shape_vec(vec![2, 4], vec![0, 0, 1, 1, 0, 1, 0, 1]).unwrap(),
        );
        let row = make_subtable_row(
            schema::polarization::REQUIRED_COLUMNS,
            &[
                (
                    "NUM_CORR",
                    Value::Scalar(ScalarValue::Int32(NUM_CORR as i32)),
                ),
                ("CORR_TYPE", Value::Array(corr_type)),
                ("CORR_PRODUCT", Value::Array(corr_product)),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        pol_table.add_row(row).unwrap();
    }

    {
        let spw_table = ms.subtable_mut(SubtableId::SpectralWindow).expect("spw");
        let freqs: Vec<f64> = (0..NUM_CHAN).map(|i| 1.0e9 + i as f64 * 1.0e6).collect();
        let widths = vec![1.0e6; NUM_CHAN];
        let f64_arr = |v: &[f64]| {
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![v.len()], v.to_vec()).unwrap())
        };
        let row = make_subtable_row(
            schema::spectral_window::REQUIRED_COLUMNS,
            &[
                (
                    "NUM_CHAN",
                    Value::Scalar(ScalarValue::Int32(NUM_CHAN as i32)),
                ),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("SPW0".to_string())),
                ),
                ("REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(1.0e9))),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(NUM_CHAN as f64 * 1.0e6)),
                ),
                ("CHAN_FREQ", Value::Array(f64_arr(&freqs))),
                ("CHAN_WIDTH", Value::Array(f64_arr(&widths))),
                ("EFFECTIVE_BW", Value::Array(f64_arr(&widths))),
                ("RESOLUTION", Value::Array(f64_arr(&widths))),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        spw_table.add_row(row).unwrap();
    }

    {
        let dd_table = ms.subtable_mut(SubtableId::DataDescription).expect("dd");
        let row = make_subtable_row(
            schema::data_description::REQUIRED_COLUMNS,
            &[
                ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        dd_table.add_row(row).unwrap();
    }
}

fn populate_main_rows(ms: &mut MeasurementSet, num_rows: usize) {
    for row in 0..num_rows {
        add_main_row(
            ms,
            &[
                ("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                ("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                ("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS + row as f64)),
                ),
                (
                    "TIME_CENTROID",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS + row as f64)),
                ),
                ("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0))),
                ("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0))),
                (
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(row as i32 + 1)),
                ),
                ("DATA", Value::Array(make_vis_data(row))),
            ],
        );
    }
}

#[test]
fn ms_create_open_read_perf_vs_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping ms_perf_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: usize = 2_048;

    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_path = dir.path().join("cpp_perf.ms");
    let rust_path = dir.path().join("rust_perf.ms");

    let cpp = MeasurementSetOracle::bench_create_open(&cpp_path, NROWS as u64)
        .expect("C++ MeasurementSet benchmark should succeed");

    let t0 = Instant::now();
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&rust_path, builder).unwrap();
    populate_subtables(&mut ms);
    populate_main_rows(&mut ms, NROWS);
    ms.save().unwrap();
    let rust_create_ns = t0.elapsed().as_nanos() as u64;

    let t0 = Instant::now();
    let ms = MeasurementSet::open(&rust_path).unwrap();
    let rust_open_ns = t0.elapsed().as_nanos() as u64;

    let data_col = ms.data_column(VisibilityDataColumn::Data).unwrap();
    let mut sink = 0.0f32;
    let t0 = Instant::now();
    for row in 0..NROWS {
        match data_col.get(row).unwrap() {
            ArrayValue::Complex32(arr) => sink += arr[[0, 0]].re,
            other => panic!(
                "expected Complex32 DATA column, got {:?}",
                other.primitive_type()
            ),
        }
    }
    let rust_read_ns = t0.elapsed().as_nanos() as u64;

    assert!(sink >= 0.0);

    let create_ratio = rust_create_ns as f64 / cpp.create_ns.max(1) as f64;
    let open_ratio = rust_open_ns as f64 / cpp.open_ns.max(1) as f64;
    let read_ratio = rust_read_ns as f64 / cpp.read_ns.max(1) as f64;

    eprintln!(
        "MeasurementSet perf ({NROWS} rows, DATA[{NUM_CORR}/{NUM_CHAN}]):\n  \
         Create+save: C++ {:.1} ms, Rust {:.1} ms, ratio {create_ratio:.1}x\n  \
         Open:        C++ {:.1} ms, Rust {:.1} ms, ratio {open_ratio:.1}x\n  \
         Read DATA:   C++ {:.1} ms, Rust {:.1} ms, ratio {read_ratio:.1}x",
        cpp.create_ns as f64 / 1e6,
        rust_create_ns as f64 / 1e6,
        cpp.open_ns as f64 / 1e6,
        rust_open_ns as f64 / 1e6,
        cpp.read_ns as f64 / 1e6,
        rust_read_ns as f64 / 1e6,
    );

    let max_ratio = create_ratio.max(open_ratio).max(read_ratio);
    if max_ratio > 2.0 {
        eprintln!("  warning: Rust MeasurementSet workload is {max_ratio:.1}x slower than C++");
    }
}
