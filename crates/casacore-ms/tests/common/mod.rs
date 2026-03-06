// SPDX-License-Identifier: LGPL-3.0-or-later

use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::{self, SubtableId};
use casacore_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, ShapeBuilder};

pub const NUM_CORR: usize = 4;
pub const NUM_CHAN: usize = 16;
pub const TIME_BASE_SECONDS: f64 = 59_000.0 * 86_400.0;

/// Create a Complex32 visibility array with shape `[num_corr, num_chan]`
/// and deterministic values based on the row index.
pub fn make_vis_data(row: usize) -> ArrayValue {
    let offset = (row * NUM_CORR * NUM_CHAN) as f32;
    let vals: Vec<Complex32> = (0..NUM_CORR * NUM_CHAN)
        .map(|i| Complex32::new(offset + i as f32, -(offset + i as f32) * 0.5))
        .collect();
    ArrayValue::Complex32(
        ArrayD::from_shape_vec(ndarray::IxDyn(&[NUM_CORR, NUM_CHAN]).f(), vals).unwrap(),
    )
}

/// Verify that a read-back visibility array matches the expected values for a row.
#[allow(dead_code)]
pub fn verify_vis_data(arr: &ArrayValue, row: usize) {
    let offset = (row * NUM_CORR * NUM_CHAN) as f32;
    match arr {
        ArrayValue::Complex32(a) => {
            assert_eq!(
                a.shape(),
                &[NUM_CORR, NUM_CHAN],
                "row {row}: shape mismatch"
            );
            let mut i = 0;
            for chan in 0..NUM_CHAN {
                for corr in 0..NUM_CORR {
                    let expected = Complex32::new(offset + i as f32, -(offset + i as f32) * 0.5);
                    let actual = a[[corr, chan]];
                    assert!(
                        (actual.re - expected.re).abs() < 1e-5
                            && (actual.im - expected.im).abs() < 1e-5,
                        "row {row} [{corr},{chan}]: expected {expected}, got {actual}"
                    );
                    i += 1;
                }
            }
        }
        other => panic!(
            "row {row}: expected Complex32, got {:?}",
            other.primitive_type()
        ),
    }
}

/// Populate the subtables needed by the interop and perf fixtures.
pub fn populate_subtables(ms: &mut MeasurementSet) {
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

pub fn populate_main_rows(ms: &mut MeasurementSet, num_rows: usize) {
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

pub fn add_main_row(ms: &mut MeasurementSet, overrides: &[(&str, Value)]) {
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

fn default_value_for_def(c: &ColumnDef) -> Value {
    match c.column_kind {
        ColumnKind::Scalar => match c.data_type {
            casacore_types::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casacore_types::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casacore_types::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casacore_types::PrimitiveType::String => {
                Value::Scalar(ScalarValue::String(String::new()))
            }
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
                casacore_types::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(shape, vec![0; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}
