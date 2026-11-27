// SPDX-License-Identifier: LGPL-3.0-or-later
//! Crate-local MeasurementSet fixture helpers for `casa-flagging` integration tests.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::schema::{self, SubtableId};
use casa_ms::{ColumnDef, ColumnKind, MeasurementSet, MeasurementSetBuilder};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

pub const NUM_CORR: usize = 4;
pub const NUM_CHAN: usize = 16;
pub const TIME_BASE_SECONDS: f64 = 59_000.0 * 86_400.0;

/// Create an on-disk MS fixture for `msexplore` tests with deterministic
/// DATA, WEIGHT, and SIGMA values and no flagged samples.
pub fn create_msexplore_fixture_ms(root: &Path) -> PathBuf {
    create_msexplore_fixture_ms_with_flags(root, &[])
}

/// Create an on-disk MS fixture for `msexplore` tests with deterministic
/// DATA, WEIGHT, and SIGMA, and caller-controlled flagged samples.
pub fn create_msexplore_fixture_ms_with_flags(
    root: &Path,
    flagged_samples: &[(usize, usize, usize)],
) -> PathBuf {
    let ms_path = root.join("msexplore_fixture.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MS");
    populate_msexplore_baselines(&mut ms, flagged_samples);
    ms.save().expect("save MS");
    ms_path
}

/// Create an in-memory MS fixture for `msexplore` tests.
pub fn create_msexplore_fixture_in_memory() -> MeasurementSet {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create memory MS");
    populate_msexplore_baselines(&mut ms, &[]);
    ms
}

fn populate_msexplore_baselines(
    ms: &mut MeasurementSet,
    flagged_samples: &[(usize, usize, usize)],
) {
    populate_subtables(ms);
    for row in 0..4usize {
        // The FLAG cell is built column-major (`.f()` below), so its
        // (corr, chan) element is `corr + chan * NUM_CORR`.
        let mut flags = vec![false; NUM_CORR * NUM_CHAN];
        for &(flag_row, corr, chan) in flagged_samples {
            if flag_row == row && corr < NUM_CORR && chan < NUM_CHAN {
                flags[corr + chan * NUM_CORR] = true;
            }
        }
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
                (
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![30.0 + row as f64, 40.0, 0.0])
                            .unwrap(),
                    )),
                ),
                ("DATA", Value::Array(make_vis_data(row))),
                (
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![NUM_CORR],
                            (0..NUM_CORR)
                                .map(|corr| ((row + 1) * 100 + corr) as f32)
                                .collect(),
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![NUM_CORR],
                            (0..NUM_CORR)
                                .map(|corr| ((row + 1) as f32) + corr as f32 / 10.0)
                                .collect(),
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[NUM_CORR, NUM_CHAN]).f(), flags).unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
    }
}

/// The [`create_msexplore_fixture_ms_with_flags`] fixture with a second
/// polarization, `corr_type` (casacore Stokes codes), sharing spectral
/// window 0 through DATA_DESC_ID 1.
pub fn create_two_polarization_fixture_ms(
    root: &Path,
    corr_type: [i32; NUM_CORR],
    flagged_samples: &[(usize, usize, usize)],
) -> PathBuf {
    let ms_path = create_msexplore_fixture_ms_with_flags(root, flagged_samples);
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    {
        let pol_table = ms.subtable_mut(SubtableId::Polarization).expect("pol");
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
                (
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![NUM_CORR], corr_type.to_vec()).unwrap(),
                    )),
                ),
                ("CORR_PRODUCT", Value::Array(corr_product)),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        pol_table.add_row(row).unwrap();
    }
    {
        let dd_table = ms.subtable_mut(SubtableId::DataDescription).expect("dd");
        let row = make_subtable_row(
            schema::data_description::REQUIRED_COLUMNS,
            &[
                ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(1))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        dd_table.add_row(row).unwrap();
    }
    for row in 0..4 {
        let second = i32::from(row >= 2);
        for (column, value) in [
            ("DATA_DESC_ID", second),
            ("OBSERVATION_ID", second),
            ("ARRAY_ID", second),
            ("SCAN_NUMBER", 1),
        ] {
            ms.main_table_mut()
                .cell_accessor_mut(row, column)
                .expect("MAIN cell")
                .set(Value::Scalar(ScalarValue::Int32(value)))
                .expect("set MAIN cell");
        }
    }
    ms.save().expect("save MS");
    ms_path
}

fn add_averaging_subtables(ms: &mut MeasurementSet) {
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
        ant.add_antenna(
            "ANT2",
            "STA2",
            "GROUND-BASED",
            "ALT-AZ",
            [200.0, 210.0, 220.0],
            [0.0; 3],
            14.0,
        )
        .unwrap();
    }

    {
        let field_table = ms.subtable_mut(SubtableId::Field).expect("field");
        for (field_id, (name, ra, dec)) in
            [("FIELD0", 1.0_f64, 0.5_f64), ("FIELD1", 1.2_f64, 0.6_f64)]
                .into_iter()
                .enumerate()
        {
            let direction =
                ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap());
            let row = make_subtable_row(
                schema::field::REQUIRED_COLUMNS,
                &[
                    ("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
                    ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                    ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                    ("DELAY_DIR", Value::Array(direction.clone())),
                    ("PHASE_DIR", Value::Array(direction.clone())),
                    ("REFERENCE_DIR", Value::Array(direction)),
                    (
                        "SOURCE_ID",
                        Value::Scalar(ScalarValue::Int32(field_id as i32)),
                    ),
                    (
                        "TIME",
                        Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS)),
                    ),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            field_table.add_row(row).unwrap();
        }
    }

    add_polarization_row(ms);

    {
        let spw_table = ms.subtable_mut(SubtableId::SpectralWindow).expect("spw");
        let f64_arr = |v: &[f64]| {
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![v.len()], v.to_vec()).unwrap())
        };
        for (spw_id, base_frequency_hz) in [1.0e9_f64, 1.1e9_f64].into_iter().enumerate() {
            let freqs: Vec<f64> = (0..NUM_CHAN)
                .map(|i| base_frequency_hz + i as f64 * 1.0e6)
                .collect();
            let widths = vec![1.0e6; NUM_CHAN];
            let row = make_subtable_row(
                schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    (
                        "NUM_CHAN",
                        Value::Scalar(ScalarValue::Int32(NUM_CHAN as i32)),
                    ),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(format!("SPW{spw_id}"))),
                    ),
                    (
                        "REF_FREQUENCY",
                        Value::Scalar(ScalarValue::Float64(base_frequency_hz)),
                    ),
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
                    (
                        "FREQ_GROUP",
                        Value::Scalar(ScalarValue::Int32(spw_id as i32)),
                    ),
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
    }

    {
        let dd_table = ms.subtable_mut(SubtableId::DataDescription).expect("dd");
        for spw_id in 0..2_i32 {
            let row = make_subtable_row(
                schema::data_description::REQUIRED_COLUMNS,
                &[
                    (
                        "SPECTRAL_WINDOW_ID",
                        Value::Scalar(ScalarValue::Int32(spw_id)),
                    ),
                    ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            dd_table.add_row(row).unwrap();
        }
    }
}

/// Create an on-disk MS fixture with multiple scans, fields, baselines, and
/// spectral windows so averaging controls can be exercised deterministically.
pub fn create_msexplore_averaging_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("msexplore_averaging_fixture.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MS");

    add_averaging_subtables(&mut ms);

    let rows = [
        (0_i32, 1_i32, 0_i32, 0_i32, 10_i32, 0_i32, 0.0_f64),
        (0, 1, 0, 0, 10, 0, 30.0),
        (0, 1, 0, 0, 11, 0, 30.0),
        (0, 1, 1, 0, 10, 0, 30.0),
        (0, 2, 0, 0, 10, 0, 0.0),
        (0, 1, 0, 1, 10, 1, 0.0),
        (0, 1, 0, 1, 10, 1, 30.0),
        (0, 2, 0, 1, 10, 1, 0.0),
    ];

    for (row, (antenna1, antenna2, field_id, ddid, scan_number, uvw_offset, time_offset)) in
        rows.into_iter().enumerate()
    {
        add_main_row(
            &mut ms,
            &[
                ("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1))),
                ("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2))),
                ("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
                ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(ddid))),
                (
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS + time_offset)),
                ),
                (
                    "TIME_CENTROID",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS + time_offset)),
                ),
                ("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0))),
                ("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0))),
                (
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                (
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![3],
                            vec![
                                30.0 + uvw_offset as f64 * 10.0 + row as f64,
                                40.0 + uvw_offset as f64 * 5.0,
                                uvw_offset as f64,
                            ],
                        )
                        .unwrap(),
                    )),
                ),
                ("DATA", Value::Array(make_vis_data(row))),
                (
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![NUM_CORR],
                            (0..NUM_CORR)
                                .map(|corr| ((row + 1) * 100 + corr) as f32)
                                .collect(),
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![NUM_CORR],
                            (0..NUM_CORR)
                                .map(|corr| ((row + 1) as f32) + corr as f32 / 10.0)
                                .collect(),
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(
                            IxDyn(&[NUM_CORR, NUM_CHAN]).f(),
                            vec![false; NUM_CORR * NUM_CHAN],
                        )
                        .unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
    }

    ms.save().expect("save MS");
    ms_path
}

fn make_vis_data(row: usize) -> ArrayValue {
    let offset = (row * NUM_CORR * NUM_CHAN) as f32;
    let vals: Vec<Complex32> = (0..NUM_CORR * NUM_CHAN)
        .map(|i| Complex32::new(offset + i as f32, -(offset + i as f32) * 0.5))
        .collect();
    ArrayValue::Complex32(ArrayD::from_shape_vec(IxDyn(&[NUM_CORR, NUM_CHAN]).f(), vals).unwrap())
}

fn add_polarization_row(ms: &mut MeasurementSet) {
    let pol_table = ms.subtable_mut(SubtableId::Polarization).expect("pol");
    let corr_type = ArrayValue::Int32(ArrayD::from_shape_vec(vec![4], vec![5, 6, 7, 8]).unwrap());
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

    add_polarization_row(ms);

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
            casa_core::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casa_core::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casa_core::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casa_core::PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
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
                casa_core::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                casa_core::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
                casa_core::PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total]).unwrap(),
                )),
                casa_core::PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(shape, vec![0; total]).unwrap(),
                )),
                casa_core::PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}
