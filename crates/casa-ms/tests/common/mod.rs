// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

pub mod casa_plotms;

use std::path::{Path, PathBuf};

use casa_ms::MeasurementSetBuilder;
use casa_ms::column_def::{ColumnDef, ColumnKind};
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::schema::{self, SubtableId};
use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
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
/// DATA, WEIGHT, SIGMA, and caller-controlled flagged samples.
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
    populate_subtables(&mut ms);

    for row in 0..4usize {
        let mut flags = vec![false; NUM_CORR * NUM_CHAN];
        for &(flag_row, corr, chan) in flagged_samples {
            if flag_row == row && corr < NUM_CORR && chan < NUM_CHAN {
                flags[corr * NUM_CHAN + chan] = true;
            }
        }
        add_main_row(
            &mut ms,
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
    ms.save().expect("save MS");
    ms_path
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

/// Create an on-disk MS fixture with channelized weight diagnostics and
/// caller-controlled FLAG_ROW values.
pub fn create_msexplore_spectrum_fixture_ms(
    root: &Path,
    include_sigma_spectrum: bool,
    flag_rows: &[usize],
) -> PathBuf {
    let ms_path = root.join("msexplore_spectrum_fixture.ms");
    let mut builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::WeightSpectrum);
    if include_sigma_spectrum {
        builder = builder.with_main_column(OptionalMainColumn::SigmaSpectrum);
    }
    let mut ms = MeasurementSet::create(&ms_path, builder).expect("create MS");
    populate_subtables(&mut ms);

    for row in 0..4usize {
        let mut fields = vec![
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
                    ArrayD::from_shape_vec(vec![3], vec![30.0 + row as f64, 40.0, 0.0]).unwrap(),
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
                "WEIGHT_SPECTRUM",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[NUM_CORR, NUM_CHAN]).f(),
                        (0..NUM_CORR)
                            .flat_map(|corr| {
                                (0..NUM_CHAN)
                                    .map(move |chan| ((row + 1) * 1000 + corr * 100 + chan) as f32)
                            })
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
            (
                "FLAG_ROW",
                Value::Scalar(ScalarValue::Bool(flag_rows.contains(&row))),
            ),
        ];
        if include_sigma_spectrum {
            fields.push((
                "SIGMA_SPECTRUM",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[NUM_CORR, NUM_CHAN]).f(),
                        (0..NUM_CORR)
                            .flat_map(|corr| {
                                (0..NUM_CHAN).map(move |chan| {
                                    ((row + 1) as f32) + corr as f32 / 10.0 + chan as f32 / 1000.0
                                })
                            })
                            .collect(),
                    )
                    .unwrap(),
                )),
            ));
        }
        add_main_row(&mut ms, &fields);
    }
    ms.save().expect("save MS");
    ms_path
}

/// Create an on-disk MS fixture with realistic antenna positions and a field
/// direction suitable for derived observational-geometry axes.
pub fn create_msexplore_geometry_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("msexplore_geometry_fixture.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MS");

    {
        let mut ant = ms.antenna_mut().expect("antenna");
        ant.add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [-1601185.4, -5041977.5, 3554875.9],
            [0.0; 3],
            25.0,
        )
        .unwrap();
        ant.add_antenna(
            "VLA02",
            "N02",
            "GROUND-BASED",
            "ALT-AZ",
            [-1601085.4, -5041977.5, 3554875.9],
            [0.0; 3],
            25.0,
        )
        .unwrap();
    }

    {
        let field_table = ms.subtable_mut(SubtableId::Field).expect("field");
        let direction =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![0.5, 0.8]).unwrap());
        let row = make_subtable_row(
            schema::field::REQUIRED_COLUMNS,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("GEOMETRY_FIELD".to_string())),
                ),
                ("CODE", Value::Scalar(ScalarValue::String("G".to_string()))),
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
        let observation_table = ms
            .subtable_mut(SubtableId::Observation)
            .expect("observation");
        let row = make_subtable_row(
            schema::observation::REQUIRED_COLUMNS,
            &[
                (
                    "TELESCOPE_NAME",
                    Value::Scalar(ScalarValue::String("VLA".to_string())),
                ),
                (
                    "TIME_RANGE",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![2],
                            vec![TIME_BASE_SECONDS, TIME_BASE_SECONDS + 14_400.0],
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "OBSERVER",
                    Value::Scalar(ScalarValue::String("test".to_string())),
                ),
                (
                    "LOG",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![1], vec![String::new()]).unwrap(),
                    )),
                ),
                (
                    "SCHEDULE_TYPE",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                (
                    "SCHEDULE",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![1], vec![String::new()]).unwrap(),
                    )),
                ),
                (
                    "PROJECT",
                    Value::Scalar(ScalarValue::String("geometry-test".to_string())),
                ),
                (
                    "RELEASE_DATE",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS)),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        observation_table.add_row(row).unwrap();
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

    for row in 0..4usize {
        add_main_row(
            &mut ms,
            &[
                ("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                ("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                ("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(
                        TIME_BASE_SECONDS + row as f64 * 3600.0,
                    )),
                ),
                (
                    "TIME_CENTROID",
                    Value::Scalar(ScalarValue::Float64(
                        TIME_BASE_SECONDS + row as f64 * 3600.0,
                    )),
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

#[allow(dead_code)]
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
            casa_types::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casa_types::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casa_types::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casa_types::PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
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
                casa_types::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                casa_types::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
                casa_types::PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total]).unwrap(),
                )),
                casa_types::PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(shape, vec![0; total]).unwrap(),
                )),
                casa_types::PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}
