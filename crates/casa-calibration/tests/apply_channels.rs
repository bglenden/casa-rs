// SPDX-License-Identifier: LGPL-3.0-or-later
//! A channel selection calibrates only its cells and summarizes selected weights.

mod common;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, execute_apply_from_path,
};
use casa_core::{ArrayValue, Complex32, PrimitiveType, Value};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::ColumnSchema;
use ndarray::{ArrayD, IxDyn};

#[test]
fn selected_channels_preserve_other_cells_and_use_the_selected_weight_median() {
    // CASA 6.7.6.14: constant antenna amplitudes 2 divide DATA by 4 and
    // multiply weights by 16. Spectral scalar weights use only selected bins.
    for existing in [false, true] {
        for spectral in [false, true] {
            for calwt in [false, true] {
                for mode in [ApplyMode::CalFlag, ApplyMode::CalOnly] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = common::create_apply_fixture_ms_with_options(
                        dir.path(),
                        existing,
                        spectral,
                    );
                    let mut ms = MeasurementSet::open(&path).unwrap();
                    let unchanged_columns = ["DATA", "FLAG", "WEIGHT", "SIGMA"];
                    let unchanged = unchanged_columns.map(|name| {
                        ms.main_table()
                            .cell_accessor(1, name)
                            .unwrap()
                            .array()
                            .unwrap()
                            .clone()
                    });
                    let previous_other = existing.then(|| {
                        ms.main_table()
                            .cell_accessor(1, "CORRECTED_DATA")
                            .unwrap()
                            .array()
                            .unwrap()
                            .clone()
                    });
                    let ArrayValue::Complex32(data) = ms
                        .main_table()
                        .cell_accessor(0, "DATA")
                        .unwrap()
                        .array()
                        .unwrap()
                    else {
                        panic!("DATA")
                    };
                    let input = data.clone();
                    let previous = input.mapv(|value| value * Complex32::new(7., 2.));
                    let write = |ms: &mut MeasurementSet, name: &str, value| {
                        ms.main_table_mut()
                            .cell_accessor_mut(0, name)
                            .unwrap()
                            .set(Value::Array(value))
                            .unwrap();
                    };
                    if existing {
                        write(
                            &mut ms,
                            "CORRECTED_DATA",
                            ArrayValue::Complex32(previous.clone()),
                        );
                    }
                    write(&mut ms, "WEIGHT", ArrayValue::from_f32_vec(vec![3.; 2]));
                    write(&mut ms, "SIGMA", ArrayValue::from_f32_vec(vec![2.; 2]));
                    if spectral {
                        ms.main_table_mut()
                            .add_column(
                                ColumnSchema::array_variable(
                                    "SIGMA_SPECTRUM",
                                    PrimitiveType::Float32,
                                    Some(2),
                                ),
                                Some(Value::Array(ArrayValue::Float32(
                                    ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1., 2., 1., 2.])
                                        .unwrap(),
                                ))),
                            )
                            .unwrap();
                        write(
                            &mut ms,
                            "SIGMA_SPECTRUM",
                            ArrayValue::Float32(
                                ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1., 2., 1., 2.])
                                    .unwrap(),
                            ),
                        );
                        write(
                            &mut ms,
                            "WEIGHT_SPECTRUM",
                            ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[2, 2]), 99.)),
                        );
                    }
                    ms.save().unwrap();
                    drop(ms);
                    let rows = [0, 1].map(|antenna| common::SyntheticGainSolutionRow {
                        time_seconds: 100.,
                        field_id: 0,
                        spectral_window_id: 0,
                        antenna_id: antenna,
                        gains: vec![Complex32::new(2., 0.); 2],
                        flags: vec![false; 2],
                    });
                    let gain = common::create_apply_gain_caltable(
                        &dir.path().join("gain.gcal"),
                        &["TARGET0"],
                        &rows,
                    );
                    let mut spec = ApplyCalibrationTableSpec::new(gain);
                    spec.calwt = calwt;
                    execute_apply_from_path(
                        &path,
                        &ApplyPlanRequest {
                            selection: MsSelection::new().spw_selector("0:0").unwrap(),
                            apply_mode: mode,
                            parang: false,
                            calibration_tables: vec![spec],
                        },
                    )
                    .unwrap();
                    let ms = MeasurementSet::open(&path).unwrap();
                    let column = |name| {
                        ms.main_table()
                            .cell_accessor(0, name)
                            .unwrap()
                            .array()
                            .unwrap()
                    };
                    let ArrayValue::Complex32(corrected) = column("CORRECTED_DATA") else {
                        panic!("CORRECTED_DATA")
                    };
                    let ArrayValue::Float32(weight) = column("WEIGHT") else {
                        panic!("WEIGHT")
                    };
                    for corr in 0..2 {
                        assert_eq!(corrected[[corr, 0]], input[[corr, 0]] / 4.);
                        assert_eq!(
                            corrected[[corr, 1]],
                            if existing {
                                previous[[corr, 1]]
                            } else {
                                input[[corr, 1]]
                            }
                        );
                        assert_eq!(
                            weight[[corr]],
                            if !calwt {
                                3.
                            } else if spectral {
                                16.
                            } else {
                                4.
                            }
                        );
                        if spectral {
                            let ArrayValue::Float32(weights) = column("WEIGHT_SPECTRUM") else {
                                panic!("WEIGHT_SPECTRUM")
                            };
                            assert_eq!(weights[[corr, 0]], if calwt { 16. } else { 99. });
                            assert_eq!(weights[[corr, 1]], 99.);
                        }
                    }
                    assert_eq!(column("SIGMA"), &ArrayValue::from_f32_vec(vec![2.; 2]));
                    for (name, before) in unchanged_columns.into_iter().zip(unchanged) {
                        assert_eq!(
                            ms.main_table()
                                .cell_accessor(1, name)
                                .unwrap()
                                .array()
                                .unwrap(),
                            &before,
                            "unselected row {name}"
                        );
                    }
                    if let Some(before) = previous_other {
                        assert_eq!(
                            ms.main_table()
                                .cell_accessor(1, "CORRECTED_DATA")
                                .unwrap()
                                .array()
                                .unwrap(),
                            &before
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn selected_bandpass_channel_uses_its_original_grid_position() {
    // The independent analytic BPOLY fixture has amplitude 2 at channel 0
    // and 4 at channel 1 for antenna 0, and unity for antenna 1.
    for calwt in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = common::create_apply_fixture_ms(dir.path(), false);
        let rows = [0, 1].map(|antenna| common::SyntheticBPolySolutionRow {
            time_seconds: 100.,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: antenna,
            scale_factor: Complex32::new(if antenna == 0 { 8_f32.sqrt() } else { 1. }, 0.),
            valid_domain_hz: [1.0e9, 1.001e9],
            amp_coefficients: vec![vec![0., if antenna == 0 { 2_f64.ln() / 2. } else { 0. }]; 2],
            phase_coefficients: vec![vec![0.]; 2],
            phase_units: "RADIANS",
        });
        let gain = common::create_apply_bpoly_caltable(&dir.path().join("bandpass.bpoly"), &rows);
        let mut spec = ApplyCalibrationTableSpec::new(gain);
        spec.calwt = calwt;
        execute_apply_from_path(
            &path,
            &ApplyPlanRequest {
                selection: MsSelection::new().spw_selector("0:1").unwrap(),
                apply_mode: ApplyMode::CalFlag,
                parang: false,
                calibration_tables: vec![spec],
            },
        )
        .unwrap();
        let ms = MeasurementSet::open(&path).unwrap();
        let ArrayValue::Complex32(corrected) = ms
            .main_table()
            .cell_accessor(0, "CORRECTED_DATA")
            .unwrap()
            .array()
            .unwrap()
        else {
            panic!("CORRECTED_DATA")
        };
        assert_eq!(corrected[[0, 0]], Complex32::new(1., 0.));
        assert_eq!(corrected[[1, 0]], Complex32::new(0., 1.));
        assert!((corrected[[0, 1]].re - 0.5).abs() < 1e-6);
        assert!((corrected[[1, 1]].im - 0.5).abs() < 1e-6);
    }
}

#[test]
fn descending_ms_bandpass_is_independent_of_calwt_after_frequency_resampling() {
    use casa_calibration::ApplyInterpolationMode;
    use casa_core::ScalarValue;
    use casa_ms::schema::SubtableId;
    use casa_tables::{Table, TableOptions};
    for calwt in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = common::create_apply_fixture_ms(dir.path(), false);
        let mut ms = MeasurementSet::open(&path).unwrap();
        ms.subtable_mut(SubtableId::SpectralWindow)
            .unwrap()
            .cell_accessor_mut(0, "CHAN_FREQ")
            .unwrap()
            .set(Value::Array(ArrayValue::from_f64_vec(vec![1.001e9, 1.0e9])))
            .unwrap();
        ms.save().unwrap();
        drop(ms);
        let rows = [0, 1].map(|antenna| common::SyntheticGainSolutionRow {
            time_seconds: 100.,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: antenna,
            gains: vec![Complex32::new(1., 0.); 2],
            flags: vec![false; 2],
        });
        let gain = common::create_apply_gain_caltable(
            &dir.path().join("bandpass.bcal"),
            &["TARGET0"],
            &rows,
        );
        let mut table = Table::open(TableOptions::new(&gain)).unwrap();
        let mut info = table.info().clone();
        info.sub_type = "B Jones".into();
        table.set_info(info);
        table.keywords_mut().upsert(
            "VisCal",
            Value::Scalar(ScalarValue::String("B Jones".into())),
        );
        for (column, element, value) in [
            (
                "CPARAM",
                PrimitiveType::Complex32,
                ArrayValue::Complex32(ArrayD::from_elem(IxDyn(&[2, 2]), Complex32::new(1., 0.))),
            ),
            (
                "FLAG",
                PrimitiveType::Bool,
                ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[2, 2]), false)),
            ),
        ] {
            table.remove_column(column).unwrap();
            table
                .add_column(
                    ColumnSchema::array_variable(column, element, Some(2)),
                    Some(Value::Array(value)),
                )
                .unwrap();
        }
        table
            .cell_accessor_mut(0, "CPARAM")
            .unwrap()
            .set(Value::Array(ArrayValue::Complex32(
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        Complex32::new(2., 0.),
                        Complex32::new(4., 0.),
                        Complex32::new(2., 0.),
                        Complex32::new(4., 0.),
                    ],
                )
                .unwrap(),
            )))
            .unwrap();
        table.save(TableOptions::new(&gain)).unwrap();
        drop(table);
        let mut spec = ApplyCalibrationTableSpec::new(gain);
        spec.calwt = calwt;
        spec.interp = ApplyInterpolationMode::NearestLinear;
        execute_apply_from_path(
            &path,
            &ApplyPlanRequest {
                selection: MsSelection::new().spw_selector("0:1").unwrap(),
                apply_mode: ApplyMode::CalFlag,
                parang: false,
                calibration_tables: vec![spec],
            },
        )
        .unwrap();
        let ms = MeasurementSet::open(&path).unwrap();
        let ArrayValue::Complex32(corrected) = ms
            .main_table()
            .cell_accessor(0, "CORRECTED_DATA")
            .unwrap()
            .array()
            .unwrap()
        else {
            panic!("CORRECTED_DATA")
        };
        // Selected original channel 1 is at 1 GHz: antenna amplitudes 2 and 1.
        // Channel 0 is unselected; both results are independent of calwt.
        assert_eq!(corrected[[0, 0]], Complex32::new(1., 0.));
        assert_eq!(corrected[[1, 0]], Complex32::new(0., 1.));
        assert_eq!(corrected[[0, 1]], Complex32::new(1., 0.));
        assert_eq!(corrected[[1, 1]], Complex32::new(0., 1.));
    }
}
