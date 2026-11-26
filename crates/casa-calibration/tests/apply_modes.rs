// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's flag-only modes preserve data/weights; public tasks write provenance.

mod common;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, execute_apply_from_path,
};
use casa_ms::{MeasurementSet, MsSelection};
use casa_types::{ArrayValue, Complex32, Value};
use ndarray::{ArrayD, IxDyn};

#[test]
fn empty_selection_is_an_error_in_every_mode_with_casa_column_setup() {
    for mode in [
        ApplyMode::CalFlag,
        ApplyMode::CalFlagStrict,
        ApplyMode::CalOnly,
        ApplyMode::FlagOnly,
        ApplyMode::FlagOnlyStrict,
        ApplyMode::Trial,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = common::create_apply_fixture_ms(dir.path(), false);
        let rows = [0, 1].map(|antenna_id| common::SyntheticGainSolutionRow {
            time_seconds: 100.,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id,
            gains: vec![Complex32::new(2., 0.); 2],
            flags: vec![false; 2],
        });
        let gain =
            common::create_apply_gain_caltable(&dir.path().join("gain.gcal"), &["TARGET0"], &rows);
        let error = execute_apply_from_path(
            &path,
            &ApplyPlanRequest {
                selection: MsSelection {
                    scan: Some("999".into()),
                    ..MsSelection::new()
                },
                apply_mode: mode,
                parang: false,
                calibration_tables: vec![ApplyCalibrationTableSpec::new(gain)],
            },
        )
        .expect_err("Calibrater::selectvis rejects zero rows before correction");
        assert!(error.to_string().contains("selection produced no rows"));
        let ms = MeasurementSet::open(&path).unwrap();
        let writes_data = matches!(
            mode,
            ApplyMode::CalFlag | ApplyMode::CalFlagStrict | ApplyMode::CalOnly
        );
        assert_eq!(
            ms.main_table()
                .schema()
                .unwrap()
                .contains_column("CORRECTED_DATA"),
            writes_data
        );
        if writes_data {
            for row in 0..ms.row_count() {
                let cell = |column| {
                    ms.main_table()
                        .cell_accessor(row, column)
                        .unwrap()
                        .array()
                        .unwrap()
                };
                assert_eq!(cell("CORRECTED_DATA"), cell("DATA"));
            }
        }
    }
}

#[test]
fn flag_only_modes_preserve_corrected_columns_and_weights() {
    // CASA task_applycal.makecorr and Calibrater::correct2 write only FLAG for
    // FLAGONLY modes. A flagged antenna is identity; its valid partner is 2.
    for mode in [
        ApplyMode::CalFlagStrict,
        ApplyMode::FlagOnly,
        ApplyMode::FlagOnlyStrict,
    ] {
        for existing in [false, true] {
            for spectral in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path =
                    common::create_apply_fixture_ms_with_options(dir.path(), existing, spectral);
                let mut ms = MeasurementSet::open(&path).unwrap();
                let ArrayValue::Complex32(input) = ms
                    .main_table()
                    .cell_accessor(0, "DATA")
                    .unwrap()
                    .array()
                    .unwrap()
                else {
                    panic!("DATA")
                };
                let input = input.clone();
                let previous = input.mapv(|value| value * Complex32::new(7., 2.));
                let put = |ms: &mut MeasurementSet, name: &str, value| {
                    ms.main_table_mut()
                        .cell_accessor_mut(0, name)
                        .unwrap()
                        .set(Value::Array(value))
                        .unwrap();
                };
                if existing {
                    put(
                        &mut ms,
                        "CORRECTED_DATA",
                        ArrayValue::Complex32(previous.clone()),
                    );
                }
                put(&mut ms, "WEIGHT", ArrayValue::from_f32_vec(vec![3.; 2]));
                put(&mut ms, "SIGMA", ArrayValue::from_f32_vec(vec![2.; 2]));
                if spectral {
                    put(
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
                    flags: vec![antenna == 0; 2],
                });
                let gain = common::create_apply_gain_caltable(
                    &dir.path().join("gain.gcal"),
                    &["TARGET0"],
                    &rows,
                );
                let mut spec = ApplyCalibrationTableSpec::new(gain);
                spec.calwt = true;
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
                assert_eq!(column("DATA"), &ArrayValue::Complex32(input.clone()));
                assert_eq!(column("SIGMA"), &ArrayValue::from_f32_vec(vec![2.; 2]));
                assert_eq!(
                    column("WEIGHT"),
                    &ArrayValue::from_f32_vec(vec![
                        if mode == ApplyMode::CalFlagStrict {
                            1.
                        } else {
                            3.
                        };
                        2
                    ])
                );
                let ArrayValue::Bool(flags) = column("FLAG") else {
                    panic!("FLAG")
                };
                for corr in 0..2 {
                    assert!(flags[[corr, 0]]);
                    assert!(!flags[[corr, 1]]);
                }
                if existing || mode == ApplyMode::CalFlagStrict {
                    let ArrayValue::Complex32(corrected) = column("CORRECTED_DATA") else {
                        panic!("CORRECTED_DATA")
                    };
                    for corr in 0..2 {
                        assert_eq!(
                            corrected[[corr, 0]],
                            if mode == ApplyMode::CalFlagStrict {
                                input[[corr, 0]] / 2.
                            } else {
                                previous[[corr, 0]]
                            }
                        );
                        assert_eq!(
                            corrected[[corr, 1]],
                            if existing {
                                previous[[corr, 1]]
                            } else {
                                input[[corr, 1]]
                            }
                        );
                    }
                } else {
                    assert!(
                        !ms.main_table()
                            .schema()
                            .unwrap()
                            .contains_column("CORRECTED_DATA")
                    );
                }
                if spectral {
                    let ArrayValue::Float32(weights) = column("WEIGHT_SPECTRUM") else {
                        panic!("WEIGHT_SPECTRUM")
                    };
                    for corr in 0..2 {
                        assert_eq!(
                            weights[[corr, 0]],
                            if mode == ApplyMode::CalFlagStrict {
                                1.
                            } else {
                                99.
                            }
                        );
                        assert_eq!(weights[[corr, 1]], 99.);
                    }
                }
            }
        }
    }
}

#[test]
fn unavailable_windows_preserve_data_weights_and_only_strict_modes_flag() {
    // Calibrater::correct2 skips unavailable windows, or flags the selected
    // cube in STRICT mode. Neither case applies data or weight corrections.
    for bpoly in [false, true] {
        for mode in [
            ApplyMode::CalFlag,
            ApplyMode::CalOnly,
            ApplyMode::CalFlagStrict,
            ApplyMode::FlagOnly,
            ApplyMode::FlagOnlyStrict,
            ApplyMode::Trial,
        ] {
            for existing in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = common::create_apply_fixture_ms_with_options(dir.path(), existing, true);
                let mut ms = MeasurementSet::open(&path).unwrap();
                let ArrayValue::Complex32(data) = ms
                    .main_table()
                    .cell_accessor(1, "DATA")
                    .unwrap()
                    .array()
                    .unwrap()
                else {
                    panic!("DATA")
                };
                let input = data.clone();
                let previous = input.mapv(|value| value * Complex32::new(7., 2.));
                for (name, value) in [
                    ("WEIGHT", ArrayValue::from_f32_vec(vec![3.; 2])),
                    ("SIGMA", ArrayValue::from_f32_vec(vec![2.; 2])),
                    (
                        "WEIGHT_SPECTRUM",
                        ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[2, 2]), 99.)),
                    ),
                ] {
                    ms.main_table_mut()
                        .cell_accessor_mut(1, name)
                        .unwrap()
                        .set(Value::Array(value))
                        .unwrap();
                }
                if existing {
                    ms.main_table_mut()
                        .cell_accessor_mut(1, "CORRECTED_DATA")
                        .unwrap()
                        .set(Value::Array(ArrayValue::Complex32(previous.clone())))
                        .unwrap();
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
                let gain = if bpoly {
                    let rows = [0, 1].map(|antenna| common::SyntheticBPolySolutionRow {
                        time_seconds: 100.,
                        field_id: 0,
                        spectral_window_id: 0,
                        antenna_id: antenna,
                        scale_factor: Complex32::new(2., 0.),
                        valid_domain_hz: [1.0e9, 1.2e9],
                        amp_coefficients: vec![vec![0.]; 2],
                        phase_coefficients: vec![vec![0.]; 2],
                        phase_units: "rad",
                    });
                    common::create_apply_bpoly_caltable(&dir.path().join("gain.bpoly"), &rows)
                } else {
                    common::create_apply_gain_caltable(
                        &dir.path().join("gain.gcal"),
                        &["TARGET0"],
                        &rows,
                    )
                };
                let mut spec = ApplyCalibrationTableSpec::new(gain);
                spec.calwt = true;
                execute_apply_from_path(
                    &path,
                    &ApplyPlanRequest {
                        selection: MsSelection::new().spw_selector("1:0").unwrap(),
                        apply_mode: mode,
                        parang: false,
                        calibration_tables: vec![spec],
                    },
                )
                .unwrap();
                let ms = MeasurementSet::open(&path).unwrap();
                let column = |row, name| {
                    ms.main_table()
                        .cell_accessor(row, name)
                        .unwrap()
                        .array()
                        .unwrap()
                };
                assert_eq!(column(1, "DATA"), &ArrayValue::Complex32(input.clone()));
                assert_eq!(column(1, "WEIGHT"), &ArrayValue::from_f32_vec(vec![3.; 2]));
                assert_eq!(column(1, "SIGMA"), &ArrayValue::from_f32_vec(vec![2.; 2]));
                assert_eq!(
                    column(1, "WEIGHT_SPECTRUM"),
                    &ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[2, 2]), 99.))
                );
                let has_corrected = ms
                    .main_table()
                    .schema()
                    .unwrap()
                    .contains_column("CORRECTED_DATA");
                let creates = matches!(
                    mode,
                    ApplyMode::CalFlag | ApplyMode::CalOnly | ApplyMode::CalFlagStrict
                );
                assert_eq!(has_corrected, existing || creates);
                if has_corrected {
                    assert_eq!(
                        column(1, "CORRECTED_DATA"),
                        &ArrayValue::Complex32(if existing { previous } else { input })
                    );
                    if !existing {
                        assert_eq!(column(0, "CORRECTED_DATA"), column(0, "DATA"));
                    }
                }
                let ArrayValue::Bool(flags) = column(1, "FLAG") else {
                    panic!("FLAG")
                };
                for corr in 0..2 {
                    assert_eq!(
                        flags[[corr, 0]],
                        matches!(mode, ApplyMode::CalFlagStrict | ApplyMode::FlagOnlyStrict)
                    );
                    assert!(!flags[[corr, 1]]);
                }
                let ArrayValue::Bool(untouched) = column(0, "FLAG") else {
                    panic!("FLAG")
                };
                assert!(untouched.iter().all(|flag| !flag));
            }
        }
    }
}
