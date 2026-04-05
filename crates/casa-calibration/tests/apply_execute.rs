// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, ApplyTableSelection, GainFieldSelector,
    execute_apply_from_path,
};
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::main_table::VisibilityDataColumn;
use casacore_ms::selection::MsSelection;
use casacore_types::ArrayValue;

#[test]
fn execute_apply_trial_does_not_mutate_measurement_set() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(5.0, 0.0),
                    casacore_types::Complex32::new(10.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    let report = execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("trial apply");

    assert!(!report.wrote_measurement_set);
    assert_eq!(report.updated_row_count, 0);

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    assert!(ms.data_column(VisibilityDataColumn::CorrectedData).is_err());
}

#[test]
fn execute_apply_creates_corrected_data_and_writes_corrected_visibilities() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(5.0, 0.0),
                    casacore_types::Complex32::new(10.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    let report = execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply calibration");

    assert!(report.created_corrected_data_column);
    assert!(report.wrote_measurement_set);
    assert_eq!(report.updated_row_count, 1);

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let corrected_column = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data accessor");
    let corrected = corrected_column.get(0).expect("read corrected row");
    let ArrayValue::Complex32(corrected) = corrected else {
        panic!("expected Complex32 corrected data");
    };

    assert_eq!(corrected[[0, 0]], casacore_types::Complex32::new(0.1, 0.0));
    assert_eq!(
        corrected[[1, 0]],
        casacore_types::Complex32::new(0.0, 0.025)
    );
    assert_eq!(corrected[[0, 1]], casacore_types::Complex32::new(0.2, 0.0));
    assert_eq!(corrected[[1, 1]], casacore_types::Complex32::new(0.0, 0.05));
}

#[test]
fn execute_apply_k_jones_delay_table_corrects_frequency_dependent_phase() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    let caltable_path = common::create_apply_delay_caltable(
        &dir.path().join("delay.kcal"),
        &["TARGET0"],
        &[
            common::SyntheticDelaySolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                delays_ns: vec![250.0, 250.0],
                flags: vec![false, false],
            },
            common::SyntheticDelaySolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                delays_ns: vec![0.0, 0.0],
                flags: vec![false, false],
            },
        ],
    );

    let report = execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply K Jones delay calibration");

    assert!(report.created_corrected_data_column);
    assert_eq!(report.updated_row_count, 1);

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let corrected_column = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data accessor");
    let corrected = corrected_column.get(0).expect("read corrected row");
    let ArrayValue::Complex32(corrected) = corrected else {
        panic!("expected Complex32 corrected data");
    };

    assert_eq!(corrected[[0, 0]], casacore_types::Complex32::new(1.0, 0.0));
    assert_eq!(corrected[[1, 0]], casacore_types::Complex32::new(0.0, 1.0));
    assert!((corrected[[0, 1]].re - 0.0).abs() < 1.0e-6);
    assert!((corrected[[0, 1]].im + 2.0).abs() < 1.0e-6);
    assert!((corrected[[1, 1]].re - 2.0).abs() < 1.0e-6);
    assert!((corrected[[1, 1]].im - 0.0).abs() < 1.0e-6);
}

#[test]
fn execute_apply_bpoly_table_expands_channelized_bandpass() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    let amp_scale = (8.0_f32).sqrt();
    let amp_slope = 2.0_f64.ln() / 2.0;
    let bpoly_path = common::create_apply_bpoly_caltable(
        &dir.path().join("bandpass.bpoly"),
        &[
            common::SyntheticBPolySolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                scale_factor: casacore_types::Complex32::new(amp_scale, 0.0),
                valid_domain_hz: [1.0e9, 1.001e9],
                amp_coefficients: vec![vec![0.0, amp_slope], vec![0.0, amp_slope]],
                phase_coefficients: vec![vec![0.0], vec![0.0]],
                phase_units: "RADIANS",
            },
            common::SyntheticBPolySolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                scale_factor: casacore_types::Complex32::new(1.0, 0.0),
                valid_domain_hz: [1.0e9, 1.001e9],
                amp_coefficients: vec![vec![0.0, 0.0], vec![0.0, 0.0]],
                phase_coefficients: vec![vec![0.0], vec![0.0]],
                phase_units: "RADIANS",
            },
        ],
    );

    let report = execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&bpoly_path)],
        },
    )
    .expect("apply BPOLY bandpass calibration");

    assert!(report.created_corrected_data_column);
    assert_eq!(report.updated_row_count, 1);

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let corrected_column = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data accessor");
    let corrected = corrected_column.get(0).expect("read corrected row");
    let ArrayValue::Complex32(corrected) = corrected else {
        panic!("expected Complex32 corrected data");
    };

    assert!((corrected[[0, 0]].re - 0.5).abs() < 1.0e-6);
    assert!((corrected[[0, 1]].re - 0.5).abs() < 1.0e-6);
    assert!((corrected[[1, 0]].im - 0.5).abs() < 1.0e-6);
    assert!((corrected[[1, 1]].im - 0.5).abs() < 1.0e-6);
}

#[test]
fn execute_apply_uses_nearest_gainfield_mapping() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    common::set_ms_field_directions(&ms_path, &[(0, 1.19, 0.5), (1, 1.20, 0.6)]);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.path().join("nearest.gcal"),
        &["CAL0", "CAL1"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(1.0, 0.0),
                    casacore_types::Complex32::new(1.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(1.0, 0.0),
                    casacore_types::Complex32::new(1.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(5.0, 0.0),
                    casacore_types::Complex32::new(10.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );
    common::set_caltable_field_directions(&caltable_path, &[(0, 1.0, 0.5), (1, 1.2, 0.5)]);

    let report = execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                path: caltable_path.clone(),
                apply_to: Default::default(),
                gainfield: Some(GainFieldSelector::Nearest),
                spwmap: Vec::new(),
                interp: casa_calibration::ApplyInterpolationMode::Nearest,
                calwt: false,
            }],
        },
    )
    .expect("apply nearest gainfield calibration");

    assert_eq!(report.updated_row_count, 1);

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let corrected_column = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data accessor");
    let corrected = corrected_column.get(0).expect("read corrected row");
    let ArrayValue::Complex32(corrected) = corrected else {
        panic!("expected Complex32 corrected data");
    };

    assert_eq!(corrected[[0, 0]], casacore_types::Complex32::new(0.1, 0.0));
    assert_eq!(
        corrected[[1, 0]],
        casacore_types::Complex32::new(0.0, 0.025)
    );
    assert_eq!(corrected[[0, 1]], casacore_types::Complex32::new(0.2, 0.0));
    assert_eq!(corrected[[1, 1]], casacore_types::Complex32::new(0.0, 0.05));
}

#[test]
fn execute_apply_calflag_marks_samples_when_solution_is_missing() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 100.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![
                casacore_types::Complex32::new(2.0, 0.0),
                casacore_types::Complex32::new(4.0, 0.0),
            ],
            flags: vec![false, false],
        }],
    );

    let report = execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply calibration with calflag");

    assert_eq!(report.flagged_sample_count, 4);
    assert_eq!(report.flagged_row_count, 1);

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let flag_column = ms.flag_column();
    let flags = flag_column.get(0).expect("read flags");
    let ArrayValue::Bool(flags) = flags else {
        panic!("expected bool flags");
    };
    assert!(flags.iter().all(|flag| *flag));
    assert!(ms.flag_row_column().get(0).expect("flag row"));
}

#[test]
fn execute_apply_calwt_updates_weight_column_for_gain_tables() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(5.0, 0.0),
                    casacore_types::Complex32::new(10.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    let mut spec = ApplyCalibrationTableSpec::new(&caltable_path);
    spec.calwt = true;
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![spec],
        },
    )
    .expect("apply calibration with calwt");

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let weight = ms
        .main_table()
        .get_array_cell(0, "WEIGHT")
        .expect("read weight row");
    let ArrayValue::Float32(weight) = weight else {
        panic!("expected float32 WEIGHT");
    };

    assert_eq!(weight[[0]], 100.0);
    assert_eq!(weight[[1]], 1600.0);
}

#[test]
fn execute_apply_calwt_updates_weight_spectrum_and_reduces_weight() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms_with_options(dir.path(), true, true);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(5.0, 0.0),
                    casacore_types::Complex32::new(10.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    let mut spec = ApplyCalibrationTableSpec::new(&caltable_path);
    spec.calwt = true;
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![spec],
        },
    )
    .expect("apply calibration with channelized calwt");

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");

    let weight_spectrum = ms
        .main_table()
        .get_array_cell(0, "WEIGHT_SPECTRUM")
        .expect("read weight spectrum row");
    let ArrayValue::Float32(weight_spectrum) = weight_spectrum else {
        panic!("expected float32 WEIGHT_SPECTRUM");
    };
    assert_eq!(weight_spectrum[[0, 0]], 700.0);
    assert_eq!(weight_spectrum[[0, 1]], 700.0);
    assert_eq!(weight_spectrum[[1, 0]], 17_600.0);
    assert_eq!(weight_spectrum[[1, 1]], 17_600.0);

    let weight = ms
        .main_table()
        .get_array_cell(0, "WEIGHT")
        .expect("read weight row");
    let ArrayValue::Float32(weight) = weight else {
        panic!("expected float32 WEIGHT");
    };

    assert_eq!(weight[[0]], 700.0);
    assert_eq!(weight[[1]], 17_600.0);
}

#[test]
fn execute_apply_respects_per_table_applicability_selection() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    let field0_table = common::create_apply_gain_caltable(
        &dir.path().join("field0.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(1.0, 0.0),
                    casacore_types::Complex32::new(1.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );
    let field1_table = common::create_apply_gain_caltable(
        &dir.path().join("field1.gcal"),
        &["TARGET1"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 200.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 0,
                gains: vec![
                    casacore_types::Complex32::new(2.0, 0.0),
                    casacore_types::Complex32::new(2.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 200.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 1,
                gains: vec![
                    casacore_types::Complex32::new(10.0, 0.0),
                    casacore_types::Complex32::new(20.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec {
                    path: field0_table,
                    apply_to: ApplyTableSelection {
                        field_ids: vec![0],
                        spectral_window_ids: Vec::new(),
                        observation_ids: Vec::new(),
                    },
                    gainfield: None,
                    spwmap: Vec::new(),
                    interp: casa_calibration::ApplyInterpolationMode::Nearest,
                    calwt: false,
                },
                ApplyCalibrationTableSpec {
                    path: field1_table,
                    apply_to: ApplyTableSelection {
                        field_ids: vec![1],
                        spectral_window_ids: Vec::new(),
                        observation_ids: Vec::new(),
                    },
                    gainfield: None,
                    spwmap: Vec::new(),
                    interp: casa_calibration::ApplyInterpolationMode::Nearest,
                    calwt: false,
                },
            ],
        },
    )
    .expect("apply per-table-selected calibration chain");

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let corrected_column = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data accessor");
    let row0 = corrected_column.get(0).expect("corrected row 0");
    let row1 = corrected_column.get(1).expect("corrected row 1");
    let ArrayValue::Complex32(row0) = row0 else {
        panic!("expected complex corrected row 0");
    };
    let ArrayValue::Complex32(row1) = row1 else {
        panic!("expected complex corrected row 1");
    };

    assert_eq!(row0[[0, 0]], casacore_types::Complex32::new(0.5, 0.0));
    assert_eq!(row1[[0, 0]], casacore_types::Complex32::new(0.05, 0.0));
}
