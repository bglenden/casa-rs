// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::main_table::VisibilityDataColumn;
use casacore_ms::selection::MsSelection;
use casacore_tables::{Table, TableOptions};
use casacore_types::{ArrayValue, Complex32};
use ndarray::Ix2;
use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, BandpassSolveCombine,
    BandpassSolveRequest, BandpassType, RefAntSelector, execute_apply_from_path,
    solve_bandpass_from_path, summarize_table,
};

#[test]
fn solve_bandpass_with_prior_gain_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let prior_gains = [
        [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        [
            Complex32::new(0.9553365, 0.29552022),
            Complex32::new(0.921061, -0.38941833),
        ],
        [
            Complex32::new(0.9800666, -0.19866933),
            Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let bandpass_gains = [
        [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        ],
        [
            [Complex32::new(1.1, 0.15), Complex32::new(0.8, -0.2)],
            [Complex32::new(0.95, -0.1), Complex32::new(1.2, 0.25)],
        ],
        [
            [Complex32::new(0.9, -0.05), Complex32::new(1.15, 0.18)],
            [Complex32::new(1.05, 0.12), Complex32::new(0.88, -0.14)],
        ],
    ];
    let ms_path = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[
            common::SyntheticBandpassTimeCluster {
                field_id: 0,
                time_seconds: 100.0,
                scan_number: 1,
                prior_gains,
                bandpass_gains,
            },
            common::SyntheticBandpassTimeCluster {
                field_id: 0,
                time_seconds: 101.0,
                scan_number: 1,
                prior_gains,
                bandpass_gains,
            },
        ],
    );

    let prior_table = common::create_apply_gain_caltable(
        &dir.path().join("prior.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![prior_gains[0][0], prior_gains[0][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![prior_gains[1][0], prior_gains[1][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: vec![prior_gains[2][0], prior_gains[2][1]],
                flags: vec![false, false],
            },
        ],
    );

    let bandpass_table = dir.path().join("bandpass.bcal");
    let report = solve_bandpass_from_path(
        &ms_path,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: bandpass_table.clone(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_table)],
            parang: false,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve bandpass");

    assert_eq!(report.solution_row_count, 3);
    assert_eq!(report.channel_count, 2);
    assert_eq!(report.table_subtype, "B Jones");

    let summary = summarize_table(&bandpass_table).expect("summarize bandpass");
    assert_eq!(summary.table_subtype, "B Jones");
    assert_eq!(summary.row_count, 3);

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec::new(&prior_table),
                ApplyCalibrationTableSpec::new(&bandpass_table),
            ],
        },
    )
    .expect("apply prior + bandpass");

    let ms = MeasurementSet::open(&ms_path).expect("reopen solved ms");
    for row in 0..ms.main_table().row_count() {
        let corrected = ms
            .main_table()
            .get_array_cell(row, VisibilityDataColumn::CorrectedData.name())
            .expect("corrected data");
        let ArrayValue::Complex32(values) = corrected else {
            panic!("corrected data row {row} was not Complex32");
        };
        let values = values
            .view()
            .into_dimensionality::<Ix2>()
            .expect("2-D corrected data");
        for sample in values {
            assert!(
                (*sample - Complex32::new(1.0, 0.0)).norm() <= 1.0e-3,
                "expected unity corrected sample, found ({:.6},{:.6})",
                sample.re,
                sample.im
            );
        }
    }
}

#[test]
fn solve_bandpass_with_combine_scan_writes_one_solution_group_across_scans() {
    let dir = TempDir::new().expect("tempdir");
    let prior_gains = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.9887711, 0.14943813),
            casacore_types::Complex32::new(0.9689124, -0.24740396),
        ],
        [
            casacore_types::Complex32::new(0.9950042, -0.09983342),
            casacore_types::Complex32::new(0.9393727, 0.3428978),
        ],
    ];
    let bandpass_gains = [
        [
            [
                casacore_types::Complex32::new(1.0, 0.0),
                casacore_types::Complex32::new(1.0, 0.0),
            ],
            [
                casacore_types::Complex32::new(1.0, 0.0),
                casacore_types::Complex32::new(1.0, 0.0),
            ],
        ],
        [
            [
                casacore_types::Complex32::new(1.05, 0.10),
                casacore_types::Complex32::new(0.95, -0.05),
            ],
            [
                casacore_types::Complex32::new(0.90, 0.02),
                casacore_types::Complex32::new(1.08, 0.03),
            ],
        ],
        [
            [
                casacore_types::Complex32::new(0.92, -0.08),
                casacore_types::Complex32::new(1.03, 0.04),
            ],
            [
                casacore_types::Complex32::new(1.10, -0.03),
                casacore_types::Complex32::new(0.97, 0.06),
            ],
        ],
    ];
    let ms_path = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[
            common::SyntheticBandpassTimeCluster {
                field_id: 0,
                time_seconds: 100.0,
                scan_number: 1,
                prior_gains,
                bandpass_gains,
            },
            common::SyntheticBandpassTimeCluster {
                field_id: 0,
                time_seconds: 200.0,
                scan_number: 2,
                prior_gains,
                bandpass_gains,
            },
        ],
    );
    let prior_table = common::create_apply_gain_caltable(
        dir.path(),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 150.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![prior_gains[0][0], prior_gains[0][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 150.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![prior_gains[1][0], prior_gains[1][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 150.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: vec![prior_gains[2][0], prior_gains[2][1]],
                flags: vec![false, false],
            },
        ],
    );

    let bandpass_table = dir.path().join("bandpass-combine-scan.bcal");
    let report = solve_bandpass_from_path(
        &ms_path,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: bandpass_table.clone(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_table)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: true,
                fields: false,
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve combined-scan bandpass");

    assert_eq!(report.solution_row_count, 3);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec::new(&prior_table),
                ApplyCalibrationTableSpec::new(&bandpass_table),
            ],
        },
    )
    .expect("apply combined-scan bandpass");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_bandpass_with_combine_field_writes_one_solution_group_across_fields() {
    let dir = TempDir::new().expect("tempdir");
    let prior_gains = [
        [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        [
            Complex32::new(0.9887711, 0.14943813),
            Complex32::new(0.9689124, -0.24740396),
        ],
        [
            Complex32::new(0.9950042, -0.09983342),
            Complex32::new(0.9393727, 0.3428978),
        ],
    ];
    let bandpass_gains = [
        [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        ],
        [
            [Complex32::new(1.05, 0.10), Complex32::new(0.95, -0.05)],
            [Complex32::new(0.90, 0.02), Complex32::new(1.08, 0.03)],
        ],
        [
            [Complex32::new(0.92, -0.08), Complex32::new(1.03, 0.04)],
            [Complex32::new(1.10, -0.03), Complex32::new(0.97, 0.06)],
        ],
    ];
    let ms_path = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[
            common::SyntheticBandpassTimeCluster {
                field_id: 0,
                time_seconds: 100.0,
                scan_number: 1,
                prior_gains,
                bandpass_gains,
            },
            common::SyntheticBandpassTimeCluster {
                field_id: 1,
                time_seconds: 101.0,
                scan_number: 1,
                prior_gains,
                bandpass_gains,
            },
        ],
    );
    let prior_table = common::create_apply_gain_caltable(
        dir.path(),
        &["TARGET0", "TARGET1"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.5,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![prior_gains[0][0], prior_gains[0][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.5,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![prior_gains[1][0], prior_gains[1][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.5,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: vec![prior_gains[2][0], prior_gains[2][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.5,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![prior_gains[0][0], prior_gains[0][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.5,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![prior_gains[1][0], prior_gains[1][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.5,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: vec![prior_gains[2][0], prior_gains[2][1]],
                flags: vec![false, false],
            },
        ],
    );

    let bandpass_table = dir.path().join("bandpass-combine-field.bcal");
    let report = solve_bandpass_from_path(
        &ms_path,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: bandpass_table.clone(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_table)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: false,
                fields: true,
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve combined-field bandpass");

    assert_eq!(report.solution_row_count, 6);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec::new(&prior_table),
                ApplyCalibrationTableSpec::new(&bandpass_table),
            ],
        },
    )
    .expect("apply combined-field bandpass");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_bandpass_with_solnorm_normalizes_per_receptor_average_amplitude() {
    let dir = TempDir::new().expect("tempdir");
    let prior_gains = [
        [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        [
            Complex32::new(0.9553365, 0.29552022),
            Complex32::new(0.921061, -0.38941833),
        ],
        [
            Complex32::new(0.9800666, -0.19866933),
            Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let bandpass_gains = [
        [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        ],
        [
            [Complex32::new(2.2, 0.3), Complex32::new(1.6, -0.4)],
            [Complex32::new(1.9, -0.2), Complex32::new(2.4, 0.5)],
        ],
        [
            [Complex32::new(1.8, -0.1), Complex32::new(2.3, 0.36)],
            [Complex32::new(2.1, 0.24), Complex32::new(1.76, -0.28)],
        ],
    ];
    let ms_path = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[common::SyntheticBandpassTimeCluster {
            field_id: 0,
            time_seconds: 100.0,
            scan_number: 1,
            prior_gains,
            bandpass_gains,
        }],
    );
    let prior_table = common::create_apply_gain_caltable(
        &dir.path().join("prior-solnorm.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![prior_gains[0][0], prior_gains[0][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![prior_gains[1][0], prior_gains[1][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: vec![prior_gains[2][0], prior_gains[2][1]],
                flags: vec![false, false],
            },
        ],
    );

    let bandpass_table = dir.path().join("bandpass-solnorm.bcal");
    solve_bandpass_from_path(
        &ms_path,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: bandpass_table.clone(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_table)],
            parang: false,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::B,
            normalize_average_amplitude: true,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve normalized bandpass");

    let table = Table::open(TableOptions::new(&bandpass_table)).expect("open bandpass table");
    for row in 0..table.row_count() {
        let gains = match table.get_array_cell(row, "CPARAM").expect("CPARAM cell") {
            ArrayValue::Complex32(values) => values
                .view()
                .into_dimensionality::<Ix2>()
                .expect("2-D bandpass gains")
                .to_owned(),
            other => panic!("unexpected CPARAM type: {other:?}"),
        };
        for receptor in 0..gains.shape()[0] {
            let rms_amplitude = (gains
                .row(receptor)
                .iter()
                .map(|gain| {
                    let amplitude = gain.norm();
                    amplitude * amplitude
                })
                .sum::<f32>()
                / gains.shape()[1] as f32)
                .sqrt();
            assert!(
                (rms_amplitude - 1.0).abs() <= 1.0e-4,
                "expected unit RMS amplitude, found {rms_amplitude}"
            );
        }
    }
}

#[test]
fn solve_bpoly_bandpass_with_prior_gain_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let prior_gains = [
        [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        [
            Complex32::new(0.9553365, 0.29552022),
            Complex32::new(0.921061, -0.38941833),
        ],
        [
            Complex32::new(0.9800666, -0.19866933),
            Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let bandpass_gains = [
        [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        ],
        [
            [Complex32::new(1.1, 0.15), Complex32::new(0.8, -0.2)],
            [Complex32::new(0.95, -0.1), Complex32::new(1.2, 0.25)],
        ],
        [
            [Complex32::new(0.9, -0.05), Complex32::new(1.15, 0.18)],
            [Complex32::new(1.05, 0.12), Complex32::new(0.88, -0.14)],
        ],
    ];
    let ms_path = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[common::SyntheticBandpassTimeCluster {
            field_id: 0,
            time_seconds: 100.0,
            scan_number: 1,
            prior_gains,
            bandpass_gains,
        }],
    );

    let prior_table = common::create_apply_gain_caltable(
        &dir.path().join("prior-bpoly.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![prior_gains[0][0], prior_gains[0][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![prior_gains[1][0], prior_gains[1][1]],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: vec![prior_gains[2][0], prior_gains[2][1]],
                flags: vec![false, false],
            },
        ],
    );

    let bandpass_table = dir.path().join("bandpass.bpoly");
    let report = solve_bandpass_from_path(
        &ms_path,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: bandpass_table.clone(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_table)],
            parang: false,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::BPoly,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve BPOLY bandpass");

    assert_eq!(report.table_subtype, "BPOLY");
    let summary = summarize_table(&bandpass_table).expect("summarize BPOLY table");
    assert_eq!(summary.table_subtype, "BPOLY");

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec::new(&prior_table),
                ApplyCalibrationTableSpec::new(&bandpass_table),
            ],
        },
    )
    .expect("apply prior + BPOLY");

    let ms = MeasurementSet::open(&ms_path).expect("reopen solved ms");
    for row in 0..ms.main_table().row_count() {
        let corrected = ms
            .main_table()
            .get_array_cell(row, VisibilityDataColumn::CorrectedData.name())
            .expect("corrected data");
        let ArrayValue::Complex32(values) = corrected else {
            panic!("corrected data row {row} was not Complex32");
        };
        let values = values
            .view()
            .into_dimensionality::<Ix2>()
            .expect("2-D corrected data");
        for sample in values {
            assert!(
                (*sample - Complex32::new(1.0, 0.0)).norm() <= 1.0e-3,
                "expected unity corrected sample, found ({:.6},{:.6})",
                sample.re,
                sample.im
            );
        }
    }
}
