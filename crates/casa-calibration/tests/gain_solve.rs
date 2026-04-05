// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, GainSolveCombine, GainSolveInterval,
    GainSolveMode, GainSolveRequest, GainType, RefAntSelector, execute_apply_from_path,
    solve_gain_from_path, summarize_table,
};
use casacore_ms::ms::MeasurementSet;
use casacore_ms::selection::MsSelection;

#[test]
fn solve_gain_phase_g_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("solved.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic G gains");

    assert_eq!(report.solution_row_count, 3);
    let summary = summarize_table(&caltable_path).expect("summarize solved table");
    assert_eq!(summary.table_subtype, "G Jones");
    assert!(summary.supported_for_v1_apply());

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply solved G table");

    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_phase_t_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::T);
    let caltable_path = dir.path().join("solved.tcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::T,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic T gains");

    assert_eq!(report.solution_row_count, 3);
    let summary = summarize_table(&caltable_path).expect("summarize solved table");
    assert_eq!(summary.table_subtype, "T Jones");
    assert!(summary.supported_for_v1_apply());

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply solved T table");

    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_amplitude_phase_g_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(
        dir.path(),
        common::SyntheticGainFixtureKind::GAmplitudePhase,
    );
    let caltable_path = dir.path().join("solved-ap.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::AmplitudePhase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic amplitude+phase G gains");

    assert_eq!(report.solution_row_count, 3);
    let summary = summarize_table(&caltable_path).expect("summarize solved table");
    assert_eq!(summary.table_subtype, "G Jones");
    assert!(summary.supported_for_v1_apply());

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply solved G table");

    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_amplitude_phase_t_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(
        dir.path(),
        common::SyntheticGainFixtureKind::TAmplitudePhase,
    );
    let caltable_path = dir.path().join("solved-ap.tcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::T,
            solve_mode: GainSolveMode::AmplitudePhase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic amplitude+phase T gains");

    assert_eq!(report.solution_row_count, 3);
    let summary = summarize_table(&caltable_path).expect("summarize solved table");
    assert_eq!(summary.table_subtype, "T Jones");
    assert!(summary.supported_for_v1_apply());

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply solved T table");

    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_phase_g_solint_integration_writes_per_integration_solutions() {
    let dir = TempDir::new().expect("tempdir");
    let gains_a = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.9553365, 0.29552022),
            casacore_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casacore_types::Complex32::new(0.9800666, -0.19866933),
            casacore_types::Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let gains_b = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.6967067, 0.7173561),
            casacore_types::Complex32::new(0.7648422, -0.64421767),
        ],
        [
            casacore_types::Complex32::new(0.5403023, -0.84147096),
            casacore_types::Complex32::new(0.8253356, 0.5646425),
        ],
    ];
    let ms_path = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[
            common::SyntheticGainTimeCluster {
                time_seconds: 100.0,
                scan_number: 1,
                gains: gains_a,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 110.0,
                scan_number: 1,
                gains: gains_a,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 200.0,
                scan_number: 1,
                gains: gains_b,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 210.0,
                scan_number: 1,
                gains: gains_b,
            },
        ],
    );
    let caltable_path = dir.path().join("solved-int.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Integration,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve integration-bucket gains");

    assert_eq!(report.solution_row_count, 12);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply solved integration-bucket table");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_phase_g_solint_seconds_groups_nearby_integrations() {
    let dir = TempDir::new().expect("tempdir");
    let gains_a = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.9553365, 0.29552022),
            casacore_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casacore_types::Complex32::new(0.9800666, -0.19866933),
            casacore_types::Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let gains_b = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.6967067, 0.7173561),
            casacore_types::Complex32::new(0.7648422, -0.64421767),
        ],
        [
            casacore_types::Complex32::new(0.5403023, -0.84147096),
            casacore_types::Complex32::new(0.8253356, 0.5646425),
        ],
    ];
    let ms_path = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[
            common::SyntheticGainTimeCluster {
                time_seconds: 100.0,
                scan_number: 1,
                gains: gains_a,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 110.0,
                scan_number: 1,
                gains: gains_a,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 200.0,
                scan_number: 1,
                gains: gains_b,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 210.0,
                scan_number: 1,
                gains: gains_b,
            },
        ],
    );
    let caltable_path = dir.path().join("solved-30s.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Seconds(30.0),
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve 30s-bucket gains");

    assert_eq!(report.solution_row_count, 6);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply solved 30s-bucket table");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_phase_g_combine_scans_writes_one_solution_group_across_scans() {
    let dir = TempDir::new().expect("tempdir");
    let gains = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.9553365, 0.29552022),
            casacore_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casacore_types::Complex32::new(0.9800666, -0.19866933),
            casacore_types::Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let ms_path = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[
            common::SyntheticGainTimeCluster {
                time_seconds: 100.0,
                scan_number: 1,
                gains,
            },
            common::SyntheticGainTimeCluster {
                time_seconds: 200.0,
                scan_number: 2,
                gains,
            },
        ],
    );
    let caltable_path = dir.path().join("solved-combine-scan.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: caltable_path.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine {
                scans: true,
                fields: false,
            },
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve combine-scan gains");

    assert_eq!(report.solution_row_count, 3);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply combined-scan solved table");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_phase_g_combine_scan_and_field_writes_one_solution_group_across_fields() {
    let dir = TempDir::new().expect("tempdir");
    let gains = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.9553365, 0.29552022),
            casacore_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casacore_types::Complex32::new(0.9800666, -0.19866933),
            casacore_types::Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let ms_path = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[common::SyntheticGainTimeCluster {
            time_seconds: 100.0,
            scan_number: 1,
            gains,
        }],
    );
    let mut ms = MeasurementSet::open(&ms_path).expect("reopen synthetic MS");
    common::append_gain_solve_cluster_for_field(
        &mut ms,
        1,
        &common::SyntheticGainTimeCluster {
            time_seconds: 200.0,
            scan_number: 2,
            gains,
        },
    );
    ms.save().expect("save multi-field synthetic MS");
    let caltable_path = dir.path().join("solved-combine-scan-field.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0, 1]),
            output_table: caltable_path.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine {
                scans: true,
                fields: true,
            },
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve combined scan+field gains");

    assert_eq!(report.solution_row_count, 3);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0, 1]),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply combined scan+field solved table");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}

#[test]
fn solve_gain_phase_g_with_prior_caltable_corrects_residual_downstream() {
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
    let residual_gains = [
        [
            casacore_types::Complex32::new(1.0, 0.0),
            casacore_types::Complex32::new(1.0, 0.0),
        ],
        [
            casacore_types::Complex32::new(0.99875027, 0.04997917),
            casacore_types::Complex32::new(0.9800666, -0.19866933),
        ],
        [
            casacore_types::Complex32::new(0.9921977, -0.12467473),
            casacore_types::Complex32::new(0.9553365, 0.29552022),
        ],
    ];
    let total_gains = [
        [
            prior_gains[0][0] * residual_gains[0][0],
            prior_gains[0][1] * residual_gains[0][1],
        ],
        [
            prior_gains[1][0] * residual_gains[1][0],
            prior_gains[1][1] * residual_gains[1][1],
        ],
        [
            prior_gains[2][0] * residual_gains[2][0],
            prior_gains[2][1] * residual_gains[2][1],
        ],
    ];
    let ms_path = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[common::SyntheticGainTimeCluster {
            time_seconds: 100.0,
            scan_number: 1,
            gains: total_gains,
        }],
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
                gains: prior_gains[0].to_vec(),
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: prior_gains[1].to_vec(),
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 2,
                gains: prior_gains[2].to_vec(),
                flags: vec![false, false],
            },
        ],
    );
    let residual_table = dir.path().join("residual.gcal");

    let report = solve_gain_from_path(
        &ms_path,
        &GainSolveRequest {
            selection: MsSelection::new(),
            output_table: residual_table.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_table)],
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve residual gains after prior caltable");

    assert_eq!(report.solution_row_count, 3);
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec::new(&prior_table),
                ApplyCalibrationTableSpec::new(&residual_table),
            ],
        },
    )
    .expect("apply prior and residual solved tables");
    common::assert_corrected_rows_are_unit_model(&ms_path);
}
