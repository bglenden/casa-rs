// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, GainSolveCombine, GainSolveInterval,
    GainSolveMode, GainSolveModelSource, GainSolveRequest, GainType, RefAntSelector,
    execute_apply_from_path, solve_gain_from_path, summarize_table,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableOptions};
use casa_types::ArrayValue;

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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
fn solve_gain_phase_g_uses_model_data_column_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_model_column_fixture_ms(
        dir.path(),
        [
            casa_types::Complex32::new(2.0, 0.0),
            casa_types::Complex32::new(3.0, 0.0),
            casa_types::Complex32::new(4.0, 0.0),
            casa_types::Complex32::new(5.0, 0.0),
        ],
    );
    let caltable_path = dir.path().join("solved-model.gcal");

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
            model_source: GainSolveModelSource::ModelColumn,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic G gains against MODEL_DATA");

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
    .expect("apply model-column solved G table");

    common::assert_corrected_rows_match_model_column(&ms_path);
}

#[test]
fn solve_gain_min_snr_flags_low_snr_solutions_and_writes_diagnostics() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("solved-minsnr.gcal");

    solve_gain_from_path(
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 1.0e9,
            min_baselines_per_antenna: 0,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic G gains with strict SNR threshold");

    let table = Table::open(TableOptions::new(&caltable_path)).expect("open gain table");
    let mut saw_finite_snr = false;
    let mut saw_positive_error = false;
    for row in 0..table.row_count() {
        let flags = match table
            .cell_accessor(row, "FLAG")
            .and_then(|cell| cell.array())
            .expect("FLAG cell")
        {
            ArrayValue::Bool(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected FLAG value: {other:?}"),
        };
        assert!(flags.iter().all(|flag| *flag));

        let snrs = match table
            .cell_accessor(row, "SNR")
            .and_then(|cell| cell.array())
            .expect("SNR cell")
        {
            ArrayValue::Float32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected SNR value: {other:?}"),
        };
        saw_finite_snr |= snrs.iter().any(|snr| snr.is_finite() && *snr > 0.0);

        let param_errors = match table
            .cell_accessor(row, "PARAMERR")
            .and_then(|cell| cell.array())
            .expect("PARAMERR cell")
        {
            ArrayValue::Float32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected PARAMERR value: {other:?}"),
        };
        saw_positive_error |= param_errors
            .iter()
            .any(|error| error.is_finite() && *error > 0.0);
    }
    assert!(saw_finite_snr);
    assert!(saw_positive_error);
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
fn solve_gain_amplitude_phase_t_with_solnorm_normalizes_average_amplitude() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(
        dir.path(),
        common::SyntheticGainFixtureKind::TAmplitudePhase,
    );
    let caltable_path = dir.path().join("solved-ap-solnorm.tcal");

    solve_gain_from_path(
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: true,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve normalized amplitude+phase T gains");

    let table = Table::open(TableOptions::new(&caltable_path)).expect("open gain table");
    let mut power_sum = 0.0_f32;
    let mut count = 0usize;
    for row in 0..table.row_count() {
        let gains = match table
            .cell_accessor(row, "CPARAM")
            .and_then(|cell| cell.array())
            .expect("CPARAM cell")
        {
            ArrayValue::Complex32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected CPARAM value: {other:?}"),
        };
        for gain in gains {
            let amplitude = gain.norm();
            power_sum += amplitude * amplitude;
            count += 1;
        }
    }

    let rms_amplitude = (power_sum / count as f32).sqrt();
    assert!(
        (rms_amplitude - 1.0).abs() <= 1.0e-4,
        "expected unit RMS amplitude, found {rms_amplitude}"
    );
}

#[test]
fn solve_gain_phase_g_solint_integration_writes_per_integration_solutions() {
    let dir = TempDir::new().expect("tempdir");
    let gains_a = [
        [
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.9553365, 0.29552022),
            casa_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casa_types::Complex32::new(0.9800666, -0.19866933),
            casa_types::Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let gains_b = [
        [
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.6967067, 0.7173561),
            casa_types::Complex32::new(0.7648422, -0.64421767),
        ],
        [
            casa_types::Complex32::new(0.5403023, -0.84147096),
            casa_types::Complex32::new(0.8253356, 0.5646425),
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.9553365, 0.29552022),
            casa_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casa_types::Complex32::new(0.9800666, -0.19866933),
            casa_types::Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let gains_b = [
        [
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.6967067, 0.7173561),
            casa_types::Complex32::new(0.7648422, -0.64421767),
        ],
        [
            casa_types::Complex32::new(0.5403023, -0.84147096),
            casa_types::Complex32::new(0.8253356, 0.5646425),
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.9553365, 0.29552022),
            casa_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casa_types::Complex32::new(0.9800666, -0.19866933),
            casa_types::Complex32::new(0.87758255, 0.47942555),
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.9553365, 0.29552022),
            casa_types::Complex32::new(0.921061, -0.38941833),
        ],
        [
            casa_types::Complex32::new(0.9800666, -0.19866933),
            casa_types::Complex32::new(0.87758255, 0.47942555),
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.9887711, 0.14943813),
            casa_types::Complex32::new(0.9689124, -0.24740396),
        ],
        [
            casa_types::Complex32::new(0.9950042, -0.09983342),
            casa_types::Complex32::new(0.9393727, 0.3428978),
        ],
    ];
    let residual_gains = [
        [
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(1.0, 0.0),
        ],
        [
            casa_types::Complex32::new(0.99875027, 0.04997917),
            casa_types::Complex32::new(0.9800666, -0.19866933),
        ],
        [
            casa_types::Complex32::new(0.9921977, -0.12467473),
            casa_types::Complex32::new(0.9553365, 0.29552022),
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
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
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
