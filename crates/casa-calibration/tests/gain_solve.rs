// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, ApplyPlanRequest,
    CalibrationDataset, CalibrationError, CalibrationSolveRequest, CalibrationSolveResult,
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainSolveReport, GainSolveRequest, GainType, RefAntSelector, execute_apply_from_path,
    solve_calibration, summarize_table,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

fn run_gain_solve(
    path: impl AsRef<std::path::Path>,
    request: &GainSolveRequest,
) -> Result<GainSolveReport, CalibrationError> {
    match solve_calibration(
        CalibrationDataset::path(path),
        CalibrationSolveRequest::Gain(request.clone()),
    )? {
        CalibrationSolveResult::Gain(report) => Ok(report),
        CalibrationSolveResult::Bandpass(_) => unreachable!("gain request result"),
    }
}

/// Assert that a solve found nothing to solve: CASA's behaviour is to write
/// no caltable at all rather than a table of flagged or fabricated rows.
fn assert_no_solutions(
    result: Result<GainSolveReport, CalibrationError>,
    caltable_path: &std::path::Path,
) {
    match result {
        Err(CalibrationError::Gain(error)) if matches!(*error, GainSolveError::NoSolutions) => {}
        other => panic!("expected NoSolutions, got {other:?}"),
    }
    assert!(
        !caltable_path.exists(),
        "no caltable should be written when nothing solves"
    );
}

fn phase_gain_request(output_table: std::path::PathBuf) -> GainSolveRequest {
    GainSolveRequest {
        selection: MsSelection::new(),
        output_table,
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
    }
}

#[test]
fn shared_solve_setup_reports_invalid_empty_missing_prior_and_refant_errors() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);

    let mut invalid = phase_gain_request(dir.path().join("invalid.gcal"));
    invalid.selection.field = Some("[".to_string());
    assert!(
        run_gain_solve(&ms_path, &invalid)
            .expect_err("invalid selector must fail")
            .to_string()
            .contains("field")
    );

    let mut empty = phase_gain_request(dir.path().join("empty.gcal"));
    empty.selection.scan = Some("999".to_string());
    assert!(
        run_gain_solve(&ms_path, &empty)
            .expect_err("empty selection must fail")
            .to_string()
            .contains("selection produced no rows")
    );

    let mut missing_prior = phase_gain_request(dir.path().join("missing-prior.gcal"));
    missing_prior.prior_calibration_tables = vec![ApplyCalibrationTableSpec::new(
        dir.path().join("does-not-exist.gcal"),
    )];
    assert!(
        run_gain_solve(&ms_path, &missing_prior)
            .expect_err("missing prior table must fail")
            .to_string()
            .contains("prior calibration")
    );

    let mut bad_refant = phase_gain_request(dir.path().join("bad-refant.gcal"));
    bad_refant.refant = RefAntSelector::AntennaName("missing".to_string());
    assert!(
        run_gain_solve(&ms_path, &bad_refant)
            .expect_err("missing refant must fail")
            .to_string()
            .contains("no ANTENNA.NAME match")
    );
}

#[test]
fn solve_gain_phase_g_corrects_synthetic_ms_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("solved.gcal");

    let report = run_gain_solve(
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
            min_snr: 3.0,
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

    let report = run_gain_solve(
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
            min_snr: 3.0,
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
fn solve_gain_channel_average_uses_sigma_spectrum_when_present() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_weight_spectrum_fixture_ms(dir.path());
    let caltable_path = dir.path().join("solved-weight-spectrum.gcal");

    run_gain_solve(
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
    .expect("solve synthetic G gains with weight spectrum");

    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("apply weight-spectrum solved G table");

    let ms = MeasurementSet::open(&ms_path).expect("reopen solved weight-spectrum MS");
    let corrected = ms
        .data_column(casa_ms::schema::main_table::VisibilityDataColumn::CorrectedData)
        .expect("corrected data column");
    for row in 0..ms.row_count() {
        let ArrayValue::Complex32(values) = corrected.get(row).expect("corrected row") else {
            panic!("expected complex corrected data");
        };
        for corr_index in 0..2 {
            let value = values[[corr_index, 0]];
            assert!(
                (value.re - 1.0).abs() <= 1.0e-4 && value.im.abs() <= 1.0e-4,
                "row {row} corr {corr_index} channel 0 corrected to {value:?}"
            );
        }
    }
}

#[test]
fn solve_gain_phase_t_uses_per_correlation_model_data_column_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_model_column_fixture_ms_for_kind(
        dir.path(),
        common::SyntheticGainFixtureKind::T,
        [
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(0.0, 2.0),
            casa_types::Complex32::new(1.0, 0.0),
            casa_types::Complex32::new(0.0, 2.0),
        ],
    );
    let caltable_path = dir.path().join("solved-model.tcal");

    let report = run_gain_solve(
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
            model_source: GainSolveModelSource::ModelColumn,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 0,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("solve synthetic T gains against per-correlation MODEL_DATA");

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
    .expect("apply model-column solved T table");

    common::assert_corrected_rows_match_model_column(&ms_path);
}

/// FLAG is set for LL on every channel. CASA flags samples independently
/// of correlation (`VisCalSolver2` via `SolveDataBuffer::enforceAPonData`
/// and `corrIndepFlags`), so no RR samples remain either; CASA then solves
/// nothing and writes no table ("No output calibration table written"). A
/// per-correlation flag reading would have solved RR.
#[test]
fn solve_gain_g_uses_casa_correlation_independent_flags() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    {
        let mut ms = MeasurementSet::open(&ms_path).expect("open synthetic MS");
        for row in 0..ms.row_count() {
            ms.main_table_mut()
                .column_accessor_mut("FLAG")
                .and_then(|mut column| {
                    column.set(
                        row,
                        Value::Array(ArrayValue::Bool(
                            ArrayD::from_shape_vec(
                                IxDyn(&[2, 2]).f(),
                                vec![false, true, false, true],
                            )
                            .expect("flag array"),
                        )),
                    )
                })
                .expect("write asymmetric flags");
        }
        let rows = (0..ms.row_count()).collect::<Vec<_>>();
        ms.main_table_mut()
            .prepare_write()
            .save_selected_rows(&["FLAG"], &rows)
            .expect("save asymmetric flags");
    }
    let caltable_path = dir.path().join("solved-corr-independent.gcal");

    let result = run_gain_solve(
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
    );

    assert_no_solutions(result, &caltable_path);
}

/// FLAG is set for LL on every channel. CASA flags samples independently
/// of correlation (`VisCalSolver2` via `SolveDataBuffer::enforceAPonData`
/// and `corrIndepFlags`), so no RR samples remain either; CASA then solves
/// nothing and writes no table ("No output calibration table written"). A
/// per-correlation flag reading would have solved RR.
#[test]
fn solve_gain_t_uses_casa_correlation_independent_flags() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::T);
    {
        let mut ms = MeasurementSet::open(&ms_path).expect("open synthetic MS");
        for row in 0..ms.row_count() {
            ms.main_table_mut()
                .column_accessor_mut("FLAG")
                .and_then(|mut column| {
                    column.set(
                        row,
                        Value::Array(ArrayValue::Bool(
                            ArrayD::from_shape_vec(
                                IxDyn(&[2, 2]).f(),
                                vec![false, true, false, true],
                            )
                            .expect("flag array"),
                        )),
                    )
                })
                .expect("write asymmetric flags");
        }
        let rows = (0..ms.row_count()).collect::<Vec<_>>();
        ms.main_table_mut()
            .prepare_write()
            .save_selected_rows(&["FLAG"], &rows)
            .expect("save asymmetric flags");
    }
    let caltable_path = dir.path().join("solved-t-per-corr-flags.tcal");

    let result = run_gain_solve(
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
    );

    assert_no_solutions(result, &caltable_path);
}

#[test]
fn solve_gain_min_snr_flags_low_snr_solutions_and_writes_diagnostics() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("solved-minsnr.gcal");

    run_gain_solve(
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

        let gains = match table
            .cell_accessor(row, "CPARAM")
            .and_then(|cell| cell.array())
            .expect("CPARAM cell")
        {
            ArrayValue::Complex32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected CPARAM value: {other:?}"),
        };
        assert!(
            gains
                .iter()
                .all(|gain| (gain.re - 1.0).abs() < 1.0e-6 && gain.im.abs() < 1.0e-6)
        );

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

    let report = run_gain_solve(
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

    let report = run_gain_solve(
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

    let report = run_gain_solve(
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

    run_gain_solve(
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

    let report = run_gain_solve(
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

    let report = run_gain_solve(
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
            calibration_tables: vec![{
                let mut spec = ApplyCalibrationTableSpec::new(&caltable_path);
                spec.interp = ApplyInterpolationMode::Nearest;
                spec
            }],
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

    let report = run_gain_solve(
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

    let report = run_gain_solve(
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

    let report = run_gain_solve(
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

/// One `SPECTRAL_WINDOW` row of a gain table: the columns it collapses.
#[derive(Debug, PartialEq)]
struct GainTableWindow {
    channels: i32,
    frequency: Vec<f64>,
    width: Vec<f64>,
    effective_bw: Vec<f64>,
    resolution: Vec<f64>,
    total_bandwidth: f64,
    flagged: bool,
}

impl GainTableWindow {
    fn read(table: &Table, row: usize) -> Self {
        let cell = |column: &str| {
            table
                .cell_accessor(row, column)
                .and_then(|cell| cell.value().map(|value| value.cloned()))
                .expect("cell")
                .expect("defined")
        };
        let array = |column: &str| match cell(column) {
            Value::Array(ArrayValue::Float64(values)) => values.iter().copied().collect(),
            other => panic!("{column}: {other:?}"),
        };
        let (
            Value::Scalar(ScalarValue::Int32(channels)),
            Value::Scalar(ScalarValue::Float64(total_bandwidth)),
            Value::Scalar(ScalarValue::Bool(flagged)),
        ) = (cell("NUM_CHAN"), cell("TOTAL_BANDWIDTH"), cell("FLAG_ROW"))
        else {
            panic!("NUM_CHAN, TOTAL_BANDWIDTH or FLAG_ROW");
        };
        Self {
            channels,
            frequency: array("CHAN_FREQ"),
            width: array("CHAN_WIDTH"),
            effective_bw: array("EFFECTIVE_BW"),
            resolution: array("RESOLUTION"),
            total_bandwidth,
            flagged,
        }
    }

    /// A window a solve reached: one channel of `width` at `frequency`, its
    /// effective bandwidth `width` and its resolution and total bandwidth
    /// `frequency`, as CASA writes them.
    fn solved(frequency: f64, width: f64, flagged: bool) -> Self {
        Self {
            channels: 1,
            frequency: vec![frequency],
            width: vec![width],
            effective_bw: vec![width],
            resolution: vec![frequency],
            total_bandwidth: frequency,
            flagged,
        }
    }

    /// The fixture's second window, which no solve reached: collapsed over
    /// its two 1 MHz channels from 1.1 GHz, its total bandwidth the
    /// MeasurementSet's, and flagged.
    fn unsolved() -> Self {
        Self {
            channels: 1,
            frequency: vec![1.1005e9],
            width: vec![2.0e6],
            effective_bw: vec![2.0e6],
            resolution: vec![2.0e6],
            total_bandwidth: 2.0e6,
            flagged: true,
        }
    }
}

/// A gain table's windows are CASA's (#917). The solved window is one
/// channel at its selected channels' arithmetic mean frequency, of their
/// summed width; the window without solutions (the fixture's second,
/// 1.1 GHz) is one channel at the mean of all its channels, its widths
/// summed and its total bandwidth the MeasurementSet's, and flagged. The
/// fixture's windows are two 1 MHz channels from 1.0 and 1.1 GHz.
#[test]
fn solve_gain_writes_windows_collapsed_as_casa_does() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    for (name, spw, frequency, width) in [
        ("all", None, 1.0005e9, 2.0e6),
        ("second", Some("0:1"), 1.001e9, 1.0e6),
    ] {
        let mut request = phase_gain_request(dir.path().join(format!("{name}.gcal")));
        if let Some(spw) = spw {
            request.selection = request.selection.spw_selector(spw).expect("spw");
        }
        let report = run_gain_solve(&ms_path, &request).expect("solve");
        assert_eq!(report.spectral_window_ids, vec![0], "{name}");
        let table = Table::open(TableOptions::new(
            request.output_table.join("SPECTRAL_WINDOW"),
        ))
        .expect("SPECTRAL_WINDOW");
        assert_eq!(
            GainTableWindow::read(&table, 0),
            GainTableWindow::solved(frequency, width, false),
            "{name}: solved"
        );
        assert_eq!(
            GainTableWindow::read(&table, 1),
            GainTableWindow::unsolved(),
            "{name}: without solutions"
        );
    }
}

/// A window a solve reached but could not solve keeps the solved
/// collapse, flagged, as CASA's: `Calibrater::genericGatherAndSolve` sets
/// the window's frequencies once an interval has unflagged weight, before
/// it solves, and `flagAbsentSpws` later flags the window it wrote no row
/// for. Here the fixture's second window holds one baseline, too few for
/// two baselines per antenna.
#[test]
fn solve_gain_keeps_the_collapse_of_a_window_it_could_not_solve() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    {
        // Row 6 is baseline 0-1 at the third time; it moves to the second
        // window, which the second data description describes.
        let mut ms = MeasurementSet::open(&ms_path).expect("open");
        ms.main_table_mut()
            .cell_accessor_mut(6, "DATA_DESC_ID")
            .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Int32(1))))
            .expect("move row 6");
        ms.save().expect("save");
    }
    // Whole, and with the second window's second channel alone selected,
    // which pins its frequency and width to the selection too.
    for (name, spw, frequency, width) in [
        ("whole", None, 1.1005e9, 2.0e6),
        ("second", Some("0,1:1"), 1.101e9, 1.0e6),
    ] {
        let mut request = phase_gain_request(dir.path().join(format!("{name}.gcal")));
        request.min_baselines_per_antenna = 2;
        if let Some(spw) = spw {
            request.selection = request.selection.spw_selector(spw).expect("spw");
        }
        let report = run_gain_solve(&ms_path, &request).expect("solve");
        assert_eq!(
            report.spectral_window_ids,
            vec![0],
            "{name}: only the first solves"
        );
        let table = Table::open(TableOptions::new(
            request.output_table.join("SPECTRAL_WINDOW"),
        ))
        .expect("SPECTRAL_WINDOW");
        assert_eq!(
            GainTableWindow::read(&table, 0),
            GainTableWindow::solved(1.0005e9, 2.0e6, false),
            "{name}"
        );
        assert_eq!(
            GainTableWindow::read(&table, 1),
            GainTableWindow::solved(frequency, width, true),
            "{name}: reached, not solved"
        );
    }
}
