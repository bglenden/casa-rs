// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn solve_phase_gain_matches_casa_gaincal_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let casa_gcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "inf",
            combine: "",
            prior_gaintables: vec![],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_gcal = dir.path().join("rust-phase.gcal");
    run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_gcal.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            min_baselines_per_antenna: 4,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust solve");

    let casa_apply_ms = dir.path().join("casa-solved.ms");
    let rust_apply_ms = dir.path().join("rust-solved.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA gaincal table");
    common::run_casa_applycal(
        &rust_apply_ms,
        &rust_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust gaincal table in CASA");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[test]
fn solve_phase_gain_with_parang_matches_casa_gaincal_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let casa_gcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "inf",
            combine: "",
            prior_gaintables: vec![],
            parang: true,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_gcal = dir.path().join("rust-phase-parang.gcal");
    run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_gcal.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: Vec::new(),
            parang: true,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            min_baselines_per_antenna: 4,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust solve with parang");

    let casa_apply_ms = dir.path().join("casa-solved-parang.ms");
    let rust_apply_ms = dir.path().join("rust-solved-parang.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        true,
    )
    .expect("apply CASA gaincal table with parang");
    common::run_casa_applycal(
        &rust_apply_ms,
        &rust_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        true,
    )
    .expect("apply Rust gaincal table in CASA with parang");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        2.0e-3,
    );
}

#[test]
fn solve_amplitude_phase_gain_matches_casa_gaincal_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let casa_gcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "ap",
            solint: "inf",
            combine: "",
            prior_gaintables: vec![],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_gcal = dir.path().join("rust-amplitude-phase.gcal");
    run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_gcal.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::AmplitudePhase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            min_baselines_per_antenna: 4,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust solve");

    let casa_apply_ms = dir.path().join("casa-solved-ap.ms");
    let rust_apply_ms = dir.path().join("rust-solved-ap.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA gaincal table");
    common::run_casa_applycal(
        &rust_apply_ms,
        &rust_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust gaincal table in CASA");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[test]
fn solve_phase_gain_with_solint_integration_matches_casa_gaincal_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let casa_gcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "int",
            combine: "",
            prior_gaintables: vec![],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_gcal = dir.path().join("rust-int.gcal");
    run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_gcal.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Integration,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            min_baselines_per_antenna: 4,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust integration solve");

    let casa_apply_ms = dir.path().join("casa-solved-int.ms");
    let rust_apply_ms = dir.path().join("rust-solved-int.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA integration gaincal table");
    common::run_casa_applycal(
        &rust_apply_ms,
        &rust_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust integration gaincal table in CASA");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[test]
fn solve_phase_gain_with_combine_scan_matches_casa_gaincal_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let casa_gcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "inf",
            combine: "scan",
            prior_gaintables: vec![],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_gcal = dir.path().join("rust-combine-scan.gcal");
    run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_gcal.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine {
                scans: true,
                fields: false,
                ..GainSolveCombine::default()
            },
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            min_baselines_per_antenna: 4,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust combine-scan solve");

    let casa_apply_ms = dir.path().join("casa-solved-combine-scan.ms");
    let rust_apply_ms = dir.path().join("rust-solved-combine-scan.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA combine-scan gaincal table");
    common::run_casa_applycal(
        &rust_apply_ms,
        &rust_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust combine-scan gaincal table in CASA");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}
