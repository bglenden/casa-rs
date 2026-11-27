// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn solve_bandpass_with_prior_gain_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
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
    let casa_b = common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
        "",
        false,
        false,
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_b = dir.path().join("rust-bandpass.bcal");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust bandpass solve");

    let casa_apply_ms = dir.path().join("casa-bandpass.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_b],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior + bandpass");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_b],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust bandpass table in CASA");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        5.0e-3,
        5.0e-3,
    );
}

#[test]
fn solve_bandpass_with_combine_field_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0,1",
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
    let casa_b = common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0,1",
        "0",
        "VA15",
        &[&prior_g],
        "field",
        false,
        false,
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_b = dir.path().join("rust-bandpass-combine-field.bcal");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: false,
                fields: true,
                ..BandpassSolveCombine::default()
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust combine-field bandpass solve");

    let casa_apply_ms = dir.path().join("casa-bandpass-combine-field.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass-combine-field.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_b],
        "0,1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior + combine-field bandpass");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_b],
        "0,1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust combine-field bandpass table in CASA");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0, 1]).spw(&[0]),
        2.0,
        1.0e-1,
    );
}

#[test]
fn solve_bandpass_with_combine_scan_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "1",
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
    let casa_b = common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "1",
        "0",
        "VA15",
        &[&prior_g],
        "scan",
        false,
        false,
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_b = dir.path().join("rust-bandpass-combine-scan.bcal");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[1]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: true,
                fields: false,
                ..BandpassSolveCombine::default()
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust combine-scan bandpass solve");

    let casa_apply_ms = dir.path().join("casa-bandpass-combine-scan.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass-combine-scan.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_b],
        "1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior + combine-scan bandpass");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_b],
        "1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust combine-scan bandpass table in CASA");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[1]).spw(&[0]),
        3.0e-2,
        3.0e-2,
    );
}

#[test]
fn solve_bandpass_with_combine_scan_and_field_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0,1",
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
    let casa_b = common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0,1",
        "0",
        "VA15",
        &[&prior_g],
        "scan,field",
        false,
        false,
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_b = dir.path().join("rust-bandpass-combine-scan-field.bcal");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: true,
                fields: true,
                ..BandpassSolveCombine::default()
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust combine-scan-field bandpass solve");

    let casa_apply_ms = dir.path().join("casa-bandpass-combine-scan-field.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass-combine-scan-field.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_b],
        "0,1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior + combine-scan-field bandpass");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_b],
        "0,1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust combine-scan-field bandpass table in CASA");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0, 1]).spw(&[0]),
        3.0e-2,
        3.0e-2,
    );
}

#[test]
fn solve_bandpass_with_prior_gain_and_parang_matches_casa_bandpass_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
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
    let casa_b = common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
        "",
        false,
        true,
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_b = dir.path().join("rust-bandpass-parang.bcal");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: true,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust bandpass solve with parang");

    let casa_apply_ms = dir.path().join("casa-bandpass-parang.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass-parang.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_b],
        "0",
        "0",
        None,
        "calflag",
        false,
        true,
    )
    .expect("apply CASA prior + bandpass with parang");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_b],
        "0",
        "0",
        None,
        "calflag",
        false,
        true,
    )
    .expect("apply Rust bandpass table in CASA with parang");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        5.0e-3,
        5.0e-3,
    );
}

#[test]
fn solve_bandpass_with_solnorm_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
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
    let casa_b = common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
        "",
        true,
        false,
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_b = dir.path().join("rust-bandpass-solnorm.bcal");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::B,
            normalize_average_amplitude: true,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust bandpass solve with solnorm");

    let casa_apply_ms = dir.path().join("casa-bandpass-solnorm.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass-solnorm.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_b],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior + normalized bandpass");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_b],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust normalized bandpass table in CASA");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        5.0e-3,
        5.0e-3,
    );
}

#[test]
fn solve_bpoly_bandpass_with_prior_gain_matches_casa_bandpass_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
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
    let casa_bpoly =
        common::run_casa_bandpass_bpoly(dir.path(), &source_ms, "0", "0", "VA15", &[&prior_g])
            .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_bpoly = dir.path().join("rust-bandpass.bpoly");
    run_bandpass_solve(
        &source_ms,
        &BandpassSolveRequest {
            model_source: Default::default(),
            solve_interval: Default::default(),
            frequency_interval: None,
            correlation_dependent_flags: false,
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_bpoly.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine::default(),
            band_type: BandpassType::BPoly,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
            cuts: None,
        },
    )
    .expect("run Rust BPOLY bandpass solve");

    let casa_apply_ms = dir.path().join("casa-bandpass-bpoly.ms");
    let rust_apply_ms = dir.path().join("rust-bandpass-bpoly.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[&prior_g, &casa_bpoly],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior + BPOLY");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[&prior_g, &rust_bpoly],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust BPOLY table in CASA");

    assert_apply_state_close_with_complex_tolerances(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        6.0e-2,
        6.0e-2,
    );
}
