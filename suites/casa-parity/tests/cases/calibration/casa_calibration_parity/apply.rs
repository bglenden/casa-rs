// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn apply_phase_gain_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let phase_gcal = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_ms = dir.path().join("rust-apply.ms");
    let casa_ms = dir.path().join("casa-apply.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");

    common::run_casa_applycal(
        &casa_ms,
        &phase_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("run CASA applycal");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&phase_gcal)],
        },
    )
    .expect("run Rust apply");

    assert_apply_state_close(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
    );
}

#[test]
fn apply_k_delay_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let delay_kcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "K",
            calmode: "p",
            solint: "inf",
            combine: "",
            prior_gaintables: Vec::new(),
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));

    let delay_summary = summarize_table(&delay_kcal).expect("summarize CASA K table");
    assert_eq!(delay_summary.table_subtype, "K Jones");
    assert_eq!(
        delay_summary.parameter_family,
        CalibrationParameterFamily::Float
    );
    assert!(delay_summary.supported_for_v1_apply());

    let rust_ms = dir.path().join("rust-delay.ms");
    let casa_ms = dir.path().join("casa-delay.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");

    common::run_casa_applycal(
        &casa_ms,
        &delay_kcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("run CASA applycal for K Jones");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&delay_kcal)],
        },
    )
    .expect("run Rust apply for K Jones");

    assert_apply_state_close_with_tolerance(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[test]
fn apply_bpoly_bandpass_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_gain = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));
    let bpoly = common::run_casa_bandpass_bpoly(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[prior_gain.as_path()],
    )
    .unwrap_or_else(|reason| panic!("{reason}"));

    let bpoly_summary = summarize_table(&bpoly).expect("summarize CASA BPOLY table");
    assert_eq!(bpoly_summary.table_subtype, "BPOLY");
    assert!(bpoly_summary.supported_for_v1_apply());

    let rust_ms = dir.path().join("rust-bpoly.ms");
    let casa_ms = dir.path().join("casa-bpoly.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");

    common::run_casa_applycal_chain(
        &casa_ms,
        &[prior_gain.as_path(), bpoly.as_path()],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("run CASA applycal with prior gain and BPOLY");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![
                ApplyCalibrationTableSpec::new(&prior_gain),
                ApplyCalibrationTableSpec::new(&bpoly),
            ],
        },
    )
    .expect("run Rust apply with prior gain and BPOLY");

    assert_apply_state_close_with_tolerance(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        2.0e-3,
    );
}

#[test]
fn apply_phase_gain_with_calwt_matches_casa_applycal_weights_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let phase_gcal = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_ms = dir.path().join("rust-calwt.ms");
    let casa_ms = dir.path().join("casa-calwt.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");

    common::run_casa_applycal(
        &casa_ms,
        &phase_gcal,
        "0",
        "0",
        None,
        "calflag",
        true,
        false,
    )
    .expect("run CASA applycal");

    let mut spec = ApplyCalibrationTableSpec::new(&phase_gcal);
    spec.calwt = true;
    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![spec],
        },
    )
    .expect("run Rust apply with calwt");

    assert_apply_state_close(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
    );
}

#[test]
fn apply_phase_gain_with_parang_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let phase_gcal = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_ms = dir.path().join("rust-apply-parang.ms");
    let casa_ms = dir.path().join("casa-apply-parang.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");

    common::run_casa_applycal(
        &casa_ms,
        &phase_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        true,
    )
    .expect("run CASA applycal with parang");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: true,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&phase_gcal)],
        },
    )
    .expect("run Rust apply with parang");

    assert_apply_state_close_with_tolerance(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[test]
fn apply_phase_gain_with_parang_matches_casa_applycal_on_nasmyth_bwg_and_other_mounts() {
    // Each mount turns the feed by its own term (casacore
    // `MSDerivedValues::parAngle`): Nasmyth by ±E, beam waveguide by
    // ±(E − A), X-Y by its own angle, equatorial not at all, and an empty
    // MOUNT as alt-az.
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let phase_gcal = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_ms = dir.path().join("rust-apply-parang-mounts.ms");
    let casa_ms = dir.path().join("casa-apply-parang-mounts.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");
    let mounts = [
        (0, "ALT-AZ+BWG-R"),
        (1, "ALT-AZ+BWG-L"),
        (2, "ALT-AZ+NASMYTH-R"),
        (3, "ALT-AZ+NASMYTH-L"),
        (4, "X-Y"),
        (5, "EQUATORIAL"),
        (6, ""),
    ];
    common::set_ms_antenna_mounts(&rust_ms, &mounts);
    common::set_ms_antenna_mounts(&casa_ms, &mounts);

    common::run_casa_applycal(
        &casa_ms,
        &phase_gcal,
        "0",
        "0",
        None,
        "calflag",
        false,
        true,
    )
    .expect("run CASA applycal with parang on mixed mounts");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: true,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&phase_gcal)],
        },
    )
    .expect("run Rust apply with parang on mixed mounts");

    assert_apply_state_close_with_tolerance(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[test]
fn apply_phase_gain_via_callib_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let phase_gcal = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));
    let callib_path = dir.path().join("apply.callib");
    std::fs::write(
        &callib_path,
        format!(
            "caltable='{}' calwt=F tinterp='nearest'\n",
            phase_gcal.display()
        ),
    )
    .expect("write callib file");

    let rust_ms = dir.path().join("rust-callib.ms");
    let casa_ms = dir.path().join("casa-callib.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");

    common::run_casa_applycal_with_callib(&casa_ms, &callib_path, "0", "0", "calflag", false)
        .expect("run CASA applycal with callib");

    let specs = load_apply_specs_from_callib(&callib_path).expect("load Rust callib");
    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: specs,
        },
    )
    .expect("run Rust apply from callib");

    assert_apply_state_close(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
    );
}

#[test]
fn apply_nearest_gainfield_matches_casa_on_real_ms() {
    let dir = TempDir::new().expect("tempdir");
    let (fixture_ms, caltable) = common::generate_casa_fluxscale_gain_fixture(
        dir.path(),
        "1331+305*",
        "1445+099*",
        "0",
        "VA15",
    )
    .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_ms = dir.path().join("rust-nearest.ms");
    let casa_ms = dir.path().join("casa-nearest.ms");
    common::copy_measurement_set(&fixture_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&fixture_ms, &casa_ms).expect("copy casa ms");
    let (field1_ra, field1_dec) = read_field_direction(&fixture_ms, 1);
    common::set_ms_field_directions(&rust_ms, &[(0, field1_ra, field1_dec)]);
    common::set_ms_field_directions(&casa_ms, &[(0, field1_ra, field1_dec)]);

    common::run_casa_applycal_with_gainfield(
        &casa_ms, &caltable, "0", "0", "nearest", "calflag", false, false,
    )
    .expect("run CASA applycal with gainfield=nearest");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                callibrary: None,
                path: caltable,
                apply_to: Default::default(),
                gainfield: Some(GainFieldSelector::Nearest),
                spwmap: Vec::new(),
                interp: casa_calibration::apply::ApplyInterpolationMode::Nearest,
                calwt: false,
            }],
        },
    )
    .expect("run Rust apply with gainfield=nearest");

    assert_apply_state_close(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
    );
}
