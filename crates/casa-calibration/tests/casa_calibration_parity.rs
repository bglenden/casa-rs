// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]

mod common;

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32};
use ndarray::Ix2;
use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, BandpassSolveCombine,
    BandpassSolveRequest, BandpassType, CalibrationParameterFamily, FluxScaleRequest,
    GainFieldSelector, GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveRequest,
    GainType, RefAntSelector, execute_apply_from_path, fluxscale, load_apply_specs_from_callib,
    solve_bandpass_from_path, solve_gain_from_path, summarize_table,
};

#[test]
fn summarize_casa_generated_gain_t_and_bandpass_tables() {
    let dir = TempDir::new().expect("tempdir");
    let (g_path, t_path, b_path) = match common::generate_casa_exemplars(dir.path()) {
        Ok(paths) => paths,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let g_summary = summarize_table(&g_path).expect("summarize G");
    let t_summary = summarize_table(&t_path).expect("summarize T");
    let b_summary = summarize_table(&b_path).expect("summarize B");

    for summary in [&g_summary, &t_summary, &b_summary] {
        assert_eq!(summary.table_type, "Calibration");
        assert_eq!(
            summary.parameter_family,
            CalibrationParameterFamily::Complex
        );
        assert!(summary.row_count > 0);
        assert!(
            summary.columns.iter().any(|column| column == "CPARAM"),
            "missing CPARAM in {}",
            summary.path.display()
        );
    }

    assert_eq!(g_summary.table_subtype, "G Jones");
    assert_eq!(t_summary.table_subtype, "T Jones");
    assert_eq!(b_summary.table_subtype, "B Jones");
}

#[test]
fn apply_phase_gain_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let phase_gcal = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

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
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let delay_kcal = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

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
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_gain = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let bpoly = match common::run_casa_bandpass_bpoly(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[prior_gain.as_path()],
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

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
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let phase_gcal = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

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

#[cfg(feature = "slow-tests")]
#[test]
fn apply_phase_gain_with_parang_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let phase_gcal = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

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

#[cfg(feature = "slow-tests")]
#[test]
fn apply_phase_gain_with_parang_matches_casa_applycal_on_bwg_mounts() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let phase_gcal = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let rust_ms = dir.path().join("rust-apply-parang-bwg.ms");
    let casa_ms = dir.path().join("casa-apply-parang-bwg.ms");
    common::copy_measurement_set(&source_ms, &rust_ms).expect("copy rust ms");
    common::copy_measurement_set(&source_ms, &casa_ms).expect("copy casa ms");
    let bwg_mounts = [
        (0, "ALT-AZ+BWG-R"),
        (1, "ALT-AZ+BWG-R"),
        (2, "ALT-AZ+BWG-R"),
    ];
    common::set_ms_antenna_mounts(&rust_ms, &bwg_mounts);
    common::set_ms_antenna_mounts(&casa_ms, &bwg_mounts);

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
    .expect("run CASA applycal with BWG parang");

    execute_apply_from_path(
        &rust_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: true,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&phase_gcal)],
        },
    )
    .expect("run Rust apply with BWG parang");

    assert_apply_state_close_with_tolerance(
        &rust_ms,
        &casa_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[cfg(feature = "slow-tests")]
#[test]
fn apply_phase_gain_via_callib_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let phase_gcal = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
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

#[cfg(feature = "slow-tests")]
#[test]
fn apply_nearest_gainfield_matches_casa_on_real_ms() {
    let dir = TempDir::new().expect("tempdir");
    let (fixture_ms, caltable) = match common::generate_casa_fluxscale_gain_fixture(
        dir.path(),
        "1331+305*",
        "1445+099*",
        "0",
        "VA15",
    ) {
        Ok(paths) => paths,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

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
                path: caltable,
                apply_to: Default::default(),
                gainfield: Some(GainFieldSelector::Nearest),
                spwmap: Vec::new(),
                interp: casa_calibration::ApplyInterpolationMode::Nearest,
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

#[test]
fn solve_phase_gain_matches_casa_gaincal_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let casa_gcal = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_gcal = dir.path().join("rust-phase.gcal");
    solve_gain_from_path(
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_phase_gain_with_parang_matches_casa_gaincal_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let casa_gcal = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_gcal = dir.path().join("rust-phase-parang.gcal");
    solve_gain_from_path(
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_amplitude_phase_gain_matches_casa_gaincal_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let casa_gcal = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_gcal = dir.path().join("rust-amplitude-phase.gcal");
    solve_gain_from_path(
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_phase_gain_with_solint_integration_matches_casa_gaincal_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let casa_gcal = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_gcal = dir.path().join("rust-int.gcal");
    solve_gain_from_path(
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
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let casa_gcal = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_gcal = dir.path().join("rust-combine-scan.gcal");
    solve_gain_from_path(
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
            },
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: Vec::new(),
            parang: false,
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

#[test]
fn apply_combined_scan_field_gain_table_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let casa_gcal = match common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0,1",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "inf",
            combine: "scan,field",
            prior_gaintables: vec![],
            parang: false,
        },
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let rust_apply_ms = dir.path().join("rust-apply-combine-scan-field.ms");
    let casa_apply_ms = dir.path().join("casa-apply-combine-scan-field.ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy Rust apply MS");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy CASA apply MS");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0,1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA combined-field gain table");
    execute_apply_from_path(
        &rust_apply_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&casa_gcal)],
        },
    )
    .expect("apply combined-field CASA gain table in Rust");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0, 1]).spw(&[0]),
        5.0e-4,
    );
}

#[test]
fn solve_gain_phase_g_combine_scan_and_field_writes_one_solution_group_across_fields() {
    let dir = TempDir::new().expect("tempdir");
    if common::discover_casa_python().is_none() {
        eprintln!("{}", common::casa_skip_reason());
        return;
    }

    let gains = [
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
    let source_ms = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[common::SyntheticGainTimeCluster {
            time_seconds: 100.0,
            scan_number: 1,
            gains,
        }],
    );
    let mut fixture_ms = MeasurementSet::open(&source_ms).expect("reopen synthetic MS");
    common::append_gain_solve_cluster_for_field(
        &mut fixture_ms,
        1,
        &common::SyntheticGainTimeCluster {
            time_seconds: 200.0,
            scan_number: 2,
            gains,
        },
    );
    fixture_ms.save().expect("save synthetic multifield MS");

    let rust_gcal = dir.path().join("rust-combine-scan-field.gcal");
    let report = solve_gain_from_path(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: rust_gcal.clone(),
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
    .expect("run Rust combine-scan-field solve");
    assert_eq!(report.solution_row_count, 3);
}

#[test]
fn fluxscale_matches_casa_on_ngc5921_gain_table() {
    let dir = TempDir::new().expect("tempdir");
    let (fixture_ms, input_gcal) = match common::generate_casa_fluxscale_gain_fixture(
        dir.path(),
        "1331+305*",
        "1445+099*",
        "0",
        "VA15",
    ) {
        Ok(paths) => paths,
        Err(reason) => {
            if reason.starts_with("CASA calibration parity skipped:") {
                eprintln!("{reason}");
                return;
            }
            panic!("{reason}");
        }
    };
    let casa_fcal = dir.path().join("casa.fcal");
    let casa_report = match common::run_casa_fluxscale(
        &fixture_ms,
        &input_gcal,
        &casa_fcal,
        "1331+305*",
        "1445+099*",
    ) {
        Ok(report) => report,
        Err(reason) => {
            if reason.starts_with("CASA calibration parity skipped:") {
                eprintln!("{reason}");
                return;
            }
            panic!("{reason}");
        }
    };

    let rust_fcal = dir.path().join("rust.fcal");
    let rust_report = fluxscale(&FluxScaleRequest {
        input_table: input_gcal.clone(),
        output_table: rust_fcal.clone(),
        reference_fields: vec!["1331+305*".to_string()],
        transfer_fields: vec!["1445+099*".to_string()],
        refspwmap: Vec::new(),
        gainthreshold: None,
        incremental: false,
    })
    .expect("run Rust fluxscale");

    let casa_field = find_casa_fluxscale_field(&casa_report, "1445+099");
    let casa_fit_fluxd = casa_fit_fluxd(casa_field);
    let casa_fit_ref_frequency_hz = casa_fit_ref_frequency_hz(casa_field);
    let rust_field = rust_report
        .fields
        .values()
        .next()
        .expect("Rust transfer field");

    assert!(
        (rust_field.fit_fluxd - casa_fit_fluxd).abs() <= 5.0e-3,
        "fit flux mismatch: casa={casa_fit_fluxd:.9} rust={:.9}",
        rust_field.fit_fluxd
    );
    assert!(
        (rust_field.fit_ref_frequency_hz - casa_fit_ref_frequency_hz).abs() <= 1.0,
        "fit ref frequency mismatch: casa={casa_fit_ref_frequency_hz:.6} rust={:.6}",
        rust_field.fit_ref_frequency_hz
    );

    let rust_rows = read_cparam_rows_for_field(&rust_fcal, 1);
    let casa_rows = read_cparam_rows_for_field(&casa_fcal, 1);
    assert_eq!(
        rust_rows.len(),
        casa_rows.len(),
        "transfer row count mismatch"
    );
    for (row_index, (rust_row, casa_row)) in rust_rows.iter().zip(casa_rows.iter()).enumerate() {
        assert_eq!(
            rust_row.len(),
            casa_row.len(),
            "transfer gain count mismatch on row {row_index}"
        );
        for (sample_index, (rust_gain, casa_gain)) in
            rust_row.iter().zip(casa_row.iter()).enumerate()
        {
            let re_diff = (rust_gain.re - casa_gain.re).abs();
            let im_diff = (rust_gain.im - casa_gain.im).abs();
            assert!(
                re_diff <= 5.0e-3 && im_diff <= 5.0e-3,
                "CPARAM mismatch on row {row_index} sample {sample_index}: casa=({:.6},{:.6}) rust=({:.6},{:.6})",
                casa_gain.re,
                casa_gain.im,
                rust_gain.re,
                rust_gain.im
            );
        }
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn solve_t_phase_gain_with_prior_g_preapply_matches_casa_gaincal_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_t = match common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "T",
            calmode: "p",
            solint: "int",
            combine: "",
            prior_gaintables: vec![prior_g.as_path()],
            parang: false,
        },
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_t = dir.path().join("rust-prior-t.gcal");
    solve_gain_from_path(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_t.clone(),
            gain_type: GainType::T,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Integration,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust T solve with prior preapply");

    let casa_apply_ms = dir.path().join("casa-solved-prior-t.ms");
    let rust_apply_ms = dir.path().join("rust-solved-prior-t.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[prior_g.as_path(), casa_t.as_path()],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior+T chain");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[prior_g.as_path(), rust_t.as_path()],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust prior+T chain");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bandpass_with_prior_gain_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_b = match common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
        "",
        false,
        false,
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_b = dir.path().join("rust-bandpass.bcal");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bandpass_with_combine_field_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_b = match common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0,1",
        "0",
        "VA15",
        &[&prior_g],
        "field",
        false,
        false,
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_b = dir.path().join("rust-bandpass-combine-field.bcal");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bandpass_with_combine_scan_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_b = match common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "1",
        "0",
        "VA15",
        &[&prior_g],
        "scan",
        false,
        false,
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_b = dir.path().join("rust-bandpass-combine-scan.bcal");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[1]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bandpass_with_combine_scan_and_field_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_b = match common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0,1",
        "0",
        "VA15",
        &[&prior_g],
        "scan,field",
        false,
        false,
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_b = dir.path().join("rust-bandpass-combine-scan-field.bcal");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: rust_b.clone(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: true,
                fields: true,
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: false,
            amplitude_degree: 3,
            phase_degree: 3,
            smodel: [1.0, 0.0, 0.0, 0.0],
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bandpass_with_prior_gain_and_parang_matches_casa_bandpass_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_b = match common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
        "",
        false,
        true,
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_b = dir.path().join("rust-bandpass-parang.bcal");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bandpass_with_solnorm_matches_casa_bandpass_downstream_via_casa_applycal() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_b = match common::run_casa_bandpass(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
        "",
        true,
        false,
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_b = dir.path().join("rust-bandpass-solnorm.bcal");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
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

#[cfg(feature = "slow-tests")]
#[test]
fn solve_bpoly_bandpass_with_prior_gain_matches_casa_bandpass_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = match common::ngc5921_ms_path() {
        Some(path) => path,
        None => {
            eprintln!("{}", common::casa_skip_reason());
            return;
        }
    };
    let prior_g = match common::run_casa_gaincal(
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
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let casa_bpoly = match common::run_casa_bandpass_bpoly(
        dir.path(),
        &source_ms,
        "0",
        "0",
        "VA15",
        &[&prior_g],
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let rust_bpoly = dir.path().join("rust-bandpass.bpoly");
    solve_bandpass_from_path(
        &source_ms,
        &BandpassSolveRequest {
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

fn assert_apply_state_close(
    left_path: &std::path::Path,
    right_path: &std::path::Path,
    selection: &MsSelection,
) {
    assert_apply_state_close_with_complex_tolerances(left_path, right_path, selection, 5.0e-4, 0.0);
}

fn assert_apply_state_close_with_tolerance(
    left_path: &std::path::Path,
    right_path: &std::path::Path,
    selection: &MsSelection,
    complex_tolerance: f32,
) {
    assert_apply_state_close_with_complex_tolerances(
        left_path,
        right_path,
        selection,
        complex_tolerance,
        0.0,
    );
}

fn assert_apply_state_close_with_complex_tolerances(
    left_path: &std::path::Path,
    right_path: &std::path::Path,
    selection: &MsSelection,
    complex_abs_tolerance: f32,
    complex_rel_tolerance: f32,
) {
    let left = MeasurementSet::open(left_path).expect("open left MeasurementSet");
    let right = MeasurementSet::open(right_path).expect("open right MeasurementSet");

    let left_rows = selection.apply(&left).expect("select left rows");
    let right_rows = selection.apply(&right).expect("select right rows");
    assert_eq!(left_rows, right_rows, "selected row mismatch");
    assert!(!left_rows.is_empty(), "selected rows should not be empty");

    let left_corrected = left
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("left CORRECTED_DATA");
    let right_corrected = right
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("right CORRECTED_DATA");

    for row in left_rows {
        let left_data = left_corrected.get(row).expect("left corrected row");
        let right_data = right_corrected.get(row).expect("right corrected row");
        assert_complex_cells_close(
            row,
            left_data,
            right_data,
            complex_abs_tolerance,
            complex_rel_tolerance,
        );

        assert_eq!(
            left.flag_row_column().get(row).expect("left flag_row"),
            right.flag_row_column().get(row).expect("right flag_row"),
            "FLAG_ROW mismatch on row {row}"
        );

        let left_flags = flag_matrix_for_row(&left, row);
        let right_flags = flag_matrix_for_row(&right, row);
        assert_eq!(left_flags, right_flags, "FLAG mismatch on row {row}");

        let left_weight = left
            .main_table()
            .get_array_cell(row, "WEIGHT")
            .expect("left WEIGHT");
        let right_weight = right
            .main_table()
            .get_array_cell(row, "WEIGHT")
            .expect("right WEIGHT");
        assert_float_cells_close(row, "WEIGHT", left_weight, right_weight, 5.0e-4);

        let left_has_weight_spectrum = left
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        let right_has_weight_spectrum = right
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        assert_eq!(
            left_has_weight_spectrum, right_has_weight_spectrum,
            "WEIGHT_SPECTRUM presence mismatch on row {row}"
        );
        if left_has_weight_spectrum {
            let left_weight_spectrum = left
                .main_table()
                .get_array_cell(row, "WEIGHT_SPECTRUM")
                .expect("left WEIGHT_SPECTRUM");
            let right_weight_spectrum = right
                .main_table()
                .get_array_cell(row, "WEIGHT_SPECTRUM")
                .expect("right WEIGHT_SPECTRUM");
            assert_float_cells_close(
                row,
                "WEIGHT_SPECTRUM",
                left_weight_spectrum,
                right_weight_spectrum,
                5.0e-4,
            );
        }
    }
}

fn assert_complex_cells_close(
    row: usize,
    left: &ArrayValue,
    right: &ArrayValue,
    abs_tolerance: f32,
    rel_tolerance: f32,
) {
    let ArrayValue::Complex32(left) = left else {
        panic!("left CORRECTED_DATA row {row} was not Complex32");
    };
    let ArrayValue::Complex32(right) = right else {
        panic!("right CORRECTED_DATA row {row} was not Complex32");
    };

    assert_eq!(
        left.shape(),
        right.shape(),
        "CORRECTED_DATA shape mismatch on row {row}"
    );

    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        assert_complex_close(
            row,
            index,
            *left_value,
            *right_value,
            abs_tolerance,
            rel_tolerance,
        );
    }
}

fn assert_complex_close(
    row: usize,
    index: usize,
    left: Complex32,
    right: Complex32,
    abs_tolerance: f32,
    rel_tolerance: f32,
) {
    let re_diff = (left.re - right.re).abs();
    let im_diff = (left.im - right.im).abs();
    let re_limit = abs_tolerance.max(rel_tolerance * left.re.abs().max(right.re.abs()));
    let im_limit = abs_tolerance.max(rel_tolerance * left.im.abs().max(right.im.abs()));
    assert!(
        re_diff <= re_limit && im_diff <= im_limit,
        "CORRECTED_DATA mismatch on row {row} sample {index}: left=({:.6},{:.6}) right=({:.6},{:.6}) abs_tolerance={abs_tolerance} rel_tolerance={rel_tolerance}",
        left.re,
        left.im,
        right.re,
        right.im
    );
}

fn assert_float_cells_close(
    row: usize,
    column: &str,
    left: &ArrayValue,
    right: &ArrayValue,
    tolerance: f32,
) {
    let ArrayValue::Float32(left) = left else {
        panic!("left {column} row {row} was not Float32");
    };
    let ArrayValue::Float32(right) = right else {
        panic!("right {column} row {row} was not Float32");
    };

    assert_eq!(
        left.shape(),
        right.shape(),
        "{column} shape mismatch on row {row}"
    );

    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        let diff = (left_value - right_value).abs();
        assert!(
            diff <= tolerance,
            "{column} mismatch on row {row} sample {index}: left={left_value:.6} right={right_value:.6} tolerance={tolerance}"
        );
    }
}

fn flag_matrix_for_row(ms: &MeasurementSet, row: usize) -> ndarray::Array2<bool> {
    match ms.flag_column().get(row).expect("flag cell") {
        ArrayValue::Bool(values) => values
            .view()
            .into_dimensionality::<Ix2>()
            .expect("2d flag cell")
            .to_owned(),
        other => panic!("unexpected FLAG cell type {:?}", other.primitive_type()),
    }
}

fn read_cparam_rows_for_field(table_path: &std::path::Path, field_id: i32) -> Vec<Vec<Complex32>> {
    let table = Table::open(TableOptions::new(table_path)).expect("open caltable");
    let mut rows = Vec::new();
    for row in 0..table.row_count() {
        let current_field_id = match table.cell(row, "FIELD_ID").expect("FIELD_ID cell") {
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Int32(value))) => *value,
            other => panic!("unexpected FIELD_ID value: {other:?}"),
        };
        if current_field_id != field_id {
            continue;
        }
        let gains = match table.get_array_cell(row, "CPARAM").expect("CPARAM cell") {
            ArrayValue::Complex32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected CPARAM value: {other:?}"),
        };
        rows.push(gains);
    }
    rows
}

fn read_field_direction(table_path: &std::path::Path, field_id: usize) -> (f64, f64) {
    let table = Table::open(TableOptions::new(table_path.join("FIELD"))).expect("open FIELD table");
    let phase_dir = match table
        .get_array_cell(field_id, "PHASE_DIR")
        .expect("PHASE_DIR cell")
    {
        ArrayValue::Float64(values) => values,
        other => panic!("unexpected PHASE_DIR value: {other:?}"),
    };
    (phase_dir[[0, 0]], phase_dir[[1, 0]])
}

fn find_casa_fluxscale_field<'a>(
    report: &'a serde_json::Value,
    name_fragment: &str,
) -> &'a serde_json::Value {
    let object = report.as_object().expect("CASA fluxscale object");
    object
        .values()
        .find(|value| {
            value
                .get("fieldName")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| name.contains(name_fragment))
        })
        .unwrap_or_else(|| panic!("missing CASA fluxscale field containing {name_fragment}"))
}

fn casa_fit_fluxd(field: &serde_json::Value) -> f64 {
    match field.get("fitFluxd").expect("fitFluxd") {
        serde_json::Value::Array(values) => values
            .first()
            .and_then(serde_json::Value::as_f64)
            .expect("fitFluxd[0]"),
        serde_json::Value::Number(value) => value.as_f64().expect("fitFluxd number"),
        other => panic!("unexpected fitFluxd value: {other:?}"),
    }
}

fn casa_fit_ref_frequency_hz(field: &serde_json::Value) -> f64 {
    field
        .get("fitRefFreq")
        .and_then(serde_json::Value::as_f64)
        .expect("fitRefFreq")
}
