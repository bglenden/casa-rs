// SPDX-License-Identifier: LGPL-3.0-or-later

//! The calibration tasks' guards (#931): CASA's parameters are the consent.
//! Product publication follows each task's CASA lifecycle;
//! a refused report changes nothing; `applycal` changes only the rows its
//! selection names; and `calonly` and `trial` change no flags.

#[path = "../../../crates/casa-calibration/tests/common/mod.rs"]
mod common;
#[path = "../../../libs/casacore/casa-ms/tests/common/mod.rs"]
mod ms_common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, CalibrationDataset, CalibrationSolveRequest,
    ExportCorrectedDataRequest, FluxScaleRequest, GainSolveCombine, GainSolveInterval,
    GainSolveMode, GainSolveModelSource, GainSolveRequest, GainType, RefAntSelector,
    export_corrected_data, fluxscale, solve_calibration,
};
use casa_core::Complex32;
use casa_ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_tasks::calibration::{CalibrationTaskRequest, ExecuteApplyTaskRequest};
use tempfile::TempDir;

/// Every file under `path`, by relative path, with its bytes.
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                walk(root, &entry.unwrap().path(), files);
            }
        } else {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            files.insert(relative, std::fs::read(path).unwrap());
        }
    }
    let mut files = BTreeMap::new();
    walk(path, path, &mut files);
    files
}

fn gain_request(output_table: PathBuf) -> GainSolveRequest {
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
        preaverage_seconds: 0.0,
        correlation_dependent_flags: false,
        normalization_type: Default::default(),
        robustness: Default::default(),
        min_snr: 0.0,
        min_baselines_per_antenna: 0,
        smodel: [1.0, 0.0, 0.0, 0.0],
    }
}

/// A gain table with a reference and a transfer field, for fluxscale.
fn two_field_gain_table(path: &Path) -> PathBuf {
    let row = |time_seconds, field_id, gain| common::SyntheticGainSolutionRow {
        time_seconds,
        field_id,
        spectral_window_id: 0,
        antenna_id: 0,
        gains: vec![Complex32::new(gain, 0.0); 2],
        flags: vec![false, false],
    };
    common::create_apply_gain_caltable(
        path,
        &["REF0", "TARGET0"],
        &[row(10.0, 0, 1.0), row(20.0, 1, 2.0)],
    )
}

/// A gain table with solutions for antenna 0 only, in spw 0.
fn antenna0_gain_table(path: &Path) -> PathBuf {
    common::create_apply_gain_caltable(
        path,
        &["TARGET0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 100.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![Complex32::new(2.0, 0.0), Complex32::new(4.0, 0.0)],
            flags: vec![false, false],
        }],
    )
}

#[test]
fn gaincal_replaces_and_fluxscale_refuses_an_existing_product() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let table = dir.path().join("solved.gcal");
    let solve = || {
        solve_calibration(
            CalibrationDataset::path(&ms),
            CalibrationSolveRequest::Gain(gain_request(table.clone())),
        )
    };
    solve().expect("first gaincal");
    solve().expect("CASA gaincal replaces by default");

    let input = two_field_gain_table(&dir.path().join("input.gcal"));
    let scaled = dir.path().join("scaled.gcal");
    let scale = || {
        fluxscale(&FluxScaleRequest {
            options: Default::default(),
            append: false,
            input_table: input.clone(),
            output_table: scaled.clone(),
            reference_fields: vec!["REF*".to_string()],
            transfer_fields: Vec::new(),
            refspwmap: Vec::new(),
            gainthreshold: None,
            incremental: false,
        })
    };
    scale().expect("first fluxscale");
    let before = snapshot(&scaled);
    let error = scale().expect_err("second fluxscale refused");
    assert!(error.to_string().contains("fluxtable exists"), "{error}");
    assert_eq!(snapshot(&scaled), before);
}

#[test]
fn the_corrected_export_refuses_an_existing_measurement_set() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_apply_fixture_ms(dir.path(), true);
    let output = dir.path().join("exported.ms");
    let request = ExportCorrectedDataRequest {
        input_ms: ms,
        output_ms: output.clone(),
        selection: MsSelection::new(),
    };
    export_corrected_data(&request).expect("first export");
    let before = snapshot(&output);
    let error = export_corrected_data(&request).expect_err("second export refused");
    assert!(error.to_string().contains("already exists"), "{error}");
    assert_eq!(snapshot(&output), before);
}

#[test]
fn a_refused_report_runs_nothing() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_apply_fixture_ms(dir.path(), false);
    let table = antenna0_gain_table(&dir.path().join("phase.gcal"));
    let report = dir.path().join("report.txt");
    std::fs::write(&report, "an earlier report").unwrap();
    let before = snapshot(&ms);

    let output = Command::new(env!("CARGO_BIN_EXE_calibrate"))
        .arg("applycal")
        .arg("--ms")
        .arg(&ms)
        .arg("--gaintables")
        .arg(&table)
        .arg("-o")
        .arg(&report)
        .arg("--no-save-last")
        .output()
        .unwrap();
    // Refused before the task runs: a validation failure.
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("refusing to overwrite"), "{stderr}");
    assert_eq!(snapshot(&ms), before);
    assert_eq!(
        std::fs::read_to_string(&report).unwrap(),
        "an earlier report"
    );
    let mut versions = ms.clone().into_os_string();
    versions.push(".flagversions");
    assert!(!PathBuf::from(versions).exists());
}

#[test]
fn overwrite_replaces_the_report_and_gaincal_replaces_its_table() {
    // CASA gaincal replaces a table by default; report overwrite is separate (#945).
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let first_table = dir.path().join("first.gcal");
    let report = dir.path().join("report.txt");
    let solve = |table: &Path| {
        Command::new(env!("CARGO_BIN_EXE_calibrate"))
            .arg("gaincal")
            .arg("--ms")
            .arg(&ms)
            .arg("--out")
            .arg(table)
            .args(["--refant", "0", "--minsnr", "0", "--minblperant", "0"])
            .arg("-o")
            .arg(&report)
            .args(["--overwrite", "--no-save-last"])
            .output()
            .unwrap()
    };
    std::fs::write(&report, "an earlier report").unwrap();
    let first = solve(&first_table);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let replaced = std::fs::read_to_string(&report).unwrap();
    assert_ne!(
        replaced, "an earlier report",
        "overwrite replaces the report"
    );

    let before = snapshot(&first_table);
    let second = solve(&first_table);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(snapshot(&first_table), before);
    assert_eq!(std::fs::read_to_string(&report).unwrap(), replaced);
}

fn applycal(ms: &Path, table: &Path, selection: MsSelection, apply_mode: ApplyMode) {
    CalibrationTaskRequest::ExecuteApply(ExecuteApplyTaskRequest {
        callibrary_error: None,
        argument_error: None,
        measurement_set: ms.to_path_buf(),
        selection,
        calibration_tables: vec![ApplyCalibrationTableSpec::new(table)],
        apply_mode,
        parang: false,
        flagbackup: false,
    })
    .execute()
    .expect("applycal");
}

fn rows(ms: &Path) -> Vec<(String, String)> {
    let ms = MeasurementSet::open(ms).unwrap();
    let corrected = ms.data_column(VisibilityDataColumn::CorrectedData).unwrap();
    let flags = ms.flag_column();
    (0..ms.row_count())
        .map(|row| {
            (
                format!("{:?}", corrected.get(row).unwrap()),
                format!("{:?}", flags.get(row).unwrap()),
            )
        })
        .collect()
}

#[test]
fn applycal_changes_only_the_selected_rows() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_apply_fixture_ms(dir.path(), true);
    let table = antenna0_gain_table(&dir.path().join("phase.gcal"));
    let before = rows(&ms);
    // Row 0 is spw 0 and row 1 spw 1; select spw 0 only. Antenna 1 has no
    // solution, so calflag flags row 0.
    applycal(
        &ms,
        &table,
        MsSelection::new().spw(&[0]),
        ApplyMode::CalFlag,
    );
    let after = rows(&ms);
    assert_ne!(after[0], before[0], "the selected row was not calibrated");
    assert_eq!(after[1], before[1], "an unselected row changed");
}

#[test]
fn calonly_and_trial_change_no_flags() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_apply_fixture_ms(dir.path(), true);
    let table = antenna0_gain_table(&dir.path().join("phase.gcal"));
    let flags = |rows: Vec<(String, String)>| rows.into_iter().map(|row| row.1).collect::<Vec<_>>();
    let before = rows(&ms);

    applycal(&ms, &table, MsSelection::new().spw(&[0]), ApplyMode::Trial);
    assert_eq!(rows(&ms), before, "trial changed the MeasurementSet");
    // Antenna 1 has no solution, so calflag would flag row 0; calonly must not.
    applycal(
        &ms,
        &table,
        MsSelection::new().spw(&[0]),
        ApplyMode::CalOnly,
    );
    assert_eq!(flags(rows(&ms)), flags(before.clone()));
    applycal(
        &ms,
        &table,
        MsSelection::new().spw(&[0]),
        ApplyMode::CalFlag,
    );
    assert_ne!(flags(rows(&ms)), flags(before), "calflag flags row 0");
}

fn run(binary: &str, args: &[&str]) -> Output {
    Command::new(binary)
        .args(args)
        .output()
        .expect("run task binary")
}

fn flag_versions(ms: &Path) -> PathBuf {
    let mut name = ms.as_os_str().to_owned();
    name.push(".flagversions");
    PathBuf::from(name)
}

#[test]
fn mstransform_and_split_refuse_an_existing_output_ms() {
    let temp = tempfile::tempdir().unwrap();
    let ms = ms_common::create_msexplore_fixture_ms(temp.path());
    let ms = ms.to_str().unwrap();
    let out = temp.path().join("out.ms");
    let args = [
        "--vis",
        ms,
        "--outputvis",
        out.to_str().unwrap(),
        "--spw",
        "0",
        "--datacolumn",
        "data",
    ];

    let first = run(env!("CARGO_BIN_EXE_mstransform"), &args);
    assert!(first.status.success(), "{first:?}");
    let before = snapshot(&out);

    let second = run(env!("CARGO_BIN_EXE_mstransform"), &args);
    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("already exists - will not overwrite it"),
        "{stderr}"
    );
    assert_eq!(snapshot(&out), before);
}

#[test]
fn transform_refusals_are_validation_at_the_parameter_at_fault() {
    // Astra's #962 F4: the same input and output, and an spw selector the
    // transform could not parse, failed as execution failures, unnamed.
    let temp = tempfile::tempdir().unwrap();
    let ms = ms_common::create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);
    let out = temp.path().join("out.ms");
    let out_arg = out.to_str().unwrap();
    let request = temp.path().join("request.json");
    let binary = env!("CARGO_BIN_EXE_mstransform");

    for surface in ["mstransform", "split"] {
        for (values, parameter) in [
            (
                serde_json::json!({"vis": ms_arg, "outputvis": ms_arg}),
                "outputvis",
            ),
            (
                serde_json::json!({"vis": ms_arg, "outputvis": out_arg, "spw": "0:bogus"}),
                "spw",
            ),
        ] {
            let mut line = vec![surface.to_string(), "--no-save-last".to_string()];
            for (name, value) in values.as_object().unwrap() {
                line.extend([format!("--{name}"), value.as_str().unwrap().to_string()]);
            }
            let refused = Command::new(binary).args(&line).output().unwrap();
            assert_eq!(refused.status.code(), Some(2), "{line:?}");
            let stderr = String::from_utf8_lossy(&refused.stderr);
            assert!(stderr.contains(parameter), "{line:?}: {stderr}");

            let run = serde_json::json!({"surface": surface, "values": values});
            std::fs::write(&request, run.to_string()).unwrap();
            let replied = run_json(binary, &request);
            assert_eq!(replied.status.code(), Some(2), "{run}");
            let reply: serde_json::Value = serde_json::from_slice(&replied.stdout).unwrap();
            assert_eq!(reply["kind"], "validation", "{reply}");
            assert_eq!(reply["parameter"], parameter, "{reply}");

            assert_eq!(snapshot(&ms), before, "{surface} {parameter}");
            assert!(!out.exists(), "{surface} {parameter}");
        }
    }
}

#[test]
fn transform_cli_distinguishes_empty_selectors_from_missing_typed_values() {
    let binary = env!("CARGO_BIN_EXE_mstransform");
    for surface in ["split", "mstransform"] {
        for flag in ["--field", "--spw", "--scan", "--antenna", "--timerange"] {
            let initial = [surface, "--no-save-last", "--show", flag];
            let empty = Command::new(binary).args(initial).arg("").output().unwrap();
            assert!(
                empty.status.success(),
                "{surface} {flag}: {}",
                String::from_utf8_lossy(&empty.stderr)
            );
            let whitespace = Command::new(binary)
                .args(initial)
                .arg("   ")
                .output()
                .unwrap();
            assert_eq!(whitespace.status.code(), Some(2), "{surface} {flag}");
            assert!(String::from_utf8_lossy(&whitespace.stderr).contains("has no typed value"));
        }
    }
}

fn run_json(binary: &str, request: &Path) -> Output {
    run(binary, &["--json-run", request.to_str().unwrap()])
}

#[test]
fn refused_arguments_change_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let ms = ms_common::create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);
    let out = temp.path().join("out.ms");

    let binary = env!("CARGO_BIN_EXE_mstransform");
    let args = [
        "--vis",
        ms_arg,
        "--outputvis",
        out.to_str().unwrap(),
        "--spw",
        "9",
    ];
    let output = run(binary, &args);
    assert!(!output.status.success(), "{binary} {args:?} succeeded");
    assert_eq!(snapshot(&ms), before, "{binary} {args:?}");
    assert!(!flag_versions(&ms).exists());
    assert!(!out.exists());
}
