// SPDX-License-Identifier: LGPL-3.0-or-later

//! The calibration tasks' guards (#931): CASA's parameters are the consent.
//! A table or MeasurementSet a second run would replace is refused and kept;
//! a refused report changes nothing; `applycal` changes only the rows its
//! selection names; and `calonly` and `trial` change no flags.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, CalibrationDataset, CalibrationSolveRequest,
    CalibrationTaskRequest, ExecuteApplyTaskRequest, ExportCorrectedDataRequest, FluxScaleRequest,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainType, RefAntSelector, export_corrected_data, fluxscale, solve_calibration,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_types::Complex32;
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
fn solved_and_scaled_tables_are_refused_and_kept() {
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
    let before = snapshot(&table);
    let error = solve().err().expect("second gaincal refused");
    assert!(
        error.to_string().contains("output already exists"),
        "{error}"
    );
    assert_eq!(snapshot(&table), before);

    let input = two_field_gain_table(&dir.path().join("input.gcal"));
    let scaled = dir.path().join("scaled.gcal");
    let scale = || {
        fluxscale(&FluxScaleRequest {
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
    let error = scale().err().expect("second fluxscale refused");
    assert!(error.to_string().contains("fluxtable exists"), "{error}");
    assert_eq!(snapshot(&scaled), before);
}

#[test]
fn the_corrected_export_refuses_an_existing_measurement_set() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_apply_fixture_ms(dir.path(), true);
    let output = dir.path().join("exported.ms");
    let export = || {
        export_corrected_data(&ExportCorrectedDataRequest {
            input_ms: ms.clone(),
            output_ms: output.clone(),
            selection: MsSelection::new(),
        })
    };
    export().expect("first export");
    let before = snapshot(&output);
    let error = export().err().expect("second export refused");
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
        .arg("apply")
        .arg(&ms)
        .arg(&table)
        .arg("-o")
        .arg(&report)
        .output()
        .unwrap();
    assert!(!output.status.success());
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

fn applycal(ms: &Path, table: &Path, selection: MsSelection, apply_mode: ApplyMode) {
    CalibrationTaskRequest::ExecuteApply(ExecuteApplyTaskRequest {
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
