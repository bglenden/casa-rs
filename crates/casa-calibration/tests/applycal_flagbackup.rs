// SPDX-License-Identifier: LGPL-3.0-or-later

//! `applycal(flagbackup=True)`: a `calflag` run saves the flags to the next
//! `applycal_N` version first, as CASA's task does, so `flagmanager` can undo
//! it. Modes that change no flags, and requests that are rejected, save none.

mod common;

use std::path::{Path, PathBuf};

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, CalibrationTaskRequest, CalibrationTaskResult,
    ExecuteApplyTaskRequest,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_ms::{FlagMerge, list_flag_versions, restore_flag_version};
use casa_types::ArrayValue;
use tempfile::TempDir;

/// An MS whose row 0 has no solution for antenna 1, so `calflag` flags it.
fn fixture(dir: &Path) -> (PathBuf, PathBuf) {
    let ms_path = common::create_apply_fixture_ms(dir, true);
    let caltable_path = common::create_apply_gain_caltable(
        &dir.join("phase.gcal"),
        &["TARGET0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 100.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![
                casa_types::Complex32::new(2.0, 0.0),
                casa_types::Complex32::new(4.0, 0.0),
            ],
            flags: vec![false, false],
        }],
    );
    (ms_path, caltable_path)
}

fn applycal(
    ms_path: &Path,
    caltable_path: &Path,
    apply_mode: ApplyMode,
    flagbackup: bool,
) -> Result<CalibrationTaskResult, String> {
    CalibrationTaskRequest::ExecuteApply(ExecuteApplyTaskRequest {
        measurement_set: ms_path.to_path_buf(),
        selection: MsSelection::new().spw(&[0]),
        calibration_tables: vec![ApplyCalibrationTableSpec::new(caltable_path)],
        apply_mode,
        parang: false,
        flagbackup,
    })
    .execute()
}

fn backup_version(result: &CalibrationTaskResult) -> Option<&str> {
    let CalibrationTaskResult::Apply(report) = result else {
        panic!("expected an apply report");
    };
    report.flag_backup_version.as_deref()
}

/// Saved flag versions, without `main` (the working copy CASA always lists).
fn version_names(ms_path: &Path) -> Vec<String> {
    let ms = MeasurementSet::open(ms_path).expect("open measurement set");
    list_flag_versions(&ms)
        .expect("list flag versions")
        .into_iter()
        .map(|entry| entry.name)
        .filter(|name| name != "main")
        .collect()
}

fn row0_flags(ms: &MeasurementSet) -> Vec<bool> {
    let column = ms.flag_column();
    let ArrayValue::Bool(flags) = column.get(0).expect("read flags") else {
        panic!("expected bool flags");
    };
    flags.iter().copied().collect()
}

#[test]
fn calflag_saves_the_prior_flags_to_the_next_applycal_version() {
    let dir = TempDir::new().expect("tempdir");
    let (ms_path, caltable_path) = fixture(dir.path());
    let before = row0_flags(&MeasurementSet::open(&ms_path).expect("open"));
    assert!(before.iter().all(|flag| !flag));

    let first = applycal(&ms_path, &caltable_path, ApplyMode::CalFlag, true).expect("applycal");
    assert_eq!(backup_version(&first), Some("applycal_1"));
    let second = applycal(&ms_path, &caltable_path, ApplyMode::CalFlag, true).expect("applycal");
    assert_eq!(backup_version(&second), Some("applycal_2"));
    assert_eq!(version_names(&ms_path), ["applycal_1", "applycal_2"]);

    let mut ms = MeasurementSet::open(&ms_path).expect("reopen");
    assert!(row0_flags(&ms).iter().all(|flag| *flag));
    restore_flag_version(&mut ms, "applycal_1", FlagMerge::Replace).expect("restore");
    assert_eq!(row0_flags(&ms), before);
}

#[test]
fn modes_that_change_no_flags_save_no_backup() {
    let dir = TempDir::new().expect("tempdir");
    let (ms_path, caltable_path) = fixture(dir.path());
    for mode in [ApplyMode::CalOnly, ApplyMode::Trial] {
        let result = applycal(&ms_path, &caltable_path, mode, true).expect("applycal");
        assert_eq!(backup_version(&result), None, "{mode:?}");
    }
    assert!(version_names(&ms_path).is_empty());
}

#[test]
fn flagbackup_false_saves_no_backup() {
    let dir = TempDir::new().expect("tempdir");
    let (ms_path, caltable_path) = fixture(dir.path());
    let result = applycal(&ms_path, &caltable_path, ApplyMode::CalFlag, false).expect("applycal");
    assert_eq!(backup_version(&result), None);
    assert!(version_names(&ms_path).is_empty());
}

#[test]
fn a_rejected_request_saves_no_backup_and_changes_no_flags() {
    let dir = TempDir::new().expect("tempdir");
    let (ms_path, _) = fixture(dir.path());
    let before = row0_flags(&MeasurementSet::open(&ms_path).expect("open"));
    let missing = dir.path().join("missing.gcal");
    applycal(&ms_path, &missing, ApplyMode::CalFlag, true).expect_err("missing table");
    assert!(version_names(&ms_path).is_empty());
    assert_eq!(
        row0_flags(&MeasurementSet::open(&ms_path).expect("reopen")),
        before
    );
}
