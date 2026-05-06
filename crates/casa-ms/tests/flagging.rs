// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_ms::{
    FlagDataAction, FlagDataMode, FlagDataRequest, FlagMerge, MeasurementSet, flagdata,
    flagdata_path, list_flag_versions, restore_flag_version, save_flag_version,
};
use casa_types::{ArrayValue, Complex32, Value};
use ndarray::{ArrayD, IxDyn};
use std::process::Command;
use tempfile::tempdir;

use common::{NUM_CHAN, NUM_CORR, create_msexplore_fixture_ms};

#[test]
fn clipzeros_matches_casa_float_epsilon_behavior() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    seed_constant_data_with_outliers(
        &ms_path,
        &[
            (0, 0, 0, 0.0),
            (0, 0, 1, f32::EPSILON),
            (0, 0, 2, f32::EPSILON * 1.25),
        ],
    );

    let report = flagdata_path(
        &ms_path,
        &FlagDataRequest {
            mode: FlagDataMode::Clip,
            flagbackup: false,
            clipzeros: true,
            ..FlagDataRequest::default()
        },
    )
    .expect("run clipzeros");

    assert_eq!(report.changed_samples, 2);
    assert_eq!(report.flagged_samples, 2);

    let ms = MeasurementSet::open(&ms_path).expect("reopen MS");
    assert_flag(&ms, 0, 0, 0);
    assert_flag(&ms, 0, 0, 1);
    assert_not_flag(&ms, 0, 0, 2);
}

#[test]
fn tfcrop_flags_time_and_frequency_outliers() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    seed_constant_data_with_outliers(&ms_path, &[(0, 0, 5, 100.0), (3, 1, 7, 120.0)]);

    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    let report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Tfcrop,
            flagbackup: false,
            timecutoff: 2.0,
            freqcutoff: 2.0,
            ..FlagDataRequest::default()
        },
    )
    .expect("run tfcrop");

    assert!(report.changed_samples >= 2);
    assert_flag(&ms, 0, 0, 5);
    assert_flag(&ms, 3, 1, 7);
}

#[test]
fn rflag_uses_explicit_thresholds_to_flag_time_and_spectral_outliers() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    seed_constant_data_with_outliers(&ms_path, &[(0, 0, 5, 100.0), (3, 1, 7, 120.0)]);

    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    let report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Rflag,
            flagbackup: false,
            timedev: Some(1.0),
            freqdev: Some(10.0),
            ..FlagDataRequest::default()
        },
    )
    .expect("run rflag");

    assert!(report.changed_samples >= 2);
    assert_eq!(report.timedev, Some(1.0));
    assert_eq!(report.freqdev, Some(10.0));
    assert_flag(&ms, 0, 0, 5);
    assert_flag(&ms, 3, 1, 7);
}

#[test]
fn flagmanager_save_and_restore_round_trips_main_flags() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");

    save_flag_version(&ms, "before", "clean flags", FlagMerge::Replace).expect("save version");
    flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Flag,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("manual flag");
    assert!(flag_count(&ms) > 0);

    restore_flag_version(&mut ms, "before", FlagMerge::Replace).expect("restore version");
    assert_eq!(flag_count(&ms), 0);

    let versions = list_flag_versions(&ms).expect("list versions");
    assert!(versions.iter().any(|entry| entry.name == "before"));
}

#[test]
fn flagdata_and_flagmanager_bins_emit_json() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());

    let flagdata_output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
        .args([
            "--vis",
            ms_path.to_str().expect("UTF-8 MS path"),
            "--mode",
            "summary",
            "--no-flagbackup",
        ])
        .output()
        .expect("run flagdata");
    assert!(
        flagdata_output.status.success(),
        "flagdata stderr: {}",
        String::from_utf8_lossy(&flagdata_output.stderr)
    );
    let flagdata_json: serde_json::Value =
        serde_json::from_slice(&flagdata_output.stdout).expect("flagdata JSON");
    assert_eq!(flagdata_json["mode"], "summary");
    assert_eq!(flagdata_json["flagged_samples"], 0);

    let flagmanager_output = Command::new(env!("CARGO_BIN_EXE_flagmanager"))
        .args([
            "--vis",
            ms_path.to_str().expect("UTF-8 MS path"),
            "--mode",
            "list",
        ])
        .output()
        .expect("run flagmanager");
    assert!(
        flagmanager_output.status.success(),
        "flagmanager stderr: {}",
        String::from_utf8_lossy(&flagmanager_output.stderr)
    );
    let flagmanager_json: serde_json::Value =
        serde_json::from_slice(&flagmanager_output.stdout).expect("flagmanager JSON");
    assert_eq!(flagmanager_json[0]["name"], "main");
}

fn seed_constant_data_with_outliers(
    ms_path: &std::path::Path,
    outliers: &[(usize, usize, usize, f32)],
) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for seed");
    for row in 0..ms.row_count() {
        let mut values = ArrayD::from_elem(IxDyn(&[NUM_CORR, NUM_CHAN]), Complex32::new(1.0, 0.0));
        for &(outlier_row, corr, chan, amp) in outliers {
            if outlier_row == row {
                values[IxDyn(&[corr, chan])] = Complex32::new(amp, 0.0);
            }
        }
        ms.main_table_mut()
            .cell_accessor_mut(row, "DATA")
            .expect("DATA cell")
            .set(Value::Array(ArrayValue::Complex32(values)))
            .expect("set DATA");
    }
    ms.save_main_table_only_assuming_valid()
        .expect("save seeded DATA");
}

fn assert_flag(ms: &MeasurementSet, row: usize, corr: usize, chan: usize) {
    let flags = match ms
        .main_table()
        .cell_accessor(row, "FLAG")
        .expect("FLAG cell")
        .value()
        .expect("FLAG value")
        .expect("defined FLAG")
    {
        Value::Array(ArrayValue::Bool(flags)) => flags,
        other => panic!("unexpected FLAG value {other:?}"),
    };
    assert!(flags[IxDyn(&[corr, chan])]);
}

fn assert_not_flag(ms: &MeasurementSet, row: usize, corr: usize, chan: usize) {
    let flags = match ms
        .main_table()
        .cell_accessor(row, "FLAG")
        .expect("FLAG cell")
        .value()
        .expect("FLAG value")
        .expect("defined FLAG")
    {
        Value::Array(ArrayValue::Bool(flags)) => flags,
        other => panic!("unexpected FLAG value {other:?}"),
    };
    assert!(!flags[IxDyn(&[corr, chan])]);
}

fn flag_count(ms: &MeasurementSet) -> usize {
    (0..ms.row_count())
        .map(|row| {
            let flags = match ms
                .main_table()
                .cell_accessor(row, "FLAG")
                .expect("FLAG cell")
                .value()
                .expect("FLAG value")
                .expect("defined FLAG")
            {
                Value::Array(ArrayValue::Bool(flags)) => flags,
                other => panic!("unexpected FLAG value {other:?}"),
            };
            flags.iter().filter(|flag| **flag).count()
        })
        .sum()
}
