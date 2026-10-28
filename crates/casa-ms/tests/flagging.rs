// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_ms::{
    FlagDataAction, FlagDataMode, FlagDataRequest, FlagMerge, MeasurementSet, QuackMode,
    delete_flag_version, flagdata, flagdata_path, list_flag_versions, rename_flag_version,
    restore_flag_version, save_flag_version,
};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
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
fn the_flagdata_binary_applies_explicit_rflag_thresholds() {
    // Astra's #962 P2: on the front end, every explicit timedev or freqdev
    // was refused as not text, on the command line and on --json-run alike.
    let flagdata_bin = env!("CARGO_BIN_EXE_flagdata");
    for route in ["command line", "--json-run"] {
        let temp = tempdir().expect("tempdir");
        let ms_path = create_msexplore_fixture_ms(temp.path());
        seed_constant_data_with_outliers(&ms_path, &[(0, 0, 5, 100.0), (3, 1, 7, 120.0)]);
        let vis = ms_path.to_str().expect("UTF-8 MS path");
        let output = if route == "--json-run" {
            let request = temp.path().join("request.json");
            let values = serde_json::json!({
                "surface": "flagdata",
                "values": {
                    "vis": vis, "mode": "rflag", "flagbackup": false,
                    "timedev": 1.0, "freqdev": 10.0,
                },
            });
            std::fs::write(&request, values.to_string()).expect("write request");
            Command::new(flagdata_bin)
                .args(["--json-run", request.to_str().expect("UTF-8 path")])
                .output()
        } else {
            Command::new(flagdata_bin)
                .args(["--vis", vis, "--mode", "rflag", "--no-flagbackup"])
                .args(["--timedev", "1.0", "--freqdev", "10", "--no-save-last"])
                .output()
        }
        .expect("run flagdata");
        assert!(
            output.status.success(),
            "{route}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let ms = MeasurementSet::open(&ms_path).expect("open MS");
        assert_flag(&ms, 0, 0, 5);
        assert_flag(&ms, 3, 1, 7);
    }
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
fn manual_flagging_respects_spw_channel_selection_and_unflag_action() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");

    let flag_report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Flag,
            spw: Some("0:2~4".to_string()),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("manual flag selected channels");

    assert_eq!(flag_report.changed_samples, ms.row_count() * NUM_CORR * 3);
    assert_flag(&ms, 0, 0, 2);
    assert_flag(&ms, 0, 3, 4);
    assert_not_flag(&ms, 0, 0, 1);
    assert_not_flag(&ms, 0, 0, 5);

    let unflag_report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Unflag,
            spw: Some("0:3".to_string()),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("manual unflag selected channel");

    assert_eq!(unflag_report.changed_samples, ms.row_count() * NUM_CORR);
    assert_flag(&ms, 0, 0, 2);
    assert_not_flag(&ms, 0, 0, 3);
    assert_flag(&ms, 0, 0, 4);
}

#[test]
fn quack_flags_scan_edges_for_beg_and_end_modes() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    set_scan_and_time(
        &ms_path,
        &[(0, 7, 0.0), (1, 7, 2.0), (2, 7, 5.0), (3, 8, 0.0)],
    );
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");

    let beg_report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Quack,
            quackinterval: 3.0,
            quackmode: QuackMode::Beg,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("quack beginning of scan");

    assert_eq!(beg_report.changed_rows, 3);
    assert_flag(&ms, 0, 0, 0);
    assert_flag(&ms, 1, 0, 0);
    assert_not_flag(&ms, 2, 0, 0);
    assert_flag(&ms, 3, 0, 0);

    flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Unflag,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("clear flags");

    let end_report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Quack,
            quackinterval: 3.0,
            quackmode: QuackMode::End,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("quack end of scan");

    assert_eq!(end_report.changed_rows, 2);
    assert_not_flag(&ms, 0, 0, 0);
    assert_flag(&ms, 2, 0, 0);
    assert_flag(&ms, 3, 0, 0);
}

#[test]
fn extend_mode_grows_existing_flags_across_pols_time_and_frequency() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    set_scan_and_time(
        &ms_path,
        &[(0, 3, 0.0), (1, 3, 1.0), (2, 3, 2.0), (3, 3, 3.0)],
    );
    set_flag(&ms_path, 0, 0, 5, true);
    set_flag(&ms_path, 1, 0, 5, true);
    set_flag(&ms_path, 2, 0, 5, true);
    for chan in 0..NUM_CHAN {
        set_flag(&ms_path, 0, 1, chan, true);
    }
    set_flag(&ms_path, 0, 2, 7, true);
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");

    let report = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Extend,
            extendpols: true,
            growtime: 25.0,
            growfreq: 25.0,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("extend existing flags");

    assert!(report.changed_samples > 0);
    assert_flag(&ms, 0, 3, 5);
    assert_flag(&ms, 3, 0, 5);
    assert_flag(&ms, 0, 1, 15);
    assert_flag(&ms, 0, 0, 7);
    assert!(flag_row(&ms, 0));
}

#[test]
fn flagmanager_merge_rename_delete_and_invalid_names_are_reported() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");

    set_flag_on_open_ms(&mut ms, 0, 0, 0, true);
    save_flag_version(&ms, "seed", "seeded flag", FlagMerge::Replace).expect("save seed");
    set_flag_on_open_ms(&mut ms, 0, 0, 1, true);
    save_flag_version(&ms, "seed", "or merge", FlagMerge::Or).expect("merge into seed");

    flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Unflag,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("clear main flags");
    restore_flag_version(&mut ms, "seed", FlagMerge::Replace).expect("restore seed");
    assert_flag(&ms, 0, 0, 0);
    assert_flag(&ms, 0, 0, 1);

    rename_flag_version(&ms, "seed", "renamed", "renamed comment").expect("rename seed");
    let renamed = list_flag_versions(&ms).expect("list renamed");
    assert!(renamed.iter().any(|entry| entry.name == "renamed"));
    assert!(!renamed.iter().any(|entry| entry.name == "seed"));

    delete_flag_version(&ms, "renamed").expect("delete renamed");
    let deleted = list_flag_versions(&ms).expect("list after delete");
    assert!(!deleted.iter().any(|entry| entry.name == "renamed"));
    assert!(save_flag_version(&ms, "main", "bad", FlagMerge::Replace).is_err());
    assert!(save_flag_version(&ms, "bad/name", "bad", FlagMerge::Replace).is_err());
}

/// The environment variable naming the table [`hold_a_table_open`] holds.
const HELD_TABLE: &str = "CASA_MS_TEST_HELD_TABLE";

/// Not a law: the other process of [`a_flag_version_open_elsewhere_is_neither_deleted_nor_renamed`].
/// Opened with [`HELD_TABLE`] set, it holds that table open, as a CASA
/// session would, until its standard input closes.
#[test]
#[ignore = "run by another test as its holding process"]
fn hold_a_table_open() {
    let Some(path) = std::env::var_os(HELD_TABLE) else {
        return;
    };
    let table = casa_tables::Table::open_with_lock(
        casa_tables::TableOptions::new(&path),
        casa_tables::LockOptions::new(casa_tables::LockMode::AutoLocking),
    )
    .expect("open the held table");
    println!("held");
    let mut line = String::new();
    while std::io::stdin()
        .read_line(&mut line)
        .is_ok_and(|read| read > 0)
    {}
    drop(table);
}

/// #695: while another process has a flag version's table open, deleting
/// or renaming that version is refused with an error naming the table, as
/// casacore's `Table::deleteTable` and `Table::rename` refuse an
/// `isMultiUsed` table; once it closes, both go ahead.
#[cfg(unix)]
#[test]
fn a_flag_version_open_elsewhere_is_neither_deleted_nor_renamed() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;

    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    save_flag_version(&ms, "held", "held elsewhere", FlagMerge::Replace).expect("save version");
    let mut table = ms_path.clone().into_os_string();
    table.push(".flagversions/flags.held");
    let mut holder = Command::new(std::env::current_exe().expect("test binary"))
        .args(["hold_a_table_open", "--exact", "--ignored", "--nocapture"])
        .env(HELD_TABLE, &table)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the holding process");
    let mut stdout = BufReader::new(holder.stdout.take().expect("holder output"));
    let mut line = String::new();
    while !line.contains("held") {
        line.clear();
        assert!(
            stdout.read_line(&mut line).expect("read the holder") > 0,
            "the holder exited before holding the table"
        );
    }
    let refused = |result: Result<(), casa_ms::FlaggingError>| match result {
        Err(casa_ms::FlaggingError::FlagVersionInUse {
            version,
            table: named,
            holder,
            ..
        }) => {
            assert_eq!(version, "held");
            assert_eq!(std::path::Path::new(&named), std::path::Path::new(&table));
            assert_eq!(holder, casa_tables::TableHolder::AnotherProcess);
        }
        other => panic!("expected the version to be in use, got {other:?}"),
    };
    refused(delete_flag_version(&ms, "held"));
    refused(rename_flag_version(&ms, "held", "moved", "moved"));
    assert!(
        list_flag_versions(&ms)
            .expect("list versions")
            .iter()
            .any(|entry| entry.name == "held"),
        "the version is untouched"
    );
    drop(holder.stdin.take());
    assert!(holder.wait().expect("the holder ends").success());
    rename_flag_version(&ms, "held", "moved", "moved").expect("rename once closed");
    delete_flag_version(&ms, "moved").expect("delete once closed");
}

/// #753: a flag version whose table a handle in this process has open is
/// neither deleted nor renamed, as casacore's `TableUtil::canDeleteTable`
/// refuses a table still open in this process; once it closes, both go
/// ahead.
#[cfg(unix)]
#[test]
fn a_flag_version_open_in_this_process_is_neither_deleted_nor_renamed() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    save_flag_version(&ms, "held", "held here", FlagMerge::Replace).expect("save version");
    let mut table = ms_path.clone().into_os_string();
    table.push(".flagversions/flags.held");
    let held = casa_tables::Table::open_with_lock(
        casa_tables::TableOptions::new(&table),
        casa_tables::LockOptions::new(casa_tables::LockMode::AutoLocking),
    )
    .expect("open the version's table");
    let refused = |result: Result<(), casa_ms::FlaggingError>| match result {
        Err(casa_ms::FlaggingError::FlagVersionInUse {
            version, holder, ..
        }) => {
            assert_eq!(version, "held");
            assert_eq!(holder, casa_tables::TableHolder::ThisProcess);
        }
        other => panic!("expected the version to be in use, got {other:?}"),
    };
    refused(delete_flag_version(&ms, "held"));
    refused(rename_flag_version(&ms, "held", "moved", "moved"));
    assert_eq!(
        held.row_count(),
        ms.main_table().row_count(),
        "the held table is intact"
    );
    drop(held);
    rename_flag_version(&ms, "held", "moved", "moved").expect("rename once closed");
    delete_flag_version(&ms, "moved").expect("delete once closed");
}

/// Renaming a flag version onto another version's name is refused, leaving
/// both; renaming one to its own name changes only its comment.
#[test]
fn renaming_onto_an_existing_flag_version_is_refused() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    save_flag_version(&ms, "first", "first", FlagMerge::Replace).expect("save first");
    save_flag_version(&ms, "second", "second", FlagMerge::Replace).expect("save second");
    let error = rename_flag_version(&ms, "first", "second", "clash")
        .expect_err("a rename onto an existing version");
    assert!(
        error.to_string().contains("\"second\" already exists"),
        "{error}"
    );
    let comments = |ms: &MeasurementSet| {
        list_flag_versions(ms)
            .expect("list versions")
            .into_iter()
            .map(|entry| (entry.name, entry.comment))
            .filter(|(name, _)| name != "main")
            .collect::<Vec<_>>()
    };
    let both = vec![
        ("first".to_string(), "first".to_string()),
        ("second".to_string(), "second".to_string()),
    ];
    assert_eq!(comments(&ms), both);
    restore_flag_version(&mut ms, "second", FlagMerge::Replace)
        .expect("the second version's table is intact");

    rename_flag_version(&ms, "first", "first", "recommented").expect("rename to itself");
    assert_eq!(
        comments(&ms)[0],
        ("first".to_string(), "recommented".to_string())
    );
}

#[test]
fn flagdata_summary_backups_and_selector_errors_match_expected_boundaries() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");

    let summary = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Summary,
            flagbackup: true,
            ..FlagDataRequest::default()
        },
    )
    .expect("summary with requested backup");
    assert_eq!(summary.mode, FlagDataMode::Summary);
    assert_eq!(summary.selected_rows, ms.row_count());
    assert_eq!(summary.backup_version, None);
    assert_eq!(list_flag_versions(&ms).expect("list versions").len(), 1);

    let manual = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Flag,
            spw: Some("0:0".to_string()),
            flagbackup: true,
            ..FlagDataRequest::default()
        },
    )
    .expect("manual flag with backup");
    assert_eq!(manual.changed_samples, ms.row_count() * NUM_CORR);
    let backup = manual.backup_version.expect("backup version");
    assert_eq!(backup, "flagdata_1");
    assert!(
        list_flag_versions(&ms)
            .expect("list versions after backup")
            .iter()
            .any(|entry| entry.name == backup && entry.comment == "flagdata auto-backup")
    );

    let invalid_selector = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Manual,
            spw: Some("0:99~100".to_string()),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    );
    assert!(invalid_selector.is_err());

    let missing = flagdata_path(
        temp.path().join("missing.ms"),
        &FlagDataRequest {
            mode: FlagDataMode::Summary,
            ..FlagDataRequest::default()
        },
    );
    assert!(missing.is_err());
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
    ms.save_main_table_only().expect("save seeded DATA");
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

fn flag_row(ms: &MeasurementSet, row: usize) -> bool {
    match ms
        .main_table()
        .cell_accessor(row, "FLAG_ROW")
        .expect("FLAG_ROW cell")
        .value()
        .expect("FLAG_ROW value")
        .expect("defined FLAG_ROW")
    {
        Value::Scalar(ScalarValue::Bool(value)) => *value,
        other => panic!("unexpected FLAG_ROW value {other:?}"),
    }
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

fn set_scan_and_time(ms_path: &std::path::Path, rows: &[(usize, i32, f64)]) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for scan/time seed");
    for &(row, scan, offset) in rows {
        ms.main_table_mut()
            .cell_accessor_mut(row, "SCAN_NUMBER")
            .expect("SCAN_NUMBER cell")
            .set(Value::Scalar(ScalarValue::Int32(scan)))
            .expect("set SCAN_NUMBER");
        ms.main_table_mut()
            .cell_accessor_mut(row, "TIME")
            .expect("TIME cell")
            .set(Value::Scalar(ScalarValue::Float64(
                common::TIME_BASE_SECONDS + offset,
            )))
            .expect("set TIME");
    }
    ms.save_main_table_only()
        .expect("save scan/time seeded MAIN");
}

fn set_flag(ms_path: &std::path::Path, row: usize, corr: usize, chan: usize, value: bool) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for flag seed");
    set_flag_on_open_ms(&mut ms, row, corr, chan, value);
    ms.save_main_table_only().expect("save flag seeded MAIN");
}

fn set_flag_on_open_ms(ms: &mut MeasurementSet, row: usize, corr: usize, chan: usize, value: bool) {
    let mut flags = match ms
        .main_table()
        .cell_accessor(row, "FLAG")
        .expect("FLAG cell")
        .value()
        .expect("FLAG value")
        .expect("defined FLAG")
    {
        Value::Array(ArrayValue::Bool(flags)) => flags.clone(),
        other => panic!("unexpected FLAG value {other:?}"),
    };
    flags[IxDyn(&[corr, chan])] = value;
    let flag_row = flags.iter().all(|flag| *flag);
    ms.main_table_mut()
        .cell_accessor_mut(row, "FLAG")
        .expect("FLAG cell")
        .set(Value::Array(ArrayValue::Bool(flags)))
        .expect("set FLAG");
    ms.main_table_mut()
        .cell_accessor_mut(row, "FLAG_ROW")
        .expect("FLAG_ROW cell")
        .set(Value::Scalar(ScalarValue::Bool(flag_row)))
        .expect("set FLAG_ROW");
}
