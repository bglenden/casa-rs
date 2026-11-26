// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_core::{ArrayValue, Complex32, PrimitiveType, ScalarValue, Value};
use casa_ms::selection::MsSelection;
use casa_ms::{
    FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataRequest, FlagMerge, FlaggingError,
    MeasurementSet, QuackMode, SubtableId, append_flag_commands, delete_flag_version, flagdata,
    flagdata_ms_program, flagdata_path, list_flag_versions, rename_flag_version,
    restore_flag_version, save_flag_version,
};
use casa_tables::{ColumnSchema, Table, TableInfo, TableOptions, TableSchema};
use ndarray::{ArrayD, IxDyn};
use std::process::Command;
use tempfile::tempdir;

use common::{
    NUM_CHAN, NUM_CORR, create_msexplore_averaging_fixture_ms, create_msexplore_fixture_in_memory,
    create_msexplore_fixture_ms, create_two_polarization_fixture_ms,
};

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
fn typed_default_clip_leaves_stored_i_clear() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_two_polarization_fixture_ms(temp.path(), [1, 2, 3, 4], &[]);
    seed_constant_data_with_outliers(&ms_path, &[(0, 0, 0, 0.0), (2, 0, 0, 0.0)]);

    flagdata_path(
        &ms_path,
        &FlagDataRequest {
            mode: FlagDataMode::Clip,
            flagbackup: false,
            clipzeros: true,
            ..FlagDataRequest::default()
        },
    )
    .expect("run typed default clip");

    let ms = MeasurementSet::open(&ms_path).expect("reopen MS");
    assert_flag(&ms, 0, 0, 0);
    assert_not_flag(&ms, 2, 0, 0);
    assert!(!flag_row(&ms, 2));
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
    // The thresholds used: each given one times its default scale of 5.
    assert_eq!(report.timedev, Some(5.0));
    assert_eq!(report.freqdev, Some(50.0));
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
fn flagmanager_can_save_an_empty_calibration_table_without_flag_row() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("empty.cal");
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "FLAG",
        PrimitiveType::Bool,
        Some(2),
    )])
    .expect("flag schema");
    let mut table = Table::with_schema(schema.clone());
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        ..TableInfo::default()
    });
    table
        .save(TableOptions::new(&path))
        .expect("save calibration table");
    let ms = MeasurementSet::open(path).expect("open calibration table");

    casa_ms::flagging::save_disk_flag_version(&ms, "empty", "no rows", FlagMerge::Replace)
        .expect("save empty flag version");
    let version = Table::open(TableOptions::new(
        temp.path().join("empty.cal.flagversions/flags.empty"),
    ))
    .expect("read empty flag version");
    assert_eq!(version.row_count(), 0);
    assert_eq!(version.schema(), Some(&schema));
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
    // Row 1 is exactly quackinterval from both ends of scan 7, and CASA's
    // quack agent flags it in both modes (Astra's #997 F7).
    set_scan_and_time(
        &ms_path,
        &[(0, 7, 0.0), (1, 7, 3.0), (2, 7, 6.0), (3, 8, 0.0)],
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

    assert_eq!(end_report.changed_rows, 3);
    assert_not_flag(&ms, 0, 0, 0);
    assert_flag(&ms, 1, 0, 0);
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
fn extend_with_an_undefined_late_flag_cell_leaves_an_open_ms_unchanged() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    set_flag_on_open_ms(&mut ms, 0, 0, 2, true);
    assert_not_flag(&ms, 0, 1, 2);
    let row = ms.row_count();
    let mut last = ms
        .main_table()
        .row_accessor()
        .row(row - 1)
        .expect("last fixture row")
        .clone();
    last.remove("FLAG");
    ms.main_table_mut()
        .add_row(last)
        .expect("add incomplete row");

    let failure = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Extend,
            extendpols: true,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect_err("undefined FLAG must fail before applying growth");
    assert!(
        failure
            .to_string()
            .contains(&format!("FLAG row {row} is undefined"))
    );
    assert_flag(&ms, 0, 0, 2);
    assert_not_flag(&ms, 0, 1, 2);
    assert!(!flag_row(&ms, 0));
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
fn flagdata_keeps_both_baseline_pairs_from_the_rust_builder() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_averaging_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    // The fixture has (0, 2) on rows 4 and 7. Give row 7 a second baseline.
    ms.main_table_mut()
        .cell_accessor_mut(7, "ANTENNA1")
        .expect("open ANTENNA1")
        .set(Value::Scalar(ScalarValue::Int32(1)))
        .expect("set ANTENNA1");
    ms.save().expect("save fixture");
    let report = flagdata(
        &mut ms,
        &FlagDataRequest {
            selection: MsSelection::new().baseline(&[(0, 2), (1, 2)]),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect("flag the two Rust baseline pairs");
    assert_eq!(report.selected_rows, 2);
    for row in 0..ms.row_count() {
        if [4, 7].contains(&row) {
            assert_flag(&ms, row, 0, 0);
        } else {
            assert_not_flag(&ms, row, 0, 0);
        }
    }
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

#[test]
fn flagdata_ms_program_stages_skips_and_persists() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());

    let requests = vec![
        FlagDataRequest {
            mode: FlagDataMode::Manual,
            casa_row_selection: true,
            selection: MsSelection {
                spw: Some("0:0".to_owned()),
                ..MsSelection::default()
            },
            spw: Some("0:0".to_owned()),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
        FlagDataRequest {
            mode: FlagDataMode::Clip,
            casa_row_selection: true,
            data_column: FlagDataColumn::CorrectedData,
            clipminmax: Some([0.0, 1.0]),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
        FlagDataRequest {
            mode: FlagDataMode::Summary,
            casa_row_selection: true,
            flagbackup: false,
            ..FlagDataRequest::default()
        },
        FlagDataRequest {
            mode: FlagDataMode::Manual,
            casa_row_selection: true,
            selection: MsSelection {
                field: Some("NO_SUCH_FIELD".to_owned()),
                ..MsSelection::default()
            },
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    ];

    // Preview run sees staged manual flags in summary while leaving disk untouched.
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    let calc_reports = flagdata_ms_program(ms, &requests, false, true, None).expect("calculate");
    assert!(calc_reports[0].is_some());
    assert!(
        calc_reports[1].is_none(),
        "missing CORRECTED_DATA skips clip"
    );
    let summary = calc_reports[2]
        .as_ref()
        .and_then(|r| r.summary.as_ref())
        .expect("summary report");
    assert_eq!(summary.overall.flagged as usize, 4 * NUM_CORR);
    assert!(
        calc_reports[3].is_none(),
        "local selection error skips in multi-command"
    );
    let reopened = MeasurementSet::open(&ms_path).expect("reopen MS");
    assert_not_flag(&reopened, 0, 0, 0);

    // Single-command selection failure (local_selection = false) propagates.
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    assert!(flagdata_ms_program(ms, &requests[3..4], false, false, None).is_err());

    // Apply writes combined flags once.
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    let apply_reports = flagdata_ms_program(ms, &requests, true, true, None).expect("apply");
    assert!(apply_reports[2].is_some());
    let persisted = MeasurementSet::open(&ms_path).expect("reopen MS");
    assert_flag(&persisted, 0, 0, 0);
}

#[test]
fn typed_list_tfcrop_keeps_its_input_time_selection() {
    let request = FlagDataRequest {
        mode: FlagDataMode::Tfcrop,
        clip_time_bin: Some("20s".into()),
        selection: MsSelection {
            timerange: Some("00:00:0.5~00:00:1.5".into()),
            ..MsSelection::default()
        },
        ..FlagDataRequest::default()
    };
    let reports = flagdata_ms_program(
        create_msexplore_fixture_in_memory(),
        &[request],
        false,
        true,
        None,
    )
    .expect("evaluate typed TFCrop");
    assert_eq!(reports[0].as_ref().expect("TFCrop report").selected_rows, 1);
}

#[test]
fn averaged_list_clip_reports_only_members_of_selected_bins() {
    let mut ms = create_msexplore_fixture_in_memory();
    // Place the first two integrations in the same averaging chunk.
    ms.main_table_mut()
        .cell_accessor_mut(1, "SCAN_NUMBER")
        .expect("second scan cell")
        .set(Value::Scalar(ScalarValue::Int32(1)))
        .expect("share the first scan");
    let reports = flagdata_ms_program(
        ms,
        &["00:00:0.3~00:00:0.7", "00:00:9~00:00:10"].map(|timerange| FlagDataRequest {
            mode: FlagDataMode::Clip,
            casa_row_selection: true,
            clip_time_bin: Some("2s".into()),
            clipminmax: Some([0.0, 0.0]),
            selection: MsSelection {
                timerange: Some(timerange.into()),
                ..MsSelection::default()
            },
            ..FlagDataRequest::default()
        }),
        false,
        true,
        None,
    )
    .expect("evaluate averaged Clip agents");
    let first = reports[0].as_ref().expect("selected Clip report");
    assert_eq!(first.selected_rows, 2);
    assert_eq!(first.changed_samples, 2 * NUM_CORR * NUM_CHAN);
    assert_eq!(first.flagged_samples, first.changed_samples);
    assert_eq!(first.total_samples, first.changed_samples);
    let empty = reports[1].as_ref().expect("empty Clip report");
    assert_eq!(empty.selected_rows, 0);
    assert_eq!(empty.flagged_samples, 0);
    assert_eq!(empty.total_samples, 0);
}

#[test]
fn antint_list_prepass_honors_its_input_and_agent_selection_policy() {
    let temp = tempdir().expect("tempdir");
    let path = create_msexplore_fixture_ms(temp.path());
    let make_input = |in_memory| {
        let mut ms = if in_memory {
            create_msexplore_fixture_in_memory()
        } else {
            MeasurementSet::open(&path).expect("open MS")
        };
        set_flag_on_open_ms(&mut ms, 0, 0, 0, true);
        ms
    };
    let manual = FlagDataRequest {
        mode: FlagDataMode::Manual,
        casa_row_selection: true,
        selection: MsSelection {
            scan: Some("2".into()),
            spw: Some("0:0".into()),
            ..MsSelection::default()
        },
        spw: Some("0:0".into()),
        flagbackup: false,
        ..FlagDataRequest::default()
    };
    let antint = FlagDataRequest {
        mode: FlagDataMode::Antint,
        casa_row_selection: true,
        antint: casa_ms::flagging::AntennaIntegrationOptions {
            reference_antenna: "ANT0".into(),
            min_channel_fraction: 0.0,
            verbose: false,
        },
        flagbackup: false,
        ..FlagDataRequest::default()
    };
    let summary = FlagDataRequest {
        mode: FlagDataMode::Summary,
        casa_row_selection: true,
        ..FlagDataRequest::default()
    };
    let samples = NUM_CORR * NUM_CHAN;
    for in_memory in [false, true] {
        let reports = flagdata_ms_program(
            make_input(in_memory),
            &[manual.clone(), antint.clone(), summary.clone()],
            false,
            true,
            None,
        )
        .expect("CASA-style antint calculate");
        let flagged = reports[2]
            .as_ref()
            .and_then(|report| report.summary.as_ref())
            .expect("summary")
            .overall
            .flagged;
        assert_eq!(flagged as usize, samples + NUM_CORR);

        let reports = flagdata_ms_program(
            make_input(in_memory),
            &[
                manual.clone(),
                FlagDataRequest {
                    casa_row_selection: false,
                    ..antint.clone()
                },
                antint.clone(),
            ],
            false,
            true,
            None,
        )
        .expect("mixed typed and CASA antint calculate");
        assert_eq!(
            reports[1].as_ref().expect("typed antint").changed_samples,
            2 * samples - NUM_CORR - 1
        );
        assert_eq!(reports[2].as_ref().expect("CASA antint").changed_samples, 0);
    }
    let persisted = MeasurementSet::open(&path).expect("open after previews");
    assert_not_flag(&persisted, 0, 0, 0);
}

#[test]
fn append_flag_commands_writes_batches_and_handles_empty() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());

    append_flag_commands(&ms_path, &[], false).expect("empty batch succeeds");
    append_flag_commands(
        &ms_path,
        &[
            ("mode='manual' spw='0:0'".to_owned(), "r1".to_owned()),
            ("mode='unflag'".to_owned(), "r2".to_owned()),
        ],
        true,
    )
    .expect("batch append");

    let table = casa_tables::Table::open(casa_tables::TableOptions::new(
        ms_path.join(SubtableId::FlagCmd.name()),
    ))
    .expect("open FLAG_CMD");
    assert_eq!(table.row_count(), 2);
    let cell = |row, col| {
        table
            .cell_accessor(row, col)
            .expect(col)
            .value()
            .expect("value")
            .expect("defined")
            .clone()
    };
    assert_eq!(
        cell(0, "COMMAND"),
        Value::Scalar(ScalarValue::String("mode='manual' spw='0:0'".to_owned()))
    );
    assert_eq!(
        cell(0, "REASON"),
        Value::Scalar(ScalarValue::String("r1".to_owned()))
    );
    assert_eq!(cell(0, "APPLIED"), Value::Scalar(ScalarValue::Bool(true)));
    assert_eq!(
        cell(1, "COMMAND"),
        Value::Scalar(ScalarValue::String("mode='unflag'".to_owned()))
    );
    assert_eq!(
        cell(1, "REASON"),
        Value::Scalar(ScalarValue::String("r2".to_owned()))
    );
    assert_eq!(cell(1, "APPLIED"), Value::Scalar(ScalarValue::Bool(true)));
}

#[test]
fn rflag_and_tfcrop_missing_column_and_bogus_correlation_extension() {
    let temp = tempdir().expect("tempdir");
    let missing_col_ms = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&missing_col_ms).expect("open MS");
    let err = flagdata(
        &mut ms,
        &FlagDataRequest {
            mode: FlagDataMode::Rflag,
            casa_row_selection: true,
            data_column: FlagDataColumn::CorrectedData,
            correlation: Some("BOGUS".to_owned()),
            flagbackup: false,
            ..FlagDataRequest::default()
        },
    )
    .expect_err("missing CORRECTED_DATA must fail before correlation skip");
    assert!(matches!(err, FlaggingError::MissingRflagColumn), "{err:?}");

    for mode in [FlagDataMode::Tfcrop, FlagDataMode::Rflag] {
        let dir = temp.path().join(format!("{mode:?}"));
        std::fs::create_dir_all(&dir).expect("create subdir");
        let ms_path = create_msexplore_fixture_ms(&dir);
        set_flag(&ms_path, 0, 0, 0, true);

        let no_ext_req = [FlagDataRequest {
            mode,
            casa_row_selection: true,
            correlation: Some("BOGUS".to_owned()),
            extendflags: false,
            extendpols: true,
            flagbackup: false,
            ..FlagDataRequest::default()
        }];
        let reports = flagdata_ms_program(
            MeasurementSet::open(&ms_path).expect("open MS"),
            &no_ext_req,
            true,
            false,
            None,
        )
        .expect("unadmitted correlation without extendflags skips cleanly");
        assert!(reports[0].is_none(), "{mode:?} without extendflags skipped");
        let reopened = MeasurementSet::open(&ms_path).expect("reopen MS");
        assert_not_flag(&reopened, 0, 1, 0);

        let ext_req = [FlagDataRequest {
            extendflags: true,
            ..no_ext_req[0].clone()
        }];
        let reports = flagdata_ms_program(
            MeasurementSet::open(&ms_path).expect("open MS"),
            &ext_req,
            true,
            false,
            None,
        )
        .expect("unadmitted correlation with extendflags still extends");
        assert!(reports[0].is_some(), "{mode:?} extendflags survives");
        let extended = MeasurementSet::open(&ms_path).expect("reopen extended MS");
        assert_flag(&extended, 0, 1, 0);
    }
}
