// SPDX-License-Identifier: LGPL-3.0-or-later

use std::process::Command;

use casa_logging::validate_log_table;
use casa_tables::{Table, TableOptions};
use casa_types::ScalarValue;

mod common;

#[test]
fn mstransform_help_writes_casa_log_table_without_stderr_noise() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let log_table = tempdir.path().join("mstransform.log");
    let output = Command::new(env!("CARGO_BIN_EXE_mstransform"))
        .arg("--log-table")
        .arg(&log_table)
        .arg("--log-stderr-priority")
        .arg("off")
        .arg("--help")
        .output()
        .expect("run mstransform --help");

    assert!(
        output.status.success(),
        "mstransform failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("mstransform"));
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");

    let table = Table::open(TableOptions::new(&log_table)).expect("open log table");
    validate_log_table(&table).expect("validate log table");
    assert!(
        table.row_count() >= 2,
        "expected start and completion log rows"
    );
    assert_log_contains(&table, "INFO", "mstransform completed");
}

#[test]
fn flagdata_empty_selection_writes_warn_log_row() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_fixture_ms(tempdir.path());
    let log_table = tempdir.path().join("flagdata.log");
    let output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
        .arg("--log-table")
        .arg(&log_table)
        .arg("--log-stderr-priority")
        .arg("off")
        .arg("--vis")
        .arg(&ms_path)
        .arg("--mode")
        .arg("summary")
        .arg("--field")
        .arg("999")
        .output()
        .expect("run flagdata summary with empty selection");

    assert!(
        output.status.success(),
        "flagdata failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");

    let table = Table::open(TableOptions::new(&log_table)).expect("open log table");
    validate_log_table(&table).expect("validate log table");
    assert_log_contains(&table, "WARN", "flagdata selected no rows");
}

#[test]
fn mstransform_failure_writes_severe_log_row() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let log_table = tempdir.path().join("mstransform-failure.log");
    let missing_ms = tempdir.path().join("missing.ms");
    let output_ms = tempdir.path().join("out.ms");
    let output = Command::new(env!("CARGO_BIN_EXE_mstransform"))
        .arg("--log-table")
        .arg(&log_table)
        .arg("--log-stderr-priority")
        .arg("off")
        .arg("--vis")
        .arg(&missing_ms)
        .arg("--out")
        .arg(&output_ms)
        .output()
        .expect("run failing mstransform");

    assert!(
        !output.status.success(),
        "mstransform unexpectedly succeeded: stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );

    let table = Table::open(TableOptions::new(&log_table)).expect("open log table");
    validate_log_table(&table).expect("validate log table");
    assert_log_contains(&table, "SEVERE", "mstransform failed");
}

fn assert_log_contains(table: &Table, priority: &str, message_fragment: &str) {
    let priorities = table
        .column_accessor("PRIORITY")
        .expect("PRIORITY column")
        .scalar_cells_owned()
        .expect("PRIORITY cells");
    let messages = table
        .column_accessor("MESSAGE")
        .expect("MESSAGE column")
        .scalar_cells_owned()
        .expect("MESSAGE cells");
    let found = priorities
        .iter()
        .zip(messages.iter())
        .any(|(row_priority, row_message)| {
            matches!(row_priority, Some(ScalarValue::String(value)) if value == priority)
                && matches!(row_message, Some(ScalarValue::String(value)) if value.contains(message_fragment))
        });
    assert!(
        found,
        "expected {priority} log row containing {message_fragment:?}"
    );
}
