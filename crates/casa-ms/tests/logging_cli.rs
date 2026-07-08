// SPDX-License-Identifier: LGPL-3.0-or-later

use std::process::Command;

use casa_logging::validate_log_table;
use casa_tables::{Table, TableOptions};

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
}
