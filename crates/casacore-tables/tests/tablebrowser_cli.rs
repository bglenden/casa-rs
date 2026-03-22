// SPDX-License-Identifier: LGPL-3.0-or-later
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserRequestEnvelope, BrowserResponse, BrowserResponseEnvelope,
    BrowserViewport,
};
use casacore_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use tempfile::tempdir;

#[test]
fn ui_schema_matches_launcher_contract() {
    let output = Command::new(tablebrowser_bin())
        .arg("--ui-schema")
        .output()
        .expect("run tablebrowser --ui-schema");
    assert!(output.status.success());

    let schema = serde_json::from_slice::<serde_json::Value>(&output.stdout).expect("parse schema");
    assert_eq!(schema["schema_version"], 1);
    assert_eq!(schema["command_id"], "tablebrowser");
    assert_eq!(schema["invocation_name"], "tablebrowser");
    assert_eq!(schema["display_name"], "Table Browser");
    assert_eq!(schema["managed_output"], serde_json::Value::Null);
    assert_eq!(schema["arguments"][0]["id"], "table_path");
    assert_eq!(schema["arguments"][0]["value_kind"], "path");
}

#[test]
fn session_returns_structured_errors_for_invalid_requests() {
    let mut child = Command::new(tablebrowser_bin())
        .arg("--session")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn tablebrowser session");

    let mut stdin = child.stdin.take().expect("session stdin");
    let stdout = child.stdout.take().expect("session stdout");
    let mut reader = BufReader::new(stdout);

    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&BrowserRequestEnvelope::new(BrowserCommand::GetSnapshot {
            viewport: None,
        }))
        .expect("serialize request")
    )
    .expect("write request");
    let response = read_response(&mut reader);
    assert_eq!(response.version, 1);
    match response.response {
        BrowserResponse::Error(error) => assert_eq!(error.code, "session_not_open"),
        other => panic!("expected session_not_open error, got {other:?}"),
    }

    stdin
        .write_all(br#"{"version":1,"command":{"command":"bogus"}}"#)
        .and_then(|_| stdin.write_all(b"\n"))
        .expect("write raw json");
    let response = read_response(&mut reader);
    match response.response {
        BrowserResponse::Error(error) => assert_eq!(error.code, "invalid_json"),
        other => panic!("expected invalid_json error, got {other:?}"),
    }

    stdin
        .write_all(br#"{"version":99,"command":{"command":"get_snapshot"}}"#)
        .and_then(|_| stdin.write_all(b"\n"))
        .expect("write unsupported version");
    let response = read_response(&mut reader);
    match response.response {
        BrowserResponse::Error(error) => assert_eq!(error.code, "unsupported_version"),
        other => panic!("expected unsupported_version error, got {other:?}"),
    }

    drop(stdin);
    assert!(child.wait().expect("wait child").success());
}

#[test]
fn session_reports_missing_root_table_as_structured_error() {
    let mut child = Command::new(tablebrowser_bin())
        .arg("--session")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn tablebrowser session");

    let mut stdin = child.stdin.take().expect("session stdin");
    let stdout = child.stdout.take().expect("session stdout");
    let mut reader = BufReader::new(stdout);

    writeln!(
        stdin,
        "{}",
        serde_json::to_string(&BrowserRequestEnvelope::new(BrowserCommand::OpenRoot {
            path: "/definitely/not/a/table".to_string(),
            viewport: BrowserViewport::new(80, 24),
        }))
        .expect("serialize open_root")
    )
    .expect("write request");
    let response = read_response(&mut reader);
    match response.response {
        BrowserResponse::Error(error) => assert_eq!(error.code, "open_root_failed"),
        other => panic!("expected open_root_failed error, got {other:?}"),
    }

    drop(stdin);
    assert!(child.wait().expect("wait child").success());
}

#[test]
fn snapshot_mode_limits_cells_to_default_row_window() {
    let temp = tempdir().expect("tempdir");
    let table_path = create_row_fixture(temp.path(), 30);

    let output = Command::new(tablebrowser_bin())
        .arg(&table_path)
        .output()
        .expect("run snapshot mode");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(stdout.contains("== Cells =="));
    assert!(stdout.contains("row-19"), "{stdout}");
    assert!(!stdout.contains("row-20"), "{stdout}");
}

fn read_response(reader: &mut impl BufRead) -> BrowserResponseEnvelope {
    let mut line = String::new();
    reader.read_line(&mut line).expect("read response line");
    assert!(!line.trim().is_empty(), "expected non-empty response line");
    serde_json::from_str(&line).expect("parse response")
}

fn create_row_fixture(root: &Path, rows: usize) -> PathBuf {
    let path = root.join("rows.tab");
    let schema = TableSchema::new(vec![ColumnSchema::scalar("name", PrimitiveType::String)])
        .expect("row fixture schema");
    let mut table = Table::with_schema(schema);
    for index in 0..rows {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String(format!("row-{index}"))),
            )]))
            .expect("add row");
    }
    table
        .save(TableOptions::new(&path))
        .expect("save fixture table");
    path
}

fn tablebrowser_bin() -> &'static str {
    env!("CARGO_BIN_EXE_tablebrowser")
}
