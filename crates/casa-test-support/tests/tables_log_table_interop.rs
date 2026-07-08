// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interop tests for CASA-compatible LOG tables.

#![cfg(feature = "cpp-interop-tests")]

use casa_logging::{
    CasaLogRecord, CasaLogSink, CasaPriority, CasaTableLogSink, TableLogOpenMode,
    validate_log_table,
};
use casa_tables::{Table, TableOptions};
use casa_test_support::{
    CppTableFixture, cpp_backend_available, cpp_table_verify, cpp_table_write,
};
use casa_types::{ScalarValue, Value};

const LOG_TABLE_README: &str = "Repository for software-generated logging messages";

#[test]
fn log_table_rust_write_cpp_verify() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_log.tbl");
    let sink = CasaTableLogSink::new(&path, CasaPriority::Debugging, TableLogOpenMode::CreateNew);
    let record =
        CasaLogRecord::new(CasaPriority::Info, "Rust log row", "rust::log").with_object_id("rust");
    sink.write(&record).expect("Rust log-table write");
    sink.flush().expect("Rust log-table flush");

    cpp_table_verify(CppTableFixture::LogTable, &path).expect("C++ should verify Rust LOG table");
}

#[test]
fn log_table_cpp_write_rust_read() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_log.tbl");
    cpp_table_write(CppTableFixture::LogTable, &path).expect("C++ LOG table write");

    let table = Table::open(TableOptions::new(&path)).expect("open C++ LOG table");
    validate_log_table(&table).expect("validate C++ LOG table schema");
    assert!(
        matches!(table.info().table_type.as_str(), "LOG" | "Log message"),
        "C++ LOG table should use a LOG-compatible TableInfo type"
    );
    assert!(
        table
            .info()
            .readme
            .iter()
            .any(|line| line == LOG_TABLE_README),
        "LOG table readme should round-trip"
    );
    let schema = table.schema().expect("LOG table schema");
    assert_eq!(
        schema.column("TIME").expect("TIME column").comment(),
        "MJD in seconds"
    );
    assert_eq!(
        schema
            .column("PRIORITY")
            .expect("PRIORITY column")
            .max_length(),
        9
    );

    let time_keywords = table.column_keywords("TIME").expect("TIME keywords");
    assert_eq!(
        time_keywords.get("UNIT"),
        Some(&Value::Scalar(ScalarValue::String("s".to_string())))
    );
    assert_eq!(
        time_keywords.get("MEASURE_TYPE"),
        Some(&Value::Scalar(ScalarValue::String("EPOCH".to_string())))
    );
    assert_eq!(
        time_keywords.get("MEASURE_REFERENCE"),
        Some(&Value::Scalar(ScalarValue::String("UTC".to_string())))
    );

    assert_eq!(table.row_count(), 1);
    assert_eq!(
        table
            .column_accessor("PRIORITY")
            .unwrap()
            .scalar_cell(0)
            .unwrap(),
        &ScalarValue::String("INFO".to_string())
    );
    assert_eq!(
        table
            .column_accessor("MESSAGE")
            .unwrap()
            .scalar_cell(0)
            .unwrap(),
        &ScalarValue::String("C++ log row".to_string())
    );
}

#[test]
fn log_table_cpp_write_cpp_verify() {
    if !cpp_backend_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_log.tbl");
    cpp_table_write(CppTableFixture::LogTable, &path).expect("C++ LOG table write");
    cpp_table_verify(CppTableFixture::LogTable, &path).expect("C++ LOG table verify");
}
