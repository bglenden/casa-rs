// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2x2 interop tests for TableInfo (Wave 18).
//!
//! Verifies that `table.info` metadata (type + subType) round-trips correctly
//! between Rust and C++ casacore.

#![cfg(feature = "cpp-interop-tests")]

use casa_tables::{Table, TableInfo, TableOptions};
use casa_test_support::table_interop::{ManagerKind, TableFixture};
use casa_test_support::{CppTableFixture, TableOracle};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use casa_tables::ColumnSchema;
use casa_tables::TableSchema;

fn table_info_fixture() -> TableFixture {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");

    let rows = vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(1)),
    )])];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::TableInfoMetadata),
        tile_shape: None,
    }
}

fn expected_info() -> TableInfo {
    TableInfo {
        table_type: "Measurement".to_string(),
        sub_type: "UVFITS".to_string(),
        readme: Vec::new(),
    }
}

fn assert_matrix_results(results: &[casa_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[TableInfo] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

/// RR: Rust write → Rust read. Verify row data AND TableInfo.
#[test]
fn table_info_rr() {
    let fixture = table_info_fixture();
    let results = casa_test_support::table_interop::run_table_cross_matrix(
        &fixture,
        ManagerKind::StManAipsIO,
    );
    assert_matrix_results(&results);

    // Additionally verify TableInfo round-trip
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rr_info");

    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table.set_info(expected_info());
    table
        .save(TableOptions::new(&path).with_data_manager(casa_tables::DataManagerKind::StManAipsIO))
        .unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.info(), &expected_info(), "RR: TableInfo mismatch");
}

/// CC: C++ write → C++ verify. Skipped if C++ unavailable.
#[test]
fn table_info_cc() {
    let fixture = table_info_fixture();
    if let Some(result) = casa_test_support::table_interop::run_cc_only(&fixture) {
        assert!(
            result.passed,
            "[TableInfo] CC: {}",
            result.error.as_deref().unwrap_or("unknown error")
        );
    } else {
        eprintln!("skipping: C++ casacore unavailable");
    }
}

/// CR: C++ write → Rust read. Verify row data AND TableInfo.
#[test]
fn table_info_cr() {
    if !casa_test_support::casacore_oracle_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }

    let fixture = table_info_fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_info");

    TableOracle::table_write(CppTableFixture::TableInfoMetadata, &path).expect("C++ write");

    // Verify row data via standard fixture comparison
    casa_test_support::table_interop::read_and_verify(&fixture, ManagerKind::StManAipsIO, &path)
        .expect("CR row data verification failed");

    // Verify TableInfo
    let table = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(table.info(), &expected_info(), "CR: TableInfo mismatch");
}

/// RC: Rust write → C++ verify. Verify row data AND TableInfo.
#[test]
fn table_info_rc() {
    if !casa_test_support::casacore_oracle_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }

    let fixture = table_info_fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc_info");

    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table.set_info(expected_info());
    table
        .save(TableOptions::new(&path).with_data_manager(casa_tables::DataManagerKind::StManAipsIO))
        .unwrap();

    TableOracle::table_verify(CppTableFixture::TableInfoMetadata, &path)
        .expect("RC: C++ verify failed");
}
