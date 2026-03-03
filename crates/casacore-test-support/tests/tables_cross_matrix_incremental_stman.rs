// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_tables::{ColumnSchema, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
    run_table_cross_matrix,
};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn ism_scalar_primitives_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_bool", PrimitiveType::Bool),
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("col_bool", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("hello".to_string())),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_bool", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(-7))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(-99.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("world".to_string())),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_bool", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("col_str", Value::Scalar(ScalarValue::String(String::new()))),
        ]),
    ];

    let table_keywords = RecordValue::new(vec![RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("test-harness".to_string())),
    )]);

    TableFixture {
        schema,
        rows,
        table_keywords,
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::IsmScalarPrimitives),
    }
}

fn ism_slowly_changing_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("SCAN_NUMBER", PrimitiveType::Int32),
        ColumnSchema::scalar("FLAG", PrimitiveType::Bool),
    ])
    .expect("schema");

    // 10 rows where values repeat across consecutive rows.
    // SCAN_NUMBER: 0,0,0,1,1,1,1,2,2,2
    // FLAG:        T,T,T,T,T,F,F,F,T,T
    let scans = [0, 0, 0, 1, 1, 1, 1, 2, 2, 2];
    let flags = [
        true, true, true, true, true, false, false, false, true, true,
    ];

    let rows: Vec<RecordValue> = scans
        .iter()
        .zip(flags.iter())
        .map(|(&s, &f)| {
            RecordValue::new(vec![
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(s))),
                RecordField::new("FLAG", Value::Scalar(ScalarValue::Bool(f))),
            ])
        })
        .collect();

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::IsmSlowlyChanging),
    }
}

fn assert_matrix_results(results: &[casacore_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[IncrementalStMan] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

// Full 2x2 cross-matrix tests: RR, CC, CR, RC.
// CC/CR/RC are skipped if C++ casacore is unavailable.

#[test]
fn ism_scalar_primitives_cross_matrix() {
    let fixture = ism_scalar_primitives_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

#[test]
fn ism_slowly_changing_cross_matrix() {
    let fixture = ism_slowly_changing_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

// Endian cross-matrix: RR-BE, RR-LE, and (when C++ is available) RC-BE, RC-LE.

#[test]
fn ism_scalar_primitives_endian_cross_matrix() {
    let fixture = ism_scalar_primitives_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

#[test]
fn ism_slowly_changing_endian_cross_matrix() {
    let fixture = ism_slowly_changing_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

// RR-only test verifying all scalar types (no C++ fixture needed).

fn ism_all_numeric_scalars_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_u8", PrimitiveType::UInt8),
        ColumnSchema::scalar("col_i16", PrimitiveType::Int16),
        ColumnSchema::scalar("col_u16", PrimitiveType::UInt16),
        ColumnSchema::scalar("col_u32", PrimitiveType::UInt32),
        ColumnSchema::scalar("col_f32", PrimitiveType::Float32),
        ColumnSchema::scalar("col_i64", PrimitiveType::Int64),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("col_u8", Value::Scalar(ScalarValue::UInt8(255))),
            RecordField::new("col_i16", Value::Scalar(ScalarValue::Int16(-1234))),
            RecordField::new("col_u16", Value::Scalar(ScalarValue::UInt16(65535))),
            RecordField::new("col_u32", Value::Scalar(ScalarValue::UInt32(100_000))),
            RecordField::new("col_f32", Value::Scalar(ScalarValue::Float32(2.75))),
            RecordField::new("col_i64", Value::Scalar(ScalarValue::Int64(i64::MAX))),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_u8", Value::Scalar(ScalarValue::UInt8(0))),
            RecordField::new("col_i16", Value::Scalar(ScalarValue::Int16(0))),
            RecordField::new("col_u16", Value::Scalar(ScalarValue::UInt16(0))),
            RecordField::new("col_u32", Value::Scalar(ScalarValue::UInt32(0))),
            RecordField::new("col_f32", Value::Scalar(ScalarValue::Float32(0.0))),
            RecordField::new("col_i64", Value::Scalar(ScalarValue::Int64(0))),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_u8", Value::Scalar(ScalarValue::UInt8(128))),
            RecordField::new("col_i16", Value::Scalar(ScalarValue::Int16(i16::MIN))),
            RecordField::new("col_u16", Value::Scalar(ScalarValue::UInt16(32768))),
            RecordField::new("col_u32", Value::Scalar(ScalarValue::UInt32(u32::MAX))),
            RecordField::new("col_f32", Value::Scalar(ScalarValue::Float32(-1e10))),
            RecordField::new("col_i64", Value::Scalar(ScalarValue::Int64(i64::MIN))),
        ]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: None,
    }
}

#[test]
fn ism_all_numeric_scalars_rr() {
    let fixture = ism_all_numeric_scalars_fixture();
    assert_matrix_results(&run_table_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}
