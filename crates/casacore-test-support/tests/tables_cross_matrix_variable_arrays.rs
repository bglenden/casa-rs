// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2×2 cross-matrix tests for variable-shape (indirect) array columns,
//! covering both StManAipsIO and StandardStMan storage managers.

use casacore_tables::{ColumnSchema, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
};
use casacore_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::ShapeBuilder;

/// Build the standard 4-row Float32 variable-shape fixture.
///
/// Row 0: shape [2,3], values 1.0..6.0
/// Row 1: shape [3,2], values 7.0..12.0
/// Row 2: shape [3,2], values 13.0..18.0
/// Row 3: shape [2,3], values 19.0..24.0
fn variable_array_rows() -> Vec<RecordValue> {
    vec![
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 2]).f(),
                    vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 2]).f(),
                    vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![19.0, 20.0, 21.0, 22.0, 23.0, 24.0],
                )
                .unwrap(),
            )),
        )]),
    ]
}

fn aipsio_variable_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    TableFixture {
        schema,
        rows: variable_array_rows(),
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::AipsIOVariableArray),
        tile_shape: None,
    }
}

fn ssm_variable_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    TableFixture {
        schema,
        rows: variable_array_rows(),
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::SsmVariableArray),
        tile_shape: None,
    }
}

fn assert_matrix_results(
    label: &str,
    results: &[casacore_test_support::table_interop::MatrixCellResult],
) {
    for result in results {
        assert!(
            result.passed,
            "[{label}] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

// ---- StManAipsIO variable-shape array tests ----

#[test]
fn aipsio_variable_array_cross_matrix() {
    let fixture = aipsio_variable_array_fixture();
    assert_matrix_results(
        "AipsIO-vararray",
        &run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO),
    );
}

#[test]
fn aipsio_variable_array_endian_cross_matrix() {
    let fixture = aipsio_variable_array_fixture();
    assert_matrix_results(
        "AipsIO-vararray-endian",
        &run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO),
    );
}

// ---- StandardStMan variable-shape array tests ----

#[test]
fn ssm_variable_array_cross_matrix() {
    let fixture = ssm_variable_array_fixture();
    assert_matrix_results(
        "SSM-vararray",
        &run_full_cross_matrix(&fixture, ManagerKind::StandardStMan),
    );
}

#[test]
fn ssm_variable_array_endian_cross_matrix() {
    let fixture = ssm_variable_array_fixture();
    assert_matrix_results(
        "SSM-vararray-endian",
        &run_endian_cross_matrix(&fixture, ManagerKind::StandardStMan),
    );
}
