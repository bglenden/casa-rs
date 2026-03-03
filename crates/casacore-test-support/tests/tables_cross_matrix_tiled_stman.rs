// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_tables::{ColumnSchema, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
};
use casacore_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::ShapeBuilder;

// ===== TiledColumnStMan fixture =====
// Fixed-shape Float32 [2,3], 3 rows, tile shape [2,3,2].
// Same cell values as the ssm_fixed_array fixture.

fn tiled_column_stman_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 3],
    )])
    .expect("schema");

    let rows = vec![
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
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                )
                .unwrap(),
            )),
        )]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::TiledColumnStMan),
        tile_shape: Some(vec![2, 3, 2]),
    }
}

// ===== TiledShapeStMan fixture =====
// Variable-shape Float32, 4 rows with two different shapes.
// Rows 0,3: [2,3], Rows 1,2: [3,2].

fn tiled_shape_stman_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    let rows = vec![
        // Row 0: [2,3], values 1..6
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
        // Row 1: [3,2], values 10..15
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 2]).f(),
                    vec![10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
                )
                .unwrap(),
            )),
        )]),
        // Row 2: [3,2], values 20..25
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 2]).f(),
                    vec![20.0, 21.0, 22.0, 23.0, 24.0, 25.0],
                )
                .unwrap(),
            )),
        )]),
        // Row 3: [2,3], values 30..35
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 3]).f(),
                    vec![30.0, 31.0, 32.0, 33.0, 34.0, 35.0],
                )
                .unwrap(),
            )),
        )]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::TiledShapeStMan),
        tile_shape: Some(vec![2, 3, 2]),
    }
}

// ===== TiledCellStMan fixture =====
// Variable-shape Float32, 3 rows each with a unique shape.
// Row 0: [2,3], Row 1: [4,2], Row 2: [3,3].

fn tiled_cell_stman_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Float32,
        Some(2),
    )])
    .expect("schema");

    let rows = vec![
        // Row 0: [2,3], values 1..6
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
        // Row 1: [4,2], values 10..17
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[4, 2]).f(),
                    vec![10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0],
                )
                .unwrap(),
            )),
        )]),
        // Row 2: [3,3], values 20..28
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3, 3]).f(),
                    vec![20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0],
                )
                .unwrap(),
            )),
        )]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::TiledCellStMan),
        tile_shape: Some(vec![4, 4]),
    }
}

fn assert_matrix_results(results: &[casacore_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[TiledStMan] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

// Full 2x2 cross-matrix tests: RR, CC, CR, RC.

#[test]
fn tiled_column_stman_cross_matrix() {
    let fixture = tiled_column_stman_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_shape_stman_cross_matrix() {
    let fixture = tiled_shape_stman_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::TiledShapeStMan,
    ));
}

#[test]
fn tiled_cell_stman_cross_matrix() {
    let fixture = tiled_cell_stman_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::TiledCellStMan,
    ));
}

// Endian cross-matrix tests: RR-BE, RR-LE, RC-BE, RC-LE.

#[test]
fn tiled_column_stman_endian_cross_matrix() {
    let fixture = tiled_column_stman_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_shape_stman_endian_cross_matrix() {
    let fixture = tiled_shape_stman_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledShapeStMan,
    ));
}

#[test]
fn tiled_cell_stman_endian_cross_matrix() {
    let fixture = tiled_cell_stman_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledCellStMan,
    ));
}
