// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_tables::{ColumnSchema, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
    run_table_cross_matrix,
};
use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
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

// --- RR-only fixtures for expanded type coverage ---

// All numeric array types with uniform shape [4] (required by TiledColumnStMan).
fn tiled_all_numeric_arrays_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_i16", PrimitiveType::Int16, vec![4]),
        ColumnSchema::array_fixed("arr_i32", PrimitiveType::Int32, vec![4]),
        ColumnSchema::array_fixed("arr_f64", PrimitiveType::Float64, vec![4]),
        ColumnSchema::array_fixed("arr_i64", PrimitiveType::Int64, vec![4]),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new(
                "arr_i16",
                Value::Array(ArrayValue::Int16(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![1, 2, 3, 4]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_i32",
                Value::Array(ArrayValue::Int32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![100, 200, 300, 400])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_f64",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.1, 2.2, 3.3, 4.4])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_i64",
                Value::Array(ArrayValue::Int64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![i64::MAX, i64::MIN, 0, 1],
                    )
                    .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_i16",
                Value::Array(ArrayValue::Int16(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![i16::MIN, i16::MAX, 0, -1],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_i32",
                Value::Array(ArrayValue::Int32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![-1, -2, -3, -4])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_f64",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![-1e10, 0.0, 1e-10, 1e10],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_i64",
                Value::Array(ArrayValue::Int64(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_i16",
                Value::Array(ArrayValue::Int16(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_i32",
                Value::Array(ArrayValue::Int32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_f64",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0.0, 0.0, 0.0, 0.0])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_i64",
                Value::Array(ArrayValue::Int64(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
        ]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: None,
        tile_shape: Some(vec![4, 2]),
    }
}

// Complex array types with uniform shape [2].
fn tiled_complex_arrays_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_c32", PrimitiveType::Complex32, vec![2]),
        ColumnSchema::array_fixed("arr_c64", PrimitiveType::Complex64, vec![2]),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new(
                "arr_c32",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex32::new(1.0, 2.0), Complex32::new(3.0, 4.0)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_c64",
                Value::Array(ArrayValue::Complex64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex64::new(5.0, 6.0), Complex64::new(7.0, 8.0)],
                    )
                    .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_c32",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex32::new(0.0, 0.0), Complex32::new(0.0, 0.0)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_c64",
                Value::Array(ArrayValue::Complex64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex64::new(0.0, 0.0), Complex64::new(0.0, 0.0)],
                    )
                    .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_c32",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex32::new(-5.5, 7.25), Complex32::new(1e3, -1e3)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_c64",
                Value::Array(ArrayValue::Complex64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex64::new(-1e10, 1e-10), Complex64::new(1e10, -1e-10)],
                    )
                    .unwrap(),
                )),
            ),
        ]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: None,
        tile_shape: Some(vec![2, 2]),
    }
}

// Table keywords with a Float32 [2] column.
fn tiled_table_keywords_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2],
    )])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(ndarray::IxDyn(&[2]), vec![1.0, 2.0]).unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(ndarray::IxDyn(&[2]), vec![3.0, 4.0]).unwrap(),
            )),
        )]),
    ];

    let table_keywords = RecordValue::new(vec![
        RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("ALMA".to_string())),
        ),
        RecordField::new("version", Value::Scalar(ScalarValue::Int32(3))),
    ]);

    TableFixture {
        schema,
        rows,
        table_keywords,
        column_keywords: vec![],
        cpp_fixture: None,
        tile_shape: Some(vec![2, 2]),
    }
}

// Column keywords with two Float64 [4] columns.
fn tiled_column_keywords_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("flux", PrimitiveType::Float64, vec![4]),
        ColumnSchema::array_fixed("data", PrimitiveType::Float64, vec![4]),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new(
                "flux",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.1, 2.2, 3.3, 4.4])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![10.0, 20.0, 30.0, 40.0],
                    )
                    .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "flux",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![5.5, 6.6, 7.7, 8.8])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![50.0, 60.0, 70.0, 80.0],
                    )
                    .unwrap(),
                )),
            ),
        ]),
    ];

    let table_keywords = RecordValue::new(vec![RecordField::new(
        "telescope",
        Value::Scalar(ScalarValue::String("VLA".to_string())),
    )]);

    let column_keywords = vec![
        (
            "flux".to_string(),
            RecordValue::new(vec![RecordField::new(
                "unit",
                Value::Scalar(ScalarValue::String("Jy".to_string())),
            )]),
        ),
        (
            "data".to_string(),
            RecordValue::new(vec![RecordField::new(
                "description",
                Value::Scalar(ScalarValue::String("raw visibilities".to_string())),
            )]),
        ),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords,
        column_keywords,
        cpp_fixture: None,
        tile_shape: Some(vec![4, 2]),
    }
}

// --- RR-only cross-matrix tests ---

#[test]
fn tiled_all_numeric_arrays_rr() {
    let fixture = tiled_all_numeric_arrays_fixture();
    assert_matrix_results(&run_table_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_complex_arrays_rr() {
    let fixture = tiled_complex_arrays_fixture();
    assert_matrix_results(&run_table_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_table_keywords_rr() {
    let fixture = tiled_table_keywords_fixture();
    assert_matrix_results(&run_table_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_column_keywords_rr() {
    let fixture = tiled_column_keywords_fixture();
    assert_matrix_results(&run_table_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

// --- Endian cross-matrix for RR-only fixtures ---

#[test]
fn tiled_all_numeric_arrays_endian_cross_matrix() {
    let fixture = tiled_all_numeric_arrays_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_complex_arrays_endian_cross_matrix() {
    let fixture = tiled_complex_arrays_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_table_keywords_endian_cross_matrix() {
    let fixture = tiled_table_keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}

#[test]
fn tiled_column_keywords_endian_cross_matrix() {
    let fixture = tiled_column_keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::TiledColumnStMan,
    ));
}
