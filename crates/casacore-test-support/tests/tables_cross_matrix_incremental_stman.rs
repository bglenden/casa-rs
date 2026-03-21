// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_tables::{ColumnOptions, ColumnSchema, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
    run_table_cross_matrix,
};
use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ShapeBuilder;

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
        tile_shape: None,
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
        tile_shape: None,
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

fn assert_supported_matrix_results(
    results: &[casacore_test_support::table_interop::MatrixCellResult],
    supported: impl Fn(&str) -> bool,
) {
    for result in results.iter().filter(|result| supported(result.label)) {
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
        tile_shape: None,
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

// --- RR-only: typed arrays ---

fn ism_typed_arrays_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_i32", PrimitiveType::Int32, vec![4])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_fixed("arr_f64", PrimitiveType::Float64, vec![2, 2])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_fixed("arr_bool", PrimitiveType::Bool, vec![3])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new(
                "arr_i32",
                Value::Array(ArrayValue::Int32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![10, 20, 30, 40])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_f64",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2, 2]).f(),
                        vec![1.1, 2.2, 3.3, 4.4],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_bool",
                Value::Array(ArrayValue::Bool(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[3]), vec![true, false, true])
                        .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
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
                        ndarray::IxDyn(&[2, 2]).f(),
                        vec![5.5, 6.6, 7.7, 8.8],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_bool",
                Value::Array(ArrayValue::Bool(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[3]), vec![false, false, false])
                        .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_i32",
                Value::Array(ArrayValue::Int32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_f64",
                Value::Array(ArrayValue::Float64(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2, 2]).f(),
                        vec![0.0, 0.0, 0.0, 0.0],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_bool",
                Value::Array(ArrayValue::Bool(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[3]), vec![true, true, true])
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
        cpp_fixture: Some(CppTableFixture::IsmTypedArrays),
        tile_shape: None,
    }
}

// --- ISM complex arrays (upgraded to full cross-matrix) ---

fn ism_complex_arrays_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_c32", PrimitiveType::Complex32, vec![2])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_fixed("arr_c64", PrimitiveType::Complex64, vec![2])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
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
        cpp_fixture: Some(CppTableFixture::IsmComplexArrays),
        tile_shape: None,
    }
}

// --- ISM column keywords (upgraded to full cross-matrix) ---

fn ism_column_keywords_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("id", PrimitiveType::Int32),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
        ]),
        RecordValue::new(vec![
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(2.7))),
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
        ]),
    ];

    let table_keywords = RecordValue::new(vec![RecordField::new(
        "telescope",
        Value::Scalar(ScalarValue::String("VLA".to_string())),
    )]);

    let column_keywords = vec![
        (
            "flux".to_string(),
            RecordValue::new(vec![
                RecordField::new("unit", Value::Scalar(ScalarValue::String("Jy".to_string()))),
                RecordField::new(
                    "ref_frame",
                    Value::Scalar(ScalarValue::String("LSRK".to_string())),
                ),
            ]),
        ),
        (
            "id".to_string(),
            RecordValue::new(vec![RecordField::new(
                "description",
                Value::Scalar(ScalarValue::String("source identifier".to_string())),
            )]),
        ),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords,
        column_keywords,
        cpp_fixture: Some(CppTableFixture::IsmColumnKeywords),
        tile_shape: None,
    }
}

// Complex scalar 2x2 cross-matrix (Complex32 + Complex64).

fn ism_complex_scalars_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_c32", PrimitiveType::Complex32),
        ColumnSchema::scalar("col_c64", PrimitiveType::Complex64),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new(
                "col_c32",
                Value::Scalar(ScalarValue::Complex32(Complex32::new(1.0, 2.0))),
            ),
            RecordField::new(
                "col_c64",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(3.0, 4.0))),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "col_c32",
                Value::Scalar(ScalarValue::Complex32(Complex32::new(0.0, 0.0))),
            ),
            RecordField::new(
                "col_c64",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(0.0, 0.0))),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "col_c32",
                Value::Scalar(ScalarValue::Complex32(Complex32::new(-5.5, 7.25))),
            ),
            RecordField::new(
                "col_c64",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(-1e10, 1e-10))),
            ),
        ]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::IsmComplexScalars),
        tile_shape: None,
    }
}

#[test]
fn ism_complex_scalars_cross_matrix() {
    let fixture = ism_complex_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

#[test]
fn ism_complex_scalars_endian_cross_matrix() {
    let fixture = ism_complex_scalars_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

// --- Full cross-matrix tests (upgraded from RR-only) ---

#[test]
fn ism_typed_arrays_cross_matrix() {
    let fixture = ism_typed_arrays_fixture();
    assert_supported_matrix_results(
        &run_full_cross_matrix(&fixture, ManagerKind::IncrementalStMan),
        |label| label != "RC",
    );
}

#[test]
fn ism_complex_arrays_cross_matrix() {
    let fixture = ism_complex_arrays_fixture();
    assert_supported_matrix_results(
        &run_full_cross_matrix(&fixture, ManagerKind::IncrementalStMan),
        |label| label != "RC",
    );
}

#[test]
fn ism_column_keywords_cross_matrix() {
    let fixture = ism_column_keywords_fixture();
    assert_matrix_results(&run_full_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

// --- Endian cross-matrix for RR-only fixtures ---

#[test]
fn ism_all_numeric_scalars_endian_cross_matrix() {
    let fixture = ism_all_numeric_scalars_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}

#[test]
fn ism_typed_arrays_endian_cross_matrix() {
    let fixture = ism_typed_arrays_fixture();
    assert_supported_matrix_results(
        &run_endian_cross_matrix(&fixture, ManagerKind::IncrementalStMan),
        |label| !label.starts_with("RC-"),
    );
}

#[test]
fn ism_complex_arrays_endian_cross_matrix() {
    let fixture = ism_complex_arrays_fixture();
    assert_supported_matrix_results(
        &run_endian_cross_matrix(&fixture, ManagerKind::IncrementalStMan),
        |label| !label.starts_with("RC-"),
    );
}

#[test]
fn ism_column_keywords_endian_cross_matrix() {
    let fixture = ism_column_keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::IncrementalStMan,
    ));
}
