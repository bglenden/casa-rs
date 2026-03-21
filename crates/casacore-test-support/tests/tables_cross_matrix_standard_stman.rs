// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_tables::{ColumnOptions, ColumnSchema, Table, TableOptions, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
    run_table_cross_matrix,
};
use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ShapeBuilder;

fn ssm_scalar_primitives_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::SsmScalarPrimitives),
        tile_shape: None,
    }
}

fn ssm_fixed_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![2, 3])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
    ])
    .expect("schema");

    // Arrays use Fortran (column-major) order to match casacore convention.
    // Flat data [1,2,3,4,5,6] in F-order for shape [2,3] means:
    //   [0,0]=1, [1,0]=2, [0,1]=3, [1,1]=4, [0,2]=5, [1,2]=6
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
        cpp_fixture: Some(CppTableFixture::SsmFixedArray),
        tile_shape: None,
    }
}

fn ssm_keywords_fixture() -> TableFixture {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");

    let rows = vec![
        RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]),
        RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(2)),
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
        cpp_fixture: Some(CppTableFixture::SsmKeywords),
        tile_shape: None,
    }
}

fn assert_matrix_results(results: &[casacore_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[StandardStMan] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

// Full 2x2 cross-matrix tests: RR, CC, CR, RC.
// CC/CR/RC are skipped if C++ casacore is unavailable.

#[test]
fn ssm_scalar_primitives_cross_matrix() {
    let fixture = ssm_scalar_primitives_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_fixed_array_cross_matrix() {
    let fixture = ssm_fixed_array_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_keywords_cross_matrix() {
    let fixture = ssm_keywords_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

// --- RR-only fixtures for expanded type coverage ---

fn ssm_all_numeric_scalars_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::SsmAllNumericScalars),
        tile_shape: None,
    }
}

fn ssm_complex_scalars_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::SsmComplexScalars),
        tile_shape: None,
    }
}

fn ssm_typed_arrays_fixture() -> TableFixture {
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
        ColumnSchema::array_fixed("arr_c32", PrimitiveType::Complex32, vec![2])
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
                "arr_c32",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex32::new(1.0, 2.0), Complex32::new(3.0, 4.0)],
                    )
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
                "arr_c32",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex32::new(-1.0, -2.0), Complex32::new(0.0, 0.0)],
                    )
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
                "arr_c32",
                Value::Array(ArrayValue::Complex32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2]),
                        vec![Complex32::new(0.0, 0.0), Complex32::new(0.0, 0.0)],
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
        cpp_fixture: Some(CppTableFixture::SsmTypedArrays),
        tile_shape: None,
    }
}

fn ssm_column_keywords_fixture() -> TableFixture {
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
        cpp_fixture: None,
        tile_shape: None,
    }
}

#[test]
fn ssm_all_numeric_scalars_cross_matrix() {
    let fixture = ssm_all_numeric_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_complex_scalars_cross_matrix() {
    let fixture = ssm_complex_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_typed_arrays_cross_matrix() {
    let fixture = ssm_typed_arrays_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_column_keywords_cross_matrix() {
    let fixture = ssm_column_keywords_fixture();
    assert_matrix_results(&run_table_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

// --- Unsigned integer arrays (Task 1.1) ---

fn ssm_unsigned_arrays_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_u8", PrimitiveType::UInt8, vec![4])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_fixed("arr_u16", PrimitiveType::UInt16, vec![4])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_fixed("arr_u32", PrimitiveType::UInt32, vec![4])
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
                "arr_u8",
                Value::Array(ArrayValue::UInt8(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![255, 128, 0, 1])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_u16",
                Value::Array(ArrayValue::UInt16(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![65535, 32768, 0, 1])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_u32",
                Value::Array(ArrayValue::UInt32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![u32::MAX, 100_000, 0, 1],
                    )
                    .unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_u8",
                Value::Array(ArrayValue::UInt8(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_u16",
                Value::Array(ArrayValue::UInt16(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_u32",
                Value::Array(ArrayValue::UInt32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![0, 0, 0, 0]).unwrap(),
                )),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "arr_u8",
                Value::Array(ArrayValue::UInt8(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![1, 2, 3, 4]).unwrap(),
                )),
            ),
            RecordField::new(
                "arr_u16",
                Value::Array(ArrayValue::UInt16(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[4]), vec![100, 200, 300, 400])
                        .unwrap(),
                )),
            ),
            RecordField::new(
                "arr_u32",
                Value::Array(ArrayValue::UInt32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![1000, 2000, 3000, 4000],
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
        cpp_fixture: Some(CppTableFixture::SsmUnsignedArrays),
        tile_shape: None,
    }
}

#[test]
fn ssm_unsigned_arrays_cross_matrix() {
    let fixture = ssm_unsigned_arrays_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

// --- String arrays (Task 1.2) ---

fn ssm_string_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_str", PrimitiveType::String, vec![3])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![RecordField::new(
            "arr_str",
            Value::Array(ArrayValue::String(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3]),
                    vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "arr_str",
            Value::Array(ArrayValue::String(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3]),
                    vec![String::new(), String::new(), String::new()],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "arr_str",
            Value::Array(ArrayValue::String(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[3]),
                    vec![
                        "hello world".to_string(),
                        "café".to_string(),
                        "line\nnewline".to_string(),
                    ],
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
        cpp_fixture: Some(CppTableFixture::SsmStringArray),
        tile_shape: None,
    }
}

#[test]
fn ssm_string_array_cross_matrix() {
    let fixture = ssm_string_array_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

// --- Complex64 2D arrays (Task 1.3) ---

fn ssm_complex64_2d_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_c64", PrimitiveType::Complex64, vec![2, 2])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![RecordField::new(
            "arr_c64",
            Value::Array(ArrayValue::Complex64(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 2]).f(),
                    vec![
                        Complex64::new(1.0, 2.0),
                        Complex64::new(3.0, 4.0),
                        Complex64::new(5.0, 6.0),
                        Complex64::new(7.0, 8.0),
                    ],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "arr_c64",
            Value::Array(ArrayValue::Complex64(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 2]).f(),
                    vec![
                        Complex64::new(0.0, 0.0),
                        Complex64::new(0.0, 0.0),
                        Complex64::new(0.0, 0.0),
                        Complex64::new(0.0, 0.0),
                    ],
                )
                .unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "arr_c64",
            Value::Array(ArrayValue::Complex64(
                ndarray::Array::from_shape_vec(
                    ndarray::IxDyn(&[2, 2]).f(),
                    vec![
                        Complex64::new(-1.0, 0.5),
                        Complex64::new(1e10, -1e10),
                        Complex64::new(0.0, 0.0),
                        Complex64::new(-0.25, 0.75),
                    ],
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
        cpp_fixture: Some(CppTableFixture::SsmComplex64Array2D),
        tile_shape: None,
    }
}

#[test]
fn ssm_complex64_2d_array_cross_matrix() {
    let fixture = ssm_complex64_2d_array_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

// --- Explicit big-endian / little-endian cross-matrix tests ---
//
// These exercise the endian-aware write path: Rust writes the table in an
// explicit endian format, then Rust reads it back (RR-BE, RR-LE) and, when
// C++ casacore is available, C++ verifies the table (RC-BE, RC-LE).

#[test]
fn ssm_scalar_primitives_endian_cross_matrix() {
    let fixture = ssm_scalar_primitives_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

#[test]
fn ssm_fixed_array_endian_cross_matrix() {
    let fixture = ssm_fixed_array_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

#[test]
fn ssm_keywords_endian_cross_matrix() {
    let fixture = ssm_keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

#[test]
fn ssm_all_numeric_scalars_endian_cross_matrix() {
    let fixture = ssm_all_numeric_scalars_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

#[test]
fn ssm_complex_scalars_endian_cross_matrix() {
    let fixture = ssm_complex_scalars_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

#[test]
fn ssm_typed_arrays_endian_cross_matrix() {
    let fixture = ssm_typed_arrays_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

#[test]
fn ssm_column_keywords_endian_cross_matrix() {
    let fixture = ssm_column_keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

// --- Post-mutation RR + RC tests ---

fn save_and_verify_mutation(
    table: &Table,
    dm: casacore_tables::DataManagerKind,
    cpp_fixture: CppTableFixture,
    label: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(label);
    table
        .save(TableOptions::new(&path).with_data_manager(dm))
        .unwrap();

    // RR: Rust reopen
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(
        reopened.row_count(),
        table.row_count(),
        "{label}: row count mismatch after RR"
    );

    // RC: C++ verify (skipped when C++ unavailable)
    if casacore_test_support::cpp_backend_available() {
        casacore_test_support::cpp_table_verify(cpp_fixture, &path)
            .unwrap_or_else(|e| panic!("{label}: C++ verify failed: {e}"));
    }
}

#[test]
fn mutation_add_column_ssm() {
    let fixture = ssm_scalar_primitives_fixture();
    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table
        .add_column(
            ColumnSchema::scalar("extra", PrimitiveType::Float32),
            Some(Value::Scalar(ScalarValue::Float32(42.0))),
        )
        .unwrap();

    assert_eq!(table.schema().unwrap().columns().len(), 5);
    save_and_verify_mutation(
        &table,
        casacore_tables::DataManagerKind::StandardStMan,
        CppTableFixture::MutationAddedColumn,
        "mutation_add_col_ssm",
    );
}

#[test]
fn mutation_remove_column_ssm() {
    let fixture = ssm_scalar_primitives_fixture();
    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table.remove_column("col_str").unwrap();

    assert_eq!(table.schema().unwrap().columns().len(), 3);
    save_and_verify_mutation(
        &table,
        casacore_tables::DataManagerKind::StandardStMan,
        CppTableFixture::MutationRemovedColumn,
        "mutation_rm_col_ssm",
    );
}

#[test]
fn mutation_remove_rows_ssm() {
    let fixture = ssm_scalar_primitives_fixture();
    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table.remove_rows(&[1]).unwrap();

    assert_eq!(table.row_count(), 2);
    save_and_verify_mutation(
        &table,
        casacore_tables::DataManagerKind::StandardStMan,
        CppTableFixture::MutationRemovedRows,
        "mutation_rm_rows_ssm",
    );
}
