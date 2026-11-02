// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "cpp-interop-tests")]

use casa_tables::{
    ColumnOptions, ColumnSchema, DataManagerKind, EndianFormat, Table, TableOptions, TableSchema,
};
use casa_test_support::table_element_arrays::{
    ElementStorage, element_arrays_fixture, save_generated_element_arrays,
};
use casa_test_support::table_interop::{
    ManagerKind, TableFixture, read_and_verify, run_endian_cross_matrix, run_full_cross_matrix,
    run_table_cross_matrix,
};
use casa_test_support::{CppTableFixture, TableOracle};
use casa_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ShapeBuilder;
use std::path::Path;

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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
    }
}

fn assert_matrix_results(results: &[casa_test_support::table_interop::MatrixCellResult]) {
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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
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
        column_bindings: Vec::new(),
    }
}

#[test]
fn ssm_string_array_cross_matrix() {
    let fixture = ssm_string_array_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

// --- Strings longer than a string bucket ---

/// About `bytes` bytes of text, a run of one- and two-byte characters
/// starting at character `seed`; the C++ fixture's `text`.
fn text(bytes: usize, seed: usize) -> String {
    let characters = ['a', 'é', 'b', 'ç', 'c', '-', 'ü', 'd'];
    let mut text = String::with_capacity(bytes + 2);
    let mut index = seed;
    while text.len() < bytes {
        text.push(characters[index % characters.len()]);
        index += 1;
    }
    text
}

/// A scalar string, a fixed `String[3]` and a variable string vector whose
/// strings reach 100 kB: casa-rs's 32 KiB buckets and casacore's smaller
/// default ones both continue them in linked buckets, splitting
/// two-byte characters.
fn ssm_long_strings_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("NAME", PrimitiveType::String),
        ColumnSchema::array_fixed("FIXED", PrimitiveType::String, vec![3])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_variable("VARIABLE", PrimitiveType::String, Some(1)),
    ])
    .expect("schema");
    let strings = |values: Vec<String>| {
        let count = values.len();
        Value::Array(ArrayValue::String(
            ndarray::Array::from_shape_vec(ndarray::IxDyn(&[count]), values).unwrap(),
        ))
    };
    let row = |name: String, fixed: Vec<String>, variable: Vec<String>| {
        RecordValue::new(vec![
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name))),
            RecordField::new("FIXED", strings(fixed)),
            RecordField::new("VARIABLE", strings(variable)),
        ])
    };
    let rows = vec![
        row(
            text(100_000, 0),
            vec![text(70_000, 1), "x".to_string(), text(40_000, 2)],
            vec![text(33_000, 3), text(5, 4)],
        ),
        row(
            "short".to_string(),
            vec![String::new(), String::new(), String::new()],
            vec!["a".to_string()],
        ),
        row(
            text(32_800, 5),
            vec!["fixed".to_string(), text(32_769, 6), String::new()],
            vec![text(65_536, 7), String::new(), text(1_000, 8)],
        ),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::SsmLongStrings),
        tile_shape: None,
        column_bindings: Vec::new(),
    }
}

#[test]
fn ssm_long_strings_cross_matrix() {
    let fixture = ssm_long_strings_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_long_strings_endian_cross_matrix() {
    let fixture = ssm_long_strings_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

/// Bytes of a StandardStMan bucket of casa-rs's long-string table, and of
/// the file header before the buckets.
const SSM_BUCKET_BYTES: usize = 32_768;
const SSM_HEADER_BYTES: usize = 512;

/// The long-string rows after [`TableOracle::update_ssm_long_strings`].
fn updated_long_string_rows() -> Vec<RecordValue> {
    let strings = |values: Vec<String>| {
        let count = values.len();
        Value::Array(ArrayValue::String(
            ndarray::Array::from_shape_vec(ndarray::IxDyn(&[count]), values).unwrap(),
        ))
    };
    let row = |name: String, fixed: Vec<String>, variable: Vec<String>| {
        RecordValue::new(vec![
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name))),
            RecordField::new("FIXED", strings(fixed)),
            RecordField::new("VARIABLE", strings(variable)),
        ])
    };
    vec![
        row(
            "n".repeat(1_700),
            vec!["f".repeat(13), "g".repeat(111), "h".repeat(25)],
            vec![text(33_000, 3), text(5, 4)],
        ),
        row(
            "short".to_string(),
            vec![String::new(), String::new(), String::new()],
            vec!["a".to_string()],
        ),
        row(
            text(32_800, 5),
            vec!["fixed".to_string(), text(32_769, 6), String::new()],
            vec!["v".repeat(250_000), "w".repeat(20)],
        ),
        row(
            "m".repeat(50_000),
            vec!["p".to_string(), "q".to_string(), "r".to_string()],
            vec!["s".to_string()],
        ),
    ]
}

/// Check that the string buckets of the StandardStMan files of the tables
/// at `ours` and `theirs`, the buckets after their one data bucket up to
/// the last string bucket, hold the same header and the same bytes in use.
/// casacore's unused bytes are not compared: `SSMStringHandler` copies its
/// whole working buffer into each bucket, so a new bucket's unused bytes
/// hold what the one before it held there.
fn assert_same_string_buckets(ours: &Path, theirs: &Path, last_string_bucket: usize, what: &str) {
    let read = |path: &Path| std::fs::read(path.join("table.f0")).expect("the SSM file");
    let (ours, theirs) = (read(ours), read(theirs));
    for bucket in 1..=last_string_bucket {
        let start = SSM_HEADER_BYTES + bucket * SSM_BUCKET_BYTES;
        let used = i32::from_be_bytes(ours[start + 4..start + 8].try_into().expect("four bytes"));
        let range = start..start + 16 + usize::try_from(used).expect("a used length");
        if let Some(at) = ours[range.clone()]
            .iter()
            .zip(&theirs[range])
            .position(|(our, their)| our != their)
        {
            let near = start + at.saturating_sub(at % 4)..start + at.saturating_sub(at % 4) + 16;
            panic!(
                "{what}: string bucket {bucket} differs from casacore's at byte {at}: \
                 {:02x?} against {:02x?}",
                &ours[near.clone()],
                &theirs[near]
            );
        }
    }
}

/// casacore updates a table of long strings casa-rs wrote as it updates its
/// own. Written column by column in buckets of 32 KiB, casacore lays the
/// strings out in the same string buckets as casa-rs, byte for byte:
/// placement, links, used and deleted counts. casacore then shrinks a
/// scalar and a fixed array that span several buckets, grows a variable
/// array into the buckets that frees and appends a long scalar, in each
/// table. Its account of the buckets (total, free, first free, the last
/// string bucket and where the index lies) is the same for both, as written
/// and as updated, and both libraries read every cell of both.
#[test]
fn ssm_long_strings_update_in_casacore_as_casacores_own() {
    if !casa_test_support::casacore_oracle_available() {
        return;
    }
    let fixture = ssm_long_strings_fixture();
    let updated = TableFixture {
        rows: updated_long_string_rows(),
        cpp_fixture: None,
        ..fixture.clone()
    };
    for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
        let directory = tempfile::tempdir().expect("directory");
        let ours = directory.path().join("casa-rs");
        let theirs = directory.path().join("casacore");
        let mut table = Table::with_schema(fixture.schema.clone());
        for row in &fixture.rows {
            table.add_row(row.clone()).expect("row");
        }
        table
            .save(
                TableOptions::new(&ours)
                    .with_data_manager(DataManagerKind::StandardStMan)
                    .with_endian_format(endian),
            )
            .expect("save");
        TableOracle::write_ssm_long_strings_by_column(&theirs, endian == EndianFormat::BigEndian)
            .expect("casacore writes the rows");

        let statistics = |path: &Path| TableOracle::ssm_statistics(path).expect("statistics");
        let written = statistics(&ours);
        println!("{endian:?} as written: {written:?}");
        assert_eq!(written, statistics(&theirs), "{endian:?}: as written");
        assert_eq!(written.free_buckets, 0, "{endian:?}: as written");
        // The small index starts after its chain header, which names no
        // next bucket twice.
        assert_eq!(written.index_bucket_offset, 8, "{endian:?}: as written");
        assert_same_string_buckets(
            &ours,
            &theirs,
            written.last_string_bucket as usize,
            &format!("{endian:?}"),
        );
        let chain = |path: &Path| {
            let file = std::fs::read(path.join("table.f0")).expect("the SSM file");
            let start = SSM_HEADER_BYTES + written.first_index_bucket as usize * SSM_BUCKET_BYTES;
            file[start..start + 8].to_vec()
        };
        assert_eq!(chain(&ours), chain(&theirs), "{endian:?}: index chain");

        for path in [&ours, &theirs] {
            TableOracle::update_ssm_long_strings(path).expect("casacore updates and reads");
        }
        let rewritten = statistics(&ours);
        println!("{endian:?} as updated: {rewritten:?}");
        assert_eq!(rewritten, statistics(&theirs), "{endian:?}: as updated");
        // casacore rewrote the index into the other half of its bucket.
        assert_eq!(
            rewritten.first_index_bucket, written.first_index_bucket,
            "{endian:?}: the index moved"
        );
        // The new strings, over 300,000 bytes, need at least nine buckets
        // beyond the room the last one had; the file grew by fewer, so
        // casacore reused the buckets the shrinking freed.
        assert!(
            rewritten.buckets < written.buckets + 9,
            "{endian:?}: no buckets reused"
        );
        for path in [&ours, &theirs] {
            read_and_verify(&updated, ManagerKind::StandardStMan, path)
                .unwrap_or_else(|error| panic!("{endian:?} {}: {error}", path.display()));
        }
    }
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
        column_bindings: Vec::new(),
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

// --- Every element type in arrays stored directly, indirectly and by shape ---

/// The storages of the every-type StandardStMan fixture: fixed-shape arrays
/// in the buckets and in `table.f0i`, and variable-shape arrays, defined in
/// every row and not. casa-rs once read an undefined variable-shape cell as
/// a placeholder array (#852, undefined indirect cells).
const SSM_ELEMENT_STORAGES: [ElementStorage; 4] = [
    ElementStorage::Direct,
    ElementStorage::Indirect,
    ElementStorage::Variable,
    ElementStorage::Undefined,
];

fn ssm_element_arrays_fixture() -> TableFixture {
    element_arrays_fixture(&SSM_ELEMENT_STORAGES, CppTableFixture::SsmElementArrays)
}

#[test]
fn ssm_element_arrays_cross_matrix() {
    let fixture = ssm_element_arrays_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}

#[test]
fn ssm_element_arrays_endian_cross_matrix() {
    let fixture = ssm_element_arrays_fixture();
    assert_matrix_results(&run_endian_cross_matrix(
        &fixture,
        ManagerKind::StandardStMan,
    ));
}

/// The every-type table saved from generated array columns, per-row
/// closures or runs, which StandardStMan writes from its column sources,
/// reads in casacore as it does from row records.
#[test]
fn ssm_element_arrays_generated_save_reads_in_cpp() {
    if !casa_test_support::casacore_oracle_available() {
        eprintln!("skipping: C++ backend unavailable");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
        for runs in [false, true] {
            let path = dir.path().join(format!("{endian:?}-runs-{runs}"));
            save_generated_element_arrays(
                &path,
                &SSM_ELEMENT_STORAGES,
                DataManagerKind::StandardStMan,
                endian,
                runs,
            )
            .expect("save generated");
            if let Err(error) = TableOracle::table_verify(CppTableFixture::SsmElementArrays, &path)
            {
                panic!("{endian:?}, runs {runs}: C++ verify failed: {error}");
            }
        }
    }
}

// --- Post-mutation RR + RC tests ---

fn save_and_verify_mutation(
    table: &Table,
    dm: casa_tables::DataManagerKind,
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
    if casa_test_support::casacore_oracle_available() {
        TableOracle::table_verify(cpp_fixture, &path)
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
        casa_tables::DataManagerKind::StandardStMan,
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
        casa_tables::DataManagerKind::StandardStMan,
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
        casa_tables::DataManagerKind::StandardStMan,
        CppTableFixture::MutationRemovedRows,
        "mutation_rm_rows_ssm",
    );
}
