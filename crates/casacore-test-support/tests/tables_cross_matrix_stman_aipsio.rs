// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
};
use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ShapeBuilder;

fn scalar_primitives_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::ScalarPrimitives),
        tile_shape: None,
    }
}

fn fixed_array_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 3],
    )])
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
        cpp_fixture: Some(CppTableFixture::FixedArray),
        tile_shape: None,
    }
}

fn keywords_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::Keywords),
        tile_shape: None,
    }
}

fn assert_matrix_results(results: &[casacore_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[StManAipsIO] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

// Full 2x2 cross-matrix tests: RR, CC, CR, RC.
// CC/CR/RC are skipped if C++ casacore is unavailable.

#[test]
fn scalar_primitives_cross_matrix() {
    let fixture = scalar_primitives_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn fixed_array_cross_matrix() {
    let fixture = fixed_array_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn keywords_cross_matrix() {
    let fixture = keywords_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

// --- RR-only fixtures for expanded type coverage ---

fn all_numeric_scalars_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::AipsioAllNumericScalars),
        tile_shape: None,
    }
}

fn complex_scalars_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::AipsioComplexScalars),
        tile_shape: None,
    }
}

fn typed_arrays_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("arr_i32", PrimitiveType::Int32, vec![4]),
        ColumnSchema::array_fixed("arr_f64", PrimitiveType::Float64, vec![2, 2]),
        ColumnSchema::array_fixed("arr_f32", PrimitiveType::Float32, vec![3]),
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
                "arr_f32",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[3]), vec![1.5, 2.5, 3.5])
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
                "arr_f32",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[3]), vec![-1.5, -2.5, -3.5])
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
                "arr_f32",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(ndarray::IxDyn(&[3]), vec![0.0, 0.0, 0.0])
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
        cpp_fixture: Some(CppTableFixture::AipsioTypedArrays),
        tile_shape: None,
    }
}

fn column_keywords_fixture() -> TableFixture {
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
        cpp_fixture: Some(CppTableFixture::ColumnKeywords),
        tile_shape: None,
    }
}

/// Undefined scalars: 4 rows, only rows 0 and 2 written.
/// Rows 1 and 3 have default values (0, 0.0, "").
fn undefined_scalars_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(100))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("written".to_string())),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("col_str", Value::Scalar(ScalarValue::String(String::new()))),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(200))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(2.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("also_written".to_string())),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("col_str", Value::Scalar(ScalarValue::String(String::new()))),
        ]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::UndefinedScalars),
        tile_shape: None,
    }
}

#[test]
fn all_numeric_scalars_cross_matrix() {
    let fixture = all_numeric_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn complex_scalars_cross_matrix() {
    let fixture = complex_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn typed_arrays_cross_matrix() {
    let fixture = typed_arrays_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn column_keywords_cross_matrix() {
    let fixture = column_keywords_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn undefined_scalars_cross_matrix() {
    let fixture = undefined_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

// --- Explicit big-endian / little-endian cross-matrix tests ---
//
// StManAipsIO always stores column data in big-endian (canonical AipsIO), but
// the table.dat endian marker still varies. These tests verify that Rust
// correctly writes and reads tables with explicit BE/LE markers, and that
// C++ casacore can verify them (RC-BE, RC-LE).

#[test]
fn scalar_primitives_endian_cross_matrix() {
    let fixture = scalar_primitives_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn fixed_array_endian_cross_matrix() {
    let fixture = fixed_array_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn keywords_endian_cross_matrix() {
    let fixture = keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn all_numeric_scalars_endian_cross_matrix() {
    let fixture = all_numeric_scalars_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn complex_scalars_endian_cross_matrix() {
    let fixture = complex_scalars_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn typed_arrays_endian_cross_matrix() {
    let fixture = typed_arrays_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn column_keywords_endian_cross_matrix() {
    let fixture = column_keywords_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

// --- 3D fixed-array cross-matrix test ---

fn fixed_array_3d_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 3, 4],
    )])
    .expect("schema");

    // Values 1..24 and 25..48 in Fortran order
    let row0_vals: Vec<f32> = (1..=24).map(|v| v as f32).collect();
    let row1_vals: Vec<f32> = (25..=48).map(|v| v as f32).collect();

    let rows = vec![
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(ndarray::IxDyn(&[2, 3, 4]).f(), row0_vals).unwrap(),
            )),
        )]),
        RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float32(
                ndarray::Array::from_shape_vec(ndarray::IxDyn(&[2, 3, 4]).f(), row1_vals).unwrap(),
            )),
        )]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::Aipsio3DFixedArray),
        tile_shape: None,
    }
}

#[test]
fn fixed_array_3d_cross_matrix() {
    let fixture = fixed_array_3d_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn fixed_array_3d_endian_cross_matrix() {
    let fixture = fixed_array_3d_fixture();
    assert_matrix_results(&run_endian_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

// --- Post-mutation RR + RC tests ---
//
// These verify that mutation → save → reopen preserves data integrity (RR),
// and that C++ casacore can read the mutated tables (RC, when available).

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
fn mutation_add_column_aipsio() {
    let fixture = scalar_primitives_fixture();
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
        casacore_tables::DataManagerKind::StManAipsIO,
        CppTableFixture::MutationAddedColumn,
        "mutation_add_col_aipsio",
    );
}

#[test]
fn mutation_remove_column_aipsio() {
    let fixture = scalar_primitives_fixture();
    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table.remove_column("col_str").unwrap();

    assert_eq!(table.schema().unwrap().columns().len(), 3);
    save_and_verify_mutation(
        &table,
        casacore_tables::DataManagerKind::StManAipsIO,
        CppTableFixture::MutationRemovedColumn,
        "mutation_rm_col_aipsio",
    );
}

#[test]
fn mutation_remove_rows_aipsio() {
    let fixture = scalar_primitives_fixture();
    let mut table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();
    table.remove_rows(&[1]).unwrap();

    assert_eq!(table.row_count(), 2);
    save_and_verify_mutation(
        &table,
        casacore_tables::DataManagerKind::StManAipsIO,
        CppTableFixture::MutationRemovedRows,
        "mutation_rm_rows_aipsio",
    );
}

// --- Row-range stride tests ---
//
// Verify that reading a column with a strided RowRange produces the correct
// subset of rows. This uses C++-written data (when available) to verify
// cross-language stride behavior.

#[test]
fn stride_read_on_rust_written_table() {
    use casacore_tables::RowRange;

    let fixture = scalar_primitives_fixture();
    let table = Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stride_test");
    table
        .save(
            TableOptions::new(&path)
                .with_data_manager(casacore_tables::DataManagerKind::StManAipsIO),
        )
        .unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();

    // Read every other row (stride=2): should get rows 0 and 2
    let strided: Vec<_> = reopened
        .get_column_range("col_i32", RowRange::with_stride(0, 3, 2))
        .unwrap()
        .collect();
    assert_eq!(strided.len(), 2, "stride=2 should select 2 rows");
    assert_eq!(
        strided[0].value,
        Some(&Value::Scalar(ScalarValue::Int32(42))),
        "row 0"
    );
    assert_eq!(strided[0].row_index, 0);
    assert_eq!(
        strided[1].value,
        Some(&Value::Scalar(ScalarValue::Int32(0))),
        "row 2"
    );
    assert_eq!(strided[1].row_index, 2);

    // Read stride=1 (all rows)
    let all: Vec<_> = reopened
        .get_column_range("col_i32", RowRange::new(0, 3))
        .unwrap()
        .collect();
    assert_eq!(all.len(), 3, "stride=1 should select all 3 rows");

    // Read starting from row 1 with stride=1
    let tail: Vec<_> = reopened
        .get_column_range("col_i32", RowRange::new(1, 3))
        .unwrap()
        .collect();
    assert_eq!(tail.len(), 2, "offset range should select 2 rows");
    assert_eq!(
        tail[0].value,
        Some(&Value::Scalar(ScalarValue::Int32(-7))),
        "row 1"
    );
    assert_eq!(tail[0].row_index, 1);
}

#[test]
fn stride_read_on_cpp_written_table() {
    if !casacore_test_support::cpp_backend_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }
    use casacore_tables::RowRange;

    // Use C++ to write a scalar_primitives table (3 rows)
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_stride_test");
    casacore_test_support::cpp_table_write(CppTableFixture::ScalarPrimitives, &path)
        .expect("C++ write");

    let table = Table::open(TableOptions::new(&path)).unwrap();

    // Read every other row
    let strided: Vec<_> = table
        .get_column_range("col_i32", RowRange::with_stride(0, 3, 2))
        .unwrap()
        .collect();
    assert_eq!(strided.len(), 2, "stride=2 on C++-written table");
    assert_eq!(
        strided[0].value,
        Some(&Value::Scalar(ScalarValue::Int32(42))),
        "row 0"
    );
    assert_eq!(
        strided[1].value,
        Some(&Value::Scalar(ScalarValue::Int32(0))),
        "row 2"
    );
}

// ---------------------------------------------------------------------------
// Record columns (Wave 16)
// ---------------------------------------------------------------------------

fn record_column_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::record("meta"),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(
                "meta",
                Value::Record(RecordValue::new(vec![
                    RecordField::new("unit", Value::Scalar(ScalarValue::String("Jy".into()))),
                    RecordField::new("value", Value::Scalar(ScalarValue::Float64(2.5))),
                ])),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new(
                "meta",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "flag",
                    Value::Scalar(ScalarValue::Bool(true)),
                )])),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(3))),
            RecordField::new("meta", Value::Record(RecordValue::default())),
        ]),
    ];

    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::AipsIORecordColumn),
        tile_shape: None,
    }
}

#[test]
fn record_column_cross_matrix() {
    let fixture = record_column_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn record_column_round_trip_values() {
    let fixture = record_column_fixture();
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().join("record_test.tab");

    let table =
        Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone()).expect("build");
    table.save(TableOptions::new(&path)).expect("save");
    drop(table);

    let table = Table::open(TableOptions::new(&path)).expect("open");
    assert_eq!(table.rows().len(), 3);

    // Read record cells via the record column API.
    let records: Vec<_> = table.get_record_column("meta").unwrap().collect();
    assert_eq!(records.len(), 3);

    // Row 0: {unit: "Jy", value: 2.5}
    let r0 = &records[0].value;
    assert_eq!(
        r0.get("unit"),
        Some(&Value::Scalar(ScalarValue::String("Jy".into())))
    );
    assert_eq!(
        r0.get("value"),
        Some(&Value::Scalar(ScalarValue::Float64(2.5)))
    );

    // Row 1: {flag: true}
    let r1 = &records[1].value;
    assert_eq!(
        r1.get("flag"),
        Some(&Value::Scalar(ScalarValue::Bool(true)))
    );

    // Row 2: empty record
    let r2 = &records[2].value;
    assert!(r2.fields().is_empty());
}

// ---------------------------------------------------------------------------
// Extended CR tests (Waves 23 + 24)
// ---------------------------------------------------------------------------

/// CR test: verify `get_cell_slice()` works on C++-written fixed array data.
#[test]
fn cr_slice_on_cpp_written_fixed_array() {
    if !casacore_test_support::cpp_backend_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }
    use casacore_tables::Slicer;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_slice_test");
    casacore_test_support::cpp_table_write(CppTableFixture::FixedArray, &path)
        .expect("C++ write FixedArray");

    let table = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(table.row_count(), 3);

    // The FixedArray fixture has shape [2,3] with F-order data.
    // Row 0: [1,2,3,4,5,6] in F-order → [0,0]=1, [1,0]=2, [0,1]=3, [1,1]=4, [0,2]=5, [1,2]=6
    // Slice [0..2, 0..2] should give a [2,2] sub-array: [1,2,3,4] in F-order
    let slicer = Slicer::contiguous(vec![0, 0], vec![2, 2]).unwrap();
    let slice = table.get_cell_slice("data", 0, &slicer).unwrap();

    match &slice {
        Value::Array(ArrayValue::Float32(arr)) => {
            assert_eq!(arr.shape(), &[2, 2], "slice shape mismatch");
            // In F-order: [0,0]=1, [1,0]=2, [0,1]=3, [1,1]=4
            assert_eq!(arr[[0, 0]], 1.0);
            assert_eq!(arr[[1, 0]], 2.0);
            assert_eq!(arr[[0, 1]], 3.0);
            assert_eq!(arr[[1, 1]], 4.0);
        }
        other => panic!("expected Float32 array, got {:?}", other),
    }

    // Also test a single-element slice on row 1
    let slicer1 = Slicer::contiguous(vec![1, 2], vec![2, 3]).unwrap();
    let slice1 = table.get_cell_slice("data", 1, &slicer1).unwrap();
    match &slice1 {
        Value::Array(ArrayValue::Float32(arr)) => {
            assert_eq!(arr.shape(), &[1, 1]);
            // Row 1 F-order data: [7,8,9,10,11,12] → [1,2]=12.0
            assert_eq!(arr[[0, 0]], 12.0);
        }
        other => panic!("expected Float32 array, got {:?}", other),
    }
}

/// CR test: verify `data_manager_info()` on a C++-written table.
#[test]
fn cr_data_manager_info_on_cpp_table() {
    if !casacore_test_support::cpp_backend_available() {
        eprintln!("skipping: C++ casacore unavailable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cr_dm_info");
    casacore_test_support::cpp_table_write(CppTableFixture::ScalarPrimitives, &path)
        .expect("C++ write ScalarPrimitives");

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let dm_info = table.data_manager_info();
    assert!(!dm_info.is_empty(), "data_manager_info should be non-empty");

    // StManAipsIO fixture should have exactly one DM
    let dm = &dm_info[0];
    assert!(
        dm.dm_type.contains("StManAipsIO"),
        "DM type should contain 'StManAipsIO', got '{}'",
        dm.dm_type
    );
    assert!(
        !dm.columns.is_empty(),
        "DM should manage at least one column"
    );

    // Verify all expected columns are managed
    let expected_cols = ["col_bool", "col_i32", "col_f64", "col_str"];
    for col in &expected_cols {
        assert!(
            dm.columns.iter().any(|c| c == col),
            "column '{}' should be managed by the DM, found {:?}",
            col,
            dm.columns
        );
    }
}
