// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_tables::{ColumnOptions, ColumnSchema, TableSchema};
use casa_test_support::CppTableFixture;
use casa_test_support::table_interop::{ManagerKind, TableFixture, run_full_cross_matrix};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn assert_matrix_results(results: &[casa_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[StManAipsIO undefined] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

/// Keep undefined-scalar cross-matrix coverage in its own integration binary.
///
/// The C++ StManAipsIO undefined-cell fixture is stable in isolation, but
/// running it in-process alongside the other AipsIO matrix fixtures leaves
/// casacore state dirty enough that `CC`/`CR` become order-dependent.
fn undefined_scalars_fixture() -> TableFixture {
    let undefined = ColumnOptions {
        direct: false,
        undefined: true,
    };
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32)
            .with_options(undefined)
            .expect("undefined scalar"),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64)
            .with_options(undefined)
            .expect("undefined scalar"),
        ColumnSchema::scalar("col_str", PrimitiveType::String)
            .with_options(undefined)
            .expect("undefined scalar"),
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
        RecordValue::new(vec![]),
        RecordValue::new(vec![
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(200))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(2.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("also_written".to_string())),
            ),
        ]),
        RecordValue::new(vec![]),
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
fn undefined_scalars_cross_matrix() {
    let fixture = undefined_scalars_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}
