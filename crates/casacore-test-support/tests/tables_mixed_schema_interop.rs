// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wave 17: mixed-schema interop fixture combining scalar, fixed array,
//! variable array, record column, table keywords, and column keywords.

use casacore_tables::{ColumnOptions, ColumnSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{
    ManagerKind, MatrixCellResult, TableFixture, run_full_cross_matrix,
};
use casacore_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ShapeBuilder;

/// 2-row table with:
///   id       : Int32 scalar
///   flux     : Float64 scalar
///   spectrum : Float32 fixed array [4]
///   vis      : Float32 variable 2-D array
///   meta     : Record column
///
/// Table keywords: telescope="ALMA", version=3
/// Column keywords: flux: unit="Jy"
fn mixed_schema_fixture() -> TableFixture {
    let schema = casacore_tables::TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::array_fixed("spectrum", PrimitiveType::Float32, vec![4])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_variable("vis", PrimitiveType::Float32, Some(2)),
        ColumnSchema::record("meta"),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new(
                "spectrum",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![10.0, 20.0, 30.0, 40.0],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "vis",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[2, 3]).f(),
                        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "meta",
                Value::Record(RecordValue::new(vec![
                    RecordField::new("source", Value::Scalar(ScalarValue::String("CasA".into()))),
                    RecordField::new("priority", Value::Scalar(ScalarValue::Int32(1))),
                ])),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(2.7))),
            RecordField::new(
                "spectrum",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[4]),
                        vec![50.0, 60.0, 70.0, 80.0],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "vis",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array::from_shape_vec(
                        ndarray::IxDyn(&[3, 2]).f(),
                        vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "meta",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "source",
                    Value::Scalar(ScalarValue::String("CygA".into())),
                )])),
            ),
        ]),
    ];

    let table_keywords = RecordValue::new(vec![
        RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("ALMA".to_string())),
        ),
        RecordField::new("version", Value::Scalar(ScalarValue::Int32(3))),
    ]);

    let column_keywords = vec![(
        "flux".to_string(),
        RecordValue::new(vec![RecordField::new(
            "unit",
            Value::Scalar(ScalarValue::String("Jy".to_string())),
        )]),
    )];

    TableFixture {
        schema,
        rows,
        table_keywords,
        column_keywords,
        cpp_fixture: Some(CppTableFixture::MixedSchema),
        tile_shape: None,
    }
}

fn assert_matrix_results(results: &[MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[MixedSchema] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

#[test]
fn mixed_schema_cross_matrix() {
    let fixture = mixed_schema_fixture();
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}
