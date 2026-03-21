// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task 1.7: Empty table cross-matrix tests — schema with columns but zero rows.
use casacore_tables::{ColumnOptions, ColumnSchema, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_test_support::table_interop::{ManagerKind, TableFixture, run_full_cross_matrix};
use casacore_types::{PrimitiveType, RecordValue};

fn empty_table_fixture(cpp_fixture: CppTableFixture) -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::array_fixed("arr_f32", PrimitiveType::Float32, vec![4])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
    ])
    .expect("schema");

    TableFixture {
        schema,
        rows: vec![],
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(cpp_fixture),
        tile_shape: None,
    }
}

fn assert_matrix_results(results: &[casacore_test_support::table_interop::MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[EmptyTable] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

#[test]
fn empty_table_aipsio_cross_matrix() {
    let fixture = empty_table_fixture(CppTableFixture::AipsioEmptyTable);
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StManAipsIO));
}

#[test]
fn empty_table_ssm_cross_matrix() {
    let fixture = empty_table_fixture(CppTableFixture::SsmEmptyTable);
    assert_matrix_results(&run_full_cross_matrix(&fixture, ManagerKind::StandardStMan));
}
