// SPDX-License-Identifier: LGPL-3.0-or-later
//! A table whose columns use different data managers, as a MeasurementSet's
//! do, written and read by casa-rs and C++ casacore in every direction.
//!
//! StandardStMan holds the first column, so StManAipsIO is not data manager
//! 0: casacore checks a StManAipsIO file's sequence number against the
//! table's (#698).
#![cfg(feature = "cpp-interop-tests")]

use std::path::Path;

use casa_tables::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_test_support::table_interop::{
    ManagerKind, MatrixCellResult, TableFixture, run_endian_cross_matrix, run_full_cross_matrix,
};
use casa_test_support::{CppTableFixture, casacore_oracle_available};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};

const TIMES: [f64; 3] = [100.0, 100.0, 110.0];

/// The fixture `CppTableFixture::MixedManagers` writes and verifies.
fn mixed_managers_fixture() -> TableFixture {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("antenna", PrimitiveType::Int32),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::array_variable("spectrum", PrimitiveType::Float32, Some(1)),
        ColumnSchema::scalar("time", PrimitiveType::Float64),
        ColumnSchema::array_variable("data", PrimitiveType::Complex32, Some(2)),
    ])
    .expect("schema");
    let rows = (0..3)
        .map(|r: usize| {
            let spectrum = (0..=r).map(|k| (r * 10 + k) as f32).collect::<Vec<_>>();
            let data = (0..6)
                .map(|k| Complex32::new((r * 10 + k) as f32, -(k as f32)))
                .collect::<Vec<_>>();
            RecordValue::new(vec![
                RecordField::new("antenna", Value::Scalar(ScalarValue::Int32(r as i32))),
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(r as f64 + 1.5))),
                RecordField::new(
                    "spectrum",
                    Value::Array(ArrayValue::Float32(
                        ndarray::Array::from_shape_vec(ndarray::IxDyn(&[r + 1]), spectrum)
                            .expect("spectrum shape"),
                    )),
                ),
                RecordField::new("time", Value::Scalar(ScalarValue::Float64(TIMES[r]))),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Complex32(
                        ndarray::Array::from_shape_vec(
                            ndarray::ShapeBuilder::f(ndarray::IxDyn(&[2, 3])),
                            data,
                        )
                        .expect("data shape"),
                    )),
                ),
            ])
        })
        .collect();
    let binding = |data_manager, tile_shape: Option<Vec<usize>>| ColumnBinding {
        data_manager,
        tile_shape,
    };
    TableFixture {
        schema,
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(CppTableFixture::MixedManagers),
        tile_shape: None,
        column_bindings: vec![
            (
                "antenna".into(),
                binding(DataManagerKind::StandardStMan, None),
            ),
            ("flux".into(), binding(DataManagerKind::StManAipsIO, None)),
            (
                "spectrum".into(),
                binding(DataManagerKind::StManAipsIO, None),
            ),
            (
                "time".into(),
                binding(DataManagerKind::IncrementalStMan, None),
            ),
            (
                "data".into(),
                binding(DataManagerKind::TiledShapeStMan, Some(vec![2, 3, 2])),
            ),
        ],
    }
}

fn assert_passed(results: &[MatrixCellResult]) {
    for result in results {
        assert!(
            result.passed,
            "[MixedManagers] {}: {}",
            result.label,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}

/// Save the fixture with casa-rs: StandardStMan by default, the bindings
/// for the rest.
fn rust_write(fixture: &TableFixture, path: &Path) {
    let mut table = Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone())
        .expect("table from the fixture");
    let bindings = fixture.column_bindings.iter().cloned().collect();
    table
        .prepare_write()
        .save_with_bindings(
            TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("save with mixed managers");
}

#[test]
fn a_mixed_manager_table_crosses_between_rust_and_cpp() {
    let fixture = mixed_managers_fixture();
    // The precondition #698 needs: StManAipsIO is not data manager 0.
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("layout");
    rust_write(&fixture, &path);
    let table = Table::open(TableOptions::new(&path)).expect("open");
    let aipsio = table
        .data_manager_info()
        .iter()
        .find(|manager| manager.dm_type == "StManAipsIO")
        .expect("a StManAipsIO manager");
    assert_ne!(
        aipsio.seq_nr, 0,
        "the fixture must put StManAipsIO after another manager"
    );
    drop(table);

    let results = run_full_cross_matrix(&fixture, ManagerKind::StandardStMan);
    if casacore_oracle_available() {
        assert_eq!(results.len(), 4, "RR, CC, CR and RC");
    }
    assert_passed(&results);
}

/// The same table written big- and little-endian. The other managers follow
/// the table's byte order; StManAipsIO's main file, sequence number
/// included, stays canonical (big-endian) either way, as casacore's AipsIO
/// writes it.
#[test]
fn a_mixed_manager_table_crosses_in_either_byte_order() {
    let results = run_endian_cross_matrix(&mixed_managers_fixture(), ManagerKind::StandardStMan);
    if casacore_oracle_available() {
        assert_eq!(results.len(), 4, "RR and RC, big- and little-endian");
    }
    assert_passed(&results);
}
