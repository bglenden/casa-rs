// SPDX-License-Identifier: LGPL-3.0-or-later
//! A table whose columns use different data managers, as a MeasurementSet's
//! do, written and read by casa-rs and C++ casacore in every direction.
//!
//! StandardStMan holds the first column, so StManAipsIO is not data manager
//! 0: casacore checks a StManAipsIO file's sequence number against the
//! table's (#698). A write-locked table unlocked or dropped with a pending
//! change keeps every column in its manager (#697).
#![cfg(feature = "cpp-interop-tests")]

use std::path::Path;

use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, LockMode, LockOptions, LockType, Table,
    TableOptions, TableSchema,
};
use casa_test_support::table_interop::{
    ManagerKind, MatrixCellResult, TableFixture, run_full_cross_matrix,
};
use casa_test_support::{CppTableFixture, TableOracle, casacore_oracle_available};
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

/// Each column's manager type, in column order.
fn managers(path: &Path) -> Vec<(String, String)> {
    let table = Table::open(TableOptions::new(path)).expect("open");
    let mut found = table
        .data_manager_info()
        .iter()
        .flat_map(|manager| {
            manager
                .columns
                .iter()
                .map(|column| (column.clone(), manager.dm_type.clone()))
        })
        .collect::<Vec<_>>();
    found.sort();
    found
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

/// Open `path` with user locking, take the write lock and rewrite one cell
/// with its own value, leaving a change pending.
fn locked_with_a_pending_change(path: &Path) -> Table {
    let mut table = Table::open_with_lock(
        TableOptions::new(path),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open with lock");
    assert!(table.lock(LockType::Write, 1).expect("write lock"));
    table
        .row_accessor_mut()
        .set_cell(0, "flux", Value::Scalar(ScalarValue::Float64(1.5)))
        .expect("change a cell");
    table
}

#[test]
fn unlocking_or_dropping_a_locked_table_keeps_each_columns_manager() {
    let fixture = mixed_managers_fixture();
    for (label, unlock) in [("unlock", true), ("drop", false)] {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join(label);
        rust_write(&fixture, &path);
        let before = managers(&path);
        let mut table = locked_with_a_pending_change(&path);
        if unlock {
            table.unlock().expect("unlock");
        }
        drop(table);
        assert_eq!(
            managers(&path),
            before,
            "{label} changed the table's managers"
        );
        if casacore_oracle_available() {
            TableOracle::table_verify(CppTableFixture::MixedManagers, &path)
                .unwrap_or_else(|error| panic!("{label}: C++ verify failed: {error}"));
        }
    }
}
