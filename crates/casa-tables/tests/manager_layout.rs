// SPDX-License-Identifier: LGPL-3.0-or-later
//! A table whose columns use different data managers, as a MeasurementSet's
//! do, writes each manager's data file as casacore expects.
//!
//! The C++ side of these laws is in casa-test-support's
//! `tables_cross_matrix_mixed_managers` (feature `cpp-interop-tests`).
#![cfg(unix)]

use std::collections::HashMap;
use std::path::Path;

use casa_aipsio::{AipsIo, AipsOpenOption};
use casa_tables::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};

/// Three rows: antenna in StandardStMan (the default, so data manager 0),
/// flux and a variable spectrum in StManAipsIO, time in IncrementalStMan,
/// and a variable complex data column in TiledShapeStMan.
fn mixed_table(path: &Path) {
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
            RecordValue::new(vec![
                RecordField::new("antenna", Value::Scalar(ScalarValue::Int32(r as i32))),
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(r as f64 + 1.5))),
                RecordField::new(
                    "spectrum",
                    Value::Array(ArrayValue::Float32(ndarray::ArrayD::from_elem(
                        ndarray::IxDyn(&[r + 1]),
                        r as f32,
                    ))),
                ),
                RecordField::new("time", Value::Scalar(ScalarValue::Float64(100.0))),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Complex32(ndarray::ArrayD::from_elem(
                        ndarray::IxDyn(&[2, 3]),
                        Complex32::new(r as f32, 1.0),
                    ))),
                ),
            ])
        })
        .collect();
    let binding = |data_manager, tile_shape| ColumnBinding {
        data_manager,
        tile_shape,
    };
    let bindings = HashMap::from([
        (
            "flux".to_string(),
            binding(DataManagerKind::StManAipsIO, None),
        ),
        (
            "spectrum".to_string(),
            binding(DataManagerKind::StManAipsIO, None),
        ),
        (
            "time".to_string(),
            binding(DataManagerKind::IncrementalStMan, None),
        ),
        (
            "data".to_string(),
            binding(DataManagerKind::TiledShapeStMan, Some(vec![2, 3, 2])),
        ),
    ]);
    let mut table = Table::from_rows_with_schema(rows, schema).expect("table");
    table
        .prepare_write()
        .save_with_bindings(
            TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("save with mixed managers");
}

/// casacore's `StManAipsIO::open` refuses a data file whose sequence number
/// is not its manager's in `table.dat` (#698). Beside StandardStMan,
/// StManAipsIO is not manager 0, and its file must say so.
#[test]
fn a_stmanaipsio_beside_other_managers_records_its_own_sequence_number() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("mixed");
    mixed_table(&path);
    let table = Table::open(TableOptions::new(&path)).expect("open");
    let aipsio = table
        .data_manager_info()
        .iter()
        .find(|manager| manager.dm_type == "StManAipsIO")
        .expect("a StManAipsIO manager")
        .seq_nr;
    assert_ne!(aipsio, 0, "the law needs StManAipsIO after another manager");
    let mut file = AipsIo::open(path.join(format!("table.f{aipsio}")), AipsOpenOption::Old)
        .expect("open the StManAipsIO file");
    file.getstart("StManAipsIO").expect("StManAipsIO object");
    file.get_string().expect("manager name");
    assert_eq!(file.get_u32().expect("sequence number"), aipsio);
}
