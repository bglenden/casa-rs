// SPDX-License-Identifier: LGPL-3.0-or-later
//! A table whose columns use different data managers, as a MeasurementSet's
//! do, keeps that layout through saves, unlocks and drops (ADR-0008).
//!
//! The C++ side of these laws is in casa-test-support's
//! `tables_cross_matrix_mixed_managers` (feature `cpp-interop-tests`).
#![cfg(unix)]

use std::collections::HashMap;
use std::path::Path;

use casa_aipsio::{AipsIo, AipsOpenOption};
use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, LockMode, LockOptions, LockType, Table,
    TableOptions, TableSchema,
};
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

/// Each column's manager type, sorted by column.
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

fn flux(path: &Path, row: usize) -> f64 {
    let table = Table::open(TableOptions::new(path)).expect("open");
    let cell = table
        .cell_accessor(row, "flux")
        .and_then(|cell| cell.value())
        .expect("read flux");
    let Some(Value::Scalar(ScalarValue::Float64(value))) = cell else {
        panic!("flux is a Float64 scalar");
    };
    *value
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

/// Open with user locking, take the write lock and change row 0's flux.
fn locked_with_a_change(path: &Path) -> Table {
    let mut table = Table::open_with_lock(
        TableOptions::new(path),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open with lock");
    assert!(table.lock(LockType::Write, 1).expect("write lock"));
    table
        .row_accessor_mut()
        .set_cell(0, "flux", Value::Scalar(ScalarValue::Float64(9.25)))
        .expect("change a cell");
    table
}

/// Unlocking or dropping a write-locked table with a pending change keeps
/// every column in its manager and the tiled column's tiles, and persists
/// the change (#697).
#[test]
fn unlocking_or_dropping_a_locked_table_keeps_each_columns_manager() {
    for (label, unlock) in [("unlock", true), ("drop", false)] {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join(label);
        mixed_table(&path);
        let before = managers(&path);
        let tile_bytes = |path: &Path| {
            Table::open(TableOptions::new(path))
                .expect("open")
                .array_column_tile_bytes("data")
                .expect("tile bytes")
        };
        let tiles = tile_bytes(&path);
        let mut table = locked_with_a_change(&path);
        if unlock {
            table.unlock().expect("unlock");
        }
        drop(table);
        assert_eq!(managers(&path), before, "{label} changed the managers");
        assert_eq!(tile_bytes(&path), tiles, "{label} changed the data tiles");
        assert_eq!(flux(&path, 0), 9.25, "{label} lost the change");
        assert_no_orphan_data_files(&path);
    }
}

/// Every data file (`table.f<N>…`) belongs to a manager `table.dat` lists:
/// a rewrite that renumbers managers leaves none of the old ones behind.
fn assert_no_orphan_data_files(path: &Path) {
    let table = Table::open(TableOptions::new(path)).expect("open");
    let managers = table
        .data_manager_info()
        .iter()
        .map(|manager| manager.seq_nr)
        .collect::<Vec<_>>();
    for entry in std::fs::read_dir(path).expect("list the table") {
        let name = entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .into_owned();
        let Some(rest) = name.strip_prefix("table.f") else {
            continue;
        };
        let number = rest
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse::<u32>()
            .expect("a data file names its manager");
        assert!(
            managers.contains(&number),
            "{name} belongs to no manager of {managers:?}"
        );
    }
}

/// A column added under the lock takes the open options' manager; the
/// columns already on disk keep theirs.
#[test]
fn a_column_added_under_the_lock_leaves_the_others_in_their_managers() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("added");
    mixed_table(&path);
    let mut expected = managers(&path);
    let mut table = Table::open_with_lock(
        TableOptions::new(&path),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open with lock");
    assert!(table.lock(LockType::Write, 1).expect("write lock"));
    table
        .add_column(
            ColumnSchema::scalar("weight", PrimitiveType::Float32),
            Some(Value::Scalar(ScalarValue::Float32(1.0))),
        )
        .expect("add a column");
    table.unlock().expect("unlock");
    drop(table);
    let found = managers(&path);
    let weight = found
        .iter()
        .find(|(column, _)| column == "weight")
        .expect("the added column is stored")
        .1
        .clone();
    expected.push(("weight".to_string(), weight));
    expected.sort();
    assert_eq!(found, expected, "the existing columns kept their managers");
    assert_no_orphan_data_files(&path);
}
