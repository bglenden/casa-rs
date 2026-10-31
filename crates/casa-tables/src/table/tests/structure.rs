// SPDX-License-Identifier: LGPL-3.0-or-later
//! Structural changes made in place on disk (`table/structure.rs`).
//!
//! A column is renamed or removed in `table.dat` alone, and the files of a
//! storage manager that goes away are deleted; every other storage manager
//! file stays as it is and no row is loaded. These laws hold the changes to
//! that, on a table of three managers (as a MeasurementSet is): `id` in a
//! `StandardStMan`, `flag` and `tag` in a `StManAipsIO`, `data` tiled.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use super::{table_array, unique_test_dir};
use crate::schema::{ColumnSchema, TableSchema};
use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
use crate::table::{ColumnBinding, DataManagerKind, Table, TableError, TableOptions};

const ROWS: usize = 3;

/// A table of three managers on disk, and its directory.
fn layered_table(prefix: &str) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
        ColumnSchema::scalar("tag", PrimitiveType::Int32),
        ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![2, 3]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row in 0..ROWS {
        let cube: Vec<f32> = (0..6).map(|cell| (row * 10 + cell) as f32).collect();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(row as i32 + 1))),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool(row % 2 == 0))),
                RecordField::new("tag", Value::Scalar(ScalarValue::Int32(100 + row as i32))),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float32(
                        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 3]), cube)
                            .expect("2x3 cell"),
                    )),
                ),
            ]))
            .expect("push row");
    }
    let root = unique_test_dir(prefix);
    std::fs::create_dir_all(&root).expect("create test dir");
    let bindings = HashMap::from([
        (
            "id".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::StandardStMan,
                tile_shape: None,
            },
        ),
        (
            "data".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::TiledColumnStMan,
                tile_shape: None,
            },
        ),
    ]);
    table
        .save_with_bindings(
            TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO),
            &bindings,
        )
        .expect("save with bindings");
    root
}

/// The bytes of every file of the table directory, the lock file apart.
fn files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(root)
        .expect("list the table")
        .map(|entry| entry.expect("entry"))
        .filter(|entry| entry.path().is_file() && entry.file_name() != "table.lock")
        .map(|entry| {
            (
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path()).expect("read a file"),
            )
        })
        .collect()
}

/// The names of the files that differ between `before` and `after`,
/// including one that is gone or new.
fn changed(before: &BTreeMap<String, Vec<u8>>, after: &BTreeMap<String, Vec<u8>>) -> Vec<String> {
    before
        .keys()
        .chain(after.keys())
        .filter(|name| before.get(*name) != after.get(*name))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn open(root: &Path) -> Table {
    Table::open(TableOptions::new(root)).expect("open")
}

fn ids(table: &Table, column: &str) -> Vec<i32> {
    (0..ROWS)
        .map(|row| {
            match table
                .column_accessor(column)
                .and_then(|c| c.scalar_cell(row))
            {
                Ok(ScalarValue::Int32(value)) => *value,
                other => panic!("an Int32 cell, got {other:?}"),
            }
        })
        .collect()
}

fn cube(table: &Table, column: &str, row: usize) -> Vec<f32> {
    match table_array(table, row, column).expect("cube") {
        ArrayValue::Float32(array) => array.iter().copied().collect(),
        other => panic!("a Float32 array, got {other:?}"),
    }
}

fn hypercolumn_data(root: &Path) -> Vec<String> {
    let TableDatResult::Plain(dat) =
        read_table_dat_dispatch(&root.join(crate::storage::TABLE_CONTROL_FILE)).expect("table.dat")
    else {
        panic!("a plain table");
    };
    let mut names = Vec::new();
    for field in dat.table_desc.private_keywords.fields() {
        if let Value::Record(definition) = &field.value
            && let Some(Value::Array(ArrayValue::String(data))) = definition.get("data")
        {
            names.extend(data.iter().cloned());
        }
    }
    names
}

#[test]
fn a_column_renamed_in_place_changes_table_dat_alone_and_loads_no_row() {
    let root = layered_table("rename_in_place");
    let before = files(&root);
    let mut table = open(&root);

    table
        .rename_column_in_place("id", "number")
        .expect("rename a standard column");
    table
        .rename_column_in_place("data", "cube")
        .expect("rename a tiled column");
    table
        .rename_column_in_place("flag", "on")
        .expect("rename a column of a shared manager");

    assert_eq!(changed(&before, &files(&root)), ["table.dat"]);
    assert!(
        !table.inner.has_loaded_rows(),
        "a rename must not load the table"
    );
    assert!(table.schema().expect("schema").column("id").is_none());
    for table in [&table, &open(&root)] {
        assert_eq!(ids(table, "number"), [1, 2, 3]);
        assert_eq!(ids(table, "tag"), [100, 101, 102]);
        assert_eq!(cube(table, "cube", 2), [20.0, 21.0, 22.0, 23.0, 24.0, 25.0]);
        assert!(table.column_accessor("id").is_err());
    }
    let names: Vec<_> = hypercolumn_data(&root);
    assert!(!names.contains(&"data".to_string()), "{names:?}");
    assert!(names.is_empty() || names == ["cube"], "{names:?}");
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn a_column_renamed_twice_keeps_the_name_its_manager_knows_it_by() {
    let root = layered_table("rename_twice");
    let mut table = open(&root);
    table
        .rename_column_in_place("id", "first")
        .expect("first rename");
    table
        .rename_column_in_place("first", "second")
        .expect("second rename");
    let reopened = open(&root);
    assert_eq!(ids(&reopened, "second"), [1, 2, 3]);
    let TableDatResult::Plain(dat) =
        read_table_dat_dispatch(&root.join(crate::storage::TABLE_CONTROL_FILE)).expect("table.dat")
    else {
        panic!("a plain table");
    };
    assert_eq!(
        dat.names_on_file,
        HashMap::from([("second".to_string(), "id".to_string())])
    );

    // Back to the name it was created with: nothing left to remember.
    table
        .rename_column_in_place("second", "id")
        .expect("rename back");
    let TableDatResult::Plain(dat) =
        read_table_dat_dispatch(&root.join(crate::storage::TABLE_CONTROL_FILE)).expect("table.dat")
    else {
        panic!("a plain table");
    };
    assert!(dat.names_on_file.is_empty(), "{:?}", dat.names_on_file);
    assert_eq!(ids(&open(&root), "id"), [1, 2, 3]);
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn a_refused_rename_leaves_the_disk_as_it_was() {
    let root = layered_table("rename_refused");
    let before = files(&root);
    let mut table = open(&root);
    assert!(matches!(
        table.rename_column_in_place("nosuch", "x"),
        Err(TableError::SchemaColumnUnknown { .. })
    ));
    assert!(table.rename_column_in_place("id", "flag").is_err());
    assert!(changed(&before, &files(&root)).is_empty());
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn removing_the_columns_of_whole_managers_in_place_deletes_their_files_only() {
    let root = layered_table("remove_in_place");
    let before = files(&root);
    let mut table = open(&root);
    let tiled = table
        .data_manager_info()
        .iter()
        .find(|manager| manager.columns == ["data"])
        .expect("the tiled manager")
        .seq_nr;
    let standard = table
        .data_manager_info()
        .iter()
        .find(|manager| manager.columns == ["id"])
        .expect("the standard manager")
        .seq_nr;

    table
        .remove_columns_in_place(&["data", "id"])
        .expect("remove two whole managers");

    let after = files(&root);
    let touched = changed(&before, &after);
    for name in &touched {
        assert!(
            name == "table.dat"
                || name.starts_with(&format!("table.f{tiled}"))
                || name.starts_with(&format!("table.f{standard}")),
            "{name} changed"
        );
        if name != "table.dat" {
            assert!(!after.contains_key(name), "{name} should be gone");
        }
    }
    assert!(touched.len() > 1, "{touched:?}");
    assert!(
        !table.inner.has_loaded_rows(),
        "a removal must not load the table"
    );
    for table in [&table, &open(&root)] {
        assert_eq!(table.schema().expect("schema").columns().len(), 2);
        assert_eq!(ids(table, "tag"), [100, 101, 102]);
    }
    assert!(
        hypercolumn_data(&root).is_empty(),
        "{:?}",
        hypercolumn_data(&root)
    );
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[test]
fn a_column_whose_manager_keeps_others_cannot_be_removed_in_place() {
    let root = layered_table("remove_shared");
    let before = files(&root);
    let mut table = open(&root);
    // `flag` and `tag` share one StManAipsIO file; removing only `flag`
    // would leave `tag` without a manager that holds it alone.
    let refusal = table.remove_columns_in_place(&["flag"]);
    assert!(
        matches!(refusal, Err(TableError::Storage(_))),
        "{refusal:?}"
    );
    assert!(table.remove_columns_in_place(&["nosuch"]).is_err());
    assert!(changed(&before, &files(&root)).is_empty());
    assert_eq!(ids(&open(&root), "tag"), [100, 101, 102]);

    // Both together are the whole manager.
    table
        .remove_columns_in_place(&["flag", "tag"])
        .expect("remove a whole manager");
    assert_eq!(open(&root).schema().expect("schema").columns().len(), 2);
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}
