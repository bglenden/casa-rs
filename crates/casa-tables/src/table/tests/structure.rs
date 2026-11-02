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
fn the_names_of_renamed_columns_are_charged_to_the_retained_metadata() {
    let root = layered_table("rename_accounting");
    let mut table = open(&root);
    let plain = table.inner.cached_control_metadata_heap_bytes();
    let long = "a_very_long_column_name_".repeat(100);
    table
        .rename_column_in_place("id", &long)
        .expect("rename to a long name");
    let renamed = table.inner.cached_control_metadata_heap_bytes();
    // The name is held three times: in the description, the column set and
    // the map that remembers what the file calls the column (the old name is
    // only a few characters, and the first two were already charged).
    assert!(
        renamed >= plain + 3 * long.len(),
        "{plain} bytes before the rename, {renamed} after a rename to {} characters",
        long.len()
    );
    let retained = table
        .retained_read_metadata_bytes()
        .expect("modelled metadata");
    assert!(retained >= renamed);
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

/// A tiled manager is named by the header of its file, the NAME it was bound
/// with, and not by the group its column's description declares (CASA makes
/// such a table with `dminfo`: `getcoldesc` and `getdminfo` say the NAME).
#[test]
fn a_tiled_manager_is_named_by_its_header_not_by_the_declared_group() {
    let root = layered_table("tiled_name");
    let tiled_name = |root: &Path| {
        open(root)
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns == ["data"])
            .expect("the tiled manager")
            .name
            .clone()
    };
    let bound_with = tiled_name(&root);
    // The description now declares another group, as CASA lets it.
    let control = root.join(crate::storage::TABLE_CONTROL_FILE);
    let TableDatResult::Plain(mut dat) = read_table_dat_dispatch(&control).expect("table.dat")
    else {
        panic!("a plain table");
    };
    for column in &mut dat.table_desc.columns {
        if column.col_name == "data" {
            column.data_manager_group = "declared".to_string();
        }
    }
    crate::storage::table_control::write_table_dat(&control, &dat).expect("write table.dat");
    assert_ne!(bound_with, "declared");
    assert_eq!(tiled_name(&root), bound_with);
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

/// A `StManAipsIO` manager with a name of its own (casacore writes the one it
/// was bound with in the header of its file; casa-rs writes it empty) is not
/// a layout a full save keeps, so it is not offered one.
#[test]
fn a_stman_aipsio_manager_with_a_name_of_its_own_is_not_rewritten_in_full() {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .expect("push row");
    let root = unique_test_dir("named_stman");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::StManAipsIO)
                .with_endian_format(crate::table::EndianFormat::BigEndian),
        )
        .expect("save");
    assert!(
        open(&root)
            .single_stman_aipsio_options()
            .expect("options")
            .is_some()
    );
    assert_eq!(open(&root).data_manager_info()[0].name, "");

    // The header object: magic, its length, the type string "StManAipsIO"
    // (a length of 11 and the characters), the version, then the manager's
    // name, empty. Give it one: "mine".
    let file = root.join("table.f0");
    let bytes = std::fs::read(&file).expect("read the manager file");
    let length = u32::from_be_bytes(bytes[4..8].try_into().expect("length"));
    let name_at = 8 + 4 + 11 + 4;
    assert_eq!(bytes[name_at..name_at + 4], [0, 0, 0, 0], "an empty name");
    let mut named = bytes[..4].to_vec();
    named.extend((length + 4).to_be_bytes());
    named.extend(&bytes[8..name_at]);
    named.extend(4_u32.to_be_bytes());
    named.extend(b"mine");
    named.extend(&bytes[name_at + 4..]);
    std::fs::write(&file, named).expect("write the named manager file");

    let reopened = open(&root);
    assert_eq!(reopened.data_manager_info()[0].name, "mine");
    assert_eq!(
        reopened
            .column_accessor("id")
            .and_then(|column| column.scalar_cell(0))
            .expect("the table still reads"),
        &ScalarValue::Int32(7)
    );
    assert!(
        reopened
            .single_stman_aipsio_options()
            .expect("options")
            .is_none()
    );
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

#[test]
fn an_id_column_of_a_tiled_manager_cannot_be_renamed_in_place_but_a_data_column_can() {
    // `multi/big`: two data columns `a` and `b` and the id column `id` in one
    // TiledDataStMan. The id's name is also held in the manager's hypercube
    // values, which an edit of `table.dat` leaves behind.
    let root = unique_test_dir("rename_tiled_id");
    std::fs::create_dir_all(&root).expect("create test dir");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/tiled_data_stman/multi/big");
    for file in ["table.dat", "table.f0", "table.f0_TSM0", "table.info"] {
        std::fs::copy(fixture.join(file), root.join(file)).expect("copy fixture file");
    }
    let before = files(&root);
    let mut table = open(&root);
    let expected: Vec<Vec<f32>> = (0..table.row_count())
        .map(|row| cube(&table, "a", row))
        .collect();

    let refusal = table.rename_column_in_place("id", "iz");
    assert!(
        matches!(refusal, Err(TableError::Storage(_))),
        "{refusal:?}"
    );
    assert!(changed(&before, &files(&root)).is_empty());
    assert!(open(&root).column_accessor("id").is_ok());

    table
        .rename_column_in_place("a", "aa")
        .expect("rename a data column");
    assert_eq!(changed(&before, &files(&root)), ["table.dat"]);
    for table in [&table, &open(&root)] {
        for (row, want) in expected.iter().enumerate() {
            assert_eq!(&cube(table, "aa", row), want, "row {row}");
        }
        assert!(table.column_accessor("id").is_ok());
    }
    std::fs::remove_dir_all(&root).expect("cleanup test dir");
}

#[cfg(unix)]
mod locked {
    //! The in-place edits and a full save under the locks a table can be opened
    //! with (Astra's review of #1036, findings 1, 2 and 7).

    use super::*;
    use crate::lock::{LockMode, LockOptions, LockType};

    fn locked(root: &Path, mode: LockMode) -> Table {
        Table::open_with_lock(TableOptions::new(root), LockOptions::new(mode)).expect("open")
    }

    /// A table of one `StManAipsIO` group of three rows.
    fn small_table(prefix: &str) -> PathBuf {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("tag", PrimitiveType::Int32),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for row in 0..3 {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(row + 1))),
                    RecordField::new("tag", Value::Scalar(ScalarValue::Int32(100 + row))),
                ]))
                .expect("push row");
        }
        let root = unique_test_dir(prefix);
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO))
            .expect("save");
        // One lock period that writes, so that `table.lock` publishes the
        // three rows, as a table that has been written under a lock does.
        let mut writer = locked(&root, LockMode::AutoLocking);
        assert!(writer.lock(LockType::Write, 1).expect("lock"));
        writer
            .save_metadata_only(TableOptions::new(&root))
            .expect("metadata save");
        writer.unlock().expect("unlock");
        assert_eq!(crate::Table::rows_on_disk(&root).expect("rows"), 3);
        root
    }

    fn rows_in_table_dat(root: &Path) -> (u64, u64) {
        let TableDatResult::Plain(dat) =
            read_table_dat_dispatch(&root.join(crate::storage::TABLE_CONTROL_FILE))
                .expect("table.dat")
        else {
            panic!("a plain table");
        };
        (dat.nrrow, dat.column_set.nrrow)
    }

    /// A full save that removes rows, under the write lock a user or a
    /// permanent handle keeps, publishes the rows it left, so that no later
    /// write of the lock period raises `table.dat` back to the old count
    /// (casacore refuses a `table.dat` that counts rows its manager lacks).
    #[test]
    fn rows_removed_by_a_full_save_stay_removed_for_every_later_write_of_the_lock_period() {
        for mode in [LockMode::UserLocking, LockMode::PermanentLocking] {
            let root = small_table(&format!("shrink_{mode:?}"));
            let mut table = locked(&root, mode);
            if mode == LockMode::UserLocking {
                assert!(table.lock(LockType::Write, 1).expect("lock"));
            }
            table.remove_rows(&[2]).expect("remove the last row");
            table
                .save(TableOptions::new(&root).with_data_manager(DataManagerKind::StManAipsIO))
                .expect("full save");
            assert_eq!(rows_in_table_dat(&root), (2, 2), "{mode:?}");

            // A metadata-only save, as a keyword write makes.
            table
                .save_metadata_only(TableOptions::new(&root))
                .expect("metadata save");
            assert_eq!(rows_in_table_dat(&root), (2, 2), "{mode:?}: metadata save");
            // A reload.
            table.resync().expect("resync");
            assert_eq!(table.row_count(), 2, "{mode:?}: resync");
            // An in-place edit, which admits only a handle whose rows are the
            // disk's.
            table
                .rename_column_in_place("tag", "label")
                .expect("rename after a shrink");
            assert_eq!(rows_in_table_dat(&root), (2, 2), "{mode:?}: rename");
            assert_eq!(table.row_count(), 2);

            if mode == LockMode::UserLocking {
                table.unlock().expect("unlock");
            }
            drop(table);
            let reader = open(&root);
            assert_eq!(reader.row_count(), 2, "{mode:?}");
            assert_eq!(
                rows_in_table_dat(&root),
                (2, 2),
                "{mode:?}: after the release"
            );
            assert_eq!(ids_of(&reader, "label"), [100, 101], "{mode:?}");
            std::fs::remove_dir_all(&root).expect("cleanup test dir");
        }
    }

    fn ids_of(table: &Table, column: &str) -> Vec<i32> {
        (0..table.row_count())
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

    /// A refused edit changes no file whatever the lock, and leaves the handle
    /// as clean as it found it: releasing the lock must not write the
    /// MeasurementSet-like table in full, as `StManAipsIO`.
    #[test]
    fn a_refused_edit_leaves_a_locked_handle_clean_and_the_disk_as_it_was() {
        for mode in [
            LockMode::AutoLocking,
            LockMode::UserLocking,
            LockMode::PermanentLocking,
        ] {
            let root = layered_table(&format!("refused_{mode:?}"));
            let before = files(&root);
            let mut table = locked(&root, mode);
            if mode == LockMode::UserLocking {
                assert!(table.lock(LockType::Write, 1).expect("lock"));
            }
            assert!(table.rename_column_in_place("nosuch", "x").is_err());
            assert!(table.rename_column_in_place("id", "flag").is_err());
            assert!(table.remove_columns_in_place(&["flag"]).is_err());
            assert!(table.remove_columns_in_place(&["nosuch"]).is_err());
            assert!(!table.holds_unflushed_changes(), "{mode:?}");
            assert!(!table.inner.has_loaded_rows(), "{mode:?}");
            if mode == LockMode::UserLocking {
                table.unlock().expect("unlock");
            }
            drop(table);
            let after = files(&root);
            assert!(
                changed(&before, &after).is_empty(),
                "{mode:?}: {:?}",
                changed(&before, &after)
            );
            std::fs::remove_dir_all(&root).expect("cleanup test dir");
        }
    }

    /// Changes the handle held before a refused edit are still its own to
    /// write: the refusal clears only what it counted itself.
    #[test]
    fn a_refused_edit_keeps_the_changes_the_handle_already_held() {
        let root = layered_table("refused_dirty");
        let mut table = locked(&root, LockMode::UserLocking);
        assert!(table.lock(LockType::Write, 1).expect("lock"));
        table
            .keywords_mut()
            .push(RecordField::new("k", Value::Scalar(ScalarValue::Int32(1))));
        assert!(table.holds_unflushed_changes());
        assert!(table.rename_column_in_place("nosuch", "x").is_err());
        assert!(
            table.holds_unflushed_changes(),
            "the earlier change was lost"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// A failure after `table.dat` was written (a manager file that cannot be
    /// deleted) leaves the handle reloaded from what the disk now holds and
    /// clean, so that the release writes nothing in full.
    #[test]
    fn a_failure_after_the_write_leaves_the_handle_what_the_disk_holds_and_clean() {
        for mode in [LockMode::AutoLocking, LockMode::PermanentLocking] {
            let root = layered_table(&format!("failed_after_{mode:?}"));
            let mut table = locked(&root, mode);
            let tiled = table
                .data_manager_info()
                .iter()
                .find(|manager| manager.columns == ["data"])
                .expect("the tiled manager")
                .seq_nr;
            // A directory among the tiled manager's files: `remove_file` fails.
            std::fs::create_dir(root.join(format!("table.f{tiled}_TSM9"))).expect("plant");
            let failure = table.remove_columns_in_place(&["data"]);
            assert!(failure.is_err(), "{mode:?}");
            assert!(!table.holds_unflushed_changes(), "{mode:?}");
            // The disk has the column gone; so has the handle.
            assert!(
                open(&root)
                    .schema()
                    .expect("schema")
                    .column("data")
                    .is_none()
            );
            assert!(table.schema().expect("schema").column("data").is_none());
            let after_failure = files(&root);
            drop(table);
            assert!(
                changed(&after_failure, &files(&root)).is_empty(),
                "{mode:?}: the release rewrote {:?}",
                changed(&after_failure, &files(&root))
            );
            std::fs::remove_dir_all(&root).expect("cleanup test dir");
        }
    }

    /// A handle that takes a lock after another process renamed a column sees
    /// the storage managers as they now are, not as they were.
    #[test]
    fn a_lock_taken_after_another_handle_renamed_a_column_sees_the_new_managers() {
        let root = layered_table("lock_reload_managers");
        let mut reader = locked(&root, LockMode::UserLocking);
        assert!(
            reader
                .data_manager_info()
                .iter()
                .any(|manager| manager.columns == ["id"])
        );
        let mut writer = locked(&root, LockMode::AutoLocking);
        writer
            .rename_column_in_place("id", "identifier")
            .expect("rename");
        drop(writer);

        assert!(reader.lock(LockType::Read, 1).expect("lock"));
        let managers: Vec<_> = reader
            .data_manager_info()
            .iter()
            .flat_map(|manager| manager.columns.clone())
            .collect();
        assert!(managers.contains(&"identifier".to_string()), "{managers:?}");
        assert!(!managers.contains(&"id".to_string()), "{managers:?}");
        reader.unlock().expect("unlock");
        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }
}
