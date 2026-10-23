// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writes to a table whose `table.dat` trails its published rows (#880).
//!
//! casacore publishes added rows in `table.lock`'s sync data and rewrites
//! `table.dat` only at its next full write, at the latest when it closes the
//! table. CASA 6.7.6's simulator aborts that close after a mosaic predict,
//! leaving `table.dat` with fewer rows than its storage managers hold.
//! casa-rs opens such a table with the published rows. casacore refuses the table ("TiledStMan::headerFileGet: mismatch in
//! #row") once `table.dat`, or the sync data, gives a count its storage
//! managers do not hold. These laws hold every write to that standard:
//! - a partial save brings `table.dat` up to the rows on disk;
//! - a partial save refuses a handle whose rows were added or removed and
//!   not saved, and leaves the disk as it was;
//! - a metadata-only save repairs the count from the disk, whatever the
//!   handle holds in memory;
//! - a write lock that saw no rewrite of `table.dat` keeps the rows it
//!   found published, while a full save that removes rows publishes fewer.

use std::path::Path;

use casa_types::{ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, Value};

use super::table_array;
use crate::lock::{LockFile, SyncData, TableWriteLock};
use crate::schema::{ColumnSchema, TableSchema};
use crate::storage::table_control::{
    TableDatContents, TableDatResult, read_table_dat_dispatch, write_table_dat,
};
use crate::table::{DataManagerKind, Table, TableError, TableOptions};
use crate::{LockMode, LockOptions, LockType};

/// The rows the aborted process persisted in the storage managers and
/// published, and the rows its stale `table.dat` still gives.
const ROWS: u64 = 4;
const STALE_ROWS: u64 = 1;

fn cell(value: f32) -> Value {
    Value::Array(ArrayValue::Complex32(
        ndarray::ArrayD::from_shape_vec(vec![2, 2], vec![Complex32::new(value, 0.0); 4])
            .expect("2x2 cell"),
    ))
}

fn row(value: f32) -> RecordValue {
    RecordValue::new(vec![RecordField::new("DATA", cell(value))])
}

fn table_dat(root: &Path) -> TableDatContents {
    match read_table_dat_dispatch(&root.join(crate::storage::TABLE_CONTROL_FILE))
        .expect("read table.dat")
    {
        TableDatResult::Plain(contents) => contents,
        _ => panic!("a plain table"),
    }
}

/// `table.dat`'s two row counts, the Table header's and the ColumnSet's.
fn table_dat_rows(root: &Path) -> (u64, u64) {
    let contents = table_dat(root);
    (contents.nrrow, contents.column_set.nrrow)
}

fn published_rows(root: &Path) -> u64 {
    let lock = LockFile::create_or_open(root, false, 5.0, false).expect("table.lock");
    lock.read_sync_data()
        .expect("read sync")
        .expect("sync data")
        .nrrow
}

/// A `TiledShapeStMan` table at `root` left as CASA's simulator leaves an
/// MS: its storage managers hold [`ROWS`] rows and its sync data publishes
/// them, while `table.dat` gives [`STALE_ROWS`] in both counts.
fn stale_table(root: &Path) {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DATA",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for value in 0..ROWS {
        table.add_row(row(value as f32)).expect("append a row");
    }
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save the tiled table");
    let mut stale = table_dat(root);
    let managers = stale.column_set.data_managers.len();
    stale.nrrow = STALE_ROWS;
    stale.column_set.nrrow = STALE_ROWS;
    write_table_dat(&root.join(crate::storage::TABLE_CONTROL_FILE), &stale)
        .expect("write the stale table.dat");
    let lock = LockFile::create_or_open(root, false, 5.0, false).expect("table.lock");
    let mut sync = lock
        .read_sync_data()
        .expect("read sync")
        .unwrap_or_else(SyncData::new);
    sync.record_write(ROWS, 1, true, &vec![true; managers]);
    lock.write_sync_data(&sync).expect("publish the rows");
}

/// An unsaved change to the rows of an open table.
#[derive(Debug, Clone, Copy)]
enum Reshape {
    Append,
    Removal,
    RemovalThenAppend,
}

const RESHAPES: [Reshape; 3] = [
    Reshape::Append,
    Reshape::Removal,
    Reshape::RemovalThenAppend,
];

fn reshape(table: &mut Table, how: Reshape) {
    match how {
        Reshape::Append => table.add_row(row(9.0)).expect("append"),
        Reshape::Removal => table.remove_rows(&[3]).expect("remove row 3"),
        Reshape::RemovalThenAppend => {
            table.remove_rows(&[0]).expect("remove row 0");
            table.add_row(row(9.0)).expect("append");
        }
    }
}

/// The partial in-place saves: of selected rows, and of the two column
/// installations.
#[derive(Debug, Clone, Copy)]
enum PartialSave {
    SelectedRows,
    TiledColumnClone,
    TiledShapeColumn,
}

const PARTIAL_SAVES: [PartialSave; 3] = [
    PartialSave::SelectedRows,
    PartialSave::TiledColumnClone,
    PartialSave::TiledShapeColumn,
];

/// Run `how` on `table`, setting row 2 of DATA first for a selected-row
/// save and adding the installed column to the schema for an installation.
fn partial_save(table: &mut Table, how: PartialSave) -> Result<(), TableError> {
    let array_column =
        |name: &str| ColumnSchema::array_variable(name, PrimitiveType::Complex32, Some(2));
    match how {
        PartialSave::SelectedRows => {
            {
                let mut prepared = table.row_accessor_mut().prepare(&["DATA"])?;
                prepared.seek(2)?;
                prepared.set_value_at(0, cell(20.0))?;
            }
            table.prepare_write().save_selected_rows(&["DATA"], &[2])
        }
        PartialSave::TiledColumnClone => {
            table.add_column(array_column("MODEL"), None)?;
            table
                .prepare_write()
                .add_tiled_column_clone("DATA", "MODEL", "TiledModel")
        }
        PartialSave::TiledShapeColumn => {
            table.add_column(array_column("CORRECTED"), None)?;
            table
                .prepare_write()
                .add_tiled_shape_column("CORRECTED", &[], None)
        }
    }
}

/// Each partial save on the stale table, under casacore's write lock, brings
/// `table.dat` up to the rows on disk and publishes them, and the rows read
/// back. A selected-row save once wrote the stale count back (#880).
#[test]
fn a_partial_save_keeps_the_rows_a_stale_table_dat_lacks() {
    for how in PARTIAL_SAVES {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path();
        stale_table(root);
        let mut table = Table::open(TableOptions::new(root)).expect("open the stale table");
        assert_eq!(table.row_count() as u64, ROWS, "the published rows");
        let mut write_lock = TableWriteLock::acquire(root, 1).expect("casacore's write lock");
        write_lock.record_write();
        partial_save(&mut table, how).unwrap_or_else(|error| panic!("{how:?}: {error}"));
        write_lock.release().expect("publish the write");

        assert_eq!(table_dat_rows(root), (ROWS, ROWS), "{how:?}");
        assert_eq!(published_rows(root), ROWS, "{how:?}");
        let reopened = Table::open(TableOptions::new(root)).expect("reopen");
        assert_eq!(reopened.row_count() as u64, ROWS, "{how:?}");
        if matches!(how, PartialSave::SelectedRows) {
            let ArrayValue::Complex32(values) = table_array(&reopened, 2, "DATA").unwrap() else {
                panic!("DATA is complex")
            };
            assert_eq!(values[[0, 0]], Complex32::new(20.0, 0.0));
        }
    }
}

/// A partial save refuses a handle with unsaved row additions or removals,
/// a removal followed by an append among them, whose count matches the disk
/// while its rows do not, and writes nothing. The write lock, which recorded
/// a write before the refusal, then keeps the published rows: the save once
/// raised `table.dat` to this handle's count, rows that were never written.
#[test]
fn a_partial_save_refuses_unsaved_row_changes() {
    for how in PARTIAL_SAVES {
        for change in RESHAPES {
            let directory = tempfile::tempdir().expect("tempdir");
            let root = directory.path();
            stale_table(root);
            let mut table = Table::open(TableOptions::new(root)).expect("open the stale table");
            reshape(&mut table, change);
            let mut write_lock = TableWriteLock::acquire(root, 1).expect("casacore's write lock");
            write_lock.record_write();
            let refused = partial_save(&mut table, how)
                .expect_err(&format!("{how:?} after {change:?} must be refused"));
            assert!(
                refused.to_string().contains("rows were added or removed"),
                "{how:?} after {change:?}: {refused}"
            );
            write_lock.release().expect("publish the write");

            assert_eq!(
                table_dat_rows(root),
                (STALE_ROWS, STALE_ROWS),
                "{how:?} after {change:?} left table.dat as it was"
            );
            assert_eq!(published_rows(root), ROWS, "{how:?} after {change:?}");
            let reopened = Table::open(TableOptions::new(root)).expect("reopen");
            assert_eq!(
                reopened.row_count() as u64,
                ROWS,
                "{how:?} after {change:?}"
            );
        }
    }
}

/// A full save to the table's own directory makes its rows the disk's
/// again, so a partial save after it is admitted.
#[test]
fn a_partial_save_after_a_full_save_is_admitted() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path();
    stale_table(root);
    let mut table = Table::open(TableOptions::new(root)).expect("open the stale table");
    reshape(&mut table, Reshape::Append);
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save the whole table");
    partial_save(&mut table, PartialSave::SelectedRows).expect("save row 2 in place");
    assert_eq!(table_dat_rows(root), (ROWS + 1, ROWS + 1));
}

/// A metadata-only save writes no rows, so it is never refused: it brings
/// `table.dat` up to the rows the sync data publishes, never to the count
/// of a handle holding unsaved rows, which it once wrote.
#[test]
fn a_metadata_only_save_repairs_the_rows_from_the_disk() {
    for change in RESHAPES {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path();
        stale_table(root);
        let mut table = Table::open(TableOptions::new(root)).expect("open the stale table");
        reshape(&mut table, change);
        table.keywords_mut().upsert(
            "REPAIRED",
            Value::Scalar(casa_types::ScalarValue::Bool(true)),
        );
        let mut write_lock = TableWriteLock::acquire(root, 1).expect("casacore's write lock");
        write_lock.record_write();
        table
            .save_metadata_only(TableOptions::new(root))
            .expect("save the keywords");
        write_lock.release().expect("publish the write");

        assert_eq!(table_dat_rows(root), (ROWS, ROWS), "after {change:?}");
        assert_eq!(published_rows(root), ROWS, "after {change:?}");
        let reopened = Table::open(TableOptions::new(root)).expect("reopen");
        assert_eq!(reopened.row_count() as u64, ROWS, "after {change:?}");
        assert!(reopened.keywords().get("REPAIRED").is_some());
    }
}

/// A write lock released with a write recorded but `table.dat` untouched,
/// as by an empty or abandoned mutation session, keeps the rows published
/// before; it once published the stale file's count.
#[test]
fn a_write_lock_that_rewrote_nothing_keeps_the_published_rows() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path();
    stale_table(root);
    let mut write_lock = TableWriteLock::acquire(root, 1).expect("casacore's write lock");
    write_lock.record_write();
    write_lock.release().expect("publish the write");
    assert_eq!(table_dat_rows(root), (STALE_ROWS, STALE_ROWS));
    assert_eq!(published_rows(root), ROWS);
    let reopened = Table::open(TableOptions::new(root)).expect("reopen");
    assert_eq!(reopened.row_count() as u64, ROWS);
}

/// A full save that removes rows under the write lock rewrites `table.dat`,
/// and its fewer rows are published: keeping the rows published before
/// applies only when the file's bytes are unchanged.
#[test]
fn a_full_save_that_removes_rows_publishes_fewer() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path();
    stale_table(root);
    let mut table = Table::open(TableOptions::new(root)).expect("open the stale table");
    table.remove_rows(&[0, 1]).expect("remove two rows");
    let mut write_lock = TableWriteLock::acquire(root, 1).expect("casacore's write lock");
    write_lock.record_write();
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save the whole table");
    write_lock.release().expect("publish the write");
    assert_eq!(table_dat_rows(root), (ROWS - 2, ROWS - 2));
    assert_eq!(published_rows(root), ROWS - 2);
    let reopened = Table::open(TableOptions::new(root)).expect("reopen");
    assert_eq!(reopened.row_count() as u64, ROWS - 2);
}

/// A handle's own write lock keeps the published rows when its saves fail
/// before they rewrite `table.dat`: here every data file is a directory, so
/// the partial save fails, and so does the save the handle makes when it is
/// dropped with its change unsaved, before it publishes. The publication
/// once announced the stale file's count.
#[test]
fn a_locked_handle_whose_saves_fail_keeps_the_published_rows() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path();
    stale_table(root);
    let mut table = Table::open_with_lock(
        TableOptions::new(root),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open the stale table with locking");
    assert!(table.lock(LockType::Write, 1).expect("lock"));
    assert_eq!(table.row_count() as u64, ROWS);
    for entry in std::fs::read_dir(root).expect("list the table") {
        let path = entry.expect("table entry").path();
        let is_data_file = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("table.f"));
        if is_data_file && path.is_file() {
            std::fs::remove_file(&path).expect("remove a data file");
            std::fs::create_dir(&path).expect("replace it with a directory");
        }
    }
    assert!(
        partial_save(&mut table, PartialSave::SelectedRows).is_err(),
        "the save must fail"
    );
    drop(table);
    assert_eq!(published_rows(root), ROWS);
}
