// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a handle that keeps its lock publishes (`Table::publish_writes`),
//! reads of the published modify counter (`Table::published_modify_counter`)
//! and the refresh of a handle that holds a read lock
//! (`Table::resync_locked`).

use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use super::unique_test_dir;
use crate::schema::{ColumnSchema, TableSchema};
use crate::table::{DataManagerKind, Table, TableError, TableOptions};
use crate::{LockMode, LockOptions, LockType};

/// A table of one row on disk, and the options that open it.
fn one_row_table(prefix: &str) -> TableOptions {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .expect("push row");
    let root = unique_test_dir(prefix);
    std::fs::create_dir_all(&root).expect("create test dir");
    let options = TableOptions::new(root.join("published.table"))
        .with_data_manager(DataManagerKind::StManAipsIO);
    table.save(options.clone()).expect("save");
    options
}

fn handle(options: &TableOptions) -> Table {
    Table::open_with_lock(options.clone(), LockOptions::new(LockMode::UserLocking)).expect("open")
}

fn mark(table: &mut Table, name: &str) {
    table
        .keywords_mut()
        .upsert(name, Value::Scalar(ScalarValue::Bool(true)));
}

#[test]
fn a_write_is_published_while_the_lock_is_kept_and_only_once() {
    let options = one_row_table("publish_writes");
    let mut reader = handle(&options);
    assert!(reader.lock(LockType::Read, 1).expect("read lock"));
    let before = reader.published_modify_counter().expect("counter");

    let mut writer = handle(&options);
    assert!(matches!(
        writer.published_modify_counter(),
        Err(TableError::NotLocked { .. })
    ));
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    // Nothing persisted yet: nothing to publish.
    assert!(!writer.publish_writes().expect("publish"));
    mark(&mut writer, "CHANGED");
    writer.save(options.clone()).expect("save");
    assert_eq!(reader.published_modify_counter().expect("counter"), before);

    assert!(writer.publish_writes().expect("publish"));
    assert!(writer.has_lock(LockType::Write), "the lock is kept");
    assert_eq!(
        reader.published_modify_counter().expect("counter"),
        before.wrapping_add(1)
    );
    // Neither a second publication nor the release announces it again.
    assert!(!writer.publish_writes().expect("publish"));
    writer.unlock().expect("unlock");
    assert_eq!(
        reader.published_modify_counter().expect("counter"),
        before.wrapping_add(1)
    );
}

#[test]
fn a_handle_without_the_write_lock_publishes_nothing() {
    let options = one_row_table("publish_no_lock");
    let mut table = handle(&options);
    assert!(!table.publish_writes().expect("unlocked"));
    assert!(table.lock(LockType::Read, 1).expect("read lock"));
    assert!(!table.publish_writes().expect("read-locked"));
}

#[test]
fn a_read_locked_handle_resyncs_to_what_another_handle_published() {
    let options = one_row_table("resync_locked");
    let mut reader = handle(&options);
    assert!(matches!(
        reader.resync_locked(),
        Err(TableError::NotLocked { .. })
    ));
    assert!(reader.lock(LockType::Read, 1).expect("read lock"));
    assert!(reader.keywords().get("CHANGED").is_none());
    assert!(!reader.resync_locked().expect("nothing new"));

    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    mark(&mut writer, "CHANGED");
    writer.unlock().expect("unlock");

    // Holding its read lock, the reader still has the old table: a lock it
    // already holds does not synchronize.
    assert!(reader.lock(LockType::Read, 1).expect("read lock again"));
    assert!(reader.keywords().get("CHANGED").is_none());
    assert!(reader.resync_locked().expect("resync"));
    assert!(reader.keywords().get("CHANGED").is_some());
    assert!(reader.has_lock(LockType::Read), "the lock is kept");
    assert!(!reader.resync_locked().expect("up to date"));
}

#[test]
fn upgrading_a_read_lock_refreshes_a_write_from_another_handle() {
    let options = one_row_table("upgrade_refresh");
    let mut reader = handle(&options);
    assert!(reader.lock(LockType::Read, 1).expect("read lock"));
    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    mark(&mut writer, "OTHER");
    writer.unlock().expect("publish");

    assert!(reader.keywords().get("OTHER").is_none());
    assert!(reader.lock(LockType::Write, 1).expect("upgrade"));
    assert!(reader.keywords().get("OTHER").is_some());
    mark(&mut reader, "MINE");
    reader.unlock().expect("publish both");
    let result = handle(&options);
    assert!(result.keywords().get("OTHER").is_some());
    assert!(result.keywords().get("MINE").is_some());
}

#[test]
fn a_write_locked_handle_is_not_resynced() {
    let options = one_row_table("resync_write_locked");
    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    mark(&mut writer, "UNSAVED");
    assert!(!writer.resync_locked().expect("write-locked"));
    assert!(writer.keywords().get("UNSAVED").is_some());
}

/// Both lock acquisition and explicit refresh must reload the disk
/// descriptor without retaining data rows. A previously read column must
/// give the new cell and retain the unchanged cell's value.
#[test]
fn published_cell_changes_keep_cold_readers_lazy_and_discard_stale_cells() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let options = TableOptions::new(temp.path().join("lazy.table"))
        .with_data_manager(DataManagerKind::StandardStMan);
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let rows = [10, 11].map(|value| {
        RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(value)),
        )])
    });
    Table::from_rows_with_schema(rows.to_vec(), schema)
        .expect("table")
        .save(options.clone())
        .expect("save");

    let mut cold = handle(&options);
    let mut warm = handle(&options);
    assert!(!cold.inner.has_loaded_rows());
    assert!(warm.lock(LockType::Read, 1).expect("warm read lock"));
    let read = |table: &Table| {
        table
            .column_accessor("id")
            .expect("id column")
            .scalar_cells_owned()
            .expect("read cells")
    };
    assert_eq!(
        read(&warm),
        vec![Some(ScalarValue::Int32(10)), Some(ScalarValue::Int32(11))]
    );
    assert!(warm.inner.has_loaded_scalar_column("id"));

    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    writer
        .cell_accessor_mut(0, "id")
        .expect("cell")
        .set(Value::Scalar(ScalarValue::Int32(90)))
        .expect("set id");
    writer
        .prepare_write()
        .save_selected_rows(&["id"], &[0])
        .expect("persist selected row");
    // Discard the writer's cached data; this test exercises the readers.
    writer.resync().expect("reload writer");
    writer.unlock().expect("publish");

    assert!(cold.lock(LockType::Read, 1).expect("cold read lock"));
    assert!(warm.resync_locked().expect("read-lock refresh"));
    for table in [&cold, &warm] {
        assert!(
            !table.inner.has_loaded_rows(),
            "reload materialized the table"
        );
        assert!(
            !table.inner.has_loaded_scalar_column("id"),
            "reload retained stale cells"
        );
        assert_eq!(
            read(table),
            vec![Some(ScalarValue::Int32(90)), Some(ScalarValue::Int32(11))]
        );
        assert!(table.has_lock(LockType::Read));
    }
}

fn mixed_table_with_write_lock() -> (tempfile::TempDir, TableOptions, Table) {
    use crate::ColumnBinding;
    let dir = tempfile::tempdir().expect("tempdir");
    let options = TableOptions::new(dir.path().join("mixed.table"))
        .with_data_manager(DataManagerKind::StandardStMan);
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("mark", PrimitiveType::Int32),
    ])
    .expect("schema");
    let rows = vec![RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new("mark", Value::Scalar(ScalarValue::Int32(2))),
    ])];
    Table::from_rows_with_schema(rows, schema)
        .expect("table")
        .save_with_bindings(
            options.clone(),
            &std::collections::HashMap::from([(
                "mark".into(),
                ColumnBinding {
                    data_manager: DataManagerKind::IncrementalStMan,
                    tile_shape: None,
                },
            )]),
        )
        .expect("save");
    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    (dir, options, writer)
}

#[test]
fn dropping_after_persisting_only_metadata_does_not_rewrite_managers() {
    let (_dir, options, mut writer) = mixed_table_with_write_lock();
    let managers = writer.data_manager_info().to_vec();
    mark(&mut writer, "CHANGED");
    writer
        .save_metadata_only(options.clone())
        .expect("save metadata");
    drop(writer);
    let reopened = handle(&options);
    assert_eq!(reopened.data_manager_info(), managers);
    assert!(reopened.keywords().get("CHANGED").is_some());
}

#[test]
fn dropping_after_a_partial_save_flushes_the_unrelated_cell_in_place() {
    let (_dir, options, mut writer) = mixed_table_with_write_lock();
    let managers = writer.data_manager_info().to_vec();
    writer
        .cell_accessor_mut(0, "id")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(10)))
        .unwrap();
    writer
        .cell_accessor_mut(0, "mark")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(20)))
        .unwrap();
    writer
        .prepare_write()
        .save_selected_rows(&["id"], &[0])
        .expect("save first cell");
    assert_eq!(
        Table::open(options.clone())
            .unwrap()
            .cell_accessor(0, "mark")
            .unwrap()
            .scalar()
            .unwrap(),
        &ScalarValue::Int32(2)
    );
    drop(writer);
    let reopened = handle(&options);
    assert_eq!(reopened.data_manager_info(), managers);
    assert_eq!(
        reopened.cell_accessor(0, "id").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(10)
    );
    assert_eq!(
        reopened.cell_accessor(0, "mark").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(20)
    );
}

#[test]
fn a_locked_aipsio_flush_keeps_the_group_and_stored_byte_order() {
    use crate::EndianFormat;
    let dir = tempfile::tempdir().unwrap();
    let original = TableOptions::new(dir.path().join("be.table"))
        .with_endian_format(EndianFormat::BigEndian)
        .with_data_manager_group("big");
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let rows = vec![RecordValue::new(vec![RecordField::new(
        "id",
        Value::Scalar(ScalarValue::Int32(1)),
    )])];
    Table::from_rows_with_schema(rows, schema)
        .unwrap()
        .save(original.clone())
        .unwrap();
    // No data-manager or endian override was requested when opening.
    let options = TableOptions::new(original.path());
    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).unwrap());
    writer
        .cell_accessor_mut(0, "id")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(6)))
        .unwrap();
    writer.unlock().unwrap();
    let reopened = Table::open(options).unwrap();
    assert_eq!(
        reopened.stored_endian_format().unwrap(),
        Some(EndianFormat::BigEndian)
    );
    assert_eq!(
        reopened
            .stored_table_description()
            .unwrap()
            .unwrap()
            .declared_managers,
        [("id".into(), "StManAipsIO".into(), "big".into())],
    );
    assert_eq!(
        reopened.cell_accessor(0, "id").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(6)
    );
}
