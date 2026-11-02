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
fn a_write_locked_handle_is_not_resynced() {
    let options = one_row_table("resync_write_locked");
    let mut writer = handle(&options);
    assert!(writer.lock(LockType::Write, 1).expect("write lock"));
    mark(&mut writer, "UNSAVED");
    assert!(!writer.resync_locked().expect("write-locked"));
    assert!(writer.keywords().get("UNSAVED").is_some());
}
