// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::lock::{LockMode, LockOptions, LockType};

fn build_test_table_on_disk(dir: &std::path::Path, dm: DataManagerKind) -> TableOptions {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("alice".into()))),
        ]))
        .unwrap();
    let opts = TableOptions::new(dir.join("test.tbl")).with_data_manager(dm);
    table.save(opts.clone()).unwrap();
    opts
}

#[test]
fn open_with_permanent_lock_acquires_immediately() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::PermanentLocking);

    let table = Table::open_with_lock(opts, lock_opts).unwrap();
    assert!(table.has_lock(LockType::Write));
    assert!(table.has_lock(LockType::Read));
    assert_eq!(table.row_count(), 1);
}

#[test]
fn flush_updates_the_descriptor_count_of_a_named_memory_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory");
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema)
        .into_named_memory_manager(&path, "mem", LockOptions::new(LockMode::UserLocking))
        .unwrap();
    assert!(table.lock(LockType::Write, 1).unwrap());
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .unwrap();
    table.flush().unwrap();
    table.unlock().unwrap();
    drop(table);
    let fresh = Table::open(TableOptions::new(path)).unwrap();
    assert_eq!(fresh.row_count(), 1);
    assert_eq!(
        fresh.get_scalar_cell(0, "id").unwrap(),
        &ScalarValue::Int32(0)
    );
}

#[test]
fn open_with_user_lock_has_no_lock_until_explicit() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
    assert!(!table.has_lock(LockType::Write));
    assert!(!table.has_lock(LockType::Read));

    // Acquire write lock explicitly.
    assert!(table.lock(LockType::Write, 1).unwrap());
    assert!(table.has_lock(LockType::Write));
    assert!(table.has_lock(LockType::Read));
}

#[test]
fn lock_unlock_cycle_user_mode() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
    assert!(table.lock(LockType::Write, 1).unwrap());
    assert!(table.has_lock(LockType::Write));

    table.unlock().unwrap();
    assert!(!table.has_lock(LockType::Write));
}

#[test]
fn user_locking_write_requires_explicit_write_lock() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
    let err = table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
        ]))
        .unwrap_err();
    assert!(matches!(err, TableError::LockFailed { .. }));

    assert!(table.lock(LockType::Write, 1).unwrap());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
        ]))
        .unwrap();
    table.unlock().unwrap();
}

#[test]
fn write_plan_partial_save_obeys_user_locking() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let mut table = Table::open_with_lock(opts, lock_opts).unwrap();

    let error = table
        .prepare_write()
        .save_selected_rows(&["id"], &[0])
        .unwrap_err();
    assert!(matches!(error, TableError::LockFailed { .. }));

    assert!(table.lock(LockType::Write, 1).unwrap());
    table
        .prepare_write()
        .save_selected_rows(&["id"], &[0])
        .unwrap();
    table.unlock().unwrap();
}

#[test]
fn auto_locking_write_ops_acquire_write_lock_automatically() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::AutoLocking);

    let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
        ]))
        .unwrap();

    assert_eq!(table.row_count(), 2);
    assert!(!table.has_lock(LockType::Write));
    let reopened = Table::open(opts).unwrap();
    assert_eq!(reopened.row_count(), 2);
}

#[test]
fn unlock_flushes_write_to_disk() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
    assert!(table.lock(LockType::Write, 1).unwrap());

    // Add a row while holding the write lock.
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
        ]))
        .unwrap();

    // Unlock should flush to disk.
    table.unlock().unwrap();

    // Reopen without locking and verify.
    let reopened = Table::open(opts).unwrap();
    assert_eq!(reopened.row_count(), 2);
}

/// A NoRead mode opens without a read lock, but an explicit request for
/// one takes it and it is held until released: casacore's
/// `PlainTable::lock` has no NoRead exception (probed in CASA 6.7.6.14
/// as `tb.haslock(write=False)` after `tb.lock(write=False)`).
#[test]
fn an_explicit_read_lock_is_taken_in_the_no_read_modes() {
    for mode in [LockMode::AutoNoReadLocking, LockMode::UserNoReadLocking] {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
        let mut table = Table::open_with_lock(opts, LockOptions::new(mode)).unwrap();
        assert!(!table.has_lock(LockType::Read), "{mode:?} opens unlocked");

        assert!(table.lock(LockType::Read, 1).unwrap());
        assert!(table.has_lock(LockType::Read), "{mode:?} holds the lock");
        assert!(!table.has_lock(LockType::Write));

        table.unlock().unwrap();
        assert!(!table.has_lock(LockType::Read), "{mode:?} released it");
    }
}

#[test]
fn observation_owner_locked_modify_counter_tracks_one_write_unlock() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    let mut first = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
    assert!(first.lock(LockType::Read, 1).unwrap());
    let before = first.locked_modify_counter().unwrap();
    first.unlock().unwrap();

    let mut writer = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    assert_eq!(writer.locked_modify_counter().unwrap(), before);
    writer
        .keywords_mut()
        .upsert("EXTERNAL_WRITE", Value::Scalar(ScalarValue::Bool(true)));
    writer.unlock().unwrap();

    assert!(first.lock(LockType::Read, 1).unwrap());
    assert_eq!(
        first.locked_modify_counter().unwrap(),
        before.wrapping_add(1)
    );
}

/// A published write describes the table as persisted: casacore takes a
/// reopened table's row count from the sync data, refuses a lock whose
/// column count differs, and asserts one change counter per data manager
/// (`ColumnSet::resync`). Covers `Table::unlock` on a table with several
/// data managers and `TableWriteLock` across a layout change.
#[test]
fn published_writes_describe_the_persisted_layout() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("mixed.tbl");
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for id in 0..3 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("n".into()))),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .unwrap();
    }
    let binding = |data_manager| ColumnBinding {
        data_manager,
        tile_shape: None,
    };
    let bindings = HashMap::from([
        (
            "name".to_string(),
            binding(DataManagerKind::IncrementalStMan),
        ),
        ("flag".to_string(), binding(DataManagerKind::StManAipsIO)),
    ]);
    table
        .save_with_bindings(
            TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .unwrap();
    let persisted = |path: &std::path::Path| {
        let contents =
            crate::storage::table_control::read_table_dat(&path.join("table.dat")).unwrap();
        (
            contents.nrrow,
            contents.table_desc.columns.len() as i32,
            contents.column_set.data_managers.len(),
        )
    };
    let published = |path: &std::path::Path| {
        let sync = crate::lock::read_sync_data_from_table_dir(path)
            .unwrap()
            .expect("published sync data");
        (
            sync.nrrow,
            sync.nrcolumn,
            sync.data_man_change_counters.len(),
        )
    };
    assert_eq!(persisted(&path), (3, 3, 3));

    // An in-place writer publishes the three managers.
    let mut lock = crate::TableWriteLock::acquire(&path, 1).unwrap();
    lock.record_write();
    lock.release().unwrap();
    assert_eq!(published(&path), persisted(&path));

    // Table::unlock currently saves through Table::save with the data
    // manager of its open options, which collapses the three managers to
    // one; that is a known problem of the save, not of the publication.
    // Whatever the save leaves, the published counters follow it, here
    // shrinking from three to one.
    let mut locked = Table::open_with_lock(
        TableOptions::new(&path),
        LockOptions::new(LockMode::UserLocking),
    )
    .unwrap();
    assert!(locked.lock(LockType::Write, 1).unwrap());
    locked
        .row_accessor_mut()
        .set_cell(
            0,
            "name",
            Value::Scalar(ScalarValue::String("changed".into())),
        )
        .unwrap();
    locked.unlock().unwrap();
    drop(locked);
    assert_eq!(published(&path), persisted(&path));

    // A layout change made under the lock adds a manager.
    let managers_before = persisted(&path).2;
    let mut lock = crate::TableWriteLock::acquire(&path, 1).unwrap();
    let mut plain = Table::open(TableOptions::new(&path)).unwrap();
    plain
        .add_column(
            ColumnSchema::array_variable("extra", PrimitiveType::Float32, Some(1)),
            None,
        )
        .unwrap();
    lock.record_write();
    plain
        .prepare_write()
        .add_tiled_shape_column("extra", &[], None)
        .unwrap();
    lock.release().unwrap();
    let after_layout_change = persisted(&path);
    assert_eq!(after_layout_change.1, 4);
    assert_eq!(after_layout_change.2, managers_before + 1);
    assert_eq!(published(&path), after_layout_change);
}

/// casacore raises the modify counter only when a write lock period
/// changed the table (`TableSyncData::write`, `PlainTable::putFile`): a
/// write lock released without a change publishes nothing, and a change
/// is published once, however many lock periods follow it.
#[test]
fn a_write_lock_that_changes_nothing_publishes_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);
    let published = || {
        let mut reader = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
        assert!(reader.lock(LockType::Read, 1).unwrap());
        reader.locked_modify_counter().unwrap()
    };
    let before = published();

    let mut writer = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
    // A write refused for want of the write lock changes nothing.
    assert!(
        writer
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
            ]))
            .is_err()
    );
    assert!(writer.lock(LockType::Write, 1).unwrap());
    assert_eq!(writer.row_count(), 1);
    writer.unlock().unwrap();
    assert_eq!(published(), before, "an unchanged write lock was published");

    assert!(writer.lock(LockType::Write, 1).unwrap());
    writer
        .keywords_mut()
        .upsert("CHANGED", Value::Scalar(ScalarValue::Bool(true)));
    writer.unlock().unwrap();
    assert_eq!(published(), before.wrapping_add(1));

    assert!(writer.lock(LockType::Write, 1).unwrap());
    writer.unlock().unwrap();
    drop(writer);
    assert_eq!(
        published(),
        before.wrapping_add(1),
        "a change was published twice"
    );
}

/// The modify counter published for the table at `opts`, read through a
/// new handle.
fn published_counter(opts: &TableOptions) -> u32 {
    let mut reader =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(reader.lock(LockType::Read, 1).unwrap());
    reader.locked_modify_counter().unwrap()
}

fn set_id(table: &mut Table, id: i32) {
    table
        .row_accessor_mut()
        .set_cell(0, "id", Value::Scalar(ScalarValue::Int32(id)))
        .unwrap();
}

/// A write persisted under the write lock is published once when the
/// lock is released, whether the handle then still holds it, reloaded it
/// with a resync, or never flushed it itself; a resync that discards
/// changes that never reached the disk publishes nothing.
#[test]
fn a_persisted_write_is_published_once_even_after_a_resync() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    let mut expected = published_counter(&opts);
    let mut lock_period = |write: &dyn Fn(&mut Table), publishes: bool| {
        assert!(writer.lock(LockType::Write, 1).unwrap());
        write(&mut writer);
        writer.unlock().unwrap();
        expected = expected.wrapping_add(u32::from(publishes));
        assert_eq!(published_counter(&opts), expected);
    };

    // A flush followed by the unlock's own flush: one publication.
    lock_period(
        &|table| {
            set_id(table, 2);
            table.flush().unwrap();
        },
        true,
    );
    // A flush, then a resync.
    lock_period(
        &|table| {
            set_id(table, 3);
            table.flush().unwrap();
            table.resync().unwrap();
        },
        true,
    );
    // Rows saved in place by a write plan, then a resync.
    lock_period(
        &|table| {
            set_id(table, 4);
            table
                .prepare_write()
                .save_selected_rows(&["id"], &[0])
                .unwrap();
            table.resync().unwrap();
        },
        true,
    );
    // A change discarded by a resync before it reached the disk.
    lock_period(
        &|table| {
            set_id(table, 5);
            table.resync().unwrap();
        },
        false,
    );
    // Nothing at all.
    lock_period(&|_| {}, false);

    drop(writer);
    let reopened = Table::open(opts).unwrap();
    assert_eq!(
        reopened.cell_accessor(0, "id").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(4)
    );
}

/// Dropping a handle that holds the write lock publishes a write it
/// persisted and then reloaded.
#[test]
fn dropping_a_handle_publishes_a_write_persisted_before_a_resync() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let before = published_counter(&opts);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    set_id(&mut writer, 2);
    writer.flush().unwrap();
    writer.resync().unwrap();
    drop(writer);
    assert_eq!(published_counter(&opts), before.wrapping_add(1));
}

#[test]
fn dropping_a_write_lock_never_retries_an_unscoped_full_save() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StandardStMan);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    // A schema edit has no selected-row persistence scope.
    writer.rename_column("name", "other").unwrap();
    set_id(&mut writer, 2);
    let control = opts.path().join("table.dat");
    let before = control.metadata().unwrap();
    std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o444)).unwrap();
    if std::fs::OpenOptions::new()
        .write(true)
        .open(&control)
        .is_ok()
    {
        eprintln!("skipping mode-0444 law: process can write the descriptor");
        return;
    }
    assert!(writer.unlock().is_err());
    assert!(writer.has_lock(LockType::Write));
    // Make a destructor retry observable even on branches where a full
    // save preflights a read-only descriptor before touching the managers.
    std::fs::set_permissions(&control, before.permissions()).unwrap();
    drop(writer);
    let reopened = Table::open(opts).unwrap();
    let id = reopened.cell_accessor(0, "id").unwrap().scalar().unwrap();
    let after = control.metadata().unwrap();
    assert!(
        id == &ScalarValue::Int32(1) && after.ino() == before.ino(),
        "after failed release and drop: id {id:?}, descriptor inode {} -> {}",
        before.ino(),
        after.ino()
    );
}

/// A writer that persists its own changes reloads the table after a
/// failure so that nothing is left to flush. When the reload fails too,
/// `resync_or_discard` still leaves nothing for the drop to write: the
/// table is not written in full with the change a failed write left in
/// memory.
#[test]
fn a_failed_reload_leaves_nothing_for_a_drop_to_write() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    set_id(&mut writer, 2);

    // The control file cannot be read, so the reload fails.
    let control = opts.path().join("table.dat");
    let hidden = opts.path().join("hidden-control");
    std::fs::rename(&control, &hidden).unwrap();
    assert!(writer.resync_or_discard().is_err(), "the reload succeeded");
    std::fs::rename(&hidden, &control).unwrap();

    let files = |dir: &std::path::Path| -> Vec<(String, Vec<u8>)> {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().is_some_and(|name| name != "table.lock"))
            .map(|path| {
                (
                    path.file_name().unwrap().to_string_lossy().into_owned(),
                    std::fs::read(&path).unwrap(),
                )
            })
            .collect();
        files.sort();
        files
    };
    let before = files(opts.path());
    drop(writer);
    assert_eq!(files(opts.path()), before, "the drop wrote the table");
    let reopened = Table::open(opts).unwrap();
    assert_eq!(
        reopened.cell_accessor(0, "id").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(1),
        "the change a failed write left in memory reached the disk"
    );
}

/// A release that fails while the table holds unwritten changes, and
/// whose reload fails too, leaves nothing for the drop to write in full
/// (`unlock_metadata_only_or_discard`). The lock may stay held, as the
/// publication of a write needs the control file too; the drop then only
/// publishes.
#[test]
fn a_failed_release_discards_the_changes_and_leaves_nothing_for_a_drop_to_write() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    set_id(&mut writer, 2);

    // The control file becomes a directory: the metadata flush of the
    // release fails, and so does the reload.
    let control = opts.path().join("table.dat");
    let hidden = opts.path().join("hidden-control");
    std::fs::rename(&control, &hidden).unwrap();
    std::fs::create_dir(&control).unwrap();
    assert!(
        writer.unlock_metadata_only_or_discard().is_err(),
        "the blocked release succeeded"
    );
    std::fs::remove_dir(&control).unwrap();
    std::fs::rename(&hidden, &control).unwrap();

    let data = std::fs::read(opts.path().join("table.f0")).unwrap();
    let dat = std::fs::read(&control).unwrap();
    drop(writer);
    assert_eq!(std::fs::read(opts.path().join("table.f0")).unwrap(), data);
    assert_eq!(std::fs::read(&control).unwrap(), dat);
    let reopened = Table::open(opts).unwrap();
    assert_eq!(
        reopened.cell_accessor(0, "id").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(1)
    );
}

/// An unlock whose flush fails returns the error and keeps the write
/// lock and the write it started; the next unlock flushes and publishes
/// it once.
#[test]
fn an_unlock_whose_flush_fails_keeps_the_lock_and_publishes_later() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let before = published_counter(&opts);
    let mut writer =
        Table::open_with_lock(opts.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    set_id(&mut writer, 2);

    // The storage file becomes a directory, which the save cannot write.
    // The save removes every other `table.f*` file, so the original is
    // kept under another name.
    let storage = opts.path().join("table.f0");
    let hidden = opts.path().join("hidden-storage");
    std::fs::rename(&storage, &hidden).unwrap();
    std::fs::create_dir(&storage).unwrap();
    std::fs::write(storage.join("blocker"), b"").unwrap();
    assert!(writer.unlock().is_err(), "the blocked flush succeeded");
    assert!(writer.has_lock(LockType::Write), "a failed unlock released");
    std::fs::remove_dir_all(&storage).unwrap();
    std::fs::rename(&hidden, &storage).unwrap();

    writer.unlock().unwrap();
    assert!(!writer.has_lock(LockType::Write));
    drop(writer);
    assert_eq!(published_counter(&opts), before.wrapping_add(1));
    let reopened = Table::open(opts).unwrap();
    assert_eq!(
        reopened.cell_accessor(0, "id").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(2)
    );
}

#[test]
fn lock_reloads_after_external_modification() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::UserLocking);

    // Open table A with locking, acquire and release.
    let mut table_a = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
    assert!(table_a.lock(LockType::Write, 1).unwrap());
    table_a.unlock().unwrap();

    // Simulate another process: open with locking, modify, unlock.
    {
        let mut table_b = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
        assert!(table_b.lock(LockType::Write, 1).unwrap());
        table_b
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String("external".into())),
                ),
            ]))
            .unwrap();
        table_b.unlock().unwrap();
        // table_b dropped here, releasing lock file fd.
    }

    // Re-acquire the lock on table_a — should reload.
    assert!(table_a.lock(LockType::Write, 1).unwrap());
    assert_eq!(table_a.row_count(), 2);
}

#[test]
fn no_lock_backward_compat() {
    // Existing open()/save() API works unchanged.
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let table = Table::open(opts).unwrap();
    assert_eq!(table.row_count(), 1);
    // has_lock returns false for non-locked tables.
    assert!(!table.has_lock(LockType::Write));
}

#[test]
fn lock_file_created_on_disk() {
    let tmp = tempfile::TempDir::new().unwrap();
    let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
    let lock_opts = LockOptions::new(LockMode::PermanentLocking);

    let _table = Table::open_with_lock(opts, lock_opts).unwrap();
    let lock_path = tmp.path().join("test.tbl").join("table.lock");
    assert!(lock_path.exists(), "table.lock should be created");
}

#[test]
fn lock_on_non_locked_table_errors() {
    let table = Table::new();
    let mut table = table;
    let result = table.lock(LockType::Write, 1);
    assert!(matches!(result, Err(TableError::NotLocked { .. })));
}

#[test]
fn permanent_lock_round_trip_both_dms() {
    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let tmp = tempfile::TempDir::new().unwrap();
        let opts = build_test_table_on_disk(tmp.path(), dm);
        let lock_opts = LockOptions::new(LockMode::PermanentLocking);

        let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
        assert!(table.has_lock(LockType::Write));
        assert_eq!(table.row_count(), 1);

        // Add a row while permanently locked.
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
            ]))
            .unwrap();

        // Unlock is a no-op in permanent mode; lock stays held.
        table.unlock().unwrap();
        assert!(table.has_lock(LockType::Write));

        // Drop closes and flushes.
        drop(table);

        // Reopen and verify.
        let reopened = Table::open(opts).unwrap();
        assert_eq!(reopened.row_count(), 2);
    }
}
