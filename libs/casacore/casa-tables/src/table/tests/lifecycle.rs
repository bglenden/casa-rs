// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn table_info_default_is_empty() {
    let table = Table::new();
    assert_eq!(table.info().table_type, "");
    assert_eq!(table.info().sub_type, "");
}

#[test]
fn table_info_set_and_get() {
    use crate::storage::TableInfo;
    let mut table = Table::new();
    table.set_info(TableInfo {
        table_type: "MeasurementSet".to_string(),
        sub_type: "UVFITS".to_string(),
        readme: Vec::new(),
    });
    assert_eq!(table.info().table_type, "MeasurementSet");
    assert_eq!(table.info().sub_type, "UVFITS");
}

#[test]
fn table_info_round_trip_disk() {
    use crate::storage::TableInfo;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("info_test.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "MeasurementSet".to_string(),
        sub_type: "UVFITS".to_string(),
        readme: Vec::new(),
    });
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(42)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&path)).unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.info().table_type, "MeasurementSet");
    assert_eq!(reopened.info().sub_type, "UVFITS");
}

#[test]
fn table_info_empty_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_info.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    table.save(TableOptions::new(&path)).unwrap();

    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.info().table_type, "");
    assert_eq!(reopened.info().sub_type, "");
}

#[test]
fn table_info_preserved_by_to_memory() {
    use crate::storage::TableInfo;
    let mut table = Table::new();
    table.set_info(TableInfo {
        table_type: "Catalog".to_string(),
        sub_type: "".to_string(),
        readme: Vec::new(),
    });
    let mem = table.to_memory().unwrap();
    assert_eq!(mem.info().table_type, "Catalog");
}

#[test]
fn table_info_preserved_by_deep_copy() {
    use crate::storage::TableInfo;
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.tbl");
    let dst_path = dir.path().join("dst.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Float64)]).unwrap();
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Sky".to_string(),
        sub_type: "Model".to_string(),
        readme: Vec::new(),
    });
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Float64(1.0)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&src_path)).unwrap();

    let original = Table::open(TableOptions::new(&src_path)).unwrap();
    original.deep_copy(TableOptions::new(&dst_path)).unwrap();

    let copy = Table::open(TableOptions::new(&dst_path)).unwrap();
    assert_eq!(copy.info().table_type, "Sky");
    assert_eq!(copy.info().sub_type, "Model");
}

// -------------------------------------------------------------------
// Lifecycle operation tests
// -------------------------------------------------------------------

#[test]
fn flush_writes_changes_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flush_test.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&path)).unwrap();

    // Reopen, mutate, and flush
    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(2)),
        )]))
        .unwrap();
    table.flush().unwrap();

    // Reopen and verify both rows
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert_eq!(reopened.row_count(), 2);
}

#[test]
fn flush_without_path_fails() {
    let mut table = Table::new();
    assert!(table.flush().is_err());
}

#[test]
fn flush_keeps_mixed_manager_assignments_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TableOptions::new(dir.path()).with_data_manager(DataManagerKind::StandardStMan);
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("flag", Value::Scalar(ScalarValue::Bool(false))),
        ]))
        .unwrap();
    let bindings = HashMap::from([(
        "flag".to_owned(),
        ColumnBinding {
            data_manager: DataManagerKind::StManAipsIO,
            tile_shape: None,
        },
    )]);
    table.save_with_bindings(opts.clone(), &bindings).unwrap();
    let mut table = Table::open(opts.clone()).unwrap();
    let managers = table.data_manager_info().to_vec();
    table
        .set_cell(0, "id", Value::Scalar(ScalarValue::Int32(2)))
        .unwrap();
    table
        .set_cell(0, "flag", Value::Scalar(ScalarValue::Bool(true)))
        .unwrap();
    table.flush().unwrap();
    let fresh = Table::open(opts).unwrap();
    assert_eq!(fresh.data_manager_info(), managers);
    assert_eq!(
        fresh.get_scalar_cell(0, "id").unwrap(),
        &ScalarValue::Int32(2)
    );
    assert_eq!(
        fresh.get_scalar_cell(0, "flag").unwrap(),
        &ScalarValue::Bool(true)
    );
}

#[test]
fn a_second_flush_uses_the_manager_of_an_added_column() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TableOptions::new(dir.path()).with_data_manager(DataManagerKind::StandardStMan);
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    table.save(opts.clone()).unwrap();
    let mut table = Table::open(opts.clone()).unwrap();
    table
        .add_column(
            ColumnSchema::scalar("added", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        )
        .unwrap();
    table.flush().unwrap();
    table
        .set_cell(0, "added", Value::Scalar(ScalarValue::Int32(7)))
        .unwrap();
    table.flush().unwrap();
    assert_eq!(
        Table::open(opts)
            .unwrap()
            .get_scalar_cell(0, "added")
            .unwrap(),
        &ScalarValue::Int32(7)
    );
}

#[test]
fn flush_keeps_a_named_aipsio_manager_when_rewriting_a_string() {
    use crate::storage::{stman_aipsio::write_named_stman_file, table_control::read_table_dat};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let opts = TableOptions::new(path);
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("name", PrimitiveType::String)]).unwrap();
    let mut table = Table::with_schema(schema);
    let string = |text: &str| Value::Scalar(ScalarValue::String(text.into()));
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "name",
            string("old"),
        )]))
        .unwrap();
    table.save(opts.clone()).unwrap();
    let dat = read_table_dat(&path.join("table.dat")).unwrap();
    write_named_stman_file(
        &path.join("table.f0"),
        0,
        &dat.table_desc.columns,
        table.inner.rows().unwrap(),
        casa_aipsio::ByteOrder::BigEndian,
        "CustomAips",
    )
    .unwrap();
    let mut table = Table::open(opts.clone()).unwrap();
    assert_eq!(table.data_manager_info()[0].name, "CustomAips");
    table
        .set_cell(0, "name", string("a different string width"))
        .unwrap();
    table.flush().unwrap();
    let fresh = Table::open(opts).unwrap();
    assert_eq!(fresh.data_manager_info()[0].name, "CustomAips");
    assert_eq!(
        fresh.get_scalar_cell(0, "name").unwrap(),
        &ScalarValue::String("a different string width".into())
    );
}

#[test]
fn flush_preflights_virtual_cells_before_touching_a_physical_group() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let opts = TableOptions::new(dir.path().join("forward"));
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("linked", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for value in [1, 2] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(value))),
                RecordField::new("linked", Value::Scalar(ScalarValue::Int32(value))),
            ]))
            .unwrap();
    }
    table.save(TableOptions::new(&target)).unwrap();
    table.bind_forward_column("linked", &target).unwrap();
    table.save(opts.clone()).unwrap();
    let generation = crate::TableWriteLock::read_modify_counter(opts.path()).unwrap();
    let mut table = Table::open(opts.clone()).unwrap();
    table
        .set_cell(0, "id", Value::Scalar(ScalarValue::Int32(11)))
        .unwrap();
    table
        .set_cell(1, "linked", Value::Scalar(ScalarValue::Int32(22)))
        .unwrap();
    assert!(table.flush().is_err());
    assert_eq!(
        crate::TableWriteLock::read_modify_counter(opts.path()).unwrap(),
        generation
    );
    let read = Table::open(opts).unwrap();
    assert_eq!(
        read.get_scalar_cell(0, "id").unwrap(),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        read.get_scalar_cell(1, "linked").unwrap(),
        &ScalarValue::Int32(2)
    );
}

#[cfg(unix)]
#[test]
fn flush_rejects_a_manager_inode_shared_with_another_table() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    let options = paths
        .each_ref()
        .map(|path| TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan));
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    for opts in &options {
        table.save(opts.clone()).unwrap();
    }
    let second_file = paths[1].join("table.f0");
    std::fs::remove_file(&second_file).unwrap();
    std::fs::hard_link(paths[0].join("table.f0"), &second_file).unwrap();
    let generations = || {
        paths
            .each_ref()
            .map(|path| crate::TableWriteLock::read_modify_counter(path).unwrap())
    };
    let before = generations();
    let mut writer = Table::open(options[0].clone()).unwrap();
    writer
        .set_cell(0, "id", Value::Scalar(ScalarValue::Int32(2)))
        .unwrap();
    assert!(writer.flush().is_err());
    assert_eq!(generations(), before);
    for opts in options {
        assert_eq!(
            Table::open(opts).unwrap().get_scalar_cell(0, "id").unwrap(),
            &ScalarValue::Int32(1)
        );
    }
}

#[test]
fn resync_discards_in_memory_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resync_test.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    table.save(TableOptions::new(&path)).unwrap();

    // Open from disk, add a row in memory (not saved)
    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(2)),
        )]))
        .unwrap();
    assert_eq!(table.row_count(), 2);

    // Resync discards the unsaved row
    table.resync().unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn resync_without_path_fails() {
    let mut table = Table::new();
    assert!(table.resync().is_err());
}

#[test]
fn mark_for_delete_removes_directory_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delete_me.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    table.save(TableOptions::new(&path)).unwrap();
    assert!(path.exists());

    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table.mark_for_delete();
    assert!(table.is_marked_for_delete());

    drop(table);
    assert!(!path.exists(), "table directory should be deleted on drop");
}

#[test]
fn unmark_for_delete_prevents_removal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keep_me.tbl");

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let table = Table::with_schema(schema);
    table.save(TableOptions::new(&path)).unwrap();

    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table.mark_for_delete();
    table.unmark_for_delete();
    assert!(!table.is_marked_for_delete());

    drop(table);
    assert!(path.exists(), "table directory should still exist");
}

// -------------------------------------------------------------------
// Locking extension tests
// -------------------------------------------------------------------

#[test]
fn lock_mode_resolve() {
    use crate::lock::LockMode;
    assert_eq!(LockMode::DefaultLocking.resolve(), LockMode::AutoLocking);
    assert_eq!(LockMode::AutoLocking.resolve(), LockMode::AutoLocking);
    assert_eq!(LockMode::NoLocking.resolve(), LockMode::NoLocking);
}

#[test]
fn lock_mode_skip_read_lock() {
    use crate::lock::LockMode;
    assert!(LockMode::AutoNoReadLocking.skip_read_lock());
    assert!(LockMode::UserNoReadLocking.skip_read_lock());
    assert!(!LockMode::AutoLocking.skip_read_lock());
    assert!(!LockMode::UserLocking.skip_read_lock());
}

#[test]
fn external_sync_hook_ordering() {
    use crate::lock::ExternalLockSync;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Recorder(Arc<Mutex<Vec<&'static str>>>);
    impl ExternalLockSync for Recorder {
        fn acquire_read(&self) {
            self.0.lock().unwrap().push("acquire_read");
        }
        fn acquire_write(&self) {
            self.0.lock().unwrap().push("acquire_write");
        }
        fn release(&self) {
            self.0.lock().unwrap().push("release");
        }
    }

    let log = Arc::new(Mutex::new(Vec::new()));
    let recorder = Recorder(log.clone());

    // Verify the trait is object-safe and can be boxed
    let _: Box<dyn ExternalLockSync> = Box::new(recorder);

    // Verify the log works
    let recorder2 = Recorder(log.clone());
    recorder2.acquire_read();
    recorder2.acquire_write();
    recorder2.release();

    let events = log.lock().unwrap();
    assert_eq!(&*events, &["acquire_read", "acquire_write", "release"]);
}

// -------------------------------------------------------------------
// Set algebra tests
// -------------------------------------------------------------------

#[test]
fn row_union_merges_and_deduplicates() {
    assert_eq!(
        Table::row_union(&[0, 2, 4], &[1, 2, 3]),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn row_intersection_keeps_common() {
    assert_eq!(
        Table::row_intersection(&[0, 1, 2, 3], &[2, 3, 4]),
        vec![2, 3]
    );
}

#[test]
fn row_difference_removes_second() {
    assert_eq!(Table::row_difference(&[0, 1, 2, 3], &[1, 3]), vec![0, 2]);
}

#[test]
fn row_set_ops_with_empty() {
    assert!(Table::row_intersection(&[0, 1], &[]).is_empty());
    assert_eq!(Table::row_union(&[], &[3, 1]), vec![1, 3]);
    assert_eq!(Table::row_difference(&[5, 3], &[]), vec![3, 5]);
}

#[test]
fn row_set_operations_cover_greater_and_unsorted_paths() {
    assert_eq!(Table::row_intersection(&[2, 4], &[1, 2]), vec![2]);
    assert_eq!(Table::row_difference(&[2, 4], &[1, 2]), vec![4]);
    assert_eq!(
        Table::row_intersection(&[4, 2, 2, 1], &[3, 4, 2]),
        vec![2, 4]
    );
    assert_eq!(Table::row_difference(&[4, 2, 2, 1], &[4, 3]), vec![1, 2]);
}

// -------------------------------------------------------------------
// NoSort iteration tests
// -------------------------------------------------------------------

#[test]
fn iter_groups_nosort_preserves_natural_order() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    // Insert pattern: A, B, A — nosort should yield 3 groups
    for v in [1, 2, 1] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "k",
                Value::Scalar(ScalarValue::Int32(v)),
            )]))
            .unwrap();
    }

    let groups: Vec<_> = table.iter_groups_nosort(&["k"]).unwrap().collect();
    assert_eq!(
        groups.len(),
        3,
        "nosort should not merge non-adjacent duplicates"
    );
    assert_eq!(groups[0].row_indices, vec![0]); // k=1
    assert_eq!(groups[1].row_indices, vec![1]); // k=2
    assert_eq!(groups[2].row_indices, vec![2]); // k=1 again (separate group)
}

#[test]
fn iter_groups_nosort_merges_consecutive_equal() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    for v in [1, 1, 2, 2, 2] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "k",
                Value::Scalar(ScalarValue::Int32(v)),
            )]))
            .unwrap();
    }

    let groups: Vec<_> = table.iter_groups_nosort(&["k"]).unwrap().collect();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].row_indices, vec![0, 1]); // k=1
    assert_eq!(groups[1].row_indices, vec![2, 3, 4]); // k=2
}
