// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;

use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use crate::storage::row_changes::FAIL_AFTER_UPDATES;
use crate::{ColumnBinding, ColumnSchema, DataManagerKind, TableOptions, TableSchema};

use super::*;

#[test]
fn full_save_restores_a_live_write_before_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let options = TableOptions::new(path);
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("other", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("other", Value::Scalar(ScalarValue::Int32(10))),
        ]))
        .unwrap();
    let bindings = HashMap::from([(
        "other".to_owned(),
        ColumnBinding {
            data_manager: DataManagerKind::StandardStMan,
            tile_shape: None,
        },
    )]);
    table
        .save_with_bindings(options.clone(), &bindings)
        .unwrap();
    let snapshot = || {
        [
            "table.dat",
            "table.f0",
            "table.f1",
            "table.info",
            "table.lock",
        ]
        .map(|name| {
            let file = path.join(name);
            (file.metadata().unwrap().ino(), fs::read(file).unwrap())
        })
    };
    let prior = snapshot();
    for (column, value) in [("id", 2), ("other", 20)] {
        table
            .cell_accessor_mut(0, column)
            .unwrap()
            .set(Value::Scalar(ScalarValue::Int32(value)))
            .unwrap();
    }
    FAIL_AFTER_UPDATES.set(Some(1));
    let failure = table
        .save_with_bindings(options.clone(), &bindings)
        .unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("injected failure after live file update")
    );
    assert_eq!(snapshot(), prior);
    assert_eq!(fs::read_dir(path).unwrap().count(), prior.len());

    table
        .save_with_bindings(options.clone(), &bindings)
        .unwrap();
    let saved = Table::open(options).unwrap();
    for (column, value) in [("id", 2), ("other", 20)] {
        assert_eq!(
            saved.cell_accessor(0, column).unwrap().scalar().unwrap(),
            &ScalarValue::Int32(value)
        );
    }
}

fn scalar_table(path: &Path) -> TableOptions {
    let options = TableOptions::new(path);
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .unwrap();
    table.save(options.clone()).unwrap();
    // Publish an initial generation, so a lost or partial sync is detectable.
    table.save(options.clone()).unwrap();
    options
}

fn read_id(table: &Table) -> i32 {
    let ScalarValue::Int32(value) = table.cell_accessor(0, "id").unwrap().scalar().unwrap() else {
        panic!("expected an Int32 id")
    };
    *value
}

fn set_id(table: &mut Table, value: i32) {
    table
        .cell_accessor_mut(0, "id")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(value)))
        .unwrap();
}

fn generation(path: &Path) -> u32 {
    TableWriteLock::read_modify_counter(path).unwrap().unwrap()
}

#[test]
fn full_save_user_lock_rejects_stale_snapshot() {
    use crate::{LockMode, LockOptions, LockType};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let options = scalar_table(path);
    let mode = LockOptions::new(LockMode::UserLocking);
    let mut stale = Table::open_with_lock(options.clone(), mode.clone()).unwrap();
    assert!(stale.lock(LockType::Read, 1).unwrap());
    assert_eq!(read_id(&stale), 1);
    stale.unlock().unwrap();

    let mut writer = Table::open_with_lock(options.clone(), mode).unwrap();
    assert!(writer.lock(LockType::Write, 1).unwrap());
    set_id(&mut writer, 2);
    writer.save(options.clone()).unwrap();
    writer.unlock().unwrap();
    let published = generation(path);

    let attempted = stale.save(options.clone());
    let fresh = read_id(&Table::open(options.clone()).unwrap());
    let after = generation(path);
    assert!(
        matches!(attempted, Err(TableError::LockFailed { .. })) && fresh == 2 && after == published,
        "stale UserLocking save returned {attempted:?}, fresh row {fresh}, generation {published} -> {after}"
    );

    assert!(stale.lock(LockType::Write, 1).unwrap());
    assert_eq!(stale.locked_modify_counter().unwrap(), published);
    assert_eq!(read_id(&stale), 2);
    set_id(&mut stale, 3);
    stale.save(options.clone()).unwrap();
    stale.unlock().unwrap();
    assert_eq!(generation(path), published + 1);
    assert_eq!(read_id(&Table::open(options).unwrap()), 3);
}

#[test]
fn full_save_automatic_lock_checks_snapshot_generation() {
    use crate::{LockMode, LockOptions, LockType};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let options = scalar_table(path);
    let mut stale =
        Table::open_with_lock(options.clone(), LockOptions::new(LockMode::AutoLocking)).unwrap();
    assert_eq!(read_id(&stale), 1);
    stale.unlock().unwrap();
    let mut writer = Table::open(options.clone()).unwrap();
    set_id(&mut writer, 2);
    writer.save(options.clone()).unwrap();
    let published = generation(path);

    let attempted = stale.save(options.clone());
    let fresh = read_id(&Table::open(options.clone()).unwrap());
    assert!(
        matches!(attempted, Err(TableError::LockFailed { .. }))
            && fresh == 2
            && generation(path) == published,
        "stale AutoLocking save returned {attempted:?}, fresh row {fresh}"
    );

    assert!(stale.lock(LockType::Read, 1).unwrap());
    assert_eq!(read_id(&stale), 2);
    // Keep the read lock while two automatic saves publish successive writes.
    stale.save(options.clone()).unwrap();
    stale.save(options).unwrap();
    assert_eq!(stale.locked_modify_counter().unwrap(), published + 2);
    assert!(!stale.has_lock(LockType::Write));
    stale.unlock().unwrap();
}

#[test]
fn full_save_restores_after_a_short_sync_write_and_enospc() {
    use crate::{LockMode, LockOptions, LockType};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let options = scalar_table(path);
    let mut held =
        Table::open_with_lock(options.clone(), LockOptions::new(LockMode::UserLocking)).unwrap();
    assert!(held.lock(LockType::Read, 1).unwrap());
    assert_eq!(read_id(&held), 1);
    held.unlock().unwrap();
    let before = crate::lock::read_sync_data_from_table_dir(path)
        .unwrap()
        .unwrap();
    let lock_inode = path.join("table.lock").metadata().unwrap().ino();
    let files = ["table.dat", "table.f0", "table.info"];
    let contents = || files.map(|name| fs::read(path.join(name)).unwrap());
    let pristine = contents();
    let mut writer = Table::open(options.clone()).unwrap();
    set_id(&mut writer, 2);
    writer
        .keywords_mut()
        .upsert("revision", Value::Scalar(ScalarValue::Int32(2)));

    crate::lock::inject_short_sync_write();
    let saved = writer.save(options.clone());
    let after = crate::lock::read_sync_data_from_table_dir(path);
    let fresh = Table::open(options.clone()).map(|table| read_id(&table));
    assert!(
        saved.as_ref().is_err_and(|error| error
            .to_string()
            .contains(&std::io::Error::from_raw_os_error(libc::ENOSPC).to_string()))
            && after
                .as_ref()
                .is_ok_and(|sync| sync.as_ref() == Some(&before))
            && matches!(fresh, Ok(1)),
        "short sync write followed by ENOSPC: save {saved:?}, fresh row {fresh:?}, sync {after:?}"
    );
    assert_eq!(contents(), pristine);
    assert!(held.lock(LockType::Read, 1).unwrap());
    assert_eq!(read_id(&held), 1);
    assert_eq!(
        held.published_modify_counter().unwrap(),
        before.modify_counter
    );
    held.unlock().unwrap();

    writer.save(options.clone()).unwrap();
    assert_eq!(generation(path), before.modify_counter + 1);
    assert_eq!(
        path.join("table.lock").metadata().unwrap().ino(),
        lock_inode
    );
    assert!(held.lock(LockType::Read, 1).unwrap());
    assert_eq!(read_id(&held), 2);
    held.unlock().unwrap();
    assert_eq!(read_id(&Table::open(options).unwrap()), 2);
}

#[cfg(target_os = "linux")]
#[test]
fn full_save_overwrites_24_tiled_shapes_with_64_file_descriptors() {
    use casa_core::ArrayValue;
    const CHILD: &str = "CASA_RS_TEST_FULL_SAVE_NOFILE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "table::io::full_save::tests::full_save_overwrites_24_tiled_shapes_with_64_file_descriptors", "--nocapture"])
            .env(CHILD, "1")
            .output().unwrap();
        assert!(
            output.status.success(),
            "low-FD child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let options = TableOptions::new(path).with_data_manager(DataManagerKind::TiledShapeStMan);
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "data",
        PrimitiveType::Int32,
        Some(1),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for len in 1..=24 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::from_i32_vec(vec![1; len])),
            )]))
            .unwrap();
    }
    table.save(options.clone()).unwrap();
    let identities = || -> std::collections::BTreeMap<_, _> {
        fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("table.f"))
            .map(|entry| (entry.file_name(), entry.metadata().unwrap().ino()))
            .collect()
    };
    let previous = identities();
    assert_eq!(previous.len(), 25, "one header and 24 cube payloads");
    for row in 0..24 {
        table
            .cell_accessor_mut(row, "data")
            .unwrap()
            .set(Value::Array(ArrayValue::from_i32_vec(vec![2; row + 1])))
            .unwrap();
    }
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limits is initialized and points to valid rlimit storage.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) },
        0
    );
    limits.rlim_cur = 64;
    // SAFETY: this subprocess lowers only its soft limit for open descriptors.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limits) }, 0);
    FAIL_AFTER_UPDATES.set(Some(8));
    let failed = table.save(options.clone()).unwrap_err();
    assert!(
        failed
            .to_string()
            .contains("injected failure after live file update"),
        "{failed}"
    );
    assert_eq!(identities(), previous);
    let assert_cells = |expected| {
        let reopened = Table::open(options.clone()).unwrap();
        for row in 0..24 {
            assert_eq!(
                reopened
                    .cell_accessor(row, "data")
                    .unwrap()
                    .array()
                    .unwrap(),
                &ArrayValue::from_i32_vec(vec![expected; row + 1])
            );
        }
    };
    assert_cells(1);

    table
        .save(options.clone())
        .expect("overwrite a 24-cube table under a 64-FD limit");
    assert_eq!(identities(), previous);
    assert_cells(2);
}
