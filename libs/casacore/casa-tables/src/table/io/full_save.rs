// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::Path;

use crate::storage::{StorageError, TABLE_CONTROL_FILE, full_save, table_control::StagedTableDat};
use crate::{Table, TableError, TableWriteLock};

impl Table {
    pub(super) fn write_full_table(
        &self,
        path: &Path,
        external: Option<&TableWriteLock>,
        write: impl FnOnce(StagedTableDat<'_>) -> Result<(), StorageError>,
    ) -> Result<(), TableError> {
        if let Some(lock) = external
            && !lock.matches_table_path(path)
        {
            return Err(lock_error(
                path,
                "destination does not match the supplied write lock",
            ));
        }
        fs::create_dir_all(path).map_err(StorageError::from)?;
        let descriptor = path.join(TABLE_CONTROL_FILE);
        if descriptor.exists() {
            crate::publication::check_write_access(&descriptor).map_err(StorageError::from)?;
        }
        #[cfg(unix)]
        if let Some(lock) = external {
            return self.publish_full_table(path, lock.sync_file()?, false, write);
        }
        #[cfg(unix)]
        {
            if let Some(state) = self.lock_state.as_ref()
                && super::super::locking::same_directory(&state.path, path)
                && state.lock_file.has_lock(crate::LockType::Write)
            {
                return self.publish_full_table(path, &state.lock_file, true, write);
            }
            let lock = TableWriteLock::acquire(path, 1)?;
            self.publish_full_table(path, lock.sync_file()?, false, write)?;
            lock.release()?;
        }
        #[cfg(not(unix))]
        {
            let (mut edit, _) = full_save::prepare(path, || false, write)?;
            if let Err(error) = edit.apply() {
                return Err(edit.restore_after(error).into());
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    fn publish_full_table(
        &self,
        path: &Path,
        lock: &crate::lock::LockFile,
        held_by_table: bool,
        write: impl FnOnce(StagedTableDat<'_>) -> Result<(), StorageError>,
    ) -> Result<(), TableError> {
        let (mut edit, overwriting) = full_save::prepare(path, || lock.is_multi_used(), write)?;
        if !lock.matches_table_path(path) {
            return Err(lock_error(
                path,
                "table.lock was replaced before full save publication",
            ));
        }
        let previous_sync = lock
            .read_sync_data()
            .map_err(|error| sync_error(path, error))?;
        if let Err(error) = edit.apply() {
            return Err(edit.restore_after(error).into());
        }
        if !overwriting {
            return Ok(());
        }
        let result = if held_by_table {
            // Keep the caller's publication boundary, but update the count
            // now so subsequent I/O under this lock sees any removed rows.
            crate::lock::publish_persisted_rows(lock, path)
        } else {
            crate::lock::publish_persisted_write(lock, path, None).map(|_| ())
        };
        if let Err(error) = result {
            let restored = edit.restore_after(StorageError::from(error));
            let restored_sync = match previous_sync {
                Some(sync) => lock.write_sync_data(&sync),
                None => lock.clear_sync_data(),
            };
            if let Err(error) = restored_sync {
                return Err(sync_error(
                    path,
                    format!("{restored}; restoring table.lock: {error}"),
                ));
            }
            return Err(restored.into());
        }
        if held_by_table {
            self.note_persisted_write(path);
        }
        Ok(())
    }
}

fn lock_error(path: &Path, message: impl Into<String>) -> TableError {
    TableError::LockFailed {
        path: path.display().to_string(),
        message: message.into(),
    }
}

#[cfg(unix)]
fn sync_error(path: &Path, message: impl std::fmt::Display) -> TableError {
    TableError::LockIo {
        path: path.display().to_string(),
        message: message.to_string(),
    }
}

#[cfg(all(test, unix))]
mod tests {
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
}
