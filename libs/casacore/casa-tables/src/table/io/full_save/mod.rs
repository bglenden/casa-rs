// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::Path;

use crate::storage::{StorageError, TABLE_CONTROL_FILE, full_save, table_control::StagedTableDat};
use crate::{Table, TableError, TableWriteLock};

#[cfg(unix)]
use super::super::{LockMode, LockState, LockType, locking::same_directory};

impl Table {
    pub(super) fn write_full_table(
        &self,
        path: &Path,
        external: Option<&TableWriteLock>,
        validate: bool,
        write: impl FnOnce(StagedTableDat<'_>) -> Result<(), TableError>,
    ) -> Result<(), TableError> {
        if let Some(lock) = external
            && !lock.matches_table_path(path)
        {
            return Err(lock_error(
                path,
                "destination does not match the supplied write lock",
            ));
        }
        #[cfg(unix)]
        let state = self
            .lock_state
            .as_ref()
            .filter(|state| same_directory(&state.path, path));
        #[cfg(unix)]
        let validate_under_lock = validate && state.is_some();
        #[cfg(not(unix))]
        let validate_under_lock = false;
        if validate && !validate_under_lock {
            self.validate()?;
        }
        let write = |control: StagedTableDat<'_>| {
            if validate_under_lock {
                self.validate()?;
            }
            write(control)
        };

        fs::create_dir_all(path).map_err(StorageError::from)?;
        let descriptor = path.join(TABLE_CONTROL_FILE);
        if descriptor.exists() {
            crate::publication::check_write_access(&descriptor).map_err(StorageError::from)?;
        }
        #[cfg(unix)]
        {
            if let Some(state) = state {
                state.admit_full_save(path)?;
                if state.lock_file.has_lock(LockType::Write) {
                    self.publish_full_table(path, &state.lock_file, Some(state), write)?;
                    return Ok(());
                }
            }
            if let Some(lock) = external {
                self.publish_full_table(path, lock.sync_file()?, state, write)?;
                return Ok(());
            }

            // An automatic handle may use a temporary guard. Its snapshot
            // is checked against the generation observed through this guard
            // before the immutable save can publish it.
            let attempts = if state.is_some() { 0 } else { 1 };
            let lock = TableWriteLock::acquire(path, attempts)?;
            self.publish_full_table(path, lock.sync_file()?, state, write)?;
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
        state: Option<&LockState>,
        write: impl FnOnce(StagedTableDat<'_>) -> Result<(), TableError>,
    ) -> Result<(), TableError> {
        if let Some(state) = state {
            let published = lock
                .read_sync_data()
                .map_err(|error| sync_error(path, error))?
                .map_or(0, |sync| sync.modify_counter);
            if published != state.observed_modify_counter() {
                return Err(lock_error(
                    path,
                    "table changed since this handle last synchronized; reacquire its write lock and retry",
                ));
            }
        }
        let (mut edit, overwriting) = full_save::prepare(path, || lock.is_multi_used(), write)?;
        if !lock.matches_table_path(path) {
            return Err(lock_error(
                path,
                "table.lock was replaced before full save publication",
            ));
        }
        if let Err(error) = edit.apply() {
            return Err(edit.restore_after(error).into());
        }
        if !overwriting {
            return Ok(());
        }
        if state.is_some_and(|state| state.lock_file.has_lock(LockType::Write)) {
            // Keep the caller's publication boundary, but update the count
            // now so subsequent I/O under this lock sees any removed rows.
            crate::lock::publish_persisted_rows(lock, path)
                .map_err(|error| edit.restore_after(StorageError::from(error)))?;
            self.note_persisted_write(path);
        } else {
            let sync = crate::lock::publish_persisted_write(lock, path, None)
                .map_err(|error| edit.restore_after(StorageError::from(error)))?;
            if let Some(state) = state {
                state.observe_modify_counter(sync.modify_counter);
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
impl LockState {
    fn admit_full_save(&self, path: &Path) -> Result<(), TableError> {
        if !self.lock_file.matches_table_path(path) {
            return Err(lock_error(
                path,
                "table.lock was replaced since this handle opened",
            ));
        }
        if self.options.read_only {
            return Err(lock_error(
                path,
                "cannot save through a read-only table handle",
            ));
        }
        if !self.lock_file.has_lock(LockType::Write)
            && matches!(
                self.options.mode,
                LockMode::UserLocking
                    | LockMode::UserNoReadLocking
                    | LockMode::PermanentLocking
                    | LockMode::PermanentLockingWait
            )
        {
            return Err(lock_error(
                path,
                "full save requires this handle's write lock",
            ));
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

#[cfg(test)]
#[cfg(unix)]
mod tests;
