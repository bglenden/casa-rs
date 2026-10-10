// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's table write lock, held across one in-place change.

use std::path::{Path, PathBuf};

#[cfg(unix)]
use super::{LockFile, LockOptions, LockType, SyncData};
use crate::{Table, TableError};

/// casacore's table write lock, held for one in-place change of a table on
/// disk.
///
/// An in-place writer takes this lock before it changes a table and holds it
/// until the change is complete or abandoned, as casacore holds a table's
/// write lock while it writes. The lock is casacore's own: the `fcntl` write
/// lock on byte 0 of the table's `table.lock`. It therefore excludes casacore
/// processes, other casa-rs processes, and every other casa-rs handle on the
/// table in this process, which is refused exactly as another process is.
///
/// Releasing the lock after [`record_write`](Self::record_write) records the
/// change in the lock file's sync data (`TableSyncData`), as casacore does
/// when it releases a write lock, so a process holding the table open
/// re-reads it when it next takes a lock. Dropping the guard releases it the
/// same way: rows written before an interruption stay written, as after an
/// interrupted casacore write.
///
/// The guard works on the table directory, not on a [`Table`] handle, so it
/// can be held across calls that each borrow the handle. It is meant for
/// handles opened without locking ([`Table::open`]). A handle opened with
/// [`Table::open_with_lock`] in auto-locking mode cannot write while another
/// handle holds this lock: its temporary write lock is refused rather than
/// waited on, because the holder may be the same thread.
///
/// On platforms without `fcntl` locking the guard holds nothing.
///
/// # C++ equivalent
///
/// `Table::lock(FileLocker::Write, nattempts)` and `Table::unlock()` on a
/// table opened with `TableLock::UserLocking`.
pub struct TableWriteLock {
    path: PathBuf,
    #[cfg(unix)]
    lock_file: Option<LockFile>,
    written: Option<WrittenTable>,
}

/// The table shape a released write publishes in the sync data.
#[derive(Clone, Copy)]
#[cfg_attr(not(unix), allow(dead_code))]
struct WrittenTable {
    rows: u64,
    columns: u32,
    data_managers: usize,
}

impl TableWriteLock {
    /// Take the write lock on the table at `table_dir`.
    ///
    /// `nattempts` has the meaning of [`Table::lock`]: 1 tries once without
    /// waiting, more retries once a second, and 0 waits indefinitely for
    /// another process (a holder in this process is never waited on).
    ///
    /// # Errors
    ///
    /// [`TableError::LockFailed`] when another process or another handle in
    /// this process holds a conflicting lock after `nattempts`, and
    /// [`TableError::LockIo`] when `table.lock` cannot be opened or locked,
    /// for example on a file system without `fcntl` locks.
    pub fn acquire(table_dir: impl AsRef<Path>, nattempts: u32) -> Result<Self, TableError> {
        let path = table_dir.as_ref().to_path_buf();
        #[cfg(unix)]
        {
            let lock_io = |error: std::io::Error| TableError::LockIo {
                path: path.display().to_string(),
                message: error.to_string(),
            };
            let mut lock_file = LockFile::create_or_open(
                &path,
                false,
                LockOptions::default().inspection_interval,
                false,
            )
            .map_err(lock_io)?;
            if !lock_file
                .acquire(LockType::Write, nattempts)
                .map_err(lock_io)?
            {
                return Err(TableError::LockFailed {
                    path: path.display().to_string(),
                    message: "the table is write-locked by another process or another handle"
                        .into(),
                });
            }
            Ok(Self {
                path,
                lock_file: Some(lock_file),
                written: None,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = nattempts;
            Ok(Self {
                path,
                written: None,
            })
        }
    }

    /// The locked table directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Record that `table` is changed under this lock.
    ///
    /// The row count, column count and data managers of `table` as passed
    /// are published in the sync data when the lock is released. Call it
    /// again if the shape changes before release.
    pub fn record_write(&mut self, table: &Table) {
        self.written = Some(WrittenTable {
            rows: table.row_count() as u64,
            columns: table
                .schema()
                .map_or(0, |schema| schema.columns().len() as u32),
            data_managers: table.data_manager_info().len().max(1),
        });
    }

    /// Release the lock, publishing any recorded write in the sync data.
    ///
    /// # Errors
    ///
    /// [`TableError::LockIo`] when the sync data cannot be written or the
    /// lock cannot be released.
    pub fn release(mut self) -> Result<(), TableError> {
        self.release_now()
    }

    fn release_now(&mut self) -> Result<(), TableError> {
        #[cfg(unix)]
        {
            let Some(mut lock_file) = self.lock_file.take() else {
                return Ok(());
            };
            let lock_io = |error: std::io::Error| TableError::LockIo {
                path: self.path.display().to_string(),
                message: error.to_string(),
            };
            let published = match self.written {
                Some(written) => lock_file
                    .read_sync_data()
                    .map(|sync| sync.unwrap_or_else(SyncData::new))
                    .and_then(|mut sync| {
                        sync.record_write(
                            written.rows,
                            written.columns,
                            true,
                            &vec![true; written.data_managers],
                        );
                        lock_file.write_sync_data(&sync)
                    }),
                None => Ok(()),
            };
            let released = lock_file.release().map(|_| ());
            published.and(released).map_err(lock_io)
        }
        #[cfg(not(unix))]
        {
            Ok(())
        }
    }
}

impl Drop for TableWriteLock {
    fn drop(&mut self) {
        let _ = self.release_now();
    }
}

impl std::fmt::Debug for TableWriteLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TableWriteLock")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
