// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's table write lock, held across one in-place change.

use std::path::{Path, PathBuf};

#[cfg(unix)]
use super::{LockFile, LockOptions, LockOutcome, LockType, SyncData};
use crate::{Table, TableError};

/// casacore's table write lock, held for one in-place change of a table on
/// disk.
///
/// An in-place writer takes this lock before it changes a table and holds it
/// until the change is complete or abandoned, as casacore holds a table's
/// write lock while it writes. The lock is casacore's own: the `fcntl` write
/// lock on byte 0 of the table's `table.lock`. It therefore excludes casacore
/// processes, other casa-rs processes, and every other casa-rs handle on the
/// table in this process.
///
/// A lock another process holds, read or write, is waited for as casacore
/// waits (see [`acquire`](Self::acquire)): the waiter adds its process id to
/// the request list in `table.lock`, so a casacore process holding the table
/// open with `AutoLocking` releases its lock at its next inspection, which
/// casacore makes when the table is next used. A casa-rs process does not
/// yet release a lock on request, so a waiter waits for it to finish
/// ([#694](https://github.com/bglenden/casa-rs/issues/694)). A write lock
/// held by another handle in this process is never waited for, because that
/// handle may belong to the waiting thread: it is refused at once.
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
/// On a file system without lock support (`fcntl` refused with `ENOLCK`, or
/// `ENOTSUP` as on macOS SMB mounts) the lock counts as acquired, with one
/// warning per lock file, as casacore does for `ENOLCK`: other handles in this
/// process are still refused, other processes are not excluded. On platforms
/// without `fcntl` locking the guard holds nothing.
///
/// # C++ equivalent
///
/// `Table::lock(FileLocker::Write, nattempts)` and `Table::unlock()` on a
/// table opened with `TableLock::UserLocking`.
pub struct TableWriteLock {
    path: PathBuf,
    #[cfg(unix)]
    lock_file: Option<LockFile>,
    #[cfg_attr(not(unix), allow(dead_code))]
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
    /// another process, as casacore's default `AutoLocking` does. While it
    /// waits, this process's id is in the lock file's request list, and the
    /// wait is logged when it starts, every ten seconds and when it ends. An
    /// indefinite wait blocks in the kernel, which refuses it when it would
    /// deadlock with another waiting process. A holder in this process is
    /// never waited for.
    ///
    /// A writer that had to wait checks the lock file's sync data once it
    /// holds the lock. When another process published a write to the table
    /// while it waited, the rows and metadata the caller read beforehand are
    /// stale; casacore would re-read them, which a handle opened without
    /// locking cannot, so the lock is released and the request refused.
    ///
    /// # Errors
    ///
    /// [`TableError::LockFailed`] when another handle in this process holds
    /// the write lock, when another process still holds a conflicting lock
    /// after `nattempts` (never with 0), when waiting would deadlock, or when
    /// another process wrote the table while this writer waited; and
    /// [`TableError::LockIo`] when
    /// `table.lock` cannot be opened or `fcntl` fails for a reason other than
    /// a held lock or missing lock support.
    pub fn acquire(table_dir: impl AsRef<Path>, nattempts: u32) -> Result<Self, TableError> {
        let path = table_dir.as_ref().to_path_buf();
        #[cfg(unix)]
        {
            let lock_io = |error: std::io::Error| TableError::LockIo {
                path: path.display().to_string(),
                message: error.to_string(),
            };
            let lock_failed = |message: String| TableError::LockFailed {
                path: path.display().to_string(),
                message,
            };
            let mut lock_file = LockFile::create_or_open(
                &path,
                false,
                LockOptions::default().inspection_interval,
                false,
            )
            .map_err(lock_io)?;
            let mut outcome = lock_file.acquire(LockType::Write, 1).map_err(lock_io)?;
            if outcome == LockOutcome::HeldByAnotherProcess && nattempts != 1 {
                let published = published_modify_counter(&lock_file);
                outcome = lock_file
                    .acquire(LockType::Write, nattempts)
                    .map_err(lock_io)?;
                if outcome.is_acquired() && published_modify_counter(&lock_file) != published {
                    return Err(lock_failed(
                        "another process wrote the table while this writer waited for its \
                         write lock; reopen the table and retry"
                            .into(),
                    ));
                }
            }
            if !outcome.is_acquired() {
                return Err(lock_failed(outcome.refusal(LockType::Write, nattempts)));
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

/// The modify counter the lock file's sync data publishes, `None` when it has
/// none. A read that fails means a writer was publishing at that moment, and
/// counts as `None`, so a change is still seen.
#[cfg(unix)]
fn published_modify_counter(lock_file: &LockFile) -> Option<u32> {
    lock_file
        .read_sync_data()
        .ok()
        .flatten()
        .map(|sync| sync.modify_counter)
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
