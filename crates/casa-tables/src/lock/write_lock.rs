// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's table write lock, held across one in-place change.

use std::path::{Path, PathBuf};

#[cfg(unix)]
use super::{LockFile, LockOptions, LockOutcome, LockType, SyncData};
use crate::TableError;

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
/// open with `AutoLocking` releases its lock at its next inspection.
/// casacore throttles inspections (`LockFile::inspect` looks at the request
/// list only after 25 table accesses and the inspection interval), so a
/// lightly used holder may keep its lock until it closes the table. A
/// casa-rs process does not yet release a lock on request, so a waiter waits
/// for it to finish ([#694](https://github.com/bglenden/casa-rs/issues/694)).
/// A write lock
/// held by another handle in this process is never waited for, because that
/// handle may belong to the waiting thread: it is refused at once.
///
/// Releasing the lock after [`record_write`](Self::record_write) records the
/// change in the lock file's sync data (`TableSyncData`), as casacore does
/// when it releases a write lock, so a process holding the table open
/// re-reads it when it next takes a lock. The sync data describes the table
/// as persisted when the lock is released. Dropping the guard releases it
/// the same way: rows written before an interruption stay written, as after
/// an interrupted casacore write, and are announced.
///
/// The guard works on the table directory, not on a [`Table`](crate::Table)
/// handle, so it can be held across calls that each borrow the handle. It is
/// meant for handles opened without locking
/// ([`Table::open`](crate::Table::open)). A handle opened with
/// [`Table::open_with_lock`](crate::Table::open_with_lock) in auto-locking
/// mode cannot write while another
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
    /// Whether a write was recorded; releasing then publishes it.
    #[cfg_attr(not(unix), allow(dead_code))]
    written: bool,
}

impl TableWriteLock {
    /// Take the write lock on the table at `table_dir`.
    ///
    /// `nattempts` has the meaning of [`Table::lock`](crate::Table::lock): 1 tries once without
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
                written: false,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = nattempts;
            Ok(Self {
                path,
                written: false,
            })
        }
    }

    /// The locked table directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Record that the table is, or may be, changed on disk under this lock.
    ///
    /// Call it before writing, so that a write interrupted part way is still
    /// announced. Releasing the lock then publishes the change in the sync
    /// data, describing the table as it is persisted when the lock is
    /// released (see [`release`](Self::release)); a table this writer never
    /// reached is left unrecorded and publishes nothing.
    pub fn record_write(&mut self) {
        self.written = true;
    }

    /// Release the lock, publishing any recorded write in the sync data.
    ///
    /// The published row, column and data-manager counts are read from the
    /// table's `table.dat` as persisted, not taken from an in-memory handle:
    /// casacore takes a reopened table's row count from the sync data and
    /// asserts that its per-data-manager counters match the table's data
    /// managers.
    ///
    /// # Errors
    ///
    /// [`TableError::LockIo`] when the persisted table cannot be read, the
    /// sync data cannot be written or the lock cannot be released.
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
            let published = if self.written {
                publish_persisted_write(&lock_file, &self.path).map(|_| ())
            } else {
                Ok(())
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

/// The shape of a table as persisted in its `table.dat`, which the sync data
/// published for it must describe.
#[cfg(unix)]
struct PersistedShape {
    rows: u64,
    columns: u32,
    data_managers: usize,
}

#[cfg(unix)]
impl PersistedShape {
    fn read(table_dir: &Path) -> std::io::Result<Self> {
        let contents = crate::storage::table_control::read_table_dat(&table_dir.join("table.dat"))
            .map_err(|error| {
                std::io::Error::other(format!(
                    "read the persisted table {} to publish its write: {error}",
                    table_dir.display()
                ))
            })?;
        Ok(Self {
            rows: contents.nrrow,
            columns: contents.table_desc.columns.len() as u32,
            data_managers: contents.column_set.data_managers.len(),
        })
    }
}

/// Publish a write to the table at `table_dir` in its lock file's sync data,
/// describing the table as persisted: its row and column counts and one
/// change counter per data manager, as casacore's `PlainTable::putFile`
/// publishes them. The counters continue from the sync data the lock file
/// holds, and every data manager counts as changed. The caller holds the
/// table's write lock. Returns the published sync data.
#[cfg(unix)]
pub(crate) fn publish_persisted_write(
    lock_file: &LockFile,
    table_dir: &Path,
) -> std::io::Result<SyncData> {
    let shape = PersistedShape::read(table_dir)?;
    let mut sync = lock_file.read_sync_data()?.unwrap_or_else(SyncData::new);
    sync.record_write(
        shape.rows,
        shape.columns,
        true,
        &vec![true; shape.data_managers],
    );
    lock_file.write_sync_data(&sync)?;
    Ok(sync)
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
