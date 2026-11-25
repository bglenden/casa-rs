// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's table write lock, held across one in-place change.

use std::path::{Path, PathBuf};

#[cfg(unix)]
use super::lock_file::path_key;
#[cfg(unix)]
use super::{LockFile, LockOptions, LockOutcome, LockType, SyncData};
use crate::TableError;

/// Casacore's in-use marker for an open table that acquires access locks
/// separately for its I/O.
pub struct TableUseLock {
    #[cfg(unix)]
    _lock_file: Option<LockFile>,
    #[cfg(unix)]
    directory: std::fs::File,
}

impl TableUseLock {
    /// Mark the table open for other processes until this guard is dropped.
    ///
    /// # Errors
    /// Returns an error if the table cannot be opened or a writer holds an
    /// access lock while this handle attaches.
    pub fn acquire(path: &Path) -> Result<Self, TableError> {
        Self::open(path, false, 1)
    }

    /// Marks the table open, waiting if another process holds its write lock.
    ///
    /// Like an automatic casacore open, a wait registers this process in
    /// the table's request list. The temporary read lock is released after
    /// the in-use marker is established.
    pub fn acquire_waiting(path: &Path) -> Result<Self, TableError> {
        Self::open(path, false, 0)
    }

    /// Mark a read-only attachment as open, waiting for a current writer.
    ///
    /// # Errors
    /// Returns an error for an unreadable table or a failed lock request.
    pub fn acquire_waiting_readonly(path: &Path) -> Result<Self, TableError> {
        Self::open(path, true, 0)
    }

    /// Mark a read-only attachment as open where locking is available.
    /// A read-only directory without a lock file can still be read. The
    /// guard retains the directory identity even when it cannot mark it open.
    ///
    /// # Errors
    /// Returns an error for an unreadable table or a conflicting writer.
    pub fn acquire_readonly(path: &Path) -> Result<Self, TableError> {
        Self::open(path, true, 1)
    }

    /// Whether a read-only directory has no lock file to join.
    pub fn readonly_without_lock_file(path: &Path) -> bool {
        let missing = path
            .join("table.lock")
            .symlink_metadata()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound);
        missing
            && crate::publication::check_write_access(path).is_err_and(|error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
                )
            })
    }

    fn open(path: &Path, readonly: bool, attempts: u32) -> Result<Self, TableError> {
        #[cfg(unix)]
        {
            let lock_io = |error: std::io::Error| TableError::LockIo {
                path: path.display().to_string(),
                message: error.to_string(),
            };
            let directory = std::fs::File::open(path).map_err(lock_io)?;
            let mut lock_file = match LockFile::create_or_open(
                path,
                false,
                LockOptions::default().inspection_interval,
                false,
            ) {
                Ok(lock_file) => lock_file,
                Err(error)
                    if readonly
                        && matches!(
                            error.kind(),
                            std::io::ErrorKind::PermissionDenied
                                | std::io::ErrorKind::ReadOnlyFilesystem
                        )
                        && !path.join("table.lock").exists() =>
                {
                    return Ok(Self {
                        _lock_file: None,
                        directory,
                    });
                }
                Err(error) => return Err(lock_io(error)),
            };
            let outcome = lock_file
                .acquire(LockType::Read, attempts)
                .map_err(lock_io)?;
            if !outcome.is_acquired() {
                return Err(TableError::LockFailed {
                    path: path.display().to_string(),
                    message: outcome.refusal(LockType::Read, attempts),
                });
            }
            lock_file.release().map_err(lock_io)?;
            Ok(Self {
                _lock_file: Some(lock_file),
                directory,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (path, readonly, attempts);
            Ok(Self {})
        }
    }

    /// Check whether the table's original directory is still at `path`.
    pub fn matches_directory(&self, path: &Path) -> bool {
        std::fs::metadata(path).is_ok_and(|current| self.matches_directory_metadata(&current))
    }

    /// Check a directory entry captured without following symbolic links.
    pub fn matches_directory_metadata(&self, current: &std::fs::Metadata) -> bool {
        if !current.is_dir() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            self.directory
                .metadata()
                .is_ok_and(|old| old.dev() == current.dev() && old.ino() == current.ino())
        }
        #[cfg(not(unix))]
        {
            true
        }
    }
}

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
    directory_key: (u64, u64),
    #[cfg(unix)]
    lock_file: Option<LockFile>,
    /// Whether a write was recorded; releasing then publishes it.
    #[cfg_attr(not(unix), allow(dead_code))]
    written: bool,
    /// `table.dat` as it was when the lock was taken, `None` when it could
    /// not be read; the publication compares the file with it.
    #[cfg(unix)]
    table_dat_at_acquire: Option<Vec<u8>>,
}

pub(crate) struct WriteSyncCheckpoint {
    written: bool,
    #[cfg(unix)]
    sync: Option<SyncData>,
    #[cfg(unix)]
    descriptor_before_write: Option<Vec<u8>>,
}

impl TableWriteLock {
    /// Inspect the last published modification counter before reading data.
    ///
    /// This call takes no lock. A caller that later acquires a write lock
    /// must compare the captured counter with [`Self::modify_counter`]
    /// before acting on a previously read snapshot.
    ///
    /// # Errors
    /// Returns an error when the lock file's synchronization data cannot
    /// be read.
    pub fn read_modify_counter(table_dir: &Path) -> Result<Option<u32>, TableError> {
        #[cfg(unix)]
        {
            super::read_sync_data_from_table_dir(table_dir)
                .map(|sync| sync.map(|sync| sync.modify_counter))
                .map_err(|error| TableError::LockIo {
                    path: table_dir.display().to_string(),
                    message: error.to_string(),
                })
        }
        #[cfg(not(unix))]
        {
            let _ = table_dir;
            Ok(None)
        }
    }

    /// Whether another process still has the table open, even without a
    /// read or write lock. Check this before removing a locked table.
    pub fn is_multi_used(&self) -> bool {
        #[cfg(unix)]
        {
            self.lock_file.as_ref().is_some_and(LockFile::is_multi_used)
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

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
        Self::take(table_dir.as_ref(), nattempts, true)
    }

    /// Take the write lock as [`acquire`](Self::acquire) does, for a writer
    /// that reads the table afresh once it holds it (it reopens the table
    /// under the lock): a write another process published while it waited
    /// is not stale for that writer, so it is not refused.
    ///
    /// # Errors
    ///
    /// As [`acquire`](Self::acquire), except for a write published while
    /// this writer waited.
    pub fn acquire_to_reread(
        table_dir: impl AsRef<Path>,
        nattempts: u32,
    ) -> Result<Self, TableError> {
        Self::take(table_dir.as_ref(), nattempts, false)
    }

    fn take(table_dir: &Path, nattempts: u32, refuse_stale: bool) -> Result<Self, TableError> {
        let path = table_dir.to_path_buf();
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
            let directory_key = path_key(&path)
                .map_err(lock_io)?
                .ok_or_else(|| lock_failed("table directory is missing".into()))?;
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
                if refuse_stale
                    && outcome.is_acquired()
                    && published_modify_counter(&lock_file) != published
                {
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
            if !matches_table_identity(&path, directory_key, &lock_file) {
                return Err(lock_failed("table directory or lock was replaced".into()));
            }
            let table_dat_at_acquire =
                std::fs::read(path.join(crate::storage::TABLE_CONTROL_FILE)).ok();
            Ok(Self {
                path,
                directory_key,
                lock_file: Some(lock_file),
                written: false,
                table_dat_at_acquire,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (nattempts, refuse_stale);
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

    /// Check that the current path still names the directory and lock file
    /// captured by this guard.
    #[must_use]
    pub fn matches_table_path(&self, path: &Path) -> bool {
        #[cfg(unix)]
        {
            self.lock_file
                .as_ref()
                .is_some_and(|lock| matches_table_identity(path, self.directory_key, lock))
        }
        #[cfg(not(unix))]
        {
            self.path == path
        }
    }

    /// Follow a directory rename while retaining this table's write lock.
    ///
    /// This updates the path used to publish synchronization data. The
    /// directory and lock inode must still be those this guard captured.
    ///
    /// # Errors
    /// The new path does not name the locked table.
    pub fn follow_directory_rename(&mut self, path: &Path) -> Result<(), TableError> {
        if !self.matches_table_path(path) {
            return Err(TableError::LockFailed {
                path: path.display().to_string(),
                message: "renamed directory does not hold this table lock".into(),
            });
        }
        self.path = path.to_path_buf();
        Ok(())
    }

    /// The modify counter the table's sync data publishes, read now; `None`
    /// when the table has no sync data.
    ///
    /// While this lock is held no other process can publish a write, so a
    /// writer that releases the lock and takes it again later compares two
    /// readings to learn whether another process wrote the table in between.
    ///
    /// # Errors
    ///
    /// [`TableError::LockIo`] when the sync data cannot be read.
    pub fn modify_counter(&self) -> Result<Option<u32>, TableError> {
        #[cfg(unix)]
        {
            let Some(lock_file) = &self.lock_file else {
                return Ok(None);
            };
            lock_file
                .read_sync_data()
                .map(|sync| sync.map(|sync| sync.modify_counter))
                .map_err(|error| TableError::LockIo {
                    path: self.path.display().to_string(),
                    message: error.to_string(),
                })
        }
        #[cfg(not(unix))]
        {
            Ok(None)
        }
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

    pub(crate) fn checkpoint_sync(&self) -> Result<WriteSyncCheckpoint, TableError> {
        Ok(WriteSyncCheckpoint {
            written: self.written,
            #[cfg(unix)]
            descriptor_before_write: self.table_dat_at_acquire.clone(),
            #[cfg(unix)]
            sync: self
                .sync_file()?
                .read_sync_data()
                .map_err(|error| self.sync_error(error))?,
        })
    }

    pub(crate) fn sync_recorded_write(&mut self) -> Result<(), TableError> {
        if !self.written {
            return Ok(());
        }
        #[cfg(unix)]
        {
            let lock = self.sync_file()?;
            publish_persisted_write(lock, &self.path, self.table_dat_at_acquire.as_deref())
                .map_err(|error| self.sync_error(error))?;
            self.table_dat_at_acquire =
                std::fs::read(self.path.join(crate::storage::TABLE_CONTROL_FILE)).ok();
        }
        self.written = false;
        Ok(())
    }

    pub(crate) fn restore_sync(
        &mut self,
        previous: WriteSyncCheckpoint,
        attempted_sync: bool,
    ) -> Result<(), TableError> {
        #[cfg(unix)]
        {
            if attempted_sync {
                let lock = self.sync_file()?;
                match previous.sync {
                    Some(sync) => lock.write_sync_data(&sync),
                    None => lock.clear_sync_data(),
                }
                .map_err(|error| self.sync_error(error))?;
            }
            self.table_dat_at_acquire = previous.descriptor_before_write;
        }
        #[cfg(not(unix))]
        let _ = attempted_sync;
        self.written = previous.written;
        Ok(())
    }

    #[cfg(unix)]
    fn sync_file(&self) -> Result<&LockFile, TableError> {
        self.lock_file
            .as_ref()
            .filter(|file| matches_table_identity(&self.path, self.directory_key, file))
            .ok_or_else(|| self.sync_error("table directory or lock was replaced"))
    }

    #[cfg(unix)]
    fn sync_error(&self, error: impl std::fmt::Display) -> TableError {
        TableError::LockIo {
            path: self.path.display().to_string(),
            message: error.to_string(),
        }
    }

    /// Release the lock, publishing any recorded write in the sync data.
    ///
    /// The published row, column and data-manager counts describe the table
    /// as persisted, not an in-memory handle: casacore takes a reopened
    /// table's row count from the sync data and asserts that its
    /// per-data-manager counters match the table's data managers. They are
    /// read from the table's `table.dat` when this writer changed its bytes;
    /// when the file is as it was when the lock was taken, the row count
    /// published before is kept, since the file can trail it (#880).
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
                if matches_table_identity(&self.path, self.directory_key, &lock_file) {
                    publish_persisted_write(
                        &lock_file,
                        &self.path,
                        self.table_dat_at_acquire.as_deref(),
                    )
                    .map(|_| ())
                } else {
                    Err(std::io::Error::other(
                        "table directory or lock was replaced",
                    ))
                }
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

#[cfg(unix)]
fn matches_table_identity(path: &Path, directory_key: (u64, u64), lock: &LockFile) -> bool {
    path_key(path).is_ok_and(|key| key == Some(directory_key)) && lock.matches_table_path(path)
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

/// Whether `table_dir`'s `table.dat` still holds `before`, byte for byte.
#[cfg(unix)]
fn table_dat_unchanged(table_dir: &Path, before: &[u8]) -> bool {
    std::fs::read(table_dir.join(crate::storage::TABLE_CONTROL_FILE)).is_ok_and(|now| now == before)
}

/// Publish a write to the table at `table_dir` in its lock file's sync data,
/// describing the table as persisted: its row and column counts and one
/// change counter per data manager, as casacore's `PlainTable::putFile`
/// publishes them. The counters continue from the sync data the lock file
/// holds, and every data manager counts as changed. The caller holds the
/// table's write lock. Returns the published sync data.
///
/// `table_dat_before` is `table.dat` as it was before the write. A write
/// that changed the file's bytes publishes the counts it gives, which may be
/// fewer rows than before, as casacore's full `putFile` flushes `table.dat`
/// before it publishes. A write that left the file's bytes as they were (one
/// that wrote only data, stopped before it reached the file, wrote nothing,
/// or wrote the file back unchanged) keeps the row count published before,
/// as casacore's data-only `putFile` publishes the count it holds without
/// rewriting the file. The file can trail that count (#880), and publishing
/// the file's would then announce fewer rows than the storage managers hold,
/// which casacore refuses.
#[cfg(unix)]
pub(crate) fn publish_persisted_write(
    lock_file: &LockFile,
    table_dir: &Path,
    table_dat_before: Option<&[u8]>,
) -> std::io::Result<SyncData> {
    let shape = PersistedShape::read(table_dir)?;
    let published = lock_file.read_sync_data()?;
    let rows = match &published {
        Some(published)
            if table_dat_before.is_some_and(|before| table_dat_unchanged(table_dir, before)) =>
        {
            published.nrrow
        }
        _ => shape.rows,
    };
    let mut sync = published.unwrap_or_else(SyncData::new);
    sync.record_write(rows, shape.columns, true, &vec![true; shape.data_managers]);
    lock_file.write_sync_data(&sync)?;
    Ok(sync)
}

/// Brings the row count the lock file's sync data publishes to the rows
/// `table.dat` holds now, while the write lock is still held, and changes
/// nothing else in it: no counter moves, so the release publishes the write
/// once, as it does for every write. Nothing is done when no sync data has
/// been published (the count then reads as 0, and the table's own is used).
///
/// A full save that removed rows needs this: until the release, a
/// metadata-only save, the admission of a partial save and a reload read
/// that count back and take the larger of it and `table.dat`'s, which would
/// raise the file to the rows the save removed.
#[cfg(unix)]
pub(crate) fn publish_persisted_rows(
    lock_file: &LockFile,
    table_dir: &Path,
) -> std::io::Result<()> {
    let Some(mut sync) = lock_file.read_sync_data()? else {
        return Ok(());
    };
    let rows = PersistedShape::read(table_dir)?.rows;
    if sync.nrrow != rows {
        sync.nrrow = rows;
        lock_file.write_sync_data(&sync)?;
    }
    Ok(())
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
