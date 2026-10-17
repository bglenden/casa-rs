// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

impl Table {
    /// Opens an existing table from disk with locking.
    ///
    /// Behaves like [`open`](Table::open) but also creates or opens the
    /// `table.lock` file and acquires a lock according to the given
    /// [`LockOptions`].
    ///
    /// - [`LockMode::PermanentLocking`]: acquires a write lock immediately;
    ///   fails if unavailable.
    /// - [`LockMode::PermanentLockingWait`]: acquires a write lock, waiting
    ///   indefinitely for another process.
    /// - [`LockMode::AutoLocking`]: acquires a read lock, waiting
    ///   indefinitely, as casacore does, while another process holds the
    ///   write lock; write operations temporarily acquire/release a write
    ///   lock.
    /// - [`LockMode::UserLocking`]: no lock is acquired until
    ///   [`lock()`](Table::lock) is called.
    /// - [`LockMode::NoLocking`]: equivalent to [`open()`](Table::open).
    ///
    /// A wait adds this process to the request list in `table.lock`, so a
    /// casacore holder using `AutoLocking` releases its lock at its next
    /// inspection, and is logged when it starts, every ten seconds and when
    /// it ends. A write lock held by another handle in this process is never
    /// waited for. The table's metadata is read only once the lock is held.
    ///
    /// C++ equivalent: `Table(name, TableLock(...), Table::Old)`.
    #[cfg(unix)]
    pub fn open_with_lock(
        options: TableOptions,
        lock_opts: LockOptions,
    ) -> Result<Self, TableError> {
        if lock_opts.mode == LockMode::NoLocking {
            return Self::open(options);
        }

        let perm = matches!(
            lock_opts.mode,
            LockMode::PermanentLocking | LockMode::PermanentLockingWait
        );
        let mut lock_file =
            LockFile::create_or_open(&options.path, false, lock_opts.inspection_interval, perm)
                .map_err(|e| TableError::LockIo {
                    path: options.path.display().to_string(),
                    message: e.to_string(),
                })?;

        // Acquire the initial lock the mode asks for, with casacore's number
        // of attempts (`TableLockData::makeLock`, `PlainTable`).
        let initial = match lock_opts.mode {
            LockMode::PermanentLocking => Some((LockType::Write, 1)),
            LockMode::PermanentLockingWait => Some((LockType::Write, 0)),
            LockMode::AutoLocking | LockMode::DefaultLocking => Some((LockType::Read, 0)),
            // AutoNoReadLocking skips the read lock on open; only write
            // locks are acquired.
            LockMode::AutoNoReadLocking
            | LockMode::UserLocking
            | LockMode::UserNoReadLocking
            | LockMode::NoLocking => None,
        };
        if let Some((lock_type, nattempts)) = initial {
            let outcome =
                lock_file
                    .acquire(lock_type, nattempts)
                    .map_err(|e| TableError::LockIo {
                        path: options.path.display().to_string(),
                        message: e.to_string(),
                    })?;
            if !outcome.is_acquired() {
                return Err(TableError::LockFailed {
                    path: options.path.display().to_string(),
                    message: outcome.refusal(lock_type, nattempts),
                });
            }
        }

        // Metadata must be read only after the initial lock is acquired. A
        // writer may publish a new table generation while this opener waits;
        // opening first would retain the stale pre-publication descriptor.
        // Ordinary open remains lazy, so retained readers do not materialize
        // row payloads here.
        let mut table = Self::open(options.clone())?;

        // Read sync data if available.
        let sync_data = lock_file
            .read_sync_data()
            .map_err(|e| TableError::LockIo {
                path: options.path.display().to_string(),
                message: e.to_string(),
            })?
            .unwrap_or_else(SyncData::new);

        table.lock_state = Some(LockState {
            path: options.path.clone(),
            lock_file,
            sync_data,
            options: lock_opts,
            data_manager: options.data_manager,
            endian_format: options.endian_format,
            flushed_generation: table.inner.generation(),
            unpublished_write: std::sync::atomic::AtomicBool::new(false),
        });

        Ok(table)
    }

    /// Acquires a lock on the table.
    ///
    /// Re-reads the table data from disk if another process modified it
    /// since the last lock was held. A table that already holds a lock is not
    /// re-read, and a read lock requested while the write lock is held keeps
    /// the write lock, as in casacore.
    ///
    /// `nattempts`: number of lock attempts. 0 means wait indefinitely for
    /// another process, 1 means try once without waiting, and more retries
    /// once a second. A wait adds this process to the request list in
    /// `table.lock`, as casacore's does, and is logged. A write lock held by
    /// another handle in this process is never waited for.
    ///
    /// Returns `true` if the lock was acquired, `false` if it could not
    /// be acquired within the given attempts or another handle in this
    /// process holds the write lock.
    ///
    /// C++ equivalent: `Table::lock(type, nattempts)`.
    #[cfg(unix)]
    pub fn lock(&mut self, lock_type: LockType, nattempts: u32) -> Result<bool, TableError> {
        // Memory tables always succeed — no file-based locking needed.
        // C++ equivalent: MemoryTable::lock() returns True.
        if self.kind == TableKind::Memory {
            return Ok(true);
        }

        // Notify external sync hook before acquiring.
        if let Some(sync) = &self.external_sync {
            match lock_type {
                LockType::Read => sync.acquire_read(),
                LockType::Write => sync.acquire_write(),
            }
        }

        let state = self
            .lock_state
            .as_mut()
            .ok_or_else(|| TableError::NotLocked {
                operation: "lock".into(),
            })?;

        // NoRead modes skip the file-level read lock entirely.
        if lock_type == LockType::Read && state.options.mode.skip_read_lock() {
            return Ok(true);
        }

        // A table already locked needs no synchronization: no other process
        // can have written it (casacore's `PlainTable::lock`). Requesting a
        // read lock while holding the write lock keeps the write lock and
        // this handle's unflushed changes.
        let already_locked = state.lock_file.has_lock(LockType::Read);
        let acquired = state
            .lock_file
            .acquire(lock_type, nattempts)
            .map_err(|e| TableError::LockIo {
                path: state.path.display().to_string(),
                message: e.to_string(),
            })?
            .is_acquired();

        if acquired && !already_locked {
            // Read sync data and check if we need to reload.
            if let Some(new_sync) =
                state
                    .lock_file
                    .read_sync_data()
                    .map_err(|e| TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    })?
                && state.sync_data.needs_reload(&new_sync)
            {
                // Another process modified the table — reload.
                let storage = CompositeStorage;
                let snapshot = storage
                    .load_with_row_hint(&state.path, Some(new_sync.nrrow))
                    .map_err(|e| TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    })?;
                self.virtual_columns = snapshot.virtual_columns;
                self.inner.replace_from_snapshot(
                    snapshot.rows,
                    snapshot.undefined_cells,
                    snapshot.keywords,
                    snapshot.column_keywords,
                    snapshot.schema,
                );
                // Update our stored sync data. The reloaded table is what is
                // on disk, so there is nothing to flush.
                if let Some(s) = self.lock_state.as_mut() {
                    s.sync_data = new_sync;
                    s.flushed_generation = self.inner.generation();
                }
            }
        }

        Ok(acquired)
    }

    /// Releases the current lock.
    ///
    /// If a write lock was held and the table changed since it was opened,
    /// reloaded or last flushed, the table is flushed to disk first. Every
    /// write this handle made to the disk under the lock, by that flush or
    /// earlier (a [`flush`](Table::flush), a save or a write plan's in-place
    /// save), is then published in the lock file's sync data, raising its
    /// modify counter so other processes re-read the table. A write is
    /// published even when a [`resync`](Table::resync) has since reloaded
    /// the table, and even when it failed after writing part of its change.
    /// A write lock under which nothing reached the disk writes and
    /// publishes nothing, as casacore's `PlainTable::putFile` writes and
    /// announces only what changed.
    ///
    /// casacore announces each flush when it writes it; casa-rs announces the
    /// lock period's writes once, when the lock is released. Other processes
    /// read the announcement only when they take a lock, which they cannot do
    /// before the write lock is released, so they see the same change.
    ///
    /// If the flush or the publication fails, the error is returned and the
    /// lock is kept, with the write still to be published by a later
    /// `unlock` or when the table is dropped.
    ///
    /// In permanent locking modes, this is a no-op (lock is held until close).
    ///
    /// C++ equivalent: `Table::unlock()`.
    #[cfg(unix)]
    pub fn unlock(&mut self) -> Result<(), TableError> {
        self.unlock_with_metadata_only_flush(false)
    }

    /// Releases the current lock after flushing only table metadata.
    ///
    /// This preserves the existing on-disk data-manager layout and must only
    /// be used when the locked mutation changed table or column metadata, not
    /// row values.
    #[cfg(unix)]
    pub fn unlock_metadata_only(&mut self) -> Result<(), TableError> {
        self.unlock_with_metadata_only_flush(true)
    }

    #[cfg(unix)]
    fn unlock_with_metadata_only_flush(&mut self, metadata_only: bool) -> Result<(), TableError> {
        // Memory tables have no lock to release.
        // C++ equivalent: MemoryTable::unlock() is a no-op.
        if self.kind == TableKind::Memory {
            return Ok(());
        }
        // Extract the info we need before borrowing self for save/schema.
        let (flush, save_opts, mode) = {
            let state = self
                .lock_state
                .as_ref()
                .ok_or_else(|| TableError::NotLocked {
                    operation: "unlock".into(),
                })?;
            let flush = state.lock_file.has_lock(LockType::Write)
                && state.has_unflushed_changes(&self.inner);
            let opts = TableOptions::new(&state.path)
                .with_data_manager(state.data_manager)
                .with_endian_format(state.endian_format);
            (flush, opts, state.options.mode)
        };

        if matches!(
            mode,
            LockMode::PermanentLocking | LockMode::PermanentLockingWait
        ) {
            return Ok(());
        }

        // If write-locked with changes, flush them to disk. The save records
        // its write before it starts, so a save that fails part way is still
        // published, by a later unlock or the drop.
        if flush {
            if metadata_only {
                self.save_metadata_only(save_opts)?;
            } else {
                self.save(save_opts)?;
            }
            let generation = self.inner.generation();
            let state = self.lock_state.as_mut().expect("lock_state present");
            state.flushed_generation = generation;
        }

        // Publish every write this lock period made to the disk, describing
        // the table as persisted, as casacore's putFile does.
        let state = self.lock_state.as_mut().expect("lock_state present");
        if state.lock_file.has_lock(LockType::Write) && state.has_unpublished_write() {
            state.sync_data =
                publish_persisted_write(&state.lock_file, &state.path).map_err(|e| {
                    TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    }
                })?;
            state
                .unpublished_write
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }

        state.lock_file.release().map_err(|e| TableError::LockIo {
            path: state.path.display().to_string(),
            message: e.to_string(),
        })?;

        // Notify external sync hook after release.
        if let Some(sync) = &self.external_sync {
            sync.release();
        }

        Ok(())
    }

    /// Returns `true` if the given lock type is currently held.
    ///
    /// Returns `false` if the table was not opened with locking.
    ///
    /// C++ equivalent: `Table::hasLock(type)`.
    #[cfg(unix)]
    pub fn has_lock(&self, lock_type: LockType) -> bool {
        // Memory tables always report holding the lock.
        // C++ equivalent: MemoryTable::hasLock() returns True.
        if self.kind == TableKind::Memory {
            return true;
        }
        self.lock_state
            .as_ref()
            .map(|s| s.lock_file.has_lock(lock_type))
            .unwrap_or(false)
    }

    /// Return the casacore-compatible modification counter observed by this lock.
    ///
    /// The value is meaningful only while this table retains a read or write
    /// lock. It is the durable counter used by table locking to decide whether
    /// an already-open table must be reloaded after another writer commits.
    #[cfg(unix)]
    pub fn locked_modify_counter(&self) -> Result<u32, TableError> {
        let state = self
            .lock_state
            .as_ref()
            .ok_or_else(|| TableError::NotLocked {
                operation: "locked_modify_counter".into(),
            })?;
        if !state.lock_file.has_lock(LockType::Read) {
            return Err(TableError::NotLocked {
                operation: "locked_modify_counter".into(),
            });
        }
        Ok(state.sync_data.modify_counter)
    }

    /// Tests if the table is opened by another process.
    ///
    /// Checks the in-use indicator in the lock file. Returns `false` if the
    /// table was not opened with locking.
    ///
    /// C++ equivalent: `Table::isMultiUsed()`.
    #[cfg(unix)]
    pub fn is_multi_used(&self) -> bool {
        // Memory tables are never shared with another process.
        // C++ equivalent: MemoryTable::isMultiUsed() returns False.
        if self.kind == TableKind::Memory {
            return false;
        }
        self.lock_state
            .as_ref()
            .map(|s| s.lock_file.is_multi_used())
            .unwrap_or(false)
    }

    /// Returns the lock options, if locking is active.
    #[cfg(unix)]
    pub fn lock_options(&self) -> Option<&LockOptions> {
        self.lock_state.as_ref().map(|s| &s.options)
    }

    /// Record that this handle is about to write the table directory at
    /// `path`. Called by every persistence path before its first write.
    ///
    /// A write to the handle's own table under the write lock it holds is
    /// published when the lock is released (see [`unlock`](Table::unlock)),
    /// however the handle's state changes in between: a resync or reload
    /// does not withdraw it, and a write that fails part way is still
    /// published. A write to another directory, such as a copy, or made
    /// without the write lock, is not recorded.
    #[cfg(unix)]
    pub(super) fn note_persisted_write(&self, path: &Path) {
        let Some(state) = self.lock_state.as_ref() else {
            return;
        };
        if state.lock_file.has_lock(LockType::Write) && same_directory(&state.path, path) {
            state
                .unpublished_write
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[cfg(not(unix))]
    pub(super) fn note_persisted_write(&self, _path: &Path) {}

    #[cfg(unix)]
    pub(super) fn begin_write_operation(&mut self, operation: &str) -> Result<bool, TableError> {
        if self.kind == TableKind::Memory {
            return Ok(false);
        }
        // Some write operations change only the data managers or the files
        // on disk, which the change count of the rows does not see. An
        // admitted operation counts even if it later fails, since it may have
        // written part of its change; a refused one does not.
        let admitted = self.admit_write_operation(operation);
        if admitted.is_ok() {
            self.inner.note_change();
        }
        admitted
    }

    #[cfg(unix)]
    fn admit_write_operation(&mut self, operation: &str) -> Result<bool, TableError> {
        let Some(state) = self.lock_state.as_mut() else {
            return Ok(false);
        };

        match state.options.mode {
            LockMode::NoLocking => Ok(false),
            LockMode::UserLocking | LockMode::UserNoReadLocking => {
                if state.lock_file.has_lock(LockType::Write) {
                    Ok(false)
                } else {
                    Err(TableError::LockFailed {
                        path: state.path.display().to_string(),
                        message: format!(
                            "{operation} requires a write lock when using UserLocking"
                        ),
                    })
                }
            }
            LockMode::PermanentLocking | LockMode::PermanentLockingWait => {
                if state.lock_file.has_lock(LockType::Write) {
                    Ok(false)
                } else {
                    Err(TableError::LockFailed {
                        path: state.path.display().to_string(),
                        message: format!(
                            "{operation} requires the permanent write lock to be held"
                        ),
                    })
                }
            }
            LockMode::AutoLocking | LockMode::AutoNoReadLocking | LockMode::DefaultLocking => {
                if state.lock_file.has_lock(LockType::Write) {
                    return Ok(false);
                }

                let outcome = state.lock_file.acquire(LockType::Write, 0).map_err(|e| {
                    TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    }
                })?;
                if outcome.is_acquired() {
                    Ok(true)
                } else {
                    Err(TableError::LockFailed {
                        path: state.path.display().to_string(),
                        message: format!(
                            "{operation} needs a temporary write lock: {}",
                            outcome.refusal(LockType::Write, 0)
                        ),
                    })
                }
            }
        }
    }

    #[cfg(not(unix))]
    pub(super) fn begin_write_operation(&mut self, _operation: &str) -> Result<bool, TableError> {
        Ok(false)
    }

    #[cfg(unix)]
    pub(super) fn finish_write_operation<R>(
        &mut self,
        auto_unlock: bool,
        result: Result<R, TableError>,
    ) -> Result<R, TableError> {
        if !auto_unlock {
            return result;
        }

        let unlock_result = self.unlock();
        match (result, unlock_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(unlock_err)) => Err(unlock_err),
            (Err(op_err), Ok(())) => Err(op_err),
            (Err(op_err), Err(_unlock_err)) => Err(op_err),
        }
    }

    #[cfg(not(unix))]
    pub(super) fn finish_write_operation<R>(
        &mut self,
        _auto_unlock: bool,
        result: Result<R, TableError>,
    ) -> Result<R, TableError> {
        result
    }
}

/// Whether `a` and `b` name the same directory: the same path, or two
/// spellings of one existing directory.
#[cfg(unix)]
fn same_directory(a: &Path, b: &Path) -> bool {
    a == b
        || matches!(
            (std::fs::canonicalize(a), std::fs::canonicalize(b)),
            (Ok(a), Ok(b)) if a == b
        )
}
