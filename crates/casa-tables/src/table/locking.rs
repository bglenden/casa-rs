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
        let permanent_lock = if lock_opts.read_only {
            LockType::Read
        } else {
            LockType::Write
        };
        let initial = match lock_opts.mode {
            LockMode::PermanentLocking => Some((permanent_lock, 1)),
            LockMode::PermanentLockingWait => Some((permanent_lock, 0)),
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
            flushed_generation: std::sync::atomic::AtomicU64::new(table.inner.generation()),
            unpublished_write: std::sync::atomic::AtomicBool::new(false),
            table_dat_before_write: std::sync::OnceLock::new(),
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

        // The NoRead modes only spare a table the read lock it takes on
        // open (`skip_read_lock`); an explicit request takes it, as
        // casacore's `PlainTable::lock` does.
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
            self.reload_if_published()?;
        }

        Ok(acquired)
    }

    /// Reloads the table when the lock file's sync data says a change was
    /// published since this handle last synchronized, and returns whether it
    /// did. The caller holds a lock, so no writer is publishing.
    #[cfg(unix)]
    fn reload_if_published(&mut self) -> Result<bool, TableError> {
        let state = self
            .lock_state
            .as_ref()
            .ok_or_else(|| TableError::NotLocked {
                operation: "resync".into(),
            })?;
        let lock_io = |message: String| TableError::LockIo {
            path: state.path.display().to_string(),
            message,
        };
        let Some(new_sync) = state
            .lock_file
            .read_sync_data()
            .map_err(|e| lock_io(e.to_string()))?
        else {
            return Ok(false);
        };
        if !state.sync_data.needs_reload(&new_sync) {
            return Ok(false);
        }
        // Another handle or process modified the table — reload.
        let storage = CompositeStorage;
        let snapshot = storage
            .load_with_row_hint(&state.path, Some(new_sync.nrrow))
            .map_err(|e| lock_io(e.to_string()))?;
        self.virtual_columns = snapshot.virtual_columns;
        // The storage managers and the table info are what another
        // process may have changed too (a renamed or removed column).
        self.dm_info = snapshot.dm_info;
        self.table_info = snapshot.table_info;
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
            s.flushed_generation.store(
                self.inner.generation(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        Ok(true)
    }

    /// Brings a table that holds a read lock up to date with what other
    /// handles of the table (in this process or another) have published
    /// since: the lock file's sync data is read again and the table is
    /// reloaded if its modify counter moved. The lock stays held. Returns
    /// whether the table was reloaded.
    ///
    /// [`lock`](Table::lock) synchronizes only when it takes a lock the
    /// handle did not hold, as casacore's `PlainTable::lock` does; a handle
    /// that keeps its lock between calls uses this to see a change another
    /// handle of the same process published (casacore shares one table
    /// between them). A handle holding the write lock is the table's only
    /// writer and is left as it is.
    ///
    /// # Errors
    ///
    /// [`TableError::NotLocked`] when no lock is held.
    ///
    /// C++ equivalent: `Table::resync()` (which reads the lock file without
    /// holding a lock).
    #[cfg(unix)]
    pub fn resync_locked(&mut self) -> Result<bool, TableError> {
        let held = self.lock_state.as_ref().map(|state| {
            (
                state.lock_file.has_lock(LockType::Read),
                state.lock_file.has_lock(LockType::Write),
            )
        });
        match held {
            Some((true, false)) => self.reload_if_published(),
            Some((_, true)) => Ok(false),
            _ => Err(TableError::NotLocked {
                operation: "resync_locked".into(),
            }),
        }
    }

    /// The modify counter the lock file publishes now, read while a lock is
    /// held. Unlike [`locked_modify_counter`](Table::locked_modify_counter),
    /// which is the counter this handle last synchronized with, it includes a
    /// change another handle has published since, and a write this handle has
    /// published with [`publish_writes`](Table::publish_writes). `0` when
    /// nothing has been published.
    ///
    /// # Errors
    ///
    /// [`TableError::NotLocked`] when no lock is held.
    #[cfg(unix)]
    pub fn published_modify_counter(&self) -> Result<u32, TableError> {
        let state = self
            .lock_state
            .as_ref()
            .filter(|state| state.lock_file.has_lock(LockType::Read))
            .ok_or_else(|| TableError::NotLocked {
                operation: "published_modify_counter".into(),
            })?;
        Ok(state
            .lock_file
            .read_sync_data()
            .map_err(|e| TableError::LockIo {
                path: state.path.display().to_string(),
                message: e.to_string(),
            })?
            .map_or(0, |sync| sync.modify_counter))
    }

    /// Publishes the writes this handle has made to the disk under the write
    /// lock it holds, as [`unlock`](Table::unlock) does, and keeps the lock:
    /// the lock file's modify counter rises, so another process (or handle)
    /// that takes a lock next re-reads the table, and the release of the lock
    /// does not announce the same writes again. Returns whether anything was
    /// published; a handle without the write lock, or without a write on the
    /// disk that is not yet published, publishes nothing.
    ///
    /// It does not write the table: a change held only in memory stays
    /// there. A writer that persists each change when it makes it (the
    /// `table` tool) calls this to make the changes it has persisted visible,
    /// as casacore's `Table::flush` does by writing and announcing at once.
    ///
    /// C++ equivalent: the announcement made by `Table::flush()`
    /// (`PlainTable::putFile`).
    #[cfg(unix)]
    pub fn publish_writes(&mut self) -> Result<bool, TableError> {
        if self.kind == TableKind::Memory {
            return Ok(false);
        }
        self.publish_pending_write()
    }

    /// Publishes the unpublished write the held write lock covers, if any.
    #[cfg(unix)]
    fn publish_pending_write(&mut self) -> Result<bool, TableError> {
        let Some(state) = self.lock_state.as_mut() else {
            return Ok(false);
        };
        if !(state.lock_file.has_lock(LockType::Write) && state.has_unpublished_write()) {
            return Ok(false);
        }
        state.sync_data = publish_persisted_write(
            &state.lock_file,
            &state.path,
            state.table_dat_before_write.get().map(Vec::as_slice),
        )
        .map_err(|e| TableError::LockIo {
            path: state.path.display().to_string(),
            message: e.to_string(),
        })?;
        state
            .unpublished_write
            .store(false, std::sync::atomic::Ordering::Relaxed);
        state.table_dat_before_write.take();
        Ok(true)
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
    /// lock period's writes once, when the lock is released, or earlier when
    /// [`publish_writes`](Table::publish_writes) is called (a write already
    /// published is not announced again). Other processes read the
    /// announcement only when they take a lock, which they cannot do before
    /// the write lock is released, so they see the same change.
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

    /// Releases the current lock as
    /// [`unlock_metadata_only`](Self::unlock_metadata_only) does and, when
    /// that fails holding the write lock, releases it all the same: the
    /// changes not yet written are discarded, the table reloaded as it is on
    /// disk where it can be, and the lock released again, publishing
    /// whatever reached the disk. A table dropped while it holds the write
    /// lock with unwritten changes is written in full with its lock's
    /// storage manager; a writer that keeps the table's managers releases
    /// through this instead. Returns the first release's error.
    ///
    /// A table held with permanent locking keeps its lock, as `unlock`
    /// keeps it, and with it its unwritten changes.
    #[cfg(unix)]
    pub fn unlock_metadata_only_or_discard(&mut self) -> Result<(), TableError> {
        let released = self.unlock_metadata_only();
        if released.is_err() && self.has_lock(LockType::Write) {
            let _ = self.resync_or_discard();
            let _ = self.unlock_metadata_only();
        }
        released
    }

    /// Reloads the table from disk, as [`resync`](Self::resync) does and,
    /// when the reload fails, discards the changes not yet written all the
    /// same: the table is left as it was in memory, but with nothing to
    /// flush, so that closing or dropping it while it holds the write lock
    /// only publishes what reached the disk and never writes the table in
    /// full. Returns the reload's error.
    ///
    /// A writer that persists its changes itself (`save_metadata_only`,
    /// `save_selected_rows`) calls this after a failure, to leave nothing
    /// for the full write of a drop to find.
    #[cfg(unix)]
    pub fn resync_or_discard(&mut self) -> Result<(), TableError> {
        let reloaded = self.resync();
        if reloaded.is_err() {
            let generation = self.inner.generation();
            if let Some(state) = self.lock_state.as_mut() {
                state
                    .flushed_generation
                    .store(generation, std::sync::atomic::Ordering::Relaxed);
            }
        }
        reloaded
    }

    /// Reloads the table from disk, as [`resync`](Self::resync) does.
    /// Without locks there is no write lock holding unflushed changes to
    /// discard.
    #[cfg(not(unix))]
    pub fn resync_or_discard(&mut self) -> Result<(), TableError> {
        self.resync()
    }

    /// Whether the handle holds changes the disk does not have, which the
    /// release of its write lock would write in full.
    #[cfg(unix)]
    pub(super) fn holds_unflushed_changes(&self) -> bool {
        self.lock_state
            .as_ref()
            .is_some_and(|state| state.has_unflushed_changes(&self.inner))
    }

    #[cfg(not(unix))]
    pub(super) fn holds_unflushed_changes(&self) -> bool {
        false
    }

    /// Records that the handle has nothing the disk lacks: a disk-only
    /// operation that began by counting as a change (every write operation
    /// does) was refused before it wrote anything.
    #[cfg(unix)]
    pub(super) fn mark_flushed(&self) {
        if let Some(state) = self.lock_state.as_ref() {
            state.flushed_generation.store(
                self.inner.generation(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }

    #[cfg(not(unix))]
    pub(super) fn mark_flushed(&self) {}

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
            state
                .flushed_generation
                .store(generation, std::sync::atomic::Ordering::Relaxed);
        }

        // Publish every write this lock period made to the disk (and not
        // yet published by `publish_writes`), describing the table as
        // persisted, as casacore's putFile does.
        self.publish_pending_write()?;

        let state = self.lock_state.as_mut().expect("lock_state present");
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
        if state.lock_file.has_lock(LockType::Write)
            && same_directory(&state.path, path)
            && !state
                .unpublished_write
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            && let Ok(table_dat) =
                std::fs::read(state.path.join(crate::storage::TABLE_CONTROL_FILE))
        {
            let _ = state.table_dat_before_write.set(table_dat);
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
pub(super) fn same_directory(a: &Path, b: &Path) -> bool {
    a == b
        || matches!(
            (std::fs::canonicalize(a), std::fs::canonicalize(b)),
            (Ok(a), Ok(b)) if a == b
        )
}
