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
        Self::open_with_lock_using(options, lock_opts, Self::open)
    }

    /// Opens a locked table with an explicit limit on its initial lock wait.
    ///
    /// Acquisition uses the attempt count of [`Table::lock`]. A publisher
    /// can attach a new table in automatic mode while holding a guard
    /// without waiting for another process to release its lock.
    #[cfg(unix)]
    pub fn open_with_lock_attempts(
        options: TableOptions,
        lock_opts: LockOptions,
        attempts: u32,
    ) -> Result<Self, TableError> {
        Self::open_with_lock_loading(options, lock_opts, Self::open, Some(attempts))
    }

    #[cfg(unix)]
    pub(super) fn open_with_lock_using(
        options: TableOptions,
        lock_opts: LockOptions,
        load: impl FnOnce(TableOptions) -> Result<Self, TableError>,
    ) -> Result<Self, TableError> {
        Self::open_with_lock_loading(options, lock_opts, load, None)
    }

    #[cfg(unix)]
    fn open_with_lock_loading(
        options: TableOptions,
        lock_opts: LockOptions,
        load: impl FnOnce(TableOptions) -> Result<Self, TableError>,
        initial_attempts: Option<u32>,
    ) -> Result<Self, TableError> {
        if lock_opts.mode == LockMode::NoLocking {
            return load(options);
        }

        let perm = matches!(
            lock_opts.mode,
            LockMode::PermanentLocking | LockMode::PermanentLockingWait
        );
        let lock_path = super::reference_writes::reference_lock_path(&options.path)?;
        let mut lock_file =
            LockFile::create_or_open(&lock_path, false, lock_opts.inspection_interval, perm)
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
            let nattempts = initial_attempts.unwrap_or(nattempts);
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

        if super::reference_writes::reference_lock_path(&options.path)? != lock_path {
            return Err(TableError::Storage(
                "the saved reference changed while acquiring its lock; reopen it".into(),
            ));
        }

        // Metadata must be read only after the initial lock is acquired. A
        // writer may publish a new table generation while this opener waits;
        // opening first would retain the stale pre-publication descriptor.
        // Ordinary open remains lazy, so retained readers do not materialize
        // row payloads here.
        let mut table = load(options.clone())?;
        table.inner.track_dirty_changes();

        // Read sync data if available.
        let observed_modify_counter = lock_file
            .read_sync_data()
            .map_err(|e| TableError::LockIo {
                path: options.path.display().to_string(),
                message: e.to_string(),
            })?
            .map_or(0, |sync| sync.modify_counter);

        table.lock_state = Some(LockState {
            path: lock_path,
            lock_file,
            observed_modify_counter: std::sync::atomic::AtomicU32::new(observed_modify_counter),
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
    /// Re-reads the table data from disk if another handle published a
    /// change since the last synchronization, when acquiring a lock or
    /// upgrading a read lock to a write lock. A repeated read lock does not
    /// refresh; a read lock requested while the write lock is held keeps
    /// that lock and its unflushed changes, as in casacore.
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
        if self.kind == TableKind::Memory && !self.is_named_memory_manager() {
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
        // Other handles in this process can publish while this one holds a
        // read lock. Refresh on an upgrade before it writes stale metadata.
        // A handle that already holds the write lock keeps its unflushed
        // changes, and a repeated read lock does not synchronize.
        let already_locked = state.lock_file.has_lock(LockType::Read);
        let already_write_locked = state.lock_file.has_lock(LockType::Write);
        let acquired = state
            .lock_file
            .acquire(lock_type, nattempts)
            .map_err(|e| TableError::LockIo {
                path: state.path.display().to_string(),
                message: e.to_string(),
            })?
            .is_acquired();

        if acquired && (!already_locked || (lock_type == LockType::Write && !already_write_locked))
        {
            self.reload_if_published()?;
        }

        Ok(acquired)
    }

    /// Reloads the table when the lock file's sync data says a change was
    /// published since this handle last synchronized, and returns whether it
    /// did. The caller holds a lock, so no writer is publishing.
    #[cfg(unix)]
    pub(super) fn reload_if_published(&mut self) -> Result<bool, TableError> {
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
        if !new_sync.needs_reload(state.observed_modify_counter()) {
            return Ok(false);
        }
        // Reopen the descriptor just as a fresh reader would. This drops
        // any stale cells and keeps unread cells on disk.
        let path = self
            .source_path
            .clone()
            .unwrap_or_else(|| state.path.clone());
        self.reload_disk_state_at(&path)
            .map_err(|error| TableError::LockIo {
                path: path.display().to_string(),
                message: error.to_string(),
            })?;
        self.lock_state
            .as_ref()
            .expect("open lock state")
            .observe_modify_counter(new_sync.modify_counter);
        Ok(true)
    }

    /// Brings a table that holds a read lock up to date with what other
    /// handles of the table (in this process or another) have published
    /// since: the lock file's sync data is read again and the table is
    /// reloaded if its modify counter moved. The lock stays held. Returns
    /// whether the table was reloaded.
    ///
    /// [`lock`](Table::lock) synchronizes on acquisition and on a read-to-
    /// write upgrade; a handle that keeps its read lock between calls uses
    /// this to see a change another handle of the same process published
    /// (casacore shares one table between them). A handle holding the write
    /// lock is the table's only writer and is left as it is.
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
        if self.kind == TableKind::Memory && !self.is_named_memory_manager() {
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
        let sync = publish_persisted_write(
            &state.lock_file,
            &state.path,
            state.table_dat_before_write.get().map(Vec::as_slice),
        )
        .map_err(|e| TableError::LockIo {
            path: state.path.display().to_string(),
            message: e.to_string(),
        })?;
        state.observe_modify_counter(sync.modify_counter);
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
    /// whatever reached the disk. Returns the first release's error.
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
    /// `save_selected_rows`) calls this after a failure, to suppress any
    /// further attempts to write unwritten changes on close.
    #[cfg(unix)]
    pub fn resync_or_discard(&mut self) -> Result<(), TableError> {
        let reloaded = self.resync();
        if reloaded.is_err() {
            self.mark_flushed();
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

    /// Whether this locked table holds changes which have yet to reach disk.
    ///
    /// Tools backed by a separate data manager use this before flushing the
    /// table metadata and their own payload at a lock release.
    #[cfg(unix)]
    pub fn holds_unflushed_changes(&self) -> bool {
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
        self.inner.clear_dirty_changes();
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
        if self.kind == TableKind::Memory && !self.is_named_memory_manager() {
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
            let source = self.source_path.as_ref().unwrap_or(&state.path);
            let opts = TableOptions::new(source)
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
            if self.is_named_memory_manager() {
                self.finish_memory_manager_write(true)?;
            } else if metadata_only {
                self.save_metadata_only(save_opts)?;
            } else {
                self.flush_locked_changes(true)?;
            }
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

    /// Flush the outstanding scope while preserving the persisted layout.
    #[cfg(unix)]
    pub(super) fn flush_locked_changes(&mut self, allow_rewrite: bool) -> Result<(), TableError> {
        let Some(state) = &self.lock_state else {
            return Ok(());
        };
        let path = self
            .source_path
            .clone()
            .unwrap_or_else(|| state.path.clone());
        let root = super::reference_writes::reference_lock_path(&path)?;
        if !same_directory(&root, &state.path) {
            return Err(TableError::Storage(
                "the table's lock path changed; reopen it".into(),
            ));
        }
        state.admit_full_save(&root)?;
        self.flush_changes(&path, allow_rewrite, None)
    }

    pub(super) fn flush_source(&mut self, path: &Path) -> Result<(), TableError> {
        let root = super::reference_writes::reference_lock_path(path)?;
        #[cfg(unix)]
        let mut attempts = 1;
        #[cfg(not(unix))]
        let attempts = 1;
        #[cfg(unix)]
        if let Some(state) = &self.lock_state {
            if !same_directory(&root, &state.path) {
                return Err(TableError::Storage(
                    "the table's lock path changed; reopen it".into(),
                ));
            }
            state.admit_full_save(&root)?;
            if state.lock_file.has_lock(LockType::Write) {
                return self.flush_changes(path, true, None);
            }
            attempts = 0;
        }
        match self.inner.has_dirty_changes() {
            Some(false) => return Ok(()),
            None if !self.is_named_memory_manager() => {
                return Err(TableError::Storage(
                    "flush requires a table opened with its rows".into(),
                ));
            }
            _ => {}
        }

        let mut guard = crate::TableWriteLock::acquire(&root, attempts)?;
        #[cfg(unix)]
        if let Some(state) = &self.lock_state
            && guard.modify_counter()?.unwrap_or(0) != state.observed_modify_counter()
        {
            return Err(TableError::LockFailed {
                path: path.display().to_string(),
                message: "table changed since this handle last synchronized; reacquire its write lock and retry".into(),
            });
        }
        let result = self.flush_changes(path, true, Some(&mut guard));
        let sync = guard.sync_recorded_write();
        #[cfg(unix)]
        if sync.is_ok()
            && let Some(state) = &self.lock_state
            && let Ok(counter) = guard.modify_counter()
        {
            state.observe_modify_counter(counter.unwrap_or(0));
        }
        result.and(sync).and(guard.release())
    }

    fn flush_changes(
        &mut self,
        path: &Path,
        allow_rewrite: bool,
        mut external: Option<&mut crate::TableWriteLock>,
    ) -> Result<(), TableError> {
        let dirty = self.inner.dirty_changes();
        if self.is_named_memory_manager() {
            let metadata = dirty.as_ref().is_none_or(|dirty| {
                dirty.metadata || dirty.whole || !dirty.added_columns.is_empty()
            });
            self.finish_memory_manager_write(metadata)?;
            if metadata && let Some(lock) = external {
                lock.record_write();
            }
            return Ok(());
        }
        let Some(mut dirty) = dirty else {
            return self.flush_full_layout(allow_rewrite, external.as_deref());
        };
        if dirty.whole || !dirty.added_columns.is_empty() {
            return self.flush_full_layout(allow_rewrite, external.as_deref());
        }
        for column in dirty.cells.keys() {
            self.require_writable_column(column)?;
        }
        if dirty.metadata || !dirty.cells.is_empty() || dirty.appended_from.is_some() {
            let root = super::reference_writes::reference_lock_path(path)?;
            self.check_flush_targets(path, &root, &dirty)?;
        }
        if dirty.metadata && !self.virtual_columns.is_empty() {
            self.check_flush_virtual_keywords(path, external.as_deref())?;
        }
        if let Some(first) = dirty.appended_from {
            if dirty
                .cells
                .values()
                .any(|rows| rows.first().is_some_and(|&row| row < first))
            {
                return self.flush_full_layout(allow_rewrite, external.as_deref());
            }
            self.persist_buffered_append(path, first)?;
            if let Some(lock) = external.as_deref_mut() {
                lock.record_write();
            }
            dirty.cells.clear();
        }
        if (dirty.metadata || !dirty.cells.is_empty())
            && let Some(lock) = external.as_deref_mut()
        {
            lock.record_write();
        }
        let mut groups = std::collections::BTreeMap::<Vec<usize>, Vec<String>>::new();
        for (column, rows) in dirty.cells {
            groups
                .entry(rows.into_iter().collect())
                .or_default()
                .push(column);
        }
        for (rows, mut columns) in groups {
            columns.sort_unstable();
            let columns: Vec<_> = columns.iter().map(String::as_str).collect();
            if let Err(error) = self.persist_selected_rows(&columns, &rows, allow_rewrite) {
                if allow_rewrite
                    && matches!(&error, TableError::Storage(message)
                    if message.starts_with("partial in-place save does not support"))
                    && let Some(options) = self.single_rewrite_options()?
                {
                    return self.save_with_write_lock(options, external.as_deref());
                }
                return Err(error);
            }
        }
        if dirty.metadata {
            self.save_metadata_only(TableOptions::new(path))?;
        }
        Ok(())
    }

    fn check_flush_virtual_keywords(
        &self,
        path: &Path,
        external: Option<&crate::TableWriteLock>,
    ) -> Result<(), TableError> {
        let peer_open = external.is_some_and(crate::TableWriteLock::is_multi_used);
        #[cfg(unix)]
        let peer_open = peer_open || self.is_multi_used();
        if !peer_open {
            return Ok(());
        }
        let persisted = Self::open(TableOptions::new(path))?;
        if self.keywords() != persisted.keywords()
            || self
                .virtual_columns
                .iter()
                .any(|name| self.column_keywords(name) != persisted.column_keywords(name))
        {
            return Err(TableError::Storage(
                "flush would change virtual manager keywords of an open table; close its other handles and retry".into(),
            ));
        }
        Ok(())
    }

    fn check_flush_targets(
        &self,
        path: &Path,
        root: &Path,
        dirty: &crate::table_impl::DirtyChanges,
    ) -> Result<(), TableError> {
        use crate::storage::{StorageError, TABLE_CONTROL_FILE, TABLE_INFO_FILE};
        let access =
            |path: &Path| crate::publication::check_write_access(path).map_err(StorageError::from);
        access(&root.join(TABLE_CONTROL_FILE))?;
        if dirty.metadata && path.join(TABLE_INFO_FILE).exists() {
            access(&path.join(TABLE_INFO_FILE))?;
        }
        if !dirty.cells.is_empty() {
            let affected: HashSet<_> = self
                .dm_info
                .iter()
                .filter(|manager| {
                    manager
                        .columns
                        .iter()
                        .any(|column| dirty.cells.contains_key(column))
                })
                .map(|manager| manager.seq_nr)
                .collect();
            crate::storage::full_save::check_manager_write_access(root, &affected)?;
        }
        Ok(())
    }

    fn flush_full_layout(
        &self,
        allow_rewrite: bool,
        external: Option<&crate::TableWriteLock>,
    ) -> Result<(), TableError> {
        if allow_rewrite && let Some(options) = self.single_rewrite_options()? {
            return self.save_with_write_lock(options, external);
        }
        Err(TableError::Storage(
            "unscoped table changes require an explicit write that preserves its layout".into(),
        ))
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
        if self.kind == TableKind::Memory && !self.is_named_memory_manager() {
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
        Ok(state.observed_modify_counter())
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
        if self.kind == TableKind::Memory && !self.is_named_memory_manager() {
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

    /// Checks whether another process is waiting for this automatic lock.
    ///
    /// Casacore's automatic locking checks the lock request list at the
    /// configured interval. Callers with separately buffered payloads check
    /// at the end of an operation so they can flush before releasing.
    #[cfg(unix)]
    pub fn auto_lock_release_requested(&mut self) -> Result<bool, TableError> {
        let Some(state) = self.lock_state.as_mut() else {
            return Ok(false);
        };
        if !matches!(
            state.options.mode.resolve(),
            LockMode::AutoLocking | LockMode::AutoNoReadLocking
        ) || !state.lock_file.has_lock(LockType::Read)
        {
            return Ok(false);
        }
        state
            .lock_file
            .inspect(false)
            .map_err(|error| TableError::LockIo {
                path: state.path.display().to_string(),
                message: error.to_string(),
            })
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
        if self.kind == TableKind::Memory && !self.is_named_memory_manager() {
            return Ok(false);
        }
        // In-memory mutators record their own scope. Disk edits record that
        // they need publication before the first write, even on failure.
        self.admit_write_operation(operation)
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
