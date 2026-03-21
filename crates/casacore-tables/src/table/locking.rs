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
    ///   indefinitely.
    /// - [`LockMode::AutoLocking`]: acquires a read lock immediately; write
    ///   operations temporarily acquire/release a write lock.
    /// - [`LockMode::UserLocking`]: no lock is acquired until
    ///   [`lock()`](Table::lock) is called.
    /// - [`LockMode::NoLocking`]: equivalent to [`open()`](Table::open).
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

        let storage = CompositeStorage;
        let row_hint = crate::lock::read_sync_data_from_table_dir(&options.path)
            .map_err(|e| TableError::LockIo {
                path: options.path.display().to_string(),
                message: e.to_string(),
            })?
            .map(|sync| sync.nrrow);
        let snapshot = storage.load_with_row_hint(&options.path, row_hint)?;
        let info = snapshot.table_info;
        let mut table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                snapshot.rows,
                snapshot.undefined_cells,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            virtual_columns: snapshot.virtual_columns,
            virtual_bindings: Vec::new(),
            table_info: info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            marked_for_delete: false,
            lock_state: None,
        };
        table.validate()?;

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

        // Acquire initial lock based on mode.
        match lock_opts.mode {
            LockMode::PermanentLocking => {
                if !lock_file
                    .acquire(LockType::Write, 1)
                    .map_err(|e| TableError::LockIo {
                        path: options.path.display().to_string(),
                        message: e.to_string(),
                    })?
                {
                    return Err(TableError::LockFailed {
                        path: options.path.display().to_string(),
                        message: "table is locked by another process".into(),
                    });
                }
            }
            LockMode::PermanentLockingWait => {
                if !lock_file
                    .acquire(LockType::Write, 0)
                    .map_err(|e| TableError::LockIo {
                        path: options.path.display().to_string(),
                        message: e.to_string(),
                    })?
                {
                    return Err(TableError::LockFailed {
                        path: options.path.display().to_string(),
                        message: "could not acquire permanent lock".into(),
                    });
                }
            }
            LockMode::AutoLocking | LockMode::DefaultLocking => {
                let _ = lock_file.acquire(LockType::Read, 1);
            }
            LockMode::AutoNoReadLocking => {
                // Skip read lock on open — only write locks are acquired.
            }
            LockMode::UserLocking | LockMode::UserNoReadLocking | LockMode::NoLocking => {}
        }

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
        });

        Ok(table)
    }

    /// Acquires a lock on the table.
    ///
    /// Re-reads the table data from disk if another process modified it
    /// since the last lock was held.
    ///
    /// `nattempts`: number of lock attempts. 0 means wait indefinitely,
    /// 1 means try once without waiting.
    ///
    /// Returns `true` if the lock was acquired, `false` if it could not
    /// be acquired within the given attempts.
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

        let acquired =
            state
                .lock_file
                .acquire(lock_type, nattempts)
                .map_err(|e| TableError::LockIo {
                    path: state.path.display().to_string(),
                    message: e.to_string(),
                })?;

        if acquired {
            // Read sync data and check if we need to reload.
            if let Some(new_sync) =
                state
                    .lock_file
                    .read_sync_data()
                    .map_err(|e| TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    })?
            {
                if state.sync_data.needs_reload(&new_sync) {
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
                    // Update our stored sync data.
                    if let Some(s) = self.lock_state.as_mut() {
                        s.sync_data = new_sync;
                    }
                }
            }
        }

        Ok(acquired)
    }

    /// Releases the current lock.
    ///
    /// If a write lock was held, the table is flushed to disk first and
    /// sync data is updated in the lock file.
    ///
    /// In permanent locking modes, this is a no-op (lock is held until close).
    ///
    /// C++ equivalent: `Table::unlock()`.
    #[cfg(unix)]
    pub fn unlock(&mut self) -> Result<(), TableError> {
        // Memory tables have no lock to release.
        // C++ equivalent: MemoryTable::unlock() is a no-op.
        if self.kind == TableKind::Memory {
            return Ok(());
        }
        // Extract the info we need before borrowing self for save/schema.
        let (is_write_locked, save_opts, mode) = {
            let state = self
                .lock_state
                .as_ref()
                .ok_or_else(|| TableError::NotLocked {
                    operation: "unlock".into(),
                })?;
            let wl = state.lock_file.has_lock(LockType::Write);
            let opts = TableOptions::new(&state.path)
                .with_data_manager(state.data_manager)
                .with_endian_format(state.endian_format);
            (wl, opts, state.options.mode)
        };

        if matches!(
            mode,
            LockMode::PermanentLocking | LockMode::PermanentLockingWait
        ) {
            return Ok(());
        }

        // If write-locked, flush data to disk.
        if is_write_locked {
            self.save(save_opts)?;

            // Gather sync info from immutable borrows.
            let nrrow = self.row_count() as u64;
            let nrcolumn = self.schema().map(|s| s.columns().len() as u32).unwrap_or(0);

            // Now borrow lock_state mutably for sync data update.
            let state = self.lock_state.as_mut().expect("lock_state present");
            state.sync_data.record_write(nrrow, nrcolumn, true, &[true]);

            state
                .lock_file
                .write_sync_data(&state.sync_data)
                .map_err(|e| TableError::LockIo {
                    path: state.path.display().to_string(),
                    message: e.to_string(),
                })?;
        }

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

    #[cfg(unix)]
    pub(super) fn begin_write_operation(&mut self, operation: &str) -> Result<bool, TableError> {
        if self.kind == TableKind::Memory {
            return Ok(false);
        }

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

                let acquired = state.lock_file.acquire(LockType::Write, 0).map_err(|e| {
                    TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    }
                })?;
                if acquired {
                    Ok(true)
                } else {
                    Err(TableError::LockFailed {
                        path: state.path.display().to_string(),
                        message: format!("could not acquire temporary write lock for {operation}"),
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
