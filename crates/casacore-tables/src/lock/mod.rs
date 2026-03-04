// SPDX-License-Identifier: LGPL-3.0-or-later
//! File-based table locking for multi-process access.
//!
//! This module implements the casacore table locking protocol, providing
//! binary-compatible lock file I/O and `fcntl`-based advisory locking that
//! interoperates with C++ casacore.
//!
//! # C++ reference
//!
//! `TableLock`, `TableLockData`, `LockFile`, `FileLocker`, `TableSyncData`.

#[cfg(unix)]
mod lock_file;
mod sync_data;

#[cfg(unix)]
pub(crate) use lock_file::LockFile;
pub(crate) use sync_data::SyncData;

/// Lock mode options for a table.
///
/// Determines when and how locks are acquired on the underlying
/// `table.lock` file. Corresponds to C++ `TableLock::LockOption`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LockMode {
    /// Lock acquired on open and held until close.
    /// Fails immediately if the lock is unavailable.
    ///
    /// C++ equivalent: `TableLock::PermanentLocking`.
    PermanentLocking,

    /// Like [`PermanentLocking`](Self::PermanentLocking) but waits
    /// indefinitely for the lock.
    ///
    /// C++ equivalent: `TableLock::PermanentLockingWait`.
    PermanentLockingWait,

    /// The system manages lock acquisition for read/write operations.
    ///
    /// The table is opened with a read lock; mutating operations acquire a
    /// temporary write lock as needed and release it on completion.
    ///
    /// C++ equivalent: `TableLock::AutoLocking`.
    AutoLocking,

    /// The application calls [`Table::lock()`](crate::Table::lock) and
    /// [`Table::unlock()`](crate::Table::unlock) explicitly.
    ///
    /// C++ equivalent: `TableLock::UserLocking`.
    UserLocking,

    /// Like [`AutoLocking`](Self::AutoLocking) but skips acquiring a read
    /// lock before read operations. Only write locks are acquired. This can
    /// improve performance when reads need not be serialized.
    ///
    /// C++ equivalent: `TableLock::AutoNoReadLocking`.
    AutoNoReadLocking,

    /// Like [`UserLocking`](Self::UserLocking) but skips acquiring a read
    /// lock when the user calls [`Table::lock(Read)`](crate::Table::lock).
    /// Only write locks are acquired.
    ///
    /// C++ equivalent: `TableLock::UserNoReadLocking`.
    UserNoReadLocking,

    /// Uses the default locking mode, which resolves to
    /// [`AutoLocking`](Self::AutoLocking).
    ///
    /// C++ equivalent: `TableLock::DefaultLocking`.
    DefaultLocking,

    /// No locking is performed. This is the default for backward
    /// compatibility with code that does not need multi-process access.
    ///
    /// C++ equivalent: `TableLock::NoLocking`.
    #[default]
    NoLocking,
}

impl LockMode {
    /// Resolves [`DefaultLocking`](Self::DefaultLocking) to
    /// [`AutoLocking`](Self::AutoLocking); all other modes are unchanged.
    pub fn resolve(self) -> Self {
        match self {
            Self::DefaultLocking => Self::AutoLocking,
            other => other,
        }
    }

    /// Returns `true` if this mode skips read locks.
    pub fn skip_read_lock(self) -> bool {
        matches!(self, Self::AutoNoReadLocking | Self::UserNoReadLocking)
    }
}

/// The type of lock being held or requested.
///
/// Read locks are shared (multiple readers), write locks are exclusive.
/// Corresponds to C++ `FileLocker::LockType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Shared read lock — multiple processes may hold this simultaneously.
    Read,
    /// Exclusive write lock — only one process may hold this at a time.
    Write,
}

/// Options controlling table lock behavior.
///
/// Used with [`Table::open_with_lock()`](crate::Table::open_with_lock) to
/// configure locking parameters.
#[derive(Debug, Clone)]
pub struct LockOptions {
    /// The locking mode. See [`LockMode`] for details.
    pub mode: LockMode,
    /// Inspection interval in seconds for [`LockMode::AutoLocking`].
    /// Default: 5.0 seconds.
    pub inspection_interval: f64,
}

impl Default for LockOptions {
    fn default() -> Self {
        Self {
            mode: LockMode::NoLocking,
            inspection_interval: 5.0,
        }
    }
}

impl LockOptions {
    /// Create lock options with the given mode and default inspection interval.
    pub fn new(mode: LockMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    /// Set the inspection interval for auto-locking mode.
    pub fn with_inspection_interval(mut self, seconds: f64) -> Self {
        self.inspection_interval = seconds;
        self
    }
}

/// Hook for external lock synchronization.
///
/// Allows an application-level lock manager (e.g. a distributed lock service
/// or a custom IPC mechanism) to participate in the table locking protocol.
/// When set on a [`Table`](crate::Table), the hooks are called around
/// every lock acquire/release pair so that external state stays in sync.
///
/// # C++ equivalent
///
/// `ExternalLockSync` (abstract base class in `TableLockData`).
pub trait ExternalLockSync: Send + Sync {
    /// Called before acquiring a read lock.
    fn acquire_read(&self);
    /// Called before acquiring a write lock.
    fn acquire_write(&self);
    /// Called after releasing any lock.
    fn release(&self);
}
