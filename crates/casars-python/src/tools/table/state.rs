// SPDX-License-Identifier: LGPL-3.0-or-later
//! The table the tool has open, its lock settings, and what a call may fail
//! with.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_tables::{LockMode, LockOptions, LockType, Table, TableError, TableOptions};
use pyo3::PyErr;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyTypeError};

use crate::tools::LogLine;

/// What a call fails with: CASA raises `RuntimeError` for what its table
/// proxy refuses and `TypeError` for an argument of the wrong type.
#[derive(Debug)]
pub(crate) enum Fail {
    /// A `RuntimeError`.
    Runtime(String),
    /// A `TypeError`.
    Type(String),
    /// A `NotImplementedError`: casa-rs cannot do what CASA does here yet.
    Unsupported(String),
}

impl From<TableError> for Fail {
    fn from(error: TableError) -> Self {
        Self::Runtime(error.to_string())
    }
}

impl From<Fail> for PyErr {
    fn from(fail: Fail) -> Self {
        match fail {
            Fail::Runtime(message) => PyRuntimeError::new_err(message),
            Fail::Type(message) => PyTypeError::new_err(message),
            Fail::Unsupported(message) => PyNotImplementedError::new_err(message),
        }
    }
}

/// The lock options CASA's `lockoptions` accepts and reports: its name for
/// the mode, the mode, and the name `lockoptions()` gives back (`default`
/// is AutoLocking).
const OPTIONS: [(&str, LockMode, &str); 7] = [
    ("default", LockMode::DefaultLocking, "auto"),
    ("auto", LockMode::AutoLocking, "auto"),
    ("autonoread", LockMode::AutoNoReadLocking, "autonoread"),
    ("user", LockMode::UserLocking, "user"),
    ("usernoread", LockMode::UserNoReadLocking, "usernoread"),
    ("permanent", LockMode::PermanentLocking, "permanent"),
    (
        "permanentwait",
        LockMode::PermanentLockingWait,
        "permanentwait",
    ),
];

/// The `lockoptions` record of `open`, as given.
#[derive(Debug, Clone, Default)]
pub(crate) struct LockRequest {
    pub(crate) option: Option<String>,
    pub(crate) interval: Option<f64>,
    pub(crate) maxwait: Option<i64>,
}

/// The lock settings a table was opened with, as `lockoptions()` reports
/// them.
#[derive(Debug, Clone)]
pub(super) struct LockSettings {
    mode: LockMode,
    option: &'static str,
    interval: f64,
    maxwait: i64,
}

impl LockSettings {
    /// Whether the table locks itself for a write (and releases the lock
    /// after), as `default`, `auto` and `autonoread` do.
    pub(super) fn automatic(&self) -> bool {
        matches!(
            self.mode,
            LockMode::DefaultLocking | LockMode::AutoLocking | LockMode::AutoNoReadLocking
        )
    }

    /// Whether the table takes the write lock when it is opened and keeps it
    /// (the permanent modes).
    pub(super) fn permanent(&self) -> bool {
        matches!(
            self.mode,
            LockMode::PermanentLocking | LockMode::PermanentLockingWait
        )
    }

    /// Whether a read takes the read lock (all but the NoRead modes), as
    /// casacore's `TableLock::readLocking` says.
    pub(super) fn reads_lock(&self) -> bool {
        !matches!(
            self.mode,
            LockMode::AutoNoReadLocking | LockMode::UserNoReadLocking
        )
    }

    /// The request that opens a table with these settings again.
    pub(super) fn request(&self) -> LockRequest {
        LockRequest {
            option: Some(self.option.to_string()),
            interval: Some(self.interval),
            maxwait: Some(self.maxwait),
        }
    }

    /// The seconds between a table's looks for a request for its lock.
    pub(super) fn interval(&self) -> f64 {
        self.interval
    }

    /// The seconds a lock may be waited for; `0` waits for it.
    pub(super) fn maxwait(&self) -> i64 {
        self.maxwait
    }

    /// The attempts at a lock `maxwait` allows; `0` waits for it.
    pub(super) fn attempts(&self) -> u32 {
        u32::try_from(self.maxwait.max(0)).unwrap_or(u32::MAX)
    }

    pub(super) fn from_request(request: &LockRequest) -> Result<Self, Fail> {
        let name = request.option.as_deref().unwrap_or("default");
        let (_, mode, reported) = OPTIONS
            .iter()
            .find(|(option, _, _)| option.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                let valid = OPTIONS.map(|(option, _, _)| option).join(",");
                Fail::Runtime(format!(
                    "'{name}' is an unknown lock option; valid are {valid}"
                ))
            })?;
        Ok(Self {
            mode: *mode,
            option: reported,
            interval: request.interval.unwrap_or(5.0),
            maxwait: request.maxwait.unwrap_or(0),
        })
    }
}

/// A table `open` attached.
pub(super) struct Open {
    pub(super) table: Table,
    /// The table's absolute path, as `name()` gives it.
    pub(super) name: PathBuf,
    pub(super) writable: bool,
    pub(super) lock: LockSettings,
    /// Memory columns added after creation have a different CASA description.
    pub(super) memory_added_columns: HashSet<String>,
    /// The modify counter the last `datachanged()` saw (casacore starts a
    /// table at 0).
    pub(super) last_modify_counter: u32,
}

impl Open {
    /// The table at `name` opened under `request`'s lock options.
    pub(super) fn attach(name: &str, request: &LockRequest, writable: bool) -> Result<Self, Fail> {
        let lock = LockSettings::from_request(request)?;
        let path = Path::new(name);
        if !path.exists() {
            return Err(Fail::Runtime(format!("Table {name} does not exist")));
        }
        // A table opened `nomodify` holds no write lock, in the permanent
        // modes too.
        let options = LockOptions::new(lock.mode)
            .with_inspection_interval(lock.interval)
            .read_only(!writable);
        let table = Table::open_with_lock(TableOptions::new(path), options)?;
        let name = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
        Ok(Self {
            table,
            name,
            writable,
            lock,
            memory_added_columns: HashSet::new(),
            last_modify_counter: 0,
        })
    }
}

/// The tool's state: at most one open table.
pub(crate) struct State {
    pub(super) open: Option<Open>,
    /// Tells the tool apart in the registry of the process's tools
    /// (`registry.rs`).
    pub(super) id: u64,
}

impl Default for State {
    fn default() -> Self {
        Self::new(None)
    }
}

impl State {
    /// The state of a tool with `open` attached.
    pub(super) fn new(open: Option<Open>) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self {
            open,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }
}

/// Logs the message CASA logs when a call needs a table and none is open.
pub(super) fn no_table(log: &mut Vec<LogLine>) {
    log.push(LogLine::info("No table opened."));
}

/// Logs the warning CASA gives the lock calls with no table open.
pub(super) fn no_table_specified(log: &mut Vec<LogLine>) {
    log.push(LogLine::warning("No table specified, please open first"));
}

impl State {
    /// The open table, or `None` with CASA's INFO message logged.
    pub(super) fn table(&self, log: &mut Vec<LogLine>) -> Option<&Open> {
        let open = self.open.as_ref();
        if open.is_none() {
            no_table(log);
        }
        open
    }

    /// CASA's `open`: closes the table already open, then attaches `name`
    /// under `request`'s lock options.
    pub(super) fn open(
        &mut self,
        name: &str,
        request: &LockRequest,
        writable: bool,
    ) -> Result<(), Fail> {
        self.open = None;
        self.open = Some(Open::attach(name, request, writable)?);
        Ok(())
    }

    /// CASA's `close` and `done`: releases the table and its locks.
    pub(super) fn close(&mut self) {
        self.open = None;
    }

    /// What every read starts with, as casacore's `TableProxy::syncTable`:
    /// the read lock, when the lock options read-lock, taken again if it was
    /// released, which also reloads what another process has written since
    /// (a write lock already held is kept).
    pub(super) fn synchronize(&mut self) -> Result<(), Fail> {
        if let Some(open) = self.open.as_mut()
            && open.lock.reads_lock()
        {
            open.table.lock(LockType::Read, 0)?;
        }
        Ok(())
    }

    /// `lockoptions()`: the settings the table was opened with.
    pub(super) fn lock_settings(&self) -> Option<(&'static str, f64, i64)> {
        self.open
            .as_ref()
            .map(|open| (open.lock.option, open.lock.interval, open.lock.maxwait))
    }

    /// `haslock(write)`.
    pub(super) fn has_lock(&self, write: bool) -> bool {
        self.open.as_ref().is_some_and(|open| {
            open.table.has_lock(if write {
                LockType::Write
            } else {
                LockType::Read
            })
        })
    }

    /// `lock(write, nattempts)`: whether the lock is held afterwards.
    pub(super) fn lock(&mut self, write: bool, nattempts: u32) -> Result<bool, Fail> {
        let Some(open) = self.open.as_mut() else {
            return Ok(false);
        };
        let kind = if write {
            LockType::Write
        } else {
            LockType::Read
        };
        Ok(open.table.lock(kind, nattempts)?)
    }

    /// `unlock()`: releases the lock. Only table metadata is flushed, so a
    /// table's storage managers stay as they are.
    pub(super) fn unlock(&mut self) -> Result<bool, Fail> {
        let Some(open) = self.open.as_mut() else {
            return Ok(false);
        };
        open.table.unlock_metadata_only()?;
        Ok(true)
    }
}
