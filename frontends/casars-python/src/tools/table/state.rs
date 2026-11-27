// SPDX-License-Identifier: LGPL-3.0-or-later
//! The table the tool has open, its lock settings, and what a call may fail
//! with.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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

/// The underlying opened table and lock/metadata state shared between a
/// root `table` handle and any `selectrows` views created from it.
pub(super) struct RootOpen {
    pub(super) table: Table,
    /// The root table's absolute path.
    pub(super) name: PathBuf,
    pub(super) writable: bool,
    pub(super) lock: LockSettings,
    /// Memory columns added after creation have a different CASA description.
    pub(super) memory_added_columns: HashSet<String>,
    /// The modify counter the last `datachanged()` saw (casacore starts a
    /// table at 0).
    pub(super) last_modify_counter: u32,
    pub(super) keyword_epoch: Arc<AtomicU64>,
    pub(super) seen_keyword_epoch: u64,
    /// `True` while this handle is the one that called `create()`, before any
    /// reopen or `resync()`: casacore leaves `StandardStMan`'s `IndexLength`
    /// as `0` until the table is opened from disk.
    pub(super) created_in_this_handle: bool,
}

/// A table `open` attached, or a `selectrows` view sharing its root.
pub(super) struct Open {
    pub(super) root: Arc<Mutex<RootOpen>>,
    /// The handle's own path as `name()` reports it (`root.name` for a plain
    /// open, a scratch name for a transient `selectrows`, or the saved
    /// `RefTable` path for `selectrows(..., name=...)`).
    pub(super) name: PathBuf,
    /// Cached `root.name` so registry scans never take `root`'s mutex.
    pub(super) root_name: PathBuf,
    /// Cached plain table paths resolved from disk (`cache_plain_table_paths`),
    /// kept outside `root`'s mutex so cache/target checks cover opened `RefTable`s.
    pub(super) plain_paths: Vec<PathBuf>,
    pub(super) is_memory: bool,
    /// Present on a `selectrows` view: maps view row indices `0..len` to row
    /// indices in `root.table`.
    pub(super) row_map: Option<Vec<usize>>,
    /// Present on a column-projected `query`/`taql` view: restricts visible
    /// columns (and column order) relative to `root.table.schema()`.
    pub(super) columns: Option<Vec<String>>,
    /// Remains `true` while this `Open` handle is attached to its parent `table`
    /// tool; dropping or closing `Open` clears it so `tablerow` handles notice.
    pub(super) alive: Arc<AtomicBool>,
}

impl Drop for Open {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
    }
}

impl Open {
    pub(super) fn from_root(root: RootOpen) -> Self {
        let name = root.name.clone();
        let plain_paths = root.table.cache_plain_table_paths().unwrap_or_default();
        let is_memory = root.table.is_memory();
        Self {
            root: Arc::new(Mutex::new(root)),
            name: name.clone(),
            root_name: name,
            plain_paths,
            is_memory,
            row_map: None,
            columns: None,
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(super) fn from_detached_table(
        table: Table,
        name: PathBuf,
        writable: bool,
        lock: LockSettings,
    ) -> Self {
        let root = RootOpen {
            table,
            name: name.clone(),
            writable,
            lock,
            memory_added_columns: HashSet::new(),
            last_modify_counter: 0,
            keyword_epoch: super::registry::keyword_epoch(&name),
            seen_keyword_epoch: 0,
            created_in_this_handle: false,
        };
        Self::from_root(root)
    }

    pub(super) fn view(&self, name: PathBuf, row_map: Vec<usize>) -> Self {
        self.view_columns(name, row_map, self.columns.clone())
    }

    pub(super) fn view_columns(
        &self,
        name: PathBuf,
        row_map: Vec<usize>,
        columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            root: Arc::clone(&self.root),
            name,
            root_name: self.root_name.clone(),
            plain_paths: self.plain_paths.clone(),
            is_memory: self.is_memory,
            row_map: Some(row_map),
            columns,
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(super) fn root(&self) -> MutexGuard<'_, RootOpen> {
        self.root.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn is_view(&self) -> bool {
        self.row_map.is_some() || self.columns.is_some()
    }

    pub(super) fn column_allowed(&self, column: &str) -> bool {
        self.columns
            .as_ref()
            .is_none_or(|cols| cols.iter().any(|c| c == column))
    }

    pub(super) fn visible_columns<'a>(
        &self,
        table: &'a Table,
    ) -> Vec<&'a casa_tables::ColumnSchema> {
        let Some(schema) = table.schema() else {
            return Vec::new();
        };
        match &self.columns {
            None => schema.columns().iter().collect(),
            Some(cols) => cols.iter().filter_map(|c| schema.column(c)).collect(),
        }
    }

    pub(super) fn nrows(&self) -> usize {
        self.row_map
            .as_ref()
            .map_or_else(|| self.root().table.row_count(), Vec::len)
    }

    pub(super) fn translate_row(&self, row: usize) -> usize {
        self.row_map.as_ref().map_or(row, |map| map[row])
    }

    pub(super) fn translate_rows(&self, rows: &[usize]) -> Vec<usize> {
        match &self.row_map {
            Some(map) => rows.iter().map(|&row| map[row]).collect(),
            None => rows.to_vec(),
        }
    }

    /// The root handle opened at `name` under `request`'s lock options.
    pub(super) fn attach_root(
        name: &str,
        request: &LockRequest,
        writable: bool,
    ) -> Result<RootOpen, Fail> {
        let lock = LockSettings::from_request(request)?;
        let path = Path::new(name);
        crate::tools::componentlist::flush_pending_keywords(path)
            .map_err(|e| Fail::Runtime(e.to_string()))?;
        if !path.exists() {
            return Err(Fail::Runtime(format!("Table {name} does not exist")));
        }
        // A table opened `nomodify` holds no write lock, in the permanent
        // modes too.
        let options = LockOptions::new(lock.mode)
            .with_inspection_interval(lock.interval)
            .read_only(!writable);
        // Register before opening, so a concurrent keyword write during the
        // open is observed on this handle's next read.
        let keyword_epoch = super::registry::keyword_epoch(path);
        let seen_keyword_epoch = keyword_epoch.load(Ordering::Acquire);
        let table = Table::open_with_lock(TableOptions::new(path), options)?;
        let name = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
        Ok(RootOpen {
            table,
            name,
            writable,
            lock,
            memory_added_columns: HashSet::new(),
            last_modify_counter: 0,
            keyword_epoch,
            seen_keyword_epoch,
            created_in_this_handle: false,
        })
    }

    /// The table at `name` opened under `request`'s lock options.
    pub(super) fn attach(name: &str, request: &LockRequest, writable: bool) -> Result<Self, Fail> {
        Self::attach_root(name, request, writable).map(Self::from_root)
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
        let _publication = super::registry::lock_attachment_publication();
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
    /// released. Refresh an already-held lock after a component-list keyword
    /// write in this process; a write lock and its changes are kept.
    pub(super) fn synchronize(&mut self) -> Result<(), Fail> {
        if let Some(open) = self.open.as_ref() {
            let mut root = open.root();
            synchronize_root(&mut root)?;
        }
        Ok(())
    }
}

/// Publish only the backing table before a call that actually needs its bytes.
pub(super) fn publish_component_keywords(path: &Path) -> Result<(), Fail> {
    crate::tools::componentlist::flush_pending_keywords(path)
        .map_err(|e| Fail::Runtime(e.to_string()))
}

/// Synchronizes `root`'s read lock and refreshes any concurrent in-process
/// component-list keyword updates.
pub(super) fn synchronize_root(root: &mut RootOpen) -> Result<(), Fail> {
    publish_component_keywords(&root.name)?;
    if root.lock.reads_lock() {
        root.table.lock(LockType::Read, 0)?;
    }
    let epoch = root.keyword_epoch.load(Ordering::Acquire);
    if epoch != root.seen_keyword_epoch {
        let already_locked = root.table.has_lock(LockType::Read);
        if !already_locked && !root.table.lock(LockType::Read, 0)? {
            return Err(Fail::Runtime(
                "Cannot lock table to refresh keywords".into(),
            ));
        }
        root.table.resync_locked()?;
        if !already_locked {
            root.table.unlock_metadata_only()?;
        }
        root.seen_keyword_epoch = epoch;
    }
    Ok(())
}

impl State {
    /// `lockoptions()`: the settings the table was opened with.
    pub(super) fn lock_settings(&self) -> Option<(&'static str, f64, i64)> {
        self.open.as_ref().map(|open| {
            let root = open.root();
            (root.lock.option, root.lock.interval, root.lock.maxwait)
        })
    }

    /// `haslock(write)`.
    pub(super) fn has_lock(&self, write: bool) -> bool {
        self.open.as_ref().is_some_and(|open| {
            open.root().table.has_lock(if write {
                LockType::Write
            } else {
                LockType::Read
            })
        })
    }

    /// `lock(write, nattempts)`: whether the lock is held afterwards.
    pub(super) fn lock(&mut self, write: bool, nattempts: u32) -> Result<bool, Fail> {
        let Some(open) = self.open.as_ref() else {
            return Ok(false);
        };
        let kind = if write {
            LockType::Write
        } else {
            LockType::Read
        };
        publish_component_keywords(&open.root_name)?;
        Ok(open.root().table.lock(kind, nattempts)?)
    }

    /// `unlock()`: releases the lock. Only table metadata is flushed, so a
    /// table's storage managers stay as they are.
    pub(super) fn unlock(&mut self) -> Result<bool, Fail> {
        let Some(open) = self.open.as_ref() else {
            return Ok(false);
        };
        open.root().table.unlock_metadata_only()?;
        Ok(true)
    }
}
