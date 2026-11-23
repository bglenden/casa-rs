// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's calls that report or move a table's state, as CASA
//! 6.7.6.14's `tb` does them (`tools/casa-compat/table_tool_oracle.py`,
//! scenarios `state` and `state_ms`): `flush`, `resync`, `datachanged`,
//! `ismultiused`, `endianformat`, `setmaxcachesize` and `clearlocks`.
//!
//! `flush` has nothing to write: every change the tool makes is on the disk
//! when its call returns (`write.rs`); a table that keeps its write lock
//! announces them, as casacore's flush does. `setmaxcachesize` checks its
//! arguments as CASA does and sets nothing, since casa-tables sizes the
//! caches of its storage managers itself.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::TryLockError;

use casa_tables::{EndianFormat, LockType, Table, TableOptions, add_directory};
use casa_types::{RecordValue, Value};
use pyo3::prelude::*;

use super::registry;
use super::state::{Fail, State, no_table, no_table_specified};
use super::{PyTableTool, flag};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

/// Collect table-reference keyword paths inside `record` in field order.
fn collect_table_refs(record: &RecordValue, out: &mut Vec<String>) {
    for field in record.fields() {
        match &field.value {
            Value::TableRef(target) => {
                out.push(target.strip_prefix("Table: ").unwrap_or(target).to_string());
            }
            Value::Record(inner) => collect_table_refs(inner, out),
            _ => {}
        }
    }
}

fn format_record_brief(record: &RecordValue) -> String {
    if record.fields().is_empty() {
        return "None".to_string();
    }
    record
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

struct SummaryView<'a> {
    display_path: &'a Path,
    base_path: &'a Path,
    nrows: usize,
    visible_cols: Option<&'a [String]>,
}

fn log_table_summary(
    table: &Table,
    view: SummaryView<'_>,
    recurse: bool,
    visited: &mut HashSet<PathBuf>,
    log: &mut Vec<LogLine>,
) -> Result<(), Fail> {
    let SummaryView {
        display_path,
        base_path,
        nrows,
        visible_cols,
    } = view;
    if display_path.exists() {
        let display_real =
            std::fs::canonicalize(display_path).unwrap_or_else(|_| display_path.to_path_buf());
        visited.insert(display_real);
    }
    let real = std::fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    visited.insert(real);
    let cols: Vec<String> = match visible_cols {
        Some(cols) => cols.to_vec(),
        None => table
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns())
            .map(|col| col.name().to_string())
            .collect(),
    };
    let info = table.info();
    let readme: String = info.readme.iter().map(|line| format!("{line}\n")).collect();
    log.push(LogLine::info(format!(
        "Table summary: {}",
        display_path.display()
    )));
    log.push(LogLine::info(format!(
        "Shape: {} columns by {nrows}",
        cols.len()
    )));
    log.push(LogLine::info(format!(
        "Info:   type: String \"{}\"  subType: String \"{}\"  readme: String \"{}\"",
        info.table_type, info.sub_type, readme
    )));
    log.push(LogLine::info(format!(
        "Table keywords: {}",
        format_record_brief(table.keywords())
    )));
    log.push(LogLine::info(format!("Columns: {}", cols.join(" "))));
    for col in &cols {
        let kw = table
            .column_keywords(col)
            .map_or_else(|| "None".to_string(), format_record_brief);
        log.push(LogLine::info(format!("{col} keywords: {kw}")));
    }
    if recurse {
        let mut refs = Vec::new();
        collect_table_refs(table.keywords(), &mut refs);
        for col in &cols {
            if let Some(kw) = table.column_keywords(col) {
                collect_table_refs(kw, &mut refs);
            }
        }
        for sub_ref in refs {
            let sub_path = add_directory(&sub_ref, base_path);
            let canonical = std::fs::canonicalize(&sub_path).unwrap_or_else(|_| sub_path.clone());
            if visited.contains(&canonical) || !sub_path.exists() {
                continue;
            }
            if let Ok(subtable) = Table::open(TableOptions::new(&sub_path)) {
                log_table_summary(
                    &subtable,
                    SummaryView {
                        display_path: &sub_path,
                        base_path: &sub_path,
                        nrows: subtable.row_count(),
                        visible_cols: None,
                    },
                    true,
                    visited,
                    log,
                )?;
            }
        }
    }
    Ok(())
}

/// Whether `record` holds a reference to another table, at any depth.
fn holds_table_reference(record: &RecordValue) -> bool {
    record.fields().iter().any(|field| match &field.value {
        Value::TableRef(_) => true,
        Value::Record(inner) => holds_table_reference(inner),
        _ => false,
    })
}

/// Whether `table` has subtables: a table-valued keyword of its own or of a
/// column.
pub(super) fn has_subtables(table: &Table) -> bool {
    holds_table_reference(table.keywords())
        || table.schema().is_some_and(|schema| {
            schema.columns().iter().any(|column| {
                table
                    .column_keywords(column.name())
                    .is_some_and(holds_table_reference)
            })
        })
}

impl State {
    /// `endianformat()`: the byte order of the table's files; `little`
    /// with no table open.
    pub(super) fn endianformat(&self, log: &mut Vec<LogLine>) -> Reply {
        let big = match self.open.as_ref() {
            Some(open) if open.is_memory => cfg!(target_endian = "big"),
            Some(open) => match open.root().table.stored_endian_format()? {
                Some(format) => format == EndianFormat::BigEndian,
                None => {
                    return Err(Fail::Unsupported(
                        "table.endianformat of a table that is a concatenation of tables is not \
                         yet implemented in casa-rs (#1028)"
                            .to_string(),
                    ));
                }
            },
            None => {
                no_table_specified(log);
                false
            }
        };
        Ok(CasaValue::Text(
            if big { "big" } else { "little" }.to_string(),
        ))
    }

    /// `flush()`. Every change reaches the disk when the call that makes it
    /// returns; what is left is the announcement of them to the table's other
    /// users, which a table holding its write lock (the user and permanent
    /// modes) makes now, as casacore's flush does, and keeps the lock.
    pub(super) fn flush(&mut self, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        open.root().table.publish_writes()?;
        Ok(CasaValue::Bool(true))
    }

    /// `resync()`: the table takes what another process, or another tool of
    /// this process, has written since it last looked. casacore reads the
    /// lock file without locking; here a table that holds a lock reads it
    /// again and reloads if it has changed, and a table that holds none takes
    /// the read lock, which reloads it if it has changed, and gives it back,
    /// so its lock state is as it was. A table that cannot be locked is not
    /// resynchronized, and the call says so.
    pub(super) fn resync(&mut self, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        if open.is_memory {
            return Ok(CasaValue::Bool(true));
        }
        let mut root = open.root();
        if root.table.has_lock(LockType::Read) {
            root.table.resync_locked()?;
        } else {
            let attempts = root.lock.attempts();
            if !root.table.lock(LockType::Read, attempts)? {
                return Err(Fail::Runtime(
                    "Table::resync: the table could not be locked to read what has changed in it"
                        .to_string(),
                ));
            }
            root.table.unlock_metadata_only()?;
        }
        root.created_in_this_handle = false;
        Ok(CasaValue::Bool(true))
    }

    /// `datachanged()`, as casacore's `Table::hasDataChanged`: a table that
    /// holds no lock tries for a read lock without waiting (another process
    /// writing means the data is changing) and gives it back; then the
    /// modify counter the lock file publishes is compared with the one the
    /// last call saw.
    pub(super) fn datachanged(&mut self, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        if open.is_memory {
            return Ok(CasaValue::Bool(false));
        }
        let mut root = open.root();
        let took_lock = !root.table.has_lock(LockType::Read);
        if took_lock && !root.table.lock(LockType::Read, 1)? {
            return Ok(CasaValue::Bool(true));
        }
        let counter = root.table.published_modify_counter();
        if took_lock {
            root.table.unlock_metadata_only()?;
        }
        let counter = counter?;
        let changed = counter != root.last_modify_counter;
        root.last_modify_counter = counter;
        Ok(CasaValue::Bool(changed))
    }

    /// `ismultiused(checksubtables)`: whether another process has the table
    /// open. Subtables are not looked at: asking for them of a table that
    /// has any is not implemented.
    pub(super) fn ismultiused(&mut self, check_subtables: bool, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        let root = open.root();
        if check_subtables && has_subtables(&root.table) {
            return Err(Fail::Unsupported(
                "table.ismultiused(checksubtables=True) of a table with subtables is not yet \
                 implemented in casa-rs (#1028)"
                    .to_string(),
            ));
        }
        Ok(CasaValue::Bool(root.table.is_multi_used()))
    }

    /// `setmaxcachesize(columnname, nbytes)`, in CASA's order of checks.
    pub(super) fn setmaxcachesize(
        &self,
        column: &str,
        nbytes: i64,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        if column.is_empty() {
            log.push(LogLine::warning("Need to specify a column name"));
            return Ok(CasaValue::Bool(false));
        }
        if nbytes <= 0 {
            log.push(LogLine::warning(
                "Need to specify cache size greater than 0",
            ));
            return Ok(CasaValue::Bool(false));
        }
        open.known(column)?;
        Ok(CasaValue::Bool(true))
    }

    /// `clearlocks()`: every table of the process that locks itself
    /// automatically gives up the lock it keeps between calls, whichever tool
    /// has it open, as casacore's `Table::relinquishAutoLocks` does; the
    /// tables locked by the user or permanently keep theirs. Always `True`,
    /// with or without a table. A tool busy in another thread is not asked.
    pub(super) fn clearlocks(&mut self) -> Reply {
        let mut result = self.relinquish_auto_lock(None);
        registry::for_each_other(self.id, |tool| {
            if let Some(tool) = tool {
                let released = tool.relinquish_auto_lock(None);
                if result.is_ok() {
                    result = released;
                }
            }
        });
        result?;
        Ok(CasaValue::Bool(true))
    }

    /// Gives up the lock an automatic mode keeps between calls.
    pub(super) fn relinquish_auto_lock(&mut self, targets: Option<&[PathBuf]>) -> Result<(), Fail> {
        if let Some(open) = self.open.as_ref() {
            let mut root = if let Some(targets) = targets {
                let matches = std::iter::once(&open.root_name)
                    .chain(&open.plain_paths)
                    .filter_map(|path| path.canonicalize().ok())
                    .any(|path| targets.contains(&path));
                if !matches {
                    return Ok(());
                }
                match open.root.try_lock() {
                    Ok(root) => root,
                    Err(TryLockError::Poisoned(error)) => error.into_inner(),
                    Err(TryLockError::WouldBlock) => return Ok(()),
                }
            } else {
                open.root()
            };
            if root.lock.automatic()
                && (root.table.has_lock(LockType::Read) || root.table.has_lock(LockType::Write))
            {
                root.table.unlock_metadata_only()?;
            }
        }
        Ok(())
    }

    /// `summary(recurse)`: logs a summary of the open table (and subtables
    /// referenced by `Table:` keywords when `recurse` is `true`).
    fn summary(&self, recurse: bool, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.open.as_ref() else {
            no_table(log);
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        };
        let display_path = open.name.clone();
        let base_path = open
            .plain_paths
            .first()
            .cloned()
            .unwrap_or_else(|| open.root_name.clone());
        let root = open.root();
        let nrows = open
            .row_map
            .as_ref()
            .map_or_else(|| root.table.row_count(), Vec::len);
        let mut visited = HashSet::new();
        if open.root_name != base_path && open.root_name.exists() {
            visited.insert(
                std::fs::canonicalize(&open.root_name).unwrap_or_else(|_| open.root_name.clone()),
            );
        }
        let visible_cols: Option<Vec<String>> = open.columns.as_ref().map(|_| {
            open.visible_columns(&root.table)
                .into_iter()
                .map(|c| c.name().to_string())
                .collect()
        });
        log_table_summary(
            &root.table,
            SummaryView {
                display_path: &display_path,
                base_path: &base_path,
                nrows,
                visible_cols: visible_cols.as_deref(),
            },
            recurse,
            &mut visited,
            log,
        )?;
        Ok(CasaValue::Bool(true))
    }

    /// `browse()`: GUI table browser is unavailable in headless casa-rs.
    fn browse(&self, log: &mut Vec<LogLine>) -> Reply {
        if self.open.is_none() {
            no_table(log);
            no_table_specified(log);
            return Ok(CasaValue::Bool(false));
        }
        log.push(LogLine::warning(
            "table.browse: GUI table browser is not available in headless casa-rs",
        ));
        Ok(CasaValue::Bool(false))
    }
}

#[pymethods]
impl PyTableTool {
    /// Log a summary of the open table.
    #[pyo3(signature = (recurse = None), text_signature = "($self, recurse=False)")]
    fn summary(&self, py: Python<'_>, recurse: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let recurse = flag(recurse, false)?;
        self.read(py, move |state, log| state.summary(recurse, log))
    }

    /// GUI table browser (unavailable in headless casa-rs).
    #[pyo3(signature = (), text_signature = "($self)")]
    fn browse(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, log| state.browse(log))
    }
}
