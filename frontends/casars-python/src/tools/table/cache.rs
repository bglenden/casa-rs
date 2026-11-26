// SPDX-License-Identifier: LGPL-3.0-or-later
//! The table-tool process cache (#1028).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use casa_tables::{LockType, table_cache_name};
use pyo3::prelude::*;

use super::registry;
use super::state::{Fail, Open, State, no_table};
use super::{PyTableTool, flag, text};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

/// Protect a plain table held by a live table tool, including an underlying
/// table opened through a reference. A busy handle cannot be inspected.
pub(crate) fn path_is_in_use(path: &Path) -> bool {
    let Ok(path) = table_cache_name(path) else {
        return true;
    };
    let mut used = false;
    registry::for_each_other(u64::MAX, |state| {
        used |= match state {
            Some(state) => state.open.as_ref().is_some_and(|open| {
                open.root()
                    .table
                    .cache_plain_table_paths()
                    .map_or(true, |paths| {
                        paths
                            .into_iter()
                            .any(|name| name.canonicalize().unwrap_or(name).starts_with(&path))
                    })
            }),
            None => true,
        };
    });
    used
}

fn add_open(open: &Open, names: &mut BTreeMap<PathBuf, bool>) -> Result<(), Fail> {
    let root = open.root();
    let locked = root.table.has_lock(LockType::Read) || root.table.has_lock(LockType::Write);
    for name in root.table.cache_plain_table_paths()? {
        names
            .entry(name)
            .and_modify(|held| *held |= locked)
            .or_insert(locked);
    }
    Ok(())
}

impl State {
    fn cached_names(&self) -> Result<BTreeMap<PathBuf, bool>, Fail> {
        let mut names = registry::attached_table_paths()
            .into_iter()
            .map(|name| (name, false))
            .collect();
        if let Some(open) = &self.open {
            add_open(open, &mut names)?;
        }
        let mut failure = None;
        registry::for_each_other(self.id, |other| {
            if failure.is_some() {
                return;
            }
            match other {
                Some(other) => {
                    if let Some(open) = &other.open
                        && let Err(error) = add_open(open, &mut names)
                    {
                        failure = Some(error);
                    }
                }
                None => {
                    failure = Some(Fail::Runtime(
                        "Cannot inspect the table cache while another tool is busy".into(),
                    ))
                }
            }
        });
        match failure {
            Some(error) => Err(error),
            None => Ok(names),
        }
    }

    fn showcache(&self, verbose: bool, log: &mut Vec<LogLine>) -> Result<CasaValue, Fail> {
        if self.open.is_none() {
            no_table(log);
        }
        let names = self.cached_names()?;
        if verbose {
            if names.is_empty() {
                log.push(LogLine::info("The Table Cache is empty."));
            } else {
                log.push(LogLine::info(format!(
                    "The Table Cache has the following {} entries:",
                    names.len()
                )));
                for (index, name) in names.keys().enumerate() {
                    log.push(LogLine::info(format!(
                        "    {index}: \"{}\"",
                        name.display()
                    )));
                }
            }
        }
        Ok(CasaValue::List(
            names
                .into_keys()
                .map(|name| CasaValue::Text(name.to_string_lossy().into_owned()))
                .collect(),
        ))
    }
}

#[pymethods]
impl PyTableTool {
    /// Whether the process table cache contains `tablename`.
    #[pyo3(signature = (tablename), text_signature = "($self, tablename)")]
    fn isopened(&self, py: Python<'_>, tablename: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let name = text(tablename, "tablename")?;
        let path = py.import("os")?.getattr("path")?;
        let expanded = path.call_method1("expandvars", (name,))?;
        let expanded: String = path.call_method1("expanduser", (expanded,))?.extract()?;
        let name = PathBuf::from(expanded);
        self.call(py, move |state, log| {
            if state.open.is_none() {
                no_table(log);
            }
            let name = table_cache_name(&name)?;
            Ok(CasaValue::Bool(state.cached_names()?.contains_key(&name)))
        })
    }

    /// List the plain tables held in the process table cache.
    #[pyo3(signature = (verbose = None), text_signature = "($self, verbose=True)")]
    fn showcache(&self, py: Python<'_>, verbose: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let verbose = flag(verbose, true)?;
        self.call(py, move |state, log| state.showcache(verbose, log))
    }

    /// Print the process's locked tables.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn listlocks(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| {
            let names = state.cached_names()?;
            let stdout = std::io::stdout();
            let mut output = stdout.lock();
            writeln!(output, "Locked tables: ")
                .map_err(|error| Fail::Runtime(error.to_string()))?;
            for (name, locked) in names {
                if locked {
                    writeln!(output, "   {}", name.display())
                        .map_err(|error| Fail::Runtime(error.to_string()))?;
                }
            }
            writeln!(output, "--").map_err(|error| Fail::Runtime(error.to_string()))?;
            Ok(CasaValue::Bool(true))
        })
    }
}
