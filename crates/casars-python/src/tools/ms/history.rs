// SPDX-License-Identifier: LGPL-3.0-or-later

#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(unix)]
use casa_ms::tool_history::plain_history_path;
use casa_ms::tool_history::{ToolHistoryEntry, read_tool_history};
use casa_tables::{Table, TableOptions, TablePartNames};
use casa_types::quanta::format::{YMD, format_time};
use pyo3::prelude::*;

#[cfg(unix)]
use super::attachment_info::FileStamp;
use super::{AttachedMs, PyMsTool, runtime_error};
use crate::tools::{Level, LogLine, emit, logger};

const LOGGER: &str = "casars.tools.ms";

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn listhistory(&self, py: Python<'_>) -> PyResult<bool> {
        let entries = py.detach(|| -> PyResult<_> {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(attachment) = attached.as_mut() else {
                return Ok(None);
            };
            let link = attachment.working_metadata_link();
            link.verify_directory().map_err(runtime_error)?;
            let entries = attachment.history_entries()?;
            link.verify_directory().map_err(runtime_error)?;
            Ok(Some(entries))
        })?;
        let Some(entries) = entries else {
            return Ok(false);
        };
        let log = logger(py, LOGGER)?;
        let enabled = |level| -> PyResult<bool> {
            log.bind(py)
                .call_method1("isEnabledFor", (level,))?
                .extract()
        };
        let (debug, info, warning, error) =
            (enabled(10)?, enabled(20)?, enabled(30)?, enabled(40)?);
        if !(debug || info || warning || error) {
            return Ok(true);
        }
        let mut lines = Vec::with_capacity(entries.len() + 1);
        if info {
            let heading = if entries.is_empty() {
                "The HISTORY table is empty".into()
            } else {
                format!("History table entries: {}", entries.len())
            };
            lines.push(LogLine::info(heading));
        }
        lines.extend(entries.iter().filter_map(|entry| {
            let (level, enabled) = match entry.priority.as_str() {
                "SEVERE" => (Level::Error, error),
                "WARN" => (Level::Warning, warning),
                "NORMAL" | "INFO" | "NORMAL1" | "INFO1" | "NORMAL2" | "INFO2" | "NORMAL3"
                | "INFO3" | "NORMAL4" | "INFO4" | "NORMAL5" | "INFO5" => (Level::Info, info),
                _ => (Level::Debug, debug),
            };
            enabled.then(|| {
                let timestamp = format_time(entry.time / 86_400.0, YMD, 0);
                LogLine {
                    level,
                    message: format!("{timestamp}\t::{}\t{}", entry.origin, entry.message),
                }
            })
        }));
        emit(py, LOGGER, &lines)?;
        Ok(true)
    }
}

impl AttachedMs {
    fn history_entries(&mut self) -> PyResult<Arc<[ToolHistoryEntry]>> {
        let refreshed = self.refresh_history_main()?;
        let main = refreshed.as_ref().unwrap_or(&self.main);
        let plain = self.known_plain_history_main(refreshed.is_none());
        #[cfg(unix)]
        if plain {
            if let Some(snapshot) = &self.history_snapshot
                && snapshot.is_current()
            {
                return Ok(Arc::clone(&snapshot.entries));
            }
            self.history_snapshot = None;
            let path = plain_history_path(main).map_err(runtime_error)?;
            let stamps = history_stamps(&path);
            let entries: Arc<[ToolHistoryEntry]> =
                read_tool_history(main, true).map_err(runtime_error)?.into();
            if let Some(stamps) = stamps {
                let snapshot = HistorySnapshot {
                    entries: Arc::clone(&entries),
                    stamps,
                };
                if snapshot.is_current() {
                    self.history_snapshot = Some(snapshot);
                }
            }
            return Ok(entries);
        }
        read_tool_history(main, plain)
            .map(Arc::from)
            .map_err(runtime_error)
    }

    fn known_plain_history_main(&self, captured: bool) -> bool {
        captured && matches!(self.part_names, TablePartNames::Plain(_))
    }

    fn refresh_history_main(&self) -> PyResult<Option<Table>> {
        if self.main_stamp.is_some() && self.main_files.same_descriptor(self.main_stamp) {
            return Ok(None);
        }
        // A table keyword can change while the MS tool remains attached.
        Table::open(TableOptions::new(&self.path))
            .map(Some)
            .map_err(runtime_error)
    }
}

#[cfg(unix)]
pub(super) struct HistorySnapshot {
    entries: Arc<[ToolHistoryEntry]>,
    stamps: Vec<(PathBuf, FileStamp)>,
}

#[cfg(unix)]
impl HistorySnapshot {
    fn is_current(&self) -> bool {
        self.stamps
            .iter()
            .all(|(path, old)| FileStamp::read(path).is_ok_and(|now| now == *old))
    }
}

#[cfg(unix)]
fn history_stamps(path: &Path) -> Option<Vec<(PathBuf, FileStamp)>> {
    let table = Table::open(TableOptions::new(path)).ok()?;
    if !matches!(
        Table::immediate_part_names(path, path).ok()?,
        TablePartNames::Plain(_)
    ) {
        return None;
    }
    let [manager] = table.data_manager_info() else {
        return None;
    };
    // StandardStMan stores all HISTORY payload and publication metadata
    // in this directory. Watch the target itself and its table files.
    if manager.dm_type != "StandardStMan" {
        return None;
    }
    let mut stamps = vec![(path.to_path_buf(), FileStamp::read(path).ok()?)];
    for entry in path.read_dir().ok()? {
        let entry = entry.ok()?;
        if entry.file_name().to_str()?.starts_with("table.") {
            let stamp = FileStamp::read(&entry.path()).ok()?;
            stamps.push((entry.path(), stamp));
        }
    }
    Some(stamps)
}
