// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::tool_history::read_tool_history;
use casa_tables::{Table, TableOptions, TablePartNames};
use casa_types::quanta::format::{YMD, format_time};
use pyo3::prelude::*;

use super::{AttachedMs, PyMsTool, runtime_error};
use crate::tools::{Level, LogLine, emit, logger};

const LOGGER: &str = "casars.tools.ms";

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn listhistory(&self, py: Python<'_>) -> PyResult<bool> {
        let entries = self.attachment_info(py, |attachment| {
            let Some(attachment) = attachment else {
                return Ok(None);
            };
            let link = attachment.working_metadata_link();
            link.verify_directory().map_err(runtime_error)?;
            let refreshed = attachment.refresh_history_main()?;
            let main = refreshed.as_ref().unwrap_or(&attachment.main);
            let plain = attachment.known_plain_history_main(refreshed.is_none());
            let entries = read_tool_history(main, plain).map_err(runtime_error)?;
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
        let mut lines = Vec::with_capacity(entries.len() + 1);
        if info {
            let heading = if entries.is_empty() {
                "The HISTORY table is empty".into()
            } else {
                format!("History table entries: {}", entries.len())
            };
            lines.push(LogLine::info(heading));
        }
        lines.extend(entries.into_iter().filter_map(|entry| {
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
