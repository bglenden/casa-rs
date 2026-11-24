// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_ms::tool_history::{append_tool_history, read_tool_history};
use casa_tables::table_cache_name;
use casa_types::quanta::format::{YMD, format_time};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::selection_indices::selection_text;
use super::visibility::requested_strings;
use super::{PyMsTool, runtime_error};
use crate::tools::{LogLine, emit};

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
            let entries = read_tool_history(&attachment.path).map_err(runtime_error)?;
            link.verify_directory().map_err(runtime_error)?;
            Ok(Some(entries))
        })?;
        let Some(entries) = entries else {
            return Ok(false);
        };
        let mut lines = Vec::with_capacity(entries.len() + 1);
        if entries.is_empty() {
            lines.push(LogLine::info("The HISTORY table is empty"));
        } else {
            lines.push(LogLine::info(format!(
                "History table entries: {}",
                entries.len()
            )));
        }
        lines.extend(entries.into_iter().map(|entry| {
            let timestamp = format_time(entry.time / 86_400.0, YMD, 0);
            let message = format!("{timestamp}\t::{}\t{}", entry.origin, entry.message);
            match entry.priority.as_str() {
                "SEVERE" => LogLine::error(message),
                "WARN" => LogLine::warning(message),
                "NORMAL" | "INFO" | "NORMAL1" | "INFO1" | "NORMAL2" | "INFO2" | "NORMAL3"
                | "INFO3" | "NORMAL4" | "INFO4" | "NORMAL5" | "INFO5" => LogLine::info(message),
                _ => LogLine::debug(message),
            }
        }));
        emit(py, "casars.tools.ms", &lines)?;
        Ok(true)
    }

    #[pyo3(signature = (message = None, parms = None, origin = None, msname = None, app = None), text_signature = "($self, message='', parms='', origin='MSHistoryHandler::addMessage()', msname='', app='ms')")]
    fn writehistory(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = message_argument)] message: Option<String>,
        #[pyo3(from_py_with = parms_argument)] parms: Option<String>,
        #[pyo3(from_py_with = origin_argument)] origin: Option<String>,
        #[pyo3(from_py_with = msname_argument)] msname: Option<PathBuf>,
        #[pyo3(from_py_with = app_argument)] app: Option<String>,
    ) -> PyResult<bool> {
        let message = message.unwrap_or_default();
        if message.is_empty() && parms.as_deref().is_none_or(str::is_empty) {
            return Ok(false);
        }
        self.append_history(py, vec![message], parms, origin, msname, app)
    }

    #[pyo3(signature = (messages = None, parms = None, origin = None, msname = None, app = None), text_signature = "($self, messages=[], parms='', origin='MSHistoryHandler::addMessage()', msname='', app='ms')")]
    fn writehistory_batch(
        &self,
        py: Python<'_>,
        messages: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = parms_argument)] parms: Option<String>,
        #[pyo3(from_py_with = origin_argument)] origin: Option<String>,
        #[pyo3(from_py_with = msname_argument)] msname: Option<PathBuf>,
        #[pyo3(from_py_with = app_argument)] app: Option<String>,
    ) -> PyResult<bool> {
        let messages = requested_strings(messages, "messages")?;
        self.append_history(py, messages, parms, origin, msname, app)
    }
}

impl PyMsTool {
    fn append_history(
        &self,
        py: Python<'_>,
        messages: Vec<String>,
        parms: Option<String>,
        origin: Option<String>,
        target: Option<PathBuf>,
        app: Option<String>,
    ) -> PyResult<bool> {
        let parms = parms.as_deref().unwrap_or("");
        if messages.is_empty() && parms.is_empty() {
            return Ok(false);
        }
        let origin = origin.as_deref().unwrap_or("");
        let app = app.as_deref().unwrap_or("ms");
        let write = |path: &Path| {
            append_tool_history(path, &messages, parms, origin, app).map_err(runtime_error)?;
            Ok(true)
        };
        if let Some(path) = target.filter(|path| !path.as_os_str().is_empty()) {
            return py.detach(|| write(&table_cache_name(&path).map_err(runtime_error)?));
        }
        self.attachment_info(py, |attachment| {
            if let Some(ms) = attachment {
                ms.working_metadata_link()
                    .verify_directory()
                    .map_err(runtime_error)?;
                write(&ms.path)
            } else {
                write(&table_cache_name(Path::new("none")).map_err(runtime_error)?)
            }
        })
    }
}

macro_rules! string_argument {
    ($function:ident, $argument:literal) => {
        fn $function(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
            selection_text(value, $argument).map(Some)
        }
    };
}

string_argument!(message_argument, "message");
string_argument!(parms_argument, "parms");
string_argument!(origin_argument, "origin");
string_argument!(app_argument, "app");

fn msname_argument(value: &Bound<'_, PyAny>) -> PyResult<Option<PathBuf>> {
    #[cfg(unix)]
    if let Ok(bytes) = value.cast::<PyBytes>() {
        use std::os::unix::ffi::OsStringExt;
        return Ok(Some(
            std::ffi::OsString::from_vec(bytes.as_bytes().to_vec()).into(),
        ));
    }
    selection_text(value, "msname").map(|path| Some(path.into()))
}
