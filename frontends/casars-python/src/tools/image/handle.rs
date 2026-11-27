// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 error/log/handle helpers delegating to `casa_imageanalysis::tool::handle`.

use std::fmt::Display;
use std::path::Path;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pub(super) use casa_imageanalysis::tool::attachment::ExternalPathCheck;
pub(super) use casa_imageanalysis::tool::error::{ImageToolError, ToolLogLevel, ToolLogLine};
pub(super) use casa_imageanalysis::tool::handle::ImageHandle;
pub(super) use casa_imageanalysis::with_image_storage as with_storage;

use crate::tools::{LogLine, emit, not_yet_implemented_in};

pub(super) fn raised(error: impl Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

pub(super) fn tool_error(error: ImageToolError) -> PyErr {
    if let Some((area, feature, issue)) = error.not_implemented_info() {
        return not_yet_implemented_in::<()>(area, feature, issue)
            .expect_err("not_yet_implemented_in always returns Err");
    }
    PyRuntimeError::new_err(error.to_string())
}

pub(super) fn emit_tool_logs(py: Python<'_>, logs: &[ToolLogLine]) -> PyResult<()> {
    if logs.is_empty() {
        return Ok(());
    }
    let converted: Vec<LogLine> = logs
        .iter()
        .map(|entry| match entry.level {
            ToolLogLevel::Info => LogLine::info(entry.message.clone()),
            ToolLogLevel::Warning => LogLine::warning(entry.message.clone()),
            ToolLogLevel::Error => LogLine::error(entry.message.clone()),
            ToolLogLevel::Debug => LogLine::debug(entry.message.clone()),
        })
        .collect();
    emit(py, "casars.tools.image", &converted)
}

pub(super) fn external_open_check() -> ExternalPathCheck<'static> {
    &|path: &Path| crate::tools::table::path_is_in_use(path)
}
