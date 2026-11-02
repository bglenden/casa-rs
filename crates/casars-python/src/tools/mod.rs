// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA tools as pyo3 classes, exposed as `casars.tools` (ADR-0019), and
//! the CASA helper classes casa-rs ports beside them (`casars.simutil`).
//!
//! Each tool is a class with CASA's name, method names, parameter order and
//! effective defaults, declared in Rust and described by its
//! [`surface::ToolSurface`]. Its state sits behind a `Mutex`, so the class is
//! `Send` and long calls release the GIL; argument conventions come from
//! [`crate::convert`]. CASA's log becomes Python logging under
//! `casars.tools.<tool>`: CASA's SEVERE messages are logged at ERROR and its
//! WARN messages at WARNING.

pub(crate) mod componentlist;
pub(crate) mod coordsys;
pub(crate) mod image;
pub(crate) mod measures;
pub(crate) mod ms;
pub(crate) mod msmetadata;
pub(crate) mod quanta;
pub(crate) mod simulator;
pub(crate) mod simutil;
pub(crate) mod surface;
pub(crate) mod table;

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

/// The `casars.tools` classes, in the object-surface bundle's order. The
/// bundle builds every tool entry from this one list
/// (`crate::object_contract`). `casars.simutil` is not a CASA tool and is
/// not listed.
pub(crate) const TOOLS: [&surface::ToolSurface; 9] = [
    &simulator::surface::SURFACE,
    &measures::surface::SURFACE,
    &quanta::surface::SURFACE,
    &componentlist::surface::SURFACE,
    &image::surface::SURFACE,
    &coordsys::surface::SURFACE,
    &table::surface::SURFACE,
    &msmetadata::surface::SURFACE,
    &ms::surface::SURFACE,
];

/// A Python logging level for a CASA log message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Level {
    /// CASA's INFO.
    Info,
    /// CASA's WARN.
    Warning,
    /// CASA's SEVERE.
    Error,
}

/// A message to log once the tool's lock is released, so a logging handler
/// that calls back into the tool cannot deadlock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogLine {
    pub(crate) level: Level,
    pub(crate) message: String,
}

impl LogLine {
    pub(crate) fn info(message: impl Into<String>) -> Self {
        Self {
            level: Level::Info,
            message: message.into(),
        }
    }

    pub(crate) fn warning(message: impl Into<String>) -> Self {
        Self {
            level: Level::Warning,
            message: message.into(),
        }
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self {
            level: Level::Error,
            message: message.into(),
        }
    }
}

/// Logs `lines` on the Python logger `logger`.
pub(crate) fn emit(py: Python<'_>, logger: &str, lines: &[LogLine]) -> PyResult<()> {
    if lines.is_empty() {
        return Ok(());
    }
    let logger = py.import("logging")?.call_method1("getLogger", (logger,))?;
    for line in lines {
        let method = match line.level {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Error => "error",
        };
        logger.call_method1(method, (line.message.as_str(),))?;
    }
    Ok(())
}

/// A method of `tool` that a later phase implements (#841).
pub(crate) fn not_yet_implemented<T>(tool: &str, method: &str) -> PyResult<T> {
    Err(PyNotImplementedError::new_err(format!(
        "{tool}.{method} is not yet implemented in casa-rs"
    )))
}

/// A method of `tool` casa-rs does not implement yet, refused naming the
/// issue that records it (#1025).
pub(crate) fn not_yet_implemented_in<T>(tool: &str, method: &str, issue: u32) -> PyResult<T> {
    Err(PyNotImplementedError::new_err(format!(
        "{tool}.{method} is not yet implemented in casa-rs (#{issue})"
    )))
}
