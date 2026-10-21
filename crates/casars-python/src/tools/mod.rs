// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA tools as pyo3 classes, exposed as `casars.tools` (ADR-0019).
//!
//! Each tool is a class with CASA's name, method names, parameter order and
//! effective defaults. Its state sits behind a `Mutex`, so the class is
//! `Send` and long calls release the GIL; argument conventions come from
//! [`crate::convert`]. CASA's log becomes Python logging under
//! `casars.tools.<tool>`: CASA's SEVERE messages are logged at ERROR.

pub(crate) mod simulator;

use pyo3::prelude::*;

/// A Python logging level for a CASA log message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Level {
    /// CASA's INFO.
    Info,
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
            Level::Error => "error",
        };
        logger.call_method1(method, (line.message.as_str(),))?;
    }
    Ok(())
}
