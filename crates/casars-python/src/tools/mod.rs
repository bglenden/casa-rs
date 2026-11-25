// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA tools as pyo3 classes, exposed as `casars.tools` (ADR-0017), and
//! the CASA helper classes casa-rs ports beside them (`casars.simutil`).
//!
//! Each tool is a class with CASA's name, method names, parameter order and
//! effective defaults, declared in Rust and described by its
//! [`surface::ToolSurface`]. Its state sits behind a `Mutex`, so the class is
//! `Send` and long calls release the GIL; argument conventions come from
//! [`crate::convert`]. CASA's log becomes Python logging under
//! `casars.tools.<tool>`: CASA's SEVERE messages are logged at ERROR and its
//! WARN messages at WARNING.

pub(crate) mod agentflagger;
pub(crate) mod atmosphere;
pub(crate) mod calanalysis;
pub(crate) mod calibrater;
pub(crate) mod componentlist;
pub(crate) mod coordsys;
pub(crate) mod image;
pub(crate) mod imager;
pub(crate) mod measures;
pub(crate) mod ms;
pub(crate) mod msmetadata;
pub(crate) mod quanta;
pub(crate) mod simulator;
pub(crate) mod simutil;
pub(crate) mod singledishms;
pub(crate) mod surface;
pub(crate) mod table;
pub(crate) mod wvr;

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

/// The `casars.tools` classes, in the object-surface bundle's order. The
/// bundle builds every tool entry from this one list
/// (`crate::object_contract`). `casars.simutil` is not a CASA tool and is
/// not listed.
pub(crate) const TOOLS: [&surface::ToolSurface; 16] = [
    &simulator::surface::SURFACE,
    &measures::surface::SURFACE,
    &quanta::surface::SURFACE,
    &componentlist::surface::SURFACE,
    &image::surface::SURFACE,
    &coordsys::surface::SURFACE,
    &table::surface::SURFACE,
    &msmetadata::surface::SURFACE,
    &ms::surface::SURFACE,
    &calibrater::surface::SURFACE,
    &imager::surface::SURFACE,
    &agentflagger::surface::SURFACE,
    &wvr::surface::SURFACE,
    &singledishms::surface::SURFACE,
    &calanalysis::surface::SURFACE,
    &atmosphere::surface::SURFACE,
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
    /// CASA's DEBUG levels, below its log's default filter.
    Debug,
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

    pub(crate) fn debug(message: impl Into<String>) -> Self {
        Self {
            level: Level::Debug,
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

// Python logging already owns one logger per name. Retain that object so
// warning-heavy tool calls do not import logging and repeat getLogger on
// every message. Levels, handlers and logging.disable stay dynamic.
static LOGGERS: Mutex<BTreeMap<String, Py<PyAny>>> = Mutex::new(BTreeMap::new());

fn logger(py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
    if let Some(logger) = LOGGERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(name)
    {
        return Ok(logger.clone_ref(py));
    }
    // Never hold the cache lock while calling Python, including handlers
    // which can re-enter a tool and log another message.
    let logger = py
        .import("logging")?
        .call_method1("getLogger", (name,))?
        .unbind();
    LOGGERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(name.into(), logger.clone_ref(py));
    Ok(logger)
}

/// Logs `lines` on the Python logger `logger`.
pub(crate) fn emit(py: Python<'_>, logger: &str, lines: &[LogLine]) -> PyResult<()> {
    if lines.is_empty() {
        return Ok(());
    }
    let logger = self::logger(py, logger)?;
    let logger = logger.bind(py);
    for line in lines {
        let method = match line.level {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Error => "error",
            Level::Debug => "debug",
        };
        logger.call_method1(method, (line.message.as_str(),))?;
    }
    Ok(())
}

/// A method of `tool` casa-rs does not implement yet, refused naming the
/// issue that records it: every refusal names one.
pub(crate) fn not_yet_implemented_in<T>(tool: &str, method: &str, issue: u32) -> PyResult<T> {
    Err(PyNotImplementedError::new_err(format!(
        "{tool}.{method} is not yet implemented in casa-rs (#{issue})"
    )))
}
