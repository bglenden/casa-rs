// SPDX-License-Identifier: LGPL-3.0-or-later
//! Methods CASA itself disables or deprecates.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{LOGGER, PySimulator};
use crate::tools::{LogLine, emit};

/// CASA's message for the corruption methods it disabled in 2020.
const DISABLED: &str =
    "Caught exception: Corruption by simulated errors temporarily disabled (06Nov20 gmoellen)";

/// `setbandpass`, `setpa` and `setpointingerror`, which CASA disables: after
/// `open` they log CASA's error and return `False`; before it, `False`.
pub(super) fn disabled_in_casa(tool: &PySimulator, py: Python<'_>) -> PyResult<bool> {
    if tool.is_open(py) {
        emit(py, LOGGER, &[LogLine::error(DISABLED)])?;
    }
    Ok(false)
}

/// `oldsetnoise`, which CASA deprecates in favour of `setnoise`.
pub(super) fn deprecated_oldsetnoise() -> PyResult<bool> {
    Err(PyRuntimeError::new_err(
        "simulator.oldsetnoise is deprecated in CASA; casa-rs provides simulator.setnoise instead",
    ))
}
