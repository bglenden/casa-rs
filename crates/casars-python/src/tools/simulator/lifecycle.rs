// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session's lifecycle: `open`, `close`/`done`, `name` and `summary`.

use std::path::PathBuf;
use std::sync::Arc;

use casa_simulation::{Simulator, SimulatorOptions, SimulatorResult};
use pyo3::prelude::*;

use super::{LOGGER, PySimulator, Session, raised};
use crate::tools::{LogLine, emit};

/// `sm.open(ms)`: close (write) any open session, then start one that will
/// create `ms` at `close`. CASA's reference time until `settimes` is the
/// time of this call. The GIL is released while the previous session writes
/// and the measures data loads.
pub(super) fn open(tool: &PySimulator, py: Python<'_>, ms: &str) -> PyResult<bool> {
    let budget = tool.memory_budget;
    let path = PathBuf::from(ms);
    py.detach(move || -> SimulatorResult<()> {
        let mut state = tool.lock();
        if let Some(previous) = state.take() {
            previous.simulator.close()?;
        }
        let mut options = SimulatorOptions::from_system()?;
        if let Some(bytes) = budget {
            options.write_resources.available_bytes = bytes;
        }
        let measures = Arc::clone(&options.measures);
        *state = Some(Session {
            simulator: Simulator::open(path, options),
            measures,
        });
        Ok(())
    })
    .map_err(|error| raised(&error))?;
    Ok(true)
}

/// `sm.close()` and `sm.done()`: write the MeasurementSet and end the session.
/// `True` even when nothing is open; a write failure raises. The GIL is
/// released while writing.
pub(super) fn close(tool: &PySimulator, py: Python<'_>) -> PyResult<bool> {
    py.detach(|| match tool.lock().take() {
        Some(session) => session.simulator.close(),
        None => Ok(()),
    })
    .map_err(|error| raised(&error))?;
    Ok(true)
}

/// `sm.name()`: the MeasurementSet's path as given to `open`, or `''`.
pub(super) fn name(tool: &PySimulator, py: Python<'_>) -> String {
    tool.lock_attached(py)
        .as_ref()
        .map(|session| session.simulator.name().to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `sm.summary()`: a few INFO lines about the session; `False` before
/// `open`. CASA's summary also lists the configuration, windows and fields.
pub(super) fn summary(tool: &PySimulator, py: Python<'_>) -> PyResult<bool> {
    let lines = tool.lock_attached(py).as_ref().map(|session| {
        let simulator = &session.simulator;
        vec![
            LogLine::info(format!(
                "Simulator summary for MeasurementSet {}",
                simulator.name().display()
            )),
            LogLine::info(format!(
                "     {} scan(s) planned; the MeasurementSet is written at close or done",
                simulator.scan_count()
            )),
        ]
    });
    match lines {
        Some(lines) => {
            emit(py, LOGGER, &lines)?;
            Ok(true)
        }
        None => Ok(false),
    }
}
