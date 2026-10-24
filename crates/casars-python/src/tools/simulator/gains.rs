// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna gains and polarization leakage: `setgain` and `setleakage`,
//! converted as casatools' `simulator_cmpt.cc` converts them and handed to
//! the session (#841 Phase 3).
//!
//! Before `open` each returns `False` without converting its arguments, as
//! CASA does. `corrupt` applies the terms when `close` writes the
//! MeasurementSet.

use casa_simulation::{GainSpec, LeakageSpec};
use pyo3::prelude::*;

use super::{PySimulator, outcome};
use crate::convert::{Variant, quantity};

/// The arguments of `sm.setgain`.
pub(super) struct GainRequest<'a> {
    pub(super) mode: &'a str,
    pub(super) table: &'a str,
    pub(super) interval: Variant,
    pub(super) amplitude: Vec<f64>,
}

/// `sm.setgain(...)`: simulate antenna gains over the rows observed so far.
///
/// As CASA's binding does, `interval` is read as a quantity (an unreadable
/// one raises), though CASA's simulation does not use it.
pub(super) fn setgain(
    tool: &PySimulator,
    py: Python<'_>,
    request: GainRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        quantity(&request.interval)?;
        let spec = GainSpec {
            mode: request.mode.to_string(),
            table: request.table.to_string(),
            amplitude: request.amplitude.clone(),
        };
        outcome(session.simulator.set_gain(&spec), lines)
    })?;
    Ok(result.unwrap_or(false))
}

/// `sm.setleakage(...)`: simulate polarization leakages over the rows
/// observed so far. CASA does not read `mode`.
pub(super) fn setleakage(
    tool: &PySimulator,
    py: Python<'_>,
    (table, amplitude, offset): (&str, Vec<f64>, Vec<f64>),
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let spec = LeakageSpec {
            table: table.to_string(),
            amplitude: amplitude.clone(),
            offset: offset.clone(),
        };
        outcome(session.simulator.set_leakage(&spec), lines)
    })?;
    Ok(result.unwrap_or(false))
}
