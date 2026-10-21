// SPDX-License-Identifier: LGPL-3.0-or-later
//! Thermal noise: `setseed`, `setnoise`, `corrupt` and `reset`, converted as
//! casatools' `simulator_cmpt.cc` converts them and handed to the session.
//!
//! Before `open` each returns `False` without converting its arguments, as
//! CASA does. The noise is applied when `close` writes the MeasurementSet;
//! `corrupt` records which scans it covers.

use casa_simulation::NoiseSpec;
use pyo3::prelude::*;

use super::{PySimulator, outcome};
use crate::convert::{Variant, quantity, value_in};

/// `sm.setseed(seed)`: the seed of noise terms set from now on.
///
/// CASA's binding takes a C++ `long` and passes it to an `Int`, which keeps
/// the low 32 bits; so does casa-rs, so a seed beyond the 32-bit range works
/// as it does in CASA.
pub(super) fn setseed(tool: &PySimulator, py: Python<'_>, seed: i64) -> PyResult<bool> {
    let set = tool.on_session(py, |session, _| {
        session.simulator.set_seed(seed as i32);
        Ok(true)
    })?;
    Ok(set.unwrap_or(false))
}

/// The quantity arguments of `sm.setnoise`; the others are plain values the
/// signature already typed.
pub(super) struct NoiseRequest<'a> {
    pub(super) mode: &'a str,
    pub(super) simple_noise: Variant,
    pub(super) ground_pressure: Variant,
    pub(super) altitude: Variant,
    pub(super) water_height: Variant,
    pub(super) water_vapour: Variant,
    pub(super) on_the_fly: bool,
}

/// `sm.setnoise(...)`: add a noise term.
///
/// As CASA's binding does, every quantity argument is read (an unreadable
/// one raises), and only `simplenoise` is converted, to Jy, in
/// `simplenoise` mode; `[]` is 0 Jy. A `simplenoise` whose unit is not a flux
/// density raises, where CASA scales it silently (see
/// [`crate::convert::quantity_value`]). `table` and the atmosphere and
/// receiver parameters are those of the Tsys modes, which later phases
/// implement; with `OTF=True` CASA writes no table.
pub(super) fn setnoise(
    tool: &PySimulator,
    py: Python<'_>,
    request: &NoiseRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let simple_noise = quantity(&request.simple_noise)?;
        for other in [
            &request.ground_pressure,
            &request.altitude,
            &request.water_height,
            &request.water_vapour,
        ] {
            quantity(other)?;
        }
        let simple_noise_jy = if request.mode == "simplenoise" {
            value_in(&simple_noise, "Jy")?
        } else {
            0.0
        };
        let spec = NoiseSpec {
            mode: request.mode.to_string(),
            simple_noise_jy,
            on_the_fly: request.on_the_fly,
        };
        outcome(session.simulator.set_noise(&spec), lines)
    })?;
    Ok(result.unwrap_or(false))
}

/// `sm.corrupt()`: apply every noise term to the rows observed so far.
pub(super) fn corrupt(tool: &PySimulator, py: Python<'_>) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        outcome(session.simulator.corrupt(), lines)
    })?;
    Ok(result.unwrap_or(false))
}

/// `sm.reset()`: remove every noise term; what `corrupt` applied stays.
pub(super) fn reset(tool: &PySimulator, py: Python<'_>) -> PyResult<bool> {
    let reset = tool.on_session(py, |session, _| {
        session.simulator.reset();
        Ok(true)
    })?;
    Ok(reset.unwrap_or(false))
}
