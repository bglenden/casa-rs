// SPDX-License-Identifier: LGPL-3.0-or-later
//! Thermal noise: `setseed`, `setnoise`, `corrupt` and `reset`, converted as
//! casatools' `simulator_cmpt.cc` converts them and handed to the session.
//!
//! Before `open` each returns `False` without converting its arguments, as
//! CASA does. The noise is applied when `close` writes the MeasurementSet;
//! `corrupt` records which scans it covers.

use casa_simulation::{NoiseSpec, TsysParameters};
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

/// The arguments of `sm.setnoise`: its quantities as given, the others as
/// the signature typed them.
pub(super) struct NoiseRequest<'a> {
    pub(super) mode: &'a str,
    pub(super) table: &'a str,
    pub(super) simple_noise: Variant,
    pub(super) ground_pressure: Variant,
    pub(super) altitude: Variant,
    pub(super) water_height: Variant,
    pub(super) water_vapour: Variant,
    /// `tatmos`, `tau`, the efficiencies, the temperatures, `senscoeff` and
    /// `rxtype`.
    pub(super) tsys: TsysParameters,
    pub(super) on_the_fly: bool,
}

/// `sm.setnoise(...)`: add a noise term.
///
/// As CASA's binding does, every quantity argument is read (an unreadable
/// one raises); `simplenoise` is converted to Jy in `simplenoise` mode, and
/// `pwv` to mm in the Tsys modes, which refuse one above 100 mm; `[]` is 0.
/// A `simplenoise` whose unit is not a flux density, or a `pwv` whose unit
/// is not a length, raises, where CASA scales it silently (see
/// [`crate::convert::quantity_value`]); a `pwv` without units is metres, as
/// in CASA. The atmosphere and receiver parameters are `tsys-manual`'s; the
/// ground pressure, humidity, altitude and water scale height are
/// `tsys-atm`'s, a later phase's. With `OTF=True` CASA writes no table for
/// `simplenoise`; for `tsys-manual` it writes the `T` term's, which casa-rs
/// refuses as not yet implemented.
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
        ] {
            quantity(other)?;
        }
        let water_vapour = quantity(&request.water_vapour)?;
        let simple_noise_jy = if request.mode == "simplenoise" {
            value_in(&simple_noise, "Jy")?
        } else {
            0.0
        };
        let water_vapour_mm = if request.mode.starts_with("tsys") {
            value_in(&water_vapour, "mm")?
        } else {
            0.0
        };
        let spec = NoiseSpec {
            mode: request.mode.to_string(),
            simple_noise_jy,
            table: request.table.to_string(),
            water_vapour_mm,
            tsys: request.tsys,
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
