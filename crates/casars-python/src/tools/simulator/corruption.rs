// SPDX-License-Identifier: LGPL-3.0-or-later
//! Thermal noise: `setseed`, `setnoise`, `corrupt` and `reset`, converted as
//! casatools' `simulator_cmpt.cc` converts them and handed to the session.
//!
//! Before `open` each returns `False` without converting its arguments, as
//! CASA does. The noise is applied when `close` writes the MeasurementSet;
//! `corrupt` records which scans it covers.

use casa_simulation::{AtmosphereSpec, NoiseSpec, TsysParameters};
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
    pub(super) relative_humidity: f64,
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
/// one raises); `simplenoise` is converted to Jy in `simplenoise` mode,
/// `pwv` to mm in the Tsys modes, which refuse one above 100 mm, and in
/// `tsys-atm` `pground` to mbar and `altitude` and `waterheight` to m; `[]`
/// is 0, which `tsys-atm` takes as unset. A quantity whose unit does not
/// conform raises, where CASA scales it silently (see
/// [`crate::convert::quantity_value`]); one without units is in the SI
/// base, as in CASA (a bare `pwv` is metres, a bare `pground` pascals).
/// `tatmos` and `tau` are `tsys-manual`'s; the efficiencies, temperatures,
/// `senscoeff` and `rxtype` both Tsys modes'; the ground pressure,
/// humidity, altitude and water scale height `tsys-atm`'s. With `OTF=True`
/// CASA writes no table for `simplenoise`; for the Tsys modes it writes the
/// `T` term's, which casa-rs refuses as not yet implemented.
pub(super) fn setnoise(
    tool: &PySimulator,
    py: Python<'_>,
    request: &NoiseRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let simple_noise = quantity(&request.simple_noise)?;
        let ground_pressure = quantity(&request.ground_pressure)?;
        let altitude = quantity(&request.altitude)?;
        let water_height = quantity(&request.water_height)?;
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
        let atmosphere = if request.mode == "tsys-atm" {
            AtmosphereSpec {
                ground_pressure_hpa: value_in(&ground_pressure, "mbar")?,
                relative_humidity_percent: request.relative_humidity,
                altitude_m: value_in(&altitude, "m")?,
                water_scale_height_m: value_in(&water_height, "m")?,
            }
        } else {
            AtmosphereSpec::default()
        };
        let spec = NoiseSpec {
            mode: request.mode.to_string(),
            simple_noise_jy,
            table: request.table.to_string(),
            water_vapour_mm,
            tsys: request.tsys,
            atmosphere,
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
