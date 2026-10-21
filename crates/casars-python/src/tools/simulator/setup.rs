// SPDX-License-Identifier: LGPL-3.0-or-later
//! The setters and `observe`: CASA's arguments converted as casatools'
//! `simulator_cmpt.cc` converts them, then handed to the session.
//!
//! Each method converts its arguments only once a session is open, as CASA
//! does (before `open`, even unreadable arguments give `False`), and in
//! CASA's order, so the first error is CASA's. Python's values are read into
//! Rust ones at the call; turning those into the session's types and the
//! session's work run with the GIL released (`PySimulator::on_session`), and
//! messages are logged once it is back.

use casa_simulation::{ArrayConfig, CoordinateSystem, ObserveSpec, SpectralWindowSpec, StateSpec};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{LOGGER, PySimulator, outcome};
use crate::convert::{ConvertError, Variant, measures, quantity_value};
use crate::tools::{LogLine, emit};

/// `sm.settimes(integrationtime, usehourangle, referencetime)`.
///
/// A reference time CASA cannot convert (`""`) is logged and taken as MJD 0,
/// as in CASA; `[]` is MJD 0 too. The session converts a reference in
/// another time scale to UTC.
pub(super) fn settimes(
    tool: &PySimulator,
    py: Python<'_>,
    integration: &Variant,
    use_hour_angle: bool,
    reference: &Variant,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let reference = match measures::epoch(reference)? {
            Some(epoch) => epoch,
            None => {
                lines.push(LogLine::error(
                    "Could not convert referencetime to an Epoch Measures",
                ));
                MEpoch::from_mjd(0.0, EpochRef::UTC)
            }
        };
        let integration_s = quantity_value(integration, "s")?;
        let result = session
            .simulator
            .set_times(integration_s, use_hour_angle, reference);
        outcome(result, lines)
    })?;
    Ok(result.unwrap_or(false))
}

/// The arguments of `sm.observe`.
pub(super) struct ObserveRequest<'a> {
    pub(super) source: &'a str,
    pub(super) spectral_window: &'a str,
    pub(super) start: Variant,
    pub(super) stop: Variant,
    pub(super) add_observation: bool,
    pub(super) state: StateRequest<'a>,
    pub(super) observer: &'a str,
    pub(super) project: &'a str,
}

/// The `state_*` arguments of `sm.observe`.
pub(super) struct StateRequest<'a> {
    pub(super) sig: bool,
    pub(super) reference: bool,
    pub(super) cal: f64,
    pub(super) load: f64,
    pub(super) sub_scan: i64,
    pub(super) obs_mode: &'a str,
}

/// `sm.observe(...)`: plan a scan. The GIL is released while the session
/// plans it.
pub(super) fn observe(
    tool: &PySimulator,
    py: Python<'_>,
    request: &ObserveRequest<'_>,
) -> PyResult<bool> {
    if !tool.is_open(py) {
        return Ok(false);
    }
    let sub_scan = i32::try_from(request.state.sub_scan).map_err(|_| {
        PyRuntimeError::new_err(format!(
            "state_sub_scan {} does not fit a 32-bit integer",
            request.state.sub_scan
        ))
    })?;
    let spec = ObserveSpec {
        source: request.source.to_string(),
        spectral_window: request.spectral_window.to_string(),
        start_s: quantity_value(&request.start, "s")?,
        stop_s: quantity_value(&request.stop, "s")?,
        add_observation: request.add_observation,
        state: StateSpec {
            sig: request.state.sig,
            reference: request.state.reference,
            cal: request.state.cal,
            load: request.state.load,
            sub_scan,
            obs_mode: request.state.obs_mode.to_string(),
        },
        observer: request.observer.to_string(),
        project: request.project.to_string(),
    };
    let planned = py.detach(|| {
        tool.lock()
            .as_mut()
            .map(|session| session.simulator.observe(&spec))
    });
    let Some(result) = planned else {
        return Ok(false);
    };
    let mut lines = Vec::new();
    let result = outcome(result, &mut lines);
    emit(py, LOGGER, &lines)?;
    result
}

/// `sm.setlimits(shadowlimit, elevationlimit)`.
pub(super) fn setlimits(
    tool: &PySimulator,
    py: Python<'_>,
    shadow_limit: f64,
    elevation_limit: &Variant,
) -> PyResult<bool> {
    let set = tool.on_session(py, |session, _| {
        let elevation_rad = quantity_value(elevation_limit, "rad")?;
        session.simulator.set_limits(shadow_limit, elevation_rad);
        Ok(true)
    })?;
    Ok(set.unwrap_or(false))
}

/// `sm.setauto(autocorrwt)`.
pub(super) fn setauto(tool: &PySimulator, py: Python<'_>, weight: f64) -> PyResult<bool> {
    let set = tool.on_session(py, |session, _| {
        session.simulator.set_auto(weight as f32);
        Ok(true)
    })?;
    Ok(set.unwrap_or(false))
}

/// The arguments of `sm.setconfig`.
pub(super) struct ConfigRequest<'a> {
    pub(super) telescope: &'a str,
    pub(super) x: Vec<f64>,
    pub(super) y: Vec<f64>,
    pub(super) z: Vec<f64>,
    pub(super) dish_diameter: Vec<f64>,
    pub(super) offset: Vec<f64>,
    pub(super) mount: Vec<String>,
    pub(super) antenna_names: Vec<String>,
    pub(super) pad_names: Vec<String>,
    pub(super) coordinate_system: &'a str,
    pub(super) reference_location: Variant,
}

/// `sm.setconfig(...)`: add antennas.
///
/// Divergence: CASA writes zero positions and returns `True` for a
/// `coordsystem` it does not know (or for `longlat`, which it does not
/// implement); casa-rs logs CASA's message and returns `False` (an unknown
/// one) or raises (`longlat`).
pub(super) fn setconfig(
    tool: &PySimulator,
    py: Python<'_>,
    request: ConfigRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let reference = match measures::position(&request.reference_location, &session.measures)? {
            Some(position) => position,
            None => {
                if request.reference_location != Variant::Empty {
                    lines.push(LogLine::error(format!(
                        "Could not convert referencelocation {} to a Position Measures",
                        request.reference_location.describe()
                    )));
                }
                MPosition::new_itrf(0.0, 0.0, 0.0)
            }
        };
        let coordinate_system = match request.coordinate_system {
            "global" => CoordinateSystem::Global,
            "local" => CoordinateSystem::Local { reference },
            "longlat" => CoordinateSystem::LongLat,
            other => {
                lines.push(LogLine::error(format!(
                    "Unknown coordinate system type: {other}"
                )));
                return Ok(false);
            }
        };
        let config = ArrayConfig {
            telescope: request.telescope.to_string(),
            x: request.x,
            y: request.y,
            z: request.z,
            dish_diameter: request.dish_diameter,
            offset: request.offset,
            mount: request.mount,
            antenna_names: request.antenna_names,
            pad_names: request.pad_names,
            coordinate_system,
        };
        outcome(session.simulator.set_config(&config), lines)
    })?;
    match result {
        Some(set) => Ok(set),
        None => {
            emit(
                py,
                LOGGER,
                &[LogLine::error("The sm tool must be opened first.")],
            )?;
            Ok(false)
        }
    }
}

/// `sm.setfeed(mode, x, y, pol)`.
pub(super) fn setfeed(
    tool: &PySimulator,
    py: Python<'_>,
    mode: &str,
    x: &[f64],
    y: &[f64],
    pol: &[String],
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        outcome(session.simulator.set_feed(mode, x, y, pol), lines)
    })?;
    Ok(result.unwrap_or(false))
}

/// `sm.setfield(sourcename, sourcedirection, calcode, distance)`. A direction
/// CASA cannot convert is logged and taken as the J2000 pole, as in CASA.
pub(super) fn setfield(
    tool: &PySimulator,
    py: Python<'_>,
    name: &str,
    direction: &Variant,
    calcode: &str,
    distance: &Variant,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let direction = match measures::direction(direction, &session.measures)? {
            Some(direction) => direction,
            None => {
                lines.push(LogLine::error(
                    "Could not convert source direction to a Direction Measure.",
                ));
                MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, DirectionRef::J2000)
            }
        };
        let distance_m = quantity_value(distance, "m")?;
        outcome(
            session
                .simulator
                .set_field(name, &direction, calcode, distance_m),
            lines,
        )
    })?;
    Ok(result.unwrap_or(false))
}

/// The arguments of `sm.setspwindow`.
pub(super) struct WindowRequest<'a> {
    pub(super) name: &'a str,
    pub(super) frequency: Variant,
    pub(super) width: Variant,
    pub(super) resolution: Variant,
    pub(super) frame: &'a str,
    pub(super) channels: i64,
    pub(super) stokes: &'a str,
}

/// `sm.setspwindow(...)`. Divergence: a negative `nchannels`, which CASA
/// passes on to array allocation, raises.
pub(super) fn setspwindow(
    tool: &PySimulator,
    py: Python<'_>,
    request: &WindowRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let start_frequency_hz = quantity_value(&request.frequency, "Hz")?;
        let channel_width_hz = quantity_value(&request.width, "Hz")?;
        let resolution_hz = quantity_value(&request.resolution, "Hz")?;
        let frame = measures::frequency_reference(request.frame)?;
        let channel_count = usize::try_from(request.channels).map_err(|_| {
            ConvertError::runtime(format!(
                "nchannels must not be negative, got {}",
                request.channels
            ))
        })?;
        let spec = SpectralWindowSpec {
            name: request.name.to_string(),
            start_frequency_hz,
            channel_width_hz,
            resolution_hz,
            frame,
            channel_count,
            stokes: request.stokes.to_string(),
        };
        outcome(session.simulator.set_spectral_window(&spec), lines)
    })?;
    Ok(result.unwrap_or(false))
}
