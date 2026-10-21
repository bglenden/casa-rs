// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prediction: `predict`, `setoptions` and `setvp`, converted as casatools'
//! `simulator_cmpt.cc` converts them and handed to the session.
//!
//! `setoptions` and `setvp` only record, and before `open` return `False`
//! without converting their arguments, as CASA does. `predict` opens every
//! named model image first, even before `open` (CASA's binding does, through
//! `ImageFactory::fromFile`), so a missing image raises there; then it
//! reads and prepares the model with the GIL released, and `close` writes
//! the prediction.

use std::path::PathBuf;

use casa_simulation::{PredictSpec, PredictionOptions, VoltagePatternSpec};
use pyo3::prelude::*;

use super::{LOGGER, PySimulator, outcome, raised};
use crate::convert::{ConvertResult, Variant, measures, quantity, value_in};
use crate::tools::{LogLine, emit};

/// `sm.predict(imagename, complist, incremental)`: predict the model into
/// the rows observed so far.
pub(super) fn predict(
    tool: &PySimulator,
    py: Python<'_>,
    images: Vec<String>,
    component_list: &str,
    incremental: bool,
) -> PyResult<bool> {
    let spec = PredictSpec {
        images: images.into_iter().map(PathBuf::from).collect(),
        component_list: (!component_list.is_empty()).then(|| PathBuf::from(component_list)),
        incremental,
    };
    py.detach(|| spec.check_images())
        .map_err(|error| raised(&error))?;
    let predicted = py.detach(|| {
        tool.lock()
            .as_mut()
            .map(|session| session.simulator.predict(&spec))
    });
    let Some(result) = predicted else {
        return Ok(false);
    };
    let mut lines = Vec::new();
    let result = outcome(result, &mut lines);
    emit(py, LOGGER, &lines)?;
    result
}

/// The arguments of `sm.setoptions`.
pub(super) struct OptionsRequest<'a> {
    pub(super) ftmachine: &'a str,
    pub(super) grid_function: &'a str,
    pub(super) location: Variant,
    pub(super) padding: f64,
}

/// `sm.setoptions(...)`: record the padding `predict` grids with.
///
/// `location` is converted as CASA's binding converts it: one that does not
/// convert is logged and the call goes on. `cache`, `tile`, `facets`,
/// `maxdata` and `wprojplanes`, which do not change what `GridFT` predicts,
/// are accepted as typed.
pub(super) fn setoptions(
    tool: &PySimulator,
    py: Python<'_>,
    request: &OptionsRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        if measures::position(&request.location, &session.measures)?.is_none()
            && request.location != Variant::Empty
        {
            lines.push(LogLine::error(format!(
                "Could not convert location {} to a Position Measures",
                request.location.describe()
            )));
        }
        let options = PredictionOptions {
            ftmachine: request.ftmachine.to_string(),
            grid_function: request.grid_function.to_string(),
            padding: request.padding,
        };
        outcome(session.simulator.set_options(&options), lines)
    })?;
    Ok(result.unwrap_or(false))
}

/// The arguments of `sm.setvp`.
pub(super) struct VoltagePatternRequest<'a> {
    pub(super) dovp: bool,
    pub(super) use_default_vp: bool,
    pub(super) vp_table: &'a str,
    pub(super) do_squint: bool,
    pub(super) parallactic_angle_increment: Variant,
    pub(super) sky_position_threshold: Variant,
}

/// A `setvp` angle in degrees: CASA's binding keeps `default_deg` for `''`
/// and `[]`, and converts anything else (raising when it cannot).
fn angle_deg(value: &Variant, default_deg: f64) -> ConvertResult<f64> {
    match value {
        Variant::Empty => Ok(default_deg),
        Variant::Text(text) if text.is_empty() => Ok(default_deg),
        other => value_in(&quantity(other)?, "deg"),
    }
}

/// `sm.setvp(...)`: record whether `predict` applies the primary beam.
/// `pblimit`, which `GridFT` does not use, is accepted as typed.
pub(super) fn setvp(
    tool: &PySimulator,
    py: Python<'_>,
    request: &VoltagePatternRequest<'_>,
) -> PyResult<bool> {
    let result = tool.on_session(py, |session, lines| {
        let parallactic_angle_increment_deg =
            angle_deg(&request.parallactic_angle_increment, 360.0)?;
        angle_deg(&request.sky_position_threshold, 180.0)?;
        let spec = VoltagePatternSpec {
            dovp: request.dovp,
            use_default_vp: request.use_default_vp,
            vp_table: request.vp_table.to_string(),
            do_squint: request.do_squint,
            parallactic_angle_increment_deg,
        };
        outcome(session.simulator.set_vp(&spec), lines)
    })?;
    Ok(result.unwrap_or(false))
}
