// SPDX-License-Identifier: LGPL-3.0-or-later
//! Display the coordinate frame in the casacore stream format.

use casa_core::measures::direction::DirectionRef;
use casa_core::measures::radial_velocity::RadialVelocityRef;
use casa_core::measures::{EpochRef, MeasFrame, MeasureError, MjdHighPrec};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{PyMeasures, State, comet::COMET_UNAVAILABLE, frame};
use crate::convert::holder::{MeasureValue, direction_angles, spatial_spherical};
use crate::convert::value::{CasaValue, stream_double};
use crate::tools::LogLine;

const MISSING_FRAME: &str = "Missing information in Frame specified for conversion";

pub(super) fn epoch_text(mjd: MjdHighPrec) -> String {
    let fraction = mjd.frac();
    let hours = (24.0 * fraction).floor() as u32;
    let minutes = (60.0 * (24.0 * fraction - f64::from(hours))).floor() as u32;
    let seconds = 86_400.0 * fraction - f64::from(minutes) * 60.0 - f64::from(hours) * 3_600.0;
    format!(
        "{}::{hours:02}:{minutes:02}:{seconds:07.4}",
        stream_double(mjd.day())
    )
}

pub(super) fn vector<const N: usize>(values: [f64; N]) -> String {
    format!("[{}]", values.map(stream_double).join(", "))
}

fn radial_error(error: MeasureError) -> String {
    if let MeasureError::MissingFrameData { what } = error {
        let name = what.split_whitespace().next().unwrap_or(what);
        format!("No MeasFrame {name} specified for conversion of Radialvelocity")
    } else {
        error.to_string()
    }
}

fn render(state: &mut State, mut native: MeasFrame) -> Result<String, String> {
    let mut elements = Vec::new();
    if let Some(epoch) = native.epoch() {
        let to = |refer| match epoch.convert_to(refer, &native) {
            Ok(value) => Ok(stream_double(value.value().as_mjd())),
            Err(MeasureError::MissingFrameData { what }) if what.starts_with("dUT1") => {
                Ok("unavailable".to_owned())
            }
            Err(error) => Err(error.to_string()),
        };
        let (tdb, ut1, tt) = (to(EpochRef::TDB)?, to(EpochRef::UT1)?, to(EpochRef::TT)?);
        let Some(MeasureValue::Epoch(raw)) =
            state.frame.epoch.as_ref().map(|epoch| epoch.measure.value)
        else {
            unreachable!("the explicit frame has an epoch");
        };
        elements.push(format!(
            "Epoch: {} (TDB = {tdb}, UT1 = {ut1}, TT = {tt})",
            epoch_text(raw)
        ));
    }
    if let (Some(MeasureValue::Position(xyz)), Some(position)) = (
        state
            .frame
            .position
            .as_ref()
            .map(|position| position.measure.value),
        native.position(),
    ) {
        let (longitude, latitude, _) = spatial_spherical(position.as_itrf());
        elements.push(format!(
            "Position: {}\n        (Longitude = {} Latitude = {})",
            vector(xyz),
            stream_double(longitude),
            stream_double(latitude)
        ));
    }
    if let Some(direction) = native.direction() {
        let converted = frame::convert_direction(state, direction, DirectionRef::J2000, &native)
            .map_err(|error| match error {
                MeasureError::MissingFrameData {
                    what: COMET_UNAVAILABLE,
                } => COMET_UNAVAILABLE.to_owned(),
                MeasureError::MissingFrameData { .. } => MISSING_FRAME.to_owned(),
                error => error.to_string(),
            })?;
        let (longitude, latitude) = direction_angles(converted.cosines());
        let Some(MeasureValue::Direction(raw)) = state
            .frame
            .direction
            .as_ref()
            .map(|direction| direction.measure.value)
        else {
            unreachable!("the explicit frame has a direction");
        };
        elements.push(format!(
            "Direction: {}\n        (J2000 = {} deg)",
            vector(raw),
            vector([longitude.to_degrees(), latitude.to_degrees()])
        ));
        if native.radial_velocity().is_some() {
            native = frame::with_resolved_direction(state, native);
        }
    }
    if let (Some(record), Some(velocity)) = (&state.frame.radial_velocity, native.radial_velocity())
    {
        let MeasureValue::RadialVelocity(raw) = record.measure.value else {
            unreachable!("the frame holds a radial velocity");
        };
        if matches!(
            velocity.refer(),
            RadialVelocityRef::GEO | RadialVelocityRef::TOPO
        ) && native.epoch().is_none()
        {
            return Err(radial_error(MeasureError::MissingFrameData {
                what: "epoch",
            }));
        }
        let lsr = velocity
            .convert_to(RadialVelocityRef::LSRK, &native)
            .map_err(radial_error)?;
        elements.push(format!(
            "Radialvelocity: {}\n        (LSR velocity = {} km/s)",
            stream_double(raw),
            stream_double(lsr.ms() / 1e3)
        ));
    }
    if let Some(comet) = &state.frame.comet {
        let [start, end] = comet.span_mjd().map(stream_double);
        elements.push(format!(
            "{} comet between MJD {start} and {end}",
            comet.name()
        ));
    }
    Ok(format!("Frame: {}", elements.join("\n       ")))
}

#[pymethods]
impl PyMeasures {
    /// Display the explicit frame and its derived coordinates.
    fn showframe(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            if let Some(text) = &state.frame.rendered {
                return Ok(CasaValue::Text(text.clone()));
            }
            let frame = if state.frame.epoch.is_some() {
                frame::native_frame(state)?
            } else {
                frame::materialize(&state.frame, MeasFrame::new())
            };
            frame
                .and_then(|frame| render(state, frame))
                .map(|text| {
                    state.frame.rendered = Some(text.clone());
                    CasaValue::Text(text)
                })
                .map_err(|error| {
                    lines.push(LogLine::error(format!("Exception Reports: {error}")));
                    PyRuntimeError::new_err(error)
                })
        })
    }
}
