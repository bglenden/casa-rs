// SPDX-License-Identifier: LGPL-3.0-or-later
//! The frame and conversions: `me.doframe`, `me.framenow`, `me.measure`,
//! `me.separation`, `me.posangle` and `me.done`.
//!
//! A conversion runs in casa-types with a frame of the epoch, position and
//! direction `doframe` set and the measures data. Divergence: casatools'
//! tool starts with a frame at MJD 0 at the geocentre, so a conversion that
//! needs a frame element converts with those values when `doframe` has not
//! set it; here the conversion fails, with CASA's "Cannot convert due to
//! missing frame information" (as CASA's does after `me.done()`).

use std::str::FromStr;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, PositionRef};
use pyo3::PyResult;

use super::{FrameValue, State, no_measures, record};
use crate::convert::RecordArg;
use crate::convert::holder::{
    CasaMeasure, Measure, MeasureKind, MeasureValue, direction_angles, epoch_ref,
};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

/// casacore's `C::degree`.
const DEGREE: f64 = std::f64::consts::PI / 180.0;

/// Set a frame element from the first value in the supplied measure record.
pub(super) fn doframe(state: &mut State, v: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let mut values = match record::Measures::read(v) {
        Ok(values) => values,
        Err(error) => {
            lines.push(LogLine::error(format!(
                "{}Non-measure type v in measure conversion",
                error.message()
            )));
            return CasaValue::Bool(false);
        }
    };
    if let Some(warning) = values.warning.take() {
        lines.push(LogLine::warning(warning));
    }
    let frame = &mut state.frame;
    let measure = values.values[0];
    let slot = match measure.kind() {
        MeasureKind::Epoch => &mut frame.epoch,
        MeasureKind::Position => &mut frame.position,
        MeasureKind::Direction => &mut frame.direction,
        MeasureKind::RadialVelocity => {
            frame.radial_velocity = Some(values.into_reference_origin());
            frame.rendered = None;
            return CasaValue::Bool(true);
        }
        MeasureKind::Frequency
        | MeasureKind::Doppler
        | MeasureKind::Baseline
        | MeasureKind::Uvw
        | MeasureKind::EarthMagnetic => return CasaValue::Bool(false),
    };
    *slot = Some(FrameValue {
        measure,
        origin: values.offset,
    });
    frame.rendered = None;
    CasaValue::Bool(true)
}

/// `me.framenow()`: the frame's epoch is the system clock's UTC.
pub(super) fn framenow(state: &mut State) -> CasaValue {
    /// MJD of the Unix epoch, 1970-01-01.
    const UNIX_EPOCH_MJD: f64 = 40_587.0;
    let since_unix_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64());
    let now = CasaMeasure::Epoch(MEpoch::from_mjd(
        UNIX_EPOCH_MJD + since_unix_s / 86_400.0,
        EpochRef::UTC,
    ));
    state.frame.epoch = Some(Measure::from_casa(&now).into());
    state.frame.rendered = None;
    CasaValue::Bool(true)
}

/// `me.done()`: the frame is emptied.
pub(super) fn done(state: &mut State) -> CasaValue {
    state.frame = super::Frame::default();
    CasaValue::Bool(true)
}

/// `me.measure(v, rf, off)`: `v` converted to the code `rf` names (its
/// kind's default if it names none).
pub(super) fn measure(
    state: &mut State,
    v: &RecordArg,
    rf: &str,
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let measure = match super::record::Measures::read(v) {
        Ok(measure) => measure,
        Err(error) => {
            lines.push(LogLine::error(format!(
                "{}Non-measure type in measure conversion",
                error.message()
            )));
            return Ok(CasaValue::empty_record());
        }
    };
    if let Some(warning) = &measure.warning {
        lines.push(LogLine::warning(warning.clone()));
    }
    let origin = if off.0.is_empty() {
        None
    } else {
        match super::record::Measures::read(off) {
            Ok(origin) => Some(origin),
            Err(error) => {
                if off.0.contains_key("offset") {
                    lines.push(LogLine::warning(format!(
                        "{}Non-measure type offset in measure conversion\n",
                        error.message()
                    )));
                }
                None
            }
        }
    };
    let kind = measure.values[0].kind();
    let target = kind
        .resolve_code(rf)
        .unwrap_or_else(|| kind.codes().default_code());
    match measure.resolve_relative(state, target, origin)? {
        Ok(converted) => Ok(converted.to_value()),
        Err(reason) if reason == "Non-conforming offset measure type" => {
            lines.push(LogLine::warning(format!(
                "{reason}\nCannot convert measure"
            )));
            Ok(CasaValue::empty_record())
        }
        Err(reason) => {
            lines.push(LogLine::warning(format!(
                "Cannot convert due to missing frame information\nCannot convert measure: {reason}"
            )));
            Ok(CasaValue::empty_record())
        }
    }
}

/// `measure` in the code `target` of its kind: itself when it is already
/// there, else converted in casa-types with the frame. The outer error is the
/// measures data failing to load; the inner one, the conversion failing.
pub(super) fn convert(
    state: &mut State,
    measure: &Measure,
    target: &'static str,
) -> PyResult<Result<Measure, String>> {
    if measure.refer == target && !target.starts_with("R_") {
        return Ok(Ok(*measure));
    }
    if let MeasureValue::Epoch(value) = measure.value
        && let (Ok((source, raze)), Ok((destination, label_razes))) =
            (epoch_ref(measure.refer), epoch_ref(target))
        && source == destination
    {
        let value = if raze {
            casa_types::measures::MjdHighPrec::new(value.day(), 0.0)
        } else {
            value
        };
        return Ok(Ok(Measure::from_casa(&CasaMeasure::Epoch(
            MEpoch::new(value, destination).with_razing(label_razes),
        ))));
    }
    if matches!(
        measure.value,
        MeasureValue::Baseline(_) | MeasureValue::Uvw(_) | MeasureValue::EarthMagnetic(_)
    ) {
        return super::geometry::convert(state, measure, target);
    }
    if let MeasureValue::Doppler(value) = measure.value {
        let converted = casa_types::measures::doppler::MDoppler::new(
            value,
            crate::convert::holder::doppler_ref(measure.refer)
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)?,
        )
        .convert_to(
            crate::convert::holder::doppler_ref(target)
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)?,
            &MeasFrame::new(),
        );
        return Ok(converted
            .map(|value| Measure {
                refer: target,
                value: MeasureValue::Doppler(value.value()),
            })
            .map_err(|error| error.to_string()));
    }
    let frame = match native_frame(state)? {
        Ok(frame) => frame,
        Err(error) => return Ok(Err(error)),
    };
    let frame = if matches!(
        measure.value,
        MeasureValue::Frequency(_) | MeasureValue::RadialVelocity(_)
    ) {
        with_comet_direction(state, frame)
    } else {
        frame
    };
    Ok(convert_native(
        *measure,
        target,
        &frame,
        |direction, refer| convert_direction(state, direction, refer, &frame),
    ))
}

fn convert_native(
    measure: Measure,
    target: &'static str,
    frame: &MeasFrame,
    convert_direction_callback: impl FnOnce(
        &MDirection,
        DirectionRef,
    )
        -> Result<MDirection, casa_types::measures::MeasureError>,
) -> Result<Measure, String> {
    if measure.refer == target && !target.starts_with("R_") {
        return Ok(measure);
    }
    let unsupported =
        |kind: &str| format!("the {target} {kind} frame is not yet implemented in casa-rs");
    let error = |error: casa_types::measures::MeasureError| error.to_string();
    let converted = match measure.to_casa()? {
        CasaMeasure::Epoch(epoch) => {
            let (reference, raze) = epoch_ref(target).map_err(|_| unsupported("epoch"))?;
            CasaMeasure::Epoch(
                epoch
                    .convert_to(reference, frame)
                    .map_err(error)?
                    .with_razing(raze),
            )
        }
        CasaMeasure::Direction(direction) => CasaMeasure::Direction(
            convert_direction_callback(
                &direction,
                DirectionRef::from_str(target).map_err(|_| unsupported("direction"))?,
            )
            .map_err(error)?,
        ),
        CasaMeasure::Position(position) => CasaMeasure::Position(
            position
                .convert_to(PositionRef::from_str(target).map_err(|_| unsupported("position"))?)
                .map_err(error)?,
        ),
        CasaMeasure::Doppler(value) => CasaMeasure::Doppler(
            value
                .convert_to(crate::convert::holder::doppler_ref(target)?, frame)
                .map_err(error)?,
        ),
        CasaMeasure::RadialVelocity(value) => CasaMeasure::RadialVelocity(
            value
                .convert_to(
                    target.parse().map_err(|_| unsupported("radialvelocity"))?,
                    frame,
                )
                .map_err(error)?,
        ),
        CasaMeasure::Frequency(frequency) => CasaMeasure::Frequency(
            frequency
                .convert_to(
                    FrequencyRef::from_str(target).map_err(|_| unsupported("frequency"))?,
                    frame,
                )
                .map_err(error)?,
        ),
    };
    Ok(Measure::from_casa(&converted))
}

pub(super) fn with_comet_direction(state: &State, frame: MeasFrame) -> MeasFrame {
    if frame
        .direction()
        .is_some_and(|dir| dir.refer() == DirectionRef::COMET)
    {
        // Resolve the active sky position and its range. If it
        // cannot be resolved, the actual conversion decides whether it needs
        // a direction, just as it would with any other incomplete frame.
        if let Ok((direction, distance)) = super::comet::resolve_direction(state, &frame) {
            return frame.with_ephemeris_direction(direction, distance);
        }
    }
    frame
}

pub(super) fn convert_direction(
    state: &mut State,
    direction: &MDirection,
    target: DirectionRef,
    frame: &MeasFrame,
) -> Result<MDirection, casa_types::measures::MeasureError> {
    if direction.refer() == target {
        return direction.convert_to(target, frame);
    }
    let (resolved, distance_m) = resolve_direction(state, direction, frame)?;
    resolved.convert_with_parallax(target, frame, distance_m)
}

pub(super) fn resolve_direction(
    state: &mut State,
    direction: &MDirection,
    frame: &MeasFrame,
) -> Result<(MDirection, f64), casa_types::measures::MeasureError> {
    use casa_measures_data::PlanetaryTable;
    use casa_types::measures::MeasureError;

    let refer = direction.refer();
    if refer == DirectionRef::COMET {
        return super::comet::resolve_direction(state, frame);
    }
    if refer.casacore_code() < 32 {
        return Ok((direction.clone(), 0.0));
    }
    if frame.epoch().is_none() {
        return Err(MeasureError::MissingFrameData { what: "epoch" });
    }
    let model_error = |error: casa_measures_data::MeasuresDataError| MeasureError::ModelError {
        model: "DE200",
        reason: error.to_string(),
    };
    if state.planets.is_none() {
        state.planets = Some(PlanetaryTable::open_discovered().map_err(model_error)?);
    }
    state
        .planets
        .as_ref()
        .expect("the planetary table has been opened")
        .geocentric_direction(direction, frame)
        .map_err(model_error)
}

/// Build the explicit native frame once for a conversion or vector plan.
pub(super) fn native_frame(state: &mut State) -> PyResult<Result<MeasFrame, String>> {
    let measures = state.measures().map_err(no_measures)?;
    Ok(materialize(
        &state.frame,
        MeasFrame::new().with_measures(measures),
    ))
}

pub(super) fn materialize(
    elements: &super::Frame,
    mut frame: MeasFrame,
) -> Result<MeasFrame, String> {
    let elements_in_order = [
        elements.position.as_ref(),
        elements.epoch.as_ref(),
        elements.direction.as_ref(),
    ];
    for element in elements_in_order.into_iter().flatten() {
        frame = install_frame_value(frame, element.measure)?;
    }
    for element in elements_in_order
        .into_iter()
        .flatten()
        .filter(|element| element.origin.is_some())
    {
        let mut convert = |measure, target| {
            convert_native(measure, target, &frame, |dir, refer| {
                dir.convert_to(refer, &frame)
            })
        };
        let absolute =
            record::absolute_value(element.measure, element.origin.as_deref(), &mut convert)?;
        frame = install_frame_value(frame, absolute)?;
    }
    if let Some(velocity) = &elements.radial_velocity {
        let value = velocity.radial_velocity(&frame)?;
        frame = frame.with_radial_velocity(value);
    }
    Ok(frame)
}

fn install_frame_value(frame: MeasFrame, measure: Measure) -> Result<MeasFrame, String> {
    Ok(match measure.to_casa()? {
        CasaMeasure::Epoch(epoch) => frame.with_epoch(epoch),
        CasaMeasure::Position(position) => frame.with_position(position),
        CasaMeasure::Direction(direction) => frame.with_direction(direction),
        _ => unreachable!("only epochs, positions and directions use this path"),
    })
}

/// What `me.separation` and `me.posangle` compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Between {
    /// The angle between two directions.
    Separation,
    /// The position angle of the second from the first, north through east.
    PositionAngle,
}

/// `me.separation(m1, m2)` and `me.posangle(m1, m2)`, in degrees, with `m2`
/// converted to `m1`'s code when they differ. A failure is logged and gives
/// casatools' `{'unit': 'deg', 'value': array([0.])}`.
pub(super) fn between(
    state: &mut State,
    m1: &RecordArg,
    m2: &RecordArg,
    what: Between,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let zero = || {
        CasaValue::record([
            ("unit", CasaValue::Text("deg".to_string())),
            ("value", CasaValue::floats(vec![0.0])),
        ])
    };
    let mut directions = Vec::with_capacity(2);
    for (name, record) in [("m1", m1), ("m2", m2)] {
        let mut input = match record::Measures::read(record) {
            Ok(input) => input.into_reference_origin(),
            Err(error) => {
                lines.push(LogLine::error(format!(
                    "{}Non-measure type {name} in measure conversion",
                    error.message()
                )));
                return Ok(zero());
            }
        };
        if let Some(warning) = input.warning.take() {
            lines.push(LogLine::warning(warning));
        }
        if input.values[0].kind() != MeasureKind::Direction {
            lines.push(LogLine::error(
                "Trying to convert non-direction to direction",
            ));
            return Ok(zero());
        }
        if DirectionRef::from_str(input.values[0].refer)
            .is_ok_and(|refer| refer.casacore_code() >= 32)
        {
            input = match input.resolve(state, "J2000")? {
                Ok(value) => value,
                Err(reason) => {
                    lines.push(LogLine::error(format!("Exception Reports: {reason}")));
                    return Ok(zero());
                }
            };
        }
        directions.push(input);
    }
    let first = directions[0].values[0];
    let second = if first.refer == directions[1].values[0].refer {
        directions[1].values[0]
    } else {
        match directions[1].resolve(state, first.refer)? {
            Ok(second) => second.values[0],
            Err(reason) => {
                lines.push(LogLine::error(format!("Exception Reports: {reason}")));
                return Ok(zero());
            }
        }
    };
    let (MeasureValue::Direction(a), MeasureValue::Direction(b)) = (first.value, second.value)
    else {
        unreachable!("both are directions");
    };
    let radians = match what {
        Between::Separation => separation(a, b),
        Between::PositionAngle => position_angle(a, b),
    };
    Ok(CasaValue::quantity(radians * (1.0 / DEGREE), "deg"))
}

/// `MVDirection::separation`.
fn separation(a: [f64; 3], b: [f64; 3]) -> f64 {
    let half = ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2) + (a[2] - b[2]).powi(2)).sqrt() / 2.0;
    2.0 * half.min(1.0).asin()
}

/// `MVDirection::positionAngle`.
fn position_angle(a: [f64; 3], b: [f64; 3]) -> f64 {
    let longitude_difference = direction_angles(a).0 - direction_angles(b).0;
    let (sin_lat1, sin_lat2) = (a[2], b[2]);
    let cos_lat2 = (1.0 - sin_lat2 * sin_lat2).abs().sqrt();
    let s1 = -cos_lat2 * longitude_difference.sin();
    let c1 = (1.0 - sin_lat1 * sin_lat1).abs().sqrt() * sin_lat2
        - sin_lat1 * cos_lat2 * longitude_difference.cos();
    if s1 != 0.0 || c1 != 0.0 {
        s1.atan2(c1)
    } else {
        0.0
    }
}
