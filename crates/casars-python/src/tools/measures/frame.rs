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

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, PositionRef};
use pyo3::PyResult;

use super::{State, no_measures};
use crate::convert::RecordArg;
use crate::convert::holder::{CasaMeasure, Measure, MeasureKind, MeasureValue, direction_angles};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

/// casacore's `C::degree`.
const DEGREE: f64 = std::f64::consts::PI / 180.0;

/// Reads a measure argument, logging casacore's messages (prefixed to CASA's
/// `context`) when it is not one.
fn read(record: &RecordArg, context: &str, lines: &mut Vec<LogLine>) -> Option<Measure> {
    match Measure::from_record(&record.0) {
        Ok(read) => {
            if let Some(warning) = read.warning {
                lines.push(LogLine::warning(warning));
            }
            Some(read.measure)
        }
        Err(error) => {
            lines.push(LogLine::error(format!("{}{context}", error.message())));
            None
        }
    }
}

/// `me.doframe(v)`: an epoch, position or direction joins the frame
/// (`True`); other measures are not frame elements (`False`).
pub(super) fn doframe(state: &mut State, v: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let Some(measure) = read(v, "Non-measure type v in measure conversion", lines) else {
        return CasaValue::Bool(false);
    };
    let frame = &mut state.frame;
    let slot = match measure.kind() {
        MeasureKind::Epoch => &mut frame.epoch,
        MeasureKind::Position => &mut frame.position,
        MeasureKind::Direction => &mut frame.direction,
        MeasureKind::Frequency => return CasaValue::Bool(false),
    };
    *slot = Some(measure);
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
    state.frame.epoch = Some(Measure::from_casa(&now));
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
    if !off.0.is_empty() {
        lines.push(LogLine::error(
            "measure offsets (off) are not yet implemented in casa-rs",
        ));
        return Ok(CasaValue::empty_record());
    }
    let Some(measure) = read(v, "Non-measure type in measure conversion", lines) else {
        return Ok(CasaValue::empty_record());
    };
    let target = measure.kind().codes().resolve_or_default(rf);
    match convert(state, &measure, target)? {
        Ok(converted) => Ok(converted.to_value()),
        Err(reason) => {
            lines.push(LogLine::warning(format!(
                "Cannot convert due to missing frame information\nCannot convert measure: \
                 {reason}"
            )));
            Ok(CasaValue::empty_record())
        }
    }
}

/// `measure` in the code `target` of its kind: itself when it is already
/// there, else converted in casa-types with the frame. The outer error is the
/// measures data failing to load; the inner one, the conversion failing.
fn convert(
    state: &mut State,
    measure: &Measure,
    target: &'static str,
) -> PyResult<Result<Measure, String>> {
    if measure.refer == target {
        return Ok(Ok(*measure));
    }
    let measures = state.measures().map_err(no_measures)?;
    let converted = (|| {
        let mut frame = MeasFrame::new().with_measures(measures);
        let elements = state.frame;
        for element in [elements.epoch, elements.position, elements.direction]
            .into_iter()
            .flatten()
        {
            frame = match element.to_casa()? {
                CasaMeasure::Epoch(epoch) => frame.with_epoch(epoch),
                CasaMeasure::Position(position) => frame.with_position(position),
                CasaMeasure::Direction(direction) => frame.with_direction(direction),
                CasaMeasure::Frequency(_) => frame,
            };
        }
        let unsupported =
            |kind: &str| format!("the {target} {kind} frame is not yet implemented in casa-rs");
        let error = |error: casa_types::measures::MeasureError| error.to_string();
        Ok::<_, String>(match measure.to_casa()? {
            CasaMeasure::Epoch(epoch) => CasaMeasure::Epoch(
                epoch
                    .convert_to(
                        EpochRef::from_str(target).map_err(|_| unsupported("epoch"))?,
                        &frame,
                    )
                    .map_err(error)?,
            ),
            CasaMeasure::Direction(direction) => CasaMeasure::Direction(
                direction
                    .convert_to(
                        DirectionRef::from_str(target).map_err(|_| unsupported("direction"))?,
                        &frame,
                    )
                    .map_err(error)?,
            ),
            CasaMeasure::Position(position) => CasaMeasure::Position(
                position
                    .convert_to(PositionRef::from_str(target).map_err(|_| unsupported("position"))?)
                    .map_err(error)?,
            ),
            CasaMeasure::Frequency(frequency) => CasaMeasure::Frequency(
                frequency
                    .convert_to(
                        FrequencyRef::from_str(target).map_err(|_| unsupported("frequency"))?,
                        &frame,
                    )
                    .map_err(error)?,
            ),
        })
    })();
    Ok(converted.map(|converted| Measure::from_casa(&converted)))
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
        let context = format!("Non-measure type {name} in measure conversion\n");
        let Some(measure) = read(record, context.trim_end(), lines) else {
            return Ok(zero());
        };
        if measure.kind() != MeasureKind::Direction {
            lines.push(LogLine::error(
                "Trying to convert non-direction to direction",
            ));
            return Ok(zero());
        }
        directions.push(measure);
    }
    let (first, second) = (directions[0], directions[1]);
    let second = match convert(state, &second, first.refer)? {
        Ok(second) => second,
        Err(reason) => {
            lines.push(LogLine::error(format!("Exception Reports: {reason}")));
            return Ok(zero());
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
