// SPDX-License-Identifier: LGPL-3.0-or-later
//! Questions about a measure record: `me.getref`, `me.gettype`,
//! `me.getvalue`, `me.ismeasure`, `me.show`, `me.dirshow` and
//! `me.listcodes`, as casatools' `measures_cmpt.cc` answers them.
//!
//! `show` writes the measure's value as casacore's `operator<<` does (six
//! significant digits): a direction's longitude and latitude in radians, an
//! epoch's MJD, a position's length, longitude and latitude, a frequency in
//! Hz; then, with `refcode`, its reference code.

use crate::convert::RecordArg;
use crate::convert::holder::{Measure, MeasureValue, direction_angles, position_spherical};
use crate::convert::value::{CasaValue, stream_double};
use crate::tools::LogLine;

/// casacore's `C::degree`.
const DEGREE: f64 = std::f64::consts::PI / 180.0;

/// The measure in `v`, or `None` after logging casacore's message and CASA's
/// `context` at `level`.
fn read(v: &RecordArg, context: &str, warning: bool, lines: &mut Vec<LogLine>) -> Option<Measure> {
    match Measure::from_record(&v.0) {
        Ok(read) => {
            if let Some(message) = read.warning {
                lines.push(LogLine::warning(message));
            }
            Some(read.measure)
        }
        Err(error) => {
            let message = format!("{}{context}", error.message());
            lines.push(if warning {
                LogLine::warning(message)
            } else {
                LogLine::error(message)
            });
            None
        }
    }
}

/// `me.getref(v)`: the reference code, or `''`.
pub(super) fn getref(v: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let refer = read(v, "Incorrect input type for getref()", true, lines)
        .map_or("", |measure| measure.refer);
    CasaValue::Text(refer.to_string())
}

/// `me.gettype(v)`: the kind, as casacore names it (`'Epoch'`), or `''`.
pub(super) fn gettype(v: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let kind = read(v, "Incorrect input type for gettype()", true, lines)
        .map_or("", |measure| measure.kind().tell_me());
    CasaValue::Text(kind.to_string())
}

/// `me.getvalue(v)`: the record (or its `return` field) without `type` and
/// `refer`, as given; `{}` for a record that is not a measure.
pub(super) fn getvalue(v: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let record = match v.0.get("return") {
        Some(crate::convert::Variant::Record(inner)) => RecordArg(inner.clone()),
        _ => v.clone(),
    };
    if read(&record, "Incorrect input type for getvalue()", true, lines).is_none() {
        return CasaValue::empty_record();
    }
    CasaValue::record(
        record
            .0
            .iter()
            .filter(|(key, _)| key.as_str() != "type" && key.as_str() != "refer")
            .map(|(key, value)| (key.clone(), CasaValue::from_variant(value))),
    )
}

/// `me.ismeasure(v)`.
pub(super) fn ismeasure(v: &RecordArg) -> CasaValue {
    CasaValue::Bool(Measure::from_record(&v.0).is_ok())
}

/// `me.show(v, refcode)`.
pub(super) fn show(v: &RecordArg, refcode: bool, lines: &mut Vec<LogLine>) -> CasaValue {
    let Some(measure) = read(v, "Non-measure input", true, lines) else {
        return CasaValue::Text(String::new());
    };
    let vector = |values: &[f64]| {
        format!(
            "[{}]",
            values
                .iter()
                .map(|value| stream_double(*value))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let mut text = match measure.value {
        MeasureValue::Direction(cosines) => {
            let (longitude, latitude) = direction_angles(cosines);
            vector(&[longitude, latitude])
        }
        MeasureValue::Epoch(mjd) => stream_double(mjd.as_mjd()),
        MeasureValue::Frequency(hz) => format!("{} Hz", stream_double(hz)),
        MeasureValue::Position(xyz) => {
            let (longitude, latitude, length) = position_spherical(xyz);
            vector(&[length, longitude, latitude])
        }
    };
    if refcode {
        text.push(' ');
        text.push_str(measure.refer);
    }
    CasaValue::Text(text)
}

/// `me.dirshow(v)`: a direction's angles in degrees and its code.
///
/// Not yet implemented in casa-rs for other measures, which CASA writes
/// with casacore's `Measure` output operator.
pub(super) fn dirshow(v: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let measure = match Measure::from_record(&v.0) {
        Ok(read) => read.measure,
        Err(error) => {
            lines.push(LogLine::error(format!(
                "Error [{}] in converting record to measure",
                error.message()
            )));
            return CasaValue::Text(String::new());
        }
    };
    let MeasureValue::Direction(cosines) = measure.value else {
        lines.push(LogLine::warning(format!(
            "dirshow of {} measures is not yet implemented in casa-rs",
            measure.kind().record_type()
        )));
        return CasaValue::Text(String::new());
    };
    let (longitude, latitude) = direction_angles(cosines);
    let degrees = |radians: f64| stream_double(radians * (1.0 / DEGREE));
    CasaValue::Text(format!(
        "[{}, {}] deg  {}",
        degrees(longitude),
        degrees(latitude),
        measure.refer
    ))
}

/// `me.listcodes(ms)`: the kind's normal and extra (planet) codes.
pub(super) fn listcodes(ms: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let Some(measure) = read(
        ms,
        "Non-measure type ms in measure conversion",
        false,
        lines,
    ) else {
        return CasaValue::empty_record();
    };
    let codes = measure.kind().codes();
    let split = codes.all.len() - codes.extra;
    let texts =
        |codes: &[&str]| CasaValue::Texts(codes.iter().map(|code| (*code).to_string()).collect());
    CasaValue::record([
        ("normal", texts(&codes.all[..split])),
        ("extra", texts(&codes.all[split..])),
    ])
}
