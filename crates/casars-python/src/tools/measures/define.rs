// SPDX-License-Identifier: LGPL-3.0-or-later
//! The measures a script defines: `me.epoch`, `me.direction`,
//! `me.position`, `me.frequency`, `me.observatory` and `me.source`, as
//! casatools' `measures_cmpt.cc` builds them.
//!
//! The values are read by the tool's own `casaQuantityFromVar`, not the
//! `casaQuantity` other tools use: it reads strings and quantity records,
//! and takes anything else, numbers included, as 0 with no unit. When
//! every value is that 0 the method substitutes its default (MJD 0, the
//! J2000 pole, the WGS84 pole at height 0, 0 Hz), so `me.epoch('UTC',
//! 50000)` is MJD 0, as in CASA.

use std::sync::Arc;

use casa_types::measures::{MPosition, MeasuresProvider, MjdHighPrec};
use casa_types::quanta::{Dimension, Quantity, UnitDim};

use crate::convert::holder::{
    DIRECTION_CODES, EPOCH_CODES, FREQUENCY_CODES, Measure, MeasureKind, MeasureValue,
    POSITION_CODES, days_of, direction_cosines, frequency_hz,
};
use crate::convert::measures::direction_measure;
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant, quantity_record};
use crate::tools::LogLine;

/// The empty record CASA returns after logging a failure.
fn failed() -> CasaValue {
    CasaValue::empty_record()
}

/// casatools' `measures::casaQuantityFromVar`: `None` after logging CASA's
/// messages for a string or record it cannot read.
fn quantity_of(value: &Variant, lines: &mut Vec<LogLine>) -> Option<Quantity> {
    let fail = |lines: &mut Vec<LogLine>, first: String, holder: &str| {
        lines.push(LogLine::error(first));
        lines.push(LogLine::error(format!("Exception Reported: {holder}")));
        lines.push(LogLine::error(format!("Exception Reports: {holder}")));
        None
    };
    const EMPTY: &str = "Empty QuantumHolder argument for asQuantumDouble";
    match value {
        Variant::Text(text) => match Quantity::read(text) {
            Ok(quantity) => Some(quantity),
            Err(_) => fail(lines, read_error(text), EMPTY),
        },
        Variant::Texts(texts) => {
            let text = format!(
                "[{}]",
                texts
                    .iter()
                    .map(|text| format!("\"{text}\""))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            fail(lines, read_error(&text), EMPTY)
        }
        Variant::Record(record) => match quantity_record(record) {
            Ok(quantity) => Some(quantity),
            Err(error) if error.starts_with("Wrong") => {
                lines.push(LogLine::error(format!("Exception Reported: {error}")));
                lines.push(LogLine::error(format!("Exception Reports: {error}")));
                None
            }
            Err(error) => fail(
                lines,
                format!("Error {error} in converting quantity from record "),
                EMPTY,
            ),
        },
        _ => Some(Quantity::dimensionless(0.0)),
    }
}

fn read_error(text: &str) -> String {
    format!(
        "Error in QuantumHolder::fromString with input string \"{text}\": Illegal input units or \
         format\n in converting quantity from string "
    )
}

/// Whether `quantity` is the 0 with no unit of an unreadable kind of value.
fn unset(quantity: &Quantity) -> bool {
    quantity.value() == 0.0 && quantity.unit().name().is_empty()
}

fn has(quantity: &Quantity, dimension: Dimension) -> bool {
    quantity.unit().val().dim == UnitDim::basis(dimension)
}

/// Logs CASA's message for a method's caught exception.
fn exception(lines: &mut Vec<LogLine>, message: impl std::fmt::Display) -> CasaValue {
    lines.push(LogLine::error(format!("Exception Reports: {message}")));
    failed()
}

/// casa-rs does not read measure offsets yet.
fn offset_refused(off: &RecordArg, lines: &mut Vec<LogLine>) -> bool {
    if off.0.is_empty() {
        return false;
    }
    lines.push(LogLine::error(
        "measure offsets (off) are not yet implemented in casa-rs",
    ));
    true
}

/// `me.epoch(rf, v0, off)`. An unknown `rf` is UTC, silently, as in CASA.
pub(super) fn epoch(
    rf: &str,
    v0: &Variant,
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    let Some(mut time) = quantity_of(v0, lines) else {
        return failed();
    };
    if unset(&time) {
        time = Quantity::new(0.0, "d").expect("d is a unit");
    }
    if !has(&time, Dimension::Time) {
        return exception(
            lines,
            format!(
                "Quantum::assure non-conforming unit type '{}'",
                time.unit().name()
            ),
        );
    }
    if offset_refused(off, lines) {
        return failed();
    }
    Measure {
        refer: EPOCH_CODES.resolve_or_default(rf),
        value: MeasureValue::Epoch(MjdHighPrec::from_mjd(days_of(&time))),
    }
    .to_value()
}

/// `me.direction(rf, v0, v1, off)`: a longitude and a latitude. An unknown
/// `rf` is J2000, with CASA's warning.
pub(super) fn direction(
    rf: &str,
    v0: &Variant,
    v1: &Variant,
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    let Some(mut longitude) = quantity_of(v0, lines) else {
        return failed();
    };
    let Some(mut latitude) = quantity_of(v1, lines) else {
        return failed();
    };
    if unset(&longitude) && unset(&latitude) {
        longitude = Quantity::new(0.0, "deg").expect("deg is a unit");
        latitude = Quantity::new(90.0, "deg").expect("deg is a unit");
    }
    // casacore's MVDirection takes cos(latitude) first.
    for angle in [&latitude, &longitude] {
        if !has(angle, Dimension::Angle) {
            return exception(
                lines,
                format!("Quantum::cos illegal unit type '{}'", angle.unit().name()),
            );
        }
    }
    let refer = DIRECTION_CODES.resolve(rf).unwrap_or_else(|| {
        lines.push(LogLine::warning(
            "Illegal reference frame string.  Reference string set to DEFAULT",
        ));
        DIRECTION_CODES.default_code()
    });
    if offset_refused(off, lines) {
        return failed();
    }
    Measure {
        refer,
        value: MeasureValue::Direction(direction_cosines(
            longitude.get_si_value(),
            latitude.get_si_value(),
        )),
    }
    .to_value()
}

/// `me.position(rf, v0, v1, v2, off)`: x, y, z lengths, or a longitude, a
/// latitude and a height (WGS84) or radius (ITRF). An unknown `rf` is ITRF,
/// with casacore's warning.
///
/// As in CASA, values whose second is an angle are reordered to length,
/// longitude, latitude before casacore reads them, and values casacore
/// cannot read leave the position at the origin.
pub(super) fn position(
    rf: &str,
    values: [&Variant; 3],
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    if let Variant::Record(record) = values[0]
        && matches!(
            record.get("value"),
            Some(Variant::Numbers(_) | Variant::Array { .. })
        )
    {
        lines.push(LogLine::error(
            "multi-valued measures are not yet implemented in casa-rs",
        ));
        return failed();
    }
    let mut quantities = Vec::with_capacity(3);
    for value in values {
        let Some(quantity) = quantity_of(value, lines) else {
            return failed();
        };
        quantities.push(quantity);
    }
    if quantities.iter().all(unset) {
        quantities = vec![
            Quantity::new(0.0, "deg").expect("deg is a unit"),
            Quantity::new(90.0, "deg").expect("deg is a unit"),
            Quantity::new(0.0, "m").expect("m is a unit"),
        ];
    }
    if has(&quantities[1], Dimension::Angle) {
        quantities.rotate_right(1);
    }
    let refer = POSITION_CODES.resolve(rf).unwrap_or_else(|| {
        if !rf.is_empty() {
            lines.push(LogLine::warning(format!(
                "Illegal or unknown reference type '{rf}' for position definition. DEFAULT \
                 (ITRF) assumed."
            )));
        }
        POSITION_CODES.default_code()
    });
    if offset_refused(off, lines) {
        return failed();
    }
    Measure::from_values(MeasureKind::Position, refer, &quantities)
        .unwrap_or(Measure {
            refer,
            value: MeasureValue::Position([0.0; 3]),
        })
        .to_value()
}

/// `me.frequency(rf, v0, off)`: a frequency, or a period, wavelength,
/// wavenumber or energy. An unknown `rf` is LSRK, with CASA's warning.
pub(super) fn frequency(
    rf: &str,
    v0: &Variant,
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    let Some(mut value) = quantity_of(v0, lines) else {
        return failed();
    };
    if unset(&value) {
        value = Quantity::new(0.0, "Hz").expect("Hz is a unit");
    }
    let Some(hz) = frequency_hz(&value) else {
        return exception(
            lines,
            format!(
                "Quantum::assure non-conforming unit type '{}'",
                value.unit().name()
            ),
        );
    };
    let refer = FREQUENCY_CODES.resolve(rf).unwrap_or_else(|| {
        lines.push(LogLine::warning("Illegal reference frame string."));
        FREQUENCY_CODES.default_code()
    });
    if offset_refused(off, lines) {
        return failed();
    }
    Measure {
        refer,
        value: MeasureValue::Frequency(hz),
    }
    .to_value()
}

/// `me.observatory(name)`: the observatory table's position, in its
/// tabulated frame.
///
/// Divergence: the name must match a table entry, ignoring case; casacore
/// also accepts a unique prefix (`me.observatory('ATC')` is ATCA), which
/// the measures data provider cannot answer.
pub(super) fn observatory(
    name: &str,
    measures: &dyn MeasuresProvider,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    match MPosition::from_observatory_name(name.trim(), measures) {
        Ok(Some(position)) => {
            let position = crate::convert::holder::CasaMeasure::Position(position);
            Measure::from_casa(&position).to_value()
        }
        Ok(None) => {
            lines.push(LogLine::error("Unknown observatory asked for"));
            failed()
        }
        Err(error) => exception(lines, error),
    }
}

/// `me.source(name)`: casatools' `casaMDirection` on `name`.
pub(super) fn source(
    name: &Variant,
    measures: &Arc<dyn MeasuresProvider>,
    lines: &mut Vec<LogLine>,
) -> pyo3::PyResult<CasaValue> {
    match direction_measure(name, measures) {
        Ok(Some(direction)) => Ok(direction.to_value()),
        Ok(None) => {
            if let Variant::Text(text) = name {
                lines.push(LogLine::info(format!(
                    "Could not understand Direction string {text}\nValid ones are [COMET, MOON, \
                     SUN, PLUTO, NEPTUNE, URANUS, SATURN, JUPITER, MARS, VENUS, MERCURY] or one \
                     of the valid known sources in the data repos"
                )));
            }
            lines.push(LogLine::error("Unknown source asked for"));
            Ok(failed())
        }
        Err(error) => Ok(exception(lines, error)),
    }
}
