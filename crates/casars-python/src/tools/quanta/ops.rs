// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `quanta` methods casa-rs implements, as casatools' `quanta_cmpt.cc`
//! computes them.
//!
//! Most methods read their arguments with casatools' `casaQuantity`
//! ([`crate::convert::quantity`]): a string, a quantity record, or a number
//! (no unit); other values are 0 with no unit. `quantity`, `unit`,
//! `getvalue`, `getunit` and `convert` also take quantity records holding
//! arrays. A failure CASA raises is a `RuntimeError` with CASA's message.

use casa_types::quanta::format::{
    CalendarDate, angle_format, format_angle, format_time, time_format,
};
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim};

use crate::convert::quantum::{self, Quantum, quantum_holder};
use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult, Variant, casacore_unit, quantity};

/// casacore's `C::circle`.
const CIRCLE: f64 = 2.0 * std::f64::consts::PI;
/// casacore's `C::day`, s.
const DAY_S: f64 = 86_400.0;

fn has(quantity: &Quantity, dimension: Dimension) -> bool {
    quantity.unit().val().dim == UnitDim::basis(dimension)
}

fn record(quantity: &Quantity) -> CasaValue {
    Quantum::scalar(quantity).to_value()
}

fn non_conforming(quantity: &Quantity) -> ConvertError {
    ConvertError::runtime(format!(
        "Quantum::assure non-conforming unit type '{}'",
        quantity.unit().name()
    ))
}

/// `qa.quantity(v, unitname, keepshape)` and `qa.unit`: a number or numbers
/// in `unitname`, or a string or record read as a quantity (`unitname`
/// unused). With `keepshape`, an array of two or more dimensions keeps its
/// shape; otherwise its values are flattened in Fortran order, as in CASA.
pub(super) fn quantity_of(
    value: &Variant,
    unit_name: &str,
    keep_shape: bool,
) -> ConvertResult<CasaValue> {
    let unit = || casacore_unit(unit_name).map_err(ConvertError::runtime);
    let quantum = match value {
        Variant::Number(number) => Quantum::scalar(&Quantity::with_unit(*number, unit()?)),
        Variant::Numbers(numbers) => Quantum::vector(numbers.clone(), unit()?),
        Variant::Array { shape, values } => Quantum {
            values: values.clone(),
            shape: Some(if keep_shape {
                shape.clone()
            } else {
                vec![values.len()]
            }),
            unit: unit()?,
        },
        other => match quantum_holder(other)? {
            Some(quantum) if quantum.is_array() => quantum,
            _ => Quantum::scalar(&quantity(other)?),
        },
    };
    Ok(quantum.to_value())
}

/// A quantity record holding an array, else `None`.
fn array_record(value: &Variant) -> ConvertResult<Option<Quantum>> {
    if !matches!(value, Variant::Record(_)) {
        return Ok(None);
    }
    Ok(quantum_holder(value)?.filter(Quantum::is_array))
}

/// `qa.getvalue(v)`: the value or values, always as an array.
pub(super) fn getvalue(value: &Variant) -> ConvertResult<CasaValue> {
    Ok(match array_record(value)? {
        Some(quantum) => CasaValue::floats(quantum.values),
        None => CasaValue::floats(vec![quantity(value)?.value()]),
    })
}

/// `qa.getunit(v)`.
pub(super) fn getunit(value: &Variant) -> ConvertResult<CasaValue> {
    let unit = match array_record(value)? {
        Some(quantum) => quantum.unit,
        None => quantity(value)?.unit().clone(),
    };
    Ok(CasaValue::Text(unit.name().to_string()))
}

/// `qa.convert(v, outunit)`: `v` in the unit of the quantity `outunit`, or
/// in SI base units when that has none.
pub(super) fn convert(value: &Variant, out_unit: &Variant) -> ConvertResult<CasaValue> {
    let holder = quantum_holder(value)?;
    let target = quantity(out_unit)?.unit().clone();
    let target = (!target.name().is_empty()).then_some(&target);
    let quantum = match holder {
        Some(quantum) if quantum.is_array() => quantum,
        _ => Quantum::scalar(&quantity(value)?),
    };
    Ok(quantum.convert(target)?.to_value())
}

/// `qa.canonical(v)` and `qa.canon(v)`: `v` in SI base units.
pub(super) fn canonical(value: &Variant) -> ConvertResult<CasaValue> {
    Ok(Quantum::scalar(&quantity(value)?).convert(None)?.to_value())
}

/// `qa.add`, `qa.sub`, `qa.mul` and `qa.div`.
pub(super) fn arithmetic(
    left: &Variant,
    right: &Variant,
    operation: char,
) -> ConvertResult<CasaValue> {
    let (left, right) = (quantity(left)?, quantity(right)?);
    let result = match operation {
        '+' => quantum::add(&left, &right, false)?,
        '-' => quantum::add(&left, &right, true)?,
        '*' => quantum::multiply(&left, &right, false)?,
        _ => quantum::multiply(&left, &right, true)?,
    };
    Ok(record(&result))
}

/// `qa.neg(v)`.
pub(super) fn neg(value: &Variant) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    Ok(record(&Quantity::with_unit(
        -value.value(),
        value.unit().clone(),
    )))
}

/// `qa.abs(v)`.
pub(super) fn abs(value: &Variant) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    Ok(record(&Quantity::with_unit(
        value.value().abs(),
        value.unit().clone(),
    )))
}

/// `qa.compare(v, a)`: whether the units have the same dimensions.
pub(super) fn compare(left: &Variant, right: &Variant) -> ConvertResult<CasaValue> {
    Ok(CasaValue::Bool(
        quantity(left)?.unit().val().dim == quantity(right)?.unit().val().dim,
    ))
}

/// `qa.check(v)`: whether casacore reads the string as a quantity.
pub(super) fn check(text: &str) -> CasaValue {
    CasaValue::Bool(Quantity::read(text).is_ok())
}

/// `qa.isquantity(v)`: whether `v` is a string casacore reads as a
/// quantity, or a quantity record.
///
/// Divergence: CASA's records with an integer value are not quantities
/// (casacore holds them as integer quanta); here every quantity record is.
pub(super) fn isquantity(value: &Variant) -> CasaValue {
    CasaValue::Bool(match value {
        Variant::Text(text) => Quantity::read(text).is_ok(),
        Variant::Record(_) => quantum_holder(value).is_ok(),
        _ => false,
    })
}

/// casacore's `MVTime(Quantity)`: MJD days of a time, or turns of an angle.
fn time_days(value: &Quantity) -> ConvertResult<f64> {
    let base = value.get_si_value();
    if has(value, Dimension::Angle) {
        Ok(base / CIRCLE)
    } else if has(value, Dimension::Time) {
        Ok(base / DAY_S)
    } else {
        Err(non_conforming(value))
    }
}

/// casacore's `MVAngle(Quantity)`: radians of an angle, or of a time at 360
/// degrees a day.
fn angle_radians(value: &Quantity) -> ConvertResult<f64> {
    let base = value.get_si_value();
    if has(value, Dimension::Angle) {
        Ok(base)
    } else if has(value, Dimension::Time) {
        Ok(base * (CIRCLE / DAY_S))
    } else {
        Err(non_conforming(value))
    }
}

/// casatools passes a `long` precision on as an unsigned one.
fn digits(precision: i64) -> u32 {
    u32::try_from(precision).unwrap_or(u32::MAX)
}

/// `qa.time(v, prec, form, showform)`: one string, formatted by casacore's
/// `MVTime` with the named formats added to `TIME`.
pub(super) fn time(value: &Variant, precision: i64, form: &[String]) -> ConvertResult<CasaValue> {
    let days = time_days(&quantity(value)?)?;
    let format = form.iter().fold(time_format("time"), |format, name| {
        format | time_format(name)
    });
    Ok(CasaValue::List(vec![CasaValue::Text(format_time(
        days,
        format,
        digits(precision),
    ))]))
}

/// `qa.angle(v, prec, form, showform)`: one string, formatted by casacore's
/// `MVAngle` with the named formats added to `ANGLE`.
pub(super) fn angle(value: &Variant, precision: i64, form: &[String]) -> ConvertResult<CasaValue> {
    let radians = angle_radians(&quantity(value)?)?;
    let format = form.iter().fold(angle_format("angle"), |format, name| {
        format | angle_format(name)
    });
    Ok(CasaValue::List(vec![CasaValue::Text(format_angle(
        radians,
        format,
        digits(precision),
    ))]))
}

/// `qa.totime(v)`: a time as it is, an angle as days.
pub(super) fn totime(value: &Variant) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    if has(&value, Dimension::Time) {
        return Ok(record(&value));
    }
    let days = time_days(&value)?;
    Ok(record(&Quantity::with_unit(days, unit("d"))))
}

/// `qa.toangle(v)`: an angle as it is, a time as radians.
pub(super) fn toangle(value: &Variant) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    if has(&value, Dimension::Angle) {
        return Ok(record(&value));
    }
    let radians = angle_radians(&value)?;
    Ok(record(&Quantity::with_unit(radians, unit("rad"))))
}

fn unit(name: &str) -> Unit {
    Unit::new(name).expect("a casacore base unit")
}

/// `qa.splitdate(v)`: a time's calendar fields and time of day.
pub(super) fn splitdate(value: &Variant) -> ConvertResult<CasaValue> {
    let mjd = time_days(&quantity(value)?)?;
    let date = CalendarDate::of(mjd);
    // casatools' chain of `fmod`s and truncations.
    let mut fraction = mjd % 1.0;
    let hour = (fraction * 24.0) as i64;
    fraction = (fraction * 24.0) % 1.0;
    let minute = (fraction * 60.0) as i64;
    fraction = (fraction * 60.0) % 1.0;
    let second = (fraction * 60.0) as i64;
    let seconds = fraction * 60.0;
    fraction = (fraction * 60.0) % 1.0;
    let int = CasaValue::Int;
    Ok(CasaValue::record([
        ("mjd", CasaValue::Float(mjd)),
        ("year", int(date.year)),
        ("yearday", int(date.yearday)),
        ("month", int(date.month)),
        ("monthday", int(date.monthday)),
        ("week", int(date.week)),
        ("weekday", int(date.weekday)),
        ("hour", int(hour)),
        ("min", int(minute)),
        ("sec", int(second)),
        ("s", CasaValue::Float(seconds)),
        ("msec", int((fraction * 1000.0) as i64)),
        ("usec", int((fraction * 1.0e6) as i64)),
    ]))
}
