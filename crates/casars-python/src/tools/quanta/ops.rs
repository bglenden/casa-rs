// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `quanta` methods casa-rs implements, as casatools' `quanta_cmpt.cc`
//! computes them.
//!
//! Most methods read their arguments with casatools' `casaQuantity`
//! ([`crate::convert::quantity`]): a string, a quantity record, or a number
//! (no unit); other values are 0 with no unit. `quantity`, `unit`,
//! `getvalue`, `getunit` and `convert` also take quantity records holding
//! arrays. A failure CASA raises is a `RuntimeError` with CASA's message.

use casa_types::quanta::casacore;
use casa_types::quanta::format::{
    CalendarDate, angle_format, format_angle, format_time, time_format,
};
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim, unique_prefix_index};

use crate::convert::holder::frequency_hz;
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

/// CASA's physical constants use casacore's case-insensitive minimax lookup.
pub(super) fn constants(name: &str) -> CasaValue {
    const NAMES: &[&str] = &[
        "pi", "ee", "c", "G", "h", "HI", "R", "NA", "e", "mp", "mp_me", "mu0", "epsilon0", "k",
        "F", "me", "re", "a0", "R0", "k2",
    ];
    const VALUES: &[(f64, &str)] = &[
        (std::f64::consts::PI, ""),
        (std::f64::consts::E, ""),
        (299_792_458.0, "m/s"),
        (6.67259e-11, "N.m2/kg2"),
        (6.6260755e-34, "J.s"),
        (1420.405751786, "MHz"),
        (8.31451, "J/K/mol"),
        (6.0221367e23, "mol-1"),
        (1.60217733e-19, "C"),
        (1.6726231e-27, "kg"),
        (1836.152701, ""),
        (1.2566370614359173e-6, "H/m"),
        (8.854187817620389e-12, "F/m"),
        (1.3806577987510647e-23, "J/K"),
        (96485.3089890101, "C/mol"),
        (9.109389971155782e-31, "kg"),
        (2.8179e-15, "m"),
        (5.2918e-11, "m"),
        (6.9599e8, "m"),
        (0.00029591220828559115, "AU3/d2/S0"),
    ];
    let (value, unit) = unique_prefix_index(name, NAMES).map_or((0.0, ""), |index| VALUES[index]);
    CasaValue::record([
        ("value", CasaValue::Float(value)),
        ("unit", CasaValue::Text(unit.into())),
    ])
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
    let Variant::Record(record) = value else {
        return Ok(None);
    };
    if matches!(record.get("value"), Some(Variant::Array { shape, .. }) if shape.is_empty()) {
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

/// `qa.ceil(v)` and `qa.floor(v)` preserve the input quantity's unit.
pub(super) fn round_integer(value: &Variant, upward: bool) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    let rounded = if upward {
        value.value().ceil()
    } else {
        value.value().floor()
    };
    Ok(record(&Quantity::with_unit(rounded, value.unit().clone())))
}

/// CASA's scalar exponential and logarithmic functions.
pub(super) fn logarithm(value: &Variant, method: &str) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    if value.unit().val().dim != UnitDim::NODIM {
        return Err(ConvertError::runtime(format!(
            "Quantum::{method} illegal unit type '{}'",
            value.unit().name()
        )));
    }
    let number = value.get_si_value();
    let result = match method {
        "exp" => number.exp(),
        "log" => number.ln(),
        "log10" => number.log10(),
        _ => unreachable!("the binding selects an exponential or logarithmic method"),
    };
    Ok(record(&Quantity::dimensionless(result)))
}

/// CASA's scalar trigonometric functions, with angular unit checking.
pub(super) fn trigonometry(value: &Variant, method: &str) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    let inverse = matches!(method, "asin" | "acos" | "atan");
    let valid = if inverse {
        value.unit().val().dim == UnitDim::NODIM
    } else {
        has(&value, Dimension::Angle)
    };
    if !valid {
        return Err(ConvertError::runtime(format!(
            "Quantum::{method} illegal unit type '{}'",
            value.unit().name()
        )));
    }
    let number = value.get_si_value();
    let result = match method {
        "sin" => number.sin(),
        "cos" => number.cos(),
        "tan" => number.tan(),
        "asin" => number.asin(),
        "acos" => number.acos(),
        "atan" => number.atan(),
        _ => unreachable!("the binding selects a trigonometric method"),
    };
    Ok(record(&if inverse {
        Quantity::with_unit(result, unit("rad"))
    } else {
        Quantity::dimensionless(result)
    }))
}

/// `qa.atan2(v, a)`: convert both dimensionless quantities before computing.
pub(super) fn atan2(value: &Variant, other: &Variant) -> ConvertResult<CasaValue> {
    let (value, other) = (quantity(value)?, quantity(other)?);
    if value.unit().val().dim != UnitDim::NODIM || other.unit().val().dim != UnitDim::NODIM {
        return Err(ConvertError::runtime(format!(
            "Quantum::atan2 illegal unit type '{}'",
            value.unit().name()
        )));
    }
    Ok(record(&Quantity::with_unit(
        value.get_si_value().atan2(other.get_si_value()),
        unit("rad"),
    )))
}

/// `qa.sqrt(v)`: take the root in SI units, as casacore does.
pub(super) fn sqrt(value: &Variant) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    let dimension =
        value.unit().val().dim.sqrt().map_err(|_| {
            ConvertError::runtime("UnitVal::UnitVal Illegal unit dimensions for root")
        })?;
    let result_unit = if dimension == UnitDim::NODIM {
        Unit::dimensionless()
    } else {
        unit(&dimension.to_string())
    };
    let number = value.get_si_value();
    // CASA's root calculation maps both signed zeros to positive zero.
    let result = if number == 0.0 { 0.0 } else { number.sqrt() };
    Ok(record(&Quantity::with_unit(result, result_unit)))
}

/// `qa.pow(v, a)`: CASA multiplies or divides repeatedly in the original unit.
pub(super) fn power(value: &Variant, power: i32) -> ConvertResult<CasaValue> {
    let quantity = quantity(value)?;
    if power.unsigned_abs() >= 100 {
        return Err(ConvertError::runtime("Quantum::pow exponent too large"));
    }
    let number = quantity.value();
    // This initialization deliberately turns infinities into NaN, as CASA does.
    let mut result = number * 0.0 + 1.0;
    if power >= 0 {
        for _ in 0..power {
            result *= number;
        }
    } else {
        for _ in power..0 {
            result /= number;
        }
    }
    let unit = if power == 0 || quantity.unit().name().is_empty() {
        String::new()
    } else {
        format!("({}){power}", quantity.unit().name())
    };
    Ok(CasaValue::record([
        ("unit", CasaValue::Text(unit)),
        ("value", CasaValue::Float(result)),
    ]))
}

/// `qa.norm(v, a)`: casacore's MVAngle normalizes in turns and returns degrees.
#[allow(clippy::manual_range_contains)] // NaN must leave the value unchanged.
pub(super) fn normalize(value: &Variant, origin: f64) -> ConvertResult<CasaValue> {
    let mut angle = angle_radians(&quantity(value)?)?;
    let turns = angle / CIRCLE - origin;
    if turns < 0.0 || turns >= 1.0 {
        angle -= turns.floor() * CIRCLE;
    }
    let degrees = unit("deg");
    Ok(record(&Quantity::with_unit(
        angle / degrees.val().factor,
        degrees,
    )))
}

/// `qa.compare(v, a)`: whether the units have the same dimensions.
pub(super) fn compare(left: &Variant, right: &Variant) -> ConvertResult<CasaValue> {
    Ok(CasaValue::Bool(
        quantity(left)?.unit().val().dim == quantity(right)?.unit().val().dim,
    ))
}

/// The six scalar relational operators convert the right value into the left
/// unit. Equality allows unlike dimensions; ordered comparisons reject them.
pub(super) fn relational(left: &Variant, right: &Variant, op: &str) -> ConvertResult<CasaValue> {
    let (left, right) = (quantity(left)?, quantity(right)?);
    let conformant = left.unit().val().dim == right.unit().val().dim;
    if !conformant && !matches!(op, "eq" | "ne") {
        return Err(ConvertError::runtime(format!(
            "Quantum::operator< unequal units '{}, '{}'",
            left.unit().name(),
            right.unit().name()
        )));
    }
    let scaled = right.value() * (right.unit().val().factor / left.unit().val().factor);
    let order = left.value().partial_cmp(&scaled);
    let equal = conformant && order == Some(std::cmp::Ordering::Equal);
    let result = match op {
        "eq" => equal,
        "ne" => !equal,
        "lt" => order == Some(std::cmp::Ordering::Less),
        "gt" => order == Some(std::cmp::Ordering::Greater),
        // casacore defines <= as !(>) and >= as !(<), including for NaN.
        "le" => order != Some(std::cmp::Ordering::Greater),
        "ge" => order != Some(std::cmp::Ordering::Less),
        _ => unreachable!("unknown quantity comparison"),
    };
    Ok(CasaValue::Bool(result))
}

/// `qa.convertfreq`: use casacore's frequency equivalencies in both directions.
pub(super) fn convert_frequency(value: &Variant, output: &str) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    let hz = frequency_hz(&value).ok_or_else(|| non_conforming(&value))?;
    let output = casacore_unit(output).map_err(ConvertError::runtime)?;
    let one = Quantity::with_unit(1.0, output.clone());
    let factor = frequency_hz(&one).ok_or_else(|| non_conforming(&one))?;
    let dim = output.val().dim;
    let result =
        if dim == UnitDim::basis(Dimension::Time) || dim == UnitDim::basis(Dimension::Length) {
            factor / hz
        } else {
            hz / factor
        };
    Ok(record(&Quantity::with_unit(result, output)))
}

/// `qa.convertdop`: a fraction of light speed and a speed are equivalent.
pub(super) fn convert_doppler(value: &Variant, output: &str) -> ConvertResult<CasaValue> {
    const C: f64 = 299_792_458.0;
    let value = quantity(value)?;
    let time = UnitDim::basis(Dimension::Time);
    let speed = UnitDim::basis(Dimension::Length) / time;
    let dimension = value.unit().val().dim;
    let ratio = if dimension == UnitDim::NODIM {
        value.get_si_value()
    } else if dimension == speed {
        value.get_si_value() / C
    } else {
        return Err(non_conforming(&value));
    };
    let output = casacore_unit(output).map_err(ConvertError::runtime)?;
    let result = if output.val().dim == UnitDim::NODIM {
        ratio / output.val().factor
    } else if output.val().dim == speed {
        ratio * C / output.val().factor
    } else {
        return Err(non_conforming(&Quantity::with_unit(1.0, output)));
    };
    Ok(record(&Quantity::with_unit(result, output)))
}

/// `qa.checkfreq(cm)`: the dimensions accepted by casacore's MVFrequency.
pub(super) fn checkfreq(value: &Variant) -> ConvertResult<CasaValue> {
    let dimension = quantity(value)?.unit().val().dim;
    let time = UnitDim::basis(Dimension::Time);
    let angle = UnitDim::basis(Dimension::Angle);
    let length = UnitDim::basis(Dimension::Length);
    let mass = UnitDim::basis(Dimension::Mass);
    let accepted = [
        time,
        UnitDim::NODIM / time,
        angle / time,
        length,
        UnitDim::NODIM / length,
        mass * length * length / time / time,
        mass * length,
    ];
    Ok(CasaValue::Bool(accepted.contains(&dimension)))
}

/// `qa.isangle(v)`: angles and times are accepted by casacore's angle form.
pub(super) fn isangle(value: &Variant) -> ConvertResult<CasaValue> {
    let value = quantity(value)?;
    Ok(CasaValue::Bool(
        has(&value, Dimension::Angle) || has(&value, Dimension::Time),
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

/// `qa.formxxx(v, format, prec)`: `None` for a format CASA does not
/// understand (it writes an empty string and warns).
pub(super) fn formxxx(
    value: &Variant,
    format: &str,
    precision: i64,
) -> ConvertResult<Option<String>> {
    Ok(casacore::formxxx(&quantity(value)?, format, precision)?)
}

/// `qa.tos(v, prec)`.
pub(super) fn tos(value: &Variant, precision: i64) -> ConvertResult<CasaValue> {
    Ok(CasaValue::Text(casacore::tos(&quantity(value)?, precision)))
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
