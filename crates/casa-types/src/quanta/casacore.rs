// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's `Quantum<Double>` arithmetic, and the strings CASA's `quanta`
//! tool (`qa`) writes, for code that must reproduce CASA's numbers to the
//! last bit: casars' `qa` tool, and `simutil`'s pointing lists, whose
//! printed digits follow every rounding of casacore's arithmetic.
//!
//! [`Quantity`]'s operators round differently:
//! - **Sums** here divide the second value by the ratio of the unit factors
//!   (`Quantum::getValue`), and take the first unit (`'1km' + '1m'` is
//!   `1.001 km`); they need units of equal dimensions.
//! - **Conversions** multiply by that ratio (`Quantum::convert`). Between
//!   angles and times they go at 360 degrees a day. For other units the value
//!   is kept in the target unit times the remaining SI dimensions (`'1.5km'`
//!   to `s` is `1500 s.m.s-1`). With no target they convert to SI base units.
//! - **Products and quotients** name their unit `a.b`, `a/(b)` and `(b)-1`.
//!
//! Unit names are normalised as casacore's `Unit::check` normalises them
//! ([`casacore_unit`]).

use std::fmt;

use super::format::{ANGLE, TIME, format_angle};
use super::{Dimension, Quantity, Unit, UnitDim, UnitVal, casacore_unit_name};

/// casacore's `C::circle`.
const CIRCLE: f64 = 2.0 * std::f64::consts::PI;
/// casacore's `C::day`, s.
const DAY_S: f64 = 86_400.0;

/// A casacore `Quantum` error, with casacore's message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantumError(pub String);

impl fmt::Display for QuantumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for QuantumError {}

/// casacore's `Unit(name)`: the name normalised as `Unit::check` normalises
/// it, then read.
pub fn casacore_unit(name: &str) -> Result<Unit, QuantumError> {
    Unit::new(&casacore_unit_name(name))
        .map_err(|_| QuantumError(format!("Unit::check Illegal unit string '{name}'")))
}

/// What `Quantum<Double>::convert` does to each value, which depends only on
/// the units: a product with the ratio of their factors, and between an
/// angle and a time the day's turn.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scaling {
    /// `value * ratio`.
    By(f64),
    /// `value * ratio * C::day / C::circle`.
    AngleToTime(f64),
    /// `value * ratio * C::circle / C::day`.
    TimeToAngle(f64),
}

impl Scaling {
    /// One value scaled.
    pub fn apply(self, value: f64) -> f64 {
        match self {
            Self::By(ratio) => value * ratio,
            Self::AngleToTime(ratio) => value * ratio * DAY_S / CIRCLE,
            Self::TimeToAngle(ratio) => value * ratio * CIRCLE / DAY_S,
        }
    }
}

/// `Quantum<Double>::convert` from `unit` to `target` (SI base units for
/// `None`): how each value scales, and the new unit.
pub fn conversion(unit: &Unit, target: Option<&Unit>) -> Result<(Scaling, Unit), QuantumError> {
    let mine = *unit.val();
    let theirs = target.map_or(UnitVal::NODIM, |target| *target.val());
    let target_unit = || target.cloned().unwrap_or_else(Unit::dimensionless);
    let angle = UnitDim::basis(Dimension::Angle);
    let time = UnitDim::basis(Dimension::Time);
    let ratio = mine.factor / theirs.factor;
    if mine.dim == theirs.dim {
        return Ok((Scaling::By(ratio), target_unit()));
    }
    if mine.dim == angle && theirs.dim == time {
        return Ok((Scaling::AngleToTime(ratio), target_unit()));
    }
    if mine.dim == time && theirs.dim == angle {
        return Ok((Scaling::TimeToAngle(ratio), target_unit()));
    }
    let remainder = mine / theirs;
    let dimensions = remainder.dim.to_string();
    let name = match target {
        Some(target) if !target.name().is_empty() => format!("{}.{dimensions}", target.name()),
        _ => dimensions,
    };
    Ok((Scaling::By(remainder.factor), casacore_unit(&name)?))
}

/// `Quantum::get(target)` of one value (`qa.convert`); SI base units for
/// `None`.
pub fn convert(value: &Quantity, target: Option<&Unit>) -> Result<Quantity, QuantumError> {
    let (scaling, unit) = conversion(value.unit(), target)?;
    Ok(Quantity::with_unit(scaling.apply(value.value()), unit))
}

/// casacore's `Quantum::operator+`: the second value in the first's unit,
/// added (`qa.add`).
pub fn add(left: &Quantity, right: &Quantity) -> Result<Quantity, QuantumError> {
    sum(left, right, false)
}

/// casacore's `Quantum::operator-` (`qa.sub`).
pub fn sub(left: &Quantity, right: &Quantity) -> Result<Quantity, QuantumError> {
    sum(left, right, true)
}

fn sum(left: &Quantity, right: &Quantity, subtract: bool) -> Result<Quantity, QuantumError> {
    if left.unit().val().dim != right.unit().val().dim {
        return Err(QuantumError(format!(
            "Quantum::operator{} unequal units '{}, '{}'",
            if subtract { '-' } else { '+' },
            left.unit().name(),
            right.unit().name()
        )));
    }
    // `other.getValue(qUnit)`: divided by the ratio of the unit factors.
    let right = right.value() / (left.unit().val().factor / right.unit().val().factor);
    let value = if subtract {
        left.value() - right
    } else {
        left.value() + right
    };
    Ok(Quantity::with_unit(value, left.unit().clone()))
}

/// casacore's `Quantum::operator*` (`qa.mul`).
pub fn mul(left: &Quantity, right: &Quantity) -> Result<Quantity, QuantumError> {
    product(left, right, false)
}

/// casacore's `Quantum::operator/` (`qa.div`).
pub fn div(left: &Quantity, right: &Quantity) -> Result<Quantity, QuantumError> {
    product(left, right, true)
}

fn product(left: &Quantity, right: &Quantity, divide: bool) -> Result<Quantity, QuantumError> {
    let (mine, theirs) = (left.unit().name(), right.unit().name());
    let value = if divide {
        left.value() / right.value()
    } else {
        left.value() * right.value()
    };
    let unit = if theirs.is_empty() {
        left.unit().clone()
    } else if mine.is_empty() {
        if divide {
            casacore_unit(&format!("({theirs})-1"))?
        } else {
            right.unit().clone()
        }
    } else if divide {
        casacore_unit(&format!("{mine}/({theirs})"))?
    } else {
        casacore_unit(&format!("{mine}.{theirs}"))?
    };
    Ok(Quantity::with_unit(value, unit))
}

/// casacore's `MVAngle(Quantity)`: radians of an angle, or of a time at 360
/// degrees a day.
pub fn mvangle_radians(value: &Quantity) -> Result<f64, QuantumError> {
    let base = value.get_si_value();
    let dim = value.unit().val().dim;
    if dim == UnitDim::basis(Dimension::Angle) {
        Ok(base)
    } else if dim == UnitDim::basis(Dimension::Time) {
        Ok(base * (CIRCLE / DAY_S))
    } else {
        Err(QuantumError(format!(
            "Quantum::assure: non-conforming unit '{}'",
            value.unit().name()
        )))
    }
}

/// casacore's `MVAngle::operator()(norm)`: `radians` moved by whole turns
/// into `[norm, norm + 1)` turns, but only when it lies outside them, so an
/// angle already inside is left exactly as it is.
pub fn mvangle_normalized(radians: f64, norm: f64) -> f64 {
    let t = radians / (2.0 * std::f64::consts::PI) - norm;
    if t < 0.0 || t >= 1.0 {
        radians - t.floor() * (2.0 * std::f64::consts::PI)
    } else {
        radians
    }
}

/// `value` as a C++ `ostream` writes a `double` by default: six significant
/// digits, in fixed or scientific notation as `%g` chooses, without
/// trailing zeros (`3.14159`, `6.37358e+06`, `58849`).
pub fn stream_double(value: f64) -> String {
    const PRECISION: usize = 6;
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan".to_string()
        } else if value > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    let scientific = format!("{:.*e}", PRECISION - 1, value);
    let (mantissa, exponent) = scientific.split_once('e').expect("Rust writes an exponent");
    let exponent: i32 = exponent.parse().expect("Rust writes an integer exponent");
    let trim = |digits: &str| {
        if digits.contains('.') {
            digits
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            digits.to_string()
        }
    };
    if exponent < -4 || exponent >= PRECISION as i32 {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exponent.abs())
    } else {
        let decimals = usize::try_from(PRECISION as i32 - 1 - exponent).unwrap_or(0);
        trim(&format!("{value:.decimals$}"))
    }
}

/// casatools' `qa.formxxx(value, format, prec)` (`ang_as_formatted_str`):
/// an angle or time as `hms` (`MVAngle::TIME`, `6 + prec` digits), `dms`
/// (`MVAngle::ANGLE` after normalising into [−180°, 180°), `7 + prec`
/// digits), or `deg`, `rad` and `+deg` as a C++ stream writes the number
/// (normalised into [−180°, 180°), or into [0°, 360°) for `+deg`).
/// `Ok(None)` for any other format, which CASA answers with an empty string
/// and a warning. The format's case is ignored.
pub fn formxxx(value: &Quantity, format: &str, precision: i64) -> Result<Option<String>, QuantumError> {
    let radians = mvangle_radians(value)?;
    // casacore's `Int ndig = 6 + precision`, passed on as an unsigned count.
    let digits = |extra: i64| u32::try_from(6 + precision + extra).unwrap_or(u32::MAX);
    // `MVAngle::degree()` divides by `C::degree` (π/180).
    let degrees = |radians: f64| radians / (std::f64::consts::PI / 180.0);
    let out = match format.to_ascii_lowercase().as_str() {
        "dms" => format_angle(mvangle_normalized(radians, -0.5), ANGLE, digits(1)),
        "hms" => format_angle(radians, TIME, digits(0)),
        "deg" => stream_double(degrees(mvangle_normalized(radians, -0.5))),
        "rad" => stream_double(mvangle_normalized(radians, -0.5)),
        "+deg" => stream_double(degrees(mvangle_normalized(radians, 0.0))),
        _ => return Ok(None),
    };
    Ok(Some(out))
}

/// casatools' `qa.tos(value, prec)`: the value in fixed notation with
/// `prec` decimals (C++'s default six for a negative one), then the unit.
pub fn tos(value: &Quantity, precision: i64) -> String {
    let decimals = usize::try_from(precision).unwrap_or(6);
    format!("{:.decimals$}{}", value.value(), value.unit().name())
}

#[cfg(test)]
#[path = "casacore_tests.rs"]
mod tests;
