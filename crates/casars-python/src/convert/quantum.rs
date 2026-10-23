// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's `Quantum` as CASA's tools compute with it: a value, or an
//! array of values, with a unit, read from arguments as casatools'
//! `QuantumHolder` reads them and written as its records.
//!
//! The arithmetic is casacore's, not [`Quantity`]'s operators:
//!
//! - **Conversion** ([`Quantum::convert`]) scales conformant units; between angles
//!   and times it goes at 360 degrees a day; for other units it keeps the
//!   value in the target unit times the remaining SI dimensions
//!   (`'1.5km'` to `s` is `1500 s.m.s-1`). With no target it converts to SI
//!   base units. Frequencies and wavelengths do not convert to each other.
//! - **Sums** need units of equal dimensions and take the first unit
//!   (`'1km' + '1m'` is `1.001 km`); products and quotients name their unit
//!   `km.s`, `km/(s)` and `(s)-1`.
//!
//! Unit names are normalised as casacore's `Unit::check` normalises them.

use std::collections::BTreeMap;

use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim, UnitVal};

use super::value::CasaValue;
use super::{ConvertError, ConvertResult, Variant, casacore_unit, quantity, quantity_record};

/// casacore's `C::circle`.
const CIRCLE: f64 = 2.0 * std::f64::consts::PI;
/// casacore's `C::day`, s.
const DAY_S: f64 = 86_400.0;

/// A quantum: one value or an array, and a unit.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Quantum {
    /// The values, in Fortran order.
    pub(crate) values: Vec<f64>,
    /// `None` for a single value; else the array's shape.
    pub(crate) shape: Option<Vec<usize>>,
    pub(crate) unit: Unit,
}

impl Quantum {
    /// One value.
    pub(crate) fn scalar(quantity: &Quantity) -> Self {
        Self {
            values: vec![quantity.value()],
            shape: None,
            unit: quantity.unit().clone(),
        }
    }

    /// A one-dimensional array.
    pub(crate) fn vector(values: Vec<f64>, unit: Unit) -> Self {
        let shape = Some(vec![values.len()]);
        Self {
            values,
            shape,
            unit,
        }
    }

    /// Whether this holds an array.
    pub(crate) fn is_array(&self) -> bool {
        self.shape.is_some()
    }

    /// `QuantumHolder::toRecord`: `{'unit': unit, 'value': value}`, the
    /// value a float or a NumPy array.
    pub(crate) fn to_value(&self) -> CasaValue {
        let value = match &self.shape {
            None => CasaValue::Float(self.values[0]),
            Some(shape) => CasaValue::Floats {
                shape: shape.clone(),
                values: self.values.clone(),
            },
        };
        CasaValue::record([
            ("unit", CasaValue::Text(self.unit.name().to_string())),
            ("value", value),
        ])
    }

    /// `Quantum::convert`: to `target`, or to SI base units for `None`. The
    /// new unit depends only on the units, so an empty array gets it too.
    pub(crate) fn convert(&self, target: Option<&Unit>) -> ConvertResult<Self> {
        let (scaling, unit) = conversion(&self.unit, target)?;
        Ok(Self {
            values: self
                .values
                .iter()
                .map(|&value| scaling.apply(value))
                .collect(),
            shape: self.shape.clone(),
            unit,
        })
    }
}

/// casatools' `quantumHolderFromVar`: a string or a quantity record as a
/// quantum, `None` for other values (CASA's empty holder). A string or
/// record casacore cannot read raises CASA's message.
pub(crate) fn quantum_holder(value: &Variant) -> ConvertResult<Option<Quantum>> {
    match value {
        Variant::Text(_) | Variant::Texts(_) => Ok(Some(Quantum::scalar(&quantity(value)?))),
        Variant::Record(record) => record_quantum(record).map(Some),
        _ => Ok(None),
    }
}

/// `QuantumHolder::fromRecord`: a value, or an array of values.
fn record_quantum(record: &BTreeMap<String, Variant>) -> ConvertResult<Quantum> {
    let wrap =
        |error: String| ConvertError::runtime(format!("Error {error} in converting quantity"));
    match (record.get("value"), record.get("unit")) {
        (Some(Variant::Numbers(values)), Some(Variant::Text(unit))) => Ok(Quantum::vector(
            values.clone(),
            casacore_unit(unit).map_err(wrap)?,
        )),
        (Some(Variant::Array { shape, values }), Some(Variant::Text(unit))) => Ok(Quantum {
            values: values.clone(),
            shape: Some(shape.clone()),
            unit: casacore_unit(unit).map_err(wrap)?,
        }),
        _ => quantity_record(record)
            .map(|quantity| Quantum::scalar(&quantity))
            .map_err(wrap),
    }
}

/// What `Quantum<Double>::convert` does to each value, which depends only on
/// the units: a product with the ratio of their factors, and between an
/// angle and a time the day's turn.
#[derive(Debug, Clone, Copy)]
enum Scaling {
    /// `value * ratio`.
    By(f64),
    /// `value * ratio * C::day / C::circle`.
    AngleToTime(f64),
    /// `value * ratio * C::circle / C::day`.
    TimeToAngle(f64),
}

impl Scaling {
    fn apply(self, value: f64) -> f64 {
        match self {
            Self::By(ratio) => value * ratio,
            Self::AngleToTime(ratio) => value * ratio * DAY_S / CIRCLE,
            Self::TimeToAngle(ratio) => value * ratio * CIRCLE / DAY_S,
        }
    }
}

/// `Quantum<Double>::convert` from `unit` to `target` (SI base units for
/// `None`): how each value scales and the new unit.
fn conversion(unit: &Unit, target: Option<&Unit>) -> ConvertResult<(Scaling, Unit)> {
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
    let unit = casacore_unit(&name).map_err(ConvertError::runtime)?;
    Ok((Scaling::By(remainder.factor), unit))
}

/// casacore's `Quantum::operator+` (`subtract` for `operator-`): the second
/// value in the first's unit added.
pub(crate) fn add(left: &Quantity, right: &Quantity, subtract: bool) -> ConvertResult<Quantity> {
    if left.unit().val().dim != right.unit().val().dim {
        return Err(ConvertError::runtime(format!(
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

/// casacore's `Quantum::operator*` (`divide` for `operator/`).
pub(crate) fn multiply(left: &Quantity, right: &Quantity, divide: bool) -> ConvertResult<Quantity> {
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
            casacore_unit(&format!("({theirs})-1")).map_err(ConvertError::runtime)?
        } else {
            right.unit().clone()
        }
    } else if divide {
        casacore_unit(&format!("{mine}/({theirs})")).map_err(ConvertError::runtime)?
    } else {
        casacore_unit(&format!("{mine}.{theirs}")).map_err(ConvertError::runtime)?
    };
    Ok(Quantity::with_unit(value, unit))
}

#[cfg(test)]
#[path = "quantum_tests.rs"]
mod tests;
