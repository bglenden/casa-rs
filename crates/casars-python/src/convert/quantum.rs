// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's `Quantum` as CASA's tools compute with it: a value, or an
//! array of values, with a unit, read from arguments as casatools'
//! `QuantumHolder` reads them and written as its records.
//!
//! The arithmetic is casacore's, not [`Quantity`]'s operators
//! ([`casa_types::quanta::casacore`]): conversions scale by the ratio of
//! the unit factors (at 360 degrees a day between angles and times, keeping
//! the remaining SI dimensions otherwise); sums take the first unit; products
//! and quotients name their unit `km.s`, `km/(s)` and `(s)-1`. Frequencies
//! and wavelengths do not convert to each other.

use std::collections::BTreeMap;

use casa_types::quanta::casacore::{self, QuantumError, conversion};
use casa_types::quanta::{Quantity, Unit};

use super::value::CasaValue;
use super::{ConvertError, ConvertResult, Variant, casacore_unit, quantity, quantity_record};

impl From<QuantumError> for ConvertError {
    fn from(error: QuantumError) -> Self {
        ConvertError::runtime(error.0)
    }
}

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

/// casacore's `Quantum::operator+` (`subtract` for `operator-`).
pub(crate) fn add(left: &Quantity, right: &Quantity, subtract: bool) -> ConvertResult<Quantity> {
    Ok(if subtract {
        casacore::sub(left, right)?
    } else {
        casacore::add(left, right)?
    })
}

/// casacore's `Quantum::operator*` (`divide` for `operator/`).
pub(crate) fn multiply(left: &Quantity, right: &Quantity, divide: bool) -> ConvertResult<Quantity> {
    Ok(if divide {
        casacore::div(left, right)?
    } else {
        casacore::mul(left, right)?
    })
}

#[cfg(test)]
#[path = "quantum_tests.rs"]
mod tests;
