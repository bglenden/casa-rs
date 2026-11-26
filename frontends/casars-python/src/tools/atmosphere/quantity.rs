// SPDX-License-Identifier: LGPL-3.0-or-later
//! The atmosphere wrapper's typed scalar and vector quantities.

use casa_contracts::CasaValue;
use casa_core::quanta::{Quantity, casacore};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

use crate::convert::{Variant, casacore_unit, read_quantity};

pub(super) enum QuantityArg {
    Omitted,
    Value(Variant),
}

impl<'a, 'py> FromPyObject<'a, 'py> for QuantityArg {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self::Value(value.extract()?))
    }
}

pub(super) struct AtmosphereQuantity {
    pub(super) values: Vec<f64>,
    pub(super) unit: String,
}

impl QuantityArg {
    pub(super) fn read(self, name: &str, default: (f64, &str)) -> PyResult<AtmosphereQuantity> {
        match self {
            Self::Omitted => Ok(AtmosphereQuantity {
                values: vec![default.0],
                unit: default.1.to_owned(),
            }),
            Self::Value(Variant::Text(text)) => {
                let parsed = read_quantity(&text, "")?;
                Ok(AtmosphereQuantity {
                    values: vec![parsed.value()],
                    unit: parsed.unit().name().to_string(),
                })
            }
            Self::Value(Variant::Record(record)) => {
                let Some(Variant::Text(unit)) = record.get("unit") else {
                    return Err(PyTypeError::new_err(format!("_{name} has no string unit")));
                };
                let values = match record.get("value") {
                    Some(Variant::Number(value)) => vec![*value],
                    Some(Variant::Numbers(values) | Variant::Array { values, .. }) => {
                        values.clone()
                    }
                    Some(Variant::Empty) => Vec::new(),
                    _ => {
                        return Err(PyTypeError::new_err(format!(
                            "_{name} has no numeric value"
                        )));
                    }
                };
                Ok(AtmosphereQuantity {
                    values,
                    unit: unit.clone(),
                })
            }
            Self::Value(_) => Err(PyTypeError::new_err(format!("_{name} is not a dictionary"))),
        }
    }
}

impl AtmosphereQuantity {
    pub(super) fn validate_unit(&self) -> PyResult<()> {
        casacore_unit(&self.unit)
            .map(|_| ())
            .map_err(PyRuntimeError::new_err)
    }

    pub(super) fn scalar(&self) -> PyResult<f64> {
        self.values
            .first()
            .copied()
            .ok_or_else(|| PyRuntimeError::new_err("Bad quantity conversion"))
    }

    pub(super) fn value_in(&self, target: &str) -> PyResult<f64> {
        self.index_in(0, target)
    }

    pub(super) fn index_in(&self, index: usize, target: &str) -> PyResult<f64> {
        let value = self
            .values
            .get(index)
            .copied()
            .ok_or_else(|| PyRuntimeError::new_err("Bad quantity conversion"))?;
        let source = casacore_unit(&self.unit).map_err(PyRuntimeError::new_err)?;
        let target = casacore_unit(target).map_err(PyRuntimeError::new_err)?;
        Ok(casacore::get_value(
            &Quantity::with_unit(value, source),
            &target,
        ))
    }

    pub(super) fn frequency_hz(&self, index: usize) -> PyResult<f64> {
        let value = self
            .values
            .get(index)
            .copied()
            .ok_or_else(|| PyRuntimeError::new_err("Bad quantity conversion"))?;
        let scale = match self.unit.as_str() {
            "THz" | "THZ" => 1e12,
            "GHz" | "ghz" => 1e9,
            "MHz" | "MHZ" | "mhz" => 1e6,
            "kHz" | "KHZ" | "khz" => 1e3,
            _ => 1.0,
        };
        Ok(value * scale)
    }

    pub(super) fn water_m(&self) -> PyResult<f64> {
        let scale = match self.unit.as_str() {
            "km" | "KM" => 1e3,
            "mm" | "MM" => 1e-3,
            "micron" | "MICRON" | "microns" | "MICRONS" => 1e-6,
            "nm" | "NM" => 1e-9,
            _ => 1.0,
        };
        Ok(self.scalar()? * scale)
    }

    pub(super) fn sky_kelvin(&self) -> PyResult<f64> {
        let value = self.scalar()?;
        Ok(match self.unit.as_str() {
            "mK" | "mk" => value * 1e-3,
            "C" | "c" => value + 273.16,
            "F" | "f" => (value - 32.) * (5. / 9.) + 273.16,
            _ => value,
        })
    }

    pub(super) fn into_record(self) -> CasaValue {
        vector(&self.unit, self.values)
    }
}

pub(super) fn vector(unit: &str, values: impl IntoIterator<Item = f64>) -> CasaValue {
    CasaValue::dict([
        ("unit", CasaValue::string(unit)),
        ("value", CasaValue::float64_vector(values)),
    ])
}

pub(super) fn scalar(unit: &str, value: f64) -> CasaValue {
    vector(unit, [value])
}

pub(super) fn empty() -> CasaValue {
    vector("", [])
}
