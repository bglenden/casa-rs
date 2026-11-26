// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType};
use casa_types::quanta::{Quantity, Unit, casacore};
use casa_types::{ArrayValue, ScalarValue, Value};
use numpy::PyReadonlyArrayDyn;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyList, PyTuple};

use crate::convert::{Variant, read_quantity};

use super::PyCoordsys;
use super::frame_metadata::missing;
use super::inspect::numbered_coordinates;
use super::replace::replace_at;

enum Frequencies {
    Numeric(Vec<f64>, Option<String>),
    Text(Vec<String>),
}

impl Frequencies {
    fn capture(value: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
        if let Ok(record) = value.cast::<PyDict>() {
            let (Some(raw), Some(unit)) = (record.get_item("value")?, record.get_item("unit")?)
            else {
                return Ok(None);
            };
            let Ok(unit) = unit.extract::<String>() else {
                return Ok(None);
            };
            return Ok(double_values(&raw, true)?.map(|values| Self::Numeric(values, Some(unit))));
        }
        if let Some(values) = double_values(value, false)? {
            return Ok(Some(Self::Numeric(values, None)));
        }
        Ok(match value.extract::<Variant>() {
            Ok(Variant::Text(value)) => Some(Self::Text(quantity_tokens(&value))),
            Ok(Variant::Texts(values)) => Some(Self::Text(values)),
            _ => None,
        })
    }

    fn in_unit(self, unit: &str) -> PyResult<Vec<f64>> {
        let unit = read_unit(unit)?;
        let (values, from) = match self {
            Self::Numeric(values, None) => return Ok(values),
            Self::Numeric(values, Some(from)) => (values, read_unit(&from)?),
            Self::Text(values) => {
                quantity_vector(&values)?.ok_or_else(|| raised("Value is not a valid frequency"))?
            }
        };
        if casacore::frequency_hz(&Quantity::with_unit(1.0, from.clone())).is_none() {
            return Err(raised("Value is not a valid frequency"));
        }
        Ok(values
            .into_iter()
            .map(|value| casacore::get_value(&Quantity::with_unit(value, from.clone()), &unit))
            .collect())
    }
}

pub(super) fn quantity_tokens(value: &str) -> Vec<String> {
    value
        .split([' ', ','])
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .collect()
}

pub(super) fn quantity_vector(texts: &[String]) -> PyResult<Option<(Vec<f64>, Unit)>> {
    let quantities = text_quantities(texts)?;
    let Some(first) = quantities.first() else {
        return Ok(None);
    };
    let unit = first.unit().clone();
    let values = quantities
        .iter()
        .map(|quantity| casacore::get_value(quantity, &unit))
        .collect();
    Ok(Some((values, unit)))
}

pub(super) fn text_quantities(texts: &[String]) -> PyResult<Vec<Quantity>> {
    texts
        .iter()
        .map(|text| {
            read_quantity(text, &format!(" In converting quantity {text}")).map_err(Into::into)
        })
        .collect()
}

fn raised(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn read_unit(unit: &str) -> PyResult<Unit> {
    casacore::casacore_unit(unit).map_err(|error| raised(error.to_string()))
}

fn double_values(value: &Bound<'_, PyAny>, record_field: bool) -> PyResult<Option<Vec<f64>>> {
    let numpy = value.py().import("numpy")?;
    let sequence = value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>();
    let ndarray = value.is_instance(&numpy.getattr("ndarray")?)?;
    if (sequence && value.len()? == 0)
        || !(sequence
            || ndarray
            || value.is_instance_of::<PyFloat>()
            || (!record_field && value.is_instance(&numpy.getattr("floating")?)?))
    {
        return Ok(None);
    }
    let Ok(array) = numpy.call_method1("asarray", (value,)) else {
        return Ok(None);
    };
    let dtype = array.getattr("dtype")?;
    let kind: String = dtype.getattr("kind")?.extract()?;
    let bytes: usize = dtype.getattr("itemsize")?.extract()?;
    let ndim: usize = array.getattr("ndim")?.extract()?;
    if kind != "f"
        || !matches!((ndim, bytes), (0, 2) | (_, 4 | 8))
        || (record_field && (ndim > 1 || (ndarray && ndim == 0)))
    {
        return Ok(None);
    }
    let array = array.call_method1("astype", ("float64",))?;
    let array = array.extract::<PyReadonlyArrayDyn<'_, f64>>()?;
    Ok(Some(array.as_array().t().iter().copied().collect()))
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value=None, which=0, append=false), text_signature = "($self, value=[], which=0, append=False)")]
    fn setrestfrequency(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        which: i64,
        append: bool,
    ) -> PyResult<bool> {
        let input = value.map(Frequencies::capture).transpose()?.flatten();
        py.detach(|| {
            let mut state = self.state();
            let kind = CoordinateType::Spectral;
            let Some((index, CoordinateModel::Spectral(spectral), placement)) =
                numbered_coordinates(&state)?
                    .into_iter()
                    .find(|(_, model, _)| model.coordinate_type() == kind)
            else {
                return Err(missing(kind));
            };
            let Some(input) = input else {
                return Ok(false);
            };
            let mut frequencies = input.in_unit(&spectral.axis_units()[0])?;
            let which = usize::try_from(which).map_err(|_| {
                raised(format!("Illegal index '{which}' into restfrequency vector"))
            })?;
            if append {
                let mut existing = spectral.rest_frequencies().to_vec();
                existing.append(&mut frequencies);
                frequencies = existing;
            }
            let spectral = spectral
                .clone()
                .with_selected_rest_frequencies(frequencies, which)
                .map_err(|error| raised(error.to_string()))?;
            let Some(Value::Record(mut target)) = state.record.get(&placement.field).cloned()
            else {
                return Err(raised("Spectral coordinate is missing from the record"));
            };
            target.upsert(
                "restfreq",
                Value::Scalar(ScalarValue::Float64(spectral.rest_frequency())),
            );
            target.upsert(
                "restfreqs",
                ArrayValue::from_f64_vec(spectral.rest_frequencies().to_vec()).into(),
            );
            replace_at(
                &mut state,
                index as i32,
                spectral.into(),
                Value::Record(target),
            )?;
            Ok(true)
        })
    }
}
