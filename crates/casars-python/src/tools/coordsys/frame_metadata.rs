// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, RecordCoordinate};
use casa_types::{ArrayValue, Value};
use ndarray::Axis;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::holder::{DIRECTION_CODES, FREQUENCY_CODES};
use crate::convert::value::CasaValue;

use super::inspect::{coordinate_type, numbered_coordinates};
use super::{CoordinateState, PyCoordsys};

/// Names and parameter counts from casacore's `Projection::nParameters`.
pub(super) const PROJECTIONS: &[(&str, i64)] = &[
    ("AZP", 2),
    ("SZP", 3),
    ("TAN", 0),
    ("SIN", 2),
    ("STG", 0),
    ("ARC", 0),
    ("ZPN", 20),
    ("ZEA", 0),
    ("AIR", 1),
    ("CYP", 2),
    ("CAR", 0),
    ("MER", 0),
    ("CEA", 1),
    ("COP", 2),
    ("COD", 2),
    ("COE", 2),
    ("COO", 2),
    ("BON", 1),
    ("PCO", 0),
    ("SFL", 0),
    ("PAR", 0),
    ("AIT", 0),
    ("MOL", 0),
    ("CSC", 0),
    ("QSC", 0),
    ("TSC", 0),
    ("HPX", 0),
];

pub(super) fn coordinate(
    state: &CoordinateState,
    kind: CoordinateType,
) -> PyResult<Option<(&CoordinateModel, RecordCoordinate)>> {
    Ok(numbered_coordinates(state)?
        .into_iter()
        .find(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
        .map(|(_, coordinate, placement)| (coordinate, placement)))
}

pub(super) fn missing(kind: CoordinateType) -> PyErr {
    PyRuntimeError::new_err(format!(
        "No coordinate of type {kind} in this CoordinateSystem"
    ))
}

fn reference(coordinate: &CoordinateModel, conversion: bool) -> &'static str {
    match coordinate {
        CoordinateModel::Direction(direction) if conversion => {
            direction.conversion_direction_ref().as_str()
        }
        CoordinateModel::Direction(direction) => direction.direction_ref().as_str(),
        CoordinateModel::Spectral(spectral) if conversion => {
            match spectral.conversion_frequency_ref() {
                Some(frame) => frame.as_str(),
                None if spectral.has_undefined_frame() => "Undefined",
                None => spectral.frequency_ref().as_str(),
            }
        }
        CoordinateModel::Spectral(spectral) if spectral.has_undefined_frame() => "Undefined",
        CoordinateModel::Spectral(spectral) => spectral.frequency_ref().as_str(),
        _ => "",
    }
}

fn linear_transform(state: &CoordinateState, kind: &str) -> PyResult<CasaValue> {
    if kind.is_empty() {
        return Err(PyRuntimeError::new_err(
            "You must specify the coordinate type",
        ));
    }
    let (model, placement) = coordinate(state, coordinate_type(kind))?
        .ok_or_else(|| PyRuntimeError::new_err("Failed AlwaysAssert which < nCoordinates()"))?;
    let native = state.record.get(&placement.field);
    let scalar = match model {
        CoordinateModel::Spectral(spectral) => Some(spectral.linear_transform()),
        CoordinateModel::Tabular(tabular) => Some(tabular.linear_transform()),
        _ => None,
    };
    if let Some(pc) = scalar {
        return Ok(CasaValue::Floats {
            shape: vec![1, 1],
            values: vec![pc],
        });
    }
    if matches!(model, CoordinateModel::Linear(_))
        && let Some(Value::Record(native)) = native
        && let Some(matrix @ Value::Array(ArrayValue::Float64(array))) = native.get("pc")
        && array.ndim() == 2
    {
        return Ok(CasaValue::from_value(matrix));
    }
    let model_record = model.to_record();
    let Some(Value::Array(ArrayValue::Float64(matrix))) = model_record.get("pc") else {
        return Err(PyRuntimeError::new_err(
            "Coordinate has no linear transform",
        ));
    };
    let mut matrix = matrix.clone();
    if matches!(model, CoordinateModel::Direction(_)) {
        for mut column in matrix.axis_iter_mut(Axis(1)) {
            let norm = column.iter().map(|value| value * value).sum::<f64>().sqrt();
            if norm > 0.0 {
                column.mapv_inplace(|value| value / norm);
            }
        }
    }
    Ok(CasaValue::from_value(&ArrayValue::Float64(matrix).into()))
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (r#type="direction"), text_signature = "($self, type='direction')")]
    fn conversiontype(&self, py: Python<'_>, r#type: &str) -> PyResult<&'static str> {
        self.getconversiontype(py, r#type, true)
    }

    #[pyo3(signature = (r#type="", showconversion=true), text_signature = "($self, type='', showconversion=True)")]
    fn getconversiontype(
        &self,
        py: Python<'_>,
        r#type: &str,
        showconversion: bool,
    ) -> PyResult<&'static str> {
        py.detach(|| {
            let state = self.state();
            Ok(coordinate(&state, coordinate_type(r#type))?
                .map_or("", |(model, _)| reference(model, showconversion)))
        })
    }

    fn lineartransform(&self, py: Python<'_>, r#type: &str) -> PyResult<Py<PyAny>> {
        py.detach(|| linear_transform(&self.state(), r#type))?
            .into_python(py)
    }

    #[pyo3(signature = (r#type=""), text_signature = "($self, type='')")]
    fn projection(&self, py: Python<'_>, r#type: &str) -> PyResult<Py<PyAny>> {
        let value = py.detach(|| {
            let state = self.state();
            let kind = CoordinateType::Direction;
            let Some((CoordinateModel::Direction(direction), _)) = coordinate(&state, kind)? else {
                return Err(missing(kind));
            };
            if r#type.is_empty() {
                let projection = direction.projection();
                return Ok(CasaValue::record([
                    ("type", CasaValue::Text(projection.name().into())),
                    (
                        "parameters",
                        CasaValue::floats(projection.parameters().to_vec()),
                    ),
                ]));
            }
            let requested = r#type.to_ascii_uppercase();
            let prefix = requested.get(..3).unwrap_or(&requested);
            if prefix == "ALL" {
                return Ok(CasaValue::record([(
                    "types",
                    CasaValue::Texts(PROJECTIONS.iter().map(|(name, _)| (*name).into()).collect()),
                )]));
            }
            let prefix = if prefix == "GLS" { "SFL" } else { prefix };
            let (_, count) = PROJECTIONS
                .iter()
                .find(|(name, _)| *name == prefix)
                .ok_or_else(|| {
                    PyRuntimeError::new_err("Projection::nParameters() - unknown projection")
                })?;
            Ok(CasaValue::record([("nparameters", CasaValue::Int(*count))]))
        })?;
        value.into_python(py)
    }

    #[pyo3(signature = (r#type="", list=false), text_signature = "($self, type='', list=False)")]
    fn referencecode(&self, py: Python<'_>, r#type: &str, list: bool) -> PyResult<Vec<String>> {
        py.detach(|| {
            let state = self.state();
            if r#type.is_empty() {
                return Ok(numbered_coordinates(&state)?
                    .into_iter()
                    .map(|(_, model, _)| reference(model, false).into())
                    .collect());
            }
            if list {
                let prefix = r#type.to_ascii_uppercase();
                let values = match prefix.as_bytes() {
                    [b'D', b'I', ..] => DIRECTION_CODES.all,
                    [b'S', b'P', ..] => FREQUENCY_CODES.all,
                    _ => &[],
                };
                return Ok(values.iter().map(|value| (*value).into()).collect());
            }
            let kind = coordinate_type(r#type);
            if matches!(kind, CoordinateType::Direction | CoordinateType::Spectral) {
                let (model, _) = coordinate(&state, kind)?.ok_or_else(|| missing(kind))?;
                Ok(vec![reference(model, false).into()])
            } else {
                Ok(vec![String::new()])
            }
        })
    }

    fn restfrequency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = py.detach(|| {
            let state = self.state();
            let kind = CoordinateType::Spectral;
            let Some((CoordinateModel::Spectral(spectral), _)) = coordinate(&state, kind)? else {
                return Err(missing(kind));
            };
            let active = spectral.rest_frequency();
            let mut frequencies =
                std::iter::once(active)
                    .chain(spectral.rest_frequencies().iter().copied().filter(|value| {
                        (value - active).abs() > 1e-13 * value.abs().max(active.abs())
                    }))
                    .collect::<Vec<_>>();
            frequencies.resize(spectral.rest_frequencies().len().max(1), 0.0);
            let unit = spectral.axis_units().into_iter().next().unwrap_or_default();
            Ok(CasaValue::record([
                ("unit", CasaValue::Text(unit)),
                ("value", CasaValue::floats(frequencies)),
            ]))
        })?;
        value.into_python(py)
    }
}
