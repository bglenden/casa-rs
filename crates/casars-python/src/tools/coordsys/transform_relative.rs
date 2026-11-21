// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_types::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Ix2;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use crate::convert::value::CasaValue;
use crate::tools::table::casa_record;

use super::inspect::axis_values;
use super::transform_many::{self, Output, world_scale};
use super::transform_single::{WorldArgument, numeric_record_value};
use super::world_reference::format_world_offsets;
use super::{CoordinateState, PyCoordsys};

fn raised(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn input_record(py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<RecordValue>> {
    value
        .and_then(|value| value.cast::<PyDict>().ok())
        .map(|record| casa_record(py, Some(record)))
        .transpose()
}

fn text_field<'a>(record: &'a RecordValue, name: &str) -> Option<&'a str> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value),
        _ => None,
    }
}

#[derive(Clone, Copy)]
pub(super) enum OutputFrame {
    Absolute,
    Relative,
}

impl OutputFrame {
    fn absolute(self) -> bool {
        matches!(self, Self::Absolute)
    }

    fn name(self) -> &'static str {
        if self.absolute() {
            "absolute"
        } else {
            "relative"
        }
    }

    fn difference(self, value: f64, reference: f64) -> f64 {
        if self.absolute() {
            value + reference
        } else {
            value - reference
        }
    }

    fn world_input(
        self,
        value: Option<&Bound<'_, PyAny>>,
        record: Option<&RecordValue>,
        isworld: i64,
    ) -> PyResult<Option<bool>> {
        let tag = record.and_then(|record| text_field(record, "pw_type"));
        if tag.is_none() && record.is_some_and(|record| record.get("pw_type").is_some()) {
            return Err(raised(
                "RecordRep::get_pointer - incorrect data type for field pw_type",
            ));
        }
        let is_world = match isworld {
            -1 => tag.map(|tag| tag == "world").or_else(|| {
                (record.is_some()
                    || value.is_some_and(|value| {
                        value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>()
                    }))
                .then_some(true)
            }),
            0 if record.is_some() && tag != Some("pixel") => None,
            _ => Some(isworld != 0),
        };
        let Some(is_world) = is_world else {
            return if self.absolute() {
                Ok(None)
            } else {
                Err(raised("Should be world value in coordsys.isValueWorld"))
            };
        };
        if let Some(tag) = record.and_then(|record| text_field(record, "ar_type"))
            && tag == self.name()
        {
            let expected = if self.absolute() {
                "relative"
            } else {
                "absolute"
            };
            return Err(raised(format!(
                "The value is {tag}, not {expected}', origin='coordsys.checkAbsRel"
            )));
        }
        Ok(Some(is_world))
    }

    fn record(self, numeric: CasaValue, world: bool) -> CasaValue {
        CasaValue::record([
            ("numeric", numeric),
            (
                "pw_type",
                CasaValue::Text(if world { "world" } else { "pixel" }.into()),
            ),
            ("ar_type", CasaValue::Text(self.name().into())),
        ])
    }

    fn defaults(self, coordinates: &CoordinateSystem, world: bool) -> Vec<f64> {
        if self.absolute() {
            vec![
                0.0;
                if world {
                    coordinates.n_world_axes()
                } else {
                    coordinates.n_pixel_axes()
                }
            ]
        } else if world {
            axis_values(coordinates, true, CoordinateModel::reference_value)
        } else {
            axis_values(coordinates, false, CoordinateModel::reference_pixel)
        }
    }

    pub(super) fn apply(
        self,
        coordinates: &CoordinateSystem,
        values: &mut [f64],
        world: bool,
    ) -> PyResult<()> {
        if world {
            return coordinates
                .convert_world_offsets(values, self.absolute())
                .map_err(|error| raised(error.to_string()));
        }

        for index in 0..coordinates.n_coordinates() {
            let model = coordinates.coordinate(index);
            let axes = coordinates
                .coordinate_pixel_axes(index)
                .expect("existing coordinate");
            for (axis, reference) in axes.into_iter().zip(model.reference_pixel()) {
                let Some(axis) = axis else { continue };
                let discrete_planes = match model {
                    CoordinateModel::Stokes(stokes) => Some(stokes.n_stokes()),
                    CoordinateModel::Quality(quality) => Some(quality.values().len()),
                    _ => None,
                };
                if let Some(count) = discrete_planes {
                    let absolute = if self.absolute() {
                        values[axis] + reference
                    } else {
                        values[axis]
                    };
                    let index = (absolute + 0.5).trunc();
                    if !(0.0..count as f64).contains(&index) {
                        return Err(raised(format!(
                            "Absolute pixel {index} is out of range [0..{}]",
                            count.saturating_sub(1)
                        )));
                    }
                }
                values[axis] = self.difference(values[axis], reference);
            }
        }
        Ok(())
    }
}

fn scalar_transform(
    state: &CoordinateState,
    output: OutputFrame,
    world: bool,
    mut argument: WorldArgument,
) -> PyResult<CasaValue> {
    let mut values = output.defaults(&state.coordinates, world);
    if world {
        let format = match &mut argument {
            WorldArgument::Text(_) | WorldArgument::Texts(_) => "s".to_string(),
            WorldArgument::Record(record) => {
                record.remove("ar_type");
                let fields = [
                    ('n', "numeric"),
                    ('q', "quantity"),
                    ('s', "string"),
                    ('m', "measure"),
                ];
                let format = fields
                    .into_iter()
                    .filter_map(|(flag, field)| record.get(field).map(|_| flag))
                    .collect::<String>();
                if format.is_empty() {
                    "m".to_owned()
                } else {
                    format
                }
            }
            _ => "n".to_owned(),
        };
        if !matches!(argument, WorldArgument::Unset) {
            values = argument.values_with_defaults(state, None, values)?;
        }
        output.apply(&state.coordinates, &mut values, true)?;
        format_world_offsets(state, &values, &format, output.absolute())
    } else {
        let supplied = match argument {
            WorldArgument::Unset => Vec::new(),
            WorldArgument::Numeric(values) => values,
            WorldArgument::Record(record) => {
                let numeric = record.get("numeric");
                if record.get("quantity").is_some()
                    || record.get("measure").is_some()
                    || (numeric.is_none() && record.get("string").is_none())
                {
                    return if output.absolute() {
                        Ok(CasaValue::empty_record())
                    } else {
                        Err(raised(
                            "Conversion of value to Record failed in coordinateValueToRecord",
                        ))
                    };
                }
                numeric_record_value(
                    numeric
                        .ok_or_else(|| raised("Input does not appear to be a pixel coordinate"))?,
                )?
            }
            _ => return Err(raised("Input does not appear to be a pixel coordinate")),
        };
        for (slot, value) in values.iter_mut().zip(supplied) {
            *slot = value;
        }
        output.apply(&state.coordinates, &mut values, false)?;
        Ok(output.record(CasaValue::floats(values), false))
    }
}

fn matrix_transform(
    coordinates: &CoordinateSystem,
    values: ArrayD<f64>,
    output: OutputFrame,
    world: bool,
) -> PyResult<CasaValue> {
    let mut values = values
        .into_dimensionality::<Ix2>()
        .map_err(|_| raised("Failed AlwaysAssert valueIn.shape().nelements()==2"))?;
    let scale = if world {
        world_scale(coordinates)?
    } else {
        vec![1.0; coordinates.n_pixel_axes()]
    };
    if values.nrows() < scale.len() {
        return Err(raised(format!(
            "Expected at least {} coordinate rows",
            scale.len()
        )));
    }
    let mut vector = vec![0.0; scale.len()];
    for mut column in values.columns_mut() {
        for (i, unit) in scale.iter().enumerate() {
            vector[i] = column[i] / unit;
        }
        output.apply(coordinates, &mut vector, world)?;
        for (i, unit) in scale.iter().enumerate() {
            column[i] = vector[i] * unit;
        }
    }
    Ok(output.record(
        CasaValue::Floats {
            shape: values.shape().to_vec(),
            values: values.t().iter().copied().collect(),
        },
        world,
    ))
}

fn absolute_matrix(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    record: Option<RecordValue>,
) -> PyResult<ArrayD<f64>> {
    if let Some(mut record) = record {
        return match record.remove("numeric") {
            Some(Value::Array(ArrayValue::Float64(values))) => Ok(values),
            Some(Value::Array(ArrayValue::Float32(values))) => Ok(values.mapv(f64::from)),
            Some(_) => Err(raised(
                "RecordRep::get_pointer - incorrect data type for field numeric",
            )),
            None => Err(raised("unsupported record type for value")),
        };
    }
    transform_many::capture(py, value, Output::Pixel)
        .map_err(|_| raised("unsupported data type for value"))
}

impl PyCoordsys {
    fn relative_scalar(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        isworld: i64,
        output: OutputFrame,
    ) -> PyResult<Py<PyAny>> {
        let record = input_record(py, value)?;
        let Some(world) = output.world_input(value, record.as_ref(), isworld)? else {
            return CasaValue::empty_record().into_python(py);
        };
        let argument = if let Some(record) = record {
            WorldArgument::Record(record)
        } else {
            WorldArgument::capture(py, value).map_err(|error| {
                if world {
                    error
                } else {
                    raised("Input does not appear to be a pixel coordinate")
                }
            })?
        };
        py.detach(|| scalar_transform(&self.state(), output, world, argument))?
            .into_python(py)
    }
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value=None, isworld=-1), text_signature = "($self, value=[], isworld=-1)")]
    fn toabs(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        isworld: i64,
    ) -> PyResult<Py<PyAny>> {
        self.relative_scalar(py, value, isworld, OutputFrame::Absolute)
    }

    #[pyo3(signature = (value=None, isworld=-1), text_signature = "($self, value=[], isworld=-1)")]
    fn torel(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        isworld: i64,
    ) -> PyResult<Py<PyAny>> {
        self.relative_scalar(py, value, isworld, OutputFrame::Relative)
    }

    #[pyo3(signature = (value=None, isworld=-1), text_signature = "($self, value=[], isworld=-1)")]
    fn toabsmany(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        isworld: i64,
    ) -> PyResult<Py<PyAny>> {
        let output = OutputFrame::Absolute;
        let record = input_record(py, value)?;
        let Some(world) = output.world_input(value, record.as_ref(), isworld)? else {
            return CasaValue::empty_record().into_python(py);
        };
        let values = absolute_matrix(py, value, record)?;
        py.detach(|| matrix_transform(&self.state().coordinates, values, output, world))?
            .into_python(py)
    }

    #[pyo3(signature = (value=None, isworld=-1), text_signature = "($self, value=[], isworld=-1)")]
    fn torelmany(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        isworld: i64,
    ) -> PyResult<Py<PyAny>> {
        let values = transform_many::capture(py, value, Output::Pixel)?;
        py.detach(|| {
            matrix_transform(
                &self.state().coordinates,
                values,
                OutputFrame::Relative,
                isworld != 0,
            )
        })?
        .into_python(py)
    }
}
