// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_types::quanta::Unit;
use casa_types::{ArrayD, ArrayValue, ScalarValue, Value};
use ndarray::{Axis, Ix2};
use numpy::{PyArrayDescrMethods, PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::convert::value::CasaValue;
use crate::tools::table::casa_record;

use super::{PyCoordsys, inspect::axis_values};

fn raised(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

#[derive(Clone, Copy)]
pub(super) enum Output {
    World,
    Pixel,
}

impl Output {
    fn wrong_type(self, actual: &str) -> PyErr {
        match self {
            Self::World => raised("You must provide a vector of doubles."),
            Self::Pixel => raised(format!("getDoubleVec called for type {actual} variant")),
        }
    }

    fn input_name(self) -> &'static str {
        match self {
            Self::World => "pixel",
            Self::Pixel => "world",
        }
    }

    fn output_name(self) -> &'static str {
        match self {
            Self::World => "world",
            Self::Pixel => "pixel",
        }
    }
}

pub(super) fn capture(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    output: Output,
) -> PyResult<ArrayD<f64>> {
    let value = value
        .filter(|value| !value.is_none())
        .ok_or_else(|| output.wrong_type("boolvec"))?;
    if value.cast::<PyDict>().is_ok() {
        return Err(output.wrong_type("ErRoR"));
    }
    if let Ok(array) = value.cast::<PyUntypedArray>() {
        // The CASA wrapper distinguishes C long (an integer variant) from
        // C long long (a double variant), even where both are 64 bits.
        match array.dtype().char() {
            b'q' | b'Q' | b'f' | b'd' | b'g' => {
                let options = PyDict::new(py);
                options.set_item("copy", false)?;
                let doubles = value.call_method("astype", ("float64",), Some(&options))?;
                let array = doubles.extract::<PyReadonlyArrayDyn<'_, f64>>()?;
                return Ok(array.as_array().to_owned());
            }
            b'b' | b'B' | b'h' | b'H' | b'i' | b'I' | b'l' | b'L' => {
                return Err(output.wrong_type("intvec"));
            }
            b'S' | b'U' => return Err(output.wrong_type("stringvec")),
            b'F' | b'D' | b'G' => return Err(output.wrong_type("ErRoR")),
            _ => return Err(output.wrong_type("boolvec")),
        }
    }
    if (value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>())
        && value
            .try_iter()?
            .any(|row| row.is_ok_and(|row| row.cast::<PyUntypedArray>().is_ok()))
    {
        return Err(output.wrong_type("boolvec"));
    }
    // For Python sequences keep the inferred type and axis order.
    let input = PyDict::new(py);
    input.set_item("coordinates", value)?;
    let raw = casa_record(py, Some(&input)).map_err(|_| output.wrong_type("ErRoR"))?;
    let mut fields = raw.into_fields();
    match fields.pop().map(|field| field.value) {
        Some(Value::Array(ArrayValue::Float64(values))) if !values.is_empty() => Ok(values),
        Some(Value::Array(ArrayValue::Int32(_) | ArrayValue::Int64(_))) => {
            Err(output.wrong_type("intvec"))
        }
        Some(Value::Array(ArrayValue::Bool(_) | ArrayValue::Float64(_))) | None => {
            Err(output.wrong_type("boolvec"))
        }
        Some(Value::Array(ArrayValue::String(_))) => Err(output.wrong_type("stringvec")),
        Some(Value::Scalar(ScalarValue::Bool(_))) => Err(output.wrong_type("bool")),
        Some(Value::Scalar(ScalarValue::Int32(_) | ScalarValue::Int64(_))) => {
            Err(output.wrong_type("int"))
        }
        Some(Value::Scalar(ScalarValue::Float64(_))) => Err(output.wrong_type("double")),
        Some(Value::Scalar(ScalarValue::String(_))) => Err(output.wrong_type("string")),
        _ => Err(output.wrong_type("ErRoR")),
    }
}

pub(super) fn world_scale(coordinates: &CoordinateSystem) -> PyResult<Vec<f64>> {
    axis_values(coordinates, true, CoordinateModel::declared_axis_units)
        .into_iter()
        .zip(coordinates.world_axis_units())
        .map(|(declared, native)| {
            if declared == native {
                return Ok(1.0);
            }
            let declared = Unit::new(&declared).map_err(|error| raised(error.to_string()))?;
            let native = Unit::new(&native).map_err(|error| raised(error.to_string()))?;
            Ok(native.val().factor / declared.val().factor)
        })
        .collect()
}

fn transform(
    coordinates: &CoordinateSystem,
    values: ArrayD<f64>,
    output: Output,
) -> PyResult<CasaValue> {
    let input_name = output.input_name();
    let mut values = values.into_dimensionality::<Ix2>().map_err(|_| {
        raised(format!(
            "Failed AlwaysAssert {input_name}.shape().nelements()==2"
        ))
    })?;
    let (input_count, output_count) = match output {
        Output::World => (coordinates.n_pixel_axes(), coordinates.n_world_axes()),
        Output::Pixel => (coordinates.n_world_axes(), coordinates.n_pixel_axes()),
    };
    if values.nrows() != input_count {
        let kind = match output {
            Output::World => "Pixel",
            Output::Pixel => "World",
        };
        return Err(raised(format!(
            "Failed AlwaysAssert n{kind}Axes()=={input_name}.nrow()"
        )));
    }
    let scale = world_scale(coordinates)?;
    let converted = match output {
        Output::World => {
            let mut world = coordinates
                .to_world_columns(values.view())
                .map_err(|error| raised(error.to_string()))?;
            for (mut row, scale) in world.axis_iter_mut(Axis(0)).zip(scale) {
                if scale != 1.0 {
                    row.mapv_inplace(|value| value * scale);
                }
            }
            world.into_raw_vec_and_offset().0
        }
        Output::Pixel => {
            for (mut row, scale) in values.axis_iter_mut(Axis(0)).zip(scale) {
                if scale != 1.0 {
                    row.mapv_inplace(|value| value / scale);
                }
            }
            coordinates
                .to_pixel_columns_or_nan(values.view())
                .map_err(|error| raised(error.to_string()))?
                .into_raw_vec_and_offset()
                .0
        }
    };
    Ok(CasaValue::record([
        (
            "numeric",
            CasaValue::Floats {
                shape: vec![output_count, values.ncols()],
                values: converted,
            },
        ),
        ("pw_type", CasaValue::Text(output.output_name().into())),
        ("ar_type", CasaValue::Text("absolute".into())),
    ]))
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value=None), text_signature = "($self, value=[])")]
    fn toworldmany(&self, py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let values = capture(py, value, Output::World)?;
        py.detach(|| transform(&self.state().coordinates, values, Output::World))?
            .into_python(py)
    }

    #[pyo3(signature = (value=None), text_signature = "($self, value=[])")]
    fn topixelmany(&self, py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let values = capture(py, value, Output::Pixel)?;
        py.detach(|| transform(&self.state().coordinates, values, Output::Pixel))?
            .into_python(py)
    }
}
