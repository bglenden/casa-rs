// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::measures::{MPosition, PositionRef, epoch_to_record, position_to_record};
use casa_types::{ScalarValue, Value};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyComplex, PyDict, PyFloat, PyInt, PyList, PyModule, PyTuple};

use crate::convert::RecordArg;
use crate::tools::measures::observation_epoch;
use crate::tools::{LogLine, emit};

use super::PyCoordsys;

fn text(value: &Bound<'_, PyAny>) -> PyResult<String> {
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _value must be a string"))
}

fn epoch_input<'py>(
    value: &Bound<'py, PyAny>,
    numpy: &Bound<'py, PyModule>,
    numpy_scalar: &Bound<'py, PyAny>,
    depth: usize,
) -> PyResult<Bound<'py, PyDict>> {
    if depth > 128 {
        return Err(PyRuntimeError::new_err(
            "Epoch offsets are too deeply nested",
        ));
    }
    let record = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("_value is not a dictionary"))?;
    let output = record.copy()?;
    let py = record.py();
    for key in ["m0", "m1", "m2"] {
        let Some(quantity) = record.get_item(key)? else {
            break;
        };
        let Ok(quantity) = quantity.cast::<PyDict>() else {
            break;
        };
        let Some(value) = quantity.get_item("value")? else {
            continue;
        };
        let normalized = if let Ok(array) = value.cast::<PyUntypedArray>() {
            let kind = array.dtype().char();
            match kind {
                b'b' | b'B' | b'h' | b'H' | b'i' | b'I' | b'l' | b'L' if array.ndim() != 0 => {
                    return Err(PyRuntimeError::new_err(
                        "Cannot convert to QuantumVectorDouble",
                    ));
                }
                b'b' | b'B' | b'h' | b'H' | b'i' | b'I' | b'l' | b'L' | b'q' | b'Q' | b'f'
                | b'd' | b'g'
                    if array.ndim() == 0 =>
                {
                    Some(0.0f64.into_bound_py_any(py)?)
                }
                b'q' | b'Q' | b'g' => Some(value.call_method1("astype", ("float64",))?),
                b'f' | b'd' => None,
                _ => Some(py.None().into_bound(py)),
            }
        } else if value.is_instance_of::<PyComplex>() {
            return Err(PyRuntimeError::new_err(
                "Wrong QuantumHolder to convert asQuantumVectorDouble",
            ));
        } else if value.is_instance_of::<PyInt>() && !value.is_instance_of::<PyBool>() {
            let integer = value.extract::<i64>()? as i32;
            Some(integer.into_bound_py_any(py)?)
        } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            let Ok(sequence) = numpy.call_method1("asarray", (&value,)) else {
                continue;
            };
            let array = sequence.cast::<PyUntypedArray>()?;
            match array.dtype().kind() {
                b'i' | b'u' if !array.is_empty() => {
                    return Err(PyRuntimeError::new_err(
                        "Cannot convert to QuantumVectorDouble",
                    ));
                }
                b'f' if !array.is_empty() => Some(sequence.call_method1("astype", ("float64",))?),
                _ => None,
            }
        } else if !value.is_instance_of::<PyFloat>() && value.is_instance(numpy_scalar)? {
            Some(py.None().into_bound(py))
        } else {
            None
        };
        if let Some(normalized) = normalized {
            let quantity = quantity.copy()?;
            quantity.set_item("value", normalized)?;
            output.set_item(key, quantity)?;
        }
    }
    if let Some(offset) = record.get_item("offset")?
        && offset.is_instance_of::<PyDict>()
    {
        output.set_item(
            "offset",
            epoch_input(&offset, numpy, numpy_scalar, depth + 1)?,
        )?;
    }
    Ok(output)
}

#[pymethods]
impl PyCoordsys {
    fn setepoch(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let numpy = py.import("numpy")?;
        let numpy_scalar = numpy.getattr("generic")?;
        let value = epoch_input(value, &numpy, &numpy_scalar, 0)?.extract::<RecordArg>()?;
        let (changed, log) = py.detach(|| {
            let mut log = Vec::new();
            let epoch = observation_epoch(&value, &mut log).map_err(PyRuntimeError::new_err)?;
            let Some(epoch) = epoch else {
                return Ok::<_, PyErr>((false, log));
            };
            let mut state = self.state();
            state
                .record
                .upsert("obsdate", epoch_to_record(&epoch).into());
            state.coordinates.obs_info_mut().date = Some(epoch);
            Ok((true, log))
        })?;
        emit(py, "casars.tools.coordsys", &log)?;
        Ok(changed)
    }

    fn setobserver(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let observer = text(value)?;
        py.detach(|| {
            let mut state = self.state();
            state.coordinates.obs_info_mut().observer = observer.clone();
            state
                .record
                .upsert("observer", Value::Scalar(ScalarValue::String(observer)));
        });
        Ok(true)
    }

    fn settelescope(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let telescope = text(value)?;
        let found = py.detach(|| {
            let measures = casa_ms::open_measures_runtime()
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let position = MPosition::from_observatory_name(&telescope, measures.as_ref())
                .and_then(|position| {
                    position
                        .map(|position| position.convert_to(PositionRef::ITRF))
                        .transpose()
                })
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let mut state = self.state();
            state.coordinates.obs_info_mut().telescope = telescope.clone();
            state
                .record
                .upsert("telescope", Value::Scalar(ScalarValue::String(telescope)));
            if let Some(position) = position {
                state
                    .record
                    .upsert("telescopeposition", position_to_record(&position).into());
                state.coordinates.obs_info_mut().telescope_position = Some(position);
                Ok::<_, PyErr>(true)
            } else {
                Ok(false)
            }
        })?;
        if !found {
            emit(
                py,
                "casars.tools.coordsys",
                &[LogLine::warning(
                    "This telescope and its position is not known to the casapy system.",
                )],
            )?;
        }
        Ok(found)
    }
}
