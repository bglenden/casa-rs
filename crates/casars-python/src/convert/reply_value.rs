// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a task returns, as Python receives it (#931): the typed reply's
//! [`CasaValue`] rebuilt as CASA's own Python types, so `imstat`'s record
//! holds NumPy arrays of CASA's dtypes and shapes and `flagdata`'s summary
//! keeps its integer keys.

use casa_provider_contracts::{CasaKey, CasaValue, ComplexDtype, FloatDtype, IntDtype};
use num_complex::{Complex32, Complex64};
use numpy::{Element, IntoPyArray};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::value::fortran_array;

/// `value` as a Python object.
pub(crate) fn reply_value(py: Python<'_>, value: &CasaValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        CasaValue::None => py.None(),
        CasaValue::Bool { value } => value.into_pyobject(py)?.to_owned().into_any().unbind(),
        CasaValue::Int { value } => value.into_pyobject(py)?.into_any().unbind(),
        CasaValue::Float { value } => value.0.into_pyobject(py)?.into_any().unbind(),
        CasaValue::String { value } => value.into_pyobject(py)?.into_any().unbind(),
        CasaValue::List { items } => {
            let items = items
                .iter()
                .map(|item| reply_value(py, item))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, items)?.into_any().unbind()
        }
        CasaValue::Dict { entries } => {
            let dict = PyDict::new(py);
            for entry in entries {
                let value = reply_value(py, &entry.value)?;
                match &entry.key {
                    CasaKey::Int(key) => dict.set_item(key, value)?,
                    CasaKey::String(key) => dict.set_item(key, value)?,
                }
            }
            dict.into_any().unbind()
        }
        CasaValue::BoolArray { shape, data } => array(py, shape, data.clone())?,
        CasaValue::IntArray { dtype, shape, data } => match dtype {
            IntDtype::Int32 => {
                let narrowed = data
                    .iter()
                    .map(|&value| {
                        i32::try_from(value).map_err(|_| {
                            PyValueError::new_err(format!("{value} does not fit an int32 array"))
                        })
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                array(py, shape, narrowed)?
            }
            IntDtype::Int64 => array(py, shape, data.clone())?,
        },
        CasaValue::FloatArray { dtype, shape, data } => match dtype {
            FloatDtype::Float32 => array(
                py,
                shape,
                data.iter().map(|value| value.0 as f32).collect(),
            )?,
            FloatDtype::Float64 => array(py, shape, data.iter().map(|value| value.0).collect())?,
        },
        CasaValue::ComplexArray { dtype, shape, data } => match dtype {
            ComplexDtype::Complex64 => array(
                py,
                shape,
                data.iter()
                    .map(|[re, im]| Complex32::new(re.0 as f32, im.0 as f32))
                    .collect(),
            )?,
            ComplexDtype::Complex128 => array(
                py,
                shape,
                data.iter()
                    .map(|[re, im]| Complex64::new(re.0, im.0))
                    .collect(),
            )?,
        },
        CasaValue::StrArray { shape, data } => {
            let numpy = py.import("numpy")?;
            let keywords = PyDict::new(py);
            keywords.set_item("dtype", "str")?;
            let flat = numpy.call_method("array", (PyList::new(py, data)?,), Some(&keywords))?;
            let order = PyDict::new(py);
            order.set_item("order", "F")?;
            flat.call_method("reshape", (shape.clone(),), Some(&order))?
                .unbind()
        }
    })
}

/// Column-major `values` as a NumPy array of `shape`.
fn array<T: Element>(py: Python<'_>, shape: &[usize], values: Vec<T>) -> PyResult<Py<PyAny>> {
    Ok(fortran_array(shape.to_vec(), values)?
        .into_pyarray(py)
        .into_any()
        .unbind())
}
