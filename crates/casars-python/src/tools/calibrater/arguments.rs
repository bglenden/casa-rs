// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA argument conversions shared by the calibrater methods.

use numpy::PyUntypedArray;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBoolMethods, PyBytes, PyFloat, PyInt, PyList, PyString};

use crate::convert::integer_vector;

pub(super) fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(Some(value.clone()))
}

pub(super) fn strict_bool(value: &Bound<'_, PyAny>, method: &str, position: u8) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|value| value.is_true())
        .map_err(|_| {
            PyTypeError::new_err(format!(
                "in method 'calibrater_{method}', argument {position} of type 'bool'"
            ))
        })
}

pub(super) fn number(
    value: Option<Bound<'_, PyAny>>,
    method: &str,
    position: u8,
    default: f64,
) -> PyResult<f64> {
    match value {
        None => Ok(default),
        Some(value) => value.extract::<f64>().map_err(|_| {
            PyTypeError::new_err(format!(
                "in method 'calibrater_{method}', argument {position} of type 'double'"
            ))
        }),
    }
}

pub(super) fn text(value: Option<Bound<'_, PyAny>>, name: &str, default: &str) -> PyResult<String> {
    match value {
        None => Ok(default.to_owned()),
        Some(value) if value.is_instance_of::<PyString>() => value.extract(),
        Some(value) if value.is_instance_of::<PyBytes>() => {
            value.call_method1("decode", ("utf-8",))?.extract()
        }
        Some(_) => Err(PyTypeError::new_err(format!(
            "argument _{name} must be a string"
        ))),
    }
}

pub(super) fn window_map(
    value: Option<Bound<'_, PyAny>>,
    method: &str,
    default: &[i32],
) -> PyResult<Vec<i32>> {
    let Some(value) = value else {
        return Ok(default.to_vec());
    };
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(
            "argument _spwmap must not be a string",
        ));
    }
    let system_error = || {
        PySystemError::new_err(format!(
            "<built-in function calibrater_{method}> returned a result with an exception set"
        ))
    };
    if !value.is_instance_of::<PyList>()
        && !value.is_instance_of::<PyUntypedArray>()
        && !value.is_instance_of::<PyInt>()
        && !value.is_instance_of::<PyFloat>()
    {
        return Err(system_error());
    }
    let (windows, pending) = integer_vector::flatten(
        &value,
        "cyclic spectral-window mapping",
        "non-finite spectral-window mapping",
    )?;
    if pending {
        return Err(system_error());
    }
    Ok(windows.into_iter().map(|window| window as i32).collect())
}
