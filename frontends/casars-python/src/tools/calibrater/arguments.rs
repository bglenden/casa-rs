// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA argument conversions shared by the calibrater methods.

use numpy::PyUntypedArray;
use pyo3::exceptions::{PyOverflowError, PySystemError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBoolMethods, PyBytes, PyComplex, PyFloat, PyInt, PyList, PyString};

use crate::convert::{double_vector_element, integer_vector};

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

pub(super) fn append_double_vector(
    value: &Bound<'_, PyAny>,
    output: &mut Vec<f64>,
    depth: usize,
) -> PyResult<()> {
    if depth > 32 {
        return Err(PyValueError::new_err("numeric vector is too deeply nested"));
    }
    if value.is_instance_of::<PyUntypedArray>() {
        let values = value
            .call_method1("ravel", ("F",))?
            .call_method0("tolist")?;
        return append_double_vector(&values, output, depth + 1);
    }
    if value.is_instance_of::<PyList>() {
        for item in value.try_iter()? {
            append_double_vector(&item?, output, depth + 1)?;
        }
    } else if let Ok(complex) = value.cast::<PyComplex>() {
        output.push(complex.real());
    } else {
        output.push(double_vector_element(value)?);
    }
    Ok(())
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

pub(super) fn listing_page_rows(value: Option<&Bound<'_, PyAny>>) -> PyResult<i32> {
    let Some(value) = value else { return Ok(50) };
    if value.is_instance_of::<PyInt>()
        || value.is_instance(&value.py().import("numpy")?.getattr("integer")?)?
    {
        return value.extract::<i64>().map(|rows| rows as i32).map_err(|_| {
            PyOverflowError::new_err("pagerows does not fit in a signed 64-bit integer")
        });
    }
    Err(PyTypeError::new_err(
        "argument _pagerows must be an integer",
    ))
}

pub(super) use crate::convert::selector::listing_selector;

pub(super) fn window_map(
    value: Option<Bound<'_, PyAny>>,
    method: &str,
    default: &[i32],
) -> PyResult<Vec<i32>> {
    let (windows, pending) = window_map_with_status(value, default)?;
    if pending {
        return Err(PySystemError::new_err(format!(
            "<built-in function calibrater_{method}> returned a result with an exception set"
        )));
    }
    Ok(windows)
}

pub(super) fn window_map_with_status(
    value: Option<Bound<'_, PyAny>>,
    default: &[i32],
) -> PyResult<(Vec<i32>, bool)> {
    let Some(value) = value else {
        return Ok((default.to_vec(), false));
    };
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(
            "argument _spwmap must not be a string",
        ));
    }
    if !value.is_instance_of::<PyList>()
        && !value.is_instance_of::<PyUntypedArray>()
        && !value.is_instance_of::<PyInt>()
        && !value.is_instance_of::<PyFloat>()
    {
        return Ok((Vec::new(), true));
    }
    let (windows, pending) = integer_vector::flatten(
        &value,
        "cyclic spectral-window mapping",
        "non-finite spectral-window mapping",
    )?;
    Ok((
        windows.into_iter().map(|window| window as i32).collect(),
        pending,
    ))
}
