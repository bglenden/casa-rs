// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::{c_char, c_long};
use std::ptr;

use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyInt, PySequence, PyString};

use crate::convert::{double_vector_element as double, numeric_text};

pub(super) struct NoiseParameters(pub(super) Vec<f64>);
pub(super) struct NoiseSeeds(pub(super) Vec<i64>);

impl<'a, 'py> FromPyObject<'a, 'py> for NoiseParameters {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Ok(Self(Vec::new()));
        }
        numbers(&value, "pars", double).map(Self)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for NoiseSeeds {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(seed_error());
        }
        numbers(&value, "seeds", seed).map(Self)
    }
}

fn numbers<T>(
    value: &Bound<'_, PyAny>,
    name: &str,
    scalar: impl Fn(&Bound<'_, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(format!(
            "argument _{name} must not be a string"
        )));
    }
    if value.cast::<PyUntypedArray>().is_ok() {
        return array_numbers(value, scalar);
    }
    if value.is_instance_of::<PySequence>() {
        let items = value.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        if items.iter().any(|item| {
            item.cast::<PyUntypedArray>().is_ok()
                || (item.is_instance_of::<PySequence>()
                    && !item.is_instance_of::<PyString>()
                    && !item.is_instance_of::<PyBytes>())
        }) {
            let array = value
                .py()
                .import("numpy")?
                .call_method1("asarray", (value,))?;
            return array_numbers(&array, scalar);
        }
        return items.iter().map(scalar).collect();
    }
    Ok(vec![scalar(value)?])
}

fn array_numbers<T>(
    value: &Bound<'_, PyAny>,
    scalar: impl Fn(&Bound<'_, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let array = value.cast::<PyUntypedArray>()?;
    if !matches!(
        array.dtype().char(),
        b'?' | b'b'
            | b'B'
            | b'h'
            | b'H'
            | b'i'
            | b'I'
            | b'l'
            | b'L'
            | b'q'
            | b'Q'
            | b'f'
            | b'd'
            | b'g'
            | b'S'
            | b'U'
    ) {
        return Ok(Vec::new());
    }
    let flat = value
        .call_method1("ravel", ("F",))?
        .call_method0("tolist")?;
    flat.try_iter()?.map(|item| scalar(&item?)).collect()
}

fn seed(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if let Some(text) = numeric_text(value)? {
        // CASA converts a string vector through 32-bit decimal integers.
        // SAFETY: text is NUL-terminated and no end pointer was requested.
        return Ok(i64::from(
            unsafe { strtol(text.as_ptr(), ptr::null_mut(), 10) } as i32,
        ));
    }
    if let Ok(integer) = value.extract::<i64>() {
        return Ok(integer);
    }
    if value.is_instance_of::<PyInt>() {
        return Err(seed_error());
    }
    let number = double(value)?;
    Ok(if (i64::MIN as f64..-(i64::MIN as f64)).contains(&number) {
        number as i64
    } else {
        i64::MIN
    })
}

unsafe extern "C" {
    fn strtol(text: *const c_char, end: *mut *mut c_char, base: i32) -> c_long;
}

fn seed_error() -> PyErr {
    PySystemError::new_err(
        "<built-in function image_addnoise> returned a result with an exception set",
    )
}
