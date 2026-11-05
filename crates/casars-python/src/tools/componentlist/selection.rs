// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's integer-vector conversion for component selection.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyString, PyTuple};

/// Convert an index argument before releasing the GIL. The flag records an
/// exception CASA's binding leaves pending even when its tool call succeeds.
pub(super) fn indices(value: &Bound<'_, PyAny>) -> PyResult<(Vec<i64>, bool)> {
    if value.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err("argument _which must not be a string"));
    }
    if value.is_none() {
        return Ok((Vec::new(), true));
    }
    let mut values = Vec::new();
    let mut pending = false;
    append(value, false, &mut values, &mut pending)?;
    Ok((values, pending))
}

fn append(
    value: &Bound<'_, PyAny>,
    nested: bool,
    values: &mut Vec<i64>,
    pending: &mut bool,
) -> PyResult<()> {
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        for item in value.try_iter()? {
            append(&item?, true, values, pending)?;
        }
        return Ok(());
    }
    let ndarray = value.py().import("numpy")?.getattr("ndarray")?;
    if value.is_instance(&ndarray)? {
        for item in value.call_method0("ravel")?.try_iter()? {
            append(&item?, true, values, pending)?;
        }
        return Ok(());
    }
    if !nested
        && !value.is_instance_of::<PyInt>()
        && matches!(
            value
                .getattr("dtype")
                .and_then(|d| d.getattr("kind"))
                .and_then(|k| k.extract::<String>())
                .as_deref(),
            Ok("i" | "u")
        )
    {
        *pending = true;
        return Ok(());
    }
    if value.is_none() {
        values.push(0);
    } else if value.is_instance_of::<PyString>() {
        let string = value.extract::<String>()?;
        values.push(number_prefix(&string) as i64 as i32 as i64);
    } else if value.is_instance_of::<PyInt>() {
        match value.extract::<i64>() {
            Ok(number) => values.push(i64::from(number as i32)),
            Err(_) => {
                values.push(-1);
                *pending = true;
            }
        }
    } else if let Ok(number) = value.extract::<i64>() {
        values.push(i64::from(number as i32));
    } else if let Ok(number) = value.extract::<u64>() {
        values.push(i64::from(number as i32));
    } else if let Ok(number) = value.extract::<f64>() {
        if !nested && !number.is_finite() {
            return Err(PyValueError::new_err(
                "non-finite component selection (CASA crash: #1176)",
            ));
        }
        values.push(i64::from(number as i64 as i32));
    } else {
        values.push(0);
    }
    Ok(())
}

/// casatools reads numeric prefixes in string elements, defaulting to zero.
fn number_prefix(value: &str) -> f64 {
    let value = value.trim_start();
    for end in (1..=value.len())
        .rev()
        .filter(|&end| value.is_char_boundary(end))
    {
        if let Ok(number) = value[..end].parse::<f64>() {
            return number;
        }
    }
    0.0
}
