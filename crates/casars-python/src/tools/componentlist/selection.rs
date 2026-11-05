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
    let bytes = value.as_bytes();
    let mut i = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let sign = if bytes.first() == Some(&b'-') {
        -1.0
    } else {
        1.0
    };
    let rest = &value[i..];
    if rest
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("inf"))
    {
        return sign * f64::INFINITY;
    }
    if rest
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("nan"))
    {
        return f64::NAN;
    }
    let mut digits = 0;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return 0.0;
    }
    let mut end = i;
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let exponent_start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i > exponent_start {
            end = i;
        }
    }
    value[..end].parse::<f64>().unwrap_or(0.0)
}
