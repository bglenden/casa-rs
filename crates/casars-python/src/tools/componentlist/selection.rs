// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's integer-vector conversion for component selection.

use std::collections::HashSet;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyString, PyTuple};

/// A pending sequence element or the end of a sequence being traversed.
enum Frame<'py> {
    Value(Bound<'py, PyAny>, bool),
    Leave(usize),
}

/// Convert an index argument before releasing the GIL. The flag records an
/// exception CASA's binding leaves pending even when its tool call succeeds.
pub(super) fn indices(value: &Bound<'_, PyAny>) -> PyResult<(Vec<i64>, bool)> {
    if value.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err("argument _which must not be a string"));
    }
    if value.is_none() {
        return Ok((Vec::new(), true));
    }
    let ndarray = value.py().import("numpy")?.getattr("ndarray")?;
    let mut frames = vec![Frame::Value(value.clone(), false)];
    let mut active = HashSet::new();
    let mut values = Vec::new();
    let mut pending = false;
    while let Some(frame) = frames.pop() {
        let (value, nested) = match frame {
            Frame::Value(value, nested) => (value, nested),
            Frame::Leave(identity) => {
                active.remove(&identity);
                continue;
            }
        };
        let sequence = value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>();
        let array = value.is_instance(&ndarray)?;
        if sequence || array {
            let identity = value.as_ptr() as usize;
            if !active.insert(identity) {
                return Err(PyValueError::new_err(
                    "cyclic component selection (CASA crash: #1176)",
                ));
            }
            let items = if array {
                value.call_method0("ravel")?
            } else {
                value.clone()
            };
            let items = items.try_iter()?.collect::<PyResult<Vec<_>>>()?;
            frames.push(Frame::Leave(identity));
            frames.extend(items.into_iter().rev().map(|item| Frame::Value(item, true)));
            continue;
        }
        if !nested
            && !value.is_instance_of::<PyInt>()
            && matches!(
                value
                    .getattr("dtype")
                    .and_then(|d| d.getattr("kind"))
                    .and_then(|k| k.extract::<String>())
                    .as_deref(),
                Ok("i" | "u" | "b")
            )
        {
            pending = true;
            continue;
        }
        if value.is_none() {
            values.push(0);
        } else if value.is_instance_of::<PyString>() {
            let string = value.extract::<String>()?;
            values.push(i64::from(number_prefix(&string) as i32));
        } else if value.is_instance_of::<PyInt>() {
            match value.extract::<i64>() {
                Ok(number) => values.push(i64::from(number as i32)),
                Err(_) => {
                    values.push(-1);
                    pending = true;
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
    }
    Ok((values, pending))
}

/// casatools reads numeric prefixes in string elements, defaulting to zero.
fn number_prefix(value: &str) -> i64 {
    let value = value.trim_start();
    let bytes = value.as_bytes();
    let mut i = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let digits = i;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
    }
    if i == digits {
        return 0;
    }
    value[..i].parse::<i64>().unwrap_or_else(|_| {
        if bytes.first() == Some(&b'-') {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}
