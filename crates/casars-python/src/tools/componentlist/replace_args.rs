// SPDX-License-Identifier: LGPL-3.0-or-later
//! The argument conversions CASA performs before its `replace` placeholder.

use std::collections::HashSet;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

/// An out-of-range Python integer leaves CASA's record conversion with an
/// exception pending. Records directly in records are traversed; records in
/// an array are ignored by that array conversion. Cycles are skipped safely.
pub(super) fn record_pending(record: &Bound<'_, PyAny>) -> PyResult<bool> {
    let mut pending = vec![(record.clone(), true)];
    let mut seen = HashSet::new();
    while let Some((value, records_allowed)) = pending.pop() {
        if value.is_instance_of::<PyInt>() {
            if value.extract::<i64>().is_err() {
                return Ok(true);
            }
        } else if records_allowed && let Ok(dict) = value.cast::<PyDict>() {
            if seen.insert((value.as_ptr() as usize, true)) {
                pending.extend(dict.iter().map(|(_, item)| (item, true)));
            }
        } else if (value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>())
            && seen.insert((value.as_ptr() as usize, false))
        {
            pending.extend(
                value
                    .try_iter()?
                    .map(|item| item.map(|v| (v, false)))
                    .collect::<PyResult<Vec<_>>>()?,
            );
        }
    }
    Ok(false)
}

enum Frame<'py> {
    Value(Bound<'py, PyAny>),
    Leave(usize),
}

/// Check the ignored index vector without converting its elements twice.
/// CASA crashes for a cyclic sequence or a non-finite scalar (#1176).
pub(super) fn indices_pending(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(
            "argument _whichones must not be a string",
        ));
    }
    if value.is_instance_of::<PyFloat>() {
        if !value.extract::<f64>()?.is_finite() {
            return Err(PyValueError::new_err(
                "non-finite component selection (CASA crash: #1176)",
            ));
        }
        return Ok(false);
    }
    if value.is_instance_of::<PyInt>() {
        return Ok(value.extract::<i64>().is_err());
    }
    let ndarray = value.py().import("numpy")?.getattr("ndarray")?;
    if value.is_instance(&ndarray)? {
        return Ok(false);
    }
    if !value.is_instance_of::<PyList>() && !value.is_instance_of::<PyTuple>() {
        return Ok(true);
    }
    let mut frames = vec![Frame::Value(value.clone())];
    let mut active = HashSet::new();
    let mut pending = false;
    while let Some(frame) = frames.pop() {
        let value = match frame {
            Frame::Value(value) => value,
            Frame::Leave(identity) => {
                active.remove(&identity);
                continue;
            }
        };
        if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            let identity = value.as_ptr() as usize;
            if !active.insert(identity) {
                return Err(PyValueError::new_err(
                    "cyclic component selection (CASA crash: #1176)",
                ));
            }
            let items = value.try_iter()?.collect::<PyResult<Vec<_>>>()?;
            frames.push(Frame::Leave(identity));
            frames.extend(items.into_iter().rev().map(Frame::Value));
        } else if value.is_instance_of::<PyInt>() {
            pending |= value.extract::<i64>().is_err();
        }
    }
    Ok(pending)
}
