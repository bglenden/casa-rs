// SPDX-License-Identifier: LGPL-3.0-or-later
//! Argument handling for CASA's component-list placeholders.

use std::collections::HashSet;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyList, PyTuple};

/// CASA leaves an integer-conversion exception pending for out-of-range
/// Python integers, including those nested in list, tuple or record values.
pub(super) fn pending_integer_overflow(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let mut pending = vec![value.clone()];
    let mut seen = HashSet::new();
    while let Some(value) = pending.pop() {
        if value.is_instance_of::<PyInt>() {
            if value.extract::<i64>().is_err() {
                return Ok(true);
            }
        } else if let Ok(record) = value.cast::<PyDict>() {
            if seen.insert(value.as_ptr() as usize) {
                pending.extend(record.iter().map(|(_, item)| item));
            }
        } else if (value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>())
            && seen.insert(value.as_ptr() as usize)
        {
            pending.extend(value.try_iter()?.collect::<PyResult<Vec<_>>>()?);
        }
    }
    Ok(false)
}
