// SPDX-License-Identifier: LGPL-3.0-or-later
//! Safe conversion of `setrefdir` quantity arguments.

use std::collections::HashSet;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::Variant;

enum Frame<'py> {
    Value(Bound<'py, PyAny>),
    Leave(usize),
}

/// Avoid the recursive Python-record conversion that crashes CASA on cycles;
/// ordinary records retain the existing shared variant conversion.
pub(super) fn argument(value: Option<Bound<'_, PyAny>>) -> PyResult<(Variant, bool)> {
    let Some(value) = value else {
        return Ok((Variant::Empty, false));
    };
    let mut active = HashSet::new();
    let mut frames = vec![Frame::Value(value.clone())];
    while let Some(frame) = frames.pop() {
        let value = match frame {
            Frame::Value(value) => value,
            Frame::Leave(identity) => {
                active.remove(&identity);
                continue;
            }
        };
        if let Ok(record) = value.cast::<PyDict>() {
            let identity = value.as_ptr() as usize;
            if !active.insert(identity) {
                return Err(PyValueError::new_err(
                    "cyclic component direction record (CASA crashes: #1053)",
                ));
            }
            let values: Vec<_> = record.iter().map(|(_, item)| item).collect();
            frames.push(Frame::Leave(identity));
            frames.extend(values.into_iter().rev().map(Frame::Value));
        }
    }
    let pending = super::replace_args::record_pending(&value)?;
    Ok((value.extract::<Variant>()?, pending))
}
