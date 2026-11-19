// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA string and record parameter errors, including explicit None.
use crate::convert::RecordArg;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

pub(super) fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(Some(value.clone()))
}

pub(super) fn reference(value: Option<&Bound<'_, PyAny>>, default: &str) -> PyResult<String> {
    text(value, "rf", default)
}

pub(super) fn text(
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
    default: &str,
) -> PyResult<String> {
    let text: String = match value {
        None => default.into(),
        Some(value) if value.is_instance_of::<PyString>() => value.extract()?,
        Some(value) if value.is_instance_of::<PyBytes>() => {
            value.call_method1("decode", ("utf-8",))?.extract()?
        }
        _ => {
            return Err(PyTypeError::new_err(format!(
                "argument _{name} must be a string"
            )));
        }
    };
    Ok(text.split('\0').next().unwrap_or_default().to_owned())
}

pub(super) fn record(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<RecordArg> {
    match value {
        None => Ok(RecordArg::empty()),
        Some(value) if value.is_instance_of::<PyDict>() => value.extract(),
        Some(_) => Err(PyTypeError::new_err(format!("_{name} is not a dictionary"))),
    }
}
