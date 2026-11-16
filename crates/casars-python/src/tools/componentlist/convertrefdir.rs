// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert a component direction using CASA's reference-frame arguments.

use std::str::FromStr;

use casa_types::measures::direction::DirectionRef;
use pyo3::exceptions::{PyNotImplementedError, PyOverflowError, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyInt, PyString};

use super::PyComponentList;
use crate::convert::holder::DIRECTION_CODES;
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

#[pymethods]
impl PyComponentList {
    /// Convert one component's direction into the requested reference frame.
    #[pyo3(signature = (which, frame), text_signature = "($self, which, frame)")]
    fn convertrefdir(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        frame: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let (which, pending) = if !which.is_instance_of::<PyInt>()
            && which.is_instance(&py.import("numpy")?.getattr("bool_")?)?
        {
            (-1, true)
        } else {
            match which.extract::<i64>() {
                Ok(index) => (i64::from(index as i32), false),
                Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                    (-1, true)
                }
                Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
            }
        };
        let name: String = if frame.is_instance_of::<PyString>() {
            frame.extract()?
        } else if frame.is_instance_of::<PyBytes>() {
            let bytes = frame.cast::<PyBytes>()?.as_bytes();
            let end = bytes
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(bytes.len());
            String::from_utf8_lossy(&bytes[..end]).into_owned()
        } else {
            return Err(PyTypeError::new_err("argument _frame must be a string"));
        };
        let name = name.split('\0').next().unwrap_or_default();
        let result = if let Some(name) = DIRECTION_CODES.resolve(name) {
            let frame = DirectionRef::from_str(name).map_err(|_| PyNotImplementedError::new_err(
                format!("direction reference {name} is not supported by the shared measure model (#1053)"),
            ))?;
            self.call(py, move |state, _| {
                state.convertrefdir(which, frame).map(CasaValue::Bool)
            })?
        } else {
            self.call(py, |_, lines| {
                lines.push(LogLine::error(
                    "Could not parse the 'frame' string: Direction frame not changed",
                ));
                Ok(CasaValue::Bool(false))
            })?
        };
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_convertrefdir> returned a result with an exception set",
            ));
        }
        Ok(result)
    }
}
