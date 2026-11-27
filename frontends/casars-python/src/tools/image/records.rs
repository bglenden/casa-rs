// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_imageanalysis::tool::metadata::{misc_info_casa, set_history_casa, set_misc_info};
use numpy::PyUntypedArray;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use crate::convert::value::CasaValue;
use crate::convert::{DoubleList, IntList};
use crate::tools::table;

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};

pub(super) fn numeric_axis_vector(value: &Bound<'_, PyAny>) -> PyResult<IntList> {
    if value.is_none() || value.is_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "axes must be a number, list, or NumPy array",
        ));
    }
    let flattened = value
        .cast::<PyUntypedArray>()
        .ok()
        .map(|_| value.call_method1("ravel", ("F",))?.call_method0("tolist"))
        .transpose()?;
    let values = flattened.as_ref().unwrap_or(value);
    if let Ok(integers) = values.extract::<IntList>() {
        return Ok(integers);
    }
    let DoubleList(numbers) = values.extract()?;
    numbers
        .into_iter()
        .map(|number| {
            if number.is_finite() && number >= i64::MIN as f64 && number < -(i64::MIN as f64) {
                Ok(number as i64)
            } else {
                Err(PyOverflowError::new_err(
                    "axes must be finite integer indices",
                ))
            }
        })
        .collect::<PyResult<Vec<_>>>()
        .map(IntList)
}

pub(super) fn string_vector(
    value: Option<&Bound<'_, PyAny>>,
    argument: &str,
) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Ok(value) = value.cast::<PyString>() {
        return Ok(vec![value.to_str()?.into()]);
    }
    let Ok(values) = value.cast::<PyList>() else {
        return Ok(Vec::new());
    };
    values
        .iter()
        .map(|message| {
            message
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err(format!("list _{argument} must contain strings")))
        })
        .collect()
}

#[pymethods]
impl PyImageTool {
    fn miscinfo(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let (record, path) = misc_info_casa(attachment).map_err(raised)?;
            Ok::<_, PyErr>(table::values::record(
                &record,
                path.as_deref().unwrap_or(Path::new(".")),
            ))
        })?;
        value.into_python(py)
    }

    fn setmiscinfo(&self, py: Python<'_>, info: &Bound<'_, PyDict>) -> PyResult<bool> {
        let record = table::casa_record(py, Some(info))?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            set_misc_info(attachment, record, state.history).map_err(raised)
        })
    }

    #[pyo3(signature = (origin="", history=None), text_signature = "($self, origin='', history=[])")]
    fn sethistory(
        &self,
        py: Python<'_>,
        origin: &str,
        history: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let messages = string_vector(history, "history")?;
        let (set, logs) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((false, Vec::new()));
            };
            let logs = set_history_casa(attachment, origin, messages).map_err(raised)?;
            Ok::<_, PyErr>((true, logs))
        })?;
        if set {
            emit_tool_logs(py, &logs)?;
        }
        Ok(set)
    }
}
