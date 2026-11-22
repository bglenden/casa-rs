// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use numpy::PyUntypedArray;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use crate::convert::value::CasaValue;
use crate::convert::{DoubleList, IntList};
use crate::tools::{LogLine, emit, table};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

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
            let Some(image) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let image = image.lock();
            image.check_single_precision("miscinfo")?;
            let record = with_pixels!(&*image, storage => with_storage!(storage, image => image.misc_info()));
            Ok::<_, PyErr>(table::values::record(&record, image.path().unwrap_or(Path::new("."))))
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
            let mut image = attachment.lock();
            if let Some(directory) = attachment.saved_expression_directory() {
                let path = image.path().ok_or_else(|| raised("Saved image expression has no name"))?;
                directory.write_metadata(path, image.kind(), &record)?;
            } else if image.image_type_name() == "ImageConcat" {
                attachment.check_bound_name(&image)?;
            } else {
                attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            }
            // CASA accepts this call on a MIRIAD attachment and records it
            // in history. Subsequent reads and exports see the empty record.
            if image.image_type_name() != "MIRIADImage" {
                with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_misc_info(record)))
                    .map_err(raised)?;
            }
            if state.history {
                image.record_history("setmiscinfo", "ia.setmiscinfo(info={...})".into())?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (origin="", history=None), text_signature = "($self, origin='', history=[])")]
    fn sethistory(
        &self,
        py: Python<'_>,
        origin: &str,
        history: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let mut messages = string_vector(history, "history")?;
        let only_empty_string = matches!(messages.as_slice(), [message] if message.is_empty());
        let set = py.detach(|| {
            let state = self.state();
            let Some(image) = state.image.as_ref() else {
                return Ok(false);
            };
            messages.retain(|message| !message.is_empty());
            if !messages.is_empty() {
                let location = if origin.is_empty() {
                    "ImageHistory::addHistory".to_owned()
                } else {
                    format!("::{origin}")
                };
                let mut handle = image.lock();
                image.check_bound_name(&handle)?;
                handle.append_history_at(&location, &messages)?;
            }
            Ok::<_, PyErr>(true)
        })?;
        if set && only_empty_string {
            emit(
                py,
                "casars.tools.image",
                &[LogLine::info("history string is empty")],
            )?;
        }
        Ok(set)
    }
}
