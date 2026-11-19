// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit, table};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

fn history_messages(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
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
                .map_err(|_| PyTypeError::new_err("list _history must contain strings"))
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
            let Some(image) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut image = image.lock();
            with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_misc_info(record)))
                .map_err(raised)?;
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
        let mut messages = history_messages(history)?;
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
                let mut image = image.lock();
                image.append_history_at(&location, &messages)?;
                image.save().map_err(raised)?;
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
