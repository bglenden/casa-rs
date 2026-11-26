// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayValue, RecordValue, Value};
use casa_imageanalysis::analysis::image_fits_header_with_beams;
use casa_images::image_history_entries;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};

fn header_record(record: &RecordValue) -> CasaValue {
    CasaValue::record(record.fields().iter().map(|field| match &field.value {
        Value::Array(ArrayValue::Int64(values)) => (
            field.name.clone(),
            CasaValue::Ints {
                shape: values.shape().to_vec(),
                values: values.iter().copied().collect(),
            },
        ),
        Value::Array(ArrayValue::Bool(values)) => (
            field.name.clone(),
            CasaValue::Bools {
                shape: values.shape().to_vec(),
                values: values.iter().copied().collect(),
            },
        ),
        other => (field.name.clone(), CasaValue::from_value(other)),
    }))
}

fn exclusions(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(vec![text.to_str()?.to_owned()]);
    }
    if let Ok(list) = value.cast::<PyList>() {
        return list
            .extract()
            .map_err(|_| PyTypeError::new_err("list _exclude must contain strings"));
    }
    Ok(Vec::new())
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (retstr=false, exclude=None), text_signature = "($self, retstr=False, exclude=[])")]
    fn fitsheader(
        &self,
        py: Python<'_>,
        retstr: bool,
        exclude: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let exclude = exclusions(exclude)?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(if retstr {
                    CasaValue::Text(String::new())
                } else {
                    CasaValue::empty_record()
                });
            };
            let handle = attachment.lock();
            let ImageHandle::Float(storage) = &*handle else {
                return Err(raised("fitsheader only supports floating point images"));
            };
            let name = handle.path().map_or_else(
                || "Temporary_Image".to_owned(),
                |path| path.display().to_string(),
            );
            let needs_history = retstr || !exclude.iter().any(|name| name == "HISTORY");
            let history = if needs_history {
                match storage {
                    Storage::Paged(_) => {
                        image_history_entries(handle.path().expect("persistent image"))
                            .map_err(raised)?
                    }
                    Storage::Temp { image, .. } => image
                        .history_rows()
                        .map_err(raised)?
                        .iter()
                        .map(|row| row.entry.clone())
                        .collect(),
                }
            } else {
                Vec::new()
            };
            let beams = attachment.beam_view();
            let header = with_storage!(storage, image =>
                image_fits_header_with_beams(
                    image.as_ref(), handle.shape(), &name, &history, Some(beams.record())
                )
            )
            .map_err(raised)?;
            Ok(if retstr {
                CasaValue::Text(header.card_string())
            } else {
                header_record(&header.record(&exclude))
            })
        })?
        .into_python(py)
    }
}
