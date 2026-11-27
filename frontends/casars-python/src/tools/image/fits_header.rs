// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.fitsheader` wrapper delegating to [`casa_imageanalysis::tool::fits`].

use casa_core::{ArrayValue, RecordValue, Value};
use casa_imageanalysis::tool::fits::{FitsHeaderOutput, fits_header};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::raised;

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
        py.detach(|| -> PyResult<CasaValue> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(if retstr {
                    CasaValue::Text(String::new())
                } else {
                    CasaValue::empty_record()
                });
            };
            match fits_header(attachment, retstr, &exclude).map_err(raised)? {
                FitsHeaderOutput::CardString(s) => Ok(CasaValue::Text(s)),
                FitsHeaderOutput::Record(rec) => Ok(header_record(&rec)),
            }
        })?
        .into_python(py)
    }
}
