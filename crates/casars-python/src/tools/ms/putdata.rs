// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write visibility records through the current MS tool view.

use casa_types::RecordValue;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::legacy_selection::deprecation;
use super::{PyMsTool, runtime_error};
use crate::tools::table::casa_record;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = RecordValue::default()), text_signature = "($self, items={})")]
    fn putdata(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = write_record)] items: RecordValue,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut().filter(|ms| ms.writable) {
                Some(ms) => ms.putdata_rows(items),
                None => Ok(false),
            }
        })
    }

    #[pyo3(signature = (items = RecordValue::default()), text_signature = "($self, items={})")]
    fn putdataold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = write_record)] items: RecordValue,
    ) -> PyResult<bool> {
        deprecation(py, "putdataold", "putdata")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut().filter(|ms| ms.writable) {
                Some(ms) => ms.put_legacy_data_rows(items),
                None => Ok(false),
            }
        })
    }
}

fn write_record(value: &Bound<'_, PyAny>) -> PyResult<RecordValue> {
    let record = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("_items is not a dictionary"))?;
    casa_record(value.py(), Some(record))
}
