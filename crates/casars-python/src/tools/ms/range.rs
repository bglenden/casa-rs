// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected MAIN column ranges through the public MS tool.

use casa_ms::tool_range::ToolRangeRequest;
use pyo3::exceptions::{PyNotImplementedError, PySystemError, PyTypeError};
use pyo3::prelude::*;

use super::legacy_selection::deprecation;
use super::metadata_link::compose_rows;
use super::visibility::requested_strings;
use super::{PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None, useflags = true, blocksize = 10), text_signature = "($self, items=[], useflags=True, blocksize=10)")]
    fn range(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] useflags: bool,
        #[pyo3(from_py_with = blocksize_argument)] blocksize: i64,
    ) -> PyResult<Py<PyAny>> {
        let items = requested_strings(items, "items")?;
        let request = ToolRangeRequest::new(&items, useflags, blocksize);
        let reply = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.visibility_range(&request),
                None => Ok(CasaValue::empty_record()),
            }
        })?;
        reply.into_python(py)
    }

    #[pyo3(signature = (items = None, useflags = true, blocksize = 10), text_signature = "($self, items=[], useflags=True, blocksize=10)")]
    fn rangeold(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] useflags: bool,
        #[pyo3(from_py_with = blocksize_argument)] blocksize: i64,
    ) -> PyResult<Py<PyAny>> {
        let items = requested_strings(items, "items")?;
        deprecation(py, "rangeold", "range")?;
        let request = ToolRangeRequest::new(&items, useflags, blocksize);
        let reply = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else { return Ok(CasaValue::empty_record()) };
            if request.uses_visibility_axes() {
                return Err(PyNotImplementedError::new_err(
                    "ms.rangeold visibility and spectral-axis ranges are not yet implemented in casa-rs (#1030)",
                ));
            }
            let link = ms.working_metadata_link();
            let base = link.selected_rows().map_err(runtime_error)?;
            let rows = compose_rows(base.as_deref(), ms.legacy_selection.rows());
            ms.visibility.range_for_rows(&ms.path, &link, rows.as_deref(), &request)
        })?;
        reply.into_python(py)
    }
}

fn blocksize_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if value.is_none() {
        return Err(PySystemError::new_err(
            "<built-in function ms_range> returned a result with an exception set",
        ));
    }
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _blocksize must be an integer"))
}
