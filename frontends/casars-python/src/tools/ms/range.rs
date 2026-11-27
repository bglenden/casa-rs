// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected MAIN column ranges through the public MS tool.

use casa_mstransform::tool_range::ToolRangeRequest;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;

use super::legacy_selection::deprecation;
use super::metadata_link::compose_rows;
use super::owned_record::visibility_record;
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
        #[pyo3(from_py_with = legacy_blocksize_argument)] blocksize: i64,
    ) -> PyResult<Py<PyAny>> {
        let items = requested_strings(items, "items")?;
        deprecation(py, "rangeold", "range")?;
        let request = ToolRangeRequest::new(&items, useflags, i64::from(blocksize as i32));
        let reply = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(CasaValue::empty_record());
            };
            let ms = ms.legacy_view_mut();
            let link = ms.working_metadata_link();
            let base = link.selected_rows().map_err(runtime_error)?;
            let rows = compose_rows(base.as_deref(), ms.legacy_selection.rows());
            let options = ms.legacy_selection.visibility_options();
            if let Some(memory) = &ms.memory_ms {
                let rows = rows.map_or_else(
                    || (0..memory.row_count()).collect(),
                    |rows| rows.into_owned(),
                );
                if rows.is_empty() {
                    return Ok(CasaValue::empty_record());
                }
                let record = request
                    .read(memory.main_table(), &[], None, &rows, options, |ids| {
                        ids.iter()
                            .map(|&id| {
                                memory
                                    .subtable(id)
                                    .map(|table| (id, std::slice::from_ref(table)))
                                    .ok_or_else(|| {
                                        casa_ms::MsError::MissingSubtable(id.name().to_owned())
                                    })
                            })
                            .collect()
                    })
                    .map_err(runtime_error)?;
                return Ok(visibility_record(record));
            }
            if let Some(main) = ms
                .legacy_selection
                .positioned_iteration()
                .and_then(|iteration| iteration.main.as_deref())
            {
                link.verify_directory().map_err(runtime_error)?;
                main.verify_iterator_layout()?;
                return ms.visibility.range_from_main(
                    main,
                    &link,
                    rows.as_deref(),
                    &request,
                    options,
                );
            }
            ms.visibility
                .range_for_rows(&ms.path, &link, rows.as_deref(), &request, options)
        })?;
        reply.into_python(py)
    }
}

fn blocksize_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    parse_blocksize(value, "ms_range")
}

fn legacy_blocksize_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    parse_blocksize(value, "ms_rangeold")
}

fn parse_blocksize(value: &Bound<'_, PyAny>, method: &str) -> PyResult<i64> {
    if value.is_none() {
        return Err(PySystemError::new_err(format!(
            "<built-in function {method}> returned a result with an exception set"
        )));
    }
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _blocksize must be an integer"))
}
