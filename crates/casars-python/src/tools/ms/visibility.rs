// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read row-axis visibility records from the current MS attachment.

use casa_ms::tool_visibility;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods, PyString};

use super::owned_record::visibility_record;
use super::{PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;
use crate::tools::not_yet_implemented_in;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None, ifraxis = false, ifraxisgap = 0, increment = 1, average = false), text_signature = "($self, items=[], ifraxis=False, ifraxisgap=0, increment=1, average=False)")]
    fn getdata(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] ifraxis: bool,
        #[pyo3(from_py_with = gap_argument)] ifraxisgap: i64,
        #[pyo3(from_py_with = increment_argument)] increment: i64,
        #[pyo3(from_py_with = strict_bool)] average: bool,
    ) -> PyResult<Py<PyAny>> {
        let items = requested_items(items)?;
        let _ = ifraxisgap;
        let reply = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(CasaValue::empty_record());
            };
            if ms.metadata_link().row_count().map_err(runtime_error)? == 0 {
                return Ok(CasaValue::empty_record());
            }
            if ifraxis || average {
                return not_yet_implemented_in("ms", "getdata ifraxis or average", super::ISSUE);
            }
            let mut snapshot = ms.metadata_link().snapshot(None).map_err(runtime_error)?;
            if !ms.visibility_initialized {
                if let Some(mut selected) = tool_visibility::heterogeneous_row_shapes(&snapshot.ms)
                    .map_err(runtime_error)?
                {
                    if let Some(prior) = &ms.selected_rows {
                        for row in &mut selected {
                            *row = prior[*row];
                        }
                    }
                    let selected_any = !selected.is_empty();
                    ms.set_selected_rows(Some(selected))?;
                    ms.selection_state.use_implicit_selection();
                    if !selected_any {
                        return Ok(CasaValue::empty_record());
                    }
                    snapshot = ms.metadata_link().snapshot(None).map_err(runtime_error)?;
                }
                ms.visibility_initialized = true;
            }
            if items.is_empty() {
                return Ok(CasaValue::empty_record());
            }
            let rows = sampled_rows(snapshot.ms.row_count(), increment);
            let read = tool_visibility::read_tool_visibility(&snapshot.ms, &rows, &items)
                .map_err(runtime_error)?;
            Ok(visibility_record(read))
        })?;
        reply.into_python(py)
    }
}

fn requested_items(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(vec![text.to_str()?.to_owned()]);
    }
    let Ok(list) = value.cast::<PyList>() else {
        return Ok(Vec::new());
    };
    list.iter()
        .map(|value| {
            value
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err("list _items must contain strings"))
        })
        .collect()
}

fn increment_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "increment")
}

fn gap_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "ifraxisgap")
}

fn integer_argument(value: &Bound<'_, PyAny>, name: &str) -> PyResult<i64> {
    if value.is_none() {
        return Err(PySystemError::new_err(
            "<built-in function ms_getdata> returned a result with an exception set",
        ));
    }
    value
        .extract::<i64>()
        .map_err(|_| PyTypeError::new_err(format!("argument _{name} must be an integer")))
}

fn sampled_rows(count: usize, increment: i64) -> Vec<usize> {
    // CASA's admission comparison narrows the stride to an unsigned int,
    // then uses the full stride in the output size and sampling.
    if increment > 1 && (increment as u32 as usize) <= count {
        let stride = increment as usize;
        (0..count).step_by(stride).take(count / stride).collect()
    } else {
        (0..count).collect()
    }
}
