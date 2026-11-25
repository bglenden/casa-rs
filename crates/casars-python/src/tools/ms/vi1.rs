// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind the deprecated VI1 cursor independently of modern and legacy selection.

use std::sync::Arc;

use casa_ms::tool_iteration::Vi1ToolIteration;
use pyo3::exceptions::PySystemError;
use pyo3::prelude::*;

use super::legacy_selection::deprecation;
use super::visibility::{VisibilityMain, channel_argument, requested_strings};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

pub(super) struct Vi1Iteration {
    cursor: Vi1ToolIteration,
    pub(super) main: Arc<VisibilityMain>,
    selected: Option<Arc<[usize]>>,
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (columns = None, interval = 0.0, maxrows = 0, adddefaultsortcolumns = true), text_signature = "($self, columns=[], interval=0.0, maxrows=0, adddefaultsortcolumns=True)")]
    fn niterinit(
        &self,
        py: Python<'_>,
        columns: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = interval_argument)] interval: f64,
        #[pyo3(from_py_with = maxrows_argument)] maxrows: i64,
        #[pyo3(from_py_with = strict_bool)] adddefaultsortcolumns: bool,
    ) -> PyResult<bool> {
        requested_strings(columns, "columns")?;
        deprecation(py, "niterinit", "iterinit")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let ms = attached.as_mut().ok_or_else(empty_view)?;
            ms.initialize_vi1(interval, maxrows, adddefaultsortcolumns)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn niterorigin(&self, py: Python<'_>) -> PyResult<bool> {
        self.vi1_control(py, |iteration| {
            iteration.rewind();
            true
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn niterend(&self, py: Python<'_>) -> PyResult<bool> {
        self.vi1_control(py, |iteration| iteration.exhausted())
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn niternext(&self, py: Python<'_>) -> PyResult<bool> {
        self.vi1_control(py, Vi1ToolIteration::advance)
    }

    #[pyo3(signature = (items = None, ifraxis = false, ifraxisgap = 0, increment = 1, average = false), text_signature = "($self, items=[], ifraxis=False, ifraxisgap=0, increment=1, average=False)")]
    fn ngetdata(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] ifraxis: bool,
        #[pyo3(from_py_with = gap_argument)] ifraxisgap: i64,
        #[pyo3(from_py_with = increment_argument)] increment: i64,
        #[pyo3(from_py_with = strict_bool)] average: bool,
    ) -> PyResult<Py<PyAny>> {
        // CASA parses these four arguments but VI1 ignores them.
        let _ = (ifraxis, ifraxisgap, increment, average);
        let items = requested_strings(items, "items")?;
        deprecation(py, "ngetdata", "getdata")?;
        let result = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let ms = attached.as_mut().ok_or_else(empty_view)?;
            if ms.visibility.vi1.is_none() {
                ms.initialize_vi1(0.0, 0, false)?;
            }
            let link = ms.working_metadata_link();
            let vi1 = ms.visibility.vi1.as_ref().expect("VI1 was installed");
            link.verify_directory().map_err(runtime_error)?;
            vi1.main.verify_iterator_layout()?;
            let result =
                vi1.main
                    .read_vi1_rows(vi1.cursor.rows(), vi1.selected.as_deref(), &items)?;
            link.verify_directory().map_err(runtime_error)?;
            Ok::<_, PyErr>(result)
        })?;
        result.into_python(py)
    }
}

impl PyMsTool {
    fn vi1_control(
        &self,
        py: Python<'_>,
        visit: impl FnOnce(&mut Vi1ToolIteration) -> bool + Send,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            let iteration = ms
                .visibility
                .vi1
                .as_mut()
                .ok_or_else(|| runtime_error("Please call niterinit() first."))?;
            Ok(visit(&mut iteration.cursor))
        })
    }
}

impl AttachedMs {
    fn initialize_vi1(&mut self, interval: f64, maxrows: i64, defaults: bool) -> PyResult<()> {
        let link = self.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let selected = link.selected_rows().map_err(runtime_error)?;
        let main = self.visibility.current_main(&self.path)?;
        let cursor = main.vi1_cursor(selected.as_deref(), interval, maxrows, defaults)?;
        link.verify_directory().map_err(runtime_error)?;
        self.visibility.vi1 = Some(Vi1Iteration {
            cursor,
            main,
            selected,
        });
        Ok(())
    }
}

fn empty_view() -> PyErr {
    runtime_error("MSIter::construct - Input MeasurementSet.has zero selected rows")
}

fn interval_argument(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    value.extract().map_err(|_| {
        PySystemError::new_err(
            "<built-in function ms_niterinit> returned a result with an exception set",
        )
    })
}

fn maxrows_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "maxrows", "ms_niterinit")
}

fn gap_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "ifraxisgap", "ms_ngetdata")
}

fn increment_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "increment", "ms_ngetdata")
}
