// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's public visibility chunk cursor.

use std::sync::Arc;

use casa_ms::tool_iteration::ToolIteration;
use casa_ms::tool_visibility::{VisibilityChannels, VisibilityPolarization};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;

use super::metadata_link::MetadataSnapshot;
use super::visibility::{VisibilityMain, requested_strings};
use super::{PyMsTool, runtime_error, strict_bool};

pub(super) struct VisibilityIteration {
    pub(super) cursor: ToolIteration,
    pub(super) layout: Option<Arc<VisibilityMain>>,
    pub(super) metadata: Option<Arc<MetadataSnapshot>>,
    pub(super) channels: Option<VisibilityChannels>,
    pub(super) polarization: Option<Arc<VisibilityPolarization>>,
    pub(super) conversion: Option<Arc<VisibilityPolarization>>,
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (columns = None, interval = 0.0, maxrows = 0, adddefaultsortcolumns = true), text_signature = "($self, columns=[], interval=0.0, maxrows=0, adddefaultsortcolumns=True)")]
    fn iterinit(
        &self,
        py: Python<'_>,
        columns: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = iteration_interval)] interval: f64,
        #[pyo3(from_py_with = iteration_maxrows)] maxrows: i64,
        #[pyo3(from_py_with = strict_bool)] adddefaultsortcolumns: bool,
    ) -> PyResult<bool> {
        let columns = requested_strings(columns, "columns")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.initialize_public_iteration(
                    &columns,
                    interval,
                    maxrows,
                    adddefaultsortcolumns,
                ),
                None => Ok(false),
            }
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iterorigin(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            if let Some(iteration) = attached
                .as_mut()
                .and_then(|ms| ms.visibility.iteration.as_mut())
            {
                iteration.cursor.rewind();
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iternext(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached
                .as_mut()
                .and_then(|ms| ms.visibility.iteration.as_mut())
            {
                Some(iteration) => iteration.cursor.advance().map_err(runtime_error),
                None => Ok(false),
            }
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iterend(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            if let Some(ms) = attached.as_mut() {
                ms.visibility.iteration = None;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }
}

fn iteration_interval(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    value.extract().map_err(|_| iteration_conversion_error())
}

fn iteration_maxrows(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if value.is_none() {
        return Err(iteration_conversion_error());
    }
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _maxrows must be an integer"))
}

fn iteration_conversion_error() -> PyErr {
    PySystemError::new_err(
        "<built-in function ms_iterinit> returned a result with an exception set",
    )
}
