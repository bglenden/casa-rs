// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 lock methods delegating to [`casa_imageanalysis::tool::locking`].

use casa_imageanalysis::tool::locking::{
    has_lock_single_precision, lock_single_precision, unlock_single_precision,
};
use numpy::{IntoPyArray, PyArray1};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyBool;

use super::PyImageTool;
use super::handle::raised;

fn write_flag(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|flag| flag.is_true())
        .map_err(|_| PyTypeError::new_err("in method 'image_lock', argument 2 of type 'bool'"))
}

fn attempts(value: &Bound<'_, PyAny>) -> PyResult<i32> {
    crate::tools::table::integer(Some(value), "nattempts", 0).map(|value| value as i32)
}

#[pymethods]
impl PyImageTool {
    fn haslock<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<bool>>> {
        let flags = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(Vec::new());
            };
            has_lock_single_precision(attachment).map_err(raised)
        })?;
        Ok(flags.into_pyarray(py))
    }

    #[pyo3(signature = (writelock=false, nattempts=0), text_signature = "($self, writelock=False, nattempts=0)")]
    fn lock(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = write_flag)] writelock: bool,
        #[pyo3(from_py_with = attempts)] nattempts: i32,
    ) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            lock_single_precision(attachment, writelock, nattempts).map_err(raised)
        })
    }

    fn unlock(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            unlock_single_precision(attachment).map_err(raised)
        })
    }
}
