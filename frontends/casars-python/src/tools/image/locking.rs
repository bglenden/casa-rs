// SPDX-License-Identifier: LGPL-3.0-or-later

//! PyO3 lock methods delegating to `casa_imageanalysis::tool::locking`.

use casa_tables::LockType;
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
            let handle = attachment.lock();
            handle.check_single_precision("haslock").map_err(raised)?;
            Ok::<_, PyErr>(
                [LockType::Read, LockType::Write]
                    .map(|kind| {
                        handle.has_storage_lock(kind)
                            && attachment
                                .expression_inputs()
                                .iter()
                                .all(|input| input.lock().has_storage_lock(kind))
                    })
                    .to_vec(),
            )
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
            let mut handle = attachment.lock();
            handle.check_single_precision("lock").map_err(raised)?;
            let kind = if writelock {
                LockType::Write
            } else {
                LockType::Read
            };
            attachment
                .lock_with_inputs(&mut handle, kind, nattempts.max(0) as u32, false)
                .map_err(raised)
        })
    }

    fn unlock(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut handle = attachment.lock();
            handle.check_single_precision("unlock").map_err(raised)?;
            attachment.check_bound_name(&handle).map_err(raised)?;
            for input in attachment.expression_inputs() {
                let mut source = input.lock();
                input.check_bound_name(&source).map_err(raised)?;
                source.unlock_storage().map_err(raised)?;
            }
            handle.unlock_storage().map_err(raised)?;
            Ok(true)
        })
    }
}
