// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `ms` tool (ms): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1030 (`stubs.rs`).

mod stubs;
pub(crate) mod surface;

use std::path::PathBuf;
use std::sync::Mutex;

use casa_ms::MeasurementSet;
use casa_tables::{Table, TableWriteLock};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::tools::not_yet_implemented_in;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1030;

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("ms", method, ISSUE)
}

/// CASA's `ms` tool.
#[pyclass(name = "ms", module = "casars.tools", frozen)]
pub(crate) struct PyMsTool {
    attached: Mutex<Option<AttachedMs>>,
}

struct AttachedMs {
    path: PathBuf,
    writable: bool,
    write_lock: Option<TableWriteLock>,
    _read_lock: Option<Table>,
}

#[pymethods]
impl PyMsTool {
    #[new]
    fn new() -> Self {
        Self {
            attached: Mutex::new(None),
        }
    }

    #[pyo3(signature = (thems = "", nomodify = true, lock = false, check = false), text_signature = "($self, thems='', nomodify=True, lock=False, check=False)")]
    fn open(
        &self,
        py: Python<'_>,
        thems: &str,
        nomodify: bool,
        lock: bool,
        check: bool,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            *attached = None;
            let path = std::fs::canonicalize(thems).map_err(runtime_error)?;
            let write_lock = (lock && !nomodify)
                .then(|| TableWriteLock::acquire(&path, 1))
                .transpose()
                .map_err(runtime_error)?;
            #[cfg(unix)]
            let read_lock = (lock && nomodify)
                .then(|| {
                    let options =
                        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking)
                            .read_only(true);
                    Table::open_with_lock(casa_tables::TableOptions::new(&path), options)
                })
                .transpose()
                .map_err(runtime_error)?;
            #[cfg(not(unix))]
            let read_lock = None;
            let ms = MeasurementSet::open(&path).map_err(runtime_error)?;
            if check {
                ms.check_referential_integrity().map_err(runtime_error)?;
            }
            *attached = Some(AttachedMs {
                path,
                writable: !nomodify,
                write_lock,
                _read_lock: read_lock,
            });
            Ok(true)
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn close(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            Ok(self
                .attached
                .lock()
                .map_err(runtime_error)?
                .take()
                .is_some())
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.close(py)
    }

    #[pyo3(signature = (reverse = false), text_signature = "($self, reverse=False)")]
    fn uvsub(&self, py: Python<'_>, reverse: bool) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut().filter(|ms| ms.writable) else {
                return Ok(false);
            };
            if let Some(lock) = ms.write_lock.take() {
                match casa_calibration::subtract_visibility_model_holding(&ms.path, reverse, lock) {
                    Ok(lock) => ms.write_lock = Some(lock),
                    Err(error) => {
                        ms.write_lock = TableWriteLock::acquire(&ms.path, 1).ok();
                        return Err(runtime_error(error));
                    }
                }
            } else {
                casa_calibration::subtract_visibility_model(&ms.path, reverse)
                    .map_err(runtime_error)?;
            }
            Ok(true)
        })
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
