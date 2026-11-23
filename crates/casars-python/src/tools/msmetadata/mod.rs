// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `msmetadata` tool (msmd): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1029 (`stubs.rs`).

mod data_description;
mod intents;
mod names;
mod scan_index;
mod scan_queries;
mod spectral;
mod spectral_classes;
mod spw_membership;
mod stubs;
pub(crate) mod surface;
mod time_state_queries;

use std::path::Path;
use std::sync::Mutex;

use casa_ms::{MeasurementSet, MsResult};
use casa_tables::lexical_absolute;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::tools::not_yet_implemented_in;
use scan_index::ScanIndex;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1029;

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("msmetadata", method, ISSUE)
}

/// CASA's `msmetadata` tool.
#[pyclass(name = "msmetadata", module = "casars.tools", frozen)]
pub(crate) struct PyMsMetadata {
    attached: Mutex<Option<Attachment>>,
}

struct Attachment {
    ms: MeasurementSet,
    max_cache_bytes: usize,
    scans: Option<ScanIndex>,
}

impl PyMsMetadata {
    fn with_attachment<T: Send>(
        &self,
        py: Python<'_>,
        query: impl FnOnce(&mut Attachment) -> MsResult<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let attached = attached.as_mut().ok_or_else(|| {
                PyRuntimeError::new_err("Tool is not attached to an MS. Use open()")
            })?;
            query(attached).map_err(runtime_error)
        })
    }

    fn with_ms<T: Send>(
        &self,
        py: Python<'_>,
        query: impl FnOnce(&MeasurementSet) -> MsResult<T> + Send,
    ) -> PyResult<T> {
        self.with_attachment(py, |attached| query(&attached.ms))
    }

    fn with_scan_index<T: Send>(
        &self,
        py: Python<'_>,
        query: impl FnOnce(&MeasurementSet, &ScanIndex) -> MsResult<T> + Send,
    ) -> PyResult<T> {
        self.with_attachment(py, |attached| {
            if let Some(index) = &attached.scans {
                return query(&attached.ms, index);
            }
            let index = ScanIndex::read(&attached.ms)?;
            if index.cached_bytes() <= attached.max_cache_bytes {
                let index = attached.scans.insert(index);
                query(&attached.ms, index)
            } else {
                query(&attached.ms, &index)
            }
        })
    }

    fn observation_column(&self, py: Python<'_>, column: &str) -> PyResult<Vec<String>> {
        self.with_ms(py, |ms| {
            let observation = ms.observation()?;
            (0..observation.row_count())
                .map(|row| observation.string(row, column))
                .collect()
        })
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pymethods]
impl PyMsMetadata {
    #[new]
    fn new() -> Self {
        Self {
            attached: Mutex::new(None),
        }
    }

    #[pyo3(signature = (msfile = "", maxcache = 50.0), text_signature = "($self, msfile='', maxcache=50.0)")]
    fn open(&self, py: Python<'_>, msfile: &str, maxcache: f64) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let path = lexical_absolute(Path::new(if msfile.is_empty() { "." } else { msfile }));
            let ms = MeasurementSet::open(path).map_err(runtime_error)?;
            ms.check_referential_integrity().map_err(runtime_error)?;
            *attached = Some(Attachment {
                ms,
                // CASA measures its metadata cache in decimal megabytes.
                max_cache_bytes: (maxcache * 1e6) as usize,
                scans: None,
            });
            Ok(true)
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn close(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            self.attached.lock().map_err(runtime_error)?.take();
            Ok(true)
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.close(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn name(&self, py: Python<'_>) -> PyResult<String> {
        self.with_ms(py, |ms| {
            Ok(ms
                .path()
                .expect("an attached MS has a path")
                .display()
                .to_string())
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn nantennas(&self, py: Python<'_>) -> PyResult<usize> {
        self.with_ms(py, |ms| Ok(ms.antenna()?.row_count()))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn nfields(&self, py: Python<'_>) -> PyResult<usize> {
        self.with_ms(py, |ms| Ok(ms.field()?.row_count()))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn nobservations(&self, py: Python<'_>) -> PyResult<usize> {
        self.with_ms(py, |ms| Ok(ms.observation()?.row_count()))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn nstates(&self, py: Python<'_>) -> PyResult<usize> {
        // CASA counts an empty STATE subtable as one unspecified state.
        self.with_ms(py, |ms| Ok(ms.state()?.row_count().max(1)))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn fieldnames(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.with_ms(py, |ms| {
            let field = ms.field()?;
            (0..field.row_count()).map(|row| field.name(row)).collect()
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn observatorynames(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.observation_column(py, "TELESCOPE_NAME")
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn observers(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.observation_column(py, "OBSERVER")
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn projects(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.observation_column(py, "PROJECT")
    }
}
