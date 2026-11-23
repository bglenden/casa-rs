// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `msmetadata` tool (msmd): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025).

mod antenna_metadata;
mod column_units;
mod data_description;
mod direction_metadata;
mod intents;
mod main_quantities;
mod main_scalar_batches;
mod main_summary;
mod names;
mod observation_metadata;
mod scan_index;
mod scan_queries;
mod source_metadata;
mod spectral;
mod spectral_classes;
mod spw_membership;
pub(crate) mod surface;
mod time_state_queries;

use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use casa_ms::{MeasurementSet, MsResult};
use casa_tables::lexical_absolute;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::ms::metadata_link::{MetadataLink, MetadataSnapshot};
pub(super) use direction_metadata::direction_record;
use scan_index::ScanIndex;

/// CASA's `msmetadata` tool.
#[pyclass(name = "msmetadata", module = "casars.tools", frozen)]
pub(crate) struct PyMsMetadata {
    attached: Mutex<Option<Attachment>>,
}

struct Attachment {
    source: Source,
    max_cache_bytes: usize,
    snapshot: Option<Arc<MetadataSnapshot>>,
    scans: Option<ScanIndex>,
    scan_source: Weak<MetadataSnapshot>,
}

enum Source {
    Open(Arc<MetadataSnapshot>),
    Linked(Arc<MetadataLink>),
}

impl Source {
    fn name(&self) -> &Path {
        match self {
            Self::Open(snapshot) => snapshot.ms.path().expect("an attached MS has a path"),
            Self::Linked(link) => link.name(),
        }
    }
}

impl Attachment {
    fn new(source: Source, maxcache: f64) -> Self {
        Self {
            source,
            // CASA measures its metadata cache in decimal megabytes.
            max_cache_bytes: (maxcache * 1e6) as usize,
            snapshot: None,
            scans: None,
            scan_source: Weak::new(),
        }
    }

    fn snapshot(&mut self) -> MsResult<Arc<MetadataSnapshot>> {
        let snapshot = match &self.source {
            Source::Open(snapshot) => return Ok(Arc::clone(snapshot)),
            Source::Linked(link) => link.snapshot(self.snapshot.as_ref())?,
        };
        if self.max_cache_bytes > 0 {
            self.snapshot = Some(Arc::clone(&snapshot));
        }
        Ok(snapshot)
    }
}

impl PyMsMetadata {
    pub(super) fn from_ms(link: Option<Arc<MetadataLink>>, maxcache: f64) -> Self {
        Self {
            attached: Mutex::new(link.map(|link| Attachment::new(Source::Linked(link), maxcache))),
        }
    }

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
        self.with_attachment(py, |attached| query(&attached.snapshot()?.ms))
    }

    fn with_scan_index<T: Send>(
        &self,
        py: Python<'_>,
        query: impl FnOnce(&MeasurementSet, &ScanIndex) -> MsResult<T> + Send,
    ) -> PyResult<T> {
        self.with_attachment(py, |attached| {
            let snapshot = attached.snapshot()?;
            let ms = &snapshot.ms;
            if attached.scan_source.as_ptr() != Arc::as_ptr(&snapshot) {
                attached.scans = None;
                attached.scan_source = Arc::downgrade(&snapshot);
            }
            if let Some(index) = &attached.scans {
                return query(ms, index);
            }
            let index = ScanIndex::read(ms)?;
            if index.cached_bytes() <= attached.max_cache_bytes {
                let index = attached.scans.insert(index);
                query(ms, index)
            } else {
                query(ms, &index)
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
            let source = Source::Open(Arc::new(MetadataSnapshot::from_open(ms)));
            *attached = Some(Attachment::new(source, maxcache));
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
        self.with_attachment(py, |attached| {
            Ok(attached.source.name().display().to_string())
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
