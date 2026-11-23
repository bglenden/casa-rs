// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only metadata attached to an MS tool's current selection.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};
use casa_tables::{Table, TableOptions, TablePartNames};
use pyo3::prelude::*;
use tempfile::TempDir;

use super::{AttachedMs, PyMsTool, runtime_error};
use crate::tools::msmetadata::PyMsMetadata;

#[derive(Default)]
struct State {
    selected: Option<Arc<[usize]>>,
    view: Option<Arc<MetadataView>>,
}

/// The metadata tools from one MS attachment share its current selection.
pub(in crate::tools) struct MetadataLink {
    path: PathBuf,
    name: PathBuf,
    state: Mutex<State>,
}

pub(in crate::tools) struct MetadataSnapshot {
    pub(in crate::tools) ms: MeasurementSet,
    // The lazy MS table keeps the selection descriptors alive while reading.
    view: Option<Arc<MetadataView>>,
}

struct MetadataView {
    path: PathBuf,
    links: Option<HashMap<SubtableId, PathBuf>>,
    _scratch: Option<TempDir>,
}

impl MetadataSnapshot {
    pub(in crate::tools) fn from_open(ms: MeasurementSet) -> Self {
        Self { ms, view: None }
    }
}

impl MetadataLink {
    pub(super) fn new(path: PathBuf, name: PathBuf, selected: Option<Arc<[usize]>>) -> Self {
        Self {
            path,
            name,
            state: Mutex::new(State {
                selected,
                view: None,
            }),
        }
    }

    pub(in crate::tools) fn name(&self) -> &Path {
        &self.name
    }

    pub(super) fn set_selected(&self, selected: Option<Arc<[usize]>>) -> MsResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        state.selected = selected;
        state.view = None;
        Ok(())
    }

    pub(in crate::tools) fn snapshot(
        &self,
        cached: Option<&Arc<MetadataSnapshot>>,
    ) -> MsResult<Arc<MetadataSnapshot>> {
        let view = {
            let mut state = self
                .state
                .lock()
                .map_err(|error| MsError::InvalidInput(error.to_string()))?;
            if state.view.is_none() {
                state.view = Some(Arc::new(self.view(state.selected.as_deref())?));
            }
            Arc::clone(state.view.as_ref().expect("metadata view exists"))
        };
        if let Some(snapshot) = cached
            && snapshot
                .view
                .as_ref()
                .is_some_and(|previous| Arc::ptr_eq(previous, &view))
        {
            return Ok(Arc::clone(snapshot));
        }
        let ms = match &view.links {
            Some(links) => MeasurementSet::open_with_subtable_paths(&view.path, links)?,
            None => MeasurementSet::open(&view.path)?,
        };
        Ok(Arc::new(MetadataSnapshot {
            ms,
            view: Some(view),
        }))
    }

    fn view(&self, selected: Option<&[usize]>) -> MsResult<MetadataView> {
        let concatenated = contains_concatenation(&self.path)?;
        if selected.is_none() && !concatenated {
            return Ok(MetadataView {
                path: self.path.clone(),
                links: None,
                _scratch: None,
            });
        }
        let source = Table::open(TableOptions::new(&self.path))?;
        let view = tempfile::Builder::new()
            .prefix("casars-ms-metadata-")
            .tempdir()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let path = if let Some(rows) = selected {
            let path = view.path().join("selection.ms");
            source.select_rows(rows)?.save(TableOptions::new(&path))?;
            path
        } else {
            self.path.clone()
        };
        let links = if concatenated {
            Some(concatenated_subtables(&source, view.path())?)
        } else {
            None
        };
        Ok(MetadataView {
            path,
            links,
            _scratch: Some(view),
        })
    }
}

fn contains_concatenation(path: &Path) -> MsResult<bool> {
    let mut path = path.to_path_buf();
    for _ in 0..128 {
        match Table::immediate_part_names(&path, &path)? {
            TablePartNames::Reference(parent) => path = parent,
            TablePartNames::Concatenation { .. } => return Ok(true),
            TablePartNames::Plain(_) => return Ok(false),
        }
    }
    Err(MsError::InvalidInput(
        "MeasurementSet reference chain is too deep".into(),
    ))
}

fn concatenated_subtables(main: &Table, scratch: &Path) -> MsResult<HashMap<SubtableId, PathBuf>> {
    let mut result = HashMap::new();
    for &id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        let paths = match MeasurementSet::linked_subtable_paths(main, id) {
            Ok(paths) => paths,
            Err(MsError::MissingSubtable(_)) => continue,
            Err(error) => return Err(error),
        };
        let path = if let [path] = paths.as_slice() {
            path.clone()
        } else {
            let tables = paths
                .into_iter()
                .map(|path| Table::open(TableOptions::new(path)))
                .collect::<Result<Vec<_>, _>>()?;
            let path = scratch.join(id.name());
            Table::concat(tables)?.save(TableOptions::new(&path))?;
            path
        };
        result.insert(id, path);
    }
    Ok(result)
}

impl AttachedMs {
    pub(super) fn set_selected_rows(&mut self, rows: Option<Vec<usize>>) -> PyResult<()> {
        if let Some(metadata) = self.metadata.get() {
            metadata
                .set_selected(rows.as_deref().map(Arc::from))
                .map_err(runtime_error)?;
        }
        self.selected_rows = rows;
        Ok(())
    }

    fn metadata_link(&self) -> Arc<MetadataLink> {
        Arc::clone(self.metadata.get_or_init(|| {
            Arc::new(MetadataLink::new(
                self.path.clone(),
                self.name.clone(),
                self.selected_rows.as_deref().map(Arc::from),
            ))
        }))
    }
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (cachesize = None), text_signature = "($self, cachesize=50.0)")]
    fn metadata(
        &self,
        py: Python<'_>,
        cachesize: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyMsMetadata>> {
        // The legacy float binding also accepts nonnumeric inputs. As in
        // CASA, the cache budget affects retained metadata, never results.
        let megabytes = cachesize
            .and_then(|value| value.extract::<f32>().ok())
            .unwrap_or(50.0);
        let link =
            self.attachment_info(py, |attached| Ok(attached.map(AttachedMs::metadata_link)))?;
        Py::new(py, PyMsMetadata::from_ms(link, megabytes.into()))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn reset(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            if let Some(ms) = attached.as_mut() {
                ms.set_selected_rows(None)?;
            }
            Ok(true)
        })
    }
}
