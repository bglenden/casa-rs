// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only metadata attached to an MS tool's current selection.

use std::collections::HashMap;
use std::fs::{self, Metadata};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};
use casa_tables::{Table, TableOptions, TablePartNames};
use pyo3::prelude::*;
use tempfile::TempDir;

use super::attachment_info::PlainRows;
use super::{AttachedMs, PyMsTool, runtime_error};
use crate::tools::msmetadata::PyMsMetadata;

struct State {
    selected: Option<Arc<[usize]>>,
    plain_rows: Option<PlainRows>,
    view: Option<Arc<MetadataView>>,
}

/// The metadata tools from one MS attachment share its current selection.
pub(in crate::tools) struct MetadataLink {
    path: PathBuf,
    name: PathBuf,
    directory: Metadata,
    original_rows: usize,
    live_concatenation: bool,
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

    pub(super) fn open(path: &Path) -> MsResult<Self> {
        Self::from_view(Arc::new(MetadataView::new(path, None)?))
    }

    fn from_view(view: Arc<MetadataView>) -> MsResult<Self> {
        let ms = match &view.links {
            Some(links) => MeasurementSet::open_with_subtable_paths(&view.path, links)?,
            None => MeasurementSet::open(&view.path)?,
        };
        Ok(Self {
            ms,
            view: Some(view),
        })
    }
}

impl MetadataLink {
    fn new(attached: &AttachedMs) -> Self {
        Self {
            path: attached.path.clone(),
            name: attached.name.clone(),
            directory: attached.directory.clone(),
            original_rows: attached.original_rows,
            live_concatenation: matches!(attached.part_names, TablePartNames::Concatenation { .. }),
            state: Mutex::new(State {
                selected: attached.selected_rows.clone(),
                plain_rows: attached
                    .plain_rows
                    .as_ref()
                    .map(|_| PlainRows::new(&attached.path)),
                view: None,
            }),
        }
    }

    pub(in crate::tools) fn name(&self) -> &Path {
        &self.name
    }

    pub(in crate::tools) fn row_count(&self) -> MsResult<usize> {
        self.verify_directory()?;
        let state = self
            .state
            .lock()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        if let Some(rows) = &state.selected {
            return Ok(rows.len());
        }
        let rows = match &state.plain_rows {
            Some(rows) => rows.count(&self.path)?,
            None if self.live_concatenation => {
                Table::open(TableOptions::new(&self.path))?.row_count()
            }
            None => self.original_rows,
        };
        self.verify_directory()?;
        Ok(rows)
    }

    pub(in crate::tools) fn spectral_window_count(&self) -> MsResult<usize> {
        self.verify_directory()?;
        // A MAIN row selection leaves its linked spectral-window table intact.
        let main = Table::open(TableOptions::new(&self.path))?;
        let paths = MeasurementSet::linked_subtable_paths(&main, SubtableId::SpectralWindow)?;
        let mut count = 0;
        for path in paths {
            count += Table::open(TableOptions::new(path))?.row_count();
        }
        self.verify_directory()?;
        Ok(count)
    }

    pub(in crate::tools) fn verify_directory(&self) -> MsResult<()> {
        let valid =
            fs::metadata(&self.path).is_ok_and(|current| same_directory(&self.directory, &current));
        if valid {
            return Ok(());
        }
        Err(MsError::InvalidInput(format!(
            "original MeasurementSet attachment at {} is unavailable or has been replaced",
            self.name.display()
        )))
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
        self.verify_directory()?;
        let view = {
            let mut state = self
                .state
                .lock()
                .map_err(|error| MsError::InvalidInput(error.to_string()))?;
            if state.view.is_none() {
                state.view = Some(Arc::new(MetadataView::new(
                    &self.path,
                    state.selected.as_deref(),
                )?));
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
        let snapshot = MetadataSnapshot::from_view(view)?;
        self.verify_directory()?;
        Ok(Arc::new(snapshot))
    }
}

impl MetadataView {
    fn new(main: &Path, selected: Option<&[usize]>) -> MsResult<Self> {
        let concatenated = contains_concatenation(main)?;
        if selected.is_none() && !concatenated {
            return Ok(Self {
                path: main.to_path_buf(),
                links: None,
                _scratch: None,
            });
        }
        let source = Table::open(TableOptions::new(main))?;
        let view = tempfile::Builder::new()
            .prefix("casars-ms-metadata-")
            .tempdir()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let path = if let Some(rows) = selected {
            let path = view.path().join("selection.ms");
            source.select_rows(rows)?.save(TableOptions::new(&path))?;
            path
        } else {
            main.to_path_buf()
        };
        let links = if concatenated {
            Some(concatenated_subtables(&source, view.path())?)
        } else {
            None
        };
        Ok(Self {
            path,
            links,
            _scratch: Some(view),
        })
    }
}

fn same_directory(expected: &Metadata, current: &Metadata) -> bool {
    if !current.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        expected.dev() == current.dev() && expected.ino() == current.ino()
    }
    #[cfg(not(unix))]
    {
        expected
            .created()
            .ok()
            .zip(current.created().ok())
            .is_some_and(|(expected, current)| expected == current)
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
    let ids = SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
        .copied()
        .collect::<Vec<_>>();
    let mut linked = MeasurementSet::linked_subtables(main, &ids)?;
    let mut result = HashMap::new();
    for id in ids {
        let Some(paths) = linked.remove(&id) else {
            continue;
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
    pub(super) fn reset_visibility_selection(&mut self) -> PyResult<()> {
        self.set_selected_rows(None)?;
        self.selection_state = super::selection_indices::SelectionState::default();
        self.visibility.reset_selection();
        Ok(())
    }

    pub(super) fn set_selected_rows(&mut self, rows: Option<Vec<usize>>) -> PyResult<()> {
        let rows = rows.map(Arc::from);
        if let Some(metadata) = self.metadata.get() {
            metadata.set_selected(rows.clone()).map_err(runtime_error)?;
        }
        self.selected_rows = rows;
        Ok(())
    }

    pub(super) fn metadata_link(&self) -> Arc<MetadataLink> {
        Arc::clone(
            self.metadata
                .get_or_init(|| Arc::new(MetadataLink::new(self))),
        )
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
                ms.reset_visibility_selection()?;
            }
            Ok(true)
        })
    }
}
