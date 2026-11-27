// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `ms` tool (ms): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025).

mod addephemeris;
mod asdm_reference;
mod attachment_info;
mod concatenate;
mod continuum;
mod fits_visibility;
mod history;
mod iteration;
mod legacy_iteration;
mod legacy_query_config;
mod legacy_query_output;
mod legacy_selection;
mod legacy_statwt;
mod legacy_visibility;
mod lister;
mod listfits;
pub(super) mod metadata_link;
mod multims;
mod owned_record;
mod partition;
mod putdata;
mod range;
mod scan_summary;
mod selection_indices;
mod selection_record;
mod sort;
mod spectral_grid;
mod spectral_rewrite;
mod split;
mod statistics;
mod statwt;
mod subtable_queries;
mod summary;
pub(crate) mod surface;
mod vi1;
mod visibility;
mod visibility_reference;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions, TablePartNames, TableWriteLock, table_cache_name};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBoolMethods};

/// CASA's `ms` tool.
#[pyclass(name = "ms", module = "casars.tools", frozen)]
pub(crate) struct PyMsTool {
    attached: Mutex<Option<AttachedMs>>,
}

struct AttachedMs {
    name: PathBuf,
    path: PathBuf,
    directory: Metadata,
    main: Table,
    main_files: attachment_info::TableFiles,
    main_stamp: Option<attachment_info::TableStamp>,
    asdm_index: Option<Option<Arc<casa_tables::AsdmBdfIndex>>>,
    #[cfg(unix)]
    history_snapshot: Option<history::HistorySnapshot>,
    subtables: subtable_queries::Subtables,
    writable: bool,
    original_rows: usize,
    plain_rows: Option<attachment_info::PlainRows>,
    part_names: TablePartNames,
    is_multi_ms: bool,
    sources: Vec<PathBuf>,
    selected_rows: Option<Arc<[usize]>>,
    legacy_selection: legacy_selection::LegacySelection,
    selection_state: selection_indices::SelectionState,
    visibility: visibility::VisibilityState,
    metadata: OnceLock<Arc<metadata_link::MetadataLink>>,
    exact_time_metadata: Option<Arc<metadata_link::MetadataLink>>,
    write_locks: BTreeMap<PathBuf, TableWriteLock>,
    read_locks: BTreeMap<PathBuf, Table>,
    _cached_paths: Arc<Vec<PathBuf>>,
}

impl AttachedMs {
    fn open(name: PathBuf, writable: bool, lock: bool, check: bool) -> PyResult<Self> {
        let publication = crate::tools::table::lock_attachment_publication();
        Self::open_with_publication_guard(name, writable, lock, check, &publication)
    }

    fn open_with_publication_guard(
        name: PathBuf,
        writable: bool,
        lock: bool,
        check: bool,
        _publication: &std::sync::MutexGuard<'static, ()>,
    ) -> PyResult<Self> {
        let path = name.canonicalize().map_err(runtime_error)?;
        let directory = std::fs::metadata(&path).map_err(runtime_error)?;
        let main_files = attachment_info::TableFiles::new(&path);
        let main_stamp = main_files.stamp();
        let main = Table::open(TableOptions::new(&path)).map_err(runtime_error)?;
        let original_rows = main.row_count();
        let is_multi_ms = main.info().sub_type == "CONCATENATED";
        let part_names = Table::immediate_part_names(&path, &name).map_err(runtime_error)?;
        let reference_multi = is_multi_ms && matches!(part_names, TablePartNames::Reference(_));
        let sources = if reference_multi
            || (is_multi_ms && matches!(part_names, TablePartNames::Plain(_)))
        {
            // A saved view can inherit the multi-MS subtype. Its descriptor
            // holds a row projection, and the view is the logical input.
            let view = MeasurementSet::open(&path).map_err(runtime_error)?;
            if view.ms_version() != Some(2.0) {
                return Err(runtime_error(format!(
                    "{} is not an MSv2 MeasurementSet",
                    path.display()
                )));
            }
            vec![path.clone()]
        } else {
            casa_calibration::uvsub::visibility_model_source_paths(&path).map_err(runtime_error)?
        };
        let physical_parts = if reference_multi && (lock || writable) {
            main.cache_plain_table_paths()
                .map_err(runtime_error)?
                .iter()
                .map(|part| part.canonicalize().map_err(runtime_error))
                .collect::<PyResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let plain_rows = matches!(part_names, TablePartNames::Plain(_))
            .then(|| attachment_info::PlainRows::new(&path));
        let cached_paths = crate::tools::table::attach_table_paths(
            std::iter::once(path.clone())
                .chain(sources.iter().filter(|source| *source != &path).cloned())
                .collect(),
        );
        let mut attached = Self {
            name,
            path,
            directory,
            main,
            main_files,
            main_stamp,
            asdm_index: None,
            #[cfg(unix)]
            history_snapshot: None,
            subtables: subtable_queries::Subtables::default(),
            writable,
            original_rows,
            plain_rows,
            part_names,
            is_multi_ms,
            sources,
            selected_rows: None,
            legacy_selection: legacy_selection::LegacySelection::default(),
            selection_state: selection_indices::SelectionState::default(),
            visibility: visibility::VisibilityState::default(),
            metadata: OnceLock::new(),
            exact_time_metadata: None,
            write_locks: BTreeMap::new(),
            read_locks: BTreeMap::new(),
            _cached_paths: cached_paths,
        };
        if writable {
            for source in attached.sources.iter().chain(&physical_parts) {
                // CASA opens MAIN for update before reporting a writable attachment.
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(source.join("table.dat"))
                    .map_err(runtime_error)?;
            }
        }
        if lock {
            attached.retain_tables(
                attached
                    .sources
                    .iter()
                    .chain(&physical_parts)
                    .cloned()
                    .collect(),
            )?;
            let mut subtables = BTreeSet::new();
            for source in attached.sources.iter().chain(&physical_parts) {
                let ms = MeasurementSet::open(source).map_err(runtime_error)?;
                for id in ms.subtable_ids() {
                    if let Some(path) = ms.subtable(id).and_then(Table::path) {
                        subtables.insert(path.canonicalize().map_err(runtime_error)?);
                    }
                }
            }
            attached.retain_tables(subtables)?;
        }
        if check {
            for source in &attached.sources {
                MeasurementSet::open(source)
                    .and_then(|ms| ms.check_referential_integrity())
                    .map_err(runtime_error)?;
            }
        }
        Ok(attached)
    }

    fn retain_tables(&mut self, tables: BTreeSet<PathBuf>) -> PyResult<()> {
        for path in tables {
            if self.writable {
                if let Entry::Vacant(entry) = self.write_locks.entry(path) {
                    let lock =
                        TableWriteLock::acquire_to_reread(entry.key(), 1).map_err(runtime_error)?;
                    entry.insert(lock);
                }
            } else {
                self.retain_read(path)?;
            }
        }
        Ok(())
    }

    fn retain_read(&mut self, path: PathBuf) -> PyResult<()> {
        #[cfg(unix)]
        if let Entry::Vacant(entry) = self.read_locks.entry(path) {
            let lock = casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking)
                .read_only(true);
            let table = Table::open_with_lock(casa_tables::TableOptions::new(entry.key()), lock)
                .map_err(runtime_error)?;
            entry.insert(table);
        }
        #[cfg(not(unix))]
        let _ = path;
        Ok(())
    }
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
        #[pyo3(from_py_with = strict_bool)] nomodify: bool,
        #[pyo3(from_py_with = strict_bool)] lock: bool,
        #[pyo3(from_py_with = strict_bool)] check: bool,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            *attached = None;
            let path = table_cache_name(Path::new(thems)).map_err(runtime_error)?;
            *attached = Some(AttachedMs::open(path, !nomodify, lock, check)?);
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
    fn uvsub(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = strict_bool)] reverse: bool,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut().filter(|ms| ms.writable) else {
                return Ok(false);
            };
            if let Some(selected) = &ms.selected_rows {
                let mut offset = 0;
                for path in &ms.sources {
                    let mut part = MeasurementSet::open(path).map_err(runtime_error)?;
                    let end = offset + part.row_count();
                    let first = selected.partition_point(|&row| row < offset);
                    let last = selected.partition_point(|&row| row < end);
                    let rows = selected[first..last]
                        .iter()
                        .map(|row| row - offset)
                        .collect::<Vec<_>>();
                    offset = end;
                    if rows.is_empty() {
                        continue;
                    }
                    let mut temporary = None;
                    let lock = if let Some(lock) = ms.write_locks.get_mut(path) {
                        lock
                    } else {
                        temporary.insert(
                            TableWriteLock::acquire_to_reread(path, 0).map_err(runtime_error)?,
                        )
                    };
                    casa_calibration::uvsub::subtract_visibility_model_selected(
                        &mut part,
                        reverse,
                        Some(&rows),
                        lock,
                    )
                    .map_err(runtime_error)?;
                    drop(part);
                    if let Some(lock) = temporary {
                        lock.release().map_err(runtime_error)?;
                    }
                }
            } else if ms.write_locks.is_empty() {
                casa_calibration::uvsub::subtract_visibility_model(&ms.path, reverse)
                    .map_err(runtime_error)?;
            } else {
                for path in &ms.sources {
                    let lock = ms
                        .write_locks
                        .get_mut(path)
                        .expect("permanent MS source lock");
                    casa_calibration::uvsub::subtract_visibility_model_holding(path, reverse, lock)
                        .map_err(runtime_error)?;
                }
            }
            Ok(true)
        })
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn strict_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(value.cast::<PyBool>()?.is_true())
}
