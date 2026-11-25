// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read visibility records from the current MS attachment.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use casa_coordinates::StokesType;
use casa_ms::tool_iteration::ToolIteration;
use casa_ms::tool_range::ToolRangeRequest;
use casa_ms::tool_visibility::{self, VisibilityChannels, VisibilityPolarization};
use casa_ms::{MeasurementSet, MsError, MsSelection};
use casa_tables::{PlainReferenceRead, Table, TableOptions, TablePartNames, TableWriteLock};
use casa_types::RecordValue;
use casa_types::measures::MeasuresProvider;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyListMethods, PyString};

use super::attachment_info::{TableFiles, TableStamp};
use super::iteration::VisibilityIteration;
use super::metadata_link::{MetadataLink, MetadataSnapshot};
use super::owned_record::visibility_record;
use super::subtable_queries::Subtables;
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;

#[derive(Default)]
pub(super) struct VisibilityState {
    initialized: bool,
    averaged: bool,
    baseline_write: Option<(i32, i64)>,
    recent_baseline_read: bool,
    main: Option<Arc<VisibilityMain>>,
    range_subtables: Subtables,
    pub(super) time_measures: OnceLock<Arc<dyn MeasuresProvider>>,
    channels: Option<VisibilityChannels>,
    polarization: Option<Arc<VisibilityPolarization>>,
    pub(super) iteration: Option<VisibilityIteration>,
}

pub(super) struct VisibilityMain {
    table: Table,
    members: Vec<Table>,
    reference: Option<PlainReferenceRead>,
    publication: Vec<VisibilityPublication>,
    plain: bool,
}

struct VisibilityPublication {
    path: PathBuf,
    files: TableFiles,
    stamp: Option<TableStamp>,
}

struct VisibilityWriteTarget {
    path: PathBuf,
    names: Vec<String>,
    rows: BTreeMap<usize, usize>,
}

impl VisibilityState {
    pub(super) fn reset_selection(&mut self) {
        self.initialized = false;
        self.averaged = false;
        self.baseline_write = None;
        self.recent_baseline_read = false;
        self.channels = None;
        self.polarization = None;
        if let Some(iteration) = &mut self.iteration {
            iteration.cursor.reset_blocking();
            iteration.conversion = None;
        }
    }

    pub(super) fn set_initialized(&mut self, initialized: bool) {
        self.initialized = initialized;
    }

    fn set_polarization(&mut self, plan: Option<Arc<VisibilityPolarization>>) -> PyResult<()> {
        if let Some(iteration) = &mut self.iteration {
            iteration.conversion = plan
                .as_deref()
                .filter(|plan| plan.is_conversion())
                .zip(iteration.polarization.as_deref())
                .map(|(live, captured)| live.over_recorded_selection(captured).map(Arc::new))
                .transpose()
                .map_err(runtime_error)?;
        }
        self.polarization = plan;
        Ok(())
    }

    fn main(&mut self, path: &Path) -> PyResult<&VisibilityMain> {
        let current = self.main.as_ref().is_some_and(|main| {
            main.publication
                .iter()
                .all(|source| source.stamp.is_some() && source.files.stamp() == source.stamp)
        });
        if !current {
            self.main = Some(Arc::new(VisibilityMain::open(path)?));
            self.range_subtables = Subtables::default();
        }
        Ok(self.main.as_deref().expect("MAIN table was opened"))
    }

    pub(super) fn range_for_rows(
        &mut self,
        path: &Path,
        link: &MetadataLink,
        requested_rows: Option<&[usize]>,
        request: &ToolRangeRequest,
    ) -> PyResult<CasaValue> {
        link.verify_directory().map_err(runtime_error)?;
        if requested_rows.is_some_and(<[usize]>::is_empty) {
            return Ok(CasaValue::empty_record());
        }
        self.main(path)?;
        let main = self.main.as_ref().expect("MAIN was opened");
        let all_rows;
        let rows = if let Some(rows) = requested_rows {
            rows
        } else {
            all_rows = (0..main.table.row_count()).collect::<Vec<_>>();
            &all_rows
        };
        let reference = main.reference.as_ref();
        let owner = reference.map_or(&main.table, PlainReferenceRead::parent);
        let record = request
            .read(&main.table, &main.members, reference, rows, |ids| {
                self.range_subtables.read_range(owner, ids)
            })
            .map_err(runtime_error)?;
        link.verify_directory().map_err(runtime_error)?;
        Ok(visibility_record(record))
    }
}

impl VisibilityMain {
    fn open(path: &Path) -> PyResult<Self> {
        // Record publication stamps before opening the row metadata. A writer
        // publishing during a read forces the next call to reopen the layout.
        let mut publication = Vec::new();
        let table = Table::open_with_layout_paths(TableOptions::new(path), |path| {
            let files = TableFiles::new(path);
            let stamp = files.stamp();
            publication.push(VisibilityPublication {
                path: path.to_owned(),
                files,
                stamp,
            });
        })
        .map_err(runtime_error)?;
        let parts = Table::immediate_part_names(path, path).map_err(runtime_error)?;
        let plain = matches!(&parts, TablePartNames::Plain(_));
        let (members, reference) = match parts {
            TablePartNames::Plain(_) => (Vec::new(), None),
            TablePartNames::Reference(_) => (
                Vec::new(),
                Table::open_plain_reference(path).map_err(runtime_error)?,
            ),
            TablePartNames::Concatenation { parts, .. } => (Self::open_plain_members(parts)?, None),
        };
        Ok(Self {
            table,
            members,
            reference,
            publication,
            plain,
        })
    }

    fn verify_iterator_layout(&self) -> PyResult<()> {
        // A direct reference carries its own saved row and column projection.
        // Its physical parent still needs to describe the same rows.
        let dependencies = if self.reference.is_some() {
            &self.publication[1..]
        } else {
            &self.publication[..]
        };
        if !dependencies
            .iter()
            .all(|source| source.files.same_descriptor(source.stamp))
        {
            return Err(runtime_error(
                "MeasurementSet iterator row layout changed. Call iterinit again.",
            ));
        }
        Ok(())
    }

    fn verify_write_layout(&self, written: &BTreeSet<PathBuf>) -> PyResult<()> {
        if self.publication.iter().all(|source| {
            source.files.same_descriptor(source.stamp)
                || (!written.is_empty()
                    && source
                        .path
                        .canonicalize()
                        .is_ok_and(|path| written.contains(&path)))
        }) {
            Ok(())
        } else {
            Err(runtime_error(
                "MeasurementSet row layout changed. Refresh the selection before putdata.",
            ))
        }
    }

    fn write_targets(
        &self,
        rows: &[usize],
        names: &[&str],
    ) -> PyResult<Vec<VisibilityWriteTarget>> {
        let own_names = || names.iter().map(|name| (*name).to_owned()).collect();
        if self.plain {
            return Ok(vec![VisibilityWriteTarget {
                path: self.table.path().expect("disk-backed MAIN").to_owned(),
                names: own_names(),
                rows: rows.iter().copied().zip(0..).collect(),
            }]);
        }
        if let Some(reference) = self
            .table
            .reference_write_projection(rows, names)
            .map_err(runtime_error)?
        {
            return Ok(vec![VisibilityWriteTarget {
                path: reference.path.canonicalize().map_err(runtime_error)?,
                names: reference.columns,
                rows: reference.rows.into_iter().zip(0..).collect(),
            }]);
        }
        let mut count = 0;
        let mut ends = Vec::with_capacity(self.members.len());
        let mut paths = Vec::with_capacity(self.members.len());
        for part in &self.members {
            count += part.row_count();
            ends.push(count);
            paths.push(
                part.path()
                    .expect("disk-backed multi-MS member")
                    .canonicalize()
                    .map_err(runtime_error)?,
            );
        }
        if ends.is_empty() || count != self.table.row_count() {
            return Err(runtime_error(
                "putdata cannot map the physical rows of this concatenation (#1030)",
            ));
        }
        let mut groups = BTreeMap::<&Path, BTreeMap<usize, usize>>::new();
        for (position, &row) in rows.iter().enumerate() {
            let index = ends.partition_point(|&end| end <= row);
            let path = paths
                .get(index)
                .ok_or_else(|| runtime_error("putdata row is outside the multi-MS"))?;
            let first = if index == 0 { 0 } else { ends[index - 1] };
            groups
                .entry(path)
                .or_default()
                .insert(row - first, position);
        }
        Ok(groups
            .into_iter()
            .map(|(path, rows)| VisibilityWriteTarget {
                path: path.to_owned(),
                names: own_names(),
                rows,
            })
            .collect())
    }

    fn open_plain_members(paths: Vec<PathBuf>) -> PyResult<Vec<Table>> {
        let mut members = Vec::with_capacity(paths.len());
        for path in paths {
            if !matches!(
                Table::immediate_part_names(&path, &path).map_err(runtime_error)?,
                TablePartNames::Plain(_)
            ) {
                return Ok(Vec::new());
            }
            members.push(Table::open(TableOptions::new(path)).map_err(runtime_error)?);
        }
        Ok(members)
    }

    fn read_selected(
        &self,
        selected: Option<&[usize]>,
        mut rows: Vec<usize>,
        items: &[String],
        metadata: Option<&MeasurementSet>,
        options: tool_visibility::VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        if let Some(selected) = selected {
            for row in &mut rows {
                *row = selected[*row];
            }
        }
        self.read_rows(&rows, items, metadata, options)
    }

    fn read_rows(
        &self,
        rows: &[usize],
        items: &[String],
        metadata: Option<&MeasurementSet>,
        options: tool_visibility::VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        let record = tool_visibility::read_tool_visibility(
            &self.table,
            &self.members,
            self.reference.as_ref(),
            rows,
            items,
            metadata,
            options,
        )
        .map_err(runtime_error)?;
        Ok(visibility_record(record))
    }
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None, ifraxis = false, ifraxisgap = 0, increment = 1, average = false), text_signature = "($self, items=[], ifraxis=False, ifraxisgap=0, increment=1, average=False)")]
    fn getdata(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] ifraxis: bool,
        #[pyo3(from_py_with = gap_argument)] ifraxisgap: i64,
        #[pyo3(from_py_with = increment_argument)] increment: i64,
        #[pyo3(from_py_with = strict_bool)] average: bool,
    ) -> PyResult<Py<PyAny>> {
        let items = requested_items(items)?;
        let options = tool_visibility::VisibilityReadOptions {
            average,
            baseline_gap: ifraxis.then_some(ifraxisgap as i32),
            baseline_pairs: None,
            channels: None,
            polarization: None,
            time_measures: None,
            iterator_read: false,
        };
        let reply = py.detach(|| -> PyResult<CasaValue> {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(CasaValue::empty_record());
            };
            let reply = ms.getdata_rows(&items, increment, options)?;
            if ms.writable {
                if ifraxis {
                    ms.visibility.baseline_write = Some((ifraxisgap as i32, increment));
                }
                if let CasaValue::Record(fields) = &reply
                    && fields.iter().any(|(name, _)| {
                        tool_visibility::VisibilityWriteRecord::accepts_column(name)
                    })
                {
                    ms.visibility.recent_baseline_read = ifraxis;
                }
            }
            Ok(reply)
        })?;
        reply.into_python(py)
    }

    #[pyo3(signature = (nchan = 1, start = 0, width = 1, inc = 1), text_signature = "($self, nchan=1, start=0, width=1, inc=1)")]
    fn selectchannel(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = nchan_argument)] nchan: i64,
        #[pyo3(from_py_with = start_argument)] start: i64,
        #[pyo3(from_py_with = width_argument)] width: i64,
        #[pyo3(from_py_with = channel_increment)] inc: i64,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            let link = ms.metadata_link();
            if link.row_count().map_err(runtime_error)? == 0 {
                return Err(runtime_error(
                    "MSIter::construct - Input MeasurementSet.has zero selected rows",
                ));
            }
            let Some(snapshot) = ms.initialize_visibility(&link)? else {
                return Ok(false);
            };
            if nchan < 1 || start < 0 || width < 1 || inc < 1 {
                return Ok(false);
            }
            let channels =
                tool_visibility::selected_channel_count(&snapshot.ms).map_err(runtime_error)?;
            let Some(selection) = VisibilityChannels::new(nchan, start, width, inc, channels)
            else {
                return Ok(false);
            };
            ms.visibility.channels = Some(selection);
            Ok(true)
        })
    }

    #[pyo3(signature = (wantedpol = None), text_signature = "($self, wantedpol=[])")]
    fn selectpolarization(
        &self,
        py: Python<'_>,
        wantedpol: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let wanted = requested_strings(wantedpol, "wantedpol")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.select_visibility_polarization(&wanted),
                None => Ok(false),
            }
        })
    }
}

impl AttachedMs {
    pub(super) fn putdata_rows(&mut self, items: RecordValue) -> PyResult<bool> {
        if items.fields().is_empty()
            || self
                .selected_rows
                .as_deref()
                .is_some_and(<[usize]>::is_empty)
            || self.visibility.averaged
            || self.visibility.channels.is_some()
            || self.visibility.iteration.as_ref().is_some_and(|iteration| {
                iteration.channels.is_some() || iteration.conversion.is_some()
            })
            || self
                .visibility
                .polarization
                .as_deref()
                .is_some_and(VisibilityPolarization::is_conversion)
        {
            return Ok(false);
        }
        let link = self.metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        if !self.visibility.initialized && self.initialize_visibility(&link)?.is_none() {
            return Ok(false);
        }
        let captured = self
            .visibility
            .iteration
            .as_ref()
            .and_then(|iteration| iteration.layout.clone());
        let main = if let Some(layout) = captured {
            layout.verify_write_layout(&BTreeSet::new())?;
            layout
        } else {
            self.visibility.main(&self.path)?;
            self.visibility
                .main
                .as_ref()
                .expect("MAIN table was opened")
                .clone()
        };
        let rows = if let Some(iteration) = &self.visibility.iteration {
            iteration.cursor.rows().map_err(runtime_error)?.to_vec()
        } else if let Some(rows) = &self.selected_rows {
            rows.to_vec()
        } else {
            (0..main.table.row_count()).collect()
        };
        if rows.is_empty() {
            return Ok(false);
        }
        let layout = self.visibility.baseline_write;
        let total_rows = self
            .selected_rows
            .as_ref()
            .map_or_else(|| main.table.row_count(), |selected| selected.len());
        let baseline_pairs = if self.visibility.iteration.is_some() {
            layout
                .map(|(_, increment)| self.iteration_baselines(&main, increment))
                .transpose()?
        } else {
            None
        };
        let options = tool_visibility::VisibilityWriteOptions {
            baseline_gap: layout.map(|(gap, _)| gap),
            sampled_baselines: layout.is_some_and(|(_, increment)| {
                increment > 1 && (increment as u32 as usize) <= total_rows
            }),
            recent_baseline_read: self.visibility.recent_baseline_read,
            baseline_pairs: baseline_pairs.as_deref(),
            polarization: self
                .visibility
                .iteration
                .as_ref()
                .map_or(self.visibility.polarization.as_deref(), |iteration| {
                    iteration.polarization.as_deref()
                }),
            iterator: self.visibility.iteration.is_some(),
        };
        let record = tool_visibility::VisibilityWriteRecord::new(&main.table, rows, items, options)
            .map_err(runtime_error)?;
        let names = record.column_names().collect::<Vec<_>>();
        if names.is_empty() {
            return Ok(false);
        }
        let targets = main.write_targets(record.rows(), &names)?;
        let refresh_cursor = self
            .visibility
            .iteration
            .as_ref()
            .is_some_and(|iteration| iteration.layout.is_some());
        let mut written = BTreeSet::new();
        let mut temporary = BTreeMap::new();
        for target in &targets {
            if refresh_cursor {
                written.insert(target.path.clone());
            }
            if !self.write_locks.contains_key(&target.path) {
                let lock =
                    TableWriteLock::acquire_to_reread(&target.path, 0).map_err(runtime_error)?;
                temporary.insert(target.path.clone(), lock);
            }
        }
        link.verify_directory().map_err(runtime_error)?;
        main.verify_write_layout(&BTreeSet::new())?;
        let prepared = targets
            .into_iter()
            .map(|target| {
                let ms = MeasurementSet::open(&target.path).map_err(runtime_error)?;
                let part = record
                    .prepare_part(&ms, target.rows.into_iter().collect(), target.names)
                    .map_err(runtime_error)?;
                Ok((target.path, ms, part))
            })
            .collect::<PyResult<Vec<_>>>()?;
        self.visibility.main = None;
        for (path, mut ms, part) in prepared {
            let lock = self
                .write_locks
                .get_mut(&path)
                .or_else(|| temporary.get_mut(&path))
                .expect("physical MAIN write lock was acquired");
            record
                .write_part(&mut ms, lock, part)
                .map_err(runtime_error)?;
        }
        if let Some(iteration) = self
            .visibility
            .iteration
            .as_mut()
            .filter(|_| refresh_cursor)
        {
            // Refresh data read bindings and the stamps changed by our own
            // write without accepting a different saved view projection.
            main.verify_write_layout(&written)?;
            let refreshed = VisibilityMain::open(&self.path)?;
            main.verify_write_layout(&written)?;
            iteration.layout = Some(Arc::new(refreshed));
        }
        for (_, lock) in temporary {
            lock.release().map_err(runtime_error)?;
        }
        Ok(true)
    }

    pub(super) fn visibility_range(&mut self, request: &ToolRangeRequest) -> PyResult<CasaValue> {
        let link = self.metadata_link();
        self.visibility
            .range_for_rows(&self.path, &link, self.selected_rows.as_deref(), request)
    }

    pub(super) fn initialize_public_iteration(
        &mut self,
        columns: &[String],
        interval: f64,
        maxrows: i64,
        defaults: bool,
    ) -> PyResult<bool> {
        self.visibility.iteration = None;
        let link = self.metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let main = self.visibility.main(&self.path)?;
        let cursor = ToolIteration::new(
            &main.table,
            main.reference.as_ref(),
            self.selected_rows.as_deref(),
            columns,
            interval,
            maxrows,
            defaults,
        )
        .map_err(runtime_error)?;
        let preserve_layout = !main.plain;
        let preserve_metadata = main.reference.is_some();
        link.verify_directory().map_err(runtime_error)?;
        let Some(cursor) = cursor else {
            return Ok(false);
        };
        let layout = preserve_layout.then(|| {
            self.visibility
                .main
                .as_ref()
                .expect("MAIN was opened")
                .clone()
        });
        let metadata = preserve_metadata
            .then(|| link.snapshot(None).map_err(runtime_error))
            .transpose()?;
        self.visibility.iteration = Some(VisibilityIteration {
            cursor,
            layout,
            metadata,
            channels: self.visibility.channels,
            polarization: self
                .visibility
                .polarization
                .clone()
                .filter(|plan| !plan.is_conversion()),
            conversion: None,
        });
        Ok(true)
    }

    fn getdata_iteration(
        &mut self,
        link: &MetadataLink,
        items: &[String],
        increment: i64,
        options: tool_visibility::VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        let axis_info = items
            .iter()
            .any(|item| item.eq_ignore_ascii_case("axis_info"));
        let snapshot = if !self.visibility.initialized {
            let Some(snapshot) = self.initialize_visibility(link)? else {
                return Ok(CasaValue::empty_record());
            };
            Some(snapshot)
        } else if axis_info
            && self
                .visibility
                .iteration
                .as_ref()
                .is_some_and(|iteration| iteration.metadata.is_none())
        {
            Some(link.snapshot(None).map_err(runtime_error)?)
        } else {
            None
        };
        if self
            .visibility
            .iteration
            .as_ref()
            .is_some_and(|iteration| iteration.layout.is_none())
        {
            self.visibility.main(&self.path)?;
        }
        let iteration = self.visibility.iteration.as_ref().expect("active iterator");
        let main = if let Some(layout) = &iteration.layout {
            layout.verify_iterator_layout()?;
            layout
        } else {
            self.visibility.main.as_ref().expect("MAIN was opened")
        };
        let rows = iteration.cursor.rows().map_err(runtime_error)?;
        let polarization = iteration
            .conversion
            .as_deref()
            .or_else(|| {
                self.visibility
                    .polarization
                    .as_deref()
                    .filter(|plan| plan.is_conversion())
            })
            .or(iteration.polarization.as_deref());
        let baseline_pairs = options
            .baseline_gap
            .is_some()
            .then(|| self.iteration_baselines(main, increment))
            .transpose()?;
        let reply = main.read_rows(
            rows,
            items,
            iteration
                .metadata
                .as_ref()
                .or(snapshot.as_ref())
                .map(|snapshot| &snapshot.ms),
            tool_visibility::VisibilityReadOptions {
                channels: iteration.channels,
                polarization,
                baseline_pairs: baseline_pairs.as_deref(),
                time_measures: axis_info.then_some(&self.visibility.time_measures),
                iterator_read: true,
                ..options
            },
        )?;
        link.verify_directory().map_err(runtime_error)?;
        Ok(reply)
    }

    fn iteration_baselines(
        &self,
        main: &VisibilityMain,
        increment: i64,
    ) -> PyResult<Vec<(i32, i32)>> {
        let selected = self.selected_rows.as_deref();
        let count = selected.map_or_else(|| main.table.row_count(), <[usize]>::len);
        let sampled = if increment > 1 && (increment as u32 as usize) <= count {
            let mut rows = sampled_rows(count, count, increment)?;
            if let Some(selected) = selected {
                for row in &mut rows {
                    *row = selected[*row];
                }
            }
            Some(rows)
        } else {
            None
        };
        tool_visibility::tool_baseline_pairs(
            &main.table,
            main.reference.as_ref(),
            sampled.as_deref().or(selected),
        )
        .map_err(runtime_error)
    }

    fn select_visibility_polarization(&mut self, wanted: &[String]) -> PyResult<bool> {
        let link = self.metadata_link();
        if link.row_count().map_err(runtime_error)? == 0 {
            return Ok(false);
        }
        let Some(snapshot) = self.initialize_visibility(&link)? else {
            return Ok(false);
        };
        if wanted.iter().all(String::is_empty) {
            return Ok(false);
        }
        let request = MsSelection {
            correlation: Some(wanted.join(",")),
            ..MsSelection::default()
        };
        let plan = match self.selection_state.parse(&snapshot.ms, &request) {
            Ok(parsed) => {
                if !self.apply_parsed_selection(&snapshot.ms, &parsed)? {
                    return Ok(false);
                }
                VisibilityPolarization::stored(parsed.into_correlations())
            }
            Err(MsError::NoPolarizationMatch(_)) => {
                let Some(types) = wanted
                    .iter()
                    .map(|name| {
                        StokesType::from_name(name).filter(|kind| *kind != StokesType::Undefined)
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    if self
                        .visibility
                        .polarization
                        .as_deref()
                        .is_some_and(VisibilityPolarization::is_conversion)
                    {
                        self.visibility.set_polarization(None)?;
                    }
                    return Ok(false);
                };
                self.selection_state.clear_polarization_indices();
                VisibilityPolarization::converted(&snapshot.ms, types).map_err(runtime_error)?
            }
            Err(_) => return Ok(false),
        };
        self.visibility.set_polarization(Some(Arc::new(plan)))?;
        Ok(true)
    }

    pub(super) fn select_visibility_description(&mut self, description: i64) -> PyResult<bool> {
        let link = self.metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let main = self.visibility.main(&self.path)?;
        let selected = tool_visibility::select_tool_data_description(
            &main.table,
            &main.members,
            main.reference.as_ref(),
            self.selected_rows.as_deref(),
            description,
        )
        .map_err(runtime_error)?;
        link.verify_directory().map_err(runtime_error)?;
        let nonempty = !selected.is_empty();
        self.set_selected_rows(Some(selected))?;
        self.selection_state.use_implicit_selection();
        self.visibility.set_initialized(nonempty);
        Ok(nonempty)
    }

    pub(super) fn initialize_record_selection(&mut self) -> PyResult<bool> {
        if self.visibility.initialized {
            return Ok(true);
        }
        let link = self.metadata_link();
        Ok(self.initialize_visibility(&link)?.is_some())
    }

    fn initialize_visibility(
        &mut self,
        link: &MetadataLink,
    ) -> PyResult<Option<Arc<MetadataSnapshot>>> {
        let mut snapshot = link.snapshot(None).map_err(runtime_error)?;
        if !self.visibility.initialized
            && let Some(mut selected) =
                tool_visibility::heterogeneous_row_shapes(&snapshot.ms).map_err(runtime_error)?
        {
            if let Some(prior) = &self.selected_rows {
                for row in &mut selected {
                    *row = prior[*row];
                }
            }
            let selected_any = !selected.is_empty();
            self.set_selected_rows(Some(selected))?;
            self.selection_state.use_implicit_selection();
            if !selected_any {
                return Ok(None);
            }
            snapshot = link.snapshot(None).map_err(runtime_error)?;
        }
        self.visibility.initialized = true;
        Ok(Some(snapshot))
    }

    fn getdata_rows(
        &mut self,
        items: &[String],
        increment: i64,
        options: tool_visibility::VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        self.visibility.averaged = options.average;
        let link = self.metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        if self
            .selected_rows
            .as_deref()
            .is_some_and(<[usize]>::is_empty)
        {
            return Ok(CasaValue::empty_record());
        }
        if self.visibility.iteration.is_some() {
            return self.getdata_iteration(&link, items, increment, options);
        }
        let initialized = self.visibility.initialized;
        let polarization = self.visibility.polarization.clone();
        let options = tool_visibility::VisibilityReadOptions {
            channels: self.visibility.channels,
            polarization: polarization.as_deref(),
            ..options
        };
        let main = self.visibility.main(&self.path)?;
        let count = self
            .selected_rows
            .as_deref()
            .map_or_else(|| main.table.row_count(), <[usize]>::len);
        if count == 0 {
            return Ok(CasaValue::empty_record());
        }
        let axis_info = items
            .iter()
            .any(|item| item.eq_ignore_ascii_case("axis_info"));
        if !initialized || axis_info {
            let Some(snapshot) = self.initialize_visibility(&link)? else {
                return Ok(CasaValue::empty_record());
            };
            let rows = sampled_rows(count, snapshot.ms.row_count(), increment)?;
            if !axis_info || polarization.is_some() {
                self.visibility.main(&self.path)?;
                let main = self
                    .visibility
                    .main
                    .as_ref()
                    .expect("MAIN table was opened");
                let reply = main.read_selected(
                    self.selected_rows.as_deref(),
                    rows,
                    items,
                    axis_info.then_some(&snapshot.ms),
                    tool_visibility::VisibilityReadOptions {
                        time_measures: axis_info.then_some(&self.visibility.time_measures),
                        ..options
                    },
                )?;
                link.verify_directory().map_err(runtime_error)?;
                return Ok(reply);
            }
            let record = tool_visibility::read_tool_visibility(
                snapshot.ms.main_table(),
                &[],
                None,
                &rows,
                items,
                Some(&snapshot.ms),
                tool_visibility::VisibilityReadOptions {
                    time_measures: Some(&self.visibility.time_measures),
                    ..options
                },
            )
            .map_err(runtime_error)?;
            return Ok(visibility_record(record));
        }
        if items.is_empty() {
            return Ok(CasaValue::empty_record());
        }
        let rows = sampled_rows(count, count, increment)?;
        let reply =
            main.read_selected(self.selected_rows.as_deref(), rows, items, None, options)?;
        link.verify_directory().map_err(runtime_error)?;
        Ok(reply)
    }
}

fn requested_items(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    requested_strings(value, "items")
}

pub(super) fn requested_strings(
    value: Option<&Bound<'_, PyAny>>,
    argument: &str,
) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(vec![text.to_str()?.to_owned()]);
    }
    let Ok(list) = value.cast::<PyList>() else {
        return Ok(Vec::new());
    };
    list.iter()
        .map(|value| {
            value
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err(format!("list _{argument} must contain strings")))
        })
        .collect()
}

fn increment_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "increment")
}

fn gap_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "ifraxisgap")
}

fn nchan_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "nchan")
}

fn start_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "start")
}

fn width_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "width")
}

fn channel_increment(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "inc")
}

fn channel_argument(value: &Bound<'_, PyAny>, name: &str) -> PyResult<i64> {
    if missing_selection_integer(value)? {
        Err(PySystemError::new_err(
            "<built-in function ms_selectchannel> returned a result with an exception set",
        ))
    } else {
        integer_argument(value, name)
    }
}

pub(super) fn missing_selection_integer(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_none() {
        return Ok(true);
    }
    if value.is_instance_of::<PyInt>() {
        return Ok(false);
    }
    value.is_instance(&value.py().import("numpy")?.getattr("bool_")?)
}

fn integer_argument(value: &Bound<'_, PyAny>, name: &str) -> PyResult<i64> {
    if value.is_none() {
        return Err(PySystemError::new_err(
            "<built-in function ms_getdata> returned a result with an exception set",
        ));
    }
    value
        .extract::<i64>()
        .map_err(|_| PyTypeError::new_err(format!("argument _{name} must be an integer")))
}

fn sampled_rows(initial_count: usize, count: usize, increment: i64) -> PyResult<Vec<usize>> {
    // CASA sizes the stride selection using the count before implicit data
    // description selection, then applies it to the current row view.
    if increment > 1 && (increment as u32 as usize) <= initial_count {
        let Ok(stride) = usize::try_from(increment) else {
            return Ok(Vec::new());
        };
        let samples = initial_count / stride;
        if samples > 0 && (samples - 1) * stride >= count {
            return Err(runtime_error("RefTable Row vector"));
        }
        Ok((0..count).step_by(stride).take(samples).collect())
    } else {
        Ok((0..count).collect())
    }
}
