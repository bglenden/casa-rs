// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row selection for the legacy MSSelector methods.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_core::{RecordField, RecordValue};
use casa_ms::SubtableId;
use casa_ms::selection::taql_selection::{select_legacy_rows, select_projected_scalar_rows};
use casa_ms::subtables::{
    MsDataDescription, MsPolarization, MsSpectralWindow, linked_subtable_row,
};
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::legacy_iteration::LegacyIteration;
use super::legacy_memory::LegacySubtables;
use super::legacy_query_output::{PublishedLegacyQuery, QueryOutputOptions, StagedLegacyQuery};
use super::legacy_query_subtable::QuerySubtable;
use super::legacy_visibility::LegacyAxes;
use super::metadata_link::compose_rows;
use super::selection_indices::{selectinit_id, selection_dictionary, selection_text};
use super::selection_record::{
    RecordSelector, integer_items, integer_items_as, real_items, selection_record,
};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::tools::{LogLine, emit};

#[derive(Default)]
pub(super) struct LegacySelection {
    // Indices are relative to the working MS, which can differ from the
    // currently selected view after an exact-time record selection.
    rows: Option<Arc<[usize]>>,
    initial: Option<InitialSelection>,
    pub(super) output: Option<Box<AttachedMs>>,
    pub(super) inactive_iterator: Option<Box<AttachedMs>>,
    pub(super) axes: LegacyAxes,
    pub(super) baselines: Option<Vec<(i32, i32)>>,
    pub(super) iteration: Option<LegacyIteration>,
    query_parent: LegacyQueryParent,
    positioned: bool,
}

#[derive(Default)]
enum LegacyQueryParent {
    #[default]
    Attachment,
    Named(Arc<Vec<PathBuf>>),
    Transient,
}

enum PreparedLegacyQuery {
    Disk(Box<StagedLegacyQuery>),
    Memory(Box<AttachedMs>),
}

impl LegacySelection {
    pub(super) fn rows(&self) -> Option<&[usize]> {
        self.rows.as_deref()
    }

    pub(super) fn shared_rows(&self) -> Option<Arc<[usize]>> {
        self.rows.clone()
    }

    pub(super) fn positioned_iteration(&self) -> Option<&LegacyIteration> {
        self.iteration.as_ref().filter(|_| self.positioned)
    }

    pub(super) fn set_iterator_rows(&mut self, rows: Option<Arc<[usize]>>, positioned: bool) {
        if self.rows != rows || self.positioned != positioned {
            self.axes.baseline_read = None;
        }
        self.rows = rows;
        self.positioned = positioned;
    }

    pub(super) fn channel_count(&self) -> usize {
        self.initial.as_ref().map_or(0, |initial| initial.channels)
    }

    pub(super) fn descriptions(&self) -> &[i32] {
        self.initial
            .as_ref()
            .map_or(&[], |initial| &initial.descriptions)
    }

    pub(super) fn query_parent<'a>(&'a self, attachment: &'a Path) -> Option<&'a Path> {
        match &self.query_parent {
            LegacyQueryParent::Attachment => Some(attachment),
            LegacyQueryParent::Named(paths) => paths.first().map(PathBuf::as_path),
            LegacyQueryParent::Transient => None,
        }
    }
}

#[derive(Clone)]
struct InitialSelection {
    descriptions: Vec<i32>,
    channels: usize,
    rows: Option<Arc<[usize]>>,
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (selected = false), text_signature = "($self, selected=False)")]
    fn nrowold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = strict_bool)] selected: bool,
    ) -> PyResult<usize> {
        deprecation(py, "nrowold", "nrow")?;
        self.attachment_info(py, |attached| {
            let Some(ms) = attached else { return Ok(0) };
            if selected {
                let view = ms.legacy_view();
                if let Some(rows) = &view.legacy_selection.rows {
                    return Ok(rows.len());
                }
                return view
                    .working_metadata_link()
                    .row_count()
                    .map_err(runtime_error);
            }
            ms.working_metadata_link()
                .row_count()
                .map_err(runtime_error)
        })
    }

    #[pyo3(signature = (datadescid = 0, reset = false), text_signature = "($self, datadescid=0, reset=False)")]
    fn selectinitold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = selectinit_id)] datadescid: i64,
        #[pyo3(from_py_with = strict_bool)] reset: bool,
    ) -> PyResult<bool> {
        deprecation(py, "selectinitold", "selectinit")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            if reset {
                ms.replace_legacy_output(None);
                ms.legacy_selection = LegacySelection {
                    iteration: ms.legacy_selection.iteration.take(),
                    inactive_iterator: ms.legacy_selection.inactive_iterator.take(),
                    ..LegacySelection::default()
                };
                return Ok(true);
            }
            let id = datadescid as i32;
            if id < 0 {
                return Err(runtime_error(
                    "The data description id must be a list of positive integers",
                ));
            }
            ms.replace_legacy_output(None);
            ms.initialize_legacy_selection(Some(id))
        })
    }

    #[pyo3(signature = (msselect = None), text_signature = "($self, msselect='')")]
    fn selecttaqlold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = taql_argument)] msselect: Option<String>,
    ) -> PyResult<bool> {
        deprecation(py, "selecttaqlold", "selecttaql")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.select_legacy_taql(msselect.as_deref().unwrap_or_default()),
                None => Ok(false),
            }
        })
    }

    #[pyo3(signature = (items = None), text_signature = "($self, items={})")]
    fn selectold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = selection_dictionary)] items: Option<Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let record = selection_record(py, items.as_ref())?;
        deprecation(py, "selectold", "select")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.select_legacy_record(record),
                None => Ok(false),
            }
        })
    }
}

impl AttachedMs {
    pub(super) fn legacy_view(&self) -> &Self {
        self.legacy_selection.output.as_deref().unwrap_or(self)
    }

    pub(super) fn legacy_view_mut(&mut self) -> &mut Self {
        if self.legacy_selection.output.is_some() {
            return self.legacy_selection.output.as_deref_mut().unwrap();
        }
        self
    }

    pub(super) fn legacy_view_for_filter(&mut self) -> &mut Self {
        if self
            .legacy_selection
            .output
            .as_ref()
            .is_some_and(|view| view.legacy_selection.initial.is_none())
        {
            self.replace_legacy_output(None);
        }
        self.legacy_view_mut()
    }

    pub(super) fn replace_legacy_output(&mut self, output: Option<Box<Self>>) {
        let previous = std::mem::replace(&mut self.legacy_selection.output, output);
        if self.legacy_selection.output.is_none()
            && let Some(view) = previous.as_ref()
        {
            self.legacy_selection.axes = view.legacy_selection.axes.without_baseline_read();
            self.legacy_selection.baselines = view.legacy_selection.baselines.clone();
        }
        if previous
            .as_ref()
            .is_some_and(|view| view.legacy_selection.iteration.is_some())
        {
            self.legacy_selection.inactive_iterator = previous;
        }
    }

    pub(super) fn restore_legacy_iterator_view(&mut self, mut source: Box<Self>) {
        let current = &self.legacy_view().legacy_selection;
        source.legacy_selection.axes = current.axes.without_baseline_read();
        source.legacy_selection.baselines = current.baselines.clone();
        if self.legacy_selection.initial.is_none() {
            source.legacy_selection.initial = None;
        }
        self.replace_legacy_output(Some(source));
    }

    pub(super) fn initialize_legacy_selection(&mut self, requested: Option<i32>) -> PyResult<bool> {
        if let Some(id) = requested
            && let Some(initial) = &self.legacy_selection.initial
            && initial.descriptions == [id]
        {
            self.legacy_selection.query_parent = if initial.rows.is_some() {
                LegacyQueryParent::Transient
            } else {
                LegacyQueryParent::Attachment
            };
            self.legacy_selection
                .set_iterator_rows(initial.rows.clone(), false);
            return Ok(true);
        }
        let working = self.working_metadata_link();
        let working_rows = working.selected_rows().map_err(runtime_error)?;
        let refreshed = self.refresh_legacy_main()?;
        let table = refreshed.as_ref().unwrap_or_else(|| self.legacy_main());
        let uncached = super::subtable_queries::Subtables::default();
        let subtables = if refreshed.is_some() {
            &uncached
        } else {
            &self.subtables
        };
        let links = [
            SubtableId::DataDescription,
            SubtableId::SpectralWindow,
            SubtableId::Polarization,
        ];
        let subtables = LegacySubtables::read(subtables, self.memory_ms.as_ref(), table, &links)
            .map_err(runtime_error)?;
        let descriptions = subtables
            .tables(SubtableId::DataDescription)
            .map_err(runtime_error)?;
        let description_count = descriptions.iter().map(Table::row_count).sum::<usize>();
        let requested = requested.filter(|_| description_count > 1);
        let columns = match working_rows {
            Some(rows) => table.required_scalar_columns_owned_for_rows(&["DATA_DESC_ID"], &rows),
            None => table.required_scalar_columns_owned(&["DATA_DESC_ID"]),
        }
        .map_err(runtime_error)?;
        let RequiredScalarColumnValues::Int32(description_by_row) = &columns["DATA_DESC_ID"] else {
            return Err(runtime_error("DATA_DESC_ID must be an integer column"));
        };
        let count = description_by_row.len();
        let mut ids = BTreeSet::new();
        let mut rows = Vec::with_capacity(count);
        for (relative, &id) in description_by_row.iter().enumerate() {
            if requested.is_none_or(|requested| requested == id) {
                ids.insert(id);
                rows.push((relative, id));
            }
        }
        let (same_shape, channels) = common_visibility_shape(
            descriptions,
            subtables
                .tables(SubtableId::SpectralWindow)
                .map_err(runtime_error)?,
            subtables
                .tables(SubtableId::Polarization)
                .map_err(runtime_error)?,
            &ids,
        )?;
        if !same_shape && let Some(first) = ids.first().copied() {
            rows.retain(|&(_, id)| id == first);
            ids.retain(|&id| id == first);
        }
        let initialized = !rows.is_empty();
        let rows = (rows.len() != count)
            .then(|| Arc::<[usize]>::from_iter(rows.into_iter().map(|(row, _)| row)));
        self.legacy_selection.initial = initialized.then(|| InitialSelection {
            descriptions: ids.into_iter().collect(),
            channels,
            rows: rows.clone(),
        });
        self.legacy_selection.query_parent = if rows.is_some() {
            LegacyQueryParent::Transient
        } else {
            LegacyQueryParent::Attachment
        };
        self.legacy_selection.set_iterator_rows(rows, false);
        self.legacy_selection.axes = LegacyAxes::default();
        self.legacy_selection.baselines = None;
        Ok(same_shape)
    }

    fn refresh_legacy_main(&self) -> PyResult<Option<Table>> {
        if self.memory_ms.is_some() {
            return Ok(None);
        }
        self.working_metadata_link()
            .verify_directory()
            .map_err(runtime_error)?;
        if self.main_stamp.is_some() && self.main_files.same_descriptor(self.main_stamp) {
            return Ok(None);
        }
        Table::open(TableOptions::new(&self.path))
            .map(Some)
            .map_err(runtime_error)
    }

    pub(super) fn legacy_correlation_layout(&self) -> PyResult<(usize, Vec<i32>)> {
        let refreshed = self.refresh_legacy_main()?;
        let table = refreshed.as_ref().unwrap_or_else(|| self.legacy_main());
        let uncached = super::subtable_queries::Subtables::default();
        let cache = if refreshed.is_some() {
            &uncached
        } else {
            &self.subtables
        };
        let ids = [SubtableId::DataDescription, SubtableId::Polarization];
        let linked = LegacySubtables::read(cache, self.memory_ms.as_ref(), table, &ids)
            .map_err(runtime_error)?;
        let descriptions = linked
            .tables(SubtableId::DataDescription)
            .map_err(runtime_error)?;
        let polarization = linked
            .tables(SubtableId::Polarization)
            .map_err(runtime_error)?;
        let count = descriptions.iter().map(Table::row_count).sum::<usize>();
        let id = self
            .legacy_selection
            .descriptions()
            .first()
            .copied()
            .ok_or_else(|| runtime_error("no initialized legacy data description"))?;
        let (table, row) =
            linked_subtable_row(descriptions, id as usize, SubtableId::DataDescription)
                .map_err(runtime_error)?;
        let pol = MsDataDescription::new(table)
            .polarization_id(row)
            .map_err(runtime_error)?;
        let (table, row) = linked_subtable_row(
            polarization,
            usize::try_from(pol).map_err(runtime_error)?,
            SubtableId::Polarization,
        )
        .map_err(runtime_error)?;
        let codes = MsPolarization::new(table)
            .corr_type(row)
            .map_err(runtime_error)?;
        Ok((count, codes))
    }

    pub(super) fn prepare_legacy_filter(&mut self) -> PyResult<bool> {
        if self.legacy_selection.initial.is_none() {
            self.initialize_legacy_selection(None)?;
        }
        Ok(self.legacy_selection.initial.is_some()
            && self
                .legacy_selection
                .rows
                .as_ref()
                .is_none_or(|rows| !rows.is_empty()))
    }

    fn select_legacy_taql(&mut self, text: &str) -> PyResult<bool> {
        let original_path = self.path.clone();
        let view = self.legacy_view_for_filter();
        if !view.prepare_legacy_filter()? || text.bytes().all(|character| character == b' ') {
            return Ok(false);
        }
        let (selected, staged) = view.prepare_legacy_taql(text, &original_path)?;
        let nonempty = !selected.is_empty();
        let output = match staged {
            Some(PreparedLegacyQuery::Disk(stage)) => Some(stage.publish(&mut self.write_locks)?),
            Some(PreparedLegacyQuery::Memory(view)) => {
                Some(PublishedLegacyQuery::Independent(view))
            }
            None => None,
        };
        let view = self.legacy_view_mut();
        if let Some(PublishedLegacyQuery::Independent(mut output)) = output {
            output.legacy_selection.initial =
                view.legacy_selection.initial.clone().map(|mut initial| {
                    initial.rows = None;
                    initial
                });
            output.legacy_selection.axes = view.legacy_selection.axes.without_baseline_read();
            output.legacy_selection.baselines = view.legacy_selection.baselines.clone();
            if !nonempty {
                output
                    .legacy_selection
                    .set_iterator_rows(Some(Arc::from([])), false);
            }
            self.replace_legacy_output(Some(output));
        } else {
            view.legacy_selection.query_parent = match output {
                Some(PublishedLegacyQuery::Reference(paths)) => LegacyQueryParent::Named(paths),
                _ => LegacyQueryParent::Transient,
            };
            view.legacy_selection
                .set_iterator_rows(Some(selected), false);
        }
        Ok(nonempty)
    }

    fn prepare_legacy_taql(
        &self,
        text: &str,
        original_path: &std::path::Path,
    ) -> PyResult<(Arc<[usize]>, Option<PreparedLegacyQuery>)> {
        let previous = self.legacy_selection.rows();
        let working = self.working_metadata_link();
        let outer = working.selected_rows().map_err(runtime_error)?;
        let physical_rows = compose_rows(outer.as_deref(), previous);
        let refreshed = self.refresh_legacy_main()?;
        let table = refreshed.as_ref().unwrap_or_else(|| self.legacy_main());
        let fast = select_projected_scalar_rows(table, text, physical_rows.as_deref())
            .map_err(runtime_error)?;
        let selection = if let Some(selection) = fast {
            selection
        } else if let Some(memory) = &self.memory_ms {
            if let Some(rows) = previous {
                let view = memory.to_memory_rows(rows).map_err(runtime_error)?;
                select_legacy_rows(&view, text).map_err(runtime_error)?
            } else {
                select_legacy_rows(memory, text).map_err(runtime_error)?
            }
        } else {
            let snapshot = working
                .snapshot_relative_rows(previous)
                .map_err(runtime_error)?;
            select_legacy_rows(&snapshot.ms, text).map_err(runtime_error)?
        };
        let selected = selection
            .rows
            .into_iter()
            .map(|row| previous.map_or(row, |previous| previous[row]))
            .collect::<Arc<[usize]>>();
        let staged = if let Some(giving) = selection.giving {
            let options = QueryOutputOptions::parse(&giving)?;
            let output = compose_rows(outer.as_deref(), Some(&selected)).expect("row projection");
            working.verify_directory().map_err(runtime_error)?;
            if options.memory {
                QuerySubtable::validate_memory_output(
                    &giving.table_name,
                    self.legacy_selection.query_parent(&self.path),
                    &self.path,
                    self.memory_ms.as_ref(),
                )?;
                self.memory_ms
                    .is_none()
                    .then(|| {
                        let mut memory = self.new_legacy_memory(&output, original_path)?;
                        memory.legacy_selection.query_parent = LegacyQueryParent::Transient;
                        Ok::<_, PyErr>(PreparedLegacyQuery::Memory(Box::new(memory)))
                    })
                    .transpose()?
            } else {
                self.stage_legacy_query(table, &output, &giving.table_name, options, original_path)?
                    .map(|stage| PreparedLegacyQuery::Disk(Box::new(stage)))
            }
        } else {
            None
        };
        Ok((selected, staged))
    }

    fn select_legacy_record(&mut self, record: RecordValue) -> PyResult<bool> {
        if !self.legacy_view_for_filter().prepare_legacy_filter()? {
            return Ok(false);
        }
        let mut fields = record.into_fields();
        fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        for field in fields {
            if field.name == "rows" {
                self.select_explicit_legacy_rows(&field)?;
            } else {
                self.legacy_view_mut().apply_legacy_record_field(field)?;
            }
        }
        Ok(self
            .legacy_view()
            .legacy_selection
            .rows
            .as_ref()
            .is_none_or(|rows| !rows.is_empty()))
    }

    fn select_explicit_legacy_rows(&mut self, field: &RecordField) -> PyResult<()> {
        let rows = integer_items_as(field, |value| value)?;
        if rows.is_empty() {
            return Ok(());
        }
        let available = self
            .working_metadata_link()
            .row_count()
            .map_err(runtime_error)?;
        let selected = rows
            .into_iter()
            .map(|row| {
                usize::try_from(row)
                    .ok()
                    .filter(|&row| row < available)
                    .ok_or_else(|| {
                        runtime_error(format!("row {row} is outside the working MeasurementSet"))
                    })
            })
            .collect::<PyResult<Arc<[usize]>>>()?;
        self.replace_legacy_output(None);
        self.legacy_selection.query_parent = LegacyQueryParent::Transient;
        self.legacy_selection
            .set_iterator_rows(Some(selected), false);
        Ok(())
    }

    fn apply_legacy_record_field(&mut self, field: RecordField) -> PyResult<()> {
        // MSSelectionKeywords looks up the legacy spelling before the
        // C++ selector uppercases it to find the physical column.
        if field.name.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Ok(());
        }
        let label = field.name.to_ascii_uppercase();
        let selector = RecordSelector::from_label(&label).expect("converted selection field");
        let predicate = match selector {
            RecordSelector::Rows => return Ok(()),
            RecordSelector::ScalarIds => {
                let ids = integer_items(&field)?;
                (!ids.is_empty()).then(|| format!("{label} IN [{ids}]", ids = int_list(&ids)))
            }
            RecordSelector::IfrNumber => {
                let mut ids = integer_items(&field)?;
                if ids.is_empty() {
                    self.legacy_selection.baselines = None;
                    return Ok(());
                }
                let snapshot;
                let source = if let Some(memory) = &self.memory_ms {
                    memory
                } else {
                    (snapshot, _) = self
                        .working_metadata_link()
                        .source_and_rows()
                        .map_err(runtime_error)?;
                    &snapshot.ms
                };
                let antennas = source.antenna().map_err(runtime_error)?.row_count() as i32;
                ids.retain(|&id| {
                    (0..antennas).contains(&(id / 1000)) && (0..antennas).contains(&(id % 1000))
                });
                self.legacy_selection.baselines = (!ids.is_empty())
                    .then(|| ids.iter().map(|id| (id / 1000, id % 1000)).collect());
                Some(if ids.is_empty() {
                    "False".into()
                } else {
                    format!(
                        "(ANTENNA1 * 1000 + ANTENNA2) IN [{ids}]",
                        ids = int_list(&ids)
                    )
                })
            }
            RecordSelector::Times => {
                let values = real_items(&field)?;
                if values.is_empty() {
                    return Ok(());
                }
                let values = values
                    .into_iter()
                    .filter(|value| !value.is_nan())
                    .map(real_literal)
                    .collect::<Vec<_>>()
                    .join(",");
                Some(if values.is_empty() {
                    "False".into()
                } else {
                    format!("TIME IN [{values}]")
                })
            }
            range => legacy_range_predicate(range, &field)?,
        };
        if let Some(predicate) = predicate {
            let (selected, _) = self.prepare_legacy_taql(&predicate, &self.path)?;
            self.legacy_selection.query_parent = LegacyQueryParent::Transient;
            self.legacy_selection
                .set_iterator_rows(Some(selected), false);
        }
        Ok(())
    }
}

fn int_list(ids: &[i32]) -> String {
    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
}

fn real_literal(value: f64) -> String {
    if value.is_infinite() {
        if value.is_sign_positive() {
            "1e309"
        } else {
            "-1e309"
        }
        .into()
    } else {
        format!("{value:?}")
    }
}

fn legacy_range_predicate(
    selector: RecordSelector,
    field: &RecordField,
) -> PyResult<Option<String>> {
    let values = real_items(field)?;
    let &[lower, upper] = values.as_slice() else {
        return Ok(None);
    };
    if lower.is_nan() || upper.is_nan() {
        return Ok(Some("False".into()));
    }
    let predicate = match selector {
        RecordSelector::Time => format!(
            "TIME >= {} && TIME <= {}",
            real_literal(lower),
            real_literal(upper)
        ),
        RecordSelector::Uvw(axis) => {
            let column = format!("UVW[{axis}]");
            format!(
                "{column} >= {} && {column} <= {}",
                real_literal(lower),
                real_literal(upper)
            )
        }
        RecordSelector::UvDistance => {
            let lo = real_literal(lower * lower);
            let hi = real_literal(upper * upper);
            let distance = "(sqr(UVW[1]) + sqr(UVW[2]))";
            format!(
                "({distance} >= {lo} || near({distance}, {lo})) && ({distance} <= {hi} || near({distance}, {hi}))"
            )
        }
        _ => unreachable!("record range field checked before dispatch"),
    };
    Ok(Some(predicate))
}

fn common_visibility_shape(
    descriptions: &[Table],
    spectral_windows: &[Table],
    polarizations: &[Table],
    ids: &BTreeSet<i32>,
) -> PyResult<(bool, usize)> {
    let mut first: Option<(usize, i32)> = None;
    for &id in ids {
        let (table, row) =
            linked_subtable_row(descriptions, id as usize, SubtableId::DataDescription)
                .map_err(runtime_error)?;
        let description = MsDataDescription::new(table);
        let spw_id = description.spectral_window_id(row).map_err(runtime_error)?;
        let pol_id = description.polarization_id(row).map_err(runtime_error)?;
        let (spw, spw_row) = linked_subtable_row(
            spectral_windows,
            spw_id as usize,
            SubtableId::SpectralWindow,
        )
        .map_err(runtime_error)?;
        let (pol, pol_row) =
            linked_subtable_row(polarizations, pol_id as usize, SubtableId::Polarization)
                .map_err(runtime_error)?;
        let shape = (
            usize::try_from(
                MsSpectralWindow::new(spw)
                    .num_chan(spw_row)
                    .map_err(runtime_error)?,
            )
            .map_err(runtime_error)?,
            MsPolarization::new(pol)
                .num_corr(pol_row)
                .map_err(runtime_error)?,
        );
        if let Some(prior) = first
            && prior != shape
        {
            return Ok((false, prior.0));
        }
        first = Some(shape);
    }
    Ok((true, first.map_or(0, |shape| shape.0)))
}

pub(super) fn deprecation(py: Python<'_>, name: &str, successor: &str) -> PyResult<()> {
    emit(
        py,
        "casars.tools.ms",
        &[LogLine::warning(format!(
            "The use of ms::{name}() is deprecated. Use ms::{successor}()."
        ))],
    )
}

fn taql_argument(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    selection_text(value, "msselect").map(Some)
}
