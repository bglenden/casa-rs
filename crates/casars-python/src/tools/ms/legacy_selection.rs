// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row selection for the legacy MSSelector methods.

use std::collections::BTreeSet;
use std::sync::Arc;

use casa_ms::SubtableId;
use casa_ms::selection::taql_selection::{select_legacy_rows, select_projected_scalar_rows};
use casa_ms::subtables::{
    MsDataDescription, MsPolarization, MsSpectralWindow, linked_subtable_row,
};
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions};
use casa_types::{RecordField, RecordValue};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

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
    pub(super) axes: LegacyAxes,
    pub(super) baselines: Option<Vec<(i32, i32)>>,
}

impl LegacySelection {
    pub(super) fn rows(&self) -> Option<&[usize]> {
        self.rows.as_deref()
    }

    pub(super) fn channel_count(&self) -> usize {
        self.initial.as_ref().map_or(0, |initial| initial.channels)
    }

    pub(super) fn descriptions(&self) -> &[i32] {
        self.initial
            .as_ref()
            .map_or(&[], |initial| &initial.descriptions)
    }
}

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
            if selected && let Some(rows) = &ms.legacy_selection.rows {
                return Ok(rows.len());
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
                ms.legacy_selection = LegacySelection::default();
                return Ok(true);
            }
            let id = datadescid as i32;
            if id < 0 {
                return Err(runtime_error(
                    "The data description id must be a list of positive integers",
                ));
            }
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
                Some(ms) => ms.select_legacy_taql(msselect.unwrap_or_default()),
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
    pub(super) fn initialize_legacy_selection(&mut self, requested: Option<i32>) -> PyResult<bool> {
        if let Some(id) = requested
            && let Some(initial) = &self.legacy_selection.initial
            && initial.descriptions == [id]
        {
            self.legacy_selection.rows = initial.rows.clone();
            return Ok(true);
        }
        let working = self.working_metadata_link();
        let working_rows = working.selected_rows().map_err(runtime_error)?;
        let refreshed = self.refresh_legacy_main()?;
        let table = refreshed.as_ref().unwrap_or(&self.main);
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
        let subtables = subtables.read_range(table, &links).map_err(runtime_error)?;
        let descriptions = &subtables[&SubtableId::DataDescription];
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
            &subtables[&SubtableId::SpectralWindow],
            &subtables[&SubtableId::Polarization],
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
        self.legacy_selection.rows = rows;
        self.legacy_selection.axes = LegacyAxes::default();
        self.legacy_selection.baselines = None;
        Ok(same_shape)
    }

    fn refresh_legacy_main(&self) -> PyResult<Option<Table>> {
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
        let table = refreshed.as_ref().unwrap_or(&self.main);
        let uncached = super::subtable_queries::Subtables::default();
        let cache = if refreshed.is_some() {
            &uncached
        } else {
            &self.subtables
        };
        let ids = [SubtableId::DataDescription, SubtableId::Polarization];
        let linked = cache.read_range(table, &ids).map_err(runtime_error)?;
        let descriptions = &linked[&SubtableId::DataDescription];
        let polarization = &linked[&SubtableId::Polarization];
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

    fn select_legacy_taql(&mut self, text: String) -> PyResult<bool> {
        if !self.prepare_legacy_filter()? || text.bytes().all(|character| character == b' ') {
            return Ok(false);
        }
        self.apply_legacy_taql(text)
    }

    fn apply_legacy_taql(&mut self, text: String) -> PyResult<bool> {
        let previous = self.legacy_selection.rows();
        let working = self.working_metadata_link();
        let outer = working.selected_rows().map_err(runtime_error)?;
        let physical_rows = compose_rows(outer.as_deref(), previous);
        let refreshed = self.refresh_legacy_main()?;
        let table = refreshed.as_ref().unwrap_or(&self.main);
        let fast = select_projected_scalar_rows(table, &text, physical_rows.as_deref())
            .map_err(runtime_error)?;
        let selection = if let Some(selection) = fast {
            selection
        } else {
            let snapshot = working
                .snapshot_relative_rows(previous)
                .map_err(runtime_error)?;
            select_legacy_rows(&snapshot.ms, &text).map_err(runtime_error)?
        };
        let selected = selection
            .rows
            .into_iter()
            .map(|row| previous.map_or(row, |previous| previous[row]))
            .collect::<Arc<[usize]>>();
        if let Some(giving) = selection.giving {
            if giving.output_type.is_some() {
                return Err(PyNotImplementedError::new_err(
                    "ms.selecttaqlold GIVING with an explicit output type is not implemented in casa-rs (#1030)",
                ));
            }
            let target = std::path::absolute(giving.table_name).map_err(runtime_error)?;
            let output = compose_rows(outer.as_deref(), Some(&selected)).expect("row projection");
            working.verify_directory().map_err(runtime_error)?;
            super::super::table::save_ms_query_selection(table, &self.sources, &output, &target)?;
        }
        let nonempty = !selected.is_empty();
        self.legacy_selection.rows = Some(selected);
        Ok(nonempty)
    }

    fn select_legacy_record(&mut self, record: RecordValue) -> PyResult<bool> {
        if !self.prepare_legacy_filter()? {
            return Ok(false);
        }
        let mut fields = record.into_fields();
        fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        for field in fields {
            self.apply_legacy_record_field(field)?;
        }
        Ok(self
            .legacy_selection
            .rows
            .as_ref()
            .is_none_or(|rows| !rows.is_empty()))
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
            RecordSelector::Rows => {
                let rows = integer_items_as(&field, |value| value)?;
                if !rows.is_empty() {
                    let available = self
                        .working_metadata_link()
                        .row_count()
                        .map_err(runtime_error)?;
                    let mut selected = Vec::with_capacity(rows.len());
                    for row in rows {
                        let index = usize::try_from(row)
                            .ok()
                            .filter(|&row| row < available)
                            .ok_or_else(|| {
                                runtime_error(format!(
                                    "row {row} is outside the working MeasurementSet"
                                ))
                            })?;
                        selected.push(index);
                    }
                    // The legacy selector interprets explicit rows relative to
                    // its base table and preserves user order and duplicates.
                    self.legacy_selection.rows = Some(selected.into());
                }
                return Ok(());
            }
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
                let (snapshot, _) = self
                    .working_metadata_link()
                    .source_and_rows()
                    .map_err(runtime_error)?;
                let antennas = snapshot.ms.antenna().map_err(runtime_error)?.row_count() as i32;
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
            self.apply_legacy_taql(predicate)?;
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
