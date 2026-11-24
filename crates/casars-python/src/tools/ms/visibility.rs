// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read visibility records from the current MS attachment.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use casa_ms::tool_visibility;
use casa_tables::{PlainReferenceRead, Table, TableOptions, TablePartNames};
use casa_types::measures::MeasuresProvider;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods, PyString};

use super::attachment_info::{TableFiles, TableStamp};
use super::owned_record::visibility_record;
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;

#[derive(Default)]
pub(super) struct VisibilityState {
    initialized: bool,
    main: Option<VisibilityMain>,
    time_measures: OnceLock<Arc<dyn MeasuresProvider>>,
}

struct VisibilityMain {
    table: Table,
    members: Vec<Table>,
    reference: Option<PlainReferenceRead>,
    publication: Vec<(TableFiles, Option<TableStamp>)>,
}

impl VisibilityState {
    pub(super) fn reset_selection(&mut self) {
        self.initialized = false;
    }

    fn main(&mut self, path: &Path) -> PyResult<&VisibilityMain> {
        let current = self.main.as_ref().is_some_and(|main| {
            main.publication
                .iter()
                .all(|(files, stamp)| stamp.is_some() && files.stamp() == *stamp)
        });
        if !current {
            self.main = Some(VisibilityMain::open(path)?);
        }
        Ok(self.main.as_ref().expect("MAIN table was opened"))
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
            publication.push((files, stamp));
        })
        .map_err(runtime_error)?;
        let (members, reference) = match Table::immediate_part_names(path, path)
            .map_err(runtime_error)?
        {
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
        })
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
        options: tool_visibility::VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        if let Some(selected) = selected {
            for row in &mut rows {
                *row = selected[*row];
            }
        }
        let record = tool_visibility::read_tool_visibility(
            &self.table,
            &self.members,
            self.reference.as_ref(),
            &rows,
            items,
            None,
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
            time_measures: None,
        };
        let reply = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(CasaValue::empty_record());
            };
            ms.getdata_rows(&items, increment, options)
        })?;
        reply.into_python(py)
    }
}

impl AttachedMs {
    fn getdata_rows(
        &mut self,
        items: &[String],
        increment: i64,
        options: tool_visibility::VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        let link = self.metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        if self
            .selected_rows
            .as_deref()
            .is_some_and(<[usize]>::is_empty)
        {
            return Ok(CasaValue::empty_record());
        }
        let initialized = self.visibility.initialized;
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
            let mut snapshot = link.snapshot(None).map_err(runtime_error)?;
            if !initialized
                && let Some(mut selected) = tool_visibility::heterogeneous_row_shapes(&snapshot.ms)
                    .map_err(runtime_error)?
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
                    return Ok(CasaValue::empty_record());
                }
                snapshot = link.snapshot(None).map_err(runtime_error)?;
            }
            self.visibility.initialized = true;
            let rows = sampled_rows(count, snapshot.ms.row_count(), increment)?;
            if !axis_info {
                let main = self.visibility.main(&self.path)?;
                let reply =
                    main.read_selected(self.selected_rows.as_deref(), rows, items, options)?;
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
        let reply = main.read_selected(self.selected_rows.as_deref(), rows, items, options)?;
        link.verify_directory().map_err(runtime_error)?;
        Ok(reply)
    }
}

fn requested_items(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
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
                .map_err(|_| PyTypeError::new_err("list _items must contain strings"))
        })
        .collect()
}

fn increment_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "increment")
}

fn gap_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "ifraxisgap")
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
