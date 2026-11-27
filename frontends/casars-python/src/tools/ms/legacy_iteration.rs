// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position the legacy selector on groups without changing the modern view.

use std::collections::BTreeSet;
use std::sync::Arc;

use casa_mstransform::tool_iteration::{LegacyIterationOptions, LegacyToolIteration};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;

use super::legacy_selection::deprecation;
use super::metadata_link::MetadataSnapshot;
use super::visibility::{VisibilityMain, requested_strings};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

mod cache;

pub(super) struct LegacyIteration {
    cursor: LegacyToolIteration,
    restore_rows: Option<Arc<[usize]>>,
    working_rows: Option<Arc<[usize]>>,
    pub(super) main: Arc<VisibilityMain>,
    pub(super) metadata: Option<Arc<MetadataSnapshot>>,
}

#[derive(Clone, Copy)]
enum Move {
    Origin,
    Next,
    End,
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (columns = None, interval = 0.0, maxrows = 0, adddefaultsortcolumns = true), text_signature = "($self, columns=[], interval=0.0, maxrows=0, adddefaultsortcolumns=True)")]
    fn iterinitold(
        &self,
        py: Python<'_>,
        columns: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = legacy_interval)] interval: f64,
        #[pyo3(from_py_with = legacy_maxrows)] maxrows: i64,
        #[pyo3(from_py_with = strict_bool)] adddefaultsortcolumns: bool,
    ) -> PyResult<bool> {
        let columns = requested_strings(columns, "columns")?;
        deprecation(py, "iterinitold", "iterinit")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.initialize_legacy_iteration(
                    &columns,
                    interval,
                    maxrows,
                    adddefaultsortcolumns,
                ),
                None => Ok(false),
            }
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iteroriginold(&self, py: Python<'_>) -> PyResult<bool> {
        self.move_legacy_iteration(py, Move::Origin)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iternextold(&self, py: Python<'_>) -> PyResult<bool> {
        self.move_legacy_iteration(py, Move::Next)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iterendold(&self, py: Python<'_>) -> PyResult<bool> {
        self.move_legacy_iteration(py, Move::End)
    }
}

impl PyMsTool {
    fn move_legacy_iteration(&self, py: Python<'_>, movement: Move) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.position_legacy_iteration(movement),
                None => Ok(false),
            }
        })
    }
}

impl AttachedMs {
    fn initialize_legacy_iteration(
        &mut self,
        columns: &[String],
        interval: f64,
        max_rows: i64,
        defaults: bool,
    ) -> PyResult<bool> {
        if !self.prepare_legacy_filter()? {
            return Ok(false);
        }
        let link = self.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let working_rows = link.selected_rows().map_err(runtime_error)?;
        let restore_rows = self.legacy_selection.shared_rows();
        let main = self.visibility.current_main(&self.path)?;
        let options = LegacyIterationOptions {
            selected: restore_rows.as_deref(),
            working_rows: working_rows.as_deref(),
            columns,
            interval,
            max_rows,
            defaults,
        };
        let source = self.legacy_sort_source(&main)?;
        let Some(mut cursor) = main.legacy_iteration(options)? else {
            return Ok(false);
        };
        link.verify_directory().map_err(runtime_error)?;
        let metadata = (!main.plain)
            .then(|| link.snapshot(None))
            .transpose()
            .map_err(runtime_error)?;
        let modern_current = source.is_some()
            && self
                .visibility
                .iteration
                .as_ref()
                .and_then(|iteration| iteration.layout.as_ref())
                .is_some_and(|layout| layout.verify_write_layout(&BTreeSet::new()).is_ok());
        let vi1_current = source.is_some()
            && self.visibility.vi1.as_ref().is_some_and(|iteration| {
                iteration.main.verify_write_layout(&BTreeSet::new()).is_ok()
            });
        let published = source
            .map(|source| self.cache_legacy_iteration(&link, &main, source, options, &mut cursor))
            .transpose()?
            .flatten();
        let main = if let Some(root) = published {
            let written = BTreeSet::from([root]);
            let refreshed = self.visibility.current_main(&self.path)?;
            main.verify_write_layout(&written)?;
            if modern_current && let Some(iteration) = &mut self.visibility.iteration {
                iteration.layout = Some(Arc::clone(&refreshed));
            }
            if vi1_current && let Some(iteration) = &mut self.visibility.vi1 {
                iteration.main = Arc::clone(&refreshed);
            }
            refreshed
        } else {
            main
        };
        self.legacy_selection.iteration = Some(LegacyIteration {
            cursor,
            restore_rows,
            working_rows,
            main,
            metadata,
        });
        Ok(true)
    }

    fn position_legacy_iteration(&mut self, movement: Move) -> PyResult<bool> {
        let link = self.working_metadata_link();
        let Some(iteration) = self.legacy_selection.iteration.as_mut() else {
            return Ok(false);
        };
        link.verify_directory().map_err(runtime_error)?;
        if link.selected_rows().map_err(runtime_error)? != iteration.working_rows {
            return Err(runtime_error(
                "legacy iterator working selection changed; call iterinitold again",
            ));
        }
        let (rows, positioned, found) = match movement {
            Move::Origin => (Some(Arc::from(iteration.cursor.rewind())), true, true),
            Move::Next => match iteration.cursor.advance() {
                Some(rows) => (Some(Arc::from(rows)), true, true),
                None => (iteration.restore_rows.clone(), false, false),
            },
            Move::End => (iteration.restore_rows.clone(), false, true),
        };
        self.legacy_selection.set_iterator_rows(rows, positioned);
        Ok(found)
    }
}

fn legacy_interval(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    value.extract().map_err(|_| legacy_conversion_error())
}

fn legacy_maxrows(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if value.is_none() {
        return Err(legacy_conversion_error());
    }
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _maxrows must be an integer"))
}

fn legacy_conversion_error() -> PyErr {
    PySystemError::new_err(
        "<built-in function ms_iterinitold> returned a result with an exception set",
    )
}
