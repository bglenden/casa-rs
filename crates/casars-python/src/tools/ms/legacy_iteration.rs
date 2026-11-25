// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position the legacy selector on groups without changing the modern view.

use std::sync::Arc;

use casa_ms::tool_iteration::{LegacyIterationOptions, LegacyToolIteration};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;

use super::legacy_selection::deprecation;
use super::visibility::requested_strings;
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

pub(super) struct LegacyIteration {
    cursor: LegacyToolIteration,
    restore_rows: Option<Arc<[usize]>>,
    working_rows: Option<Arc<[usize]>>,
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
        let main = self.visibility.main(&self.path)?;
        let options = LegacyIterationOptions {
            selected: restore_rows.as_deref(),
            working_rows: working_rows.as_deref(),
            columns,
            interval,
            max_rows,
            defaults,
        };
        let Some(cursor) = main.legacy_iteration(options)? else {
            return Ok(false);
        };
        link.verify_directory().map_err(runtime_error)?;
        self.legacy_selection.iteration = Some(LegacyIteration {
            cursor,
            restore_rows,
            working_rows,
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
        let (rows, found) = match movement {
            Move::Origin => (Some(Arc::from(iteration.cursor.rewind())), true),
            Move::Next => match iteration.cursor.advance() {
                Some(rows) => (Some(Arc::from(rows)), true),
                None => (iteration.restore_rows.clone(), false),
            },
            Move::End => (iteration.restore_rows.clone(), true),
        };
        self.legacy_selection.set_iterator_rows(rows);
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
