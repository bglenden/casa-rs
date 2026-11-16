// SPDX-License-Identifier: LGPL-3.0-or-later
//! Root row numbers and `selectrows` views for open tables and saved references (#1028).

use std::path::{Path, PathBuf};

use casa_tables::TableOptions;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::copy::{
    check_target, refuse_nested, replace_with, same_directory, used_by_another_process,
};
use super::create::scratch_name;
use super::registry;
use super::state::{Fail, Open, State, no_table, no_table_specified};
use super::{LOGGER, PyTableTool, integer, row_numbers, text};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

impl State {
    fn selectrows(
        &mut self,
        rownrs: Vec<i64>,
        name: String,
        log: &mut Vec<LogLine>,
    ) -> Result<Open, Fail> {
        if self.open.is_none() {
            no_table(log);
            no_table_specified(log);
            return Err(Fail::Runtime("Unable to create table".to_string()));
        }
        self.synchronize()?;
        let open = self.open.as_ref().expect("checked above");
        let max_rows = open.nrows();
        let mut selected = Vec::with_capacity(rownrs.len());
        for row in rownrs {
            let Ok(index) = usize::try_from(row) else {
                return Err(Fail::Runtime(
                    "rownumbers should be >= 1 and <= nrow".to_string(),
                ));
            };
            if index >= max_rows {
                return Err(Fail::Runtime(
                    "rownumbers should be >= 1 and <= nrow".to_string(),
                ));
            }
            selected.push(index);
        }
        let root_rows = open.translate_rows(&selected);
        if name.is_empty() {
            return Ok(open.view(scratch_name(), root_rows));
        }
        let target = std::path::absolute(Path::new(&name))
            .map_err(|error| Fail::Runtime(error.to_string()))?;
        for protected in [open.name.as_path(), open.root_name.as_path()]
            .into_iter()
            .chain(open.plain_paths.iter().map(PathBuf::as_path))
        {
            if same_directory(&target, protected) {
                return Err(Fail::Runtime(format!(
                    "Table {} already exists",
                    target.display()
                )));
            }
            refuse_nested(&target, protected)?;
        }
        check_target(&target)?;
        self.refuse_open_target(&target)?;
        if used_by_another_process(&target) {
            return Err(Fail::Runtime(format!(
                "Table {} is open in another process and was not replaced",
                target.display()
            )));
        }
        let root = open.root();
        let ref_table = root.table.select_rows(&root_rows)?;
        replace_with(&target, |staging| {
            ref_table.save(TableOptions::new(staging))?;
            Ok(())
        })?;
        drop(root);
        Ok(open.view(target, root_rows))
    }
}

#[pymethods]
impl PyTableTool {
    /// Return the root row numbers of the open table.
    #[pyo3(signature = (tab = None, nbytes = None), text_signature = "($self, tab={}, nbytes=0)")]
    fn rownumbers(
        &self,
        py: Python<'_>,
        tab: Option<&Bound<'_, PyAny>>,
        nbytes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(tab) = tab {
            tab.cast::<PyDict>()?;
        }
        integer(nbytes, "nbytes", 0)?;
        self.read(py, |state, log| {
            let rows = match state.open.as_ref() {
                Some(open) => {
                    let root_rows = open.root().table.root_row_numbers()?;
                    match &open.row_map {
                        Some(map) => map
                            .iter()
                            .map(|&row| {
                                root_rows.get(row).copied().ok_or_else(|| {
                                    Fail::Runtime(format!(
                                        "Table row {row} is out of range ({} rows)",
                                        root_rows.len()
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        None => root_rows,
                    }
                }
                None => {
                    no_table(log);
                    no_table_specified(log);
                    Vec::new()
                }
            };
            let count = rows.len();
            let values = rows
                .into_iter()
                .map(|row| i32::try_from(row).map_or(-1, i64::from))
                .collect();
            Ok(CasaValue::Ints {
                shape: vec![count],
                values,
            })
        })
    }

    /// Return a new table tool referencing the requested rows of the open table.
    #[pyo3(signature = (rownrs, name = None), text_signature = "($self, rownrs, name='')")]
    fn selectrows(
        &self,
        py: Python<'_>,
        rownrs: &Bound<'_, PyAny>,
        name: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let rows = row_numbers(rownrs, "rownrs")?;
        let name = name.map_or(Ok(String::new()), |value| text(value, "name"))?;
        let mut lines = Vec::new();
        let selected = py.detach(|| self.guard().selectrows(rows, name, &mut lines));
        emit(py, LOGGER, &lines)?;
        let tool = Self {
            state: registry::track(State::new(Some(selected?))),
        };
        Ok(Py::new(py, tool)?.into_any())
    }
}
