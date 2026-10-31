// SPDX-License-Identifier: LGPL-3.0-or-later
//! Root row numbers for tables and saved references (#1028).

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::state::{no_table, no_table_specified};
use super::{PyTableTool, integer};
use crate::convert::value::CasaValue;

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
                Some(open) => open.table.root_row_numbers()?,
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
}
