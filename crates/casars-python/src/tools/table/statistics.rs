// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA table-column statistics (#1028).

use casa_tables::statistics::{ColumnStatisticsError, column_statistics};
use pyo3::prelude::*;

use super::state::{Fail, State, no_table_specified};
use super::{PyTableTool, text};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

impl State {
    fn statistics(
        &self,
        column: &str,
        mode: &str,
        log: &mut Vec<LogLine>,
    ) -> Result<CasaValue, Fail> {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::empty_record());
        };
        if open.row_map.as_ref().is_some_and(Vec::is_empty) {
            return Err(Fail::Runtime("Table has zero rows, cannot continue".into()));
        }
        if !open.column_allowed(column) {
            return Err(Fail::Runtime(format!("Table column {column} is unknown")));
        }
        let root = open.root();
        let result =
            column_statistics(&root.table, open.row_map.as_deref(), column, mode, |line| {
                log.push(LogLine::info(line));
            })
            .map_err(|error| match error {
                ColumnStatisticsError::Unsupported(message) => Fail::Unsupported(message),
                error => Fail::Runtime(error.to_string()),
            })?;
        Ok(CasaValue::record(result.into_iter().map(
            |(column, stats)| {
                (
                    column,
                    CasaValue::record(
                        stats
                            .into_iter()
                            .map(|(key, value)| (key, CasaValue::Float(value))),
                    ),
                )
            },
        )))
    }
}

#[pymethods]
impl PyTableTool {
    /// Compute CASA statistics for a table column.
    #[pyo3(signature = (column, complex_value = None), text_signature = "($self, column, complex_value='')")]
    fn statistics(
        &self,
        py: Python<'_>,
        column: &Bound<'_, PyAny>,
        complex_value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(column, "column")?;
        let mode = complex_value.map_or(Ok(String::new()), |value| text(value, "complex_value"))?;
        self.read(py, move |state, log| state.statistics(&column, &mode, log))
    }
}
