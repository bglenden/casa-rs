// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's IncrementalStMan corruption diagnostic (#1028).

use casa_tables::IncrementalLayoutCheck;
use pyo3::prelude::*;

use super::state::{Fail, no_table_specified};
use super::{PyTableTool, text};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

#[pymethods]
impl PyTableTool {
    /// Check the IncrementalStMan bound to `column` for non-ascending row IDs.
    #[pyo3(signature = (column), text_signature = "($self, column)")]
    fn testincrstman(&self, py: Python<'_>, column: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let column = text(column, "column")?;
        self.call(py, move |state, log| {
            if state.open.is_none() {
                no_table_specified(log);
                return Ok(CasaValue::Bool(false));
            }
            let result = state.synchronize().and_then(|()| {
                Ok(state.open.as_ref().unwrap().table.check_incremental_layout(&column)?)
            });
            let valid = match result {
                Ok(IncrementalLayoutCheck::NotIncremental) => {
                    log.push(LogLine::warning(format!("Data manager type of column {column} is not Incremental Store Manager")));
                    if column == "FLAG_ROW" {
                        log.push(LogLine::info("Starting at 4.2.2 the default store manager for FLAG_ROW is Standard Store Manager \nTherefore a MS imported with CASA version >= 4.2.2 is not exposed to the Incremental Store Manager problem"));
                    }
                    true
                }
                Ok(IncrementalLayoutCheck::Valid) => {
                    log.push(LogLine::info(format!("Incremental Store Manager corruption not detected in column {column}")));
                    true
                }
                Ok(IncrementalLayoutCheck::Corrupt(c)) => {
                    log.push(LogLine::error(format!("Incremental Store Manager corruption (not ascending rowId) detected at bucket number {} (startRow={}, nRows={}, offendingBucketNr={}, cursor={}) column={} index={} rowId={} preRowId={}", c.bucket_number, c.bucket_start_row, c.bucket_rows, c.bucket_number, c.cursor, c.column, c.index, c.row, c.previous_row)));
                    false
                }
                Err(Fail::Runtime(message) | Fail::Type(message) | Fail::Unsupported(message)) => {
                    log.push(LogLine::error(message));
                    false
                }
            };
            Ok(CasaValue::Bool(valid))
        })
    }
}
