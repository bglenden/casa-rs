// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python arguments and handle state for the native ASCII table codec.

use std::path::Path;

use casa_tables::{
    DataManagerKind, TableOptions,
    ascii::{AsciiInput, read_ascii, write_ascii},
};
use pyo3::prelude::*;

use super::copy::{check_target, replace_with, used_by_another_process};
use super::{Fail, LockRequest, PyTableTool, column_names, flag, integer, row_numbers, text};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

fn optional_text(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<String> {
    value.map_or_else(|| Ok(String::new()), |value| text(value, name))
}

fn selected_names(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<Vec<String>> {
    let names = value.map_or_else(|| Ok(Vec::new()), |value| column_names(value, name))?;
    Ok(if names.first().is_some_and(String::is_empty) {
        Vec::new()
    } else {
        names
    })
}

#[pymethods]
impl PyTableTool {
    /// Create a casacore table from ASCII data and attach it to this tool.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (tablename, asciifile, headerfile = None, autoheader = None, autoshape = None, sep = None, commentmarker = None, firstline = None, lastline = None, nomodify = None, columnnames = None, datatypes = None), text_signature = "($self, tablename, asciifile, headerfile='', autoheader=False, autoshape=[-1], sep='', commentmarker='', firstline=0, lastline=-1, nomodify=True, columnnames=[], datatypes=[])")]
    fn fromascii(
        &self,
        py: Python<'_>,
        tablename: &Bound<'_, PyAny>,
        asciifile: &Bound<'_, PyAny>,
        headerfile: Option<&Bound<'_, PyAny>>,
        autoheader: Option<&Bound<'_, PyAny>>,
        autoshape: Option<&Bound<'_, PyAny>>,
        sep: Option<&Bound<'_, PyAny>>,
        commentmarker: Option<&Bound<'_, PyAny>>,
        firstline: Option<&Bound<'_, PyAny>>,
        lastline: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        columnnames: Option<&Bound<'_, PyAny>>,
        datatypes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let name = text(tablename, "tablename")?;
        let data = text(asciifile, "asciifile")?;
        let header = optional_text(headerfile, "headerfile")?;
        let separator = optional_text(sep, "sep")?;
        let comment = optional_text(commentmarker, "commentmarker")?;
        let names = selected_names(columnnames, "columnnames")?;
        let types = selected_names(datatypes, "datatypes")?;
        // CASA validates these arguments at the binding and ignores them.
        if let Some(shape) = autoshape {
            row_numbers(shape, "autoshape")?;
        }
        flag(nomodify, true)?;
        let input = AsciiInput {
            header: (!header.is_empty()).then(|| Path::new(&header)),
            auto_header: flag(autoheader, false)?,
            separator: &separator,
            comment_marker: &comment,
            first_line: integer(firstline, "firstline", 0)?,
            last_line: integer(lastline, "lastline", -1)?,
            column_names: &names,
            data_types: &types,
        };
        self.call(py, |state, _| {
            state.close();
            let table = read_ascii(Path::new(&data), &input)
                .map_err(|error| Fail::Runtime(error.to_string()))?;
            let target = std::path::absolute(Path::new(&name))
                .map_err(|error| Fail::Runtime(error.to_string()))?;
            check_target(&target)?;
            state.refuse_open_target(&target)?;
            if used_by_another_process(&target) {
                return Err(Fail::Runtime(format!(
                    "Table {} is open in another process and was not replaced",
                    target.display()
                )));
            }
            replace_with(&target, |stage| {
                table.save(
                    TableOptions::new(stage)
                        .with_data_manager(DataManagerKind::StandardStMan)
                        .with_data_manager_group("StandardStMan")
                        .with_casacore_default_ssm(true),
                )?;
                Ok(())
            })?;
            state.open(&name, &LockRequest::default(), false)?;
            Ok(CasaValue::Bool(true))
        })
    }

    /// Write the visible columns and rows of this table as CASA ASCII text.
    #[pyo3(signature = (asciifile, headerfile = None, columns = None, sep = None), text_signature = "($self, asciifile, headerfile='', columns=[], sep='')")]
    fn toasciifmt(
        &self,
        py: Python<'_>,
        asciifile: &Bound<'_, PyAny>,
        headerfile: Option<&Bound<'_, PyAny>>,
        columns: Option<&Bound<'_, PyAny>>,
        sep: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let data = text(asciifile, "asciifile")?;
        let header = optional_text(headerfile, "headerfile")?;
        let names = selected_names(columns, "columns")?;
        let separator = optional_text(sep, "sep")?;
        self.read(py, |state, log| {
            let Some(open) = state.open.as_ref() else {
                log.push(LogLine::warning(
                    "toasciifmt: No table specified, please open first",
                ));
                return Ok(CasaValue::Bool(false));
            };
            if data.is_empty() {
                log.push(LogLine::warning("toasciifmt: No output file specified"));
                return Ok(CasaValue::Bool(false));
            }
            let nrows = open.nrows();
            let root = open.root();
            let mut visible = open.visible_columns(&root.table);
            if !names.is_empty() {
                visible = names
                    .iter()
                    .map(|name| {
                        visible
                            .iter()
                            .copied()
                            .find(|col| col.name() == name)
                            .ok_or_else(|| Fail::Runtime(format!("Table column {name} is unknown")))
                    })
                    .collect::<Result<_, _>>()?;
            }
            write_ascii(
                &root.table,
                (0..nrows).map(|row| open.translate_row(row)),
                &visible,
                Path::new(&data),
                (!header.is_empty()).then(|| Path::new(&header)),
                &separator,
            )
            .map_err(|error| Fail::Runtime(error.to_string()))?;
            Ok(CasaValue::Bool(true))
        })
    }
}
