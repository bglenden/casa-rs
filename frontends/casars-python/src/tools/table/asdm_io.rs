// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python binding for the legacy ASDM XML table conversion.

use std::path::Path;

use casa_ms::read_asdm_table;
use casa_tables::{DataManagerKind, TableOptions};
use pyo3::prelude::*;

use super::copy::{check_target, replace_with, used_by_another_process};
use super::{Fail, PyTableTool, text};
use crate::convert::value::CasaValue;

#[pymethods]
impl PyTableTool {
    /// Convert an ASDM XML file to a casacore table without changing this tool.
    #[allow(non_snake_case)]
    #[pyo3(signature = (tablename, xmlfile), text_signature = "($self, tablename, xmlfile)")]
    fn fromASDM(
        &self,
        py: Python<'_>,
        tablename: &Bound<'_, PyAny>,
        xmlfile: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let name = text(tablename, "tablename")?;
        let xml = text(xmlfile, "xmlfile")?;
        self.call(py, |state, _| {
            let table = read_asdm_table(Path::new(&xml))
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
            Ok(CasaValue::Bool(true))
        })
    }
}
