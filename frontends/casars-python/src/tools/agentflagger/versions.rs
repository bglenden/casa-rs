// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct flag-version methods and the tool's raw naming rules.

use casa_ms::MeasurementSet;
use casa_ms::flagging::{
    FlagMerge, delete_flag_version, flag_version_ms_path, initialize_flag_versions,
    list_flag_versions, restore_flag_version, save_agent_disk_flag_version,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::StringList;
use crate::tools::{LogLine, emit};

use super::{LOGGER, PyAgentFlagger, Session, open_table, runtime_error};

#[pymethods]
impl PyAgentFlagger {
    #[pyo3(signature = (printflags = true), text_signature = "($self, printflags=True)")]
    fn getflagversionlist(&self, py: Python<'_>, printflags: bool) -> PyResult<Vec<String>> {
        let lines = self.with_session(py, |session| {
            let Some(table) = version_table(session)? else {
                return Ok(Vec::new());
            };
            let target = session
                .attached
                .as_ref()
                .expect("an attached version table");
            let mut lines = vec![format!("\nMS : {}\n", target.path.display())];
            lines.extend(
                list_flag_versions(&table)
                    .map_err(runtime_error)?
                    .into_iter()
                    .map(|entry| format!("{} : {}", entry.name, entry.comment)),
            );
            Ok(lines)
        })?;
        if printflags {
            emit(
                py,
                LOGGER,
                &lines.iter().map(LogLine::info).collect::<Vec<_>>(),
            )?;
        }
        Ok(lines)
    }

    #[pyo3(signature = (versionname = "", comment = "", merge = ""), text_signature = "($self, versionname='', comment='', merge='')")]
    fn saveflagversion(
        &self,
        py: Python<'_>,
        versionname: &str,
        comment: &str,
        merge: &str,
    ) -> PyResult<bool> {
        self.with_session(py, |session| {
            let Some(table) = version_table(session)? else {
                return Ok(false);
            };
            if versionname != "main" {
                save_agent_disk_flag_version(&table, versionname, comment, merge_mode(merge))
                    .map_err(runtime_error)?;
            }
            Ok(true)
        })
    }

    #[pyo3(signature = (versionname = StringList::of(&[]), merge = ""), text_signature = "($self, versionname=[], merge='')")]
    fn restoreflagversion(
        &self,
        py: Python<'_>,
        versionname: StringList,
        merge: &str,
    ) -> PyResult<bool> {
        self.with_session(py, |session| {
            let Some(mut table) = version_table(session)? else {
                return Ok(false);
            };
            for name in versionname.0 {
                if name.is_empty() {
                    return Err(PyRuntimeError::new_err("Flag version  does not exist"));
                }
                if name != "main" {
                    restore_flag_version(&mut table, &name, merge_mode(merge))
                        .map_err(runtime_error)?;
                }
            }
            Ok(true)
        })
    }

    #[pyo3(signature = (versionname = StringList::of(&[])), text_signature = "($self, versionname=[])")]
    fn deleteflagversion(&self, py: Python<'_>, versionname: StringList) -> PyResult<bool> {
        self.with_session(py, |session| {
            let Some(table) = version_table(session)? else {
                return Ok(false);
            };
            for name in versionname.0 {
                if !matches!(name.as_str(), "" | "main") {
                    delete_flag_version(&table, &name).map_err(runtime_error)?;
                }
            }
            Ok(true)
        })
    }
}

fn version_table(session: &Session) -> PyResult<Option<MeasurementSet>> {
    let Some(target) = &session.attached else {
        return Ok(None);
    };
    let table = open_table(&flag_version_ms_path(&target.path))?;
    initialize_flag_versions(&table).map_err(runtime_error)?;
    Ok(Some(table))
}

fn merge_mode(value: &str) -> FlagMerge {
    match value {
        "or" => FlagMerge::Or,
        "and" => FlagMerge::And,
        _ => FlagMerge::Replace,
    }
}
