// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python arguments and handle state for FITS table import.

use std::path::Path;

use casa_images::fits_table::read_fits_table;
use casa_tables::{DataManagerKind, LockMode, LockOptions, LockType, TableOptions};
use pyo3::prelude::*;

use super::state::{LockSettings, Open};
use super::{Fail, LockRequest, PyTableTool, State, flag, integer, registry, text};

#[pymethods]
impl PyTableTool {
    /// Convert a FITS table extension and return a new writable table tool.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (tablename, fitsfile, whichhdu = None, storage = None, convention = None, nomodify = None, ack = None), text_signature = "($self, tablename, fitsfile, whichhdu=1, storage='standard', convention='none', nomodify=True, ack=True)")]
    fn fromfits(
        &self,
        py: Python<'_>,
        tablename: &Bound<'_, PyAny>,
        fitsfile: &Bound<'_, PyAny>,
        whichhdu: Option<&Bound<'_, PyAny>>,
        storage: Option<&Bound<'_, PyAny>>,
        convention: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        ack: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let target = text(tablename, "tablename")?;
        let input = text(fitsfile, "fitsfile")?;
        let hdu = integer(whichhdu, "whichhdu", 1)?;
        let storage = storage.map_or_else(|| Ok("standard".into()), |v| text(v, "storage"))?;
        let convention = convention.map_or_else(|| Ok("none".into()), |v| text(v, "convention"))?;
        flag(nomodify, true)?;
        flag(ack, true)?;
        let opened = py.detach(|| import_fits(&target, &input, hdu, &storage, &convention))?;
        let tool = Self {
            state: registry::track(State::new(Some(opened))),
        };
        Ok(Py::new(py, tool)?.into_any())
    }
}

fn import_fits(
    target: &str,
    input: &str,
    hdu: i64,
    storage: &str,
    convention: &str,
) -> Result<Open, Fail> {
    let (kind, group) = match storage.to_ascii_lowercase().as_str() {
        "standard" => (Some(DataManagerKind::StandardStMan), "StandardStMan"),
        "incremental" => (Some(DataManagerKind::IncrementalStMan), "ISM"),
        "memory" => (None, "MemSt"),
        invalid => {
            return Err(Fail::Runtime(format!(
                "{invalid}  is not a valid storage manager"
            )));
        }
    };
    let hdu = usize::try_from(hdu)
        .map_err(|_| Fail::Runtime("whichHDU is not valid, must be >= 1".into()))?;
    let table = read_fits_table(
        Path::new(input),
        hdu,
        convention.to_ascii_lowercase().contains("sdfi"),
    )
    .map_err(|error| Fail::Runtime(error.to_string()))?;
    let path =
        std::path::absolute(Path::new(target)).map_err(|error| Fail::Runtime(error.to_string()))?;
    let request = LockRequest {
        option: Some("autonoread".into()),
        ..LockRequest::default()
    };
    let Some(kind) = kind else {
        let mut table = table.into_named_memory_manager(
            &path,
            group,
            LockOptions::new(LockMode::AutoNoReadLocking),
        )?;
        if !table.lock(LockType::Write, 1)? {
            return Err(Fail::Runtime(format!(
                "Table {} could not be write locked",
                path.display()
            )));
        }
        return Ok(Open::from_detached_table(
            table,
            path,
            true,
            LockSettings::from_request(&request)?,
        ));
    };
    std::fs::create_dir(&path)
        .map_err(|error| Fail::Runtime(format!("Table {}: {error}", path.display())))?;
    let options = TableOptions::new(&path)
        .with_data_manager(kind)
        .with_data_manager_group(group)
        .with_casacore_default_ssm(kind == DataManagerKind::StandardStMan);
    if let Err(error) = table.save(options) {
        let _ = std::fs::remove_dir_all(&path);
        return Err(error.into());
    }
    Open::attach(target, &request, true)
}
