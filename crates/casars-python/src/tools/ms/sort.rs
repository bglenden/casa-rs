// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_ms::tool_sort::write_sorted_measurement_set;
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement,
    rename_without_replacing,
};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods};

use super::metadata_link::same_directory;
use super::selection_indices::selection_text;
use super::{AttachedMs, PyMsTool, runtime_error};

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (newmsname = String::new(), columns = None), text_signature = "($self, newmsname='', columns=[])")]
    fn sort(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = sort_name)] newmsname: String,
        columns: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let columns = columns
            .and_then(|value| value.cast::<PyList>().ok())
            .map(|names| names.iter().map(|name| name.extract::<String>()).collect())
            .transpose()?
            .unwrap_or_default();
        self.sort_measurement_set(py, newmsname, Some(columns))
    }

    #[pyo3(signature = (newmsname = String::new()), text_signature = "($self, newmsname='')")]
    fn timesort(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = sort_name)] newmsname: String,
    ) -> PyResult<bool> {
        self.sort_measurement_set(py, newmsname, None)
    }
}

impl PyMsTool {
    fn sort_measurement_set(
        &self,
        py: Python<'_>,
        name: String,
        columns: Option<Vec<String>>,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_ref() else {
                return Ok(false);
            };
            let in_place = name.is_empty();
            if in_place && ms.selected_rows.is_some() {
                return Err(runtime_error(
                    "an in-place sort would discard unselected rows; supply an output path",
                ));
            }
            let output = if in_place {
                ms.path.clone()
            } else {
                sorted_output_path(Path::new(&name), &ms.path)?
            };
            let original = sorted_output_directory(&output)?;
            let _locks = if original.is_some() && output.join("table.dat").is_file() {
                check_write_access(&output).map_err(runtime_error)?;
                lock_table_tree_for_replacement(&output).map_err(runtime_error)?
            } else {
                Vec::new()
            };
            let link = ms.working_metadata_link();
            let source = link.snapshot(None).map_err(runtime_error)?;
            let workspace = tempfile::tempdir_in(parent_directory(&output)).map_err(runtime_error)?;
            let staged = workspace.path().join("sorted.ms");
            write_sorted_measurement_set(
                &source.ms,
                workspace.path(),
                &staged,
                columns.as_deref(),
                in_place,
                &ms.path,
            )
            .map_err(runtime_error)?;
            link.verify_directory().map_err(runtime_error)?;
            let Some(original) = original else {
                rename_without_replacing(&staged, &output).map_err(runtime_error)?;
                return Ok(true);
            };
            let current = fs::symlink_metadata(&output).map_err(runtime_error)?;
            if !same_directory(&original, &current) {
                return Err(runtime_error("sorted output directory changed before publication"));
            }
            exchange_directories(&staged, &output).map_err(runtime_error)?;
            if in_place {
                match AttachedMs::open(output.clone(), true, false, false) {
                    Ok(mut replacement) => {
                        replacement.name = ms.name.clone();
                        *attached = Some(replacement);
                    }
                    Err(error) => {
                        if let Err(rollback) = exchange_directories(&staged, &output) {
                            let backup = workspace.keep();
                            return Err(runtime_error(format!(
                                "could not reopen sorted MS: {error}; rollback failed: {rollback}; original at {}",
                                backup.join("sorted.ms").display()
                            )));
                        }
                        return Err(error);
                    }
                }
            }
            Ok(true)
        })
    }
}

fn sort_name(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "newmsname")
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sorted_output_path(path: &Path, source: &Path) -> PyResult<PathBuf> {
    let parent = parent_directory(path)
        .canonicalize()
        .map_err(runtime_error)?;
    let name = path
        .file_name()
        .ok_or_else(|| runtime_error("sorted output must name a new MeasurementSet"))?;
    let path = parent.join(name);
    if path == source || source.starts_with(&path) {
        return Err(runtime_error(
            "cannot overwrite the attached MeasurementSet with a named copy",
        ));
    }
    Ok(path)
}

fn sorted_output_directory(path: &Path) -> PyResult<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            if path.join("table.dat").is_file()
                || fs::read_dir(path).map_err(runtime_error)?.next().is_none()
            {
                Ok(Some(metadata))
            } else {
                Err(runtime_error(
                    "sorted output already exists and is not a table",
                ))
            }
        }
        Ok(_) => Err(runtime_error(
            "sorted output already exists and is not a table",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(runtime_error(error)),
    }
}
