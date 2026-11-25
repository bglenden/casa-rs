// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_ms::tool_sort::{check_named_sort_output, write_sorted_measurement_set};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement_with_held,
    rename_without_replacing,
};
use casa_tables::{Table, TableOptions};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods, PyString};

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
        let columns = match columns {
            Some(value) if value.is_instance_of::<PyString>() => vec![value.extract::<String>()?],
            Some(value) => value
                .cast::<PyList>()
                .ok()
                .map(|names| names.iter().map(|name| name.extract::<String>()).collect())
                .transpose()?
                .unwrap_or_default(),
            None => Vec::new(),
        };
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
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            let in_place = name.is_empty();
            if in_place && ms.selected_rows.is_some() {
                return Err(runtime_error(
                    "an in-place sort would discard unselected rows; supply an output path",
                ));
            }
            let read_locks = if in_place {
                // Replacing a read-locked input also reopens it for update.
                // Release our read guards before trying the exclusive lock.
                std::mem::take(&mut ms.read_locks).into_keys().collect()
            } else {
                BTreeSet::new()
            };
            match sort_attached(ms, &name, columns.as_deref()) {
                Ok(Some(replacement)) => {
                    *attached = Some(replacement);
                    Ok(true)
                }
                Ok(None) => Ok(true),
                Err(error) => {
                    ms.retain_tables(read_locks)?;
                    Err(error)
                }
            }
        })
    }
}

fn sort_attached(
    ms: &AttachedMs,
    name: &str,
    columns: Option<&[String]>,
) -> PyResult<Option<AttachedMs>> {
    let in_place = name.is_empty();
    let output = if in_place {
        ms.path.clone()
    } else {
        sorted_output_path(Path::new(name), &ms.path)?
    };
    let link = ms.working_metadata_link();
    link.verify_directory().map_err(runtime_error)?;
    let destination = SortedDestination::inspect(&output, "sorted")?;
    let locks = if let SortedDestination::Table(_) = destination {
        check_write_access(&output).map_err(runtime_error)?;
        lock_table_tree_for_replacement_with_held(&output, &ms.write_locks)
            .map_err(runtime_error)?
    } else {
        Vec::new()
    };
    let source = link.snapshot(None).map_err(runtime_error)?;
    if !in_place {
        check_named_sort_output(&source.ms, &output).map_err(runtime_error)?;
    }
    let held = ms
        .read_locks
        .keys()
        .chain(ms.write_locks.keys())
        .cloned()
        .chain(locks.iter().map(|lock| lock.path().to_path_buf()))
        .collect();
    let workspace = tempfile::tempdir_in(parent_directory(&output)).map_err(runtime_error)?;
    let staged = workspace.path().join("sorted.ms");
    write_sorted_measurement_set(
        &source.ms,
        workspace.path(),
        &staged,
        columns,
        in_place,
        &ms.path,
        &held,
    )
    .map_err(runtime_error)?;
    link.verify_directory().map_err(runtime_error)?;
    destination.publish(&staged, &output, "sorted")?;
    if !in_place {
        return Ok(None);
    }
    match AttachedMs::open(output.clone(), true, false, false) {
        Ok(mut replacement) => {
            replacement.name = ms.name.clone();
            Ok(Some(replacement))
        }
        Err(error) => {
            if let Err(rollback) = exchange_directories(&staged, &output) {
                let backup = workspace.keep();
                return Err(runtime_error(format!(
                    "could not reopen sorted MS: {error}; rollback failed: {rollback}; original at {}",
                    backup.join("sorted.ms").display()
                )));
            }
            Err(error)
        }
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

pub(super) enum SortedDestination {
    Absent,
    Empty(fs::Metadata),
    Table(fs::Metadata),
}

impl SortedDestination {
    pub(super) fn inspect(path: &Path, operation: &str) -> PyResult<Self> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Self::Absent),
            Err(error) => return Err(runtime_error(error)),
        };
        if metadata.is_dir() {
            if fs::read_dir(path).map_err(runtime_error)?.next().is_none() {
                return Ok(Self::Empty(metadata));
            }
            if Table::open(TableOptions::new(path)).is_ok() {
                return Ok(Self::Table(metadata));
            }
        }
        Err(runtime_error(format!(
            "{operation} output already exists and is not a table"
        )))
    }

    pub(super) fn is_table(&self) -> bool {
        matches!(self, Self::Table(_))
    }

    pub(super) fn publish(self, staged: &Path, output: &Path, operation: &str) -> PyResult<()> {
        let original = match &self {
            Self::Absent => return rename_without_replacing(staged, output).map_err(runtime_error),
            Self::Empty(original) | Self::Table(original) => original,
        };
        let current = fs::symlink_metadata(output).map_err(runtime_error)?;
        if !same_directory(original, &current) {
            return Err(runtime_error(format!(
                "{operation} output directory changed before publication"
            )));
        }
        match self {
            // rename rejects a directory that acquired files during staging.
            Self::Empty(_) => fs::rename(staged, output).map_err(runtime_error),
            Self::Table(_) => exchange_directories(staged, output).map_err(runtime_error),
            Self::Absent => unreachable!("handled above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_destination_that_gained_files_is_preserved() {
        let scratch = tempfile::tempdir().unwrap();
        let output = scratch.path().join("output");
        let staged = scratch.path().join("staged");
        fs::create_dir(&output).unwrap();
        fs::create_dir(&staged).unwrap();
        let destination = SortedDestination::inspect(&output, "sorted").unwrap();
        let marker = output.join("keep");
        fs::write(&marker, "still there").unwrap();
        assert!(destination.publish(&staged, &output, "sorted").is_err());
        assert_eq!(fs::read_to_string(marker).unwrap(), "still there");
        assert!(staged.is_dir());
    }
}
