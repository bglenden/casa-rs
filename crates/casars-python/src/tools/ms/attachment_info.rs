// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_tables::Table;
use pyo3::prelude::*;

use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

impl AttachedMs {
    fn nrows(&self, selected: bool) -> PyResult<usize> {
        if selected && let Some(rows) = &self.selected_rows {
            return Ok(rows.len());
        }
        if self.part_names.as_slice() == std::slice::from_ref(&self.path) {
            // A plain attached table can receive rows through another handle.
            // Read its persisted count without reopening the MS's subtables.
            return Table::rows_on_disk(&self.sources[0]).map_err(runtime_error);
        }
        // A reference or concatenation has its row order fixed by the
        // descriptor read at attachment time.
        Ok(self.original_rows)
    }
}

impl PyMsTool {
    fn attachment_info<T: Send>(
        &self,
        py: Python<'_>,
        read: impl FnOnce(Option<&AttachedMs>) -> PyResult<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let attached = self.attached.lock().map_err(runtime_error)?;
            read(attached.as_ref())
        })
    }
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn name(&self, py: Python<'_>) -> PyResult<String> {
        self.attachment_info(py, |attached| {
            Ok(attached.map_or_else(
                || "none".into(),
                |ms| ms.path.to_string_lossy().into_owned(),
            ))
        })
    }

    #[pyo3(signature = (selected = false), text_signature = "($self, selected=False)")]
    fn nrow(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = strict_bool)] selected: bool,
    ) -> PyResult<usize> {
        self.attachment_info(py, |attached| {
            attached.map_or(Ok(0), |ms| ms.nrows(selected))
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn iswritable(&self, py: Python<'_>) -> PyResult<bool> {
        self.attachment_info(py, |attached| Ok(attached.is_some_and(|ms| ms.writable)))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn ismultims(&self, py: Python<'_>) -> PyResult<bool> {
        self.attachment_info(py, |attached| Ok(attached.is_some_and(|ms| ms.is_multi_ms)))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn getreferencedtables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.attachment_info(py, |attached| {
            Ok(attached.map_or_else(Vec::new, |ms| {
                ms.part_names
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect()
            }))
        })
    }
}
