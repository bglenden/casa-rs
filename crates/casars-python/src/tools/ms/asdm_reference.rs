// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and retarget the original ASDM files used by a lazy MeasurementSet.

use std::borrow::Cow;
use std::collections::BTreeSet;

use casa_tables::TableWriteLock;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::tools::casacore_path::expand_asdm_path;

use super::selection_indices::selection_text;
use super::{AttachedMs, PyMsTool, runtime_error};

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (abspath = Ok(String::new())), text_signature = "($self, abspath='')")]
    fn asdmref(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = path_argument)] abspath: PyResult<String>,
    ) -> PyResult<String> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            match attached.as_mut() {
                Some(ms) => ms.asdm_reference(abspath),
                None => Ok(String::new()),
            }
        })
    }
}

impl AttachedMs {
    fn asdm_reference(&mut self, new_path: PyResult<String>) -> PyResult<String> {
        let link = self.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let stable_plain =
            self.plain_rows.is_some() && self.main_files.same_descriptor(self.main_stamp);
        let captured = if stable_plain {
            None
        } else {
            Some(self.visibility.current_main(&self.path)?)
        };
        let fresh;
        let binding = if let Some(layout) = &captured {
            fresh = layout.table.asdm_bdf_index().map_err(runtime_error)?;
            &fresh
        } else {
            if self.asdm_index.is_none() {
                self.asdm_index = Some(self.main.asdm_bdf_index().map_err(runtime_error)?);
            }
            self.asdm_index.as_ref().expect("cached plain ASDM binding")
        };
        let Some(index) = binding else {
            return Ok(String::new());
        };
        let new_path = new_path?;
        if new_path.is_empty() || new_path == "/" {
            return index.reference(expand_asdm_path).map_err(runtime_error);
        }

        // CASA permits this explicit index retarget even on a nomodify
        // attachment. The cells themselves remain read-only.
        let root = if stable_plain {
            Cow::Borrowed(self.path.as_path())
        } else {
            Cow::Owned(index.table_path().canonicalize().map_err(runtime_error)?)
        };
        let temporary = if self.write_locks.contains_key(root.as_ref()) {
            None
        } else {
            Some(TableWriteLock::acquire_to_reread(root.as_ref(), 0).map_err(runtime_error)?)
        };
        link.verify_directory().map_err(runtime_error)?;
        if let Some(layout) = &captured {
            layout.verify_write_layout(&BTreeSet::new())?;
        } else if !self.main_files.same_descriptor(self.main_stamp) {
            return Err(runtime_error(
                "MeasurementSet ASDM binding changed. Reopen the table.",
            ));
        }
        let lock = self
            .write_locks
            .get(root.as_ref())
            .or(temporary.as_ref())
            .expect("ASDM index update holds its physical table lock");
        let relocated = index
            .relocate(&new_path, lock, expand_asdm_path)
            .map_err(runtime_error)?;
        if let Some(lock) = temporary {
            lock.release().map_err(runtime_error)?;
        }
        if !relocated.is_empty() {
            self.refresh_asdm_readers()?;
        }
        Ok(relocated)
    }
}

fn path_argument(value: &Bound<'_, PyAny>) -> PyResult<PyResult<String>> {
    // CASA consumes bytes only when it addresses an ASDM binding.
    if value.is_instance_of::<PyBytes>() {
        Ok(selection_text(value, "abspath"))
    } else {
        selection_text(value, "abspath").map(Ok)
    }
}
