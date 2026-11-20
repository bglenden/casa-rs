// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_tables::LockType;
use numpy::{IntoPyArray, PyArray1};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyBool;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::handle::{ImageHandle, Storage, raised, with_pixels};

fn write_flag(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|flag| flag.is_true())
        .map_err(|_| PyTypeError::new_err("in method 'image_lock', argument 2 of type 'bool'"))
}

fn attempts(value: &Bound<'_, PyAny>) -> PyResult<i32> {
    crate::tools::table::integer(Some(value), "nattempts", 0).map(|value| value as i32)
}

impl ImageHandle {
    pub(super) fn lock_storage(&mut self, kind: LockType, attempts: u32) -> PyResult<bool> {
        #[cfg(unix)]
        return with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.lock(kind, attempts).map_err(raised),
            Storage::Temp { .. } => Ok(true),
        });
        #[cfg(not(unix))]
        {
            let _ = (kind, attempts);
            Ok(true)
        }
    }

    pub(super) fn has_storage_lock(&self, kind: LockType) -> bool {
        #[cfg(unix)]
        return with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.has_lock(kind),
            Storage::Temp { .. } => true,
        });
        #[cfg(not(unix))]
        {
            let _ = kind;
            true
        }
    }

    pub(super) fn unlock_storage(&mut self) -> PyResult<()> {
        #[cfg(unix)]
        return with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.unlock().map_err(raised),
            Storage::Temp { .. } => Ok(()),
        });
        #[cfg(not(unix))]
        Ok(())
    }

    pub(super) fn release_requested_lock(&mut self) -> PyResult<()> {
        #[cfg(unix)]
        return with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.release_requested_lock().map_err(raised),
            Storage::Temp { .. } => Ok(()),
        });
        #[cfg(not(unix))]
        Ok(())
    }
}

impl AttachedImage {
    pub(super) fn require_lock(&self, handle: &mut ImageHandle, kind: LockType) -> PyResult<()> {
        self.check_bound_name(handle)?;
        if handle.lock_storage(kind, 0)? {
            Ok(())
        } else {
            Err(raised(format!("Could not acquire the image {kind:?} lock")))
        }
    }
}

#[pymethods]
impl PyImageTool {
    fn haslock<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<bool>>> {
        let flags = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(Vec::new());
            };
            let handle = attachment.lock();
            handle.check_single_precision("haslock")?;
            Ok::<_, PyErr>(vec![
                handle.has_storage_lock(LockType::Read),
                handle.has_storage_lock(LockType::Write),
            ])
        })?;
        Ok(flags.into_pyarray(py))
    }

    #[pyo3(signature = (writelock=false, nattempts=0), text_signature = "($self, writelock=False, nattempts=0)")]
    fn lock(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = write_flag)] writelock: bool,
        #[pyo3(from_py_with = attempts)] nattempts: i32,
    ) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut handle = attachment.lock();
            handle.check_single_precision("lock")?;
            attachment.check_bound_name(&handle)?;
            let kind = if writelock {
                LockType::Write
            } else {
                LockType::Read
            };
            handle.lock_storage(kind, nattempts.max(0) as u32)
        })
    }

    fn unlock(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut handle = attachment.lock();
            handle.check_single_precision("unlock")?;
            attachment.check_bound_name(&handle)?;
            handle.unlock_storage()?;
            Ok(true)
        })
    }
}
