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
            Storage::Temp { image, .. } => image.concat_source().map_or(Ok(true), |source| source.lock(kind, attempts).map_err(raised)),
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
            Storage::Temp { image, .. } => image.concat_source().is_none_or(|source| source.has_lock(kind)),
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
            Storage::Temp { image, .. } => image.concat_source().map_or(Ok(()), |source| source.unlock().map_err(raised)),
        });
        #[cfg(not(unix))]
        Ok(())
    }

    pub(super) fn release_requested_lock(&mut self) -> PyResult<()> {
        #[cfg(unix)]
        return with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.release_requested_lock().map_err(raised),
            Storage::Temp { image, .. } => image.concat_source().map_or(Ok(()), |source| source.release_requested_lock().map_err(raised)),
        });
        #[cfg(not(unix))]
        Ok(())
    }
}

impl AttachedImage {
    pub(super) fn lock_expression_operand(&self, kind: LockType, attempts: u32) -> PyResult<bool> {
        let mut source = self.lock();
        self.check_bound_name(&source)?;
        source.lock_storage(kind, attempts)
    }

    fn lock_with_inputs(
        &self,
        handle: &mut ImageHandle,
        kind: LockType,
        attempts: u32,
    ) -> PyResult<bool> {
        self.check_bound_name(handle)?;
        for input in self.expression_inputs() {
            if !input.lock_expression_operand(kind, attempts)? {
                return Ok(false);
            }
        }
        handle.lock_storage(kind, attempts)
    }

    pub(super) fn require_lock(&self, handle: &mut ImageHandle, kind: LockType) -> PyResult<()> {
        self.check_bound_name(handle)?;
        if kind == LockType::Write
            && with_pixels!(&*handle, storage => matches!(storage, Storage::Paged(image) if !image.is_writable()))
        {
            let path = handle
                .path()
                .expect("a read-only paged image has a table path");
            return Err(raised(format!(
                "Table {} cannot be opened for read/write",
                path.display()
            )));
        }
        let implicit_concat = with_pixels!(&*handle, storage => match storage {
            Storage::Temp { image, .. } => image.concat_source().is_some_and(|source|
                kind == LockType::Read || source.temp_close()),
            Storage::Paged(_) => false,
        });
        if implicit_concat {
            return Ok(());
        }
        if handle.lock_storage(kind, 0)? {
            Ok(())
        } else {
            Err(raised(format!("Could not acquire the image {kind:?} lock")))
        }
    }

    pub(super) fn require_pixel_read_lock(&self, handle: &mut ImageHandle) -> PyResult<()> {
        if handle.image_type_name() == "ImageConcat" {
            return self.check_bound_name(handle);
        }
        if self.lock_with_inputs(handle, LockType::Read, 0)? {
            Ok(())
        } else {
            Err(raised("Could not acquire the image Read lock"))
        }
    }

    pub(super) fn release_requested_lock(&self, handle: &mut ImageHandle) -> PyResult<()> {
        for input in self.expression_inputs() {
            input.lock().release_requested_lock()?;
        }
        handle.release_requested_lock()
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
            Ok::<_, PyErr>(
                [LockType::Read, LockType::Write]
                    .map(|kind| {
                        handle.has_storage_lock(kind)
                            && attachment
                                .expression_inputs()
                                .iter()
                                .all(|input| input.lock().has_storage_lock(kind))
                    })
                    .to_vec(),
            )
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
            let kind = if writelock {
                LockType::Write
            } else {
                LockType::Read
            };
            attachment.lock_with_inputs(&mut handle, kind, nattempts.max(0) as u32)
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
            for input in attachment.expression_inputs() {
                let mut source = input.lock();
                input.check_bound_name(&source)?;
                source.unlock_storage()?;
            }
            handle.unlock_storage()?;
            Ok(true)
        })
    }
}
