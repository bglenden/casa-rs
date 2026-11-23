// SPDX-License-Identifier: LGPL-3.0-or-later

#[cfg(unix)]
use std::cell::Cell;
#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

use casa_tables::{Table, TablePartNames};
use pyo3::prelude::*;

use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    identity: (u64, u64),
    len: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

#[cfg(unix)]
impl FileStamp {
    fn read(path: &Path) -> io::Result<Self> {
        let meta = std::fs::metadata(path)?;
        Ok(Self {
            identity: (meta.dev(), meta.ino()),
            len: meta.len(),
            modified: (meta.mtime(), meta.mtime_nsec()),
            changed: (meta.ctime(), meta.ctime_nsec()),
        })
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct RowStamp {
    control: FileStamp,
    lock: Option<FileStamp>,
}

pub(super) struct PlainRows {
    #[cfg(unix)]
    control: PathBuf,
    #[cfg(unix)]
    lock: PathBuf,
    #[cfg(unix)]
    cached: Cell<Option<(RowStamp, usize)>>,
}

impl PlainRows {
    pub(super) fn new(path: &Path) -> Self {
        #[cfg(not(unix))]
        let _ = path;
        Self {
            #[cfg(unix)]
            control: path.join("table.dat"),
            #[cfg(unix)]
            lock: path.join("table.lock"),
            #[cfg(unix)]
            cached: Cell::new(None),
        }
    }

    #[cfg(unix)]
    fn stamp(&self) -> io::Result<RowStamp> {
        let control = FileStamp::read(&self.control)?;
        let lock = match FileStamp::read(&self.lock) {
            Ok(stamp) => Some(stamp),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(RowStamp { control, lock })
    }

    fn count(&self, path: &Path) -> PyResult<usize> {
        #[cfg(unix)]
        let stamp = self.stamp().ok();
        #[cfg(unix)]
        if let (Some(stamp), Some((cached, count))) = (stamp, self.cached.get())
            && stamp == cached
        {
            return Ok(count);
        }

        // casacore can publish new rows in table.lock before rewriting table.dat.
        // Record the stamp from before reading so concurrent publication forces
        // the next query to check the persisted count again.
        let count = Table::rows_on_disk(path).map_err(runtime_error)?;
        #[cfg(unix)]
        self.cached.set(stamp.map(|stamp| (stamp, count)));
        Ok(count)
    }
}

impl AttachedMs {
    fn nrows(&self, selected: bool) -> PyResult<usize> {
        if selected && let Some(rows) = &self.selected_rows {
            return Ok(rows.len());
        }
        if let Some(rows) = &self.plain_rows {
            return rows.count(&self.path);
        }
        // A reference or concatenation has its row order fixed by the
        // descriptor read at attachment time.
        Ok(self.original_rows)
    }
}

impl PyMsTool {
    pub(super) fn attachment_info<T: Send>(
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
                |ms| ms.name.to_string_lossy().into_owned(),
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
                let parts = match &ms.part_names {
                    TablePartNames::Plain(path) | TablePartNames::Reference(path) => {
                        std::slice::from_ref(path)
                    }
                    // A selection is a reference over the entire concat table.
                    TablePartNames::Concatenation(_) if ms.selected_rows.is_some() => {
                        std::slice::from_ref(&ms.name)
                    }
                    TablePartNames::Concatenation(parts) => parts.as_slice(),
                };
                parts
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect()
            }))
        })
    }
}
