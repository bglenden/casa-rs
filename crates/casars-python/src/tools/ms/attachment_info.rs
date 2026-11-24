// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::Cell;
#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

use casa_tables::{Table, TableError, TablePartNames};
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
pub(super) struct TableStamp {
    control: FileStamp,
    lock: Option<FileStamp>,
}

#[cfg(not(unix))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct TableStamp;

pub(super) struct TableFiles {
    #[cfg(unix)]
    control: PathBuf,
    #[cfg(unix)]
    lock: PathBuf,
}

impl TableFiles {
    pub(super) fn new(path: &Path) -> Self {
        #[cfg(not(unix))]
        let _ = path;
        Self {
            #[cfg(unix)]
            control: path.join("table.dat"),
            #[cfg(unix)]
            lock: path.join("table.lock"),
        }
    }

    pub(super) fn stamp(&self) -> Option<TableStamp> {
        #[cfg(unix)]
        {
            self.read_stamp().ok()
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    pub(super) fn same_descriptor(&self, stamp: Option<TableStamp>) -> bool {
        #[cfg(unix)]
        {
            stamp.is_some_and(|stamp| {
                FileStamp::read(&self.control).is_ok_and(|now| now == stamp.control)
            })
        }
        #[cfg(not(unix))]
        {
            let _ = stamp;
            true
        }
    }

    #[cfg(unix)]
    fn read_stamp(&self) -> io::Result<TableStamp> {
        let control = FileStamp::read(&self.control)?;
        let lock = match FileStamp::read(&self.lock) {
            Ok(stamp) => Some(stamp),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(TableStamp { control, lock })
    }
}

pub(super) struct PlainRows {
    files: TableFiles,
    cached: Cell<Option<(TableStamp, usize)>>,
}

impl PlainRows {
    pub(super) fn new(path: &Path) -> Self {
        Self {
            files: TableFiles::new(path),
            cached: Cell::new(None),
        }
    }

    pub(super) fn count(&self, path: &Path) -> Result<usize, TableError> {
        let stamp = self.files.stamp();
        if let (Some(stamp), Some((cached, count))) = (stamp, self.cached.get())
            && stamp == cached
        {
            return Ok(count);
        }

        // casacore can publish new rows in table.lock before rewriting table.dat.
        // Record the stamp from before reading so concurrent publication forces
        // the next query to check the persisted count again.
        let count = Table::rows_on_disk(path)?;
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
            return rows.count(&self.path).map_err(runtime_error);
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
                    TablePartNames::Concatenation { .. } if ms.selected_rows.is_some() => {
                        std::slice::from_ref(&ms.name)
                    }
                    TablePartNames::Concatenation { parts, .. } => parts.as_slice(),
                };
                parts
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect()
            }))
        })
    }
}
