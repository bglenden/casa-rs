// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::{File, Metadata};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use pyo3::prelude::*;

use super::handle::raised;

/// A text report written beside its destination and installed atomically.
pub(super) struct StagedTextOutput {
    path: PathBuf,
    previous: Option<Metadata>,
    file: tempfile::NamedTempFile,
}

impl StagedTextOutput {
    pub(super) fn new(path: &Path, text: &str, append: bool) -> PyResult<Self> {
        let (path, previous) = match path.symlink_metadata() {
            Ok(_) => {
                let target = path.canonicalize().map_err(raised)?;
                let metadata = target.metadata().map_err(raised)?;
                if !metadata.is_file() {
                    return Err(raised(format!(
                        "{} is not a regular log file",
                        path.display()
                    )));
                }
                (target, Some(metadata))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => (path.to_path_buf(), None),
            Err(error) => return Err(raised(error)),
        };
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut file = tempfile::NamedTempFile::new_in(parent).map_err(raised)?;
        if let Some(metadata) = &previous {
            file.as_file()
                .set_permissions(metadata.permissions())
                .map_err(raised)?;
            if append {
                io::copy(&mut File::open(&path).map_err(raised)?, file.as_file_mut())
                    .map_err(raised)?;
            }
        }
        file.write_all(text.as_bytes()).map_err(raised)?;
        file.flush().map_err(raised)?;
        Ok(Self {
            path,
            previous,
            file,
        })
    }

    pub(super) fn install(self) -> PyResult<()> {
        if let Some(previous) = self.previous {
            let current = self.path.symlink_metadata().map_err(raised)?;
            if !current.is_file()
                || previous.len() != current.len()
                || previous.modified().ok() != current.modified().ok()
            {
                return Err(raised(format!(
                    "Log {} changed during fitting",
                    self.path.display()
                )));
            }
            self.file.persist(self.path).map_err(raised)?;
        } else {
            self.file.persist_noclobber(self.path).map_err(raised)?;
        }
        Ok(())
    }
}
