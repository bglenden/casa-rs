// SPDX-License-Identifier: LGPL-3.0-or-later

//! Atomic staging and installation for text log files written by image tools.

use std::fs::{File, Metadata};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::error::{ImageToolError, ImageToolResult};

/// A text report written beside its destination and installed atomically.
pub struct StagedTextOutput {
    path: PathBuf,
    previous: Option<Metadata>,
    file: tempfile::NamedTempFile,
}

impl StagedTextOutput {
    /// Stages `text` beside `path`, preserving existing permissions and optionally appending.
    pub fn new(path: &Path, text: &str, append: bool) -> ImageToolResult<Self> {
        let (path, previous) = match path.symlink_metadata() {
            Ok(_) => {
                let target = path.canonicalize()?;
                let metadata = target.metadata()?;
                if !metadata.is_file() {
                    return Err(ImageToolError::new(format!(
                        "{} is not a regular log file",
                        path.display()
                    )));
                }
                (target, Some(metadata))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => (path.to_path_buf(), None),
            Err(error) => return Err(error.into()),
        };
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut file = tempfile::NamedTempFile::new_in(parent)?;
        if let Some(metadata) = &previous {
            file.as_file().set_permissions(metadata.permissions())?;
            if append {
                io::copy(&mut File::open(&path)?, file.as_file_mut())?;
            }
        }
        file.write_all(text.as_bytes())?;
        file.flush()?;
        Ok(Self {
            path,
            previous,
            file,
        })
    }

    /// Atomically persists the staged text file over its destination.
    pub fn install(self) -> ImageToolResult<()> {
        if let Some(previous) = self.previous {
            let current = self.path.symlink_metadata()?;
            if !current.is_file()
                || previous.len() != current.len()
                || previous.modified().ok() != current.modified().ok()
            {
                return Err(ImageToolError::new(format!(
                    "Log {} changed during fitting",
                    self.path.display()
                )));
            }
            self.file.persist(self.path)?;
        } else {
            self.file.persist_noclobber(self.path)?;
        }
        Ok(())
    }
}
