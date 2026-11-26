// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::{Path, PathBuf};

use casa_images::ImageError;

/// RAII staging guard ensuring FITS output is written cleanly in the destination directory
/// without CFITSIO `!`-clobber or extended-filename parsing, and replaces any existing target
/// atomically only after all header, history, pixel, and `BEAMS` writes succeed.
pub(super) struct StagedFitsFile {
    path: PathBuf,
    _directory: tempfile::TempDir,
}

impl StagedFitsFile {
    pub(super) fn new_sibling(target: &Path) -> Result<Self, ImageError> {
        let parent = match target.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let canonical_parent =
            fs::canonicalize(parent).map_err(|error| ImageError::Io(error.to_string()))?;
        let directory = tempfile::Builder::new()
            .prefix(".exportfits.")
            .tempdir_in(canonical_parent)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        Ok(Self {
            path: directory.path().join("image.fits"),
            _directory: directory,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn persist_to(self, target: &Path, overwrite: bool) -> Result<(), ImageError> {
        let result = if overwrite {
            fs::rename(&self.path, target)
        } else {
            fs::hard_link(&self.path, target)
        };
        result.map_err(|error| ImageError::Io(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_fits_keeps_a_destination_created_during_export() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.fits");
        let staged = StagedFitsFile::new_sibling(&target).unwrap();
        let path = staged.path().to_path_buf();
        fs::write(&path, b"new FITS image").unwrap();
        fs::write(&target, b"concurrent image").unwrap();
        assert!(staged.persist_to(&target, false).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"concurrent image");
        assert!(!path.exists());
    }
}
