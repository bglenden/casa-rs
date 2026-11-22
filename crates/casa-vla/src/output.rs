// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private staging and publication of a VLA import's on-disk products.

use std::path::{Path, PathBuf};

use casa_tables::publication::rename_without_replacing;

use crate::VlaError;

pub(crate) struct ImportOutput {
    _staging: tempfile::TempDir,
    path: PathBuf,
    final_path: PathBuf,
    initial_flags: bool,
}

impl ImportOutput {
    pub(crate) fn prepare(final_path: &Path, initial_flags: bool) -> Result<Self, VlaError> {
        let name = final_path
            .file_name()
            .ok_or_else(|| VlaError::InvalidArgument {
                argument: "vis",
                message: "output MeasurementSet needs a file name".into(),
            })?;
        for product in [
            Some(final_path.to_owned()),
            initial_flags.then(|| flagversions(final_path)),
        ]
        .into_iter()
        .flatten()
        {
            if product.symlink_metadata().is_ok() {
                return Err(VlaError::InvalidArgument {
                    argument: "vis",
                    message: format!("output already exists: {}", product.display()),
                });
            }
        }
        let parent = final_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let staging = tempfile::Builder::new()
            .prefix(".importvla-")
            .tempdir_in(parent)
            .map_err(|source| {
                VlaError::import(format!("stage {}: {source}", final_path.display()))
            })?;
        let path = staging.path().join(name);
        Ok(Self {
            _staging: staging,
            path,
            final_path: final_path.to_owned(),
            initial_flags,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn publish(self) -> Result<(), VlaError> {
        let error =
            |source| VlaError::import(format!("publish {}: {source}", self.final_path.display()));
        let staged_flags = flagversions(&self.path);
        let final_flags = flagversions(&self.final_path);
        if self.initial_flags {
            rename_without_replacing(&staged_flags, &final_flags).map_err(error)?;
        }
        if let Err(source) = rename_without_replacing(&self.path, &self.final_path) {
            if self.initial_flags
                && let Err(rollback) = rename_without_replacing(&final_flags, &staged_flags)
            {
                return Err(VlaError::import(format!(
                    "{0}; roll back {1}: {rollback}",
                    error(source),
                    final_flags.display()
                )));
            }
            return Err(error(source));
        }
        Ok(())
    }
}

fn flagversions(path: &Path) -> PathBuf {
    let mut companion = path.as_os_str().to_owned();
    companion.push(".flagversions");
    PathBuf::from(companion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn a_conflicting_ms_keeps_its_files_and_rolls_back_our_original_flags() {
        let root = tempfile::tempdir().unwrap();
        let dest = root.path().join("result.ms");
        let output = ImportOutput::prepare(&dest, true).unwrap();
        let stage = output.path().parent().unwrap().to_owned();
        fs::create_dir(output.path()).unwrap();
        fs::create_dir(flagversions(output.path())).unwrap();
        fs::create_dir(&dest).unwrap();
        fs::write(dest.join("marker"), b"other writer").unwrap();

        assert!(output.publish().is_err());
        assert_eq!(fs::read(dest.join("marker")).unwrap(), b"other writer");
        assert!(!flagversions(&dest).exists());
        assert!(!stage.exists());
    }
}
