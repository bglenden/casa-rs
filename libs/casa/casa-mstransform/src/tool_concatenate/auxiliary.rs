// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and publish CASA's virtual-concatenation observation and scan file.

use std::fs::{self, Metadata};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};

use casa_tables::{Table, publication::check_write_access};
use tempfile::NamedTempFile;

use super::{
    locking::same_identity,
    main_rows::sparse_id,
    publication::io,
    scan::ScanBounds,
    virtual_rows::{SCAN_COLUMNS, int32_column},
};
use casa_ms::MsResult;

pub(super) struct ScanFile {
    initial: Option<ScanBounds>,
    destination: Option<Destination>,
    staged: Option<NamedTempFile>,
}

struct Destination {
    declared: PathBuf,
    resolved: PathBuf,
    original: Option<(Metadata, String)>,
}

impl ScanFile {
    pub(super) fn inspect(path: &Path) -> MsResult<Self> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = match parent.canonicalize() {
            Ok(parent) => parent,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                tracing::warn!(path = %path.display(), "cannot create virtual concatenation scan file");
                return Ok(Self {
                    initial: None,
                    destination: None,
                    staged: None,
                });
            }
            Err(error) => return Err(io(error)),
        };
        let name = path
            .file_name()
            .ok_or_else(|| io("invalid virtual concatenation scan file name"))?;
        let declared = parent.join(name);
        let (resolved, original) = match fs::symlink_metadata(&declared) {
            Ok(_) => {
                let resolved = declared.canonicalize().map_err(io)?;
                let metadata = fs::metadata(&resolved).map_err(io)?;
                if !metadata.is_file() {
                    return Err(io("virtual concatenation scan file must be a regular file"));
                }
                let text = fs::read_to_string(&resolved).map_err(io)?;
                (resolved, Some((metadata, text)))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => (declared.clone(), None),
            Err(error) => return Err(io(error)),
        };
        let initial = original
            .as_ref()
            .map(|(_, text)| ScanBounds::parse(text))
            .transpose()?;
        Ok(Self {
            initial,
            destination: Some(Destination {
                declared,
                resolved,
                original,
            }),
            staged: None,
        })
    }

    pub(super) fn starting_bounds(
        &mut self,
        attached: &Table,
        selected: Option<&[usize]>,
        observations: &[usize],
    ) -> MsResult<ScanBounds> {
        if let Some(bounds) = self.initial.take() {
            return bounds.remap_observations(observations);
        }
        let columns = if let Some(selected) = selected {
            attached.required_scalar_columns_owned_for_rows(SCAN_COLUMNS, selected)?
        } else {
            attached.required_scalar_columns_owned(SCAN_COLUMNS)?
        };
        let mut bounds = ScanBounds::default();
        for (&id, &scan) in int32_column(&columns, "OBSERVATION_ID")?
            .iter()
            .zip(int32_column(&columns, "SCAN_NUMBER")?)
        {
            bounds.observe(sparse_id(observations, id)?, scan);
        }
        Ok(bounds)
    }

    pub(super) fn stage(&mut self, text: &str) {
        let Some(output) = &self.destination else {
            return;
        };
        match output.stage(text) {
            Ok(staged) => self.staged = Some(staged),
            Err(error) => {
                // CASA uses the computed IDs even if it cannot write the
                // auxiliary file for a later call.
                tracing::warn!(path = %output.declared.display(), %error, "cannot write virtual concatenation scan file");
            }
        }
    }

    pub(super) fn publish(self) -> MsResult<()> {
        let Some((staged, output)) = self.staged.zip(self.destination) else {
            return Ok(());
        };
        if let Some((expected, text)) = &output.original {
            let path = output.declared.canonicalize().map_err(io)?;
            if path != output.resolved
                || !same_identity(expected, &fs::metadata(&path).map_err(io)?)
                || fs::read_to_string(&path).map_err(io)? != *text
            {
                return Err(io("virtual concatenation scan file changed during staging"));
            }
            staged.persist(path).map_err(io)?;
        } else {
            staged.persist_noclobber(output.resolved).map_err(io)?;
        }
        Ok(())
    }
}

impl Destination {
    fn stage(&self, text: &str) -> std::io::Result<NamedTempFile> {
        let parent = self.resolved.parent().expect("resolved scan file parent");
        let mut staging = tempfile::Builder::new()
            .prefix(".casa-rs-ms-scan-")
            .tempfile_in(parent)?;
        if let Some((metadata, _)) = &self.original {
            check_write_access(&self.resolved)?;
            staging.as_file().set_permissions(metadata.permissions())?;
        }
        staging.write_all(text.as_bytes())?;
        staging.as_file().sync_all()?;
        Ok(staging)
    }
}
