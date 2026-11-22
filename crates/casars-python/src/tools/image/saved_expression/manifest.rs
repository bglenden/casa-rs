// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use casa_images::ImagePixelType;
use casa_images::expr_file::{
    ToolExpressionManifest, encode_tool_manifest, read_tool_manifest,
    read_tool_manifest_from_reader,
};
use casa_types::{PrimitiveType, RecordValue};
use pyo3::prelude::*;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use crate::tools::image::handle::same_file_snapshot;
use crate::tools::image::handle::{raised, same_file_identity};

const MANIFEST: &str = "imageexpr.json";
type ManifestBinding = Mutex<fs::File>;
static MANIFESTS: Mutex<BTreeMap<PathBuf, Weak<ManifestBinding>>> = Mutex::new(BTreeMap::new());

pub(in crate::tools::image) struct SavedExpressionDirectory {
    pub(super) path: PathBuf,
    pub(super) identity: fs::Metadata,
    pub(super) expression: String,
    manifest: Option<Arc<ManifestBinding>>,
    #[cfg(unix)]
    handle: fs::File,
}

impl SavedExpressionDirectory {
    pub(super) fn open(name: &Path, path: PathBuf) -> PyResult<(Self, ToolExpressionManifest)> {
        #[cfg(unix)]
        let handle = casa_tables::publication::open_directory_identity(&path).map_err(raised)?;
        #[cfg(unix)]
        let identity = handle.metadata().map_err(raised)?;
        #[cfg(not(unix))]
        let identity = path.metadata().map_err(raised)?;
        let mut directory = Self {
            path,
            identity,
            expression: String::new(),
            manifest: None,
            #[cfg(unix)]
            handle,
        };
        directory.check_name(name)?;
        let entry = directory.path.join(MANIFEST);
        let attributes = entry.symlink_metadata().map_err(raised)?;
        let manifest = if attributes.is_symlink() {
            // Casacore permits reading a linked manifest. Writes need an
            // owned regular entry in the captured directory.
            read_tool_manifest(&directory.path).map_err(raised)?
        } else {
            let file = directory.open_regular_manifest()?;
            let manifest = read_tool_manifest_from_reader(&file).map_err(raised)?;
            let attributes = file.metadata().map_err(raised)?;
            let visible = directory
                .open_regular_manifest()?
                .metadata()
                .map_err(raised)?;
            if !same_file_identity(&attributes, &visible) {
                return Err(raised("Saved image expression manifest has changed"));
            }
            directory.manifest = Some(bind_manifest(entry, &attributes, file)?);
            manifest
        };
        directory.expression = manifest.expression.clone();
        Ok((directory, manifest))
    }

    pub(super) fn check_name(&self, name: &Path) -> PyResult<()> {
        if name.canonicalize().is_ok_and(|path| path == self.path)
            && self
                .path
                .symlink_metadata()
                .is_ok_and(|current| same_file_identity(&self.identity, &current))
        {
            return Ok(());
        }
        Err(raised(format!(
            "Saved image expression at {} has changed",
            name.display()
        )))
    }

    fn open_regular_manifest(&self) -> PyResult<fs::File> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return casa_tables::publication::open_regular_directory_entry(
            &self.handle,
            OsStr::new(MANIFEST),
        )
        .map_err(raised);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let path = self.path.join(MANIFEST);
            if !path.symlink_metadata().map_err(raised)?.is_file() {
                return Err(raised(
                    "Saved image expression manifest must be a regular file",
                ));
            }
            fs::File::open(path).map_err(raised)
        }
    }

    pub(super) fn lock_manifest(&self) -> Option<MutexGuard<'_, fs::File>> {
        self.manifest
            .as_ref()
            .map(|binding| binding.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub(super) fn read_bound_manifest(
        &self,
        bound: Option<&fs::File>,
    ) -> PyResult<ToolExpressionManifest> {
        let manifest = if let Some(bound) = bound {
            let current = self.open_regular_manifest()?;
            if !same_file_identity(
                &bound.metadata().map_err(raised)?,
                &current.metadata().map_err(raised)?,
            ) {
                return Err(raised("Saved image expression manifest has changed"));
            }
            read_tool_manifest_from_reader(current).map_err(raised)?
        } else {
            read_tool_manifest(&self.path).map_err(raised)?
        };
        if manifest.expression != self.expression {
            return Err(raised("Saved image expression manifest has changed"));
        }
        Ok(manifest)
    }

    pub(in crate::tools::image) fn write_metadata(
        &self,
        name: &Path,
        pixel_type: ImagePixelType,
        record: &RecordValue,
    ) -> PyResult<()> {
        let pixel_type = match pixel_type {
            ImagePixelType::Float32 => PrimitiveType::Float32,
            ImagePixelType::Float64 => PrimitiveType::Float64,
            ImagePixelType::Complex32 => PrimitiveType::Complex32,
            ImagePixelType::Complex64 => PrimitiveType::Complex64,
        };
        let content = encode_tool_manifest(&self.expression, pixel_type, record).map_err(raised)?;
        let binding = self
            .manifest
            .as_ref()
            .ok_or_else(|| raised("Saved image expression manifest must be a regular file"))?;
        let mut bound = binding.lock().unwrap_or_else(PoisonError::into_inner);
        self.check_name(name)?;
        let current = self.open_regular_manifest()?;
        let before = current.metadata().map_err(raised)?;
        if !same_file_identity(&bound.metadata().map_err(raised)?, &before) {
            return Err(raised("Saved image expression manifest has changed"));
        }
        casa_tables::publication::check_write_access(&self.path.join(MANIFEST)).map_err(raised)?;
        if read_tool_manifest_from_reader(&current)
            .map_err(raised)?
            .expression
            != self.expression
        {
            return Err(raised("Saved image expression manifest has changed"));
        }
        *bound = self.publish(name, &before, &content)?;
        Ok(())
    }

    fn publish(&self, name: &Path, before: &fs::Metadata, content: &[u8]) -> PyResult<fs::File> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let mut staged =
                casa_tables::publication::StagedDirectoryFile::new(&self.handle, &self.path)
                    .map_err(raised)?;
            write_staged(staged.file_mut(), before, content)?;
            self.check_name(name)?;
            staged.replace(OsStr::new(MANIFEST), before).map_err(raised)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let mut staged = tempfile::Builder::new()
                .prefix(".imageexpr-")
                .tempfile_in(&self.path)
                .map_err(raised)?;
            write_staged(staged.as_file_mut(), before, content)?;
            self.check_name(name)?;
            let target = self.path.join(MANIFEST);
            if !target
                .symlink_metadata()
                .is_ok_and(|current| same_file_snapshot(before, &current))
            {
                return Err(raised("Saved image expression manifest has changed"));
            }
            staged.persist(target).map_err(raised)
        }
    }
}

fn bind_manifest(
    entry: PathBuf,
    attributes: &fs::Metadata,
    file: fs::File,
) -> PyResult<Arc<ManifestBinding>> {
    let mut manifests = MANIFESTS.lock().unwrap_or_else(PoisonError::into_inner);
    manifests.retain(|_, binding| binding.strong_count() > 0);
    if let Some(existing) = manifests.get(&entry).and_then(Weak::upgrade) {
        let existing_attributes = existing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .metadata()
            .map_err(raised)?;
        if same_file_identity(attributes, &existing_attributes) {
            return Ok(existing);
        }
    }
    let binding = Arc::new(Mutex::new(file));
    manifests.insert(entry, Arc::downgrade(&binding));
    Ok(binding)
}

fn write_staged(file: &mut fs::File, before: &fs::Metadata, content: &[u8]) -> PyResult<()> {
    file.write_all(content).map_err(raised)?;
    file.set_permissions(before.permissions()).map_err(raised)?;
    file.sync_all().map_err(raised)
}
