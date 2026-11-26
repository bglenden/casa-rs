// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
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
    manifest: Arc<ManifestBinding>,
    target: ManifestTarget,
    link: Option<ManifestLink>,
    #[cfg(unix)]
    _directory: Arc<fs::File>,
}

struct ManifestLink {
    identity: fs::Metadata,
    contents: PathBuf,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    _handle: fs::File,
}

struct ManifestTarget {
    path: PathBuf,
    parent_identity: fs::Metadata,
    #[cfg(unix)]
    parent: Arc<fs::File>,
}

impl SavedExpressionDirectory {
    pub(super) fn open(name: &Path, path: PathBuf) -> PyResult<(Self, ToolExpressionManifest)> {
        #[cfg(unix)]
        let directory =
            Arc::new(casa_tables::publication::open_directory_identity(&path).map_err(raised)?);
        #[cfg(unix)]
        let identity = directory.metadata().map_err(raised)?;
        #[cfg(not(unix))]
        let identity = path.metadata().map_err(raised)?;
        check_name(name, &path, &identity)?;
        let entry = path.join(MANIFEST);
        let attributes = entry.symlink_metadata().map_err(raised)?;
        let (target_path, link) = if attributes.is_symlink() {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            let handle = casa_tables::publication::open_symlink_directory_entry(
                &directory,
                OsStr::new(MANIFEST),
            )
            .map_err(raised)?;
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            if !same_file_identity(&attributes, &handle.metadata().map_err(raised)?) {
                return Err(manifest_changed());
            }
            let contents = fs::read_link(&entry).map_err(raised)?;
            let link = ManifestLink {
                contents,
                identity: attributes,
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                _handle: handle,
            };
            (entry.canonicalize().map_err(raised)?, Some(link))
        } else {
            (entry.clone(), None)
        };
        let parent = target_path.parent().ok_or_else(manifest_changed)?;
        #[cfg(unix)]
        let target_parent = if parent == path {
            Arc::clone(&directory)
        } else {
            Arc::new(casa_tables::publication::open_directory_identity(parent).map_err(raised)?)
        };
        #[cfg(unix)]
        let parent_identity = target_parent.metadata().map_err(raised)?;
        #[cfg(not(unix))]
        let parent_identity = parent.symlink_metadata().map_err(raised)?;
        let target = ManifestTarget {
            path: target_path,
            parent_identity,
            #[cfg(unix)]
            parent: target_parent,
        };
        let binding = bind_manifest(&target)?;
        let bound = binding.lock().unwrap_or_else(PoisonError::into_inner);
        let file = target.open()?;
        let attributes = file.metadata().map_err(raised)?;
        if !same_file_identity(&bound.metadata().map_err(raised)?, &attributes) {
            return Err(manifest_changed());
        }
        let manifest = read_tool_manifest_from_reader(&file).map_err(raised)?;
        check_name(name, &path, &identity)?;
        target.check_location(link.as_ref(), &entry)?;
        let current = target.open()?.metadata().map_err(raised)?;
        if !same_file_identity(&attributes, &current) {
            return Err(manifest_changed());
        }
        drop(bound);
        let captured = Self {
            path,
            identity,
            expression: manifest.expression.clone(),
            manifest: binding,
            target,
            link,
            #[cfg(unix)]
            _directory: directory,
        };
        Ok((captured, manifest))
    }

    pub(super) fn check_name(&self, name: &Path) -> PyResult<()> {
        check_name(name, &self.path, &self.identity)
    }

    pub(super) fn linked_manifest_paths(&self) -> Option<(PathBuf, &Path)> {
        self.link
            .as_ref()
            .map(|_| (self.path.join(MANIFEST), self.target.path.as_path()))
    }

    pub(super) fn lock_manifest(&self) -> MutexGuard<'_, fs::File> {
        self.manifest.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open_bound_file(&self, bound: &fs::File) -> PyResult<(fs::File, fs::Metadata)> {
        self.target
            .check_location(self.link.as_ref(), &self.path.join(MANIFEST))?;
        let current = self.target.open()?;
        let before = current.metadata().map_err(raised)?;
        if !same_file_identity(&bound.metadata().map_err(raised)?, &before) {
            return Err(manifest_changed());
        }
        Ok((current, before))
    }

    fn read_manifest(&self, file: impl std::io::Read) -> PyResult<ToolExpressionManifest> {
        let manifest = read_tool_manifest_from_reader(file).map_err(raised)?;
        if manifest.expression != self.expression {
            return Err(manifest_changed());
        }
        Ok(manifest)
    }

    pub(super) fn read_bound_manifest(&self, bound: &fs::File) -> PyResult<ToolExpressionManifest> {
        let (current, _) = self.open_bound_file(bound)?;
        self.read_manifest(current)
    }

    pub(super) fn check_staged_manifest(&self, staged: &Path) -> PyResult<()> {
        let manifest = match &self.link {
            Some(link) => {
                if !fs::read_link(staged.join(MANIFEST))
                    .is_ok_and(|contents| contents == link.contents)
                {
                    return Err(manifest_changed());
                }
                // A relative link can resolve differently at a temporary staging path.
                // Read the copied internal file itself, or validate the original external
                // file when the link reaches its final location.
                let Ok(relative) = self.target.path.strip_prefix(&self.path) else {
                    return Ok(());
                };
                self.read_manifest(fs::File::open(staged.join(relative)).map_err(raised)?)?
            }
            None => {
                let manifest = read_tool_manifest(staged).map_err(raised)?;
                if manifest.expression != self.expression {
                    return Err(manifest_changed());
                }
                manifest
            }
        };
        manifest.misc_info().map(drop).map_err(raised)
    }

    pub(super) fn check_relocated_manifest(
        &self,
        relocated: &Self,
        before: &fs::Metadata,
        copied: bool,
    ) -> PyResult<()> {
        let relative = self.target.path.strip_prefix(&self.path).ok();
        let expected = relative.map(|path| relocated.path.join(path));
        if relocated.target.path != expected.as_deref().unwrap_or(&self.target.path) {
            return Err(manifest_changed());
        }
        match (&self.link, &relocated.link) {
            (Some(old), Some(new))
                if old.contents == new.contents
                    && (copied || same_file_identity(&old.identity, &new.identity)) => {}
            (None, None) => {}
            _ => return Err(manifest_changed()),
        }
        if !copied || relative.is_none() {
            let current = relocated.lock_manifest().metadata().map_err(raised)?;
            if !same_file_identity(before, &current) {
                return Err(manifest_changed());
            }
        }
        Ok(())
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
        let mut bound = self.lock_manifest();
        self.check_name(name)?;
        let (current, before) = self.open_bound_file(&bound)?;
        casa_tables::publication::check_write_access(&self.target.path).map_err(raised)?;
        self.read_manifest(current)?;
        *bound = self.target.publish(&before, &content, || {
            self.check_name(name)?;
            self.target
                .check_location(self.link.as_ref(), &self.path.join(MANIFEST))
        })?;
        Ok(())
    }
}

impl ManifestTarget {
    fn parent_path(&self) -> &Path {
        self.path.parent().expect("manifest has a parent")
    }

    fn entry_name(&self) -> &OsStr {
        self.path.file_name().expect("manifest has a name")
    }

    fn open(&self) -> PyResult<fs::File> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return casa_tables::publication::open_regular_directory_entry(
            &self.parent,
            self.entry_name(),
        )
        .map_err(raised);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if !self.path.symlink_metadata().map_err(raised)?.is_file() {
                return Err(raised(
                    "Saved image expression manifest must be a regular file",
                ));
            }
            fs::File::open(&self.path).map_err(raised)
        }
    }

    fn check_location(&self, link: Option<&ManifestLink>, entry: &Path) -> PyResult<()> {
        if !self
            .parent_path()
            .symlink_metadata()
            .is_ok_and(|actual| same_file_identity(&self.parent_identity, &actual))
        {
            return Err(manifest_changed());
        }
        if let Some(link) = link
            && !(entry
                .symlink_metadata()
                .is_ok_and(|actual| same_file_identity(&link.identity, &actual))
                && fs::read_link(entry).is_ok_and(|contents| contents == link.contents)
                && entry.canonicalize().is_ok_and(|path| path == self.path))
        {
            return Err(manifest_changed());
        }
        Ok(())
    }

    fn publish(
        &self,
        before: &fs::Metadata,
        content: &[u8],
        check_location: impl FnOnce() -> PyResult<()>,
    ) -> PyResult<fs::File> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let mut staged = casa_tables::publication::StagedDirectoryFile::new(
                &self.parent,
                self.parent_path(),
            )
            .map_err(raised)?;
            write_staged(staged.file_mut(), before, content)?;
            check_location()?;
            staged.replace(self.entry_name(), before).map_err(raised)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let mut staged = tempfile::Builder::new()
                .prefix(".imageexpr-")
                .tempfile_in(self.parent_path())
                .map_err(raised)?;
            write_staged(staged.as_file_mut(), before, content)?;
            check_location()?;
            if !self
                .path
                .symlink_metadata()
                .is_ok_and(|current| same_file_snapshot(before, &current))
            {
                return Err(manifest_changed());
            }
            staged.persist(&self.path).map_err(raised)
        }
    }
}

fn check_name(name: &Path, path: &Path, identity: &fs::Metadata) -> PyResult<()> {
    if name.canonicalize().is_ok_and(|actual| actual == path)
        && path
            .symlink_metadata()
            .is_ok_and(|current| same_file_identity(identity, &current))
    {
        return Ok(());
    }
    Err(raised(format!(
        "Saved image expression at {} has changed",
        name.display()
    )))
}

fn manifest_changed() -> PyErr {
    raised("Saved image expression manifest has changed")
}

fn bind_manifest(target: &ManifestTarget) -> PyResult<Arc<ManifestBinding>> {
    let mut manifests = MANIFESTS.lock().unwrap_or_else(PoisonError::into_inner);
    manifests.retain(|_, binding| binding.strong_count() > 0);
    let file = if let Some(existing) = manifests.get(&target.path).and_then(Weak::upgrade) {
        // An atomic update may be replacing the referent. Inspect the open
        // file and its name under the same lock the publisher holds.
        let bound = existing.lock().unwrap_or_else(PoisonError::into_inner);
        let file = target.open()?;
        if same_file_identity(
            &bound.metadata().map_err(raised)?,
            &file.metadata().map_err(raised)?,
        ) {
            drop(bound);
            return Ok(existing);
        }
        file
    } else {
        target.open()?
    };
    let binding = Arc::new(Mutex::new(file));
    manifests.insert(target.path.clone(), Arc::downgrade(&binding));
    Ok(binding)
}

fn write_staged(file: &mut fs::File, before: &fs::Metadata, content: &[u8]) -> PyResult<()> {
    file.write_all(content).map_err(raised)?;
    file.set_permissions(before.permissions()).map_err(raised)?;
    file.sync_all().map_err(raised)
}
