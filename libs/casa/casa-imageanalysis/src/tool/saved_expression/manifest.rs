// SPDX-License-Identifier: LGPL-3.0-or-later

//! Identity-bound manifest reading, atomic updates, and validation for saved LEL expressions.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use casa_core::{PrimitiveType, RecordValue};
use casa_images::ImagePixelType;
use casa_images::expr_file::{
    ToolExpressionManifest, encode_tool_manifest, is_image_expr, read_tool_manifest,
    read_tool_manifest_from_reader,
};

use super::super::error::{ImageToolError, ImageToolResult};
use super::super::handle::same_file_identity;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use super::super::handle::same_file_snapshot;

const MANIFEST: &str = "imageexpr.json";
type ManifestBinding = Mutex<fs::File>;
static MANIFESTS: Mutex<BTreeMap<PathBuf, Weak<ManifestBinding>>> = Mutex::new(BTreeMap::new());

/// Identity-bound handle to a saved LEL expression directory and its `imageexpr.json` manifest.
pub struct SavedExpressionDirectory {
    /// Canonical path to the expression directory.
    pub path: PathBuf,
    /// Metadata snapshot captured when the directory was opened.
    pub identity: fs::Metadata,
    /// Stored LEL expression text.
    pub expression: String,
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
    /// Opens `name` (resolving to `path`) and verifies the `imageexpr.json` manifest binding.
    pub fn open(name: &Path, path: PathBuf) -> ImageToolResult<(Self, ToolExpressionManifest)> {
        #[cfg(unix)]
        let directory = Arc::new(casa_tables::publication::open_directory_identity(&path)?);
        #[cfg(unix)]
        let identity = directory.metadata()?;
        #[cfg(not(unix))]
        let identity = path.metadata()?;
        check_name(name, &path, &identity)?;
        let entry = path.join(MANIFEST);
        let attributes = entry.symlink_metadata()?;
        let (target_path, link) = if attributes.is_symlink() {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            let handle = casa_tables::publication::open_symlink_directory_entry(
                &directory,
                OsStr::new(MANIFEST),
            )?;
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            if !same_file_identity(&attributes, &handle.metadata()?) {
                return Err(manifest_changed());
            }
            let contents = fs::read_link(&entry)?;
            let link = ManifestLink {
                contents,
                identity: attributes,
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                _handle: handle,
            };
            (entry.canonicalize()?, Some(link))
        } else {
            (entry.clone(), None)
        };
        let parent = target_path.parent().ok_or_else(manifest_changed)?;
        #[cfg(unix)]
        let target_parent = if parent == path {
            Arc::clone(&directory)
        } else {
            Arc::new(casa_tables::publication::open_directory_identity(parent)?)
        };
        #[cfg(unix)]
        let parent_identity = target_parent.metadata()?;
        #[cfg(not(unix))]
        let parent_identity = parent.symlink_metadata()?;
        let target = ManifestTarget {
            path: target_path,
            parent_identity,
            #[cfg(unix)]
            parent: target_parent,
        };
        let binding = bind_manifest(&target)?;
        let bound = binding.lock().unwrap_or_else(PoisonError::into_inner);
        let file = target.open()?;
        let attributes = file.metadata()?;
        if !same_file_identity(&bound.metadata()?, &attributes) {
            return Err(manifest_changed());
        }
        let manifest = read_tool_manifest_from_reader(&file)?;
        check_name(name, &path, &identity)?;
        target.check_location(link.as_ref(), &entry)?;
        let current = target.open()?.metadata()?;
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

    /// Checks that `name` on disk still resolves to this directory's bound identity.
    pub fn check_name(&self, name: &Path) -> ImageToolResult<()> {
        check_name(name, &self.path, &self.identity)
    }

    /// Returns `(symlink_entry, target_path)` if `imageexpr.json` is a symbolic link.
    pub fn linked_manifest_paths(&self) -> Option<(PathBuf, &Path)> {
        self.link
            .as_ref()
            .map(|_| (self.path.join(MANIFEST), self.target.path.as_path()))
    }

    /// Acquires the process-wide mutex protecting the bound manifest file.
    pub fn lock_manifest(&self) -> MutexGuard<'_, fs::File> {
        self.manifest.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open_bound_file(&self, bound: &fs::File) -> ImageToolResult<(fs::File, fs::Metadata)> {
        self.target
            .check_location(self.link.as_ref(), &self.path.join(MANIFEST))?;
        let current = self.target.open()?;
        let before = current.metadata()?;
        if !same_file_identity(&bound.metadata()?, &before) {
            return Err(manifest_changed());
        }
        Ok((current, before))
    }

    fn read_manifest(&self, file: impl std::io::Read) -> ImageToolResult<ToolExpressionManifest> {
        let manifest = read_tool_manifest_from_reader(file)?;
        if manifest.expression != self.expression {
            return Err(manifest_changed());
        }
        Ok(manifest)
    }

    /// Reads and validates the bound manifest under `bound`.
    pub fn read_bound_manifest(&self, bound: &fs::File) -> ImageToolResult<ToolExpressionManifest> {
        let (current, _) = self.open_bound_file(bound)?;
        self.read_manifest(current)
    }

    /// Verifies that a staged copy of the expression directory preserved the manifest intact.
    pub fn check_staged_manifest(&self, staged: &Path) -> ImageToolResult<()> {
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
                self.read_manifest(fs::File::open(staged.join(relative))?)?
            }
            None => {
                let manifest = read_tool_manifest(staged)?;
                if manifest.expression != self.expression {
                    return Err(manifest_changed());
                }
                manifest
            }
        };
        manifest.misc_info().map(drop).map_err(Into::into)
    }

    /// Verifies that the relocated directory preserves the manifest identity/links.
    pub fn check_relocated_manifest(
        &self,
        relocated: &Self,
        before: &fs::Metadata,
        copied: bool,
    ) -> ImageToolResult<()> {
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
            let current = relocated.lock_manifest().metadata()?;
            if !same_file_identity(before, &current) {
                return Err(manifest_changed());
            }
        }
        Ok(())
    }

    /// Atomically updates `miscinfo` in the bound manifest file.
    pub fn write_metadata(
        &self,
        name: &Path,
        pixel_type: ImagePixelType,
        record: &RecordValue,
    ) -> ImageToolResult<()> {
        let pixel_type = match pixel_type {
            ImagePixelType::Float32 => PrimitiveType::Float32,
            ImagePixelType::Float64 => PrimitiveType::Float64,
            ImagePixelType::Complex32 => PrimitiveType::Complex32,
            ImagePixelType::Complex64 => PrimitiveType::Complex64,
        };
        let content = encode_tool_manifest(&self.expression, pixel_type, record)?;
        let mut bound = self.lock_manifest();
        self.check_name(name)?;
        let (current, before) = self.open_bound_file(&bound)?;
        casa_tables::publication::check_write_access(&self.target.path)?;
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

    fn open(&self) -> ImageToolResult<fs::File> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return Ok(casa_tables::publication::open_regular_directory_entry(
            &self.parent,
            self.entry_name(),
        )?);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if !self.path.symlink_metadata()?.is_file() {
                return Err(ImageToolError::new(
                    "Saved image expression manifest must be a regular file",
                ));
            }
            Ok(fs::File::open(&self.path)?)
        }
    }

    fn check_location(&self, link: Option<&ManifestLink>, entry: &Path) -> ImageToolResult<()> {
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
        check_location: impl FnOnce() -> ImageToolResult<()>,
    ) -> ImageToolResult<fs::File> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let mut staged = casa_tables::publication::StagedDirectoryFile::new(
                &self.parent,
                self.parent_path(),
            )?;
            write_staged(staged.file_mut(), before, content)?;
            check_location()?;
            Ok(staged.replace(self.entry_name(), before)?)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let mut staged = tempfile::Builder::new()
                .prefix(".imageexpr-")
                .tempfile_in(self.parent_path())?;
            write_staged(staged.as_file_mut(), before, content)?;
            check_location()?;
            if !self
                .path
                .symlink_metadata()
                .is_ok_and(|current| same_file_snapshot(before, &current))
            {
                return Err(manifest_changed());
            }
            Ok(staged.persist(&self.path)?)
        }
    }
}

fn check_name(name: &Path, path: &Path, identity: &fs::Metadata) -> ImageToolResult<()> {
    if name.canonicalize().is_ok_and(|actual| actual == path)
        && path
            .symlink_metadata()
            .is_ok_and(|current| same_file_identity(identity, &current))
    {
        return Ok(());
    }
    Err(ImageToolError::new(format!(
        "Saved image expression at {} has changed",
        name.display()
    )))
}

fn manifest_changed() -> ImageToolError {
    ImageToolError::new("Saved image expression manifest has changed")
}

fn bind_manifest(target: &ManifestTarget) -> ImageToolResult<Arc<ManifestBinding>> {
    let mut manifests = MANIFESTS.lock().unwrap_or_else(PoisonError::into_inner);
    manifests.retain(|_, binding| binding.strong_count() > 0);
    let file = if let Some(existing) = manifests.get(&target.path).and_then(Weak::upgrade) {
        // An atomic update may be replacing the referent. Inspect the open
        // file and its name under the same lock the publisher holds.
        let bound = existing.lock().unwrap_or_else(PoisonError::into_inner);
        let file = target.open()?;
        if same_file_identity(&bound.metadata()?, &file.metadata()?) {
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

fn write_staged(file: &mut fs::File, before: &fs::Metadata, content: &[u8]) -> ImageToolResult<()> {
    file.write_all(content)?;
    file.set_permissions(before.permissions())?;
    Ok(file.sync_all()?)
}

/// Returns whether `path` is a saved LEL expression directory (`imageexpr.json`).
pub fn is_saved_expression_dir(path: &Path) -> bool {
    is_image_expr(path)
}

/// Reads a saved expression manifest from `directory/imageexpr.json`.
pub fn read_manifest(directory: &Path) -> ImageToolResult<ToolExpressionManifest> {
    Ok(read_tool_manifest(directory)?)
}

/// Reads a saved expression manifest from an open reader.
pub fn read_manifest_from_reader(
    reader: impl std::io::Read,
) -> ImageToolResult<ToolExpressionManifest> {
    Ok(read_tool_manifest_from_reader(reader)?)
}

/// Writes `imageexpr.json` atomically into `directory` via temp file persistence.
pub fn write_manifest(
    directory: &Path,
    expression: &str,
    pixel_type: PrimitiveType,
    misc: &RecordValue,
) -> ImageToolResult<()> {
    fs::create_dir_all(directory)?;
    let encoded = encode_tool_manifest(expression, pixel_type, misc)?;
    let manifest_path = directory.join(MANIFEST);
    let mut temp = tempfile::NamedTempFile::new_in(directory)?;
    temp.write_all(&encoded)?;
    temp.as_file().sync_all()?;
    temp.persist(&manifest_path)?;
    Ok(())
}

/// Saves `misc_info` back into an existing expression directory, preserving expression text.
pub fn update_manifest_misc_info(
    directory: &Path,
    pixel_type: PrimitiveType,
    misc: &RecordValue,
) -> ImageToolResult<()> {
    let manifest = read_tool_manifest(directory)?;
    write_manifest(directory, &manifest.expression, pixel_type, misc)
}

/// Opens the manifest file inside `directory` and parses it.
pub fn open_and_read_manifest(directory: &Path) -> ImageToolResult<ToolExpressionManifest> {
    let file = File::open(directory.join(MANIFEST)).map_err(|err| {
        ImageToolError::new(format!(
            "Cannot open expression manifest in '{}': {err}",
            directory.display()
        ))
    })?;
    read_manifest_from_reader(file)
}

/// Returns the manifest file path for a saved expression directory.
pub fn manifest_file_path(directory: &Path) -> PathBuf {
    directory.join(MANIFEST)
}
