// SPDX-License-Identifier: LGPL-3.0-or-later

//! Identity-checked directory handle and atomic rename for MIRIAD images.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, Metadata};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_tables::publication::check_write_access;

use super::attachment::{AttachedImage, ExternalPathCheck, conflicting_images, registry};
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, same_file_identity};
use super::native_rename::{check_path_relationship, move_read_only_directory, native_destination};
use super::table_locks::{
    containing_tables, lock_table_ancestors, lock_table_tree, lock_table_tree_for_read_only_move,
};

/// Bound MIRIAD image directory tracking its canonical path, inode identity, and directory fd.
pub struct MiriadDirectory {
    physical_path: PathBuf,
    identity: Metadata,
    #[cfg(unix)]
    _directory: fs::File,
}

impl MiriadDirectory {
    /// Opens and captures the physical path and inode identity of a MIRIAD directory.
    pub fn open(path: &Path) -> ImageToolResult<Self> {
        let physical_path = path.canonicalize()?;
        #[cfg(unix)]
        let directory = casa_tables::publication::open_directory_identity(&physical_path)?;
        #[cfg(unix)]
        let identity = directory.metadata()?;
        #[cfg(not(unix))]
        let identity = physical_path.metadata()?;
        Ok(Self {
            physical_path,
            identity,
            #[cfg(unix)]
            _directory: directory,
        })
    }

    /// Verifies that `name` still resolves to the captured MIRIAD directory identity.
    pub fn check(&self, name: &Path) -> ImageToolResult<()> {
        if name
            .canonicalize()
            .is_ok_and(|path| path == self.physical_path)
            && self.physical_path.symlink_metadata().is_ok_and(|current| {
                current.is_dir() && same_file_identity(&self.identity, &current)
            })
        {
            return Ok(());
        }
        Err(ImageToolError::new(format!(
            "Cannot rename image {} because the image at that path has changed",
            name.display()
        )))
    }

    /// Atomically renames this MIRIAD directory to `name` (`expanded`), reopening the attachment.
    pub fn rename(
        &self,
        old_name: &Path,
        name: &str,
        expanded: &str,
        overwrite: bool,
        history: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<AttachedImage> {
        let same_name = || {
            ImageToolError::new(
                "Exception: Specified output name is the same as the current image name.",
            )
        };
        if OsStr::new(name) == old_name.as_os_str() {
            return Err(same_name());
        }
        let source = &self.physical_path;
        let destination = native_destination(expanded, name)?;
        let target_snapshot = match destination.symlink_metadata() {
            Ok(metadata) if overwrite => Some(metadata),
            Ok(_) => {
                return Err(ImageToolError::new(format!(
                    "Exception: File {name} exists, and the user does not want to remove it.."
                )));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if &destination == source {
            return Err(same_name());
        }
        check_path_relationship(source, &destination, name)?;

        let images = registry();
        self.check(old_name)?;
        let replace_alias = target_snapshot
            .as_ref()
            .is_some_and(|entry| entry.is_symlink());
        if conflicting_images(&images, source, 0)
            || conflicting_images(&images, &destination, 0)
            || containing_tables(source).any(is_external_open)
            || containing_tables(&destination).any(is_external_open)
            || is_external_open(source)
            || (!replace_alias && is_external_open(&destination))
        {
            return Err(ImageToolError::new(
                "Cannot rename MIRIAD image while a table in either path is open",
            ));
        }
        if target_snapshot.is_some() && check_write_access(&destination).is_err() {
            return Err(ImageToolError::new(format!(
                "RegularFile::copy/move: target file {} already exists and is not writable",
                destination.display()
            )));
        }

        let mut leases = BTreeMap::new();
        lock_table_tree_for_read_only_move(source, &mut leases)?;
        if target_snapshot.as_ref().is_some_and(Metadata::is_dir) {
            lock_table_tree(&destination, &mut leases)?;
        } else {
            lock_table_ancestors(&destination, &mut leases)?;
        }
        self.check(old_name)?;
        let moved = move_read_only_directory(
            source,
            &self.identity,
            &destination,
            target_snapshot.as_ref(),
            |staged| {
                ImageHandle::open_miriad(staged)
                    .map(drop)
                    .map_err(Into::into)
            },
        )?;
        let reopen = || -> ImageToolResult<AttachedImage> {
            moved.check_target(&destination)?;
            let attachment = AttachedImage::open_miriad(&destination)?;
            moved.check_target(&destination)?;
            if history {
                attachment.lock().append_history_at(
                    "::image::rename",
                    &[
                        "Ran ia.rename".into(),
                        format!("ia.rename(name={name:?}, overwrite={overwrite})"),
                        format!("original name was {}", old_name.display()),
                    ],
                )?;
            }
            Ok(attachment)
        };
        let attachment = match reopen() {
            Ok(attachment) => attachment,
            Err(error) => return moved.restore(source, &destination, error),
        };
        moved.finish(&destination)?;
        Ok(attachment)
    }
}
