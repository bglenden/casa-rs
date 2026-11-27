// SPDX-License-Identifier: LGPL-3.0-or-later

//! Atomic rename and rollback for saved LEL image expression directories (`.imgexpr`).

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::Metadata;
use std::io::ErrorKind;
use std::path::Path;

use casa_core::RecordValue;
use casa_images::ImageExprValue;
use casa_tables::publication::check_write_access;

use super::super::attachment::{AttachedImage, ExternalPathCheck, conflicting_images, registry};
use super::super::error::{ImageToolError, ImageToolResult};
use super::super::handle::{ImageHandle, Storage};
use super::super::native_rename::{
    NativeTableMove, check_path_relationship, move_read_only_directory, native_destination,
};
use super::super::table_locks::{
    containing_tables, lock_table_ancestors, lock_table_tree, lock_table_tree_for_read_only_move,
};
use super::{SavedExpressionDirectory, expression_storage};

impl SavedExpressionDirectory {
    /// Atomically renames this saved expression directory to `name` (`expanded`), checking operands and locks.
    pub fn rename(
        &self,
        original: &AttachedImage,
        name: &str,
        expanded: &str,
        overwrite: bool,
        history: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<AttachedImage> {
        let old_name = original
            .lock()
            .path()
            .ok_or_else(|| ImageToolError::new("Saved image expression has no name"))?
            .to_path_buf();
        let same_name = || {
            ImageToolError::new(
                "Exception: Specified output name is the same as the current image name.",
            )
        };
        if OsStr::new(name) == old_name.as_os_str() {
            return Err(same_name());
        }
        let source = &self.path;
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
        let target_key = destination
            .canonicalize()
            .unwrap_or_else(|_| destination.clone());
        if &target_key == source {
            return Err(same_name());
        }
        check_path_relationship(source, &destination, name)?;
        for operand in original.operand_paths() {
            if operand.starts_with(source)
                || operand.starts_with(&old_name)
                || operand.starts_with(&destination)
                || destination.starts_with(operand)
                || operand.starts_with(&target_key)
                || target_key.starts_with(operand)
            {
                return Err(ImageToolError::new(
                    "Cannot rename a saved image expression over or away from a captured operand",
                ));
            }
        }

        let images = registry();
        self.check_name(&old_name)?;
        let bound = self.lock_manifest();
        self.read_bound_manifest(&bound)?.misc_info()?;
        let replace_alias = target_snapshot.as_ref().is_some_and(Metadata::is_symlink);
        if conflicting_images(&images, source, 0)
            || conflicting_images(&images, &destination, 0)
            || containing_tables(source).any(is_external_open)
            || containing_tables(&destination).any(is_external_open)
            || is_external_open(source)
            || (!replace_alias && is_external_open(&destination))
        {
            return Err(ImageToolError::new(
                "Cannot rename saved image expression while a table in either path is open",
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
        self.check_name(&old_name)?;
        self.read_bound_manifest(&bound)?;
        let before = bound.metadata()?;
        let moved = move_read_only_directory(
            source,
            &self.identity,
            &destination,
            target_snapshot.as_ref(),
            |staged| self.check_staged_manifest(staged),
        )?;
        let copied = matches!(&moved, NativeTableMove::Copied { .. });
        drop(bound);
        let reopen = || -> ImageToolResult<AttachedImage> {
            moved.check_target(&destination)?;
            let (directory, manifest) = Self::open(&destination, destination.clone())?;
            self.check_relocated_manifest(&directory, &before, copied)?;
            if manifest.expression != self.expression {
                return Err(ImageToolError::new(
                    "Saved image expression manifest has changed",
                ));
            }
            let misc = manifest.misc_info()?;
            let image = reopen_captured(&original.lock(), &destination, misc)?;
            let attachment = AttachedImage::with_saved_expression(image, directory);
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

fn reopen_captured(
    original: &ImageHandle,
    destination: &Path,
    misc: Option<RecordValue>,
) -> ImageToolResult<ImageHandle> {
    match original {
        ImageHandle::Float(storage) => {
            reopened_storage(storage, destination, misc).map(ImageHandle::Float)
        }
        ImageHandle::Complex(storage) => {
            reopened_storage(storage, destination, misc).map(ImageHandle::Complex)
        }
        ImageHandle::Double(_) | ImageHandle::DComplex(_) => Err(ImageToolError::new(
            "Exception: rename does not support images with double precision pixel values.",
        )),
    }
}

fn reopened_storage<T: ImageExprValue>(
    storage: &Storage<T>,
    destination: &Path,
    misc: Option<RecordValue>,
) -> ImageToolResult<Storage<T>> {
    let Storage::Temp { image, .. } = storage else {
        return Err(ImageToolError::new(
            "A saved image expression must have a captured graph",
        ));
    };
    let expression = image
        .saved_expression()
        .ok_or_else(|| ImageToolError::new("Saved image expression has no captured graph"))?
        .clone();
    expression_storage(expression, destination, misc)
}
