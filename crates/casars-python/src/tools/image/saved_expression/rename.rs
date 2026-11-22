// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::Metadata;
use std::io::ErrorKind;
use std::path::Path;

use casa_images::image_expr::ImageExprValue;
use casa_tables::publication::check_write_access;
use casa_types::RecordValue;
use pyo3::prelude::*;

use super::{SavedExpressionDirectory, expression_storage};
use crate::tools::image::attachment::{
    AttachedImage, conflicting_images, containing_tables, lock_table_ancestors, lock_table_tree,
    lock_table_tree_for_read_only_move, registry,
};
use crate::tools::image::handle::{ImageHandle, Storage, raised};
use crate::tools::image::native_rename::{
    check_path_relationship, move_read_only_directory, native_destination,
};

impl SavedExpressionDirectory {
    pub(in crate::tools::image) fn rename(
        &self,
        original: &AttachedImage,
        name: &str,
        expanded: &str,
        overwrite: bool,
        history: bool,
    ) -> PyResult<AttachedImage> {
        let old_name = original
            .lock()
            .path()
            .ok_or_else(|| raised("Saved image expression has no name"))?
            .to_path_buf();
        let same_name =
            || raised("Exception: Specified output name is the same as the current image name.");
        if OsStr::new(name) == old_name.as_os_str() {
            return Err(same_name());
        }
        let source = &self.path;
        let destination = native_destination(expanded, name)?;
        let target_snapshot = match destination.symlink_metadata() {
            Ok(metadata) if overwrite => Some(metadata),
            Ok(_) => {
                return Err(raised(format!(
                    "Exception: File {name} exists, and the user does not want to remove it.."
                )));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(raised(error)),
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
                return Err(raised(
                    "Cannot rename a saved image expression over or away from a captured operand",
                ));
            }
        }

        let images = registry();
        self.check_name(&old_name)?;
        self.persisted_misc(source)?;
        if conflicting_images(&images, source, 0)
            || conflicting_images(&images, &destination, 0)
            || containing_tables(source).any(crate::tools::table::path_is_in_use)
            || containing_tables(&destination).any(crate::tools::table::path_is_in_use)
        {
            return Err(raised(
                "Cannot rename saved image expression while a table in either path is open",
            ));
        }
        if target_snapshot.is_some() && check_write_access(&destination).is_err() {
            return Err(raised(format!(
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
        let moved = move_read_only_directory(
            source,
            &self.identity,
            &destination,
            target_snapshot.as_ref(),
            |staged| self.persisted_misc(staged).map(drop),
        )?;
        let reopen = || {
            moved.check_target(&destination)?;
            let misc = self.persisted_misc(&destination)?;
            let image = reopen_captured(&original.lock(), &destination, misc)?;
            let directory =
                Self::capture(&destination, destination.clone(), self.expression.clone())?;
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
            Ok::<_, PyErr>(attachment)
        };
        let attachment = match reopen() {
            Ok(attachment) => attachment,
            Err(error) => return moved.restore(source, &destination, error),
        };
        moved.finish(&destination)?;
        Ok(attachment)
    }

    fn persisted_misc(&self, path: &Path) -> PyResult<Option<RecordValue>> {
        let manifest = casa_images::expr_file::read_tool_manifest(path).map_err(raised)?;
        if manifest.expression != self.expression {
            return Err(raised("Saved image expression manifest has changed"));
        }
        manifest.misc_info().map_err(raised)
    }
}

fn reopen_captured(
    original: &ImageHandle,
    destination: &Path,
    misc: Option<RecordValue>,
) -> PyResult<ImageHandle> {
    match original {
        ImageHandle::Float(storage) => {
            reopened_storage(storage, destination, misc).map(ImageHandle::Float)
        }
        ImageHandle::Complex(storage) => {
            reopened_storage(storage, destination, misc).map(ImageHandle::Complex)
        }
        ImageHandle::Double(_) | ImageHandle::DComplex(_) => Err(raised(
            "Exception: rename does not support images with double precision pixel values.",
        )),
    }
}

fn reopened_storage<T: ImageExprValue>(
    storage: &Storage<T>,
    destination: &Path,
    misc: Option<RecordValue>,
) -> PyResult<Storage<T>> {
    let Storage::Temp { image, .. } = storage else {
        return Err(raised(
            "A saved image expression must have a captured graph",
        ));
    };
    let mut expression = image
        .saved_expression()
        .ok_or_else(|| raised("Saved image expression has no captured graph"))?
        .clone();
    if let Some(misc) = misc {
        expression = expression.with_misc_info(misc);
    }
    expression_storage(expression, destination)
}
