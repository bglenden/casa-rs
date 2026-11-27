// SPDX-License-Identifier: LGPL-3.0-or-later

//! Atomic rename and cross-device rollback for FITS file image attachments.

use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use casa_tables::lexical_absolute;
use casa_tables::publication::{check_write_access, rename_without_replacing};

use super::attachment::{
    AttachedImage, ExternalPathCheck, conflicting_images, in_use_by_expression, key, registry,
};
use super::error::{ImageToolError, ImageToolResult};
use super::handle::same_file_snapshot;
use super::native_rename::rename_destination;

/// Namespace wrapper for FITS image file rename operations.
pub struct FitsDirectory;

impl FitsDirectory {
    /// Renames a FITS file image from `old_name` to `name` (`expanded`), reopening the attachment.
    pub fn rename(
        old_name: &Path,
        name: &str,
        expanded: &str,
        overwrite: bool,
        history: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<AttachedImage> {
        rename(
            old_name,
            name,
            expanded,
            overwrite,
            history,
            is_external_open,
        )
    }
}

/// Renames a FITS file image from `old_name` to `name` (`expanded`), reopening the attachment.
pub fn rename(
    old_name: &Path,
    name: &str,
    expanded: &str,
    overwrite: bool,
    history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    if old_name.as_os_str() == OsStr::new(name) {
        return Err(ImageToolError::new(
            "Exception: Specified output name is the same as the current image name.",
        ));
    }
    let source = old_name.canonicalize().map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            ImageToolError::new("Exception: The image to be renamed no longer exists.")
        } else {
            error.into()
        }
    })?;
    if !source.is_file() {
        return Err(ImageToolError::new(format!(
            "Failed to rename file {} to {name}",
            old_name.display()
        )));
    }

    let requested = Path::new(expanded);
    let output = rename_destination(expanded, name)?;
    if !overwrite && output.symlink_metadata().is_ok() {
        return Err(ImageToolError::new(format!(
            "Exception: File {name} exists, and the user does not want to remove it.."
        )));
    }
    let destination = if output.is_dir() {
        output.join(
            source
                .file_name()
                .ok_or_else(|| ImageToolError::new("Current file name is empty, cannot rename"))?,
        )
    } else {
        output.clone()
    };
    let directory_label = if destination != output {
        requested
    } else {
        requested
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
    };
    let parent = destination
        .parent()
        .ok_or_else(|| ImageToolError::new("Invalid output image path"))?;
    if !parent.is_dir() || check_write_access(parent).is_err() {
        return Err(ImageToolError::new(format!(
            "RegularFile::copy/move: target directory {} is not writable",
            directory_label.display()
        )));
    }
    if destination.exists() && check_write_access(&destination).is_err() {
        return Err(ImageToolError::new(format!(
            "RegularFile::copy/move: target file {} already exists and is not writable",
            destination.display()
        )));
    }
    if is_external_open(&destination) {
        return Err(ImageToolError::new(format!(
            "Cannot rename image because {} is still open in this process",
            destination.display()
        )));
    }
    if destination.exists() {
        let dest_key = key(&destination)?;
        let registry = registry();
        if conflicting_images(&registry, &dest_key, 0)
            || in_use_by_expression(&registry, &dest_key)
            || is_external_open(&dest_key)
        {
            return Err(ImageToolError::new(format!(
                "Cannot rename image because {} is still open in this process",
                destination.display()
            )));
        }
    }
    let result = if overwrite {
        fs::rename(&source, &destination)
    } else {
        rename_without_replacing(&source, &destination)
    };
    match result {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::CrossesDevices => {
            move_across_devices(&source, &destination, overwrite)?;
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists && !overwrite => {
            return Err(ImageToolError::new(format!(
                "Exception: File {name} exists, and the user does not want to remove it.."
            )));
        }
        Err(error) => {
            return Err(ImageToolError::new(format!(
                "RegularFile::move error on {} to {}: {error}",
                source.display(),
                destination.display(),
            )));
        }
    }

    let attachment = AttachedImage::open(&lexical_absolute(&output))
        .map_err(|_| ImageToolError::new(format!("Exception: Unable to open image {name}.")))?;
    if history {
        attachment.lock().append_history_at(
            "::image::rename",
            &[
                "Ran ia.rename".to_owned(),
                format!("ia.rename(name={name:?}, overwrite={overwrite})"),
                format!("original name was {}", old_name.display()),
            ],
        )?;
    }
    Ok(attachment)
}

fn move_across_devices(source: &Path, destination: &Path, overwrite: bool) -> std::io::Result<()> {
    let parent = source
        .parent()
        .ok_or_else(|| std::io::Error::other("Invalid source image path"))?;
    let source_stage = tempfile::tempdir_in(parent)?;
    let claimed = source_stage.path().join("source");
    let expected = source.symlink_metadata()?;
    rename_without_replacing(source, &claimed)?;

    // Claim the source before publication. Publishing the copy is then the
    // last fallible move, so rollback never touches the public destination.
    let publish = || {
        let original = claimed.symlink_metadata()?;
        if !original.is_file() || !same_file_snapshot(&expected, &original) {
            return Err(std::io::Error::other(
                "The source FITS file changed before it could be moved",
            ));
        }
        let parent = destination
            .parent()
            .ok_or_else(|| std::io::Error::other("Invalid output image path"))?;
        let destination_stage = tempfile::tempdir_in(parent)?;
        let copy = destination_stage.path().join("image");
        fs::copy(&claimed, &copy)?;
        if !same_file_snapshot(&original, &claimed.symlink_metadata()?) {
            return Err(std::io::Error::other(
                "The source FITS file changed while it was being copied",
            ));
        }
        if overwrite {
            fs::rename(&copy, destination)
        } else {
            rename_without_replacing(&copy, destination)
        }
    };
    if let Err(error) = publish() {
        if let Err(restore) = rename_without_replacing(&claimed, source) {
            let backup = source_stage.keep().join("source");
            return Err(std::io::Error::other(format!(
                "Unable to publish the FITS file ({error}). Restoring the source failed ({restore}). The source is preserved at {}.",
                backup.display()
            )));
        }
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_failure_restores_the_claimed_source() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.fits");
        let destination = directory.path().join("existing-directory");
        fs::write(&source, b"source FITS bytes").unwrap();
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("marker"), b"existing destination").unwrap();

        assert!(move_across_devices(&source, &destination, true).is_err());
        assert_eq!(fs::read(&source).unwrap(), b"source FITS bytes");
        assert_eq!(
            fs::read(destination.join("marker")).unwrap(),
            b"existing destination"
        );
    }
}
