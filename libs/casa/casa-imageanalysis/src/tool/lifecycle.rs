// SPDX-License-Identifier: LGPL-3.0-or-later

//! Stateful image-tool lifecycle (`open`, `close`, `done`, `name`, `rename`, `removefile`).

use std::path::{Path, PathBuf};

use super::attachment::{AttachedImage, ExternalPathCheck, remove_image_path};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::ImageHandle;

/// Pure-Rust stateful image tool matching CASA `casac.image` session semantics.
#[derive(Default)]
pub struct ImageToolState {
    /// Currently attached image, if open.
    pub image: Option<AttachedImage>,
    /// Whether `ia` methods should append history rows to persistent outputs.
    pub history: bool,
}

impl ImageToolState {
    /// Creates a detached `ImageToolState` with history recording enabled.
    pub fn new() -> Self {
        Self {
            image: None,
            history: true,
        }
    }

    /// Creates an `ImageToolState` attached to `attached`.
    pub fn attached(attached: AttachedImage) -> Self {
        Self {
            image: Some(attached),
            history: true,
        }
    }

    /// Creates an `ImageToolState` directly from an [`ImageHandle`].
    pub fn from_handle(handle: ImageHandle) -> Self {
        Self::attached(AttachedImage::from_handle(handle))
    }

    /// Returns whether an image is currently attached (`isopen`).
    pub fn is_open(&self) -> bool {
        self.image.is_some()
    }

    /// Returns the attached image or CASA's standard error when detached.
    pub fn attached_image(&self) -> ImageToolResult<&AttachedImage> {
        self.image
            .as_ref()
            .ok_or_else(|| ImageToolError::new("No image is attached to this image tool."))
    }

    /// Opens an image from `infile` and attaches it (`open`).
    pub fn open(&mut self, infile: &Path, cache: bool) -> ImageToolResult<()> {
        self.image = None;
        let path = std::path::absolute(infile)?;
        let lexical = casa_tables::lexical_absolute(&path);
        let attached = AttachedImage::open_resolved_with_cache(&lexical, &path, infile, cache)?;
        self.image = Some(attached);
        self.history = true;
        Ok(())
    }

    /// Detaches and flushes the attached image (`close`).
    pub fn close(&mut self) -> ImageToolResult<bool> {
        if let Some(attached) = self.image.take() {
            let mut image = attached.lock();
            image.save()?;
            let _ = image.unlock_storage();
        }
        Ok(true)
    }

    /// Releases the attached image and optionally deletes persistent files (`done`).
    pub fn done(
        &mut self,
        remove: bool,
        verbose: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<(bool, Vec<ToolLogLine>)> {
        let mut logs = Vec::new();
        if let Some(attached) = self.image.take() {
            if remove {
                let removed = attached.remove_registered(is_external_open)?;
                logs.push(ToolLogLine::info(format!("deleted table {removed}")));
            } else {
                let mut image = attached.lock();
                image.save()?;
                let _ = image.unlock_storage();
            }
            if verbose {
                logs.push(ToolLogLine::info("Image tool closed."));
            }
        }
        Ok((true, logs))
    }

    /// Removes the specified file or currently attached image (`removefile` / `remove`).
    pub fn remove(
        &mut self,
        done: bool,
        force: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Vec<ToolLogLine>> {
        let Some(attached) = self.image.as_ref() else {
            return Err(ImageToolError::new("No image attached to remove."));
        };
        let logs = attached.remove_disk_file(force, is_external_open)?;
        if done {
            self.image = None;
        }
        Ok(logs)
    }

    /// Removes an arbitrary image path from disk without requiring an attachment.
    pub fn remove_path(
        file: &Path,
        force: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Vec<ToolLogLine>> {
        if !force && is_external_open(file) {
            return Err(ImageToolError::new(format!(
                "Cannot remove {}: file is currently open in a table tool.",
                file.display()
            )));
        }
        remove_image_path(file)?;
        Ok(vec![ToolLogLine::info(format!(
            "Removed {}",
            file.display()
        ))])
    }

    /// Returns the image name/path (`name`), optionally stripped of directory prefix (`strippath`).
    pub fn name(&self, strippath: bool) -> ImageToolResult<String> {
        let attached = self.attached_image()?;
        let image = attached.lock();
        let raw = image
            .path()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|| "(temp)image".to_owned());
        if strippath {
            Ok(Path::new(&raw)
                .file_name()
                .map_or_else(|| raw.clone(), |name| name.to_string_lossy().into_owned()))
        } else {
            Ok(raw)
        }
    }

    /// Renames the attached image on disk (`rename`).
    pub fn rename(
        &mut self,
        name: &str,
        overwrite: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<(PathBuf, Vec<ToolLogLine>)> {
        let Some(attached) = self.image.take() else {
            return Err(ImageToolError::new("No image attached to rename."));
        };
        let old_path = attached.lock().path().map(Path::to_path_buf);
        let renamed = attached.rename_with(
            name,
            || Ok(name.to_owned()),
            overwrite,
            self.history,
            is_external_open,
        )?;
        let target = renamed
            .lock()
            .path()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(name));
        self.image = Some(renamed);
        let from = old_path
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "temporary image".to_owned());
        Ok((
            target.clone(),
            vec![ToolLogLine::info(format!(
                "Renamed image {from} to {}",
                target.display()
            ))],
        ))
    }
}
