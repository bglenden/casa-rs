// SPDX-License-Identifier: LGPL-3.0-or-later

//! Stateful image-tool lifecycle (`open`, `close`, `done`, `name`, `rename`, `removefile`).

use std::path::{Path, PathBuf};

use casa_images::ImagePixelType;

use super::attachment::{
    AttachedImage, ExternalPathCheck, remove_file as remove_registered_file, remove_image_path,
};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels};

fn save_if_writable(attached: &AttachedImage, image: &mut ImageHandle) -> ImageToolResult<()> {
    if attached.is_read_only() || image.read_only_file_type().is_some() {
        return Ok(());
    }
    let writable = with_pixels!(&*image, storage => match storage {
        Storage::Paged(paged) => paged.is_writable(),
        Storage::Temp { .. } => true,
    });
    if writable {
        image.save()?;
    }
    Ok(())
}

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

    /// Returns whether an image is currently attached (`isopen`).
    pub fn is_open(&self) -> bool {
        self.image.is_some()
    }

    /// Returns whether the attached image is backed by persistent disk storage (`ispersistent`).
    pub fn is_persistent(&self) -> ImageToolResult<bool> {
        self.image
            .as_ref()
            .map_or(Ok(false), AttachedImage::is_persistent)
    }

    /// Returns the image pixel shape as `i64` vector (`shape`).
    pub fn shape_i64(&self) -> Vec<i64> {
        self.image.as_ref().map_or_else(Vec::new, |image| {
            image
                .lock()
                .shape()
                .iter()
                .map(|&size| size as i64)
                .collect()
        })
    }

    /// Returns CASA's pixel type name (`"float"`, `"double"`, `"complex"`, `"dcomplex"`, or `""`).
    pub fn pixel_type_casa(&self) -> &'static str {
        match self.image.as_ref().map(|image| image.lock().kind()) {
            Some(ImagePixelType::Float32) => "float",
            Some(ImagePixelType::Float64) => "double",
            Some(ImagePixelType::Complex32) => "complex",
            Some(ImagePixelType::Complex64) => "dcomplex",
            None => "",
        }
    }

    /// Returns CASA's attached image display path or temporary label (`name(strippath)`).
    pub fn casa_name(&self, strippath: bool) -> ImageToolResult<String> {
        let Some(attached) = self.image.as_ref() else {
            return Ok("none".into());
        };
        let image = attached.lock();
        image.check_single_precision("name")?;
        let Some(path) = attached.name_path(&image) else {
            let label = match image.image_type_name() {
                "ImageExpr" => "",
                "ComponentListImage" => "Temporary ComponentListImage",
                _ => "Temporary_Image",
            };
            return Ok(label.into());
        };
        if strippath {
            Ok(path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned())
        } else {
            let path = std::path::absolute(path)?;
            Ok(path.to_string_lossy().into_owned())
        }
    }

    /// Opens an image given pre-resolved `expanded` path string and display `infile` (`open`).
    pub fn open_expanded(
        &mut self,
        infile: &str,
        expanded: &str,
        cache: bool,
    ) -> ImageToolResult<()> {
        if infile.is_empty() {
            return Err(ImageToolError::new("Exception: File name is empty."));
        }
        let path = Path::new(expanded);
        if !path.exists() {
            return Err(ImageToolError::new(format!(
                "Exception: File {infile} does not exist.."
            )));
        }
        self.image = None;
        let path = std::path::absolute(path)?;
        let lexical = casa_tables::lexical_absolute(&path);
        let attached =
            AttachedImage::open_resolved_with_cache(&lexical, &path, Path::new(infile), cache)?;
        self.image = Some(attached);
        Ok(())
    }

    /// Detaches without flushing or erroring (`casac.image.close`).
    pub fn detach(&mut self) -> Option<AttachedImage> {
        self.image.take()
    }

    /// Detaches and optionally removes the attached image, returning the removed name (`casac.image.done`).
    pub fn done_casa(
        &mut self,
        remove: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Option<String>> {
        let attached = self.image.take();
        if remove && let Some(attached) = attached {
            attached.remove_registered(is_external_open).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Detaches and removes the attached image, returning `Ok(None)` if already detached (`casac.image.remove`).
    pub fn remove_casa(
        &mut self,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Option<String>> {
        self.image
            .take()
            .map(|attached| attached.remove_registered(is_external_open))
            .transpose()
    }

    /// Renames the currently attached single-precision image in place (`casac.image.rename`).
    pub fn rename_casa<E>(
        &mut self,
        name: &str,
        expanded: Result<String, E>,
        overwrite: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<bool>
    where
        E: Into<ImageToolError>,
    {
        let Some(current) = self.image.as_ref() else {
            return Ok(false);
        };
        current.lock().check_single_precision("rename")?;
        let current = self.image.take().expect("an attached image");
        let renamed = current.rename_with(
            name,
            || expanded.map_err(Into::into),
            overwrite,
            self.history,
            is_external_open,
        )?;
        self.image = Some(renamed);
        Ok(true)
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
            save_if_writable(&attached, &mut image)?;
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
                save_if_writable(&attached, &mut image)?;
                let _ = image.unlock_storage();
            }
            if verbose {
                logs.push(ToolLogLine::info("Image tool closed."));
            }
        }
        Ok((true, logs))
    }

    /// Removes the currently attached image (`remove`) via `OpenImages`-checked deletion.
    pub fn remove(
        &mut self,
        done: bool,
        _force: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Vec<ToolLogLine>> {
        // Both CASA forms detach the tool before attempting deletion so a removed path stays detached.
        let _ = done;
        let Some(attached) = self.image.take() else {
            return Err(ImageToolError::new("No image attached to remove."));
        };
        let removed = attached.remove_registered(is_external_open)?;
        Ok(vec![ToolLogLine::info(format!("deleted table {removed}"))])
    }

    /// Removes an arbitrary image path from disk via `OpenImages`-checked table deletion.
    pub fn remove_path(
        file: &Path,
        force: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Vec<ToolLogLine>> {
        if file.is_dir() {
            if remove_registered_file(file, is_external_open)? {
                return Ok(vec![ToolLogLine::info(format!(
                    "Removed {}",
                    file.display()
                ))]);
            }
            return Err(ImageToolError::new(format!(
                "{} does not exist or is not a table.",
                file.display()
            )));
        }
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
