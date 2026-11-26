// SPDX-License-Identifier: LGPL-3.0-or-later
//! The casacore image concatenation manifest.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use casa_core::RecordValue;
use casa_tables::{add_directory, strip_directory};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::expr_file::tool_metadata;
use crate::{ImageError, ImagePixelType};

const FILE: &str = "imageconcat.json";

/// Whether the directory contains a virtual concatenation manifest.
pub fn is_image_concat(path: &Path) -> bool {
    path.join(FILE).is_file()
}

/// File metadata for an ordered set of source images.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ConcatManifest {
    #[serde(default = "first_version")]
    version: u32,
    data_type: String,
    axis: usize,
    #[serde(default = "default_temp_close")]
    temp_close: bool,
    images: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    misc_info: Option<Value>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl ConcatManifest {
    /// Read a version-one descriptor from a published image directory.
    ///
    /// # Errors
    /// Returns an error for unreadable, malformed or unsupported metadata.
    pub fn open(directory: &Path) -> Result<Self, ImageError> {
        Self::open_descriptor(&directory.join(FILE))
    }

    pub(crate) fn open_descriptor(path: &Path) -> Result<Self, ImageError> {
        let bytes = fs::read(path).map_err(|error| ImageError::Io(error.to_string()))?;
        let manifest: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ImageError::InvalidMetadata(format!("{}: {error}", path.display())))?;
        if manifest.version != 1 {
            return Err(ImageError::InvalidMetadata(format!(
                "Unsupported imageconcat.json version {}",
                manifest.version,
            )));
        }
        if manifest.images.iter().any(String::is_empty)
            || manifest
                .misc_info
                .as_ref()
                .is_some_and(|misc| !misc.is_object())
        {
            return Err(ImageError::InvalidMetadata(
                "Invalid Images or MiscInfo field in imageconcat.json".into(),
            ));
        }
        manifest.pixel_type()?;
        manifest.misc_info()?;
        Ok(manifest)
    }

    /// Prepare a descriptor for operands already in validated output order.
    /// Relative paths use the final manifest directory as their owner.
    ///
    /// # Errors
    /// Returns an error for metadata that cannot be represented in JSON.
    pub fn new(
        owner: &Path,
        kind: ImagePixelType,
        axis: usize,
        temp_close: bool,
        images: impl IntoIterator<Item = PathBuf>,
        misc: &RecordValue,
    ) -> Result<Self, ImageError> {
        let data_type = match kind {
            ImagePixelType::Float32 => "float",
            ImagePixelType::Float64 => "double",
            ImagePixelType::Complex32 => "Complex",
            ImagePixelType::Complex64 => "DComplex",
        };
        Ok(Self {
            version: 1,
            data_type: data_type.into(),
            axis,
            temp_close,
            images: images
                .into_iter()
                .map(|image| strip_directory(&image, owner))
                .collect(),
            misc_info: Some(tool_metadata::encode(misc)?),
            extra: Map::new(),
        })
    }

    /// Return the pixel type required from each operand.
    ///
    /// # Errors
    /// Returns an error for unsupported casacore types.
    pub fn pixel_type(&self) -> Result<ImagePixelType, ImageError> {
        match self.data_type.to_ascii_lowercase().as_str() {
            "float" => Ok(ImagePixelType::Float32),
            "double" => Ok(ImagePixelType::Float64),
            "complex" => Ok(ImagePixelType::Complex32),
            "dcomplex" => Ok(ImagePixelType::Complex64),
            other => Err(ImageError::InvalidMetadata(format!(
                "Unsupported imageconcat.json DataType {other}",
            ))),
        }
    }

    /// Pixel axis along which the source images are joined.
    pub fn axis(&self) -> usize {
        self.axis
    }

    /// Whether the operands should be closed between operations.
    pub fn temp_close(&self) -> bool {
        self.temp_close
    }

    /// Input paths in manifest order, resolved as casacore resolves them.
    pub fn paths(&self, owner: &Path) -> Vec<PathBuf> {
        self.images
            .iter()
            .map(|image| {
                let named_ancestor = image.strip_suffix("/.").is_some_and(|suffix| {
                    !suffix.is_empty() && Path::new(suffix).is_relative() && owner.ends_with(suffix)
                });
                if image.starts_with("./") || named_ancestor {
                    add_directory(image, owner)
                } else {
                    // Casacore leaves unmarked names relative to the working directory.
                    PathBuf::from(image)
                }
            })
            .collect()
    }

    /// Explicit miscellaneous image metadata, if the file supplies it.
    ///
    /// # Errors
    /// Returns an error for a malformed record.
    pub fn misc_info(&self) -> Result<Option<RecordValue>, ImageError> {
        self.misc_info
            .as_ref()
            .map(tool_metadata::decode)
            .transpose()
    }

    /// Serialize a new descriptor in an unpublished staging directory.
    ///
    /// # Errors
    /// Returns an error if the staged file cannot be written.
    pub fn write(&self, directory: &Path) -> Result<(), ImageError> {
        let bytes = self.to_bytes()?;
        fs::write(directory.join(FILE), bytes).map_err(io)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ImageError> {
        serde_json::to_vec_pretty(self)
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
    }
}

pub(crate) fn update_misc_info(directory: &Path, misc: &RecordValue) -> Result<(), ImageError> {
    let named = directory.join(FILE);
    let target = named.canonicalize().map_err(io)?;
    let parent = target.parent().expect("canonical manifest has a parent");
    let name = target
        .file_name()
        .expect("canonical manifest has a filename");
    casa_tables::publication::check_write_access(&target).map_err(io)?;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use casa_tables::publication::{
            StagedDirectoryFile, open_directory_identity, open_regular_directory_entry,
        };
        let dir = open_directory_identity(parent).map_err(io)?;
        let mut input = open_regular_directory_entry(&dir, name).map_err(io)?;
        let expected = input.metadata().map_err(io)?;
        let mut contents = Vec::new();
        input.read_to_end(&mut contents).map_err(io)?;
        let bytes = replace_misc_info(&contents, misc)?;
        let mut stage = StagedDirectoryFile::new(&dir, parent).map_err(io)?;
        stage.file_mut().write_all(&bytes).map_err(io)?;
        stage
            .file_mut()
            .set_permissions(expected.permissions())
            .map_err(io)?;
        stage.file_mut().sync_all().map_err(io)?;
        if named.canonicalize().map_err(io)? != target {
            return Err(ImageError::Io(
                "The concatenation manifest link changed".into(),
            ));
        }
        stage.replace(name, &expected).map_err(io)?;
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = name;
        let contents = fs::read(&target).map_err(io)?;
        let bytes = replace_misc_info(&contents, misc)?;
        let mut stage = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
        stage.write_all(&bytes).map_err(io)?;
        stage.persist(&target).map_err(|error| io(error.error))?;
        Ok(())
    }
}

fn replace_misc_info(contents: &[u8], misc: &RecordValue) -> Result<Vec<u8>, ImageError> {
    let mut manifest: ConcatManifest = serde_json::from_slice(contents)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    manifest.misc_info = Some(tool_metadata::encode(misc)?);
    manifest.to_bytes()
}

fn io(error: std::io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

fn default_temp_close() -> bool {
    true
}

fn first_version() -> u32 {
    1
}
