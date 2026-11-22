// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_lattices::{Lattice, LatticeMut, PagedArray, TiledShape};
use casa_tables::publication::{lock_table_tree_for_replacement, rename_without_replacing};
use casa_tables::{Table, TableOptions, TableWriteLock, add_directory};
use casa_types::{RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::native::{ComponentTable, keyword_record};
use crate::ImageError;
use crate::image::make_paged_mask_record;
use crate::mask_reference::subtable_reference;

pub(super) struct ComponentMasks {
    entries: RecordValue,
    default: Option<String>,
}

impl ComponentMasks {
    pub(super) fn from_keywords(keywords: &RecordValue) -> Self {
        let default = match keywords.get("Image_defaultmask") {
            Some(Value::Scalar(ScalarValue::String(name))) if !name.is_empty() => {
                Some(name.clone())
            }
            _ => None,
        };
        Self {
            entries: keyword_record(keywords, "masks"),
            default,
        }
    }

    pub(super) fn names(&self) -> Vec<String> {
        self.entries
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    }

    pub(super) fn default_name(&self) -> Option<String> {
        self.default.clone()
    }

    pub(super) fn active(&self) -> Option<&str> {
        self.default
            .as_deref()
            .filter(|name| self.entries.get(name).is_some())
    }

    fn path(&self, root: &Path, name: &str) -> Result<PathBuf, ImageError> {
        let Some(Value::Record(region)) = self.entries.get(name) else {
            return Err(ImageError::MaskNotFound(name.into()));
        };
        let reference = match region.get("mask") {
            Some(Value::TableRef(reference) | Value::Scalar(ScalarValue::String(reference))) => {
                reference
            }
            _ => {
                return Err(ImageError::InvalidMetadata(
                    "mask record is missing table reference".into(),
                ));
            }
        };
        Ok(add_directory(reference, root))
    }

    fn local_path(&self, root: &Path, name: &str) -> Result<PathBuf, ImageError> {
        validate_name(name)?;
        let path = self.path(root, name)?;
        if path != root.join(name) || !path.symlink_metadata().is_ok_and(|m| m.is_dir()) {
            return Err(ImageError::InvalidMetadata(format!(
                "Mask {name} does not own a local mask table",
            )));
        }
        Ok(path)
    }

    fn save(&self, keywords: &mut RecordValue) {
        keywords.upsert("masks", Value::Record(self.entries.clone()));
        if let Some(name) = &self.default {
            keywords.upsert(
                "Image_defaultmask",
                Value::Scalar(ScalarValue::String(name.clone())),
            );
        } else {
            keywords.remove("Image_defaultmask");
        }
    }
}

impl ComponentTable {
    pub(super) fn open_mask(&self, name: &str) -> Result<PagedArray<bool>, ImageError> {
        let path = self.mask_state().path(&self.path, name)?;
        PagedArray::<bool>::open(path).map_err(Into::into)
    }

    pub(super) fn read_mask(
        &self,
        name: &str,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        self.open_mask(name)?
            .get_slice(start, shape, stride)
            .map_err(Into::into)
    }

    pub(crate) fn put_mask(
        &self,
        name: &str,
        image_shape: &[usize],
        data: &ArrayD<bool>,
    ) -> Result<(), ImageError> {
        validate_name(name)?;
        if data.shape() != image_shape {
            return Err(ImageError::ShapeMismatch {
                expected: image_shape.to_vec(),
                got: data.shape().to_vec(),
            });
        }
        let start = vec![0; image_shape.len()];
        self.edit_masks(|masks| {
            if masks.entries.get(name).is_some() {
                let path = masks.local_path(&self.path, name)?;
                let mut lock = TableWriteLock::acquire_to_reread(&path, 1)?;
                let mut target = PagedArray::<bool>::open(&path)?;
                target.put_slice(data, &start)?;
                target.flush()?;
                lock.record_write();
                lock.release()?;
                return Ok(MaskChange::Pixels);
            }
            self.add_mask(masks, name, image_shape, |target| {
                target.put_slice(data, &start).map_err(Into::into)
            })
        })
    }

    pub(crate) fn create_filled_mask(
        &self,
        name: &str,
        image_shape: &[usize],
        value: bool,
    ) -> Result<(), ImageError> {
        validate_name(name)?;
        self.edit_masks(|masks| {
            self.add_mask(masks, name, image_shape, |target| {
                target.set(value).map_err(Into::into)
            })
        })
    }

    fn add_mask(
        &self,
        masks: &mut ComponentMasks,
        name: &str,
        image_shape: &[usize],
        write: impl FnOnce(&mut PagedArray<bool>) -> Result<(), ImageError>,
    ) -> Result<MaskChange, ImageError> {
        if masks.entries.get(name).is_some() {
            return Err(ImageError::InvalidMetadata(format!(
                "Mask {name} already exists"
            )));
        }
        let stage = tempfile::tempdir_in(&self.path).map_err(io)?;
        let product = stage.path().join("mask");
        let tiled = TiledShape::new(image_shape.to_vec(), size_of::<bool>())?;
        let mut mask = PagedArray::<bool>::create(tiled, &product)?;
        write(&mut mask)?;
        mask.flush()?;
        drop(mask);
        let change =
            MaskChange::move_directory(product, self.path.join(name), Some(stage), Vec::new())?;
        masks.entries.upsert(
            name,
            Value::Record(make_paged_mask_record(
                &subtable_reference(name),
                image_shape,
            )),
        );
        Ok(change)
    }

    pub(crate) fn set_default_mask(
        &self,
        name: Option<&str>,
        image_shape: &[usize],
    ) -> Result<(), ImageError> {
        self.edit_masks(|masks| {
            if let Some(name) = name {
                let path = masks.path(&self.path, name)?;
                let mask = PagedArray::<bool>::open(path)?;
                validate_coverage(name, mask.shape(), image_shape)?;
            }
            masks.default = name.map(str::to_owned);
            Ok(MaskChange::Keywords)
        })
    }

    pub(crate) fn rename_mask(&self, from: &str, to: &str) -> Result<(), ImageError> {
        validate_name(to)?;
        self.edit_masks(|masks| {
            let source = masks.local_path(&self.path, from)?;
            if masks.entries.get(to).is_some() {
                return Err(ImageError::InvalidMetadata(format!(
                    "Mask {to} already exists"
                )));
            }
            let locks = lock_table_tree_for_replacement(&source)?;
            let change = MaskChange::move_directory(source, self.path.join(to), None, locks)?;
            masks.entries.rename_field(from, to);
            if let Some(Value::Record(region)) = masks.entries.get_mut(to) {
                region.upsert("mask", Value::TableRef(subtable_reference(to)));
            }
            if masks.default.as_deref() == Some(from) {
                masks.default = Some(to.into());
            }
            Ok(change)
        })
    }

    pub(crate) fn remove_mask(&self, name: &str) -> Result<(), ImageError> {
        self.edit_masks(|masks| {
            let source = masks.local_path(&self.path, name)?;
            let locks = lock_table_tree_for_replacement(&source)?;
            let stage = tempfile::tempdir_in(&self.path).map_err(io)?;
            let staged = stage.path().join("mask");
            let change = MaskChange::move_directory(source, staged, Some(stage), locks)?;
            masks.entries.remove(name);
            if masks.default.as_deref() == Some(name) {
                masks.default = None;
            }
            Ok(change)
        })
    }

    fn edit_masks(
        &self,
        edit: impl FnOnce(&mut ComponentMasks) -> Result<MaskChange, ImageError>,
    ) -> Result<(), ImageError> {
        let mut lock = TableWriteLock::acquire_to_reread(&self.path, 1)?;
        let options = TableOptions::new(&self.path);
        let mut table = Table::open_metadata_only(options.clone())?;
        let mut masks = ComponentMasks::from_keywords(table.keywords());
        let change = edit(&mut masks)?;
        if !matches!(change, MaskChange::Pixels) {
            masks.save(table.keywords_mut());
            if let Err(error) = table.save_metadata_only(options) {
                return Err(change.rollback(error.into()));
            }
            lock.record_write();
        }
        *self
            .masks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = masks;
        lock.release()?;
        Ok(())
    }
}

pub(super) fn validate_coverage(
    name: &str,
    mask_shape: &[usize],
    image_shape: &[usize],
) -> Result<(), ImageError> {
    if mask_shape.len() != image_shape.len()
        || mask_shape
            .iter()
            .zip(image_shape)
            .any(|(mask, image)| mask < image)
    {
        return Err(ImageError::CasaSelection(format!(
            "region {name} does not cover the full image",
        )));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), ImageError> {
    if Path::new(name).file_name().and_then(|name| name.to_str()) != Some(name) {
        return Err(ImageError::InvalidMetadata(format!(
            "Invalid mask name {name}"
        )));
    }
    Ok(())
}

enum MaskChange {
    Pixels,
    Keywords,
    Directory {
        from: PathBuf,
        to: PathBuf,
        stage: Option<tempfile::TempDir>,
        _locks: Vec<TableWriteLock>,
        #[cfg(unix)]
        identity: std::fs::File,
    },
}

impl MaskChange {
    fn move_directory(
        from: PathBuf,
        to: PathBuf,
        stage: Option<tempfile::TempDir>,
        locks: Vec<TableWriteLock>,
    ) -> Result<Self, ImageError> {
        #[cfg(unix)]
        let identity = casa_tables::publication::open_directory_identity(&from).map_err(io)?;
        rename_without_replacing(&from, &to).map_err(io)?;
        Ok(Self::Directory {
            from,
            to,
            stage,
            _locks: locks,
            #[cfg(unix)]
            identity,
        })
    }

    fn rollback(self, error: ImageError) -> ImageError {
        let Self::Directory {
            from,
            to,
            stage,
            _locks,
            #[cfg(unix)]
            identity,
        } = self
        else {
            return error;
        };
        let restore = || {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let original = identity.metadata()?;
                let current = to.symlink_metadata()?;
                if (original.dev(), original.ino()) != (current.dev(), current.ino()) {
                    return Err(std::io::Error::other("the moved mask directory changed"));
                }
            }
            rename_without_replacing(&to, &from)
        };
        if let Err(restore) = restore() {
            if let Some(stage) = stage {
                let _ = stage.keep();
            }
            return ImageError::Io(format!(
                "{error}. Could not restore mask table {} from {}: {restore}",
                from.display(),
                to.display()
            ));
        }
        error
    }
}

fn io(error: std::io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_mask_metadata_save_restores_the_original_directory() {
        let root = tempfile::tempdir().unwrap();
        let original = root.path().join("default");
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("data"), "mask").unwrap();
        let stage = tempfile::tempdir_in(root.path()).unwrap();
        let moved = stage.path().join("mask");
        let change =
            MaskChange::move_directory(original.clone(), moved.clone(), Some(stage), Vec::new())
                .unwrap();
        let error = change.rollback(ImageError::Io("metadata save failed".into()));
        assert!(error.to_string().contains("metadata save failed"));
        assert_eq!(
            std::fs::read_to_string(original.join("data")).unwrap(),
            "mask"
        );
        assert!(!moved.exists());
    }

    #[test]
    fn rollback_keeps_a_mask_when_its_original_name_is_occupied() {
        let root = tempfile::tempdir().unwrap();
        let original = root.path().join("default");
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("data"), "mask").unwrap();
        let stage = tempfile::tempdir_in(root.path()).unwrap();
        let moved = stage.path().join("mask");
        let change =
            MaskChange::move_directory(original.clone(), moved.clone(), Some(stage), Vec::new())
                .unwrap();
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("data"), "replacement").unwrap();
        let error = change.rollback(ImageError::Io("metadata save failed".into()));
        assert!(error.to_string().contains("Could not restore mask table"));
        assert_eq!(std::fs::read_to_string(moved.join("data")).unwrap(), "mask");
        assert_eq!(
            std::fs::read_to_string(original.join("data")).unwrap(),
            "replacement",
        );
    }
}
