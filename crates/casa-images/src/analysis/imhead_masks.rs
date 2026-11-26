// SPDX-License-Identifier: LGPL-3.0-or-later
//! Removal of named mask metadata and its image-owned backing tables.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_core::{RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions, TableWriteLock, add_directory};

use crate::ImageError;
use crate::mask_reference::is_paged_mask_table;

pub(super) fn remove(image: &Path, name: &str) -> Result<(), ImageError> {
    let image = image.canonicalize().map_err(image_io)?;
    let mut lock = TableWriteLock::acquire_to_reread(&image, 1)?;
    let options = TableOptions::new(&image);
    let mut table = Table::open_metadata_only(options.clone())?;
    let Some(Value::Record(masks)) = table.keywords().get("masks") else {
        return if name.is_empty() {
            Ok(())
        } else {
            Err(missing_mask(name))
        };
    };
    if !name.is_empty() && masks.get(name).is_none() {
        return Err(missing_mask(name));
    }
    if masks.is_empty() {
        return Ok(());
    }
    let mut remaining = RecordValue::default();
    let mut obsolete = BTreeSet::new();
    let mut retained = BTreeSet::new();
    for field in masks.fields() {
        if name.is_empty() || field.name == name {
            obsolete.extend(backing_table(&image, &field.value));
        } else {
            retained.extend(backing_table(&image, &field.value));
            remaining.upsert(&field.name, field.value.clone());
        }
    }
    let mut mask_locks = Vec::new();
    for path in obsolete.difference(&retained) {
        if !is_paged_mask_table(path) {
            continue;
        }
        let mask_lock = TableWriteLock::acquire_to_reread(path, 1)?;
        if mask_lock.is_multi_used() {
            return Err(ImageError::Io(format!(
                "mask table {} is in use",
                path.display()
            )));
        }
        mask_locks.push((path, mask_lock));
    }
    let remove_default = matches!(table.keywords().get("Image_defaultmask"),
        Some(Value::Scalar(ScalarValue::String(default))) if name.is_empty() || default == name);
    let keywords = table.keywords_mut();
    keywords.upsert("masks", Value::Record(remaining));
    if remove_default {
        keywords.remove("Image_defaultmask");
    }

    // Publish the new region list before deleting any backing table. If the
    // metadata write fails, the old list still has all its backing storage.
    lock.record_write();
    table.save_metadata_only(options)?;
    for (path, _mask_lock) in mask_locks {
        std::fs::remove_dir_all(path).map_err(image_io)?;
    }
    lock.release()?;
    Ok(())
}

fn backing_table(image: &Path, value: &Value) -> Option<PathBuf> {
    let Value::Record(region) = value else {
        return None;
    };
    let reference = match region.get("mask")? {
        Value::TableRef(path) | Value::Scalar(ScalarValue::String(path)) => path,
        _ => return None,
    };
    let path = add_directory(reference, image);
    let resolved = path.canonicalize().ok()?;
    // Casacore permits references elsewhere, but only a real subtable
    // contained in this image is owned by the image being edited.
    (resolved.parent() == Some(image) && resolved.is_dir()).then_some(resolved)
}

fn missing_mask(name: &str) -> ImageError {
    ImageError::InvalidMetadata(format!("No mask named {name} found."))
}

fn image_io(error: std::io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use casa_coordinates::{CoordinateSystem, LinearCoordinate};
    use casa_core::ArrayValue;
    use ndarray::{ArrayD, IxDyn};

    use super::*;
    use crate::{PagedImage, image::image_history_entries};

    #[test]
    fn mask_removal_preserves_history_and_shared_backing_tables() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("aliases.im");
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(LinearCoordinate::new(
            2,
            vec!["X".into(), "Y".into()],
            vec!["pix".into(), "pix".into()],
        ));
        let mut image = PagedImage::<f32>::create(vec![2, 2], coordinates, &path).unwrap();
        let pixels = ArrayD::from_elem(IxDyn(&[2, 2]), true);
        image.put_mask("storage", &pixels).unwrap();
        image.put_mask("earlier", &pixels).unwrap();
        image.add_history("Preserve this image history").unwrap();
        image.save().unwrap();
        drop(image);

        let options = TableOptions::new(&path);
        let mut table = Table::open_metadata_only(options.clone()).unwrap();
        let Some(Value::Record(masks)) = table.keywords_mut().get_mut("masks") else {
            panic!("masks keyword")
        };
        masks.upsert("alias", masks.get("storage").unwrap().clone());
        masks.upsert("logtable", Value::Array(ArrayValue::Bool(pixels)));
        table.save_metadata_only(options).unwrap();
        drop(table);

        let locked = TableWriteLock::acquire(path.join("storage"), 1).unwrap();
        assert!(remove(&path, "").is_err());
        assert!(path.join("earlier").is_dir() && path.join("storage").is_dir());
        let image = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(
            image.mask_names(),
            ["storage", "earlier", "alias", "logtable"]
        );
        drop(image);
        locked.release().unwrap();

        remove(&path, "logtable").unwrap();
        remove(&path, "storage").unwrap();
        let image = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(image.mask_names(), ["earlier", "alias"]);
        assert!(
            image
                .get_named_mask("alias")
                .unwrap()
                .iter()
                .all(|value| *value)
        );
        drop(image);
        assert!(
            image_history_entries(&path)
                .unwrap()
                .iter()
                .any(|entry| entry.message == "Preserve this image history")
        );

        remove(&path, "").unwrap();
        assert!(!path.join("storage").exists() && !path.join("earlier").exists());
    }
}
