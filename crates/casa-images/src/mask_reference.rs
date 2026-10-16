// SPDX-License-Identifier: LGPL-3.0-or-later
//! How an image's mask regions name their mask tables.
//!
//! Each mask of a persistent image is an `LCPagedMask` region record in the
//! image's `masks` keyword. Its `mask` field is a table keyword naming the
//! mask table, a subtable of the image named after the mask. casacore's
//! `PagedImage` stores it relative to the image (`././mask0`, see
//! [`casa_tables::strip_directory`]), and so does casa-rs, so the image can be
//! moved or copied as a directory.
//!
//! casa-rs builds before #715 stored the image's absolute path at the time of
//! writing, or `<image name>/<mask>`. [`heal_legacy_mask_references`] points
//! such a region back at the image's own mask table when the image is opened.

use std::path::Path;

use casa_tables::add_directory;
use casa_types::{RecordValue, ScalarValue, Value};

/// The field of a mask region record that names its mask table.
const MASK_TABLE_FIELD: &str = "mask";

/// The stored reference to the subtable `name` inside the image: casacore's
/// form for a table inside its owner, as `PagedImage` writes its `logtable`
/// and mask tables.
pub(crate) fn subtable_reference(name: &str) -> String {
    format!("././{name}")
}

/// Points every mask region of `masks` at the mask table of the same name
/// inside the image. Returns whether any reference changed.
///
/// Used before an image is written to a new path, where its masks are written
/// as subtables named after them.
pub(crate) fn point_mask_references_inside(masks: &mut RecordValue) -> bool {
    rewrite_mask_references(masks, |_, _| true)
}

/// Points mask regions that an older casa-rs build wrote at the image's own
/// mask tables. Returns whether any reference changed.
///
/// A region named `<name>` is rewritten to `././<name>` when the image at
/// `image_path` has a `<name>` subtable and the stored reference
///
/// - resolves to that subtable (an older form, in place);
/// - resolves to no table (the image was moved or renamed); or
/// - is absolute and ends in `<name>` (a copy whose reference still names the
///   original image's mask table).
///
/// casa-rs only ever wrote mask tables as subtables named after their regions,
/// so these all mean the image's own table. Other references, such as a
/// casacore region whose mask table is another existing table, are kept.
pub(crate) fn heal_legacy_mask_references(masks: &mut RecordValue, image_path: &Path) -> bool {
    rewrite_mask_references(masks, |name, stored| {
        let own = image_path.join(name);
        if !own.is_dir() {
            return false;
        }
        let resolved = add_directory(stored, image_path);
        let names_own_table = Path::new(stored).is_absolute()
            && resolved.file_name().is_some_and(|file| file == name);
        resolved == own || !resolved.exists() || names_own_table
    })
}

/// Rewrites the `mask` reference of each region `<name>` to `././<name>`
/// where `rewrite(name, stored)` holds and it is not already in that form.
fn rewrite_mask_references(
    masks: &mut RecordValue,
    mut rewrite: impl FnMut(&str, &str) -> bool,
) -> bool {
    let mut changed = false;
    for field in masks.fields_mut() {
        let Value::Record(region) = &mut field.value else {
            continue;
        };
        let stored = match region.get(MASK_TABLE_FIELD) {
            Some(Value::TableRef(stored)) | Some(Value::Scalar(ScalarValue::String(stored))) => {
                stored.clone()
            }
            _ => continue,
        };
        let reference = Value::TableRef(subtable_reference(&field.name));
        if region.get(MASK_TABLE_FIELD) == Some(&reference) || !rewrite(&field.name, &stored) {
            continue;
        }
        region.upsert(MASK_TABLE_FIELD, reference);
        changed = true;
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::RecordField;
    use std::path::PathBuf;

    fn masks(regions: &[(&str, &str)]) -> RecordValue {
        RecordValue::new(
            regions
                .iter()
                .map(|(name, stored)| {
                    RecordField::new(
                        *name,
                        Value::Record(RecordValue::new(vec![RecordField::new(
                            MASK_TABLE_FIELD,
                            Value::table_ref(*stored),
                        )])),
                    )
                })
                .collect(),
        )
    }

    fn stored(masks: &RecordValue, name: &str) -> String {
        let Some(Value::Record(region)) = masks.get(name) else {
            panic!("no region {name}");
        };
        region
            .get(MASK_TABLE_FIELD)
            .and_then(Value::as_table_ref)
            .unwrap()
            .to_string()
    }

    /// An image directory `<root>/<image>` with empty subtable directories.
    fn image_with_subtables(root: &Path, image: &str, subtables: &[&str]) -> PathBuf {
        let path = root.join(image);
        for subtable in subtables {
            std::fs::create_dir_all(path.join(subtable)).unwrap();
        }
        path
    }

    #[test]
    fn older_forms_that_name_the_images_own_table_are_healed() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("a.image");
        let image = image_with_subtables(dir.path(), "b.image", &["moved", "named", "inplace"]);
        image_with_subtables(dir.path(), "a.image", &["copied"]);
        let mut masks = masks(&[
            // Renamed from a.image: the absolute path no longer exists.
            ("moved", &original.join("moved").to_string_lossy()),
            // The older `<image name>/<mask>` form, after the rename.
            ("named", "a.image/named"),
            // The older `<image name>/<mask>` form, in place.
            ("inplace", "b.image/inplace"),
            // Copied from a.image, which still exists.
            ("copied", &original.join("copied").to_string_lossy()),
        ]);
        std::fs::create_dir_all(image.join("copied")).unwrap();

        assert!(heal_legacy_mask_references(&mut masks, &image));
        for name in ["moved", "named", "inplace", "copied"] {
            assert_eq!(stored(&masks, name), format!("././{name}"), "{name}");
        }
    }

    #[test]
    fn references_to_other_existing_tables_or_missing_subtables_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let image = image_with_subtables(dir.path(), "img.image", &["own", "shared", "external"]);
        let other = image_with_subtables(dir.path(), "other.image", &["flags"]);
        let external = other.join("flags").to_string_lossy().into_owned();
        let mut masks = masks(&[
            ("own", "././own"),
            // A casacore region using another image's existing mask table.
            ("shared", "./other.image/flags"),
            // An absolute reference to an existing table of another name.
            ("external", &external),
            // No subtable of that name: nothing to heal to.
            ("missing", "/gone/img.image/missing"),
        ]);

        assert!(!heal_legacy_mask_references(&mut masks, &image));
        assert_eq!(stored(&masks, "shared"), "./other.image/flags");
        assert_eq!(stored(&masks, "external"), external);
        assert_eq!(stored(&masks, "missing"), "/gone/img.image/missing");
    }

    #[test]
    fn a_string_reference_becomes_a_table_reference() {
        let dir = tempfile::tempdir().unwrap();
        let image = image_with_subtables(dir.path(), "img.image", &["mask0"]);
        let mut masks = RecordValue::new(vec![RecordField::new(
            "mask0",
            Value::Record(RecordValue::new(vec![RecordField::new(
                MASK_TABLE_FIELD,
                Value::Scalar(ScalarValue::String("img.image/mask0".into())),
            )])),
        )]);
        assert!(heal_legacy_mask_references(&mut masks, &image));
        assert_eq!(stored(&masks, "mask0"), "././mask0");
    }

    #[test]
    fn a_temporary_image_saved_with_a_mask_keeps_it_after_a_move() {
        use crate::{PagedImage, TempImage, TempStoragePolicy};
        use casa_coordinates::CoordinateSystem;
        use ndarray::{ArrayD, IxDyn};

        let dir = tempfile::tempdir().unwrap();
        let mask = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, true]).unwrap();
        let mut temp = TempImage::<f32>::new(
            vec![2, 2],
            CoordinateSystem::new(),
            TempStoragePolicy::Memory,
        )
        .unwrap();
        temp.put_mask("mask0", &mask).unwrap();
        temp.set_default_mask("mask0").unwrap();
        drop(temp.save_as(dir.path().join("temp.image")).unwrap());

        let moved = dir.path().join("moved").join("temp.image");
        std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
        std::fs::rename(dir.path().join("temp.image"), &moved).unwrap();
        let reopened = PagedImage::<f32>::open(&moved).unwrap();
        assert_eq!(reopened.get_mask().unwrap(), Some(mask));
    }

    #[test]
    fn writing_to_a_new_path_points_every_region_inside() {
        let mut masks = masks(&[("a", "/old/x.image/a"), ("b", "././b")]);
        assert!(point_mask_references_inside(&mut masks));
        assert_eq!(stored(&masks, "a"), "././a");
        assert_eq!(stored(&masks, "b"), "././b");
        assert!(!point_mask_references_inside(&mut masks));
    }
}
