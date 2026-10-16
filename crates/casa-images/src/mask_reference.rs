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
//! writing. Such an image still reads in place, and a save in place stores the
//! relative form; once moved, it needs re-running (or, for CASA, a symlink at
//! the old path).

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
    let mut changed = false;
    for field in masks.fields_mut() {
        let Value::Record(region) = &mut field.value else {
            continue;
        };
        if !matches!(
            region.get(MASK_TABLE_FIELD),
            Some(Value::TableRef(_)) | Some(Value::Scalar(ScalarValue::String(_)))
        ) {
            continue;
        }
        let reference = Value::TableRef(subtable_reference(&field.name));
        if region.get(MASK_TABLE_FIELD) != Some(&reference) {
            region.upsert(MASK_TABLE_FIELD, reference);
            changed = true;
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::RecordField;

    fn region(stored: Value) -> Value {
        Value::Record(RecordValue::new(vec![RecordField::new(
            MASK_TABLE_FIELD,
            stored,
        )]))
    }

    fn stored(masks: &RecordValue, name: &str) -> Option<Value> {
        let Some(Value::Record(region)) = masks.get(name) else {
            panic!("no region {name}");
        };
        region.get(MASK_TABLE_FIELD).cloned()
    }

    #[test]
    fn writing_to_a_new_path_points_every_region_inside() {
        let mut masks = RecordValue::new(vec![
            RecordField::new("a", region(Value::table_ref("/old/x.image/a"))),
            RecordField::new("b", region(Value::table_ref("././b"))),
            RecordField::new(
                "c",
                region(Value::Scalar(ScalarValue::String("x.image/c".into()))),
            ),
        ]);
        assert!(point_mask_references_inside(&mut masks));
        for name in ["a", "b", "c"] {
            assert_eq!(
                stored(&masks, name),
                Some(Value::table_ref(format!("././{name}")))
            );
        }
        assert!(!point_mask_references_inside(&mut masks));
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
}
