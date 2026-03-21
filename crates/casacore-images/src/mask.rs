// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pixel mask support for images.
//!
//! Masks are boolean arrays stored as keywords in the image table. Each
//! mask has a name; one may be designated the "default" mask.
//!
//! In C++ casacore, masks are stored as sub-tables (using `LCPagedMask`).
//! This Rust implementation stores masks as boolean array values in
//! keyword records for simplicity and portability. The on-disk format
//! is a sub-record under the `"masks"` table keyword.

use casacore_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use crate::error::ImageError;

/// Name of the table keyword holding all masks.
pub(crate) const MASKS_KEYWORD: &str = "masks";
/// Name of the keyword holding the default mask name.
pub(crate) const DEFAULT_MASK_KEYWORD: &str = "default_mask";

/// Retrieves the masks sub-record from a keyword record, or returns a default.
pub(crate) fn get_masks_record(keywords: &RecordValue) -> RecordValue {
    match keywords.get(MASKS_KEYWORD) {
        Some(Value::Record(rec)) => rec.clone(),
        _ => RecordValue::default(),
    }
}

/// Reads a named boolean mask from the masks record.
pub(crate) fn read_mask(
    masks_rec: &RecordValue,
    name: &str,
    expected_shape: &[usize],
) -> Result<ArrayD<bool>, ImageError> {
    match masks_rec.get(name) {
        Some(Value::Array(ArrayValue::Bool(arr))) => {
            if arr.shape() != expected_shape {
                return Err(ImageError::ShapeMismatch {
                    expected: expected_shape.to_vec(),
                    got: arr.shape().to_vec(),
                });
            }
            Ok(arr.clone())
        }
        Some(_) => Err(ImageError::InvalidMetadata(format!(
            "mask '{name}' is not a Bool array"
        ))),
        None => Err(ImageError::MaskNotFound(name.to_string())),
    }
}

/// Returns the default mask name, if any.
pub(crate) fn default_mask_name(keywords: &RecordValue) -> Option<String> {
    match keywords.get(DEFAULT_MASK_KEYWORD) {
        Some(Value::Scalar(ScalarValue::String(s))) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::IxDyn;

    #[test]
    fn get_masks_record_empty() {
        let kw = RecordValue::default();
        let masks = get_masks_record(&kw);
        assert!(masks.fields().is_empty());
    }

    #[test]
    fn read_mask_missing() {
        let masks = RecordValue::default();
        assert!(matches!(
            read_mask(&masks, "foo", &[4, 4]),
            Err(ImageError::MaskNotFound(_))
        ));
    }

    #[test]
    fn read_mask_found() {
        let mut masks = RecordValue::default();
        let data = ArrayD::from_elem(IxDyn(&[4, 4]), true);
        masks.upsert("test", Value::Array(ArrayValue::Bool(data.clone())));
        let result = read_mask(&masks, "test", &[4, 4]).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn get_masks_record_existing_and_default_mask_empty_string() {
        let mut nested = RecordValue::default();
        nested.upsert(
            "plane",
            Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[1, 1]), true))),
        );
        let mut kw = RecordValue::default();
        kw.upsert(MASKS_KEYWORD, Value::Record(nested.clone()));
        kw.upsert(
            DEFAULT_MASK_KEYWORD,
            Value::Scalar(ScalarValue::String(String::new())),
        );
        assert_eq!(get_masks_record(&kw), nested);
        assert!(default_mask_name(&kw).is_none());
    }

    #[test]
    fn read_mask_rejects_wrong_type_and_shape() {
        let mut masks = RecordValue::default();
        masks.upsert(
            "wrong_type",
            Value::Scalar(ScalarValue::String("not a mask".into())),
        );
        assert!(matches!(
            read_mask(&masks, "wrong_type", &[2, 2]),
            Err(ImageError::InvalidMetadata(_))
        ));

        masks.upsert(
            "wrong_shape",
            Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[2, 3]), false))),
        );
        assert!(matches!(
            read_mask(&masks, "wrong_shape", &[3, 2]),
            Err(ImageError::ShapeMismatch { expected, got })
                if expected == vec![3, 2] && got == vec![2, 3]
        ));
    }

    #[test]
    fn default_mask_name_none() {
        let kw = RecordValue::default();
        assert!(default_mask_name(&kw).is_none());
    }

    #[test]
    fn default_mask_name_set() {
        let mut kw = RecordValue::default();
        kw.upsert(
            DEFAULT_MASK_KEYWORD,
            Value::Scalar(ScalarValue::String("mymask".into())),
        );
        assert_eq!(default_mask_name(&kw).as_deref(), Some("mymask"));
    }
}
