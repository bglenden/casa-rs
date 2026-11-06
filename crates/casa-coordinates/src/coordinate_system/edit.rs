// SPDX-License-Identifier: LGPL-3.0-or-later
//! Edits of coordinate records that preserve their original axis maps.

use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use super::CoordinateSystem;
use crate::{CoordinateError, CoordinateType};

impl CoordinateSystem {
    /// Return the coordinate type owning a zero-based pixel axis.
    #[must_use]
    pub fn pixel_axis_type(&self, pixel_axis: usize) -> Option<CoordinateType> {
        let mut offset = 0;
        for index in 0..self.n_coordinates() {
            let coordinate = self.coordinate(index);
            let next = offset + coordinate.n_pixel_axes();
            if (offset..next).contains(&pixel_axis) {
                return Some(coordinate.coordinate_type());
            }
            offset = next;
        }
        None
    }

    /// Update one zero-based pixel axis's reference pixel in a stored record.
    ///
    /// Returns the updated coordinate system and the previous reference pixel.
    /// The original record's coordinate order, axis maps, and unrelated fields
    /// are preserved. The record changes only if the resulting system parses
    /// successfully. As in casacore, a polarization-axis update is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis, non-finite value, malformed
    /// record, or unsupported axis layout.
    pub fn set_reference_pixel_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        if !value.is_finite() {
            return Err(CoordinateError::InvalidRecord(
                "reference pixel must be finite".into(),
            ));
        }
        let coordinates = Self::record_coordinates(record)?;
        let naxes = coordinates
            .iter()
            .flat_map(|coordinate| coordinate.pixel_axes.iter().flatten())
            .max()
            .map_or(0, |axis| axis + 1);
        let (coordinate, local_axis) = coordinates
            .iter()
            .find_map(|coordinate| {
                coordinate
                    .pixel_axes
                    .iter()
                    .position(|axis| *axis == Some(pixel_axis))
                    .map(|axis| (coordinate, axis))
            })
            .ok_or(CoordinateError::AxisOutOfRange {
                index: pixel_axis,
                naxes,
            })?;
        if coordinate.field.starts_with("stokes") {
            return Ok((Self::from_record(record)?, 0.0));
        }
        let mut updated = record.clone();
        let Some(Value::Record(coordinate_record)) = updated.get_mut(&coordinate.field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing coordinate record".into(),
            ));
        };
        let nested = if coordinate.field.starts_with("spectral") {
            if coordinate_record.get("wcs").is_some() {
                Some("wcs")
            } else if coordinate_record.get("tabular").is_some() {
                Some("tabular")
            } else {
                None
            }
        } else {
            None
        };
        let target = if let Some(name) = nested {
            match coordinate_record.get_mut(name) {
                Some(Value::Record(record)) => record,
                _ => {
                    return Err(CoordinateError::InvalidRecord(format!(
                        "invalid {name} coordinate record"
                    )));
                }
            }
        } else {
            coordinate_record
        };
        let old = match target.get_mut("crpix") {
            Some(Value::Scalar(ScalarValue::Float64(old))) if local_axis == 0 => {
                std::mem::replace(old, value)
            }
            Some(Value::Array(ArrayValue::Float64(values))) if values.ndim() == 1 => {
                let old = values.iter_mut().nth(local_axis).ok_or_else(|| {
                    CoordinateError::InvalidRecord("reference pixel is missing an axis".into())
                })?;
                std::mem::replace(old, value)
            }
            _ => {
                return Err(CoordinateError::InvalidRecord(
                    "invalid reference pixel record".into(),
                ));
            }
        };
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, old))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LinearCoordinate, StokesCoordinate, StokesType};

    #[test]
    fn reference_pixel_edits_follow_transposed_maps_and_are_atomic() {
        let mut original = CoordinateSystem::new();
        original.add_coordinate(LinearCoordinate::new(
            1,
            vec!["x".into()],
            vec!["pix".into()],
        ));
        original.add_coordinate(LinearCoordinate::new(
            1,
            vec!["y".into()],
            vec!["pix".into()],
        ));
        let mut record = original.to_record();
        for (key, axis) in [
            ("pixelmap0", 1),
            ("worldmap0", 1),
            ("pixelmap1", 0),
            ("worldmap1", 0),
        ] {
            record.upsert(key, Value::Array(ArrayValue::from_i32_vec(vec![axis])));
        }
        let maps = record.clone();
        let (updated, old) =
            CoordinateSystem::set_reference_pixel_in_record(&mut record, 0, 7.5).unwrap();
        assert_eq!(old, 0.0);
        assert_eq!(updated.coordinate(0).reference_pixel(), vec![7.5]);
        assert_eq!(updated.coordinate(1).reference_pixel(), vec![0.0]);
        for key in ["pixelmap0", "pixelmap1", "worldmap0", "worldmap1"] {
            assert_eq!(record.get(key), maps.get(key));
        }
        let before = record.clone();
        assert!(CoordinateSystem::set_reference_pixel_in_record(&mut record, 2, 1.0).is_err());
        assert_eq!(record, before);
        let mut stokes = CoordinateSystem::new();
        stokes.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        let mut record = stokes.to_record();
        let before = record.clone();
        let (unchanged, old) =
            CoordinateSystem::set_reference_pixel_in_record(&mut record, 0, 1.0).unwrap();
        assert_eq!(old, 0.0);
        assert_eq!(unchanged.pixel_axis_type(0), Some(CoordinateType::Stokes));
        assert_eq!(record, before);
    }
}
