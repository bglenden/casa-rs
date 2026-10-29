// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where the image tasks find an image's axes, and moving the reference
//! pixels of a coordinate-system record with a subimage.
//!
//! casa-rs reads a CASA image whose coordinates own transposed axes with
//! the coordinates in axis order (#1001), so its coordinate numbers can
//! differ from the record's: a record's coordinates are found by the fields
//! they were read from.

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::{ImageError, ImageInterface, ImagePixel, PagedImage};

/// Refuses `image` unless its direction coordinate, if it has one, owns
/// pixel axes 0 and 1, where `task` takes the direction plane.
///
/// A CASA image can hold its direction on other axes (`imtrans`); casa-rs
/// reads its coordinates so (#1001), but these tasks do not yet look for
/// the direction there (#1002).
pub(super) fn require_direction_on_first_axes<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    task: &str,
) -> Result<(), ImageError> {
    match image.find_axis(CoordinateType::Direction) {
        None | Some(0) => Ok(()),
        Some(_) => Err(ImageError::InvalidMetadata(format!(
            "{task} takes the direction plane from the image's first two axes, but its axes \
             are {}; casa-rs does not yet take it from other axes (#1002). In CASA, transpose \
             the image (imtrans) to put the direction first",
            image.axis_names().join(", ")
        ))),
    }
}

/// `coordinates` for a subimage starting at pixel `start`.
pub(super) fn subimage_coordinates(
    coordinates: &CoordinateSystem,
    start: &[usize],
) -> Result<CoordinateSystem, ImageError> {
    let mut record = coordinates.to_record();
    shift_record_reference_pixels(&mut record, coordinates, start)?;
    CoordinateSystem::from_record(&record).map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "failed to build subimage coordinate system: {error}"
        ))
    })
}

/// `image`'s own `coords` record for a subimage starting at pixel `start`,
/// which keeps what CASA wrote there; `None` without one.
pub(super) fn legacy_subimage_coordinates<T: ImagePixel>(
    image: &PagedImage<T>,
    start: &[usize],
) -> Option<RecordValue> {
    let Some(Value::Record(mut coords)) = image.table().keywords().get("coords").cloned() else {
        return None;
    };
    // The image's coordinates were read from this record, so its fields
    // resolve; if not, the output keeps the coordinates casa-rs writes.
    shift_record_reference_pixels(&mut coords, image.coordinates(), start).ok()?;
    Some(coords)
}

/// Moves the reference pixel of each coordinate in `record`, which holds
/// `coordinates`, back by `start` along the coordinate's pixel axes.
///
/// `record` may number its coordinates unlike `coordinates` (a transposed
/// casacore image keeps its coordinates in creation order), so each is
/// found by the field it was read from.
fn shift_record_reference_pixels(
    record: &mut RecordValue,
    coordinates: &CoordinateSystem,
    start: &[usize],
) -> Result<(), ImageError> {
    let fields = CoordinateSystem::record_coordinate_fields(record).map_err(|error| {
        ImageError::InvalidMetadata(format!("subimage coordinate system: {error}"))
    })?;
    let mut axis_offset = 0usize;
    for (coordinate_index, field) in fields.iter().enumerate() {
        let axis_count = coordinates.coordinate(coordinate_index).n_pixel_axes();
        let axes = &start[axis_offset..axis_offset + axis_count];
        if let Some(Value::Record(coordinate_record)) = record.get_mut(field) {
            shift_coordinate_reference_pixel(coordinate_record, axes);
            shift_legacy_spectral_wcs_reference_pixel(coordinate_record, axes);
        }
        axis_offset += axis_count;
    }
    Ok(())
}

fn shift_coordinate_reference_pixel(record: &mut RecordValue, start: &[usize]) {
    let Some(Value::Array(array)) = record.get_mut("crpix") else {
        return;
    };
    let values = match array {
        ArrayValue::Float64(values) => values
            .iter()
            .enumerate()
            .map(|(axis, value)| *value - start.get(axis).copied().unwrap_or(0) as f64)
            .collect::<Vec<_>>(),
        ArrayValue::Float32(values) => values
            .iter()
            .enumerate()
            .map(|(axis, value)| *value as f64 - start.get(axis).copied().unwrap_or(0) as f64)
            .collect::<Vec<_>>(),
        _ => return,
    };
    *array = ArrayValue::from_f64_vec(values);
}

fn shift_legacy_spectral_wcs_reference_pixel(record: &mut RecordValue, start: &[usize]) {
    let Some(Value::Record(wcs)) = record.get_mut("wcs") else {
        return;
    };
    let Some(Value::Scalar(ScalarValue::Float64(crpix))) = wcs.get_mut("crpix") else {
        return;
    };
    *crpix -= start.first().copied().unwrap_or(0) as f64;
}
