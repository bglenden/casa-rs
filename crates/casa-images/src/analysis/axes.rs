// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where the image tasks find an image's axes, and moving the reference
//! pixels of a coordinate-system record with a subimage.
//!
//! casa-rs reads a CASA image whose coordinates own transposed axes with
//! the coordinates in axis order, and drops a linear coordinate's removed
//! axes (#1001), so its coordinates can differ from the record's: a
//! record's coordinates are found where the record places them.

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::{ImageError, ImageInterface, ImagePixel, PagedImage};

/// The pixel axes of `image`'s direction plane, (longitude, latitude):
/// its direction coordinate's wherever they are, or the first two axes of an
/// image without one.
///
/// A CASA image can hold its direction after other axes (`imtrans`); casa-rs
/// reads a coordinate's axes adjacent and in its own order (#1001), so the
/// latitude follows the longitude, and the world axes are numbered as the
/// pixel axes are (#1002).
///
/// - Regions take their plane here, as CASA does.
/// - `feather` does too, where CASA 6.7.6's `feather` fails on such an
///   image (`docs/CASA (C++) bugs.md`).
/// - `impv` takes it only on axes 0 and 1 until its output has a PV image's
///   coordinates (#1024).
pub(super) fn direction_plane_axes<T: ImagePixel>(image: &impl ImageInterface<T>) -> [usize; 2] {
    match image.find_axis(CoordinateType::Direction) {
        Some(longitude) => [longitude, longitude + 1],
        None => [0, 1],
    }
}

/// The axes of a `rank`-axis image other than its plane's, in order.
pub(super) fn other_axes(rank: usize, plane: [usize; 2]) -> Vec<usize> {
    (0..rank).filter(|axis| !plane.contains(axis)).collect()
}

/// `coordinates` for a subimage starting at pixel `start`.
pub(super) fn subimage_coordinates(
    coordinates: &CoordinateSystem,
    start: &[usize],
) -> Result<CoordinateSystem, ImageError> {
    let mut record = coordinates.to_record();
    shift_record_reference_pixels(&mut record, start)?;
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
    shift_record_reference_pixels(&mut coords, start).ok()?;
    Some(coords)
}

/// Moves each coordinate in `record` back by `start`, the subimage's first
/// pixel, along the coordinate's own axes.
///
/// Each coordinate is found as the record places it
/// ([`CoordinateSystem::record_coordinates`]): a transposed casacore image
/// numbers its coordinates in creation order, and a linear coordinate keeps
/// its removed axes in the record, where they do not move. A lookup table
/// (a tabular spectral coordinate's, or a tabular coordinate's) moves its
/// pixels as well as its reference pixel, since casacore and casa-rs
/// rebuild such a coordinate from the table. casacore's
/// `CoordinateSystem::subImageInSitu` instead rebuilds a tabular spectral
/// table over the subimage's channels, which gives the same world values.
fn shift_record_reference_pixels(
    record: &mut RecordValue,
    start: &[usize],
) -> Result<(), ImageError> {
    let placed = CoordinateSystem::record_coordinates(record).map_err(|error| {
        ImageError::InvalidMetadata(format!("subimage coordinate system: {error}"))
    })?;
    for coordinate in placed {
        let shifts = coordinate
            .pixel_axes
            .iter()
            .map(|axis| axis.map_or(0.0, |axis| start[axis] as f64))
            .collect::<Vec<_>>();
        let Some(Value::Record(coordinate_record)) = record.get_mut(&coordinate.field) else {
            continue;
        };
        shift_pixels(coordinate_record, &shifts);
        for parameters in ["wcs", "tabular"] {
            if let Some(Value::Record(inner)) = coordinate_record.get_mut(parameters) {
                shift_pixels(inner, &shifts);
            }
        }
    }
    Ok(())
}

/// Moves `record`'s `crpix` back by `shifts`, one per axis, and the
/// `pixelvalues` of its lookup table, which has one axis, by the first.
fn shift_pixels(record: &mut RecordValue, shifts: &[f64]) {
    let first = shifts.first().copied().unwrap_or(0.0);
    match record.get_mut("crpix") {
        Some(Value::Array(array)) => {
            shift_values(array, |axis| shifts.get(axis).copied().unwrap_or(0.0));
        }
        Some(Value::Scalar(ScalarValue::Float64(crpix))) => *crpix -= first,
        _ => {}
    }
    if let Some(Value::Array(array)) = record.get_mut("pixelvalues") {
        shift_values(array, |_| first);
    }
}

/// Subtracts `shift(i)` from element `i` of `array`, a float array.
fn shift_values(array: &mut ArrayValue, shift: impl Fn(usize) -> f64) {
    let values = match array {
        ArrayValue::Float64(values) => values
            .iter()
            .enumerate()
            .map(|(i, value)| *value - shift(i))
            .collect::<Vec<_>>(),
        ArrayValue::Float32(values) => values
            .iter()
            .enumerate()
            .map(|(i, value)| f64::from(*value) - shift(i))
            .collect::<Vec<_>>(),
        _ => return,
    };
    *array = ArrayValue::from_f64_vec(values);
}
