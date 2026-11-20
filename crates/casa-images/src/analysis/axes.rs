// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where the image tasks find an image's axes, and moving the reference
//! pixels of a coordinate-system record with a subimage.
//!
//! casa-rs reads a CASA image whose coordinates own transposed axes with
//! the coordinates in axis order, and drops a linear coordinate's removed
//! axes (#1001), so its coordinates can differ from the record's: a
//! record's coordinates are found where the record places them.

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, StokesCoordinate, StokesType,
};
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::{ImageError, ImageInterface, ImagePixel, PagedImage};

pub(super) struct StridedSection<'a> {
    pub(super) start: &'a [usize],
    /// Counts of selected pixels on each axis.
    pub(super) shape: &'a [usize],
    pub(super) stride: &'a [usize],
}

impl StridedSection<'_> {
    pub(super) fn cursors(&self, dense_cursor: Vec<usize>) -> TraversalCursorIter {
        // Reading a strided tile may fetch its dense enclosing box. Keep that
        // box bounded by the chosen source tile on every axis.
        let packed = dense_cursor
            .into_iter()
            .zip(self.stride)
            .map(|(size, &step)| size.div_ceil(step).max(1))
            .collect::<Vec<_>>();
        TraversalCursorIter::new(self.shape.to_vec(), packed, TraversalSpec::tiles())
    }

    pub(super) fn source_start(&self, position: &[usize]) -> Vec<usize> {
        position
            .iter()
            .zip(self.start)
            .zip(self.stride)
            .map(|((&offset, &start), &step)| start + offset * step)
            .collect()
    }

    pub(super) fn dense_shape(&self, shape: &[usize]) -> Vec<usize> {
        shape
            .iter()
            .zip(self.stride)
            .map(|(&count, &step)| (count - 1) * step + 1)
            .collect()
    }
}

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
pub(super) fn direction_plane_axes<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
) -> Result<[usize; 2], ImageError> {
    let coordinates = image.coordinates();
    let Some(index) = coordinates.find_coordinate(CoordinateType::Direction) else {
        return Ok([0, 1]);
    };
    if coordinates.coordinate(index).n_pixel_axes() != 2 {
        return Err(ImageError::InvalidMetadata(
            "selecting a direction plane requires both direction axes".into(),
        ));
    }
    let longitude = (0..index)
        .map(|i| coordinates.coordinate(i).n_pixel_axes())
        .sum();
    Ok([longitude, longitude + 1])
}

/// The axes of a `rank`-axis image other than its plane's, in order.
pub(super) fn other_axes(rank: usize, plane: [usize; 2]) -> Vec<usize> {
    (0..rank).filter(|axis| !plane.contains(axis)).collect()
}

/// `coordinates` for a subimage starting at pixel `start`.
pub(super) fn subimage_coordinates(
    coordinates: &CoordinateSystem,
    start: &[usize],
    shape: &[usize],
) -> Result<CoordinateSystem, ImageError> {
    let mut record = coordinates.to_record();
    shift_record_reference_pixels(&mut record, start, shape)?;
    CoordinateSystem::from_record(&record).map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "failed to build subimage coordinate system: {error}"
        ))
    })
}

/// `coordinates` for a subimage starting at pixel `start`, optionally replacing
/// the Stokes coordinate with `stokes`.
pub(super) fn subimage_coordinates_with_stokes(
    coordinates: &CoordinateSystem,
    start: &[usize],
    shape: &[usize],
    stokes: Option<&[StokesType]>,
) -> Result<CoordinateSystem, ImageError> {
    let mut coords = subimage_coordinates(coordinates, start, shape)?;
    if let Some(stokes_list) = stokes {
        replace_stokes_coordinate(&mut coords, stokes_list);
    }
    Ok(coords)
}

/// Stokes list owned by pixel `axis` of `coords`.
pub(super) fn stokes_for_axis(
    coords: &CoordinateSystem,
    axis: usize,
) -> Result<Vec<StokesType>, ImageError> {
    let mut offset = 0usize;
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        let next = offset + coord.n_pixel_axes();
        if (offset..next).contains(&axis)
            && let CoordinateModel::Stokes(stokes) = coord
        {
            return Ok(stokes.stokes().to_vec());
        }
        offset = next;
    }
    Err(ImageError::InvalidMetadata(
        "Stokes selection requires a Stokes coordinate".into(),
    ))
}

/// Replaces any Stokes coordinate in `coords` with `stokes`.
pub(super) fn replace_stokes_coordinate(coords: &mut CoordinateSystem, stokes: &[StokesType]) {
    if let Some(idx) = coords.find_coordinate(CoordinateType::Stokes) {
        coords.replace_coordinate(idx, StokesCoordinate::new(stokes.to_vec()));
    }
}

/// `image`'s own `coords` record for a subimage starting at pixel `start`,
/// optionally replacing its Stokes list.
pub(super) fn legacy_subimage_coordinates_with_stokes<T: ImagePixel>(
    image: &PagedImage<T>,
    start: &[usize],
    shape: &[usize],
    stokes: Option<&[StokesType]>,
) -> Option<RecordValue> {
    let Some(Value::Record(mut coords)) = image.table().keywords().get("coords").cloned() else {
        return None;
    };
    // The image's coordinates were read from this record, so its fields
    // resolve; if not, the output keeps the coordinates casa-rs writes.
    shift_record_reference_pixels(&mut coords, start, shape).ok()?;
    if let Some(stokes_list) = stokes {
        replace_stokes_in_record(&mut coords, stokes_list).ok()?;
    }
    Some(coords)
}

/// Updates the `stokes*` sub-record inside a stored `coords` record to hold `stokes`.
pub(super) fn replace_stokes_in_record(
    record: &mut RecordValue,
    stokes: &[StokesType],
) -> Result<(), ImageError> {
    let placed = CoordinateSystem::record_coordinates(record).map_err(|error| {
        ImageError::InvalidMetadata(format!("stokes record coordinates: {error}"))
    })?;
    for coordinate in placed {
        if !coordinate.field.starts_with("stokes") {
            continue;
        }
        let Some(Value::Record(stokes_rec)) = record.get_mut(&coordinate.field) else {
            continue;
        };
        let use_codes = matches!(
            stokes_rec.get("stokes"),
            Some(Value::Array(ArrayValue::Int32(_)))
        );
        if use_codes {
            stokes_rec.upsert(
                "stokes",
                Value::Array(ArrayValue::from_i32_vec(
                    stokes.iter().map(|s| s.code()).collect(),
                )),
            );
        } else {
            stokes_rec.upsert(
                "stokes",
                Value::Array(ArrayValue::from_string_vec(
                    stokes.iter().map(|s| s.name().to_string()).collect(),
                )),
            );
        }
        if let Some(&first) = stokes.first() {
            if matches!(stokes_rec.get("crval"), Some(Value::Array(_))) {
                stokes_rec.upsert(
                    "crval",
                    Value::Array(ArrayValue::from_f64_vec(vec![f64::from(first.code())])),
                );
            }
            if matches!(stokes_rec.get("crpix"), Some(Value::Array(_))) {
                stokes_rec.upsert("crpix", Value::Array(ArrayValue::from_f64_vec(vec![0.0])));
            }
            if let Some(suffix) = coordinate.field.strip_prefix("stokes") {
                let wr_key = format!("worldreplace{suffix}");
                if matches!(record.get(&wr_key), Some(Value::Array(_))) {
                    record.upsert(
                        wr_key,
                        Value::Array(ArrayValue::from_f64_vec(vec![f64::from(first.code())])),
                    );
                }
            }
        }
    }
    Ok(())
}

/// Moves each coordinate in `record` back by `start`, the subimage's first
/// pixel, along the coordinate's own axes.
///
/// Each coordinate is found as the record places it
/// ([`CoordinateSystem::record_coordinates`]): a transposed casacore image
/// numbers its coordinates in creation order, and a linear coordinate keeps
/// its removed axes in the record, where they do not move. A lookup table
/// (a tabular spectral coordinate's, or a tabular coordinate's) is adjusted
/// as well. Spectral tables are rebuilt over the selected channels, matching
/// casacore's `CoordinateSystem::subImageInSitu` for nonlinear frequencies.
pub(super) fn shift_record_reference_pixels(
    record: &mut RecordValue,
    start: &[usize],
    shape: &[usize],
) -> Result<(), ImageError> {
    section_record_reference_pixels(record, start, shape, None)
}

pub(super) fn section_record_reference_pixels(
    record: &mut RecordValue,
    start: &[usize],
    shape: &[usize],
    stride: Option<&[usize]>,
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
        let steps = stride.map(|stride| {
            coordinate
                .pixel_axes
                .iter()
                .map(|axis| axis.map_or(1.0, |axis| stride[axis] as f64))
                .collect::<Vec<_>>()
        });
        let steps = steps.as_deref().unwrap_or_default();
        let Some(Value::Record(coordinate_record)) = record.get_mut(&coordinate.field) else {
            continue;
        };
        if coordinate.field.starts_with("spectral")
            && let Some(Some(axis)) = coordinate.pixel_axes.first()
            && casa_coordinates::SpectralCoordinate::subimage_tabular_record_with_stride(
                coordinate_record,
                start[*axis],
                shape[*axis],
                stride.map_or(1, |stride| stride[*axis]),
            )
            .map_err(|error| {
                ImageError::InvalidMetadata(format!("subimage spectral coordinate: {error}"))
            })?
        {
            continue;
        }
        section_pixels(coordinate_record, &shifts, steps);
        for parameters in ["wcs", "tabular"] {
            if let Some(Value::Record(inner)) = coordinate_record.get_mut(parameters) {
                section_pixels(inner, &shifts, steps);
            }
        }
    }
    Ok(())
}

/// Adjust casacore reference pixels and increments for a section. The
/// increments scale by the pixel steps, leaving the PC matrix unchanged.
fn section_pixels(record: &mut RecordValue, shifts: &[f64], steps: &[f64]) {
    let step = |axis| steps.get(axis).copied().unwrap_or(1.0);
    let position = |axis, value| (value - shifts.get(axis).copied().unwrap_or(0.0)) / step(axis);
    match record.get_mut("crpix") {
        Some(Value::Array(array)) => map_numeric_values(array, position),
        Some(Value::Scalar(ScalarValue::Float64(crpix))) => *crpix = position(0, *crpix),
        _ => {}
    }
    if let Some(Value::Array(array)) = record.get_mut("pixelvalues") {
        map_numeric_values(array, |_, value| position(0, value));
    }
    if steps.iter().any(|&step| step != 1.0) {
        match record.get_mut("cdelt") {
            Some(Value::Array(array)) => {
                map_numeric_values(array, |axis, value| value * step(axis))
            }
            Some(Value::Scalar(ScalarValue::Float64(cdelt))) => *cdelt *= step(0),
            _ => {}
        }
    }
}

/// Transforms the elements of a stored floating-point coordinate array.
fn map_numeric_values(array: &mut ArrayValue, transform: impl Fn(usize, f64) -> f64) {
    let values = match array {
        ArrayValue::Float64(values) => values
            .iter()
            .enumerate()
            .map(|(i, value)| transform(i, *value))
            .collect::<Vec<_>>(),
        ArrayValue::Float32(values) => values
            .iter()
            .enumerate()
            .map(|(i, value)| transform(i, f64::from(*value)))
            .collect::<Vec<_>>(),
        _ => return,
    };
    *array = ArrayValue::from_f64_vec(values);
}

#[cfg(test)]
mod tests {
    use super::StridedSection;

    #[test]
    fn sparse_source_windows_never_exceed_a_dense_cursor() {
        let section = StridedSection {
            start: &[1, 3],
            shape: &[5, 8],
            stride: &[100_000, 3],
        };
        let mut read_count = 0;
        for cursor in section.cursors(vec![64, 8]) {
            let cursor = cursor.unwrap();
            let origin = section.source_start(&cursor.position);
            let extent = section.dense_shape(&cursor.shape);
            assert_eq!(extent[0], 1);
            assert!(extent[1] <= 8);
            assert_eq!((origin[0] - 1) % 100_000, 0);
            read_count += 1;
        }
        assert_eq!(read_count, 15);
    }
}
