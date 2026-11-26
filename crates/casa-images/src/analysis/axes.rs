// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where the image tasks find an image's axes, and moving the reference
//! pixels of a coordinate-system record with a subimage.
//!
//! casa-rs reads a CASA image whose coordinates own transposed axes with
//! the coordinates in axis order, and drops a linear coordinate's removed
//! axes (#1001), so its coordinates can differ from the record's: a
//! record's coordinates are found where the record places them.

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, QualityCoordinate, QualityType,
    StokesCoordinate, StokesType,
};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_lattices::{TraversalCursorIter, TraversalSpec};

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
/// The direction axes can be swapped or separated by other coordinates.
///
/// - Regions take their plane here, as CASA does.
/// - `feather` does too, where CASA 6.7.6's `feather` fails on such an
///   image (`docs/CASA (C++) bugs.md`).
/// - `impv` takes it only on axes 0 and 1 until its output has a PV image's
///   coordinates (#1024).
pub(super) fn direction_plane_axes<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
) -> Result<[usize; 2], ImageError> {
    direction_plane_axes_for_coordinates(image.coordinates())
}

pub(super) fn direction_plane_axes_for_coordinates(
    coordinates: &CoordinateSystem,
) -> Result<[usize; 2], ImageError> {
    let Some(index) = coordinates.find_coordinate(CoordinateType::Direction) else {
        return Ok([0, 1]);
    };
    let axes = coordinates.coordinate_pixel_axes(index).unwrap_or_default();
    let [Some(longitude), Some(latitude)] = axes.as_slice() else {
        return Err(ImageError::InvalidMetadata(
            "selecting a direction plane requires both direction axes".into(),
        ));
    };
    Ok([*longitude, *latitude])
}

pub(super) fn world_axes_for_plane(
    coordinates: &CoordinateSystem,
    plane: [usize; 2],
) -> Result<[usize; 2], ImageError> {
    let [Some(first), Some(second)] = plane.map(|axis| coordinates.pixel_axis_to_world_axis(axis))
    else {
        return Err(ImageError::InvalidMetadata(
            "a world region requires both world axes of the image plane".into(),
        ));
    };
    Ok([first, second])
}

/// `coordinates` for a subimage starting at pixel `start`.
pub(super) fn subimage_coordinates(
    coordinates: &CoordinateSystem,
    start: &[usize],
    shape: &[usize],
) -> Result<CoordinateSystem, ImageError> {
    let mut record = coordinates.to_record();
    shift_record_reference_pixels(&mut record, start, shape)?;
    coordinates
        .restore_record_with_layout(&record)
        .map_err(|error| {
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
    if let Some((index, _)) = coords.pixel_axis_owner(axis)
        && let CoordinateModel::Stokes(stokes) = coords.coordinate(index)
    {
        return Ok(stokes.stokes().to_vec());
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
    let Some(Value::Record(coords)) = image.table().keywords().get("coords") else {
        return None;
    };
    subimage_coordinate_record(coords.clone(), start, shape, stokes)
}

pub(super) fn subimage_coordinate_record(
    mut coords: RecordValue,
    start: &[usize],
    shape: &[usize],
    stokes: Option<&[StokesType]>,
) -> Option<RecordValue> {
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
        if coordinate.field.starts_with("stokes")
            && matches!(record.get(&coordinate.field), Some(Value::Record(_)))
        {
            replace_stokes_axis_in_record(record, &coordinate.field, stokes)?;
        }
    }
    Ok(())
}

/// Replace a single stored Stokes axis and synchronize its reference fields.
///
/// # Errors
///
/// Returns an error if the named coordinate record is missing.
pub fn replace_stokes_axis_in_record(
    record: &mut RecordValue,
    field: &str,
    stokes: &[StokesType],
) -> Result<(), ImageError> {
    let Some(Value::Record(stokes_rec)) = record.get_mut(field) else {
        return Err(ImageError::InvalidMetadata(format!(
            "Missing Stokes coordinate {field}"
        )));
    };
    let use_codes = matches!(
        stokes_rec.get("stokes"),
        Some(Value::Array(ArrayValue::Int32(_)))
    );
    let labels = if use_codes {
        ArrayValue::from_i32_vec(stokes.iter().map(|s| s.code()).collect())
    } else {
        ArrayValue::from_string_vec(stokes.iter().map(|s| s.name().to_string()).collect())
    };
    stokes_rec.upsert("stokes", Value::Array(labels));
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
        if let Some(suffix) = field.strip_prefix("stokes") {
            let wr_key = format!("worldreplace{suffix}");
            if matches!(record.get(&wr_key), Some(Value::Array(_))) {
                record.upsert(
                    wr_key,
                    Value::Array(ArrayValue::from_f64_vec(vec![f64::from(first.code())])),
                );
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
        if coordinate_record.get("quality").is_some() {
            if let [Some(axis)] = coordinate.pixel_axes.as_slice() {
                section_quality_labels(
                    coordinate_record,
                    start[*axis],
                    shape[*axis],
                    stride.map_or(1, |stride| stride[*axis]),
                )?;
            }
            continue;
        }
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

/// Keep the planes selected by the image section in a native Quality subrecord.
///
/// # Errors
/// The stride must be positive and every selected plane must have a label.
pub fn section_quality_labels(
    record: &mut RecordValue,
    start: usize,
    count: usize,
    stride: usize,
) -> Result<(), ImageError> {
    if stride == 0 {
        return Err(ImageError::InvalidMetadata(
            "Quality image section requires a positive stride".into(),
        ));
    }
    let quality = QualityCoordinate::from_record(record)?;
    let labels: Vec<_> = quality
        .values()
        .iter()
        .copied()
        .skip(start)
        .step_by(stride)
        .take(count)
        .collect();
    if labels.len() != count {
        return Err(ImageError::InvalidMetadata(
            "Quality axis has fewer planes than the image section".into(),
        ));
    }
    write_quality_labels(record, &labels)
}

/// Replace the plane labels and reference values of a native Quality subrecord.
///
/// # Errors
/// An empty label list cannot describe a Quality coordinate.
pub fn write_quality_labels(
    record: &mut RecordValue,
    labels: &[QualityType],
) -> Result<(), ImageError> {
    let first = labels.first().ok_or_else(|| {
        ImageError::InvalidMetadata("A Quality axis requires at least one plane".into())
    })?;
    record.upsert(
        "quality",
        ArrayValue::from_string_vec(labels.iter().map(|kind| kind.name().to_owned()).collect())
            .into(),
    );
    record.upsert(
        "crval",
        ArrayValue::from_f64_vec(vec![f64::from(first.code())]).into(),
    );
    record.upsert("crpix", ArrayValue::from_f64_vec(vec![0.0]).into());
    Ok(())
}

/// Adjust the reference pixels and scale the columns of the coordinate transform.
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
        // CDELT scales world rows. The stored PC transposes the matrix
        // used by the transform, so its first index is the input axis.
        let shape = [steps.len(), steps.len()];
        match record.get_mut("pc") {
            Some(Value::Array(ArrayValue::Float64(matrix))) if matrix.shape() == shape => {
                for (index, coefficient) in matrix.indexed_iter_mut() {
                    *coefficient *= steps[index[0]] / steps[index[1]];
                }
            }
            Some(Value::Array(ArrayValue::Float32(matrix))) if matrix.shape() == shape => {
                for (index, coefficient) in matrix.indexed_iter_mut() {
                    *coefficient *= (steps[index[0]] / steps[index[1]]) as f32;
                }
            }
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
    use casa_coordinates::defaults::coordinate_system_for_shape;
    use casa_lattices::TempStoragePolicy;

    use crate::TempImage;
    use crate::analysis::{SubimageOptions, SubimagePlan, SubimageRegion};

    use super::{CoordinateType, StridedSection, direction_plane_axes, stokes_for_axis};

    #[test]
    fn image_axes_follow_pixels_but_the_direction_plane_follows_local_order() {
        let mut coordinates = coordinate_system_for_shape(&[4, 3, 2, 5], false);
        coordinates
            .transpose_axes(&[1, 2, 0, 3], &[1, 2, 0, 3])
            .unwrap();
        let image = TempImage::<f32>::new(
            vec![3, 2, 4, 5],
            coordinates.clone(),
            TempStoragePolicy::Memory,
        )
        .unwrap();
        assert_eq!(
            image.axis_names(),
            ["Declination", "Stokes", "Right Ascension", "Frequency"],
        );
        assert_eq!(
            image.axis_types(),
            [
                CoordinateType::Direction,
                CoordinateType::Stokes,
                CoordinateType::Direction,
                CoordinateType::Spectral
            ],
        );
        assert_eq!(image.find_axis(CoordinateType::Direction), Some(0));
        assert_eq!(image.find_axis(CoordinateType::Stokes), Some(1));
        assert_eq!(image.find_axis_by_name("right asc"), Some(2));
        assert_eq!(direction_plane_axes(&image).unwrap(), [2, 0]);
        assert_eq!(stokes_for_axis(&coordinates, 1).unwrap().len(), 2);
        let summaries = super::super::axis_summaries(&coordinates, &[3, 2, 4, 5], false);
        assert_eq!(
            summaries
                .iter()
                .map(|axis| (axis.axis, axis.name.as_str(), axis.shape))
                .collect::<Vec<_>>(),
            [
                (0, "Declination", 3),
                (1, "Stokes", 2),
                (2, "Right Ascension", 4),
                (3, "Frequency", 5)
            ],
        );
        coordinates.remove_pixel_axis(2, 0.0).unwrap();
        let sliced =
            TempImage::<f32>::new(vec![3, 2, 5], coordinates, TempStoragePolicy::Memory).unwrap();
        assert!(direction_plane_axes(&sliced).is_err());
    }

    #[test]
    fn mapped_world_regions_and_pixel_sections_keep_the_selected_geometry() {
        let mut coordinates = coordinate_system_for_shape(&[4, 3, 2, 5], false);
        coordinates
            .transpose_axes(&[0, 1, 2, 3], &[1, 2, 0, 3])
            .unwrap();
        let image = TempImage::<f32>::new(
            vec![3, 2, 4, 5],
            coordinates.clone(),
            TempStoragePolicy::Memory,
        )
        .unwrap();
        let first = coordinates.to_world(&[1.0, 0.0, 1.0, 0.0]).unwrap();
        let last = coordinates.to_world(&[2.0, 0.0, 2.0, 0.0]).unwrap();
        let world_box = [first[0], first[1], last[0], last[1]];
        let pixel_box = super::super::crtf_world_box_pixels(&image, world_box).unwrap();
        assert_eq!(pixel_box, "1,1,2,2");
        let region = format!(
            "box[[{}rad, {}rad], [{}rad, {}rad]]",
            first[0], first[1], last[0], last[1],
        );
        for text in ["box[[1pix, 1pix], [2pix, 2pix]]", &region] {
            let plan = SubimagePlan::new(
                &image,
                Some(coordinates.to_record()),
                SubimageOptions {
                    region: Some(SubimageRegion::Text(text)),
                    dropdeg: false,
                    keepaxes: &[],
                },
            )
            .unwrap();
            assert_eq!(plan.bounds().0, [1, 0, 1, 0]);
            assert_eq!(plan.shape, [2, 2, 2, 5]);
            assert_eq!(plan.coordinates.pixel_axis_to_world_axis(0), Some(1));
            let reference = plan.coordinates.to_world(&[0.0; 4]).unwrap();
            for (actual, expected) in reference.iter().zip(&first) {
                assert!((actual - expected).abs() < 1e-12);
            }
        }
    }

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
