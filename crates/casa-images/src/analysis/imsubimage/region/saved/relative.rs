// SPDX-License-Identifier: LGPL-3.0-or-later
//! Project reference-relative and image-center-relative saved shapes.

use casa_coordinates::CoordinateSystem;
use casa_core::quanta::{Quantity, Unit};
use casa_core::{RecordValue, ScalarValue, Value};

use crate::analysis::world_axes_for_plane;
use crate::saved_region::{
    PolygonPixelPlane, native_region_kind, parse_quantum_vector_field, polygon_pixel_plane,
};
use crate::{ImageError, PolygonRegion};

use super::super::{ParsedRegion, SavedRegionMask};
use super::{make_region_world_absolute, wc_pixel_axes};

pub(super) fn mode(record: &RecordValue) -> Option<f64> {
    match record.get("absrel") {
        Some(Value::Scalar(ScalarValue::Int32(mode @ (2 | 3)))) => Some(f64::from(*mode)),
        _ => None,
    }
}

pub(super) fn decode_polygon(
    name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
) -> Result<ParsedRegion, ImageError> {
    let (axes, plane) = polygon_plane(name, record, coords, shape)?;
    let (start, shape) = plane.section(shape, axes);
    Ok(ParsedRegion {
        is_compound: false,
        is_crtf: false,
        shapes: Vec::new(),
        saved_mask: Some(SavedRegionMask::Polygon { axes, plane }),
        saved_bounds: Some((start, shape)),
        stride: None,
    })
}

pub(super) fn polygon_plane(
    name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
) -> Result<([usize; 2], PolygonPixelPlane), ImageError> {
    let empty =
        || ImageError::CasaSelection(format!("saved region '{name}' selects no pixels in image"));
    let Some(mode) = mode(record) else {
        let polygon = PolygonRegion::from_record(name, record, coords)?;
        let plane = polygon.lattice_plane(coords, shape)?.ok_or_else(empty)?;
        return Ok((polygon.pixel_axes, plane));
    };
    native_region_kind(name, record)?;
    let (axes, _) = wc_pixel_axes(name, "WCPolygon", record, coords)?;
    let [x, y] = axes.as_slice() else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved polygon '{name}' needs two axes"
        )));
    };
    let axes = [*x, *y];
    if x == y {
        return Err(ImageError::InvalidMetadata(format!(
            "saved polygon '{name}' needs distinct axes"
        )));
    }
    let (x_values, x_unit) = parse_quantum_vector_field(record, "x", name)?;
    let (y_values, y_unit) = parse_quantum_vector_field(record, "y", name)?;
    if x_values.len() != y_values.len() || x_values.len() < 3 {
        return Err(ImageError::InvalidMetadata(format!(
            "saved polygon '{name}' has invalid vertex counts"
        )));
    }
    // WCPolygon assigns its mode to the two mapped world axes.
    let mut modes = vec![1.0; coords.n_world_axes()];
    let world_axes = world_axes_for_plane(coords, axes)?;
    for axis in world_axes {
        modes[axis] = mode;
    }
    let points = x_values.into_iter().zip(y_values).map(|(x, y)| [x, y]);
    let projected = project_points(
        coords,
        shape,
        world_axes,
        axes,
        [&x_unit, &y_unit],
        points,
        &modes,
    )?;
    let plane = polygon_pixel_plane(&[projected], shape, axes).ok_or_else(empty)?;
    Ok((axes, plane))
}

pub(super) fn project_points(
    coords: &CoordinateSystem,
    shape: &[usize],
    world_axes: [usize; 2],
    pixel_axes: [usize; 2],
    units: [&str; 2],
    points: impl IntoIterator<Item = [f64; 2]>,
    modes: &[f64],
) -> Result<Vec<[f64; 2]>, ImageError> {
    let reference = coords.reference_value();
    let reference_pixels = coords.reference_pixel();
    let axis_units = coords.world_axis_units();
    let scale = |index: usize| {
        if units[index] == "default" {
            return Err(ImageError::InvalidMetadata(
                "default units are not allowed in saved region shapes".into(),
            ));
        }
        if matches!(units[index], "pix" | "frac") {
            return Ok(None);
        }
        let convert = || {
            Quantity::new(1.0, units[index])?
                .get_value_in(&Unit::new(&axis_units[world_axes[index]])?)
        };
        convert()
            .map(Some)
            .map_err(|err| ImageError::InvalidMetadata(err.to_string()))
    };
    let scales = [scale(0)?, scale(1)?];
    let mut world = reference.clone();
    points
        .into_iter()
        .map(|values| {
            for index in 0..2 {
                if let Some(scale) = scales[index] {
                    world[world_axes[index]] = values[index] * scale;
                }
            }
            make_region_world_absolute(coords, shape, modes, &reference, &mut world)?;
            let pixels = coords.to_pixel(&world)?;
            let pixel = |index: usize| {
                let axis = pixel_axes[index];
                let offset = if modes[world_axes[index]] == 2.0 {
                    reference_pixels[coords
                        .world_axis_to_pixel_axis(world_axes[index])
                        .unwrap_or(axis)]
                } else {
                    shape[axis] as f64 * 0.5
                };
                match units[index] {
                    "pix" => values[index] + offset,
                    "frac" => values[index] * shape[axis] as f64 + offset,
                    _ => pixels[axis],
                }
            };
            let projected = [pixel(0), pixel(1)];
            if projected.iter().any(|p| !p.is_finite()) {
                return Err(ImageError::InvalidMetadata(
                    "non-finite saved region pixel position".into(),
                ));
            }
            Ok(projected)
        })
        .collect()
}
