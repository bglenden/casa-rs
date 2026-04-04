// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn validate_polygon_shape(shape: &ImageRegionShape) -> Result<(), ImageError> {
    if !shape.closed {
        return Err(ImageError::InvalidMetadata(
            "close or cancel the current polygon before saving".into(),
        ));
    }
    if shape.vertices.len() < 3 {
        return Err(ImageError::InvalidMetadata(
            "polygon regions require at least 3 vertices".into(),
        ));
    }
    if polygon_self_intersects(&shape.vertices) {
        return Err(ImageError::InvalidMetadata(
            "self-intersecting polygons are not supported".into(),
        ));
    }
    Ok(())
}

pub(super) fn region_has_closed_shapes(region: &ImageRegion) -> bool {
    region
        .shapes
        .iter()
        .any(|shape| shape.closed && shape.vertices.len() >= 3)
}

pub(super) fn coordinate_system_pixel_to_world(
    coords: &CoordinateSystem,
    pixel_indices: &[usize],
) -> Result<Vec<f64>, casa_coordinates::CoordinateError> {
    coords.to_world(
        &pixel_indices
            .iter()
            .map(|&pixel| pixel as f64)
            .collect::<Vec<_>>(),
    )
}

pub(super) fn coordinate_system_world_to_pixel(
    coords: &CoordinateSystem,
    world: &[f64],
) -> Result<Vec<f64>, casa_coordinates::CoordinateError> {
    coords.to_pixel(world)
}

pub(super) fn validate_region_axes(
    region: &ImageRegion,
    display_axes: [usize; 2],
) -> Result<(), ImageError> {
    if region.display_axes != display_axes {
        return Err(ImageError::InvalidMetadata(
            "region display axes do not match the active plane".into(),
        ));
    }
    Ok(())
}

pub(super) fn plane_context_world(
    view: &OpenedImageView,
    window: &ImageViewWindow,
    non_display_indices: &[usize],
) -> Result<Vec<f64>, ImageError> {
    let display_axes = view
        .axis_model()
        .display_axes
        .ok_or_else(|| ImageError::InvalidMetadata(view.status_line()))?;
    let center_x = window.sampled_axis_len(display_axes[0]) / 2;
    let center_y = window.sampled_axis_len(display_axes[1]) / 2;
    let probe =
        view.probe_with_window_and_axes((center_x, center_y), window, non_display_indices)?;
    if probe.world_axes.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "regions require world-coordinate support".into(),
        ));
    }
    view.pixel_indices_to_world(&probe.pixel_indices)
}

pub(super) fn full_world_at_pixel(
    coords: &CoordinateSystem,
    shape: &[usize],
    _descriptors: &[AxisDescriptor],
    display_axes: &[usize; 2],
    non_display_pixels: &[(usize, usize)],
) -> Result<Vec<f64>, ImageError> {
    let mut pixel = shape
        .iter()
        .enumerate()
        .map(|(axis, &len)| {
            if display_axes.contains(&axis) {
                len.saturating_sub(1) / 2
            } else {
                0
            }
        })
        .collect::<Vec<_>>();
    for &(axis, value) in non_display_pixels {
        pixel[axis] = value;
    }
    coordinate_system_pixel_to_world(coords, &pixel).map_err(ImageError::from)
}

pub(super) fn region_vertex_to_plane_pixel(
    coords: &CoordinateSystem,
    base_world: &[f64],
    display_axes: [usize; 2],
    vertex: &ImageRegionVertex,
) -> Result<(f64, f64), casa_coordinates::CoordinateError> {
    let mut world = base_world.to_vec();
    world[display_axes[0]] = vertex.world[0];
    world[display_axes[1]] = vertex.world[1];
    let pixel = coordinate_system_world_to_pixel(coords, &world)?;
    Ok((pixel[display_axes[0]], pixel[display_axes[1]]))
}

pub(super) fn region_vertex_to_sampled_plane(
    coords: &CoordinateSystem,
    base_world: &[f64],
    display_axes: [usize; 2],
    window: &ImageViewWindow,
    vertex: &ImageRegionVertex,
) -> Result<(f64, f64), casa_coordinates::CoordinateError> {
    let (pixel_x, pixel_y) =
        region_vertex_to_plane_pixel(coords, base_world, display_axes, vertex)?;
    Ok((
        (pixel_x - window.blc()[display_axes[0]] as f64) / window.inc()[display_axes[0]] as f64,
        (pixel_y - window.blc()[display_axes[1]] as f64) / window.inc()[display_axes[1]] as f64,
    ))
}

fn point_on_segment(point: (f64, f64), left: (f64, f64), right: (f64, f64)) -> bool {
    let dx = right.0 - left.0;
    let dy = right.1 - left.1;
    let length_sq = dx * dx + dy * dy;
    let length = length_sq.sqrt();
    let tolerance = length * 1e-6 + 1e-12;
    if length_sq <= tolerance * tolerance {
        return (point.0 - left.0).hypot(point.1 - left.1) <= tolerance;
    }
    let dot = (point.0 - left.0) * dx + (point.1 - left.1) * dy;
    let t = dot / length_sq;
    if !(-1e-6..=1.0 + 1e-6).contains(&t) {
        return false;
    }
    let projected = (left.0 + t * dx, left.1 + t * dy);
    (point.0 - projected.0).hypot(point.1 - projected.1) <= tolerance
}

pub(super) fn polygon_contains_pixel(polygon: &[(f64, f64)], point: (f64, f64)) -> bool {
    if polygon.len() < 3 {
        return false;
    }
    for index in 0..polygon.len() {
        let left = polygon[index];
        let right = polygon[(index + 1) % polygon.len()];
        if point_on_segment(point, left, right) {
            return true;
        }
    }
    let mut inside = false;
    for index in 0..polygon.len() {
        let left = polygon[index];
        let right = polygon[(index + 1) % polygon.len()];
        let intersects = ((left.1 > point.1) != (right.1 > point.1))
            && (point.0
                < (right.0 - left.0) * (point.1 - left.1) / (right.1 - left.1 + f64::EPSILON)
                    + left.0);
        if intersects {
            inside = !inside;
        }
    }
    inside
}

pub(super) fn fill_region_mask_plane(
    mask: &mut ArrayD<bool>,
    polygons: &[Vec<(f64, f64)>],
    display_axes: [usize; 2],
    non_display_pixels: &[(usize, usize)],
    shape: &[usize],
) {
    if polygons.is_empty() {
        return;
    }
    let width = shape[display_axes[0]];
    let height = shape[display_axes[1]];
    for x in 0..width {
        for y in 0..height {
            if !polygons
                .iter()
                .any(|polygon| polygon_contains_pixel(polygon, (x as f64, y as f64)))
            {
                continue;
            }
            let mut indices = vec![0usize; shape.len()];
            indices[display_axes[0]] = x;
            indices[display_axes[1]] = y;
            for &(axis, pixel) in non_display_pixels {
                indices[axis] = pixel;
            }
            mask[IxDyn(&indices)] = true;
        }
    }
}

pub(super) fn enumerate_axis_indices(lengths: &[usize]) -> Vec<Vec<usize>> {
    if lengths.is_empty() {
        return vec![Vec::new()];
    }
    let mut contexts = Vec::new();
    let mut current = vec![0usize; lengths.len()];
    loop {
        contexts.push(current.clone());
        let mut axis = lengths.len();
        while axis > 0 {
            axis -= 1;
            current[axis] += 1;
            if current[axis] < lengths[axis] {
                break;
            }
            current[axis] = 0;
        }
        if axis == 0 && current[0] == 0 {
            break;
        }
    }
    contexts
}

fn segments_intersect(a0: (f64, f64), a1: (f64, f64), b0: (f64, f64), b1: (f64, f64)) -> bool {
    fn orientation(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> f64 {
        (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
    }
    let o1 = orientation(a0, a1, b0);
    let o2 = orientation(a0, a1, b1);
    let o3 = orientation(b0, b1, a0);
    let o4 = orientation(b0, b1, a1);
    if (o1 > 0.0) != (o2 > 0.0) && (o3 > 0.0) != (o4 > 0.0) {
        return true;
    }
    point_on_segment(b0, a0, a1)
        || point_on_segment(b1, a0, a1)
        || point_on_segment(a0, b0, b1)
        || point_on_segment(a1, b0, b1)
}

pub(crate) fn polygon_self_intersects(vertices: &[ImageRegionVertex]) -> bool {
    if vertices.len() < 4 {
        return false;
    }
    for left in 0..vertices.len() {
        let next_left = (left + 1) % vertices.len();
        let left_segment = (vertices[left].world, vertices[next_left].world);
        for right in left + 1..vertices.len() {
            let next_right = (right + 1) % vertices.len();
            if left == right || left == next_right || next_left == right || next_left == next_right
            {
                continue;
            }
            let right_segment = (vertices[right].world, vertices[next_right].world);
            if segments_intersect(
                (left_segment.0[0], left_segment.0[1]),
                (left_segment.1[0], left_segment.1[1]),
                (right_segment.0[0], right_segment.0[1]),
                (right_segment.1[0], right_segment.1[1]),
            ) {
                return true;
            }
        }
    }
    false
}
