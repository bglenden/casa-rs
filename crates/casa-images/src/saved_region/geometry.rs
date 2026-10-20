// SPDX-License-Identifier: LGPL-3.0-or-later
//! Planar polygon geometry for saved regions.

use crate::error::ImageError;

/// Checks that a polygon has at least three vertices and does not cross itself.
pub(super) fn validate_polygon(vertices: &[[f64; 2]]) -> Result<(), ImageError> {
    if vertices.len() < 3 {
        return Err(ImageError::InvalidMetadata(
            "polygon regions require at least 3 vertices".into(),
        ));
    }
    if polygon_self_intersects(vertices) {
        return Err(ImageError::InvalidMetadata(
            "self-intersecting polygons are not supported".into(),
        ));
    }
    Ok(())
}

fn point_on_segment(point: [f64; 2], left: [f64; 2], right: [f64; 2]) -> bool {
    let dx = right[0] - left[0];
    let dy = right[1] - left[1];
    let length_sq = dx * dx + dy * dy;
    let length = length_sq.sqrt();
    let tolerance = length * 1e-6 + 1e-12;
    if length_sq <= tolerance * tolerance {
        return (point[0] - left[0]).hypot(point[1] - left[1]) <= tolerance;
    }
    let dot = (point[0] - left[0]) * dx + (point[1] - left[1]) * dy;
    let t = dot / length_sq;
    if !(-1e-6..=1.0 + 1e-6).contains(&t) {
        return false;
    }
    let projected = [left[0] + t * dx, left[1] + t * dy];
    (point[0] - projected[0]).hypot(point[1] - projected[1]) <= tolerance
}

/// Returns `true` when `point` lies inside `polygon` or on its boundary.
///
/// Callers pass pixel centres, so a pixel belongs to the polygon when its
/// centre does.
pub(super) fn polygon_contains_pixel(polygon: &[[f64; 2]], point: [f64; 2]) -> bool {
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
        let intersects = ((left[1] > point[1]) != (right[1] > point[1]))
            && (point[0]
                < (right[0] - left[0]) * (point[1] - left[1])
                    / (right[1] - left[1] + f64::EPSILON)
                    + left[0]);
        if intersects {
            inside = !inside;
        }
    }
    inside
}

/// Enumerates every index combination for axes of the given lengths, the
/// last axis varying fastest. No axes yields one empty combination.
pub(super) fn enumerate_axis_indices(lengths: &[usize]) -> Vec<Vec<usize>> {
    if lengths.is_empty() {
        return vec![Vec::new()];
    }
    if lengths.contains(&0) {
        return Vec::new();
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

fn segments_intersect(a0: [f64; 2], a1: [f64; 2], b0: [f64; 2], b1: [f64; 2]) -> bool {
    fn orientation(a: [f64; 2], b: [f64; 2], c: [f64; 2]) -> f64 {
        (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
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

fn polygon_self_intersects(vertices: &[[f64; 2]]) -> bool {
    if vertices.len() < 4 {
        return false;
    }
    for left in 0..vertices.len() {
        let next_left = (left + 1) % vertices.len();
        for right in left + 1..vertices.len() {
            let next_right = (right + 1) % vertices.len();
            if left == right || left == next_right || next_left == right || next_left == next_right
            {
                continue;
            }
            if segments_intersect(
                vertices[left],
                vertices[next_left],
                vertices[right],
                vertices[next_right],
            ) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polygon_self_intersection_ignores_small_world_scale_non_intersecting_shapes() {
        let vertices = [
            [3.0, 0.37],
            [3.0 + 2.0e-5, 0.37],
            [3.0 + 2.0e-5, 0.37 + 2.0e-5],
            [3.0, 0.37 + 2.0e-5],
        ];
        assert!(!polygon_self_intersects(&vertices));
    }

    #[test]
    fn polygon_self_intersection_still_rejects_crossing_shapes() {
        let vertices = [
            [3.0, 0.37],
            [3.0 + 2.0e-5, 0.37 + 2.0e-5],
            [3.0, 0.37 + 2.0e-5],
            [3.0 + 2.0e-5, 0.37],
        ];
        assert!(polygon_self_intersects(&vertices));
        assert!(validate_polygon(&vertices).is_err());
    }

    #[test]
    fn pixel_centres_on_the_boundary_are_inside() {
        let square = [[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0]];
        assert!(polygon_contains_pixel(&square, [1.0, 1.0]));
        assert!(polygon_contains_pixel(&square, [2.0, 3.0]));
        assert!(polygon_contains_pixel(&square, [2.0, 2.0]));
        assert!(!polygon_contains_pixel(&square, [0.0, 2.0]));
        assert!(!polygon_contains_pixel(&square, [4.0, 4.0]));
    }

    #[test]
    fn axis_index_enumeration_varies_the_last_axis_fastest() {
        assert_eq!(enumerate_axis_indices(&[]), vec![Vec::<usize>::new()]);
        assert_eq!(
            enumerate_axis_indices(&[2, 2]),
            vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]
        );
        assert!(enumerate_axis_indices(&[3, 0]).is_empty());
    }
}
