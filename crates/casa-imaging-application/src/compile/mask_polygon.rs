// SPDX-License-Identifier: LGPL-3.0-or-later

//! Project CRTF sky vertices through the output direction coordinate and
//! fill each scanline with casacore LCPolygon's Float edge convention.

use std::sync::Arc;

use casa_core::measures::direction::DirectionRef;
use casa_imaging_model::DirectionCoordinateSpec;

use super::PrepareError;
use crate::tclean::MaskPolygon;

struct Edge {
    start_y: f32,
    end_y: f32,
    slope: f32,
    intercept: f32,
    take_start: bool,
}

impl Edge {
    fn goes_down(&self) -> bool {
        self.start_y > self.end_y
    }

    fn crosses(&self, y: f32) -> bool {
        if near(y, self.start_y) {
            return self.take_start;
        }
        near(y, self.end_y)
            || (y > self.start_y.min(self.end_y) && y < self.start_y.max(self.end_y))
    }
}

/// Peak and retained bytes for the target support and the per-vertex
/// projection and scanline scratch.
pub(crate) fn bytes(polygon: &MaskPolygon, [nx, ny]: [usize; 2]) -> (u64, u64) {
    let retained = (nx as u64)
        .saturating_mul(ny as u64)
        .saturating_add(size_of::<[usize; 2]>() as u64);
    let scratch_per_vertex = 2 * size_of::<[f32; 2]>() + size_of::<Edge>() + size_of::<f32>();
    let scratch = (polygon.vertices.len() as u64).saturating_mul(scratch_per_vertex as u64);
    (retained.saturating_mul(2).saturating_add(scratch), retained)
}

pub(crate) fn support(
    polygon: &MaskPolygon,
    direction: DirectionCoordinateSpec,
    shape: [usize; 2],
) -> Result<Arc<[bool]>, PrepareError> {
    let mut coordinate = direction.coordinate();
    if polygon.j2000 {
        coordinate = coordinate.with_conversion_direction_ref(DirectionRef::J2000);
    }
    let mut vertices = Vec::with_capacity(polygon.vertices.len());
    for sky in polygon.vertices.iter() {
        let pixel = coordinate
            .world_to_pixel(*sky)
            .map_err(|source| PrepareError::MaskPolygonProjection { source })?;
        let pixel = pixel.map(|value| value as f32);
        if !pixel.into_iter().all(f32::is_finite) {
            return Err(PrepareError::MaskPolygon);
        }
        vertices.push(pixel);
    }
    let Some(first) = vertices.first() else {
        return Err(PrepareError::MaskPolygon);
    };
    if let Some(last) = vertices.last()
        && near(first[0], last[0])
        && near(first[1], last[1])
    {
        vertices.pop();
    }
    if vertices.len() < 3 {
        return Err(PrepareError::MaskPolygon);
    }
    raster(&vertices, shape)
}

fn raster(vertices: &[[f32; 2]], [nx, ny]: [usize; 2]) -> Result<Arc<[bool]>, PrepareError> {
    let bounds = |axis, limit| {
        let min = vertices
            .iter()
            .map(|point| point[axis])
            .fold(f32::INFINITY, f32::min);
        let max = vertices
            .iter()
            .map(|point| point[axis])
            .fold(f32::NEG_INFINITY, f32::max);
        (start(min), end(max).min(limit as i64 - 1))
    };
    let (left, right) = bounds(0, nx);
    let (bottom, top) = bounds(1, ny);
    let mut mask = vec![false; nx * ny];
    if left > right || bottom > top {
        return Ok(mask.into());
    }

    // LCPolygon clips after evaluating a crossing relative to the
    // clipped bounding-box origin, and includes pixels on its boundary.
    let mut fill_span = |y: i64, a: f32, b: f32| {
        let x0 = left.saturating_add(start(a.min(b)));
        let x1 = right.min(left.saturating_add(end(a.max(b))));
        for x in x0..=x1 {
            mask[x as usize * ny + y as usize] = true;
        }
    };
    let mut edges = Vec::with_capacity(vertices.len());
    for (&[x0, y0], &[x1, y1]) in vertices.iter().zip(vertices.iter().cycle().skip(1)) {
        if near(y0, y1) {
            let y = y0 as i64;
            if (bottom..=top).contains(&y) && near(y as f32, y0) {
                fill_span(y, x0 - left as f32, x1 - left as f32);
            }
        } else {
            let slope = (x0 - x1) / (y0 - y1);
            edges.push(Edge {
                start_y: y0,
                end_y: y1,
                slope,
                intercept: x0 - slope * y0,
                take_start: false,
            });
        }
    }

    // A scanline through a vertex counts the prior edge's endpoint. It
    // also counts the next edge's start when the edges reverse direction.
    if let Some(last) = edges.last() {
        let mut previous = last.goes_down();
        for edge in &mut edges {
            let down = edge.goes_down();
            edge.take_start = down != previous;
            previous = down;
        }
    }
    let mut crossings = Vec::with_capacity(edges.len());
    for row in bottom..=top {
        let y = row as f32;
        crossings.clear();
        crossings.extend(
            edges
                .iter()
                .filter(|edge| edge.crosses(y))
                .map(|edge| edge.slope * y + edge.intercept - left as f32),
        );
        if !crossings.len().is_multiple_of(2) || !crossings.iter().copied().all(f32::is_finite) {
            return Err(PrepareError::MaskPolygon);
        }
        crossings.sort_unstable_by(f32::total_cmp);
        for &[a, b] in crossings.as_chunks::<2>().0 {
            fill_span(row, a, b);
        }
    }
    Ok(mask.into())
}

/// The pixel centres inside an axis-aligned fractional CRTF box.
/// LCPolygon computes its horizontal crossings relative to its clipped
/// bounding-box origin. Keep that origin when applying the Float edge rule.
pub(crate) fn fractional_pixel_box([xa, ya, xb, yb]: [f64; 4]) -> Vec<[i64; 4]> {
    let [xa, ya, xb, yb] = [xa, ya, xb, yb].map(|value| value as f32);
    if ![xa, ya, xb, yb].into_iter().all(f32::is_finite) {
        return Vec::new();
    }
    let (left, right) = (xa.min(xb), xa.max(xb));
    let origin = start(left);
    let x0 = origin.saturating_add(start(left - origin as f32));
    // A thin box can end just below its rounded origin. CASA truncates
    // that relative crossing to zero. The absolute bound still clips
    // intervals lying wholly outside the image.
    let relative_right = (right - origin as f32).max(0.0);
    let x1 = end(right).min(origin.saturating_add(end(relative_right)));
    let (y0, y1) = (start(ya.min(yb)), end(ya.max(yb)));
    if x1 < x0 || y1 < y0 {
        return Vec::new();
    }
    if near(ya, yb) {
        // All edges are horizontal at LCPolygon's tolerance. Only an
        // edge whose truncated ordinate still coincides with the row paints.
        let mut rows = [ya, yb]
            .into_iter()
            .filter_map(|edge| {
                let row = edge as i64;
                ((y0..=y1).contains(&row) && near(row as f32, edge)).then_some([x0, row, x1, row])
            })
            .collect::<Vec<_>>();
        rows.dedup();
        return rows;
    }
    vec![[x0, y0, x1, y1]]
}

fn near(a: f32, b: f32) -> bool {
    // LCPolygon::_isNear uses nearAbs at zero, near elsewhere.
    let scale = if a == 0.0 || b == 0.0 {
        1.0
    } else {
        a.abs().max(b.abs())
    };
    f64::from((a - b).abs()) <= 1.0e-5 * f64::from(scale)
}

fn start(value: f32) -> i64 {
    let toward_pixel = f64::from(value) + 0.1;
    let pixel = if near(value, toward_pixel.floor() as f32) {
        toward_pixel as i64
    } else {
        (value + 1.0) as i64
    };
    pixel.max(0)
}

fn end(value: f32) -> i64 {
    let inclusive_pixel = (f64::from(value) + 0.1).floor();
    if near(value, inclusive_pixel as f32) {
        inclusive_pixel as i64
    } else {
        // Truncation toward zero in LCPolygon lets a wholly negative
        // interval less than a pixel away select column zero (#823).
        value.floor() as i64
    }
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::{DirectionFrame, Projection, SkyDirection};

    use super::*;

    #[test]
    fn a_named_sky_frame_converts_the_vertices_before_rasterizing() {
        let direction = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::B1950, 1.2, -0.5),
            [16.0; 2],
            [-1.0e-5, 1.0e-5],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        );
        for j2000 in [false, true] {
            let mut coordinate = direction.coordinate();
            if j2000 {
                coordinate = coordinate.with_conversion_direction_ref(DirectionRef::J2000);
            }
            let vertices = [[4.25, 6.25], [13.25, 6.25], [13.25, 12.25], [4.25, 12.25]]
                .into_iter()
                .map(|pixel| coordinate.pixel_to_world(pixel).unwrap())
                .collect();
            let polygon = MaskPolygon { vertices, j2000 };
            let mask = support(&polygon, direction, [32; 2]).unwrap();
            for (index, got) in mask.iter().enumerate() {
                let (x, y) = (index / 32, index % 32);
                let expected = (5..=13).contains(&x) && (7..=12).contains(&y);
                assert_eq!(*got, expected, "explicit J2000 = {j2000}, pixel [{x}, {y}]");
            }
        }
    }
}
