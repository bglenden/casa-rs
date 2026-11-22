// SPDX-License-Identifier: LGPL-3.0-or-later
//! Occupancy-based sky edges on the orthographic pointing grid.

/// Project celestial directions with CASA's SIN geometry, then select data
/// on successive eight-connected boundaries of the occupied region. The
/// selected fraction is a lower bound because the final layer is whole.
pub(super) fn select(directions: &mut [[f64; 2]], fraction: f64) -> Result<Vec<bool>, String> {
    let threshold = (fraction as f32 * directions.len() as f32) as usize;
    if threshold == 0 || fraction > 1. || directions.len() < 2 {
        return Err("OTF sky fraction selects no valid edge points.".into());
    }
    wrap_longitudes(directions);
    let (projected, use_unit_spacing) = project(directions)?;
    let spacing = if use_unit_spacing {
        2.
    } else {
        median_spacing(&projected, |left, right| {
            (right[0] - left[0]).hypot(right[1] - left[1])
        }) / 2.
    };
    if spacing == 0. {
        return Err("OTF neighboring positions have zero median separation.".into());
    }
    let limit = bounds(&projected[..projected.len() - 1]);
    let dimensions = std::array::from_fn(|axis| {
        ((limit[axis][1] - limit[axis][0]) * 1.1 / spacing).ceil() as usize
    });
    let [nx, ny] = dimensions;
    let size = nx
        .checked_mul(ny)
        .filter(|&size| size > 0)
        .ok_or("Invalid OTF pointing grid extent.")?;
    let mut occupancy = Vec::new();
    occupancy
        .try_reserve_exact(size)
        .map_err(|error| error.to_string())?;
    occupancy.resize(size, false);
    let pixels = projected
        .iter()
        .map(|point| {
            let index = |axis: usize| {
                let [low, high] = limit[axis];
                if !(low..=high).contains(&point[axis]) {
                    return None;
                }
                let value = (dimensions[axis] - 1) as f64 / 2.
                    + (point[axis] - (low + high) / 2.) / spacing;
                let rounded = value.round();
                (rounded >= 0. && rounded < dimensions[axis] as f64)
                    .then_some((rounded as usize, value >= 0.))
            };
            let (x, x_inside) = index(0)?;
            let (y, y_inside) = index(1)?;
            let pixel = x * ny + y;
            // The occupancy stage checks fractional coordinates, while
            // the mask lookup uses their rounded pixel indices.
            occupancy[pixel] |= x_inside && y_inside;
            Some(pixel)
        })
        .collect::<Vec<_>>();
    fill(&mut occupancy, nx, ny);
    peel(occupancy, nx, ny, &pixels, threshold)
}

fn wrap_longitudes(points: &mut [[f64; 2]]) {
    use std::f64::consts::{FRAC_PI_2, TAU};
    let mut quadrants = [0; 4];
    let quadrant = |longitude: f64| ((longitude / FRAC_PI_2) as usize).min(3);
    for point in points.iter_mut() {
        point[0] = point[0].rem_euclid(TAU);
        quadrants[quadrant(point[0])] += 1;
    }
    if quadrants[0] > 0 && quadrants[3] > 0 && (quadrants[1] == 0 || quadrants[2] == 0) {
        let wrap = if quadrants[1] == 0 { 2 } else { 3 };
        for point in points.iter_mut() {
            if quadrant(point[0]) >= wrap {
                point[0] -= TAU;
            }
        }
    }
}

fn bounds(points: &[[f64; 2]]) -> [[f64; 2]; 2] {
    let mut result = std::array::from_fn(|axis| [points[0][axis]; 2]);
    for point in &points[1..] {
        for (limits, &value) in result.iter_mut().zip(point) {
            limits[0] = limits[0].min(value);
            limits[1] = limits[1].max(value);
        }
    }
    result
}

fn median_spacing(points: &[[f64; 2]], separation: impl Fn([f64; 2], [f64; 2]) -> f64) -> f64 {
    let mut distances = points
        .windows(2)
        .map(|pair| separation(pair[0], pair[1]))
        .collect::<Vec<_>>();
    let middle = distances.len() / 2;
    *distances.select_nth_unstable_by(middle, f64::total_cmp).1
}

fn project(points: &[[f64; 2]]) -> Result<(Vec<[f64; 2]>, bool), String> {
    let angles = bounds(points);
    let [longitude, latitude] = angles.map(|[low, high]| (low + high) / 2.);
    let (sin_latitude, cos_latitude) = latitude.sin_cos();
    let dy = median_spacing(points, |left, right| {
        ((right[0] - left[0]) * ((left[1] + right[1]) / 2.).cos()).hypot(right[1] - left[1])
    }) / 2.;
    let dx = dy / cos_latitude;
    if dx <= 0. || dy <= 0. || !dx.is_finite() || !dy.is_finite() {
        return Err("Invalid geometry for OTF sky projection.".into());
    }
    let [nx, ny] = std::array::from_fn(|axis| {
        let [low, high] = angles[axis];
        ((high - low) * 1.1 / [dx, dy][axis]).ceil()
    });
    let projected = points
        .iter()
        .map(|&[ra, dec]| {
            let (sin_dec, cos_dec) = dec.sin_cos();
            let (sin_lon, cos_lon) = (ra - longitude).sin_cos();
            [
                (nx - 1.) / 2. + cos_dec * sin_lon / dx,
                (ny - 1.) / 2. + (sin_dec * cos_latitude - cos_dec * sin_latitude * cos_lon) / dy,
            ]
        })
        .collect();
    // CASA doubles the edge-grid spacing when the corresponding two arrays
    // of machine-word pixels would consume at least 2 GB.
    Ok((projected, nx * ny * 16. >= 2.0e9))
}

fn fill(grid: &mut [bool], nx: usize, ny: usize) {
    for _ in 0..10 {
        let mut changed = false;
        for (lines, count, stride, offset) in [(nx, ny, 1, ny), (ny, nx, ny, 1)] {
            for line in 0..lines {
                let pixel = |index| line * offset + index * stride;
                let Some(first) = (0..count).find(|&index| grid[pixel(index)]) else {
                    continue;
                };
                let last = (first..count).rfind(|&index| grid[pixel(index)]).unwrap();
                for index in first..last {
                    let value = &mut grid[pixel(index)];
                    changed |= !*value;
                    *value = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
}

fn peel(
    mut occupancy: Vec<bool>,
    nx: usize,
    ny: usize,
    samples: &[Option<usize>],
    threshold: usize,
) -> Result<Vec<bool>, String> {
    let mut remaining = threshold;
    let mut selected = vec![false; samples.len()];
    let mut edge = Vec::new();
    edge.try_reserve_exact(occupancy.len())
        .map_err(|error| error.to_string())?;
    edge.resize(occupancy.len(), false);
    while remaining > 0 {
        let mut found_boundary = false;
        for (pixel, boundary) in edge.iter_mut().enumerate() {
            let x = pixel / ny;
            let y = pixel % ny;
            *boundary = occupancy[pixel]
                && (x == 0
                    || y == 0
                    || x + 1 == nx
                    || y + 1 == ny
                    || (x - 1..=x + 1)
                        .any(|xx| (y - 1..=y + 1).any(|yy| !occupancy[xx * ny + yy])));
            found_boundary |= *boundary;
        }
        if !found_boundary {
            return Err("OTF sky fraction exceeds the available edge points.".into());
        }
        for (sample, selected) in samples.iter().zip(&mut selected) {
            if !*selected && sample.is_some_and(|pixel| edge[pixel]) {
                *selected = true;
                remaining = remaining.saturating_sub(1);
            }
        }
        occupancy
            .iter_mut()
            .zip(&edge)
            .for_each(|(value, edge)| *value &= !*edge);
    }
    Ok(selected)
}
