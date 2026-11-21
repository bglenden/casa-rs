// SPDX-License-Identifier: LGPL-3.0-or-later
//! Local cubic and band integration for the tabulated 2012 Solar System models.

pub(super) fn nearest_index(coordinates: &[f64], coordinate: f64) -> usize {
    let upper = coordinates.partition_point(|&value| value < coordinate);
    let upper = upper.clamp(1, coordinates.len() - 1);
    if coordinate < (coordinates[upper - 1] + coordinates[upper]) * 0.5 {
        upper - 1
    } else {
        upper
    }
}

pub(super) fn window(count: usize, nearest: usize) -> std::ops::Range<usize> {
    let first = nearest.saturating_sub(5);
    let last = (nearest + 5).min(count);
    if first == 0 {
        0..count.min(11)
    } else if last == count {
        count.saturating_sub(11)..count
    } else {
        first..last
    }
}

pub(super) fn interpolate(coordinates: &[f64], values: &[f64], coordinate: f64) -> f64 {
    if values.len() == 1 {
        return values[0];
    }
    let nearest = nearest_index(coordinates, coordinate);
    let range = window(values.len(), nearest);
    interpolate_window(&coordinates[range.clone()], &values[range], coordinate)
}

pub(super) fn interpolate_window(x: &[f64], y: &[f64], coordinate: f64) -> f64 {
    if x.len() == 1 {
        return y[0];
    }
    let nearest = nearest_index(x, coordinate);
    let upper = x
        .partition_point(|&value| value < coordinate)
        .clamp(1, x.len() - 1);
    let left = upper - 1;
    let width = x[upper] - x[left];
    let t = (coordinate - x[left]) / width;
    let linear = y[left] + t * (y[upper] - y[left]);
    let min = y.iter().copied().fold(f64::INFINITY, f64::min);
    let max = y.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let half_span = (max - min) * 0.5;
    let credible = (min - half_span)..=(max + half_span);
    if x.len() >= 4 {
        let [a, b] = spline_slopes(x, y, left);
        let t2 = t * t;
        let t3 = t2 * t;
        let cubic = (2. * t3 - 3. * t2 + 1.) * y[left]
            + (t3 - 2. * t2 + t) * width * a
            + (-2. * t3 + 3. * t2) * y[upper]
            + (t3 - t2) * width * b;
        if credible.contains(&cubic) {
            return cubic;
        }
    }
    if credible.contains(&linear) {
        linear
    } else {
        y[nearest]
    }
}

fn spline_slopes(x: &[f64], y: &[f64], segment: usize) -> [f64; 2] {
    // Solve the tridiagonal first-derivative equations for a not-a-knot
    // cubic spline, using the same end conditions as scipy interp1d.
    let count = x.len();
    let mut upper = [0.; 11];
    let mut rhs = [0.; 11];
    let step = |row: usize| x[row + 1] - x[row];
    let slope = |row: usize| (y[row + 1] - y[row]) / step(row);
    let first = step(0);
    let second = step(1);
    let span = first + second;
    upper[0] = span / second;
    rhs[0] = ((first + 2. * span) * second * slope(0) + first * first * slope(1)) / (span * second);
    for i in 1..count {
        let left = step(i - 1);
        let (lower, diagonal, above, target) = if i + 1 == count {
            let prior = step(i - 2);
            let span = prior + left;
            (
                span,
                prior,
                0.,
                (left * left * slope(i - 2) + (2. * span + left) * prior * slope(i - 1)) / span,
            )
        } else {
            let right = step(i);
            (
                right,
                2. * (left + right),
                left,
                3. * (right * slope(i - 1) + left * slope(i)),
            )
        };
        let pivot = diagonal - lower * upper[i - 1];
        upper[i] = above / pivot;
        rhs[i] = (target - lower * rhs[i - 1]) / pivot;
    }
    for i in (segment..count - 1).rev() {
        rhs[i] -= upper[i] * rhs[i + 1];
    }
    [rhs[segment], rhs[segment + 1]]
}

pub(super) fn average_over_band(
    coordinates: &[f64],
    values: &[f64],
    band: [f64; 2],
) -> Option<f64> {
    let [low, high] = band;
    if low < *coordinates.first()? || high > *coordinates.last()? || high < low {
        return None;
    }
    let low_value = interpolate(coordinates, values, low);
    if low == high {
        return Some(low_value);
    }
    let start = coordinates.partition_point(|&value| value < low);
    let end = coordinates.partition_point(|&value| value <= high);
    let high_value = interpolate(coordinates, values, high);
    let mut area = 0.;
    let (mut previous, mut brightness) = (low, low_value);
    for (&next, &value) in coordinates[start..end].iter().zip(&values[start..end]) {
        area += (next - previous) * (brightness + value) * 0.5;
        (previous, brightness) = (next, value);
    }
    area += (high - previous) * (brightness + high_value) * 0.5;
    Some(area / (high - low))
}
