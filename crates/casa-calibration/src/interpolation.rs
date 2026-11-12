// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar amplitude/phase curves shared by time and coordinate interpolation.
use casa_types::Complex32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Curve {
    Nearest,
    Linear,
    Cubic,
    Spline,
}

pub(crate) fn unwrap_phase(previous: f32, mut phase: f32) -> f32 {
    while f64::from(phase - previous) > std::f64::consts::PI {
        phase = (f64::from(phase) - std::f64::consts::TAU) as f32;
    }
    while f64::from(phase - previous) < -std::f64::consts::PI {
        phase = (f64::from(phase) + std::f64::consts::TAU) as f32;
    }
    phase
}

pub(crate) fn sample(
    points: &[(f64, f32, f32)],
    coordinate: f64,
    mode: Curve,
    descending: bool,
    derivatives: &[[f64; 2]],
) -> Option<Complex32> {
    sample_polar(points, coordinate, mode, descending, derivatives)
        .map(|(amplitude, phase)| Complex32::from_polar(amplitude, phase))
}

pub(crate) fn sample_polar(
    points: &[(f64, f32, f32)],
    coordinate: f64,
    mode: Curve,
    descending: bool,
    derivatives: &[[f64; 2]],
) -> Option<(f32, f32)> {
    let first = points.first()?;
    if points.len() == 1 || coordinate <= first.0 {
        return Some((first.1, first.2));
    }
    let last = points.last().expect("nonempty");
    if coordinate >= last.0 {
        return Some((last.1, last.2));
    }
    let upper = points.partition_point(|point| point.0 < coordinate);
    let (low, high) = (points[upper - 1], points[upper]);
    let (amplitude, phase) = if mode == Curve::Nearest {
        let nearest = if high.0 - coordinate < coordinate - low.0
            || (descending && high.0 - coordinate == coordinate - low.0)
        {
            high
        } else {
            low
        };
        (nearest.1, nearest.2)
    } else if mode == Curve::Cubic {
        let first = upper.saturating_sub(2).min(points.len().saturating_sub(4));
        let stencil = &points[first..(first + 4).min(points.len())];
        let polynomial = |axis: usize| {
            let mut c = [0.0f32; 4];
            let mut d = [0.0f32; 4];
            for (index, point) in stencil.iter().enumerate() {
                c[index] = if axis == 0 { point.1 } else { point.2 };
                d[index] = c[index];
            }
            let mut value = c[0];
            for order in 1..stencil.len() {
                for index in 0..stencil.len() - order {
                    let w = f64::from(c[index + 1] - d[index])
                        / (stencil[index].0 - stencil[index + order].0);
                    c[index] = ((stencil[index].0 - coordinate) * w) as f32;
                    d[index] = ((stencil[index + order].0 - coordinate) * w) as f32;
                }
                value += c[0];
            }
            value
        };
        (polynomial(0), polynomial(1))
    } else if mode == Curve::Spline {
        let width = high.0 - low.0;
        let a = (high.0 - coordinate) / width;
        let b = 1.0 - a;
        let evaluate = |left: f32, right: f32, axis: usize| {
            (a * f64::from(left)
                + b * f64::from(right)
                + width * width / 6.0
                    * ((a * a * a - a) * derivatives[upper - 1][axis]
                        + (b * b * b - b) * derivatives[upper][axis])) as f32
        };
        (evaluate(low.1, high.1, 0), evaluate(low.2, high.2, 1))
    } else {
        let fraction = (coordinate - low.0) / (high.0 - low.0);
        let interpolate = |a: f32, b: f32| (f64::from(a) + f64::from(b - a) * fraction) as f32;
        (interpolate(low.1, high.1), interpolate(low.2, high.2))
    };
    Some((amplitude, phase))
}

/// Tridiagonal solution for natural (zero endpoint curvature) cubic splines.
pub(crate) fn spline_derivatives(points: &[(f64, f32, f32)]) -> Vec<[f64; 2]> {
    let mut second = vec![[0.0; 2]; points.len()];
    if points.len() < 3 {
        return second;
    }
    let mut upper = vec![0.0; points.len()];
    for index in 1..points.len() - 1 {
        let left = points[index].0 - points[index - 1].0;
        let right = points[index + 1].0 - points[index].0;
        let diagonal = 2.0 * (left + right) - left * upper[index - 1];
        upper[index] = right / diagonal;
        let previous = second[index - 1];
        for (axis, value) in second[index].iter_mut().enumerate() {
            let y = |index: usize| {
                f64::from(if axis == 0 {
                    points[index].1
                } else {
                    points[index].2
                })
            };
            let slope = (y(index + 1) - y(index)) / right - (y(index) - y(index - 1)) / left;
            *value = (6.0 * slope - left * previous[axis]) / diagonal;
        }
    }
    for index in (1..points.len() - 1).rev() {
        let next = second[index + 1];
        for (value, next) in second[index].iter_mut().zip(next) {
            *value -= upper[index] * next;
        }
    }
    second
}
