// SPDX-License-Identifier: LGPL-3.0-or-later
//! Trim outliers against a median absolute-deviation fit before least squares.

pub(super) fn trim(
    x: &[f64],
    y: &[f64],
    errors: &[f64],
    flags: &mut [bool],
    degree: usize,
    weighted: bool,
) -> Option<()> {
    let points = x
        .iter()
        .zip(y)
        .zip(flags.iter())
        .filter_map(|((&x, &y), &flag)| (!flag).then_some((x, y)))
        .collect::<Vec<_>>();
    let slope = match degree {
        0 => 0.,
        1 => robust_slope(&points)?,
        _ => return None,
    };
    let intercept = median(&mut offsets(&points, slope))?;
    let residuals = x
        .iter()
        .zip(y)
        .zip(errors)
        .map(|((&x, &y), &error)| (y - (slope * x + intercept)) / if weighted { error } else { 1. })
        .collect::<Vec<_>>();
    let limit = 5. * average_deviation(&residuals);
    for (flag, residual) in flags.iter_mut().zip(residuals) {
        *flag |= residual.abs() >= limit;
    }
    Some(())
}

fn robust_slope(points: &[(f64, f64)]) -> Option<f64> {
    let mut pairs = Vec::new();
    for (index, &(first_x, first_y)) in points.iter().enumerate() {
        for &(second_x, second_y) in &points[index + 1..] {
            if first_x != second_x || first_y != second_y {
                pairs.push((second_y - first_y) / (second_x - first_x));
            }
        }
    }
    let estimate = median(&mut pairs)?;
    let width = 10. * average_deviation(&pairs);
    let (mut low, mut high) = (estimate - width, estimate + width);
    for iteration in 0..30 {
        let step = (high - low) / 20.;
        let initial = slope_sign(points, low)?;
        let mut bracketed = false;
        for index in 1..=20 {
            let candidate = low + f64::from(index) * step;
            let sign = slope_sign(points, candidate)?;
            if sign == 0. {
                return Some(candidate);
            }
            if sign != initial {
                (low, high) = (candidate - step, candidate);
                bracketed = true;
                break;
            }
        }
        if !bracketed && iteration == 0 {
            return None;
        }
    }
    Some(0.5 * (low + high))
}

fn slope_sign(points: &[(f64, f64)], slope: f64) -> Option<f64> {
    let intercept = median(&mut offsets(points, slope))?;
    Some(sign(
        points
            .iter()
            .map(|&(x, y)| x * sign(y - slope * x - intercept))
            .sum(),
    ))
}

fn sign(value: f64) -> f64 {
    if value > 0. {
        1.
    } else if value < 0. {
        -1.
    } else {
        0.
    }
}

fn offsets(points: &[(f64, f64)], slope: f64) -> Vec<f64> {
    points.iter().map(|&(x, y)| y - slope * x).collect()
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let even = values.len() % 2 == 0;
    let (lower, middle, _) = values.select_nth_unstable_by(values.len() / 2, f64::total_cmp);
    Some(if even {
        (lower.iter().copied().fold(f64::NEG_INFINITY, f64::max) + *middle) / 2.
    } else {
        *middle
    })
}

fn average_deviation(values: &[f64]) -> f64 {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values.iter().map(|value| (value - mean).abs()).sum::<f64>() / values.len() as f64
}
