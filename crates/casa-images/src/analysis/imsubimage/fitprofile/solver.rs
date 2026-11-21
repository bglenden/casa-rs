// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_numerics::{
    invert_symmetric_positive_definite, solve_symmetric_positive_definite,
    solve_weighted_least_squares,
};

use super::curve::Curve;

#[derive(Clone, Copy)]
pub(super) struct Sample {
    pub(super) x: f64,
    pub(super) y: f64,
    pub(super) weight: f64,
}

pub(super) struct Solution {
    pub(super) values: Vec<f64>,
    pub(super) errors: Vec<f64>,
    pub(super) iterations: usize,
    pub(super) converged: bool,
}

pub(super) fn solve(curve: &Curve, samples: &[Sample]) -> Option<Solution> {
    let free: Vec<_> = curve
        .fixed
        .iter()
        .enumerate()
        .filter_map(|(index, &fixed)| (!fixed).then_some(index))
        .collect();
    if samples.len() <= free.len() || samples.is_empty() {
        return None;
    }
    let mut values = curve.parameters.clone();
    let mut score = cost(curve, &values, samples);
    if !score.is_finite() {
        return None;
    }
    if curve.is_linear() && !free.is_empty() {
        let values = linear_solution(curve, values, &free, samples)?;
        let score = cost(curve, &values, samples);
        let errors = uncertainty(curve, &values, &free, samples, score);
        return Some(Solution {
            values,
            errors,
            iterations: 1,
            converged: true,
        });
    }
    let mut damping = 0.001;
    let mut iterations = 0;
    let power = samples
        .iter()
        .map(|row| row.weight * row.y * row.y)
        .sum::<f64>();
    let precision = f64::EPSILON * power.max(f64::MIN_POSITIVE);
    let mut converged = free.is_empty() || score <= precision;
    let mut candidate = values.clone();
    while !converged && iterations < 200 && damping <= 1e12 {
        iterations += 1;
        let (mut normal, rhs) = normal_equations(curve, &values, &free, samples);
        for index in 0..free.len() {
            let diagonal = &mut normal[index * (free.len() + 1)];
            *diagonal += damping * diagonal.abs() + 1e-18;
        }
        if let Some(delta) = solve_symmetric_positive_definite(&normal, &rhs) {
            candidate.copy_from_slice(&values);
            for (&index, step) in free.iter().zip(&delta) {
                candidate[index] += step;
            }
            if curve.valid_widths(&candidate) {
                let next = cost(curve, &candidate, samples);
                if next < score {
                    values.copy_from_slice(&candidate);
                    converged = next <= precision || score - next <= 1e-9 * score;
                    score = next;
                    damping *= 0.25;
                    continue;
                }
                if damping <= 0.001 && (next - score).abs() <= 1e-9 * score {
                    converged = true;
                    continue;
                }
            }
        }
        damping *= 10.;
    }
    let errors = if converged && !free.is_empty() {
        uncertainty(curve, &values, &free, samples, score)
    } else {
        vec![0.; values.len()]
    };
    Some(Solution {
        values,
        errors,
        iterations,
        converged,
    })
}

fn linear_solution(
    curve: &Curve,
    mut values: Vec<f64>,
    free: &[usize],
    samples: &[Sample],
) -> Option<Vec<f64>> {
    let mut slopes = vec![0.; values.len()];
    let mut scale = vec![0_f64; free.len()];
    let mut rows: Vec<_> = samples
        .iter()
        .map(|sample| {
            curve.gradient(&values, sample.x, &mut slopes);
            let basis = free
                .iter()
                .zip(&mut scale)
                .map(|(&parameter, scale)| {
                    let slope = slopes[parameter];
                    *scale = scale.max(slope.abs());
                    slope
                })
                .collect::<Vec<_>>();
            let residual = sample.y - curve.fitted_value(&values, sample.x);
            (basis, residual, sample.weight)
        })
        .collect();
    if scale.iter().any(|&value| value == 0. || !value.is_finite()) {
        return None;
    }
    for (basis, _, _) in &mut rows {
        for (value, scale) in basis.iter_mut().zip(&scale) {
            *value /= scale;
        }
    }
    let solution = solve_weighted_least_squares(&rows, free.len())?;
    for ((&parameter, step), scale) in free.iter().zip(solution).zip(scale) {
        values[parameter] += step / scale;
    }
    Some(values)
}

fn uncertainty(
    curve: &Curve,
    values: &[f64],
    free: &[usize],
    samples: &[Sample],
    score: f64,
) -> Vec<f64> {
    let mut errors = vec![0.; values.len()];
    let (normal, _) = normal_equations(curve, values, free, samples);
    let variance = score / (samples.len() - free.len()) as f64;
    let covariance = invert_symmetric_positive_definite(&normal, free.len());
    for (column, &parameter) in free.iter().enumerate() {
        errors[parameter] = covariance.as_ref().map_or(f64::NAN, |covariance| {
            (covariance[column * (free.len() + 1)] * variance).sqrt()
        });
    }
    errors
}

fn cost(curve: &Curve, parameters: &[f64], samples: &[Sample]) -> f64 {
    samples
        .iter()
        .map(|row| row.weight * (row.y - curve.fitted_value(parameters, row.x)).powi(2))
        .sum()
}

fn normal_equations(
    curve: &Curve,
    parameters: &[f64],
    free: &[usize],
    samples: &[Sample],
) -> (Vec<f64>, Vec<f64>) {
    let count = free.len();
    let mut normal = vec![0.; count * count];
    let mut rhs = vec![0.; count];
    let mut slopes = vec![0.; parameters.len()];
    for row in samples {
        let residual = row.y - curve.fitted_value(parameters, row.x);
        curve.gradient(parameters, row.x, &mut slopes);
        for (i, &parameter) in free.iter().enumerate() {
            let weighted = row.weight * slopes[parameter];
            rhs[i] += weighted * residual;
            for (j, &parameter) in free[..=i].iter().enumerate() {
                normal[i * count + j] += weighted * slopes[parameter];
            }
        }
    }
    for i in 0..count {
        for j in 0..i {
            normal[j * count + i] = normal[i * count + j];
        }
    }
    (normal, rhs)
}
