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

// MomentCalcBase uses casacore's Float NonLinearFitLM. Its iteration limit
// and final undamped correction affect whether low-SNR profiles are masked.
pub(super) fn solve_moment_gaussian(curve: &Curve, samples: &[Sample]) -> Option<Vec<f64>> {
    const ITERATIONS: usize = 50;
    const EPSILON: f64 = 1e-6;
    let count = curve.parameters.len();
    if samples.len() < count {
        return None;
    }
    let free = (0..count).collect::<Vec<_>>();
    let norm = |values: &[f64]| values.iter().map(|value| value * value).sum::<f64>().sqrt();
    let add = |base: &[f64], step: &[f64]| {
        base.iter()
            .zip(step)
            .map(|(&value, &step)| f64::from((value + step) as f32))
            .collect::<Vec<_>>()
    };
    let mut best = curve.parameters.clone();
    let mut score = cost(curve, &best, samples);
    let mut equations = normal_equations(curve, &best, &free, samples);
    let mut damping = 0.001;
    let mut increase = 2.;
    for iteration in 0..ITERATIONS - 1 {
        let (normal, gradient) = &equations;
        if gradient.iter().all(|value| value.abs() <= EPSILON) {
            return Some(moment_correction(curve, samples, &free, best));
        }
        let mut damped = normal.clone();
        for index in 0..count {
            damped[index * (count + 1)] *= 1. + damping;
        }
        let step = solve_symmetric_positive_definite(&damped, gradient)?;
        let trial = add(&best, &step);
        if norm(&step) <= EPSILON * (norm(&best) + EPSILON) {
            return Some(moment_correction(curve, samples, &free, trial));
        }
        let next = cost(curve, &trial, samples);
        let candidate = normal_equations(curve, &trial, &free, samples);
        let predicted = step
            .iter()
            .enumerate()
            .map(|(index, &step)| {
                step * (damping * step * candidate.0[index * (count + 1)] + candidate.1[index])
            })
            .sum::<f64>();
        if next < score && predicted > 0. {
            best = trial;
            score = next;
            equations = candidate;
            damping *= 0.3;
            increase = 2.;
        } else {
            damping *= increase;
            increase *= 2.;
            if increase > 1e10 && iteration + 2 < ITERATIONS {
                return Some(moment_correction(curve, samples, &free, best));
            }
        }
    }
    None
}

fn moment_correction(
    curve: &Curve,
    samples: &[Sample],
    free: &[usize],
    mut values: Vec<f64>,
) -> Vec<f64> {
    let (normal, gradient) = normal_equations(curve, &values, free, samples);
    if let Some(correction) = solve_symmetric_positive_definite(&normal, &gradient) {
        for (value, step) in values.iter_mut().zip(correction) {
            *value = f64::from((*value + step) as f32);
        }
    }
    values
}

pub(super) fn solve(curve: &Curve, samples: &[Sample]) -> Option<Solution> {
    solve_profile(curve, samples, false)
}

pub(super) fn solve_channel_profiles(curve: &Curve, samples: &[Sample]) -> Option<Solution> {
    solve_profile(curve, samples, true)
}

fn solve_profile(
    curve: &Curve,
    samples: &[Sample],
    allow_underdetermined: bool,
) -> Option<Solution> {
    let free: Vec<_> = curve
        .fixed
        .iter()
        .enumerate()
        .filter_map(|(index, &fixed)| (!fixed).then_some(index))
        .collect();
    let determined = samples.len() > free.len();
    if !determined && !allow_underdetermined {
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
    let precision = 16. * f64::EPSILON * power.max(f64::MIN_POSITIVE);
    let mut converged = free.is_empty() || score <= precision;
    let mut candidate = values.clone();
    while !converged && iterations < 200 && damping <= 1e12 {
        iterations += 1;
        let (mut normal, rhs) = normal_equations(curve, &values, &free, samples);
        let stride = free.len() + 1;
        let stationary = rhs.iter().enumerate().all(|(index, gradient)| {
            let curvature = normal[index * stride];
            curvature > 0. && gradient.abs() <= 128. * f64::EPSILON * (power * curvature).sqrt()
        });
        if stationary {
            converged = true;
            break;
        }
        for index in 0..free.len() {
            normal[index * stride] *= 1. + damping;
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
    // No residual degrees of freedom are available for a covariance estimate.
    let errors = if converged && determined && !free.is_empty() {
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
    if values.len() == 1 && curve.polynomial.is_some() {
        // The normal equation for a constant is the weighted mean.
        let (sum, weight) = samples.iter().fold((0., 0.), |(sum, weight), sample| {
            (sum + sample.weight * sample.y, weight + sample.weight)
        });
        values[0] = sum / weight;
        return (weight > 0. && values[0].is_finite()).then_some(values);
    }
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
