// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_numerics::{
    CasacoreLdlt, invert_symmetric_positive_definite, pseudoinvert_normal_matrix,
    solve_symmetric_positive_definite, solve_weighted_least_squares,
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

struct NormalEquations {
    curvature: Vec<f64>,
    gradient: Vec<f64>,
    score: f64,
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
    let mut equations = normal_equations(curve, &best, &free, samples);
    let mut damping = 0.001;
    let mut increase = 2.;
    for iteration in 0..ITERATIONS - 1 {
        if equations
            .gradient
            .iter()
            .all(|value| value.abs() <= EPSILON)
        {
            return Some(moment_correction(curve, samples, &free, best));
        }
        let mut damped = equations.curvature.clone();
        for index in 0..count {
            damped[index * (count + 1)] *= 1. + damping;
        }
        let step = solve_symmetric_positive_definite(&damped, &equations.gradient)?;
        let trial = add(&best, &step);
        if norm(&step) <= EPSILON * (norm(&best) + EPSILON) {
            return Some(moment_correction(curve, samples, &free, trial));
        }
        let candidate = normal_equations(curve, &trial, &free, samples);
        let predicted = step
            .iter()
            .enumerate()
            .map(|(index, &step)| {
                step * (damping * step * candidate.curvature[index * (count + 1)]
                    + candidate.gradient[index])
            })
            .sum::<f64>();
        if candidate.score < equations.score && predicted > 0. {
            best = trial;
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
    let equations = normal_equations(curve, &values, free, samples);
    if let Some(correction) =
        solve_symmetric_positive_definite(&equations.curvature, &equations.gradient)
    {
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
    if !allow_underdetermined && !free.is_empty() {
        return Some(solve_image_profile(curve, samples, &free, values));
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
        let NormalEquations {
            curvature: mut normal,
            gradient: rhs,
            ..
        } = normal_equations(curve, &values, &free, samples);
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

fn solve_image_profile(
    curve: &Curve,
    samples: &[Sample],
    free: &[usize],
    mut values: Vec<f64>,
) -> Solution {
    // ImageProfileFitter uses SpectralFit's double-precision LM policy:
    // stopping on an absolute gradient or a relative parameter increment,
    // then taking one undamped correction for the final estimate.
    const EPSILON: f64 = 1e-8;
    let components = curve
        .lines
        .iter()
        .enumerate()
        .filter(|&(index, line)| {
            line.multiplet.is_none()
                || index == 0
                || line.multiplet != curve.lines[index - 1].multiplet
        })
        .count()
        + usize::from(curve.polynomial.is_some())
        + usize::from(curve.spectral.is_some());
    let maximum = 50 + 10 * components;
    let norm = |values: &[f64]| values.iter().map(|value| value * value).sum::<f64>().sqrt();
    // With fixed peaks, every free derivative is a shape derivative and
    // its gradient scales as intensity squared. Retain the amplitude
    // scaling correction for these fits with dim lines.
    let gradient_floor = if curve.fits_only_line_shapes() {
        let intensity = samples
            .iter()
            .fold(0_f64, |peak, sample| peak.max(sample.y.abs()));
        EPSILON * intensity.min(1.).powi(2)
    } else {
        EPSILON
    };
    let mut iterations = 0;
    let mut damping = 0.001;
    let mut increase = 2.;
    let mut equations = normal_equations(curve, &values, free, samples);
    let mut converged = false;
    let stride = free.len() + 1;
    let mut damped = equations.curvature.clone();
    let mut step = equations.gradient.clone();
    let mut trial = values.clone();
    while iterations < maximum {
        iterations += 1;
        if equations
            .gradient
            .iter()
            .all(|gradient| gradient.abs() <= gradient_floor)
        {
            converged = true;
            break;
        }
        damped.copy_from_slice(&equations.curvature);
        for index in 0..free.len() {
            let diagonal = &mut damped[index * stride];
            *diagonal = if *diagonal == 0. {
                1.
            } else {
                *diagonal * (1. + damping)
            };
        }
        let Some(factor) = CasacoreLdlt::factor(&mut damped, free.len(), 0.) else {
            break;
        };
        step.copy_from_slice(&equations.gradient);
        if factor.solve(&mut step).is_none() || !step.iter().all(|value| value.is_finite()) {
            break;
        }
        trial.copy_from_slice(&values);
        for (&index, &delta) in free.iter().zip(&step) {
            trial[index] += delta;
        }
        if curve.valid_widths(&trial) {
            let free_norm = free
                .iter()
                .map(|&index| values[index].powi(2))
                .sum::<f64>()
                .sqrt();
            if norm(&step) <= EPSILON * (free_norm + EPSILON) {
                std::mem::swap(&mut values, &mut trial);
                equations = normal_equations(curve, &values, free, samples);
                converged = true;
                break;
            }
            let candidate = normal_equations(curve, &trial, free, samples);
            let predicted = step
                .iter()
                .enumerate()
                .map(|(index, &step)| {
                    step * (damping * step * candidate.curvature[index * stride]
                        + candidate.gradient[index])
                })
                .sum::<f64>();
            if candidate.score < equations.score && predicted > 0. {
                std::mem::swap(&mut values, &mut trial);
                equations = candidate;
                damping *= 0.3;
                increase = 2.;
                continue;
            }
        }
        damping *= increase;
        increase *= 2.;
        if increase > 1e10 {
            // The reference tests a rejected trial in its next iteration.
            iterations += 1;
            converged = true;
            break;
        }
    }
    converged &= iterations < maximum;
    let mut errors = vec![0.; values.len()];
    if converged {
        converged = finish_image_fit(equations, samples.len(), free, &mut values, &mut errors)
            .is_some()
            && curve.valid_widths(&values);
    }
    Solution {
        values,
        errors,
        iterations,
        converged,
    }
}

fn finish_image_fit(
    equations: NormalEquations,
    samples: usize,
    free: &[usize],
    values: &mut [f64],
    errors: &mut [f64],
) -> Option<()> {
    let NormalEquations {
        curvature,
        gradient,
        score,
    } = equations;
    let (correction, covariance) = undamped_correction(curvature, &gradient)?;
    let improvement = correction
        .iter()
        .zip(gradient)
        .map(|(delta, gradient)| delta * gradient)
        .sum::<f64>();
    let rms = ((score - improvement).max(0.) / (samples - free.len()) as f64).sqrt();
    for ((&parameter, step), variance) in free.iter().zip(correction).zip(covariance) {
        values[parameter] += step;
        errors[parameter] = variance.abs().sqrt() * rms;
    }
    Some(())
}

fn undamped_correction(mut curvature: Vec<f64>, gradient: &[f64]) -> Option<(Vec<f64>, Vec<f64>)> {
    let size = gradient.len();
    for index in 0..size {
        let diagonal = &mut curvature[index * (size + 1)];
        if *diagonal == 0. {
            *diagonal = 1.;
        }
    }
    let mut factors = curvature.clone();
    if let Some(factor) = CasacoreLdlt::factor(&mut factors, size, 0.) {
        let mut correction = gradient.to_vec();
        factor.solve(&mut correction)?;
        let mut covariance = Vec::with_capacity(size);
        let mut column = vec![0.; size];
        for index in 0..size {
            column.fill(0.);
            column[index] = 1.;
            factor.solve(&mut column)?;
            covariance.push(column[index]);
        }
        return Some((correction, covariance));
    }

    // The nonlinear iteration uses a damped full-rank matrix. The final
    // undamped correction can have dependent columns, for example when
    // two fitted peak amplitudes share the same fixed center and width.
    let inverse = pseudoinvert_normal_matrix(&curvature, size)?;
    let correction = inverse
        .chunks_exact(size)
        .map(|row| {
            row.iter()
                .zip(gradient)
                .map(|(&value, &slope)| value * slope)
                .sum::<f64>()
        })
        .collect();
    let covariance = (0..size).map(|index| inverse[index * (size + 1)]).collect();
    Some((correction, covariance))
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
    let equations = normal_equations(curve, values, free, samples);
    errors_from_curvature(
        &equations.curvature,
        values.len(),
        free,
        samples.len(),
        score,
    )
}

fn errors_from_curvature(
    normal: &[f64],
    parameters: usize,
    free: &[usize],
    samples: usize,
    score: f64,
) -> Vec<f64> {
    let mut errors = vec![0.; parameters];
    let variance = score / (samples - free.len()) as f64;
    let covariance = invert_symmetric_positive_definite(normal, free.len());
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
) -> NormalEquations {
    let count = free.len();
    let mut normal = vec![0.; count * count];
    let mut rhs = vec![0.; count];
    let mut slopes = vec![0.; parameters.len()];
    let mut score = 0.;
    for row in samples {
        let residual = row.y - curve.fitted_value(parameters, row.x);
        score += row.weight * residual.powi(2);
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
    NormalEquations {
        curvature: normal,
        gradient: rhs,
        score,
    }
}
