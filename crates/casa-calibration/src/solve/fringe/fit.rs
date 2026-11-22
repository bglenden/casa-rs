// SPDX-License-Identifier: LGPL-3.0-or-later
//! Refine reference-baseline FFT estimates with all connected baselines.

use std::f64::consts::TAU;

use super::search::FringePeak;
use super::{FringeBaseline, FringeParameters, FringeSample};
use crate::solve::normal::{NormalBand, Term};

// Rates occupy a much smaller numerical range than coefficients in radians
// and nanoseconds. Solve in comparable units and convert at the table boundary.
const SCALE: [f64; 4] = [1., 1., 1e-12, 1e6];

pub(super) struct FitOptions {
    pub(super) reference_antenna: usize,
    pub(super) first_time_seconds: f64,
    pub(super) first_frequency_hz: f64,
    pub(super) active_parameters: [bool; 4],
    pub(super) iterations: usize,
}

pub(super) fn refine(
    baselines: &[FringeBaseline],
    estimates: &mut [Option<FringePeak>],
    options: &FitOptions,
) -> Option<()> {
    let mut state = Vec::new();
    let indices = estimates
        .iter()
        .enumerate()
        .map(|(antenna, estimate)| {
            estimate
                .as_ref()
                .filter(|estimate| estimate.active)
                .map(|estimate| {
                    let values = estimate.parameters.values();
                    std::array::from_fn(|parameter| {
                        (antenna != options.reference_antenna
                            && options.active_parameters[parameter])
                            .then(|| {
                                let index = state.len();
                                state.push(f64::from(values[parameter] as f32) / SCALE[parameter]);
                                index
                            })
                    })
                })
        })
        .collect::<Vec<_>>();
    if state.is_empty() {
        return Some(());
    }

    let initial = state.clone();
    let mut normal = NormalBand::new(state.len(), state.len())?;
    visit_samples(
        baselines,
        &indices,
        |sample, indices, design| {
            let (terms, count) = terms(indices, design);
            let weight = f64::from(sample.weight);
            normal.accumulate(&terms[..count], 0., weight);
        },
        options,
    );
    let objective = Objective {
        baselines,
        indices: &indices,
        options,
    };
    optimize(&objective, &mut normal, &mut state)?;
    let precision = signal_to_noise(baselines, &indices, &state, &initial, options);
    for (estimate, indices) in estimates.iter_mut().zip(&indices) {
        if let (Some(estimate), Some(indices)) = (estimate, indices)
            && let Some(phase) = indices[0]
        {
            estimate.parameters = FringeParameters::from_values(std::array::from_fn(|parameter| {
                indices[parameter].map_or(0., |index| state[index] * SCALE[parameter])
            }));
            estimate.snr = precision[phase];
        }
    }
    Some(())
}

type ParameterIndices = [Option<usize>; 4];

struct Objective<'a> {
    baselines: &'a [FringeBaseline],
    indices: &'a [Option<ParameterIndices>],
    options: &'a FitOptions,
}

impl Objective<'_> {
    fn evaluate(&self, state: &[f64], mut right_hand_side: Option<&mut NormalBand>) -> f64 {
        let mut cost = 0.;
        visit_samples(
            self.baselines,
            self.indices,
            |sample, indices, design| {
                let (terms, count) = terms(indices, design);
                let terms = &terms[..count];
                let residual = f64::from(sample.visibility.arg()) - predict(terms, state);
                let weight = f64::from(sample.weight);
                cost += 2. * weight * (0.5 * residual).sin().powi(2);
                if let Some(normal) = right_hand_side.as_deref_mut() {
                    normal.add_rhs(terms, residual.sin(), weight);
                }
            },
            self.options,
        );
        cost
    }
}

fn optimize(
    objective: &Objective<'_>,
    normal: &mut NormalBand,
    state: &mut Vec<f64>,
) -> Option<()> {
    let mut scratch = NormalBand::new(state.len(), state.len())?;
    let mut trial = state.clone();
    let mut damping = 0.001;
    let mut cost = objective.evaluate(state, Some(normal));
    // CASA always attempts an initial step, including when niter is zero.
    // Subsequent iterations stop at its fractional objective tolerance.
    for iteration in 0..objective.options.iterations.max(1) {
        let mut growth = 2.;
        let mut accepted = false;
        for _ in 0..16 {
            let (step, reduction) = normal.damped_step(&mut scratch, damping)?;
            for ((value, &previous), &delta) in trial.iter_mut().zip(state.iter()).zip(step) {
                *value = previous + delta;
            }
            let trial_cost = objective.evaluate(&trial, None);
            let improvement = cost - trial_cost;
            if improvement > 0. && reduction > 0. {
                let ratio = improvement / reduction;
                damping *= (1. - (2. * ratio - 1.).powi(3)).max(1. / 3.);
                std::mem::swap(state, &mut trial);
                if iteration >= 1 && improvement < 5e-6 * cost {
                    return Some(());
                }
                normal.clear_rhs();
                cost = objective.evaluate(state, Some(normal));
                accepted = true;
                break;
            }
            damping *= growth;
            growth *= 2.;
        }
        if !accepted {
            break;
        }
    }
    Some(())
}

fn visit_samples(
    baselines: &[FringeBaseline],
    indices: &[Option<ParameterIndices>],
    mut visit: impl FnMut(&FringeSample, [ParameterIndices; 2], [f64; 4]),
    options: &FitOptions,
) {
    for baseline in baselines {
        let [Some(left), Some(right)] = baseline.antennas.map(|antenna| indices[antenna]) else {
            continue;
        };
        for sample in baseline.samples.iter().filter(|sample| sample.weight > 0.) {
            visit(sample, [left, right], design(sample, options));
        }
    }
}

fn design(sample: &FringeSample, options: &FitOptions) -> [f64; 4] {
    let frequency = sample.frequency_hz as f32;
    let [low, high] = sample.window_edges_hz;
    // The dispersive basis drops the straight line through each window's ends.
    let curvature = (frequency - low - high) / (low * high);
    let dispersion = ((1. / f64::from(frequency) + f64::from(curvature)) * TAU * 1e6) as f32;
    [
        1.,
        TAU * (sample.frequency_hz - options.first_frequency_hz) * 1e-9,
        TAU * (sample.time_seconds - options.first_time_seconds)
            * options.first_frequency_hz
            * SCALE[2],
        f64::from(dispersion) * SCALE[3],
    ]
}

fn terms(indices: [ParameterIndices; 2], design: [f64; 4]) -> ([Term; 8], usize) {
    let mut terms = [Term {
        index: 0,
        coefficient: 0.,
    }; 8];
    let mut count = 0;
    for (indices, sign) in indices.into_iter().zip([1., -1.]) {
        for (index, coefficient) in indices.into_iter().zip(design) {
            if let Some(index) = index {
                terms[count] = Term {
                    index,
                    coefficient: sign * coefficient,
                };
                count += 1;
            }
        }
    }
    (terms, count)
}

fn predict(terms: &[Term], state: &[f64]) -> f64 {
    terms
        .iter()
        .map(|term| term.coefficient * state[term.index])
        .sum()
}

fn signal_to_noise(
    baselines: &[FringeBaseline],
    indices: &[Option<ParameterIndices>],
    state: &[f64],
    initial: &[f64],
    options: &FitOptions,
) -> Vec<f32> {
    let size = state.len();
    let mut covariance = vec![0.; size * size];
    let mut observations = 0;
    let mut total_weight = 0.;
    let mut squared_error = 0.;
    visit_samples(
        baselines,
        indices,
        |sample, antenna_indices, design| {
            let (terms, n) = terms(antenna_indices, design);
            let observed = f64::from(sample.visibility.arg());
            let weight = f64::from(sample.weight);
            let residual = observed - predict(&terms[..n], state);
            squared_error += 4. * weight * (0.5 * residual).sin().powi(2);
            total_weight += weight;
            observations += 1;
            let curvature = weight * (observed - predict(&terms[..n], initial)).cos();
            let [_, delay, rate, dispersion] = design;
            // CASA evaluates the AIPS phase covariance at the initial FFT estimate.
            let local = [
                [1., delay, rate, dispersion * delay],
                [
                    delay,
                    delay * delay,
                    rate * delay,
                    dispersion * delay * delay,
                ],
                [
                    rate,
                    rate * delay,
                    rate * rate * delay,
                    rate * dispersion * delay * delay,
                ],
                [
                    dispersion * delay,
                    dispersion * delay * delay,
                    rate * dispersion * delay * delay,
                    dispersion * dispersion * (rate / SCALE[2]) * delay * delay,
                ],
            ];
            for (left, left_sign) in antenna_indices.into_iter().zip([1., -1.]) {
                for (right, right_sign) in antenna_indices.into_iter().zip([1., -1.]) {
                    for (left_parameter, left) in left.into_iter().enumerate() {
                        for (right_parameter, right) in right.into_iter().enumerate() {
                            if let (Some(row), Some(column)) = (left, right) {
                                covariance[row * size + column] += curvature
                                    * left_sign
                                    * right_sign
                                    * local[left_parameter][right_parameter];
                            }
                        }
                    }
                }
            }
        },
        options,
    );
    let mut snr = vec![0.; size];
    if observations <= size || total_weight <= 0. {
        return snr;
    }
    let Some(pivot) = factor(&mut covariance, size) else {
        return snr;
    };
    let sigma2 = squared_error.max(1e-25) / (observations - size) as f64 * observations as f64
        / total_weight;
    for phase in indices.iter().flatten().filter_map(|indices| indices[0]) {
        let variance = inverse_entry(&covariance, &pivot, phase);
        let error = (0.5 * sigma2 * variance).sqrt();
        let error = if error > 9999.999 { 9999.999 } else { error };
        snr[phase] = if error > 1e-20 {
            error.recip() as f32
        } else {
            error as f32
        };
    }
    snr
}

fn factor(matrix: &mut [f64], size: usize) -> Option<Vec<usize>> {
    let mut permutation = (0..size).collect::<Vec<_>>();
    for column in 0..size {
        let index = |row: usize| row * size + column;
        let pivot = (column..size).max_by(|&left, &right| {
            matrix[index(left)]
                .abs()
                .total_cmp(&matrix[index(right)].abs())
        })?;
        let diagonal = matrix[index(pivot)];
        if !diagonal.is_finite() || diagonal.abs() < f64::EPSILON {
            return None;
        }
        permutation.swap(column, pivot);
        for col in 0..size {
            matrix.swap(column * size + col, pivot * size + col);
        }
        for row in column + 1..size {
            let scale = matrix[index(row)] / diagonal;
            matrix[index(row)] = scale;
            for col in column + 1..size {
                matrix[row * size + col] -= scale * matrix[column * size + col];
            }
        }
    }
    Some(permutation)
}

fn inverse_entry(matrix: &[f64], permutation: &[usize], index: usize) -> f64 {
    let size = permutation.len();
    let mut rhs = permutation
        .iter()
        .map(|&row| f64::from(row == index))
        .collect::<Vec<_>>();
    for row in 0..size {
        let correction = (0..row)
            .map(|column| matrix[row * size + column] * rhs[column])
            .sum::<f64>();
        rhs[row] -= correction;
    }
    for row in (index..size).rev() {
        let correction = (row + 1..size)
            .map(|column| matrix[row * size + column] * rhs[column])
            .sum::<f64>();
        rhs[row] = (rhs[row] - correction) / matrix[row * size + row];
    }
    rhs[index]
}
