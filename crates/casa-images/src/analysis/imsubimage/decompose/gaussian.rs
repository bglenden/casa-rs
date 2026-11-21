// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{LN_2, PI};

use casa_numerics::solve_symmetric_positive_definite;

use super::points::{Pixels, neighborhood};

pub(super) type Parameters = [f64; 9];

pub(super) struct FitOptions {
    pub(super) maximum_rms: f64,
    pub(super) retries: i64,
    pub(super) iterations: usize,
    pub(super) criterion: f64,
}

pub(super) fn estimate(pixels: &Pixels, points: &[usize]) -> Parameters {
    let mut parameters: Parameters = [0.; 9];
    let rank = pixels.rank();
    let mut total = 0.;
    let mut first = [0.; 3];
    for &point in points {
        let value = f64::from(pixels.samples[point].value);
        parameters[0] = parameters[0].max(value);
        total += value;
        for (axis, sum) in first.iter_mut().enumerate().take(rank) {
            *sum += value * pixels.coordinate(pixels.samples[point].index, axis) as f64;
        }
    }
    if total == 0. {
        let point = pixels.position(points[0]);
        for axis in 0..rank {
            parameters[1 + axis] = point[axis] as f64;
            parameters[1 + rank + axis] = 1.;
        }
        return parameters;
    }
    let mean = first.map(|sum| sum / total);
    parameters[1..=rank].copy_from_slice(&mean[..rank]);
    let mut covariance = [[0.; 3]; 3];
    for &point in points {
        let value = f64::from(pixels.samples[point].value);
        let index = pixels.samples[point].index;
        let centered: [f64; 3] = std::array::from_fn(|axis| {
            if axis < rank {
                pixels.coordinate(index, axis) as f64 - mean[axis]
            } else {
                0.
            }
        });
        for i in 0..rank {
            for j in i..rank {
                covariance[i][j] += value * centered[i] * centered[j] / total;
            }
        }
    }
    let xx = covariance[0][0];
    let yy = covariance[1][1];
    let xy = covariance[0][1];
    let angle = 0.5 * (2. * xy / (xx - yy)).atan();
    let angle = if angle.is_nan() { 0. } else { angle };
    if rank == 2 {
        let width = (2.84 * yy.sqrt()).max(1.);
        let ratio = (xx / yy).sqrt();
        parameters[3] = width;
        parameters[4] = if ratio.is_nan() {
            width.recip()
        } else {
            ratio.max(width.recip())
        };
        parameters[5] = angle;
    } else {
        let zz = covariance[2][2];
        parameters[4] = (2.84 * xx.sqrt()).max(1.);
        parameters[5] = (2.84 * yy.sqrt()).max(1.);
        parameters[6] = (2.84 * zz.sqrt()).max(1.);
        parameters[7] = angle;
        let phi = -0.5
            * (2. * covariance[0][2] / ((xx - zz) * angle.cos() + (yy - zz) * angle.sin())).atan();
        parameters[8] = if phi.is_nan() { 0. } else { phi };
    }
    parameters
}

pub(super) fn peak_estimates(pixels: &Pixels, group: &[usize]) -> Vec<Parameters> {
    let rank = pixels.rank();
    let offsets = neighborhood(rank, 2);
    group
        .iter()
        .copied()
        .filter(|&point| {
            pixels
                .neighbors(point, &offsets)
                .all(|other| pixels.samples[other].value <= pixels.samples[point].value)
        })
        .map(|point| {
            let value = pixels.samples[point].value;
            let center = pixels.position(point);
            let width: [f64; 3] = std::array::from_fn(|axis| {
                if axis < rank {
                    width_at_peak(pixels, &center, axis, value)
                } else {
                    1.
                }
            });
            let mut parameters: Parameters = [0.; 9];
            parameters[0] = f64::from(value);
            for axis in 0..rank {
                parameters[axis + 1] = center[axis] as f64;
            }
            if rank == 2 {
                parameters[3] = width[1];
                parameters[4] = width[0] / width[1];
            } else {
                parameters[4..7].copy_from_slice(&width);
            }
            parameters
        })
        .collect()
}

fn width_at_peak(pixels: &Pixels, center: &[usize], axis: usize, peak: f32) -> f64 {
    let threshold = peak * 0.25;
    let side = |direction| {
        let mut point = center.to_vec();
        let mut last = peak;
        let mut count = 0usize;
        while let Some(next) = point[axis]
            .checked_add_signed(direction)
            .filter(|&next| next < pixels.axis_len(axis))
        {
            count += 1;
            point[axis] = next;
            let value = pixels.position_value(&point);
            if value < threshold {
                return Some(count as f64 - f64::from((threshold - value) / (last - value)));
            }
            if value > last {
                return Some(count as f64);
            }
            last = value;
        }
        None
    };
    let width = match (side(-1), side(1)) {
        (Some(a), Some(b)) => (a + b) / 2.,
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => pixels.axis_len(axis) as f64,
    };
    if width.is_finite() && width > 0. {
        width
    } else {
        1.
    }
}

#[derive(Clone, Copy)]
struct Observation {
    position: [f64; 3],
    value: f64,
}

pub(super) fn fit(
    pixels: &Pixels,
    points: &[usize],
    mut estimates: Vec<Parameters>,
    options: &FitOptions,
) -> Vec<Parameters> {
    if estimates.is_empty() || points.is_empty() {
        return estimates;
    }
    let rank = pixels.rank();
    let sample = points
        .iter()
        .map(|&point| {
            let index = pixels.samples[point].index;
            let position = std::array::from_fn(|i| {
                if i < rank {
                    pixels.coordinate(index, i) as f64
                } else {
                    0.
                }
            });
            Observation {
                position,
                value: f64::from(pixels.samples[point].value),
            }
        })
        .collect::<Vec<_>>();
    if estimates.len() <= 32 {
        let mut solution = optimize(&sample, &estimates, rank, options);
        if solution.is_empty() {
            solution = estimates;
        }
        return solution;
    }

    // Fit a crowded island in bounded blocks. Subtract the frozen components
    // before fitting each block against the active samples.
    for first in (0..estimates.len()).step_by(32) {
        let last = estimates.len().min(first + 32);
        let residual = sample
            .iter()
            .copied()
            .map(|mut row| {
                for component in estimates[..first].iter().chain(&estimates[last..]) {
                    row.value -= gaussian(component, rank, row.position);
                }
                row
            })
            .collect::<Vec<_>>();
        let values = optimize(&residual, &estimates[first..last], rank, options);
        if !values.is_empty() {
            estimates[first..last].copy_from_slice(&values);
        }
    }
    estimates
}

fn optimize(
    observations: &[Observation],
    seeds: &[Parameters],
    rank: usize,
    options: &FitOptions,
) -> Vec<Parameters> {
    let count = rank * 3;
    let mut variables = (0..seeds.len())
        .flat_map(|i| (0..count).map(move |j| (i, j)))
        .collect::<Vec<_>>();
    if observations.len() <= seeds.len() {
        variables.retain(|&(i, j)| j == 0 && i + observations.len() >= seeds.len());
    }

    let evaluate = |model: &[Parameters], row: Observation| {
        model
            .iter()
            .map(|p| gaussian(p, rank, row.position))
            .sum::<f64>()
    };
    let error = |model: &[Parameters]| {
        observations
            .iter()
            .map(|&row| (row.value - evaluate(model, row)).powi(2))
            .sum::<f64>()
    };
    let mut best = seeds.to_vec();
    let mut best_cost = error(&best);
    let retries = if options.retries < 0 {
        rank * seeds.len()
    } else {
        options.retries as usize
    };
    for attempt in 0..=retries {
        let mut model = seeds.to_vec();
        if attempt > 0 {
            let component = (attempt - 1) % model.len();
            let factor = [0.8, 1.25, 0.55, 1.55][((attempt - 1) / model.len()) % 4];
            let width_count = if rank == 2 { 1 } else { rank };
            for width in &mut model[component][rank + 1..rank + 1 + width_count] {
                *width *= factor;
            }
        }
        let mut score = error(&model);
        let mut damping = 0.001;
        let mut candidate = model.clone();
        let mut slope = vec![0.; variables.len()];
        for _ in 0..options.iterations {
            let size = variables.len();
            let mut gram = vec![0.; size * size];
            let mut rhs = vec![0.; size];
            for &row in observations {
                let residual = row.value - evaluate(&model, row);
                for (column, &(i, j)) in variables.iter().enumerate() {
                    let mut p = model[i];
                    let delta = p[j].abs().max(1.) * 1e-5;
                    p[j] += delta;
                    let plus = gaussian(&p, rank, row.position);
                    p[j] -= 2. * delta;
                    slope[column] = (plus - gaussian(&p, rank, row.position)) / (2. * delta);
                }
                for i in 0..size {
                    rhs[i] += slope[i] * residual;
                    for j in i..size {
                        gram[i * size + j] += slope[i] * slope[j];
                    }
                }
            }
            for i in 0..size {
                gram[i * size + i] *= 1. + damping;
                gram[i * size + i] += 1e-18;
                for j in i + 1..size {
                    gram[j * size + i] = gram[i * size + j];
                }
            }
            let Some(step) = solve_symmetric_positive_definite(&gram, &rhs) else {
                break;
            };
            candidate.copy_from_slice(&model);
            for (&(i, j), delta) in variables.iter().zip(step) {
                candidate[i][j] += delta;
            }
            let widths = if rank == 2 { 3..5 } else { 4..7 };
            if candidate
                .iter()
                .any(|p| p[widths.clone()].iter().any(|&width| width <= 0.))
            {
                damping *= 10.;
                continue;
            }
            let cost = error(&candidate);
            if cost < score {
                model.copy_from_slice(&candidate);
                let converged =
                    (score - cost) <= options.criterion.powi(2).max(1e-16) * score.max(1.);
                score = cost;
                damping *= 0.25;
                if converged {
                    break;
                }
            } else {
                damping *= 10.;
                if damping > 1e12 {
                    break;
                }
            }
        }
        if score < best_cost {
            best_cost = score;
            best = model;
        }
        if (best_cost / observations.len() as f64).sqrt() <= options.maximum_rms.max(1e-6) {
            break;
        }
    }
    for component in &mut best {
        if rank == 2 {
            if component[4] > 1. {
                component[3] *= component[4];
                component[4] = component[4].recip();
                component[5] += PI / 2.;
            }
            component[5] = component[5].rem_euclid(PI);
            if (component[5] - PI).abs() < 1e-7 {
                component[5] = 0.;
            }
        }
    }
    best
}

fn gaussian(p: &Parameters, rank: usize, position: [f64; 3]) -> f64 {
    let x = position[0] - p[1];
    let y = position[1] - p[2];
    let shape = if rank == 2 {
        let (sin, cos) = p[5].sin_cos();
        let long = (sin * x + cos * y) / p[3];
        let short = (cos * x - sin * y) / (p[3] * p[4]);
        long * long + short * short
    } else {
        let z = position[2] - p[3];
        let (st, ct) = p[7].sin_cos();
        let (sp, cp) = p[8].sin_cos();
        let first = (ct * cp * x + st * y - ct * sp * z) / p[4];
        let second = (-st * cp * x + ct * y + st * sp * z) / p[5];
        let third = (sp * x + cp * z) / p[6];
        first * first + second * second + third * third
    };
    p[0] * (-4. * LN_2 * shape).exp()
}
