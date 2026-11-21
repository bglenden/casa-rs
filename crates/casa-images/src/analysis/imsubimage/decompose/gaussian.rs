// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{LN_2, PI};

use casa_numerics::solve_symmetric_positive_definite;

use super::points::{Pixels, neighborhood};
use super::{ImageError, PixelSections, SubimagePlan};

pub(in crate::analysis::imsubimage) type Parameters = [f64; 9];

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

pub(super) fn peak_estimates(
    pixels: &Pixels,
    group: &[usize],
    reader: &PixelSections<'_, f32>,
    plan: &SubimagePlan,
) -> Result<Vec<Parameters>, ImageError> {
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
            let mut width = [1.; 3];
            for (axis, entry) in width.iter_mut().enumerate().take(rank) {
                *entry = width_at_peak(reader, plan, &center, axis, value)?;
            }
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
            Ok(parameters)
        })
        .collect()
}

fn width_at_peak(
    reader: &PixelSections<'_, f32>,
    plan: &SubimagePlan,
    center: &[usize],
    axis: usize,
    peak: f32,
) -> Result<f64, ImageError> {
    let extent = plan.shape[axis];
    let source_axis = plan.kept_axes[axis];
    let rank = plan.selection.start.len();
    let unit = vec![1; rank];
    let stride = plan.selection.stride.as_deref().unwrap_or(&unit);
    let mut start = plan.selection.start.clone();
    for (&axis, &index) in plan.kept_axes.iter().zip(center) {
        start[axis] += index * stride[axis];
    }
    let mut shape = unit.clone();
    let threshold = peak * 0.25;
    let mut side = |forward: bool| {
        let limit = if forward {
            extent - 1 - center[axis]
        } else {
            center[axis]
        };
        let mut last = peak;
        let block = (128 / stride[source_axis]).max(1);
        for offset in (1..=limit).step_by(block) {
            let length = (limit + 1 - offset).min(block);
            let first = if forward {
                center[axis] + offset
            } else {
                center[axis] + 1 - offset - length
            };
            shape[source_axis] = length;
            start[source_axis] = plan.selection.start[source_axis] + first * stride[source_axis];
            let (line, mask) = reader.read(&start, &shape, stride)?;
            let values = line.as_slice_memory_order().expect("contiguous line");
            let valid = mask
                .as_ref()
                .map(|m| m.as_slice_memory_order().expect("contiguous mask"));
            for step in 0..length {
                let index = if forward { step } else { length - 1 - step };
                let value = if valid.is_none_or(|mask| mask[index]) {
                    values[index]
                } else {
                    0.
                };
                let distance = (offset + step) as f32;
                if value < threshold {
                    return Ok::<_, ImageError>((
                        Some(distance - (threshold - value) / (last - value)),
                        value,
                    ));
                }
                if value > last {
                    return Ok((Some(distance), value));
                }
                last = value;
            }
        }
        Ok((None, last))
    };
    let (left, _) = side(false)?;
    let (right, last) = side(true)?;
    let width = match (left, right) {
        (Some(a), Some(b)) => (a + b) / 2.,
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => (extent - 1 - center[axis]) as f32 * (peak - threshold) / (peak - last),
    };
    Ok(if width.is_finite() && width > 0. {
        f64::from(width)
    } else {
        extent as f64
    })
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
    let scale = points
        .iter()
        .map(|&point| f64::from(pixels.samples[point].value))
        .fold(0., f64::max);
    if scale == 0. || !scale.is_finite() {
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
                value: f64::from(pixels.samples[point].value) / scale,
            }
        })
        .collect::<Vec<_>>();
    for component in &mut estimates {
        component[0] /= scale;
    }
    let mut solution = optimize(&sample, &estimates, rank, options, scale).unwrap_or(estimates);
    for component in &mut solution {
        component[0] *= scale;
    }
    solution
}

pub(in crate::analysis::imsubimage) enum WindowFit {
    Point,
    DefaultWidth,
    Fitted(Parameters),
}

const FIT_2D: FitOptions = FitOptions {
    maximum_rms: -1.,
    retries: 0,
    iterations: 1000,
    criterion: 0.0001,
};

pub(in crate::analysis::imsubimage) struct SkyFit {
    pub(in crate::analysis::imsubimage) gaussians: Vec<Parameters>,
    pub(in crate::analysis::imsubimage) offset: f64,
    pub(in crate::analysis::imsubimage) offset_error: f64,
}

fn sky_observation(([x, y], pixel): ([f64; 2], f32)) -> Observation {
    Observation {
        position: [x, y, 0.],
        value: f64::from(pixel),
    }
}

// Fit2D's initial guess uses absolute moments above five percent of the
// largest magnitude. Lower samples still enter the least-squares fit.
fn moment_seed(samples: impl Iterator<Item = Observation>, scale: f64) -> Option<Parameters> {
    const FACTOR: f64 = 4. * LN_2;
    let clip = f64::from((0.05 * scale) as f32);
    let mut signed_sum = 0.;
    let mut moments = [0.; 6];
    for row in samples {
        let magnitude = row.value.abs();
        if magnitude > clip {
            let [x, y, _] = row.position;
            signed_sum += row.value;
            for (sum, term) in moments.iter_mut().zip([1., x, y, x * y, x * x, y * y]) {
                *sum += magnitude * term;
            }
        }
    }
    let [sum, x, y, xy, xx, yy] = moments;
    let x = x / sum;
    let y = y / sum;
    let xy = xy / sum - x * y;
    let xx = xx / sum - x * x;
    let yy = yy / sum - y * y;
    let span = (xx - yy).hypot(2. * xy);
    let major = (FACTOR * (xx + yy + span)).sqrt();
    let minor = (FACTOR * (xx + yy - span)).sqrt();
    if minor <= 0. || !major.is_finite() || !minor.is_finite() {
        return None;
    }
    let angle = 0.5 * (2. * xy).atan2(yy - xx);
    let height = signed_sum.signum() * FACTOR * sum / (PI * major * minor);
    let major = 0.95 * major;
    Some([height, x, y, major, minor / major, -angle, 0., 0., 0.])
}

pub(in crate::analysis::imsubimage) fn fit_window(
    pixels: impl IntoIterator<Item = ([f64; 2], f32)>,
    peak: f64,
) -> WindowFit {
    let mut samples: Vec<_> = pixels.into_iter().map(sky_observation).collect();
    if samples.len() < 6 || samples.iter().any(|row| !row.value.is_finite()) {
        return WindowFit::Point;
    }
    let scale = samples.iter().map(|row| row.value.abs()).fold(0., f64::max);
    let Some(mut seed) = moment_seed(samples.iter().copied(), scale) else {
        return WindowFit::Point;
    };
    seed[0] = peak / scale;
    for row in &mut samples {
        row.value /= scale;
    }
    let Some(mut result) =
        optimize(&samples, &[seed], 2, &FIT_2D, scale).and_then(|mut fit| fit.pop())
    else {
        return WindowFit::DefaultWidth;
    };
    result[0] *= scale;
    WindowFit::Fitted(result)
}

pub(in crate::analysis::imsubimage) fn fit_sky(
    pixels: impl Iterator<Item = ([f64; 2], f32)> + Clone,
    shape: [usize; 2],
    provided: Option<(&[Parameters], &[[bool; 6]])>,
    zero_level: Option<(f64, bool)>,
) -> Option<SkyFit> {
    let unscaled = pixels.map(sky_observation);
    let mut selected = 0;
    let mut extremum: Option<Observation> = None;
    for row in unscaled.clone() {
        if !row.value.is_finite() {
            return None;
        }
        selected += 1;
        if extremum.is_none_or(|old| (row.value.abs(), row.value) > (old.value.abs(), old.value)) {
            extremum = Some(row);
        }
    }
    let extremum = extremum?;
    let default = || {
        let [x, y, _] = extremum.position;
        let width = (shape[0].max(shape[1]) / 2).max(1) as f64;
        [extremum.value, x, y, width, 0.9, -PI / 2., 0., 0., 0.]
    };
    let scale = extremum
        .value
        .abs()
        .max(zero_level.map_or(0., |(offset, _)| offset.abs()));
    let scale = if scale > 0. { scale } else { 1. };
    let (mut gaussians, fixed) = provided.map_or_else(
        || {
            (
                vec![moment_seed(unscaled.clone(), scale).unwrap_or_else(default)],
                None,
            )
        },
        |(seeds, fixed)| (seeds.to_vec(), Some(fixed)),
    );
    let samples = unscaled.map(|mut row| {
        row.value /= scale;
        row
    });
    for seed in &mut gaussians {
        seed[0] /= scale;
    }
    let offset = zero_level.map_or(0., |(value, _)| value / scale);
    let fit_offset = zero_level.is_some_and(|(_, fixed)| !fixed);
    let solver = Solver {
        observations: samples,
        observation_count: selected,
        variables: parameters(gaussians.len(), 2, fixed, fit_offset),
        rank: 2,
        scale,
    };
    if selected < solver.variables.len() {
        return None;
    }
    let mut fit = solver.optimize(Model { gaussians, offset }, &FIT_2D)?;
    let offset_error = if fit_offset {
        solver.offset_error(&fit) * scale
    } else {
        0.
    };
    for component in &mut fit.gaussians {
        normalize_shape(component, 2);
        component[0] *= scale;
    }
    Some(SkyFit {
        gaussians: fit.gaussians,
        offset: fit.offset * scale,
        offset_error,
    })
}

fn optimize(
    observations: &[Observation],
    seeds: &[Parameters],
    rank: usize,
    options: &FitOptions,
    scale: f64,
) -> Option<Vec<Parameters>> {
    let mut variables = parameters(seeds.len(), rank, None, false);
    if observations.len() <= seeds.len() {
        variables.retain(|parameter| {
            matches!(*parameter, Parameter::Gaussian(i, 0) if i + observations.len() >= seeds.len())
        });
    }
    let solver = Solver {
        observations: observations.iter().copied(),
        observation_count: observations.len(),
        variables,
        rank,
        scale,
    };
    solver
        .optimize(
            Model {
                gaussians: seeds.to_vec(),
                offset: 0.,
            },
            options,
        )
        .map(|mut model| {
            for component in &mut model.gaussians {
                normalize_shape(component, rank);
            }
            model.gaussians
        })
}

#[derive(Clone, Copy)]
enum Parameter {
    Gaussian(usize, usize),
    Offset,
}

fn parameters(
    count: usize,
    rank: usize,
    fixed: Option<&[[bool; 6]]>,
    fit_offset: bool,
) -> Vec<Parameter> {
    let mut parameters: Vec<_> = (0..count)
        .flat_map(|i| {
            (0..rank * 3).filter_map(move |j| {
                fixed
                    .is_none_or(|fixed| !fixed[i][j])
                    .then_some(Parameter::Gaussian(i, j))
            })
        })
        .collect();
    parameters.extend(fit_offset.then_some(Parameter::Offset));
    parameters
}

#[derive(Clone)]
struct Model {
    gaussians: Vec<Parameters>,
    offset: f64,
}

impl Model {
    fn value(&self, rank: usize, position: [f64; 3]) -> f64 {
        self.gaussians
            .iter()
            .map(|p| gaussian(p, rank, position))
            .sum::<f64>()
            + self.offset
    }

    fn slope(&self, rank: usize, position: [f64; 3], parameter: Parameter) -> f64 {
        let Parameter::Gaussian(i, j) = parameter else {
            return 1.;
        };
        let mut component = self.gaussians[i];
        let delta = component[j].abs().max(1.) * 1e-5;
        component[j] += delta;
        let plus = gaussian(&component, rank, position);
        component[j] -= 2. * delta;
        (plus - gaussian(&component, rank, position)) / (2. * delta)
    }

    fn adjust(&mut self, changes: impl IntoIterator<Item = (Parameter, f64)>) {
        for (parameter, delta) in changes {
            match parameter {
                Parameter::Gaussian(i, j) => self.gaussians[i][j] += delta,
                Parameter::Offset => self.offset += delta,
            }
        }
    }
}

struct Solver<I> {
    observations: I,
    observation_count: usize,
    variables: Vec<Parameter>,
    rank: usize,
    scale: f64,
}

impl<I: Iterator<Item = Observation> + Clone> Solver<I> {
    fn optimize(&self, seeds: Model, options: &FitOptions) -> Option<Model> {
        let mut best = None;
        let mut best_cost = f64::INFINITY;
        let precision = f64::EPSILON * self.observation_count as f64;
        let retries = if options.retries < 0 {
            self.rank * seeds.gaussians.len()
        } else {
            options.retries as usize
        };
        for attempt in 0..=retries {
            let mut model = seeds.clone();
            if attempt > 0 {
                let count = model.gaussians.len();
                let component = (attempt - 1) % count;
                let factor = [0.8, 1.25, 0.55, 1.55][((attempt - 1) / count) % 4];
                let width_count = if self.rank == 2 { 1 } else { self.rank };
                for width in
                    &mut model.gaussians[component][self.rank + 1..self.rank + 1 + width_count]
                {
                    *width *= factor;
                }
            }
            if let Some((model, score)) = self.refine(model, options, precision)
                && score < best_cost
            {
                best_cost = score;
                best = Some(model);
            }
            if self.scale * (best_cost / self.observation_count as f64).sqrt()
                <= options.maximum_rms
                || best_cost <= precision
            {
                break;
            }
        }
        best
    }

    fn cost(&self, model: &Model) -> f64 {
        self.observations
            .clone()
            .map(|row| (row.value - model.value(self.rank, row.position)).powi(2))
            .sum()
    }

    fn refine(
        &self,
        mut model: Model,
        options: &FitOptions,
        precision: f64,
    ) -> Option<(Model, f64)> {
        let mut score = self.cost(&model);
        if self.variables.is_empty() {
            return score.is_finite().then_some((model, score));
        }
        let mut converged = score <= precision;
        let mut damping = 0.001;
        let mut candidate = model.clone();
        let criterion = options.criterion.powi(2);
        for _ in 0..options.iterations {
            if converged {
                break;
            }
            let linearize = |row: Observation, slope: &mut [f64]| {
                for (entry, &parameter) in slope.iter_mut().zip(&self.variables) {
                    *entry = model.slope(self.rank, row.position, parameter);
                }
            };
            let residual = |row: Observation| row.value - model.value(self.rank, row.position);
            let Some(step) = damped_step(
                self.observations.clone(),
                self.variables.len(),
                damping,
                linearize,
                residual,
            ) else {
                break;
            };
            candidate.clone_from(&model);
            candidate.adjust(self.variables.iter().copied().zip(step));
            let widths = if self.rank == 2 { 3..5 } else { 4..7 };
            if candidate
                .gaussians
                .iter()
                .any(|p| p[widths.clone()].iter().any(|&width| width <= 0.))
            {
                damping *= 10.;
                continue;
            }
            let cost = self.cost(&candidate);
            if cost < score {
                model.clone_from(&candidate);
                converged = cost <= precision || score - cost <= criterion * score;
                score = cost;
                damping *= 0.25;
            } else if damping <= 0.001 && (score - cost).abs() <= criterion * score {
                converged = true;
            } else {
                damping *= 10.;
                if damping > 1e12 {
                    break;
                }
            }
        }
        converged.then_some((model, score))
    }

    fn offset_error(&self, model: &Model) -> f64 {
        let count = self.variables.len() - 1;
        let degrees = self.observation_count.saturating_sub(self.variables.len()) as f64;
        if degrees == 0. {
            return f64::NAN;
        }
        // The offset column is one for every row. Remove its projection onto
        // the fitted Gaussian columns to obtain the Schur complement of the
        // normal matrix without constructing a separate dense inverse.
        let mut precision = self.observation_count as f64;
        if count > 0 {
            let linearize = |row: Observation, slope: &mut [f64]| {
                for (entry, &parameter) in slope.iter_mut().zip(&self.variables) {
                    *entry = model.slope(self.rank, row.position, parameter);
                }
            };
            if let Some(projection) =
                damped_step(self.observations.clone(), count, 0., linearize, |_| 1.)
            {
                for row in self.observations.clone() {
                    precision -= projection
                        .iter()
                        .zip(&self.variables)
                        .map(|(&weight, &parameter)| {
                            weight * model.slope(self.rank, row.position, parameter)
                        })
                        .sum::<f64>();
                }
            }
        }
        (self.cost(model) / degrees / precision).sqrt()
    }
}

fn normalize_shape(component: &mut Parameters, rank: usize) {
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
    } else {
        for (angle, width) in [(7, 5), (8, 6)] {
            let position = (component[angle] + PI / 2.).rem_euclid(PI) - PI / 2.;
            component[angle] = if position.abs() > PI / 4. {
                component.swap(4, width);
                position - position.signum() * PI / 2.
            } else {
                position
            };
        }
    }
}

pub(in crate::analysis::imsubimage) fn evaluate_sky(
    parameters: &Parameters,
    position: [f64; 2],
) -> f64 {
    gaussian(parameters, 2, [position[0], position[1], 0.])
}

fn damped_step(
    observations: impl Iterator<Item = Observation> + Clone,
    size: usize,
    damping: f64,
    linearize: impl Fn(Observation, &mut [f64]),
    residual: impl Fn(Observation) -> f64,
) -> Option<Vec<f64>> {
    const DENSE_LIMIT: usize = 512;
    let mut rhs = vec![0.; size];
    let mut slope = vec![0.; size];
    let mut diagonal = vec![0.; size];
    let mut gram = (size <= DENSE_LIMIT).then(|| vec![0.; size * size]);
    for row in observations.clone() {
        linearize(row, &mut slope);
        let residual = residual(row);
        for (i, &entry) in slope.iter().enumerate() {
            rhs[i] += entry * residual;
            diagonal[i] += entry * entry;
            if let Some(gram) = &mut gram {
                for (output, &other) in gram[i * size + i + 1..(i + 1) * size]
                    .iter_mut()
                    .zip(&slope[i + 1..])
                {
                    *output += entry * other;
                }
            }
        }
    }
    let regularization = diagonal
        .iter()
        .map(|&entry| entry * damping + 1e-18)
        .collect::<Vec<_>>();
    if let Some(mut gram) = gram {
        for i in 0..size {
            gram[i * size + i] = diagonal[i] + regularization[i];
            for j in i + 1..size {
                gram[j * size + i] = gram[i * size + j];
            }
        }
        return solve_symmetric_positive_definite(&gram, &rhs);
    }

    // Apply the full JᵀJ across all components, streaming the Jacobian rows
    // to keep solver storage linear for crowded islands.
    let dot = |a: &[f64], b: &[f64]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f64>();
    for (entry, extra) in diagonal.iter_mut().zip(&regularization) {
        *entry += extra;
    }
    let mut direction = rhs
        .iter()
        .zip(&diagonal)
        .map(|(r, d)| r / d)
        .collect::<Vec<_>>();
    let mut rho = dot(&rhs, &direction);
    let tolerance = rho * 1e-16;
    let mut solution = vec![0.; size];
    let mut product = vec![0.; size];
    for _ in 0..size.min(128) {
        if rho <= tolerance {
            break;
        }
        for ((out, &lambda), &step) in product.iter_mut().zip(&regularization).zip(&direction) {
            *out = lambda * step;
        }
        for row in observations.clone() {
            linearize(row, &mut slope);
            let projection = dot(&slope, &direction);
            for (entry, derivative) in product.iter_mut().zip(&slope) {
                *entry += projection * derivative;
            }
        }
        let step = rho / dot(&direction, &product);
        if !step.is_finite() || step <= 0. {
            return None;
        }
        for i in 0..size {
            solution[i] += step * direction[i];
            rhs[i] -= step * product[i];
        }
        let next = rhs.iter().zip(&diagonal).map(|(r, d)| r * r / d).sum();
        for i in 0..size {
            direction[i] = rhs[i] / diagonal[i] + (next / rho) * direction[i];
        }
        rho = next;
    }
    Some(solution)
}

fn gaussian(p: &Parameters, rank: usize, position: [f64; 3]) -> f64 {
    let x = position[0] - p[1];
    let y = position[1] - p[2];
    let shape = if rank == 2 {
        let (sin, cos) = p[5].sin_cos();
        let long = (-sin * x + cos * y) / p[3];
        let short = (cos * x + sin * y) / (p[3] * p[4]);
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

#[cfg(test)]
mod tests {
    use super::{Observation, damped_step};

    #[test]
    fn large_fit_solves_the_coupled_damped_system() {
        const SIZE: usize = 513;
        let rows = (0..SIZE)
            .map(|i| Observation {
                position: [i as f64, 0., 0.],
                value: 1. + (i as f64).sin(),
            })
            .collect::<Vec<_>>();
        let damping = 0.01;
        let linearize = |row: Observation, slope: &mut [f64]| {
            let index = row.position[0] as usize;
            slope.fill(0.);
            slope[index] = 1.;
            slope[(index + 1) % SIZE] = 0.25;
        };
        let result = damped_step(rows.iter().copied(), SIZE, damping, linearize, |row| {
            row.value
        })
        .unwrap();
        let residual = (0..SIZE)
            .map(|i| result[i] + 0.25 * result[(i + 1) % SIZE] - rows[i].value)
            .collect::<Vec<_>>();
        for i in 0..SIZE {
            let error = residual[i]
                + 0.25 * residual[(i + SIZE - 1) % SIZE]
                + damping * (1. + 0.25_f64.powi(2)) * result[i];
            assert!(error.abs() < 5e-6, "normal equation {i}: {error}");
        }
    }
}
