// SPDX-License-Identifier: LGPL-3.0-or-later
//! Posterior water-vapour retrieval from a four-channel observation.

use super::data::Temperatures;
use super::model::{RadiometerModel, WaterLayer};
use super::paths::Retrieval;

mod axes;
mod random;
use axes::principal_axes;
use random::Random;

const LIVE_SAMPLES: usize = 200;
const PROPOSALS_PER_SAMPLE: usize = 100;
const MIN_COEFFICIENT_PROBABILITY: f64 = 1e-10;
const PRIOR: [(f64, f64); 4] = [(0.0, 10.0), (250.0, 295.0), (300.0, 550.0), (0.0, 0.2)];

#[derive(Clone, Copy)]
struct Sample {
    parameters: [f64; 4],
    cost: f64,
}

impl Sample {
    fn water_layer(self) -> WaterLayer {
        let [
            precipitable_water_mm,
            temperature_k,
            pressure_hpa,
            cloud_opacity,
        ] = self.parameters;
        WaterLayer {
            precipitable_water_mm,
            temperature_k,
            pressure_hpa,
            cloud_opacity,
        }
    }
}

pub(super) struct Posterior {
    pub coefficients: Retrieval,
    pub precipitable_water_mm: f64,
    pub precipitable_water_error_mm: f64,
    pub evidence: f64,
}

pub(super) fn fit(
    temperatures: Temperatures,
    time_seconds: f64,
    elevation_rad: f64,
    seed: u32,
    continuum: bool,
) -> Option<Posterior> {
    if temperatures
        .iter()
        .any(|&value| !(2.7..=350.0).contains(&value))
        || !elevation_rad.is_finite()
        || elevation_rad.sin() <= 0.0
    {
        return None;
    }
    let model = RadiometerModel::new(elevation_rad);
    let dimensions = if continuum { 4 } else { 3 };
    let initial_seed = if continuum || seed == 0 { 43 } else { seed };
    let distribution = Distribution {
        temperatures,
        model: &model,
        dimensions,
    };
    let mut live = initial_samples(&distribution, initial_seed);
    let mut random = Random::new(5489);
    let mut weighted = Vec::new();
    let maximum_steps = if continuum { 3_000 } else { 10_000 };
    for iteration in 0..maximum_steps {
        let worst = *live.last()?;
        let replacement = sample_contour(&distribution, &live, worst, &mut random);
        if replacement.cost >= worst.cost {
            break;
        }
        // Each discarded sample integrates the next shell of prior mass.
        let compression = -1.0 / live.len() as f64;
        let log_weight = iteration as f64 * compression + (-compression.exp_m1()).ln();
        weighted.push((worst, log_weight - worst.cost));
        live.pop();
        match live.binary_search_by(|sample| sample.cost.total_cmp(&replacement.cost)) {
            Ok(_) => break,
            Err(index) => live.insert(index, replacement),
        }
    }
    moments(&model, time_seconds, &weighted)
}

struct Distribution<'a> {
    temperatures: Temperatures,
    model: &'a RadiometerModel,
    dimensions: usize,
}

impl Distribution<'_> {
    fn evaluate(&self, parameters: [f64; 4]) -> Sample {
        let mut sample = Sample {
            parameters,
            cost: f64::INFINITY,
        };
        if !(0..self.dimensions)
            .all(|index| (PRIOR[index].0..=PRIOR[index].1).contains(&parameters[index]))
        {
            return sample;
        }
        let predicted = self.model.temperatures(sample.water_layer());
        // The likelihood treats the four independent brightness errors as 1 K.
        sample.cost = 2.0 * std::f64::consts::TAU.ln()
            + 0.5
                * self
                    .temperatures
                    .iter()
                    .zip(predicted)
                    .map(|(&actual, predicted)| (actual - predicted).powi(2))
                    .sum::<f64>();
        sample
    }
}

fn initial_samples(distribution: &Distribution<'_>, seed: u32) -> Vec<Sample> {
    let mut random = Random::new(seed);
    let mut live = Vec::with_capacity(LIVE_SAMPLES);
    for _ in 0..LIVE_SAMPLES {
        let mut parameters = [0.0; 4];
        for (value, &(low, high)) in parameters[..distribution.dimensions].iter_mut().zip(&PRIOR) {
            *value = low + (high - low) * random.uniform();
        }
        live.push(distribution.evaluate(parameters));
    }
    live.sort_by(|left, right| left.cost.total_cmp(&right.cost));
    live.dedup_by(|left, right| left.cost == right.cost);
    live
}

fn sample_contour(
    distribution: &Distribution<'_>,
    live: &[Sample],
    threshold: Sample,
    random: &mut Random,
) -> Sample {
    let dimensions = distribution.dimensions;
    let proposal_axes = covariance_axes(live, dimensions);
    let mut current = threshold;
    for _ in 0..PROPOSALS_PER_SAMPLE {
        let mut proposal = current.parameters;
        for axis in &proposal_axes[..dimensions] {
            let factor = random.normal();
            for (value, step) in proposal.iter_mut().zip(axis) {
                *value += factor * step;
            }
        }
        let evaluated = distribution.evaluate(proposal);
        if evaluated.cost < threshold.cost {
            current = evaluated;
        } else {
            // Keep the independent uniform draw for the rejected proposal.
            random.uniform();
        }
    }
    current
}

fn covariance_axes(live: &[Sample], dimensions: usize) -> [[f64; 4]; 4] {
    let mut center = [0.0; 4];
    let count = live.len() as f64;
    for sample in live {
        for (average, value) in center.iter_mut().zip(sample.parameters) {
            *average += value;
        }
    }
    center.iter_mut().for_each(|value| *value /= count);
    let mut covariance = [[0.0; 4]; 4];
    for sample in live {
        let delta: [f64; 4] = std::array::from_fn(|index| sample.parameters[index] - center[index]);
        for (axis, row) in covariance.iter_mut().enumerate().take(dimensions) {
            for (column, value) in row.iter_mut().enumerate().take(dimensions) {
                *value += delta[axis] * delta[column];
            }
        }
    }
    covariance
        .iter_mut()
        .flatten()
        .for_each(|value| *value /= count);
    principal_axes(covariance, dimensions)
}

fn moments(
    model: &RadiometerModel,
    time_seconds: f64,
    samples: &[(Sample, f64)],
) -> Option<Posterior> {
    let max_log_weight = samples
        .iter()
        .map(|&(_, weight)| weight)
        .max_by(f64::total_cmp)?;
    let scaled_probability = |log_weight: f64| (log_weight - max_log_weight).exp();
    let total = samples
        .iter()
        .map(|&(_, weight)| scaled_probability(weight))
        .sum::<f64>();
    let evidence = (max_log_weight + total.ln()).exp();
    if !evidence.is_finite() || evidence == 0.0 {
        return None;
    }
    let mut water = 0.0;
    let mut derivatives = Vec::new();
    let mut coefficients = [0.0; 4];
    for &(sample, weight) in samples {
        let probability = scaled_probability(weight) / total;
        water += sample.parameters[0] * probability;
        if probability > MIN_COEFFICIENT_PROBABILITY {
            let value = model.temperature_per_path(sample.water_layer());
            for channel in 0..4 {
                coefficients[channel] += value[channel] * probability;
            }
            derivatives.push((value, probability));
        }
    }
    let water_variance = samples
        .iter()
        .map(|&(sample, weight)| {
            (sample.parameters[0] - water).powi(2) * scaled_probability(weight) / total
        })
        .sum::<f64>();
    let variance = std::array::from_fn(|channel| {
        derivatives
            .iter()
            .map(|(value, probability)| {
                (value[channel] - coefficients[channel]).powi(2) * probability
            })
            .sum::<f64>()
    });
    Some(Posterior {
        coefficients: Retrieval {
            time_seconds,
            temperature_per_path_k_per_meter: coefficients,
            coefficient_second_moment: variance,
        },
        precipitable_water_mm: water,
        precipitable_water_error_mm: water_variance.sqrt(),
        evidence,
    })
}
