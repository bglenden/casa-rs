// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert time-dependent WVR retrievals to antenna paths and gains.

use std::f64::consts::TAU;

use casa_core::Complex32;

use super::data::Temperatures;

const CHANNEL_NOISE_K: [f64; 4] = [0.10, 0.08, 0.08, 0.09];
const CASA_LIGHT_SPEED_METERS_PER_SECOND: f64 = 3e8;

#[derive(Clone, Copy)]
pub(super) struct Retrieval {
    pub time_seconds: f64,
    pub temperature_per_path_k_per_meter: [f64; 4],
    pub coefficient_second_moment: [f64; 4],
}

pub(super) struct CoefficientTimeline {
    knots: Vec<Retrieval>,
}

impl CoefficientTimeline {
    pub fn new(mut knots: Vec<Retrieval>) -> Option<Self> {
        knots.retain(|reading| reading.time_seconds.is_finite());
        knots.sort_by(|left, right| left.time_seconds.total_cmp(&right.time_seconds));
        knots.dedup_by(|left, right| left.time_seconds == right.time_seconds);
        (!knots.is_empty()).then_some(Self { knots })
    }

    pub fn representative(&self) -> Retrieval {
        self.knots[self.knots.len() / 2..]
            .iter()
            .copied()
            .find(|fit| fit.temperature_per_path_k_per_meter[0] > 0.0)
            .unwrap_or(self.knots[self.knots.len() - 1])
    }

    pub fn at(&self, time_seconds: f64) -> Retrieval {
        let high = self
            .knots
            .partition_point(|reading| reading.time_seconds <= time_seconds);
        let first = self.knots[high.saturating_sub(1)];
        if high == 0 || high == self.knots.len() {
            return first;
        }
        let second = self.knots[high];
        let fraction =
            (time_seconds - first.time_seconds) / (second.time_seconds - first.time_seconds);
        let interpolate = |left: [f64; 4], right: [f64; 4]| {
            std::array::from_fn(|channel| {
                (1.0 - fraction) * left[channel] + fraction * right[channel]
            })
        };
        Retrieval {
            time_seconds,
            temperature_per_path_k_per_meter: interpolate(
                first.temperature_per_path_k_per_meter,
                second.temperature_per_path_k_per_meter,
            ),
            coefficient_second_moment: interpolate(
                first.coefficient_second_moment,
                second.coefficient_second_moment,
            ),
        }
    }
}

/// Inverse-variance weights for the path each radiometer channel measures.
pub(super) fn path_weights(coefficients: [f64; 4]) -> [f64; 4] {
    let inverse_variance =
        |channel: usize| (coefficients[channel] / CHANNEL_NOISE_K[channel]).powi(2);
    let total = (0..4).map(inverse_variance).sum::<f64>();
    if total <= 0.0 || !total.is_finite() {
        return [0.0; 4];
    }
    std::array::from_fn(|channel| {
        coefficients[channel] / (CHANNEL_NOISE_K[channel].powi(2) * total)
    })
}

pub(super) fn thermal_error_meters(coefficients: [f64; 4]) -> f64 {
    path_weights(coefficients)
        .into_iter()
        .zip(CHANNEL_NOISE_K)
        .map(|(weight, noise)| (weight * noise).powi(2))
        .sum::<f64>()
        .sqrt()
}

pub(super) fn antenna_path_meters(temperatures: Option<Temperatures>, weights: [f64; 4]) -> f64 {
    temperatures.map_or(0.0, |values| {
        values
            .into_iter()
            .zip(weights)
            .filter(|&(temperature, _)| temperature > 0.0)
            .map(|(temperature, weight)| temperature * weight)
            .sum()
    })
}

/// CASA publishes unity with a flag when a path is unavailable.
pub(super) fn gain(
    path_meters: f64,
    frequency_hz: f64,
    reverse: bool,
    dispersion: f64,
) -> (Complex32, bool) {
    if path_meters == 0.0 || !path_meters.is_finite() {
        return (Complex32::new(1.0, 0.0), true);
    }
    let sign = if reverse { 1.0 } else { -1.0 };
    let phase = sign * TAU * frequency_hz / CASA_LIGHT_SPEED_METERS_PER_SECOND
        * (1.0 + dispersion)
        * path_meters;
    let (imaginary, real) = phase.sin_cos();
    (Complex32::new(real as f32, imaginary as f32), false)
}
