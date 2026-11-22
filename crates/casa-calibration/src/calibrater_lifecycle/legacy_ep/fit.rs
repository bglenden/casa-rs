// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit the two pointing axes of each receptor from baseline residuals.

use casa_types::Complex32;

use super::degrid::PointingSample;

#[derive(Default)]
pub(super) struct ObservedHand {
    pub(super) data: Complex32,
    pub(super) channels: usize,
    pub(super) samples: Vec<PointingSample>,
}

pub(super) struct Observation {
    pub(super) weight: f32,
    pub(super) hands: [ObservedHand; 2],
}

pub(super) struct Baseline {
    pub(super) antennas: [usize; 2],
    pub(super) observations: Vec<Observation>,
}

#[derive(Clone, Copy, Default)]
struct Assessment {
    values: [Complex32; 3],
    weight: f32,
}

impl Baseline {
    fn assess(&self, offsets: &[[f32; 4]], axes: [usize; 2], hand: usize) -> Assessment {
        let [a, b] = self.antennas;
        let offset = axes.map(|axis| f64::from(offsets[a][axis]) + f64::from(offsets[b][axis]));
        let mut result = Assessment::default();
        for observation in &self.observations {
            let hand = &observation.hands[hand];
            if hand.channels == 0 {
                continue;
            }
            let mut predicted = [Complex32::default(); 3];
            for sample in &hand.samples {
                for (sum, value) in predicted.iter_mut().zip(sample.evaluate(offset)) {
                    *sum += value;
                }
            }
            predicted[0] -= hand.data;
            let scale = observation.weight / hand.channels as f32;
            for (sum, value) in result.values.iter_mut().zip(predicted) {
                *sum += value * scale;
            }
            result.weight += observation.weight;
        }
        if result.weight > 0. {
            result.values = result.values.map(|value| value / result.weight);
        }
        result
    }
}

pub(super) fn solve(baselines: &[Baseline], offsets: &mut [[f32; 4]]) -> bool {
    let mut converged = false;
    for hand in 0..2 {
        // dpbwgrad uses the conjugate kernel plane. solve2 fits the
        // opposite receptor's two offsets for each measured correlation.
        let axes = [1 - hand, 3 - hand];
        let mut current = assess(baselines, offsets, axes, hand);
        let Some(mut best_chi) = chi_square(&current) else {
            return false;
        };
        let mut best = offsets.to_vec();
        let mut gain = 0.01;
        converged = false;
        for _ in 0..250 {
            descend(baselines, &current, offsets, axes, gain);
            current = assess(baselines, offsets, axes, hand);
            let Some(chi) = chi_square(&current) else {
                return false;
            };
            if (chi - best_chi).abs() < 1e-9 {
                offsets.copy_from_slice(&best);
                converged = true;
                break;
            }
            if chi < best_chi {
                best_chi = chi;
                best.copy_from_slice(offsets);
            } else {
                offsets.copy_from_slice(&best);
                gain *= 0.5;
                if gain < 1e-20 {
                    converged = true;
                    break;
                }
                current = assess(baselines, offsets, axes, hand);
            }
        }
    }
    converged
}

fn assess(
    baselines: &[Baseline],
    offsets: &[[f32; 4]],
    axes: [usize; 2],
    hand: usize,
) -> Vec<Assessment> {
    baselines
        .iter()
        .map(|baseline| baseline.assess(offsets, axes, hand))
        .collect()
}

fn chi_square(assessment: &[Assessment]) -> Option<f64> {
    let (value, weight) = assessment.iter().fold((0., 0.), |(sum, weight), item| {
        let residual = item.values[0];
        let power = f64::from(residual.re).powi(2) + f64::from(residual.im).powi(2);
        let w = f64::from(item.weight);
        (sum + w * power, weight + w)
    });
    let mean = value / weight;
    (weight > 0. && mean.is_finite()).then_some(mean)
}

fn descend(
    baselines: &[Baseline],
    current: &[Assessment],
    offsets: &mut [[f32; 4]],
    axes: [usize; 2],
    gain: f64,
) {
    let antennas = offsets.len();
    let mut by_antenna = vec![[Complex32::default(); 3]; antennas * antennas];
    let mut weights = vec![0.; antennas];
    for (baseline, fit) in baselines.iter().zip(current) {
        if fit.weight <= 0. {
            continue;
        }
        let [a, b] = baseline.antennas;
        let [residual, azimuth, elevation] = fit.values;
        let mut values = [residual, azimuth * fit.weight, elevation * fit.weight];
        if a <= b {
            values = values.map(|value| value.conj());
        }
        for (i, j) in [(a, b), (b, a)] {
            for (sum, value) in by_antenna[i * antennas + j].iter_mut().zip(values) {
                *sum += value;
            }
            weights[i] += f64::from(fit.weight);
        }
    }
    for ((values, offset), weight) in by_antenna.chunks_exact(antennas).zip(offsets).zip(weights) {
        for (component, axis) in axes.into_iter().enumerate() {
            if weight == 0. {
                offset[axis] = 0.;
                continue;
            }
            let mut numerator = 0.;
            let mut denominator = 0.;
            for row in values {
                let gradient = row[component + 1];
                numerator += (row[0].conj() * gradient).re;
                denominator += gradient.norm_sqr();
            }
            if denominator > 0. {
                let step = -2. * gain * weight * f64::from(numerator) / f64::from(denominator);
                offset[axis] = (f64::from(offset[axis]) + step) as f32;
            }
        }
    }
}
