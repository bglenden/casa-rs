// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted cross-hand phase and position angle for a known source.

use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

use super::SolveSamples;

/// VI1 normalizes each correlation's scalar weight by the square of its
/// mean nonzero model amplitude over unflagged channels.
pub(super) fn legacy_row_weights(
    samples: &SolveSamples<'_>,
    flags: &ArrayD<bool>,
    model: &ArrayD<Complex32>,
    order: [usize; 4],
    channels: &[usize],
) -> [f32; 4] {
    let mut amplitudes = [0.0; 4];
    let mut counts = [0; 4];
    let mut weights = [0.0; 4];
    for &channel in channels {
        if channel >= samples.data.ncols() || order.iter().any(|&corr| flags[[corr, channel]]) {
            continue;
        }
        for (index, corr) in order.into_iter().enumerate() {
            let amplitude = model[[corr, channel]].norm();
            if amplitude > 0.0 {
                if counts[index] == 0 {
                    weights[index] = samples.weights[[corr, channel]];
                }
                amplitudes[index] += amplitude;
                counts[index] += 1;
            }
        }
    }
    for (index, weight) in weights.iter_mut().enumerate() {
        if counts[index] != 0 {
            let amplitude = amplitudes[index] / counts[index] as f32;
            *weight *= amplitude * amplitude;
        }
    }
    weights
}

#[derive(Clone, Copy)]
pub(super) enum Measurement {
    Phase,
    CircularAngle,
    LinearAngle,
}

#[derive(Default)]
pub(super) struct CrossHandFit {
    saw_samples: bool,
    sum: Complex64,
}

impl CrossHandFit {
    pub(super) fn add(
        &mut self,
        measurement: Measurement,
        data: [Complex32; 4],
        model: [Complex32; 4],
        weights: [f32; 4],
    ) {
        self.saw_samples = true;
        let cross_weight = 0.5 * (weights[1] + weights[2]);
        self.sum += match measurement {
            Measurement::Phase => {
                complex32_to_64(cross_weight * data[1])
                    + complex32_to_64(cross_weight * data[2]).conj()
            }
            Measurement::CircularAngle => {
                ratio(data[1], model[1], cross_weight)
                    + ratio(data[2], model[2], cross_weight).conj()
            }
            Measurement::LinearAngle => {
                let polarization = |[xx, xy, yx, yy]: [Complex32; 4]| {
                    0.5 * (xx - yy) + Complex32::I * (0.5 * (xy + yx))
                };
                ratio(
                    polarization(data),
                    polarization(model),
                    0.25 * weights.into_iter().sum::<f32>(),
                )
            }
        };
    }

    pub(super) fn saw_samples(&self) -> bool {
        self.saw_samples
    }

    pub(super) fn phase(self) -> Option<Complex32> {
        let amplitude = self.sum.norm();
        if amplitude <= 0.0 || !amplitude.is_finite() {
            return None;
        }
        let unit = self.sum / amplitude;
        Some(Complex32::new(unit.re as f32, unit.im as f32))
    }
}

fn ratio(data: Complex32, model: Complex32, weight: f32) -> Complex64 {
    if data.norm_sqr() > 0.0 && model.norm_sqr() > 0.0 && weight > 0.0 {
        complex32_to_64(weight * data / model)
    } else {
        Complex64::ZERO
    }
}
