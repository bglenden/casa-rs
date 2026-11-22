// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted cross-hand phase and position angle for a known source.

use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};
use std::collections::BTreeMap;

use crate::solve::kernel::complex32_to_64;

use super::{BaselineSegment, CoherencyMean, SelectedSolveRow, SolveSamples, WeightedTime};

/// VisBuffGroupAcc uses one time origin per field/window and gives X Jones
/// the first nonempty accumulated buffer for its phase fit.
#[derive(Default)]
pub(super) struct LegacyCrossHandAveraging {
    starts: BTreeMap<(i32, i32), f64>,
    output: Option<(i32, i32)>,
    times: BTreeMap<(i32, i32, u64), WeightedTime>,
}

impl LegacyCrossHandAveraging {
    pub(super) fn start(&mut self, row: &SelectedSolveRow, span: f64, shared: bool) -> f64 {
        let group = if shared {
            (0, 0)
        } else {
            (row.field_id, row.data_spw_id)
        };
        if row.flag_row {
            return self.starts.get(&group).copied().unwrap_or(row.time_seconds);
        }
        let start = self.starts.entry(group).or_insert(row.time_seconds);
        if row.time_seconds - *start > span {
            *start = row.time_seconds;
        }
        *start
    }

    pub(super) fn observe(&mut self, segment: BaselineSegment) -> bool {
        let group = (segment.field, segment.spectral_window);
        *self.output.get_or_insert(group) == group
    }

    pub(super) fn fits(&self, segment: BaselineSegment) -> bool {
        self.output == Some((segment.field, segment.spectral_window))
    }

    pub(super) fn field(&self) -> Option<i32> {
        self.output.map(|(field, _)| field)
    }

    pub(super) fn observe_time(&mut self, segment: BaselineSegment, time: f64, weight: f64) {
        self.times
            .entry(legacy_time_key(segment))
            .or_default()
            .observe(time, weight);
    }

    pub(super) fn time(&self, segment: BaselineSegment) -> Option<f64> {
        self.times
            .get(&legacy_time_key(segment))
            .copied()
            .and_then(WeightedTime::mean)
    }
}

fn legacy_time_key(segment: BaselineSegment) -> (i32, i32, u64) {
    (
        segment.field,
        segment.spectral_window,
        segment.start.to_bits(),
    )
}

pub(super) fn legacy_bin_counts(
    flags: &ArrayD<bool>,
    order: [usize; 4],
    channels: &[usize],
    width: usize,
) -> Vec<usize> {
    channels
        .chunks(width)
        .map(|bin| {
            bin.iter()
                .filter(|&&channel| {
                    channel < flags.shape()[1] && order.iter().all(|&corr| !flags[[corr, channel]])
                })
                .count()
        })
        .collect()
}

/// VI1 normalizes each correlation's scalar weight by the square of its
/// mean nonzero model amplitude over unflagged channels.
pub(super) fn legacy_row_weights(
    samples: &SolveSamples<'_>,
    flags: &ArrayD<bool>,
    model: &ArrayD<Complex32>,
    order: [usize; 4],
    channels: &[usize],
    normalize: bool,
) -> ([f32; 4], usize) {
    let mut amplitudes = [0.0; 4];
    let mut counts = [0; 4];
    let mut weights = [0.0; 4];
    let mut usable = 0;
    for &channel in channels {
        if channel >= samples.data.ncols() || order.iter().any(|&corr| flags[[corr, channel]]) {
            continue;
        }
        usable += 1;
        for (index, corr) in order.into_iter().enumerate() {
            let amplitude = if normalize {
                model[[corr, channel]].norm()
            } else {
                1.0
            };
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
    (weights, usable)
}

/// CalVisBuffer adjusts its scalar weights once, after time averaging.
pub(super) fn legacy_phase_normalization(bins: &[CoherencyMean]) -> [f32; 4] {
    let mut amplitudes = [0.0; 4];
    let mut counts = [0; 4];
    for bin in bins {
        for (corr, weight) in bin.weights.into_iter().enumerate() {
            if weight > 0.0 {
                let amplitude = (bin.data[corr] / weight).norm();
                if amplitude > 0.0 {
                    amplitudes[corr] += amplitude;
                    counts[corr] += 1;
                }
            }
        }
    }
    std::array::from_fn(|corr| {
        let average = amplitudes[corr] / counts[corr].max(1) as f32;
        average * average
    })
}

#[derive(Clone, Copy)]
pub(super) enum Measurement {
    Phase,
    MuellerPhase,
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
        unflagged: u8,
        data: [Complex32; 4],
        model: [Complex32; 4],
        weights: [f32; 4],
    ) {
        if matches!(measurement, Measurement::MuellerPhase) && weights[0] + weights[3] <= 0.0 {
            return;
        }
        self.saw_samples = true;
        let required = match measurement {
            Measurement::LinearAngle => 0b1111,
            Measurement::Phase | Measurement::MuellerPhase | Measurement::CircularAngle => 0b0110,
        };
        if unflagged & required != required {
            return;
        }
        let cross_weight = 0.5 * (weights[1] + weights[2]);
        self.sum += match measurement {
            Measurement::Phase => {
                complex32_to_64(cross_weight * data[1])
                    + complex32_to_64(cross_weight * data[2]).conj()
            }
            Measurement::MuellerPhase => {
                // XMueller normalizes after the raw frequency and time averages.
                let amplitude = model[1].norm();
                let weight = cross_weight * (amplitude * amplitude);
                ratio(data[1], model[1], weight) + ratio(data[2], model[2], weight).conj()
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
