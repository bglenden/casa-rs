// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered sample arithmetic, separated from per-row array addressing.

use casa_types::Complex32;
use ndarray::Axis;

use super::{SolveEdgeAccumulation, SolveSamples, median_with_even_mean};

/// CASA's correlation-independent flags are shared by every receptor.
pub(super) fn channel_flags(samples: &SolveSamples<'_>) -> Vec<bool> {
    samples
        .flags
        .axis_iter(Axis(1))
        .map(|channel| channel.iter().any(|flag| *flag))
        .collect()
}

pub(super) struct SampleAccumulation<'a> {
    pub(super) values: &'a mut [SolveEdgeAccumulation],
    pub(super) attempted: &'a mut bool,
    pub(super) channelized: bool,
}

struct Sample {
    data: Complex32,
    model: Complex32,
    weight: f32,
    flagged: bool,
}

impl SampleAccumulation<'_> {
    pub(super) fn observe(
        &mut self,
        samples: &SolveSamples<'_>,
        correlation: usize,
        flags: &[bool],
        channels: &[usize],
        stokes_i: f32,
    ) -> f64 {
        let data = samples.data.index_axis(Axis(0), correlation);
        let weights = samples.weights.index_axis(Axis(0), correlation);
        let model = samples
            .model
            .map(|model| model.index_axis_move(Axis(0), correlation));
        let point = Complex32::new(stokes_i, 0.0);
        if channels.iter().copied().eq(0..data.len()) {
            // Strided row iterators avoid repeated two-dimensional indexing;
            // scalar weights retain their broadcast stride of zero.
            let values = data.iter().zip(weights.iter()).zip(flags);
            if let Some(model) = &model {
                self.add(
                    values
                        .zip(model)
                        .map(|(((&data, &weight), &flagged), &model)| Sample {
                            data,
                            model,
                            weight,
                            flagged,
                        }),
                )
            } else {
                self.add(values.map(|((&data, &weight), &flagged)| Sample {
                    data,
                    model: point,
                    weight,
                    flagged,
                }))
            }
        } else {
            self.add(channels.iter().map(|&channel| Sample {
                data: data[channel],
                model: model.as_ref().map_or(point, |model| model[channel]),
                weight: weights[channel],
                flagged: flags[channel],
            }))
        }
    }

    fn add(&mut self, samples: impl Iterator<Item = Sample>) -> f64 {
        if self.channelized {
            self.add_ordered::<true>(samples)
        } else {
            self.add_ordered::<false>(samples)
        }
    }

    fn add_ordered<const CHANNELIZED: bool>(
        &mut self,
        samples: impl Iterator<Item = Sample>,
    ) -> f64 {
        let mut weights = Vec::new();
        if CHANNELIZED {
            weights.reserve(self.values.len());
        }
        let mut summed_weight = 0.0_f64;
        for (channel, sample) in samples.enumerate() {
            let output = if CHANNELIZED { channel } else { 0 };
            let weight = self.values[output].add_sample(self.attempted, sample);
            if CHANNELIZED {
                weights.push(weight);
            } else {
                summed_weight += f64::from(weight);
            }
        }
        if CHANNELIZED {
            f64::from(median_with_even_mean(&mut weights))
        } else {
            summed_weight
        }
    }
}

impl SolveEdgeAccumulation {
    /// Model division and weighting follow the original ordered arithmetic.
    fn add_sample(&mut self, attempted: &mut bool, sample: Sample) -> f32 {
        if sample.weight <= 0.0 || sample.flagged {
            return 0.0;
        }
        let model_norm_sqr = sample.model.norm_sqr();
        if model_norm_sqr <= f32::EPSILON {
            return 0.0;
        }
        *attempted = true;
        let value = sample.data / sample.model;
        let power = value.norm_sqr();
        if power <= f32::EPSILON * f32::EPSILON {
            return 0.0;
        }
        let weight = sample.weight * model_norm_sqr;
        self.weighted_sum += value * Complex32::new(weight, 0.0);
        self.weight += weight;
        self.weighted_sample_power += f64::from(weight) * f64::from(power);
        self.sample_count += 1;
        weight
    }
}
