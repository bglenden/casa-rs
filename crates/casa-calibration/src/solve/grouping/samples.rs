// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered sample arithmetic, separated from per-row array addressing.

use casa_types::Complex32;
use ndarray::Axis;

use super::{SolveEdgeAccumulation, SolveSamples, median_with_even_mean};

/// CASA's correlation-independent flags are shared by every receptor.
pub(super) fn channel_flags(samples: &SolveSamples<'_>) -> Vec<bool> {
    let correlations = samples.flags.shape()[0];
    if correlations > 0
        && samples.flags.strides()[0] == 1
        && usize::try_from(samples.flags.strides()[1]) == Ok(correlations)
        && let Some(flags) = samples.flags.as_slice_memory_order()
    {
        return flags
            .chunks_exact(correlations)
            .map(|channel| channel.iter().any(|flag| *flag))
            .collect();
    }
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
        all_channels: bool,
    ) -> f64 {
        let data = samples.data.index_axis(Axis(0), correlation);
        let weights = samples.weights.index_axis(Axis(0), correlation);
        let model = samples
            .model
            .map(|model| model.index_axis_move(Axis(0), correlation));
        let point = Complex32::new(stokes_i, 0.0);
        let correlations = samples.data.nrows();
        if all_channels
            && model.is_none()
            && !weights.is_empty()
            && weights.strides()[0] == 0
            && samples.data.strides()[0] == 1
            && usize::try_from(samples.data.strides()[1]) == Ok(correlations)
            && let Some(packed) = samples.data.as_slice_memory_order()
        {
            // Scalar SIGMA broadcasts a single weight. Casacore's packed
            // channel cells avoid generic strided array iteration as well.
            let weight = weights[0];
            return self.add::<true>(packed.chunks_exact(correlations).zip(flags).map(
                |(channel, &flagged)| Sample {
                    data: channel[correlation],
                    model: point,
                    weight,
                    flagged,
                },
            ));
        }
        if all_channels {
            // Strided row iterators avoid repeated two-dimensional indexing;
            // scalar weights retain their broadcast stride of zero.
            let values = data.iter().zip(weights.iter()).zip(flags);
            if let Some(model) = &model {
                self.add::<false>(values.zip(model).map(
                    |(((&data, &weight), &flagged), &model)| Sample {
                        data,
                        model,
                        weight,
                        flagged,
                    },
                ))
            } else {
                self.add::<true>(values.map(|((&data, &weight), &flagged)| Sample {
                    data,
                    model: point,
                    weight,
                    flagged,
                }))
            }
        } else {
            self.add::<false>(channels.iter().map(|&channel| Sample {
                data: data[channel],
                model: model.as_ref().map_or(point, |model| model[channel]),
                weight: weights[channel],
                flagged: flags[channel],
            }))
        }
    }

    fn add<const POINT: bool>(&mut self, samples: impl Iterator<Item = Sample>) -> f64 {
        if self.channelized {
            self.add_ordered::<true, POINT>(samples)
        } else {
            self.add_ordered::<false, POINT>(samples)
        }
    }

    fn add_ordered<const CHANNELIZED: bool, const POINT: bool>(
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
            let weight = self.values[output].add_sample::<POINT>(self.attempted, sample);
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
    /// Accumulate w |M|² (D/M) = w D conj(M), with power w |D|².
    /// Cancelling the model division avoids two scalar divides per sample.
    fn add_sample<const POINT: bool>(&mut self, attempted: &mut bool, sample: Sample) -> f32 {
        if sample.weight <= 0.0 || sample.flagged {
            return 0.0;
        }
        let model_norm_sqr = sample.model.norm_sqr();
        if model_norm_sqr <= f32::EPSILON {
            return 0.0;
        }
        *attempted = true;
        let power = sample.data.norm_sqr();
        if power <= f32::EPSILON * f32::EPSILON * model_norm_sqr {
            return 0.0;
        }
        let weight = sample.weight * model_norm_sqr;
        self.weighted_sum += if POINT
            && power.is_finite()
            && model_norm_sqr.is_finite()
            && sample.weight.is_finite()
        {
            // A point Stokes-I model is real. Keep the general expression for
            // non-finite values so its existing propagation is preserved.
            let scale = sample.weight * sample.model.re;
            Complex32::new(sample.data.re * scale, sample.data.im * scale)
        } else {
            (sample.data * sample.model.conj()) * Complex32::new(sample.weight, 0.0)
        };
        self.weight += weight;
        self.weighted_sample_power += f64::from(sample.weight) * f64::from(power);
        self.sample_count += 1;
        weight
    }
}
