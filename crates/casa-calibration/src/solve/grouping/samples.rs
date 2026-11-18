// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered sample arithmetic, separated from per-row array addressing.

use casa_types::Complex32;
use ndarray::Axis;
use num_complex::Complex64;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

use super::{
    AveragedBaseline, GainSolveMode, SolveAccumulator, SolveEdgeAccumulation, SolveSamples,
    median_with_even_mean,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct SolveAccumulationKey {
    pub(super) baseline: AveragedBaseline,
    pub(super) correlation_index: usize,
    pub(super) parallel_hand: bool,
    pub(super) zero_model: bool,
}

impl SolveAccumulator {
    /// Scalar gains start from the parallel-hand phasors in the first
    /// gathered buffer (`TJones::guessPar`). Later buffers enter the solve.
    pub(in crate::solve) fn scalar_initial_gains(
        &self,
        antennas: &BTreeSet<i32>,
        preferred: i32,
        mode: GainSolveMode,
    ) -> Option<HashMap<i32, Complex64>> {
        let buffer = self.first_t_buffer?;
        let samples = self.receptor_accumulations.first()?;
        let first = samples.iter().filter_map(|(key, channels)| {
            let baseline = key.baseline;
            let data = channels.first()?;
            (key.parallel_hand
                && !key.zero_model
                && buffer.contains(baseline)
                && data.weight > f32::EPSILON)
                .then(|| (baseline.antennas, data.weighted_sum / data.weight))
        });
        let guess_antenna = first
            .clone()
            .flat_map(|((a, b), _)| [a, b])
            .min_by_key(|id| (*id != preferred, *id));
        let mut guesses = HashMap::<i32, Complex32>::new();
        let mut amplitude_sum = 0.0;
        let mut count = 0;
        for ((a, b), visibility) in first {
            if (a, b) == buffer.skipped_baseline {
                continue;
            }
            let amplitude = visibility.norm();
            if amplitude <= 0.0 {
                continue;
            }
            let phasor = if mode == GainSolveMode::Amplitude {
                Complex32::new(1.0, 0.0)
            } else {
                visibility / amplitude
            };
            let (antenna, phasor) = if Some(a) == guess_antenna {
                (b, phasor.conj())
            } else if Some(b) == guess_antenna {
                (a, phasor)
            } else {
                continue;
            };
            *guesses.entry(antenna).or_default() += phasor / self.parallel_hands as f32;
            amplitude_sum += if mode == GainSolveMode::Phase {
                1.0
            } else {
                amplitude
            };
            count += 1;
        }
        let scale = if count == 0 {
            0.3
        } else {
            (amplitude_sum / count as f32).sqrt()
        };
        Some(
            antennas
                .iter()
                .map(|&antenna| {
                    let value = guesses
                        .get(&antenna)
                        .filter(|gain| gain.norm_sqr() > 0.0)
                        .map_or(Complex32::new(scale, 0.0), |gain| *gain / scale);
                    (
                        antenna,
                        Complex64::new(f64::from(value.re), f64::from(value.im)),
                    )
                })
                .collect(),
        )
    }
}

/// The selected point model from `VisBufferImpl2::setVisCubeModel`.
/// CASA indexes its basis vector relative to the first selected correlation.
pub(super) fn point_visibility([i, q, u, v]: [f32; 4], code: i32, first_code: i32) -> Complex32 {
    let (parallel, cross) = if code < 9 { (v, (q, u)) } else { (q, (u, v)) };
    match code - first_code {
        0 => Complex32::new(i + parallel, 0.0),
        1 => Complex32::new(cross.0, cross.1),
        2 => Complex32::new(cross.0, -cross.1),
        _ => Complex32::new(i - parallel, 0.0),
    }
}

/// Zero expected visibilities still contribute residuals to a scalar solve.
pub(super) fn zero_model(
    samples: &SolveSamples<'_>,
    correlation: usize,
    channels: &[usize],
    point: Complex32,
) -> bool {
    samples
        .model
        .map_or(point == Complex32::default(), |model| {
            channels
                .iter()
                .all(|&channel| model[[correlation, channel]] == Complex32::default())
        })
}

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
        shared_flags: Option<&[bool]>,
        channels: &[usize],
        point: Complex32,
        all_channels: bool,
    ) -> f64 {
        let data = samples.data.index_axis(Axis(0), correlation);
        let weights = samples.weights.index_axis(Axis(0), correlation);
        let flags = shared_flags.map_or_else(
            || Cow::Owned(samples.flags.row(correlation).to_vec()),
            Cow::Borrowed,
        );
        let model = samples
            .model
            .map(|model| model.index_axis_move(Axis(0), correlation));
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
            return self.add_uniform(
                packed
                    .chunks_exact(correlations)
                    .zip(flags.iter())
                    .map(|(channel, &flagged)| Sample {
                        data: channel[correlation],
                        model: point,
                        weight,
                        flagged,
                    }),
                weight * point.norm_sqr(),
            );
        }
        if all_channels {
            // Strided row iterators avoid repeated two-dimensional indexing;
            // scalar weights retain their broadcast stride of zero.
            let values = data.iter().zip(weights.iter()).zip(flags.iter());
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

    /// CASA's partialMedians for weights in {0, w}: count rejected channels
    /// rather than constructing and selecting an array with two values.
    fn add_uniform(&mut self, samples: impl Iterator<Item = Sample>, weight: f32) -> f64 {
        if !self.channelized || !weight.is_finite() || weight <= 0.0 {
            return self.add::<true>(samples);
        }
        let mut count = 0;
        let mut rejected = 0;
        for (channel, sample) in samples.enumerate() {
            let actual_weight = self.values[channel].add_sample::<true>(self.attempted, sample);
            rejected += usize::from(actual_weight == 0.0);
            count = channel + 1;
        }
        let middle = count / 2;
        if count == 0 || rejected > middle {
            0.0
        } else if count.is_multiple_of(2) {
            let lower = if rejected == middle { 0.0 } else { weight };
            f64::from((lower + weight) / 2.0)
        } else {
            f64::from(weight)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_weight_channel_median_matches_casas_partial_medians() {
        // Eight channels with weights 0 or 2 have medians 2, 1, and 0
        // when three, four, or five channels are rejected respectively.
        for (channels, flagged, expected) in [
            (8, 3, 2.0),
            (8, 4, 1.0),
            (8, 5, 0.0),
            (7, 3, 2.0),
            (7, 4, 0.0),
        ] {
            let mut values = vec![SolveEdgeAccumulation::default(); channels];
            let mut attempted = false;
            let median = SampleAccumulation {
                values: &mut values,
                attempted: &mut attempted,
                channelized: true,
            }
            .add_uniform(
                (0..channels).map(|channel| Sample {
                    data: Complex32::new(4.0, 0.0),
                    model: Complex32::new(2.0, 0.0),
                    weight: 0.5,
                    flagged: channel < flagged,
                }),
                2.0,
            );
            assert_eq!(median, expected);
            assert!(attempted);
            assert_eq!(
                values.iter().map(|v| v.sample_count).sum::<usize>(),
                channels - flagged
            );
        }
    }
}
