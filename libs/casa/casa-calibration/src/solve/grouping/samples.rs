// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered sample arithmetic, separated from per-row array addressing.

use casa_core::Complex32;
use casa_ms::{ChannelSelection, MeasurementSet};
use ndarray::{ArrayD, ArrayView2, Axis, Ix2, IxDyn, ShapeBuilder};
use num_complex::Complex64;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use crate::execute::{ParallacticAngleState, SolvePriorApply};
use crate::solve::model::ModelInputs;

use super::{
    AveragedBaseline, GainSolveError, GainSolveMode, SelectedChannels, SelectedSolveRow,
    SolveAccumulator, SolveSamples, get_i32,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct SolveAccumulationKey {
    pub(super) baseline: AveragedBaseline,
    pub(super) correlation_index: usize,
    pub(super) parallel_hand: bool,
}

/// Apply the requested amplitude/phase mode to one collapsed visibility.
pub(super) fn transform_solve_sample(
    visibility: Complex32,
    weight: f32,
    mode: GainSolveMode,
    fixed_auto: bool,
) -> Option<(Complex32, f32)> {
    Some(match mode {
        GainSolveMode::Phase => {
            let amplitude = visibility.norm();
            if fixed_auto && amplitude == 0.0 {
                // CASA leaves the nominal weight and zero DATA in the residual.
                return Some((visibility, weight));
            }
            if !fixed_auto && amplitude <= f32::EPSILON {
                return None;
            }
            (
                visibility / Complex32::new(amplitude, 0.0),
                weight * amplitude * amplitude,
            )
        }
        GainSolveMode::Amplitude => (Complex32::new(visibility.norm(), 0.0), weight),
        GainSolveMode::AmplitudePhase => (visibility, weight),
    })
}

/// One MAIN row's checked two-dimensional solve arrays. Scalar SIGMA weights
/// have one channel, broadcast to the data's channel count without copying.
pub(super) struct SolveRowInputs<'a> {
    pub(super) data: &'a ArrayD<Complex32>,
    pub(super) flags: Cow<'a, ArrayD<bool>>,
    pub(super) weights: ArrayD<f32>,
    pub(super) model: Option<&'a ArrayD<Complex32>>,
}

impl SolveRowInputs<'_> {
    /// The arrays as [`SolveSamples`] views.
    pub(super) fn samples(&self) -> SolveSamples<'_> {
        SolveSamples {
            data: two_dimensional(self.data),
            flags: two_dimensional(&self.flags),
            weights: self
                .weights
                .broadcast(self.data.raw_dim())
                .expect("checked receptor/channel weight shape")
                .into_dimensionality::<Ix2>()
                .expect("checked two-dimensional"),
            model: self.model.map(two_dimensional),
            point_model: None,
        }
    }
}

/// A MAIN cell checked two-dimensional, as a view.
pub(super) fn two_dimensional<T>(cell: &ArrayD<T>) -> ArrayView2<'_, T> {
    cell.view()
        .into_dimensionality::<Ix2>()
        .expect("checked two-dimensional")
}

pub(super) struct SolveRowLayouts<'a> {
    pub(super) models: &'a ModelInputs,
    pub(super) by_ddid: HashMap<i32, SolveRowLayout>,
    pub(super) rotation: Option<ParallacticAngleState>,
    pub(super) forward_model: Option<&'a SolvePriorApply>,
    pub(super) channel_bin_widths: &'a BTreeMap<i32, usize>,
}

impl<'a> SolveRowLayouts<'a> {
    pub(super) fn new(
        ms: &MeasurementSet,
        rotate_model: bool,
        models: &'a ModelInputs,
        channel_bin_widths: &'a BTreeMap<i32, usize>,
    ) -> Result<Self, GainSolveError> {
        let rotation = rotate_model
            .then(|| crate::execute::load_parallactic_angle_state(ms))
            .transpose()
            .map_err(|source| GainSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .unwrap_or(Path::new("<memory>"))
                    .display()
                    .to_string(),
                source,
            })?;
        Ok(Self {
            by_ddid: HashMap::new(),
            rotation,
            forward_model: None,
            models,
            channel_bin_widths,
        })
    }
}

/// A data description's row-invariant layout; shape/selection changes refresh
/// the plan, so variable-shaped cells keep the same validation.
pub(super) struct SolveRowLayout {
    pub(super) correlations: Vec<i32>,
    pub(super) channel_count: usize,
    pub(super) selection: Option<ChannelSelection>,
    pub(super) channels: Vec<usize>,
    pub(super) excluded: Vec<bool>,
    pub(super) all_channels: bool,
}

impl SolveRowLayout {
    pub(super) fn select_channels(
        &mut self,
        row: &SelectedSolveRow,
        channel_count: usize,
        polynomial: bool,
    ) -> Result<(), GainSolveError> {
        let selection = row.channel_selection.as_ref();
        (self.channels, self.excluded) = if polynomial {
            super::polynomial::channel_selection(selection, channel_count)?
        } else {
            (
                super::selected_channels(selection, channel_count)?,
                Vec::new(),
            )
        };
        self.all_channels = self.channels.iter().copied().eq(0..channel_count);
        self.channel_count = channel_count;
        self.selection.clone_from(&row.channel_selection);
        Ok(())
    }
}

impl SolveAccumulator {
    pub(super) fn update_solution_channels(
        &mut self,
        channels: SelectedChannels<'_>,
        gain_type: super::GainType,
        data_shape: &[usize],
    ) -> Result<(), GainSolveError> {
        let channel_count = channels
            .indices
            .len()
            .div_ceil(channels.bin_width.unwrap_or(1));
        if !gain_type.is_delay() && self.sample_rows > 0 && self.solution_channels != channel_count
        {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<measurement-set DATA>".to_string(),
                shape: data_shape.to_vec(),
            });
        }
        self.solution_channels = self.solution_channels.max(channel_count);
        Ok(())
    }

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
            let (first, second) = baseline.antennas;
            let data = channels.first()?;
            (first != second
                && key.parallel_hand
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

pub(super) fn corrupt_model(
    gains: &ArrayD<Complex32>,
    model: Option<&ArrayD<Complex32>>,
    rotated_point: Option<&[Complex32]>,
    correlations: &[i32],
    stokes: [f32; 4],
) -> ArrayD<Complex32> {
    ArrayD::from_shape_fn(gains.raw_dim().f(), |index| {
        let correlation = index[0];
        let visibility = if let Some(model) = model {
            model[&index]
        } else if let Some(point) = rotated_point {
            point[correlation]
        } else {
            point_visibility(stokes, correlations[correlation], correlations[0])
        };
        gains[index] * visibility
    })
}

pub(super) fn rotate_model(
    ms: &MeasurementSet,
    row: &SelectedSolveRow,
    state: &crate::execute::ParallacticAngleState,
    correlations: &[i32],
    stokes: [f32; 4],
    model: Option<&mut ArrayD<Complex32>>,
) -> Result<Option<Vec<Complex32>>, GainSolveError> {
    let apply_error = |source| GainSolveError::PriorCalibrationApply {
        source: Box::new(source),
    };
    let angle = |antenna, feed_column| {
        let feed = get_i32(ms.main_table(), row.row_index, feed_column)?;
        state
            .feed_parallactic_angle(
                row.time_seconds,
                row.field_id,
                antenna,
                feed,
                row.data_spw_id,
            )
            .map_err(apply_error)
    };
    // The apply kernel inverts P. Negating its angles applies P to the sky model.
    let first = -angle(row.antenna1, "FEED1")?;
    let second = -angle(row.antenna2, "FEED2")?;
    let rotate = |model| {
        crate::execute::parang::apply(row.data_desc_id, correlations, first, second, model)
            .map_err(apply_error)
    };
    if let Some(model) = model {
        rotate(model)?;
        return Ok(None);
    }
    let mut point = ArrayD::from_shape_fn(IxDyn(&[correlations.len(), 1]), |index| {
        point_visibility(stokes, correlations[index[0]], correlations[0])
    });
    rotate(&mut point)?;
    Ok(Some(point.into_raw_vec_and_offset().0))
}

/// The median, the mean of the middle two for an even count, as casacore's
/// `partialMedians(..., takeEvenMean=True)` that CASA uses for a row's
/// channel-independent weight.
pub(crate) fn median_with_even_mean(values: &mut [f32]) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    let middle = values.len() / 2;
    let even = values.len().is_multiple_of(2);
    let first = values[0];
    if values
        .iter()
        .all(|value| value.to_bits() == first.to_bits())
    {
        return if even { (first + first) / 2.0 } else { first };
    }
    let (lower, upper, _) = values.select_nth_unstable_by(middle, f32::total_cmp);
    let upper = *upper;
    if even {
        let lower = lower
            .iter()
            .copied()
            .max_by(f32::total_cmp)
            .expect("nonempty even lower half");
        (lower + upper) / 2.0
    } else {
        upper
    }
}

/// The time averager sees all correlations before G restricts the fit.
pub(super) fn centroid_weight(
    samples: &SolveSamples<'_>,
    correlation: usize,
    channels: SelectedChannels<'_>,
    shared_flags: Option<&[bool]>,
    normalization: impl Fn(usize) -> Complex32,
    channelized: bool,
) -> f64 {
    let flagged = |channel| {
        shared_flags.map_or_else(
            || samples.flags[[correlation, channel]],
            |flags| flags[channel],
        )
    };
    let weight = |&channel: &usize| {
        if flagged(channel) {
            0.0
        } else {
            let power = normalization(channel).norm_sqr();
            let scale = if power == 0.0 { 1.0 } else { power };
            samples.weights[[correlation, channel]] * scale
        }
    };
    if let Some(width) = channels.bin_width {
        let weights = channels
            .indices
            .chunks(width)
            .filter(|bin| bin.iter().any(|&channel| !flagged(channel)))
            .map(|bin| bin.iter().map(weight).sum::<f32>());
        mean_weights(weights)
    } else if channelized {
        // VisEquation::divideCorrByModel replaces each scalar row weight
        // by the median of the channel weights after model normalization.
        let mut weights = channels.indices.iter().map(weight).collect::<Vec<_>>();
        f64::from(median_with_even_mean(&mut weights))
    } else {
        channels.indices.iter().map(weight).map(f64::from).sum()
    }
}

/// CASA's correlation-independent flags are shared by every receptor.
/// Direct antenna solve counts include correlations that are later excluded
/// from the fit. The first and last input positions define the two counters.
#[derive(Debug, Clone, Default)]
pub(super) struct DirectGainGather {
    pub(super) with_data: [BTreeSet<i32>; 2],
    pub(super) attempted: bool,
}

impl DirectGainGather {
    pub(super) fn observe(
        &mut self,
        row: &super::SolveSampleRow,
        samples: &SolveSamples<'_>,
        channels: &[usize],
        shared_flags: Option<&[bool]>,
    ) {
        let positive = |correlation| {
            channels.iter().any(|&channel| {
                !shared_flags.is_some_and(|flags| flags[channel])
                    && !samples.flags[[correlation, channel]]
                    && samples.weights[[correlation, channel]] > 0.0
            })
        };
        let Some(last) = samples.data.nrows().checked_sub(1) else {
            return;
        };
        for (hand, correlation) in [0, last].into_iter().enumerate() {
            if positive(correlation) {
                self.with_data[hand].extend([row.antenna1, row.antenna2]);
                self.attempted = true;
            }
        }
        if !self.attempted {
            self.attempted = (1..last).any(positive);
        }
    }
}

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
    pub(super) keep_zero_data: bool,
    pub(super) retain_zero_model: bool,
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
        channels: SelectedChannels<'_>,
        point: Complex32,
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
        let selected = |&channel: &usize| Sample {
            data: data[channel],
            model: model.as_ref().map_or(point, |model| model[channel]),
            weight: weights[channel],
            flagged: flags[channel],
        };
        if let Some(width) = channels.bin_width {
            let weights = self
                .values
                .iter_mut()
                .zip(channels.indices.chunks(width))
                .filter_map(|(value, bin)| {
                    let (average, unflagged) =
                        average_channels::<false, true>(self.attempted, bin.iter().map(selected));
                    value.merge(average);
                    unflagged.then_some(average.weight)
                });
            return mean_weights(weights);
        }
        if self.keep_zero_data {
            let selected = channels.indices.iter().map(selected);
            return if self.channelized {
                self.add_ordered::<true, false, true>(selected)
            } else {
                self.add_scalar_row::<false, true>(selected)
            };
        }
        let correlations = samples.data.nrows();
        if channels.all_channels
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
        if channels.all_channels {
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
            self.add::<false>(channels.indices.iter().map(selected))
        }
    }

    fn add<const POINT: bool>(&mut self, samples: impl Iterator<Item = Sample>) -> f64 {
        if self.channelized {
            self.add_ordered::<true, POINT, false>(samples)
        } else {
            self.add_ordered::<false, POINT, false>(samples)
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
            let actual_weight = self.values[channel].add_sample::<true, false>(
                self.attempted,
                sample,
                self.retain_zero_model,
            );
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

    fn add_ordered<const CHANNELIZED: bool, const POINT: bool, const KEEP_ZERO_DATA: bool>(
        &mut self,
        samples: impl Iterator<Item = Sample>,
    ) -> f64 {
        if !CHANNELIZED && self.retain_zero_model {
            return self.add_scalar_row::<POINT, KEEP_ZERO_DATA>(samples);
        }
        let mut weights = Vec::new();
        if CHANNELIZED {
            weights.reserve(self.values.len());
        }
        let mut summed_weight = 0.0_f64;
        for (channel, sample) in samples.enumerate() {
            let output = if CHANNELIZED { channel } else { 0 };
            let weight = self.values[output].add_sample::<POINT, KEEP_ZERO_DATA>(
                self.attempted,
                sample,
                self.retain_zero_model,
            );
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

    fn add_scalar_row<const POINT: bool, const KEEP_ZERO_DATA: bool>(
        &mut self,
        samples: impl Iterator<Item = Sample>,
    ) -> f64 {
        let (average, _) = average_channels::<POINT, KEEP_ZERO_DATA>(self.attempted, samples);
        self.values[0].merge(average);
        f64::from(average.weight)
    }
}

fn average_channels<const POINT: bool, const KEEP_ZERO_DATA: bool>(
    attempted: &mut bool,
    samples: impl Iterator<Item = Sample>,
) -> (SolveEdgeAccumulation, bool) {
    let mut average = SolveEdgeAccumulation::default();
    let mut channel_count = 0;
    let mut model_channels = 0;
    for sample in samples {
        if !sample.flagged {
            channel_count += 1;
            model_channels += usize::from(sample.model.norm_sqr() > 0.0);
        }
        average.add_sample::<POINT, KEEP_ZERO_DATA>(attempted, sample, true);
    }
    if channel_count > 0 {
        // ChannelAverageTVI averages the normalized model with flags
        // alone. VbAvg then time-averages that model with data weights.
        average.model_weight = average.weight * model_channels as f32 / channel_count as f32;
    }
    (average, channel_count > 0)
}

/// The old GSPLINE iterator divides by the model before frequency averaging.
/// The row weight uses the squared mean model amplitude over accepted channels.
pub(super) fn collapse_spline_channels(
    samples: &SolveSamples<'_>,
    correlation: usize,
    channels: &[usize],
    flags: &[bool],
    point: Complex32,
) -> SolveEdgeAccumulation {
    let mut data = Complex32::default();
    let mut amplitude = 0.;
    let mut modeled_channels = 0;
    let mut count = 0;
    let promote =
        |value: Complex32| num_complex::Complex64::new(f64::from(value.re), f64::from(value.im));
    for &channel in channels {
        if !flags[channel] {
            count += 1;
            let sample = samples.data[[correlation, channel]];
            if let Some(model) = &samples.model {
                let model = model[[correlation, channel]];
                let scale = model.norm();
                if scale > 0. {
                    let normalized = promote(sample) / promote(model);
                    data += Complex32::new(normalized.re as f32, normalized.im as f32);
                    amplitude += scale;
                    modeled_channels += 1;
                }
            } else {
                data += sample;
            }
        }
    }
    if count == 0 {
        return SolveEdgeAccumulation::default();
    }
    let scale = (count as f32).recip();
    let (normalized, model_power) = if samples.model.is_some() {
        (
            data * scale,
            (amplitude / modeled_channels.max(1) as f32).powi(2),
        )
    } else if point.norm_sqr() > 0. {
        let normalized = promote(data * scale) / promote(point);
        (
            Complex32::new(normalized.re as f32, normalized.im as f32),
            point.norm_sqr(),
        )
    } else {
        return SolveEdgeAccumulation::default();
    };
    let weight = samples.weights[[correlation, channels[0]]] * model_power;
    SolveEdgeAccumulation {
        weighted_sum: normalized * weight,
        weight,
        ..SolveEdgeAccumulation::default()
    }
}

/// After frequency averaging, FreqAxisTVI uses the mean of the weights of
/// unflagged bins as the scalar weight for the following time average.
fn mean_weights(weights: impl Iterator<Item = f32>) -> f64 {
    let (sum, count) = weights.fold((0.0, 0), |(sum, count), value| (sum + value, count + 1));
    f64::from(sum / count.max(1) as f32)
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct SolveEdgeAccumulation {
    pub(super) weighted_sum: Complex32,
    pub(super) weight: f32,
    pub(super) model_weight: f32,
    pub(super) weighted_sample_power: f64,
    pub(super) sample_count: usize,
}

impl SolveEdgeAccumulation {
    pub(super) fn merge(&mut self, other: Self) {
        self.weighted_sum += other.weighted_sum;
        self.weight += other.weight;
        self.model_weight += other.model_weight;
        self.weighted_sample_power += other.weighted_sample_power;
        self.sample_count += other.sample_count;
    }

    /// Accumulate w |M|² (D/M) = w D conj(M), with power w |D|².
    /// Cancelling the model division avoids two scalar divides per sample.
    fn add_sample<const POINT: bool, const KEEP_ZERO_DATA: bool>(
        &mut self,
        attempted: &mut bool,
        sample: Sample,
        retain_zero_model: bool,
    ) -> f32 {
        if sample.weight <= 0.0 || sample.flagged {
            return 0.0;
        }
        let model_norm_sqr = sample.model.norm_sqr();
        let zero_model = model_norm_sqr == 0.0;
        if zero_model && !retain_zero_model {
            return 0.0;
        }
        *attempted = true;
        let power = sample.data.norm_sqr();
        if !KEEP_ZERO_DATA && !zero_model && power <= f32::EPSILON * f32::EPSILON * model_norm_sqr {
            return 0.0;
        }
        // CASA keeps data and nominal weight for zero model cells. After
        // averaging, their model contribution is zero instead of unity.
        let weight = if zero_model {
            sample.weight
        } else {
            sample.weight * model_norm_sqr
        };
        self.weighted_sum += if zero_model {
            sample.data * sample.weight
        } else if POINT
            && sample.model.im == 0.0
            && power.is_finite()
            && model_norm_sqr.is_finite()
            && sample.weight.is_finite()
        {
            // Real point models need only a scalar multiply. Complex and
            // non-finite values use the full expression.
            let scale = sample.weight * sample.model.re;
            Complex32::new(sample.data.re * scale, sample.data.im * scale)
        } else {
            (sample.data * sample.model.conj()) * Complex32::new(sample.weight, 0.0)
        };
        self.weight += weight;
        if !zero_model {
            self.model_weight += weight;
        }
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
                keep_zero_data: false,
                retain_zero_model: false,
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
