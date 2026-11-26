// SPDX-License-Identifier: LGPL-3.0-or-later
//! The old CASA iterator's baseline spectra for BPOLY.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::Complex32;
use casa_ms::ChannelSelection;
use num_complex::Complex64;

use super::{
    GainSolveError, SelectedChannels, SolveAccumulator, SolveSampleRow, SolveSamples, WeightedTime,
    samples,
};

/// The old CASA iterator selects a continuous channel range. It replaces
/// strides with one and flags holes between the selected segments.
pub(crate) fn channel_selection(
    selection: Option<&ChannelSelection>,
    count: usize,
) -> Result<(Vec<usize>, Vec<bool>), GainSolveError> {
    let Some(selection) = selection else {
        return Ok(((0..count).collect(), Vec::new()));
    };
    let mut selection = selection.clone();
    for segment in &mut selection.segments {
        segment.stride = 1;
    }
    let mut selected = super::selected_channels(Some(&selection), count)?;
    selected.sort_unstable();
    let mut excluded = vec![true; count];
    for &channel in &selected {
        excluded[channel] = false;
    }
    let envelope = selected
        .first()
        .zip(selected.last())
        .map_or_else(Vec::new, |(&first, &last)| (first..=last).collect());
    Ok((envelope, excluded))
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PolynomialAccumulation {
    spectra: BTreeMap<(i32, i32, [i32; 2], usize), BaselineSpectrum>,
    descriptions: BTreeSet<i32>,
    time: WeightedTime,
}

#[derive(Debug, Clone)]
pub(crate) struct BaselineSpectrum {
    pub(crate) weighted_visibilities: Vec<Complex32>,
    pub(crate) weight: f32,
}

impl SolveAccumulator {
    pub(super) fn observe_polynomial(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlations: &[i32],
        channels: SelectedChannels<'_>,
        point_stokes: [f32; 4],
    ) {
        self.observe_metadata(row, false);
        self.polynomial
            .get_or_insert_with(Default::default)
            .observe(row, samples, correlations, channels, point_stokes);
    }
}

impl PolynomialAccumulation {
    pub(crate) fn descriptions(&self) -> &BTreeSet<i32> {
        &self.descriptions
    }

    pub(crate) fn spectra(
        &self,
    ) -> impl Iterator<Item = (i32, [i32; 2], usize, &BaselineSpectrum)> {
        self.spectra
            .iter()
            .map(|(&(description, _, antennas, hand), spectrum)| {
                (description, antennas, hand, spectrum)
            })
    }

    pub(crate) fn time(&self) -> Option<f64> {
        self.time.mean()
    }

    pub(super) fn observe(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlations: &[i32],
        channels: SelectedChannels<'_>,
        point_stokes: [f32; 4],
    ) {
        let width = channels.bin_width.unwrap_or(1);
        let channel_flags = samples::channel_flags(samples);
        let bin_count = channels.indices.len().div_ceil(width);
        let excluded = |channel: usize| channels.excluded.get(channel).copied().unwrap_or(false);
        let weight_at = |correlation, channel| {
            channels
                .bin_weights
                .map_or(1.0, |weights| weights[[correlation, channel]])
        };
        let usable = |correlation, channel| {
            !excluded(channel)
                && !samples.flags[[correlation, channel]]
                && weight_at(correlation, channel) > 0.0
        };
        // VisBuffer::channelAve averages each correlation separately.
        // Its output channel is shared-flagged if one hand has no sample.
        let averaged_flags = (width > 1).then(|| {
            channels
                .indices
                .chunks(width)
                .map(|bin| {
                    (0..correlations.len())
                        .any(|correlation| bin.iter().all(|&channel| !usable(correlation, channel)))
                })
                .collect::<Vec<_>>()
        });
        let mut antennas = [row.antenna1, row.antenna2];
        let reverse_baseline = antennas[0] > antennas[1];
        antennas.sort_unstable();
        let mut time_weight = 0.0;
        let last = correlations.len() - 1;
        for (hand, correlation) in [0, last]
            .into_iter()
            .take(correlations.len().min(2))
            .enumerate()
        {
            let point = samples.point_model.map_or_else(
                || {
                    samples::point_visibility(
                        point_stokes,
                        correlations[correlation],
                        correlations[0],
                    )
                },
                |model| model[correlation],
            );
            let model = |channel| {
                samples
                    .model
                    .map_or(point, |model| model[[correlation, channel]])
            };
            let flag = |channel: usize| channel_flags[channel] || excluded(channel);
            let mut amplitude_sum = 0.0;
            let mut count = 0;
            for &channel in channels.indices {
                if !flag(channel) {
                    let amplitude = model(channel).norm();
                    if amplitude > 0.0 {
                        amplitude_sum += amplitude;
                        count += 1;
                    }
                }
            }
            if count == 0
                || averaged_flags
                    .as_ref()
                    .is_some_and(|flags| flags.iter().all(|flag| *flag))
            {
                continue;
            }

            // VisBuffer::normalize scales the scalar weight by the squared
            // mean model amplitude. VisBuffAccumulator then assigns that
            // same row weight to all channels, including flagged ones in the
            // denominator of a time average.
            let weight = samples.weights[[correlation, 0]]
                * (amplitude_sum / count as f32).powi(2)
                * width as f32;
            if weight <= 0.0 {
                continue;
            }
            let spectrum = self
                .spectra
                .entry((row.data_desc_id, row.field_id, antennas, hand))
                .or_insert_with(|| BaselineSpectrum {
                    weighted_visibilities: vec![Complex32::default(); bin_count],
                    weight: 0.0,
                });
            spectrum.weight += weight;
            for (index, (sum, bin)) in spectrum
                .weighted_visibilities
                .iter_mut()
                .zip(channels.indices.chunks(width))
                .enumerate()
            {
                if averaged_flags.as_ref().is_some_and(|flags| flags[index]) {
                    continue;
                }
                let mut visibility = Complex32::default();
                let mut weight_sum = 0.0_f64;
                for &channel in bin {
                    if if width == 1 {
                        !flag(channel)
                    } else {
                        usable(correlation, channel)
                    } {
                        let data = samples.data[[correlation, channel]];
                        let model = model(channel);
                        let channel_weight = if width == 1 {
                            1.0
                        } else {
                            weight_at(correlation, channel)
                        };
                        // Normalization precedes binning. It only touches a
                        // channel when the input's shared flag is clear.
                        if flag(channel) {
                            visibility += data * channel_weight;
                        } else if model.norm_sqr() > 0.0 {
                            let divided = Complex64::new(data.re.into(), data.im.into())
                                / Complex64::new(model.re.into(), model.im.into());
                            visibility += Complex32::new(divided.re as f32, divided.im as f32)
                                * channel_weight;
                        }
                        weight_sum += f64::from(channel_weight);
                    }
                }
                if weight_sum > 0.0 {
                    let visibility = if reverse_baseline {
                        visibility.conj()
                    } else {
                        visibility
                    };
                    *sum += visibility * (weight / weight_sum as f32);
                }
            }
            time_weight += f64::from(weight);
        }
        if time_weight > 0.0 {
            self.descriptions.insert(row.data_desc_id);
            self.time.observe(row.time_seconds, time_weight);
        }
    }
}
