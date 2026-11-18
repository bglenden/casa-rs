// SPDX-License-Identifier: LGPL-3.0-or-later
//! The old CASA iterator's baseline spectra for BPOLY.

use std::collections::{BTreeMap, BTreeSet};

use casa_types::Complex32;
use num_complex::Complex64;

use super::{SelectedChannels, SolveSampleRow, SolveSamples, WeightedTime, samples};

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
            let flag = |channel| channel_flags[channel];
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
            if count == 0 {
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
            for (sum, bin) in spectrum
                .weighted_visibilities
                .iter_mut()
                .zip(channels.indices.chunks(width))
            {
                let mut visibility = Complex32::default();
                let mut unflagged = 0;
                for &channel in bin {
                    if !flag(channel) {
                        let data = samples.data[[correlation, channel]];
                        let model = model(channel);
                        if model.norm_sqr() > 0.0 {
                            let divided = Complex64::new(data.re.into(), data.im.into())
                                / Complex64::new(model.re.into(), model.im.into());
                            visibility += Complex32::new(divided.re as f32, divided.im as f32);
                        }
                        unflagged += 1;
                    }
                }
                if unflagged > 0 {
                    let visibility = if reverse_baseline {
                        visibility.conj()
                    } else {
                        visibility
                    };
                    *sum += visibility * (weight / unflagged as f32);
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
