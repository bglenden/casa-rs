// SPDX-License-Identifier: LGPL-3.0-or-later
//! The old iterator's model normalization and baseline time average.

use std::collections::{BTreeMap, BTreeSet};

use num_complex::Complex64;

use super::samples::point_visibility;
use super::time::WeightedTime;
use super::{
    Complex32, SelectedChannels, SolveAccumulator, SolveGroupOptions, SolveSampleRow, SolveSamples,
};
use crate::solve::{GainSolveError, GainSolveMode, kernel::SolutionRow};

#[derive(Clone, Debug, Default)]
pub(super) struct LegacyMuellerGather {
    pairs: BTreeMap<(i32, i32), LegacyBaseline>,
    shape: Option<(usize, usize)>,
    first_time: Option<f64>,
    time: WeightedTime,
}

#[derive(Clone, Debug)]
struct LegacyBaseline {
    sums: Vec<Complex32>,
    flagged: Vec<bool>,
    weight: [f32; 2],
}

impl LegacyBaseline {
    fn new(channels: usize) -> Self {
        Self {
            sums: vec![Complex32::ZERO; 2 * channels],
            flagged: vec![true; 2 * channels],
            weight: [0.; 2],
        }
    }
}

impl SolveAccumulator {
    pub(super) fn observe_legacy_baseline(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlations: &[i32],
        channels: SelectedChannels<'_>,
        options: &SolveGroupOptions,
    ) -> Result<(), GainSolveError> {
        self.observe_metadata(row, false);
        let gather = self.legacy_baselines.get_or_insert_with(Default::default);
        let (channel_count, has_data) =
            gather.observe(row, samples, correlations, channels, options)?;
        self.solution_channels = channel_count;
        self.attempted |= has_data;
        Ok(())
    }

    pub(in crate::solve) fn legacy_baseline_solutions(
        &self,
        antennas: &BTreeSet<i32>,
        mode: GainSolveMode,
    ) -> Option<Vec<SolutionRow>> {
        let gather = self.legacy_baselines.as_ref()?;
        let time = gather.time.mean().unwrap_or_else(|| self.aggregate_time());
        let cells = 2 * self.solution_channels;
        let rows = antennas
            .iter()
            .flat_map(|&p| antennas.range(p..).map(move |&q| (p, q)))
            .map(|pair| {
                let mut gains = vec![Complex32::new(1., 0.); cells];
                let mut flags = vec![true; cells];
                if let Some(responses) = gather.pairs.get(&pair) {
                    flags.clone_from(&responses.flagged);
                    for (index, (gain, flag)) in gains.iter_mut().zip(&flags).enumerate() {
                        if !flag {
                            let weight = responses.weight[index % 2];
                            let value = if weight > 0. {
                                responses.sums[index] / weight
                            } else {
                                Complex32::ZERO
                            };
                            *gain = match mode {
                                GainSolveMode::Phase if value.norm() > 0. => value / value.norm(),
                                GainSolveMode::Amplitude => Complex32::new(value.norm(), 0.),
                                _ => value,
                            };
                        }
                    }
                }
                SolutionRow {
                    time_seconds: time,
                    field_id: self.field_id,
                    spw_id: self.spw_id,
                    antenna_id: pair.0,
                    scan_number: self.scan_number(),
                    observation_id: self.observation_id,
                    refant_id: pair.1,
                    gains,
                    flags,
                    param_errors: vec![0.; cells],
                    snrs: vec![0.; cells],
                    weights: Vec::new(),
                }
            })
            .collect();
        Some(rows)
    }
}

impl LegacyMuellerGather {
    fn observe(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlations: &[i32],
        channels: SelectedChannels<'_>,
        options: &SolveGroupOptions,
    ) -> Result<(usize, bool), GainSolveError> {
        let channel_count = if options.channelized {
            channels.indices.len()
        } else {
            1
        };
        let shape = (channel_count, correlations.len());
        if *self.shape.get_or_insert(shape) != shape {
            return Err(GainSolveError::Baseline(
                "legacy baseline buffers have incompatible shapes".into(),
            ));
        }
        let accepted = channels
            .indices
            .iter()
            .enumerate()
            .filter(|&(_, &channel)| {
                !channels.excluded.get(channel).copied().unwrap_or(false)
                    && !samples.flags.column(channel).iter().any(|&flagged| flagged)
            })
            .collect::<Vec<_>>();
        if accepted.is_empty() {
            return Ok((channel_count, false));
        }
        let first = *self.first_time.get_or_insert(row.time_seconds);
        if options
            .preaverage_max_span
            .is_some_and(|span| row.time_seconds - first >= span)
        {
            return Err(GainSolveError::Baseline(
                "legacy baseline preaveraging produced multiple baseline blocks".into(),
            ));
        }
        let last = match correlations.len() {
            1 => 0,
            2 => 1,
            4 => 3,
            _ => {
                return Err(GainSolveError::Baseline(
                    "unsupported legacy baseline correlation count".into(),
                ));
            }
        };
        let (p, q) = (row.antenna1, row.antenna2);
        let baseline = self
            .pairs
            .entry((p.min(q), p.max(q)))
            .or_insert_with(|| LegacyBaseline::new(channel_count));
        let mut row_weight = 0.;
        for (hand, correlation) in [0, last]
            .into_iter()
            .take(correlations.len().min(2))
            .enumerate()
        {
            let point = samples.point_model.map_or_else(
                || {
                    point_visibility(
                        options.point_stokes,
                        correlations[correlation],
                        correlations[0],
                    )
                },
                |point| point[correlation],
            );
            let mut values = Vec::with_capacity(accepted.len());
            let (mut model_sum, mut model_count) = (0., 0);
            for &(output, &channel) in &accepted {
                let model = samples
                    .model
                    .map_or(point, |model| model[[correlation, channel]]);
                let data = samples.data[[correlation, channel]];
                let amplitude = model.norm();
                let value = if amplitude > 0. {
                    model_sum += amplitude;
                    model_count += 1;
                    let divided = Complex64::new(data.re.into(), data.im.into())
                        / Complex64::new(model.re.into(), model.im.into());
                    Complex32::new(divided.re as f32, divided.im as f32)
                } else {
                    Complex32::ZERO
                };
                values.push((output, if p > q { value.conj() } else { value }));
            }
            let weight = if model_count > 0 {
                let scale = model_sum / model_count as f32;
                samples.weights[[correlation, *accepted[0].1]] * scale * scale
            } else {
                0.
            };
            row_weight += weight;
            baseline.weight[hand] += weight;
            if options.channelized {
                for (channel, value) in values {
                    baseline.sums[2 * channel + hand] += value * weight;
                    baseline.flagged[2 * channel + hand] = false;
                }
            } else {
                let sum = values
                    .into_iter()
                    .map(|(_, value)| value)
                    .sum::<Complex32>();
                baseline.sums[hand] += sum * (1. / accepted.len() as f32) * weight;
                baseline.flagged[hand] = false;
            }
        }
        // The old time averager uses the mean of the first and last hand.
        if last > 0 {
            row_weight *= 0.5;
        }
        self.time.observe(row.time_seconds, f64::from(row_weight));
        Ok((channel_count, row_weight > 0.))
    }
}
