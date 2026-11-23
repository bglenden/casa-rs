// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy continuum fit uses the time-averaged data of every correlation.

use std::collections::{BTreeMap, BTreeSet};

use casa_numerics::solve_weighted_least_squares;
use casa_types::Complex32;

use super::{SelectedChannels, SolveAccumulator, SolveSampleRow, SolveSamples, WeightedTime};
use crate::solve::{GainSolveError, GainSolveMode, kernel::SolutionRow};

#[derive(Clone, Debug, Default)]
pub(in crate::solve) struct Gather {
    pub(in crate::solve) windows: BTreeMap<i32, Spectrum>,
    time: WeightedTime,
}

#[derive(Clone, Debug)]
pub(in crate::solve) struct Spectrum {
    pub(in crate::solve) channels: Vec<usize>,
    pairs: BTreeMap<(i32, i32), Vec<Average>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Average {
    sum: Complex32,
    weight: f32,
}

impl Gather {
    pub(super) fn observe(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlations: &[i32],
        channels: SelectedChannels<'_>,
    ) -> Result<bool, GainSolveError> {
        let count = correlations.len();
        if !matches!(count, 1 | 2 | 4) {
            return Err(GainSolveError::Baseline(
                "unsupported additive fit correlation count".into(),
            ));
        }
        let window = self
            .windows
            .entry(row.data_desc_id)
            .or_insert_with(|| Spectrum {
                channels: channels.indices.to_vec(),
                pairs: BTreeMap::new(),
            });
        let (p, q) = (row.antenna1, row.antenna2);
        let spectrum = window
            .pairs
            .entry((p.min(q), p.max(q)))
            .or_insert_with(|| vec![Average::default(); count * channels.indices.len()]);
        let mut used = false;
        for (position, &channel) in channels.indices.iter().enumerate() {
            if channels.excluded.get(channel).copied().unwrap_or(false)
                || samples.flags.column(channel).iter().any(|flagged| *flagged)
            {
                continue;
            }
            for hand in 0..count {
                let source_hand = if p > q && count == 4 && matches!(hand, 1 | 2) {
                    3 - hand
                } else {
                    hand
                };
                let weight = samples.weights[[source_hand, channel]];
                if !weight.is_finite() || weight <= 0. {
                    continue;
                }
                let data = samples.data[[source_hand, channel]];
                let value = if p > q { data.conj() } else { data };
                let average = &mut spectrum[position * count + hand];
                average.sum += value * weight;
                average.weight += weight;
                used = true;
            }
        }
        if used {
            let first = samples.weights[[0, channels.indices[0]]];
            let last = samples.weights[[count - 1, channels.indices[0]]];
            self.time
                .observe(row.time_seconds, f64::from(0.5 * (first + last)));
        }
        Ok(used)
    }

    pub(in crate::solve) fn solutions(
        &self,
        group: &SolveAccumulator,
        antennas: &BTreeSet<i32>,
        frequencies: &BTreeMap<i32, Vec<f64>>,
        domain: [f64; 2],
        terms: usize,
        mode: GainSolveMode,
    ) -> Vec<SolutionRow> {
        let correlations = *group
            .baseline_correlation_counts
            .first()
            .expect("fit shape");
        let time = self.time.mean().unwrap_or_else(|| group.aggregate_time());
        let cells = terms * correlations;
        antennas
            .iter()
            .flat_map(|&p| antennas.range(p..).map(move |&q| (p, q)))
            .map(|pair| {
                let mut gains = vec![Complex32::ONE; cells];
                let mut flags = vec![true; cells];
                for hand in 0..correlations {
                    let observations = self
                        .windows
                        .iter()
                        .filter_map(|(dd, window)| {
                            window
                                .pairs
                                .get(&pair)
                                .map(|averages| (&frequencies[dd], window, averages))
                        })
                        .flat_map(|(frequencies, window, averages)| {
                            window
                                .channels
                                .iter()
                                .zip(averages[hand..].iter().step_by(correlations))
                                .filter(|(_, average)| average.weight > 0.)
                                .map(move |(&channel, average)| {
                                    let location = coordinate(frequencies[channel], domain);
                                    let value = average.sum / average.weight;
                                    let value = match mode {
                                        GainSolveMode::Phase if value.norm() > 0. => {
                                            value / value.norm()
                                        }
                                        GainSolveMode::Amplitude => {
                                            Complex32::new(value.norm(), 0.)
                                        }
                                        _ => value,
                                    };
                                    (
                                        location,
                                        value,
                                        f64::from(average.weight) / window.channels.len() as f64,
                                    )
                                })
                        })
                        .collect::<Vec<_>>();
                    let coefficients = fit(&observations, terms);
                    if !coefficients.is_empty() {
                        for order in 0..terms {
                            let cell = order * correlations + hand;
                            gains[cell] =
                                coefficients.get(order).copied().unwrap_or(Complex32::ZERO);
                            flags[cell] = order >= coefficients.len();
                        }
                    }
                }
                SolutionRow {
                    time_seconds: time,
                    field_id: group.field_id,
                    spw_id: group.spw_id,
                    antenna_id: pair.0,
                    scan_number: group.scan_number(),
                    observation_id: group.observation_id,
                    refant_id: pair.1,
                    gains,
                    flags,
                    param_errors: vec![0.; cells],
                    snrs: vec![0.; cells],
                    weights: Vec::new(),
                }
            })
            .collect()
    }
}

pub(in crate::solve) fn coordinate(frequency: f64, [low, high]: [f64; 2]) -> f64 {
    if low == high {
        0.
    } else {
        2. * (frequency - low) / (high - low) - 1.
    }
}

fn fit(observations: &[(f64, Complex32, f64)], terms: usize) -> Vec<Complex32> {
    let terms = terms.min(observations.len());
    if terms == 0 {
        return Vec::new();
    }
    let scale = observations
        .iter()
        .map(|sample| sample.2)
        .fold(0., f64::max);
    let mut rows = observations
        .iter()
        .map(|&(x, sample, weight)| {
            let basis = std::iter::successors(Some(1.), move |term| Some(term * x))
                .take(terms)
                .collect();
            (basis, f64::from(sample.re), weight / scale)
        })
        .collect::<Vec<_>>();
    let Some(real) = solve_weighted_least_squares(&rows, terms) else {
        return Vec::new();
    };
    for (row, &(_, sample, _)) in rows.iter_mut().zip(observations) {
        row.1 = f64::from(sample.im);
    }
    let Some(imaginary) = solve_weighted_least_squares(&rows, terms) else {
        return Vec::new();
    };
    real.into_iter()
        .zip(imaginary)
        .map(|(re, im)| Complex32::new(re as f32, im as f32))
        .collect()
}
