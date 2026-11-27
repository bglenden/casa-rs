// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy continuum fit uses the time-averaged data of every correlation.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::Complex32;
use casa_numerics::solve_weighted_least_squares;

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
    sample_shape: [usize; 2],
    pairs: BTreeMap<(i32, i32), Baseline>,
}

#[derive(Clone, Debug)]
struct Baseline {
    sums: Vec<Complex32>,
    observed: Vec<bool>,
    weights: Vec<f32>,
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
        let sample_shape = [samples.data.nrows(), samples.data.ncols()];
        let window = self
            .windows
            .entry(row.data_desc_id)
            .or_insert_with(|| Spectrum {
                channels: channels.indices.to_vec(),
                sample_shape,
                pairs: BTreeMap::new(),
            });
        if sample_shape != window.sample_shape || channels.indices != window.channels {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: format!("DATA_DESC_ID {} additive fit", row.data_desc_id),
                shape: sample_shape.to_vec(),
            });
        }
        let accepted = channels
            .indices
            .iter()
            .copied()
            .enumerate()
            .filter(|&(_, channel)| {
                !channels.excluded.get(channel).copied().unwrap_or(false)
                    && !samples.flags.column(channel).iter().any(|flagged| *flagged)
            })
            .collect::<Vec<_>>();
        let Some(&(_, first)) = accepted.first() else {
            return Ok(false);
        };
        let (p, q) = (row.antenna1, row.antenna2);
        let spectrum = window
            .pairs
            .entry((p.min(q), p.max(q)))
            .or_insert_with(|| Baseline {
                sums: vec![Complex32::ZERO; count * channels.indices.len()],
                observed: vec![false; count * channels.indices.len()],
                weights: vec![0.; count],
            });
        let mut used = false;
        for hand in 0..count {
            let source_hand = if p > q && count == 4 && matches!(hand, 1 | 2) {
                3 - hand
            } else {
                hand
            };
            let weight = samples.weights[[source_hand, first]];
            if !weight.is_finite() || weight <= 0. {
                continue;
            }
            // VI1 divides every channel by the same total baseline weight.
            spectrum.weights[hand] += weight;
            for &(position, channel) in &accepted {
                let data = samples.data[[source_hand, channel]];
                let value = if p > q { data.conj() } else { data };
                let index = position * count + hand;
                spectrum.sums[index] += value * weight;
                spectrum.observed[index] = true;
            }
            used = true;
        }
        if used {
            let first_weight = samples.weights[[0, first]];
            let last_weight = samples.weights[[count - 1, first]];
            self.time.observe(
                row.time_seconds,
                f64::from(0.5 * (first_weight + last_weight)),
            );
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
                        .flat_map(|(frequencies, window, baseline)| {
                            let weight = baseline.weights[hand];
                            window.channels.iter().enumerate().filter_map(
                                move |(position, &channel)| {
                                    let cell = position * correlations + hand;
                                    if !baseline.observed[cell] {
                                        return None;
                                    }
                                    let location = coordinate(frequencies[channel], domain);
                                    let value = baseline.sums[cell] / weight;
                                    let value = match mode {
                                        GainSolveMode::Phase if value.norm() > 0. => {
                                            value / value.norm()
                                        }
                                        GainSolveMode::Amplitude => {
                                            Complex32::new(value.norm(), 0.)
                                        }
                                        _ => value,
                                    };
                                    Some((
                                        location,
                                        value,
                                        f64::from(weight) / window.channels.len() as f64,
                                    ))
                                },
                            )
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
