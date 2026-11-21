// SPDX-License-Identifier: LGPL-3.0-or-later
//! Solve the averaged coherencies and carry the leakage reference across intervals.

use num_complex::Complex32;

use super::fit::LeakageFit;
use super::standard::{StandardSample, solve_channel};
use super::{
    CoherencyMean, LeakageEstimate, LeakageSolution, LeakageSolveKind, LeakageSolveRequest,
};

pub(super) enum IntervalFits {
    LeastSquares(Vec<LeakageFit>),
    Iterative(Vec<Vec<StandardSample>>),
}

type ChannelEstimates = Vec<LeakageEstimate>;
type IntervalEstimates = (i32, Vec<ChannelEstimates>);

impl IntervalFits {
    pub(super) fn new(channels: usize, antennas: usize, kind: LeakageSolveKind) -> Self {
        if kind.iterative() {
            Self::Iterative((0..channels).map(|_| Vec::new()).collect())
        } else {
            Self::LeastSquares((0..channels).map(|_| LeakageFit::new(antennas)).collect())
        }
    }

    pub(super) fn accumulate(
        &mut self,
        pair: [i32; 2],
        bins: &mut [CoherencyMean],
        antennas: usize,
    ) {
        let pair = pair.map(|antenna| antenna as usize);
        if pair.iter().any(|&antenna| antenna >= antennas) {
            return;
        }
        for (channel, mean) in bins.iter_mut().enumerate() {
            if let Some((data, model, weights)) = std::mem::take(mean).finish() {
                match self {
                    Self::LeastSquares(fits) => {
                        fits[channel].add_baseline(pair, data, model, weights)
                    }
                    Self::Iterative(samples) => samples[channel].push(StandardSample {
                        pair,
                        cross_hands: [data[1], data[2]],
                        model,
                        weights: [weights[1], weights[2]],
                    }),
                }
            }
        }
    }

    pub(super) fn finish(
        self,
        antennas: usize,
        references: &[i32],
        previous: Option<&LeakageSolution>,
        request: &LeakageSolveRequest,
    ) -> Option<IntervalEstimates> {
        match self {
            Self::LeastSquares(fits) => least_squares(fits, antennas, references, previous),
            Self::Iterative(samples) => {
                iterative(samples, antennas, request).map(|values| (-1, values))
            }
        }
    }
}

fn least_squares(
    fits: Vec<LeakageFit>,
    antennas: usize,
    references: &[i32],
    previous: Option<&LeakageSolution>,
) -> Option<IntervalEstimates> {
    let reference = choose_reference(references, previous, |antenna| {
        fits.iter().any(|fit| fit.has_antenna(antenna))
    });
    let mut nonempty = false;
    let values = fits
        .into_iter()
        .enumerate()
        .map(|(channel, fit)| {
            let target = previous_reference(previous, channel, reference);
            let Some(fitted) = fit.solve(reference, target) else {
                return vec![LeakageEstimate::default(); antennas];
            };
            nonempty |= fitted.iter().any(Option::is_some);
            fitted
                .into_iter()
                .map(|pair| {
                    pair.filter(|_| references.is_empty() || reference >= 0)
                        .map_or_else(LeakageEstimate::default, |gain| LeakageEstimate {
                            gain,
                            valid: [true; 2],
                            ..LeakageEstimate::default()
                        })
                })
                .collect()
        })
        .collect();
    nonempty.then_some((reference, values))
}

fn iterative(
    samples: Vec<Vec<StandardSample>>,
    antennas: usize,
    request: &LeakageSolveRequest,
) -> Option<Vec<ChannelEstimates>> {
    let mut gains = vec![[Complex32::ZERO; 2]; antennas];
    let mut diagnostics = vec![LeakageEstimate::default(); antennas];
    let mut converged = false;
    let mut values = samples
        .into_iter()
        .rev()
        .map(|samples| {
            if let Some(result) =
                solve_channel(&samples, &mut gains, request.min_baselines, request.min_snr)
            {
                diagnostics.copy_from_slice(&result);
                converged = true;
                result
            } else {
                // A failed channel preserves the preceding channel's
                // diagnostics. Its flags exclude those values from readers.
                diagnostics
                    .iter()
                    .zip(&gains)
                    .map(|(diagnostics, &gain)| LeakageEstimate {
                        gain,
                        valid: [false; 2],
                        ..*diagnostics
                    })
                    .collect()
            }
        })
        .collect::<Vec<_>>();
    values.reverse();
    converged.then_some(values)
}

pub(super) fn reference_iterative_solutions(
    solutions: &mut [LeakageSolution],
    references: &mut [i32],
) {
    if references.is_empty() {
        return;
    }
    let second_receptor = solutions
        .iter()
        .filter(|solution| {
            solution
                .values
                .iter()
                .flatten()
                .any(|sample| sample.valid[1])
        })
        .map(|solution| solution.spectral_window)
        .collect::<std::collections::BTreeSet<_>>();
    let mut previous: Option<usize> = None;
    for index in 0..solutions.len() {
        let (earlier, remaining) = solutions.split_at_mut(index);
        let current = &mut remaining[0];
        let previous_solution = previous
            .map(|index| &earlier[index])
            .filter(|previous| previous.spectral_window == current.spectral_window);
        let receptors = 1 + usize::from(second_receptor.contains(&current.spectral_window));
        let reference = references
            .iter()
            .copied()
            .find(|&antenna| {
                usable_reference(&current.values, antenna, receptors)
                    && previous_solution.is_none_or(|previous| {
                        usable_reference(&previous.values, antenna, receptors)
                    })
            })
            .unwrap_or(-1);
        current.reference = reference;
        for (channel, values) in current.values.iter_mut().enumerate() {
            if reference >= 0 {
                let target = previous_reference(previous_solution, channel, reference);
                let offset = values[reference as usize].gain[0] - target;
                for estimate in values {
                    estimate.gain[0] -= offset;
                    estimate.gain[1] += offset.conj();
                }
            } else {
                for estimate in values {
                    estimate.valid = [false; 2];
                }
            }
        }
        if reference >= 0 {
            references[1] = reference;
            previous = Some(index);
        }
    }
}

fn choose_reference(
    references: &[i32],
    previous: Option<&LeakageSolution>,
    available: impl Fn(i32) -> bool,
) -> i32 {
    references
        .iter()
        .copied()
        .find(|&antenna| {
            available(antenna)
                && previous.is_none_or(|previous| usable_reference(&previous.values, antenna, 2))
        })
        .unwrap_or(-1)
}

fn usable_reference(values: &[ChannelEstimates], antenna: i32, receptors: usize) -> bool {
    (0..receptors).all(|receptor| {
        values.iter().any(|channel| {
            channel
                .get(antenna as usize)
                .is_some_and(|sample| sample.valid[receptor])
        })
    })
}

fn previous_reference(
    previous: Option<&LeakageSolution>,
    channel: usize,
    reference: i32,
) -> Complex32 {
    previous
        .and_then(|previous| previous.values.get(channel))
        .and_then(|channel| channel.get(reference as usize))
        .map_or(Complex32::ZERO, |sample| sample.gain[0])
}
