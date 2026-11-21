// SPDX-License-Identifier: LGPL-3.0-or-later
//! Solve the averaged coherencies and carry the leakage reference across intervals.

use num_complex::Complex32;

use super::channel_samples::ChannelSamples;
use super::cross_hand::{CrossHandFit, Measurement};
use super::fit::LeakageFit;
use super::joint::{SourceFit, SourceFitMode};
use super::parallactic::{BaselineSegment, ParallacticAverages};
use super::standard::{LeakageAlgebra, StandardSample, solve_channel};
use super::{
    CoherencyMean, GainSolveError, LeakageEstimate, LeakageSolution, LeakageSolveKind,
    LeakageSolveRequest, ParallacticAngleState,
};

pub(super) enum IntervalFits {
    LeastSquares(Vec<LeakageFit>),
    CrossHand(Vec<CrossHandFit>, Measurement),
    Parallactic(ParallacticAverages),
    Iterative {
        algebra: LeakageAlgebra,
        samples: ChannelSamples,
        source: Option<(SourceFitMode, SourceFit)>,
    },
}

type ChannelEstimates = Vec<LeakageEstimate>;
type IntervalEstimates = (i32, Vec<ChannelEstimates>, Option<Complex32>);

impl IntervalFits {
    pub(super) fn new(
        channels: usize,
        antennas: usize,
        kind: LeakageSolveKind,
        circular: bool,
    ) -> Self {
        if kind.parallactic() {
            Self::Parallactic(ParallacticAverages::new(channels, circular, kind.gain_phase()))
        } else if kind.cross_hand() {
            let measurement = match (kind, circular) {
                (LeakageSolveKind::PosAng, true) => Measurement::CircularAngle,
                (LeakageSolveKind::PosAng, false) => Measurement::LinearAngle,
                _ => Measurement::Phase,
            };
            Self::CrossHand(
                (0..channels).map(|_| CrossHandFit::default()).collect(),
                measurement,
            )
        } else if kind.iterative() {
            Self::Iterative {
                algebra: kind.algebra(),
                samples: ChannelSamples::new(channels),
                source: kind.source().map(|mode| (mode, SourceFit::default())),
            }
        } else {
            Self::LeastSquares((0..channels).map(|_| LeakageFit::new(antennas)).collect())
        }
    }

    pub(super) fn accumulate(
        &mut self,
        pair: [i32; 2],
        bins: &mut [CoherencyMean],
        antennas: usize,
        segment: BaselineSegment,
    ) -> Result<(), GainSolveError> {
        let pair = pair.map(|antenna| antenna as usize);
        if pair.iter().any(|&antenna| antenna >= antennas) {
            return Ok(());
        }
        if let Self::Parallactic(average) = self {
            return average.add(pair.map(|antenna| antenna as i32), segment, bins);
        }
        let require_all = matches!(self, Self::LeastSquares(_));
        for (channel, mean) in bins.iter_mut().enumerate() {
            if let Some((data, model, weights)) = std::mem::take(mean).finish(require_all) {
                match self {
                    Self::LeastSquares(fits) => {
                        fits[channel].add_baseline(pair, data, model, weights)
                    }
                    Self::CrossHand(fits, measurement) => {
                        fits[channel].add(*measurement, data, model, weights)
                    }
                    Self::Parallactic(_) => unreachable!("time-segment accumulation"),
                    Self::Iterative {
                        algebra,
                        samples,
                        source,
                    } => {
                        let mut weights = weights;
                        if matches!(algebra, LeakageAlgebra::Linearized) {
                            weights[0] = 0.0;
                            weights[3] = 0.0;
                        }
                        let sample = StandardSample {
                            pair,
                            data,
                            model,
                            weights,
                        };
                        if let Some((_, fit)) = source {
                            fit.observe(&sample);
                        }
                        samples
                            .push(channel, sample)
                            .map_err(GainSolveError::PolarizationScratch)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        antennas: usize,
        references: &[i32],
        previous: Option<&LeakageSolution>,
        request: &LeakageSolveRequest,
        location: (i32, i32),
        angles: Option<&ParallacticAngleState>,
    ) -> Result<Option<IntervalEstimates>, GainSolveError> {
        match self {
            Self::LeastSquares(fits) => Ok(least_squares(fits, antennas, references, previous)),
            Self::CrossHand(fits, measurement) => {
                Ok(cross_hand(fits, measurement, antennas, location))
            }
            Self::Parallactic(average) => {
                let angles = angles.expect("parallactic angle geometry");
                let (phases, source) = average.finish(angles)?;
                SourceFitMode::Polarization.report(source, location);
                let gain_phase = request.kind.gain_phase();
                let values = phases
                    .into_iter()
                    .map(|phase| {
                        let estimate = LeakageEstimate {
                            gain: [
                                phase.unwrap_or(Complex32::ONE),
                                if gain_phase { Complex32::ONE } else { Complex32::ZERO },
                            ],
                            valid: [phase.is_some(), gain_phase && phase.is_some()],
                            ..LeakageEstimate::default()
                        };
                        vec![estimate; antennas]
                    })
                    .collect();
                Ok(Some((-1, values, Some(source))))
            }
            Self::Iterative {
                algebra,
                samples,
                source,
            } => {
                if samples.is_empty() || source.as_ref().is_some_and(|(_, fit)| !fit.has_model()) {
                    return Ok(None);
                }
                let multiplier = source
                    .map(|(mode, fit)| {
                        let multiplier = fit
                            .finish(mode)
                            .ok_or(GainSolveError::SingularSourcePolarization)?;
                        mode.report(multiplier, location);
                        Ok::<_, GainSolveError>(multiplier)
                    })
                    .transpose()?;
                Ok(iterative(samples, antennas, request, multiplier, algebra)?
                    .map(|values| (-1, values, None)))
            }
        }
    }
}

fn cross_hand(
    fits: Vec<CrossHandFit>,
    measurement: Measurement,
    antennas: usize,
    (field, spw): (i32, i32),
) -> Option<IntervalEstimates> {
    if !fits.iter().any(CrossHandFit::saw_samples) {
        return None;
    }
    let mut sum = Complex32::ZERO;
    let mut channels = 0;
    let position_angle = !matches!(measurement, Measurement::Phase);
    let values = fits
        .into_iter()
        .map(|fit| {
            let phase = fit.phase();
            if let Some(phase) = phase {
                sum += phase;
                channels += 1;
            }
            let parameter = if position_angle {
                Complex32::new(phase.map_or(0.0, |value| 0.5 * value.arg()), 0.0)
            } else {
                phase.unwrap_or(Complex32::ONE)
            };
            let estimate = LeakageEstimate {
                gain: [parameter, Complex32::ZERO],
                valid: [phase.is_some(), false],
                ..LeakageEstimate::default()
            };
            vec![estimate; antennas]
        })
        .collect();
    if channels > 0 && position_angle {
        tracing::info!(
            field,
            spw,
            position_angle_degrees = 0.5 * sum.arg().to_degrees(),
            "Mean position angle offset solution"
        );
    } else if channels > 0 {
        tracing::info!(
            field,
            spw,
            phase_degrees = sum.arg().to_degrees(),
            "Mean cross-hand phase solution"
        );
    }
    Some((-1, values, None))
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
    nonempty.then_some((reference, values, None))
}

fn iterative(
    mut samples: ChannelSamples,
    antennas: usize,
    request: &LeakageSolveRequest,
    multiplier: Option<Complex32>,
    algebra: LeakageAlgebra,
) -> Result<Option<Vec<ChannelEstimates>>, GainSolveError> {
    let mut gains = vec![[Complex32::ZERO; 2]; antennas];
    let mut diagnostics = vec![LeakageEstimate::default(); antennas];
    let mut converged = false;
    let mut values = Vec::with_capacity(samples.len());
    for channel in (0..samples.len()).rev() {
        let mut channel_samples = samples
            .take(channel)
            .map_err(GainSolveError::PolarizationScratch)?;
        if let Some(multiplier) = multiplier {
            for sample in &mut channel_samples {
                sample.model[1] *= multiplier;
                sample.model[2] *= multiplier.conj();
            }
        }
        values.push(
            match solve_channel(
                &channel_samples,
                &mut gains,
                request.min_baselines,
                request.min_snr,
                algebra,
            ) {
                Ok(result) => {
                    diagnostics.copy_from_slice(&result);
                    converged = true;
                    result
                }
                Err(valid) => {
                    // CASA keeps the prior diagnostics and the current
                    // parameter mask even when this channel cannot converge.
                    diagnostics
                        .iter()
                        .zip(gains.iter().zip(valid))
                        .map(|(diagnostics, (&gain, valid))| LeakageEstimate {
                            gain,
                            valid,
                            ..*diagnostics
                        })
                        .collect()
                }
            },
        );
    }
    values.reverse();
    Ok(converged.then_some(values))
}

pub(super) fn reference_iterative_solutions(
    solutions: &mut [LeakageSolution],
    references: &mut [i32],
) {
    solutions.sort_by(|a, b| {
        a.spectral_window
            .cmp(&b.spectral_window)
            .then_with(|| a.time.total_cmp(&b.time))
    });
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
        let current = &solutions[index];
        let previous_solution = previous
            .map(|index| &solutions[index])
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
        let offsets = (reference >= 0).then(|| {
            current
                .values
                .iter()
                .enumerate()
                .map(|(channel, values)| {
                    values[reference as usize].gain[0]
                        - previous_reference(previous_solution, channel, reference)
                })
                .collect::<Vec<_>>()
        });
        let current = &mut solutions[index];
        current.reference = reference;
        for (channel, values) in current.values.iter_mut().enumerate() {
            if let Some(offsets) = &offsets {
                let offset = offsets[channel];
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
