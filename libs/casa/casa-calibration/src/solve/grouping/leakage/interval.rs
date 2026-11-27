// SPDX-License-Identifier: LGPL-3.0-or-later
//! Solve the averaged coherencies and carry the leakage reference across intervals.

use std::collections::{BTreeMap, BTreeSet};

use num_complex::Complex32;

use crate::solve::GainNormalization;

use super::channel_samples::ChannelSamples;
use super::cross_hand::{CrossHandFit, LegacyCrossHandAveraging, Measurement};
use super::fit::LeakageFit;
use super::joint::{SourceFit, SourceFitMode};
use super::parallactic::{BaselineSegment, ParallacticAverages};
use super::parallactic_fit::solve_sample;
use super::standard::{LeakageAlgebra, StandardSample, eligible_antennas, solve_channel};
use super::{
    CoherencyMean, GainSolveError, GainSolveMode, LeakageEstimate, LeakageSolution,
    LeakageSolveKind, LeakageSolveRequest, ParallacticAngleState,
};

pub(super) enum IntervalFits {
    LeastSquares(Vec<LeakageFit>, GainSolveMode),
    CrossHand(Vec<CrossHandFit>, Measurement, GainSolveMode),
    Parallactic(ParallacticAverages),
    Iterative {
        algebra: LeakageAlgebra,
        samples: ChannelSamples,
        source: Option<(SourceFitMode, SourceFit)>,
        mode: GainSolveMode,
    },
}

type ChannelEstimates = Vec<LeakageEstimate>;
type IntervalEstimates = (i32, Vec<ChannelEstimates>, Option<Complex32>);

#[derive(Default)]
pub(super) struct LeakageFitStages {
    pub(super) constrained: BTreeSet<i32>,
    pub(super) passed: Vec<[bool; 2]>,
}

/// Convert and normalize the referenced parameters before publication, as
/// the Calibrater's generic Jones post-solve pass does.
pub(super) fn finish_tool_parameters(
    solutions: &mut [LeakageSolution],
    mode: GainSolveMode,
    normalization: Option<GainNormalization>,
) {
    if mode != GainSolveMode::AmplitudePhase {
        for estimate in solutions
            .iter_mut()
            .flat_map(|slot| slot.values.iter_mut().flatten())
        {
            for (gain, valid) in estimate.gain.iter_mut().zip(estimate.valid) {
                let amplitude = gain.norm();
                *gain = if !valid || amplitude == 0.0 {
                    Complex32::ONE
                } else if mode == GainSolveMode::Phase {
                    *gain / amplitude
                } else {
                    amplitude.into()
                };
            }
        }
    }
    let Some(normalization) = normalization else {
        return;
    };
    let mut power = BTreeMap::<i32, (f64, usize, Vec<f32>)>::new();
    for slot in solutions.iter() {
        let (sum, count, samples) = power.entry(slot.spectral_window).or_default();
        for estimate in slot.values.iter().flatten() {
            for (gain, valid) in estimate.gain.iter().zip(estimate.valid) {
                if valid {
                    let value = gain.norm_sqr();
                    match normalization {
                        GainNormalization::Mean => *sum += f64::from(value),
                        GainNormalization::Median => samples.push(value),
                    }
                    *count += 1;
                }
            }
        }
    }
    let factors: BTreeMap<_, _> = power
        .into_iter()
        .filter_map(|(spw, (sum, count, mut samples))| {
            if count <= 1 {
                return None;
            }
            let factor = match normalization {
                GainNormalization::Mean => (sum / count as f64).sqrt() as f32,
                GainNormalization::Median => {
                    super::super::median_with_even_mean(&mut samples).sqrt()
                }
            };
            (factor > 0.0 && factor.is_finite()).then_some((spw, factor))
        })
        .collect();
    for solution in solutions {
        if let Some(&factor) = factors.get(&solution.spectral_window) {
            for estimate in solution.values.iter_mut().flatten() {
                for gain in &mut estimate.gain {
                    *gain /= factor;
                }
            }
        }
    }
}

impl IntervalFits {
    pub(super) fn new(
        channels: usize,
        antennas: usize,
        kind: LeakageSolveKind,
        circular: bool,
        mode: GainSolveMode,
        legacy: bool,
    ) -> Self {
        if kind.parallactic() {
            Self::Parallactic(ParallacticAverages::new(
                channels,
                circular,
                kind.gain_phase(),
                mode,
                legacy,
            ))
        } else if kind.cross_hand() {
            let measurement = match (kind, circular) {
                (LeakageSolveKind::PosAng, true) => Measurement::CircularAngle,
                (LeakageSolveKind::PosAng, false) => Measurement::LinearAngle,
                (LeakageSolveKind::XMueller, _) => Measurement::MuellerPhase,
                _ => Measurement::Phase,
            };
            Self::CrossHand(
                (0..channels).map(|_| CrossHandFit::default()).collect(),
                measurement,
                if kind == LeakageSolveKind::XMueller {
                    GainSolveMode::AmplitudePhase
                } else {
                    mode
                },
            )
        } else if kind.iterative() {
            Self::Iterative {
                algebra: kind.algebra(),
                samples: ChannelSamples::new(channels),
                source: kind.source().map(|mode| (mode, SourceFit::default())),
                mode,
            }
        } else {
            Self::LeastSquares(
                (0..channels).map(|_| LeakageFit::new(antennas)).collect(),
                mode,
            )
        }
    }

    pub(super) fn accumulate(
        &mut self,
        pair: [i32; 2],
        bins: &mut [CoherencyMean],
        antennas: usize,
        segment: BaselineSegment,
        legacy_cross_hand: bool,
    ) -> Result<(), GainSolveError> {
        let pair = pair.map(|antenna| antenna as usize);
        if pair.iter().any(|&antenna| antenna >= antennas) {
            return Ok(());
        }
        if let Self::Parallactic(average) = self {
            return average.add(pair.map(|antenna| antenna as i32), segment, bins);
        }
        let require_all = matches!(self, Self::LeastSquares(..));
        let legacy_phase_normalization = (legacy_cross_hand
            && matches!(self, Self::CrossHand(_, _, GainSolveMode::Phase)))
        .then(|| super::cross_hand::legacy_phase_normalization(bins));
        for (channel, mean) in bins.iter_mut().enumerate() {
            let unflagged = if pair[0] == pair[1] {
                0
            } else {
                mean.unflagged
            };
            if let Some((mut data, model, mut weights)) = std::mem::take(mean).finish(require_all) {
                if let Self::LeastSquares(_, mode)
                | Self::CrossHand(_, _, mode)
                | Self::Iterative { mode, .. } = self
                    && *mode != GainSolveMode::AmplitudePhase
                {
                    for (corr, (value, weight)) in data.iter_mut().zip(&mut weights).enumerate() {
                        let (normalized, scaled) = solve_sample(*mode, *value, *weight);
                        *value = normalized;
                        *weight = legacy_phase_normalization
                            .map_or(scaled, |normalization| *weight * normalization[corr]);
                    }
                }
                match self {
                    Self::LeastSquares(fits, _) => {
                        fits[channel].add_baseline(pair, data, model, weights)
                    }
                    Self::CrossHand(fits, measurement, _) => {
                        fits[channel].add(*measurement, unflagged, data, model, weights)
                    }
                    Self::Parallactic(_) => unreachable!("time-segment accumulation"),
                    Self::Iterative {
                        algebra,
                        samples,
                        source,
                        ..
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn finish(
        self,
        antennas: usize,
        references: &[i32],
        previous: Option<&LeakageSolution>,
        request: &LeakageSolveRequest,
        location: (i32, i32),
        angles: Option<&ParallacticAngleState>,
        legacy_times: Option<&LegacyCrossHandAveraging>,
        stages: Option<&mut LeakageFitStages>,
    ) -> Result<Option<IntervalEstimates>, GainSolveError> {
        match self {
            Self::LeastSquares(fits, _) => Ok(least_squares(
                fits,
                antennas,
                references,
                previous,
                stages.is_some(),
            )),
            Self::CrossHand(fits, measurement, _) => {
                Ok(cross_hand(fits, measurement, antennas, location))
            }
            Self::Parallactic(average) => {
                let angles = angles.expect("parallactic angle geometry");
                let (phases, source) = average.finish(angles, legacy_times)?;
                SourceFitMode::Polarization.report(source, location);
                let gain_phase = request.kind.gain_phase();
                let values = phases
                    .into_iter()
                    .map(|phase| {
                        let estimate = LeakageEstimate {
                            gain: [
                                phase.unwrap_or(Complex32::ONE),
                                if gain_phase {
                                    Complex32::ONE
                                } else {
                                    Complex32::ZERO
                                },
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
                ..
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
                Ok(
                    iterative(samples, antennas, request, multiplier, algebra, stages)?
                        .map(|values| (-1, values, None)),
                )
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
    let position_angle = matches!(
        measurement,
        Measurement::CircularAngle | Measurement::LinearAngle
    );
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
    keep_flagged: bool,
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
    // Direct generic gather retains an attempted self-directed solve even
    // when its individual fits could only write flagged parameters.
    (nonempty || keep_flagged).then_some((reference, values, None))
}

fn iterative(
    mut samples: ChannelSamples,
    antennas: usize,
    request: &LeakageSolveRequest,
    multiplier: Option<Complex32>,
    algebra: LeakageAlgebra,
    mut stages: Option<&mut LeakageFitStages>,
) -> Result<Option<Vec<ChannelEstimates>>, GainSolveError> {
    let mut gains = vec![[Complex32::ZERO; 2]; antennas];
    let mut diagnostics = vec![LeakageEstimate::default(); antennas];
    let mut converged = false;
    let mut values = Vec::with_capacity(samples.len());
    if let Some(stages) = &mut stages {
        stages.passed.resize(antennas, [false; 2]);
    }
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
        let eligible = eligible_antennas(&channel_samples, antennas, request.min_baselines);
        if let Some(stages) = &mut stages {
            stages.constrained.extend(
                eligible
                    .iter()
                    .enumerate()
                    .filter_map(|(id, &valid)| valid.then_some(id as i32)),
            );
        }
        values.push(
            match solve_channel(
                &channel_samples,
                &mut gains,
                &eligible,
                request.min_snr,
                algebra,
            ) {
                Ok(result) => {
                    if let Some(stages) = &mut stages {
                        for (passed, estimate) in stages.passed.iter_mut().zip(&result) {
                            for (pass, valid) in passed.iter_mut().zip(estimate.valid) {
                                *pass |= valid;
                            }
                        }
                    }
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
