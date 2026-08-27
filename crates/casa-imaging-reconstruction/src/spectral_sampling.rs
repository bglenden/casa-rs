// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned compilation of paired sparse spectral stencils.

use casa_imaging_model::{
    CompiledProblem, SelectedObservationSample, SelectedSpectralContribution,
    SelectedSpectralContributions, SelectedSpectralEvaluation, SpectralCovariance,
    SpectralEdgePolicy, SpectralKernel,
};
use smallvec::SmallVec;
use thiserror::Error;

/// Why a selected sample did or did not acquire output spectral support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralStencilValidity {
    /// The valid source sample has one or more output terms.
    Mapped,
    /// Source flags or effective weight rejected the sample.
    Flagged,
    /// The valid source sample is outside the law's edge support.
    Unmapped,
}

/// The sole compiled sparse stencil consumed by prediction and adjoint paths.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralStencilReceipt {
    contributions: SelectedSpectralContributions,
    evaluation: SelectedSpectralEvaluation,
    validity: SpectralStencilValidity,
    covariance: SpectralCovariance,
}

impl SpectralStencilReceipt {
    /// Return sparse coefficients in output-channel order.
    #[must_use]
    pub const fn contributions(&self) -> &SelectedSpectralContributions {
        &self.contributions
    }

    /// Return the source-backed trace used to compile the stencil.
    #[must_use]
    pub const fn evaluation(&self) -> SelectedSpectralEvaluation {
        self.evaluation
    }

    /// Return mapped, flagged, or edge-unmapped validity.
    #[must_use]
    pub const fn validity(&self) -> SpectralStencilValidity {
        self.validity
    }

    /// Return the covariance propagation declaration.
    #[must_use]
    pub const fn covariance(&self) -> SpectralCovariance {
        self.covariance
    }
}

/// A spectral law or geometry could not produce a coherent sparse stencil.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SpectralStencilError {
    /// Output WCS centres or boundaries were invalid.
    #[error("compiled output spectral geometry is invalid")]
    InvalidOutputGeometry,
    /// Identity sampling could not locate the selected source channel.
    #[error("identity sampling could not locate the selected source channel")]
    IdentitySourceMismatch,
    /// Integration exceeded its planner-declared sparse term bound.
    #[error("spectral integration exceeded its planner term bound")]
    PlannerTermBound,
    /// Generated terms were not a valid sparse contribution set.
    #[error("compiled spectral coefficients are invalid")]
    InvalidCoefficients,
}

/// Compile one paired sparse stencil from a source trace and logical sampling law.
pub fn compile_spectral_stencil(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    evaluation: SelectedSpectralEvaluation,
) -> Result<SpectralStencilReceipt, SpectralStencilError> {
    let law = problem.science().spectral().sampling();
    if !evaluation.is_valid() {
        return Ok(receipt(
            SelectedSpectralContributions::empty(),
            evaluation,
            SpectralStencilValidity::Flagged,
            law.covariance(),
        ));
    }
    let spectral = problem.geometry().spectral();
    let centres = (0..spectral.output_channels())
        .map(|channel| spectral.channel_centre_hz(channel))
        .collect::<Option<Vec<_>>>()
        .ok_or(SpectralStencilError::InvalidOutputGeometry)?;
    let boundaries = (0..=spectral.output_channels())
        .map(|boundary| spectral.channel_boundary_hz(boundary))
        .collect::<Option<Vec<_>>>()
        .ok_or(SpectralStencilError::InvalidOutputGeometry)?;
    validate_axis(&centres, &boundaries)?;
    let frequency_hz = evaluation.output_frame().centre_hz();
    let terms = match law.kernel() {
        SpectralKernel::Identity => identity_terms(problem, sample, frequency_hz)?,
        SpectralKernel::Nearest => nearest_terms(&centres, &boundaries, frequency_hz),
        SpectralKernel::Linear => linear_terms(&centres, frequency_hz),
        SpectralKernel::Cubic => cubic_terms(&centres, frequency_hz),
        SpectralKernel::ChannelIntegration { maximum_terms } => integration_terms(
            &boundaries,
            evaluation.output_frame().boundaries_hz(),
            law.edge_policy(),
            maximum_terms,
            frequency_hz,
        )?,
    };
    let contributions = SelectedSpectralContributions::new(terms)
        .ok_or(SpectralStencilError::InvalidCoefficients)?;
    let validity = if contributions.is_empty() {
        SpectralStencilValidity::Unmapped
    } else {
        SpectralStencilValidity::Mapped
    };
    Ok(receipt(
        contributions,
        evaluation,
        validity,
        law.covariance(),
    ))
}

fn receipt(
    contributions: SelectedSpectralContributions,
    evaluation: SelectedSpectralEvaluation,
    validity: SpectralStencilValidity,
    covariance: SpectralCovariance,
) -> SpectralStencilReceipt {
    SpectralStencilReceipt {
        contributions,
        evaluation,
        validity,
        covariance,
    }
}

fn validate_axis(centres: &[f64], boundaries: &[f64]) -> Result<(), SpectralStencilError> {
    if centres.is_empty()
        || boundaries.len() != centres.len() + 1
        || centres
            .iter()
            .chain(boundaries)
            .any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err(SpectralStencilError::InvalidOutputGeometry);
    }
    let direction = (centres[centres.len() - 1] - centres[0]).signum();
    if centres.len() > 1
        && (direction == 0.0
            || centres
                .windows(2)
                .any(|pair| (pair[1] - pair[0]).signum() != direction))
    {
        return Err(SpectralStencilError::InvalidOutputGeometry);
    }
    Ok(())
}

fn identity_terms(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    let source = problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .find(|source| source.identity() == sample.address.measurement_set)
        .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
    let selection = source
        .selection()
        .spectral_windows()
        .iter()
        .find(|selection| selection.spectral_window_id() == sample.address.spectral_window_id)
        .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
    let ordinal = selection
        .channel_indices()
        .iter()
        .position(|channel| *channel == sample.address.channel_index)
        .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
    if ordinal >= problem.geometry().spectral().output_channels() {
        return Ok(SmallVec::new());
    }
    one_term(ordinal, 1.0, frequency_hz)
}

fn nearest_terms(
    centres: &[f64],
    boundaries: &[f64],
    frequency_hz: f64,
) -> SmallVec<[SelectedSpectralContribution; 4]> {
    let Some((index, _)) = centres.iter().enumerate().min_by(|(_, left), (_, right)| {
        (*left - frequency_hz)
            .abs()
            .total_cmp(&(*right - frequency_hz).abs())
    }) else {
        return SmallVec::new();
    };
    let low = boundaries[index].min(boundaries[index + 1]);
    let high = boundaries[index].max(boundaries[index + 1]);
    if !(low..=high).contains(&frequency_hz) {
        return SmallVec::new();
    }
    one_term(index, 1.0, frequency_hz).unwrap_or_default()
}

fn linear_terms(centres: &[f64], frequency_hz: f64) -> SmallVec<[SelectedSpectralContribution; 4]> {
    interpolation_interval(centres, frequency_hz)
        .and_then(|index| {
            let first = centres[index];
            let second = centres[index + 1];
            let upper = (frequency_hz - first) / (second - first);
            sparse_terms([(index, 1.0 - upper), (index + 1, upper)], frequency_hz)
        })
        .unwrap_or_default()
}

fn cubic_terms(centres: &[f64], frequency_hz: f64) -> SmallVec<[SelectedSpectralContribution; 4]> {
    if centres.len() < 4 {
        return SmallVec::new();
    }
    let Some(interval) = interpolation_interval(centres, frequency_hz) else {
        return SmallVec::new();
    };
    let bracket = interval + 1;
    let start = if bracket > 1 && bracket < centres.len() - 1 {
        bracket - 2
    } else if bracket <= 1 {
        0
    } else {
        centres.len() - 4
    };
    let mut terms = SmallVec::<[(usize, f64); 4]>::new();
    for index in start..start + 4 {
        let mut coefficient = 1.0;
        for other in start..start + 4 {
            if other != index {
                coefficient *= (frequency_hz - centres[other]) / (centres[index] - centres[other]);
            }
        }
        terms.push((index, coefficient));
    }
    sparse_terms(terms, frequency_hz).unwrap_or_default()
}

fn integration_terms(
    output_boundaries_hz: &[f64],
    source_boundaries_hz: [f64; 2],
    edge_policy: SpectralEdgePolicy,
    maximum_terms: usize,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    let source_low = source_boundaries_hz[0].min(source_boundaries_hz[1]);
    let source_high = source_boundaries_hz[0].max(source_boundaries_hz[1]);
    let axis_end = output_boundaries_hz[output_boundaries_hz.len() - 1];
    let axis_low = output_boundaries_hz[0].min(axis_end);
    let axis_high = output_boundaries_hz[0].max(axis_end);
    if edge_policy == SpectralEdgePolicy::CompleteSupport
        && (source_low < axis_low || source_high > axis_high)
    {
        return Ok(SmallVec::new());
    }
    let mut terms = SmallVec::<[SelectedSpectralContribution; 4]>::new();
    for (output_channel, pair) in output_boundaries_hz.windows(2).enumerate() {
        let output_low = pair[0].min(pair[1]);
        let output_high = pair[0].max(pair[1]);
        let overlap = source_high.min(output_high) - source_low.max(output_low);
        if overlap > 0.0 {
            if terms.len() == maximum_terms {
                return Err(SpectralStencilError::PlannerTermBound);
            }
            terms.push(
                SelectedSpectralContribution::new(
                    u32::try_from(output_channel)
                        .map_err(|_| SpectralStencilError::InvalidCoefficients)?,
                    overlap / (output_high - output_low),
                    frequency_hz,
                )
                .ok_or(SpectralStencilError::InvalidCoefficients)?,
            );
        }
    }
    Ok(terms)
}

fn interpolation_interval(centres: &[f64], frequency_hz: f64) -> Option<usize> {
    centres.windows(2).position(|pair| {
        pair[0].min(pair[1]) <= frequency_hz && frequency_hz <= pair[0].max(pair[1])
    })
}

fn one_term(
    output_channel: usize,
    factor: f64,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    sparse_terms([(output_channel, factor)], frequency_hz)
        .ok_or(SpectralStencilError::InvalidCoefficients)
}

fn sparse_terms(
    terms: impl IntoIterator<Item = (usize, f64)>,
    frequency_hz: f64,
) -> Option<SmallVec<[SelectedSpectralContribution; 4]>> {
    terms
        .into_iter()
        .filter(|(_, factor)| *factor != 0.0)
        .map(|(output_channel, factor)| {
            SelectedSpectralContribution::new(
                u32::try_from(output_channel).ok()?,
                factor,
                frequency_hz,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t35_one_channel_identity_matches_constant_basis_and_is_exactly_paired() {
        let stencil = one_term(0, 1.0, 1.4e9).expect("identity stencil");
        assert_eq!(
            stencil
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 1.0)]
        );

        let model = 2.5_f64;
        let visibility = -3.0_f64;
        let weight = 4.0_f64;
        let channel_prediction = stencil
            .iter()
            .map(|term| term.factor() * model)
            .sum::<f64>();
        let channel_dirty = stencil
            .iter()
            .map(|term| term.factor() * weight * visibility)
            .sum::<f64>();
        let channel_psf = stencil
            .iter()
            .map(|term| term.factor() * weight * term.factor())
            .sum::<f64>();
        let channel_sum_weight = weight;
        assert_eq!(channel_prediction, model);
        assert_eq!(channel_dirty, weight * visibility);
        assert_eq!(channel_psf, weight);
        assert_eq!(channel_sum_weight, weight);

        let lhs = channel_prediction * visibility;
        let adjoint_visibility = stencil
            .iter()
            .map(|term| term.factor() * visibility)
            .sum::<f64>();
        let rhs = model * adjoint_visibility;
        assert_eq!(lhs, rhs, "prediction and adjoint must use the same stencil");
    }

    #[test]
    fn t35_dense_oracle_matches_sparse_linear_and_signed_cubic_stencils() {
        let centres = [10.0, 20.0, 30.0, 40.0];
        let linear = linear_terms(&centres, 25.0);
        assert_eq!(
            linear
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(1, 0.5), (2, 0.5)]
        );

        let cubic = cubic_terms(&centres, 25.0);
        let dense = cubic.iter().fold([0.0; 4], |mut dense, term| {
            dense[term.output_channel() as usize] = term.factor();
            dense
        });
        assert_eq!(dense, [-0.0625, 0.5625, 0.5625, -0.0625]);
        for degree in 0_i32..=3 {
            let reconstructed = dense
                .iter()
                .zip(centres)
                .map(|(coefficient, centre)| coefficient * centre.powi(degree))
                .sum::<f64>();
            assert!((reconstructed - 25.0_f64.powi(degree)).abs() < 1.0e-9);
        }
    }

    #[test]
    fn t36_descending_partial_overlap_and_planner_bounds_are_explicit() {
        let descending = [45.0, 35.0, 25.0, 15.0, 5.0];
        let terms = integration_terms(
            &descending,
            [42.0, 28.0],
            SpectralEdgePolicy::PartialOverlap,
            2,
            35.0,
        )
        .unwrap();
        assert_eq!(
            terms
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 0.7), (1, 0.7)]
        );
        assert_eq!(
            integration_terms(
                &descending,
                [42.0, 18.0],
                SpectralEdgePolicy::PartialOverlap,
                2,
                30.0,
            ),
            Err(SpectralStencilError::PlannerTermBound)
        );
    }

    #[test]
    fn t36_edge_validity_and_source_channel_order_are_deterministic() {
        assert!(linear_terms(&[30.0, 20.0, 10.0], 35.0).is_empty());
        assert_eq!(
            linear_terms(&[30.0, 20.0, 10.0], 25.0)
                .iter()
                .map(|term| term.output_channel())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(cubic_terms(&[10.0, 20.0, 30.0], 15.0).is_empty());
    }
}
