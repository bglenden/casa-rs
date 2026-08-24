// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompiledProblem, SelectedObservationSample, SelectedSpectralContribution,
    SelectedSpectralContributions, SpectralSampling,
};

use crate::spectral_selection::{CubeInterpolation, source_frequency_output_contributions};

use super::BoundObservationSourceError;

/// One owner-issued selected sample and its evaluated output spectral support.
///
/// This envelope is constructed only by retained [`BoundSelectedObservation`](super::BoundSelectedObservation)
/// traversal after the selected sample has passed compiled-problem validation.
/// It is not persisted and does not participate in selected-sample content
/// identity.
///
/// ```compile_fail
/// use casa_ms::SelectedObservationTraversalSample;
///
/// let _forged = SelectedObservationTraversalSample {};
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationTraversalSample {
    sample: SelectedObservationSample,
    spectral_contributions: SelectedSpectralContributions,
}

impl SelectedObservationTraversalSample {
    pub(super) fn from_owner(
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
    ) -> Result<Self, BoundObservationSourceError> {
        let spectral_contributions = derive_spectral_contributions(problem, &sample)?;
        Ok(Self {
            sample,
            spectral_contributions,
        })
    }

    /// Return the selected sample validated by the compiled problem.
    #[must_use]
    pub const fn selected(&self) -> &SelectedObservationSample {
        &self.sample
    }

    /// Return storage-owner-evaluated output spectral contributions.
    #[must_use]
    pub const fn spectral_contributions(&self) -> SelectedSpectralContributions {
        self.spectral_contributions
    }
}

fn derive_spectral_contributions(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
) -> Result<SelectedSpectralContributions, BoundObservationSourceError> {
    let spectral = problem.geometry().spectral();
    if spectral.source_frame() != spectral.output_frame() {
        return Err(
            BoundObservationSourceError::UnsupportedSpectralContributionFrame {
                source_frame: spectral.source_frame(),
                output_frame: spectral.output_frame(),
            },
        );
    }

    let contributions = match problem.science().spectral().sampling() {
        SpectralSampling::Identity | SpectralSampling::Nearest => {
            interpolation_contributions(problem, sample, CubeInterpolation::Nearest)?
        }
        SpectralSampling::Linear => {
            interpolation_contributions(problem, sample, CubeInterpolation::Linear)?
        }
        SpectralSampling::ChannelAverage { channels_per_bin } => {
            channel_average_contribution(problem, sample, channels_per_bin)?
        }
    };
    SelectedSpectralContributions::new(contributions)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)
}

fn interpolation_contributions(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    interpolation: CubeInterpolation,
) -> Result<[Option<SelectedSpectralContribution>; 2], BoundObservationSourceError> {
    let spectral = problem.geometry().spectral();
    let centres = (0..spectral.output_channels())
        .map(|channel| {
            spectral
                .channel_centre_hz(channel)
                .ok_or(BoundObservationSourceError::SpectralContributionMismatch)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let widths = (0..spectral.output_channels())
        .map(|channel| {
            let first = spectral
                .channel_boundary_hz(channel)
                .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
            let second = spectral
                .channel_boundary_hz(channel + 1)
                .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
            Ok::<_, BoundObservationSourceError>((second - first).abs())
        })
        .collect::<Result<Vec<_>, _>>()?;
    pack_contributions(source_frequency_output_contributions(
        &centres,
        &widths,
        interpolation,
        sample.address.frequency_centre_hz,
    ))
}

fn channel_average_contribution(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    channels_per_bin: usize,
) -> Result<[Option<SelectedSpectralContribution>; 2], BoundObservationSourceError> {
    let source = problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .find(|source| source.identity() == sample.address.measurement_set)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let selection = source
        .selection()
        .spectral_windows()
        .iter()
        .find(|selection| selection.spectral_window_id() == sample.address.spectral_window_id)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let ordinal = selection
        .channel_indices()
        .iter()
        .position(|channel| *channel == sample.address.channel_index)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let output_channel = ordinal / channels_per_bin;
    if output_channel >= problem.geometry().spectral().output_channels() {
        return Ok([None, None]);
    }
    let bin_start = output_channel
        .checked_mul(channels_per_bin)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let bin_len = channels_per_bin.min(selection.channel_indices().len() - bin_start);
    let factor = 1.0 / bin_len as f32;
    let output_channel = u32::try_from(output_channel)
        .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
    let contribution = SelectedSpectralContribution::new(output_channel, factor)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    Ok([Some(contribution), None])
}

fn pack_contributions(
    contributions: Vec<crate::spectral_selection::CubeChannelContribution>,
) -> Result<[Option<SelectedSpectralContribution>; 2], BoundObservationSourceError> {
    if contributions.len() > 2 {
        return Err(BoundObservationSourceError::SpectralContributionMismatch);
    }
    let mut packed = [None, None];
    for (slot, contribution) in contributions.into_iter().enumerate() {
        let output_channel = u32::try_from(contribution.source_channel)
            .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
        packed[slot] = Some(
            SelectedSpectralContribution::new(output_channel, contribution.factor)
                .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?,
        );
    }
    Ok(packed)
}
