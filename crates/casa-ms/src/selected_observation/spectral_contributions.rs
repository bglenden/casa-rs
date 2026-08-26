// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompiledProblem, DirectionFrame, FrequencyFrame, SelectedObservationSample,
    SelectedSpectralContribution, SelectedSpectralContributions, SpectralFrameAnchor,
    SpectralSampling, TimeScale,
};

use casa_types::measures::{
    direction::{DirectionRef, MDirection},
    epoch::{EpochRef, MEpoch},
    frequency::FrequencyRef,
    position::MPosition,
};

use crate::{
    derived::engine::MsCalEngine,
    spectral_selection::{
        CubeInterpolation, convert_frequency_to_frame_with_frames,
        source_frequency_output_contributions,
    },
};

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
        geometry_engine: &MsCalEngine,
    ) -> Result<Self, BoundObservationSourceError> {
        let spectral_contributions =
            derive_spectral_contributions(problem, &sample, geometry_engine)?;
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
    geometry_engine: &MsCalEngine,
) -> Result<SelectedSpectralContributions, BoundObservationSourceError> {
    let evaluation_frequency_hz = evaluated_frequency_hz(problem, sample, geometry_engine)?;
    let contributions = match problem.science().spectral().sampling() {
        SpectralSampling::Identity | SpectralSampling::Nearest => interpolation_contributions(
            problem,
            sample,
            CubeInterpolation::Nearest,
            evaluation_frequency_hz,
        )?,
        SpectralSampling::Linear => interpolation_contributions(
            problem,
            sample,
            CubeInterpolation::Linear,
            evaluation_frequency_hz,
        )?,
        SpectralSampling::ChannelAverage { channels_per_bin } => channel_average_contribution(
            problem,
            sample,
            channels_per_bin,
            evaluation_frequency_hz,
        )?,
    };
    SelectedSpectralContributions::new(contributions)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)
}

fn interpolation_contributions(
    problem: &CompiledProblem,
    _sample: &SelectedObservationSample,
    interpolation: CubeInterpolation,
    evaluation_frequency_hz: f64,
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
    pack_contributions(
        source_frequency_output_contributions(
            &centres,
            &widths,
            interpolation,
            evaluation_frequency_hz,
        ),
        evaluation_frequency_hz,
    )
}

fn evaluated_frequency_hz(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    geometry_engine: &MsCalEngine,
) -> Result<f64, BoundObservationSourceError> {
    let spectral = problem.geometry().spectral();
    let source_ref = frequency_ref(sample.address.frequency_frame);
    let output_ref = frequency_ref(spectral.output_frame());
    if source_ref == output_ref {
        return Ok(sample.address.frequency_centre_hz);
    }
    let field_id = usize::try_from(sample.metadata.field_id)
        .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
    let source_frame = geometry_engine
        .spectral_frame_observatory(sample.coordinates.time.mjd_days() * 86_400.0, field_id)?;
    let SpectralFrameAnchor::Conversion {
        epoch,
        direction,
        observatory_position,
    } = spectral.anchor()
    else {
        return Err(BoundObservationSourceError::SpectralContributionMismatch);
    };
    let [x_metres, y_metres, z_metres] = observatory_position.metres();
    let output_frame = geometry_engine.spectral_frame_explicit(
        MEpoch::from_mjd(epoch.mjd_days(), epoch_ref(epoch.scale())),
        MPosition::new_itrf(x_metres, y_metres, z_metres),
        MDirection::from_angles(
            direction.longitude_rad(),
            direction.latitude_rad(),
            direction_ref(direction.frame()),
        ),
    );
    convert_frequency_to_frame_with_frames(
        source_ref,
        output_ref,
        sample.address.frequency_centre_hz,
        Some(&source_frame),
        Some(&output_frame),
    )
    .map_err(Into::into)
}

const fn direction_ref(frame: DirectionFrame) -> DirectionRef {
    match frame {
        DirectionFrame::Icrs => DirectionRef::ICRS,
        DirectionFrame::J2000 => DirectionRef::J2000,
        DirectionFrame::B1950 => DirectionRef::B1950,
        DirectionFrame::Galactic => DirectionRef::GALACTIC,
    }
}

const fn epoch_ref(scale: TimeScale) -> EpochRef {
    match scale {
        TimeScale::Utc => EpochRef::UTC,
        TimeScale::Tai => EpochRef::TAI,
        TimeScale::Tt => EpochRef::TT,
        TimeScale::Tdb => EpochRef::TDB,
    }
}

const fn frequency_ref(frame: FrequencyFrame) -> FrequencyRef {
    match frame {
        FrequencyFrame::Topocentric => FrequencyRef::TOPO,
        FrequencyFrame::Barycentric => FrequencyRef::BARY,
        FrequencyFrame::Lsrk => FrequencyRef::LSRK,
    }
}

fn channel_average_contribution(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    channels_per_bin: usize,
    evaluation_frequency_hz: f64,
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
    let contribution =
        SelectedSpectralContribution::new(output_channel, factor, evaluation_frequency_hz)
            .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    Ok([Some(contribution), None])
}

fn pack_contributions(
    contributions: Vec<crate::spectral_selection::CubeChannelContribution>,
    evaluation_frequency_hz: f64,
) -> Result<[Option<SelectedSpectralContribution>; 2], BoundObservationSourceError> {
    if contributions.len() > 2 {
        return Err(BoundObservationSourceError::SpectralContributionMismatch);
    }
    let mut packed = [None, None];
    for (slot, contribution) in contributions.into_iter().enumerate() {
        let output_channel = u32::try_from(contribution.source_channel)
            .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
        packed[slot] = Some(
            SelectedSpectralContribution::new(
                output_channel,
                contribution.factor,
                evaluation_frequency_hz,
            )
            .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?,
        );
    }
    Ok(packed)
}
