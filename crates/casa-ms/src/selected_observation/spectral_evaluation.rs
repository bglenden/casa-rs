// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage-owned source and output-frame spectral evaluation.

use casa_imaging_model::{
    CompiledProblem, DirectionFrame, FrequencyFrame, SelectedObservationSample,
    SelectedSpectralEvaluation, SelectedSpectralInterval, SpectralFrameAnchor, TimeScale,
};

use casa_types::measures::{
    direction::{DirectionRef, MDirection},
    epoch::{EpochRef, MEpoch},
    frame::MeasFrame,
    frequency::FrequencyRef,
    position::MPosition,
};

use crate::{
    derived::engine::MsCalEngine, spectral_selection::convert_frequency_to_frame_with_frames,
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
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationTraversalSample {
    sample: SelectedObservationSample,
    spectral_evaluation: SelectedSpectralEvaluation,
}

impl SelectedObservationTraversalSample {
    pub(super) fn with_spectral_evaluation(
        sample: SelectedObservationSample,
        spectral_evaluation: SelectedSpectralEvaluation,
    ) -> Self {
        Self {
            sample,
            spectral_evaluation,
        }
    }

    /// Return the selected sample validated by the compiled problem.
    #[must_use]
    pub const fn selected(&self) -> &SelectedObservationSample {
        &self.sample
    }

    /// Return source-backed native/output-frame intervals, flag validity, and effective weight.
    #[must_use]
    pub const fn spectral_evaluation(&self) -> SelectedSpectralEvaluation {
        self.spectral_evaluation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpectralProjectionKey {
    measurement_set: casa_imaging_model::MeasurementSetIdentity,
    spectral_window_id: u32,
    channel_index: u32,
    frequency_centre_bits: u64,
    frequency_lower_bits: u64,
    frequency_upper_bits: u64,
    channel_width_bits: u64,
    frequency_frame: FrequencyFrame,
    field_id: i32,
    time_mjd_days_bits: u64,
    input_weight_bits: u32,
    validity: [bool; 3],
}

impl SpectralProjectionKey {
    fn from_sample(sample: &SelectedObservationSample) -> Self {
        Self {
            measurement_set: sample.address.measurement_set,
            spectral_window_id: sample.address.spectral_window_id,
            channel_index: sample.address.channel_index,
            frequency_centre_bits: sample.address.frequency_centre_hz.to_bits(),
            frequency_lower_bits: sample.address.frequency_lower_hz.to_bits(),
            frequency_upper_bits: sample.address.frequency_upper_hz.to_bits(),
            channel_width_bits: sample.address.channel_width_hz.to_bits(),
            frequency_frame: sample.address.frequency_frame,
            field_id: sample.metadata.field_id,
            time_mjd_days_bits: sample.coordinates.time.mjd_days().to_bits(),
            input_weight_bits: sample.input_weight.to_bits(),
            validity: [
                sample.channel_flag,
                sample.parallel_hand_group_flag,
                sample.row_flag,
            ],
        }
    }
}

/// Retains frame-conversion state across the canonical correlation-major stream.
///
/// Spectral assignment is independent of correlation and visibility value. A
/// row/channel's parallel-hand samples therefore reuse the exact same derived
/// evaluation, while a row, channel interval, field, time, frame, weight, flag, SPW, or source
/// change forces a fresh owner evaluation.
pub(super) struct SpectralEvaluationProjector {
    last_source: Option<casa_imaging_model::MeasurementSetIdentity>,
    output_frame: Option<MeasFrame>,
    source_frame: Option<(i32, u64, MeasFrame)>,
    last_projection: Option<(SpectralProjectionKey, SelectedSpectralEvaluation)>,
}

impl SpectralEvaluationProjector {
    pub(super) const fn new() -> Self {
        Self {
            last_source: None,
            output_frame: None,
            source_frame: None,
            last_projection: None,
        }
    }

    pub(super) fn project(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSample,
        geometry_engine: &MsCalEngine,
    ) -> Result<SelectedObservationTraversalSample, BoundObservationSourceError> {
        let key = SpectralProjectionKey::from_sample(&sample);
        if let Some((cached_key, evaluation)) = self.last_projection
            && cached_key == key
        {
            return Ok(
                SelectedObservationTraversalSample::with_spectral_evaluation(sample, evaluation),
            );
        }
        if self.last_source != Some(sample.address.measurement_set) {
            self.last_source = Some(sample.address.measurement_set);
            self.output_frame = None;
            self.source_frame = None;
        }
        let evaluation = derive_spectral_evaluation_cached(
            problem,
            &sample,
            geometry_engine,
            &mut self.source_frame,
            &mut self.output_frame,
        )?;
        self.last_projection = Some((key, evaluation));
        Ok(SelectedObservationTraversalSample::with_spectral_evaluation(sample, evaluation))
    }
}

fn derive_spectral_evaluation_cached(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    geometry_engine: &MsCalEngine,
    source_frame: &mut Option<(i32, u64, MeasFrame)>,
    output_frame: &mut Option<MeasFrame>,
) -> Result<SelectedSpectralEvaluation, BoundObservationSourceError> {
    let native_boundaries = if sample.address.channel_width_hz >= 0.0 {
        [
            sample.address.frequency_lower_hz,
            sample.address.frequency_upper_hz,
        ]
    } else {
        [
            sample.address.frequency_upper_hz,
            sample.address.frequency_lower_hz,
        ]
    };
    let native = SelectedSpectralInterval::new(
        sample.address.frequency_centre_hz,
        native_boundaries[0],
        native_boundaries[1],
    )
    .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let output_centre_hz = evaluated_frequency_hz_cached(
        problem,
        sample,
        native.centre_hz(),
        geometry_engine,
        source_frame,
        output_frame,
    )?;
    let output_first_hz = evaluated_frequency_hz_cached(
        problem,
        sample,
        native_boundaries[0],
        geometry_engine,
        source_frame,
        output_frame,
    )?;
    let output_second_hz = evaluated_frequency_hz_cached(
        problem,
        sample,
        native_boundaries[1],
        geometry_engine,
        source_frame,
        output_frame,
    )?;
    let output = SelectedSpectralInterval::new(output_centre_hz, output_first_hz, output_second_hz)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let valid = !sample.channel_flag
        && !sample.parallel_hand_group_flag
        && !sample.row_flag
        && sample.input_weight.is_finite()
        && sample.input_weight > 0.0;
    SelectedSpectralEvaluation::new(
        native,
        output,
        if valid {
            f64::from(sample.input_weight)
        } else {
            0.0
        },
        valid,
    )
    .ok_or(BoundObservationSourceError::SpectralContributionMismatch)
}

fn evaluated_frequency_hz_cached(
    problem: &CompiledProblem,
    sample: &SelectedObservationSample,
    frequency_hz: f64,
    geometry_engine: &MsCalEngine,
    source_frame_cache: &mut Option<(i32, u64, MeasFrame)>,
    output_frame_cache: &mut Option<MeasFrame>,
) -> Result<f64, BoundObservationSourceError> {
    let spectral = problem.geometry().spectral();
    let source_ref = frequency_ref(sample.address.frequency_frame);
    let output_ref = frequency_ref(spectral.output_frame());
    if source_ref == output_ref {
        return Ok(frequency_hz);
    }
    let field_id = usize::try_from(sample.metadata.field_id)
        .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
    let time_mjd_seconds = sample.coordinates.time.mjd_days() * 86_400.0;
    let time_bits = time_mjd_seconds.to_bits();
    let source_frame = match source_frame_cache {
        Some((cached_field, cached_time, frame))
            if *cached_field == sample.metadata.field_id && *cached_time == time_bits =>
        {
            frame
        }
        slot => {
            *slot = Some((
                sample.metadata.field_id,
                time_bits,
                geometry_engine.spectral_frame_observatory(time_mjd_seconds, field_id)?,
            ));
            &slot.as_ref().expect("source frame was inserted").2
        }
    };
    let SpectralFrameAnchor::Conversion {
        epoch,
        direction,
        observatory_position,
    } = spectral.anchor()
    else {
        return Err(BoundObservationSourceError::SpectralContributionMismatch);
    };
    let output_frame = output_frame_cache.get_or_insert_with(|| {
        let [x_metres, y_metres, z_metres] = observatory_position.metres();
        geometry_engine.spectral_frame_explicit(
            MEpoch::from_mjd(epoch.mjd_days(), epoch_ref(epoch.scale())),
            MPosition::new_itrf(x_metres, y_metres, z_metres),
            MDirection::from_angles(
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction_ref(direction.frame()),
            ),
        )
    });
    convert_frequency_to_frame_with_frames(
        source_ref,
        output_ref,
        frequency_hz,
        Some(source_frame),
        Some(output_frame),
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
