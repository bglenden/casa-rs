// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage-owned source and output-frame spectral evaluation.

use casa_imaging_model::{
    CompiledProblem, DirectionFrame, FrequencyFrame, SelectedObservationRunChannel,
    SelectedObservationRunCorrelation, SelectedObservationRunRow, SelectedObservationSampleView,
    SelectedSpectralEvaluation, SelectedSpectralInterval, SpectralFrameAnchor, TimeScale,
};

use casa_types::measures::{
    direction::{DirectionRef, MDirection},
    epoch::{EpochRef, MEpoch},
    frame::MeasFrame,
    frequency::FrequencyRef,
    position::MPosition,
};

use crate::{derived::engine::MsCalEngine, spectral_selection::PreparedFrequencyFrameConversion};

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
pub struct SelectedObservationTraversalSample<'a> {
    sample: SelectedObservationSampleView<'a>,
    spectral_evaluation: SelectedSpectralEvaluation,
}

impl<'a> SelectedObservationTraversalSample<'a> {
    pub(super) fn with_spectral_evaluation(
        sample: SelectedObservationSampleView<'a>,
        spectral_evaluation: SelectedSpectralEvaluation,
    ) -> Self {
        Self {
            sample,
            spectral_evaluation,
        }
    }

    /// Return the selected sample validated by the compiled problem.
    #[must_use]
    pub const fn selected(&self) -> SelectedObservationSampleView<'a> {
        self.sample
    }

    /// Return source-backed native/output-frame intervals, flag validity, and effective weight.
    #[must_use]
    pub const fn spectral_evaluation(&self) -> SelectedSpectralEvaluation {
        self.spectral_evaluation
    }
}

/// One borrowed row/channel run validated in canonical correlation order.
///
/// The run borrows one row and reusable correlation/evaluation scratch while
/// keeping its channel inline. It is released before the source block can be
/// refilled or the scratch can be reused.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationTraversalRun<'a> {
    row: &'a SelectedObservationRunRow,
    channel: SelectedObservationRunChannel,
    correlations: &'a [SelectedObservationRunCorrelation],
    evaluations: &'a [SelectedSpectralEvaluation],
}

impl<'a> SelectedObservationTraversalRun<'a> {
    pub(super) fn new(
        row: &'a SelectedObservationRunRow,
        channel: SelectedObservationRunChannel,
        correlations: &'a [SelectedObservationRunCorrelation],
        evaluations: &'a [SelectedSpectralEvaluation],
    ) -> Self {
        debug_assert_eq!(correlations.len(), evaluations.len());
        Self {
            row,
            channel,
            correlations,
            evaluations,
        }
    }

    /// Iterate through validated correlation members in canonical order.
    pub fn samples(&self) -> impl Iterator<Item = SelectedObservationTraversalSample<'_>> {
        self.correlations
            .iter()
            .zip(self.evaluations.iter())
            .map(|(correlation, evaluation)| {
                SelectedObservationTraversalSample::with_spectral_evaluation(
                    SelectedObservationSampleView::from_run(self.row, &self.channel, correlation),
                    *evaluation,
                )
            })
    }

    /// Return the number of correlations in this row/channel run.
    #[must_use]
    pub fn len(&self) -> usize {
        self.correlations.len()
    }

    /// Return whether the run contains no correlations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.correlations.is_empty()
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
}

impl SpectralProjectionKey {
    fn from_sample(sample: SelectedObservationSampleView<'_>) -> Self {
        let address = sample.address();
        let metadata = sample.metadata();
        let coordinates = sample.coordinates();
        Self {
            measurement_set: address.measurement_set,
            spectral_window_id: address.spectral_window_id,
            channel_index: address.channel_index,
            frequency_centre_bits: address.frequency_centre_hz.to_bits(),
            frequency_lower_bits: address.frequency_lower_hz.to_bits(),
            frequency_upper_bits: address.frequency_upper_hz.to_bits(),
            channel_width_bits: address.channel_width_hz.to_bits(),
            frequency_frame: address.frequency_frame,
            field_id: metadata.field_id,
            time_mjd_days_bits: coordinates.time.mjd_days().to_bits(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpectralTransformKey {
    measurement_set: casa_imaging_model::MeasurementSetIdentity,
    field_id: i32,
    time_mjd_seconds_bits: u64,
    source_ref: FrequencyRef,
    output_ref: FrequencyRef,
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
    last_transform: Option<(SpectralTransformKey, PreparedFrequencyFrameConversion)>,
    last_projection: Option<(
        SpectralProjectionKey,
        SelectedSpectralInterval,
        SelectedSpectralInterval,
    )>,
}

impl SpectralEvaluationProjector {
    pub(super) const fn new() -> Self {
        Self {
            last_source: None,
            output_frame: None,
            source_frame: None,
            last_transform: None,
            last_projection: None,
        }
    }

    pub(super) fn project<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSampleView<'a>,
        geometry_engine: &MsCalEngine,
    ) -> Result<SelectedObservationTraversalSample<'a>, BoundObservationSourceError> {
        let key = SpectralProjectionKey::from_sample(sample);
        let address = sample.address();
        if self.last_source != Some(address.measurement_set) {
            self.last_source = Some(address.measurement_set);
            self.output_frame = None;
            self.source_frame = None;
            self.last_transform = None;
            self.last_projection = None;
        }
        let (native, output) = if let Some((cached_key, native, output)) = self.last_projection
            && cached_key == key
        {
            (native, output)
        } else {
            let intervals = derive_spectral_intervals_cached(
                problem,
                sample,
                geometry_engine,
                &mut self.source_frame,
                &mut self.output_frame,
                &mut self.last_transform,
            )?;
            self.last_projection = Some((key, intervals.0, intervals.1));
            intervals
        };
        let input_weight = sample.input_weight();
        let valid = !sample.channel_flag()
            && !sample.parallel_hand_group_flag()
            && !sample.row_flag()
            && input_weight.is_finite()
            && input_weight > 0.0;
        let evaluation = SelectedSpectralEvaluation::new(
            native,
            output,
            if valid { f64::from(input_weight) } else { 0.0 },
            valid,
        )
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
        Ok(SelectedObservationTraversalSample::with_spectral_evaluation(sample, evaluation))
    }
}

fn derive_spectral_intervals_cached(
    problem: &CompiledProblem,
    sample: SelectedObservationSampleView<'_>,
    geometry_engine: &MsCalEngine,
    source_frame: &mut Option<(i32, u64, MeasFrame)>,
    output_frame: &mut Option<MeasFrame>,
    last_transform: &mut Option<(SpectralTransformKey, PreparedFrequencyFrameConversion)>,
) -> Result<(SelectedSpectralInterval, SelectedSpectralInterval), BoundObservationSourceError> {
    let address = sample.address();
    let native_boundaries = if address.channel_width_hz >= 0.0 {
        [address.frequency_lower_hz, address.frequency_upper_hz]
    } else {
        [address.frequency_upper_hz, address.frequency_lower_hz]
    };
    let native = SelectedSpectralInterval::new(
        address.frequency_centre_hz,
        native_boundaries[0],
        native_boundaries[1],
    )
    .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let conversion = prepared_frequency_conversion_cached(
        problem,
        sample,
        geometry_engine,
        source_frame,
        output_frame,
        last_transform,
    )?;
    let output_centre_hz = conversion.convert_hz(native.centre_hz());
    let output_first_hz = conversion.convert_hz(native_boundaries[0]);
    let output_second_hz = conversion.convert_hz(native_boundaries[1]);
    let output = SelectedSpectralInterval::new(output_centre_hz, output_first_hz, output_second_hz)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    Ok((native, output))
}

fn prepared_frequency_conversion_cached(
    problem: &CompiledProblem,
    sample: SelectedObservationSampleView<'_>,
    geometry_engine: &MsCalEngine,
    source_frame_cache: &mut Option<(i32, u64, MeasFrame)>,
    output_frame_cache: &mut Option<MeasFrame>,
    last_transform: &mut Option<(SpectralTransformKey, PreparedFrequencyFrameConversion)>,
) -> Result<PreparedFrequencyFrameConversion, BoundObservationSourceError> {
    let spectral = problem.geometry().spectral();
    let address = sample.address();
    let metadata = sample.metadata();
    let coordinates = sample.coordinates();
    let source_ref = frequency_ref(address.frequency_frame);
    let output_ref = frequency_ref(spectral.output_frame());
    let time_mjd_seconds = coordinates.time.mjd_days() * 86_400.0;
    let time_bits = time_mjd_seconds.to_bits();
    let key = SpectralTransformKey {
        measurement_set: address.measurement_set,
        field_id: metadata.field_id,
        time_mjd_seconds_bits: time_bits,
        source_ref,
        output_ref,
    };
    if let Some((cached_key, conversion)) = *last_transform
        && cached_key == key
    {
        return Ok(conversion);
    }
    if source_ref == output_ref {
        let conversion = PreparedFrequencyFrameConversion::new(source_ref, output_ref, None, None)
            .map_err(BoundObservationSourceError::from)?;
        *last_transform = Some((key, conversion));
        return Ok(conversion);
    }
    let field_id = usize::try_from(metadata.field_id)
        .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
    let source_frame = match source_frame_cache {
        Some((cached_field, cached_time, frame))
            if *cached_field == metadata.field_id && *cached_time == time_bits =>
        {
            frame
        }
        slot => {
            *slot = Some((
                metadata.field_id,
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
    let conversion = PreparedFrequencyFrameConversion::new(
        source_ref,
        output_ref,
        Some(source_frame),
        Some(output_frame),
    )
    .map_err(BoundObservationSourceError::from)?;
    *last_transform = Some((key, conversion));
    Ok(conversion)
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
