// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn uses_native_source_interpolation(axis_config: &CubeAxisConfig) -> bool {
    axis_config.start.is_none()
        && matches!(
            axis_config.width,
            Some(CubeAxisValue::VelocityMs { ms, .. }) if ms < 0.0
        )
        || axis_config.start.is_none()
            && matches!(
                axis_config.width,
                Some(CubeAxisValue::Doppler { value, .. }) if value < 0.0
            )
}

pub(super) fn spectral_frame_observatory(
    derived_engine: &MsCalEngine,
    time_mjd_sec: f64,
    fallback_field_id: usize,
    phase_center_direction: Option<MDirection>,
) -> MsResult<MeasFrame> {
    if let Some(direction) = phase_center_direction {
        derived_engine.spectral_frame_observatory_direction(time_mjd_sec, direction)
    } else {
        derived_engine.spectral_frame_observatory(time_mjd_sec, fallback_field_id)
    }
}

pub(super) fn effective_output_frequency_ref(
    source_freq_ref: FrequencyRef,
    axis_config: &CubeAxisConfig,
) -> MsResult<FrequencyRef> {
    if matches!(axis_config.specmode, CubeSpecMode::Cubedata) {
        return Ok(source_freq_ref);
    }
    let start_frame = cube_axis_explicit_frame(axis_config.start.as_ref());
    let width_frame = cube_axis_explicit_frame(axis_config.width.as_ref());
    match (start_frame, width_frame) {
        (Some(left), Some(right)) if left != right => Err(MsError::VersionError(format!(
            "cube axis start frame {left} and width frame {right} disagree"
        ))),
        (Some(frame), _) | (_, Some(frame)) => Ok(frame),
        (None, None) => Ok(axis_config.outframe),
    }
}

pub(super) fn cube_axis_explicit_frame(axis_value: Option<&CubeAxisValue>) -> Option<FrequencyRef> {
    match axis_value {
        Some(CubeAxisValue::FrequencyHz { frame, .. })
        | Some(CubeAxisValue::VelocityMs { frame, .. }) => *frame,
        Some(CubeAxisValue::Channel(_)) | Some(CubeAxisValue::Doppler { .. }) | None => None,
    }
}

pub(super) fn required_rest_frequency_hz(
    rest_frequency_hz: Option<f64>,
    value: &impl std::fmt::Debug,
) -> MsResult<f64> {
    rest_frequency_hz.ok_or_else(|| {
        MsError::VersionError(format!(
            "cube axis value {value:?} requires a rest frequency"
        ))
    })
}

/// Convert one scalar frequency between spectral frames using the MeasurementSet
/// observatory frame for the given row time and field.
pub fn convert_frequency_to_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    row_time_mjd_sec: f64,
    field_id: usize,
    derived_engine: &MsCalEngine,
) -> MsResult<f64> {
    if source_freq_ref == target_freq_ref {
        return Ok(frequency_hz);
    }
    let frame = derived_engine
        .spectral_frame_observatory(row_time_mjd_sec, field_id)
        .map_err(|error| {
            MsError::VersionError(format!(
                "build spectral frame for field {field_id}: {error}"
            ))
        })?;
    convert_frequency_to_frame_with_frame(
        source_freq_ref,
        target_freq_ref,
        frequency_hz,
        Some(&frame),
    )
}

/// Convert one scalar frequency between spectral frames using a caller-provided
/// measures frame.
///
/// If the source and target references are identical, the input frequency is
/// returned unchanged and the frame is ignored.
pub fn convert_frequency_to_frame_with_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    frame: Option<&MeasFrame>,
) -> MsResult<f64> {
    convert_frequency_to_frame_with_frames(
        source_freq_ref,
        target_freq_ref,
        frequency_hz,
        frame,
        frame,
    )
}

pub(crate) fn convert_frequency_to_frame_with_frames(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    frequency_hz: f64,
    source_frame: Option<&MeasFrame>,
    target_frame: Option<&MeasFrame>,
) -> MsResult<f64> {
    prepare_frequency_conversion(source_freq_ref, target_freq_ref, source_frame, target_frame)
        .map(|conversion| conversion.convert_hz(frequency_hz))
}

/// [`PreparedFrequencyConversion::new`], its error as the MS's.
fn prepare_frequency_conversion(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    source_frame: Option<&MeasFrame>,
    target_frame: Option<&MeasFrame>,
) -> MsResult<PreparedFrequencyConversion> {
    PreparedFrequencyConversion::new(source_freq_ref, target_freq_ref, source_frame, target_frame)
        .map_err(|error| {
            MsError::VersionError(format!(
                "prepare frequency conversion from {source_freq_ref} to {target_freq_ref}: {error}"
            ))
        })
}

/// Convert one scalar frequency into a doppler velocity in m/s using the given
/// rest frequency and velocity definition.
pub fn velocity_ms_from_frequency_hz(
    frequency_hz: f64,
    rest_frequency_hz: f64,
    doppler_ref: DopplerRef,
) -> MsResult<f64> {
    MDoppler::new(frequency_hz / rest_frequency_hz, DopplerRef::RATIO)
        .convert_to(doppler_ref, &MeasFrame::new())
        .map(|doppler| doppler.value() * SPEED_OF_LIGHT_M_S)
        .map_err(|error| {
            MsError::VersionError(format!(
                "convert frequency {frequency_hz} Hz to {doppler_ref} velocity: {error}"
            ))
        })
}

pub(super) fn convert_channel_widths_to_frame_with_frame(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frame: &MeasFrame,
) -> MsResult<Vec<f64>> {
    convert_channel_widths_to_frame_with_frames(
        source_freq_ref,
        target_freq_ref,
        source_channel_frequencies_hz,
        source_channel_widths_hz,
        frame,
        frame,
    )
}

pub(super) fn convert_channel_widths_to_frame_with_frames(
    source_freq_ref: FrequencyRef,
    target_freq_ref: FrequencyRef,
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    source_frame: &MeasFrame,
    target_frame: &MeasFrame,
) -> MsResult<Vec<f64>> {
    if source_channel_frequencies_hz.len() != source_channel_widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "frequency/width arrays must match for frame conversion, got {} and {}",
            source_channel_frequencies_hz.len(),
            source_channel_widths_hz.len()
        )));
    }
    if source_freq_ref == target_freq_ref {
        return Ok(source_channel_widths_hz
            .iter()
            .map(|width_hz| width_hz.abs())
            .collect());
    }
    source_channel_frequencies_hz
        .iter()
        .copied()
        .zip(source_channel_widths_hz.iter().copied())
        .map(|(frequency_hz, width_hz)| {
            let half_width_hz = width_hz.abs() / 2.0;
            let upper_hz = convert_frequency_to_frame_with_frames(
                source_freq_ref,
                target_freq_ref,
                frequency_hz + half_width_hz,
                Some(source_frame),
                Some(target_frame),
            )?;
            let lower_hz = convert_frequency_to_frame_with_frames(
                source_freq_ref,
                target_freq_ref,
                frequency_hz - half_width_hz,
                Some(source_frame),
                Some(target_frame),
            )?;
            Ok((upper_hz - lower_hz).abs())
        })
        .collect()
}
