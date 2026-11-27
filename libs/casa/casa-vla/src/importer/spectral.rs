// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn same_spectral_descriptor(
    left: &SpectralDescriptor,
    right: &SpectralDescriptor,
    tolerance_hz: f64,
) -> bool {
    let center_frequency_matches = if left.doppler_tracking {
        let left_ref = spectral_reference_frequency(left);
        let right_ref = spectral_reference_frequency(right);
        (left_ref.hz() - right_ref.hz()).abs() <= tolerance_hz
            && left_ref.refer() == right_ref.refer()
    } else {
        (left.observed_frequency_hz - right.observed_frequency_hz).abs() <= tolerance_hz
    };

    same_spectral_window_shape(left, left.channel_width_hz * left.num_chan as f64, right)
        && left.doppler_tracking == right.doppler_tracking
        && center_frequency_matches
        && (!left.doppler_tracking
            || (left.rest_frame == right.rest_frame
                && left.doppler_definition == right.doppler_definition
                && (left.rest_frequency_hz - right.rest_frequency_hz).abs() <= tolerance_hz
                && (left.doppler_velocity_mps - right.doppler_velocity_mps).abs() <= 1.0e-6))
}

pub(super) fn same_spectral_window_shape(
    left: &SpectralDescriptor,
    stored_bandwidth: f64,
    right: &SpectralDescriptor,
) -> bool {
    // CASA tests bandwidth separately from the reference-frequency tolerance.
    let requested_bandwidth = right.channel_width_hz * right.num_chan as f64;
    left.num_chan == right.num_chan
        && left.if_conv_chain == right.if_conv_chain
        && (stored_bandwidth - requested_bandwidth).abs() <= requested_bandwidth.abs() / 4.0
}

pub(super) fn spectral_window_matches(
    existing: &SpectralWindowEntry,
    descriptor: &SpectralDescriptor,
    time_seconds: f64,
    source_direction: [f64; 2],
    direction_epoch: DirectionEpoch,
    tolerance_hz: f64,
    measures: Arc<dyn MeasuresProvider>,
) -> Result<bool, VlaError> {
    if !same_spectral_window_shape(
        &existing.descriptor,
        existing.stored_bandwidth_hz,
        descriptor,
    ) {
        return Ok(false);
    }

    let existing_reference = spectral_reference_frequency(&existing.descriptor);
    let current_reference = spectral_reference_frequency(descriptor);
    if existing_reference.refer() == current_reference.refer() {
        return Ok((existing_reference.hz() - current_reference.hz()).abs() <= tolerance_hz);
    }

    let existing_frame = spectral_conversion_frame(
        existing.time_seconds,
        existing.source_direction,
        existing.direction_epoch,
        measures.clone(),
    )?;
    let current_frame =
        spectral_conversion_frame(time_seconds, source_direction, direction_epoch, measures)?;
    let existing_topo = existing_reference
        .convert_to(FrequencyRef::TOPO, &existing_frame)
        .map_err(|error| {
            VlaError::import(format!("convert existing spectral window to TOPO: {error}"))
        })?
        .hz();
    let current_topo = current_reference
        .convert_to(FrequencyRef::TOPO, &current_frame)
        .map_err(|error| {
            VlaError::import(format!("convert current spectral window to TOPO: {error}"))
        })?
        .hz();
    Ok((existing_topo - current_topo).abs() <= tolerance_hz)
}

pub(super) fn correlation_types_for_group(
    record: &LogicalRecord,
    cda_ids: &[CdaId],
    n_antennas: usize,
) -> Result<Vec<i32>, VlaError> {
    let _ = n_antennas;
    let mut corr_types = Vec::new();
    for &cda_id in cda_ids {
        for stokes in record
            .stokes_products(cda_id, 0, 0)
            .map_err(|error| VlaError::import(format!("derive correlation types: {error}")))?
        {
            corr_types.push(stokes_code(stokes));
        }
    }
    if corr_types.iter().all(|code| matches!(code, 5..=8)) && corr_types.len() > 1 {
        let mut standardized = Vec::with_capacity(corr_types.len());
        for code in [5, 6, 7, 8] {
            if corr_types.contains(&code) {
                standardized.push(code);
            }
        }
        return Ok(standardized);
    }
    Ok(corr_types)
}

pub(super) fn spectral_window_name(
    descriptor: &SpectralDescriptor,
    ref_frequency_hz: f64,
    frequency_ref: FrequencyRef,
) -> String {
    let (chan_width_value, chan_width_unit) = if descriptor.channel_width_hz < 1.0e6 {
        (descriptor.channel_width_hz / 1.0e3, "kHz")
    } else {
        (descriptor.channel_width_hz / 1.0e6, "MHz")
    };
    let (ref_freq_value, ref_freq_unit) = if ref_frequency_hz / 1.0e9 < 1.0 {
        (ref_frequency_hz / 1.0e6, "MHz")
    } else {
        (ref_frequency_hz / 1.0e9, "GHz")
    };
    format!(
        "{}*{} {} channels @ {} {} ({})",
        descriptor.num_chan,
        format_sigfigs(chan_width_value, 3),
        chan_width_unit,
        format_sigfigs(ref_freq_value, 3),
        ref_freq_unit,
        frequency_ref.as_str()
    )
}

pub(super) fn spectral_reference_frequency(descriptor: &SpectralDescriptor) -> MFrequency {
    let refer = frequency_reference_for_descriptor(descriptor);
    if descriptor.doppler_tracking {
        let doppler = doppler_measure_from_descriptor(descriptor);
        MFrequency::new(
            doppler.shift_frequency_hz(descriptor.rest_frequency_hz),
            refer,
        )
    } else {
        MFrequency::new(descriptor.observed_frequency_hz, refer)
    }
}

pub(super) fn doppler_measure_from_descriptor(descriptor: &SpectralDescriptor) -> MDoppler {
    let value = descriptor.doppler_velocity_mps / SPEED_OF_LIGHT_M_PER_S;
    MDoppler::new(
        value,
        doppler_reference_for_definition(descriptor.doppler_definition),
    )
}

pub(super) fn frequency_reference_for_descriptor(descriptor: &SpectralDescriptor) -> FrequencyRef {
    if descriptor.doppler_tracking {
        frequency_reference_for_frame(descriptor.rest_frame)
    } else {
        FrequencyRef::TOPO
    }
}

pub(super) fn frequency_reference_for_frame(frame: FrequencyFrame) -> FrequencyRef {
    match frame {
        FrequencyFrame::Topocentric => FrequencyRef::TOPO,
        FrequencyFrame::Geocentric => FrequencyRef::GEO,
        FrequencyFrame::Barycentric => FrequencyRef::BARY,
        FrequencyFrame::Lsrk => FrequencyRef::LSRK,
    }
}

pub(super) fn doppler_reference_for_definition(definition: DopplerDefinition) -> DopplerRef {
    match definition {
        DopplerDefinition::Optical => DopplerRef::Z,
        DopplerDefinition::Radio | DopplerDefinition::Unknown => DopplerRef::RADIO,
    }
}

pub(super) fn spectral_conversion_frame(
    time_seconds: f64,
    source_direction: [f64; 2],
    direction_epoch: DirectionEpoch,
    measures: Arc<dyn MeasuresProvider>,
) -> Result<MeasFrame, VlaError> {
    let observatory = MPosition::from_observatory_name(DEFAULT_TELESCOPE_NAME, measures.as_ref())
        .map_err(|error| VlaError::import(format!("read VLA observatory position: {error}")))?
        .ok_or_else(|| {
            VlaError::import("resolve observatory position for VLA from measures catalog")
        })?;
    let direction = MDirection::from_angles(
        source_direction[0],
        source_direction[1],
        direction_reference_for_epoch(direction_epoch),
    );
    Ok(MeasFrame::new()
        .with_measures(measures)
        .with_epoch(MEpoch::from_mjd(time_seconds / 86_400.0, EpochRef::TAI))
        .with_position(observatory)
        .with_direction(direction))
}

pub(super) fn direction_reference_for_epoch(epoch: DirectionEpoch) -> DirectionRef {
    match epoch {
        DirectionEpoch::J2000 => DirectionRef::J2000,
        DirectionEpoch::B1950Vla => DirectionRef::B1950,
        DirectionEpoch::Apparent => DirectionRef::APP,
        DirectionEpoch::Unknown(_) => DirectionRef::J2000,
    }
}

pub(super) fn convert_topocentric_frequency(
    hz: f64,
    target: FrequencyRef,
    frame: &MeasFrame,
) -> Result<f64, VlaError> {
    Ok(MFrequency::new(hz, FrequencyRef::TOPO)
        .convert_to(target, frame)
        .map_err(|error| VlaError::import(format!("convert TOPO frequency to {target}: {error}")))?
        .hz())
}

pub(super) fn format_sigfigs(value: f64, sigfigs: usize) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let digits_before = value.abs().log10().floor() as i32 + 1;
    let decimals = (sigfigs as i32 - digits_before).max(0) as usize;
    let mut formatted = format!("{value:.decimals$}");
    if formatted.contains('.') {
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
    }
    formatted
}
