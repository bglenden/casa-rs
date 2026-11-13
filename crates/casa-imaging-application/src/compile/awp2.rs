// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bind the native AWPLPG beam inputs from the selected observation.

use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_imaging_runtime::{Demand, admit};
use casa_types::measures::{
    direction::MDirection,
    frequency::{FrequencyRef, MFrequencyConverter},
};

use super::{PrepareError, Surveyed, domains::PreparedImageDomain, native_aw};
use crate::{Awp2Deployment, Gridder, ImagingRequest, RunContext};

pub(super) fn plan(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    domain: &PreparedImageDomain,
    context: &RunContext,
) -> Result<Option<Awp2Deployment>, PrepareError> {
    let Gridder::Awp2 {
        evla_surface,
        wprojplanes,
        ..
    } = &request.gridder
    else {
        return Ok(None);
    };
    let [window] = surveyed.survey.spectral_windows.as_slice() else {
        return Err(PrepareError::Awp2Selection);
    };
    if surveyed.stores.len() != 1
        || surveyed.msv2.is_none()
        || surveyed
            .survey
            .fields
            .iter()
            .any(|field| surveyed.engine.field_moves(*field as usize))
        || window.frequency_reference != FrequencyRef::TOPO
        || !matches!(domain.telescope.trim(), "VLA" | "EVLA")
    {
        return Err(PrepareError::Awp2Selection);
    }
    let coordinate_index = domain
        .coordinates
        .find_coordinate(CoordinateType::Direction)
        .expect("an imaging domain has direction axes");
    let CoordinateModel::Direction(direction) = domain.coordinates.coordinate(coordinate_index)
    else {
        unreachable!("direction coordinate type");
    };
    // AWPLPG evaluates the spectral reference conversion at the image's
    // epoch and direction at pixel (0, 0).
    let [longitude, latitude] = direction
        .pixel_to_world([0.0; 2])
        .map_err(|_| PrepareError::Awp2Selection)?;
    let direction = MDirection::from_angles(longitude, latitude, direction.direction_ref());
    let frame = surveyed
        .engine
        .spectral_frame_observatory_direction(surveyed.survey.image_time_mjd_seconds, direction)?;
    let spectral = &surveyed.spectral.image;
    let mut converter =
        MFrequencyConverter::new(spectral.frequency_reference, window.frequency_reference);
    let [lo, hi] =
        [-0.5, 0.5].map(|pixel| spectral.reference_frequency_hz + pixel * spectral.increment_hz);
    let band = [
        converter.convert_hz(lo, &frame)?.hz(),
        converter.convert_hz(hi, &frame)?.hz(),
    ];
    let frequencies_hz =
        beam_frequencies(&window.frequencies_hz, window.channel_widths_hz[0], band)?;

    let count = wprojplanes.map_or(1, std::num::NonZeroUsize::get);
    let maximum_w = surveyed.survey.w.maximum_abs_m * frequencies_hz.last().unwrap()
        / super::specification::SPEED_OF_LIGHT_M_PER_S;
    let step = maximum_w / (count.saturating_sub(1).max(1) as f64).powi(2);
    let w_planes: Box<[f64]> = (0..count).map(|i| (i as f64).powi(2) * step).collect();
    let channels = super::selection::selected_channels(
        request,
        &surveyed.survey.spw_channels,
        window.spw_id,
        &window.frequencies_hz,
    )?;
    let row_frequency_hz = channels
        .iter()
        .map(|&channel| window.frequencies_hz[channel])
        .sum::<f64>()
        / channels.len() as f64;
    let path = evla_surface
        .as_deref()
        .ok_or(PrepareError::Awp2SurfacePath)?;
    // The bounded parser holds raw text, radial samples and their shared
    // allocation during conversion. Retain only the physical data afterward.
    let mut charge = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "AWP2 dish surface",
            memory: 16 << 20,
        },
    )?;
    let surface = native_aw::read_dish_surface(path)?;
    charge.retain(
        (std::mem::size_of_val(surface.samples())
            + std::mem::size_of_val(frequencies_hz.as_ref())
            + std::mem::size_of_val(w_planes.as_ref())
            + 2 * size_of::<usize>()) as u64,
    );
    Ok(Some(Awp2Deployment {
        single_field: surveyed.survey.fields.len() == 1 && count == 1,
        legacy_vla: domain.telescope.trim() == "VLA",
        frequencies_hz,
        w_planes,
        row_frequency_hz,
        surface,
        _charge: charge,
    }))
}

/// SimplePBConvFunc::findUsefulChannels works in the native SPW frame,
/// sampling the whole SPW first and then filtering by the image band.
fn beam_frequencies(
    window_hz: &[f64],
    first_width_hz: f64,
    [first_edge, last_edge]: [f64; 2],
) -> Result<Box<[f64]>, PrepareError> {
    let [low, high] = window_hz
        .iter()
        .fold([f64::INFINITY, f64::NEG_INFINITY], |[low, high], &hz| {
            [low.min(hz), high.max(hz)]
        });
    if !(0.9e9..=2e9 + 1e-6).contains(&low) || !(0.9e9..=2e9 + 1e-6).contains(&high) {
        return Err(PrepareError::Awp2Selection);
    }
    let tolerance = (high / 100.0).max(first_width_hz.abs());
    let count = (((high - low).round() / tolerance) as usize).max(1);
    let mut frequencies: Vec<_> = (0..count)
        .map(|k| low + k as f64 * tolerance)
        .filter(|hz| (first_edge.min(last_edge)..=first_edge.max(last_edge)).contains(hz))
        .collect();
    if frequencies.is_empty() {
        frequencies.push((first_edge + last_edge) / 2.0);
    }
    Ok(frequencies.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn awp2_samples_the_whole_spw_before_cutting_to_the_image_band() {
        let first = 999_999_999.999_999_9;
        let channels = [first, 1.5e9, 2e9];
        // CASA's captured inputs omit the high endpoint even though the
        // wide MFS image reaches it.
        let frequencies = beam_frequencies(&channels, 5e8, [0.75e9, 2.25e9]).unwrap();
        assert_eq!(&*frequencies, &[first, 1.5e9]);
        // Inclusive image edges, regardless of axis order.
        let frequencies = beam_frequencies(&channels, 5e8, [1.5e9, 1.25e9]).unwrap();
        assert_eq!(&*frequencies, &[1.5e9]);
        let frequencies = beam_frequencies(&channels, 5e8, [1.5e9 + 1.0, 1.7e9]).unwrap();
        assert_eq!(&*frequencies, &[1.6e9 + 0.5]);
    }
}
