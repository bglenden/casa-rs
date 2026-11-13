// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bind the native AWPLPG beam inputs from the selected observation.

use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::CubeInterpolation;
use casa_types::measures::{
    direction::MDirection,
    frequency::{FrequencyRef, MFrequencyConverter},
};

use super::{
    PrepareError, Surveyed, domains::PreparedImageDomain, native_aw,
    selection::SourceSpectralWindow,
};
use crate::{Awp2Deployment, Deconvolver, Gridder, ImagingRequest, RunContext, SpecMode};

pub(super) fn plan(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    domain: &PreparedImageDomain,
    context: &RunContext,
) -> Result<Option<Awp2Deployment>, PrepareError> {
    let Gridder::Awp2 {
        evla_surface,
        wprojplanes,
        computepastep,
        ..
    } = &request.gridder
    else {
        return Ok(None);
    };
    let windows = &surveyed.survey.spectral_windows;
    if !supported_windows(
        windows,
        request.specmode,
        request.interpolation,
        request.deconvolver,
    ) {
        return Err(PrepareError::Awp2Selection);
    }
    let window = &windows[0];
    if surveyed.stores.len() != 1
        || surveyed.msv2.is_none()
        || surveyed
            .survey
            .fields
            .iter()
            .any(|field| surveyed.engine.field_moves(*field as usize))
        || windows
            .iter()
            .any(|window| window.frequency_reference != FrequencyRef::TOPO)
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
    let last_edge = surveyed.spectral.output_channels as f64 - 0.5;
    let [lo, hi] = [-0.5, last_edge]
        .map(|pixel| spectral.reference_frequency_hz + pixel * spectral.increment_hz);
    let band = [
        converter.convert_hz(lo, &frame)?.hz(),
        converter.convert_hz(hi, &frame)?.hz(),
    ];
    let path = evla_surface
        .as_deref()
        .ok_or(PrepareError::Awp2SurfacePath)?;
    // Cover the bounded dish parser and the frequency and PA axes before
    // allocating them. The latter stay charged with the deployment.
    let channels: usize = windows
        .iter()
        .map(|window| window.frequencies_hz.len())
        .sum();
    let frequency_scratch = channels.max(1).saturating_mul(2 * size_of::<f64>()) as u64;
    let mut charge = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "AWP2 dish surface",
            memory: (16_u64 << 20).saturating_add(frequency_scratch),
        },
    )?;
    let frequencies_hz = beam_frequencies(windows, band)?;
    let count = wprojplanes.map_or(1, std::num::NonZeroUsize::get);
    let maximum_w = surveyed.survey.w.maximum_abs_m * frequencies_hz.last().unwrap()
        / super::specification::SPEED_OF_LIGHT_M_PER_S;
    let step = maximum_w / (count.saturating_sub(1).max(1) as f64).powi(2);
    let w_planes: Box<[f64]> = (0..count).map(|i| (i as f64).powi(2) * step).collect();
    let row_frequency_hz = if count == 1 {
        // The only W plane is zero, so no buffer-frequency scaling is needed.
        spectral.reference_frequency_hz
    } else {
        let channels = super::selection::selected_channels(
            request,
            &surveyed.survey.spw_channels,
            window.spw_id,
            &window.frequencies_hz,
        )?;
        channels
            .iter()
            .map(|&channel| window.frequencies_hz[channel])
            .sum::<f64>()
            / channels.len() as f64
    };
    let surface = native_aw::read_dish_surface(path)?;
    let (pa_centres, feed_angle_rad) = if *computepastep < 180.0 {
        squint_centres(
            surveyed,
            window.spw_id,
            *computepastep,
            count,
            frequencies_hz.len(),
        )?
    } else {
        (Box::default(), 0.0)
    };
    charge.retain(
        (std::mem::size_of_val(surface.samples())
            + std::mem::size_of_val(frequencies_hz.as_ref())
            + std::mem::size_of_val(w_planes.as_ref())
            + std::mem::size_of_val(pa_centres.as_ref())
            + 2 * size_of::<usize>()) as u64,
    );
    Ok(Some(Awp2Deployment {
        single_field: surveyed.survey.fields.len() == 1 && count == 1,
        legacy_vla: domain.telescope.trim() == "VLA",
        frequencies_hz,
        w_planes,
        row_frequency_hz,
        pa_centres,
        feed_angle_rad,
        surface,
        _charge: charge,
    }))
}

/// The finite PA lattice AWConvFuncHolder builds over the observed angles.
fn squint_centres(
    surveyed: &Surveyed<'_>,
    spw_id: usize,
    step_deg: f32,
    w_count: usize,
    frequency_count: usize,
) -> Result<(Box<[f64]>, f32), PrepareError> {
    if surveyed.survey.fields.len() != 1 {
        return Err(PrepareError::Awp2Selection);
    }
    let feed_rad =
        native_aw::receptor_zero_angle(surveyed.catalogue, surveyed.msv2, spw_id as u32)? as f32;
    let mut limits = [f64::INFINITY, f64::NEG_INFINITY];
    for epoch in surveyed.survey.window_epochs(spw_id) {
        // AWPLPG::init surveys even an all-flagged buffer.
        // Its MJD-zero feed angle lies outside the native Measures range.
        if !epoch.has_unflagged_cross {
            return Err(PrepareError::Awp2FlaggedPaEpoch);
        }
        let pa = f64::from(native_aw::feed_pa(
            surveyed,
            epoch.time_mjd_seconds,
            epoch.field_id,
            feed_rad,
        )?);
        limits = [limits[0].min(pa), limits[1].max(pa)];
    }
    let [minimum, maximum] = limits;
    // The all-identical-angle special case in AWPLPG does not establish
    // an observable range. Leave it outside the verified scope.
    if minimum >= maximum {
        return Err(PrepareError::Awp2Selection);
    }
    let limit = usize::from(u16::MAX) / w_count.max(frequency_count);
    Ok((
        pa_centres(minimum.abs().max(maximum), step_deg, limit)?,
        feed_rad,
    ))
}

fn pa_centres(max_abs_pa: f64, step_deg: f32, limit: usize) -> Result<Box<[f64]>, PrepareError> {
    let step_rad = f64::from(step_deg) * std::f64::consts::PI / 180.0;
    let count = (2.0 * max_abs_pa / step_rad).ceil();
    if !(1.0..=limit as f64).contains(&count) {
        return Err(PrepareError::Awp2Selection);
    }
    Ok((0..count as usize)
        .map(|index| index as f64 * step_rad - max_abs_pa)
        .collect())
}

fn supported_windows(
    windows: &[SourceSpectralWindow],
    mode: SpecMode,
    interpolation: CubeInterpolation,
    deconvolver: Deconvolver,
) -> bool {
    match mode {
        SpecMode::Mfs => {
            windows.len() == 1
                || (deconvolver == Deconvolver::Mtmfs
                    && !windows.is_empty()
                    && windows
                        .iter()
                        .all(|window| window.frequencies_hz.len() == 1))
        }
        SpecMode::Cube => {
            !windows.is_empty()
                && (interpolation == CubeInterpolation::Nearest
                    || windows
                        .iter()
                        .all(|window| window.frequencies_hz.len() == 1))
        }
        _ => false,
    }
}

/// SimplePBConvFunc samples the entire native window before applying the
/// image band. AWPLPG merges the surviving frequencies of all buffers.
fn beam_frequencies(
    windows: &[SourceSpectralWindow],
    [first_edge, last_edge]: [f64; 2],
) -> Result<Box<[f64]>, PrepareError> {
    let band = first_edge.min(last_edge)..=first_edge.max(last_edge);
    let mut frequencies = Vec::new();
    for window in windows {
        let [low, high] = window
            .frequencies_hz
            .iter()
            .fold([f64::INFINITY, f64::NEG_INFINITY], |[low, high], &hz| {
                [low.min(hz), high.max(hz)]
            });
        if !(0.9e9..=2e9 + 1e-6).contains(&low) || !(0.9e9..=2e9 + 1e-6).contains(&high) {
            return Err(PrepareError::Awp2Selection);
        }
        let tolerance = (high / 100.0).max(window.channel_widths_hz[0].abs());
        let count = (((high - low).round() / tolerance) as usize).max(1);
        frequencies.extend(
            (0..count)
                .map(|k| low + k as f64 * tolerance)
                .filter(|hz| band.contains(hz)),
        );
    }
    frequencies.sort_unstable_by(f64::total_cmp);
    frequencies.dedup();
    if frequencies.len() > usize::from(u16::MAX) {
        return Err(PrepareError::Awp2Selection);
    }
    if frequencies.is_empty() {
        frequencies.push((first_edge + last_edge) / 2.0);
    }
    Ok(frequencies.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn awp2_pa_lattice_uses_the_captured_extent_and_exclusive_end() {
        // AWConvFuncHolder on vla_wideband_2ptg_w_squint.ms, field 1.
        let max_pa = 3.132_975_339_889_526_4_f64;
        let centres = pa_centres(max_pa, 15.0, 24).unwrap();
        assert_eq!(centres.len(), 24);
        assert_eq!(centres[0], -max_pa);
        assert!((centres[1] + 2.871_175_952_090_377).abs() < 1e-15);
        assert!((centres[23] - 2.888_410_579_490_91).abs() < 1e-15);
        assert!(pa_centres(max_pa, 15.0, 23).is_err());
        let half_turn = 90.0_f64.to_radians();
        assert_eq!(
            &*pa_centres(half_turn, 90.0, 2).unwrap(),
            &[-half_turn, 0.0]
        );
    }

    #[test]
    fn awp2_cube_nearest_accepts_a_multichannel_window() {
        let windows = [SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: FrequencyRef::TOPO,
            frequencies_hz: vec![1.0e9, 1.5e9, 2.0e9],
            channel_widths_hz: vec![0.5e9; 3],
        }];
        assert!(supported_windows(
            &windows,
            SpecMode::Cube,
            CubeInterpolation::Nearest,
            Deconvolver::Hogbom
        ));
        for interpolation in [CubeInterpolation::Linear, CubeInterpolation::Cubic] {
            assert!(!supported_windows(
                &windows,
                SpecMode::Cube,
                interpolation,
                Deconvolver::Hogbom
            ));
        }
    }

    #[test]
    fn awp2_taylor_admits_several_single_channel_windows() {
        let mut windows = [1.2e9, 1.5e9, 1.8e9].map(|frequency| SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: FrequencyRef::TOPO,
            frequencies_hz: vec![frequency],
            channel_widths_hz: vec![2.5e7],
        });
        for (index, window) in windows.iter_mut().enumerate() {
            window.spw_id = index;
        }
        let admitted = |windows: &[SourceSpectralWindow], deconvolver| {
            supported_windows(
                windows,
                SpecMode::Mfs,
                CubeInterpolation::Linear,
                deconvolver,
            )
        };
        assert!(admitted(&windows, Deconvolver::Mtmfs));
        assert!(!admitted(&windows, Deconvolver::Hogbom));
        windows[0].frequencies_hz.push(1.225e9);
        windows[0].channel_widths_hz.push(2.5e7);
        assert!(!admitted(&windows, Deconvolver::Mtmfs));
    }

    #[test]
    fn awp2_cube_combines_native_windows_in_frequency_order() {
        let windows = [1.8e9, 1.2e9, 1.5e9, 1.5e9, 1.95e9]
            .into_iter()
            .enumerate()
            .map(|(spw_id, frequency)| SourceSpectralWindow {
                spw_id,
                frequency_reference: FrequencyRef::TOPO,
                frequencies_hz: vec![frequency],
                channel_widths_hz: vec![2.5e7],
            })
            .collect::<Vec<_>>();
        let frequencies = beam_frequencies(&windows, [1.9e9, 1.1e9]).unwrap();
        assert_eq!(&*frequencies, &[1.2e9, 1.5e9, 1.8e9]);
    }

    #[test]
    fn awp2_samples_the_whole_spw_before_cutting_to_the_image_band() {
        let first = 999_999_999.999_999_9;
        let windows = [SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: FrequencyRef::TOPO,
            frequencies_hz: vec![first, 1.5e9, 2e9],
            channel_widths_hz: vec![5e8; 3],
        }];
        // CASA's captured inputs omit the high endpoint even though the
        // wide MFS image reaches it.
        let frequencies = beam_frequencies(&windows, [0.75e9, 2.25e9]).unwrap();
        assert_eq!(&*frequencies, &[first, 1.5e9]);
        // Inclusive image edges, regardless of axis order.
        let frequencies = beam_frequencies(&windows, [1.5e9, 1.25e9]).unwrap();
        assert_eq!(&*frequencies, &[1.5e9]);
        let frequencies = beam_frequencies(&windows, [1.5e9 + 1.0, 1.7e9]).unwrap();
        assert_eq!(&*frequencies, &[1.6e9 + 0.5]);
    }
}
