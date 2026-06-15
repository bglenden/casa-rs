// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::VecDeque, fs, path::PathBuf};

use ndarray::s;

use super::*;
use crate::weighting::fractional_bandwidth_from_frequency_range;

/// Run spectral-cube imaging for an ordered set of spectral planes.
///
/// Each spectral plane is imaged independently in the selected data-channel
/// frame using the same concrete path as [`run_imaging()`], and the resulting
/// products are stacked onto a real spectral axis in CASA ordering. This
/// cleaned-cube wave intentionally stays narrow: runtime Doppler correction is
/// still expected to happen in the MeasurementSet adapter, and per-channel
/// CLEAN currently supports [`Deconvolver::Hogbom`], [`Deconvolver::Clark`],
/// and per-plane [`Deconvolver::Multiscale`].
pub fn run_cube(request: &CubeImagingRequest) -> Result<CubeImagingResult, ImagingError> {
    if request.clean.niter > 0
        || matches!(request.weight_density_mode, WeightDensityMode::PerPlane)
        || request
            .channels
            .iter()
            .any(|channel| !channel.density_batches.is_empty())
    {
        return run_clean_cube(request);
    }
    let total_started = Instant::now();
    request.validate()?;
    if request.compatibility != CompatibilityMode::CasaStandardMfs {
        return Err(ImagingError::Unsupported(
            "only CASA standard cube compatibility mode is implemented".to_string(),
        ));
    }
    if request.clean.niter > 0
        && !matches!(
            request.deconvolver,
            Deconvolver::Hogbom | Deconvolver::Clark | Deconvolver::Multiscale
        )
    {
        return Err(ImagingError::Unsupported(
            "cube CLEAN currently supports only Hogbom, Clark, and Multiscale deconvolution"
                .to_string(),
        ));
    }

    let [nx, ny] = request.geometry.image_shape;
    let nchan = request.channels.len();
    let mut psf = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut residual = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut model = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut image = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut sumwt = Array4::<f32>::zeros((1, 1, 1, nchan));
    let mut beams = Vec::with_capacity(nchan);
    let mut channel_diagnostics = Vec::with_capacity(nchan);
    let mut warnings = Vec::new();
    let mut stage_timings = ImagingStageTimings::default();
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;

    for (channel_index, channel) in request.channels.iter().enumerate() {
        let plane_request = ImagingRequest {
            geometry: request.geometry,
            visibility_batches: channel.visibility_batches.clone(),
            gridder_mode: GridderMode::Standard,
            plane_stokes: request.plane_stokes,
            weighting: request.weighting,
            reffreq_hz: channel.channel_frequency_hz,
            selected_frequency_range_hz: [
                channel.channel_frequency_hz,
                channel.channel_frequency_hz,
            ],
            deconvolver: request.deconvolver,
            multiscale_scales: request.multiscale_scales.clone(),
            small_scale_bias: request.small_scale_bias,
            clean: request.clean,
            clean_mask: clean_mask_for_channel(request, channel_index),
            initial_model: None,
            w_term_mode: request.w_term_mode,
            w_project_planes: request.w_project_planes,
            compatibility: request.compatibility,
        };
        match run_imaging(&plane_request) {
            Ok(plane_result) => {
                psf.slice_mut(s![.., .., 0, channel_index])
                    .assign(&plane_result.psf.slice(s![.., .., 0, 0]));
                residual
                    .slice_mut(s![.., .., 0, channel_index])
                    .assign(&plane_result.residual.slice(s![.., .., 0, 0]));
                model
                    .slice_mut(s![.., .., 0, channel_index])
                    .assign(&plane_result.model.slice(s![.., .., 0, 0]));
                image
                    .slice_mut(s![.., .., 0, channel_index])
                    .assign(&plane_result.image.slice(s![.., .., 0, 0]));
                sumwt[(0, 0, 0, channel_index)] = plane_result.sumwt[(0, 0, 0, 0)];

                for warning in &plane_result.diagnostics.warnings {
                    warnings.push(format!("channel {channel_index}: {warning}"));
                }
                add_stage_timings(&mut stage_timings, plane_result.diagnostics.stage_timings);
                gridded_samples += plane_result.diagnostics.gridded_samples;
                skipped_samples += plane_result.diagnostics.skipped_samples;
                beams.push(plane_result.beam);
                channel_diagnostics.push(plane_result.diagnostics);
            }
            Err(ImagingError::NoUsableSamples) => {
                let warning = "no usable visibility samples remain after validation and flagging; writing blank cube plane".to_string();
                warnings.push(format!("channel {channel_index}: {warning}"));
                beams.push(None);
                channel_diagnostics.push(blank_plane_diagnostics(&plane_request, warning, None));
            }
            Err(error) => return Err(error),
        }
    }
    stage_timings.total = total_started.elapsed();

    let channel_frequencies_hz = request
        .channels
        .iter()
        .map(|channel| channel.channel_frequency_hz)
        .collect::<Vec<_>>();
    let reffreq_hz = 0.5
        * (channel_frequencies_hz[0] + channel_frequencies_hz[channel_frequencies_hz.len() - 1]);

    Ok(CubeImagingResult {
        psf,
        residual,
        model,
        image,
        sumwt,
        clean_mask: None,
        restored_beams: beams.clone(),
        beams,
        diagnostics: CubeImagingDiagnostics {
            warnings,
            gridded_samples,
            skipped_samples,
            major_cycles: casa_major_cycle_count(0, request.clean.niter),
            minor_iterations: 0,
            clean_stop_reason: None,
            channel_diagnostics,
            stage_timings,
        },
        compatibility: CompatibilityMetadata {
            axis_order: [
                AxisKind::RightAscension,
                AxisKind::Declination,
                AxisKind::Stokes,
                AxisKind::Frequency,
            ],
            plane_stokes: request.plane_stokes,
            reffreq_hz,
            channel_frequencies_hz,
            psf_units: String::new(),
            residual_units: "Jy/beam".to_string(),
            model_units: "Jy/pixel".to_string(),
            image_units: "Jy/beam".to_string(),
        },
    })
}

/// Run dirty-imaging cube formation for an ordered set of spectral planes.
///
/// This is a thin wrapper around [`run_cube()`] that fixes the per-channel
/// deconvolution controls to the dirty-image case.
pub fn run_dirty_cube(request: &CubeImagingRequest) -> Result<CubeImagingResult, ImagingError> {
    let mut dirty_request = request.clone();
    dirty_request.deconvolver = Deconvolver::Hogbom;
    dirty_request.multiscale_scales.clear();
    dirty_request.clean = dirty_clean_config(request.psf_cutoff);
    dirty_request.clean_mask = None;
    dirty_request.channel_clean_mask = None;
    dirty_request.auto_mask = None;
    run_cube(&dirty_request)
}

struct CubePlaneWork {
    request: ImagingRequest,
    weighted_batches: Vec<VisibilityBatch>,
    residual_sample_plans: Vec<Vec<Option<PlannedSample>>>,
    model_interpolation_batches: Vec<CubeModelInterpolationBatch>,
    dependent_model_channels: Vec<bool>,
    gridder: StandardGridder,
    psf_state: PsfState,
    clark_psf_patch: Option<ClarkPsfPatch>,
    multiscale_state: Option<MultiscaleState>,
    model: Array2<f32>,
    residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: Vec<String>,
    stage_timings: ImagingStageTimings,
    major_cycles: usize,
    minor_iterations: usize,
    clean_stop_reason: Option<CleanStopReason>,
    minor_cycle_traces: Vec<MinorCycleTrace>,
    final_cycle_threshold_jy_per_beam: f32,
    cached_peak_residual_jy_per_beam: f32,
    cached_nsigma_threshold_jy_per_beam: f32,
    auto_mask_beam: Option<BeamFit>,
    auto_mask_skip: bool,
    min_residual_peak_jy_per_beam: f32,
    divergence_warned: bool,
    is_blank: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HogbomMinorCycleOutcome {
    pub(crate) updated_model: bool,
    pub(crate) actual_updates: usize,
    pub(crate) reported_updates: usize,
    pub(crate) stop_reason: Option<CleanStopReason>,
    pub(crate) final_cycle_threshold_jy_per_beam: f32,
    pub(crate) final_nsigma_threshold_jy_per_beam: f32,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MinorCycleProbe {
    pub(crate) initial_scale_pixels: Option<f32>,
    pub(crate) initial_candidate_strength_jy_per_beam: Option<f32>,
    pub(crate) initial_candidate_position: Option<[usize; 2]>,
}

fn blank_psf_state(image_shape: [usize; 2]) -> PsfState {
    PsfState {
        psf: Array2::<f32>::zeros((image_shape[0], image_shape[1])),
        normalization_sumwt: 0.0,
        reported_sumwt: 0.0,
        psf_peak: 0.0,
        gridded_samples: 0,
        skipped_samples: 0,
    }
}

fn imaging_progress_enabled() -> bool {
    env::var_os("CASA_RS_IMAGING_PROGRESS").is_some()
}

fn residual_metrics(
    residual: &Array2<f32>,
    clean_mask: Option<&Array2<bool>>,
    clean: CleanConfig,
) -> (f32, f32) {
    (
        peak_abs_value_masked(residual, clean_mask),
        nsigma_threshold_jy_per_beam(residual, clean_mask, clean),
    )
}

fn clean_mask_for_channel(
    request: &CubeImagingRequest,
    channel_index: usize,
) -> Option<Array2<bool>> {
    let shared = request.clean_mask.clone();
    let Some(channel_mask) = request.channel_clean_mask.as_ref() else {
        return shared;
    };
    let mut plane = channel_mask.slice(s![.., .., 0, channel_index]).to_owned();
    if let Some(shared) = shared.as_ref() {
        Zip::from(&mut plane)
            .and(shared)
            .for_each(|out, shared| *out = *out && *shared);
    }
    Some(plane)
}

#[derive(Debug, Clone, Copy)]
struct CubeAutoMaskBeamShape {
    sigma_x_pixels: f64,
    sigma_y_pixels: f64,
    position_angle_rad: f64,
}

#[derive(Debug, Clone, Copy)]
struct CubeAutoMaskStats {
    median: f32,
    robust_rms: f32,
    absmax: f32,
}

fn update_cube_auto_multithresh_masks(
    planes: &mut [CubePlaneWork],
    geometry: ImageGeometry,
    beams: &[Option<BeamFit>],
    config: CubeAutoMultiThresholdConfig,
    allow_grow: bool,
) {
    for (channel_index, plane) in planes.iter_mut().enumerate() {
        if plane.is_blank {
            continue;
        }
        let beam = beams.get(channel_index).copied().flatten();
        let min_region_pixels = cube_auto_mask_min_region_pixels(geometry, beam, config);
        let beam_shape = cube_auto_mask_beam_shape(geometry, beam, config);
        if plane.auto_mask_skip {
            continue;
        }
        let (updated_mask, used_noise_threshold) = cube_auto_multithresh_plane_mask(
            &plane.residual,
            plane.request.clean_mask.as_ref(),
            plane.max_psf_sidelobe_level,
            min_region_pixels,
            beam_shape,
            config,
            allow_grow,
        );
        if used_noise_threshold && !updated_mask.iter().any(|value| *value) {
            plane.auto_mask_skip = true;
        }
        plane.request.clean_mask = Some(updated_mask);
        let (peak, nsigma) = residual_metrics(
            &plane.residual,
            plane.request.clean_mask.as_ref(),
            plane.request.clean,
        );
        plane.cached_peak_residual_jy_per_beam = peak;
        plane.cached_nsigma_threshold_jy_per_beam = nsigma;
    }
}

fn cube_auto_multithresh_plane_mask(
    residual: &Array2<f32>,
    previous_mask: Option<&Array2<bool>>,
    max_psf_sidelobe_level: f32,
    min_region_pixels: usize,
    beam_shape: Option<CubeAutoMaskBeamShape>,
    config: CubeAutoMultiThresholdConfig,
    allow_grow: bool,
) -> (Array2<bool>, bool) {
    let Some(stats) = cube_auto_mask_stats(residual) else {
        return (
            previous_mask
                .cloned()
                .unwrap_or_else(|| Array2::<bool>::from_elem(residual.dim(), false)),
            false,
        );
    };
    let sidelobe_threshold =
        stats.median + max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax;
    let noise_threshold = stats.median + config.noise_threshold * stats.robust_rms;
    let low_noise_threshold = stats.median
        + (max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax)
            .max(config.low_noise_threshold * stats.robust_rms);
    let main_threshold = sidelobe_threshold.max(noise_threshold);
    let used_noise_threshold = noise_threshold > sidelobe_threshold;
    let mut current = previous_mask
        .cloned()
        .unwrap_or_else(|| Array2::<bool>::from_elem(residual.dim(), false));

    let mut threshold_mask = cube_threshold_positive_mask(residual, main_threshold);
    cube_prune_small_regions(&mut threshold_mask, min_region_pixels);
    let threshold_mask =
        cube_smooth_and_cut_mask(&threshold_mask, beam_shape, config.cut_threshold);
    Zip::from(&mut current)
        .and(&threshold_mask)
        .for_each(|out, generated| *out = *out || *generated);

    if allow_grow && config.grow_iterations > 0 {
        let mut grown = previous_mask
            .cloned()
            .unwrap_or_else(|| Array2::<bool>::from_elem(residual.dim(), false));
        let constraint = cube_threshold_positive_mask(residual, low_noise_threshold);
        grow_mask_constrained(&mut grown, &constraint, config.grow_iterations);
        if config.do_grow_prune {
            cube_prune_small_regions(&mut grown, min_region_pixels);
        }
        let grown = cube_smooth_and_cut_mask(&grown, beam_shape, config.cut_threshold);
        Zip::from(&mut current)
            .and(&grown)
            .for_each(|out, generated| *out = *out || *generated);
    }

    if config.negative_threshold > 0.0 {
        let negative_threshold = stats.median
            - (max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax)
                .max(config.negative_threshold * stats.robust_rms);
        let mut negative = cube_threshold_negative_mask(residual, negative_threshold);
        cube_prune_small_regions(&mut negative, min_region_pixels);
        let negative = cube_smooth_and_cut_mask(&negative, beam_shape, config.cut_threshold);
        Zip::from(&mut current)
            .and(&negative)
            .for_each(|out, generated| *out = *out || *generated);
    }
    (current, used_noise_threshold)
}

fn cube_auto_mask_stats(residual: &Array2<f32>) -> Option<CubeAutoMaskStats> {
    let mut values = residual
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let median = cube_sorted_median(&values);
    let absmax = values
        .iter()
        .fold(0.0f32, |acc, value| acc.max(value.abs()));
    let mut deviations = values
        .iter()
        .map(|value| (*value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(|a, b| a.total_cmp(b));
    let robust_rms = cube_sorted_median(&deviations) * 1.4826;
    Some(CubeAutoMaskStats {
        median,
        robust_rms,
        absmax,
    })
}

fn cube_sorted_median(values: &[f32]) -> f32 {
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        0.5 * (values[middle - 1] + values[middle])
    } else {
        values[middle]
    }
}

fn cube_threshold_positive_mask(residual: &Array2<f32>, threshold: f32) -> Array2<bool> {
    residual.mapv(|value| value.is_finite() && value > threshold)
}

fn cube_threshold_negative_mask(residual: &Array2<f32>, threshold: f32) -> Array2<bool> {
    residual.mapv(|value| value.is_finite() && value < threshold)
}

fn cube_auto_mask_min_region_pixels(
    geometry: ImageGeometry,
    beam: Option<BeamFit>,
    config: CubeAutoMultiThresholdConfig,
) -> usize {
    if config.min_beam_frac <= 0.0 {
        return 1;
    }
    let Some(beam) = beam else {
        return 1;
    };
    let cell_area = geometry.cell_size_rad[0].abs() * geometry.cell_size_rad[1].abs();
    if !(cell_area.is_finite() && cell_area > 0.0) {
        return 1;
    }
    let beam_area = std::f64::consts::PI * beam.major_fwhm_rad.abs() * beam.minor_fwhm_rad.abs()
        / (4.0 * std::f64::consts::LN_2);
    if !(beam_area.is_finite() && beam_area > 0.0) {
        return 1;
    }
    ((config.min_beam_frac as f64 * beam_area / cell_area).ceil() as usize).max(1)
}

fn cube_auto_mask_beam_shape(
    geometry: ImageGeometry,
    beam: Option<BeamFit>,
    config: CubeAutoMultiThresholdConfig,
) -> Option<CubeAutoMaskBeamShape> {
    let beam = beam?;
    if config.smooth_factor <= 0.0 {
        return None;
    }
    let cell_x = geometry.cell_size_rad[0].abs();
    let cell_y = geometry.cell_size_rad[1].abs();
    if !(cell_x.is_finite() && cell_x > 0.0 && cell_y.is_finite() && cell_y > 0.0) {
        return None;
    }
    let sigma_from_fwhm = |fwhm_rad: f64, cell_rad: f64| {
        config.smooth_factor as f64 * fwhm_rad.abs()
            / (2.0 * (2.0 * std::f64::consts::LN_2).sqrt() * cell_rad)
    };
    let sigma_x_pixels = sigma_from_fwhm(beam.minor_fwhm_rad, cell_x);
    let sigma_y_pixels = sigma_from_fwhm(beam.major_fwhm_rad, cell_y);
    (sigma_x_pixels.is_finite()
        && sigma_x_pixels > 0.0
        && sigma_y_pixels.is_finite()
        && sigma_y_pixels > 0.0)
        .then_some(CubeAutoMaskBeamShape {
            sigma_x_pixels,
            sigma_y_pixels,
            position_angle_rad: beam.position_angle_rad,
        })
}

fn cube_smooth_and_cut_mask(
    mask: &Array2<bool>,
    beam_shape: Option<CubeAutoMaskBeamShape>,
    cut_threshold: f32,
) -> Array2<bool> {
    let Some(beam_shape) = beam_shape else {
        return mask.clone();
    };
    let (nx, ny) = mask.dim();
    let radius_x = (beam_shape.sigma_x_pixels * 4.0).ceil().max(1.0) as isize;
    let radius_y = (beam_shape.sigma_y_pixels * 4.0).ceil().max(1.0) as isize;
    let cos_pa = beam_shape.position_angle_rad.cos();
    let sin_pa = beam_shape.position_angle_rad.sin();
    let mut smoothed = Array2::<f32>::zeros((nx, ny));
    for ((x, y), value) in mask.indexed_iter() {
        if !*value {
            continue;
        }
        let x = x as isize;
        let y = y as isize;
        for dx in -radius_x..=radius_x {
            let xx = x + dx;
            if !(0..nx as isize).contains(&xx) {
                continue;
            }
            for dy in -radius_y..=radius_y {
                let yy = y + dy;
                if !(0..ny as isize).contains(&yy) {
                    continue;
                }
                let rotated_x = dx as f64 * cos_pa + dy as f64 * sin_pa;
                let rotated_y = -dx as f64 * sin_pa + dy as f64 * cos_pa;
                let exponent = -0.5
                    * ((rotated_x / beam_shape.sigma_x_pixels).powi(2)
                        + (rotated_y / beam_shape.sigma_y_pixels).powi(2));
                smoothed[(xx as usize, yy as usize)] += exponent.exp() as f32;
            }
        }
    }
    let peak = smoothed.iter().copied().fold(0.0f32, f32::max);
    if !(peak.is_finite() && peak > 0.0) {
        return Array2::<bool>::from_elem((nx, ny), false);
    }
    let threshold = cut_threshold.max(0.0) * peak;
    smoothed.mapv(|value| value.is_finite() && value > threshold)
}

fn grow_mask_constrained(
    mask: &mut Array2<bool>,
    constraint: &Array2<bool>,
    max_iterations: usize,
) {
    let (nx, ny) = mask.dim();
    for _ in 0..max_iterations {
        let mut next = mask.clone();
        let mut changed = false;
        for x in 0..nx {
            for y in 0..ny {
                if !mask[(x, y)] || !constraint[(x, y)] {
                    continue;
                }
                for (nx0, ny0) in cube_neighbors4(mask.dim(), x, y) {
                    if !next[(nx0, ny0)] {
                        next[(nx0, ny0)] = true;
                        changed = true;
                    }
                }
            }
        }
        *mask = next;
        if !changed {
            break;
        }
    }
    Zip::from(mask)
        .and(constraint)
        .for_each(|out, allowed| *out = *out && *allowed);
}

fn cube_prune_small_regions(mask: &mut Array2<bool>, min_pixels: usize) {
    if min_pixels <= 1 {
        return;
    }
    let (nx, ny) = mask.dim();
    let mut visited = Array2::<bool>::from_elem((nx, ny), false);
    for x0 in 0..nx {
        for y0 in 0..ny {
            if visited[(x0, y0)] || !mask[(x0, y0)] {
                continue;
            }
            let mut region = Vec::new();
            let mut queue = VecDeque::from([(x0, y0)]);
            visited[(x0, y0)] = true;
            while let Some((x, y)) = queue.pop_front() {
                region.push((x, y));
                for (nx0, ny0) in cube_neighbors4(mask.dim(), x, y) {
                    if !visited[(nx0, ny0)] && mask[(nx0, ny0)] {
                        visited[(nx0, ny0)] = true;
                        queue.push_back((nx0, ny0));
                    }
                }
            }
            if region.len() < min_pixels {
                for (x, y) in region {
                    mask[(x, y)] = false;
                }
            }
        }
    }
}

fn cube_neighbors4(
    (nx, ny): (usize, usize),
    x: usize,
    y: usize,
) -> impl Iterator<Item = (usize, usize)> {
    let mut neighbors = [(usize::MAX, usize::MAX); 4];
    let mut count = 0;
    if x > 0 {
        neighbors[count] = (x - 1, y);
        count += 1;
    }
    if x + 1 < nx {
        neighbors[count] = (x + 1, y);
        count += 1;
    }
    if y > 0 {
        neighbors[count] = (x, y - 1);
        count += 1;
    }
    if y + 1 < ny {
        neighbors[count] = (x, y + 1);
        count += 1;
    }
    neighbors.into_iter().take(count)
}

fn final_cube_clean_mask(
    planes: &[CubePlaneWork],
    nx: usize,
    ny: usize,
    nchan: usize,
) -> Option<Array4<bool>> {
    if !planes
        .iter()
        .any(|plane| plane.request.clean_mask.is_some())
    {
        return None;
    }
    let mut mask = Array4::<bool>::from_elem((nx, ny, 1, nchan), false);
    for (channel_index, plane) in planes.iter().enumerate() {
        if let Some(channel_mask) = plane.request.clean_mask.as_ref() {
            mask.slice_mut(s![.., .., 0, channel_index])
                .assign(channel_mask);
        }
    }
    Some(mask)
}

fn blank_plane_diagnostics(
    request: &ImagingRequest,
    warning: String,
    clean_stop_reason: Option<CleanStopReason>,
) -> ImagingDiagnostics {
    ImagingDiagnostics {
        warnings: vec![warning],
        gridded_samples: 0,
        skipped_samples: 0,
        normalization_sumwt: 0.0,
        reported_sumwt: 0.0,
        psf_peak_normalization: 0.0,
        major_cycles: casa_major_cycle_count(0, request.clean.niter),
        minor_iterations: 0,
        clean_stop_reason,
        minor_cycle_traces: Vec::new(),
        initial_residual_peak_jy_per_beam: 0.0,
        final_residual_peak_jy_per_beam: 0.0,
        max_abs_w_lambda: 0.0,
        fractional_bandwidth: 0.0,
        max_psf_sidelobe_level: 0.0,
        final_cycle_threshold_jy_per_beam: request.clean.threshold_jy_per_beam,
        clean_mask_pixels: request
            .clean_mask
            .as_ref()
            .map(|mask| mask.iter().filter(|value| **value).count())
            .unwrap_or(request.geometry.nx() * request.geometry.ny()),
        beam_fit_attempts: 0,
        beam_fit_cutoff_used: Some(request.clean.psf_cutoff),
        beam_fit_debug: None,
        mosaic_weight_image: None,
        stage_timings: ImagingStageTimings::default(),
    }
}

fn compute_dirty_cube_plane_initial_residual(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>, Array2<f32>), ImagingError> {
    let [nx, ny] = request.geometry.image_shape;
    let model = Array2::<f32>::zeros((nx, ny));
    if request.clean.niter == 0
        && request.w_term_mode == WTermMode::None
        && !standard_mfs_fixed_tile_backend_enabled()
        && !standard_mfs_metal_backend_enabled()
        && standard_mfs_sample_count(weighted_batches) <= standard_mfs_executor_max_samples()
    {
        let executor_build_started = Instant::now();
        let mut executor = StandardMfsCpuExecutor::new(gridder, weighted_batches)?;
        let executor_build_elapsed = executor_build_started.elapsed();
        stage_timings.executor_build += executor_build_elapsed;
        if crate::profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_executor_build caller=dirty_cube_initial samples={} elapsed_ms={:.3}",
                standard_mfs_sample_count(weighted_batches),
                crate::profile::millis(executor_build_elapsed),
            );
        }
        let psf_state = compute_psf_standard(&mut executor, stage_timings)?;
        let residual = compute_residual_standard_with_executor(
            &mut executor,
            &model,
            &psf_state,
            stage_timings,
        )?;
        return Ok((psf_state, model, residual));
    }

    let psf_state = compute_psf(request, weighted_batches, gridder, stage_timings)?;
    let residual = compute_residual(
        request,
        weighted_batches,
        gridder,
        &model,
        &psf_state,
        crate::StandardMfsExecutionConfig::default(),
        stage_timings,
    )?;
    Ok((psf_state, model, residual))
}

fn run_clean_cube(request: &CubeImagingRequest) -> Result<CubeImagingResult, ImagingError> {
    let total_started = Instant::now();
    request.validate()?;
    if request.compatibility != CompatibilityMode::CasaStandardMfs {
        return Err(ImagingError::Unsupported(
            "only CASA standard cube compatibility mode is implemented".to_string(),
        ));
    }
    if !matches!(
        request.deconvolver,
        Deconvolver::Hogbom | Deconvolver::Clark | Deconvolver::Multiscale
    ) {
        return Err(ImagingError::Unsupported(
            "cube CLEAN currently supports only Hogbom, Clark, and Multiscale deconvolution"
                .to_string(),
        ));
    }

    let [nx, ny] = request.geometry.image_shape;
    let nchan = request.channels.len();
    let combined_density_batches =
        matches!(request.weight_density_mode, WeightDensityMode::Combined).then(|| {
            request
                .channels
                .iter()
                .flat_map(|channel| channel.visibility_batches.iter().cloned())
                .collect::<Vec<_>>()
        });
    let cube_fractional_bandwidth = cube_fractional_bandwidth(request);
    let mut planes = Vec::with_capacity(nchan);
    for (channel_index, channel) in request.channels.iter().enumerate() {
        let plane_request = ImagingRequest {
            geometry: request.geometry,
            visibility_batches: channel.visibility_batches.clone(),
            gridder_mode: GridderMode::Standard,
            plane_stokes: request.plane_stokes,
            weighting: request.weighting,
            reffreq_hz: channel.channel_frequency_hz,
            selected_frequency_range_hz: [
                channel.channel_frequency_hz,
                channel.channel_frequency_hz,
            ],
            deconvolver: request.deconvolver,
            multiscale_scales: request.multiscale_scales.clone(),
            small_scale_bias: request.small_scale_bias,
            clean: request.clean,
            clean_mask: clean_mask_for_channel(request, channel_index),
            initial_model: None,
            w_term_mode: request.w_term_mode,
            w_project_planes: request.w_project_planes,
            compatibility: request.compatibility,
        };
        plane_request.validate()?;
        let gridder = StandardGridder::new(plane_request.geometry)?;
        let mut plane_stage_timings = ImagingStageTimings::default();
        let density_batches = match request.weight_density_mode {
            WeightDensityMode::Combined => combined_density_batches
                .as_deref()
                .expect("combined cube density batches prepared"),
            WeightDensityMode::PerPlane if channel.density_batches.is_empty() => {
                &plane_request.visibility_batches
            }
            WeightDensityMode::PerPlane => &channel.density_batches,
        };
        let weighting_started = Instant::now();
        let weighted_batches = apply_weighting_with_density_source(
            plane_request.weighting,
            request.weight_density_mode,
            request.uv_taper,
            cube_fractional_bandwidth,
            &plane_request.visibility_batches,
            density_batches,
            &gridder,
        )?;
        plane_stage_timings.weighting += weighting_started.elapsed();
        let residual_sample_plans = if plane_request.clean.niter > 0 {
            let executor_build_started = Instant::now();
            let plans = build_standard_residual_sample_plans(&gridder, &weighted_batches);
            plane_stage_timings.executor_build += executor_build_started.elapsed();
            plans
        } else {
            Vec::new()
        };
        let (
            psf_state,
            auto_mask_beam,
            model,
            residual,
            multiscale_state,
            initial_peak,
            warnings,
            is_blank,
        ) =
            match compute_dirty_cube_plane_initial_residual(
                &plane_request,
                &weighted_batches,
                &gridder,
                &mut plane_stage_timings,
            ) {
                Ok((psf_state, model, residual)) => {
                    let BeamFitOutcome {
                        beam: auto_mask_beam,
                        ..
                    } = fit_beam_from_psf(
                        &psf_state.psf,
                        plane_request.geometry.cell_size_rad,
                        plane_request.clean.psf_cutoff,
                    );
                    let multiscale_state =
                        matches!(plane_request.deconvolver, Deconvolver::Multiscale).then(|| {
                            let scales = effective_multiscale_scales(&plane_request);
                            build_multiscale_state(
                                &residual,
                                &psf_state.psf,
                                &scales,
                                plane_request.small_scale_bias,
                                plane_request.clean_mask.as_ref(),
                            )
                        });
                    let initial_peak =
                        peak_abs_value_masked(&residual, plane_request.clean_mask.as_ref());
                    (
                        psf_state,
                        auto_mask_beam,
                        model,
                        residual,
                        multiscale_state,
                        initial_peak,
                        Vec::new(),
                        false,
                    )
                }
                Err(ImagingError::NoUsableSamples) => (
                    blank_psf_state(plane_request.geometry.image_shape),
                    None,
                    Array2::<f32>::zeros((nx, ny)),
                    Array2::<f32>::zeros((nx, ny)),
                    None,
                    0.0,
                    vec![
                        "no usable visibility samples remain after validation and flagging; writing blank cube plane"
                            .to_string(),
                    ],
                    true,
                ),
                Err(error) => return Err(error),
            };
        let cached_nsigma_threshold_jy_per_beam = nsigma_threshold_jy_per_beam(
            &residual,
            plane_request.clean_mask.as_ref(),
            plane_request.clean,
        );
        planes.push(CubePlaneWork {
            clark_psf_patch: matches!(plane_request.deconvolver, Deconvolver::Clark).then(|| {
                build_clark_psf_patch(
                    &psf_state.psf,
                    plane_request.geometry.cell_size_rad,
                    plane_request.clean.psf_cutoff,
                )
            }),
            max_psf_sidelobe_level: estimate_psf_sidelobe_level(
                &psf_state.psf,
                plane_request.geometry.cell_size_rad,
                plane_request.clean.psf_cutoff,
            ),
            min_residual_peak_jy_per_beam: initial_peak,
            final_cycle_threshold_jy_per_beam: plane_request.clean.threshold_jy_per_beam,
            cached_peak_residual_jy_per_beam: initial_peak,
            cached_nsigma_threshold_jy_per_beam,
            auto_mask_beam,
            auto_mask_skip: false,
            request: plane_request,
            weighted_batches,
            residual_sample_plans,
            model_interpolation_batches: channel.model_interpolation_batches.clone(),
            dependent_model_channels: cube_model_dependency_mask(
                nchan,
                channel_index,
                &channel.model_interpolation_batches,
            ),
            gridder,
            psf_state,
            multiscale_state,
            model,
            residual,
            initial_peak,
            warnings,
            stage_timings: plane_stage_timings,
            major_cycles: 0,
            minor_iterations: 0,
            clean_stop_reason: is_blank.then_some(CleanStopReason::NoCleanablePixels),
            minor_cycle_traces: Vec::new(),
            divergence_warned: false,
            is_blank,
        });
    }

    let mut total_reported_minor_iterations = 0usize;
    let mut cube_major_cycle_blocks = 0usize;
    let mut cube_clean_stop_reason = None::<CleanStopReason>;
    let mut refresh_flags = vec![false; planes.len()];
    let mut final_refresh_pending = false;
    let cube_minor_cycle_capture = cube_minor_cycle_capture_config();
    let cube_clean_started = Instant::now();
    let cube_max_psf_sidelobe_level = planes
        .iter()
        .map(|plane| plane.max_psf_sidelobe_level)
        .fold(0.0f32, f32::max);
    let auto_mask_beams = select_restored_cube_beams(
        &planes
            .iter()
            .map(|plane| plane.auto_mask_beam)
            .collect::<Vec<_>>(),
        request.restoring_beam_mode,
    )?;
    if let Some(config) = request.auto_mask {
        update_cube_auto_multithresh_masks(
            &mut planes,
            request.geometry,
            &auto_mask_beams,
            config,
            false,
        );
    }
    while total_reported_minor_iterations < request.clean.niter {
        if request
            .clean
            .major_cycle_limit
            .is_some_and(|limit| cube_major_cycle_blocks >= limit)
        {
            cube_clean_stop_reason = Some(CleanStopReason::MajorCycleLimitReached);
            for plane in &mut planes {
                plane
                    .clean_stop_reason
                    .get_or_insert(CleanStopReason::MajorCycleLimitReached);
            }
            break;
        }
        let global_peak = planes
            .iter()
            .filter(|plane| !plane.is_blank)
            .map(|plane| plane.cached_peak_residual_jy_per_beam)
            .fold(0.0f32, f32::max);
        let cube_nsigma_threshold_jy_per_beam = global_nsigma_threshold_jy_per_beam(
            &planes
                .iter()
                .filter(|plane| !plane.is_blank)
                .map(|plane| plane.cached_nsigma_threshold_jy_per_beam)
                .collect::<Vec<_>>(),
        );
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            global_peak,
            request.clean.threshold_jy_per_beam,
            cube_nsigma_threshold_jy_per_beam,
        ) {
            cube_clean_stop_reason = Some(stop_reason);
            for plane in &mut planes {
                plane.clean_stop_reason.get_or_insert(stop_reason);
            }
            break;
        }

        let cube_cycle_threshold_jy_per_beam =
            compute_cycle_threshold(global_peak, cube_max_psf_sidelobe_level, request.clean);
        let capture_model_cube = cube_minor_cycle_capture.as_ref().and_then(|capture| {
            (capture.block_index == cube_major_cycle_blocks).then(|| {
                planes
                    .iter()
                    .map(|plane| plane.model.clone())
                    .collect::<Vec<_>>()
            })
        });
        let mut any_model_update = false;
        let mut updated_model_channels = vec![false; planes.len()];
        for (plane_index, plane) in planes.iter_mut().enumerate() {
            if plane.is_blank {
                continue;
            }
            let cycle_reported_niter = request.clean.minor_cycle_length;
            let start_reported_iteration = total_reported_minor_iterations;
            let plane_nsigma_threshold_jy_per_beam = plane.cached_nsigma_threshold_jy_per_beam;
            let start_peak_residual_jy_per_beam = plane.cached_peak_residual_jy_per_beam;
            maybe_capture_cube_minor_cycle_state(
                cube_minor_cycle_capture.as_ref(),
                plane_index,
                plane.minor_cycle_traces.len(),
                cycle_reported_niter,
                &plane.request,
                &plane.psf_state.psf,
                &plane.residual,
                &plane.model,
                capture_model_cube.as_deref(),
                cube_cycle_threshold_jy_per_beam,
                plane_nsigma_threshold_jy_per_beam,
            );
            let multiscale_probe = if plane.request.deconvolver == Deconvolver::Multiscale {
                let scales = effective_multiscale_scales(&plane.request);
                select_multiscale_candidate(
                    plane
                        .multiscale_state
                        .as_ref()
                        .expect("missing cube multiscale state"),
                    plane.request.clean_mask.as_ref(),
                    ImageWindow::full(plane.residual.dim()),
                )
                .map(|candidate| MinorCycleProbe {
                    initial_scale_pixels: Some(scales[candidate.scale_index]),
                    initial_candidate_strength_jy_per_beam: Some(candidate.strength),
                    initial_candidate_position: Some([candidate.position.0, candidate.position.1]),
                })
                .unwrap_or_default()
            } else {
                MinorCycleProbe::default()
            };
            let outcome = match plane.request.deconvolver {
                Deconvolver::Hogbom => run_hogbom_minor_cycle(
                    &plane.request,
                    &plane.psf_state,
                    &mut plane.model,
                    &mut plane.residual,
                    cycle_reported_niter,
                    cube_cycle_threshold_jy_per_beam,
                    plane_nsigma_threshold_jy_per_beam,
                    &mut plane.stage_timings,
                ),
                Deconvolver::Clark => run_clark_minor_cycle(
                    &plane.request,
                    &plane.psf_state.psf,
                    &mut plane.model,
                    &mut plane.residual,
                    cycle_reported_niter,
                    cube_cycle_threshold_jy_per_beam,
                    plane_nsigma_threshold_jy_per_beam,
                    plane
                        .clark_psf_patch
                        .as_ref()
                        .expect("missing cube Clark PSF patch"),
                    &mut plane.stage_timings,
                ),
                Deconvolver::Multiscale => run_multiscale_minor_cycle(
                    &plane.request,
                    &plane.psf_state.psf,
                    plane
                        .multiscale_state
                        .as_mut()
                        .expect("missing cube multiscale state"),
                    &mut plane.model,
                    &mut plane.residual,
                    cycle_reported_niter,
                    cube_cycle_threshold_jy_per_beam,
                    plane_nsigma_threshold_jy_per_beam,
                    &mut plane.stage_timings,
                ),
                Deconvolver::Mtmfs => {
                    return Err(ImagingError::Unsupported(
                        "cube CLEAN does not support deconvolver='mtmfs'".to_string(),
                    ));
                }
            };
            let trace = make_minor_cycle_trace(
                plane.minor_cycle_traces.len(),
                start_reported_iteration,
                outcome,
                start_peak_residual_jy_per_beam,
                &plane.residual,
                &plane.model,
                multiscale_probe,
            );
            plane.minor_iterations += outcome.actual_updates;
            plane.final_cycle_threshold_jy_per_beam = outcome.final_cycle_threshold_jy_per_beam;
            if let Some(reason) = outcome.stop_reason {
                plane.clean_stop_reason = Some(reason);
            }
            total_reported_minor_iterations += outcome.reported_updates;
            plane.minor_cycle_traces.push(trace);
            if !outcome.updated_model {
                continue;
            }
            any_model_update = true;
            updated_model_channels[plane_index] = true;
            if matches!(
                plane.request.deconvolver,
                Deconvolver::Hogbom | Deconvolver::Multiscale
            ) {
                let minor_peak =
                    peak_abs_value_masked(&plane.residual, plane.request.clean_mask.as_ref());
                update_divergence_state(
                    &mut plane.warnings,
                    &mut plane.min_residual_peak_jy_per_beam,
                    minor_peak,
                    &mut plane.divergence_warned,
                );
            }
        }
        if !any_model_update {
            cube_clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            for plane in &mut planes {
                plane
                    .clean_stop_reason
                    .get_or_insert(CleanStopReason::NoCleanablePixels);
            }
            break;
        }
        refresh_flags = cube_refresh_flags(&planes, &updated_model_channels);
        final_refresh_pending = true;
        cube_major_cycle_blocks += 1;
        if total_reported_minor_iterations >= request.clean.niter {
            cube_clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
            for plane in &mut planes {
                plane
                    .clean_stop_reason
                    .get_or_insert(CleanStopReason::IterationLimitReached);
            }
            break;
        }
        let refreshed_planes = refresh_cube_residuals_exact(&mut planes, &refresh_flags)?;
        final_refresh_pending = false;
        let global_peak_after_refresh = planes
            .iter()
            .filter(|plane| !plane.is_blank)
            .map(|plane| plane.cached_peak_residual_jy_per_beam)
            .fold(0.0f32, f32::max);
        let dominant_channel = planes
            .iter()
            .enumerate()
            .filter(|(_, plane)| !plane.is_blank)
            .max_by(|(_, left), (_, right)| {
                left.cached_peak_residual_jy_per_beam
                    .partial_cmp(&right.cached_peak_residual_jy_per_beam)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(index, plane)| {
                (
                    index,
                    plane.cached_peak_residual_jy_per_beam,
                    plane.cached_nsigma_threshold_jy_per_beam,
                )
            });
        let cube_nsigma_threshold_after_refresh_jy_per_beam = global_nsigma_threshold_jy_per_beam(
            &planes
                .iter()
                .filter(|plane| !plane.is_blank)
                .map(|plane| plane.cached_nsigma_threshold_jy_per_beam)
                .collect::<Vec<_>>(),
        );
        if imaging_progress_enabled() {
            let refreshed_channel_indices = refresh_flags
                .iter()
                .enumerate()
                .filter_map(|(index, refreshed)| refreshed.then_some(index))
                .collect::<Vec<_>>();
            eprintln!(
                "cube-clean block={} elapsed_s={:.3} reported_minor_iterations={} refreshed_planes={} refreshed_channels={:?} dominant_channel={:?} global_peak_jy_per_beam={:.9e} cube_nsigma_threshold_jy_per_beam={:.9e}",
                cube_major_cycle_blocks,
                cube_clean_started.elapsed().as_secs_f64(),
                total_reported_minor_iterations,
                refreshed_planes,
                refreshed_channel_indices,
                dominant_channel,
                global_peak_after_refresh,
                cube_nsigma_threshold_after_refresh_jy_per_beam,
            );
        }
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            global_peak_after_refresh,
            request.clean.threshold_jy_per_beam,
            cube_nsigma_threshold_after_refresh_jy_per_beam,
        ) {
            cube_clean_stop_reason = Some(stop_reason);
            for plane in &mut planes {
                plane.clean_stop_reason.get_or_insert(stop_reason);
            }
            break;
        }
        if let Some(config) = request.auto_mask {
            update_cube_auto_multithresh_masks(
                &mut planes,
                request.geometry,
                &auto_mask_beams,
                config,
                true,
            );
        }
    }

    if final_refresh_pending {
        refresh_cube_residuals_exact(&mut planes, &refresh_flags)?;
    }

    let mut fitted_beams = Vec::with_capacity(nchan);
    let mut beam_warning_sets = Vec::with_capacity(nchan);
    let mut beam_fit_attempts = Vec::with_capacity(nchan);
    let mut beam_fit_cutoff_used = Vec::with_capacity(nchan);
    let mut beam_fit_debug = Vec::with_capacity(nchan);
    for plane in &mut planes {
        if plane.is_blank {
            fitted_beams.push(None);
            beam_warning_sets.push(Vec::new());
            beam_fit_attempts.push(0usize);
            beam_fit_cutoff_used.push(Some(plane.request.clean.psf_cutoff));
            beam_fit_debug.push(None);
            continue;
        }
        let beam_fit_started = Instant::now();
        let BeamFitOutcome {
            beam,
            warnings,
            attempts,
            cutoff_used,
            debug,
        } = fit_beam_from_psf(
            &plane.psf_state.psf,
            plane.request.geometry.cell_size_rad,
            plane.request.clean.psf_cutoff,
        );
        plane.stage_timings.beam_fit += beam_fit_started.elapsed();
        fitted_beams.push(beam);
        beam_warning_sets.push(warnings);
        beam_fit_attempts.push(attempts);
        beam_fit_cutoff_used.push(cutoff_used);
        beam_fit_debug.push(debug);
    }
    let restored_beams = select_restored_cube_beams(&fitted_beams, request.restoring_beam_mode)?;

    let mut psf = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut residual = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut model = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut image = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut sumwt = Array4::<f32>::zeros((1, 1, 1, nchan));
    let mut beams = Vec::with_capacity(nchan);
    let mut result_restored_beams = Vec::with_capacity(nchan);
    let mut channel_diagnostics = Vec::with_capacity(nchan);
    let mut warnings = Vec::new();
    let mut stage_timings = ImagingStageTimings::default();
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;
    let clean_mask = final_cube_clean_mask(&planes, nx, ny, nchan);

    for (channel_index, mut plane) in planes.into_iter().enumerate() {
        let beam = fitted_beams[channel_index];
        let restored_beam = restored_beams[channel_index];
        let beam_warnings = beam_warning_sets[channel_index].clone();
        let beam_fit_attempts = beam_fit_attempts[channel_index];
        let beam_fit_cutoff_used = beam_fit_cutoff_used[channel_index];
        let beam_fit_debug = beam_fit_debug[channel_index].clone();
        let restored_image = if plane.is_blank {
            Array2::<f32>::zeros((nx, ny))
        } else {
            let restore_started = Instant::now();
            let residual_to_add = match (restored_beam, beam) {
                (Some(restored_beam), Some(fitted_beam))
                    if request.restoring_beam_mode == RestoringBeamMode::Common =>
                {
                    match rescale_residual_to_restored_beam(
                        &plane.residual,
                        plane.request.geometry.cell_size_rad,
                        restored_beam,
                        fitted_beam,
                    ) {
                        Ok(rescaled) => rescaled,
                        Err(error) => {
                            plane.warnings.push(format!(
                                "restore-time residual rescaling failed for channel {channel_index}: {error}"
                            ));
                            plane.residual.clone()
                        }
                    }
                }
                _ => plane.residual.clone(),
            };
            let restored_image = if plane.request.clean.niter == 0 {
                residual_to_add
            } else {
                let restored_model = restore_model(
                    &plane.model,
                    plane.request.geometry.cell_size_rad,
                    restored_beam,
                );
                &restored_model + &residual_to_add
            };
            plane.stage_timings.restore += restore_started.elapsed();
            restored_image
        };
        plane.stage_timings.total = plane.stage_timings.total.saturating_add(Duration::ZERO);

        psf.slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.psf_state.psf);
        residual
            .slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.residual);
        model
            .slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.model);
        image
            .slice_mut(s![.., .., 0, channel_index])
            .assign(&restored_image);
        sumwt[(0, 0, 0, channel_index)] = plane.psf_state.reported_sumwt;

        plane.warnings.extend(beam_warnings);
        for warning in &plane.warnings {
            warnings.push(format!("channel {channel_index}: {warning}"));
        }
        add_stage_timings(&mut stage_timings, plane.stage_timings);
        gridded_samples += plane.psf_state.gridded_samples;
        skipped_samples += plane.psf_state.skipped_samples;
        beams.push(beam);
        result_restored_beams.push(restored_beam);
        channel_diagnostics.push(ImagingDiagnostics {
            warnings: plane.warnings,
            gridded_samples: plane.psf_state.gridded_samples,
            skipped_samples: plane.psf_state.skipped_samples,
            normalization_sumwt: plane.psf_state.normalization_sumwt,
            reported_sumwt: plane.psf_state.reported_sumwt,
            psf_peak_normalization: plane.psf_state.psf_peak,
            major_cycles: casa_major_cycle_count(plane.major_cycles, plane.request.clean.niter),
            minor_iterations: plane.minor_iterations,
            clean_stop_reason: plane.clean_stop_reason,
            minor_cycle_traces: plane.minor_cycle_traces,
            initial_residual_peak_jy_per_beam: plane.initial_peak,
            final_residual_peak_jy_per_beam: peak_abs_value_masked(
                &plane.residual,
                plane.request.clean_mask.as_ref(),
            ),
            max_abs_w_lambda: plane
                .request
                .visibility_batches
                .iter()
                .flat_map(|batch| batch.w_lambda.iter())
                .fold(0.0f64, |max_value, value| max_value.max(value.abs())),
            fractional_bandwidth: 0.0,
            max_psf_sidelobe_level: plane.max_psf_sidelobe_level,
            final_cycle_threshold_jy_per_beam: plane.final_cycle_threshold_jy_per_beam,
            clean_mask_pixels: plane
                .request
                .clean_mask
                .as_ref()
                .map(|mask| mask.iter().filter(|value| **value).count())
                .unwrap_or(nx * ny),
            beam_fit_attempts,
            beam_fit_cutoff_used,
            beam_fit_debug,
            mosaic_weight_image: None,
            stage_timings: plane.stage_timings,
        });
    }
    stage_timings.total = total_started.elapsed();

    let channel_frequencies_hz = request
        .channels
        .iter()
        .map(|channel| channel.channel_frequency_hz)
        .collect::<Vec<_>>();
    let reffreq_hz = 0.5
        * (channel_frequencies_hz[0] + channel_frequencies_hz[channel_frequencies_hz.len() - 1]);

    Ok(CubeImagingResult {
        psf,
        residual,
        model,
        image,
        sumwt,
        clean_mask,
        beams,
        restored_beams: result_restored_beams,
        diagnostics: CubeImagingDiagnostics {
            warnings,
            gridded_samples,
            skipped_samples,
            major_cycles: casa_major_cycle_count(cube_major_cycle_blocks, request.clean.niter),
            minor_iterations: total_reported_minor_iterations,
            clean_stop_reason: cube_clean_stop_reason,
            channel_diagnostics,
            stage_timings,
        },
        compatibility: CompatibilityMetadata {
            axis_order: [
                AxisKind::RightAscension,
                AxisKind::Declination,
                AxisKind::Stokes,
                AxisKind::Frequency,
            ],
            plane_stokes: request.plane_stokes,
            reffreq_hz,
            channel_frequencies_hz,
            psf_units: String::new(),
            residual_units: "Jy/beam".to_string(),
            model_units: "Jy/pixel".to_string(),
            image_units: "Jy/beam".to_string(),
        },
    })
}

fn cube_fractional_bandwidth(request: &CubeImagingRequest) -> f64 {
    let Some(first) = request.channels.first() else {
        return 0.0;
    };
    let Some(last) = request.channels.last() else {
        return 0.0;
    };
    fractional_bandwidth_from_frequency_range([
        first.channel_frequency_hz,
        last.channel_frequency_hz,
    ])
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CubeMinorCycleCaptureConfig {
    channel_index: usize,
    block_index: usize,
    directory: PathBuf,
}

fn cube_minor_cycle_capture_config() -> Option<CubeMinorCycleCaptureConfig> {
    let channel_index = std::env::var("CASA_RS_CUBE_CAPTURE_CHANNEL")
        .ok()?
        .parse::<usize>()
        .ok()?;
    let block_index = std::env::var("CASA_RS_CUBE_CAPTURE_BLOCK")
        .ok()?
        .parse::<usize>()
        .ok()?;
    let directory = PathBuf::from(std::env::var_os("CASA_RS_CUBE_CAPTURE_DIR")?);
    Some(CubeMinorCycleCaptureConfig {
        channel_index,
        block_index,
        directory,
    })
}

#[allow(clippy::too_many_arguments)]
fn maybe_capture_cube_minor_cycle_state(
    capture: Option<&CubeMinorCycleCaptureConfig>,
    channel_index: usize,
    block_index: usize,
    cycle_reported_niter: usize,
    request: &ImagingRequest,
    psf: &Array2<f32>,
    residual: &Array2<f32>,
    model: &Array2<f32>,
    model_cube: Option<&[Array2<f32>]>,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
) {
    let Some(capture) = capture else {
        return;
    };
    if capture.channel_index != channel_index || capture.block_index != block_index {
        return;
    }
    if let Err(error) = write_cube_minor_cycle_capture(
        capture,
        cycle_reported_niter,
        request,
        psf,
        residual,
        model,
        model_cube,
        cycle_threshold_jy_per_beam,
        nsigma_threshold_jy_per_beam,
    ) {
        eprintln!(
            "failed to capture cube minor-cycle state for channel={channel_index} block={block_index}: {error}"
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn write_cube_minor_cycle_capture(
    capture: &CubeMinorCycleCaptureConfig,
    cycle_reported_niter: usize,
    request: &ImagingRequest,
    psf: &Array2<f32>,
    residual: &Array2<f32>,
    model: &Array2<f32>,
    model_cube: Option<&[Array2<f32>]>,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
) -> Result<(), std::io::Error> {
    fs::create_dir_all(&capture.directory)?;
    let [nx, ny] = request.geometry.image_shape;
    let channel_count = model_cube.map_or(0, |cube| cube.len());
    let meta = format!(
        concat!(
            "channel_index={}\n",
            "block_index={}\n",
            "nx={}\n",
            "ny={}\n",
            "channel_count={}\n",
            "gain={:.9e}\n",
            "absolute_threshold_jy_per_beam={:.9e}\n",
            "cycle_threshold_jy_per_beam={:.9e}\n",
            "nsigma_threshold_jy_per_beam={:.9e}\n",
            "cycle_reported_niter={}\n"
        ),
        capture.channel_index,
        capture.block_index,
        nx,
        ny,
        channel_count,
        request.clean.gain,
        request.clean.threshold_jy_per_beam,
        cycle_threshold_jy_per_beam,
        nsigma_threshold_jy_per_beam,
        cycle_reported_niter,
    );
    fs::write(capture.directory.join("meta.txt"), meta)?;
    write_capture_plane(&capture.directory.join("psf.txt"), psf)?;
    write_capture_plane(&capture.directory.join("residual.txt"), residual)?;
    write_capture_plane(&capture.directory.join("model.txt"), model)?;
    if let Some(model_cube) = model_cube {
        for (model_channel_index, model_plane) in model_cube.iter().enumerate() {
            write_capture_plane(
                &capture
                    .directory
                    .join(format!("model_channel_{model_channel_index}.txt")),
                model_plane,
            )?;
        }
    }
    Ok(())
}

fn write_capture_plane(path: &std::path::Path, plane: &Array2<f32>) -> Result<(), std::io::Error> {
    let body = plane
        .iter()
        .map(|value| format!("{value:.9e}"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{body}\n"))
}

fn cube_model_dependency_mask(
    nchan: usize,
    identity_model_channel_index: usize,
    model_interpolation_batches: &[CubeModelInterpolationBatch],
) -> Vec<bool> {
    let mut dependencies = vec![false; nchan];
    if model_interpolation_batches.is_empty() {
        if identity_model_channel_index < nchan {
            dependencies[identity_model_channel_index] = true;
        }
        return dependencies;
    }
    for batch in model_interpolation_batches {
        for sample_contributions in &batch.sample_contributions {
            for contribution in sample_contributions {
                if contribution.factor.is_finite()
                    && contribution.factor > 0.0
                    && contribution.model_channel_index < nchan
                {
                    dependencies[contribution.model_channel_index] = true;
                }
            }
        }
    }
    dependencies
}

fn cube_refresh_flags(planes: &[CubePlaneWork], updated_model_channels: &[bool]) -> Vec<bool> {
    planes
        .iter()
        .enumerate()
        .map(|(plane_index, plane)| {
            updated_model_channels
                .get(plane_index)
                .copied()
                .unwrap_or(false)
                || plane
                    .dependent_model_channels
                    .iter()
                    .zip(updated_model_channels.iter())
                    .any(|(depends_on_model, updated)| *depends_on_model && *updated)
        })
        .collect()
}

fn refresh_cube_residuals_exact(
    planes: &mut [CubePlaneWork],
    refresh_flags: &[bool],
) -> Result<usize, ImagingError> {
    let refreshed_planes = refresh_flags.iter().filter(|flag| **flag).count();
    if refreshed_planes == 0 {
        return Ok(0);
    }

    let mut cube_model_timings = ResidualComputationTimings::default();
    let cube_model_grids = build_cube_model_grids(
        &planes[0].gridder,
        planes.iter().map(|plane| &plane.model),
        &mut cube_model_timings,
    );
    let model_channel_frequencies_hz = planes
        .iter()
        .map(|plane| plane.request.reffreq_hz)
        .collect::<Vec<_>>();
    let mut cube_model_fft_accounted = false;
    for (plane_index, (plane, should_refresh)) in planes
        .iter_mut()
        .zip(refresh_flags.iter().copied())
        .enumerate()
    {
        if plane.is_blank || !should_refresh {
            continue;
        }
        let refresh_started = Instant::now();
        let mut residual_timings = ResidualComputationTimings::default();
        if !cube_model_fft_accounted {
            plane.stage_timings.model_fft += cube_model_timings.model_fft;
            cube_model_fft_accounted = true;
        }
        plane.residual = compute_residual_trace_cube_standard_with_model_grids(
            &plane.weighted_batches,
            Some(&plane.residual_sample_plans),
            &plane.model_interpolation_batches,
            &plane.gridder,
            &cube_model_grids,
            plane_index,
            plane.request.reffreq_hz,
            &model_channel_frequencies_hz,
            CubePredictionLambdaMode::OutputChannel,
            &plane.psf_state,
            false,
            &mut residual_timings,
        )?
        .residual_image;
        plane.stage_timings.major_cycle_refresh += refresh_started.elapsed();
        plane.stage_timings.residual_degrid_grid += residual_timings.degrid_grid;
        plane.stage_timings.residual_fft += residual_timings.fft;
        plane.stage_timings.residual_normalize += residual_timings.normalize;
        plane.major_cycles += 1;
        let (refreshed_peak, refreshed_nsigma_threshold_jy_per_beam) = residual_metrics(
            &plane.residual,
            plane.request.clean_mask.as_ref(),
            plane.request.clean,
        );
        plane.cached_peak_residual_jy_per_beam = refreshed_peak;
        plane.cached_nsigma_threshold_jy_per_beam = refreshed_nsigma_threshold_jy_per_beam;
        if plane.request.deconvolver == Deconvolver::Multiscale {
            let scales = effective_multiscale_scales(&plane.request);
            plane.multiscale_state = Some(build_multiscale_state(
                &plane.residual,
                &plane.psf_state.psf,
                &scales,
                plane.request.small_scale_bias,
                plane.request.clean_mask.as_ref(),
            ));
        }
        if plane.request.deconvolver == Deconvolver::Clark {
            update_divergence_state(
                &mut plane.warnings,
                &mut plane.min_residual_peak_jy_per_beam,
                refreshed_peak,
                &mut plane.divergence_warned,
            );
            if let Some(stop_reason) = tolerant_clean_stop_reason(
                refreshed_peak,
                plane.request.clean.threshold_jy_per_beam,
                refreshed_nsigma_threshold_jy_per_beam,
            ) {
                plane.clean_stop_reason = Some(stop_reason);
            }
        }
    }
    Ok(refreshed_planes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::s;
    use num_complex::Complex32;

    fn tiny_cube_request(niter: usize, visibility: Complex32) -> CubeImagingRequest {
        let geometry = ImageGeometry {
            image_shape: [8, 8],
            cell_size_rad: [1.0, 1.0],
        };
        let model_interpolation_batches = if niter > 0 {
            vec![CubeModelInterpolationBatch {
                sample_contributions: vec![vec![CubeModelChannelContribution {
                    model_channel_index: 0,
                    factor: 1.0,
                }]],
            }]
        } else {
            Vec::new()
        };
        CubeImagingRequest {
            geometry,
            channels: vec![CubeChannelRequest {
                channel_frequency_hz: 1.0,
                visibility_batches: vec![VisibilityBatch {
                    u_lambda: vec![0.0],
                    v_lambda: vec![0.0],
                    w_lambda: vec![0.0],
                    weight: vec![1.0],
                    sumwt_factor: vec![1.0],
                    gridable: vec![true],
                    visibility: vec![visibility],
                }],
                density_batches: Vec::new(),
                model_interpolation_batches,
            }],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter,
                major_cycle_limit: None,
                gain: 0.5,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: niter.max(1),
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 1.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        }
    }

    #[test]
    fn hogbom_cube_refreshes_residual_when_iteration_limit_is_reached() {
        let request = tiny_cube_request(1, Complex32::new(1.0, 0.0));
        let clean = run_cube(&request).unwrap();

        assert_eq!(clean.diagnostics.minor_iterations, 1);
        assert_eq!(
            clean.diagnostics.channel_diagnostics[0].minor_iterations, 2,
            "CASA-inclusive Hogbom commits one extra internal component"
        );
        assert!(clean.diagnostics.channel_diagnostics[0].major_cycles >= 2);
        let model_sum = clean.model.iter().copied().sum::<f32>();
        assert!(model_sum > 0.0, "test must exercise a real model update");

        let gridder = StandardGridder::new(request.geometry).unwrap();
        let model_plane = clean.model.slice(s![.., .., 0, 0]).to_owned();
        let mut residual_timings = ResidualComputationTimings::default();
        let model_grids = build_cube_model_grids(&gridder, [&model_plane], &mut residual_timings);
        let planned_batches =
            build_standard_residual_sample_plans(&gridder, &request.channels[0].visibility_batches);
        let channel_diagnostics = &clean.diagnostics.channel_diagnostics[0];
        let psf_state = PsfState {
            psf: clean.psf.slice(s![.., .., 0, 0]).to_owned(),
            normalization_sumwt: channel_diagnostics.normalization_sumwt,
            reported_sumwt: channel_diagnostics.reported_sumwt,
            psf_peak: channel_diagnostics.psf_peak_normalization,
            gridded_samples: channel_diagnostics.gridded_samples,
            skipped_samples: channel_diagnostics.skipped_samples,
        };
        let exact_refreshed_residual = compute_residual_trace_cube_standard_with_model_grids(
            &request.channels[0].visibility_batches,
            Some(&planned_batches),
            &request.channels[0].model_interpolation_batches,
            &gridder,
            &model_grids,
            0,
            request.channels[0].channel_frequency_hz,
            &[request.channels[0].channel_frequency_hz],
            CubePredictionLambdaMode::OutputChannel,
            &psf_state,
            false,
            &mut residual_timings,
        )
        .unwrap()
        .residual_image;
        let max_exact_error = clean
            .residual
            .slice(s![.., .., 0, 0])
            .iter()
            .zip(exact_refreshed_residual.iter())
            .map(|(actual, exact)| (actual - exact).abs())
            .fold(0.0f32, f32::max);
        let max_image_residual_error = clean
            .image
            .slice(s![.., .., 0, 0])
            .iter()
            .zip(exact_refreshed_residual.iter())
            .map(|(actual, exact)| (actual - exact).abs())
            .fold(0.0f32, f32::max);
        let max_model_value = clean
            .model
            .slice(s![.., .., 0, 0])
            .iter()
            .copied()
            .fold(0.0f32, f32::max);
        let max_stale_error = clean
            .residual
            .slice(s![.., .., 0, 0])
            .iter()
            .zip(clean.model.slice(s![.., .., 0, 0]).iter())
            .zip(exact_refreshed_residual.iter())
            .map(|((residual, model), exact)| (residual + model - exact).abs())
            .fold(0.0f32, f32::max);

        assert!(
            max_exact_error < 1.0e-6,
            "cube CLEAN must return the exact final residual after the iteration-limit stop"
        );
        assert!(
            max_model_value > 1.0e-3
                && (max_image_residual_error > 1.0e-3 || max_stale_error > 1.0e-3),
            "regression guard should fail if the exact refresh is not distinguishable"
        );
    }

    #[test]
    fn hogbom_cube_treats_empty_model_interpolation_as_identity() {
        let mut request = tiny_cube_request(1, Complex32::new(1.0, 0.0));
        request.channels[0].model_interpolation_batches.clear();

        let clean = run_cube(&request).unwrap();

        assert_eq!(clean.diagnostics.minor_iterations, 1);
        assert!(clean.model.iter().copied().sum::<f32>() > 0.0);
        assert!(clean.diagnostics.channel_diagnostics[0].major_cycles >= 2);
    }
}
