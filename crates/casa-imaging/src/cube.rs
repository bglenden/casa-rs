// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{fs, path::PathBuf};

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
            clean_mask: request.clean_mask.clone(),
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

fn blank_plane_diagnostics(
    request: &ImagingRequest,
    warning: String,
    clean_stop_reason: Option<CleanStopReason>,
) -> ImagingDiagnostics {
    ImagingDiagnostics {
        warnings: vec![warning],
        gridded_samples: 0,
        skipped_samples: 0,
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
    for channel in &request.channels {
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
            clean_mask: request.clean_mask.clone(),
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
        let residual_sample_plans =
            build_standard_residual_sample_plans(&gridder, &weighted_batches);
        let (
            psf_state,
            model,
            residual,
            multiscale_state,
            initial_peak,
            warnings,
            is_blank,
        ) =
            match compute_psf(
                &plane_request,
                &weighted_batches,
                &gridder,
                &mut plane_stage_timings,
            ) {
                Ok(psf_state) => {
                    let model = Array2::<f32>::zeros((nx, ny));
                    let residual = compute_residual(
                        &plane_request,
                        &weighted_batches,
                        &gridder,
                        &model,
                        &psf_state,
                        &mut plane_stage_timings,
                    )?;
                    let multiscale_state =
                        matches!(plane_request.deconvolver, Deconvolver::Multiscale).then(|| {
                            let scales = effective_multiscale_scales(&plane_request);
                            build_multiscale_state(
                                &residual,
                                &psf_state.psf,
                                &scales,
                                plane_request.small_scale_bias,
                            )
                        });
                    let initial_peak =
                        peak_abs_value_masked(&residual, plane_request.clean_mask.as_ref());
                    (
                        psf_state,
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
            request: plane_request,
            weighted_batches,
            residual_sample_plans,
            model_interpolation_batches: channel.model_interpolation_batches.clone(),
            dependent_model_channels: cube_model_dependency_mask(
                nchan,
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
    let cube_minor_cycle_capture = cube_minor_cycle_capture_config();
    let cube_clean_started = Instant::now();
    let cube_max_psf_sidelobe_level = planes
        .iter()
        .map(|plane| plane.max_psf_sidelobe_level)
        .fold(0.0f32, f32::max);
    while total_reported_minor_iterations < request.clean.niter {
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
        let mut refresh_flags = vec![false; planes.len()];
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
        let refreshed_planes = refresh_flags.iter().filter(|flag| **flag).count();
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
        for (plane, should_refresh) in planes.iter_mut().zip(refresh_flags.iter().copied()) {
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
            let restored_model = restore_model(
                &plane.model,
                plane.request.geometry.cell_size_rad,
                restored_beam,
            );
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
            plane.stage_timings.restore += restore_started.elapsed();
            &restored_model + &residual_to_add
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
    model_interpolation_batches: &[CubeModelInterpolationBatch],
) -> Vec<bool> {
    let mut dependencies = vec![false; nchan];
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
