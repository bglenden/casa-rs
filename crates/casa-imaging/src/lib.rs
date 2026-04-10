// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Pure imaging kernels and CLEAN orchestration for CASA-compatible imaging.
//!
//! This crate is the reusable imaging boundary for the first Rust imager wave.
//! The public contract is a documented [`ImagingRequest`] to [`ImagingResult`]
//! transformation with explicit axis order, units, normalization, and
//! unsupported-mode errors.
//!
//! Persistence and MeasurementSet concerns stay out of this crate. Adapters are
//! expected to:
//!
//! - select rows and channels from a backend such as a MeasurementSet
//! - resolve column-level storage into columnar visibility batches
//! - call this crate with scalar batches or strict paired parallel hands
//! - persist the resulting CASA-style products elsewhere
//!
//! The current implementation intentionally stages compatibility:
//!
//! - concrete prolate-spheroidal 7x7 gridder
//! - concrete FFT path
//! - natural, uniform, and Briggs weighting
//! - strict Stokes-I collapse for paired parallel hands
//! - staged Hogbom major/minor-cycle CLEAN with explicit stop reasons
//! - PSF-cutoff beam fitting with interpolation and retry semantics

mod beam;
mod error;
mod fft;
mod gridder;
mod types;
mod weighting;

use std::time::{Duration, Instant};

use casa_images::ImageBeamSet;
use casa_lattices::array_madfm;
use libm::erfc;
use ndarray::{Array2, Array4, s};
use num_complex::Complex32;

use beam::{
    BeamFitOutcome, beamfit_to_gaussian, estimate_psf_sidelobe_level, fit_beam_from_psf,
    gaussian_to_beamfit, rescale_residual_to_restored_beam, restore_model,
};
use fft::{centered_fft2, centered_ifft2};
use gridder::StandardGridder;
use weighting::{apply_weighting, apply_weighting_with_density_source};

pub use error::ImagingError;
pub use types::{
    AxisKind, BeamFit, BeamFitDebugSummary, CleanConfig, CleanStopReason, CompatibilityMetadata,
    CompatibilityMode, CubeChannelRequest, CubeImagingDiagnostics, CubeImagingRequest,
    CubeImagingResult, Deconvolver, GaussianUvTaper, ImageGeometry, ImagingDiagnostics,
    ImagingRequest, ImagingResult, ImagingStageTimings, MinorCycleTrace, ParallelHandBatch,
    PlaneStokes, PsfBeamFitResult, RestoringBeamMode, UvTaperSize, VisibilityBatch, WTermMode,
    WeightDensityMode, WeightingMode,
};

/// Fit a CASA-style restoring beam directly from a PSF image plane.
///
/// This exposes the same beam-fit path used internally by [`run_imaging`] and
/// [`run_cube`], following the `StokesImageUtil::FitGaussianPSF` / `psfcutoff`
/// workflow used by CASA.
pub fn fit_restoring_beam_from_psf(
    psf: &Array2<f32>,
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> PsfBeamFitResult {
    let outcome = fit_beam_from_psf(psf, cell_size_rad, cutoff);
    PsfBeamFitResult {
        beam: outcome.beam,
        warnings: outcome.warnings,
        attempts: outcome.attempts,
        cutoff_used: outcome.cutoff_used,
        debug: outcome.debug,
    }
}

/// Estimate the maximum absolute PSF sidelobe level outside the fitted main lobe.
///
/// This exposes the same sidelobe-estimation path used internally by
/// [`run_imaging`] and [`run_cube`], following CASA's
/// `SIImageStore::getPSFSidelobeLevel` beam-subtraction workflow.
pub fn estimate_psf_sidelobe_from_psf(
    psf: &Array2<f32>,
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> f32 {
    estimate_psf_sidelobe_level(psf, cell_size_rad, cutoff)
}

/// Run the concrete CASA-style MFS imaging pipeline for the supplied request.
pub fn run_imaging(request: &ImagingRequest) -> Result<ImagingResult, ImagingError> {
    let total_started = Instant::now();
    request.validate()?;
    if request.compatibility != CompatibilityMode::CasaStandardMfs {
        return Err(ImagingError::Unsupported(
            "only CASA standard MFS compatibility mode is implemented".to_string(),
        ));
    }

    let gridder = StandardGridder::new(request.geometry)?;
    let weighted_batches = apply_weighting(request, &gridder)?;
    let mut stage_timings = ImagingStageTimings::default();
    let psf_state = compute_psf(request, &weighted_batches, &gridder, &mut stage_timings)?;
    let [nx, ny] = request.geometry.image_shape;
    let mut model = Array2::<f32>::zeros((nx, ny));
    let mut residual = compute_residual(
        request,
        &weighted_batches,
        &gridder,
        &model,
        &psf_state,
        &mut stage_timings,
    )?;
    let max_psf_sidelobe_level = estimate_psf_sidelobe_level(
        &psf_state.psf,
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    let clean_mask_pixels = request
        .clean_mask
        .as_ref()
        .map(|mask| mask.iter().filter(|value| **value).count())
        .unwrap_or(nx * ny);
    let initial_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
    let mut warnings = Vec::new();

    let controller_started = Instant::now();
    let clean_state = run_cotton_schwab_controller(
        request,
        &weighted_batches,
        &gridder,
        &psf_state,
        &mut stage_timings,
        &mut model,
        residual,
        max_psf_sidelobe_level,
        initial_peak,
        &mut warnings,
    )?;
    let controller_elapsed = controller_started.elapsed();
    let accounted = stage_timings
        .minor_cycle_solve
        .saturating_add(stage_timings.major_cycle_refresh);
    stage_timings.controller_overhead += controller_elapsed.saturating_sub(accounted);
    residual = clean_state.residual;

    let beam_fit_started = Instant::now();
    let BeamFitOutcome {
        beam,
        warnings: beam_warnings,
        attempts: beam_fit_attempts,
        cutoff_used: beam_fit_cutoff_used,
        debug: beam_fit_debug,
    } = fit_beam_from_psf(
        &psf_state.psf,
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    stage_timings.beam_fit += beam_fit_started.elapsed();
    let restore_started = Instant::now();
    let restored_model = restore_model(&model, request.geometry.cell_size_rad, beam);
    stage_timings.restore += restore_started.elapsed();
    let restored_image = &restored_model + &residual;

    let max_abs_w_lambda = request
        .visibility_batches
        .iter()
        .flat_map(|batch| batch.w_lambda.iter())
        .fold(0.0f64, |max_value, value| max_value.max(value.abs()));
    let fractional_bandwidth = (request.selected_frequency_range_hz[1]
        - request.selected_frequency_range_hz[0])
        / request.reffreq_hz;
    if fractional_bandwidth > 0.1 {
        warnings.push(format!(
            "fractional bandwidth {:.3} exceeds the narrow-band nterms=1 comfort zone",
            fractional_bandwidth
        ));
    }
    let w_phase_metric = max_abs_w_lambda * request.geometry.field_of_view_rad().powi(2);
    if w_phase_metric > 0.1 {
        warnings.push(format!(
            "max |w| * fov^2 = {:.3} suggests 2-D standard imaging may show non-coplanar artifacts",
            w_phase_metric
        ));
    }
    warnings.extend(beam_warnings);
    stage_timings.total = total_started.elapsed();

    Ok(ImagingResult {
        psf: expand_plane(&psf_state.psf),
        residual: expand_plane(&residual),
        model: expand_plane(&model),
        image: expand_plane(&restored_image),
        sumwt: expand_scalar(psf_state.reported_sumwt),
        beam,
        diagnostics: ImagingDiagnostics {
            warnings,
            gridded_samples: psf_state.gridded_samples,
            skipped_samples: psf_state.skipped_samples,
            major_cycles: casa_major_cycle_count(clean_state.major_cycles, request.clean.niter),
            minor_iterations: clean_state.minor_iterations,
            clean_stop_reason: clean_state.clean_stop_reason,
            minor_cycle_traces: clean_state.minor_cycle_traces,
            initial_residual_peak_jy_per_beam: initial_peak,
            final_residual_peak_jy_per_beam: peak_abs_value_masked(
                &residual,
                request.clean_mask.as_ref(),
            ),
            max_abs_w_lambda,
            fractional_bandwidth,
            max_psf_sidelobe_level,
            final_cycle_threshold_jy_per_beam: clean_state.final_cycle_threshold_jy_per_beam,
            clean_mask_pixels,
            beam_fit_attempts,
            beam_fit_cutoff_used,
            beam_fit_debug,
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
            reffreq_hz: request.reffreq_hz,
            channel_frequencies_hz: vec![request.reffreq_hz],
            psf_units: String::new(),
            residual_units: "Jy/beam".to_string(),
            model_units: "Jy/pixel".to_string(),
            image_units: "Jy/beam".to_string(),
        },
    })
}

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
    if request.clean.niter > 0 {
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
    min_residual_peak_jy_per_beam: f32,
    divergence_warned: bool,
    is_blank: bool,
}

#[derive(Debug, Clone, Copy)]
struct HogbomMinorCycleOutcome {
    updated_model: bool,
    actual_updates: usize,
    reported_updates: usize,
    stop_reason: Option<CleanStopReason>,
    final_cycle_threshold_jy_per_beam: f32,
    final_nsigma_threshold_jy_per_beam: f32,
}

#[derive(Debug, Clone, Copy, Default)]
struct MinorCycleProbe {
    initial_scale_pixels: Option<f32>,
    initial_candidate_strength_jy_per_beam: Option<f32>,
    initial_candidate_position: Option<[usize; 2]>,
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
    let mut planes = Vec::with_capacity(nchan);
    for channel in &request.channels {
        let plane_request = ImagingRequest {
            geometry: request.geometry,
            visibility_batches: channel.visibility_batches.clone(),
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
            compatibility: request.compatibility,
        };
        plane_request.validate()?;
        let gridder = StandardGridder::new(plane_request.geometry)?;
        let mut plane_stage_timings = ImagingStageTimings::default();
        let density_batches = match request.weight_density_mode {
            WeightDensityMode::Combined => combined_density_batches
                .as_deref()
                .expect("combined cube density batches prepared"),
            WeightDensityMode::PerPlane => &plane_request.visibility_batches,
        };
        let weighted_batches = apply_weighting_with_density_source(
            plane_request.weighting,
            request.weight_density_mode,
            request.uv_taper,
            &plane_request.visibility_batches,
            density_batches,
            &gridder,
        )?;
        let (psf_state, model, residual, multiscale_state, initial_peak, warnings, is_blank) =
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
            request: plane_request,
            weighted_batches,
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
    let cube_max_psf_sidelobe_level = planes
        .iter()
        .map(|plane| plane.max_psf_sidelobe_level)
        .fold(0.0f32, f32::max);
    while total_reported_minor_iterations < request.clean.niter {
        let global_peak = planes
            .iter()
            .filter_map(|plane| {
                peak_location_masked(&plane.residual, plane.request.clean_mask.as_ref())
                    .map(|(_, value)| value.abs())
            })
            .fold(0.0f32, f32::max);
        let cube_nsigma_threshold_jy_per_beam = global_nsigma_threshold_jy_per_beam(
            &planes
                .iter()
                .filter(|plane| !plane.is_blank)
                .map(|plane| (&plane.residual, plane.request.clean_mask.as_ref()))
                .collect::<Vec<_>>(),
            request.clean,
        );
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            global_peak,
            request.clean.threshold_jy_per_beam,
            cube_nsigma_threshold_jy_per_beam,
        ) {
            for plane in &mut planes {
                plane.clean_stop_reason.get_or_insert(stop_reason);
            }
            break;
        }

        let cube_cycle_threshold_jy_per_beam =
            compute_cycle_threshold(global_peak, cube_max_psf_sidelobe_level, request.clean);
        let mut any_model_update = false;
        let mut refresh_flags = vec![false; planes.len()];
        for (plane_index, plane) in planes.iter_mut().enumerate() {
            if plane.is_blank {
                continue;
            }
            let cycle_reported_niter = request.clean.minor_cycle_length;
            let start_reported_iteration = total_reported_minor_iterations;
            let plane_nsigma_threshold_jy_per_beam = nsigma_threshold_jy_per_beam(
                &plane.residual,
                plane.request.clean_mask.as_ref(),
                plane.request.clean,
            );
            let start_peak_residual_jy_per_beam =
                peak_abs_value_masked(&plane.residual, plane.request.clean_mask.as_ref());
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
            refresh_flags[plane_index] = true;
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
            for plane in &mut planes {
                plane
                    .clean_stop_reason
                    .get_or_insert(CleanStopReason::NoCleanablePixels);
            }
            break;
        }
        cube_major_cycle_blocks += 1;
        if total_reported_minor_iterations >= request.clean.niter {
            for plane in &mut planes {
                plane
                    .clean_stop_reason
                    .get_or_insert(CleanStopReason::IterationLimitReached);
            }
            break;
        }
        for (plane, should_refresh) in planes.iter_mut().zip(refresh_flags) {
            if plane.is_blank || !should_refresh {
                continue;
            }
            let refresh_started = Instant::now();
            plane.residual = compute_residual(
                &plane.request,
                &plane.weighted_batches,
                &plane.gridder,
                &plane.model,
                &plane.psf_state,
                &mut plane.stage_timings,
            )?;
            plane.stage_timings.major_cycle_refresh += refresh_started.elapsed();
            plane.major_cycles += 1;
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
                let refreshed_peak =
                    peak_abs_value_masked(&plane.residual, plane.request.clean_mask.as_ref());
                update_divergence_state(
                    &mut plane.warnings,
                    &mut plane.min_residual_peak_jy_per_beam,
                    refreshed_peak,
                    &mut plane.divergence_warned,
                );
                let refreshed_nsigma_threshold_jy_per_beam = nsigma_threshold_jy_per_beam(
                    &plane.residual,
                    plane.request.clean_mask.as_ref(),
                    plane.request.clean,
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
            .filter_map(|plane| {
                peak_location_masked(&plane.residual, plane.request.clean_mask.as_ref())
                    .map(|(_, value)| value.abs())
            })
            .fold(0.0f32, f32::max);
        let cube_nsigma_threshold_after_refresh_jy_per_beam = global_nsigma_threshold_jy_per_beam(
            &planes
                .iter()
                .filter(|plane| !plane.is_blank)
                .map(|plane| (&plane.residual, plane.request.clean_mask.as_ref()))
                .collect::<Vec<_>>(),
            request.clean,
        );
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            global_peak_after_refresh,
            request.clean.threshold_jy_per_beam,
            cube_nsigma_threshold_after_refresh_jy_per_beam,
        ) {
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

struct CottonSchwabState {
    residual: Array2<f32>,
    major_cycles: usize,
    minor_iterations: usize,
    clean_stop_reason: Option<CleanStopReason>,
    minor_cycle_traces: Vec<MinorCycleTrace>,
    final_cycle_threshold_jy_per_beam: f32,
}

fn image_center_value(image: &Array2<f32>) -> f32 {
    let center = (image.dim().0 / 2, image.dim().1 / 2);
    image[center]
}

fn make_minor_cycle_trace(
    cycle_index: usize,
    start_reported_iteration: usize,
    outcome: HogbomMinorCycleOutcome,
    start_peak_residual_jy_per_beam: f32,
    residual: &Array2<f32>,
    model: &Array2<f32>,
    probe: MinorCycleProbe,
) -> MinorCycleTrace {
    MinorCycleTrace {
        cycle_index,
        start_reported_iteration,
        reported_updates: outcome.reported_updates,
        actual_updates: outcome.actual_updates,
        start_peak_residual_jy_per_beam,
        end_peak_residual_jy_per_beam: peak_abs_value(residual),
        cycle_threshold_jy_per_beam: outcome.final_cycle_threshold_jy_per_beam,
        nsigma_threshold_jy_per_beam: outcome.final_nsigma_threshold_jy_per_beam,
        clean_stop_reason: outcome.stop_reason,
        initial_scale_pixels: probe.initial_scale_pixels,
        initial_candidate_strength_jy_per_beam: probe.initial_candidate_strength_jy_per_beam,
        initial_candidate_position: probe.initial_candidate_position,
        center_model_value_jy_per_pixel: image_center_value(model),
        center_residual_value_jy_per_beam: image_center_value(residual),
    }
}

fn casa_multiscale_reported_updates(
    actual_updates: usize,
    cycle_reported_niter: usize,
    stop_reason: Option<CleanStopReason>,
    updated_model: bool,
) -> usize {
    actual_updates
        + usize::from(
            updated_model && actual_updates < cycle_reported_niter && stop_reason.is_some(),
        )
}

#[allow(clippy::too_many_arguments)]
fn run_cotton_schwab_controller(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    model: &mut Array2<f32>,
    residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    match request.deconvolver {
        Deconvolver::Hogbom => run_hogbom_cotton_schwab(
            request,
            weighted_batches,
            gridder,
            psf_state,
            stage_timings,
            model,
            residual,
            max_psf_sidelobe_level,
            initial_peak,
            warnings,
        ),
        Deconvolver::Clark => run_clark_cotton_schwab(
            request,
            weighted_batches,
            gridder,
            psf_state,
            stage_timings,
            model,
            residual,
            max_psf_sidelobe_level,
            initial_peak,
            warnings,
        ),
        Deconvolver::Multiscale => run_multiscale_cotton_schwab(
            request,
            weighted_batches,
            gridder,
            psf_state,
            stage_timings,
            model,
            residual,
            max_psf_sidelobe_level,
            initial_peak,
            warnings,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_hogbom_cotton_schwab(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    model: &mut Array2<f32>,
    mut residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    let mut minor_iterations = 0usize;
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;

    while reported_minor_iterations < request.clean.niter {
        let Some((_, cycle_peak_value)) =
            peak_location_masked(&residual, request.clean_mask.as_ref())
        else {
            clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let cycle_peak = cycle_peak_value.abs();
        let cycle_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            cycle_peak,
            request.clean.threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
        ) {
            clean_stop_reason = Some(stop_reason);
            break;
        }
        let remaining_reported = request.clean.niter - reported_minor_iterations;
        let cycle_reported_niter = remaining_reported.min(request.clean.minor_cycle_length);
        let start_reported_iteration = reported_minor_iterations;
        let cycle_threshold_jy_per_beam =
            compute_cycle_threshold(cycle_peak, max_psf_sidelobe_level, request.clean);
        let outcome = run_hogbom_minor_cycle(
            request,
            psf_state,
            model,
            &mut residual,
            cycle_reported_niter,
            cycle_threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
            stage_timings,
        );
        minor_cycle_traces.push(make_minor_cycle_trace(
            minor_cycle_traces.len(),
            start_reported_iteration,
            outcome,
            cycle_peak,
            &residual,
            model,
            MinorCycleProbe::default(),
        ));
        minor_iterations += outcome.actual_updates;
        reported_minor_iterations += outcome.reported_updates;
        final_cycle_threshold_jy_per_beam = outcome.final_cycle_threshold_jy_per_beam;
        if let Some(reason) = outcome.stop_reason {
            clean_stop_reason = Some(reason);
        }
        if !outcome.updated_model {
            break;
        }
        let minor_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        update_divergence_state(
            warnings,
            &mut min_residual_peak_jy_per_beam,
            minor_peak,
            &mut divergence_warned,
        );
        if clean_stop_reason.is_none() && reported_minor_iterations >= request.clean.niter {
            clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
            break;
        }
        let refresh_started = Instant::now();
        residual = compute_residual(
            request,
            weighted_batches,
            gridder,
            model,
            psf_state,
            stage_timings,
        )?;
        stage_timings.major_cycle_refresh += refresh_started.elapsed();
        major_cycles += 1;
        let refreshed_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        let refreshed_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            refreshed_peak,
            request.clean.threshold_jy_per_beam,
            refreshed_nsigma_threshold_jy_per_beam,
        ) {
            clean_stop_reason = Some(stop_reason);
            break;
        }
    }
    if request.clean.niter > 0 && clean_stop_reason.is_none() {
        clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
    }

    Ok(CottonSchwabState {
        residual,
        major_cycles,
        minor_iterations,
        clean_stop_reason,
        minor_cycle_traces,
        final_cycle_threshold_jy_per_beam,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_hogbom_minor_cycle(
    request: &ImagingRequest,
    psf_state: &PsfState,
    model: &mut Array2<f32>,
    residual: &mut Array2<f32>,
    cycle_reported_niter: usize,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
    stage_timings: &mut ImagingStageTimings,
) -> HogbomMinorCycleOutcome {
    let cycle_component_budget =
        casa_hogbom_component_budget(cycle_reported_niter, request.compatibility);
    let mut cycle_component_updates = 0usize;
    let mut updated_model = false;
    let mut stop_reason = None;
    let minor_started = Instant::now();
    while cycle_component_updates < cycle_component_budget {
        let Some(((peak_x, peak_y), peak_value)) =
            peak_location_masked(residual, request.clean_mask.as_ref())
        else {
            stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let peak_abs = peak_value.abs();
        if let Some(reason) = minor_cycle_stop_reason(
            peak_abs,
            request.clean.threshold_jy_per_beam,
            cycle_threshold_jy_per_beam,
            nsigma_threshold_jy_per_beam,
        ) {
            stop_reason = Some(reason);
            break;
        }
        let component = request.clean.gain * peak_value;
        model[(peak_x, peak_y)] += component;
        subtract_shifted_psf(residual, &psf_state.psf, (peak_x, peak_y), component);
        cycle_component_updates += 1;
        updated_model = true;
    }
    let minor_elapsed = minor_started.elapsed();
    stage_timings.minor_cycle += minor_elapsed;
    stage_timings.minor_cycle_solve += minor_elapsed;
    HogbomMinorCycleOutcome {
        updated_model,
        actual_updates: cycle_component_updates,
        reported_updates: cycle_component_updates.min(cycle_reported_niter),
        stop_reason,
        final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
        final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_clark_minor_cycle(
    request: &ImagingRequest,
    psf: &Array2<f32>,
    model: &mut Array2<f32>,
    residual: &mut Array2<f32>,
    cycle_reported_niter: usize,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
    psf_patch: &ClarkPsfPatch,
    stage_timings: &mut ImagingStageTimings,
) -> HogbomMinorCycleOutcome {
    let base_residual = residual.clone();
    let mut working_residual = residual.clone();
    let mut max_res_previous =
        peak_abs_value_masked(&working_residual, request.clean_mask.as_ref());
    let mut cycle_component_updates = 0usize;
    let mut updated_model = false;
    let mut stop_reason = None;
    let mut factor = 1.0f32 / 3.0f32;
    let mut max_minor_iterations_this_cycle = cycle_reported_niter;
    let mut num_major_cycles = 0usize;
    let mut delta_components = Vec::<((usize, usize), f32)>::new();
    let minor_started = Instant::now();
    while cycle_component_updates < cycle_reported_niter && num_major_cycles < 10 {
        let Some((_, cycle_peak_value)) =
            peak_location_masked(&working_residual, request.clean_mask.as_ref())
        else {
            stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let cycle_peak_abs = cycle_peak_value.abs();
        if let Some(reason) = minor_cycle_stop_reason(
            cycle_peak_abs,
            request.clean.threshold_jy_per_beam,
            cycle_threshold_jy_per_beam,
            nsigma_threshold_jy_per_beam,
        ) {
            stop_reason = Some(reason);
            break;
        }
        let mut flux_limit = cycle_peak_abs * psf_patch.max_exterior_abs * factor;
        if factor > 1.0 {
            flux_limit = (0.95 * cycle_peak_abs).min(flux_limit);
        }
        let selection_limit = flux_limit.max(cycle_threshold_jy_per_beam);
        let mut active_pixels = collect_clark_active_pixels(
            &working_residual,
            request.clean_mask.as_ref(),
            selection_limit,
        );
        if active_pixels.is_empty() {
            stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        }
        let cycle_start_iterations = cycle_component_updates;
        let remaining_iterations = cycle_reported_niter - cycle_component_updates;
        let cycle_minor_limit = remaining_iterations.min(max_minor_iterations_this_cycle);
        let mut cur_iter = 0usize;
        let mut fmn = 0.0f32;
        let fac = if flux_limit > 0.0 {
            (flux_limit / cycle_peak_abs).powf(-1.0)
        } else {
            0.0
        };
        let mut iter_flux_limit = flux_limit.max(cycle_threshold_jy_per_beam);
        while cur_iter < cycle_minor_limit {
            let Some((_, peak_pixel)) = peak_clark_active_pixel(&active_pixels) else {
                break;
            };
            let peak_abs = peak_pixel.value.abs();
            if let Some(reason) = minor_cycle_stop_reason(
                peak_abs,
                request.clean.threshold_jy_per_beam,
                iter_flux_limit,
                nsigma_threshold_jy_per_beam,
            ) {
                stop_reason = Some(reason);
                break;
            }
            let component = request.clean.gain * peak_pixel.value;
            model[(peak_pixel.x, peak_pixel.y)] += component;
            delta_components.push(((peak_pixel.x, peak_pixel.y), component));
            subtract_clark_component_from_active(
                &mut active_pixels,
                peak_pixel.x,
                peak_pixel.y,
                component,
                psf_patch,
            );
            cycle_component_updates += 1;
            cur_iter += 1;
            updated_model = true;
            fmn += fac / (cycle_start_iterations as f32 + cur_iter as f32);
            iter_flux_limit = (flux_limit * fmn).max(cycle_threshold_jy_per_beam);
        }
        if cur_iter == 0 {
            stop_reason = Some(CleanStopReason::CycleThresholdReached);
            break;
        }
        working_residual.assign(&base_residual);
        for &((peak_x, peak_y), component) in &delta_components {
            subtract_shifted_psf(&mut working_residual, psf, (peak_x, peak_y), component);
        }
        let current_peak = peak_abs_value_masked(&working_residual, request.clean_mask.as_ref());
        if current_peak > max_res_previous {
            factor *= 3.0;
            max_minor_iterations_this_cycle = 10;
        }
        max_res_previous = current_peak;
        num_major_cycles += 1;
    }
    residual.assign(&working_residual);
    if !updated_model && stop_reason.is_none() {
        stop_reason = Some(CleanStopReason::NoCleanablePixels);
    }
    let minor_elapsed = minor_started.elapsed();
    stage_timings.minor_cycle += minor_elapsed;
    stage_timings.minor_cycle_solve += minor_elapsed;
    HogbomMinorCycleOutcome {
        updated_model,
        actual_updates: cycle_component_updates,
        reported_updates: cycle_component_updates,
        stop_reason,
        final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
        final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_multiscale_minor_cycle(
    request: &ImagingRequest,
    psf: &Array2<f32>,
    multiscale_state: &mut MultiscaleState,
    model: &mut Array2<f32>,
    residual: &mut Array2<f32>,
    cycle_reported_niter: usize,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
    stage_timings: &mut ImagingStageTimings,
) -> HogbomMinorCycleOutcome {
    let Some(cycle_candidate) =
        select_multiscale_candidate(multiscale_state, request.clean_mask.as_ref())
    else {
        return HogbomMinorCycleOutcome {
            updated_model: false,
            actual_updates: 0,
            reported_updates: 0,
            stop_reason: Some(CleanStopReason::NoCleanablePixels),
            final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
            final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
        };
    };
    let cycle_peak = cycle_candidate.strength.abs();
    let initial_cycle_peak = cycle_peak;
    if let Some(reason) = minor_cycle_stop_reason(
        cycle_peak,
        request.clean.threshold_jy_per_beam,
        cycle_threshold_jy_per_beam,
        nsigma_threshold_jy_per_beam,
    ) {
        return HogbomMinorCycleOutcome {
            updated_model: false,
            actual_updates: 0,
            reported_updates: 0,
            stop_reason: Some(reason),
            final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
            final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
        };
    }

    let mut cycle_component_updates = 0usize;
    let mut updated_model = false;
    let mut stop_reason = None;
    let mut delta_model = Array2::<f32>::zeros(model.dim());
    let minor_started = Instant::now();
    while cycle_component_updates < cycle_reported_niter {
        let Some(candidate) =
            select_multiscale_candidate(multiscale_state, request.clean_mask.as_ref())
        else {
            stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let peak_abs = candidate.strength.abs();
        if let Some(reason) = minor_cycle_stop_reason(
            peak_abs,
            request.clean.threshold_jy_per_beam,
            cycle_threshold_jy_per_beam,
            nsigma_threshold_jy_per_beam,
        ) {
            stop_reason = Some(reason);
            break;
        }
        if cycle_component_updates > 0 && peak_abs > initial_cycle_peak * 1.5 {
            stop_reason = Some(CleanStopReason::DivergenceDetected);
            break;
        }

        let component = request.clean.gain * candidate.strength;
        add_shifted_kernel(
            model,
            &multiscale_state.scales[candidate.scale_index],
            candidate.position,
            component,
        );
        add_shifted_kernel(
            &mut delta_model,
            &multiscale_state.scales[candidate.scale_index],
            candidate.position,
            component,
        );
        subtract_multiscale_component(
            multiscale_state,
            candidate.scale_index,
            candidate.position,
            component,
        );
        cycle_component_updates += 1;
        updated_model = true;
    }
    if updated_model {
        *residual = &*residual - &fft_convolve_real(psf, &delta_model);
        refresh_multiscale_dirty_conv_scales(multiscale_state, residual);
    }
    let minor_elapsed = minor_started.elapsed();
    stage_timings.minor_cycle += minor_elapsed;
    stage_timings.minor_cycle_solve += minor_elapsed;
    let reported_updates = casa_multiscale_reported_updates(
        cycle_component_updates,
        cycle_reported_niter,
        stop_reason,
        updated_model,
    );
    HogbomMinorCycleOutcome {
        updated_model,
        actual_updates: cycle_component_updates,
        reported_updates,
        stop_reason,
        final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
        final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
    }
}

struct ClarkPsfPatch {
    patch: Array2<f32>,
    radius_x: usize,
    radius_y: usize,
    max_exterior_abs: f32,
}

struct MultiscaleState {
    scales: Vec<Array2<f32>>,
    dirty_conv_scales: Vec<Array2<f32>>,
    psf_conv_scales: Vec<Vec<Array2<f32>>>,
    peak_psf_conv_scales: Vec<f32>,
    scale_bias: Vec<f32>,
}

fn refresh_multiscale_dirty_conv_scales(state: &mut MultiscaleState, residual: &Array2<f32>) {
    state.dirty_conv_scales = state
        .scales
        .iter()
        .map(|scale| fft_convolve_real(residual, scale))
        .collect::<Vec<_>>();
}

#[derive(Debug, Clone, Copy)]
struct ClarkActivePixel {
    x: usize,
    y: usize,
    value: f32,
}

#[allow(clippy::too_many_arguments)]
fn run_clark_cotton_schwab(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    model: &mut Array2<f32>,
    mut residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    let psf_patch = build_clark_psf_patch(
        &psf_state.psf,
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    let mut minor_iterations = 0usize;
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;

    while reported_minor_iterations < request.clean.niter {
        let Some((_, cycle_peak_value)) =
            peak_location_masked(&residual, request.clean_mask.as_ref())
        else {
            clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let cycle_peak = cycle_peak_value.abs();
        let cycle_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            cycle_peak,
            request.clean.threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
        ) {
            clean_stop_reason = Some(stop_reason);
            break;
        }
        let remaining_reported = request.clean.niter - reported_minor_iterations;
        let cycle_reported_niter = remaining_reported.min(request.clean.minor_cycle_length);
        let start_reported_iteration = reported_minor_iterations;
        final_cycle_threshold_jy_per_beam =
            compute_cycle_threshold(cycle_peak, max_psf_sidelobe_level, request.clean);
        let outcome = run_clark_minor_cycle(
            request,
            &psf_state.psf,
            model,
            &mut residual,
            cycle_reported_niter,
            final_cycle_threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
            &psf_patch,
            stage_timings,
        );
        minor_cycle_traces.push(make_minor_cycle_trace(
            minor_cycle_traces.len(),
            start_reported_iteration,
            outcome,
            cycle_peak,
            &residual,
            model,
            MinorCycleProbe::default(),
        ));
        minor_iterations += outcome.actual_updates;
        reported_minor_iterations += outcome.reported_updates;
        if let Some(reason) = outcome.stop_reason {
            clean_stop_reason = Some(reason);
        }
        if !outcome.updated_model {
            break;
        }
        let minor_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        update_divergence_state(
            warnings,
            &mut min_residual_peak_jy_per_beam,
            minor_peak,
            &mut divergence_warned,
        );
        if clean_stop_reason.is_none() && reported_minor_iterations >= request.clean.niter {
            clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
            break;
        }
        let refresh_started = Instant::now();
        residual = compute_residual(
            request,
            weighted_batches,
            gridder,
            model,
            psf_state,
            stage_timings,
        )?;
        stage_timings.major_cycle_refresh += refresh_started.elapsed();
        major_cycles += 1;
        let refreshed_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        let refreshed_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            refreshed_peak,
            request.clean.threshold_jy_per_beam,
            refreshed_nsigma_threshold_jy_per_beam,
        ) {
            clean_stop_reason = Some(stop_reason);
            break;
        }
    }
    if request.clean.niter > 0 && clean_stop_reason.is_none() {
        clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
    }

    Ok(CottonSchwabState {
        residual,
        major_cycles,
        minor_iterations,
        clean_stop_reason,
        minor_cycle_traces,
        final_cycle_threshold_jy_per_beam,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_multiscale_cotton_schwab(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    model: &mut Array2<f32>,
    mut residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    let scales = effective_multiscale_scales(request);
    let mut multiscale_state =
        build_multiscale_state(&residual, &psf_state.psf, &scales, request.small_scale_bias);
    let mut minor_iterations = 0usize;
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;

    while reported_minor_iterations < request.clean.niter {
        let Some(cycle_candidate) =
            select_multiscale_candidate(&multiscale_state, request.clean_mask.as_ref())
        else {
            clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let cycle_peak = cycle_candidate.strength.abs();
        let initial_cycle_peak = cycle_peak;
        let cycle_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            cycle_peak,
            request.clean.threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
        ) {
            clean_stop_reason = Some(stop_reason);
            break;
        }
        let remaining_reported = request.clean.niter - reported_minor_iterations;
        let cycle_reported_niter = remaining_reported.min(request.clean.minor_cycle_length);
        let start_reported_iteration = reported_minor_iterations;
        let probe = MinorCycleProbe {
            initial_scale_pixels: Some(scales[cycle_candidate.scale_index]),
            initial_candidate_strength_jy_per_beam: Some(cycle_candidate.strength),
            initial_candidate_position: Some([
                cycle_candidate.position.0,
                cycle_candidate.position.1,
            ]),
        };
        let mut cycle_component_updates = 0usize;
        let mut updated_model = false;
        final_cycle_threshold_jy_per_beam =
            compute_cycle_threshold(cycle_peak, max_psf_sidelobe_level, request.clean);
        let minor_started = Instant::now();
        while cycle_component_updates < cycle_reported_niter {
            let Some(candidate) =
                select_multiscale_candidate(&multiscale_state, request.clean_mask.as_ref())
            else {
                clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
                break;
            };
            let peak_abs = candidate.strength.abs();
            if let Some(stop_reason) = minor_cycle_stop_reason(
                peak_abs,
                request.clean.threshold_jy_per_beam,
                final_cycle_threshold_jy_per_beam,
                cycle_nsigma_threshold_jy_per_beam,
            ) {
                clean_stop_reason = Some(stop_reason);
                break;
            }
            if cycle_component_updates > 0 && peak_abs > initial_cycle_peak * 1.5 {
                clean_stop_reason = Some(CleanStopReason::DivergenceDetected);
                break;
            }

            let component = request.clean.gain * candidate.strength;
            add_shifted_kernel(
                model,
                &multiscale_state.scales[candidate.scale_index],
                candidate.position,
                component,
            );
            subtract_multiscale_component(
                &mut multiscale_state,
                candidate.scale_index,
                candidate.position,
                component,
            );
            cycle_component_updates += 1;
            minor_iterations += 1;
            updated_model = true;
        }
        let minor_elapsed = minor_started.elapsed();
        stage_timings.minor_cycle += minor_elapsed;
        stage_timings.minor_cycle_solve += minor_elapsed;
        let reported_updates = casa_multiscale_reported_updates(
            cycle_component_updates,
            cycle_reported_niter,
            clean_stop_reason,
            updated_model,
        );
        minor_cycle_traces.push(make_minor_cycle_trace(
            minor_cycle_traces.len(),
            start_reported_iteration,
            HogbomMinorCycleOutcome {
                updated_model,
                actual_updates: cycle_component_updates,
                reported_updates,
                stop_reason: clean_stop_reason,
                final_cycle_threshold_jy_per_beam,
                final_nsigma_threshold_jy_per_beam: cycle_nsigma_threshold_jy_per_beam,
            },
            cycle_peak,
            &multiscale_state.dirty_conv_scales[0],
            model,
            probe,
        ));
        reported_minor_iterations += reported_updates;
        if !updated_model {
            break;
        }
        residual = multiscale_state.dirty_conv_scales[0].clone();
        let minor_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        update_divergence_state(
            warnings,
            &mut min_residual_peak_jy_per_beam,
            minor_peak,
            &mut divergence_warned,
        );
        if clean_stop_reason.is_none() && reported_minor_iterations >= request.clean.niter {
            clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
            break;
        }
        let refresh_started = Instant::now();
        residual = compute_residual(
            request,
            weighted_batches,
            gridder,
            model,
            psf_state,
            stage_timings,
        )?;
        stage_timings.major_cycle_refresh += refresh_started.elapsed();
        multiscale_state =
            build_multiscale_state(&residual, &psf_state.psf, &scales, request.small_scale_bias);
        major_cycles += 1;
        let refreshed_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        let refreshed_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            refreshed_peak,
            request.clean.threshold_jy_per_beam,
            refreshed_nsigma_threshold_jy_per_beam,
        ) {
            clean_stop_reason = Some(stop_reason);
            break;
        }
    }
    if request.clean.niter > 0 && clean_stop_reason.is_none() {
        clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
    }

    Ok(CottonSchwabState {
        residual,
        major_cycles,
        minor_iterations,
        clean_stop_reason,
        minor_cycle_traces,
        final_cycle_threshold_jy_per_beam,
    })
}

#[derive(Debug, Clone, Copy)]
struct MultiscaleCandidate {
    scale_index: usize,
    position: (usize, usize),
    strength: f32,
}

fn effective_multiscale_scales(request: &ImagingRequest) -> Vec<f32> {
    if request.multiscale_scales.is_empty() {
        vec![0.0]
    } else {
        request.multiscale_scales.clone()
    }
}

fn build_multiscale_state(
    residual: &Array2<f32>,
    psf: &Array2<f32>,
    scales: &[f32],
    small_scale_bias: f32,
) -> MultiscaleState {
    let scale_images = scales
        .iter()
        .map(|scale| make_multiscale_kernel(residual.dim(), *scale))
        .collect::<Vec<_>>();
    let dirty_conv_scales = scale_images
        .iter()
        .map(|scale| fft_convolve_real(residual, scale))
        .collect::<Vec<_>>();
    let psf_conv_scales = (0..scale_images.len())
        .map(|scale_index| {
            (0..scale_images.len())
                .map(|other_index| {
                    fft_convolve_real(
                        &fft_convolve_real(psf, &scale_images[scale_index]),
                        &scale_images[other_index],
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let peak_psf_conv_scales = (0..scale_images.len())
        .map(|scale_index| peak_abs_value(&psf_conv_scales[scale_index][scale_index]).max(1.0e-6))
        .collect::<Vec<_>>();
    let max_scale = scales.iter().copied().fold(0.0f32, f32::max);
    let scale_bias = if max_scale > 0.0 && scales.len() > 1 {
        scales
            .iter()
            .map(|scale| 1.0 - small_scale_bias * (*scale / max_scale))
            .collect::<Vec<_>>()
    } else {
        vec![1.0; scales.len()]
    };

    MultiscaleState {
        scales: scale_images,
        dirty_conv_scales,
        psf_conv_scales,
        peak_psf_conv_scales,
        scale_bias,
    }
}

fn select_multiscale_candidate(
    state: &MultiscaleState,
    mask: Option<&Array2<bool>>,
) -> Option<MultiscaleCandidate> {
    let mut best = None::<(MultiscaleCandidate, f32)>;
    for scale_index in 0..state.dirty_conv_scales.len() {
        let Some((position, value)) =
            peak_location_masked(&state.dirty_conv_scales[scale_index], mask)
        else {
            continue;
        };
        let peak_psf = state.peak_psf_conv_scales[scale_index];
        if peak_psf <= 0.0 || value == 0.0 {
            continue;
        }
        let strength = value / peak_psf;
        let score = state.scale_bias[scale_index] * value * strength;
        if best
            .map(|(_, best_score)| score.abs() > best_score.abs())
            .unwrap_or(true)
        {
            best = Some((
                MultiscaleCandidate {
                    scale_index,
                    position,
                    strength,
                },
                score,
            ));
        }
    }
    best.map(|(candidate, _)| candidate)
}

fn subtract_multiscale_component(
    state: &mut MultiscaleState,
    optimum_scale: usize,
    position: (usize, usize),
    scale_factor: f32,
) {
    for scale_index in 0..state.dirty_conv_scales.len() {
        subtract_shifted_kernel(
            &mut state.dirty_conv_scales[scale_index],
            &state.psf_conv_scales[scale_index][optimum_scale],
            position,
            scale_factor,
        );
    }
}

fn make_multiscale_kernel(shape: (usize, usize), scale_size: f32) -> Array2<f32> {
    let (nx, ny) = shape;
    let mut kernel = Array2::<f32>::zeros((nx, ny));
    let refi = nx as f32 / 2.0;
    let refj = ny as f32 / 2.0;
    if scale_size == 0.0 {
        kernel[(refi as usize, refj as usize)] = 1.0;
        return kernel;
    }

    let mini = ((refi - scale_size).floor() as isize).max(0) as usize;
    let maxi = ((refi + scale_size).ceil() as usize).min(nx - 1);
    let minj = ((refj - scale_size).floor() as isize).max(0) as usize;
    let maxj = ((refj + scale_size).ceil() as usize).min(ny - 1);
    let mut volume = 0.0f32;
    for j in minj..=maxj {
        let ypart = ((refj - j as f32) / scale_size).powi(2);
        for i in mini..=maxi {
            let rad2 = ypart + ((refi - i as f32) / scale_size).powi(2);
            if rad2 < 1.0 {
                let rad = if rad2 <= 0.0 { 0.0 } else { rad2.sqrt() };
                let value = (1.0 - rad2) * multiscale_spheroidal(rad);
                kernel[(i, j)] = value;
                volume += value;
            }
        }
    }
    if volume > 0.0 {
        kernel /= volume;
    }
    kernel
}

fn multiscale_spheroidal(nu: f32) -> f32 {
    if nu <= 0.0 {
        return 1.0;
    }
    if nu >= 1.0 {
        return 0.0;
    }

    let (p, q, nuend) = if nu < 0.75 {
        (
            [
                8.203343e-2,
                -3.644705e-1,
                6.278_66e-1,
                -5.335581e-1,
                2.312756e-1,
            ],
            [1.0, 8.212018e-1, 2.078043e-1],
            0.75f32,
        )
    } else {
        (
            [
                4.028559e-3,
                -3.697768e-2,
                1.021332e-1,
                -1.201436e-1,
                6.412774e-2,
            ],
            [1.0, 9.599102e-1, 2.918724e-1],
            1.0f32,
        )
    };
    let delnusq = nu * nu - nuend * nuend;
    let numerator = p
        .iter()
        .rev()
        .fold(0.0f32, |acc, coefficient| acc * delnusq + coefficient);
    let denominator = q
        .iter()
        .rev()
        .fold(0.0f32, |acc, coefficient| acc * delnusq + coefficient);
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn fft_convolve_real(lhs: &Array2<f32>, rhs: &Array2<f32>) -> Array2<f32> {
    let lhs_complex = lhs.mapv(|value| Complex32::new(value, 0.0));
    let rhs_complex = rhs.mapv(|value| Complex32::new(value, 0.0));
    let product = centered_fft2(&lhs_complex) * centered_fft2(&rhs_complex);
    centered_ifft2(&product).mapv(|value| value.re)
}

fn build_clark_psf_patch(
    psf: &Array2<f32>,
    cell_size_rad: [f64; 2],
    psf_cutoff: f32,
) -> ClarkPsfPatch {
    let BeamFitOutcome { beam, .. } = fit_beam_from_psf(psf, cell_size_rad, psf_cutoff);
    let (major_pixels, minor_pixels) = beam
        .map(|beam| {
            (
                (beam.major_fwhm_rad / cell_size_rad[0]).ceil().max(1.0) as usize,
                (beam.minor_fwhm_rad / cell_size_rad[1]).ceil().max(1.0) as usize,
            )
        })
        .unwrap_or((4, 4));
    let ncent = 4usize.max(major_pixels).max(minor_pixels);
    let patch_size_x = 3 * ncent + 1;
    let patch_size_y = 3 * ncent + 1;
    let center_x = psf.dim().0 / 2;
    let center_y = psf.dim().1 / 2;
    let radius_x = patch_size_x / 2;
    let radius_y = patch_size_y / 2;
    let x0 = center_x.saturating_sub(radius_x);
    let y0 = center_y.saturating_sub(radius_y);
    let x1 = (x0 + patch_size_x).min(psf.dim().0);
    let y1 = (y0 + patch_size_y).min(psf.dim().1);
    let patch = psf.slice(s![x0..x1, y0..y1]).to_owned();
    let max_exterior_abs = max_abs_outside_patch(psf, x0, x1, y0, y1);
    ClarkPsfPatch {
        patch,
        radius_x,
        radius_y,
        max_exterior_abs,
    }
}

fn max_abs_outside_patch(psf: &Array2<f32>, x0: usize, x1: usize, y0: usize, y1: usize) -> f32 {
    let mut max_abs = 0.0f32;
    for ((x, y), value) in psf.indexed_iter() {
        if x < x0 || x >= x1 || y < y0 || y >= y1 {
            max_abs = max_abs.max(value.abs());
        }
    }
    max_abs
}

fn collect_clark_active_pixels(
    residual: &Array2<f32>,
    mask: Option<&Array2<bool>>,
    flux_limit: f32,
) -> Vec<ClarkActivePixel> {
    let mut active = Vec::new();
    for ((x, y), value) in residual.indexed_iter() {
        if value.abs() < flux_limit {
            continue;
        }
        if mask.is_some_and(|mask| !mask[(x, y)]) {
            continue;
        }
        active.push(ClarkActivePixel {
            x,
            y,
            value: *value,
        });
    }
    active
}

fn peak_clark_active_pixel(
    active_pixels: &[ClarkActivePixel],
) -> Option<(usize, ClarkActivePixel)> {
    let mut best = None::<(usize, ClarkActivePixel)>;
    for (index, pixel) in active_pixels.iter().copied().enumerate() {
        match best {
            None => best = Some((index, pixel)),
            Some((_, current)) if pixel.value.abs() > current.value.abs() => {
                best = Some((index, pixel))
            }
            _ => {}
        }
    }
    best
}

fn subtract_clark_component_from_active(
    active_pixels: &mut [ClarkActivePixel],
    peak_x: usize,
    peak_y: usize,
    component: f32,
    psf_patch: &ClarkPsfPatch,
) {
    for pixel in active_pixels {
        let Some(patch_x) = pixel
            .x
            .checked_add(psf_patch.radius_x)
            .and_then(|value| value.checked_sub(peak_x))
        else {
            continue;
        };
        let Some(patch_y) = pixel
            .y
            .checked_add(psf_patch.radius_y)
            .and_then(|value| value.checked_sub(peak_y))
        else {
            continue;
        };
        if patch_x >= psf_patch.patch.dim().0 || patch_y >= psf_patch.patch.dim().1 {
            continue;
        }
        pixel.value -= component * psf_patch.patch[(patch_x, patch_y)];
    }
}

fn casa_hogbom_component_budget(
    reported_cycle_niter: usize,
    compatibility: CompatibilityMode,
) -> usize {
    if reported_cycle_niter == 0 {
        return 0;
    }
    match compatibility {
        // CASA's current `SDAlgorithmHogbomClean` path forwards `cycleniter`
        // into the Fortran `hclean` kernel with `siter = 0`, and `hclean`
        // iterates over the inclusive range `siter..niter`.
        CompatibilityMode::CasaStandardMfs => reported_cycle_niter.saturating_add(1),
    }
}

struct PsfState {
    psf: Array2<f32>,
    normalization_sumwt: f32,
    reported_sumwt: f32,
    psf_peak: f32,
    gridded_samples: usize,
    skipped_samples: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct PsfComputationTimings {
    grid: Duration,
    fft: Duration,
    normalize: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct ResidualComputationTimings {
    model_fft: Duration,
    degrid_grid: Duration,
    fft: Duration,
    normalize: Duration,
}

fn select_restored_cube_beams(
    fitted_beams: &[Option<BeamFit>],
    mode: RestoringBeamMode,
) -> Result<Vec<Option<BeamFit>>, ImagingError> {
    match mode {
        RestoringBeamMode::PerPlane => Ok(fitted_beams.to_vec()),
        RestoringBeamMode::Common => {
            let Some(first) = fitted_beams.iter().flatten().next().copied() else {
                return Ok(vec![None; fitted_beams.len()]);
            };
            let mut beam_set =
                ImageBeamSet::with_shape(fitted_beams.len().max(1), 1, beamfit_to_gaussian(first));
            for (channel, beam) in fitted_beams.iter().enumerate() {
                if let Some(beam) = beam {
                    beam_set
                        .set_beam(Some(channel), Some(0), beamfit_to_gaussian(*beam))
                        .map_err(|error| {
                            ImagingError::InvalidRequest(format!(
                                "set common restoring beam input for channel {channel}: {error}"
                            ))
                        })?;
                }
            }
            let common = beam_set.common_beam().map_err(|error| {
                ImagingError::InvalidRequest(format!(
                    "determine common restoring beam across cube planes: {error}"
                ))
            })?;
            Ok(vec![Some(gaussian_to_beamfit(common)); fitted_beams.len()])
        }
    }
}

fn compute_psf(
    request: &ImagingRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    if request.w_term_mode == WTermMode::Direct {
        return compute_psf_direct(request.geometry, batches, stage_timings);
    }
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex32>::zeros((nx, ny));
    let mut normalization_sumwt = 0.0f32;
    let mut reported_sumwt = 0.0f32;
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;
    let mut timings = PsfComputationTimings::default();

    let grid_started = Instant::now();
    for batch in batches {
        for index in 0..batch.len() {
            if !batch.gridable[index] {
                skipped_samples += 1;
                continue;
            }
            let weight = batch.weight[index];
            let sumwt_factor = batch.sumwt_factor[index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0)
            {
                skipped_samples += 1;
                continue;
            }
            let Some(plan) = gridder.plan_sample(batch.u_lambda[index], batch.v_lambda[index])
            else {
                skipped_samples += 1;
                continue;
            };
            let psf_weight = Complex32::new(weight, 0.0);
            gridder.grid_sample_planned(
                &mut psf_grid,
                &plan.positive_x,
                &plan.positive_y,
                psf_weight,
            );
            gridder.grid_sample_planned(
                &mut psf_grid,
                &plan.negative_x,
                &plan.negative_y,
                psf_weight,
            );
            normalization_sumwt += 2.0 * weight;
            reported_sumwt += weight * sumwt_factor;
            gridded_samples += 1;
        }
    }
    timings.grid = grid_started.elapsed();

    if normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_psf = centered_ifft2(&psf_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid(&raw_psf);
    psf.mapv_inplace(|value| value / normalization_sumwt);
    let psf_peak = peak_abs_value(&psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "PSF peak is non-finite or zero".to_string(),
        ));
    }
    psf.mapv_inplace(|value| value / psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.psf_grid += timings.grid;
    stage_timings.psf_fft += timings.fft;
    stage_timings.psf_normalize += timings.normalize;

    Ok(PsfState {
        psf,
        normalization_sumwt,
        reported_sumwt,
        psf_peak,
        gridded_samples,
        skipped_samples,
    })
}

fn compute_residual(
    request: &ImagingRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    if request.w_term_mode == WTermMode::Direct {
        return compute_residual_direct(request.geometry, batches, model, psf_state, stage_timings);
    }
    let [nx, ny] = gridder.grid_shape();
    let mut residual_grid = Array2::<Complex32>::zeros((nx, ny));
    let mut timings = ResidualComputationTimings::default();
    let model_grid = if model.iter().any(|value| value.abs() > 0.0) {
        let model_fft_started = Instant::now();
        let transformed = centered_fft2(&gridder.apodize_model(model));
        timings.model_fft = model_fft_started.elapsed();
        Some(transformed)
    } else {
        None
    };

    let degrid_grid_started = Instant::now();
    for batch in batches {
        for index in 0..batch.len() {
            if !batch.gridable[index] {
                continue;
            }
            let weight = batch.weight[index];
            let sample = batch.visibility[index];
            if !(weight.is_finite()
                && weight > 0.0
                && sample.re.is_finite()
                && sample.im.is_finite())
            {
                continue;
            }
            let Some(plan) = gridder.plan_sample(batch.u_lambda[index], batch.v_lambda[index])
            else {
                continue;
            };
            let predicted = model_grid.as_ref().map_or_else(
                || Complex32::new(0.0, 0.0),
                |grid| gridder.degrid_sample_planned(grid, &plan.positive_x, &plan.positive_y),
            );
            let residual = (sample - predicted) * weight;
            gridder.grid_sample_planned(
                &mut residual_grid,
                &plan.positive_x,
                &plan.positive_y,
                residual,
            );
            gridder.grid_sample_planned(
                &mut residual_grid,
                &plan.negative_x,
                &plan.negative_y,
                residual.conj(),
            );
        }
    }
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    Ok(image)
}

#[derive(Debug, Clone, Copy)]
struct DirectPixelCoordinate {
    l: f64,
    m: f64,
    n_minus_one: f64,
}

#[derive(Debug, Clone, Copy)]
struct DirectComponent {
    value: f32,
    l: f64,
    m: f64,
    n_minus_one: f64,
}

fn compute_psf_direct(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    let [nx, ny] = geometry.image_shape;
    let pixels = build_direct_pixel_coordinates(geometry);
    let mut psf = Array2::<f32>::zeros((nx, ny));
    let mut normalization_sumwt = 0.0f32;
    let mut reported_sumwt = 0.0f32;
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;

    let accumulate_started = Instant::now();
    for batch in batches {
        for index in 0..batch.len() {
            if !batch.gridable[index] {
                skipped_samples += 1;
                continue;
            }
            let weight = batch.weight[index];
            let sumwt_factor = batch.sumwt_factor[index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0)
            {
                skipped_samples += 1;
                continue;
            }
            normalization_sumwt += 2.0 * weight;
            reported_sumwt += weight * sumwt_factor;
            gridded_samples += 1;
            accumulate_direct_adjoint(
                &mut psf,
                &pixels,
                ny,
                batch.u_lambda[index],
                batch.v_lambda[index],
                batch.w_lambda[index],
                Complex32::new(weight, 0.0),
            );
        }
    }
    stage_timings.psf_grid += accumulate_started.elapsed();

    if normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let normalize_started = Instant::now();
    psf.mapv_inplace(|value| value / normalization_sumwt);
    let psf_peak = peak_abs_value(&psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "PSF peak is non-finite or zero".to_string(),
        ));
    }
    psf.mapv_inplace(|value| value / psf_peak);
    stage_timings.psf_normalize += normalize_started.elapsed();

    Ok(PsfState {
        psf,
        normalization_sumwt,
        reported_sumwt,
        psf_peak,
        gridded_samples,
        skipped_samples,
    })
}

fn compute_residual_direct(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    model: &Array2<f32>,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    let [nx, ny] = geometry.image_shape;
    let pixels = build_direct_pixel_coordinates(geometry);
    let components = build_direct_components(model, &pixels, ny);
    let mut image = Array2::<f32>::zeros((nx, ny));

    let accumulate_started = Instant::now();
    for batch in batches {
        for index in 0..batch.len() {
            if !batch.gridable[index] {
                continue;
            }
            let weight = batch.weight[index];
            let sample = batch.visibility[index];
            if !(weight.is_finite()
                && weight > 0.0
                && sample.re.is_finite()
                && sample.im.is_finite())
            {
                continue;
            }
            let predicted = if components.is_empty() {
                Complex32::new(0.0, 0.0)
            } else {
                direct_predict_visibility(
                    &components,
                    batch.u_lambda[index],
                    batch.v_lambda[index],
                    batch.w_lambda[index],
                )
            };
            let residual = (sample - predicted) * weight;
            accumulate_direct_adjoint(
                &mut image,
                &pixels,
                ny,
                batch.u_lambda[index],
                batch.v_lambda[index],
                batch.w_lambda[index],
                residual,
            );
        }
    }
    stage_timings.residual_degrid_grid += accumulate_started.elapsed();

    let normalize_started = Instant::now();
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    stage_timings.residual_normalize += normalize_started.elapsed();
    Ok(image)
}

fn build_direct_pixel_coordinates(geometry: ImageGeometry) -> Vec<DirectPixelCoordinate> {
    let [nx, ny] = geometry.image_shape;
    let center_x = nx as f64 / 2.0;
    let center_y = ny as f64 / 2.0;
    let mut pixels = Vec::with_capacity(nx * ny);
    for x in 0..nx {
        for y in 0..ny {
            let l = (x as f64 - center_x) * geometry.cell_size_rad[0];
            // Match CASA GridFT's effective Dec-axis convention. The C++
            // gridder negates UVW's first two axes before locating samples on
            // the padded grid, which maps positive m to lower array indices.
            let m = (center_y - y as f64) * geometry.cell_size_rad[1];
            let radius_sq = l * l + m * m;
            let n_minus_one = if radius_sq < 1.0 {
                (1.0 - radius_sq).sqrt() - 1.0
            } else {
                -1.0
            };
            pixels.push(DirectPixelCoordinate { l, m, n_minus_one });
        }
    }
    pixels
}

fn build_direct_components(
    model: &Array2<f32>,
    pixels: &[DirectPixelCoordinate],
    ny: usize,
) -> Vec<DirectComponent> {
    model
        .indexed_iter()
        .filter_map(|((x, y), value)| {
            if value.abs() <= 0.0 {
                return None;
            }
            let pixel = pixels[x * ny + y];
            Some(DirectComponent {
                value: *value,
                l: pixel.l,
                m: pixel.m,
                n_minus_one: pixel.n_minus_one,
            })
        })
        .collect()
}

fn direct_predict_visibility(
    components: &[DirectComponent],
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
) -> Complex32 {
    let mut predicted = Complex32::new(0.0, 0.0);
    for component in components {
        let phase = std::f64::consts::TAU
            * (u_lambda * component.l + v_lambda * component.m + w_lambda * component.n_minus_one);
        predicted.re += component.value * phase.cos() as f32;
        predicted.im -= component.value * phase.sin() as f32;
    }
    predicted
}

fn accumulate_direct_adjoint(
    image: &mut Array2<f32>,
    pixels: &[DirectPixelCoordinate],
    ny: usize,
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
    value: Complex32,
) {
    for (index, pixel) in pixels.iter().enumerate() {
        let phase = std::f64::consts::TAU
            * (u_lambda * pixel.l + v_lambda * pixel.m + w_lambda * pixel.n_minus_one);
        let contribution = 2.0 * (value.re * phase.cos() as f32 - value.im * phase.sin() as f32);
        image[(index / ny, index % ny)] += contribution;
    }
}

fn peak_abs_value_masked(image: &Array2<f32>, mask: Option<&Array2<bool>>) -> f32 {
    peak_location_masked(image, mask)
        .map(|(_, value)| value.abs())
        .unwrap_or(0.0)
}

fn peak_location_masked(
    image: &Array2<f32>,
    mask: Option<&Array2<bool>>,
) -> Option<((usize, usize), f32)> {
    image.indexed_iter().fold(None, |best, (index, value)| {
        if mask.is_some_and(|current| !current[index]) {
            return best;
        }
        match best {
            None => Some((index, *value)),
            Some((_, best_value)) if value.abs() > best_value.abs() => Some((index, *value)),
            _ => best,
        }
    })
}

fn compute_cycle_threshold(
    peak_residual_jy_per_beam: f32,
    max_psf_sidelobe_level: f32,
    clean: CleanConfig,
) -> f32 {
    let psf_fraction = (max_psf_sidelobe_level * clean.cyclefactor)
        .clamp(clean.min_psf_fraction, clean.max_psf_fraction);
    (peak_residual_jy_per_beam * psf_fraction).max(clean.threshold_jy_per_beam)
}

fn robust_rms_jy_per_beam(residual: &Array2<f32>, clean_mask: Option<&Array2<bool>>) -> f32 {
    let full_mask = clean_mask
        .map(|mask| mask.iter().all(|value| *value))
        .unwrap_or(true);
    let mut values = residual_noise_values(residual, clean_mask, full_mask);
    if values.is_empty() {
        return 0.0;
    }
    if full_mask {
        apply_chauvenet_clipping(&mut values, -1.0, 5);
    }
    if values.is_empty() {
        return 0.0;
    }
    let clipped = ndarray::Array1::from_vec(values).into_dyn();
    (array_madfm(&clipped) as f32) * 1.4826
}

fn nsigma_threshold_jy_per_beam(
    residual: &Array2<f32>,
    clean_mask: Option<&Array2<bool>>,
    clean: CleanConfig,
) -> f32 {
    if clean.nsigma > 0.0 {
        clean.nsigma * robust_rms_jy_per_beam(residual, clean_mask)
    } else {
        0.0
    }
}

fn global_nsigma_threshold_jy_per_beam(
    residual_planes: &[(&Array2<f32>, Option<&Array2<bool>>)],
    clean: CleanConfig,
) -> f32 {
    if clean.nsigma <= 0.0 {
        return 0.0;
    }
    residual_planes
        .iter()
        .map(|(residual, clean_mask)| nsigma_threshold_jy_per_beam(residual, *clean_mask, clean))
        .fold(0.0f32, f32::max)
}

fn residual_noise_values(
    residual: &Array2<f32>,
    clean_mask: Option<&Array2<bool>>,
    full_mask: bool,
) -> Vec<f32> {
    residual
        .iter()
        .zip(
            clean_mask
                .into_iter()
                .flat_map(|mask| mask.iter())
                .chain(std::iter::repeat(&false)),
        )
        .filter_map(|(value, masked)| {
            if !value.is_finite() {
                return None;
            }
            if !full_mask && *masked {
                None
            } else {
                Some(*value)
            }
        })
        .collect()
}

fn apply_chauvenet_clipping(values: &mut Vec<f32>, zscore: f64, max_iterations: i32) {
    if values.is_empty() {
        return;
    }
    let max_i = if max_iterations >= 0 {
        max_iterations as usize
    } else {
        1000usize
    };
    let mut prev_npts = 0usize;
    let mut iteration = 0usize;
    while iteration <= max_i && !values.is_empty() {
        let current_npts = values.len();
        if iteration > 0 && current_npts == prev_npts {
            break;
        }
        let (mean, stddev) = mean_stddev(values);
        if !mean.is_finite() || !stddev.is_finite() || stddev <= 0.0 {
            break;
        }
        let z = if zscore >= 0.0 {
            zscore
        } else {
            chauvenet_max_zscore(current_npts as u64)
        };
        let low = mean - z * stddev;
        let high = mean + z * stddev;
        values.retain(|value| {
            let value = *value as f64;
            value >= low && value <= high
        });
        prev_npts = current_npts;
        iteration += 1;
    }
}

fn mean_stddev(values: &[f32]) -> (f64, f64) {
    let n = values.len() as f64;
    let mean = values.iter().map(|value| *value as f64).sum::<f64>() / n;
    let sumsq = values
        .iter()
        .map(|value| {
            let diff = *value as f64 - mean;
            diff * diff
        })
        .sum::<f64>();
    let variance = if values.len() > 1 {
        sumsq / (n - 1.0)
    } else {
        0.0
    };
    (mean, variance.sqrt())
}

fn chauvenet_max_zscore(npts: u64) -> f64 {
    const NPTS_TO_MAX_ZSCORE: &[(u64, f64)] = &[
        (0, 0.5),
        (1, 1.0),
        (3, 1.5),
        (10, 2.0),
        (40, 2.5),
        (185, 3.0),
        (1074, 3.5),
        (7893, 4.0),
        (73579, 4.5),
        (872138, 5.0),
        (13165126, 5.5),
        (253398672, 6.0),
        (6225098696, 6.5),
        (195341107722, 7.0),
    ];
    if let Some((_, zscore)) = NPTS_TO_MAX_ZSCORE.iter().find(|(count, _)| *count == npts) {
        return *zscore;
    }
    let mut low_index = 0usize;
    let mut high_index = 1usize;
    if npts > NPTS_TO_MAX_ZSCORE[NPTS_TO_MAX_ZSCORE.len() - 1].0 {
        let mut z = NPTS_TO_MAX_ZSCORE[NPTS_TO_MAX_ZSCORE.len() - 1].1 + 0.5;
        loop {
            let npts_min = chauvenet_zscore_to_npts(z);
            if npts_min >= npts {
                low_index = NPTS_TO_MAX_ZSCORE.len() - 2;
                high_index = NPTS_TO_MAX_ZSCORE.len() - 1;
                break;
            }
            z += 0.5;
        }
    } else {
        while high_index < NPTS_TO_MAX_ZSCORE.len() && NPTS_TO_MAX_ZSCORE[high_index].0 < npts {
            low_index += 1;
            high_index += 1;
        }
    }
    let (mut low_z, mut high_z) = (
        NPTS_TO_MAX_ZSCORE[low_index].1,
        NPTS_TO_MAX_ZSCORE[high_index].1,
    );
    let mut z = (low_z + high_z) / 2.0;
    loop {
        let npts_min = chauvenet_zscore_to_npts(z);
        if npts_min == npts || (high_z - low_z).abs() <= 1e-12 {
            return z;
        }
        if npts_min > npts {
            high_z = z;
        } else {
            low_z = z;
        }
        z = (low_z + high_z) / 2.0;
    }
}

fn chauvenet_zscore_to_npts(zscore: f64) -> u64 {
    (0.5 / erfc(zscore / std::f64::consts::SQRT_2)) as u64
}

fn threshold_reached_with_tolerance(peak_abs_jy_per_beam: f32, threshold_jy_per_beam: f32) -> bool {
    if threshold_jy_per_beam <= 0.0 {
        return peak_abs_jy_per_beam <= threshold_jy_per_beam;
    }
    peak_abs_jy_per_beam <= threshold_jy_per_beam
        || ((peak_abs_jy_per_beam - threshold_jy_per_beam).abs() / threshold_jy_per_beam) < 0.01
}

fn strict_clean_stop_reason(
    peak_abs_jy_per_beam: f32,
    threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
) -> Option<CleanStopReason> {
    if peak_abs_jy_per_beam <= threshold_jy_per_beam {
        Some(CleanStopReason::GlobalThresholdReached)
    } else if nsigma_threshold_jy_per_beam > threshold_jy_per_beam
        && peak_abs_jy_per_beam <= nsigma_threshold_jy_per_beam
    {
        Some(CleanStopReason::NsigmaThresholdReached)
    } else {
        None
    }
}

fn tolerant_clean_stop_reason(
    peak_abs_jy_per_beam: f32,
    threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
) -> Option<CleanStopReason> {
    if threshold_reached_with_tolerance(peak_abs_jy_per_beam, threshold_jy_per_beam) {
        Some(CleanStopReason::GlobalThresholdReached)
    } else if nsigma_threshold_jy_per_beam > threshold_jy_per_beam
        && threshold_reached_with_tolerance(peak_abs_jy_per_beam, nsigma_threshold_jy_per_beam)
    {
        Some(CleanStopReason::NsigmaThresholdReached)
    } else {
        None
    }
}

fn minor_cycle_stop_reason(
    peak_abs_jy_per_beam: f32,
    threshold_jy_per_beam: f32,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
) -> Option<CleanStopReason> {
    if let Some(reason) = strict_clean_stop_reason(
        peak_abs_jy_per_beam,
        threshold_jy_per_beam,
        nsigma_threshold_jy_per_beam,
    ) {
        Some(reason)
    } else if peak_abs_jy_per_beam <= cycle_threshold_jy_per_beam {
        Some(CleanStopReason::CycleThresholdReached)
    } else {
        None
    }
}

fn update_divergence_state(
    warnings: &mut Vec<String>,
    min_residual_peak_jy_per_beam: &mut f32,
    current_peak: f32,
    divergence_warned: &mut bool,
) {
    if current_peak < *min_residual_peak_jy_per_beam {
        *min_residual_peak_jy_per_beam = current_peak;
    } else if *min_residual_peak_jy_per_beam > 0.0
        && (current_peak - *min_residual_peak_jy_per_beam) / *min_residual_peak_jy_per_beam > 0.1
        && !*divergence_warned
    {
        warnings.push(format!(
            "minor-cycle divergence detected: residual peak {:.6} Jy/beam exceeded prior minimum {:.6} Jy/beam by more than 10%",
            current_peak, *min_residual_peak_jy_per_beam
        ));
        *divergence_warned = true;
    }
}

fn subtract_shifted_psf(
    residual: &mut Array2<f32>,
    psf: &Array2<f32>,
    peak_index: (usize, usize),
    component: f32,
) {
    subtract_shifted_kernel(residual, psf, peak_index, component);
}

fn subtract_shifted_kernel(
    image: &mut Array2<f32>,
    kernel: &Array2<f32>,
    peak_index: (usize, usize),
    scale_factor: f32,
) {
    let kernel_center = (kernel.shape()[0] / 2, kernel.shape()[1] / 2);
    for x in 0..image.shape()[0] {
        for y in 0..image.shape()[1] {
            let kernel_x = x as isize - peak_index.0 as isize + kernel_center.0 as isize;
            let kernel_y = y as isize - peak_index.1 as isize + kernel_center.1 as isize;
            if !(0..kernel.shape()[0] as isize).contains(&kernel_x)
                || !(0..kernel.shape()[1] as isize).contains(&kernel_y)
            {
                continue;
            }
            image[(x, y)] -= scale_factor * kernel[(kernel_x as usize, kernel_y as usize)];
        }
    }
}

fn add_shifted_kernel(
    image: &mut Array2<f32>,
    kernel: &Array2<f32>,
    peak_index: (usize, usize),
    scale_factor: f32,
) {
    let kernel_center = (kernel.shape()[0] / 2, kernel.shape()[1] / 2);
    for x in 0..image.shape()[0] {
        for y in 0..image.shape()[1] {
            let kernel_x = x as isize - peak_index.0 as isize + kernel_center.0 as isize;
            let kernel_y = y as isize - peak_index.1 as isize + kernel_center.1 as isize;
            if !(0..kernel.shape()[0] as isize).contains(&kernel_x)
                || !(0..kernel.shape()[1] as isize).contains(&kernel_y)
            {
                continue;
            }
            image[(x, y)] += scale_factor * kernel[(kernel_x as usize, kernel_y as usize)];
        }
    }
}

fn peak_abs_value(image: &Array2<f32>) -> f32 {
    image
        .iter()
        .fold(0.0f32, |best, value| best.max(value.abs()))
}

fn dirty_clean_config(psf_cutoff: f32) -> CleanConfig {
    CleanConfig {
        niter: 0,
        gain: 0.1,
        threshold_jy_per_beam: 0.0,
        nsigma: 0.0,
        psf_cutoff,
        minor_cycle_length: 1,
        cyclefactor: 1.0,
        min_psf_fraction: 0.05,
        max_psf_fraction: 0.8,
    }
}

fn add_stage_timings(total: &mut ImagingStageTimings, part: ImagingStageTimings) {
    total.controller_overhead += part.controller_overhead;
    total.psf_grid += part.psf_grid;
    total.psf_fft += part.psf_fft;
    total.psf_normalize += part.psf_normalize;
    total.model_fft += part.model_fft;
    total.residual_degrid_grid += part.residual_degrid_grid;
    total.residual_fft += part.residual_fft;
    total.residual_normalize += part.residual_normalize;
    total.minor_cycle += part.minor_cycle;
    total.minor_cycle_solve += part.minor_cycle_solve;
    total.major_cycle_refresh += part.major_cycle_refresh;
    total.beam_fit += part.beam_fit;
    total.restore += part.restore;
    total.total += part.total;
}

fn casa_major_cycle_count(refreshes: usize, clean_niter: usize) -> usize {
    if clean_niter > 0 { refreshes + 1 } else { 0 }
}

fn expand_plane(plane: &Array2<f32>) -> Array4<f32> {
    let (nx, ny) = plane.dim();
    let mut expanded = Array4::<f32>::zeros((nx, ny, 1, 1));
    expanded.slice_mut(s![.., .., 0, 0]).assign(plane);
    expanded
}

fn expand_scalar(value: f32) -> Array4<f32> {
    let mut expanded = Array4::<f32>::zeros((1, 1, 1, 1));
    expanded[(0, 0, 0, 0)] = value;
    expanded
}

#[cfg(test)]
#[allow(clippy::excessive_precision, clippy::useless_vec)]
mod tests {
    use ndarray::{Array2, s};
    use num_complex::Complex32;

    use super::{
        CleanConfig, CleanStopReason, CompatibilityMode, CubeChannelRequest, CubeImagingRequest,
        Deconvolver, DirectComponent, ImageGeometry, ImagingRequest, ImagingStageTimings,
        ParallelHandBatch, PlaneStokes, RestoringBeamMode, StandardGridder, VisibilityBatch,
        WTermMode, WeightDensityMode, WeightingMode, add_shifted_kernel, apply_chauvenet_clipping,
        build_direct_components, build_direct_pixel_coordinates, centered_fft2,
        compute_cycle_threshold, compute_psf, compute_psf_direct, compute_residual,
        compute_residual_direct, direct_predict_visibility, dirty_clean_config,
        make_multiscale_kernel, mean_stddev, peak_abs_value, run_cube, run_dirty_cube, run_imaging,
        tolerant_clean_stop_reason,
    };
    fn point_source_visibilities(
        samples: &[(f64, f64, f64)],
        cell_rad: f64,
        image_shape: [usize; 2],
        offset_pixels: (f64, f64),
        flux: f32,
    ) -> VisibilityBatch {
        point_source_visibilities_with_mode(
            samples,
            cell_rad,
            image_shape,
            offset_pixels,
            flux,
            false,
        )
    }

    fn point_source_visibilities_with_w_term(
        samples: &[(f64, f64, f64)],
        cell_rad: f64,
        image_shape: [usize; 2],
        offset_pixels: (f64, f64),
        flux: f32,
    ) -> VisibilityBatch {
        point_source_visibilities_with_mode(
            samples,
            cell_rad,
            image_shape,
            offset_pixels,
            flux,
            true,
        )
    }

    fn point_source_visibilities_with_mode(
        samples: &[(f64, f64, f64)],
        cell_rad: f64,
        image_shape: [usize; 2],
        offset_pixels: (f64, f64),
        flux: f32,
        include_w_term: bool,
    ) -> VisibilityBatch {
        let center_x = image_shape[0] as f64 / 2.0;
        let center_y = image_shape[1] as f64 / 2.0;
        let l = (offset_pixels.0 - center_x) * cell_rad;
        let m = (center_y - offset_pixels.1) * cell_rad;
        let n_minus_one = if include_w_term {
            (1.0 - l * l - m * m).sqrt() - 1.0
        } else {
            0.0
        };
        let mut batch = VisibilityBatch {
            u_lambda: Vec::with_capacity(samples.len()),
            v_lambda: Vec::with_capacity(samples.len()),
            w_lambda: Vec::with_capacity(samples.len()),
            weight: Vec::with_capacity(samples.len()),
            sumwt_factor: Vec::with_capacity(samples.len()),
            gridable: Vec::with_capacity(samples.len()),
            visibility: Vec::with_capacity(samples.len()),
        };
        for (u, v, w) in samples {
            let phase = -2.0 * std::f64::consts::PI * (u * l + v * m + w * n_minus_one);
            batch.u_lambda.push(*u);
            batch.v_lambda.push(*v);
            batch.w_lambda.push(*w);
            batch.weight.push(1.0);
            batch.sumwt_factor.push(1.0);
            batch.gridable.push(true);
            batch.visibility.push(Complex32::new(
                flux * phase.cos() as f32,
                flux * phase.sin() as f32,
            ));
        }
        batch
    }

    fn rms_difference(left: &Array2<f32>, right: &Array2<f32>) -> f32 {
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for (lhs, rhs) in left.iter().zip(right.iter()) {
            let delta = f64::from(*lhs - *rhs);
            sum += delta * delta;
            count += 1;
        }
        (sum / count as f64).sqrt() as f32
    }

    #[test]
    fn strict_stokes_i_rejects_flagged_parallel_hand_samples() {
        let batch = ParallelHandBatch {
            u_lambda: vec![10.0, 20.0],
            v_lambda: vec![5.0, 8.0],
            w_lambda: vec![0.0, 0.0],
            first_visibility: vec![Complex32::new(1.0, 0.0); 2],
            second_visibility: vec![Complex32::new(1.0, 0.0); 2],
            first_weight: vec![1.0, 1.0],
            second_weight: vec![1.0, 1.0],
            first_flagged: vec![false, true],
            second_flagged: vec![false, false],
            gridable: vec![true, true],
        };
        let collapsed = batch.collapse_to_stokes_i().unwrap();
        assert_eq!(collapsed.len(), 1);
    }

    #[test]
    fn strict_stokes_i_uses_half_sum_visibility_and_casa_style_weight() {
        let batch = ParallelHandBatch {
            u_lambda: vec![10.0],
            v_lambda: vec![5.0],
            w_lambda: vec![0.0],
            first_visibility: vec![Complex32::new(2.0, 1.0)],
            second_visibility: vec![Complex32::new(6.0, -1.0)],
            first_weight: vec![1.5],
            second_weight: vec![3.0],
            first_flagged: vec![false],
            second_flagged: vec![false],
            gridable: vec![true],
        };
        let collapsed = batch.collapse_to_stokes_i().unwrap();
        assert_eq!(collapsed.len(), 1);
        assert!((collapsed.visibility[0].re - 4.0).abs() < 1.0e-6);
        assert!(collapsed.visibility[0].im.abs() < 1.0e-6);
        assert!((collapsed.weight[0] - 2.25).abs() < 1.0e-6);
        assert!((collapsed.sumwt_factor[0] - 2.0).abs() < 1.0e-6);
    }

    #[test]
    fn reported_sumwt_tracks_logical_samples_not_mirrored_normalization() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = VisibilityBatch {
            u_lambda: vec![10.0],
            v_lambda: vec![5.0],
            w_lambda: vec![0.0],
            weight: vec![1.5],
            sumwt_factor: vec![1.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(1.0, 0.0)],
        };
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::XX,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!((result.sumwt[(0, 0, 0, 0)] - 1.5).abs() < 1.0e-5);
    }

    #[test]
    fn clean_global_threshold_can_stop_before_iterations() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                gain: 0.2,
                threshold_jy_per_beam: 10.0,
                nsigma: 0.0,
                ..CleanConfig::default()
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(
            result.diagnostics.clean_stop_reason,
            Some(CleanStopReason::GlobalThresholdReached)
        );
        assert_eq!(result.diagnostics.minor_iterations, 0);
        assert_eq!(result.diagnostics.major_cycles, 1);
    }

    #[test]
    fn dirty_image_recovers_centered_point_source() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        let center = result.residual[(32, 32, 0, 0)];
        assert!(center > 0.7);
        assert!((result.psf[(32, 32, 0, 0)] - 1.0).abs() < 1.0e-4);
    }

    #[test]
    fn dirty_image_tracks_off_center_peak_location() {
        let samples = vec![
            (-150.0, -120.0, 0.0),
            (-90.0, 75.0, 0.0),
            (60.0, -90.0, 0.0),
            (130.0, 85.0, 0.0),
            (20.0, 15.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (37.0, 28.0), 1.0);
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        let mut best = ((0usize, 0usize), 0.0f32);
        for x in 0..64 {
            for y in 0..64 {
                let value = result.residual[(x, y, 0, 0)].abs();
                if value > best.1 {
                    best = ((x, y), value);
                }
            }
        }
        assert!(
            (best.0.0 as isize - 37).abs() <= 2,
            "unexpected x peak location: {:?}",
            best.0
        );
        assert!(
            (best.0.1 as isize - 28).abs() <= 2,
            "unexpected y peak location: {:?}",
            best.0
        );
    }

    #[test]
    fn dirty_image_matches_casa_dec_axis_convention_for_positive_m() {
        let samples = vec![
            (-150.0, -120.0, 0.0),
            (-90.0, 75.0, 0.0),
            (60.0, -90.0, 0.0),
            (130.0, 85.0, 0.0),
            (20.0, 15.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let center_x = 32.0f64;
        let center_y = 32.0f64;
        let target = (37usize, 28usize);
        let l = (target.0 as f64 - center_x) * geometry.cell_size_rad[0];
        let m = (center_y - target.1 as f64) * geometry.cell_size_rad[1];
        let visibility = VisibilityBatch {
            u_lambda: samples.iter().map(|(u, _, _)| *u).collect(),
            v_lambda: samples.iter().map(|(_, v, _)| *v).collect(),
            w_lambda: samples.iter().map(|(_, _, w)| *w).collect(),
            weight: vec![1.0; samples.len()],
            sumwt_factor: vec![1.0; samples.len()],
            gridable: vec![true; samples.len()],
            visibility: samples
                .iter()
                .map(|(u, v, w)| {
                    let phase = -2.0 * std::f64::consts::PI * (u * l + v * m + w * 0.0);
                    Complex32::new(phase.cos() as f32, phase.sin() as f32)
                })
                .collect(),
        };
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![visibility],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        let mut best = ((0usize, 0usize), 0.0f32);
        for x in 0..64 {
            for y in 0..64 {
                let value = result.residual[(x, y, 0, 0)].abs();
                if value > best.1 {
                    best = ((x, y), value);
                }
            }
        }
        assert!(
            (best.0.0 as isize - target.0 as isize).abs() <= 2,
            "unexpected x peak location: {:?}",
            best.0
        );
        assert!(
            (best.0.1 as isize - target.1 as isize).abs() <= 2,
            "unexpected y peak location: {:?}",
            best.0
        );
    }

    #[test]
    fn fft_major_cycle_prediction_matches_direct_for_off_center_source() {
        let samples = vec![
            (-150.0, -120.0, 0.0),
            (-90.0, 75.0, 0.0),
            (60.0, -90.0, 0.0),
            (130.0, 85.0, 0.0),
            (20.0, 15.0, 0.0),
            (-45.0, 40.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (37.0, 28.0), 1.0);
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut stage_timings = ImagingStageTimings::default();
        let psf_state = compute_psf(
            &request,
            std::slice::from_ref(&batch),
            &gridder,
            &mut stage_timings,
        )
        .unwrap();
        let mut model = Array2::<f32>::zeros((64, 64));
        model[(37, 28)] = 1.0;

        let fft_residual = compute_residual(
            &request,
            std::slice::from_ref(&batch),
            &gridder,
            &model,
            &psf_state,
            &mut stage_timings,
        )
        .unwrap();
        let direct_residual = compute_residual_direct(
            geometry,
            std::slice::from_ref(&batch),
            &model,
            &psf_state,
            &mut stage_timings,
        )
        .unwrap();

        let fft_peak = peak_abs_value(&fft_residual);
        let direct_peak = peak_abs_value(&direct_residual);
        let model_grid = centered_fft2(&gridder.apodize_model(&model));
        let predicted_direct = direct_predict_visibility(
            &[DirectComponent {
                value: 1.0,
                l: (37.0 - 32.0) * 1.0e-4,
                m: (32.0 - 28.0) * 1.0e-4,
                n_minus_one: 0.0,
            }],
            batch.u_lambda[0],
            batch.v_lambda[0],
            batch.w_lambda[0],
        );
        let predicted_fft = gridder
            .degrid_sample(&model_grid, batch.u_lambda[0], batch.v_lambda[0])
            .unwrap();
        assert!(
            (predicted_fft.re - predicted_direct.re).abs() < 8.0e-4
                && (predicted_fft.im - predicted_direct.im).abs() < 8.0e-4,
            "FFT prediction should match the direct model: direct={predicted_direct:?} fft={predicted_fft:?}"
        );
        assert!(
            direct_peak < 1.0e-4,
            "direct residual peak should be nearly zero, got {direct_peak}"
        );
        assert!(
            fft_peak < 1.0e-2,
            "FFT residual peak should stay small when prediction matches the direct model, got {fft_peak}"
        );
    }

    #[test]
    fn fft_major_cycle_prediction_matches_direct_for_structured_model() {
        let samples = vec![
            (-310.25, -205.5, 0.0),
            (-248.75, 140.125, 0.0),
            (-180.5, 285.75, 0.0),
            (-95.125, -310.875, 0.0),
            (24.625, 96.5, 0.0),
            (77.25, -55.875, 0.0),
            (138.875, 228.125, 0.0),
            (255.5, -170.625, 0.0),
            (312.75, 45.25, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let mut model = Array2::<f32>::zeros((64, 64));
        let scale5 = make_multiscale_kernel((64, 64), 5.0);
        let scale12 = make_multiscale_kernel((64, 64), 12.0);
        add_shifted_kernel(&mut model, &scale5, (29, 34), 0.8);
        add_shifted_kernel(&mut model, &scale12, (39, 26), -0.35);
        model[(33, 31)] += 0.25;
        let pixels = build_direct_pixel_coordinates(geometry);
        let components = build_direct_components(&model, &pixels, 64);
        let visibilities = samples
            .iter()
            .map(|(u, v, w)| direct_predict_visibility(&components, *u, *v, *w))
            .collect::<Vec<_>>();
        let batch = VisibilityBatch {
            u_lambda: samples.iter().map(|(u, _, _)| *u).collect(),
            v_lambda: samples.iter().map(|(_, v, _)| *v).collect(),
            w_lambda: samples.iter().map(|(_, _, w)| *w).collect(),
            visibility: visibilities,
            weight: vec![1.0; samples.len()],
            sumwt_factor: vec![1.0; samples.len()],
            gridable: vec![true; samples.len()],
        };
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Multiscale,
            multiscale_scales: vec![0.0, 5.0, 12.0],
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut stage_timings = ImagingStageTimings::default();
        let psf_state = compute_psf(
            &request,
            std::slice::from_ref(&batch),
            &gridder,
            &mut stage_timings,
        )
        .unwrap();

        let fft_residual = compute_residual(
            &request,
            std::slice::from_ref(&batch),
            &gridder,
            &model,
            &psf_state,
            &mut stage_timings,
        )
        .unwrap();
        let direct_residual = compute_residual_direct(
            geometry,
            std::slice::from_ref(&batch),
            &model,
            &psf_state,
            &mut stage_timings,
        )
        .unwrap();
        let fft_peak = peak_abs_value(&fft_residual);
        let direct_peak = peak_abs_value(&direct_residual);
        let model_grid = centered_fft2(&gridder.apodize_model(&model));
        let predicted_fft = gridder
            .degrid_sample(&model_grid, batch.u_lambda[0], batch.v_lambda[0])
            .unwrap();
        let predicted_direct = direct_predict_visibility(
            &components,
            batch.u_lambda[0],
            batch.v_lambda[0],
            batch.w_lambda[0],
        );
        assert!(
            (predicted_fft.re - predicted_direct.re).abs() < 1.0e-3
                && (predicted_fft.im - predicted_direct.im).abs() < 1.0e-3,
            "FFT prediction should match the direct structured model: direct={predicted_direct:?} fft={predicted_fft:?}"
        );
        assert!(
            direct_peak < 1.0e-4,
            "direct residual peak should be nearly zero for the structured model, got {direct_peak}"
        );
        assert!(
            fft_peak < 3.0e-2,
            "FFT residual peak should stay small for the structured model, got {fft_peak}"
        );
    }

    #[test]
    fn fft_dirty_image_matches_direct_adjoint_for_off_center_source() {
        let samples = vec![
            (-310.25, -205.5, 0.0),
            (-248.75, 140.125, 0.0),
            (-180.5, 285.75, 0.0),
            (-95.125, -310.875, 0.0),
            (24.625, 96.5, 0.0),
            (77.25, -55.875, 0.0),
            (138.875, 228.125, 0.0),
            (255.5, -170.625, 0.0),
            (312.75, 45.25, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (42.0, 21.0), 1.0);
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut stage_timings = ImagingStageTimings::default();
        let psf_state = compute_psf(
            &request,
            std::slice::from_ref(&batch),
            &gridder,
            &mut stage_timings,
        )
        .unwrap();
        let direct_psf_state =
            compute_psf_direct(geometry, std::slice::from_ref(&batch), &mut stage_timings).unwrap();
        let model = Array2::<f32>::zeros((64, 64));
        let fft_dirty = compute_residual(
            &request,
            std::slice::from_ref(&batch),
            &gridder,
            &model,
            &psf_state,
            &mut stage_timings,
        )
        .unwrap();
        let direct_dirty = compute_residual_direct(
            geometry,
            std::slice::from_ref(&batch),
            &model,
            &direct_psf_state,
            &mut stage_timings,
        )
        .unwrap();
        let psf_rms = rms_difference(&psf_state.psf, &direct_psf_state.psf);
        let rms = rms_difference(&fft_dirty, &direct_dirty);
        assert!(
            rms < 3.0e-2 && psf_rms < 3.0e-2,
            "FFT dirty image should match the direct adjoint: dirty_rms={rms} psf_rms={psf_rms} fft_peak={} direct_peak={} fft_psf_peak={} direct_psf_peak={}",
            peak_abs_value(&fft_dirty),
            peak_abs_value(&direct_dirty),
            peak_abs_value(&psf_state.psf),
            peak_abs_value(&direct_psf_state.psf),
        );
    }

    #[test]
    fn partitioning_batches_is_invariant() {
        let samples = vec![
            (-150.0, -120.0, 0.0),
            (-90.0, 75.0, 0.0),
            (60.0, -90.0, 0.0),
            (130.0, 85.0, 0.0),
            (20.0, 15.0, 0.0),
            (-45.0, 40.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let all = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let split_left = VisibilityBatch {
            u_lambda: all.u_lambda[..3].to_vec(),
            v_lambda: all.v_lambda[..3].to_vec(),
            w_lambda: all.w_lambda[..3].to_vec(),
            weight: all.weight[..3].to_vec(),
            sumwt_factor: all.sumwt_factor[..3].to_vec(),
            gridable: all.gridable[..3].to_vec(),
            visibility: all.visibility[..3].to_vec(),
        };
        let split_right = VisibilityBatch {
            u_lambda: all.u_lambda[3..].to_vec(),
            v_lambda: all.v_lambda[3..].to_vec(),
            w_lambda: all.w_lambda[3..].to_vec(),
            weight: all.weight[3..].to_vec(),
            sumwt_factor: all.sumwt_factor[3..].to_vec(),
            gridable: all.gridable[3..].to_vec(),
            visibility: all.visibility[3..].to_vec(),
        };
        let full = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![all],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let split = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![split_left, split_right],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        for (a, b) in full.residual.iter().zip(split.residual.iter()) {
            assert!((a - b).abs() < 1.0e-5);
        }
    }

    #[test]
    fn dirty_cube_stacks_channel_planes_on_spectral_axis() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.5e-4, 1.5e-4],
        };
        let samples = [(20.0, -10.0, 0.0), (-15.0, 25.0, 0.0), (30.0, 12.0, 0.0)];
        let channel_a = CubeChannelRequest {
            channel_frequency_hz: 1.40e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (16.0, 16.0),
                1.0,
            )],
        };
        let channel_b = CubeChannelRequest {
            channel_frequency_hz: 1.41e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (18.0, 14.0),
                2.0,
            )],
        };

        let result = run_dirty_cube(&CubeImagingRequest {
            geometry,
            channels: vec![channel_a, channel_b],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(result.image.shape(), &[32, 32, 1, 2]);
        assert_eq!(result.sumwt.shape(), &[1, 1, 1, 2]);
        assert_eq!(
            result.compatibility.channel_frequencies_hz,
            vec![1.40e9, 1.41e9]
        );
        assert!(result.sumwt[(0, 0, 0, 0)] > 0.0);
        assert!(result.sumwt[(0, 0, 0, 1)] > 0.0);
        let plane_difference = (&result.image.slice(s![.., .., 0, 0])
            - &result.image.slice(s![.., .., 0, 1]))
            .iter()
            .map(|value| value.abs())
            .fold(0.0f32, f32::max);
        assert!(plane_difference > 1.0e-3);
        assert_eq!(result.beams.len(), 2);
        assert_eq!(result.diagnostics.channel_diagnostics.len(), 2);
    }

    #[test]
    fn dirty_cube_allows_blank_planes_from_empty_channel_batches() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.5e-4, 1.5e-4],
        };
        let samples = [(20.0, -10.0, 0.0), (-15.0, 25.0, 0.0), (30.0, 12.0, 0.0)];
        let populated = CubeChannelRequest {
            channel_frequency_hz: 1.40e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (16.0, 16.0),
                1.0,
            )],
        };
        let blank = CubeChannelRequest {
            channel_frequency_hz: 1.45e9,
            visibility_batches: vec![VisibilityBatch {
                u_lambda: Vec::new(),
                v_lambda: Vec::new(),
                w_lambda: Vec::new(),
                weight: Vec::new(),
                sumwt_factor: Vec::new(),
                gridable: Vec::new(),
                visibility: Vec::new(),
            }],
        };

        let result = run_dirty_cube(&CubeImagingRequest {
            geometry,
            channels: vec![populated, blank],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(result.sumwt[(0, 0, 0, 0)] > 0.0);
        assert_eq!(result.sumwt[(0, 0, 0, 1)], 0.0);
        assert_eq!(
            peak_abs_value(&result.image.slice(s![.., .., 0, 1]).to_owned()),
            0.0
        );
        assert!(result.beams[1].is_none());
    }

    #[test]
    fn hogbom_cube_cleans_each_channel_independently() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let channel_a = CubeChannelRequest {
            channel_frequency_hz: 1.40e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
        };
        let channel_b = CubeChannelRequest {
            channel_frequency_hz: 1.41e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
        };

        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels: vec![channel_a, channel_b],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 20,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
            },
            clean_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 0]).to_owned()) > 1.0e-3);
        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 1]).to_owned()) > 1.0e-3);
        assert!(
            result.diagnostics.channel_diagnostics[0].minor_iterations > 0,
            "expected cube plane 0 to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].minor_iterations > 0,
            "expected cube plane 1 to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[0].major_cycles > 0,
            "expected cube plane 0 to refresh residuals"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].major_cycles > 0,
            "expected cube plane 1 to refresh residuals"
        );
    }

    #[test]
    fn cube_hogbom_can_report_more_iterations_than_niter_with_multiple_planes() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let make_channel = |freq_hz, center| CubeChannelRequest {
            channel_frequency_hz: freq_hz,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                center,
                1.0,
            )],
        };
        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels: vec![
                make_channel(1.40e9, (24.0, 24.0)),
                make_channel(1.41e9, (26.0, 22.0)),
                make_channel(1.42e9, (20.0, 28.0)),
            ],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
            },
            clean_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(
            result.diagnostics.minor_iterations > 1,
            "cube controller should spend one full cycle budget per plane before checking niter"
        );
    }

    #[test]
    fn clark_cube_cleans_each_channel_independently() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let channel_a = CubeChannelRequest {
            channel_frequency_hz: 1.40e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
        };
        let channel_b = CubeChannelRequest {
            channel_frequency_hz: 1.41e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
        };

        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels: vec![channel_a, channel_b],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Clark,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 20,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
            },
            clean_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 0]).to_owned()) > 1.0e-3);
        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 1]).to_owned()) > 1.0e-3);
        assert!(
            result.diagnostics.channel_diagnostics[0].minor_iterations > 0,
            "expected cube Clark plane 0 to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].minor_iterations > 0,
            "expected cube Clark plane 1 to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[0].major_cycles > 0,
            "expected cube Clark plane 0 to refresh residuals"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].major_cycles > 0,
            "expected cube Clark plane 1 to refresh residuals"
        );
        assert!(
            result
                .diagnostics
                .channel_diagnostics
                .iter()
                .map(|diagnostics| diagnostics.minor_iterations)
                .sum::<usize>()
                <= 20,
            "cube Clark should respect the shared cube niter budget"
        );
    }

    #[test]
    fn multiscale_cube_cleans_each_channel_independently() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let channel_a = CubeChannelRequest {
            channel_frequency_hz: 1.40e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
        };
        let channel_b = CubeChannelRequest {
            channel_frequency_hz: 1.41e9,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
        };

        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels: vec![channel_a, channel_b],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Multiscale,
            multiscale_scales: vec![0.0, 4.0],
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 20,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
            },
            clean_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 0]).to_owned()) > 1.0e-3);
        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 1]).to_owned()) > 1.0e-3);
        assert!(
            result.diagnostics.channel_diagnostics[0].minor_iterations > 0,
            "expected cube multiscale plane 0 to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].minor_iterations > 0,
            "expected cube multiscale plane 1 to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[0].major_cycles > 0,
            "expected cube multiscale plane 0 to refresh residuals"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].major_cycles > 0,
            "expected cube multiscale plane 1 to refresh residuals"
        );
        assert!(
            result
                .diagnostics
                .channel_diagnostics
                .iter()
                .map(|diagnostics| diagnostics.minor_iterations)
                .sum::<usize>()
                <= 20,
            "cube multiscale should respect the shared cube niter budget"
        );
    }

    #[test]
    fn hogbom_reduces_peak_residual() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let dirty = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let clean = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(
            clean.diagnostics.final_residual_peak_jy_per_beam
                < dirty.diagnostics.final_residual_peak_jy_per_beam
        );
        assert!(clean.model[(32, 32, 0, 0)] > 0.0);
    }

    #[test]
    fn casa_hogbom_compatibility_uses_inclusive_cycle_iteration_budget() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(
            result.diagnostics.clean_stop_reason,
            Some(CleanStopReason::IterationLimitReached)
        );
        assert_eq!(result.diagnostics.major_cycles, 1);
        assert_eq!(result.diagnostics.minor_iterations, 2);
        assert!(result.model[(32, 32, 0, 0)] > 0.0);
    }

    #[test]
    fn multiscale_scales_zero_matches_clark_single_component_behavior() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let clark = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Clark,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let multiscale = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Multiscale,
            multiscale_scales: vec![0.0],
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(multiscale.diagnostics.minor_iterations, 1);
        for (left, right) in clark.model.iter().zip(multiscale.model.iter()) {
            assert!((left - right).abs() < 1.0e-5);
        }
    }

    #[test]
    fn clark_niter_one_uses_one_minor_iteration() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Clark,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(
            result.diagnostics.clean_stop_reason,
            Some(CleanStopReason::IterationLimitReached)
        );
        assert_eq!(result.diagnostics.major_cycles, 1);
        assert_eq!(result.diagnostics.minor_iterations, 1);
        let nonzero = result
            .model
            .iter()
            .filter(|value| value.abs() > 0.0)
            .count();
        assert_eq!(nonzero, 1);
    }

    #[test]
    fn multiscale_nonzero_scale_spreads_model_support() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let offsets = [
            (30.0, 30.0),
            (31.0, 30.0),
            (32.0, 30.0),
            (33.0, 30.0),
            (30.0, 31.0),
            (31.0, 31.0),
            (32.0, 31.0),
            (33.0, 31.0),
            (30.0, 32.0),
            (31.0, 32.0),
            (32.0, 32.0),
            (33.0, 32.0),
        ];
        let samples = [
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let mut visibility = VisibilityBatch {
            u_lambda: Vec::new(),
            v_lambda: Vec::new(),
            w_lambda: Vec::new(),
            weight: Vec::new(),
            sumwt_factor: Vec::new(),
            gridable: Vec::new(),
            visibility: Vec::new(),
        };
        for (u, v, w) in samples {
            let mut vis = Complex32::new(0.0, 0.0);
            for offset in offsets {
                let component =
                    point_source_visibilities(&[(u, v, w)], 1.0e-4, [64, 64], offset, 1.0);
                vis += component.visibility[0];
            }
            visibility.u_lambda.push(u);
            visibility.v_lambda.push(v);
            visibility.w_lambda.push(w);
            visibility.weight.push(1.0);
            visibility.sumwt_factor.push(1.0);
            visibility.gridable.push(true);
            visibility.visibility.push(vis);
        }

        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![visibility],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Multiscale,
            multiscale_scales: vec![4.0],
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        let nonzero = result
            .model
            .iter()
            .filter(|value| value.abs() > 1.0e-6)
            .count();
        assert!(nonzero > 1);
    }

    #[test]
    fn clean_mask_restricts_component_selection() {
        let samples = vec![
            (-150.0, -120.0, 0.0),
            (-90.0, 75.0, 0.0),
            (60.0, -90.0, 0.0),
            (130.0, 85.0, 0.0),
            (20.0, 15.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (40.0, 24.0), 1.0);
        let mut mask = Array2::<bool>::from_elem((64, 64), false);
        for x in 8..16 {
            for y in 8..16 {
                mask[(x, y)] = true;
            }
        }
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 8,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
            },
            clean_mask: Some(mask),
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(result.diagnostics.clean_mask_pixels, 64);
        assert_eq!(result.model[(40, 24, 0, 0)], 0.0);
    }

    #[test]
    fn empty_clean_mask_stops_without_cleanable_pixels() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let mask = Array2::<bool>::from_elem((64, 64), false);
        let result = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                ..CleanConfig::default()
            },
            clean_mask: Some(mask),
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(
            result.diagnostics.clean_stop_reason,
            Some(CleanStopReason::NoCleanablePixels)
        );
        assert_eq!(result.diagnostics.minor_iterations, 0);
    }

    #[test]
    fn higher_cyclefactor_triggers_more_major_cycles() {
        let samples = vec![
            (-140.0, -110.0, 0.0),
            (-80.0, 60.0, 0.0),
            (45.0, -95.0, 0.0),
            (120.0, 70.0, 0.0),
            (0.0, 0.0, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(&samples, 1.0e-4, [64, 64], (32.0, 32.0), 1.0);
        let relaxed = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 12,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 12,
                cyclefactor: 0.5,
                min_psf_fraction: 0.01,
                max_psf_fraction: 0.4,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let strict = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 12,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 12,
                cyclefactor: 3.0,
                min_psf_fraction: 0.4,
                max_psf_fraction: 0.9,
            },
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(strict.diagnostics.major_cycles > relaxed.diagnostics.major_cycles);
        assert!(
            strict.diagnostics.final_cycle_threshold_jy_per_beam
                > relaxed.diagnostics.final_cycle_threshold_jy_per_beam
        );
    }

    #[test]
    fn non_gridable_samples_do_not_change_dirty_image() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let cross_only = VisibilityBatch {
            u_lambda: vec![15.0],
            v_lambda: vec![-20.0],
            w_lambda: vec![0.0],
            weight: vec![1.0],
            sumwt_factor: vec![1.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(1.0, 0.0)],
        };
        let with_auto = VisibilityBatch {
            u_lambda: vec![0.0, 15.0],
            v_lambda: vec![0.0, -20.0],
            w_lambda: vec![0.0, 0.0],
            weight: vec![50.0, 1.0],
            sumwt_factor: vec![1.0, 1.0],
            gridable: vec![false, true],
            visibility: vec![Complex32::new(50.0, 0.0), Complex32::new(1.0, 0.0)],
        };

        for w_term_mode in [WTermMode::None, WTermMode::Direct] {
            let request = ImagingRequest {
                geometry,
                visibility_batches: vec![cross_only.clone()],
                plane_stokes: PlaneStokes::I,
                weighting: WeightingMode::Natural,
                reffreq_hz: 1.4e9,
                selected_frequency_range_hz: [1.399e9, 1.401e9],
                deconvolver: Deconvolver::Hogbom,
                multiscale_scales: Vec::new(),
                small_scale_bias: 0.0,
                clean: CleanConfig::default(),
                clean_mask: None,
                w_term_mode,
                compatibility: CompatibilityMode::CasaStandardMfs,
            };
            let baseline = run_imaging(&request).unwrap();
            let mut with_auto_request = request.clone();
            with_auto_request.visibility_batches = vec![with_auto.clone()];
            let with_extra = run_imaging(&with_auto_request).unwrap();

            for (expected, actual) in baseline.residual.iter().zip(with_extra.residual.iter()) {
                assert!(
                    (expected - actual).abs() < 1.0e-5,
                    "residual mismatch for {w_term_mode:?}: expected={expected}, actual={actual}"
                );
            }
        }
    }

    #[test]
    fn direct_w_term_mode_recovers_off_axis_sources_better_than_2d_mode() {
        let samples = vec![
            (-80.0, -45.0, 300.0),
            (-50.0, 35.0, -225.0),
            (37.5, -52.5, 262.5),
            (65.0, 42.5, -275.0),
            (20.0, 15.0, 175.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [4.0e-3, 4.0e-3],
        };
        let batch =
            point_source_visibilities_with_w_term(&samples, 4.0e-3, [64, 64], (42.0, 20.0), 1.0);
        let two_d = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let direct = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: WTermMode::Direct,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(direct.residual[(42, 20, 0, 0)] > two_d.residual[(42, 20, 0, 0)]);
        assert!(
            direct.diagnostics.final_residual_peak_jy_per_beam
                > two_d.diagnostics.final_residual_peak_jy_per_beam
        );
    }

    #[test]
    fn tolerant_clean_stop_reason_prefers_absolute_threshold_before_nsigma() {
        assert_eq!(
            tolerant_clean_stop_reason(0.995, 1.0, 2.0),
            Some(CleanStopReason::GlobalThresholdReached)
        );
        assert_eq!(
            tolerant_clean_stop_reason(1.5, 1.0, 2.0),
            Some(CleanStopReason::NsigmaThresholdReached)
        );
        assert_eq!(tolerant_clean_stop_reason(2.5, 1.0, 2.0), None);
    }

    #[test]
    fn compute_cycle_threshold_uses_psf_fraction_only() {
        let clean = CleanConfig {
            niter: 10,
            gain: 0.1,
            threshold_jy_per_beam: 0.5,
            nsigma: 5.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 5,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
        };
        let cycle_threshold = compute_cycle_threshold(10.0, 0.02, clean);
        assert_eq!(cycle_threshold, 0.5);
    }

    fn chauvenet_reference_fixture() -> Vec<f32> {
        vec![
            -2.61279178,
            -2.59342551,
            -2.16943479,
            -2.13970494,
            -1.91509378,
            -1.91133809,
            -1.84780550,
            -1.67959487,
            -1.55754685,
            -1.49124575,
            -1.47779667,
            -1.38040781,
            -1.37083769,
            -1.34913635,
            -1.29416192,
            -1.10022914,
            -1.07126451,
            -1.05194223,
            -1.03733921,
            -1.02524054,
            -0.984085381,
            -0.946198046,
            -0.923078358,
            -0.921401978,
            -0.876483500,
            -0.860657215,
            -0.826754928,
            -0.759524405,
            -0.736167967,
            -0.676235080,
            -0.672010839,
            -0.633015037,
            -0.591541886,
            -0.587743282,
            -0.528600693,
            -0.503111005,
            -0.484272331,
            -0.387220532,
            -0.362094551,
            -0.312986404,
            -0.301742464,
            -0.286407530,
            -0.277583510,
            -0.237437248,
            -0.237364024,
            -0.235247806,
            -0.211185545,
            -0.192734912,
            -0.187121660,
            -0.177792773,
            -0.169995695,
            -0.145033970,
            -0.116942599,
            -0.0627262741,
            -0.0345510058,
            -0.0306752156,
            -0.0179617219,
            -0.0114524942,
            -0.00316955987,
            0.000729589257,
            0.124999344,
            0.212515876,
            0.250957519,
            0.279240131,
            0.281288683,
            0.305763662,
            0.311809599,
            0.340768367,
            0.351874888,
            0.391162097,
            0.458450705,
            0.482642174,
            0.496854514,
            0.720111370,
            0.722756803,
            0.725001752,
            0.835289240,
            0.846509099,
            0.893022776,
            0.900427580,
            0.917734325,
            0.918030262,
            1.04210591,
            1.05506992,
            1.09472048,
            1.15250385,
            1.16275501,
            1.21244884,
            1.22725236,
            1.31463480,
            1.33273876,
            1.57637489,
            1.58221984,
            1.65665936,
            1.80032420,
            1.91410339,
            2.02669597,
            2.08605909,
            2.09777880,
            2.21240473,
            3.5,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            1_000_000.0,
        ]
    }

    #[test]
    fn chauvenet_clipping_matches_casacore_reference_counts() {
        let mut no_iterations = chauvenet_reference_fixture();
        apply_chauvenet_clipping(&mut no_iterations, 3.5, 0);
        assert_eq!(no_iterations.len(), 106);
        assert!(
            (no_iterations
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max)
                - 8.0)
                .abs()
                < 1.0e-6
        );

        let mut one_iteration = chauvenet_reference_fixture();
        apply_chauvenet_clipping(&mut one_iteration, 3.5, 1);
        assert_eq!(one_iteration.len(), 104);
        assert!(
            (one_iteration
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max)
                - 6.0)
                .abs()
                < 1.0e-6
        );

        let mut until_converged = chauvenet_reference_fixture();
        apply_chauvenet_clipping(&mut until_converged, 3.5, -1);
        assert_eq!(until_converged.len(), 102);
        assert!(
            (until_converged
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max)
                - 4.0)
                .abs()
                < 1.0e-6
        );

        let mut automatic_zscore = chauvenet_reference_fixture();
        apply_chauvenet_clipping(&mut automatic_zscore, -1.0, -1);
        assert_eq!(automatic_zscore.len(), 100);
        assert!(
            (automatic_zscore
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max)
                - 2.21240473)
                .abs()
                < 1.0e-6
        );
    }

    #[test]
    fn mean_stddev_uses_sample_variance_like_casacore_statsframework() {
        let values = [1.0_f32, 2.0, 3.0, 4.0];
        let (mean, stddev) = mean_stddev(&values);
        assert!((mean - 2.5).abs() < 1.0e-12);
        assert!((stddev - 1.290_994_448_735_805_6).abs() < 1.0e-12);
    }
}
