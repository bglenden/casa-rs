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

use std::collections::BTreeMap;

mod beam;
mod cube;
mod error;
mod execution;
mod fft;
mod gridder;
mod profile;
mod trace;
mod types;
mod weighting;

use std::{
    env,
    fs::OpenOptions,
    io::Write,
    thread,
    time::{Duration, Instant},
};

use casa_images::ImageBeamSet;
use casa_lattices::array_madfm;
use libm::{erfc, j1};
use ndarray::{Array2, Array4, Zip, s};
use num_complex::{Complex32, Complex64};

use beam::{
    BeamFitOutcome, beamfit_to_gaussian, estimate_psf_sidelobe_level, fit_beam_from_psf,
    gaussian_to_beamfit, rescale_residual_to_restored_beam, restore_model,
};
#[cfg(target_os = "macos")]
use execution::StandardMfsMetalExecutor;
use execution::{
    StandardMfsCpuExecutor, StandardMfsDirtyAccumulation, StandardMfsDirtyCpuExecutor,
    StandardMfsPlannedSample, StandardMfsTiledCpuExecutor, StandardMfsTiledResidualAccumulation,
    StandardMfsVisibilityPlan, finite_visibility,
};
use fft::{centered_fft2, centered_ifft2, centered_ifft2_f64};
use gridder::{
    PlannedSample, PositiveTapSet, STANDARD_GRIDDER_SUPPORT, STANDARD_GRIDDER_TAP_COUNT,
    ScreenProjector, StandardGridder, StandardMfsTapCensus, StandardMfsTapSkipReason, TapAxisSpan,
    WProjectSamplePlan, WProjector, hetarray_screen_conv_size_for_support,
};
pub use weighting::StandardMfsStreamingWeightingPlan;
use weighting::{
    apply_weighting, apply_weighting_to_owned_batches, apply_weighting_with_density_source,
    fractional_bandwidth_from_frequency_range, trace_weighting_with_density_source,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CubePredictionLambdaMode {
    OutputChannel,
    ModelChannel,
}

type MosaicProjectorKey = ((u8, u64, u64), u64, u8);
type MosaicProjectorCache = BTreeMap<MosaicProjectorKey, ScreenProjector>;
const DEFAULT_STANDARD_MFS_EXECUTOR_MAX_SAMPLES: usize = 8_000_000;
const STANDARD_MFS_GRID_THREADS_ENV: &str = "CASA_RS_STANDARD_MFS_GRID_THREADS";
const STANDARD_MFS_BACKEND_ENV: &str = "CASA_RS_STANDARD_MFS_BACKEND";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StandardMfsBackendSelection {
    Cpu,
    FixedTile,
    Metal,
}

pub(crate) use cube::{HogbomMinorCycleOutcome, MinorCycleProbe};
pub use cube::{run_cube, run_dirty_cube};
pub(crate) use trace::{ResidualRefreshTraceInternal, ResidualSampleTraceInternal};
pub use trace::{
    trace_cube_channel_residual_refresh, trace_cube_channel_residual_refresh_model_channel_lambda,
    trace_cube_channel_w_project_plan, trace_cube_weighting, trace_residual_refresh,
    trace_w_project_plan, trace_weighting,
};

pub use error::ImagingError;
pub use types::{
    AxisKind, BeamFit, BeamFitDebugSummary, CleanConfig, CleanStopReason, CompatibilityMetadata,
    CompatibilityMode, CubeAutoMultiThresholdConfig, CubeChannelRequest, CubeImagingDiagnostics,
    CubeImagingRequest, CubeImagingResult, CubeModelChannelContribution,
    CubeModelInterpolationBatch, Deconvolver, GaussianUvTaper, GridderMode, HogbomIterationMode,
    ImageGeometry, ImagingDiagnostics, ImagingRequest, ImagingResult, ImagingStageTimings,
    MinorCycleTrace, MosaicGridderConfig, MtmfsRequest, MtmfsResult, ParallelHandBatch,
    PlaneStokes, PrimaryBeamModel, PsfBeamFitResult, ResidualRefreshDiagnostics,
    ResidualSampleDiagnostics, RestoringBeamMode, StandardMfsPlannedWeightedSample,
    StandardMfsWeightedSample, UvTaperSize, VisibilityBatch, VisibilityMetadataBatch,
    WProjectDiagnostics, WProjectKernelDiagnostics, WProjectSamplePlanDiagnostics,
    WProjectSkipReason, WProjectSkippedSampleDiagnostics, WTermMode, WeightDensityMode,
    WeightingDiagnostics, WeightingMode, WeightingSampleDiagnostics,
};

/// FFT-backed predictor for a standard MFS component model.
///
/// This mirrors the standard-gridder model prediction path used during major
/// cycle residual refreshes, but exposes only the per-sample model visibility
/// needed by frontends that persist a `MODEL_DATA` column.
pub struct StandardMfsModelPredictor {
    gridder: StandardGridder,
    model_grid: Option<Array2<Complex32>>,
}

/// Planner for bounded standard-MFS weighted sample blocks.
///
/// Frontends use this helper after applying natural/uniform/Briggs weighting to
/// convert compact weighted samples into the shared planned representation used
/// by the CPU gridders. The planner owns the private standard gridder state, so
/// callers do not need access to internal tap tables.
pub struct StandardMfsPlannedSampleBuilder {
    gridder: StandardGridder,
}

impl StandardMfsPlannedSampleBuilder {
    /// Build a planner for standard-MFS image geometry.
    pub fn new(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Ok(Self {
            gridder: StandardGridder::new(geometry)?,
        })
    }

    /// Plan one weighted sample, returning `None` when it does not grid.
    pub fn plan_sample(
        &self,
        sample: StandardMfsWeightedSample,
    ) -> Result<Option<StandardMfsPlannedWeightedSample>, ImagingError> {
        if !sample.gridable {
            return Ok(None);
        }
        if !(sample.weight.is_finite()
            && sample.weight > 0.0
            && sample.sumwt_factor.is_finite()
            && sample.sumwt_factor > 0.0)
        {
            return Ok(None);
        }
        let grid_weight = sample.weight * sample.sumwt_factor;
        if !(grid_weight.is_finite() && grid_weight > 0.0) {
            return Ok(None);
        }
        let Some(taps) = self
            .gridder
            .plan_positive_taps(sample.u_lambda, sample.v_lambda)
        else {
            return Ok(None);
        };
        let center = taps.center();
        let kernel_u = u16::try_from(taps.x.weight_index).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS planned sample x tap weight index exceeds u16".to_string(),
            )
        })?;
        let kernel_v = u16::try_from(taps.y.weight_index).map_err(|_| {
            ImagingError::InvalidRequest(
                "standard MFS planned sample y tap weight index exceeds u16".to_string(),
            )
        })?;
        let flags = if finite_visibility(sample.visibility) {
            StandardMfsPlannedWeightedSample::FINITE_VISIBILITY
        } else {
            StandardMfsPlannedWeightedSample::PSF_ONLY
        };
        Ok(Some(StandardMfsPlannedWeightedSample {
            center_x: center[0] as u32,
            center_y: center[1] as u32,
            kernel_u,
            kernel_v,
            support_id: 0,
            flags,
            tap_count: (STANDARD_GRIDDER_TAP_COUNT * STANDARD_GRIDDER_TAP_COUNT) as u8,
            grid_weight,
            w_lambda: sample.w_lambda,
            visibility: sample.visibility,
        }))
    }

    /// Plan a weighted row block into a caller-provided output buffer.
    pub fn plan_samples_into(
        &self,
        samples: &[StandardMfsWeightedSample],
        planned: &mut Vec<StandardMfsPlannedWeightedSample>,
    ) -> Result<usize, ImagingError> {
        let initial_len = planned.len();
        planned.reserve(samples.len());
        for &sample in samples {
            if let Some(planned_sample) = self.plan_sample(sample)? {
                planned.push(planned_sample);
            }
        }
        Ok(planned.len() - initial_len)
    }

    /// Plan an owned visibility batch into a caller-provided output buffer.
    pub fn plan_visibility_batch_into(
        &self,
        batch: &VisibilityBatch,
        planned: &mut Vec<StandardMfsPlannedWeightedSample>,
    ) -> Result<usize, ImagingError> {
        batch.validate()?;
        let initial_len = planned.len();
        planned.reserve(batch.len());
        for sample_index in 0..batch.len() {
            let sample = StandardMfsWeightedSample {
                u_lambda: batch.u_lambda[sample_index],
                v_lambda: batch.v_lambda[sample_index],
                w_lambda: batch.w_lambda[sample_index],
                weight: batch.weight[sample_index],
                sumwt_factor: batch.sumwt_factor[sample_index],
                gridable: batch.gridable[sample_index],
                visibility: batch.visibility[sample_index],
            };
            if let Some(planned_sample) = self.plan_sample(sample)? {
                planned.push(planned_sample);
            }
        }
        Ok(planned.len() - initial_len)
    }
}

/// Runtime execution knobs for standard-MFS backends.
///
/// These values are deliberately separate from [`ImagingRequest`] so callers
/// can tune memory residency without changing the imaging contract itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StandardMfsExecutionConfig {
    /// Optional byte budget for resident fixed-tile stage grids.
    ///
    /// When the fixed-tile backend is enabled, the backend converts this budget
    /// into a deterministic resident tile count from the actual padded grid and
    /// halo geometry. `CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT` remains an
    /// explicit debug override.
    pub fixed_tile_resident_bytes: Option<usize>,
    /// Optional fixed-tile interior edge selected by the frontend memory planner.
    ///
    /// `CASA_RS_STANDARD_MFS_TILE_EDGE` remains the explicit debug override.
    pub fixed_tile_edge: Option<usize>,
    /// Use the gridder-center boundary anchor when no tile-anchor env override is set.
    pub fixed_tile_center_boundary: bool,
    /// Maximum prepared row blocks the tiled scheduler may keep live.
    ///
    /// The first bounded scheduler uses one live row block; later read-ahead
    /// must be explicitly represented here and in the memory plan.
    pub fixed_tile_max_live_row_blocks: usize,
}

/// Metadata for streaming natural-weighted standard MFS dirty accumulation.
///
/// This request mirrors the dirty-only subset of [`ImagingRequest`] but leaves
/// visibility ownership with the caller. Frontends can therefore load one MS
/// chunk, feed its already-natural-weighted [`VisibilityBatch`] values into
/// [`StandardMfsDirtyAccumulator`], and release the chunk before reading the
/// next one.
#[derive(Debug, Clone, PartialEq)]
pub struct StandardMfsDirtyAccumulatorRequest {
    /// Requested image geometry for the MFS image plane.
    pub geometry: ImageGeometry,
    /// Scalar imaging plane to produce.
    pub plane_stokes: PlaneStokes,
    /// Reference frequency in Hz for metadata and diagnostics.
    pub reffreq_hz: f64,
    /// Inclusive selected frequency range in Hz.
    pub selected_frequency_range_hz: [f64; 2],
    /// Dirty-run clean controls, used for PSF fitting diagnostics and thresholds.
    pub clean: CleanConfig,
    /// Optional image-plane clean mask. `true` pixels are eligible for diagnostics.
    pub clean_mask: Option<Array2<bool>>,
}

/// Streaming standard-gridder MFS dirty accumulator for natural-weighted batches.
///
/// The accumulator preserves the dirty `run_imaging()` math for the supported
/// subset while avoiding a single in-memory `Vec<VisibilityBatch>` for large
/// MeasurementSets. It intentionally does not implement CLEAN, MT-MFS,
/// non-natural weighting, W-term handling, or mosaic/PB semantics.
pub struct StandardMfsDirtyAccumulator {
    request: StandardMfsDirtyAccumulatorRequest,
    executor: StandardMfsDirtyCpuExecutor,
    stage_timings: ImagingStageTimings,
    total_started: Instant,
}

impl StandardMfsDirtyAccumulator {
    /// Create an empty accumulator for one standard MFS dirty image.
    pub fn new(request: StandardMfsDirtyAccumulatorRequest) -> Result<Self, ImagingError> {
        request.geometry.validate()?;
        request.clean.validate()?;
        if request.clean.niter != 0 {
            return Err(ImagingError::Unsupported(
                "standard MFS dirty accumulator requires clean.niter=0".to_string(),
            ));
        }
        if !(request.reffreq_hz.is_finite() && request.reffreq_hz > 0.0) {
            return Err(ImagingError::InvalidRequest(
                "reffreq_hz must be a finite positive frequency".to_string(),
            ));
        }
        if !(request.selected_frequency_range_hz[0].is_finite()
            && request.selected_frequency_range_hz[1].is_finite()
            && request.selected_frequency_range_hz[0] > 0.0
            && request.selected_frequency_range_hz[1] >= request.selected_frequency_range_hz[0])
        {
            return Err(ImagingError::InvalidRequest(
                "selected_frequency_range_hz must be a finite ordered positive range".to_string(),
            ));
        }
        if let Some(mask) = &request.clean_mask {
            if mask.dim() != (request.geometry.nx(), request.geometry.ny()) {
                return Err(ImagingError::InvalidRequest(format!(
                    "clean mask shape {:?} does not match image shape {:?}",
                    mask.dim(),
                    (request.geometry.nx(), request.geometry.ny())
                )));
            }
        }
        let executor = StandardMfsDirtyCpuExecutor::new(request.geometry)?;
        Ok(Self {
            request,
            executor,
            stage_timings: ImagingStageTimings::default(),
            total_started: Instant::now(),
        })
    }

    /// Accumulate one or more already-natural-weighted visibility batches.
    pub fn accumulate_batches(&mut self, batches: &[VisibilityBatch]) -> Result<(), ImagingError> {
        let grid_started = Instant::now();
        self.executor.accumulate_batches(batches)?;
        let grid_elapsed = grid_started.elapsed();
        let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
        self.stage_timings.psf_grid += split_grid_elapsed;
        self.stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);
        Ok(())
    }

    /// Finish accumulation and return CASA-style dirty products and diagnostics.
    pub fn finish(mut self) -> Result<ImagingResult, ImagingError> {
        let accumulation = self.executor.accumulation();
        if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
            return Err(ImagingError::NoUsableSamples);
        }
        let (psf_grid, residual_grid) = self.executor.dirty_grids();
        let psf_fft_started = Instant::now();
        let raw_psf = centered_ifft2_f64(psf_grid);
        self.stage_timings.psf_fft += psf_fft_started.elapsed();
        let psf_normalize_started = Instant::now();
        let mut psf = self
            .executor
            .gridder()
            .corrected_image_from_grid_f64(&raw_psf);
        psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
        let psf_peak = peak_abs_value(&psf);
        if !(psf_peak.is_finite() && psf_peak > 0.0) {
            return Err(ImagingError::Normalization(
                "PSF peak is non-finite or zero".to_string(),
            ));
        }
        psf.mapv_inplace(|value| value / psf_peak);
        self.stage_timings.psf_normalize += psf_normalize_started.elapsed();

        let residual_fft_started = Instant::now();
        let raw_residual = centered_ifft2_f64(residual_grid);
        self.stage_timings.residual_fft += residual_fft_started.elapsed();
        let residual_normalize_started = Instant::now();
        let mut residual = self
            .executor
            .gridder()
            .corrected_image_from_grid_f64(&raw_residual);
        residual.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32 / psf_peak);
        self.stage_timings.residual_normalize += residual_normalize_started.elapsed();

        let max_psf_sidelobe_level = estimate_psf_sidelobe_level(
            &psf,
            self.request.geometry.cell_size_rad,
            self.request.clean.psf_cutoff,
        );
        let [nx, ny] = self.request.geometry.image_shape;
        let model = Array2::<f32>::zeros((nx, ny));
        let clean_mask_pixels = self
            .request
            .clean_mask
            .as_ref()
            .map(|mask| mask.iter().filter(|value| **value).count())
            .unwrap_or(nx * ny);
        let initial_peak = peak_abs_value_masked(&residual, self.request.clean_mask.as_ref());
        let mut warnings = Vec::new();
        let fractional_bandwidth = (self.request.selected_frequency_range_hz[1]
            - self.request.selected_frequency_range_hz[0])
            / self.request.reffreq_hz;
        if fractional_bandwidth > 0.1 {
            warnings.push(format!(
                "fractional bandwidth {:.3} exceeds the narrow-band nterms=1 comfort zone",
                fractional_bandwidth
            ));
        }
        let w_phase_metric =
            accumulation.max_abs_w_lambda * self.request.geometry.field_of_view_rad().powi(2);
        if w_phase_metric > 0.1 {
            warnings.push(format!(
                "max |w| * fov^2 = {:.3} suggests 2-D standard imaging may show non-coplanar artifacts",
                w_phase_metric
            ));
        }

        let beam_fit_started = Instant::now();
        let BeamFitOutcome {
            beam,
            warnings: beam_warnings,
            attempts: beam_fit_attempts,
            cutoff_used: beam_fit_cutoff_used,
            debug: beam_fit_debug,
        } = fit_beam_from_psf(
            &psf,
            self.request.geometry.cell_size_rad,
            self.request.clean.psf_cutoff,
        );
        self.stage_timings.beam_fit += beam_fit_started.elapsed();
        let restore_started = Instant::now();
        let restored_model = restore_model(&model, self.request.geometry.cell_size_rad, beam);
        self.stage_timings.restore += restore_started.elapsed();
        let restored_image = &restored_model + &residual;

        warnings.extend(beam_warnings);
        self.stage_timings.total = self.total_started.elapsed();

        Ok(ImagingResult {
            psf: expand_plane(&psf),
            residual: expand_plane(&residual),
            model: expand_plane(&model),
            image: expand_plane(&restored_image),
            sumwt: expand_scalar(accumulation.reported_sumwt as f32),
            beam,
            diagnostics: ImagingDiagnostics {
                warnings,
                gridded_samples: accumulation.gridded_samples,
                skipped_samples: accumulation.skipped_samples,
                major_cycles: 0,
                minor_iterations: 0,
                clean_stop_reason: None,
                minor_cycle_traces: Vec::new(),
                initial_residual_peak_jy_per_beam: initial_peak,
                final_residual_peak_jy_per_beam: initial_peak,
                max_abs_w_lambda: accumulation.max_abs_w_lambda,
                fractional_bandwidth,
                max_psf_sidelobe_level,
                final_cycle_threshold_jy_per_beam: self.request.clean.threshold_jy_per_beam,
                clean_mask_pixels,
                beam_fit_attempts,
                beam_fit_cutoff_used,
                beam_fit_debug,
                mosaic_weight_image: None,
                stage_timings: self.stage_timings,
            },
            compatibility: CompatibilityMetadata {
                axis_order: [
                    AxisKind::RightAscension,
                    AxisKind::Declination,
                    AxisKind::Stokes,
                    AxisKind::Frequency,
                ],
                plane_stokes: self.request.plane_stokes,
                reffreq_hz: self.request.reffreq_hz,
                channel_frequencies_hz: vec![self.request.reffreq_hz],
                psf_units: String::new(),
                residual_units: "Jy/beam".to_string(),
                model_units: "Jy/pixel".to_string(),
                image_units: "Jy/beam".to_string(),
            },
        })
    }
}

impl StandardMfsModelPredictor {
    /// Build a predictor for one image geometry and final model plane.
    pub fn new(geometry: ImageGeometry, model: &Array2<f32>) -> Result<Self, ImagingError> {
        let gridder = StandardGridder::new_with_casa_composite_padding(geometry)?;
        let model_has_components = model.iter().any(|value| value.abs() > 0.0);
        let model_grid = model_has_components.then(|| centered_fft2(&gridder.apodize_model(model)));
        Ok(Self {
            gridder,
            model_grid,
        })
    }

    /// Predict the model visibility at one `(u, v)` coordinate in wavelengths.
    pub fn predict(&self, u_lambda: f64, v_lambda: f64) -> Complex32 {
        let Some(model_grid) = self.model_grid.as_ref() else {
            return Complex32::new(0.0, 0.0);
        };
        if let Some(predicted) = self
            .gridder
            .degrid_sample_product_planned_sectdgrid(model_grid, u_lambda, v_lambda)
        {
            return predicted;
        }
        let Some(plan) = self.gridder.plan_sample(u_lambda, v_lambda) else {
            return Complex32::new(0.0, 0.0);
        };
        self.gridder
            .degrid_sample_product_planned(model_grid, &plan.positive)
    }
}

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

/// Restore a standard MFS component model with an already-selected restoring beam.
///
/// Frontends that orchestrate more than one image plane can use this to build
/// the same restored-image product that [`run_imaging`] writes after its
/// internal Cotton-Schwab controller finishes.
pub fn restore_standard_mfs_model(
    model: &Array2<f32>,
    cell_size_rad: [f64; 2],
    beam: Option<BeamFit>,
) -> Array2<f32> {
    restore_model(model, cell_size_rad, beam)
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
    if request.deconvolver == Deconvolver::Mtmfs {
        return Err(ImagingError::Unsupported(
            "deconvolver='mtmfs' requires the dedicated run_mtmfs() entrypoint".to_string(),
        ));
    }
    ensure_standard_mfs_backend_available()?;
    if let GridderMode::Mosaic(config) = &request.gridder_mode {
        return run_mosaic_dirty_imaging(request, config, total_started);
    }

    let gridder = StandardGridder::new(request.geometry)?;
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let weighted_batches = apply_weighting(request, &gridder)?;
    stage_timings.weighting += weighting_started.elapsed();
    run_standard_mfs_imaging_with_weighted_batches(
        request,
        &weighted_batches,
        &gridder,
        StandardMfsExecutionConfig::default(),
        stage_timings,
        total_started,
    )
}

/// Run CASA-style standard MFS imaging while consuming the request's visibility batches.
///
/// This preserves the borrowed [`run_imaging`] API for general callers, but
/// lets frontend owners pass prepared batches through weighting without
/// cloning the full visibility payload.
pub fn run_imaging_owned(request: ImagingRequest) -> Result<ImagingResult, ImagingError> {
    run_imaging_owned_with_execution_config(request, StandardMfsExecutionConfig::default())
}

/// Run CASA-style standard MFS imaging with caller-supplied backend execution knobs.
pub fn run_imaging_owned_with_execution_config(
    mut request: ImagingRequest,
    execution_config: StandardMfsExecutionConfig,
) -> Result<ImagingResult, ImagingError> {
    let total_started = Instant::now();
    request.validate()?;
    if request.compatibility != CompatibilityMode::CasaStandardMfs {
        return Err(ImagingError::Unsupported(
            "only CASA standard MFS compatibility mode is implemented".to_string(),
        ));
    }
    if request.deconvolver == Deconvolver::Mtmfs {
        return Err(ImagingError::Unsupported(
            "deconvolver='mtmfs' requires the dedicated run_mtmfs() entrypoint".to_string(),
        ));
    }
    ensure_standard_mfs_backend_available()?;
    if let GridderMode::Mosaic(config) = &request.gridder_mode {
        return run_mosaic_dirty_imaging(&request, config, total_started);
    }

    let gridder = StandardGridder::new(request.geometry)?;
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let source_batches = std::mem::take(&mut request.visibility_batches);
    let weighted_batches = apply_weighting_to_owned_batches(&request, &gridder, source_batches)?;
    stage_timings.weighting += weighting_started.elapsed();
    request.visibility_batches = weighted_batches;
    run_standard_mfs_imaging_with_weighted_batches(
        &request,
        &request.visibility_batches,
        &gridder,
        execution_config,
        stage_timings,
        total_started,
    )
}

fn run_standard_mfs_imaging_with_weighted_batches(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    mut stage_timings: ImagingStageTimings,
    total_started: Instant,
) -> Result<ImagingResult, ImagingError> {
    let use_metal_executor = should_use_standard_mfs_metal_backend(request);
    let use_tiled_executor = should_use_standard_mfs_tiled_backend(request);
    let mut standard_executor = (!(use_metal_executor || use_tiled_executor)
        && should_use_standard_mfs_executor(request, weighted_batches))
    .then(|| StandardMfsCpuExecutor::new(gridder, weighted_batches))
    .transpose()?;
    let [nx, ny] = request.geometry.image_shape;
    let mut model = request
        .initial_model
        .clone()
        .unwrap_or_else(|| Array2::<f32>::zeros((nx, ny)));
    let has_initial_model = request.initial_model.is_some();
    let can_start_from_combined_dirty_pass =
        !has_initial_model && matches!(request.w_term_mode, WTermMode::None);
    let (psf_state, mut residual) = if can_start_from_combined_dirty_pass {
        if use_metal_executor {
            compute_dirty_psf_and_residual_standard_metal(
                weighted_batches,
                gridder,
                execution_config,
                &mut stage_timings,
            )?
        } else if use_tiled_executor {
            compute_dirty_psf_and_residual_standard_tiled(
                weighted_batches,
                gridder,
                execution_config,
                &mut stage_timings,
            )?
        } else if let Some(executor) = standard_executor.as_mut() {
            compute_dirty_psf_and_residual_standard_with_executor(executor, &mut stage_timings)?
        } else {
            compute_dirty_psf_and_residual_standard_streaming(
                weighted_batches,
                gridder,
                &mut stage_timings,
            )?
        }
    } else {
        let psf_state = if use_metal_executor {
            compute_psf_standard_metal(
                weighted_batches,
                gridder,
                execution_config,
                &mut stage_timings,
            )?
        } else if use_tiled_executor {
            compute_psf_standard_tiled(
                weighted_batches,
                gridder,
                execution_config,
                &mut stage_timings,
            )?
        } else if let Some(executor) = standard_executor.as_mut() {
            compute_psf_standard(executor, &mut stage_timings)?
        } else {
            compute_psf_standard_streaming(weighted_batches, gridder, &mut stage_timings)?
        };
        let residual = if use_metal_executor || use_tiled_executor {
            compute_residual_standard_tiled(
                weighted_batches,
                gridder,
                &model,
                &psf_state,
                execution_config,
                &mut stage_timings,
            )?
        } else if let Some(executor) = standard_executor.as_mut() {
            compute_residual_standard_with_executor(
                executor,
                &model,
                &psf_state,
                &mut stage_timings,
            )?
        } else {
            compute_residual_standard_streaming(
                request.geometry,
                weighted_batches,
                gridder,
                &model,
                &psf_state,
                false,
                &mut stage_timings,
            )?
        };
        (psf_state, residual)
    };
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
        weighted_batches,
        &mut standard_executor,
        gridder,
        &psf_state,
        execution_config,
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

    let max_abs_w_lambda = weighted_batches
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
            mosaic_weight_image: None,
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

/// Run standard-MFS CLEAN from replayable weighted row-block batches.
///
/// The callback is invoked once for the initial dirty/PSF pass and once for
/// each exact residual-refresh pass. Each invocation must stream batches in a
/// stable MeasurementSet order and pass ownership of each bounded row block to
/// the supplied consumer.
pub fn run_standard_mfs_weighted_streaming_with_execution_config<F>(
    mut request: ImagingRequest,
    execution_config: StandardMfsExecutionConfig,
    mut replay_weighted_batches: F,
) -> Result<ImagingResult, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let total_started = Instant::now();
    request.geometry.validate()?;
    request.clean.validate()?;
    if !matches!(request.gridder_mode, GridderMode::Standard) {
        return Err(ImagingError::Unsupported(
            "streaming standard MFS clean requires gridder='standard'".to_string(),
        ));
    }
    if !matches!(request.w_term_mode, WTermMode::None) {
        return Err(ImagingError::Unsupported(
            "streaming standard MFS clean does not support W-term gridding".to_string(),
        ));
    }
    if request.deconvolver == Deconvolver::Mtmfs {
        return Err(ImagingError::Unsupported(
            "streaming standard MFS clean does not support deconvolver='mtmfs'".to_string(),
        ));
    }
    let gridder = StandardGridder::new(request.geometry)?;
    let [nx, ny] = request.geometry.image_shape;
    let mut model = request
        .initial_model
        .take()
        .unwrap_or_else(|| Array2::<f32>::zeros((nx, ny)));
    let has_initial_model = model.iter().any(|value| value.abs() > 0.0);
    let mut stage_timings = ImagingStageTimings::default();

    let (psf_state, mut residual, max_abs_w_lambda) = if !has_initial_model {
        compute_dirty_psf_and_residual_standard_tiled_replay(
            &gridder,
            execution_config,
            &mut replay_weighted_batches,
            &mut stage_timings,
        )?
    } else {
        let psf_state = compute_psf_standard_tiled_replay(
            &gridder,
            execution_config,
            &mut replay_weighted_batches,
            &mut stage_timings,
        )?;
        let residual = compute_residual_standard_tiled_replay(
            request.geometry,
            &gridder,
            &model,
            &psf_state,
            execution_config,
            &mut replay_weighted_batches,
            &mut stage_timings,
        )?;
        (psf_state, residual, 0.0)
    };
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

    let mut refresh_residual = |model: &Array2<f32>,
                                stage_timings: &mut ImagingStageTimings|
     -> Result<Array2<f32>, ImagingError> {
        let before = ResidualRefreshTimingSnapshot::capture(stage_timings);
        let refresh_started = Instant::now();
        let residual = compute_residual_standard_tiled_replay(
            request.geometry,
            &gridder,
            model,
            &psf_state,
            execution_config,
            &mut replay_weighted_batches,
            stage_timings,
        )?;
        let refresh_elapsed = refresh_started.elapsed();
        let accounted = before.accounted_delta(stage_timings);
        stage_timings.major_cycle_refresh += refresh_elapsed;
        stage_timings.residual_refresh_overhead += refresh_elapsed.saturating_sub(accounted);
        Ok(residual)
    };

    let controller_started = Instant::now();
    let clean_state = run_cotton_schwab_controller_with_refresh(
        &request,
        &psf_state,
        &mut stage_timings,
        &mut refresh_residual,
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
            mosaic_weight_image: None,
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

/// Run standard-MFS CLEAN from replayable weighted samples.
///
/// This is the sample-stream equivalent of
/// [`run_standard_mfs_weighted_streaming_with_execution_config`]. The callback
/// is invoked once for the initial dirty/PSF pass and once for each exact
/// residual-refresh pass. Each invocation must stream samples in a stable
/// MeasurementSet order.
pub fn run_standard_mfs_weighted_sample_streaming_with_execution_config<F>(
    request: ImagingRequest,
    execution_config: StandardMfsExecutionConfig,
    mut replay_weighted_samples: F,
) -> Result<ImagingResult, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(StandardMfsWeightedSample) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    run_standard_mfs_weighted_sample_block_streaming_with_execution_config(
        request,
        execution_config,
        |consume_blocks| {
            replay_weighted_samples(&mut |sample| consume_blocks(std::slice::from_ref(&sample)))
        },
    )
}

/// Run standard-MFS CLEAN from replayable blocks of weighted samples.
///
/// This is the preferred frontend/core boundary for trace-free streaming: the
/// frontend can hand one bounded row block of compact weighted samples to the
/// core, avoiding a dynamic callback crossing for every scalar sample.
pub fn run_standard_mfs_weighted_sample_block_streaming_with_execution_config<F>(
    request: ImagingRequest,
    execution_config: StandardMfsExecutionConfig,
    mut replay_weighted_samples: F,
) -> Result<ImagingResult, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(&[StandardMfsWeightedSample]) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let planner = StandardMfsPlannedSampleBuilder::new(request.geometry)?;
    let mut planned = Vec::<StandardMfsPlannedWeightedSample>::new();
    run_standard_mfs_planned_sample_block_streaming_with_execution_config(
        request,
        execution_config,
        |consume_planned| {
            replay_weighted_samples(&mut |weighted| {
                planned.clear();
                planner.plan_samples_into(weighted, &mut planned)?;
                consume_planned(&planned)
            })
        },
    )
}

/// Run standard-MFS CLEAN from replayable blocks of planned weighted samples.
///
/// Planned sample blocks are the shared single-worker and fixed-tile work-unit
/// shape: they are bounded to row-block lifetime and already carry compact
/// standard-gridder tap identity.
pub fn run_standard_mfs_planned_sample_block_streaming_with_execution_config<F>(
    mut request: ImagingRequest,
    _execution_config: StandardMfsExecutionConfig,
    mut replay_weighted_samples: F,
) -> Result<ImagingResult, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let total_started = Instant::now();
    request.geometry.validate()?;
    request.clean.validate()?;
    if !matches!(request.gridder_mode, GridderMode::Standard) {
        return Err(ImagingError::Unsupported(
            "sample streaming standard MFS clean requires gridder='standard'".to_string(),
        ));
    }
    if !matches!(request.w_term_mode, WTermMode::None) {
        return Err(ImagingError::Unsupported(
            "sample streaming standard MFS clean does not support W-term gridding".to_string(),
        ));
    }
    if request.deconvolver == Deconvolver::Mtmfs {
        return Err(ImagingError::Unsupported(
            "sample streaming standard MFS clean does not support deconvolver='mtmfs'".to_string(),
        ));
    }
    let gridder = StandardGridder::new(request.geometry)?;
    let [nx, ny] = request.geometry.image_shape;
    let mut model = request
        .initial_model
        .take()
        .unwrap_or_else(|| Array2::<f32>::zeros((nx, ny)));
    let has_initial_model = model.iter().any(|value| value.abs() > 0.0);
    let mut stage_timings = ImagingStageTimings::default();

    let (psf_state, mut residual, max_abs_w_lambda) = if !has_initial_model {
        compute_dirty_psf_and_residual_standard_sample_replay(
            &gridder,
            &mut replay_weighted_samples,
            &mut stage_timings,
        )?
    } else {
        let psf_state = compute_psf_standard_sample_replay(
            &gridder,
            &mut replay_weighted_samples,
            &mut stage_timings,
        )?;
        let residual = compute_residual_standard_sample_replay(
            request.geometry,
            &gridder,
            &model,
            &psf_state,
            &mut replay_weighted_samples,
            &mut stage_timings,
        )?;
        (psf_state, residual, 0.0)
    };
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

    let mut refresh_residual = |model: &Array2<f32>,
                                stage_timings: &mut ImagingStageTimings|
     -> Result<Array2<f32>, ImagingError> {
        let before = ResidualRefreshTimingSnapshot::capture(stage_timings);
        let refresh_started = Instant::now();
        let residual = compute_residual_standard_sample_replay(
            request.geometry,
            &gridder,
            model,
            &psf_state,
            &mut replay_weighted_samples,
            stage_timings,
        )?;
        let refresh_elapsed = refresh_started.elapsed();
        let accounted = before.accounted_delta(stage_timings);
        stage_timings.major_cycle_refresh += refresh_elapsed;
        stage_timings.residual_refresh_overhead += refresh_elapsed.saturating_sub(accounted);
        Ok(residual)
    };

    let controller_started = Instant::now();
    let clean_state = run_cotton_schwab_controller_with_refresh(
        &request,
        &psf_state,
        &mut stage_timings,
        &mut refresh_residual,
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
            mosaic_weight_image: None,
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

fn accumulate_dirty_accumulation(
    target: &mut StandardMfsDirtyAccumulation,
    source: StandardMfsDirtyAccumulation,
) {
    target.normalization_sumwt += source.normalization_sumwt;
    target.reported_sumwt += source.reported_sumwt;
    target.gridded_samples += source.gridded_samples;
    target.skipped_samples += source.skipped_samples;
    target.max_abs_w_lambda = target.max_abs_w_lambda.max(source.max_abs_w_lambda);
}

fn accumulate_residual_accumulation(
    target: &mut StandardMfsTiledResidualAccumulation,
    source: StandardMfsTiledResidualAccumulation,
) {
    target.valid_samples += source.valid_samples;
    target.planned_samples += source.planned_samples;
    target.gridded_residual_samples += source.gridded_residual_samples;
    target.skipped_not_gridable += source.skipped_not_gridable;
    target.skipped_invalid_weight += source.skipped_invalid_weight;
    target.skipped_invalid_sumwt += source.skipped_invalid_sumwt;
    target.skipped_out_of_grid += source.skipped_out_of_grid;
    target.skipped_nonfinite_visibility += source.skipped_nonfinite_visibility;
}

fn compute_dirty_psf_and_residual_standard_tiled_replay<F>(
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    replay_weighted_batches: &mut F,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>, f64), ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut residual_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let executor =
        StandardMfsTiledCpuExecutor::new_with_execution_config(gridder, execution_config)?;
    let mut accumulation = StandardMfsDirtyAccumulation {
        normalization_sumwt: 0.0,
        reported_sumwt: 0.0,
        gridded_samples: 0,
        skipped_samples: 0,
        max_abs_w_lambda: 0.0,
    };

    let grid_started = Instant::now();
    if executor.has_all_resident_tiles() {
        accumulation = executor.accumulate_dirty_grids_direct_owned_replay(
            replay_weighted_batches,
            &mut psf_grid,
            &mut residual_grid,
            execution_config.fixed_tile_max_live_row_blocks,
        )?;
    } else {
        replay_weighted_batches(&mut |batches| {
            let block_accumulation =
                executor.accumulate_dirty_grids(&batches, &mut psf_grid, &mut residual_grid)?;
            accumulate_dirty_accumulation(&mut accumulation, block_accumulation);
            Ok(())
        })?;
    }
    let grid_elapsed = grid_started.elapsed();
    let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
    stage_timings.psf_grid += split_grid_elapsed;
    stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);

    dirty_grids_to_psf_and_residual(
        gridder,
        &psf_grid,
        &residual_grid,
        accumulation,
        stage_timings,
    )
    .map(|(psf_state, residual)| (psf_state, residual, accumulation.max_abs_w_lambda))
}

#[inline(always)]
fn planned_sample_positive_taps(
    sample: StandardMfsPlannedWeightedSample,
) -> Result<PositiveTapSet, ImagingError> {
    if sample.support_id != 0 {
        return Err(ImagingError::InvalidRequest(format!(
            "standard MFS planned sample has unsupported tap support id {}",
            sample.support_id
        )));
    }
    let center_x = sample.center_x as usize;
    let center_y = sample.center_y as usize;
    if center_x < STANDARD_GRIDDER_SUPPORT {
        return Err(ImagingError::InvalidRequest(
            "standard MFS planned sample has invalid x tap center".to_string(),
        ));
    }
    if center_y < STANDARD_GRIDDER_SUPPORT {
        return Err(ImagingError::InvalidRequest(
            "standard MFS planned sample has invalid y tap center".to_string(),
        ));
    }
    Ok(PositiveTapSet {
        x: TapAxisSpan {
            start: center_x - STANDARD_GRIDDER_SUPPORT,
            weight_index: usize::from(sample.kernel_u),
        },
        y: TapAxisSpan {
            start: center_y - STANDARD_GRIDDER_SUPPORT,
            weight_index: usize::from(sample.kernel_v),
        },
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PlannedCompactTaps {
    x_start: usize,
    y_start: usize,
    x_weight_index: usize,
    y_weight_index: usize,
}

#[inline(always)]
fn planned_sample_compact_taps(
    sample: StandardMfsPlannedWeightedSample,
) -> Result<PlannedCompactTaps, ImagingError> {
    if sample.support_id != 0 {
        return Err(ImagingError::InvalidRequest(format!(
            "standard MFS planned sample has unsupported tap support id {}",
            sample.support_id
        )));
    }
    let center_x = sample.center_x as usize;
    let center_y = sample.center_y as usize;
    if center_x < STANDARD_GRIDDER_SUPPORT {
        return Err(ImagingError::InvalidRequest(
            "standard MFS planned sample has invalid x tap center".to_string(),
        ));
    }
    if center_y < STANDARD_GRIDDER_SUPPORT {
        return Err(ImagingError::InvalidRequest(
            "standard MFS planned sample has invalid y tap center".to_string(),
        ));
    }
    Ok(PlannedCompactTaps {
        x_start: center_x - STANDARD_GRIDDER_SUPPORT,
        y_start: center_y - STANDARD_GRIDDER_SUPPORT,
        x_weight_index: usize::from(sample.kernel_u),
        y_weight_index: usize::from(sample.kernel_v),
    })
}

fn compute_dirty_psf_and_residual_standard_sample_replay<F>(
    gridder: &StandardGridder,
    replay_weighted_samples: &mut F,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>, f64), ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut residual_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut accumulation = StandardMfsDirtyAccumulation {
        normalization_sumwt: 0.0,
        reported_sumwt: 0.0,
        gridded_samples: 0,
        skipped_samples: 0,
        max_abs_w_lambda: 0.0,
    };

    let grid_started = Instant::now();
    if let (Some(psf_storage), Some(residual_storage)) = (
        psf_grid.as_slice_memory_order_mut(),
        residual_grid.as_slice_memory_order_mut(),
    ) {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let taps = planned_sample_compact_taps(sample)?;
                let grid_weight = sample.grid_weight;
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                if sample.finite_visibility() {
                    let residual = Complex64::new(
                        f64::from(sample.visibility.re) * grid_weight,
                        f64::from(sample.visibility.im) * grid_weight,
                    );
                    gridder.grid_compact_taps_real_complex_pair_planned_f64_storage(
                        psf_storage,
                        grid_weight,
                        residual_storage,
                        residual,
                        taps.x_start,
                        taps.y_start,
                        taps.x_weight_index,
                        taps.y_weight_index,
                    );
                } else {
                    gridder.grid_compact_taps_real_planned_f64_storage(
                        psf_storage,
                        grid_weight,
                        taps.x_start,
                        taps.y_start,
                        taps.x_weight_index,
                        taps.y_weight_index,
                    );
                }
            }
            Ok(())
        })?;
    } else {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let taps = planned_sample_positive_taps(sample)?;
                let grid_weight = sample.grid_weight;
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                if sample.finite_visibility() {
                    let residual = Complex64::new(
                        f64::from(sample.visibility.re) * grid_weight,
                        f64::from(sample.visibility.im) * grid_weight,
                    );
                    gridder.grid_sample_taps_real_complex_pair_planned_f64(
                        &mut psf_grid,
                        grid_weight,
                        &mut residual_grid,
                        residual,
                        &taps,
                    );
                } else {
                    gridder.grid_sample_taps_real_planned_f64(&mut psf_grid, &taps, grid_weight);
                }
            }
            Ok(())
        })?;
    }
    let grid_elapsed = grid_started.elapsed();
    let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
    stage_timings.psf_grid += split_grid_elapsed;
    stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);

    dirty_grids_to_psf_and_residual(
        gridder,
        &psf_grid,
        &residual_grid,
        accumulation,
        stage_timings,
    )
    .map(|(psf_state, residual)| (psf_state, residual, accumulation.max_abs_w_lambda))
}

fn compute_psf_standard_tiled_replay<F>(
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    replay_weighted_batches: &mut F,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let executor =
        StandardMfsTiledCpuExecutor::new_with_execution_config(gridder, execution_config)?;
    let mut accumulation = StandardMfsDirtyAccumulation {
        normalization_sumwt: 0.0,
        reported_sumwt: 0.0,
        gridded_samples: 0,
        skipped_samples: 0,
        max_abs_w_lambda: 0.0,
    };

    let grid_started = Instant::now();
    if executor.has_all_resident_tiles() {
        accumulation = executor.accumulate_psf_grid_direct_owned_replay(
            replay_weighted_batches,
            &mut psf_grid,
            execution_config.fixed_tile_max_live_row_blocks,
        )?;
    } else {
        replay_weighted_batches(&mut |batches| {
            let block_accumulation = executor.accumulate_psf_grid(&batches, &mut psf_grid)?;
            accumulate_dirty_accumulation(&mut accumulation, block_accumulation);
            Ok(())
        })?;
    }
    stage_timings.psf_grid += grid_started.elapsed();
    psf_grid_to_state(gridder, &psf_grid, accumulation, stage_timings)
}

fn compute_psf_standard_sample_replay<F>(
    gridder: &StandardGridder,
    replay_weighted_samples: &mut F,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut accumulation = StandardMfsDirtyAccumulation {
        normalization_sumwt: 0.0,
        reported_sumwt: 0.0,
        gridded_samples: 0,
        skipped_samples: 0,
        max_abs_w_lambda: 0.0,
    };

    let grid_started = Instant::now();
    if let Some(psf_storage) = psf_grid.as_slice_memory_order_mut() {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let taps = planned_sample_compact_taps(sample)?;
                let grid_weight = sample.grid_weight;
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                gridder.grid_compact_taps_real_planned_f64_storage(
                    psf_storage,
                    grid_weight,
                    taps.x_start,
                    taps.y_start,
                    taps.x_weight_index,
                    taps.y_weight_index,
                );
            }
            Ok(())
        })?;
    } else {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulation.max_abs_w_lambda =
                    accumulation.max_abs_w_lambda.max(sample.w_lambda.abs());
                let taps = planned_sample_positive_taps(sample)?;
                let grid_weight = sample.grid_weight;
                let grid_weight = f64::from(grid_weight);
                accumulation.normalization_sumwt += grid_weight;
                accumulation.reported_sumwt += grid_weight;
                accumulation.gridded_samples += 1;
                gridder.grid_sample_taps_real_planned_f64(&mut psf_grid, &taps, grid_weight);
            }
            Ok(())
        })?;
    }
    stage_timings.psf_grid += grid_started.elapsed();
    psf_grid_to_state(gridder, &psf_grid, accumulation, stage_timings)
}

fn compute_residual_standard_tiled_replay<F>(
    _geometry: ImageGeometry,
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    execution_config: StandardMfsExecutionConfig,
    replay_weighted_batches: &mut F,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(Vec<VisibilityBatch>) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut residual_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut timings = ResidualComputationTimings::default();
    let model_grid = if model.iter().any(|value| value.abs() > 0.0) {
        let model_fft_started = Instant::now();
        let transformed = centered_fft2(&gridder.apodize_model(model));
        timings.model_fft = model_fft_started.elapsed();
        Some(transformed)
    } else {
        None
    };

    let executor =
        StandardMfsTiledCpuExecutor::new_with_execution_config(gridder, execution_config)?;
    let mut counts = StandardMfsTiledResidualAccumulation::default();
    let degrid_grid_started = Instant::now();
    if executor.has_all_resident_tiles() {
        counts = executor.accumulate_residual_grid_direct_owned_replay(
            replay_weighted_batches,
            model_grid.as_ref(),
            &mut residual_grid,
            execution_config.fixed_tile_max_live_row_blocks,
        )?;
    } else {
        replay_weighted_batches(&mut |batches| {
            let block_counts = executor.accumulate_residual_grid(
                &batches,
                model_grid.as_ref(),
                &mut residual_grid,
            )?;
            accumulate_residual_accumulation(&mut counts, block_counts);
            Ok(())
        })?;
    }
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2_f64(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid_f64(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;

    profile::log_serial_stage(profile::SerialStageProfile {
        stage: "streaming_tiled_residual_refresh",
        samples_total: counts.valid_samples
            + counts.skipped_not_gridable
            + counts.skipped_invalid_weight
            + counts.skipped_nonfinite_visibility,
        finite_visibility_samples: counts.valid_samples,
        nonfinite_visibility_samples: counts.skipped_nonfinite_visibility,
        planned_samples: counts.planned_samples,
        model_grid_present_samples: if model_grid.is_some() {
            counts.gridded_residual_samples
        } else {
            0
        },
        model_grid_absent_samples: if model_grid.is_some() {
            0
        } else {
            counts.gridded_residual_samples
        },
        degrid_tap_visits: if model_grid.is_some() {
            counts.gridded_residual_samples.saturating_mul(49)
        } else {
            0
        },
        grid_tap_visits: counts.gridded_residual_samples.saturating_mul(49),
        stage_duration: timings.degrid_grid,
    });

    Ok(image)
}

fn compute_residual_standard_sample_replay<F>(
    _geometry: ImageGeometry,
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    replay_weighted_samples: &mut F,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError>
where
    F: FnMut(
        &mut dyn FnMut(&[StandardMfsPlannedWeightedSample]) -> Result<(), ImagingError>,
    ) -> Result<(), ImagingError>,
{
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut residual_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut timings = ResidualComputationTimings::default();
    let model_grid = if model.iter().any(|value| value.abs() > 0.0) {
        let model_fft_started = Instant::now();
        let transformed = centered_fft2(&gridder.apodize_model(model));
        timings.model_fft = model_fft_started.elapsed();
        Some(transformed)
    } else {
        None
    };

    let mut counts = StandardMfsTiledResidualAccumulation::default();
    let degrid_grid_started = Instant::now();
    if let (Some(model_storage), Some(residual_storage)) = (
        model_grid
            .as_ref()
            .and_then(|grid| grid.as_slice_memory_order()),
        residual_grid.as_slice_memory_order_mut(),
    ) {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulate_weighted_residual_sample_storage(
                    gridder,
                    sample,
                    Some(model_storage),
                    residual_storage,
                    &mut counts,
                )?;
            }
            Ok(())
        })?;
    } else if model_grid.is_none()
        && let Some(residual_storage) = residual_grid.as_slice_memory_order_mut()
    {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulate_weighted_residual_sample_storage(
                    gridder,
                    sample,
                    None,
                    residual_storage,
                    &mut counts,
                )?;
            }
            Ok(())
        })?;
    } else {
        replay_weighted_samples(&mut |samples| {
            for &sample in samples {
                accumulate_weighted_residual_sample_array(
                    gridder,
                    sample,
                    model_grid.as_ref(),
                    &mut residual_grid,
                    &mut counts,
                )?;
            }
            Ok(())
        })?;
    }
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2_f64(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid_f64(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;

    profile::log_serial_stage(profile::SerialStageProfile {
        stage: "sample_streaming_residual_refresh",
        samples_total: counts.valid_samples
            + counts.skipped_not_gridable
            + counts.skipped_invalid_weight
            + counts.skipped_nonfinite_visibility,
        finite_visibility_samples: counts.valid_samples,
        nonfinite_visibility_samples: counts.skipped_nonfinite_visibility,
        planned_samples: counts.planned_samples,
        model_grid_present_samples: if model_grid.is_some() {
            counts.gridded_residual_samples
        } else {
            0
        },
        model_grid_absent_samples: if model_grid.is_some() {
            0
        } else {
            counts.gridded_residual_samples
        },
        degrid_tap_visits: if model_grid.is_some() {
            counts.gridded_residual_samples.saturating_mul(49)
        } else {
            0
        },
        grid_tap_visits: counts.gridded_residual_samples.saturating_mul(49),
        stage_duration: timings.degrid_grid,
    });

    Ok(image)
}

fn accumulate_weighted_residual_sample_storage(
    gridder: &StandardGridder,
    sample: StandardMfsPlannedWeightedSample,
    model_storage: Option<&[Complex32]>,
    residual_storage: &mut [Complex64],
    counts: &mut StandardMfsTiledResidualAccumulation,
) -> Result<(), ImagingError> {
    if !sample.finite_visibility() {
        counts.skipped_nonfinite_visibility += 1;
        return Ok(());
    }
    counts.valid_samples += 1;
    let taps = planned_sample_compact_taps(sample)?;
    counts.planned_samples += 1;
    let residual_weight = sample.grid_weight;
    let residual_weight = f64::from(residual_weight);
    if let Some(model_storage) = model_storage {
        gridder.degrid_model_and_grid_residual_compact_taps_planned_f64_storage(
            model_storage,
            residual_storage,
            sample.visibility,
            residual_weight,
            taps.x_start,
            taps.y_start,
            taps.x_weight_index,
            taps.y_weight_index,
        );
    } else {
        let residual = Complex64::new(
            f64::from(sample.visibility.re) * residual_weight,
            f64::from(sample.visibility.im) * residual_weight,
        );
        gridder.grid_compact_taps_planned_f64_storage(
            residual_storage,
            residual,
            taps.x_start,
            taps.y_start,
            taps.x_weight_index,
            taps.y_weight_index,
        );
    }
    counts.gridded_residual_samples += 1;
    Ok(())
}

fn accumulate_weighted_residual_sample_array(
    gridder: &StandardGridder,
    sample: StandardMfsPlannedWeightedSample,
    model_grid: Option<&Array2<Complex32>>,
    residual_grid: &mut Array2<Complex64>,
    counts: &mut StandardMfsTiledResidualAccumulation,
) -> Result<(), ImagingError> {
    if !sample.finite_visibility() {
        counts.skipped_nonfinite_visibility += 1;
        return Ok(());
    }
    counts.valid_samples += 1;
    let taps = planned_sample_positive_taps(sample)?;
    counts.planned_samples += 1;
    let residual_weight = sample.grid_weight;
    let residual_weight = f64::from(residual_weight);
    if let Some(model_grid) = model_grid {
        gridder.degrid_model_and_grid_residual_taps_planned_f64(
            model_grid,
            residual_grid,
            &taps,
            sample.visibility,
            residual_weight,
        );
    } else {
        let residual = Complex64::new(
            f64::from(sample.visibility.re) * residual_weight,
            f64::from(sample.visibility.im) * residual_weight,
        );
        gridder.grid_sample_taps_planned_f64(residual_grid, &taps, residual);
    }
    counts.gridded_residual_samples += 1;
    Ok(())
}

fn dirty_grids_to_psf_and_residual(
    gridder: &StandardGridder,
    psf_grid: &Array2<Complex64>,
    residual_grid: &Array2<Complex64>,
    accumulation: StandardMfsDirtyAccumulation,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>), ImagingError> {
    if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let psf_fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(psf_grid);
    stage_timings.psf_fft += psf_fft_started.elapsed();
    let psf_normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
    let psf_peak = peak_abs_value(&psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "PSF peak is non-finite or zero".to_string(),
        ));
    }
    psf.mapv_inplace(|value| value / psf_peak);
    stage_timings.psf_normalize += psf_normalize_started.elapsed();

    let residual_fft_started = Instant::now();
    let raw_residual = centered_ifft2_f64(residual_grid);
    stage_timings.residual_fft += residual_fft_started.elapsed();
    let residual_normalize_started = Instant::now();
    let mut residual = gridder.corrected_image_from_grid_f64(&raw_residual);
    residual.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32 / psf_peak);
    stage_timings.residual_normalize += residual_normalize_started.elapsed();

    Ok((
        PsfState {
            psf,
            normalization_sumwt: accumulation.normalization_sumwt as f32,
            reported_sumwt: accumulation.reported_sumwt as f32,
            psf_peak,
            gridded_samples: accumulation.gridded_samples,
            skipped_samples: accumulation.skipped_samples,
        },
        residual,
    ))
}

fn psf_grid_to_state(
    gridder: &StandardGridder,
    psf_grid: &Array2<Complex64>,
    accumulation: StandardMfsDirtyAccumulation,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(psf_grid);
    stage_timings.psf_fft += fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
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
        normalization_sumwt: accumulation.normalization_sumwt as f32,
        reported_sumwt: accumulation.reported_sumwt as f32,
        psf_peak,
        gridded_samples: accumulation.gridded_samples,
        skipped_samples: accumulation.skipped_samples,
    })
}

fn should_use_standard_mfs_executor(
    request: &ImagingRequest,
    weighted_batches: &[VisibilityBatch],
) -> bool {
    if !matches!(request.w_term_mode, WTermMode::None) {
        return false;
    }
    standard_mfs_sample_count(weighted_batches) <= standard_mfs_executor_max_samples()
}

fn should_use_standard_mfs_tiled_backend(request: &ImagingRequest) -> bool {
    matches!(request.w_term_mode, WTermMode::None) && standard_mfs_fixed_tile_backend_enabled()
}

fn should_use_standard_mfs_metal_backend(request: &ImagingRequest) -> bool {
    matches!(request.w_term_mode, WTermMode::None) && standard_mfs_metal_backend_enabled()
}

fn ensure_standard_mfs_backend_available() -> Result<(), ImagingError> {
    match standard_mfs_backend_selection_from_env()? {
        StandardMfsBackendSelection::Cpu | StandardMfsBackendSelection::FixedTile => Ok(()),
        StandardMfsBackendSelection::Metal => {
            #[cfg(target_os = "macos")]
            {
                Ok(())
            }
            #[cfg(not(target_os = "macos"))]
            {
                Err(ImagingError::Unsupported(
                    "standard MFS backend 'metal' requires macOS Metal and is not available \
                     on this platform"
                        .to_string(),
                ))
            }
        }
    }
}

fn standard_mfs_fixed_tile_backend_enabled() -> bool {
    matches!(
        standard_mfs_backend_selection_from_env(),
        Ok(StandardMfsBackendSelection::FixedTile)
    )
}

fn standard_mfs_metal_backend_enabled() -> bool {
    matches!(
        standard_mfs_backend_selection_from_env(),
        Ok(StandardMfsBackendSelection::Metal)
    )
}

fn standard_mfs_backend_selection_from_env() -> Result<StandardMfsBackendSelection, ImagingError> {
    env::var(STANDARD_MFS_BACKEND_ENV)
        .map(|value| parse_standard_mfs_backend_selection(&value))
        .unwrap_or(Ok(StandardMfsBackendSelection::Cpu))
}

fn parse_standard_mfs_backend_selection(
    value: &str,
) -> Result<StandardMfsBackendSelection, ImagingError> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "cpu" | "default" => Ok(StandardMfsBackendSelection::Cpu),
        "fixed_tile" | "fixed-tile" | "tile" | "tiled" | "streaming_fixed_tile" => {
            Ok(StandardMfsBackendSelection::FixedTile)
        }
        "metal" => Ok(StandardMfsBackendSelection::Metal),
        _ => Err(ImagingError::Unsupported(format!(
            "standard MFS backend '{value}' is not recognized; expected cpu, fixed_tile, or metal"
        ))),
    }
}

fn standard_mfs_executor_max_samples() -> usize {
    env::var("CASA_RS_STANDARD_MFS_EXECUTOR_MAX_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_STANDARD_MFS_EXECUTOR_MAX_SAMPLES)
}

fn standard_mfs_grid_threads() -> usize {
    standard_mfs_thread_count_from_env()
}

fn standard_mfs_thread_count_from_env() -> usize {
    env::var(STANDARD_MFS_GRID_THREADS_ENV)
        .ok()
        .and_then(|value| parse_standard_mfs_thread_count(&value))
        .unwrap_or(1)
}

fn parse_standard_mfs_thread_count(value: &str) -> Option<usize> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("auto") {
        return Some(thread::available_parallelism().map_or(1, |value| value.get()));
    }
    value.parse::<usize>().ok().filter(|value| *value > 0)
}

fn standard_mfs_sample_count(batches: &[VisibilityBatch]) -> usize {
    batches.iter().map(VisibilityBatch::len).sum()
}

fn complex64_grid_bytes(nx: usize, ny: usize, grid_count: usize) -> usize {
    nx.saturating_mul(ny)
        .saturating_mul(grid_count)
        .saturating_mul(std::mem::size_of::<Complex64>())
}

/// Run CASA-style MTMFS imaging on already-prepared MFS visibilities.
///
/// The current Rust implementation follows CASA's point-source MTMFS structure:
/// Taylor-weighted dirty/PSF terms, a coupled Hessian solve in the minor
/// cycle, Cotton-Schwab residual refreshes against the measured visibilities,
/// and CASA-style `.tt*`, `.alpha`, and `.alpha.error` products.
///
/// This implementation mirrors CASA's historical Hogbom off-by-one behavior
/// for MTMFS minor-cycle budgeting: the reported `niter` remains capped, but a
/// single minor-cycle call can commit one extra component.
pub fn run_mtmfs(request: &MtmfsRequest) -> Result<MtmfsResult, ImagingError> {
    let total_started = Instant::now();
    request.validate()?;
    if request.compatibility != CompatibilityMode::CasaStandardMfs {
        return Err(ImagingError::Unsupported(
            "only CASA standard MFS compatibility mode is implemented".to_string(),
        ));
    }
    if !matches!(request.gridder_mode, GridderMode::Standard) {
        return Err(ImagingError::Unsupported(
            "MTMFS currently supports gridder='standard' only".to_string(),
        ));
    }

    let gridder = StandardGridder::new(request.geometry)?;
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let weighting_request = ImagingRequest {
        geometry: request.geometry,
        visibility_batches: request.visibility_batches.clone(),
        gridder_mode: request.gridder_mode.clone(),
        plane_stokes: request.plane_stokes,
        weighting: request.weighting,
        reffreq_hz: request.reffreq_hz,
        selected_frequency_range_hz: request.selected_frequency_range_hz,
        deconvolver: Deconvolver::Hogbom,
        multiscale_scales: request.multiscale_scales.clone(),
        small_scale_bias: request.small_scale_bias,
        clean: request.clean,
        clean_mask: request.clean_mask.clone(),
        initial_model: None,
        w_term_mode: request.w_term_mode,
        w_project_planes: request.w_project_planes,
        compatibility: request.compatibility,
    };
    let weighted_batches = apply_weighting(&weighting_request, &gridder)?;
    stage_timings.weighting += weighting_started.elapsed();

    let psf_state =
        compute_mtmfs_psf_terms(request, &weighted_batches, &gridder, &mut stage_timings)?;
    let [nx, ny] = request.geometry.image_shape;
    let mut model_terms = vec![Array2::<f32>::zeros((nx, ny)); request.nterms];
    let mut residual_terms = compute_mtmfs_residual_terms(
        request,
        &weighted_batches,
        &gridder,
        &model_terms,
        &psf_state,
        &mut stage_timings,
    )?;
    let max_psf_sidelobe_level = estimate_psf_sidelobe_level(
        &psf_state.psf_terms[0],
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    let clean_mask_pixels = request
        .clean_mask
        .as_ref()
        .map(|mask| mask.iter().filter(|value| **value).count())
        .unwrap_or(nx * ny);
    let initial_peak = peak_abs_value_masked(&residual_terms[0], request.clean_mask.as_ref());
    let mut warnings = Vec::new();

    let hessian = mtmfs_hessian(&psf_state.psf_terms, request.nterms)?;
    let inv_hessian = invert_small_matrix(&hessian)?;

    let controller_started = Instant::now();
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;
    let mut residual_needs_refresh = false;

    while reported_minor_iterations < request.clean.niter {
        if request
            .clean
            .major_cycle_limit
            .is_some_and(|limit| major_cycles >= limit)
        {
            clean_stop_reason = Some(CleanStopReason::MajorCycleLimitReached);
            break;
        }
        let Some((_, cycle_peak_value)) =
            peak_location_masked(&residual_terms[0], request.clean_mask.as_ref())
        else {
            clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let cycle_peak = cycle_peak_value.abs();
        let cycle_nsigma_threshold_jy_per_beam = nsigma_threshold_jy_per_beam(
            &residual_terms[0],
            request.clean_mask.as_ref(),
            request.clean,
        );
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
        let (outcome, probe) = run_mtmfs_minor_cycle(
            request,
            &psf_state.psf_terms,
            &hessian,
            &inv_hessian,
            &mut model_terms,
            &mut residual_terms,
            cycle_reported_niter,
            cycle_threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
            &mut stage_timings,
        );
        minor_cycle_traces.push(make_minor_cycle_trace(
            minor_cycle_traces.len(),
            start_reported_iteration,
            outcome,
            cycle_peak,
            &residual_terms[0],
            &model_terms[0],
            probe,
        ));
        reported_minor_iterations += outcome.reported_updates;
        final_cycle_threshold_jy_per_beam = outcome.final_cycle_threshold_jy_per_beam;
        let mut stop_after_refresh = None::<CleanStopReason>;
        if let Some(reason) = outcome.stop_reason {
            match reason {
                CleanStopReason::CycleThresholdReached if outcome.updated_model => {}
                CleanStopReason::CycleThresholdReached => {
                    clean_stop_reason = Some(reason);
                }
                _ => {
                    clean_stop_reason = Some(reason);
                    stop_after_refresh = Some(reason);
                }
            }
        }
        if !outcome.updated_model {
            break;
        }
        residual_needs_refresh = true;
        let minor_peak = peak_abs_value_masked(&residual_terms[0], request.clean_mask.as_ref());
        update_divergence_state(
            &mut warnings,
            &mut min_residual_peak_jy_per_beam,
            minor_peak,
            &mut divergence_warned,
        );
        if reported_minor_iterations >= request.clean.niter {
            clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
            break;
        }
        let refresh_started = Instant::now();
        residual_terms = compute_mtmfs_residual_terms(
            request,
            &weighted_batches,
            &gridder,
            &model_terms,
            &psf_state,
            &mut stage_timings,
        )?;
        stage_timings.major_cycle_refresh += refresh_started.elapsed();
        major_cycles += 1;
        residual_needs_refresh = false;
        let refreshed_peak = peak_abs_value_masked(&residual_terms[0], request.clean_mask.as_ref());
        let refreshed_nsigma_threshold_jy_per_beam = nsigma_threshold_jy_per_beam(
            &residual_terms[0],
            request.clean_mask.as_ref(),
            request.clean,
        );
        if stop_after_refresh.is_some() {
            break;
        }
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
    if residual_needs_refresh {
        let refresh_started = Instant::now();
        residual_terms = compute_mtmfs_residual_terms(
            request,
            &weighted_batches,
            &gridder,
            &model_terms,
            &psf_state,
            &mut stage_timings,
        )?;
        stage_timings.major_cycle_refresh += refresh_started.elapsed();
    }
    let controller_elapsed = controller_started.elapsed();
    let accounted = stage_timings
        .minor_cycle_solve
        .saturating_add(stage_timings.major_cycle_refresh);
    stage_timings.controller_overhead += controller_elapsed.saturating_sub(accounted);

    let beam_fit_started = Instant::now();
    let BeamFitOutcome {
        beam,
        warnings: beam_warnings,
        attempts: beam_fit_attempts,
        cutoff_used: beam_fit_cutoff_used,
        debug: beam_fit_debug,
    } = fit_beam_from_psf(
        &psf_state.psf_terms[0],
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    stage_timings.beam_fit += beam_fit_started.elapsed();
    let restore_started = Instant::now();
    let principal_residual_terms = principal_solution_terms(&residual_terms, &inv_hessian);
    let mut image_terms = Vec::with_capacity(request.nterms);
    for (model_term, residual_term) in model_terms.iter().zip(principal_residual_terms.iter()) {
        let restored_model = restore_model(model_term, request.geometry.cell_size_rad, beam);
        image_terms.push(&restored_model + residual_term);
    }
    stage_timings.restore += restore_started.elapsed();

    let max_abs_w_lambda = weighted_batches
        .iter()
        .flat_map(|batch| batch.w_lambda.iter())
        .fold(0.0f64, |max_value, value| max_value.max(value.abs()));
    let fractional_bandwidth = (request.selected_frequency_range_hz[1]
        - request.selected_frequency_range_hz[0])
        / request.reffreq_hz;
    warnings.extend(beam_warnings);
    stage_timings.total = total_started.elapsed();

    let (alpha, alpha_error) =
        compute_mtmfs_alpha_products(&image_terms, &principal_residual_terms);

    Ok(MtmfsResult {
        psf_terms: psf_state.psf_terms.iter().map(expand_plane).collect(),
        residual_terms: residual_terms.iter().map(expand_plane).collect(),
        model_terms: model_terms.iter().map(expand_plane).collect(),
        image_terms: image_terms.iter().map(expand_plane).collect(),
        sumwt_terms: psf_state
            .reported_sumwt_terms
            .iter()
            .copied()
            .map(expand_scalar)
            .collect(),
        alpha: alpha.as_ref().map(expand_plane),
        alpha_error: alpha_error.as_ref().map(expand_plane),
        beam,
        diagnostics: ImagingDiagnostics {
            warnings,
            gridded_samples: psf_state.gridded_samples,
            skipped_samples: psf_state.skipped_samples,
            major_cycles: casa_major_cycle_count(major_cycles, request.clean.niter),
            minor_iterations: reported_minor_iterations,
            clean_stop_reason,
            minor_cycle_traces,
            initial_residual_peak_jy_per_beam: initial_peak,
            final_residual_peak_jy_per_beam: peak_abs_value_masked(
                &residual_terms[0],
                request.clean_mask.as_ref(),
            ),
            max_abs_w_lambda,
            fractional_bandwidth,
            max_psf_sidelobe_level,
            final_cycle_threshold_jy_per_beam,
            clean_mask_pixels,
            beam_fit_attempts,
            beam_fit_cutoff_used,
            beam_fit_debug,
            mosaic_weight_image: None,
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

#[derive(Debug, Clone)]
struct MosaicPointingGroup {
    pointing_direction_rad: [f64; 2],
    frequency_hz: f64,
    primary_beam_model: PrimaryBeamModel,
    batch: VisibilityBatch,
    sample_frequency_hz: Vec<f64>,
}

fn run_mosaic_dirty_imaging(
    request: &ImagingRequest,
    config: &MosaicGridderConfig,
    total_started: Instant,
) -> Result<ImagingResult, ImagingError> {
    if request.w_term_mode != WTermMode::None {
        return Err(ImagingError::Unsupported(
            "mosaic gridder currently supports only w_term_mode='none'".to_string(),
        ));
    }

    let gridder = StandardGridder::new_unpadded(request.geometry)?;
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let weighted_batches = apply_mosaic_weighting(request, config, &gridder)?;
    stage_timings.weighting += weighting_started.elapsed();
    let conv_sampling = mosaic_projector_sampling(request.geometry);
    let groups = build_mosaic_pointing_groups(&weighted_batches, &config.metadata_batches)?;
    if groups.is_empty() {
        return Err(ImagingError::NoUsableSamples);
    }

    let [nx, ny] = request.geometry.image_shape;
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
    let mut model = Array2::<f32>::zeros((nx, ny));
    let mut accumulated_residual_image = Array2::<f32>::zeros((nx, ny));
    let mut accumulated_weight_image = Array2::<f32>::zeros((nx, ny));
    let dump_grids = mosaic_grid_dump_enabled();
    let mut accumulated_residual_grid =
        dump_grids.then(|| Array2::<Complex64>::zeros((grid_nx, grid_ny)));
    let mut accumulated_mosaic_weight_grid =
        dump_grids.then(|| Array2::<Complex64>::zeros((grid_nx, grid_ny)));
    let mut reported_sumwt = 0.0f64;
    let mut normalization_sumwt = 0.0f64;
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;
    let mut projector_cache = MosaicProjectorCache::new();
    let trace_enabled = mosaic_trace_enabled();
    let cell_trace_targets = mosaic_cell_trace_targets();

    let pb_weight_image = build_mosaic_weight_image(request.geometry, config, &groups)?;

    for group in &groups {
        if !mosaic_pointing_contributes_by_simple_pb_center(
            request.geometry,
            config.phase_center_direction_rad,
            group.pointing_direction_rad,
        ) {
            if env::var_os("CASA_RS_DEBUG_MOSAIC").is_some() {
                eprintln!(
                    "mosaic skipping group outside image footprint dir={:?} freq_hz={:.6e}",
                    group.pointing_direction_rad, group.frequency_hz
                );
            }
            continue;
        }
        let projector_started = Instant::now();
        let projector = cached_mosaic_projector(
            &mut projector_cache,
            request.geometry,
            &gridder,
            config.phase_center_direction_rad,
            group.pointing_direction_rad,
            group.primary_beam_model,
            group.frequency_hz,
            conv_sampling,
            2,
        )?;
        let weight_projector = cached_mosaic_projector(
            &mut projector_cache,
            request.geometry,
            &gridder,
            config.phase_center_direction_rad,
            group.pointing_direction_rad,
            group.primary_beam_model,
            group.frequency_hz,
            conv_sampling,
            4,
        )?;
        let weight_plan = weight_projector.plan_sample(0.0, 0.0).ok_or_else(|| {
            ImagingError::Normalization(
                "mosaic weight projector failed to plan the centered kernel".to_string(),
            )
        })?;
        let mut group_residual_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
        let mut group_weight_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
        stage_timings.psf_grid += projector_started.elapsed();
        if env::var_os("CASA_RS_DEBUG_MOSAIC").is_some() {
            eprintln!(
                "mosaic group dir={:?} freq_hz={:.6e} support={} sampling={} samples={}",
                group.pointing_direction_rad,
                group.frequency_hz,
                projector.support(),
                projector.sampling(),
                group.batch.len()
            );
        }
        if trace_enabled {
            append_mosaic_trace(format!(
                "{{\"event\":\"group\",\"pointing_ra\":{:.17e},\"pointing_dec\":{:.17e},\"freq_hz\":{:.17e},\"support\":{},\"sampling\":{},\"samples\":{},\"imaging_kernel_sum_re\":{:.17e},\"imaging_kernel_sum_im\":{:.17e},\"weight_kernel_sum_re\":{:.17e},\"weight_kernel_sum_im\":{:.17e}}}",
                group.pointing_direction_rad[0],
                group.pointing_direction_rad[1],
                group.frequency_hz,
                projector.support(),
                projector.sampling(),
                group.batch.len(),
                projector.normalization_sum().re,
                projector.normalization_sum().im,
                weight_projector.normalization_sum().re,
                weight_projector.normalization_sum().im,
            ));
        }
        let grid_started = Instant::now();
        let mut logged_first_sample = !trace_enabled;
        let mut trace_group_value_sum = Complex32::new(0.0, 0.0);
        let mut trace_group_weight_sum = 0.0f64;
        let mut trace_group_sample_count = 0usize;
        let mut trace_group_center_count = 0usize;
        let mut group_weight_grid_sum = 0.0f64;
        for sample_index in 0..group.batch.len() {
            if !group.batch.gridable[sample_index] {
                skipped_samples += 1;
                continue;
            }
            let weight = group.batch.weight[sample_index];
            let sumwt_factor = group.batch.sumwt_factor[sample_index];
            let visibility = group.batch.visibility[sample_index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0
                && visibility.re.is_finite()
                && visibility.im.is_finite())
            {
                skipped_samples += 1;
                continue;
            }
            let grid_weight = weight * sumwt_factor;
            let Some(plan) = projector.plan_sample(
                group.batch.u_lambda[sample_index],
                group.batch.v_lambda[sample_index],
            ) else {
                skipped_samples += 1;
                continue;
            };
            if trace_enabled {
                trace_group_value_sum += visibility * grid_weight;
                trace_group_weight_sum += f64::from(grid_weight);
                trace_group_sample_count += 1;
            }
            if trace_enabled && !cell_trace_targets.is_empty() {
                let value = Complex64::new(visibility.re as f64, visibility.im as f64)
                    * f64::from(grid_weight);
                for &(target_x, target_y) in &cell_trace_targets {
                    let ix = target_x - plan.loc_x;
                    let iy = target_y - plan.loc_y;
                    let Some(tap) = projector.trace_sample_tap_at(&plan, ix, iy) else {
                        continue;
                    };
                    let contribution = value * Complex64::new(tap.re as f64, tap.im as f64);
                    let sample_frequency_hz = group
                        .sample_frequency_hz
                        .get(sample_index)
                        .copied()
                        .unwrap_or(group.frequency_hz);
                    append_mosaic_trace(format!(
                        "{{\"event\":\"cell_contribution\",\"target\":[{},{}],\"pointing_ra\":{:.17e},\"pointing_dec\":{:.17e},\"freq_hz\":{:.17e},\"plane_freq_hz\":{:.17e},\"sample_freq_hz\":{:.17e},\"sample_index\":{},\"u_lambda\":{:.17e},\"v_lambda\":{:.17e},\"w_lambda\":{:.17e},\"loc\":[{},{}],\"off\":[{},{}],\"tap_offset\":[{},{}],\"tap\":[{:.17e},{:.17e}],\"visibility\":[{:.17e},{:.17e}],\"weight\":{:.17e},\"sumwt_factor\":{:.17e},\"grid_weight\":{:.17e},\"weighted_value\":[{:.17e},{:.17e}],\"contribution\":[{:.17e},{:.17e}]}}",
                        target_x,
                        target_y,
                        group.pointing_direction_rad[0],
                        group.pointing_direction_rad[1],
                        group.frequency_hz,
                        request.reffreq_hz,
                        sample_frequency_hz,
                        sample_index,
                        group.batch.u_lambda[sample_index],
                        group.batch.v_lambda[sample_index],
                        group.batch.w_lambda[sample_index],
                        plan.loc_x,
                        plan.loc_y,
                        plan.off_x,
                        plan.off_y,
                        ix,
                        iy,
                        tap.re,
                        tap.im,
                        visibility.re,
                        visibility.im,
                        weight,
                        sumwt_factor,
                        grid_weight,
                        value.re,
                        value.im,
                        contribution.re,
                        contribution.im,
                    ));
                }
            }
            if !logged_first_sample {
                let (tap_sum, center_tap, first_tap) =
                    projector.trace_sample_taps(&plan).unwrap_or((
                        Complex32::new(0.0, 0.0),
                        Complex32::new(0.0, 0.0),
                        Complex32::new(0.0, 0.0),
                    ));
                let tap_matrix_json = projector
                    .trace_sample_tap_matrix(&plan)
                    .map(|taps| {
                        taps.into_iter()
                            .map(|(ix, iy, tap)| {
                                format!("[{ix},{iy},{:.17e},{:.17e}]", tap.re, tap.im)
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_default();
                append_mosaic_trace(format!(
                    "{{\"event\":\"sample\",\"pointing_ra\":{:.17e},\"pointing_dec\":{:.17e},\"sample_index\":{},\"u_lambda\":{:.17e},\"v_lambda\":{:.17e},\"w_lambda\":{:.17e},\"loc\":[{},{}],\"off\":[{},{}],\"normalization\":{:.17e},\"tap_sum\":[{:.17e},{:.17e}],\"center_tap\":[{:.17e},{:.17e}],\"first_tap\":[{:.17e},{:.17e}],\"taps\":[{}],\"visibility\":[{:.17e},{:.17e}],\"weight\":{:.17e},\"sumwt_factor\":{:.17e},\"grid_weight\":{:.17e}}}",
                    group.pointing_direction_rad[0],
                    group.pointing_direction_rad[1],
                    sample_index,
                    group.batch.u_lambda[sample_index],
                    group.batch.v_lambda[sample_index],
                    group.batch.w_lambda[sample_index],
                    plan.loc_x,
                    plan.loc_y,
                    plan.off_x,
                    plan.off_y,
                    plan.normalization,
                    tap_sum.re,
                    tap_sum.im,
                    center_tap.re,
                    center_tap.im,
                    first_tap.re,
                    first_tap.im,
                    tap_matrix_json,
                    visibility.re,
                    visibility.im,
                    weight,
                    sumwt_factor,
                    grid_weight,
                ));
                logged_first_sample = true;
            }
            let grid_weight64 = f64::from(grid_weight);
            projector.grid_sample_planned_f64(
                &mut psf_grid,
                &plan,
                Complex64::new(grid_weight64, 0.0),
            );
            projector.grid_sample_planned_f64(
                &mut group_residual_grid,
                &plan,
                Complex64::new(visibility.re as f64, visibility.im as f64) * grid_weight64,
            );
            group_weight_grid_sum += grid_weight64;
            let reported = f64::from(grid_weight);
            if plan.center_in_bounds {
                normalization_sumwt += reported;
                reported_sumwt += reported;
                trace_group_center_count += 1;
            }
            gridded_samples += 1;
        }
        if group_weight_grid_sum > 0.0 {
            weight_projector.grid_sample_planned_f64(
                &mut group_weight_grid,
                &weight_plan,
                Complex64::new(group_weight_grid_sum, 0.0),
            );
        }
        if let Some(residual_grid) = accumulated_residual_grid.as_mut() {
            Zip::from(residual_grid)
                .and(&group_residual_grid)
                .for_each(|accumulated, value| *accumulated += *value);
        }
        if let Some(weight_grid) = accumulated_mosaic_weight_grid.as_mut() {
            Zip::from(weight_grid)
                .and(&group_weight_grid)
                .for_each(|accumulated, value| *accumulated += *value);
        }
        if trace_enabled {
            append_mosaic_trace(format!(
                "{{\"event\":\"group_aggregate\",\"pointing_ra\":{:.17e},\"pointing_dec\":{:.17e},\"freq_hz\":{:.17e},\"samples\":{},\"center_samples\":{},\"grid_weight_sum\":{:.17e},\"weighted_visibility_sum\":[{:.17e},{:.17e}]}}",
                group.pointing_direction_rad[0],
                group.pointing_direction_rad[1],
                group.frequency_hz,
                trace_group_sample_count,
                trace_group_center_count,
                trace_group_weight_sum,
                trace_group_value_sum.re,
                trace_group_value_sum.im,
            ));
        }
        let raw_group_residual = centered_ifft2_f64(&group_residual_grid);
        let group_residual_image =
            gridder.corrected_mosaic_image_from_grid_f64(&raw_group_residual, conv_sampling);
        let raw_group_weight = centered_ifft2_f64(&group_weight_grid);
        let group_weight_image =
            gridder.corrected_mosaic_image_from_grid_f64(&raw_group_weight, conv_sampling);
        if trace_enabled {
            append_mosaic_trace(format!(
                "{{\"event\":\"group_image\",\"pointing_ra\":{:.17e},\"pointing_dec\":{:.17e},\"freq_hz\":{:.17e},\"pixels\":{}}}",
                group.pointing_direction_rad[0],
                group.pointing_direction_rad[1],
                group.frequency_hz,
                mosaic_trace_group_pixels_json(&group_residual_image, &group_weight_image),
            ));
        }
        Zip::from(&mut accumulated_residual_image)
            .and(&group_residual_image)
            .for_each(|accumulated, residual_value| {
                *accumulated += *residual_value;
            });
        Zip::from(&mut accumulated_weight_image)
            .and(&group_weight_image)
            .for_each(|accumulated, weight_value| {
                *accumulated += *weight_value;
            });
        stage_timings.psf_grid += grid_started.elapsed();
    }

    if !(normalization_sumwt.is_finite() && normalization_sumwt > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic normalization sumwt is non-finite or zero".to_string(),
        ));
    }
    if !(reported_sumwt.is_finite() && reported_sumwt > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic reported sumwt is non-finite or zero".to_string(),
        ));
    }

    let fft_started = Instant::now();
    dump_mosaic_complex_grid("psf_prefft", request.reffreq_hz, &psf_grid);
    if let Some(residual_grid) = accumulated_residual_grid.as_ref() {
        dump_mosaic_complex_grid("residual_prefft", request.reffreq_hz, residual_grid);
    }
    if let Some(weight_grid) = accumulated_mosaic_weight_grid.as_ref() {
        dump_mosaic_complex_grid("weight_prefft", request.reffreq_hz, weight_grid);
    }
    let raw_psf = centered_ifft2_f64(&psf_grid);
    stage_timings.psf_fft += fft_started.elapsed();

    let normalize_started = Instant::now();
    let mut accumulated_psf = gridder.corrected_mosaic_image_from_grid_f64(&raw_psf, conv_sampling);
    let mut accumulated_residual = accumulated_residual_image;
    let normalization_weight_image = accumulated_weight_image;
    let fft_sumwt_scale =
        (request.geometry.nx() * request.geometry.ny()) as f32 / reported_sumwt as f32;
    accumulated_residual.mapv_inplace(|value| value * fft_sumwt_scale);
    accumulated_psf.mapv_inplace(|value| value * fft_sumwt_scale);
    if trace_enabled {
        append_mosaic_trace(format!(
            "{{\"event\":\"pre_normalize\",\"normalization_sumwt\":{:.17e},\"reported_sumwt\":{:.17e},\"raw_residual_peak\":{:.17e},\"raw_psf_peak\":{:.17e},\"normalization_weight_peak\":{:.17e},\"pixels\":{}}}",
            normalization_sumwt,
            reported_sumwt,
            peak_abs_value(&accumulated_residual),
            peak_abs_value(&accumulated_psf),
            peak_abs_value(&normalization_weight_image),
            mosaic_trace_pixels_json(
                &accumulated_residual,
                &accumulated_psf,
                &normalization_weight_image,
                None
            ),
        ));
    }
    let weight_image = normalization_weight_image.mapv(|value| value * fft_sumwt_scale);
    if env::var_os("CASA_RS_DEBUG_MOSAIC").is_some() {
        let pre_weight_peak = peak_abs_value(&accumulated_residual);
        let pre_weight_peak_loc = peak_location_masked(&accumulated_residual, None);
        eprintln!(
            "mosaic pre-weight residual peak={pre_weight_peak:.9e} loc={pre_weight_peak_loc:?}"
        );
    }
    if env::var_os("CASA_RS_DEBUG_MOSAIC").is_some() {
        let weight_peak = peak_abs_value(&normalization_weight_image);
        let weight_peak_loc = peak_location_masked(&normalization_weight_image, None);
        eprintln!("mosaic weight peak={weight_peak:.9e} loc={weight_peak_loc:?}");
    }

    let weight_peak = weight_image
        .iter()
        .copied()
        .fold(0.0f32, |peak, value| peak.max(value));
    if !(weight_peak.is_finite() && weight_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic weight peak is non-finite or zero".to_string(),
        ));
    }
    let pb_limit_threshold = config.pb_limit.abs() * config.pb_limit.abs() * weight_peak;
    for ((x, y), weight_value) in weight_image.indexed_iter() {
        let sensitivity = weight_value.max(0.0);
        if sensitivity > pb_limit_threshold {
            accumulated_residual[(x, y)] /= (sensitivity * weight_peak).sqrt();
        } else {
            accumulated_residual[(x, y)] = 0.0;
        }
    }

    let psf_peak = peak_abs_value(&accumulated_psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic PSF peak is non-finite or zero".to_string(),
        ));
    }
    accumulated_psf.mapv_inplace(|value| value / psf_peak);
    if trace_enabled {
        append_mosaic_trace(format!(
            "{{\"event\":\"post_normalize\",\"psf_peak\":{:.17e},\"weight_peak\":{:.17e},\"pb_limit_threshold\":{:.17e},\"pixels\":{}}}",
            psf_peak,
            weight_peak,
            pb_limit_threshold,
            mosaic_trace_pixels_json(
                &accumulated_residual,
                &accumulated_psf,
                &normalization_weight_image,
                Some(&weight_image)
            ),
        ));
    }
    stage_timings.psf_normalize += normalize_started.elapsed();
    if env::var_os("CASA_RS_DEBUG_MOSAIC").is_some() {
        eprintln!(
            "mosaic totals: gridded={gridded_samples} skipped={skipped_samples} normalization_sumwt={normalization_sumwt:.9e} reported_sumwt={reported_sumwt:.9e}"
        );
    }

    let reported_sumwt = reported_sumwt as f32;
    let max_psf_sidelobe_level = estimate_psf_sidelobe_level(
        &accumulated_psf,
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    let mosaic_clean_mask = build_mosaic_clean_mask(
        &pb_weight_image,
        config.pb_limit,
        request.clean_mask.as_ref(),
    )?;
    let clean_request = request_for_mosaic_clean_mask(request, mosaic_clean_mask);
    let clean_mask_pixels = clean_request
        .clean_mask
        .as_ref()
        .map(|mask| mask.iter().filter(|value| **value).count())
        .unwrap_or(nx * ny);
    let initial_peak =
        peak_abs_value_masked(&accumulated_residual, clean_request.clean_mask.as_ref());
    let psf_state = PsfState {
        psf: accumulated_psf,
        normalization_sumwt: normalization_sumwt as f32,
        reported_sumwt,
        psf_peak,
        gridded_samples,
        skipped_samples,
    };
    let mut warnings = Vec::<String>::new();
    let controller_started = Instant::now();
    let clean_state = if request.clean.niter > 0 {
        run_mosaic_image_domain_controller(
            &clean_request,
            config,
            &gridder,
            &groups,
            &mut projector_cache,
            conv_sampling,
            &weight_image,
            &psf_state,
            &mut stage_timings,
            &mut model,
            accumulated_residual,
            max_psf_sidelobe_level,
            initial_peak,
            &mut warnings,
        )?
    } else {
        CottonSchwabState {
            residual: accumulated_residual,
            major_cycles: 0,
            minor_iterations: 0,
            clean_stop_reason: None,
            minor_cycle_traces: Vec::new(),
            final_cycle_threshold_jy_per_beam: 0.0,
        }
    };
    let accounted = stage_timings
        .minor_cycle_solve
        .saturating_add(stage_timings.major_cycle_refresh);
    stage_timings.controller_overhead += controller_started.elapsed().saturating_sub(accounted);
    let residual = clean_state.residual;

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
    warnings.extend(beam_warnings);
    let restore_started = Instant::now();
    let restored_model = restore_model(&model, request.geometry.cell_size_rad, beam);
    stage_timings.restore += restore_started.elapsed();
    let restored_image = &restored_model + &residual;
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
            max_abs_w_lambda: request
                .visibility_batches
                .iter()
                .flat_map(|batch| batch.w_lambda.iter())
                .fold(0.0f64, |max_value, value| max_value.max(value.abs())),
            fractional_bandwidth: (request.selected_frequency_range_hz[1]
                - request.selected_frequency_range_hz[0])
                / request.reffreq_hz,
            max_psf_sidelobe_level,
            final_cycle_threshold_jy_per_beam: clean_state.final_cycle_threshold_jy_per_beam,
            clean_mask_pixels,
            beam_fit_attempts,
            beam_fit_cutoff_used,
            beam_fit_debug,
            mosaic_weight_image: Some(weight_image),
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

fn primary_beam_model_key(model: PrimaryBeamModel) -> (u8, u64, u64) {
    match model {
        PrimaryBeamModel::Airy {
            dish_diameter_m,
            blockage_diameter_m,
        } => (0, dish_diameter_m.to_bits(), blockage_diameter_m.to_bits()),
        PrimaryBeamModel::EvlaLBandCommon => (1, 0, 0),
    }
}

fn build_mosaic_pointing_groups(
    batches: &[VisibilityBatch],
    metadata_batches: &[VisibilityMetadataBatch],
) -> Result<Vec<MosaicPointingGroup>, ImagingError> {
    let mut group_indices = BTreeMap::<(u64, u64, u64, (u8, u64, u64)), usize>::new();
    let mut groups = Vec::<MosaicPointingGroup>::new();
    for (batch, metadata) in batches.iter().zip(metadata_batches.iter()) {
        for sample_index in 0..batch.len() {
            let pointing_direction_rad = metadata.pointing_direction_rad[sample_index];
            let frequency_hz = metadata.beam_frequency_hz[sample_index];
            let key = (
                pointing_direction_rad[0].to_bits(),
                pointing_direction_rad[1].to_bits(),
                frequency_hz.to_bits(),
                primary_beam_model_key(metadata.primary_beam_model),
            );
            let group_index = match group_indices.get(&key).copied() {
                Some(index) => index,
                None => {
                    let index = groups.len();
                    group_indices.insert(key, index);
                    groups.push(MosaicPointingGroup {
                        pointing_direction_rad,
                        frequency_hz,
                        primary_beam_model: metadata.primary_beam_model,
                        batch: VisibilityBatch {
                            u_lambda: Vec::new(),
                            v_lambda: Vec::new(),
                            w_lambda: Vec::new(),
                            weight: Vec::new(),
                            sumwt_factor: Vec::new(),
                            gridable: Vec::new(),
                            visibility: Vec::new(),
                        },
                        sample_frequency_hz: Vec::new(),
                    });
                    index
                }
            };
            let entry = &mut groups[group_index];
            entry.batch.u_lambda.push(batch.u_lambda[sample_index]);
            entry.batch.v_lambda.push(batch.v_lambda[sample_index]);
            entry.batch.w_lambda.push(batch.w_lambda[sample_index]);
            entry.batch.weight.push(batch.weight[sample_index]);
            entry
                .batch
                .sumwt_factor
                .push(batch.sumwt_factor[sample_index]);
            entry.batch.gridable.push(batch.gridable[sample_index]);
            entry.batch.visibility.push(batch.visibility[sample_index]);
            entry
                .sample_frequency_hz
                .push(metadata.sample_frequency_hz[sample_index]);
        }
    }
    Ok(groups)
}

fn apply_mosaic_weighting(
    request: &ImagingRequest,
    config: &MosaicGridderConfig,
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, ImagingError> {
    if request.weighting == WeightingMode::Natural {
        return apply_weighting(request, gridder);
    }
    if request.visibility_batches.len() != config.metadata_batches.len() {
        return Err(ImagingError::InvalidRequest(
            "mosaic metadata batch count does not match visibility batch count".to_string(),
        ));
    }

    let mut grouped = BTreeMap::<(u64, u64), (Vec<(usize, usize)>, VisibilityBatch)>::new();
    for (batch_index, (batch, metadata)) in request
        .visibility_batches
        .iter()
        .zip(config.metadata_batches.iter())
        .enumerate()
    {
        if batch.len() != metadata.pointing_direction_rad.len() {
            return Err(ImagingError::InvalidRequest(
                "mosaic metadata sample count does not match visibility batch sample count"
                    .to_string(),
            ));
        }
        for sample_index in 0..batch.len() {
            let pointing = metadata.pointing_direction_rad[sample_index];
            let entry = grouped
                .entry((pointing[0].to_bits(), pointing[1].to_bits()))
                .or_insert_with(|| {
                    (
                        Vec::new(),
                        VisibilityBatch {
                            u_lambda: Vec::new(),
                            v_lambda: Vec::new(),
                            w_lambda: Vec::new(),
                            weight: Vec::new(),
                            sumwt_factor: Vec::new(),
                            gridable: Vec::new(),
                            visibility: Vec::new(),
                        },
                    )
                });
            entry.0.push((batch_index, sample_index));
            entry.1.u_lambda.push(batch.u_lambda[sample_index]);
            entry.1.v_lambda.push(batch.v_lambda[sample_index]);
            entry.1.w_lambda.push(batch.w_lambda[sample_index]);
            entry.1.weight.push(batch.weight[sample_index]);
            entry.1.sumwt_factor.push(batch.sumwt_factor[sample_index]);
            entry.1.gridable.push(batch.gridable[sample_index]);
            entry.1.visibility.push(batch.visibility[sample_index]);
        }
    }

    let mut weighted_batches = request.visibility_batches.clone();
    let fractional_bandwidth =
        fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz);
    let density_mode = match request.weighting {
        WeightingMode::BriggsBwTaper { .. } => WeightDensityMode::PerPlane,
        _ => WeightDensityMode::Combined,
    };
    for (_pointing, (positions, group_batch)) in grouped {
        let weighted_group = apply_weighting_with_density_source(
            request.weighting,
            density_mode,
            None,
            fractional_bandwidth,
            std::slice::from_ref(&group_batch),
            std::slice::from_ref(&group_batch),
            gridder,
        )?;
        let weighted_group = &weighted_group[0];
        for (group_index, (batch_index, sample_index)) in positions.into_iter().enumerate() {
            weighted_batches[batch_index].weight[sample_index] = weighted_group.weight[group_index];
            weighted_batches[batch_index].sumwt_factor[sample_index] =
                weighted_group.sumwt_factor[group_index];
        }
    }

    Ok(weighted_batches)
}

fn append_mosaic_trace(line: String) {
    let Some(path) = env::var_os("CASA_RS_MOSAIC_TRACE") else {
        return;
    };
    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut file) => {
            let _ = writeln!(file, "{line}");
        }
        Err(error) => {
            eprintln!("failed to append CASA_RS_MOSAIC_TRACE: {error}");
        }
    }
}

fn append_hogbom_trace(line: String) {
    let Some(path) = env::var_os("CASA_RS_HOGBOM_TRACE") else {
        return;
    };
    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut file) => {
            let _ = writeln!(file, "{line}");
        }
        Err(error) => {
            eprintln!("failed to append CASA_RS_HOGBOM_TRACE: {error}");
        }
    }
}

fn mosaic_trace_enabled() -> bool {
    env::var_os("CASA_RS_MOSAIC_TRACE").is_some()
}

fn mosaic_cell_trace_targets() -> Vec<(isize, isize)> {
    let Some(raw) = env::var_os("CASA_RS_MOSAIC_CELL_TRACE") else {
        return Vec::new();
    };
    raw.to_string_lossy()
        .split([';', ' ', '\n', '\t'])
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let (x, y) = entry.split_once(',')?;
            Some((x.trim().parse().ok()?, y.trim().parse().ok()?))
        })
        .collect()
}

fn mosaic_grid_dump_enabled() -> bool {
    env::var_os("CASA_RS_MOSAIC_GRID_DUMP").is_some()
}

fn dump_mosaic_complex_grid(role: &str, frequency_hz: f64, grid: &Array2<Complex64>) {
    let Some(root) = env::var_os("CASA_RS_MOSAIC_GRID_DUMP") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    if let Err(error) = std::fs::create_dir_all(&root) {
        eprintln!(
            "failed to create CASA_RS_MOSAIC_GRID_DUMP directory {}: {error}",
            root.display()
        );
        return;
    }
    let frequency_tag = format!("{frequency_hz:.0}");
    let meta_path = root.join(format!("rust-{role}-{frequency_tag}.json"));
    let data_path = root.join(format!("rust-{role}-{frequency_tag}.bin"));
    let (nx, ny) = grid.dim();
    let meta = format!(
        "{{\"producer\":\"rust\",\"role\":\"{}\",\"frequency_hz\":{:.17e},\"shape\":[{},{}],\"dtype\":\"complex128\",\"order\":\"x_y_re_im_f64_le\"}}\n",
        role, frequency_hz, nx, ny
    );
    if let Err(error) = std::fs::write(&meta_path, meta) {
        eprintln!(
            "failed to write mosaic grid dump metadata {}: {error}",
            meta_path.display()
        );
        return;
    }
    let mut file = match OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&data_path)
    {
        Ok(file) => file,
        Err(error) => {
            eprintln!(
                "failed to open mosaic grid dump {}: {error}",
                data_path.display()
            );
            return;
        }
    };
    for x in 0..nx {
        for y in 0..ny {
            let value = grid[(x, y)];
            if file.write_all(&value.re.to_le_bytes()).is_err()
                || file.write_all(&value.im.to_le_bytes()).is_err()
            {
                eprintln!("failed to write mosaic grid dump {}", data_path.display());
                return;
            }
        }
    }
}

fn mosaic_trace_pixels_json(
    residual: &Array2<f32>,
    psf: &Array2<f32>,
    normalization_weight: &Array2<f32>,
    output_weight: Option<&Array2<f32>>,
) -> String {
    let candidates = [
        ("center", residual.dim().0 / 2, residual.dim().1 / 2),
        ("central_source", 246usize, 259usize),
        ("worst_negative_source", 74usize, 199usize),
        ("worst_positive_source", 76usize, 383usize),
        ("pb_edge_source", 424usize, 110usize),
        ("upper_source", 133usize, 415usize),
        ("off_axis_source", 425usize, 137usize),
        ("off_axis_source_2", 426usize, 138usize),
    ];
    let mut entries = Vec::new();
    for (name, x, y) in candidates {
        if x >= residual.dim().0 || y >= residual.dim().1 {
            continue;
        }
        let output_weight_value = output_weight
            .map(|weight| format!("{:.17e}", weight[(x, y)]))
            .unwrap_or_else(|| "null".to_string());
        entries.push(format!(
            "{{\"name\":\"{}\",\"x\":{},\"y\":{},\"residual\":{:.17e},\"psf\":{:.17e},\"normalization_weight\":{:.17e},\"output_weight\":{}}}",
            name,
            x,
            y,
            residual[(x, y)],
            psf[(x, y)],
            normalization_weight[(x, y)],
            output_weight_value
        ));
    }
    format!("[{}]", entries.join(","))
}

fn mosaic_trace_group_pixels_json(residual: &Array2<f32>, weight: &Array2<f32>) -> String {
    let candidates = [
        ("center", residual.dim().0 / 2, residual.dim().1 / 2),
        ("central_source", 246usize, 259usize),
        ("worst_negative_source", 74usize, 199usize),
        ("worst_positive_source", 76usize, 383usize),
        ("pb_edge_source", 424usize, 110usize),
        ("upper_source", 133usize, 415usize),
        ("off_axis_source", 425usize, 137usize),
        ("off_axis_source_2", 426usize, 138usize),
    ];
    let mut entries = Vec::new();
    for (name, x, y) in candidates {
        if x >= residual.dim().0 || y >= residual.dim().1 {
            continue;
        }
        entries.push(format!(
            "{{\"name\":\"{}\",\"x\":{},\"y\":{},\"residual\":{:.17e},\"weight\":{:.17e}}}",
            name,
            x,
            y,
            residual[(x, y)],
            weight[(x, y)],
        ));
    }
    format!("[{}]", entries.join(","))
}

fn build_mosaic_weight_image(
    geometry: ImageGeometry,
    config: &MosaicGridderConfig,
    groups: &[MosaicPointingGroup],
) -> Result<Array2<f32>, ImagingError> {
    let mut pointing_weights =
        BTreeMap::<(u64, u64, (u8, u64, u64)), ([f64; 2], PrimaryBeamModel, f64, f64)>::new();
    for group in groups {
        if !mosaic_pointing_contributes_by_simple_pb_center(
            geometry,
            config.phase_center_direction_rad,
            group.pointing_direction_rad,
        ) {
            continue;
        }
        let mut group_weight = 0.0f64;
        for sample_index in 0..group.batch.len() {
            if !group.batch.gridable[sample_index] {
                continue;
            }
            let weight = group.batch.weight[sample_index];
            if weight.is_finite() && weight > 0.0 {
                group_weight += f64::from(weight);
            }
        }
        if !(group_weight.is_finite() && group_weight > 0.0) {
            continue;
        }
        let key = (
            group.pointing_direction_rad[0].to_bits(),
            group.pointing_direction_rad[1].to_bits(),
            primary_beam_model_key(group.primary_beam_model),
        );
        let entry = pointing_weights.entry(key).or_insert((
            group.pointing_direction_rad,
            group.primary_beam_model,
            0.0,
            0.0,
        ));
        entry.2 += group.frequency_hz * group_weight;
        entry.3 += group_weight;
    }

    let mut weight_image = Array2::<f32>::zeros((geometry.nx(), geometry.ny()));
    for (pointing_direction_rad, primary_beam_model, weighted_frequency_hz, pointing_weight) in
        pointing_weights.into_values()
    {
        if !(pointing_weight.is_finite() && pointing_weight > 0.0) {
            continue;
        }
        let frequency_hz = weighted_frequency_hz / pointing_weight;
        let [pointing_x, pointing_y] = mosaic_pointing_pixel_position(
            geometry,
            config.phase_center_direction_rad,
            pointing_direction_rad,
        );
        for x in 0..geometry.nx() {
            let l = (x as f64 - pointing_x) * geometry.cell_size_rad[0].abs();
            for y in 0..geometry.ny() {
                let m = (y as f64 - pointing_y) * geometry.cell_size_rad[1].abs();
                let radius_rad = (l * l + m * m).sqrt();
                let vp = primary_beam_voltage_pattern(primary_beam_model, radius_rad, frequency_hz);
                let pb = vp * vp;
                weight_image[(x, y)] += (pointing_weight as f32) * pb * pb;
            }
        }
    }
    if weight_image
        .iter()
        .copied()
        .any(|value| value.is_finite() && value > 0.0)
    {
        Ok(weight_image)
    } else {
        Err(ImagingError::Normalization(
            "mosaic weight image has no finite positive samples".to_string(),
        ))
    }
}

fn build_mosaic_clean_mask(
    weight_image: &Array2<f32>,
    pb_limit: f32,
    user_clean_mask: Option<&Array2<bool>>,
) -> Result<Array2<bool>, ImagingError> {
    let peak = weight_image
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .fold(0.0f32, f32::max);
    if !(peak.is_finite() && peak > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic PB clean mask peak is non-finite or zero".to_string(),
        ));
    }
    let threshold = pb_limit.abs() * pb_limit.abs() * peak;
    let mut mask = Array2::<bool>::from_elem(weight_image.dim(), false);
    for ((x, y), value) in weight_image.indexed_iter() {
        let in_pb_support = value.is_finite() && *value > threshold;
        let in_user_mask = user_clean_mask.is_none_or(|user_mask| user_mask[(x, y)]);
        mask[(x, y)] = in_pb_support && in_user_mask;
    }
    Ok(mask)
}

fn request_for_mosaic_clean_mask(
    request: &ImagingRequest,
    clean_mask: Array2<bool>,
) -> ImagingRequest {
    ImagingRequest {
        geometry: request.geometry,
        visibility_batches: Vec::new(),
        gridder_mode: GridderMode::Standard,
        plane_stokes: request.plane_stokes,
        weighting: request.weighting,
        reffreq_hz: request.reffreq_hz,
        selected_frequency_range_hz: request.selected_frequency_range_hz,
        deconvolver: request.deconvolver,
        multiscale_scales: request.multiscale_scales.clone(),
        small_scale_bias: request.small_scale_bias,
        clean: request.clean,
        clean_mask: Some(clean_mask),
        initial_model: None,
        w_term_mode: request.w_term_mode,
        w_project_planes: request.w_project_planes,
        compatibility: request.compatibility,
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_mosaic_residual(
    request: &ImagingRequest,
    config: &MosaicGridderConfig,
    gridder: &StandardGridder,
    groups: &[MosaicPointingGroup],
    projector_cache: &mut MosaicProjectorCache,
    conv_sampling: usize,
    model: &Array2<f32>,
    psf_state: &PsfState,
    weight_image: &Array2<f32>,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    let refresh_started = Instant::now();
    let [nx, ny] = request.geometry.image_shape;
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut accumulated_residual_image = Array2::<f32>::zeros((nx, ny));
    let mut timings = ResidualComputationTimings::default();
    let model_grid = if model.iter().any(|value| value.abs() > 0.0) {
        let model_fft_started = Instant::now();
        let model_for_prediction =
            flat_sky_mosaic_model_for_prediction(model, weight_image, config.pb_limit)?;
        let transformed =
            centered_fft2(&gridder.apodize_mosaic_model(&model_for_prediction, conv_sampling));
        timings.model_fft = model_fft_started.elapsed();
        Some(transformed)
    } else {
        None
    };

    let degrid_started = Instant::now();
    for group in groups {
        if !mosaic_pointing_contributes_by_simple_pb_center(
            request.geometry,
            config.phase_center_direction_rad,
            group.pointing_direction_rad,
        ) {
            continue;
        }
        let projector = cached_mosaic_projector(
            projector_cache,
            request.geometry,
            gridder,
            config.phase_center_direction_rad,
            group.pointing_direction_rad,
            group.primary_beam_model,
            group.frequency_hz,
            conv_sampling,
            2,
        )?;
        let mut group_residual_grid = Array2::<Complex64>::zeros((grid_nx, grid_ny));
        for sample_index in 0..group.batch.len() {
            if !group.batch.gridable[sample_index] {
                continue;
            }
            let weight = group.batch.weight[sample_index];
            let sumwt_factor = group.batch.sumwt_factor[sample_index];
            let visibility = group.batch.visibility[sample_index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0
                && visibility.re.is_finite()
                && visibility.im.is_finite())
            {
                continue;
            }
            let Some(plan) = projector.plan_sample(
                group.batch.u_lambda[sample_index],
                group.batch.v_lambda[sample_index],
            ) else {
                continue;
            };
            let predicted = model_grid
                .as_ref()
                .map(|grid| projector.degrid_sample_planned(grid, &plan))
                .unwrap_or_else(|| Complex32::new(0.0, 0.0));
            let residual_visibility = visibility - predicted;
            let grid_weight = weight * sumwt_factor;
            projector.grid_sample_planned_f64(
                &mut group_residual_grid,
                &plan,
                Complex64::new(residual_visibility.re as f64, residual_visibility.im as f64)
                    * f64::from(grid_weight),
            );
        }
        let fft_started = Instant::now();
        let raw_group_residual = centered_ifft2_f64(&group_residual_grid);
        timings.fft += fft_started.elapsed();
        let normalize_started = Instant::now();
        let group_residual_image =
            gridder.corrected_mosaic_image_from_grid_f64(&raw_group_residual, conv_sampling);
        Zip::from(&mut accumulated_residual_image)
            .and(&group_residual_image)
            .for_each(|accumulated, residual_value| {
                *accumulated += *residual_value;
            });
        timings.normalize += normalize_started.elapsed();
    }
    timings.degrid_grid = degrid_started
        .elapsed()
        .saturating_sub(timings.fft + timings.normalize);

    let normalize_started = Instant::now();
    let fft_sumwt_scale =
        (request.geometry.nx() * request.geometry.ny()) as f32 / psf_state.reported_sumwt;
    let mut residual = accumulated_residual_image;
    residual.mapv_inplace(|value| value * fft_sumwt_scale);
    let weight_peak = weight_image
        .iter()
        .copied()
        .fold(0.0f32, |peak, value| peak.max(value));
    if !(weight_peak.is_finite() && weight_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic residual refresh weight peak is non-finite or zero".to_string(),
        ));
    }
    let pb_limit_threshold = config.pb_limit.abs() * config.pb_limit.abs() * weight_peak;
    for ((x, y), weight_value) in weight_image.indexed_iter() {
        let sensitivity = weight_value.max(0.0);
        if sensitivity > pb_limit_threshold {
            residual[(x, y)] /= (sensitivity * weight_peak).sqrt();
        } else {
            residual[(x, y)] = 0.0;
        }
    }
    timings.normalize += normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    stage_timings.major_cycle_refresh += refresh_started.elapsed();
    Ok(residual)
}

fn flat_sky_mosaic_model_for_prediction(
    model: &Array2<f32>,
    weight_image: &Array2<f32>,
    pb_limit: f32,
) -> Result<Array2<f32>, ImagingError> {
    let weight_peak = weight_image
        .iter()
        .copied()
        .fold(0.0f32, |peak, value| peak.max(value));
    if !(weight_peak.is_finite() && weight_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic model prediction weight peak is non-finite or zero".to_string(),
        ));
    }
    let pb_scale_factor = weight_peak.sqrt();
    let mut prediction_model = Array2::<f32>::zeros(model.dim());
    for ((x, y), model_value) in model.indexed_iter() {
        let deno = weight_image[(x, y)].abs().sqrt() / pb_scale_factor;
        if deno.is_finite() && deno > pb_limit.abs() {
            prediction_model[(x, y)] = *model_value / deno;
        }
    }
    Ok(prediction_model)
}

#[allow(clippy::too_many_arguments)]
fn cached_mosaic_projector(
    cache: &mut MosaicProjectorCache,
    geometry: ImageGeometry,
    gridder: &StandardGridder,
    phase_center_direction_rad: [f64; 2],
    pointing_direction_rad: [f64; 2],
    primary_beam_model: PrimaryBeamModel,
    frequency_hz: f64,
    conv_sampling: usize,
    screen_power: u8,
) -> Result<ScreenProjector, ImagingError> {
    let key = (
        primary_beam_model_key(primary_beam_model),
        frequency_hz.to_bits(),
        screen_power,
    );
    let base = if let Some(projector) = cache.get(&key) {
        projector.clone()
    } else {
        let projector = build_mosaic_projector(
            geometry,
            gridder,
            phase_center_direction_rad,
            phase_center_direction_rad,
            primary_beam_model,
            frequency_hz,
            conv_sampling,
            screen_power,
            false,
        )?;
        cache.insert(key, projector.clone());
        projector
    };
    let pixel_offset =
        mosaic_pointing_pixel_offset(geometry, phase_center_direction_rad, pointing_direction_rad);
    let phase_gradient_rad_per_sample = [
        -pixel_offset[0] * std::f64::consts::TAU / (geometry.nx() as f64 * conv_sampling as f64),
        -pixel_offset[1] * std::f64::consts::TAU / (geometry.ny() as f64 * conv_sampling as f64),
    ];
    Ok(base.with_phase_gradient(phase_gradient_rad_per_sample))
}

#[allow(clippy::too_many_arguments)]
fn build_mosaic_projector(
    geometry: ImageGeometry,
    gridder: &StandardGridder,
    phase_center_direction_rad: [f64; 2],
    pointing_direction_rad: [f64; 2],
    primary_beam_model: PrimaryBeamModel,
    frequency_hz: f64,
    conv_sampling: usize,
    screen_power: u8,
    apply_phase_gradient: bool,
) -> Result<ScreenProjector, ImagingError> {
    let projector = ScreenProjector::from_hetarray_screens(
        geometry,
        gridder,
        conv_sampling,
        mosaic_hetarray_screen_conv_size(geometry, primary_beam_model, frequency_hz),
        |l, m| {
            let radius_rad = (l * l + m * m).sqrt();
            let vp = primary_beam_voltage_pattern(primary_beam_model, radius_rad, frequency_hz);
            let value = match screen_power {
                1 => vp,
                2 => vp * vp,
                4 => {
                    let pb = vp * vp;
                    pb * pb
                }
                _ => unreachable!("unsupported mosaic screen power"),
            };
            Complex32::new(value, 0.0)
        },
        |l, m| {
            let radius_rad = (l * l + m * m).sqrt();
            let vp = primary_beam_voltage_pattern(primary_beam_model, radius_rad, frequency_hz);
            let pb = vp * vp;
            Complex32::new(pb * pb, 0.0)
        },
    )?;
    if !apply_phase_gradient {
        return Ok(projector);
    }
    let pixel_offset =
        mosaic_pointing_pixel_offset(geometry, phase_center_direction_rad, pointing_direction_rad);
    let phase_gradient_rad_per_sample = [
        -pixel_offset[0] * std::f64::consts::TAU / (geometry.nx() as f64 * conv_sampling as f64),
        -pixel_offset[1] * std::f64::consts::TAU / (geometry.ny() as f64 * conv_sampling as f64),
    ];
    Ok(projector.with_phase_gradient(phase_gradient_rad_per_sample))
}

fn mosaic_projector_sampling(geometry: ImageGeometry) -> usize {
    // CASA's HetArrayConvFunc path uses the `mosaic.oversampling` Aipsrc value,
    // which defaults to 10, after enforcing a minimum of 10.
    let _ = geometry;
    10
}

fn mosaic_hetarray_screen_conv_size(
    geometry: ImageGeometry,
    primary_beam_model: PrimaryBeamModel,
    frequency_hz: f64,
) -> usize {
    let pb_support_pixels =
        primary_beam_support_diameter_pixels(primary_beam_model, geometry, frequency_hz)
            .floor()
            .max(0.0) as usize;
    hetarray_screen_conv_size_for_support(geometry, pb_support_pixels)
}

fn primary_beam_support_diameter_pixels(
    primary_beam_model: PrimaryBeamModel,
    geometry: ImageGeometry,
    frequency_hz: f64,
) -> f64 {
    if !(frequency_hz.is_finite() && frequency_hz > 0.0) {
        return 0.0;
    }
    let Some(max_radius_arcsec_at_100ghz) =
        primary_beam_max_radius_arcsec_at_100ghz(primary_beam_model)
    else {
        return 0.0;
    };
    let radius_rad = (max_radius_arcsec_at_100ghz / 3600.0).to_radians() * (100.0e9 / frequency_hz);
    let pixel_rad = geometry.cell_size_rad[0]
        .abs()
        .min(geometry.cell_size_rad[1].abs());
    if pixel_rad > 0.0 {
        2.0 * radius_rad / pixel_rad
    } else {
        0.0
    }
}

fn mosaic_pointing_contributes_by_simple_pb_center(
    geometry: ImageGeometry,
    phase_center_direction_rad: [f64; 2],
    pointing_direction_rad: [f64; 2],
) -> bool {
    // CASA's SimplePBConvFunc::addPBToFlux() records PB coverage only when
    // the pointing center converts to a pixel inside the target image. This is
    // intentionally a center-pixel rule, not a PB-wing overlap estimate; an
    // earlier broader estimate admitted far off-image pointings and broke
    // `papersky_mosaic.ms` parity.
    let [pixel_x, pixel_y] = mosaic_pointing_pixel_position(
        geometry,
        phase_center_direction_rad,
        pointing_direction_rad,
    );
    mosaic_pointing_pixel_inside_image(geometry, [pixel_x, pixel_y])
}

fn mosaic_pointing_pixel_inside_image(geometry: ImageGeometry, pixel: [f64; 2]) -> bool {
    pixel[0] >= 0.0
        && pixel[0] < geometry.nx() as f64
        && pixel[1] >= 0.0
        && pixel[1] < geometry.ny() as f64
}

fn mosaic_pointing_pixel_offset(
    geometry: ImageGeometry,
    phase_center_direction_rad: [f64; 2],
    pointing_direction_rad: [f64; 2],
) -> [f64; 2] {
    let [pixel_x, pixel_y] = mosaic_pointing_pixel_position(
        geometry,
        phase_center_direction_rad,
        pointing_direction_rad,
    );
    [
        pixel_x - geometry.nx() as f64 / 2.0,
        pixel_y - geometry.ny() as f64 / 2.0,
    ]
}

fn mosaic_pointing_pixel_position(
    geometry: ImageGeometry,
    phase_center_direction_rad: [f64; 2],
    pointing_direction_rad: [f64; 2],
) -> [f64; 2] {
    let delta_ra =
        circular_angle_delta_rad(pointing_direction_rad[0] - phase_center_direction_rad[0]);
    let dec = pointing_direction_rad[1];
    let dec0 = phase_center_direction_rad[1];
    let l = dec.cos() * delta_ra.sin();
    let m = dec.sin() * dec0.cos() - dec.cos() * dec0.sin() * delta_ra.cos();
    [
        geometry.nx() as f64 / 2.0 - l / geometry.cell_size_rad[0].abs(),
        geometry.ny() as f64 / 2.0 + m / geometry.cell_size_rad[1].abs(),
    ]
}

fn circular_angle_delta_rad(angle_rad: f64) -> f64 {
    (angle_rad + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU) - std::f64::consts::PI
}

/// Return the CASA-compatible voltage-pattern value for a homogeneous primary beam.
pub fn primary_beam_voltage_pattern(
    primary_beam_model: PrimaryBeamModel,
    radius_rad: f64,
    frequency_hz: f64,
) -> f32 {
    match primary_beam_model {
        PrimaryBeamModel::Airy {
            dish_diameter_m,
            blockage_diameter_m,
        } => airy_voltage_pattern(
            radius_rad,
            frequency_hz,
            dish_diameter_m,
            blockage_diameter_m,
        ),
        PrimaryBeamModel::EvlaLBandCommon => evla_common_voltage_pattern(radius_rad, frequency_hz),
    }
}

fn airy_voltage_pattern(
    radius_rad: f64,
    frequency_hz: f64,
    dish_diameter_m: f64,
    blockage_diameter_m: f64,
) -> f32 {
    if !(radius_rad.is_finite()
        && radius_rad >= 0.0
        && frequency_hz.is_finite()
        && frequency_hz > 0.0)
    {
        return 0.0;
    }
    // CASA PBMath1D stores per-pixel radius terms as Float before looking up
    // the tabulated voltage pattern.
    let radius_arcmin_ghz = (radius_rad.to_degrees() * 60.0 * (frequency_hz / 1.0e9)) as f32 as f64;
    let maximum_radius_arcmin_ghz = casa_airy_max_radius_arcmin_ghz();
    if radius_arcmin_ghz > maximum_radius_arcmin_ghz {
        return 0.0;
    }
    let sample_count_minus_one = 9_999.0;
    let index = (radius_arcmin_ghz * sample_count_minus_one / maximum_radius_arcmin_ghz)
        .floor()
        .clamp(0.0, sample_count_minus_one);
    let dimensionless_max_radius =
        maximum_radius_arcmin_ghz * 7.016 / (1.566 * 60.0) * dish_diameter_m / 24.5;
    let x = index * dimensionless_max_radius / sample_count_minus_one;
    if x.abs() <= f64::EPSILON {
        return 1.0;
    }
    if blockage_diameter_m <= 0.0 {
        return (2.0 * j1(x) / x) as f32;
    }
    let area_ratio = (dish_diameter_m / blockage_diameter_m).powi(2);
    let area_norm = area_ratio - 1.0;
    let length_ratio = dish_diameter_m / blockage_diameter_m;
    ((area_ratio * 2.0 * j1(x) / x - 2.0 * j1(x * length_ratio) / (x * length_ratio)) / area_norm)
        as f32
}

fn primary_beam_max_radius_arcsec_at_100ghz(primary_beam_model: PrimaryBeamModel) -> Option<f64> {
    match primary_beam_model {
        PrimaryBeamModel::Airy {
            dish_diameter_m,
            blockage_diameter_m,
        } => Some(airy_max_radius_arcsec_at_100ghz(
            dish_diameter_m,
            blockage_diameter_m,
        )),
        PrimaryBeamModel::EvlaLBandCommon => Some(58.0 * 60.0 / 100.0),
    }
}

fn airy_max_radius_arcsec_at_100ghz(dish_diameter_m: f64, blockage_diameter_m: f64) -> f64 {
    let _ = (dish_diameter_m, blockage_diameter_m);
    casa_airy_max_radius_arcmin_ghz() * 60.0 / 100.0
}

fn casa_airy_max_radius_arcmin_ghz() -> f64 {
    1.784 * 60.0
}

fn evla_common_voltage_pattern(radius_rad: f64, frequency_hz: f64) -> f32 {
    if !(radius_rad.is_finite()
        && radius_rad >= 0.0
        && frequency_hz.is_finite()
        && frequency_hz > 0.0)
    {
        return 0.0;
    }
    // Mirror CASA PBMath1DEVLA::nearestVPArray(), PBMath1DPoly::fillPBArray(),
    // and PBMath1D::apply(): CASA samples 10,000 voltage-pattern entries and
    // then uses integer-truncated radial lookup when applying the image PB.
    let coefficients = nearest_evla_common_coefficients(frequency_hz * 1.0e-6);
    let radius_arcmin_ghz = radius_rad.to_degrees() * 60.0 * (frequency_hz / 1.0e9);
    if radius_arcmin_ghz > 58.0 {
        return 0.0;
    }
    let inverse_increment_radius = 9_999.0 / 58.0;
    let sample_index = (radius_arcmin_ghz * inverse_increment_radius).floor();
    let sampled_radius_arcmin_ghz = sample_index / inverse_increment_radius;
    let x2 = sampled_radius_arcmin_ghz * sampled_radius_arcmin_ghz;
    let mut taper = 0.0f64;
    let mut power = 1.0f64;
    for coefficient in coefficients {
        taper += coefficient * power;
        power *= x2;
    }
    if taper <= 0.0 {
        0.0
    } else {
        taper.sqrt() as f32
    }
}

fn nearest_evla_common_coefficients(frequency_mhz: f64) -> [f64; 4] {
    let (clamped_frequency_mhz, coefficients) = if (3990.0..=8001.0).contains(&frequency_mhz) {
        (
            frequency_mhz.clamp(4052.0, 7948.0),
            EVLA_C_BAND_COEFFICIENTS,
        )
    } else {
        (
            frequency_mhz.clamp(1040.0, 2000.0),
            EVLA_L_BAND_COEFFICIENTS,
        )
    };
    let mut best = coefficients[0].1;
    let mut best_delta_mhz = f64::INFINITY;
    for &(candidate_frequency_mhz, candidate_coefficients) in coefficients {
        let delta_mhz = (clamped_frequency_mhz - candidate_frequency_mhz).abs();
        if delta_mhz < best_delta_mhz {
            best_delta_mhz = delta_mhz;
            best = candidate_coefficients;
        }
    }
    best
}

const EVLA_L_BAND_COEFFICIENTS: &[(f64, [f64; 4])] = &[
    (1040.0, [1.000, -1.529e-3, 8.69e-7, -1.88e-10]),
    (1104.0, [1.000, -1.486e-3, 8.15e-7, -1.68e-10]),
    (1168.0, [1.000, -1.439e-3, 7.53e-7, -1.45e-10]),
    (1232.0, [1.000, -1.450e-3, 7.87e-7, -1.63e-10]),
    (1296.0, [1.000, -1.428e-3, 7.62e-7, -1.54e-10]),
    (1360.0, [1.000, -1.449e-3, 8.02e-7, -1.74e-10]),
    (1424.0, [1.000, -1.462e-3, 8.23e-7, -1.83e-10]),
    (1488.0, [1.000, -1.455e-3, 7.92e-7, -1.63e-10]),
    (1552.0, [1.000, -1.435e-3, 7.54e-7, -1.49e-10]),
    (1680.0, [1.000, -1.443e-3, 7.74e-7, -1.57e-10]),
    (1744.0, [1.000, -1.462e-3, 8.02e-7, -1.69e-10]),
    (1808.0, [1.000, -1.488e-3, 8.38e-7, -1.83e-10]),
    (1872.0, [1.000, -1.486e-3, 8.26e-7, -1.75e-10]),
    (1936.0, [1.000, -1.459e-3, 7.93e-7, -1.62e-10]),
    (2000.0, [1.000, -1.508e-3, 8.31e-7, -1.68e-10]),
];

const EVLA_C_BAND_COEFFICIENTS: &[(f64, [f64; 4])] = &[
    (4052.0, [1.000, -1.406e-3, 7.41e-7, -1.48e-10]),
    (4180.0, [1.000, -1.385e-3, 7.09e-7, -1.36e-10]),
    (4308.0, [1.000, -1.380e-3, 7.08e-7, -1.37e-10]),
    (4436.0, [1.000, -1.362e-3, 6.95e-7, -1.35e-10]),
    (4564.0, [1.000, -1.365e-3, 6.92e-7, -1.31e-10]),
    (4692.0, [1.000, -1.339e-3, 6.56e-7, -1.17e-10]),
    (4820.0, [1.000, -1.371e-3, 7.06e-7, -1.40e-10]),
    (4948.0, [1.000, -1.358e-3, 6.91e-7, -1.34e-10]),
    (5052.0, [1.000, -1.360e-3, 6.91e-7, -1.33e-10]),
    (5180.0, [1.000, -1.353e-3, 6.74e-7, -1.25e-10]),
    (5308.0, [1.000, -1.359e-3, 6.82e-7, -1.27e-10]),
    (5436.0, [1.000, -1.380e-3, 7.05e-7, -1.37e-10]),
    (5564.0, [1.000, -1.376e-3, 6.99e-7, -1.31e-10]),
    (5692.0, [1.000, -1.405e-3, 7.39e-7, -1.47e-10]),
    (5820.0, [1.000, -1.394e-3, 7.29e-7, -1.45e-10]),
    (5948.0, [1.000, -1.428e-3, 7.57e-7, -1.57e-10]),
    (6052.0, [1.000, -1.445e-3, 7.68e-7, -1.50e-10]),
    (6148.0, [1.000, -1.422e-3, 7.38e-7, -1.38e-10]),
    (6308.0, [1.000, -1.463e-3, 7.94e-7, -1.62e-10]),
    (6436.0, [1.000, -1.478e-3, 8.22e-7, -1.74e-10]),
    (6564.0, [1.000, -1.473e-3, 8.00e-7, -1.62e-10]),
    (6692.0, [1.000, -1.455e-3, 7.76e-7, -1.53e-10]),
    (6820.0, [1.000, -1.487e-3, 8.22e-7, -1.72e-10]),
    (6948.0, [1.000, -1.472e-3, 8.05e-7, -1.67e-10]),
    (7052.0, [1.000, -1.470e-3, 8.01e-7, -1.64e-10]),
    (7180.0, [1.000, -1.503e-3, 8.50e-7, -1.84e-10]),
    (7308.0, [1.000, -1.482e-3, 8.19e-7, -1.72e-10]),
    (7436.0, [1.000, -1.498e-3, 8.22e-7, -1.66e-10]),
    (7564.0, [1.000, -1.490e-3, 8.18e-7, -1.66e-10]),
    (7692.0, [1.000, -1.481e-3, 7.98e-7, -1.56e-10]),
    (7820.0, [1.000, -1.474e-3, 7.94e-7, -1.57e-10]),
    (7948.0, [1.000, -1.448e-3, 7.69e-7, -1.51e-10]),
];

struct CottonSchwabState {
    residual: Array2<f32>,
    major_cycles: usize,
    minor_iterations: usize,
    clean_stop_reason: Option<CleanStopReason>,
    minor_cycle_traces: Vec<MinorCycleTrace>,
    final_cycle_threshold_jy_per_beam: f32,
}

type StandardMfsResidualRefreshCallback<'a> =
    dyn FnMut(&Array2<f32>, &mut ImagingStageTimings) -> Result<Array2<f32>, ImagingError> + 'a;

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
        model_flux_jy: model.iter().copied().sum(),
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
    standard_executor: &mut Option<StandardMfsCpuExecutor<'_>>,
    gridder: &StandardGridder,
    psf_state: &PsfState,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
    model: &mut Array2<f32>,
    residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    let mut refresh_residual = |model: &Array2<f32>,
                                stage_timings: &mut ImagingStageTimings|
     -> Result<Array2<f32>, ImagingError> {
        refresh_standard_mfs_residual(
            request,
            weighted_batches,
            standard_executor,
            gridder,
            model,
            psf_state,
            execution_config,
            stage_timings,
        )
    };
    run_cotton_schwab_controller_with_refresh(
        request,
        psf_state,
        stage_timings,
        &mut refresh_residual,
        model,
        residual,
        max_psf_sidelobe_level,
        initial_peak,
        warnings,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_cotton_schwab_controller_with_refresh(
    request: &ImagingRequest,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    refresh_residual: &mut StandardMfsResidualRefreshCallback<'_>,
    model: &mut Array2<f32>,
    residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    match request.deconvolver {
        Deconvolver::Hogbom => run_hogbom_cotton_schwab(
            request,
            psf_state,
            stage_timings,
            refresh_residual,
            model,
            residual,
            max_psf_sidelobe_level,
            initial_peak,
            warnings,
        ),
        Deconvolver::Clark => run_clark_cotton_schwab(
            request,
            psf_state,
            stage_timings,
            refresh_residual,
            model,
            residual,
            max_psf_sidelobe_level,
            initial_peak,
            warnings,
        ),
        Deconvolver::Multiscale => run_multiscale_cotton_schwab(
            request,
            psf_state,
            stage_timings,
            refresh_residual,
            model,
            residual,
            max_psf_sidelobe_level,
            initial_peak,
            warnings,
        ),
        Deconvolver::Mtmfs => Err(ImagingError::Unsupported(
            "standard MFS CLEAN does not support deconvolver='mtmfs'; use run_mtmfs()".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_mosaic_image_domain_controller(
    request: &ImagingRequest,
    config: &MosaicGridderConfig,
    gridder: &StandardGridder,
    groups: &[MosaicPointingGroup],
    projector_cache: &mut MosaicProjectorCache,
    conv_sampling: usize,
    weight_image: &Array2<f32>,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    model: &mut Array2<f32>,
    mut residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    if request.deconvolver == Deconvolver::Mtmfs {
        return Err(ImagingError::Unsupported(
            "mosaic gridder does not support deconvolver='mtmfs'".to_string(),
        ));
    }
    let mut multiscale_state = (request.deconvolver == Deconvolver::Multiscale).then(|| {
        let scales = effective_multiscale_scales(request);
        build_multiscale_state(
            &residual,
            &psf_state.psf,
            &scales,
            request.small_scale_bias,
            request.clean_mask.as_ref(),
        )
    });
    let clark_psf_patch = (request.deconvolver == Deconvolver::Clark).then(|| {
        build_clark_psf_patch(
            &psf_state.psf,
            request.geometry.cell_size_rad,
            request.clean.psf_cutoff,
        )
    });
    let mut minor_iterations = 0usize;
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;
    let mut residual_needs_refresh = false;
    while reported_minor_iterations < request.clean.niter {
        if request
            .clean
            .major_cycle_limit
            .is_some_and(|limit| major_cycles >= limit)
        {
            clean_stop_reason = Some(CleanStopReason::MajorCycleLimitReached);
            break;
        }
        let cycle_peak = match request.deconvolver {
            Deconvolver::Multiscale => multiscale_state.as_ref().map(|state| {
                peak_abs_value_masked(&state.dirty_conv_scales[0], request.clean_mask.as_ref())
            }),
            _ => peak_location_masked(&residual, request.clean_mask.as_ref())
                .map(|(_, value)| value.abs()),
        };
        let Some(cycle_peak) = cycle_peak else {
            clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
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

        let (outcome, probe) = match request.deconvolver {
            Deconvolver::Hogbom => (
                run_hogbom_minor_cycle(
                    request,
                    psf_state,
                    model,
                    &mut residual,
                    cycle_reported_niter,
                    final_cycle_threshold_jy_per_beam,
                    cycle_nsigma_threshold_jy_per_beam,
                    stage_timings,
                ),
                MinorCycleProbe::default(),
            ),
            Deconvolver::Clark => {
                let patch = clark_psf_patch.as_ref().expect("Clark patch should exist");
                (
                    run_clark_minor_cycle(
                        request,
                        &psf_state.psf,
                        model,
                        &mut residual,
                        cycle_reported_niter,
                        final_cycle_threshold_jy_per_beam,
                        cycle_nsigma_threshold_jy_per_beam,
                        patch,
                        stage_timings,
                    ),
                    MinorCycleProbe::default(),
                )
            }
            Deconvolver::Multiscale => {
                let state = multiscale_state
                    .as_mut()
                    .expect("multiscale state should exist");
                let probe = select_multiscale_candidate(state, request.clean_mask.as_ref())
                    .map(|candidate| MinorCycleProbe {
                        initial_scale_pixels: Some(
                            effective_multiscale_scales(request)[candidate.scale_index],
                        ),
                        initial_candidate_strength_jy_per_beam: Some(candidate.strength),
                        initial_candidate_position: Some([
                            candidate.position.0,
                            candidate.position.1,
                        ]),
                    })
                    .unwrap_or_default();
                (
                    run_multiscale_minor_cycle(
                        request,
                        &psf_state.psf,
                        state,
                        model,
                        &mut residual,
                        cycle_reported_niter,
                        final_cycle_threshold_jy_per_beam,
                        cycle_nsigma_threshold_jy_per_beam,
                        stage_timings,
                    ),
                    probe,
                )
            }
            Deconvolver::Mtmfs => unreachable!("MTMFS mosaic was rejected above"),
        };

        minor_cycle_traces.push(make_minor_cycle_trace(
            minor_cycle_traces.len(),
            start_reported_iteration,
            outcome,
            cycle_peak,
            &residual,
            model,
            probe,
        ));
        minor_iterations += outcome.actual_updates;
        reported_minor_iterations += outcome.reported_updates;
        let mut stop_after_refresh = None::<CleanStopReason>;
        if let Some(reason) = outcome.stop_reason {
            match reason {
                CleanStopReason::CycleThresholdReached if outcome.updated_model => {}
                CleanStopReason::CycleThresholdReached => {
                    clean_stop_reason = Some(reason);
                }
                _ => {
                    clean_stop_reason = Some(reason);
                    stop_after_refresh = Some(reason);
                }
            }
        }
        if !outcome.updated_model {
            break;
        }
        residual_needs_refresh = true;
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

        residual = compute_mosaic_residual(
            request,
            config,
            gridder,
            groups,
            projector_cache,
            conv_sampling,
            model,
            psf_state,
            weight_image,
            stage_timings,
        )?;
        major_cycles += 1;
        residual_needs_refresh = false;
        if let Some(state) = multiscale_state.as_mut() {
            refresh_multiscale_dirty_conv_scales(state, &residual);
        }
        let refreshed_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        let refreshed_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if stop_after_refresh.is_some() {
            break;
        }
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
    if residual_needs_refresh {
        residual = compute_mosaic_residual(
            request,
            config,
            gridder,
            groups,
            projector_cache,
            conv_sampling,
            model,
            psf_state,
            weight_image,
            stage_timings,
        )?;
        major_cycles += 1;
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
fn run_hogbom_cotton_schwab(
    request: &ImagingRequest,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    refresh_residual: &mut StandardMfsResidualRefreshCallback<'_>,
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
    let mut residual_needs_refresh = false;

    while reported_minor_iterations < request.clean.niter {
        if request
            .clean
            .major_cycle_limit
            .is_some_and(|limit| major_cycles >= limit)
        {
            clean_stop_reason = Some(CleanStopReason::MajorCycleLimitReached);
            break;
        }
        let cycle_setup_started = Instant::now();
        let Some((_, cycle_peak_value)) =
            peak_location_masked(&residual, request.clean_mask.as_ref())
        else {
            stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
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
            stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
            clean_stop_reason = Some(stop_reason);
            break;
        }
        let remaining_reported = request.clean.niter - reported_minor_iterations;
        let cycle_reported_niter = remaining_reported.min(request.clean.minor_cycle_length);
        let start_reported_iteration = reported_minor_iterations;
        let cycle_threshold_jy_per_beam =
            compute_cycle_threshold(cycle_peak, max_psf_sidelobe_level, request.clean);
        stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
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
        let mut stop_after_refresh = None::<CleanStopReason>;
        if let Some(reason) = outcome.stop_reason {
            match reason {
                CleanStopReason::CycleThresholdReached if outcome.updated_model => {}
                CleanStopReason::CycleThresholdReached => {
                    clean_stop_reason = Some(reason);
                }
                _ => {
                    clean_stop_reason = Some(reason);
                    stop_after_refresh = Some(reason);
                }
            }
        }
        if !outcome.updated_model {
            break;
        }
        residual_needs_refresh = true;
        let minor_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        update_divergence_state(
            warnings,
            &mut min_residual_peak_jy_per_beam,
            minor_peak,
            &mut divergence_warned,
        );
        if reported_minor_iterations >= request.clean.niter {
            clean_stop_reason = Some(CleanStopReason::IterationLimitReached);
            break;
        }
        residual = refresh_residual(model, stage_timings)?;
        major_cycles += 1;
        residual_needs_refresh = false;
        let refreshed_peak = peak_abs_value_masked(&residual, request.clean_mask.as_ref());
        let refreshed_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if stop_after_refresh.is_some() {
            break;
        }
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
    if residual_needs_refresh {
        residual = refresh_residual(model, stage_timings)?;
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
    let cycle_component_budget = hogbom_component_budget(cycle_reported_niter, request.clean);
    let mut cycle_component_updates = 0usize;
    let mut updated_model = false;
    let mut stop_reason = None;
    let minor_started = Instant::now();
    while cycle_component_updates < cycle_component_budget {
        let Some(((peak_x, peak_y), peak_value)) =
            hclean_peak_location_masked(residual, request.clean_mask.as_ref())
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
        append_hogbom_trace(format!(
            "{{\"event\":\"component\",\"cycle_update\":{},\"x\":{},\"y\":{},\"peak\":{:.17e},\"component\":{:.17e}}}",
            cycle_component_updates, peak_x, peak_y, peak_value, component
        ));
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
    let cycle_peak = peak_abs_value_masked(
        &multiscale_state.dirty_conv_scales[0],
        request.clean_mask.as_ref(),
    );
    let initial_cycle_component = cycle_candidate.strength.abs();
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
        let component_abs = candidate.strength.abs();
        if let Some(reason) = minor_cycle_stop_reason(
            component_abs,
            request.clean.threshold_jy_per_beam,
            cycle_threshold_jy_per_beam,
            nsigma_threshold_jy_per_beam,
        ) {
            stop_reason = Some(reason);
            break;
        }
        if cycle_component_updates > 0 && component_abs > initial_cycle_component * 1.5 {
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
    scale_masks: Option<Vec<Array2<bool>>>,
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
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    refresh_residual: &mut StandardMfsResidualRefreshCallback<'_>,
    model: &mut Array2<f32>,
    mut residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    let deconvolver_setup_started = Instant::now();
    let psf_patch = build_clark_psf_patch(
        &psf_state.psf,
        request.geometry.cell_size_rad,
        request.clean.psf_cutoff,
    );
    stage_timings.deconvolver_setup += deconvolver_setup_started.elapsed();
    let mut minor_iterations = 0usize;
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;
    let mut residual_needs_refresh = false;

    while reported_minor_iterations < request.clean.niter {
        if request
            .clean
            .major_cycle_limit
            .is_some_and(|limit| major_cycles >= limit)
        {
            clean_stop_reason = Some(CleanStopReason::MajorCycleLimitReached);
            break;
        }
        let cycle_setup_started = Instant::now();
        let Some((_, cycle_peak_value)) =
            peak_location_masked(&residual, request.clean_mask.as_ref())
        else {
            stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
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
            stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
            clean_stop_reason = Some(stop_reason);
            break;
        }
        let remaining_reported = request.clean.niter - reported_minor_iterations;
        let cycle_reported_niter = remaining_reported.min(request.clean.minor_cycle_length);
        let start_reported_iteration = reported_minor_iterations;
        final_cycle_threshold_jy_per_beam =
            compute_cycle_threshold(cycle_peak, max_psf_sidelobe_level, request.clean);
        stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
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
        residual_needs_refresh = true;
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
        residual = refresh_residual(model, stage_timings)?;
        major_cycles += 1;
        residual_needs_refresh = false;
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
    if residual_needs_refresh {
        residual = refresh_residual(model, stage_timings)?;
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
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
    refresh_residual: &mut StandardMfsResidualRefreshCallback<'_>,
    model: &mut Array2<f32>,
    mut residual: Array2<f32>,
    max_psf_sidelobe_level: f32,
    initial_peak: f32,
    warnings: &mut Vec<String>,
) -> Result<CottonSchwabState, ImagingError> {
    let deconvolver_setup_started = Instant::now();
    let scales = effective_multiscale_scales(request);
    let mut multiscale_state = build_multiscale_state(
        &residual,
        &psf_state.psf,
        &scales,
        request.small_scale_bias,
        request.clean_mask.as_ref(),
    );
    stage_timings.deconvolver_setup += deconvolver_setup_started.elapsed();
    let mut minor_iterations = 0usize;
    let mut reported_minor_iterations = 0usize;
    let mut major_cycles = 0usize;
    let mut clean_stop_reason = None::<CleanStopReason>;
    let mut minor_cycle_traces = Vec::<MinorCycleTrace>::new();
    let mut final_cycle_threshold_jy_per_beam = request.clean.threshold_jy_per_beam;
    let mut min_residual_peak_jy_per_beam = initial_peak;
    let mut divergence_warned = false;
    let mut residual_needs_refresh = false;

    while reported_minor_iterations < request.clean.niter {
        if request
            .clean
            .major_cycle_limit
            .is_some_and(|limit| major_cycles >= limit)
        {
            clean_stop_reason = Some(CleanStopReason::MajorCycleLimitReached);
            break;
        }
        let cycle_setup_started = Instant::now();
        let Some(cycle_candidate) =
            select_multiscale_candidate(&multiscale_state, request.clean_mask.as_ref())
        else {
            stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
            clean_stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let cycle_peak = peak_abs_value_masked(
            &multiscale_state.dirty_conv_scales[0],
            request.clean_mask.as_ref(),
        );
        let cycle_nsigma_threshold_jy_per_beam =
            nsigma_threshold_jy_per_beam(&residual, request.clean_mask.as_ref(), request.clean);
        if let Some(stop_reason) = tolerant_clean_stop_reason(
            cycle_peak,
            request.clean.threshold_jy_per_beam,
            cycle_nsigma_threshold_jy_per_beam,
        ) {
            stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
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
        let initial_cycle_component = cycle_candidate.strength.abs();
        let mut cycle_component_updates = 0usize;
        let mut updated_model = false;
        let mut cycle_stop_reason = None::<CleanStopReason>;
        final_cycle_threshold_jy_per_beam =
            compute_cycle_threshold(cycle_peak, max_psf_sidelobe_level, request.clean);
        stage_timings.clean_cycle_setup += cycle_setup_started.elapsed();
        let minor_started = Instant::now();
        while cycle_component_updates < cycle_reported_niter {
            let Some(candidate) =
                select_multiscale_candidate(&multiscale_state, request.clean_mask.as_ref())
            else {
                cycle_stop_reason = Some(CleanStopReason::NoCleanablePixels);
                break;
            };
            let component_abs = candidate.strength.abs();
            if let Some(stop_reason) = minor_cycle_stop_reason(
                component_abs,
                request.clean.threshold_jy_per_beam,
                final_cycle_threshold_jy_per_beam,
                cycle_nsigma_threshold_jy_per_beam,
            ) {
                cycle_stop_reason = Some(stop_reason);
                break;
            }
            if cycle_component_updates > 0 && component_abs > initial_cycle_component * 1.5 {
                cycle_stop_reason = Some(CleanStopReason::DivergenceDetected);
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
            cycle_stop_reason,
            updated_model,
        );
        minor_cycle_traces.push(make_minor_cycle_trace(
            minor_cycle_traces.len(),
            start_reported_iteration,
            HogbomMinorCycleOutcome {
                updated_model,
                actual_updates: cycle_component_updates,
                reported_updates,
                stop_reason: cycle_stop_reason,
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
            clean_stop_reason = cycle_stop_reason;
            break;
        }
        residual = multiscale_state.dirty_conv_scales[0].clone();
        residual_needs_refresh = true;
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
        residual = refresh_residual(model, stage_timings)?;
        let multiscale_refresh_started = Instant::now();
        multiscale_state = build_multiscale_state(
            &residual,
            &psf_state.psf,
            &scales,
            request.small_scale_bias,
            request.clean_mask.as_ref(),
        );
        stage_timings.multiscale_scale_refresh += multiscale_refresh_started.elapsed();
        major_cycles += 1;
        residual_needs_refresh = false;
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
    if residual_needs_refresh {
        residual = refresh_residual(model, stage_timings)?;
        let multiscale_refresh_started = Instant::now();
        let _refreshed_multiscale_state = build_multiscale_state(
            &residual,
            &psf_state.psf,
            &scales,
            request.small_scale_bias,
            request.clean_mask.as_ref(),
        );
        stage_timings.multiscale_scale_refresh += multiscale_refresh_started.elapsed();
        major_cycles += 1;
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
    mask: Option<&Array2<bool>>,
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
    let scale_masks =
        mask.map(|clean_mask| build_multiscale_scale_masks(clean_mask, &scale_images, scales));

    MultiscaleState {
        scales: scale_images,
        dirty_conv_scales,
        psf_conv_scales,
        peak_psf_conv_scales,
        scale_bias,
        scale_masks,
    }
}

fn build_multiscale_scale_masks(
    mask: &Array2<bool>,
    scale_images: &[Array2<f32>],
    scale_sizes: &[f32],
) -> Vec<Array2<bool>> {
    let mask_values = mask.mapv(|cleanable| if cleanable { 1.0 } else { 0.0 });
    scale_images
        .iter()
        .zip(scale_sizes.iter().copied())
        .map(|(scale_image, scale_size)| {
            let convolved = fft_convolve_real(&mask_values, scale_image);
            let mut scale_mask = convolved.mapv(|value| value > 0.9);
            apply_casa_multiscale_edge_mask(&mut scale_mask, scale_size);
            scale_mask
        })
        .collect()
}

fn apply_casa_multiscale_edge_mask(mask: &mut Array2<bool>, scale_size: f32) {
    let (nx, ny) = mask.dim();
    let border = (scale_size * 1.5) as usize;
    for x in 0..nx {
        for y in 0..ny {
            if x <= border
                || y <= border
                || x >= nx.saturating_sub(border + 1)
                || y >= ny.saturating_sub(border + 1)
            {
                mask[(x, y)] = false;
            }
        }
    }
}

fn select_multiscale_candidate(
    state: &MultiscaleState,
    mask: Option<&Array2<bool>>,
) -> Option<MultiscaleCandidate> {
    let mut best = None::<(MultiscaleCandidate, f32)>;
    for scale_index in 0..state.dirty_conv_scales.len() {
        let search_mask = state
            .scale_masks
            .as_ref()
            .map(|scale_masks| &scale_masks[scale_index])
            .or(mask);
        let Some((position, value)) =
            peak_location_masked(&state.dirty_conv_scales[scale_index], search_mask)
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

fn hogbom_component_budget(reported_cycle_niter: usize, clean: CleanConfig) -> usize {
    match clean.hogbom_iteration_mode {
        // CASA's `SDAlgorithmHogbomClean` passes `siter = 0` and
        // `cycleNiter` into casacore's Fortran `hclean`, whose
        // `do iter=siter,niter` loop is inclusive. The kernel can therefore
        // commit one more component than the reported `iterdone`, which is
        // clamped back to `cycleNiter` before returning.
        HogbomIterationMode::CasaInclusive => reported_cycle_niter.saturating_add(1),
        HogbomIterationMode::Strict => reported_cycle_niter,
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

struct MtmfsPsfState {
    psf_terms: Vec<Array2<f32>>,
    normalization_sumwt: f32,
    reported_sumwt_terms: Vec<f32>,
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

fn mtmfs_taylor_weight(frequency_hz: f64, reffreq_hz: f64, order: usize) -> f32 {
    if order == 0 {
        return 1.0;
    }
    let scaled = (frequency_hz - reffreq_hz) / reffreq_hz;
    scaled.powi(order as i32) as f32
}

#[allow(clippy::needless_range_loop)]
fn compute_mtmfs_psf_terms(
    request: &MtmfsRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<MtmfsPsfState, ImagingError> {
    if request.w_term_mode == WTermMode::WProject {
        return compute_mtmfs_psf_terms_w_project(request, batches, gridder, stage_timings);
    }
    let term_count = 2 * request.nterms - 1;
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grids = (0..term_count)
        .map(|_| Array2::<Complex32>::zeros((nx, ny)))
        .collect::<Vec<_>>();
    let mut normalization_sumwt = 0.0f64;
    let mut reported_sumwt_terms = vec![0.0f64; term_count];
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;
    let mut timings = PsfComputationTimings::default();

    let grid_started = Instant::now();
    for (batch_index, batch) in batches.iter().enumerate() {
        let frequencies_hz = request
            .sample_frequency_batches_hz
            .get(batch_index)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "missing MTMFS sample-frequency batch for visibility batch {batch_index}"
                ))
            })?;
        for (index, &frequency_hz) in frequencies_hz.iter().enumerate().take(batch.len()) {
            if !batch.gridable[index] {
                skipped_samples += 1;
                continue;
            }
            let weight = batch.weight[index];
            let sumwt_factor = batch.sumwt_factor[index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0
                && frequency_hz.is_finite()
                && frequency_hz > 0.0)
            {
                skipped_samples += 1;
                continue;
            }
            let Some(plan) = gridder.plan_sample(batch.u_lambda[index], batch.v_lambda[index])
            else {
                skipped_samples += 1;
                continue;
            };
            normalization_sumwt += 2.0 * f64::from(weight);
            for order in 0..term_count {
                let factor = mtmfs_taylor_weight(frequency_hz, request.reffreq_hz, order);
                let psf_weight = Complex32::new(weight * factor, 0.0);
                gridder.grid_sample_product_planned(
                    &mut psf_grids[order],
                    &plan.positive,
                    psf_weight,
                );
                gridder.grid_sample_product_planned(
                    &mut psf_grids[order],
                    &plan.negative,
                    psf_weight,
                );
                reported_sumwt_terms[order] +=
                    f64::from(weight) * f64::from(factor) * f64::from(sumwt_factor);
            }
            gridded_samples += 1;
        }
    }
    timings.grid = grid_started.elapsed();

    if normalization_sumwt <= 0.0
        || !reported_sumwt_terms[0].is_finite()
        || reported_sumwt_terms[0] <= 0.0
    {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_terms = psf_grids.iter().map(centered_ifft2).collect::<Vec<_>>();
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf_terms = raw_terms
        .iter()
        .map(|raw| {
            let mut corrected = gridder.corrected_image_from_grid(raw);
            corrected.mapv_inplace(|value| value / normalization_sumwt as f32);
            corrected
        })
        .collect::<Vec<_>>();
    let psf_peak = peak_abs_value(&psf_terms[0]);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "MTMFS PSF peak is non-finite or zero".to_string(),
        ));
    }
    for psf_term in &mut psf_terms {
        psf_term.mapv_inplace(|value| value / psf_peak);
    }
    timings.normalize = normalize_started.elapsed();
    stage_timings.psf_grid += timings.grid;
    stage_timings.psf_fft += timings.fft;
    stage_timings.psf_normalize += timings.normalize;

    Ok(MtmfsPsfState {
        psf_terms,
        normalization_sumwt: normalization_sumwt as f32,
        reported_sumwt_terms: reported_sumwt_terms
            .into_iter()
            .map(|value| value as f32)
            .collect(),
        psf_peak,
        gridded_samples,
        skipped_samples,
    })
}

#[allow(clippy::needless_range_loop)]
fn compute_mtmfs_psf_terms_w_project(
    request: &MtmfsRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<MtmfsPsfState, ImagingError> {
    let prepare_started = Instant::now();
    let prepared =
        prepare_w_project_data(request.geometry, batches, gridder, request.w_project_planes)?;
    let mut timings = PsfComputationTimings {
        grid: prepare_started.elapsed(),
        ..PsfComputationTimings::default()
    };
    let term_count = 2 * request.nterms - 1;
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grids = (0..term_count)
        .map(|_| Array2::<Complex32>::zeros((nx, ny)))
        .collect::<Vec<_>>();
    let mut reported_sumwt_terms = vec![0.0f64; term_count];

    let grid_started = Instant::now();
    for sample in &prepared.samples {
        let frequency_hz =
            mtmfs_sample_frequency(request, sample.batch_index, sample.sample_index)?;
        for order in 0..term_count {
            let factor = mtmfs_taylor_weight(frequency_hz, request.reffreq_hz, order);
            let psf_weight = Complex32::new(sample.weight * factor, 0.0);
            prepared.projector.grid_sample_planned(
                &mut psf_grids[order],
                &sample.positive_plan,
                psf_weight,
            );
            reported_sumwt_terms[order] +=
                f64::from(sample.weight) * f64::from(factor) * f64::from(sample.sumwt_factor);
        }
    }
    timings.grid += grid_started.elapsed();

    if prepared.normalization_sumwt <= 0.0
        || !reported_sumwt_terms[0].is_finite()
        || reported_sumwt_terms[0] <= 0.0
    {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_terms = psf_grids.iter().map(centered_ifft2).collect::<Vec<_>>();
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf_terms = raw_terms
        .iter()
        .map(|raw| {
            let mut corrected =
                gridder.corrected_w_project_image_from_grid(raw, prepared.projector.sampling());
            corrected.mapv_inplace(|value| 2.0 * value / prepared.normalization_sumwt);
            corrected
        })
        .collect::<Vec<_>>();
    let psf_peak = peak_abs_value(&psf_terms[0]);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "MTMFS W-projection PSF peak is non-finite or zero".to_string(),
        ));
    }
    for psf_term in &mut psf_terms {
        psf_term.mapv_inplace(|value| value / psf_peak);
    }
    timings.normalize = normalize_started.elapsed();
    stage_timings.psf_grid += timings.grid;
    stage_timings.psf_fft += timings.fft;
    stage_timings.psf_normalize += timings.normalize;

    Ok(MtmfsPsfState {
        psf_terms,
        normalization_sumwt: prepared.normalization_sumwt,
        reported_sumwt_terms: reported_sumwt_terms
            .into_iter()
            .map(|value| value as f32)
            .collect(),
        psf_peak,
        gridded_samples: prepared.gridded_samples,
        skipped_samples: prepared.skipped_samples.len(),
    })
}

#[allow(clippy::needless_range_loop)]
fn compute_mtmfs_residual_terms(
    request: &MtmfsRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model_terms: &[Array2<f32>],
    psf_state: &MtmfsPsfState,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Vec<Array2<f32>>, ImagingError> {
    if request.w_term_mode == WTermMode::WProject {
        return compute_mtmfs_residual_terms_w_project(
            request,
            batches,
            gridder,
            model_terms,
            psf_state,
            stage_timings,
        );
    }
    let [nx, ny] = gridder.grid_shape();
    let mut residual_grids = (0..request.nterms)
        .map(|_| Array2::<Complex32>::zeros((nx, ny)))
        .collect::<Vec<_>>();
    let mut timings = ResidualComputationTimings::default();
    let model_grids = if model_terms
        .iter()
        .any(|term| term.iter().any(|value| value.abs() > 0.0))
    {
        let model_fft_started = Instant::now();
        let grids = model_terms
            .iter()
            .map(|model_term| centered_fft2(&gridder.apodize_model(model_term)))
            .collect::<Vec<_>>();
        timings.model_fft = model_fft_started.elapsed();
        Some(grids)
    } else {
        None
    };

    let degrid_grid_started = Instant::now();
    for (batch_index, batch) in batches.iter().enumerate() {
        let frequencies_hz = request
            .sample_frequency_batches_hz
            .get(batch_index)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "missing MTMFS sample-frequency batch for visibility batch {batch_index}"
                ))
            })?;
        for (index, &frequency_hz) in frequencies_hz.iter().enumerate().take(batch.len()) {
            let weight = batch.weight[index];
            let observed_visibility = batch.visibility[index];
            let gridable = batch.gridable[index];
            let planned_sample = if gridable
                && weight.is_finite()
                && weight > 0.0
                && observed_visibility.re.is_finite()
                && observed_visibility.im.is_finite()
                && frequency_hz.is_finite()
                && frequency_hz > 0.0
            {
                gridder.plan_sample(batch.u_lambda[index], batch.v_lambda[index])
            } else {
                None
            };
            let Some(plan) = planned_sample.as_ref() else {
                continue;
            };
            let predicted_visibility_terms = if let Some(model_grids) = model_grids.as_ref() {
                model_grids
                    .iter()
                    .map(|grid| {
                        gridder.degrid_sample_product_planned_normalized(grid, &plan.positive)
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![Complex32::new(0.0, 0.0); request.nterms]
            };
            for (residual_order, residual_grid) in
                residual_grids.iter_mut().enumerate().take(request.nterms)
            {
                let observed_term = observed_visibility
                    * mtmfs_taylor_weight(frequency_hz, request.reffreq_hz, residual_order);
                let mut predicted_term = Complex32::new(0.0, 0.0);
                for (model_order, predicted_visibility) in predicted_visibility_terms
                    .iter()
                    .enumerate()
                    .take(request.nterms)
                {
                    let factor = mtmfs_taylor_weight(
                        frequency_hz,
                        request.reffreq_hz,
                        residual_order + model_order,
                    );
                    predicted_term += *predicted_visibility * factor;
                }
                let residual_visibility = observed_term - predicted_term;
                let residual = residual_visibility * weight;
                gridder.grid_sample_product_planned(residual_grid, &plan.positive, residual);
                gridder.grid_sample_product_planned(residual_grid, &plan.negative, residual.conj());
            }
        }
    }
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw_terms = residual_grids
        .iter()
        .map(centered_ifft2)
        .collect::<Vec<_>>();
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let residual_terms = raw_terms
        .iter()
        .map(|raw| {
            let mut image = gridder.corrected_image_from_grid(raw);
            image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
            image
        })
        .collect::<Vec<_>>();
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    Ok(residual_terms)
}

#[allow(clippy::needless_range_loop)]
fn compute_mtmfs_residual_terms_w_project(
    request: &MtmfsRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model_terms: &[Array2<f32>],
    psf_state: &MtmfsPsfState,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Vec<Array2<f32>>, ImagingError> {
    let prepare_started = Instant::now();
    let prepared =
        prepare_w_project_data(request.geometry, batches, gridder, request.w_project_planes)?;
    let mut timings = ResidualComputationTimings {
        degrid_grid: prepare_started.elapsed(),
        ..ResidualComputationTimings::default()
    };
    let [nx, ny] = gridder.grid_shape();
    let mut residual_grids = (0..request.nterms)
        .map(|_| Array2::<Complex32>::zeros((nx, ny)))
        .collect::<Vec<_>>();
    let model_grids = if model_terms
        .iter()
        .any(|term| term.iter().any(|value| value.abs() > 0.0))
    {
        let model_fft_started = Instant::now();
        let grids = model_terms
            .iter()
            .map(|model_term| {
                centered_fft2(
                    &gridder.apodize_w_project_model(model_term, prepared.projector.sampling()),
                )
            })
            .collect::<Vec<_>>();
        timings.model_fft = model_fft_started.elapsed();
        Some(grids)
    } else {
        None
    };

    let degrid_started = Instant::now();
    for sample in &prepared.samples {
        let frequency_hz =
            mtmfs_sample_frequency(request, sample.batch_index, sample.sample_index)?;
        let predicted_visibility_terms = if let Some(model_grids) = model_grids.as_ref() {
            model_grids
                .iter()
                .map(|grid| {
                    prepared
                        .projector
                        .degrid_sample_planned(grid, &sample.positive_plan)
                })
                .collect::<Vec<_>>()
        } else {
            vec![Complex32::new(0.0, 0.0); request.nterms]
        };
        for (residual_order, residual_grid) in
            residual_grids.iter_mut().enumerate().take(request.nterms)
        {
            let observed_term = sample.visibility
                * mtmfs_taylor_weight(frequency_hz, request.reffreq_hz, residual_order);
            let mut predicted_term = Complex32::new(0.0, 0.0);
            for (model_order, predicted_visibility) in predicted_visibility_terms
                .iter()
                .enumerate()
                .take(request.nterms)
            {
                let factor = mtmfs_taylor_weight(
                    frequency_hz,
                    request.reffreq_hz,
                    residual_order + model_order,
                );
                predicted_term += *predicted_visibility * factor;
            }
            let residual = (observed_term - predicted_term) * sample.weight;
            prepared
                .projector
                .grid_sample_planned(residual_grid, &sample.positive_plan, residual);
        }
    }
    timings.degrid_grid += degrid_started.elapsed();

    let fft_started = Instant::now();
    let raw_terms = residual_grids
        .iter()
        .map(centered_ifft2)
        .collect::<Vec<_>>();
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let residual_terms = raw_terms
        .iter()
        .map(|raw| {
            let mut image =
                gridder.corrected_w_project_image_from_grid(raw, prepared.projector.sampling());
            image.mapv_inplace(|value| {
                2.0 * value / psf_state.normalization_sumwt / psf_state.psf_peak
            });
            image
        })
        .collect::<Vec<_>>();
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    Ok(residual_terms)
}

fn mtmfs_sample_frequency(
    request: &MtmfsRequest,
    batch_index: usize,
    sample_index: usize,
) -> Result<f64, ImagingError> {
    let frequency_hz = request
        .sample_frequency_batches_hz
        .get(batch_index)
        .and_then(|batch| batch.get(sample_index))
        .copied()
        .ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "missing MTMFS sample frequency for batch {batch_index} sample {sample_index}"
            ))
        })?;
    if !(frequency_hz.is_finite() && frequency_hz > 0.0) {
        return Err(ImagingError::InvalidRequest(format!(
            "MTMFS sample frequency for batch {batch_index} sample {sample_index} must be finite and > 0 Hz"
        )));
    }
    Ok(frequency_hz)
}

fn mtmfs_hessian(psf_terms: &[Array2<f32>], nterms: usize) -> Result<Vec<Vec<f32>>, ImagingError> {
    if psf_terms.len() < 2 * nterms - 1 {
        return Err(ImagingError::InvalidRequest(format!(
            "MTMFS PSF stack length {} is smaller than required {}",
            psf_terms.len(),
            2 * nterms - 1
        )));
    }
    let center = (psf_terms[0].dim().0 / 2, psf_terms[0].dim().1 / 2);
    Ok((0..nterms)
        .map(|row| {
            (0..nterms)
                .map(|col| psf_terms[row + col][center])
                .collect::<Vec<_>>()
        })
        .collect())
}

#[allow(clippy::needless_range_loop)]
fn invert_small_matrix(matrix: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, ImagingError> {
    let n = matrix.len();
    if n == 0 || matrix.iter().any(|row| row.len() != n) {
        return Err(ImagingError::InvalidRequest(
            "MTMFS Hessian must be a non-empty square matrix".to_string(),
        ));
    }
    let mut augmented = vec![vec![0.0f64; 2 * n]; n];
    for row in 0..n {
        for col in 0..n {
            augmented[row][col] = matrix[row][col] as f64;
        }
        augmented[row][n + row] = 1.0;
    }
    for pivot in 0..n {
        let mut best_row = pivot;
        let mut best_value = augmented[pivot][pivot].abs();
        for row in (pivot + 1)..n {
            let value = augmented[row][pivot].abs();
            if value > best_value {
                best_value = value;
                best_row = row;
            }
        }
        if !(best_value.is_finite() && best_value > 0.0) {
            return Err(ImagingError::Unsupported(
                "MTMFS Hessian is singular at the image center".to_string(),
            ));
        }
        if best_row != pivot {
            augmented.swap(best_row, pivot);
        }
        let pivot_value = augmented[pivot][pivot];
        for col in 0..(2 * n) {
            augmented[pivot][col] /= pivot_value;
        }
        for row in 0..n {
            if row == pivot {
                continue;
            }
            let factor = augmented[row][pivot];
            if factor == 0.0 {
                continue;
            }
            for col in 0..(2 * n) {
                augmented[row][col] -= factor * augmented[pivot][col];
            }
        }
    }
    Ok((0..n)
        .map(|row| {
            (0..n)
                .map(|col| augmented[row][n + col] as f32)
                .collect::<Vec<_>>()
        })
        .collect())
}

fn solve_mtmfs_coefficients(rhs: &[f32], inv_hessian: &[Vec<f32>]) -> Vec<f32> {
    inv_hessian
        .iter()
        .map(|row| {
            row.iter()
                .zip(rhs.iter())
                .map(|(left, right)| *left * *right)
                .sum()
        })
        .collect()
}

fn principal_solution_terms(
    residual_terms: &[Array2<f32>],
    inv_hessian: &[Vec<f32>],
) -> Vec<Array2<f32>> {
    let nterms = residual_terms.len();
    let shape = residual_terms[0].raw_dim();
    let mut principal_terms = (0..nterms)
        .map(|_| Array2::<f32>::zeros(shape))
        .collect::<Vec<_>>();
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let rhs = residual_terms
                .iter()
                .map(|term| term[(x, y)])
                .collect::<Vec<_>>();
            let coeffs = solve_mtmfs_coefficients(&rhs, inv_hessian);
            for (term, coeff) in coeffs.into_iter().enumerate() {
                principal_terms[term][(x, y)] = coeff;
            }
        }
    }
    principal_terms
}

fn find_mtmfs_component(
    residual_terms: &[Array2<f32>],
    hessian: &[Vec<f32>],
    inv_hessian: &[Vec<f32>],
    clean_mask: Option<&Array2<bool>>,
) -> Option<((usize, usize), Vec<f32>, f32)> {
    let (nx, ny) = residual_terms.first()?.dim();
    let mut best = None::<((usize, usize), Vec<f32>, f32)>;
    let mut best_penalty = -1.0f32;
    for x in 0..nx {
        for y in 0..ny {
            if clean_mask.is_some_and(|mask| !mask[(x, y)]) {
                continue;
            }
            let rhs = residual_terms
                .iter()
                .map(|term| term[(x, y)])
                .collect::<Vec<_>>();
            let coeffs = solve_mtmfs_coefficients(&rhs, inv_hessian);
            let mut penalty = 0.0f32;
            for row in 0..coeffs.len() {
                penalty += 2.0 * coeffs[row] * rhs[row];
                for col in 0..coeffs.len() {
                    penalty -= coeffs[row] * coeffs[col] * hessian[row][col];
                }
            }
            let penalty_abs = penalty.abs();
            if penalty_abs > best_penalty {
                best_penalty = penalty_abs;
                best = Some(((x, y), coeffs, penalty_abs));
            }
        }
    }
    best
}

#[allow(clippy::too_many_arguments)]
fn run_mtmfs_minor_cycle(
    request: &MtmfsRequest,
    psf_terms: &[Array2<f32>],
    hessian: &[Vec<f32>],
    inv_hessian: &[Vec<f32>],
    model_terms: &mut [Array2<f32>],
    residual_terms: &mut [Array2<f32>],
    cycle_reported_niter: usize,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
    stage_timings: &mut ImagingStageTimings,
) -> (HogbomMinorCycleOutcome, MinorCycleProbe) {
    if !request.multiscale_scales.is_empty() {
        return run_mtmfs_multiscale_minor_cycle(
            request,
            psf_terms,
            hessian,
            inv_hessian,
            model_terms,
            residual_terms,
            cycle_reported_niter,
            cycle_threshold_jy_per_beam,
            nsigma_threshold_jy_per_beam,
            stage_timings,
        );
    }
    let cycle_component_budget = hogbom_component_budget(cycle_reported_niter, request.clean);
    let mut cycle_component_updates = 0usize;
    let mut updated_model = false;
    let mut stop_reason = None;
    let mut probe = MinorCycleProbe::default();
    let minor_started = Instant::now();
    while cycle_component_updates < cycle_component_budget {
        let peak_abs = peak_abs_value_masked(&residual_terms[0], request.clean_mask.as_ref());
        if let Some(reason) = minor_cycle_stop_reason(
            peak_abs,
            request.clean.threshold_jy_per_beam,
            cycle_threshold_jy_per_beam,
            nsigma_threshold_jy_per_beam,
        ) {
            stop_reason = Some(reason);
            break;
        }
        let Some(((peak_x, peak_y), coeffs, candidate_strength)) = find_mtmfs_component(
            residual_terms,
            hessian,
            inv_hessian,
            request.clean_mask.as_ref(),
        ) else {
            stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        if cycle_component_updates == 0 {
            probe = MinorCycleProbe {
                initial_scale_pixels: Some(0.0),
                initial_candidate_strength_jy_per_beam: Some(candidate_strength),
                initial_candidate_position: Some([peak_x, peak_y]),
            };
        }
        for (term_index, coefficient) in coeffs.iter().enumerate() {
            let component = request.clean.gain * *coefficient;
            model_terms[term_index][(peak_x, peak_y)] += component;
        }
        for residual_order in 0..request.nterms {
            for model_order in 0..request.nterms {
                let component = request.clean.gain * coeffs[model_order];
                subtract_shifted_kernel(
                    &mut residual_terms[residual_order],
                    &psf_terms[residual_order + model_order],
                    (peak_x, peak_y),
                    component,
                );
            }
        }
        cycle_component_updates += 1;
        updated_model = true;
    }
    let minor_elapsed = minor_started.elapsed();
    stage_timings.minor_cycle += minor_elapsed;
    stage_timings.minor_cycle_solve += minor_elapsed;
    (
        HogbomMinorCycleOutcome {
            updated_model,
            actual_updates: cycle_component_updates,
            reported_updates: cycle_component_updates.min(cycle_reported_niter),
            stop_reason,
            final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
            final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
        },
        probe,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_mtmfs_multiscale_minor_cycle(
    request: &MtmfsRequest,
    psf_terms: &[Array2<f32>],
    _hessian: &[Vec<f32>],
    inv_hessian: &[Vec<f32>],
    model_terms: &mut [Array2<f32>],
    residual_terms: &mut [Array2<f32>],
    cycle_reported_niter: usize,
    cycle_threshold_jy_per_beam: f32,
    nsigma_threshold_jy_per_beam: f32,
    stage_timings: &mut ImagingStageTimings,
) -> (HogbomMinorCycleOutcome, MinorCycleProbe) {
    let scales = effective_mtmfs_multiscale_scales(request);
    let mut principal_terms = principal_solution_terms(residual_terms, inv_hessian);
    let mut multiscale_state = build_multiscale_state(
        &principal_terms[0],
        &psf_terms[0],
        &scales,
        request.small_scale_bias,
        request.clean_mask.as_ref(),
    );
    let Some(first_candidate) =
        select_multiscale_candidate(&multiscale_state, request.clean_mask.as_ref())
    else {
        return (
            HogbomMinorCycleOutcome {
                updated_model: false,
                actual_updates: 0,
                reported_updates: 0,
                stop_reason: Some(CleanStopReason::NoCleanablePixels),
                final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
                final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
            },
            MinorCycleProbe::default(),
        );
    };
    let probe = MinorCycleProbe {
        initial_scale_pixels: Some(scales[first_candidate.scale_index]),
        initial_candidate_strength_jy_per_beam: Some(first_candidate.strength.abs()),
        initial_candidate_position: Some([first_candidate.position.0, first_candidate.position.1]),
    };
    let initial_cycle_peak = peak_abs_value_masked(
        &multiscale_state.dirty_conv_scales[0],
        request.clean_mask.as_ref(),
    );
    if let Some(reason) = minor_cycle_stop_reason(
        initial_cycle_peak,
        request.clean.threshold_jy_per_beam,
        cycle_threshold_jy_per_beam,
        nsigma_threshold_jy_per_beam,
    ) {
        return (
            HogbomMinorCycleOutcome {
                updated_model: false,
                actual_updates: 0,
                reported_updates: 0,
                stop_reason: Some(reason),
                final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
                final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
            },
            probe,
        );
    }

    let cycle_component_budget = hogbom_component_budget(cycle_reported_niter, request.clean);
    let psf_scale_terms = build_mtmfs_psf_scale_terms(psf_terms, &multiscale_state.scales);
    let mut cycle_component_updates = 0usize;
    let mut updated_model = false;
    let mut stop_reason = None;
    let minor_started = Instant::now();
    while cycle_component_updates < cycle_component_budget {
        let Some(candidate) =
            select_multiscale_candidate(&multiscale_state, request.clean_mask.as_ref())
        else {
            stop_reason = Some(CleanStopReason::NoCleanablePixels);
            break;
        };
        let peak_abs = peak_abs_value_masked(
            &multiscale_state.dirty_conv_scales[0],
            request.clean_mask.as_ref(),
        );
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

        let coeffs = mtmfs_multiscale_coefficients(
            residual_terms,
            &multiscale_state.scales[candidate.scale_index],
            candidate.position,
            inv_hessian,
        );
        for (term_index, coefficient) in coeffs.iter().enumerate().take(request.nterms) {
            let component = request.clean.gain * *coefficient;
            add_shifted_kernel(
                &mut model_terms[term_index],
                &multiscale_state.scales[candidate.scale_index],
                candidate.position,
                component,
            );
        }
        for residual_order in 0..request.nterms {
            for model_order in 0..request.nterms {
                let component = request.clean.gain * coeffs[model_order];
                subtract_shifted_kernel(
                    &mut residual_terms[residual_order],
                    &psf_scale_terms[residual_order + model_order][candidate.scale_index],
                    candidate.position,
                    component,
                );
            }
        }
        cycle_component_updates += 1;
        updated_model = true;
        principal_terms = principal_solution_terms(residual_terms, inv_hessian);
        refresh_multiscale_dirty_conv_scales(&mut multiscale_state, &principal_terms[0]);
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
    (
        HogbomMinorCycleOutcome {
            updated_model,
            actual_updates: cycle_component_updates,
            reported_updates,
            stop_reason,
            final_cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam,
            final_nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam,
        },
        probe,
    )
}

fn effective_mtmfs_multiscale_scales(request: &MtmfsRequest) -> Vec<f32> {
    if request.multiscale_scales.is_empty() {
        vec![0.0]
    } else {
        request.multiscale_scales.clone()
    }
}

fn build_mtmfs_psf_scale_terms(
    psf_terms: &[Array2<f32>],
    scale_kernels: &[Array2<f32>],
) -> Vec<Vec<Array2<f32>>> {
    psf_terms
        .iter()
        .map(|psf| {
            scale_kernels
                .iter()
                .map(|scale| fft_convolve_real(psf, scale))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn mtmfs_multiscale_coefficients(
    residual_terms: &[Array2<f32>],
    scale_kernel: &Array2<f32>,
    position: (usize, usize),
    inv_hessian: &[Vec<f32>],
) -> Vec<f32> {
    let rhs = residual_terms
        .iter()
        .map(|term| fft_convolve_real(term, scale_kernel)[position])
        .collect::<Vec<_>>();
    solve_mtmfs_coefficients(&rhs, inv_hessian)
}

fn compute_mtmfs_alpha_products(
    image_terms: &[Array2<f32>],
    residual_terms: &[Array2<f32>],
) -> (Option<Array2<f32>>, Option<Array2<f32>>) {
    if image_terms.len() < 2 || residual_terms.len() < 2 {
        return (None, None);
    }
    let tt0 = &image_terms[0];
    let tt1 = &image_terms[1];
    let residual0 = &residual_terms[0];
    let residual1 = &residual_terms[1];
    let specthreshold = peak_abs_value(residual0) / 10.0;
    let (nx, ny) = tt0.dim();
    let mut alpha = Array2::<f32>::zeros((nx, ny));
    let mut alpha_error = Array2::<f32>::zeros((nx, ny));
    for x in 0..nx {
        for y in 0..ny {
            let image0 = tt0[(x, y)];
            if image0 <= specthreshold {
                continue;
            }
            let image1 = tt1[(x, y)];
            if image0 == 0.0 || image1 == 0.0 {
                continue;
            }
            let alpha_value = image1 / image0;
            alpha[(x, y)] = alpha_value;
            let term0 = residual0[(x, y)] / image0;
            let term1 = residual1[(x, y)] / image1;
            alpha_error[(x, y)] = alpha_value.abs() * (term0 * term0 + term1 * term1).sqrt();
        }
    }
    (Some(alpha), Some(alpha_error))
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
    match request.w_term_mode {
        WTermMode::Direct => {
            return compute_psf_direct(request.geometry, batches, stage_timings);
        }
        WTermMode::WProject => {
            return compute_psf_w_project(
                request.geometry,
                batches,
                gridder,
                request.w_project_planes,
                stage_timings,
            );
        }
        WTermMode::None => {}
    }
    if standard_mfs_fixed_tile_backend_enabled() {
        return compute_psf_standard_tiled(
            batches,
            gridder,
            StandardMfsExecutionConfig::default(),
            stage_timings,
        );
    }
    if standard_mfs_metal_backend_enabled() {
        return compute_psf_standard_metal(
            batches,
            gridder,
            StandardMfsExecutionConfig::default(),
            stage_timings,
        );
    }
    if standard_mfs_sample_count(batches) > standard_mfs_executor_max_samples() {
        return compute_psf_standard_streaming(batches, gridder, stage_timings);
    }
    let mut executor = StandardMfsCpuExecutor::new(gridder, batches)?;
    compute_psf_standard(&mut executor, stage_timings)
}

fn compute_psf_standard(
    executor: &mut StandardMfsCpuExecutor<'_>,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    let mut timings = PsfComputationTimings::default();
    let (gridder, plan, workspace) = executor.parts_mut();
    let psf_grid = workspace.clear_psf_grid();

    let grid_started = Instant::now();
    for sample in plan.samples() {
        gridder.grid_sample_taps_real_planned_f64(
            psf_grid,
            &sample.positive_taps,
            sample.grid_weight_f64(),
        );
    }
    timings.grid = grid_started.elapsed();

    let normalization_sumwt = plan.normalization_sumwt();
    let reported_sumwt = plan.reported_sumwt();
    if normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(psf_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / normalization_sumwt as f32);
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
        normalization_sumwt: normalization_sumwt as f32,
        reported_sumwt: reported_sumwt as f32,
        psf_peak,
        gridded_samples: plan.gridded_samples(),
        skipped_samples: plan.skipped_samples(),
    })
}

fn compute_psf_standard_streaming(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    let mut timings = PsfComputationTimings::default();
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((nx, ny));
    let mut normalization_sumwt = 0.0f64;
    let mut reported_sumwt = 0.0f64;
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;

    let grid_started = Instant::now();
    for batch in batches {
        for sample_index in 0..batch.len() {
            if !batch.gridable[sample_index] {
                skipped_samples += 1;
                continue;
            }
            let weight = batch.weight[sample_index];
            let sumwt_factor = batch.sumwt_factor[sample_index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0)
            {
                skipped_samples += 1;
                continue;
            }
            let Some(plan) = gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
            else {
                skipped_samples += 1;
                continue;
            };
            let grid_weight = f64::from(weight * sumwt_factor);
            normalization_sumwt += grid_weight;
            reported_sumwt += grid_weight;
            gridded_samples += 1;
            gridder.grid_sample_taps_real_planned_f64(&mut psf_grid, &plan, grid_weight);
        }
    }
    timings.grid = grid_started.elapsed();

    if normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(&psf_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / normalization_sumwt as f32);
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
        normalization_sumwt: normalization_sumwt as f32,
        reported_sumwt: reported_sumwt as f32,
        psf_peak,
        gridded_samples,
        skipped_samples,
    })
}

fn compute_psf_standard_tiled(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    let mut timings = PsfComputationTimings::default();
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((nx, ny));
    let executor =
        StandardMfsTiledCpuExecutor::new_with_execution_config(gridder, execution_config)?;

    let grid_started = Instant::now();
    let accumulation = executor.accumulate_psf_grid(batches, &mut psf_grid)?;
    timings.grid = grid_started.elapsed();

    if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(&psf_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
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
        normalization_sumwt: accumulation.normalization_sumwt as f32,
        reported_sumwt: accumulation.reported_sumwt as f32,
        psf_peak,
        gridded_samples: accumulation.gridded_samples,
        skipped_samples: accumulation.skipped_samples,
    })
}

fn compute_psf_standard_metal(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    #[cfg(target_os = "macos")]
    {
        let mut timings = PsfComputationTimings::default();
        let [nx, ny] = gridder.grid_shape();
        let mut psf_grid = Array2::<Complex64>::zeros((nx, ny));
        let executor = StandardMfsMetalExecutor::new_with_resident_bytes(
            gridder,
            execution_config.fixed_tile_resident_bytes,
        )?;

        let grid_started = Instant::now();
        let accumulation = executor.accumulate_psf_grid(batches, &mut psf_grid)?;
        timings.grid = grid_started.elapsed();

        if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
            return Err(ImagingError::NoUsableSamples);
        }

        let fft_started = Instant::now();
        let raw_psf = centered_ifft2_f64(&psf_grid);
        timings.fft = fft_started.elapsed();
        let normalize_started = Instant::now();
        let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
        psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
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
            normalization_sumwt: accumulation.normalization_sumwt as f32,
            reported_sumwt: accumulation.reported_sumwt as f32,
            psf_peak,
            gridded_samples: accumulation.gridded_samples,
            skipped_samples: accumulation.skipped_samples,
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (batches, gridder, execution_config, stage_timings);
        Err(ImagingError::Unsupported(
            "standard MFS backend 'metal' requires macOS Metal and is not available on this platform"
                .to_string(),
        ))
    }
}

#[cfg(test)]
fn compute_dirty_psf_and_residual_standard(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>), ImagingError> {
    let mut executor = StandardMfsCpuExecutor::new(gridder, batches)?;
    compute_dirty_psf_and_residual_standard_with_executor(&mut executor, stage_timings)
}

fn compute_dirty_psf_and_residual_standard_with_executor(
    executor: &mut StandardMfsCpuExecutor<'_>,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>), ImagingError> {
    let (gridder, plan, workspace) = executor.parts_mut();
    let (psf_grid, residual_grid) = workspace.clear_dirty_grids();

    let grid_started = Instant::now();
    for sample in plan.samples() {
        let grid_weight = sample.grid_weight_f64();
        gridder.grid_sample_taps_real_planned_f64(psf_grid, &sample.positive_taps, grid_weight);

        let observed_visibility = sample.visibility;
        if finite_visibility(observed_visibility) {
            let residual = Complex64::new(
                f64::from(observed_visibility.re) * grid_weight,
                f64::from(observed_visibility.im) * grid_weight,
            );
            gridder.grid_sample_taps_planned_f64(residual_grid, &sample.positive_taps, residual);
        }
    }
    let grid_elapsed = grid_started.elapsed();
    let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
    stage_timings.psf_grid += split_grid_elapsed;
    stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);

    let normalization_sumwt = plan.normalization_sumwt();
    let reported_sumwt = plan.reported_sumwt();
    if normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let psf_fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(psf_grid);
    stage_timings.psf_fft += psf_fft_started.elapsed();
    let psf_normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / normalization_sumwt as f32);
    let psf_peak = peak_abs_value(&psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "PSF peak is non-finite or zero".to_string(),
        ));
    }
    psf.mapv_inplace(|value| value / psf_peak);
    stage_timings.psf_normalize += psf_normalize_started.elapsed();

    let residual_fft_started = Instant::now();
    let raw_residual = centered_ifft2_f64(residual_grid);
    stage_timings.residual_fft += residual_fft_started.elapsed();
    let residual_normalize_started = Instant::now();
    let mut residual = gridder.corrected_image_from_grid_f64(&raw_residual);
    residual.mapv_inplace(|value| value / normalization_sumwt as f32 / psf_peak);
    stage_timings.residual_normalize += residual_normalize_started.elapsed();

    Ok((
        PsfState {
            psf,
            normalization_sumwt: normalization_sumwt as f32,
            reported_sumwt: reported_sumwt as f32,
            psf_peak,
            gridded_samples: plan.gridded_samples(),
            skipped_samples: plan.skipped_samples(),
        },
        residual,
    ))
}

fn compute_dirty_psf_and_residual_standard_streaming(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>), ImagingError> {
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((nx, ny));
    let mut residual_grid = Array2::<Complex64>::zeros((nx, ny));

    let grid_started = Instant::now();
    let DirtyGridAccumulation {
        normalization_sumwt,
        reported_sumwt,
        gridded_samples,
        skipped_samples,
        ..
    } = accumulate_dirty_psf_and_residual_standard_streaming_grid(
        batches,
        gridder,
        &mut psf_grid,
        &mut residual_grid,
    )?;
    let grid_elapsed = grid_started.elapsed();
    let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
    stage_timings.psf_grid += split_grid_elapsed;
    stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);

    if normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let psf_fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(&psf_grid);
    stage_timings.psf_fft += psf_fft_started.elapsed();
    let psf_normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / normalization_sumwt as f32);
    let psf_peak = peak_abs_value(&psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "PSF peak is non-finite or zero".to_string(),
        ));
    }
    psf.mapv_inplace(|value| value / psf_peak);
    stage_timings.psf_normalize += psf_normalize_started.elapsed();

    let residual_fft_started = Instant::now();
    let raw_residual = centered_ifft2_f64(&residual_grid);
    stage_timings.residual_fft += residual_fft_started.elapsed();
    let residual_normalize_started = Instant::now();
    let mut residual = gridder.corrected_image_from_grid_f64(&raw_residual);
    residual.mapv_inplace(|value| value / normalization_sumwt as f32 / psf_peak);
    stage_timings.residual_normalize += residual_normalize_started.elapsed();

    Ok((
        PsfState {
            psf,
            normalization_sumwt: normalization_sumwt as f32,
            reported_sumwt: reported_sumwt as f32,
            psf_peak,
            gridded_samples,
            skipped_samples,
        },
        residual,
    ))
}

fn compute_dirty_psf_and_residual_standard_tiled(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>), ImagingError> {
    let [nx, ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex64>::zeros((nx, ny));
    let mut residual_grid = Array2::<Complex64>::zeros((nx, ny));
    let executor =
        StandardMfsTiledCpuExecutor::new_with_execution_config(gridder, execution_config)?;

    let grid_started = Instant::now();
    let accumulation =
        executor.accumulate_dirty_grids(batches, &mut psf_grid, &mut residual_grid)?;
    let grid_elapsed = grid_started.elapsed();
    let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
    stage_timings.psf_grid += split_grid_elapsed;
    stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);

    if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let psf_fft_started = Instant::now();
    let raw_psf = centered_ifft2_f64(&psf_grid);
    stage_timings.psf_fft += psf_fft_started.elapsed();
    let psf_normalize_started = Instant::now();
    let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
    psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
    let psf_peak = peak_abs_value(&psf);
    if !(psf_peak.is_finite() && psf_peak > 0.0) {
        return Err(ImagingError::Normalization(
            "PSF peak is non-finite or zero".to_string(),
        ));
    }
    psf.mapv_inplace(|value| value / psf_peak);
    stage_timings.psf_normalize += psf_normalize_started.elapsed();

    let residual_fft_started = Instant::now();
    let raw_residual = centered_ifft2_f64(&residual_grid);
    stage_timings.residual_fft += residual_fft_started.elapsed();
    let residual_normalize_started = Instant::now();
    let mut residual = gridder.corrected_image_from_grid_f64(&raw_residual);
    residual.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32 / psf_peak);
    stage_timings.residual_normalize += residual_normalize_started.elapsed();

    Ok((
        PsfState {
            psf,
            normalization_sumwt: accumulation.normalization_sumwt as f32,
            reported_sumwt: accumulation.reported_sumwt as f32,
            psf_peak,
            gridded_samples: accumulation.gridded_samples,
            skipped_samples: accumulation.skipped_samples,
        },
        residual,
    ))
}

fn compute_dirty_psf_and_residual_standard_metal(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<(PsfState, Array2<f32>), ImagingError> {
    #[cfg(target_os = "macos")]
    {
        let [nx, ny] = gridder.grid_shape();
        let mut psf_grid = Array2::<Complex64>::zeros((nx, ny));
        let mut residual_grid = Array2::<Complex64>::zeros((nx, ny));
        let executor = StandardMfsMetalExecutor::new_with_resident_bytes(
            gridder,
            execution_config.fixed_tile_resident_bytes,
        )?;

        let grid_started = Instant::now();
        let accumulation =
            executor.accumulate_dirty_grids(batches, &mut psf_grid, &mut residual_grid)?;
        let grid_elapsed = grid_started.elapsed();
        let split_grid_elapsed = Duration::from_secs_f64(grid_elapsed.as_secs_f64() * 0.5);
        stage_timings.psf_grid += split_grid_elapsed;
        stage_timings.residual_degrid_grid += grid_elapsed.saturating_sub(split_grid_elapsed);

        if accumulation.normalization_sumwt <= 0.0 || accumulation.reported_sumwt <= 0.0 {
            return Err(ImagingError::NoUsableSamples);
        }

        let psf_fft_started = Instant::now();
        let raw_psf = centered_ifft2_f64(&psf_grid);
        stage_timings.psf_fft += psf_fft_started.elapsed();
        let psf_normalize_started = Instant::now();
        let mut psf = gridder.corrected_image_from_grid_f64(&raw_psf);
        psf.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32);
        let psf_peak = peak_abs_value(&psf);
        if !(psf_peak.is_finite() && psf_peak > 0.0) {
            return Err(ImagingError::Normalization(
                "PSF peak is non-finite or zero".to_string(),
            ));
        }
        psf.mapv_inplace(|value| value / psf_peak);
        stage_timings.psf_normalize += psf_normalize_started.elapsed();

        let residual_fft_started = Instant::now();
        let raw_residual = centered_ifft2_f64(&residual_grid);
        stage_timings.residual_fft += residual_fft_started.elapsed();
        let residual_normalize_started = Instant::now();
        let mut residual = gridder.corrected_image_from_grid_f64(&raw_residual);
        residual.mapv_inplace(|value| value / accumulation.normalization_sumwt as f32 / psf_peak);
        stage_timings.residual_normalize += residual_normalize_started.elapsed();

        Ok((
            PsfState {
                psf,
                normalization_sumwt: accumulation.normalization_sumwt as f32,
                reported_sumwt: accumulation.reported_sumwt as f32,
                psf_peak,
                gridded_samples: accumulation.gridded_samples,
                skipped_samples: accumulation.skipped_samples,
            },
            residual,
        ))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (batches, gridder, execution_config, stage_timings);
        Err(ImagingError::Unsupported(
            "standard MFS backend 'metal' requires macOS Metal and is not available on this platform"
                .to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DirtyGridAccumulation {
    normalization_sumwt: f64,
    reported_sumwt: f64,
    gridded_samples: usize,
    skipped_samples: usize,
    finite_visibility_samples: usize,
    nonfinite_visibility_samples: usize,
    skipped_not_gridable: usize,
    skipped_invalid_weight: usize,
    skipped_invalid_sumwt: usize,
    skipped_out_of_grid: usize,
}

impl DirtyGridAccumulation {
    fn add(&mut self, other: Self) {
        self.normalization_sumwt += other.normalization_sumwt;
        self.reported_sumwt += other.reported_sumwt;
        self.gridded_samples += other.gridded_samples;
        self.skipped_samples += other.skipped_samples;
        self.finite_visibility_samples += other.finite_visibility_samples;
        self.nonfinite_visibility_samples += other.nonfinite_visibility_samples;
        self.skipped_not_gridable += other.skipped_not_gridable;
        self.skipped_invalid_weight += other.skipped_invalid_weight;
        self.skipped_invalid_sumwt += other.skipped_invalid_sumwt;
        self.skipped_out_of_grid += other.skipped_out_of_grid;
    }
}

fn accumulate_dirty_psf_and_residual_standard_streaming_grid(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    psf_grid: &mut Array2<Complex64>,
    residual_grid: &mut Array2<Complex64>,
) -> Result<DirtyGridAccumulation, ImagingError> {
    let requested_threads = standard_mfs_grid_threads();
    let thread_count = requested_threads
        .min(batches.len())
        .min(thread::available_parallelism().map_or(1, |value| value.get()))
        .max(1);
    if thread_count <= 1 || batches.len() < 2 {
        return Ok(
            accumulate_dirty_psf_and_residual_standard_streaming_grid_serial(
                batches,
                gridder,
                psf_grid,
                residual_grid,
            ),
        );
    }

    let [nx, ny] = gridder.grid_shape();
    let chunk_len = batches.len().div_ceil(thread_count);
    let stage_started = profile::maybe_profile_now();
    let mut local_results = Vec::with_capacity(thread_count);
    let join_started = profile::maybe_profile_now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for (chunk_index, chunk) in batches.chunks(chunk_len).enumerate() {
            let batch_offset = chunk_index * chunk_len;
            let worker_samples = standard_mfs_sample_count(chunk);
            handles.push(scope.spawn(move || {
                let alloc_started = profile::maybe_profile_now();
                let mut local_psf_grid = Array2::<Complex64>::zeros((nx, ny));
                let mut local_residual_grid = Array2::<Complex64>::zeros((nx, ny));
                let alloc_elapsed = profile::elapsed_since(alloc_started);
                let compute_started = profile::maybe_profile_now();
                let counts = accumulate_dirty_psf_and_residual_standard_streaming_grid_serial(
                    chunk,
                    gridder,
                    &mut local_psf_grid,
                    &mut local_residual_grid,
                );
                let compute_elapsed = profile::elapsed_since(compute_started);
                (
                    batch_offset,
                    worker_samples,
                    local_psf_grid,
                    local_residual_grid,
                    counts,
                    alloc_elapsed,
                    compute_elapsed,
                )
            }));
        }
        for handle in handles {
            local_results.push(handle.join().map_err(|_| {
                ImagingError::Unsupported("standard MFS dirty grid worker panicked".to_string())
            })?);
        }
        Ok::<(), ImagingError>(())
    })?;
    let join_elapsed = profile::elapsed_since(join_started);

    local_results.sort_by_key(|(batch_offset, _, _, _, _, _, _)| *batch_offset);
    let mut counts = DirtyGridAccumulation::default();
    let merge_started = profile::maybe_profile_now();
    for (_, _, local_psf_grid, local_residual_grid, local_counts, _, _) in &local_results {
        add_complex64_grid(psf_grid, local_psf_grid);
        add_complex64_grid(residual_grid, local_residual_grid);
        counts.add(*local_counts);
    }
    let merge_elapsed = profile::elapsed_since(merge_started);
    profile::log_parallel_stage(profile::ParallelStageProfile {
        stage: "dirty_psf_residual_grid",
        requested_threads,
        actual_threads: local_results.len(),
        chunking: "batch",
        chunk_len,
        samples_total: standard_mfs_sample_count(batches),
        samples_per_worker: local_results
            .iter()
            .map(|(_, worker_samples, _, _, _, _, _)| *worker_samples)
            .collect(),
        local_grid_bytes_per_worker: complex64_grid_bytes(nx, ny, 2),
        local_grid_count: 2,
        local_alloc_zero_by_worker: local_results
            .iter()
            .map(|(_, _, _, _, _, alloc_elapsed, _)| *alloc_elapsed)
            .collect(),
        worker_compute_by_worker: local_results
            .iter()
            .map(|(_, _, _, _, _, _, compute_elapsed)| *compute_elapsed)
            .collect(),
        join_duration: join_elapsed,
        merge_duration: merge_elapsed,
        stage_duration: profile::elapsed_since(stage_started),
    });
    for (worker_index, (_, worker_samples, _, _, local_counts, alloc_elapsed, compute_elapsed)) in
        local_results.iter().enumerate()
    {
        profile::log_parallel_worker(profile::ParallelWorkerProfile {
            stage: "dirty_psf_residual_grid",
            worker_index,
            samples: *worker_samples,
            accepted_samples: local_counts.gridded_samples,
            finite_visibility_samples: local_counts.finite_visibility_samples,
            nonfinite_visibility_samples: local_counts.nonfinite_visibility_samples,
            skipped_not_gridable: local_counts.skipped_not_gridable,
            skipped_invalid_weight: local_counts.skipped_invalid_weight,
            skipped_invalid_sumwt: local_counts.skipped_invalid_sumwt,
            skipped_invalid_density: 0,
            skipped_out_of_grid: local_counts.skipped_out_of_grid,
            degrid_tap_visits: 0,
            grid_tap_visits: local_counts.gridded_samples.saturating_mul(49),
            density_cell_hits: 0,
            local_alloc_zero: *alloc_elapsed,
            worker_compute: *compute_elapsed,
        });
    }
    Ok(counts)
}

fn accumulate_dirty_psf_and_residual_standard_streaming_grid_serial(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    psf_grid: &mut Array2<Complex64>,
    residual_grid: &mut Array2<Complex64>,
) -> DirtyGridAccumulation {
    let mut counts = DirtyGridAccumulation::default();
    let collect_profile = profile::standard_mfs_profile_detail_enabled();
    for batch in batches {
        for sample_index in 0..batch.len() {
            if !batch.gridable[sample_index] {
                counts.skipped_samples += 1;
                if collect_profile {
                    counts.skipped_not_gridable += 1;
                }
                continue;
            }
            let weight = batch.weight[sample_index];
            let sumwt_factor = batch.sumwt_factor[sample_index];
            if !(weight.is_finite()
                && weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0)
            {
                counts.skipped_samples += 1;
                if collect_profile {
                    if !(weight.is_finite() && weight > 0.0) {
                        counts.skipped_invalid_weight += 1;
                    } else {
                        counts.skipped_invalid_sumwt += 1;
                    }
                }
                continue;
            }
            let Some(plan) = gridder
                .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
            else {
                counts.skipped_samples += 1;
                if collect_profile {
                    counts.skipped_out_of_grid += 1;
                }
                continue;
            };
            let grid_weight = f64::from(weight * sumwt_factor);
            counts.normalization_sumwt += grid_weight;
            counts.reported_sumwt += grid_weight;
            counts.gridded_samples += 1;
            let observed_visibility = batch.visibility[sample_index];
            if finite_visibility(observed_visibility) {
                if collect_profile {
                    counts.finite_visibility_samples += 1;
                }
                let residual = Complex64::new(
                    f64::from(observed_visibility.re) * grid_weight,
                    f64::from(observed_visibility.im) * grid_weight,
                );
                gridder.grid_sample_taps_real_complex_pair_planned_f64(
                    psf_grid,
                    grid_weight,
                    residual_grid,
                    residual,
                    &plan,
                );
            } else {
                if collect_profile {
                    counts.nonfinite_visibility_samples += 1;
                }
                gridder.grid_sample_taps_real_planned_f64(psf_grid, &plan, grid_weight);
            }
        }
    }
    counts
}

fn compute_residual(
    request: &ImagingRequest,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    match request.w_term_mode {
        WTermMode::Direct => {
            return compute_residual_direct(
                request.geometry,
                batches,
                model,
                psf_state,
                stage_timings,
            );
        }
        WTermMode::WProject => {
            return compute_residual_w_project(
                request.geometry,
                batches,
                gridder,
                model,
                psf_state,
                request.w_project_planes,
                stage_timings,
            );
        }
        WTermMode::None => {}
    }
    compute_residual_standard(
        request.geometry,
        batches,
        gridder,
        model,
        psf_state,
        false,
        execution_config,
        stage_timings,
    )
}

#[derive(Debug, Clone, Copy)]
struct ResidualRefreshTimingSnapshot {
    model_fft: Duration,
    residual_degrid_grid: Duration,
    residual_fft: Duration,
    residual_normalize: Duration,
}

impl ResidualRefreshTimingSnapshot {
    fn capture(timings: &ImagingStageTimings) -> Self {
        Self {
            model_fft: timings.model_fft,
            residual_degrid_grid: timings.residual_degrid_grid,
            residual_fft: timings.residual_fft,
            residual_normalize: timings.residual_normalize,
        }
    }

    fn accounted_delta(self, timings: &ImagingStageTimings) -> Duration {
        timings
            .model_fft
            .saturating_sub(self.model_fft)
            .saturating_add(
                timings
                    .residual_degrid_grid
                    .saturating_sub(self.residual_degrid_grid),
            )
            .saturating_add(timings.residual_fft.saturating_sub(self.residual_fft))
            .saturating_add(
                timings
                    .residual_normalize
                    .saturating_sub(self.residual_normalize),
            )
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_standard_mfs_residual(
    request: &ImagingRequest,
    batches: &[VisibilityBatch],
    standard_executor: &mut Option<StandardMfsCpuExecutor<'_>>,
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    let before = ResidualRefreshTimingSnapshot::capture(stage_timings);
    let refresh_started = Instant::now();
    let residual = if let Some(executor) = standard_executor.as_mut() {
        compute_residual_standard_with_executor(executor, model, psf_state, stage_timings)?
    } else {
        compute_residual(
            request,
            batches,
            gridder,
            model,
            psf_state,
            execution_config,
            stage_timings,
        )?
    };
    let refresh_elapsed = refresh_started.elapsed();
    let accounted = before.accounted_delta(stage_timings);
    stage_timings.major_cycle_refresh += refresh_elapsed;
    stage_timings.residual_refresh_overhead += refresh_elapsed.saturating_sub(accounted);
    Ok(residual)
}

fn build_standard_residual_sample_plans(
    gridder: &StandardGridder,
    batches: &[VisibilityBatch],
) -> Vec<Vec<Option<PlannedSample>>> {
    batches
        .iter()
        .map(|batch| {
            batch
                .u_lambda
                .iter()
                .zip(batch.v_lambda.iter())
                .zip(batch.weight.iter())
                .zip(batch.visibility.iter())
                .zip(batch.gridable.iter())
                .map(
                    |((((&u_lambda, &v_lambda), &weight), &visibility), &gridable)| {
                        if gridable
                            && weight.is_finite()
                            && weight > 0.0
                            && visibility.re.is_finite()
                            && visibility.im.is_finite()
                        {
                            gridder.plan_sample(u_lambda, v_lambda)
                        } else {
                            None
                        }
                    },
                )
                .collect()
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn compute_residual_standard(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    use_direct_point_predict: bool,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    if !use_direct_point_predict
        && (standard_mfs_fixed_tile_backend_enabled() || standard_mfs_metal_backend_enabled())
    {
        return compute_residual_standard_tiled(
            batches,
            gridder,
            model,
            psf_state,
            execution_config,
            stage_timings,
        );
    }
    if !use_direct_point_predict
        && standard_mfs_sample_count(batches) <= standard_mfs_executor_max_samples()
    {
        let mut executor = StandardMfsCpuExecutor::new(gridder, batches)?;
        return compute_residual_standard_with_executor(
            &mut executor,
            model,
            psf_state,
            stage_timings,
        );
    }
    compute_residual_standard_streaming(
        geometry,
        batches,
        gridder,
        model,
        psf_state,
        use_direct_point_predict,
        stage_timings,
    )
}

fn compute_residual_standard_streaming(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    use_direct_point_predict: bool,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    Ok(compute_residual_standard_internal(
        geometry,
        batches,
        gridder,
        model,
        psf_state,
        use_direct_point_predict,
        false,
        stage_timings,
    )?
    .residual_image)
}

fn compute_residual_standard_tiled(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    execution_config: StandardMfsExecutionConfig,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    let [nx, ny] = gridder.grid_shape();
    let mut residual_grid = Array2::<Complex64>::zeros((nx, ny));
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
    let executor =
        StandardMfsTiledCpuExecutor::new_with_execution_config(gridder, execution_config)?;
    let counts =
        executor.accumulate_residual_grid(batches, model_grid.as_ref(), &mut residual_grid)?;
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2_f64(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid_f64(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;

    profile::log_serial_stage(profile::SerialStageProfile {
        stage: "tiled_residual_refresh",
        samples_total: standard_mfs_sample_count(batches),
        finite_visibility_samples: counts.valid_samples,
        nonfinite_visibility_samples: counts.skipped_nonfinite_visibility,
        planned_samples: counts.planned_samples,
        model_grid_present_samples: if model_grid.is_some() {
            counts.gridded_residual_samples
        } else {
            0
        },
        model_grid_absent_samples: if model_grid.is_some() {
            0
        } else {
            counts.gridded_residual_samples
        },
        degrid_tap_visits: if model_grid.is_some() {
            counts.gridded_residual_samples.saturating_mul(49)
        } else {
            0
        },
        grid_tap_visits: counts.gridded_residual_samples.saturating_mul(49),
        stage_duration: timings.degrid_grid,
    });

    Ok(image)
}

fn compute_residual_standard_with_executor(
    executor: &mut StandardMfsCpuExecutor<'_>,
    model: &Array2<f32>,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    let (gridder, plan, workspace) = executor.parts_mut();
    if profile::standard_mfs_profile_detail_enabled() {
        let [nx, ny] = gridder.grid_shape();
        eprintln!(
            "standard_mfs_executor_plan stage=residual_refresh samples={} skipped_samples={} normalization_sumwt={:.12e} reported_sumwt={:.12e} sample_plan_bytes={} residual_grid_bytes={}",
            plan.gridded_samples(),
            plan.skipped_samples(),
            plan.normalization_sumwt(),
            plan.reported_sumwt(),
            plan.estimated_bytes(),
            complex64_grid_bytes(nx, ny, 1),
        );
    }
    let residual_grid = workspace.clear_residual_grid();
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
    accumulate_standard_mfs_residual_grid(gridder, plan, model_grid.as_ref(), residual_grid)?;
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2_f64(residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid_f64(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    Ok(image)
}

fn accumulate_standard_mfs_residual_grid(
    gridder: &StandardGridder,
    plan: &StandardMfsVisibilityPlan,
    model_grid: Option<&Array2<Complex32>>,
    residual_grid: &mut Array2<Complex64>,
) -> Result<(), ImagingError> {
    let requested_threads = standard_mfs_grid_threads();
    let thread_count = requested_threads
        .min(plan.samples().len())
        .min(thread::available_parallelism().map_or(1, |value| value.get()))
        .max(1);
    if thread_count <= 1 || plan.samples().len() < 100_000 {
        let stage_started = profile::maybe_profile_now();
        let mut finite_samples = 0usize;
        for sample in plan.samples() {
            if finite_visibility(sample.visibility) {
                finite_samples += 1;
            }
            accumulate_standard_mfs_residual_sample(gridder, model_grid, sample, residual_grid);
        }
        let stage_duration = profile::elapsed_since(stage_started);
        profile::log_serial_stage(profile::SerialStageProfile {
            stage: "executor_residual_refresh",
            samples_total: plan.samples().len(),
            finite_visibility_samples: finite_samples,
            nonfinite_visibility_samples: plan.samples().len().saturating_sub(finite_samples),
            planned_samples: plan.samples().len(),
            model_grid_present_samples: if model_grid.is_some() {
                finite_samples
            } else {
                0
            },
            model_grid_absent_samples: if model_grid.is_some() {
                0
            } else {
                finite_samples
            },
            degrid_tap_visits: if model_grid.is_some() {
                finite_samples.saturating_mul(49)
            } else {
                0
            },
            grid_tap_visits: finite_samples.saturating_mul(49),
            stage_duration,
        });
        return Ok(());
    }

    let [nx, ny] = gridder.grid_shape();
    let chunk_len = plan.samples().len().div_ceil(thread_count);
    let stage_started = profile::maybe_profile_now();
    let mut local_grids = Vec::with_capacity(thread_count);
    let join_started = profile::maybe_profile_now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for samples in plan.samples().chunks(chunk_len) {
            handles.push(scope.spawn(move || {
                let alloc_started = profile::maybe_profile_now();
                let mut local_grid = Array2::<Complex64>::zeros((nx, ny));
                let alloc_elapsed = profile::elapsed_since(alloc_started);
                let compute_started = profile::maybe_profile_now();
                let mut finite_samples = 0usize;
                for sample in samples {
                    if finite_visibility(sample.visibility) {
                        finite_samples += 1;
                    }
                    accumulate_standard_mfs_residual_sample(
                        gridder,
                        model_grid,
                        sample,
                        &mut local_grid,
                    );
                }
                let compute_elapsed = profile::elapsed_since(compute_started);
                (
                    samples.len(),
                    finite_samples,
                    local_grid,
                    alloc_elapsed,
                    compute_elapsed,
                )
            }));
        }
        for handle in handles {
            local_grids.push(handle.join().map_err(|_| {
                ImagingError::Unsupported("standard MFS grid worker panicked".to_string())
            })?);
        }
        Ok::<(), ImagingError>(())
    })?;
    let join_elapsed = profile::elapsed_since(join_started);

    let merge_started = profile::maybe_profile_now();
    for (_, _, local_grid, _, _) in &local_grids {
        add_complex64_grid(residual_grid, local_grid);
    }
    let merge_elapsed = profile::elapsed_since(merge_started);
    let finite_samples = local_grids
        .iter()
        .map(|(_, finite_samples, _, _, _)| *finite_samples)
        .sum::<usize>();
    profile::log_parallel_stage(profile::ParallelStageProfile {
        stage: "executor_residual_refresh_grid",
        requested_threads,
        actual_threads: local_grids.len(),
        chunking: "planned_sample",
        chunk_len,
        samples_total: plan.samples().len(),
        samples_per_worker: local_grids
            .iter()
            .map(|(worker_samples, _, _, _, _)| *worker_samples)
            .collect(),
        local_grid_bytes_per_worker: complex64_grid_bytes(nx, ny, 1),
        local_grid_count: 1,
        local_alloc_zero_by_worker: local_grids
            .iter()
            .map(|(_, _, _, alloc_elapsed, _)| *alloc_elapsed)
            .collect(),
        worker_compute_by_worker: local_grids
            .iter()
            .map(|(_, _, _, _, compute_elapsed)| *compute_elapsed)
            .collect(),
        join_duration: join_elapsed,
        merge_duration: merge_elapsed,
        stage_duration: profile::elapsed_since(stage_started),
    });
    for (worker_index, (worker_samples, finite_samples, _, alloc_elapsed, compute_elapsed)) in
        local_grids.iter().enumerate()
    {
        profile::log_parallel_worker(profile::ParallelWorkerProfile {
            stage: "executor_residual_refresh_grid",
            worker_index,
            samples: *worker_samples,
            accepted_samples: *finite_samples,
            finite_visibility_samples: *finite_samples,
            nonfinite_visibility_samples: worker_samples.saturating_sub(*finite_samples),
            skipped_not_gridable: 0,
            skipped_invalid_weight: 0,
            skipped_invalid_sumwt: 0,
            skipped_invalid_density: 0,
            skipped_out_of_grid: 0,
            degrid_tap_visits: if model_grid.is_some() {
                finite_samples.saturating_mul(49)
            } else {
                0
            },
            grid_tap_visits: finite_samples.saturating_mul(49),
            density_cell_hits: 0,
            local_alloc_zero: *alloc_elapsed,
            worker_compute: *compute_elapsed,
        });
    }
    profile::log_serial_stage(profile::SerialStageProfile {
        stage: "executor_residual_refresh_counts",
        samples_total: plan.samples().len(),
        finite_visibility_samples: finite_samples,
        nonfinite_visibility_samples: plan.samples().len().saturating_sub(finite_samples),
        planned_samples: plan.samples().len(),
        model_grid_present_samples: if model_grid.is_some() {
            finite_samples
        } else {
            0
        },
        model_grid_absent_samples: if model_grid.is_some() {
            0
        } else {
            finite_samples
        },
        degrid_tap_visits: if model_grid.is_some() {
            finite_samples.saturating_mul(49)
        } else {
            0
        },
        grid_tap_visits: finite_samples.saturating_mul(49),
        stage_duration: Duration::ZERO,
    });
    Ok(())
}

fn accumulate_standard_mfs_residual_sample(
    gridder: &StandardGridder,
    model_grid: Option<&Array2<Complex32>>,
    sample: &StandardMfsPlannedSample,
    residual_grid: &mut Array2<Complex64>,
) {
    let observed_visibility = sample.visibility;
    if !finite_visibility(observed_visibility) {
        return;
    }
    let residual_weight = sample.grid_weight_f64();
    if let Some(model_grid) = model_grid {
        gridder.degrid_model_and_grid_residual_taps_planned_f64(
            model_grid,
            residual_grid,
            &sample.positive_taps,
            observed_visibility,
            residual_weight,
        );
    } else {
        let residual = Complex64::new(
            f64::from(observed_visibility.re) * residual_weight,
            f64::from(observed_visibility.im) * residual_weight,
        );
        gridder.grid_sample_taps_planned_f64(residual_grid, &sample.positive_taps, residual);
    }
}

fn add_complex64_grid(target: &mut Array2<Complex64>, source: &Array2<Complex64>) {
    if let (Some(target), Some(source)) = (
        target.as_slice_memory_order_mut(),
        source.as_slice_memory_order(),
    ) {
        for (target, source) in target.iter_mut().zip(source.iter()) {
            *target += *source;
        }
        return;
    }
    Zip::from(target).and(source).for_each(|target, source| {
        *target += *source;
    });
}

#[derive(Debug, Clone, Copy, Default)]
struct ResidualGridAccumulation {
    valid_samples: usize,
    planned_samples: usize,
    gridded_residual_samples: usize,
    skipped_not_gridable: usize,
    skipped_invalid_weight: usize,
    skipped_invalid_sumwt: usize,
    skipped_out_of_grid: usize,
    skipped_nonfinite_visibility: usize,
}

impl ResidualGridAccumulation {
    fn add(&mut self, other: Self) {
        self.valid_samples += other.valid_samples;
        self.planned_samples += other.planned_samples;
        self.gridded_residual_samples += other.gridded_residual_samples;
        self.skipped_not_gridable += other.skipped_not_gridable;
        self.skipped_invalid_weight += other.skipped_invalid_weight;
        self.skipped_invalid_sumwt += other.skipped_invalid_sumwt;
        self.skipped_out_of_grid += other.skipped_out_of_grid;
        self.skipped_nonfinite_visibility += other.skipped_nonfinite_visibility;
    }
}

#[allow(clippy::too_many_arguments)]
fn accumulate_streaming_standard_mfs_residual_grid_serial(
    gridder: &StandardGridder,
    batches: &[VisibilityBatch],
    model_grid: Option<&Array2<Complex32>>,
    direct_components: Option<&Vec<DirectComponent>>,
    use_direct_point_predict: bool,
    mut samples: Option<&mut Vec<ResidualSampleTraceInternal>>,
    residual_grid: &mut Array2<Complex64>,
) -> ResidualGridAccumulation {
    let mut counts = ResidualGridAccumulation::default();
    let mut census = StandardMfsTapCensus::new("streaming_residual_grid");
    let collect_profile = profile::standard_mfs_profile_detail_enabled();
    for (batch_index, batch) in batches.iter().enumerate() {
        for index in 0..batch.len() {
            accumulate_streaming_standard_mfs_residual_sample(
                gridder,
                batch,
                batch_index,
                index,
                model_grid,
                direct_components,
                use_direct_point_predict,
                samples.as_deref_mut(),
                residual_grid,
                &mut counts,
                collect_profile,
                census.as_mut(),
            );
        }
    }
    if let Some(census) = census {
        census.log(std::mem::size_of::<StandardMfsPlannedSample>());
    }
    counts
}

fn accumulate_streaming_standard_mfs_residual_grid_parallel(
    gridder: &StandardGridder,
    batches: &[VisibilityBatch],
    model_grid: Option<&Array2<Complex32>>,
    residual_grid: &mut Array2<Complex64>,
    requested_threads: usize,
) -> Result<ResidualGridAccumulation, ImagingError> {
    let thread_count = requested_threads
        .min(batches.len())
        .min(thread::available_parallelism().map_or(1, |value| value.get()))
        .max(1);
    if thread_count <= 1 || batches.len() < 2 {
        let stage_started = profile::maybe_profile_now();
        let counts = accumulate_streaming_standard_mfs_residual_grid_serial(
            gridder,
            batches,
            model_grid,
            None,
            false,
            None,
            residual_grid,
        );
        let samples_total = standard_mfs_sample_count(batches);
        profile::log_serial_stage(profile::SerialStageProfile {
            stage: "streaming_residual_refresh",
            samples_total,
            finite_visibility_samples: counts.valid_samples,
            nonfinite_visibility_samples: samples_total.saturating_sub(counts.valid_samples),
            planned_samples: counts.planned_samples,
            model_grid_present_samples: if model_grid.is_some() {
                counts.gridded_residual_samples
            } else {
                0
            },
            model_grid_absent_samples: if model_grid.is_some() {
                0
            } else {
                counts.gridded_residual_samples
            },
            degrid_tap_visits: if model_grid.is_some() {
                counts.gridded_residual_samples.saturating_mul(49)
            } else {
                0
            },
            grid_tap_visits: counts.gridded_residual_samples.saturating_mul(49),
            stage_duration: profile::elapsed_since(stage_started),
        });
        return Ok(counts);
    }

    let [nx, ny] = gridder.grid_shape();
    let chunk_len = batches.len().div_ceil(thread_count);
    let stage_started = profile::maybe_profile_now();
    let mut local_results = Vec::with_capacity(thread_count);
    let join_started = profile::maybe_profile_now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for (chunk_index, chunk) in batches.chunks(chunk_len).enumerate() {
            let batch_offset = chunk_index * chunk_len;
            let worker_samples = standard_mfs_sample_count(chunk);
            handles.push(scope.spawn(move || {
                let alloc_started = profile::maybe_profile_now();
                let mut local_grid = Array2::<Complex64>::zeros((nx, ny));
                let alloc_elapsed = profile::elapsed_since(alloc_started);
                let compute_started = profile::maybe_profile_now();
                let counts = accumulate_streaming_standard_mfs_residual_grid_serial(
                    gridder,
                    chunk,
                    model_grid,
                    None,
                    false,
                    None,
                    &mut local_grid,
                );
                let compute_elapsed = profile::elapsed_since(compute_started);
                (
                    batch_offset,
                    worker_samples,
                    local_grid,
                    counts,
                    alloc_elapsed,
                    compute_elapsed,
                )
            }));
        }
        for handle in handles {
            local_results.push(handle.join().map_err(|_| {
                ImagingError::Unsupported("standard MFS streaming grid worker panicked".to_string())
            })?);
        }
        Ok::<(), ImagingError>(())
    })?;
    let join_elapsed = profile::elapsed_since(join_started);

    local_results.sort_by_key(|(batch_offset, _, _, _, _, _)| *batch_offset);
    let mut counts = ResidualGridAccumulation::default();
    let merge_started = profile::maybe_profile_now();
    for (_, _, local_grid, local_counts, _, _) in &local_results {
        add_complex64_grid(residual_grid, local_grid);
        counts.add(*local_counts);
    }
    let merge_elapsed = profile::elapsed_since(merge_started);
    profile::log_parallel_stage(profile::ParallelStageProfile {
        stage: "streaming_residual_refresh_grid",
        requested_threads,
        actual_threads: local_results.len(),
        chunking: "batch",
        chunk_len,
        samples_total: standard_mfs_sample_count(batches),
        samples_per_worker: local_results
            .iter()
            .map(|(_, worker_samples, _, _, _, _)| *worker_samples)
            .collect(),
        local_grid_bytes_per_worker: complex64_grid_bytes(nx, ny, 1),
        local_grid_count: 1,
        local_alloc_zero_by_worker: local_results
            .iter()
            .map(|(_, _, _, _, alloc_elapsed, _)| *alloc_elapsed)
            .collect(),
        worker_compute_by_worker: local_results
            .iter()
            .map(|(_, _, _, _, _, compute_elapsed)| *compute_elapsed)
            .collect(),
        join_duration: join_elapsed,
        merge_duration: merge_elapsed,
        stage_duration: profile::elapsed_since(stage_started),
    });
    for (worker_index, (_, worker_samples, _, local_counts, alloc_elapsed, compute_elapsed)) in
        local_results.iter().enumerate()
    {
        profile::log_parallel_worker(profile::ParallelWorkerProfile {
            stage: "streaming_residual_refresh_grid",
            worker_index,
            samples: *worker_samples,
            accepted_samples: local_counts.planned_samples,
            finite_visibility_samples: local_counts.valid_samples,
            nonfinite_visibility_samples: local_counts.skipped_nonfinite_visibility,
            skipped_not_gridable: local_counts.skipped_not_gridable,
            skipped_invalid_weight: local_counts.skipped_invalid_weight,
            skipped_invalid_sumwt: local_counts.skipped_invalid_sumwt,
            skipped_invalid_density: 0,
            skipped_out_of_grid: local_counts.skipped_out_of_grid,
            degrid_tap_visits: if model_grid.is_some() {
                local_counts.gridded_residual_samples.saturating_mul(49)
            } else {
                0
            },
            grid_tap_visits: local_counts.gridded_residual_samples.saturating_mul(49),
            density_cell_hits: 0,
            local_alloc_zero: *alloc_elapsed,
            worker_compute: *compute_elapsed,
        });
    }
    profile::log_serial_stage(profile::SerialStageProfile {
        stage: "streaming_residual_refresh_counts",
        samples_total: standard_mfs_sample_count(batches),
        finite_visibility_samples: counts.valid_samples,
        nonfinite_visibility_samples: standard_mfs_sample_count(batches)
            .saturating_sub(counts.valid_samples),
        planned_samples: counts.planned_samples,
        model_grid_present_samples: if model_grid.is_some() {
            counts.gridded_residual_samples
        } else {
            0
        },
        model_grid_absent_samples: if model_grid.is_some() {
            0
        } else {
            counts.gridded_residual_samples
        },
        degrid_tap_visits: if model_grid.is_some() {
            counts.gridded_residual_samples.saturating_mul(49)
        } else {
            0
        },
        grid_tap_visits: counts.gridded_residual_samples.saturating_mul(49),
        stage_duration: Duration::ZERO,
    });
    Ok(counts)
}

#[allow(clippy::too_many_arguments)]
fn accumulate_streaming_standard_mfs_residual_sample(
    gridder: &StandardGridder,
    batch: &VisibilityBatch,
    batch_index: usize,
    index: usize,
    model_grid: Option<&Array2<Complex32>>,
    direct_components: Option<&Vec<DirectComponent>>,
    use_direct_point_predict: bool,
    samples: Option<&mut Vec<ResidualSampleTraceInternal>>,
    residual_grid: &mut Array2<Complex64>,
    counts: &mut ResidualGridAccumulation,
    collect_profile: bool,
    mut census: Option<&mut StandardMfsTapCensus>,
) {
    let weight = batch.weight[index];
    let observed_visibility = batch.visibility[index];
    let gridable = batch.gridable[index];
    let planned_sample: Option<PositiveTapSet> =
        if gridable && weight.is_finite() && weight > 0.0 && finite_visibility(observed_visibility)
        {
            counts.valid_samples += 1;
            let planned_sample =
                gridder.plan_positive_taps(batch.u_lambda[index], batch.v_lambda[index]);
            if let Some(plan) = planned_sample.as_ref() {
                counts.planned_samples += 1;
                if let Some(census) = census.as_mut() {
                    census.observe_accepted(plan);
                }
            } else {
                if collect_profile {
                    counts.skipped_out_of_grid += 1;
                }
                if let Some(census) = census.as_mut() {
                    census.observe_skip(StandardMfsTapSkipReason::OutOfGrid);
                }
            }
            planned_sample
        } else {
            if !gridable {
                if collect_profile {
                    counts.skipped_not_gridable += 1;
                }
                if let Some(census) = census.as_mut() {
                    census.observe_skip(StandardMfsTapSkipReason::NotGridable);
                }
            } else if !(weight.is_finite() && weight > 0.0) {
                if collect_profile {
                    counts.skipped_invalid_weight += 1;
                }
                if let Some(census) = census.as_mut() {
                    census.observe_skip(StandardMfsTapSkipReason::InvalidWeight);
                }
            } else {
                if collect_profile {
                    counts.skipped_nonfinite_visibility += 1;
                }
                if let Some(census) = census.as_mut() {
                    census.observe_skip(StandardMfsTapSkipReason::NonfiniteVisibility);
                }
            }
            None
        };
    let sumwt_factor = batch.sumwt_factor[index];
    let sumwt_valid = sumwt_factor.is_finite() && sumwt_factor > 0.0;
    let mut fused_residual_gridded = false;
    let predicted_visibility = if let Some(plan) = planned_sample.as_ref() {
        if use_direct_point_predict {
            direct_components.map_or_else(
                || Complex32::new(0.0, 0.0),
                |components| {
                    direct_predict_visibility(
                        components,
                        batch.u_lambda[index],
                        batch.v_lambda[index],
                        0.0,
                    )
                },
            )
        } else if let Some(model_grid) = model_grid {
            if sumwt_valid {
                let residual_weight = f64::from(weight * sumwt_factor);
                counts.gridded_residual_samples += 1;
                fused_residual_gridded = true;
                gridder.degrid_model_and_grid_residual_taps_planned_f64(
                    model_grid,
                    residual_grid,
                    plan,
                    observed_visibility,
                    residual_weight,
                )
            } else {
                gridder.degrid_sample_taps_planned_normalized(model_grid, plan)
            }
        } else {
            Complex32::new(0.0, 0.0)
        }
    } else {
        Complex32::new(0.0, 0.0)
    };
    let residual_visibility = observed_visibility - predicted_visibility;
    if let Some(samples) = samples {
        samples.push(ResidualSampleTraceInternal {
            batch_index,
            sample_index: index,
            u_lambda: batch.u_lambda[index],
            v_lambda: batch.v_lambda[index],
            w_lambda: batch.w_lambda[index],
            observed_visibility,
            predicted_visibility,
            residual_visibility,
            weight,
            gridable,
        });
    }
    let Some(plan) = planned_sample.as_ref() else {
        return;
    };
    if !sumwt_valid {
        if collect_profile {
            counts.skipped_invalid_sumwt += 1;
        }
        if let Some(census) = census.as_mut() {
            census.observe_skip(StandardMfsTapSkipReason::InvalidSumwt);
        }
        return;
    }
    if fused_residual_gridded {
        return;
    }
    counts.gridded_residual_samples += 1;
    let residual_weight = f64::from(weight * sumwt_factor);
    let residual = Complex64::new(
        f64::from(residual_visibility.re) * residual_weight,
        f64::from(residual_visibility.im) * residual_weight,
    );
    gridder.grid_sample_taps_planned_f64(residual_grid, plan, residual);
}

fn compute_residual_trace_standard(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    use_direct_point_predict: bool,
    stage_timings: &mut ImagingStageTimings,
) -> Result<ResidualRefreshTraceInternal, ImagingError> {
    compute_residual_standard_internal(
        geometry,
        batches,
        gridder,
        model,
        psf_state,
        use_direct_point_predict,
        true,
        stage_timings,
    )
}

#[allow(clippy::too_many_arguments)]
fn compute_residual_standard_internal(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    use_direct_point_predict: bool,
    capture_samples: bool,
    stage_timings: &mut ImagingStageTimings,
) -> Result<ResidualRefreshTraceInternal, ImagingError> {
    let trace_timing = env::var_os("CASA_RS_TRACE_RESIDUAL_TIMING").is_some();
    let total_started = trace_timing.then(Instant::now);
    let [nx, ny] = gridder.grid_shape();
    let mut residual_grid = Array2::<Complex64>::zeros((nx, ny));
    let mut timings = ResidualComputationTimings::default();
    let mut samples = if capture_samples {
        Vec::with_capacity(batches.iter().map(VisibilityBatch::len).sum())
    } else {
        Vec::new()
    };
    let model_nonzero_components = model.iter().filter(|&&value| value.abs() > 0.0).count();
    let direct_setup_started = trace_timing.then(Instant::now);
    let direct_pixels = use_direct_point_predict.then(|| build_direct_pixel_coordinates(geometry));
    let direct_components = direct_pixels
        .as_ref()
        .map(|pixels| build_direct_components(model, pixels, geometry.image_shape[1]));
    let direct_setup_elapsed = direct_setup_started.map(|started| started.elapsed());
    let model_grid = if !use_direct_point_predict && model_nonzero_components > 0 {
        let model_fft_started = Instant::now();
        let transformed = centered_fft2(&gridder.apodize_model(model));
        timings.model_fft = model_fft_started.elapsed();
        Some(transformed)
    } else {
        None
    };

    let degrid_grid_started = Instant::now();
    let grid_threads = if capture_samples || use_direct_point_predict {
        1
    } else {
        standard_mfs_grid_threads()
    };
    let samples_total = standard_mfs_sample_count(batches);
    let ResidualGridAccumulation {
        valid_samples,
        planned_samples,
        gridded_residual_samples,
        ..
    } = if grid_threads > 1 {
        accumulate_streaming_standard_mfs_residual_grid_parallel(
            gridder,
            batches,
            model_grid.as_ref(),
            &mut residual_grid,
            grid_threads,
        )?
    } else {
        let stage_started = profile::maybe_profile_now();
        let counts = accumulate_streaming_standard_mfs_residual_grid_serial(
            gridder,
            batches,
            model_grid.as_ref(),
            direct_components.as_ref(),
            use_direct_point_predict,
            capture_samples.then_some(&mut samples),
            &mut residual_grid,
        );
        profile::log_serial_stage(profile::SerialStageProfile {
            stage: "streaming_residual_refresh",
            samples_total,
            finite_visibility_samples: counts.valid_samples,
            nonfinite_visibility_samples: samples_total.saturating_sub(counts.valid_samples),
            planned_samples: counts.planned_samples,
            model_grid_present_samples: if model_grid.is_some() {
                counts.gridded_residual_samples
            } else {
                0
            },
            model_grid_absent_samples: if model_grid.is_some() {
                0
            } else {
                counts.gridded_residual_samples
            },
            degrid_tap_visits: if model_grid.is_some() {
                counts.gridded_residual_samples.saturating_mul(49)
            } else {
                0
            },
            grid_tap_visits: counts.gridded_residual_samples.saturating_mul(49),
            stage_duration: profile::elapsed_since(stage_started),
        });
        counts
    };
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2_f64(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid_f64(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    if trace_timing {
        eprintln!(
            "CASA_RS_TRACE_RESIDUAL_TIMING residual_refresh mode={} grid_threads={} batches={} input_samples={} valid_samples={} planned_samples={} gridded_residual_samples={} model_nonzero={} direct_components={} direct_setup_ms={:.3} model_fft_ms={:.3} degrid_grid_ms={:.3} residual_fft_ms={:.3} normalize_ms={:.3} total_ms={:.3}",
            if use_direct_point_predict {
                "direct"
            } else {
                "fft_grid"
            },
            grid_threads,
            batches.len(),
            batches.iter().map(VisibilityBatch::len).sum::<usize>(),
            valid_samples,
            planned_samples,
            gridded_residual_samples,
            model_nonzero_components,
            direct_components.as_ref().map_or(0, Vec::len),
            direct_setup_elapsed.unwrap_or_default().as_secs_f64() * 1000.0,
            timings.model_fft.as_secs_f64() * 1000.0,
            timings.degrid_grid.as_secs_f64() * 1000.0,
            timings.fft.as_secs_f64() * 1000.0,
            timings.normalize.as_secs_f64() * 1000.0,
            total_started
                .map(|started| started.elapsed().as_secs_f64() * 1000.0)
                .unwrap_or_default()
        );
    }
    Ok(ResidualRefreshTraceInternal {
        samples,
        residual_image: image,
        normalization_sumwt: psf_state.normalization_sumwt,
        reported_sumwt: psf_state.reported_sumwt,
        psf_peak: psf_state.psf_peak,
        gridded_samples: psf_state.gridded_samples,
        skipped_samples: psf_state.skipped_samples,
    })
}

#[allow(clippy::too_many_arguments)]
fn compute_residual_trace_cube_standard(
    batches: &[VisibilityBatch],
    model_interpolation_batches: &[CubeModelInterpolationBatch],
    gridder: &StandardGridder,
    model_planes: &[Array2<f32>],
    output_channel_frequency_hz: f64,
    model_channel_frequencies_hz: &[f64],
    prediction_lambda_mode: CubePredictionLambdaMode,
    psf_state: &PsfState,
    stage_timings: &mut ImagingStageTimings,
) -> Result<ResidualRefreshTraceInternal, ImagingError> {
    let mut timings = ResidualComputationTimings::default();
    let model_grids = build_cube_model_grids(gridder, model_planes.iter(), &mut timings);
    let planned_batches = build_standard_residual_sample_plans(gridder, batches);
    let trace = compute_residual_trace_cube_standard_with_model_grids(
        batches,
        Some(&planned_batches),
        model_interpolation_batches,
        gridder,
        &model_grids,
        output_channel_frequency_hz,
        model_channel_frequencies_hz,
        prediction_lambda_mode,
        psf_state,
        true,
        &mut timings,
    )?;
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    Ok(trace)
}

fn build_cube_model_grids<'a, I>(
    gridder: &StandardGridder,
    model_planes: I,
    timings: &mut ResidualComputationTimings,
) -> Vec<Option<Array2<Complex32>>>
where
    I: IntoIterator<Item = &'a Array2<f32>>,
{
    let model_planes = model_planes.into_iter();
    let (lower_bound, _) = model_planes.size_hint();
    let mut model_grids = Vec::with_capacity(lower_bound);
    for model_plane in model_planes {
        if model_plane.iter().any(|value| value.abs() > 0.0) {
            let model_fft_started = Instant::now();
            let transformed = centered_fft2(&gridder.apodize_model(model_plane));
            timings.model_fft += model_fft_started.elapsed();
            model_grids.push(Some(transformed));
        } else {
            model_grids.push(None);
        }
    }
    model_grids
}

#[allow(clippy::too_many_arguments)]
fn compute_residual_trace_cube_standard_with_model_grids(
    batches: &[VisibilityBatch],
    planned_batches: Option<&[Vec<Option<PlannedSample>>]>,
    model_interpolation_batches: &[CubeModelInterpolationBatch],
    gridder: &StandardGridder,
    model_grids: &[Option<Array2<Complex32>>],
    output_channel_frequency_hz: f64,
    model_channel_frequencies_hz: &[f64],
    prediction_lambda_mode: CubePredictionLambdaMode,
    psf_state: &PsfState,
    capture_samples: bool,
    timings: &mut ResidualComputationTimings,
) -> Result<ResidualRefreshTraceInternal, ImagingError> {
    if model_interpolation_batches.len() != batches.len() {
        return Err(ImagingError::InvalidRequest(format!(
            "cube model interpolation batch count {} does not match visibility batch count {}",
            model_interpolation_batches.len(),
            batches.len()
        )));
    }
    if let Some(plans) = planned_batches
        && plans.len() != batches.len()
    {
        return Err(ImagingError::InvalidRequest(format!(
            "planned batch count {} does not match visibility batch count {}",
            plans.len(),
            batches.len()
        )));
    }
    let [nx, ny] = gridder.grid_shape();
    let mut residual_grid = Array2::<Complex64>::zeros((nx, ny));
    let mut samples = if capture_samples {
        Vec::with_capacity(batches.iter().map(VisibilityBatch::len).sum())
    } else {
        Vec::new()
    };
    let degrid_grid_started = Instant::now();
    for (batch_index, (batch, interpolation_batch)) in batches
        .iter()
        .zip(model_interpolation_batches.iter())
        .enumerate()
    {
        if interpolation_batch.sample_contributions.len() != batch.len() {
            return Err(ImagingError::InvalidRequest(format!(
                "cube model interpolation batch {batch_index} length {} does not match visibility batch length {}",
                interpolation_batch.sample_contributions.len(),
                batch.len()
            )));
        }
        for index in 0..batch.len() {
            let weight = batch.weight[index];
            let observed_visibility = batch.visibility[index];
            let gridable = batch.gridable[index];
            let planned_sample = planned_batches
                .and_then(|plans| {
                    plans
                        .get(batch_index)
                        .and_then(|batch_plans| batch_plans.get(index))
                })
                .copied()
                .flatten()
                .or_else(|| {
                    if gridable
                        && weight.is_finite()
                        && weight > 0.0
                        && observed_visibility.re.is_finite()
                        && observed_visibility.im.is_finite()
                    {
                        gridder.plan_sample(batch.u_lambda[index], batch.v_lambda[index])
                    } else {
                        None
                    }
                });
            let predicted_visibility = if let Some(plan) = planned_sample.as_ref() {
                let mut predicted = Complex32::new(0.0, 0.0);
                for contribution in &interpolation_batch.sample_contributions[index] {
                    if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
                        continue;
                    }
                    let Some(model_grid) = model_grids.get(contribution.model_channel_index) else {
                        return Err(ImagingError::InvalidRequest(format!(
                            "cube model interpolation references channel {} beyond {} model planes",
                            contribution.model_channel_index,
                            model_grids.len()
                        )));
                    };
                    if let Some(model_grid) = model_grid.as_ref() {
                        let contribution_prediction = match prediction_lambda_mode {
                            CubePredictionLambdaMode::OutputChannel => gridder
                                .degrid_sample_product_planned_normalized(
                                    model_grid,
                                    &plan.positive,
                                ),
                            CubePredictionLambdaMode::ModelChannel => {
                                let Some(&model_frequency_hz) = model_channel_frequencies_hz
                                    .get(contribution.model_channel_index)
                                else {
                                    return Err(ImagingError::InvalidRequest(format!(
                                        "cube model interpolation references model frequency for channel {} beyond {} channels",
                                        contribution.model_channel_index,
                                        model_channel_frequencies_hz.len()
                                    )));
                                };
                                if !(output_channel_frequency_hz.is_finite()
                                    && output_channel_frequency_hz > 0.0
                                    && model_frequency_hz.is_finite()
                                    && model_frequency_hz > 0.0)
                                {
                                    continue;
                                }
                                let uv_scale = model_frequency_hz / output_channel_frequency_hz;
                                let Some(model_plan) = gridder.plan_sample(
                                    batch.u_lambda[index] * uv_scale,
                                    batch.v_lambda[index] * uv_scale,
                                ) else {
                                    continue;
                                };
                                gridder.degrid_sample_product_planned_normalized(
                                    model_grid,
                                    &model_plan.positive,
                                )
                            }
                        };
                        predicted += contribution_prediction * contribution.factor;
                    }
                }
                predicted
            } else {
                Complex32::new(0.0, 0.0)
            };
            let residual_visibility = observed_visibility - predicted_visibility;
            if capture_samples {
                samples.push(ResidualSampleTraceInternal {
                    batch_index,
                    sample_index: index,
                    u_lambda: batch.u_lambda[index],
                    v_lambda: batch.v_lambda[index],
                    w_lambda: batch.w_lambda[index],
                    observed_visibility,
                    predicted_visibility,
                    residual_visibility,
                    weight,
                    gridable,
                });
            }
            let Some(plan) = planned_sample.as_ref() else {
                continue;
            };
            let sumwt_factor = batch.sumwt_factor[index];
            if !(sumwt_factor.is_finite() && sumwt_factor > 0.0) {
                continue;
            }
            let residual_weight = f64::from(weight * sumwt_factor);
            let residual = Complex64::new(
                f64::from(residual_visibility.re) * residual_weight,
                f64::from(residual_visibility.im) * residual_weight,
            );
            gridder.grid_sample_product_planned_f64(&mut residual_grid, &plan.positive, residual);
        }
    }
    timings.degrid_grid = degrid_grid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2_f64(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image = gridder.corrected_image_from_grid_f64(&raw);
    image.mapv_inplace(|value| value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    Ok(ResidualRefreshTraceInternal {
        samples,
        residual_image: image,
        normalization_sumwt: psf_state.normalization_sumwt,
        reported_sumwt: psf_state.reported_sumwt,
        psf_peak: psf_state.psf_peak,
        gridded_samples: psf_state.gridded_samples,
        skipped_samples: psf_state.skipped_samples,
    })
}

#[derive(Debug, Clone, Copy)]
struct RawWProjectSample {
    batch_index: usize,
    sample_index: usize,
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
    weight: f32,
    visibility: Complex32,
    sumwt_factor: f32,
}

#[derive(Debug, Clone, Copy)]
struct WProjectSkippedSample {
    batch_index: usize,
    sample_index: usize,
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
    weight: f32,
    sumwt_factor: f32,
    reason: WProjectSkipReason,
}

#[derive(Debug, Clone, Copy)]
struct WProjectPreparedSample {
    batch_index: usize,
    sample_index: usize,
    u_lambda: f64,
    v_lambda: f64,
    w_lambda: f64,
    sumwt_factor: f32,
    positive_plan: WProjectSamplePlan,
    weight: f32,
    visibility: Complex32,
}

struct WProjectPreparedData {
    requested_plane_count: Option<usize>,
    max_abs_w_lambda: f64,
    projector: WProjector,
    samples: Vec<WProjectPreparedSample>,
    skipped_samples: Vec<WProjectSkippedSample>,
    normalization_sumwt: f32,
    reported_sumwt: f32,
    gridded_samples: usize,
}

fn compute_psf_w_project(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    w_project_planes: Option<usize>,
    stage_timings: &mut ImagingStageTimings,
) -> Result<PsfState, ImagingError> {
    let prepare_started = Instant::now();
    let prepared = prepare_w_project_data(geometry, batches, gridder, w_project_planes)?;
    let mut timings = PsfComputationTimings {
        grid: prepare_started.elapsed(),
        ..PsfComputationTimings::default()
    };
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let mut psf_grid = Array2::<Complex32>::zeros((grid_nx, grid_ny));

    let grid_started = Instant::now();
    for sample in &prepared.samples {
        let psf_weight = Complex32::new(sample.weight, 0.0);
        prepared
            .projector
            .grid_sample_planned(&mut psf_grid, &sample.positive_plan, psf_weight);
    }
    timings.grid += grid_started.elapsed();

    if prepared.normalization_sumwt <= 0.0 || prepared.reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    let fft_started = Instant::now();
    let raw_psf = centered_ifft2(&psf_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut psf =
        gridder.corrected_w_project_image_from_grid(&raw_psf, prepared.projector.sampling());
    psf.mapv_inplace(|value| 2.0 * value / prepared.normalization_sumwt);
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
        normalization_sumwt: prepared.normalization_sumwt,
        reported_sumwt: prepared.reported_sumwt,
        psf_peak,
        gridded_samples: prepared.gridded_samples,
        skipped_samples: prepared.skipped_samples.len(),
    })
}

fn compute_residual_w_project(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    model: &Array2<f32>,
    psf_state: &PsfState,
    w_project_planes: Option<usize>,
    stage_timings: &mut ImagingStageTimings,
) -> Result<Array2<f32>, ImagingError> {
    let prepare_started = Instant::now();
    let prepared = prepare_w_project_data(geometry, batches, gridder, w_project_planes)?;
    let mut timings = ResidualComputationTimings {
        degrid_grid: prepare_started.elapsed(),
        ..ResidualComputationTimings::default()
    };
    let model_nonzero = model.iter().any(|value| value.abs() > 0.0);
    let [grid_nx, grid_ny] = gridder.grid_shape();
    let model_grid = if model_nonzero {
        let model_fft_started = Instant::now();
        let transformed =
            centered_fft2(&gridder.apodize_w_project_model(model, prepared.projector.sampling()));
        timings.model_fft = model_fft_started.elapsed();
        Some(transformed)
    } else {
        None
    };

    let degrid_started = Instant::now();
    let mut residual_grid = Array2::<Complex32>::zeros((grid_nx, grid_ny));
    for sample in &prepared.samples {
        let predicted = model_grid.as_ref().map_or_else(
            || Complex32::new(0.0, 0.0),
            |grid| {
                prepared
                    .projector
                    .degrid_sample_planned(grid, &sample.positive_plan)
            },
        );
        let residual = (sample.visibility - predicted) * sample.weight;
        prepared
            .projector
            .grid_sample_planned(&mut residual_grid, &sample.positive_plan, residual);
    }
    timings.degrid_grid += degrid_started.elapsed();

    let fft_started = Instant::now();
    let raw = centered_ifft2(&residual_grid);
    timings.fft = fft_started.elapsed();
    let normalize_started = Instant::now();
    let mut image =
        gridder.corrected_w_project_image_from_grid(&raw, prepared.projector.sampling());
    image.mapv_inplace(|value| 2.0 * value / psf_state.normalization_sumwt / psf_state.psf_peak);
    timings.normalize = normalize_started.elapsed();
    stage_timings.model_fft += timings.model_fft;
    stage_timings.residual_degrid_grid += timings.degrid_grid;
    stage_timings.residual_fft += timings.fft;
    stage_timings.residual_normalize += timings.normalize;
    Ok(image)
}

fn prepare_w_project_data(
    geometry: ImageGeometry,
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    w_project_planes: Option<usize>,
) -> Result<WProjectPreparedData, ImagingError> {
    let (raw_samples, mut skipped_samples, max_abs_w_lambda) =
        collect_w_project_raw_samples(batches);
    if raw_samples.is_empty() {
        return Err(ImagingError::NoUsableSamples);
    }
    let projector = WProjector::new(geometry, gridder, max_abs_w_lambda, w_project_planes)?;
    let mut samples = Vec::with_capacity(raw_samples.len());
    let mut normalization_sumwt = 0.0f64;
    let mut reported_sumwt = 0.0f64;
    let mut gridded_samples = 0usize;

    for sample in raw_samples {
        let Some(positive_plan) =
            projector.plan_sample(sample.u_lambda, sample.v_lambda, sample.w_lambda)
        else {
            skipped_samples.push(WProjectSkippedSample {
                batch_index: sample.batch_index,
                sample_index: sample.sample_index,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                weight: sample.weight,
                sumwt_factor: sample.sumwt_factor,
                reason: WProjectSkipReason::OutsideGrid,
            });
            continue;
        };
        normalization_sumwt +=
            2.0 * f64::from(sample.weight) * f64::from(positive_plan.normalization);
        reported_sumwt += f64::from(sample.weight) * f64::from(sample.sumwt_factor);
        gridded_samples += 1;
        samples.push(WProjectPreparedSample {
            batch_index: sample.batch_index,
            sample_index: sample.sample_index,
            u_lambda: sample.u_lambda,
            v_lambda: sample.v_lambda,
            w_lambda: sample.w_lambda,
            sumwt_factor: sample.sumwt_factor,
            positive_plan,
            weight: sample.weight,
            visibility: sample.visibility,
        });
    }

    if samples.is_empty() || normalization_sumwt <= 0.0 || reported_sumwt <= 0.0 {
        return Err(ImagingError::NoUsableSamples);
    }

    skipped_samples.sort_by_key(|sample| (sample.batch_index, sample.sample_index));

    Ok(WProjectPreparedData {
        requested_plane_count: w_project_planes,
        max_abs_w_lambda,
        projector,
        samples,
        skipped_samples,
        normalization_sumwt: normalization_sumwt as f32,
        reported_sumwt: reported_sumwt as f32,
        gridded_samples,
    })
}

fn collect_w_project_raw_samples(
    batches: &[VisibilityBatch],
) -> (Vec<RawWProjectSample>, Vec<WProjectSkippedSample>, f64) {
    let mut raw_samples = Vec::<RawWProjectSample>::new();
    let mut skipped_samples = Vec::<WProjectSkippedSample>::new();
    let mut max_abs_w_lambda = 0.0f64;

    for (batch_index, batch) in batches.iter().enumerate() {
        for sample_index in 0..batch.len() {
            let sample = RawWProjectSample {
                batch_index,
                sample_index,
                u_lambda: batch.u_lambda[sample_index],
                v_lambda: batch.v_lambda[sample_index],
                w_lambda: batch.w_lambda[sample_index],
                weight: batch.weight[sample_index],
                visibility: batch.visibility[sample_index],
                sumwt_factor: batch.sumwt_factor[sample_index],
            };
            if !batch.gridable[sample_index] {
                skipped_samples.push(WProjectSkippedSample {
                    batch_index,
                    sample_index,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    weight: sample.weight,
                    sumwt_factor: sample.sumwt_factor,
                    reason: WProjectSkipReason::NotGridable,
                });
                continue;
            }
            if !(sample.weight.is_finite()
                && sample.weight > 0.0
                && sample.sumwt_factor.is_finite()
                && sample.sumwt_factor > 0.0
                && sample.visibility.re.is_finite()
                && sample.visibility.im.is_finite()
                && sample.u_lambda.is_finite()
                && sample.v_lambda.is_finite()
                && sample.w_lambda.is_finite())
            {
                skipped_samples.push(WProjectSkippedSample {
                    batch_index,
                    sample_index,
                    u_lambda: sample.u_lambda,
                    v_lambda: sample.v_lambda,
                    w_lambda: sample.w_lambda,
                    weight: sample.weight,
                    sumwt_factor: sample.sumwt_factor,
                    reason: WProjectSkipReason::InvalidInput,
                });
                continue;
            }
            max_abs_w_lambda = max_abs_w_lambda.max(sample.w_lambda.abs());
            raw_samples.push(sample);
        }
    }

    (raw_samples, skipped_samples, max_abs_w_lambda)
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
    let mut normalization_sumwt = 0.0f64;
    let mut reported_sumwt = 0.0f64;
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
            normalization_sumwt += 2.0 * f64::from(weight);
            reported_sumwt += f64::from(weight) * f64::from(sumwt_factor);
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
    psf.mapv_inplace(|value| value / normalization_sumwt as f32);
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
        normalization_sumwt: normalization_sumwt as f32,
        reported_sumwt: reported_sumwt as f32,
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
    peak_location_masked_with_relative_tolerance(image, mask, 0.0)
}

fn hclean_peak_location_masked(
    image: &Array2<f32>,
    mask: Option<&Array2<bool>>,
) -> Option<((usize, usize), f32)> {
    peak_location_masked_with_relative_tolerance(image, mask, 0.0)
}

fn peak_location_masked_with_relative_tolerance(
    image: &Array2<f32>,
    mask: Option<&Array2<bool>>,
    relative_tolerance: f32,
) -> Option<((usize, usize), f32)> {
    let (nx, ny) = image.dim();
    let mut best = None;
    // Match casacore's `hclean` search order: y-major, with an optional
    // tolerance to keep near-tied peaks stable against gridding roundoff.
    for y in 0..ny {
        for x in 0..nx {
            if mask.is_some_and(|current| !current[(x, y)]) {
                continue;
            }
            let value = image[(x, y)];
            match best {
                None => best = Some(((x, y), value)),
                Some((_, best_value))
                    if value.abs() > best_value.abs() * (1.0 + relative_tolerance) =>
                {
                    best = Some(((x, y), value));
                }
                _ => {}
            }
        }
    }
    best
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

fn global_nsigma_threshold_jy_per_beam(nsigma_thresholds_jy_per_beam: &[f32]) -> f32 {
    nsigma_thresholds_jy_per_beam
        .iter()
        .copied()
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
    let tolerance_jy_per_beam = (0.01 * threshold_jy_per_beam).max(2.0e-8);
    peak_abs_jy_per_beam <= threshold_jy_per_beam
        || (peak_abs_jy_per_beam - threshold_jy_per_beam).abs() <= tolerance_jy_per_beam
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
    let threshold_floor_active = threshold_jy_per_beam > 0.0
        && (0.01 * threshold_jy_per_beam) < 2.0e-8
        && cycle_threshold_jy_per_beam <= threshold_jy_per_beam;
    if peak_abs_jy_per_beam < threshold_jy_per_beam
        || (threshold_floor_active
            && threshold_reached_with_tolerance(peak_abs_jy_per_beam, threshold_jy_per_beam))
    {
        Some(CleanStopReason::GlobalThresholdReached)
    } else if nsigma_threshold_jy_per_beam > threshold_jy_per_beam
        && peak_abs_jy_per_beam < nsigma_threshold_jy_per_beam
    {
        Some(CleanStopReason::NsigmaThresholdReached)
    } else if peak_abs_jy_per_beam < cycle_threshold_jy_per_beam {
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
        major_cycle_limit: None,
        gain: 0.1,
        threshold_jy_per_beam: 0.0,
        nsigma: 0.0,
        psf_cutoff,
        minor_cycle_length: 1,
        cyclefactor: 1.0,
        min_psf_fraction: 0.05,
        max_psf_fraction: 0.8,
        hogbom_iteration_mode: HogbomIterationMode::Strict,
    }
}

fn add_stage_timings(total: &mut ImagingStageTimings, part: ImagingStageTimings) {
    total.controller_overhead += part.controller_overhead;
    total.weighting += part.weighting;
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
    use casa_test_support::gridder_interop::{
        cpp_convolve_gridder_make_model_residual_image_2d,
        cpp_convolve_gridder_predict_visibility_2d,
    };
    use casa_test_support::hogbom_interop::cpp_hogbom_clean_minor_cycle_2d;
    use ndarray::{Array2, Array4, s};
    use num_complex::Complex32;
    use serial_test::serial;

    use super::{
        CleanConfig, CleanStopReason, CompatibilityMode, CubeChannelRequest, CubeImagingRequest,
        CubeModelChannelContribution, CubeModelInterpolationBatch, Deconvolver, GridderMode,
        HogbomIterationMode, ImageGeometry, ImagingRequest, ImagingStageTimings,
        MosaicGridderConfig, MtmfsRequest, ParallelHandBatch, PlaneStokes, PrimaryBeamModel,
        PsfState, RestoringBeamMode, StandardGridder, StandardMfsBackendSelection,
        StandardMfsDirtyAccumulator, StandardMfsDirtyAccumulatorRequest,
        StandardMfsExecutionConfig, StandardMfsModelPredictor, StandardMfsPlannedSampleBuilder,
        StandardMfsPlannedWeightedSample, StandardMfsWeightedSample, VisibilityBatch,
        VisibilityMetadataBatch, WProjectSkipReason, WTermMode, WeightDensityMode, WeightingMode,
        add_shifted_kernel, apply_chauvenet_clipping, apply_weighting, build_direct_components,
        build_direct_pixel_coordinates, build_multiscale_scale_masks, compute_cycle_threshold,
        compute_dirty_psf_and_residual_standard, compute_dirty_psf_and_residual_standard_metal,
        compute_psf, compute_psf_direct, compute_residual, compute_residual_direct,
        direct_predict_visibility, dirty_clean_config, make_multiscale_kernel, mean_stddev,
        minor_cycle_stop_reason, mosaic_pointing_contributes_by_simple_pb_center,
        mosaic_pointing_pixel_inside_image, mosaic_projector_sampling,
        parse_standard_mfs_backend_selection, parse_standard_mfs_thread_count, peak_abs_value,
        peak_location_masked, run_cube, run_dirty_cube, run_hogbom_minor_cycle, run_imaging,
        run_imaging_owned, run_mtmfs,
        run_standard_mfs_planned_sample_block_streaming_with_execution_config,
        run_standard_mfs_weighted_sample_block_streaming_with_execution_config,
        run_standard_mfs_weighted_sample_streaming_with_execution_config,
        run_standard_mfs_weighted_streaming_with_execution_config, tolerant_clean_stop_reason,
        trace_cube_channel_residual_refresh,
        trace_cube_channel_residual_refresh_model_channel_lambda,
        trace_cube_channel_w_project_plan, trace_cube_weighting, trace_residual_refresh,
        trace_w_project_plan, trace_weighting,
    };

    #[test]
    fn standard_mfs_thread_count_parser_accepts_numeric_and_auto_values() {
        assert_eq!(parse_standard_mfs_thread_count("1"), Some(1));
        assert_eq!(parse_standard_mfs_thread_count("10"), Some(10));
        assert_eq!(parse_standard_mfs_thread_count(" 4 "), Some(4));
        assert_eq!(parse_standard_mfs_thread_count("0"), None);
        assert_eq!(parse_standard_mfs_thread_count("not-a-count"), None);
        assert!(parse_standard_mfs_thread_count("auto").is_some_and(|value| value >= 1));
        assert!(parse_standard_mfs_thread_count("AUTO").is_some_and(|value| value >= 1));
    }

    #[test]
    fn standard_mfs_metal_backend_selection_is_explicit_and_gated() {
        assert_eq!(
            parse_standard_mfs_backend_selection("metal").unwrap(),
            StandardMfsBackendSelection::Metal
        );
        assert_eq!(
            parse_standard_mfs_backend_selection("fixed_tile").unwrap(),
            StandardMfsBackendSelection::FixedTile
        );
        assert!(
            parse_standard_mfs_backend_selection("not-a-backend")
                .unwrap_err()
                .to_string()
                .contains("expected cpu, fixed_tile, or metal")
        );

        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![point_source_visibilities(
                &[(0.0, 0.0, 0.0)],
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (16.0, 16.0),
                1.0,
            )],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.4e9, 1.4e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 0,
                ..CleanConfig::default()
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        #[cfg(target_os = "macos")]
        {
            let gridder = StandardGridder::new(geometry).unwrap();
            let weighted_batches = apply_weighting(&request, &gridder).unwrap();
            let mut cpu_timings = ImagingStageTimings::default();
            let (cpu_psf, cpu_residual) = compute_dirty_psf_and_residual_standard(
                &weighted_batches,
                &gridder,
                &mut cpu_timings,
            )
            .unwrap();
            let mut metal_timings = ImagingStageTimings::default();
            let metal_result = compute_dirty_psf_and_residual_standard_metal(
                &weighted_batches,
                &gridder,
                StandardMfsExecutionConfig::default(),
                &mut metal_timings,
            );
            let (metal_psf, metal_residual) = match metal_result {
                Ok(result) => result,
                Err(error)
                    if error
                        .to_string()
                        .contains("could not find a default Metal device") =>
                {
                    return;
                }
                Err(error) => panic!("{error}"),
            };
            let max_psf_delta = metal_psf
                .psf
                .iter()
                .zip(cpu_psf.psf.iter())
                .map(|(lhs, rhs)| (lhs - rhs).abs())
                .fold(0.0f32, f32::max);
            let max_residual_delta = metal_residual
                .iter()
                .zip(cpu_residual.iter())
                .map(|(lhs, rhs)| (lhs - rhs).abs())
                .fold(0.0f32, f32::max);
            assert!(max_psf_delta < 1.0e-4, "max PSF delta {max_psf_delta}");
            assert!(
                max_residual_delta < 1.0e-4,
                "max residual delta {max_residual_delta}"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = request;
        }
    }

    #[test]
    fn mosaic_projector_sampling_matches_casa_hetarray_default() {
        let line_probe_geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [
                0.13_f64.to_radians() / 3600.0,
                0.13_f64.to_radians() / 3600.0,
            ],
        };
        let continuum_geometry = ImageGeometry {
            image_shape: [750, 750],
            cell_size_rad: [
                0.13_f64.to_radians() / 3600.0,
                0.13_f64.to_radians() / 3600.0,
            ],
        };

        assert_eq!(mosaic_projector_sampling(line_probe_geometry), 10);
        assert_eq!(mosaic_projector_sampling(continuum_geometry), 10);
    }

    #[test]
    fn alma_aca_airy_voltage_uses_casa_common_pb_radius() {
        let inside_casa_support = (60.0_f64 / 3600.0).to_radians();
        let outside_casa_support = (70.0_f64 / 3600.0).to_radians();

        assert_ne!(
            super::primary_beam_voltage_pattern(
                PrimaryBeamModel::Airy {
                    dish_diameter_m: 10.7,
                    blockage_diameter_m: 0.75,
                },
                inside_casa_support,
                100.0e9,
            ),
            0.0
        );
        assert_ne!(
            super::primary_beam_voltage_pattern(
                PrimaryBeamModel::Airy {
                    dish_diameter_m: 6.25,
                    blockage_diameter_m: 0.75,
                },
                inside_casa_support,
                100.0e9,
            ),
            0.0
        );
        assert_eq!(
            super::primary_beam_voltage_pattern(
                PrimaryBeamModel::Airy {
                    dish_diameter_m: 10.7,
                    blockage_diameter_m: 0.75,
                },
                outside_casa_support,
                100.0e9,
            ),
            0.0
        );
        assert_eq!(
            super::primary_beam_voltage_pattern(
                PrimaryBeamModel::Airy {
                    dish_diameter_m: 6.25,
                    blockage_diameter_m: 0.75,
                },
                outside_casa_support,
                100.0e9,
            ),
            0.0
        );
    }

    #[test]
    fn evla_primary_beam_uses_casa_sampled_radius_lookup() {
        let frequency_hz = 1.578_964_191_647_556_8e9_f64;
        let sample_index = 2_210.0_f64;
        let sampled_radius_arcmin_ghz = sample_index / (9_999.0_f64 / 58.0);
        let radius_arcmin_ghz = sampled_radius_arcmin_ghz + 0.75 / (9_999.0_f64 / 58.0);
        let radius_rad = (radius_arcmin_ghz / 60.0_f64 / (frequency_hz / 1.0e9_f64)).to_radians();
        let x2 = sampled_radius_arcmin_ghz * sampled_radius_arcmin_ghz;
        let expected_power: f64 = 1.0 - 1.435e-3 * x2 + 7.54e-7 * x2 * x2 - 1.49e-10 * x2 * x2 * x2;
        let expected_voltage = expected_power.sqrt() as f32;

        let actual = super::primary_beam_voltage_pattern(
            PrimaryBeamModel::EvlaLBandCommon,
            radius_rad,
            frequency_hz,
        );

        assert!((actual - expected_voltage).abs() < 1.0e-7);
    }

    #[test]
    fn mosaic_pointing_contribution_follows_casa_simple_pb_center_pixel_rule() {
        let geometry = ImageGeometry {
            image_shape: [100, 80],
            cell_size_rad: [1.0e-6, 1.0e-6],
        };

        assert!(mosaic_pointing_pixel_inside_image(geometry, [0.0, 0.0]));
        assert!(mosaic_pointing_pixel_inside_image(
            geometry,
            [99.999, 79.999]
        ));
        assert!(!mosaic_pointing_pixel_inside_image(
            geometry,
            [-0.001, 40.0]
        ));
        assert!(!mosaic_pointing_pixel_inside_image(geometry, [100.0, 40.0]));
        assert!(!mosaic_pointing_pixel_inside_image(geometry, [50.0, 80.0]));

        let phase_center = [1.0, 0.5];
        assert!(mosaic_pointing_contributes_by_simple_pb_center(
            geometry,
            phase_center,
            phase_center
        ));
        assert!(!mosaic_pointing_contributes_by_simple_pb_center(
            geometry,
            phase_center,
            [phase_center[0] + 2.0, phase_center[1]]
        ));

        let papersky_geometry = ImageGeometry {
            image_shape: [128, 128],
            cell_size_rad: [8.0_f64.to_radians() / 3600.0, 8.0_f64.to_radians() / 3600.0],
        };
        let papersky_phase_center = [5.224970365079775, 0.7022114079242685];
        let north_neighbor = [5.224970365079775, 0.7065747310542544];
        let east_neighbor = [5.229333688209761, 0.7022114079242685];
        assert!(mosaic_pointing_contributes_by_simple_pb_center(
            papersky_geometry,
            papersky_phase_center,
            papersky_phase_center
        ));
        assert!(!mosaic_pointing_contributes_by_simple_pb_center(
            papersky_geometry,
            papersky_phase_center,
            north_neighbor
        ));
        assert!(!mosaic_pointing_contributes_by_simple_pb_center(
            papersky_geometry,
            papersky_phase_center,
            east_neighbor
        ));
    }

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

    fn assert_standard_residual_trace_matches_casacore(
        label: &str,
        request: &ImagingRequest,
        gridder: &StandardGridder,
        model: &Array2<f32>,
        expected_samples: usize,
    ) -> (f32, f32) {
        let trace = trace_residual_refresh(request, model).unwrap();
        assert_eq!(trace.samples.len(), expected_samples);
        let pixels = build_direct_pixel_coordinates(request.geometry);
        let components = build_direct_components(model, &pixels, request.geometry.image_shape[1]);
        let mut max_trace_casacore_delta = 0.0f32;
        let mut max_trace_direct_delta = 0.0f32;
        for trace_sample in &trace.samples {
            let cpp = match cpp_convolve_gridder_predict_visibility_2d(
                gridder.grid_shape(),
                request.geometry.image_shape,
                [
                    gridder.grid_shape()[0] as f64 * request.geometry.cell_size_rad[0],
                    gridder.grid_shape()[1] as f64 * request.geometry.cell_size_rad[1],
                ],
                [
                    gridder.grid_shape()[0] as f64 / 2.0,
                    gridder.grid_shape()[1] as f64 / 2.0,
                ],
                [trace_sample.u_lambda, -trace_sample.v_lambda],
                model.as_slice().unwrap(),
            ) {
                Ok(result) => result,
                Err(error) if error == "casacore C++ backend unavailable" => return (0.0, 0.0),
                Err(error) => panic!("run predict-visibility interop: {error}"),
            };
            let cpp_value = Complex32::new(cpp.re, cpp.im);
            let direct = direct_predict_visibility(
                &components,
                trace_sample.u_lambda,
                trace_sample.v_lambda,
                trace_sample.w_lambda,
            );
            max_trace_casacore_delta = max_trace_casacore_delta
                .max((trace_sample.predicted_visibility - cpp_value).norm());
            max_trace_direct_delta =
                max_trace_direct_delta.max((trace_sample.predicted_visibility - direct).norm());
        }
        assert!(
            max_trace_casacore_delta < 1.0e-5,
            "standard MFS residual-refresh predictions should match casacore per visibility for {label}: max_casacore_delta={max_trace_casacore_delta} max_direct_delta={max_trace_direct_delta}"
        );
        (max_trace_casacore_delta, max_trace_direct_delta)
    }

    fn assert_close_f32(actual: f32, expected: f32, tol: f32) {
        let delta = (actual - expected).abs();
        assert!(
            delta <= tol,
            "expected {expected}, got {actual}, delta={delta}, tol={tol}"
        );
    }

    fn identity_cube_model_interpolation_batches(
        model_channel_index: usize,
        visibility_batches: &[VisibilityBatch],
    ) -> Vec<CubeModelInterpolationBatch> {
        visibility_batches
            .iter()
            .map(|batch| CubeModelInterpolationBatch {
                sample_contributions: (0..batch.len())
                    .map(|_| {
                        vec![CubeModelChannelContribution {
                            model_channel_index,
                            factor: 1.0,
                        }]
                    })
                    .collect(),
            })
            .collect()
    }

    fn cube_channel_request_identity(
        channel_frequency_hz: f64,
        visibility_batches: Vec<VisibilityBatch>,
        model_channel_index: usize,
    ) -> CubeChannelRequest {
        let model_interpolation_batches =
            identity_cube_model_interpolation_batches(model_channel_index, &visibility_batches);
        CubeChannelRequest {
            channel_frequency_hz,
            visibility_batches,
            density_batches: Vec::new(),
            model_interpolation_batches,
        }
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

    fn centered_mosaic_metadata(
        batch: &VisibilityBatch,
        frequency_hz: f64,
    ) -> VisibilityMetadataBatch {
        VisibilityMetadataBatch {
            sample_frequency_hz: vec![frequency_hz; batch.len()],
            beam_frequency_hz: vec![frequency_hz; batch.len()],
            primary_beam_model: PrimaryBeamModel::Airy {
                dish_diameter_m: 25.0,
                blockage_diameter_m: 1.0,
            },
            pointing_direction_rad: vec![[0.0, 0.0]; batch.len()],
        }
    }

    fn assert_array4_close(actual: &Array4<f32>, expected: &Array4<f32>, tolerance: f32) {
        assert_eq!(actual.dim(), expected.dim());
        let max_abs = actual
            .iter()
            .zip(expected.iter())
            .map(|(actual, expected)| (actual - expected).abs())
            .fold(0.0f32, f32::max);
        assert!(
            max_abs <= tolerance,
            "max abs difference {max_abs:.6e} exceeded tolerance {tolerance:.6e}"
        );
    }

    #[test]
    fn owned_standard_mfs_briggs_clean_matches_borrowed_run() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let samples = [
            (-95.0, -35.0, 0.0),
            (-70.0, 40.0, 0.0),
            (-35.0, -80.0, 0.0),
            (22.0, 75.0, 0.0),
            (55.0, -25.0, 0.0),
            (105.0, 50.0, 0.0),
        ];
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (34.0, 30.0),
                1.0,
            )],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Briggs { robust: 0.5 },
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 4,
                gain: 0.2,
                minor_cycle_length: 2,
                ..CleanConfig::default()
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        let borrowed = run_imaging(&request).unwrap();
        let owned = run_imaging_owned(request).unwrap();

        assert_eq!(owned.psf, borrowed.psf);
        assert_eq!(owned.residual, borrowed.residual);
        assert_eq!(owned.model, borrowed.model);
        assert_eq!(owned.image, borrowed.image);
        assert_eq!(owned.sumwt, borrowed.sumwt);
        assert_eq!(
            owned.diagnostics.minor_iterations,
            borrowed.diagnostics.minor_iterations
        );
        assert_eq!(
            owned.diagnostics.clean_stop_reason,
            borrowed.diagnostics.clean_stop_reason
        );
    }

    #[test]
    fn streaming_weighted_standard_mfs_clean_matches_retained_batches() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let samples = [
            (-95.0, -35.0, 0.0),
            (-70.0, 40.0, 0.0),
            (-35.0, -80.0, 0.0),
            (22.0, 75.0, 0.0),
            (55.0, -25.0, 0.0),
            (105.0, 50.0, 0.0),
        ];
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (34.0, 30.0),
                1.0,
            )],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Briggs { robust: 0.5 },
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 4,
                gain: 0.2,
                minor_cycle_length: 2,
                ..CleanConfig::default()
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let weighted_batches = apply_weighting(&request, &gridder).unwrap();
        let retained = run_imaging(&request).unwrap();
        let mut streaming_request = request.clone();
        streaming_request.visibility_batches.clear();
        let streaming = run_standard_mfs_weighted_streaming_with_execution_config(
            streaming_request,
            StandardMfsExecutionConfig {
                fixed_tile_resident_bytes: Some(usize::MAX),
                fixed_tile_edge: Some(16),
                fixed_tile_center_boundary: false,
                fixed_tile_max_live_row_blocks: 1,
            },
            |consumer| {
                for batch in &weighted_batches {
                    consumer(vec![batch.clone()])?;
                }
                Ok(())
            },
        )
        .unwrap();

        assert_array4_close(&streaming.psf, &retained.psf, 1.0e-5);
        assert_array4_close(&streaming.residual, &retained.residual, 1.0e-5);
        assert_array4_close(&streaming.model, &retained.model, 1.0e-5);
        assert_array4_close(&streaming.image, &retained.image, 1.0e-5);
        assert_eq!(
            streaming.diagnostics.minor_iterations,
            retained.diagnostics.minor_iterations
        );
        assert_eq!(
            streaming.diagnostics.clean_stop_reason,
            retained.diagnostics.clean_stop_reason
        );
    }

    #[test]
    fn sample_streaming_weighted_standard_mfs_clean_matches_batch_streaming() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let samples = [
            (-95.0, -35.0, 0.0),
            (-70.0, 40.0, 0.0),
            (-35.0, -80.0, 0.0),
            (22.0, 75.0, 0.0),
            (55.0, -25.0, 0.0),
            (105.0, 50.0, 0.0),
        ];
        let request = ImagingRequest {
            geometry,
            visibility_batches: Vec::new(),
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Briggs { robust: 0.5 },
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 4,
                gain: 0.2,
                minor_cycle_length: 2,
                ..CleanConfig::default()
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let weighted_batches = vec![point_source_visibilities(
            &samples,
            geometry.cell_size_rad[0],
            geometry.image_shape,
            (34.0, 30.0),
            1.0,
        )];
        let execution_config = StandardMfsExecutionConfig {
            fixed_tile_resident_bytes: Some(usize::MAX),
            fixed_tile_edge: Some(16),
            fixed_tile_center_boundary: false,
            fixed_tile_max_live_row_blocks: 1,
        };
        let batch_streaming = run_standard_mfs_weighted_streaming_with_execution_config(
            request.clone(),
            execution_config,
            |consumer| {
                for batch in &weighted_batches {
                    consumer(vec![batch.clone()])?;
                }
                Ok(())
            },
        )
        .unwrap();
        let block_streaming =
            run_standard_mfs_weighted_sample_block_streaming_with_execution_config(
                request.clone(),
                execution_config,
                |consumer| {
                    let mut block = Vec::<StandardMfsWeightedSample>::new();
                    for batch in &weighted_batches {
                        block.clear();
                        for sample_index in 0..batch.len() {
                            block.push(StandardMfsWeightedSample {
                                u_lambda: batch.u_lambda[sample_index],
                                v_lambda: batch.v_lambda[sample_index],
                                w_lambda: batch.w_lambda[sample_index],
                                weight: batch.weight[sample_index],
                                sumwt_factor: batch.sumwt_factor[sample_index],
                                gridable: batch.gridable[sample_index],
                                visibility: batch.visibility[sample_index],
                            });
                        }
                        consumer(&block)?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        let planned_builder = StandardMfsPlannedSampleBuilder::new(geometry).unwrap();
        let planned_streaming =
            run_standard_mfs_planned_sample_block_streaming_with_execution_config(
                request.clone(),
                execution_config,
                |consumer| {
                    let mut block = Vec::<StandardMfsPlannedWeightedSample>::new();
                    for batch in &weighted_batches {
                        block.clear();
                        for sample_index in 0..batch.len() {
                            let sample = StandardMfsWeightedSample {
                                u_lambda: batch.u_lambda[sample_index],
                                v_lambda: batch.v_lambda[sample_index],
                                w_lambda: batch.w_lambda[sample_index],
                                weight: batch.weight[sample_index],
                                sumwt_factor: batch.sumwt_factor[sample_index],
                                gridable: batch.gridable[sample_index],
                                visibility: batch.visibility[sample_index],
                            };
                            if let Some(planned) = planned_builder.plan_sample(sample)? {
                                block.push(planned);
                            }
                        }
                        consumer(&block)?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        let sample_streaming = run_standard_mfs_weighted_sample_streaming_with_execution_config(
            request,
            execution_config,
            |consumer| {
                for batch in &weighted_batches {
                    for sample_index in 0..batch.len() {
                        consumer(StandardMfsWeightedSample {
                            u_lambda: batch.u_lambda[sample_index],
                            v_lambda: batch.v_lambda[sample_index],
                            w_lambda: batch.w_lambda[sample_index],
                            weight: batch.weight[sample_index],
                            sumwt_factor: batch.sumwt_factor[sample_index],
                            gridable: batch.gridable[sample_index],
                            visibility: batch.visibility[sample_index],
                        })?;
                    }
                }
                Ok(())
            },
        )
        .unwrap();

        assert_array4_close(&block_streaming.psf, &batch_streaming.psf, 1.0e-5);
        assert_array4_close(&block_streaming.residual, &batch_streaming.residual, 1.0e-5);
        assert_array4_close(&block_streaming.model, &batch_streaming.model, 1.0e-5);
        assert_array4_close(&block_streaming.image, &batch_streaming.image, 1.0e-5);
        assert_array4_close(&planned_streaming.psf, &batch_streaming.psf, 1.0e-5);
        assert_array4_close(
            &planned_streaming.residual,
            &batch_streaming.residual,
            1.0e-5,
        );
        assert_array4_close(&planned_streaming.model, &batch_streaming.model, 1.0e-5);
        assert_array4_close(&planned_streaming.image, &batch_streaming.image, 1.0e-5);
        assert_array4_close(&sample_streaming.psf, &batch_streaming.psf, 1.0e-5);
        assert_array4_close(
            &sample_streaming.residual,
            &batch_streaming.residual,
            1.0e-5,
        );
        assert_array4_close(&sample_streaming.model, &batch_streaming.model, 1.0e-5);
        assert_array4_close(&sample_streaming.image, &batch_streaming.image, 1.0e-5);
        assert_eq!(
            sample_streaming.diagnostics.minor_iterations,
            batch_streaming.diagnostics.minor_iterations
        );
        assert_eq!(
            sample_streaming.diagnostics.clean_stop_reason,
            batch_streaming.diagnostics.clean_stop_reason
        );
    }

    #[test]
    fn mosaic_clean_reduces_residual_peak_and_tracks_pb_weight_image() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let samples = [
            (-95.0, -35.0, 0.0),
            (-70.0, 40.0, 0.0),
            (-35.0, -80.0, 0.0),
            (22.0, 75.0, 0.0),
            (55.0, -25.0, 0.0),
            (105.0, 50.0, 0.0),
        ];
        let batch = point_source_visibilities(
            &samples,
            geometry.cell_size_rad[0],
            geometry.image_shape,
            (34.0, 30.0),
            1.0,
        );
        let metadata = centered_mosaic_metadata(&batch, 1.4e9);
        let gridder_mode = GridderMode::Mosaic(MosaicGridderConfig {
            phase_center_direction_rad: [0.0, 0.0],
            primary_beam_model: PrimaryBeamModel::Airy {
                dish_diameter_m: 25.0,
                blockage_diameter_m: 0.0,
            },
            pb_limit: 0.1,
            metadata_batches: vec![metadata],
        });
        let request = |niter| ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            gridder_mode: gridder_mode.clone(),
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Briggs { robust: 0.5 },
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter,
                gain: 0.2,
                minor_cycle_length: 2,
                ..CleanConfig::default()
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let dirty = run_imaging(&request(0)).unwrap();
        let clean = run_imaging(&request(8)).unwrap();

        assert!(clean.diagnostics.mosaic_weight_image.is_some());
        assert!(clean.diagnostics.minor_iterations > 0);
        assert!(clean.diagnostics.major_cycles > 0);
        assert_eq!(
            clean.diagnostics.clean_stop_reason,
            Some(CleanStopReason::IterationLimitReached)
        );
        assert!(
            clean.diagnostics.final_residual_peak_jy_per_beam
                < dirty.diagnostics.initial_residual_peak_jy_per_beam
        );
        assert!(peak_abs_value(&clean.model.slice(s![.., .., 0, 0]).to_owned()) > 0.0);
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

    fn assert_error_contains<T>(result: Result<T, super::ImagingError>, expected: &str) {
        let Err(err) = result else {
            panic!("expected request to fail");
        };
        let message = err.to_string();
        assert!(
            message.contains(expected),
            "expected error containing {expected:?}, got {message:?}"
        );
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::XX,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!((result.sumwt[(0, 0, 0, 0)] - 1.5).abs() < 1.0e-5);
    }

    #[test]
    fn combined_dirty_standard_path_matches_separate_psf_and_residual_passes() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = point_source_visibilities(
            &[
                (10.0, 5.0, 0.0),
                (25.5, -3.25, 0.0),
                (-16.0, 11.0, 0.0),
                (32.0, -18.0, 0.0),
            ],
            geometry.cell_size_rad[0],
            geometry.image_shape,
            (37.0, 29.0),
            2.0,
        );
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let weighted_batches = apply_weighting(&request, &gridder).unwrap();
        let mut separate_timings = ImagingStageTimings::default();
        let separate_psf =
            compute_psf(&request, &weighted_batches, &gridder, &mut separate_timings).unwrap();
        let model = Array2::<f32>::zeros((geometry.image_shape[0], geometry.image_shape[1]));
        let separate_residual = compute_residual(
            &request,
            &weighted_batches,
            &gridder,
            &model,
            &separate_psf,
            StandardMfsExecutionConfig::default(),
            &mut separate_timings,
        )
        .unwrap();

        let mut combined_timings = ImagingStageTimings::default();
        let (combined_psf, combined_residual) = compute_dirty_psf_and_residual_standard(
            &weighted_batches,
            &gridder,
            &mut combined_timings,
        )
        .unwrap();

        assert_close_f32(
            combined_psf.normalization_sumwt,
            separate_psf.normalization_sumwt,
            1.0e-6,
        );
        assert_close_f32(
            combined_psf.reported_sumwt,
            separate_psf.reported_sumwt,
            1.0e-6,
        );
        assert_close_f32(combined_psf.psf_peak, separate_psf.psf_peak, 1.0e-6);
        assert_eq!(combined_psf.gridded_samples, separate_psf.gridded_samples);
        assert_eq!(combined_psf.skipped_samples, separate_psf.skipped_samples);
        assert!(
            rms_difference(&combined_psf.psf, &separate_psf.psf) < 1.0e-6,
            "combined PSF should match separate PSF pass"
        );
        assert!(
            rms_difference(&combined_residual, &separate_residual) < 1.0e-6,
            "combined residual should match separate residual pass"
        );

        let mut streaming_timings = ImagingStageTimings::default();
        let (streaming_psf, streaming_residual) =
            super::compute_dirty_psf_and_residual_standard_streaming(
                &weighted_batches,
                &gridder,
                &mut streaming_timings,
            )
            .unwrap();

        assert_close_f32(
            streaming_psf.normalization_sumwt,
            combined_psf.normalization_sumwt,
            1.0e-6,
        );
        assert_close_f32(
            streaming_psf.reported_sumwt,
            combined_psf.reported_sumwt,
            1.0e-6,
        );
        assert_close_f32(streaming_psf.psf_peak, combined_psf.psf_peak, 1.0e-6);
        assert_eq!(streaming_psf.gridded_samples, combined_psf.gridded_samples);
        assert_eq!(streaming_psf.skipped_samples, combined_psf.skipped_samples);
        assert!(
            rms_difference(&streaming_psf.psf, &combined_psf.psf) < 1.0e-6,
            "streaming PSF should match planned combined PSF pass"
        );
        assert!(
            rms_difference(&streaming_residual, &combined_residual) < 1.0e-6,
            "streaming residual should match planned combined residual pass"
        );
    }

    #[test]
    fn trace_weighting_reports_normalization_and_reported_sumwt_separately() {
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
        let diagnostics = trace_weighting(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::XX,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(diagnostics.samples.len(), 1);
        assert!((diagnostics.normalization_sumwt - 1.5).abs() < 1.0e-5);
        assert!((diagnostics.reported_sumwt - 1.5).abs() < 1.0e-5);
        assert!((diagnostics.normalization_sumwt - diagnostics.reported_sumwt).abs() < 1.0e-5);
        let sample = &diagnostics.samples[0];
        assert!((sample.output_weight - 1.5).abs() < 1.0e-6);
        assert!((sample.normalization_contribution - 1.5).abs() < 1.0e-5);
        assert!((sample.reported_contribution - 1.5).abs() < 1.0e-5);
        assert_eq!(sample.density_weight, None);
    }

    #[test]
    fn trace_cube_weighting_exposes_combined_density_and_taper_effects() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let make_batch = |weight: f32| VisibilityBatch {
            u_lambda: vec![100.0],
            v_lambda: vec![50.0],
            w_lambda: vec![0.0],
            weight: vec![weight],
            sumwt_factor: vec![1.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(1.0, 0.0)],
        };
        let diagnostics = trace_cube_weighting(&CubeImagingRequest {
            geometry,
            channels: vec![
                cube_channel_request_identity(1.4e9, vec![make_batch(1.0)], 0),
                cube_channel_request_identity(1.41e9, vec![make_batch(3.0)], 1),
            ],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Uniform,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: Some(crate::GaussianUvTaper {
                major: crate::UvTaperSize::BaselineHwhmLambda(50.0),
                minor: crate::UvTaperSize::BaselineHwhmLambda(50.0),
                position_angle_rad: 0.0,
            }),
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(diagnostics.len(), 2);
        for diagnostic in &diagnostics {
            assert_eq!(diagnostic.weighting, WeightingMode::Uniform);
            assert_eq!(diagnostic.weight_density_mode, WeightDensityMode::Combined);
            assert!(diagnostic.uv_taper.is_some());
            assert_eq!(diagnostic.samples.len(), 1);
            let sample = &diagnostic.samples[0];
            assert_close_f32(sample.density_weight.unwrap(), 4.0, 1.0e-5);
            assert!(sample.output_weight > 0.0);
            assert!(sample.output_weight < sample.input_weight / 4.0);
            assert_close_f32(
                diagnostic.normalization_sumwt,
                sample.normalization_contribution,
                1.0e-6,
            );
            assert_close_f32(
                diagnostic.reported_sumwt,
                sample.reported_contribution,
                1.0e-6,
            );
        }
    }

    #[test]
    fn trace_cube_channel_w_project_plan_records_channel_specific_skips_and_validation() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [4.0e-3, 4.0e-3],
        };
        let channel_zero = cube_channel_request_identity(
            1.40e9,
            vec![VisibilityBatch {
                u_lambda: vec![5.0],
                v_lambda: vec![6.0],
                w_lambda: vec![7.0],
                weight: vec![1.0],
                sumwt_factor: vec![1.0],
                gridable: vec![true],
                visibility: vec![Complex32::new(9.0, 0.0)],
            }],
            0,
        );
        let channel_one = cube_channel_request_identity(
            1.41e9,
            vec![VisibilityBatch {
                u_lambda: vec![15.0, 50_000.0, 0.0, 20.0],
                v_lambda: vec![-20.0, 0.0, 0.0, 10.0],
                w_lambda: vec![30.0, 40.0, 50.0, f64::NAN],
                weight: vec![1.0, 2.0, 5.0, 1.0],
                sumwt_factor: vec![1.0, 2.0, 3.0, 1.0],
                gridable: vec![true, true, false, true],
                visibility: vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(2.0, 0.0),
                    Complex32::new(5.0, 0.0),
                    Complex32::new(1.0, 1.0),
                ],
            }],
            1,
        );
        let request = CubeImagingRequest {
            geometry,
            channels: vec![channel_zero, channel_one],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::PerPlane,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::WProject,
            w_project_planes: Some(8),
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        let trace = trace_cube_channel_w_project_plan(&request, 1).unwrap();

        assert_eq!(trace.requested_plane_count, Some(8));
        assert_eq!(trace.plane_count, 8);
        assert_eq!(trace.gridded_samples, 1);
        assert_eq!(trace.samples.len(), 1);
        assert_eq!(trace.samples[0].sample_index, 0);
        assert_eq!(trace.samples[0].u_lambda, 15.0);
        assert_eq!(trace.samples[0].w_lambda, 30.0);
        assert_eq!(trace.samples[0].sumwt_factor, 1.0);
        assert_eq!(trace.skipped_samples.len(), 3);
        assert_eq!(trace.skipped_samples[0].sample_index, 1);
        assert_eq!(
            trace.skipped_samples[0].reason,
            WProjectSkipReason::OutsideGrid
        );
        assert_eq!(trace.skipped_samples[1].sample_index, 2);
        assert_eq!(
            trace.skipped_samples[1].reason,
            WProjectSkipReason::NotGridable
        );
        assert_eq!(trace.skipped_samples[2].sample_index, 3);
        assert_eq!(
            trace.skipped_samples[2].reason,
            WProjectSkipReason::InvalidInput
        );
        assert_eq!(trace.max_abs_w_lambda, 40.0);

        assert_error_contains(
            trace_cube_channel_w_project_plan(&request, 2),
            "cube channel index 2 is out of range for 2 channels",
        );

        let mut standard_request = request.clone();
        standard_request.w_term_mode = WTermMode::None;
        assert_error_contains(
            trace_cube_channel_w_project_plan(&standard_request, 1),
            "trace_cube_channel_w_project_plan requires w_term_mode='wproject'",
        );
    }

    #[test]
    fn trace_cube_channel_residual_refresh_validates_channel_and_model_planes() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let samples = [(-120.0, -90.0, 0.0), (45.0, -75.0, 0.0), (110.0, 65.0, 0.0)];
        let channel_zero = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (32.0, 32.0),
                1.0,
            )],
            0,
        );
        let channel_one = cube_channel_request_identity(
            1.41e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (34.0, 31.0),
                0.8,
            )],
            1,
        );
        let request = CubeImagingRequest {
            geometry,
            channels: vec![channel_zero, channel_one],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::PerPlane,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let model_planes = vec![
            Array2::<f32>::zeros((64, 64)),
            Array2::<f32>::zeros((64, 64)),
        ];

        let trace = trace_cube_channel_residual_refresh(&request, 1, &model_planes).unwrap();
        assert_eq!(trace.samples.len(), samples.len());
        assert_eq!(trace.samples[0].batch_index, 0);
        assert_eq!(trace.samples[0].sample_index, 0);
        assert_eq!(
            trace.samples[0].observed_visibility,
            trace.samples[0].residual_visibility
        );
        assert_eq!(
            trace.samples[0].predicted_visibility,
            Complex32::new(0.0, 0.0)
        );

        assert_error_contains(
            trace_cube_channel_residual_refresh(&request, 2, &model_planes),
            "cube residual-refresh trace channel index 2 is out of range for 2 channels",
        );
        assert_error_contains(
            trace_cube_channel_residual_refresh(&request, 0, &model_planes[..1]),
            "cube residual-refresh trace model plane count 1 does not match request channel count 2",
        );

        let wrong_shape_planes = vec![
            Array2::<f32>::zeros((64, 64)),
            Array2::<f32>::zeros((32, 64)),
        ];
        assert_error_contains(
            trace_cube_channel_residual_refresh(&request, 0, &wrong_shape_planes),
            "cube residual-refresh trace model plane 1 shape",
        );
    }

    #[test]
    fn trace_cube_channel_residual_refresh_model_channel_lambda_differs_from_output_channel_lambda()
    {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let batch = VisibilityBatch {
            u_lambda: vec![95.0, -80.0, 35.0],
            v_lambda: vec![42.0, 55.0, -70.0],
            w_lambda: vec![0.0, 0.0, 0.0],
            weight: vec![1.0, 1.0, 1.0],
            sumwt_factor: vec![1.0, 1.0, 1.0],
            gridable: vec![true, true, true],
            visibility: vec![Complex32::new(0.0, 0.0); 3],
        };
        let channel_zero = CubeChannelRequest {
            channel_frequency_hz: 1.0e9,
            visibility_batches: vec![batch.clone()],
            density_batches: Vec::new(),
            model_interpolation_batches: vec![CubeModelInterpolationBatch {
                sample_contributions: (0..batch.len())
                    .map(|_| {
                        vec![CubeModelChannelContribution {
                            model_channel_index: 1,
                            factor: 1.0,
                        }]
                    })
                    .collect(),
            }],
        };
        let channel_one = cube_channel_request_identity(1.8e9, vec![batch.clone()], 1);
        let request = CubeImagingRequest {
            geometry,
            channels: vec![channel_zero, channel_one],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::PerPlane,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let mut model_planes = vec![
            Array2::<f32>::zeros((64, 64)),
            Array2::<f32>::zeros((64, 64)),
        ];
        model_planes[1][(35, 29)] = 1.0;

        let output_lambda_trace =
            trace_cube_channel_residual_refresh(&request, 0, &model_planes).unwrap();
        let model_lambda_trace =
            trace_cube_channel_residual_refresh_model_channel_lambda(&request, 0, &model_planes)
                .unwrap();

        assert_eq!(output_lambda_trace.samples.len(), batch.len());
        assert_eq!(model_lambda_trace.samples.len(), batch.len());
        let max_prediction_delta = output_lambda_trace
            .samples
            .iter()
            .zip(model_lambda_trace.samples.iter())
            .map(|(output_sample, model_sample)| {
                (output_sample.predicted_visibility - model_sample.predicted_visibility).norm()
            })
            .fold(0.0f32, f32::max);
        assert!(
            max_prediction_delta > 1.0e-4,
            "expected model-channel lambda to change cube predictions, max delta={max_prediction_delta}"
        );
        assert!(
            rms_difference(
                &output_lambda_trace.residual_image,
                &model_lambda_trace.residual_image
            ) > 1.0e-6
        );
    }

    #[test]
    fn trace_residual_refresh_matches_fft_residual_and_prediction_order() {
        let samples = vec![
            (-310.25, -205.5, 0.0),
            (-248.75, 140.125, 0.0),
            (-180.5, 285.75, 0.0),
            (-95.125, -310.875, 0.0),
            (24.625, 96.5, 0.0),
            (77.25, -55.875, 0.0),
        ];
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let mut model = Array2::<f32>::zeros((64, 64));
        model[(31, 28)] = 0.75;
        model[(36, 34)] = -0.2;
        let pixels = build_direct_pixel_coordinates(geometry);
        let components = build_direct_components(&model, &pixels, 64);
        let batch = VisibilityBatch {
            u_lambda: samples.iter().map(|(u, _, _)| *u).collect(),
            v_lambda: samples.iter().map(|(_, v, _)| *v).collect(),
            w_lambda: samples.iter().map(|(_, _, w)| *w).collect(),
            visibility: samples
                .iter()
                .map(|(u, v, w)| direct_predict_visibility(&components, *u, *v, *w))
                .collect(),
            weight: vec![1.0; samples.len()],
            sumwt_factor: vec![1.0; samples.len()],
            gridable: vec![true; samples.len()],
        };
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let weighted_batches = apply_weighting(&request, &gridder).unwrap();
        let mut stage_timings = ImagingStageTimings::default();
        let psf_state =
            compute_psf(&request, &weighted_batches, &gridder, &mut stage_timings).unwrap();
        let fft_residual = compute_residual(
            &request,
            &weighted_batches,
            &gridder,
            &model,
            &psf_state,
            StandardMfsExecutionConfig::default(),
            &mut stage_timings,
        )
        .unwrap();

        let trace = trace_residual_refresh(&request, &model).unwrap();
        assert_eq!(trace.samples.len(), batch.len());
        for (sample, source) in trace.samples.iter().zip(samples.iter()) {
            assert_eq!(sample.u_lambda, source.0);
            assert_eq!(sample.v_lambda, source.1);
            assert_eq!(sample.w_lambda, source.2);
            assert!(sample.weight > 0.0);
            assert!(sample.gridable);
            let recomposed = sample.predicted_visibility + sample.residual_visibility;
            assert_close_f32(recomposed.re, sample.observed_visibility.re, 1.0e-5);
            assert_close_f32(recomposed.im, sample.observed_visibility.im, 1.0e-5);
        }
        let residual_rms = rms_difference(&trace.residual_image, &fft_residual);
        assert!(
            residual_rms < 1.0e-6,
            "trace/image mismatch rms={residual_rms}"
        );
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 10.0,
                nsigma: 0.0,
                ..CleanConfig::default()
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
    #[serial(casa_cpp)]
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            StandardMfsExecutionConfig::default(),
            &mut stage_timings,
        )
        .unwrap();
        let fft_peak = peak_abs_value(&fft_residual);
        let (max_trace_casacore_delta, max_trace_direct_delta) =
            assert_standard_residual_trace_matches_casacore(
                "off-center source",
                &request,
                &gridder,
                &model,
                samples.len(),
            );
        let cpp = match cpp_convolve_gridder_make_model_residual_image_2d(
            gridder.grid_shape(),
            geometry.image_shape,
            [
                gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
            ],
            [
                gridder.grid_shape()[0] as f64 / 2.0,
                gridder.grid_shape()[1] as f64 / 2.0,
            ],
            &batch.u_lambda,
            &batch.v_lambda,
            &batch
                .visibility
                .iter()
                .map(|value| value.re)
                .collect::<Vec<_>>(),
            &batch
                .visibility
                .iter()
                .map(|value| value.im)
                .collect::<Vec<_>>(),
            &batch.weight,
            &batch.gridable,
            model.as_slice().unwrap(),
        ) {
            Ok(result) => result,
            Err(error) if error == "casacore C++ backend unavailable" => return,
            Err(error) => panic!("run model residual interop: {error}"),
        };
        let mut sum_sq = 0.0f64;
        let mut max_abs = 0.0f32;
        let mut cpp_peak = 0.0f32;
        for (&rust_value, &cpp_value) in fft_residual.iter().zip(&cpp.pixels) {
            let delta = rust_value - cpp_value;
            sum_sq += f64::from(delta) * f64::from(delta);
            max_abs = max_abs.max(delta.abs());
            cpp_peak = cpp_peak.max(cpp_value.abs());
        }
        let rms = (sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
        eprintln!(
            "standard MFS off-center predictor diagnostics: max_trace_casacore_delta={max_trace_casacore_delta:.3e} max_trace_direct_delta={max_trace_direct_delta:.3e} residual_rms={rms:.3e} residual_max_abs={max_abs:.3e}"
        );
        assert!(
            rms < 1.0e-5 && max_abs < 1.0e-4,
            "FFT residual should match casacore for the off-center source: rust_peak={fft_peak} cpp_peak={cpp_peak} rms={rms} max_abs={max_abs}"
        );
    }

    #[test]
    #[serial(casa_cpp)]
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            StandardMfsExecutionConfig::default(),
            &mut stage_timings,
        )
        .unwrap();
        let fft_peak = peak_abs_value(&fft_residual);
        let (max_trace_casacore_delta, max_trace_direct_delta) =
            assert_standard_residual_trace_matches_casacore(
                "structured model",
                &request,
                &gridder,
                &model,
                samples.len(),
            );
        let cpp = match cpp_convolve_gridder_make_model_residual_image_2d(
            gridder.grid_shape(),
            geometry.image_shape,
            [
                gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
            ],
            [
                gridder.grid_shape()[0] as f64 / 2.0,
                gridder.grid_shape()[1] as f64 / 2.0,
            ],
            &batch.u_lambda,
            &batch.v_lambda,
            &batch
                .visibility
                .iter()
                .map(|value| value.re)
                .collect::<Vec<_>>(),
            &batch
                .visibility
                .iter()
                .map(|value| value.im)
                .collect::<Vec<_>>(),
            &batch.weight,
            &batch.gridable,
            model.as_slice().unwrap(),
        ) {
            Ok(result) => result,
            Err(error) if error == "casacore C++ backend unavailable" => return,
            Err(error) => panic!("run model residual interop: {error}"),
        };
        let mut sum_sq = 0.0f64;
        let mut max_abs = 0.0f32;
        let mut cpp_peak = 0.0f32;
        for (&rust_value, &cpp_value) in fft_residual.iter().zip(&cpp.pixels) {
            let delta = rust_value - cpp_value;
            sum_sq += f64::from(delta) * f64::from(delta);
            max_abs = max_abs.max(delta.abs());
            cpp_peak = cpp_peak.max(cpp_value.abs());
        }
        let rms = (sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
        eprintln!(
            "standard MFS structured residual-refresh diagnostics: max_trace_casacore_delta={max_trace_casacore_delta:.3e} max_trace_direct_delta={max_trace_direct_delta:.3e} residual_rms={rms:.3e} residual_max_abs={max_abs:.3e}"
        );
        assert!(
            rms < 1.0e-5 && max_abs < 1.0e-4,
            "FFT residual should match casacore for the structured model: rust_peak={fft_peak} cpp_peak={cpp_peak} rms={rms} max_abs={max_abs}"
        );
    }

    #[test]
    #[serial(casa_cpp)]
    fn standard_mfs_model_predictor_matches_casacore_for_structured_model() {
        let geometry = ImageGeometry {
            image_shape: [257, 257],
            cell_size_rad: [8.638_889_530_690e-7_f64.to_radians(); 2],
        };
        let mut model = Array2::<f32>::zeros((257, 257));
        for x in 0..257 {
            for y in 0..257 {
                let dx = (x as f32 - 129.25) / 27.0;
                let dy = (y as f32 - 126.5) / 19.0;
                let ring = (-(dx * dx + dy * dy)).exp();
                let shoulder = (-(((x as f32 - 87.0) / 13.0).powi(2)
                    + ((y as f32 - 169.0) / 21.0).powi(2)))
                .exp();
                model[(x, y)] = 0.0025 * ring - 0.0007 * shoulder;
            }
        }
        let predictor = StandardMfsModelPredictor::new(geometry, &model).unwrap();
        let gridder = StandardGridder::new_with_casa_composite_padding(geometry).unwrap();
        let pixels = build_direct_pixel_coordinates(geometry);
        let components = build_direct_components(&model, &pixels, geometry.image_shape[1]);
        let samples = [
            (4_806.297_926_382_51_f64, 41_290.840_313_424_32_f64),
            (-38_890.191_177_123_3_f64, -12_300.584_882_047_77_f64),
            (24_915.177_739_689_71_f64, -34_020.365_105_376_14_f64),
            (-9_024.365_419_946_97_f64, 7_115.436_092_750_48_f64),
        ];

        let mut max_casacore_delta = 0.0f32;
        let mut max_direct_delta = 0.0f32;
        for &(u, v) in &samples {
            let rust = predictor.predict(u, v);
            let direct = direct_predict_visibility(&components, u, v, 0.0);
            let cpp = match cpp_convolve_gridder_predict_visibility_2d(
                gridder.grid_shape(),
                geometry.image_shape,
                [
                    gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                    gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
                ],
                [
                    gridder.grid_shape()[0] as f64 / 2.0,
                    gridder.grid_shape()[1] as f64 / 2.0,
                ],
                [u, -v],
                model.as_slice().unwrap(),
            ) {
                Ok(result) => result,
                Err(error) if error == "casacore C++ backend unavailable" => return,
                Err(error) => panic!("run predict-visibility interop: {error}"),
            };
            let cpp_value = Complex32::new(cpp.re, cpp.im);
            max_casacore_delta = max_casacore_delta.max((rust - cpp_value).norm());
            max_direct_delta = max_direct_delta.max((rust - direct).norm());
        }
        assert!(
            max_casacore_delta < 1.0e-5,
            "standard MFS MODEL_DATA predictor should match casacore ConvolveGridder for a structured model: max_casacore_delta={max_casacore_delta} max_direct_delta={max_direct_delta}"
        );
        eprintln!(
            "standard MFS MODEL_DATA predictor diagnostics: max_casacore_delta={max_casacore_delta:.3e} max_direct_delta={max_direct_delta:.3e}"
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            StandardMfsExecutionConfig::default(),
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let split = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![split_left, split_right],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        for (a, b) in full.residual.iter().zip(split.residual.iter()) {
            assert!((a - b).abs() < 1.0e-5);
        }
    }

    #[test]
    fn standard_mfs_dirty_accumulator_matches_split_run_imaging() {
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
        let request = ImagingRequest {
            geometry,
            visibility_batches: vec![all],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let full = run_imaging(&request).unwrap();
        let mut accumulator =
            StandardMfsDirtyAccumulator::new(StandardMfsDirtyAccumulatorRequest {
                geometry,
                plane_stokes: PlaneStokes::I,
                reffreq_hz: 1.4e9,
                selected_frequency_range_hz: [1.399e9, 1.401e9],
                clean: CleanConfig::default(),
                clean_mask: None,
            })
            .unwrap();
        accumulator.accumulate_batches(&[split_left]).unwrap();
        accumulator.accumulate_batches(&[split_right]).unwrap();
        let streamed = accumulator.finish().unwrap();

        assert_eq!(
            streamed.diagnostics.gridded_samples,
            full.diagnostics.gridded_samples
        );
        for (a, b) in full.psf.iter().zip(streamed.psf.iter()) {
            assert!((a - b).abs() < 1.0e-5);
        }
        for (a, b) in full.residual.iter().zip(streamed.residual.iter()) {
            assert!((a - b).abs() < 1.0e-5);
        }
        for (a, b) in full.image.iter().zip(streamed.image.iter()) {
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
        let channel_a = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (16.0, 16.0),
                1.0,
            )],
            0,
        );
        let channel_b = cube_channel_request_identity(
            1.41e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (18.0, 14.0),
                2.0,
            )],
            1,
        );

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
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
        let populated = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (16.0, 16.0),
                1.0,
            )],
            0,
        );
        let blank = cube_channel_request_identity(
            1.45e9,
            vec![VisibilityBatch {
                u_lambda: Vec::new(),
                v_lambda: Vec::new(),
                w_lambda: Vec::new(),
                weight: Vec::new(),
                sumwt_factor: Vec::new(),
                gridable: Vec::new(),
                visibility: Vec::new(),
            }],
            1,
        );

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
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
        let channel_a = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
            0,
        );
        let channel_b = cube_channel_request_identity(
            1.41e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
            1,
        );

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
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
    fn cube_channel_clean_mask_restricts_only_selected_channels() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let channel_a = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
            0,
        );
        let channel_b = cube_channel_request_identity(
            1.41e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
            1,
        );
        let mut channel_mask = Array4::<bool>::from_elem((48, 48, 1, 2), false);
        channel_mask[(24, 24, 0, 0)] = true;

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
                niter: 8,
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: Some(channel_mask),
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(peak_abs_value(&result.model.slice(s![.., .., 0, 0]).to_owned()) > 1.0e-3);
        assert_eq!(
            peak_abs_value(&result.model.slice(s![.., .., 0, 1]).to_owned()),
            0.0,
            "channel 1 should not clean from channel 0's mask"
        );
        assert_eq!(
            result.diagnostics.channel_diagnostics[0].clean_mask_pixels,
            1
        );
        assert_eq!(
            result.diagnostics.channel_diagnostics[1].clean_mask_pixels,
            0
        );
    }

    #[test]
    fn cube_hogbom_can_report_more_iterations_than_niter_with_multiple_planes() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let make_channel = |freq_hz, center, model_channel_index| {
            cube_channel_request_identity(
                freq_hz,
                vec![point_source_visibilities(
                    &samples,
                    geometry.cell_size_rad[0],
                    geometry.image_shape,
                    center,
                    1.0,
                )],
                model_channel_index,
            )
        };
        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels: vec![
                make_channel(1.40e9, (24.0, 24.0), 0),
                make_channel(1.41e9, (26.0, 22.0), 1),
                make_channel(1.42e9, (20.0, 28.0), 2),
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
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(
            result.diagnostics.minor_iterations > 1,
            "cube controller should spend one full cycle budget per plane before checking niter"
        );
    }

    #[test]
    fn cube_major_cycle_refreshes_planes_with_cross_channel_model_dependencies() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let channel0_batch = point_source_visibilities(
            &samples,
            geometry.cell_size_rad[0],
            geometry.image_shape,
            (24.0, 24.0),
            1.0,
        );
        let mut channel1_batch = channel0_batch.clone();
        for visibility in &mut channel1_batch.visibility {
            *visibility = Complex32::new(0.0, 0.0);
        }
        let channel0 = cube_channel_request_identity(1.40e9, vec![channel0_batch.clone()], 0);
        let channel1 = CubeChannelRequest {
            channel_frequency_hz: 1.41e9,
            visibility_batches: vec![channel1_batch.clone()],
            density_batches: Vec::new(),
            model_interpolation_batches: identity_cube_model_interpolation_batches(
                0,
                &[channel1_batch],
            ),
        };

        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels: vec![channel0, channel1],
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            weight_density_mode: WeightDensityMode::Combined,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 4,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert!(
            result.diagnostics.channel_diagnostics[0].minor_iterations > 0,
            "expected driving channel to clean"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].major_cycles > 0,
            "expected dependent channel to refresh after channel 0 model updates"
        );
        assert!(
            result.diagnostics.channel_diagnostics[1].final_residual_peak_jy_per_beam > 0.0,
            "expected dependent channel residual to reflect the refreshed cross-channel prediction"
        );
    }

    #[test]
    fn clark_cube_cleans_each_channel_independently() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let channel_a = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
            0,
        );
        let channel_b = cube_channel_request_identity(
            1.41e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
            1,
        );

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
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
        let channel_a = cube_channel_request_identity(
            1.40e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (24.0, 24.0),
                1.0,
            )],
            0,
        );
        let channel_b = cube_channel_request_identity(
            1.41e9,
            vec![point_source_visibilities(
                &samples,
                geometry.cell_size_rad[0],
                geometry.image_shape,
                (26.0, 22.0),
                1.5,
            )],
            1,
        );

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
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
            psf_cutoff: 0.35,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
    fn mtmfs_run_produces_taylor_terms_and_alpha_products() {
        let geometry = ImageGeometry {
            image_shape: [48, 48],
            cell_size_rad: [1.2e-4, 1.2e-4],
        };
        let samples = [(25.0, -12.0, 0.0), (-18.0, 21.0, 0.0), (8.0, 11.0, 0.0)];
        let low = point_source_visibilities(
            &samples,
            geometry.cell_size_rad[0],
            geometry.image_shape,
            (24.0, 24.0),
            0.7,
        );
        let high = point_source_visibilities(
            &samples,
            geometry.cell_size_rad[0],
            geometry.image_shape,
            (24.0, 24.0),
            1.3,
        );
        let mut batch = VisibilityBatch {
            u_lambda: Vec::new(),
            v_lambda: Vec::new(),
            w_lambda: Vec::new(),
            weight: Vec::new(),
            sumwt_factor: Vec::new(),
            gridable: Vec::new(),
            visibility: Vec::new(),
        };
        let mut frequencies_hz = Vec::new();
        for (source_batch, frequency_hz) in [(&low, 1.39e9_f64), (&high, 1.41e9_f64)] {
            batch
                .u_lambda
                .extend_from_slice(source_batch.u_lambda.as_slice());
            batch
                .v_lambda
                .extend_from_slice(source_batch.v_lambda.as_slice());
            batch
                .w_lambda
                .extend_from_slice(source_batch.w_lambda.as_slice());
            batch
                .weight
                .extend_from_slice(source_batch.weight.as_slice());
            batch
                .sumwt_factor
                .extend_from_slice(source_batch.sumwt_factor.as_slice());
            batch
                .gridable
                .extend_from_slice(source_batch.gridable.as_slice());
            batch
                .visibility
                .extend_from_slice(source_batch.visibility.as_slice());
            frequencies_hz.extend(std::iter::repeat_n(frequency_hz, source_batch.len()));
        }

        let result = run_mtmfs(&MtmfsRequest {
            geometry,
            visibility_batches: vec![batch.clone()],
            sample_frequency_batches_hz: vec![frequencies_hz.clone()],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.40e9,
            selected_frequency_range_hz: [1.39e9, 1.41e9],
            nterms: 2,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            clean: CleanConfig {
                niter: 6,
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(result.psf_terms.len(), 3);
        assert_eq!(result.residual_terms.len(), 2);
        assert_eq!(result.model_terms.len(), 2);
        assert_eq!(result.image_terms.len(), 2);
        assert_eq!(result.sumwt_terms.len(), 3);
        assert!(result.alpha.is_some());
        assert!(result.alpha_error.is_some());
        assert!(result.diagnostics.gridded_samples > 0);
        assert!(result.diagnostics.major_cycles > 0);
        assert!(result.diagnostics.minor_iterations > 0);
        assert_eq!(result.compatibility.channel_frequencies_hz, vec![1.40e9]);
        assert_eq!(result.image_terms[0].shape(), &[48, 48, 1, 1]);
        assert!(peak_abs_value(&result.image_terms[0].slice(s![.., .., 0, 0]).to_owned()) > 1.0e-3);
        assert!(peak_abs_value(&result.image_terms[1].slice(s![.., .., 0, 0]).to_owned()) > 1.0e-4);
        assert!(
            peak_abs_value(
                &result
                    .alpha
                    .as_ref()
                    .unwrap()
                    .slice(s![.., .., 0, 0])
                    .to_owned()
            ) > 1.0e-4
        );

        let multiscale_wproject = run_mtmfs(&MtmfsRequest {
            geometry,
            visibility_batches: vec![batch],
            sample_frequency_batches_hz: vec![frequencies_hz],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.40e9,
            selected_frequency_range_hz: [1.39e9, 1.41e9],
            nterms: 2,
            multiscale_scales: vec![0.0, 3.0, 8.0],
            small_scale_bias: 0.9,
            w_term_mode: WTermMode::WProject,
            w_project_planes: Some(4),
            clean: CleanConfig {
                niter: 4,
                major_cycle_limit: None,
                gain: 0.1,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();

        assert_eq!(multiscale_wproject.psf_terms.len(), 3);
        assert_eq!(multiscale_wproject.model_terms.len(), 2);
        assert!(multiscale_wproject.diagnostics.gridded_samples > 0);
        assert!(multiscale_wproject.diagnostics.minor_iterations > 0);
        assert!(
            multiscale_wproject
                .diagnostics
                .minor_cycle_traces
                .iter()
                .any(|trace| trace.initial_scale_pixels.is_some_and(|scale| scale >= 0.0))
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let clean = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 2,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
    fn casa_hogbom_compatibility_matches_hclean_reported_niter() {
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
        let nonzero = result
            .model
            .iter()
            .filter(|value| value.abs() > 0.0)
            .count();
        assert_eq!(nonzero, 1);
    }

    #[test]
    fn multiscale_clean_mask_uses_casa_scale_dependent_edges() {
        let shape = (32, 32);
        let mask = Array2::<bool>::from_elem(shape, true);
        let scales = vec![0.0, 8.0];
        let scale_images = scales
            .iter()
            .copied()
            .map(|scale| make_multiscale_kernel(shape, scale))
            .collect::<Vec<_>>();

        let scale_masks = build_multiscale_scale_masks(&mask, &scale_images, &scales);

        assert!(!scale_masks[0][(0, 16)]);
        assert!(scale_masks[0][(1, 16)]);
        assert!(!scale_masks[1][(12, 16)]);
        assert!(scale_masks[1][(13, 16)]);
        assert!(scale_masks[1][(18, 16)]);
        assert!(!scale_masks[1][(19, 16)]);
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Clark,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let multiscale = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Multiscale,
            multiscale_scales: vec![0.0],
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Clark,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Multiscale,
            multiscale_scales: vec![4.0],
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 0.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 8,
                cyclefactor: 1.0,
                min_psf_fraction: 0.05,
                max_psf_fraction: 0.8,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: Some(mask),
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 8,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                ..CleanConfig::default()
            },
            clean_mask: Some(mask),
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 12,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 12,
                cyclefactor: 0.5,
                min_psf_fraction: 0.01,
                max_psf_fraction: 0.4,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let strict = run_imaging(&ImagingRequest {
            geometry,
            visibility_batches: vec![batch],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 12,
                major_cycle_limit: None,
                gain: 0.2,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 12,
                cyclefactor: 3.0,
                min_psf_fraction: 0.4,
                max_psf_fraction: 0.9,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
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

        for w_term_mode in [WTermMode::None, WTermMode::Direct, WTermMode::WProject] {
            let request = ImagingRequest {
                geometry,
                visibility_batches: vec![cross_only.clone()],
                gridder_mode: GridderMode::Standard,
                plane_stokes: PlaneStokes::I,
                weighting: WeightingMode::Natural,
                reffreq_hz: 1.4e9,
                selected_frequency_range_hz: [1.399e9, 1.401e9],
                deconvolver: Deconvolver::Hogbom,
                multiscale_scales: Vec::new(),
                small_scale_bias: 0.0,
                clean: CleanConfig::default(),
                clean_mask: None,
                initial_model: None,
                w_term_mode,
                w_project_planes: None,
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
        let build_request = |w_term_mode| ImagingRequest {
            geometry,
            visibility_batches: vec![point_source_visibilities_with_w_term(
                &samples,
                4.0e-3,
                [64, 64],
                (42.0, 20.0),
                1.0,
            )],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            initial_model: None,
            w_term_mode,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let evaluate_dirty = |request: &ImagingRequest| {
            let gridder = StandardGridder::new(request.geometry).unwrap();
            let weighted = apply_weighting(request, &gridder).unwrap();
            let mut timings = ImagingStageTimings::default();
            let psf = compute_psf(request, &weighted, &gridder, &mut timings).unwrap();
            let residual = compute_residual(
                request,
                &weighted,
                &gridder,
                &Array2::<f32>::zeros((geometry.nx(), geometry.ny())),
                &psf,
                StandardMfsExecutionConfig::default(),
                &mut timings,
            )
            .unwrap();
            (residual, peak_abs_value(&psf.psf))
        };

        let (two_d_residual, two_d_psf_peak) = evaluate_dirty(&build_request(WTermMode::None));
        let (direct_residual, direct_psf_peak) = evaluate_dirty(&build_request(WTermMode::Direct));
        let (wproject_residual, wproject_psf_peak) =
            evaluate_dirty(&build_request(WTermMode::WProject));

        assert!(direct_residual[(42, 20)] > two_d_residual[(42, 20)]);
        assert!(wproject_residual[(42, 20)] > two_d_residual[(42, 20)]);
        assert!(direct_psf_peak >= two_d_psf_peak);
        assert!(wproject_psf_peak >= two_d_psf_peak);
    }

    #[test]
    fn trace_w_project_plan_records_planned_and_skipped_samples() {
        let request = ImagingRequest {
            geometry: ImageGeometry {
                image_shape: [64, 64],
                cell_size_rad: [4.0e-3, 4.0e-3],
            },
            visibility_batches: vec![VisibilityBatch {
                u_lambda: vec![15.0, 50_000.0, 0.0, 20.0],
                v_lambda: vec![-20.0, 0.0, 0.0, 10.0],
                w_lambda: vec![30.0, 40.0, 50.0, f64::NAN],
                weight: vec![1.0, 2.0, 5.0, 1.0],
                sumwt_factor: vec![1.0, 2.0, 3.0, 1.0],
                gridable: vec![true, true, false, true],
                visibility: vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(2.0, 0.0),
                    Complex32::new(5.0, 0.0),
                    Complex32::new(1.0, 1.0),
                ],
            }],
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: dirty_clean_config(0.35),
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::WProject,
            w_project_planes: Some(8),
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        let trace = trace_w_project_plan(&request).unwrap();

        assert_eq!(trace.requested_plane_count, Some(8));
        assert_eq!(trace.plane_count, 8);
        assert_eq!(trace.gridded_samples, 1);
        assert_eq!(trace.samples.len(), 1);
        assert_eq!(trace.samples[0].batch_index, 0);
        assert_eq!(trace.samples[0].sample_index, 0);
        assert_eq!(trace.samples[0].sumwt_factor, 1.0);
        assert!(trace.samples[0].plane_index < trace.plane_count);
        assert_eq!(trace.skipped_samples.len(), 3);
        assert_eq!(trace.skipped_samples[0].sample_index, 1);
        assert_eq!(
            trace.skipped_samples[0].reason,
            WProjectSkipReason::OutsideGrid
        );
        assert_eq!(trace.skipped_samples[1].sample_index, 2);
        assert_eq!(
            trace.skipped_samples[1].reason,
            WProjectSkipReason::NotGridable
        );
        assert_eq!(trace.skipped_samples[2].sample_index, 3);
        assert_eq!(
            trace.skipped_samples[2].reason,
            WProjectSkipReason::InvalidInput
        );
        assert_eq!(trace.max_abs_w_lambda, 40.0);
    }

    #[test]
    fn wproject_plan_matches_casa_kernel_conjugation_sign() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [4.0e-3, 4.0e-3],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let projector =
            crate::gridder::WProjector::new(geometry, &gridder, 400.0, Some(8)).unwrap();

        let positive = projector.plan_sample(15.0, -10.0, 120.0).unwrap();
        let negative = projector.plan_sample(15.0, -10.0, -120.0).unwrap();

        assert!(
            positive.conjugate_kernel,
            "CASA wprojgrid.f conjugates the kernel when uvw(3) > 0"
        );
        assert!(
            !negative.conjugate_kernel,
            "CASA wprojgrid.f uses the stored kernel directly when uvw(3) <= 0"
        );
        assert_eq!(positive.plane_index, negative.plane_index);
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
    fn minor_cycle_stop_reason_uses_threshold_tolerance() {
        assert_eq!(
            minor_cycle_stop_reason(1.009e-6, 1.0e-6, 2.0e-6, 0.0),
            Some(CleanStopReason::CycleThresholdReached)
        );
        assert_eq!(
            minor_cycle_stop_reason(1.013e-6, 1.0e-6, 2.0e-6, 0.0),
            Some(CleanStopReason::CycleThresholdReached)
        );
        assert_eq!(
            minor_cycle_stop_reason(1.013e-6, 1.0e-6, 1.0e-6, 0.0),
            Some(CleanStopReason::GlobalThresholdReached)
        );
        assert_eq!(minor_cycle_stop_reason(0.505, 0.50001, 0.50001, 0.0), None);
        assert_eq!(minor_cycle_stop_reason(1.009e-6, 0.0, 1.0e-6, 0.0), None);
        assert_eq!(
            minor_cycle_stop_reason(1.021e-6, 1.0e-6, 2.0e-6, 0.0),
            Some(CleanStopReason::CycleThresholdReached)
        );
    }

    #[test]
    fn peak_location_masked_matches_hclean_y_major_tie_breaking() {
        let image = Array2::from_shape_vec(
            (3, 3),
            vec![
                0.0, 4.0, 0.0, //
                -4.0, 0.0, 0.0, //
                0.0, 0.0, 0.0,
            ],
        )
        .unwrap();

        assert_eq!(peak_location_masked(&image, None), Some(((1, 0), -4.0)));
    }

    #[test]
    fn hogbom_minor_cycle_matches_hclean_inclusive_iteration_budget() {
        let request = ImagingRequest {
            geometry: ImageGeometry {
                image_shape: [6, 6],
                cell_size_rad: [1.0, 1.0],
            },
            visibility_batches: Vec::new(),
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.0,
            selected_frequency_range_hz: [1.0, 1.0],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 1,
                major_cycle_limit: None,
                gain: 0.5,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 1,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 1.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let psf = Array2::from_shape_vec(
            (6, 6),
            vec![
                0.0, 0.0, 0.0, 0.05, 0.0, 0.0, //
                0.0, 0.0, 0.1, 0.2, 0.1, 0.0, //
                0.0, 0.1, 0.2, 0.4, 0.2, 0.05, //
                0.05, 0.2, 0.4, 1.0, 0.4, 0.1, //
                0.0, 0.1, 0.2, 0.4, 0.2, 0.05, //
                0.0, 0.0, 0.05, 0.1, 0.05, 0.0,
            ],
        )
        .unwrap();
        let psf_state = PsfState {
            psf,
            normalization_sumwt: 1.0,
            reported_sumwt: 1.0,
            psf_peak: 1.0,
            gridded_samples: 0,
            skipped_samples: 0,
        };
        let mut model = Array2::<f32>::zeros((6, 6));
        let mut residual = Array2::from_shape_vec(
            (6, 6),
            vec![
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, //
                0.0, 0.0, 0.05, 0.1, 0.05, 0.0, //
                0.0, 0.05, 0.15, 0.4, 0.15, 0.0, //
                0.0, 0.1, 0.4, 1.2, 0.4, 0.05, //
                0.0, 0.05, 0.15, 0.4, 0.15, 0.0, //
                0.0, 0.0, 0.0, 0.05, 0.0, 0.0,
            ],
        )
        .unwrap();
        let mut stage_timings = ImagingStageTimings::default();

        let outcome = run_hogbom_minor_cycle(
            &request,
            &psf_state,
            &mut model,
            &mut residual,
            1,
            0.0,
            0.0,
            &mut stage_timings,
        );

        assert_eq!(outcome.actual_updates, 2);
        assert_eq!(outcome.reported_updates, 1);
        assert!((model[(3, 3)] - 0.9).abs() < 1.0e-6);
    }

    #[test]
    #[serial(casa_cpp)]
    fn hogbom_minor_cycle_matches_casacore_hclean_on_simple_plane() {
        let request = ImagingRequest {
            geometry: ImageGeometry {
                image_shape: [6, 6],
                cell_size_rad: [1.0, 1.0],
            },
            visibility_batches: Vec::new(),
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.0,
            selected_frequency_range_hz: [1.0, 1.0],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig {
                niter: 4,
                major_cycle_limit: None,
                gain: 0.5,
                threshold_jy_per_beam: 0.0,
                nsigma: 0.0,
                psf_cutoff: 0.35,
                minor_cycle_length: 4,
                cyclefactor: 1.0,
                min_psf_fraction: 0.0,
                max_psf_fraction: 1.0,
                hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let psf = Array2::from_shape_vec(
            (6, 6),
            vec![
                0.0, 0.0, 0.0, 0.05, 0.0, 0.0, //
                0.0, 0.0, 0.1, 0.2, 0.1, 0.0, //
                0.0, 0.1, 0.2, 0.4, 0.2, 0.05, //
                0.05, 0.2, 0.4, 1.0, 0.4, 0.1, //
                0.0, 0.1, 0.2, 0.4, 0.2, 0.05, //
                0.0, 0.0, 0.05, 0.1, 0.05, 0.0,
            ],
        )
        .unwrap();
        let psf_state = PsfState {
            psf: psf.clone(),
            normalization_sumwt: 1.0,
            reported_sumwt: 1.0,
            psf_peak: 1.0,
            gridded_samples: 0,
            skipped_samples: 0,
        };
        let mut model = Array2::<f32>::zeros((6, 6));
        let mut residual = Array2::from_shape_vec(
            (6, 6),
            vec![
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, //
                0.0, 0.0, 0.05, 0.1, 0.05, 0.0, //
                0.0, 0.05, 0.15, 0.4, 0.15, 0.0, //
                0.0, 0.1, 0.4, 1.2, 0.4, 0.05, //
                0.0, 0.05, 0.15, 0.4, 0.15, 0.0, //
                0.0, 0.0, 0.0, 0.05, 0.0, 0.0,
            ],
        )
        .unwrap();
        let mut stage_timings = ImagingStageTimings::default();
        let outcome = run_hogbom_minor_cycle(
            &request,
            &psf_state,
            &mut model,
            &mut residual,
            4,
            0.15,
            0.0,
            &mut stage_timings,
        );

        let cpp = match cpp_hogbom_clean_minor_cycle_2d(
            psf.as_slice().unwrap(),
            &[
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, //
                0.0, 0.0, 0.05, 0.1, 0.05, 0.0, //
                0.0, 0.05, 0.15, 0.4, 0.15, 0.0, //
                0.0, 0.1, 0.4, 1.2, 0.4, 0.05, //
                0.0, 0.05, 0.15, 0.4, 0.15, 0.0, //
                0.0, 0.0, 0.0, 0.05, 0.0, 0.0,
            ],
            [6, 6],
            0.5,
            0.15,
            4,
        ) {
            Ok(cpp) => cpp,
            Err(error) if error == "casacore C++ backend unavailable" => return,
            Err(error) => panic!("run casacore hclean shim: {error}"),
        };

        assert_eq!(
            outcome.reported_updates, cpp.iterdone,
            "reported updates mismatch\nrust model={model:?}\nrust residual={residual:?}\ncpp model={:?}\ncpp residual={:?}",
            cpp.model, cpp.residual
        );
        for (&rust_value, &cpp_value) in residual.iter().zip(&cpp.residual) {
            assert!(
                (rust_value - cpp_value).abs() < 1.0e-6,
                "residual mismatch: rust={rust_value} cpp={cpp_value}"
            );
        }
        for (&rust_value, &cpp_value) in model.iter().zip(&cpp.model) {
            assert!(
                (rust_value - cpp_value).abs() < 1.0e-6,
                "model mismatch: rust={rust_value} cpp={cpp_value}"
            );
        }
    }

    #[test]
    fn compute_cycle_threshold_uses_psf_fraction_only() {
        let clean = CleanConfig {
            niter: 10,
            major_cycle_limit: None,
            gain: 0.1,
            threshold_jy_per_beam: 0.5,
            nsigma: 5.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 5,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
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
