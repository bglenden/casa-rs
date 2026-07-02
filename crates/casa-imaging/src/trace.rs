// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn public_weighting_diagnostics(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
    uv_taper: Option<GaussianUvTaper>,
    trace: weighting::WeightingTraceInternal,
) -> WeightingDiagnostics {
    WeightingDiagnostics {
        weighting,
        weight_density_mode,
        uv_taper,
        samples: trace
            .samples
            .into_iter()
            .map(|sample| WeightingSampleDiagnostics {
                batch_index: sample.batch_index,
                sample_index: sample.sample_index,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                input_weight: sample.input_weight,
                density_weight: sample.density_weight,
                output_weight: sample.output_weight,
                sumwt_factor: sample.sumwt_factor,
                gridable: sample.gridable,
                normalization_contribution: sample.normalization_contribution,
                reported_contribution: sample.reported_contribution,
            })
            .collect(),
        gridded_samples: trace.gridded_samples,
        skipped_samples: trace.skipped_samples,
        normalization_sumwt: trace.normalization_sumwt,
        reported_sumwt: trace.reported_sumwt,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResidualSampleTraceInternal {
    pub(crate) batch_index: usize,
    pub(crate) sample_index: usize,
    pub(crate) u_lambda: f64,
    pub(crate) v_lambda: f64,
    pub(crate) w_lambda: f64,
    pub(crate) observed_visibility: Complex32,
    pub(crate) predicted_visibility: Complex32,
    pub(crate) residual_visibility: Complex32,
    pub(crate) weight: f32,
    pub(crate) gridable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResidualRefreshTraceInternal {
    pub(crate) samples: Vec<ResidualSampleTraceInternal>,
    pub(crate) residual_image: Array2<f32>,
    pub(crate) normalization_sumwt: f32,
    pub(crate) reported_sumwt: f32,
    pub(crate) psf_peak: f32,
    pub(crate) gridded_samples: usize,
    pub(crate) skipped_samples: usize,
}

/// Frequency convention used when tracing cube model prediction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CubePredictionLambdaMode {
    /// Degrid every contributing model plane at the output channel frequency.
    OutputChannel,
    /// Degrid each contributing model plane at that model channel's frequency.
    ModelChannel,
}

fn public_residual_refresh_diagnostics(
    trace: ResidualRefreshTraceInternal,
) -> ResidualRefreshDiagnostics {
    ResidualRefreshDiagnostics {
        samples: trace
            .samples
            .into_iter()
            .map(|sample| ResidualSampleDiagnostics {
                batch_index: sample.batch_index,
                sample_index: sample.sample_index,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                observed_visibility: sample.observed_visibility,
                predicted_visibility: sample.predicted_visibility,
                residual_visibility: sample.residual_visibility,
                weight: sample.weight,
                gridable: sample.gridable,
            })
            .collect(),
        residual_image: trace.residual_image,
        normalization_sumwt: trace.normalization_sumwt,
        reported_sumwt: trace.reported_sumwt,
        psf_peak: trace.psf_peak,
        gridded_samples: trace.gridded_samples,
        skipped_samples: trace.skipped_samples,
    }
}

/// Trace the explicit weighting seam for one MFS imaging request.
///
/// This exposes the final imaging weight assigned to each scalar sample, along
/// with the separate normalization and persisted-`sumwt` accumulators that the
/// CASA-compatible dirty path uses downstream.
pub fn trace_weighting(request: &ImagingRequest) -> Result<WeightingDiagnostics, ImagingError> {
    request.validate()?;
    let gridder = StandardGridder::new(request.geometry)?;
    let trace = trace_weighting_with_density_source(
        request.weighting,
        WeightDensityMode::Combined,
        None,
        weighting::fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
        &request.visibility_batches,
        &request.visibility_batches,
        &gridder,
    )?;
    Ok(public_weighting_diagnostics(
        request.weighting,
        WeightDensityMode::Combined,
        None,
        trace,
    ))
}

fn public_w_project_diagnostics(prepared: WProjectPreparedData) -> WProjectDiagnostics {
    let kernels = (0..prepared.projector.plane_count())
        .map(|plane_index| WProjectKernelDiagnostics {
            plane_index,
            w_lambda: prepared.projector.kernel_w_lambda(plane_index),
            support: prepared.projector.kernel_support(plane_index),
            kernel_integral: prepared.projector.kernel_integral(plane_index),
        })
        .collect();
    let samples = prepared
        .samples
        .into_iter()
        .map(|sample| WProjectSamplePlanDiagnostics {
            batch_index: sample.batch_index,
            sample_index: sample.sample_index,
            u_lambda: sample.u_lambda,
            v_lambda: sample.v_lambda,
            w_lambda: sample.w_lambda,
            weight: sample.weight,
            sumwt_factor: sample.sumwt_factor,
            plane_index: sample.positive_plan.plane_index,
            loc_x: sample.positive_plan.loc_x,
            loc_y: sample.positive_plan.loc_y,
            off_x: sample.positive_plan.off_x,
            off_y: sample.positive_plan.off_y,
            conjugate_kernel: sample.positive_plan.conjugate_kernel,
            normalization: sample.positive_plan.normalization,
            support: prepared
                .projector
                .kernel_support(sample.positive_plan.plane_index),
        })
        .collect();
    let skipped_samples = prepared
        .skipped_samples
        .into_iter()
        .map(|sample| WProjectSkippedSampleDiagnostics {
            batch_index: sample.batch_index,
            sample_index: sample.sample_index,
            u_lambda: sample.u_lambda,
            v_lambda: sample.v_lambda,
            w_lambda: sample.w_lambda,
            weight: sample.weight,
            sumwt_factor: sample.sumwt_factor,
            reason: sample.reason,
        })
        .collect();
    WProjectDiagnostics {
        requested_plane_count: prepared.requested_plane_count,
        plane_count: prepared.projector.plane_count(),
        sampling: prepared.projector.sampling(),
        w_scale: prepared.projector.w_scale(),
        max_abs_w_lambda: prepared.max_abs_w_lambda,
        kernels,
        samples,
        skipped_samples,
        normalization_sumwt: prepared.normalization_sumwt,
        reported_sumwt: prepared.reported_sumwt,
        gridded_samples: prepared.gridded_samples,
    }
}

/// Trace the explicit `wproject` CF/grid-planning seam for one imaging plane.
pub fn trace_w_project_plan(request: &ImagingRequest) -> Result<WProjectDiagnostics, ImagingError> {
    request.validate()?;
    if request.w_term_mode != WTermMode::WProject {
        return Err(ImagingError::InvalidRequest(
            "trace_w_project_plan requires w_term_mode='wproject'".to_string(),
        ));
    }
    let gridder = StandardGridder::new(request.geometry)?;
    let weighted = apply_weighting(request, &gridder)?;
    let prepared = prepare_w_project_data(
        request.geometry,
        &weighted,
        &gridder,
        request.w_project_planes,
    )?;
    Ok(public_w_project_diagnostics(prepared))
}

/// Trace the standard major-cycle residual-refresh seam for one imaging plane.
///
/// This applies the normal weighting/PSF path and then exposes the predicted
/// visibilities, residual visibilities, and refreshed residual image for the
/// supplied model. The current trace surface is limited to standard 2-D
/// imaging (`w_term_mode = None`).
pub fn trace_residual_refresh(
    request: &ImagingRequest,
    model: &Array2<f32>,
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    request.validate()?;
    if model.dim()
        != (
            request.geometry.image_shape[0],
            request.geometry.image_shape[1],
        )
    {
        return Err(ImagingError::InvalidRequest(format!(
            "residual-refresh trace model shape {:?} does not match image geometry {:?}",
            model.dim(),
            request.geometry.image_shape,
        )));
    }
    if request.w_term_mode != WTermMode::None {
        return Err(ImagingError::Unsupported(
            "trace_residual_refresh currently supports only standard 2-D imaging".to_string(),
        ));
    }
    let gridder = StandardGridder::new(request.geometry)?;
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let weighted_batches = apply_weighting(request, &gridder)?;
    stage_timings.weighting += weighting_started.elapsed();
    let psf_state = compute_psf(request, &weighted_batches, &gridder, &mut stage_timings)?;
    let trace = compute_residual_trace_standard(
        request.geometry,
        &weighted_batches,
        &gridder,
        model,
        &psf_state,
        false,
        &mut stage_timings,
    )?;
    Ok(public_residual_refresh_diagnostics(trace))
}

/// Trace the standard major-cycle residual-refresh seam for one cube plane.
///
/// Unlike [`trace_residual_refresh`], this uses explicit per-sample cube
/// interpolation state so predicted visibilities can draw from neighboring
/// model planes in the same way CASA's cube major cycle does. The trace surface
/// is currently limited to standard 2-D imaging (`w_term_mode = None`).
pub fn trace_cube_channel_residual_refresh(
    request: &ImagingRequest,
    model_interpolation_batches: &[CubeModelInterpolationBatch],
    identity_model_channel_index: usize,
    model_planes: &[Array2<f32>],
    model_channel_frequencies_hz: &[f64],
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    trace_cube_channel_residual_refresh_with_mode(
        request,
        model_interpolation_batches,
        identity_model_channel_index,
        model_planes,
        model_channel_frequencies_hz,
        CubePredictionLambdaMode::OutputChannel,
    )
}

/// Trace the cube residual-refresh seam while degridding each model
/// contribution at its own model-plane frequency instead of the output-plane
/// frequency.
///
/// This is a diagnostic helper for parity work on cube prediction semantics.
pub fn trace_cube_channel_residual_refresh_model_channel_lambda(
    request: &ImagingRequest,
    model_interpolation_batches: &[CubeModelInterpolationBatch],
    identity_model_channel_index: usize,
    model_planes: &[Array2<f32>],
    model_channel_frequencies_hz: &[f64],
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    trace_cube_channel_residual_refresh_with_mode(
        request,
        model_interpolation_batches,
        identity_model_channel_index,
        model_planes,
        model_channel_frequencies_hz,
        CubePredictionLambdaMode::ModelChannel,
    )
}

fn trace_cube_channel_residual_refresh_with_mode(
    request: &ImagingRequest,
    model_interpolation_batches: &[CubeModelInterpolationBatch],
    identity_model_channel_index: usize,
    model_planes: &[Array2<f32>],
    model_channel_frequencies_hz: &[f64],
    prediction_lambda_mode: CubePredictionLambdaMode,
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    request.validate()?;
    if request.w_term_mode != WTermMode::None {
        return Err(ImagingError::Unsupported(
            "trace_cube_channel_residual_refresh currently supports only standard 2-D imaging"
                .to_string(),
        ));
    }
    if model_planes.len() != model_channel_frequencies_hz.len() {
        return Err(ImagingError::InvalidRequest(format!(
            "cube residual-refresh trace model plane count {} does not match model frequency count {}",
            model_planes.len(),
            model_channel_frequencies_hz.len()
        )));
    }
    let expected_shape = (
        request.geometry.image_shape[0],
        request.geometry.image_shape[1],
    );
    for (model_channel_index, model_plane) in model_planes.iter().enumerate() {
        if model_plane.dim() != expected_shape {
            return Err(ImagingError::InvalidRequest(format!(
                "cube residual-refresh trace model plane {model_channel_index} shape {:?} does not match image geometry {:?}",
                model_plane.dim(),
                request.geometry.image_shape
            )));
        }
    }
    if identity_model_channel_index >= model_planes.len() {
        return Err(ImagingError::InvalidRequest(format!(
            "cube residual-refresh trace identity channel index {identity_model_channel_index} is out of range for {} model planes",
            model_planes.len()
        )));
    }
    let gridder = StandardGridder::new(request.geometry)?;
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let weighted_batches = apply_weighting(request, &gridder)?;
    stage_timings.weighting += weighting_started.elapsed();
    let psf_state = compute_psf(request, &weighted_batches, &gridder, &mut stage_timings)?;
    let trace = compute_residual_trace_cube_standard(
        &weighted_batches,
        model_interpolation_batches,
        &gridder,
        model_planes,
        identity_model_channel_index,
        request.reffreq_hz,
        model_channel_frequencies_hz,
        prediction_lambda_mode,
        &psf_state,
        &mut stage_timings,
    )?;
    Ok(public_residual_refresh_diagnostics(trace))
}
