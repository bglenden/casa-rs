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

/// Trace the explicit weighting seam for every plane of a spectral-cube request.
///
/// The returned diagnostics stay in channel order and preserve the
/// `perchanweightdensity` / taper settings that feed CASA-style cube dirty
/// imaging.
pub fn trace_cube_weighting(
    request: &CubeImagingRequest,
) -> Result<Vec<WeightingDiagnostics>, ImagingError> {
    request.validate()?;
    let combined_density_batches =
        matches!(request.weight_density_mode, WeightDensityMode::Combined).then(|| {
            request
                .channels
                .iter()
                .flat_map(|channel| channel.visibility_batches.iter().cloned())
                .collect::<Vec<_>>()
        });
    let mut diagnostics = Vec::with_capacity(request.channels.len());
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
            initial_model: None,
            w_term_mode: request.w_term_mode,
            w_project_planes: request.w_project_planes,
            compatibility: request.compatibility,
        };
        plane_request.validate()?;
        let gridder = StandardGridder::new(plane_request.geometry)?;
        let density_batches = match request.weight_density_mode {
            WeightDensityMode::Combined => combined_density_batches
                .as_deref()
                .expect("combined cube density batches prepared"),
            WeightDensityMode::PerPlane if channel.density_batches.is_empty() => {
                &plane_request.visibility_batches
            }
            WeightDensityMode::PerPlane => &channel.density_batches,
        };
        let trace = trace_weighting_with_density_source(
            plane_request.weighting,
            request.weight_density_mode,
            request.uv_taper,
            weighting::fractional_bandwidth_from_frequency_range([
                request
                    .channels
                    .first()
                    .map(|channel| channel.channel_frequency_hz)
                    .unwrap_or(channel.channel_frequency_hz),
                request
                    .channels
                    .last()
                    .map(|channel| channel.channel_frequency_hz)
                    .unwrap_or(channel.channel_frequency_hz),
            ]),
            &plane_request.visibility_batches,
            density_batches,
            &gridder,
        )?;
        diagnostics.push(public_weighting_diagnostics(
            plane_request.weighting,
            request.weight_density_mode,
            request.uv_taper,
            trace,
        ));
    }
    Ok(diagnostics)
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

/// Trace the explicit `wproject` CF/grid-planning seam for one cube channel.
pub fn trace_cube_channel_w_project_plan(
    request: &CubeImagingRequest,
    channel_index: usize,
) -> Result<WProjectDiagnostics, ImagingError> {
    request.validate()?;
    if request.w_term_mode != WTermMode::WProject {
        return Err(ImagingError::InvalidRequest(
            "trace_cube_channel_w_project_plan requires w_term_mode='wproject'".to_string(),
        ));
    }
    let Some(channel) = request.channels.get(channel_index) else {
        return Err(ImagingError::InvalidRequest(format!(
            "cube channel index {channel_index} is out of range for {} channels",
            request.channels.len()
        )));
    };
    let plane_request = ImagingRequest {
        geometry: request.geometry,
        visibility_batches: channel.visibility_batches.clone(),
        gridder_mode: GridderMode::Standard,
        plane_stokes: request.plane_stokes,
        weighting: request.weighting,
        reffreq_hz: channel.channel_frequency_hz,
        selected_frequency_range_hz: [channel.channel_frequency_hz, channel.channel_frequency_hz],
        deconvolver: request.deconvolver,
        multiscale_scales: request.multiscale_scales.clone(),
        small_scale_bias: request.small_scale_bias,
        clean: request.clean,
        clean_mask: request.clean_mask.clone(),
        initial_model: None,
        w_term_mode: request.w_term_mode,
        w_project_planes: request.w_project_planes,
        compatibility: request.compatibility,
    };
    trace_w_project_plan(&plane_request)
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
/// Unlike [`trace_residual_refresh`], this uses the per-sample cube
/// interpolation state carried by [`CubeChannelRequest`] so the predicted
/// visibilities can draw from neighboring model planes in the same way CASA's
/// cube major cycle does. The trace surface is currently limited to standard
/// 2-D imaging (`w_term_mode = None`).
pub fn trace_cube_channel_residual_refresh(
    request: &CubeImagingRequest,
    channel_index: usize,
    model_planes: &[Array2<f32>],
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    trace_cube_channel_residual_refresh_with_mode(
        request,
        channel_index,
        model_planes,
        CubePredictionLambdaMode::OutputChannel,
    )
}

/// Trace the cube residual-refresh seam while degridding each model
/// contribution at its own model-plane frequency instead of the output-plane
/// frequency.
///
/// This is a diagnostic helper for parity work on cube prediction semantics.
pub fn trace_cube_channel_residual_refresh_model_channel_lambda(
    request: &CubeImagingRequest,
    channel_index: usize,
    model_planes: &[Array2<f32>],
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    trace_cube_channel_residual_refresh_with_mode(
        request,
        channel_index,
        model_planes,
        CubePredictionLambdaMode::ModelChannel,
    )
}

fn trace_cube_channel_residual_refresh_with_mode(
    request: &CubeImagingRequest,
    channel_index: usize,
    model_planes: &[Array2<f32>],
    prediction_lambda_mode: CubePredictionLambdaMode,
) -> Result<ResidualRefreshDiagnostics, ImagingError> {
    request.validate()?;
    if request.w_term_mode != WTermMode::None {
        return Err(ImagingError::Unsupported(
            "trace_cube_channel_residual_refresh currently supports only standard 2-D imaging"
                .to_string(),
        ));
    }
    if model_planes.len() != request.channels.len() {
        return Err(ImagingError::InvalidRequest(format!(
            "cube residual-refresh trace model plane count {} does not match request channel count {}",
            model_planes.len(),
            request.channels.len()
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
    let Some(channel) = request.channels.get(channel_index) else {
        return Err(ImagingError::InvalidRequest(format!(
            "cube residual-refresh trace channel index {channel_index} is out of range for {} channels",
            request.channels.len()
        )));
    };
    let plane_request = ImagingRequest {
        geometry: request.geometry,
        visibility_batches: channel.visibility_batches.clone(),
        gridder_mode: GridderMode::Standard,
        plane_stokes: request.plane_stokes,
        weighting: request.weighting,
        reffreq_hz: channel.channel_frequency_hz,
        selected_frequency_range_hz: [channel.channel_frequency_hz, channel.channel_frequency_hz],
        deconvolver: request.deconvolver,
        multiscale_scales: request.multiscale_scales.clone(),
        small_scale_bias: request.small_scale_bias,
        clean: request.clean,
        clean_mask: request.clean_mask.clone(),
        initial_model: None,
        w_term_mode: request.w_term_mode,
        w_project_planes: request.w_project_planes,
        compatibility: request.compatibility,
    };
    plane_request.validate()?;
    let gridder = StandardGridder::new(plane_request.geometry)?;
    let combined_density_batches =
        matches!(request.weight_density_mode, WeightDensityMode::Combined).then(|| {
            request
                .channels
                .iter()
                .flat_map(|cube_channel| cube_channel.visibility_batches.iter().cloned())
                .collect::<Vec<_>>()
        });
    let density_batches = match request.weight_density_mode {
        WeightDensityMode::Combined => combined_density_batches
            .as_deref()
            .expect("combined cube density batches prepared"),
        WeightDensityMode::PerPlane if channel.density_batches.is_empty() => {
            &plane_request.visibility_batches
        }
        WeightDensityMode::PerPlane => &channel.density_batches,
    };
    let mut stage_timings = ImagingStageTimings::default();
    let weighting_started = Instant::now();
    let weighted_batches = apply_weighting_with_density_source(
        plane_request.weighting,
        request.weight_density_mode,
        request.uv_taper,
        weighting::fractional_bandwidth_from_frequency_range([
            request
                .channels
                .first()
                .map(|channel| channel.channel_frequency_hz)
                .unwrap_or(plane_request.reffreq_hz),
            request
                .channels
                .last()
                .map(|channel| channel.channel_frequency_hz)
                .unwrap_or(plane_request.reffreq_hz),
        ]),
        &plane_request.visibility_batches,
        density_batches,
        &gridder,
    )?;
    stage_timings.weighting += weighting_started.elapsed();
    let psf_state = compute_psf(
        &plane_request,
        &weighted_batches,
        &gridder,
        &mut stage_timings,
    )?;
    let trace = compute_residual_trace_cube_standard(
        &weighted_batches,
        &channel.model_interpolation_batches,
        &gridder,
        model_planes,
        channel_index,
        channel.channel_frequency_hz,
        &request
            .channels
            .iter()
            .map(|cube_channel| cube_channel.channel_frequency_hz)
            .collect::<Vec<_>>(),
        prediction_lambda_mode,
        &psf_state,
        &mut stage_timings,
    )?;
    Ok(public_residual_refresh_diagnostics(trace))
}
