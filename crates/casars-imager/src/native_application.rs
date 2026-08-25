// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin task-surface projection into the native imaging application.

use std::time::Instant;

use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumStopReason,
    ContinuumWeighting, TaskRouteRequirement, execute_continuum,
};

use super::{
    CleanMaskMode, CleanStopReason, CliConfig, Deconvolver, ImagingFftBackendPolicy,
    ImagingFftPrecisionPolicy, ImagingMemoryPressurePolicy, RestoringBeamMode, RunSummary,
    SaveModelMode, SpectralMode, StandardMfsAccelerationPolicy, WTermMode, WeightingMode,
};

pub(super) fn execute(config: &CliConfig) -> Result<RunSummary, String> {
    let started = Instant::now();
    let result =
        execute_continuum(application_request(config)).map_err(|error| error.to_string())?;
    let native = result.outcome.output;
    let clean_stop_reason = result.minor_stop_reason.map(|reason| match reason {
        ContinuumStopReason::ThresholdReached => CleanStopReason::GlobalThresholdReached,
        ContinuumStopReason::IterationBound => CleanStopReason::IterationLimitReached,
        ContinuumStopReason::StalenessBound => CleanStopReason::MajorCycleLimitReached,
    });
    Ok(RunSummary {
        warnings: Vec::new(),
        gridded_samples: usize::try_from(native.scientific.normal_state().sample_count())
            .map_err(|_| "native selected-sample count exceeds usize".to_string())?,
        major_cycles: usize::from(native.final_major_receipt.is_some()) + 1,
        minor_iterations: result.minor_iterations,
        clean_stop_reason,
        elapsed: started.elapsed(),
        output_products: result.product_names,
    })
}

fn application_request(config: &CliConfig) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set: config.ms.clone(),
        image_name: config.imagename.clone(),
        image_size: config.imsize,
        cell_arcsec: config.cell_arcsec,
        field_ids: config.field_ids.clone(),
        uv_range: config.uvrange.clone(),
        intent: config.intent.clone(),
        data_description: config.ddid,
        spectral_window: config
            .spw_selector
            .clone()
            .or_else(|| config.spw.map(|spw| spw.to_string())),
        channel_start: config.channel_start,
        channel_count: config.channel_count,
        data_column: config.datacolumn.clone(),
        algorithm: if config.dirty_only || config.niter == 0 {
            ContinuumAlgorithm::Dirty
        } else {
            match config.deconvolver {
                Deconvolver::Hogbom => ContinuumAlgorithm::Hogbom,
                Deconvolver::Clark => ContinuumAlgorithm::Clark,
                Deconvolver::Multiscale => ContinuumAlgorithm::Multiscale {
                    scales_px: config
                        .multiscale_scales
                        .iter()
                        .copied()
                        .map(f64::from)
                        .collect(),
                },
                Deconvolver::Mtmfs => ContinuumAlgorithm::Mtmfs {
                    terms: config.nterms,
                },
            }
        },
        weighting: match config.weighting {
            WeightingMode::Natural => ContinuumWeighting::Natural,
            WeightingMode::Uniform => ContinuumWeighting::Uniform,
            WeightingMode::Briggs { robust } => ContinuumWeighting::Briggs(f64::from(robust)),
            WeightingMode::BriggsBwTaper { robust } => {
                ContinuumWeighting::BriggsBandwidthTaper(f64::from(robust))
            }
        },
        iterations: config.niter,
        gain: f64::from(config.gain),
        threshold_jy: f64::from(config.threshold_jy),
        psf_cutoff: config.psf_cutoff,
        beam_policy: match config.restoring_beam_mode {
            RestoringBeamMode::PerPlane => ContinuumBeamPolicy::PerPlane,
            RestoringBeamMode::Common => ContinuumBeamPolicy::Common,
        },
        route_requirements: task_route_requirements(config),
    }
}

fn task_route_requirements(config: &CliConfig) -> Vec<TaskRouteRequirement> {
    let mut requirements = vec![match config.spectral_mode {
        SpectralMode::Mfs => TaskRouteRequirement::SerialCpu,
        SpectralMode::Cube => TaskRouteRequirement::SpectralCube,
        SpectralMode::Cubedata => TaskRouteRequirement::SpectralCubedata,
    }];
    if config.aw_project.is_some() {
        requirements.push(TaskRouteRequirement::AwProjection);
    } else if config.w_term_mode == WTermMode::WProject {
        requirements.push(TaskRouteRequirement::WProjection);
    } else if config.use_pointing && !config.force_standard_gridder {
        requirements.push(TaskRouteRequirement::MosaicGridder);
    }
    if config.outlier_file.is_some() {
        requirements.push(TaskRouteRequirement::FacetsOutliers);
    }
    if config.use_mask == CleanMaskMode::AutoMultiThreshold {
        requirements.push(TaskRouteRequirement::Automasking);
    }
    if config.use_mask == CleanMaskMode::AutoMultiThreshold
        || !config.mask_boxes.is_empty()
        || config.mask_image.is_some()
    {
        requirements.push(TaskRouteRequirement::MaskProduct);
    }
    if config.start_model.is_some() {
        requirements.push(TaskRouteRequirement::StartModel);
    }
    if config.save_model != SaveModelMode::None {
        requirements.push(TaskRouteRequirement::ModelColumnWrite);
    }
    requirements.extend(backend_requirements(config));
    if unsupported_native_controls(config) {
        requirements.push(TaskRouteRequirement::NativeV1UnsupportedControls);
    }
    requirements
}

fn backend_requirements(config: &CliConfig) -> Vec<TaskRouteRequirement> {
    let mut requirements = Vec::new();
    match config.standard_mfs_acceleration {
        StandardMfsAccelerationPolicy::Auto => {
            requirements.push(TaskRouteRequirement::ExecutionAuto);
        }
        StandardMfsAccelerationPolicy::Cpu => {}
        StandardMfsAccelerationPolicy::MultiCpu => {
            requirements.push(TaskRouteRequirement::FixedTileCpu);
        }
        StandardMfsAccelerationPolicy::Metal => {
            requirements.push(TaskRouteRequirement::MetalRowRunGroupedGridder);
        }
    }
    if let Some(backend) = config.standard_mfs_backend.as_deref() {
        requirements.push(match backend {
            "cpu" | "serial" | "serial-cpu" => TaskRouteRequirement::SerialCpu,
            "fixed-tile" | "fixed-tile-cpu" => TaskRouteRequirement::FixedTileCpu,
            "metal" | "metal-gridder" => TaskRouteRequirement::MetalGridder,
            "metal-row-run" | "metal-row-run-gridder" => TaskRouteRequirement::MetalRowRunGridder,
            "metal-row-run-grouped" | "metal-row-run-grouped-gridder" => {
                TaskRouteRequirement::MetalRowRunGroupedGridder
            }
            _ => TaskRouteRequirement::NativeV1UnsupportedControls,
        });
    }
    match config.imaging_fft_backend {
        ImagingFftBackendPolicy::RustFft => {}
        ImagingFftBackendPolicy::Auto => requirements.push(TaskRouteRequirement::FftAuto),
        ImagingFftBackendPolicy::Accelerate => requirements.push(TaskRouteRequirement::Accelerate),
        ImagingFftBackendPolicy::MetalMpsGraph => {
            requirements.push(TaskRouteRequirement::MetalMpsGraph)
        }
        ImagingFftBackendPolicy::Fftw => requirements.push(TaskRouteRequirement::Fftw),
    }
    requirements
}

fn unsupported_native_controls(config: &CliConfig) -> bool {
    config.phasecenter_field.is_some()
        || config.phasecenter.is_some()
        || config
            .correlation
            .as_deref()
            .is_some_and(|plane| !plane.eq_ignore_ascii_case("I"))
        || config.uv_taper.is_some()
        || config.nmajor.is_some()
        || config.nsigma != 0.0
        || config.minor_cycle_length != 1000
        || config.cyclefactor != 1.0
        || config.min_psf_fraction != 0.05
        || config.max_psf_fraction != 0.8
        || config.hogbom_iteration_mode != super::HogbomIterationMode::Strict
        || config.fullsummary
        || config.pbcor
        || config.write_pb
        || config.chanchunks.is_some()
        || config.per_channel_weight_density
        || config.w_project_planes.is_some()
        || config.standard_mfs_grid_threads.is_some()
        || config.standard_mfs_tile_anchor.is_some()
        || config.standard_mfs_residual_backend.is_some()
        || config.standard_mfs_initial_dirty_backend.is_some()
        || config.standard_mfs_metal_minor_cycle_chunk.is_some()
        || config.standard_mfs_metal_grouped_input_cache.is_some()
        || config.standard_mfs_memory_target_mb.is_some()
        || config.standard_mfs_prepare_buffer_mb.is_some()
        || config.imaging_memory_target_mb.is_some()
        || config.imaging_memory_pressure_policy != ImagingMemoryPressurePolicy::Auto
        || config.imaging_prepare_buffer_mb.is_some()
        || config.imaging_row_block_rows.is_some()
        || config.imaging_prepare_workers.is_some()
        || config.imaging_read_ahead_blocks.is_some()
        || config.imaging_fft_precision != ImagingFftPrecisionPolicy::Auto
        || config.write_preview_pngs
        || config.niter > config.minor_cycle_length
        || !matches!(config.deconvolver, Deconvolver::Hogbom)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{CliConfig, TaskRouteRequirement, backend_requirements};

    fn config(extra: &[&str]) -> CliConfig {
        let mut args = vec![
            "--ms",
            "fixture.ms",
            "--imagename",
            "image",
            "--imsize",
            "16",
            "--cell-arcsec",
            "1",
        ];
        args.extend_from_slice(extra);
        CliConfig::parse(args.into_iter().map(OsString::from)).expect("test config")
    }

    #[test]
    fn automatic_backend_choices_remain_explicit_route_requirements() {
        assert_eq!(
            backend_requirements(&config(&[
                "--standard-mfs-acceleration",
                "auto",
                "--imaging-fft-backend",
                "auto",
            ])),
            vec![
                TaskRouteRequirement::ExecutionAuto,
                TaskRouteRequirement::FftAuto,
            ]
        );
    }

    #[test]
    fn default_backend_choices_select_the_available_native_cpu_route() {
        assert!(backend_requirements(&config(&[])).is_empty());
    }

    #[test]
    fn metal_acceleration_uses_the_matrix_grouped_row_run_capability() {
        assert_eq!(
            backend_requirements(&config(&["--standard-mfs-acceleration", "metal"])),
            vec![TaskRouteRequirement::MetalRowRunGroupedGridder]
        );
    }
}
