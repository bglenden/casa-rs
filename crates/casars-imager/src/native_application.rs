// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin task-surface projection into the native imaging application.

use std::time::Instant;

use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumBeamPolicy, ContinuumImagingRequest,
    ContinuumMask, ContinuumMaskBox, ContinuumStopReason, ContinuumWeighting,
    HogbomIterationAccounting, SpectralImagingMode, TaskRequirement,
    VisibilityContinuumSubtraction, execute_continuum,
};

use super::{
    CleanMaskMode, CleanStopReason, CliConfig, CubeAxisValue, Deconvolver, ImagingFftBackendPolicy,
    ImagingFftPrecisionPolicy, ImagingMemoryPressurePolicy, RestoringBeamMode, RunSummary,
    SaveModelMode, SpectralMode, StandardMfsAccelerationPolicy, WTermMode, WeightingMode,
};

fn hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

pub(super) fn execute(config: &CliConfig) -> Result<RunSummary, String> {
    let started = Instant::now();
    let result =
        execute_continuum(application_request(config)?).map_err(|error| error.to_string())?;
    let minor_cycles = result.minor_cycles.clone();
    let native = result.outcome.output;
    let visibility_products = native.visibility_products.map(|completion| {
        crate::task_contract::ImagerVisibilityProductDiagnostic {
            problem_id: hex(completion.problem_id().as_bytes()),
            final_model_generation: hex(completion.final_model().as_bytes()),
            selected_generation: hex(completion.selected_generation().as_bytes()),
            weighting_generation: hex(completion.weighting_generation().as_bytes()),
            model_product: hex(completion.model_product().as_bytes()),
            residual_product: hex(completion.residual_product().as_bytes()),
            sample_count: completion.sample_count(),
        }
    });
    let clean_stop_reason = result.minor_stop_reason.map(|reason| match reason {
        ContinuumStopReason::ThresholdReached => CleanStopReason::GlobalThresholdReached,
        ContinuumStopReason::IterationBound => CleanStopReason::IterationLimitReached,
        ContinuumStopReason::StalenessBound => CleanStopReason::MajorCycleLimitReached,
        ContinuumStopReason::MultiscaleDivergence => CleanStopReason::DivergenceDetected,
    });
    Ok(RunSummary {
        warnings: Vec::new(),
        gridded_samples: usize::try_from(native.scientific.normal_state().sample_count())
            .map_err(|_| "native selected-sample count exceeds usize".to_string())?,
        major_cycles: native.major_cycle_count,
        minor_iterations: result.minor_iterations,
        actual_minor_iterations: result.actual_minor_iterations,
        clean_stop_reason,
        minor_cycles,
        visibility_products,
        elapsed: started.elapsed(),
        output_products: result.product_names,
    })
}

fn application_request(config: &CliConfig) -> Result<ContinuumImagingRequest, String> {
    let spectral_mode = match config.spectral_mode {
        SpectralMode::Mfs => SpectralImagingMode::Continuum,
        SpectralMode::Cube | SpectralMode::Cubedata => {
            let mut axis = config.cube_axis.clone();
            axis.specmode = config.spectral_mode.cube_specmode();
            if axis.start.is_none()
                && let Some(start) = config.channel_start
            {
                axis.start = Some(CubeAxisValue::Channel(
                    i32::try_from(start)
                        .map_err(|_| "cube channel start exceeds i32".to_string())?,
                ));
            }
            SpectralImagingMode::Cube {
                axis,
                output_channels: config.channel_count,
            }
        }
    };
    let algorithm = if config.dirty_only || config.niter == 0 {
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
                small_scale_bias: f64::from(config.small_scale_bias),
            },
            Deconvolver::Mtmfs => ContinuumAlgorithm::Mtmfs {
                terms: config.nterms,
                scales_px: if config.multiscale_scales.is_empty() {
                    vec![0.0]
                } else {
                    config
                        .multiscale_scales
                        .iter()
                        .copied()
                        .map(f64::from)
                        .collect()
                },
                small_scale_bias: f64::from(config.small_scale_bias),
            },
        }
    };
    let hogbom_iteration_accounting = if matches!(&algorithm, ContinuumAlgorithm::Hogbom) {
        match config.hogbom_iteration_mode {
            super::HogbomIterationMode::Strict => HogbomIterationAccounting::Strict,
            super::HogbomIterationMode::CasaInclusive => HogbomIterationAccounting::CasaInclusive,
        }
    } else {
        HogbomIterationAccounting::Strict
    };
    let iterations = config.niter;
    let cycle_iterations = config.minor_cycle_length.min(iterations.max(1));
    Ok(ContinuumImagingRequest {
        measurement_set: config.ms.clone(),
        image_name: config.imagename.clone(),
        image_size: config.imsize,
        cell_arcsec: config.cell_arcsec,
        phase_center_field: config.phasecenter_field,
        phase_center: config.phasecenter.clone(),
        outlier_file: config.outlier_file.clone(),
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
        spectral_mode,
        continuum_subtraction: config.continuum_fit_spw.as_ref().map(|fit_spw| {
            VisibilityContinuumSubtraction {
                fit_spw: fit_spw.clone(),
                fit_order: config.continuum_fit_order,
            }
        }),
        data_column: config.datacolumn.clone(),
        algorithm,
        weighting: match config.weighting {
            WeightingMode::Natural => ContinuumWeighting::Natural,
            WeightingMode::Uniform => ContinuumWeighting::Uniform,
            WeightingMode::Briggs { robust } => ContinuumWeighting::Briggs(f64::from(robust)),
            WeightingMode::BriggsBwTaper { robust } => {
                ContinuumWeighting::BriggsBandwidthTaper(f64::from(robust))
            }
        },
        iterations,
        cycle_iterations,
        hogbom_iteration_accounting,
        maximum_major_cycles: config.nmajor,
        noise_sigma: (config.nsigma > 0.0).then_some(f64::from(config.nsigma)),
        cycle_factor: f64::from(config.cyclefactor),
        minimum_psf_fraction: f64::from(config.min_psf_fraction),
        maximum_psf_fraction: f64::from(config.max_psf_fraction),
        gain: f64::from(config.gain),
        threshold_jy: f64::from(config.threshold_jy),
        psf_cutoff: config.psf_cutoff,
        beam_policy: match config.restoring_beam_mode {
            RestoringBeamMode::PerPlane => ContinuumBeamPolicy::PerPlane,
            RestoringBeamMode::Common => ContinuumBeamPolicy::Common,
        },
        mask: match (&config.mask_image, config.use_mask) {
            (Some(path), CleanMaskMode::User) => ContinuumMask::Image(path.clone()),
            (_, CleanMaskMode::AutoMultiThreshold) => {
                ContinuumMask::AutoMultithresh(ContinuumAutoMaskControls {
                    sidelobe_factor: f64::from(config.auto_mask.sidelobe_threshold),
                    noise_factor: f64::from(config.auto_mask.noise_threshold),
                    low_noise_factor: f64::from(config.auto_mask.low_noise_threshold),
                    negative_factor: f64::from(config.auto_mask.negative_threshold),
                    minimum_beam_fraction: f64::from(config.auto_mask.min_beam_frac),
                    smooth_factor: f64::from(config.auto_mask.smooth_factor),
                    cut_threshold: f64::from(config.auto_mask.cut_threshold),
                    grow_iterations: config.auto_mask.grow_iterations,
                    minimum_percent_change: f64::from(config.auto_mask.min_percent_change),
                })
            }
            (None, CleanMaskMode::User) if config.mask_boxes.is_empty() => ContinuumMask::FullPlane,
            (None, CleanMaskMode::User) => ContinuumMask::Boxes(
                config
                    .mask_boxes
                    .iter()
                    .map(|region| ContinuumMaskBox {
                        blc: [region[0], region[1]],
                        trc: [region[2], region[3]],
                    })
                    .collect(),
            ),
        },
        save_model_column: config.save_model == SaveModelMode::ModelColumn,
        save_continuum_residual: config.save_continuum_residual,
        write_primary_beam: config.write_pb,
        pbcor: config.pbcor,
        task_requirements: task_requirements(config),
    })
}

fn task_requirements(config: &CliConfig) -> Vec<TaskRequirement> {
    let mut requirements = vec![match config.spectral_mode {
        SpectralMode::Mfs => TaskRequirement::SerialCpu,
        SpectralMode::Cube => TaskRequirement::SpectralCube,
        SpectralMode::Cubedata => TaskRequirement::SpectralCubedata,
    }];
    if config.aw_project.is_some() {
        requirements.push(TaskRequirement::AwProjection);
    } else if config.w_term_mode == WTermMode::WProject {
        requirements.push(TaskRequirement::WProjection);
    } else if config.use_pointing && !config.force_standard_gridder {
        requirements.push(TaskRequirement::MosaicGridder);
    }
    if config.use_mask == CleanMaskMode::AutoMultiThreshold {
        requirements.push(TaskRequirement::Automasking);
    }
    if config.use_mask == CleanMaskMode::AutoMultiThreshold
        || !config.mask_boxes.is_empty()
        || config.mask_image.is_some()
    {
        requirements.push(TaskRequirement::MaskProduct);
    }
    if config.start_model.is_some() {
        requirements.push(TaskRequirement::StartModel);
    }
    if config.save_model != SaveModelMode::None {
        requirements.push(TaskRequirement::ModelColumnWrite);
    }
    requirements.extend(backend_requirements(config));
    if unsupported_native_controls(config) {
        requirements.push(TaskRequirement::UnsupportedControls);
    }
    requirements
}

fn backend_requirements(config: &CliConfig) -> Vec<TaskRequirement> {
    let mut requirements = Vec::new();
    match config.standard_mfs_acceleration {
        StandardMfsAccelerationPolicy::Auto => {
            requirements.push(TaskRequirement::ExecutionAuto);
        }
        StandardMfsAccelerationPolicy::Cpu => {}
        StandardMfsAccelerationPolicy::MultiCpu => {
            requirements.push(TaskRequirement::FixedTileCpu);
        }
        StandardMfsAccelerationPolicy::Metal => {
            requirements.push(TaskRequirement::MetalRowRunGroupedGridder);
        }
    }
    if let Some(backend) = config.standard_mfs_backend.as_deref() {
        requirements.push(match backend {
            "cpu" | "serial" | "serial-cpu" => TaskRequirement::SerialCpu,
            "fixed-tile" | "fixed-tile-cpu" => TaskRequirement::FixedTileCpu,
            "metal" | "metal-gridder" => TaskRequirement::MetalGridder,
            "metal-row-run" | "metal-row-run-gridder" => TaskRequirement::MetalRowRunGridder,
            "metal-row-run-grouped" | "metal-row-run-grouped-gridder" => {
                TaskRequirement::MetalRowRunGroupedGridder
            }
            _ => TaskRequirement::UnsupportedControls,
        });
    }
    match config.imaging_fft_backend {
        ImagingFftBackendPolicy::RustFft => {}
        ImagingFftBackendPolicy::Auto => requirements.push(TaskRequirement::FftAuto),
        ImagingFftBackendPolicy::Accelerate => requirements.push(TaskRequirement::Accelerate),
        ImagingFftBackendPolicy::MetalMpsGraph => requirements.push(TaskRequirement::MetalMpsGraph),
        ImagingFftBackendPolicy::Fftw => requirements.push(TaskRequirement::Fftw),
    }
    requirements
}

fn unsupported_native_controls(config: &CliConfig) -> bool {
    let standard_mtmfs_products = matches!(config.deconvolver, Deconvolver::Mtmfs);
    config
        .correlation
        .as_deref()
        .is_some_and(|plane| !plane.eq_ignore_ascii_case("I"))
        || config.uv_taper.is_some()
        || config.fullsummary
        || ((config.pbcor || config.write_pb) && !standard_mtmfs_products)
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
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{
        CliConfig, HogbomIterationAccounting, StandardMfsAccelerationPolicy, TaskRequirement,
        application_request, backend_requirements, task_requirements,
    };

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
    fn automatic_backend_choices_remain_explicit_task_requirements() {
        assert_eq!(
            backend_requirements(&config(&[
                "--standard-mfs-acceleration",
                "auto",
                "--imaging-fft-backend",
                "auto",
            ])),
            vec![TaskRequirement::ExecutionAuto, TaskRequirement::FftAuto,]
        );
    }

    #[test]
    fn default_backend_choices_select_the_installed_native_cpu_implementation() {
        assert!(backend_requirements(&config(&[])).is_empty());
    }

    #[test]
    fn parallel_flags_select_serial_or_planned_multi_cpu_requirements() {
        let serial = config(&["--no-parallel"]);
        assert_eq!(
            serial.standard_mfs_acceleration,
            StandardMfsAccelerationPolicy::Cpu
        );
        assert_eq!(task_requirements(&serial), vec![TaskRequirement::SerialCpu]);

        let parallel = config(&["--parallel"]);
        assert_eq!(
            parallel.standard_mfs_acceleration,
            StandardMfsAccelerationPolicy::MultiCpu
        );
        assert_eq!(
            task_requirements(&parallel),
            vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu,]
        );

        assert_eq!(
            config(&["--parallel", "--no-parallel"]).standard_mfs_acceleration,
            StandardMfsAccelerationPolicy::Cpu
        );
    }

    #[test]
    fn issue_540_reduced_mfs_gate_uses_only_installed_controls() {
        let requirements = task_requirements(&config(&[
            "--field",
            "0",
            "--spw",
            "0",
            "--channel-start",
            "0",
            "--channel-count",
            "8",
            "--weighting",
            "briggs",
            "--robust",
            "0.5",
            "--dirty-only",
            "--standard-mfs-acceleration",
            "cpu",
            "--imaging-fft-backend",
            "rustfft",
            "--no-preview-pngs",
            "--gridder",
            "standard",
            "--deconvolver",
            "hogbom",
            "--specmode",
            "mfs",
            "--stokes",
            "I",
            "--datacolumn",
            "DATA",
            "--no-parallel",
        ]));

        assert!(!requirements.contains(&TaskRequirement::UnsupportedControls));
    }

    #[test]
    fn bounded_minor_cycles_are_a_supported_native_controller_control() {
        let requirements = task_requirements(&config(&[
            "--niter",
            "12",
            "--minor-cycle-length",
            "6",
            "--nmajor",
            "3",
        ]));
        assert!(!requirements.contains(&TaskRequirement::UnsupportedControls));
    }

    #[test]
    fn phase_center_and_outlier_file_are_transported_without_frontend_science() {
        let config = config(&[
            "--phasecenter",
            "J2000 19:59:28.500 +40.44.01.50",
            "--outlierfile",
            "outliers.txt",
        ]);
        let request = application_request(&config).expect("native multi-domain request");
        assert_eq!(
            request.phase_center.as_deref(),
            Some("J2000 19:59:28.500 +40.44.01.50")
        );
        assert_eq!(
            request.outlier_file.as_deref(),
            Some(std::path::Path::new("outliers.txt"))
        );
        assert!(!task_requirements(&config).contains(&TaskRequirement::UnsupportedControls));
    }

    #[test]
    fn casa_unlimited_nmajor_is_not_truncated_to_one_major_cycle() {
        let config = config(&[
            "--niter",
            "500",
            "--minor-cycle-length",
            "50",
            "--nmajor",
            "-1",
        ]);
        let request = application_request(&config).expect("native request");

        assert_eq!(request.maximum_major_cycles, None);
    }

    #[test]
    fn casa_inclusive_hogbom_mode_preserves_reported_budgets_and_selects_typed_policy() {
        let config = config(&[
            "--niter",
            "500",
            "--minor-cycle-length",
            "50",
            "--hogbom-iteration-mode",
            "casa-inclusive",
        ]);
        let request = application_request(&config).expect("native request");
        assert_eq!(request.iterations, 500);
        assert_eq!(request.cycle_iterations, 50);
        assert_eq!(
            request.hogbom_iteration_accounting,
            HogbomIterationAccounting::CasaInclusive
        );
        assert!(!task_requirements(&config).contains(&TaskRequirement::UnsupportedControls));
    }

    #[test]
    fn hogbom_iteration_mode_does_not_change_other_algorithm_budgets() {
        let config = config(&[
            "--deconvolver",
            "clark",
            "--niter",
            "12",
            "--minor-cycle-length",
            "6",
            "--hogbom-iteration-mode",
            "casa-inclusive",
        ]);
        let request = application_request(&config).expect("native Clark request");

        assert_eq!(request.iterations, 12);
        assert_eq!(request.cycle_iterations, 6);
        assert_eq!(
            request.hogbom_iteration_accounting,
            HogbomIterationAccounting::Strict
        );
    }

    #[test]
    fn metal_acceleration_requires_the_uninstalled_grouped_row_run_implementation() {
        assert_eq!(
            backend_requirements(&config(&["--standard-mfs-acceleration", "metal"])),
            vec![TaskRequirement::MetalRowRunGroupedGridder]
        );
    }
}
