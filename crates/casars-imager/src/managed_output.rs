// SPDX-License-Identifier: LGPL-3.0-or-later
//! Structured run report emitted for launcher-managed imaging runs.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::task_contract::{
    ImagerArtifactKind, ImagerDeconvolver, ImagerHogbomIterationMode, ImagerRestoringBeamMode,
    ImagerRunTaskResult, ImagerSaveModel, ImagerSpectralMode, ImagerWTermMode, ImagerWeighting,
};
use crate::{
    ChannelRunSummary, CliConfig, FrontendStageTimings, RunSummary, SpectralMode,
    canonical_deconvolver_name, canonical_hogbom_iteration_mode_name,
    canonical_restoring_beam_mode_name, canonical_spectral_mode_name, canonical_w_term_mode_name,
    canonical_weighting_name,
};

/// Structured imaging run report consumed by the `casars` workflow shell.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedImagingOutput {
    /// High-level request summary used by the shell overview.
    pub request: ManagedImagingRequest,
    /// Structured run metrics and per-channel diagnostics.
    pub run: ManagedImagingRun,
    /// Expected CASA image products written under the configured prefix.
    pub artifacts: Vec<ManagedImagingArtifact>,
}

/// Launcher-facing view of the requested imaging configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedImagingRequest {
    /// MeasurementSet path supplied for the run.
    pub measurement_set: String,
    /// Output CASA image prefix.
    pub imagename: String,
    /// Spectral imaging mode.
    pub spectral_mode: String,
    /// Requested visibility weighting.
    pub weighting: String,
    /// Requested minor-cycle deconvolver.
    pub deconvolver: String,
    /// Hogbom minor-cycle iteration accounting policy.
    pub hogbom_iteration_mode: String,
    /// Requested `w`-term handling mode.
    pub w_term_mode: String,
    /// Optional data-column override.
    pub data_column: Option<String>,
    /// Requested model persistence mode.
    pub save_model: String,
    /// Image size in pixels.
    pub imsize: usize,
    /// Cell size in arcseconds.
    pub cell_arcsec: f64,
    /// Whether the run skipped CLEAN.
    pub dirty_only: bool,
    /// Whether preview PNG sidecars were requested.
    pub write_preview_pngs: bool,
    /// Whether the primary-beam product was requested.
    pub write_pb: bool,
    /// Whether per-channel density estimation was requested for cube weighting.
    pub per_channel_weight_density: bool,
    /// Requested MTMFS Taylor-term count.
    pub nterms: usize,
    /// Output channel count for cube-like runs.
    pub output_channels: usize,
    /// Requested raw-correlation or Stokes plane, when explicitly selected.
    pub correlation: Option<String>,
    /// Requested restoring-beam mode for restored products.
    pub restoring_beam_mode: String,
}

/// Structured run metrics emitted after one successful imaging run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedImagingRun {
    /// Warnings emitted by the imaging run.
    pub warnings: Vec<String>,
    /// Number of scalar samples that reached the gridder.
    pub gridded_samples: usize,
    /// Total major-cycle count reported by the run.
    pub major_cycles: usize,
    /// Total minor-cycle component updates reported by the run.
    pub minor_iterations: usize,
    /// Final CLEAN stop reason when deconvolution ran.
    pub clean_stop_reason: Option<String>,
    /// Timing breakdown reported by the pure imaging core.
    pub stage_timings: ManagedImagingStageTimings,
    /// Timing breakdown for the MeasurementSet-backed frontend.
    pub frontend_timings: ManagedImagingStageTimings,
    /// Channel-level diagnostics for cube-like runs.
    pub channels: Vec<ManagedImagingChannelRun>,
}

/// Simple duration bundle serialized as nanoseconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedImagingStageTimings {
    /// Named stage durations in nanoseconds.
    pub values_ns: Vec<(String, u64)>,
}

/// Channel-level convergence summary emitted for cube-like runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedImagingChannelRun {
    /// Zero-based output channel index.
    pub channel_index: usize,
    /// Major-cycle count for this plane.
    pub major_cycles: usize,
    /// Minor-cycle component updates for this plane.
    pub minor_iterations: usize,
    /// Final CLEAN stop reason for this plane.
    pub clean_stop_reason: Option<String>,
    /// Peak residual before minor cycles.
    pub initial_residual_peak_jy_per_beam: f32,
    /// Peak residual after the final exact refresh.
    pub final_residual_peak_jy_per_beam: f32,
    /// Final CASA-style cycle threshold for this plane.
    pub final_cycle_threshold_jy_per_beam: f32,
    /// Whether the beam-fit debug summary was available for this plane.
    pub beam_fit_available: bool,
}

/// One expected output artifact written by the imaging run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedImagingArtifact {
    /// Stable artifact kind identifier such as `psf`, `image`, or `alpha`.
    pub kind: String,
    /// Human-readable artifact label.
    pub label: String,
    /// On-disk path for the CASA image product.
    pub path: String,
    /// Whether that product exists after the run.
    pub exists: bool,
    /// Optional preview sidecar path.
    pub preview_png_path: Option<String>,
    /// Whether the preview sidecar exists after the run.
    pub preview_png_exists: bool,
}

impl ManagedImagingOutput {
    /// Build the structured launcher report from one completed run.
    pub fn from_run(config: &CliConfig, summary: &RunSummary) -> Self {
        Self {
            request: ManagedImagingRequest {
                measurement_set: config.ms.display().to_string(),
                imagename: config.imagename.display().to_string(),
                spectral_mode: canonical_spectral_mode_name(config.spectral_mode).to_string(),
                weighting: canonical_weighting_name(config.weighting),
                deconvolver: canonical_deconvolver_name(config.deconvolver).to_string(),
                hogbom_iteration_mode: canonical_hogbom_iteration_mode_name(
                    config.hogbom_iteration_mode,
                )
                .to_string(),
                w_term_mode: canonical_w_term_mode_name(config.w_term_mode).to_string(),
                data_column: config.datacolumn.clone(),
                save_model: match config.save_model {
                    crate::SaveModelMode::None => "none",
                    crate::SaveModelMode::ModelColumn => "modelcolumn",
                }
                .to_string(),
                imsize: config.imsize,
                cell_arcsec: config.cell_arcsec,
                dirty_only: config.dirty_only,
                write_preview_pngs: config.write_preview_pngs,
                write_pb: config.write_pb,
                per_channel_weight_density: config.per_channel_weight_density,
                nterms: config.nterms,
                output_channels: summary.channel_summaries.len(),
                correlation: config.correlation.clone(),
                restoring_beam_mode: canonical_restoring_beam_mode_name(config.restoring_beam_mode)
                    .to_string(),
            },
            run: ManagedImagingRun {
                warnings: summary.warnings.clone(),
                gridded_samples: summary.gridded_samples,
                major_cycles: summary.major_cycles,
                minor_iterations: summary.minor_iterations,
                clean_stop_reason: summary
                    .clean_stop_reason
                    .map(|reason| format!("{reason:?}")),
                stage_timings: stage_timings_from_core(summary),
                frontend_timings: stage_timings_from_frontend(summary.frontend_timings),
                channels: summary
                    .channel_summaries
                    .iter()
                    .map(channel_run_from_summary)
                    .collect(),
            },
            artifacts: imaging_artifacts(config),
        }
    }

    /// Build the launcher report from the canonical task result.
    pub fn from_task_result(result: &ImagerRunTaskResult) -> Self {
        let request = &result.request;
        Self {
            request: ManagedImagingRequest {
                measurement_set: request.measurement_set.display().to_string(),
                imagename: request.image_name.display().to_string(),
                spectral_mode: match request.spectral_mode {
                    ImagerSpectralMode::Mfs => "mfs".to_string(),
                    ImagerSpectralMode::Cube => "cube".to_string(),
                    ImagerSpectralMode::Cubedata => "cubedata".to_string(),
                },
                weighting: match &request.weighting {
                    ImagerWeighting::Natural => "natural".to_string(),
                    ImagerWeighting::Uniform => "uniform".to_string(),
                    ImagerWeighting::Briggs { robust } => format!("briggs:{robust}"),
                    ImagerWeighting::BriggsBwTaper { robust } => {
                        format!("briggsbwtaper:{robust}")
                    }
                },
                deconvolver: match request.deconvolver {
                    ImagerDeconvolver::Hogbom => "hogbom".to_string(),
                    ImagerDeconvolver::Mtmfs => "mtmfs".to_string(),
                    ImagerDeconvolver::Clark => "clark".to_string(),
                    ImagerDeconvolver::Multiscale => "multiscale".to_string(),
                },
                hogbom_iteration_mode: match request.hogbom_iteration_mode {
                    ImagerHogbomIterationMode::Strict => "strict".to_string(),
                    ImagerHogbomIterationMode::CasaInclusive => "casa".to_string(),
                },
                w_term_mode: match request.w_term_mode {
                    ImagerWTermMode::None => "none".to_string(),
                    ImagerWTermMode::Direct => "direct".to_string(),
                    ImagerWTermMode::Wproject => "wproject".to_string(),
                },
                data_column: request.data_column.clone(),
                save_model: match request.save_model {
                    ImagerSaveModel::None => "none",
                    ImagerSaveModel::ModelColumn => "modelcolumn",
                }
                .to_string(),
                imsize: request.image_size,
                cell_arcsec: request.cell_arcsec,
                dirty_only: request.dirty_only,
                write_preview_pngs: request.write_preview_pngs,
                write_pb: request.write_pb,
                per_channel_weight_density: managed_request_per_channel_weight_density(request),
                nterms: request.nterms,
                output_channels: result.run.channels.len(),
                correlation: request
                    .correlation
                    .map(|value| value.as_cli_text().to_string()),
                restoring_beam_mode: match request.restoring_beam_mode {
                    ImagerRestoringBeamMode::PerPlane => "per_plane".to_string(),
                    ImagerRestoringBeamMode::Common => "common".to_string(),
                },
            },
            run: ManagedImagingRun {
                warnings: result.run.warnings.clone(),
                gridded_samples: result.run.gridded_samples,
                major_cycles: result.run.major_cycles,
                minor_iterations: result.run.minor_iterations,
                clean_stop_reason: result
                    .run
                    .clean_stop_reason
                    .map(|reason| format!("{reason:?}")),
                stage_timings: ManagedImagingStageTimings {
                    values_ns: vec![
                        (
                            "controller_total".to_string(),
                            result.run.stage_timings.total_ns,
                        ),
                        (
                            "controller_overhead".to_string(),
                            result.run.stage_timings.controller_overhead_ns,
                        ),
                        (
                            "weighting".to_string(),
                            result.run.stage_timings.weighting_ns,
                        ),
                        (
                            "executor_build".to_string(),
                            result.run.stage_timings.executor_build_ns,
                        ),
                        (
                            "major_cycle_refresh".to_string(),
                            result.run.stage_timings.major_cycle_refresh_ns,
                        ),
                        (
                            "psf_grid_alloc".to_string(),
                            result.run.stage_timings.psf_grid_alloc_ns,
                        ),
                        (
                            "planned_sample_replay".to_string(),
                            result.run.stage_timings.planned_sample_replay_ns,
                        ),
                        (
                            "grid_update".to_string(),
                            result.run.stage_timings.grid_update_ns,
                        ),
                        ("psf_grid".to_string(), result.run.stage_timings.psf_grid_ns),
                        ("psf_fft".to_string(), result.run.stage_timings.psf_fft_ns),
                        (
                            "psf_image_correction".to_string(),
                            result.run.stage_timings.psf_image_correction_ns,
                        ),
                        (
                            "psf_normalize".to_string(),
                            result.run.stage_timings.psf_normalize_ns,
                        ),
                        (
                            "model_fft".to_string(),
                            result.run.stage_timings.model_fft_ns,
                        ),
                        (
                            "residual_grid_alloc".to_string(),
                            result.run.stage_timings.residual_grid_alloc_ns,
                        ),
                        (
                            "residual_degrid_grid".to_string(),
                            result.run.stage_timings.residual_degrid_grid_ns,
                        ),
                        (
                            "residual_fft".to_string(),
                            result.run.stage_timings.residual_fft_ns,
                        ),
                        (
                            "residual_image_correction".to_string(),
                            result.run.stage_timings.residual_image_correction_ns,
                        ),
                        (
                            "residual_normalize".to_string(),
                            result.run.stage_timings.residual_normalize_ns,
                        ),
                        (
                            "minor_cycle".to_string(),
                            result.run.stage_timings.minor_cycle_ns,
                        ),
                        (
                            "minor_cycle_solve".to_string(),
                            result.run.stage_timings.minor_cycle_solve_ns,
                        ),
                        ("beam_fit".to_string(), result.run.stage_timings.beam_fit_ns),
                        ("restore".to_string(), result.run.stage_timings.restore_ns),
                    ],
                },
                frontend_timings: ManagedImagingStageTimings {
                    values_ns: vec![
                        (
                            "open_measurement_set".to_string(),
                            result.run.frontend_timings.open_measurement_set_ns,
                        ),
                        (
                            "prepare_plane_input".to_string(),
                            result.run.frontend_timings.prepare_plane_input_ns,
                        ),
                        (
                            "extract_phase_center".to_string(),
                            result.run.frontend_timings.extract_phase_center_ns,
                        ),
                        (
                            "run_imaging".to_string(),
                            result.run.frontend_timings.run_imaging_ns,
                        ),
                        (
                            "build_coordinate_system".to_string(),
                            result.run.frontend_timings.build_coordinate_system_ns,
                        ),
                        (
                            "write_products".to_string(),
                            result.run.frontend_timings.write_products_ns,
                        ),
                        ("total".to_string(), result.run.frontend_timings.total_ns),
                    ],
                },
                channels: result
                    .run
                    .channels
                    .iter()
                    .map(|summary| ManagedImagingChannelRun {
                        channel_index: summary.channel_index,
                        major_cycles: summary.major_cycles,
                        minor_iterations: summary.minor_iterations,
                        clean_stop_reason: summary
                            .clean_stop_reason
                            .map(|reason| format!("{reason:?}")),
                        initial_residual_peak_jy_per_beam: summary
                            .initial_residual_peak_jy_per_beam,
                        final_residual_peak_jy_per_beam: summary.final_residual_peak_jy_per_beam,
                        final_cycle_threshold_jy_per_beam: summary
                            .final_cycle_threshold_jy_per_beam,
                        beam_fit_available: summary.beam_fit_available,
                    })
                    .collect(),
            },
            artifacts: result
                .artifacts
                .iter()
                .map(|artifact| ManagedImagingArtifact {
                    kind: match artifact.kind {
                        ImagerArtifactKind::Psf => "psf".to_string(),
                        ImagerArtifactKind::Residual => "residual".to_string(),
                        ImagerArtifactKind::Model => "model".to_string(),
                        ImagerArtifactKind::Image => "image".to_string(),
                        ImagerArtifactKind::Mask => "mask".to_string(),
                        ImagerArtifactKind::Weight => "weight".to_string(),
                        ImagerArtifactKind::PrimaryBeam => "pb".to_string(),
                        ImagerArtifactKind::ImagePbcor => "image.pbcor".to_string(),
                        ImagerArtifactKind::Alpha => "alpha".to_string(),
                    },
                    label: artifact.label.clone(),
                    path: artifact.path.clone(),
                    exists: artifact.exists,
                    preview_png_path: artifact.preview_png_path.clone(),
                    preview_png_exists: artifact.preview_png_exists,
                })
                .collect(),
        }
    }
}

fn stage_timings_from_core(summary: &RunSummary) -> ManagedImagingStageTimings {
    ManagedImagingStageTimings {
        values_ns: vec![
            (
                "controller_total".to_string(),
                summary.stage_timings.total.as_nanos() as u64,
            ),
            (
                "controller_overhead".to_string(),
                summary.stage_timings.controller_overhead.as_nanos() as u64,
            ),
            (
                "weighting".to_string(),
                summary.stage_timings.weighting.as_nanos() as u64,
            ),
            (
                "executor_build".to_string(),
                summary.stage_timings.executor_build.as_nanos() as u64,
            ),
            (
                "major_cycle_refresh".to_string(),
                summary.stage_timings.major_cycle_refresh.as_nanos() as u64,
            ),
            (
                "residual_refresh_overhead".to_string(),
                summary.stage_timings.residual_refresh_overhead.as_nanos() as u64,
            ),
            (
                "clean_cycle_setup".to_string(),
                summary.stage_timings.clean_cycle_setup.as_nanos() as u64,
            ),
            (
                "deconvolver_setup".to_string(),
                summary.stage_timings.deconvolver_setup.as_nanos() as u64,
            ),
            (
                "multiscale_scale_refresh".to_string(),
                summary.stage_timings.multiscale_scale_refresh.as_nanos() as u64,
            ),
            (
                "psf_grid_alloc".to_string(),
                summary.stage_timings.psf_grid_alloc.as_nanos() as u64,
            ),
            (
                "planned_sample_replay".to_string(),
                summary.stage_timings.planned_sample_replay.as_nanos() as u64,
            ),
            (
                "grid_update".to_string(),
                summary.stage_timings.grid_update.as_nanos() as u64,
            ),
            (
                "psf_grid".to_string(),
                summary.stage_timings.psf_grid.as_nanos() as u64,
            ),
            (
                "psf_fft".to_string(),
                summary.stage_timings.psf_fft.as_nanos() as u64,
            ),
            (
                "psf_image_correction".to_string(),
                summary.stage_timings.psf_image_correction.as_nanos() as u64,
            ),
            (
                "psf_normalize".to_string(),
                summary.stage_timings.psf_normalize.as_nanos() as u64,
            ),
            (
                "model_fft".to_string(),
                summary.stage_timings.model_fft.as_nanos() as u64,
            ),
            (
                "residual_grid_alloc".to_string(),
                summary.stage_timings.residual_grid_alloc.as_nanos() as u64,
            ),
            (
                "residual_degrid_grid".to_string(),
                summary.stage_timings.residual_degrid_grid.as_nanos() as u64,
            ),
            (
                "residual_fft".to_string(),
                summary.stage_timings.residual_fft.as_nanos() as u64,
            ),
            (
                "residual_image_correction".to_string(),
                summary.stage_timings.residual_image_correction.as_nanos() as u64,
            ),
            (
                "residual_normalize".to_string(),
                summary.stage_timings.residual_normalize.as_nanos() as u64,
            ),
            (
                "minor_cycle".to_string(),
                summary.stage_timings.minor_cycle.as_nanos() as u64,
            ),
            (
                "minor_cycle_solve".to_string(),
                summary.stage_timings.minor_cycle_solve.as_nanos() as u64,
            ),
            (
                "beam_fit".to_string(),
                summary.stage_timings.beam_fit.as_nanos() as u64,
            ),
            (
                "restore".to_string(),
                summary.stage_timings.restore.as_nanos() as u64,
            ),
        ],
    }
}

fn stage_timings_from_frontend(timings: FrontendStageTimings) -> ManagedImagingStageTimings {
    ManagedImagingStageTimings {
        values_ns: vec![
            (
                "open_measurement_set".to_string(),
                timings.open_measurement_set.as_nanos() as u64,
            ),
            (
                "prepare_plane_input".to_string(),
                timings.prepare_plane_input.as_nanos() as u64,
            ),
            (
                "extract_phase_center".to_string(),
                timings.extract_phase_center.as_nanos() as u64,
            ),
            (
                "run_imaging".to_string(),
                timings.run_imaging.as_nanos() as u64,
            ),
            (
                "build_coordinate_system".to_string(),
                timings.build_coordinate_system.as_nanos() as u64,
            ),
            (
                "write_products".to_string(),
                timings.write_products.as_nanos() as u64,
            ),
            ("total".to_string(), timings.total.as_nanos() as u64),
        ],
    }
}

fn managed_request_per_channel_weight_density(
    request: &crate::task_contract::ImagerRunTaskRequest,
) -> bool {
    request
        .per_channel_weight_density
        .unwrap_or(matches!(request.spectral_mode, ImagerSpectralMode::Cube))
}

fn channel_run_from_summary(summary: &ChannelRunSummary) -> ManagedImagingChannelRun {
    ManagedImagingChannelRun {
        channel_index: summary.channel_index,
        major_cycles: summary.major_cycles,
        minor_iterations: summary.minor_iterations,
        clean_stop_reason: summary
            .clean_stop_reason
            .map(|reason| format!("{reason:?}")),
        initial_residual_peak_jy_per_beam: summary.initial_residual_peak_jy_per_beam,
        final_residual_peak_jy_per_beam: summary.final_residual_peak_jy_per_beam,
        final_cycle_threshold_jy_per_beam: summary.final_cycle_threshold_jy_per_beam,
        beam_fit_available: summary.beam_fit_debug.is_some(),
    }
}

fn imaging_artifacts(config: &CliConfig) -> Vec<ManagedImagingArtifact> {
    let base = config.imagename.to_string_lossy().to_string();
    let mut artifacts = Vec::new();
    match config.spectral_mode {
        SpectralMode::Mfs
            if canonical_deconvolver_name(config.deconvolver) == "mtmfs" && config.nterms > 1 =>
        {
            for term in 0..config.nterms {
                for (kind, label) in [
                    ("psf", "PSF"),
                    ("residual", "Residual"),
                    ("model", "Model"),
                    ("image", "Restored Image"),
                ] {
                    let suffix = format!("{kind}.tt{term}");
                    let path = PathBuf::from(format!("{base}.{suffix}"));
                    let preview = (term == 0 && config.write_preview_pngs)
                        .then(|| PathBuf::from(format!("{base}.{suffix}.png")));
                    artifacts.push(artifact(label_for_term(label, term), kind, path, preview));
                }
            }
            let alpha = PathBuf::from(format!("{base}.alpha"));
            let alpha_preview = config
                .write_preview_pngs
                .then(|| PathBuf::from(format!("{base}.alpha.png")));
            artifacts.push(artifact(
                "Spectral Index".to_string(),
                "alpha",
                alpha,
                alpha_preview,
            ));
        }
        _ => {
            for (kind, label) in [
                ("psf", "PSF"),
                ("residual", "Residual"),
                ("model", "Model"),
                ("image", "Restored Image"),
            ] {
                let path = PathBuf::from(format!("{base}.{kind}"));
                let preview = config
                    .write_preview_pngs
                    .then(|| PathBuf::from(format!("{base}.{kind}.png")));
                artifacts.push(artifact(label.to_string(), kind, path, preview));
            }
            if config.write_pb || config.pbcor {
                let path = PathBuf::from(format!("{base}.pb"));
                let preview = config
                    .write_preview_pngs
                    .then(|| PathBuf::from(format!("{base}.pb.png")));
                artifacts.push(artifact("Primary Beam".to_string(), "pb", path, preview));
            }
            if config.pbcor {
                let path = PathBuf::from(format!("{base}.image.pbcor"));
                let preview = config
                    .write_preview_pngs
                    .then(|| PathBuf::from(format!("{base}.image.pbcor.png")));
                artifacts.push(artifact(
                    "PB-corrected Image".to_string(),
                    "image.pbcor",
                    path,
                    preview,
                ));
            }
        }
    }
    artifacts
}

fn label_for_term(base: &str, term: usize) -> String {
    format!("{base} TT{term}")
}

fn artifact(
    label: String,
    kind: &str,
    path: PathBuf,
    preview: Option<PathBuf>,
) -> ManagedImagingArtifact {
    ManagedImagingArtifact {
        kind: kind.to_string(),
        label,
        exists: path.exists(),
        path: path.display().to_string(),
        preview_png_exists: preview.as_ref().is_some_and(|path| path.exists()),
        preview_png_path: preview.map(|path| path.display().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{ManagedImagingOutput, artifact, imaging_artifacts, label_for_term};
    use crate::task_contract::{
        ImagerArtifact, ImagerArtifactKind, ImagerAutoMultiThresholdConfig, ImagerCleanMaskMode,
        ImagerCleanStopReason, ImagerDeconvolver, ImagerHogbomIterationMode, ImagerPlaneSelection,
        ImagerRestoringBeamMode, ImagerRunReport, ImagerRunTaskRequest, ImagerRunTaskResult,
        ImagerSaveModel, ImagerSpectralMode, ImagerWTermMode, ImagerWeighting,
    };
    use crate::{
        AutoMultiThresholdConfig, ChannelRunSummary, CleanMaskMode, CliConfig, CubeAxisConfig,
        FrontendStageTimings, ImagingFftBackendPolicy, ImagingFftPrecisionPolicy, RunSummary,
        SaveModelMode, SpectralMode, StandardMfsAccelerationPolicy,
    };
    use casa_imaging::{
        CleanStopReason, Deconvolver, HogbomIterationMode, ImagingStageTimings, RestoringBeamMode,
        WTermMode, WeightingMode,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::tempdir;

    fn sample_cli_config(imagename: PathBuf) -> CliConfig {
        CliConfig {
            ms: PathBuf::from("/tmp/demo.ms"),
            imagename,
            imsize: 256,
            cell_arcsec: 1.5,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: Some("CORRECTED_DATA".to_string()),
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: true,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::Common,
            deconvolver: Deconvolver::Mtmfs,
            nterms: 2,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 50,
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.1,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 1000,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::Strict,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::Direct,
            force_standard_gridder: false,
            w_project_planes: None,
            dirty_only: false,
            chanchunks: None,
            standard_mfs_acceleration: StandardMfsAccelerationPolicy::Auto,
            standard_mfs_backend: None,
            standard_mfs_grid_threads: None,
            standard_mfs_tile_anchor: None,
            standard_mfs_residual_backend: None,
            standard_mfs_initial_dirty_backend: None,
            standard_mfs_metal_minor_cycle_chunk: None,
            standard_mfs_metal_grouped_input_cache: None,
            standard_mfs_memory_target_mb: None,
            standard_mfs_prepare_buffer_mb: None,
            imaging_memory_target_mb: None,
            imaging_prepare_buffer_mb: None,
            imaging_row_block_rows: None,
            imaging_prepare_workers: None,
            imaging_read_ahead_blocks: None,
            imaging_fft_precision: ImagingFftPrecisionPolicy::Auto,
            imaging_fft_backend: ImagingFftBackendPolicy::Auto,
            write_preview_pngs: true,
        }
    }

    fn sample_run_summary() -> RunSummary {
        let stage_timings = ImagingStageTimings {
            controller_overhead: Duration::from_nanos(11),
            weighting: Duration::from_nanos(12),
            executor_build: Duration::from_nanos(28),
            psf_grid_alloc: Duration::from_nanos(29),
            planned_sample_replay: Duration::from_nanos(33),
            grid_update: Duration::from_nanos(34),
            psf_grid: Duration::from_nanos(13),
            psf_fft: Duration::from_nanos(14),
            psf_image_correction: Duration::from_nanos(30),
            psf_normalize: Duration::from_nanos(15),
            model_fft: Duration::from_nanos(16),
            residual_grid_alloc: Duration::from_nanos(31),
            residual_degrid_grid: Duration::from_nanos(15),
            residual_fft: Duration::from_nanos(16),
            residual_image_correction: Duration::from_nanos(32),
            residual_normalize: Duration::from_nanos(17),
            clean_cycle_setup: Duration::from_nanos(24),
            deconvolver_setup: Duration::from_nanos(25),
            minor_cycle: Duration::from_nanos(18),
            minor_cycle_solve: Duration::from_nanos(19),
            deconvolver_peak_search: Duration::from_nanos(35),
            deconvolver_active_set_build: Duration::from_nanos(36),
            deconvolver_model_update: Duration::from_nanos(37),
            deconvolver_psf_subtract: Duration::from_nanos(38),
            deconvolver_residual_replay: Duration::from_nanos(39),
            deconvolver_fft_convolve: Duration::from_nanos(40),
            deconvolver_peak_searches: 41,
            deconvolver_model_updates: 42,
            deconvolver_subtract_updates: 43,
            deconvolver_pixels_searched: 44,
            deconvolver_pixels_touched: 45,
            deconvolver_full_window_peak_searches: 46,
            deconvolver_full_window_subtract_updates: 47,
            deconvolver_peak_search_window_pixels_max: 48,
            deconvolver_subtract_window_pixels_max: 49,
            major_cycle_refresh: Duration::from_nanos(20),
            residual_refresh_overhead: Duration::from_nanos(26),
            multiscale_scale_refresh: Duration::from_nanos(27),
            beam_fit: Duration::from_nanos(22),
            restore: Duration::from_nanos(23),
            total: Duration::from_nanos(21),
        };

        RunSummary {
            warnings: vec!["warning-a".to_string()],
            gridded_samples: 42,
            major_cycles: 3,
            minor_iterations: 9,
            clean_stop_reason: Some(CleanStopReason::CycleThresholdReached),
            minor_cycle_traces: Vec::new(),
            channel_summaries: vec![ChannelRunSummary {
                channel_index: 7,
                major_cycles: 4,
                minor_iterations: 10,
                clean_stop_reason: Some(CleanStopReason::IterationLimitReached),
                initial_residual_peak_jy_per_beam: 3.5,
                final_residual_peak_jy_per_beam: 1.5,
                final_cycle_threshold_jy_per_beam: 0.25,
                minor_cycle_traces: Vec::new(),
                beam_fit_debug: None,
            }],
            stage_timings,
            frontend_timings: FrontendStageTimings {
                open_measurement_set: Duration::from_nanos(31),
                prepare_plane_input: Duration::from_nanos(32),
                get_ms_values_into_processing_buffer: Duration::from_nanos(38),
                prepare_processing_buffer: Duration::from_nanos(39),
                extract_phase_center: Duration::from_nanos(33),
                run_imaging: Duration::from_nanos(34),
                build_coordinate_system: Duration::from_nanos(35),
                write_products: Duration::from_nanos(36),
                total: Duration::from_nanos(37),
            },
        }
    }

    #[test]
    fn from_run_reports_mtmfs_artifacts_and_preview_sidecars() {
        let tempdir = tempdir().unwrap();
        let imagename = tempdir.path().join("managed-output");
        fs::write(imagename.with_extension("psf.tt0"), b"psf").unwrap();
        fs::write(imagename.with_extension("psf.tt0.png"), b"png").unwrap();
        fs::write(imagename.with_extension("alpha"), b"alpha").unwrap();
        fs::write(imagename.with_extension("alpha.png"), b"png").unwrap();

        let output =
            ManagedImagingOutput::from_run(&sample_cli_config(imagename), &sample_run_summary());

        assert_eq!(output.request.spectral_mode, "mfs");
        assert_eq!(output.request.weighting, "natural");
        assert_eq!(output.request.deconvolver, "mtmfs");
        assert_eq!(output.request.w_term_mode, "direct");
        assert_eq!(output.request.restoring_beam_mode, "common");
        assert_eq!(output.request.output_channels, 1);
        assert_eq!(output.request.correlation.as_deref(), Some("XX"));

        assert_eq!(
            output.run.clean_stop_reason.as_deref(),
            Some("CycleThresholdReached")
        );
        assert_eq!(
            output.run.stage_timings.values_ns[0],
            ("controller_total".to_string(), 21)
        );
        let stage_timings = output
            .run
            .stage_timings
            .values_ns
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(stage_timings["executor_build"], 28);
        assert_eq!(stage_timings["psf_normalize"], 15);
        assert_eq!(stage_timings["model_fft"], 16);
        assert_eq!(stage_timings["minor_cycle"], 18);
        assert_eq!(stage_timings["minor_cycle_solve"], 19);
        assert_eq!(stage_timings["major_cycle_refresh"], 20);
        assert_eq!(stage_timings["beam_fit"], 22);
        assert_eq!(stage_timings["restore"], 23);
        assert_eq!(
            output.run.frontend_timings.values_ns[0],
            ("open_measurement_set".to_string(), 31)
        );
        assert_eq!(output.run.channels.len(), 1);
        assert_eq!(output.run.channels[0].channel_index, 7);
        assert_eq!(
            output.run.channels[0].clean_stop_reason.as_deref(),
            Some("IterationLimitReached")
        );
        assert!(!output.run.channels[0].beam_fit_available);

        assert_eq!(output.artifacts.len(), 9);
        assert_eq!(output.artifacts[0].label, "PSF TT0");
        assert_eq!(output.artifacts[0].kind, "psf");
        assert!(output.artifacts[0].exists);
        assert!(output.artifacts[0].preview_png_exists);
        assert!(
            output.artifacts[0]
                .preview_png_path
                .as_deref()
                .unwrap()
                .ends_with(".psf.tt0.png")
        );
        assert_eq!(output.artifacts[4].label, "PSF TT1");
        assert_eq!(output.artifacts[4].preview_png_path, None);
        assert_eq!(output.artifacts[8].label, "Spectral Index");
        assert_eq!(output.artifacts[8].kind, "alpha");
        assert!(output.artifacts[8].exists);
        assert!(output.artifacts[8].preview_png_exists);
    }

    #[test]
    fn from_task_result_serializes_contract_values() {
        let result = ImagerRunTaskResult {
            request: ImagerRunTaskRequest {
                measurement_set: PathBuf::from("/tmp/from-task.ms"),
                image_name: PathBuf::from("/tmp/from-task"),
                image_size: 512,
                cell_arcsec: 2.5,
                field_ids: Some(vec![3]),
                phasecenter_field: None,
                phasecenter: Some("J2000 00:00:00.0 +00.00.00.0".to_string()),
                ddid: Some(1),
                spw_selector: Some("2".to_string()),
                channel_start: Some(4),
                channel_count: Some(8),
                data_column: Some("MODEL_DATA".to_string()),
                save_model: ImagerSaveModel::ModelColumn,
                start_model: None,
                outlier_file: None,
                correlation: Some(ImagerPlaneSelection::CorrXX),
                spectral_mode: ImagerSpectralMode::Cube,
                cube_axis: Default::default(),
                weighting: ImagerWeighting::Briggs { robust: -0.25 },
                per_channel_weight_density: Some(true),
                use_pointing: true,
                uv_taper: None,
                restoring_beam_mode: ImagerRestoringBeamMode::PerPlane,
                deconvolver: ImagerDeconvolver::Clark,
                nterms: 1,
                multiscale_scales: vec![0.0, 5.0],
                small_scale_bias: 0.3,
                niter: 100,
                nmajor: Some(4),
                fullsummary: true,
                gain: 0.2,
                threshold_jy: 0.01,
                nsigma: 5.0,
                psf_cutoff: 0.4,
                mosaic_pb_limit: 0.1,
                pbcor: false,
                write_pb: false,
                minor_cycle_length: 16,
                cyclefactor: 1.2,
                min_psf_fraction: 0.15,
                max_psf_fraction: 0.9,
                hogbom_iteration_mode: ImagerHogbomIterationMode::Strict,
                use_mask: ImagerCleanMaskMode::User,
                auto_mask: ImagerAutoMultiThresholdConfig::default(),
                mask_boxes: vec![[1, 2, 3, 4]],
                mask_image: None,
                w_term_mode: ImagerWTermMode::Wproject,
                force_standard_gridder: true,
                w_project_planes: Some(32),
                dirty_only: true,
                parallel: None,
                chanchunks: None,
                standard_mfs_acceleration: StandardMfsAccelerationPolicy::Auto,
                standard_mfs_backend: None,
                standard_mfs_grid_threads: None,
                standard_mfs_tile_anchor: None,
                standard_mfs_residual_backend: None,
                standard_mfs_initial_dirty_backend: None,
                standard_mfs_metal_minor_cycle_chunk: None,
                standard_mfs_metal_grouped_input_cache: None,
                standard_mfs_memory_target_mb: None,
                standard_mfs_prepare_buffer_mb: None,
                imaging_memory_target_mb: None,
                imaging_prepare_buffer_mb: None,
                imaging_row_block_rows: None,
                imaging_prepare_workers: None,
                imaging_read_ahead_blocks: None,
                imaging_fft_precision: ImagingFftPrecisionPolicy::Auto,
                imaging_fft_backend: ImagingFftBackendPolicy::Auto,
                write_preview_pngs: false,
                progress: None,
            },
            run: ImagerRunReport {
                warnings: vec!["watch residuals".to_string()],
                gridded_samples: 1024,
                major_cycles: 6,
                minor_iterations: 24,
                iterdone: 24,
                nmajordone: 6,
                stopcode: 10,
                clean_stop_reason: Some(ImagerCleanStopReason::DivergenceDetected),
                summaryminor: Vec::new(),
                stage_timings: crate::ImagerCoreStageTimings {
                    controller_overhead_ns: 1,
                    weighting_ns: 2,
                    executor_build_ns: 3,
                    psf_grid_alloc_ns: 17,
                    planned_sample_replay_ns: 21,
                    grid_update_ns: 22,
                    psf_grid_ns: 4,
                    psf_fft_ns: 5,
                    psf_image_correction_ns: 18,
                    psf_normalize_ns: 6,
                    model_fft_ns: 7,
                    residual_grid_alloc_ns: 19,
                    residual_degrid_grid_ns: 8,
                    residual_fft_ns: 9,
                    residual_image_correction_ns: 20,
                    residual_normalize_ns: 10,
                    minor_cycle_ns: 11,
                    minor_cycle_solve_ns: 12,
                    major_cycle_refresh_ns: 13,
                    beam_fit_ns: 14,
                    restore_ns: 15,
                    total_ns: 16,
                },
                frontend_timings: crate::ImagerFrontendTaskStageTimings {
                    open_measurement_set_ns: 16,
                    prepare_plane_input_ns: 17,
                    extract_phase_center_ns: 18,
                    run_imaging_ns: 19,
                    build_coordinate_system_ns: 20,
                    write_products_ns: 21,
                    total_ns: 22,
                },
                channels: vec![crate::ImagerChannelRunResult {
                    channel_index: 2,
                    major_cycles: 5,
                    minor_iterations: 9,
                    clean_stop_reason: Some(ImagerCleanStopReason::NoCleanablePixels),
                    initial_residual_peak_jy_per_beam: 4.0,
                    final_residual_peak_jy_per_beam: 2.0,
                    final_cycle_threshold_jy_per_beam: 0.4,
                    beam_fit_available: true,
                }],
            },
            artifacts: vec![ImagerArtifact {
                kind: ImagerArtifactKind::Alpha,
                label: "Spectral Index".to_string(),
                path: "/tmp/from-task.alpha".to_string(),
                exists: true,
                preview_png_path: Some("/tmp/from-task.alpha.png".to_string()),
                preview_png_exists: false,
            }],
        };

        let output = ManagedImagingOutput::from_task_result(&result);

        assert_eq!(output.request.spectral_mode, "cube");
        assert_eq!(output.request.weighting, "briggs:-0.25");
        assert_eq!(output.request.deconvolver, "clark");
        assert_eq!(output.request.w_term_mode, "wproject");
        assert_eq!(output.request.save_model, "modelcolumn");
        assert_eq!(output.request.restoring_beam_mode, "per_plane");
        assert_eq!(output.request.output_channels, 1);
        assert_eq!(output.request.correlation.as_deref(), Some("XX"));
        assert!(output.request.dirty_only);
        assert!(!output.request.write_preview_pngs);

        assert_eq!(
            output.run.clean_stop_reason.as_deref(),
            Some("DivergenceDetected")
        );
        assert_eq!(
            output.run.stage_timings.values_ns[0],
            ("controller_total".to_string(), 16)
        );
        let stage_timings = output
            .run
            .stage_timings
            .values_ns
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(stage_timings["executor_build"], 3);
        assert_eq!(stage_timings["psf_normalize"], 6);
        assert_eq!(stage_timings["model_fft"], 7);
        assert_eq!(stage_timings["minor_cycle"], 11);
        assert_eq!(stage_timings["minor_cycle_solve"], 12);
        assert_eq!(stage_timings["major_cycle_refresh"], 13);
        assert_eq!(stage_timings["beam_fit"], 14);
        assert_eq!(stage_timings["restore"], 15);
        assert_eq!(
            output.run.frontend_timings.values_ns[6],
            ("total".to_string(), 22)
        );
        assert!(output.run.channels[0].beam_fit_available);
        assert_eq!(
            output.run.channels[0].clean_stop_reason.as_deref(),
            Some("NoCleanablePixels")
        );

        assert_eq!(output.artifacts.len(), 1);
        assert_eq!(output.artifacts[0].kind, "alpha");
        assert_eq!(output.artifacts[0].label, "Spectral Index");
        assert!(output.artifacts[0].exists);
        assert_eq!(
            output.artifacts[0].preview_png_path.as_deref(),
            Some("/tmp/from-task.alpha.png")
        );
        assert!(!output.artifacts[0].preview_png_exists);
    }

    #[test]
    fn artifact_helpers_cover_standard_products_and_preview_flags() {
        let tempdir = tempdir().unwrap();
        let imagename = tempdir.path().join("standard-output");
        fs::write(imagename.with_extension("image"), b"image").unwrap();
        fs::write(imagename.with_extension("image.png"), b"png").unwrap();

        let mut config = sample_cli_config(imagename.clone());
        config.deconvolver = Deconvolver::Clark;
        config.nterms = 1;
        config.spectral_mode = SpectralMode::Cube;
        config.write_preview_pngs = true;

        let artifacts = imaging_artifacts(&config);
        assert_eq!(artifacts.len(), 4);
        assert_eq!(
            artifacts
                .iter()
                .map(|artifact| artifact.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["psf", "residual", "model", "image"]
        );
        assert_eq!(artifacts[0].label, "PSF");
        assert!(!artifacts[0].exists);
        assert_eq!(
            artifacts[0].preview_png_path.as_deref(),
            Some(
                imagename
                    .with_extension("psf.png")
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert_eq!(artifacts[3].label, "Restored Image");
        assert!(artifacts[3].exists);
        assert!(artifacts[3].preview_png_exists);

        let manual = artifact(
            label_for_term("Residual", 2),
            "residual",
            imagename.with_extension("residual"),
            None,
        );
        assert_eq!(manual.label, "Residual TT2");
        assert_eq!(manual.kind, "residual");
        assert!(!manual.exists);
        assert_eq!(manual.preview_png_path, None);
        assert!(!manual.preview_png_exists);
    }
}
