// SPDX-License-Identifier: LGPL-3.0-or-later
//! Structured run report emitted for launcher-managed imaging runs.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::task_contract::{
    ImagerArtifactKind, ImagerDeconvolver, ImagerRestoringBeamMode, ImagerRunTaskResult,
    ImagerSpectralMode, ImagerWTermMode, ImagerWeighting,
};
use crate::{
    ChannelRunSummary, CliConfig, FrontendStageTimings, RunSummary, SpectralMode,
    canonical_deconvolver_name, canonical_restoring_beam_mode_name, canonical_spectral_mode_name,
    canonical_w_term_mode_name, canonical_weighting_name,
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
    /// Requested `w`-term handling mode.
    pub w_term_mode: String,
    /// Optional data-column override.
    pub data_column: Option<String>,
    /// Image size in pixels.
    pub imsize: usize,
    /// Cell size in arcseconds.
    pub cell_arcsec: f64,
    /// Whether the run skipped CLEAN.
    pub dirty_only: bool,
    /// Whether preview PNG sidecars were requested.
    pub write_preview_pngs: bool,
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
                w_term_mode: canonical_w_term_mode_name(config.w_term_mode).to_string(),
                data_column: config.datacolumn.clone(),
                imsize: config.imsize,
                cell_arcsec: config.cell_arcsec,
                dirty_only: config.dirty_only,
                write_preview_pngs: config.write_preview_pngs,
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
                },
                deconvolver: match request.deconvolver {
                    ImagerDeconvolver::Hogbom => "hogbom".to_string(),
                    ImagerDeconvolver::Mtmfs => "mtmfs".to_string(),
                    ImagerDeconvolver::Clark => "clark".to_string(),
                    ImagerDeconvolver::Multiscale => "multiscale".to_string(),
                },
                w_term_mode: match request.w_term_mode {
                    ImagerWTermMode::None => "none".to_string(),
                    ImagerWTermMode::Direct => "direct".to_string(),
                    ImagerWTermMode::Wproject => "wproject".to_string(),
                },
                data_column: request.data_column.clone(),
                imsize: request.image_size,
                cell_arcsec: request.cell_arcsec,
                dirty_only: request.dirty_only,
                write_preview_pngs: request.write_preview_pngs,
                per_channel_weight_density: request.per_channel_weight_density,
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
                            "major_cycle_refresh".to_string(),
                            result.run.stage_timings.major_cycle_refresh_ns,
                        ),
                        ("psf_grid".to_string(), result.run.stage_timings.psf_grid_ns),
                        ("psf_fft".to_string(), result.run.stage_timings.psf_fft_ns),
                        (
                            "residual_degrid_grid".to_string(),
                            result.run.stage_timings.residual_degrid_grid_ns,
                        ),
                        (
                            "residual_fft".to_string(),
                            result.run.stage_timings.residual_fft_ns,
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
                "major_cycle_refresh".to_string(),
                summary.stage_timings.major_cycle_refresh.as_nanos() as u64,
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
                "residual_degrid_grid".to_string(),
                summary.stage_timings.residual_degrid_grid.as_nanos() as u64,
            ),
            (
                "residual_fft".to_string(),
                summary.stage_timings.residual_fft.as_nanos() as u64,
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
    use std::fs;
    use std::time::Duration;

    use casa_imaging::{BeamFitDebugSummary, CleanStopReason, ImagingStageTimings};
    use tempfile::TempDir;

    use super::*;

    fn sample_run_summary() -> RunSummary {
        RunSummary {
            warnings: vec!["warn".to_string()],
            gridded_samples: 42,
            major_cycles: 3,
            minor_iterations: 9,
            clean_stop_reason: Some(CleanStopReason::IterationLimitReached),
            channel_summaries: vec![ChannelRunSummary {
                channel_index: 2,
                major_cycles: 4,
                minor_iterations: 7,
                clean_stop_reason: Some(CleanStopReason::CycleThresholdReached),
                initial_residual_peak_jy_per_beam: 1.5,
                final_residual_peak_jy_per_beam: 0.25,
                final_cycle_threshold_jy_per_beam: 0.1,
                minor_cycle_traces: Vec::new(),
                beam_fit_debug: Some(BeamFitDebugSummary {
                    peak_index: (1, 2),
                    peak_value: 1.0,
                    first_pass_points: 4,
                    first_pass_blc: (0, 0),
                    first_pass_trc: (3, 3),
                    expanded_window_shape: (5, 5),
                    oversampling: 2,
                    resampled_shape: (10, 10),
                    second_pass_points: 8,
                    second_pass_blc: (1, 1),
                    second_pass_trc: (8, 8),
                }),
            }],
            stage_timings: ImagingStageTimings {
                controller_overhead: Duration::from_nanos(10),
                weighting: Duration::from_nanos(20),
                psf_grid: Duration::from_nanos(30),
                psf_fft: Duration::from_nanos(40),
                psf_normalize: Duration::ZERO,
                model_fft: Duration::ZERO,
                residual_degrid_grid: Duration::from_nanos(50),
                residual_fft: Duration::from_nanos(60),
                residual_normalize: Duration::from_nanos(70),
                minor_cycle: Duration::from_nanos(80),
                minor_cycle_solve: Duration::from_nanos(90),
                major_cycle_refresh: Duration::from_nanos(100),
                beam_fit: Duration::ZERO,
                restore: Duration::ZERO,
                total: Duration::from_nanos(110),
            },
            frontend_timings: FrontendStageTimings {
                open_measurement_set: Duration::from_nanos(11),
                prepare_plane_input: Duration::from_nanos(22),
                extract_phase_center: Duration::from_nanos(33),
                run_imaging: Duration::from_nanos(44),
                build_coordinate_system: Duration::from_nanos(55),
                write_products: Duration::from_nanos(66),
                total: Duration::from_nanos(77),
            },
        }
    }

    #[test]
    fn from_run_reports_mtmfs_artifacts_and_preview_sidecars() {
        let dir = TempDir::new().expect("tempdir");
        let base = dir.path().join("mtmfs");
        for suffix in [
            "psf.tt0",
            "residual.tt0",
            "model.tt0",
            "image.tt0",
            "psf.tt1",
            "residual.tt1",
            "model.tt1",
            "image.tt1",
            "alpha",
            "psf.tt0.png",
            "residual.tt0.png",
            "model.tt0.png",
            "image.tt0.png",
            "alpha.png",
        ] {
            fs::write(dir.path().join(format!("mtmfs.{suffix}")), []).unwrap();
        }

        let config = CliConfig::parse([
            "--ms".into(),
            "demo.ms".into(),
            "--imagename".into(),
            base.as_os_str().to_os_string(),
            "--imsize".into(),
            "64".into(),
            "--cell-arcsec".into(),
            "1.5".into(),
            "--deconvolver".into(),
            "mtmfs".into(),
            "--nterms".into(),
            "2".into(),
        ])
        .unwrap();

        let output = ManagedImagingOutput::from_run(&config, &sample_run_summary());
        assert_eq!(output.request.spectral_mode, "mfs");
        assert_eq!(output.request.deconvolver, "mtmfs");
        assert_eq!(output.request.output_channels, 1);
        assert_eq!(
            output.run.clean_stop_reason.as_deref(),
            Some("IterationLimitReached")
        );
        assert_eq!(
            output.run.stage_timings.values_ns[0],
            ("controller_total".to_string(), 110)
        );
        assert_eq!(
            output.run.frontend_timings.values_ns[6],
            ("total".to_string(), 77)
        );
        assert_eq!(
            output.run.channels[0].clean_stop_reason.as_deref(),
            Some("CycleThresholdReached")
        );
        assert!(output.run.channels[0].beam_fit_available);
        assert_eq!(output.artifacts.len(), 9);
        assert!(
            output
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "alpha" && artifact.preview_png_exists)
        );
    }

    #[test]
    fn from_task_result_serializes_contract_values() {
        let dir = TempDir::new().expect("tempdir");
        let base = dir.path().join("cube");
        for suffix in ["psf", "residual", "model", "image"] {
            fs::write(dir.path().join(format!("cube.{suffix}")), []).unwrap();
        }

        let config = CliConfig::parse([
            "--ms".into(),
            "demo.ms".into(),
            "--imagename".into(),
            base.as_os_str().to_os_string(),
            "--imsize".into(),
            "64".into(),
            "--cell-arcsec".into(),
            "1.5".into(),
            "--specmode".into(),
            "cube".into(),
            "--corr".into(),
            "XX".into(),
            "--weighting".into(),
            "briggs".into(),
            "--robust".into(),
            "0.5".into(),
            "--wterm".into(),
            "wproject".into(),
            "--wprojplanes".into(),
            "8".into(),
            "--restoringbeam".into(),
            "common".into(),
            "--dirty-only".into(),
            "--no-preview-pngs".into(),
        ])
        .unwrap();
        let request = crate::task_contract::ImagerRunTaskRequest::from_cli_config(&config);
        let result =
            crate::task_contract::ImagerRunTaskResult::from_run(request, &sample_run_summary());

        let output = ManagedImagingOutput::from_task_result(&result);
        assert_eq!(output.request.spectral_mode, "cube");
        assert_eq!(output.request.weighting, "briggs:0.5");
        assert_eq!(output.request.w_term_mode, "wproject");
        assert_eq!(output.request.correlation.as_deref(), Some("XX"));
        assert_eq!(output.request.restoring_beam_mode, "common");
        assert!(output.request.dirty_only);
        assert!(!output.request.write_preview_pngs);
        assert_eq!(output.run.channels.len(), 1);
        assert_eq!(output.run.channels[0].channel_index, 2);
        assert_eq!(output.artifacts.len(), 4);
        assert!(output.artifacts.iter().all(|artifact| artifact.exists));
    }
}
