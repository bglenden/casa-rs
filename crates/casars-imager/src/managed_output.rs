// SPDX-License-Identifier: LGPL-3.0-or-later
//! Structured run report emitted for launcher-managed imaging runs.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
