// SPDX-License-Identifier: LGPL-3.0-or-later
//! Structured run report emitted for launcher-managed imaging runs.

#[cfg(test)]
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::task_contract::{
    ImagerArtifactKind, ImagerDeconvolver, ImagerHogbomIterationMode, ImagerMinorCycleDiagnostic,
    ImagerRestoringBeamMode, ImagerRunTaskResult, ImagerSaveModel, ImagerSpectralMode,
    ImagerVisibilityProductDiagnostic, ImagerWTermMode, ImagerWeighting,
};
use crate::{
    CliConfig, RunSummary, canonical_deconvolver_name, canonical_hogbom_iteration_mode_name,
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
    /// Effective visibility-gridder family.
    pub gridder: String,
    /// Hogbom minor-cycle iteration accounting policy.
    pub hogbom_iteration_mode: String,
    /// Requested `w`-term handling mode.
    pub w_term_mode: String,
    /// Optional data-column override.
    pub data_column: Option<String>,
    /// Requested model persistence mode.
    pub save_model: String,
    /// Whether continuum residual visibilities overwrite existing CORRECTED_DATA.
    pub save_continuum_residual: bool,
    /// Image size in pixels.
    pub imsize: usize,
    /// Cell size in arcseconds.
    pub cell_arcsec: f64,
    /// Image direction-coordinate projection.
    pub projection: String,
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
    /// Total minor-cycle count charged to the reported task/controller budget.
    pub minor_iterations: usize,
    /// Total minor-cycle components actually applied by the run.
    pub actual_minor_iterations: usize,
    /// Final CLEAN stop reason when deconvolution ran.
    pub clean_stop_reason: Option<String>,
    /// Ordered owner-calculated solver diagnostics.
    pub minor_cycles: Vec<ImagerMinorCycleDiagnostic>,
    /// Final paired-operator visibility identities and provenance, when produced.
    pub visibility_products: Option<ImagerVisibilityProductDiagnostic>,
    /// Measured end-to-end application wall time.
    pub elapsed_ns: u64,
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
                gridder: managed_gridder_from_config(config).to_string(),
                hogbom_iteration_mode: canonical_hogbom_iteration_mode_name(
                    config.hogbom_iteration_mode,
                )
                .to_string(),
                w_term_mode: if config.aw_project.is_some() {
                    "awproject".to_string()
                } else {
                    canonical_w_term_mode_name(config.w_term_mode).to_string()
                },
                data_column: config.datacolumn.clone(),
                save_model: match config.save_model {
                    crate::SaveModelMode::None => "none",
                    crate::SaveModelMode::ModelColumn => "modelcolumn",
                }
                .to_string(),
                save_continuum_residual: config.save_continuum_residual,
                imsize: config.imsize,
                cell_arcsec: config.cell_arcsec,
                projection: "SIN".to_string(),
                dirty_only: config.dirty_only,
                write_preview_pngs: config.write_preview_pngs,
                write_pb: config.write_pb,
                per_channel_weight_density: config.per_channel_weight_density,
                nterms: config.nterms,
                output_channels: 1,
                correlation: config.correlation.clone(),
                restoring_beam_mode: canonical_restoring_beam_mode_name(config.restoring_beam_mode)
                    .to_string(),
            },
            run: ManagedImagingRun {
                warnings: summary.warnings.clone(),
                gridded_samples: summary.gridded_samples,
                major_cycles: summary.major_cycles,
                minor_iterations: summary.minor_iterations,
                actual_minor_iterations: summary.actual_minor_iterations,
                clean_stop_reason: summary
                    .clean_stop_reason
                    .map(|reason| format!("{reason:?}")),
                minor_cycles: crate::task_contract::project_minor_cycles(&summary.minor_cycles),
                visibility_products: summary.visibility_products.clone(),
                elapsed_ns: summary.elapsed.as_nanos() as u64,
            },
            artifacts: imaging_artifacts(config, &summary.output_products),
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
                gridder: managed_gridder_from_request(request).to_string(),
                hogbom_iteration_mode: match request.hogbom_iteration_mode {
                    ImagerHogbomIterationMode::Strict => "strict".to_string(),
                    ImagerHogbomIterationMode::CasaInclusive => "casa".to_string(),
                },
                w_term_mode: if request.aw_project.is_some() {
                    "awproject".to_string()
                } else {
                    match request.w_term_mode {
                        ImagerWTermMode::None => "none".to_string(),
                        ImagerWTermMode::Direct => "direct".to_string(),
                        ImagerWTermMode::Wproject => "wproject".to_string(),
                    }
                },
                data_column: request.data_column.clone(),
                save_model: match request.save_model {
                    ImagerSaveModel::None => "none",
                    ImagerSaveModel::ModelColumn => "modelcolumn",
                }
                .to_string(),
                save_continuum_residual: request.save_continuum_residual,
                imsize: request.image_size,
                cell_arcsec: request.cell_arcsec,
                projection: request.projection.as_cli_text().to_string(),
                dirty_only: request.dirty_only,
                write_preview_pngs: request.write_preview_pngs,
                write_pb: request.write_pb,
                per_channel_weight_density: managed_request_per_channel_weight_density(request),
                nterms: request.nterms,
                output_channels: 1,
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
                actual_minor_iterations: result.run.actual_minor_iterations,
                clean_stop_reason: result
                    .run
                    .clean_stop_reason
                    .map(|reason| format!("{reason:?}")),
                minor_cycles: result.run.minor_cycles.clone(),
                visibility_products: result.run.visibility_products.clone(),
                elapsed_ns: result.run.elapsed_ns,
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
                        ImagerArtifactKind::Sumwt => "sumwt".to_string(),
                        ImagerArtifactKind::PrimaryBeam => "pb".to_string(),
                        ImagerArtifactKind::ImagePbcor => "image.pbcor".to_string(),
                        ImagerArtifactKind::Alpha => "alpha".to_string(),
                        ImagerArtifactKind::AlphaError => "alpha.error".to_string(),
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

fn managed_request_per_channel_weight_density(
    request: &crate::task_contract::ImagerRunTaskRequest,
) -> bool {
    request
        .per_channel_weight_density
        .unwrap_or(matches!(request.spectral_mode, ImagerSpectralMode::Cube))
}

fn managed_gridder_from_config(config: &CliConfig) -> &'static str {
    if config.aw_project.is_some() {
        "awproject"
    } else if config.force_standard_gridder {
        "standard"
    } else if matches!(
        config.w_term_mode,
        crate::WTermMode::WProject | crate::WTermMode::Direct
    ) {
        if matches!(config.w_term_mode, crate::WTermMode::Direct) {
            "widefield"
        } else {
            "wproject"
        }
    } else if config.use_pointing
        || config
            .field_ids
            .as_ref()
            .is_some_and(|field_ids| field_ids.len() > 1)
    {
        "mosaic"
    } else {
        "standard"
    }
}

fn managed_gridder_from_request(
    request: &crate::task_contract::ImagerRunTaskRequest,
) -> &'static str {
    if request.aw_project.is_some() {
        "awproject"
    } else if matches!(request.w_term_mode, ImagerWTermMode::Wproject) {
        "wproject"
    } else if request.use_pointing
        || request
            .field_ids
            .as_ref()
            .is_some_and(|field_ids| field_ids.len() > 1)
    {
        "mosaic"
    } else {
        "standard"
    }
}

fn imaging_artifacts(config: &CliConfig, products: &[String]) -> Vec<ManagedImagingArtifact> {
    let request = crate::task_contract::ImagerRunTaskRequest::from_cli_config(config);
    super::task_contract::build_artifacts_for_products(&request, products)
        .into_iter()
        .map(|artifact| ManagedImagingArtifact {
            kind: artifact.kind.as_suffix().to_string(),
            label: artifact.label,
            path: artifact.path,
            exists: artifact.exists,
            preview_png_path: artifact.preview_png_path,
            preview_png_exists: artifact.preview_png_exists,
        })
        .collect()
}

#[cfg(test)]
fn label_for_term(base: &str, term: usize) -> String {
    format!("{base} TT{term}")
}

#[cfg(test)]
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
        AutoMultiThresholdConfig, AwProjectControls, AwProjectNormalization, CleanMaskMode,
        CleanStopReason, CliConfig, CubeAxisConfig, Deconvolver, HogbomIterationMode,
        ImagingFftBackendPolicy, ImagingFftPrecisionPolicy, RestoringBeamMode, RunSummary,
        SaveModelMode, SpectralMode, StandardMfsAccelerationPolicy, WTermMode, WeightingMode,
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
            uvrange: None,
            intent: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            continuum_fit_spw: None,
            continuum_fit_order: 0,
            datacolumn: Some("CORRECTED_DATA".to_string()),
            save_model: SaveModelMode::None,
            save_continuum_residual: false,
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
            aw_project: None,
            dirty_only: false,
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
            imaging_memory_pressure_policy: Default::default(),
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
        RunSummary {
            warnings: vec!["warning-a".to_string()],
            gridded_samples: 42,
            major_cycles: 3,
            minor_iterations: 9,
            actual_minor_iterations: 10,
            clean_stop_reason: Some(CleanStopReason::CycleThresholdReached),
            minor_cycles: Vec::new(),
            visibility_products: None,
            elapsed: Duration::from_nanos(37),
            output_products: vec![
                ".psf".to_string(),
                ".residual".to_string(),
                ".model".to_string(),
                ".image".to_string(),
                ".sumwt".to_string(),
            ],
        }
    }

    #[test]
    fn from_run_reports_the_application_product_inventory() {
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
        assert_eq!(output.request.gridder, "widefield");
        assert_eq!(output.request.w_term_mode, "direct");
        assert_eq!(output.request.restoring_beam_mode, "common");
        assert_eq!(output.request.output_channels, 1);
        assert_eq!(output.request.correlation.as_deref(), Some("XX"));

        assert_eq!(
            output.run.clean_stop_reason.as_deref(),
            Some("CycleThresholdReached")
        );
        assert_eq!(output.run.elapsed_ns, 37);

        assert_eq!(output.artifacts.len(), 5);
        let psf = output
            .artifacts
            .iter()
            .find(|artifact| artifact.path.ends_with(".psf"))
            .unwrap();
        assert_eq!(psf.label, "PSF");
        assert_eq!(psf.kind, "psf");
        assert_eq!(
            output
                .artifacts
                .iter()
                .filter(|artifact| artifact.kind == "sumwt")
                .count(),
            1
        );
        assert!(
            output
                .artifacts
                .iter()
                .all(|artifact| !artifact.path.contains(".tt"))
        );
    }

    #[test]
    fn from_run_reports_awproject_as_the_effective_gridder_and_w_term() {
        let tempdir = tempdir().unwrap();
        let mut config = sample_cli_config(tempdir.path().join("awproject-output"));
        config.aw_project = Some(AwProjectControls {
            cf_cache: PathBuf::from("/tmp/vlass-cf-cache"),
            cf_resident_bytes: 512 * 1024 * 1024,
            facets: 1,
            w_plane_count: Some(32),
            psf_phase_center_direction_rad: None,
            vp_table: None,
            a_term: true,
            ps_term: false,
            wb_awp: true,
            conjugate_beams: true,
            compute_pa_step_deg: 360.0,
            rotate_pa_step_deg: 360.0,
            pointing_offset_sigdev: vec![0.0],
            use_pointing: true,
            mosaic_weighting: false,
            normalization: AwProjectNormalization::FlatNoise,
        });
        config.use_pointing = true;
        config.write_pb = true;
        config.w_term_mode = WTermMode::None;

        let output = ManagedImagingOutput::from_run(&config, &sample_run_summary());

        assert_eq!(output.request.gridder, "awproject");
        assert_eq!(output.request.w_term_mode, "awproject");
        assert_eq!(output.artifacts.len(), 5);
        assert!(
            output
                .artifacts
                .iter()
                .all(|artifact| !artifact.path.ends_with(".pb.tt1"))
        );
    }

    #[test]
    fn from_task_result_serializes_contract_values() {
        let result = ImagerRunTaskResult {
            request: ImagerRunTaskRequest {
                measurement_set: PathBuf::from("/tmp/from-task.ms"),
                image_name: PathBuf::from("/tmp/from-task"),
                image_size: 512,
                cell_arcsec: 2.5,
                projection: crate::ImagerProjection::Sin,
                field_ids: Some(vec![3]),
                uvrange: None,
                intent: None,
                phasecenter_field: None,
                phasecenter: Some("J2000 00:00:00.0 +00.00.00.0".to_string()),
                ddid: Some(1),
                spw_selector: Some("2".to_string()),
                channel_start: Some(4),
                channel_count: Some(8),
                continuum_fit_spw: None,
                continuum_fit_order: 0,
                data_column: Some("MODEL_DATA".to_string()),
                save_model: ImagerSaveModel::ModelColumn,
                save_continuum_residual: false,
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
                aw_project: None,
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
                imaging_memory_pressure_policy: Default::default(),
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
                actual_minor_iterations: 25,
                iterdone: 24,
                nmajordone: 6,
                stopcode: 10,
                clean_stop_reason: Some(ImagerCleanStopReason::DivergenceDetected),
                minor_cycles: Vec::new(),
                visibility_products: None,
                elapsed_ns: 22,
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
        assert_eq!(output.request.gridder, "wproject");
        assert_eq!(output.request.w_term_mode, "wproject");
        assert_eq!(output.request.save_model, "modelcolumn");
        assert_eq!(output.request.restoring_beam_mode, "per_plane");
        assert_eq!(output.request.output_channels, 1);
        assert_eq!(output.request.correlation.as_deref(), Some("XX"));
        assert!(output.request.dirty_only);
        assert!(!output.request.write_preview_pngs);
        assert_eq!(output.request.projection, "SIN");
        assert_eq!(
            output.run.clean_stop_reason.as_deref(),
            Some("DivergenceDetected")
        );
        assert_eq!(output.run.elapsed_ns, 22);

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

        let artifacts = imaging_artifacts(&config, &sample_run_summary().output_products);
        assert_eq!(artifacts.len(), 5);
        assert_eq!(
            artifacts
                .iter()
                .map(|artifact| artifact.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["psf", "residual", "model", "image", "sumwt"]
        );
        assert_eq!(artifacts[3].label, "Restored Image");
        assert!(artifacts[3].exists);
        assert_eq!(
            artifacts[3].preview_png_path.as_deref(),
            Some(
                imagename
                    .with_extension("image.png")
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert!(artifacts[3].preview_png_exists);
        assert_eq!(artifacts[4].label, "Sum of Weights");
        assert!(!artifacts[4].exists);

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
