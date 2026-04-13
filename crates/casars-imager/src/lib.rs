// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Thin MeasurementSet-backed frontend for the pure `casa-imaging` core.

mod oracle;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_imaging::{
    BeamFit, BeamFitDebugSummary, CleanConfig, CleanStopReason, CompatibilityMode,
    CubeChannelRequest, CubeImagingRequest, CubeModelChannelContribution,
    CubeModelInterpolationBatch, Deconvolver, GaussianUvTaper, GridderMode, ImageGeometry,
    ImagingRequest, ImagingStageTimings, MinorCycleTrace, MosaicGridderConfig, MtmfsRequest,
    ParallelHandBatch, PlaneStokes, PrimaryBeamModel, ResidualRefreshDiagnostics,
    RestoringBeamMode, UvTaperSize, VisibilityBatch, VisibilityMetadataBatch, WProjectDiagnostics,
    WProjectSkipReason, WTermMode, WeightDensityMode, WeightingMode, run_cube, run_imaging,
    run_mtmfs, trace_cube_channel_residual_refresh,
    trace_cube_channel_residual_refresh_model_channel_lambda, trace_cube_channel_w_project_plan,
    trace_w_project_plan,
};
use casa_ms::MeasurementSet;
use casa_ms::columns::data_columns::DataColumn;
use casa_ms::columns::flag_columns::{FlagColumn, FlagRowColumn};
use casa_ms::columns::main_ids;
use casa_ms::columns::time_columns::TimeColumn;
use casa_ms::columns::uvw_column::UvwColumn;
use casa_ms::columns::weight_columns::WeightSpectrumColumn;
use casa_ms::derived::engine::{MsCalEngine, resolve_field_phase_direction_j2000};
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::spectral_selection::CubeRowSpectralContributions;
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeChannelContribution, CubeInterpolation, CubeSpecMode,
    CubeSpectralSetup, parse_numeric_id_selector,
    parse_rest_frequency_hz as parse_ms_rest_frequency_hz, parse_spw_selector,
    resolve_channel_selector_selection, resolve_contiguous_channel_selection,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use image::{ImageBuffer, Rgb};
use ndarray::{Array2, Array4, IxDyn, s};
use num_complex::Complex32;

pub use oracle::{
    ArtifactFormat, ChannelContributionTrace, DatasetTier, ORACLE_SCHEMA_VERSION,
    OracleArtifactManifest, OracleBundleManifest, OracleBundleOverrides, PhaseCenterTrace,
    PreparedGeometryRowTrace, PreparedGeometryTraceBundle, PreparedOutputChannelTrace,
    PreparedSampleRejectionReason, PreparedSourceChannelTrace, PreparedSpectralAxisTrace,
    PreparedVisibilitySampleTrace, PreparedVisibilityTraceBundle,
    RejectedPreparedVisibilitySampleTrace, SelectedRowTrace, ToleranceClass, TruthDomain,
    WProjectKernelTrace, WProjectSamplePlanTrace, WProjectSkipReasonTrace,
    WProjectSkippedSampleTrace, WProjectTraceBundle, WeightSourceKind, sha256_hex_path,
    write_json_gzip_hashed, write_json_pretty, write_json_pretty_hashed,
};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const DEFAULT_BATCH_SIZE: usize = 4096;

/// Spectral imaging mode for the CLI frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralMode {
    /// Collapse all selected channels into a single MFS image plane.
    Mfs,
    /// Produce one image plane per selected channel in CASA `specmode='cube'`.
    ///
    /// This builds the output spectral axis in the requested frame and applies
    /// per-row runtime frequency conversion during cube assignment.
    Cube,
    /// Produce one image plane per selected channel in CASA
    /// `specmode='cubedata'`.
    ///
    /// This keeps the cube spectral axis in the native data frame and skips
    /// runtime frequency conversion.
    Cubedata,
}

impl SpectralMode {
    fn is_cube_like(self) -> bool {
        matches!(self, Self::Cube | Self::Cubedata)
    }

    fn cube_specmode(self) -> CubeSpecMode {
        match self {
            Self::Mfs => CubeSpecMode::Cube,
            Self::Cube => CubeSpecMode::Cube,
            Self::Cubedata => CubeSpecMode::Cubedata,
        }
    }
}

fn canonical_spectral_mode_name(mode: SpectralMode) -> &'static str {
    match mode {
        SpectralMode::Mfs => "mfs",
        SpectralMode::Cube => "cube",
        SpectralMode::Cubedata => "cubedata",
    }
}

fn canonical_data_column_name(column: VisibilityDataColumn) -> &'static str {
    column.name()
}

fn canonical_weighting_name(weighting: WeightingMode) -> String {
    match weighting {
        WeightingMode::Natural => "natural".to_string(),
        WeightingMode::Uniform => "uniform".to_string(),
        WeightingMode::Briggs { robust } => format!("briggs:{robust}"),
    }
}

fn canonical_restoring_beam_mode_name(mode: RestoringBeamMode) -> &'static str {
    match mode {
        RestoringBeamMode::PerPlane => "per_plane",
        RestoringBeamMode::Common => "common",
    }
}

fn canonical_deconvolver_name(mode: Deconvolver) -> &'static str {
    match mode {
        Deconvolver::Hogbom => "hogbom",
        Deconvolver::Mtmfs => "mtmfs",
        Deconvolver::Clark => "clark",
        Deconvolver::Multiscale => "multiscale",
    }
}

fn canonical_w_term_mode_name(mode: WTermMode) -> &'static str {
    match mode {
        WTermMode::None => "none",
        WTermMode::Direct => "direct",
        WTermMode::WProject => "wproject",
    }
}

fn canonical_cube_interpolation_name(mode: CubeInterpolation) -> &'static str {
    match mode {
        CubeInterpolation::Nearest => "nearest",
        CubeInterpolation::Linear => "linear",
        CubeInterpolation::Cubic => "cubic",
    }
}

fn canonical_cube_axis_value(value: &CubeAxisValue) -> String {
    match value {
        CubeAxisValue::Channel(channel) => format!("channel:{channel}"),
        CubeAxisValue::FrequencyHz { hz, frame } => match frame {
            Some(frame) => format!("frequency_hz:{hz}@{}", frame.as_str()),
            None => format!("frequency_hz:{hz}"),
        },
        CubeAxisValue::VelocityMs { ms, frame } => match frame {
            Some(frame) => format!("velocity_ms:{ms}@{}", frame.as_str()),
            None => format!("velocity_ms:{ms}"),
        },
        CubeAxisValue::Doppler { value, convention } => {
            format!("doppler:{value}@{}", convention.as_str())
        }
    }
}

fn canonical_uv_taper(taper: GaussianUvTaper) -> String {
    fn axis_text(axis: UvTaperSize) -> String {
        match axis {
            UvTaperSize::ImageFwhmRad(value) => format!("image_fwhm_rad:{value}"),
            UvTaperSize::BaselineHwhmLambda(value) => format!("baseline_hwhm_lambda:{value}"),
        }
    }
    format!(
        "major={},minor={},pa_rad={}",
        axis_text(taper.major),
        axis_text(taper.minor),
        taper.position_angle_rad
    )
}

fn optional_numeric_list(values: Option<&[i32]>) -> String {
    values
        .map(|values| {
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|| "none".to_string())
}

fn oracle_parameter_manifest(config: &CliConfig) -> BTreeMap<String, String> {
    let mut manifest = BTreeMap::new();
    manifest.insert(
        "field_ids".to_string(),
        optional_numeric_list(config.field_ids.as_deref()),
    );
    manifest.insert(
        "phasecenter_field".to_string(),
        config
            .phasecenter_field
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "phasecenter".to_string(),
        config
            .phasecenter
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "ddid".to_string(),
        config
            .ddid
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "spw".to_string(),
        config
            .spw
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "spw_selector".to_string(),
        config
            .spw_selector
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "channel_start".to_string(),
        config
            .channel_start
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "channel_count".to_string(),
        config
            .channel_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "datacolumn".to_string(),
        config
            .datacolumn
            .clone()
            .unwrap_or_else(|| "auto".to_string()),
    );
    manifest.insert(
        "correlation".to_string(),
        config
            .correlation
            .clone()
            .unwrap_or_else(|| "stokes_i_or_native".to_string()),
    );
    manifest.insert(
        "spectral_mode".to_string(),
        canonical_spectral_mode_name(config.spectral_mode).to_string(),
    );
    manifest.insert(
        "cube_specmode".to_string(),
        match config.cube_axis.specmode {
            CubeSpecMode::Cube => "cube".to_string(),
            CubeSpecMode::Cubedata => "cubedata".to_string(),
        },
    );
    manifest.insert(
        "cube_outframe".to_string(),
        config.cube_axis.outframe.as_str().to_string(),
    );
    manifest.insert(
        "cube_veltype".to_string(),
        config.cube_axis.veltype.as_str().to_string(),
    );
    manifest.insert(
        "cube_interpolation".to_string(),
        canonical_cube_interpolation_name(config.cube_axis.interpolation).to_string(),
    );
    manifest.insert(
        "cube_rest_frequency_hz".to_string(),
        config
            .cube_axis
            .rest_frequency_hz
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "cube_start".to_string(),
        config
            .cube_axis
            .start
            .as_ref()
            .map(canonical_cube_axis_value)
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "cube_width".to_string(),
        config
            .cube_axis
            .width
            .as_ref()
            .map(canonical_cube_axis_value)
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "weighting".to_string(),
        canonical_weighting_name(config.weighting),
    );
    manifest.insert(
        "per_channel_weight_density".to_string(),
        config.per_channel_weight_density.to_string(),
    );
    manifest.insert(
        "uv_taper".to_string(),
        config
            .uv_taper
            .map(canonical_uv_taper)
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "restoring_beam_mode".to_string(),
        canonical_restoring_beam_mode_name(config.restoring_beam_mode).to_string(),
    );
    manifest.insert(
        "deconvolver".to_string(),
        canonical_deconvolver_name(config.deconvolver).to_string(),
    );
    manifest.insert("nterms".to_string(), config.nterms.to_string());
    manifest.insert(
        "multiscale_scales".to_string(),
        if config.multiscale_scales.is_empty() {
            "none".to_string()
        } else {
            config
                .multiscale_scales
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        },
    );
    manifest.insert(
        "small_scale_bias".to_string(),
        config.small_scale_bias.to_string(),
    );
    manifest.insert("niter".to_string(), config.niter.to_string());
    manifest.insert("gain".to_string(), config.gain.to_string());
    manifest.insert("threshold_jy".to_string(), config.threshold_jy.to_string());
    manifest.insert("nsigma".to_string(), config.nsigma.to_string());
    manifest.insert("psf_cutoff".to_string(), config.psf_cutoff.to_string());
    manifest.insert(
        "minor_cycle_length".to_string(),
        config.minor_cycle_length.to_string(),
    );
    manifest.insert("cyclefactor".to_string(), config.cyclefactor.to_string());
    manifest.insert(
        "min_psf_fraction".to_string(),
        config.min_psf_fraction.to_string(),
    );
    manifest.insert(
        "max_psf_fraction".to_string(),
        config.max_psf_fraction.to_string(),
    );
    manifest.insert(
        "mask_boxes".to_string(),
        if config.mask_boxes.is_empty() {
            "none".to_string()
        } else {
            config
                .mask_boxes
                .iter()
                .map(|bounds| format!("{},{},{},{}", bounds[0], bounds[1], bounds[2], bounds[3]))
                .collect::<Vec<_>>()
                .join(";")
        },
    );
    manifest.insert(
        "mask_image".to_string(),
        config
            .mask_image
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert(
        "w_term_mode".to_string(),
        canonical_w_term_mode_name(config.w_term_mode).to_string(),
    );
    manifest.insert(
        "w_project_planes".to_string(),
        config
            .w_project_planes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    manifest.insert("dirty_only".to_string(), config.dirty_only.to_string());
    manifest.insert(
        "write_preview_pngs".to_string(),
        config.write_preview_pngs.to_string(),
    );
    manifest
}

/// Run the imager CLI with already-split argument strings.
pub fn run_with_cli_args(args: impl IntoIterator<Item = OsString>) -> Result<(), String> {
    let config = CliConfig::parse(args)?;
    let output = run_from_config(&config)?;
    for warning in &output.warnings {
        eprintln!("warning: {warning}");
    }
    println!(
        "Wrote CASA-compatible products at prefix {} ({} gridded samples, {} major cycles, {} minor iterations, stop={:?})",
        config.imagename.display(),
        output.gridded_samples,
        output.major_cycles,
        output.minor_iterations,
        output.clean_stop_reason
    );
    Ok(())
}

/// Build a frozen-oracle trace for the current `prepare_plane_input()` seam.
///
/// This opens the MeasurementSet, resolves the selected data column, and emits
/// the row-selection plus prepared-sample trace without running imaging.
pub fn build_prepare_plane_trace_from_config(
    config: &CliConfig,
) -> Result<PreparedVisibilityTraceBundle, String> {
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let (_, trace) = prepare_plane_input_with_trace(&ms, config, data_column)?;
    Ok(trace)
}

/// Build a frozen-oracle trace for the spectral preparation seam.
///
/// This currently reuses the full `prepare_plane_input()` trace bundle because
/// the final prepared samples, rejected samples, weight-source provenance, and
/// output-channel mapping all live at that seam.
pub fn build_prepare_spectral_trace_from_config(
    config: &CliConfig,
) -> Result<PreparedVisibilityTraceBundle, String> {
    build_prepare_plane_trace_from_config(config)
}

/// Derive the spectral-axis artifact for one prepared-visibility trace bundle.
pub fn build_prepare_spectral_axis_trace(
    trace: &PreparedVisibilityTraceBundle,
) -> PreparedSpectralAxisTrace {
    PreparedSpectralAxisTrace {
        spectral_mode: trace.spectral_mode.clone(),
        source_channels: trace
            .source_channel_indices
            .iter()
            .enumerate()
            .map(|(slot, source_channel_index)| PreparedSourceChannelTrace {
                source_channel_slot: slot,
                source_channel_index: *source_channel_index,
                frequency_hz: trace.source_channel_frequencies_hz[slot],
                width_hz: trace.source_channel_widths_hz[slot],
            })
            .collect(),
        output_channels: trace
            .output_channel_frequencies_hz
            .iter()
            .copied()
            .enumerate()
            .map(
                |(output_channel_index, frequency_hz)| PreparedOutputChannelTrace {
                    output_channel_index,
                    frequency_hz,
                },
            )
            .collect(),
    }
}

/// Build a frozen-oracle trace for the row-level geometric preparation seam.
///
/// This opens the MeasurementSet, resolves the selected rows and phase center,
/// and emits one row-level geometry record per selected MAIN row before any
/// spectral interpolation or weighting is applied.
pub fn build_prepare_geometry_trace_from_config(
    config: &CliConfig,
) -> Result<PreparedGeometryTraceBundle, String> {
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_description = ms
        .data_description()
        .map_err(|error| format!("open DATA_DESCRIPTION: {error}"))?;
    let ddid_info = data_description_index(&data_description)?;
    let selection = select_main_rows(&ms, config, &ddid_info)?;
    let derived_engine = selection
        .needs_geometry_engine
        .then(|| MsCalEngine::new(&ms).map_err(|error| format!("build derived engine: {error}")))
        .transpose()?;
    let rows = build_prepared_geometry_rows(
        &ms,
        &selection.selected_rows,
        &selection.phase_center,
        derived_engine.as_ref(),
    )?;
    Ok(PreparedGeometryTraceBundle {
        schema_version: ORACLE_SCHEMA_VERSION,
        ms_path: config.ms.display().to_string(),
        phase_center: PhaseCenterTrace {
            field_id: selection.phase_center.field_id,
            reference: selection.phase_center.reference.as_str().to_string(),
            angles_rad: selection.phase_center.angles_rad,
        },
        selected_rows: selection
            .selected_rows
            .iter()
            .map(SelectedMainRow::trace)
            .collect(),
        rows: rows.iter().map(PreparedGeometryRow::trace).collect(),
    })
}

fn w_project_skip_reason_trace(reason: WProjectSkipReason) -> WProjectSkipReasonTrace {
    match reason {
        WProjectSkipReason::NotGridable => WProjectSkipReasonTrace::NotGridable,
        WProjectSkipReason::InvalidInput => WProjectSkipReasonTrace::InvalidInput,
        WProjectSkipReason::OutsideGrid => WProjectSkipReasonTrace::OutsideGrid,
    }
}

fn build_w_project_trace_bundle(
    config: &CliConfig,
    diagnostics: WProjectDiagnostics,
    channel_index: Option<usize>,
    channel_frequency_hz: Option<f64>,
) -> WProjectTraceBundle {
    WProjectTraceBundle {
        schema_version: ORACLE_SCHEMA_VERSION,
        ms_path: config.ms.display().to_string(),
        spectral_mode: canonical_spectral_mode_name(config.spectral_mode).to_string(),
        channel_index,
        channel_frequency_hz,
        requested_plane_count: diagnostics.requested_plane_count,
        plane_count: diagnostics.plane_count,
        sampling: diagnostics.sampling,
        w_scale: diagnostics.w_scale,
        max_abs_w_lambda: diagnostics.max_abs_w_lambda,
        kernels: diagnostics
            .kernels
            .into_iter()
            .map(|kernel| WProjectKernelTrace {
                plane_index: kernel.plane_index,
                w_lambda: kernel.w_lambda,
                support: kernel.support,
                kernel_integral: kernel.kernel_integral,
            })
            .collect(),
        samples: diagnostics
            .samples
            .into_iter()
            .map(|sample| WProjectSamplePlanTrace {
                batch_index: sample.batch_index,
                sample_index: sample.sample_index,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                weight: sample.weight,
                sumwt_factor: sample.sumwt_factor,
                plane_index: sample.plane_index,
                loc_x: sample.loc_x,
                loc_y: sample.loc_y,
                off_x: sample.off_x,
                off_y: sample.off_y,
                conjugate_kernel: sample.conjugate_kernel,
                normalization: sample.normalization,
                support: sample.support,
            })
            .collect(),
        skipped_samples: diagnostics
            .skipped_samples
            .into_iter()
            .map(|sample| WProjectSkippedSampleTrace {
                batch_index: sample.batch_index,
                sample_index: sample.sample_index,
                u_lambda: sample.u_lambda,
                v_lambda: sample.v_lambda,
                w_lambda: sample.w_lambda,
                weight: sample.weight,
                sumwt_factor: sample.sumwt_factor,
                reason: w_project_skip_reason_trace(sample.reason),
            })
            .collect(),
        normalization_sumwt: diagnostics.normalization_sumwt,
        reported_sumwt: diagnostics.reported_sumwt,
        gridded_samples: diagnostics.gridded_samples,
    }
}

/// Build a frozen-oracle trace for the `wproject` CF/grid-planning seam on an
/// MFS imaging request.
pub fn build_w_project_trace_from_config(
    config: &CliConfig,
) -> Result<WProjectTraceBundle, String> {
    let geometry = ImageGeometry {
        image_shape: [config.imsize, config.imsize],
        cell_size_rad: [
            config.cell_arcsec * arcsec_to_rad(),
            config.cell_arcsec * arcsec_to_rad(),
        ],
    };
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let PreparedInput::Mfs(plane) = prepared else {
        return Err("build_w_project_trace_from_config requires mfs input".into());
    };
    let diagnostics = trace_w_project_plan(&ImagingRequest {
        geometry,
        visibility_batches: plane.batches,
        gridder_mode: plane.gridder_mode,
        plane_stokes: plane.plane_stokes,
        weighting: config.weighting,
        reffreq_hz: plane.reffreq_hz,
        selected_frequency_range_hz: plane.selected_frequency_range_hz,
        deconvolver: config.deconvolver,
        multiscale_scales: config.multiscale_scales.clone(),
        small_scale_bias: config.small_scale_bias,
        clean: CleanConfig {
            niter: if config.dirty_only { 0 } else { config.niter },
            gain: config.gain,
            threshold_jy_per_beam: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        w_term_mode: config.w_term_mode,
        w_project_planes: config.w_project_planes,
        compatibility: CompatibilityMode::CasaStandardMfs,
    })
    .map_err(|error| error.to_string())?;
    Ok(build_w_project_trace_bundle(
        config,
        diagnostics,
        None,
        None,
    ))
}

/// Build a frozen-oracle trace for the `wproject` CF/grid-planning seam on one
/// prepared cube channel.
pub fn build_cube_channel_w_project_trace_from_config(
    config: &CliConfig,
    channel_index: usize,
) -> Result<WProjectTraceBundle, String> {
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let PreparedInput::Cube(cube) = prepared else {
        return Err("build_cube_channel_w_project_trace_from_config requires cube input".into());
    };
    let request = CubeImagingRequest {
        geometry: ImageGeometry {
            image_shape: [config.imsize, config.imsize],
            cell_size_rad: [
                config.cell_arcsec * arcsec_to_rad(),
                config.cell_arcsec * arcsec_to_rad(),
            ],
        },
        channels: cube.channels,
        plane_stokes: cube.plane_stokes,
        weighting: config.weighting,
        weight_density_mode: if config.per_channel_weight_density {
            WeightDensityMode::PerPlane
        } else {
            WeightDensityMode::Combined
        },
        uv_taper: config.uv_taper,
        restoring_beam_mode: config.restoring_beam_mode,
        deconvolver: config.deconvolver,
        multiscale_scales: config.multiscale_scales.clone(),
        small_scale_bias: config.small_scale_bias,
        clean: CleanConfig {
            niter: if config.dirty_only { 0 } else { config.niter },
            gain: config.gain,
            threshold_jy_per_beam: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        psf_cutoff: config.psf_cutoff,
        w_term_mode: config.w_term_mode,
        w_project_planes: config.w_project_planes,
        compatibility: CompatibilityMode::CasaStandardMfs,
    };
    let diagnostics = trace_cube_channel_w_project_plan(&request, channel_index)
        .map_err(|error| error.to_string())?;
    let channel_frequency_hz = request
        .channels
        .get(channel_index)
        .map(|channel| channel.channel_frequency_hz)
        .ok_or_else(|| {
            format!(
                "cube channel index {channel_index} is out of range for {} prepared channels",
                request.channels.len()
            )
        })?;
    Ok(build_w_project_trace_bundle(
        config,
        diagnostics,
        Some(channel_index),
        Some(channel_frequency_hz),
    ))
}

/// Infer the frozen-oracle dataset tier from a MeasurementSet path.
pub fn infer_oracle_dataset_tier(ms_path: &Path) -> DatasetTier {
    let Some(name) = ms_path.file_name().and_then(|name| name.to_str()) else {
        return DatasetTier::TierA;
    };
    match name {
        "refim_point_withline.ms"
        | "refim_Cband.G37line.ms"
        | "refim_point_wterm_vlad.ms"
        | "n2403.short.ms"
        | "refim_alma_mosaic.ms"
        | "refim_point_linXY.ms"
        | "refim_point_stokes.ms"
        | "polcal_LINEAR_BASIS.ms"
        | "polcal_CIRCULAR_BASIS.ms" => DatasetTier::TierB,
        "M51.ms"
        | "papersky_mosaic.ms"
        | "refim_oneshiftpoint.mosaic.ms"
        | "refim_heterogeneous_pointings.ms" => DatasetTier::TierC,
        _ => DatasetTier::TierA,
    }
}

/// Persist a frozen-oracle bundle for the current `prepare_plane_input()` seam.
pub fn write_prepare_plane_oracle_bundle_from_config(
    config: &CliConfig,
    output_dir: &Path,
    dataset_tier: DatasetTier,
) -> Result<OracleBundleManifest, String> {
    write_prepare_plane_oracle_bundle_from_config_with_overrides(
        config,
        output_dir,
        dataset_tier,
        &OracleBundleOverrides::default(),
    )
}

/// Persist a frozen-oracle bundle for the current `prepare_plane_input()` seam
/// with optional manifest overrides supplied by a higher-level freezing
/// workflow.
pub fn write_prepare_plane_oracle_bundle_from_config_with_overrides(
    config: &CliConfig,
    output_dir: &Path,
    dataset_tier: DatasetTier,
    overrides: &OracleBundleOverrides,
) -> Result<OracleBundleManifest, String> {
    let trace = build_prepare_spectral_trace_from_config(config)?;
    let spectral_axis = build_prepare_spectral_axis_trace(&trace);
    if output_dir.exists() {
        fs::remove_dir_all(output_dir).map_err(|error| {
            format!(
                "remove existing oracle dir {}: {error}",
                output_dir.display()
            )
        })?;
    }
    fs::create_dir_all(output_dir)
        .map_err(|error| format!("create oracle dir {}: {error}", output_dir.display()))?;

    let selected_rows_path = output_dir.join("selected_rows.json.gz");
    let phase_center_path = output_dir.join("phase_center.json");
    let spectral_axis_path = output_dir.join("spectral_axis.json");
    let prepared_samples_path = output_dir.join("prepared_samples.json.gz");
    let rejected_samples_path = output_dir.join("rejected_samples.json.gz");
    let trace_bundle_path = output_dir.join("prepare_trace_bundle.json.gz");

    let selected_rows_sha = write_json_gzip_hashed(&trace.selected_rows, &selected_rows_path)?;
    let phase_center_sha = write_json_pretty_hashed(&trace.phase_center, &phase_center_path)?;
    let spectral_axis_sha = write_json_pretty_hashed(&spectral_axis, &spectral_axis_path)?;
    let prepared_samples_sha = write_json_gzip_hashed(&trace.samples, &prepared_samples_path)?;
    let rejected_samples_sha =
        write_json_gzip_hashed(&trace.rejected_samples, &rejected_samples_path)?;
    let trace_bundle_sha = write_json_gzip_hashed(&trace, &trace_bundle_path)?;

    let dataset_sha256 = match overrides.dataset_sha256.clone() {
        Some(value) => value,
        None => sha256_hex_path(&config.ms)?,
    };
    let canonical_dataset_path = config
        .ms
        .canonicalize()
        .unwrap_or_else(|_| config.ms.clone());
    let dataset_identity = format!("sha256:{dataset_sha256}");

    let manifest = OracleBundleManifest {
        schema_version: ORACLE_SCHEMA_VERSION,
        dataset_path: overrides
            .dataset_path
            .clone()
            .unwrap_or_else(|| canonical_dataset_path.display().to_string()),
        dataset_identity: Some(
            overrides
                .dataset_identity
                .clone()
                .unwrap_or(dataset_identity),
        ),
        dataset_sha256: Some(dataset_sha256),
        dataset_tier,
        casa_version: overrides.casa_version.clone(),
        casacore_version: overrides.casacore_version.clone(),
        parameter_manifest: oracle_parameter_manifest(config),
        artifacts: vec![
            OracleArtifactManifest {
                name: "selected_rows".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::Exact,
                relative_path: "selected_rows.json.gz".to_string(),
                format: ArtifactFormat::JsonGzip,
                sha256: Some(selected_rows_sha),
                notes: Some("Stable selected MAIN-row identity and ordering".to_string()),
            },
            OracleArtifactManifest {
                name: "phase_center".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::Geometry,
                relative_path: "phase_center.json".to_string(),
                format: ArtifactFormat::Json,
                sha256: Some(phase_center_sha),
                notes: Some("Resolved imaging phase-center metadata".to_string()),
            },
            OracleArtifactManifest {
                name: "spectral_axis".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::IntermediateFloat,
                relative_path: "spectral_axis.json".to_string(),
                format: ArtifactFormat::Json,
                sha256: Some(spectral_axis_sha),
                notes: Some(
                    "Selected source channels plus resolved output-channel frequencies".to_string(),
                ),
            },
            OracleArtifactManifest {
                name: "prepared_samples".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::IntermediateFloat,
                relative_path: "prepared_samples.json.gz".to_string(),
                format: ArtifactFormat::JsonGzip,
                sha256: Some(prepared_samples_sha),
                notes: Some("Final prepared scalar visibility samples".to_string()),
            },
            OracleArtifactManifest {
                name: "rejected_prepared_samples".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::IntermediateFloat,
                relative_path: "rejected_samples.json.gz".to_string(),
                format: ArtifactFormat::JsonGzip,
                sha256: Some(rejected_samples_sha),
                notes: Some("Paired-hand samples rejected during scalar collapse".to_string()),
            },
            OracleArtifactManifest {
                name: "prepare_trace_bundle".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::IntermediateFloat,
                relative_path: "prepare_trace_bundle.json.gz".to_string(),
                format: ArtifactFormat::JsonGzip,
                sha256: Some(trace_bundle_sha),
                notes: Some("Full prepare_plane_input seam bundle".to_string()),
            },
        ],
    };
    write_json_pretty(&manifest, &output_dir.join("bundle_manifest.json"))?;
    Ok(manifest)
}

/// Execute the imager using an already-parsed configuration.
pub fn run_from_config(config: &CliConfig) -> Result<RunSummary, String> {
    let total_start = Instant::now();
    let stage_start = Instant::now();
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let open_measurement_set = stage_start.elapsed();
    maybe_log_frontend_progress(
        "open_measurement_set",
        open_measurement_set,
        total_start.elapsed(),
    );
    let stage_start = Instant::now();
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let prepare_plane_time = stage_start.elapsed();
    maybe_log_frontend_progress(
        "prepare_plane_input",
        prepare_plane_time,
        total_start.elapsed(),
    );

    let stage_start = Instant::now();
    let phase_center = prepared.phase_center().clone();
    let extract_phase_center = stage_start.elapsed();
    maybe_log_frontend_progress(
        "extract_phase_center",
        extract_phase_center,
        total_start.elapsed(),
    );

    let stage_start = Instant::now();
    let geometry = ImageGeometry {
        image_shape: [config.imsize, config.imsize],
        cell_size_rad: [
            config.cell_arcsec * arcsec_to_rad(),
            config.cell_arcsec * arcsec_to_rad(),
        ],
    };
    let prepared_freq_ref = match &prepared {
        PreparedInput::Mfs(plane) => plane.freq_ref,
        PreparedInput::Cube(cube) => cube.freq_ref,
    };
    let prepared_input = prepared;
    let run_result = match prepared_input {
        PreparedInput::Mfs(plane) => {
            let clean = CleanConfig {
                niter: if config.dirty_only { 0 } else { config.niter },
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            };
            let clean_mask = build_clean_mask(
                config.imsize,
                &config.mask_boxes,
                config.mask_image.as_deref(),
            )?;
            if config.deconvolver == Deconvolver::Mtmfs {
                RunProducts::Mtmfs(
                    run_mtmfs(&MtmfsRequest {
                        geometry,
                        visibility_batches: plane.batches,
                        sample_frequency_batches_hz: plane.sample_frequency_batches_hz,
                        gridder_mode: plane.gridder_mode,
                        plane_stokes: plane.plane_stokes,
                        weighting: config.weighting,
                        reffreq_hz: plane.reffreq_hz,
                        selected_frequency_range_hz: plane.selected_frequency_range_hz,
                        nterms: config.nterms,
                        clean,
                        clean_mask,
                        compatibility: CompatibilityMode::CasaStandardMfs,
                    })
                    .map_err(|error| error.to_string())?,
                )
            } else {
                RunProducts::Mfs(
                    run_imaging(&ImagingRequest {
                        geometry,
                        visibility_batches: plane.batches,
                        gridder_mode: plane.gridder_mode,
                        plane_stokes: plane.plane_stokes,
                        weighting: config.weighting,
                        reffreq_hz: plane.reffreq_hz,
                        selected_frequency_range_hz: plane.selected_frequency_range_hz,
                        deconvolver: config.deconvolver,
                        multiscale_scales: config.multiscale_scales.clone(),
                        small_scale_bias: config.small_scale_bias,
                        clean,
                        clean_mask,
                        w_term_mode: config.w_term_mode,
                        w_project_planes: config.w_project_planes,
                        compatibility: CompatibilityMode::CasaStandardMfs,
                    })
                    .map_err(|error| error.to_string())?,
                )
            }
        }
        PreparedInput::Cube(cube) => RunProducts::Cube(
            run_cube(&CubeImagingRequest {
                geometry,
                channels: cube.channels,
                plane_stokes: cube.plane_stokes,
                weighting: config.weighting,
                weight_density_mode: if config.per_channel_weight_density {
                    WeightDensityMode::PerPlane
                } else {
                    WeightDensityMode::Combined
                },
                uv_taper: config.uv_taper,
                restoring_beam_mode: config.restoring_beam_mode,
                deconvolver: config.deconvolver,
                multiscale_scales: config.multiscale_scales.clone(),
                small_scale_bias: config.small_scale_bias,
                clean: CleanConfig {
                    niter: if config.dirty_only { 0 } else { config.niter },
                    gain: config.gain,
                    threshold_jy_per_beam: config.threshold_jy,
                    nsigma: config.nsigma,
                    psf_cutoff: config.psf_cutoff,
                    minor_cycle_length: config.minor_cycle_length,
                    cyclefactor: config.cyclefactor,
                    min_psf_fraction: config.min_psf_fraction,
                    max_psf_fraction: config.max_psf_fraction,
                },
                clean_mask: build_clean_mask(
                    config.imsize,
                    &config.mask_boxes,
                    config.mask_image.as_deref(),
                )?,
                psf_cutoff: config.psf_cutoff,
                w_term_mode: config.w_term_mode,
                w_project_planes: config.w_project_planes,
                compatibility: CompatibilityMode::CasaStandardMfs,
            })
            .map_err(|error| error.to_string())?,
        ),
    };
    let run_imaging_time = stage_start.elapsed();
    maybe_log_frontend_progress("run_imaging", run_imaging_time, total_start.elapsed());

    let stage_start = Instant::now();
    let coords = build_coordinate_system(
        config.imsize,
        phase_center.angles_rad,
        config.cell_arcsec,
        prepared_freq_ref,
        phase_center.reference,
        run_result.plane_stokes(),
        run_result.channel_frequencies_hz(),
    );
    let build_coordinate_system = stage_start.elapsed();
    maybe_log_frontend_progress(
        "build_coordinate_system",
        build_coordinate_system,
        total_start.elapsed(),
    );
    let stage_start = Instant::now();
    write_products(config, &coords, &run_result)?;
    let write_products_time = stage_start.elapsed();
    maybe_log_frontend_progress("write_products", write_products_time, total_start.elapsed());

    Ok(RunSummary {
        warnings: run_result.warnings(),
        gridded_samples: run_result.gridded_samples(),
        major_cycles: run_result.major_cycles(),
        minor_iterations: run_result.minor_iterations(),
        clean_stop_reason: run_result.clean_stop_reason(),
        channel_summaries: run_result.channel_summaries(),
        stage_timings: run_result.stage_timings(),
        frontend_timings: FrontendStageTimings {
            open_measurement_set,
            prepare_plane_input: prepare_plane_time,
            extract_phase_center,
            run_imaging: run_imaging_time,
            build_coordinate_system,
            write_products: write_products_time,
            total: total_start.elapsed(),
        },
    })
}

fn frontend_progress_enabled() -> bool {
    env::var_os("CASA_RS_IMAGING_PROGRESS").is_some()
}

fn maybe_log_frontend_progress(stage: &str, stage_elapsed: Duration, total_elapsed: Duration) {
    if frontend_progress_enabled() {
        eprintln!(
            "frontend stage={} stage_elapsed_s={:.3} total_elapsed_s={:.3}",
            stage,
            stage_elapsed.as_secs_f64(),
            total_elapsed.as_secs_f64(),
        );
    }
}

/// Trace the standard residual-refresh seam for a single prepared cube channel.
///
/// This reuses the same MeasurementSet preparation path as `run_from_config()`,
/// then rebuilds the per-channel `ImagingRequest` that CASA-style cube imaging
/// uses internally before the residual refresh. The returned diagnostics are
/// intended for oracle and parity work where an external source of truth
/// supplies one target model plane to degrid while all other cube-model planes
/// are treated as zero.
pub fn trace_cube_channel_residual_refresh_from_config(
    config: &CliConfig,
    channel_index: usize,
    model: &Array2<f32>,
) -> Result<ResidualRefreshDiagnostics, String> {
    let geometry = ImageGeometry {
        image_shape: [config.imsize, config.imsize],
        cell_size_rad: [
            config.cell_arcsec * arcsec_to_rad(),
            config.cell_arcsec * arcsec_to_rad(),
        ],
    };
    let mut model_planes = Vec::new();
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let PreparedInput::Cube(cube) = prepared else {
        return Err("trace_cube_channel_residual_refresh_from_config requires cube input".into());
    };
    if model.dim() != (geometry.image_shape[0], geometry.image_shape[1]) {
        return Err(format!(
            "model shape {:?} does not match image geometry {:?}",
            model.dim(),
            geometry.image_shape
        ));
    }
    model_planes.resize_with(cube.channels.len(), || {
        Array2::<f32>::zeros((geometry.image_shape[0], geometry.image_shape[1]))
    });
    let Some(target_model) = model_planes.get_mut(channel_index) else {
        return Err(format!(
            "cube channel index {channel_index} is out of range for {} prepared channels",
            cube.channels.len()
        ));
    };
    *target_model = model.clone();
    let request = CubeImagingRequest {
        geometry,
        channels: cube.channels,
        plane_stokes: cube.plane_stokes,
        weighting: config.weighting,
        weight_density_mode: if config.per_channel_weight_density {
            WeightDensityMode::PerPlane
        } else {
            WeightDensityMode::Combined
        },
        uv_taper: config.uv_taper,
        restoring_beam_mode: config.restoring_beam_mode,
        deconvolver: config.deconvolver,
        multiscale_scales: config.multiscale_scales.clone(),
        small_scale_bias: config.small_scale_bias,
        clean: CleanConfig {
            niter: if config.dirty_only { 0 } else { config.niter },
            gain: config.gain,
            threshold_jy_per_beam: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        psf_cutoff: config.psf_cutoff,
        w_term_mode: config.w_term_mode,
        w_project_planes: config.w_project_planes,
        compatibility: CompatibilityMode::CasaStandardMfs,
    };
    trace_cube_channel_residual_refresh(&request, channel_index, &model_planes)
        .map_err(|error| error.to_string())
}

/// Trace the standard residual-refresh seam for a single prepared cube channel
/// with an explicit full-cube model.
pub fn trace_cube_channel_residual_refresh_from_config_with_model_cube(
    config: &CliConfig,
    channel_index: usize,
    model_planes: &[Array2<f32>],
) -> Result<ResidualRefreshDiagnostics, String> {
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let PreparedInput::Cube(cube) = prepared else {
        return Err("trace_cube_channel_residual_refresh_from_config requires cube input".into());
    };
    let request = CubeImagingRequest {
        geometry: ImageGeometry {
            image_shape: [config.imsize, config.imsize],
            cell_size_rad: [
                config.cell_arcsec * arcsec_to_rad(),
                config.cell_arcsec * arcsec_to_rad(),
            ],
        },
        channels: cube.channels,
        plane_stokes: cube.plane_stokes,
        weighting: config.weighting,
        weight_density_mode: if config.per_channel_weight_density {
            WeightDensityMode::PerPlane
        } else {
            WeightDensityMode::Combined
        },
        uv_taper: config.uv_taper,
        restoring_beam_mode: config.restoring_beam_mode,
        deconvolver: config.deconvolver,
        multiscale_scales: config.multiscale_scales.clone(),
        small_scale_bias: config.small_scale_bias,
        clean: CleanConfig {
            niter: if config.dirty_only { 0 } else { config.niter },
            gain: config.gain,
            threshold_jy_per_beam: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        psf_cutoff: config.psf_cutoff,
        w_term_mode: config.w_term_mode,
        w_project_planes: config.w_project_planes,
        compatibility: CompatibilityMode::CasaStandardMfs,
    };
    trace_cube_channel_residual_refresh(&request, channel_index, model_planes)
        .map_err(|error| error.to_string())
}

/// Trace the standard residual-refresh seam for a single prepared cube channel
/// with an explicit full-cube model while degridding each contributing model
/// plane at its own model-channel frequency.
///
/// This is a diagnostic parity helper for isolating cube prediction semantics.
pub fn trace_cube_channel_residual_refresh_from_config_with_model_cube_model_channel_lambda(
    config: &CliConfig,
    channel_index: usize,
    model_planes: &[Array2<f32>],
) -> Result<ResidualRefreshDiagnostics, String> {
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let PreparedInput::Cube(cube) = prepared else {
        return Err("trace_cube_channel_residual_refresh_from_config requires cube input".into());
    };
    let request = CubeImagingRequest {
        geometry: ImageGeometry {
            image_shape: [config.imsize, config.imsize],
            cell_size_rad: [
                config.cell_arcsec * arcsec_to_rad(),
                config.cell_arcsec * arcsec_to_rad(),
            ],
        },
        channels: cube.channels,
        plane_stokes: cube.plane_stokes,
        weighting: config.weighting,
        weight_density_mode: if config.per_channel_weight_density {
            WeightDensityMode::PerPlane
        } else {
            WeightDensityMode::Combined
        },
        uv_taper: config.uv_taper,
        restoring_beam_mode: config.restoring_beam_mode,
        deconvolver: config.deconvolver,
        multiscale_scales: config.multiscale_scales.clone(),
        small_scale_bias: config.small_scale_bias,
        clean: CleanConfig {
            niter: if config.dirty_only { 0 } else { config.niter },
            gain: config.gain,
            threshold_jy_per_beam: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        psf_cutoff: config.psf_cutoff,
        w_term_mode: config.w_term_mode,
        w_project_planes: config.w_project_planes,
        compatibility: CompatibilityMode::CasaStandardMfs,
    };
    trace_cube_channel_residual_refresh_model_channel_lambda(&request, channel_index, model_planes)
        .map_err(|error| error.to_string())
}

/// Parsed CLI configuration for the standalone imager.
#[derive(Debug, Clone, PartialEq)]
pub struct CliConfig {
    /// Input MeasurementSet path.
    pub ms: PathBuf,
    /// Output image prefix. Products are written as `PREFIX.psf`, `PREFIX.image`, and so on.
    pub imagename: PathBuf,
    /// Square image size in pixels.
    pub imsize: usize,
    /// Cell size in arcseconds.
    pub cell_arcsec: f64,
    /// Optional selected `FIELD_ID`s.
    pub field_ids: Option<Vec<i32>>,
    /// Optional `FIELD_ID` used as the image phase center.
    pub phasecenter_field: Option<i32>,
    /// Optional explicit direction used as the image phase center.
    ///
    /// The current frontend accepts CASA-style fixed J2000 strings such as
    /// `J2000 19:59:28.500 +40.44.01.50`.
    pub phasecenter: Option<String>,
    /// Optional `DATA_DESC_ID` restriction.
    pub ddid: Option<i32>,
    /// Optional spectral-window restriction when DDID is not supplied.
    pub spw: Option<i32>,
    /// Optional CASA-style SPW selector text, including channel clauses.
    pub spw_selector: Option<String>,
    /// Optional first selected channel.
    pub channel_start: Option<usize>,
    /// Optional selected-channel count.
    pub channel_count: Option<usize>,
    /// Optional explicit data-column override.
    pub datacolumn: Option<String>,
    /// Optional explicit scalar-plane override.
    ///
    /// Raw-correlation overrides use `XX`, `YY`, `RR`, or `LL`. Stokes-plane
    /// overrides use `I`, `Q`, `U`, or `V`.
    pub correlation: Option<String>,
    /// Spectral imaging mode.
    pub spectral_mode: SpectralMode,
    /// CASA-style cube-axis configuration for `specmode='cube'` and
    /// `specmode='cubedata'`.
    pub cube_axis: CubeAxisConfig,
    /// Visibility weighting policy.
    pub weighting: WeightingMode,
    /// CASA-style `perchanweightdensity` toggle for spectral cubes.
    pub per_channel_weight_density: bool,
    /// Optional CASA-style Gaussian UV taper.
    pub uv_taper: Option<GaussianUvTaper>,
    /// Restoring-beam policy for restored products.
    pub restoring_beam_mode: RestoringBeamMode,
    /// Requested minor-cycle deconvolver.
    pub deconvolver: Deconvolver,
    /// Requested MTMFS Taylor-term count for `deconvolver='mtmfs'`.
    pub nterms: usize,
    /// Requested multiscale kernel sizes in pixels.
    pub multiscale_scales: Vec<f32>,
    /// CASA-style multiscale selection bias.
    pub small_scale_bias: f32,
    /// Minor-cycle iteration count.
    pub niter: usize,
    /// Minor-cycle loop gain.
    pub gain: f32,
    /// Absolute CLEAN stopping threshold in `Jy/beam`.
    pub threshold_jy: f32,
    /// CASA-style robust-RMS stopping multiplier.
    pub nsigma: f32,
    /// Restoring-beam fit cutoff.
    pub psf_cutoff: f32,
    /// Residual-refresh cadence.
    pub minor_cycle_length: usize,
    /// CASA-style cycle-threshold scale factor.
    pub cyclefactor: f32,
    /// Lower clamp for the PSF fraction used to derive cycle thresholds.
    pub min_psf_fraction: f32,
    /// Upper clamp for the PSF fraction used to derive cycle thresholds.
    pub max_psf_fraction: f32,
    /// Optional inclusive pixel-space clean boxes `(x0, y0, x1, y1)`.
    pub mask_boxes: Vec<[usize; 4]>,
    /// Optional CASA image mask whose non-zero pixels are cleanable.
    pub mask_image: Option<PathBuf>,
    /// Requested `w`-term handling mode.
    pub w_term_mode: WTermMode,
    /// Optional explicit `wproject` plane budget.
    pub w_project_planes: Option<usize>,
    /// Skip CLEAN and only write dirty/residual products.
    pub dirty_only: bool,
    /// Write PNG preview sidecars for the CASA image products.
    pub write_preview_pngs: bool,
}

impl CliConfig {
    /// Parse a standalone-imager configuration from already-split CLI arguments.
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, String> {
        let mut ms = None::<PathBuf>;
        let mut imagename = None::<PathBuf>;
        let mut imsize = None::<usize>;
        let mut cell_arcsec = None::<f64>;
        let mut field_ids = None::<Vec<i32>>;
        let mut phasecenter_field = None::<i32>;
        let mut phasecenter = None::<String>;
        let mut ddid = None::<i32>;
        let mut spw = None::<i32>;
        let mut spw_selector = None::<String>;
        let mut channel_start = None::<usize>;
        let mut channel_count = None::<usize>;
        let mut datacolumn = None::<String>;
        let mut correlation = None::<String>;
        let mut spectral_mode = SpectralMode::Mfs;
        let mut cube_axis = CubeAxisConfig::default();
        let mut weighting_name = String::from("natural");
        let mut per_channel_weight_density = false;
        let mut deconvolver_name = String::from("hogbom");
        let mut nterms = 1usize;
        let mut uv_taper = None::<GaussianUvTaper>;
        let mut restoring_beam_mode = RestoringBeamMode::PerPlane;
        let mut multiscale_scales = Vec::<f32>::new();
        let mut small_scale_bias = 0.0f32;
        let mut robust = 0.5f32;
        let mut niter = 0usize;
        let mut gain = 0.1f32;
        let mut threshold_jy = 0.0f32;
        let mut nsigma = 0.0f32;
        let mut psf_cutoff = 0.35f32;
        let mut minor_cycle_length = 8usize;
        let mut cyclefactor = 1.0f32;
        let mut min_psf_fraction = 0.1f32;
        let mut max_psf_fraction = 0.8f32;
        let mut mask_boxes = Vec::<[usize; 4]>::new();
        let mut mask_image = None::<PathBuf>;
        let mut w_term_mode = WTermMode::None;
        let mut w_project_planes = None::<usize>;
        let mut dirty_only = false;
        let mut write_preview_pngs = true;

        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            let arg = argument.to_string_lossy();
            match arg.as_ref() {
                "--help" | "-h" => {
                    return Err(help_text());
                }
                "--ms" => {
                    ms = Some(next_path(&mut args, "--ms")?);
                    continue;
                }
                "--imagename" => {
                    imagename = Some(next_path(&mut args, "--imagename")?);
                    continue;
                }
                "--imsize" => {
                    imsize = Some(
                        next_value(&mut args, "--imsize")?
                            .parse()
                            .map_err(|error| format!("parse --imsize: {error}"))?,
                    );
                    continue;
                }
                "--cell-arcsec" => {
                    cell_arcsec = Some(
                        next_value(&mut args, "--cell-arcsec")?
                            .parse()
                            .map_err(|error| format!("parse --cell-arcsec: {error}"))?,
                    );
                    continue;
                }
                "--field" => {
                    field_ids = Some(parse_numeric_selector(
                        &next_value(&mut args, "--field")?,
                        "field",
                    )?);
                    continue;
                }
                "--phasecenter-field" => {
                    phasecenter_field = Some(parse_single_numeric_selector(
                        &next_value(&mut args, "--phasecenter-field")?,
                        "phasecenter-field",
                    )?);
                    continue;
                }
                "--phasecenter" => {
                    phasecenter = Some(next_value(&mut args, "--phasecenter")?);
                    continue;
                }
                "--ddid" => {
                    ddid = Some(parse_single_numeric_selector(
                        &next_value(&mut args, "--ddid")?,
                        "ddid",
                    )?);
                    continue;
                }
                "--spw" => {
                    let value = next_value(&mut args, "--spw")?;
                    spw = parse_single_numeric_selector(&value, "spw").ok();
                    spw_selector = Some(value);
                    continue;
                }
                "--channel-start" => {
                    channel_start = Some(
                        next_value(&mut args, "--channel-start")?
                            .parse()
                            .map_err(|error| format!("parse --channel-start: {error}"))?,
                    );
                    continue;
                }
                "--channel-count" => {
                    channel_count = Some(
                        next_value(&mut args, "--channel-count")?
                            .parse()
                            .map_err(|error| format!("parse --channel-count: {error}"))?,
                    );
                    continue;
                }
                "--datacolumn" => {
                    datacolumn = Some(next_value(&mut args, "--datacolumn")?);
                    continue;
                }
                "--corr" | "--stokes" => {
                    correlation = Some(next_value(&mut args, "--corr")?);
                    continue;
                }
                "--specmode" => {
                    spectral_mode = parse_spectral_mode(&next_value(&mut args, "--specmode")?)?;
                    continue;
                }
                "--start" => {
                    cube_axis.start = Some(parse_cube_axis_value(
                        &next_value(&mut args, "--start")?,
                        cube_axis.veltype,
                    )?);
                    continue;
                }
                "--width" => {
                    cube_axis.width = Some(parse_cube_axis_value(
                        &next_value(&mut args, "--width")?,
                        cube_axis.veltype,
                    )?);
                    continue;
                }
                "--outframe" => {
                    cube_axis.outframe = next_value(&mut args, "--outframe")?
                        .parse::<FrequencyRef>()
                        .map_err(|error| format!("parse --outframe: {error}"))?;
                    continue;
                }
                "--veltype" => {
                    cube_axis.veltype = next_value(&mut args, "--veltype")?
                        .parse::<DopplerRef>()
                        .map_err(|error| format!("parse --veltype: {error}"))?;
                    continue;
                }
                "--interpolation" => {
                    cube_axis.interpolation =
                        parse_cube_interpolation(&next_value(&mut args, "--interpolation")?)?;
                    continue;
                }
                "--restfreq" => {
                    cube_axis.rest_frequency_hz = Some(parse_rest_frequency_hz(&next_value(
                        &mut args,
                        "--restfreq",
                    )?)?);
                    continue;
                }
                "--weighting" => {
                    weighting_name = next_value(&mut args, "--weighting")?;
                    continue;
                }
                "--perchanweightdensity" => {
                    per_channel_weight_density = true;
                    continue;
                }
                "--deconvolver" => {
                    deconvolver_name = next_value(&mut args, "--deconvolver")?;
                    continue;
                }
                "--nterms" => {
                    nterms = next_value(&mut args, "--nterms")?
                        .parse()
                        .map_err(|error| format!("parse --nterms: {error}"))?;
                    continue;
                }
                "--scales" => {
                    multiscale_scales =
                        parse_multiscale_scales(&next_value(&mut args, "--scales")?)?;
                    continue;
                }
                "--smallscalebias" => {
                    small_scale_bias = next_value(&mut args, "--smallscalebias")?
                        .parse()
                        .map_err(|error| format!("parse --smallscalebias: {error}"))?;
                    continue;
                }
                "--uvtaper" => {
                    uv_taper = Some(parse_uv_taper(&next_value(&mut args, "--uvtaper")?)?);
                    continue;
                }
                "--restoringbeam" => {
                    restoring_beam_mode = match next_value(&mut args, "--restoringbeam")?
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "common" => RestoringBeamMode::Common,
                        other => {
                            return Err(format!(
                                "unsupported --restoringbeam {other:?}; expected common"
                            ));
                        }
                    };
                    continue;
                }
                "--robust" => {
                    robust = next_value(&mut args, "--robust")?
                        .parse()
                        .map_err(|error| format!("parse --robust: {error}"))?;
                    continue;
                }
                "--niter" => {
                    niter = next_value(&mut args, "--niter")?
                        .parse()
                        .map_err(|error| format!("parse --niter: {error}"))?;
                    continue;
                }
                "--gain" => {
                    gain = next_value(&mut args, "--gain")?
                        .parse()
                        .map_err(|error| format!("parse --gain: {error}"))?;
                    continue;
                }
                "--threshold-jy" => {
                    threshold_jy = next_value(&mut args, "--threshold-jy")?
                        .parse()
                        .map_err(|error| format!("parse --threshold-jy: {error}"))?;
                    continue;
                }
                "--nsigma" => {
                    nsigma = next_value(&mut args, "--nsigma")?
                        .parse()
                        .map_err(|error| format!("parse --nsigma: {error}"))?;
                    continue;
                }
                "--psfcutoff" => {
                    psf_cutoff = next_value(&mut args, "--psfcutoff")?
                        .parse()
                        .map_err(|error| format!("parse --psfcutoff: {error}"))?;
                    continue;
                }
                "--minor-cycle-length" => {
                    minor_cycle_length = next_value(&mut args, "--minor-cycle-length")?
                        .parse()
                        .map_err(|error| format!("parse --minor-cycle-length: {error}"))?;
                    continue;
                }
                "--cycleniter" => {
                    minor_cycle_length = next_value(&mut args, "--cycleniter")?
                        .parse()
                        .map_err(|error| format!("parse --cycleniter: {error}"))?;
                    continue;
                }
                "--cyclefactor" => {
                    cyclefactor = next_value(&mut args, "--cyclefactor")?
                        .parse()
                        .map_err(|error| format!("parse --cyclefactor: {error}"))?;
                    continue;
                }
                "--minpsffraction" => {
                    min_psf_fraction = next_value(&mut args, "--minpsffraction")?
                        .parse()
                        .map_err(|error| format!("parse --minpsffraction: {error}"))?;
                    continue;
                }
                "--maxpsffraction" => {
                    max_psf_fraction = next_value(&mut args, "--maxpsffraction")?
                        .parse()
                        .map_err(|error| format!("parse --maxpsffraction: {error}"))?;
                    continue;
                }
                "--mask-box" => {
                    mask_boxes.push(parse_mask_box(&next_value(&mut args, "--mask-box")?)?);
                    continue;
                }
                "--mask-image" => {
                    mask_image = Some(next_path(&mut args, "--mask-image")?);
                    continue;
                }
                "--wterm" => {
                    w_term_mode = parse_w_term_mode(&next_value(&mut args, "--wterm")?)?;
                    continue;
                }
                "--wprojplanes" => {
                    w_project_planes = Some(
                        next_value(&mut args, "--wprojplanes")?
                            .parse()
                            .map_err(|error| format!("parse --wprojplanes: {error}"))?,
                    );
                    continue;
                }
                "--dirty-only" => {
                    dirty_only = true;
                    continue;
                }
                "--no-preview-pngs" => {
                    write_preview_pngs = false;
                    continue;
                }
                unknown => return Err(format!("unknown argument {unknown:?}\n\n{}", help_text())),
            }
        }

        let weighting = parse_weighting_mode(&weighting_name, robust)?;
        let deconvolver = parse_deconvolver(&deconvolver_name)?;
        cube_axis.specmode = spectral_mode.cube_specmode();
        if phasecenter_field.is_some() && phasecenter.is_some() {
            return Err("--phasecenter and --phasecenter-field are mutually exclusive".to_string());
        }
        if deconvolver == Deconvolver::Mtmfs && spectral_mode != SpectralMode::Mfs {
            return Err("deconvolver='mtmfs' currently requires --specmode mfs".to_string());
        }
        if deconvolver != Deconvolver::Mtmfs && nterms != 1 {
            return Err("nterms > 1 currently requires --deconvolver mtmfs".to_string());
        }
        if nterms == 0 {
            return Err("--nterms must be at least 1".to_string());
        }

        Ok(Self {
            ms: ms.ok_or_else(|| format!("missing --ms\n\n{}", help_text()))?,
            imagename: imagename
                .ok_or_else(|| format!("missing --imagename\n\n{}", help_text()))?,
            imsize: imsize.ok_or_else(|| format!("missing --imsize\n\n{}", help_text()))?,
            cell_arcsec: cell_arcsec
                .ok_or_else(|| format!("missing --cell-arcsec\n\n{}", help_text()))?,
            field_ids,
            phasecenter_field,
            phasecenter,
            ddid,
            spw,
            spw_selector,
            channel_start,
            channel_count,
            datacolumn,
            correlation,
            spectral_mode,
            cube_axis,
            weighting,
            per_channel_weight_density,
            uv_taper,
            restoring_beam_mode,
            deconvolver,
            nterms,
            multiscale_scales,
            small_scale_bias,
            niter,
            gain,
            threshold_jy,
            nsigma,
            psf_cutoff,
            minor_cycle_length,
            cyclefactor,
            min_psf_fraction,
            max_psf_fraction,
            mask_boxes,
            mask_image,
            w_term_mode,
            w_project_planes,
            dirty_only,
            write_preview_pngs,
        })
    }
}

/// Compact run summary returned after a successful CLI-style run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSummary {
    /// Warning strings emitted by the imaging core.
    pub warnings: Vec<String>,
    /// Number of scalar samples that reached the gridder.
    pub gridded_samples: usize,
    /// CASA-style major-cycle count reported for the run.
    ///
    /// When CLEAN is requested, this includes the initial residual
    /// calculation plus each subsequent exact residual refresh.
    pub major_cycles: usize,
    /// Number of Hogbom component updates executed.
    pub minor_iterations: usize,
    /// Final reason why the CLEAN controller stopped, when CLEAN was requested.
    pub clean_stop_reason: Option<CleanStopReason>,
    /// Per-channel cube diagnostics when running cube-like spectral modes,
    /// empty for MFS runs.
    pub channel_summaries: Vec<ChannelRunSummary>,
    /// Stage timing breakdown reported by the pure imaging core.
    pub stage_timings: ImagingStageTimings,
    /// Stage timing breakdown for the MeasurementSet-backed frontend and persistence path.
    pub frontend_timings: FrontendStageTimings,
}

/// Channel-level run summary for cube imaging.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelRunSummary {
    /// Zero-based spectral channel index in the selected output cube.
    pub channel_index: usize,
    /// CASA-style major-cycle count reported for this plane.
    ///
    /// When CLEAN is requested, this includes the initial residual
    /// calculation plus each subsequent exact residual refresh.
    pub major_cycles: usize,
    /// Number of minor-cycle component updates executed for this plane.
    pub minor_iterations: usize,
    /// Final reason why this plane stopped cleaning, when CLEAN was requested.
    pub clean_stop_reason: Option<CleanStopReason>,
    /// Peak absolute residual before CLEAN iterations.
    pub initial_residual_peak_jy_per_beam: f32,
    /// Peak absolute residual after the final refresh.
    pub final_residual_peak_jy_per_beam: f32,
    /// Final CASA-style cycle threshold used for this plane.
    pub final_cycle_threshold_jy_per_beam: f32,
    /// Per-block minor-cycle trace recorded by the shared imaging library.
    pub minor_cycle_traces: Vec<MinorCycleTrace>,
    /// PSF beam-fit search diagnostics for this plane, when available.
    pub beam_fit_debug: Option<BeamFitDebugSummary>,
}

/// Stage timing breakdown for the MeasurementSet-backed frontend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrontendStageTimings {
    /// Time spent opening the MeasurementSet and its top-level metadata.
    pub open_measurement_set: Duration,
    /// Time spent resolving selection identity, spectral setup, and adapting MAIN rows into `VisibilityBatch` values.
    pub prepare_plane_input: Duration,
    /// Time spent extracting and validating the phase center.
    pub extract_phase_center: Duration,
    /// Time spent inside the pure `casa-imaging` core.
    pub run_imaging: Duration,
    /// Time spent building the CASA coordinate system metadata for output products.
    pub build_coordinate_system: Duration,
    /// Time spent writing CASA image products and optional preview sidecars.
    pub write_products: Duration,
    /// Total elapsed time for `run_from_config()`.
    pub total: Duration,
}

struct PlaneInput {
    phase_center: PhaseCenter,
    freq_ref: FrequencyRef,
    reffreq_hz: f64,
    selected_frequency_range_hz: [f64; 2],
    plane_stokes: PlaneStokes,
    batches: Vec<VisibilityBatch>,
    sample_frequency_batches_hz: Vec<Vec<f64>>,
    gridder_mode: GridderMode,
}

struct CubePlaneInput {
    phase_center: PhaseCenter,
    freq_ref: FrequencyRef,
    plane_stokes: PlaneStokes,
    channels: Vec<CubeChannelRequest>,
}

enum PreparedInput {
    Mfs(PlaneInput),
    Cube(CubePlaneInput),
}

impl PreparedInput {
    fn phase_center(&self) -> &PhaseCenter {
        match self {
            Self::Mfs(plane) => &plane.phase_center,
            Self::Cube(cube) => &cube.phase_center,
        }
    }
}

enum RunProducts {
    Mfs(casa_imaging::ImagingResult),
    Mtmfs(casa_imaging::MtmfsResult),
    Cube(casa_imaging::CubeImagingResult),
}

impl RunProducts {
    fn plane_stokes(&self) -> PlaneStokes {
        match self {
            Self::Mfs(result) => result.compatibility.plane_stokes,
            Self::Mtmfs(result) => result.compatibility.plane_stokes,
            Self::Cube(result) => result.compatibility.plane_stokes,
        }
    }

    fn channel_frequencies_hz(&self) -> &[f64] {
        match self {
            Self::Mfs(result) => &result.compatibility.channel_frequencies_hz,
            Self::Mtmfs(result) => &result.compatibility.channel_frequencies_hz,
            Self::Cube(result) => &result.compatibility.channel_frequencies_hz,
        }
    }

    fn warnings(&self) -> Vec<String> {
        match self {
            Self::Mfs(result) => result.diagnostics.warnings.clone(),
            Self::Mtmfs(result) => result.diagnostics.warnings.clone(),
            Self::Cube(result) => result.diagnostics.warnings.clone(),
        }
    }

    fn gridded_samples(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.gridded_samples,
            Self::Mtmfs(result) => result.diagnostics.gridded_samples,
            Self::Cube(result) => result.diagnostics.gridded_samples,
        }
    }

    fn major_cycles(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.major_cycles,
            Self::Mtmfs(result) => result.diagnostics.major_cycles,
            Self::Cube(result) => result.diagnostics.major_cycles,
        }
    }

    fn minor_iterations(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.minor_iterations,
            Self::Mtmfs(result) => result.diagnostics.minor_iterations,
            Self::Cube(result) => result.diagnostics.minor_iterations,
        }
    }

    fn clean_stop_reason(&self) -> Option<CleanStopReason> {
        match self {
            Self::Mfs(result) => result.diagnostics.clean_stop_reason,
            Self::Mtmfs(result) => result.diagnostics.clean_stop_reason,
            Self::Cube(result) => result.diagnostics.clean_stop_reason,
        }
    }

    fn channel_summaries(&self) -> Vec<ChannelRunSummary> {
        match self {
            Self::Mfs(_) => Vec::new(),
            Self::Mtmfs(_) => Vec::new(),
            Self::Cube(result) => result
                .diagnostics
                .channel_diagnostics
                .iter()
                .enumerate()
                .map(|(channel_index, diag)| ChannelRunSummary {
                    channel_index,
                    major_cycles: diag.major_cycles,
                    minor_iterations: diag.minor_iterations,
                    clean_stop_reason: diag.clean_stop_reason,
                    initial_residual_peak_jy_per_beam: diag.initial_residual_peak_jy_per_beam,
                    final_residual_peak_jy_per_beam: diag.final_residual_peak_jy_per_beam,
                    final_cycle_threshold_jy_per_beam: diag.final_cycle_threshold_jy_per_beam,
                    minor_cycle_traces: diag.minor_cycle_traces.clone(),
                    beam_fit_debug: diag.beam_fit_debug.clone(),
                })
                .collect(),
        }
    }

    fn stage_timings(&self) -> ImagingStageTimings {
        match self {
            Self::Mfs(result) => result.diagnostics.stage_timings,
            Self::Mtmfs(result) => result.diagnostics.stage_timings,
            Self::Cube(result) => result.diagnostics.stage_timings,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PhaseCenter {
    field_id: Option<usize>,
    angles_rad: [f64; 2],
    reference: DirectionRef,
}

#[derive(Debug, Clone)]
struct SelectedMainRow {
    row_index: usize,
    field_id: usize,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    time_mjd_seconds: Option<f64>,
}

impl SelectedMainRow {
    fn trace(&self) -> SelectedRowTrace {
        SelectedRowTrace {
            row_index: self.row_index,
            field_id: self.field_id,
            ddid: self.ddid,
            spw_id: self.spw_id,
            polarization_id: self.polarization_id,
            time_mjd_seconds: self.time_mjd_seconds,
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedGeometryRow {
    selected_row: SelectedMainRow,
    phase_center_field_id: Option<usize>,
    pointing_id: Option<i32>,
    field_phase_center_direction_rad: [f64; 2],
    antenna1_pointing: ResolvedPointingDirection,
    antenna2_pointing: ResolvedPointingDirection,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    raw_uvw_m: [f64; 3],
    transform: RowImagingTransform,
}

impl PreparedGeometryRow {
    fn trace(&self) -> PreparedGeometryRowTrace {
        PreparedGeometryRowTrace {
            row_index: self.selected_row.row_index,
            input_field_id: self.selected_row.field_id,
            phase_center_field_id: self.phase_center_field_id,
            ddid: self.selected_row.ddid,
            spw_id: self.selected_row.spw_id,
            polarization_id: self.selected_row.polarization_id,
            pointing_id: self.pointing_id,
            antenna1_pointing_row: self.antenna1_pointing.source_row_index,
            antenna1_pointing_direction_rad: self.antenna1_pointing.angles_rad,
            antenna1_pointing_used_fallback: self.antenna1_pointing.used_fallback,
            antenna2_pointing_row: self.antenna2_pointing.source_row_index,
            antenna2_pointing_direction_rad: self.antenna2_pointing.angles_rad,
            antenna2_pointing_used_fallback: self.antenna2_pointing.used_fallback,
            antenna1_id: self.antenna1_id,
            antenna2_id: self.antenna2_id,
            is_cross: self.is_cross,
            raw_uvw_m: self.raw_uvw_m,
            imaging_uvw_m: self.transform.uvw_m,
            phase_shift_m: self.transform.phase_shift_m,
            field_phase_center_direction_rad: self.field_phase_center_direction_rad,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedPointingDirection {
    source_row_index: Option<usize>,
    used_fallback: bool,
    angles_rad: [f64; 2],
}

#[derive(Debug, Clone, Copy)]
struct PointingDirectionRow {
    row_index: usize,
    antenna_id: i32,
    time_mjd_seconds: f64,
    interval_seconds: f64,
    angles_rad: [f64; 2],
}

#[derive(Debug, Clone)]
struct PointingDirectionResolver {
    by_antenna: BTreeMap<i32, Vec<PointingDirectionRow>>,
    by_row_index: HashMap<usize, PointingDirectionRow>,
}

impl PointingDirectionResolver {
    fn new(ms: &MeasurementSet) -> Result<Option<Self>, String> {
        let Ok(pointing) = ms.pointing() else {
            return Ok(None);
        };
        if pointing.row_count() == 0 {
            return Ok(None);
        }
        let table = pointing.table();
        let mut by_antenna = BTreeMap::<i32, Vec<PointingDirectionRow>>::new();
        let mut by_row_index = HashMap::<usize, PointingDirectionRow>::new();
        for row_index in 0..table.row_count() {
            let antenna_id = match table
                .get_scalar_cell(row_index, "ANTENNA_ID")
                .map_err(|error| format!("read POINTING.ANTENNA_ID row {row_index}: {error}"))?
            {
                ScalarValue::Int32(value) => *value,
                other => {
                    return Err(format!(
                        "POINTING.ANTENNA_ID row {row_index} must be Int32, found {:?}",
                        other.primitive_type()
                    ));
                }
            };
            let time_mjd_seconds = match table
                .get_scalar_cell(row_index, "TIME")
                .map_err(|error| format!("read POINTING.TIME row {row_index}: {error}"))?
            {
                ScalarValue::Float64(value) => *value,
                other => {
                    return Err(format!(
                        "POINTING.TIME row {row_index} must be Float64, found {:?}",
                        other.primitive_type()
                    ));
                }
            };
            let interval_seconds = match table
                .get_scalar_cell(row_index, "INTERVAL")
                .map_err(|error| format!("read POINTING.INTERVAL row {row_index}: {error}"))?
            {
                ScalarValue::Float64(value) => *value,
                other => {
                    return Err(format!(
                        "POINTING.INTERVAL row {row_index} must be Float64, found {:?}",
                        other.primitive_type()
                    ));
                }
            };
            let angles_rad = extract_constant_direction_angles(
                table
                    .get_array_cell(row_index, "DIRECTION")
                    .map_err(|error| format!("read POINTING.DIRECTION row {row_index}: {error}"))?,
                "POINTING.DIRECTION",
                row_index,
            )?;
            let entry = PointingDirectionRow {
                row_index,
                antenna_id,
                time_mjd_seconds,
                interval_seconds,
                angles_rad,
            };
            by_antenna.entry(antenna_id).or_default().push(entry);
            by_row_index.insert(row_index, entry);
        }
        for entries in by_antenna.values_mut() {
            entries.sort_by(|left, right| {
                left.time_mjd_seconds
                    .partial_cmp(&right.time_mjd_seconds)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        Ok(Some(Self {
            by_antenna,
            by_row_index,
        }))
    }

    fn resolve(
        &self,
        pointing_id: Option<i32>,
        antenna_id: i32,
        time_mjd_seconds: f64,
        fallback_angles_rad: [f64; 2],
    ) -> ResolvedPointingDirection {
        if let Some(pointing_row) = pointing_id
            .and_then(|value| usize::try_from(value).ok())
            .and_then(|row_index| self.by_row_index.get(&row_index).copied())
            .filter(|entry| entry.antenna_id == antenna_id)
        {
            return ResolvedPointingDirection {
                source_row_index: Some(pointing_row.row_index),
                used_fallback: false,
                angles_rad: pointing_row.angles_rad,
            };
        }
        let Some(entries) = self.by_antenna.get(&antenna_id) else {
            return ResolvedPointingDirection {
                source_row_index: None,
                used_fallback: true,
                angles_rad: fallback_angles_rad,
            };
        };
        let lower = entries.partition_point(|entry| entry.time_mjd_seconds < time_mjd_seconds);
        for candidate_index in [lower.checked_sub(1), Some(lower)].into_iter().flatten() {
            let entry = entries[candidate_index];
            if time_mjd_seconds >= entry.time_mjd_seconds - entry.interval_seconds
                && time_mjd_seconds <= entry.time_mjd_seconds + entry.interval_seconds
            {
                return ResolvedPointingDirection {
                    source_row_index: Some(entry.row_index),
                    used_fallback: false,
                    angles_rad: entry.angles_rad,
                };
            }
        }
        ResolvedPointingDirection {
            source_row_index: None,
            used_fallback: true,
            angles_rad: fallback_angles_rad,
        }
    }
}

#[derive(Debug, Clone)]
struct SelectedRowsContext {
    selected_rows: Vec<SelectedMainRow>,
    selected_ddid: usize,
    phase_center: PhaseCenter,
    reference_row_time_mjd_sec: Option<f64>,
    time_bounds_mjd_sec: Option<[f64; 2]>,
    needs_geometry_engine: bool,
}

#[derive(Debug, Clone)]
struct TraceSampleCommon {
    row_index: usize,
    input_field_id: usize,
    phase_center_field_id: Option<usize>,
    ddid: usize,
    spw_id: usize,
    polarization_id: usize,
    antenna1_id: i32,
    antenna2_id: i32,
    is_cross: bool,
    raw_uvw_m: [f64; 3],
    imaging_uvw_m: [f64; 3],
    phase_shift_m: f64,
    output_channel_index: Option<usize>,
    output_frequency_hz: f64,
    field_phase_center_direction_rad: [f64; 2],
    pointing_direction_rad: [f64; 2],
    source_contributions: Vec<ChannelContributionTrace>,
    gridable: bool,
}

#[derive(Debug, Clone)]
struct PendingPairedSampleTrace {
    common: TraceSampleCommon,
    correlation_indices: [usize; 2],
    first_visibility: Complex32,
    second_visibility: Complex32,
    first_weight: f32,
    second_weight: f32,
    first_weight_source: WeightSourceKind,
    second_weight_source: WeightSourceKind,
    first_flagged: bool,
    second_flagged: bool,
}

enum CollapsedPairTrace {
    Accepted(PreparedVisibilitySampleTrace),
    Rejected(RejectedPreparedVisibilitySampleTrace),
}

enum PreparedTraceState {
    ExplicitMfs {
        samples: Vec<PreparedVisibilitySampleTrace>,
    },
    ExplicitCube {
        channel_samples: Vec<Vec<PreparedVisibilitySampleTrace>>,
    },
    PairedMfs {
        samples: Vec<PendingPairedSampleTrace>,
    },
    PairedCube {
        channel_samples: Vec<Vec<PendingPairedSampleTrace>>,
    },
}

fn select_main_rows(
    ms: &MeasurementSet,
    config: &CliConfig,
    ddid_info: &[Option<(usize, usize)>],
) -> Result<SelectedRowsContext, String> {
    let needs_pointing_times = ms
        .pointing()
        .map(|pointing| pointing.row_count() > 0)
        .unwrap_or(false);
    let needs_row_times = config.spectral_mode.is_cube_like()
        || config.w_term_mode != WTermMode::None
        || selection_may_require_phase_reprojection(config)
        || needs_pointing_times;
    let field_column = ms
        .main_table()
        .get_column("FIELD_ID")
        .map_err(|error| format!("open FIELD_ID column: {error}"))?;
    let ddid_column = ms
        .main_table()
        .get_column("DATA_DESC_ID")
        .map_err(|error| format!("open DATA_DESC_ID column: {error}"))?;
    let allowed_ddids = allowed_ddids(config, ddid_info)?;
    let time_column = TimeColumn::new(ms.main_table());
    let allowed_field_ids = config
        .field_ids
        .as_ref()
        .map(|ids| ids.iter().copied().collect::<BTreeSet<_>>());
    let mut selected_fields = BTreeSet::<i32>::new();
    let mut selected_ddid = None::<i32>;
    let mut selected_rows = Vec::<SelectedMainRow>::new();
    let mut reference_row_time_mjd_sec = None::<f64>;
    let mut time_bounds_mjd_sec = None::<[f64; 2]>;

    for (field_cell, ddid_cell) in field_column.zip(ddid_column) {
        let row = field_cell.row_index;
        let field_id = match field_cell.value {
            Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
            Some(other) => {
                return Err(format!(
                    "FIELD_ID row {row} must be Int32, found {:?}",
                    other.kind()
                ));
            }
            None => return Err(format!("FIELD_ID row {row} is missing")),
        };
        let ddid = match ddid_cell.value {
            Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
            Some(other) => {
                return Err(format!(
                    "DATA_DESC_ID row {row} must be Int32, found {:?}",
                    other.kind()
                ));
            }
            None => return Err(format!("DATA_DESC_ID row {row} is missing")),
        };
        if ddid < 0 {
            continue;
        }
        if allowed_field_ids
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&field_id))
        {
            continue;
        }
        if config.ddid.is_some_and(|value| value != ddid) {
            continue;
        }
        if !allowed_ddids.is_empty() && !allowed_ddids[ddid as usize] {
            continue;
        }

        selected_fields.insert(field_id);
        selected_ddid = combine_single(selected_ddid, ddid, "DATA_DESC_ID")?;
        let field_id_usize = usize::try_from(field_id)
            .map_err(|_| format!("FIELD_ID row {row} must be non-negative, found {field_id}"))?;
        let row_time_mjd_sec = if needs_row_times {
            let row_time_mjd_sec = time_column
                .get_mjd_seconds(row)
                .map_err(|error| format!("read TIME row {row}: {error}"))?;
            reference_row_time_mjd_sec.get_or_insert(row_time_mjd_sec);
            if config.spectral_mode.is_cube_like() {
                match &mut time_bounds_mjd_sec {
                    Some(bounds) => {
                        bounds[0] = bounds[0].min(row_time_mjd_sec);
                        bounds[1] = bounds[1].max(row_time_mjd_sec);
                    }
                    None => {
                        time_bounds_mjd_sec = Some([row_time_mjd_sec, row_time_mjd_sec]);
                    }
                }
            }
            Some(row_time_mjd_sec)
        } else {
            None
        };
        let (spw_id, polarization_id) = ddid_info
            .get(ddid as usize)
            .copied()
            .flatten()
            .ok_or_else(|| format!("map DDID {ddid} to SPW/POLARIZATION"))?;
        selected_rows.push(SelectedMainRow {
            row_index: row,
            field_id: field_id_usize,
            ddid: ddid as usize,
            spw_id,
            polarization_id,
            time_mjd_seconds: row_time_mjd_sec,
        });
    }

    if selected_fields.is_empty() {
        return Err("selection resolved to no field".to_string());
    }
    if selected_rows.is_empty() {
        return Err("selection resolved to no rows".to_string());
    }
    if config.spectral_mode.is_cube_like() && config.phasecenter.is_some() {
        return Err(
            "explicit --phasecenter is currently supported only for specmode=mfs; use --phasecenter-field for cube imaging"
                .to_string(),
        );
    }
    let phase_center = resolve_phase_center(ms, &selected_fields, config)?;
    let selected_ddid = selected_ddid.ok_or_else(|| "selection resolved to no DDID".to_string())?;
    let needs_geometry_engine = config.spectral_mode.is_cube_like()
        || config.w_term_mode != WTermMode::None
        || config.phasecenter.is_some()
        || selected_fields
            .iter()
            .copied()
            .any(|field_id| usize::try_from(field_id).ok() != phase_center.field_id);

    Ok(SelectedRowsContext {
        selected_rows,
        selected_ddid: selected_ddid as usize,
        phase_center,
        reference_row_time_mjd_sec,
        time_bounds_mjd_sec,
        needs_geometry_engine,
    })
}

fn load_optional_i32_main_column(
    ms: &MeasurementSet,
    column_name: &'static str,
) -> Result<Option<Vec<Option<i32>>>, String> {
    let Ok(column) = ms.main_table().get_column(column_name) else {
        return Ok(None);
    };
    let mut values = vec![None; ms.main_table().row_count()];
    for cell in column {
        let value = match cell.value {
            Some(Value::Scalar(ScalarValue::Int32(value))) => Some(*value),
            Some(other) => {
                return Err(format!(
                    "{column_name} row {} must be Int32, found {:?}",
                    cell.row_index,
                    other.kind()
                ));
            }
            None => None,
        };
        values[cell.row_index] = value;
    }
    Ok(Some(values))
}

fn extract_constant_direction_angles(
    value: &ArrayValue,
    column_name: &str,
    row_index: usize,
) -> Result<[f64; 2], String> {
    match value {
        ArrayValue::Float64(values) => {
            let shape = values.shape();
            if shape.len() != 2 || shape[0] != 2 || shape[1] == 0 {
                return Err(format!(
                    "{column_name} row {row_index} must have shape [2, N], found {shape:?}"
                ));
            }
            Ok([values[[0, 0]], values[[1, 0]]])
        }
        other => Err(format!(
            "{column_name} row {row_index} must be Float64 array, found {:?}",
            other.primitive_type()
        )),
    }
}

fn build_prepared_geometry_rows(
    ms: &MeasurementSet,
    selected_rows: &[SelectedMainRow],
    phase_center: &PhaseCenter,
    derived_engine: Option<&MsCalEngine>,
) -> Result<Vec<PreparedGeometryRow>, String> {
    let uvw = UvwColumn::new(ms.main_table());
    let antenna1 = main_ids::antenna1(ms.main_table());
    let antenna2 = main_ids::antenna2(ms.main_table());
    let pointing_ids = load_optional_i32_main_column(ms, "POINTING_ID")?;
    let pointing_resolver = PointingDirectionResolver::new(ms)?;
    let mut field_phase_centers = BTreeMap::<usize, [f64; 2]>::new();
    let mut rows = Vec::with_capacity(selected_rows.len());
    for selected_row in selected_rows {
        let row = selected_row.row_index;
        let antenna1_id = antenna1
            .get(row)
            .map_err(|error| format!("read ANTENNA1 row {row}: {error}"))?;
        let antenna2_id = antenna2
            .get(row)
            .map_err(|error| format!("read ANTENNA2 row {row}: {error}"))?;
        let is_cross = antenna1_id != antenna2_id;
        let raw_uvw_m = uvw
            .get(row)
            .map_err(|error| format!("read UVW row {row}: {error}"))?;
        let transform = row_imaging_transform(
            row,
            selected_row.field_id,
            phase_center,
            raw_uvw_m,
            derived_engine,
        )?;
        let row_phase_center =
            if let Some(angles_rad) = field_phase_centers.get(&selected_row.field_id) {
                *angles_rad
            } else {
                let direction = resolve_field_phase_direction_j2000(ms, selected_row.field_id)
                    .map_err(|error| {
                        format!(
                            "resolve FIELD.PHASE_DIR[{}] to J2000 for row {row}: {error}",
                            selected_row.field_id
                        )
                    })?;
                let (ra, dec) = direction.as_angles();
                let angles_rad = [ra, dec];
                field_phase_centers.insert(selected_row.field_id, angles_rad);
                angles_rad
            };
        let antenna1_pointing = match (pointing_resolver.as_ref(), selected_row.time_mjd_seconds) {
            (Some(resolver), Some(time_mjd_seconds)) => resolver.resolve(
                pointing_ids
                    .as_ref()
                    .and_then(|values| values.get(row))
                    .copied()
                    .flatten(),
                antenna1_id,
                time_mjd_seconds,
                row_phase_center,
            ),
            (Some(_), None) => {
                return Err(format!(
                    "row {row} requires TIME to resolve POINTING directions"
                ));
            }
            (None, _) => ResolvedPointingDirection {
                source_row_index: None,
                used_fallback: true,
                angles_rad: row_phase_center,
            },
        };
        let antenna2_pointing = match (pointing_resolver.as_ref(), selected_row.time_mjd_seconds) {
            (Some(resolver), Some(time_mjd_seconds)) => resolver.resolve(
                pointing_ids
                    .as_ref()
                    .and_then(|values| values.get(row))
                    .copied()
                    .flatten(),
                antenna2_id,
                time_mjd_seconds,
                row_phase_center,
            ),
            (Some(_), None) => {
                return Err(format!(
                    "row {row} requires TIME to resolve POINTING directions"
                ));
            }
            (None, _) => ResolvedPointingDirection {
                source_row_index: None,
                used_fallback: true,
                angles_rad: row_phase_center,
            },
        };
        rows.push(PreparedGeometryRow {
            selected_row: selected_row.clone(),
            phase_center_field_id: phase_center.field_id,
            pointing_id: pointing_ids
                .as_ref()
                .and_then(|values| values.get(row))
                .copied()
                .flatten(),
            field_phase_center_direction_rad: row_phase_center,
            antenna1_pointing,
            antenna2_pointing,
            antenna1_id,
            antenna2_id,
            is_cross,
            raw_uvw_m,
            transform,
        });
    }
    Ok(rows)
}

fn prepare_plane_input(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
) -> Result<PreparedInput, String> {
    prepare_plane_input_with_trace(ms, config, data_column_kind).map(|(prepared, _)| prepared)
}

fn prepare_plane_input_with_trace(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
) -> Result<(PreparedInput, PreparedVisibilityTraceBundle), String> {
    let data_description = ms
        .data_description()
        .map_err(|error| format!("open DATA_DESCRIPTION: {error}"))?;
    let ddid_info = data_description_index(&data_description)?;
    let spectral_window = ms
        .spectral_window()
        .map_err(|error| format!("open SPECTRAL_WINDOW: {error}"))?;
    let polarization = ms
        .polarization()
        .map_err(|error| format!("open POLARIZATION: {error}"))?;
    let data_column = ms
        .data_column(data_column_kind)
        .map_err(|error| format!("open data column: {error}"))?;
    let flag_column = ms.flag_column();
    let flag_row = ms.flag_row_column();
    let weight_column = ms.weight_column();
    let weight_spectrum = WeightSpectrumColumn::new(ms.main_table()).ok();
    let selection = select_main_rows(ms, config, &ddid_info)?;
    let derived_engine = if selection.needs_geometry_engine {
        Some(MsCalEngine::new(ms).map_err(|error| format!("build derived engine: {error}"))?)
    } else {
        None
    };
    let geometry_rows = build_prepared_geometry_rows(
        ms,
        &selection.selected_rows,
        &selection.phase_center,
        derived_engine.as_ref(),
    )?;
    let cube_context = if config.spectral_mode.is_cube_like() {
        Some(CubeSetupContext {
            phase_center_field_id: selection.phase_center.field_id.ok_or_else(|| {
                "internal error: cube imaging requires a field-backed phase center".to_string()
            })?,
            reference_row_time_mjd_sec: selection
                .reference_row_time_mjd_sec
                .ok_or_else(|| "selection resolved to no cube rows".to_string())?,
            time_bounds_mjd_sec: selection
                .time_bounds_mjd_sec
                .ok_or_else(|| "selection resolved to no cube time bounds".to_string())?,
            derived_engine: derived_engine
                .as_ref()
                .expect("cube mode always builds a derived engine"),
        })
    } else {
        None
    };
    let mut prepared = PreparedSelection::new(
        config,
        selection.selected_ddid,
        &ddid_info,
        &spectral_window,
        &polarization,
        selection.phase_center.clone(),
        cube_context,
    );
    if let Some(init_error) = prepared.initialization_error.take() {
        return Err(init_error);
    }
    let selected_row_traces = selection
        .selected_rows
        .iter()
        .map(SelectedMainRow::trace)
        .collect::<Vec<_>>();
    for row in &geometry_rows {
        prepared.accumulate_row(
            row,
            &data_column,
            &flag_column,
            &flag_row,
            &weight_column,
            weight_spectrum.as_ref(),
            derived_engine.as_ref(),
        )?;
    }
    prepared.finish_with_trace(
        ms,
        config.ms.display().to_string(),
        canonical_data_column_name(data_column_kind).to_string(),
        config.spectral_mode,
        PhaseCenterTrace {
            field_id: selection.phase_center.field_id,
            reference: selection.phase_center.reference.as_str().to_string(),
            angles_rad: selection.phase_center.angles_rad,
        },
        selected_row_traces,
    )
}

fn data_description_index(
    data_description: &casa_ms::subtables::data_description::MsDataDescription<'_>,
) -> Result<Vec<Option<(usize, usize)>>, String> {
    let mut index = Vec::with_capacity(data_description.row_count());
    for row in 0..data_description.row_count() {
        let spw_id = data_description
            .spectral_window_id(row)
            .map_err(|error| format!("map DDID {row} to SPW: {error}"))?;
        let polarization_id = data_description
            .polarization_id(row)
            .map_err(|error| format!("map DDID {row} to POLARIZATION: {error}"))?;
        if spw_id < 0 || polarization_id < 0 {
            index.push(None);
        } else {
            index.push(Some((spw_id as usize, polarization_id as usize)));
        }
    }
    Ok(index)
}

fn allowed_ddids(
    config: &CliConfig,
    ddid_info: &[Option<(usize, usize)>],
) -> Result<Vec<bool>, String> {
    let mut allowed = vec![true; ddid_info.len()];
    let selected_spw = selected_spw_id(config)?;
    if let Some(spw) = selected_spw {
        allowed.fill(false);
        for (ddid, info) in ddid_info.iter().enumerate() {
            if info.is_some_and(|(row_spw, _)| row_spw == spw as usize) {
                allowed[ddid] = true;
            }
        }
        if !allowed.iter().any(|value| *value) {
            return Err(format!("selection resolved to no DDID for SPW {spw}"));
        }
    }
    if let Some(ddid) = config.ddid {
        if ddid < 0 || ddid as usize >= ddid_info.len() || ddid_info[ddid as usize].is_none() {
            return Err(format!(
                "DATA_DESC_ID {ddid} is outside the DATA_DESCRIPTION table"
            ));
        }
        if let Some(spw) = selected_spw
            && ddid_info[ddid as usize].is_none_or(|(row_spw, _)| row_spw != spw as usize)
        {
            return Err(format!("DATA_DESC_ID {ddid} does not map to SPW {spw}"));
        }
    }
    Ok(allowed)
}

fn selected_spw_id(config: &CliConfig) -> Result<Option<i32>, String> {
    if let Some(selector_text) = config.spw_selector.as_deref() {
        let selectors = parse_spw_selector(selector_text).map_err(|error| error.to_string())?;
        let ids = selectors
            .iter()
            .map(|selector| selector.spw_id)
            .collect::<std::collections::BTreeSet<_>>();
        return match ids.len() {
            0 => Ok(None),
            1 => Ok(ids.into_iter().next()),
            _ => Err(format!(
                "spw selector {selector_text:?} resolved to multiple SPWs; the current frontend accepts exactly one"
            )),
        };
    }
    Ok(config.spw)
}

fn selected_spw_channel_selector(
    config: &CliConfig,
    spw_id: usize,
) -> Result<Option<casa_ms::ChannelSelection>, String> {
    let Some(selector_text) = config.spw_selector.as_deref() else {
        return Ok(None);
    };
    let selectors = parse_spw_selector(selector_text).map_err(|error| error.to_string())?;
    let Some(selector) = selectors
        .into_iter()
        .find(|selector| selector.spw_id == spw_id as i32)
    else {
        return Ok(None);
    };
    Ok(selector.channels)
}

fn selection_may_require_phase_reprojection(config: &CliConfig) -> bool {
    config.phasecenter_field.is_some()
        || config.phasecenter.is_some()
        || config
            .field_ids
            .as_ref()
            .is_none_or(|field_ids| field_ids.len() != 1)
}

fn extract_phase_center(ms: &MeasurementSet, field_id: usize) -> Result<PhaseCenter, String> {
    let field = ms.field().map_err(|error| format!("open FIELD: {error}"))?;
    if field
        .num_poly(field_id)
        .map_err(|error| format!("read FIELD.NUM_POLY: {error}"))?
        != 0
    {
        return Err(
            "moving or tracked phase centers (NUM_POLY != 0) are not supported".to_string(),
        );
    }
    let phase_dir = resolve_field_phase_direction_j2000(ms, field_id)
        .map_err(|error| format!("resolve FIELD.PHASE_DIR[{field_id}] to J2000: {error}"))?;
    let (ra, dec) = phase_dir.as_angles();
    Ok(PhaseCenter {
        field_id: Some(field_id),
        angles_rad: [ra, dec],
        reference: DirectionRef::J2000,
    })
}

fn resolve_phase_center(
    ms: &MeasurementSet,
    selected_fields: &BTreeSet<i32>,
    config: &CliConfig,
) -> Result<PhaseCenter, String> {
    let Some(first_selected) = selected_fields.iter().next().copied() else {
        return Err("selection resolved to no field".to_string());
    };
    if let Some(text) = config.phasecenter.as_deref() {
        return parse_phase_center_literal(text);
    }
    if let Some(field_id) = config.phasecenter_field {
        if !selected_fields.contains(&field_id) {
            return Err(format!(
                "phase-center FIELD_ID {field_id} is not part of the selected field set {:?}",
                selected_fields
            ));
        }
        return extract_phase_center(ms, field_id as usize);
    }
    if selected_fields.len() == 1 {
        return extract_phase_center(ms, first_selected as usize);
    }
    let reference = extract_phase_center(ms, first_selected as usize)?;
    let all_match = selected_fields.iter().copied().all(|field_id| {
        extract_phase_center(ms, field_id as usize)
            .map(|candidate| {
                candidate.reference == reference.reference
                    && (candidate.angles_rad[0] - reference.angles_rad[0]).abs() <= 1.0e-12
                    && (candidate.angles_rad[1] - reference.angles_rad[1]).abs() <= 1.0e-12
            })
            .unwrap_or(false)
    });
    if all_match {
        Ok(reference)
    } else {
        Err(format!(
            "field selection {:?} spans multiple phase centers; set --phasecenter-field",
            selected_fields
        ))
    }
}

fn parse_phase_center_literal(text: &str) -> Result<PhaseCenter, String> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(format!(
            "phasecenter {text:?} must be 'REF lon lat', for example 'J2000 19:59:28.500 +40.44.01.50'"
        ));
    }
    let reference = parts[0]
        .parse::<DirectionRef>()
        .map_err(|error| format!("parse phasecenter reference {:?}: {error}", parts[0]))?;
    if reference != DirectionRef::J2000 {
        return Err(format!(
            "phasecenter reference {reference:?} is not supported yet; expected J2000"
        ));
    }
    let ra = parse_phase_center_angle(parts[1], true)?;
    let dec = parse_phase_center_angle(parts[2], false)?;
    Ok(PhaseCenter {
        field_id: None,
        angles_rad: [ra, dec],
        reference,
    })
}

fn parse_phase_center_angle(text: &str, longitude: bool) -> Result<f64, String> {
    let unit = Unit::new("rad").expect("rad is a valid unit");
    if let Ok(quantity) = text.parse::<Quantity>() {
        return quantity
            .get_value_in(&unit)
            .map_err(|error| format!("convert phasecenter angle {text:?} to radians: {error}"));
    }
    if let Some(value) = parse_hms_token(text) {
        if longitude {
            return Ok(value * std::f64::consts::PI / 12.0);
        }
        return Err(format!(
            "phasecenter latitude {text:?} looks like a right ascension; expected a declination"
        ));
    }
    if let Some(value) = parse_dms_token(text) {
        if longitude {
            return Ok(value * std::f64::consts::PI / 180.0);
        }
        return Ok(value * std::f64::consts::PI / 180.0);
    }
    Err(format!(
        "unsupported phasecenter angle {text:?}; use a quantity like '1.2rad'/'40deg' or sexagesimal text"
    ))
}

fn parse_hms_token(text: &str) -> Option<f64> {
    let normalized = normalize_sexagesimal_token(text, true)?;
    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let hours = parts[0].parse::<f64>().ok()?;
    let minutes = parts[1].parse::<f64>().ok()?;
    let seconds = parts[2].parse::<f64>().ok()?;
    let sign = if hours < 0.0 { -1.0 } else { 1.0 };
    Some(sign * (hours.abs() + minutes / 60.0 + seconds / 3600.0))
}

fn parse_dms_token(text: &str) -> Option<f64> {
    let normalized = normalize_sexagesimal_token(text, false)?;
    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let degrees = parts[0].parse::<f64>().ok()?;
    let minutes = parts[1].parse::<f64>().ok()?;
    let seconds = parts[2].parse::<f64>().ok()?;
    let sign = if degrees < 0.0 { -1.0 } else { 1.0 };
    Some(sign * (degrees.abs() + minutes / 60.0 + seconds / 3600.0))
}

fn normalize_sexagesimal_token(text: &str, hour_style: bool) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let sign = if trimmed.starts_with('-') {
        "-"
    } else if trimmed.starts_with('+') {
        "+"
    } else {
        ""
    };
    let body = trimmed.trim_start_matches(['+', '-']);
    let split_by = if body.contains(':') {
        Some(body.split(':').map(str::to_string).collect::<Vec<_>>())
    } else if body.contains('h') || body.contains('d') || body.contains('m') || body.contains('s') {
        let normalized = body
            .replace(['h', 'd', 'm', 's'], " ")
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        Some(normalized)
    } else if !hour_style && body.matches('.').count() >= 2 {
        let mut parts = body.split('.').map(str::to_string).collect::<Vec<_>>();
        let tail = parts.split_off(2);
        let mut normalized = parts;
        normalized.push(tail.join("."));
        Some(normalized)
    } else {
        None
    }?;
    if split_by.len() != 3 {
        return None;
    }
    let mut normalized = split_by;
    normalized[0] = format!("{sign}{}", normalized[0]);
    Some(normalized.join(" "))
}

fn resolve_data_column(
    ms: &MeasurementSet,
    explicit: Option<&str>,
) -> Result<VisibilityDataColumn, String> {
    if let Some(name) = explicit {
        return parse_data_column(name);
    }
    if ms.data_column(VisibilityDataColumn::CorrectedData).is_ok() {
        Ok(VisibilityDataColumn::CorrectedData)
    } else if ms.data_column(VisibilityDataColumn::Data).is_ok() {
        Ok(VisibilityDataColumn::Data)
    } else {
        Err("MS has neither CORRECTED_DATA nor DATA".to_string())
    }
}

fn parse_data_column(name: &str) -> Result<VisibilityDataColumn, String> {
    match name.to_ascii_uppercase().as_str() {
        "DATA" => Ok(VisibilityDataColumn::Data),
        "CORRECTED_DATA" | "CORRECTED" => Ok(VisibilityDataColumn::CorrectedData),
        "MODEL_DATA" | "MODEL" => Ok(VisibilityDataColumn::ModelData),
        _ => Err(format!("unsupported data column {name:?}")),
    }
}

fn parse_single_numeric_selector(value: &str, label: &str) -> Result<i32, String> {
    let parsed = parse_numeric_id_selector(value, label).map_err(|error| error.to_string())?;
    match parsed.as_slice() {
        [single] => Ok(*single),
        [] => Err(format!("{label} selector {value:?} resolved to no ids")),
        _ => Err(format!(
            "{label} selector {value:?} resolved to multiple ids; the current frontend accepts exactly one"
        )),
    }
}

fn parse_numeric_selector(value: &str, label: &str) -> Result<Vec<i32>, String> {
    let ids = parse_numeric_id_selector(value, label).map_err(|error| error.to_string())?;
    if ids.is_empty() {
        Err(format!("{label} selector {value:?} resolved to no ids"))
    } else {
        Ok(ids)
    }
}

fn parse_cube_axis_value(text: &str, veltype: DopplerRef) -> Result<CubeAxisValue, String> {
    CubeAxisValue::parse(text, veltype).map_err(|error| error.to_string())
}

fn parse_cube_interpolation(text: &str) -> Result<CubeInterpolation, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "nearest" => Ok(CubeInterpolation::Nearest),
        "linear" => Ok(CubeInterpolation::Linear),
        "cubic" => Err(
            "unsupported cube interpolation \"cubic\"; cubic is not implemented yet".to_string(),
        ),
        other => Err(format!("unsupported cube interpolation {other:?}")),
    }
}

fn parse_rest_frequency_hz(text: &str) -> Result<f64, String> {
    parse_ms_rest_frequency_hz(text).map_err(|error| error.to_string())
}

struct PreparedSelection {
    initialization_error: Option<String>,
    source_channel_indices: Vec<usize>,
    source_channel_frequencies_hz: Vec<f64>,
    source_channel_widths_hz: Vec<f64>,
    selected_frequency_range_hz: [f64; 2],
    reffreq_hz: f64,
    freq_ref: FrequencyRef,
    cube_spectral_setup: Option<CubeSpectralSetup>,
    cube_row_spectral_cache: HashMap<(u64, usize), Rc<CubeRowSpectralContributions>>,
    phase_center: PhaseCenter,
    state: PreparedState,
    trace_state: PreparedTraceState,
}

#[derive(Clone, Copy)]
struct CubeSetupContext<'a> {
    phase_center_field_id: usize,
    reference_row_time_mjd_sec: f64,
    time_bounds_mjd_sec: [f64; 2],
    derived_engine: &'a MsCalEngine,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RowImagingTransform {
    uvw_m: [f64; 3],
    phase_shift_m: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairCollapseTransform {
    HalfSum,
    HalfDifference,
    PositiveHalfImagDifference,
    NegativeHalfImagDifference,
}

enum PreparedState {
    ExplicitMfs {
        plane_stokes: PlaneStokes,
        corr_index: usize,
        batch: VisibilityBatch,
    },
    ExplicitCube {
        plane_stokes: PlaneStokes,
        corr_index: usize,
        channel_batches: Vec<VisibilityBatch>,
        channel_model_interpolation_samples: Vec<Vec<Vec<CubeModelChannelContribution>>>,
    },
    PairedMfs {
        plane_stokes: PlaneStokes,
        transform: PairCollapseTransform,
        paired: ParallelHandBatch,
        pair: (usize, usize),
    },
    PairedCube {
        plane_stokes: PlaneStokes,
        transform: PairCollapseTransform,
        channel_batches: Vec<ParallelHandBatch>,
        channel_model_interpolation_samples: Vec<Vec<Vec<CubeModelChannelContribution>>>,
        pair: (usize, usize),
    },
}

fn row_imaging_transform(
    row: usize,
    row_field_id: usize,
    phase_center: &PhaseCenter,
    raw_uvw_m: [f64; 3],
    derived_engine: Option<&MsCalEngine>,
) -> Result<RowImagingTransform, String> {
    if phase_center.field_id == Some(row_field_id) {
        return Ok(RowImagingTransform {
            uvw_m: raw_uvw_m,
            phase_shift_m: 0.0,
        });
    }

    let imaging_transform = if let Some(phase_center_field_id) = phase_center.field_id {
        reproject_row_uvw_m(
            row,
            raw_uvw_m,
            derived_engine,
            row_field_id,
            phase_center_field_id,
        )?
    } else {
        reproject_row_uvw_to_phase_center(
            row,
            raw_uvw_m,
            derived_engine,
            row_field_id,
            phase_center,
        )?
    };
    Ok(RowImagingTransform {
        uvw_m: imaging_transform.0,
        phase_shift_m: imaging_transform.1,
    })
}

fn reproject_row_uvw_m(
    row: usize,
    raw_uvw_m: [f64; 3],
    derived_engine: Option<&MsCalEngine>,
    source_field_id: usize,
    target_field_id: usize,
) -> Result<([f64; 3], f64), String> {
    let derived_engine = derived_engine
        .ok_or_else(|| "internal error: missing derived engine for row reprojection".to_string())?;
    derived_engine
        .reproject_raw_uvw_between_fields(raw_uvw_m, source_field_id, target_field_id)
        .map_err(|error| format!("reproject UVW row {row} between field phase centers: {error}"))
}

fn reproject_row_uvw_to_phase_center(
    row: usize,
    raw_uvw_m: [f64; 3],
    derived_engine: Option<&MsCalEngine>,
    source_field_id: usize,
    phase_center: &PhaseCenter,
) -> Result<([f64; 3], f64), String> {
    let derived_engine = derived_engine
        .ok_or_else(|| "internal error: missing derived engine for row reprojection".to_string())?;
    let target = MDirection::from_angles(
        phase_center.angles_rad[0],
        phase_center.angles_rad[1],
        phase_center.reference,
    );
    derived_engine
        .reproject_raw_uvw_to_direction(raw_uvw_m, source_field_id, &target)
        .map_err(|error| format!("reproject UVW row {row} to explicit phase center: {error}"))
}

fn phase_rotate_visibility(
    visibility: Complex32,
    phase_shift_m: f64,
    frequency_hz: f64,
) -> Complex32 {
    if phase_shift_m == 0.0 || frequency_hz == 0.0 {
        return visibility;
    }
    let phase = -std::f64::consts::TAU * phase_shift_m * frequency_hz / SPEED_OF_LIGHT_M_PER_S;
    let phasor = Complex32::new(phase.cos() as f32, phase.sin() as f32);
    visibility * phasor
}

impl PreparedSelection {
    fn new(
        config: &CliConfig,
        ddid: usize,
        ddid_info: &[Option<(usize, usize)>],
        spectral_window: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
        polarization: &casa_ms::subtables::polarization::MsPolarization<'_>,
        phase_center: PhaseCenter,
        cube_context: Option<CubeSetupContext<'_>>,
    ) -> Self {
        let result = (|| -> Result<Self, String> {
            let (spw_id, polarization_id) = ddid_info
                .get(ddid)
                .copied()
                .flatten()
                .ok_or_else(|| format!("map DDID {ddid} to SPW/POLARIZATION"))?;
            let spw_freqs = spectral_window
                .chan_freq(spw_id)
                .map_err(|error| format!("read CHAN_FREQ: {error}"))?;
            let spw_widths = spectral_window
                .chan_width(spw_id)
                .map_err(|error| format!("read CHAN_WIDTH: {error}"))?;
            let freq_ref = FrequencyRef::from_casacore_code(
                spectral_window
                    .meas_freq_ref(spw_id)
                    .map_err(|error| format!("read MEAS_FREQ_REF: {error}"))?,
            )
            .unwrap_or(FrequencyRef::TOPO);
            let explicit_channel_selector =
                selected_spw_channel_selector(config, spw_id).map_err(|error| error.to_string())?;
            let mut source_channel_selection =
                match (&config.spectral_mode, explicit_channel_selector.as_ref()) {
                    (_, Some(selector)) => resolve_channel_selector_selection(&spw_freqs, selector)
                        .map_err(|error| error.to_string())?,
                    (SpectralMode::Mfs, None) => resolve_contiguous_channel_selection(
                        &spw_freqs,
                        config.channel_start,
                        config.channel_count,
                    )
                    .map_err(|error| error.to_string())?,
                    (SpectralMode::Cube | SpectralMode::Cubedata, None) => {
                        resolve_contiguous_channel_selection(
                            &spw_freqs,
                            Some(0),
                            Some(spw_freqs.len()),
                        )
                        .map_err(|error| error.to_string())?
                    }
                };
            let cube_spectral_setup = if config.spectral_mode.is_cube_like() {
                let cube_context = cube_context
                    .ok_or_else(|| "internal error: missing cube setup context".to_string())?;
                let mut cube_axis = config.cube_axis.clone();
                cube_axis.specmode = config.spectral_mode.cube_specmode();
                if cube_axis.start.is_none() {
                    cube_axis.start = config
                        .channel_start
                        .map(|value| CubeAxisValue::Channel(value as i32))
                        .or_else(|| {
                            explicit_channel_selector
                                .as_ref()
                                .and_then(|_| source_channel_selection.indices.first().copied())
                                .map(|value| CubeAxisValue::Channel(value as i32))
                        });
                }
                let (cube_setup, support_selection) = CubeSpectralSetup::for_casa_cube_axis(
                    freq_ref,
                    &spw_freqs,
                    &spw_widths,
                    config.channel_count.unwrap_or(spw_freqs.len()),
                    &cube_axis,
                    cube_context.reference_row_time_mjd_sec,
                    cube_context.phase_center_field_id,
                    cube_context.time_bounds_mjd_sec,
                    cube_context.derived_engine,
                )
                .map_err(|error| error.to_string())?;
                if explicit_channel_selector.is_none() {
                    source_channel_selection = support_selection;
                }
                Some(cube_setup)
            } else {
                None
            };
            let source_channel_frequencies_hz = source_channel_selection.frequencies_hz.clone();
            let source_channel_widths_hz = source_channel_selection
                .indices
                .iter()
                .map(|&index| {
                    spw_widths.get(index).copied().ok_or_else(|| {
                        format!(
                            "channel width selection index {index} is outside SPW width array with {} channels",
                            spw_widths.len()
                        )
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let output_channel_frequencies_hz = cube_spectral_setup
                .as_ref()
                .map(|setup| setup.output_channel_frequencies_hz.clone())
                .unwrap_or_else(|| source_channel_frequencies_hz.clone());
            let output_freq_ref = cube_spectral_setup
                .as_ref()
                .map(|setup| setup.output_freq_ref)
                .unwrap_or(freq_ref);
            let selected_frequency_range_hz = [
                *output_channel_frequencies_hz
                    .first()
                    .ok_or_else(|| "channel selection resolved to zero frequencies".to_string())?,
                *output_channel_frequencies_hz
                    .last()
                    .ok_or_else(|| "channel selection resolved to zero frequencies".to_string())?,
            ];
            let reffreq_hz =
                0.5 * (selected_frequency_range_hz[0] + selected_frequency_range_hz[1]);
            let corr_types = polarization
                .corr_type(polarization_id)
                .map_err(|error| format!("read CORR_TYPE: {error}"))?;
            let max_samples = source_channel_frequencies_hz.len();
            let explicit_plane = config
                .correlation
                .as_deref()
                .map(parse_plane_stokes)
                .transpose()?;
            let use_explicit_corr = explicit_plane.and_then(plane_to_corr_code).is_some();
            let state = if let Some(plane_stokes) = explicit_plane {
                if let Some(corr_code) = plane_to_corr_code(plane_stokes) {
                    let corr_index = corr_types
                        .iter()
                        .position(|code| *code == corr_code)
                        .ok_or_else(|| {
                            format!(
                                "requested raw correlation plane {} is not present",
                                plane_stokes.as_str()
                            )
                        })?;
                    match config.spectral_mode {
                        SpectralMode::Mfs => PreparedState::ExplicitMfs {
                            plane_stokes,
                            corr_index,
                            batch: empty_visibility_batch(max_samples),
                        },
                        SpectralMode::Cube | SpectralMode::Cubedata => {
                            PreparedState::ExplicitCube {
                                plane_stokes,
                                corr_index,
                                channel_batches: output_channel_frequencies_hz
                                    .iter()
                                    .map(|_| empty_visibility_batch(16))
                                    .collect(),
                                channel_model_interpolation_samples: output_channel_frequencies_hz
                                    .iter()
                                    .map(|_| Vec::new())
                                    .collect(),
                            }
                        }
                    }
                } else {
                    let (pair, transform) =
                        derive_stokes_pair_selection(plane_stokes, &corr_types)?;
                    match config.spectral_mode {
                        SpectralMode::Mfs => PreparedState::PairedMfs {
                            plane_stokes,
                            transform,
                            pair,
                            paired: empty_parallel_hand_batch(max_samples),
                        },
                        SpectralMode::Cube | SpectralMode::Cubedata => PreparedState::PairedCube {
                            plane_stokes,
                            transform,
                            pair,
                            channel_batches: output_channel_frequencies_hz
                                .iter()
                                .map(|_| empty_parallel_hand_batch(16))
                                .collect(),
                            channel_model_interpolation_samples: output_channel_frequencies_hz
                                .iter()
                                .map(|_| Vec::new())
                                .collect(),
                        },
                    }
                }
            } else {
                let (pair, transform) = derive_stokes_pair_selection(PlaneStokes::I, &corr_types)?;
                match config.spectral_mode {
                    SpectralMode::Mfs => PreparedState::PairedMfs {
                        plane_stokes: PlaneStokes::I,
                        transform,
                        pair,
                        paired: empty_parallel_hand_batch(max_samples),
                    },
                    SpectralMode::Cube | SpectralMode::Cubedata => PreparedState::PairedCube {
                        plane_stokes: PlaneStokes::I,
                        transform,
                        pair,
                        channel_batches: output_channel_frequencies_hz
                            .iter()
                            .map(|_| empty_parallel_hand_batch(16))
                            .collect(),
                        channel_model_interpolation_samples: output_channel_frequencies_hz
                            .iter()
                            .map(|_| Vec::new())
                            .collect(),
                    },
                }
            };
            let trace_state = if use_explicit_corr {
                match config.spectral_mode {
                    SpectralMode::Mfs => PreparedTraceState::ExplicitMfs {
                        samples: Vec::new(),
                    },
                    SpectralMode::Cube | SpectralMode::Cubedata => {
                        PreparedTraceState::ExplicitCube {
                            channel_samples: output_channel_frequencies_hz
                                .iter()
                                .map(|_| Vec::new())
                                .collect(),
                        }
                    }
                }
            } else {
                match config.spectral_mode {
                    SpectralMode::Mfs => PreparedTraceState::PairedMfs {
                        samples: Vec::new(),
                    },
                    SpectralMode::Cube | SpectralMode::Cubedata => PreparedTraceState::PairedCube {
                        channel_samples: output_channel_frequencies_hz
                            .iter()
                            .map(|_| Vec::new())
                            .collect(),
                    },
                }
            };
            Ok(Self {
                initialization_error: None,
                source_channel_indices: source_channel_selection.indices,
                source_channel_frequencies_hz,
                source_channel_widths_hz,
                selected_frequency_range_hz,
                reffreq_hz,
                freq_ref: output_freq_ref,
                cube_spectral_setup,
                cube_row_spectral_cache: HashMap::new(),
                phase_center,
                state,
                trace_state,
            })
        })();
        match result {
            Ok(selection) => selection,
            Err(error) => Self {
                initialization_error: Some(error),
                source_channel_indices: Vec::new(),
                source_channel_frequencies_hz: Vec::new(),
                source_channel_widths_hz: Vec::new(),
                selected_frequency_range_hz: [0.0, 0.0],
                reffreq_hz: 0.0,
                freq_ref: FrequencyRef::TOPO,
                cube_spectral_setup: None,
                cube_row_spectral_cache: HashMap::new(),
                phase_center: PhaseCenter {
                    field_id: Some(0),
                    angles_rad: [0.0, 0.0],
                    reference: DirectionRef::J2000,
                },
                state: PreparedState::ExplicitMfs {
                    plane_stokes: PlaneStokes::I,
                    corr_index: 0,
                    batch: empty_visibility_batch(0),
                },
                trace_state: PreparedTraceState::ExplicitMfs {
                    samples: Vec::new(),
                },
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn accumulate_row(
        &mut self,
        geometry_row: &PreparedGeometryRow,
        data_column: &DataColumn<'_>,
        flag_column: &FlagColumn<'_>,
        flag_row: &FlagRowColumn<'_>,
        weight_column: &casa_ms::columns::weight_columns::WeightColumn<'_>,
        weight_spectrum: Option<&WeightSpectrumColumn<'_>>,
        derived_engine: Option<&MsCalEngine>,
    ) -> Result<(), String> {
        let selected_row = &geometry_row.selected_row;
        let row = selected_row.row_index;
        if flag_row
            .get(row)
            .map_err(|error| format!("read FLAG_ROW row {row}: {error}"))?
        {
            return Ok(());
        }
        let data = data_column
            .get(row)
            .map_err(|error| format!("read data row {row}: {error}"))?;
        let flags = flag_column
            .get(row)
            .map_err(|error| format!("read FLAG row {row}: {error}"))?;
        let row_weights = weight_column
            .get(row)
            .map_err(|error| format!("read WEIGHT row {row}: {error}"))?;
        let weight_spectrum_row = weight_spectrum.and_then(|column| column.get(row).ok());
        let antenna1_id = geometry_row.antenna1_id;
        let antenna2_id = geometry_row.antenna2_id;
        let is_cross = geometry_row.is_cross;
        let raw_uvw_m = geometry_row.raw_uvw_m;
        let transform = geometry_row.transform;
        let uvw_m = transform.uvw_m;
        let baseline_pointing_direction_rad = combine_pointing_direction_rad(
            geometry_row.antenna1_pointing.angles_rad,
            geometry_row.antenna2_pointing.angles_rad,
        );
        let cube_output_channel_frequencies_hz = self
            .cube_spectral_setup
            .as_ref()
            .map(|setup| setup.output_channel_frequencies_hz.clone());
        let cube_row_spectral_contributions = if matches!(
            &self.state,
            PreparedState::ExplicitCube { .. } | PreparedState::PairedCube { .. }
        ) {
            let cube_setup = self
                .cube_spectral_setup
                .as_ref()
                .ok_or_else(|| "internal error: missing cube spectral setup".to_string())?;
            let derived_engine = derived_engine.ok_or_else(|| {
                "internal error: missing derived engine for cube imaging".to_string()
            })?;
            let row_time_mjd_sec = selected_row
                .time_mjd_seconds
                .ok_or_else(|| "internal error: missing row time for cube imaging".to_string())?;
            let cache_key = (row_time_mjd_sec.to_bits(), selected_row.field_id);
            if let Some(cached) = self.cube_row_spectral_cache.get(&cache_key) {
                Some(Rc::clone(cached))
            } else {
                let computed = Rc::new(
                    cube_setup
                        .row_spectral_contributions(
                            &self.source_channel_frequencies_hz,
                            &self.source_channel_widths_hz,
                            row_time_mjd_sec,
                            selected_row.field_id,
                            derived_engine,
                        )
                        .map_err(|error| error.to_string())?,
                );
                self.cube_row_spectral_cache
                    .insert(cache_key, Rc::clone(&computed));
                Some(computed)
            }
        } else {
            None
        };

        match (&mut self.state, &mut self.trace_state) {
            (
                PreparedState::ExplicitMfs {
                    corr_index, batch, ..
                },
                PreparedTraceState::ExplicitMfs { samples },
            ) => {
                batch
                    .u_lambda
                    .reserve(self.source_channel_frequencies_hz.len());
                batch
                    .v_lambda
                    .reserve(self.source_channel_frequencies_hz.len());
                batch
                    .w_lambda
                    .reserve(self.source_channel_frequencies_hz.len());
                batch
                    .weight
                    .reserve(self.source_channel_frequencies_hz.len());
                batch
                    .sumwt_factor
                    .reserve(self.source_channel_frequencies_hz.len());
                batch
                    .gridable
                    .reserve(self.source_channel_frequencies_hz.len());
                batch
                    .visibility
                    .reserve(self.source_channel_frequencies_hz.len());
                samples.reserve(self.source_channel_frequencies_hz.len());
                for (channel_slot, (channel_index, frequency_hz)) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                    .enumerate()
                {
                    if bool_at_2d(flags, *corr_index, channel_index)? {
                        continue;
                    }
                    let visibility = phase_rotate_visibility(
                        complex32_at_2d(data, *corr_index, channel_index)?,
                        transform.phase_shift_m,
                        frequency_hz,
                    );
                    let (weight, weight_source) = resolve_weight_with_source(
                        row_weights,
                        weight_spectrum_row,
                        *corr_index,
                        channel_index,
                    )?;
                    if !(weight.is_finite() && weight > 0.0) {
                        continue;
                    }
                    let lambda_scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    batch.weight.push(weight);
                    batch.sumwt_factor.push(1.0);
                    batch.gridable.push(is_cross);
                    batch.visibility.push(visibility);
                    samples.push(PreparedVisibilitySampleTrace {
                        row_index: selected_row.row_index,
                        input_field_id: selected_row.field_id,
                        phase_center_field_id: self.phase_center.field_id,
                        ddid: selected_row.ddid,
                        spw_id: selected_row.spw_id,
                        polarization_id: selected_row.polarization_id,
                        antenna1_id,
                        antenna2_id,
                        is_cross,
                        raw_uvw_m,
                        imaging_uvw_m: uvw_m,
                        phase_shift_m: transform.phase_shift_m,
                        correlation_indices: vec![*corr_index],
                        output_channel_index: None,
                        output_frequency_hz: frequency_hz,
                        field_phase_center_direction_rad: geometry_row
                            .field_phase_center_direction_rad,
                        pointing_direction_rad: baseline_pointing_direction_rad,
                        visibility_re: visibility.re,
                        visibility_im: visibility.im,
                        weight,
                        weight_source,
                        sumwt_factor: 1.0,
                        gridable: is_cross,
                        source_contributions: vec![ChannelContributionTrace {
                            source_channel_slot: channel_slot,
                            source_channel_index: channel_index,
                            source_frequency_hz: frequency_hz,
                            factor: 1.0,
                        }],
                    });
                }
            }
            (
                PreparedState::ExplicitCube {
                    corr_index,
                    channel_batches,
                    channel_model_interpolation_samples,
                    ..
                },
                PreparedTraceState::ExplicitCube { channel_samples },
            ) => {
                let row_spectral_contributions = cube_row_spectral_contributions
                    .as_ref()
                    .expect("cube spectral contributions prepared for cube state");
                let source_model_contributions =
                    &row_spectral_contributions.source_channel_model_contributions;
                let assignments = &row_spectral_contributions.output_channel_contributions;
                for (output_channel, contributions) in assignments.iter().enumerate() {
                    if contributions.is_empty() {
                        continue;
                    }
                    let output_frequency_hz = cube_output_channel_frequencies_hz
                        .as_ref()
                        .expect("missing cube spectral setup")[output_channel];
                    let Some(sample) = interpolate_explicit_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *corr_index,
                        &self.source_channel_indices,
                        &self.source_channel_frequencies_hz,
                        transform.phase_shift_m,
                        contributions,
                    )?
                    else {
                        continue;
                    };
                    if !(output_frequency_hz.is_finite() && output_frequency_hz > 0.0) {
                        continue;
                    }
                    let lambda_scale = output_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    let batch = &mut channel_batches[output_channel];
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    batch.weight.push(sample.weight);
                    batch.sumwt_factor.push(sample.sumwt_factor);
                    batch.gridable.push(is_cross);
                    batch.visibility.push(sample.visibility);
                    channel_model_interpolation_samples[output_channel].push(
                        combine_model_channel_contributions(
                            contributions,
                            source_model_contributions,
                        ),
                    );
                    channel_samples[output_channel].push(PreparedVisibilitySampleTrace {
                        row_index: selected_row.row_index,
                        input_field_id: selected_row.field_id,
                        phase_center_field_id: self.phase_center.field_id,
                        ddid: selected_row.ddid,
                        spw_id: selected_row.spw_id,
                        polarization_id: selected_row.polarization_id,
                        antenna1_id,
                        antenna2_id,
                        is_cross,
                        raw_uvw_m,
                        imaging_uvw_m: uvw_m,
                        phase_shift_m: transform.phase_shift_m,
                        correlation_indices: vec![*corr_index],
                        output_channel_index: Some(output_channel),
                        output_frequency_hz,
                        field_phase_center_direction_rad: geometry_row
                            .field_phase_center_direction_rad,
                        pointing_direction_rad: baseline_pointing_direction_rad,
                        visibility_re: sample.visibility.re,
                        visibility_im: sample.visibility.im,
                        weight: sample.weight,
                        weight_source: sample.weight_source,
                        sumwt_factor: sample.sumwt_factor,
                        gridable: is_cross,
                        source_contributions: build_source_contribution_traces(
                            &self.source_channel_indices,
                            &self.source_channel_frequencies_hz,
                            contributions,
                        ),
                    });
                }
            }
            (
                PreparedState::PairedMfs { paired, pair, .. },
                PreparedTraceState::PairedMfs { samples },
            ) => {
                paired
                    .u_lambda
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .v_lambda
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .w_lambda
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .first_visibility
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .second_visibility
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .first_weight
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .second_weight
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .first_flagged
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .second_flagged
                    .reserve(self.source_channel_frequencies_hz.len());
                paired
                    .gridable
                    .reserve(self.source_channel_frequencies_hz.len());
                samples.reserve(self.source_channel_frequencies_hz.len());
                for (channel_slot, (channel_index, frequency_hz)) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                    .enumerate()
                {
                    let lambda_scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    let first_visibility = phase_rotate_visibility(
                        complex32_at_2d(data, pair.0, channel_index)?,
                        transform.phase_shift_m,
                        frequency_hz,
                    );
                    let second_visibility = phase_rotate_visibility(
                        complex32_at_2d(data, pair.1, channel_index)?,
                        transform.phase_shift_m,
                        frequency_hz,
                    );
                    let (first_weight, first_weight_source) = resolve_weight_with_source(
                        row_weights,
                        weight_spectrum_row,
                        pair.0,
                        channel_index,
                    )?;
                    let (second_weight, second_weight_source) = resolve_weight_with_source(
                        row_weights,
                        weight_spectrum_row,
                        pair.1,
                        channel_index,
                    )?;
                    let first_flagged = bool_at_2d(flags, pair.0, channel_index)?;
                    let second_flagged = bool_at_2d(flags, pair.1, channel_index)?;
                    paired.u_lambda.push(uvw_m[0] * lambda_scale);
                    paired.v_lambda.push(uvw_m[1] * lambda_scale);
                    paired.w_lambda.push(uvw_m[2] * lambda_scale);
                    paired.first_visibility.push(first_visibility);
                    paired.second_visibility.push(second_visibility);
                    paired.first_weight.push(first_weight);
                    paired.second_weight.push(second_weight);
                    paired.first_flagged.push(first_flagged);
                    paired.second_flagged.push(second_flagged);
                    paired.gridable.push(is_cross);
                    samples.push(PendingPairedSampleTrace {
                        common: TraceSampleCommon {
                            row_index: selected_row.row_index,
                            input_field_id: selected_row.field_id,
                            phase_center_field_id: self.phase_center.field_id,
                            ddid: selected_row.ddid,
                            spw_id: selected_row.spw_id,
                            polarization_id: selected_row.polarization_id,
                            antenna1_id,
                            antenna2_id,
                            is_cross,
                            raw_uvw_m,
                            imaging_uvw_m: uvw_m,
                            phase_shift_m: transform.phase_shift_m,
                            output_channel_index: None,
                            output_frequency_hz: frequency_hz,
                            field_phase_center_direction_rad: geometry_row
                                .field_phase_center_direction_rad,
                            pointing_direction_rad: baseline_pointing_direction_rad,
                            source_contributions: vec![ChannelContributionTrace {
                                source_channel_slot: channel_slot,
                                source_channel_index: channel_index,
                                source_frequency_hz: frequency_hz,
                                factor: 1.0,
                            }],
                            gridable: is_cross,
                        },
                        correlation_indices: [pair.0, pair.1],
                        first_visibility,
                        second_visibility,
                        first_weight,
                        second_weight,
                        first_weight_source,
                        second_weight_source,
                        first_flagged,
                        second_flagged,
                    });
                }
            }
            (
                PreparedState::PairedCube {
                    channel_batches,
                    channel_model_interpolation_samples,
                    pair,
                    ..
                },
                PreparedTraceState::PairedCube { channel_samples },
            ) => {
                let row_spectral_contributions = cube_row_spectral_contributions
                    .as_ref()
                    .expect("cube spectral contributions prepared for cube state");
                let source_model_contributions =
                    &row_spectral_contributions.source_channel_model_contributions;
                let assignments = &row_spectral_contributions.output_channel_contributions;
                for (output_channel, contributions) in assignments.iter().enumerate() {
                    if contributions.is_empty() {
                        continue;
                    }
                    let output_frequency_hz = cube_output_channel_frequencies_hz
                        .as_ref()
                        .expect("missing cube spectral setup")[output_channel];
                    let Some(sample) = interpolate_paired_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *pair,
                        &self.source_channel_indices,
                        &self.source_channel_frequencies_hz,
                        transform.phase_shift_m,
                        contributions,
                    )?
                    else {
                        continue;
                    };
                    if !(output_frequency_hz.is_finite() && output_frequency_hz > 0.0) {
                        continue;
                    }
                    let lambda_scale = output_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    let batch = &mut channel_batches[output_channel];
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    batch.first_visibility.push(sample.first_visibility);
                    batch.second_visibility.push(sample.second_visibility);
                    batch.first_weight.push(sample.first_weight);
                    batch.second_weight.push(sample.second_weight);
                    batch.first_flagged.push(false);
                    batch.second_flagged.push(false);
                    batch.gridable.push(is_cross);
                    channel_model_interpolation_samples[output_channel].push(
                        combine_model_channel_contributions(
                            contributions,
                            source_model_contributions,
                        ),
                    );
                    channel_samples[output_channel].push(PendingPairedSampleTrace {
                        common: TraceSampleCommon {
                            row_index: selected_row.row_index,
                            input_field_id: selected_row.field_id,
                            phase_center_field_id: self.phase_center.field_id,
                            ddid: selected_row.ddid,
                            spw_id: selected_row.spw_id,
                            polarization_id: selected_row.polarization_id,
                            antenna1_id,
                            antenna2_id,
                            is_cross,
                            raw_uvw_m,
                            imaging_uvw_m: uvw_m,
                            phase_shift_m: transform.phase_shift_m,
                            output_channel_index: Some(output_channel),
                            output_frequency_hz,
                            field_phase_center_direction_rad: geometry_row
                                .field_phase_center_direction_rad,
                            pointing_direction_rad: baseline_pointing_direction_rad,
                            source_contributions: build_source_contribution_traces(
                                &self.source_channel_indices,
                                &self.source_channel_frequencies_hz,
                                contributions,
                            ),
                            gridable: is_cross,
                        },
                        correlation_indices: [pair.0, pair.1],
                        first_visibility: sample.first_visibility,
                        second_visibility: sample.second_visibility,
                        first_weight: sample.first_weight,
                        second_weight: sample.second_weight,
                        first_weight_source: sample.first_weight_source,
                        second_weight_source: sample.second_weight_source,
                        first_flagged: false,
                        second_flagged: false,
                    });
                }
            }
            _ => {
                return Err(
                    "internal error: prepared sample state and trace state are out of sync"
                        .to_string(),
                );
            }
        }
        Ok(())
    }

    fn finish_with_trace(
        self,
        ms: &MeasurementSet,
        ms_path: String,
        data_column: String,
        spectral_mode: SpectralMode,
        phase_center: PhaseCenterTrace,
        selected_rows: Vec<SelectedRowTrace>,
    ) -> Result<(PreparedInput, PreparedVisibilityTraceBundle), String> {
        let PreparedSelection {
            initialization_error: _,
            source_channel_indices,
            source_channel_frequencies_hz,
            source_channel_widths_hz,
            selected_frequency_range_hz,
            reffreq_hz,
            freq_ref,
            cube_spectral_setup,
            cube_row_spectral_cache: _,
            phase_center: prepared_phase_center,
            state,
            trace_state,
        } = self;
        let output_channel_frequencies_hz = cube_spectral_setup
            .as_ref()
            .map(|setup| setup.output_channel_frequencies_hz.clone())
            .unwrap_or_else(|| source_channel_frequencies_hz.clone());
        let make_trace_bundle =
            |samples: Vec<PreparedVisibilitySampleTrace>,
             rejected_samples: Vec<RejectedPreparedVisibilitySampleTrace>| {
                PreparedVisibilityTraceBundle {
                    schema_version: ORACLE_SCHEMA_VERSION,
                    ms_path: ms_path.clone(),
                    data_column: data_column.clone(),
                    spectral_mode: canonical_spectral_mode_name(spectral_mode).to_string(),
                    phase_center: phase_center.clone(),
                    source_channel_indices: source_channel_indices.clone(),
                    source_channel_frequencies_hz: source_channel_frequencies_hz.clone(),
                    source_channel_widths_hz: source_channel_widths_hz.clone(),
                    output_channel_frequencies_hz: output_channel_frequencies_hz.clone(),
                    selected_rows: selected_rows.clone(),
                    samples,
                    rejected_samples,
                }
            };
        match (state, trace_state) {
            (
                PreparedState::ExplicitMfs {
                    plane_stokes,
                    batch,
                    ..
                },
                PreparedTraceState::ExplicitMfs { samples },
            ) => {
                let gridder_mode = infer_mfs_gridder_mode(ms, &prepared_phase_center, &samples)?;
                Ok((
                    PreparedInput::Mfs(PlaneInput {
                        phase_center: prepared_phase_center.clone(),
                        freq_ref,
                        reffreq_hz,
                        selected_frequency_range_hz,
                        plane_stokes,
                        batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                        sample_frequency_batches_hz: chunk_sample_frequencies_hz_from_samples(
                            &samples,
                            DEFAULT_BATCH_SIZE,
                        ),
                        gridder_mode,
                    }),
                    make_trace_bundle(samples, Vec::new()),
                ))
            }
            (
                PreparedState::PairedMfs {
                    plane_stokes,
                    paired,
                    transform,
                    ..
                },
                PreparedTraceState::PairedMfs { samples },
            ) => {
                let collapsed = collapse_paired_visibility_batch(&paired, transform, plane_stokes)
                    .map_err(|error| error.to_string())?;
                let (accepted, rejected) =
                    collapse_pending_pair_traces(samples, transform, plane_stokes);
                let gridder_mode = infer_mfs_gridder_mode(ms, &prepared_phase_center, &accepted)?;
                Ok((
                    PreparedInput::Mfs(PlaneInput {
                        phase_center: prepared_phase_center.clone(),
                        freq_ref,
                        reffreq_hz,
                        selected_frequency_range_hz,
                        plane_stokes,
                        batches: chunk_visibility_batch(collapsed, DEFAULT_BATCH_SIZE),
                        sample_frequency_batches_hz: chunk_sample_frequencies_hz_from_samples(
                            &accepted,
                            DEFAULT_BATCH_SIZE,
                        ),
                        gridder_mode,
                    }),
                    make_trace_bundle(accepted, rejected),
                ))
            }
            (
                PreparedState::ExplicitCube {
                    plane_stokes,
                    channel_batches,
                    channel_model_interpolation_samples,
                    ..
                },
                PreparedTraceState::ExplicitCube { channel_samples },
            ) => {
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |((channel_frequency_hz, batch), model_interpolation_samples)| {
                            CubeChannelRequest {
                                channel_frequency_hz,
                                visibility_batches: chunk_visibility_batch(
                                    batch,
                                    DEFAULT_BATCH_SIZE,
                                ),
                                model_interpolation_batches: chunk_model_interpolation_batches(
                                    model_interpolation_samples,
                                    DEFAULT_BATCH_SIZE,
                                ),
                            }
                        },
                    )
                    .collect();
                Ok((
                    PreparedInput::Cube(CubePlaneInput {
                        phase_center: prepared_phase_center.clone(),
                        freq_ref,
                        plane_stokes,
                        channels,
                    }),
                    make_trace_bundle(channel_samples.into_iter().flatten().collect(), Vec::new()),
                ))
            }
            (
                PreparedState::PairedCube {
                    plane_stokes,
                    transform,
                    channel_batches,
                    channel_model_interpolation_samples,
                    ..
                },
                PreparedTraceState::PairedCube { channel_samples },
            ) => {
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_samples.iter())
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            ((channel_frequency_hz, batch), trace_samples),
                            model_interpolation_samples,
                        )| {
                            let collapsed =
                                collapse_paired_visibility_batch(&batch, transform, plane_stokes)
                                    .map_err(|error| error.to_string())?;
                            let collapsed_model_interpolation_samples =
                                collapse_pending_pair_model_interpolation_samples(
                                    trace_samples,
                                    model_interpolation_samples,
                                    transform,
                                )?;
                            Ok(CubeChannelRequest {
                                channel_frequency_hz,
                                visibility_batches: chunk_visibility_batch(
                                    collapsed,
                                    DEFAULT_BATCH_SIZE,
                                ),
                                model_interpolation_batches: chunk_model_interpolation_batches(
                                    collapsed_model_interpolation_samples,
                                    DEFAULT_BATCH_SIZE,
                                ),
                            })
                        },
                    )
                    .collect::<Result<Vec<_>, String>>()?;
                let (accepted, rejected) = collapse_pending_pair_traces(
                    channel_samples.into_iter().flatten().collect(),
                    transform,
                    plane_stokes,
                );
                Ok((
                    PreparedInput::Cube(CubePlaneInput {
                        phase_center: prepared_phase_center,
                        freq_ref,
                        plane_stokes,
                        channels,
                    }),
                    make_trace_bundle(accepted, rejected),
                ))
            }
            _ => Err(
                "internal error: prepared state and trace state diverged during finalize"
                    .to_string(),
            ),
        }
    }
}

fn write_products(
    config: &CliConfig,
    coords: &CoordinateSystem,
    result: &RunProducts,
) -> Result<(), String> {
    let base = config.imagename.to_string_lossy().to_string();
    let channel_frequencies_hz = result.channel_frequencies_hz();
    let plane_stokes = result.plane_stokes().as_str();
    let reffreq_hz = if channel_frequencies_hz.is_empty() {
        0.0
    } else {
        0.5 * (channel_frequencies_hz[0] + channel_frequencies_hz[channel_frequencies_hz.len() - 1])
    };
    if let RunProducts::Mtmfs(result) = result {
        let psf_beam_set = result
            .beam
            .map(beam_to_gaussian)
            .map(ImageBeamSet::new)
            .unwrap_or_default();
        let image_beam_set = result
            .beam
            .map(beam_to_gaussian)
            .map(ImageBeamSet::new)
            .unwrap_or_default();
        for (term_index, psf_term) in result.psf_terms.iter().enumerate() {
            write_single_product(
                &PathBuf::from(format!("{base}.psf.tt{term_index}")),
                psf_term,
                coords,
                result.compatibility.psf_units.as_str(),
                psf_beam_set.clone(),
                "psf",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        for (term_index, residual_term) in result.residual_terms.iter().enumerate() {
            write_single_product(
                &PathBuf::from(format!("{base}.residual.tt{term_index}")),
                residual_term,
                coords,
                result.compatibility.residual_units.as_str(),
                image_beam_set.clone(),
                "residual",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        for (term_index, model_term) in result.model_terms.iter().enumerate() {
            write_single_product(
                &PathBuf::from(format!("{base}.model.tt{term_index}")),
                model_term,
                coords,
                result.compatibility.model_units.as_str(),
                ImageBeamSet::default(),
                "model",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        for (term_index, image_term) in result.image_terms.iter().enumerate() {
            write_single_product(
                &PathBuf::from(format!("{base}.image.tt{term_index}")),
                image_term,
                coords,
                result.compatibility.image_units.as_str(),
                image_beam_set.clone(),
                "image",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        for (term_index, sumwt_term) in result.sumwt_terms.iter().enumerate() {
            write_single_product(
                &PathBuf::from(format!("{base}.sumwt.tt{term_index}")),
                sumwt_term,
                coords,
                "",
                ImageBeamSet::default(),
                "sumwt",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        if let Some(alpha) = result.alpha.as_ref() {
            write_single_product(
                &PathBuf::from(format!("{base}.alpha")),
                alpha,
                coords,
                "",
                image_beam_set.clone(),
                "alpha",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        if let Some(alpha_error) = result.alpha_error.as_ref() {
            write_single_product(
                &PathBuf::from(format!("{base}.alpha.error")),
                alpha_error,
                coords,
                "",
                image_beam_set,
                "alpha.error",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
        if config.write_preview_pngs {
            if let Some(psf_tt0) = result.psf_terms.first() {
                write_preview_png(&PathBuf::from(format!("{base}.psf.tt0.png")), psf_tt0)?;
            }
            if let Some(residual_tt0) = result.residual_terms.first() {
                write_preview_png(
                    &PathBuf::from(format!("{base}.residual.tt0.png")),
                    residual_tt0,
                )?;
            }
            if let Some(model_tt0) = result.model_terms.first() {
                write_preview_png(&PathBuf::from(format!("{base}.model.tt0.png")), model_tt0)?;
            }
            if let Some(image_tt0) = result.image_terms.first() {
                write_preview_png(&PathBuf::from(format!("{base}.image.tt0.png")), image_tt0)?;
            }
            if let Some(alpha) = result.alpha.as_ref() {
                write_preview_png(&PathBuf::from(format!("{base}.alpha.png")), alpha)?;
            }
        }
        return Ok(());
    }
    let (
        psf,
        residual,
        model,
        image,
        sumwt,
        debug_weight_image,
        psf_beams,
        residual_beams,
        image_beams,
        psf_units,
        residual_units,
        model_units,
        image_units,
    ) = match result {
        RunProducts::Mfs(result) => (
            &result.psf,
            &result.residual,
            &result.model,
            &result.image,
            &result.sumwt,
            result.diagnostics.mosaic_weight_image.as_ref(),
            result
                .beam
                .map(beam_to_gaussian)
                .map(ImageBeamSet::new)
                .unwrap_or_default(),
            result
                .beam
                .map(beam_to_gaussian)
                .map(ImageBeamSet::new)
                .unwrap_or_default(),
            result
                .beam
                .map(beam_to_gaussian)
                .map(ImageBeamSet::new)
                .unwrap_or_default(),
            result.compatibility.psf_units.as_str(),
            result.compatibility.residual_units.as_str(),
            result.compatibility.model_units.as_str(),
            result.compatibility.image_units.as_str(),
        ),
        RunProducts::Cube(result) => (
            &result.psf,
            &result.residual,
            &result.model,
            &result.image,
            &result.sumwt,
            None,
            beam_set_from_channel_beams(&result.beams, RestoringBeamMode::PerPlane)?,
            beam_set_from_channel_beams(&result.beams, RestoringBeamMode::PerPlane)?,
            beam_set_from_channel_beams(&result.restored_beams, RestoringBeamMode::PerPlane)?,
            result.compatibility.psf_units.as_str(),
            result.compatibility.residual_units.as_str(),
            result.compatibility.model_units.as_str(),
            result.compatibility.image_units.as_str(),
        ),
        RunProducts::Mtmfs(_) => unreachable!("MTMFS products are handled by the early return"),
    };
    write_single_product(
        &PathBuf::from(format!("{base}.psf")),
        psf,
        coords,
        psf_units,
        psf_beams,
        "psf",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
    )?;
    write_single_product(
        &PathBuf::from(format!("{base}.residual")),
        residual,
        coords,
        residual_units,
        residual_beams,
        "residual",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
    )?;
    write_single_product(
        &PathBuf::from(format!("{base}.model")),
        model,
        coords,
        model_units,
        ImageBeamSet::default(),
        "model",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
    )?;
    write_single_product(
        &PathBuf::from(format!("{base}.image")),
        image,
        coords,
        image_units,
        image_beams,
        "image",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
    )?;
    write_single_product(
        &PathBuf::from(format!("{base}.sumwt")),
        sumwt,
        coords,
        "",
        ImageBeamSet::default(),
        "sumwt",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
    )?;
    if env::var_os("CASA_RS_WRITE_MOSAIC_DEBUG_PRODUCTS").is_some() {
        if let Some(weight_image) = debug_weight_image {
            let weight_product = expand_plane_for_write(weight_image);
            write_single_product(
                &PathBuf::from(format!("{base}.weight")),
                &weight_product,
                coords,
                "",
                ImageBeamSet::default(),
                "weight",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
            )?;
        }
    }

    if config.write_preview_pngs {
        write_preview_png(&PathBuf::from(format!("{base}.psf.png")), psf)?;
        write_preview_png(&PathBuf::from(format!("{base}.residual.png")), residual)?;
        write_preview_png(&PathBuf::from(format!("{base}.model.png")), model)?;
        write_preview_png(&PathBuf::from(format!("{base}.image.png")), image)?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_single_product(
    path: &Path,
    data: &Array4<f32>,
    coords: &CoordinateSystem,
    units: &str,
    beam_set: ImageBeamSet,
    role: &str,
    plane_stokes: &str,
    channel_frequencies_hz: &[f64],
    reffreq_hz: f64,
) -> Result<(), String> {
    remove_existing_product(path)?;
    let mut image = PagedImage::<f32>::create(data.shape().to_vec(), coords.clone(), path)
        .map_err(|error| format!("create image {}: {error}", path.display()))?;
    image
        .put_slice(&data.clone().into_dyn(), &[0, 0, 0, 0])
        .map_err(|error| format!("write pixels {}: {error}", path.display()))?;
    image
        .set_units(units)
        .map_err(|error| format!("set units {}: {error}", path.display()))?;

    let mut info = ImageInfo {
        beam_set,
        image_type: if role == "psf" {
            ImageType::Beam
        } else {
            ImageType::Intensity
        },
        object_name: role.to_string(),
    };
    if role == "sumwt" {
        info.image_type = ImageType::Undefined;
    }
    image
        .set_image_info(&info)
        .map_err(|error| format!("set imageinfo {}: {error}", path.display()))?;

    let misc = RecordValue::new(vec![
        RecordField::new(
            "casars_imager_role",
            Value::Scalar(ScalarValue::String(role.to_string())),
        ),
        RecordField::new(
            "plane_stokes",
            Value::Scalar(ScalarValue::String(plane_stokes.to_string())),
        ),
        RecordField::new(
            "reffreq_hz",
            Value::Scalar(ScalarValue::Float64(reffreq_hz)),
        ),
        RecordField::new(
            "channel_count",
            Value::Scalar(ScalarValue::Int32(channel_frequencies_hz.len() as i32)),
        ),
    ]);
    image
        .set_misc_info(misc)
        .map_err(|error| format!("set miscinfo {}: {error}", path.display()))?;
    image
        .save()
        .map_err(|error| format!("save image {}: {error}", path.display()))?;
    Ok(())
}

fn remove_existing_product(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path)
            .map_err(|error| format!("remove existing {}: {error}", path.display()))
    } else {
        std::fs::remove_file(path)
            .map_err(|error| format!("remove existing {}: {error}", path.display()))
    }
}

fn expand_plane_for_write(plane: &Array2<f32>) -> Array4<f32> {
    let (nx, ny) = plane.dim();
    let mut expanded = Array4::<f32>::zeros((nx, ny, 1, 1));
    expanded.slice_mut(s![.., .., 0, 0]).assign(plane);
    expanded
}

fn write_preview_png(path: &Path, data: &Array4<f32>) -> Result<(), String> {
    if path.exists() {
        std::fs::remove_file(path)
            .map_err(|error| format!("remove existing preview {}: {error}", path.display()))?;
    }
    let plane = data.slice(s![.., .., 0, 0]);
    let mut amplitudes = plane.iter().map(|value| value.abs()).collect::<Vec<_>>();
    amplitudes.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let limit = if amplitudes.is_empty() {
        1.0
    } else {
        amplitudes[((amplitudes.len() as f64 * 0.995).floor() as usize).min(amplitudes.len() - 1)]
            .max(1.0e-6)
    };

    let mut image =
        ImageBuffer::<Rgb<u8>, Vec<u8>>::new(plane.shape()[0] as u32, plane.shape()[1] as u32);
    for x in 0..plane.shape()[0] {
        for y in 0..plane.shape()[1] {
            let scaled = (plane[(x, y)] / limit).clamp(-1.0, 1.0);
            let pixel = if scaled >= 0.0 {
                let shade = ((1.0 - scaled) * 255.0) as u8;
                Rgb([255, shade, shade])
            } else {
                let shade = ((1.0 + scaled) * 255.0) as u8;
                Rgb([shade, shade, 255])
            };
            image.put_pixel(x as u32, y as u32, pixel);
        }
    }
    image
        .save(path)
        .map_err(|error| format!("write preview {}: {error}", path.display()))
}

fn build_coordinate_system(
    imsize: usize,
    phase_center: [f64; 2],
    cell_arcsec: f64,
    freq_ref: FrequencyRef,
    direction_ref: DirectionRef,
    plane_stokes: PlaneStokes,
    channel_frequencies_hz: &[f64],
) -> CoordinateSystem {
    let cell_rad = cell_arcsec * arcsec_to_rad();
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(Box::new(DirectionCoordinate::new(
        direction_ref,
        Projection::new(ProjectionType::SIN),
        phase_center,
        [-cell_rad, cell_rad],
        [imsize as f64 / 2.0, imsize as f64 / 2.0],
    )));
    coords.add_coordinate(Box::new(StokesCoordinate::new(vec![plane_to_stokes_type(
        plane_stokes,
    )])));
    coords.add_coordinate(Box::new(build_spectral_coordinate(
        freq_ref,
        channel_frequencies_hz,
    )));
    coords
}

fn build_spectral_coordinate(
    freq_ref: FrequencyRef,
    channel_frequencies_hz: &[f64],
) -> SpectralCoordinate {
    let rest_frequency = if channel_frequencies_hz.is_empty() {
        0.0
    } else {
        0.5 * (channel_frequencies_hz[0] + channel_frequencies_hz[channel_frequencies_hz.len() - 1])
    };
    match channel_frequencies_hz {
        [] => SpectralCoordinate::new(freq_ref, 0.0, 1.0, 0.0, rest_frequency),
        [single] => SpectralCoordinate::new(freq_ref, *single, 1.0, 0.0, *single),
        frequencies => {
            let delta = frequencies[1] - frequencies[0];
            let is_linear = frequencies.windows(2).all(|window| {
                let step = window[1] - window[0];
                (step - delta).abs() <= delta.abs().max(1.0) * 1.0e-9
            });
            if is_linear {
                SpectralCoordinate::new(freq_ref, frequencies[0], delta, 0.0, rest_frequency)
            } else {
                SpectralCoordinate::from_tabular(
                    freq_ref,
                    (0..frequencies.len()).map(|index| index as f64).collect(),
                    frequencies.to_vec(),
                    frequencies[0],
                    delta,
                    0.0,
                    rest_frequency,
                )
                .expect("validated channel frequency table")
            }
        }
    }
}

fn beam_set_from_channel_beams(
    beams: &[Option<BeamFit>],
    mode: RestoringBeamMode,
) -> Result<ImageBeamSet, String> {
    let Some(first) = beams.iter().flatten().next().copied() else {
        return Ok(ImageBeamSet::default());
    };
    if mode == RestoringBeamMode::Common {
        let mut beam_set = ImageBeamSet::with_shape(beams.len().max(1), 1, beam_to_gaussian(first));
        for (channel, beam) in beams.iter().enumerate() {
            if let Some(beam) = beam {
                beam_set
                    .set_beam(Some(channel), Some(0), beam_to_gaussian(*beam))
                    .map_err(|error| format!("set beam for channel {channel}: {error}"))?;
            }
        }
        let common = beam_set
            .common_beam()
            .map_err(|error| format!("determine common restoring beam: {error}"))?;
        return Ok(ImageBeamSet::new(common));
    }
    let mut beam_set = ImageBeamSet::with_shape(beams.len(), 1, beam_to_gaussian(first));
    for (channel, beam) in beams.iter().enumerate() {
        if let Some(beam) = beam {
            beam_set
                .set_beam(Some(channel), Some(0), beam_to_gaussian(*beam))
                .map_err(|error| format!("set beam for channel {channel}: {error}"))?;
        }
    }
    if beam_set.single_beam().is_none()
        && beam_set.shape().0 > 0
        && beam_set.shape().1 > 0
        && beam_set.equivalent(&ImageBeamSet::new(*beam_set.beam(0, 0)))
    {
        Ok(ImageBeamSet::new(*beam_set.beam(0, 0)))
    } else {
        Ok(beam_set)
    }
}

fn plane_to_stokes_type(plane: PlaneStokes) -> StokesType {
    match plane {
        PlaneStokes::I => StokesType::I,
        PlaneStokes::Q => StokesType::Q,
        PlaneStokes::U => StokesType::U,
        PlaneStokes::V => StokesType::V,
        PlaneStokes::XX => StokesType::XX,
        PlaneStokes::YY => StokesType::YY,
        PlaneStokes::RR => StokesType::RR,
        PlaneStokes::LL => StokesType::LL,
    }
}

fn plane_to_corr_code(plane: PlaneStokes) -> Option<i32> {
    match plane {
        PlaneStokes::RR => Some(5),
        PlaneStokes::LL => Some(8),
        PlaneStokes::XX => Some(9),
        PlaneStokes::YY => Some(12),
        PlaneStokes::I | PlaneStokes::Q | PlaneStokes::U | PlaneStokes::V => None,
    }
}

fn correlation_index(corr_types: &[i32], corr_code: i32) -> Option<usize> {
    corr_types.iter().position(|code| *code == corr_code)
}

fn derive_stokes_pair_selection(
    plane_stokes: PlaneStokes,
    corr_types: &[i32],
) -> Result<((usize, usize), PairCollapseTransform), String> {
    let xx_yy = correlation_index(corr_types, 9).zip(correlation_index(corr_types, 12));
    let xy_yx = correlation_index(corr_types, 10).zip(correlation_index(corr_types, 11));
    let rr_ll = correlation_index(corr_types, 5).zip(correlation_index(corr_types, 8));
    let rl_lr = correlation_index(corr_types, 6).zip(correlation_index(corr_types, 7));

    match plane_stokes {
        PlaneStokes::I => xx_yy
            .map(|pair| (pair, PairCollapseTransform::HalfSum))
            .or_else(|| rr_ll.map(|pair| (pair, PairCollapseTransform::HalfSum)))
            .ok_or_else(|| {
                "Stokes I imaging requires XX+YY or RR+LL unless an explicit raw correlation plane is selected"
                    .to_string()
            }),
        PlaneStokes::Q => xx_yy
            .map(|pair| (pair, PairCollapseTransform::HalfDifference))
            .or_else(|| rl_lr.map(|pair| (pair, PairCollapseTransform::HalfSum)))
            .ok_or_else(|| {
                "Stokes Q imaging requires XX+YY (linear basis) or RL+LR (circular basis)"
                    .to_string()
            }),
        PlaneStokes::U => xy_yx
            .map(|pair| (pair, PairCollapseTransform::HalfSum))
            .or_else(|| rl_lr.map(|pair| (pair, PairCollapseTransform::PositiveHalfImagDifference)))
            .ok_or_else(|| {
                "Stokes U imaging requires XY+YX (linear basis) or RL+LR (circular basis)"
                    .to_string()
            }),
        PlaneStokes::V => xy_yx
            .map(|pair| (pair, PairCollapseTransform::NegativeHalfImagDifference))
            .or_else(|| rr_ll.map(|pair| (pair, PairCollapseTransform::HalfDifference)))
            .ok_or_else(|| {
                "Stokes V imaging requires XY+YX (linear basis) or RR+LL (circular basis)"
                    .to_string()
            }),
        PlaneStokes::XX | PlaneStokes::YY | PlaneStokes::RR | PlaneStokes::LL => {
            Err(format!("{plane_stokes:?} is a raw correlation plane, not a derived Stokes plane"))
        }
    }
}

fn parse_plane_stokes(text: &str) -> Result<PlaneStokes, String> {
    match text.to_ascii_uppercase().as_str() {
        "I" => Ok(PlaneStokes::I),
        "Q" => Ok(PlaneStokes::Q),
        "U" => Ok(PlaneStokes::U),
        "V" => Ok(PlaneStokes::V),
        "XX" => Ok(PlaneStokes::XX),
        "YY" => Ok(PlaneStokes::YY),
        "RR" => Ok(PlaneStokes::RR),
        "LL" => Ok(PlaneStokes::LL),
        _ => Err(format!("unsupported scalar plane value {text:?}")),
    }
}

fn parse_spectral_mode(text: &str) -> Result<SpectralMode, String> {
    match text.to_ascii_lowercase().as_str() {
        "mfs" => Ok(SpectralMode::Mfs),
        "cube" => Ok(SpectralMode::Cube),
        "cubedata" => Ok(SpectralMode::Cubedata),
        _ => Err(format!(
            "unsupported --specmode value {text:?}; expected mfs, cube, or cubedata"
        )),
    }
}

fn parse_weighting_mode(text: &str, robust: f32) -> Result<WeightingMode, String> {
    match text.to_ascii_lowercase().as_str() {
        "natural" => Ok(WeightingMode::Natural),
        "uniform" => Ok(WeightingMode::Uniform),
        "briggs" | "robust" => Ok(WeightingMode::Briggs { robust }),
        _ => Err(format!(
            "unsupported --weighting value {text:?}; expected natural, uniform, or briggs"
        )),
    }
}

fn parse_deconvolver(text: &str) -> Result<Deconvolver, String> {
    match text.to_ascii_lowercase().as_str() {
        "hogbom" => Ok(Deconvolver::Hogbom),
        "mtmfs" => Ok(Deconvolver::Mtmfs),
        "clark" => Ok(Deconvolver::Clark),
        "multiscale" => Ok(Deconvolver::Multiscale),
        _ => Err(format!(
            "unsupported --deconvolver value {text:?}; expected hogbom, mtmfs, clark, or multiscale"
        )),
    }
}

fn parse_multiscale_scales(text: &str) -> Result<Vec<f32>, String> {
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    text.split(',')
        .map(|part| {
            let trimmed = part.trim();
            let value = trimmed
                .parse::<f32>()
                .map_err(|error| format!("parse --scales entry {trimmed:?}: {error}"))?;
            if !(value.is_finite() && value >= 0.0) {
                return Err(format!(
                    "invalid --scales entry {trimmed:?}; scales must be finite and >= 0"
                ));
            }
            Ok(value)
        })
        .collect()
}

fn parse_w_term_mode(text: &str) -> Result<WTermMode, String> {
    match text.to_ascii_lowercase().as_str() {
        "none" | "2d" => Ok(WTermMode::None),
        "direct" => Ok(WTermMode::Direct),
        "wproject" => Ok(WTermMode::WProject),
        _ => Err(format!(
            "unsupported --wterm value {text:?}; expected none, direct, or wproject"
        )),
    }
}

fn parse_mask_box(text: &str) -> Result<[usize; 4], String> {
    let parts = text
        .split(',')
        .map(str::trim)
        .map(|part| {
            part.parse::<usize>()
                .map_err(|error| format!("parse --mask-box component {part:?}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if parts.len() != 4 {
        return Err(format!("--mask-box expects x0,y0,x1,y1, found {text:?}"));
    }
    Ok([parts[0], parts[1], parts[2], parts[3]])
}

fn build_clean_mask(
    imsize: usize,
    mask_boxes: &[[usize; 4]],
    mask_image: Option<&Path>,
) -> Result<Option<Array2<bool>>, String> {
    if mask_boxes.is_empty() && mask_image.is_none() {
        return Ok(None);
    }
    let mut mask = Array2::<bool>::from_elem((imsize, imsize), false);
    for [x0, y0, x1, y1] in mask_boxes {
        if x0 > x1 || y0 > y1 {
            return Err("--mask-box requires x0 <= x1 and y0 <= y1".to_string());
        }
        if *x1 >= imsize || *y1 >= imsize {
            return Err(format!(
                "--mask-box [{x0},{y0},{x1},{y1}] exceeds image bounds 0..{}",
                imsize.saturating_sub(1)
            ));
        }
        for x in *x0..=*x1 {
            for y in *y0..=*y1 {
                mask[(x, y)] = true;
            }
        }
    }
    if let Some(path) = mask_image {
        merge_mask_image(&mut mask, path)?;
    }
    Ok(Some(mask))
}

fn merge_mask_image(mask: &mut Array2<bool>, path: &Path) -> Result<(), String> {
    let image = PagedImage::<f32>::open(path)
        .map_err(|error| format!("open --mask-image {}: {error}", path.display()))?;
    let shape = image.shape().to_vec();
    let (nx, ny) = mask.dim();
    match shape.as_slice() {
        [sx, sy] if *sx == nx && *sy == ny => {
            let plane = image
                .get_slice(&[0, 0], &[nx, ny])
                .map_err(|error| format!("read --mask-image {}: {error}", path.display()))?;
            for x in 0..nx {
                for y in 0..ny {
                    if plane[[x, y]] != 0.0 {
                        mask[(x, y)] = true;
                    }
                }
            }
            Ok(())
        }
        [sx, sy, 1, 1] if *sx == nx && *sy == ny => {
            let plane = image
                .get_slice(&[0, 0, 0, 0], &[nx, ny, 1, 1])
                .map_err(|error| format!("read --mask-image {}: {error}", path.display()))?;
            for x in 0..nx {
                for y in 0..ny {
                    if plane[[x, y, 0, 0]] != 0.0 {
                        mask[(x, y)] = true;
                    }
                }
            }
            Ok(())
        }
        _ => Err(format!(
            "--mask-image {} has shape {:?}, expected [{nx}, {ny}] or [{nx}, {ny}, 1, 1]",
            path.display(),
            shape
        )),
    }
}

fn beam_to_gaussian(beam: BeamFit) -> GaussianBeam {
    GaussianBeam::new(
        beam.major_fwhm_rad,
        beam.minor_fwhm_rad,
        beam.position_angle_rad,
    )
}

fn empty_visibility_batch(capacity: usize) -> VisibilityBatch {
    VisibilityBatch {
        u_lambda: Vec::with_capacity(capacity),
        v_lambda: Vec::with_capacity(capacity),
        w_lambda: Vec::with_capacity(capacity),
        weight: Vec::with_capacity(capacity),
        sumwt_factor: Vec::with_capacity(capacity),
        gridable: Vec::with_capacity(capacity),
        visibility: Vec::with_capacity(capacity),
    }
}

fn empty_parallel_hand_batch(capacity: usize) -> ParallelHandBatch {
    ParallelHandBatch {
        u_lambda: Vec::with_capacity(capacity),
        v_lambda: Vec::with_capacity(capacity),
        w_lambda: Vec::with_capacity(capacity),
        first_visibility: Vec::with_capacity(capacity),
        second_visibility: Vec::with_capacity(capacity),
        first_weight: Vec::with_capacity(capacity),
        second_weight: Vec::with_capacity(capacity),
        first_flagged: Vec::with_capacity(capacity),
        second_flagged: Vec::with_capacity(capacity),
        gridable: Vec::with_capacity(capacity),
    }
}

fn chunk_visibility_batch(batch: VisibilityBatch, max_batch_size: usize) -> Vec<VisibilityBatch> {
    if batch.len() <= max_batch_size {
        return vec![batch];
    }
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < batch.len() {
        let end = (start + max_batch_size).min(batch.len());
        batches.push(VisibilityBatch {
            u_lambda: batch.u_lambda[start..end].to_vec(),
            v_lambda: batch.v_lambda[start..end].to_vec(),
            w_lambda: batch.w_lambda[start..end].to_vec(),
            weight: batch.weight[start..end].to_vec(),
            sumwt_factor: batch.sumwt_factor[start..end].to_vec(),
            gridable: batch.gridable[start..end].to_vec(),
            visibility: batch.visibility[start..end].to_vec(),
        });
        start = end;
    }
    batches
}

fn chunk_sample_frequencies_hz_from_samples(
    samples: &[PreparedVisibilitySampleTrace],
    max_batch_size: usize,
) -> Vec<Vec<f64>> {
    if samples.len() <= max_batch_size {
        return vec![
            samples
                .iter()
                .map(|sample| sample.output_frequency_hz)
                .collect(),
        ];
    }
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < samples.len() {
        let end = (start + max_batch_size).min(samples.len());
        batches.push(
            samples[start..end]
                .iter()
                .map(|sample| sample.output_frequency_hz)
                .collect(),
        );
        start = end;
    }
    batches
}

fn chunk_visibility_metadata_batches(
    samples: &[PreparedVisibilitySampleTrace],
    max_batch_size: usize,
) -> Vec<VisibilityMetadataBatch> {
    let beam_frequencies_hz = infer_mosaic_beam_frequencies_hz(samples);
    if samples.len() <= max_batch_size {
        return vec![VisibilityMetadataBatch {
            sample_frequency_hz: samples
                .iter()
                .map(|sample| sample.output_frequency_hz)
                .collect(),
            beam_frequency_hz: beam_frequencies_hz,
            pointing_direction_rad: samples
                .iter()
                .map(|sample| sample.pointing_direction_rad)
                .collect(),
        }];
    }
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < samples.len() {
        let end = (start + max_batch_size).min(samples.len());
        let slice = &samples[start..end];
        batches.push(VisibilityMetadataBatch {
            sample_frequency_hz: slice
                .iter()
                .map(|sample| sample.output_frequency_hz)
                .collect(),
            beam_frequency_hz: beam_frequencies_hz[start..end].to_vec(),
            pointing_direction_rad: slice
                .iter()
                .map(|sample| sample.pointing_direction_rad)
                .collect(),
        });
        start = end;
    }
    batches
}

fn infer_mosaic_beam_frequencies_hz(samples: &[PreparedVisibilitySampleTrace]) -> Vec<f64> {
    if samples.is_empty() {
        return Vec::new();
    }
    let mut unique_frequencies_hz = samples
        .iter()
        .map(|sample| sample.output_frequency_hz)
        .collect::<Vec<_>>();
    unique_frequencies_hz.sort_by(|left, right| {
        left.partial_cmp(right)
            .expect("prepared sample frequencies should be finite")
    });
    unique_frequencies_hz.dedup_by(|left, right| left.to_bits() == right.to_bits());
    if unique_frequencies_hz.len() <= 1 {
        return vec![unique_frequencies_hz[0]; samples.len()];
    }

    // Mirror CASA SimplePBConvFunc::findUsefulChannels(): nearby selected
    // channels can share one PB / convolution-function frequency bucket.
    let min_frequency_hz = unique_frequencies_hz[0];
    let max_frequency_hz = *unique_frequencies_hz.last().expect("non-empty");
    let orig_width_hz =
        (max_frequency_hz - min_frequency_hz) / (unique_frequencies_hz.len() - 1) as f64;
    let mut tolerance_hz = max_frequency_hz * 0.5 / 100.0;
    if tolerance_hz < orig_width_hz / 2.0 {
        tolerance_hz = orig_width_hz / 2.0;
    }

    let top_frequency_hz = max_frequency_hz;
    let mut bottom_frequency_hz = top_frequency_hz;
    let mut beam_channel_count = 0usize;
    while bottom_frequency_hz > min_frequency_hz {
        beam_channel_count += 1;
        bottom_frequency_hz -= tolerance_hz;
    }
    if beam_channel_count > 1 {
        beam_channel_count -= 1;
        bottom_frequency_hz += tolerance_hz;
    }
    if beam_channel_count >= unique_frequencies_hz.len().saturating_sub(1) {
        return samples
            .iter()
            .map(|sample| sample.output_frequency_hz)
            .collect();
    }
    if beam_channel_count == 0 {
        return vec![bottom_frequency_hz; samples.len()];
    }

    let beam_frequencies_hz = (0..beam_channel_count)
        .map(|index| bottom_frequency_hz + index as f64 * tolerance_hz)
        .collect::<Vec<_>>();
    samples
        .iter()
        .map(|sample| {
            let sample_frequency_hz = sample.output_frequency_hz;
            let mut best_frequency_hz = beam_frequencies_hz[0];
            let mut best_delta_hz = f64::INFINITY;
            for beam_frequency_hz in &beam_frequencies_hz {
                let delta_hz = (sample_frequency_hz - *beam_frequency_hz).abs();
                if delta_hz < best_delta_hz {
                    best_delta_hz = delta_hz;
                    best_frequency_hz = *beam_frequency_hz;
                }
            }
            best_frequency_hz
        })
        .collect()
}

fn direction_separation_rad(left: [f64; 2], right: [f64; 2]) -> f64 {
    let delta_ra = {
        let raw = left[0] - right[0];
        (raw + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU) - std::f64::consts::PI
    };
    let sin_dec = left[1].sin() * right[1].sin() + left[1].cos() * right[1].cos() * delta_ra.cos();
    sin_dec.clamp(-1.0, 1.0).acos()
}

fn combine_pointing_direction_rad(left: [f64; 2], right: [f64; 2]) -> [f64; 2] {
    let left_vec = [
        left[1].cos() * left[0].cos(),
        left[1].cos() * left[0].sin(),
        left[1].sin(),
    ];
    let right_vec = [
        right[1].cos() * right[0].cos(),
        right[1].cos() * right[0].sin(),
        right[1].sin(),
    ];
    let summed = [
        left_vec[0] + right_vec[0],
        left_vec[1] + right_vec[1],
        left_vec[2] + right_vec[2],
    ];
    let norm = (summed[0] * summed[0] + summed[1] * summed[1] + summed[2] * summed[2]).sqrt();
    if !(norm.is_finite() && norm > 0.0) {
        return left;
    }
    let x = summed[0] / norm;
    let y = summed[1] / norm;
    let z = summed[2] / norm;
    [
        y.atan2(x).rem_euclid(std::f64::consts::TAU),
        z.atan2((x * x + y * y).sqrt()),
    ]
}

fn infer_primary_beam_model(ms: &MeasurementSet) -> Result<PrimaryBeamModel, String> {
    let observation = ms
        .observation()
        .map_err(|error| format!("open OBSERVATION: {error}"))?;
    let telescope_name = observation
        .string(0, "TELESCOPE_NAME")
        .map_err(|error| format!("read OBSERVATION.TELESCOPE_NAME: {error}"))?
        .to_ascii_uppercase();
    let antenna = ms
        .antenna()
        .map_err(|error| format!("open ANTENNA: {error}"))?;
    let mut diameters = Vec::new();
    for row in 0..antenna.row_count() {
        let diameter_m = antenna
            .dish_diameter(row)
            .map_err(|error| format!("read ANTENNA.DISH_DIAMETER row {row}: {error}"))?;
        if diameter_m.is_finite() && diameter_m > 0.0 {
            diameters.push(diameter_m);
        }
    }
    let dish_diameter_m = diameters
        .into_iter()
        .reduce(f64::max)
        .ok_or_else(|| "no positive ANTENNA.DISH_DIAMETER entries were found".to_string())?;
    // Mirror CASA's common-PB defaults for the telescopes covered by the
    // current Wave 14 gate set instead of assuming the physical dish size is
    // always the effective Airy aperture diameter.
    let model = if telescope_name.contains("ALMA") {
        PrimaryBeamModel::Airy {
            dish_diameter_m: 10.7,
            blockage_diameter_m: 0.75,
        }
    } else if telescope_name.contains("ACA") {
        PrimaryBeamModel::Airy {
            dish_diameter_m: 6.25,
            blockage_diameter_m: 0.75,
        }
    } else if telescope_name.contains("EVLA") {
        PrimaryBeamModel::EvlaLBandCommon
    } else {
        PrimaryBeamModel::Airy {
            dish_diameter_m,
            blockage_diameter_m: dish_diameter_m / 25.0 * 2.0,
        }
    };
    Ok(model)
}

fn infer_mfs_gridder_mode(
    ms: &MeasurementSet,
    phase_center: &PhaseCenter,
    samples: &[PreparedVisibilitySampleTrace],
) -> Result<GridderMode, String> {
    let phase_center_direction_rad = phase_center.angles_rad;
    let needs_mosaic = samples.iter().any(|sample| {
        direction_separation_rad(sample.pointing_direction_rad, phase_center_direction_rad) > 1.0e-8
    });
    if !needs_mosaic {
        return Ok(GridderMode::Standard);
    }
    Ok(GridderMode::Mosaic(MosaicGridderConfig {
        phase_center_direction_rad,
        primary_beam_model: infer_primary_beam_model(ms)?,
        pb_limit: 0.1,
        metadata_batches: chunk_visibility_metadata_batches(samples, DEFAULT_BATCH_SIZE),
    }))
}

fn chunk_model_interpolation_batches(
    sample_contributions: Vec<Vec<CubeModelChannelContribution>>,
    max_batch_size: usize,
) -> Vec<CubeModelInterpolationBatch> {
    if sample_contributions.len() <= max_batch_size {
        return vec![CubeModelInterpolationBatch {
            sample_contributions,
        }];
    }
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < sample_contributions.len() {
        let end = (start + max_batch_size).min(sample_contributions.len());
        batches.push(CubeModelInterpolationBatch {
            sample_contributions: sample_contributions[start..end].to_vec(),
        });
        start = end;
    }
    batches
}

fn complex32_at_2d(data: &ArrayValue, corr: usize, chan: usize) -> Result<Complex32, String> {
    match data {
        ArrayValue::Complex32(values) => values
            .get(IxDyn(&[corr, chan]))
            .copied()
            .ok_or_else(|| format!("complex32 visibility index [{corr}, {chan}] out of bounds")),
        ArrayValue::Complex64(values) => values
            .get(IxDyn(&[corr, chan]))
            .map(|value| Complex32::new(value.re as f32, value.im as f32))
            .ok_or_else(|| format!("complex64 visibility index [{corr}, {chan}] out of bounds")),
        other => Err(format!(
            "visibility data must be Complex32/Complex64, found {:?}",
            other.primitive_type()
        )),
    }
}

fn bool_at_2d(data: &ArrayValue, corr: usize, chan: usize) -> Result<bool, String> {
    match data {
        ArrayValue::Bool(values) => values
            .get(IxDyn(&[corr, chan]))
            .copied()
            .ok_or_else(|| format!("flag index [{corr}, {chan}] out of bounds")),
        other => Err(format!(
            "FLAG must be Bool, found {:?}",
            other.primitive_type()
        )),
    }
}

fn resolve_weight_with_source(
    weight_row: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    corr: usize,
    chan: usize,
) -> Result<(f32, WeightSourceKind), String> {
    if let Some(spectrum) = weight_spectrum_row {
        match spectrum {
            ArrayValue::Float32(values) => {
                if let Some(weight) = values.get(IxDyn(&[corr, chan])) {
                    return Ok((*weight, WeightSourceKind::WeightSpectrum));
                }
            }
            ArrayValue::Float64(values) => {
                if let Some(weight) = values.get(IxDyn(&[corr, chan])) {
                    return Ok((*weight as f32, WeightSourceKind::WeightSpectrum));
                }
            }
            _ => {}
        }
    }
    match weight_row {
        ArrayValue::Float32(values) => values
            .get(IxDyn(&[corr]))
            .copied()
            .map(|weight| (weight, WeightSourceKind::Weight))
            .ok_or_else(|| format!("WEIGHT index [{corr}] out of bounds")),
        ArrayValue::Float64(values) => values
            .get(IxDyn(&[corr]))
            .map(|value| (*value as f32, WeightSourceKind::Weight))
            .ok_or_else(|| format!("WEIGHT index [{corr}] out of bounds")),
        other => Err(format!(
            "WEIGHT must be Float32/Float64, found {:?}",
            other.primitive_type()
        )),
    }
}

fn weight_source_union(first: WeightSourceKind, second: WeightSourceKind) -> WeightSourceKind {
    if first == second {
        first
    } else {
        WeightSourceKind::Mixed
    }
}

fn combine_model_channel_contributions(
    output_sample_contributions: &[CubeChannelContribution],
    source_model_contributions: &[Vec<CubeChannelContribution>],
) -> Vec<CubeModelChannelContribution> {
    let mut by_model_channel = BTreeMap::<usize, f32>::new();
    for contribution in output_sample_contributions {
        if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
            continue;
        }
        let Some(model_contributions) = source_model_contributions.get(contribution.source_channel)
        else {
            continue;
        };
        for model_contribution in model_contributions {
            if !(model_contribution.factor.is_finite() && model_contribution.factor > 0.0) {
                continue;
            }
            *by_model_channel
                .entry(model_contribution.source_channel)
                .or_default() += contribution.factor * model_contribution.factor;
        }
    }
    by_model_channel
        .into_iter()
        .filter_map(|(model_channel_index, factor)| {
            (factor.is_finite() && factor > 0.0).then_some(CubeModelChannelContribution {
                model_channel_index,
                factor,
            })
        })
        .collect()
}

fn build_source_contribution_traces(
    source_channel_indices: &[usize],
    source_channel_frequencies_hz: &[f64],
    contributions: &[CubeChannelContribution],
) -> Vec<ChannelContributionTrace> {
    contributions
        .iter()
        .map(|contribution| ChannelContributionTrace {
            source_channel_slot: contribution.source_channel,
            source_channel_index: source_channel_indices[contribution.source_channel],
            source_frequency_hz: source_channel_frequencies_hz[contribution.source_channel],
            factor: contribution.factor,
        })
        .collect()
}

fn collapse_pending_pair_traces(
    samples: Vec<PendingPairedSampleTrace>,
    transform: PairCollapseTransform,
    plane_stokes: PlaneStokes,
) -> (
    Vec<PreparedVisibilitySampleTrace>,
    Vec<RejectedPreparedVisibilitySampleTrace>,
) {
    let mut accepted = Vec::with_capacity(samples.len());
    let mut rejected = Vec::new();
    for sample in samples {
        match collapse_pending_pair_trace(
            sample,
            transform,
            reported_sumwt_factor_for_paired_plane(plane_stokes),
        ) {
            CollapsedPairTrace::Accepted(sample) => accepted.push(sample),
            CollapsedPairTrace::Rejected(sample) => rejected.push(sample),
        }
    }
    (accepted, rejected)
}

fn collapse_pending_pair_model_interpolation_samples(
    samples: &[PendingPairedSampleTrace],
    model_interpolation_samples: Vec<Vec<CubeModelChannelContribution>>,
    transform: PairCollapseTransform,
) -> Result<Vec<Vec<CubeModelChannelContribution>>, String> {
    if samples.len() != model_interpolation_samples.len() {
        return Err(format!(
            "paired cube model interpolation sample count {} does not match paired trace count {}",
            model_interpolation_samples.len(),
            samples.len()
        ));
    }
    Ok(samples
        .iter()
        .cloned()
        .zip(model_interpolation_samples)
        .filter_map(|(sample, model_contributions)| {
            matches!(
                collapse_pending_pair_trace(sample, transform, 1.0),
                CollapsedPairTrace::Accepted(_)
            )
            .then_some(model_contributions)
        })
        .collect())
}

fn collapse_pending_pair_trace(
    sample: PendingPairedSampleTrace,
    transform: PairCollapseTransform,
    reported_sumwt_factor: f32,
) -> CollapsedPairTrace {
    if sample.first_flagged || sample.second_flagged {
        return CollapsedPairTrace::Rejected(rejected_pending_pair_trace(
            &sample,
            PreparedSampleRejectionReason::FlaggedCorrelation,
        ));
    }
    if !(sample.first_weight.is_finite()
        && sample.first_weight > 0.0
        && sample.second_weight.is_finite()
        && sample.second_weight > 0.0)
    {
        return CollapsedPairTrace::Rejected(rejected_pending_pair_trace(
            &sample,
            PreparedSampleRejectionReason::NonPositiveWeight,
        ));
    }
    let visibility =
        collapse_paired_visibility(sample.first_visibility, sample.second_visibility, transform);
    if !(visibility.re.is_finite() && visibility.im.is_finite()) {
        return CollapsedPairTrace::Rejected(rejected_pending_pair_trace(
            &sample,
            PreparedSampleRejectionReason::NonFiniteVisibility,
        ));
    }
    let combined_weight = 0.5 * (sample.first_weight + sample.second_weight);
    if !(combined_weight.is_finite() && combined_weight > 0.0) {
        return CollapsedPairTrace::Rejected(rejected_pending_pair_trace(
            &sample,
            PreparedSampleRejectionReason::NonPositiveWeight,
        ));
    }
    CollapsedPairTrace::Accepted(PreparedVisibilitySampleTrace {
        row_index: sample.common.row_index,
        input_field_id: sample.common.input_field_id,
        phase_center_field_id: sample.common.phase_center_field_id,
        ddid: sample.common.ddid,
        spw_id: sample.common.spw_id,
        polarization_id: sample.common.polarization_id,
        antenna1_id: sample.common.antenna1_id,
        antenna2_id: sample.common.antenna2_id,
        is_cross: sample.common.is_cross,
        raw_uvw_m: sample.common.raw_uvw_m,
        imaging_uvw_m: sample.common.imaging_uvw_m,
        phase_shift_m: sample.common.phase_shift_m,
        correlation_indices: sample.correlation_indices.to_vec(),
        output_channel_index: sample.common.output_channel_index,
        output_frequency_hz: sample.common.output_frequency_hz,
        field_phase_center_direction_rad: sample.common.field_phase_center_direction_rad,
        pointing_direction_rad: sample.common.pointing_direction_rad,
        visibility_re: visibility.re,
        visibility_im: visibility.im,
        weight: combined_weight,
        weight_source: weight_source_union(sample.first_weight_source, sample.second_weight_source),
        sumwt_factor: reported_sumwt_factor,
        gridable: sample.common.gridable,
        source_contributions: sample.common.source_contributions,
    })
}

fn collapse_paired_visibility(
    first_visibility: Complex32,
    second_visibility: Complex32,
    transform: PairCollapseTransform,
) -> Complex32 {
    match transform {
        PairCollapseTransform::HalfSum => (first_visibility + second_visibility) * 0.5,
        PairCollapseTransform::HalfDifference => (first_visibility - second_visibility) * 0.5,
        PairCollapseTransform::PositiveHalfImagDifference => {
            (first_visibility - second_visibility) * Complex32::new(0.0, 0.5)
        }
        PairCollapseTransform::NegativeHalfImagDifference => {
            (first_visibility - second_visibility) * Complex32::new(0.0, -0.5)
        }
    }
}

fn collapse_paired_visibility_batch(
    paired: &ParallelHandBatch,
    transform: PairCollapseTransform,
    plane_stokes: PlaneStokes,
) -> Result<VisibilityBatch, String> {
    let expected = paired.first_visibility.len();
    for (label, len) in [
        ("u_lambda", paired.u_lambda.len()),
        ("v_lambda", paired.v_lambda.len()),
        ("w_lambda", paired.w_lambda.len()),
        ("second_visibility", paired.second_visibility.len()),
        ("first_weight", paired.first_weight.len()),
        ("second_weight", paired.second_weight.len()),
        ("first_flagged", paired.first_flagged.len()),
        ("second_flagged", paired.second_flagged.len()),
        ("gridable", paired.gridable.len()),
    ] {
        if len != expected {
            return Err(format!(
                "paired batch length mismatch: first_visibility={expected}, {label}={len}"
            ));
        }
    }

    let mut u_lambda = Vec::with_capacity(paired.len());
    let mut v_lambda = Vec::with_capacity(paired.len());
    let mut w_lambda = Vec::with_capacity(paired.len());
    let mut weight = Vec::with_capacity(paired.len());
    let mut sumwt_factor = Vec::with_capacity(paired.len());
    let mut gridable = Vec::with_capacity(paired.len());
    let mut visibility = Vec::with_capacity(paired.len());

    for index in 0..paired.len() {
        if paired.first_flagged[index] || paired.second_flagged[index] {
            continue;
        }
        let first_weight = paired.first_weight[index];
        let second_weight = paired.second_weight[index];
        if !(first_weight.is_finite()
            && first_weight > 0.0
            && second_weight.is_finite()
            && second_weight > 0.0)
        {
            continue;
        }
        let vis = collapse_paired_visibility(
            paired.first_visibility[index],
            paired.second_visibility[index],
            transform,
        );
        if !(vis.re.is_finite() && vis.im.is_finite()) {
            continue;
        }
        let combined_weight = 0.5 * (first_weight + second_weight);
        if !(combined_weight.is_finite() && combined_weight > 0.0) {
            continue;
        }

        u_lambda.push(paired.u_lambda[index]);
        v_lambda.push(paired.v_lambda[index]);
        w_lambda.push(paired.w_lambda[index]);
        weight.push(combined_weight);
        sumwt_factor.push(reported_sumwt_factor_for_paired_plane(plane_stokes));
        gridable.push(paired.gridable[index]);
        visibility.push(vis);
    }

    let collapsed = VisibilityBatch {
        u_lambda,
        v_lambda,
        w_lambda,
        weight,
        sumwt_factor,
        gridable,
        visibility,
    };
    Ok(collapsed)
}

fn reported_sumwt_factor_for_paired_plane(plane_stokes: PlaneStokes) -> f32 {
    match plane_stokes {
        PlaneStokes::I => 2.0,
        PlaneStokes::Q | PlaneStokes::U | PlaneStokes::V => 1.0,
        PlaneStokes::XX | PlaneStokes::YY | PlaneStokes::RR | PlaneStokes::LL => 1.0,
    }
}

fn rejected_pending_pair_trace(
    sample: &PendingPairedSampleTrace,
    rejection_reason: PreparedSampleRejectionReason,
) -> RejectedPreparedVisibilitySampleTrace {
    RejectedPreparedVisibilitySampleTrace {
        row_index: sample.common.row_index,
        input_field_id: sample.common.input_field_id,
        phase_center_field_id: sample.common.phase_center_field_id,
        ddid: sample.common.ddid,
        spw_id: sample.common.spw_id,
        polarization_id: sample.common.polarization_id,
        antenna1_id: sample.common.antenna1_id,
        antenna2_id: sample.common.antenna2_id,
        is_cross: sample.common.is_cross,
        raw_uvw_m: sample.common.raw_uvw_m,
        imaging_uvw_m: sample.common.imaging_uvw_m,
        phase_shift_m: sample.common.phase_shift_m,
        correlation_indices: sample.correlation_indices.to_vec(),
        output_channel_index: sample.common.output_channel_index,
        output_frequency_hz: sample.common.output_frequency_hz,
        field_phase_center_direction_rad: sample.common.field_phase_center_direction_rad,
        pointing_direction_rad: sample.common.pointing_direction_rad,
        first_weight: sample.first_weight,
        second_weight: sample.second_weight,
        first_weight_source: sample.first_weight_source,
        second_weight_source: sample.second_weight_source,
        first_flagged: sample.first_flagged,
        second_flagged: sample.second_flagged,
        source_contributions: sample.common.source_contributions.clone(),
        rejection_reason,
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ExplicitCubeOutputSample {
    visibility: Complex32,
    weight: f32,
    weight_source: WeightSourceKind,
    sumwt_factor: f32,
}

#[allow(clippy::too_many_arguments)]
fn interpolate_explicit_cube_output_sample(
    data: &ArrayValue,
    flags: &ArrayValue,
    row_weights: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    corr_index: usize,
    source_channel_indices: &[usize],
    source_channel_frequencies_hz: &[f64],
    phase_shift_m: f64,
    contributions: &[CubeChannelContribution],
) -> Result<Option<ExplicitCubeOutputSample>, String> {
    let mut visibility = Complex32::new(0.0, 0.0);
    let mut weight = 0.0f32;
    let mut weight_source = None::<WeightSourceKind>;
    let mut sumwt_factor = 0.0f32;

    for contribution in contributions {
        if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
            return Ok(None);
        }
        let channel_index = source_channel_indices[contribution.source_channel];
        let source_frequency_hz = source_channel_frequencies_hz[contribution.source_channel];
        if bool_at_2d(flags, corr_index, channel_index)? {
            return Ok(None);
        }
        let source_visibility = phase_rotate_visibility(
            complex32_at_2d(data, corr_index, channel_index)?,
            phase_shift_m,
            source_frequency_hz,
        );
        let (source_weight, source_weight_source) = resolve_weight_with_source(
            row_weights,
            weight_spectrum_row,
            corr_index,
            channel_index,
        )?;
        if !(source_visibility.re.is_finite()
            && source_visibility.im.is_finite()
            && source_weight.is_finite())
        {
            return Ok(None);
        }
        visibility += source_visibility * contribution.factor;
        weight += source_weight * contribution.factor;
        weight_source = Some(match weight_source {
            None => source_weight_source,
            Some(existing) => weight_source_union(existing, source_weight_source),
        });
        sumwt_factor += contribution.factor;
    }

    if !(visibility.re.is_finite()
        && visibility.im.is_finite()
        && weight.is_finite()
        && weight > 0.0
        && sumwt_factor.is_finite()
        && sumwt_factor > 0.0)
    {
        return Ok(None);
    }

    Ok(Some(ExplicitCubeOutputSample {
        visibility,
        weight,
        weight_source: weight_source.unwrap_or(WeightSourceKind::Weight),
        sumwt_factor,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PairedCubeOutputSample {
    first_visibility: Complex32,
    second_visibility: Complex32,
    first_weight: f32,
    second_weight: f32,
    first_weight_source: WeightSourceKind,
    second_weight_source: WeightSourceKind,
}

#[allow(clippy::too_many_arguments)]
fn interpolate_paired_cube_output_sample(
    data: &ArrayValue,
    flags: &ArrayValue,
    row_weights: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    pair: (usize, usize),
    source_channel_indices: &[usize],
    source_channel_frequencies_hz: &[f64],
    phase_shift_m: f64,
    contributions: &[CubeChannelContribution],
) -> Result<Option<PairedCubeOutputSample>, String> {
    let mut first_visibility = Complex32::new(0.0, 0.0);
    let mut second_visibility = Complex32::new(0.0, 0.0);
    let mut first_weight = 0.0f32;
    let mut second_weight = 0.0f32;
    let mut first_weight_source = None::<WeightSourceKind>;
    let mut second_weight_source = None::<WeightSourceKind>;

    for contribution in contributions {
        if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
            return Ok(None);
        }
        let channel_index = source_channel_indices[contribution.source_channel];
        let source_frequency_hz = source_channel_frequencies_hz[contribution.source_channel];
        if bool_at_2d(flags, pair.0, channel_index)? || bool_at_2d(flags, pair.1, channel_index)? {
            return Ok(None);
        }
        let source_first_visibility = phase_rotate_visibility(
            complex32_at_2d(data, pair.0, channel_index)?,
            phase_shift_m,
            source_frequency_hz,
        );
        let source_second_visibility = phase_rotate_visibility(
            complex32_at_2d(data, pair.1, channel_index)?,
            phase_shift_m,
            source_frequency_hz,
        );
        let (source_first_weight, source_first_weight_source) =
            resolve_weight_with_source(row_weights, weight_spectrum_row, pair.0, channel_index)?;
        let (source_second_weight, source_second_weight_source) =
            resolve_weight_with_source(row_weights, weight_spectrum_row, pair.1, channel_index)?;
        if !(source_first_visibility.re.is_finite()
            && source_first_visibility.im.is_finite()
            && source_second_visibility.re.is_finite()
            && source_second_visibility.im.is_finite()
            && source_first_weight.is_finite()
            && source_second_weight.is_finite())
        {
            return Ok(None);
        }
        first_visibility += source_first_visibility * contribution.factor;
        second_visibility += source_second_visibility * contribution.factor;
        first_weight += source_first_weight * contribution.factor;
        second_weight += source_second_weight * contribution.factor;
        first_weight_source = Some(match first_weight_source {
            None => source_first_weight_source,
            Some(existing) => weight_source_union(existing, source_first_weight_source),
        });
        second_weight_source = Some(match second_weight_source {
            None => source_second_weight_source,
            Some(existing) => weight_source_union(existing, source_second_weight_source),
        });
    }

    if !(first_visibility.re.is_finite()
        && first_visibility.im.is_finite()
        && second_visibility.re.is_finite()
        && second_visibility.im.is_finite()
        && first_weight.is_finite()
        && first_weight > 0.0
        && second_weight.is_finite()
        && second_weight > 0.0)
    {
        return Ok(None);
    }

    Ok(Some(PairedCubeOutputSample {
        first_visibility,
        second_visibility,
        first_weight,
        second_weight,
        first_weight_source: first_weight_source.unwrap_or(WeightSourceKind::Weight),
        second_weight_source: second_weight_source.unwrap_or(WeightSourceKind::Weight),
    }))
}

fn combine_single(
    current: Option<i32>,
    candidate: i32,
    label: &str,
) -> Result<Option<i32>, String> {
    match current {
        None => Ok(Some(candidate)),
        Some(existing) if existing == candidate => Ok(Some(existing)),
        Some(existing) => Err(format!(
            "selection spans multiple {label} values ({existing} and {candidate}); narrow it with --field/--ddid/--spw"
        )),
    }
}

fn next_value(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
    args.next()
        .map(|value| value.to_string_lossy().to_string())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn next_path(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<PathBuf, String> {
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| format!("{flag} requires a path"))
}

fn arcsec_to_rad() -> f64 {
    std::f64::consts::PI / (180.0 * 3600.0)
}

fn degrees_to_rad() -> f64 {
    std::f64::consts::PI / 180.0
}

fn parse_uv_taper_size(text: &str) -> Result<casa_imaging::UvTaperSize, String> {
    let lower = text.trim().to_ascii_lowercase();
    if let Some(value) = lower.strip_suffix("arcsec") {
        let parsed = value
            .trim()
            .parse::<f64>()
            .map_err(|error| format!("parse uvtaper arcsec {text:?}: {error}"))?;
        return Ok(casa_imaging::UvTaperSize::ImageFwhmRad(
            parsed * arcsec_to_rad(),
        ));
    }
    if let Some(value) = lower.strip_suffix("lambda") {
        let parsed = value
            .trim()
            .parse::<f64>()
            .map_err(|error| format!("parse uvtaper lambda {text:?}: {error}"))?;
        return Ok(casa_imaging::UvTaperSize::BaselineHwhmLambda(parsed));
    }
    Err(format!(
        "unsupported --uvtaper size {text:?}; expected units arcsec or lambda"
    ))
}

fn parse_uv_taper(text: &str) -> Result<GaussianUvTaper, String> {
    let parts = text
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [major] => {
            let size = parse_uv_taper_size(major)?;
            Ok(GaussianUvTaper {
                major: size,
                minor: size,
                position_angle_rad: 0.0,
            })
        }
        [major, minor] => Ok(GaussianUvTaper {
            major: parse_uv_taper_size(major)?,
            minor: parse_uv_taper_size(minor)?,
            position_angle_rad: 0.0,
        }),
        [major, minor, pa] => {
            let pa = pa
                .strip_suffix("deg")
                .ok_or_else(|| {
                    format!("unsupported --uvtaper position angle {pa:?}; expected deg units")
                })?
                .trim()
                .parse::<f64>()
                .map_err(|error| format!("parse --uvtaper position angle {pa:?}: {error}"))?;
            Ok(GaussianUvTaper {
                major: parse_uv_taper_size(major)?,
                minor: parse_uv_taper_size(minor)?,
                position_angle_rad: pa * degrees_to_rad(),
            })
        }
        _ => Err(format!(
            "unsupported --uvtaper value {text:?}; expected MAJOR[,MINOR[,PA]]"
        )),
    }
}

fn help_text() -> String {
    "Usage: casars-imager --ms PATH --imagename PREFIX --imsize N --cell-arcsec ARCSEC [options]

Options:
  --field IDS               restrict to selected FIELD_IDs (CASA selector syntax)
  --phasecenter-field ID    FIELD_ID used as the image phase center
  --phasecenter TEXT        explicit CASA-style direction used as the image phase center
  --ddid ID                 restrict to one DATA_DESC_ID
  --spw ID                  restrict to one spectral window when DDID is omitted
  --channel-start N         first selected channel
  --channel-count N         number of selected channels
  --datacolumn NAME         DATA, CORRECTED_DATA, or MODEL_DATA
  --corr XX|YY|RR|LL        explicit raw-correlation imaging
  --stokes I|Q|U|V          explicit scalar Stokes-plane imaging
  --specmode MODE           mfs, cube, or cubedata
  --weighting MODE          natural, uniform, or briggs
  --perchanweightdensity    cube uniform/briggs density per output channel
  --uvtaper SPEC            gaussian taper: MAJOR[,MINOR[,PA]] with arcsec/deg/lambda units
  --restoringbeam MODE      common
  --deconvolver MODE        hogbom, mtmfs, clark, or multiscale
  --nterms N                MTMFS Taylor-term count (default 1)
  --scales PIXELS           comma-separated multiscale sizes in pixels
  --smallscalebias VALUE    CASA multiscale bias in [-1, 1] (default 0.0)
  --robust VALUE            Briggs robust value in [-2, 2]
  --niter N                 minor-cycle iteration count
  --gain VALUE              minor-cycle gain (default 0.1)
  --threshold-jy VALUE      absolute CLEAN threshold in Jy/beam
  --nsigma VALUE            robust-RMS stopping multiplier (default 0.0)
  --psfcutoff VALUE         PSF beam-fit cutoff fraction (default 0.35)
  --minor-cycle-length N    residual refresh cadence (default 8)
  --cycleniter N            alias for --minor-cycle-length
  --cyclefactor VALUE       cycle-threshold scale factor (default 1.0)
  --minpsffraction VALUE    lower PSF-fraction clamp (default 0.1)
  --maxpsffraction VALUE    upper PSF-fraction clamp (default 0.8)
  --mask-box X0,Y0,X1,Y1    inclusive clean mask box in pixel coordinates (repeatable)
  --mask-image PATH         CASA image mask whose non-zero pixels are cleanable
  --wterm MODE              none, direct, or wproject
  --wprojplanes N           explicit CASA-style wproject plane budget
  --dirty-only              write dirty/residual products without CLEAN
  --no-preview-pngs         skip writing PNG preview sidecars
  -h, --help                show this help
"
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};

    use casa_images::PagedImage;
    use casa_ms::{MeasurementSetBuilder, OptionalMainColumn, SubtableId};
    use casa_tables::table_measures::{MeasureType, TableMeasDesc};
    use casa_test_support::{
        gridder_interop::{
            cpp_convolve_gridder_make_dirty_image_2d,
            cpp_convolve_gridder_make_model_residual_image_2d,
            cpp_convolve_gridder_predict_visibility_2d,
        },
        hogbom_interop::cpp_hogbom_clean_minor_cycle_2d,
    };
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::epoch::{EpochRef, MEpoch};
    use casa_types::measures::frame::MeasFrame;
    use casa_types::measures::frequency::MFrequency;
    use casa_types::measures::position::MPosition;
    use casa_types::{RecordField, RecordValue};
    use ndarray::{Array2, ArrayD, IxDyn};
    use tempfile::tempdir;

    use super::*;

    fn diagnostic_padded_len(image_len: usize, padding_factor: f64) -> usize {
        let padded = (image_len as f64 * padding_factor).round() as usize;
        if padded % 2 == image_len % 2 {
            padded
        } else {
            padded + 1
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct CapturedCubeMinorCycle {
        nx: usize,
        ny: usize,
        gain: f32,
        absolute_threshold_jy_per_beam: f32,
        cycle_threshold_jy_per_beam: f32,
        nsigma_threshold_jy_per_beam: f32,
        cycle_reported_niter: usize,
        psf: Vec<f32>,
        residual: Vec<f32>,
        model: Vec<f32>,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct HogbomReplay2d {
        iterdone: usize,
        residual: Vec<f32>,
    }

    fn read_captured_cube_minor_cycle(directory: &Path) -> CapturedCubeMinorCycle {
        let meta = fs::read_to_string(directory.join("meta.txt")).unwrap();
        let mut nx = None::<usize>;
        let mut ny = None::<usize>;
        let mut gain = None::<f32>;
        let mut absolute_threshold_jy_per_beam = None::<f32>;
        let mut cycle_threshold_jy_per_beam = None::<f32>;
        let mut nsigma_threshold_jy_per_beam = None::<f32>;
        let mut cycle_reported_niter = None::<usize>;
        for line in meta.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key {
                "nx" => nx = Some(value.parse().unwrap()),
                "ny" => ny = Some(value.parse().unwrap()),
                "gain" => gain = Some(value.parse().unwrap()),
                "absolute_threshold_jy_per_beam" => {
                    absolute_threshold_jy_per_beam = Some(value.parse().unwrap())
                }
                "cycle_threshold_jy_per_beam" => {
                    cycle_threshold_jy_per_beam = Some(value.parse().unwrap())
                }
                "nsigma_threshold_jy_per_beam" => {
                    nsigma_threshold_jy_per_beam = Some(value.parse().unwrap())
                }
                "cycle_reported_niter" => cycle_reported_niter = Some(value.parse().unwrap()),
                _ => {}
            }
        }
        CapturedCubeMinorCycle {
            nx: nx.expect("captured nx"),
            ny: ny.expect("captured ny"),
            gain: gain.expect("captured gain"),
            absolute_threshold_jy_per_beam: absolute_threshold_jy_per_beam
                .expect("captured absolute threshold"),
            cycle_threshold_jy_per_beam: cycle_threshold_jy_per_beam
                .expect("captured cycle threshold"),
            nsigma_threshold_jy_per_beam: nsigma_threshold_jy_per_beam
                .expect("captured nsigma threshold"),
            cycle_reported_niter: cycle_reported_niter.expect("captured cycle reported niter"),
            psf: read_captured_plane(directory.join("psf.txt")),
            residual: read_captured_plane(directory.join("residual.txt")),
            model: read_captured_plane(directory.join("model.txt")),
        }
    }

    fn read_captured_plane(path: PathBuf) -> Vec<f32> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.parse::<f32>().unwrap())
            .collect()
    }

    fn replay_rust_hogbom_minor_cycle_2d(
        psf: &[f32],
        residual: &[f32],
        shape: [usize; 2],
        gain: f32,
        threshold: f32,
        cycle_reported_niter: usize,
    ) -> HogbomReplay2d {
        let [nx, ny] = shape;
        let mut residual = residual.to_vec();
        let mut cycle_component_updates = 0usize;
        let cycle_component_budget = cycle_reported_niter.saturating_add(1);
        while cycle_component_updates < cycle_component_budget {
            let Some((peak_index, peak_value)) = peak_location_flat_xy(&residual, [nx, ny]) else {
                break;
            };
            if peak_value.abs() < threshold {
                break;
            }
            let component = gain * peak_value;
            subtract_shifted_psf_flat(&mut residual, psf, [nx, ny], peak_index, component);
            cycle_component_updates += 1;
        }
        HogbomReplay2d {
            iterdone: cycle_component_updates.min(cycle_reported_niter),
            residual,
        }
    }

    fn peak_location_flat_xy(values: &[f32], shape: [usize; 2]) -> Option<((usize, usize), f32)> {
        let [nx, ny] = shape;
        let mut best = None;
        for y in 0..ny {
            for x in 0..nx {
                let value = values[x * ny + y];
                match best {
                    None => best = Some(((x, y), value)),
                    Some((_, best_value)) if value.abs() > best_value.abs() => {
                        best = Some(((x, y), value));
                    }
                    _ => {}
                }
            }
        }
        best
    }

    fn subtract_shifted_psf_flat(
        residual: &mut [f32],
        psf: &[f32],
        shape: [usize; 2],
        peak_index: (usize, usize),
        component: f32,
    ) {
        let [nx, ny] = shape;
        let kernel_center = (nx / 2, ny / 2);
        for x in 0..nx {
            for y in 0..ny {
                let kernel_x = x as isize - peak_index.0 as isize + kernel_center.0 as isize;
                let kernel_y = y as isize - peak_index.1 as isize + kernel_center.1 as isize;
                if !(0..nx as isize).contains(&kernel_x) || !(0..ny as isize).contains(&kernel_y) {
                    continue;
                }
                let image_index = x * ny + y;
                let kernel_index = kernel_x as usize * ny + kernel_y as usize;
                residual[image_index] -= component * psf[kernel_index];
            }
        }
    }

    fn descend_f14_cube_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(10),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::LSRK,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: None,
                width: Some(
                    CubeAxisValue::parse("-1.1991563418e4km/s", DopplerRef::RADIO).unwrap(),
                ),
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn refim_point_default_cube_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 10.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(20),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn cube_channel_sample_count(channel: &CubeChannelRequest) -> usize {
        channel
            .visibility_batches
            .iter()
            .map(VisibilityBatch::len)
            .sum()
    }

    fn refim_point_cube11_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(10),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::TOPO,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: Some(CubeAxisValue::parse("11991.7km/s", DopplerRef::RADIO).unwrap()),
                width: None,
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn refim_point_cube18_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(10),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::TOPO,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: None,
                width: Some(CubeAxisValue::parse("11991.7km/s", DopplerRef::RADIO).unwrap()),
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn refim_point_cube20_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(10),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::LSRK,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: Some(CubeAxisValue::VelocityMs {
                    ms: 11_994_336.493_630_42,
                    frame: None,
                }),
                width: None,
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn refim_point_cube13_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(8),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::LSRK,
                veltype: DopplerRef::Z,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: Some(CubeAxisValue::parse("-41347.8km/s", DopplerRef::Z).unwrap()),
                width: Some(CubeAxisValue::parse("20000km/s", DopplerRef::Z).unwrap()),
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn refim_point_withline_default_cube_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: Some(0),
            channel_count: Some(20),
            datacolumn: Some("DATA".to_string()),
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    #[test]
    fn cli_parses_required_arguments() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
        ])
        .unwrap();
        assert_eq!(config.imsize, 64);
        assert_eq!(config.cell_arcsec, 1.5);
        assert_eq!(config.weighting, WeightingMode::Natural);
        assert_eq!(config.deconvolver, Deconvolver::Hogbom);
        assert!(config.multiscale_scales.is_empty());
        assert_eq!(config.w_term_mode, WTermMode::None);
        assert!(config.write_preview_pngs);
    }

    #[test]
    fn parse_plane_stokes_accepts_stokes_and_raw_corr_planes() {
        assert_eq!(parse_plane_stokes("I").unwrap(), PlaneStokes::I);
        assert_eq!(parse_plane_stokes("Q").unwrap(), PlaneStokes::Q);
        assert_eq!(parse_plane_stokes("U").unwrap(), PlaneStokes::U);
        assert_eq!(parse_plane_stokes("V").unwrap(), PlaneStokes::V);
        assert_eq!(parse_plane_stokes("XX").unwrap(), PlaneStokes::XX);
    }

    #[test]
    fn cli_parses_weighting_mask_and_wterm() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--weighting"),
            OsString::from("briggs"),
            OsString::from("--robust"),
            OsString::from("-1.0"),
            OsString::from("--mask-box"),
            OsString::from("1,2,10,20"),
            OsString::from("--mask-box"),
            OsString::from("4,5,6,7"),
            OsString::from("--mask-image"),
            OsString::from("demo.mask"),
            OsString::from("--wterm"),
            OsString::from("direct"),
        ])
        .unwrap();
        assert_eq!(config.weighting, WeightingMode::Briggs { robust: -1.0 });
        assert_eq!(config.mask_boxes, vec![[1, 2, 10, 20], [4, 5, 6, 7]]);
        assert_eq!(config.mask_image, Some(PathBuf::from("demo.mask")));
        assert_eq!(config.w_term_mode, WTermMode::Direct);
    }

    #[test]
    fn cli_parses_wproject_wterm_mode() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--wterm"),
            OsString::from("wproject"),
        ])
        .unwrap();
        assert_eq!(config.w_term_mode, WTermMode::WProject);
    }

    #[test]
    fn cli_parses_explicit_wprojplanes() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--wterm"),
            OsString::from("wproject"),
            OsString::from("--wprojplanes"),
            OsString::from("8"),
        ])
        .unwrap();
        assert_eq!(config.w_term_mode, WTermMode::WProject);
        assert_eq!(config.w_project_planes, Some(8));
    }

    #[test]
    fn cli_parses_multi_field_selector_and_phasecenter_field() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--field"),
            OsString::from("0,2~3"),
            OsString::from("--phasecenter-field"),
            OsString::from("2"),
        ])
        .unwrap();
        assert_eq!(config.field_ids, Some(vec![0, 2, 3]));
        assert_eq!(config.phasecenter_field, Some(2));
    }

    #[test]
    fn cli_parses_explicit_phasecenter_text() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--phasecenter"),
            OsString::from("J2000 19:59:28.500 +40.44.01.50"),
        ])
        .unwrap();
        assert_eq!(
            config.phasecenter.as_deref(),
            Some("J2000 19:59:28.500 +40.44.01.50")
        );
        assert_eq!(config.phasecenter_field, None);
    }

    #[test]
    fn cli_rejects_conflicting_phasecenter_options() {
        let error = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--phasecenter-field"),
            OsString::from("0"),
            OsString::from("--phasecenter"),
            OsString::from("J2000 19:59:28.500 +40.44.01.50"),
        ])
        .unwrap_err();
        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn parse_phase_center_literal_accepts_casa_style_j2000_text() {
        let phase_center = parse_phase_center_literal("J2000 19:59:28.500 +40.44.01.50").unwrap();
        assert_eq!(phase_center.field_id, None);
        assert_eq!(phase_center.reference, DirectionRef::J2000);
        assert!((phase_center.angles_rad[0] - 5.233_697_011_339_746).abs() < 1.0e-12);
        assert!((phase_center.angles_rad[1] - 0.710_938_054_184_240_3).abs() < 1.0e-12);
    }

    #[test]
    fn cli_parses_cubedata_mode_into_cube_axis_specmode() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--specmode"),
            OsString::from("cubedata"),
        ])
        .unwrap();
        assert_eq!(config.spectral_mode, SpectralMode::Cubedata);
        assert_eq!(config.cube_axis.specmode, CubeSpecMode::Cubedata);
    }

    #[test]
    fn cli_rejects_cubic_cube_interpolation_until_implemented() {
        let error = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--interpolation"),
            OsString::from("cubic"),
        ])
        .unwrap_err();
        assert!(error.contains("cubic is not implemented yet"));
    }

    #[test]
    fn cli_parses_deconvolver_selection() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--deconvolver"),
            OsString::from("clark"),
            OsString::from("--scales"),
            OsString::from("0,5,15"),
        ])
        .unwrap();
        assert_eq!(config.deconvolver, Deconvolver::Clark);
        assert_eq!(config.multiscale_scales, vec![0.0, 5.0, 15.0]);
    }

    #[test]
    fn cli_can_disable_preview_pngs() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--no-preview-pngs"),
        ])
        .unwrap();
        assert!(!config.write_preview_pngs);
    }

    #[test]
    fn parser_helpers_cover_modes_numeric_selectors_and_units() {
        assert_eq!(
            parse_data_column("data").unwrap(),
            VisibilityDataColumn::Data
        );
        assert_eq!(
            parse_data_column("corrected").unwrap(),
            VisibilityDataColumn::CorrectedData
        );
        assert_eq!(
            parse_data_column("model").unwrap(),
            VisibilityDataColumn::ModelData
        );
        assert!(parse_data_column("unsupported").is_err());

        assert_eq!(parse_single_numeric_selector("7", "field").unwrap(), 7);
        let multi = parse_single_numeric_selector("0,2~4", "spw").unwrap_err();
        assert!(multi.contains("multiple ids"));

        assert_eq!(
            parse_cube_interpolation("nearest").unwrap(),
            CubeInterpolation::Nearest
        );
        assert_eq!(
            parse_cube_interpolation("linear").unwrap(),
            CubeInterpolation::Linear
        );
        let cubic_error = parse_cube_interpolation("cubic").unwrap_err();
        assert!(cubic_error.contains("cubic is not implemented yet"));
        assert!(parse_cube_interpolation("spline").is_err());

        assert_eq!(parse_spectral_mode("mfs").unwrap(), SpectralMode::Mfs);
        assert_eq!(parse_spectral_mode("cube").unwrap(), SpectralMode::Cube);
        assert_eq!(
            parse_spectral_mode("cubedata").unwrap(),
            SpectralMode::Cubedata
        );
        assert!(parse_spectral_mode("other").is_err());

        assert_eq!(
            parse_weighting_mode("natural", 0.0).unwrap(),
            WeightingMode::Natural
        );
        assert_eq!(
            parse_weighting_mode("uniform", 0.0).unwrap(),
            WeightingMode::Uniform
        );
        assert_eq!(
            parse_weighting_mode("briggs", 0.5).unwrap(),
            WeightingMode::Briggs { robust: 0.5 }
        );
        assert!(parse_weighting_mode("invalid", 0.0).is_err());

        assert_eq!(parse_deconvolver("hogbom").unwrap(), Deconvolver::Hogbom);
        assert_eq!(parse_deconvolver("clark").unwrap(), Deconvolver::Clark);
        assert_eq!(
            parse_deconvolver("multiscale").unwrap(),
            Deconvolver::Multiscale
        );
        assert!(parse_deconvolver("other").is_err());

        assert_eq!(parse_multiscale_scales("").unwrap(), Vec::<f32>::new());
        assert_eq!(
            parse_multiscale_scales("0,5,15").unwrap(),
            vec![0.0, 5.0, 15.0]
        );
        assert!(parse_multiscale_scales("1,-1").is_err());

        assert_eq!(parse_w_term_mode("none").unwrap(), WTermMode::None);
        assert_eq!(parse_w_term_mode("2d").unwrap(), WTermMode::None);
        assert_eq!(parse_w_term_mode("direct").unwrap(), WTermMode::Direct);
        assert!(parse_w_term_mode("wproj").is_err());

        assert_eq!(parse_mask_box("1,2,3,4").unwrap(), [1, 2, 3, 4]);
        assert!(parse_mask_box("1,2,3").is_err());
        assert!(parse_mask_box("1,2,three,4").is_err());

        assert_eq!(
            parse_uv_taper_size("10arcsec").unwrap(),
            casa_imaging::UvTaperSize::ImageFwhmRad(10.0 * arcsec_to_rad())
        );
        assert_eq!(
            parse_uv_taper_size("20lambda").unwrap(),
            casa_imaging::UvTaperSize::BaselineHwhmLambda(20.0)
        );
        assert!(parse_uv_taper_size("10degrees").is_err());

        let single = parse_uv_taper("10arcsec").unwrap();
        assert_eq!(
            single.major,
            casa_imaging::UvTaperSize::ImageFwhmRad(10.0 * arcsec_to_rad())
        );
        assert_eq!(single.minor, single.major);
        assert_eq!(single.position_angle_rad, 0.0);
        let pair = parse_uv_taper("10arcsec,20lambda").unwrap();
        assert_eq!(
            pair.major,
            casa_imaging::UvTaperSize::ImageFwhmRad(10.0 * arcsec_to_rad())
        );
        assert_eq!(
            pair.minor,
            casa_imaging::UvTaperSize::BaselineHwhmLambda(20.0)
        );
        let triplet = parse_uv_taper("10arcsec,20lambda,30deg").unwrap();
        assert!((triplet.position_angle_rad - 30.0 * degrees_to_rad()).abs() < 1e-12);
        assert!(parse_uv_taper("10arcsec,20lambda,30deg,40deg").is_err());

        assert!(help_text().contains("--specmode"));
        assert!(help_text().contains("--uvtaper"));
    }

    #[test]
    fn resolve_data_column_prefers_corrected_data_when_available() {
        let corrected_ms = casa_ms::MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::CorrectedData),
        )
        .unwrap();
        assert_eq!(
            resolve_data_column(&corrected_ms, None).unwrap(),
            VisibilityDataColumn::CorrectedData
        );

        let data_ms = casa_ms::MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        assert_eq!(
            resolve_data_column(&data_ms, None).unwrap(),
            VisibilityDataColumn::Data
        );
        assert_eq!(
            resolve_data_column(&data_ms, Some("model")).unwrap(),
            VisibilityDataColumn::ModelData
        );
        assert!(resolve_data_column(&data_ms, Some("unsupported")).is_err());
    }

    #[test]
    fn clean_mask_rejects_invalid_boxes_and_mask_images() {
        assert!(build_clean_mask(4, &[[2, 1, 1, 0]], None).is_err());
        assert!(build_clean_mask(4, &[[0, 0, 4, 0]], None).is_err());

        let tmp = tempdir().unwrap();
        let path = tmp.path().join("mask.im");
        let coords = CoordinateSystem::default();
        let mut image = PagedImage::<f32>::create(vec![2, 3, 1, 1], coords, &path).unwrap();
        image.save().unwrap();

        let error = build_clean_mask(4, &[], Some(&path)).unwrap_err();
        assert!(error.contains("expected [4, 4]") || error.contains("expected [4, 4, 1, 1]"));
    }

    #[test]
    fn clean_mask_unions_boxes_and_mask_image() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("mask.im");
        let coords = CoordinateSystem::default();
        let mut image = PagedImage::<f32>::create(vec![8, 8, 1, 1], coords, &path).unwrap();
        let mut data = Array4::<f32>::zeros((8, 8, 1, 1));
        data[(6, 1, 0, 0)] = 1.0;
        image.put_slice(&data.into_dyn(), &[0, 0, 0, 0]).unwrap();
        image.save().unwrap();

        let mask = build_clean_mask(8, &[[1, 2, 2, 3], [4, 4, 4, 4]], Some(&path))
            .unwrap()
            .unwrap();
        assert!(mask[(1, 2)]);
        assert!(mask[(2, 3)]);
        assert!(mask[(4, 4)]);
        assert!(mask[(6, 1)]);
        assert!(!mask[(0, 0)]);
    }

    #[test]
    fn weight_spectrum_takes_precedence_over_weight() {
        let weight_row =
            ArrayValue::Float32(ArrayD::from_shape_vec(vec![2], vec![1.0f32, 2.0]).unwrap());
        let weight_spectrum =
            ArrayValue::Float32(ArrayD::from_shape_vec(vec![2, 1], vec![3.0f32, 4.0]).unwrap());
        let (weight, source) =
            resolve_weight_with_source(&weight_row, Some(&weight_spectrum), 1, 0).unwrap();
        assert_eq!(weight, 4.0);
        assert_eq!(source, WeightSourceKind::WeightSpectrum);
    }

    #[test]
    fn explicit_cube_linear_interpolation_drops_when_any_contributor_is_flagged() {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![1, 2],
                vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 0.0)],
            )
            .unwrap(),
        );
        let flags =
            ArrayValue::Bool(ArrayD::from_shape_vec(vec![1, 2], vec![false, true]).unwrap());
        let weights = ArrayValue::Float32(ArrayD::from_shape_vec(vec![1], vec![2.0f32]).unwrap());
        let contributions = vec![
            CubeChannelContribution {
                source_channel: 0,
                source_frequency_hz: 1.0,
                factor: 0.25,
            },
            CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 2.0,
                factor: 0.75,
            },
        ];

        let sample = interpolate_explicit_cube_output_sample(
            &data,
            &flags,
            &weights,
            None,
            0,
            &[0, 1],
            &[1.0, 2.0],
            0.0,
            &contributions,
        )
        .unwrap();
        assert!(
            sample.is_none(),
            "expected CASA-style linear interpolation to discard the whole output sample when any contributing source channel is flagged"
        );
    }

    #[test]
    fn explicit_cube_linear_interpolation_aggregates_visibility_and_weight() {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![1, 2],
                vec![Complex32::new(1.0, 2.0), Complex32::new(5.0, 6.0)],
            )
            .unwrap(),
        );
        let flags =
            ArrayValue::Bool(ArrayD::from_shape_vec(vec![1, 2], vec![false, false]).unwrap());
        let weights = ArrayValue::Float32(ArrayD::from_shape_vec(vec![1], vec![4.0f32]).unwrap());
        let weight_spectrum =
            ArrayValue::Float32(ArrayD::from_shape_vec(vec![1, 2], vec![2.0f32, 10.0]).unwrap());
        let contributions = vec![
            CubeChannelContribution {
                source_channel: 0,
                source_frequency_hz: 1.0,
                factor: 0.25,
            },
            CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 2.0,
                factor: 0.75,
            },
        ];

        let sample = interpolate_explicit_cube_output_sample(
            &data,
            &flags,
            &weights,
            Some(&weight_spectrum),
            0,
            &[0, 1],
            &[1.0, 2.0],
            0.0,
            &contributions,
        )
        .unwrap()
        .expect("expected interpolated sample");
        assert!((sample.visibility.re - 4.0).abs() < 1.0e-6);
        assert!((sample.visibility.im - 5.0).abs() < 1.0e-6);
        assert!((sample.weight - 8.0).abs() < 1.0e-6);
        assert!((sample.sumwt_factor - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn paired_cube_linear_interpolation_drops_when_any_hand_contributor_is_flagged() {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2, 2],
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(3.0, 0.0),
                    Complex32::new(2.0, 0.0),
                    Complex32::new(4.0, 0.0),
                ],
            )
            .unwrap(),
        );
        let flags = ArrayValue::Bool(
            ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, true]).unwrap(),
        );
        let weights =
            ArrayValue::Float32(ArrayD::from_shape_vec(vec![2], vec![1.0f32, 1.0]).unwrap());
        let contributions = vec![
            CubeChannelContribution {
                source_channel: 0,
                source_frequency_hz: 1.0,
                factor: 0.5,
            },
            CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 2.0,
                factor: 0.5,
            },
        ];

        let sample = interpolate_paired_cube_output_sample(
            &data,
            &flags,
            &weights,
            None,
            (0, 1),
            &[0, 1],
            &[1.0, 2.0],
            0.0,
            &contributions,
        )
        .unwrap();
        assert!(
            sample.is_none(),
            "expected strict Stokes-I linear interpolation to discard the output sample when either contributing hand is flagged"
        );
    }

    #[test]
    fn dynamic_phase_reference_is_converted_to_j2000() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("dynamic_phase.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_row(&mut ms);
        let time_mjd_sec = 59_000.5 * 86_400.0;
        let j2000 = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(time_mjd_sec / 86_400.0, EpochRef::UTC))
            .with_position(MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z))
            .with_bundled_eop();
        let azel = j2000.convert_to(DirectionRef::AZEL, &frame).unwrap();
        add_field_row_with_direction(&mut ms, azel, time_mjd_sec);
        let field_table = ms.subtable_mut(SubtableId::Field).unwrap();
        TableMeasDesc::new_fixed("PHASE_DIR", MeasureType::Direction, "AZEL")
            .write(field_table)
            .unwrap();
        ms.save().unwrap();

        let phase_center = extract_phase_center(&ms, 0).unwrap();
        assert_eq!(phase_center.reference, DirectionRef::J2000);
        assert!((phase_center.angles_rad[0] - 1.0).abs() < 1e-9);
        assert!((phase_center.angles_rad[1] - 0.5).abs() < 1e-9);
    }

    #[test]
    fn prepare_plane_input_accepts_multi_field_selection_with_shared_phase_center() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("shared_phasecenter.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };
        let ms = MeasurementSet::open(&config.ms).unwrap();
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        assert_eq!(prepared.phase_center().field_id, Some(0));
        match prepared {
            PreparedInput::Mfs(plane) => {
                let sample_count = plane
                    .batches
                    .iter()
                    .map(VisibilityBatch::len)
                    .sum::<usize>();
                assert_eq!(sample_count, 2);
            }
            PreparedInput::Cube(_) => panic!("expected MFS prepared input"),
        }
    }

    #[test]
    fn prepare_plane_input_requires_explicit_phasecenter_for_distinct_multi_field_selection() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("distinct_phasecenter.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_field_row_with_direction(
            &mut ms,
            MDirection::from_angles(1.1, 0.55, DirectionRef::J2000),
            TEST_TIME_MJD_SEC,
        );
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };
        let ms = MeasurementSet::open(&config.ms).unwrap();
        let error = match prepare_plane_input(&ms, &config, VisibilityDataColumn::Data) {
            Ok(_) => {
                panic!("expected distinct multi-field selection to require --phasecenter-field")
            }
            Err(error) => error,
        };
        assert!(error.contains("--phasecenter-field"));
    }

    #[test]
    fn prepare_plane_input_reprojects_distinct_phase_center_rows_to_target_field() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("reproject_distinct_phasecenter.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_observation_row(&mut ms);
        add_field_row(&mut ms);
        add_field_row_with_direction(
            &mut ms,
            MDirection::from_angles(1.1, 0.55, DirectionRef::J2000),
            TEST_TIME_MJD_SEC,
        );
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 5.0],
            &[Complex32::new(1.0, 0.5), Complex32::new(0.0, 0.0)],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, -7.5],
            &[Complex32::new(0.25, 1.25), Complex32::new(0.0, 0.0)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path.clone(),
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let ms = MeasurementSet::open(&config.ms).unwrap();
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Mfs(plane) = prepared else {
            panic!("expected MFS prepared input");
        };
        let samples = plane
            .batches
            .iter()
            .flat_map(|batch| {
                (0..batch.len()).map(move |index| {
                    (
                        batch.u_lambda[index],
                        batch.v_lambda[index],
                        batch.w_lambda[index],
                        batch.visibility[index],
                    )
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(samples.len(), 2);

        let lambda_scale = 1.4e9 / SPEED_OF_LIGHT_M_PER_S;
        assert!((samples[0].0 - 30.0 * lambda_scale).abs() < 1.0e-9);
        assert!((samples[0].1 - -15.0 * lambda_scale).abs() < 1.0e-9);
        assert!((samples[0].2 - 5.0 * lambda_scale).abs() < 1.0e-9);
        assert!((samples[0].3 - Complex32::new(1.0, 0.5)).norm() < 1.0e-6);

        let engine = MsCalEngine::new(&ms).unwrap();
        let (target_uvw_m, phase_shift_m) = engine
            .reproject_raw_uvw_between_fields([-25.0, 20.0, -7.5], 1, 0)
            .unwrap();
        let expected_visibility =
            phase_rotate_visibility(Complex32::new(0.25, 1.25), phase_shift_m, 1.4e9);
        assert!((samples[1].0 - target_uvw_m[0] * lambda_scale).abs() < 1.0e-9);
        assert!((samples[1].1 - target_uvw_m[1] * lambda_scale).abs() < 1.0e-9);
        assert!((samples[1].2 - target_uvw_m[2] * lambda_scale).abs() < 1.0e-9);
        assert!((samples[1].3 - expected_visibility).norm() < 1.0e-5);
    }

    #[test]
    fn prepare_plane_trace_records_row_identity_and_reprojection() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_reproject.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_observation_row(&mut ms);
        add_field_row(&mut ms);
        add_field_row_with_direction(
            &mut ms,
            MDirection::from_angles(1.1, 0.55, DirectionRef::J2000),
            TEST_TIME_MJD_SEC,
        );
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 5.0],
            &[Complex32::new(1.0, 0.5), Complex32::new(0.0, 0.0)],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, -7.5],
            &[Complex32::new(0.25, 1.25), Complex32::new(0.0, 0.0)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path.clone(),
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_plane_trace_from_config(&config).unwrap();
        assert_eq!(trace.schema_version, ORACLE_SCHEMA_VERSION);
        assert_eq!(trace.data_column, "DATA");
        assert_eq!(trace.spectral_mode, "mfs");
        assert_eq!(trace.phase_center.reference, "J2000");
        assert_eq!(trace.selected_rows.len(), 2);
        assert_eq!(trace.samples.len(), 2);
        assert!(trace.rejected_samples.is_empty());
        assert_eq!(trace.selected_rows[0].row_index, 0);
        assert_eq!(trace.selected_rows[1].row_index, 1);
        assert_eq!(trace.selected_rows[1].field_id, 1);

        let first = &trace.samples[0];
        assert_eq!(first.row_index, 0);
        assert_eq!(first.input_field_id, 0);
        assert_eq!(first.phase_center_field_id, Some(0));
        assert_eq!(first.correlation_indices, vec![0]);
        assert_eq!(first.output_channel_index, None);
        assert_eq!(first.weight_source, WeightSourceKind::Weight);
        assert!((first.raw_uvw_m[0] - 30.0).abs() < 1.0e-9);
        assert!((first.imaging_uvw_m[2] - 5.0).abs() < 1.0e-9);

        let second = &trace.samples[1];
        let ms = MeasurementSet::open(&config.ms).unwrap();
        let engine = MsCalEngine::new(&ms).unwrap();
        let (target_uvw_m, phase_shift_m) = engine
            .reproject_raw_uvw_between_fields([-25.0, 20.0, -7.5], 1, 0)
            .unwrap();
        let expected_visibility =
            phase_rotate_visibility(Complex32::new(0.25, 1.25), phase_shift_m, 1.4e9);
        assert_eq!(second.row_index, 1);
        assert_eq!(second.input_field_id, 1);
        assert_eq!(second.phase_center_field_id, Some(0));
        assert_eq!(second.weight_source, WeightSourceKind::Weight);
        assert!((second.raw_uvw_m[0] + 25.0).abs() < 1.0e-9);
        assert!((second.raw_uvw_m[2] + 7.5).abs() < 1.0e-9);
        assert!((second.imaging_uvw_m[0] - target_uvw_m[0]).abs() < 1.0e-9);
        assert!((second.imaging_uvw_m[1] - target_uvw_m[1]).abs() < 1.0e-9);
        assert!((second.imaging_uvw_m[2] - target_uvw_m[2]).abs() < 1.0e-9);
        assert!((second.phase_shift_m - phase_shift_m).abs() < 1.0e-9);
        assert!((second.visibility_re - expected_visibility.re).abs() < 1.0e-6);
        assert!((second.visibility_im - expected_visibility.im).abs() < 1.0e-6);
        assert_eq!(second.source_contributions.len(), 1);
        assert_eq!(second.source_contributions[0].source_channel_index, 0);
    }

    #[test]
    fn build_w_project_trace_from_config_emits_serializable_plan_bundle() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("wproject_trace.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [15.0, -20.0, 30.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(0.0, 0.0)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 64,
            cell_arcsec: 800.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::WProject,
            w_project_planes: Some(6),
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_w_project_trace_from_config(&config).unwrap();

        assert_eq!(trace.schema_version, ORACLE_SCHEMA_VERSION);
        assert_eq!(trace.spectral_mode, "mfs");
        assert_eq!(trace.channel_index, None);
        assert_eq!(trace.channel_frequency_hz, None);
        assert_eq!(trace.requested_plane_count, Some(6));
        assert_eq!(trace.plane_count, 6);
        assert_eq!(trace.gridded_samples, 1);
        assert_eq!(trace.samples.len(), 1);
        assert!(trace.skipped_samples.is_empty());
        assert_eq!(trace.samples[0].batch_index, 0);
        assert_eq!(trace.samples[0].sample_index, 0);
        assert_eq!(trace.samples[0].sumwt_factor, 1.0);
    }

    #[test]
    #[ignore = "diagnostic for Wave 12 source-backed wproject plan summary on refim_point_wterm_vlad"]
    fn wave12_wproject_plan_summary_on_refim_point_wterm_vlad() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT").map(PathBuf::from) else {
            eprintln!("skipping diagnostic: CASA_RS_TESTDATA_ROOT not set");
            return;
        };
        let ms_path = root.join("unittest/tclean/refim_point_wterm_vlad.ms");
        if !ms_path.exists() {
            eprintln!("skipping diagnostic: missing {}", ms_path.display());
            return;
        }
        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 256,
            cell_arcsec: 80.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: None,
            channel_start: Some(0),
            channel_count: Some(1),
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Uniform,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::WProject,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_w_project_trace_from_config(&config).unwrap();
        let mut plane_histogram = BTreeMap::<usize, usize>::new();
        let mut support_histogram = BTreeMap::<usize, usize>::new();
        for sample in &trace.samples {
            *plane_histogram.entry(sample.plane_index).or_default() += 1;
            *support_histogram.entry(sample.support).or_default() += 1;
        }
        let kernel_supports = trace
            .kernels
            .iter()
            .map(|kernel| kernel.support * trace.sampling)
            .collect::<Vec<_>>();
        eprintln!(
            "wave12 wproject trace: planes={} sampling={} max_abs_w_lambda={:.6} gridded={} skipped={} normalization_sumwt={:.6} reported_sumwt={:.6} kernel_support_pixels={:?} plane_hist={:?} support_hist={:?}",
            trace.plane_count,
            trace.sampling,
            trace.max_abs_w_lambda,
            trace.gridded_samples,
            trace.skipped_samples.len(),
            trace.normalization_sumwt,
            trace.reported_sumwt,
            kernel_supports,
            plane_histogram,
            support_histogram,
        );
    }

    #[test]
    fn prepare_geometry_trace_records_row_identity_and_reprojection() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("geometry_reproject.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_field_row_with_direction(
            &mut ms,
            MDirection::from_angles(1.1, 0.55, DirectionRef::J2000),
            TEST_TIME_MJD_SEC,
        );
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 5.0],
            &[Complex32::new(1.0, 0.5), Complex32::new(0.0, 0.0)],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, -7.5],
            &[Complex32::new(0.25, 1.25), Complex32::new(0.0, 0.0)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path.clone(),
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_geometry_trace_from_config(&config).unwrap();
        assert_eq!(trace.schema_version, ORACLE_SCHEMA_VERSION);
        assert_eq!(trace.phase_center.field_id, Some(0));
        assert_eq!(trace.phase_center.reference, "J2000");
        assert_eq!(trace.selected_rows.len(), 2);
        assert_eq!(trace.rows.len(), 2);
        assert_eq!(trace.rows[0].row_index, 0);
        assert_eq!(trace.rows[1].row_index, 1);
        assert_eq!(trace.rows[0].input_field_id, 0);
        assert_eq!(trace.rows[1].input_field_id, 1);
        assert_eq!(trace.rows[1].phase_center_field_id, Some(0));
        assert_eq!(trace.rows[0].pointing_id, None);
        assert_eq!(trace.rows[1].pointing_id, None);
        assert!((trace.rows[0].raw_uvw_m[0] - 30.0).abs() < 1.0e-9);
        assert!((trace.rows[0].imaging_uvw_m[2] - 5.0).abs() < 1.0e-9);

        let ms = MeasurementSet::open(&config.ms).unwrap();
        let engine = MsCalEngine::new(&ms).unwrap();
        let (target_uvw_m, phase_shift_m) = engine
            .reproject_raw_uvw_between_fields([-25.0, 20.0, -7.5], 1, 0)
            .unwrap();
        assert!((trace.rows[1].raw_uvw_m[0] + 25.0).abs() < 1.0e-9);
        assert!((trace.rows[1].raw_uvw_m[2] + 7.5).abs() < 1.0e-9);
        assert!((trace.rows[1].imaging_uvw_m[0] - target_uvw_m[0]).abs() < 1.0e-9);
        assert!((trace.rows[1].imaging_uvw_m[1] - target_uvw_m[1]).abs() < 1.0e-9);
        assert!((trace.rows[1].imaging_uvw_m[2] - target_uvw_m[2]).abs() < 1.0e-9);
        assert!((trace.rows[1].phase_shift_m - phase_shift_m).abs() < 1.0e-9);
    }

    #[test]
    fn prepare_plane_trace_records_weight_spectrum_for_stokes_i_collapse() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_weight_spectrum.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new()
                .with_main_column(OptionalMainColumn::Data)
                .with_main_column(OptionalMainColumn::WeightSpectrum),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_channels_and_weight_spectrum(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(3.0, 0.5)],
            &[3.0, 5.0],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_plane_trace_from_config(&config).unwrap();
        assert_eq!(trace.samples.len(), 1);
        assert!(trace.rejected_samples.is_empty());
        let sample = &trace.samples[0];
        assert_eq!(sample.correlation_indices, vec![0, 1]);
        assert_eq!(sample.weight_source, WeightSourceKind::WeightSpectrum);
        assert!((sample.weight - 4.0).abs() < 1.0e-6);
        assert!((sample.sumwt_factor - 2.0).abs() < 1.0e-6);
        assert!((sample.visibility_re - 2.0).abs() < 1.0e-6);
        assert!((sample.visibility_im - 0.25).abs() < 1.0e-6);
    }

    #[test]
    fn prepare_plane_trace_preserves_rejected_paired_samples() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_rejected_pair.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_channels(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(3.0, 0.5)],
        );
        ms.main_table_mut()
            .set_cell(
                0,
                "FLAG",
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(vec![2, 1], vec![true, false]).unwrap(),
                )),
            )
            .unwrap();
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_plane_trace_from_config(&config).unwrap();
        assert!(trace.samples.is_empty());
        assert_eq!(trace.rejected_samples.len(), 1);
        let rejected = &trace.rejected_samples[0];
        assert_eq!(rejected.row_index, 0);
        assert_eq!(rejected.correlation_indices, vec![0, 1]);
        assert!(rejected.first_flagged);
        assert!(!rejected.second_flagged);
        assert_eq!(
            rejected.rejection_reason,
            PreparedSampleRejectionReason::FlaggedCorrelation
        );
    }

    #[test]
    fn prepare_plane_trace_records_linear_stokes_q_collapse() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_linear_q.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_full_linear_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_corr_channels(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            4,
            &[
                Complex32::new(5.0, 0.0),
                Complex32::new(1.0, 2.0),
                Complex32::new(1.0, -2.0),
                Complex32::new(3.0, 0.0),
            ],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("Q".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_plane_trace_from_config(&config).unwrap();
        assert_eq!(trace.samples.len(), 1);
        let sample = &trace.samples[0];
        assert_eq!(sample.correlation_indices, vec![0, 3]);
        assert!((sample.visibility_re - 1.0).abs() < 1.0e-6);
        assert!(sample.visibility_im.abs() < 1.0e-6);
        assert!((sample.weight - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn prepare_plane_trace_records_circular_stokes_u_collapse() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_circular_u.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_full_circular_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_corr_channels(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            4,
            &[
                Complex32::new(5.0, 0.0),
                Complex32::new(2.0, -1.0),
                Complex32::new(2.0, 1.0),
                Complex32::new(3.0, 0.0),
            ],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("U".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_plane_trace_from_config(&config).unwrap();
        assert_eq!(trace.samples.len(), 1);
        let sample = &trace.samples[0];
        assert_eq!(sample.correlation_indices, vec![1, 2]);
        assert!((sample.visibility_re - 1.0).abs() < 1.0e-6);
        assert!(sample.visibility_im.abs() < 1.0e-6);
        assert!((sample.weight - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn prepare_spectral_trace_records_linear_cube_contributions_and_weight_source() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_cube_linear.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new()
                .with_main_column(OptionalMainColumn::Data)
                .with_main_column(OptionalMainColumn::WeightSpectrum),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.0e9, 1.2e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_channels_and_weight_spectrum(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(3.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
            ],
            &[2.0, 4.0, 1.0, 1.0],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(1),
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::TOPO,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: Some(CubeAxisValue::FrequencyHz {
                    hz: 1.1e9,
                    frame: None,
                }),
                width: Some(CubeAxisValue::FrequencyHz {
                    hz: 1.0e8,
                    frame: None,
                }),
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_spectral_trace_from_config(&config).unwrap();
        let spectral_axis = build_prepare_spectral_axis_trace(&trace);
        assert_eq!(trace.spectral_mode, "cube");
        assert_eq!(trace.samples.len(), 1);
        assert!(trace.rejected_samples.is_empty());
        assert_eq!(trace.source_channel_indices, vec![0, 1]);
        assert_eq!(spectral_axis.source_channels.len(), 2);
        assert_eq!(spectral_axis.output_channels.len(), 1);
        assert!((spectral_axis.output_channels[0].frequency_hz - 1.1e9).abs() < 1.0e-3);

        let sample = &trace.samples[0];
        assert_eq!(sample.output_channel_index, Some(0));
        assert_eq!(sample.correlation_indices, vec![0]);
        assert_eq!(sample.weight_source, WeightSourceKind::WeightSpectrum);
        assert!((sample.output_frequency_hz - 1.1e9).abs() < 1.0e-3);
        assert!((sample.visibility_re - 2.0).abs() < 1.0e-6);
        assert!(sample.visibility_im.abs() < 1.0e-6);
        assert!((sample.weight - 3.0).abs() < 1.0e-6);
        assert!((sample.sumwt_factor - 1.0).abs() < 1.0e-6);
        assert_eq!(sample.source_contributions.len(), 2);
        assert_eq!(sample.source_contributions[0].source_channel_index, 0);
        assert_eq!(sample.source_contributions[1].source_channel_index, 1);
        assert!((sample.source_contributions[0].factor - 0.5).abs() < 1.0e-6);
        assert!((sample.source_contributions[1].factor - 0.5).abs() < 1.0e-6);
    }

    #[test]
    fn prepare_spectral_trace_cubedata_keeps_native_output_axis_with_outframe_override() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_cubedata_native.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.0e9, 1.1e9, 1.2e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(2.0, 0.0),
                Complex32::new(3.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
            ],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(2),
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Cubedata,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cubedata,
                outframe: FrequencyRef::LSRK,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: Some(CubeAxisValue::Channel(1)),
                width: Some(CubeAxisValue::Channel(1)),
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_spectral_trace_from_config(&config).unwrap();
        let spectral_axis = build_prepare_spectral_axis_trace(&trace);
        assert_eq!(trace.spectral_mode, "cubedata");
        assert_eq!(trace.samples.len(), 2);
        let output_frequencies = spectral_axis
            .output_channels
            .iter()
            .map(|channel| channel.frequency_hz)
            .collect::<Vec<_>>();
        assert!((output_frequencies[0] - 1.1e9).abs() < 1.0e-3);
        assert!((output_frequencies[1] - 1.2e9).abs() < 1.0e-3);
        assert_eq!(trace.samples[0].source_contributions.len(), 1);
        assert_eq!(
            trace.samples[0].source_contributions[0].source_channel_index,
            1
        );
        assert!((trace.samples[0].source_contributions[0].factor - 1.0).abs() < 1.0e-6);
        assert_eq!(trace.samples[1].source_contributions.len(), 1);
        assert_eq!(
            trace.samples[1].source_contributions[0].source_channel_index,
            2
        );
        assert!((trace.samples[1].source_contributions[0].factor - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn infer_oracle_dataset_tier_classifies_known_datasets() {
        assert_eq!(
            infer_oracle_dataset_tier(Path::new("/tmp/refim_point_withline.ms")),
            DatasetTier::TierB
        );
        assert_eq!(
            infer_oracle_dataset_tier(Path::new("/tmp/M51.ms")),
            DatasetTier::TierC
        );
        assert_eq!(
            infer_oracle_dataset_tier(Path::new("/tmp/ngc5921.ms")),
            DatasetTier::TierA
        );
    }

    #[test]
    fn write_prepare_plane_oracle_bundle_persists_manifest_and_artifacts() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("oracle_bundle.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(3.0, 0.5)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path.clone(),
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let output_dir = tmp.path().join("oracle_bundle");
        let manifest =
            write_prepare_plane_oracle_bundle_from_config(&config, &output_dir, DatasetTier::TierA)
                .unwrap();
        assert_eq!(manifest.dataset_tier, DatasetTier::TierA);
        let dataset_sha256 = manifest.dataset_sha256.clone().unwrap();
        let expected_identity = format!("sha256:{dataset_sha256}");
        assert_eq!(
            manifest.dataset_identity.as_deref(),
            Some(expected_identity.as_str())
        );
        assert_eq!(manifest.artifacts.len(), 6);
        assert!(output_dir.join("bundle_manifest.json").exists());
        assert!(output_dir.join("spectral_axis.json").exists());
        for artifact in &manifest.artifacts {
            assert!(artifact.sha256.is_some());
            assert!(output_dir.join(&artifact.relative_path).exists());
        }
        let persisted: OracleBundleManifest = serde_json::from_slice(
            &std::fs::read(output_dir.join("bundle_manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            persisted.parameter_manifest.get("spectral_mode"),
            Some(&"mfs".to_string())
        );
        assert_eq!(
            persisted.parameter_manifest.get("correlation"),
            Some(&"XX".to_string())
        );
    }

    #[test]
    fn write_prepare_plane_oracle_bundle_honors_manifest_overrides() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("oracle_bundle_override.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[Complex32::new(1.0, 0.0), Complex32::new(3.0, 0.5)],
        );
        ms.save().unwrap();

        let config = CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let output_dir = tmp.path().join("oracle_bundle_override");
        let manifest = write_prepare_plane_oracle_bundle_from_config_with_overrides(
            &config,
            &output_dir,
            DatasetTier::TierB,
            &OracleBundleOverrides {
                dataset_path: Some("/frozen/source.ms".to_string()),
                dataset_identity: Some("sha256:deadbeef".to_string()),
                dataset_sha256: Some("deadbeef".to_string()),
                casa_version: Some("casa-x".to_string()),
                casacore_version: Some("casacore-y".to_string()),
            },
        )
        .unwrap();
        assert_eq!(manifest.dataset_path, "/frozen/source.ms");
        assert_eq!(
            manifest.dataset_identity.as_deref(),
            Some("sha256:deadbeef")
        );
        assert_eq!(manifest.dataset_sha256.as_deref(), Some("deadbeef"));
        assert_eq!(manifest.casa_version.as_deref(), Some("casa-x"));
        assert_eq!(manifest.casacore_version.as_deref(), Some("casacore-y"));
    }

    #[test]
    fn end_to_end_smoke_writes_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny.ms");
        let image_prefix = tmp.path().join("tiny_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [-25.0, 20.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [10.0, 35.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 4,
            gain: 0.2,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: false,
        })
        .unwrap();
        assert!(summary.gridded_samples > 0);
        assert!(summary.frontend_timings.total > Duration::default());

        for suffix in ["psf", "residual", "model", "image", "sumwt"] {
            let path = format!("{}.{}", image_prefix.display(), suffix);
            assert!(Path::new(&path).exists(), "missing product {path}");
        }
        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[32, 32, 1, 1]);
        assert_eq!(image.units(), "Jy/beam");
        let residual =
            PagedImage::<f32>::open(format!("{}.residual", image_prefix.display())).unwrap();
        assert_eq!(residual.units(), "Jy/beam");
        let sumwt = PagedImage::<f32>::open(format!("{}.sumwt", image_prefix.display())).unwrap();
        assert_eq!(sumwt.shape(), &[1, 1, 1, 1]);
    }

    #[test]
    fn multi_field_smoke_writes_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_multifield.ms");
        let image_prefix = tmp.path().join("tiny_multifield_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, 0.0],
            &[
                Complex32::new(0.8, 0.0),
                Complex32::new(0.8, 0.0),
                Complex32::new(0.5, 0.0),
                Complex32::new(0.5, 0.0),
            ],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [10.0, 35.0, 0.0],
            &[
                Complex32::new(0.7, 0.0),
                Complex32::new(0.7, 0.0),
                Complex32::new(0.4, 0.0),
                Complex32::new(0.4, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 2,
            gain: 0.2,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        for suffix in ["psf", "residual", "model", "image", "sumwt"] {
            let path = format!("{}.{}", image_prefix.display(), suffix);
            assert!(Path::new(&path).exists(), "missing product {path}");
        }
    }

    #[test]
    fn cube_dirty_smoke_writes_channelized_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_cube.ms");
        let image_prefix = tmp.path().join("tiny_cube_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9, 1.401e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [-25.0, 20.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(0.3, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.3, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [10.0, 35.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(0.8, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.8, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[32, 32, 1, 2]);
        let sumwt = PagedImage::<f32>::open(format!("{}.sumwt", image_prefix.display())).unwrap();
        assert_eq!(sumwt.shape(), &[1, 1, 1, 2]);
    }

    #[test]
    fn mtmfs_smoke_writes_taylor_terms_and_preview_pngs() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_mtmfs.ms");
        let image_prefix = tmp.path().join("tiny_mtmfs_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.39e9, 1.41e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(0.8, 0.0),
                Complex32::new(0.8, 0.0),
                Complex32::new(1.2, 0.0),
                Complex32::new(1.2, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [-25.0, 20.0, 0.0],
            &[
                Complex32::new(0.7, 0.0),
                Complex32::new(0.7, 0.0),
                Complex32::new(1.3, 0.0),
                Complex32::new(1.3, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [10.0, 35.0, 0.0],
            &[
                Complex32::new(0.9, 0.0),
                Complex32::new(0.9, 0.0),
                Complex32::new(1.1, 0.0),
                Complex32::new(1.1, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Mtmfs,
            nterms: 2,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 6,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        assert!(summary.major_cycles > 0);
        assert!(summary.minor_iterations > 0);

        for suffix in [
            "psf.tt0",
            "psf.tt1",
            "residual.tt0",
            "residual.tt1",
            "model.tt0",
            "model.tt1",
            "image.tt0",
            "image.tt1",
            "sumwt.tt0",
            "sumwt.tt1",
            "alpha",
            "alpha.error",
        ] {
            let path = format!("{}.{}", image_prefix.display(), suffix);
            assert!(Path::new(&path).exists(), "missing MTMFS product {path}");
        }

        for suffix in [
            "psf.tt0.png",
            "residual.tt0.png",
            "model.tt0.png",
            "image.tt0.png",
            "alpha.png",
        ] {
            let path = format!("{}.{}", image_prefix.display(), suffix);
            assert!(Path::new(&path).exists(), "missing MTMFS preview {path}");
        }

        let image_tt0 =
            PagedImage::<f32>::open(format!("{}.image.tt0", image_prefix.display())).unwrap();
        assert_eq!(image_tt0.shape(), &[32, 32, 1, 1]);
        assert_eq!(image_tt0.units(), "Jy/beam");
        let alpha = PagedImage::<f32>::open(format!("{}.alpha", image_prefix.display())).unwrap();
        assert_eq!(alpha.shape(), &[32, 32, 1, 1]);
    }

    #[test]
    fn clark_smoke_writes_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_clark.ms");
        let image_prefix = tmp.path().join("tiny_clark_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [-25.0, 20.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.3, 0.0),
                Complex32::new(0.3, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [10.0, 35.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.8, 0.0),
                Complex32::new(0.8, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Clark,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 4,
            gain: 0.2,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        assert!(summary.minor_iterations > 0);
        assert!(summary.major_cycles > 0);

        for suffix in ["psf", "residual", "model", "image", "sumwt"] {
            let path = format!("{}.{}", image_prefix.display(), suffix);
            assert!(Path::new(&path).exists(), "missing product {path}");
        }

        let model = PagedImage::<f32>::open(format!("{}.model", image_prefix.display())).unwrap();
        assert_eq!(model.shape(), &[32, 32, 1, 1]);
        let max_model = model
            .get()
            .unwrap()
            .iter()
            .fold(0.0f32, |current, value| current.max(value.abs()));
        assert!(max_model > 0.0);
    }

    #[test]
    fn multiscale_smoke_writes_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_multiscale.ms");
        let image_prefix = tmp.path().join("tiny_multiscale_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_channels(
            &mut ms,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.6, 0.0),
                Complex32::new(0.6, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [-25.0, 20.0, 0.0],
            &[
                Complex32::new(0.8, 0.0),
                Complex32::new(0.8, 0.0),
                Complex32::new(0.5, 0.0),
                Complex32::new(0.5, 0.0),
            ],
        );
        add_main_row_channels(
            &mut ms,
            [10.0, 35.0, 0.0],
            &[
                Complex32::new(0.7, 0.0),
                Complex32::new(0.7, 0.0),
                Complex32::new(0.4, 0.0),
                Complex32::new(0.4, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Multiscale,
            nterms: 1,
            multiscale_scales: vec![0.0, 3.0],
            small_scale_bias: 0.6,
            niter: 4,
            gain: 0.2,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        assert!(summary.minor_iterations > 0);
        assert!(summary.major_cycles > 0);

        for suffix in ["psf", "residual", "model", "image", "sumwt"] {
            let path = format!("{}.{}", image_prefix.display(), suffix);
            assert!(Path::new(&path).exists(), "missing product {path}");
        }

        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[32, 32, 1, 1]);
        let max_image = image
            .get()
            .unwrap()
            .iter()
            .fold(0.0f32, |current, value| current.max(value.abs()));
        assert!(max_image > 0.0);
    }

    #[test]
    fn cube_linear_interpolation_smoke_writes_channelized_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_cube_linear.ms");
        let image_prefix = tmp.path().join("tiny_cube_linear_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.0e9, 1.2e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_channels_and_weight_spectrum(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            &[
                Complex32::new(1.0, 0.0),
                Complex32::new(3.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
            ],
            &[2.0, 4.0, 1.0, 1.0],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: Some(0),
            spw_selector: Some("0".to_string()),
            channel_start: None,
            channel_count: Some(1),
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig {
                specmode: CubeSpecMode::Cube,
                outframe: FrequencyRef::TOPO,
                veltype: DopplerRef::RADIO,
                interpolation: CubeInterpolation::Linear,
                rest_frequency_hz: Some(1.25e9),
                start: Some(CubeAxisValue::FrequencyHz {
                    hz: 1.1e9,
                    frame: None,
                }),
                width: Some(CubeAxisValue::FrequencyHz {
                    hz: 1.0e8,
                    frame: None,
                }),
            },
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[32, 32, 1, 1]);
        let sumwt = PagedImage::<f32>::open(format!("{}.sumwt", image_prefix.display())).unwrap();
        assert_eq!(sumwt.shape(), &[1, 1, 1, 1]);
    }

    #[test]
    fn multi_field_phasecenter_smoke_writes_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_multifield_phasecenter.ms");
        let image_prefix = tmp.path().join("tiny_multifield_phasecenter_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_observation_row(&mut ms);
        add_field_row(&mut ms);
        add_field_row_with_direction(
            &mut ms,
            MDirection::from_angles(1.1, 0.55, DirectionRef::J2000),
            TEST_TIME_MJD_SEC,
        );
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_channels(
            &mut ms,
            0,
            [30.0, -15.0, 5.0],
            &[Complex32::new(1.0, 0.5), Complex32::new(1.0, 0.5)],
        );
        add_main_row_with_field_channels(
            &mut ms,
            1,
            [-25.0, 20.0, -7.5],
            &[Complex32::new(0.25, 1.25), Complex32::new(0.25, 1.25)],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: Some(vec![0, 1]),
            phasecenter_field: Some(0),
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[32, 32, 1, 1]);
    }

    #[test]
    fn stokes_q_dirty_smoke_writes_casa_products() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_stokes_q.ms");
        let image_prefix = tmp.path().join("tiny_stokes_q_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_vla_antenna_pair(&mut ms);
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_full_linear_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_field_and_antennas_corr_channels(
            &mut ms,
            0,
            0,
            1,
            [30.0, -15.0, 0.0],
            4,
            &[
                Complex32::new(5.0, 0.0),
                Complex32::new(1.0, 2.0),
                Complex32::new(1.0, -2.0),
                Complex32::new(3.0, 0.0),
            ],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
            imagename: image_prefix.clone(),
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: Some("Q".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        })
        .unwrap();

        assert!(summary.gridded_samples > 0);
        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[32, 32, 1, 1]);
        let sumwt = PagedImage::<f32>::open(format!("{}.sumwt", image_prefix.display())).unwrap();
        assert_eq!(sumwt.shape(), &[1, 1, 1, 1]);
    }

    #[test]
    fn autocorrelations_are_excluded_from_gridding() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("auto.ms");
        let image_prefix = tmp.path().join("auto_image");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_field_row(&mut ms);
        add_spectral_window_row(&mut ms, &[1.4e9]);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_main_row_with_antennas(
            &mut ms,
            0,
            0,
            [0.0, 0.0, 0.0],
            [Complex32::new(50.0, 0.0), Complex32::new(50.0, 0.0)],
        );
        add_main_row_with_antennas(
            &mut ms,
            0,
            1,
            [15.0, -20.0, 0.0],
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path,
            imagename: image_prefix,
            imsize: 32,
            cell_arcsec: 20.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: RestoringBeamMode::PerPlane,
            deconvolver: Deconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        })
        .unwrap();

        assert_eq!(summary.gridded_samples, 1);
    }

    #[test]
    fn descending_frequency_f14_cube_setup_clips_low_edge_before_imaging() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_descendingfreqs.ms"),
            root.join("measurementset/vla/refim_point_descendingfreqs.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let config = descend_f14_cube_config(ms_path);
        let data_description = ms.data_description().unwrap();
        let ddid_info = data_description_index(&data_description).unwrap();
        let spectral_window = ms.spectral_window().unwrap();
        let polarization = ms.polarization().unwrap();
        let engine = MsCalEngine::new(&ms).unwrap();
        let time_column = TimeColumn::new(ms.main_table());
        let field_column = ms.main_table().get_column("FIELD_ID").unwrap();
        let ddid_column = ms.main_table().get_column("DATA_DESC_ID").unwrap();
        let mut reference_time = None::<f64>;
        let mut bounds = None::<[f64; 2]>;
        for (field_cell, ddid_cell) in field_column.zip(ddid_column) {
            let row = field_cell.row_index;
            let field_id = match field_cell.value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                _ => continue,
            };
            let ddid = match ddid_cell.value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                _ => continue,
            };
            if field_id != 0 || ddid != 0 {
                continue;
            }
            let row_time = time_column.get_mjd_seconds(row).unwrap();
            reference_time.get_or_insert(row_time);
            match &mut bounds {
                Some(existing) => {
                    existing[0] = existing[0].min(row_time);
                    existing[1] = existing[1].max(row_time);
                }
                None => bounds = Some([row_time, row_time]),
            }
        }
        let prepared = PreparedSelection::new(
            &config,
            0,
            &ddid_info,
            &spectral_window,
            &polarization,
            PhaseCenter {
                field_id: Some(0),
                angles_rad: [0.0, 0.0],
                reference: DirectionRef::J2000,
            },
            Some(CubeSetupContext {
                phase_center_field_id: 0,
                reference_row_time_mjd_sec: reference_time.unwrap(),
                time_bounds_mjd_sec: bounds.unwrap(),
                derived_engine: &engine,
            }),
        );
        assert!(
            prepared.initialization_error.is_none(),
            "prepared selection init error: {:?}",
            prepared.initialization_error
        );
        let cube_setup = prepared.cube_spectral_setup.as_ref().unwrap();
        let contributions = cube_setup
            .row_output_channel_contributions_batch(
                &prepared.source_channel_frequencies_hz,
                &prepared.source_channel_widths_hz,
                reference_time.unwrap(),
                0,
                &engine,
            )
            .unwrap();
        assert!(
            contributions[0].is_empty(),
            "expected no low-edge support for output channel 0, got output_freq={} support={:?} source_freqs={:?}",
            cube_setup.output_channel_frequencies_hz[0],
            contributions[0],
            prepared.source_channel_frequencies_hz
        );
    }

    #[test]
    fn descending_frequency_f14_prepared_cube_leaves_first_plane_empty() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_descendingfreqs.ms"),
            root.join("measurementset/vla/refim_point_descendingfreqs.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = descend_f14_cube_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 10);
        assert_eq!(
            cube_channel_sample_count(&cube.channels[0]),
            0,
            "expected output plane 0 to remain empty before imaging"
        );
        assert!(
            cube_channel_sample_count(&cube.channels[1]) > 0,
            "expected output plane 1 to receive interpolated visibility samples"
        );
        assert!(
            cube.channels[0].channel_frequency_hz < cube.channels[1].channel_frequency_hz,
            "expected ascending low-to-high output cube axis"
        );
    }

    #[test]
    fn descending_frequency_f14_in_memory_cube_keeps_first_plane_blank() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_descendingfreqs.ms"),
            root.join("measurementset/vla/refim_point_descendingfreqs.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = descend_f14_cube_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        let result = run_cube(&CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: cube.channels,
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales,
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: 0,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode: config.w_term_mode,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        let blank_plane_sum_abs: f32 = result
            .residual
            .slice(s![.., .., 0, 0])
            .iter()
            .map(|value| value.abs())
            .sum();
        let populated_plane_sum_abs: f32 = result
            .residual
            .slice(s![.., .., 0, 1])
            .iter()
            .map(|value| value.abs())
            .sum();
        assert_eq!(
            blank_plane_sum_abs, 0.0,
            "expected blank first cube plane before persistence"
        );
        assert!(
            populated_plane_sum_abs > 0.0,
            "expected a populated second cube plane before persistence"
        );
    }

    #[test]
    fn descending_frequency_f14_persisted_cube_keeps_first_plane_blank() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_descendingfreqs.ms"),
            root.join("measurementset/vla/refim_point_descendingfreqs.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let tmp = tempdir().unwrap();
        let image_prefix = tmp.path().join("descend_f14_cube");
        let mut config = descend_f14_cube_config(ms_path);
        config.imagename = image_prefix.clone();
        let summary = run_from_config(&config).unwrap();
        assert!(summary.gridded_samples > 0);

        let residual =
            PagedImage::<f32>::open(format!("{}.residual", image_prefix.display())).unwrap();
        let slice = residual.get_slice(&[0, 0, 0, 0], residual.shape()).unwrap();
        assert_eq!(
            slice[IxDyn(&[50, 50, 0, 0])],
            0.0,
            "expected persisted channel 0 center to remain blank"
        );
        assert!(
            slice[IxDyn(&[50, 50, 0, 1])].abs() > 0.0,
            "expected persisted channel 1 center to remain populated"
        );
    }

    #[test]
    fn descending_frequency_f14_staged_copy_keeps_first_plane_blank() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_descendingfreqs.ms"),
            root.join("measurementset/vla/refim_point_descendingfreqs.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let tmp = tempdir().unwrap();
        let staged_ms_path = tmp.path().join("refim_point_descending.ms");
        let copy_status = std::process::Command::new("cp")
            .arg("-R")
            .arg(&ms_path)
            .arg(&staged_ms_path)
            .status()
            .unwrap();
        assert!(copy_status.success());

        let image_prefix = tmp.path().join("descend_f14_cube_staged");
        let mut config = descend_f14_cube_config(staged_ms_path);
        config.imagename = image_prefix.clone();
        let summary = run_from_config(&config).unwrap();
        assert!(summary.gridded_samples > 0);

        let residual =
            PagedImage::<f32>::open(format!("{}.residual", image_prefix.display())).unwrap();
        let slice = residual.get_slice(&[0, 0, 0, 0], residual.shape()).unwrap();
        assert_eq!(
            slice[IxDyn(&[50, 50, 0, 0])],
            0.0,
            "expected persisted staged-copy channel 0 center to remain blank"
        );
        assert!(
            slice[IxDyn(&[50, 50, 0, 1])].abs() > 0.0,
            "expected persisted staged-copy channel 1 center to remain populated"
        );
    }

    #[test]
    fn refim_point_cube11_prepared_cube_keeps_channel_four_populated() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = refim_point_cube11_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 10);
        let channel_frequencies_hz = cube
            .channels
            .iter()
            .map(|channel| channel.channel_frequency_hz)
            .collect::<Vec<_>>();
        let expected_frequencies_hz = vec![
            1.2e9, 1.15e9, 1.1e9, 1.05e9, 1.0e9, 0.95e9, 0.9e9, 0.85e9, 0.8e9, 0.75e9,
        ];
        assert!(
            channel_frequencies_hz
                .iter()
                .copied()
                .zip(expected_frequencies_hz.iter().copied())
                .all(|(actual_hz, expected_hz)| (actual_hz - expected_hz).abs() < 10.0),
            "expected cube11 output axis to follow CASA's descending TOPO defaults, got {:?}",
            channel_frequencies_hz
        );
        assert!(
            cube_channel_sample_count(&cube.channels[4]) > 0,
            "expected output plane 4 to remain populated for cube11; sample counts={:?}",
            cube.channels
                .iter()
                .map(cube_channel_sample_count)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            cube_channel_sample_count(&cube.channels[5]),
            0,
            "expected output plane 5 to clip beyond the low-frequency edge for cube11; sample counts={:?}",
            cube.channels
                .iter()
                .map(cube_channel_sample_count)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn refim_point_cube11_dirty_persisted_cube_keeps_channel_four_signal() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let tmp = tempdir().unwrap();
        let image_prefix = tmp.path().join("refim_point_cube11");
        let mut config = refim_point_cube11_config(ms_path);
        config.imagename = image_prefix.clone();
        let summary = run_from_config(&config).unwrap();
        assert!(summary.gridded_samples > 0);

        let residual =
            PagedImage::<f32>::open(format!("{}.residual", image_prefix.display())).unwrap();
        let slice = residual.get_slice(&[0, 0, 0, 0], residual.shape()).unwrap();
        assert!(
            slice[IxDyn(&[50, 50, 0, 4])].abs() > 1.0,
            "expected persisted dirty cube11 channel 4 center to retain source signal"
        );
        assert_eq!(
            slice[IxDyn(&[50, 50, 0, 5])],
            0.0,
            "expected persisted dirty cube11 channel 5 center to remain blank"
        );
    }

    #[test]
    fn refim_point_cube18_prepared_cube_keeps_descending_default_velocity_axis() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = refim_point_cube18_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 10);
        let channel_frequencies_hz = cube
            .channels
            .iter()
            .map(|channel| channel.channel_frequency_hz)
            .collect::<Vec<_>>();
        let expected_frequencies_hz = vec![
            1.45e9, 1.40e9, 1.35e9, 1.30e9, 1.25e9, 1.20e9, 1.15e9, 1.10e9, 1.05e9, 1.0e9,
        ];
        assert!(
            channel_frequencies_hz
                .iter()
                .copied()
                .zip(expected_frequencies_hz.iter().copied())
                .all(|(actual_hz, expected_hz)| (actual_hz - expected_hz).abs() < 100.0),
            "expected cube18 output axis to follow CASA's default velocity-width rule, got {:?}",
            channel_frequencies_hz
        );
        assert!(
            cube_channel_sample_count(&cube.channels[9]) > 0,
            "expected output plane 9 to remain populated for cube18; sample counts={:?}",
            cube.channels
                .iter()
                .map(cube_channel_sample_count)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn refim_point_cube20_prepared_cube_keeps_doppler_start_on_channel_four() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = refim_point_cube20_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 10);
        let channel_frequencies_hz = cube
            .channels
            .iter()
            .map(|channel| channel.channel_frequency_hz)
            .collect::<Vec<_>>();
        let sample_counts = cube
            .channels
            .iter()
            .map(cube_channel_sample_count)
            .collect::<Vec<_>>();
        assert!(
            (channel_frequencies_hz[0] - 1.199_989_152e9).abs() < 5.0e4,
            "expected cube20 channel 0 frequency near CASA's doppler-derived start, got {:?}",
            channel_frequencies_hz
        );
        assert!(
            sample_counts[4] > 0,
            "expected cube20 output plane 4 to remain populated; channel frequencies={:?}, sample counts={:?}",
            channel_frequencies_hz,
            sample_counts
        );
    }

    #[test]
    fn refim_point_cube13_prepared_cube_uses_casa_optical_velocity_axis() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = refim_point_cube13_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 8);
        let channel_frequencies_hz = cube
            .channels
            .iter()
            .map(|channel| channel.channel_frequency_hz)
            .collect::<Vec<_>>();
        let expected_frequencies_hz = [
            1.253_244_052_817_556_9e9,
            1.176_783_981_410_044_4e9,
            1.109_124_277_594_536_8e9,
            1.048_827_218_620_406_3e9,
            9.885_301_596_462_758e8,
            9.282_331_006_721_452e8,
            8.679_360_416_980_147e8,
            8.076_389_827_238_842e8,
        ];
        assert!(
            channel_frequencies_hz
                .iter()
                .copied()
                .zip(expected_frequencies_hz.iter().copied())
                .all(|(actual_hz, expected_hz)| (actual_hz - expected_hz).abs() < 5.0e4),
            "expected cube13 output axis to follow CASA's nonlinear optical-velocity grid, got {:?}",
            channel_frequencies_hz
        );
        let deltas = channel_frequencies_hz
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect::<Vec<_>>();
        assert!(
            deltas
                .windows(2)
                .any(|pair| (pair[1] - pair[0]).abs() > 1.0e5),
            "expected cube13 output axis to be nonlinear in frequency, got deltas {:?}",
            deltas
        );
    }

    #[test]
    fn refim_point_cube13_clean_persisted_cube_keeps_all_eight_planes() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let tmp = tempdir().unwrap();
        let image_prefix = tmp.path().join("refim_point_cube13_clean");
        let mut config = refim_point_cube13_config(ms_path);
        config.imagename = image_prefix.clone();
        config.dirty_only = false;
        config.niter = 10;
        let summary = run_from_config(&config).unwrap();
        assert!(summary.gridded_samples > 0);

        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[100, 100, 1, 8]);
        let residual =
            PagedImage::<f32>::open(format!("{}.residual", image_prefix.display())).unwrap();
        assert_eq!(residual.shape(), &[100, 100, 1, 8]);
    }

    #[test]
    fn refim_point_cube13_clean_in_memory_cube_keeps_all_eight_planes() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(ms_path.clone()).unwrap();
        let config = refim_point_cube13_config(ms_path);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 8);

        let result = run_cube(&CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: cube.channels,
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales,
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: 10,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode: config.w_term_mode,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .unwrap();
        assert_eq!(result.image.shape(), &[100, 100, 1, 8]);
        assert_eq!(result.residual.shape(), &[100, 100, 1, 8]);
    }

    #[test]
    fn refim_point_cube13_clean_staged_copy_keeps_all_eight_planes() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let tmp = tempdir().unwrap();
        let staged_ms_path = tmp.path().join("refim_point.ms");
        let copy_status = std::process::Command::new("cp")
            .arg("-R")
            .arg(&ms_path)
            .arg(&staged_ms_path)
            .status()
            .unwrap();
        assert!(copy_status.success());

        let image_prefix = tmp.path().join("refim_point_cube13_clean_staged");
        let mut config = refim_point_cube13_config(staged_ms_path);
        config.imagename = image_prefix.clone();
        config.dirty_only = false;
        config.niter = 10;
        let summary = run_from_config(&config).unwrap();
        assert!(summary.gridded_samples > 0);

        let image = PagedImage::<f32>::open(format!("{}.image", image_prefix.display())).unwrap();
        assert_eq!(image.shape(), &[100, 100, 1, 8]);
        let residual =
            PagedImage::<f32>::open(format!("{}.residual", image_prefix.display())).unwrap();
        assert_eq!(residual.shape(), &[100, 100, 1, 8]);
    }

    #[test]
    #[ignore = "diagnostic for cube dirty 2d direct-vs-gridded parity on refim_point_withline"]
    fn refim_point_withline_cube_dirty_direct_matches_gridded_channels_five_and_seven() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let mut config = refim_point_withline_default_cube_config(ms_path);
        config.channel_start = Some(5);
        config.channel_count = Some(3);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 3);

        let mut channels = cube.channels;
        for channel in &mut channels {
            for batch in &mut channel.visibility_batches {
                for w_lambda in &mut batch.w_lambda {
                    *w_lambda = 0.0;
                }
            }
        }

        let make_request = |w_term_mode| CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: channels.clone(),
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: 0,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        let gridded = run_cube(&make_request(WTermMode::None)).unwrap();
        let direct = run_cube(&make_request(WTermMode::Direct)).unwrap();
        for &(channel_index, dataset_channel) in &[(0usize, 5usize), (2usize, 7usize)] {
            let gridded_plane = gridded.residual.slice(s![.., .., 0, channel_index]);
            let direct_plane = direct.residual.slice(s![.., .., 0, channel_index]);
            let mut sum_sq = 0.0f64;
            let mut max_abs = 0.0f32;
            let mut peak_gridded = 0.0f32;
            let mut peak_direct = 0.0f32;
            for (&lhs, &rhs) in gridded_plane.iter().zip(direct_plane.iter()) {
                let delta = lhs - rhs;
                sum_sq += f64::from(delta) * f64::from(delta);
                max_abs = max_abs.max(delta.abs());
                peak_gridded = peak_gridded.max(lhs.abs());
                peak_direct = peak_direct.max(rhs.abs());
            }
            let rms = (sum_sq / gridded_plane.len() as f64).sqrt() as f32;
            eprintln!(
                "refim_point_withline dirty 2d direct-vs-gridded dataset_channel={dataset_channel} local_channel={channel_index}: rms_diff={rms:.9e} max_abs_diff={max_abs:.9e} peak_gridded={peak_gridded:.9e} peak_direct={peak_direct:.9e}"
            );
        }
    }

    #[test]
    #[ignore = "diagnostic for cube dirty casacore-vs-rust gridded parity on refim_point_withline"]
    fn refim_point_withline_cube_dirty_casacore_matches_rust_channels_five_and_seven() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let mut config = refim_point_withline_default_cube_config(ms_path);
        config.channel_start = Some(5);
        config.channel_count = Some(3);
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        assert_eq!(cube.channels.len(), 3);

        let request = CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: cube.channels.clone(),
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: 0,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let rust = run_cube(&request).unwrap();

        let grid_shape = [
            diagnostic_padded_len(config.imsize, 1.2),
            diagnostic_padded_len(config.imsize, 1.2),
        ];
        let scale = [
            grid_shape[0] as f64 * request.geometry.cell_size_rad[0],
            grid_shape[1] as f64 * request.geometry.cell_size_rad[1],
        ];
        let offset = [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0];

        for &(channel_index, dataset_channel) in &[(0usize, 5usize), (2usize, 7usize)] {
            let channel = &request.channels[channel_index];
            let mut u_lambda = Vec::new();
            let mut v_lambda = Vec::new();
            let mut visibility_re = Vec::new();
            let mut visibility_im = Vec::new();
            let mut weight = Vec::new();
            let mut gridable = Vec::new();
            for batch in &channel.visibility_batches {
                u_lambda.extend_from_slice(&batch.u_lambda);
                v_lambda.extend_from_slice(&batch.v_lambda);
                visibility_re.extend(batch.visibility.iter().map(|value| value.re));
                visibility_im.extend(batch.visibility.iter().map(|value| value.im));
                weight.extend_from_slice(&batch.weight);
                gridable.extend_from_slice(&batch.gridable);
            }
            let Ok(cpp) = cpp_convolve_gridder_make_dirty_image_2d(
                grid_shape,
                request.geometry.image_shape,
                scale,
                offset,
                &u_lambda,
                &v_lambda,
                &visibility_re,
                &visibility_im,
                &weight,
                &gridable,
            ) else {
                return;
            };
            let rust_plane = rust.residual.slice(s![.., .., 0, channel_index]);
            let mut sum_sq = 0.0f64;
            let mut max_abs = 0.0f32;
            let mut peak_rust = 0.0f32;
            let mut peak_cpp = 0.0f32;
            for (rust_value, cpp_value) in rust_plane.iter().zip(cpp.pixels.iter()) {
                let delta = *rust_value - *cpp_value;
                sum_sq += f64::from(delta) * f64::from(delta);
                max_abs = max_abs.max(delta.abs());
                peak_rust = peak_rust.max(rust_value.abs());
                peak_cpp = peak_cpp.max(cpp_value.abs());
            }
            let rms = (sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
            eprintln!(
                "refim_point_withline dirty casacore-vs-rust dataset_channel={dataset_channel} local_channel={channel_index}: rms_diff={rms:.9e} max_abs_diff={max_abs:.9e} peak_rust={peak_rust:.9e} peak_cpp={peak_cpp:.9e}"
            );
            assert!(
                rms <= 2.0e-6,
                "dirty casacore-vs-rust RMS too large for dataset_channel={dataset_channel}: {rms}"
            );
            assert!(
                max_abs <= 3.0e-5,
                "dirty casacore-vs-rust max abs too large for dataset_channel={dataset_channel}: {max_abs}"
            );
            assert!(
                (peak_rust - peak_cpp).abs() <= 3.0e-5,
                "dirty casacore-vs-rust peak mismatch too large for dataset_channel={dataset_channel}: rust={peak_rust} cpp={peak_cpp}"
            );
        }
    }

    #[test]
    #[ignore = "diagnostic for captured late-block hclean parity on refim_point_withline cube nsigma case"]
    fn refim_point_withline_cube_nsigma_captured_late_blocks_match_hclean() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let mut config = refim_point_withline_default_cube_config(ms_path);
        config.dirty_only = false;
        config.niter = 1_000_000;
        config.gain = 0.5;
        config.threshold_jy = 0.000001;
        config.nsigma = 10.0;
        config.minor_cycle_length = 10;
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };

        let make_request = || CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: cube.channels.clone(),
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: config.niter,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        for &(channel_index, block_index) in &[(7usize, 8usize), (5usize, 9usize), (9usize, 9usize)]
        {
            let tmp = tempdir().unwrap();
            let capture_dir = tmp.path().join(format!(
                "capture-channel-{channel_index}-block-{block_index}"
            ));
            unsafe {
                env::set_var("CASA_RS_CUBE_CAPTURE_CHANNEL", channel_index.to_string());
                env::set_var("CASA_RS_CUBE_CAPTURE_BLOCK", block_index.to_string());
                env::set_var("CASA_RS_CUBE_CAPTURE_DIR", &capture_dir);
            }
            let _ = run_cube(&make_request()).unwrap();
            unsafe {
                env::remove_var("CASA_RS_CUBE_CAPTURE_CHANNEL");
                env::remove_var("CASA_RS_CUBE_CAPTURE_BLOCK");
                env::remove_var("CASA_RS_CUBE_CAPTURE_DIR");
            }

            let captured = read_captured_cube_minor_cycle(&capture_dir);
            let combined_threshold = captured
                .cycle_threshold_jy_per_beam
                .max(captured.absolute_threshold_jy_per_beam)
                .max(captured.nsigma_threshold_jy_per_beam);
            let rust = replay_rust_hogbom_minor_cycle_2d(
                &captured.psf,
                &captured.residual,
                [captured.nx, captured.ny],
                captured.gain,
                combined_threshold,
                captured.cycle_reported_niter,
            );
            let cpp = match cpp_hogbom_clean_minor_cycle_2d(
                &captured.psf,
                &captured.residual,
                [captured.nx, captured.ny],
                captured.gain,
                combined_threshold,
                captured.cycle_reported_niter,
            ) {
                Ok(result) => result,
                Err(error) if error == "casacore C++ backend unavailable" => return,
                Err(error) => panic!("run captured hclean interop: {error}"),
            };

            eprintln!(
                "captured cube block channel={channel_index} block={block_index}: combined_threshold={combined_threshold:.9e} cycle_threshold={:.9e} abs_threshold={:.9e} nsigma_threshold={:.9e} rust_iterdone={} cpp_iterdone={}",
                captured.cycle_threshold_jy_per_beam,
                captured.absolute_threshold_jy_per_beam,
                captured.nsigma_threshold_jy_per_beam,
                rust.iterdone,
                cpp.iterdone,
            );
            assert_eq!(
                rust.iterdone, cpp.iterdone,
                "captured hclean iterdone mismatch for channel={channel_index} block={block_index}"
            );
            for (&rust_value, &cpp_value) in rust.residual.iter().zip(&cpp.residual) {
                assert!(
                    (rust_value - cpp_value).abs() < 1.0e-6,
                    "captured hclean residual mismatch for channel={channel_index} block={block_index}: rust={rust_value} cpp={cpp_value}"
                );
            }

            let request = make_request();
            let channel = &request.channels[channel_index];
            let mut u_lambda = Vec::new();
            let mut v_lambda = Vec::new();
            let mut visibility_re = Vec::new();
            let mut visibility_im = Vec::new();
            let mut weight = Vec::new();
            let mut gridable = Vec::new();
            for batch in &channel.visibility_batches {
                u_lambda.extend_from_slice(&batch.u_lambda);
                v_lambda.extend_from_slice(&batch.v_lambda);
                visibility_re.extend(batch.visibility.iter().map(|value| value.re));
                visibility_im.extend(batch.visibility.iter().map(|value| value.im));
                weight.extend_from_slice(&batch.weight);
                gridable.extend_from_slice(&batch.gridable);
            }
            let grid_shape = [
                diagnostic_padded_len(config.imsize, 1.2),
                diagnostic_padded_len(config.imsize, 1.2),
            ];
            let scale = [
                grid_shape[0] as f64 * request.geometry.cell_size_rad[0],
                grid_shape[1] as f64 * request.geometry.cell_size_rad[1],
            ];
            let offset = [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0];
            let cpp_residual = match cpp_convolve_gridder_make_model_residual_image_2d(
                grid_shape,
                [captured.nx, captured.ny],
                scale,
                offset,
                &u_lambda,
                &v_lambda,
                &visibility_re,
                &visibility_im,
                &weight,
                &gridable,
                &captured.model,
            ) {
                Ok(result) => result,
                Err(error) if error == "casacore C++ backend unavailable" => return,
                Err(error) => panic!("run captured model-residual interop: {error}"),
            };
            let mut model_planes = (0..request.channels.len())
                .map(|_| Array2::<f32>::zeros((captured.nx, captured.ny)))
                .collect::<Vec<_>>();
            model_planes[channel_index] =
                Array2::from_shape_vec((captured.nx, captured.ny), captured.model.clone())
                    .expect("captured model plane shape");
            let refresh_trace =
                trace_cube_channel_residual_refresh(&request, channel_index, &model_planes)
                    .expect("trace captured residual refresh");
            let mut residual_sum_sq = 0.0f64;
            let mut residual_max_abs = 0.0f32;
            let mut residual_peak_rust = 0.0f32;
            let mut residual_peak_cpp = 0.0f32;
            for (&rust_value, &cpp_value) in captured.residual.iter().zip(&cpp_residual.pixels) {
                let delta = rust_value - cpp_value;
                residual_sum_sq += f64::from(delta) * f64::from(delta);
                residual_max_abs = residual_max_abs.max(delta.abs());
                residual_peak_rust = residual_peak_rust.max(rust_value.abs());
                residual_peak_cpp = residual_peak_cpp.max(cpp_value.abs());
            }
            let residual_rms = (residual_sum_sq / captured.residual.len() as f64).sqrt() as f32;
            let mut scaled_sum_sq = 0.0f64;
            let mut scaled_max_abs = 0.0f32;
            let mut scaled_peak_cpp = 0.0f32;
            for (&rust_value, &cpp_value) in captured.residual.iter().zip(&cpp_residual.pixels) {
                let scaled_cpp_value = cpp_value / refresh_trace.psf_peak;
                let delta = rust_value - scaled_cpp_value;
                scaled_sum_sq += f64::from(delta) * f64::from(delta);
                scaled_max_abs = scaled_max_abs.max(delta.abs());
                scaled_peak_cpp = scaled_peak_cpp.max(scaled_cpp_value.abs());
            }
            let scaled_rms = (scaled_sum_sq / captured.residual.len() as f64).sqrt() as f32;
            eprintln!(
                "captured residual refresh channel={channel_index} block={block_index}: rms_diff={residual_rms:.9e} max_abs_diff={residual_max_abs:.9e} peak_rust={residual_peak_rust:.9e} peak_cpp={residual_peak_cpp:.9e} psf_peak={:.9e} scaled_rms_diff={scaled_rms:.9e} scaled_max_abs_diff={scaled_max_abs:.9e} scaled_peak_cpp={scaled_peak_cpp:.9e}",
                refresh_trace.psf_peak,
            );
        }
    }

    #[test]
    #[ignore = "diagnostic for refim_point_withline cube per-sample prediction parity against casacore"]
    fn refim_point_withline_cube_prediction_matches_casacore_on_selected_samples() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let mut config = refim_point_withline_default_cube_config(ms_path);
        config.dirty_only = false;
        config.niter = 1_000_000;
        config.gain = 0.5;
        config.threshold_jy = 0.000001;
        config.nsigma = 10.0;
        config.minor_cycle_length = 10;
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        let request = CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: cube.channels.clone(),
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: config.niter,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        let temp = tempdir().unwrap();
        let rust_prefix = temp.path().join("rust-refim-point-withline-predict-parity");
        config.imagename = rust_prefix.clone();
        run_from_config(&config).expect("run rust cube nsigma case for prediction parity");

        let rust_model = PagedImage::<f32>::open(format!("{}.model", rust_prefix.display()))
            .expect("open rust model cube");
        let rust_model_cube = rust_model
            .get_slice(&[0, 0, 0, 0], rust_model.shape())
            .expect("read rust model cube");
        let model_planes = (0..request.channels.len())
            .map(|channel_index| {
                let mut plane = Array2::<f32>::zeros((
                    request.geometry.image_shape[0],
                    request.geometry.image_shape[1],
                ));
                for x in 0..request.geometry.image_shape[0] {
                    for y in 0..request.geometry.image_shape[1] {
                        plane[(x, y)] = rust_model_cube[IxDyn(&[x, y, 0, channel_index])];
                    }
                }
                plane
            })
            .collect::<Vec<_>>();
        let channel_index = 9usize;
        let trace = trace_cube_channel_residual_refresh(&request, channel_index, &model_planes)
            .expect("trace cube residual refresh");
        let channel = &request.channels[channel_index];
        let grid_shape = [
            diagnostic_padded_len(config.imsize, 1.2),
            diagnostic_padded_len(config.imsize, 1.2),
        ];
        let scale = [
            grid_shape[0] as f64 * request.geometry.cell_size_rad[0],
            grid_shape[1] as f64 * request.geometry.cell_size_rad[1],
        ];
        let offset = [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0];

        let sample_count = trace.samples.len();
        let stride = (sample_count / 32).max(1);
        let mut checked = 0usize;
        let mut sum_sq = 0.0f64;
        let mut max_abs = 0.0f32;
        for sample in trace.samples.iter().step_by(stride) {
            let contributions = &channel.model_interpolation_batches[sample.batch_index]
                .sample_contributions[sample.sample_index];
            let mut effective_model = Array2::<f32>::zeros((
                request.geometry.image_shape[0],
                request.geometry.image_shape[1],
            ));
            for contribution in contributions {
                if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
                    continue;
                }
                let plane = &model_planes[contribution.model_channel_index];
                for ((x, y), value) in plane.indexed_iter() {
                    effective_model[(x, y)] += *value * contribution.factor;
                }
            }
            let cpp = match cpp_convolve_gridder_predict_visibility_2d(
                grid_shape,
                request.geometry.image_shape,
                scale,
                offset,
                [sample.u_lambda, -sample.v_lambda],
                effective_model.as_slice().unwrap(),
            ) {
                Ok(value) => value,
                Err(error) if error == "casacore C++ backend unavailable" => return,
                Err(error) => panic!("run casacore predict-visibility shim: {error}"),
            };
            let delta_re = sample.predicted_visibility.re - cpp.re;
            let delta_im = sample.predicted_visibility.im - cpp.im;
            sum_sq += f64::from(delta_re) * f64::from(delta_re)
                + f64::from(delta_im) * f64::from(delta_im);
            max_abs = max_abs.max(delta_re.abs().max(delta_im.abs()));
            checked += 1;
        }
        let rms = (sum_sq / (2 * checked).max(1) as f64).sqrt() as f32;
        eprintln!(
            "refim_point_withline channel {channel_index} prediction casacore-vs-rust: checked={checked} stride={stride} rms_diff={rms:.9e} max_abs_diff={max_abs:.9e}"
        );
    }

    #[test]
    #[ignore = "diagnostic for refim_point_withline cube residual-image parity against casacore from rust residual visibilities"]
    fn refim_point_withline_cube_residual_image_matches_casacore_from_rust_residual_visibilities() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let mut config = refim_point_withline_default_cube_config(ms_path);
        config.dirty_only = false;
        config.niter = 1_000_000;
        config.gain = 0.5;
        config.threshold_jy = 0.000001;
        config.nsigma = 10.0;
        config.minor_cycle_length = 10;
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        let request = CubeImagingRequest {
            geometry: ImageGeometry {
                image_shape: [config.imsize, config.imsize],
                cell_size_rad: [
                    config.cell_arcsec * arcsec_to_rad(),
                    config.cell_arcsec * arcsec_to_rad(),
                ],
            },
            channels: cube.channels.clone(),
            plane_stokes: cube.plane_stokes,
            weighting: config.weighting,
            weight_density_mode: if config.per_channel_weight_density {
                WeightDensityMode::PerPlane
            } else {
                WeightDensityMode::Combined
            },
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean: CleanConfig {
                niter: config.niter,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
            },
            clean_mask: None,
            psf_cutoff: config.psf_cutoff,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };

        let temp = tempdir().unwrap();
        let rust_prefix = temp
            .path()
            .join("rust-refim-point-withline-residual-parity");
        config.imagename = rust_prefix.clone();
        run_from_config(&config).expect("run rust cube nsigma case for residual parity");

        let rust_model = PagedImage::<f32>::open(format!("{}.model", rust_prefix.display()))
            .expect("open rust model cube");
        let rust_model_cube = rust_model
            .get_slice(&[0, 0, 0, 0], rust_model.shape())
            .expect("read rust model cube");
        let model_planes = (0..request.channels.len())
            .map(|channel_index| {
                let mut plane = Array2::<f32>::zeros((
                    request.geometry.image_shape[0],
                    request.geometry.image_shape[1],
                ));
                for x in 0..request.geometry.image_shape[0] {
                    for y in 0..request.geometry.image_shape[1] {
                        plane[(x, y)] = rust_model_cube[IxDyn(&[x, y, 0, channel_index])];
                    }
                }
                plane
            })
            .collect::<Vec<_>>();
        let channel_index = 9usize;
        let trace = trace_cube_channel_residual_refresh(&request, channel_index, &model_planes)
            .expect("trace cube residual refresh");
        let grid_shape = [
            diagnostic_padded_len(config.imsize, 1.2),
            diagnostic_padded_len(config.imsize, 1.2),
        ];
        let scale = [
            grid_shape[0] as f64 * request.geometry.cell_size_rad[0],
            grid_shape[1] as f64 * request.geometry.cell_size_rad[1],
        ];
        let offset = [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0];
        let u_lambda = trace
            .samples
            .iter()
            .map(|sample| sample.u_lambda)
            .collect::<Vec<_>>();
        let v_lambda = trace
            .samples
            .iter()
            .map(|sample| sample.v_lambda)
            .collect::<Vec<_>>();
        let residual_re = trace
            .samples
            .iter()
            .map(|sample| sample.residual_visibility.re)
            .collect::<Vec<_>>();
        let residual_im = trace
            .samples
            .iter()
            .map(|sample| sample.residual_visibility.im)
            .collect::<Vec<_>>();
        let weight = trace
            .samples
            .iter()
            .map(|sample| sample.weight)
            .collect::<Vec<_>>();
        let gridable = trace
            .samples
            .iter()
            .map(|sample| sample.gridable)
            .collect::<Vec<_>>();
        let cpp = match cpp_convolve_gridder_make_dirty_image_2d(
            grid_shape,
            request.geometry.image_shape,
            scale,
            offset,
            &u_lambda,
            &v_lambda,
            &residual_re,
            &residual_im,
            &weight,
            &gridable,
        ) {
            Ok(value) => value,
            Err(error) if error == "casacore C++ backend unavailable" => return,
            Err(error) => {
                panic!("run casacore dirty-image shim from rust residual visibilities: {error}")
            }
        };
        let mut sum_sq = 0.0f64;
        let mut max_abs = 0.0f32;
        let mut peak_rust = 0.0f32;
        let mut peak_cpp = 0.0f32;
        for (&rust_value, &cpp_value) in trace.residual_image.iter().zip(cpp.pixels.iter()) {
            let delta = rust_value - cpp_value;
            sum_sq += f64::from(delta) * f64::from(delta);
            max_abs = max_abs.max(delta.abs());
            peak_rust = peak_rust.max(rust_value.abs());
            peak_cpp = peak_cpp.max(cpp_value.abs());
        }
        let rms = (sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
        eprintln!(
            "refim_point_withline channel {channel_index} residual-image casacore-vs-rust from rust residual visibilities: rms_diff={rms:.9e} max_abs_diff={max_abs:.9e} peak_rust={peak_rust:.9e} peak_cpp={peak_cpp:.9e}"
        );
    }

    #[test]
    #[ignore = "diagnostic for refim_point_withline channel-9 model interpolation profile"]
    fn refim_point_withline_channel_nine_model_interpolation_profile() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let mut config = refim_point_withline_default_cube_config(ms_path);
        config.dirty_only = false;
        config.niter = 1_000_000;
        config.gain = 0.5;
        config.threshold_jy = 0.000001;
        config.nsigma = 10.0;
        config.minor_cycle_length = 10;
        let prepared = prepare_plane_input(&ms, &config, VisibilityDataColumn::Data).unwrap();
        let PreparedInput::Cube(cube) = prepared else {
            panic!("expected cube prepared input");
        };
        let channel_index = 9usize;
        let channel = &cube.channels[channel_index];
        let mut identity = 0usize;
        let mut mixed = 0usize;
        let mut empty = 0usize;
        let mut mixed_pairs = std::collections::BTreeMap::<(usize, usize), usize>::new();
        let mut upper_factor_min = f32::INFINITY;
        let mut upper_factor_max = f32::NEG_INFINITY;
        for batch in &channel.model_interpolation_batches {
            for contributions in &batch.sample_contributions {
                match contributions.as_slice() {
                    [] => empty += 1,
                    [only] => {
                        identity += 1;
                        *mixed_pairs
                            .entry((only.model_channel_index, only.model_channel_index))
                            .or_default() += 1;
                    }
                    [first, second] => {
                        mixed += 1;
                        *mixed_pairs
                            .entry((first.model_channel_index, second.model_channel_index))
                            .or_default() += 1;
                        upper_factor_min = upper_factor_min.min(second.factor);
                        upper_factor_max = upper_factor_max.max(second.factor);
                    }
                    other => panic!("unexpected contribution shape: {other:?}"),
                }
            }
        }
        eprintln!(
            "refim_point_withline channel 9 model interpolation profile: identity={identity} mixed={mixed} empty={empty} upper_factor_range=({upper_factor_min}, {upper_factor_max}) pairs={mixed_pairs:?}"
        );
    }

    #[test]
    #[ignore = "diagnostic for refim_point_withline channel-9 output interpolation profile"]
    fn refim_point_withline_channel_nine_output_interpolation_profile() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point_withline.ms"),
            root.join("measurementset/vla/refim_point_withline.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let config = refim_point_withline_default_cube_config(ms_path);
        let data_description = ms.data_description().unwrap();
        let ddid_info = data_description_index(&data_description).unwrap();
        let spectral_window = ms.spectral_window().unwrap();
        let polarization = ms.polarization().unwrap();
        let engine = MsCalEngine::new(&ms).unwrap();
        let time_column = TimeColumn::new(ms.main_table());
        let field_column = ms.main_table().get_column("FIELD_ID").unwrap();
        let ddid_column = ms.main_table().get_column("DATA_DESC_ID").unwrap();
        let mut reference_time = None::<f64>;
        let mut bounds = None::<[f64; 2]>;
        let mut selected_rows = Vec::new();
        for (field_cell, ddid_cell) in field_column.zip(ddid_column) {
            let row = field_cell.row_index;
            let field_id = match field_cell.value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                _ => continue,
            };
            let ddid = match ddid_cell.value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                _ => continue,
            };
            if field_id != 0 || ddid != 0 {
                continue;
            }
            let row_time = time_column.get_mjd_seconds(row).unwrap();
            selected_rows.push(row_time);
            reference_time.get_or_insert(row_time);
            match &mut bounds {
                Some(existing) => {
                    existing[0] = existing[0].min(row_time);
                    existing[1] = existing[1].max(row_time);
                }
                None => bounds = Some([row_time, row_time]),
            }
        }
        let prepared = PreparedSelection::new(
            &config,
            0,
            &ddid_info,
            &spectral_window,
            &polarization,
            PhaseCenter {
                field_id: Some(0),
                angles_rad: [0.0, 0.0],
                reference: DirectionRef::J2000,
            },
            Some(CubeSetupContext {
                phase_center_field_id: 0,
                reference_row_time_mjd_sec: reference_time.unwrap(),
                time_bounds_mjd_sec: bounds.unwrap(),
                derived_engine: &engine,
            }),
        );
        assert!(
            prepared.initialization_error.is_none(),
            "prepared selection init error: {:?}",
            prepared.initialization_error
        );
        let cube_setup = prepared.cube_spectral_setup.as_ref().unwrap();
        let channel_index = 9usize;
        let mut empty = 0usize;
        let mut single = 0usize;
        let mut mixed = 0usize;
        let mut lower_factor_min = f32::INFINITY;
        let mut lower_factor_max = f32::NEG_INFINITY;
        let mut upper_factor_min = f32::INFINITY;
        let mut upper_factor_max = f32::NEG_INFINITY;
        let mut source8_minus_output_min = f64::INFINITY;
        let mut source8_minus_output_max = f64::NEG_INFINITY;
        let mut source9_minus_output_min = f64::INFINITY;
        let mut source9_minus_output_max = f64::NEG_INFINITY;
        let mut source10_minus_output_min = f64::INFINITY;
        let mut source10_minus_output_max = f64::NEG_INFINITY;
        for row_time_mjd_sec in &selected_rows {
            let frame = engine
                .spectral_frame_observatory(*row_time_mjd_sec, 0)
                .unwrap();
            let source8_output_hz = MFrequency::new(
                prepared.source_channel_frequencies_hz[8],
                cube_setup.source_freq_ref,
            )
            .convert_to(cube_setup.output_freq_ref, &frame)
            .unwrap()
            .hz();
            source8_minus_output_min = source8_minus_output_min
                .min(source8_output_hz - cube_setup.output_channel_frequencies_hz[channel_index]);
            source8_minus_output_max = source8_minus_output_max
                .max(source8_output_hz - cube_setup.output_channel_frequencies_hz[channel_index]);

            let source9_output_hz = MFrequency::new(
                prepared.source_channel_frequencies_hz[9],
                cube_setup.source_freq_ref,
            )
            .convert_to(cube_setup.output_freq_ref, &frame)
            .unwrap()
            .hz();
            source9_minus_output_min = source9_minus_output_min
                .min(source9_output_hz - cube_setup.output_channel_frequencies_hz[channel_index]);
            source9_minus_output_max = source9_minus_output_max
                .max(source9_output_hz - cube_setup.output_channel_frequencies_hz[channel_index]);

            let source10_output_hz = MFrequency::new(
                prepared.source_channel_frequencies_hz[10],
                cube_setup.source_freq_ref,
            )
            .convert_to(cube_setup.output_freq_ref, &frame)
            .unwrap()
            .hz();
            source10_minus_output_min = source10_minus_output_min
                .min(source10_output_hz - cube_setup.output_channel_frequencies_hz[channel_index]);
            source10_minus_output_max = source10_minus_output_max
                .max(source10_output_hz - cube_setup.output_channel_frequencies_hz[channel_index]);
            let contributions = cube_setup
                .row_output_channel_contributions_batch(
                    &prepared.source_channel_frequencies_hz,
                    &prepared.source_channel_widths_hz,
                    *row_time_mjd_sec,
                    0,
                    &engine,
                )
                .unwrap();
            match contributions[channel_index].as_slice() {
                [] => empty += 1,
                [only] => {
                    single += 1;
                    assert_eq!(only.source_channel, channel_index);
                }
                [first, second] => {
                    mixed += 1;
                    lower_factor_min = lower_factor_min.min(first.factor);
                    lower_factor_max = lower_factor_max.max(first.factor);
                    upper_factor_min = upper_factor_min.min(second.factor);
                    upper_factor_max = upper_factor_max.max(second.factor);
                }
                [first, middle, third] => {
                    mixed += 1;
                    assert_eq!(middle.source_channel, channel_index);
                    lower_factor_min = lower_factor_min.min(first.factor);
                    lower_factor_max = lower_factor_max.max(first.factor);
                    upper_factor_min = upper_factor_min.min(third.factor);
                    upper_factor_max = upper_factor_max.max(third.factor);
                }
                other => panic!("unexpected channel-9 contribution shape: {other:?}"),
            }
        }
        eprintln!(
            "refim_point_withline channel 9 output interpolation profile: rows={} empty={} single={} mixed={} lower_factor_range=({lower_factor_min}, {lower_factor_max}) upper_factor_range=({upper_factor_min}, {upper_factor_max}) output9={} source8-output-range=({source8_minus_output_min}, {source8_minus_output_max}) source9-output-range=({source9_minus_output_min}, {source9_minus_output_max}) source10-output-range=({source10_minus_output_min}, {source10_minus_output_max})",
            selected_rows.len(),
            empty,
            single,
            mixed,
            cube_setup.output_channel_frequencies_hz[channel_index]
        );
    }

    #[test]
    #[ignore = "diagnostic for refim_point default cube channel-0 interpolation profile"]
    fn refim_point_default_cube_channel_zero_interpolation_profile() {
        let Some(root) = env::var_os("CASA_RS_TESTDATA_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let candidates = [
            root.join("unittest/tclean/refim_point.ms"),
            root.join("measurementset/vla/refim_point.ms"),
        ];
        let Some(ms_path) = candidates.into_iter().find(|path| path.exists()) else {
            return;
        };

        let ms = MeasurementSet::open(&ms_path).unwrap();
        let config = refim_point_default_cube_config(ms_path);
        let data_description = ms.data_description().unwrap();
        let ddid_info = data_description_index(&data_description).unwrap();
        let spectral_window = ms.spectral_window().unwrap();
        let polarization = ms.polarization().unwrap();
        let engine = MsCalEngine::new(&ms).unwrap();
        let time_column = TimeColumn::new(ms.main_table());
        let field_column = ms.main_table().get_column("FIELD_ID").unwrap();
        let ddid_column = ms.main_table().get_column("DATA_DESC_ID").unwrap();
        let mut reference_time = None::<f64>;
        let mut bounds = None::<[f64; 2]>;
        let mut selected_rows = Vec::new();
        for (field_cell, ddid_cell) in field_column.zip(ddid_column) {
            let row = field_cell.row_index;
            let field_id = match field_cell.value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                _ => continue,
            };
            let ddid = match ddid_cell.value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                _ => continue,
            };
            if field_id != 0 || ddid != 0 {
                continue;
            }
            let row_time = time_column.get_mjd_seconds(row).unwrap();
            selected_rows.push(row_time);
            reference_time.get_or_insert(row_time);
            match &mut bounds {
                Some(existing) => {
                    existing[0] = existing[0].min(row_time);
                    existing[1] = existing[1].max(row_time);
                }
                None => bounds = Some([row_time, row_time]),
            }
        }
        let prepared = PreparedSelection::new(
            &config,
            0,
            &ddid_info,
            &spectral_window,
            &polarization,
            PhaseCenter {
                field_id: Some(0),
                angles_rad: [0.0, 0.0],
                reference: DirectionRef::J2000,
            },
            Some(CubeSetupContext {
                phase_center_field_id: 0,
                reference_row_time_mjd_sec: reference_time.unwrap(),
                time_bounds_mjd_sec: bounds.unwrap(),
                derived_engine: &engine,
            }),
        );
        assert!(
            prepared.initialization_error.is_none(),
            "prepared selection init error: {:?}",
            prepared.initialization_error
        );
        let cube_setup = prepared.cube_spectral_setup.as_ref().unwrap();
        let row_count = selected_rows.len();
        let mut channel0_empty = 0usize;
        let mut channel0_single = 0usize;
        let mut channel0_mixed = 0usize;
        let mut channel0_upper_factor_min = f32::INFINITY;
        let mut channel0_upper_factor_max = f32::NEG_INFINITY;
        let mut source0_minus_output_min = f64::INFINITY;
        let mut source0_minus_output_max = f64::NEG_INFINITY;
        let mut source1_minus_output_min = f64::INFINITY;
        let mut source1_minus_output_max = f64::NEG_INFINITY;
        for row_time_mjd_sec in selected_rows {
            let frame = engine
                .spectral_frame_observatory(row_time_mjd_sec, 0)
                .unwrap();
            let source0_output_hz = MFrequency::new(
                prepared.source_channel_frequencies_hz[0],
                cube_setup.source_freq_ref,
            )
            .convert_to(cube_setup.output_freq_ref, &frame)
            .unwrap()
            .hz();
            let source1_output_hz = MFrequency::new(
                prepared.source_channel_frequencies_hz[1],
                cube_setup.source_freq_ref,
            )
            .convert_to(cube_setup.output_freq_ref, &frame)
            .unwrap()
            .hz();
            source0_minus_output_min = source0_minus_output_min
                .min(source0_output_hz - cube_setup.output_channel_frequencies_hz[0]);
            source0_minus_output_max = source0_minus_output_max
                .max(source0_output_hz - cube_setup.output_channel_frequencies_hz[0]);
            source1_minus_output_min = source1_minus_output_min
                .min(source1_output_hz - cube_setup.output_channel_frequencies_hz[0]);
            source1_minus_output_max = source1_minus_output_max
                .max(source1_output_hz - cube_setup.output_channel_frequencies_hz[0]);
            let contributions = cube_setup
                .row_output_channel_contributions_batch(
                    &prepared.source_channel_frequencies_hz,
                    &prepared.source_channel_widths_hz,
                    row_time_mjd_sec,
                    0,
                    &engine,
                )
                .unwrap();
            match contributions[0].as_slice() {
                [] => channel0_empty += 1,
                [only] => {
                    channel0_single += 1;
                    assert_eq!(only.source_channel, 0);
                }
                [first, second] => {
                    channel0_mixed += 1;
                    assert_eq!(first.source_channel, 0);
                    assert_eq!(second.source_channel, 1);
                    channel0_upper_factor_min = channel0_upper_factor_min.min(second.factor);
                    channel0_upper_factor_max = channel0_upper_factor_max.max(second.factor);
                }
                other => panic!("unexpected channel-0 contribution shape: {other:?}"),
            }
        }
        eprintln!(
            "refim_point default cube channel0 profile: rows={} empty={} single={} mixed={} upper_factor_range=({channel0_upper_factor_min}, {channel0_upper_factor_max}) output0={} source0-output-range=({source0_minus_output_min}, {source0_minus_output_max}) source1-output-range=({source1_minus_output_min}, {source1_minus_output_max})",
            row_count,
            channel0_empty,
            channel0_single,
            channel0_mixed,
            cube_setup.output_channel_frequencies_hz[0]
        );
        panic!("diagnostic complete");
    }

    fn add_field_row(ms: &mut MeasurementSet) {
        add_field_row_with_direction(
            ms,
            MDirection::from_angles(1.0, 0.5, DirectionRef::J2000),
            TEST_TIME_MJD_SEC,
        );
    }

    fn add_observation_row(ms: &mut MeasurementSet) {
        let table = ms.subtable_mut(SubtableId::Observation).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new(
                    "LOG",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![1], vec![String::new()]).unwrap(),
                    )),
                ),
                RecordField::new(
                    "OBSERVER",
                    Value::Scalar(ScalarValue::String("test".to_string())),
                ),
                RecordField::new(
                    "PROJECT",
                    Value::Scalar(ScalarValue::String("casars-imager".to_string())),
                ),
                RecordField::new(
                    "RELEASE_DATE",
                    Value::Scalar(ScalarValue::Float64(TEST_TIME_MJD_SEC)),
                ),
                RecordField::new(
                    "SCHEDULE",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![1], vec![String::new()]).unwrap(),
                    )),
                ),
                RecordField::new(
                    "SCHEDULE_TYPE",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                RecordField::new(
                    "TELESCOPE_NAME",
                    Value::Scalar(ScalarValue::String("VLA".to_string())),
                ),
                RecordField::new(
                    "TIME_RANGE",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![2],
                            vec![TEST_TIME_MJD_SEC, TEST_TIME_MJD_SEC + 14_400.0],
                        )
                        .unwrap(),
                    )),
                ),
            ]))
            .unwrap();
    }

    const VLA_X: f64 = -1601185.4;
    const VLA_Y: f64 = -5041977.5;
    const VLA_Z: f64 = 3554875.9;
    const TEST_TIME_MJD_SEC: f64 = 59_000.0 * 86_400.0;

    fn add_vla_antenna_row(ms: &mut MeasurementSet) {
        ms.antenna_mut()
            .unwrap()
            .add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [VLA_X, VLA_Y, VLA_Z],
                [0.0; 3],
                25.0,
            )
            .unwrap();
    }

    fn add_vla_antenna_pair(ms: &mut MeasurementSet) {
        add_vla_antenna_row(ms);
        ms.antenna_mut()
            .unwrap()
            .add_antenna(
                "VLA02",
                "N02",
                "GROUND-BASED",
                "ALT-AZ",
                [VLA_X + 100.0, VLA_Y, VLA_Z],
                [0.0; 3],
                25.0,
            )
            .unwrap();
    }

    fn add_field_row_with_direction(
        ms: &mut MeasurementSet,
        direction: MDirection,
        time_mjd_sec: f64,
    ) {
        let table = ms.subtable_mut(SubtableId::Field).unwrap();
        let (lon, lat) = direction.as_angles();
        let direction =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![lon, lat]).unwrap());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("CODE", Value::Scalar(ScalarValue::String(String::new()))),
                RecordField::new("DELAY_DIR", Value::Array(direction.clone())),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new(
                    "NAME",
                    Value::Scalar(ScalarValue::String("field0".to_string())),
                ),
                RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("PHASE_DIR", Value::Array(direction.clone())),
                RecordField::new("REFERENCE_DIR", Value::Array(direction)),
                RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time_mjd_sec))),
            ]))
            .unwrap();
    }

    fn add_spectral_window_row(ms: &mut MeasurementSet, frequencies_hz: &[f64]) {
        let table = ms.subtable_mut(SubtableId::SpectralWindow).unwrap();
        let widths = vec![1.0e6; frequencies_hz.len()];
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![frequencies_hz.len()], frequencies_hz.to_vec())
                            .unwrap(),
                    )),
                ),
                RecordField::new(
                    "CHAN_WIDTH",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![widths.len()], widths.clone()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "EFFECTIVE_BW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![widths.len()], widths.clone()).unwrap(),
                    )),
                ),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String("group".to_string())),
                ),
                RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                RecordField::new(
                    "NAME",
                    Value::Scalar(ScalarValue::String("SPW0".to_string())),
                ),
                RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new(
                    "NUM_CHAN",
                    Value::Scalar(ScalarValue::Int32(frequencies_hz.len() as i32)),
                ),
                RecordField::new(
                    "REF_FREQUENCY",
                    Value::Scalar(ScalarValue::Float64(frequencies_hz[0])),
                ),
                RecordField::new(
                    "RESOLUTION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![widths.len()], widths.clone()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(widths.iter().sum())),
                ),
            ]))
            .unwrap();
    }

    fn add_polarization_row(ms: &mut MeasurementSet) {
        let table = ms.subtable_mut(SubtableId::Polarization).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 2], vec![0, 1, 0, 1]).unwrap(),
                    )),
                ),
                RecordField::new(
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2], vec![9, 12]).unwrap(),
                    )),
                ),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("NUM_CORR", Value::Scalar(ScalarValue::Int32(2))),
            ]))
            .unwrap();
    }

    fn add_full_linear_polarization_row(ms: &mut MeasurementSet) {
        let table = ms.subtable_mut(SubtableId::Polarization).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 4], vec![0, 0, 1, 1, 0, 1, 0, 1]).unwrap(),
                    )),
                ),
                RecordField::new(
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![4], vec![9, 10, 11, 12]).unwrap(),
                    )),
                ),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("NUM_CORR", Value::Scalar(ScalarValue::Int32(4))),
            ]))
            .unwrap();
    }

    fn add_full_circular_polarization_row(ms: &mut MeasurementSet) {
        let table = ms.subtable_mut(SubtableId::Polarization).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 4], vec![0, 0, 1, 1, 0, 1, 0, 1]).unwrap(),
                    )),
                ),
                RecordField::new(
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![4], vec![5, 6, 7, 8]).unwrap(),
                    )),
                ),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("NUM_CORR", Value::Scalar(ScalarValue::Int32(4))),
            ]))
            .unwrap();
    }

    fn add_data_description_row(ms: &mut MeasurementSet) {
        let table = ms.subtable_mut(SubtableId::DataDescription).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
            ]))
            .unwrap();
    }

    fn add_main_row_channels(ms: &mut MeasurementSet, uvw: [f64; 3], vis: &[Complex32]) {
        add_main_row_with_field_and_antennas_channels(ms, 0, 0, 1, uvw, vis);
    }

    fn add_main_row_with_field_channels(
        ms: &mut MeasurementSet,
        field_id: i32,
        uvw: [f64; 3],
        vis: &[Complex32],
    ) {
        add_main_row_with_field_and_antennas_channels(ms, field_id, 0, 1, uvw, vis);
    }

    fn add_main_row_with_antennas(
        ms: &mut MeasurementSet,
        antenna1: i32,
        antenna2: i32,
        uvw: [f64; 3],
        vis: [Complex32; 2],
    ) {
        add_main_row_with_field_and_antennas_channels(ms, 0, antenna1, antenna2, uvw, &vis);
    }

    fn add_main_row_with_field_and_antennas_channels(
        ms: &mut MeasurementSet,
        field_id: i32,
        antenna1: i32,
        antenna2: i32,
        uvw: [f64; 3],
        vis: &[Complex32],
    ) {
        add_main_row_with_field_and_antennas_corr_channels(
            ms, field_id, antenna1, antenna2, uvw, 2, vis,
        );
    }

    fn add_main_row_with_field_and_antennas_corr_channels(
        ms: &mut MeasurementSet,
        field_id: i32,
        antenna1: i32,
        antenna2: i32,
        uvw: [f64; 3],
        num_corr: usize,
        vis: &[Complex32],
    ) {
        assert!(
            vis.len().is_multiple_of(num_corr),
            "test helper expects [num_corr, num_chan] visibility ordering"
        );
        let nchan = vis.len() / num_corr;
        let schema = ms.main_table().schema().unwrap().clone();
        let fields = schema
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => {
                    RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1)))
                }
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => {
                    RecordField::new("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(1.0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(1.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => {
                    RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(0)))
                }
                "STATE_ID" => RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                "TIME" => RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(TEST_TIME_MJD_SEC)),
                ),
                "TIME_CENTROID" => RecordField::new(
                    "TIME_CENTROID",
                    Value::Scalar(ScalarValue::Float64(TEST_TIME_MJD_SEC)),
                ),
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], uvw.to_vec()).unwrap(),
                    )),
                ),
                "DATA" => RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(vec![num_corr, nchan], vis.to_vec()).unwrap(),
                    )),
                ),
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(
                            vec![num_corr, nchan],
                            vec![false; num_corr * nchan],
                        )
                        .unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![num_corr], vec![1.0; num_corr]).unwrap(),
                    )),
                ),
                name => RecordField::new(name, default_main_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    fn add_main_row_with_field_and_antennas_channels_and_weight_spectrum(
        ms: &mut MeasurementSet,
        field_id: i32,
        antenna1: i32,
        antenna2: i32,
        uvw: [f64; 3],
        vis: &[Complex32],
        weight_spectrum: &[f32],
    ) {
        assert!(
            vis.len().is_multiple_of(2),
            "test helper expects [num_corr=2, num_chan] visibility ordering"
        );
        let nchan = vis.len() / 2;
        assert_eq!(weight_spectrum.len(), 2 * nchan);
        let schema = ms.main_table().schema().unwrap().clone();
        let fields = schema
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => {
                    RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1)))
                }
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => {
                    RecordField::new("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(1.0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(1.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => {
                    RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(0)))
                }
                "STATE_ID" => RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                "TIME" => RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(TEST_TIME_MJD_SEC)),
                ),
                "TIME_CENTROID" => RecordField::new(
                    "TIME_CENTROID",
                    Value::Scalar(ScalarValue::Float64(TEST_TIME_MJD_SEC)),
                ),
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], uvw.to_vec()).unwrap(),
                    )),
                ),
                "DATA" => RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(vec![2, nchan], vis.to_vec()).unwrap(),
                    )),
                ),
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![2, nchan], vec![false; 2 * nchan]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "WEIGHT_SPECTRUM" => RecordField::new(
                    "WEIGHT_SPECTRUM",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2, nchan], weight_spectrum.to_vec()).unwrap(),
                    )),
                ),
                name => RecordField::new(name, default_main_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    fn default_main_value(column_name: &str) -> Value {
        let schema = casa_ms::schema::main_table::REQUIRED_COLUMNS
            .iter()
            .chain(casa_ms::schema::main_table::OPTIONAL_COLUMNS.iter())
            .find(|column| column.name == column_name)
            .unwrap();
        match schema.column_kind {
            casa_ms::column_def::ColumnKind::Scalar => match schema.data_type {
                casa_types::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
                casa_types::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
                casa_types::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
                casa_types::PrimitiveType::String => {
                    Value::Scalar(ScalarValue::String(String::new()))
                }
                _ => Value::Scalar(ScalarValue::Float64(0.0)),
            },
            casa_ms::column_def::ColumnKind::FixedArray { shape } => {
                let total: usize = shape.iter().product();
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
                ))
            }
            casa_ms::column_def::ColumnKind::VariableArray { ndim } => {
                let shape = vec![1; ndim];
                let total: usize = shape.iter().product();
                match schema.data_type {
                    casa_types::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                    )),
                    casa_types::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                    )),
                    casa_types::PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total])
                            .unwrap(),
                    )),
                    _ => Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                    )),
                }
            }
        }
    }
}
