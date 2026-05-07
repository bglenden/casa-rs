// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Thin MeasurementSet-backed frontend for the pure `casa-imaging` core.

mod managed_output;
mod oracle;
mod schema;
mod task_contract;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
    AxisKind, BeamFit, BeamFitDebugSummary, CleanConfig, CleanStopReason, CompatibilityMetadata,
    CompatibilityMode, CubeAutoMultiThresholdConfig, CubeChannelRequest, CubeImagingDiagnostics,
    CubeImagingRequest, CubeImagingResult, CubeModelChannelContribution,
    CubeModelInterpolationBatch, Deconvolver, GaussianUvTaper, GridderMode, HogbomIterationMode,
    ImageGeometry, ImagingDiagnostics, ImagingError, ImagingRequest, ImagingResult,
    ImagingStageTimings, MinorCycleTrace, MosaicGridderConfig, MtmfsRequest, ParallelHandBatch,
    PlaneStokes, PrimaryBeamModel, ResidualRefreshDiagnostics, RestoringBeamMode,
    StandardMfsModelPredictor, UvTaperSize, VisibilityBatch, VisibilityMetadataBatch,
    WProjectDiagnostics, WProjectSkipReason, WTermMode, WeightDensityMode, WeightingMode, run_cube,
    run_imaging, run_mtmfs, trace_cube_channel_residual_refresh,
    trace_cube_channel_residual_refresh_model_channel_lambda, trace_cube_channel_w_project_plan,
    trace_w_project_plan,
};
use casa_ms::MeasurementSet;
#[cfg(test)]
use casa_ms::columns::time_columns::TimeColumn;
use casa_ms::columns::weight_columns::WeightSpectrumColumn;
use casa_ms::derived::engine::{MsCalEngine, resolve_field_phase_direction_j2000};
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::spectral_selection::CubeRowSpectralContributions;
use casa_ms::ui_schema::UiCommandSchema;
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeChannelContribution, CubeInterpolation, CubeSpecMode,
    CubeSpectralSetup, convert_frequency_to_frame, parse_numeric_id_selector,
    parse_rest_frequency_hz as parse_ms_rest_frequency_hz, parse_spw_selector,
    resolve_channel_selector_selection, resolve_contiguous_channel_selection,
};
use casa_tables::ColumnSchema;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use image::{ImageBuffer, Rgb};
use ndarray::{Array2, Array4, ArrayD, IxDyn, Zip, s};
use num_complex::{Complex32, Complex64};

pub use managed_output::{
    ManagedImagingArtifact, ManagedImagingChannelRun, ManagedImagingOutput, ManagedImagingRequest,
    ManagedImagingRun, ManagedImagingStageTimings,
};
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
pub use schema::command_schema;
pub use task_contract::{
    IMAGER_TASK_PROTOCOL_NAME, IMAGER_TASK_PROTOCOL_VERSION, ImagerArtifact, ImagerArtifactKind,
    ImagerAutoMultiThresholdConfig, ImagerChannelRunResult, ImagerCleanMaskMode,
    ImagerCleanStopReason, ImagerCoreStageTimings, ImagerCubeAxisConfig, ImagerCubeAxisValue,
    ImagerCubeInterpolation, ImagerDeconvolver,
    ImagerFrontendStageTimings as ImagerFrontendTaskStageTimings, ImagerHogbomIterationMode,
    ImagerPlaneSelection, ImagerProtocolInfo, ImagerRestoringBeamMode, ImagerRunReport,
    ImagerRunTaskRequest, ImagerRunTaskResult, ImagerSaveModel, ImagerSpectralMode,
    ImagerTaskRequest, ImagerTaskResult, ImagerTaskSchemaBundle, ImagerUvTaper, ImagerUvTaperSize,
    ImagerWTermMode, ImagerWeighting,
};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const DEFAULT_BATCH_SIZE: usize = 65_536;
const OUTLIER_IMAGE_FIELDS: &[&str] = &[
    "imagename",
    "imsize",
    "cell",
    "phasecenter",
    "startmodel",
    "mask",
    "specmode",
    "nchan",
    "start",
    "width",
    "nterms",
    "reffreq",
    "gridder",
    "deconvolver",
    "wprojplanes",
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct OutlierFileDefinition {
    image_name: Option<String>,
    imsize: Option<Vec<usize>>,
    cell: Option<Vec<String>>,
    phasecenter: Option<String>,
    startmodel: Option<String>,
    mask: Option<String>,
    specmode: Option<String>,
    nchan: Option<usize>,
    start: Option<String>,
    width: Option<String>,
    nterms: Option<usize>,
    reffreq: Option<String>,
    gridder: Option<String>,
    deconvolver: Option<String>,
    wprojplanes: Option<usize>,
    ignored_fields: Vec<String>,
}

impl OutlierFileDefinition {
    fn empty() -> Self {
        Self {
            image_name: None,
            imsize: None,
            cell: None,
            phasecenter: None,
            startmodel: None,
            mask: None,
            specmode: None,
            nchan: None,
            start: None,
            width: None,
            nterms: None,
            reffreq: None,
            gridder: None,
            deconvolver: None,
            wprojplanes: None,
            ignored_fields: Vec::new(),
        }
    }

    fn has_any_field(&self) -> bool {
        self.image_name.is_some()
            || self.imsize.is_some()
            || self.cell.is_some()
            || self.phasecenter.is_some()
            || self.startmodel.is_some()
            || self.mask.is_some()
            || self.specmode.is_some()
            || self.nchan.is_some()
            || self.start.is_some()
            || self.width.is_some()
            || self.nterms.is_some()
            || self.reffreq.is_some()
            || self.gridder.is_some()
            || self.deconvolver.is_some()
            || self.wprojplanes.is_some()
            || !self.ignored_fields.is_empty()
    }
}

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

/// CASA-style clean-mask generation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanMaskMode {
    /// Use only explicit user masks supplied through boxes or a CASA mask image.
    #[default]
    User,
    /// Generate a clean mask with the CASA `auto-multithresh` control family.
    AutoMultiThreshold,
}

/// Guide-visible CASA `auto-multithresh` controls.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMultiThresholdConfig {
    /// Sidelobe threshold factor multiplied by the PSF sidelobe level.
    pub sidelobe_threshold: f32,
    /// Noise threshold factor multiplied by the robust residual RMS.
    pub noise_threshold: f32,
    /// Lower noise threshold factor used when growing a mask.
    pub low_noise_threshold: f32,
    /// Negative-feature threshold factor; zero disables negative masks.
    pub negative_threshold: f32,
    /// Smoothing factor for CASA's beam-scaled mask smoothing stage.
    pub smooth_factor: f32,
    /// Minimum region size as a fraction of the fitted beam area.
    pub min_beam_frac: f32,
    /// Fraction of the smoothed mask peak used to cut mask edges.
    pub cut_threshold: f32,
    /// Maximum constrained binary-dilation iterations for mask growth.
    pub grow_iterations: usize,
    /// Whether grown masks are pruned after dilation.
    pub do_grow_prune: bool,
    /// CASA percent-change stop control for later automask updates.
    pub min_percent_change: f32,
    /// Use CASA's fast-noise statistics path.
    pub fast_noise: bool,
}

impl Default for AutoMultiThresholdConfig {
    fn default() -> Self {
        Self {
            sidelobe_threshold: 3.0,
            noise_threshold: 5.0,
            low_noise_threshold: 1.5,
            negative_threshold: 0.0,
            smooth_factor: 1.0,
            min_beam_frac: 0.3,
            cut_threshold: 0.01,
            grow_iterations: 75,
            do_grow_prune: true,
            min_percent_change: -1.0,
            fast_noise: true,
        }
    }
}

impl From<AutoMultiThresholdConfig> for CubeAutoMultiThresholdConfig {
    fn from(value: AutoMultiThresholdConfig) -> Self {
        Self {
            sidelobe_threshold: value.sidelobe_threshold,
            noise_threshold: value.noise_threshold,
            low_noise_threshold: value.low_noise_threshold,
            negative_threshold: value.negative_threshold,
            smooth_factor: value.smooth_factor,
            min_beam_frac: value.min_beam_frac,
            cut_threshold: value.cut_threshold,
            grow_iterations: value.grow_iterations,
            do_grow_prune: value.do_grow_prune,
            min_percent_change: value.min_percent_change,
            fast_noise: value.fast_noise,
        }
    }
}

/// CASA-style model persistence after imaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveModelMode {
    /// Do not write a visibility model back to the MeasurementSet.
    None,
    /// Predict the final MFS model image into MAIN.MODEL_DATA.
    ModelColumn,
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
        WeightingMode::BriggsBwTaper { robust } => format!("briggsbwtaper:{robust}"),
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

fn canonical_hogbom_iteration_mode_name(mode: HogbomIterationMode) -> &'static str {
    match mode {
        HogbomIterationMode::Strict => "strict",
        HogbomIterationMode::CasaInclusive => "casa",
    }
}

fn canonical_clean_mask_mode_name(mode: CleanMaskMode) -> &'static str {
    match mode {
        CleanMaskMode::User => "user",
        CleanMaskMode::AutoMultiThreshold => "auto-multithresh",
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
    manifest.insert(
        "hogbom_iteration_mode".to_string(),
        canonical_hogbom_iteration_mode_name(config.hogbom_iteration_mode).to_string(),
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
        "mosaic_pb_limit".to_string(),
        config.mosaic_pb_limit.to_string(),
    );
    manifest.insert("pbcor".to_string(), config.pbcor.to_string());
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
        "use_mask".to_string(),
        canonical_clean_mask_mode_name(config.use_mask).to_string(),
    );
    manifest.insert(
        "auto_sidelobe_threshold".to_string(),
        config.auto_mask.sidelobe_threshold.to_string(),
    );
    manifest.insert(
        "auto_noise_threshold".to_string(),
        config.auto_mask.noise_threshold.to_string(),
    );
    manifest.insert(
        "auto_low_noise_threshold".to_string(),
        config.auto_mask.low_noise_threshold.to_string(),
    );
    manifest.insert(
        "auto_negative_threshold".to_string(),
        config.auto_mask.negative_threshold.to_string(),
    );
    manifest.insert(
        "auto_smooth_factor".to_string(),
        config.auto_mask.smooth_factor.to_string(),
    );
    manifest.insert(
        "auto_min_beam_frac".to_string(),
        config.auto_mask.min_beam_frac.to_string(),
    );
    manifest.insert(
        "auto_cut_threshold".to_string(),
        config.auto_mask.cut_threshold.to_string(),
    );
    manifest.insert(
        "auto_grow_iterations".to_string(),
        config.auto_mask.grow_iterations.to_string(),
    );
    manifest.insert(
        "auto_do_grow_prune".to_string(),
        config.auto_mask.do_grow_prune.to_string(),
    );
    manifest.insert(
        "auto_min_percent_change".to_string(),
        config.auto_mask.min_percent_change.to_string(),
    );
    manifest.insert(
        "auto_fast_noise".to_string(),
        config.auto_mask.fast_noise.to_string(),
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
    let args = args.into_iter().collect::<Vec<_>>();
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("--ui-schema")))
    {
        println!(
            "{}",
            command_schema("casars-imager")
                .render_json_pretty()
                .map_err(|error| format!("serialize ui schema: {error}"))?
        );
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("--json-schema")))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&ImagerTaskSchemaBundle::current())
                .map_err(|error| format!("serialize imager task schema: {error}"))?
        );
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("--protocol-info")))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&ImagerProtocolInfo::current())
                .map_err(|error| format!("serialize imager protocol info: {error}"))?
        );
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("-h" | "--help")))
    {
        println!("{}", render_help(&command_schema("casars-imager")));
        return Ok(());
    }

    let (json_run, filtered_args) = extract_string_option(&args, "--json-run")?;
    if let Some(source) = json_run {
        let request = ImagerTaskRequest::read_from_source(&source)?;
        let result = request.execute()?;
        println!(
            "{}",
            serde_json::to_string_pretty(&result)
                .map_err(|error| format!("serialize imager task result: {error}"))?
        );
        return Ok(());
    }

    let (managed_output, filtered_args) = extract_option_value(&filtered_args, "--managed-output")?;
    let config = CliConfig::parse(filtered_args)?;
    let result = ImagerRunTaskRequest::from_cli_config(&config).execute()?;
    if managed_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&ManagedImagingOutput::from_task_result(&result))
                .map_err(|error| format!("serialize managed imaging output: {error}"))?
        );
        return Ok(());
    }
    for warning in &result.run.warnings {
        eprintln!("warning: {warning}");
    }
    println!(
        "Wrote CASA-compatible products at prefix {} ({} gridded samples, {} major cycles, {} minor iterations, stop={:?})",
        result.request.image_name.display(),
        result.run.gridded_samples,
        result.run.major_cycles,
        result.run.minor_iterations,
        result.run.clean_stop_reason
    );
    Ok(())
}

fn extract_option_value(args: &[OsString], flag: &str) -> Result<(bool, Vec<OsString>), String> {
    let mut enabled = false;
    let mut filtered = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        let Some(current) = args[index].to_str() else {
            filtered.push(args[index].clone());
            index += 1;
            continue;
        };
        if current != flag {
            filtered.push(args[index].clone());
            index += 1;
            continue;
        }
        let value = args
            .get(index + 1)
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("{flag} requires a value"))?;
        enabled = match value {
            "true" => true,
            "false" => false,
            other => return Err(format!("{flag} expects true or false, got {other:?}")),
        };
        index += 2;
    }
    Ok((enabled, filtered))
}

fn extract_string_option(
    args: &[OsString],
    flag: &str,
) -> Result<(Option<String>, Vec<OsString>), String> {
    let mut value = None;
    let mut filtered = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        let Some(current) = args[index].to_str() else {
            filtered.push(args[index].clone());
            index += 1;
            continue;
        };
        if current != flag {
            filtered.push(args[index].clone());
            index += 1;
            continue;
        }
        let next = args
            .get(index + 1)
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("missing value for {flag}"))?;
        value = Some(next.to_string());
        index += 2;
    }
    Ok((value, filtered))
}

fn render_help(schema: &UiCommandSchema) -> String {
    format!(
        "{}\n\nMachine-readable:\n  --ui-schema              Emit the launcher/TUI schema\n  --json-schema            Emit the canonical imager task JSON schema\n  --protocol-info          Emit the imager task protocol descriptor\n  --json-run <SOURCE>      Execute one JSON ImagerTaskRequest from SOURCE or - for stdin\n",
        schema.render_help()
    )
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
        config.use_pointing,
        uvw_reprojection_mode_for_selection(config, &selection),
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
            hogbom_iteration_mode: config.hogbom_iteration_mode,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        initial_model: None,
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
            hogbom_iteration_mode: config.hogbom_iteration_mode,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        channel_clean_mask: None,
        auto_mask: None,
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
    validate_save_model_request(config)?;
    validate_start_model_request(config)?;
    validate_outlier_file_request(config)?;
    validate_auto_mask_config(config.use_mask, &config.auto_mask)?;
    let total_start = Instant::now();
    let stage_start = Instant::now();
    let ms_paths = measurement_set_paths(config)?;
    let mut ms = MeasurementSet::open(
        ms_paths
            .first()
            .ok_or_else(|| "internal error: empty MeasurementSet input list".to_string())?,
    )
    .map_err(|error| format!("open MS: {error}"))?;
    let extra_ms = ms_paths
        .iter()
        .skip(1)
        .map(|path| {
            MeasurementSet::open(path)
                .map_err(|error| format!("open MS {}: {error}", path.display()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let open_measurement_set = stage_start.elapsed();
    maybe_log_frontend_progress(
        "open_measurement_set",
        open_measurement_set,
        total_start.elapsed(),
    );
    let stage_start = Instant::now();
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let (prepared, model_trace) = if config.save_model == SaveModelMode::ModelColumn {
        let (prepared, trace) = prepare_plane_input_with_trace(&ms, config, data_column)?;
        (prepared, Some(trace))
    } else {
        let mut prepared_inputs = vec![merge_prepared_inputs_for_same_measurement_set(
            prepare_inputs_for_measurement_set(&ms, config, data_column)?,
        )?];
        for (path, extra) in ms_paths.iter().skip(1).zip(extra_ms.iter()) {
            let mut extra_config = config.clone();
            extra_config.ms = path.clone();
            let extra_data_column = resolve_data_column(extra, config.datacolumn.as_deref())?;
            prepared_inputs.push(merge_prepared_inputs_for_same_measurement_set(
                prepare_inputs_for_measurement_set(extra, &extra_config, extra_data_column)?,
            )?);
        }
        (merge_prepared_inputs(prepared_inputs)?, None)
    };
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
    let (run_result, effective_clean_mask) = match prepared_input {
        PreparedInput::Mfs(plane) => {
            let start_model = load_start_model_image(config, geometry, &plane.gridder_mode)?;
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            };
            let user_clean_mask = build_clean_mask(
                config.imsize,
                &config.mask_boxes,
                config.mask_image.as_deref(),
            )?;
            if config.deconvolver == Deconvolver::Mtmfs {
                if config.use_mask == CleanMaskMode::AutoMultiThreshold {
                    return Err(
                        "usemask='auto-multithresh' does not yet support deconvolver='mtmfs'"
                            .to_string(),
                    );
                }
                (
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
                            clean_mask: user_clean_mask.clone(),
                            compatibility: CompatibilityMode::CasaStandardMfs,
                        })
                        .map_err(|error| error.to_string())?,
                    ),
                    user_clean_mask.map(EffectiveCleanMask::Plane),
                )
            } else {
                let mut clean_mask = user_clean_mask;
                let common_request = ImagingRequest {
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
                    clean_mask: clean_mask.clone(),
                    initial_model: start_model,
                    w_term_mode: config.w_term_mode,
                    w_project_planes: config.w_project_planes,
                    compatibility: CompatibilityMode::CasaStandardMfs,
                };
                let mut dirty_seed = None;
                if config.use_mask == CleanMaskMode::AutoMultiThreshold {
                    let mut dirty_request = common_request.clone();
                    dirty_request.clean = frontend_dirty_clean_config(clean.psf_cutoff);
                    dirty_request.clean_mask = clean_mask.clone();
                    let dirty = run_imaging(&dirty_request).map_err(|error| error.to_string())?;
                    clean_mask = Some(build_auto_multithresh_clean_mask(
                        geometry,
                        &dirty.residual,
                        dirty.diagnostics.max_psf_sidelobe_level,
                        dirty.beam,
                        clean_mask.as_ref(),
                        &config.auto_mask,
                    )?);
                    dirty_seed = Some(dirty);
                }
                if clean.niter == 0 {
                    let result = if let Some(dirty) = dirty_seed {
                        dirty
                    } else {
                        run_imaging(&common_request).map_err(|error| error.to_string())?
                    };
                    (
                        RunProducts::Mfs(result),
                        clean_mask.map(EffectiveCleanMask::Plane),
                    )
                } else {
                    let mut request = common_request;
                    request.clean_mask = clean_mask.clone();
                    (
                        RunProducts::Mfs(run_imaging(&request).map_err(|error| error.to_string())?),
                        clean_mask.map(EffectiveCleanMask::Plane),
                    )
                }
            }
        }
        PreparedInput::Cube(cube) => {
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            };
            let clean_mask = build_clean_mask(
                config.imsize,
                &config.mask_boxes,
                config.mask_image.as_deref(),
            )?;
            let mut channel_clean_mask = None::<Array4<bool>>;
            let mut dirty_seed = None;
            let standard_cube = cube
                .gridder_modes
                .iter()
                .all(|mode| matches!(mode, GridderMode::Standard));
            if config.use_mask == CleanMaskMode::AutoMultiThreshold && !standard_cube {
                let dirty = run_frontend_cube(
                    geometry,
                    cube.clone(),
                    config,
                    frontend_dirty_clean_config(clean.psf_cutoff),
                    clean_mask.clone(),
                    None,
                )?;
                let cube_mask = build_auto_multithresh_cube_clean_mask(
                    geometry,
                    &dirty.result,
                    config.restoring_beam_mode,
                    clean_mask.as_ref(),
                    &config.auto_mask,
                )?;
                channel_clean_mask = Some(cube_mask);
                dirty_seed = Some(dirty);
            }
            if clean.niter == 0 {
                let result = if let Some(dirty) = dirty_seed {
                    dirty
                } else {
                    run_frontend_cube(geometry, cube, config, clean, clean_mask.clone(), None)?
                };
                let effective_clean_mask = result
                    .result
                    .clean_mask
                    .clone()
                    .map(EffectiveCleanMask::Cube)
                    .or_else(|| clean_mask.clone().map(EffectiveCleanMask::Plane));
                (RunProducts::Cube(result), effective_clean_mask)
            } else {
                let result = run_frontend_cube(
                    geometry,
                    cube,
                    config,
                    clean,
                    clean_mask.clone(),
                    channel_clean_mask,
                )?;
                let effective_clean_mask = result
                    .result
                    .clean_mask
                    .clone()
                    .map(EffectiveCleanMask::Cube)
                    .or_else(|| clean_mask.clone().map(EffectiveCleanMask::Plane));
                (RunProducts::Cube(result), effective_clean_mask)
            }
        }
    };
    let run_imaging_time = stage_start.elapsed();
    maybe_log_frontend_progress("run_imaging", run_imaging_time, total_start.elapsed());

    let stage_start = Instant::now();
    let coords = build_coordinate_system(CoordinateSystemBuild {
        imsize: config.imsize,
        phase_center: phase_center.angles_rad,
        cell_arcsec: config.cell_arcsec,
        freq_ref: prepared_freq_ref,
        direction_ref: phase_center.reference,
        plane_stokes: run_result.plane_stokes(),
        channel_frequencies_hz: run_result.channel_frequencies_hz(),
        requested_rest_frequency_hz: config.cube_axis.rest_frequency_hz,
    });
    let build_coordinate_system = stage_start.elapsed();
    maybe_log_frontend_progress(
        "build_coordinate_system",
        build_coordinate_system,
        total_start.elapsed(),
    );
    let stage_start = Instant::now();
    if config.save_model == SaveModelMode::ModelColumn {
        let trace = model_trace.as_ref().ok_or_else(|| {
            "internal error: savemodel=modelcolumn requires prepared visibility trace".to_string()
        })?;
        let written = write_model_column(&mut ms, config, &run_result, trace)?;
        maybe_log_frontend_progress(
            "write_model_column",
            stage_start.elapsed(),
            total_start.elapsed(),
        );
        maybe_log_frontend_progress(
            &format!("write_model_column/written_samples/{written}"),
            stage_start.elapsed(),
            total_start.elapsed(),
        );
    }
    write_products(config, &coords, &run_result, effective_clean_mask.as_ref())?;
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

fn validate_save_model_request(config: &CliConfig) -> Result<(), String> {
    if config.save_model == SaveModelMode::ModelColumn && measurement_set_paths(config)?.len() > 1 {
        return Err("savemodel=modelcolumn currently supports exactly one --ms input".to_string());
    }
    if config.save_model != SaveModelMode::ModelColumn {
        return Ok(());
    }
    if config.deconvolver == Deconvolver::Mtmfs {
        return Err("savemodel=modelcolumn does not yet support deconvolver='mtmfs'".to_string());
    }
    Ok(())
}

fn validate_start_model_request(config: &CliConfig) -> Result<(), String> {
    let Some(start_model) = config.start_model.as_ref() else {
        return Ok(());
    };
    if config.spectral_mode != SpectralMode::Mfs {
        return Err("startmodel currently supports only specmode='mfs'".to_string());
    }
    if config.deconvolver == Deconvolver::Mtmfs {
        return Err(
            "startmodel currently supports only single-term deconvolvers; mtmfs uses multi-term startmodel images"
                .to_string(),
        );
    }
    if !start_model.exists() {
        return Err(format!(
            "startmodel image {} does not exist",
            start_model.display()
        ));
    }
    let mut output_model = config.imagename.as_os_str().to_os_string();
    output_model.push(".model");
    let output_model = PathBuf::from(output_model);
    if output_model.exists() {
        return Err(format!(
            "imagename.model {} already exists; unset startmodel or remove the existing model image",
            output_model.display()
        ));
    }
    Ok(())
}

fn validate_outlier_file_request(config: &CliConfig) -> Result<(), String> {
    let Some(outlier_file) = config.outlier_file.as_ref() else {
        return Ok(());
    };
    let definitions = parse_outlier_file(outlier_file)?;
    let ignored_fields = definitions
        .iter()
        .flat_map(|definition| definition.ignored_fields.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let ignored_text = if ignored_fields.is_empty() {
        "none".to_string()
    } else {
        ignored_fields.into_iter().collect::<Vec<_>>().join(",")
    };
    Err(format!(
        "outlierfile {} defines {} outlier image(s), but casa-rs currently preserves a single-image frontend boundary; parsed CASA outlier fields are [{}], ignored fields are [{}], and multi-image/outlier-field orchestration is not implemented",
        outlier_file.display(),
        definitions.len(),
        OUTLIER_IMAGE_FIELDS.join(","),
        ignored_text
    ))
}

fn parse_outlier_file(path: &Path) -> Result<Vec<OutlierFileDefinition>, String> {
    let text = fs::read_to_string(path).map_err(|error| {
        format!(
            "Cannot find or read outlier file {}: {error}",
            path.display()
        )
    })?;
    let mut definitions = Vec::<OutlierFileDefinition>::new();
    let mut current = OutlierFileDefinition::empty();
    for (line_index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((raw_key, raw_value)) = line.split_once('=') else {
            return Err(format!(
                "Error in outlier file {} line {}: expected parameter=value, got {raw_line:?}",
                path.display(),
                line_index + 1
            ));
        };
        if raw_value.contains('=') {
            return Err(format!(
                "Error in outlier file {} line {}: expected one parameter=value pair, got {raw_line:?}",
                path.display(),
                line_index + 1
            ));
        }
        let key = raw_key.trim();
        let value = raw_value.trim();
        if key == "imagename" && current.has_any_field() {
            definitions.push(current);
            current = OutlierFileDefinition::empty();
        }
        apply_outlier_parameter(&mut current, key, value, path, line_index + 1)?;
    }
    if current.has_any_field() {
        definitions.push(current);
    }
    Ok(definitions)
}

fn apply_outlier_parameter(
    definition: &mut OutlierFileDefinition,
    key: &str,
    value: &str,
    path: &Path,
    line_number: usize,
) -> Result<(), String> {
    match key {
        "imagename" => definition.image_name = Some(trim_outlier_string(value).to_string()),
        "imsize" => {
            definition.imsize = Some(parse_outlier_usize_vec(value, key, path, line_number)?)
        }
        "cell" => definition.cell = Some(parse_outlier_string_vec(value)),
        "phasecenter" => definition.phasecenter = Some(trim_outlier_string(value).to_string()),
        "startmodel" => definition.startmodel = Some(trim_outlier_string(value).to_string()),
        "mask" => definition.mask = Some(trim_outlier_string(value).to_string()),
        "specmode" => definition.specmode = Some(trim_outlier_string(value).to_string()),
        "nchan" => definition.nchan = Some(parse_outlier_usize(value, key, path, line_number)?),
        "start" => definition.start = Some(trim_outlier_string(value).to_string()),
        "width" => definition.width = Some(trim_outlier_string(value).to_string()),
        "nterms" => definition.nterms = Some(parse_outlier_usize(value, key, path, line_number)?),
        "reffreq" => definition.reffreq = Some(trim_outlier_string(value).to_string()),
        "gridder" => definition.gridder = Some(trim_outlier_string(value).to_string()),
        "deconvolver" => definition.deconvolver = Some(trim_outlier_string(value).to_string()),
        "wprojplanes" => {
            definition.wprojplanes = Some(parse_outlier_usize(value, key, path, line_number)?)
        }
        other => definition.ignored_fields.push(other.to_string()),
    }
    Ok(())
}

fn parse_outlier_usize(
    value: &str,
    key: &str,
    path: &Path,
    line_number: usize,
) -> Result<usize, String> {
    trim_outlier_string(value)
        .parse::<usize>()
        .map_err(|error| {
            format!(
                "Cannot evaluate outlier field parameter {key:?} in {} line {}: {error}",
                path.display(),
                line_number
            )
        })
}

fn parse_outlier_usize_vec(
    value: &str,
    key: &str,
    path: &Path,
    line_number: usize,
) -> Result<Vec<usize>, String> {
    parse_outlier_list(value)
        .into_iter()
        .map(|part| {
            part.parse::<usize>().map_err(|error| {
                format!(
                    "Cannot evaluate outlier field parameter {key:?} in {} line {}: {error}",
                    path.display(),
                    line_number
                )
            })
        })
        .collect()
}

fn parse_outlier_string_vec(value: &str) -> Vec<String> {
    parse_outlier_list(value)
}

fn parse_outlier_list(value: &str) -> Vec<String> {
    let trimmed = value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(trim_outlier_string)
        .map(str::to_string)
        .collect()
}

fn trim_outlier_string(value: &str) -> &str {
    value
        .trim()
        .trim_matches(',')
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
}

fn load_start_model_image(
    config: &CliConfig,
    geometry: ImageGeometry,
    gridder_mode: &GridderMode,
) -> Result<Option<Array2<f32>>, String> {
    let Some(path) = config.start_model.as_ref() else {
        return Ok(None);
    };
    if matches!(gridder_mode, GridderMode::Mosaic(_)) {
        return Err("startmodel does not yet support mosaic gridder runs".to_string());
    }
    let image =
        PagedImage::<f32>::open(path).map_err(|error| format!("open startmodel image: {error}"))?;
    let shape = image.shape().to_vec();
    let [nx, ny] = geometry.image_shape;
    match shape.as_slice() {
        [sx, sy] if *sx == nx && *sy == ny => {}
        [sx, sy, stokes, channel] if *sx == nx && *sy == ny && *stokes == 1 && *channel == 1 => {}
        _ => {
            return Err(format!(
                "startmodel image {} has shape {:?}; expected [{nx}, {ny}] or [{nx}, {ny}, 1, 1]",
                path.display(),
                shape
            ));
        }
    }
    let pixels = image
        .get_slice(&vec![0; shape.len()], &shape)
        .map_err(|error| format!("read startmodel image {}: {error}", path.display()))?;
    let mut model = Array2::<f32>::zeros((nx, ny));
    for x in 0..nx {
        for y in 0..ny {
            let value = match shape.len() {
                2 => pixels[IxDyn(&[x, y])],
                4 => pixels[IxDyn(&[x, y, 0, 0])],
                _ => unreachable!("validated startmodel shape"),
            };
            if !value.is_finite() {
                return Err(format!(
                    "startmodel image {} contains non-finite pixel at [{x}, {y}]",
                    path.display()
                ));
            }
            model[(x, y)] = value;
        }
    }
    Ok(Some(model))
}

fn measurement_set_paths(config: &CliConfig) -> Result<Vec<PathBuf>, String> {
    let text = config.ms.to_string_lossy();
    let paths = text
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Err("missing --ms input path".to_string());
    }
    Ok(paths)
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

fn config_for_cube_residual_trace_preparation(config: &CliConfig) -> CliConfig {
    let mut trace_config = config.clone();
    trace_config.dirty_only = false;
    trace_config.niter = trace_config.niter.max(1);
    trace_config
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
    let trace_config = config_for_cube_residual_trace_preparation(config);
    let prepared = prepare_plane_input(&ms, &trace_config, data_column)?;
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
            hogbom_iteration_mode: config.hogbom_iteration_mode,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        channel_clean_mask: None,
        auto_mask: None,
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
    let trace_config = config_for_cube_residual_trace_preparation(config);
    let prepared = prepare_plane_input(&ms, &trace_config, data_column)?;
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
            hogbom_iteration_mode: config.hogbom_iteration_mode,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        channel_clean_mask: None,
        auto_mask: None,
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
    let trace_config = config_for_cube_residual_trace_preparation(config);
    let prepared = prepare_plane_input(&ms, &trace_config, data_column)?;
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
            hogbom_iteration_mode: config.hogbom_iteration_mode,
        },
        clean_mask: build_clean_mask(
            config.imsize,
            &config.mask_boxes,
            config.mask_image.as_deref(),
        )?,
        channel_clean_mask: None,
        auto_mask: None,
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
    /// CASA-style model persistence mode.
    pub save_model: SaveModelMode,
    /// Optional CASA image used to seed the initial model product.
    pub start_model: Option<PathBuf>,
    /// Optional CASA outlier-field definition file.
    pub outlier_file: Option<PathBuf>,
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
    /// CASA-style `usepointing` toggle for POINTING-table direction corrections.
    pub use_pointing: bool,
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
    /// Mosaic primary-beam cutoff used for flat-noise normalization.
    pub mosaic_pb_limit: f32,
    /// Write CASA-style PB-corrected mosaic image products.
    pub pbcor: bool,
    /// Residual-refresh cadence.
    pub minor_cycle_length: usize,
    /// CASA-style cycle-threshold scale factor.
    pub cyclefactor: f32,
    /// Lower clamp for the PSF fraction used to derive cycle thresholds.
    pub min_psf_fraction: f32,
    /// Upper clamp for the PSF fraction used to derive cycle thresholds.
    pub max_psf_fraction: f32,
    /// Hogbom minor-cycle iteration accounting policy.
    pub hogbom_iteration_mode: HogbomIterationMode,
    /// CASA-style clean mask mode.
    pub use_mask: CleanMaskMode,
    /// CASA-style `auto-multithresh` controls used when `use_mask` is auto.
    pub auto_mask: AutoMultiThresholdConfig,
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
        let mut save_model = SaveModelMode::None;
        let mut start_model = None::<PathBuf>;
        let mut outlier_file = None::<PathBuf>;
        let mut correlation = None::<String>;
        let mut spectral_mode = SpectralMode::Mfs;
        let mut cube_axis = CubeAxisConfig::default();
        let mut weighting_name = String::from("natural");
        let mut per_channel_weight_density = false;
        let mut use_pointing = false;
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
        let mut mosaic_pb_limit = 0.2f32;
        let mut pbcor = false;
        let mut minor_cycle_length = 8usize;
        let mut cyclefactor = 1.0f32;
        let mut min_psf_fraction = 0.05f32;
        let mut max_psf_fraction = 0.8f32;
        let mut hogbom_iteration_mode = HogbomIterationMode::Strict;
        let mut use_mask = CleanMaskMode::User;
        let mut auto_mask = AutoMultiThresholdConfig::default();
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
                "--savemodel" => {
                    save_model = parse_save_model_mode(&next_value(&mut args, "--savemodel")?)?;
                    continue;
                }
                "--startmodel" => {
                    start_model = Some(next_path(&mut args, "--startmodel")?);
                    continue;
                }
                "--outlierfile" => {
                    outlier_file = Some(next_path(&mut args, "--outlierfile")?);
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
                "--usepointing" | "--use-pointing" => {
                    use_pointing = true;
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
                "--pblimit" => {
                    mosaic_pb_limit = next_value(&mut args, "--pblimit")?
                        .parse()
                        .map_err(|error| format!("parse --pblimit: {error}"))?;
                    continue;
                }
                "--pbcor" => {
                    pbcor = true;
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
                "--hogbom-iteration-mode" => {
                    hogbom_iteration_mode = parse_hogbom_iteration_mode(&next_value(
                        &mut args,
                        "--hogbom-iteration-mode",
                    )?)?;
                    continue;
                }
                "--casa-hogbom-iterations" => {
                    hogbom_iteration_mode = HogbomIterationMode::CasaInclusive;
                    continue;
                }
                "--usemask" => {
                    use_mask = parse_clean_mask_mode(&next_value(&mut args, "--usemask")?)?;
                    continue;
                }
                "--sidelobethreshold" => {
                    auto_mask.sidelobe_threshold =
                        parse_auto_mask_f32(&next_value(&mut args, "--sidelobethreshold")?)?;
                    continue;
                }
                "--noisethreshold" => {
                    auto_mask.noise_threshold =
                        parse_auto_mask_f32(&next_value(&mut args, "--noisethreshold")?)?;
                    continue;
                }
                "--lownoisethreshold" => {
                    auto_mask.low_noise_threshold =
                        parse_auto_mask_f32(&next_value(&mut args, "--lownoisethreshold")?)?;
                    continue;
                }
                "--negativethreshold" => {
                    auto_mask.negative_threshold =
                        parse_auto_mask_f32(&next_value(&mut args, "--negativethreshold")?)?;
                    continue;
                }
                "--smoothfactor" => {
                    auto_mask.smooth_factor =
                        parse_auto_mask_f32(&next_value(&mut args, "--smoothfactor")?)?;
                    continue;
                }
                "--minbeamfrac" => {
                    auto_mask.min_beam_frac =
                        parse_auto_mask_f32(&next_value(&mut args, "--minbeamfrac")?)?;
                    continue;
                }
                "--cutthreshold" => {
                    auto_mask.cut_threshold =
                        parse_auto_mask_f32(&next_value(&mut args, "--cutthreshold")?)?;
                    continue;
                }
                "--growiterations" => {
                    auto_mask.grow_iterations = next_value(&mut args, "--growiterations")?
                        .parse()
                        .map_err(|error| format!("parse --growiterations: {error}"))?;
                    continue;
                }
                "--dogrowprune" => {
                    auto_mask.do_grow_prune = true;
                    continue;
                }
                "--no-dogrowprune" => {
                    auto_mask.do_grow_prune = false;
                    continue;
                }
                "--minpercentchange" => {
                    auto_mask.min_percent_change =
                        parse_auto_mask_f32(&next_value(&mut args, "--minpercentchange")?)?;
                    continue;
                }
                "--fastnoise" => {
                    auto_mask.fast_noise = true;
                    continue;
                }
                "--no-fastnoise" => {
                    auto_mask.fast_noise = false;
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
        if !(mosaic_pb_limit.is_finite() && mosaic_pb_limit > 0.0) {
            return Err("--pblimit must be finite and > 0".to_string());
        }
        validate_auto_mask_config(use_mask, &auto_mask)?;

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
            save_model,
            start_model,
            outlier_file,
            correlation,
            spectral_mode,
            cube_axis,
            weighting,
            per_channel_weight_density,
            use_pointing,
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
            mosaic_pb_limit,
            pbcor,
            minor_cycle_length,
            cyclefactor,
            min_psf_fraction,
            max_psf_fraction,
            hogbom_iteration_mode,
            use_mask,
            auto_mask,
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

#[derive(Clone)]
struct CubePlaneInput {
    phase_center: PhaseCenter,
    freq_ref: FrequencyRef,
    plane_stokes: PlaneStokes,
    channels: Vec<CubeChannelRequest>,
    gridder_modes: Vec<GridderMode>,
    casa_cube_briggs_preweighting: Option<CasaCubeBriggsPreparedWeighting>,
}

#[derive(Clone)]
struct CasaCubeBriggsPreparedWeighting {
    weighting: WeightingMode,
    fractional_bandwidth: f64,
    density_plane_count: usize,
    density_group_field_ids: Vec<usize>,
    density_batches: Vec<VisibilityBatch>,
    density_sample_rows: Vec<Vec<usize>>,
    channel_density_plane_indices: Vec<Vec<Option<usize>>>,
    channel_sample_rows: Vec<Vec<usize>>,
    channel_sample_source_channels: Vec<Vec<Option<usize>>>,
}

enum FrontendCubePlaneSeed {
    Plane(Box<ImagingResult>),
    Blank(String),
}

enum PreparedInput {
    Mfs(PlaneInput),
    Cube(CubePlaneInput),
}

#[derive(Clone, Copy)]
enum CasaCubeBriggsDensityMerge {
    SameMeasurementSet,
    DistinctMeasurementSet,
}

impl PreparedInput {
    fn phase_center(&self) -> &PhaseCenter {
        match self {
            Self::Mfs(plane) => &plane.phase_center,
            Self::Cube(cube) => &cube.phase_center,
        }
    }
}

fn merge_prepared_inputs(inputs: Vec<PreparedInput>) -> Result<PreparedInput, String> {
    merge_prepared_inputs_with_density_merge(
        inputs,
        CasaCubeBriggsDensityMerge::DistinctMeasurementSet,
    )
}

fn merge_prepared_inputs_for_same_measurement_set(
    inputs: Vec<PreparedInput>,
) -> Result<PreparedInput, String> {
    merge_prepared_inputs_with_density_merge(inputs, CasaCubeBriggsDensityMerge::SameMeasurementSet)
}

fn merge_prepared_inputs_with_density_merge(
    mut inputs: Vec<PreparedInput>,
    density_merge: CasaCubeBriggsDensityMerge,
) -> Result<PreparedInput, String> {
    if inputs.is_empty() {
        return Err("multi-MS imaging resolved to no prepared inputs".to_string());
    }
    let mut merged = inputs.remove(0);
    for input in inputs {
        merged = merge_two_prepared_inputs(merged, input, density_merge)?;
    }
    Ok(merged)
}

fn merge_two_prepared_inputs(
    left: PreparedInput,
    right: PreparedInput,
    density_merge: CasaCubeBriggsDensityMerge,
) -> Result<PreparedInput, String> {
    match (left, right) {
        (PreparedInput::Mfs(mut left), PreparedInput::Mfs(right)) => {
            ensure_same_phase_center(&left.phase_center, &right.phase_center)?;
            if left.freq_ref != right.freq_ref {
                return Err(format!(
                    "multi-MS MFS inputs use different frequency frames: {:?} versus {:?}",
                    left.freq_ref, right.freq_ref
                ));
            }
            if left.plane_stokes != right.plane_stokes {
                return Err(format!(
                    "multi-MS MFS inputs use different imaging planes: {:?} versus {:?}",
                    left.plane_stokes, right.plane_stokes
                ));
            }
            left.gridder_mode = merge_gridder_modes(
                left.gridder_mode,
                right.gridder_mode,
                left.batches.len(),
                right.batches.len(),
            )?;
            left.selected_frequency_range_hz = [
                left.selected_frequency_range_hz[0].min(right.selected_frequency_range_hz[0]),
                left.selected_frequency_range_hz[1].max(right.selected_frequency_range_hz[1]),
            ];
            left.batches.extend(right.batches);
            left.sample_frequency_batches_hz
                .extend(right.sample_frequency_batches_hz);
            Ok(PreparedInput::Mfs(left))
        }
        (PreparedInput::Cube(mut left), PreparedInput::Cube(right)) => {
            ensure_same_phase_center(&left.phase_center, &right.phase_center)?;
            if left.freq_ref != right.freq_ref {
                return Err(format!(
                    "multi-MS cube inputs use different frequency frames: {:?} versus {:?}",
                    left.freq_ref, right.freq_ref
                ));
            }
            if left.plane_stokes != right.plane_stokes {
                return Err(format!(
                    "multi-MS cube inputs use different imaging planes: {:?} versus {:?}",
                    left.plane_stokes, right.plane_stokes
                ));
            }
            if left.channels.len() != right.channels.len() {
                return Err(format!(
                    "multi-MS cube inputs resolved to different channel counts: {} versus {}",
                    left.channels.len(),
                    right.channels.len()
                ));
            }
            if left.gridder_modes.len() != right.gridder_modes.len() {
                return Err(format!(
                    "multi-MS cube inputs resolved to different gridder-mode counts: {} versus {}",
                    left.gridder_modes.len(),
                    right.gridder_modes.len()
                ));
            }
            left.casa_cube_briggs_preweighting = merge_casa_cube_briggs_preweighting(
                left.casa_cube_briggs_preweighting,
                right.casa_cube_briggs_preweighting,
                density_merge,
            )?;
            let left_visibility_batch_counts = left
                .channels
                .iter()
                .map(|channel| channel.visibility_batches.len())
                .collect::<Vec<_>>();
            let right_visibility_batch_counts = right
                .channels
                .iter()
                .map(|channel| channel.visibility_batches.len())
                .collect::<Vec<_>>();
            for (gridder_index, (left_mode, right_mode)) in left
                .gridder_modes
                .iter_mut()
                .zip(right.gridder_modes)
                .enumerate()
            {
                *left_mode = merge_gridder_modes(
                    left_mode.clone(),
                    right_mode,
                    left_visibility_batch_counts[gridder_index],
                    right_visibility_batch_counts[gridder_index],
                )?;
            }
            for (channel_index, (left_channel, right_channel)) in
                left.channels.iter_mut().zip(right.channels).enumerate()
            {
                if !frequencies_close(
                    left_channel.channel_frequency_hz,
                    right_channel.channel_frequency_hz,
                ) {
                    return Err(format!(
                        "multi-MS cube channel {channel_index} frequency differs: {} Hz versus {} Hz",
                        left_channel.channel_frequency_hz, right_channel.channel_frequency_hz
                    ));
                }
                left_channel
                    .visibility_batches
                    .extend(right_channel.visibility_batches);
                left_channel
                    .density_batches
                    .extend(right_channel.density_batches);
                left_channel
                    .model_interpolation_batches
                    .extend(right_channel.model_interpolation_batches);
            }
            Ok(PreparedInput::Cube(left))
        }
        (PreparedInput::Mfs(_), PreparedInput::Cube(_))
        | (PreparedInput::Cube(_), PreparedInput::Mfs(_)) => {
            Err("multi-MS inputs resolved to mixed MFS and cube preparations".to_string())
        }
    }
}

fn merge_casa_cube_briggs_preweighting(
    left: Option<CasaCubeBriggsPreparedWeighting>,
    right: Option<CasaCubeBriggsPreparedWeighting>,
    density_merge: CasaCubeBriggsDensityMerge,
) -> Result<Option<CasaCubeBriggsPreparedWeighting>, String> {
    match (left, right) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => {
            Err("multi-MS cube inputs disagree on CASA cube Briggs preweighting state".to_string())
        }
        (Some(mut left), Some(right)) => {
            if left.weighting != right.weighting {
                return Err(format!(
                    "multi-MS cube Briggs weighting differs: {:?} versus {:?}",
                    left.weighting, right.weighting
                ));
            }
            if !frequencies_close(left.fractional_bandwidth, right.fractional_bandwidth) {
                return Err(format!(
                    "multi-MS cube Briggs fractional bandwidth differs: {:.9e} versus {:.9e}",
                    left.fractional_bandwidth, right.fractional_bandwidth
                ));
            }
            if left.density_plane_count != right.density_plane_count {
                return Err(format!(
                    "multi-MS cube Briggs density-plane count per group differs: {} versus {}",
                    left.density_plane_count, right.density_plane_count
                ));
            }
            if left.channel_density_plane_indices.len() != right.channel_density_plane_indices.len()
            {
                return Err(format!(
                    "multi-MS cube Briggs lookup-channel count differs: {} versus {}",
                    left.channel_density_plane_indices.len(),
                    right.channel_density_plane_indices.len()
                ));
            }
            let mut right_density_index_remap = vec![None; right.density_batches.len()];
            for (right_group_index, right_field_id) in
                right.density_group_field_ids.iter().copied().enumerate()
            {
                let right_group_base = right_group_index * right.density_plane_count;
                let left_group_base = match density_merge {
                    CasaCubeBriggsDensityMerge::SameMeasurementSet => left
                        .density_group_field_ids
                        .iter()
                        .position(|known_field_id| *known_field_id == right_field_id)
                        .map(|group_index| group_index * left.density_plane_count)
                        .unwrap_or_else(|| {
                            let group_index = left.density_group_field_ids.len();
                            left.density_group_field_ids.push(right_field_id);
                            left.density_batches.extend(
                                (0..left.density_plane_count).map(|_| empty_visibility_batch(16)),
                            );
                            left.density_sample_rows
                                .extend((0..left.density_plane_count).map(|_| Vec::new()));
                            group_index * left.density_plane_count
                        }),
                    CasaCubeBriggsDensityMerge::DistinctMeasurementSet => {
                        let group_index = left.density_group_field_ids.len();
                        left.density_group_field_ids.push(right_field_id);
                        left.density_batches.extend(
                            (0..left.density_plane_count).map(|_| empty_visibility_batch(16)),
                        );
                        left.density_sample_rows
                            .extend((0..left.density_plane_count).map(|_| Vec::new()));
                        group_index * left.density_plane_count
                    }
                };
                for plane in 0..right.density_plane_count {
                    let right_index = right_group_base + plane;
                    let left_index = left_group_base + plane;
                    right_density_index_remap[right_index] = Some(left_index);
                }
            }
            for (right_index, right_batch) in right.density_batches.into_iter().enumerate() {
                let Some(left_index) = right_density_index_remap
                    .get(right_index)
                    .copied()
                    .flatten()
                else {
                    continue;
                };
                extend_visibility_batch(&mut left.density_batches[left_index], right_batch);
            }
            for (right_index, right_rows) in right.density_sample_rows.into_iter().enumerate() {
                let Some(left_index) = right_density_index_remap
                    .get(right_index)
                    .copied()
                    .flatten()
                else {
                    continue;
                };
                left.density_sample_rows[left_index].extend(right_rows);
            }
            for (left_indices, right_indices) in left
                .channel_density_plane_indices
                .iter_mut()
                .zip(right.channel_density_plane_indices)
            {
                left_indices.extend(right_indices.into_iter().map(|index| {
                    index.and_then(|index| right_density_index_remap.get(index).copied().flatten())
                }));
            }
            for (left_rows, right_rows) in left
                .channel_sample_rows
                .iter_mut()
                .zip(right.channel_sample_rows)
            {
                left_rows.extend(right_rows);
            }
            for (left_channels, right_channels) in left
                .channel_sample_source_channels
                .iter_mut()
                .zip(right.channel_sample_source_channels)
            {
                left_channels.extend(right_channels);
            }
            Ok(Some(left))
        }
    }
}

fn extend_visibility_batch(left: &mut VisibilityBatch, right: VisibilityBatch) {
    left.u_lambda.extend(right.u_lambda);
    left.v_lambda.extend(right.v_lambda);
    left.w_lambda.extend(right.w_lambda);
    left.weight.extend(right.weight);
    left.sumwt_factor.extend(right.sumwt_factor);
    left.gridable.extend(right.gridable);
    left.visibility.extend(right.visibility);
}

fn casa_cube_briggs_density_group_base(
    preweighting: &mut CasaCubeBriggsPreparedWeighting,
    field_id: usize,
) -> usize {
    if let Some(group_index) = preweighting
        .density_group_field_ids
        .iter()
        .position(|known_field_id| *known_field_id == field_id)
    {
        return group_index * preweighting.density_plane_count;
    }

    let group_index = preweighting.density_group_field_ids.len();
    preweighting.density_group_field_ids.push(field_id);
    preweighting
        .density_batches
        .extend((0..preweighting.density_plane_count).map(|_| empty_visibility_batch(16)));
    preweighting
        .density_sample_rows
        .extend((0..preweighting.density_plane_count).map(|_| Vec::new()));
    group_index * preweighting.density_plane_count
}

fn casa_cube_briggs_density_plane_index(
    preweighting: &CasaCubeBriggsPreparedWeighting,
    group_base: usize,
    output_channel: usize,
) -> Option<usize> {
    (output_channel < preweighting.density_plane_count)
        .then_some(group_base + output_channel)
        .filter(|index| *index < preweighting.density_batches.len())
}

fn merge_gridder_modes(
    left: GridderMode,
    right: GridderMode,
    left_batch_count: usize,
    right_batch_count: usize,
) -> Result<GridderMode, String> {
    match (left, right) {
        (GridderMode::Standard, GridderMode::Standard) => Ok(GridderMode::Standard),
        (GridderMode::Mosaic(mut left), GridderMode::Mosaic(right)) => {
            if left.phase_center_direction_rad != right.phase_center_direction_rad
                || left.pb_limit != right.pb_limit
            {
                return Err(
                    "multi-MS mosaic inputs resolved to incompatible gridder metadata".to_string(),
                );
            }
            if left.metadata_batches.len() != left_batch_count {
                return Err(format!(
                    "left mosaic metadata batch count {} does not match visibility batch count {left_batch_count}",
                    left.metadata_batches.len()
                ));
            }
            if right.metadata_batches.len() != right_batch_count {
                return Err(format!(
                    "right mosaic metadata batch count {} does not match visibility batch count {right_batch_count}",
                    right.metadata_batches.len()
                ));
            }
            left.metadata_batches.extend(right.metadata_batches);
            Ok(GridderMode::Mosaic(left))
        }
        (GridderMode::Standard, GridderMode::Mosaic(_))
        | (GridderMode::Mosaic(_), GridderMode::Standard) => {
            Err("multi-MS inputs resolved to mixed standard and mosaic gridders".to_string())
        }
    }
}

fn ensure_same_phase_center(left: &PhaseCenter, right: &PhaseCenter) -> Result<(), String> {
    if left.reference != right.reference
        || !angles_close(left.angles_rad[0], right.angles_rad[0])
        || !angles_close(left.angles_rad[1], right.angles_rad[1])
    {
        return Err(format!(
            "multi-MS inputs resolved to different phase centers: {:?} versus {:?}",
            left, right
        ));
    }
    Ok(())
}

fn angles_close(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1.0e-12
}

fn frequencies_close(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= scale * 1.0e-10
}

enum RunProducts {
    Mfs(casa_imaging::ImagingResult),
    Mtmfs(casa_imaging::MtmfsResult),
    Cube(CubeRunProducts),
}

struct CubeRunProducts {
    result: casa_imaging::CubeImagingResult,
    mosaic_weight: Option<Array4<f32>>,
}

impl RunProducts {
    fn plane_stokes(&self) -> PlaneStokes {
        match self {
            Self::Mfs(result) => result.compatibility.plane_stokes,
            Self::Mtmfs(result) => result.compatibility.plane_stokes,
            Self::Cube(products) => products.result.compatibility.plane_stokes,
        }
    }

    fn channel_frequencies_hz(&self) -> &[f64] {
        match self {
            Self::Mfs(result) => &result.compatibility.channel_frequencies_hz,
            Self::Mtmfs(result) => &result.compatibility.channel_frequencies_hz,
            Self::Cube(products) => &products.result.compatibility.channel_frequencies_hz,
        }
    }

    fn warnings(&self) -> Vec<String> {
        match self {
            Self::Mfs(result) => result.diagnostics.warnings.clone(),
            Self::Mtmfs(result) => result.diagnostics.warnings.clone(),
            Self::Cube(products) => products.result.diagnostics.warnings.clone(),
        }
    }

    fn gridded_samples(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.gridded_samples,
            Self::Mtmfs(result) => result.diagnostics.gridded_samples,
            Self::Cube(products) => products.result.diagnostics.gridded_samples,
        }
    }

    fn major_cycles(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.major_cycles,
            Self::Mtmfs(result) => result.diagnostics.major_cycles,
            Self::Cube(products) => products.result.diagnostics.major_cycles,
        }
    }

    fn minor_iterations(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.minor_iterations,
            Self::Mtmfs(result) => result.diagnostics.minor_iterations,
            Self::Cube(products) => products.result.diagnostics.minor_iterations,
        }
    }

    fn clean_stop_reason(&self) -> Option<CleanStopReason> {
        match self {
            Self::Mfs(result) => result.diagnostics.clean_stop_reason,
            Self::Mtmfs(result) => result.diagnostics.clean_stop_reason,
            Self::Cube(products) => products.result.diagnostics.clean_stop_reason,
        }
    }

    fn channel_summaries(&self) -> Vec<ChannelRunSummary> {
        match self {
            Self::Mfs(_) => Vec::new(),
            Self::Mtmfs(_) => Vec::new(),
            Self::Cube(products) => products
                .result
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
            Self::Cube(products) => products.result.diagnostics.stage_timings,
        }
    }
}

fn add_imaging_stage_timings(total: &mut ImagingStageTimings, next: ImagingStageTimings) {
    total.controller_overhead += next.controller_overhead;
    total.weighting += next.weighting;
    total.psf_grid += next.psf_grid;
    total.psf_fft += next.psf_fft;
    total.psf_normalize += next.psf_normalize;
    total.model_fft += next.model_fft;
    total.residual_degrid_grid += next.residual_degrid_grid;
    total.residual_fft += next.residual_fft;
    total.residual_normalize += next.residual_normalize;
    total.minor_cycle += next.minor_cycle;
    total.minor_cycle_solve += next.minor_cycle_solve;
    total.major_cycle_refresh += next.major_cycle_refresh;
    total.beam_fit += next.beam_fit;
    total.restore += next.restore;
    total.total += next.total;
}

fn trace_casa_cube_briggs_row(row: Option<usize>) -> bool {
    let Some(row) = row else {
        return false;
    };
    match std::env::var("CASA_RS_TRACE_CUBE_BRIGGS_ROW") {
        Ok(value) if value == "all" => true,
        Ok(value) => value
            .split([',', ';', ' ', '\n', '\t'])
            .filter_map(|part| part.trim().parse::<usize>().ok())
            .any(|target| target == row),
        Err(_) => matches!(
            row,
            31 | 36 | 44 | 46 | 50 | 53 | 74 | 1675 | 1680 | 1688 | 1690 | 1694 | 1697
        ),
    }
}

fn trace_casa_cube_briggs_sample(trace_sample_index: usize, row: Option<usize>) -> bool {
    matches!(
        std::env::var("CASA_RS_TRACE_RUST_WEIGHTING").as_deref(),
        Ok("all")
    ) || trace_sample_index < 180
        || trace_casa_cube_briggs_row(row)
}

fn trace_m100_row(row: usize) -> bool {
    match std::env::var("CASA_RS_TRACE_M100_ROW0") {
        Ok(value) if value == "all" => true,
        Ok(value) => value
            .split([',', ';', ' ', '\n', '\t'])
            .filter_map(|part| part.trim().parse::<usize>().ok())
            .any(|target| target == row),
        Err(_) => false,
    }
}

fn apply_casa_cube_briggs_preweighting(
    geometry: ImageGeometry,
    preweighting: &CasaCubeBriggsPreparedWeighting,
    channels: &mut [CubeChannelRequest],
) -> Result<(), String> {
    if channels.len() != preweighting.channel_density_plane_indices.len() {
        return Err(format!(
            "CASA cube Briggs lookup channel count {} does not match cube channel count {}",
            preweighting.channel_density_plane_indices.len(),
            channels.len()
        ));
    }
    let density_grids = preweighting
        .density_batches
        .iter()
        .map(|batch| build_casa_cube_briggs_density_grid(geometry, batch))
        .collect::<Vec<_>>();
    let f2_by_plane = density_grids
        .iter()
        .map(|density| casa_cube_briggs_f2(preweighting.weighting, density))
        .collect::<Vec<_>>();
    if std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some() {
        for (plane, (density, f2)) in density_grids.iter().zip(f2_by_plane.iter()).enumerate() {
            let density_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
            let density_max = density.iter().copied().fold(0.0f32, f32::max);
            let density_nonzero = density.iter().filter(|value| **value > 0.0).count();
            eprintln!(
                "CASA_RS_TRACE_RUST_WEIGHTING cube_briggs_preweight plane={plane} density_sum={density_sum:.12e} density_max={density_max:.12e} density_nonzero={density_nonzero} f2={f2:.12e}"
            );
        }
        for (plane, batch) in preweighting.density_batches.iter().enumerate() {
            for sample_index in 0..batch.len() {
                let row = preweighting
                    .density_sample_rows
                    .get(plane)
                    .and_then(|rows| rows.get(sample_index))
                    .copied();
                if sample_index >= 180 && !trace_casa_cube_briggs_row(row) {
                    continue;
                }
                let Some((x, y)) = casa_cube_briggs_gridft_density_cell(
                    geometry,
                    batch.u_lambda[sample_index],
                    batch.v_lambda[sample_index],
                ) else {
                    continue;
                };
                eprintln!(
                    "CASA_RS_TRACE_RUST_WEIGHTING cube_briggs_density_sample plane={plane} sample={sample_index} row={row:?} cell=({x},{y}) weight={:.12e}",
                    batch.weight[sample_index]
                );
            }
        }
    }

    for (channel_index, channel) in channels.iter_mut().enumerate() {
        let density_plane_indices = &preweighting.channel_density_plane_indices[channel_index];
        let mut sample_offset = 0usize;
        if channel.visibility_batches.len() != channel.density_batches.len() {
            return Err(format!(
                "CASA cube Briggs channel {channel_index} has {} visibility batches but {} density lookup batches",
                channel.visibility_batches.len(),
                channel.density_batches.len()
            ));
        }
        let sample_rows = preweighting
            .channel_sample_rows
            .get(channel_index)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let sample_source_channels = preweighting
            .channel_sample_source_channels
            .get(channel_index)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for (visibility_batch, lookup_batch) in channel
            .visibility_batches
            .iter_mut()
            .zip(channel.density_batches.iter())
        {
            if visibility_batch.len() != lookup_batch.len() {
                return Err(format!(
                    "CASA cube Briggs channel {channel_index} lookup length {} does not match visibility length {}",
                    lookup_batch.len(),
                    visibility_batch.len()
                ));
            }
            for sample_index in 0..visibility_batch.len() {
                let Some(density_plane) = density_plane_indices
                    .get(sample_offset + sample_index)
                    .copied()
                    .flatten()
                else {
                    visibility_batch.weight[sample_index] = 0.0;
                    continue;
                };
                let Some(density_grid) = density_grids.get(density_plane) else {
                    visibility_batch.weight[sample_index] = 0.0;
                    continue;
                };
                let Some((x, y)) = casa_cube_briggs_density_cell(
                    geometry,
                    lookup_batch.u_lambda[sample_index],
                    lookup_batch.v_lambda[sample_index],
                ) else {
                    visibility_batch.weight[sample_index] = 0.0;
                    continue;
                };
                let density = density_grid[(x, y)];
                let f2 = f2_by_plane.get(density_plane).copied().unwrap_or(0.0);
                let input_weight = visibility_batch.weight[sample_index];
                let trace_sample_index = sample_offset + sample_index;
                let trace_row = sample_rows.get(trace_sample_index).copied();
                let trace_source_channel = sample_source_channels
                    .get(trace_sample_index)
                    .copied()
                    .flatten();
                if !(density.is_finite()
                    && density > 0.0
                    && f2.is_finite()
                    && f2 > 0.0
                    && input_weight.is_finite()
                    && input_weight > 0.0)
                {
                    visibility_batch.weight[sample_index] = 0.0;
                    if std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some()
                        && trace_casa_cube_briggs_sample(trace_sample_index, trace_row)
                    {
                        eprintln!(
                            "CASA_RS_TRACE_RUST_WEIGHTING cube_briggs_preweight_sample channel={channel_index} sample={trace_sample_index} row={trace_row:?} source_channel={trace_source_channel:?} plane={density_plane} cell=({x},{y}) input_weight={input_weight:.12e} density={density:.12e} f2={f2:.12e} output_weight=0"
                        );
                    }
                    continue;
                }
                let taper_factor = match preweighting.weighting {
                    WeightingMode::BriggsBwTaper { .. } => {
                        casa_cube_briggs_bw_taper_uv_distance_factor(
                            geometry,
                            preweighting.fractional_bandwidth,
                            lookup_batch.u_lambda[sample_index],
                            lookup_batch.v_lambda[sample_index],
                        ) as f32
                    }
                    _ => 1.0,
                };
                let output_weight = input_weight / ((f2 * density) / taper_factor + 1.0);
                if std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some()
                    && trace_casa_cube_briggs_sample(trace_sample_index, trace_row)
                {
                    eprintln!(
                        "CASA_RS_TRACE_RUST_WEIGHTING cube_briggs_preweight_sample channel={channel_index} sample={trace_sample_index} row={trace_row:?} source_channel={trace_source_channel:?} plane={density_plane} cell=({x},{y}) input_weight={input_weight:.12e} density={density:.12e} f2={f2:.12e} taper={taper_factor:.12e} output_weight={output_weight:.12e}"
                    );
                }
                visibility_batch.weight[sample_index] = output_weight;
            }
            sample_offset += visibility_batch.len();
        }
        if sample_offset != density_plane_indices.len() {
            return Err(format!(
                "CASA cube Briggs channel {channel_index} has {sample_offset} visibility samples but {} density-plane lookups",
                density_plane_indices.len()
            ));
        }
        channel.density_batches.clear();
    }
    Ok(())
}

fn build_casa_cube_briggs_density_grid(
    geometry: ImageGeometry,
    batch: &VisibilityBatch,
) -> Array2<f32> {
    let [nx, ny] = geometry.image_shape;
    let mut density = Array2::<f32>::zeros((nx, ny));
    for sample_index in 0..batch.len() {
        if !batch.gridable[sample_index] {
            continue;
        }
        let weight = batch.weight[sample_index];
        if !(weight.is_finite() && weight > 0.0) {
            continue;
        }
        if let Some((x, y)) = casa_cube_briggs_gridft_density_cell(
            geometry,
            batch.u_lambda[sample_index],
            batch.v_lambda[sample_index],
        ) {
            density[(x, y)] += weight;
            let mirror_x = nx.saturating_sub(x);
            let mirror_y = ny.saturating_sub(y);
            if mirror_x < nx && mirror_y < ny {
                density[(mirror_x, mirror_y)] += weight;
            }
        }
    }
    density
}

fn casa_cube_briggs_f2(weighting: WeightingMode, density: &Array2<f32>) -> f32 {
    let robust = match weighting {
        WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => robust,
        _ => return 0.0,
    };
    let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
    let sumlocwt = density
        .iter()
        .filter(|value| **value > 0.0)
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if sumlocwt > 0.0 && density_weight_sum > 0.0 {
        ((5.0f64 * 10f64.powf(-(robust as f64))).powi(2) / (sumlocwt / density_weight_sum)) as f32
    } else {
        0.0
    }
}

fn casa_cube_briggs_density_cell(
    geometry: ImageGeometry,
    u_lambda: f64,
    v_lambda: f64,
) -> Option<(usize, usize)> {
    let nx_f32 = geometry.image_shape[0] as f32;
    let ny_f32 = geometry.image_shape[1] as f32;
    let x = ((u_lambda as f32) * nx_f32 * (geometry.cell_size_rad[0] as f32) + nx_f32 / 2.0).round()
        as isize;
    let y = (-(v_lambda as f32) * ny_f32 * (geometry.cell_size_rad[1] as f32) + ny_f32 / 2.0)
        .round() as isize;
    if x <= 0
        || y <= 0
        || x >= geometry.image_shape[0] as isize
        || y >= geometry.image_shape[1] as isize
    {
        return None;
    }
    Some((x as usize, y as usize))
}

fn casa_cube_briggs_gridft_density_cell(
    geometry: ImageGeometry,
    u_lambda: f64,
    v_lambda: f64,
) -> Option<(usize, usize)> {
    let nx = geometry.image_shape[0] as f64;
    let ny = geometry.image_shape[1] as f64;
    let x_loc = (u_lambda * nx * geometry.cell_size_rad[0] + nx / 2.0 + 1.0).round() as isize;
    let y_loc = (-v_lambda * ny * geometry.cell_size_rad[1] + ny / 2.0 + 1.0).round() as isize;
    let x = x_loc - 1;
    let y = y_loc - 1;
    if x <= 0
        || y <= 0
        || x >= geometry.image_shape[0] as isize
        || y >= geometry.image_shape[1] as isize
    {
        return None;
    }
    Some((x as usize, y as usize))
}

fn casa_cube_briggs_bw_taper_uv_distance_factor(
    geometry: ImageGeometry,
    fractional_bandwidth: f64,
    u_lambda: f64,
    v_lambda: f64,
) -> f64 {
    let nx = geometry.image_shape[0] as f64;
    let ny = geometry.image_shape[1] as f64;
    let u_cells = u_lambda * nx * geometry.cell_size_rad[0];
    let v_cells = v_lambda * ny * geometry.cell_size_rad[1];
    let n_cells_bw = fractional_bandwidth * (u_cells * u_cells + v_cells * v_cells).sqrt();
    let mut factor = n_cells_bw + 0.5;
    if factor < 1.5 {
        factor = (4.0 - n_cells_bw) / (4.0 - 2.0 * n_cells_bw);
    }
    factor.max(f64::MIN_POSITIVE)
}

fn run_frontend_cube(
    geometry: ImageGeometry,
    cube: CubePlaneInput,
    config: &CliConfig,
    clean: CleanConfig,
    clean_mask: Option<Array2<bool>>,
    channel_clean_mask: Option<Array4<bool>>,
) -> Result<CubeRunProducts, String> {
    if cube.gridder_modes.len() != cube.channels.len() {
        return Err(format!(
            "internal error: cube gridder mode count {} does not match channel count {}",
            cube.gridder_modes.len(),
            cube.channels.len()
        ));
    }
    let weight_density_mode = if config.per_channel_weight_density {
        WeightDensityMode::PerPlane
    } else {
        WeightDensityMode::Combined
    };
    let mut channels = cube.channels;
    let gridder_modes = cube.gridder_modes;
    let effective_weighting = if let Some(preweighting) = cube.casa_cube_briggs_preweighting {
        apply_casa_cube_briggs_preweighting(geometry, &preweighting, &mut channels)?;
        WeightingMode::Natural
    } else {
        config.weighting
    };
    let effective_weight_density_mode = if effective_weighting == WeightingMode::Natural {
        WeightDensityMode::Combined
    } else {
        weight_density_mode
    };
    if gridder_modes
        .iter()
        .all(|mode| matches!(mode, GridderMode::Standard))
    {
        let result = run_cube(&CubeImagingRequest {
            geometry,
            channels,
            plane_stokes: cube.plane_stokes,
            weighting: effective_weighting,
            weight_density_mode: effective_weight_density_mode,
            uv_taper: config.uv_taper,
            restoring_beam_mode: config.restoring_beam_mode,
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean,
            clean_mask,
            channel_clean_mask,
            auto_mask: (config.use_mask == CleanMaskMode::AutoMultiThreshold && clean.niter > 0)
                .then_some(config.auto_mask.into()),
            psf_cutoff: config.psf_cutoff,
            w_term_mode: config.w_term_mode,
            w_project_planes: config.w_project_planes,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .map_err(|error| error.to_string())?;
        return Ok(CubeRunProducts {
            result,
            mosaic_weight: None,
        });
    }
    if config.uv_taper.is_some() {
        return Err("mosaic cube frontend path does not yet support uv taper".to_string());
    }
    if effective_weighting != WeightingMode::Natural
        && weight_density_mode != WeightDensityMode::PerPlane
    {
        return Err(
            "mosaic cube frontend path currently requires --perchanweightdensity".to_string(),
        );
    }

    let started = Instant::now();
    let [nx, ny] = geometry.image_shape;
    let nchan = channels.len();
    let clean_masks_by_channel = frontend_channel_clean_masks(
        geometry,
        nchan,
        clean_mask.as_ref(),
        channel_clean_mask.as_ref(),
    )?;
    let channel_frequencies_hz = channels
        .iter()
        .map(|channel| channel.channel_frequency_hz)
        .collect::<Vec<_>>();
    let min_freq_hz = channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let max_freq_hz = channel_frequencies_hz
        .iter()
        .copied()
        .fold(0.0f64, f64::max);
    let reffreq_hz = 0.5 * (channel_frequencies_hz[0] + channel_frequencies_hz[nchan - 1]);

    let mut psf = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut residual = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut model = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut image = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut sumwt = Array4::<f32>::zeros((1, 1, 1, nchan));
    let mut weight = Array4::<f32>::zeros((nx, ny, 1, nchan));
    let mut has_mosaic_weight = false;
    let mut beams = Vec::with_capacity(nchan);
    let mut diagnostics = Vec::with_capacity(nchan);
    let mut warnings = Vec::new();
    let mut stage_timings = ImagingStageTimings::default();
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;
    let mut major_cycles = 0usize;
    let mut minor_iterations = 0usize;
    let clean_threshold_override = if clean.niter > 0 {
        let mut dirty_seeds = Vec::with_capacity(nchan);
        let mut global_peak = 0.0f32;
        let mut max_psf_sidelobe = 0.0f32;
        for (channel_index, (channel, gridder_mode)) in channels
            .iter()
            .zip(gridder_modes.iter().cloned())
            .enumerate()
        {
            let channel_clean_mask = clean_masks_by_channel[channel_index].clone();
            let request = ImagingRequest {
                geometry,
                visibility_batches: channel.visibility_batches.clone(),
                gridder_mode,
                plane_stokes: cube.plane_stokes,
                weighting: effective_weighting,
                reffreq_hz: channel.channel_frequency_hz,
                selected_frequency_range_hz: [min_freq_hz, max_freq_hz],
                deconvolver: config.deconvolver,
                multiscale_scales: config.multiscale_scales.clone(),
                small_scale_bias: config.small_scale_bias,
                clean: frontend_dirty_clean_config(clean.psf_cutoff),
                clean_mask: channel_clean_mask.clone(),
                initial_model: None,
                w_term_mode: config.w_term_mode,
                w_project_planes: config.w_project_planes,
                compatibility: CompatibilityMode::CasaStandardMfs,
            };
            match run_imaging(&request) {
                Ok(plane) => {
                    let plane_peak =
                        frontend_peak_abs_masked(&plane.residual, channel_clean_mask.as_ref());
                    global_peak = global_peak.max(plane_peak);
                    max_psf_sidelobe =
                        max_psf_sidelobe.max(plane.diagnostics.max_psf_sidelobe_level);
                    dirty_seeds.push(FrontendCubePlaneSeed::Plane(Box::new(plane)));
                }
                Err(ImagingError::NoUsableSamples) => {
                    dirty_seeds.push(FrontendCubePlaneSeed::Blank(format!(
                        "channel {channel_index}: no usable visibility samples remain after validation and flagging; writing blank cube plane"
                    )));
                }
                Err(error) => {
                    return Err(format!(
                        "run dirty mosaic cube channel {channel_index} at {:.9e} Hz: {error}",
                        channel.channel_frequency_hz
                    ));
                }
            }
        }
        let threshold = frontend_cycle_threshold(global_peak, max_psf_sidelobe, clean);
        if frontend_progress_enabled() {
            eprintln!(
                "frontend-cube-clean dirty_global_peak_jy_per_beam={global_peak:.9e} max_psf_sidelobe={max_psf_sidelobe:.9e} cycle_threshold_jy_per_beam={threshold:.9e}",
            );
        }
        Some((threshold, dirty_seeds))
    } else {
        None
    };

    let mut clean_threshold_override = clean_threshold_override;
    for (channel_index, (channel, gridder_mode)) in
        channels.into_iter().zip(gridder_modes).enumerate()
    {
        let dirty_seed = clean_threshold_override.as_mut().map(|(_, seeds)| {
            std::mem::replace(
                &mut seeds[channel_index],
                FrontendCubePlaneSeed::Blank(String::new()),
            )
        });
        if let Some(FrontendCubePlaneSeed::Blank(warning)) = dirty_seed {
            warnings.push(warning.clone());
            beams.push(None);
            diagnostics.push(blank_frontend_cube_channel_diagnostics(
                &ImagingRequest {
                    geometry,
                    visibility_batches: channel.visibility_batches,
                    gridder_mode,
                    plane_stokes: cube.plane_stokes,
                    weighting: effective_weighting,
                    reffreq_hz: channel.channel_frequency_hz,
                    selected_frequency_range_hz: [min_freq_hz, max_freq_hz],
                    deconvolver: config.deconvolver,
                    multiscale_scales: config.multiscale_scales.clone(),
                    small_scale_bias: config.small_scale_bias,
                    clean,
                    clean_mask: clean_masks_by_channel[channel_index].clone(),
                    initial_model: None,
                    w_term_mode: config.w_term_mode,
                    w_project_planes: config.w_project_planes,
                    compatibility: CompatibilityMode::CasaStandardMfs,
                },
                warning,
            ));
            continue;
        }
        let request = ImagingRequest {
            geometry,
            visibility_batches: channel.visibility_batches,
            gridder_mode,
            plane_stokes: cube.plane_stokes,
            weighting: effective_weighting,
            reffreq_hz: channel.channel_frequency_hz,
            selected_frequency_range_hz: [min_freq_hz, max_freq_hz],
            deconvolver: config.deconvolver,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            clean,
            clean_mask: clean_masks_by_channel[channel_index].clone(),
            initial_model: None,
            w_term_mode: config.w_term_mode,
            w_project_planes: config.w_project_planes,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let plane = if let Some((threshold, _)) = clean_threshold_override.as_ref() {
            if let Some(FrontendCubePlaneSeed::Plane(dirty_plane)) = dirty_seed {
                let dirty_peak = frontend_peak_abs_masked(
                    &dirty_plane.residual,
                    clean_masks_by_channel[channel_index].as_ref(),
                );
                if dirty_peak <= *threshold {
                    if frontend_progress_enabled() {
                        eprintln!(
                            "frontend-cube-clean channel={channel_index} dirty_peak_jy_per_beam={dirty_peak:.9e} skipped_below_cycle_threshold={threshold:.9e}",
                        );
                    }
                    *dirty_plane
                } else {
                    let mut clean_request = request.clone();
                    clean_request.clean =
                        frontend_cube_channel_clean_config(clean_request.clean, *threshold);
                    match run_imaging(&clean_request) {
                        Ok(plane) => {
                            if frontend_progress_enabled() {
                                eprintln!(
                                    "frontend-cube-clean channel={channel_index} dirty_peak_jy_per_beam={dirty_peak:.9e} minor_iterations={} stop={:?}",
                                    plane.diagnostics.minor_iterations,
                                    plane.diagnostics.clean_stop_reason,
                                );
                            }
                            plane
                        }
                        Err(ImagingError::NoUsableSamples) => {
                            let warning = format!(
                                "channel {channel_index}: no usable visibility samples remain after validation and flagging; writing blank cube plane"
                            );
                            warnings.push(warning.clone());
                            beams.push(None);
                            diagnostics
                                .push(blank_frontend_cube_channel_diagnostics(&request, warning));
                            continue;
                        }
                        Err(error) => {
                            return Err(format!(
                                "run mosaic cube channel {channel_index} at {:.9e} Hz: {error}",
                                channel.channel_frequency_hz
                            ));
                        }
                    }
                }
            } else {
                unreachable!("mosaic cube clean threshold requires a dirty seed")
            }
        } else {
            match run_imaging(&request) {
                Ok(plane) => plane,
                Err(ImagingError::NoUsableSamples) => {
                    let warning = format!(
                        "channel {channel_index}: no usable visibility samples remain after validation and flagging; writing blank cube plane"
                    );
                    warnings.push(warning.clone());
                    beams.push(None);
                    diagnostics.push(blank_frontend_cube_channel_diagnostics(&request, warning));
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "run mosaic cube channel {channel_index} at {:.9e} Hz: {error}",
                        channel.channel_frequency_hz
                    ));
                }
            }
        };
        psf.slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.psf.slice(s![.., .., 0, 0]));
        residual
            .slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.residual.slice(s![.., .., 0, 0]));
        model
            .slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.model.slice(s![.., .., 0, 0]));
        image
            .slice_mut(s![.., .., 0, channel_index])
            .assign(&plane.image.slice(s![.., .., 0, 0]));
        sumwt[(0, 0, 0, channel_index)] = plane.sumwt[(0, 0, 0, 0)];
        if let Some(weight_image) = plane.diagnostics.mosaic_weight_image.as_ref() {
            weight
                .slice_mut(s![.., .., 0, channel_index])
                .assign(weight_image);
            has_mosaic_weight = true;
        }
        for warning in &plane.diagnostics.warnings {
            warnings.push(format!("channel {channel_index}: {warning}"));
        }
        gridded_samples += plane.diagnostics.gridded_samples;
        skipped_samples += plane.diagnostics.skipped_samples;
        major_cycles = major_cycles.max(plane.diagnostics.major_cycles);
        minor_iterations += plane.diagnostics.minor_iterations;
        add_imaging_stage_timings(&mut stage_timings, plane.diagnostics.stage_timings);
        beams.push(plane.beam);
        diagnostics.push(plane.diagnostics);
    }
    let restored_beams = select_frontend_restored_cube_beams(&beams, config.restoring_beam_mode)?;
    if config.restoring_beam_mode == RestoringBeamMode::Common {
        for channel_index in 0..nchan {
            let restored_beam = restored_beams[channel_index];
            let fitted_beam = beams[channel_index];
            let model_plane = model.slice(s![.., .., 0, channel_index]).to_owned();
            let residual_plane = residual.slice(s![.., .., 0, channel_index]).to_owned();
            let restored_model =
                restore_frontend_model(&model_plane, geometry.cell_size_rad, restored_beam);
            let residual_to_add = match (restored_beam, fitted_beam) {
                (Some(restored_beam), Some(fitted_beam)) => rescale_frontend_residual_to_beam(
                    &residual_plane,
                    geometry.cell_size_rad,
                    restored_beam,
                    fitted_beam,
                )
                .map_err(|error| {
                    format!(
                        "rescale mosaic cube residual to common beam for channel {channel_index}: {error}"
                    )
                })?,
                _ => residual_plane,
            };
            image
                .slice_mut(s![.., .., 0, channel_index])
                .assign(&(&restored_model + &residual_to_add));
        }
    }
    stage_timings.total = started.elapsed();

    Ok(CubeRunProducts {
        result: CubeImagingResult {
            psf,
            residual,
            model,
            image,
            sumwt,
            clean_mask: final_cube_clean_mask_from_channel_masks(&clean_masks_by_channel, nx, ny),
            restored_beams,
            beams,
            diagnostics: CubeImagingDiagnostics {
                warnings,
                gridded_samples,
                skipped_samples,
                major_cycles,
                minor_iterations,
                clean_stop_reason: None,
                channel_diagnostics: diagnostics,
                stage_timings,
            },
            compatibility: CompatibilityMetadata {
                axis_order: [
                    AxisKind::RightAscension,
                    AxisKind::Declination,
                    AxisKind::Stokes,
                    AxisKind::Frequency,
                ],
                plane_stokes: cube.plane_stokes,
                reffreq_hz,
                channel_frequencies_hz,
                psf_units: String::new(),
                residual_units: "Jy/beam".to_string(),
                model_units: "Jy/pixel".to_string(),
                image_units: "Jy/beam".to_string(),
            },
        },
        mosaic_weight: has_mosaic_weight.then_some(weight),
    })
}

fn blank_frontend_cube_channel_diagnostics(
    request: &ImagingRequest,
    warning: String,
) -> ImagingDiagnostics {
    ImagingDiagnostics {
        warnings: vec![warning],
        gridded_samples: 0,
        skipped_samples: 0,
        major_cycles: 0,
        minor_iterations: 0,
        clean_stop_reason: None,
        minor_cycle_traces: Vec::new(),
        initial_residual_peak_jy_per_beam: 0.0,
        final_residual_peak_jy_per_beam: 0.0,
        max_abs_w_lambda: 0.0,
        fractional_bandwidth: 0.0,
        max_psf_sidelobe_level: 0.0,
        final_cycle_threshold_jy_per_beam: request.clean.threshold_jy_per_beam,
        clean_mask_pixels: request
            .clean_mask
            .as_ref()
            .map(|mask| mask.iter().filter(|value| **value).count())
            .unwrap_or(request.geometry.nx() * request.geometry.ny()),
        beam_fit_attempts: 0,
        beam_fit_cutoff_used: Some(request.clean.psf_cutoff),
        beam_fit_debug: None,
        mosaic_weight_image: None,
        stage_timings: ImagingStageTimings::default(),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SelectedMainArrayColumn {
    column_name: &'static str,
    values: Vec<Option<ArrayValue>>,
}

impl SelectedMainArrayColumn {
    fn load(
        ms: &MeasurementSet,
        column_name: &'static str,
        row_indices: &[usize],
    ) -> Result<Self, String> {
        let values = ms
            .main_table()
            .column_accessor(column_name)
            .and_then(|column| column.array_cells_owned(row_indices))
            .map_err(|error| format!("load selected {column_name} rows: {error}"))?;
        Ok(Self {
            column_name,
            values,
        })
    }

    fn get(&self, row_slot: usize) -> Result<&ArrayValue, String> {
        self.values
            .get(row_slot)
            .and_then(|value| value.as_ref())
            .ok_or_else(|| {
                format!(
                    "{} data missing for selected row slot {}",
                    self.column_name, row_slot
                )
            })
    }

    fn get_optional(&self, row_slot: usize) -> Result<Option<&ArrayValue>, String> {
        self.values
            .get(row_slot)
            .map(|value| value.as_ref())
            .ok_or_else(|| {
                format!(
                    "{} selected row slot {} is out of bounds",
                    self.column_name, row_slot
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SelectedMainDataSource {
    Single(SelectedMainArrayColumn),
}

impl SelectedMainDataSource {
    fn load(
        ms: &MeasurementSet,
        column: VisibilityDataColumn,
        row_indices: &[usize],
    ) -> Result<Self, String> {
        let column_name = match column {
            VisibilityDataColumn::Data => "DATA",
            VisibilityDataColumn::CorrectedData => "CORRECTED_DATA",
            VisibilityDataColumn::ModelData => "MODEL_DATA",
        };
        Ok(Self::Single(SelectedMainArrayColumn::load(
            ms,
            column_name,
            row_indices,
        )?))
    }

    fn get(&self, row_slot: usize) -> Result<&ArrayValue, String> {
        match self {
            Self::Single(column) => column.get(row_slot),
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
                .cell_accessor(row_index, "ANTENNA_ID")
                .and_then(|cell| cell.scalar())
                .map_err(|error| format!("read POINTING.ANTENNA_ID row {row_index}: {error}"))?
            {
                &ScalarValue::Int32(value) => value,
                other => {
                    return Err(format!(
                        "POINTING.ANTENNA_ID row {row_index} must be Int32, found {:?}",
                        other.primitive_type()
                    ));
                }
            };
            let time_mjd_seconds = match table
                .cell_accessor(row_index, "TIME")
                .and_then(|cell| cell.scalar())
                .map_err(|error| format!("read POINTING.TIME row {row_index}: {error}"))?
            {
                &ScalarValue::Float64(value) => value,
                other => {
                    return Err(format!(
                        "POINTING.TIME row {row_index} must be Float64, found {:?}",
                        other.primitive_type()
                    ));
                }
            };
            let interval_seconds = match table
                .cell_accessor(row_index, "INTERVAL")
                .and_then(|cell| cell.scalar())
                .map_err(|error| format!("read POINTING.INTERVAL row {row_index}: {error}"))?
            {
                &ScalarValue::Float64(value) => value,
                other => {
                    return Err(format!(
                        "POINTING.INTERVAL row {row_index} must be Float64, found {:?}",
                        other.primitive_type()
                    ));
                }
            };
            let angles_rad = extract_constant_direction_angles(
                table
                    .cell_accessor(row_index, "DIRECTION")
                    .and_then(|cell| cell.array())
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
        for entry in [lower.checked_sub(1), Some(lower)]
            .into_iter()
            .flatten()
            .filter_map(|candidate_index| entries.get(candidate_index).copied())
        {
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
    let select_started_at = Instant::now();
    let needs_pointing_times = config.use_pointing
        && ms
            .pointing()
            .map(|pointing| pointing.row_count() > 0)
            .unwrap_or(false);
    let needs_row_times = matches!(config.spectral_mode, SpectralMode::Mfs)
        || config.spectral_mode.is_cube_like()
        || config.w_term_mode != WTermMode::None
        || selection_may_require_phase_reprojection(config)
        || needs_pointing_times;
    let field_values = load_i32_main_column_owned(ms, "FIELD_ID")?;
    let ddid_values = load_i32_main_column_owned(ms, "DATA_DESC_ID")?;
    maybe_log_frontend_progress(
        "prepare_plane_input/select_main_rows/load_scalar_columns",
        select_started_at.elapsed(),
        select_started_at.elapsed(),
    );
    let allowed_ddids = allowed_ddids(config, ddid_info)?;
    let time_values = if needs_row_times {
        Some(load_f64_main_column_owned(ms, "TIME")?)
    } else {
        None
    };
    let allowed_field_ids = config
        .field_ids
        .as_ref()
        .map(|ids| ids.iter().copied().collect::<BTreeSet<_>>());
    let mut selected_fields = BTreeSet::<i32>::new();
    let mut selected_ddid = None::<i32>;
    let mut selected_rows = Vec::<SelectedMainRow>::new();
    let mut reference_row_time_mjd_sec = None::<f64>;
    let mut time_bounds_mjd_sec = None::<[f64; 2]>;

    for (row, (&field_id, &ddid)) in field_values.iter().zip(ddid_values.iter()).enumerate() {
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
            let row_time_mjd_sec = *time_values
                .as_ref()
                .and_then(|values| values.get(row))
                .ok_or_else(|| format!("TIME row {row} is missing"))?;
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
    maybe_log_frontend_progress(
        "prepare_plane_input/select_main_rows/scan_rows",
        select_started_at.elapsed(),
        select_started_at.elapsed(),
    );

    if selected_fields.is_empty() {
        return Err("selection resolved to no field".to_string());
    }
    if selected_rows.is_empty() {
        return Err("selection resolved to no rows".to_string());
    }
    let phase_center = resolve_phase_center(ms, &selected_fields, config)?;
    maybe_log_frontend_progress(
        "prepare_plane_input/select_main_rows/resolve_phase_center",
        select_started_at.elapsed(),
        select_started_at.elapsed(),
    );
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

fn load_i32_main_column_owned(
    ms: &MeasurementSet,
    column_name: &'static str,
) -> Result<Vec<i32>, String> {
    let values = ms
        .main_table()
        .column_accessor(column_name)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|error| format!("load {column_name} column: {error}"))?;
    if values.len() != ms.main_table().row_count() {
        return Err(format!(
            "{column_name} length {} does not match MAIN row count {}",
            values.len(),
            ms.main_table().row_count()
        ));
    }
    values
        .into_iter()
        .enumerate()
        .map(|(row, value)| match value {
            Some(ScalarValue::Int32(value)) => Ok(value),
            Some(other) => Err(format!(
                "{column_name} row {row} must be Int32, found {:?}",
                other.primitive_type()
            )),
            None => Err(format!("{column_name} row {row} is missing")),
        })
        .collect()
}

fn load_f64_main_column_owned(
    ms: &MeasurementSet,
    column_name: &'static str,
) -> Result<Vec<f64>, String> {
    let values = ms
        .main_table()
        .column_accessor(column_name)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|error| format!("load {column_name} column: {error}"))?;
    if values.len() != ms.main_table().row_count() {
        return Err(format!(
            "{column_name} length {} does not match MAIN row count {}",
            values.len(),
            ms.main_table().row_count()
        ));
    }
    values
        .into_iter()
        .enumerate()
        .map(|(row, value)| match value {
            Some(ScalarValue::Float64(value)) => Ok(value),
            Some(ScalarValue::Float32(value)) => Ok(value as f64),
            Some(other) => Err(format!(
                "{column_name} row {row} must be Float64, found {:?}",
                other.primitive_type()
            )),
            None => Err(format!("{column_name} row {row} is missing")),
        })
        .collect()
}

fn load_bool_main_column_owned(
    ms: &MeasurementSet,
    column_name: &'static str,
) -> Result<Vec<bool>, String> {
    let values = ms
        .main_table()
        .column_accessor(column_name)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|error| format!("load {column_name} column: {error}"))?;
    if values.len() != ms.main_table().row_count() {
        return Err(format!(
            "{column_name} length {} does not match MAIN row count {}",
            values.len(),
            ms.main_table().row_count()
        ));
    }
    values
        .into_iter()
        .enumerate()
        .map(|(row, value)| match value {
            Some(ScalarValue::Bool(value)) => Ok(value),
            Some(other) => Err(format!(
                "{column_name} row {row} must be Bool, found {:?}",
                other.primitive_type()
            )),
            None => Err(format!("{column_name} row {row} is missing")),
        })
        .collect()
}

fn load_optional_i32_main_column(
    ms: &MeasurementSet,
    column_name: &'static str,
) -> Result<Option<Vec<Option<i32>>>, String> {
    let Ok(column) = ms
        .main_table()
        .column_accessor(column_name)
        .and_then(|column| column.iter())
    else {
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

fn extract_uvw_from_array(value: &ArrayValue, row_index: usize) -> Result<[f64; 3], String> {
    match value {
        ArrayValue::Float64(values) => {
            let slice = values
                .as_slice()
                .ok_or_else(|| format!("UVW row {row_index} must be contiguous Float64[3] data"))?;
            if slice.len() != 3 {
                return Err(format!(
                    "UVW row {row_index} must have shape [3], found length {}",
                    slice.len()
                ));
            }
            Ok([slice[0], slice[1], slice[2]])
        }
        other => Err(format!(
            "UVW row {row_index} must be Float64 array, found {:?}",
            other.primitive_type()
        )),
    }
}

fn build_prepared_geometry_rows(
    ms: &MeasurementSet,
    selected_rows: &[SelectedMainRow],
    phase_center: &PhaseCenter,
    derived_engine: Option<&MsCalEngine>,
    use_pointing: bool,
    reprojection_mode: UvwReprojectionMode,
) -> Result<Vec<PreparedGeometryRow>, String> {
    let geometry_started_at = Instant::now();
    let antenna1 = load_i32_main_column_owned(ms, "ANTENNA1")?;
    let antenna2 = load_i32_main_column_owned(ms, "ANTENNA2")?;
    maybe_log_frontend_progress(
        "prepare_plane_input/build_prepared_geometry_rows/load_antenna_ids",
        geometry_started_at.elapsed(),
        geometry_started_at.elapsed(),
    );
    let selected_row_indices = selected_rows
        .iter()
        .map(|selected_row| selected_row.row_index)
        .collect::<Vec<_>>();
    let selected_uvw = SelectedMainArrayColumn::load(ms, "UVW", &selected_row_indices)?;
    maybe_log_frontend_progress(
        "prepare_plane_input/build_prepared_geometry_rows/load_selected_uvw",
        geometry_started_at.elapsed(),
        geometry_started_at.elapsed(),
    );
    let pointing_ids = if use_pointing {
        load_optional_i32_main_column(ms, "POINTING_ID")?
    } else {
        None
    };
    maybe_log_frontend_progress(
        "prepare_plane_input/build_prepared_geometry_rows/load_pointing_ids",
        geometry_started_at.elapsed(),
        geometry_started_at.elapsed(),
    );
    let pointing_resolver = if use_pointing {
        PointingDirectionResolver::new(ms)?
    } else {
        None
    };
    maybe_log_frontend_progress(
        "prepare_plane_input/build_prepared_geometry_rows/build_pointing_resolver",
        geometry_started_at.elapsed(),
        geometry_started_at.elapsed(),
    );
    let mut field_phase_centers = BTreeMap::<usize, [f64; 2]>::new();
    let mut rows = Vec::with_capacity(selected_rows.len());
    for (row_slot, selected_row) in selected_rows.iter().enumerate() {
        let row = selected_row.row_index;
        let antenna1_id = *antenna1
            .get(row)
            .ok_or_else(|| format!("read ANTENNA1 row {row}: row is out of bounds"))?;
        let antenna2_id = *antenna2
            .get(row)
            .ok_or_else(|| format!("read ANTENNA2 row {row}: row is out of bounds"))?;
        let is_cross = antenna1_id != antenna2_id;
        let raw_uvw_m = extract_uvw_from_array(selected_uvw.get(row_slot)?, row)?;
        let transform = row_imaging_transform(
            row,
            selected_row.field_id,
            phase_center,
            raw_uvw_m,
            derived_engine,
            reprojection_mode,
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
    maybe_log_frontend_progress(
        "prepare_plane_input/build_prepared_geometry_rows/row_loop",
        geometry_started_at.elapsed(),
        geometry_started_at.elapsed(),
    );
    Ok(rows)
}

fn prepare_plane_input(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
) -> Result<PreparedInput, String> {
    prepare_plane_input_inner(ms, config, data_column_kind, false).map(|(prepared, _)| prepared)
}

fn prepare_plane_input_with_trace(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
) -> Result<(PreparedInput, PreparedVisibilityTraceBundle), String> {
    let (prepared, trace) = prepare_plane_input_inner(ms, config, data_column_kind, true)?;
    trace
        .map(|trace| (prepared, trace))
        .ok_or_else(|| "internal error: requested prepare trace was not built".to_string())
}

fn prepare_inputs_for_measurement_set(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
) -> Result<Vec<PreparedInput>, String> {
    if has_explicit_spectral_selection(config) {
        return Ok(vec![prepare_plane_input(ms, config, data_column_kind)?]);
    }
    selected_data_desc_ids_for_unrestricted_spectral_selection(ms, config)?
        .into_iter()
        .map(|ddid| {
            let mut ddid_config = config.clone();
            ddid_config.ddid = Some(ddid);
            prepare_plane_input(ms, &ddid_config, data_column_kind)
        })
        .collect()
}

fn has_explicit_spectral_selection(config: &CliConfig) -> bool {
    config.ddid.is_some() || config.spw.is_some() || config.spw_selector.is_some()
}

fn selected_data_desc_ids_for_unrestricted_spectral_selection(
    ms: &MeasurementSet,
    config: &CliConfig,
) -> Result<Vec<i32>, String> {
    let field_values = load_i32_main_column_owned(ms, "FIELD_ID")?;
    let ddid_values = load_i32_main_column_owned(ms, "DATA_DESC_ID")?;
    let allowed_field_ids = config
        .field_ids
        .as_ref()
        .map(|ids| ids.iter().copied().collect::<BTreeSet<_>>());
    let mut selected_ddids = BTreeSet::<i32>::new();
    for (&field_id, &ddid) in field_values.iter().zip(ddid_values.iter()) {
        if ddid < 0 {
            continue;
        }
        if allowed_field_ids
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&field_id))
        {
            continue;
        }
        selected_ddids.insert(ddid);
    }
    if selected_ddids.is_empty() {
        return Err("selection resolved to no DDID".to_string());
    }
    Ok(selected_ddids.into_iter().collect())
}

fn prepare_plane_input_inner(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
    force_trace: bool,
) -> Result<(PreparedInput, Option<PreparedVisibilityTraceBundle>), String> {
    let prepare_started_at = Instant::now();
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
    let selection = select_main_rows(ms, config, &ddid_info)?;
    maybe_log_frontend_progress(
        "prepare_plane_input/select_main_rows",
        prepare_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let flag_row = load_bool_main_column_owned(ms, "FLAG_ROW")?;
    maybe_log_frontend_progress(
        "prepare_plane_input/load_flag_row_column",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let rows_skipped_by_flag_row = selection
        .selected_rows
        .iter()
        .filter(|selected_row| {
            flag_row
                .get(selected_row.row_index)
                .copied()
                .unwrap_or(false)
        })
        .count();
    let active_selected_rows = selection
        .selected_rows
        .iter()
        .filter(|selected_row| {
            flag_row
                .get(selected_row.row_index)
                .copied()
                .map(|flagged| !flagged)
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    let selected_row_indices = active_selected_rows
        .iter()
        .map(|selected_row| selected_row.row_index)
        .collect::<Vec<_>>();
    let stage_started_at = Instant::now();
    let data_column = SelectedMainDataSource::load(ms, data_column_kind, &selected_row_indices)?;
    maybe_log_frontend_progress(
        "prepare_plane_input/load_data_column",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let flag_column = SelectedMainArrayColumn::load(ms, "FLAG", &selected_row_indices)?;
    maybe_log_frontend_progress(
        "prepare_plane_input/load_flag_column",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let weight_column = SelectedMainArrayColumn::load(ms, "WEIGHT", &selected_row_indices)?;
    maybe_log_frontend_progress(
        "prepare_plane_input/load_weight_column",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let weight_spectrum = WeightSpectrumColumn::new(ms.main_table())
        .ok()
        .map(|_| SelectedMainArrayColumn::load(ms, "WEIGHT_SPECTRUM", &selected_row_indices))
        .transpose()?;
    maybe_log_frontend_progress(
        "prepare_plane_input/load_weight_spectrum_column",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let selected_spw_id = ddid_info
        .get(selection.selected_ddid)
        .copied()
        .flatten()
        .map(|(spw_id, _)| spw_id)
        .ok_or_else(|| {
            format!(
                "map selected DDID {} to SPW/POLARIZATION",
                selection.selected_ddid
            )
        })?;
    let selected_freq_ref = FrequencyRef::from_casacore_code(
        spectral_window
            .meas_freq_ref(selected_spw_id)
            .map_err(|error| format!("read MEAS_FREQ_REF: {error}"))?,
    )
    .unwrap_or(FrequencyRef::TOPO);
    let mfs_needs_frequency_conversion = matches!(config.spectral_mode, SpectralMode::Mfs)
        && selected_freq_ref != FrequencyRef::LSRK;
    let derived_engine = if selection.needs_geometry_engine || mfs_needs_frequency_conversion {
        Some(MsCalEngine::new(ms).map_err(|error| format!("build derived engine: {error}"))?)
    } else {
        None
    };
    maybe_log_frontend_progress(
        "prepare_plane_input/build_derived_engine",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let geometry_rows = build_prepared_geometry_rows(
        ms,
        &active_selected_rows,
        &selection.phase_center,
        derived_engine.as_ref(),
        config.use_pointing,
        uvw_reprojection_mode_for_selection(config, &selection),
    )?;
    maybe_log_frontend_progress(
        "prepare_plane_input/build_prepared_geometry_rows",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    let cube_context = if config.spectral_mode.is_cube_like() {
        let spectral_frame_field_id = selection
            .phase_center
            .field_id
            .unwrap_or_else(|| selection.selected_rows[0].field_id);
        Some(CubeSetupContext {
            spectral_frame_field_id,
            phase_center_direction: (selection.phase_center.field_id.is_none()).then(|| {
                MDirection::from_angles(
                    selection.phase_center.angles_rad[0],
                    selection.phase_center.angles_rad[1],
                    selection.phase_center.reference,
                )
            }),
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
    let fast_standard_mfs =
        !force_trace && can_prepare_standard_mfs_without_trace(config, &selection);
    let trace_free_cube = !force_trace
        && config.spectral_mode.is_cube_like()
        && !config.use_pointing
        && config.phasecenter_field.is_none()
        && config.phasecenter.is_none();
    let build_trace = force_trace
        || (matches!(config.spectral_mode, SpectralMode::Mfs) && !fast_standard_mfs)
        || (config.spectral_mode.is_cube_like() && !trace_free_cube);
    let mut prepared = PreparedSelection::new(
        config,
        selection.selected_ddid,
        &ddid_info,
        &spectral_window,
        &polarization,
        selection.phase_center.clone(),
        cube_context,
        build_trace,
    );
    if let Some(init_error) = prepared.initialization_error.take() {
        return Err(init_error);
    }
    let stage_started_at = Instant::now();
    let mut accumulate_timings = AccumulateRowTimings {
        rows_skipped_by_flag_row,
        ..Default::default()
    };
    for (row_slot, row) in geometry_rows.iter().enumerate() {
        prepared.accumulate_row(
            row,
            &data_column,
            &flag_column,
            &flag_row,
            &weight_column,
            weight_spectrum.as_ref(),
            derived_engine.as_ref(),
            row_slot,
            &mut accumulate_timings,
        )?;
        if frontend_progress_enabled()
            && (row_slot + 1 == geometry_rows.len() || (row_slot + 1) % 1_000 == 0)
        {
            eprintln!(
                "frontend stage=prepare_plane_input/accumulate_rows/progress rows_done={} rows_total={} total_elapsed_s={:.3}",
                row_slot + 1,
                geometry_rows.len(),
                prepare_started_at.elapsed().as_secs_f64(),
            );
        }
    }
    accumulate_timings.log(prepare_started_at.elapsed());
    maybe_log_frontend_progress(
        "prepare_plane_input/accumulate_rows",
        stage_started_at.elapsed(),
        prepare_started_at.elapsed(),
    );
    if fast_standard_mfs {
        let stage_started_at = Instant::now();
        let prepared_input = prepared.finish_standard_mfs_without_trace()?;
        maybe_log_frontend_progress(
            "prepare_plane_input/finish_standard_mfs_without_trace",
            stage_started_at.elapsed(),
            prepare_started_at.elapsed(),
        );
        return Ok((prepared_input, None));
    }
    if trace_free_cube {
        let stage_started_at = Instant::now();
        let prepared_input = prepared.finish_cube_without_trace()?;
        maybe_log_frontend_progress(
            "prepare_plane_input/finish_cube_without_trace",
            stage_started_at.elapsed(),
            prepare_started_at.elapsed(),
        );
        return Ok((prepared_input, None));
    }
    let selected_row_traces = selection
        .selected_rows
        .iter()
        .map(SelectedMainRow::trace)
        .collect::<Vec<_>>();
    prepared
        .finish_with_trace(
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
        .map(|(prepared, trace)| (prepared, Some(trace)))
}

fn can_prepare_standard_mfs_without_trace(
    config: &CliConfig,
    selection: &SelectedRowsContext,
) -> bool {
    matches!(config.spectral_mode, SpectralMode::Mfs)
        && !config.use_pointing
        && config.deconvolver != Deconvolver::Mtmfs
        && selection.phase_center.field_id.is_some()
        && selection
            .selected_rows
            .iter()
            .all(|row| Some(row.field_id) == selection.phase_center.field_id)
}

fn uvw_reprojection_mode_for_selection(
    config: &CliConfig,
    selection: &SelectedRowsContext,
) -> UvwReprojectionMode {
    if config.use_pointing
        || selection.phase_center.field_id.is_none()
        || selection
            .selected_rows
            .iter()
            .any(|row| Some(row.field_id) != selection.phase_center.field_id)
    {
        UvwReprojectionMode::Mosaic
    } else {
        UvwReprojectionMode::Standard
    }
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
    if !longitude {
        if let Some(value) = parse_dms_token(text) {
            return Ok(value * std::f64::consts::PI / 180.0);
        }
    }
    if let Some(value) = parse_hms_token(text) {
        if longitude {
            return Ok(value * std::f64::consts::PI / 12.0);
        }
        return Err(format!(
            "phasecenter latitude {text:?} looks like a right ascension; expected a declination"
        ));
    }
    if longitude {
        if let Some(value) = parse_dms_token(text) {
            return Ok(value * std::f64::consts::PI / 180.0);
        }
    } else if let Some(value) = parse_dms_token(text) {
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

fn parse_save_model_mode(name: &str) -> Result<SaveModelMode, String> {
    match name.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
        "none" => Ok(SaveModelMode::None),
        "modelcolumn" => Ok(SaveModelMode::ModelColumn),
        _ => Err(format!(
            "unsupported savemodel value {name:?}; expected none or modelcolumn"
        )),
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
    cube_row_source_frequency_cache: HashMap<(u64, usize), Rc<Vec<f64>>>,
    mfs_frequency_scale_cache: HashMap<(u64, usize), f64>,
    casa_cube_grid_interpolation: bool,
    casa_cube_briggs_preweighting: Option<CasaCubeBriggsPreparedWeighting>,
    use_density_batches: bool,
    use_model_interpolation_batches: bool,
    mosaic_pb_limit: f32,
    phase_center: PhaseCenter,
    state: PreparedState,
    trace_state: PreparedTraceState,
    trace_enabled: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct AccumulateRowTimings {
    flag_row: Duration,
    data_column: Duration,
    flag_column: Duration,
    weight_column: Duration,
    weight_spectrum: Duration,
    adapt_samples: Duration,
    rows_seen: usize,
    rows_flagged: usize,
    rows_skipped_by_flag_row: usize,
}

impl AccumulateRowTimings {
    fn log(self, total_elapsed: Duration) {
        if frontend_progress_enabled() {
            eprintln!(
                "frontend stage=prepare_plane_input/accumulate_rows/detail rows_seen={} rows_flagged={} rows_skipped_by_flag_row={} flag_row_ms={:.3} data_ms={:.3} flag_ms={:.3} weight_ms={:.3} weight_spectrum_ms={:.3} adapt_samples_ms={:.3} total_elapsed_s={:.3}",
                self.rows_seen,
                self.rows_flagged,
                self.rows_skipped_by_flag_row,
                self.flag_row.as_secs_f64() * 1_000.0,
                self.data_column.as_secs_f64() * 1_000.0,
                self.flag_column.as_secs_f64() * 1_000.0,
                self.weight_column.as_secs_f64() * 1_000.0,
                self.weight_spectrum.as_secs_f64() * 1_000.0,
                self.adapt_samples.as_secs_f64() * 1_000.0,
                total_elapsed.as_secs_f64(),
            );
        }
    }
}

#[derive(Clone)]
struct CubeSetupContext<'a> {
    spectral_frame_field_id: usize,
    phase_center_direction: Option<MDirection>,
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
enum UvwReprojectionMode {
    Standard,
    Mosaic,
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
        channel_density_batches: Vec<VisibilityBatch>,
        channel_model_interpolation_samples: Vec<Vec<Vec<CubeModelChannelContribution>>>,
    },
    PairedMfs {
        plane_stokes: PlaneStokes,
        transform: PairCollapseTransform,
        paired: ParallelHandBatch,
        pair: (usize, usize),
    },
    CollapsedMfs {
        plane_stokes: PlaneStokes,
        transform: PairCollapseTransform,
        pair: (usize, usize),
        batch: VisibilityBatch,
    },
    PairedCube {
        plane_stokes: PlaneStokes,
        transform: PairCollapseTransform,
        channel_batches: Vec<ParallelHandBatch>,
        channel_density_batches: Vec<VisibilityBatch>,
        channel_model_interpolation_samples: Vec<Vec<Vec<CubeModelChannelContribution>>>,
        pair: (usize, usize),
    },
    CollapsedCube {
        plane_stokes: PlaneStokes,
        transform: PairCollapseTransform,
        pair: (usize, usize),
        channel_batches: Vec<VisibilityBatch>,
        channel_density_batches: Vec<VisibilityBatch>,
        channel_model_interpolation_samples: Vec<Vec<Vec<CubeModelChannelContribution>>>,
    },
}

fn row_imaging_transform(
    row: usize,
    row_field_id: usize,
    phase_center: &PhaseCenter,
    raw_uvw_m: [f64; 3],
    derived_engine: Option<&MsCalEngine>,
    reprojection_mode: UvwReprojectionMode,
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
            reprojection_mode,
        )?
    } else {
        reproject_row_uvw_to_phase_center(
            row,
            raw_uvw_m,
            derived_engine,
            row_field_id,
            phase_center,
            reprojection_mode,
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
    reprojection_mode: UvwReprojectionMode,
) -> Result<([f64; 3], f64), String> {
    let derived_engine = derived_engine
        .ok_or_else(|| "internal error: missing derived engine for row reprojection".to_string())?;
    let result = match reprojection_mode {
        UvwReprojectionMode::Standard => derived_engine.reproject_raw_uvw_between_fields(
            raw_uvw_m,
            source_field_id,
            target_field_id,
        ),
        UvwReprojectionMode::Mosaic => derived_engine.reproject_raw_uvw_for_mosaic_between_fields(
            raw_uvw_m,
            source_field_id,
            target_field_id,
        ),
    };
    result.map_err(|error| format!("reproject UVW row {row} between field phase centers: {error}"))
}

fn reproject_row_uvw_to_phase_center(
    row: usize,
    raw_uvw_m: [f64; 3],
    derived_engine: Option<&MsCalEngine>,
    source_field_id: usize,
    phase_center: &PhaseCenter,
    reprojection_mode: UvwReprojectionMode,
) -> Result<([f64; 3], f64), String> {
    let derived_engine = derived_engine
        .ok_or_else(|| "internal error: missing derived engine for row reprojection".to_string())?;
    let target = MDirection::from_angles(
        phase_center.angles_rad[0],
        phase_center.angles_rad[1],
        phase_center.reference,
    );
    let result =
        match reprojection_mode {
            UvwReprojectionMode::Standard => {
                derived_engine.reproject_raw_uvw_to_direction(raw_uvw_m, source_field_id, &target)
            }
            UvwReprojectionMode::Mosaic => derived_engine
                .reproject_raw_uvw_for_mosaic_to_direction(raw_uvw_m, source_field_id, &target),
        };
    result.map_err(|error| format!("reproject UVW row {row} to explicit phase center: {error}"))
}

fn gridft_density_uvw_m(
    raw_uvw_m: [f64; 3],
    source_direction_rad: [f64; 2],
    target_direction_rad: [f64; 2],
) -> [f64; 3] {
    let casa_input_uvw_m = [-raw_uvw_m[0], -raw_uvw_m[1], raw_uvw_m[2]];
    let rot = gridft_uvw_rotation_matrix(source_direction_rad, target_direction_rad);
    let casa_output_uvw_m = row_vec3_mul_mat3(casa_input_uvw_m, rot);
    [
        -casa_output_uvw_m[0],
        -casa_output_uvw_m[1],
        casa_output_uvw_m[2],
    ]
}

fn gridft_uvw_rotation_matrix(
    source_direction_rad: [f64; 2],
    target_direction_rad: [f64; 2],
) -> [[f64; 3]; 3] {
    let [source_ra, source_dec] = source_direction_rad;
    let [target_ra, target_dec] = target_direction_rad;
    let rot1 = gridft_euler_rotation(&[
        (-(std::f64::consts::FRAC_PI_2 - source_ra), GridftAxis::Z),
        (source_dec - std::f64::consts::FRAC_PI_2, GridftAxis::X),
    ]);
    let rot3 = gridft_euler_rotation(&[
        (std::f64::consts::FRAC_PI_2 - target_dec, GridftAxis::X),
        (-(target_ra - std::f64::consts::FRAC_PI_2), GridftAxis::Z),
    ]);
    transpose3(mat3_mul_mat3(rot3, rot1))
}

fn row_vec3_mul_mat3(row: [f64; 3], matrix: [[f64; 3]; 3]) -> [f64; 3] {
    [
        row[0] * matrix[0][0] + row[1] * matrix[1][0] + row[2] * matrix[2][0],
        row[0] * matrix[0][1] + row[1] * matrix[1][1] + row[2] * matrix[2][1],
        row[0] * matrix[0][2] + row[1] * matrix[1][2] + row[2] * matrix[2][2],
    ]
}

fn identity3() -> [[f64; 3]; 3] {
    [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
}

fn mat3_mul_mat3(left: [[f64; 3]; 3], right: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut out = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            out[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    out
}

fn transpose3(matrix: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    [
        [matrix[0][0], matrix[1][0], matrix[2][0]],
        [matrix[0][1], matrix[1][1], matrix[2][1]],
        [matrix[0][2], matrix[1][2], matrix[2][2]],
    ]
}

#[derive(Clone, Copy)]
enum GridftAxis {
    X,
    Z,
}

fn gridft_euler_rotation(operations: &[(f64, GridftAxis)]) -> [[f64; 3]; 3] {
    let mut matrix = identity3();
    for (angle, axis) in operations {
        matrix = mat3_mul_mat3(matrix, gridft_axis_rotation(*angle, *axis));
    }
    matrix
}

fn gridft_axis_rotation(angle: f64, axis: GridftAxis) -> [[f64; 3]; 3] {
    let (sin_angle, cos_angle) = angle.sin_cos();
    match axis {
        GridftAxis::X => [
            [1.0, 0.0, 0.0],
            [0.0, cos_angle, -sin_angle],
            [0.0, sin_angle, cos_angle],
        ],
        GridftAxis::Z => [
            [cos_angle, -sin_angle, 0.0],
            [sin_angle, cos_angle, 0.0],
            [0.0, 0.0, 1.0],
        ],
    }
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

fn mfs_imaging_frequency_scale(
    freq_ref: FrequencyRef,
    reference_frequency_hz: f64,
    selected_row: &SelectedMainRow,
    derived_engine: Option<&MsCalEngine>,
) -> Result<f64, String> {
    if freq_ref == FrequencyRef::LSRK {
        return Ok(1.0);
    }
    let row_time_mjd_sec = selected_row.time_mjd_seconds.ok_or_else(|| {
        "internal error: missing row time for MFS frequency-frame conversion".to_string()
    })?;
    let derived_engine = derived_engine.ok_or_else(|| {
        "internal error: missing derived engine for MFS frequency-frame conversion".to_string()
    })?;
    convert_frequency_to_frame(
        freq_ref,
        FrequencyRef::LSRK,
        reference_frequency_hz,
        row_time_mjd_sec,
        selected_row.field_id,
        derived_engine,
    )
    .map(|converted_hz| converted_hz / reference_frequency_hz)
    .map_err(|error| error.to_string())
}

impl PreparedSelection {
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: &CliConfig,
        ddid: usize,
        ddid_info: &[Option<(usize, usize)>],
        spectral_window: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
        polarization: &casa_ms::subtables::polarization::MsPolarization<'_>,
        phase_center: PhaseCenter,
        cube_context: Option<CubeSetupContext<'_>>,
        trace_enabled: bool,
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
                    cube_context.spectral_frame_field_id,
                    cube_context.phase_center_direction.clone(),
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
            if std::env::var_os("CASA_RS_TRACE_CUBE_GRID_INTERP").is_some() {
                eprintln!(
                    "CASA_RS_TRACE_CUBE_GRID_INTERP prepared_selection spw_id={spw_id} source_channels={} output_channels={} casa_grid_interp={}",
                    source_channel_selection.indices.len(),
                    output_channel_frequencies_hz.len(),
                    config.per_channel_weight_density
                        && matches!(
                            config.weighting,
                            WeightingMode::Briggs { .. } | WeightingMode::BriggsBwTaper { .. }
                        )
                );
            }
            let output_freq_ref = cube_spectral_setup
                .as_ref()
                .map(|setup| setup.output_freq_ref)
                .unwrap_or(freq_ref);
            let use_density_batches = config.weighting != WeightingMode::Natural;
            let use_model_interpolation_batches = !(config.dirty_only || config.niter == 0);
            let selected_frequency_range_hz = frequency_range_hz(&output_channel_frequencies_hz)?;
            let reffreq_hz =
                0.5 * (selected_frequency_range_hz[0] + selected_frequency_range_hz[1]);
            let casa_cube_briggs_preweighting = (config.spectral_mode.is_cube_like()
                && config.per_channel_weight_density
                && output_channel_frequencies_hz.len() > 1
                && matches!(
                    config.weighting,
                    WeightingMode::Briggs { .. } | WeightingMode::BriggsBwTaper { .. }
                ))
            .then(|| CasaCubeBriggsPreparedWeighting {
                weighting: config.weighting,
                fractional_bandwidth: fractional_bandwidth_from_range(selected_frequency_range_hz),
                density_plane_count: output_channel_frequencies_hz.len() + 4,
                density_group_field_ids: Vec::new(),
                density_batches: Vec::new(),
                density_sample_rows: Vec::new(),
                channel_density_plane_indices: output_channel_frequencies_hz
                    .iter()
                    .map(|_| Vec::new())
                    .collect(),
                channel_sample_rows: output_channel_frequencies_hz
                    .iter()
                    .map(|_| Vec::new())
                    .collect(),
                channel_sample_source_channels: output_channel_frequencies_hz
                    .iter()
                    .map(|_| Vec::new())
                    .collect(),
            });
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
                                channel_density_batches: output_channel_frequencies_hz
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
                        SpectralMode::Mfs if trace_enabled => PreparedState::PairedMfs {
                            plane_stokes,
                            transform,
                            pair,
                            paired: empty_parallel_hand_batch(max_samples),
                        },
                        SpectralMode::Mfs => PreparedState::CollapsedMfs {
                            plane_stokes,
                            transform,
                            pair,
                            batch: empty_visibility_batch(max_samples),
                        },
                        SpectralMode::Cube | SpectralMode::Cubedata if trace_enabled => {
                            PreparedState::PairedCube {
                                plane_stokes,
                                transform,
                                pair,
                                channel_batches: output_channel_frequencies_hz
                                    .iter()
                                    .map(|_| empty_parallel_hand_batch(16))
                                    .collect(),
                                channel_density_batches: output_channel_frequencies_hz
                                    .iter()
                                    .map(|_| empty_visibility_batch(16))
                                    .collect(),
                                channel_model_interpolation_samples: output_channel_frequencies_hz
                                    .iter()
                                    .map(|_| Vec::new())
                                    .collect(),
                            }
                        }
                        SpectralMode::Cube | SpectralMode::Cubedata => {
                            PreparedState::CollapsedCube {
                                plane_stokes,
                                transform,
                                pair,
                                channel_batches: output_channel_frequencies_hz
                                    .iter()
                                    .map(|_| empty_visibility_batch(16))
                                    .collect(),
                                channel_density_batches: output_channel_frequencies_hz
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
                }
            } else {
                let (pair, transform) = derive_stokes_pair_selection(PlaneStokes::I, &corr_types)?;
                match config.spectral_mode {
                    SpectralMode::Mfs if trace_enabled => PreparedState::PairedMfs {
                        plane_stokes: PlaneStokes::I,
                        transform,
                        pair,
                        paired: empty_parallel_hand_batch(max_samples),
                    },
                    SpectralMode::Mfs => PreparedState::CollapsedMfs {
                        plane_stokes: PlaneStokes::I,
                        transform,
                        pair,
                        batch: empty_visibility_batch(max_samples),
                    },
                    SpectralMode::Cube | SpectralMode::Cubedata if trace_enabled => {
                        PreparedState::PairedCube {
                            plane_stokes: PlaneStokes::I,
                            transform,
                            pair,
                            channel_batches: output_channel_frequencies_hz
                                .iter()
                                .map(|_| empty_parallel_hand_batch(16))
                                .collect(),
                            channel_density_batches: output_channel_frequencies_hz
                                .iter()
                                .map(|_| empty_visibility_batch(16))
                                .collect(),
                            channel_model_interpolation_samples: output_channel_frequencies_hz
                                .iter()
                                .map(|_| Vec::new())
                                .collect(),
                        }
                    }
                    SpectralMode::Cube | SpectralMode::Cubedata => PreparedState::CollapsedCube {
                        plane_stokes: PlaneStokes::I,
                        transform,
                        pair,
                        channel_batches: output_channel_frequencies_hz
                            .iter()
                            .map(|_| empty_visibility_batch(16))
                            .collect(),
                        channel_density_batches: output_channel_frequencies_hz
                            .iter()
                            .map(|_| empty_visibility_batch(16))
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
                cube_row_source_frequency_cache: HashMap::new(),
                mfs_frequency_scale_cache: HashMap::new(),
                casa_cube_grid_interpolation: config.per_channel_weight_density,
                casa_cube_briggs_preweighting,
                use_density_batches,
                use_model_interpolation_batches,
                mosaic_pb_limit: config.mosaic_pb_limit,
                phase_center,
                state,
                trace_state,
                trace_enabled,
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
                cube_row_source_frequency_cache: HashMap::new(),
                mfs_frequency_scale_cache: HashMap::new(),
                casa_cube_grid_interpolation: false,
                casa_cube_briggs_preweighting: None,
                use_density_batches: false,
                use_model_interpolation_batches: false,
                mosaic_pb_limit: config.mosaic_pb_limit,
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
                trace_enabled,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn accumulate_row(
        &mut self,
        geometry_row: &PreparedGeometryRow,
        data_column: &SelectedMainDataSource,
        flag_column: &SelectedMainArrayColumn,
        flag_row: &[bool],
        weight_column: &SelectedMainArrayColumn,
        weight_spectrum: Option<&SelectedMainArrayColumn>,
        derived_engine: Option<&MsCalEngine>,
        row_slot: usize,
        timings: &mut AccumulateRowTimings,
    ) -> Result<(), String> {
        timings.rows_seen += 1;
        let selected_row = &geometry_row.selected_row;
        let row = selected_row.row_index;
        let stage_started_at = Instant::now();
        if *flag_row
            .get(row)
            .ok_or_else(|| format!("read FLAG_ROW row {row}: row is out of bounds"))?
        {
            timings.flag_row += stage_started_at.elapsed();
            timings.rows_flagged += 1;
            return Ok(());
        }
        timings.flag_row += stage_started_at.elapsed();
        let stage_started_at = Instant::now();
        let data = data_column
            .get(row_slot)
            .map_err(|error| format!("read data row {row}: {error}"))?;
        timings.data_column += stage_started_at.elapsed();
        let stage_started_at = Instant::now();
        let flags = flag_column
            .get(row_slot)
            .map_err(|error| format!("read FLAG row {row}: {error}"))?;
        timings.flag_column += stage_started_at.elapsed();
        let stage_started_at = Instant::now();
        let row_weights = weight_column
            .get(row_slot)
            .map_err(|error| format!("read WEIGHT row {row}: {error}"))?;
        timings.weight_column += stage_started_at.elapsed();
        let stage_started_at = Instant::now();
        let weight_spectrum_row = weight_spectrum
            .map(|column| column.get_optional(row_slot))
            .transpose()
            .map(|row| row.flatten())
            .map_err(|error| format!("read WEIGHT_SPECTRUM row {row}: {error}"))?;
        timings.weight_spectrum += stage_started_at.elapsed();
        let adapt_started_at = Instant::now();
        let data_2d = ComplexRow2d::new(data)?;
        let flags_2d = BoolRow2d::new(flags)?;
        let weights = WeightRow::new(row_weights, weight_spectrum_row)?;
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
            PreparedState::ExplicitCube { .. }
                | PreparedState::PairedCube { .. }
                | PreparedState::CollapsedCube { .. }
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
                if std::env::var_os("CASA_RS_TRACE_CUBE_GRID_INTERP").is_some() {
                    let nonempty_output = computed
                        .output_channel_contributions
                        .iter()
                        .filter(|contributions| !contributions.is_empty())
                        .count();
                    let grid_samples = computed.grid_channel_contributions.len();
                    let padded_grid_samples = computed.padded_grid_channel_contributions.len();
                    let mut grid_per_output =
                        vec![0usize; cube_setup.output_channel_frequencies_hz.len()];
                    for grid in &computed.grid_channel_contributions {
                        if let Some(slot) = grid_per_output.get_mut(grid.output_channel) {
                            *slot += 1;
                        }
                    }
                    eprintln!(
                        "CASA_RS_TRACE_CUBE_GRID_INTERP row_spectral row={row} field={} nonempty_output={nonempty_output} grid_samples={grid_samples} padded_grid_samples={padded_grid_samples} grid_per_output={grid_per_output:?}",
                        selected_row.field_id
                    );
                    if row < 100 {
                        eprintln!(
                            "CASA_RS_TRACE_CUBE_GRID_INTERP row_spectral_maps row={row} source_map={:?} padded_source_map={:?} grid_freqs={:?} padded_grid_freqs={:?}",
                            computed.source_channel_output_map,
                            computed.padded_source_channel_output_map,
                            computed
                                .grid_channel_contributions
                                .iter()
                                .map(|grid| (grid.output_channel, grid.grid_frequency_hz))
                                .collect::<Vec<_>>(),
                            computed
                                .padded_grid_channel_contributions
                                .iter()
                                .map(|grid| (grid.output_channel, grid.grid_frequency_hz))
                                .collect::<Vec<_>>()
                        );
                    }
                }
                self.cube_row_spectral_cache
                    .insert(cache_key, Rc::clone(&computed));
                Some(computed)
            }
        } else {
            None
        };
        let cube_row_source_frequencies_for_interpolation =
            if self.casa_cube_grid_interpolation && cube_row_spectral_contributions.is_some() {
                let cube_setup = self
                    .cube_spectral_setup
                    .as_ref()
                    .ok_or_else(|| "internal error: missing cube spectral setup".to_string())?;
                let derived_engine = derived_engine.ok_or_else(|| {
                    "internal error: missing derived engine for cube imaging".to_string()
                })?;
                let row_time_mjd_sec = selected_row.time_mjd_seconds.ok_or_else(|| {
                    "internal error: missing row time for cube imaging".to_string()
                })?;
                let cache_key = (row_time_mjd_sec.to_bits(), selected_row.field_id);
                if let Some(cached) = self.cube_row_source_frequency_cache.get(&cache_key) {
                    Some(Rc::clone(cached))
                } else {
                    let computed = Rc::new(
                        cube_setup
                            .row_source_frequencies_for_interpolation(
                                &self.source_channel_frequencies_hz,
                                row_time_mjd_sec,
                                selected_row.field_id,
                                derived_engine,
                            )
                            .map_err(|error| error.to_string())?,
                    );
                    self.cube_row_source_frequency_cache
                        .insert(cache_key, Rc::clone(&computed));
                    Some(computed)
                }
            } else {
                None
            };
        let cube_row_source_frequencies_for_interpolation =
            cube_row_source_frequencies_for_interpolation
                .as_deref()
                .map(Vec::as_slice);
        if std::env::var_os("CASA_RS_TRACE_CUBE_GRID_INTERP").is_some()
            && trace_m100_row(selected_row.row_index)
        {
            if let Some(frequencies) = cube_row_source_frequencies_for_interpolation {
                let start = frequencies.len().min(6);
                let end = frequencies.len().min(20);
                eprintln!(
                    "CASA_RS_TRACE_CUBE_GRID_INTERP source_interp_freqs row={} slots_{}_{end}={:?}",
                    selected_row.row_index,
                    start,
                    frequencies[start..end]
                        .iter()
                        .enumerate()
                        .map(|(offset, freq)| (start + offset, *freq))
                        .collect::<Vec<_>>()
                );
            }
        }
        let trace_enabled = self.trace_enabled;
        let use_casa_cube_grid_interpolation = self.casa_cube_grid_interpolation;
        let use_density_batches = self.use_density_batches;
        let use_model_interpolation_batches = self.use_model_interpolation_batches;
        let mfs_frequency_scale = if matches!(
            &self.state,
            PreparedState::ExplicitMfs { .. }
                | PreparedState::PairedMfs { .. }
                | PreparedState::CollapsedMfs { .. }
        ) {
            self.mfs_imaging_frequency_scale_for_row(selected_row, derived_engine)?
        } else {
            1.0
        };
        let casa_cube_briggs_density_uvw_m = gridft_density_uvw_m(
            raw_uvw_m,
            geometry_row.field_phase_center_direction_rad,
            self.phase_center.angles_rad,
        );
        let casa_cube_briggs_lookup_uvw_m = raw_uvw_m;
        if std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some()
            && trace_casa_cube_briggs_row(Some(row))
        {
            let mosaic_transform = (self.phase_center.field_id.is_none())
                .then(|| {
                    reproject_row_uvw_to_phase_center(
                        row,
                        raw_uvw_m,
                        derived_engine,
                        selected_row.field_id,
                        &self.phase_center,
                        UvwReprojectionMode::Mosaic,
                    )
                    .ok()
                })
                .flatten();
            eprintln!(
                "CASA_RS_TRACE_RUST_WEIGHTING cube_briggs_row_geometry row={row} field={} phase_center_field={:?} raw_uvw_m=({:.12e},{:.12e},{:.12e}) imaging_uvw_m=({:.12e},{:.12e},{:.12e}) gridft_density_uvw_m=({:.12e},{:.12e},{:.12e}) phase_shift_m={:.12e} mosaic_uvw_m={:?}",
                selected_row.field_id,
                self.phase_center.field_id,
                raw_uvw_m[0],
                raw_uvw_m[1],
                raw_uvw_m[2],
                uvw_m[0],
                uvw_m[1],
                uvw_m[2],
                casa_cube_briggs_density_uvw_m[0],
                casa_cube_briggs_density_uvw_m[1],
                casa_cube_briggs_density_uvw_m[2],
                transform.phase_shift_m,
                mosaic_transform.map(|(uvw, _)| uvw)
            );
        }
        let casa_cube_briggs_preweighting = &mut self.casa_cube_briggs_preweighting;

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
                if trace_enabled {
                    samples.reserve(self.source_channel_frequencies_hz.len());
                }
                for (channel_slot, (channel_index, frequency_hz)) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                    .enumerate()
                {
                    let imaging_frequency_hz = frequency_hz * mfs_frequency_scale;
                    if flags_2d.get(*corr_index, channel_index)? {
                        continue;
                    }
                    let visibility = phase_rotate_visibility(
                        data_2d.get(*corr_index, channel_index)?,
                        transform.phase_shift_m,
                        imaging_frequency_hz,
                    );
                    let (weight, weight_source) = weights.get(*corr_index, channel_index)?;
                    if !(weight.is_finite() && weight > 0.0) {
                        continue;
                    }
                    let lambda_scale = imaging_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    batch.weight.push(weight);
                    batch.sumwt_factor.push(1.0);
                    batch.gridable.push(is_cross);
                    batch.visibility.push(visibility);
                    if trace_enabled {
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
                            output_frequency_hz: imaging_frequency_hz,
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
            }
            (
                PreparedState::ExplicitCube {
                    corr_index,
                    channel_batches,
                    channel_density_batches,
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
                let density_slot_offset = (self
                    .source_channel_indices
                    .len()
                    .saturating_sub(channel_density_batches.len()))
                    / 2;
                if use_density_batches && !use_casa_cube_grid_interpolation {
                    for (output_channel, density_batch) in
                        channel_density_batches.iter_mut().enumerate()
                    {
                        let source_slot = if use_casa_cube_grid_interpolation {
                            output_channel + density_slot_offset
                        } else {
                            match row_spectral_contributions
                                .source_channel_output_map
                                .iter()
                                .position(|mapped| *mapped == Some(output_channel))
                            {
                                Some(source_slot) => source_slot,
                                None => continue,
                            }
                        };
                        if source_slot >= self.source_channel_indices.len() {
                            continue;
                        }
                        push_explicit_cube_density_sample(
                            density_batch,
                            &flags_2d,
                            &weights,
                            *corr_index,
                            self.source_channel_indices[source_slot],
                            self.source_channel_frequencies_hz[source_slot],
                            uvw_m,
                            is_cross,
                        )?;
                    }
                }
                if let Some(preweighting) = casa_cube_briggs_preweighting.as_mut() {
                    let density_group_base =
                        casa_cube_briggs_density_group_base(preweighting, selected_row.field_id);
                    for grid in &row_spectral_contributions.padded_grid_channel_contributions {
                        let Some(sample) = interpolate_explicit_cube_output_sample(
                            data,
                            flags,
                            row_weights,
                            weight_spectrum_row,
                            *corr_index,
                            &self.source_channel_indices,
                            transform.phase_shift_m,
                            grid.grid_frequency_hz,
                            &grid.contributions,
                            false,
                        )?
                        else {
                            continue;
                        };
                        if let Some(density_plane) = casa_cube_briggs_density_plane_index(
                            preweighting,
                            density_group_base,
                            grid.output_channel,
                        ) {
                            let batch = &mut preweighting.density_batches[density_plane];
                            let before = batch.len();
                            push_casa_cube_briggs_density_sample(
                                batch,
                                grid.grid_frequency_hz,
                                casa_cube_briggs_density_uvw_m,
                                sample.weight,
                                is_cross,
                            );
                            if batch.len() > before {
                                if let Some(rows) =
                                    preweighting.density_sample_rows.get_mut(density_plane)
                                {
                                    rows.push(row);
                                }
                            }
                        }
                    }
                }
                let grid_assignments;
                let assignment_iter: Box<
                    dyn Iterator<Item = (usize, f64, &[CubeChannelContribution])> + '_,
                > = if use_casa_cube_grid_interpolation {
                    Box::new(
                        row_spectral_contributions
                            .grid_channel_contributions
                            .iter()
                            .map(|grid| {
                                (
                                    grid.output_channel,
                                    grid.grid_frequency_hz,
                                    grid.contributions.as_slice(),
                                )
                            }),
                    )
                } else {
                    grid_assignments = assignments
                        .iter()
                        .enumerate()
                        .map(|(output_channel, contributions)| {
                            (
                                output_channel,
                                cube_output_channel_frequencies_hz
                                    .as_ref()
                                    .expect("missing cube spectral setup")[output_channel],
                                contributions.as_slice(),
                            )
                        })
                        .collect::<Vec<_>>();
                    Box::new(grid_assignments.into_iter())
                };
                for (output_channel, output_frequency_hz, contributions) in assignment_iter {
                    if contributions.is_empty() {
                        continue;
                    }
                    let Some(sample) = interpolate_explicit_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *corr_index,
                        &self.source_channel_indices,
                        transform.phase_shift_m,
                        output_frequency_hz,
                        contributions,
                        use_casa_cube_grid_interpolation,
                    )?
                    else {
                        continue;
                    };
                    if !(output_frequency_hz.is_finite() && output_frequency_hz > 0.0) {
                        continue;
                    }
                    if use_density_batches && use_casa_cube_grid_interpolation {
                        push_explicit_cube_density_sample_from_contributions(
                            &mut channel_density_batches[output_channel],
                            &flags_2d,
                            &weights,
                            *corr_index,
                            &self.source_channel_indices,
                            cube_row_source_frequencies_for_interpolation,
                            contributions,
                            casa_cube_briggs_lookup_uvw_m,
                            is_cross,
                        )?;
                        if let Some(preweighting) = casa_cube_briggs_preweighting.as_mut() {
                            let density_group_base = casa_cube_briggs_density_group_base(
                                preweighting,
                                selected_row.field_id,
                            );
                            let density_contribution =
                                nearest_cube_density_contribution(contributions);
                            let density_plane = density_contribution.and_then(|contribution| {
                                row_spectral_contributions
                                    .padded_source_channel_output_map
                                    .get(contribution.source_channel)
                                    .copied()
                                    .flatten()
                                    .and_then(|output_channel| {
                                        casa_cube_briggs_density_plane_index(
                                            preweighting,
                                            density_group_base,
                                            output_channel,
                                        )
                                    })
                            });
                            let source_channel = density_contribution.and_then(|contribution| {
                                self.source_channel_indices
                                    .get(contribution.source_channel)
                                    .copied()
                            });
                            if let Some(indices) = preweighting
                                .channel_density_plane_indices
                                .get_mut(output_channel)
                            {
                                indices.push(density_plane);
                            }
                            if let Some(rows) =
                                preweighting.channel_sample_rows.get_mut(output_channel)
                            {
                                rows.push(row);
                            }
                            if let Some(channels) = preweighting
                                .channel_sample_source_channels
                                .get_mut(output_channel)
                            {
                                channels.push(source_channel);
                            }
                        }
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
                    if trace_m100_row(selected_row.row_index) {
                        eprintln!(
                            "CASA_RS_TRACE_M100_ROW0 explicit row={} corr={} output={} freq={:.17e} contributions={:?} visibility=({:.17e},{:.17e}) weight={:.17e} sumwt_factor={:.17e}",
                            selected_row.row_index,
                            corr_index,
                            output_channel,
                            output_frequency_hz,
                            contributions,
                            sample.visibility.re,
                            sample.visibility.im,
                            sample.weight,
                            sample.sumwt_factor,
                        );
                    }
                    if use_model_interpolation_batches {
                        channel_model_interpolation_samples[output_channel].push(
                            combine_model_channel_contributions(
                                contributions,
                                source_model_contributions,
                            ),
                        );
                    }
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
                PreparedState::CollapsedMfs {
                    plane_stokes,
                    transform: pair_transform,
                    pair,
                    batch,
                },
                PreparedTraceState::PairedMfs { .. },
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
                let sumwt_factor = reported_sumwt_factor_for_paired_plane(*plane_stokes);
                for (channel_index, frequency_hz) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                {
                    let imaging_frequency_hz = frequency_hz * mfs_frequency_scale;
                    let first_visibility = phase_rotate_visibility(
                        data_2d.get(pair.0, channel_index)?,
                        transform.phase_shift_m,
                        imaging_frequency_hz,
                    );
                    let second_visibility = phase_rotate_visibility(
                        data_2d.get(pair.1, channel_index)?,
                        transform.phase_shift_m,
                        imaging_frequency_hz,
                    );
                    let (first_weight, _) = weights.get(pair.0, channel_index)?;
                    let (second_weight, _) = weights.get(pair.1, channel_index)?;
                    let first_flagged = flags_2d.get(pair.0, channel_index)?;
                    let second_flagged = flags_2d.get(pair.1, channel_index)?;
                    if first_flagged || second_flagged {
                        continue;
                    }
                    if !(first_weight.is_finite()
                        && first_weight > 0.0
                        && second_weight.is_finite()
                        && second_weight > 0.0)
                    {
                        continue;
                    }
                    let visibility = collapse_paired_visibility(
                        first_visibility,
                        second_visibility,
                        *pair_transform,
                    );
                    if !(visibility.re.is_finite() && visibility.im.is_finite()) {
                        continue;
                    }
                    let combined_weight = 0.5 * (first_weight + second_weight);
                    if !(combined_weight.is_finite() && combined_weight > 0.0) {
                        continue;
                    }
                    let lambda_scale = imaging_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    batch.weight.push(combined_weight);
                    batch.sumwt_factor.push(sumwt_factor);
                    batch.gridable.push(is_cross);
                    batch.visibility.push(visibility);
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
                if trace_enabled {
                    samples.reserve(self.source_channel_frequencies_hz.len());
                }
                for (channel_slot, (channel_index, frequency_hz)) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                    .enumerate()
                {
                    let imaging_frequency_hz = frequency_hz * mfs_frequency_scale;
                    let lambda_scale = imaging_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    let first_visibility = phase_rotate_visibility(
                        data_2d.get(pair.0, channel_index)?,
                        transform.phase_shift_m,
                        imaging_frequency_hz,
                    );
                    let second_visibility = phase_rotate_visibility(
                        data_2d.get(pair.1, channel_index)?,
                        transform.phase_shift_m,
                        imaging_frequency_hz,
                    );
                    let (first_weight, first_weight_source) = weights.get(pair.0, channel_index)?;
                    let (second_weight, second_weight_source) =
                        weights.get(pair.1, channel_index)?;
                    let first_flagged = flags_2d.get(pair.0, channel_index)?;
                    let second_flagged = flags_2d.get(pair.1, channel_index)?;
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
                    if trace_enabled {
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
                                output_frequency_hz: imaging_frequency_hz,
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
            }
            (
                PreparedState::CollapsedCube {
                    plane_stokes,
                    transform: pair_transform,
                    channel_batches,
                    channel_density_batches,
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
                let density_slot_offset = (self
                    .source_channel_indices
                    .len()
                    .saturating_sub(channel_density_batches.len()))
                    / 2;
                if use_density_batches && !use_casa_cube_grid_interpolation {
                    for (output_channel, density_batch) in
                        channel_density_batches.iter_mut().enumerate()
                    {
                        let source_slot = if use_casa_cube_grid_interpolation {
                            output_channel + density_slot_offset
                        } else {
                            match row_spectral_contributions
                                .source_channel_output_map
                                .iter()
                                .position(|mapped| *mapped == Some(output_channel))
                            {
                                Some(source_slot) => source_slot,
                                None => continue,
                            }
                        };
                        if source_slot >= self.source_channel_indices.len() {
                            continue;
                        }
                        push_paired_cube_density_sample(
                            density_batch,
                            &flags_2d,
                            &weights,
                            *pair,
                            self.source_channel_indices[source_slot],
                            self.source_channel_frequencies_hz[source_slot],
                            uvw_m,
                            is_cross,
                        )?;
                    }
                }
                if let Some(preweighting) = casa_cube_briggs_preweighting.as_mut() {
                    let density_group_base =
                        casa_cube_briggs_density_group_base(preweighting, selected_row.field_id);
                    for grid in &row_spectral_contributions.padded_grid_channel_contributions {
                        let Some(sample) = interpolate_paired_cube_output_sample(
                            data,
                            flags,
                            row_weights,
                            weight_spectrum_row,
                            *pair,
                            &self.source_channel_indices,
                            transform.phase_shift_m,
                            grid.grid_frequency_hz,
                            &grid.contributions,
                            false,
                        )?
                        else {
                            continue;
                        };
                        if let Some(density_plane) = casa_cube_briggs_density_plane_index(
                            preweighting,
                            density_group_base,
                            grid.output_channel,
                        ) {
                            let batch = &mut preweighting.density_batches[density_plane];
                            let before = batch.len();
                            push_casa_cube_briggs_density_sample(
                                batch,
                                grid.grid_frequency_hz,
                                casa_cube_briggs_density_uvw_m,
                                sample.first_weight + sample.second_weight,
                                is_cross,
                            );
                            if batch.len() > before {
                                if let Some(rows) =
                                    preweighting.density_sample_rows.get_mut(density_plane)
                                {
                                    rows.push(row);
                                }
                            }
                        }
                    }
                }
                let grid_assignments;
                let assignment_iter: Box<
                    dyn Iterator<Item = (usize, f64, &[CubeChannelContribution])> + '_,
                > = if use_casa_cube_grid_interpolation {
                    Box::new(
                        row_spectral_contributions
                            .grid_channel_contributions
                            .iter()
                            .map(|grid| {
                                (
                                    grid.output_channel,
                                    grid.grid_frequency_hz,
                                    grid.contributions.as_slice(),
                                )
                            }),
                    )
                } else {
                    grid_assignments = assignments
                        .iter()
                        .enumerate()
                        .map(|(output_channel, contributions)| {
                            (
                                output_channel,
                                cube_output_channel_frequencies_hz
                                    .as_ref()
                                    .expect("missing cube spectral setup")[output_channel],
                                contributions.as_slice(),
                            )
                        })
                        .collect::<Vec<_>>();
                    Box::new(grid_assignments.into_iter())
                };
                let sumwt_factor = reported_sumwt_factor_for_paired_plane(*plane_stokes);
                for (output_channel, output_frequency_hz, contributions) in assignment_iter {
                    if contributions.is_empty() {
                        continue;
                    }
                    let Some(sample) = interpolate_paired_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *pair,
                        &self.source_channel_indices,
                        transform.phase_shift_m,
                        output_frequency_hz,
                        contributions,
                        use_casa_cube_grid_interpolation,
                    )?
                    else {
                        continue;
                    };
                    if !(output_frequency_hz.is_finite() && output_frequency_hz > 0.0) {
                        continue;
                    }
                    let visibility = collapse_paired_visibility(
                        sample.first_visibility,
                        sample.second_visibility,
                        *pair_transform,
                    );
                    if !(visibility.re.is_finite() && visibility.im.is_finite()) {
                        continue;
                    }
                    if use_density_batches && use_casa_cube_grid_interpolation {
                        push_paired_cube_density_sample_from_contributions(
                            &mut channel_density_batches[output_channel],
                            &flags_2d,
                            &weights,
                            *pair,
                            &self.source_channel_indices,
                            cube_row_source_frequencies_for_interpolation,
                            contributions,
                            casa_cube_briggs_lookup_uvw_m,
                            is_cross,
                        )?;
                        if let Some(preweighting) = casa_cube_briggs_preweighting.as_mut() {
                            let density_group_base = casa_cube_briggs_density_group_base(
                                preweighting,
                                selected_row.field_id,
                            );
                            let density_contribution =
                                nearest_cube_density_contribution(contributions);
                            let density_plane = density_contribution.and_then(|contribution| {
                                row_spectral_contributions
                                    .padded_source_channel_output_map
                                    .get(contribution.source_channel)
                                    .copied()
                                    .flatten()
                                    .and_then(|output_channel| {
                                        casa_cube_briggs_density_plane_index(
                                            preweighting,
                                            density_group_base,
                                            output_channel,
                                        )
                                    })
                            });
                            let source_channel = density_contribution.and_then(|contribution| {
                                self.source_channel_indices
                                    .get(contribution.source_channel)
                                    .copied()
                            });
                            if let Some(indices) = preweighting
                                .channel_density_plane_indices
                                .get_mut(output_channel)
                            {
                                indices.push(density_plane);
                            }
                            if let Some(rows) =
                                preweighting.channel_sample_rows.get_mut(output_channel)
                            {
                                rows.push(row);
                            }
                            if let Some(channels) = preweighting
                                .channel_sample_source_channels
                                .get_mut(output_channel)
                            {
                                channels.push(source_channel);
                            }
                        }
                    }
                    if trace_m100_row(selected_row.row_index) {
                        eprintln!(
                            "CASA_RS_TRACE_M100_ROW0 row={} output={} freq={:.17e} contributions={:?} first=({:.17e},{:.17e}) second=({:.17e},{:.17e}) first_weight={:.17e} second_weight={:.17e} collapsed=({:.17e},{:.17e})",
                            selected_row.row_index,
                            output_channel,
                            output_frequency_hz,
                            contributions,
                            sample.first_visibility.re,
                            sample.first_visibility.im,
                            sample.second_visibility.re,
                            sample.second_visibility.im,
                            sample.first_weight,
                            sample.second_weight,
                            visibility.re,
                            visibility.im,
                        );
                    }
                    let combined_weight = 0.5 * (sample.first_weight + sample.second_weight);
                    if !(combined_weight.is_finite() && combined_weight > 0.0) {
                        continue;
                    }
                    let lambda_scale = output_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    let batch = &mut channel_batches[output_channel];
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    batch.weight.push(combined_weight);
                    batch.sumwt_factor.push(sumwt_factor);
                    batch.gridable.push(is_cross);
                    batch.visibility.push(visibility);
                    if use_model_interpolation_batches {
                        channel_model_interpolation_samples[output_channel].push(
                            combine_model_channel_contributions(
                                contributions,
                                source_model_contributions,
                            ),
                        );
                    }
                    if trace_enabled {
                        let PairedCubeOutputSample {
                            first_visibility,
                            second_visibility,
                            first_weight,
                            second_weight,
                            first_weight_source,
                            second_weight_source,
                        } = sample;
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
                            first_visibility,
                            second_visibility,
                            first_weight,
                            second_weight,
                            first_weight_source,
                            second_weight_source,
                            first_flagged: false,
                            second_flagged: false,
                        });
                    }
                }
            }
            (
                PreparedState::PairedCube {
                    channel_batches,
                    channel_density_batches,
                    channel_model_interpolation_samples,
                    pair,
                    transform: pair_transform,
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
                let density_slot_offset = (self
                    .source_channel_indices
                    .len()
                    .saturating_sub(channel_density_batches.len()))
                    / 2;
                if use_density_batches && !use_casa_cube_grid_interpolation {
                    for (output_channel, density_batch) in
                        channel_density_batches.iter_mut().enumerate()
                    {
                        let source_slot = if use_casa_cube_grid_interpolation {
                            output_channel + density_slot_offset
                        } else {
                            match row_spectral_contributions
                                .source_channel_output_map
                                .iter()
                                .position(|mapped| *mapped == Some(output_channel))
                            {
                                Some(source_slot) => source_slot,
                                None => continue,
                            }
                        };
                        if source_slot >= self.source_channel_indices.len() {
                            continue;
                        }
                        push_paired_cube_density_sample(
                            density_batch,
                            &flags_2d,
                            &weights,
                            *pair,
                            self.source_channel_indices[source_slot],
                            self.source_channel_frequencies_hz[source_slot],
                            uvw_m,
                            is_cross,
                        )?;
                    }
                }
                if let Some(preweighting) = casa_cube_briggs_preweighting.as_mut() {
                    let density_group_base =
                        casa_cube_briggs_density_group_base(preweighting, selected_row.field_id);
                    for grid in &row_spectral_contributions.padded_grid_channel_contributions {
                        let Some(sample) = interpolate_paired_cube_output_sample(
                            data,
                            flags,
                            row_weights,
                            weight_spectrum_row,
                            *pair,
                            &self.source_channel_indices,
                            transform.phase_shift_m,
                            grid.grid_frequency_hz,
                            &grid.contributions,
                            false,
                        )?
                        else {
                            continue;
                        };
                        if let Some(density_plane) = casa_cube_briggs_density_plane_index(
                            preweighting,
                            density_group_base,
                            grid.output_channel,
                        ) {
                            let batch = &mut preweighting.density_batches[density_plane];
                            let before = batch.len();
                            push_casa_cube_briggs_density_sample(
                                batch,
                                grid.grid_frequency_hz,
                                casa_cube_briggs_density_uvw_m,
                                sample.first_weight + sample.second_weight,
                                is_cross,
                            );
                            if batch.len() > before {
                                if let Some(rows) =
                                    preweighting.density_sample_rows.get_mut(density_plane)
                                {
                                    rows.push(row);
                                }
                            }
                        }
                    }
                }
                let grid_assignments;
                let assignment_iter: Box<
                    dyn Iterator<Item = (usize, f64, &[CubeChannelContribution])> + '_,
                > = if use_casa_cube_grid_interpolation {
                    Box::new(
                        row_spectral_contributions
                            .grid_channel_contributions
                            .iter()
                            .map(|grid| {
                                (
                                    grid.output_channel,
                                    grid.grid_frequency_hz,
                                    grid.contributions.as_slice(),
                                )
                            }),
                    )
                } else {
                    grid_assignments = assignments
                        .iter()
                        .enumerate()
                        .map(|(output_channel, contributions)| {
                            (
                                output_channel,
                                cube_output_channel_frequencies_hz
                                    .as_ref()
                                    .expect("missing cube spectral setup")[output_channel],
                                contributions.as_slice(),
                            )
                        })
                        .collect::<Vec<_>>();
                    Box::new(grid_assignments.into_iter())
                };
                for (output_channel, output_frequency_hz, contributions) in assignment_iter {
                    if contributions.is_empty() {
                        continue;
                    }
                    let Some(sample) = interpolate_paired_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *pair,
                        &self.source_channel_indices,
                        transform.phase_shift_m,
                        output_frequency_hz,
                        contributions,
                        use_casa_cube_grid_interpolation,
                    )?
                    else {
                        continue;
                    };
                    if !(output_frequency_hz.is_finite() && output_frequency_hz > 0.0) {
                        continue;
                    }
                    if use_density_batches && use_casa_cube_grid_interpolation {
                        push_paired_cube_density_sample_from_contributions(
                            &mut channel_density_batches[output_channel],
                            &flags_2d,
                            &weights,
                            *pair,
                            &self.source_channel_indices,
                            cube_row_source_frequencies_for_interpolation,
                            contributions,
                            casa_cube_briggs_lookup_uvw_m,
                            is_cross,
                        )?;
                        if let Some(preweighting) = casa_cube_briggs_preweighting.as_mut() {
                            let density_group_base = casa_cube_briggs_density_group_base(
                                preweighting,
                                selected_row.field_id,
                            );
                            let density_contribution =
                                nearest_cube_density_contribution(contributions);
                            let density_plane = density_contribution.and_then(|contribution| {
                                row_spectral_contributions
                                    .padded_source_channel_output_map
                                    .get(contribution.source_channel)
                                    .copied()
                                    .flatten()
                                    .and_then(|output_channel| {
                                        casa_cube_briggs_density_plane_index(
                                            preweighting,
                                            density_group_base,
                                            output_channel,
                                        )
                                    })
                            });
                            let source_channel = density_contribution.and_then(|contribution| {
                                self.source_channel_indices
                                    .get(contribution.source_channel)
                                    .copied()
                            });
                            if let Some(indices) = preweighting
                                .channel_density_plane_indices
                                .get_mut(output_channel)
                            {
                                indices.push(density_plane);
                            }
                            if let Some(rows) =
                                preweighting.channel_sample_rows.get_mut(output_channel)
                            {
                                rows.push(row);
                            }
                            if let Some(channels) = preweighting
                                .channel_sample_source_channels
                                .get_mut(output_channel)
                            {
                                channels.push(source_channel);
                            }
                        }
                    }
                    let lambda_scale = output_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    let batch = &mut channel_batches[output_channel];
                    batch.u_lambda.push(uvw_m[0] * lambda_scale);
                    batch.v_lambda.push(uvw_m[1] * lambda_scale);
                    batch.w_lambda.push(uvw_m[2] * lambda_scale);
                    if trace_m100_row(selected_row.row_index) {
                        let visibility = collapse_paired_visibility(
                            sample.first_visibility,
                            sample.second_visibility,
                            *pair_transform,
                        );
                        eprintln!(
                            "CASA_RS_TRACE_M100_ROW0 paired row={} output={} freq={:.17e} contributions={:?} first=({:.17e},{:.17e}) second=({:.17e},{:.17e}) first_weight={:.17e} second_weight={:.17e} collapsed=({:.17e},{:.17e})",
                            selected_row.row_index,
                            output_channel,
                            output_frequency_hz,
                            contributions,
                            sample.first_visibility.re,
                            sample.first_visibility.im,
                            sample.second_visibility.re,
                            sample.second_visibility.im,
                            sample.first_weight,
                            sample.second_weight,
                            visibility.re,
                            visibility.im,
                        );
                    }
                    batch.first_visibility.push(sample.first_visibility);
                    batch.second_visibility.push(sample.second_visibility);
                    batch.first_weight.push(sample.first_weight);
                    batch.second_weight.push(sample.second_weight);
                    batch.first_flagged.push(false);
                    batch.second_flagged.push(false);
                    batch.gridable.push(is_cross);
                    if use_model_interpolation_batches {
                        channel_model_interpolation_samples[output_channel].push(
                            combine_model_channel_contributions(
                                contributions,
                                source_model_contributions,
                            ),
                        );
                    }
                    if trace_enabled {
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
            }
            _ => {
                return Err(
                    "internal error: prepared sample state and trace state are out of sync"
                        .to_string(),
                );
            }
        }
        timings.adapt_samples += adapt_started_at.elapsed();
        Ok(())
    }

    fn mfs_imaging_frequency_scale_for_row(
        &mut self,
        selected_row: &SelectedMainRow,
        derived_engine: Option<&MsCalEngine>,
    ) -> Result<f64, String> {
        if self.freq_ref == FrequencyRef::LSRK {
            return Ok(1.0);
        }
        let row_time_mjd_sec = selected_row.time_mjd_seconds.ok_or_else(|| {
            "internal error: missing row time for MFS frequency-frame conversion".to_string()
        })?;
        let cache_key = (row_time_mjd_sec.to_bits(), selected_row.field_id);
        if let Some(scale) = self.mfs_frequency_scale_cache.get(&cache_key) {
            return Ok(*scale);
        }
        let reference_frequency_hz = self
            .source_channel_frequencies_hz
            .first()
            .copied()
            .ok_or_else(|| {
                "internal error: MFS preparation has no source frequencies".to_string()
            })?;
        let scale = mfs_imaging_frequency_scale(
            self.freq_ref,
            reference_frequency_hz,
            selected_row,
            derived_engine,
        )?;
        self.mfs_frequency_scale_cache.insert(cache_key, scale);
        Ok(scale)
    }

    fn finish_standard_mfs_without_trace(self) -> Result<PreparedInput, String> {
        let PreparedSelection {
            initialization_error: _,
            source_channel_indices: _,
            source_channel_frequencies_hz: _,
            source_channel_widths_hz: _,
            selected_frequency_range_hz,
            reffreq_hz,
            freq_ref,
            cube_spectral_setup: _,
            cube_row_spectral_cache: _,
            cube_row_source_frequency_cache: _,
            mfs_frequency_scale_cache: _,
            casa_cube_grid_interpolation: _,
            casa_cube_briggs_preweighting: _,
            use_density_batches: _,
            use_model_interpolation_batches: _,
            mosaic_pb_limit: _,
            phase_center,
            state,
            trace_state: _,
            trace_enabled: _,
        } = self;
        match state {
            PreparedState::ExplicitMfs {
                plane_stokes,
                batch,
                ..
            } => Ok(PreparedInput::Mfs(PlaneInput {
                phase_center,
                freq_ref,
                reffreq_hz,
                selected_frequency_range_hz,
                plane_stokes,
                batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                sample_frequency_batches_hz: Vec::new(),
                gridder_mode: GridderMode::Standard,
            })),
            PreparedState::PairedMfs {
                plane_stokes,
                paired,
                transform,
                ..
            } => {
                let collapsed = collapse_paired_visibility_batch(&paired, transform, plane_stokes)
                    .map_err(|error| error.to_string())?;
                Ok(PreparedInput::Mfs(PlaneInput {
                    phase_center,
                    freq_ref,
                    reffreq_hz,
                    selected_frequency_range_hz,
                    plane_stokes,
                    batches: chunk_visibility_batch(collapsed, DEFAULT_BATCH_SIZE),
                    sample_frequency_batches_hz: Vec::new(),
                    gridder_mode: GridderMode::Standard,
                }))
            }
            PreparedState::CollapsedMfs {
                plane_stokes,
                batch,
                ..
            } => Ok(PreparedInput::Mfs(PlaneInput {
                phase_center,
                freq_ref,
                reffreq_hz,
                selected_frequency_range_hz,
                plane_stokes,
                batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                sample_frequency_batches_hz: Vec::new(),
                gridder_mode: GridderMode::Standard,
            })),
            _ => Err("internal error: fast trace-free prepare requires MFS state".to_string()),
        }
    }

    fn finish_cube_without_trace(self) -> Result<PreparedInput, String> {
        let PreparedSelection {
            initialization_error: _,
            source_channel_indices: _,
            source_channel_frequencies_hz,
            source_channel_widths_hz: _,
            selected_frequency_range_hz: _,
            reffreq_hz: _,
            freq_ref,
            cube_spectral_setup,
            cube_row_spectral_cache: _,
            cube_row_source_frequency_cache: _,
            mfs_frequency_scale_cache: _,
            casa_cube_grid_interpolation: _,
            casa_cube_briggs_preweighting,
            use_density_batches,
            use_model_interpolation_batches,
            mosaic_pb_limit: _,
            phase_center,
            state,
            trace_state: _,
            trace_enabled: _,
        } = self;
        let output_channel_frequencies_hz = cube_spectral_setup
            .as_ref()
            .map(|setup| setup.output_channel_frequencies_hz.clone())
            .unwrap_or(source_channel_frequencies_hz);
        match state {
            PreparedState::ExplicitCube {
                plane_stokes,
                channel_batches,
                channel_density_batches,
                channel_model_interpolation_samples,
                ..
            } => {
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_density_batches)
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            ((channel_frequency_hz, batch), density_batch),
                            model_interpolation_samples,
                        )| CubeChannelRequest {
                            channel_frequency_hz,
                            visibility_batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                            density_batches: chunk_density_batch(
                                density_batch,
                                use_density_batches,
                            ),
                            model_interpolation_batches:
                                chunk_model_interpolation_batches_if_needed(
                                    model_interpolation_samples,
                                    use_model_interpolation_batches,
                                ),
                        },
                    )
                    .collect();
                Ok(PreparedInput::Cube(CubePlaneInput {
                    phase_center,
                    freq_ref,
                    plane_stokes,
                    channels,
                    gridder_modes: vec![GridderMode::Standard; output_channel_frequencies_hz.len()],
                    casa_cube_briggs_preweighting,
                }))
            }
            PreparedState::CollapsedCube {
                plane_stokes,
                transform: _,
                pair: _,
                channel_batches,
                channel_density_batches,
                channel_model_interpolation_samples,
                ..
            } => {
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_density_batches)
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            ((channel_frequency_hz, batch), density_batch),
                            model_interpolation_samples,
                        )| CubeChannelRequest {
                            channel_frequency_hz,
                            visibility_batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                            density_batches: chunk_density_batch(
                                density_batch,
                                use_density_batches,
                            ),
                            model_interpolation_batches:
                                chunk_model_interpolation_batches_if_needed(
                                    model_interpolation_samples,
                                    use_model_interpolation_batches,
                                ),
                        },
                    )
                    .collect();
                Ok(PreparedInput::Cube(CubePlaneInput {
                    phase_center,
                    freq_ref,
                    plane_stokes,
                    channels,
                    gridder_modes: vec![GridderMode::Standard; output_channel_frequencies_hz.len()],
                    casa_cube_briggs_preweighting,
                }))
            }
            PreparedState::PairedCube {
                plane_stokes,
                transform,
                channel_batches,
                channel_density_batches,
                channel_model_interpolation_samples,
                ..
            } => {
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_density_batches)
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            ((channel_frequency_hz, batch), density_batch),
                            model_interpolation_samples,
                        )| {
                            let collapsed =
                                collapse_paired_visibility_batch(&batch, transform, plane_stokes)
                                    .map_err(|error| error.to_string())?;
                            let collapsed_model_interpolation_samples =
                                if use_model_interpolation_batches {
                                    collapse_paired_model_interpolation_samples_from_batch(
                                        &batch,
                                        model_interpolation_samples,
                                        transform,
                                    )?
                                } else {
                                    Vec::new()
                                };
                            Ok(CubeChannelRequest {
                                channel_frequency_hz,
                                visibility_batches: chunk_visibility_batch(
                                    collapsed,
                                    DEFAULT_BATCH_SIZE,
                                ),
                                density_batches: chunk_density_batch(
                                    density_batch,
                                    use_density_batches,
                                ),
                                model_interpolation_batches:
                                    chunk_model_interpolation_batches_if_needed(
                                        collapsed_model_interpolation_samples,
                                        use_model_interpolation_batches,
                                    ),
                            })
                        },
                    )
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(PreparedInput::Cube(CubePlaneInput {
                    phase_center,
                    freq_ref,
                    plane_stokes,
                    channels,
                    gridder_modes: vec![GridderMode::Standard; output_channel_frequencies_hz.len()],
                    casa_cube_briggs_preweighting,
                }))
            }
            _ => Err("internal error: trace-free cube prepare requires cube state".to_string()),
        }
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
            cube_row_source_frequency_cache: _,
            mfs_frequency_scale_cache: _,
            casa_cube_grid_interpolation: _,
            casa_cube_briggs_preweighting,
            use_density_batches,
            use_model_interpolation_batches,
            mosaic_pb_limit,
            phase_center: prepared_phase_center,
            state,
            trace_state,
            trace_enabled: _,
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
                let gridder_mode =
                    infer_mfs_gridder_mode(ms, &prepared_phase_center, &samples, mosaic_pb_limit)?;
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
                let gridder_mode =
                    infer_mfs_gridder_mode(ms, &prepared_phase_center, &accepted, mosaic_pb_limit)?;
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
                    channel_density_batches,
                    channel_model_interpolation_samples,
                    ..
                },
                PreparedTraceState::ExplicitCube { channel_samples },
            ) => {
                let gridder_modes = channel_samples
                    .iter()
                    .map(|samples| {
                        infer_mfs_gridder_mode(ms, &prepared_phase_center, samples, mosaic_pb_limit)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_density_batches)
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            ((channel_frequency_hz, batch), density_batch),
                            model_interpolation_samples,
                        )| {
                            CubeChannelRequest {
                                channel_frequency_hz,
                                visibility_batches: chunk_visibility_batch(
                                    batch,
                                    DEFAULT_BATCH_SIZE,
                                ),
                                density_batches: chunk_density_batch(
                                    density_batch,
                                    use_density_batches,
                                ),
                                model_interpolation_batches:
                                    chunk_model_interpolation_batches_if_needed(
                                        model_interpolation_samples,
                                        use_model_interpolation_batches,
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
                        gridder_modes,
                        casa_cube_briggs_preweighting,
                    }),
                    make_trace_bundle(channel_samples.into_iter().flatten().collect(), Vec::new()),
                ))
            }
            (
                PreparedState::PairedCube {
                    plane_stokes,
                    transform,
                    channel_batches,
                    channel_density_batches,
                    channel_model_interpolation_samples,
                    ..
                },
                PreparedTraceState::PairedCube { channel_samples },
            ) => {
                let mut channel_gridder_modes = Vec::with_capacity(channel_samples.len());
                let mut all_accepted = Vec::new();
                let mut all_rejected = Vec::new();
                for samples in &channel_samples {
                    let (accepted, rejected) =
                        collapse_pending_pair_traces(samples.clone(), transform, plane_stokes);
                    channel_gridder_modes.push(infer_mfs_gridder_mode(
                        ms,
                        &prepared_phase_center,
                        &accepted,
                        mosaic_pb_limit,
                    )?);
                    all_accepted.extend(accepted);
                    all_rejected.extend(rejected);
                }
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_density_batches)
                    .zip(channel_samples.iter())
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            (((channel_frequency_hz, batch), density_batch), trace_samples),
                            model_interpolation_samples,
                        )| {
                            let collapsed =
                                collapse_paired_visibility_batch(&batch, transform, plane_stokes)
                                    .map_err(|error| error.to_string())?;
                            let collapsed_model_interpolation_samples =
                                if use_model_interpolation_batches {
                                    collapse_pending_pair_model_interpolation_samples(
                                        trace_samples,
                                        model_interpolation_samples,
                                        transform,
                                    )?
                                } else {
                                    Vec::new()
                                };
                            Ok(CubeChannelRequest {
                                channel_frequency_hz,
                                visibility_batches: chunk_visibility_batch(
                                    collapsed,
                                    DEFAULT_BATCH_SIZE,
                                ),
                                density_batches: chunk_density_batch(
                                    density_batch,
                                    use_density_batches,
                                ),
                                model_interpolation_batches:
                                    chunk_model_interpolation_batches_if_needed(
                                        collapsed_model_interpolation_samples,
                                        use_model_interpolation_batches,
                                    ),
                            })
                        },
                    )
                    .collect::<Result<Vec<_>, String>>()?;
                Ok((
                    PreparedInput::Cube(CubePlaneInput {
                        phase_center: prepared_phase_center,
                        freq_ref,
                        plane_stokes,
                        channels,
                        gridder_modes: channel_gridder_modes,
                        casa_cube_briggs_preweighting,
                    }),
                    make_trace_bundle(all_accepted, all_rejected),
                ))
            }
            (
                PreparedState::CollapsedCube {
                    plane_stokes,
                    transform,
                    channel_batches,
                    channel_density_batches,
                    channel_model_interpolation_samples,
                    ..
                },
                PreparedTraceState::PairedCube { channel_samples },
            ) => {
                let mut channel_gridder_modes = Vec::with_capacity(channel_samples.len());
                let mut all_accepted = Vec::new();
                let mut all_rejected = Vec::new();
                for samples in &channel_samples {
                    let (accepted, rejected) =
                        collapse_pending_pair_traces(samples.clone(), transform, plane_stokes);
                    channel_gridder_modes.push(infer_mfs_gridder_mode(
                        ms,
                        &prepared_phase_center,
                        &accepted,
                        mosaic_pb_limit,
                    )?);
                    all_accepted.extend(accepted);
                    all_rejected.extend(rejected);
                }
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .zip(channel_density_batches)
                    .zip(channel_model_interpolation_samples)
                    .map(
                        |(
                            ((channel_frequency_hz, batch), density_batch),
                            model_interpolation_samples,
                        )| CubeChannelRequest {
                            channel_frequency_hz,
                            visibility_batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                            density_batches: chunk_density_batch(
                                density_batch,
                                use_density_batches,
                            ),
                            model_interpolation_batches:
                                chunk_model_interpolation_batches_if_needed(
                                    model_interpolation_samples,
                                    use_model_interpolation_batches,
                                ),
                        },
                    )
                    .collect();
                Ok((
                    PreparedInput::Cube(CubePlaneInput {
                        phase_center: prepared_phase_center,
                        freq_ref,
                        plane_stokes,
                        channels,
                        gridder_modes: channel_gridder_modes,
                        casa_cube_briggs_preweighting,
                    }),
                    make_trace_bundle(all_accepted, all_rejected),
                ))
            }
            _ => Err(
                "internal error: prepared state and trace state diverged during finalize"
                    .to_string(),
            ),
        }
    }
}

#[cfg(test)]
fn mosaic_pb_product_from_weight(weight_image: &Array2<f32>) -> Array4<f32> {
    mosaic_pb_product_from_weight_product(&expand_plane_for_write(weight_image))
}

fn mosaic_pb_product_from_weight_product(weight_product: &Array4<f32>) -> Array4<f32> {
    let mut pb = Array4::<f32>::zeros(weight_product.dim());
    let (_, _, nstokes, nchan) = weight_product.dim();
    for stokes_index in 0..nstokes {
        for channel_index in 0..nchan {
            let weight_plane = weight_product.slice(s![.., .., stokes_index, channel_index]);
            let peak = weight_plane
                .iter()
                .copied()
                .filter(|value| value.is_finite())
                .fold(0.0f32, f32::max);
            if peak <= 0.0 {
                continue;
            }
            let mut pb_plane = pb.slice_mut(s![.., .., stokes_index, channel_index]);
            Zip::from(&mut pb_plane)
                .and(&weight_plane)
                .for_each(|pb_value, weight_value| {
                    let normalized = (*weight_value).max(0.0) / peak;
                    if normalized.is_finite() && normalized > 0.0 {
                        *pb_value = normalized.sqrt();
                    }
                });
        }
    }
    pb
}

fn pb_correct_image_product(image: &Array4<f32>, pb: &Array4<f32>, pb_limit: f32) -> Array4<f32> {
    let mut corrected = Array4::<f32>::zeros(image.dim());
    Zip::from(&mut corrected).and(image).and(pb).for_each(
        |corrected_value, image_value, pb_value| {
            if pb_value.is_finite() && *pb_value > pb_limit {
                *corrected_value = *image_value / *pb_value;
            }
        },
    );
    corrected
}

fn pb_support_mask_product(pb: &Array4<f32>, pb_limit: f32) -> ArrayD<bool> {
    pb.mapv(|value| value.is_finite() && value > pb_limit)
        .into_dyn()
}

#[derive(Debug, Clone)]
enum EffectiveCleanMask {
    Plane(Array2<bool>),
    Cube(Array4<bool>),
}

fn clean_mask_product(mask: &EffectiveCleanMask, result: &RunProducts) -> Array4<f32> {
    match mask {
        EffectiveCleanMask::Plane(mask) => {
            let channel_count = result.channel_frequencies_hz().len().max(1);
            let (nx, ny) = mask.dim();
            let mut product = Array4::<f32>::zeros((nx, ny, 1, channel_count));
            for channel_index in 0..channel_count {
                for x in 0..nx {
                    for y in 0..ny {
                        product[(x, y, 0, channel_index)] = if mask[(x, y)] { 1.0 } else { 0.0 };
                    }
                }
            }
            product
        }
        EffectiveCleanMask::Cube(mask) => mask.mapv(|value| if value { 1.0 } else { 0.0 }),
    }
}

fn pb_limited_product(pb: &Array4<f32>, pb_limit: f32) -> Array4<f32> {
    pb.mapv(|value| {
        if value.is_finite() && value > pb_limit {
            value
        } else {
            0.0
        }
    })
}

fn write_products(
    config: &CliConfig,
    coords: &CoordinateSystem,
    result: &RunProducts,
    clean_mask: Option<&EffectiveCleanMask>,
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
        debug_weight_product,
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
            result
                .diagnostics
                .mosaic_weight_image
                .as_ref()
                .map(expand_plane_for_write),
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
        RunProducts::Cube(products) => (
            &products.result.psf,
            &products.result.residual,
            &products.result.model,
            &products.result.image,
            &products.result.sumwt,
            products.mosaic_weight.clone(),
            beam_set_from_channel_beams(&products.result.beams, RestoringBeamMode::PerPlane)?,
            beam_set_from_channel_beams(&products.result.beams, RestoringBeamMode::PerPlane)?,
            beam_set_from_channel_beams(
                &products.result.restored_beams,
                config.restoring_beam_mode,
            )?,
            products.result.compatibility.psf_units.as_str(),
            products.result.compatibility.residual_units.as_str(),
            products.result.compatibility.model_units.as_str(),
            products.result.compatibility.image_units.as_str(),
        ),
        RunProducts::Mtmfs(_) => unreachable!("MTMFS products are handled by the early return"),
    };
    let mosaic_pb_product = debug_weight_product
        .as_ref()
        .map(mosaic_pb_product_from_weight_product);
    let mosaic_support_mask = mosaic_pb_product
        .as_ref()
        .map(|pb| pb_support_mask_product(pb, config.mosaic_pb_limit));
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
    write_single_product_inner(SingleProductWrite {
        path: &PathBuf::from(format!("{base}.residual")),
        data: residual,
        coords,
        units: residual_units,
        beam_set: residual_beams,
        role: "residual",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
        mask: mosaic_support_mask.as_ref(),
    })?;
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
    write_single_product_inner(SingleProductWrite {
        path: &PathBuf::from(format!("{base}.image")),
        data: image,
        coords,
        units: image_units,
        beam_set: image_beams,
        role: "image",
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
        mask: mosaic_support_mask.as_ref(),
    })?;
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
    if let Some(clean_mask) = clean_mask {
        let mask_product = clean_mask_product(clean_mask, result);
        write_single_product(
            &PathBuf::from(format!("{base}.mask")),
            &mask_product,
            coords,
            "",
            ImageBeamSet::default(),
            "mask",
            plane_stokes,
            channel_frequencies_hz,
            reffreq_hz,
        )?;
    }
    if let Some(weight_product) = debug_weight_product.as_ref() {
        write_single_product(
            &PathBuf::from(format!("{base}.weight")),
            weight_product,
            coords,
            "",
            ImageBeamSet::default(),
            "weight",
            plane_stokes,
            channel_frequencies_hz,
            reffreq_hz,
        )?;
        let pb_product = mosaic_pb_product.expect("mosaic PB product computed from weight image");
        let limited_pb_product = pb_limited_product(&pb_product, config.mosaic_pb_limit);
        write_single_product_inner(SingleProductWrite {
            path: &PathBuf::from(format!("{base}.pb")),
            data: &limited_pb_product,
            coords,
            units: "",
            beam_set: ImageBeamSet::default(),
            role: "pb",
            plane_stokes,
            channel_frequencies_hz,
            reffreq_hz,
            mask: mosaic_support_mask.as_ref(),
        })?;
        if config.pbcor {
            let pbcor_product =
                pb_correct_image_product(image, &pb_product, config.mosaic_pb_limit);
            write_single_product_inner(SingleProductWrite {
                path: &PathBuf::from(format!("{base}.image.pbcor")),
                data: &pbcor_product,
                coords,
                units: image_units,
                beam_set: ImageBeamSet::default(),
                role: "image.pbcor",
                plane_stokes,
                channel_frequencies_hz,
                reffreq_hz,
                mask: mosaic_support_mask.as_ref(),
            })?;
        }
    }

    if config.write_preview_pngs {
        write_preview_png(&PathBuf::from(format!("{base}.psf.png")), psf)?;
        write_preview_png(&PathBuf::from(format!("{base}.residual.png")), residual)?;
        write_preview_png(&PathBuf::from(format!("{base}.model.png")), model)?;
        write_preview_png(&PathBuf::from(format!("{base}.image.png")), image)?;
        if let Some(weight_product) = debug_weight_product.as_ref() {
            write_preview_png(&PathBuf::from(format!("{base}.weight.png")), weight_product)?;
            let pb_product = mosaic_pb_product_from_weight_product(weight_product);
            let limited_pb_product = pb_limited_product(&pb_product, config.mosaic_pb_limit);
            write_preview_png(
                &PathBuf::from(format!("{base}.pb.png")),
                &limited_pb_product,
            )?;
            if config.pbcor {
                let pbcor_product =
                    pb_correct_image_product(image, &pb_product, config.mosaic_pb_limit);
                write_preview_png(
                    &PathBuf::from(format!("{base}.image.pbcor.png")),
                    &pbcor_product,
                )?;
            }
        }
    }

    Ok(())
}

fn write_model_column(
    ms: &mut MeasurementSet,
    config: &CliConfig,
    result: &RunProducts,
    trace: &PreparedVisibilityTraceBundle,
) -> Result<usize, String> {
    let model_cube = match result {
        RunProducts::Mfs(result) => result.model.clone(),
        RunProducts::Mtmfs(_) => {
            return Err(
                "savemodel=modelcolumn does not yet support deconvolver='mtmfs'".to_string(),
            );
        }
        RunProducts::Cube(products) => products.result.model.clone(),
    };
    let (_, _, _, model_channel_count) = model_cube.dim();
    let geometry = ImageGeometry {
        image_shape: [config.imsize, config.imsize],
        cell_size_rad: [
            config.cell_arcsec * arcsec_to_rad(),
            config.cell_arcsec * arcsec_to_rad(),
        ],
    };
    let predictors = (0..model_channel_count)
        .map(|channel_index| {
            let model_plane = model_cube.slice(s![.., .., 0, channel_index]).to_owned();
            StandardMfsModelPredictor::new(geometry, &model_plane).map_err(|error| {
                format!("prepare MODEL_DATA predictor for channel {channel_index}: {error}")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let created_model_data_column = ensure_model_data_column(ms)?;

    let mut rows = trace
        .selected_rows
        .iter()
        .map(|row| {
            zero_model_row_like_data(ms, row.row_index).map(|model_row| (row.row_index, model_row))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut written_samples = 0usize;
    for sample in &trace.samples {
        if !sample.gridable {
            continue;
        }
        if sample.source_contributions.is_empty() {
            continue;
        }
        let row_shape = rows
            .get(&sample.row_index)
            .ok_or_else(|| {
                format!(
                    "prepared sample row {} was not present in selected rows",
                    sample.row_index
                )
            })?
            .shape()
            .to_vec();
        let row_model = rows
            .get_mut(&sample.row_index)
            .expect("row model shape was just read");
        let model_channel_index = sample.output_channel_index.unwrap_or(0);
        let Some(predictor) = predictors.get(model_channel_index) else {
            continue;
        };
        for contribution in &sample.source_contributions {
            let lambda_scale = contribution.source_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            let predicted = predictor.predict(
                sample.imaging_uvw_m[0] * lambda_scale,
                sample.imaging_uvw_m[1] * lambda_scale,
            );
            let predicted = phase_rotate_visibility(
                predicted,
                -sample.phase_shift_m,
                contribution.source_frequency_hz,
            );
            for &corr_index in &sample.correlation_indices {
                if corr_index >= row_shape[0] || contribution.source_channel_index >= row_shape[1] {
                    continue;
                }
                row_model[[corr_index, contribution.source_channel_index]] = predicted;
                written_samples += 1;
            }
        }
    }

    let changed_rows = rows.keys().copied().collect::<Vec<_>>();
    for (row_index, row_model) in rows {
        ms.main_table_mut()
            .column_accessor_mut(VisibilityDataColumn::ModelData.name())
            .and_then(|mut column| {
                column.set_array_assuming_valid(row_index, ArrayValue::Complex32(row_model))
            })
            .map_err(|error| format!("write MODEL_DATA row {row_index}: {error}"))?;
    }
    if created_model_data_column {
        ms.save_main_table_only_assuming_valid().map_err(|error| {
            format!(
                "save MODEL_DATA updates to {}: {error}",
                config.ms.display()
            )
        })?;
    } else {
        ms.main_table()
            .save_selected_rows_in_place_assuming_valid(
                &[VisibilityDataColumn::ModelData.name()],
                &changed_rows,
            )
            .map_err(|error| {
                format!(
                    "save MODEL_DATA updates to {}: {error}",
                    config.ms.display()
                )
            })?;
    }
    Ok(written_samples)
}

fn ensure_model_data_column(ms: &mut MeasurementSet) -> Result<bool, String> {
    if ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(VisibilityDataColumn::ModelData.name()))
    {
        return Ok(false);
    }
    let zero_rows = (0..ms.row_count())
        .map(|row_index| zero_model_row_like_data(ms, row_index).map(|row| (row_index, row)))
        .collect::<Result<Vec<_>, String>>()?;
    ms.main_table_mut()
        .add_column(
            ColumnSchema::array_variable(
                VisibilityDataColumn::ModelData.name(),
                casa_types::PrimitiveType::Complex32,
                Some(2),
            ),
            None,
        )
        .map_err(|error| format!("add MODEL_DATA column: {error}"))?;
    for (row_index, row_model) in zero_rows {
        ms.main_table_mut()
            .cell_accessor_mut(row_index, VisibilityDataColumn::ModelData.name())
            .and_then(|mut cell| cell.set(Value::Array(ArrayValue::Complex32(row_model))))
            .map_err(|error| format!("initialize MODEL_DATA row {row_index}: {error}"))?;
    }
    Ok(true)
}

fn zero_model_row_like_data(
    ms: &MeasurementSet,
    row_index: usize,
) -> Result<ArrayD<Complex32>, String> {
    let shape = ms
        .main_table()
        .cell_accessor(row_index, VisibilityDataColumn::Data.name())
        .and_then(|cell| cell.array())
        .map_err(|error| format!("read DATA row {row_index} shape for MODEL_DATA: {error}"))?
        .shape()
        .to_vec();
    if shape.len() != 2 {
        return Err(format!(
            "DATA row {row_index} must be rank-2 to seed MODEL_DATA, found shape {shape:?}"
        ));
    }
    Ok(ArrayD::from_elem(IxDyn(&shape), Complex32::new(0.0, 0.0)))
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
    write_single_product_inner(SingleProductWrite {
        path,
        data,
        coords,
        units,
        beam_set,
        role,
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
        mask: None,
    })
}

struct SingleProductWrite<'a> {
    path: &'a Path,
    data: &'a Array4<f32>,
    coords: &'a CoordinateSystem,
    units: &'a str,
    beam_set: ImageBeamSet,
    role: &'a str,
    plane_stokes: &'a str,
    channel_frequencies_hz: &'a [f64],
    reffreq_hz: f64,
    mask: Option<&'a ArrayD<bool>>,
}

fn write_single_product_inner(spec: SingleProductWrite<'_>) -> Result<(), String> {
    let SingleProductWrite {
        path,
        data,
        coords,
        units,
        beam_set,
        role,
        plane_stokes,
        channel_frequencies_hz,
        reffreq_hz,
        mask,
    } = spec;
    remove_existing_product(path)?;
    let mut image = PagedImage::<f32>::create(data.shape().to_vec(), coords.clone(), path)
        .map_err(|error| format!("create image {}: {error}", path.display()))?;
    image
        .put_slice(&data.clone().into_dyn(), &[0, 0, 0, 0])
        .map_err(|error| format!("write pixels {}: {error}", path.display()))?;
    if let Some(mask) = mask {
        image
            .put_mask("mask0", mask)
            .map_err(|error| format!("write mask {}: {error}", path.display()))?;
        image
            .set_default_mask("mask0")
            .map_err(|error| format!("set default mask {}: {error}", path.display()))?;
    }
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

struct CoordinateSystemBuild<'a> {
    imsize: usize,
    phase_center: [f64; 2],
    cell_arcsec: f64,
    freq_ref: FrequencyRef,
    direction_ref: DirectionRef,
    plane_stokes: PlaneStokes,
    channel_frequencies_hz: &'a [f64],
    requested_rest_frequency_hz: Option<f64>,
}

fn build_coordinate_system(config: CoordinateSystemBuild<'_>) -> CoordinateSystem {
    let CoordinateSystemBuild {
        imsize,
        phase_center,
        cell_arcsec,
        freq_ref,
        direction_ref,
        plane_stokes,
        channel_frequencies_hz,
        requested_rest_frequency_hz,
    } = config;
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
        requested_rest_frequency_hz,
    )));
    coords
}

fn build_spectral_coordinate(
    freq_ref: FrequencyRef,
    channel_frequencies_hz: &[f64],
    requested_rest_frequency_hz: Option<f64>,
) -> SpectralCoordinate {
    let rest_frequency = requested_rest_frequency_hz.unwrap_or_else(|| {
        if channel_frequencies_hz.is_empty() {
            0.0
        } else {
            0.5 * (channel_frequencies_hz[0]
                + channel_frequencies_hz[channel_frequencies_hz.len() - 1])
        }
    });
    match channel_frequencies_hz {
        [] => SpectralCoordinate::new(freq_ref, 0.0, 1.0, 0.0, rest_frequency),
        [single] => SpectralCoordinate::new(freq_ref, *single, 1.0, 0.0, rest_frequency),
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

fn frequency_range_hz(frequencies_hz: &[f64]) -> Result<[f64; 2], String> {
    let mut min_hz = f64::INFINITY;
    let mut max_hz = f64::NEG_INFINITY;
    for &frequency_hz in frequencies_hz {
        if !frequency_hz.is_finite() || frequency_hz <= 0.0 {
            return Err(
                "channel selection resolved to a non-positive or non-finite frequency".to_string(),
            );
        }
        min_hz = min_hz.min(frequency_hz);
        max_hz = max_hz.max(frequency_hz);
    }
    if min_hz.is_infinite() {
        return Err("channel selection resolved to zero frequencies".to_string());
    }
    Ok([min_hz, max_hz])
}

fn fractional_bandwidth_from_range(frequency_range_hz: [f64; 2]) -> f64 {
    let min_freq = frequency_range_hz[0].abs().min(frequency_range_hz[1].abs());
    let max_freq = frequency_range_hz[0].abs().max(frequency_range_hz[1].abs());
    if min_freq > 0.0 && max_freq.is_finite() {
        2.0 * (max_freq - min_freq) / (max_freq + min_freq)
    } else {
        0.0
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
        "briggsbwtaper" => Ok(WeightingMode::BriggsBwTaper { robust }),
        _ => Err(format!(
            "unsupported --weighting value {text:?}; expected natural, uniform, briggs, or briggsbwtaper"
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

fn parse_hogbom_iteration_mode(text: &str) -> Result<HogbomIterationMode, String> {
    match text.to_ascii_lowercase().as_str() {
        "strict" => Ok(HogbomIterationMode::Strict),
        "casa" | "casa-inclusive" | "inclusive" => Ok(HogbomIterationMode::CasaInclusive),
        _ => Err(format!(
            "unsupported --hogbom-iteration-mode value {text:?}; expected strict or casa"
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

fn parse_clean_mask_mode(text: &str) -> Result<CleanMaskMode, String> {
    match text.to_ascii_lowercase().as_str() {
        "user" => Ok(CleanMaskMode::User),
        "auto-multithresh" | "automultithresh" => Ok(CleanMaskMode::AutoMultiThreshold),
        other => Err(format!(
            "unsupported --usemask {other:?}; expected user or auto-multithresh"
        )),
    }
}

fn parse_auto_mask_f32(text: &str) -> Result<f32, String> {
    let value = text
        .parse::<f32>()
        .map_err(|error| format!("parse automask value {text:?}: {error}"))?;
    if !value.is_finite() {
        return Err(format!("automask value {text:?} must be finite"));
    }
    Ok(value)
}

fn validate_auto_mask_config(
    mode: CleanMaskMode,
    config: &AutoMultiThresholdConfig,
) -> Result<(), String> {
    if mode != CleanMaskMode::AutoMultiThreshold {
        return Ok(());
    }
    for (name, value) in [
        ("sidelobethreshold", config.sidelobe_threshold),
        ("noisethreshold", config.noise_threshold),
        ("lownoisethreshold", config.low_noise_threshold),
        ("negativethreshold", config.negative_threshold),
        ("smoothfactor", config.smooth_factor),
        ("minbeamfrac", config.min_beam_frac),
        ("cutthreshold", config.cut_threshold),
        ("minpercentchange", config.min_percent_change),
    ] {
        if !value.is_finite() {
            return Err(format!(
                "{name} must be finite for usemask='auto-multithresh'"
            ));
        }
    }
    if config.sidelobe_threshold < 0.0
        || config.noise_threshold < 0.0
        || config.low_noise_threshold < 0.0
        || config.negative_threshold < 0.0
        || config.smooth_factor <= 0.0
        || config.min_beam_frac < 0.0
        || config.cut_threshold < 0.0
    {
        return Err(
            "auto-multithresh thresholds must be non-negative and smoothfactor must be > 0"
                .to_string(),
        );
    }
    Ok(())
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

fn build_auto_multithresh_clean_mask(
    geometry: ImageGeometry,
    residual: &Array4<f32>,
    max_psf_sidelobe_level: f32,
    beam: Option<BeamFit>,
    user_mask: Option<&Array2<bool>>,
    config: &AutoMultiThresholdConfig,
) -> Result<Array2<bool>, String> {
    let shape = residual.shape();
    if shape.len() != 4 {
        return Err(format!(
            "auto-multithresh residual product must be rank-4, found shape {shape:?}"
        ));
    }
    if shape[0] != geometry.nx() || shape[1] != geometry.ny() {
        return Err(format!(
            "auto-multithresh residual shape {:?} does not match image geometry {:?}",
            &shape[0..2],
            geometry.image_shape
        ));
    }
    let min_region_pixels = auto_mask_min_region_pixels(geometry, beam, config);
    let beam_shape = auto_mask_beam_shape(geometry, beam, config);
    let mut mask = user_mask
        .cloned()
        .unwrap_or_else(|| Array2::<bool>::from_elem((geometry.nx(), geometry.ny()), false));
    for stokes_index in 0..shape[2] {
        for channel_index in 0..shape[3] {
            let plane = residual
                .slice(s![.., .., stokes_index, channel_index])
                .to_owned();
            let plane_mask = auto_multithresh_plane_mask(
                &plane,
                max_psf_sidelobe_level,
                min_region_pixels,
                beam_shape,
                config,
            );
            Zip::from(&mut mask)
                .and(&plane_mask)
                .for_each(|out, generated| *out = *out || *generated);
        }
    }
    Ok(mask)
}

fn build_auto_multithresh_cube_clean_mask(
    geometry: ImageGeometry,
    dirty: &CubeImagingResult,
    restoring_beam_mode: RestoringBeamMode,
    user_mask: Option<&Array2<bool>>,
    config: &AutoMultiThresholdConfig,
) -> Result<Array4<bool>, String> {
    let residual = &dirty.residual;
    let shape = residual.shape();
    if shape.len() != 4 {
        return Err(format!(
            "auto-multithresh residual product must be rank-4, found shape {shape:?}"
        ));
    }
    if shape[0] != geometry.nx() || shape[1] != geometry.ny() {
        return Err(format!(
            "auto-multithresh residual shape {:?} does not match image geometry {:?}",
            &shape[0..2],
            geometry.image_shape
        ));
    }
    let nstokes = shape[2];
    let nchan = shape[3];
    let result_channel_count = dirty.compatibility.channel_frequencies_hz.len();
    if result_channel_count != nchan {
        return Err(format!(
            "auto-multithresh residual channel count {nchan} does not match result channel count {}",
            result_channel_count
        ));
    }
    let mask_beams = select_frontend_restored_cube_beams(&dirty.beams, restoring_beam_mode)?;
    let mut mask = Array4::<bool>::from_elem((geometry.nx(), geometry.ny(), 1, nchan), false);
    for channel_index in 0..nchan {
        let mut channel_mask = user_mask
            .cloned()
            .unwrap_or_else(|| Array2::<bool>::from_elem((geometry.nx(), geometry.ny()), false));
        let max_psf_sidelobe_level = dirty
            .diagnostics
            .channel_diagnostics
            .get(channel_index)
            .map(|diagnostics| diagnostics.max_psf_sidelobe_level)
            .unwrap_or(0.0);
        let beam = mask_beams.get(channel_index).copied().flatten();
        let min_region_pixels = auto_mask_min_region_pixels(geometry, beam, config);
        let beam_shape = auto_mask_beam_shape(geometry, beam, config);
        for stokes_index in 0..nstokes {
            let plane = residual
                .slice(s![.., .., stokes_index, channel_index])
                .to_owned();
            let plane_mask = auto_multithresh_plane_mask(
                &plane,
                max_psf_sidelobe_level,
                min_region_pixels,
                beam_shape,
                config,
            );
            Zip::from(&mut channel_mask)
                .and(&plane_mask)
                .for_each(|out, generated| *out = *out || *generated);
        }
        mask.slice_mut(s![.., .., 0, channel_index])
            .assign(&channel_mask);
    }
    Ok(mask)
}

fn auto_mask_min_region_pixels(
    geometry: ImageGeometry,
    beam: Option<BeamFit>,
    config: &AutoMultiThresholdConfig,
) -> usize {
    if config.min_beam_frac <= 0.0 {
        return 1;
    }
    let Some(beam) = beam else {
        return 1;
    };
    let cell_area = geometry.cell_size_rad[0].abs() * geometry.cell_size_rad[1].abs();
    if !(cell_area.is_finite() && cell_area > 0.0) {
        return 1;
    }
    let beam_area = std::f64::consts::PI * beam.major_fwhm_rad.abs() * beam.minor_fwhm_rad.abs()
        / (4.0 * std::f64::consts::LN_2);
    if !(beam_area.is_finite() && beam_area > 0.0) {
        return 1;
    }
    ((config.min_beam_frac as f64 * beam_area / cell_area).ceil() as usize).max(1)
}

#[derive(Debug, Clone, Copy)]
struct AutoMaskBeamShape {
    sigma_x_pixels: f64,
    sigma_y_pixels: f64,
    position_angle_rad: f64,
}

fn auto_mask_beam_shape(
    geometry: ImageGeometry,
    beam: Option<BeamFit>,
    config: &AutoMultiThresholdConfig,
) -> Option<AutoMaskBeamShape> {
    let beam = beam?;
    if config.smooth_factor <= 0.0 {
        return None;
    }
    let cell_x = geometry.cell_size_rad[0].abs();
    let cell_y = geometry.cell_size_rad[1].abs();
    if !(cell_x.is_finite() && cell_x > 0.0 && cell_y.is_finite() && cell_y > 0.0) {
        return None;
    }
    let sigma_from_fwhm = |fwhm_rad: f64, cell_rad: f64| {
        config.smooth_factor as f64 * fwhm_rad.abs()
            / (2.0 * (2.0 * std::f64::consts::LN_2).sqrt() * cell_rad)
    };
    let sigma_x_pixels = sigma_from_fwhm(beam.minor_fwhm_rad, cell_x);
    let sigma_y_pixels = sigma_from_fwhm(beam.major_fwhm_rad, cell_y);
    (sigma_x_pixels.is_finite()
        && sigma_x_pixels > 0.0
        && sigma_y_pixels.is_finite()
        && sigma_y_pixels > 0.0)
        .then_some(AutoMaskBeamShape {
            sigma_x_pixels,
            sigma_y_pixels,
            position_angle_rad: beam.position_angle_rad,
        })
}

fn auto_multithresh_plane_mask(
    residual: &Array2<f32>,
    max_psf_sidelobe_level: f32,
    min_region_pixels: usize,
    beam_shape: Option<AutoMaskBeamShape>,
    config: &AutoMultiThresholdConfig,
) -> Array2<bool> {
    let Some(stats) = robust_plane_stats(residual) else {
        return Array2::<bool>::from_elem(residual.dim(), false);
    };
    let sidelobe_threshold =
        stats.median + max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax;
    let noise_threshold = stats.median + config.noise_threshold * stats.robust_rms;
    let low_noise_threshold = stats.median
        + (max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax)
            .max(config.low_noise_threshold * stats.robust_rms);
    let main_threshold = sidelobe_threshold.max(noise_threshold);
    let mut initial = threshold_positive_mask(residual, main_threshold);
    prune_small_regions(&mut initial, min_region_pixels);
    let mut grown = smooth_and_cut_mask(&initial, beam_shape, config.cut_threshold);
    if config.grow_iterations > 0 {
        let constraint = threshold_positive_mask(residual, low_noise_threshold);
        grow_mask_constrained(&mut grown, &constraint, config.grow_iterations);
        if config.do_grow_prune {
            prune_small_regions(&mut grown, min_region_pixels);
        }
    }
    if config.negative_threshold > 0.0 {
        let negative_threshold = stats.median
            - (max_psf_sidelobe_level.max(0.0) * config.sidelobe_threshold * stats.absmax)
                .max(config.negative_threshold * stats.robust_rms);
        let mut negative = threshold_negative_mask(residual, negative_threshold);
        prune_small_regions(&mut negative, min_region_pixels);
        negative = smooth_and_cut_mask(&negative, beam_shape, config.cut_threshold);
        Zip::from(&mut grown)
            .and(&negative)
            .for_each(|out, generated| *out = *out || *generated);
    }
    grown
}

#[derive(Debug, Clone, Copy)]
struct RobustPlaneStats {
    median: f32,
    robust_rms: f32,
    absmax: f32,
}

fn robust_plane_stats(residual: &Array2<f32>) -> Option<RobustPlaneStats> {
    let mut values = residual
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let median = sorted_median(&values);
    let absmax = values
        .iter()
        .fold(0.0f32, |acc, value| acc.max(value.abs()));
    let mut deviations = values
        .iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(|a, b| a.total_cmp(b));
    let mad_rms = sorted_median(&deviations) * 1.4826;
    let rms = (values.iter().map(|value| value * value).sum::<f32>() / values.len() as f32).sqrt();
    let robust_rms = if mad_rms > 0.0 { mad_rms } else { rms };
    Some(RobustPlaneStats {
        median,
        robust_rms,
        absmax,
    })
}

fn sorted_median(values: &[f32]) -> f32 {
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        0.5 * (values[mid - 1] + values[mid])
    } else {
        values[mid]
    }
}

fn threshold_positive_mask(residual: &Array2<f32>, threshold: f32) -> Array2<bool> {
    residual.mapv(|value| value.is_finite() && value > threshold)
}

fn threshold_negative_mask(residual: &Array2<f32>, threshold: f32) -> Array2<bool> {
    residual.mapv(|value| value.is_finite() && value < threshold)
}

fn smooth_and_cut_mask(
    mask: &Array2<bool>,
    beam_shape: Option<AutoMaskBeamShape>,
    cut_threshold: f32,
) -> Array2<bool> {
    let Some(beam_shape) = beam_shape else {
        return mask.clone();
    };
    let (nx, ny) = mask.dim();
    let radius_x = (beam_shape.sigma_x_pixels * 4.0).ceil().max(1.0) as isize;
    let radius_y = (beam_shape.sigma_y_pixels * 4.0).ceil().max(1.0) as isize;
    let cos_pa = beam_shape.position_angle_rad.cos();
    let sin_pa = beam_shape.position_angle_rad.sin();
    let mut smoothed = Array2::<f32>::zeros((nx, ny));
    for ((x, y), value) in mask.indexed_iter() {
        if !*value {
            continue;
        }
        let x = x as isize;
        let y = y as isize;
        for dx in -radius_x..=radius_x {
            let xx = x + dx;
            if !(0..nx as isize).contains(&xx) {
                continue;
            }
            for dy in -radius_y..=radius_y {
                let yy = y + dy;
                if !(0..ny as isize).contains(&yy) {
                    continue;
                }
                let rotated_x = dx as f64 * cos_pa + dy as f64 * sin_pa;
                let rotated_y = -dx as f64 * sin_pa + dy as f64 * cos_pa;
                let exponent = -0.5
                    * ((rotated_x / beam_shape.sigma_x_pixels).powi(2)
                        + (rotated_y / beam_shape.sigma_y_pixels).powi(2));
                smoothed[(xx as usize, yy as usize)] += exponent.exp() as f32;
            }
        }
    }
    let peak = smoothed.iter().copied().fold(0.0f32, f32::max);
    if !(peak.is_finite() && peak > 0.0) {
        return Array2::<bool>::from_elem((nx, ny), false);
    }
    let threshold = cut_threshold.max(0.0) * peak;
    smoothed.mapv(|value| value.is_finite() && value > threshold)
}

fn grow_mask_constrained(
    mask: &mut Array2<bool>,
    constraint: &Array2<bool>,
    max_iterations: usize,
) {
    let (nx, ny) = mask.dim();
    for _ in 0..max_iterations {
        let mut next = mask.clone();
        let mut changed = false;
        for x in 0..nx {
            for y in 0..ny {
                if mask[(x, y)] || !constraint[(x, y)] {
                    continue;
                }
                if neighboring_masked(mask, x, y) {
                    next[(x, y)] = true;
                    changed = true;
                }
            }
        }
        *mask = next;
        if !changed {
            break;
        }
    }
}

fn neighboring_masked(mask: &Array2<bool>, x: usize, y: usize) -> bool {
    let (nx, ny) = mask.dim();
    (x > 0 && mask[(x - 1, y)])
        || (x + 1 < nx && mask[(x + 1, y)])
        || (y > 0 && mask[(x, y - 1)])
        || (y + 1 < ny && mask[(x, y + 1)])
}

fn prune_small_regions(mask: &mut Array2<bool>, min_pixels: usize) {
    if min_pixels <= 1 {
        return;
    }
    let (nx, ny) = mask.dim();
    let mut visited = Array2::<bool>::from_elem((nx, ny), false);
    for x0 in 0..nx {
        for y0 in 0..ny {
            if visited[(x0, y0)] || !mask[(x0, y0)] {
                continue;
            }
            let mut region = Vec::new();
            let mut queue = VecDeque::from([(x0, y0)]);
            visited[(x0, y0)] = true;
            while let Some((x, y)) = queue.pop_front() {
                region.push((x, y));
                for (nx0, ny0) in neighbors4(mask.dim(), x, y) {
                    if !visited[(nx0, ny0)] && mask[(nx0, ny0)] {
                        visited[(nx0, ny0)] = true;
                        queue.push_back((nx0, ny0));
                    }
                }
            }
            if region.len() < min_pixels {
                for (x, y) in region {
                    mask[(x, y)] = false;
                }
            }
        }
    }
}

fn neighbors4(
    (nx, ny): (usize, usize),
    x: usize,
    y: usize,
) -> impl Iterator<Item = (usize, usize)> {
    let mut neighbors = [(usize::MAX, usize::MAX); 4];
    let mut count = 0;
    if x > 0 {
        neighbors[count] = (x - 1, y);
        count += 1;
    }
    if x + 1 < nx {
        neighbors[count] = (x + 1, y);
        count += 1;
    }
    if y > 0 {
        neighbors[count] = (x, y - 1);
        count += 1;
    }
    if y + 1 < ny {
        neighbors[count] = (x, y + 1);
        count += 1;
    }
    neighbors.into_iter().take(count)
}

fn beam_to_gaussian(beam: BeamFit) -> GaussianBeam {
    GaussianBeam::new(
        beam.major_fwhm_rad,
        beam.minor_fwhm_rad,
        beam.position_angle_rad,
    )
}

fn gaussian_to_beamfit(beam: GaussianBeam) -> BeamFit {
    BeamFit {
        major_fwhm_rad: beam.major,
        minor_fwhm_rad: beam.minor,
        position_angle_rad: beam.position_angle,
    }
}

fn frontend_dirty_clean_config(psf_cutoff: f32) -> CleanConfig {
    CleanConfig {
        niter: 0,
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

fn frontend_cycle_threshold(
    peak_residual_jy_per_beam: f32,
    max_psf_sidelobe_level: f32,
    clean: CleanConfig,
) -> f32 {
    let psf_fraction = (max_psf_sidelobe_level * clean.cyclefactor)
        .clamp(clean.min_psf_fraction, clean.max_psf_fraction);
    (peak_residual_jy_per_beam * psf_fraction).max(clean.threshold_jy_per_beam)
}

fn frontend_cube_channel_clean_config(mut clean: CleanConfig, cycle_threshold: f32) -> CleanConfig {
    clean.threshold_jy_per_beam = clean.threshold_jy_per_beam.max(cycle_threshold);
    clean.cyclefactor = f32::MIN_POSITIVE;
    clean.min_psf_fraction = 0.0;
    clean.max_psf_fraction = 0.0;
    clean
}

fn frontend_peak_abs_masked(cube: &Array4<f32>, clean_mask: Option<&Array2<bool>>) -> f32 {
    let mut peak = 0.0f32;
    let plane = cube.slice(s![.., .., 0, 0]);
    for ((x, y), value) in plane.indexed_iter() {
        if clean_mask.is_some_and(|mask| !mask[(x, y)]) {
            continue;
        }
        if value.is_finite() {
            peak = peak.max(value.abs());
        }
    }
    peak
}

fn frontend_channel_clean_masks(
    geometry: ImageGeometry,
    nchan: usize,
    shared_mask: Option<&Array2<bool>>,
    channel_mask: Option<&Array4<bool>>,
) -> Result<Vec<Option<Array2<bool>>>, String> {
    let Some(channel_mask) = channel_mask else {
        return Ok(vec![shared_mask.cloned(); nchan]);
    };
    let expected = (geometry.nx(), geometry.ny(), 1, nchan);
    if channel_mask.dim() != expected {
        return Err(format!(
            "channel clean mask shape {:?} does not match cube image shape {:?}",
            channel_mask.dim(),
            expected
        ));
    }
    let mut masks = Vec::with_capacity(nchan);
    for channel_index in 0..nchan {
        let mut plane = channel_mask.slice(s![.., .., 0, channel_index]).to_owned();
        if let Some(shared_mask) = shared_mask {
            Zip::from(&mut plane)
                .and(shared_mask)
                .for_each(|out, shared| *out = *out && *shared);
        }
        masks.push(Some(plane));
    }
    Ok(masks)
}

fn final_cube_clean_mask_from_channel_masks(
    masks: &[Option<Array2<bool>>],
    nx: usize,
    ny: usize,
) -> Option<Array4<bool>> {
    if !masks.iter().any(Option::is_some) {
        return None;
    }
    let mut cube_mask = Array4::<bool>::from_elem((nx, ny, 1, masks.len()), false);
    for (channel_index, mask) in masks.iter().enumerate() {
        if let Some(mask) = mask {
            cube_mask
                .slice_mut(s![.., .., 0, channel_index])
                .assign(mask);
        }
    }
    Some(cube_mask)
}

fn select_frontend_restored_cube_beams(
    fitted_beams: &[Option<BeamFit>],
    mode: RestoringBeamMode,
) -> Result<Vec<Option<BeamFit>>, String> {
    match mode {
        RestoringBeamMode::PerPlane => Ok(fitted_beams.to_vec()),
        RestoringBeamMode::Common => {
            let Some(first) = fitted_beams.iter().flatten().next().copied() else {
                return Ok(vec![None; fitted_beams.len()]);
            };
            let mut beam_set =
                ImageBeamSet::with_shape(fitted_beams.len().max(1), 1, beam_to_gaussian(first));
            for (channel, beam) in fitted_beams.iter().enumerate() {
                if let Some(beam) = beam {
                    beam_set
                        .set_beam(Some(channel), Some(0), beam_to_gaussian(*beam))
                        .map_err(|error| {
                            format!(
                                "set common restoring beam input for channel {channel}: {error}"
                            )
                        })?;
                }
            }
            let common = beam_set
                .common_beam()
                .map_err(|error| format!("determine common restoring beam: {error}"))?;
            Ok(vec![Some(gaussian_to_beamfit(common)); fitted_beams.len()])
        }
    }
}

fn restore_frontend_model(
    model: &Array2<f32>,
    cell_size_rad: [f64; 2],
    beam: Option<BeamFit>,
) -> Array2<f32> {
    if model.iter().all(|flux| flux.abs() <= 1.0e-12) {
        return model.clone();
    }
    let Some(beam) = beam else {
        return model.clone();
    };
    let Some(kernel) = frontend_gaussian_kernel(beam, cell_size_rad, false) else {
        return model.clone();
    };
    apply_frontend_kernel(model, &kernel)
}

fn rescale_frontend_residual_to_beam(
    residual: &Array2<f32>,
    cell_size_rad: [f64; 2],
    restored_beam: BeamFit,
    fitted_beam: BeamFit,
) -> Result<Array2<f32>, String> {
    let restored = beam_to_gaussian(restored_beam);
    let fitted = beam_to_gaussian(fitted_beam);
    let Some(convolving_beam) = restored
        .deconvolving_beam(fitted)
        .map_err(|error| format!("deconvolve restoring beam: {error}"))?
    else {
        return Ok(residual.clone());
    };
    let pixel_width = cell_size_rad[0].hypot(cell_size_rad[1]);
    if convolving_beam.minor <= pixel_width {
        return Ok(residual.clone());
    }
    let Some(kernel) =
        frontend_gaussian_kernel(gaussian_to_beamfit(convolving_beam), cell_size_rad, true)
    else {
        return Ok(residual.clone());
    };
    let mut rescaled = apply_frontend_kernel(residual, &kernel);
    let area_ratio = restored.area() / fitted.area();
    rescaled.mapv_inplace(|value| (f64::from(value) * area_ratio) as f32);
    Ok(rescaled)
}

fn frontend_gaussian_kernel(
    beam: BeamFit,
    cell_size_rad: [f64; 2],
    normalize_volume: bool,
) -> Option<Vec<(isize, isize, f32)>> {
    let sigma_major = beam.major_fwhm_rad / 2.354_820_045_030_949_3;
    let sigma_minor = beam.minor_fwhm_rad / 2.354_820_045_030_949_3;
    if !(sigma_major.is_finite()
        && sigma_major > 1.0e-12
        && sigma_minor.is_finite()
        && sigma_minor > 1.0e-12)
    {
        return None;
    }
    let radius_rad = 5.0 * sigma_major.max(sigma_minor);
    let radius_x = (radius_rad / cell_size_rad[0]).ceil() as isize;
    let radius_y = (radius_rad / cell_size_rad[1]).ceil() as isize;
    let kernel_image = make_frontend_casa_gaussian_psf_image(
        (2 * radius_x + 1) as usize,
        (2 * radius_y + 1) as usize,
        cell_size_rad,
        beam,
        normalize_volume,
    );
    let mut kernel = Vec::new();
    let center_x = radius_x as usize;
    let center_y = radius_y as usize;
    for dx in -radius_x..=radius_x {
        for dy in -radius_y..=radius_y {
            let weight = kernel_image[(
                (center_x as isize + dx) as usize,
                (center_y as isize + dy) as usize,
            )];
            if weight > 1.0e-6 {
                kernel.push((dx, dy, weight));
            }
        }
    }
    Some(kernel)
}

fn make_frontend_casa_gaussian_psf_image(
    nx: usize,
    ny: usize,
    cell_size_rad: [f64; 2],
    beam: BeamFit,
    normalize_volume: bool,
) -> Array2<f32> {
    let mut image = Array2::<f32>::zeros((nx, ny));
    if !(beam.major_fwhm_rad.is_finite()
        && beam.major_fwhm_rad > 0.0
        && beam.minor_fwhm_rad.is_finite()
        && beam.minor_fwhm_rad > 0.0)
    {
        return image;
    }

    let pa = beam.position_angle_rad + std::f64::consts::FRAC_PI_2;
    let cos_pa = pa.cos();
    let sin_pa = pa.sin();
    let ref_x = (nx / 2) as f64;
    let ref_y = (ny / 2) as f64;
    let dx = cell_size_rad[0].abs();
    let dy = cell_size_rad[1].abs();
    let sbmaj = 4.0 * std::f64::consts::LN_2 * (1.0 / beam.major_fwhm_rad).powi(2);
    let sbmin = 4.0 * std::f64::consts::LN_2 * (1.0 / beam.minor_fwhm_rad).powi(2);
    let mut volume = 0.0f64;
    for y in 0..ny {
        for x in 0..nx {
            let major = cos_pa * (x as f64 - ref_x) * dx + sin_pa * (y as f64 - ref_y) * dy;
            let minor = -sin_pa * (x as f64 - ref_x) * dx + cos_pa * (y as f64 - ref_y) * dy;
            let radius = sbmaj * major.powi(2) + sbmin * minor.powi(2);
            let value = if radius < 20.0 {
                (-radius).exp_m1() + 1.0
            } else {
                0.0
            };
            image[(x, y)] = value as f32;
            volume += value;
        }
    }
    if normalize_volume && volume > 0.0 {
        image.mapv_inplace(|value| (f64::from(value) / volume) as f32);
    }
    image
}

fn apply_frontend_kernel(model: &Array2<f32>, kernel: &[(isize, isize, f32)]) -> Array2<f32> {
    let mut restored = Array2::<f32>::zeros(model.raw_dim());
    for ((center_x, center_y), flux) in model.indexed_iter() {
        if flux.abs() <= 1.0e-12 {
            continue;
        }
        for &(dx, dy, weight) in kernel {
            let x = center_x as isize + dx;
            let y = center_y as isize + dy;
            if !(0..model.shape()[0] as isize).contains(&x)
                || !(0..model.shape()[1] as isize).contains(&y)
            {
                continue;
            }
            restored[(x as usize, y as usize)] += *flux * weight;
        }
    }
    restored
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

fn chunk_density_batch(batch: VisibilityBatch, use_density_batches: bool) -> Vec<VisibilityBatch> {
    if use_density_batches {
        chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE)
    } else {
        Vec::new()
    }
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
    beam_frequencies_hz: &[f64],
    primary_beam_model: PrimaryBeamModel,
    max_batch_size: usize,
) -> Vec<VisibilityMetadataBatch> {
    debug_assert_eq!(beam_frequencies_hz.len(), samples.len());
    if samples.len() <= max_batch_size {
        return vec![VisibilityMetadataBatch {
            sample_frequency_hz: samples
                .iter()
                .map(|sample| sample.output_frequency_hz)
                .collect(),
            beam_frequency_hz: beam_frequencies_hz.to_vec(),
            primary_beam_model,
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
            primary_beam_model,
            pointing_direction_rad: slice
                .iter()
                .map(|sample| sample.pointing_direction_rad)
                .collect(),
        });
        start = end;
    }
    batches
}

fn infer_mosaic_beam_frequencies_hz(
    ms: &MeasurementSet,
    samples: &[PreparedVisibilitySampleTrace],
) -> Result<Vec<f64>, String> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let spectral_window = ms
        .spectral_window()
        .map_err(|error| format!("open SPECTRAL_WINDOW for mosaic PB channels: {error}"))?;
    let mut sample_indices_by_spw = BTreeMap::<usize, Vec<usize>>::new();
    for (sample_index, sample) in samples.iter().enumerate() {
        sample_indices_by_spw
            .entry(sample.spw_id)
            .or_default()
            .push(sample_index);
    }

    let mut beam_frequencies_hz = vec![0.0; samples.len()];
    for (spw_id, sample_indices) in sample_indices_by_spw {
        let spw_frequencies_hz = spectral_window
            .chan_freq(spw_id)
            .map_err(|error| format!("read SPECTRAL_WINDOW.CHAN_FREQ row {spw_id}: {error}"))?;
        let spw_widths_hz = spectral_window
            .chan_width(spw_id)
            .map_err(|error| format!("read SPECTRAL_WINDOW.CHAN_WIDTH row {spw_id}: {error}"))?;
        let sample_frequencies_hz = sample_indices
            .iter()
            .map(|sample_index| samples[*sample_index].output_frequency_hz)
            .collect::<Vec<_>>();
        let spw_beam_frequencies_hz = casa_simplepb_beam_frequencies_for_samples(
            &sample_frequencies_hz,
            &spw_frequencies_hz,
            &spw_widths_hz,
        );
        for (sample_index, beam_frequency_hz) in
            sample_indices.into_iter().zip(spw_beam_frequencies_hz)
        {
            beam_frequencies_hz[sample_index] = beam_frequency_hz;
        }
    }
    Ok(beam_frequencies_hz)
}

fn casa_simplepb_beam_frequencies_for_samples(
    sample_frequencies_hz: &[f64],
    spw_frequencies_hz: &[f64],
    spw_widths_hz: &[f64],
) -> Vec<f64> {
    if sample_frequencies_hz.is_empty() {
        return Vec::new();
    }
    let useful_beam_frequencies_hz = casa_simplepb_useful_beam_frequencies(
        sample_frequencies_hz,
        spw_frequencies_hz,
        spw_widths_hz,
    );
    sample_frequencies_hz
        .iter()
        .map(|sample_frequency_hz| {
            useful_beam_frequencies_hz
                .iter()
                .copied()
                .min_by(|left, right| {
                    (sample_frequency_hz - *left)
                        .abs()
                        .partial_cmp(&(sample_frequency_hz - *right).abs())
                        .expect("beam frequency deltas should be finite")
                })
                .unwrap_or(*sample_frequency_hz)
        })
        .collect()
}

fn casa_simplepb_useful_beam_frequencies(
    sample_frequencies_hz: &[f64],
    spw_frequencies_hz: &[f64],
    spw_widths_hz: &[f64],
) -> Vec<f64> {
    let mut unique_frequencies_hz = sample_frequencies_hz
        .iter()
        .copied()
        .filter(|frequency_hz| frequency_hz.is_finite() && *frequency_hz > 0.0)
        .collect::<Vec<_>>();
    unique_frequencies_hz.sort_by(|left, right| {
        left.partial_cmp(right)
            .expect("prepared sample frequencies should be finite")
    });
    unique_frequencies_hz.dedup_by(|left, right| left.to_bits() == right.to_bits());
    if unique_frequencies_hz.len() <= 1 {
        return unique_frequencies_hz;
    }

    // Mirror CASA SimplePBConvFunc::findUsefulChannels().  The important
    // detail is that the 0.5% PB-frequency ladder starts from the full SPW
    // maximum, not from the selected or interpolated channel maximum.
    let min_frequency_hz = unique_frequencies_hz[0];
    let max_frequency_hz = *unique_frequencies_hz.last().expect("non-empty");
    let orig_width_hz =
        (max_frequency_hz - min_frequency_hz) / (unique_frequencies_hz.len() - 1) as f64;
    let max_spw_frequency_hz = spw_frequencies_hz
        .iter()
        .copied()
        .filter(|frequency_hz| frequency_hz.is_finite() && *frequency_hz > 0.0)
        .fold(max_frequency_hz, f64::max);
    let spw_width_hz = spw_widths_hz
        .iter()
        .copied()
        .find(|width_hz| width_hz.is_finite() && *width_hz != 0.0)
        .map(f64::abs)
        .unwrap_or(orig_width_hz.abs());
    let mut tolerance_hz = max_spw_frequency_hz * 0.5 / 100.0;
    if tolerance_hz < orig_width_hz / 2.0 {
        tolerance_hz = orig_width_hz / 2.0;
    }
    if !(tolerance_hz.is_finite() && tolerance_hz > 0.0) {
        return unique_frequencies_hz;
    }

    let mut top_frequency_hz = max_spw_frequency_hz;
    while top_frequency_hz > max_frequency_hz {
        top_frequency_hz -= tolerance_hz;
    }
    if top_frequency_hz < min_frequency_hz {
        top_frequency_hz += tolerance_hz;
    }
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
    if beam_channel_count > unique_frequencies_hz.len() {
        beam_channel_count = unique_frequencies_hz.len();
        tolerance_hz = spw_width_hz;
        bottom_frequency_hz = min_frequency_hz;
    }
    if beam_channel_count >= unique_frequencies_hz.len().saturating_sub(1) {
        return unique_frequencies_hz;
    }
    if beam_channel_count == 0 {
        return vec![bottom_frequency_hz];
    }

    (0..beam_channel_count)
        .map(|index| bottom_frequency_hz + index as f64 * tolerance_hz)
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

fn infer_primary_beam_model(
    ms: &MeasurementSet,
    samples: &[PreparedVisibilitySampleTrace],
) -> Result<PrimaryBeamModel, String> {
    let observation = ms
        .observation()
        .map_err(|error| format!("open OBSERVATION: {error}"))?;
    let telescope_name = if observation.row_count() > 0 {
        observation
            .string(0, "TELESCOPE_NAME")
            .map_err(|error| format!("read OBSERVATION.TELESCOPE_NAME: {error}"))?
            .to_ascii_uppercase()
    } else {
        String::new()
    };
    let antenna = ms
        .antenna()
        .map_err(|error| format!("open ANTENNA: {error}"))?;
    let mut antenna_diameters = Vec::new();
    for row in 0..antenna.row_count() {
        let diameter_m = antenna
            .dish_diameter(row)
            .map_err(|error| format!("read ANTENNA.DISH_DIAMETER row {row}: {error}"))?;
        if diameter_m.is_finite() && diameter_m > 0.0 {
            antenna_diameters.push(diameter_m);
        }
    }
    let selected_diameters = samples
        .iter()
        .flat_map(|sample| [sample.antenna1_id, sample.antenna2_id])
        .filter_map(|antenna_id| {
            usize::try_from(antenna_id)
                .ok()
                .and_then(|index| antenna_diameters.get(index).copied())
        })
        .collect::<Vec<_>>();
    let selected_dish_diameter_m = selected_diameters
        .iter()
        .copied()
        .reduce(f64::max)
        .or_else(|| antenna_diameters.iter().copied().reduce(f64::max))
        .ok_or_else(|| "no positive ANTENNA.DISH_DIAMETER entries were found".to_string())?;
    let min_selected_dish_diameter_m = selected_diameters
        .iter()
        .copied()
        .reduce(f64::min)
        .unwrap_or(selected_dish_diameter_m);
    let dish_diameter_m = antenna_diameters
        .into_iter()
        .reduce(f64::max)
        .ok_or_else(|| "no positive ANTENNA.DISH_DIAMETER entries were found".to_string())?;
    // Mirror CASA's common-PB defaults for the telescopes covered by the
    // current Wave 14 gate set instead of assuming the physical dish size is
    // always the effective Airy aperture diameter.
    let model = if telescope_name.contains("ALMA") && selected_dish_diameter_m <= 7.5 {
        PrimaryBeamModel::Airy {
            dish_diameter_m: 6.25,
            blockage_diameter_m: 0.75,
        }
    } else if telescope_name.contains("ALMA") && min_selected_dish_diameter_m >= 10.0 {
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
    mosaic_pb_limit: f32,
) -> Result<GridderMode, String> {
    let phase_center_direction_rad = phase_center.angles_rad;
    let needs_mosaic = samples.iter().any(|sample| {
        direction_separation_rad(sample.pointing_direction_rad, phase_center_direction_rad) > 1.0e-8
    });
    if !needs_mosaic {
        if frontend_progress_enabled() {
            eprintln!(
                "frontend stage=infer_mfs_gridder_mode mode=standard samples={}",
                samples.len()
            );
        }
        return Ok(GridderMode::Standard);
    }
    if frontend_progress_enabled() {
        eprintln!(
            "frontend stage=infer_mfs_gridder_mode mode=mosaic samples={}",
            samples.len()
        );
    }
    let primary_beam_model = infer_primary_beam_model(ms, samples)?;
    let beam_frequencies_hz = infer_mosaic_beam_frequencies_hz(ms, samples)?;
    Ok(GridderMode::Mosaic(MosaicGridderConfig {
        phase_center_direction_rad,
        primary_beam_model,
        pb_limit: mosaic_pb_limit,
        metadata_batches: chunk_visibility_metadata_batches(
            samples,
            &beam_frequencies_hz,
            primary_beam_model,
            DEFAULT_BATCH_SIZE,
        ),
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

fn chunk_model_interpolation_batches_if_needed(
    sample_contributions: Vec<Vec<CubeModelChannelContribution>>,
    use_model_interpolation_batches: bool,
) -> Vec<CubeModelInterpolationBatch> {
    if use_model_interpolation_batches {
        chunk_model_interpolation_batches(sample_contributions, DEFAULT_BATCH_SIZE)
    } else {
        Vec::new()
    }
}

enum ComplexRow2d<'a> {
    Complex32Slice {
        values: &'a [Complex32],
        channels: usize,
    },
    Complex64Slice {
        values: &'a [Complex64],
        channels: usize,
    },
    Complex32Array(&'a ArrayD<Complex32>),
    Complex64Array(&'a ArrayD<Complex64>),
}

impl<'a> ComplexRow2d<'a> {
    fn new(data: &'a ArrayValue) -> Result<Self, String> {
        match data {
            ArrayValue::Complex32(values) => match (values.shape(), values.as_slice()) {
                ([_, channels], Some(slice)) => Ok(Self::Complex32Slice {
                    values: slice,
                    channels: *channels,
                }),
                ([_, _], None) => Ok(Self::Complex32Array(values)),
                (shape, _) => Err(format!(
                    "visibility data must be 2-D Complex32/Complex64, found shape {shape:?}"
                )),
            },
            ArrayValue::Complex64(values) => match (values.shape(), values.as_slice()) {
                ([_, channels], Some(slice)) => Ok(Self::Complex64Slice {
                    values: slice,
                    channels: *channels,
                }),
                ([_, _], None) => Ok(Self::Complex64Array(values)),
                (shape, _) => Err(format!(
                    "visibility data must be 2-D Complex32/Complex64, found shape {shape:?}"
                )),
            },
            other => Err(format!(
                "visibility data must be Complex32/Complex64, found {:?}",
                other.primitive_type()
            )),
        }
    }

    fn get(&self, corr: usize, chan: usize) -> Result<Complex32, String> {
        match self {
            Self::Complex32Slice { values, channels } => {
                values.get(corr * *channels + chan).copied().ok_or_else(|| {
                    format!("complex32 visibility index [{corr}, {chan}] out of bounds")
                })
            }
            Self::Complex64Slice { values, channels } => values
                .get(corr * *channels + chan)
                .map(|value| Complex32::new(value.re as f32, value.im as f32))
                .ok_or_else(|| {
                    format!("complex64 visibility index [{corr}, {chan}] out of bounds")
                }),
            Self::Complex32Array(values) => {
                values.get(IxDyn(&[corr, chan])).copied().ok_or_else(|| {
                    format!("complex32 visibility index [{corr}, {chan}] out of bounds")
                })
            }
            Self::Complex64Array(values) => values
                .get(IxDyn(&[corr, chan]))
                .map(|value| Complex32::new(value.re as f32, value.im as f32))
                .ok_or_else(|| {
                    format!("complex64 visibility index [{corr}, {chan}] out of bounds")
                }),
        }
    }
}

enum BoolRow2d<'a> {
    Slice { values: &'a [bool], channels: usize },
    Array(&'a ArrayD<bool>),
}

impl<'a> BoolRow2d<'a> {
    fn new(data: &'a ArrayValue) -> Result<Self, String> {
        match data {
            ArrayValue::Bool(values) => match (values.shape(), values.as_slice()) {
                ([_, channels], Some(slice)) => Ok(Self::Slice {
                    values: slice,
                    channels: *channels,
                }),
                ([_, _], None) => Ok(Self::Array(values)),
                (shape, _) => Err(format!("FLAG must be 2-D Bool, found shape {shape:?}")),
            },
            other => Err(format!(
                "FLAG must be Bool, found {:?}",
                other.primitive_type()
            )),
        }
    }

    fn get(&self, corr: usize, chan: usize) -> Result<bool, String> {
        match self {
            Self::Slice { values, channels } => values
                .get(corr * *channels + chan)
                .copied()
                .ok_or_else(|| format!("flag index [{corr}, {chan}] out of bounds")),
            Self::Array(values) => values
                .get(IxDyn(&[corr, chan]))
                .copied()
                .ok_or_else(|| format!("flag index [{corr}, {chan}] out of bounds")),
        }
    }
}

enum FloatRow1d<'a> {
    Float32Slice(&'a [f32]),
    Float64Slice(&'a [f64]),
    Float32Array(&'a ArrayD<f32>),
    Float64Array(&'a ArrayD<f64>),
}

impl<'a> FloatRow1d<'a> {
    fn new(data: &'a ArrayValue, label: &str) -> Result<Self, String> {
        match data {
            ArrayValue::Float32(values) => match (values.shape(), values.as_slice()) {
                ([_], Some(slice)) => Ok(Self::Float32Slice(slice)),
                ([_], None) => Ok(Self::Float32Array(values)),
                (shape, _) => Err(format!(
                    "{label} must be 1-D Float32/Float64, found shape {shape:?}"
                )),
            },
            ArrayValue::Float64(values) => match (values.shape(), values.as_slice()) {
                ([_], Some(slice)) => Ok(Self::Float64Slice(slice)),
                ([_], None) => Ok(Self::Float64Array(values)),
                (shape, _) => Err(format!(
                    "{label} must be 1-D Float32/Float64, found shape {shape:?}"
                )),
            },
            other => Err(format!(
                "{label} must be Float32/Float64, found {:?}",
                other.primitive_type()
            )),
        }
    }

    fn get(&self, corr: usize, label: &str) -> Result<f32, String> {
        match self {
            Self::Float32Slice(values) => values
                .get(corr)
                .copied()
                .ok_or_else(|| format!("{label} index [{corr}] out of bounds")),
            Self::Float64Slice(values) => values
                .get(corr)
                .map(|value| *value as f32)
                .ok_or_else(|| format!("{label} index [{corr}] out of bounds")),
            Self::Float32Array(values) => values
                .get(IxDyn(&[corr]))
                .copied()
                .ok_or_else(|| format!("{label} index [{corr}] out of bounds")),
            Self::Float64Array(values) => values
                .get(IxDyn(&[corr]))
                .map(|value| *value as f32)
                .ok_or_else(|| format!("{label} index [{corr}] out of bounds")),
        }
    }
}

enum FloatRow2d<'a> {
    Float32Slice { values: &'a [f32], channels: usize },
    Float64Slice { values: &'a [f64], channels: usize },
    Float32Array(&'a ArrayD<f32>),
    Float64Array(&'a ArrayD<f64>),
}

impl<'a> FloatRow2d<'a> {
    fn new(data: &'a ArrayValue, label: &str) -> Result<Self, String> {
        match data {
            ArrayValue::Float32(values) => match (values.shape(), values.as_slice()) {
                ([_, channels], Some(slice)) => Ok(Self::Float32Slice {
                    values: slice,
                    channels: *channels,
                }),
                ([_, _], None) => Ok(Self::Float32Array(values)),
                (shape, _) => Err(format!(
                    "{label} must be 2-D Float32/Float64, found shape {shape:?}"
                )),
            },
            ArrayValue::Float64(values) => match (values.shape(), values.as_slice()) {
                ([_, channels], Some(slice)) => Ok(Self::Float64Slice {
                    values: slice,
                    channels: *channels,
                }),
                ([_, _], None) => Ok(Self::Float64Array(values)),
                (shape, _) => Err(format!(
                    "{label} must be 2-D Float32/Float64, found shape {shape:?}"
                )),
            },
            other => Err(format!(
                "{label} must be Float32/Float64, found {:?}",
                other.primitive_type()
            )),
        }
    }

    fn get(&self, corr: usize, chan: usize, _label: &str) -> Option<f32> {
        match self {
            Self::Float32Slice { values, channels } => values.get(corr * *channels + chan).copied(),
            Self::Float64Slice { values, channels } => values
                .get(corr * *channels + chan)
                .map(|value| *value as f32),
            Self::Float32Array(values) => values.get(IxDyn(&[corr, chan])).copied(),
            Self::Float64Array(values) => {
                values.get(IxDyn(&[corr, chan])).map(|value| *value as f32)
            }
        }
    }
}

struct WeightRow<'a> {
    weights: FloatRow1d<'a>,
    spectrum: Option<FloatRow2d<'a>>,
}

impl<'a> WeightRow<'a> {
    fn new(
        weight_row: &'a ArrayValue,
        weight_spectrum_row: Option<&'a ArrayValue>,
    ) -> Result<Self, String> {
        Ok(Self {
            weights: FloatRow1d::new(weight_row, "WEIGHT")?,
            spectrum: weight_spectrum_row
                .map(|row| FloatRow2d::new(row, "WEIGHT_SPECTRUM"))
                .transpose()?,
        })
    }

    fn get(&self, corr: usize, chan: usize) -> Result<(f32, WeightSourceKind), String> {
        if let Some(spectrum) = &self.spectrum
            && let Some(weight) = spectrum.get(corr, chan, "WEIGHT_SPECTRUM")
        {
            return Ok((weight, WeightSourceKind::WeightSpectrum));
        }
        self.weights
            .get(corr, "WEIGHT")
            .map(|weight| (weight, WeightSourceKind::Weight))
    }
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

fn collapse_paired_model_interpolation_samples_from_batch(
    paired: &ParallelHandBatch,
    model_interpolation_samples: Vec<Vec<CubeModelChannelContribution>>,
    transform: PairCollapseTransform,
) -> Result<Vec<Vec<CubeModelChannelContribution>>, String> {
    if paired.len() != model_interpolation_samples.len() {
        return Err(format!(
            "paired cube model interpolation sample count {} does not match paired batch count {}",
            model_interpolation_samples.len(),
            paired.len()
        ));
    }
    Ok(model_interpolation_samples
        .into_iter()
        .enumerate()
        .filter_map(|(index, model_contributions)| {
            if paired.first_flagged[index] || paired.second_flagged[index] {
                return None;
            }
            let first_weight = paired.first_weight[index];
            let second_weight = paired.second_weight[index];
            if !(first_weight.is_finite()
                && first_weight > 0.0
                && second_weight.is_finite()
                && second_weight > 0.0)
            {
                return None;
            }
            let visibility = collapse_paired_visibility(
                paired.first_visibility[index],
                paired.second_visibility[index],
                transform,
            );
            if !(visibility.re.is_finite() && visibility.im.is_finite()) {
                return None;
            }
            let combined_weight = 0.5 * (first_weight + second_weight);
            (combined_weight.is_finite() && combined_weight > 0.0).then_some(model_contributions)
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
    phase_shift_m: f64,
    interpolation_frequency_hz: f64,
    contributions: &[CubeChannelContribution],
    nearest_weight: bool,
) -> Result<Option<ExplicitCubeOutputSample>, String> {
    let mut visibility = Complex32::new(0.0, 0.0);
    let mut weight = 0.0f32;
    let mut weight_source = None::<WeightSourceKind>;
    let mut sumwt_factor = 0.0f32;
    let mut nearest_weight_candidate = None::<(f32, f32, WeightSourceKind)>;

    for contribution in contributions {
        if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
            return Ok(None);
        }
        let channel_index = source_channel_indices[contribution.source_channel];
        if bool_at_2d(flags, corr_index, channel_index)? {
            return Ok(None);
        }
        let source_visibility = complex32_at_2d(data, corr_index, channel_index)?;
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
        if nearest_weight {
            match nearest_weight_candidate {
                None => {
                    nearest_weight_candidate =
                        Some((contribution.factor, source_weight, source_weight_source));
                }
                Some((best_factor, _, _)) if contribution.factor > best_factor => {
                    nearest_weight_candidate =
                        Some((contribution.factor, source_weight, source_weight_source));
                }
                _ => {}
            }
        } else {
            weight += source_weight * contribution.factor;
        }
        weight_source = Some(match weight_source {
            None => source_weight_source,
            Some(existing) => weight_source_union(existing, source_weight_source),
        });
        sumwt_factor += contribution.factor;
    }

    if nearest_weight {
        let Some((_, nearest_source_weight, nearest_source_kind)) = nearest_weight_candidate else {
            return Ok(None);
        };
        weight = nearest_source_weight;
        weight_source = Some(nearest_source_kind);
        sumwt_factor = 1.0;
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

    visibility = phase_rotate_visibility(visibility, phase_shift_m, interpolation_frequency_hz);

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

fn nearest_cube_density_contribution(
    contributions: &[CubeChannelContribution],
) -> Option<CubeChannelContribution> {
    contributions
        .iter()
        .copied()
        .filter(|contribution| contribution.factor.is_finite() && contribution.factor > 0.0)
        .max_by(|left, right| {
            left.factor
                .partial_cmp(&right.factor)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

#[allow(clippy::too_many_arguments)]
fn interpolate_paired_cube_output_sample(
    data: &ArrayValue,
    flags: &ArrayValue,
    row_weights: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    pair: (usize, usize),
    source_channel_indices: &[usize],
    phase_shift_m: f64,
    interpolation_frequency_hz: f64,
    contributions: &[CubeChannelContribution],
    nearest_weight: bool,
) -> Result<Option<PairedCubeOutputSample>, String> {
    let mut first_visibility = Complex32::new(0.0, 0.0);
    let mut second_visibility = Complex32::new(0.0, 0.0);
    let mut first_weight = 0.0f32;
    let mut second_weight = 0.0f32;
    let mut first_weight_source = None::<WeightSourceKind>;
    let mut second_weight_source = None::<WeightSourceKind>;
    let mut nearest_weight_candidate = None::<(f32, f32, WeightSourceKind, f32, WeightSourceKind)>;

    for contribution in contributions {
        if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
            return Ok(None);
        }
        let channel_index = source_channel_indices[contribution.source_channel];
        if bool_at_2d(flags, pair.0, channel_index)? || bool_at_2d(flags, pair.1, channel_index)? {
            return Ok(None);
        }
        let source_first_visibility = complex32_at_2d(data, pair.0, channel_index)?;
        let source_second_visibility = complex32_at_2d(data, pair.1, channel_index)?;
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
        if nearest_weight {
            match nearest_weight_candidate {
                None => {
                    nearest_weight_candidate = Some((
                        contribution.factor,
                        source_first_weight,
                        source_first_weight_source,
                        source_second_weight,
                        source_second_weight_source,
                    ));
                }
                Some((best_factor, _, _, _, _)) if contribution.factor > best_factor => {
                    nearest_weight_candidate = Some((
                        contribution.factor,
                        source_first_weight,
                        source_first_weight_source,
                        source_second_weight,
                        source_second_weight_source,
                    ));
                }
                _ => {}
            }
        } else {
            first_weight += source_first_weight * contribution.factor;
            second_weight += source_second_weight * contribution.factor;
        }
        first_weight_source = Some(match first_weight_source {
            None => source_first_weight_source,
            Some(existing) => weight_source_union(existing, source_first_weight_source),
        });
        second_weight_source = Some(match second_weight_source {
            None => source_second_weight_source,
            Some(existing) => weight_source_union(existing, source_second_weight_source),
        });
    }

    if nearest_weight {
        let Some((
            _,
            nearest_first_weight,
            nearest_first_source,
            nearest_second_weight,
            nearest_second_source,
        )) = nearest_weight_candidate
        else {
            return Ok(None);
        };
        first_weight = nearest_first_weight;
        second_weight = nearest_second_weight;
        first_weight_source = Some(nearest_first_source);
        second_weight_source = Some(nearest_second_source);
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

    first_visibility =
        phase_rotate_visibility(first_visibility, phase_shift_m, interpolation_frequency_hz);
    second_visibility =
        phase_rotate_visibility(second_visibility, phase_shift_m, interpolation_frequency_hz);

    Ok(Some(PairedCubeOutputSample {
        first_visibility,
        second_visibility,
        first_weight,
        second_weight,
        first_weight_source: first_weight_source.unwrap_or(WeightSourceKind::Weight),
        second_weight_source: second_weight_source.unwrap_or(WeightSourceKind::Weight),
    }))
}

#[allow(clippy::too_many_arguments)]
fn push_explicit_cube_density_sample(
    batch: &mut VisibilityBatch,
    flags: &BoolRow2d<'_>,
    weights: &WeightRow<'_>,
    corr_index: usize,
    source_channel_index: usize,
    source_frequency_hz: f64,
    uvw_m: [f64; 3],
    is_cross: bool,
) -> Result<(), String> {
    if flags.get(corr_index, source_channel_index)? {
        return Ok(());
    }
    let (weight, _) = weights.get(corr_index, source_channel_index)?;
    if !(source_frequency_hz.is_finite()
        && source_frequency_hz > 0.0
        && weight.is_finite()
        && weight > 0.0)
    {
        return Ok(());
    }
    let lambda_scale = source_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
    batch.u_lambda.push(uvw_m[0] * lambda_scale);
    batch.v_lambda.push(uvw_m[1] * lambda_scale);
    batch.w_lambda.push(uvw_m[2] * lambda_scale);
    batch.weight.push(weight);
    batch.sumwt_factor.push(1.0);
    batch.gridable.push(is_cross);
    batch.visibility.push(Complex32::new(0.0, 0.0));
    Ok(())
}

fn push_casa_cube_briggs_density_sample(
    batch: &mut VisibilityBatch,
    frequency_hz: f64,
    uvw_m: [f64; 3],
    weight: f32,
    is_cross: bool,
) {
    if !(frequency_hz.is_finite() && frequency_hz > 0.0 && weight.is_finite() && weight > 0.0) {
        return;
    }
    let lambda_scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
    batch.u_lambda.push(uvw_m[0] * lambda_scale);
    batch.v_lambda.push(uvw_m[1] * lambda_scale);
    batch.w_lambda.push(uvw_m[2] * lambda_scale);
    batch.weight.push(weight);
    batch.sumwt_factor.push(1.0);
    batch.gridable.push(is_cross);
    batch.visibility.push(Complex32::new(0.0, 0.0));
}

#[allow(clippy::too_many_arguments)]
fn push_explicit_cube_density_sample_from_contributions(
    batch: &mut VisibilityBatch,
    flags: &BoolRow2d<'_>,
    weights: &WeightRow<'_>,
    corr_index: usize,
    source_channel_indices: &[usize],
    source_lookup_frequencies_hz: Option<&[f64]>,
    contributions: &[CubeChannelContribution],
    uvw_m: [f64; 3],
    is_cross: bool,
) -> Result<(), String> {
    let Some(contribution) = nearest_cube_density_contribution(contributions) else {
        return Ok(());
    };
    let Some(&source_channel_index) = source_channel_indices.get(contribution.source_channel)
    else {
        return Ok(());
    };
    let source_frequency_hz = source_lookup_frequencies_hz
        .and_then(|frequencies| frequencies.get(contribution.source_channel))
        .copied()
        .unwrap_or(contribution.source_frequency_hz);
    let before = batch.len();
    push_explicit_cube_density_sample(
        batch,
        flags,
        weights,
        corr_index,
        source_channel_index,
        source_frequency_hz,
        uvw_m,
        is_cross,
    )?;
    if batch.len() > before {
        let index = batch.len() - 1;
        quantize_casa_briggs_lookup_uv(batch, index, source_frequency_hz, uvw_m);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_paired_cube_density_sample(
    batch: &mut VisibilityBatch,
    flags: &BoolRow2d<'_>,
    weights: &WeightRow<'_>,
    pair: (usize, usize),
    source_channel_index: usize,
    source_frequency_hz: f64,
    uvw_m: [f64; 3],
    is_cross: bool,
) -> Result<(), String> {
    if flags.get(pair.0, source_channel_index)? || flags.get(pair.1, source_channel_index)? {
        return Ok(());
    }
    let (first_weight, _) = weights.get(pair.0, source_channel_index)?;
    let (second_weight, _) = weights.get(pair.1, source_channel_index)?;
    let weight = first_weight + second_weight;
    if !(source_frequency_hz.is_finite()
        && source_frequency_hz > 0.0
        && weight.is_finite()
        && weight > 0.0)
    {
        return Ok(());
    }
    let lambda_scale = source_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
    batch.u_lambda.push(uvw_m[0] * lambda_scale);
    batch.v_lambda.push(uvw_m[1] * lambda_scale);
    batch.w_lambda.push(uvw_m[2] * lambda_scale);
    batch.weight.push(weight);
    batch.sumwt_factor.push(1.0);
    batch.gridable.push(is_cross);
    batch.visibility.push(Complex32::new(0.0, 0.0));
    Ok(())
}

fn quantize_casa_briggs_lookup_uv(
    batch: &mut VisibilityBatch,
    index: usize,
    frequency_hz: f64,
    uvw_m: [f64; 3],
) {
    let frequency_scale = (frequency_hz / SPEED_OF_LIGHT_M_PER_S) as f32;
    let casa_u = (-uvw_m[0] * f64::from(frequency_scale)) as f32;
    let casa_v = (-uvw_m[1] * f64::from(frequency_scale)) as f32;
    batch.u_lambda[index] = -f64::from(casa_u);
    batch.v_lambda[index] = -f64::from(casa_v);
}

#[allow(clippy::too_many_arguments)]
fn push_paired_cube_density_sample_from_contributions(
    batch: &mut VisibilityBatch,
    flags: &BoolRow2d<'_>,
    weights: &WeightRow<'_>,
    pair: (usize, usize),
    source_channel_indices: &[usize],
    source_lookup_frequencies_hz: Option<&[f64]>,
    contributions: &[CubeChannelContribution],
    uvw_m: [f64; 3],
    is_cross: bool,
) -> Result<(), String> {
    let Some(contribution) = nearest_cube_density_contribution(contributions) else {
        return Ok(());
    };
    let Some(&source_channel_index) = source_channel_indices.get(contribution.source_channel)
    else {
        return Ok(());
    };
    let source_frequency_hz = source_lookup_frequencies_hz
        .and_then(|frequencies| frequencies.get(contribution.source_channel))
        .copied()
        .unwrap_or(contribution.source_frequency_hz);
    let before = batch.len();
    push_paired_cube_density_sample(
        batch,
        flags,
        weights,
        pair,
        source_channel_index,
        source_frequency_hz,
        uvw_m,
        is_cross,
    )?;
    if batch.len() > before {
        let index = batch.len() - 1;
        quantize_casa_briggs_lookup_uv(batch, index, source_frequency_hz, uvw_m);
    }
    Ok(())
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
  --savemodel MODE          none or modelcolumn
  --startmodel PATH         CASA image used as the initial model for single-image MFS
  --corr XX|YY|RR|LL        explicit raw-correlation imaging
  --stokes I|Q|U|V          explicit scalar Stokes-plane imaging
  --specmode MODE           mfs, cube, or cubedata
  --weighting MODE          natural, uniform, briggs, or briggsbwtaper
  --perchanweightdensity    cube uniform/briggs density per output channel
  --usepointing             use POINTING-table directions instead of FIELD phase centers
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
  --pblimit VALUE           mosaic primary-beam cutoff for flat-noise normalization (default 0.2)
  --pbcor                   write mosaic primary-beam-corrected image products
  --minor-cycle-length N    residual refresh cadence (default 8)
  --cycleniter N            alias for --minor-cycle-length
  --cyclefactor VALUE       cycle-threshold scale factor (default 1.0)
  --minpsffraction VALUE    lower PSF-fraction clamp (default 0.05)
  --maxpsffraction VALUE    upper PSF-fraction clamp (default 0.8)
  --hogbom-iteration-mode MODE
                            strict or casa; casa mirrors CASA's inclusive hclean loop
  --casa-hogbom-iterations  alias for --hogbom-iteration-mode casa
  --usemask MODE            user or auto-multithresh
  --sidelobethreshold VALUE auto-multithresh sidelobe factor (default 3.0)
  --noisethreshold VALUE    auto-multithresh noise factor (default 5.0)
  --lownoisethreshold VALUE auto-multithresh grow-mask noise factor (default 1.5)
  --negativethreshold VALUE auto-multithresh negative-feature factor (default 0.0)
  --smoothfactor VALUE      auto-multithresh smoothing factor (default 1.0)
  --minbeamfrac VALUE       auto-multithresh pruning beam fraction (default 0.3)
  --cutthreshold VALUE      auto-multithresh smoothed-mask cutoff (default 0.01)
  --growiterations N        auto-multithresh grow iterations (default 75)
  --no-dogrowprune          skip pruning after auto-multithresh growth
  --minpercentchange VALUE  auto-multithresh percent-change stop control
  --no-fastnoise            use the slower robust-noise automask setting
  --mask-box X0,Y0,X1,Y1    inclusive clean mask box in pixel coordinates (repeatable)
  --mask-image PATH         CASA image mask whose non-zero pixels are cleanable
  --wterm MODE              none, direct, or wproject
  --wprojplanes N           explicit CASA-style wproject plane budget
  --dirty-only              write dirty/residual products without CLEAN
  --no-preview-pngs         skip writing PNG preview sidecars
  --ui-schema               emit the launcher/TUI schema
  --json-schema             emit the canonical imager task JSON schema
  --protocol-info           emit the imager task protocol descriptor
  --json-run <SOURCE>       execute one JSON ImagerTaskRequest from SOURCE or - for stdin
  -h, --help                show this help
"
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
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
        let padded = (padding_factor * image_len as f64 - 0.5).floor() as usize;
        let padded = padded.max(image_len);
        if padded % 2 == 0 { padded } else { padded + 1 }
    }

    fn test_phase_center() -> PhaseCenter {
        PhaseCenter {
            field_id: Some(0),
            angles_rad: [1.0, 0.5],
            reference: DirectionRef::J2000,
        }
    }

    fn test_visibility_batch(u_lambda: f64) -> VisibilityBatch {
        VisibilityBatch {
            u_lambda: vec![u_lambda],
            v_lambda: vec![0.0],
            w_lambda: vec![0.0],
            weight: vec![1.0],
            sumwt_factor: vec![1.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(1.0, 0.0)],
        }
    }

    fn prepared_sample_for_batching(
        index: usize,
        frequency_hz: f64,
    ) -> PreparedVisibilitySampleTrace {
        PreparedVisibilitySampleTrace {
            row_index: index,
            input_field_id: 0,
            phase_center_field_id: Some(0),
            ddid: 0,
            spw_id: 0,
            polarization_id: 0,
            antenna1_id: 0,
            antenna2_id: 1,
            is_cross: true,
            raw_uvw_m: [index as f64, 0.0, 0.0],
            imaging_uvw_m: [index as f64, 0.0, 0.0],
            phase_shift_m: 0.0,
            correlation_indices: vec![0],
            output_channel_index: Some(index),
            output_frequency_hz: frequency_hz,
            field_phase_center_direction_rad: [1.0, 0.5],
            pointing_direction_rad: [1.0 + index as f64 * 1.0e-3, 0.5],
            visibility_re: 1.0,
            visibility_im: 0.0,
            weight: 1.0,
            weight_source: WeightSourceKind::Weight,
            sumwt_factor: 1.0,
            gridable: true,
            source_contributions: Vec::new(),
        }
    }

    #[test]
    fn paired_batch_stokes_i_uses_half_sum_visibility_and_casa_style_weight() {
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

        let collapsed = collapse_paired_visibility_batch(
            &batch,
            PairCollapseTransform::HalfSum,
            PlaneStokes::I,
        )
        .unwrap();

        assert_eq!(collapsed.len(), 1);
        assert!((collapsed.visibility[0].re - 4.0).abs() < 1.0e-6);
        assert!(collapsed.visibility[0].im.abs() < 1.0e-6);
        assert!((collapsed.weight[0] - 2.25).abs() < 1.0e-6);
        assert!((collapsed.sumwt_factor[0] - 2.0).abs() < 1.0e-6);
    }

    fn test_mosaic_gridder(metadata_frequency_hz: f64) -> GridderMode {
        GridderMode::Mosaic(MosaicGridderConfig {
            phase_center_direction_rad: [1.0, 0.5],
            primary_beam_model: PrimaryBeamModel::Airy {
                dish_diameter_m: 12.0,
                blockage_diameter_m: 0.75,
            },
            pb_limit: 0.2,
            metadata_batches: vec![VisibilityMetadataBatch {
                sample_frequency_hz: vec![metadata_frequency_hz],
                beam_frequency_hz: vec![metadata_frequency_hz],
                primary_beam_model: PrimaryBeamModel::Airy {
                    dish_diameter_m: 12.0,
                    blockage_diameter_m: 0.75,
                },
                pointing_direction_rad: vec![[1.0, 0.5]],
            }],
        })
    }

    #[test]
    fn casa_simplepb_beam_frequency_buckets_start_from_spw_max() {
        let sample_frequencies_hz = vec![
            114_730_333_766.559_8,
            114_730_974_605.461_99,
            114_731_615_444.364_17,
            114_732_256_283.266_34,
            114_732_897_122.168_53,
            114_733_537_961.070_71,
        ];
        let spw_frequencies_hz = vec![113_726_419_140.625, 115_600_930_859.375];
        let beam_frequencies_hz = casa_simplepb_beam_frequencies_for_samples(
            &sample_frequencies_hz,
            &spw_frequencies_hz,
            &[488_281.25],
        );

        assert_eq!(beam_frequencies_hz.len(), sample_frequencies_hz.len());
        for beam_frequency_hz in beam_frequencies_hz {
            assert!((beam_frequency_hz - 114_444_921_550.781_25).abs() < 1.0e-3);
        }
    }

    #[test]
    fn measurement_set_paths_accepts_comma_separated_ms_list() {
        let mut config = synthetic_cube_trace_config(PathBuf::from("/tmp/one.ms,/tmp/two.ms"));

        assert_eq!(
            measurement_set_paths(&config).unwrap(),
            vec![PathBuf::from("/tmp/one.ms"), PathBuf::from("/tmp/two.ms")]
        );

        config.save_model = SaveModelMode::ModelColumn;
        assert!(validate_save_model_request(&config).is_err());
    }

    #[test]
    fn merge_prepared_inputs_appends_mfs_visibility_and_mosaic_metadata() {
        let left = PreparedInput::Mfs(PlaneInput {
            phase_center: test_phase_center(),
            freq_ref: FrequencyRef::TOPO,
            reffreq_hz: 115.0e9,
            selected_frequency_range_hz: [114.0e9, 115.0e9],
            plane_stokes: PlaneStokes::I,
            batches: vec![test_visibility_batch(10.0)],
            sample_frequency_batches_hz: vec![vec![114.5e9]],
            gridder_mode: test_mosaic_gridder(114.5e9),
        });
        let right = PreparedInput::Mfs(PlaneInput {
            phase_center: test_phase_center(),
            freq_ref: FrequencyRef::TOPO,
            reffreq_hz: 115.0e9,
            selected_frequency_range_hz: [115.0e9, 116.0e9],
            plane_stokes: PlaneStokes::I,
            batches: vec![test_visibility_batch(20.0)],
            sample_frequency_batches_hz: vec![vec![115.5e9]],
            gridder_mode: test_mosaic_gridder(115.5e9),
        });

        let PreparedInput::Mfs(merged) = merge_prepared_inputs(vec![left, right]).unwrap() else {
            panic!("expected merged MFS input");
        };

        assert_eq!(merged.batches.len(), 2);
        assert_eq!(merged.sample_frequency_batches_hz.len(), 2);
        assert_eq!(merged.selected_frequency_range_hz, [114.0e9, 116.0e9]);
        let GridderMode::Mosaic(gridder) = merged.gridder_mode else {
            panic!("expected merged mosaic gridder");
        };
        assert_eq!(gridder.metadata_batches.len(), 2);
        assert_eq!(
            gridder.metadata_batches[1].sample_frequency_hz,
            vec![115.5e9]
        );
    }

    #[test]
    fn cube_briggs_density_groups_are_separate_per_field() {
        let mut preweighting = CasaCubeBriggsPreparedWeighting {
            weighting: WeightingMode::BriggsBwTaper { robust: 0.5 },
            fractional_bandwidth: 1.0e-5,
            density_plane_count: 6,
            density_group_field_ids: Vec::new(),
            density_batches: Vec::new(),
            density_sample_rows: Vec::new(),
            channel_density_plane_indices: vec![Vec::new(), Vec::new()],
            channel_sample_rows: vec![Vec::new(), Vec::new()],
            channel_sample_source_channels: vec![Vec::new(), Vec::new()],
        };

        let first_base = casa_cube_briggs_density_group_base(&mut preweighting, 3);
        let repeated_base = casa_cube_briggs_density_group_base(&mut preweighting, 3);
        let second_base = casa_cube_briggs_density_group_base(&mut preweighting, 4);

        assert_eq!(first_base, 0);
        assert_eq!(repeated_base, 0);
        assert_eq!(second_base, 6);
        assert_eq!(preweighting.density_group_field_ids, vec![3, 4]);
        assert_eq!(preweighting.density_batches.len(), 12);
        assert_eq!(
            casa_cube_briggs_density_plane_index(&preweighting, second_base, 2),
            Some(8)
        );
    }

    #[test]
    fn cube_briggs_density_lookup_uses_casa_float_rounding() {
        let geometry = ImageGeometry {
            image_shape: [800, 800],
            cell_size_rad: [0.5 * arcsec_to_rad(), 0.5 * arcsec_to_rad()],
        };

        assert_eq!(
            casa_cube_briggs_density_cell(geometry, 36242.06640625, 7992.76708984375),
            Some((470, 385))
        );
        assert_eq!(
            casa_cube_briggs_density_cell(geometry, 36235.1875, 7996.9716796875),
            Some((470, 384))
        );
    }

    #[test]
    fn merge_cube_briggs_preweighting_offsets_second_ms_density_indices() {
        let left = CasaCubeBriggsPreparedWeighting {
            weighting: WeightingMode::BriggsBwTaper { robust: 0.5 },
            fractional_bandwidth: 1.0e-5,
            density_plane_count: 3,
            density_group_field_ids: vec![0],
            density_batches: vec![
                test_visibility_batch(10.0),
                test_visibility_batch(11.0),
                test_visibility_batch(12.0),
            ],
            density_sample_rows: vec![vec![10], vec![11], vec![12]],
            channel_density_plane_indices: vec![vec![Some(1)]],
            channel_sample_rows: vec![vec![100]],
            channel_sample_source_channels: vec![vec![Some(7)]],
        };
        let right = CasaCubeBriggsPreparedWeighting {
            weighting: WeightingMode::BriggsBwTaper { robust: 0.5 },
            fractional_bandwidth: 1.0e-5,
            density_plane_count: 3,
            density_group_field_ids: vec![0],
            density_batches: vec![
                test_visibility_batch(20.0),
                test_visibility_batch(21.0),
                test_visibility_batch(22.0),
            ],
            density_sample_rows: vec![vec![20], vec![21], vec![22]],
            channel_density_plane_indices: vec![vec![Some(1)]],
            channel_sample_rows: vec![vec![200]],
            channel_sample_source_channels: vec![vec![Some(8)]],
        };

        let merged = merge_casa_cube_briggs_preweighting(
            Some(left),
            Some(right),
            CasaCubeBriggsDensityMerge::DistinctMeasurementSet,
        )
        .unwrap()
        .unwrap();

        assert_eq!(merged.density_group_field_ids, vec![0, 0]);
        assert_eq!(merged.density_batches.len(), 6);
        assert_eq!(merged.density_batches[4].u_lambda, vec![21.0]);
        assert_eq!(merged.density_sample_rows[4], vec![21]);
        assert_eq!(
            merged.channel_density_plane_indices,
            vec![vec![Some(1), Some(4)]]
        );
        assert_eq!(merged.channel_sample_rows, vec![vec![100, 200]]);
        assert_eq!(
            merged.channel_sample_source_channels,
            vec![vec![Some(7), Some(8)]]
        );
    }

    #[test]
    fn merge_cube_briggs_preweighting_combines_same_ms_field_groups() {
        let left = CasaCubeBriggsPreparedWeighting {
            weighting: WeightingMode::BriggsBwTaper { robust: 0.5 },
            fractional_bandwidth: 1.0e-5,
            density_plane_count: 3,
            density_group_field_ids: vec![0],
            density_batches: vec![
                test_visibility_batch(10.0),
                test_visibility_batch(11.0),
                test_visibility_batch(12.0),
            ],
            density_sample_rows: vec![vec![10], vec![11], vec![12]],
            channel_density_plane_indices: vec![vec![Some(1)]],
            channel_sample_rows: vec![vec![100]],
            channel_sample_source_channels: vec![vec![Some(7)]],
        };
        let right = CasaCubeBriggsPreparedWeighting {
            weighting: WeightingMode::BriggsBwTaper { robust: 0.5 },
            fractional_bandwidth: 1.0e-5,
            density_plane_count: 3,
            density_group_field_ids: vec![0],
            density_batches: vec![
                test_visibility_batch(20.0),
                test_visibility_batch(21.0),
                test_visibility_batch(22.0),
            ],
            density_sample_rows: vec![vec![20], vec![21], vec![22]],
            channel_density_plane_indices: vec![vec![Some(1)]],
            channel_sample_rows: vec![vec![200]],
            channel_sample_source_channels: vec![vec![Some(8)]],
        };

        let merged = merge_casa_cube_briggs_preweighting(
            Some(left),
            Some(right),
            CasaCubeBriggsDensityMerge::SameMeasurementSet,
        )
        .unwrap()
        .unwrap();

        assert_eq!(merged.density_group_field_ids, vec![0]);
        assert_eq!(merged.density_batches.len(), 3);
        assert_eq!(merged.density_batches[1].u_lambda, vec![11.0, 21.0]);
        assert_eq!(merged.density_sample_rows[1], vec![11, 21]);
        assert_eq!(
            merged.channel_density_plane_indices,
            vec![vec![Some(1), Some(1)]]
        );
        assert_eq!(merged.channel_sample_rows, vec![vec![100, 200]]);
        assert_eq!(
            merged.channel_sample_source_channels,
            vec![vec![Some(7), Some(8)]]
        );
    }

    #[test]
    fn cube_spectral_coordinate_preserves_requested_rest_frequency() {
        let coord = build_spectral_coordinate(
            FrequencyRef::LSRK,
            &[372_672_490_000.0, 372_671_868_449.0],
            Some(372_672_490_000.0),
        );

        assert!((coord.rest_frequency() - 372_672_490_000.0).abs() < 1.0);
    }

    #[test]
    #[ignore = "diagnostic for TW Hydra dirty-image Rust-vs-casacore gridder isolation"]
    fn twhya_second_image_natural_dirty_prepared_samples_match_casacore_gridder() {
        let ms_path = env::var_os("CASA_RS_WAVE3_118_MS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/wdad-wave3-118/casa/twhya_selfcal.ms"));
        if !ms_path.exists() {
            return;
        }
        let config = CliConfig::parse([
            OsString::from("--ms"),
            ms_path.clone().into_os_string(),
            OsString::from("--imagename"),
            OsString::from("target/wdad-wave3-118/rust/twhya_gridder_diagnostic"),
            OsString::from("--imsize"),
            OsString::from("250"),
            OsString::from("--cell-arcsec"),
            OsString::from("0.1"),
            OsString::from("--weighting"),
            OsString::from("natural"),
            OsString::from("--niter"),
            OsString::from("0"),
            OsString::from("--no-preview-pngs"),
        ])
        .expect("parse diagnostic config");
        let ms = MeasurementSet::open(&ms_path).expect("open TW Hydra selfcal MS");
        let data_column =
            resolve_data_column(&ms, config.datacolumn.as_deref()).expect("resolve data column");
        let (prepared, prepare_trace) = prepare_plane_input_with_trace(&ms, &config, data_column)
            .expect("prepare TW Hydra MFS samples");
        let PreparedInput::Mfs(plane) = prepared else {
            panic!("expected MFS prepared input");
        };
        for (index, sample) in prepare_trace.samples.iter().take(6).enumerate() {
            eprintln!(
                "TW Hydra prepared sample {index}: row={} chan={:?} freq={:.12e} uvw=({:.12e},{:.12e},{:.12e}) weight={:.12e} sumwt_factor={:.1} gridable={}",
                sample.row_index,
                sample
                    .source_contributions
                    .first()
                    .map(|contribution| contribution.source_channel_index),
                sample.output_frequency_hz,
                sample.imaging_uvw_m[0],
                sample.imaging_uvw_m[1],
                sample.imaging_uvw_m[2],
                sample.weight,
                sample.sumwt_factor,
                sample.gridable,
            );
        }
        let geometry = ImageGeometry {
            image_shape: [config.imsize, config.imsize],
            cell_size_rad: [
                config.cell_arcsec * arcsec_to_rad(),
                config.cell_arcsec * arcsec_to_rad(),
            ],
        };
        let batches = plane.batches.clone();
        let prepared_weight_sum: f64 = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .weight
                    .iter()
                    .zip(&batch.sumwt_factor)
                    .zip(&batch.gridable)
            })
            .filter_map(|((&weight, &sumwt_factor), &gridable)| {
                (gridable && weight.is_finite() && weight > 0.0).then_some((
                    f64::from(weight),
                    f64::from(weight) * f64::from(sumwt_factor),
                ))
            })
            .map(|(_, reported)| reported)
            .sum();
        let prepared_weighted_vis = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .visibility
                    .iter()
                    .zip(&batch.weight)
                    .zip(&batch.gridable)
            })
            .filter_map(|((&visibility, &weight), &gridable)| {
                (gridable && weight.is_finite() && weight > 0.0).then_some((
                    f64::from(weight) * f64::from(visibility.re),
                    f64::from(weight) * f64::from(visibility.im),
                ))
            })
            .fold((0.0f64, 0.0f64), |acc, value| {
                (acc.0 + value.0, acc.1 + value.1)
            });
        eprintln!(
            "TW Hydra prepared aggregate: batches={} samples={} reported_sumwt={prepared_weight_sum:.9e} weighted_re={:.9e} weighted_im={:.9e}",
            batches.len(),
            batches.iter().map(VisibilityBatch::len).sum::<usize>(),
            prepared_weighted_vis.0,
            prepared_weighted_vis.1,
        );
        let rust = run_imaging(&ImagingRequest {
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
                niter: 0,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: config.w_term_mode,
            w_project_planes: config.w_project_planes,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .expect("run Rust natural dirty image");
        eprintln!(
            "TW Hydra run aggregate: result_sumwt={:.9e}",
            rust.sumwt[(0, 0, 0, 0)]
        );

        let grid_shape = [
            diagnostic_padded_len(config.imsize, 1.2),
            diagnostic_padded_len(config.imsize, 1.2),
        ];
        let scale = [
            grid_shape[0] as f64 * geometry.cell_size_rad[0],
            grid_shape[1] as f64 * geometry.cell_size_rad[1],
        ];
        let offset = [grid_shape[0] as f64 / 2.0, grid_shape[1] as f64 / 2.0];
        let mut u_lambda = Vec::new();
        let mut v_lambda = Vec::new();
        let mut visibility_re = Vec::new();
        let mut visibility_im = Vec::new();
        let mut weight = Vec::new();
        let mut gridable = Vec::new();
        for batch in &batches {
            u_lambda.extend_from_slice(&batch.u_lambda);
            v_lambda.extend_from_slice(&batch.v_lambda);
            visibility_re.extend(batch.visibility.iter().map(|value| value.re));
            visibility_im.extend(batch.visibility.iter().map(|value| value.im));
            weight.extend_from_slice(&batch.weight);
            gridable.extend_from_slice(&batch.gridable);
        }
        let cpp = match cpp_convolve_gridder_make_dirty_image_2d(
            grid_shape,
            geometry.image_shape,
            scale,
            offset,
            &u_lambda,
            &v_lambda,
            &visibility_re,
            &visibility_im,
            &weight,
            &gridable,
        ) {
            Ok(result) => result,
            Err(error) if error == "casacore C++ backend unavailable" => return,
            Err(error) => panic!("run casacore dirty-image shim: {error}"),
        };
        let rust_residual = rust.residual.slice(s![.., .., 0, 0]);
        let mut sum_sq = 0.0f64;
        let mut max_abs = 0.0f32;
        let mut peak_rust = 0.0f32;
        let mut peak_cpp = 0.0f32;
        for (&rust_value, &cpp_value) in rust_residual.iter().zip(&cpp.pixels) {
            let delta = rust_value - cpp_value;
            sum_sq += f64::from(delta) * f64::from(delta);
            max_abs = max_abs.max(delta.abs());
            peak_rust = peak_rust.max(rust_value.abs());
            peak_cpp = peak_cpp.max(cpp_value.abs());
        }
        let rms = (sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
        eprintln!(
            "TW Hydra natural dirty prepared-sample casacore-vs-rust: rms_diff={rms:.9e} max_abs_diff={max_abs:.9e} peak_rust={peak_rust:.9e} peak_cpp={peak_cpp:.9e}"
        );

        let mut casa_residual_path =
            PathBuf::from("target/wdad-wave3-118/casa/dirty_natural_second.residual");
        if !casa_residual_path.exists() {
            casa_residual_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(casa_residual_path);
        }
        if casa_residual_path.exists() {
            let casa_residual =
                PagedImage::<f32>::open(&casa_residual_path).expect("open CASA natural residual");
            let casa_pixels = casa_residual
                .get_slice(&[0, 0, 0, 0], casa_residual.shape())
                .expect("read CASA natural residual");
            let mut rust_casa_sum_sq = 0.0f64;
            let mut rust_casa_max_abs = 0.0f32;
            let mut cpp_casa_sum_sq = 0.0f64;
            let mut cpp_casa_max_abs = 0.0f32;
            let mut index = 0usize;
            for x in 0..config.imsize {
                for y in 0..config.imsize {
                    let casa_value = casa_pixels[IxDyn(&[x, y, 0, 0])];
                    let rust_delta = rust_residual[(x, y)] - casa_value;
                    let cpp_delta = cpp.pixels[index] - casa_value;
                    rust_casa_sum_sq += f64::from(rust_delta) * f64::from(rust_delta);
                    cpp_casa_sum_sq += f64::from(cpp_delta) * f64::from(cpp_delta);
                    rust_casa_max_abs = rust_casa_max_abs.max(rust_delta.abs());
                    cpp_casa_max_abs = cpp_casa_max_abs.max(cpp_delta.abs());
                    index += 1;
                }
            }
            let rust_casa_rms = (rust_casa_sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
            let cpp_casa_rms = (cpp_casa_sum_sq / cpp.pixels.len() as f64).sqrt() as f32;
            eprintln!(
                "TW Hydra natural dirty vs CASA: rust_rms={rust_casa_rms:.9e} rust_max_abs={rust_casa_max_abs:.9e} cpp_rms={cpp_casa_rms:.9e} cpp_max_abs={cpp_casa_max_abs:.9e}"
            );
        }
    }

    #[test]
    #[ignore = "diagnostic for TW Hydra Briggs weighting parity"]
    fn twhya_second_image_briggs_weighting_trace() {
        let ms_path = env::var_os("CASA_RS_WAVE3_118_MS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/wdad-wave3-118/casa/twhya_selfcal.ms"));
        if !ms_path.exists() {
            return;
        }
        let config = CliConfig::parse([
            OsString::from("--ms"),
            ms_path.clone().into_os_string(),
            OsString::from("--imagename"),
            OsString::from("target/wdad-wave3-118/rust/twhya_briggs_weighting_diagnostic"),
            OsString::from("--spw"),
            OsString::from("0"),
            OsString::from("--imsize"),
            OsString::from("250"),
            OsString::from("--cell-arcsec"),
            OsString::from("0.1"),
            OsString::from("--weighting"),
            OsString::from("briggs"),
            OsString::from("--robust"),
            OsString::from("0.5"),
            OsString::from("--niter"),
            OsString::from("0"),
            OsString::from("--no-preview-pngs"),
        ])
        .expect("parse diagnostic config");
        let ms = MeasurementSet::open(&ms_path).expect("open TW Hydra selfcal MS");
        let data_column =
            resolve_data_column(&ms, config.datacolumn.as_deref()).expect("resolve data column");
        let (prepared, prepare_trace) = prepare_plane_input_with_trace(&ms, &config, data_column)
            .expect("prepare TW Hydra MFS samples");
        let PreparedInput::Mfs(plane) = prepared else {
            panic!("expected MFS prepared input");
        };
        for (index, sample) in prepare_trace.samples.iter().take(6).enumerate() {
            eprintln!(
                "TW Hydra prepared Briggs sample {index}: row={} chan={:?} freq={:.12e} raw_uvw=({:.12e},{:.12e},{:.12e}) imaging_uvw=({:.12e},{:.12e},{:.12e}) weight={:.12e} sumwt_factor={:.1} gridable={}",
                sample.row_index,
                sample
                    .source_contributions
                    .first()
                    .map(|contribution| contribution.source_channel_index),
                sample.output_frequency_hz,
                sample.raw_uvw_m[0],
                sample.raw_uvw_m[1],
                sample.raw_uvw_m[2],
                sample.imaging_uvw_m[0],
                sample.imaging_uvw_m[1],
                sample.imaging_uvw_m[2],
                sample.weight,
                sample.sumwt_factor,
                sample.gridable,
            );
        }
        let geometry = ImageGeometry {
            image_shape: [config.imsize, config.imsize],
            cell_size_rad: [
                config.cell_arcsec * arcsec_to_rad(),
                config.cell_arcsec * arcsec_to_rad(),
            ],
        };
        let diagnostics = casa_imaging::trace_weighting(&ImagingRequest {
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
                niter: 0,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: config.w_term_mode,
            w_project_planes: config.w_project_planes,
            compatibility: CompatibilityMode::CasaStandardMfs,
        })
        .expect("trace Briggs weighting");
        let input_weight_sum: f64 = diagnostics
            .samples
            .iter()
            .filter(|sample| {
                sample.gridable && sample.input_weight.is_finite() && sample.input_weight > 0.0
            })
            .map(|sample| f64::from(sample.input_weight) * f64::from(sample.sumwt_factor))
            .sum();
        let output_weight_sum: f64 = diagnostics
            .samples
            .iter()
            .map(|sample| f64::from(sample.reported_contribution))
            .sum();
        eprintln!(
            "TW Hydra Briggs weighting aggregate: samples={} gridded={} skipped={} input_sum={input_weight_sum:.12e} output_sum={output_weight_sum:.12e} reported_sumwt={:.12e}",
            diagnostics.samples.len(),
            diagnostics.gridded_samples,
            diagnostics.skipped_samples,
            diagnostics.reported_sumwt,
        );
        for (index, sample) in diagnostics
            .samples
            .iter()
            .filter(|sample| sample.gridable)
            .take(6)
            .enumerate()
        {
            let implied_f2 = sample
                .density_weight
                .filter(|density| *density > 0.0)
                .map(|density| (sample.input_weight / sample.output_weight - 1.0) / density);
            eprintln!(
                "TW Hydra Briggs weighting sample {index}: u={:.9e} v={:.9e} density={:?} input={:.12e} output={:.12e} sumwt_factor={:.1} implied_f2={:?}",
                sample.u_lambda,
                sample.v_lambda,
                sample.density_weight,
                sample.input_weight,
                sample.output_weight,
                sample.sumwt_factor,
                implied_f2,
            );
        }
    }

    #[test]
    #[ignore = "diagnostic for TW Hydra Briggs residual-refresh parity"]
    fn twhya_second_image_briggs_residual_refresh_trace() {
        let ms_path = env::var_os("CASA_RS_WAVE3_118_MS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/wdad-wave3-118/casa/twhya_selfcal.ms"));
        if !ms_path.exists() {
            return;
        }
        let model_path = env::var_os("CASA_RS_WAVE3_118_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from("target/wdad-wave3-118/casa/second_image_current_n79.model")
            });
        let residual_path = env::var_os("CASA_RS_WAVE3_118_RESIDUAL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from("target/wdad-wave3-118/casa/second_image_current_n79.residual")
            });
        if !model_path.exists() || !residual_path.exists() {
            return;
        }
        let config = CliConfig::parse([
            OsString::from("--ms"),
            ms_path.clone().into_os_string(),
            OsString::from("--imagename"),
            OsString::from("target/wdad-wave3-118/rust/twhya_briggs_residual_diagnostic"),
            OsString::from("--spw"),
            OsString::from("0"),
            OsString::from("--imsize"),
            OsString::from("250"),
            OsString::from("--cell-arcsec"),
            OsString::from("0.1"),
            OsString::from("--weighting"),
            OsString::from("briggs"),
            OsString::from("--robust"),
            OsString::from("0.5"),
            OsString::from("--niter"),
            OsString::from("79"),
            OsString::from("--hogbom-iteration-mode"),
            OsString::from("casa"),
            OsString::from("--no-preview-pngs"),
        ])
        .expect("parse diagnostic config");
        let ms = MeasurementSet::open(&ms_path).expect("open TW Hydra selfcal MS");
        let data_column =
            resolve_data_column(&ms, config.datacolumn.as_deref()).expect("resolve data column");
        let prepared =
            prepare_plane_input(&ms, &config, data_column).expect("prepare TW Hydra MFS samples");
        let PreparedInput::Mfs(plane) = prepared else {
            panic!("expected MFS prepared input");
        };
        let model_image = PagedImage::<f32>::open(&model_path).expect("open CASA model");
        let model_pixels = model_image
            .get_slice(&[0, 0, 0, 0], model_image.shape())
            .expect("read CASA model");
        let mut model = Array2::<f32>::zeros((config.imsize, config.imsize));
        for x in 0..config.imsize {
            for y in 0..config.imsize {
                model[(x, y)] = model_pixels[IxDyn(&[x, y, 0, 0])];
            }
        }
        let geometry = ImageGeometry {
            image_shape: [config.imsize, config.imsize],
            cell_size_rad: [
                config.cell_arcsec * arcsec_to_rad(),
                config.cell_arcsec * arcsec_to_rad(),
            ],
        };
        let request = ImagingRequest {
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
                niter: config.niter,
                gain: config.gain,
                threshold_jy_per_beam: config.threshold_jy,
                nsigma: config.nsigma,
                psf_cutoff: config.psf_cutoff,
                minor_cycle_length: config.minor_cycle_length,
                cyclefactor: config.cyclefactor,
                min_psf_fraction: config.min_psf_fraction,
                max_psf_fraction: config.max_psf_fraction,
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            initial_model: None,
            w_term_mode: config.w_term_mode,
            w_project_planes: config.w_project_planes,
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let trace = casa_imaging::trace_residual_refresh(&request, &model)
            .expect("trace TW Hydra residual refresh");
        let casa_residual =
            PagedImage::<f32>::open(&residual_path).expect("open CASA residual image");
        let casa_pixels = casa_residual
            .get_slice(&[0, 0, 0, 0], casa_residual.shape())
            .expect("read CASA residual image");
        let mut rust_casa_sum_sq = 0.0f64;
        let mut rust_casa_max_abs = 0.0f32;
        let mut peak_rust = 0.0f32;
        let mut peak_casa = 0.0f32;
        for x in 0..config.imsize {
            for y in 0..config.imsize {
                let rust_value = trace.residual_image[(x, y)];
                let casa_value = casa_pixels[IxDyn(&[x, y, 0, 0])];
                let delta = rust_value - casa_value;
                rust_casa_sum_sq += f64::from(delta) * f64::from(delta);
                rust_casa_max_abs = rust_casa_max_abs.max(delta.abs());
                peak_rust = peak_rust.max(rust_value.abs());
                peak_casa = peak_casa.max(casa_value.abs());
            }
        }
        let rust_casa_rms =
            (rust_casa_sum_sq / (config.imsize * config.imsize) as f64).sqrt() as f32;
        eprintln!(
            "TW Hydra Briggs residual refresh vs CASA n79: rms_diff={rust_casa_rms:.9e} max_abs_diff={rust_casa_max_abs:.9e} peak_rust={peak_rust:.9e} peak_casa={peak_casa:.9e} norm_sumwt={:.9e} reported_sumwt={:.9e} psf_peak={:.9e} samples={} gridded={} skipped={}",
            trace.normalization_sumwt,
            trace.reported_sumwt,
            trace.psf_peak,
            trace.samples.len(),
            trace.gridded_samples,
            trace.skipped_samples,
        );
        for batch_index in 0..2 {
            let Some(batch) = request.visibility_batches.get(batch_index) else {
                continue;
            };
            let mut weighted_re_sum = 0.0f64;
            let mut weighted_im_sum = 0.0f64;
            let mut used_weight_sum = 0.0f64;
            let mut used_samples = 0usize;
            for sample in trace
                .samples
                .iter()
                .filter(|sample| sample.batch_index == batch_index)
            {
                let sumwt_factor = batch.sumwt_factor[sample.sample_index];
                let usable = sample.gridable
                    && sample.weight.is_finite()
                    && sample.weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0
                    && sample.residual_visibility.re.is_finite()
                    && sample.residual_visibility.im.is_finite();
                if !usable {
                    continue;
                }
                let residual_weight = f64::from(sample.weight) * f64::from(sumwt_factor);
                used_weight_sum += residual_weight;
                weighted_re_sum += f64::from(sample.residual_visibility.re) * residual_weight;
                weighted_im_sum += f64::from(sample.residual_visibility.im) * residual_weight;
                used_samples += 1;
            }
            eprintln!(
                "TW Hydra Briggs residual refresh batch {batch_index}: used_samples={used_samples} used_weight_sum={used_weight_sum:.12e} weighted_re_sum={weighted_re_sum:.12e} weighted_im_sum={weighted_im_sum:.12e}"
            );
        }
        for prefix_len in [58_752usize, 117_504usize] {
            let mut weighted_re_sum = 0.0f64;
            let mut weighted_im_sum = 0.0f64;
            let mut used_weight_sum = 0.0f64;
            let mut used_samples = 0usize;
            for sample in trace.samples.iter().take(prefix_len) {
                let batch = &request.visibility_batches[sample.batch_index];
                let sumwt_factor = batch.sumwt_factor[sample.sample_index];
                let usable = sample.gridable
                    && sample.weight.is_finite()
                    && sample.weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0
                    && sample.residual_visibility.re.is_finite()
                    && sample.residual_visibility.im.is_finite();
                if !usable {
                    continue;
                }
                let residual_weight = f64::from(sample.weight) * f64::from(sumwt_factor);
                used_weight_sum += residual_weight;
                weighted_re_sum += f64::from(sample.residual_visibility.re) * residual_weight;
                weighted_im_sum += f64::from(sample.residual_visibility.im) * residual_weight;
                used_samples += 1;
            }
            eprintln!(
                "TW Hydra Briggs residual refresh prefix {prefix_len}: used_samples={used_samples} used_weight_sum={used_weight_sum:.12e} weighted_re_sum={weighted_re_sum:.12e} weighted_im_sum={weighted_im_sum:.12e}"
            );
        }
        for sample in trace.samples.iter().take(6) {
            let batch = &request.visibility_batches[sample.batch_index];
            eprintln!(
                "TW Hydra Briggs residual refresh sample batch={} sample={} u={:.12e} v={:.12e} weight={:.12e} sumwt_factor={:.1} observed=({:.12e},{:.12e}) predicted=({:.12e},{:.12e}) residual=({:.12e},{:.12e}) gridable={}",
                sample.batch_index,
                sample.sample_index,
                sample.u_lambda,
                sample.v_lambda,
                sample.weight,
                batch.sumwt_factor[sample.sample_index],
                sample.observed_visibility.re,
                sample.observed_visibility.im,
                sample.predicted_visibility.re,
                sample.predicted_visibility.im,
                sample.residual_visibility.re,
                sample.residual_visibility.im,
                sample.gridable,
            );
        }

        let grid_shape = [
            diagnostic_padded_len(config.imsize, 1.2),
            diagnostic_padded_len(config.imsize, 1.2),
        ];
        let scale = [
            grid_shape[0] as f64 * geometry.cell_size_rad[0],
            grid_shape[1] as f64 * geometry.cell_size_rad[1],
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
        let visibility_re = trace
            .samples
            .iter()
            .map(|sample| sample.observed_visibility.re)
            .collect::<Vec<_>>();
        let visibility_im = trace
            .samples
            .iter()
            .map(|sample| sample.observed_visibility.im)
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
        let cpp = match cpp_convolve_gridder_make_model_residual_image_2d(
            grid_shape,
            geometry.image_shape,
            scale,
            offset,
            &u_lambda,
            &v_lambda,
            &visibility_re,
            &visibility_im,
            &weight,
            &gridable,
            model.as_slice().unwrap(),
        ) {
            Ok(result) => result,
            Err(error) if error == "casacore C++ backend unavailable" => return,
            Err(error) => panic!("run casacore model-residual shim: {error}"),
        };
        let mut rust_cpp_sum_sq = 0.0f64;
        let mut rust_cpp_max_abs = 0.0f32;
        let mut cpp_casa_sum_sq = 0.0f64;
        let mut cpp_casa_max_abs = 0.0f32;
        let mut cpp_index = 0usize;
        for x in 0..config.imsize {
            for y in 0..config.imsize {
                let rust_value = trace.residual_image[(x, y)];
                let cpp_value = cpp.pixels[cpp_index];
                let casa_value = casa_pixels[IxDyn(&[x, y, 0, 0])];
                let rust_cpp_delta = rust_value - cpp_value;
                let cpp_casa_delta = cpp_value - casa_value;
                rust_cpp_sum_sq += f64::from(rust_cpp_delta) * f64::from(rust_cpp_delta);
                cpp_casa_sum_sq += f64::from(cpp_casa_delta) * f64::from(cpp_casa_delta);
                rust_cpp_max_abs = rust_cpp_max_abs.max(rust_cpp_delta.abs());
                cpp_casa_max_abs = cpp_casa_max_abs.max(cpp_casa_delta.abs());
                cpp_index += 1;
            }
        }
        let pixels = (config.imsize * config.imsize) as f64;
        eprintln!(
            "TW Hydra Briggs residual refresh C++ shim: rust_cpp_rms={:.9e} rust_cpp_max_abs={rust_cpp_max_abs:.9e} cpp_casa_rms={:.9e} cpp_casa_max_abs={cpp_casa_max_abs:.9e}",
            (rust_cpp_sum_sq / pixels).sqrt(),
            (cpp_casa_sum_sq / pixels).sqrt(),
        );
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
        assert!(!config.use_pointing);
        assert_eq!(config.w_term_mode, WTermMode::None);
        assert_eq!(config.min_psf_fraction, 0.05);
        assert!(config.write_preview_pngs);
    }

    #[test]
    fn canonical_manifest_names_cover_cube_and_imaging_modes() {
        assert_eq!(canonical_spectral_mode_name(SpectralMode::Mfs), "mfs");
        assert_eq!(canonical_spectral_mode_name(SpectralMode::Cube), "cube");
        assert_eq!(
            canonical_spectral_mode_name(SpectralMode::Cubedata),
            "cubedata"
        );
        assert_eq!(
            canonical_data_column_name(VisibilityDataColumn::Data),
            "DATA"
        );
        assert_eq!(canonical_weighting_name(WeightingMode::Natural), "natural");
        assert_eq!(canonical_weighting_name(WeightingMode::Uniform), "uniform");
        assert_eq!(
            canonical_weighting_name(WeightingMode::Briggs { robust: -0.5 }),
            "briggs:-0.5"
        );
        assert_eq!(
            canonical_weighting_name(WeightingMode::BriggsBwTaper { robust: 1.25 }),
            "briggsbwtaper:1.25"
        );
        assert_eq!(
            canonical_restoring_beam_mode_name(RestoringBeamMode::PerPlane),
            "per_plane"
        );
        assert_eq!(
            canonical_restoring_beam_mode_name(RestoringBeamMode::Common),
            "common"
        );
        assert_eq!(canonical_deconvolver_name(Deconvolver::Hogbom), "hogbom");
        assert_eq!(canonical_deconvolver_name(Deconvolver::Mtmfs), "mtmfs");
        assert_eq!(canonical_deconvolver_name(Deconvolver::Clark), "clark");
        assert_eq!(
            canonical_deconvolver_name(Deconvolver::Multiscale),
            "multiscale"
        );
        assert_eq!(
            canonical_hogbom_iteration_mode_name(HogbomIterationMode::Strict),
            "strict"
        );
        assert_eq!(
            canonical_hogbom_iteration_mode_name(HogbomIterationMode::CasaInclusive),
            "casa"
        );
        assert_eq!(canonical_w_term_mode_name(WTermMode::None), "none");
        assert_eq!(canonical_w_term_mode_name(WTermMode::Direct), "direct");
        assert_eq!(canonical_w_term_mode_name(WTermMode::WProject), "wproject");
        assert_eq!(
            canonical_cube_interpolation_name(CubeInterpolation::Nearest),
            "nearest"
        );
        assert_eq!(
            canonical_cube_interpolation_name(CubeInterpolation::Linear),
            "linear"
        );
        assert_eq!(
            canonical_cube_interpolation_name(CubeInterpolation::Cubic),
            "cubic"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::FrequencyHz {
                hz: 1.42e9,
                frame: Some(FrequencyRef::LSRK),
            }),
            "frequency_hz:1420000000@LSRK"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::VelocityMs {
                ms: -12_300.0,
                frame: Some(FrequencyRef::BARY),
            }),
            "velocity_ms:-12300@BARY"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::Doppler {
                value: 0.01,
                convention: DopplerRef::RADIO,
            }),
            "doppler:0.01@RADIO"
        );
        assert_eq!(
            canonical_uv_taper(GaussianUvTaper {
                major: UvTaperSize::ImageFwhmRad(1.0e-4),
                minor: UvTaperSize::BaselineHwhmLambda(500.0),
                position_angle_rad: 0.25,
            }),
            "major=image_fwhm_rad:0.0001,minor=baseline_hwhm_lambda:500,pa_rad=0.25"
        );
    }

    #[test]
    fn parse_modes_accept_aliases_and_reject_unsupported_values() {
        assert_eq!(parse_spectral_mode("MFS").unwrap(), SpectralMode::Mfs);
        assert_eq!(parse_spectral_mode("cube").unwrap(), SpectralMode::Cube);
        assert_eq!(
            parse_spectral_mode("cubedata").unwrap(),
            SpectralMode::Cubedata
        );
        assert!(
            parse_spectral_mode("channel")
                .unwrap_err()
                .contains("specmode")
        );

        assert_eq!(
            parse_weighting_mode("natural", 0.0).unwrap(),
            WeightingMode::Natural
        );
        assert_eq!(
            parse_weighting_mode("uniform", 0.0).unwrap(),
            WeightingMode::Uniform
        );
        assert_eq!(
            parse_weighting_mode("robust", 0.75).unwrap(),
            WeightingMode::Briggs { robust: 0.75 }
        );
        assert_eq!(
            parse_weighting_mode("briggsbwtaper", -1.5).unwrap(),
            WeightingMode::BriggsBwTaper { robust: -1.5 }
        );
        assert!(
            parse_weighting_mode("radial", 0.0)
                .unwrap_err()
                .contains("weighting")
        );

        assert_eq!(parse_deconvolver("hogbom").unwrap(), Deconvolver::Hogbom);
        assert_eq!(parse_deconvolver("mtmfs").unwrap(), Deconvolver::Mtmfs);
        assert_eq!(parse_deconvolver("clark").unwrap(), Deconvolver::Clark);
        assert_eq!(
            parse_deconvolver("multiscale").unwrap(),
            Deconvolver::Multiscale
        );
        assert!(
            parse_deconvolver("mem")
                .unwrap_err()
                .contains("deconvolver")
        );

        assert_eq!(
            parse_hogbom_iteration_mode("casa-inclusive").unwrap(),
            HogbomIterationMode::CasaInclusive
        );
        assert_eq!(
            parse_hogbom_iteration_mode("inclusive").unwrap(),
            HogbomIterationMode::CasaInclusive
        );
        assert_eq!(
            parse_hogbom_iteration_mode("strict").unwrap(),
            HogbomIterationMode::Strict
        );
        assert!(
            parse_hogbom_iteration_mode("loose")
                .unwrap_err()
                .contains("hogbom")
        );

        assert_eq!(parse_w_term_mode("2d").unwrap(), WTermMode::None);
        assert_eq!(parse_w_term_mode("direct").unwrap(), WTermMode::Direct);
        assert_eq!(parse_w_term_mode("wproject").unwrap(), WTermMode::WProject);
        assert!(
            parse_w_term_mode("awproject")
                .unwrap_err()
                .contains("wterm")
        );
    }

    #[test]
    fn mask_boxes_validate_bounds_and_fill_inclusive_pixels() {
        assert_eq!(parse_mask_box("1, 2, 3, 4").unwrap(), [1, 2, 3, 4]);
        assert!(parse_mask_box("1,2,3").unwrap_err().contains("expects"));

        let mask = build_clean_mask(6, &[[1, 2, 3, 4]], None)
            .unwrap()
            .expect("mask");
        assert!(mask[(1, 2)]);
        assert!(mask[(3, 4)]);
        assert!(!mask[(0, 0)]);
        assert!(
            build_clean_mask(6, &[[4, 1, 3, 2]], None)
                .unwrap_err()
                .contains("x0 <= x1")
        );
        assert!(
            build_clean_mask(6, &[[0, 0, 6, 1]], None)
                .unwrap_err()
                .contains("exceeds image bounds")
        );
        assert!(build_clean_mask(6, &[], None).unwrap().is_none());
    }

    #[test]
    fn cli_parses_auto_multithresh_controls() {
        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("demo.ms"),
            OsString::from("--imagename"),
            OsString::from("out/demo"),
            OsString::from("--imsize"),
            OsString::from("64"),
            OsString::from("--cell-arcsec"),
            OsString::from("1.5"),
            OsString::from("--usemask"),
            OsString::from("auto-multithresh"),
            OsString::from("--sidelobethreshold"),
            OsString::from("2.0"),
            OsString::from("--noisethreshold"),
            OsString::from("4.25"),
            OsString::from("--lownoisethreshold"),
            OsString::from("1.5"),
            OsString::from("--negativethreshold"),
            OsString::from("3.0"),
            OsString::from("--minbeamfrac"),
            OsString::from("0.1"),
            OsString::from("--growiterations"),
            OsString::from("12"),
            OsString::from("--no-dogrowprune"),
            OsString::from("--no-fastnoise"),
        ])
        .unwrap();

        assert_eq!(config.use_mask, CleanMaskMode::AutoMultiThreshold);
        assert_eq!(config.auto_mask.sidelobe_threshold, 2.0);
        assert_eq!(config.auto_mask.noise_threshold, 4.25);
        assert_eq!(config.auto_mask.low_noise_threshold, 1.5);
        assert_eq!(config.auto_mask.negative_threshold, 3.0);
        assert_eq!(config.auto_mask.min_beam_frac, 0.1);
        assert_eq!(config.auto_mask.grow_iterations, 12);
        assert!(!config.auto_mask.do_grow_prune);
        assert!(!config.auto_mask.fast_noise);
    }

    #[test]
    fn auto_multithresh_mask_thresholds_grows_and_prunes_regions() {
        let mut residual = Array2::<f32>::zeros((9, 9));
        residual[(4, 4)] = 10.0;
        residual[(4, 5)] = 4.0;
        residual[(5, 4)] = 4.0;
        residual[(0, 0)] = 9.0;
        let config = AutoMultiThresholdConfig {
            sidelobe_threshold: 0.0,
            noise_threshold: 2.0,
            low_noise_threshold: 0.4,
            min_beam_frac: 0.0,
            grow_iterations: 2,
            ..AutoMultiThresholdConfig::default()
        };

        let mask = auto_multithresh_plane_mask(&residual, 0.0, 2, None, &config);
        assert!(mask[(4, 4)]);
        assert!(mask[(4, 5)]);
        assert!(mask[(5, 4)]);
        assert!(!mask[(0, 0)], "single-pixel island should be pruned");
    }

    #[test]
    fn auto_multithresh_cube_mask_keeps_channels_separate() {
        let geometry = ImageGeometry {
            image_shape: [9, 9],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let mut residual = Array4::<f32>::zeros((9, 9, 1, 2));
        residual[(4, 4, 0, 0)] = 10.0;
        residual[(1, 7, 0, 1)] = 10.0;
        let diagnostics = |max_psf_sidelobe_level| ImagingDiagnostics {
            warnings: Vec::new(),
            gridded_samples: 0,
            skipped_samples: 0,
            major_cycles: 0,
            minor_iterations: 0,
            clean_stop_reason: None,
            minor_cycle_traces: Vec::new(),
            initial_residual_peak_jy_per_beam: 0.0,
            final_residual_peak_jy_per_beam: 0.0,
            max_abs_w_lambda: 0.0,
            fractional_bandwidth: 0.0,
            max_psf_sidelobe_level,
            final_cycle_threshold_jy_per_beam: 0.0,
            clean_mask_pixels: 0,
            beam_fit_attempts: 0,
            beam_fit_cutoff_used: None,
            beam_fit_debug: None,
            mosaic_weight_image: None,
            stage_timings: ImagingStageTimings::default(),
        };
        let dirty = CubeImagingResult {
            psf: Array4::<f32>::zeros((9, 9, 1, 2)),
            residual,
            model: Array4::<f32>::zeros((9, 9, 1, 2)),
            image: Array4::<f32>::zeros((9, 9, 1, 2)),
            sumwt: Array4::<f32>::zeros((1, 1, 1, 2)),
            clean_mask: None,
            beams: vec![None, None],
            restored_beams: vec![None, None],
            diagnostics: CubeImagingDiagnostics {
                warnings: Vec::new(),
                gridded_samples: 0,
                skipped_samples: 0,
                major_cycles: 0,
                minor_iterations: 0,
                clean_stop_reason: None,
                channel_diagnostics: vec![diagnostics(0.1), diagnostics(0.1)],
                stage_timings: ImagingStageTimings::default(),
            },
            compatibility: CompatibilityMetadata {
                axis_order: [
                    AxisKind::RightAscension,
                    AxisKind::Declination,
                    AxisKind::Stokes,
                    AxisKind::Frequency,
                ],
                plane_stokes: PlaneStokes::I,
                reffreq_hz: 1.5,
                channel_frequencies_hz: vec![1.0, 2.0],
                psf_units: String::new(),
                residual_units: "Jy/beam".to_string(),
                model_units: "Jy/pixel".to_string(),
                image_units: "Jy/beam".to_string(),
            },
        };
        let config = AutoMultiThresholdConfig {
            sidelobe_threshold: 0.5,
            noise_threshold: 0.0,
            low_noise_threshold: 0.0,
            min_beam_frac: 0.0,
            grow_iterations: 0,
            ..AutoMultiThresholdConfig::default()
        };

        let mask = build_auto_multithresh_cube_clean_mask(
            geometry,
            &dirty,
            RestoringBeamMode::PerPlane,
            None,
            &config,
        )
        .unwrap();

        assert!(mask[(4, 4, 0, 0)]);
        assert!(!mask[(4, 4, 0, 1)]);
        assert!(mask[(1, 7, 0, 1)]);
        assert!(!mask[(1, 7, 0, 0)]);
    }

    #[test]
    fn stokes_pair_selection_follows_linear_and_circular_bases() {
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::I, &[9, 12]).unwrap(),
            ((0, 1), PairCollapseTransform::HalfSum)
        );
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::Q, &[9, 12]).unwrap(),
            ((0, 1), PairCollapseTransform::HalfDifference)
        );
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::U, &[10, 11]).unwrap(),
            ((0, 1), PairCollapseTransform::HalfSum)
        );
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::V, &[10, 11]).unwrap(),
            ((0, 1), PairCollapseTransform::NegativeHalfImagDifference)
        );
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::Q, &[6, 7]).unwrap(),
            ((0, 1), PairCollapseTransform::HalfSum)
        );
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::U, &[6, 7]).unwrap(),
            ((0, 1), PairCollapseTransform::PositiveHalfImagDifference)
        );
        assert_eq!(
            derive_stokes_pair_selection(PlaneStokes::V, &[5, 8]).unwrap(),
            ((0, 1), PairCollapseTransform::HalfDifference)
        );
        assert!(
            derive_stokes_pair_selection(PlaneStokes::XX, &[9, 12])
                .unwrap_err()
                .contains("raw correlation")
        );
        assert!(
            derive_stokes_pair_selection(PlaneStokes::I, &[10, 11])
                .unwrap_err()
                .contains("Stokes I")
        );
    }

    #[test]
    fn beam_and_restoration_helpers_cover_common_edge_cases() {
        let fitted = vec![
            Some(BeamFit {
                major_fwhm_rad: 3.0e-4,
                minor_fwhm_rad: 2.0e-4,
                position_angle_rad: 0.1,
            }),
            Some(BeamFit {
                major_fwhm_rad: 3.2e-4,
                minor_fwhm_rad: 2.1e-4,
                position_angle_rad: 0.12,
            }),
        ];
        assert_eq!(
            select_frontend_restored_cube_beams(&fitted, RestoringBeamMode::PerPlane).unwrap(),
            fitted
        );
        let common = select_frontend_restored_cube_beams(&fitted, RestoringBeamMode::Common)
            .expect("common beam");
        assert_eq!(common.len(), 2);
        assert!(common.iter().all(Option::is_some));
        assert_eq!(
            select_frontend_restored_cube_beams(&[None, None], RestoringBeamMode::Common).unwrap(),
            vec![None, None]
        );

        let invalid_kernel = frontend_gaussian_kernel(
            BeamFit {
                major_fwhm_rad: 0.0,
                minor_fwhm_rad: 2.0e-4,
                position_angle_rad: 0.0,
            },
            [1.0e-4, 1.0e-4],
            false,
        );
        assert!(invalid_kernel.is_none());

        let psf =
            make_frontend_casa_gaussian_psf_image(7, 7, [1.0e-4, 1.0e-4], fitted[0].unwrap(), true);
        assert!((psf.sum() - 1.0).abs() < 1.0e-5);

        let mut model = Array2::<f32>::zeros((7, 7));
        model[(3, 3)] = 2.0;
        let restored = restore_frontend_model(&model, [1.0e-4, 1.0e-4], fitted[0]);
        assert!(restored[(3, 3)] > 0.0);
        assert!(restored.sum() > model[(3, 3)]);

        let empty = Array2::<f32>::zeros((7, 7));
        assert_eq!(
            restore_frontend_model(&empty, [1.0e-4, 1.0e-4], fitted[0]),
            empty
        );
        let residual = Array2::<f32>::from_elem((5, 5), 1.0);
        let unchanged = rescale_frontend_residual_to_beam(
            &residual,
            [1.0e-4, 1.0e-4],
            fitted[0].unwrap(),
            fitted[0].unwrap(),
        )
        .expect("rescale");
        assert_eq!(unchanged, residual);
    }

    #[test]
    fn frontend_frequency_and_batch_helpers_preserve_sample_alignment() {
        let samples = (0..5)
            .map(|index| prepared_sample_for_batching(index, 1.40e9 + index as f64 * 1.0e6))
            .collect::<Vec<_>>();
        let frequency_batches = chunk_sample_frequencies_hz_from_samples(&samples, 2);
        assert_eq!(
            frequency_batches,
            vec![vec![1.40e9, 1.401e9], vec![1.402e9, 1.403e9], vec![1.404e9],]
        );

        let beam_frequencies = casa_simplepb_beam_frequencies_for_samples(
            &[1.40e9, 1.401e9, 1.402e9, 1.403e9, 1.404e9],
            &[1.40e9, 1.402e9, 1.404e9],
            &[1.0e6, 1.0e6, 1.0e6],
        );
        assert_eq!(beam_frequencies.len(), 5);
        assert!(beam_frequencies.windows(2).all(|pair| pair[0] <= pair[1]));

        let metadata_batches = chunk_visibility_metadata_batches(
            &samples,
            &beam_frequencies,
            PrimaryBeamModel::EvlaLBandCommon,
            2,
        );
        assert_eq!(metadata_batches.len(), 3);
        assert_eq!(
            metadata_batches[0].sample_frequency_hz,
            vec![1.40e9, 1.401e9]
        );
        assert_eq!(metadata_batches[2].pointing_direction_rad.len(), 1);

        let mut batch = empty_visibility_batch(0);
        for index in 0..5 {
            batch.u_lambda.push(index as f64);
            batch.v_lambda.push(index as f64 + 10.0);
            batch.w_lambda.push(index as f64 + 20.0);
            batch.weight.push(1.0 + index as f32);
            batch.sumwt_factor.push(0.5);
            batch.gridable.push(index % 2 == 0);
            batch.visibility.push(Complex32::new(index as f32, 0.0));
        }
        let chunks = chunk_visibility_batch(batch, 2);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[1].u_lambda, vec![2.0, 3.0]);
        assert_eq!(chunks[2].visibility, vec![Complex32::new(4.0, 0.0)]);
        assert!(chunk_density_batch(test_visibility_batch(1.0), false).is_empty());
        assert_eq!(
            chunk_density_batch(test_visibility_batch(1.0), true).len(),
            1
        );
    }

    #[test]
    fn spherical_direction_helpers_handle_ra_wrap_and_antipodal_fallback() {
        let left = [std::f64::consts::TAU - 1.0e-4, 0.1];
        let right = [1.0e-4, 0.1];
        assert!(direction_separation_rad(left, right) < 3.0e-4);

        let combined = combine_pointing_direction_rad(left, right);
        assert!(combined[0] < 1.0e-3 || combined[0] > std::f64::consts::TAU - 1.0e-3);
        assert!((combined[1] - 0.1).abs() < 1.0e-6);

        let antipodal = combine_pointing_direction_rad([0.0, 0.0], [std::f64::consts::PI, 0.0]);
        assert!((antipodal[0] - std::f64::consts::FRAC_PI_2).abs() < 1.0e-12);
        assert_eq!(antipodal[1], 0.0);
        assert_eq!(fractional_bandwidth_from_range([90.0, 110.0]), 0.2);
        assert_eq!(fractional_bandwidth_from_range([0.0, 110.0]), 0.0);
    }

    #[test]
    fn frontend_cube_fixed_cycle_threshold_uses_casa_controller_threshold() {
        let clean = CleanConfig {
            niter: 32,
            gain: 0.1,
            threshold_jy_per_beam: 0.005,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            minor_cycle_length: 32,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
        };

        let channel_clean = frontend_cube_channel_clean_config(clean, 0.24860433);

        assert_eq!(channel_clean.threshold_jy_per_beam, 0.24860433);
        assert_eq!(channel_clean.cyclefactor, f32::MIN_POSITIVE);
        assert_eq!(channel_clean.min_psf_fraction, 0.0);
        assert_eq!(channel_clean.max_psf_fraction, 0.0);
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
            OsString::from("--usepointing"),
            OsString::from("--wterm"),
            OsString::from("direct"),
            OsString::from("--pblimit"),
            OsString::from("0.2"),
            OsString::from("--pbcor"),
        ])
        .unwrap();
        assert_eq!(config.weighting, WeightingMode::Briggs { robust: -1.0 });
        assert_eq!(config.mask_boxes, vec![[1, 2, 10, 20], [4, 5, 6, 7]]);
        assert_eq!(config.mask_image, Some(PathBuf::from("demo.mask")));
        assert!(config.use_pointing);
        assert_eq!(config.w_term_mode, WTermMode::Direct);
        assert_eq!(config.mosaic_pb_limit, 0.2);
        assert!(config.pbcor);
    }

    #[test]
    fn pbcor_products_apply_primary_beam_cutoff() {
        let weight = Array2::from_shape_vec((2, 2), vec![4.0, 1.0, 0.04, 0.0]).unwrap();
        let pb = mosaic_pb_product_from_weight(&weight);
        assert_eq!(pb[[0, 0, 0, 0]], 1.0);
        assert_eq!(pb[[0, 1, 0, 0]], 0.5);
        assert!((pb[[1, 0, 0, 0]] - 0.1).abs() < 1.0e-6);
        assert_eq!(pb[[1, 1, 0, 0]], 0.0);
        let limited_pb = pb_limited_product(&pb, 0.1);
        assert_eq!(limited_pb[[0, 0, 0, 0]], 1.0);
        assert_eq!(limited_pb[[0, 1, 0, 0]], 0.5);
        assert_eq!(limited_pb[[1, 0, 0, 0]], 0.0);
        assert_eq!(limited_pb[[1, 1, 0, 0]], 0.0);

        let image = Array4::from_shape_vec((2, 2, 1, 1), vec![2.0, 2.0, 2.0, 2.0]).unwrap();
        let corrected = pb_correct_image_product(&image, &pb, 0.1);
        assert_eq!(corrected[[0, 0, 0, 0]], 2.0);
        assert_eq!(corrected[[0, 1, 0, 0]], 4.0);
        assert_eq!(corrected[[1, 0, 0, 0]], 0.0);
        assert_eq!(corrected[[1, 1, 0, 0]], 0.0);

        let support = pb_support_mask_product(&pb, 0.1);
        assert!(support[[0, 0, 0, 0]]);
        assert!(support[[0, 1, 0, 0]]);
        assert!(!support[[1, 0, 0, 0]]);
        assert!(!support[[1, 1, 0, 0]]);
    }

    #[test]
    fn cube_pb_product_normalizes_each_channel_plane() {
        let mut weight = Array4::<f32>::zeros((2, 2, 1, 2));
        weight[[0, 0, 0, 0]] = 4.0;
        weight[[0, 1, 0, 0]] = 1.0;
        weight[[1, 0, 0, 0]] = 0.04;
        weight[[0, 0, 0, 1]] = 16.0;
        weight[[0, 1, 0, 1]] = 4.0;
        weight[[1, 0, 0, 1]] = 1.0;

        let pb = mosaic_pb_product_from_weight_product(&weight);

        assert_eq!(pb[[0, 0, 0, 0]], 1.0);
        assert_eq!(pb[[0, 1, 0, 0]], 0.5);
        assert!((pb[[1, 0, 0, 0]] - 0.1).abs() < 1.0e-6);
        assert_eq!(pb[[1, 1, 0, 0]], 0.0);
        assert_eq!(pb[[0, 0, 0, 1]], 1.0);
        assert_eq!(pb[[0, 1, 0, 1]], 0.5);
        assert_eq!(pb[[1, 0, 0, 1]], 0.25);
        assert_eq!(pb[[1, 1, 0, 1]], 0.0);

        let support = pb_support_mask_product(&pb, 0.1);
        assert!(support[[0, 0, 0, 0]]);
        assert!(support[[0, 1, 0, 0]]);
        assert!(!support[[1, 0, 0, 0]]);
        assert!(support[[0, 0, 0, 1]]);
        assert!(support[[0, 1, 0, 1]]);
        assert!(support[[1, 0, 0, 1]]);
    }

    #[test]
    fn frequency_range_hz_accepts_descending_spectral_windows() {
        assert_eq!(
            frequency_range_hz(&[344.8e9, 344.2e9, 343.9e9]).unwrap(),
            [343.9e9, 344.8e9]
        );
        assert!(frequency_range_hz(&[]).is_err());
        assert!(frequency_range_hz(&[1.0e9, f64::NAN]).is_err());
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

        let m100 = parse_phase_center_literal("J2000 12h22m54.9 +15d49m15").unwrap();
        assert!((m100.angles_rad[0] - 3.241_578_203_113_418).abs() < 1.0e-12);
        assert!((m100.angles_rad[1] - 0.276_125_632_075_936_2).abs() < 1.0e-12);
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
        assert_eq!(
            parse_weighting_mode("briggsbwtaper", 0.5).unwrap(),
            WeightingMode::BriggsBwTaper { robust: 0.5 }
        );
        assert!(parse_weighting_mode("invalid", 0.0).is_err());

        assert_eq!(parse_deconvolver("hogbom").unwrap(), Deconvolver::Hogbom);
        assert_eq!(parse_deconvolver("clark").unwrap(), Deconvolver::Clark);
        assert_eq!(
            parse_deconvolver("multiscale").unwrap(),
            Deconvolver::Multiscale
        );
        assert!(parse_deconvolver("other").is_err());
        assert_eq!(
            parse_hogbom_iteration_mode("strict").unwrap(),
            HogbomIterationMode::Strict
        );
        assert_eq!(
            parse_hogbom_iteration_mode("casa").unwrap(),
            HogbomIterationMode::CasaInclusive
        );
        assert!(parse_hogbom_iteration_mode("other").is_err());

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
        assert!(help_text().contains("--json-schema"));
        assert!(help_text().contains("--protocol-info"));
        assert!(help_text().contains("--json-run <SOURCE>"));
    }

    #[test]
    fn canonical_helpers_manifest_and_cli_option_parsers_cover_remaining_paths() {
        assert_eq!(canonical_spectral_mode_name(SpectralMode::Mfs), "mfs");
        assert_eq!(canonical_spectral_mode_name(SpectralMode::Cube), "cube");
        assert_eq!(
            canonical_spectral_mode_name(SpectralMode::Cubedata),
            "cubedata"
        );
        assert_eq!(
            canonical_weighting_name(WeightingMode::Briggs { robust: -0.5 }),
            "briggs:-0.5"
        );
        assert_eq!(
            canonical_weighting_name(WeightingMode::BriggsBwTaper { robust: -0.5 }),
            "briggsbwtaper:-0.5"
        );
        assert_eq!(
            canonical_restoring_beam_mode_name(RestoringBeamMode::Common),
            "common"
        );
        assert_eq!(canonical_deconvolver_name(Deconvolver::Clark), "clark");
        assert_eq!(canonical_w_term_mode_name(WTermMode::WProject), "wproject");
        assert_eq!(
            canonical_cube_interpolation_name(CubeInterpolation::Linear),
            "linear"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::Channel(7)),
            "channel:7"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::FrequencyHz {
                hz: 1.5e9,
                frame: Some(FrequencyRef::LSRK),
            }),
            "frequency_hz:1500000000@LSRK"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::VelocityMs {
                ms: 12.5,
                frame: None,
            }),
            "velocity_ms:12.5"
        );
        assert_eq!(
            canonical_cube_axis_value(&CubeAxisValue::Doppler {
                value: 0.125,
                convention: DopplerRef::RADIO,
            }),
            "doppler:0.125@RADIO"
        );
        assert_eq!(
            canonical_uv_taper(GaussianUvTaper {
                major: UvTaperSize::ImageFwhmRad(1.0),
                minor: UvTaperSize::BaselineHwhmLambda(2.0),
                position_angle_rad: 0.5,
            }),
            "major=image_fwhm_rad:1,minor=baseline_hwhm_lambda:2,pa_rad=0.5"
        );
        assert_eq!(optional_numeric_list(None), "none");
        assert_eq!(optional_numeric_list(Some(&[1, 3, 5])), "1,3,5");

        let config = CliConfig::parse([
            OsString::from("--ms"),
            OsString::from("fixture.ms"),
            OsString::from("--imagename"),
            OsString::from("fixture.image"),
            OsString::from("--imsize"),
            OsString::from("256"),
            OsString::from("--cell-arcsec"),
            OsString::from("0.5"),
            OsString::from("--field"),
            OsString::from("1,2"),
            OsString::from("--phasecenter"),
            OsString::from("J2000 1rad 2rad"),
            OsString::from("--ddid"),
            OsString::from("4"),
            OsString::from("--spw"),
            OsString::from("5"),
            OsString::from("--channel-start"),
            OsString::from("6"),
            OsString::from("--channel-count"),
            OsString::from("7"),
            OsString::from("--datacolumn"),
            OsString::from("data"),
            OsString::from("--corr"),
            OsString::from("stokes_i"),
            OsString::from("--specmode"),
            OsString::from("cubedata"),
            OsString::from("--start"),
            OsString::from("9"),
            OsString::from("--width"),
            OsString::from("10m/s"),
            OsString::from("--outframe"),
            OsString::from("BARY"),
            OsString::from("--weighting"),
            OsString::from("briggs"),
            OsString::from("--robust"),
            OsString::from("0.0"),
            OsString::from("--perchanweightdensity"),
            OsString::from("--uvtaper"),
            OsString::from("10arcsec,2lambda,30deg"),
            OsString::from("--restoringbeam"),
            OsString::from("common"),
            OsString::from("--deconvolver"),
            OsString::from("multiscale"),
            OsString::from("--scales"),
            OsString::from("0,5,15"),
            OsString::from("--mask-box"),
            OsString::from("1,2,3,4"),
            OsString::from("--mask-image"),
            OsString::from("mask.im"),
            OsString::from("--wterm"),
            OsString::from("direct"),
            OsString::from("--wprojplanes"),
            OsString::from("16"),
            OsString::from("--dirty-only"),
        ])
        .unwrap();
        let manifest = oracle_parameter_manifest(&config);
        assert_eq!(manifest.get("field_ids").unwrap(), "1,2");
        assert_eq!(manifest.get("spectral_mode").unwrap(), "cubedata");
        assert_eq!(manifest.get("cube_start").unwrap(), "channel:9");
        assert_eq!(manifest.get("cube_width").unwrap(), "velocity_ms:10");
        assert_eq!(manifest.get("weighting").unwrap(), "briggs:0");
        assert!(
            manifest
                .get("uv_taper")
                .unwrap()
                .contains("major=image_fwhm_rad:")
        );
        assert_eq!(manifest.get("mask_boxes").unwrap(), "1,2,3,4");
        assert_eq!(manifest.get("w_term_mode").unwrap(), "direct");

        let args = vec![
            OsString::from("--managed-output"),
            OsString::from("true"),
            OsString::from("--json-run"),
            OsString::from("request.json"),
            OsString::from("--keep"),
        ];
        let (managed_output, filtered) = extract_option_value(&args, "--managed-output").unwrap();
        assert!(managed_output);
        assert!(!filtered.iter().any(|arg| arg == "--managed-output"));
        let (json_run, filtered) = extract_string_option(&filtered, "--json-run").unwrap();
        assert_eq!(json_run.as_deref(), Some("request.json"));
        assert_eq!(filtered, vec![OsString::from("--keep")]);
        assert!(
            extract_option_value(&[OsString::from("--managed-output")], "--managed-output")
                .unwrap_err()
                .contains("requires a value")
        );
        assert!(
            extract_string_option(&[OsString::from("--json-run")], "--json-run")
                .unwrap_err()
                .contains("missing value")
        );
        assert!(
            run_with_cli_args([
                OsString::from("casars-imager"),
                OsString::from("--managed-output"),
                OsString::from("maybe"),
            ])
            .unwrap_err()
            .contains("--managed-output expects true or false")
        );
    }

    #[test]
    fn render_help_mentions_json_protocol_surface() {
        let help = render_help(&command_schema("casars-imager-test"));
        assert!(help.contains("--ui-schema"));
        assert!(help.contains("--json-schema"));
        assert!(help.contains("--protocol-info"));
        assert!(help.contains("--json-run <SOURCE>"));
    }

    #[test]
    fn run_with_cli_args_handles_meta_output_flags() {
        for args in [
            vec![
                OsString::from("casars-imager"),
                OsString::from("--ui-schema"),
            ],
            vec![
                OsString::from("casars-imager"),
                OsString::from("--json-schema"),
            ],
            vec![
                OsString::from("casars-imager"),
                OsString::from("--protocol-info"),
            ],
            vec![OsString::from("casars-imager"), OsString::from("--help")],
        ] {
            run_with_cli_args(args).unwrap();
        }
    }

    #[test]
    fn option_extractors_preserve_non_utf8_args_and_false_values() {
        let non_utf8 = OsString::from_vec(vec![0xff, b'a']);
        let args = vec![
            non_utf8.clone(),
            OsString::from("--managed-output"),
            OsString::from("false"),
            OsString::from("--json-run"),
            OsString::from("bundle.json"),
        ];

        let (managed_output, filtered) = extract_option_value(&args, "--managed-output").unwrap();
        assert!(!managed_output);
        assert_eq!(filtered.len(), 3);
        assert_eq!(
            filtered[0].as_os_str().as_bytes(),
            non_utf8.as_os_str().as_bytes()
        );

        let (json_run, filtered) = extract_string_option(&filtered, "--json-run").unwrap();
        assert_eq!(json_run.as_deref(), Some("bundle.json"));
        assert_eq!(filtered, vec![non_utf8]);
    }

    #[test]
    fn synthetic_trace_helpers_preserve_spectral_and_w_project_details() {
        let prepared_trace = PreparedVisibilityTraceBundle {
            schema_version: ORACLE_SCHEMA_VERSION,
            ms_path: "demo.ms".to_string(),
            data_column: "DATA".to_string(),
            spectral_mode: "cubedata".to_string(),
            phase_center: PhaseCenterTrace {
                field_id: Some(3),
                reference: "J2000".to_string(),
                angles_rad: [1.0, -0.5],
            },
            source_channel_indices: vec![4, 7],
            source_channel_frequencies_hz: vec![1.1e9, 1.2e9],
            source_channel_widths_hz: vec![1.5e6, 2.5e6],
            output_channel_frequencies_hz: vec![1.15e9, 1.25e9],
            selected_rows: vec![SelectedRowTrace {
                row_index: 12,
                field_id: 3,
                ddid: 5,
                spw_id: 7,
                polarization_id: 11,
                time_mjd_seconds: Some(1234.5),
            }],
            samples: Vec::new(),
            rejected_samples: Vec::new(),
        };
        let spectral_axis = build_prepare_spectral_axis_trace(&prepared_trace);
        assert_eq!(spectral_axis.spectral_mode, "cubedata");
        assert_eq!(
            spectral_axis.source_channels,
            vec![
                PreparedSourceChannelTrace {
                    source_channel_slot: 0,
                    source_channel_index: 4,
                    frequency_hz: 1.1e9,
                    width_hz: 1.5e6,
                },
                PreparedSourceChannelTrace {
                    source_channel_slot: 1,
                    source_channel_index: 7,
                    frequency_hz: 1.2e9,
                    width_hz: 2.5e6,
                }
            ]
        );
        assert_eq!(
            spectral_axis.output_channels,
            vec![
                PreparedOutputChannelTrace {
                    output_channel_index: 0,
                    frequency_hz: 1.15e9,
                },
                PreparedOutputChannelTrace {
                    output_channel_index: 1,
                    frequency_hz: 1.25e9,
                }
            ]
        );

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
        let diagnostics = WProjectDiagnostics {
            requested_plane_count: Some(8),
            plane_count: 3,
            sampling: 4,
            w_scale: 1.5,
            max_abs_w_lambda: 22.0,
            kernels: vec![
                casa_imaging::WProjectKernelDiagnostics {
                    plane_index: 0,
                    w_lambda: 0.0,
                    support: 5,
                    kernel_integral: 1.0,
                },
                casa_imaging::WProjectKernelDiagnostics {
                    plane_index: 1,
                    w_lambda: 11.0,
                    support: 7,
                    kernel_integral: 0.75,
                },
            ],
            samples: vec![casa_imaging::WProjectSamplePlanDiagnostics {
                batch_index: 2,
                sample_index: 9,
                u_lambda: 3.0,
                v_lambda: -4.0,
                w_lambda: 5.0,
                weight: 6.5,
                sumwt_factor: 2.0,
                plane_index: 1,
                loc_x: 12,
                loc_y: -8,
                off_x: 3,
                off_y: -2,
                conjugate_kernel: true,
                normalization: 0.5,
                support: 7,
            }],
            skipped_samples: vec![
                casa_imaging::WProjectSkippedSampleDiagnostics {
                    batch_index: 0,
                    sample_index: 1,
                    u_lambda: 1.0,
                    v_lambda: 2.0,
                    w_lambda: 3.0,
                    weight: 4.0,
                    sumwt_factor: 5.0,
                    reason: WProjectSkipReason::NotGridable,
                },
                casa_imaging::WProjectSkippedSampleDiagnostics {
                    batch_index: 1,
                    sample_index: 2,
                    u_lambda: 6.0,
                    v_lambda: 7.0,
                    w_lambda: 8.0,
                    weight: 9.0,
                    sumwt_factor: 10.0,
                    reason: WProjectSkipReason::InvalidInput,
                },
                casa_imaging::WProjectSkippedSampleDiagnostics {
                    batch_index: 3,
                    sample_index: 4,
                    u_lambda: 11.0,
                    v_lambda: 12.0,
                    w_lambda: 13.0,
                    weight: 14.0,
                    sumwt_factor: 15.0,
                    reason: WProjectSkipReason::OutsideGrid,
                },
            ],
            normalization_sumwt: 17.0,
            reported_sumwt: 19.0,
            gridded_samples: 23,
        };
        let w_project_trace =
            build_w_project_trace_bundle(&config, diagnostics, Some(6), Some(1.42e9));
        assert_eq!(w_project_trace.ms_path, "demo.ms");
        assert_eq!(w_project_trace.spectral_mode, "cubedata");
        assert_eq!(w_project_trace.channel_index, Some(6));
        assert_eq!(w_project_trace.channel_frequency_hz, Some(1.42e9));
        assert_eq!(w_project_trace.kernels.len(), 2);
        assert_eq!(w_project_trace.samples.len(), 1);
        assert_eq!(
            w_project_trace
                .skipped_samples
                .iter()
                .map(|sample| sample.reason)
                .collect::<Vec<_>>(),
            vec![
                WProjectSkipReasonTrace::NotGridable,
                WProjectSkipReasonTrace::InvalidInput,
                WProjectSkipReasonTrace::OutsideGrid,
            ]
        );
        assert_eq!(w_project_trace.normalization_sumwt, 17.0);
        assert_eq!(w_project_trace.reported_sumwt, 19.0);
        assert_eq!(w_project_trace.gridded_samples, 23);
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
    fn start_model_image_loads_single_plane_seed() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("seed.model");
        let coords = CoordinateSystem::default();
        let mut image = PagedImage::<f32>::create(vec![4, 4, 1, 1], coords, &path).unwrap();
        let mut data = Array4::<f32>::zeros((4, 4, 1, 1));
        data[(1, 2, 0, 0)] = 0.25;
        image.put_slice(&data.into_dyn(), &[0, 0, 0, 0]).unwrap();
        image.save().unwrap();

        let mut config =
            minimal_start_model_config(tmp.path().join("tiny.ms"), tmp.path().join("out"));
        config.imsize = 4;
        config.start_model = Some(path);
        let model = load_start_model_image(
            &config,
            ImageGeometry {
                image_shape: [4, 4],
                cell_size_rad: [arcsec_to_rad(), arcsec_to_rad()],
            },
            &GridderMode::Standard,
        )
        .unwrap()
        .expect("loaded startmodel");
        assert_eq!(model[(1, 2)], 0.25);
        assert_eq!(model[(0, 0)], 0.0);
    }

    #[test]
    fn start_model_image_rejects_shape_mismatch() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("seed.model");
        let coords = CoordinateSystem::default();
        let mut image = PagedImage::<f32>::create(vec![2, 4, 1, 1], coords, &path).unwrap();
        image.save().unwrap();

        let mut config =
            minimal_start_model_config(tmp.path().join("tiny.ms"), tmp.path().join("out"));
        config.imsize = 4;
        config.start_model = Some(path);
        let error = load_start_model_image(
            &config,
            ImageGeometry {
                image_shape: [4, 4],
                cell_size_rad: [arcsec_to_rad(), arcsec_to_rad()],
            },
            &GridderMode::Standard,
        )
        .unwrap_err();
        assert!(error.contains("expected [4, 4]"));
    }

    #[test]
    fn outlier_file_parser_inventories_casa_new_format_fields() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("outliers.txt");
        fs::write(
            &path,
            r#"
# CASA outlier-field definitions.
imagename=tst1
imsize=[80,80]
cell=[8.0arcsec,8.0arcsec]
phasecenter=J2000 19:58:40.895 +40.55.58.543
mask=circle[[40pix,40pix],10pix]
unknown=ignored

imagename=tst2
nchan=4
nterms=2
wprojplanes=16
gridder=wproject
deconvolver=mtmfs
"#,
        )
        .unwrap();

        let definitions = parse_outlier_file(&path).unwrap();
        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].image_name.as_deref(), Some("tst1"));
        assert_eq!(definitions[0].imsize.as_deref(), Some([80, 80].as_slice()));
        assert_eq!(
            definitions[0].cell.as_deref(),
            Some(["8.0arcsec".to_string(), "8.0arcsec".to_string()].as_slice())
        );
        assert_eq!(
            definitions[0].phasecenter.as_deref(),
            Some("J2000 19:58:40.895 +40.55.58.543")
        );
        assert_eq!(definitions[0].ignored_fields, vec!["unknown"]);
        assert_eq!(definitions[1].image_name.as_deref(), Some("tst2"));
        assert_eq!(definitions[1].nchan, Some(4));
        assert_eq!(definitions[1].nterms, Some(2));
        assert_eq!(definitions[1].wprojplanes, Some(16));
        assert_eq!(definitions[1].gridder.as_deref(), Some("wproject"));
        assert_eq!(definitions[1].deconvolver.as_deref(), Some("mtmfs"));
    }

    #[test]
    fn outlier_file_request_rejects_multi_image_boundary_with_inventory() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("outliers.txt");
        fs::write(
            &path,
            "imagename=tst1\nimsize=[80,80]\nphasecenter=J2000 0deg 0deg\n",
        )
        .unwrap();

        let mut config =
            minimal_start_model_config(tmp.path().join("tiny.ms"), tmp.path().join("out"));
        config.outlier_file = Some(path);
        let error = validate_outlier_file_request(&config).unwrap_err();
        assert!(error.contains("defines 1 outlier image"));
        assert!(error.contains("single-image frontend boundary"));
        assert!(error.contains("imagename,imsize,cell,phasecenter,startmodel"));
    }

    fn minimal_start_model_config(ms: PathBuf, imagename: PathBuf) -> CliConfig {
        CliConfig {
            ms,
            imagename,
            imsize: 4,
            cell_arcsec: 1.0,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.2,
            pbcor: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::Strict,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        }
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
    fn selected_weight_spectrum_missing_cells_fall_back_to_weight() {
        let column = SelectedMainArrayColumn {
            column_name: "WEIGHT_SPECTRUM",
            values: vec![None],
        };
        assert_eq!(column.get_optional(0).unwrap(), None);
        assert!(
            column
                .get_optional(1)
                .unwrap_err()
                .contains("out of bounds")
        );

        let weight_row =
            ArrayValue::Float32(ArrayD::from_shape_vec(vec![2], vec![1.0f32, 2.0]).unwrap());
        let (weight, source) = resolve_weight_with_source(&weight_row, None, 1, 0).unwrap();
        assert_eq!(weight, 2.0);
        assert_eq!(source, WeightSourceKind::Weight);
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
            0.0,
            1.75,
            &contributions,
            false,
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
            0.0,
            1.75,
            &contributions,
            false,
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
            0.0,
            1.5,
            &contributions,
            false,
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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

        let engine = MsCalEngine::new(&ms).unwrap();
        let frequency_hz = convert_frequency_to_frame(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            1.4e9,
            TEST_TIME_MJD_SEC,
            0,
            &engine,
        )
        .unwrap();
        let lambda_scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
        assert!((samples[0].0 - 30.0 * lambda_scale).abs() < 1.0e-9);
        assert!((samples[0].1 - -15.0 * lambda_scale).abs() < 1.0e-9);
        assert!((samples[0].2 - 5.0 * lambda_scale).abs() < 1.0e-9);
        assert!((samples[0].3 - Complex32::new(1.0, 0.5)).norm() < 1.0e-6);

        let (target_uvw_m, phase_shift_m) = engine
            .reproject_raw_uvw_for_mosaic_between_fields([-25.0, 20.0, -7.5], 1, 0)
            .unwrap();
        let second_frequency_hz = convert_frequency_to_frame(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            1.4e9,
            TEST_TIME_MJD_SEC,
            1,
            &engine,
        )
        .unwrap();
        let second_lambda_scale = second_frequency_hz / SPEED_OF_LIGHT_M_PER_S;
        let expected_visibility = phase_rotate_visibility(
            Complex32::new(0.25, 1.25),
            phase_shift_m,
            second_frequency_hz,
        );
        assert!((samples[1].0 - target_uvw_m[0] * second_lambda_scale).abs() < 1.0e-9);
        assert!((samples[1].1 - target_uvw_m[1] * second_lambda_scale).abs() < 1.0e-9);
        assert!((samples[1].2 - target_uvw_m[2] * second_lambda_scale).abs() < 1.0e-9);
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            .reproject_raw_uvw_for_mosaic_between_fields([-25.0, 20.0, -7.5], 1, 0)
            .unwrap();
        let frequency_hz = convert_frequency_to_frame(
            FrequencyRef::TOPO,
            FrequencyRef::LSRK,
            1.4e9,
            TEST_TIME_MJD_SEC,
            1,
            &engine,
        )
        .unwrap();
        let expected_visibility =
            phase_rotate_visibility(Complex32::new(0.25, 1.25), phase_shift_m, frequency_hz);
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Uniform,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            .reproject_raw_uvw_for_mosaic_between_fields([-25.0, 20.0, -7.5], 1, 0)
            .unwrap();
        assert!((trace.rows[1].raw_uvw_m[0] + 25.0).abs() < 1.0e-9);
        assert!((trace.rows[1].raw_uvw_m[2] + 7.5).abs() < 1.0e-9);
        assert!((trace.rows[1].imaging_uvw_m[0] - target_uvw_m[0]).abs() < 1.0e-9);
        assert!((trace.rows[1].imaging_uvw_m[1] - target_uvw_m[1]).abs() < 1.0e-9);
        assert!((trace.rows[1].imaging_uvw_m[2] - target_uvw_m[2]).abs() < 1.0e-9);
        assert!((trace.rows[1].phase_shift_m - phase_shift_m).abs() < 1.0e-9);
    }

    #[test]
    fn prepare_geometry_trace_uses_pointing_rows_when_time_window_matches() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("geometry_pointing.ms");
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
            [30.0, -15.0, 5.0],
            &[Complex32::new(1.0, 0.5), Complex32::new(0.0, 0.0)],
        );
        add_pointing_row(&mut ms, 0, [1.2, 0.4], TEST_TIME_MJD_SEC, 5.0);
        add_pointing_row(&mut ms, 1, [1.3, 0.45], TEST_TIME_MJD_SEC, 5.0);
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
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: true,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_geometry_trace_from_config(&config).unwrap();

        assert_eq!(trace.rows.len(), 1);
        assert_eq!(trace.rows[0].antenna1_pointing_row, Some(0));
        assert_eq!(trace.rows[0].antenna2_pointing_row, Some(1));
        assert!(!trace.rows[0].antenna1_pointing_used_fallback);
        assert!(!trace.rows[0].antenna2_pointing_used_fallback);
        assert_eq!(trace.rows[0].antenna1_pointing_direction_rad, [1.2, 0.4]);
        assert_eq!(trace.rows[0].antenna2_pointing_direction_rad, [1.3, 0.45]);
    }

    #[test]
    fn prepare_geometry_trace_ignores_pointing_rows_by_default() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("geometry_pointing_default.ms");
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
            [30.0, -15.0, 5.0],
            &[Complex32::new(1.0, 0.5), Complex32::new(0.0, 0.0)],
        );
        add_pointing_row(&mut ms, 0, [1.2, 0.4], TEST_TIME_MJD_SEC, 5.0);
        add_pointing_row(&mut ms, 1, [1.3, 0.45], TEST_TIME_MJD_SEC, 5.0);
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
            spw: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            datacolumn: None,
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        };

        let trace = build_prepare_geometry_trace_from_config(&config).unwrap();

        assert_eq!(trace.rows.len(), 1);
        assert_eq!(trace.rows[0].pointing_id, None);
        assert_eq!(trace.rows[0].antenna1_pointing_row, None);
        assert_eq!(trace.rows[0].antenna2_pointing_row, None);
        assert!(trace.rows[0].antenna1_pointing_used_fallback);
        assert!(trace.rows[0].antenna2_pointing_used_fallback);
        assert_eq!(trace.rows[0].field_phase_center_direction_rad, [1.0, 0.5]);
        assert_eq!(trace.rows[0].antenna1_pointing_direction_rad, [1.0, 0.5]);
        assert_eq!(trace.rows[0].antenna2_pointing_direction_rad, [1.0, 0.5]);
    }

    #[test]
    fn pointing_direction_resolver_prefers_row_ids_and_falls_back_when_needed() {
        let first = PointingDirectionRow {
            row_index: 0,
            antenna_id: 0,
            time_mjd_seconds: TEST_TIME_MJD_SEC,
            interval_seconds: 5.0,
            angles_rad: [1.2, 0.4],
        };
        let second = PointingDirectionRow {
            row_index: 1,
            antenna_id: 0,
            time_mjd_seconds: TEST_TIME_MJD_SEC + 30.0,
            interval_seconds: 5.0,
            angles_rad: [1.25, 0.45],
        };
        let other_antenna = PointingDirectionRow {
            row_index: 2,
            antenna_id: 1,
            time_mjd_seconds: TEST_TIME_MJD_SEC,
            interval_seconds: 5.0,
            angles_rad: [1.3, 0.5],
        };
        let resolver = PointingDirectionResolver {
            by_antenna: BTreeMap::from([(0, vec![first, second]), (1, vec![other_antenna])]),
            by_row_index: HashMap::from([(0, first), (1, second), (2, other_antenna)]),
        };

        let fallback_angles = [0.9, 0.1];
        let explicit = resolver.resolve(Some(0), 0, TEST_TIME_MJD_SEC + 100.0, fallback_angles);
        assert_eq!(explicit.source_row_index, Some(0));
        assert!(!explicit.used_fallback);
        assert_eq!(explicit.angles_rad, [1.2, 0.4]);

        let nearest = resolver.resolve(None, 0, TEST_TIME_MJD_SEC + 31.0, fallback_angles);
        assert_eq!(nearest.source_row_index, Some(1));
        assert!(!nearest.used_fallback);
        assert_eq!(nearest.angles_rad, [1.25, 0.45]);

        let no_matching_window =
            resolver.resolve(Some(2), 0, TEST_TIME_MJD_SEC + 500.0, fallback_angles);
        assert_eq!(no_matching_window.source_row_index, None);
        assert!(no_matching_window.used_fallback);
        assert_eq!(no_matching_window.angles_rad, fallback_angles);

        let missing_antenna = resolver.resolve(None, 9, TEST_TIME_MJD_SEC, fallback_angles);
        assert_eq!(missing_antenna.source_row_index, None);
        assert!(missing_antenna.used_fallback);
        assert_eq!(missing_antenna.angles_rad, fallback_angles);
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            .cell_accessor_mut(0, "FLAG")
            .unwrap()
            .set(Value::Array(ArrayValue::Bool(
                ArrayD::from_shape_vec(vec![2, 1], vec![true, false]).unwrap(),
            )))
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("Q".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("U".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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

    fn synthetic_cube_trace_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: true,
            write_preview_pngs: false,
        }
    }

    fn write_synthetic_cube_trace_ms(ms_path: &Path) {
        let mut ms = MeasurementSet::create(
            ms_path,
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
    }

    #[test]
    fn cube_channel_w_project_trace_wrapper_emits_cube_channel_metadata() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_cube_wproject.ms");
        write_synthetic_cube_trace_ms(&ms_path);

        let mut config = synthetic_cube_trace_config(ms_path);
        config.w_term_mode = WTermMode::WProject;
        config.w_project_planes = Some(4);

        let trace = build_cube_channel_w_project_trace_from_config(&config, 0).unwrap();

        assert_eq!(trace.schema_version, ORACLE_SCHEMA_VERSION);
        assert_eq!(trace.spectral_mode, "cube");
        assert_eq!(trace.channel_index, Some(0));
        assert_eq!(trace.requested_plane_count, Some(4));
        assert!(trace.channel_frequency_hz.is_some());
        assert!(trace.plane_count >= 1);
        assert_eq!(trace.gridded_samples, 1);
        assert_eq!(trace.samples.len(), 1);
        assert!(trace.skipped_samples.is_empty());
    }

    #[test]
    fn cube_residual_refresh_wrappers_trace_single_plane_models() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_cube_residual.ms");
        write_synthetic_cube_trace_ms(&ms_path);

        let config = synthetic_cube_trace_config(ms_path);
        let mut model = Array2::<f32>::zeros((config.imsize, config.imsize));
        model[(config.imsize / 2, config.imsize / 2)] = 2.0;

        let single_plane =
            trace_cube_channel_residual_refresh_from_config(&config, 0, &model).unwrap();
        let model_cube = vec![model.clone()];
        let explicit_cube = trace_cube_channel_residual_refresh_from_config_with_model_cube(
            &config,
            0,
            &model_cube,
        )
        .unwrap();
        let model_lambda =
            trace_cube_channel_residual_refresh_from_config_with_model_cube_model_channel_lambda(
                &config,
                0,
                &model_cube,
            )
            .unwrap();

        for trace in [&single_plane, &explicit_cube, &model_lambda] {
            assert_eq!(trace.samples.len(), 1);
            assert_eq!(trace.gridded_samples, 1);
            assert_eq!(trace.skipped_samples, 0);
            assert_eq!(trace.residual_image.dim(), (config.imsize, config.imsize));
            assert!(trace.psf_peak.is_finite());
            assert!(trace.normalization_sumwt.is_finite());
            assert!(trace.reported_sumwt.is_finite());
            assert!(trace.samples[0].gridable);
        }

        assert_eq!(single_plane.samples, explicit_cube.samples);
        assert_eq!(single_plane.residual_image, explicit_cube.residual_image);
        assert_eq!(single_plane.samples, model_lambda.samples);
        assert_eq!(single_plane.residual_image, model_lambda.residual_image);
    }

    #[test]
    fn cube_trace_wrappers_reject_non_cube_requests_and_invalid_models() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("trace_cube_wrapper_errors.ms");
        write_synthetic_cube_trace_ms(&ms_path);

        let cube_config = synthetic_cube_trace_config(ms_path.clone());
        let invalid_shape = Array2::<f32>::zeros((8, 8));
        assert!(
            trace_cube_channel_residual_refresh_from_config(&cube_config, 0, &invalid_shape)
                .unwrap_err()
                .contains("model shape")
        );
        assert!(build_cube_channel_w_project_trace_from_config(&cube_config, 99).is_err());
        assert!(
            trace_cube_channel_residual_refresh_from_config_with_model_cube(&cube_config, 0, &[],)
                .unwrap_err()
                .contains("model plane count 0")
        );

        let mut mfs_config = synthetic_cube_trace_config(ms_path);
        mfs_config.spectral_mode = SpectralMode::Mfs;
        mfs_config.channel_count = None;
        mfs_config.channel_start = None;
        mfs_config.w_term_mode = WTermMode::None;
        mfs_config.w_project_planes = None;
        let model = Array2::<f32>::zeros((mfs_config.imsize, mfs_config.imsize));
        assert!(
            build_cube_channel_w_project_trace_from_config(&mfs_config, 0)
                .unwrap_err()
                .contains("requires cube input")
        );
        assert!(
            trace_cube_channel_residual_refresh_from_config(&mfs_config, 0, &model)
                .unwrap_err()
                .contains("requires cube input")
        );
        assert!(
            trace_cube_channel_residual_refresh_from_config_with_model_cube(
                &mfs_config,
                0,
                &[model],
            )
            .unwrap_err()
            .contains("requires cube input")
        );
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::ModelColumn,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert!(
            reopened
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("MODEL_DATA"))
        );
        let model_data = reopened
            .main_table()
            .cell_accessor(0, "MODEL_DATA")
            .and_then(|cell| cell.array())
            .unwrap();
        let ArrayValue::Complex32(model_data) = model_data else {
            panic!("MODEL_DATA should be complex");
        };
        assert!(
            model_data.iter().any(|value| value.norm() > 0.0),
            "savemodel=modelcolumn should write non-zero predicted visibilities"
        );
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
    fn cube_savemodel_modelcolumn_writes_channelized_model_data() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("tiny_cube_savemodel.ms");
        let image_prefix = tmp.path().join("tiny_cube_savemodel_image");
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
            save_model: SaveModelMode::ModelColumn,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Cube,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: false,
        })
        .unwrap();
        assert!(summary.minor_iterations > 0);

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert!(
            reopened
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("MODEL_DATA"))
        );
        let mut non_zero_model_values = 0usize;
        for row_index in 0..reopened.row_count() {
            let model_data = reopened
                .main_table()
                .cell_accessor(row_index, "MODEL_DATA")
                .and_then(|cell| cell.array())
                .unwrap();
            let ArrayValue::Complex32(model_data) = model_data else {
                panic!("MODEL_DATA should be complex");
            };
            non_zero_model_values += model_data.iter().filter(|value| value.norm() > 0.0).count();
        }
        assert!(
            non_zero_model_values >= 2,
            "cube savemodel=modelcolumn should write channelized predicted visibilities"
        );
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
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
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("XX".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: Some("Q".to_string()),
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
            save_model: SaveModelMode::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
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
    fn savemodel_modelcolumn_leaves_autocorrelations_zero() {
        let tmp = tempdir().unwrap();
        let ms_path = tmp.path().join("auto_savemodel.ms");
        let image_prefix = tmp.path().join("auto_savemodel_image");
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
        add_main_row_with_antennas(
            &mut ms,
            1,
            2,
            [-12.0, 18.0, 0.0],
            [Complex32::new(0.8, 0.0), Complex32::new(0.8, 0.0)],
        );
        ms.save().unwrap();

        let summary = run_from_config(&CliConfig {
            ms: ms_path.clone(),
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
            save_model: SaveModelMode::ModelColumn,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: SpectralMode::Mfs,
            cube_axis: CubeAxisConfig::default(),
            weighting: WeightingMode::Natural,
            per_channel_weight_density: false,
            use_pointing: false,
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
            mosaic_pb_limit: 0.1,
            pbcor: false,
            minor_cycle_length: 2,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::CasaInclusive,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: false,
        })
        .unwrap();
        assert!(summary.gridded_samples > 0);

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        let model_row_norm = |row_index| {
            let model_data = reopened
                .main_table()
                .cell_accessor(row_index, "MODEL_DATA")
                .and_then(|cell| cell.array())
                .unwrap();
            let ArrayValue::Complex32(model_data) = model_data else {
                panic!("MODEL_DATA should be complex");
            };
            model_data.iter().map(|value| value.norm()).sum::<f32>()
        };
        assert_eq!(
            model_row_norm(0),
            0.0,
            "CASA leaves autocorrelation MODEL_DATA rows zero"
        );
        assert!(
            model_row_norm(1) > 0.0 || model_row_norm(2) > 0.0,
            "cross-correlation rows should still receive predicted model visibilities"
        );
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
        let field_column = ms
            .main_table()
            .column_accessor("FIELD_ID")
            .unwrap()
            .iter()
            .unwrap();
        let ddid_column = ms
            .main_table()
            .column_accessor("DATA_DESC_ID")
            .unwrap()
            .iter()
            .unwrap();
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
                spectral_frame_field_id: 0,
                phase_center_direction: None,
                reference_row_time_mjd_sec: reference_time.unwrap(),
                time_bounds_mjd_sec: bounds.unwrap(),
                derived_engine: &engine,
            }),
            true,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
                hogbom_iteration_mode: config.hogbom_iteration_mode,
            },
            clean_mask: None,
            channel_clean_mask: None,
            auto_mask: None,
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
        let field_column = ms
            .main_table()
            .column_accessor("FIELD_ID")
            .unwrap()
            .iter()
            .unwrap();
        let ddid_column = ms
            .main_table()
            .column_accessor("DATA_DESC_ID")
            .unwrap()
            .iter()
            .unwrap();
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
                spectral_frame_field_id: 0,
                phase_center_direction: None,
                reference_row_time_mjd_sec: reference_time.unwrap(),
                time_bounds_mjd_sec: bounds.unwrap(),
                derived_engine: &engine,
            }),
            true,
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
        let field_column = ms
            .main_table()
            .column_accessor("FIELD_ID")
            .unwrap()
            .iter()
            .unwrap();
        let ddid_column = ms
            .main_table()
            .column_accessor("DATA_DESC_ID")
            .unwrap()
            .iter()
            .unwrap();
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
                spectral_frame_field_id: 0,
                phase_center_direction: None,
                reference_row_time_mjd_sec: reference_time.unwrap(),
                time_bounds_mjd_sec: bounds.unwrap(),
                derived_engine: &engine,
            }),
            true,
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

    fn add_pointing_row(
        ms: &mut MeasurementSet,
        antenna_id: i32,
        direction_rad: [f64; 2],
        time_mjd_sec: f64,
        interval_seconds: f64,
    ) {
        let table = ms.subtable_mut(SubtableId::Pointing).unwrap();
        let direction = ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![2, 1], direction_rad.to_vec()).unwrap(),
        );
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna_id))),
                RecordField::new("DIRECTION", Value::Array(direction.clone())),
                RecordField::new(
                    "INTERVAL",
                    Value::Scalar(ScalarValue::Float64(interval_seconds)),
                ),
                RecordField::new(
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("pointing-{antenna_id}"))),
                ),
                RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TARGET", Value::Array(direction)),
                RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time_mjd_sec))),
                RecordField::new(
                    "TIME_ORIGIN",
                    Value::Scalar(ScalarValue::Float64(time_mjd_sec)),
                ),
                RecordField::new("TRACKING", Value::Scalar(ScalarValue::Bool(true))),
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
