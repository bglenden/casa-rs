// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical imager task request/result contracts shared by CLI, shell, and Python.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_imaging::{
    CleanStopReason, Deconvolver, GaussianUvTaper, RestoringBeamMode, UvTaperSize, WTermMode,
    WeightingMode,
};
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeInterpolation,
    parse_rest_frequency_hz as parse_ms_rest_frequency_hz,
};
use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::FrequencyRef;
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    ChannelRunSummary, CliConfig, FrontendStageTimings, RunSummary, SpectralMode, command_schema,
    run_from_config,
};

/// Stable protocol name advertised by `casars-imager --protocol-info`.
pub const IMAGER_TASK_PROTOCOL_NAME: &str = "casa_imager_task";
/// Stable protocol version advertised by `casars-imager --protocol-info`.
pub const IMAGER_TASK_PROTOCOL_VERSION: u32 = 1;

/// Version/compatibility information for the JSON task protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl ImagerProtocolInfo {
    /// Build the current imager protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: IMAGER_TASK_PROTOCOL_NAME.to_string(),
            protocol_version: IMAGER_TASK_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Task,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// JSON-schema bundle for the public imager task protocol.
#[derive(Debug, Clone, Serialize)]
pub struct ImagerTaskSchemaBundle {
    /// Compatibility descriptor for the request/result schemas.
    pub protocol: ImagerProtocolInfo,
    /// Canonical semantic task contract.
    pub semantic: TaskSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI and CLI consumers.
    pub projections: ProviderProjectionMetadata,
    /// JSON schema for [`ImagerTaskRequest`].
    pub request_schema: RootSchema,
    /// JSON schema for [`ImagerTaskResult`].
    pub result_schema: RootSchema,
}

impl ImagerTaskSchemaBundle {
    /// Build the current request/result schema bundle.
    pub fn current() -> Self {
        let request_schema = schema_for!(ImagerTaskRequest);
        let result_schema = schema_for!(ImagerTaskResult);
        let ui_schema = serde_json::to_value(command_schema("casars-imager"))
            .expect("serialize imager ui schema projection");
        Self {
            protocol: ImagerProtocolInfo::current(),
            semantic: TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: vec![TaskOperationDescriptor {
                    name: "run".to_string(),
                    request_kind: "run".to_string(),
                    result_kind: Some("run".to_string()),
                }],
            },
            components: merged_components([&request_schema, &result_schema]),
            annotations: derived_ui_schema_annotations(),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        ui_schema: Some("--ui-schema".to_string()),
                        json_schema: Some("--json-schema".to_string()),
                        protocol_info: Some("--protocol-info".to_string()),
                        json_run: Some("--json-run <SOURCE>".to_string()),
                        session: None,
                    },
                }),
                ui_schema: Some(ui_schema),
                python: None,
            },
            request_schema,
            result_schema,
        }
    }

    /// Return the launcher/TUI compatibility view projected from the bundle.
    pub fn ui_schema_projection(&self) -> Result<casa_ms::ui_schema::UiCommandSchema, String> {
        let value = self
            .projections
            .ui_schema
            .clone()
            .ok_or_else(|| "missing ui_schema projection".to_string())?;
        serde_json::from_value(value).map_err(|error| format!("parse imager ui schema: {error}"))
    }
}

/// Supported scalar imaging planes and explicit raw correlations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ImagerPlaneSelection {
    /// Stokes I.
    #[serde(rename = "I")]
    StokesI,
    /// Stokes Q.
    #[serde(rename = "Q")]
    StokesQ,
    /// Stokes U.
    #[serde(rename = "U")]
    StokesU,
    /// Stokes V.
    #[serde(rename = "V")]
    StokesV,
    /// Raw XX correlation.
    #[serde(rename = "XX")]
    CorrXX,
    /// Raw YY correlation.
    #[serde(rename = "YY")]
    CorrYY,
    /// Raw RR correlation.
    #[serde(rename = "RR")]
    CorrRR,
    /// Raw LL correlation.
    #[serde(rename = "LL")]
    CorrLL,
}

impl ImagerPlaneSelection {
    /// Return the CLI-compatible string form.
    pub fn as_cli_text(self) -> &'static str {
        match self {
            Self::StokesI => "I",
            Self::StokesQ => "Q",
            Self::StokesU => "U",
            Self::StokesV => "V",
            Self::CorrXX => "XX",
            Self::CorrYY => "YY",
            Self::CorrRR => "RR",
            Self::CorrLL => "LL",
        }
    }

    fn from_cli_text(text: &str) -> Result<Self, String> {
        match text {
            "I" => Ok(Self::StokesI),
            "Q" => Ok(Self::StokesQ),
            "U" => Ok(Self::StokesU),
            "V" => Ok(Self::StokesV),
            "XX" => Ok(Self::CorrXX),
            "YY" => Ok(Self::CorrYY),
            "RR" => Ok(Self::CorrRR),
            "LL" => Ok(Self::CorrLL),
            other => Err(format!("unsupported scalar plane value {other:?}")),
        }
    }
}

/// Spectral imaging mode for the task protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerSpectralMode {
    /// Collapse selected channels into one MFS plane.
    #[default]
    Mfs,
    /// Produce a spectral cube in the requested frame.
    Cube,
    /// Produce a spectral cube in the native data frame.
    Cubedata,
}

impl From<SpectralMode> for ImagerSpectralMode {
    fn from(value: SpectralMode) -> Self {
        match value {
            SpectralMode::Mfs => Self::Mfs,
            SpectralMode::Cube => Self::Cube,
            SpectralMode::Cubedata => Self::Cubedata,
        }
    }
}

impl From<ImagerSpectralMode> for SpectralMode {
    fn from(value: ImagerSpectralMode) -> Self {
        match value {
            ImagerSpectralMode::Mfs => Self::Mfs,
            ImagerSpectralMode::Cube => Self::Cube,
            ImagerSpectralMode::Cubedata => Self::Cubedata,
        }
    }
}

/// Weighting policy for imaging runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImagerWeighting {
    /// Natural weighting.
    #[default]
    Natural,
    /// Uniform weighting.
    Uniform,
    /// Briggs robust weighting.
    Briggs {
        /// CASA-style robust parameter in `[-2, 2]`.
        robust: f32,
    },
}

impl From<WeightingMode> for ImagerWeighting {
    fn from(value: WeightingMode) -> Self {
        match value {
            WeightingMode::Natural => Self::Natural,
            WeightingMode::Uniform => Self::Uniform,
            WeightingMode::Briggs { robust } => Self::Briggs { robust },
        }
    }
}

impl From<ImagerWeighting> for WeightingMode {
    fn from(value: ImagerWeighting) -> Self {
        match value {
            ImagerWeighting::Natural => Self::Natural,
            ImagerWeighting::Uniform => Self::Uniform,
            ImagerWeighting::Briggs { robust } => Self::Briggs { robust },
        }
    }
}

/// Restoring-beam policy for restored image products.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerRestoringBeamMode {
    /// Fit one beam per plane.
    #[default]
    PerPlane,
    /// Fit one common beam for the full cube.
    Common,
}

impl From<RestoringBeamMode> for ImagerRestoringBeamMode {
    fn from(value: RestoringBeamMode) -> Self {
        match value {
            RestoringBeamMode::PerPlane => Self::PerPlane,
            RestoringBeamMode::Common => Self::Common,
        }
    }
}

impl From<ImagerRestoringBeamMode> for RestoringBeamMode {
    fn from(value: ImagerRestoringBeamMode) -> Self {
        match value {
            ImagerRestoringBeamMode::PerPlane => Self::PerPlane,
            ImagerRestoringBeamMode::Common => Self::Common,
        }
    }
}

/// Minor-cycle deconvolver selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerDeconvolver {
    /// Hogbom clean.
    #[default]
    Hogbom,
    /// Multi-term multi-frequency synthesis.
    Mtmfs,
    /// Clark clean.
    Clark,
    /// Multiscale clean.
    Multiscale,
}

impl From<Deconvolver> for ImagerDeconvolver {
    fn from(value: Deconvolver) -> Self {
        match value {
            Deconvolver::Hogbom => Self::Hogbom,
            Deconvolver::Mtmfs => Self::Mtmfs,
            Deconvolver::Clark => Self::Clark,
            Deconvolver::Multiscale => Self::Multiscale,
        }
    }
}

impl From<ImagerDeconvolver> for Deconvolver {
    fn from(value: ImagerDeconvolver) -> Self {
        match value {
            ImagerDeconvolver::Hogbom => Self::Hogbom,
            ImagerDeconvolver::Mtmfs => Self::Mtmfs,
            ImagerDeconvolver::Clark => Self::Clark,
            ImagerDeconvolver::Multiscale => Self::Multiscale,
        }
    }
}

/// `w`-term handling mode for the imaging task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerWTermMode {
    /// Standard 2-D imaging.
    #[default]
    None,
    /// Exact direct per-sample correction.
    Direct,
    /// `wproject` request.
    Wproject,
}

impl From<WTermMode> for ImagerWTermMode {
    fn from(value: WTermMode) -> Self {
        match value {
            WTermMode::None => Self::None,
            WTermMode::Direct => Self::Direct,
            WTermMode::WProject => Self::Wproject,
        }
    }
}

impl From<ImagerWTermMode> for WTermMode {
    fn from(value: ImagerWTermMode) -> Self {
        match value {
            ImagerWTermMode::None => Self::None,
            ImagerWTermMode::Direct => Self::Direct,
            ImagerWTermMode::Wproject => Self::WProject,
        }
    }
}

/// Spectral interpolation policy for cube imaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerCubeInterpolation {
    /// Nearest-neighbour interpolation.
    Nearest,
    /// Linear interpolation.
    #[default]
    Linear,
}

impl From<CubeInterpolation> for ImagerCubeInterpolation {
    fn from(value: CubeInterpolation) -> Self {
        match value {
            CubeInterpolation::Nearest => Self::Nearest,
            CubeInterpolation::Linear | CubeInterpolation::Cubic => Self::Linear,
        }
    }
}

impl From<ImagerCubeInterpolation> for CubeInterpolation {
    fn from(value: ImagerCubeInterpolation) -> Self {
        match value {
            ImagerCubeInterpolation::Nearest => Self::Nearest,
            ImagerCubeInterpolation::Linear => Self::Linear,
        }
    }
}

/// Typed cube-axis value corresponding to CASA `start` / `width`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImagerCubeAxisValue {
    /// Channel number in the source SPW.
    Channel {
        /// Zero-based source channel.
        channel: i32,
    },
    /// Frequency-like quantity in Hz.
    FrequencyHz {
        /// Frequency value in Hz.
        hz: f64,
        /// Optional explicit frequency frame.
        frame: Option<String>,
    },
    /// Velocity-like quantity in m/s.
    VelocityMs {
        /// Velocity value in m/s.
        ms: f64,
        /// Optional explicit frequency frame.
        frame: Option<String>,
    },
    /// Dimensionless Doppler value.
    Doppler {
        /// Doppler value in the chosen convention.
        value: f64,
        /// Doppler convention name.
        convention: String,
    },
}

impl From<&CubeAxisValue> for ImagerCubeAxisValue {
    fn from(value: &CubeAxisValue) -> Self {
        match value {
            CubeAxisValue::Channel(channel) => Self::Channel { channel: *channel },
            CubeAxisValue::FrequencyHz { hz, frame } => Self::FrequencyHz {
                hz: *hz,
                frame: frame.map(|frame| frame.to_string()),
            },
            CubeAxisValue::VelocityMs { ms, frame } => Self::VelocityMs {
                ms: *ms,
                frame: frame.map(|frame| frame.to_string()),
            },
            CubeAxisValue::Doppler { value, convention } => Self::Doppler {
                value: *value,
                convention: convention.to_string(),
            },
        }
    }
}

impl ImagerCubeAxisValue {
    fn into_runtime(self) -> Result<CubeAxisValue, String> {
        Ok(match self {
            Self::Channel { channel } => CubeAxisValue::Channel(channel),
            Self::FrequencyHz { hz, frame } => CubeAxisValue::FrequencyHz {
                hz,
                frame: frame.as_deref().map(parse_frequency_ref).transpose()?,
            },
            Self::VelocityMs { ms, frame } => CubeAxisValue::VelocityMs {
                ms,
                frame: frame.as_deref().map(parse_frequency_ref).transpose()?,
            },
            Self::Doppler { value, convention } => CubeAxisValue::Doppler {
                value,
                convention: parse_doppler_ref(&convention)?,
            },
        })
    }
}

/// CASA-style cube-axis construction options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerCubeAxisConfig {
    /// Output frequency frame for the image spectral axis.
    #[serde(default = "default_frequency_ref")]
    pub outframe: String,
    /// Velocity convention used by velocity-like start/width values.
    #[serde(default = "default_doppler_ref")]
    pub veltype: String,
    /// Output-axis interpolation policy.
    #[serde(default)]
    pub interpolation: ImagerCubeInterpolation,
    /// Rest frequency in Hz.
    #[serde(default)]
    pub rest_frequency_hz: Option<f64>,
    /// Optional cube-axis start value.
    #[serde(default)]
    pub start: Option<ImagerCubeAxisValue>,
    /// Optional cube-axis width value.
    #[serde(default)]
    pub width: Option<ImagerCubeAxisValue>,
}

impl Default for ImagerCubeAxisConfig {
    fn default() -> Self {
        Self {
            outframe: default_frequency_ref(),
            veltype: default_doppler_ref(),
            interpolation: ImagerCubeInterpolation::Linear,
            rest_frequency_hz: None,
            start: None,
            width: None,
        }
    }
}

impl From<&CubeAxisConfig> for ImagerCubeAxisConfig {
    fn from(value: &CubeAxisConfig) -> Self {
        Self {
            outframe: value.outframe.to_string(),
            veltype: value.veltype.to_string(),
            interpolation: value.interpolation.into(),
            rest_frequency_hz: value.rest_frequency_hz,
            start: value.start.as_ref().map(ImagerCubeAxisValue::from),
            width: value.width.as_ref().map(ImagerCubeAxisValue::from),
        }
    }
}

impl ImagerCubeAxisConfig {
    fn into_runtime(self, spectral_mode: SpectralMode) -> Result<CubeAxisConfig, String> {
        let veltype = parse_doppler_ref(&self.veltype)?;
        if let Some(rest_frequency_hz) = self.rest_frequency_hz {
            let text = format!("{rest_frequency_hz}Hz");
            parse_ms_rest_frequency_hz(&text).map_err(|error| error.to_string())?;
        }
        Ok(CubeAxisConfig {
            specmode: spectral_mode.cube_specmode(),
            outframe: parse_frequency_ref(&self.outframe)?,
            veltype,
            interpolation: self.interpolation.into(),
            rest_frequency_hz: self.rest_frequency_hz,
            start: self.start.map(|value| value.into_runtime()).transpose()?,
            width: self.width.map(|value| value.into_runtime()).transpose()?,
        })
    }
}

/// One axis length for a CASA-style Gaussian UV taper.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImagerUvTaperSize {
    /// Image-domain Gaussian FWHM in radians.
    ImageFwhmRad {
        /// Taper size in radians.
        value: f64,
    },
    /// UV-domain Gaussian HWHM in wavelengths.
    BaselineHwhmLambda {
        /// Taper size in wavelengths.
        value: f64,
    },
}

impl From<UvTaperSize> for ImagerUvTaperSize {
    fn from(value: UvTaperSize) -> Self {
        match value {
            UvTaperSize::ImageFwhmRad(value) => Self::ImageFwhmRad { value },
            UvTaperSize::BaselineHwhmLambda(value) => Self::BaselineHwhmLambda { value },
        }
    }
}

impl From<ImagerUvTaperSize> for UvTaperSize {
    fn from(value: ImagerUvTaperSize) -> Self {
        match value {
            ImagerUvTaperSize::ImageFwhmRad { value } => Self::ImageFwhmRad(value),
            ImagerUvTaperSize::BaselineHwhmLambda { value } => Self::BaselineHwhmLambda(value),
        }
    }
}

/// CASA-style Gaussian UV taper applied after imaging-weight calculation.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerUvTaper {
    /// Major-axis taper size.
    pub major: ImagerUvTaperSize,
    /// Minor-axis taper size.
    pub minor: ImagerUvTaperSize,
    /// Position angle in radians, zero along +y and increasing toward -x.
    pub position_angle_rad: f64,
}

impl From<GaussianUvTaper> for ImagerUvTaper {
    fn from(value: GaussianUvTaper) -> Self {
        Self {
            major: value.major.into(),
            minor: value.minor.into(),
            position_angle_rad: value.position_angle_rad,
        }
    }
}

impl From<ImagerUvTaper> for GaussianUvTaper {
    fn from(value: ImagerUvTaper) -> Self {
        Self {
            major: value.major.into(),
            minor: value.minor.into(),
            position_angle_rad: value.position_angle_rad,
        }
    }
}

/// Canonical imager task request for one end-to-end run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerRunTaskRequest {
    /// Input MeasurementSet path.
    pub measurement_set: PathBuf,
    /// Output image prefix.
    pub image_name: PathBuf,
    /// Square image size in pixels.
    pub image_size: usize,
    /// Cell size in arcseconds.
    pub cell_arcsec: f64,
    /// Optional selected `FIELD_ID`s.
    #[serde(default)]
    pub field_ids: Option<Vec<i32>>,
    /// Optional `FIELD_ID` used as the image phase center.
    #[serde(default)]
    pub phasecenter_field: Option<i32>,
    /// Optional explicit CASA-style phase center.
    #[serde(default)]
    pub phasecenter: Option<String>,
    /// Optional `DATA_DESC_ID` restriction.
    #[serde(default)]
    pub ddid: Option<i32>,
    /// Optional CASA-style SPW selector text.
    #[serde(default)]
    pub spw_selector: Option<String>,
    /// Optional first selected input channel.
    #[serde(default)]
    pub channel_start: Option<usize>,
    /// Optional selected-channel count.
    #[serde(default)]
    pub channel_count: Option<usize>,
    /// Optional explicit data-column override.
    #[serde(default)]
    pub data_column: Option<String>,
    /// Optional explicit scalar plane or raw correlation.
    #[serde(default)]
    pub correlation: Option<ImagerPlaneSelection>,
    /// Spectral imaging mode.
    #[serde(default)]
    pub spectral_mode: ImagerSpectralMode,
    /// CASA-style cube-axis configuration.
    #[serde(default)]
    pub cube_axis: ImagerCubeAxisConfig,
    /// Visibility weighting policy.
    #[serde(default)]
    pub weighting: ImagerWeighting,
    /// CASA-style `perchanweightdensity` toggle for spectral cubes.
    #[serde(default)]
    pub per_channel_weight_density: bool,
    /// Optional CASA-style Gaussian UV taper.
    #[serde(default)]
    pub uv_taper: Option<ImagerUvTaper>,
    /// Restoring-beam policy for restored products.
    #[serde(default)]
    pub restoring_beam_mode: ImagerRestoringBeamMode,
    /// Requested minor-cycle deconvolver.
    #[serde(default)]
    pub deconvolver: ImagerDeconvolver,
    /// Requested MTMFS Taylor-term count.
    #[serde(default = "default_nterms")]
    pub nterms: usize,
    /// Requested multiscale kernel sizes in pixels.
    #[serde(default)]
    pub multiscale_scales: Vec<f32>,
    /// CASA-style multiscale selection bias.
    #[serde(default)]
    pub small_scale_bias: f32,
    /// Minor-cycle iteration count.
    #[serde(default)]
    pub niter: usize,
    /// Minor-cycle loop gain.
    #[serde(default = "default_gain")]
    pub gain: f32,
    /// Absolute CLEAN stopping threshold in `Jy/beam`.
    #[serde(default)]
    pub threshold_jy: f32,
    /// CASA-style robust-RMS stopping multiplier.
    #[serde(default)]
    pub nsigma: f32,
    /// Restoring-beam fit cutoff.
    #[serde(default = "default_psf_cutoff")]
    pub psf_cutoff: f32,
    /// Residual-refresh cadence.
    #[serde(default = "default_minor_cycle_length")]
    pub minor_cycle_length: usize,
    /// CASA-style cycle-threshold scale factor.
    #[serde(default = "default_cyclefactor")]
    pub cyclefactor: f32,
    /// Lower clamp for the PSF fraction used to derive cycle thresholds.
    #[serde(default = "default_min_psf_fraction")]
    pub min_psf_fraction: f32,
    /// Upper clamp for the PSF fraction used to derive cycle thresholds.
    #[serde(default = "default_max_psf_fraction")]
    pub max_psf_fraction: f32,
    /// Optional inclusive pixel-space clean boxes `(x0, y0, x1, y1)`.
    #[serde(default)]
    pub mask_boxes: Vec<[usize; 4]>,
    /// Optional CASA image mask whose non-zero pixels are cleanable.
    #[serde(default)]
    pub mask_image: Option<PathBuf>,
    /// Requested `w`-term handling mode.
    #[serde(default)]
    pub w_term_mode: ImagerWTermMode,
    /// Optional explicit `wproject` plane budget.
    #[serde(default)]
    pub w_project_planes: Option<usize>,
    /// Skip CLEAN and only write dirty/residual products.
    #[serde(default)]
    pub dirty_only: bool,
    /// Write PNG preview sidecars for the CASA image products.
    #[serde(default = "default_write_preview_pngs")]
    pub write_preview_pngs: bool,
}

impl ImagerRunTaskRequest {
    /// Build the canonical request from one parsed CLI config.
    pub fn from_cli_config(config: &CliConfig) -> Self {
        Self {
            measurement_set: config.ms.clone(),
            image_name: config.imagename.clone(),
            image_size: config.imsize,
            cell_arcsec: config.cell_arcsec,
            field_ids: config.field_ids.clone(),
            phasecenter_field: config.phasecenter_field,
            phasecenter: config.phasecenter.clone(),
            ddid: config.ddid,
            spw_selector: config.spw_selector.clone(),
            channel_start: config.channel_start,
            channel_count: config.channel_count,
            data_column: config.datacolumn.clone(),
            correlation: config
                .correlation
                .as_deref()
                .map(Self::plane_from_text)
                .transpose()
                .expect("CliConfig correlation should already be valid"),
            spectral_mode: config.spectral_mode.into(),
            cube_axis: (&config.cube_axis).into(),
            weighting: config.weighting.into(),
            per_channel_weight_density: config.per_channel_weight_density,
            uv_taper: config.uv_taper.map(Into::into),
            restoring_beam_mode: config.restoring_beam_mode.into(),
            deconvolver: config.deconvolver.into(),
            nterms: config.nterms,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            niter: config.niter,
            gain: config.gain,
            threshold_jy: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
            mask_boxes: config.mask_boxes.clone(),
            mask_image: config.mask_image.clone(),
            w_term_mode: config.w_term_mode.into(),
            w_project_planes: config.w_project_planes,
            dirty_only: config.dirty_only,
            write_preview_pngs: config.write_preview_pngs,
        }
    }

    /// Convert the canonical request back into the runtime CLI config.
    pub fn to_cli_config(&self) -> Result<CliConfig, String> {
        let spectral_mode: SpectralMode = self.spectral_mode.into();
        let deconvolver: Deconvolver = self.deconvolver.into();
        if self.phasecenter_field.is_some() && self.phasecenter.is_some() {
            return Err("--phasecenter and --phasecenter-field are mutually exclusive".to_string());
        }
        if deconvolver == Deconvolver::Mtmfs && spectral_mode != SpectralMode::Mfs {
            return Err("deconvolver='mtmfs' currently requires specmode='mfs'".to_string());
        }
        if deconvolver != Deconvolver::Mtmfs && self.nterms != 1 {
            return Err("nterms > 1 currently requires deconvolver='mtmfs'".to_string());
        }
        if self.nterms == 0 {
            return Err("nterms must be at least 1".to_string());
        }
        for scale in &self.multiscale_scales {
            if !(scale.is_finite() && *scale >= 0.0) {
                return Err(format!(
                    "invalid multiscale scale {scale}; expected finite value >= 0"
                ));
            }
        }
        Ok(CliConfig {
            ms: self.measurement_set.clone(),
            imagename: self.image_name.clone(),
            imsize: self.image_size,
            cell_arcsec: self.cell_arcsec,
            field_ids: self.field_ids.clone(),
            phasecenter_field: self.phasecenter_field,
            phasecenter: self.phasecenter.clone(),
            ddid: self.ddid,
            spw: self
                .spw_selector
                .as_deref()
                .and_then(|selector| selector.trim().parse::<i32>().ok()),
            spw_selector: self.spw_selector.clone(),
            channel_start: self.channel_start,
            channel_count: self.channel_count,
            datacolumn: self.data_column.clone(),
            correlation: self
                .correlation
                .map(|value| value.as_cli_text().to_string()),
            spectral_mode,
            cube_axis: self.cube_axis.clone().into_runtime(spectral_mode)?,
            weighting: self.weighting.clone().into(),
            per_channel_weight_density: self.per_channel_weight_density,
            uv_taper: self.uv_taper.map(Into::into),
            restoring_beam_mode: self.restoring_beam_mode.into(),
            deconvolver,
            nterms: self.nterms,
            multiscale_scales: self.multiscale_scales.clone(),
            small_scale_bias: self.small_scale_bias,
            niter: self.niter,
            gain: self.gain,
            threshold_jy: self.threshold_jy,
            nsigma: self.nsigma,
            psf_cutoff: self.psf_cutoff,
            minor_cycle_length: self.minor_cycle_length,
            cyclefactor: self.cyclefactor,
            min_psf_fraction: self.min_psf_fraction,
            max_psf_fraction: self.max_psf_fraction,
            mask_boxes: self.mask_boxes.clone(),
            mask_image: self.mask_image.clone(),
            w_term_mode: self.w_term_mode.into(),
            w_project_planes: self.w_project_planes,
            dirty_only: self.dirty_only,
            write_preview_pngs: self.write_preview_pngs,
        })
    }

    /// Execute the imaging task and return the canonical run result.
    pub fn execute(&self) -> Result<ImagerRunTaskResult, String> {
        let summary = run_from_config(&self.to_cli_config()?)?;
        Ok(ImagerRunTaskResult::from_run(self.clone(), &summary))
    }

    fn plane_from_text(text: &str) -> Result<ImagerPlaneSelection, String> {
        ImagerPlaneSelection::from_cli_text(text)
    }
}

/// Stable stop reasons for CLEAN controller completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImagerCleanStopReason {
    /// The requested global CLEAN threshold was already satisfied.
    GlobalThresholdReached,
    /// The requested robust-RMS-derived `nsigma` threshold was satisfied.
    NsigmaThresholdReached,
    /// The current minor cycle hit its CASA-style `cyclethreshold`.
    CycleThresholdReached,
    /// The requested total iteration budget was exhausted.
    IterationLimitReached,
    /// No cleanable masked pixel was available.
    NoCleanablePixels,
    /// The residual peak increased materially after prior progress.
    DivergenceDetected,
}

impl From<CleanStopReason> for ImagerCleanStopReason {
    fn from(value: CleanStopReason) -> Self {
        match value {
            CleanStopReason::GlobalThresholdReached => Self::GlobalThresholdReached,
            CleanStopReason::NsigmaThresholdReached => Self::NsigmaThresholdReached,
            CleanStopReason::CycleThresholdReached => Self::CycleThresholdReached,
            CleanStopReason::IterationLimitReached => Self::IterationLimitReached,
            CleanStopReason::NoCleanablePixels => Self::NoCleanablePixels,
            CleanStopReason::DivergenceDetected => Self::DivergenceDetected,
        }
    }
}

/// Stable timing breakdown reported by the pure imaging core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerCoreStageTimings {
    /// Controller bookkeeping time outside explicit solve/refresh stages.
    pub controller_overhead_ns: u64,
    /// Imaging-weighting and taper time.
    pub weighting_ns: u64,
    /// PSF grid time.
    pub psf_grid_ns: u64,
    /// PSF FFT time.
    pub psf_fft_ns: u64,
    /// PSF normalization time.
    pub psf_normalize_ns: u64,
    /// Model FFT time.
    pub model_fft_ns: u64,
    /// Residual degrid/grid time.
    pub residual_degrid_grid_ns: u64,
    /// Residual FFT time.
    pub residual_fft_ns: u64,
    /// Residual normalization time.
    pub residual_normalize_ns: u64,
    /// Minor-cycle total time.
    pub minor_cycle_ns: u64,
    /// Solver-only minor-cycle time.
    pub minor_cycle_solve_ns: u64,
    /// Major-cycle refresh time.
    pub major_cycle_refresh_ns: u64,
    /// Restoring-beam fit time.
    pub beam_fit_ns: u64,
    /// Restore time.
    pub restore_ns: u64,
    /// Total imaging-core time.
    pub total_ns: u64,
}

/// Stable timing breakdown for the MeasurementSet-backed frontend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerFrontendStageTimings {
    /// Time spent opening the MeasurementSet.
    pub open_measurement_set_ns: u64,
    /// Time spent resolving selection and adapting rows.
    pub prepare_plane_input_ns: u64,
    /// Time spent extracting and validating the phase center.
    pub extract_phase_center_ns: u64,
    /// Time spent inside the pure imaging core.
    pub run_imaging_ns: u64,
    /// Time spent building output coordinates.
    pub build_coordinate_system_ns: u64,
    /// Time spent writing image products.
    pub write_products_ns: u64,
    /// Total end-to-end frontend time.
    pub total_ns: u64,
}

/// Channel-level convergence summary for cube imaging.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerChannelRunResult {
    /// Zero-based output channel index.
    pub channel_index: usize,
    /// Major-cycle count for this plane.
    pub major_cycles: usize,
    /// Minor-cycle component updates for this plane.
    pub minor_iterations: usize,
    /// Final CLEAN stop reason for this plane.
    pub clean_stop_reason: Option<ImagerCleanStopReason>,
    /// Peak residual before minor cycles.
    pub initial_residual_peak_jy_per_beam: f32,
    /// Peak residual after the final exact refresh.
    pub final_residual_peak_jy_per_beam: f32,
    /// Final CASA-style cycle threshold for this plane.
    pub final_cycle_threshold_jy_per_beam: f32,
    /// Whether the beam-fit debug summary was available for this plane.
    pub beam_fit_available: bool,
}

/// Stable run metrics emitted after one successful imaging run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerRunReport {
    /// Warnings emitted by the imaging run.
    pub warnings: Vec<String>,
    /// Number of scalar samples that reached the gridder.
    pub gridded_samples: usize,
    /// Total major-cycle count reported by the run.
    pub major_cycles: usize,
    /// Total minor-cycle component updates reported by the run.
    pub minor_iterations: usize,
    /// Final CLEAN stop reason when deconvolution ran.
    pub clean_stop_reason: Option<ImagerCleanStopReason>,
    /// Timing breakdown reported by the pure imaging core.
    pub stage_timings: ImagerCoreStageTimings,
    /// Timing breakdown for the MeasurementSet-backed frontend.
    pub frontend_timings: ImagerFrontendStageTimings,
    /// Channel-level diagnostics for cube-like runs.
    pub channels: Vec<ImagerChannelRunResult>,
}

/// Stable artifact kind identifiers for written image products.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImagerArtifactKind {
    /// Point-spread function image.
    Psf,
    /// Residual image.
    Residual,
    /// Model image.
    Model,
    /// Restored image.
    Image,
    /// Spectral-index image.
    Alpha,
}

impl ImagerArtifactKind {
    fn as_suffix(self) -> &'static str {
        match self {
            Self::Psf => "psf",
            Self::Residual => "residual",
            Self::Model => "model",
            Self::Image => "image",
            Self::Alpha => "alpha",
        }
    }
}

/// One expected output artifact written by the imaging run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerArtifact {
    /// Stable artifact kind identifier.
    pub kind: ImagerArtifactKind,
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

/// Canonical imager task result for one end-to-end run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerRunTaskResult {
    /// Request echo for result attribution.
    pub request: ImagerRunTaskRequest,
    /// Stable run report.
    pub run: ImagerRunReport,
    /// Expected written image products.
    pub artifacts: Vec<ImagerArtifact>,
}

impl ImagerRunTaskResult {
    /// Build the canonical run result from one completed run.
    pub fn from_run(request: ImagerRunTaskRequest, summary: &RunSummary) -> Self {
        Self {
            request: request.clone(),
            run: ImagerRunReport {
                warnings: summary.warnings.clone(),
                gridded_samples: summary.gridded_samples,
                major_cycles: summary.major_cycles,
                minor_iterations: summary.minor_iterations,
                clean_stop_reason: summary.clean_stop_reason.map(Into::into),
                stage_timings: core_stage_timings(&summary.stage_timings),
                frontend_timings: frontend_stage_timings(summary.frontend_timings),
                channels: summary
                    .channel_summaries
                    .iter()
                    .map(channel_result)
                    .collect(),
            },
            artifacts: build_artifacts(&request),
        }
    }
}

/// Canonical imager task request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ImagerTaskRequest {
    /// Execute one end-to-end imaging run.
    Run(ImagerRunTaskRequest),
}

impl ImagerTaskRequest {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<ImagerTaskResult, String> {
        match self {
            Self::Run(request) => Ok(ImagerTaskResult::Run(request.execute()?)),
        }
    }

    /// Read one task request from a file path or `-` for stdin.
    pub fn read_from_source(source: &str) -> Result<Self, String> {
        let payload = if source == "-" {
            let mut payload = String::new();
            std::io::stdin()
                .read_to_string(&mut payload)
                .map_err(|error| format!("failed to read JSON request from stdin: {error}"))?;
            payload
        } else {
            fs::read_to_string(source).map_err(|error| {
                format!(
                    "failed to read JSON request from {}: {error}",
                    Path::new(source).display()
                )
            })?
        };
        serde_json::from_str(&payload)
            .map_err(|error| format!("failed to parse imager task request: {error}"))
    }
}

/// Canonical imager task result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ImagerTaskResult {
    /// Completed end-to-end imaging run.
    Run(ImagerRunTaskResult),
}

fn parse_frequency_ref(value: &str) -> Result<FrequencyRef, String> {
    value
        .parse::<FrequencyRef>()
        .map_err(|error| format!("parse frequency frame {value:?}: {error}"))
}

fn parse_doppler_ref(value: &str) -> Result<DopplerRef, String> {
    value
        .parse::<DopplerRef>()
        .map_err(|error| format!("parse doppler convention {value:?}: {error}"))
}

fn default_frequency_ref() -> String {
    FrequencyRef::LSRK.to_string()
}

fn default_doppler_ref() -> String {
    DopplerRef::RADIO.to_string()
}

fn default_nterms() -> usize {
    1
}

fn default_gain() -> f32 {
    0.1
}

fn default_psf_cutoff() -> f32 {
    0.35
}

fn default_minor_cycle_length() -> usize {
    8
}

fn default_cyclefactor() -> f32 {
    1.0
}

fn default_min_psf_fraction() -> f32 {
    0.1
}

fn default_max_psf_fraction() -> f32 {
    0.8
}

fn default_write_preview_pngs() -> bool {
    true
}

fn core_stage_timings(timings: &casa_imaging::ImagingStageTimings) -> ImagerCoreStageTimings {
    ImagerCoreStageTimings {
        controller_overhead_ns: timings.controller_overhead.as_nanos() as u64,
        weighting_ns: timings.weighting.as_nanos() as u64,
        psf_grid_ns: timings.psf_grid.as_nanos() as u64,
        psf_fft_ns: timings.psf_fft.as_nanos() as u64,
        psf_normalize_ns: timings.psf_normalize.as_nanos() as u64,
        model_fft_ns: timings.model_fft.as_nanos() as u64,
        residual_degrid_grid_ns: timings.residual_degrid_grid.as_nanos() as u64,
        residual_fft_ns: timings.residual_fft.as_nanos() as u64,
        residual_normalize_ns: timings.residual_normalize.as_nanos() as u64,
        minor_cycle_ns: timings.minor_cycle.as_nanos() as u64,
        minor_cycle_solve_ns: timings.minor_cycle_solve.as_nanos() as u64,
        major_cycle_refresh_ns: timings.major_cycle_refresh.as_nanos() as u64,
        beam_fit_ns: timings.beam_fit.as_nanos() as u64,
        restore_ns: timings.restore.as_nanos() as u64,
        total_ns: timings.total.as_nanos() as u64,
    }
}

fn frontend_stage_timings(timings: FrontendStageTimings) -> ImagerFrontendStageTimings {
    ImagerFrontendStageTimings {
        open_measurement_set_ns: timings.open_measurement_set.as_nanos() as u64,
        prepare_plane_input_ns: timings.prepare_plane_input.as_nanos() as u64,
        extract_phase_center_ns: timings.extract_phase_center.as_nanos() as u64,
        run_imaging_ns: timings.run_imaging.as_nanos() as u64,
        build_coordinate_system_ns: timings.build_coordinate_system.as_nanos() as u64,
        write_products_ns: timings.write_products.as_nanos() as u64,
        total_ns: timings.total.as_nanos() as u64,
    }
}

fn channel_result(summary: &ChannelRunSummary) -> ImagerChannelRunResult {
    ImagerChannelRunResult {
        channel_index: summary.channel_index,
        major_cycles: summary.major_cycles,
        minor_iterations: summary.minor_iterations,
        clean_stop_reason: summary.clean_stop_reason.map(Into::into),
        initial_residual_peak_jy_per_beam: summary.initial_residual_peak_jy_per_beam,
        final_residual_peak_jy_per_beam: summary.final_residual_peak_jy_per_beam,
        final_cycle_threshold_jy_per_beam: summary.final_cycle_threshold_jy_per_beam,
        beam_fit_available: summary.beam_fit_debug.is_some(),
    }
}

fn artifact(
    kind: ImagerArtifactKind,
    label: String,
    path: PathBuf,
    preview: Option<PathBuf>,
) -> ImagerArtifact {
    ImagerArtifact {
        kind,
        label,
        exists: path.exists(),
        path: path.display().to_string(),
        preview_png_path: preview.as_ref().map(|path| path.display().to_string()),
        preview_png_exists: preview.as_ref().is_some_and(|path| path.exists()),
    }
}

fn build_artifacts(request: &ImagerRunTaskRequest) -> Vec<ImagerArtifact> {
    let base = request.image_name.to_string_lossy().to_string();
    let mut artifacts = Vec::new();
    match request.spectral_mode {
        ImagerSpectralMode::Mfs
            if request.deconvolver == ImagerDeconvolver::Mtmfs && request.nterms > 1 =>
        {
            for term in 0..request.nterms {
                for (kind, label) in [
                    (ImagerArtifactKind::Psf, "PSF"),
                    (ImagerArtifactKind::Residual, "Residual"),
                    (ImagerArtifactKind::Model, "Model"),
                    (ImagerArtifactKind::Image, "Restored Image"),
                ] {
                    let suffix = format!("{}.tt{term}", kind.as_suffix());
                    let preview = (term == 0 && request.write_preview_pngs)
                        .then(|| PathBuf::from(format!("{base}.{suffix}.png")));
                    artifacts.push(artifact(
                        kind,
                        format!("{label} tt{term}"),
                        PathBuf::from(format!("{base}.{suffix}")),
                        preview,
                    ));
                }
            }
            let alpha_preview = request
                .write_preview_pngs
                .then(|| PathBuf::from(format!("{base}.alpha.png")));
            artifacts.push(artifact(
                ImagerArtifactKind::Alpha,
                "Spectral Index".to_string(),
                PathBuf::from(format!("{base}.alpha")),
                alpha_preview,
            ));
        }
        _ => {
            for (kind, label) in [
                (ImagerArtifactKind::Psf, "PSF"),
                (ImagerArtifactKind::Residual, "Residual"),
                (ImagerArtifactKind::Model, "Model"),
                (ImagerArtifactKind::Image, "Restored Image"),
            ] {
                let suffix = kind.as_suffix();
                let preview = request
                    .write_preview_pngs
                    .then(|| PathBuf::from(format!("{base}.{suffix}.png")));
                artifacts.push(artifact(
                    kind,
                    label.to_string(),
                    PathBuf::from(format!("{base}.{suffix}")),
                    preview,
                ));
            }
        }
    }
    artifacts
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    use casa_imaging::{
        BeamFitDebugSummary, CleanStopReason, Deconvolver, ImagingStageTimings, RestoringBeamMode,
        WTermMode, WeightingMode,
    };
    use casa_provider_contracts::ProviderSurfaceKind;
    use tempfile::TempDir;

    use super::{
        IMAGER_TASK_PROTOCOL_NAME, IMAGER_TASK_PROTOCOL_VERSION, ImagerCleanStopReason,
        ImagerCubeAxisConfig, ImagerCubeAxisValue, ImagerDeconvolver, ImagerPlaneSelection,
        ImagerRunTaskRequest, ImagerTaskRequest, ImagerTaskSchemaBundle, ImagerWeighting,
        default_cyclefactor, default_doppler_ref, default_frequency_ref, default_gain,
        default_max_psf_fraction, default_min_psf_fraction, default_minor_cycle_length,
        default_nterms, default_psf_cutoff, default_write_preview_pngs,
    };
    use crate::{ChannelRunSummary, CliConfig, FrontendStageTimings, RunSummary, SpectralMode};

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
                psf_normalize: Duration::from_nanos(45),
                model_fft: Duration::from_nanos(46),
                residual_degrid_grid: Duration::from_nanos(50),
                residual_fft: Duration::from_nanos(60),
                residual_normalize: Duration::from_nanos(70),
                minor_cycle: Duration::from_nanos(80),
                minor_cycle_solve: Duration::from_nanos(90),
                major_cycle_refresh: Duration::from_nanos(100),
                beam_fit: Duration::from_nanos(101),
                restore: Duration::from_nanos(102),
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

    fn default_request() -> ImagerRunTaskRequest {
        ImagerRunTaskRequest {
            measurement_set: PathBuf::from("demo.ms"),
            image_name: PathBuf::from("out/demo"),
            image_size: 64,
            cell_arcsec: 1.5,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            data_column: None,
            correlation: None,
            spectral_mode: Default::default(),
            cube_axis: Default::default(),
            weighting: Default::default(),
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: Default::default(),
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
            w_term_mode: Default::default(),
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        }
    }

    #[test]
    fn schema_bundle_uses_current_protocol_and_definitions() {
        let bundle = ImagerTaskSchemaBundle::current();
        assert_eq!(bundle.protocol.protocol_name, IMAGER_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            IMAGER_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 1);
        assert_eq!(bundle.semantic.operations[0].request_kind, "run");
        assert!(bundle.components.contains_key("ImagerRunTaskRequest"));
        assert!(bundle.projections.ui_schema.is_some());
        let request_schema = serde_json::to_value(&bundle.request_schema).unwrap();
        let result_schema = serde_json::to_value(&bundle.result_schema).unwrap();
        assert!(request_schema.to_string().contains("ImagerTaskRequest"));
        assert!(result_schema.to_string().contains("ImagerTaskResult"));
        let ui_schema = bundle.ui_schema_projection().expect("ui schema projection");
        assert_eq!(ui_schema.command_id, "imager");
    }

    #[test]
    fn run_request_round_trips_cli_config() {
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
            OsString::from("--spw"),
            OsString::from("5:10~19"),
            OsString::from("--datacolumn"),
            OsString::from("CORRECTED_DATA"),
            OsString::from("--corr"),
            OsString::from("XX"),
            OsString::from("--specmode"),
            OsString::from("cube"),
            OsString::from("--start"),
            OsString::from("1.1GHz"),
            OsString::from("--width"),
            OsString::from("10MHz"),
            OsString::from("--outframe"),
            OsString::from("BARY"),
            OsString::from("--veltype"),
            OsString::from("optical"),
            OsString::from("--interpolation"),
            OsString::from("nearest"),
            OsString::from("--restfreq"),
            OsString::from("1.42GHz"),
            OsString::from("--weighting"),
            OsString::from("briggs"),
            OsString::from("--robust"),
            OsString::from("-1.0"),
            OsString::from("--perchanweightdensity"),
            OsString::from("--uvtaper"),
            OsString::from("10arcsec,8arcsec,45deg"),
            OsString::from("--restoringbeam"),
            OsString::from("common"),
            OsString::from("--deconvolver"),
            OsString::from("multiscale"),
            OsString::from("--scales"),
            OsString::from("0,5,15"),
            OsString::from("--smallscalebias"),
            OsString::from("0.25"),
            OsString::from("--niter"),
            OsString::from("12"),
            OsString::from("--gain"),
            OsString::from("0.2"),
            OsString::from("--threshold-jy"),
            OsString::from("0.01"),
            OsString::from("--nsigma"),
            OsString::from("4.0"),
            OsString::from("--psfcutoff"),
            OsString::from("0.4"),
            OsString::from("--minor-cycle-length"),
            OsString::from("11"),
            OsString::from("--cyclefactor"),
            OsString::from("1.5"),
            OsString::from("--minpsffraction"),
            OsString::from("0.2"),
            OsString::from("--maxpsffraction"),
            OsString::from("0.7"),
            OsString::from("--mask-box"),
            OsString::from("1,2,10,20"),
            OsString::from("--mask-image"),
            OsString::from("demo.mask"),
            OsString::from("--wterm"),
            OsString::from("wproject"),
            OsString::from("--wprojplanes"),
            OsString::from("8"),
            OsString::from("--dirty-only"),
            OsString::from("--no-preview-pngs"),
        ])
        .unwrap();

        let request = ImagerRunTaskRequest::from_cli_config(&config);
        let restored = request.to_cli_config().unwrap();

        assert_eq!(restored.ms, PathBuf::from("demo.ms"));
        assert_eq!(restored.imagename, PathBuf::from("out/demo"));
        assert_eq!(restored.field_ids, Some(vec![0, 2, 3]));
        assert_eq!(restored.phasecenter_field, Some(2));
        assert_eq!(restored.spw_selector.as_deref(), Some("5:10~19"));
        assert_eq!(restored.datacolumn.as_deref(), Some("CORRECTED_DATA"));
        assert_eq!(restored.correlation.as_deref(), Some("XX"));
        assert_eq!(restored.spectral_mode, SpectralMode::Cube);
        assert_eq!(restored.weighting, WeightingMode::Briggs { robust: -1.0 });
        assert!(restored.per_channel_weight_density);
        assert_eq!(restored.restoring_beam_mode, RestoringBeamMode::Common);
        assert_eq!(restored.deconvolver, Deconvolver::Multiscale);
        assert_eq!(restored.w_term_mode, WTermMode::WProject);
        assert_eq!(restored.w_project_planes, Some(8));
        assert!(restored.dirty_only);
        assert!(!restored.write_preview_pngs);
    }

    #[test]
    fn task_request_defaults_match_cli_defaults() {
        let request = ImagerRunTaskRequest {
            measurement_set: PathBuf::from("demo.ms"),
            image_name: PathBuf::from("out/demo"),
            image_size: 64,
            cell_arcsec: 1.5,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw_selector: None,
            channel_start: None,
            channel_count: None,
            data_column: None,
            correlation: None,
            spectral_mode: Default::default(),
            cube_axis: Default::default(),
            weighting: Default::default(),
            per_channel_weight_density: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: Default::default(),
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
            w_term_mode: Default::default(),
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        };
        let config = request.to_cli_config().unwrap();
        assert_eq!(config.weighting, WeightingMode::Natural);
        assert_eq!(config.deconvolver, Deconvolver::Hogbom);
        assert_eq!(config.spectral_mode, SpectralMode::Mfs);
    }

    #[test]
    fn briggs_weighting_round_trips() {
        let request = ImagerRunTaskRequest {
            weighting: ImagerWeighting::Briggs { robust: 0.5 },
            ..default_request()
        };
        let config = request.to_cli_config().unwrap();
        assert_eq!(config.weighting, WeightingMode::Briggs { robust: 0.5 });
    }

    #[test]
    fn plane_selection_and_enum_conversions_cover_public_variants() {
        let cases = [
            (ImagerPlaneSelection::StokesI, "I"),
            (ImagerPlaneSelection::StokesQ, "Q"),
            (ImagerPlaneSelection::StokesU, "U"),
            (ImagerPlaneSelection::StokesV, "V"),
            (ImagerPlaneSelection::CorrXX, "XX"),
            (ImagerPlaneSelection::CorrYY, "YY"),
            (ImagerPlaneSelection::CorrRR, "RR"),
            (ImagerPlaneSelection::CorrLL, "LL"),
        ];
        for (selection, text) in cases {
            assert_eq!(selection.as_cli_text(), text);
            assert_eq!(
                ImagerRunTaskRequest::plane_from_text(text).unwrap(),
                selection
            );
        }
        assert!(ImagerRunTaskRequest::plane_from_text("XY").is_err());

        let all_reasons = [
            CleanStopReason::GlobalThresholdReached,
            CleanStopReason::NsigmaThresholdReached,
            CleanStopReason::CycleThresholdReached,
            CleanStopReason::IterationLimitReached,
            CleanStopReason::NoCleanablePixels,
            CleanStopReason::DivergenceDetected,
        ];
        for reason in all_reasons {
            let stable: ImagerCleanStopReason = reason.into();
            match (reason, stable) {
                (
                    CleanStopReason::GlobalThresholdReached,
                    ImagerCleanStopReason::GlobalThresholdReached,
                )
                | (
                    CleanStopReason::NsigmaThresholdReached,
                    ImagerCleanStopReason::NsigmaThresholdReached,
                )
                | (
                    CleanStopReason::CycleThresholdReached,
                    ImagerCleanStopReason::CycleThresholdReached,
                )
                | (
                    CleanStopReason::IterationLimitReached,
                    ImagerCleanStopReason::IterationLimitReached,
                )
                | (CleanStopReason::NoCleanablePixels, ImagerCleanStopReason::NoCleanablePixels)
                | (
                    CleanStopReason::DivergenceDetected,
                    ImagerCleanStopReason::DivergenceDetected,
                ) => {}
                other => panic!("unexpected stop-reason mapping {other:?}"),
            }
        }

        assert_eq!(default_frequency_ref(), "LSRK");
        assert_eq!(default_doppler_ref(), "RADIO");
        assert_eq!(default_nterms(), 1);
        assert_eq!(default_gain(), 0.1);
        assert_eq!(default_psf_cutoff(), 0.35);
        assert_eq!(default_minor_cycle_length(), 8);
        assert_eq!(default_cyclefactor(), 1.0);
        assert_eq!(default_min_psf_fraction(), 0.1);
        assert_eq!(default_max_psf_fraction(), 0.8);
        assert!(default_write_preview_pngs());
    }

    #[test]
    fn cube_axis_values_and_config_validate_runtime_inputs() {
        let config = ImagerCubeAxisConfig {
            outframe: "BARY".to_string(),
            veltype: "optical".to_string(),
            interpolation: super::ImagerCubeInterpolation::Nearest,
            rest_frequency_hz: Some(1.42e9),
            start: Some(ImagerCubeAxisValue::FrequencyHz {
                hz: 1.1e9,
                frame: Some("LSRK".to_string()),
            }),
            width: Some(ImagerCubeAxisValue::VelocityMs {
                ms: 123.0,
                frame: Some("BARY".to_string()),
            }),
        };
        let runtime = config.clone().into_runtime(SpectralMode::Cube).unwrap();
        assert_eq!(runtime.outframe.to_string(), "BARY");
        assert_eq!(runtime.veltype.to_string(), "Z");
        assert_eq!(runtime.rest_frequency_hz, Some(1.42e9));
        assert!(matches!(
            runtime.start,
            Some(casa_ms::CubeAxisValue::FrequencyHz { hz, .. }) if (hz - 1.1e9).abs() < 1.0
        ));
        assert!(matches!(
            runtime.width,
            Some(casa_ms::CubeAxisValue::VelocityMs { ms, .. }) if (ms - 123.0).abs() < 1e-6
        ));

        let bad_config = ImagerCubeAxisConfig {
            outframe: "INVALID".to_string(),
            ..Default::default()
        };
        assert!(bad_config.into_runtime(SpectralMode::Cube).is_err());

        let bad_value = ImagerCubeAxisValue::Doppler {
            value: 0.1,
            convention: "not-a-convention".to_string(),
        };
        assert!(bad_value.into_runtime().is_err());
    }

    #[test]
    fn to_cli_config_rejects_invalid_request_combinations() {
        let both_phase_centers = ImagerRunTaskRequest {
            phasecenter_field: Some(1),
            phasecenter: Some("J2000 00:00:00 00.00.00".to_string()),
            ..default_request()
        };
        assert!(
            both_phase_centers
                .to_cli_config()
                .unwrap_err()
                .contains("mutually exclusive")
        );

        let mtmfs_cube = ImagerRunTaskRequest {
            spectral_mode: super::ImagerSpectralMode::Cube,
            deconvolver: ImagerDeconvolver::Mtmfs,
            ..default_request()
        };
        assert!(
            mtmfs_cube
                .to_cli_config()
                .unwrap_err()
                .contains("requires specmode='mfs'")
        );

        let nterms_without_mtmfs = ImagerRunTaskRequest {
            nterms: 2,
            ..default_request()
        };
        assert!(
            nterms_without_mtmfs
                .to_cli_config()
                .unwrap_err()
                .contains("nterms > 1")
        );

        let zero_nterms = ImagerRunTaskRequest {
            nterms: 0,
            ..default_request()
        };
        assert!(
            zero_nterms
                .to_cli_config()
                .unwrap_err()
                .contains("nterms > 1")
        );

        let invalid_scale = ImagerRunTaskRequest {
            multiscale_scales: vec![f32::NAN],
            ..default_request()
        };
        assert!(
            invalid_scale
                .to_cli_config()
                .unwrap_err()
                .contains("invalid multiscale scale")
        );
    }

    #[test]
    fn task_request_read_and_artifact_generation_cover_file_and_layout_branches() {
        let dir = TempDir::new().expect("tempdir");
        let base = dir.path().join("demo");
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
            fs::write(dir.path().join(format!("demo.{suffix}")), []).unwrap();
        }

        let request = ImagerRunTaskRequest {
            measurement_set: PathBuf::from("demo.ms"),
            image_name: base.clone(),
            deconvolver: ImagerDeconvolver::Mtmfs,
            nterms: 2,
            ..default_request()
        };
        let task_request = ImagerTaskRequest::Run(request.clone());
        let path = dir.path().join("request.json");
        fs::write(&path, serde_json::to_string(&task_request).unwrap()).unwrap();

        let decoded = ImagerTaskRequest::read_from_source(path.to_str().unwrap()).unwrap();
        assert_eq!(decoded, task_request);

        let missing_error =
            ImagerTaskRequest::read_from_source(dir.path().join("missing.json").to_str().unwrap())
                .unwrap_err();
        assert!(missing_error.contains("failed to read JSON request"));

        let result = super::ImagerRunTaskResult::from_run(request, &sample_run_summary());
        assert_eq!(result.run.stage_timings.total_ns, 110);
        assert_eq!(result.run.stage_timings.beam_fit_ns, 101);
        assert_eq!(result.run.frontend_timings.total_ns, 77);
        assert_eq!(result.run.channels.len(), 1);
        assert!(result.run.channels[0].beam_fit_available);
        assert_eq!(
            result.run.clean_stop_reason,
            Some(ImagerCleanStopReason::IterationLimitReached)
        );
        assert_eq!(result.artifacts.len(), 9);
        assert!(
            result
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == super::ImagerArtifactKind::Alpha
                    && artifact.preview_png_exists)
        );

        let no_preview_request = ImagerRunTaskRequest {
            image_name: dir.path().join("simple"),
            write_preview_pngs: false,
            ..default_request()
        };
        let no_preview_result =
            super::ImagerRunTaskResult::from_run(no_preview_request, &sample_run_summary());
        assert_eq!(no_preview_result.artifacts.len(), 4);
        assert!(no_preview_result
            .artifacts
            .iter()
            .all(|artifact| artifact.preview_png_path.is_none() && !artifact.preview_png_exists));
    }
}
