// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical imager task request/result contracts shared by CLI, shell, and Python.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_imaging::{
    CleanStopReason, Deconvolver, GaussianUvTaper, HogbomIterationMode, MinorCycleTrace,
    RestoringBeamMode, UvTaperSize, WTermMode, WeightingMode,
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
    AutoMultiThresholdConfig, ChannelRunSummary, CleanMaskMode, CliConfig, FrontendStageTimings,
    RunSummary, SaveModelMode, SpectralMode, command_schema, run_from_config,
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

/// CASA-style model persistence after imaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerSaveModel {
    /// Do not write a visibility model back to the MeasurementSet.
    #[default]
    None,
    /// Predict the final MFS model image into MAIN.MODEL_DATA.
    ModelColumn,
}

impl From<SaveModelMode> for ImagerSaveModel {
    fn from(value: SaveModelMode) -> Self {
        match value {
            SaveModelMode::None => Self::None,
            SaveModelMode::ModelColumn => Self::ModelColumn,
        }
    }
}

impl From<ImagerSaveModel> for SaveModelMode {
    fn from(value: ImagerSaveModel) -> Self {
        match value {
            ImagerSaveModel::None => Self::None,
            ImagerSaveModel::ModelColumn => Self::ModelColumn,
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
    /// CASA Briggs bandwidth taper weighting.
    BriggsBwTaper {
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
            WeightingMode::BriggsBwTaper { robust } => Self::BriggsBwTaper { robust },
        }
    }
}

impl From<ImagerWeighting> for WeightingMode {
    fn from(value: ImagerWeighting) -> Self {
        match value {
            ImagerWeighting::Natural => Self::Natural,
            ImagerWeighting::Uniform => Self::Uniform,
            ImagerWeighting::Briggs { robust } => Self::Briggs { robust },
            ImagerWeighting::BriggsBwTaper { robust } => Self::BriggsBwTaper { robust },
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

/// Hogbom minor-cycle iteration accounting policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagerHogbomIterationMode {
    /// Treat `niter` and `cycleniter` as strict caps on committed components.
    #[default]
    Strict,
    /// Mirror CASA's inclusive `hclean` iteration loop for parity checks.
    CasaInclusive,
}

impl From<HogbomIterationMode> for ImagerHogbomIterationMode {
    fn from(value: HogbomIterationMode) -> Self {
        match value {
            HogbomIterationMode::Strict => Self::Strict,
            HogbomIterationMode::CasaInclusive => Self::CasaInclusive,
        }
    }
}

impl From<ImagerHogbomIterationMode> for HogbomIterationMode {
    fn from(value: ImagerHogbomIterationMode) -> Self {
        match value {
            ImagerHogbomIterationMode::Strict => Self::Strict,
            ImagerHogbomIterationMode::CasaInclusive => Self::CasaInclusive,
        }
    }
}

/// CASA-style clean-mask generation mode for the task protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ImagerCleanMaskMode {
    /// Use only explicit user mask boxes or a mask image.
    #[default]
    User,
    /// Generate a mask using CASA's `auto-multithresh` control family.
    AutoMultithresh,
}

impl From<CleanMaskMode> for ImagerCleanMaskMode {
    fn from(value: CleanMaskMode) -> Self {
        match value {
            CleanMaskMode::User => Self::User,
            CleanMaskMode::AutoMultiThreshold => Self::AutoMultithresh,
        }
    }
}

impl From<ImagerCleanMaskMode> for CleanMaskMode {
    fn from(value: ImagerCleanMaskMode) -> Self {
        match value {
            ImagerCleanMaskMode::User => Self::User,
            ImagerCleanMaskMode::AutoMultithresh => Self::AutoMultiThreshold,
        }
    }
}

/// CASA `auto-multithresh` controls for the task protocol.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerAutoMultiThresholdConfig {
    /// Sidelobe threshold factor multiplied by the PSF sidelobe level.
    #[serde(default = "default_auto_sidelobe_threshold")]
    pub sidelobe_threshold: f32,
    /// Noise threshold factor multiplied by the robust residual RMS.
    #[serde(default = "default_auto_noise_threshold")]
    pub noise_threshold: f32,
    /// Lower noise threshold factor used when growing a mask.
    #[serde(default = "default_auto_low_noise_threshold")]
    pub low_noise_threshold: f32,
    /// Negative-feature threshold factor; zero disables negative masks.
    #[serde(default)]
    pub negative_threshold: f32,
    /// Smoothing factor for CASA's beam-scaled mask smoothing stage.
    #[serde(default = "default_auto_smooth_factor")]
    pub smooth_factor: f32,
    /// Minimum region size as a fraction of the fitted beam area.
    #[serde(default = "default_auto_min_beam_frac")]
    pub min_beam_frac: f32,
    /// Fraction of the smoothed mask peak used to cut mask edges.
    #[serde(default = "default_auto_cut_threshold")]
    pub cut_threshold: f32,
    /// Maximum constrained binary-dilation iterations for mask growth.
    #[serde(default = "default_auto_grow_iterations")]
    pub grow_iterations: usize,
    /// Whether grown masks are pruned after dilation.
    #[serde(default = "default_true")]
    pub do_grow_prune: bool,
    /// CASA percent-change stop control for later automask updates.
    #[serde(default = "default_auto_min_percent_change")]
    pub min_percent_change: f32,
    /// Use CASA's fast-noise statistics path.
    #[serde(default = "default_true")]
    pub fast_noise: bool,
}

impl Default for ImagerAutoMultiThresholdConfig {
    fn default() -> Self {
        AutoMultiThresholdConfig::default().into()
    }
}

impl From<AutoMultiThresholdConfig> for ImagerAutoMultiThresholdConfig {
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

impl From<ImagerAutoMultiThresholdConfig> for AutoMultiThresholdConfig {
    fn from(value: ImagerAutoMultiThresholdConfig) -> Self {
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

fn default_auto_sidelobe_threshold() -> f32 {
    AutoMultiThresholdConfig::default().sidelobe_threshold
}

fn default_auto_noise_threshold() -> f32 {
    AutoMultiThresholdConfig::default().noise_threshold
}

fn default_auto_low_noise_threshold() -> f32 {
    AutoMultiThresholdConfig::default().low_noise_threshold
}

fn default_auto_smooth_factor() -> f32 {
    AutoMultiThresholdConfig::default().smooth_factor
}

fn default_auto_min_beam_frac() -> f32 {
    AutoMultiThresholdConfig::default().min_beam_frac
}

fn default_auto_cut_threshold() -> f32 {
    AutoMultiThresholdConfig::default().cut_threshold
}

fn default_auto_grow_iterations() -> usize {
    AutoMultiThresholdConfig::default().grow_iterations
}

fn default_auto_min_percent_change() -> f32 {
    AutoMultiThresholdConfig::default().min_percent_change
}

fn default_true() -> bool {
    true
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
#[serde(deny_unknown_fields)]
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
    /// Model persistence mode.
    #[serde(default)]
    pub save_model: ImagerSaveModel,
    /// Optional CASA image used to seed the initial model product.
    #[serde(default)]
    pub start_model: Option<PathBuf>,
    /// Optional CASA outlier-field definition file.
    #[serde(default)]
    pub outlier_file: Option<PathBuf>,
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
    /// CASA-style `usepointing` toggle for POINTING-table direction corrections.
    #[serde(default)]
    pub use_pointing: bool,
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
    /// CASA-style major-cycle limit. `None` corresponds to CASA `nmajor=-1`.
    #[serde(default)]
    pub nmajor: Option<usize>,
    /// Include long-form CASA-compatible `summaryminor` fields.
    #[serde(default)]
    pub fullsummary: bool,
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
    /// Mosaic primary-beam cutoff used for flat-noise normalization.
    #[serde(default = "default_mosaic_pb_limit")]
    pub mosaic_pb_limit: f32,
    /// Write CASA-style PB-corrected mosaic image products.
    #[serde(default)]
    pub pbcor: bool,
    /// Write the primary-beam image used for PB correction.
    #[serde(default)]
    pub write_pb: bool,
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
    /// Hogbom minor-cycle iteration accounting policy.
    #[serde(default)]
    pub hogbom_iteration_mode: ImagerHogbomIterationMode,
    /// CASA-style clean mask mode.
    #[serde(default)]
    pub use_mask: ImagerCleanMaskMode,
    /// CASA-style `auto-multithresh` controls.
    #[serde(default)]
    pub auto_mask: ImagerAutoMultiThresholdConfig,
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
            save_model: config.save_model.into(),
            start_model: config.start_model.clone(),
            outlier_file: config.outlier_file.clone(),
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
            use_pointing: config.use_pointing,
            uv_taper: config.uv_taper.map(Into::into),
            restoring_beam_mode: config.restoring_beam_mode.into(),
            deconvolver: config.deconvolver.into(),
            nterms: config.nterms,
            multiscale_scales: config.multiscale_scales.clone(),
            small_scale_bias: config.small_scale_bias,
            niter: config.niter,
            nmajor: config.nmajor,
            fullsummary: config.fullsummary,
            gain: config.gain,
            threshold_jy: config.threshold_jy,
            nsigma: config.nsigma,
            psf_cutoff: config.psf_cutoff,
            mosaic_pb_limit: config.mosaic_pb_limit,
            pbcor: config.pbcor,
            write_pb: config.write_pb,
            minor_cycle_length: config.minor_cycle_length,
            cyclefactor: config.cyclefactor,
            min_psf_fraction: config.min_psf_fraction,
            max_psf_fraction: config.max_psf_fraction,
            hogbom_iteration_mode: config.hogbom_iteration_mode.into(),
            use_mask: config.use_mask.into(),
            auto_mask: config.auto_mask.into(),
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
        if self.start_model.is_some() {
            if spectral_mode != SpectralMode::Mfs {
                return Err("start_model currently supports only spectral_mode='mfs'".to_string());
            }
            if deconvolver == Deconvolver::Mtmfs {
                return Err(
                    "start_model currently supports only single-term deconvolvers".to_string(),
                );
            }
        }
        if !(self.mosaic_pb_limit.is_finite() && self.mosaic_pb_limit != 0.0) {
            return Err("mosaic_pb_limit must be finite and non-zero".to_string());
        }
        for scale in &self.multiscale_scales {
            if !(scale.is_finite() && *scale >= 0.0) {
                return Err(format!(
                    "invalid multiscale scale {scale}; expected finite value >= 0"
                ));
            }
        }
        if self.use_mask == ImagerCleanMaskMode::AutoMultithresh {
            for (name, value) in [
                ("sidelobe_threshold", self.auto_mask.sidelobe_threshold),
                ("noise_threshold", self.auto_mask.noise_threshold),
                ("low_noise_threshold", self.auto_mask.low_noise_threshold),
                ("negative_threshold", self.auto_mask.negative_threshold),
                ("smooth_factor", self.auto_mask.smooth_factor),
                ("min_beam_frac", self.auto_mask.min_beam_frac),
                ("cut_threshold", self.auto_mask.cut_threshold),
                ("min_percent_change", self.auto_mask.min_percent_change),
            ] {
                if !value.is_finite() {
                    return Err(format!("{name} must be finite"));
                }
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
            save_model: self.save_model.into(),
            start_model: self.start_model.clone(),
            outlier_file: self.outlier_file.clone(),
            correlation: self
                .correlation
                .map(|value| value.as_cli_text().to_string()),
            spectral_mode,
            cube_axis: self.cube_axis.clone().into_runtime(spectral_mode)?,
            weighting: self.weighting.clone().into(),
            per_channel_weight_density: self.per_channel_weight_density,
            use_pointing: self.use_pointing,
            uv_taper: self.uv_taper.map(Into::into),
            restoring_beam_mode: self.restoring_beam_mode.into(),
            deconvolver,
            nterms: self.nterms,
            multiscale_scales: self.multiscale_scales.clone(),
            small_scale_bias: self.small_scale_bias,
            niter: self.niter,
            nmajor: self.nmajor,
            fullsummary: self.fullsummary,
            gain: self.gain,
            threshold_jy: self.threshold_jy,
            nsigma: self.nsigma,
            psf_cutoff: self.psf_cutoff,
            mosaic_pb_limit: self.mosaic_pb_limit,
            pbcor: self.pbcor,
            write_pb: self.write_pb,
            minor_cycle_length: self.minor_cycle_length,
            cyclefactor: self.cyclefactor,
            min_psf_fraction: self.min_psf_fraction,
            max_psf_fraction: self.max_psf_fraction,
            hogbom_iteration_mode: self.hogbom_iteration_mode.into(),
            use_mask: self.use_mask.into(),
            auto_mask: self.auto_mask.into(),
            mask_boxes: self.mask_boxes.clone(),
            mask_image: self.mask_image.clone(),
            w_term_mode: self.w_term_mode.into(),
            force_standard_gridder: false,
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
    /// The requested major-cycle budget was exhausted.
    MajorCycleLimitReached,
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
            CleanStopReason::MajorCycleLimitReached => Self::MajorCycleLimitReached,
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

/// One CASA-compatible minor-cycle summary row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerMinorCycleSummary {
    /// Zero-based output channel index. MFS reports channel 0.
    pub channel_index: usize,
    /// Zero-based Stokes index. Current scalar-plane runs report Stokes 0.
    pub stokes_index: usize,
    /// Zero-based minor-cycle block index within this channel.
    pub cycle_index: usize,
    /// CASA `summaryminor.iterDone`: reported iterations consumed by this block.
    pub iter_done: usize,
    /// CASA `summaryminor.peakRes`: peak residual after this block.
    pub peak_res_jy_per_beam: f32,
    /// CASA `summaryminor.modelFlux`: model flux after this block.
    pub model_flux_jy: f32,
    /// CASA `summaryminor.cycleThresh`: cycle threshold for this block.
    pub cycle_threshold_jy_per_beam: f32,
    /// CASA deconvolver id. Current task reports a single deconvolver as 0.
    pub deconvolver_id: usize,
    /// CASA `summaryminor.cycleStartIter`, present when `fullsummary=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle_start_iter: Option<usize>,
    /// CASA `summaryminor.startIterDone`, present when `fullsummary=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_iter_done: Option<usize>,
    /// CASA `summaryminor.startPeakRes`, present when `fullsummary=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_peak_res_jy_per_beam: Option<f32>,
    /// CASA `summaryminor.peakResNM`, present when `fullsummary=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_res_no_mask_jy_per_beam: Option<f32>,
    /// CASA `summaryminor.stopCode`, present when `fullsummary=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_code: Option<i32>,
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
    /// Total minor-cycle component updates executed by the run.
    pub minor_iterations: usize,
    /// CASA-compatible `iterdone` task-return value.
    pub iterdone: usize,
    /// CASA-compatible `nmajordone` task-return value.
    pub nmajordone: usize,
    /// CASA-compatible `stopcode` task-return value.
    pub stopcode: i32,
    /// Final CLEAN stop reason when deconvolution ran.
    pub clean_stop_reason: Option<ImagerCleanStopReason>,
    /// CASA-compatible minor-cycle summary rows.
    pub summaryminor: Vec<ImagerMinorCycleSummary>,
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
    /// Clean mask image.
    Mask,
    /// Mosaic weight/sensitivity image.
    Weight,
    /// Mosaic primary-beam image.
    PrimaryBeam,
    /// Primary-beam-corrected restored image.
    ImagePbcor,
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
            Self::Mask => "mask",
            Self::Weight => "weight",
            Self::PrimaryBeam => "pb",
            Self::ImagePbcor => "image.pbcor",
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
                iterdone: summary.minor_iterations,
                nmajordone: summary.major_cycles,
                stopcode: casa_stop_code(summary.clean_stop_reason),
                clean_stop_reason: summary.clean_stop_reason.map(Into::into),
                summaryminor: build_summaryminor(summary, request.fullsummary),
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

fn default_mosaic_pb_limit() -> f32 {
    0.2
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

fn casa_stop_code(reason: Option<CleanStopReason>) -> i32 {
    match reason {
        Some(CleanStopReason::IterationLimitReached) => 1,
        Some(CleanStopReason::GlobalThresholdReached) => 2,
        Some(CleanStopReason::NsigmaThresholdReached) => 2,
        Some(CleanStopReason::CycleThresholdReached) => 3,
        Some(CleanStopReason::NoCleanablePixels) => 7,
        Some(CleanStopReason::MajorCycleLimitReached) => 9,
        Some(CleanStopReason::DivergenceDetected) => 10,
        None => 0,
    }
}

fn build_summaryminor(summary: &RunSummary, fullsummary: bool) -> Vec<ImagerMinorCycleSummary> {
    if summary.channel_summaries.is_empty() {
        return summary
            .minor_cycle_traces
            .iter()
            .map(|trace| minor_cycle_summary(0, 0, trace, fullsummary))
            .collect();
    }
    summary
        .channel_summaries
        .iter()
        .flat_map(|channel| {
            channel
                .minor_cycle_traces
                .iter()
                .map(move |trace| minor_cycle_summary(channel.channel_index, 0, trace, fullsummary))
        })
        .collect()
}

fn minor_cycle_summary(
    channel_index: usize,
    stokes_index: usize,
    trace: &MinorCycleTrace,
    fullsummary: bool,
) -> ImagerMinorCycleSummary {
    ImagerMinorCycleSummary {
        channel_index,
        stokes_index,
        cycle_index: trace.cycle_index,
        iter_done: trace.reported_updates,
        peak_res_jy_per_beam: trace.end_peak_residual_jy_per_beam,
        model_flux_jy: trace.model_flux_jy,
        cycle_threshold_jy_per_beam: trace.cycle_threshold_jy_per_beam,
        deconvolver_id: 0,
        cycle_start_iter: fullsummary.then_some(trace.start_reported_iteration),
        start_iter_done: fullsummary.then_some(trace.start_reported_iteration),
        start_peak_res_jy_per_beam: fullsummary.then_some(trace.start_peak_residual_jy_per_beam),
        peak_res_no_mask_jy_per_beam: fullsummary.then_some(trace.end_peak_residual_jy_per_beam),
        stop_code: fullsummary.then_some(casa_stop_code(trace.clean_stop_reason)),
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
            if request.write_pb || request.pbcor {
                for term in 0..request.nterms {
                    let suffix = format!("pb.tt{term}");
                    let preview = (term == 0 && request.write_preview_pngs)
                        .then(|| PathBuf::from(format!("{base}.{suffix}.png")));
                    artifacts.push(artifact(
                        ImagerArtifactKind::PrimaryBeam,
                        format!("Primary Beam tt{term}"),
                        PathBuf::from(format!("{base}.{suffix}")),
                        preview,
                    ));
                }
            }
        }
        _ => {
            for (kind, label) in [
                (ImagerArtifactKind::Psf, "PSF"),
                (ImagerArtifactKind::Residual, "Residual"),
                (ImagerArtifactKind::Model, "Model"),
                (ImagerArtifactKind::Image, "Restored Image"),
                (ImagerArtifactKind::Mask, "Clean Mask"),
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
            if request.write_pb || request.pbcor {
                let suffix = ImagerArtifactKind::PrimaryBeam.as_suffix();
                let preview = request
                    .write_preview_pngs
                    .then(|| PathBuf::from(format!("{base}.{suffix}.png")));
                artifacts.push(artifact(
                    ImagerArtifactKind::PrimaryBeam,
                    "Primary Beam".to_string(),
                    PathBuf::from(format!("{base}.{suffix}")),
                    preview,
                ));
            }
            if request.pbcor {
                let kind = ImagerArtifactKind::ImagePbcor;
                let suffix = kind.as_suffix();
                let preview = request
                    .write_preview_pngs
                    .then(|| PathBuf::from(format!("{base}.{suffix}.png")));
                artifacts.push(artifact(
                    kind,
                    "PB-corrected Image".to_string(),
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
    use std::path::{Path, PathBuf};

    use casa_imaging::{
        CleanStopReason, Deconvolver, GaussianUvTaper, RestoringBeamMode, UvTaperSize, WTermMode,
        WeightingMode,
    };
    use casa_ms::{CubeAxisConfig, CubeAxisValue, CubeInterpolation};
    use casa_provider_contracts::ProviderSurfaceKind;
    use casa_types::measures::doppler::DopplerRef;
    use casa_types::measures::frequency::FrequencyRef;
    use tempfile::{NamedTempFile, tempdir};

    use super::{
        IMAGER_TASK_PROTOCOL_NAME, IMAGER_TASK_PROTOCOL_VERSION, ImagerArtifactKind,
        ImagerAutoMultiThresholdConfig, ImagerCleanMaskMode, ImagerCleanStopReason,
        ImagerCubeAxisConfig, ImagerCubeAxisValue, ImagerCubeInterpolation, ImagerDeconvolver,
        ImagerHogbomIterationMode, ImagerPlaneSelection, ImagerRestoringBeamMode,
        ImagerRunTaskRequest, ImagerSaveModel, ImagerSpectralMode, ImagerTaskRequest,
        ImagerTaskSchemaBundle, ImagerUvTaper, ImagerUvTaperSize, ImagerWTermMode, ImagerWeighting,
    };
    use crate::{CliConfig, SaveModelMode, SpectralMode};

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
            OsString::from("--savemodel"),
            OsString::from("modelcolumn"),
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
            OsString::from("--usepointing"),
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
            OsString::from("--usemask"),
            OsString::from("auto-multithresh"),
            OsString::from("--sidelobethreshold"),
            OsString::from("2.0"),
            OsString::from("--noisethreshold"),
            OsString::from("4.25"),
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
        assert_eq!(restored.save_model, SaveModelMode::ModelColumn);
        assert_eq!(restored.correlation.as_deref(), Some("XX"));
        assert_eq!(restored.spectral_mode, SpectralMode::Cube);
        assert_eq!(restored.weighting, WeightingMode::Briggs { robust: -1.0 });
        assert!(restored.per_channel_weight_density);
        assert!(restored.use_pointing);
        assert_eq!(restored.restoring_beam_mode, RestoringBeamMode::Common);
        assert_eq!(restored.deconvolver, Deconvolver::Multiscale);
        assert_eq!(restored.use_mask, crate::CleanMaskMode::AutoMultiThreshold);
        assert_eq!(restored.auto_mask.sidelobe_threshold, 2.0);
        assert_eq!(restored.auto_mask.noise_threshold, 4.25);
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
            save_model: ImagerSaveModel::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: Default::default(),
            cube_axis: Default::default(),
            weighting: Default::default(),
            per_channel_weight_density: false,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: Default::default(),
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.2,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: ImagerHogbomIterationMode::Strict,
            use_mask: ImagerCleanMaskMode::User,
            auto_mask: ImagerAutoMultiThresholdConfig::default(),
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
        assert!(!config.use_pointing);
    }

    #[test]
    fn briggs_weighting_round_trips() {
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
            save_model: ImagerSaveModel::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: Default::default(),
            cube_axis: Default::default(),
            weighting: ImagerWeighting::Briggs { robust: 0.5 },
            per_channel_weight_density: false,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: Default::default(),
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.1,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: ImagerHogbomIterationMode::Strict,
            use_mask: ImagerCleanMaskMode::User,
            auto_mask: ImagerAutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: Default::default(),
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        };
        let config = request.to_cli_config().unwrap();
        assert_eq!(config.weighting, WeightingMode::Briggs { robust: 0.5 });
    }

    #[test]
    fn briggs_bandwidth_taper_weighting_round_trips() {
        assert_eq!(
            ImagerWeighting::from(WeightingMode::BriggsBwTaper { robust: 0.25 }),
            ImagerWeighting::BriggsBwTaper { robust: 0.25 }
        );
        assert_eq!(
            WeightingMode::from(ImagerWeighting::BriggsBwTaper { robust: 0.25 }),
            WeightingMode::BriggsBwTaper { robust: 0.25 }
        );
    }

    #[test]
    fn plane_selection_and_enum_conversions_cover_public_variants() {
        for (text, plane) in [
            ("I", ImagerPlaneSelection::StokesI),
            ("Q", ImagerPlaneSelection::StokesQ),
            ("U", ImagerPlaneSelection::StokesU),
            ("V", ImagerPlaneSelection::StokesV),
            ("XX", ImagerPlaneSelection::CorrXX),
            ("YY", ImagerPlaneSelection::CorrYY),
            ("RR", ImagerPlaneSelection::CorrRR),
            ("LL", ImagerPlaneSelection::CorrLL),
        ] {
            assert_eq!(ImagerRunTaskRequest::plane_from_text(text).unwrap(), plane);
            assert_eq!(plane.as_cli_text(), text);
        }
        assert!(ImagerRunTaskRequest::plane_from_text("XY").is_err());

        assert_eq!(
            ImagerSpectralMode::from(SpectralMode::Cubedata),
            ImagerSpectralMode::Cubedata
        );
        assert_eq!(
            SpectralMode::from(ImagerSpectralMode::Cube),
            SpectralMode::Cube
        );
        assert_eq!(
            ImagerWeighting::from(WeightingMode::Uniform),
            ImagerWeighting::Uniform
        );
        assert_eq!(
            WeightingMode::from(ImagerWeighting::Briggs { robust: 1.0 }),
            WeightingMode::Briggs { robust: 1.0 }
        );
        assert_eq!(
            WeightingMode::from(ImagerWeighting::BriggsBwTaper { robust: 1.0 }),
            WeightingMode::BriggsBwTaper { robust: 1.0 }
        );
        assert_eq!(
            ImagerRestoringBeamMode::from(RestoringBeamMode::Common),
            ImagerRestoringBeamMode::Common
        );
        assert_eq!(
            RestoringBeamMode::from(ImagerRestoringBeamMode::PerPlane),
            RestoringBeamMode::PerPlane
        );
        assert_eq!(
            ImagerDeconvolver::from(Deconvolver::Clark),
            ImagerDeconvolver::Clark
        );
        assert_eq!(
            Deconvolver::from(ImagerDeconvolver::Multiscale),
            Deconvolver::Multiscale
        );
        assert_eq!(
            ImagerWTermMode::from(WTermMode::WProject),
            ImagerWTermMode::Wproject
        );
        assert_eq!(WTermMode::from(ImagerWTermMode::Direct), WTermMode::Direct);
        assert_eq!(
            ImagerCubeInterpolation::from(CubeInterpolation::Cubic),
            ImagerCubeInterpolation::Linear
        );
        assert_eq!(
            CubeInterpolation::from(ImagerCubeInterpolation::Nearest),
            CubeInterpolation::Nearest
        );
        assert_eq!(
            ImagerUvTaperSize::from(UvTaperSize::ImageFwhmRad(1.5)),
            ImagerUvTaperSize::ImageFwhmRad { value: 1.5 }
        );
        assert_eq!(
            UvTaperSize::from(ImagerUvTaperSize::BaselineHwhmLambda { value: 3.0 }),
            UvTaperSize::BaselineHwhmLambda(3.0)
        );
        assert_eq!(
            ImagerUvTaper::from(GaussianUvTaper {
                major: UvTaperSize::ImageFwhmRad(2.0),
                minor: UvTaperSize::BaselineHwhmLambda(4.0),
                position_angle_rad: 0.25,
            }),
            ImagerUvTaper {
                major: ImagerUvTaperSize::ImageFwhmRad { value: 2.0 },
                minor: ImagerUvTaperSize::BaselineHwhmLambda { value: 4.0 },
                position_angle_rad: 0.25,
            }
        );
        assert_eq!(
            ImagerCleanStopReason::from(CleanStopReason::DivergenceDetected),
            ImagerCleanStopReason::DivergenceDetected
        );
    }

    #[test]
    fn cube_axis_values_and_config_validate_runtime_inputs() {
        assert_eq!(
            ImagerCubeAxisValue::from(&CubeAxisValue::Channel(7))
                .into_runtime()
                .unwrap(),
            CubeAxisValue::Channel(7)
        );
        assert_eq!(
            ImagerCubeAxisValue::from(&CubeAxisValue::FrequencyHz {
                hz: 1.4e9,
                frame: Some(FrequencyRef::BARY),
            })
            .into_runtime()
            .unwrap(),
            CubeAxisValue::FrequencyHz {
                hz: 1.4e9,
                frame: Some(FrequencyRef::BARY),
            }
        );
        assert_eq!(
            ImagerCubeAxisValue::from(&CubeAxisValue::VelocityMs {
                ms: 12.0,
                frame: Some(FrequencyRef::LSRK),
            })
            .into_runtime()
            .unwrap(),
            CubeAxisValue::VelocityMs {
                ms: 12.0,
                frame: Some(FrequencyRef::LSRK),
            }
        );
        assert_eq!(
            ImagerCubeAxisValue::from(&CubeAxisValue::Doppler {
                value: 0.25,
                convention: DopplerRef::Z,
            })
            .into_runtime()
            .unwrap(),
            CubeAxisValue::Doppler {
                value: 0.25,
                convention: DopplerRef::Z,
            }
        );

        let config = ImagerCubeAxisConfig::from(&CubeAxisConfig {
            specmode: SpectralMode::Cube.cube_specmode(),
            outframe: FrequencyRef::BARY,
            veltype: DopplerRef::Z,
            interpolation: CubeInterpolation::Nearest,
            rest_frequency_hz: Some(1.42e9),
            start: Some(CubeAxisValue::Channel(3)),
            width: Some(CubeAxisValue::FrequencyHz {
                hz: 1.0e6,
                frame: Some(FrequencyRef::LSRK),
            }),
        });
        let runtime = config.clone().into_runtime(SpectralMode::Cube).unwrap();
        assert_eq!(runtime.outframe, FrequencyRef::BARY);
        assert_eq!(runtime.veltype, DopplerRef::Z);
        assert_eq!(runtime.interpolation, CubeInterpolation::Nearest);
        assert_eq!(runtime.rest_frequency_hz, Some(1.42e9));

        let invalid_frame = ImagerCubeAxisConfig {
            outframe: "bad-frame".to_string(),
            ..config.clone()
        };
        assert!(invalid_frame.into_runtime(SpectralMode::Cube).is_err());

        let invalid_veltype = ImagerCubeAxisConfig {
            veltype: "bad-doppler".to_string(),
            ..config.clone()
        };
        assert!(invalid_veltype.into_runtime(SpectralMode::Cube).is_err());

        let invalid_restfreq = ImagerCubeAxisConfig {
            rest_frequency_hz: Some(f64::NAN),
            ..config
        };
        assert!(invalid_restfreq.into_runtime(SpectralMode::Cube).is_err());
    }

    #[test]
    fn to_cli_config_rejects_invalid_request_combinations() {
        let base = ImagerRunTaskRequest {
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
            save_model: ImagerSaveModel::None,
            start_model: None,
            outlier_file: None,
            correlation: None,
            spectral_mode: ImagerSpectralMode::Mfs,
            cube_axis: Default::default(),
            weighting: Default::default(),
            per_channel_weight_density: false,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: Default::default(),
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.1,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: ImagerHogbomIterationMode::Strict,
            use_mask: ImagerCleanMaskMode::User,
            auto_mask: ImagerAutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: Default::default(),
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        };

        assert!(
            base.clone()
                .to_cli_config()
                .expect("base request converts")
                .write_preview_pngs
        );

        assert!(base.clone().to_cli_config().is_ok());
        assert!(
            base.clone()
                .to_cli_config()
                .expect("valid config")
                .correlation
                .is_none()
        );

        let conflict = ImagerRunTaskRequest {
            phasecenter_field: Some(1),
            phasecenter: Some("J2000 1rad 2rad".to_string()),
            ..base.clone()
        };
        assert!(
            conflict
                .to_cli_config()
                .unwrap_err()
                .contains("mutually exclusive")
        );

        let mtmfs_cube = ImagerRunTaskRequest {
            spectral_mode: ImagerSpectralMode::Cube,
            deconvolver: ImagerDeconvolver::Mtmfs,
            ..base.clone()
        };
        assert!(
            mtmfs_cube
                .to_cli_config()
                .unwrap_err()
                .contains("requires specmode='mfs'")
        );

        let non_mtmfs_nterms = ImagerRunTaskRequest {
            deconvolver: ImagerDeconvolver::Clark,
            nterms: 2,
            ..base.clone()
        };
        assert!(
            non_mtmfs_nterms
                .to_cli_config()
                .unwrap_err()
                .contains("nterms > 1")
        );

        let zero_nterms = ImagerRunTaskRequest {
            deconvolver: ImagerDeconvolver::Mtmfs,
            nterms: 0,
            ..base.clone()
        };
        assert!(
            zero_nterms
                .to_cli_config()
                .unwrap_err()
                .contains("at least 1")
        );

        let invalid_scales = ImagerRunTaskRequest {
            multiscale_scales: vec![1.0, -1.0],
            ..base
        };
        assert!(
            invalid_scales
                .to_cli_config()
                .unwrap_err()
                .contains("invalid multiscale scale")
        );
    }

    #[test]
    fn task_request_read_and_artifact_generation_cover_file_and_layout_branches() {
        let file = NamedTempFile::new().expect("temp request");
        fs::write(
            file.path(),
            r#"{"kind":"run","request":{"measurement_set":"demo.ms","image_name":"out/demo","image_size":64,"cell_arcsec":1.5}}"#,
        )
        .expect("write request");
        match ImagerTaskRequest::read_from_source(file.path().to_str().unwrap()).unwrap() {
            ImagerTaskRequest::Run(request) => {
                assert_eq!(request.measurement_set, PathBuf::from("demo.ms"));
                assert_eq!(request.image_name, PathBuf::from("out/demo"));
            }
        }
        assert!(
            ImagerTaskRequest::read_from_source("/definitely/missing/imager.json")
                .unwrap_err()
                .contains("failed to read JSON request")
        );
        let bad = NamedTempFile::new().expect("bad request");
        fs::write(bad.path(), "{not-json").expect("write bad request");
        assert!(
            ImagerTaskRequest::read_from_source(bad.path().to_str().unwrap())
                .unwrap_err()
                .contains("failed to parse imager task request")
        );

        let temp = tempdir().expect("artifact dir");
        let image_name = temp.path().join("artifact/demo");
        fs::create_dir_all(image_name.parent().unwrap()).expect("artifact parent");
        fs::write(image_name.with_extension("psf"), b"psf").expect("write psf");
        fs::write(image_name.with_extension("psf.png"), b"png").expect("write psf png");
        let standard = ImagerRunTaskRequest {
            measurement_set: PathBuf::from("demo.ms"),
            image_name: image_name.clone(),
            image_size: 64,
            cell_arcsec: 1.5,
            field_ids: None,
            phasecenter_field: None,
            phasecenter: None,
            ddid: None,
            spw_selector: Some("7".to_string()),
            channel_start: None,
            channel_count: None,
            data_column: None,
            save_model: ImagerSaveModel::None,
            start_model: None,
            outlier_file: None,
            correlation: Some(ImagerPlaneSelection::CorrXX),
            spectral_mode: ImagerSpectralMode::Mfs,
            cube_axis: Default::default(),
            weighting: Default::default(),
            per_channel_weight_density: false,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: Default::default(),
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.1,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.1,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: ImagerHogbomIterationMode::Strict,
            use_mask: ImagerCleanMaskMode::User,
            auto_mask: ImagerAutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: Default::default(),
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        };
        let standard_config = standard.to_cli_config().unwrap();
        assert_eq!(standard_config.spw, Some(7));
        assert_eq!(standard_config.correlation.as_deref(), Some("XX"));

        let standard_artifacts = super::build_artifacts(&standard);
        assert_eq!(standard_artifacts.len(), 5);
        assert_eq!(standard_artifacts[0].kind, ImagerArtifactKind::Psf);
        assert!(standard_artifacts[0].exists);
        assert!(standard_artifacts[0].preview_png_exists);
        assert!(
            standard_artifacts
                .iter()
                .any(|artifact| artifact.kind == ImagerArtifactKind::Mask
                    && artifact.label == "Clean Mask")
        );

        let mtmfs = ImagerRunTaskRequest {
            deconvolver: ImagerDeconvolver::Mtmfs,
            nterms: 2,
            write_preview_pngs: false,
            ..standard
        };
        let mtmfs_artifacts = super::build_artifacts(&mtmfs);
        assert_eq!(mtmfs_artifacts.len(), 9);
        assert_eq!(
            mtmfs_artifacts
                .iter()
                .filter(|artifact| artifact.kind == ImagerArtifactKind::Alpha)
                .count(),
            1
        );
        assert!(
            mtmfs_artifacts
                .iter()
                .all(|artifact| artifact.preview_png_path.is_none())
        );
    }

    #[test]
    fn dirty_imaging_json_request_accepts_gui_selection_fields() {
        let payload = r#"{
          "kind": "run",
          "request": {
            "measurement_set": "/data/probed.ms",
            "image_name": "/data/casa-rs-runs/probed-dirty",
            "image_size": 256,
            "cell_arcsec": 0.25,
            "field_ids": [0],
            "phasecenter_field": 0,
            "spw_selector": "0",
            "channel_start": 2,
            "channel_count": 4,
            "data_column": "DATA",
            "weighting": {
              "kind": "briggs",
              "robust": 0.5
            },
            "niter": 0,
            "dirty_only": true,
            "write_preview_pngs": true
          }
        }"#;
        let request: ImagerTaskRequest =
            serde_json::from_str(payload).expect("parse GUI dirty-imaging request");
        let ImagerTaskRequest::Run(request) = request;
        assert_eq!(request.measurement_set, PathBuf::from("/data/probed.ms"));
        assert_eq!(
            request.image_name,
            PathBuf::from("/data/casa-rs-runs/probed-dirty")
        );
        assert_eq!(request.field_ids, Some(vec![0]));
        assert_eq!(request.phasecenter_field, Some(0));
        assert_eq!(request.spw_selector.as_deref(), Some("0"));
        assert_eq!(request.channel_start, Some(2));
        assert_eq!(request.channel_count, Some(4));
        assert_eq!(request.data_column.as_deref(), Some("DATA"));
        assert_eq!(request.weighting, ImagerWeighting::Briggs { robust: 0.5 });
        assert_eq!(request.niter, 0);
        assert!(request.dirty_only);
        assert!(request.write_preview_pngs);

        let config = request.to_cli_config().expect("restore CLI config");
        assert_eq!(config.field_ids, Some(vec![0]));
        assert_eq!(config.phasecenter_field, Some(0));
        assert_eq!(config.spw_selector.as_deref(), Some("0"));
        assert_eq!(config.channel_start, Some(2));
        assert_eq!(config.channel_count, Some(4));
        assert_eq!(config.datacolumn.as_deref(), Some("DATA"));
        assert_eq!(config.weighting, WeightingMode::Briggs { robust: 0.5 });
        assert!(config.dirty_only);
    }

    #[test]
    fn task_request_roundtrips_start_model_path() {
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
            save_model: ImagerSaveModel::None,
            start_model: Some(PathBuf::from("seed.model")),
            outlier_file: Some(PathBuf::from("outliers.txt")),
            correlation: None,
            spectral_mode: ImagerSpectralMode::Mfs,
            cube_axis: Default::default(),
            weighting: Default::default(),
            per_channel_weight_density: false,
            use_pointing: false,
            uv_taper: None,
            restoring_beam_mode: Default::default(),
            deconvolver: ImagerDeconvolver::Hogbom,
            nterms: 1,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            niter: 0,
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.2,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 8,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: Default::default(),
            use_mask: Default::default(),
            auto_mask: Default::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: Default::default(),
            w_project_planes: None,
            dirty_only: false,
            write_preview_pngs: true,
        };

        let config = request.to_cli_config().expect("restore CLI config");
        assert_eq!(config.start_model.as_deref(), Some(Path::new("seed.model")));
        assert_eq!(
            config.outlier_file.as_deref(),
            Some(Path::new("outliers.txt"))
        );
        let restored = ImagerRunTaskRequest::from_cli_config(&config);
        assert_eq!(
            restored.start_model.as_deref(),
            Some(Path::new("seed.model"))
        );
        assert_eq!(
            restored.outlier_file.as_deref(),
            Some(Path::new("outliers.txt"))
        );
    }

    #[test]
    fn json_request_rejects_unsupported_tclean_controls_instead_of_ignoring_them() {
        let payload = r#"{
          "kind": "run",
          "request": {
            "measurement_set": "/data/probed.ms",
            "image_name": "/data/casa-rs-runs/probed-dirty",
            "image_size": 256,
            "cell_arcsec": 0.25,
            "startmodel": "/data/seed.model",
            "outlierfile": "/data/outliers.txt"
          }
        }"#;
        let error = serde_json::from_str::<ImagerTaskRequest>(payload)
            .expect_err("unsupported CASA tclean controls must not be silently ignored");
        let message = error.to_string();
        assert!(
            message.contains("unknown field"),
            "expected unknown-field rejection, got {message}"
        );
    }
}
