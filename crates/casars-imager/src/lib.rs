// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Thin task and CLI surface for native CASA-RS imaging.
//!
//! This crate owns parsing, canonical task conversion, application invocation,
//! and result presentation. MeasurementSet interpretation, scientific
//! composition, execution planning, and publication live in
//! `casa-imaging-application` and its library dependencies.

mod managed_output;
mod native_application;
mod schema;
mod surface_types;
mod task_contract;

use std::{collections::BTreeMap, ffi::OsString, path::PathBuf, time::Duration};

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{
    BaseSource, OpenSessionRequest, ParameterRuntime, ResolutionPatch,
    parse_parameter_cli_overrides,
};

pub use casa_ms::{CubeAxisConfig, CubeAxisValue, CubeInterpolation};
pub use managed_output::*;
pub use schema::command_schema;
pub use surface_types::*;
pub use task_contract::*;

/// Spectral imaging family requested at the task surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralMode {
    /// Continuum multi-frequency synthesis.
    Mfs,
    /// Frame-aware spectral cube.
    Cube,
    /// Spectral cube in the native data frame.
    Cubedata,
    /// Source-rest-frame cube for a moving target.
    Cubesource,
    /// Multi-term continuum reconstruction through cube major cycles.
    Mvc,
}

impl SpectralMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "mfs" => Ok(Self::Mfs),
            "cube" => Ok(Self::Cube),
            "cubedata" => Ok(Self::Cubedata),
            "cubesource" => Ok(Self::Cubesource),
            "mvc" => Ok(Self::Mvc),
            _ => Err(format!("unsupported spectral mode {value:?}")),
        }
    }

    pub(crate) const fn cube_specmode(self) -> casa_ms::spectral_selection::CubeSpecMode {
        match self {
            Self::Mfs | Self::Cube | Self::Cubesource | Self::Mvc => {
                casa_ms::spectral_selection::CubeSpecMode::Cube
            }
            Self::Cubedata => casa_ms::spectral_selection::CubeSpecMode::Cubedata,
        }
    }
}

/// User-facing gridder family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridderRequest {
    /// Standard two-dimensional gridder.
    Standard,
    /// Mosaic gridder.
    Mosaic,
    /// W-projection gridder.
    WProject,
    /// A/W-projection gridder.
    AwProject,
}

/// Clean-mask generation mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CleanMaskMode {
    /// Use caller-provided masks only.
    #[default]
    User,
    /// Generate an auto-multithreshold mask.
    AutoMultiThreshold,
}

/// Auto-multithreshold task controls retained for canonical request transport.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoMultiThresholdConfig {
    /// Sidelobe threshold multiplier.
    pub sidelobe_threshold: f32,
    /// Noise threshold multiplier.
    pub noise_threshold: f32,
    /// Low-noise growth multiplier.
    pub low_noise_threshold: f32,
    /// Negative-feature threshold multiplier.
    pub negative_threshold: f32,
    /// Beam-scaled smoothing factor.
    pub smooth_factor: f32,
    /// Minimum region fraction of a beam.
    pub min_beam_frac: f32,
    /// Smoothed-mask cutoff.
    pub cut_threshold: f32,
    /// Maximum growth iterations.
    pub grow_iterations: usize,
    /// Whether grown regions are pruned.
    pub do_grow_prune: bool,
    /// Percent-change stopping control.
    pub min_percent_change: f32,
    /// Whether fast-noise statistics are requested.
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

/// MODEL_DATA persistence request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SaveModelMode {
    /// Do not persist a model column.
    #[default]
    None,
    /// Persist MODEL_DATA.
    ModelColumn,
}

/// Imaging memory-pressure policy transported by the task surface.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum ImagingMemoryPressurePolicy {
    /// Resource-adaptive default.
    #[default]
    Auto,
    /// Require physical-memory headroom.
    ConservativeNoSwap,
    /// Permit compression or incidental swapping.
    Aggressive,
    /// Permit explicit oversubscription experiments.
    Oversubscribe,
}

/// Dirty/residual FFT precision request.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum ImagingFftPrecisionPolicy {
    /// Use the application default.
    #[default]
    Auto,
    /// Force f64 where available.
    F64,
    /// Force f32 where available.
    F32,
}

/// Dirty/residual FFT backend request.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum ImagingFftBackendPolicy {
    /// Request automatic backend selection.
    Auto,
    /// Use RustFFT, the native portable default.
    #[default]
    RustFft,
    /// Use Apple Accelerate.
    Accelerate,
    /// Use Metal MPSGraph.
    MetalMpsGraph,
    /// Use FFTW.
    Fftw,
}

/// Explicit standard-MFS acceleration request.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum StandardMfsAccelerationPolicy {
    /// Request automatic acceleration selection.
    Auto,
    /// Select serial CPU, the native portable default.
    #[default]
    Cpu,
    /// Select fixed-tile multi-CPU.
    MultiCpu,
    /// Select Metal acceleration.
    Metal,
}

/// Parsed standalone-imager task configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct CliConfig {
    /// Input MeasurementSet path.
    pub ms: PathBuf,
    /// Output product prefix.
    pub imagename: PathBuf,
    /// Square image size.
    pub imsize: usize,
    /// Direction cell size in arcseconds.
    pub cell_arcsec: f64,
    /// Number of regular image facets along each direction axis.
    pub facets: usize,
    /// Selected fields.
    pub field_ids: Option<Vec<i32>>,
    /// UV-distance selector.
    pub uvrange: Option<String>,
    /// Observing-intent selector.
    pub intent: Option<String>,
    /// Phase-centre field override.
    pub phasecenter_field: Option<i32>,
    /// Explicit phase-centre override.
    pub phasecenter: Option<String>,
    /// Selected DDID.
    pub ddid: Option<i32>,
    /// Selected SPW when expressed as one integer.
    pub spw: Option<i32>,
    /// CASA SPW selector.
    pub spw_selector: Option<String>,
    /// First selected channel.
    pub channel_start: Option<usize>,
    /// Selected channel count.
    pub channel_count: Option<usize>,
    /// CASA-style line-free channels used for visibility-domain continuum fitting.
    pub continuum_fit_spw: Option<String>,
    /// Polynomial order for visibility-domain continuum fitting.
    pub continuum_fit_order: usize,
    /// Visibility-column override.
    pub datacolumn: Option<String>,
    /// Model persistence mode.
    pub save_model: SaveModelMode,
    /// Overwrite selected output-role `CORRECTED_DATA` cells with continuum residuals.
    pub save_continuum_residual: bool,
    /// Initial model path.
    pub start_model: Option<PathBuf>,
    /// Outlier definition path.
    pub outlier_file: Option<PathBuf>,
    /// Scalar plane or raw correlation.
    pub correlation: Option<String>,
    /// Spectral mode.
    pub spectral_mode: SpectralMode,
    /// Cube-axis controls.
    pub cube_axis: CubeAxisConfig,
    /// Visibility weighting.
    pub weighting: WeightingMode,
    /// Per-channel density toggle.
    pub per_channel_weight_density: bool,
    /// Pointing correction toggle.
    pub use_pointing: bool,
    /// Optional UV taper.
    pub uv_taper: Option<GaussianUvTaper>,
    /// Restoring beam policy.
    pub restoring_beam_mode: RestoringBeamMode,
    /// Minor-cycle solver.
    pub deconvolver: Deconvolver,
    /// Taylor term count.
    pub nterms: usize,
    /// Multiscale kernel sizes.
    pub multiscale_scales: Vec<f32>,
    /// Multiscale bias.
    pub small_scale_bias: f32,
    /// Minor-cycle iteration count.
    pub niter: usize,
    /// Major-cycle limit.
    pub nmajor: Option<usize>,
    /// Long-form minor summary toggle.
    pub fullsummary: bool,
    /// Minor-cycle gain.
    pub gain: f32,
    /// Absolute threshold in Jy/beam.
    pub threshold_jy: f32,
    /// Robust RMS multiplier.
    pub nsigma: f32,
    /// PSF fit cutoff.
    pub psf_cutoff: f32,
    /// Mosaic primary-beam limit.
    pub mosaic_pb_limit: f32,
    /// PB-corrected image toggle.
    pub pbcor: bool,
    /// Primary-beam product toggle.
    pub write_pb: bool,
    /// Minor-cycle refresh cadence.
    pub minor_cycle_length: usize,
    /// CASA cycle-factor control.
    pub cyclefactor: f32,
    /// Lower PSF-fraction clamp.
    pub min_psf_fraction: f32,
    /// Upper PSF-fraction clamp.
    pub max_psf_fraction: f32,
    /// Högbom iteration accounting.
    pub hogbom_iteration_mode: HogbomIterationMode,
    /// Masking mode.
    pub use_mask: CleanMaskMode,
    /// Auto-mask controls.
    pub auto_mask: AutoMultiThresholdConfig,
    /// Explicit mask boxes.
    pub mask_boxes: Vec<[usize; 4]>,
    /// Explicit mask image.
    pub mask_image: Option<PathBuf>,
    /// W-term mode.
    pub w_term_mode: WTermMode,
    /// Explicit standard-gridder request.
    pub force_standard_gridder: bool,
    /// W-projection plane request.
    pub w_project_planes: Option<usize>,
    /// A/W-projection controls.
    pub aw_project: Option<AwProjectControls>,
    /// Dirty-only toggle.
    pub dirty_only: bool,
    /// Explicit CASA-like local parallel execution intent.
    pub parallel: Option<bool>,
    /// Cube chunk count.
    pub chanchunks: Option<usize>,
    /// Acceleration request.
    pub standard_mfs_acceleration: StandardMfsAccelerationPolicy,
    /// Explicit standard-MFS backend.
    pub standard_mfs_backend: Option<String>,
    /// Explicit grid thread count.
    pub standard_mfs_grid_threads: Option<String>,
    /// Explicit fixed-tile anchor.
    pub standard_mfs_tile_anchor: Option<String>,
    /// Explicit residual backend.
    pub standard_mfs_residual_backend: Option<String>,
    /// Explicit initial-dirty backend.
    pub standard_mfs_initial_dirty_backend: Option<String>,
    /// Metal minor-cycle chunk.
    pub standard_mfs_metal_minor_cycle_chunk: Option<String>,
    /// Metal grouped-cache toggle.
    pub standard_mfs_metal_grouped_input_cache: Option<bool>,
    /// Standard-MFS memory target.
    pub standard_mfs_memory_target_mb: Option<usize>,
    /// Standard-MFS prepare buffer.
    pub standard_mfs_prepare_buffer_mb: Option<usize>,
    /// Shared imaging memory target.
    pub imaging_memory_target_mb: Option<usize>,
    /// Memory-pressure policy.
    pub imaging_memory_pressure_policy: ImagingMemoryPressurePolicy,
    /// Shared prepare buffer.
    pub imaging_prepare_buffer_mb: Option<usize>,
    /// Source row-block size.
    pub imaging_row_block_rows: Option<usize>,
    /// Prepare-worker count.
    pub imaging_prepare_workers: Option<usize>,
    /// Read-ahead block count.
    pub imaging_read_ahead_blocks: Option<usize>,
    /// FFT precision request.
    pub imaging_fft_precision: ImagingFftPrecisionPolicy,
    /// FFT backend request.
    pub imaging_fft_backend: ImagingFftBackendPolicy,
    /// Preview PNG toggle.
    pub write_preview_pngs: bool,
}

impl CliConfig {
    #[cfg(test)]
    /// Parse the direct CLI surface. Machine JSON requests are handled before
    /// this method by `TaskCliHost`.
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, String> {
        let mut config = Self::defaults();
        let mut robust = None;
        let args = args.into_iter().collect::<Vec<_>>();
        let mut index = 0;
        while index < args.len() {
            let flag = args[index]
                .to_str()
                .ok_or_else(|| "CLI argument is not UTF-8".to_string())?;
            let value = |offset: usize| -> Result<&str, String> {
                args.get(index + offset)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| format!("{flag} requires a value"))
            };
            let mut consumed = 2;
            match flag {
                "--ms" => config.ms = PathBuf::from(value(1)?),
                "--imagename" => config.imagename = PathBuf::from(value(1)?),
                "--imsize" => config.imsize = parse(value(1)?, flag)?,
                "--cell-arcsec" => config.cell_arcsec = parse(value(1)?, flag)?,
                "--projection" if value(1)?.eq_ignore_ascii_case("sin") => {}
                "--projection" => return Err("only SIN projection is supported".to_string()),
                "--field" => config.field_ids = Some(parse_ids(value(1)?, flag)?),
                "--uvrange" => config.uvrange = Some(value(1)?.to_string()),
                "--intent" => config.intent = Some(value(1)?.to_string()),
                "--phasecenter-field" => config.phasecenter_field = Some(parse(value(1)?, flag)?),
                "--phasecenter" => config.phasecenter = Some(value(1)?.to_string()),
                "--ddid" => config.ddid = Some(parse(value(1)?, flag)?),
                "--spw" => {
                    let text = value(1)?.to_string();
                    config.spw = text.parse().ok();
                    config.spw_selector = Some(text);
                }
                "--channel-start" => config.channel_start = Some(parse(value(1)?, flag)?),
                "--channel-count" | "--nchan" => {
                    config.channel_count = Some(parse(value(1)?, flag)?);
                }
                "--fitspw" => {
                    let selector = value(1)?;
                    config.continuum_fit_spw = (!selector.is_empty()
                        && !selector.eq_ignore_ascii_case("none"))
                    .then(|| selector.to_string());
                }
                "--fitorder" => config.continuum_fit_order = parse(value(1)?, flag)?,
                "--datacolumn" => config.datacolumn = Some(value(1)?.to_string()),
                "--savemodel" => config.save_model = parse_save_model(value(1)?)?,
                "--save-continuum-residual" => {
                    config.save_continuum_residual = true;
                    consumed = 1;
                }
                "--startmodel" => config.start_model = Some(PathBuf::from(value(1)?)),
                "--outlierfile" => config.outlier_file = Some(PathBuf::from(value(1)?)),
                "--corr" | "--stokes" => config.correlation = Some(value(1)?.to_string()),
                "--specmode" => config.spectral_mode = SpectralMode::parse(value(1)?)?,
                "--start" => {
                    config.cube_axis.start = Some(
                        CubeAxisValue::parse(value(1)?, config.cube_axis.veltype)
                            .map_err(|error| error.to_string())?,
                    )
                }
                "--width" => {
                    config.cube_axis.width = Some(
                        CubeAxisValue::parse(value(1)?, config.cube_axis.veltype)
                            .map_err(|error| error.to_string())?,
                    )
                }
                "--outframe" => {
                    config.cube_axis.outframe = value(1)?
                        .parse()
                        .map_err(|error| format!("parse --outframe: {error}"))?
                }
                "--veltype" => {
                    config.cube_axis.veltype = value(1)?
                        .parse()
                        .map_err(|error| format!("parse --veltype: {error}"))?
                }
                "--interpolation" => {
                    config.cube_axis.interpolation = parse_cube_interpolation(value(1)?)?
                }
                "--restfreq" => {
                    config.cube_axis.rest_frequency_hz = Some(
                        casa_ms::parse_rest_frequency_hz(value(1)?)
                            .map_err(|error| error.to_string())?,
                    )
                }
                "--weighting" => config.weighting = parse_weighting(value(1)?, None)?,
                "--robust" => {
                    robust = Some(parse(value(1)?, flag)?);
                }
                "--deconvolver" => config.deconvolver = parse_deconvolver(value(1)?)?,
                "--nterms" => config.nterms = parse(value(1)?, flag)?,
                "--scales" => config.multiscale_scales = parse_csv(value(1)?, flag)?,
                "--smallscalebias" => config.small_scale_bias = parse(value(1)?, flag)?,
                "--niter" => config.niter = parse(value(1)?, flag)?,
                "--nmajor" => {
                    let limit = parse::<i64>(value(1)?, flag)?;
                    config.nmajor = match limit {
                        -1 => None,
                        0.. => Some(limit as usize),
                        _ => return Err("--nmajor expects -1 or a non-negative value".to_string()),
                    };
                }
                "--fullsummary" => {
                    config.fullsummary = true;
                    consumed = 1;
                }
                "--gain" => config.gain = parse(value(1)?, flag)?,
                "--threshold-jy" | "--threshold" => config.threshold_jy = parse(value(1)?, flag)?,
                "--nsigma" => config.nsigma = parse(value(1)?, flag)?,
                "--psf-cutoff" | "--psfcutoff" => config.psf_cutoff = parse(value(1)?, flag)?,
                "--pblimit" => config.mosaic_pb_limit = parse(value(1)?, flag)?,
                "--pbcor" => {
                    config.pbcor = true;
                    consumed = 1;
                }
                "--write-pb" => {
                    config.write_pb = true;
                    consumed = 1;
                }
                "--minor-cycle-length" => config.minor_cycle_length = parse(value(1)?, flag)?,
                "--cyclefactor" => config.cyclefactor = parse(value(1)?, flag)?,
                "--minpsffraction" => config.min_psf_fraction = parse(value(1)?, flag)?,
                "--maxpsffraction" => config.max_psf_fraction = parse(value(1)?, flag)?,
                "--hogbom-iteration-mode" => {
                    config.hogbom_iteration_mode = match value(1)? {
                        "strict" => HogbomIterationMode::Strict,
                        "casa-inclusive" => HogbomIterationMode::CasaInclusive,
                        other => {
                            return Err(format!("unsupported --hogbom-iteration-mode {other:?}"));
                        }
                    }
                }
                "--perchanweightdensity" => {
                    config.per_channel_weight_density = true;
                    consumed = 1;
                }
                "--no-perchanweightdensity" => {
                    config.per_channel_weight_density = false;
                    consumed = 1;
                }
                "--uvtaper" => config.uv_taper = Some(parse_uv_taper(value(1)?)?),
                "--restoringbeam" => config.restoring_beam_mode = parse_restoring_beam(value(1)?)?,
                "--usemask" => config.use_mask = parse_clean_mask(value(1)?)?,
                "--sidelobethreshold" => {
                    config.auto_mask.sidelobe_threshold = parse(value(1)?, flag)?
                }
                "--noisethreshold" => config.auto_mask.noise_threshold = parse(value(1)?, flag)?,
                "--lownoisethreshold" => {
                    config.auto_mask.low_noise_threshold = parse(value(1)?, flag)?
                }
                "--negativethreshold" => {
                    config.auto_mask.negative_threshold = parse(value(1)?, flag)?
                }
                "--minbeamfrac" => config.auto_mask.min_beam_frac = parse(value(1)?, flag)?,
                "--growiterations" => config.auto_mask.grow_iterations = parse(value(1)?, flag)?,
                "--mask-box" => config.mask_boxes.push(parse_box(value(1)?)?),
                "--mask-image" => config.mask_image = Some(PathBuf::from(value(1)?)),
                "--gridder" => set_gridder(&mut config, value(1)?)?,
                "--wterm" => config.w_term_mode = parse_w_term(value(1)?)?,
                "--wprojplanes" => config.w_project_planes = Some(parse(value(1)?, flag)?),
                "--usepointing" => {
                    config.use_pointing = true;
                    consumed = 1;
                }
                "--dirty-only" => {
                    config.dirty_only = true;
                    consumed = 1;
                }
                "--parallel" => {
                    apply_parallel_runtime_control(Some(true), &mut config)?;
                    consumed = 1;
                }
                "--no-parallel" => {
                    apply_parallel_runtime_control(Some(false), &mut config)?;
                    consumed = 1;
                }
                "--chanchunks" => config.chanchunks = Some(parse(value(1)?, flag)?),
                "--no-preview-pngs" => {
                    config.write_preview_pngs = false;
                    consumed = 1;
                }
                "--write-preview-pngs" => {
                    config.write_preview_pngs = parse_bool(value(1)?, flag)?;
                }
                "--cfcache" => aw_controls(&mut config).cf_cache = PathBuf::from(value(1)?),
                "--cf-resident-mb" => {
                    aw_controls(&mut config).cf_resident_bytes =
                        parse::<usize>(value(1)?, flag)?.saturating_mul(1024 * 1024)
                }
                "--facets" => config.facets = parse(value(1)?, flag)?,
                "--psfphasecenter" => {
                    let direction = parse_csv::<f64>(value(1)?, flag)?;
                    aw_controls(&mut config).psf_phase_center_direction_rad = Some(
                        direction
                            .try_into()
                            .map_err(|_| "--psfphasecenter expects two radians".to_string())?,
                    );
                }
                "--vptable" => aw_controls(&mut config).vp_table = Some(PathBuf::from(value(1)?)),
                "--aterm" => {
                    aw_controls(&mut config).a_term = true;
                    consumed = 1;
                }
                "--no-aterm" => {
                    aw_controls(&mut config).a_term = false;
                    consumed = 1;
                }
                "--psterm" => {
                    aw_controls(&mut config).ps_term = true;
                    consumed = 1;
                }
                "--no-psterm" => {
                    aw_controls(&mut config).ps_term = false;
                    consumed = 1;
                }
                "--wbawp" => {
                    aw_controls(&mut config).wb_awp = true;
                    consumed = 1;
                }
                "--no-wbawp" => {
                    aw_controls(&mut config).wb_awp = false;
                    consumed = 1;
                }
                "--conjbeams" => {
                    aw_controls(&mut config).conjugate_beams = true;
                    consumed = 1;
                }
                "--no-conjbeams" => {
                    aw_controls(&mut config).conjugate_beams = false;
                    consumed = 1;
                }
                "--computepastep" => {
                    aw_controls(&mut config).compute_pa_step_deg = parse(value(1)?, flag)?
                }
                "--rotatepastep" => {
                    aw_controls(&mut config).rotate_pa_step_deg = parse(value(1)?, flag)?
                }
                "--pointingoffsetsigdev" => {
                    aw_controls(&mut config).pointing_offset_sigdev = parse_csv(value(1)?, flag)?
                }
                "--mosweight" => {
                    aw_controls(&mut config).mosaic_weighting = true;
                    consumed = 1;
                }
                "--no-mosweight" => {
                    aw_controls(&mut config).mosaic_weighting = false;
                    consumed = 1;
                }
                "--normtype" => {
                    aw_controls(&mut config).normalization = parse_aw_normalization(value(1)?)?
                }
                "--imaging-fft-precision" => {
                    config.imaging_fft_precision = parse_fft_precision(value(1)?)?
                }
                "--imaging-memory-pressure-policy" => {
                    config.imaging_memory_pressure_policy = parse_memory_policy(value(1)?)?
                }
                "--imaging-memory-target-mb" => {
                    config.imaging_memory_target_mb = Some(parse(value(1)?, flag)?)
                }
                "--imaging-prepare-buffer-mb" => {
                    config.imaging_prepare_buffer_mb = Some(parse(value(1)?, flag)?)
                }
                "--imaging-row-block-rows" => {
                    config.imaging_row_block_rows = Some(parse(value(1)?, flag)?)
                }
                "--imaging-prepare-workers" => {
                    config.imaging_prepare_workers = Some(parse(value(1)?, flag)?)
                }
                "--imaging-read-ahead-blocks" => {
                    config.imaging_read_ahead_blocks = Some(parse(value(1)?, flag)?)
                }
                "--standard-mfs-acceleration" => {
                    config.standard_mfs_acceleration = parse_acceleration(value(1)?)?;
                }
                "--standard-mfs-backend" => {
                    config.standard_mfs_backend = Some(value(1)?.to_string());
                }
                "--standard-mfs-grid-threads" => {
                    let threads = value(1)?;
                    config.standard_mfs_grid_threads =
                        (threads != "auto").then(|| threads.to_string());
                }
                "--imaging-fft-backend" => {
                    config.imaging_fft_backend = parse_fft_backend(value(1)?)?;
                }
                other => return Err(format!("unknown casars-imager option {other:?}")),
            }
            index += consumed;
        }
        config.weighting = match (config.weighting, robust) {
            (WeightingMode::Briggs { robust }, None) => WeightingMode::Briggs { robust },
            (WeightingMode::BriggsBwTaper { robust }, None) => {
                WeightingMode::BriggsBwTaper { robust }
            }
            (WeightingMode::Briggs { .. }, Some(robust)) => WeightingMode::Briggs { robust },
            (WeightingMode::BriggsBwTaper { .. }, Some(robust)) => {
                WeightingMode::BriggsBwTaper { robust }
            }
            (weighting, _) => weighting,
        };
        if config.ms.as_os_str().is_empty() || config.imagename.as_os_str().is_empty() {
            return Err("--ms and --imagename are required".to_string());
        }
        if config.imsize == 0
            || config.facets == 0
            || config.cell_arcsec <= 0.0
            || !config.cell_arcsec.is_finite()
        {
            return Err("--imsize, --cell-arcsec, and --facets must be positive".to_string());
        }
        Ok(config)
    }

    fn defaults() -> Self {
        Self {
            ms: PathBuf::new(),
            imagename: PathBuf::new(),
            imsize: 0,
            cell_arcsec: 0.0,
            facets: 1,
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
            datacolumn: None,
            save_model: SaveModelMode::None,
            save_continuum_residual: false,
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
            nmajor: None,
            fullsummary: false,
            gain: 0.1,
            threshold_jy: 0.0,
            nsigma: 0.0,
            psf_cutoff: 0.35,
            mosaic_pb_limit: 0.2,
            pbcor: false,
            write_pb: false,
            minor_cycle_length: 1000,
            cyclefactor: 1.0,
            min_psf_fraction: 0.05,
            max_psf_fraction: 0.8,
            hogbom_iteration_mode: HogbomIterationMode::Strict,
            use_mask: CleanMaskMode::User,
            auto_mask: AutoMultiThresholdConfig::default(),
            mask_boxes: Vec::new(),
            mask_image: None,
            w_term_mode: WTermMode::None,
            force_standard_gridder: false,
            w_project_planes: None,
            aw_project: None,
            dirty_only: false,
            parallel: None,
            chanchunks: None,
            standard_mfs_acceleration: StandardMfsAccelerationPolicy::Cpu,
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
            imaging_memory_pressure_policy: ImagingMemoryPressurePolicy::Auto,
            imaging_prepare_buffer_mb: None,
            imaging_row_block_rows: None,
            imaging_prepare_workers: None,
            imaging_read_ahead_blocks: None,
            imaging_fft_precision: ImagingFftPrecisionPolicy::Auto,
            imaging_fft_backend: ImagingFftBackendPolicy::RustFft,
            write_preview_pngs: false,
        }
    }

    /// Project a fully resolved parameter-catalog value set into runtime
    /// configuration without reparsing generated CLI arguments.
    pub(crate) fn from_parameter_values(
        values: &BTreeMap<String, ParameterValue>,
    ) -> Result<Self, String> {
        let mut config = Self::defaults();
        let text = |name: &str| parameter_text(values, name);
        let optional_text = |name: &str| -> Result<Option<String>, String> {
            let value = text(name)?;
            Ok(
                (!matches!(value.to_ascii_lowercase().as_str(), "none" | "auto")
                    && !value.is_empty())
                .then_some(value),
            )
        };
        let integer = |name: &str| parameter_integer(values, name);
        let float = |name: &str| parameter_float(values, name);
        let boolean = |name: &str| parameter_bool(values, name);

        config.ms = PathBuf::from(text("vis")?);
        config.imagename = PathBuf::from(text("imagename")?);
        config.imsize = parameter_square_usize(values, "imsize")?;
        config.cell_arcsec = parameter_square_quantity(values, "cell", "arcsec")?;
        config.datacolumn = optional_text("datacolumn")?;
        config.save_model = parse_save_model(&text("savemodel")?)?;
        config.start_model = optional_text("startmodel")?.map(PathBuf::from);
        config.outlier_file = optional_text("outlierfile")?.map(PathBuf::from);
        config.field_ids = optional_text("field")?
            .map(|value| parse_ids(&value, "field"))
            .transpose()?;
        config.phasecenter_field = optional_text("phasecenter_field")?
            .map(|value| value.parse::<i32>().map_err(|error| error.to_string()))
            .transpose()?;
        config.ddid = optional_text("ddid")?
            .map(|value| value.parse::<i32>().map_err(|error| error.to_string()))
            .transpose()?;
        config.phasecenter = optional_text("phasecenter")?;
        config.spw_selector = optional_text("spw")?;
        config.spw = config
            .spw_selector
            .as_deref()
            .and_then(|value| value.parse().ok());
        config.channel_start = optional_usize(values, "channel_start")?;
        config.channel_count = optional_usize(values, "channel_count")?;
        config.correlation = Some(text("stokes")?);
        config.spectral_mode = SpectralMode::parse(&text("specmode")?)?;
        config.chanchunks = optional_usize(values, "chanchunks")?;
        if let Some(value) = optional_text("outframe")? {
            config.cube_axis.outframe = value
                .parse()
                .map_err(|error| format!("parse outframe: {error}"))?;
        }
        if let Some(value) = optional_text("veltype")? {
            config.cube_axis.veltype = value
                .parse()
                .map_err(|error| format!("parse veltype: {error}"))?;
        }
        if let Some(value) = optional_text("start")? {
            config.cube_axis.start = Some(
                CubeAxisValue::parse(&value, config.cube_axis.veltype)
                    .map_err(|error| error.to_string())?,
            );
        }
        if let Some(value) = optional_text("width")? {
            config.cube_axis.width = Some(
                CubeAxisValue::parse(&value, config.cube_axis.veltype)
                    .map_err(|error| error.to_string())?,
            );
        }
        if let Some(value) = optional_text("interpolation")? {
            config.cube_axis.interpolation = parse_cube_interpolation(&value)?;
        }
        if let Some(value) = optional_text("restfreq")? {
            config.cube_axis.rest_frequency_hz =
                Some(casa_ms::parse_rest_frequency_hz(&value).map_err(|error| error.to_string())?);
        }
        if let Some(value) = optional_text("restoringbeam")? {
            config.restoring_beam_mode = parse_restoring_beam(&value)?;
        }
        config.per_channel_weight_density = boolean("perchanweightdensity")?;
        config.dirty_only = boolean("dirty_only")?;
        config.niter = usize::try_from(integer("niter")?).map_err(|error| error.to_string())?;
        config.threshold_jy = parameter_quantity(values, "threshold", "Jy")? as f32;
        config.nmajor = match integer("nmajor")? {
            -1 => None,
            value if value >= 0 => Some(value as usize),
            value => return Err(format!("nmajor expects -1 or non-negative, found {value}")),
        };
        config.fullsummary = boolean("fullsummary")?;
        config.gain = float("gain")? as f32;
        config.nsigma = float("nsigma")? as f32;
        config.psf_cutoff = float("psfcutoff")? as f32;
        config.minor_cycle_length =
            usize::try_from(integer("minor_cycle_length")?).map_err(|error| error.to_string())?;
        config.cyclefactor = float("cyclefactor")? as f32;
        config.deconvolver = parse_deconvolver(&text("deconvolver")?)?;
        config.min_psf_fraction = float("minpsffraction")? as f32;
        config.max_psf_fraction = float("maxpsffraction")? as f32;
        config.nterms = usize::try_from(integer("nterms")?).map_err(|error| error.to_string())?;
        config.hogbom_iteration_mode = match text("hogbom_iteration_mode")?.as_str() {
            "strict" => HogbomIterationMode::Strict,
            "casa-inclusive" => HogbomIterationMode::CasaInclusive,
            value => return Err(format!("unsupported hogbom_iteration_mode {value:?}")),
        };
        config.multiscale_scales = optional_text("scales")?
            .map(|value| parse_csv(&value, "scales"))
            .transpose()?
            .unwrap_or_default();
        config.small_scale_bias = float("smallscalebias")? as f32;
        config.use_mask = parse_clean_mask(&text("usemask")?)?;
        config.auto_mask.sidelobe_threshold = float("sidelobethreshold")? as f32;
        config.auto_mask.noise_threshold = float("noisethreshold")? as f32;
        config.auto_mask.low_noise_threshold = float("lownoisethreshold")? as f32;
        config.auto_mask.negative_threshold = float("negativethreshold")? as f32;
        config.auto_mask.min_beam_frac = float("minbeamfrac")? as f32;
        config.auto_mask.grow_iterations =
            usize::try_from(integer("growiterations")?).map_err(|error| error.to_string())?;
        config.mask_boxes = optional_text("mask_box")?
            .map(|value| value.split(';').map(parse_box).collect())
            .transpose()?
            .unwrap_or_default();
        config.mask_image = optional_text("mask_image")?.map(PathBuf::from);
        config.weighting = parse_weighting(&text("weighting")?, Some(float("robust")? as f32))?;
        config.w_project_planes = optional_usize(values, "wprojplanes")?;
        config.facets = values
            .get("facets")
            .map(|_| integer("facets"))
            .transpose()?
            .map_or(Ok(1), |value| {
                usize::try_from(value).map_err(|error| error.to_string())
            })?;
        config.use_pointing = boolean("usepointing")?;
        config.uv_taper = optional_text("uvtaper")?
            .map(|value| parse_uv_taper(&value))
            .transpose()?;
        config.write_preview_pngs = boolean("write_preview_pngs")?;
        config.write_pb = boolean("write_pb")?;
        config.pbcor = boolean("pbcor")?;
        config.mosaic_pb_limit = float("pblimit")? as f32;
        config.w_term_mode = parse_w_term(&text("wterm")?)?;
        set_gridder(&mut config, &text("gridder")?)?;
        config.standard_mfs_acceleration = parse_acceleration(&text("standard_mfs_acceleration")?)?;
        config.parallel = optional_bool(values, "parallel")?;
        validate_parallel_acceleration(config.parallel, config.standard_mfs_acceleration)?;
        config.imaging_read_ahead_blocks = optional_usize(values, "imaging_read_ahead_blocks")?;
        config.imaging_fft_backend = parse_fft_backend(&text("imaging_fft_backend")?)?;
        config.uvrange = optional_text("uvrange")?;
        config.intent = optional_text("intent")?;
        if config.aw_project.is_some() {
            let controls = aw_controls(&mut config);
            controls.cf_cache = PathBuf::from(text("cfcache")?);
            controls.cf_resident_bytes = usize::try_from(integer("cf_resident_mb")?)
                .map_err(|error| error.to_string())?
                .saturating_mul(1024 * 1024);
            controls.psf_phase_center_direction_rad = optional_text("psfphasecenter")?
                .map(|value| {
                    let values = parse_csv::<f64>(&value, "psfphasecenter")?;
                    values
                        .try_into()
                        .map_err(|_| "psfphasecenter expects two radians".to_string())
                })
                .transpose()?;
            controls.vp_table = optional_text("vptable")?.map(PathBuf::from);
            controls.a_term = boolean("aterm")?;
            controls.ps_term = boolean("psterm")?;
            controls.wb_awp = boolean("wbawp")?;
            controls.conjugate_beams = boolean("conjbeams")?;
            controls.compute_pa_step_deg = float("computepastep")?;
            controls.rotate_pa_step_deg = float("rotatepastep")?;
            controls.pointing_offset_sigdev =
                parse_csv(&text("pointingoffsetsigdev")?, "pointingoffsetsigdev")?;
            controls.mosaic_weighting = boolean("mosweight")?;
            controls.normalization = parse_aw_normalization(&text("normtype")?)?;
        }
        config.imaging_memory_target_mb = optional_usize(values, "imaging_memory_target_mb")?;
        config.imaging_memory_pressure_policy =
            parse_memory_policy(&text("imaging_memory_pressure_policy")?)?;
        config.imaging_prepare_buffer_mb = optional_usize(values, "imaging_prepare_buffer_mb")?;
        config.imaging_row_block_rows = optional_usize(values, "imaging_row_block_rows")?;
        config.imaging_prepare_workers = optional_usize(values, "imaging_prepare_workers")?;
        config.imaging_fft_precision = parse_fft_precision(&text("imaging_fft_precision")?)?;
        config.standard_mfs_grid_threads =
            optional_usize(values, "standard_mfs_grid_threads")?.map(|value| value.to_string());
        if !text("projection")?.eq_ignore_ascii_case("sin") {
            return Err("only SIN projection is supported".to_string());
        }
        config.continuum_fit_spw = optional_text("fitspw")?;
        config.continuum_fit_order =
            usize::try_from(integer("fitorder")?).map_err(|error| error.to_string())?;
        config.save_continuum_residual = boolean("save_continuum_residual")?;
        if config.ms.as_os_str().is_empty() || config.imagename.as_os_str().is_empty() {
            return Err("vis and imagename are required".to_string());
        }
        Ok(config)
    }
}

fn parameter_value<'a>(
    values: &'a BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<&'a ParameterValue, String> {
    values
        .get(name)
        .ok_or_else(|| format!("resolved imager parameter {name:?} is missing"))
}

fn parameter_text(values: &BTreeMap<String, ParameterValue>, name: &str) -> Result<String, String> {
    match parameter_value(values, name)? {
        ParameterValue::String(value) => Ok(value.clone()),
        ParameterValue::Array(values) if values.len() == 1 => match &values[0] {
            ParameterValue::String(value) => Ok(value.clone()),
            value => Err(format!(
                "resolved imager parameter {name:?} singleton must be text, found {value:?}"
            )),
        },
        value => Err(format!(
            "resolved imager parameter {name:?} must be text, found {value:?}"
        )),
    }
}

fn parameter_integer(values: &BTreeMap<String, ParameterValue>, name: &str) -> Result<i64, String> {
    match parameter_value(values, name)? {
        ParameterValue::Integer(value) => Ok(*value),
        value => Err(format!(
            "resolved imager parameter {name:?} must be integer, found {value:?}"
        )),
    }
}

fn parameter_float(values: &BTreeMap<String, ParameterValue>, name: &str) -> Result<f64, String> {
    match parameter_value(values, name)? {
        ParameterValue::Float(value) => Ok(*value),
        ParameterValue::Integer(value) => Ok(*value as f64),
        value => Err(format!(
            "resolved imager parameter {name:?} must be numeric, found {value:?}"
        )),
    }
}

fn parameter_bool(values: &BTreeMap<String, ParameterValue>, name: &str) -> Result<bool, String> {
    match parameter_value(values, name)? {
        ParameterValue::Bool(value) => Ok(*value),
        value => Err(format!(
            "resolved imager parameter {name:?} must be boolean, found {value:?}"
        )),
    }
}

fn optional_usize(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<Option<usize>, String> {
    match parameter_value(values, name)? {
        ParameterValue::Integer(value) => usize::try_from(*value)
            .map(Some)
            .map_err(|error| error.to_string()),
        ParameterValue::String(value)
            if matches!(value.to_ascii_lowercase().as_str(), "none" | "auto") =>
        {
            Ok(None)
        }
        ParameterValue::String(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|error| error.to_string()),
        value => Err(format!(
            "resolved imager parameter {name:?} must be optional integer, found {value:?}"
        )),
    }
}

fn optional_bool(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<Option<bool>, String> {
    match parameter_value(values, name)? {
        ParameterValue::Bool(value) => Ok(Some(*value)),
        ParameterValue::String(value) if value.eq_ignore_ascii_case("none") => Ok(None),
        value => Err(format!(
            "resolved imager parameter {name:?} must be optional boolean, found {value:?}"
        )),
    }
}

fn parameter_square_usize(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<usize, String> {
    match parameter_value(values, name)? {
        ParameterValue::Integer(value) => {
            usize::try_from(*value).map_err(|error| error.to_string())
        }
        ParameterValue::Array(items) if items.len() == 2 => {
            let [first, second] = items.as_slice() else {
                unreachable!()
            };
            if first != second {
                return Err(format!("{name} must be square"));
            }
            match first {
                ParameterValue::Integer(value) => {
                    usize::try_from(*value).map_err(|error| error.to_string())
                }
                value => Err(format!("{name} axis must be integer, found {value:?}")),
            }
        }
        value => Err(format!(
            "resolved imager parameter {name:?} must be integer or square pair, found {value:?}"
        )),
    }
}

fn parameter_square_quantity(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
    suffix: &str,
) -> Result<f64, String> {
    match parameter_value(values, name)? {
        ParameterValue::String(value) => parse_suffixed(value, suffix),
        ParameterValue::Array(items) if items.len() == 2 && items[0] == items[1] => match &items[0]
        {
            ParameterValue::String(value) => parse_suffixed(value, suffix),
            value => Err(format!("{name} axis must be text, found {value:?}")),
        },
        value => Err(format!(
            "resolved imager parameter {name:?} must be quantity or square pair, found {value:?}"
        )),
    }
}

fn parameter_quantity(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
    suffix: &str,
) -> Result<f64, String> {
    match parameter_value(values, name)? {
        ParameterValue::Float(value) => Ok(*value),
        ParameterValue::Integer(value) => Ok(*value as f64),
        ParameterValue::String(value) => parse_suffixed(value, suffix),
        value => Err(format!(
            "resolved imager parameter {name:?} must be quantity, found {value:?}"
        )),
    }
}

fn parse_suffixed(value: &str, suffix: &str) -> Result<f64, String> {
    value
        .strip_suffix(suffix)
        .unwrap_or(value)
        .trim()
        .parse()
        .map_err(|error| format!("parse {value:?}: {error}"))
}

/// Compact presentation result from one application invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSummary {
    /// Warnings.
    pub warnings: Vec<String>,
    /// Accepted scalar samples.
    pub gridded_samples: usize,
    /// Major-cycle count.
    pub major_cycles: usize,
    /// Minor-cycle count charged to the reported task/controller budget.
    pub minor_iterations: usize,
    /// Minor-cycle components actually applied.
    pub actual_minor_iterations: usize,
    /// Minor-cycle stop reason.
    pub clean_stop_reason: Option<CleanStopReason>,
    /// Ordered owner-calculated minor-cycle diagnostics.
    pub minor_cycles: Vec<casa_imaging_application::NativeMinorCycleOutcome>,
    /// Final paired-operator visibility identities and provenance, when produced.
    pub visibility_products: Option<task_contract::ImagerVisibilityProductDiagnostic>,
    /// Measured wall-clock time for the application call and result projection.
    pub elapsed: Duration,
    /// Exact application-planned conventional CASA product suffixes.
    pub output_products: Vec<String>,
}

/// Execute one parsed config through the application owner.
pub fn run_from_config(config: &CliConfig) -> Result<RunSummary, String> {
    native_application::execute(config)
}

/// Execute one canonical task request through the application owner.
pub fn run_from_request(request: &ImagerRunTaskRequest) -> Result<RunSummary, String> {
    run_from_config(&request.to_cli_config()?)
}

fn request_from_parameter_cli_args(args: &[OsString]) -> Result<ImagerRunTaskRequest, String> {
    let bundle = casa_provider_contracts::builtin_surface_bundle("imager")?;
    let override_patch = parse_parameter_cli_overrides(&bundle, args)?;
    let session = ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle,
            workspace: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch,
            managed_save: false,
        })
        .map_err(|error| format!("resolve imager parameters: {error}"))?;
    let adaptation = imager_provider_invocation(&session.values(), Vec::new())?;
    let stdin = adaptation
        .invocation
        .stdin
        .ok_or_else(|| "imager provider projection omitted its canonical request".to_string())?;
    match serde_json::from_str::<ImagerTaskRequest>(&stdin)
        .map_err(|error| format!("decode canonical imager task request: {error}"))?
    {
        ImagerTaskRequest::Run(request) => Ok(request),
    }
}

/// Run the machine or direct CLI surface.
pub fn run_with_cli_args(args: impl IntoIterator<Item = OsString>) -> Result<(), String> {
    let raw_args = args.into_iter().collect::<Vec<_>>();
    let mut managed_output = false;
    let mut args = Vec::with_capacity(raw_args.len());
    let mut index = 0;
    while index < raw_args.len() {
        if raw_args[index].to_str() == Some("--managed-output") {
            managed_output = raw_args
                .get(index + 1)
                .and_then(|value| value.to_str())
                .map(|value| parse_bool(value, "--managed-output"))
                .transpose()?
                .unwrap_or(true);
            index += usize::from(
                raw_args
                    .get(index + 1)
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| matches!(value, "true" | "false")),
            ) + 1;
            continue;
        }
        args.push(raw_args[index].clone());
        index += 1;
    }
    let host = casa_task_runtime::TaskCliHost::new(
        imager_task_schema_bundle(),
        |request: ImagerTaskRequest| request.execute(),
    );
    if let Some(output) = host.dispatch(&args).map_err(|error| error.to_string())? {
        if managed_output
            && args
                .iter()
                .any(|argument| argument.to_str() == Some("--json-run"))
        {
            let result: ImagerTaskResult = serde_json::from_str(&output)
                .map_err(|error| format!("decode canonical imager task result: {error}"))?;
            let ImagerTaskResult::Run(result) = result;
            println!(
                "{}",
                serde_json::to_string_pretty(&ManagedImagingOutput::from_task_result(&result))
                    .map_err(|error| format!("serialize managed imaging output: {error}"))?
            );
        } else {
            println!("{output}");
        }
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("-h" | "--help")))
    {
        println!("{}", command_schema("casars-imager").render_help());
        return Ok(());
    }
    let request = request_from_parameter_cli_args(&args)?;
    let unsupported = request.unsupported_reasons()?;
    if !unsupported.is_empty() {
        return Err(format!(
            "imager request is unavailable in this installed build: {}",
            unsupported
                .iter()
                .map(|reason| format!("{}/{}", reason.kind, reason.id))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let summary = run_from_request(&request)?;
    let result = ImagerRunTaskResult::from_run(request, &summary);
    if managed_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&ManagedImagingOutput::from_task_result(&result))
                .map_err(|error| format!("serialize managed imaging output: {error}"))?
        );
    } else {
        for warning in &result.run.warnings {
            eprintln!("warning: {warning}");
        }
        println!(
            "Wrote CASA-compatible products at prefix {} ({} gridded samples, {} major cycles, {} reported minor iterations, {} actual components, stop={:?})",
            result.request.image_name.display(),
            result.run.gridded_samples,
            result.run.major_cycles,
            result.run.minor_iterations,
            result.run.actual_minor_iterations,
            result.run.clean_stop_reason,
        );
    }
    Ok(())
}

pub(crate) fn apply_parallel_runtime_control(
    parallel: Option<bool>,
    config: &mut CliConfig,
) -> Result<(), String> {
    if let Some(parallel) = parallel {
        config.parallel = Some(parallel);
    }
    Ok(())
}

pub(crate) fn validate_parallel_acceleration(
    parallel: Option<bool>,
    acceleration: StandardMfsAccelerationPolicy,
) -> Result<(), String> {
    if parallel == Some(false) && acceleration != StandardMfsAccelerationPolicy::Cpu {
        return Err(format!(
            "parallel=false conflicts with standard_mfs_acceleration={acceleration:?}"
        ));
    }
    Ok(())
}

pub(crate) const fn canonical_spectral_mode_name(value: SpectralMode) -> &'static str {
    match value {
        SpectralMode::Mfs => "mfs",
        SpectralMode::Cube => "cube",
        SpectralMode::Cubedata => "cubedata",
        SpectralMode::Cubesource => "cubesource",
        SpectralMode::Mvc => "mvc",
    }
}

pub(crate) fn canonical_weighting_name(value: WeightingMode) -> String {
    match value {
        WeightingMode::Natural => "natural".to_string(),
        WeightingMode::Uniform => "uniform".to_string(),
        WeightingMode::Briggs { robust } => format!("briggs({robust})"),
        WeightingMode::BriggsBwTaper { robust } => format!("briggs-bwtaper({robust})"),
    }
}

pub(crate) const fn canonical_deconvolver_name(value: Deconvolver) -> &'static str {
    match value {
        Deconvolver::Hogbom => "hogbom",
        Deconvolver::Clark => "clark",
        Deconvolver::Multiscale => "multiscale",
        Deconvolver::Mtmfs => "mtmfs",
    }
}

pub(crate) const fn canonical_hogbom_iteration_mode_name(
    value: HogbomIterationMode,
) -> &'static str {
    match value {
        HogbomIterationMode::Strict => "strict",
        HogbomIterationMode::CasaInclusive => "casa-inclusive",
    }
}

pub(crate) const fn canonical_restoring_beam_mode_name(value: RestoringBeamMode) -> &'static str {
    match value {
        RestoringBeamMode::PerPlane => "per-plane",
        RestoringBeamMode::Common => "common",
    }
}

pub(crate) const fn canonical_w_term_mode_name(value: WTermMode) -> &'static str {
    match value {
        WTermMode::None => "none",
        WTermMode::Direct => "direct",
        WTermMode::WProject => "wproject",
    }
}

fn parse<T: std::str::FromStr>(value: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("parse {flag}: {error}"))
}

fn parse_bool(value: &str, flag: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{flag} expects true or false")),
    }
}

fn parse_ids(value: &str, flag: &str) -> Result<Vec<i32>, String> {
    casa_ms::parse_numeric_id_selector(value, flag).map_err(|error| error.to_string())
}

fn parse_csv<T: std::str::FromStr>(value: &str, flag: &str) -> Result<Vec<T>, String>
where
    T::Err: std::fmt::Display,
{
    value
        .split(',')
        .map(|item| parse(item.trim(), flag))
        .collect()
}

fn parse_box(value: &str) -> Result<[usize; 4], String> {
    let values = parse_csv::<usize>(value, "--mask-box")?;
    values
        .try_into()
        .map_err(|_| "--mask-box expects x0,y0,x1,y1".to_string())
}

fn parse_cube_interpolation(value: &str) -> Result<CubeInterpolation, String> {
    match value.to_ascii_lowercase().as_str() {
        "nearest" => Ok(CubeInterpolation::Nearest),
        "linear" => Ok(CubeInterpolation::Linear),
        _ => Err(format!("unsupported cube interpolation {value:?}")),
    }
}

fn parse_restoring_beam(value: &str) -> Result<RestoringBeamMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "per-plane" | "perplane" => Ok(RestoringBeamMode::PerPlane),
        "common" => Ok(RestoringBeamMode::Common),
        _ => Err(format!("unsupported restoring beam {value:?}")),
    }
}

fn parse_clean_mask(value: &str) -> Result<CleanMaskMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "user" => Ok(CleanMaskMode::User),
        "auto-multithresh" | "automultithresh" => Ok(CleanMaskMode::AutoMultiThreshold),
        _ => Err(format!("unsupported mask mode {value:?}")),
    }
}

fn parse_w_term(value: &str) -> Result<WTermMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "none" => Ok(WTermMode::None),
        "direct" => Ok(WTermMode::Direct),
        "wproject" => Ok(WTermMode::WProject),
        _ => Err(format!("unsupported w-term mode {value:?}")),
    }
}

fn parse_uv_taper(value: &str) -> Result<GaussianUvTaper, String> {
    use casa_types::quanta::{Quantity, Unit};
    let values = value.split(',').collect::<Vec<_>>();
    if values.len() != 3 {
        return Err("--uvtaper expects major,minor,position-angle".to_string());
    }
    let radians = Unit::new("rad").expect("rad unit");
    let quantity = |text: &str| -> Result<f64, String> {
        text.parse::<Quantity>()
            .map_err(|error| format!("parse UV taper {text:?}: {error}"))?
            .get_value_in(&radians)
            .map_err(|error| format!("convert UV taper {text:?}: {error}"))
    };
    Ok(GaussianUvTaper {
        major: UvTaperSize::ImageFwhmRad(quantity(values[0])?),
        minor: UvTaperSize::ImageFwhmRad(quantity(values[1])?),
        position_angle_rad: quantity(values[2])?,
    })
}

fn aw_controls(config: &mut CliConfig) -> &mut AwProjectControls {
    config
        .aw_project
        .get_or_insert_with(|| AwProjectControls::casa_defaults(PathBuf::new()))
}

fn parse_aw_normalization(value: &str) -> Result<AwProjectNormalization, String> {
    match value.to_ascii_lowercase().as_str() {
        "flatnoise" => Ok(AwProjectNormalization::FlatNoise),
        "flatsky" => Ok(AwProjectNormalization::FlatSky),
        "pbsquare" => Ok(AwProjectNormalization::PbSquare),
        _ => Err(format!("unsupported AW normalization {value:?}")),
    }
}

fn parse_save_model(value: &str) -> Result<SaveModelMode, String> {
    match value.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
        "none" => Ok(SaveModelMode::None),
        "modelcolumn" => Ok(SaveModelMode::ModelColumn),
        _ => Err(format!("unsupported savemodel {value:?}")),
    }
}

fn parse_weighting(value: &str, robust: Option<f32>) -> Result<WeightingMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "natural" => Ok(WeightingMode::Natural),
        "uniform" => Ok(WeightingMode::Uniform),
        "briggs" => Ok(WeightingMode::Briggs {
            robust: robust.unwrap_or(0.5),
        }),
        "briggs-bwtaper" | "briggsbwtaper" => Ok(WeightingMode::BriggsBwTaper {
            robust: robust.unwrap_or(0.5),
        }),
        _ => Err(format!("unsupported weighting {value:?}")),
    }
}

fn parse_deconvolver(value: &str) -> Result<Deconvolver, String> {
    match value.to_ascii_lowercase().as_str() {
        "hogbom" => Ok(Deconvolver::Hogbom),
        "clark" => Ok(Deconvolver::Clark),
        "multiscale" => Ok(Deconvolver::Multiscale),
        "mtmfs" => Ok(Deconvolver::Mtmfs),
        _ => Err(format!("unsupported deconvolver {value:?}")),
    }
}

fn set_gridder(config: &mut CliConfig, value: &str) -> Result<(), String> {
    match value.to_ascii_lowercase().as_str() {
        "standard" => config.force_standard_gridder = true,
        "mosaic" => config.use_pointing = true,
        "wproject" => config.w_term_mode = WTermMode::WProject,
        "widefield" => {}
        "awproject" => {
            let _ = aw_controls(config);
        }
        _ => return Err(format!("unsupported gridder {value:?}")),
    }
    Ok(())
}

fn parse_fft_precision(value: &str) -> Result<ImagingFftPrecisionPolicy, String> {
    match value {
        "auto" => Ok(ImagingFftPrecisionPolicy::Auto),
        "f64" => Ok(ImagingFftPrecisionPolicy::F64),
        "f32" => Ok(ImagingFftPrecisionPolicy::F32),
        _ => Err(format!("unsupported FFT precision {value:?}")),
    }
}

fn parse_acceleration(value: &str) -> Result<StandardMfsAccelerationPolicy, String> {
    match value {
        "auto" => Ok(StandardMfsAccelerationPolicy::Auto),
        "cpu" => Ok(StandardMfsAccelerationPolicy::Cpu),
        "multi-cpu" => Ok(StandardMfsAccelerationPolicy::MultiCpu),
        "metal" => Ok(StandardMfsAccelerationPolicy::Metal),
        _ => Err(format!("unsupported acceleration {value:?}")),
    }
}

fn parse_fft_backend(value: &str) -> Result<ImagingFftBackendPolicy, String> {
    match value {
        "auto" => Ok(ImagingFftBackendPolicy::Auto),
        "rustfft" | "rust-fft" => Ok(ImagingFftBackendPolicy::RustFft),
        "accelerate" => Ok(ImagingFftBackendPolicy::Accelerate),
        "metal-mpsgraph" => Ok(ImagingFftBackendPolicy::MetalMpsGraph),
        "fftw" => Ok(ImagingFftBackendPolicy::Fftw),
        _ => Err(format!("unsupported FFT backend {value:?}")),
    }
}

fn parse_memory_policy(value: &str) -> Result<ImagingMemoryPressurePolicy, String> {
    match value {
        "auto" => Ok(ImagingMemoryPressurePolicy::Auto),
        "conservative-no-swap" => Ok(ImagingMemoryPressurePolicy::ConservativeNoSwap),
        "aggressive" => Ok(ImagingMemoryPressurePolicy::Aggressive),
        "oversubscribe" => Ok(ImagingMemoryPressurePolicy::Oversubscribe),
        _ => Err(format!("unsupported memory policy {value:?}")),
    }
}

#[cfg(test)]
mod cli_projection_tests {
    use super::*;

    #[test]
    fn standalone_cli_uses_only_catalog_declared_imager_spellings() {
        let canonical = request_from_parameter_cli_args(&[
            "--ms".into(),
            "input.ms".into(),
            "--imagename".into(),
            "products/image".into(),
            "--threshold-jy".into(),
            "2.5Jy".into(),
        ])
        .unwrap();
        assert_eq!(canonical.threshold_jy, 2.5);

        let error = request_from_parameter_cli_args(&[
            "--ms".into(),
            "input.ms".into(),
            "--imagename".into(),
            "products/image".into(),
            "--threshold".into(),
            "2.5Jy".into(),
        ])
        .unwrap_err();
        assert!(error.contains("unknown imager parameter flag --threshold"));
    }
}
