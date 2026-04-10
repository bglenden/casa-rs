// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Thin MeasurementSet-backed frontend for the pure `casa-imaging` core.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_imaging::{
    BeamFit, BeamFitDebugSummary, CleanConfig, CleanStopReason, CompatibilityMode,
    CubeChannelRequest, CubeImagingRequest, Deconvolver, GaussianUvTaper, ImageGeometry,
    ImagingRequest, ImagingStageTimings, MinorCycleTrace, ParallelHandBatch, PlaneStokes,
    RestoringBeamMode, VisibilityBatch, WTermMode, WeightDensityMode, WeightingMode, run_cube,
    run_imaging,
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
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeChannelContribution, CubeInterpolation, CubeSpecMode,
    CubeSpectralSetup, parse_numeric_id_selector, parse_spw_selector,
    resolve_channel_selector_selection, resolve_contiguous_channel_selection,
};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use image::{ImageBuffer, Rgb};
use ndarray::{Array2, Array4, IxDyn, s};
use num_complex::Complex32;

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

/// Execute the imager using an already-parsed configuration.
pub fn run_from_config(config: &CliConfig) -> Result<RunSummary, String> {
    let total_start = Instant::now();
    let stage_start = Instant::now();
    let ms = MeasurementSet::open(&config.ms).map_err(|error| format!("open MS: {error}"))?;
    let open_measurement_set = stage_start.elapsed();
    let stage_start = Instant::now();
    let data_column = resolve_data_column(&ms, config.datacolumn.as_deref())?;
    let prepared = prepare_plane_input(&ms, config, data_column)?;
    let prepare_plane_time = stage_start.elapsed();

    let stage_start = Instant::now();
    let phase_center = extract_phase_center(&ms, prepared.field_id())?;
    let extract_phase_center = stage_start.elapsed();

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
        PreparedInput::Mfs(plane) => RunProducts::Mfs(
            run_imaging(&ImagingRequest {
                geometry,
                visibility_batches: plane.batches,
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
                compatibility: CompatibilityMode::CasaStandardMfs,
            })
            .map_err(|error| error.to_string())?,
        ),
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
                compatibility: CompatibilityMode::CasaStandardMfs,
            })
            .map_err(|error| error.to_string())?,
        ),
    };
    let run_imaging_time = stage_start.elapsed();

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
    let stage_start = Instant::now();
    write_products(config, &coords, &run_result)?;
    let write_products_time = stage_start.elapsed();

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
    /// Optional `FIELD_ID` restriction.
    pub field: Option<i32>,
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
    /// Optional explicit single-correlation override.
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
    /// Skip CLEAN and only write dirty/residual products.
    pub dirty_only: bool,
    /// Write PNG preview sidecars for the CASA image products.
    pub write_preview_pngs: bool,
}

impl CliConfig {
    fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, String> {
        let mut ms = None::<PathBuf>;
        let mut imagename = None::<PathBuf>;
        let mut imsize = None::<usize>;
        let mut cell_arcsec = None::<f64>;
        let mut field = None::<i32>;
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
                    field = Some(parse_single_numeric_selector(
                        &next_value(&mut args, "--field")?,
                        "field",
                    )?);
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
                "--corr" => {
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

        Ok(Self {
            ms: ms.ok_or_else(|| format!("missing --ms\n\n{}", help_text()))?,
            imagename: imagename
                .ok_or_else(|| format!("missing --imagename\n\n{}", help_text()))?,
            imsize: imsize.ok_or_else(|| format!("missing --imsize\n\n{}", help_text()))?,
            cell_arcsec: cell_arcsec
                .ok_or_else(|| format!("missing --cell-arcsec\n\n{}", help_text()))?,
            field,
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
    field_id: usize,
    freq_ref: FrequencyRef,
    reffreq_hz: f64,
    selected_frequency_range_hz: [f64; 2],
    plane_stokes: PlaneStokes,
    batches: Vec<VisibilityBatch>,
}

struct CubePlaneInput {
    field_id: usize,
    freq_ref: FrequencyRef,
    plane_stokes: PlaneStokes,
    channels: Vec<CubeChannelRequest>,
}

enum PreparedInput {
    Mfs(PlaneInput),
    Cube(CubePlaneInput),
}

impl PreparedInput {
    fn field_id(&self) -> usize {
        match self {
            Self::Mfs(plane) => plane.field_id,
            Self::Cube(cube) => cube.field_id,
        }
    }
}

enum RunProducts {
    Mfs(casa_imaging::ImagingResult),
    Cube(casa_imaging::CubeImagingResult),
}

impl RunProducts {
    fn plane_stokes(&self) -> PlaneStokes {
        match self {
            Self::Mfs(result) => result.compatibility.plane_stokes,
            Self::Cube(result) => result.compatibility.plane_stokes,
        }
    }

    fn channel_frequencies_hz(&self) -> &[f64] {
        match self {
            Self::Mfs(result) => &result.compatibility.channel_frequencies_hz,
            Self::Cube(result) => &result.compatibility.channel_frequencies_hz,
        }
    }

    fn warnings(&self) -> Vec<String> {
        match self {
            Self::Mfs(result) => result.diagnostics.warnings.clone(),
            Self::Cube(result) => result.diagnostics.warnings.clone(),
        }
    }

    fn gridded_samples(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.gridded_samples,
            Self::Cube(result) => result.diagnostics.gridded_samples,
        }
    }

    fn major_cycles(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.major_cycles,
            Self::Cube(result) => result.diagnostics.major_cycles,
        }
    }

    fn minor_iterations(&self) -> usize {
        match self {
            Self::Mfs(result) => result.diagnostics.minor_iterations,
            Self::Cube(result) => result.diagnostics.minor_iterations,
        }
    }

    fn clean_stop_reason(&self) -> Option<CleanStopReason> {
        match self {
            Self::Mfs(result) => result.diagnostics.clean_stop_reason,
            Self::Cube(result) => {
                let mut reasons = result
                    .diagnostics
                    .channel_diagnostics
                    .iter()
                    .filter_map(|diag| diag.clean_stop_reason);
                let first = reasons.next()?;
                if reasons.all(|reason| reason == first) {
                    Some(first)
                } else {
                    None
                }
            }
        }
    }

    fn channel_summaries(&self) -> Vec<ChannelRunSummary> {
        match self {
            Self::Mfs(_) => Vec::new(),
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
            Self::Cube(result) => result.diagnostics.stage_timings,
        }
    }
}

struct PhaseCenter {
    angles_rad: [f64; 2],
    reference: DirectionRef,
}

fn prepare_plane_input(
    ms: &MeasurementSet,
    config: &CliConfig,
    data_column_kind: VisibilityDataColumn,
) -> Result<PreparedInput, String> {
    let field_column = ms
        .main_table()
        .get_column("FIELD_ID")
        .map_err(|error| format!("open FIELD_ID column: {error}"))?;
    let ddid_column = ms
        .main_table()
        .get_column("DATA_DESC_ID")
        .map_err(|error| format!("open DATA_DESC_ID column: {error}"))?;
    let data_description = ms
        .data_description()
        .map_err(|error| format!("open DATA_DESCRIPTION: {error}"))?;
    let ddid_info = data_description_index(&data_description)?;
    let allowed_ddids = allowed_ddids(config, &ddid_info)?;
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
    let time_column = TimeColumn::new(ms.main_table());
    let uvw = UvwColumn::new(ms.main_table());
    let antenna1 = main_ids::antenna1(ms.main_table());
    let antenna2 = main_ids::antenna2(ms.main_table());
    let derived_engine = if config.spectral_mode.is_cube_like() {
        Some(MsCalEngine::new(ms).map_err(|error| format!("build derived engine: {error}"))?)
    } else {
        None
    };

    let mut selected_field = None::<i32>;
    let mut selected_ddid = None::<i32>;
    let mut selected_rows = Vec::<(usize, Option<f64>)>::new();
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
        if config.field.is_some_and(|value| value != field_id) {
            continue;
        }
        if config.ddid.is_some_and(|value| value != ddid) {
            continue;
        }
        if !allowed_ddids.is_empty() && !allowed_ddids[ddid as usize] {
            continue;
        }

        selected_field = combine_single(selected_field, field_id, "FIELD_ID")?;
        selected_ddid = combine_single(selected_ddid, ddid, "DATA_DESC_ID")?;
        let row_time_mjd_sec = if config.spectral_mode.is_cube_like() {
            let row_time_mjd_sec = time_column
                .get_mjd_seconds(row)
                .map_err(|error| format!("read TIME row {row}: {error}"))?;
            reference_row_time_mjd_sec.get_or_insert(row_time_mjd_sec);
            match &mut time_bounds_mjd_sec {
                Some(bounds) => {
                    bounds[0] = bounds[0].min(row_time_mjd_sec);
                    bounds[1] = bounds[1].max(row_time_mjd_sec);
                }
                None => {
                    time_bounds_mjd_sec = Some([row_time_mjd_sec, row_time_mjd_sec]);
                }
            }
            Some(row_time_mjd_sec)
        } else {
            None
        };
        selected_rows.push((row, row_time_mjd_sec));
    }

    let field_id = selected_field.ok_or_else(|| "selection resolved to no field".to_string())?;
    let ddid = selected_ddid.ok_or_else(|| "selection resolved to no DDID".to_string())?;
    let cube_context = if config.spectral_mode.is_cube_like() {
        Some(CubeSetupContext {
            field_id: field_id as usize,
            reference_row_time_mjd_sec: reference_row_time_mjd_sec
                .ok_or_else(|| "selection resolved to no cube rows".to_string())?,
            time_bounds_mjd_sec: time_bounds_mjd_sec
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
        ddid as usize,
        &ddid_info,
        &spectral_window,
        &polarization,
        cube_context,
    );
    if let Some(init_error) = prepared.initialization_error.take() {
        return Err(init_error);
    }
    if selected_rows.is_empty() {
        return Err("selection resolved to no rows".to_string());
    }
    for (row, row_time_mjd_sec) in selected_rows {
        prepared.accumulate_row(
            row,
            field_id as usize,
            row_time_mjd_sec,
            &data_column,
            &flag_column,
            &flag_row,
            &weight_column,
            weight_spectrum.as_ref(),
            derived_engine.as_ref(),
            &uvw,
            &antenna1,
            &antenna2,
        )?;
    }
    prepared.finish(field_id as usize)
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
            && !ddid_info[ddid as usize].is_some_and(|(row_spw, _)| row_spw == spw as usize)
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
        angles_rad: [ra, dec],
        reference: DirectionRef::J2000,
    })
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

fn parse_cube_axis_value(text: &str, veltype: DopplerRef) -> Result<CubeAxisValue, String> {
    CubeAxisValue::parse(text, veltype).map_err(|error| error.to_string())
}

fn parse_cube_interpolation(text: &str) -> Result<CubeInterpolation, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "nearest" => Ok(CubeInterpolation::Nearest),
        "linear" => Ok(CubeInterpolation::Linear),
        "cubic" => Ok(CubeInterpolation::Cubic),
        other => Err(format!("unsupported cube interpolation {other:?}")),
    }
}

fn parse_rest_frequency_hz(text: &str) -> Result<f64, String> {
    let parsed = parse_cube_axis_value(text, DopplerRef::RADIO)?;
    match parsed {
        CubeAxisValue::FrequencyHz { hz, .. } => Ok(hz),
        other => Err(format!(
            "rest frequency must be a frequency quantity, found {other:?}"
        )),
    }
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
    state: PreparedState,
}

#[derive(Clone, Copy)]
struct CubeSetupContext<'a> {
    field_id: usize,
    reference_row_time_mjd_sec: f64,
    time_bounds_mjd_sec: [f64; 2],
    derived_engine: &'a MsCalEngine,
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
    },
    PairedMfs {
        paired: ParallelHandBatch,
        pair: (usize, usize),
    },
    PairedCube {
        channel_batches: Vec<ParallelHandBatch>,
        pair: (usize, usize),
    },
}

impl PreparedSelection {
    fn new(
        config: &CliConfig,
        ddid: usize,
        ddid_info: &[Option<(usize, usize)>],
        spectral_window: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
        polarization: &casa_ms::subtables::polarization::MsPolarization<'_>,
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
                    cube_context.field_id,
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
            let state = if let Some(correlation) = config.correlation.as_deref() {
                let plane_stokes = parse_plane_stokes(correlation)?;
                let corr_code = plane_to_corr_code(plane_stokes);
                let corr_index = corr_types
                    .iter()
                    .position(|code| *code == corr_code)
                    .ok_or_else(|| format!("requested correlation {correlation} is not present"))?;
                match config.spectral_mode {
                    SpectralMode::Mfs => PreparedState::ExplicitMfs {
                        plane_stokes,
                        corr_index,
                        batch: empty_visibility_batch(max_samples),
                    },
                    SpectralMode::Cube | SpectralMode::Cubedata => PreparedState::ExplicitCube {
                        plane_stokes,
                        corr_index,
                        channel_batches: output_channel_frequencies_hz
                            .iter()
                            .map(|_| empty_visibility_batch(16))
                            .collect(),
                    },
                }
            } else {
                let pair = if let (Some(xx), Some(yy)) = (
                    corr_types.iter().position(|code| *code == 9),
                    corr_types.iter().position(|code| *code == 12),
                ) {
                    (xx, yy)
                } else if let (Some(rr), Some(ll)) = (
                    corr_types.iter().position(|code| *code == 5),
                    corr_types.iter().position(|code| *code == 8),
                ) {
                    (rr, ll)
                } else {
                    return Err(
                        "strict Stokes I imaging requires XX+YY or RR+LL unless --corr is supplied"
                            .to_string(),
                    );
                };
                match config.spectral_mode {
                    SpectralMode::Mfs => PreparedState::PairedMfs {
                        pair,
                        paired: empty_parallel_hand_batch(max_samples),
                    },
                    SpectralMode::Cube | SpectralMode::Cubedata => PreparedState::PairedCube {
                        pair,
                        channel_batches: output_channel_frequencies_hz
                            .iter()
                            .map(|_| empty_parallel_hand_batch(16))
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
                state,
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
                state: PreparedState::ExplicitMfs {
                    plane_stokes: PlaneStokes::I,
                    corr_index: 0,
                    batch: empty_visibility_batch(0),
                },
            },
        }
    }

    fn accumulate_row(
        &mut self,
        row: usize,
        field_id: usize,
        row_time_mjd_sec: Option<f64>,
        data_column: &DataColumn<'_>,
        flag_column: &FlagColumn<'_>,
        flag_row: &FlagRowColumn<'_>,
        weight_column: &casa_ms::columns::weight_columns::WeightColumn<'_>,
        weight_spectrum: Option<&WeightSpectrumColumn<'_>>,
        derived_engine: Option<&MsCalEngine>,
        uvw: &UvwColumn<'_>,
        antenna1: &main_ids::ScalarIdColumn<'_>,
        antenna2: &main_ids::ScalarIdColumn<'_>,
    ) -> Result<(), String> {
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
        let uvw_m = uvw
            .get(row)
            .map_err(|error| format!("read UVW row {row}: {error}"))?;
        let is_cross = antenna1
            .get(row)
            .and_then(|a1| antenna2.get(row).map(|a2| a1 != a2))
            .map_err(|error| format!("read antenna IDs row {row}: {error}"))?;

        match &mut self.state {
            PreparedState::ExplicitMfs {
                corr_index, batch, ..
            } => {
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
                for (channel_index, frequency_hz) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                {
                    if bool_at_2d(flags, *corr_index, channel_index)? {
                        continue;
                    }
                    let visibility = complex32_at_2d(data, *corr_index, channel_index)?;
                    let weight = resolve_weight(
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
                }
            }
            PreparedState::ExplicitCube {
                corr_index,
                channel_batches,
                ..
            } => {
                let cube_setup = self
                    .cube_spectral_setup
                    .as_ref()
                    .ok_or_else(|| "internal error: missing cube spectral setup".to_string())?;
                let derived_engine = derived_engine.ok_or_else(|| {
                    "internal error: missing derived engine for cube imaging".to_string()
                })?;
                let row_time_mjd_sec = row_time_mjd_sec.ok_or_else(|| {
                    "internal error: missing row time for cube imaging".to_string()
                })?;
                let assignments = cube_setup
                    .row_output_channel_contributions_batch(
                        &self.source_channel_frequencies_hz,
                        &self.source_channel_widths_hz,
                        row_time_mjd_sec,
                        field_id,
                        derived_engine,
                    )
                    .map_err(|error| error.to_string())?;
                for (output_channel, contributions) in assignments.into_iter().enumerate() {
                    if contributions.is_empty() {
                        continue;
                    }
                    let output_frequency_hz =
                        cube_setup.output_channel_frequencies_hz[output_channel];
                    let Some(sample) = interpolate_explicit_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *corr_index,
                        &self.source_channel_indices,
                        &contributions,
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
                }
            }
            PreparedState::PairedMfs { paired, pair } => {
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
                for (channel_index, frequency_hz) in self
                    .source_channel_indices
                    .iter()
                    .copied()
                    .zip(self.source_channel_frequencies_hz.iter().copied())
                {
                    let lambda_scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
                    paired.u_lambda.push(uvw_m[0] * lambda_scale);
                    paired.v_lambda.push(uvw_m[1] * lambda_scale);
                    paired.w_lambda.push(uvw_m[2] * lambda_scale);
                    paired
                        .first_visibility
                        .push(complex32_at_2d(data, pair.0, channel_index)?);
                    paired
                        .second_visibility
                        .push(complex32_at_2d(data, pair.1, channel_index)?);
                    paired.first_weight.push(resolve_weight(
                        row_weights,
                        weight_spectrum_row,
                        pair.0,
                        channel_index,
                    )?);
                    paired.second_weight.push(resolve_weight(
                        row_weights,
                        weight_spectrum_row,
                        pair.1,
                        channel_index,
                    )?);
                    paired
                        .first_flagged
                        .push(bool_at_2d(flags, pair.0, channel_index)?);
                    paired
                        .second_flagged
                        .push(bool_at_2d(flags, pair.1, channel_index)?);
                    paired.gridable.push(is_cross);
                }
            }
            PreparedState::PairedCube {
                channel_batches,
                pair,
            } => {
                let cube_setup = self
                    .cube_spectral_setup
                    .as_ref()
                    .ok_or_else(|| "internal error: missing cube spectral setup".to_string())?;
                let derived_engine = derived_engine.ok_or_else(|| {
                    "internal error: missing derived engine for cube imaging".to_string()
                })?;
                let row_time_mjd_sec = row_time_mjd_sec.ok_or_else(|| {
                    "internal error: missing row time for cube imaging".to_string()
                })?;
                let assignments = cube_setup
                    .row_output_channel_contributions_batch(
                        &self.source_channel_frequencies_hz,
                        &self.source_channel_widths_hz,
                        row_time_mjd_sec,
                        field_id,
                        derived_engine,
                    )
                    .map_err(|error| error.to_string())?;
                for (output_channel, contributions) in assignments.into_iter().enumerate() {
                    if contributions.is_empty() {
                        continue;
                    }
                    let output_frequency_hz =
                        cube_setup.output_channel_frequencies_hz[output_channel];
                    let Some(sample) = interpolate_paired_cube_output_sample(
                        data,
                        flags,
                        row_weights,
                        weight_spectrum_row,
                        *pair,
                        &self.source_channel_indices,
                        &contributions,
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
                }
            }
        }
        Ok(())
    }

    fn finish(self, field_id: usize) -> Result<PreparedInput, String> {
        let PreparedSelection {
            initialization_error: _,
            source_channel_indices: _,
            source_channel_frequencies_hz: _,
            source_channel_widths_hz: _,
            selected_frequency_range_hz,
            reffreq_hz,
            freq_ref,
            cube_spectral_setup,
            state,
        } = self;
        let (plane_stokes, batches) = match state {
            PreparedState::ExplicitMfs {
                plane_stokes,
                batch,
                ..
            } => (
                plane_stokes,
                chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
            ),
            PreparedState::PairedMfs { paired, .. } => {
                let collapsed = paired
                    .collapse_to_stokes_i()
                    .map_err(|error| error.to_string())?;
                (
                    PlaneStokes::I,
                    chunk_visibility_batch(collapsed, DEFAULT_BATCH_SIZE),
                )
            }
            PreparedState::ExplicitCube {
                plane_stokes,
                channel_batches,
                ..
            } => {
                let output_channel_frequencies_hz = cube_spectral_setup
                    .as_ref()
                    .map(|setup| &setup.output_channel_frequencies_hz)
                    .ok_or_else(|| "internal error: missing cube spectral setup".to_string())?;
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .map(|(channel_frequency_hz, batch)| CubeChannelRequest {
                        channel_frequency_hz,
                        visibility_batches: chunk_visibility_batch(batch, DEFAULT_BATCH_SIZE),
                    })
                    .collect();
                return Ok(PreparedInput::Cube(CubePlaneInput {
                    field_id,
                    freq_ref,
                    plane_stokes,
                    channels,
                }));
            }
            PreparedState::PairedCube {
                channel_batches, ..
            } => {
                let output_channel_frequencies_hz = cube_spectral_setup
                    .as_ref()
                    .map(|setup| &setup.output_channel_frequencies_hz)
                    .ok_or_else(|| "internal error: missing cube spectral setup".to_string())?;
                let channels = output_channel_frequencies_hz
                    .iter()
                    .copied()
                    .zip(channel_batches)
                    .map(|(channel_frequency_hz, batch)| {
                        let collapsed = batch
                            .collapse_to_stokes_i()
                            .map_err(|error| error.to_string())?;
                        Ok(CubeChannelRequest {
                            channel_frequency_hz,
                            visibility_batches: chunk_visibility_batch(
                                collapsed,
                                DEFAULT_BATCH_SIZE,
                            ),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                return Ok(PreparedInput::Cube(CubePlaneInput {
                    field_id,
                    freq_ref,
                    plane_stokes: PlaneStokes::I,
                    channels,
                }));
            }
        };
        Ok(PreparedInput::Mfs(PlaneInput {
            field_id,
            freq_ref,
            reffreq_hz,
            selected_frequency_range_hz,
            plane_stokes,
            batches,
        }))
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
    let (
        psf,
        residual,
        model,
        image,
        sumwt,
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
            beam_set_from_channel_beams(&result.beams, RestoringBeamMode::PerPlane)?,
            beam_set_from_channel_beams(&result.beams, RestoringBeamMode::PerPlane)?,
            beam_set_from_channel_beams(&result.restored_beams, RestoringBeamMode::PerPlane)?,
            result.compatibility.psf_units.as_str(),
            result.compatibility.residual_units.as_str(),
            result.compatibility.model_units.as_str(),
            result.compatibility.image_units.as_str(),
        ),
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

    if config.write_preview_pngs {
        write_preview_png(&PathBuf::from(format!("{base}.psf.png")), psf)?;
        write_preview_png(&PathBuf::from(format!("{base}.residual.png")), residual)?;
        write_preview_png(&PathBuf::from(format!("{base}.model.png")), model)?;
        write_preview_png(&PathBuf::from(format!("{base}.image.png")), image)?;
    }

    Ok(())
}

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
        PlaneStokes::XX => StokesType::XX,
        PlaneStokes::YY => StokesType::YY,
        PlaneStokes::RR => StokesType::RR,
        PlaneStokes::LL => StokesType::LL,
    }
}

fn plane_to_corr_code(plane: PlaneStokes) -> i32 {
    match plane {
        PlaneStokes::I => 1,
        PlaneStokes::RR => 5,
        PlaneStokes::LL => 8,
        PlaneStokes::XX => 9,
        PlaneStokes::YY => 12,
    }
}

fn parse_plane_stokes(text: &str) -> Result<PlaneStokes, String> {
    match text.to_ascii_uppercase().as_str() {
        "I" => Err(
            "omit --corr for strict Stokes I; --corr only accepts raw parallel-hand correlations"
                .to_string(),
        ),
        "XX" => Ok(PlaneStokes::XX),
        "YY" => Ok(PlaneStokes::YY),
        "RR" => Ok(PlaneStokes::RR),
        "LL" => Ok(PlaneStokes::LL),
        _ => Err(format!("unsupported --corr value {text:?}")),
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
        "clark" => Ok(Deconvolver::Clark),
        "multiscale" => Ok(Deconvolver::Multiscale),
        _ => Err(format!(
            "unsupported --deconvolver value {text:?}; expected hogbom, clark, or multiscale"
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
        _ => Err(format!(
            "unsupported --wterm value {text:?}; expected none or direct"
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
        _ => {
            return Err(format!(
                "--mask-image {} has shape {:?}, expected [{nx}, {ny}] or [{nx}, {ny}, 1, 1]",
                path.display(),
                shape
            ));
        }
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

fn resolve_weight(
    weight_row: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    corr: usize,
    chan: usize,
) -> Result<f32, String> {
    if let Some(spectrum) = weight_spectrum_row {
        match spectrum {
            ArrayValue::Float32(values) => {
                if let Some(weight) = values.get(IxDyn(&[corr, chan])) {
                    return Ok(*weight);
                }
            }
            ArrayValue::Float64(values) => {
                if let Some(weight) = values.get(IxDyn(&[corr, chan])) {
                    return Ok(*weight as f32);
                }
            }
            _ => {}
        }
    }
    match weight_row {
        ArrayValue::Float32(values) => values
            .get(IxDyn(&[corr]))
            .copied()
            .ok_or_else(|| format!("WEIGHT index [{corr}] out of bounds")),
        ArrayValue::Float64(values) => values
            .get(IxDyn(&[corr]))
            .map(|value| *value as f32)
            .ok_or_else(|| format!("WEIGHT index [{corr}] out of bounds")),
        other => Err(format!(
            "WEIGHT must be Float32/Float64, found {:?}",
            other.primitive_type()
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ExplicitCubeOutputSample {
    visibility: Complex32,
    weight: f32,
    sumwt_factor: f32,
}

fn interpolate_explicit_cube_output_sample(
    data: &ArrayValue,
    flags: &ArrayValue,
    row_weights: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    corr_index: usize,
    source_channel_indices: &[usize],
    contributions: &[CubeChannelContribution],
) -> Result<Option<ExplicitCubeOutputSample>, String> {
    let mut visibility = Complex32::new(0.0, 0.0);
    let mut weight = 0.0f32;
    let mut sumwt_factor = 0.0f32;

    for contribution in contributions {
        if !(contribution.factor.is_finite() && contribution.factor > 0.0) {
            return Ok(None);
        }
        let channel_index = source_channel_indices[contribution.source_channel];
        if bool_at_2d(flags, corr_index, channel_index)? {
            return Ok(None);
        }
        let source_visibility = complex32_at_2d(data, corr_index, channel_index)?;
        let source_weight =
            resolve_weight(row_weights, weight_spectrum_row, corr_index, channel_index)?;
        if !(source_visibility.re.is_finite()
            && source_visibility.im.is_finite()
            && source_weight.is_finite())
        {
            return Ok(None);
        }
        visibility += source_visibility * contribution.factor;
        weight += source_weight * contribution.factor;
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
        sumwt_factor,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PairedCubeOutputSample {
    first_visibility: Complex32,
    second_visibility: Complex32,
    first_weight: f32,
    second_weight: f32,
}

fn interpolate_paired_cube_output_sample(
    data: &ArrayValue,
    flags: &ArrayValue,
    row_weights: &ArrayValue,
    weight_spectrum_row: Option<&ArrayValue>,
    pair: (usize, usize),
    source_channel_indices: &[usize],
    contributions: &[CubeChannelContribution],
) -> Result<Option<PairedCubeOutputSample>, String> {
    let mut first_visibility = Complex32::new(0.0, 0.0);
    let mut second_visibility = Complex32::new(0.0, 0.0);
    let mut first_weight = 0.0f32;
    let mut second_weight = 0.0f32;

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
        let source_first_weight =
            resolve_weight(row_weights, weight_spectrum_row, pair.0, channel_index)?;
        let source_second_weight =
            resolve_weight(row_weights, weight_spectrum_row, pair.1, channel_index)?;
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
  --field ID                restrict to one FIELD_ID
  --ddid ID                 restrict to one DATA_DESC_ID
  --spw ID                  restrict to one spectral window when DDID is omitted
  --channel-start N         first selected channel
  --channel-count N         number of selected channels
  --datacolumn NAME         DATA, CORRECTED_DATA, or MODEL_DATA
  --corr XX|YY|RR|LL        explicit single-correlation imaging
  --specmode MODE           mfs, cube, or cubedata
  --weighting MODE          natural, uniform, or briggs
  --perchanweightdensity    cube uniform/briggs density per output channel
  --uvtaper SPEC            gaussian taper: MAJOR[,MINOR[,PA]] with arcsec/deg/lambda units
  --restoringbeam MODE      common
  --deconvolver MODE        hogbom, clark, or multiscale
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
  --wterm MODE              none or direct
  --dirty-only              write dirty/residual products without CLEAN
  --no-preview-pngs         skip writing PNG preview sidecars
  -h, --help                show this help
"
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::{Path, PathBuf};

    use casa_ms::{MeasurementSetBuilder, OptionalMainColumn, SubtableId};
    use casa_tables::table_measures::{MeasureType, TableMeasDesc};
    use casa_test_support::gridder_interop::cpp_convolve_gridder_make_dirty_image_2d;
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::epoch::{EpochRef, MEpoch};
    use casa_types::measures::frame::MeasFrame;
    use casa_types::measures::frequency::MFrequency;
    use casa_types::measures::position::MPosition;
    use casa_types::{RecordField, RecordValue};
    use ndarray::ArrayD;
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

    fn descend_f14_cube_config(ms_path: PathBuf) -> CliConfig {
        CliConfig {
            ms: ms_path,
            imagename: PathBuf::from("unused"),
            imsize: 100,
            cell_arcsec: 8.0,
            field: Some(0),
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
            field: Some(0),
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
            field: Some(0),
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
            field: Some(0),
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
            field: Some(0),
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
            field: Some(0),
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
            field: Some(0),
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
    fn explicit_corr_i_is_rejected() {
        let error = parse_plane_stokes("I").unwrap_err();
        assert!(error.contains("omit --corr"));
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
        let weight = resolve_weight(&weight_row, Some(&weight_spectrum), 1, 0).unwrap();
        assert_eq!(weight, 4.0);
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
            field: None,
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
            field: None,
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
            field: None,
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
            Some(CubeSetupContext {
                field_id: 0,
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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
        let expected_frequencies_hz = vec![
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

        let ms = MeasurementSet::open(&ms_path.clone()).unwrap();
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
        }
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
            Some(CubeSetupContext {
                field_id: 0,
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
        add_main_row_with_antennas_channels(ms, 0, 1, uvw, vis);
    }

    fn add_main_row_with_antennas(
        ms: &mut MeasurementSet,
        antenna1: i32,
        antenna2: i32,
        uvw: [f64; 3],
        vis: [Complex32; 2],
    ) {
        add_main_row_with_antennas_channels(ms, antenna1, antenna2, uvw, &vis);
    }

    fn add_main_row_with_antennas_channels(
        ms: &mut MeasurementSet,
        antenna1: i32,
        antenna2: i32,
        uvw: [f64; 3],
        vis: &[Complex32],
    ) {
        assert!(
            vis.len().is_multiple_of(2),
            "test helper expects [num_corr=2, num_chan] visibility ordering"
        );
        let nchan = vis.len() / 2;
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
                "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
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
