// SPDX-License-Identifier: LGPL-3.0-or-later
//! Profile `casars-imager` on a real MeasurementSet and summarize stage timings.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release -p casars-imager --example profile_imager -- \
//!   /path/to.ms --field 0 --spw 0 --channel-count 1 --dirty-only
//! ```

use std::env;
use std::fs;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::time::Duration;

use casa_imaging::{
    Deconvolver, HogbomIterationMode, RestoringBeamMode, WTermMode, WeightingMode,
    fft_backend::wall_to_io_ratio,
};
use casa_ms::{CubeAxisConfig, CubeAxisValue, CubeInterpolation};
use casa_types::measures::doppler::DopplerRef;
use casars_imager::{
    CliConfig, ImagerRunTaskRequest, ImagingFftBackendPolicy, ImagingFftPrecisionPolicy,
    RunSummary, SpectralMode, StandardMfsAccelerationPolicy, run_from_request,
};

#[derive(Debug, Clone)]
struct Options {
    ms: PathBuf,
    field_ids: Option<Vec<i32>>,
    phasecenter_field: Option<i32>,
    ddid: Option<i32>,
    spw: Option<i32>,
    channel_start: Option<usize>,
    channel_count: Option<usize>,
    cube_start: Option<CubeAxisValue>,
    cube_width: Option<CubeAxisValue>,
    datacolumn: Option<String>,
    correlation: Option<String>,
    spectral_mode: SpectralMode,
    interpolation: CubeInterpolation,
    weighting: WeightingMode,
    per_channel_weight_density: bool,
    use_pointing: bool,
    deconvolver: Deconvolver,
    standard_mfs_acceleration: StandardMfsAccelerationPolicy,
    standard_mfs_grid_threads: Option<String>,
    standard_mfs_metal_minor_cycle_chunk: Option<String>,
    imaging_memory_target_mb: Option<usize>,
    imaging_prepare_buffer_mb: Option<usize>,
    imaging_row_block_rows: Option<usize>,
    imaging_prepare_workers: Option<usize>,
    imaging_read_ahead_blocks: Option<usize>,
    parallel: Option<bool>,
    chanchunks: Option<usize>,
    imaging_fft_precision: ImagingFftPrecisionPolicy,
    imaging_fft_backend: ImagingFftBackendPolicy,
    nterms: usize,
    multiscale_scales: Vec<f32>,
    small_scale_bias: f32,
    imsize: usize,
    cell_arcsec: f64,
    niter: usize,
    gain: f32,
    threshold_jy: f32,
    nsigma: f32,
    psf_cutoff: f32,
    mosaic_pb_limit: f32,
    pbcor: bool,
    write_pb: bool,
    minor_cycle_length: usize,
    cyclefactor: f32,
    min_psf_fraction: f32,
    max_psf_fraction: f32,
    hogbom_iteration_mode: HogbomIterationMode,
    mask_boxes: Vec<[usize; 4]>,
    mask_image: Option<PathBuf>,
    w_term_mode: WTermMode,
    force_standard_gridder: bool,
    w_project_planes: Option<usize>,
    dirty_only: bool,
    repeats: usize,
    warmups: usize,
}

fn main() {
    if let Err(error) = run() {
        if error == help_text() {
            println!("{error}");
            return;
        }
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
    let temp = create_temp_workdir()?;

    for warmup in 0..options.warmups {
        let prefix = temp.join(format!("warmup-{warmup}"));
        let _ = run_profile_request(&options, prefix)?;
    }

    let mut runs = Vec::with_capacity(options.repeats);
    for run_index in 0..options.repeats {
        let prefix = temp.join(format!("run-{run_index}"));
        let summary = run_profile_request(&options, prefix)?;
        let io_time = frontend_io_time(&summary);
        let wall_io_ratio = wall_to_io_ratio(summary.frontend_timings.total, io_time);
        println!(
            "run={} frontend_total_ms={:.3} open_ms={:.3} prepare_ms={:.3} source_read_ms={:.3} source_prepare_ms={:.3} phase_center_ms={:.3} imaging_ms={:.3} coords_ms={:.3} write_ms={:.3} io_time_ms={:.3} wall_to_io_ratio={} core_total_ms={:.3} controller_ms={:.3} weighting_ms={:.3} executor_build_ms={:.3} major_refresh_ms={:.3} residual_refresh_overhead_ms={:.3} clean_cycle_setup_ms={:.3} deconvolver_setup_ms={:.3} multiscale_scale_refresh_ms={:.3} psf_grid_ms={:.3} psf_fft_ms={:.3} psf_normalize_ms={:.3} model_fft_ms={:.3} residual_grid_ms={:.3} residual_fft_ms={:.3} residual_normalize_ms={:.3} minor_ms={:.3} minor_solve_ms={:.3} beam_fit_ms={:.3} restore_ms={:.3}",
            run_index + 1,
            millis(summary.frontend_timings.total),
            millis(summary.frontend_timings.open_measurement_set),
            millis(summary.frontend_timings.prepare_plane_input),
            millis(
                summary
                    .frontend_timings
                    .get_ms_values_into_processing_buffer,
            ),
            millis(summary.frontend_timings.prepare_processing_buffer),
            millis(summary.frontend_timings.extract_phase_center),
            millis(summary.frontend_timings.run_imaging),
            millis(summary.frontend_timings.build_coordinate_system),
            millis(summary.frontend_timings.write_products),
            millis(io_time),
            format_optional_ratio(wall_io_ratio),
            millis(summary.stage_timings.total),
            millis(summary.stage_timings.controller_overhead),
            millis(summary.stage_timings.weighting),
            millis(summary.stage_timings.executor_build),
            millis(summary.stage_timings.major_cycle_refresh),
            millis(summary.stage_timings.residual_refresh_overhead),
            millis(summary.stage_timings.clean_cycle_setup),
            millis(summary.stage_timings.deconvolver_setup),
            millis(summary.stage_timings.multiscale_scale_refresh),
            millis(summary.stage_timings.psf_grid),
            millis(summary.stage_timings.psf_fft),
            millis(summary.stage_timings.psf_normalize),
            millis(summary.stage_timings.model_fft),
            millis(summary.stage_timings.residual_degrid_grid),
            millis(summary.stage_timings.residual_fft),
            millis(summary.stage_timings.residual_normalize),
            millis(summary.stage_timings.minor_cycle),
            millis(summary.stage_timings.minor_cycle_solve),
            millis(summary.stage_timings.beam_fit),
            millis(summary.stage_timings.restore),
        );
        maybe_print_standard_mfs_profile_run(run_index + 1, &options, &summary);
        runs.push(summary);
    }

    println!(
        "ms={} field_ids={:?} phasecenter_field={:?} ddid={:?} spw={:?} channel_start={:?} channel_count={:?} cube_start={:?} cube_width={:?} corr={:?} interpolation={:?} weighting={:?} use_pointing={} deconvolver={:?} nterms={} scales={:?} wterm={:?} wprojplanes={:?} imsize={} cell_arcsec={} dirty_only={} niter={} repeats={} warmups={}",
        options.ms.display(),
        options.field_ids,
        options.phasecenter_field,
        options.ddid,
        options.spw,
        options.channel_start,
        options.channel_count,
        options.cube_start,
        options.cube_width,
        options.correlation,
        options.interpolation,
        options.weighting,
        options.use_pointing,
        options.deconvolver,
        options.nterms,
        options.multiscale_scales,
        options.w_term_mode,
        options.w_project_planes,
        options.imsize,
        options.cell_arcsec,
        options.dirty_only,
        options.niter,
        options.repeats,
        options.warmups,
    );
    println!("stage medians (ms):");
    println!("frontend:");
    print_stage(
        "open_measurement_set",
        median_duration(&runs, |run| run.frontend_timings.open_measurement_set),
    );
    print_stage(
        "prepare_plane_input",
        median_duration(&runs, |run| run.frontend_timings.prepare_plane_input),
    );
    print_stage(
        "prepared_source_read",
        median_duration(&runs, |run| {
            run.frontend_timings.get_ms_values_into_processing_buffer
        }),
    );
    print_stage(
        "prepared_source_prepare",
        median_duration(&runs, |run| run.frontend_timings.prepare_processing_buffer),
    );
    print_stage(
        "run_imaging",
        median_duration(&runs, |run| run.frontend_timings.run_imaging),
    );
    print_stage(
        "extract_phase_center",
        median_duration(&runs, |run| run.frontend_timings.extract_phase_center),
    );
    print_stage(
        "build_coordinate_system",
        median_duration(&runs, |run| run.frontend_timings.build_coordinate_system),
    );
    print_stage(
        "write_products",
        median_duration(&runs, |run| run.frontend_timings.write_products),
    );
    print_stage(
        "frontend_total",
        median_duration(&runs, |run| run.frontend_timings.total),
    );
    print_stage("io_time", median_duration(&runs, frontend_io_time));
    print_ratio_stage(
        "wall_to_io_ratio",
        median_optional_f64(&runs, |run| {
            wall_to_io_ratio(run.frontend_timings.total, frontend_io_time(run))
        }),
    );
    println!("core:");
    print_stage(
        "controller_overhead",
        median_duration(&runs, |run| run.stage_timings.controller_overhead),
    );
    print_stage(
        "weighting",
        median_duration(&runs, |run| run.stage_timings.weighting),
    );
    print_stage(
        "executor_build",
        median_duration(&runs, |run| run.stage_timings.executor_build),
    );
    print_stage(
        "psf_grid",
        median_duration(&runs, |run| run.stage_timings.psf_grid),
    );
    print_stage(
        "psf_fft",
        median_duration(&runs, |run| run.stage_timings.psf_fft),
    );
    print_stage(
        "psf_normalize",
        median_duration(&runs, |run| run.stage_timings.psf_normalize),
    );
    print_stage(
        "model_fft",
        median_duration(&runs, |run| run.stage_timings.model_fft),
    );
    print_stage(
        "residual_degrid_grid",
        median_duration(&runs, |run| run.stage_timings.residual_degrid_grid),
    );
    print_stage(
        "residual_fft",
        median_duration(&runs, |run| run.stage_timings.residual_fft),
    );
    print_stage(
        "residual_normalize",
        median_duration(&runs, |run| run.stage_timings.residual_normalize),
    );
    print_stage(
        "major_cycle_refresh",
        median_duration(&runs, |run| run.stage_timings.major_cycle_refresh),
    );
    print_stage(
        "residual_refresh_overhead",
        median_duration(&runs, |run| run.stage_timings.residual_refresh_overhead),
    );
    print_stage(
        "clean_cycle_setup",
        median_duration(&runs, |run| run.stage_timings.clean_cycle_setup),
    );
    print_stage(
        "deconvolver_setup",
        median_duration(&runs, |run| run.stage_timings.deconvolver_setup),
    );
    print_stage(
        "multiscale_scale_refresh",
        median_duration(&runs, |run| run.stage_timings.multiscale_scale_refresh),
    );
    print_stage(
        "minor_cycle",
        median_duration(&runs, |run| run.stage_timings.minor_cycle),
    );
    print_stage(
        "minor_cycle_solve",
        median_duration(&runs, |run| run.stage_timings.minor_cycle_solve),
    );
    print_stage(
        "beam_fit",
        median_duration(&runs, |run| run.stage_timings.beam_fit),
    );
    print_stage(
        "restore",
        median_duration(&runs, |run| run.stage_timings.restore),
    );
    print_stage(
        "total",
        median_duration(&runs, |run| run.stage_timings.total),
    );
    println!(
        "result medians: gridded_samples={} major_cycles={} minor_iterations={}",
        median_usize(&runs, |run| run.gridded_samples),
        median_usize(&runs, |run| run.major_cycles),
        median_usize(&runs, |run| run.minor_iterations),
    );
    if let Some(max_channels) = runs.iter().map(|run| run.channel_summaries.len()).max()
        && max_channels > 0
    {
        println!("cube channel medians:");
        for channel_index in 0..max_channels {
            print!(
                "  channel={channel_index} major_cycles={} minor_iterations={}",
                median_usize(&runs, |run| {
                    run.channel_summaries
                        .get(channel_index)
                        .map(|summary| summary.major_cycles)
                        .unwrap_or(0)
                }),
                median_usize(&runs, |run| {
                    run.channel_summaries
                        .get(channel_index)
                        .map(|summary| summary.minor_iterations)
                        .unwrap_or(0)
                }),
            );
            if let Some(summary) = runs
                .iter()
                .find_map(|run| run.channel_summaries.get(channel_index).cloned())
            {
                print!(
                    " initial_peak={:.6} final_peak={:.6} cycle_threshold={:.6} stop={:?}",
                    summary.initial_residual_peak_jy_per_beam,
                    summary.final_residual_peak_jy_per_beam,
                    summary.final_cycle_threshold_jy_per_beam,
                    summary.clean_stop_reason,
                );
            }
            println!();
        }
    }
    fs::remove_dir_all(&temp)
        .map_err(|error| format!("remove temp workdir {}: {error}", temp.display()))?;
    Ok(())
}

fn create_temp_workdir() -> Result<PathBuf, String> {
    let path = env::temp_dir().join(format!(
        "casars-imager-profile-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| format!("clock before unix epoch: {error}"))?
            .as_nanos()
    ));
    fs::create_dir_all(&path)
        .map_err(|error| format!("create temp workdir {}: {error}", path.display()))?;
    Ok(path)
}

fn build_cli_config(options: &Options, imagename: PathBuf) -> CliConfig {
    let mut config = CliConfig {
        ms: options.ms.clone(),
        imagename,
        imsize: options.imsize,
        cell_arcsec: options.cell_arcsec,
        field_ids: options.field_ids.clone(),
        phasecenter_field: options.phasecenter_field,
        phasecenter: None,
        ddid: options.ddid,
        spw: options.spw,
        spw_selector: None,
        channel_start: options.channel_start,
        channel_count: options.channel_count,
        datacolumn: options.datacolumn.clone(),
        save_model: casars_imager::SaveModelMode::None,
        start_model: None,
        outlier_file: None,
        correlation: options.correlation.clone(),
        spectral_mode: options.spectral_mode,
        cube_axis: CubeAxisConfig {
            interpolation: options.interpolation,
            start: options.cube_start.clone(),
            width: options.cube_width.clone(),
            ..CubeAxisConfig::default()
        },
        weighting: options.weighting,
        per_channel_weight_density: options.per_channel_weight_density,
        use_pointing: options.use_pointing,
        uv_taper: None,
        restoring_beam_mode: RestoringBeamMode::PerPlane,
        deconvolver: options.deconvolver,
        nterms: options.nterms,
        multiscale_scales: options.multiscale_scales.clone(),
        small_scale_bias: options.small_scale_bias,
        niter: options.niter,
        nmajor: None,
        fullsummary: false,
        gain: options.gain,
        threshold_jy: options.threshold_jy,
        nsigma: options.nsigma,
        psf_cutoff: options.psf_cutoff,
        mosaic_pb_limit: options.mosaic_pb_limit,
        pbcor: options.pbcor,
        write_pb: options.write_pb,
        minor_cycle_length: options.minor_cycle_length,
        cyclefactor: options.cyclefactor,
        min_psf_fraction: options.min_psf_fraction,
        max_psf_fraction: options.max_psf_fraction,
        hogbom_iteration_mode: options.hogbom_iteration_mode,
        use_mask: Default::default(),
        auto_mask: Default::default(),
        mask_boxes: options.mask_boxes.clone(),
        mask_image: options.mask_image.clone(),
        w_term_mode: options.w_term_mode,
        force_standard_gridder: options.force_standard_gridder,
        w_project_planes: options.w_project_planes,
        dirty_only: options.dirty_only,
        chanchunks: options.chanchunks,
        standard_mfs_acceleration: options.standard_mfs_acceleration,
        standard_mfs_backend: None,
        standard_mfs_grid_threads: options.standard_mfs_grid_threads.clone(),
        standard_mfs_tile_anchor: None,
        standard_mfs_residual_backend: None,
        standard_mfs_initial_dirty_backend: None,
        standard_mfs_metal_minor_cycle_chunk: options.standard_mfs_metal_minor_cycle_chunk.clone(),
        standard_mfs_metal_grouped_input_cache: None,
        standard_mfs_memory_target_mb: None,
        standard_mfs_prepare_buffer_mb: None,
        imaging_memory_target_mb: options.imaging_memory_target_mb,
        imaging_prepare_buffer_mb: options.imaging_prepare_buffer_mb,
        imaging_row_block_rows: options.imaging_row_block_rows,
        imaging_prepare_workers: options.imaging_prepare_workers,
        imaging_read_ahead_blocks: options.imaging_read_ahead_blocks,
        imaging_fft_precision: options.imaging_fft_precision,
        imaging_fft_backend: options.imaging_fft_backend,
        write_preview_pngs: false,
    };
    if options.parallel == Some(false) {
        config.standard_mfs_acceleration = StandardMfsAccelerationPolicy::Cpu;
        config.standard_mfs_grid_threads = Some("1".to_string());
        config.imaging_prepare_workers = Some(1);
        config.imaging_read_ahead_blocks = Some(1);
        config.standard_mfs_metal_grouped_input_cache = Some(false);
    }
    config
}

fn run_profile_request(options: &Options, imagename: PathBuf) -> Result<RunSummary, String> {
    let config = build_cli_config(options, imagename);
    let request = ImagerRunTaskRequest::from_cli_config(&config);
    run_from_request(&request)
}

fn median_duration(runs: &[RunSummary], selector: impl Fn(&RunSummary) -> Duration) -> Duration {
    let mut values = runs.iter().map(selector).collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_usize(runs: &[RunSummary], selector: impl Fn(&RunSummary) -> usize) -> usize {
    let mut values = runs.iter().map(selector).collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_optional_f64(
    runs: &[RunSummary],
    selector: impl Fn(&RunSummary) -> Option<f64>,
) -> Option<f64> {
    let mut values = runs.iter().filter_map(selector).collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    Some(values[values.len() / 2])
}

fn print_stage(label: &str, value: Duration) {
    println!("  {label}={:.3}", millis(value));
}

fn print_ratio_stage(label: &str, value: Option<f64>) {
    println!("  {label}={}", format_optional_ratio(value));
}

fn frontend_io_time(summary: &RunSummary) -> Duration {
    summary.frontend_timings.open_measurement_set
        + summary
            .frontend_timings
            .get_ms_values_into_processing_buffer
        + summary.frontend_timings.write_products
}

fn format_optional_ratio(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "none".to_string())
}

fn maybe_print_standard_mfs_profile_run(
    run_number: usize,
    options: &Options,
    summary: &RunSummary,
) {
    if env::var_os("CASA_RS_STANDARD_MFS_PROFILE_DETAIL").is_none() {
        return;
    }
    let io_time = frontend_io_time(summary);
    let wall_io_ratio = wall_to_io_ratio(summary.frontend_timings.total, io_time);
    println!(
        "standard_mfs_profile_run run={} workload_ms={} field_ids={:?} phasecenter_field={:?} ddid={:?} spw={:?} channel_start={:?} channel_count={:?} spectral_mode={:?} weighting={:?} deconvolver={:?} nterms={} imsize={} niter={} dirty_only={} gridded_samples={} major_cycles={} minor_iterations={} grid_threads={:?} row_block_rows={:?} prepare_workers={:?} read_ahead_blocks={:?} memory_target_mb={:?} frontend_total_ms={:.3} io_time_ms={:.3} wall_to_io_ratio={} core_total_ms={:.3} prepare_plane_input_ms={:.3} source_read_ms={:.3} source_prepare_ms={:.3} weighting_ms={:.3} executor_build_ms={:.3} psf_grid_ms={:.3} residual_degrid_grid_ms={:.3} major_cycle_refresh_ms={:.3} peak_rss_bytes={} product_status=written",
        run_number,
        options.ms.display(),
        options.field_ids,
        options.phasecenter_field,
        options.ddid,
        options.spw,
        options.channel_start,
        options.channel_count,
        options.spectral_mode,
        options.weighting,
        options.deconvolver,
        options.nterms,
        options.imsize,
        options.niter,
        options.dirty_only,
        summary.gridded_samples,
        summary.major_cycles,
        summary.minor_iterations,
        options.standard_mfs_grid_threads,
        options.imaging_row_block_rows,
        options.imaging_prepare_workers,
        options.imaging_read_ahead_blocks,
        options.imaging_memory_target_mb,
        millis(summary.frontend_timings.total),
        millis(io_time),
        format_optional_ratio(wall_io_ratio),
        millis(summary.stage_timings.total),
        millis(summary.frontend_timings.prepare_plane_input),
        millis(
            summary
                .frontend_timings
                .get_ms_values_into_processing_buffer,
        ),
        millis(summary.frontend_timings.prepare_processing_buffer),
        millis(summary.stage_timings.weighting),
        millis(summary.stage_timings.executor_build),
        millis(summary.stage_timings.psf_grid),
        millis(summary.stage_timings.residual_degrid_grid),
        millis(summary.stage_timings.major_cycle_refresh),
        peak_rss_bytes().unwrap_or(0),
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss;
    if max_rss < 0 {
        return None;
    }
    #[cfg(target_os = "macos")]
    {
        Some(max_rss as u64)
    }
    #[cfg(target_os = "linux")]
    {
        Some((max_rss as u64).saturating_mul(1024))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

fn millis(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut ms = None::<PathBuf>;
    let mut field_ids = None::<Vec<i32>>;
    let mut phasecenter_field = None::<i32>;
    let mut ddid = None::<i32>;
    let mut spw = None::<i32>;
    let mut channel_start = None::<usize>;
    let mut channel_count = None::<usize>;
    let mut cube_start = None::<CubeAxisValue>;
    let mut cube_width = None::<CubeAxisValue>;
    let mut datacolumn = Some("DATA".to_string());
    let mut correlation = None::<String>;
    let mut spectral_mode = SpectralMode::Mfs;
    let mut interpolation = CubeInterpolation::Linear;
    let mut weighting_name = String::from("natural");
    let mut robust = 0.5f32;
    let mut per_channel_weight_density = None::<bool>;
    let mut use_pointing = false;
    let mut deconvolver = Deconvolver::Hogbom;
    let mut standard_mfs_acceleration = StandardMfsAccelerationPolicy::Auto;
    let mut standard_mfs_grid_threads = None::<String>;
    let mut standard_mfs_metal_minor_cycle_chunk = None::<String>;
    let mut imaging_memory_target_mb = None::<usize>;
    let mut imaging_prepare_buffer_mb = None::<usize>;
    let mut imaging_row_block_rows = None::<usize>;
    let mut imaging_prepare_workers = None::<usize>;
    let mut imaging_read_ahead_blocks = None::<usize>;
    let mut parallel = None::<bool>;
    let mut chanchunks = None::<usize>;
    let mut imaging_fft_precision = ImagingFftPrecisionPolicy::Auto;
    let mut imaging_fft_backend = ImagingFftBackendPolicy::Auto;
    let mut nterms = 1usize;
    let mut multiscale_scales = Vec::<f32>::new();
    let mut small_scale_bias = 0.0f32;
    let mut imsize = 128usize;
    let mut cell_arcsec = 30.0f64;
    let mut niter = 0usize;
    let mut gain = 0.1f32;
    let mut threshold_jy = 0.0f32;
    let mut nsigma = 0.0f32;
    let mut psf_cutoff = 0.35f32;
    let mut mosaic_pb_limit = 0.2f32;
    let mut pbcor = false;
    let mut write_pb = false;
    let mut minor_cycle_length = 2usize;
    let mut cyclefactor = 1.0f32;
    let mut min_psf_fraction = 0.1f32;
    let mut max_psf_fraction = 0.8f32;
    let mut hogbom_iteration_mode = HogbomIterationMode::Strict;
    let mut mask_boxes = Vec::<[usize; 4]>::new();
    let mut mask_image = None::<PathBuf>;
    let mut w_term_mode = WTermMode::None;
    let mut force_standard_gridder = false;
    let mut w_project_planes = None::<usize>;
    let mut dirty_only = false;
    let mut repeats = 5usize;
    let mut warmups = 1usize;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(help_text()),
            "--field" => {
                field_ids = Some(
                    casa_ms::parse_numeric_id_selector(&next_value(&mut args, "--field")?, "field")
                        .map_err(|error| error.to_string())?,
                )
            }
            "--phasecenter-field" => {
                phasecenter_field = Some(parse_next(&mut args, "--phasecenter-field")?)
            }
            "--ddid" => ddid = Some(parse_next(&mut args, "--ddid")?),
            "--spw" => spw = Some(parse_next(&mut args, "--spw")?),
            "--channel-start" => channel_start = Some(parse_next(&mut args, "--channel-start")?),
            "--channel-count" => channel_count = Some(parse_next(&mut args, "--channel-count")?),
            "--start" => {
                cube_start = Some(parse_cube_axis_value(&next_value(&mut args, "--start")?)?)
            }
            "--width" => {
                cube_width = Some(parse_cube_axis_value(&next_value(&mut args, "--width")?)?)
            }
            "--chanchunks" => chanchunks = Some(parse_next(&mut args, "--chanchunks")?),
            "--datacolumn" => datacolumn = Some(next_value(&mut args, "--datacolumn")?),
            "--corr" => correlation = Some(next_value(&mut args, "--corr")?),
            "--specmode" => {
                spectral_mode = parse_spectral_mode(&next_value(&mut args, "--specmode")?)?
            }
            "--interpolation" => {
                interpolation =
                    parse_cube_interpolation(&next_value(&mut args, "--interpolation")?)?
            }
            "--weighting" => weighting_name = next_value(&mut args, "--weighting")?,
            "--robust" => robust = parse_next(&mut args, "--robust")?,
            "--perchanweightdensity" => per_channel_weight_density = Some(true),
            "--no-perchanweightdensity" => per_channel_weight_density = Some(false),
            "--usepointing" | "--use-pointing" => use_pointing = true,
            "--deconvolver" => {
                deconvolver = parse_deconvolver(&next_value(&mut args, "--deconvolver")?)?
            }
            "--standard-mfs-acceleration" => {
                standard_mfs_acceleration = parse_standard_mfs_acceleration(&next_value(
                    &mut args,
                    "--standard-mfs-acceleration",
                )?)?
            }
            "--standard-mfs-grid-threads" => {
                standard_mfs_grid_threads =
                    Some(next_value(&mut args, "--standard-mfs-grid-threads")?)
            }
            "--standard-mfs-metal-minor-cycle-chunk" => {
                let value = next_value(&mut args, "--standard-mfs-metal-minor-cycle-chunk")?;
                validate_metal_minor_cycle_chunk(&value)?;
                standard_mfs_metal_minor_cycle_chunk = Some(value);
            }
            "--imaging-memory-target-mb" => {
                imaging_memory_target_mb =
                    Some(parse_next(&mut args, "--imaging-memory-target-mb")?)
            }
            "--imaging-prepare-buffer-mb" => {
                imaging_prepare_buffer_mb =
                    Some(parse_next(&mut args, "--imaging-prepare-buffer-mb")?)
            }
            "--imaging-row-block-rows" => {
                imaging_row_block_rows = Some(parse_next(&mut args, "--imaging-row-block-rows")?)
            }
            "--imaging-prepare-workers" => {
                imaging_prepare_workers = Some(parse_next(&mut args, "--imaging-prepare-workers")?)
            }
            "--imaging-read-ahead-blocks" => {
                imaging_read_ahead_blocks =
                    Some(parse_next(&mut args, "--imaging-read-ahead-blocks")?)
            }
            "--parallel" => parallel = Some(true),
            "--no-parallel" => parallel = Some(false),
            "--imaging-fft-precision" | "--fft-precision" => {
                imaging_fft_precision =
                    parse_imaging_fft_precision(&next_value(&mut args, "--imaging-fft-precision")?)?
            }
            "--imaging-fft-backend" | "--fft-backend" => {
                imaging_fft_backend =
                    parse_imaging_fft_backend(&next_value(&mut args, "--imaging-fft-backend")?)?
            }
            "--nterms" => nterms = parse_next(&mut args, "--nterms")?,
            "--scales" => {
                multiscale_scales = parse_multiscale_scales(&next_value(&mut args, "--scales")?)?
            }
            "--smallscalebias" => small_scale_bias = parse_next(&mut args, "--smallscalebias")?,
            "--imsize" => imsize = parse_next(&mut args, "--imsize")?,
            "--cell-arcsec" => cell_arcsec = parse_next(&mut args, "--cell-arcsec")?,
            "--niter" => niter = parse_next(&mut args, "--niter")?,
            "--gain" => gain = parse_next(&mut args, "--gain")?,
            "--threshold-jy" => threshold_jy = parse_next(&mut args, "--threshold-jy")?,
            "--nsigma" => nsigma = parse_next(&mut args, "--nsigma")?,
            "--psfcutoff" => psf_cutoff = parse_next(&mut args, "--psfcutoff")?,
            "--pblimit" => mosaic_pb_limit = parse_next(&mut args, "--pblimit")?,
            "--pbcor" => {
                pbcor = true;
                write_pb = true;
            }
            "--write-pb" => write_pb = true,
            "--minor-cycle-length" => {
                minor_cycle_length = parse_next(&mut args, "--minor-cycle-length")?
            }
            "--cycleniter" => minor_cycle_length = parse_next(&mut args, "--cycleniter")?,
            "--cyclefactor" => cyclefactor = parse_next(&mut args, "--cyclefactor")?,
            "--minpsffraction" => min_psf_fraction = parse_next(&mut args, "--minpsffraction")?,
            "--maxpsffraction" => max_psf_fraction = parse_next(&mut args, "--maxpsffraction")?,
            "--hogbom-iteration-mode" => {
                hogbom_iteration_mode =
                    parse_hogbom_iteration_mode(&next_value(&mut args, "--hogbom-iteration-mode")?)?
            }
            "--casa-hogbom-iterations" => {
                hogbom_iteration_mode = HogbomIterationMode::CasaInclusive
            }
            "--mask-box" => mask_boxes.push(parse_mask_box(&next_value(&mut args, "--mask-box")?)?),
            "--mask-image" => {
                mask_image = Some(PathBuf::from(next_value(&mut args, "--mask-image")?))
            }
            "--wterm" => w_term_mode = parse_w_term_mode(&next_value(&mut args, "--wterm")?)?,
            "--gridder" => {
                let (mode, force_standard) =
                    parse_gridder_request(&next_value(&mut args, "--gridder")?)?;
                w_term_mode = mode;
                force_standard_gridder = force_standard;
            }
            "--wprojplanes" => w_project_planes = Some(parse_next(&mut args, "--wprojplanes")?),
            "--dirty-only" => dirty_only = true,
            "--repeats" => repeats = parse_next(&mut args, "--repeats")?,
            "--warmups" => warmups = parse_next(&mut args, "--warmups")?,
            value if value.starts_with('-') => {
                return Err(format!("unknown flag {value:?}\n\n{}", help_text()));
            }
            path => {
                if ms.is_some() {
                    return Err(format!(
                        "unexpected extra argument {path:?}\n\n{}",
                        help_text()
                    ));
                }
                ms = Some(PathBuf::from(path));
            }
        }
    }

    let weighting = parse_weighting_mode(&weighting_name, robust)?;
    let per_channel_weight_density =
        per_channel_weight_density.unwrap_or(matches!(spectral_mode, SpectralMode::Cube));
    if parallel == Some(false) && imaging_fft_backend == ImagingFftBackendPolicy::MetalMpsGraph {
        return Err(
            "--no-parallel conflicts with --imaging-fft-backend=metal-mpsgraph".to_string(),
        );
    }

    Ok(Options {
        ms: ms.ok_or_else(help_text)?,
        field_ids,
        phasecenter_field,
        ddid,
        spw,
        channel_start,
        channel_count,
        cube_start,
        cube_width,
        datacolumn,
        correlation,
        spectral_mode,
        interpolation,
        weighting,
        per_channel_weight_density,
        use_pointing,
        deconvolver,
        standard_mfs_acceleration,
        standard_mfs_grid_threads,
        standard_mfs_metal_minor_cycle_chunk,
        imaging_memory_target_mb,
        imaging_prepare_buffer_mb,
        imaging_row_block_rows,
        imaging_prepare_workers,
        imaging_read_ahead_blocks,
        parallel,
        chanchunks,
        imaging_fft_precision,
        imaging_fft_backend,
        nterms,
        multiscale_scales,
        small_scale_bias,
        imsize,
        cell_arcsec,
        niter,
        gain,
        threshold_jy,
        nsigma,
        psf_cutoff,
        mosaic_pb_limit,
        pbcor,
        write_pb,
        minor_cycle_length,
        cyclefactor,
        min_psf_fraction,
        max_psf_fraction,
        hogbom_iteration_mode,
        mask_boxes,
        mask_image,
        w_term_mode,
        force_standard_gridder,
        w_project_planes,
        dirty_only,
        repeats,
        warmups,
    })
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    next_value(args, flag)?
        .parse()
        .map_err(|error| format!("parse {flag}: {error}"))
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_weighting_mode(text: &str, robust: f32) -> Result<WeightingMode, String> {
    match text.to_ascii_lowercase().as_str() {
        "natural" => Ok(WeightingMode::Natural),
        "uniform" => Ok(WeightingMode::Uniform),
        "briggs" | "robust" => Ok(WeightingMode::Briggs { robust }),
        "briggsbwtaper" => Ok(WeightingMode::BriggsBwTaper { robust }),
        _ => Err(format!("unsupported --weighting value {text:?}")),
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

fn parse_gridder_request(text: &str) -> Result<(WTermMode, bool), String> {
    match text.to_ascii_lowercase().as_str() {
        "standard" | "gridft" | "ft" => Ok((WTermMode::None, true)),
        "mosaic" => Ok((WTermMode::None, false)),
        "wproject" => Ok((WTermMode::WProject, false)),
        "widefield" | "awproject" | "awp2" | "awphpg" => {
            eprintln!(
                "aw_widefield_alias_plan gridder_request={} core_projection=wproject a_term_cf_planning=not_implemented remaining_capability_issue=https://github.com/bglenden/casa-rs/issues/52",
                text.to_ascii_lowercase()
            );
            Ok((WTermMode::WProject, false))
        }
        _ => Err(format!(
            "unsupported --gridder value {text:?}; expected standard, wproject, widefield, mosaic, awproject, awp2, or awphpg"
        )),
    }
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

fn parse_cube_axis_value(text: &str) -> Result<CubeAxisValue, String> {
    CubeAxisValue::parse(text, DopplerRef::RADIO).map_err(|error| error.to_string())
}

fn parse_deconvolver(text: &str) -> Result<Deconvolver, String> {
    match text.to_ascii_lowercase().as_str() {
        "hogbom" => Ok(Deconvolver::Hogbom),
        "clark" => Ok(Deconvolver::Clark),
        "multiscale" => Ok(Deconvolver::Multiscale),
        "mtmfs" => Ok(Deconvolver::Mtmfs),
        _ => Err(format!(
            "unsupported --deconvolver value {text:?}; expected hogbom, clark, multiscale, or mtmfs"
        )),
    }
}

fn parse_standard_mfs_acceleration(text: &str) -> Result<StandardMfsAccelerationPolicy, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "" | "auto" | "default" => Ok(StandardMfsAccelerationPolicy::Auto),
        "cpu" | "serial" | "off" | "none" => Ok(StandardMfsAccelerationPolicy::Cpu),
        "multi-cpu" | "multicpu" | "fixed-tile" | "fixed_tile" | "tile" | "tiled" => {
            Ok(StandardMfsAccelerationPolicy::MultiCpu)
        }
        "metal" | "gpu" => Ok(StandardMfsAccelerationPolicy::Metal),
        _ => Err(format!(
            "unsupported --standard-mfs-acceleration value {text:?}; expected auto, cpu, multi-cpu, or metal"
        )),
    }
}

fn parse_imaging_fft_precision(text: &str) -> Result<ImagingFftPrecisionPolicy, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "" | "auto" | "default" => Ok(ImagingFftPrecisionPolicy::Auto),
        "f64" | "double" | "double-precision" | "accurate" => Ok(ImagingFftPrecisionPolicy::F64),
        "f32" | "single" | "single-precision" | "fast" | "fast-f32" | "auto-f32" => {
            Ok(ImagingFftPrecisionPolicy::F32)
        }
        _ => Err(format!(
            "unsupported --imaging-fft-precision value {text:?}; expected auto, f64, or f32"
        )),
    }
}

fn parse_imaging_fft_backend(text: &str) -> Result<ImagingFftBackendPolicy, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "" | "auto" | "default" => Ok(ImagingFftBackendPolicy::Auto),
        "rustfft" | "rust-fft" | "cpu" => Ok(ImagingFftBackendPolicy::RustFft),
        "accelerate" | "vdsp" => Ok(ImagingFftBackendPolicy::Accelerate),
        "metal-mpsgraph" | "mpsgraph" | "mps-graph" | "metal" | "gpu" => {
            Ok(ImagingFftBackendPolicy::MetalMpsGraph)
        }
        _ => Err(format!(
            "unsupported --imaging-fft-backend value {text:?}; expected auto, rustfft, accelerate, or metal-mpsgraph"
        )),
    }
}

fn validate_metal_minor_cycle_chunk(text: &str) -> Result<(), String> {
    let value = text.trim();
    if value.eq_ignore_ascii_case("auto")
        || value.eq_ignore_ascii_case("full")
        || parse_metal_minor_cycle_auto_target_ms(value).is_some()
    {
        return Ok(());
    }
    match value.parse::<usize>() {
        Ok(parsed) if parsed > 0 => Ok(()),
        _ => Err(format!(
            "unsupported --standard-mfs-metal-minor-cycle-chunk value {text:?}; expected auto, auto:<positive-ms>, full, or a positive integer"
        )),
    }
}

fn parse_metal_minor_cycle_auto_target_ms(value: &str) -> Option<f64> {
    let lowercase = value.trim().to_ascii_lowercase();
    let target_ms = lowercase.strip_prefix("auto:")?.parse::<f64>().ok()?;
    target_ms
        .is_finite()
        .then_some(target_ms)
        .filter(|value| *value > 0.0)
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
        .map(str::trim)
        .map(|part| {
            let value = part
                .parse::<f32>()
                .map_err(|error| format!("parse --scales component {part:?}: {error}"))?;
            if value.is_finite() && value >= 0.0 {
                Ok(value)
            } else {
                Err(format!(
                    "parse --scales component {part:?}: expected finite non-negative value"
                ))
            }
        })
        .collect()
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

fn help_text() -> String {
    "Usage: profile_imager <ms-path> [options]

Options:
  --field IDS
  --phasecenter-field ID
  --ddid ID
  --spw ID
  --channel-start N
  --channel-count N
  --start VALUE
  --width VALUE
  --chanchunks N
  --datacolumn NAME
  --corr XX|YY|RR|LL
  --specmode mfs|cube|cubedata
  --interpolation nearest|linear
  --weighting natural|uniform|briggs|briggsbwtaper
  --robust VALUE
  --perchanweightdensity
  --no-perchanweightdensity
  --usepointing
  --deconvolver hogbom|clark|multiscale|mtmfs
  --standard-mfs-acceleration auto|cpu|multi-cpu|metal
  --standard-mfs-grid-threads N|auto
  --standard-mfs-metal-minor-cycle-chunk auto|auto:MS|full|N
  --imaging-fft-precision auto|f64|f32
  --imaging-fft-backend auto|rustfft|accelerate|metal-mpsgraph
  --imaging-memory-target-mb N
  --imaging-prepare-buffer-mb N
  --imaging-row-block-rows N
  --imaging-prepare-workers N
  --imaging-read-ahead-blocks N
  --parallel
  --no-parallel
  --nterms N
  --scales PIXELS
  --smallscalebias VALUE
  --imsize N
  --cell-arcsec ARCSEC
  --niter N
  --gain VALUE
  --threshold-jy VALUE
  --nsigma VALUE
  --psfcutoff VALUE
  --pblimit VALUE
  --write-pb
  --pbcor
  --minor-cycle-length N
  --cycleniter N
  --cyclefactor VALUE
  --minpsffraction VALUE
  --maxpsffraction VALUE
  --hogbom-iteration-mode strict|casa
  --casa-hogbom-iterations
  --mask-box X0,Y0,X1,Y1
  --mask-image PATH
  --wterm none|direct|wproject
  --gridder standard|mosaic|wproject
  --wprojplanes N
  --dirty-only
  --repeats N
  --warmups N
"
    .to_string()
}
