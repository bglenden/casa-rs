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
use std::ffi::OsString;
use std::fs;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::path::PathBuf;
use std::time::Duration;

use casa_imaging::fft_backend::wall_to_io_ratio;
use casars_imager::{CliConfig, ImagerRunTaskRequest, RunSummary, run_from_request};

#[derive(Debug, Clone)]
struct Options {
    config: CliConfig,
    repeats: usize,
    warmups: usize,
}

impl Deref for Options {
    type Target = CliConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
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
        "ms={} field_ids={:?} phasecenter_field={:?} ddid={:?} spw={:?} spw_selector={:?} channel_start={:?} channel_count={:?} cube_start={:?} cube_width={:?} corr={:?} interpolation={:?} weighting={:?} use_pointing={} deconvolver={:?} nterms={} scales={:?} wterm={:?} wprojplanes={:?} imsize={} cell_arcsec={} dirty_only={} niter={} repeats={} warmups={}",
        options.ms.display(),
        options.field_ids,
        options.phasecenter_field,
        options.ddid,
        options.spw,
        options.spw_selector,
        options.channel_start,
        options.channel_count,
        options.cube_axis.start,
        options.cube_axis.width,
        options.correlation,
        options.cube_axis.interpolation,
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
    let mut config = options.config.clone();
    config.imagename = imagename;
    config.write_preview_pngs = false;
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
        "standard_mfs_profile_run run={} workload_ms={} field_ids={:?} phasecenter_field={:?} ddid={:?} spw={:?} spw_selector={:?} channel_start={:?} channel_count={:?} spectral_mode={:?} weighting={:?} deconvolver={:?} nterms={} imsize={} niter={} dirty_only={} gridded_samples={} major_cycles={} minor_iterations={} grid_threads={:?} row_block_rows={:?} prepare_workers={:?} read_ahead_blocks={:?} memory_target_mb={:?} frontend_total_ms={:.3} io_time_ms={:.3} wall_to_io_ratio={} core_total_ms={:.3} prepare_plane_input_ms={:.3} source_read_ms={:.3} source_prepare_ms={:.3} weighting_ms={:.3} executor_build_ms={:.3} psf_grid_ms={:.3} residual_degrid_grid_ms={:.3} major_cycle_refresh_ms={:.3} peak_rss_bytes={} product_status=written",
        run_number,
        options.ms.display(),
        options.field_ids,
        options.phasecenter_field,
        options.ddid,
        options.spw,
        options.spw_selector,
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
    let mut arguments = args.into_iter();
    let ms = arguments.next().ok_or_else(help_text)?;
    if matches!(ms.as_str(), "--help" | "-h") {
        return Err(help_text());
    }
    if ms.starts_with('-') {
        return Err(format!(
            "the MeasurementSet path must be the first argument, found {ms:?}\n\n{}",
            help_text()
        ));
    }

    let mut repeats = 5usize;
    let mut warmups = 1usize;
    let mut cli_args = Vec::<OsString>::new();
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Err(help_text()),
            "--repeats" => {
                repeats = parse_profile_count(arguments.next(), "--repeats")?;
            }
            "--warmups" => {
                warmups = parse_profile_count(arguments.next(), "--warmups")?;
            }
            "--ms" | "--imagename" => {
                return Err(format!(
                    "{argument} is managed by profile_imager and cannot be supplied\n\n{}",
                    help_text()
                ));
            }
            _ => cli_args.push(argument.into()),
        }
    }

    if repeats == 0 {
        return Err("--repeats must be positive".to_string());
    }

    inject_default_option(&mut cli_args, &["--imsize"], "--imsize", "128");
    inject_default_option(&mut cli_args, &["--cell-arcsec"], "--cell-arcsec", "30");
    inject_default_option(&mut cli_args, &["--datacolumn"], "--datacolumn", "DATA");
    inject_default_option(
        &mut cli_args,
        &["--minor-cycle-length", "--cycleniter"],
        "--minor-cycle-length",
        "2",
    );
    inject_default_option(
        &mut cli_args,
        &["--minpsffraction"],
        "--minpsffraction",
        "0.1",
    );

    cli_args.extend([
        OsString::from("--ms"),
        OsString::from(ms),
        OsString::from("--imagename"),
        env::temp_dir()
            .join("casars-imager-profile-placeholder")
            .into_os_string(),
    ]);
    let config = CliConfig::parse(cli_args)?;
    Ok(Options {
        config,
        repeats,
        warmups,
    })
}

fn parse_profile_count(value: Option<String>, flag: &str) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse()
        .map_err(|error| format!("parse {flag}: {error}"))
}

fn inject_default_option(
    arguments: &mut Vec<OsString>,
    aliases: &[&str],
    option: &str,
    value: &str,
) {
    if arguments
        .iter()
        .any(|argument| aliases.iter().any(|alias| argument == alias))
    {
        return;
    }
    arguments.extend([OsString::from(option), OsString::from(value)]);
}

fn help_text() -> String {
    "Usage: profile_imager <ms-path> [options]

Profiler options:
  --repeats N              measured runs (default 5)
  --warmups N              unmeasured warmup runs (default 1)

All production casars-imager imaging options are accepted unchanged, including
multi-SPW selection and true AWProject controls. The profiler supplies --ms,
--imagename, and legacy profiling defaults for imsize, cell size, data column,
minor-cycle length, and minpsffraction when those options are omitted.
"
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_args;

    #[test]
    fn production_parser_preserves_multi_spw_awproject_controls() {
        let options = parse_args(
            [
                "/tmp/fixture.ms",
                "--imsize",
                "4096",
                "--cell-arcsec",
                "0.6",
                "--field",
                "1107~1127,1512~1532",
                "--spw",
                "2~17",
                "--specmode",
                "mfs",
                "--deconvolver",
                "mtmfs",
                "--nterms",
                "2",
                "--gridder",
                "awproject",
                "--cfcache",
                "/tmp/cf-cache",
                "--cf-resident-mb",
                "384",
                "--wprojplanes",
                "32",
                "--usepointing",
                "--pointingoffsetsigdev",
                "0",
                "--repeats",
                "3",
                "--warmups",
                "0",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("parse profiler request");

        assert_eq!(options.spw_selector.as_deref(), Some("2~17"));
        let fields = options.field_ids.as_deref().expect("field selection");
        assert_eq!(fields.len(), 42);
        assert_eq!(fields.first(), Some(&1107));
        assert_eq!(fields.last(), Some(&1532));
        let aw = options.aw_project.as_ref().expect("AWProject controls");
        assert_eq!(aw.cf_resident_bytes, 384 * 1024 * 1024);
        assert_eq!(aw.w_plane_count, Some(32));
        assert!(aw.use_pointing);
        assert_eq!(options.repeats, 3);
        assert_eq!(options.warmups, 0);
    }
}
