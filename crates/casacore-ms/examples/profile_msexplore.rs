// SPDX-License-Identifier: LGPL-3.0-or-later
//! Profile `msexplore` in pipeline stages on a real MeasurementSet.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release -p casacore-ms --example profile_msexplore -- \
//!   /path/to.ms --preset amplitude_vs_time --spw 0 --iteraxis scan --gridcols 2
//! ```

use std::env;
use std::hint::black_box;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::Instant;

use casacore_ms::{
    MeasurementSet, MeasurementSetPlotTheme, MeasurementSetSummary,
    MeasurementSetSummaryOutputFormat, MsColorAxis, MsIterationAxis, MsPlotPayload, MsPlotPreset,
    MsPlotSpec, MsSelectionSpec, build_msexplore_plot_payload, render_msexplore_plot_image,
};
use image::ImageFormat;

#[derive(Debug, Clone)]
struct Options {
    ms_path: PathBuf,
    stage: ProfileStage,
    preset: MsPlotPreset,
    field: Option<String>,
    spw: Option<String>,
    scan: Option<String>,
    correlation: Option<String>,
    msselect: Option<String>,
    color_by: MsColorAxis,
    iteraxis: Option<MsIterationAxis>,
    gridrows: usize,
    gridcols: usize,
    xselfscale: bool,
    yselfscale: bool,
    xsharedaxis: bool,
    ysharedaxis: bool,
    plot_width: u32,
    plot_height: u32,
    repeats: usize,
    warmups: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProfileStage {
    Pipeline,
    Build,
    Render,
    Full,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
    let selection = MsSelectionSpec {
        field: options.field.clone(),
        spw: options.spw.clone(),
        scan: options.scan.clone(),
        correlation: options.correlation.clone(),
        msselect: options.msselect.clone(),
        ..MsSelectionSpec::default()
    };
    let mut spec = MsPlotSpec::from_preset(options.preset);
    spec.color_by = options.color_by;
    spec.layout.gridrows = options.gridrows.max(1);
    spec.layout.gridcols = options.gridcols.max(1);
    spec.iteration.iteraxis = options.iteraxis;
    spec.iteration.xselfscale = options.xselfscale;
    spec.iteration.yselfscale = options.yselfscale;
    spec.iteration.xsharedaxis = options.xsharedaxis;
    spec.iteration.ysharedaxis = options.ysharedaxis;
    spec.validate()?;

    println!(
        "ms={} stage={} preset={} color_by={} iteraxis={} grid={}x{} repeats={} warmups={}",
        options.ms_path.display(),
        options.stage.as_str(),
        spec.preset
            .map(|preset| preset.as_str().to_string())
            .unwrap_or_else(|| "custom".to_string()),
        spec.color_by,
        spec.iteration
            .iteraxis
            .map(|axis| axis.as_str().to_string())
            .unwrap_or_else(|| "none".to_string()),
        spec.layout.gridrows,
        spec.layout.gridcols,
        options.repeats,
        options.warmups,
    );
    println!(
        "selection field={:?} spw={:?} scan={:?} correlation={:?} msselect={:?}",
        selection.field, selection.spw, selection.scan, selection.correlation, selection.msselect
    );
    println!(
        "render size={}x{} xselfscale={} yselfscale={} xsharedaxis={} ysharedaxis={}",
        options.plot_width,
        options.plot_height,
        options.xselfscale,
        options.yselfscale,
        options.xsharedaxis,
        options.ysharedaxis
    );

    let open = measure("open_ms", options.repeats, options.warmups, || {
        let ms = MeasurementSet::open(&options.ms_path).map_err(|error| error.to_string())?;
        Ok(ms.row_count())
    })?;

    let open_ms = MeasurementSet::open(&options.ms_path).map_err(|error| error.to_string())?;
    let summary = measure(
        "summary_from_open_ms",
        options.repeats,
        options.warmups,
        || {
            let summary = MeasurementSetSummary::from_ms_with_options(
                &open_ms,
                &selection.to_summary_options(),
            )
            .map_err(|error| error.to_string())?;
            let rendered = summary
                .render(MeasurementSetSummaryOutputFormat::Json)
                .map_err(|error| error.to_string())?;
            Ok(rendered.len())
        },
    )?;

    let payload = build_msexplore_plot_payload(&open_ms, &selection, &spec)?;
    let payload_points = payload_point_count(&payload);
    let payload_panels = payload_panel_count(&payload);
    println!(
        "payload kind={} panels={} grid={} points={payload_points}",
        payload_kind(&payload),
        payload_panels,
        payload_grid(&payload),
    );

    if options.stage == ProfileStage::Build {
        let build = measure("build_payload", options.repeats, options.warmups, || {
            let payload = build_msexplore_plot_payload(&open_ms, &selection, &spec)?;
            Ok(payload_point_count(&payload))
        })?;
        println!();
        println!("build-only median={:.2} ms", build.median_ms);
        return Ok(());
    }

    if options.stage == ProfileStage::Render {
        let render = measure("render_bitmap", options.repeats, options.warmups, || {
            let image = render_msexplore_plot_image(
                &payload,
                MeasurementSetPlotTheme::light(),
                options.plot_width,
                options.plot_height,
            )?;
            Ok((image.width() as usize) * (image.height() as usize))
        })?;
        println!();
        println!("render-only median={:.2} ms", render.median_ms);
        return Ok(());
    }

    let build = measure("build_payload", options.repeats, options.warmups, || {
        let payload = build_msexplore_plot_payload(&open_ms, &selection, &spec)?;
        Ok(payload_point_count(&payload))
    })?;

    let render = measure("render_bitmap", options.repeats, options.warmups, || {
        let image = render_msexplore_plot_image(
            &payload,
            MeasurementSetPlotTheme::light(),
            options.plot_width,
            options.plot_height,
        )?;
        Ok((image.width() as usize) * (image.height() as usize))
    })?;

    let rendered = render_msexplore_plot_image(
        &payload,
        MeasurementSetPlotTheme::light(),
        options.plot_width,
        options.plot_height,
    )?;
    let encode = measure(
        "encode_png_in_memory",
        options.repeats,
        options.warmups,
        || {
            let mut bytes = Vec::new();
            rendered
                .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
                .map_err(|error| error.to_string())?;
            Ok(bytes.len())
        },
    )?;

    let full = measure(
        "full_pipeline_in_memory",
        options.repeats,
        options.warmups,
        || {
            let ms = MeasurementSet::open(&options.ms_path).map_err(|error| error.to_string())?;
            let summary =
                MeasurementSetSummary::from_ms_with_options(&ms, &selection.to_summary_options())
                    .map_err(|error| error.to_string())?;
            let rendered_summary = summary
                .render(MeasurementSetSummaryOutputFormat::Json)
                .map_err(|error| error.to_string())?;
            let payload = build_msexplore_plot_payload(&ms, &selection, &spec)?;
            let image = render_msexplore_plot_image(
                &payload,
                MeasurementSetPlotTheme::light(),
                options.plot_width,
                options.plot_height,
            )?;
            let mut bytes = Vec::new();
            image
                .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
                .map_err(|error| error.to_string())?;
            Ok(rendered_summary.len() ^ bytes.len() ^ payload_point_count(&payload))
        },
    )?;

    if options.stage == ProfileStage::Full {
        println!();
        println!("full-only median={:.2} ms", full.median_ms);
        return Ok(());
    }

    println!();
    println!(
        "stage shares of full pipeline: open={:.1}% summary={:.1}% build={:.1}% render={:.1}% encode={:.1}%",
        percent(open.median_ms, full.median_ms),
        percent(summary.median_ms, full.median_ms),
        percent(build.median_ms, full.median_ms),
        percent(render.median_ms, full.median_ms),
        percent(encode.median_ms, full.median_ms),
    );
    println!(
        "render_vs_library ratio: render / (open + summary + build) = {:.2}x",
        render.median_ms / (open.median_ms + summary.median_ms + build.median_ms).max(0.001)
    );

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct Timing {
    median_ms: f64,
}

fn measure(
    label: &str,
    repeats: usize,
    warmups: usize,
    mut run_once: impl FnMut() -> Result<usize, String>,
) -> Result<Timing, String> {
    for _ in 0..warmups {
        black_box(run_once()?);
    }
    let mut samples = Vec::with_capacity(repeats);
    let mut sink = 0usize;
    for _ in 0..repeats {
        let start = Instant::now();
        sink ^= run_once()?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    black_box(sink);
    samples.sort_by(|left, right| left.total_cmp(right));
    let median_ms = if samples.is_empty() {
        0.0
    } else {
        samples[samples.len() / 2]
    };
    let min_ms = samples.first().copied().unwrap_or(0.0);
    let max_ms = samples.last().copied().unwrap_or(0.0);
    println!(
        "{label:24} median {:9.2} ms   min {:9.2} ms   max {:9.2} ms",
        median_ms, min_ms, max_ms
    );
    Ok(Timing { median_ms })
}

fn percent(part_ms: f64, whole_ms: f64) -> f64 {
    if whole_ms <= 0.0 {
        0.0
    } else {
        100.0 * part_ms / whole_ms
    }
}

fn payload_kind(payload: &MsPlotPayload) -> &'static str {
    match payload {
        MsPlotPayload::ListObs(_) => "metadata",
        MsPlotPayload::Scatter(_) => "scatter",
        MsPlotPayload::ScatterGrid(_) => "scatter_grid",
        MsPlotPayload::ScatterPage(_) => "scatter_page",
    }
}

fn payload_panel_count(payload: &MsPlotPayload) -> usize {
    match payload {
        MsPlotPayload::ListObs(_) | MsPlotPayload::Scatter(_) => 1,
        MsPlotPayload::ScatterGrid(payload) => payload.panels.len(),
        MsPlotPayload::ScatterPage(payload) => payload.items.len(),
    }
}

fn payload_point_count(payload: &MsPlotPayload) -> usize {
    match payload {
        MsPlotPayload::ListObs(_) => 0,
        MsPlotPayload::Scatter(payload) => payload
            .series
            .iter()
            .map(|series| series.points.len())
            .sum(),
        MsPlotPayload::ScatterGrid(payload) => payload
            .panels
            .iter()
            .flat_map(|panel| panel.series.iter())
            .map(|series| series.points.len())
            .sum(),
        MsPlotPayload::ScatterPage(payload) => payload
            .items
            .iter()
            .flat_map(|item| item.plot.series.iter())
            .map(|series| series.points.len())
            .sum(),
    }
}

fn payload_grid(payload: &MsPlotPayload) -> String {
    match payload {
        MsPlotPayload::ListObs(_) | MsPlotPayload::Scatter(_) => "1x1".to_string(),
        MsPlotPayload::ScatterGrid(payload) => format!("{}x{}", payload.gridrows, payload.gridcols),
        MsPlotPayload::ScatterPage(payload) => format!("{}x{}", payload.gridrows, payload.gridcols),
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_help();
        std::process::exit(0);
    }

    let mut index = 0usize;
    let mut ms_path = None;
    let mut stage = ProfileStage::Pipeline;
    let mut preset = MsPlotPreset::AmplitudeVsTime;
    let mut field = None;
    let mut spw = None;
    let mut scan = None;
    let mut correlation = None;
    let mut msselect = None;
    let mut color_by = MsColorAxis::Field;
    let mut iteraxis = None;
    let mut gridrows = 1usize;
    let mut gridcols = 1usize;
    let mut xselfscale = false;
    let mut yselfscale = false;
    let mut xsharedaxis = false;
    let mut ysharedaxis = false;
    let mut plot_width = 1600u32;
    let mut plot_height = 900u32;
    let mut repeats = 5usize;
    let mut warmups = 1usize;

    while index < args.len() {
        let raw = &args[index];
        let take_value =
            |index: &mut usize, args: &[String], flag: &str| -> Result<String, String> {
                *index += 1;
                args.get(*index)
                    .cloned()
                    .ok_or_else(|| format!("missing value for {flag}"))
            };
        match raw.as_str() {
            "--stage" => stage = ProfileStage::parse(&take_value(&mut index, &args, "--stage")?)?,
            "--preset" => {
                preset = MsPlotPreset::parse(&take_value(&mut index, &args, "--preset")?)?
            }
            "--field" => field = Some(take_value(&mut index, &args, "--field")?),
            "--spw" => spw = Some(take_value(&mut index, &args, "--spw")?),
            "--scan" => scan = Some(take_value(&mut index, &args, "--scan")?),
            "--correlation" => correlation = Some(take_value(&mut index, &args, "--correlation")?),
            "--msselect" => msselect = Some(take_value(&mut index, &args, "--msselect")?),
            "--color-by" => {
                color_by = MsColorAxis::parse(&take_value(&mut index, &args, "--color-by")?)?
            }
            "--iteraxis" => {
                iteraxis = Some(MsIterationAxis::parse(&take_value(
                    &mut index,
                    &args,
                    "--iteraxis",
                )?)?)
            }
            "--gridrows" => {
                gridrows = take_value(&mut index, &args, "--gridrows")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --gridrows".to_string())?
                    .max(1)
            }
            "--gridcols" => {
                gridcols = take_value(&mut index, &args, "--gridcols")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --gridcols".to_string())?
                    .max(1)
            }
            "--xselfscale" => xselfscale = true,
            "--yselfscale" => yselfscale = true,
            "--xsharedaxis" => xsharedaxis = true,
            "--ysharedaxis" => ysharedaxis = true,
            "--plot-width" => {
                plot_width = take_value(&mut index, &args, "--plot-width")?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer value for --plot-width".to_string())?
                    .max(1)
            }
            "--plot-height" => {
                plot_height = take_value(&mut index, &args, "--plot-height")?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer value for --plot-height".to_string())?
                    .max(1)
            }
            "--repeats" => {
                repeats = take_value(&mut index, &args, "--repeats")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --repeats".to_string())?
                    .max(1)
            }
            "--warmups" => {
                warmups = take_value(&mut index, &args, "--warmups")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --warmups".to_string())?
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option {value:?}"));
            }
            value => {
                if ms_path.is_some() {
                    return Err("expected exactly one MeasurementSet path".to_string());
                }
                ms_path = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }

    Ok(Options {
        ms_path: ms_path.ok_or_else(|| "missing required MeasurementSet path".to_string())?,
        stage,
        preset,
        field,
        spw,
        scan,
        correlation,
        msselect,
        color_by,
        iteraxis,
        gridrows,
        gridcols,
        xselfscale,
        yselfscale,
        xsharedaxis,
        ysharedaxis,
        plot_width,
        plot_height,
        repeats,
        warmups,
    })
}

fn print_help() {
    println!(
        "profile_msexplore - profile msexplore pipeline stages\n\
\n\
Usage:\n\
  profile_msexplore <ms-path> [OPTIONS]\n\
\n\
Options:\n\
  --stage <MODE>            Stage subset: pipeline, build, render, full (default: pipeline)\n\
  --preset <PRESET>          Plot preset to profile (default: amplitude_vs_time)\n\
  --field <EXPR>             FIELD selection\n\
  --spw <EXPR>               SPW selection\n\
  --scan <EXPR>              SCAN selection\n\
  --correlation <EXPR>       CORRELATION selection\n\
  --msselect <EXPR>          Raw MSSelection expression\n\
  --color-by <AXIS>          Color grouping axis (default: field)\n\
  --iteraxis <AXIS>          Iteration axis\n\
  --gridrows <N>             Grid rows for iterated plots (default: 1)\n\
  --gridcols <N>             Grid cols for iterated plots (default: 1)\n\
  --xselfscale               Use self-scaled X bounds per panel\n\
  --yselfscale               Use self-scaled Y bounds per panel\n\
  --xsharedaxis              Force shared X bounds across panels\n\
  --ysharedaxis              Force shared Y bounds across panels\n\
  --plot-width <PIXELS>      Render width (default: 1600)\n\
  --plot-height <PIXELS>     Render height (default: 900)\n\
  --repeats <N>              Timed repeats (default: 5)\n\
  --warmups <N>              Warmup repeats (default: 1)\n\
  -h, --help                 Show this help\n"
    );
}

impl ProfileStage {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pipeline" => Ok(Self::Pipeline),
            "build" => Ok(Self::Build),
            "render" => Ok(Self::Render),
            "full" => Ok(Self::Full),
            _ => Err(format!("invalid value for --stage: {value}")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Pipeline => "pipeline",
            Self::Build => "build",
            Self::Render => "render",
            Self::Full => "full",
        }
    }
}
