// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for `msexplore`.

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use super::{
    MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec, MsExportFormat, MsIterationAxis,
    MsPageExportRange, MsPlotPreset, MsPlotSpec, MsPlotStyleSpec, MsSelectionSpec,
    build_msexplore_payload, export_msexplore_plot,
};
use crate::MeasurementSet;
use crate::listobs::cli::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiInjectedArgument,
    UiManagedOutputSchema, UiValueKind,
};
use crate::{ListObsOutputFormat, ListObsSummary};

const UI_SCHEMA_VERSION: u32 = 1;
const COMMAND_ID: &str = "msexplore";
const DISPLAY_NAME: &str = "MSExplore";
const CATEGORY: &str = "MeasurementSet";
const SUMMARY: &str = "explore and export common MeasurementSet plotms-style plots";

#[derive(Debug)]
enum CliAction {
    Help,
    UiSchema,
    Run(CliOptions),
}

#[derive(Debug)]
struct CliOptions {
    ms_path: PathBuf,
    summary_format: ListObsOutputFormat,
    summary_output: Option<PathBuf>,
    overwrite: bool,
    selection: MsSelectionSpec,
    page_spec: Option<PathBuf>,
    preset: Option<MsPlotPreset>,
    x_axis: Option<MsAxis>,
    y_axis: Option<MsAxis>,
    y_axis2: Option<MsAxis>,
    data_column: MsDataColumn,
    color_by: MsColorAxis,
    avgchannel: Option<usize>,
    scalar: bool,
    freqframe: Option<String>,
    restfreq: Option<String>,
    veldef: String,
    iteraxis: Option<MsIterationAxis>,
    gridrows: usize,
    gridcols: usize,
    xselfscale: bool,
    yselfscale: bool,
    xsharedaxis: bool,
    ysharedaxis: bool,
    title: Option<String>,
    xlabel: Option<String>,
    ylabel: Option<String>,
    showlegend: bool,
    showmajorgrid: bool,
    showminorgrid: bool,
    plot_output: Option<PathBuf>,
    plot_format: MsExportFormat,
    plot_width: u32,
    plot_height: u32,
}

#[derive(Debug, Deserialize)]
struct CliPageSpecFile {
    #[serde(default = "one")]
    gridrows: usize,
    #[serde(default = "one")]
    gridcols: usize,
    #[serde(default)]
    page_title: Option<String>,
    #[serde(default)]
    exprange: MsPageExportRange,
    plots: Vec<CliPagePlotSpec>,
}

#[derive(Debug, Deserialize)]
struct CliPagePlotSpec {
    #[serde(default)]
    preset: Option<MsPlotPreset>,
    #[serde(default)]
    x_axis: Option<MsAxis>,
    #[serde(default)]
    y_axis: Option<MsAxis>,
    #[serde(default)]
    y_axis2: Option<MsAxis>,
    #[serde(default)]
    data_column: Option<MsDataColumn>,
    #[serde(default)]
    color_by: Option<MsColorAxis>,
    #[serde(default)]
    avgchannel: Option<usize>,
    #[serde(default)]
    scalar: bool,
    #[serde(default)]
    freqframe: Option<String>,
    #[serde(default)]
    restfreq: Option<String>,
    #[serde(default)]
    veldef: Option<String>,
    #[serde(default)]
    rowindex: usize,
    #[serde(default)]
    colindex: usize,
    #[serde(default)]
    plotindex: usize,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    xlabel: Option<String>,
    #[serde(default)]
    ylabel: Option<String>,
    #[serde(default)]
    showlegend: bool,
    #[serde(default)]
    showmajorgrid: bool,
    #[serde(default)]
    showminorgrid: bool,
}

const fn one() -> usize {
    1
}

/// Parse environment arguments, run `msexplore`, and return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    let schema = command_schema(program_name);
    match parse_args(std::env::args_os().skip(1)) {
        Ok(CliAction::Help) => {
            print!("{}", schema.render_help());
            0
        }
        Ok(CliAction::UiSchema) => match schema.render_json_pretty() {
            Ok(json) => {
                print!("{json}");
                0
            }
            Err(error) => {
                eprintln!("Error: failed to serialize --ui-schema output: {error}");
                1
            }
        },
        Ok(CliAction::Run(options)) => match run(options) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        },
        Err(error) => {
            eprintln!("Error: {error}\n");
            eprintln!("{}", schema.render_help());
            1
        }
    }
}

/// Build the machine-readable command schema for `msexplore`.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: UI_SCHEMA_VERSION,
        command_id: COMMAND_ID.to_string(),
        invocation_name: program_name.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        category: CATEGORY.to_string(),
        summary: SUMMARY.to_string(),
        usage: format!("{program_name} [OPTIONS] <ms-path>"),
        arguments: vec![
            positional_argument(
                "ms_path",
                "MeasurementSet Path",
                0,
                "ms-path",
                UiValueKind::Path,
                "Path to the MeasurementSet root directory",
                "Input",
            ),
            option_argument(
                "format",
                "Output Format",
                1,
                &["--format"],
                "FORMAT",
                UiValueKind::Choice,
                Some("text"),
                &["text", "json"],
                "Summary output format",
                "Output",
                false,
                true,
            ),
            option_argument(
                "output",
                "Output Path",
                2,
                &["-o", "--output"],
                "PATH",
                UiValueKind::Path,
                None,
                &[],
                "Write the summary output to PATH",
                "Output",
                true,
                false,
            ),
            toggle_argument(
                "overwrite",
                "Overwrite Output",
                3,
                &["--overwrite"],
                &[],
                false,
                "Replace an existing output file",
                "Output",
                true,
                false,
            ),
            toggle_argument(
                "selectdata",
                "Apply Selection",
                4,
                &["--selectdata"],
                &["--no-selectdata"],
                true,
                "Apply row-selection controls",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "field",
                "Field",
                5,
                &["--field"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select field ids, names, or globs",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "spw",
                "Spectral Window",
                6,
                &["--spw"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select spectral-window ids or ranges",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "timerange",
                "Time Range",
                7,
                &["--timerange"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select rows by CASA-style UTC time expressions",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "uvrange",
                "UV Range",
                8,
                &["--uvrange"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select UV-distance ranges in m/lambda units",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "antenna",
                "Antenna",
                9,
                &["--antenna"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select antenna ids, names, or exact baselines a&&b",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "scan",
                "Scan",
                10,
                &["--scan"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select scan numbers or ranges",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "correlation",
                "Correlation",
                11,
                &["--correlation"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select correlation products such as XX,YY",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "observation",
                "Observation",
                12,
                &["--observation"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select observation ids or ranges",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "array",
                "Array",
                13,
                &["--array"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select array ids or ranges",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "intent",
                "Intent",
                14,
                &["--intent"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select intents by exact name or simple '*' globs",
                "Selection",
                false,
                false,
            ),
            option_argument(
                "feed",
                "Feed",
                15,
                &["--feed"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Select feed ids or ranges",
                "Selection",
                true,
                false,
            ),
            option_argument(
                "msselect",
                "MSSelect",
                16,
                &["--msselect"],
                "EXPR",
                UiValueKind::String,
                None,
                &[],
                "Raw TaQL/MSSelection expression",
                "Selection",
                true,
                false,
            ),
            option_argument(
                "page_spec",
                "Page Spec",
                17,
                &["--page-spec"],
                "PATH",
                UiValueKind::Path,
                None,
                &[],
                "Load a JSON multi-plot page spec instead of the single-plot axis/preset flags",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "preset",
                "Preset",
                17,
                &["--preset"],
                "PRESET",
                UiValueKind::Choice,
                None,
                &[
                    "uv_coverage",
                    "antenna_layout",
                    "scan_timeline",
                    "spectral_window_coverage",
                    "amplitude_vs_time",
                    "phase_vs_time",
                    "amplitude_phase_vs_time_stacked",
                    "amplitude_vs_uv_distance",
                    "weight_vs_time",
                    "sigma_vs_time",
                    "flag_vs_time",
                    "weight_spectrum_vs_time",
                    "sigma_spectrum_vs_time",
                    "flagrow_vs_time",
                    "elevation_vs_time",
                    "azimuth_vs_time",
                    "hour_angle_vs_time",
                    "parallactic_angle_vs_time",
                    "azimuth_vs_elevation",
                    "amplitude_vs_channel",
                    "phase_vs_channel",
                    "amplitude_vs_frequency",
                    "phase_vs_frequency",
                    "amplitude_vs_velocity",
                    "phase_vs_velocity",
                    "real_vs_imaginary",
                ],
                "Use a named common plot preset",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "x_axis",
                "X Axis",
                18,
                &["--xaxis"],
                "AXIS",
                UiValueKind::String,
                None,
                &[],
                "Explicit x axis when not using a preset",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "y_axis",
                "Y Axis",
                19,
                &["--yaxis"],
                "AXIS",
                UiValueKind::String,
                None,
                &[],
                "Explicit y axis when not using a preset",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "y_axis2",
                "Second Y Axis",
                20,
                &["--yaxis2"],
                "AXIS",
                UiValueKind::String,
                None,
                &[],
                "Optional second y axis rendered on the right side",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "data_column",
                "Data Column",
                21,
                &["--data-column"],
                "COLUMN",
                UiValueKind::String,
                Some("data"),
                &[],
                "Visibility data column or derived expression",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "color_by",
                "Color By",
                22,
                &["--color-by"],
                "AXIS",
                UiValueKind::Choice,
                Some("field"),
                &["none", "field", "scan", "spw", "baseline", "correlation"],
                "Metadata axis used to group series colors",
                "Plot",
                false,
                false,
            ),
            option_argument(
                "avgchannel",
                "Average Channels",
                23,
                &["--avgchannel"],
                "N",
                UiValueKind::Float,
                None,
                &[],
                "Channel bin size for channel/frequency plots",
                "Averaging",
                false,
                false,
            ),
            toggle_argument(
                "scalar",
                "Scalar Average",
                24,
                &["--scalar"],
                &[],
                false,
                "Use scalar averaging instead of vector averaging",
                "Averaging",
                false,
                false,
            ),
            option_argument(
                "freqframe",
                "Frequency Frame",
                24,
                &["--freqframe"],
                "FRAME",
                UiValueKind::Choice,
                None,
                &[
                    "LSRK", "LSRD", "BARY", "GEO", "TOPO", "GALACTO", "LGROUP", "CMB",
                ],
                "Render frequency and velocity axes in the requested frame",
                "Transforms",
                false,
                false,
            ),
            option_argument(
                "restfreq",
                "Rest Frequency",
                25,
                &["--restfreq"],
                "FREQ",
                UiValueKind::String,
                None,
                &[],
                "Rest frequency for velocity rendering; empty uses the SPW center frequency",
                "Transforms",
                false,
                false,
            ),
            option_argument(
                "veldef",
                "Velocity Definition",
                26,
                &["--veldef"],
                "DEF",
                UiValueKind::Choice,
                Some("RADIO"),
                &["RADIO", "OPTICAL", "TRUE"],
                "Velocity definition used for velocity axes",
                "Transforms",
                false,
                false,
            ),
            option_argument(
                "iteraxis",
                "Iterate By",
                27,
                &["--iteraxis"],
                "AXIS",
                UiValueKind::Choice,
                None,
                &["field", "scan", "spw", "correlation"],
                "Split the plot into one panel per iteraxis value",
                "Layout",
                false,
                false,
            ),
            option_argument(
                "gridrows",
                "Grid Rows",
                28,
                &["--gridrows"],
                "N",
                UiValueKind::Float,
                Some("1"),
                &[],
                "Requested grid row count for iterated plots",
                "Layout",
                false,
                false,
            ),
            option_argument(
                "gridcols",
                "Grid Columns",
                29,
                &["--gridcols"],
                "N",
                UiValueKind::Float,
                Some("1"),
                &[],
                "Requested grid column count for iterated plots",
                "Layout",
                false,
                false,
            ),
            toggle_argument(
                "xselfscale",
                "X Self Scale",
                30,
                &["--xselfscale"],
                &[],
                false,
                "Use per-panel X bounds on iterated plots",
                "Layout",
                false,
                false,
            ),
            toggle_argument(
                "yselfscale",
                "Y Self Scale",
                31,
                &["--yselfscale"],
                &[],
                false,
                "Use per-panel Y bounds on iterated plots",
                "Layout",
                false,
                false,
            ),
            toggle_argument(
                "xsharedaxis",
                "Share X Axis",
                32,
                &["--xsharedaxis"],
                &[],
                false,
                "Force a shared X axis across iterated panels",
                "Layout",
                false,
                false,
            ),
            toggle_argument(
                "ysharedaxis",
                "Share Y Axis",
                33,
                &["--ysharedaxis"],
                &[],
                false,
                "Force a shared Y axis across iterated panels",
                "Layout",
                false,
                false,
            ),
            option_argument(
                "title",
                "Title",
                34,
                &["--title"],
                "TEXT",
                UiValueKind::String,
                None,
                &[],
                "Override the plot title",
                "Style",
                true,
                false,
            ),
            option_argument(
                "xlabel",
                "X Label",
                35,
                &["--xlabel"],
                "TEXT",
                UiValueKind::String,
                None,
                &[],
                "Override the x-axis label",
                "Style",
                true,
                false,
            ),
            option_argument(
                "ylabel",
                "Y Label",
                36,
                &["--ylabel"],
                "TEXT",
                UiValueKind::String,
                None,
                &[],
                "Override the y-axis label",
                "Style",
                true,
                false,
            ),
            toggle_argument(
                "showlegend",
                "Show Legend",
                37,
                &["--showlegend"],
                &[],
                false,
                "Show a legend for grouped series",
                "Style",
                true,
                false,
            ),
            toggle_argument(
                "showmajorgrid",
                "Major Grid",
                38,
                &["--showmajorgrid"],
                &[],
                false,
                "Show major grid lines",
                "Style",
                true,
                false,
            ),
            toggle_argument(
                "showminorgrid",
                "Minor Grid",
                39,
                &["--showminorgrid"],
                &[],
                false,
                "Show minor grid lines",
                "Style",
                true,
                false,
            ),
            option_argument(
                "plot_output",
                "Plot Output",
                40,
                &["--plot-output"],
                "PATH",
                UiValueKind::Path,
                None,
                &[],
                "Write the plot export to PATH",
                "Export",
                false,
                false,
            ),
            option_argument(
                "plot_format",
                "Plot Format",
                41,
                &["--plot-format"],
                "FORMAT",
                UiValueKind::Choice,
                Some("png"),
                &["png", "pdf", "txt"],
                "Plot export format",
                "Export",
                false,
                false,
            ),
            option_argument(
                "plot_width",
                "Plot Width",
                42,
                &["--plot-width"],
                "PIXELS",
                UiValueKind::Float,
                Some("1600"),
                &[],
                "Rendered plot width in pixels",
                "Export",
                true,
                false,
            ),
            option_argument(
                "plot_height",
                "Plot Height",
                43,
                &["--plot-height"],
                "PIXELS",
                UiValueKind::Float,
                Some("900"),
                &[],
                "Rendered plot height in pixels",
                "Export",
                true,
                false,
            ),
            action_argument(44, "ui_schema", &["--ui-schema"], UiActionKind::UiSchema),
            action_argument(45, "help", &["-h", "--help"], UiActionKind::Help),
        ],
        managed_output: Some(UiManagedOutputSchema {
            renderer: "listobs-summary-v1".to_string(),
            stdout_format: "json".to_string(),
            inject_arguments: vec![UiInjectedArgument {
                flag: "--format".to_string(),
                value: "json".to_string(),
            }],
            raw_stdout_available: true,
            raw_stderr_available: true,
        }),
    }
}

fn run(options: CliOptions) -> Result<(), String> {
    let ms = MeasurementSet::open(&options.ms_path).map_err(|error| {
        if options.ms_path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                options.ms_path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", options.ms_path.display())
        }
    })?;

    let summary =
        ListObsSummary::from_ms_with_options(&ms, &options.selection.to_listobs_options())
            .map_err(|error| error.to_string())?;
    let summary_text = summary
        .render(options.summary_format)
        .map_err(|error| error.to_string())?;
    write_output(
        options.summary_output.as_deref(),
        options.overwrite,
        &summary_text,
    )?;

    if let Some(plot_output) = &options.plot_output {
        let explore_spec = build_explore_spec(&options)?;
        let payload = build_msexplore_payload(&ms, &explore_spec)?;
        export_msexplore_plot(
            &payload,
            crate::plot::ListObsPlotTheme::light(),
            plot_output,
            options.plot_format,
            options.plot_width,
            options.plot_height,
        )?;
    }

    Ok(())
}

fn build_explore_spec(options: &CliOptions) -> Result<MsExploreSpec, String> {
    if let Some(page_spec_path) = &options.page_spec {
        let page_spec = load_page_spec_file(page_spec_path)?;
        let plots = page_spec
            .plots
            .into_iter()
            .map(|plot| {
                build_plot_spec_from_values(
                    plot.preset,
                    plot.x_axis,
                    plot.y_axis,
                    plot.y_axis2,
                    plot.data_column.unwrap_or(MsDataColumn::Data),
                    plot.color_by.unwrap_or(MsColorAxis::Field),
                    plot.avgchannel,
                    plot.scalar,
                    plot.freqframe,
                    plot.restfreq,
                    plot.veldef.unwrap_or_else(|| "RADIO".to_string()),
                    None,
                    page_spec.gridrows.max(1),
                    page_spec.gridcols.max(1),
                    plot.rowindex,
                    plot.colindex,
                    plot.plotindex,
                    false,
                    false,
                    false,
                    false,
                    plot.title,
                    plot.xlabel,
                    plot.ylabel,
                    plot.showlegend,
                    plot.showmajorgrid,
                    plot.showminorgrid,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let spec = MsExploreSpec {
            ms_path: options.ms_path.clone(),
            summary_format: options.summary_format,
            selection: options.selection.clone(),
            page_title: page_spec.page_title,
            exprange: page_spec.exprange,
            plots,
        };
        spec.validate()?;
        return Ok(spec);
    }

    let plot_spec = build_plot_spec(options)?;
    let spec = MsExploreSpec {
        ms_path: options.ms_path.clone(),
        summary_format: options.summary_format,
        selection: options.selection.clone(),
        page_title: None,
        exprange: MsPageExportRange::Current,
        plots: vec![plot_spec],
    };
    spec.validate()?;
    Ok(spec)
}

fn build_plot_spec(options: &CliOptions) -> Result<MsPlotSpec, String> {
    let spec = build_plot_spec_from_values(
        options.preset,
        options.x_axis,
        options.y_axis,
        options.y_axis2,
        options.data_column,
        options.color_by,
        options.avgchannel,
        options.scalar,
        options.freqframe.clone(),
        options.restfreq.clone(),
        options.veldef.clone(),
        options.iteraxis,
        options.gridrows,
        options.gridcols,
        0,
        0,
        0,
        options.xselfscale,
        options.yselfscale,
        options.xsharedaxis,
        options.ysharedaxis,
        options.title.clone(),
        options.xlabel.clone(),
        options.ylabel.clone(),
        options.showlegend,
        options.showmajorgrid,
        options.showminorgrid,
    )?;
    spec.validate()?;
    Ok(spec)
}

#[allow(clippy::too_many_arguments)]
fn build_plot_spec_from_values(
    preset: Option<MsPlotPreset>,
    x_axis: Option<MsAxis>,
    y_axis: Option<MsAxis>,
    y_axis2: Option<MsAxis>,
    data_column: MsDataColumn,
    color_by: MsColorAxis,
    avgchannel: Option<usize>,
    scalar: bool,
    freqframe: Option<String>,
    restfreq: Option<String>,
    veldef: String,
    iteraxis: Option<MsIterationAxis>,
    gridrows: usize,
    gridcols: usize,
    rowindex: usize,
    colindex: usize,
    plotindex: usize,
    xselfscale: bool,
    yselfscale: bool,
    xsharedaxis: bool,
    ysharedaxis: bool,
    title: Option<String>,
    xlabel: Option<String>,
    ylabel: Option<String>,
    showlegend: bool,
    showmajorgrid: bool,
    showminorgrid: bool,
) -> Result<MsPlotSpec, String> {
    let mut spec = if let Some(preset) = preset {
        MsPlotSpec::from_preset(preset)
    } else {
        let x_axis = x_axis.ok_or_else(|| {
            "msexplore plot specs require either a preset or both x_axis and y_axis".to_string()
        })?;
        let y_axis = y_axis.ok_or_else(|| {
            "msexplore plot specs require either a preset or both x_axis and y_axis".to_string()
        })?;
        let mut y_axes = vec![y_axis];
        if let Some(y_axis2) = y_axis2 {
            y_axes.push(y_axis2);
        }
        MsPlotSpec {
            preset: None,
            x_axis,
            y_axes,
            data_column,
            color_by,
            averaging: Default::default(),
            transforms: Default::default(),
            layout: Default::default(),
            iteration: Default::default(),
            style: Default::default(),
            flag_edit: None,
        }
    };
    spec.data_column = data_column;
    spec.color_by = color_by;
    if spec.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) && y_axis2.is_some() {
        return Err(
            "--yaxis2 is not supported with stacked paired presets; the preset already defines both panels"
                .to_string(),
        );
    }
    if let Some(y_axis2) = y_axis2 {
        spec.y_axes.truncate(1);
        spec.y_axes.push(y_axis2);
    } else {
        spec.y_axes.truncate(1);
    }
    spec.averaging.avgchannel = avgchannel;
    spec.averaging.scalar = scalar;
    spec.transforms.freqframe = freqframe;
    spec.transforms.restfreq = restfreq;
    spec.transforms.veldef = veldef;
    spec.layout.gridrows = gridrows.max(1);
    spec.layout.gridcols = gridcols.max(1);
    spec.layout.rowindex = rowindex;
    spec.layout.colindex = colindex;
    spec.layout.plotindex = plotindex;
    spec.iteration.iteraxis = iteraxis;
    spec.iteration.xselfscale = xselfscale;
    spec.iteration.yselfscale = yselfscale;
    spec.iteration.xsharedaxis = xsharedaxis;
    spec.iteration.ysharedaxis = ysharedaxis;
    spec.style = MsPlotStyleSpec {
        title,
        xlabel,
        ylabel,
        showlegend,
        showmajorgrid,
        showminorgrid,
    };
    Ok(spec)
}

fn load_page_spec_file(path: &std::path::Path) -> Result<CliPageSpecFile, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("read --page-spec {}: {error}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|error| format!("parse --page-spec {}: {error}", path.display()))
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<CliAction, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--ui-schema") {
        return Ok(CliAction::UiSchema);
    }
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        return Ok(CliAction::Help);
    }

    let mut index = 0usize;
    let mut ms_path = None;
    let mut summary_format = "text".to_string();
    let mut summary_output = None;
    let mut overwrite = false;
    let mut selection = MsSelectionSpec::default();
    let mut page_spec = None;
    let mut preset = None;
    let mut x_axis = None;
    let mut y_axis = None;
    let mut y_axis2 = None;
    let mut data_column = MsDataColumn::Data;
    let mut color_by = MsColorAxis::Field;
    let mut avgchannel = None;
    let mut scalar = false;
    let mut freqframe = None;
    let mut restfreq = None;
    let mut veldef = "RADIO".to_string();
    let mut iteraxis = None;
    let mut gridrows = 1usize;
    let mut gridcols = 1usize;
    let mut xselfscale = false;
    let mut yselfscale = false;
    let mut xsharedaxis = false;
    let mut ysharedaxis = false;
    let mut title = None;
    let mut xlabel = None;
    let mut ylabel = None;
    let mut showlegend = false;
    let mut showmajorgrid = false;
    let mut showminorgrid = false;
    let mut plot_output = None;
    let mut plot_format = MsExportFormat::Png;
    let mut plot_width = 1600u32;
    let mut plot_height = 900u32;
    let mut plot_control_used = false;

    while index < args.len() {
        let raw = args[index].to_string_lossy().to_string();
        let take_value =
            |index: &mut usize, args: &[OsString], flag: &str| -> Result<String, String> {
                *index += 1;
                args.get(*index)
                    .map(|value| value.to_string_lossy().to_string())
                    .ok_or_else(|| format!("missing value for {flag}"))
            };
        match raw.as_str() {
            "--format" => summary_format = take_value(&mut index, &args, "--format")?,
            "-o" | "--output" => {
                summary_output = Some(PathBuf::from(take_value(&mut index, &args, raw.as_str())?))
            }
            "--overwrite" => overwrite = true,
            "--selectdata" => selection.selectdata = true,
            "--no-selectdata" => selection.selectdata = false,
            "--field" => selection.field = Some(take_value(&mut index, &args, "--field")?),
            "--spw" => selection.spw = Some(take_value(&mut index, &args, "--spw")?),
            "--timerange" => {
                selection.timerange = Some(take_value(&mut index, &args, "--timerange")?)
            }
            "--uvrange" => selection.uvrange = Some(take_value(&mut index, &args, "--uvrange")?),
            "--antenna" => selection.antenna = Some(take_value(&mut index, &args, "--antenna")?),
            "--scan" => selection.scan = Some(take_value(&mut index, &args, "--scan")?),
            "--correlation" => {
                selection.correlation = Some(take_value(&mut index, &args, "--correlation")?)
            }
            "--observation" => {
                selection.observation = Some(take_value(&mut index, &args, "--observation")?)
            }
            "--array" => selection.array = Some(take_value(&mut index, &args, "--array")?),
            "--intent" => selection.intent = Some(take_value(&mut index, &args, "--intent")?),
            "--feed" => selection.feed = Some(take_value(&mut index, &args, "--feed")?),
            "--msselect" => selection.msselect = Some(take_value(&mut index, &args, "--msselect")?),
            "--page-spec" => {
                page_spec = Some(PathBuf::from(take_value(&mut index, &args, "--page-spec")?))
            }
            "--preset" => {
                plot_control_used = true;
                preset = Some(MsPlotPreset::parse(&take_value(
                    &mut index, &args, "--preset",
                )?)?)
            }
            "--xaxis" => {
                plot_control_used = true;
                x_axis = Some(MsAxis::parse(&take_value(&mut index, &args, "--xaxis")?)?)
            }
            "--yaxis" => {
                plot_control_used = true;
                y_axis = Some(MsAxis::parse(&take_value(&mut index, &args, "--yaxis")?)?)
            }
            "--yaxis2" => {
                plot_control_used = true;
                y_axis2 = Some(MsAxis::parse(&take_value(&mut index, &args, "--yaxis2")?)?)
            }
            "--data-column" => {
                plot_control_used = true;
                data_column = MsDataColumn::parse(&take_value(&mut index, &args, "--data-column")?)?
            }
            "--color-by" => {
                plot_control_used = true;
                color_by = MsColorAxis::parse(&take_value(&mut index, &args, "--color-by")?)?
            }
            "--avgchannel" => {
                plot_control_used = true;
                avgchannel = Some(
                    take_value(&mut index, &args, "--avgchannel")?
                        .parse::<usize>()
                        .map_err(|_| "invalid integer value for --avgchannel".to_string())?,
                )
            }
            "--scalar" => {
                plot_control_used = true;
                scalar = true
            }
            "--freqframe" => {
                plot_control_used = true;
                freqframe = Some(take_value(&mut index, &args, "--freqframe")?)
            }
            "--restfreq" => {
                plot_control_used = true;
                restfreq = Some(take_value(&mut index, &args, "--restfreq")?)
            }
            "--veldef" => {
                plot_control_used = true;
                veldef = take_value(&mut index, &args, "--veldef")?
            }
            "--iteraxis" => {
                plot_control_used = true;
                iteraxis = Some(MsIterationAxis::parse(&take_value(
                    &mut index,
                    &args,
                    "--iteraxis",
                )?)?)
            }
            "--gridrows" => {
                plot_control_used = true;
                gridrows = take_value(&mut index, &args, "--gridrows")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --gridrows".to_string())?
            }
            "--gridcols" => {
                plot_control_used = true;
                gridcols = take_value(&mut index, &args, "--gridcols")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --gridcols".to_string())?
            }
            "--xselfscale" => {
                plot_control_used = true;
                xselfscale = true
            }
            "--yselfscale" => {
                plot_control_used = true;
                yselfscale = true
            }
            "--xsharedaxis" => {
                plot_control_used = true;
                xsharedaxis = true
            }
            "--ysharedaxis" => {
                plot_control_used = true;
                ysharedaxis = true
            }
            "--title" => {
                plot_control_used = true;
                title = Some(take_value(&mut index, &args, "--title")?)
            }
            "--xlabel" => {
                plot_control_used = true;
                xlabel = Some(take_value(&mut index, &args, "--xlabel")?)
            }
            "--ylabel" => {
                plot_control_used = true;
                ylabel = Some(take_value(&mut index, &args, "--ylabel")?)
            }
            "--showlegend" => {
                plot_control_used = true;
                showlegend = true
            }
            "--showmajorgrid" => {
                plot_control_used = true;
                showmajorgrid = true
            }
            "--showminorgrid" => {
                plot_control_used = true;
                showminorgrid = true
            }
            "--plot-output" => {
                plot_output = Some(PathBuf::from(take_value(
                    &mut index,
                    &args,
                    "--plot-output",
                )?))
            }
            "--plot-format" => {
                plot_format =
                    MsExportFormat::parse(&take_value(&mut index, &args, "--plot-format")?)?
            }
            "--plot-width" => {
                plot_width = take_value(&mut index, &args, "--plot-width")?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer value for --plot-width".to_string())?
            }
            "--plot-height" => {
                plot_height = take_value(&mut index, &args, "--plot-height")?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer value for --plot-height".to_string())?
            }
            value if value.starts_with('-') => return Err(format!("unknown option {value:?}")),
            value => {
                if ms_path.is_some() {
                    return Err("expected exactly one MeasurementSet path".to_string());
                }
                ms_path = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }

    let ms_path = ms_path.ok_or_else(|| "missing required MeasurementSet path".to_string())?;
    let summary_format = match summary_format.as_str() {
        "text" => ListObsOutputFormat::Text,
        "json" => ListObsOutputFormat::Json,
        other => {
            return Err(format!(
                "unsupported format {other:?}; expected text or json"
            ));
        }
    };
    if page_spec.is_some() && plot_control_used {
        return Err(
            "--page-spec cannot be combined with the single-plot preset/axis/layout flags; put those settings in the JSON page spec instead".to_string(),
        );
    }
    if plot_output.is_some()
        && page_spec.is_none()
        && preset.is_none()
        && (x_axis.is_none() || y_axis.is_none())
    {
        return Err(
            "--plot-output requires either --preset or both --xaxis and --yaxis".to_string(),
        );
    }
    Ok(CliAction::Run(CliOptions {
        ms_path,
        summary_format,
        summary_output,
        overwrite,
        selection,
        page_spec,
        preset,
        x_axis,
        y_axis,
        y_axis2,
        data_column,
        color_by,
        avgchannel,
        scalar,
        freqframe,
        restfreq,
        veldef,
        iteraxis,
        gridrows: gridrows.max(1),
        gridcols: gridcols.max(1),
        xselfscale,
        yselfscale,
        xsharedaxis,
        ysharedaxis,
        title,
        xlabel,
        ylabel,
        showlegend,
        showmajorgrid,
        showminorgrid,
        plot_output,
        plot_format,
        plot_width: plot_width.max(1),
        plot_height: plot_height.max(1),
    }))
}

fn write_output(path: Option<&std::path::Path>, overwrite: bool, text: &str) -> Result<(), String> {
    match path {
        Some(path) => {
            if path.exists() && !overwrite {
                return Err(format!(
                    "refusing to overwrite existing output {}; pass --overwrite to replace it",
                    path.display()
                ));
            }
            fs::write(path, text).map_err(|error| error.to_string())
        }
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

fn positional_argument(
    id: &str,
    label: &str,
    order: usize,
    metavar: &str,
    value_kind: UiValueKind,
    help: &str,
    group: &str,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Positional {
            metavar: metavar.to_string(),
        },
        value_kind,
        required: true,
        default: None,
        help: help.to_string(),
        group: group.to_string(),
        advanced: false,
        hidden_in_tui: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn option_argument(
    id: &str,
    label: &str,
    order: usize,
    flags: &[&str],
    metavar: &str,
    value_kind: UiValueKind,
    default: Option<&str>,
    choices: &[&str],
    help: &str,
    group: &str,
    advanced: bool,
    hidden_in_tui: bool,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Option {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            metavar: metavar.to_string(),
            choices: choices.iter().map(|choice| (*choice).to_string()).collect(),
        },
        value_kind,
        required: false,
        default: default.map(str::to_string),
        help: help.to_string(),
        group: group.to_string(),
        advanced,
        hidden_in_tui,
    }
}

fn toggle_argument(
    id: &str,
    label: &str,
    order: usize,
    true_flags: &[&str],
    false_flags: &[&str],
    default: bool,
    help: &str,
    group: &str,
    advanced: bool,
    hidden_in_tui: bool,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Toggle {
            true_flags: true_flags.iter().map(|flag| (*flag).to_string()).collect(),
            false_flags: false_flags.iter().map(|flag| (*flag).to_string()).collect(),
        },
        value_kind: UiValueKind::Bool,
        required: false,
        default: Some(default.to_string()),
        help: help.to_string(),
        group: group.to_string(),
        advanced,
        hidden_in_tui,
    }
}

fn action_argument(
    order: usize,
    id: &str,
    flags: &[&str],
    action: UiActionKind,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: id.replace('_', " "),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: match action {
            UiActionKind::Help => "Print this help message".to_string(),
            UiActionKind::UiSchema => {
                "Print the machine-readable UI schema for this command".to_string()
            }
        },
        group: "Meta".to_string(),
        advanced: true,
        hidden_in_tui: true,
    }
}
