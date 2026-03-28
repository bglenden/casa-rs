// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for `msexplore`.

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use super::{
    MsAxis, MsColorAxis, MsDataColumn, MsExportFormat, MsPlotPreset, MsPlotSpec, MsPlotStyleSpec,
    MsSelectionSpec, build_msexplore_plot_payload, export_msexplore_plot,
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
    preset: Option<MsPlotPreset>,
    x_axis: Option<MsAxis>,
    y_axis: Option<MsAxis>,
    data_column: MsDataColumn,
    color_by: MsColorAxis,
    avgchannel: Option<usize>,
    scalar: bool,
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
                    "amplitude_vs_uv_distance",
                    "amplitude_vs_channel",
                    "phase_vs_channel",
                    "amplitude_vs_frequency",
                    "phase_vs_frequency",
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
                "data_column",
                "Data Column",
                20,
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
                21,
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
                22,
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
                23,
                &["--scalar"],
                &[],
                false,
                "Use scalar averaging instead of vector averaging",
                "Averaging",
                false,
                false,
            ),
            option_argument(
                "title",
                "Title",
                24,
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
                25,
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
                26,
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
                27,
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
                28,
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
                29,
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
                30,
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
                31,
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
                32,
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
                33,
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
            action_argument(34, "ui_schema", &["--ui-schema"], UiActionKind::UiSchema),
            action_argument(35, "help", &["-h", "--help"], UiActionKind::Help),
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
        let plot_spec = build_plot_spec(&options)?;
        let payload = build_msexplore_plot_payload(&ms, &options.selection, &plot_spec)?;
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

fn build_plot_spec(options: &CliOptions) -> Result<MsPlotSpec, String> {
    let mut spec = if let Some(preset) = options.preset {
        MsPlotSpec::from_preset(preset)
    } else {
        let x_axis = options.x_axis.ok_or_else(|| {
            "--plot-output requires either --preset or --xaxis/--yaxis".to_string()
        })?;
        let y_axis = options.y_axis.ok_or_else(|| {
            "--plot-output requires either --preset or --xaxis/--yaxis".to_string()
        })?;
        MsPlotSpec {
            preset: None,
            x_axis,
            y_axes: vec![y_axis],
            data_column: options.data_column,
            color_by: options.color_by,
            averaging: Default::default(),
            transforms: Default::default(),
            layout: Default::default(),
            iteration: Default::default(),
            style: Default::default(),
            flag_edit: None,
        }
    };
    spec.data_column = options.data_column;
    spec.color_by = options.color_by;
    spec.averaging.avgchannel = options.avgchannel;
    spec.averaging.scalar = options.scalar;
    spec.style = MsPlotStyleSpec {
        title: options.title.clone(),
        xlabel: options.xlabel.clone(),
        ylabel: options.ylabel.clone(),
        showlegend: options.showlegend,
        showmajorgrid: options.showmajorgrid,
        showminorgrid: options.showminorgrid,
    };
    spec.validate()?;
    Ok(spec)
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
    let mut preset = None;
    let mut x_axis = None;
    let mut y_axis = None;
    let mut data_column = MsDataColumn::Data;
    let mut color_by = MsColorAxis::Field;
    let mut avgchannel = None;
    let mut scalar = false;
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
            "--preset" => {
                preset = Some(MsPlotPreset::parse(&take_value(
                    &mut index, &args, "--preset",
                )?)?)
            }
            "--xaxis" => x_axis = Some(MsAxis::parse(&take_value(&mut index, &args, "--xaxis")?)?),
            "--yaxis" => y_axis = Some(MsAxis::parse(&take_value(&mut index, &args, "--yaxis")?)?),
            "--data-column" => {
                data_column = MsDataColumn::parse(&take_value(&mut index, &args, "--data-column")?)?
            }
            "--color-by" => {
                color_by = MsColorAxis::parse(&take_value(&mut index, &args, "--color-by")?)?
            }
            "--avgchannel" => {
                avgchannel = Some(
                    take_value(&mut index, &args, "--avgchannel")?
                        .parse::<usize>()
                        .map_err(|_| "invalid integer value for --avgchannel".to_string())?,
                )
            }
            "--scalar" => scalar = true,
            "--title" => title = Some(take_value(&mut index, &args, "--title")?),
            "--xlabel" => xlabel = Some(take_value(&mut index, &args, "--xlabel")?),
            "--ylabel" => ylabel = Some(take_value(&mut index, &args, "--ylabel")?),
            "--showlegend" => showlegend = true,
            "--showmajorgrid" => showmajorgrid = true,
            "--showminorgrid" => showminorgrid = true,
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
    if plot_output.is_some() && preset.is_none() && (x_axis.is_none() || y_axis.is_none()) {
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
        preset,
        x_axis,
        y_axis,
        data_column,
        color_by,
        avgchannel,
        scalar,
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
