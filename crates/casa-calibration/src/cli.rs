// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI for the first public calibration workflow slice.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use casacore_ms::msexplore::cli::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiInjectedArgument,
    UiManagedOutputSchema, UiValueKind,
};
use casacore_ms::selection::MsSelection;

use crate::{
    ApplyCalibrationTableSpec, ApplyExecutionReport, ApplyInterpolationMode, ApplyMode, ApplyPlan,
    ApplyPlanRequest, BandpassSolveCombine, BandpassSolveReport, BandpassSolveRequest,
    BandpassType, CalibrationStatsAxis, CalibrationStatsReport, CalibrationStatsRequest,
    CalibrationTableSummary, FluxScaleReport, FluxScaleRequest, GainFieldSelector,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveReport, GainSolveRequest,
    GainType, ManagedCalibrationOutput, RefAntSelector, calibration_stats, execute_apply_from_path,
    fluxscale, load_apply_specs_from_callib, plan_apply_from_path, solve_bandpass_from_path,
    solve_gain_from_path, summarize_tables,
};

const UI_SCHEMA_VERSION: u32 = 1;
const COMMAND_ID: &str = "calibrate";
const DISPLAY_NAME: &str = "Calibrate";
const CATEGORY: &str = "Calibration";
const SUMMARY: &str = "apply, inspect, and solve CASA-style calibration workflows";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug)]
struct ApplyOptions {
    measurement_set: PathBuf,
    calibration_tables: Vec<ApplyCalibrationTableSpec>,
    apply_mode: ApplyMode,
    parang: bool,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
    selection: SelectionOptions,
}

#[derive(Debug, Default)]
struct SelectionOptions {
    selectdata: bool,
    field: Option<String>,
    spw: Option<String>,
    antenna: Option<String>,
    scan: Option<String>,
    observation: Option<String>,
    array: Option<String>,
    timerange: Option<String>,
    msselect: Option<String>,
}

#[derive(Debug)]
struct SummaryOptions {
    paths: Vec<PathBuf>,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
}

#[derive(Debug)]
struct ApplyPlanOptions {
    measurement_set: PathBuf,
    calibration_tables: Vec<ApplyCalibrationTableSpec>,
    parang: bool,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
}

#[derive(Debug)]
struct StatsOptions {
    path: PathBuf,
    axis: CalibrationStatsAxis,
    datacolumn: Option<String>,
    use_flags: bool,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
}

#[derive(Debug)]
struct SolveGainOptions {
    measurement_set: PathBuf,
    output_table: PathBuf,
    gain_type: GainType,
    solve_mode: GainSolveMode,
    solve_interval: GainSolveInterval,
    combine: GainSolveCombine,
    refant: RefAntSelector,
    prior_calibration_tables: Vec<ApplyCalibrationTableSpec>,
    parang: bool,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
    selection: SelectionOptions,
}

#[derive(Debug)]
struct SolveBandpassOptions {
    measurement_set: PathBuf,
    output_table: PathBuf,
    refant: RefAntSelector,
    prior_calibration_tables: Vec<ApplyCalibrationTableSpec>,
    parang: bool,
    combine: BandpassSolveCombine,
    band_type: BandpassType,
    normalize_average_amplitude: bool,
    amplitude_degree: usize,
    phase_degree: usize,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
    selection: SelectionOptions,
}

#[derive(Debug)]
struct FluxScaleOptions {
    input_table: PathBuf,
    output_table: PathBuf,
    reference_fields: Vec<String>,
    transfer_fields: Vec<String>,
    refspwmap: Vec<i32>,
    gainthreshold: Option<f64>,
    incremental: bool,
    format: OutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
}

#[derive(Debug)]
enum Command {
    Apply(ApplyOptions),
    Summarize(SummaryOptions),
    PlanApply(ApplyPlanOptions),
    Stats(StatsOptions),
    SolveGain(SolveGainOptions),
    SolveBandpass(SolveBandpassOptions),
    FluxScale(FluxScaleOptions),
}

#[derive(Debug)]
enum CliAction {
    Help,
    UiSchema,
    Run(Box<RunRequest>),
}

#[derive(Debug)]
struct RunRequest {
    command: Command,
    managed_output: bool,
}

struct OptionArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    flags: &'a [&'a str],
    metavar: &'a str,
    value_kind: UiValueKind,
    default: Option<&'a str>,
    choices: &'a [&'a str],
    help: &'a str,
    group: &'a str,
    required: bool,
    advanced: bool,
}

struct ToggleArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    help: &'a str,
    true_flags: &'a [&'a str],
    false_flags: &'a [&'a str],
    default: bool,
    group: &'a str,
    advanced: bool,
}

/// Parse environment arguments, run the CLI, and return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    let schema = command_schema(program_name);
    match parse_args(std::env::args_os().skip(1)) {
        Ok(CliAction::Help) => {
            print!("{}", render_help(&schema));
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
        Ok(CliAction::Run(request)) => match run(*request) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        },
        Err(error) => {
            eprintln!("Error: {error}\n");
            eprintln!("{}", render_help(&schema));
            1
        }
    }
}

/// Build the machine-readable command schema for the public `calibrate` app.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: UI_SCHEMA_VERSION,
        command_id: COMMAND_ID.to_string(),
        invocation_name: program_name.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        category: CATEGORY.to_string(),
        summary: SUMMARY.to_string(),
        usage: format!("{program_name} --mode MODE [WORKFLOW OPTIONS]"),
        arguments: vec![
            option_argument(OptionArgumentConfig {
                id: "mode",
                label: "Workflow",
                order: 0,
                flags: &["--mode"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("apply"),
                choices: &[
                    "apply",
                    "summary",
                    "stats",
                    "solve_gain",
                    "solve_bandpass",
                    "fluxscale",
                ],
                help: "Calibration workflow to run from the launcher form",
                group: "Workflow",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "measurement_set",
                label: "MeasurementSet Path",
                order: 1,
                flags: &["--ms"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Path to the MeasurementSet root directory for apply and solve workflows",
                group: "Input",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "summary_paths",
                label: "Summary Tables",
                order: 2,
                flags: &["--summary-paths"],
                metavar: "PATH[,PATH...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated calibration-table paths for summary mode",
                group: "Inspect",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "table_path",
                label: "Table Path",
                order: 3,
                flags: &["--table"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Calibration-table path for stats mode",
                group: "Inspect",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "gaintables",
                label: "Calibration Tables",
                order: 4,
                flags: &["--gaintables"],
                metavar: "PATH[,PATH...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated list of calibration-table paths to apply in order",
                group: "Input",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "callib",
                label: "Callibrary File",
                order: 5,
                flags: &["--callib"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "CASA callibrary file describing the calibration-table chain",
                group: "Input",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "gainfield",
                label: "Gainfield Overrides",
                order: 6,
                flags: &["--gainfield"],
                metavar: "GFIELD[;GFIELD...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Semicolon-separated gainfield overrides aligned to --gaintables (single value applies to all)",
                group: "Apply",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "interp",
                label: "Interpolation",
                order: 7,
                flags: &["--interp"],
                metavar: "MODE[;MODE...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Semicolon-separated interpolation modes aligned to --gaintables: nearest, linear, nearest,linear",
                group: "Apply",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "spwmap",
                label: "SPW Maps",
                order: 8,
                flags: &["--spwmap"],
                metavar: "MAP[;MAP...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Semicolon-separated SPW maps aligned to --gaintables; each MAP is a comma-separated integer list",
                group: "Apply",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "apply_mode",
                label: "Apply Mode",
                order: 9,
                flags: &["--apply-mode"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("calflag"),
                choices: &["calflag", "calonly", "trial"],
                help: "How to handle flags and mutation during apply",
                group: "Apply",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "calwt",
                label: "Cal Weight",
                order: 10,
                flags: &["--calwt"],
                metavar: "BOOL[,BOOL...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated weight-update flags aligned to --gaintables (single value applies to all)",
                group: "Apply",
                required: false,
                advanced: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "parang",
                label: "Parallactic Angle",
                order: 11,
                help: "Apply the parallactic-angle P Jones term during calibration",
                true_flags: &["--parang"],
                false_flags: &["--no-parang"],
                default: false,
                group: "Apply",
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "format",
                label: "Output Format",
                order: 12,
                flags: &["--format"],
                metavar: "FORMAT",
                value_kind: UiValueKind::Choice,
                default: Some("text"),
                choices: &["text", "json"],
                help: "Execution report format",
                group: "Output",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "output",
                label: "Output Path",
                order: 10,
                flags: &["-o", "--output"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Write the execution report to PATH",
                group: "Output",
                required: false,
                advanced: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "overwrite",
                label: "Overwrite Output",
                order: 11,
                help: "Replace an existing output file",
                true_flags: &["--overwrite"],
                false_flags: &[],
                default: false,
                group: "Output",
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "stats_axis",
                label: "Stats Axis",
                order: 12,
                flags: &["--axis"],
                metavar: "AXIS",
                value_kind: UiValueKind::Choice,
                default: Some("amp"),
                choices: &["amp", "phase", "real", "imag"],
                help: "Complex axis to summarize in stats mode",
                group: "Inspect",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "stats_datacolumn",
                label: "Stats Data Column",
                order: 13,
                flags: &["--datacolumn"],
                metavar: "COLUMN",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Calibration-table column to inspect in stats mode",
                group: "Inspect",
                required: false,
                advanced: true,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "use_flags",
                label: "Use Flags",
                order: 14,
                help: "Include flagged values in stats calculations",
                true_flags: &["--use-flags"],
                false_flags: &[],
                default: false,
                group: "Inspect",
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "out_table",
                label: "Output Table",
                order: 15,
                flags: &["--out"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Output calibration-table path for solve and fluxscale workflows",
                group: "Solve",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "refant",
                label: "Reference Antenna",
                order: 16,
                flags: &["--refant"],
                metavar: "ANTENNA",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Reference antenna name or id for solve workflows",
                group: "Solve",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "gain_type",
                label: "Gain Type",
                order: 17,
                flags: &["--gain-type"],
                metavar: "TYPE",
                value_kind: UiValueKind::Choice,
                default: Some("g"),
                choices: &["g", "t"],
                help: "Gain family for solve-gain mode",
                group: "Solve Gain",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "solve_mode",
                label: "Solve Mode",
                order: 18,
                flags: &["--mode-gain"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("p"),
                choices: &["p", "ap"],
                help: "Gain solve mode for solve-gain mode",
                group: "Solve Gain",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "solint",
                label: "Solution Interval",
                order: 19,
                flags: &["--solint"],
                metavar: "INTERVAL",
                value_kind: UiValueKind::String,
                default: Some("inf"),
                choices: &[],
                help: "Gain solution interval such as inf, int, or 30s",
                group: "Solve Gain",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "gain_combine",
                label: "Gain Combine",
                order: 20,
                flags: &["--combine-gain"],
                metavar: "AXES",
                value_kind: UiValueKind::String,
                default: Some("none"),
                choices: &[],
                help: "Gain solve combine axes such as scan or scan,field",
                group: "Solve Gain",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "bandpass_combine",
                label: "Bandpass Combine",
                order: 21,
                flags: &["--combine-bandpass"],
                metavar: "AXES",
                value_kind: UiValueKind::Choice,
                default: Some("none"),
                choices: &["none", "scan", "field", "scan,field"],
                help: "Bandpass combine axes",
                group: "Solve Bandpass",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "bandtype",
                label: "Bandpass Type",
                order: 22,
                flags: &["--bandtype"],
                metavar: "TYPE",
                value_kind: UiValueKind::Choice,
                default: Some("b"),
                choices: &["b", "bpoly"],
                help: "Bandpass output family for solve-bandpass mode",
                group: "Solve Bandpass",
                required: false,
                advanced: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "solnorm",
                label: "Normalize Bandpass",
                order: 23,
                help: "Normalize solved bandpass rows to unity average amplitude",
                true_flags: &["--solnorm"],
                false_flags: &["--no-solnorm"],
                default: false,
                group: "Solve Bandpass",
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "fluxscale_input",
                label: "Fluxscale Input Table",
                order: 24,
                flags: &["--in"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Input gain table for fluxscale mode",
                group: "Fluxscale",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "reference_fields",
                label: "Reference Fields",
                order: 25,
                flags: &["--reference"],
                metavar: "FIELD[,FIELD...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Reference fields for fluxscale mode",
                group: "Fluxscale",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "transfer_fields",
                label: "Transfer Fields",
                order: 26,
                flags: &["--transfer"],
                metavar: "FIELD[,FIELD...]",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Optional transfer-field restriction for fluxscale mode",
                group: "Fluxscale",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "refspwmap",
                label: "Reference SPW Map",
                order: 27,
                flags: &["--refspwmap"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Optional fluxscale reference spectral-window remap",
                group: "Fluxscale",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "gainthreshold",
                label: "Gain Threshold",
                order: 28,
                flags: &["--gainthreshold"],
                metavar: "FLOAT",
                value_kind: UiValueKind::Float,
                default: None,
                choices: &[],
                help: "Reject unstable transfer factors above this threshold in fluxscale mode",
                group: "Fluxscale",
                required: false,
                advanced: true,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "incremental",
                label: "Incremental Fluxscale",
                order: 29,
                help: "Write incremental correction factors instead of absolute transfer gains",
                true_flags: &["--incremental"],
                false_flags: &["--no-incremental"],
                default: false,
                group: "Fluxscale",
                advanced: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "selectdata",
                label: "Apply Selection",
                order: 30,
                help: "Apply the selection controls below before calibration",
                true_flags: &["--selectdata"],
                false_flags: &["--no-selectdata"],
                default: true,
                group: "Selection",
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "field",
                label: "Field IDs",
                order: 31,
                flags: &["--field"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated FIELD_ID integers",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "spw",
                label: "SPW IDs",
                order: 32,
                flags: &["--spw"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated SPECTRAL_WINDOW_ID integers",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "antenna",
                label: "Antenna IDs",
                order: 33,
                flags: &["--antenna"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated antenna ids matched against ANTENNA1/ANTENNA2",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "scan",
                label: "Scan Numbers",
                order: 34,
                flags: &["--scan"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated scan numbers",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "observation",
                label: "Observation IDs",
                order: 35,
                flags: &["--observation"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated OBSERVATION_ID integers",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "array",
                label: "Array IDs",
                order: 36,
                flags: &["--array"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated ARRAY_ID integers",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "timerange",
                label: "Time Range",
                order: 37,
                flags: &["--timerange"],
                metavar: "START:END",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Inclusive TIME range in MJD seconds",
                group: "Selection",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "msselect",
                label: "MSSelect",
                order: 38,
                flags: &["--msselect"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Raw TaQL WHERE expression to AND with the structured selectors",
                group: "Selection",
                required: false,
                advanced: true,
            }),
            action_argument(39, "ui_schema", &["--ui-schema"], UiActionKind::UiSchema),
            action_argument(40, "help", &["-h", "--help"], UiActionKind::Help),
        ],
        managed_output: Some(UiManagedOutputSchema {
            renderer: "calibration-report-v1".to_string(),
            stdout_format: "json".to_string(),
            inject_arguments: vec![
                UiInjectedArgument {
                    flag: "--format".to_string(),
                    value: "json".to_string(),
                },
                UiInjectedArgument {
                    flag: "--managed-output".to_string(),
                    value: "true".to_string(),
                },
            ],
            raw_stdout_available: true,
            raw_stderr_available: true,
        }),
    }
}

fn run(request: RunRequest) -> Result<(), String> {
    match request.command {
        Command::Apply(options) => run_apply(options, request.managed_output),
        Command::Summarize(options) => run_summary(options, request.managed_output),
        Command::PlanApply(options) => run_apply_plan(options, request.managed_output),
        Command::Stats(options) => run_stats(options, request.managed_output),
        Command::SolveGain(options) => run_solve_gain(options, request.managed_output),
        Command::SolveBandpass(options) => run_solve_bandpass(options, request.managed_output),
        Command::FluxScale(options) => run_fluxscale(options, request.managed_output),
    }
}

fn run_apply(options: ApplyOptions, managed_output: bool) -> Result<(), String> {
    let selection = build_selection(&options.selection)?;
    let report = execute_apply_from_path(
        &options.measurement_set,
        &ApplyPlanRequest {
            selection,
            apply_mode: options.apply_mode,
            parang: options.parang,
            calibration_tables: options.calibration_tables,
        },
    )
    .map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_apply_report_text(&report),
        OutputFormat::Json => render_json_output(
            managed_output,
            &report,
            ManagedCalibrationOutput::Apply(report.clone()),
            "apply report",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn run_summary(options: SummaryOptions, managed_output: bool) -> Result<(), String> {
    let path_refs: Vec<_> = options.paths.iter().map(PathBuf::as_path).collect();
    let summaries = summarize_tables(path_refs).map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_summary_text(&summaries),
        OutputFormat::Json => render_json_output(
            managed_output,
            &summaries,
            ManagedCalibrationOutput::Summary(summaries.clone()),
            "summary report",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn run_apply_plan(options: ApplyPlanOptions, managed_output: bool) -> Result<(), String> {
    let plan = plan_apply_from_path(
        &options.measurement_set,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::Trial,
            parang: options.parang,
            calibration_tables: options.calibration_tables,
        },
    )
    .map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_apply_plan_text(&plan),
        OutputFormat::Json => render_json_output(
            managed_output,
            &plan,
            ManagedCalibrationOutput::PlanApply(plan.clone()),
            "apply plan",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn run_stats(options: StatsOptions, managed_output: bool) -> Result<(), String> {
    let report = calibration_stats(
        &options.path,
        &CalibrationStatsRequest {
            axis: options.axis,
            datacolumn: options.datacolumn,
            use_flags: options.use_flags,
        },
    )
    .map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_stats_text(&report),
        OutputFormat::Json => render_json_output(
            managed_output,
            &report,
            ManagedCalibrationOutput::Stats(report.clone()),
            "stats report",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn run_solve_gain(options: SolveGainOptions, managed_output: bool) -> Result<(), String> {
    let selection = build_selection(&options.selection)?;
    let report = solve_gain_from_path(
        &options.measurement_set,
        &GainSolveRequest {
            selection,
            output_table: options.output_table,
            gain_type: options.gain_type,
            solve_mode: options.solve_mode,
            solve_interval: options.solve_interval,
            combine: options.combine,
            refant: options.refant,
            prior_calibration_tables: options.prior_calibration_tables,
            parang: options.parang,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_gain_solve_report_text(&report),
        OutputFormat::Json => render_json_output(
            managed_output,
            &report,
            ManagedCalibrationOutput::SolveGain(report.clone()),
            "gain solve report",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn run_solve_bandpass(options: SolveBandpassOptions, managed_output: bool) -> Result<(), String> {
    let selection = build_selection(&options.selection)?;
    let report = solve_bandpass_from_path(
        &options.measurement_set,
        &BandpassSolveRequest {
            selection,
            output_table: options.output_table,
            refant: options.refant,
            prior_calibration_tables: options.prior_calibration_tables,
            parang: options.parang,
            combine: options.combine,
            band_type: options.band_type,
            normalize_average_amplitude: options.normalize_average_amplitude,
            amplitude_degree: options.amplitude_degree,
            phase_degree: options.phase_degree,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_bandpass_solve_report_text(&report),
        OutputFormat::Json => render_json_output(
            managed_output,
            &report,
            ManagedCalibrationOutput::SolveBandpass(report.clone()),
            "bandpass solve report",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn run_fluxscale(options: FluxScaleOptions, managed_output: bool) -> Result<(), String> {
    let report = fluxscale(&FluxScaleRequest {
        input_table: options.input_table,
        output_table: options.output_table,
        reference_fields: options.reference_fields,
        transfer_fields: options.transfer_fields,
        refspwmap: options.refspwmap,
        gainthreshold: options.gainthreshold,
        incremental: options.incremental,
    })
    .map_err(|error| error.to_string())?;
    let rendered = match options.format {
        OutputFormat::Text => render_fluxscale_report_text(&report),
        OutputFormat::Json => render_json_output(
            managed_output,
            &report,
            ManagedCalibrationOutput::FluxScale(report.clone()),
            "fluxscale report",
        )?,
    };
    write_output(options.output.as_deref(), options.overwrite, &rendered)
}

fn build_selection(options: &SelectionOptions) -> Result<MsSelection, String> {
    if !options.selectdata {
        return Ok(MsSelection::new());
    }

    let mut selection = MsSelection::new();
    if let Some(field) = &options.field {
        let ids = parse_i32_list("--field", field)?;
        selection = selection.field(&ids);
    }
    if let Some(spw) = &options.spw {
        let ids = parse_i32_list("--spw", spw)?;
        selection = selection.spw(&ids);
    }
    if let Some(antenna) = &options.antenna {
        let ids = parse_i32_list("--antenna", antenna)?;
        selection = selection.antenna(&ids);
    }
    if let Some(scan) = &options.scan {
        let ids = parse_i32_list("--scan", scan)?;
        selection = selection.scan(&ids);
    }
    if let Some(observation) = &options.observation {
        let ids = parse_i32_list("--observation", observation)?;
        selection = selection.observation(&ids);
    }
    if let Some(array) = &options.array {
        let ids = parse_i32_list("--array", array)?;
        selection = selection.array(&ids);
    }
    if let Some(timerange) = &options.timerange {
        let (start, end) = parse_time_range(timerange)?;
        selection = selection.time_range(start, end);
    }
    if let Some(msselect) = &options.msselect {
        if !msselect.trim().is_empty() {
            selection = selection.taql(msselect.trim());
        }
    }
    Ok(selection)
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<CliAction, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--ui-schema") {
        return Ok(CliAction::UiSchema);
    }
    if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        return Ok(CliAction::Help);
    }

    let (managed_output_arg, args) = extract_option_value(&args, "--managed-output")?;
    let managed_output = managed_output_arg
        .as_deref()
        .map(parse_bool_literal)
        .transpose()?
        .unwrap_or(false);

    match args.first().and_then(|value| value.to_str()) {
        Some("summary") => parse_summary_args(&args[1..], managed_output),
        Some("plan-apply") => parse_apply_plan_args(&args[1..], managed_output),
        Some("stats") => parse_stats_args(&args[1..], managed_output),
        Some("solve-gain") => parse_solve_gain_args(&args[1..], managed_output),
        Some("solve-bandpass") => parse_solve_bandpass_args(&args[1..], managed_output),
        Some("fluxscale") => parse_fluxscale_args(&args[1..], managed_output),
        Some("apply") => parse_apply_args(&args[1..], managed_output),
        _ => {
            let (workflow_mode, remaining_args) = extract_option_value(&args, "--mode")?;
            match workflow_mode.as_deref() {
                Some("apply") => parse_apply_args(&remaining_args, managed_output),
                Some("summary") => parse_summary_args(&remaining_args, managed_output),
                Some("stats") => parse_stats_args(&remaining_args, managed_output),
                Some("solve_gain") => parse_solve_gain_args(&remaining_args, managed_output),
                Some("solve_bandpass") => {
                    parse_solve_bandpass_args(&remaining_args, managed_output)
                }
                Some("fluxscale") => parse_fluxscale_args(&remaining_args, managed_output),
                Some(other) => Err(format!(
                    "unsupported --mode {other:?}; expected apply, summary, stats, solve_gain, solve_bandpass, or fluxscale"
                )),
                None => parse_apply_args(&args, managed_output),
            }
        }
    }
}

fn parse_apply_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut measurement_set = None;
    let mut calibration_table_paths = Vec::new();
    let mut callib = None;
    let mut gainfield = None;
    let mut interp = None;
    let mut spwmap = None;
    let mut calwt = None;
    let mut apply_mode = ApplyMode::CalFlag;
    let mut parang = false;
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;
    let mut selection = SelectionOptions {
        selectdata: true,
        ..SelectionOptions::default()
    };

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--ms" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --ms".to_string())?;
                measurement_set = Some(PathBuf::from(value));
            }
            "--gaintables" => {
                index += 1;
                let value = args
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "missing value for --gaintables".to_string())?;
                calibration_table_paths.extend(parse_path_list(value));
            }
            "--callib" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --callib".to_string())?;
                callib = Some(PathBuf::from(value));
            }
            "--gainfield" => {
                index += 1;
                gainfield = Some(take_string_value(index, args, "--gainfield")?);
            }
            "--interp" => {
                index += 1;
                interp = Some(take_string_value(index, args, "--interp")?);
            }
            "--spwmap" => {
                index += 1;
                spwmap = Some(take_string_value(index, args, "--spwmap")?);
            }
            "--calwt" => {
                index += 1;
                calwt = Some(take_string_value(index, args, "--calwt")?);
            }
            "--apply-mode" => {
                index += 1;
                let value = args
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "missing value for --apply-mode".to_string())?;
                apply_mode = parse_apply_mode(value)?;
            }
            "--parang" => parang = true,
            "--no-parang" => parang = false,
            "--format" => {
                index += 1;
                let value = args
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "missing value for --format".to_string())?;
                format = parse_output_format("--format", value)?;
            }
            "-o" | "--output" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --output".to_string())?;
                output = Some(PathBuf::from(value));
            }
            "--overwrite" => overwrite = true,
            "--selectdata" => selection.selectdata = true,
            "--no-selectdata" => selection.selectdata = false,
            "--field" => {
                index += 1;
                selection.field = Some(take_string_value(index, args, "--field")?);
            }
            "--spw" => {
                index += 1;
                selection.spw = Some(take_string_value(index, args, "--spw")?);
            }
            "--antenna" => {
                index += 1;
                selection.antenna = Some(take_string_value(index, args, "--antenna")?);
            }
            "--scan" => {
                index += 1;
                selection.scan = Some(take_string_value(index, args, "--scan")?);
            }
            "--observation" => {
                index += 1;
                selection.observation = Some(take_string_value(index, args, "--observation")?);
            }
            "--array" => {
                index += 1;
                selection.array = Some(take_string_value(index, args, "--array")?);
            }
            "--timerange" => {
                index += 1;
                selection.timerange = Some(take_string_value(index, args, "--timerange")?);
            }
            "--msselect" => {
                index += 1;
                selection.msselect = Some(take_string_value(index, args, "--msselect")?);
            }
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => {
                if measurement_set.is_none() {
                    measurement_set = Some(PathBuf::from(&args[index]));
                } else {
                    calibration_table_paths.push(PathBuf::from(&args[index]));
                }
            }
        }
        index += 1;
    }

    let measurement_set =
        measurement_set.ok_or_else(|| "expected <ms-path> for calibration apply".to_string())?;
    let calibration_tables = build_input_calibration_table_specs(
        callib,
        calibration_table_paths,
        gainfield.as_deref(),
        interp.as_deref(),
        spwmap.as_deref(),
        calwt.as_deref(),
    )?;

    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::Apply(ApplyOptions {
            measurement_set,
            calibration_tables,
            apply_mode,
            parang,
            format,
            output,
            overwrite,
            selection,
        }),
    })))
}

fn parse_summary_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut paths = Vec::new();
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--summary-paths" => {
                index += 1;
                paths.extend(parse_path_list(&take_string_value(
                    index,
                    args,
                    "--summary-paths",
                )?));
            }
            "--format" | "--summary-format" => {
                index += 1;
                format = parse_output_format(raw, &take_string_value(index, args, raw)?)?;
            }
            "--output" | "--summary-output" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| format!("missing value for {raw}"))?;
                output = Some(PathBuf::from(value));
            }
            "--overwrite" => overwrite = true,
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => paths.push(PathBuf::from(&args[index])),
        }
        index += 1;
    }

    if paths.is_empty() {
        return Err("summary requires at least one calibration-table path".to_string());
    }

    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::Summarize(SummaryOptions {
            paths,
            format,
            output,
            overwrite,
        }),
    })))
}

fn parse_apply_plan_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut measurement_set = None;
    let mut calibration_table_paths = Vec::new();
    let mut callib = None;
    let mut gainfield = None;
    let mut interp = None;
    let mut spwmap = None;
    let mut calwt = None;
    let mut parang = false;
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--ms" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --ms".to_string())?;
                measurement_set = Some(PathBuf::from(value));
            }
            "--callib" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --callib".to_string())?;
                callib = Some(PathBuf::from(value));
            }
            "--gainfield" => {
                index += 1;
                gainfield = Some(take_string_value(index, args, "--gainfield")?);
            }
            "--interp" => {
                index += 1;
                interp = Some(take_string_value(index, args, "--interp")?);
            }
            "--spwmap" => {
                index += 1;
                spwmap = Some(take_string_value(index, args, "--spwmap")?);
            }
            "--calwt" => {
                index += 1;
                calwt = Some(take_string_value(index, args, "--calwt")?);
            }
            "--parang" => parang = true,
            "--no-parang" => parang = false,
            "--plan-format" => {
                index += 1;
                let value = args
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "missing value for --plan-format".to_string())?;
                format = parse_output_format("--plan-format", value)?;
            }
            "--plan-output" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --plan-output".to_string())?;
                output = Some(PathBuf::from(value));
            }
            "--overwrite" => overwrite = true,
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => calibration_table_paths.push(PathBuf::from(&args[index])),
        }
        index += 1;
    }

    let measurement_set =
        measurement_set.ok_or_else(|| "plan-apply requires --ms <measurement-set>".to_string())?;
    let calibration_tables = build_input_calibration_table_specs(
        callib,
        calibration_table_paths,
        gainfield.as_deref(),
        interp.as_deref(),
        spwmap.as_deref(),
        calwt.as_deref(),
    )?;

    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::PlanApply(ApplyPlanOptions {
            measurement_set,
            calibration_tables,
            parang,
            format,
            output,
            overwrite,
        }),
    })))
}

fn parse_stats_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut path = None;
    let mut axis = CalibrationStatsAxis::Amplitude;
    let mut datacolumn = None;
    let mut use_flags = false;
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--table" => {
                index += 1;
                path = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --table".to_string())?,
                ));
            }
            "--axis" => {
                index += 1;
                axis = CalibrationStatsAxis::parse(&take_string_value(index, args, "--axis")?);
            }
            "--datacolumn" => {
                index += 1;
                datacolumn = Some(take_string_value(index, args, "--datacolumn")?);
            }
            "--use-flags" => use_flags = true,
            "--format" | "--stats-format" => {
                index += 1;
                format = parse_output_format(raw, &take_string_value(index, args, raw)?)?;
            }
            "--output" | "--stats-output" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| format!("missing value for {raw}"))?;
                output = Some(PathBuf::from(value));
            }
            "--overwrite" => overwrite = true,
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => {
                if path.is_none() {
                    path = Some(PathBuf::from(&args[index]));
                } else {
                    return Err(format!("unexpected extra positional argument {raw:?}"));
                }
            }
        }
        index += 1;
    }

    let path = path.ok_or_else(|| "stats requires a calibration-table path".to_string())?;
    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::Stats(StatsOptions {
            path,
            axis,
            datacolumn,
            use_flags,
            format,
            output,
            overwrite,
        }),
    })))
}

fn parse_solve_gain_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut measurement_set = None;
    let mut output_table = None;
    let mut calibration_table_paths = Vec::new();
    let mut callib = None;
    let mut gainfield = None;
    let mut interp = None;
    let mut spwmap = None;
    let mut gain_type = GainType::G;
    let mut solve_mode = GainSolveMode::Phase;
    let mut solve_interval = GainSolveInterval::Infinite;
    let mut combine = GainSolveCombine::default();
    let mut refant = None;
    let mut parang = false;
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;
    let mut selection = SelectionOptions {
        selectdata: true,
        ..SelectionOptions::default()
    };

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--ms" => {
                index += 1;
                measurement_set = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --ms".to_string())?,
                ));
            }
            "--out" => {
                index += 1;
                output_table = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --out".to_string())?,
                ));
            }
            "--gaintables" => {
                index += 1;
                calibration_table_paths.extend(parse_path_list(&take_string_value(
                    index,
                    args,
                    "--gaintables",
                )?));
            }
            "--callib" => {
                index += 1;
                callib = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --callib".to_string())?,
                ));
            }
            "--gainfield" => {
                index += 1;
                gainfield = Some(take_string_value(index, args, "--gainfield")?);
            }
            "--interp" => {
                index += 1;
                interp = Some(take_string_value(index, args, "--interp")?);
            }
            "--spwmap" => {
                index += 1;
                spwmap = Some(take_string_value(index, args, "--spwmap")?);
            }
            "--gain-type" => {
                index += 1;
                gain_type = parse_gain_type(&take_string_value(index, args, "--gain-type")?)?;
            }
            "--mode" | "--mode-gain" => {
                index += 1;
                solve_mode = parse_gain_solve_mode(&take_string_value(index, args, raw)?)?;
            }
            "--solint" => {
                index += 1;
                solve_interval =
                    parse_gain_solve_interval(&take_string_value(index, args, "--solint")?)?;
            }
            "--combine" | "--combine-gain" => {
                index += 1;
                combine = parse_gain_solve_combine(&take_string_value(index, args, raw)?)?;
            }
            "--refant" => {
                index += 1;
                refant = Some(parse_refant_selector(&take_string_value(
                    index, args, "--refant",
                )?));
            }
            "--parang" => parang = true,
            "--no-parang" => parang = false,
            "--format" => {
                index += 1;
                format =
                    parse_output_format("--format", &take_string_value(index, args, "--format")?)?;
            }
            "-o" | "--output" => {
                index += 1;
                output = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --output".to_string())?,
                ));
            }
            "--overwrite" => overwrite = true,
            "--selectdata" => selection.selectdata = true,
            "--no-selectdata" => selection.selectdata = false,
            "--field" => {
                index += 1;
                selection.field = Some(take_string_value(index, args, "--field")?);
            }
            "--spw" => {
                index += 1;
                selection.spw = Some(take_string_value(index, args, "--spw")?);
            }
            "--antenna" => {
                index += 1;
                selection.antenna = Some(take_string_value(index, args, "--antenna")?);
            }
            "--scan" => {
                index += 1;
                selection.scan = Some(take_string_value(index, args, "--scan")?);
            }
            "--observation" => {
                index += 1;
                selection.observation = Some(take_string_value(index, args, "--observation")?);
            }
            "--array" => {
                index += 1;
                selection.array = Some(take_string_value(index, args, "--array")?);
            }
            "--timerange" => {
                index += 1;
                selection.timerange = Some(take_string_value(index, args, "--timerange")?);
            }
            "--msselect" => {
                index += 1;
                selection.msselect = Some(take_string_value(index, args, "--msselect")?);
            }
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => return Err(format!("unexpected positional argument {raw:?}")),
        }
        index += 1;
    }

    let measurement_set =
        measurement_set.ok_or_else(|| "solve-gain requires --ms <measurement-set>".to_string())?;
    let output_table =
        output_table.ok_or_else(|| "solve-gain requires --out <caltable>".to_string())?;
    let refant = refant.ok_or_else(|| "solve-gain requires --refant <antenna>".to_string())?;
    let prior_calibration_tables = build_input_calibration_table_specs_for_solver(
        callib,
        calibration_table_paths,
        gainfield.as_deref(),
        interp.as_deref(),
        spwmap.as_deref(),
    )?;

    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::SolveGain(SolveGainOptions {
            measurement_set,
            output_table,
            gain_type,
            solve_mode,
            solve_interval,
            combine,
            refant,
            prior_calibration_tables,
            parang,
            format,
            output,
            overwrite,
            selection,
        }),
    })))
}

fn parse_solve_bandpass_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut measurement_set = None;
    let mut output_table = None;
    let mut calibration_table_paths = Vec::new();
    let mut callib = None;
    let mut gainfield = None;
    let mut interp = None;
    let mut spwmap = None;
    let mut refant = None;
    let mut parang = false;
    let mut combine = BandpassSolveCombine::default();
    let mut band_type = BandpassType::B;
    let mut normalize_average_amplitude = false;
    let mut amplitude_degree = 3_usize;
    let mut phase_degree = 3_usize;
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;
    let mut selection = SelectionOptions {
        selectdata: true,
        ..SelectionOptions::default()
    };

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--ms" => {
                index += 1;
                measurement_set = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --ms".to_string())?,
                ));
            }
            "--out" => {
                index += 1;
                output_table = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --out".to_string())?,
                ));
            }
            "--gaintables" => {
                index += 1;
                calibration_table_paths.extend(parse_path_list(&take_string_value(
                    index,
                    args,
                    "--gaintables",
                )?));
            }
            "--callib" => {
                index += 1;
                callib = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --callib".to_string())?,
                ));
            }
            "--gainfield" => {
                index += 1;
                gainfield = Some(take_string_value(index, args, "--gainfield")?);
            }
            "--interp" => {
                index += 1;
                interp = Some(take_string_value(index, args, "--interp")?);
            }
            "--spwmap" => {
                index += 1;
                spwmap = Some(take_string_value(index, args, "--spwmap")?);
            }
            "--refant" => {
                index += 1;
                refant = Some(parse_refant_selector(&take_string_value(
                    index, args, "--refant",
                )?));
            }
            "--combine" | "--combine-bandpass" => {
                index += 1;
                combine = parse_bandpass_combine(&take_string_value(index, args, raw)?)?;
            }
            "--bandtype" => {
                index += 1;
                band_type = parse_bandpass_type(&take_string_value(index, args, "--bandtype")?)?;
            }
            "--solnorm" => normalize_average_amplitude = true,
            "--no-solnorm" => normalize_average_amplitude = false,
            "--degamp" => {
                index += 1;
                amplitude_degree =
                    parse_usize_flag("--degamp", &take_string_value(index, args, "--degamp")?)?;
            }
            "--degphase" => {
                index += 1;
                phase_degree =
                    parse_usize_flag("--degphase", &take_string_value(index, args, "--degphase")?)?;
            }
            "--parang" => parang = true,
            "--no-parang" => parang = false,
            "--format" => {
                index += 1;
                format =
                    parse_output_format("--format", &take_string_value(index, args, "--format")?)?;
            }
            "-o" | "--output" => {
                index += 1;
                output = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --output".to_string())?,
                ));
            }
            "--overwrite" => overwrite = true,
            "--selectdata" => selection.selectdata = true,
            "--no-selectdata" => selection.selectdata = false,
            "--field" => {
                index += 1;
                selection.field = Some(take_string_value(index, args, "--field")?);
            }
            "--spw" => {
                index += 1;
                selection.spw = Some(take_string_value(index, args, "--spw")?);
            }
            "--antenna" => {
                index += 1;
                selection.antenna = Some(take_string_value(index, args, "--antenna")?);
            }
            "--scan" => {
                index += 1;
                selection.scan = Some(take_string_value(index, args, "--scan")?);
            }
            "--observation" => {
                index += 1;
                selection.observation = Some(take_string_value(index, args, "--observation")?);
            }
            "--array" => {
                index += 1;
                selection.array = Some(take_string_value(index, args, "--array")?);
            }
            "--timerange" => {
                index += 1;
                selection.timerange = Some(take_string_value(index, args, "--timerange")?);
            }
            "--msselect" => {
                index += 1;
                selection.msselect = Some(take_string_value(index, args, "--msselect")?);
            }
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => return Err(format!("unexpected positional argument {raw:?}")),
        }
        index += 1;
    }

    let measurement_set = measurement_set
        .ok_or_else(|| "solve-bandpass requires --ms <measurement-set>".to_string())?;
    let output_table =
        output_table.ok_or_else(|| "solve-bandpass requires --out <caltable>".to_string())?;
    let refant = refant.ok_or_else(|| "solve-bandpass requires --refant <antenna>".to_string())?;
    let prior_calibration_tables = build_input_calibration_table_specs_for_solver(
        callib,
        calibration_table_paths,
        gainfield.as_deref(),
        interp.as_deref(),
        spwmap.as_deref(),
    )?;

    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::SolveBandpass(SolveBandpassOptions {
            measurement_set,
            output_table,
            refant,
            prior_calibration_tables,
            parang,
            combine,
            band_type,
            normalize_average_amplitude,
            amplitude_degree,
            phase_degree,
            format,
            output,
            overwrite,
            selection,
        }),
    })))
}

fn parse_fluxscale_args(args: &[OsString], managed_output: bool) -> Result<CliAction, String> {
    let mut input_table = None;
    let mut output_table = None;
    let mut reference_fields = Vec::new();
    let mut transfer_fields = Vec::new();
    let mut refspwmap = Vec::new();
    let mut gainthreshold = None;
    let mut incremental = false;
    let mut format = OutputFormat::Text;
    let mut output = None;
    let mut overwrite = false;

    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        match raw {
            "--in" => {
                index += 1;
                input_table = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --in".to_string())?,
                ));
            }
            "--out" => {
                index += 1;
                output_table = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --out".to_string())?,
                ));
            }
            "--reference" => {
                index += 1;
                reference_fields =
                    parse_string_list(&take_string_value(index, args, "--reference")?);
            }
            "--transfer" => {
                index += 1;
                transfer_fields = parse_string_list(&take_string_value(index, args, "--transfer")?);
            }
            "--refspwmap" => {
                index += 1;
                refspwmap = parse_i32_list(
                    "--refspwmap",
                    &take_string_value(index, args, "--refspwmap")?,
                )?;
            }
            "--gainthreshold" => {
                index += 1;
                gainthreshold = Some(
                    take_string_value(index, args, "--gainthreshold")?
                        .parse::<f64>()
                        .map_err(|error| format!("parse --gainthreshold: {error}"))?,
                );
            }
            "--incremental" => incremental = true,
            "--no-incremental" => incremental = false,
            "--format" => {
                index += 1;
                format =
                    parse_output_format("--format", &take_string_value(index, args, "--format")?)?;
            }
            "-o" | "--output" => {
                index += 1;
                output = Some(PathBuf::from(
                    args.get(index)
                        .ok_or_else(|| "missing value for --output".to_string())?,
                ));
            }
            "--overwrite" => overwrite = true,
            _ if raw.starts_with('-') => return Err(format!("unsupported argument {raw:?}")),
            _ => return Err(format!("unexpected positional argument {raw:?}")),
        }
        index += 1;
    }

    let input_table =
        input_table.ok_or_else(|| "fluxscale requires --in <gain-table>".to_string())?;
    let output_table =
        output_table.ok_or_else(|| "fluxscale requires --out <flux-table>".to_string())?;
    if reference_fields.is_empty() {
        return Err("fluxscale requires --reference FIELD[,FIELD...]".to_string());
    }

    Ok(CliAction::Run(Box::new(RunRequest {
        managed_output,
        command: Command::FluxScale(FluxScaleOptions {
            input_table,
            output_table,
            reference_fields,
            transfer_fields,
            refspwmap,
            gainthreshold,
            incremental,
            format,
            output,
            overwrite,
        }),
    })))
}

fn parse_output_format(flag: &str, value: &str) -> Result<OutputFormat, String> {
    match value {
        "text" => Ok(OutputFormat::Text),
        "json" => Ok(OutputFormat::Json),
        other => Err(format!(
            "unsupported {flag} {other:?}; expected text or json"
        )),
    }
}

fn parse_bool_literal(value: &str) -> Result<bool, String> {
    match value {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        other => Err(format!(
            "parse boolean value {other:?}: expected true or false"
        )),
    }
}

fn parse_apply_mode(value: &str) -> Result<ApplyMode, String> {
    match value {
        "calflag" => Ok(ApplyMode::CalFlag),
        "calonly" => Ok(ApplyMode::CalOnly),
        "trial" => Ok(ApplyMode::Trial),
        other => Err(format!(
            "unsupported --apply-mode {other:?}; expected calflag, calonly, or trial"
        )),
    }
}

fn parse_gain_type(value: &str) -> Result<GainType, String> {
    match value {
        "g" | "G" => Ok(GainType::G),
        "t" | "T" => Ok(GainType::T),
        other => Err(format!(
            "unsupported --gain-type {other:?}; expected g or t"
        )),
    }
}

fn parse_gain_solve_mode(value: &str) -> Result<GainSolveMode, String> {
    match value {
        "p" => Ok(GainSolveMode::Phase),
        "ap" => Ok(GainSolveMode::AmplitudePhase),
        other => Err(format!("unsupported --mode {other:?}; expected p or ap")),
    }
}

fn parse_bandpass_type(value: &str) -> Result<BandpassType, String> {
    match value.to_ascii_lowercase().as_str() {
        "b" => Ok(BandpassType::B),
        "bpoly" => Ok(BandpassType::BPoly),
        other => Err(format!(
            "unsupported --bandtype {other:?}; expected b or bpoly"
        )),
    }
}

fn parse_gain_solve_interval(value: &str) -> Result<GainSolveInterval, String> {
    match value {
        "inf" => Ok(GainSolveInterval::Infinite),
        "int" => Ok(GainSolveInterval::Integration),
        other => {
            let raw = other.strip_suffix('s').unwrap_or(other);
            let seconds = raw
                .parse::<f64>()
                .map_err(|error| format!("parse --solint {other:?}: {error}"))?;
            Ok(GainSolveInterval::Seconds(seconds))
        }
    }
}

fn parse_gain_solve_combine(value: &str) -> Result<GainSolveCombine, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Ok(GainSolveCombine::default());
    }

    let mut combine = GainSolveCombine::default();
    for axis in trimmed
        .split(',')
        .map(str::trim)
        .filter(|axis| !axis.is_empty())
    {
        match axis {
            "scan" => combine.scans = true,
            "field" => combine.fields = true,
            other => {
                return Err(format!(
                    "unsupported --combine axis {other:?}; expected scan and/or field"
                ));
            }
        }
    }
    Ok(combine)
}

fn parse_bandpass_combine(value: &str) -> Result<BandpassSolveCombine, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "none" {
        return Ok(BandpassSolveCombine::default());
    }

    let mut combine = BandpassSolveCombine::default();
    for part in trimmed
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        match part {
            "scan" => combine.scans = true,
            "field" => combine.fields = true,
            other => {
                return Err(format!(
                    "unsupported --combine {other:?}; expected scan, field, scan,field, or none"
                ));
            }
        }
    }
    Ok(combine)
}

fn parse_usize_flag(flag: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("unsupported {flag} {value:?}; expected a non-negative integer"))
}

fn parse_refant_selector(value: &str) -> RefAntSelector {
    match value.parse::<i32>() {
        Ok(id) => RefAntSelector::AntennaId(id),
        Err(_) => RefAntSelector::AntennaName(value.to_string()),
    }
}

fn take_string_value(index: usize, args: &[OsString], flag: &str) -> Result<String, String> {
    args.get(index)
        .and_then(|value| value.to_str())
        .map(ToString::to_string)
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn extract_option_value(
    args: &[OsString],
    flag: &str,
) -> Result<(Option<String>, Vec<OsString>), String> {
    let mut remaining = Vec::with_capacity(args.len());
    let mut value = None;
    let mut index = 0;

    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
        if raw == flag {
            if value.is_some() {
                return Err(format!("duplicate {flag}"));
            }
            index += 1;
            value = Some(take_string_value(index, args, flag)?);
        } else {
            remaining.push(args[index].clone());
        }
        index += 1;
    }

    Ok((value, remaining))
}

fn parse_path_list(value: &str) -> Vec<PathBuf> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn parse_string_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_semicolon_segments(value: &str) -> Vec<String> {
    if !value.contains(';') {
        return vec![value.trim().to_string()];
    }
    value
        .split(';')
        .map(|item| item.trim().to_string())
        .collect()
}

fn expand_aligned_values<T: Clone>(
    flag: &str,
    values: Vec<T>,
    len: usize,
    default: T,
) -> Result<Vec<T>, String> {
    if values.is_empty() {
        Ok(vec![default; len])
    } else if values.len() == 1 {
        Ok(vec![values[0].clone(); len])
    } else if values.len() == len {
        Ok(values)
    } else {
        Err(format!(
            "{flag} provided {} values for {} calibration tables; expected one value or one per table",
            values.len(),
            len
        ))
    }
}

fn parse_gainfield_value(_flag: &str, value: &str) -> Result<Option<GainFieldSelector>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.eq_ignore_ascii_case("nearest") {
        return Ok(Some(GainFieldSelector::Nearest));
    }
    if let Ok(field_id) = value.parse::<i32>() {
        return Ok(Some(GainFieldSelector::FieldId(field_id)));
    }
    Ok(Some(GainFieldSelector::FieldName(value.to_string())))
}

fn parse_optional_gainfield_list(
    flag: &str,
    value: &str,
) -> Result<Vec<Option<GainFieldSelector>>, String> {
    parse_semicolon_segments(value)
        .into_iter()
        .map(|segment| parse_gainfield_value(flag, &segment))
        .collect()
}

fn parse_interp_value(flag: &str, value: &str) -> Result<ApplyInterpolationMode, String> {
    let normalized = value.trim().to_ascii_lowercase().replace(' ', "");
    match normalized.as_str() {
        "" | "nearest" => Ok(ApplyInterpolationMode::Nearest),
        "linear" => Ok(ApplyInterpolationMode::Linear),
        "nearest,linear" | "nearestlinear" => Ok(ApplyInterpolationMode::NearestLinear),
        _ => Err(format!(
            "{flag} value {value:?} is unsupported; expected nearest, linear, or nearest,linear"
        )),
    }
}

fn parse_interp_list(flag: &str, value: &str) -> Result<Vec<ApplyInterpolationMode>, String> {
    parse_semicolon_segments(value)
        .into_iter()
        .map(|segment| parse_interp_value(flag, &segment))
        .collect()
}

fn parse_spwmap_value(flag: &str, value: &str) -> Result<Vec<i32>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let inner = value
        .strip_prefix('[')
        .and_then(|candidate| candidate.strip_suffix(']'))
        .unwrap_or(value);
    parse_i32_list(flag, inner)
}

fn parse_spwmap_list(flag: &str, value: &str) -> Result<Vec<Vec<i32>>, String> {
    parse_semicolon_segments(value)
        .into_iter()
        .map(|segment| parse_spwmap_value(flag, &segment))
        .collect()
}

fn build_calibration_table_specs(
    paths: Vec<PathBuf>,
    gainfield: Option<&str>,
    interp: Option<&str>,
    spwmap: Option<&str>,
    calwt: Option<&str>,
) -> Result<Vec<ApplyCalibrationTableSpec>, String> {
    let gainfield_values = match gainfield {
        Some(raw) => parse_optional_gainfield_list("--gainfield", raw)?,
        None => Vec::new(),
    };
    let interp_values = match interp {
        Some(raw) => parse_interp_list("--interp", raw)?,
        None => Vec::new(),
    };
    let spwmap_values = match spwmap {
        Some(raw) => parse_spwmap_list("--spwmap", raw)?,
        None => Vec::new(),
    };
    let calwt_values = match calwt {
        Some(raw) => parse_bool_list("--calwt", raw)?,
        None => Vec::new(),
    };
    let expanded_gainfield =
        expand_aligned_values("--gainfield", gainfield_values, paths.len(), None)?;
    let expanded_interp = expand_aligned_values(
        "--interp",
        interp_values,
        paths.len(),
        ApplyInterpolationMode::Nearest,
    )?;
    let expanded_spwmap =
        expand_aligned_values("--spwmap", spwmap_values, paths.len(), Vec::new())?;
    let expanded_calwt = if calwt_values.is_empty() {
        vec![false; paths.len()]
    } else if calwt_values.len() == 1 {
        vec![calwt_values[0]; paths.len()]
    } else if calwt_values.len() == paths.len() {
        calwt_values
    } else {
        return Err(format!(
            "--calwt provided {} values for {} calibration tables; expected one value or one per table",
            calwt_values.len(),
            paths.len()
        ));
    };

    Ok(paths
        .into_iter()
        .zip(expanded_gainfield)
        .zip(expanded_interp)
        .zip(expanded_spwmap)
        .zip(expanded_calwt)
        .map(|((((path, gainfield), interp), spwmap), calwt)| {
            let mut spec = ApplyCalibrationTableSpec::new(path);
            spec.gainfield = gainfield;
            spec.interp = interp;
            spec.spwmap = spwmap;
            spec.calwt = calwt;
            spec
        })
        .collect())
}

fn build_input_calibration_table_specs(
    callib: Option<PathBuf>,
    calibration_table_paths: Vec<PathBuf>,
    gainfield: Option<&str>,
    interp: Option<&str>,
    spwmap: Option<&str>,
    calwt: Option<&str>,
) -> Result<Vec<ApplyCalibrationTableSpec>, String> {
    match (callib, calibration_table_paths.is_empty()) {
        (Some(_), false) => {
            Err("pass either --callib PATH or --gaintables / positional caltables, not both".to_string())
        }
        (Some(callib), true) => {
            if gainfield.is_some() || interp.is_some() || spwmap.is_some() || calwt.is_some() {
                return Err("--gainfield, --interp, --spwmap, and --calwt cannot be combined with --callib; set per-table chain controls inside the callibrary file".to_string());
            }
            load_apply_specs_from_callib(&callib).map_err(|error| error.to_string())
        }
        (None, true) => {
            Err("apply requires calibration input; pass --callib PATH or --gaintables PATH[,PATH...] or positional caltable paths".to_string())
        }
        (None, false) => build_calibration_table_specs(
            calibration_table_paths,
            gainfield,
            interp,
            spwmap,
            calwt,
        ),
    }
}

fn build_input_calibration_table_specs_for_solver(
    callib: Option<PathBuf>,
    calibration_table_paths: Vec<PathBuf>,
    gainfield: Option<&str>,
    interp: Option<&str>,
    spwmap: Option<&str>,
) -> Result<Vec<ApplyCalibrationTableSpec>, String> {
    match (callib, calibration_table_paths.is_empty()) {
        (Some(_), false) => {
            Err("pass either --callib PATH or --gaintables PATH[,PATH...], not both".to_string())
        }
        (Some(callib), true) => {
            if gainfield.is_some() || interp.is_some() || spwmap.is_some() {
                return Err("--gainfield, --interp, and --spwmap cannot be combined with --callib; set per-table chain controls inside the callibrary file".to_string());
            }
            load_apply_specs_from_callib(&callib).map_err(|error| error.to_string())
        }
        (None, true) => Ok(Vec::new()),
        (None, false) => {
            build_calibration_table_specs(calibration_table_paths, gainfield, interp, spwmap, None)
        }
    }
}

fn parse_i32_list(flag: &str, value: &str) -> Result<Vec<i32>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|item| {
            item.parse::<i32>()
                .map_err(|error| format!("parse {flag} value {item:?}: {error}"))
        })
        .collect()
}

fn parse_bool_list(flag: &str, value: &str) -> Result<Vec<bool>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|item| {
            parse_bool_literal(item)
                .map_err(|_| format!("parse {flag} value {item:?}: expected true/false"))
        })
        .collect()
}

fn parse_time_range(value: &str) -> Result<(f64, f64), String> {
    let (start, end) = value
        .split_once(':')
        .ok_or_else(|| "expected --timerange START:END in MJD seconds".to_string())?;
    let start = start
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("parse timerange start {start:?}: {error}"))?;
    let end = end
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("parse timerange end {end:?}: {error}"))?;
    Ok((start, end))
}

fn render_help(schema: &UiCommandSchema) -> String {
    format!(
        "{}\n\nDeveloper subcommands:\n  {} summary [SUMMARY OPTIONS] <caltable>...\n  {} stats [STATS OPTIONS] <caltable>\n  {} plan-apply --ms <measurement-set> [PLAN OPTIONS] <caltable>...\n  {} solve-gain --ms <measurement-set> --out <caltable> --refant <antenna> [SOLVE OPTIONS]\n  {} solve-bandpass --ms <measurement-set> --out <caltable> --refant <antenna> [BANDPASS OPTIONS]\n  {} fluxscale --in <gain-table> --out <flux-table> --reference FIELD[,FIELD...] [FLUXSCALE OPTIONS]\n",
        schema.render_help(),
        schema.invocation_name,
        schema.invocation_name,
        schema.invocation_name,
        schema.invocation_name,
        schema.invocation_name,
        schema.invocation_name
    )
}

fn render_summary_text(summaries: &[CalibrationTableSummary]) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    for (index, summary) in summaries.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let _ = writeln!(out, "Calibration Table: {}", summary.path.display());
        let _ = writeln!(
            out,
            "  type={} subtype={}",
            summary.table_type, summary.table_subtype
        );
        let _ = writeln!(out, "  rows={}", summary.row_count);
        let _ = writeln!(out, "  par_type={:?}", summary.keywords.par_type);
        let _ = writeln!(out, "  vis_cal={:?}", summary.keywords.vis_cal);
        let _ = writeln!(out, "  parameter_family={:?}", summary.parameter_family);
        let _ = writeln!(
            out,
            "  supported_for_v1_apply={}",
            summary.supported_for_v1_apply()
        );
        let _ = writeln!(out, "  columns={}", summary.columns.join(", "));
        let _ = writeln!(out, "  field_ids={:?}", summary.field_ids);
        let _ = writeln!(
            out,
            "  spectral_window_ids={:?}",
            summary.spectral_window_ids
        );
        let _ = writeln!(out, "  antenna1_ids={:?}", summary.antenna1_ids);
        let _ = writeln!(out, "  antenna2_ids={:?}", summary.antenna2_ids);
        let _ = writeln!(out, "  observation_ids={:?}", summary.observation_ids);
        if let Some(time) = &summary.time_coverage {
            let _ = writeln!(
                out,
                "  time_coverage=[{}, {}] interval={:?}..{:?}",
                time.min_time, time.max_time, time.min_interval, time.max_interval
            );
        }
        for subtable in &summary.subtables {
            let _ = writeln!(
                out,
                "  subtable {} exists={} rows={:?} path={}",
                subtable.name,
                subtable.exists,
                subtable.row_count,
                subtable
                    .resolved_path
                    .as_deref()
                    .unwrap_or_else(|| Path::new("<missing>"))
                    .display()
            );
        }
        if summary.issues.is_empty() {
            let _ = writeln!(out, "  issues=none");
        } else {
            let _ = writeln!(out, "  issues:");
            for issue in &summary.issues {
                let _ = writeln!(
                    out,
                    "    - {:?} {}: {}",
                    issue.severity, issue.code, issue.message
                );
            }
        }
    }
    out
}

fn render_apply_report_text(report: &ApplyExecutionReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Calibration Apply Report: {}",
        report
            .plan
            .measurement_set_path
            .as_deref()
            .unwrap_or_else(|| Path::new("<in-memory>"))
            .display()
    );
    let _ = writeln!(out, "  apply_mode={:?}", report.plan.apply_mode);
    let _ = writeln!(out, "  parang={}", report.plan.parang);
    let _ = writeln!(out, "  selected_rows={}", report.plan.selected_row_count);
    let _ = writeln!(
        out,
        "  created_corrected_data_column={}",
        report.created_corrected_data_column
    );
    let _ = writeln!(
        out,
        "  wrote_measurement_set={}",
        report.wrote_measurement_set
    );
    let _ = writeln!(out, "  updated_rows={}", report.updated_row_count);
    let _ = writeln!(out, "  flagged_rows={}", report.flagged_row_count);
    let _ = writeln!(out, "  flagged_samples={}", report.flagged_sample_count);
    let _ = writeln!(out, "  timings:");
    let _ = writeln!(
        out,
        "    total={} planning={} open_ms={}",
        format_duration_ns(report.timings.total_ns),
        format_duration_ns(report.timings.planning_ns),
        format_duration_ns(report.timings.open_measurement_set_ns)
    );
    let _ = writeln!(
        out,
        "    planning.selection={} planning.selected_rows={} planning.ms_spws={} planning.table_plans={}",
        format_duration_ns(report.timings.planning_selection_ns),
        format_duration_ns(report.timings.planning_selected_rows_ns),
        format_duration_ns(report.timings.planning_measurement_set_spectral_windows_ns),
        format_duration_ns(report.timings.planning_calibration_table_plans_ns)
    );
    let _ = writeln!(
        out,
        "    ensure_corrected_data={} correlation_lookup={} calibration_load={}",
        format_duration_ns(report.timings.ensure_corrected_data_ns),
        format_duration_ns(report.timings.correlation_lookup_ns),
        format_duration_ns(report.timings.calibration_load_ns)
    );
    let _ = writeln!(
        out,
        "    row_compute={} row_writeback={} save={}",
        format_duration_ns(report.timings.row_compute_ns),
        format_duration_ns(report.timings.row_writeback_ns),
        format_duration_ns(report.timings.save_ns)
    );
    for table in &report.plan.calibration_tables {
        let _ = writeln!(out, "  table {}", table.spec.path.display());
        let _ = writeln!(
            out,
            "    applicable_selected_rows={}",
            table.applicable_selected_row_count
        );
        let _ = writeln!(out, "    interp={:?}", table.interp);
        let _ = writeln!(out, "    calwt={}", table.calwt);
        if !table.spec.apply_to.is_empty() {
            let _ = writeln!(
                out,
                "    apply_to={}",
                format_apply_table_selection(&table.spec.apply_to)
            );
        }
        if let Some(gainfield) = &table.resolved_gainfield {
            let _ = writeln!(
                out,
                "    gainfield={} {:?}",
                gainfield.field_id, gainfield.field_name
            );
        }
        for nearest in &table.resolved_nearest_gainfields {
            let _ = writeln!(
                out,
                "    gainfield[ms_field={}] -> {} {:?} sep={:.6}rad",
                nearest.measurement_set_field_id,
                nearest.calibration_field_id,
                nearest.calibration_field_name,
                nearest.angular_separation_rad
            );
        }
    }
    out
}

fn format_duration_ns(duration_ns: u64) -> String {
    if duration_ns >= 1_000_000_000 {
        format!("{:.3}s", duration_ns as f64 / 1_000_000_000.0)
    } else if duration_ns >= 1_000_000 {
        format!("{:.3}ms", duration_ns as f64 / 1_000_000.0)
    } else if duration_ns >= 1_000 {
        format!("{:.3}us", duration_ns as f64 / 1_000.0)
    } else {
        format!("{duration_ns}ns")
    }
}

fn render_apply_plan_text(plan: &ApplyPlan) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Apply Plan: {}",
        plan.measurement_set_path
            .as_deref()
            .unwrap_or_else(|| Path::new("<in-memory>"))
            .display()
    );
    let _ = writeln!(out, "  apply_mode={:?}", plan.apply_mode);
    let _ = writeln!(out, "  parang={}", plan.parang);
    let _ = writeln!(
        out,
        "  requires_corrected_data_column={}",
        plan.requires_corrected_data_column
    );
    let _ = writeln!(out, "  selected_rows={}", plan.selected_row_count);
    let _ = writeln!(out, "  selected_fields={:?}", plan.selected_field_ids);
    let _ = writeln!(
        out,
        "  selected_data_desc_ids={:?}",
        plan.selected_data_desc_ids
    );
    let _ = writeln!(out, "  selected_data_spws={:?}", plan.selected_data_spw_ids);
    for table in &plan.calibration_tables {
        let _ = writeln!(out, "  table {}", table.spec.path.display());
        let _ = writeln!(out, "    vis_cal={:?}", table.summary.keywords.vis_cal);
        let _ = writeln!(
            out,
            "    applicable_selected_rows={}",
            table.applicable_selected_row_count
        );
        let _ = writeln!(out, "    interp={:?}", table.interp);
        let _ = writeln!(out, "    calwt={}", table.calwt);
        if !table.spec.apply_to.is_empty() {
            let _ = writeln!(
                out,
                "    apply_to={}",
                format_apply_table_selection(&table.spec.apply_to)
            );
        }
        if let Some(gainfield) = &table.resolved_gainfield {
            let _ = writeln!(
                out,
                "    gainfield={} {:?}",
                gainfield.field_id, gainfield.field_name
            );
        }
        for nearest in &table.resolved_nearest_gainfields {
            let _ = writeln!(
                out,
                "    gainfield[ms_field={}] -> {} {:?} sep={:.6}rad",
                nearest.measurement_set_field_id,
                nearest.calibration_field_id,
                nearest.calibration_field_name,
                nearest.angular_separation_rad
            );
        }
        for mapping in &table.spw_mapping {
            let _ = writeln!(
                out,
                "    spw {} -> {}",
                mapping.data_spw_id, mapping.calibration_spw_id
            );
        }
    }
    out
}

fn format_apply_table_selection(selection: &crate::ApplyTableSelection) -> String {
    let mut parts = Vec::new();
    if !selection.field_ids.is_empty() {
        parts.push(format!("field={:?}", selection.field_ids));
    }
    if !selection.spectral_window_ids.is_empty() {
        parts.push(format!("spw={:?}", selection.spectral_window_ids));
    }
    if !selection.observation_ids.is_empty() {
        parts.push(format!("obs={:?}", selection.observation_ids));
    }
    parts.join(" ")
}

fn render_stats_text(report: &CalibrationStatsReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(out, "Calibration Stats: {}", report.path.display());
    let _ = writeln!(out, "  axis={}", report.axis.display_name());
    let _ = writeln!(out, "  datacolumn={:?}", report.datacolumn);
    let _ = writeln!(out, "  rows={}", report.row_count);
    let _ = writeln!(out, "  global:");
    render_value_stats_block(&mut out, &report.global, 4);

    for (label, groups) in [
        ("field_id", &report.by_field_id),
        ("spectral_window_id", &report.by_spectral_window_id),
        ("antenna1_id", &report.by_antenna1_id),
        ("observation_id", &report.by_observation_id),
    ] {
        let _ = writeln!(out, "  grouped_by_{label}:");
        for group in groups {
            let _ = writeln!(out, "    {}={}", label, group.key);
            render_value_stats_block(&mut out, &group.stats, 6);
        }
    }

    out
}

fn render_gain_solve_report_text(report: &GainSolveReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(out, "Gain Solve Report: {}", report.output_table.display());
    let _ = writeln!(out, "  gain_type={:?}", report.gain_type);
    let _ = writeln!(out, "  refant_antenna_id={}", report.refant_antenna_id);
    let _ = writeln!(out, "  field_ids={:?}", report.field_ids);
    let _ = writeln!(
        out,
        "  spectral_window_ids={:?}",
        report.spectral_window_ids
    );
    let _ = writeln!(out, "  solution_rows={}", report.solution_row_count);
    out
}

fn render_bandpass_solve_report_text(report: &BandpassSolveReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Bandpass Solve Report: {}",
        report.output_table.display()
    );
    let _ = writeln!(out, "  refant_antenna_id={}", report.refant_antenna_id);
    let _ = writeln!(out, "  field_ids={:?}", report.field_ids);
    let _ = writeln!(
        out,
        "  spectral_window_ids={:?}",
        report.spectral_window_ids
    );
    let _ = writeln!(out, "  solution_rows={}", report.solution_row_count);
    let _ = writeln!(out, "  channel_count={}", report.channel_count);
    out
}

fn render_fluxscale_report_text(report: &FluxScaleReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(out, "FluxScale Report: {}", report.output_table.display());
    let _ = writeln!(out, "  spw_ids={:?}", report.spw_ids);
    let _ = writeln!(out, "  frequencies_hz={:?}", report.frequencies_hz);
    for (field_id, field) in &report.fields {
        let _ = writeln!(out, "  field {field_id} ({})", field.field_name);
        for (spw_id, spw) in &field.spw_results {
            let _ = writeln!(
                out,
                "    spw {spw_id}: fluxd={:?} fluxd_err={:?} num_sol={:?}",
                spw.fluxd, spw.fluxd_err, spw.num_sol
            );
        }
    }
    out
}

fn render_value_stats_block(out: &mut String, stats: &crate::CalibrationValueStats, indent: usize) {
    use std::fmt::Write;

    let prefix = " ".repeat(indent);
    let _ = writeln!(
        out,
        "{prefix}npts={} flagged_npts={} total_npts={}",
        stats.npts, stats.flagged_npts, stats.total_npts
    );
    let _ = writeln!(
        out,
        "{prefix}min={:.6} max={:.6} mean={:.6} median={:.6}",
        stats.min, stats.max, stats.mean, stats.median
    );
    let _ = writeln!(
        out,
        "{prefix}sum={:.6} sumsq={:.6} rms={:.6}",
        stats.sum, stats.sumsq, stats.rms
    );
    let _ = writeln!(
        out,
        "{prefix}stddev={:.6} var={:.6} medabsdevmed={:.6} quartile={:.6}",
        stats.stddev, stats.var, stats.medabsdevmed, stats.quartile
    );
}

fn write_output(path: Option<&Path>, overwrite: bool, text: &str) -> Result<(), String> {
    match path {
        Some(path) => {
            if path.exists() && !overwrite {
                return Err(format!(
                    "refusing to overwrite existing output {}; pass --overwrite to replace it",
                    path.display()
                ));
            }
            fs::write(path, text).map_err(|error| format!("write {}: {error}", path.display()))
        }
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

fn render_json_output<T: serde::Serialize>(
    managed_output: bool,
    raw: &T,
    wrapped: ManagedCalibrationOutput,
    label: &str,
) -> Result<String, String> {
    if managed_output {
        serde_json::to_string_pretty(&wrapped)
            .map_err(|error| format!("serialize managed {label}: {error}"))
    } else {
        serde_json::to_string_pretty(raw).map_err(|error| format!("serialize {label}: {error}"))
    }
}

fn option_argument(config: OptionArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Option {
            flags: config
                .flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
            metavar: config.metavar.to_string(),
            choices: config
                .choices
                .iter()
                .map(|choice| (*choice).to_string())
                .collect(),
        },
        value_kind: config.value_kind,
        required: config.required,
        default: config.default.map(ToString::to_string),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: config.advanced,
        hidden_in_tui: false,
    }
}

fn toggle_argument(config: ToggleArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Toggle {
            true_flags: config
                .true_flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
            false_flags: config
                .false_flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
        },
        value_kind: UiValueKind::Bool,
        required: false,
        default: Some(config.default.to_string()),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: config.advanced,
        hidden_in_tui: false,
    }
}

fn action_argument(
    order: usize,
    id: &str,
    flags: &[&str],
    action: UiActionKind,
) -> UiArgumentSchema {
    let help = match action {
        UiActionKind::Help => "Show this help message",
        UiActionKind::UiSchema => "Print the machine-readable UI schema for this command",
    };
    UiArgumentSchema {
        id: id.to_string(),
        label: id.to_string(),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: help.to_string(),
        group: "Meta".to_string(),
        advanced: true,
        hidden_in_tui: true,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use casacore_ms::msexplore::cli::UiValueKind;
    use tempfile::TempDir;

    use super::{CliAction, Command, OutputFormat, command_schema, parse_args};
    use crate::{
        ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, BandpassSolveCombine,
        BandpassType, GainFieldSelector, GainSolveInterval, GainSolveMode, GainType,
        RefAntSelector,
    };

    #[test]
    fn command_schema_describes_public_workflow_surface() {
        let schema = command_schema("calibrate-test");
        assert_eq!(schema.command_id, "calibrate");
        assert_eq!(schema.display_name, "Calibrate");
        assert_eq!(schema.category, "Calibration");
        assert!(schema.usage.contains("calibrate-test --mode MODE"));
        let workflow_mode = schema.argument("mode").expect("workflow mode");
        assert_eq!(workflow_mode.value_kind, UiValueKind::Choice);
        assert_eq!(workflow_mode.default.as_deref(), Some("apply"));
        let measurement_set = schema.argument("measurement_set").expect("measurement_set");
        assert_eq!(measurement_set.value_kind, UiValueKind::Path);
        assert_eq!(measurement_set.group, "Input");
        assert!(schema.argument("summary_paths").is_some());
        assert!(schema.argument("table_path").is_some());
        let gaintables = schema.argument("gaintables").expect("gaintables");
        assert!(!gaintables.required);
        assert!(schema.argument("callib").is_some());
        let apply_mode = schema.argument("apply_mode").expect("apply mode");
        assert_eq!(apply_mode.default.as_deref(), Some("calflag"));
        assert!(schema.argument("refant").is_some());
        assert!(schema.argument("out_table").is_some());
        assert!(schema.argument("calwt").is_some());
        let managed_output = schema.managed_output.expect("managed output");
        assert_eq!(managed_output.renderer, "calibration-report-v1");
        assert_eq!(managed_output.stdout_format, "json");
        assert!(
            managed_output
                .inject_arguments
                .iter()
                .any(|argument| argument.flag == "--format" && argument.value == "json")
        );
        assert!(
            managed_output
                .inject_arguments
                .iter()
                .any(|argument| argument.flag == "--managed-output" && argument.value == "true")
        );
    }

    #[test]
    fn parse_args_defaults_to_apply_mode() {
        let action = parse_args([
            "dataset.ms".into(),
            "--gaintables".into(),
            "phase.gcal".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Apply(options) = request.command else {
                    panic!("expected apply action");
                };
                assert_eq!(options.measurement_set, PathBuf::from("dataset.ms"));
                assert_eq!(
                    options.calibration_tables,
                    vec![ApplyCalibrationTableSpec::new("phase.gcal")]
                );
                assert_eq!(options.apply_mode, ApplyMode::CalFlag);
                assert_eq!(options.format, OutputFormat::Text);
            }
            _ => panic!("expected apply action"),
        }
    }

    #[test]
    fn parse_args_accepts_apply_json_and_selection_flags() {
        let action = parse_args([
            "apply".into(),
            "dataset.ms".into(),
            "--gaintables".into(),
            "phase.gcal,bandpass.bcal".into(),
            "--calwt".into(),
            "true,false".into(),
            "--apply-mode".into(),
            "trial".into(),
            "--format".into(),
            "json".into(),
            "--spw".into(),
            "0,1".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Apply(options) = request.command else {
                    panic!("expected apply action");
                };
                assert_eq!(
                    options.calibration_tables,
                    vec![
                        {
                            let mut spec = ApplyCalibrationTableSpec::new("phase.gcal");
                            spec.calwt = true;
                            spec
                        },
                        {
                            let mut spec = ApplyCalibrationTableSpec::new("bandpass.bcal");
                            spec.calwt = false;
                            spec
                        },
                    ]
                );
                assert_eq!(options.apply_mode, ApplyMode::Trial);
                assert_eq!(options.format, OutputFormat::Json);
                assert_eq!(options.selection.spw.as_deref(), Some("0,1"));
            }
            _ => panic!("expected apply action"),
        }
    }

    #[test]
    fn parse_args_accepts_aligned_apply_chain_metadata_lists() {
        let action = parse_args([
            "apply".into(),
            "dataset.ms".into(),
            "--gaintables".into(),
            "phase.gcal,bandpass.bcal".into(),
            "--gainfield".into(),
            "nearest;0".into(),
            "--interp".into(),
            "linear;nearest,linear".into(),
            "--spwmap".into(),
            ";0,0".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Apply(options) = request.command else {
                    panic!("expected apply action");
                };
                assert_eq!(
                    options.calibration_tables,
                    vec![
                        {
                            let mut spec = ApplyCalibrationTableSpec::new("phase.gcal");
                            spec.gainfield = Some(GainFieldSelector::Nearest);
                            spec.interp = ApplyInterpolationMode::Linear;
                            spec
                        },
                        {
                            let mut spec = ApplyCalibrationTableSpec::new("bandpass.bcal");
                            spec.gainfield = Some(GainFieldSelector::FieldId(0));
                            spec.interp = ApplyInterpolationMode::NearestLinear;
                            spec.spwmap = vec![0, 0];
                            spec
                        },
                    ]
                );
            }
            _ => panic!("expected apply action"),
        }
    }

    #[test]
    fn parse_args_accepts_apply_workflow_mode_schema_form() {
        let action = parse_args([
            "--mode".into(),
            "apply".into(),
            "--ms".into(),
            "dataset.ms".into(),
            "--gaintables".into(),
            "phase.gcal".into(),
            "--apply-mode".into(),
            "calonly".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Apply(options) = request.command else {
                    panic!("expected apply action");
                };
                assert_eq!(options.measurement_set, PathBuf::from("dataset.ms"));
                assert_eq!(
                    options.calibration_tables,
                    vec![ApplyCalibrationTableSpec::new("phase.gcal")]
                );
                assert_eq!(options.apply_mode, ApplyMode::CalOnly);
            }
            _ => panic!("expected apply action"),
        }
    }

    #[test]
    fn parse_args_accepts_callib_apply_input() {
        let dir = TempDir::new().expect("tempdir");
        let callib = dir.path().join("apply.callib");
        fs::write(&callib, "caltable='phase.gcal' calwt=F tinterp='nearest'\n")
            .expect("write callib");

        let action = parse_args([
            "dataset.ms".into(),
            "--callib".into(),
            callib.as_os_str().into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Apply(options) = request.command else {
                    panic!("expected apply action");
                };
                assert_eq!(options.measurement_set, PathBuf::from("dataset.ms"));
                assert_eq!(options.calibration_tables.len(), 1);
                assert_eq!(
                    options.calibration_tables[0].path,
                    dir.path().join("phase.gcal")
                );
            }
            _ => panic!("expected apply action"),
        }
    }

    #[test]
    fn parse_args_accepts_summary_subcommand() {
        let action = parse_args([
            "summary".into(),
            "--summary-format".into(),
            "json".into(),
            "example.gcal".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Summarize(options) = request.command else {
                    panic!("expected summary action");
                };
                assert_eq!(options.format, OutputFormat::Json);
                assert_eq!(options.paths, vec![PathBuf::from("example.gcal")]);
            }
            _ => panic!("expected summary action"),
        }
    }

    #[test]
    fn parse_args_accepts_summary_workflow_mode_schema_form() {
        let action = parse_args([
            "--mode".into(),
            "summary".into(),
            "--summary-paths".into(),
            "phase.gcal,bandpass.bcal".into(),
            "--format".into(),
            "json".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Summarize(options) = request.command else {
                    panic!("expected summary action");
                };
                assert_eq!(options.format, OutputFormat::Json);
                assert_eq!(
                    options.paths,
                    vec![PathBuf::from("phase.gcal"), PathBuf::from("bandpass.bcal")]
                );
            }
            _ => panic!("expected summary action"),
        }
    }

    #[test]
    fn parse_args_accepts_plan_apply_command() {
        let action = parse_args([
            "plan-apply".into(),
            "--ms".into(),
            "dataset.ms".into(),
            "--calwt".into(),
            "true".into(),
            "--plan-format".into(),
            "json".into(),
            "phase.gcal".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::PlanApply(options) = request.command else {
                    panic!("expected plan-apply action");
                };
                assert_eq!(options.measurement_set, PathBuf::from("dataset.ms"));
                assert_eq!(
                    options.calibration_tables,
                    vec![{
                        let mut spec = ApplyCalibrationTableSpec::new("phase.gcal");
                        spec.calwt = true;
                        spec
                    }]
                );
                assert_eq!(options.format, OutputFormat::Json);
            }
            _ => panic!("expected plan-apply action"),
        }
    }

    #[test]
    fn parse_args_accepts_stats_command() {
        let action = parse_args([
            "stats".into(),
            "--axis".into(),
            "phase".into(),
            "--datacolumn".into(),
            "CPARAM".into(),
            "--use-flags".into(),
            "--stats-format".into(),
            "json".into(),
            "example.gcal".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Stats(options) = request.command else {
                    panic!("expected stats action");
                };
                assert_eq!(options.path, PathBuf::from("example.gcal"));
                assert_eq!(options.axis, crate::CalibrationStatsAxis::Phase);
                assert_eq!(options.datacolumn.as_deref(), Some("CPARAM"));
                assert!(options.use_flags);
                assert_eq!(options.format, OutputFormat::Json);
            }
            _ => panic!("expected stats action"),
        }
    }

    #[test]
    fn parse_args_accepts_stats_workflow_mode_schema_form() {
        let action = parse_args([
            "--mode".into(),
            "stats".into(),
            "--table".into(),
            "example.gcal".into(),
            "--axis".into(),
            "real".into(),
            "--format".into(),
            "json".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::Stats(options) = request.command else {
                    panic!("expected stats action");
                };
                assert_eq!(options.path, PathBuf::from("example.gcal"));
                assert_eq!(options.axis, crate::CalibrationStatsAxis::Real);
                assert_eq!(options.format, OutputFormat::Json);
            }
            _ => panic!("expected stats action"),
        }
    }

    #[test]
    fn parse_args_accepts_solve_gain_command() {
        let action = parse_args([
            "solve-gain".into(),
            "--ms".into(),
            "dataset.ms".into(),
            "--out".into(),
            "phase.gcal".into(),
            "--refant".into(),
            "VA15".into(),
            "--gain-type".into(),
            "t".into(),
            "--mode".into(),
            "ap".into(),
            "--solint".into(),
            "30s".into(),
            "--combine".into(),
            "scan,field".into(),
            "--gaintables".into(),
            "prior.gcal".into(),
            "--format".into(),
            "json".into(),
            "--field".into(),
            "0".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::SolveGain(options) = request.command else {
                    panic!("expected solve-gain action");
                };
                assert_eq!(options.measurement_set, PathBuf::from("dataset.ms"));
                assert_eq!(options.output_table, PathBuf::from("phase.gcal"));
                assert_eq!(options.gain_type, GainType::T);
                assert_eq!(options.solve_mode, GainSolveMode::AmplitudePhase);
                assert_eq!(options.solve_interval, GainSolveInterval::Seconds(30.0));
                assert!(options.combine.scans);
                assert!(options.combine.fields);
                assert_eq!(
                    options.refant,
                    RefAntSelector::AntennaName("VA15".to_string())
                );
                assert_eq!(options.prior_calibration_tables.len(), 1);
                assert_eq!(options.format, OutputFormat::Json);
                assert_eq!(options.selection.field.as_deref(), Some("0"));
            }
            _ => panic!("expected solve-gain action"),
        }
    }

    #[test]
    fn parse_args_accepts_solve_gain_workflow_mode_schema_form() {
        let action = parse_args([
            "--mode".into(),
            "solve_gain".into(),
            "--ms".into(),
            "dataset.ms".into(),
            "--out".into(),
            "phase.gcal".into(),
            "--refant".into(),
            "VA15".into(),
            "--mode-gain".into(),
            "ap".into(),
            "--combine-gain".into(),
            "scan,field".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::SolveGain(options) = request.command else {
                    panic!("expected solve-gain action");
                };
                assert_eq!(options.solve_mode, GainSolveMode::AmplitudePhase);
                assert!(options.combine.scans);
                assert!(options.combine.fields);
            }
            _ => panic!("expected solve-gain action"),
        }
    }

    #[test]
    fn parse_args_accepts_solve_bandpass_command() {
        let action = parse_args([
            "solve-bandpass".into(),
            "--ms".into(),
            "dataset.ms".into(),
            "--out".into(),
            "bandpass.bcal".into(),
            "--refant".into(),
            "7".into(),
            "--combine".into(),
            "scan,field".into(),
            "--bandtype".into(),
            "bpoly".into(),
            "--degamp".into(),
            "5".into(),
            "--degphase".into(),
            "4".into(),
            "--solnorm".into(),
            "--gaintables".into(),
            "prior.gcal".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::SolveBandpass(options) = request.command else {
                    panic!("expected solve-bandpass action");
                };
                assert_eq!(options.measurement_set, PathBuf::from("dataset.ms"));
                assert_eq!(options.output_table, PathBuf::from("bandpass.bcal"));
                assert_eq!(options.refant, RefAntSelector::AntennaId(7));
                assert_eq!(
                    options.combine,
                    BandpassSolveCombine {
                        scans: true,
                        fields: true,
                    }
                );
                assert_eq!(options.band_type, BandpassType::BPoly);
                assert!(options.normalize_average_amplitude);
                assert_eq!(options.amplitude_degree, 5);
                assert_eq!(options.phase_degree, 4);
                assert_eq!(options.prior_calibration_tables.len(), 1);
            }
            _ => panic!("expected solve-bandpass action"),
        }
    }

    #[test]
    fn parse_args_accepts_solve_bandpass_workflow_mode_schema_form() {
        let action = parse_args([
            "--mode".into(),
            "solve_bandpass".into(),
            "--ms".into(),
            "dataset.ms".into(),
            "--out".into(),
            "bandpass.bcal".into(),
            "--refant".into(),
            "7".into(),
            "--combine-bandpass".into(),
            "field".into(),
            "--bandtype".into(),
            "b".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::SolveBandpass(options) = request.command else {
                    panic!("expected solve-bandpass action");
                };
                assert_eq!(
                    options.combine,
                    BandpassSolveCombine {
                        scans: false,
                        fields: true,
                    }
                );
                assert_eq!(options.band_type, BandpassType::B);
            }
            _ => panic!("expected solve-bandpass action"),
        }
    }

    #[test]
    fn parse_args_accepts_fluxscale_command() {
        let action = parse_args([
            "fluxscale".into(),
            "--in".into(),
            "gain.gcal".into(),
            "--out".into(),
            "flux.gcal".into(),
            "--reference".into(),
            "1331+305".into(),
            "--transfer".into(),
            "1445+099".into(),
            "--refspwmap".into(),
            "0,0".into(),
            "--gainthreshold".into(),
            "0.2".into(),
            "--incremental".into(),
            "--format".into(),
            "json".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::FluxScale(options) = request.command else {
                    panic!("expected fluxscale action");
                };
                assert_eq!(options.input_table, PathBuf::from("gain.gcal"));
                assert_eq!(options.output_table, PathBuf::from("flux.gcal"));
                assert_eq!(options.reference_fields, vec!["1331+305".to_string()]);
                assert_eq!(options.transfer_fields, vec!["1445+099".to_string()]);
                assert_eq!(options.refspwmap, vec![0, 0]);
                assert_eq!(options.gainthreshold, Some(0.2));
                assert!(options.incremental);
                assert_eq!(options.format, OutputFormat::Json);
            }
            _ => panic!("expected fluxscale action"),
        }
    }

    #[test]
    fn parse_args_accepts_fluxscale_workflow_mode_schema_form() {
        let action = parse_args([
            "--mode".into(),
            "fluxscale".into(),
            "--in".into(),
            "gain.gcal".into(),
            "--out".into(),
            "flux.gcal".into(),
            "--reference".into(),
            "1331+305".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                let Command::FluxScale(options) = request.command else {
                    panic!("expected fluxscale action");
                };
                assert_eq!(options.input_table, PathBuf::from("gain.gcal"));
                assert_eq!(options.output_table, PathBuf::from("flux.gcal"));
                assert_eq!(options.reference_fields, vec!["1331+305".to_string()]);
            }
            _ => panic!("expected fluxscale action"),
        }
    }

    #[test]
    fn parse_args_accepts_managed_output_flag() {
        let action = parse_args([
            "--managed-output".into(),
            "true".into(),
            "--mode".into(),
            "summary".into(),
            "--summary-paths".into(),
            "example.gcal".into(),
        ])
        .expect("parse succeeds");
        match action {
            CliAction::Run(request) => {
                assert!(request.managed_output);
                let Command::Summarize(options) = request.command else {
                    panic!("expected summary action");
                };
                assert_eq!(options.paths, vec![PathBuf::from("example.gcal")]);
            }
            _ => panic!("expected summary action"),
        }
    }
}
