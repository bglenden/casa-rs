// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagdata` - native CASA-style MeasurementSet flagging.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_ms::selection::MsSelection;
use casa_ms::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind,
};
use casa_ms::{
    FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataRequest, QuackMode, flagdata_path,
    parse_numeric_id_selector,
};
use schemars::schema_for;
use serde_json::json;

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", command_schema("flagdata").render_help());
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--ui-schema") {
        println!(
            "{}",
            command_schema("flagdata")
                .render_json_pretty()
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--json-schema") {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema_bundle("flagdata"))
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let (vis, request) = parse_args(&args)?;
    let report = flagdata_path(vis, &request).map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn parse_args(args: &[String]) -> Result<(PathBuf, FlagDataRequest), String> {
    let mut vis = None;
    let mut request = FlagDataRequest::default();
    let mut selection = MsSelection::new();
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--vis" | "--ms" => {
                index += 1;
                vis = Some(PathBuf::from(args.get(index).ok_or_else(usage)?));
            }
            "--mode" => {
                index += 1;
                request.mode = parse_mode(args.get(index).ok_or_else(usage)?)?;
            }
            "--spw" => {
                index += 1;
                let spw = args.get(index).ok_or_else(usage)?.clone();
                selection = selection
                    .spw_selector(&spw)
                    .map_err(|error| error.to_string())?;
                request.spw = Some(spw);
            }
            "--field" => {
                index += 1;
                selection = selection.field(
                    &parse_numeric_id_selector(args.get(index).ok_or_else(usage)?, "field")
                        .map_err(|error| error.to_string())?,
                );
            }
            "--scan" => {
                index += 1;
                selection = selection.scan(
                    &parse_numeric_id_selector(args.get(index).ok_or_else(usage)?, "scan")
                        .map_err(|error| error.to_string())?,
                );
            }
            "--antenna" => {
                index += 1;
                let value = args.get(index).ok_or_else(usage)?;
                selection = apply_antenna_selection(selection, value)?;
            }
            "--datacolumn" => {
                index += 1;
                request.data_column = parse_data_column(args.get(index).ok_or_else(usage)?)?;
            }
            "--action" => {
                index += 1;
                request.action = parse_action(args.get(index).ok_or_else(usage)?)?;
            }
            "--clipzeros" => request.clipzeros = true,
            "--quackinterval" => {
                index += 1;
                request.quackinterval =
                    parse_f64(args.get(index).ok_or_else(usage)?, "quackinterval")?;
            }
            "--quackmode" => {
                index += 1;
                request.quackmode = parse_quackmode(args.get(index).ok_or_else(usage)?)?;
            }
            "--timecutoff" => {
                index += 1;
                request.timecutoff = parse_f64(args.get(index).ok_or_else(usage)?, "timecutoff")?;
            }
            "--freqcutoff" => {
                index += 1;
                request.freqcutoff = parse_f64(args.get(index).ok_or_else(usage)?, "freqcutoff")?;
            }
            "--timedev" => {
                index += 1;
                request.timedev = Some(parse_f64(args.get(index).ok_or_else(usage)?, "timedev")?);
            }
            "--freqdev" => {
                index += 1;
                request.freqdev = Some(parse_f64(args.get(index).ok_or_else(usage)?, "freqdev")?);
            }
            "--timedevscale" => {
                index += 1;
                request.timedevscale =
                    parse_f64(args.get(index).ok_or_else(usage)?, "timedevscale")?;
            }
            "--freqdevscale" => {
                index += 1;
                request.freqdevscale =
                    parse_f64(args.get(index).ok_or_else(usage)?, "freqdevscale")?;
            }
            "--spectralmax" => {
                index += 1;
                request.spectralmax = parse_f64(args.get(index).ok_or_else(usage)?, "spectralmax")?;
            }
            "--spectralmin" => {
                index += 1;
                request.spectralmin = parse_f64(args.get(index).ok_or_else(usage)?, "spectralmin")?;
            }
            "--extendflags" => request.extendflags = true,
            "--no-extendflags" | "--extendflags=false" => request.extendflags = false,
            "--extendpols" => request.extendpols = true,
            "--growtime" => {
                index += 1;
                request.growtime = parse_f64(args.get(index).ok_or_else(usage)?, "growtime")?;
            }
            "--growfreq" => {
                index += 1;
                request.growfreq = parse_f64(args.get(index).ok_or_else(usage)?, "growfreq")?;
            }
            "--flagbackup" => request.flagbackup = true,
            "--no-flagbackup" | "--flagbackup=false" => request.flagbackup = false,
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        index += 1;
    }
    request.selection = selection;
    Ok((vis.ok_or_else(usage)?, request))
}

fn apply_antenna_selection(selection: MsSelection, value: &str) -> Result<MsSelection, String> {
    let parts = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.iter().all(|part| part.parse::<i32>().is_ok()) {
        Ok(selection.antenna(
            &parts
                .iter()
                .map(|part| part.parse::<i32>().expect("checked int"))
                .collect::<Vec<_>>(),
        ))
    } else {
        Ok(selection.antenna_name(&parts))
    }
}

fn parse_mode(value: &str) -> Result<FlagDataMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "manual" => Ok(FlagDataMode::Manual),
        "clip" => Ok(FlagDataMode::Clip),
        "quack" => Ok(FlagDataMode::Quack),
        "tfcrop" => Ok(FlagDataMode::Tfcrop),
        "rflag" => Ok(FlagDataMode::Rflag),
        "extend" => Ok(FlagDataMode::Extend),
        "summary" => Ok(FlagDataMode::Summary),
        other => Err(format!("unsupported mode {other:?}")),
    }
}

fn parse_action(value: &str) -> Result<FlagDataAction, String> {
    match value.to_ascii_lowercase().as_str() {
        "flag" | "apply" => Ok(FlagDataAction::Flag),
        "unflag" => Ok(FlagDataAction::Unflag),
        other => Err(format!("unsupported action {other:?}")),
    }
}

fn parse_data_column(value: &str) -> Result<FlagDataColumn, String> {
    match value.to_ascii_lowercase().as_str() {
        "data" => Ok(FlagDataColumn::Data),
        "corrected" | "corrected_data" => Ok(FlagDataColumn::CorrectedData),
        other => Err(format!("unsupported datacolumn {other:?}")),
    }
}

fn parse_quackmode(value: &str) -> Result<QuackMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "beg" => Ok(QuackMode::Beg),
        "end" => Ok(QuackMode::End),
        other => Err(format!("unsupported quackmode {other:?}")),
    }
}

fn parse_f64(value: &str, label: &str) -> Result<f64, String> {
    value
        .parse::<f64>()
        .map_err(|error| format!("invalid {label} {value:?}: {error}"))
}

fn usage() -> String {
    "usage: flagdata --vis <ms> --mode manual|clip|quack|tfcrop|rflag|extend|summary [--spw <selector>] [--field <ids>] [--scan <ids>] [--antenna <ids-or-names>] [--datacolumn data|corrected] [--no-flagbackup]".to_string()
}

fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: 1,
        command_id: "flagdata".to_string(),
        invocation_name: program_name.to_string(),
        display_name: "Flag Data".to_string(),
        category: "Flagging".to_string(),
        summary: "Run native CASA-style MeasurementSet flagging.".to_string(),
        usage: usage(),
        arguments: vec![
            option_argument(OptionConfig {
                id: "vis",
                label: "MeasurementSet",
                order: 0,
                flags: &["--vis", "--ms"],
                metavar: "MS",
                value_kind: UiValueKind::Path,
                choices: &[],
                default: None,
                required: true,
                help: "Input MeasurementSet path.",
                group: "Input",
            }),
            option_argument(OptionConfig {
                id: "mode",
                label: "Mode",
                order: 1,
                flags: &["--mode"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                choices: &[
                    "manual", "clip", "quack", "tfcrop", "rflag", "extend", "summary",
                ],
                default: Some("summary"),
                required: true,
                help: "Flagging mode.",
                group: "Flagging",
            }),
            option_argument(OptionConfig {
                id: "spw",
                label: "Spectral Window",
                order: 2,
                flags: &["--spw"],
                metavar: "SELECTOR",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "CASA-style spectral-window selector.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "field",
                label: "Field",
                order: 3,
                flags: &["--field"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Comma-separated numeric field ids.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "scan",
                label: "Scan",
                order: 4,
                flags: &["--scan"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Comma-separated numeric scan ids.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "antenna",
                label: "Antenna",
                order: 5,
                flags: &["--antenna"],
                metavar: "IDS_OR_NAMES",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Comma-separated antenna ids or names.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "datacolumn",
                label: "Data Column",
                order: 6,
                flags: &["--datacolumn"],
                metavar: "COLUMN",
                value_kind: UiValueKind::Choice,
                choices: &["data", "corrected"],
                default: Some("data"),
                required: false,
                help: "Visibility data column for automatic flagging modes.",
                group: "Data",
            }),
            option_argument(OptionConfig {
                id: "action",
                label: "Action",
                order: 7,
                flags: &["--action"],
                metavar: "ACTION",
                value_kind: UiValueKind::Choice,
                choices: &["flag", "unflag"],
                default: Some("flag"),
                required: false,
                help: "Manual mode flagging action.",
                group: "Flagging",
            }),
            toggle_argument(ToggleConfig {
                id: "flagbackup",
                label: "Create Flag Backup",
                order: 8,
                true_flags: &["--flagbackup"],
                false_flags: &["--no-flagbackup"],
                default: Some("true"),
                help: "Create a flagmanager backup before mutating flags.",
                group: "Safety",
            }),
            toggle_argument(ToggleConfig {
                id: "clipzeros",
                label: "Clip Zeros",
                order: 9,
                true_flags: &["--clipzeros"],
                false_flags: &[],
                default: Some("false"),
                help: "Flag exact zero amplitudes in clip mode.",
                group: "Flagging",
            }),
            option_argument(OptionConfig {
                id: "quackinterval",
                label: "Quack Interval",
                order: 10,
                flags: &["--quackinterval"],
                metavar: "SECONDS",
                value_kind: UiValueKind::Float,
                choices: &[],
                default: Some("0"),
                required: false,
                help: "Quack interval in seconds.",
                group: "Quack",
            }),
            option_argument(OptionConfig {
                id: "quackmode",
                label: "Quack Mode",
                order: 11,
                flags: &["--quackmode"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                choices: &["beg", "end"],
                default: Some("beg"),
                required: false,
                help: "Quack scan edge.",
                group: "Quack",
            }),
            option_argument(OptionConfig {
                id: "timecutoff",
                label: "Time Cutoff",
                order: 12,
                flags: &["--timecutoff"],
                metavar: "SIGMA",
                value_kind: UiValueKind::Float,
                choices: &[],
                default: Some("4.0"),
                required: false,
                help: "TFCrop time cutoff in robust sigma units.",
                group: "Automatic",
            }),
            option_argument(OptionConfig {
                id: "freqcutoff",
                label: "Frequency Cutoff",
                order: 13,
                flags: &["--freqcutoff"],
                metavar: "SIGMA",
                value_kind: UiValueKind::Float,
                choices: &[],
                default: Some("3.0"),
                required: false,
                help: "TFCrop frequency cutoff in robust sigma units.",
                group: "Automatic",
            }),
            option_argument(OptionConfig {
                id: "timedev",
                label: "Time Deviation",
                order: 14,
                flags: &["--timedev"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                choices: &[],
                default: None,
                required: false,
                help: "Explicit RFlag time threshold.",
                group: "Automatic",
            }),
            option_argument(OptionConfig {
                id: "freqdev",
                label: "Frequency Deviation",
                order: 15,
                flags: &["--freqdev"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                choices: &[],
                default: None,
                required: false,
                help: "Explicit RFlag spectral threshold.",
                group: "Automatic",
            }),
            toggle_argument(ToggleConfig {
                id: "extendflags",
                label: "Extend Flags",
                order: 16,
                true_flags: &["--extendflags"],
                false_flags: &["--no-extendflags", "--extendflags=false"],
                default: Some("false"),
                help: "Run post-extension after automatic flagging.",
                group: "Extend",
            }),
            toggle_argument(ToggleConfig {
                id: "extendpols",
                label: "Extend Polarizations",
                order: 17,
                true_flags: &["--extendpols"],
                false_flags: &[],
                default: Some("false"),
                help: "Extend flags across correlations.",
                group: "Extend",
            }),
            action_argument("help", "Help", 100, &["-h", "--help"], UiActionKind::Help),
            action_argument(
                "ui_schema",
                "UI Schema",
                101,
                &["--ui-schema"],
                UiActionKind::UiSchema,
            ),
        ],
        managed_output: None,
    }
}

fn schema_bundle(program_name: &str) -> serde_json::Value {
    json!({
        "protocol": {
            "protocol_name": "casa_ms_flagdata_task",
            "protocol_version": 1,
            "surface_kind": "task"
        },
        "projections": {
            "ui_schema": command_schema(program_name)
        },
        "request_schema": {
            "type": "object",
            "required": ["vis", "mode"],
            "properties": {
                "vis": {"type": "string"},
                "mode": {"type": "string", "enum": ["manual", "clip", "quack", "tfcrop", "rflag", "extend", "summary"]},
                "spw": {"type": "string"},
                "field": {"type": "string"},
                "scan": {"type": "string"},
                "antenna": {"type": "string"},
                "datacolumn": {"type": "string", "enum": ["data", "corrected"]},
                "flagbackup": {"type": "boolean"}
            }
        },
        "result_schema": schema_for!(casa_ms::FlagDataReport)
    })
}

struct OptionConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    flags: &'a [&'a str],
    metavar: &'a str,
    value_kind: UiValueKind,
    choices: &'a [&'a str],
    default: Option<&'a str>,
    required: bool,
    help: &'a str,
    group: &'a str,
}

fn option_argument(config: OptionConfig<'_>) -> UiArgumentSchema {
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
        default: config.default.map(str::to_string),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: false,
        hidden_in_tui: false,
    }
}

struct ToggleConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    true_flags: &'a [&'a str],
    false_flags: &'a [&'a str],
    default: Option<&'a str>,
    help: &'a str,
    group: &'a str,
}

fn toggle_argument(config: ToggleConfig<'_>) -> UiArgumentSchema {
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
        default: config.default.map(str::to_string),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: false,
        hidden_in_tui: false,
    }
}

fn action_argument(
    id: &str,
    label: &str,
    order: usize,
    flags: &[&str],
    action: UiActionKind,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: label.to_string(),
        group: "Machine".to_string(),
        advanced: true,
        hidden_in_tui: true,
    }
}
