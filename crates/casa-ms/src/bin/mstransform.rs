// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_ms::selection::MsSelection;
use casa_ms::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind,
};
use casa_ms::{MsTransformRequest, TransformDataColumn, mstransform, parse_numeric_id_selector};
use schemars::schema_for;
use serde_json::json;

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("Error: {error}");
            process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", command_schema("mstransform").render_help());
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--ui-schema") {
        println!(
            "{}",
            command_schema("mstransform")
                .render_json_pretty()
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--json-schema") {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema_bundle("mstransform"))
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let request = parse_request(&args)?;
    let report = mstransform(&request).map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn parse_request(args: &[String]) -> Result<MsTransformRequest, String> {
    let mut input_ms = None;
    let mut output_ms = None;
    let mut spw = String::new();
    let mut width = 1usize;
    let mut data_column = TransformDataColumn::default();
    let mut selection = MsSelection::default();
    let mut keep_flags = true;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--ms" | "--vis" => {
                index += 1;
                input_ms = Some(PathBuf::from(args.get(index).ok_or_else(usage)?));
            }
            "--out" | "--outputvis" => {
                index += 1;
                output_ms = Some(PathBuf::from(args.get(index).ok_or_else(usage)?));
            }
            "--spw" => {
                index += 1;
                spw = args.get(index).ok_or_else(usage)?.clone();
            }
            "--width" => {
                index += 1;
                let value = args.get(index).ok_or_else(usage)?;
                width = parse_width(value)?;
            }
            "--datacolumn" => {
                index += 1;
                data_column = parse_data_column(args.get(index).ok_or_else(usage)?)?;
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
                selection = selection.antenna(
                    &parse_numeric_id_selector(args.get(index).ok_or_else(usage)?, "antenna")
                        .map_err(|error| error.to_string())?,
                );
            }
            "--timerange" => {
                index += 1;
                let value = args.get(index).ok_or_else(usage)?;
                let (start, end) = parse_time_range(value)?;
                selection = selection.time_range(start, end);
            }
            "--msselect" => {
                index += 1;
                selection = selection.taql(args.get(index).ok_or_else(usage)?);
            }
            "--keepflags" => keep_flags = true,
            "--no-keepflags" => keep_flags = false,
            "--selectdata" => {}
            "--no-selectdata" => {}
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        index += 1;
    }
    Ok(MsTransformRequest {
        input_ms: input_ms.ok_or_else(usage)?,
        output_ms: output_ms.ok_or_else(usage)?,
        spw,
        width,
        data_column,
        selection,
        keep_flags,
    })
}

fn parse_width(value: &str) -> Result<usize, String> {
    let first = value
        .split(',')
        .next()
        .unwrap_or(value)
        .trim()
        .trim_matches('"')
        .trim_matches('\'');
    let parsed = first
        .parse::<usize>()
        .map_err(|error| format!("invalid --width {value:?}: {error}"))?;
    if parsed == 0 {
        return Err("--width must be at least 1".to_string());
    }
    Ok(parsed)
}

fn parse_data_column(value: &str) -> Result<TransformDataColumn, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "DATA" => Ok(TransformDataColumn::Data),
        "CORRECTED" | "CORRECTED_DATA" => Ok(TransformDataColumn::CorrectedData),
        other => Err(format!(
            "unsupported --datacolumn {other:?}; expected DATA or CORRECTED_DATA"
        )),
    }
}

fn parse_time_range(value: &str) -> Result<(f64, f64), String> {
    let (start, end) = value
        .split_once('~')
        .ok_or_else(|| format!("--timerange must be start~end MJD seconds, got {value:?}"))?;
    let start = start
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid timerange start {start:?}: {error}"))?;
    let end = end
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid timerange end {end:?}: {error}"))?;
    Ok((start, end))
}

fn usage() -> String {
    "usage: mstransform --ms <input.ms> --out <output.ms> [--spw <spw[:channels]>] [--field <ids>] [--width <n>] [--datacolumn DATA|CORRECTED_DATA] [--keepflags|--no-keepflags]".to_string()
}

fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: 1,
        command_id: "mstransform".to_string(),
        invocation_name: program_name.to_string(),
        display_name: "MSTransform".to_string(),
        category: "MeasurementSet".to_string(),
        summary: "Materialize a selected MeasurementSet into a new output MS.".to_string(),
        usage: usage(),
        arguments: vec![
            option_argument(OptionConfig {
                id: "ms",
                label: "Input MS",
                order: 0,
                flags: &["--ms", "--vis"],
                metavar: "MS",
                value_kind: UiValueKind::Path,
                choices: &[],
                default: None,
                required: true,
                help: "Input MeasurementSet path.",
                group: "Input",
            }),
            option_argument(OptionConfig {
                id: "out",
                label: "Output MS",
                order: 1,
                flags: &["--out", "--outputvis"],
                metavar: "MS",
                value_kind: UiValueKind::Path,
                choices: &[],
                default: None,
                required: true,
                help: "Output MeasurementSet path to create.",
                group: "Output",
            }),
            option_argument(OptionConfig {
                id: "spw",
                label: "Spectral Window",
                order: 2,
                flags: &["--spw"],
                metavar: "SPW[:CHANNELS]",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "CASA-style spectral-window and channel selector.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "width",
                label: "Channel Width",
                order: 3,
                flags: &["--width"],
                metavar: "N",
                value_kind: UiValueKind::String,
                choices: &[],
                default: Some("1"),
                required: false,
                help: "Average this many adjacent selected channels into each output channel, matching CASA split width.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "field",
                label: "Field",
                order: 4,
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
                order: 5,
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
                order: 6,
                flags: &["--antenna"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Comma-separated numeric antenna ids.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "timerange",
                label: "Time Range",
                order: 7,
                flags: &["--timerange"],
                metavar: "START~END",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "MJD-second time range.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "msselect",
                label: "MS Select",
                order: 8,
                flags: &["--msselect"],
                metavar: "TAQL",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "TAQL row-selection expression.",
                group: "Selection",
            }),
            option_argument(OptionConfig {
                id: "datacolumn",
                label: "Data Column",
                order: 9,
                flags: &["--datacolumn"],
                metavar: "COLUMN",
                value_kind: UiValueKind::Choice,
                choices: &["DATA", "CORRECTED_DATA"],
                default: Some("DATA"),
                required: false,
                help: "Input visibility column copied to output DATA.",
                group: "Data",
            }),
            toggle_argument(ToggleConfig {
                id: "keepflags",
                label: "Keep Fully Flagged Rows",
                order: 10,
                true_flags: &["--keepflags"],
                false_flags: &["--no-keepflags"],
                default: Some("true"),
                help: "Preserve rows that are fully flagged in the selected output.",
                group: "Data",
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
            "protocol_name": "casa_ms_transform_task",
            "protocol_version": 1,
            "surface_kind": "task"
        },
        "projections": {
            "ui_schema": command_schema(program_name)
        },
        "request_schema": {
            "type": "object",
            "required": ["input_ms", "output_ms"],
            "properties": {
                "input_ms": {"type": "string"},
                "output_ms": {"type": "string"},
                "spw": {"type": "string"},
                "width": {"type": "integer", "minimum": 1},
                "data_column": {"type": "string", "enum": ["DATA", "CORRECTED_DATA"]},
                "keep_flags": {"type": "boolean"}
            }
        },
        "result_schema": schema_for!(casa_ms::MsTransformReport)
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
