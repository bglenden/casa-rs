// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` - native CASA-style flag-version management.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_ms::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind,
};
use casa_ms::{
    FlagMerge, MeasurementSet, delete_flag_version, list_flag_versions, rename_flag_version,
    restore_flag_version, save_flag_version,
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
        println!("{}", command_schema("flagmanager").render_help());
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--ui-schema") {
        println!(
            "{}",
            command_schema("flagmanager")
                .render_json_pretty()
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--json-schema") {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema_bundle("flagmanager"))
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let request = parse_args(&args)?;
    let mut ms = MeasurementSet::open(&request.vis).map_err(|error| error.to_string())?;
    let value = match request.mode.as_str() {
        "list" => serde_json::to_value(list_flag_versions(&ms).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?,
        "save" => {
            save_flag_version(
                &ms,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.comment.as_deref().unwrap_or(""),
                request.merge,
            )
            .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"save","versionname":request.versionname})
        }
        "restore" => {
            restore_flag_version(
                &mut ms,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.merge,
            )
            .map_err(|error| error.to_string())?;
            ms.save_main_table_only_assuming_valid()
                .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"restore","versionname":request.versionname})
        }
        "delete" => {
            delete_flag_version(&ms, request.versionname.as_deref().ok_or_else(usage)?)
                .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"delete","versionname":request.versionname})
        }
        "rename" => {
            rename_flag_version(
                &ms,
                request.oldname.as_deref().ok_or_else(usage)?,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.comment.as_deref().unwrap_or(""),
            )
            .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"rename","oldname":request.oldname,"versionname":request.versionname})
        }
        other => return Err(format!("unsupported mode {other:?}\n{}", usage())),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn parse_args(args: &[String]) -> Result<Request, String> {
    let mut request = Request {
        vis: PathBuf::new(),
        mode: "list".to_string(),
        versionname: None,
        oldname: None,
        comment: None,
        merge: FlagMerge::Replace,
    };
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--vis" | "--ms" => {
                index += 1;
                request.vis = PathBuf::from(args.get(index).ok_or_else(usage)?);
            }
            "--mode" => {
                index += 1;
                request.mode = args.get(index).ok_or_else(usage)?.to_ascii_lowercase();
            }
            "--versionname" => {
                index += 1;
                request.versionname = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--oldname" => {
                index += 1;
                request.oldname = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--comment" => {
                index += 1;
                request.comment = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--merge" => {
                index += 1;
                request.merge = parse_merge(args.get(index).ok_or_else(usage)?)?;
            }
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        index += 1;
    }
    if request.vis.as_os_str().is_empty() {
        return Err(usage());
    }
    Ok(request)
}

fn parse_merge(value: &str) -> Result<FlagMerge, String> {
    match value.to_ascii_lowercase().as_str() {
        "replace" => Ok(FlagMerge::Replace),
        "or" => Ok(FlagMerge::Or),
        "and" => Ok(FlagMerge::And),
        other => Err(format!("unsupported merge {other:?}")),
    }
}

fn usage() -> String {
    "usage: flagmanager --vis <ms> --mode list|save|restore|delete|rename [--versionname <name>] [--oldname <name>] [--comment <text>] [--merge replace|or|and]".to_string()
}

struct Request {
    vis: PathBuf,
    mode: String,
    versionname: Option<String>,
    oldname: Option<String>,
    comment: Option<String>,
    merge: FlagMerge,
}

fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: 1,
        command_id: "flagmanager".to_string(),
        invocation_name: program_name.to_string(),
        display_name: "Flag Manager".to_string(),
        category: "Flagging".to_string(),
        summary: "Manage MeasurementSet flag-version snapshots.".to_string(),
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
                choices: &["list", "save", "restore", "delete", "rename"],
                default: Some("list"),
                required: false,
                help: "Flag-version operation.",
                group: "Operation",
            }),
            option_argument(OptionConfig {
                id: "versionname",
                label: "Version Name",
                order: 2,
                flags: &["--versionname"],
                metavar: "NAME",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Target flag-version name.",
                group: "Operation",
            }),
            option_argument(OptionConfig {
                id: "oldname",
                label: "Old Name",
                order: 3,
                flags: &["--oldname"],
                metavar: "NAME",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Existing flag-version name for rename.",
                group: "Operation",
            }),
            option_argument(OptionConfig {
                id: "comment",
                label: "Comment",
                order: 4,
                flags: &["--comment"],
                metavar: "TEXT",
                value_kind: UiValueKind::String,
                choices: &[],
                default: None,
                required: false,
                help: "Comment stored with saved or renamed versions.",
                group: "Operation",
            }),
            option_argument(OptionConfig {
                id: "merge",
                label: "Merge",
                order: 5,
                flags: &["--merge"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                choices: &["replace", "or", "and"],
                default: Some("replace"),
                required: false,
                help: "Merge policy for save and restore operations.",
                group: "Operation",
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
            "protocol_name": "casa_ms_flagmanager_task",
            "protocol_version": 1,
            "surface_kind": "task"
        },
        "projections": {
            "ui_schema": command_schema(program_name)
        },
        "request_schema": {
            "type": "object",
            "required": ["vis"],
            "properties": {
                "vis": {"type": "string"},
                "mode": {"type": "string", "enum": ["list", "save", "restore", "delete", "rename"]},
                "versionname": {"type": "string"},
                "oldname": {"type": "string"},
                "comment": {"type": "string"},
                "merge": {"type": "string", "enum": ["replace", "or", "and"]}
            }
        },
        "result_schema": {
            "oneOf": [
                {"type": "array", "items": schema_for!(casa_ms::FlagVersionEntry)},
                {"type": "object"}
            ]
        }
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
