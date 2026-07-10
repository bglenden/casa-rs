// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` - native CASA-style flag-version management.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process;

use casa_ms::ui_schema::UiCommandSchema;
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
    let (logging_guard, args) =
        casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1))
            .map_err(|error| format!("failed to initialize logging: {error}"))?;
    let args = os_args_to_strings(args)?;
    tracing::info!("flagmanager started");
    let result = run_with_args(args);
    if result.is_ok() {
        tracing::info!("flagmanager completed");
    } else if let Err(error) = &result {
        tracing::error!(casa.priority = "SEVERE", error = %error, "flagmanager failed");
    }
    logging_guard
        .flush()
        .map_err(|error| format!("failed to flush logging: {error}"))?;
    result
}

fn run_with_args(args: Vec<String>) -> Result<(), String> {
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

fn os_args_to_strings(args: Vec<OsString>) -> Result<Vec<String>, String> {
    args.into_iter()
        .map(|arg| {
            arg.into_string()
                .map_err(|_| "non-UTF-8 command-line argument".to_string())
        })
        .collect()
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
    let bundle = casa_provider_contracts::builtin_surface_bundle("flagmanager")
        .expect("built-in flagmanager parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_schema(&bundle))
            .expect("canonical flagmanager UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}
fn schema_bundle(program_name: &str) -> serde_json::Value {
    let parameter_surfaces = vec![
        casa_provider_contracts::builtin_surface_bundle("flagmanager")
            .expect("built-in flagmanager parameter surface must remain valid"),
    ];
    json!({
        "protocol": {
            "protocol_name": "casa_ms_flagmanager_task",
            "protocol_version": 1,
            "surface_kind": "task"
        },
        "projections": {
            "ui_schema": command_schema(program_name)
        },
        "parameter_surfaces": parameter_surfaces,
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

#[cfg(test)]
mod tests {
    use casa_provider_contracts::SurfaceContractBundle;

    use super::*;

    #[test]
    fn schema_bundle_embeds_flagmanager_parameter_contract() {
        let bundle = schema_bundle("flagmanager");
        assert_eq!(
            bundle["protocol"]["protocol_name"],
            "casa_ms_flagmanager_task"
        );
        assert!(bundle["request_schema"]["properties"]["mode"].is_object());
        assert!(bundle["result_schema"].is_object());

        let surfaces = serde_json::from_value::<Vec<SurfaceContractBundle>>(
            bundle["parameter_surfaces"].clone(),
        )
        .expect("serialized flagmanager parameter surface");
        assert_eq!(surfaces.len(), 1);
        assert_eq!(surfaces[0].surface.id(), "flagmanager");
        surfaces[0]
            .validate()
            .expect("embedded flagmanager parameter surface");
    }
}
