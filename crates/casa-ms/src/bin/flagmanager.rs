// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` - native CASA-style flag-version management.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process;

use casa_ms::presentation::UiCommandSchema;
use casa_ms::{
    FlagManagerMutationResult, FlagManagerTaskRequest, FlagManagerTaskResult, FlagMerge,
    MeasurementSet, delete_flag_version, flagmanager_task_schema_bundle, list_flag_versions,
    rename_flag_version, restore_flag_version, save_flag_version,
};

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

fn run_with_args(args: Vec<OsString>) -> Result<(), String> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "{}\n\n{}",
            command_schema("flagmanager").render_help(),
            casa_task_runtime::task_cli_machine_help("FlagManagerTaskRequest")
        );
        return Ok(());
    }
    let host =
        casa_task_runtime::TaskCliHost::new(flagmanager_task_schema_bundle(), execute_task_request);
    if let Some(output) = host.dispatch(&args).map_err(|error| error.to_string())? {
        println!("{output}");
        return Ok(());
    }
    let args = os_args_to_strings(args)?;
    let request = parse_args(&args)?;
    let value = execute_task_request(request)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn execute_task_request(request: FlagManagerTaskRequest) -> Result<FlagManagerTaskResult, String> {
    let mut ms = MeasurementSet::open(&request.vis).map_err(|error| error.to_string())?;
    let value = match request.mode.as_str() {
        "list" => FlagManagerTaskResult::Versions(
            list_flag_versions(&ms).map_err(|error| error.to_string())?,
        ),
        "save" => {
            save_flag_version(
                &ms,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.comment.as_deref().unwrap_or(""),
                request.merge,
            )
            .map_err(|error| error.to_string())?;
            mutation("save", request.versionname, None)
        }
        "restore" => {
            restore_flag_version(
                &mut ms,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.merge,
            )
            .map_err(|error| error.to_string())?;
            ms.save_main_table_only()
                .map_err(|error| error.to_string())?;
            mutation("restore", request.versionname, None)
        }
        "delete" => {
            delete_flag_version(&ms, request.versionname.as_deref().ok_or_else(usage)?)
                .map_err(|error| error.to_string())?;
            mutation("delete", request.versionname, None)
        }
        "rename" => {
            rename_flag_version(
                &ms,
                request.oldname.as_deref().ok_or_else(usage)?,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.comment.as_deref().unwrap_or(""),
            )
            .map_err(|error| error.to_string())?;
            mutation("rename", request.versionname, request.oldname)
        }
        other => return Err(format!("unsupported mode {other:?}\n{}", usage())),
    };
    Ok(value)
}

fn mutation(
    mode: &str,
    versionname: Option<String>,
    oldname: Option<String>,
) -> FlagManagerTaskResult {
    FlagManagerTaskResult::Mutation(FlagManagerMutationResult {
        mode: mode.to_string(),
        versionname,
        oldname,
    })
}

fn os_args_to_strings(args: Vec<OsString>) -> Result<Vec<String>, String> {
    args.into_iter()
        .map(|arg| {
            arg.into_string()
                .map_err(|_| "non-UTF-8 command-line argument".to_string())
        })
        .collect()
}

fn parse_args(args: &[String]) -> Result<FlagManagerTaskRequest, String> {
    let mut request = FlagManagerTaskRequest {
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

fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("flagmanager")
        .expect("built-in flagmanager parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical flagmanager UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_bundle_embeds_flagmanager_parameter_contract() {
        let bundle = flagmanager_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, "casa_ms_flagmanager_task");
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "flagmanager");
        bundle.validate().expect("valid flagmanager provider");
    }
}
