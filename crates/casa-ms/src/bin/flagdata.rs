// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagdata` - native CASA-style MeasurementSet flagging.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process;

use casa_ms::selection::MsSelection;
use casa_ms::ui_schema::UiCommandSchema;
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
    let (logging_guard, args) =
        casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1))
            .map_err(|error| format!("failed to initialize logging: {error}"))?;
    let args = os_args_to_strings(args)?;
    tracing::info!("flagdata started");
    let result = run_with_args(args);
    if result.is_ok() {
        tracing::info!("flagdata completed");
    } else if let Err(error) = &result {
        tracing::error!(casa.priority = "SEVERE", error = %error, "flagdata failed");
    }
    logging_guard
        .flush()
        .map_err(|error| format!("failed to flush logging: {error}"))?;
    result
}

fn run_with_args(args: Vec<String>) -> Result<(), String> {
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

fn os_args_to_strings(args: Vec<OsString>) -> Result<Vec<String>, String> {
    args.into_iter()
        .map(|arg| {
            arg.into_string()
                .map_err(|_| "non-UTF-8 command-line argument".to_string())
        })
        .collect()
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
    let bundle = casa_provider_contracts::builtin_surface_bundle("flagdata")
        .expect("built-in flagdata parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_schema(&bundle))
            .expect("canonical flagdata UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}
fn schema_bundle(program_name: &str) -> serde_json::Value {
    let parameter_surfaces = vec![
        casa_provider_contracts::builtin_surface_bundle("flagdata")
            .expect("built-in flagdata parameter surface must remain valid"),
    ];
    json!({
        "protocol": {
            "protocol_name": "casa_ms_flagdata_task",
            "protocol_version": 1,
            "surface_kind": "task"
        },
        "projections": {
            "ui_schema": command_schema(program_name)
        },
        "parameter_surfaces": parameter_surfaces,
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

#[cfg(test)]
mod tests {
    use casa_provider_contracts::SurfaceContractBundle;

    use super::*;

    #[test]
    fn schema_bundle_embeds_flagdata_parameter_contract() {
        let bundle = schema_bundle("flagdata");
        assert_eq!(bundle["protocol"]["protocol_name"], "casa_ms_flagdata_task");
        assert!(bundle["request_schema"]["properties"]["mode"].is_object());
        assert!(bundle["result_schema"].is_object());

        let surfaces = serde_json::from_value::<Vec<SurfaceContractBundle>>(
            bundle["parameter_surfaces"].clone(),
        )
        .expect("serialized flagdata parameter surface");
        assert_eq!(surfaces.len(), 1);
        assert_eq!(surfaces[0].surface.id(), "flagdata");
        surfaces[0]
            .validate()
            .expect("embedded flagdata parameter surface");
    }
}
