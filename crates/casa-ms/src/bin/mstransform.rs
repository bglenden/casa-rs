// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process;

use casa_ms::presentation::UiCommandSchema;
use casa_ms::selection::MsSelection;
use casa_ms::{
    MsTransformReport, MsTransformRequest, MsTransformTaskRequest, TransformDataColumn,
    mstransform, mstransform_task_schema_bundle, parse_numeric_id_selector,
};

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
    let (logging_guard, args) =
        casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1))
            .map_err(|error| format!("failed to initialize logging: {error}"))?;
    tracing::info!("mstransform started");
    let result = run_with_args(args);
    if result.is_ok() {
        tracing::info!("mstransform completed");
    } else if let Err(error) = &result {
        tracing::error!(casa.priority = "SEVERE", error = %error, "mstransform failed");
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
            command_schema("mstransform").render_help(),
            casa_task_runtime::task_cli_machine_help("MsTransformTaskRequest")
        );
        return Ok(());
    }
    let host =
        casa_task_runtime::TaskCliHost::new(mstransform_task_schema_bundle(), execute_task_request);
    if let Some(output) = host.dispatch(&args).map_err(|error| error.to_string())? {
        println!("{output}");
        return Ok(());
    }
    let args = os_args_to_strings(args)?;
    let request = parse_request(&args)?;
    let report = mstransform(&request).map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn execute_task_request(request: MsTransformTaskRequest) -> Result<MsTransformReport, String> {
    let mut selection = MsSelection::default();
    if !request.field.is_empty() {
        selection = selection.field(
            &parse_numeric_id_selector(&request.field, "field")
                .map_err(|error| error.to_string())?,
        );
    }
    if !request.scan.is_empty() {
        selection = selection.scan(
            &parse_numeric_id_selector(&request.scan, "scan").map_err(|error| error.to_string())?,
        );
    }
    if !request.antenna.is_empty() {
        selection = selection.antenna(
            &parse_numeric_id_selector(&request.antenna, "antenna")
                .map_err(|error| error.to_string())?,
        );
    }
    if !request.timerange.is_empty() {
        let (start, end) = parse_time_range(&request.timerange)?;
        selection = selection.time_range(start, end);
    }
    if !request.msselect.is_empty() {
        selection = selection.taql(&request.msselect);
    }
    if request.width == 0 {
        return Err("width must be at least 1".to_string());
    }
    mstransform(&MsTransformRequest {
        input_ms: request.vis,
        output_ms: request.outputvis,
        spw: request.spw,
        width: request.width,
        data_column: parse_data_column(&request.datacolumn)?,
        selection,
        keep_flags: request.keepflags,
    })
    .map_err(|error| error.to_string())
}

fn os_args_to_strings(args: Vec<OsString>) -> Result<Vec<String>, String> {
    args.into_iter()
        .map(|arg| {
            arg.into_string()
                .map_err(|_| "non-UTF-8 command-line argument".to_string())
        })
        .collect()
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
    let bundle = casa_provider_contracts::builtin_surface_bundle("mstransform")
        .expect("built-in mstransform parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical mstransform UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_bundle_embeds_transform_family_parameter_contracts() {
        let bundle = mstransform_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, "casa_ms_transform_task");
        assert_eq!(
            bundle
                .parameter_surfaces
                .iter()
                .map(|surface| surface.surface.id())
                .collect::<Vec<_>>(),
            ["mstransform", "split"]
        );
        bundle.validate().expect("valid transform provider");
    }
}
