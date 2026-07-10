// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-backed compatibility task adapter for GUI/TUI parity gaps.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use casa_calibration::{
    CalibrationPlotPreset, CalibrationPlotRequest, build_calibration_plot_payload,
};
use casa_ms::MsSelectionSpec;
use casa_ms::ui_schema::UiCommandSchema;
use casa_provider_contracts::{
    DefaultSpec, ParameterType, Predicate, SurfaceContractBundle, builtin_surface_bundle,
    project_ui_schema,
};
use serde_json::json;

const DEFAULT_CASA_TASKS_PYTHON: &str =
    "/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python";

fn main() {
    if let Err(error) = run(env::args_os().skip(1).collect()) {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run(args: Vec<OsString>) -> Result<(), String> {
    let task = extract_task(&args)?;
    let bundle = adapter_surface(task)?;

    if has_flag(&args, "-h") || has_flag(&args, "--help") {
        print!("{}", command_schema(&bundle).render_help());
        return Ok(());
    }
    if has_flag(&args, "--ui-schema") {
        print!(
            "{}",
            command_schema(&bundle)
                .render_json_pretty()
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if has_flag(&args, "--json-schema") {
        print!("{}", schema_bundle_json(&bundle)?);
        return Ok(());
    }
    if has_flag(&args, "--protocol-info") {
        print!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "protocol_name": "casars_casa_task_adapter",
                "protocol_version": 1,
                "surface_kind": "task",
                "backend": if task == "plotcal" { "casa-rs" } else { "casatasks" }
            }))
            .map_err(|error| error.to_string())?
        );
        return Ok(());
    }

    let values = parse_values(&bundle, &args)?;
    if task == "plotcal" {
        run_plotcal(values)
    } else {
        run_casatask(task, &bundle, values)
    }
}

fn adapter_surface(task: &str) -> Result<SurfaceContractBundle, String> {
    let bundle = builtin_surface_bundle(task)
        .map_err(|error| format!("unknown CASA-backed task {task:?}: {error}"))?;
    let execution = bundle.surface.execution();
    let routed_here = execution.invocation_name == "casars-casa-task"
        && execution
            .fixed_args
            .windows(2)
            .any(|args| args[0] == "--task" && args[1] == task);
    if !routed_here {
        return Err(format!(
            "task {task:?} is not routed through casars-casa-task"
        ));
    }
    Ok(bundle)
}

fn extract_task(args: &[OsString]) -> Result<&str, String> {
    for (index, arg) in args.iter().enumerate() {
        if arg == "--task" {
            return args
                .get(index + 1)
                .and_then(|value| value.to_str())
                .ok_or_else(|| "--task requires a value".to_string());
        }
        if let Some(value) = arg.to_str().and_then(|value| value.strip_prefix("--task=")) {
            return Ok(value);
        }
    }
    Err("--task is required".to_string())
}

fn has_flag(args: &[OsString], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn command_schema(bundle: &SurfaceContractBundle) -> UiCommandSchema {
    let mut schema: UiCommandSchema = serde_json::from_value(project_ui_schema(bundle))
        .expect("canonical adapter UI projection must match UiCommandSchema");
    schema.usage = format!(
        "{} {} [parameters]",
        schema.invocation_name,
        bundle.surface.execution().fixed_args.join(" ")
    );
    schema
}

fn schema_bundle_json(bundle: &SurfaceContractBundle) -> Result<String, String> {
    serde_json::to_string_pretty(&json!({
        "protocol": {
            "protocol_name": "casars_casa_task_adapter",
            "protocol_version": 1,
            "surface_kind": "task",
            "backend": if bundle.surface.id() == "plotcal" { "casa-rs" } else { "casatasks" }
        },
        "projections": {
            "ui_schema": project_ui_schema(bundle)
        },
        "parameter_surfaces": [bundle],
        "request_schema": {
            "type": "object",
            "additionalProperties": true
        },
        "result_schema": {
            "type": "object",
            "additionalProperties": true
        }
    }))
    .map_err(|error| error.to_string())
}

fn parse_values(
    bundle: &SurfaceContractBundle,
    args: &[OsString],
) -> Result<BTreeMap<String, String>, String> {
    let mut values = BTreeMap::new();
    let mut positionals = bundle
        .surface
        .bindings()
        .iter()
        .filter_map(|binding| {
            binding
                .projections
                .cli
                .as_ref()
                .and_then(|projection| projection.positional)
                .map(|position| (position, binding))
        })
        .collect::<BTreeMap<_, _>>();
    let mut positional_index = 0usize;
    let mut index = 0usize;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| format!("argument {index} is not valid UTF-8"))?;
        if raw == "--task" {
            index += 2;
            continue;
        }
        if raw.starts_with("--task=")
            || matches!(
                raw,
                "--ui-schema" | "--json-schema" | "--protocol-info" | "-h" | "--help"
            )
        {
            index += 1;
            continue;
        }
        if raw.starts_with("--") {
            let binding = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| {
                    binding.projections.cli.as_ref().is_some_and(|projection| {
                        projection.flags.iter().any(|flag| flag == raw)
                            || projection.false_flags.iter().any(|flag| flag == raw)
                    })
                })
                .ok_or_else(|| format!("{} does not accept option {raw}", bundle.surface.id()))?;
            let projection = binding
                .projections
                .cli
                .as_ref()
                .expect("matched CLI projection");
            let name = binding
                .projections
                .python
                .as_ref()
                .map_or(binding.name.as_str(), |projection| projection.name.as_str());
            if is_bool_domain(
                &bundle
                    .catalog
                    .concept(&binding.concept)
                    .expect("validated adapter concept")
                    .value_domain,
            ) {
                let enabled = !projection.false_flags.iter().any(|flag| flag == raw);
                values.insert(name.to_string(), enabled.to_string());
                index += 1;
                continue;
            }
            let value = args
                .get(index + 1)
                .and_then(|value| value.to_str())
                .ok_or_else(|| format!("{raw} requires a value"))?;
            values.insert(name.to_string(), value.to_string());
            index += 2;
            continue;
        }
        let binding = positionals
            .remove(&positional_index)
            .ok_or_else(|| format!("unexpected positional argument {raw:?}"))?;
        let name = binding
            .projections
            .python
            .as_ref()
            .map_or(binding.name.as_str(), |projection| projection.name.as_str());
        values.insert(name.to_string(), raw.to_string());
        positional_index += 1;
        index += 1;
    }

    for binding in bundle.surface.bindings() {
        if matches!(binding.default, DefaultSpec::Required)
            && matches!(binding.required_when, Predicate::Always)
        {
            let name = binding
                .projections
                .python
                .as_ref()
                .map_or(binding.name.as_str(), |projection| projection.name.as_str());
            if values.get(name).is_none_or(|value| value.trim().is_empty()) {
                return Err(format!(
                    "{} requires --{}",
                    bundle.surface.id(),
                    binding.name.replace('_', "-")
                ));
            }
        }
    }
    Ok(values)
}

fn is_bool_domain(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Bool => true,
        ParameterType::Optional { value, .. } => is_bool_domain(value),
        _ => false,
    }
}

fn run_casatask(
    task: &str,
    bundle: &SurfaceContractBundle,
    values: BTreeMap<String, String>,
) -> Result<(), String> {
    let python = env::var_os("CASA_RS_CASATASKS_PYTHON")
        .unwrap_or_else(|| OsString::from(DEFAULT_CASA_TASKS_PYTHON));
    let payload = serde_json::to_string(&json!({
        "task": task,
        "values": values,
        "param_types": bundle.surface.bindings().iter().map(|binding| {
            let name = binding.projections.python.as_ref()
                .map_or(binding.name.as_str(), |projection| projection.name.as_str());
            let domain = &bundle.catalog.concept(&binding.concept)
                .expect("validated adapter concept").value_domain;
            (name, value_type_name(domain))
        }).collect::<BTreeMap<_, _>>()
    }))
    .map_err(|error| error.to_string())?;

    let script = r#"
import ast
import contextlib
import io
import inspect
import json
import os
import sys

request = json.loads(sys.stdin.read())
task_name = request["task"]
values = request["values"]
param_types = request["param_types"]

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/casa-rs-mpl")
with contextlib.redirect_stdout(io.StringIO()) as captured:
    import casatasks

task = getattr(casatasks, task_name)
signature = inspect.signature(task)

def convert(name, value):
    kind = param_types.get(name, "string")
    if kind == "bool":
        return str(value).lower() == "true"
    if kind == "integer":
        return int(value)
    if kind == "float":
        return float(value)
    if kind == "array":
        text = str(value)
        try:
            parsed = ast.literal_eval(text)
            return list(parsed) if isinstance(parsed, (list, tuple)) else [parsed]
        except Exception:
            return [part.strip() for part in text.split(",") if part.strip()]
    text = str(value)
    if text == "":
        return text
    default = signature.parameters.get(name).default if name in signature.parameters else inspect._empty
    if isinstance(default, bool):
        return text.lower() == "true"
    if isinstance(default, int) and not isinstance(default, bool):
        return int(text)
    if isinstance(default, float):
        return float(text)
    if isinstance(default, (list, tuple, dict)):
        try:
            return ast.literal_eval(text)
        except Exception:
            return text
    if text[0:1] in "[{(":
        try:
            return ast.literal_eval(text)
        except Exception:
            return text
    return text

kwargs = {name: convert(name, value) for name, value in values.items()}
result = task(**kwargs)
print(json.dumps({"task": task_name, "kwargs": kwargs, "result": result}, default=str, indent=2))
"#;
    let mut child = Command::new(&python)
        .arg("-c")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            format!(
                "spawn CASA Python {}: {error}",
                PathBuf::from(&python).display()
            )
        })?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| "failed to open CASA Python stdin".to_string())?
        .write_all(payload.as_bytes())
        .map_err(|error| format!("write CASA task payload: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for CASA Python: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "CASA task {} exited with {}: {}",
            task,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.trim().is_empty() {
        eprintln!("{}", stderr.trim());
    }
    print!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

fn value_type_name(domain: &ParameterType) -> &'static str {
    match domain {
        ParameterType::Bool => "bool",
        ParameterType::Integer => "integer",
        ParameterType::Float => "float",
        ParameterType::Array { .. } => "array",
        ParameterType::Table { .. } => "table",
        ParameterType::Optional { value, .. } => value_type_name(value),
        ParameterType::String
        | ParameterType::Path { .. }
        | ParameterType::Choice { .. }
        | ParameterType::Quantity { .. } => "string",
    }
}

fn run_plotcal(values: BTreeMap<String, String>) -> Result<(), String> {
    let preset = values
        .get("preset")
        .map(String::as_str)
        .unwrap_or("gain_phase_vs_time");
    let preset = parse_plotcal_preset(preset)?;
    let request = CalibrationPlotRequest {
        measurement_set_path: optional_path(&values, "vis"),
        calibration_table_path: optional_path(&values, "caltable"),
        selection: MsSelectionSpec {
            selectdata: true,
            field: optional_string(&values, "field"),
            spw: optional_string(&values, "spw"),
            timerange: optional_string(&values, "timerange"),
            uvrange: optional_string(&values, "uvrange"),
            antenna: optional_string(&values, "antenna"),
            scan: optional_string(&values, "scan"),
            correlation: optional_string(&values, "correlation"),
            observation: optional_string(&values, "observation"),
            array: optional_string(&values, "array"),
            intent: optional_string(&values, "intent"),
            feed: optional_string(&values, "feed"),
            msselect: optional_string(&values, "msselect"),
        },
    };
    let payload = build_calibration_plot_payload(&request, preset)
        .map_err(|error| format!("plotcal failed: {error}"))?;
    print!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "task": "plotcal",
            "preset": format!("{preset:?}"),
            "payload_debug": format!("{payload:#?}"),
        }))
        .map_err(|error| error.to_string())?
    );
    Ok(())
}

fn optional_path(values: &BTreeMap<String, String>, key: &str) -> Option<PathBuf> {
    optional_string(values, key).map(PathBuf::from)
}

fn optional_string(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_plotcal_preset(value: &str) -> Result<CalibrationPlotPreset, String> {
    match value {
        "gain_phase_vs_time" => Ok(CalibrationPlotPreset::GainPhaseVsTime),
        "gain_amplitude_vs_time" => Ok(CalibrationPlotPreset::GainAmplitudeVsTime),
        "bandpass_amplitude_vs_frequency" => {
            Ok(CalibrationPlotPreset::BandpassAmplitudeVsFrequency)
        }
        "bandpass_phase_vs_frequency" => Ok(CalibrationPlotPreset::BandpassPhaseVsFrequency),
        "corrected_amplitude_vs_time" => Ok(CalibrationPlotPreset::CorrectedAmplitudeVsTime),
        "corrected_phase_vs_time" => Ok(CalibrationPlotPreset::CorrectedPhaseVsTime),
        "corrected_amplitude_vs_frequency" => {
            Ok(CalibrationPlotPreset::CorrectedAmplitudeVsFrequency)
        }
        "corrected_phase_vs_frequency" => Ok(CalibrationPlotPreset::CorrectedPhaseVsFrequency),
        other => Err(format!("unknown plotcal preset {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{SurfaceContractBundle, builtin_surface_catalog};

    use super::*;

    #[test]
    fn schema_bundles_embed_each_current_adapter_parameter_contract() {
        let aggregate = builtin_surface_catalog().expect("built-in parameter catalog");
        let adapter_surfaces = aggregate
            .surfaces
            .iter()
            .filter(|surface| surface.execution().invocation_name == "casars-casa-task")
            .collect::<Vec<_>>();
        assert!(!adapter_surfaces.is_empty());
        for surface in adapter_surfaces {
            let expected_id = surface.id();
            let contract = adapter_surface(expected_id).expect("current adapter surface");
            let bundle = serde_json::from_str::<serde_json::Value>(
                &schema_bundle_json(&contract).expect("serialize adapter schema bundle"),
            )
            .expect("parse adapter schema bundle");

            assert_eq!(
                bundle["protocol"]["protocol_name"],
                "casars_casa_task_adapter"
            );
            assert!(bundle["request_schema"].is_object());
            assert!(bundle["result_schema"].is_object());

            let surfaces = serde_json::from_value::<Vec<SurfaceContractBundle>>(
                bundle["parameter_surfaces"].clone(),
            )
            .expect("serialized adapter parameter surface");
            assert_eq!(surfaces.len(), 1);
            assert_eq!(surfaces[0].surface.id(), expected_id);
            surfaces[0]
                .validate()
                .expect("embedded adapter parameter surface");
            assert_eq!(
                bundle["projections"]["ui_schema"],
                project_ui_schema(&contract)
            );
        }
    }
}
