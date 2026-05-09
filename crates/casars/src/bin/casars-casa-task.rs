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
use casa_ms::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind,
};
use serde_json::json;

const DEFAULT_CASA_TASKS_PYTHON: &str =
    "/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python";

#[derive(Debug, Clone, Copy)]
struct TaskSpec {
    id: &'static str,
    display_name: &'static str,
    category: &'static str,
    summary: &'static str,
    mutation_class: MutationClass,
    params: &'static [ParamSpec],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationClass {
    ReadOnly,
    WritesProducts,
    MutatesInput,
}

#[derive(Debug, Clone, Copy)]
struct ParamSpec {
    id: &'static str,
    label: &'static str,
    value_kind: UiValueKind,
    default: Option<&'static str>,
    required: bool,
    choices: &'static [&'static str],
    help: &'static str,
    group: &'static str,
}

fn main() {
    if let Err(error) = run(env::args_os().skip(1).collect()) {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run(args: Vec<OsString>) -> Result<(), String> {
    let task = extract_task(&args)?;
    let spec = task_spec(task).ok_or_else(|| format!("unknown CASA-backed task {task:?}"))?;

    if has_flag(&args, "-h") || has_flag(&args, "--help") {
        print!("{}", command_schema(spec).render_help());
        return Ok(());
    }
    if has_flag(&args, "--ui-schema") {
        print!(
            "{}",
            command_schema(spec)
                .render_json_pretty()
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if has_flag(&args, "--json-schema") {
        print!("{}", schema_bundle_json(spec)?);
        return Ok(());
    }
    if has_flag(&args, "--protocol-info") {
        print!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "protocol_name": "casars_casa_task_adapter",
                "protocol_version": 1,
                "surface_kind": "task",
                "backend": if spec.id == "plotcal" { "casa-rs" } else { "casatasks" }
            }))
            .map_err(|error| error.to_string())?
        );
        return Ok(());
    }

    let values = parse_values(spec, &args)?;
    if spec.id == "plotcal" {
        run_plotcal(values)
    } else {
        run_casatask(spec, values)
    }
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

fn command_schema(spec: &TaskSpec) -> UiCommandSchema {
    let mut arguments = vec![UiArgumentSchema {
        id: "task".to_string(),
        label: "Task".to_string(),
        order: 0,
        parser: UiArgumentParser::Option {
            flags: vec!["--task".to_string()],
            metavar: "TASK".to_string(),
            choices: vec![spec.id.to_string()],
        },
        value_kind: UiValueKind::Choice,
        required: false,
        default: Some(spec.id.to_string()),
        help: "Hidden CASA task adapter selector.".to_string(),
        group: "Meta".to_string(),
        advanced: true,
        hidden_in_tui: true,
    }];
    arguments.extend(
        spec.params
            .iter()
            .enumerate()
            .map(|(index, param)| argument_schema(index + 1, *param)),
    );
    arguments.push(action_argument(
        "help",
        "Help",
        900,
        &["-h", "--help"],
        UiActionKind::Help,
    ));
    arguments.push(action_argument(
        "ui_schema",
        "UI Schema",
        901,
        &["--ui-schema"],
        UiActionKind::UiSchema,
    ));

    UiCommandSchema {
        schema_version: 1,
        command_id: spec.id.to_string(),
        invocation_name: "casars-casa-task".to_string(),
        display_name: spec.display_name.to_string(),
        category: spec.category.to_string(),
        summary: spec.summary.to_string(),
        usage: format!("casars-casa-task --task {} [options]", spec.id),
        arguments,
        managed_output: None,
    }
}

fn argument_schema(order: usize, param: ParamSpec) -> UiArgumentSchema {
    let flag = format!("--{}", param.id.replace('_', "-"));
    let parser = if param.value_kind == UiValueKind::Bool {
        UiArgumentParser::Toggle {
            true_flags: vec![flag],
            false_flags: vec![format!("--no-{}", param.id.replace('_', "-"))],
        }
    } else {
        UiArgumentParser::Option {
            flags: vec![flag],
            metavar: param.id.to_ascii_uppercase(),
            choices: param
                .choices
                .iter()
                .map(|choice| (*choice).to_string())
                .collect(),
        }
    };
    UiArgumentSchema {
        id: param.id.to_string(),
        label: param.label.to_string(),
        order,
        parser,
        value_kind: param.value_kind,
        required: param.required,
        default: param.default.map(str::to_string),
        help: param.help.to_string(),
        group: param.group.to_string(),
        advanced: !param.required && param.group == "Advanced",
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
        group: "Meta".to_string(),
        advanced: true,
        hidden_in_tui: true,
    }
}

fn schema_bundle_json(spec: &TaskSpec) -> Result<String, String> {
    serde_json::to_string_pretty(&json!({
        "protocol": {
            "protocol_name": "casars_casa_task_adapter",
            "protocol_version": 1,
            "surface_kind": "task",
            "backend": if spec.id == "plotcal" { "casa-rs" } else { "casatasks" },
            "mutation_class": mutation_class_name(spec.mutation_class)
        },
        "projections": {
            "ui_schema": command_schema(spec)
        },
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

fn mutation_class_name(class: MutationClass) -> &'static str {
    match class {
        MutationClass::ReadOnly => "read_only",
        MutationClass::WritesProducts => "writes_products",
        MutationClass::MutatesInput => "mutates_input",
    }
}

fn parse_values(spec: &TaskSpec, args: &[OsString]) -> Result<BTreeMap<String, String>, String> {
    let mut values = BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| format!("argument {index} is not valid UTF-8"))?;
        if matches!(
            raw,
            "--task" | "--ui-schema" | "--json-schema" | "--protocol-info" | "-h" | "--help"
        ) {
            index += if raw == "--task" { 2 } else { 1 };
            continue;
        }
        if raw.starts_with("--no-") {
            let id = raw.trim_start_matches("--no-").replace('-', "_");
            values.insert(id, "false".to_string());
            index += 1;
            continue;
        }
        if raw.starts_with("--") {
            let id = raw.trim_start_matches("--").replace('-', "_");
            let param = spec
                .params
                .iter()
                .find(|param| param.id == id)
                .ok_or_else(|| format!("{} does not accept option {raw}", spec.id))?;
            if param.value_kind == UiValueKind::Bool {
                values.insert(id, "true".to_string());
                index += 1;
                continue;
            }
            let value = args
                .get(index + 1)
                .and_then(|value| value.to_str())
                .ok_or_else(|| format!("{raw} requires a value"))?;
            values.insert(id, value.to_string());
            index += 2;
            continue;
        }
        return Err(format!("unexpected positional argument {raw:?}"));
    }
    for param in spec.params {
        if param.required
            && values
                .get(param.id)
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(format!(
                "{} requires --{}",
                spec.id,
                param.id.replace('_', "-")
            ));
        }
    }
    Ok(values)
}

fn run_casatask(spec: &TaskSpec, values: BTreeMap<String, String>) -> Result<(), String> {
    let python = env::var_os("CASA_RS_CASATASKS_PYTHON")
        .unwrap_or_else(|| OsString::from(DEFAULT_CASA_TASKS_PYTHON));
    let payload = serde_json::to_string(&json!({
        "task": spec.id,
        "values": values,
        "param_types": spec.params.iter().map(|param| (param.id, value_type_name(param.value_kind))).collect::<BTreeMap<_, _>>()
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
    if kind == "float":
        return float(text)
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
            spec.id,
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

fn value_type_name(kind: UiValueKind) -> &'static str {
    match kind {
        UiValueKind::Bool => "bool",
        UiValueKind::Float => "float",
        UiValueKind::Choice => "choice",
        UiValueKind::Path => "path",
        UiValueKind::String => "string",
        UiValueKind::None => "none",
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

fn task_spec(id: &str) -> Option<&'static TaskSpec> {
    TASKS.iter().find(|task| task.id == id)
}

const PLOTCAL_PARAMS: &[ParamSpec] = &[
    path_param(
        "caltable",
        "Calibration Table",
        None,
        false,
        "Input calibration table.",
        "Input",
    ),
    path_param(
        "vis",
        "MeasurementSet",
        None,
        false,
        "Input MeasurementSet for corrected-data diagnostics.",
        "Input",
    ),
    choice_param(
        "preset",
        "Preset",
        Some("gain_phase_vs_time"),
        &[
            "gain_phase_vs_time",
            "gain_amplitude_vs_time",
            "bandpass_amplitude_vs_frequency",
            "bandpass_phase_vs_frequency",
            "corrected_amplitude_vs_time",
            "corrected_phase_vs_time",
            "corrected_amplitude_vs_frequency",
            "corrected_phase_vs_frequency",
        ],
        false,
        "Plot preset.",
        "Plot",
    ),
    string_param(
        "field",
        "Field",
        None,
        false,
        "CASA field selector.",
        "Selection",
    ),
    string_param(
        "spw",
        "Spectral Window",
        None,
        false,
        "CASA SPW selector.",
        "Selection",
    ),
    string_param(
        "timerange",
        "Time Range",
        None,
        false,
        "CASA time selector.",
        "Selection",
    ),
    string_param(
        "uvrange",
        "UV Range",
        None,
        false,
        "CASA UV range selector.",
        "Selection",
    ),
    string_param(
        "antenna",
        "Antenna",
        None,
        false,
        "CASA antenna selector.",
        "Selection",
    ),
    string_param(
        "scan",
        "Scan",
        None,
        false,
        "CASA scan selector.",
        "Selection",
    ),
    string_param(
        "correlation",
        "Correlation",
        None,
        false,
        "CASA correlation selector.",
        "Selection",
    ),
    string_param(
        "observation",
        "Observation",
        None,
        false,
        "CASA observation selector.",
        "Selection",
    ),
    string_param(
        "array",
        "Array",
        None,
        false,
        "CASA array selector.",
        "Selection",
    ),
    string_param(
        "intent",
        "Intent",
        None,
        false,
        "CASA intent selector.",
        "Selection",
    ),
    string_param(
        "feed",
        "Feed",
        None,
        false,
        "CASA feed selector.",
        "Selection",
    ),
    string_param(
        "msselect",
        "MS Select",
        None,
        false,
        "TAQL selection.",
        "Selection",
    ),
];

const IMCOLLAPSE_PARAMS: &[ParamSpec] = &[
    path_param(
        "imagename",
        "Image",
        None,
        true,
        "Input CASA image.",
        "Input",
    ),
    choice_param(
        "function",
        "Function",
        Some("mean"),
        &[
            "mean", "median", "sum", "flux", "sqrtsum", "stddev", "min", "max",
        ],
        true,
        "Collapse function.",
        "Collapse",
    ),
    string_param(
        "axes",
        "Axes",
        Some("[]"),
        false,
        "Axes to collapse, e.g. [2].",
        "Collapse",
    ),
    path_param(
        "outfile",
        "Output Image",
        None,
        true,
        "Output image.",
        "Output",
    ),
    string_param("box", "Box", Some(""), false, "Pixel box.", "Selection"),
    string_param(
        "region",
        "Region",
        Some(""),
        false,
        "Region file or expression.",
        "Selection",
    ),
    string_param(
        "chans",
        "Channels",
        Some(""),
        false,
        "Channel selector.",
        "Selection",
    ),
    string_param(
        "stokes",
        "Stokes",
        Some(""),
        false,
        "Stokes selector.",
        "Selection",
    ),
    string_param(
        "mask",
        "Mask",
        Some(""),
        false,
        "Mask expression.",
        "Selection",
    ),
    bool_param(
        "overwrite",
        "Overwrite",
        Some("false"),
        false,
        "Overwrite output.",
        "Output",
    ),
    bool_param(
        "stretch",
        "Stretch",
        Some("false"),
        false,
        "Stretch mask.",
        "Advanced",
    ),
];

const IMFIT_PARAMS: &[ParamSpec] = &[
    path_param(
        "imagename",
        "Image",
        None,
        true,
        "Input CASA image.",
        "Input",
    ),
    string_param("box", "Box", Some(""), false, "Pixel box.", "Selection"),
    string_param(
        "region",
        "Region",
        Some(""),
        false,
        "Region file or expression.",
        "Selection",
    ),
    string_param(
        "chans",
        "Channels",
        Some(""),
        false,
        "Channel selector.",
        "Selection",
    ),
    string_param(
        "stokes",
        "Stokes",
        Some(""),
        false,
        "Stokes selector.",
        "Selection",
    ),
    string_param(
        "mask",
        "Mask",
        Some(""),
        false,
        "Mask expression.",
        "Selection",
    ),
    string_param(
        "includepix",
        "Include Pixels",
        Some("[]"),
        false,
        "Pixel include range.",
        "Selection",
    ),
    string_param(
        "excludepix",
        "Exclude Pixels",
        Some("[]"),
        false,
        "Pixel exclude range.",
        "Selection",
    ),
    path_param(
        "residual",
        "Residual Image",
        Some(""),
        false,
        "Residual output image.",
        "Output",
    ),
    path_param(
        "model",
        "Model Image",
        Some(""),
        false,
        "Model output image.",
        "Output",
    ),
    path_param(
        "estimates",
        "Estimates",
        Some(""),
        false,
        "Input estimates file.",
        "Input",
    ),
    path_param(
        "logfile",
        "Log File",
        Some(""),
        false,
        "Fit log output.",
        "Output",
    ),
    bool_param(
        "append",
        "Append Log",
        Some("true"),
        false,
        "Append to logfile.",
        "Output",
    ),
    path_param(
        "newestimates",
        "New Estimates",
        Some(""),
        false,
        "Updated estimates output.",
        "Output",
    ),
    path_param(
        "complist",
        "Component List",
        Some(""),
        false,
        "Component-list output.",
        "Output",
    ),
    bool_param(
        "overwrite",
        "Overwrite",
        Some("false"),
        false,
        "Overwrite outputs.",
        "Output",
    ),
    bool_param(
        "dooff",
        "Fit Offset",
        Some("false"),
        false,
        "Fit zero-level offset.",
        "Advanced",
    ),
    float_param(
        "offset",
        "Offset",
        Some("0.0"),
        false,
        "Initial offset.",
        "Advanced",
    ),
    bool_param(
        "fixoffset",
        "Fix Offset",
        Some("false"),
        false,
        "Hold offset fixed.",
        "Advanced",
    ),
    bool_param(
        "stretch",
        "Stretch",
        Some("false"),
        false,
        "Stretch mask.",
        "Advanced",
    ),
    float_param("rms", "RMS", Some("0"), false, "Noise RMS.", "Advanced"),
    string_param(
        "noisefwhm",
        "Noise FWHM",
        Some(""),
        false,
        "Noise correlation FWHM.",
        "Advanced",
    ),
    path_param(
        "summary",
        "Summary",
        Some(""),
        false,
        "Summary output.",
        "Output",
    ),
];

const IMPBCOR_PARAMS: &[ParamSpec] = &[
    path_param("imagename", "Image", None, true, "Input image.", "Input"),
    string_param(
        "pbimage",
        "PB Image",
        Some("[]"),
        true,
        "Primary-beam image or scalar.",
        "Input",
    ),
    path_param(
        "outfile",
        "Output Image",
        None,
        true,
        "Output image.",
        "Output",
    ),
    bool_param(
        "overwrite",
        "Overwrite",
        Some("false"),
        false,
        "Overwrite output.",
        "Output",
    ),
    string_param("box", "Box", Some(""), false, "Pixel box.", "Selection"),
    string_param(
        "region",
        "Region",
        Some(""),
        false,
        "Region file or expression.",
        "Selection",
    ),
    string_param(
        "chans",
        "Channels",
        Some(""),
        false,
        "Channel selector.",
        "Selection",
    ),
    string_param(
        "stokes",
        "Stokes",
        Some(""),
        false,
        "Stokes selector.",
        "Selection",
    ),
    string_param(
        "mask",
        "Mask",
        Some(""),
        false,
        "Mask expression.",
        "Selection",
    ),
    choice_param(
        "mode",
        "Mode",
        Some("divide"),
        &["divide", "multiply"],
        false,
        "Correction mode.",
        "Correction",
    ),
    float_param(
        "cutoff",
        "Cutoff",
        Some("-1.0"),
        false,
        "PB cutoff.",
        "Correction",
    ),
    bool_param(
        "stretch",
        "Stretch",
        Some("false"),
        false,
        "Stretch mask.",
        "Advanced",
    ),
];

const WIDEBANDPBCOR_PARAMS: &[ParamSpec] = &[
    path_param(
        "vis",
        "MeasurementSet",
        Some(""),
        false,
        "Input MeasurementSet.",
        "Input",
    ),
    path_param(
        "imagename",
        "Image Root",
        None,
        true,
        "MT-MFS image root.",
        "Input",
    ),
    string_param(
        "nterms",
        "N Terms",
        Some("2"),
        false,
        "Number of Taylor terms.",
        "Correction",
    ),
    string_param(
        "threshold",
        "Threshold",
        Some(""),
        false,
        "Mask threshold.",
        "Correction",
    ),
    choice_param(
        "action",
        "Action",
        Some("pbcor"),
        &["pbcor", "calcalpha"],
        false,
        "Wideband PB action.",
        "Correction",
    ),
    string_param(
        "reffreq",
        "Reference Frequency",
        Some(""),
        false,
        "Reference frequency.",
        "Correction",
    ),
    float_param(
        "pbmin",
        "PB Min",
        Some("0.2"),
        false,
        "Minimum PB.",
        "Correction",
    ),
    string_param(
        "field",
        "Field",
        Some(""),
        false,
        "Field selector.",
        "Selection",
    ),
    string_param(
        "spwlist",
        "SPW List",
        Some("[0]"),
        false,
        "SPW list.",
        "Selection",
    ),
    string_param(
        "chanlist",
        "Channel List",
        Some("[0]"),
        false,
        "Channel list.",
        "Selection",
    ),
    string_param(
        "weightlist",
        "Weight List",
        Some("[0.0]"),
        false,
        "Weight list.",
        "Selection",
    ),
];

const CONCAT_PARAMS: &[ParamSpec] = &[
    string_param(
        "vis",
        "Input MS List",
        None,
        true,
        "Input MeasurementSets as a Python list.",
        "Input",
    ),
    path_param(
        "concatvis",
        "Output MS",
        None,
        true,
        "Concatenated MeasurementSet.",
        "Output",
    ),
    string_param(
        "freqtol",
        "Frequency Tolerance",
        Some(""),
        false,
        "Frequency tolerance.",
        "Combine",
    ),
    string_param(
        "dirtol",
        "Direction Tolerance",
        Some(""),
        false,
        "Direction tolerance.",
        "Combine",
    ),
    bool_param(
        "respectname",
        "Respect Names",
        Some("false"),
        false,
        "Respect source names.",
        "Combine",
    ),
    bool_param(
        "timesort",
        "Time Sort",
        Some("false"),
        false,
        "Sort by time.",
        "Combine",
    ),
    bool_param(
        "copypointing",
        "Copy Pointing",
        Some("true"),
        false,
        "Copy POINTING table.",
        "Combine",
    ),
    string_param(
        "visweightscale",
        "Visibility Weight Scale",
        Some("[]"),
        false,
        "Per-MS weight scaling list.",
        "Combine",
    ),
    string_param(
        "forcesingleephemfield",
        "Force Single Ephem Field",
        Some(""),
        false,
        "Ephemeris field override.",
        "Advanced",
    ),
];

const STATWT_PARAMS: &[ParamSpec] = &[
    path_param(
        "vis",
        "MeasurementSet",
        None,
        true,
        "Input MeasurementSet.",
        "Input",
    ),
    bool_param(
        "selectdata",
        "Select Data",
        Some("true"),
        false,
        "Enable data selection.",
        "Selection",
    ),
    string_param(
        "field",
        "Field",
        Some(""),
        false,
        "Field selector.",
        "Selection",
    ),
    string_param(
        "spw",
        "Spectral Window",
        Some(""),
        false,
        "SPW selector.",
        "Selection",
    ),
    string_param(
        "intent",
        "Intent",
        Some(""),
        false,
        "Intent selector.",
        "Selection",
    ),
    string_param(
        "array",
        "Array",
        Some(""),
        false,
        "Array selector.",
        "Selection",
    ),
    string_param(
        "observation",
        "Observation",
        Some(""),
        false,
        "Observation selector.",
        "Selection",
    ),
    string_param(
        "scan",
        "Scan",
        Some(""),
        false,
        "Scan selector.",
        "Selection",
    ),
    string_param(
        "combine",
        "Combine",
        Some(""),
        false,
        "Axes to combine.",
        "Weighting",
    ),
    float_param(
        "timebin",
        "Time Bin",
        Some("1"),
        false,
        "Time bin.",
        "Weighting",
    ),
    bool_param(
        "slidetimebin",
        "Slide Time Bin",
        Some("false"),
        false,
        "Use sliding time bins.",
        "Weighting",
    ),
    string_param(
        "chanbin",
        "Channel Bin",
        Some("spw"),
        false,
        "Channel bin mode.",
        "Weighting",
    ),
    string_param(
        "minsamp",
        "Min Samples",
        Some("2"),
        false,
        "Minimum samples.",
        "Weighting",
    ),
    choice_param(
        "statalg",
        "Statistic",
        Some("classic"),
        &["classic", "chauvenet", "fit-half", "hinges-fences"],
        false,
        "Statistics algorithm.",
        "Weighting",
    ),
    float_param(
        "fence",
        "Fence",
        Some("-1.0"),
        false,
        "Outlier fence.",
        "Weighting",
    ),
    choice_param(
        "center",
        "Center",
        Some("mean"),
        &["mean", "median"],
        false,
        "Center estimator.",
        "Weighting",
    ),
    bool_param(
        "lside",
        "Left Side",
        Some("true"),
        false,
        "Use left side for fit-half.",
        "Weighting",
    ),
    float_param(
        "zscore",
        "Z Score",
        Some("-1.0"),
        false,
        "Z-score cutoff.",
        "Weighting",
    ),
    string_param(
        "maxiter",
        "Max Iterations",
        Some("-1"),
        false,
        "Maximum iterations.",
        "Weighting",
    ),
    string_param(
        "fitspw",
        "Fit SPW",
        Some(""),
        false,
        "Fit SPW selector.",
        "Selection",
    ),
    bool_param(
        "excludechans",
        "Exclude Channels",
        Some("false"),
        false,
        "Exclude fit channels.",
        "Selection",
    ),
    string_param(
        "wtrange",
        "Weight Range",
        Some("[]"),
        false,
        "Allowed weight range.",
        "Weighting",
    ),
    bool_param(
        "flagbackup",
        "Flag Backup",
        Some("true"),
        false,
        "Back up flags.",
        "Safety",
    ),
    bool_param(
        "preview",
        "Preview",
        Some("false"),
        false,
        "Preview without applying.",
        "Safety",
    ),
    choice_param(
        "datacolumn",
        "Data Column",
        Some("corrected"),
        &["data", "corrected", "float_data"],
        false,
        "Data column.",
        "Input",
    ),
];

const HANNINGSMOOTH_PARAMS: &[ParamSpec] = &[
    path_param(
        "vis",
        "MeasurementSet",
        None,
        true,
        "Input MeasurementSet.",
        "Input",
    ),
    path_param(
        "outputvis",
        "Output MS",
        None,
        true,
        "Output MeasurementSet.",
        "Output",
    ),
    bool_param(
        "keepmms",
        "Keep MMS",
        Some("true"),
        false,
        "Preserve MMS structure.",
        "Output",
    ),
    string_param(
        "field",
        "Field",
        Some(""),
        false,
        "Field selector.",
        "Selection",
    ),
    string_param(
        "spw",
        "Spectral Window",
        Some(""),
        false,
        "SPW selector.",
        "Selection",
    ),
    string_param(
        "scan",
        "Scan",
        Some(""),
        false,
        "Scan selector.",
        "Selection",
    ),
    string_param(
        "antenna",
        "Antenna",
        Some(""),
        false,
        "Antenna selector.",
        "Selection",
    ),
    string_param(
        "correlation",
        "Correlation",
        Some(""),
        false,
        "Correlation selector.",
        "Selection",
    ),
    string_param(
        "timerange",
        "Time Range",
        Some(""),
        false,
        "Time selector.",
        "Selection",
    ),
    string_param(
        "intent",
        "Intent",
        Some(""),
        false,
        "Intent selector.",
        "Selection",
    ),
    string_param(
        "array",
        "Array",
        Some(""),
        false,
        "Array selector.",
        "Selection",
    ),
    string_param(
        "uvrange",
        "UV Range",
        Some(""),
        false,
        "UV range selector.",
        "Selection",
    ),
    string_param(
        "observation",
        "Observation",
        Some(""),
        false,
        "Observation selector.",
        "Selection",
    ),
    string_param(
        "feed",
        "Feed",
        Some(""),
        false,
        "Feed selector.",
        "Selection",
    ),
    string_param(
        "smooth_spw",
        "Smooth SPW",
        Some(""),
        false,
        "SPW smoothing selector.",
        "Selection",
    ),
    choice_param(
        "datacolumn",
        "Data Column",
        Some("all"),
        &["all", "data", "corrected", "model"],
        false,
        "Data column.",
        "Input",
    ),
];

const CLEARCAL_PARAMS: &[ParamSpec] = &[
    path_param(
        "vis",
        "MeasurementSet",
        None,
        true,
        "Input MeasurementSet.",
        "Input",
    ),
    string_param(
        "field",
        "Field",
        Some(""),
        false,
        "Field selector.",
        "Selection",
    ),
    string_param(
        "spw",
        "Spectral Window",
        Some(""),
        false,
        "SPW selector.",
        "Selection",
    ),
    string_param(
        "intent",
        "Intent",
        Some(""),
        false,
        "Intent selector.",
        "Selection",
    ),
    bool_param(
        "addmodel",
        "Add MODEL_DATA",
        Some("false"),
        false,
        "Create MODEL_DATA column.",
        "Calibration",
    ),
];

const DELMOD_PARAMS: &[ParamSpec] = &[
    path_param(
        "vis",
        "MeasurementSet",
        None,
        true,
        "Input MeasurementSet.",
        "Input",
    ),
    bool_param(
        "otf",
        "On-The-Fly Model",
        Some("true"),
        false,
        "Delete OTF model.",
        "Calibration",
    ),
    string_param(
        "field",
        "Field",
        Some(""),
        false,
        "Field selector.",
        "Selection",
    ),
    bool_param(
        "scr",
        "Scratch Model",
        Some("false"),
        false,
        "Delete scratch model.",
        "Calibration",
    ),
];

const FT_PARAMS: &[ParamSpec] = &[
    path_param(
        "vis",
        "MeasurementSet",
        None,
        true,
        "Input MeasurementSet.",
        "Input",
    ),
    string_param(
        "field",
        "Field",
        Some(""),
        false,
        "Field selector.",
        "Selection",
    ),
    string_param(
        "spw",
        "Spectral Window",
        Some(""),
        false,
        "SPW selector.",
        "Selection",
    ),
    string_param(
        "model",
        "Model Image",
        Some(""),
        false,
        "Model image list.",
        "Input",
    ),
    string_param(
        "nterms",
        "N Terms",
        Some("1"),
        false,
        "Number of Taylor terms.",
        "Model",
    ),
    string_param(
        "reffreq",
        "Reference Frequency",
        Some(""),
        false,
        "Reference frequency.",
        "Model",
    ),
    path_param(
        "complist",
        "Component List",
        Some(""),
        false,
        "Component list.",
        "Input",
    ),
    bool_param(
        "incremental",
        "Incremental",
        Some("false"),
        false,
        "Add to existing model.",
        "Model",
    ),
    bool_param(
        "usescratch",
        "Use Scratch",
        Some("false"),
        false,
        "Write MODEL_DATA.",
        "Model",
    ),
];

const IMCONTSUB_PARAMS: &[ParamSpec] = &[
    path_param(
        "imagename",
        "Image",
        None,
        true,
        "Input image cube.",
        "Input",
    ),
    path_param(
        "linefile",
        "Line Output",
        None,
        true,
        "Line output image.",
        "Output",
    ),
    path_param(
        "contfile",
        "Continuum Output",
        None,
        true,
        "Continuum output image.",
        "Output",
    ),
    string_param(
        "fitorder",
        "Fit Order",
        Some("0"),
        false,
        "Polynomial fit order.",
        "Fit",
    ),
    string_param(
        "region",
        "Region",
        Some(""),
        false,
        "Region selector.",
        "Selection",
    ),
    string_param("box", "Box", Some(""), false, "Pixel box.", "Selection"),
    string_param(
        "chans",
        "Channels",
        Some(""),
        false,
        "Fit channel selector.",
        "Selection",
    ),
    string_param(
        "stokes",
        "Stokes",
        Some(""),
        false,
        "Stokes selector.",
        "Selection",
    ),
];

const SIMANALYZE_PARAMS: &[ParamSpec] = &[
    string_param(
        "project",
        "Project",
        Some("sim"),
        false,
        "Simulation project.",
        "Project",
    ),
    bool_param(
        "image",
        "Image",
        Some("true"),
        false,
        "Run imaging.",
        "Image",
    ),
    path_param(
        "imagename",
        "Image Name",
        Some("default"),
        false,
        "Output image name.",
        "Image",
    ),
    path_param(
        "skymodel",
        "Sky Model",
        Some(""),
        false,
        "Sky model image.",
        "Input",
    ),
    path_param(
        "vis",
        "MeasurementSet",
        Some("default"),
        false,
        "Input MS.",
        "Input",
    ),
    path_param(
        "modelimage",
        "Model Image",
        Some(""),
        false,
        "Model image.",
        "Input",
    ),
    string_param(
        "imsize",
        "Image Size",
        Some("[0, 0]"),
        false,
        "Image size.",
        "Image",
    ),
    string_param(
        "imdirection",
        "Image Direction",
        Some(""),
        false,
        "Image direction.",
        "Image",
    ),
    string_param("cell", "Cell", Some(""), false, "Cell size.", "Image"),
    bool_param(
        "interactive",
        "Interactive",
        Some("false"),
        false,
        "Interactive clean.",
        "Image",
    ),
    string_param(
        "niter",
        "Iterations",
        Some("0"),
        false,
        "Clean iterations.",
        "Image",
    ),
    string_param(
        "threshold",
        "Threshold",
        Some("0.1mJy"),
        false,
        "Clean threshold.",
        "Image",
    ),
    choice_param(
        "weighting",
        "Weighting",
        Some("natural"),
        &["natural", "uniform", "briggs"],
        false,
        "Visibility weighting.",
        "Image",
    ),
    string_param("mask", "Mask", Some("[]"), false, "Clean mask.", "Image"),
    string_param(
        "outertaper",
        "Outer Taper",
        Some("[]"),
        false,
        "Outer taper.",
        "Image",
    ),
    bool_param(
        "pbcor",
        "PB Correction",
        Some("true"),
        false,
        "Apply PB correction.",
        "Image",
    ),
    string_param(
        "stokes",
        "Stokes",
        Some("I"),
        false,
        "Stokes planes.",
        "Image",
    ),
    path_param(
        "featherimage",
        "Feather Image",
        Some(""),
        false,
        "Feather image.",
        "Analyze",
    ),
    bool_param(
        "analyze",
        "Analyze",
        Some("false"),
        false,
        "Run analysis.",
        "Analyze",
    ),
    bool_param(
        "showuv",
        "Show UV",
        Some("true"),
        false,
        "Show UV plot.",
        "Analyze",
    ),
    bool_param(
        "showpsf",
        "Show PSF",
        Some("true"),
        false,
        "Show PSF.",
        "Analyze",
    ),
    bool_param(
        "showmodel",
        "Show Model",
        Some("true"),
        false,
        "Show model.",
        "Analyze",
    ),
    bool_param(
        "showconvolved",
        "Show Convolved",
        Some("false"),
        false,
        "Show convolved model.",
        "Analyze",
    ),
    bool_param(
        "showclean",
        "Show Clean",
        Some("true"),
        false,
        "Show clean image.",
        "Analyze",
    ),
    bool_param(
        "showresidual",
        "Show Residual",
        Some("false"),
        false,
        "Show residual.",
        "Analyze",
    ),
    bool_param(
        "showdifference",
        "Show Difference",
        Some("true"),
        false,
        "Show difference.",
        "Analyze",
    ),
    bool_param(
        "showfidelity",
        "Show Fidelity",
        Some("true"),
        false,
        "Show fidelity.",
        "Analyze",
    ),
    choice_param(
        "graphics",
        "Graphics",
        Some("both"),
        &["both", "file", "screen", "none"],
        false,
        "Graphics output.",
        "Output",
    ),
    bool_param(
        "verbose",
        "Verbose",
        Some("false"),
        false,
        "Verbose logging.",
        "Output",
    ),
    bool_param(
        "overwrite",
        "Overwrite",
        Some("true"),
        false,
        "Overwrite outputs.",
        "Output",
    ),
    bool_param(
        "dryrun",
        "Dry Run",
        Some("false"),
        false,
        "Dry run.",
        "Output",
    ),
    path_param(
        "logfile",
        "Log File",
        Some(""),
        false,
        "Log file.",
        "Output",
    ),
];

const SIMALMA_PARAMS: &[ParamSpec] = &[
    string_param(
        "project",
        "Project",
        Some("sim"),
        false,
        "Simulation project.",
        "Project",
    ),
    bool_param(
        "dryrun",
        "Dry Run",
        Some("true"),
        false,
        "Dry run.",
        "Project",
    ),
    path_param(
        "skymodel",
        "Sky Model",
        Some(""),
        false,
        "Sky model image.",
        "Input",
    ),
    string_param(
        "inbright",
        "Input Brightness",
        Some(""),
        false,
        "Input brightness.",
        "Input",
    ),
    string_param(
        "indirection",
        "Input Direction",
        Some(""),
        false,
        "Input direction.",
        "Input",
    ),
    string_param(
        "incell",
        "Input Cell",
        Some(""),
        false,
        "Input cell.",
        "Input",
    ),
    string_param(
        "incenter",
        "Input Center",
        Some(""),
        false,
        "Input center.",
        "Input",
    ),
    string_param(
        "inwidth",
        "Input Width",
        Some(""),
        false,
        "Input width.",
        "Input",
    ),
    path_param(
        "complist",
        "Component List",
        Some(""),
        false,
        "Component list.",
        "Input",
    ),
    string_param(
        "compwidth",
        "Component Width",
        Some("\"8GHz\""),
        false,
        "Component width.",
        "Input",
    ),
    bool_param(
        "setpointings",
        "Set Pointings",
        Some("true"),
        false,
        "Generate pointings.",
        "Pointing",
    ),
    path_param(
        "ptgfile",
        "Pointing File",
        Some("$project.ptg.txt"),
        false,
        "Pointing file.",
        "Pointing",
    ),
    string_param(
        "integration",
        "Integration",
        Some("10s"),
        false,
        "Integration time.",
        "Observation",
    ),
    string_param(
        "direction",
        "Direction",
        Some("[]"),
        false,
        "Pointing directions.",
        "Observation",
    ),
    string_param(
        "mapsize",
        "Map Size",
        Some("['', '']"),
        false,
        "Map size.",
        "Observation",
    ),
    string_param(
        "antennalist",
        "Antenna List",
        Some("['alma.cycle1.1.cfg', 'aca.cycle1.cfg']"),
        false,
        "Antenna config list.",
        "Observation",
    ),
    string_param(
        "correlator",
        "Correlator",
        Some("['BLC', 'BLC', 'ACASpec']"),
        false,
        "Correlator modes.",
        "Observation",
    ),
    string_param(
        "hourangle",
        "Hour Angle",
        Some("transit"),
        false,
        "Hour angle.",
        "Observation",
    ),
    string_param(
        "totaltime",
        "Total Time",
        Some("['20min', '1h']"),
        false,
        "Total observing time.",
        "Observation",
    ),
    string_param(
        "tpnant",
        "TP Antennas",
        Some("0"),
        false,
        "Total-power antennas.",
        "Observation",
    ),
    string_param(
        "tptime",
        "TP Time",
        Some("0s"),
        false,
        "Total-power time.",
        "Observation",
    ),
    float_param(
        "pwv",
        "PWV",
        Some("0.5"),
        false,
        "Precipitable water vapor.",
        "Observation",
    ),
    bool_param(
        "image",
        "Image",
        Some("true"),
        false,
        "Run imaging.",
        "Image",
    ),
    string_param(
        "imsize",
        "Image Size",
        Some("[128, 128]"),
        false,
        "Image size.",
        "Image",
    ),
    string_param(
        "imdirection",
        "Image Direction",
        Some(""),
        false,
        "Image direction.",
        "Image",
    ),
    string_param("cell", "Cell", Some(""), false, "Cell size.", "Image"),
    string_param(
        "niter",
        "Iterations",
        Some("0"),
        false,
        "Clean iterations.",
        "Image",
    ),
    string_param(
        "threshold",
        "Threshold",
        Some("0.1mJy"),
        false,
        "Clean threshold.",
        "Image",
    ),
    choice_param(
        "graphics",
        "Graphics",
        Some("both"),
        &["both", "file", "screen", "none"],
        false,
        "Graphics output.",
        "Output",
    ),
    bool_param(
        "verbose",
        "Verbose",
        Some("false"),
        false,
        "Verbose logging.",
        "Output",
    ),
    bool_param(
        "overwrite",
        "Overwrite",
        Some("false"),
        false,
        "Overwrite outputs.",
        "Output",
    ),
];

const TASKS: &[TaskSpec] = &[
    task(
        "plotcal",
        "PlotCal",
        "Plotting",
        "Plot calibration tables and corrected-data diagnostics.",
        MutationClass::ReadOnly,
        PLOTCAL_PARAMS,
    ),
    task(
        "imcollapse",
        "Image Collapse",
        "Images",
        "Collapse image axes using CASA imcollapse.",
        MutationClass::WritesProducts,
        IMCOLLAPSE_PARAMS,
    ),
    task(
        "imfit",
        "Image Fit",
        "Images",
        "Fit Gaussian components using CASA imfit.",
        MutationClass::WritesProducts,
        IMFIT_PARAMS,
    ),
    task(
        "impbcor",
        "Primary Beam Correction",
        "Images",
        "Apply primary-beam correction using CASA impbcor.",
        MutationClass::WritesProducts,
        IMPBCOR_PARAMS,
    ),
    task(
        "widebandpbcor",
        "Wideband PB Correction",
        "Images",
        "Apply wideband primary-beam correction using CASA widebandpbcor.",
        MutationClass::WritesProducts,
        WIDEBANDPBCOR_PARAMS,
    ),
    task(
        "concat",
        "Concat",
        "MeasurementSet",
        "Concatenate MeasurementSets using CASA concat.",
        MutationClass::WritesProducts,
        CONCAT_PARAMS,
    ),
    task(
        "statwt",
        "StatWT",
        "MeasurementSet",
        "Compute and write visibility weights using CASA statwt.",
        MutationClass::MutatesInput,
        STATWT_PARAMS,
    ),
    task(
        "hanningsmooth",
        "Hanning Smooth",
        "MeasurementSet",
        "Hanning smooth channel data using CASA hanningsmooth.",
        MutationClass::WritesProducts,
        HANNINGSMOOTH_PARAMS,
    ),
    task(
        "clearcal",
        "Clearcal",
        "Calibration",
        "Reset calibration columns using CASA clearcal.",
        MutationClass::MutatesInput,
        CLEARCAL_PARAMS,
    ),
    task(
        "delmod",
        "Delmod",
        "Calibration",
        "Delete MeasurementSet model data using CASA delmod.",
        MutationClass::MutatesInput,
        DELMOD_PARAMS,
    ),
    task(
        "ft",
        "FT",
        "Calibration",
        "Insert model visibilities using CASA ft.",
        MutationClass::MutatesInput,
        FT_PARAMS,
    ),
    task(
        "imcontsub",
        "Image Continuum Subtraction",
        "Images",
        "Subtract image-cube continuum using CASA imcontsub.",
        MutationClass::WritesProducts,
        IMCONTSUB_PARAMS,
    ),
    task(
        "simanalyze",
        "SimAnalyze",
        "Simulation",
        "Image and analyze simulated observations using CASA simanalyze.",
        MutationClass::WritesProducts,
        SIMANALYZE_PARAMS,
    ),
    task(
        "simalma",
        "SimALMA",
        "Simulation",
        "Run ALMA simulation workflows using CASA simalma.",
        MutationClass::WritesProducts,
        SIMALMA_PARAMS,
    ),
];

const fn task(
    id: &'static str,
    display_name: &'static str,
    category: &'static str,
    summary: &'static str,
    mutation_class: MutationClass,
    params: &'static [ParamSpec],
) -> TaskSpec {
    TaskSpec {
        id,
        display_name,
        category,
        summary,
        mutation_class,
        params,
    }
}

const fn path_param(
    id: &'static str,
    label: &'static str,
    default: Option<&'static str>,
    required: bool,
    help: &'static str,
    group: &'static str,
) -> ParamSpec {
    ParamSpec {
        id,
        label,
        value_kind: UiValueKind::Path,
        default,
        required,
        choices: &[],
        help,
        group,
    }
}

const fn string_param(
    id: &'static str,
    label: &'static str,
    default: Option<&'static str>,
    required: bool,
    help: &'static str,
    group: &'static str,
) -> ParamSpec {
    ParamSpec {
        id,
        label,
        value_kind: UiValueKind::String,
        default,
        required,
        choices: &[],
        help,
        group,
    }
}

const fn float_param(
    id: &'static str,
    label: &'static str,
    default: Option<&'static str>,
    required: bool,
    help: &'static str,
    group: &'static str,
) -> ParamSpec {
    ParamSpec {
        id,
        label,
        value_kind: UiValueKind::Float,
        default,
        required,
        choices: &[],
        help,
        group,
    }
}

const fn bool_param(
    id: &'static str,
    label: &'static str,
    default: Option<&'static str>,
    required: bool,
    help: &'static str,
    group: &'static str,
) -> ParamSpec {
    ParamSpec {
        id,
        label,
        value_kind: UiValueKind::Bool,
        default,
        required,
        choices: &[],
        help,
        group,
    }
}

const fn choice_param(
    id: &'static str,
    label: &'static str,
    default: Option<&'static str>,
    choices: &'static [&'static str],
    required: bool,
    help: &'static str,
    group: &'static str,
) -> ParamSpec {
    ParamSpec {
        id,
        label,
        value_kind: UiValueKind::Choice,
        default,
        required,
        choices,
        help,
        group,
    }
}
