// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-plotcal` - native calibration-table and corrected-data plots.
//!
//! Projects the canonical `plotcal` parameter surface onto
//! [`casa_calibration::build_calibration_plot_payload`].

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

use casa_calibration::{
    CalibrationPlotPreset, CalibrationPlotRequest, build_calibration_plot_payload,
};
use casa_ms::MsSelection;
use casa_ms::presentation::UiCommandSchema;
use casa_provider_contracts::{
    DefaultSpec, NoAdditionalProviderSchemas, ParameterType, Predicate, ProviderCliMachineActions,
    ProviderCliProjection, ProviderProjectionMetadata, ProviderProtocolDescriptor,
    ProviderSurfaceKind, SurfaceContractBundle, TaskOperationDescriptor, TaskProviderContract,
    TaskProviderSchemas, TaskSemanticContract, builtin_surface_bundle, merged_components,
    project_ui_form,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

const SURFACE_ID: &str = "plotcal";

fn main() {
    if let Err(error) = run(env::args_os().skip(1).collect()) {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run(args: Vec<OsString>) -> Result<(), String> {
    let bundle = plotcal_surface()?;

    if has_flag(&args, "-h") || has_flag(&args, "--help") {
        print!(
            "{}\n\n{}\n",
            command_schema(&bundle).render_help().trim_end(),
            casa_task_runtime::task_cli_machine_help("PlotcalTaskRequest")
        );
        return Ok(());
    }
    let host = casa_task_runtime::TaskCliHost::new(plotcal_task_schema_bundle(&bundle), execute);
    if let Some(output) = host.dispatch(&args).map_err(|error| error.to_string())? {
        print!("{output}");
        return Ok(());
    }

    let values = parse_values(&bundle, &args)?;
    let result = execute(PlotcalTaskRequest { values })?;
    print!(
        "{}",
        serde_json::to_string_pretty(&result.output).map_err(|error| error.to_string())?
    );
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct PlotcalTaskRequest {
    values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct PlotcalTaskResult {
    task: String,
    output: JsonValue,
}

fn plotcal_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        "casars_plotcal",
        1,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

fn plotcal_task_schema_bundle(bundle: &SurfaceContractBundle) -> TaskProviderContract {
    let request_schema = schema_for!(PlotcalTaskRequest);
    let result_schema = schema_for!(PlotcalTaskResult);
    TaskProviderContract {
        protocol: plotcal_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: vec![TaskOperationDescriptor {
                name: "run".to_string(),
                request_kind: "run".to_string(),
                result_kind: Some("run".to_string()),
            }],
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: json!({ "backend": "casa-rs" }),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                    session: None,
                },
            }),
            python: None,
        },
        parameter_surfaces: vec![bundle.clone()],
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

fn execute(request: PlotcalTaskRequest) -> Result<PlotcalTaskResult, String> {
    Ok(PlotcalTaskResult {
        task: SURFACE_ID.to_string(),
        output: run_plotcal(request.values)?,
    })
}

fn plotcal_surface() -> Result<SurfaceContractBundle, String> {
    builtin_surface_bundle(SURFACE_ID)
        .map_err(|error| format!("load {SURFACE_ID} parameter surface: {error}"))
}

fn has_flag(args: &[OsString], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn command_schema(bundle: &SurfaceContractBundle) -> UiCommandSchema {
    let mut schema: UiCommandSchema = serde_json::from_value(project_ui_form(bundle))
        .expect("canonical plotcal UI projection must match UiCommandSchema");
    schema.usage = format!("{} [parameters]", schema.invocation_name);
    schema
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
        if matches!(raw, "--json-schema" | "--protocol-info" | "-h" | "--help") {
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
                    .expect("validated plotcal concept")
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

fn run_plotcal(values: BTreeMap<String, String>) -> Result<JsonValue, String> {
    let preset = values
        .get("preset")
        .map(String::as_str)
        .unwrap_or("gain_phase_vs_time");
    let preset = parse_plotcal_preset(preset)?;
    let request = CalibrationPlotRequest {
        measurement_set_path: optional_path(&values, "vis"),
        calibration_table_path: optional_path(&values, "caltable"),
        selection: MsSelection {
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
            data_description: None,
            state: None,
            msselect: optional_string(&values, "msselect"),
        },
    };
    let payload = build_calibration_plot_payload(&request, preset)
        .map_err(|error| format!("plotcal failed: {error}"))?;
    Ok(json!({
        "task": "plotcal",
        "preset": format!("{preset:?}"),
        "payload_debug": format!("{payload:#?}"),
    }))
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
    use casa_provider_contracts::SurfaceContractBundle;

    use super::*;

    #[test]
    fn schema_bundle_embeds_the_plotcal_parameter_contract() {
        let contract = plotcal_surface().expect("plotcal surface");
        assert_eq!(
            contract.surface.execution().invocation_name,
            "casars-plotcal"
        );
        let typed_bundle = plotcal_task_schema_bundle(&contract);
        typed_bundle
            .validate()
            .expect("valid plotcal provider contract");
        let bundle = serde_json::to_value(&typed_bundle).expect("serialize plotcal schema bundle");

        assert_eq!(bundle["protocol"]["protocol_name"], "casars_plotcal");
        assert!(bundle["request_schema"].is_object());
        assert!(bundle["result_schema"].is_object());

        let surfaces = serde_json::from_value::<Vec<SurfaceContractBundle>>(
            bundle["parameter_surfaces"].clone(),
        )
        .expect("serialized plotcal parameter surface");
        assert_eq!(surfaces.len(), 1);
        assert_eq!(surfaces[0].surface.id(), SURFACE_ID);
        surfaces[0]
            .validate()
            .expect("embedded plotcal parameter surface");
    }
}
