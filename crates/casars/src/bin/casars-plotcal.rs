// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-plotcal` - native calibration-table and corrected-data plots, on
//! the shared task front end.
//!
//! Reads the canonical `plotcal` parameter surface into
//! [`casa_calibration::build_calibration_plot_payload`].

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use casa_calibration::{
    CalibrationPlotPreset, CalibrationPlotRequest, build_calibration_plot_payload,
};
use casa_ms::MsSelection;
use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    SurfaceContractBundle, TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas,
    TaskSemanticContract, builtin_surface_bundle, merged_components,
};
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

const SURFACE_ID: &str = "plotcal";

fn main() -> ExitCode {
    TaskFrontEnd::new(
        "casars-plotcal",
        plotcal_task_schema_bundle(&plotcal_surface()),
        &[SURFACE_ID],
        run,
    )
    .expect("the plotcal contract carries its surface")
    .main(
        std::env::args_os().skip(1),
        CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

/// The domain request: the plotcal parameters by name.
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
            front_end: None,
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: json!({ "backend": "casa-rs" }),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
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

fn plotcal_surface() -> SurfaceContractBundle {
    builtin_surface_bundle(SURFACE_ID).expect("the built-in plotcal surface stays valid")
}

/// Run one resolved plotcal request; it returns None, and its result
/// carries the plot's description.
fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let preset = parse_plotcal_preset(&task.required_text("preset")?)
        .map_err(|error| TaskFailure::validation(error).at("preset"))?;
    let mut vis = task.paths("vis")?;
    if vis.len() > 1 {
        return Err(TaskFailure::validation("plotcal reads one MeasurementSet").at("vis"));
    }
    let text = |name: &str| task.text(name);
    let request = CalibrationPlotRequest {
        measurement_set_path: vis.pop(),
        calibration_table_path: text("caltable")?.map(PathBuf::from),
        selection: MsSelection {
            selectdata: true,
            field: text("field")?,
            spw: text("spw")?,
            timerange: text("timerange")?,
            uvrange: text("uvrange")?,
            antenna: text("antenna")?,
            scan: text("scan")?,
            correlation: text("correlation")?,
            observation: text("observation")?,
            array: text("array")?,
            intent: text("intent")?,
            feed: text("feed")?,
            data_description: None,
            state: None,
            msselect: text("msselect")?,
        },
    };
    let payload = build_calibration_plot_payload(&request, preset)
        .map_err(|error| TaskFailure::execution(format!("plotcal failed: {error}")))?;
    let output = json!({
        "task": "plotcal",
        "preset": format!("{preset:?}"),
        "payload_debug": format!("{payload:#?}"),
    });
    let rendered = serde_json::to_string_pretty(&output)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let result = PlotcalTaskResult {
        task: SURFACE_ID.to_string(),
        output,
    };
    Ok(TaskSuccess::new(&(), &result)?.with_text(rendered))
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
        let contract = plotcal_surface();
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

    #[test]
    fn every_preset_the_surface_offers_is_read() {
        let bundle = plotcal_surface();
        let preset = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| binding.name == "preset")
            .expect("preset");
        let values = preset
            .refinements
            .iter()
            .find_map(|refinement| match refinement {
                casa_provider_contracts::NarrowingConstraint::AllowedValues { values } => {
                    Some(values)
                }
                _ => None,
            })
            .expect("plotcal narrows the presets to its own");
        for value in values {
            parse_plotcal_preset(value).unwrap_or_else(|error| panic!("{error}"));
        }
    }
}
