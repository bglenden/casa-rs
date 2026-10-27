// SPDX-License-Identifier: LGPL-3.0-or-later
//! simobserve's task contract over the shared provider envelope: CASA's
//! catalog-named parameters of one run in, its report and products out.

use std::collections::BTreeMap;

use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ParameterValue, ProviderCliMachineActions, ProviderCliProjection,
    ProviderInvocation, ProviderInvocationAdaptation, ProviderProjectionMetadata,
    ProviderProtocolDescriptor, ProviderSurfaceKind, RunProductKind, TaskOperationDescriptor,
    TaskProviderContract, TaskProviderSchemas, TaskSemanticContract, builtin_surface_bundle,
    merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

/// Stable protocol name advertised by `simobserve --protocol-info`.
pub const SIMOBSERVE_TASK_PROTOCOL_NAME: &str = "casa_simobserve_task";
/// Protocol version advertised by `simobserve --protocol-info`: 3 for
/// CASA's parameters at contract 5.
pub const SIMOBSERVE_TASK_PROTOCOL_VERSION: u32 = 3;

/// simobserve's protocol descriptor.
pub fn simobserve_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        SIMOBSERVE_TASK_PROTOCOL_NAME,
        SIMOBSERVE_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// simobserve's schema bundle in the shared envelope; the catalog's
/// `simobserve` surface defines the request's parameters.
pub fn simobserve_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(SimobserveTaskRequest);
    let result_schema = schema_for!(SimobserveTaskResult);
    TaskProviderContract {
        protocol: simobserve_protocol_descriptor(),
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
        annotations: serde_json::json!({}),
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
        parameter_surfaces: vec![
            builtin_surface_bundle("simobserve")
                .expect("built-in simobserve parameter surface must remain valid"),
        ],
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

/// Project one resolved parameter set into simobserve's invocation: the
/// parameters, as JSON on stdin, run with `--json-run -`.
pub fn simobserve_provider_invocation(
    values: &BTreeMap<String, ParameterValue>,
    direct_args: Vec<String>,
) -> Result<ProviderInvocationAdaptation, String> {
    let mut args = managed_output_args(direct_args)?;
    args.extend(["--json-run".to_string(), "-".to_string()]);
    let request = SimobserveTaskRequest::Run(SimobserveTaskParameters(
        values
            .iter()
            .map(|(name, value)| (name.clone(), value.to_plain_json()))
            .collect(),
    ));
    let mut stdin = serde_json::to_string(&request)
        .map_err(|error| format!("serialize the simobserve request: {error}"))?;
    stdin.push('\n');
    Ok(ProviderInvocationAdaptation {
        invocation: ProviderInvocation {
            args,
            stdin: Some(stdin),
        },
        consumed_parameters: values.keys().cloned().collect(),
    })
}

/// The `--managed-output <bool>` pair of the direct arguments; the task's
/// own parameters travel on stdin.
fn managed_output_args(args: Vec<String>) -> Result<Vec<String>, String> {
    let mut managed = Vec::new();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument != "--managed-output" {
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| "--managed-output requires its projected boolean value".to_string())?;
        if !matches!(value.as_str(), "true" | "false") {
            return Err(format!(
                "--managed-output expects true or false, found {value:?}"
            ));
        }
        managed.extend([argument, value]);
    }
    Ok(managed)
}

/// Catalog-named simobserve parameters as plain JSON: sparse in a request,
/// every parameter once resolved (the run's request echo).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct SimobserveTaskParameters(pub serde_json::Map<String, serde_json::Value>);

/// simobserve's request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum SimobserveTaskRequest {
    /// Simulate one observation; the parameters are resolved over CASA's
    /// defaults exactly as the command line's are.
    Run(SimobserveTaskParameters),
}

impl SimobserveTaskRequest {
    /// Resolve and run the request.
    pub fn execute(&self) -> Result<SimobserveTaskResult, String> {
        let Self::Run(parameters) = self;
        let (parameters, task) = crate::resolve_request(parameters)?;
        crate::run(parameters, task).map(SimobserveTaskResult::Run)
    }
}

/// simobserve's result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum SimobserveTaskResult {
    /// A completed run.
    Run(SimobserveRunTaskResult),
}

/// The result of one run: the resolved parameters, its report and its
/// products.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveRunTaskResult {
    /// The resolved parameters the run observed with.
    pub request: SimobserveTaskParameters,
    /// The run's report.
    pub run: SimobserveRunReport,
    /// The files written in the project directory, each with whether it
    /// exists after the run.
    pub artifacts: Vec<SimobserveArtifact>,
}

/// The report of one completed run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveRunReport {
    /// `<project>.<configuration>`, the stem of the files' names.
    pub stem: String,
    /// The project directory.
    pub directory: String,
    /// What the run did not do that it was asked to, such as graphics.
    pub warnings: Vec<String>,
    /// Wall time of the run.
    pub elapsed_ns: u64,
}

/// One file the run wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveArtifact {
    /// Its suffix after the stem (`ms`, `noisy.ms`, `ptg.txt`, ...).
    pub kind: String,
    /// Its file name.
    pub label: String,
    /// Its path.
    pub path: String,
    /// Whether it exists after the run.
    pub exists: bool,
    /// What it is.
    pub resource_kind: RunProductKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_values_travel_on_stdin_with_the_managed_output_flag() {
        let values = BTreeMap::from([
            ("project".to_string(), ParameterValue::String("p".into())),
            ("overwrite".to_string(), ParameterValue::Bool(true)),
        ]);
        let adaptation = simobserve_provider_invocation(
            &values,
            vec![
                "--project".into(),
                "p".into(),
                "--managed-output".into(),
                "true".into(),
            ],
        )
        .expect("an invocation");
        assert_eq!(
            adaptation.invocation.args,
            ["--managed-output", "true", "--json-run", "-"]
        );
        let request: serde_json::Value =
            serde_json::from_str(adaptation.invocation.stdin.as_deref().expect("stdin"))
                .expect("JSON");
        assert_eq!(request["kind"], "run");
        assert_eq!(request["request"]["project"], "p");
        assert_eq!(request["request"]["overwrite"], true);
        assert_eq!(adaptation.consumed_parameters.len(), 2);
    }

    #[test]
    fn the_schema_bundle_carries_the_contract_5_surface() {
        let bundle = simobserve_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, SIMOBSERVE_TASK_PROTOCOL_NAME);
        assert_eq!(bundle.parameter_surfaces[0].surface.contract_version(), 5);
    }
}
