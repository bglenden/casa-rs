// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical `importvla` task request/result contracts shared by CLI, shell, and Python.

use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::{
    ArchiveSummary, ImportReport, ImportVlaOptions,
    import_archive_files_to_measurement_set_from_options, scan_disk_archive_files_from_options,
};

/// Stable protocol name advertised by `importvla --protocol-info`.
pub const IMPORTVLA_TASK_PROTOCOL_NAME: &str = "casa_importvla_task";
/// Stable protocol version advertised by `importvla --protocol-info`.
pub const IMPORTVLA_TASK_PROTOCOL_VERSION: u32 = 1;

/// Build the current shared `importvla` protocol descriptor.
pub fn importvla_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        IMPORTVLA_TASK_PROTOCOL_NAME,
        IMPORTVLA_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// Request for scanning VLA export archives without writing an MS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImportVlaScanTaskRequest {
    /// Task-style scan options.
    pub options: ImportVlaOptions,
}

/// Request for importing VLA export archives into a MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImportVlaImportTaskRequest {
    /// Task-style import options.
    pub options: ImportVlaOptions,
}

/// Canonical `importvla` task request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ImportVlaTaskRequest {
    /// Scan one or more disk archives.
    Scan(ImportVlaScanTaskRequest),
    /// Import one or more disk archives into an MS.
    Import(ImportVlaImportTaskRequest),
}

impl ImportVlaTaskRequest {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<ImportVlaTaskResult, String> {
        match self {
            Self::Scan(request) => scan_disk_archive_files_from_options(&request.options)
                .map(ImportVlaTaskResult::Scan)
                .map_err(|error| error.to_string()),
            Self::Import(request) => {
                import_archive_files_to_measurement_set_from_options(&request.options)
                    .map(ImportVlaTaskResult::Import)
                    .map_err(|error| error.to_string())
            }
        }
    }
}

/// Canonical `importvla` task result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ImportVlaTaskResult {
    /// Archive scan summary.
    Scan(ArchiveSummary),
    /// Import execution report.
    Import(ImportReport),
}

/// Build the current request/result schema bundle with the shared envelope.
pub fn importvla_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(ImportVlaTaskRequest);
    let result_schema = schema_for!(ImportVlaTaskResult);
    TaskProviderContract {
        protocol: importvla_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: importvla_task_operations(),
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: serde_json::json!({}),
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
        parameter_surfaces: vec![
            builtin_surface_bundle("importvla")
                .expect("built-in importvla parameter surface must remain valid"),
        ],
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

fn importvla_task_operations() -> Vec<TaskOperationDescriptor> {
    vec![
        TaskOperationDescriptor {
            name: "scan".to_string(),
            request_kind: "scan".to_string(),
            result_kind: Some("scan".to_string()),
        },
        TaskOperationDescriptor {
            name: "import".to_string(),
            request_kind: "import".to_string(),
            result_kind: Some("import".to_string()),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_bundle_advertises_importvla_protocol_and_actions() {
        let bundle = importvla_task_schema_bundle();
        bundle.validate().expect("shared provider envelope");
        assert_eq!(bundle.protocol.protocol_name, IMPORTVLA_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            IMPORTVLA_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 2);
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "importvla");
        bundle.parameter_surfaces[0]
            .validate()
            .expect("embedded importvla parameter surface");
        assert_eq!(
            serde_json::to_value(&bundle).unwrap()["parameter_surfaces"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            bundle
                .projections
                .cli
                .as_ref()
                .expect("cli projection")
                .machine_actions
                .json_run
                .as_deref(),
            Some("--json-run <SOURCE>")
        );
    }

    #[test]
    fn canonical_surface_projects_presentation_form() {
        let bundle = importvla_task_schema_bundle();
        let form = casa_provider_contracts::project_ui_form(&bundle.parameter_surfaces[0]);
        assert_eq!(form["command_id"], "importvla");
        assert!(form.to_string().contains("--archivefiles"));
    }
}
