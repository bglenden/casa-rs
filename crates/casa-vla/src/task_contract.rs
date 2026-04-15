// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical `importvla` task request/result contracts shared by CLI, shell, and Python.

use casa_ms::ui_schema::UiCommandSchema;
use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    ArchiveSummary, ImportReport, ImportVlaOptions, cli::command_schema,
    import_archive_files_to_measurement_set_from_options, scan_disk_archive_files_from_options,
};

/// Stable protocol name advertised by `importvla --protocol-info`.
pub const IMPORTVLA_TASK_PROTOCOL_NAME: &str = "casa_importvla_task";
/// Stable protocol version advertised by `importvla --protocol-info`.
pub const IMPORTVLA_TASK_PROTOCOL_VERSION: u32 = 1;

/// Version/compatibility information for the JSON task protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImportVlaProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl ImportVlaProtocolInfo {
    /// Build the current `importvla` protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: IMPORTVLA_TASK_PROTOCOL_NAME.to_string(),
            protocol_version: IMPORTVLA_TASK_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Task,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
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

/// JSON-schema bundle for the public `importvla` task protocol.
#[derive(Debug, Clone, Serialize)]
pub struct ImportVlaTaskSchemaBundle {
    /// Compatibility descriptor for the request/result schemas.
    pub protocol: ImportVlaProtocolInfo,
    /// Canonical semantic task contract.
    pub semantic: TaskSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI and CLI consumers.
    pub projections: ProviderProjectionMetadata,
    /// JSON schema for [`ImportVlaTaskRequest`].
    pub request_schema: RootSchema,
    /// JSON schema for [`ImportVlaTaskResult`].
    pub result_schema: RootSchema,
}

impl ImportVlaTaskSchemaBundle {
    /// Build the current request/result schema bundle.
    pub fn current() -> Self {
        let request_schema = schema_for!(ImportVlaTaskRequest);
        let result_schema = schema_for!(ImportVlaTaskResult);
        let ui_schema = serde_json::to_value(command_schema("importvla"))
            .expect("serialize importvla ui schema projection");
        Self {
            protocol: ImportVlaProtocolInfo::current(),
            semantic: TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: importvla_task_operations(),
            },
            components: merged_components([&request_schema, &result_schema]),
            annotations: derived_ui_schema_annotations(),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        ui_schema: Some("--ui-schema".to_string()),
                        json_schema: Some("--json-schema".to_string()),
                        protocol_info: Some("--protocol-info".to_string()),
                        json_run: Some("--json-run <SOURCE>".to_string()),
                        session: None,
                    },
                }),
                ui_schema: Some(ui_schema),
                python: None,
            },
            request_schema,
            result_schema,
        }
    }

    /// Return the launcher/TUI compatibility view projected from the bundle.
    pub fn ui_schema_projection(&self) -> Result<UiCommandSchema, String> {
        let value = self
            .projections
            .ui_schema
            .clone()
            .ok_or_else(|| "missing ui_schema projection".to_string())?;
        serde_json::from_value(value).map_err(|error| format!("parse importvla ui schema: {error}"))
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
        let bundle = ImportVlaTaskSchemaBundle::current();
        assert_eq!(bundle.protocol.protocol_name, IMPORTVLA_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            IMPORTVLA_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 2);
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
    fn schema_bundle_projects_ui_schema() {
        let ui_schema = ImportVlaTaskSchemaBundle::current()
            .ui_schema_projection()
            .expect("ui schema projection");
        assert_eq!(ui_schema.command_id, "importvla");
        assert!(ui_schema.render_help().contains("--archivefiles"));
    }
}
