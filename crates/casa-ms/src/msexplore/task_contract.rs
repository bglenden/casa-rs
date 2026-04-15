// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical `msexplore` task request/result contracts shared by CLI and UIs.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::cli::command_schema;
use super::{
    MsExploreSpec, MsExportFormat, MsFlagEditPreview, MsFlagEditSpec, build_msexplore_payload,
    export_msexplore_plot, preview_msexplore_flag_edit_for_request,
};
use crate::{MeasurementSet, MeasurementSetSummary};

/// Stable protocol name advertised by `msexplore --protocol-info`.
pub const MSEXPLORE_TASK_PROTOCOL_NAME: &str = "casa_msexplore_task";
/// Stable protocol version advertised by `msexplore --protocol-info`.
pub const MSEXPLORE_TASK_PROTOCOL_VERSION: u32 = 1;

/// Version/compatibility information for the JSON task protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsExploreProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl MsExploreProtocolInfo {
    /// Build the current `msexplore` protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: MSEXPLORE_TASK_PROTOCOL_NAME.to_string(),
            protocol_version: MSEXPLORE_TASK_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Task,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Optional staged flag edit performed as part of one `msexplore` task request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsExploreFlagEditRequest {
    /// Staged flag edit specification.
    pub edit: MsFlagEditSpec,
    /// Apply the edit to disk instead of only previewing it.
    #[serde(default)]
    pub apply: bool,
    /// Optional preview-output path written as JSON.
    pub output_path: Option<PathBuf>,
}

/// Optional plot export performed as part of one `msexplore` task request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsExplorePlotExportRequest {
    /// Destination path for the exported plot.
    pub output_path: PathBuf,
    /// Export format.
    pub format: MsExportFormat,
    /// Raster width in pixels.
    pub width: u32,
    /// Raster height in pixels.
    pub height: u32,
}

/// One end-to-end `msexplore` task request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsExploreRunTaskRequest {
    /// Canonical `msexplore` plot/summary request.
    pub spec: MsExploreSpec,
    /// Optional summary-output path written in the requested summary format.
    pub summary_output_path: Option<PathBuf>,
    /// Permit replacement of existing output artifacts.
    #[serde(default)]
    pub overwrite_outputs: bool,
    /// Optional staged flag edit request.
    pub flag_edit: Option<MsExploreFlagEditRequest>,
    /// Optional plot export request.
    pub plot_export: Option<MsExplorePlotExportRequest>,
}

impl MsExploreRunTaskRequest {
    /// Execute the request and return structured output metadata.
    pub fn execute(&self) -> Result<MsExploreRunTaskResult, String> {
        let mut ms = MeasurementSet::open(&self.spec.ms_path).map_err(|error| {
            if self.spec.ms_path.is_dir() {
                format!(
                    "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                    self.spec.ms_path.display()
                )
            } else {
                format!(
                    "open MeasurementSet {}: {error}",
                    self.spec.ms_path.display()
                )
            }
        })?;

        let summary = MeasurementSetSummary::from_ms_with_options(
            &ms,
            &self.spec.selection.to_summary_options(),
        )
        .map_err(|error| error.to_string())?;
        if let Some(path) = self.summary_output_path.as_deref() {
            let rendered = summary
                .render(self.spec.summary_format)
                .map_err(|error| error.to_string())?;
            write_output(path, self.overwrite_outputs, &rendered)?;
        }

        let flag_edit_preview = if let Some(flag_edit) = &self.flag_edit {
            let preview = if flag_edit.apply {
                let preview = crate::apply_msexplore_flag_edit_for_request(
                    &mut ms,
                    &self.spec,
                    &flag_edit.edit,
                )?;
                ms.save().map_err(|error| error.to_string())?;
                preview
            } else {
                preview_msexplore_flag_edit_for_request(&ms, &self.spec, &flag_edit.edit)?
            };
            if let Some(path) = flag_edit.output_path.as_deref() {
                let json = serde_json::to_string_pretty(&preview)
                    .map_err(|error| format!("serialize flag preview: {error}"))?;
                write_output(path, self.overwrite_outputs, &json)?;
            }
            Some(preview)
        } else {
            None
        };

        let plot_export = if let Some(plot_export) = &self.plot_export {
            let payload = build_msexplore_payload(&ms, &self.spec)?;
            export_msexplore_plot(
                &payload,
                crate::MeasurementSetPlotTheme::light(),
                &plot_export.output_path,
                plot_export.format,
                plot_export.width,
                plot_export.height,
            )?;
            Some(MsExplorePlotArtifact {
                output_path: plot_export.output_path.clone(),
                format: plot_export.format,
                width: plot_export.width,
                height: plot_export.height,
            })
        } else {
            None
        };

        Ok(MsExploreRunTaskResult {
            summary,
            summary_output_path: self.summary_output_path.clone(),
            flag_edit_preview,
            flag_edit_output_path: self
                .flag_edit
                .as_ref()
                .and_then(|flag_edit| flag_edit.output_path.clone()),
            plot_export,
        })
    }
}

/// Metadata for one plot artifact written by `msexplore`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsExplorePlotArtifact {
    /// Destination path for the exported plot.
    pub output_path: PathBuf,
    /// Export format.
    pub format: MsExportFormat,
    /// Raster width in pixels.
    pub width: u32,
    /// Raster height in pixels.
    pub height: u32,
}

/// Structured result for one `msexplore` task execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsExploreRunTaskResult {
    /// Structured MeasurementSet summary.
    pub summary: MeasurementSetSummary,
    /// Summary artifact path when one was written.
    pub summary_output_path: Option<PathBuf>,
    /// Structured staged-flag preview when requested.
    pub flag_edit_preview: Option<MsFlagEditPreview>,
    /// Flag-preview artifact path when one was written.
    pub flag_edit_output_path: Option<PathBuf>,
    /// Plot artifact metadata when a plot was exported.
    pub plot_export: Option<MsExplorePlotArtifact>,
}

/// Canonical `msexplore` task request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum MsExploreTaskRequest {
    /// Execute one `msexplore` request.
    Run(MsExploreRunTaskRequest),
}

impl MsExploreTaskRequest {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<MsExploreTaskResult, String> {
        match self {
            Self::Run(request) => Ok(MsExploreTaskResult::Run(request.execute()?)),
        }
    }

    /// Read one task request from a file path or `-` for stdin.
    pub fn read_from_source(source: &str) -> Result<Self, String> {
        let payload = if source == "-" {
            let mut payload = String::new();
            std::io::stdin()
                .read_to_string(&mut payload)
                .map_err(|error| format!("failed to read JSON request from stdin: {error}"))?;
            payload
        } else {
            fs::read_to_string(source).map_err(|error| {
                format!(
                    "failed to read JSON request from {}: {error}",
                    Path::new(source).display()
                )
            })?
        };
        serde_json::from_str(&payload)
            .map_err(|error| format!("failed to parse msexplore task request: {error}"))
    }
}

/// Canonical `msexplore` task result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum MsExploreTaskResult {
    /// Completed end-to-end `msexplore` run.
    Run(MsExploreRunTaskResult),
}

/// JSON-schema bundle for the public `msexplore` task protocol.
#[derive(Debug, Clone, Serialize)]
pub struct MsExploreTaskSchemaBundle {
    /// Compatibility descriptor for the request/result schemas.
    pub protocol: MsExploreProtocolInfo,
    /// Canonical semantic task contract.
    pub semantic: TaskSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI and CLI consumers.
    pub projections: ProviderProjectionMetadata,
    /// JSON schema for [`MsExploreTaskRequest`].
    pub request_schema: RootSchema,
    /// JSON schema for [`MsExploreTaskResult`].
    pub result_schema: RootSchema,
}

impl MsExploreTaskSchemaBundle {
    /// Build the current request/result schema bundle.
    pub fn current() -> Self {
        let request_schema = schema_for!(MsExploreTaskRequest);
        let result_schema = schema_for!(MsExploreTaskResult);
        let ui_schema = serde_json::to_value(command_schema("msexplore"))
            .expect("serialize msexplore ui schema projection");
        Self {
            protocol: MsExploreProtocolInfo::current(),
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
    pub fn ui_schema_projection(&self) -> Result<crate::ui_schema::UiCommandSchema, String> {
        let value = self
            .projections
            .ui_schema
            .clone()
            .ok_or_else(|| "missing ui_schema projection".to_string())?;
        serde_json::from_value(value).map_err(|error| format!("parse msexplore ui schema: {error}"))
    }
}

fn write_output(path: &Path, overwrite: bool, text: &str) -> Result<(), String> {
    if path.exists() && !overwrite {
        return Err(format!(
            "refusing to overwrite existing output {}; pass overwrite_outputs=true to replace it",
            path.display()
        ));
    }
    fs::write(path, text).map_err(|error| format!("write output {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::ProviderSurfaceKind;

    use super::{
        MSEXPLORE_TASK_PROTOCOL_NAME, MSEXPLORE_TASK_PROTOCOL_VERSION, MsExploreProtocolInfo,
        MsExploreTaskSchemaBundle,
    };

    #[test]
    fn schema_bundle_uses_current_protocol_and_projection() {
        let bundle = MsExploreTaskSchemaBundle::current();
        assert_eq!(bundle.protocol.protocol_name, MSEXPLORE_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            MSEXPLORE_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 1);
        assert_eq!(bundle.semantic.operations[0].request_kind, "run");
        assert!(bundle.components.contains_key("MsExploreRunTaskRequest"));
        let ui_schema = bundle.ui_schema_projection().expect("ui schema projection");
        assert_eq!(ui_schema.command_id, "msexplore");
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = MsExploreProtocolInfo::current();
        assert_eq!(info.protocol_name, MSEXPLORE_TASK_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, MSEXPLORE_TASK_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Task);
    }
}
