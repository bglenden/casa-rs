// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical `msexplore` task request/result contracts shared by CLI and UIs.

use std::fs;
use std::path::{Path, PathBuf};

use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use super::{
    MsExploreSpec, MsExportFormat, MsFlagEditPreview, MsFlagEditSpec, build_msexplore_payload,
    export_msexplore_plot, preview_msexplore_flag_edit_for_request,
};
use crate::{MeasurementSet, MeasurementSetSummary};

/// Stable protocol name advertised by `msexplore --protocol-info`.
pub const MSEXPLORE_TASK_PROTOCOL_NAME: &str = "casa_msexplore_task";
/// Stable protocol version advertised by `msexplore --protocol-info`.
pub const MSEXPLORE_TASK_PROTOCOL_VERSION: u32 = 1;

/// Build the current shared `msexplore` protocol descriptor.
pub fn msexplore_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        MSEXPLORE_TASK_PROTOCOL_NAME,
        MSEXPLORE_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
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
}

/// Canonical `msexplore` task result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum MsExploreTaskResult {
    /// Completed end-to-end `msexplore` run.
    Run(MsExploreRunTaskResult),
}

/// Build the current `msexplore` schema bundle with the shared envelope.
pub fn msexplore_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(MsExploreTaskRequest);
    let result_schema = schema_for!(MsExploreTaskResult);
    TaskProviderContract {
        protocol: msexplore_protocol_descriptor(),
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
        parameter_surfaces: ["msexplore", "plotms"]
            .into_iter()
            .map(|surface| {
                builtin_surface_bundle(surface).unwrap_or_else(|error| {
                    panic!("built-in MS explorer parameter surface {surface:?}: {error}")
                })
            })
            .collect(),
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
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
    use tempfile::tempdir;

    use super::{
        MSEXPLORE_TASK_PROTOCOL_NAME, MSEXPLORE_TASK_PROTOCOL_VERSION, MsExploreRunTaskRequest,
        MsExploreTaskRequest, msexplore_protocol_descriptor, msexplore_task_schema_bundle,
        write_output,
    };
    use crate::{
        MeasurementSetSummaryOutputFormat, MsExploreSpec, MsPageExportRange, MsPlotPreset,
        MsPlotSpec, MsSelectionSpec,
    };

    fn test_spec(ms_path: &str) -> MsExploreSpec {
        MsExploreSpec {
            ms_path: ms_path.into(),
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelectionSpec::default(),
            header_items: Vec::new(),
            page_title: None,
            exprange: MsPageExportRange::Current,
            max_plot_points: 100,
            plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
        }
    }

    #[test]
    fn schema_bundle_uses_current_protocol_and_projection() {
        let bundle = msexplore_task_schema_bundle();
        bundle.validate().expect("shared provider envelope");
        assert_eq!(bundle.protocol.protocol_name, MSEXPLORE_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            MSEXPLORE_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 1);
        assert_eq!(bundle.semantic.operations[0].request_kind, "run");
        assert!(bundle.components.contains_key("MsExploreRunTaskRequest"));
        assert_eq!(
            bundle
                .parameter_surfaces
                .iter()
                .map(|surface| surface.surface.id())
                .collect::<Vec<_>>(),
            ["msexplore", "plotms"]
        );
        assert!(
            bundle
                .parameter_surfaces
                .iter()
                .all(|surface| surface.validate().is_ok())
        );
        assert_eq!(
            serde_json::to_value(&bundle).unwrap()["parameter_surfaces"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let form = casa_provider_contracts::project_ui_form(&bundle.parameter_surfaces[0]);
        assert_eq!(form["command_id"], "msexplore");
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = msexplore_protocol_descriptor();
        assert_eq!(info.protocol_name, MSEXPLORE_TASK_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, MSEXPLORE_TASK_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Task);
    }

    #[test]
    fn request_and_result_envelopes_round_trip_with_stable_kind_tags() {
        let request = MsExploreTaskRequest::Run(MsExploreRunTaskRequest {
            spec: test_spec("example.ms"),
            summary_output_path: None,
            overwrite_outputs: true,
            flag_edit: None,
            plot_export: None,
        });
        let request_json = serde_json::to_string_pretty(&request).expect("serialize request");
        assert!(request_json.contains("\"kind\": \"run\""));
        assert!(request_json.contains("\"ms_path\": \"example.ms\""));
        assert_eq!(
            serde_json::from_str::<MsExploreTaskRequest>(&request_json).expect("parse request"),
            request
        );
    }

    #[test]
    fn write_output_refuses_existing_files_unless_overwrite_is_enabled() {
        let dir = tempdir().expect("tempdir");
        let output = dir.path().join("summary.txt");
        write_output(&output, false, "first").expect("first write");
        let error = write_output(&output, false, "second").expect_err("overwrite guard");
        assert!(error.contains("refusing to overwrite existing output"));
        assert_eq!(
            std::fs::read_to_string(&output).expect("read output"),
            "first"
        );

        write_output(&output, true, "second").expect("overwrite");
        assert_eq!(
            std::fs::read_to_string(&output).expect("read output"),
            "second"
        );
    }
}
