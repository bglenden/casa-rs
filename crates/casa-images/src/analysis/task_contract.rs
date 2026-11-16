// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image-analysis provider contract, shared by the build and runtime.

use super::schema::{ImageAnalysisTaskRequest, ImageAnalysisTaskResult};
use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use schemars::schema_for;

/// Stable protocol name advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_NAME: &str = "casa_image_analysis_task";
/// Stable protocol version advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION: u32 = 1;

pub(crate) const IMAGE_ANALYSIS_TASK_SURFACES: [&str; 11] = [
    "imhead",
    "imstat",
    "immoments",
    "impv",
    "imsubimage",
    "immath",
    "impbcor",
    "imregrid",
    "feather",
    "exportfits",
    "importfits",
];

/// Build the current image-analysis protocol descriptor.
pub fn image_analysis_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        IMAGE_ANALYSIS_TASK_PROTOCOL_NAME,
        IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// Build the current request/result schema bundle.
pub fn image_analysis_task_schema_bundle() -> TaskProviderContract {
    try_image_analysis_task_schema_bundle().unwrap_or_else(|error| panic!("{error}"))
}

pub(super) fn try_image_analysis_task_schema_bundle() -> Result<TaskProviderContract, String> {
    let request_schema = schema_for!(ImageAnalysisTaskRequest);
    let result_schema = schema_for!(ImageAnalysisTaskResult);
    Ok(TaskProviderContract {
        protocol: image_analysis_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: vec![
                TaskOperationDescriptor {
                    name: "imhead".to_string(),
                    request_kind: "imhead".to_string(),
                    result_kind: Some("imhead".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imstat".to_string(),
                    request_kind: "imstat".to_string(),
                    result_kind: Some("imstat".to_string()),
                },
                TaskOperationDescriptor {
                    name: "immoments".to_string(),
                    request_kind: "immoments".to_string(),
                    result_kind: Some("immoments".to_string()),
                },
                TaskOperationDescriptor {
                    name: "impv".to_string(),
                    request_kind: "impv".to_string(),
                    result_kind: Some("impv".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imsubimage".to_string(),
                    request_kind: "imsubimage".to_string(),
                    result_kind: Some("imsubimage".to_string()),
                },
                TaskOperationDescriptor {
                    name: "immath".to_string(),
                    request_kind: "immath".to_string(),
                    result_kind: Some("immath".to_string()),
                },
                TaskOperationDescriptor {
                    name: "impbcor".to_string(),
                    request_kind: "impbcor".to_string(),
                    result_kind: Some("impbcor".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imregrid".to_string(),
                    request_kind: "imregrid".to_string(),
                    result_kind: Some("imregrid".to_string()),
                },
                TaskOperationDescriptor {
                    name: "feather".to_string(),
                    request_kind: "feather".to_string(),
                    result_kind: Some("feather".to_string()),
                },
                TaskOperationDescriptor {
                    name: "exportfits".to_string(),
                    request_kind: "exportfits".to_string(),
                    result_kind: Some("exportfits".to_string()),
                },
                TaskOperationDescriptor {
                    name: "importfits".to_string(),
                    request_kind: "importfits".to_string(),
                    result_kind: Some("importfits".to_string()),
                },
            ],
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
            python: Some(serde_json::json!({
                "module": "casars.tasks.image_analysis",
                "functions": [
                    "imhead",
                    "imstat",
                    "immoments",
                    "impv",
                    "imsubimage",
                    "immath",
                    "impbcor",
                    "imregrid",
                    "feather",
                    "exportfits",
                    "importfits"
                ],
            })),
        },
        parameter_surfaces: IMAGE_ANALYSIS_TASK_SURFACES
            .into_iter()
            .map(|surface| {
                builtin_surface_bundle(surface).map_err(|error| {
                    format!("built-in image-analysis parameter surface {surface:?}: {error}")
                })
            })
            .collect::<Result<_, _>>()?,
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas::default(),
        },
    })
}
