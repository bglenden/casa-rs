// SPDX-License-Identifier: LGPL-3.0-or-later
//! The tsdimaging host; scientific composition lives in the application crate.
use super::{ImagerArtifact, ImagerArtifactKind, ImagingParameters, imager_task_schema_bundle};
use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskProviderContract, builtin_surface_bundle,
    merged_components,
};
use casa_imaging_application::{
    Cancel, HostResources, ResourcePolicy, RunContext, single_dish::image_from_parameters,
};
use schemars::{JsonSchema, schema_for};
use serde::Serialize;
use std::time::Instant;

/// The task's structured result, sharing the existing run/artifact wire format.
#[derive(Serialize, JsonSchema)]
pub struct TotalPowerTaskResult {
    /// Resolved CASA parameters.
    pub request: ImagingParameters,
    /// Run identity and timing.
    pub run: TotalPowerRun,
    /// Both published CASA images.
    pub artifacts: Vec<ImagerArtifact>,
}
/// Metadata for a completed total-power run.
#[derive(Serialize, JsonSchema)]
pub struct TotalPowerRun {
    /// Output stem.
    pub stem: String,
    /// Output directory.
    pub directory: String,
    /// Scientific warnings, when applicable.
    pub warnings: Vec<String>,
    /// Wall time in nanoseconds.
    pub elapsed_ns: u64,
}
/// The native task contract, with CASA's ordered 33-parameter surface.
pub fn tsdimaging_task_schema_bundle() -> TaskProviderContract {
    let mut bundle = imager_task_schema_bundle();
    bundle.protocol = ProviderProtocolDescriptor::new(
        "casa_tsdimaging_task",
        1,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    );
    let result = schema_for!(TotalPowerTaskResult);
    bundle.semantic.result_schema = result.clone();
    bundle.domain_schemas.result_schema = result;
    bundle.components = merged_components([
        &bundle.domain_schemas.request_schema,
        &bundle.domain_schemas.result_schema,
    ]);
    bundle.parameter_surfaces =
        vec![builtin_surface_bundle("tsdimaging").expect("built-in tsdimaging surface")];
    bundle
}
/// The CLI and Python task host on the shared task envelope, observing
/// `cancel`.
pub fn tsdimaging_front_end_with_cancel(
    cancel: Cancel,
) -> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "tsdimaging",
        tsdimaging_task_schema_bundle(),
        &["tsdimaging"],
        move |task| run(task, cancel.clone()),
    )
    .expect("tsdimaging contract")
}
/// The CLI and Python task host on the shared task envelope.
pub fn tsdimaging_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    tsdimaging_front_end_with_cancel(Cancel::new())
}
fn run(task: &TaskInvocation<'_>, cancel: Cancel) -> Result<TaskSuccess, TaskFailure> {
    let started = Instant::now();
    let parameters = task.plain_values();
    let context = RunContext {
        host: HostResources::detect().map_err(|e| TaskFailure::execution(e.to_string()))?,
        policy: ResourcePolicy::Balanced,
        cancel,
        summary: None,
    };
    let output = image_from_parameters(&parameters, &context)
        .map_err(|e| TaskFailure::execution(e.to_string()))?;
    let artifacts = [
        (ImagerArtifactKind::Image, &output.image),
        (ImagerArtifactKind::Weight, &output.weight),
    ]
    .into_iter()
    .map(|(kind, path)| ImagerArtifact {
        kind,
        label: path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        path: path.display().to_string(),
        exists: path.exists(),
    })
    .collect();
    let result = TotalPowerTaskResult {
        run: TotalPowerRun {
            stem: parameters["outfile"].as_str().unwrap_or_default().into(),
            directory: output
                .image
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .display()
                .to_string(),
            warnings: Vec::new(),
            elapsed_ns: started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
        },
        request: ImagingParameters(parameters),
        artifacts,
    };
    TaskSuccess::new(crate::CasaValue::None, &result)
}
