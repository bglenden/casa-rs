// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The command-line and task surface of native CASA-RS imaging, on the
//! shared task front end (#931).
//!
//! The binary hosts CASA's `tclean` (the imager re-surfaced, #931). The
//! front end resolves tclean's parameters through the provider-contracts
//! catalog: command-line flags and a `--json-run` request become the same
//! resolved values, and those values, and nothing else, make the
//! [`ImagingRequest`] the application runs, read as CASA's modes read them
//! ([`ImagingRequest::from_tclean`]). A run's reply carries its
//! [`ImagerRunTaskResult`], and returns CASA's summary record
//! ([`tclean_record`]).

mod casa_return;
pub mod interrupt;
mod schema;
mod task_contract;

use std::{path::PathBuf, time::Instant};

use casa_imaging_application::{HostResources, ImagingRequest, RunContext, SummaryTarget, execute};
use casa_provider_contracts::SurfaceContractBundle;
use casa_task_runtime::{
    TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess, resolve_plain_values,
};
use serde_json::{Map, Value};

pub use casa_return::tclean_record;
pub use schema::command_schema;
pub use task_contract::*;

fn imager_surface() -> Result<SurfaceContractBundle, String> {
    casa_provider_contracts::builtin_surface_bundle("tclean")
}

fn workspace() -> Result<PathBuf, String> {
    std::env::current_dir().map_err(|error| format!("the working directory: {error}"))
}

/// The request `resolved`, every tclean parameter, makes. A value the
/// catalog accepts but casa-rs cannot honour in the request's modes, or that
/// the request's own read refuses (`stokes = "IX"`), is refused at its
/// parameter.
fn request_from(
    resolved: Map<String, Value>,
) -> Result<(ImagingParameters, ImagingRequest), TaskFailure> {
    let request = ImagingRequest::from_tclean(&resolved).map_err(|refusal| {
        let failure = TaskFailure::validation(refusal.message);
        if refusal.parameter.is_empty() {
            failure
        } else {
            failure.at(refusal.parameter)
        }
    })?;
    Ok((ImagingParameters(resolved), request))
}

/// Resolve `parameters`, catalog-named and sparse, over the catalog's
/// defaults: the resolved parameters and the request they make.
pub fn resolve_request(
    parameters: &ImagingParameters,
) -> Result<(ImagingParameters, ImagingRequest), String> {
    let resolved = resolve_plain_values(imager_surface()?, workspace()?, &parameters.0)
        .map_err(|error| format!("resolve tclean parameters: {error}"))?;
    request_from(resolved).map_err(|failure| failure.message)
}

/// Run `request`, resolved as `parameters`: the run summary, with the
/// parameters as its echo, is written beside the products
/// (`<imagename>.summary.json`); SIGINT cancels the run.
pub fn run(
    parameters: ImagingParameters,
    request: &ImagingRequest,
) -> Result<ImagerRunTaskResult, String> {
    let started = Instant::now();
    let mut summary = request.imagename.clone().into_os_string();
    summary.push(".summary.json");
    let context = RunContext {
        host: HostResources::detect().map_err(|error| error.to_string())?,
        policy: request.resource_policy(),
        cancel: interrupt::token().clone(),
        summary: Some(SummaryTarget {
            path: summary.into(),
            request: Value::Object(parameters.0.clone()),
        }),
    };
    let outcome = execute(request, context).map_err(|error| error.to_string())?;
    Ok(ImagerRunTaskResult::from_outcome(
        parameters,
        request,
        &outcome,
        started.elapsed(),
    ))
}

/// The front end of the `casars-imager` binary, which hosts `tclean`.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new(
        "casars-imager",
        imager_task_schema_bundle(),
        &["tclean"],
        run_task,
    )
    .expect("the imager contract carries its surface")
}

/// The resolved parameters of `task`, a tclean run, and the request they
/// make; a request they cannot make is refused.
pub fn task_request(
    task: &TaskInvocation<'_>,
) -> Result<(ImagingParameters, ImagingRequest), TaskFailure> {
    request_from(task.plain_values())
}

/// Run `task`, one resolved imager request, as the `casars-imager` binary
/// does: the execution callback of [`task_front_end`]. SIGINT cancels it:
/// the run stops at its next safe point, publishes nothing, and replies that
/// it was interrupted.
pub fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let (parameters, request) = task_request(task)?;
    // The record echoes some of tclean's values as they were given.
    let values = parameters.0.clone();
    interrupt::install().map_err(|error| {
        TaskFailure::execution(format!("install the interrupt handler: {error}"))
    })?;
    let result = run(parameters, &request).map_err(|error| {
        if interrupt::token().is_cancelled() {
            TaskFailure::interrupted(format!("{error}; nothing was published"))
        } else {
            TaskFailure::execution(error)
        }
    })?;
    let text = format!(
        "Wrote CASA-compatible products at prefix {} ({} gridded samples, {} major cycles, \
         {} reported minor iterations, {} actual components, stop={:?})\n",
        request.imagename.display(),
        result.run.gridded_samples,
        result.run.major_cycles,
        result.run.minor_iterations,
        result.run.actual_minor_iterations,
        result.run.clean_stop_reason,
    );
    let value = tclean_record(&request, &result.run, &values);
    Ok(TaskSuccess::new(value, &result)?.with_text(text))
}
