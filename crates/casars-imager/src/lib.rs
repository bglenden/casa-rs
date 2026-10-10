// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The command-line and task surface of native CASA-RS imaging.
//!
//! Both routes resolve the imager's parameters through the provider-
//! contracts catalog: command-line flags and the catalog-named JSON of
//! `--json-run` become the same resolved values, and those values, and
//! nothing else, make the [`ImagingRequest`] the application runs.

pub mod interrupt;
mod schema;
mod task_contract;

use std::{ffi::OsString, path::PathBuf, time::Instant};

use casa_imaging_application::{HostResources, ImagingRequest, RunContext, SummaryTarget, execute};
use casa_provider_contracts::SurfaceContractBundle;
use casa_task_runtime::{
    parse_parameter_cli_overrides, resolve_active_values, resolve_plain_values,
};
use serde_json::{Map, Value};

pub use schema::command_schema;
pub use task_contract::*;

fn imager_surface() -> Result<SurfaceContractBundle, String> {
    casa_provider_contracts::builtin_surface_bundle("imager")
}

fn workspace() -> Result<PathBuf, String> {
    std::env::current_dir().map_err(|error| format!("the working directory: {error}"))
}

/// The request `resolved`, every active imager parameter, makes.
fn request_from(
    resolved: Map<String, Value>,
) -> Result<(ImagingParameters, ImagingRequest), String> {
    let request = serde_json::from_value(Value::Object(resolved.clone()))
        .map_err(|error| format!("imager parameters: {error}"))?;
    Ok((ImagingParameters(resolved), request))
}

/// Resolve `parameters`, catalog-named and sparse, over the catalog's
/// defaults: the resolved parameters and the request they make.
pub fn resolve_request(
    parameters: &ImagingParameters,
) -> Result<(ImagingParameters, ImagingRequest), String> {
    let resolved = resolve_plain_values(imager_surface()?, workspace()?, &parameters.0)
        .map_err(|error| format!("resolve imager parameters: {error}"))?;
    request_from(resolved)
}

/// Resolve command-line flags over the catalog's defaults: the resolved
/// parameters and the request they make.
pub fn resolve_cli_request(
    args: &[OsString],
) -> Result<(ImagingParameters, ImagingRequest), String> {
    let bundle = imager_surface()?;
    let overrides = parse_parameter_cli_overrides(&bundle, args)?;
    let values = resolve_active_values(bundle, workspace()?, overrides)
        .map_err(|error| format!("resolve imager parameters: {error}"))?;
    request_from(
        values
            .into_iter()
            .map(|(name, value)| (name, value.to_plain_json()))
            .collect(),
    )
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

/// Run the command line: a machine action (`--json-run`, `--json-schema`,
/// `--protocol-info`), `--help`, or imaging parameter flags.
/// `--managed-output` prints the run's result as JSON.
pub fn run_with_cli_args(args: impl IntoIterator<Item = OsString>) -> Result<(), String> {
    let (managed_output, args) = take_managed_output(args.into_iter().collect());
    let host = casa_task_runtime::TaskCliHost::new(
        imager_task_schema_bundle(),
        |request: ImagerTaskRequest| request.execute(),
    );
    if let Some(output) = host.dispatch(&args).map_err(|error| error.to_string())? {
        if managed_output && args.iter().any(|argument| argument == "--json-run") {
            let ImagerTaskResult::Run(result) = serde_json::from_str(&output)
                .map_err(|error| format!("decode the imager result: {error}"))?;
            println!("{}", json(&result)?);
        } else {
            println!("{output}");
        }
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("-h" | "--help")))
    {
        println!("{}", schema::render_help("casars-imager"));
        return Ok(());
    }
    let (parameters, request) = resolve_cli_request(&args)?;
    let result = run(parameters, &request)?;
    if managed_output {
        println!("{}", json(&result)?);
    } else {
        println!(
            "Wrote CASA-compatible products at prefix {} ({} gridded samples, {} major cycles, \
             {} reported minor iterations, {} actual components, stop={:?})",
            request.imagename.display(),
            result.run.gridded_samples,
            result.run.major_cycles,
            result.run.minor_iterations,
            result.run.actual_minor_iterations,
            result.run.clean_stop_reason,
        );
    }
    Ok(())
}

fn json(result: &ImagerRunTaskResult) -> Result<String, String> {
    serde_json::to_string_pretty(result).map_err(|error| format!("serialize the result: {error}"))
}

/// Split `--managed-output [true|false]` from the other arguments.
fn take_managed_output(raw: Vec<OsString>) -> (bool, Vec<OsString>) {
    let mut managed = false;
    let mut args = Vec::with_capacity(raw.len());
    let mut raw = raw.into_iter().peekable();
    while let Some(argument) = raw.next() {
        if argument != "--managed-output" {
            args.push(argument);
            continue;
        }
        managed = match raw.peek().and_then(|value| value.to_str()) {
            Some("true") => {
                raw.next();
                true
            }
            Some("false") => {
                raw.next();
                false
            }
            _ => true,
        };
    }
    (managed, args)
}
