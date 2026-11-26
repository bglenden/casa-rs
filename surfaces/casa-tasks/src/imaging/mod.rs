// SPDX-License-Identifier: LGPL-3.0-or-later

//! The task surface of native CASA-RS imaging, on the shared task front end
//! (#931).
//!
//! The front end resolves `tclean` parameters through the contracts catalog:
//! command-line flags and `--json-run` requests become the same resolved
//! values, and those values, and nothing else, make the [`ImagingRequest`]
//! the application runs, read as CASA's modes read them
//! ([`ImagingRequest::from_tclean`]). A run's reply carries its
//! [`ImagerRunTaskResult`], and returns CASA's summary record
//! ([`tclean_record`]).

mod casa_return;
mod history;
pub mod single_dish;
mod task_contract;

use std::{path::PathBuf, time::Instant};

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess, resolve_plain_values};
use casa_contracts::{ParameterValue, SurfaceContractBundle};
use casa_imaging_application::{
    Cancel, HostResources, ImagingRequest, RunContext, SummaryTarget, execute,
};
use serde_json::{Map, Value};

pub use casa_return::{RunReturn, tclean_record};
pub use task_contract::*;

/// Per-invocation execution options for the `tclean` imaging task adapter.
#[derive(Clone, Debug)]
pub struct ImagingExecutionOptions {
    /// Cooperative cancellation token observed by the imaging run.
    pub cancel: Cancel,
    /// Whether to write `<imagename>.summary.json` beside the products.
    pub write_run_summary: bool,
    /// Optional path to the EVLA dish surface table used by AWP2.
    pub evla_surface: Option<PathBuf>,
}

impl ImagingExecutionOptions {
    /// Read CLI environment configuration (`CASA_RS_IMAGER_RUN_SUMMARY` and
    /// `HOME`) with a fresh cancellation token.
    pub fn from_env() -> Self {
        Self {
            cancel: Cancel::new(),
            write_run_summary: run_summary_requested(),
            evla_surface: default_evla_surface(),
        }
    }
}

impl Default for ImagingExecutionOptions {
    fn default() -> Self {
        Self {
            cancel: Cancel::new(),
            write_run_summary: false,
            evla_surface: default_evla_surface(),
        }
    }
}

fn default_evla_surface() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".casa/data/nrao/VLA/VLA.surface"))
}

fn imager_surface() -> Result<SurfaceContractBundle, String> {
    casa_contracts::builtin_surface_bundle("tclean")
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
    evla_surface_fallback: Option<&PathBuf>,
) -> Result<(ImagingParameters, ImagingRequest), TaskFailure> {
    let mut request = ImagingRequest::from_tclean(&resolved).map_err(|refusal| {
        let failure = TaskFailure::validation(refusal.message);
        if refusal.parameter.is_empty() {
            failure
        } else {
            failure.at(refusal.parameter)
        }
    })?;
    // AWP2 reads the same physical dish data as CASA when the host has
    // the local data tree. The imaging library takes an explicit path.
    if let casa_imaging_application::Gridder::Awp2 { evla_surface, .. } = &mut request.gridder
        && evla_surface.is_none()
    {
        *evla_surface = evla_surface_fallback.cloned();
    }
    Ok((ImagingParameters(resolved), request))
}

/// Resolve `parameters`, catalog-named and sparse, over the catalog's
/// defaults: the resolved parameters and the request they make.
pub fn resolve_request(
    parameters: &ImagingParameters,
) -> Result<(ImagingParameters, ImagingRequest), String> {
    let resolved = resolve_plain_values(imager_surface()?, workspace()?, &parameters.0)
        .map_err(|error| format!("resolve tclean parameters: {error}"))?;
    let default_surface = default_evla_surface();
    request_from(resolved, default_surface.as_ref()).map_err(|failure| failure.message)
}

/// The environment variable that asks the `casars-imager` binary for its
/// run summary. CASA's `tclean` writes nothing beside its image products, so
/// neither does a tclean run here (owner decision of 2026-11-02, #980); set
/// to `1`, the binary also writes `<imagename>.summary.json`, the run's
/// status, resolved parameters, backend and each phase's time and memory,
/// for performance tooling ([`casa_imaging_application::RunSummary`]).
pub const RUN_SUMMARY_VARIABLE: &str = "CASA_RS_IMAGER_RUN_SUMMARY";

/// Whether this process was asked for run summaries
/// ([`RUN_SUMMARY_VARIABLE`]).
fn run_summary_requested() -> bool {
    std::env::var_os(RUN_SUMMARY_VARIABLE).is_some_and(|value| value == "1")
}

/// Run `request`, resolved as `parameters`, as CASA's tclean does: it
/// writes the image products and nothing else beside them.
pub fn run(
    parameters: ImagingParameters,
    request: &ImagingRequest,
) -> Result<ImagerRunTaskResult, String> {
    run_with_options(parameters, request, &ImagingExecutionOptions::default())
        .map(|(result, _)| result)
}

/// Keep what the CASA return needs beyond the report in memory, without
/// adding fields to the serialized task report or the run summary, which is
/// written beside the products only when `options.write_run_summary` asks
/// for it.
fn run_with_options(
    parameters: ImagingParameters,
    request: &ImagingRequest,
    options: &ImagingExecutionOptions,
) -> Result<(ImagerRunTaskResult, RunReturn), String> {
    // Task continuation is distinct from internal atomic output replacement.
    let restart = parameters
        .0
        .get("restart")
        .and_then(Value::as_bool)
        .ok_or_else(|| "resolved tclean restart must be boolean".to_owned())?;
    let request = request
        .with_tclean_output_policy(restart)
        .map_err(|error| error.to_string())?;
    let request = &request;
    let started = Instant::now();
    let summary = options.write_run_summary.then(|| {
        let mut path = request.imagename.clone().into_os_string();
        path.push(".summary.json");
        SummaryTarget {
            path: path.into(),
            request: Value::Object(parameters.0.clone()),
        }
    });
    let context = RunContext {
        host: HostResources::detect().map_err(|error| error.to_string())?,
        policy: request.resource_policy(),
        cancel: options.cancel.clone(),
        summary,
    };
    let outcome = execute(request, context).map_err(|error| error.to_string())?;
    let result =
        ImagerRunTaskResult::from_outcome(parameters, request, &outcome, started.elapsed());
    Ok((
        result,
        RunReturn {
            statistics: outcome.final_residual_statistics,
            plane_rows: outcome.plane_rows,
        },
    ))
}

/// The front end of the `casars-imager` binary, which hosts `tclean`, with
/// caller-provided execution options and cancellation token.
pub fn task_front_end_with_options(
    options: ImagingExecutionOptions,
) -> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "casars-imager",
        imager_task_schema_bundle(),
        &["tclean"],
        move |task| run_task_with_options(task, &options),
    )
    .expect("the imager contract carries its surface")
}

/// The front end of the `casars-imager` binary, using default per-invocation
/// execution options.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    task_front_end_with_options(ImagingExecutionOptions::from_env())
}

/// The resolved parameters of `task`, a tclean run, and the request they
/// make; a request they cannot make is refused.
pub fn task_request(
    task: &TaskInvocation<'_>,
) -> Result<(ImagingParameters, ImagingRequest), TaskFailure> {
    let default_surface = default_evla_surface();
    task_request_with_surface(task, default_surface.as_ref())
}

fn task_request_with_surface(
    task: &TaskInvocation<'_>,
    evla_surface: Option<&PathBuf>,
) -> Result<(ImagingParameters, ImagingRequest), TaskFailure> {
    let raw = task.plain_values();
    if task.native_options().is_none() {
        return request_from(raw, evla_surface);
    }
    let mut resolved = raw.clone();
    task.resolve_file_values(
        &mut resolved,
        &[
            "vis",
            "imagename",
            "outlierfile",
            "startmodel",
            "vptable",
            "cfcache",
            "native_cf_cache",
            "evla_surface",
        ],
    );
    for name in ["mask", "phasecenter"] {
        if let Some(value) = resolved.get_mut(name) {
            let rebase = |value: &mut Value| {
                if let Value::String(text) = value
                    && !text.is_empty()
                {
                    let path = task.io_path(text.as_str());
                    if path.exists() || (name == "mask" && !text.contains(['[', ',', ';'])) {
                        *text = path.to_string_lossy().into_owned();
                    }
                }
            };
            match value {
                Value::Array(items) => items.iter_mut().for_each(rebase),
                _ => rebase(value),
            }
        }
    }
    let (_, request) = request_from(resolved, evla_surface)?;
    Ok((
        ImagingParameters(raw),
        request.with_task_workspace(task.workspace()),
    ))
}

/// Run `task`, one resolved imager request, with default execution options.
pub fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    run_task_with_options(task, &ImagingExecutionOptions::from_env())
}

/// Run `task`, one resolved imager request, observing `options.cancel`: if
/// cancelled, the run stops at its next safe point, publishes nothing, and
/// replies that it was interrupted.
pub fn run_task_with_options(
    task: &TaskInvocation<'_>,
    options: &ImagingExecutionOptions,
) -> Result<TaskSuccess, TaskFailure> {
    let (parameters, request) = task_request_with_surface(task, options.evla_surface.as_ref())?;
    // The record echoes some of tclean's values: a dirty run's threshold as
    // the caller gave it, a number as a number and text as text, and
    // tclean's default 0.0 where it was not given.
    let mut values = parameters.0.clone();
    values.insert(
        "threshold".to_string(),
        task.session()
            .given("threshold")
            .map_or(Value::from(0.0), ParameterValue::to_plain_json),
    );
    let (result, held) = run_with_options(parameters, &request, options).map_err(|error| {
        if options.cancel.is_cancelled() {
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
    let value = tclean_record(&request, &result.run, &values, &held);
    history::write_tclean_history(&request.imagename, task);
    Ok(TaskSuccess::new(value, &result)?.with_text(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_application::{default_tclean_parameters, single_dish};
    use serde_json::json;

    #[test]
    fn native_aw_paths_use_the_workspace() {
        use casa_imaging_application::{AwCfSource, Gridder};

        let workspace = tempfile::tempdir().unwrap();
        let bundle = casa_contracts::builtin_surface_bundle("tclean").unwrap();
        let mut session = crate::ParameterSession::defaults(bundle).unwrap();
        let overrides = json!({
            "vis": ["input.ms"],
            "imagename": "output",
            "cell": "1arcsec",
            "gridder": "awproject",
            "conjbeams": true,
            "wprojplanes": 32,
            "aw_cf_source": "native-evla",
            "native_cf_cache": "cache",
            "evla_surface": "surface",
            "native_cf_working_size": 64,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 1_048_576,
            "native_cf_maximum_cells": 64,
        });
        for (key, value) in overrides.as_object().unwrap() {
            session
                .set(
                    key,
                    casa_contracts::ParameterValue::from_plain_json(value).unwrap(),
                )
                .unwrap();
        }
        let names = std::collections::BTreeMap::new();
        let native = crate::NativeTaskOptions::default();
        let invocation = TaskInvocation::in_process(&session, workspace.path(), &names, &native);
        let (_, request) = task_request_with_surface(&invocation, None).unwrap();
        let Gridder::Awproject(aw) = request.gridder else {
            panic!("expected awproject");
        };
        let AwCfSource::NativeEvla {
            native_cf_cache,
            evla_surface,
            ..
        } = aw.cf_source
        else {
            panic!("expected native EVLA convolution functions");
        };
        assert_eq!(native_cf_cache, workspace.path().join("cache"));
        assert_eq!(evla_surface, workspace.path().join("surface"));
    }

    #[test]
    fn tclean_domain_defaults_match_catalog_resolution() {
        let overrides = json!({
            "vis": ["unused.ms"],
            "imagename": "unused",
            "cell": ["1arcsec"],
        });
        let overrides = overrides.as_object().expect("object");
        let catalog_defaults = resolve_plain_values(
            casa_contracts::builtin_surface_bundle("tclean").expect("tclean surface"),
            PathBuf::from("."),
            overrides,
        )
        .expect("tclean defaults resolve");
        let mut expected = default_tclean_parameters();
        for (key, value) in overrides {
            expected.insert(key.clone(), value.clone());
        }
        assert_eq!(expected, catalog_defaults);
    }

    #[test]
    fn tsdimaging_domain_defaults_match_catalog_resolution() {
        let catalog_defaults = resolve_plain_values(
            casa_contracts::builtin_surface_bundle("tsdimaging").expect("tsdimaging surface"),
            PathBuf::from("."),
            &Map::new(),
        )
        .expect("tsdimaging defaults resolve");
        assert_eq!(single_dish::default_parameters(), catalog_defaults);
    }

    #[test]
    fn tclean_catalog_refuses_overlong_imsize() {
        let overrides = json!({
            "vis": "unused.ms",
            "imagename": "unused",
            "cell": "1arcsec",
            "imsize": [64, 32, 16],
        });
        assert!(
            resolve_plain_values(
                casa_contracts::builtin_surface_bundle("tclean").expect("tclean surface"),
                PathBuf::from("."),
                overrides.as_object().expect("object"),
            )
            .is_err()
        );
    }
}
