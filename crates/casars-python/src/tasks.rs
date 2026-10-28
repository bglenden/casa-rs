// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task launches for `casars.tasks`: the application catalog's launch
//! entries, the executable a task runs, and how a finished task process
//! ended, with a successful task's declared products.

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use casa_provider_contracts::{
    CasaValue, RunProductKind, RunProductRole, builtin_application_catalog,
    builtin_surface_bundle, installed_suite_root, resolve_task_executable,
};
use casa_task_runtime::{
    TaskExit, TaskOutcome, TaskOutputValue, classify_task_exit, decode_task_completion,
};
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;
use serde::Serialize;

use crate::convert::reply_value::reply_value;
use crate::parameters::{reason_error, to_python};

/// One application-catalog entry: its identity and how to launch it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ApplicationView {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) category: String,
    pub(crate) display_name: String,
    pub(crate) executable: String,
    pub(crate) cargo_package: String,
    pub(crate) override_env: String,
    pub(crate) dataset_kinds: Vec<String>,
    pub(crate) include_in_suite: bool,
}

/// Every application in the catalog, with the surface names of its task.
pub(crate) fn applications() -> Result<Vec<ApplicationView>, String> {
    let catalog = builtin_application_catalog()?;
    catalog
        .applications
        .iter()
        .map(|application| {
            let bundle = application
                .surface_bundle()
                .map_err(|reason| format!("resolve application {}: {reason}", application.id))?;
            let kind = serde_json::to_value(application.kind)
                .map_err(|error| format!("project application kind: {error}"))?
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| {
                    format!(
                        "application {} has a non-string enum projection",
                        application.id
                    )
                })?;
            Ok(ApplicationView {
                id: application.id.clone(),
                kind,
                display_name: bundle.surface.display_name().to_string(),
                category: bundle.surface.category().to_string(),
                executable: application.launch.executable.clone(),
                cargo_package: application.launch.cargo_package.clone(),
                override_env: application.launch.override_env.clone(),
                dataset_kinds: application.dataset_kinds.clone(),
                include_in_suite: application.include_in_suite,
            })
        })
        .collect()
}

/// One product a completed task declared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ProductView {
    pub(crate) id: String,
    pub(crate) role: &'static str,
    pub(crate) resource_kind: &'static str,
    pub(crate) label: String,
    pub(crate) path: String,
    pub(crate) exists: bool,
    pub(crate) preview_path: Option<String>,
    pub(crate) preview_exists: bool,
    pub(crate) diagnostic: Option<String>,
}

/// A successful task's summary, products and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CompletionView {
    pub(crate) surface_id: String,
    pub(crate) summary: String,
    pub(crate) products: Vec<ProductView>,
    pub(crate) diagnostics: Vec<String>,
}

/// Decode one successful provider result into its contract-declared
/// products. `values` are the task's parameters as display text.
pub(crate) fn completion(
    surface_id: &str,
    stdout: &str,
    workspace: &Path,
    values: Vec<(String, String)>,
) -> Result<CompletionView, String> {
    let bundle = builtin_surface_bundle(surface_id)?;
    let values = values
        .into_iter()
        .map(|(name, value)| TaskOutputValue { name, value })
        .collect::<Vec<_>>();
    let completion = decode_task_completion(&bundle, stdout, workspace, &values)
        .map_err(|error| error.to_string())?;
    let products = completion
        .products
        .into_iter()
        .map(|product| ProductView {
            id: product.id,
            role: match product.role {
                RunProductRole::Primary => "PRIMARY",
                RunProductRole::Auxiliary => "AUXILIARY",
                RunProductRole::Preview => "PREVIEW",
            },
            resource_kind: match product.resource_kind {
                RunProductKind::MeasurementSet => "MEASUREMENT_SET",
                RunProductKind::CasaImage => "CASA_IMAGE",
                RunProductKind::CasaTable => "CASA_TABLE",
                RunProductKind::FitsImage => "FITS_IMAGE",
                RunProductKind::File => "FILE",
            },
            label: product.label,
            path: product.path.to_string_lossy().into_owned(),
            exists: product.exists,
            preview_path: product
                .preview_path
                .map(|path| path.to_string_lossy().into_owned()),
            preview_exists: product.preview_exists,
            diagnostic: product.diagnostic,
        })
        .collect();
    Ok(CompletionView {
        surface_id: completion.surface_id,
        summary: completion.summary,
        products,
        diagnostics: completion.diagnostics,
    })
}

/// The executable that runs `surface_id`: its catalog launch entry,
/// resolved against `environment` and the installed suite whose Python
/// environment is `prefix`, if it is one.
pub(crate) fn executable(
    surface_id: &str,
    environment: &HashMap<String, String>,
    prefix: &Path,
) -> Result<PathBuf, String> {
    let application = builtin_application_catalog()?
        .application(surface_id)
        .ok_or_else(|| format!("no task {surface_id:?} in the application catalog"))?;
    resolve_task_executable(
        &application.launch,
        &|name| environment.get(name).map(OsString::from),
        installed_suite_root(prefix).as_deref(),
    )
}

/// How one task process ended, as Python reports it. The statuses are
/// [`TaskOutcome`]'s, less a launch failure, which Python sees itself.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum OutcomeView {
    /// The task succeeded: CASA's return value and the declared products.
    /// The value reaches Python as CASA's own types, apart from this view.
    Succeeded {
        #[serde(skip)]
        value: CasaValue,
        completion: CompletionView,
    },
    /// The task refused its parameters before it changed anything.
    Refused {
        message: String,
        parameter: Option<String>,
    },
    /// The task started and failed.
    Failed {
        message: String,
        parameter: Option<String>,
    },
    /// The task caught an interrupt and stopped.
    Interrupted { message: String },
    /// A signal ended the task process.
    Signalled { signal: i32 },
    /// The process's reply is missing, invalid, or disagrees with its exit.
    MalformedReply {
        exit_code: Option<i32>,
        reason: String,
    },
}

/// Classify the process that ran `surface_id` from its return code (as
/// Python's `subprocess` gives it: negative for a signal) and its stdout
/// reply, and decode a success's products. `values` are the task's
/// parameters as display text. A success whose result the contract's
/// decoder refuses is an error.
pub(crate) fn outcome(
    surface_id: &str,
    returncode: i32,
    stdout: &str,
    workspace: &Path,
    values: Vec<(String, String)>,
) -> Result<OutcomeView, String> {
    let exit = if returncode < 0 {
        TaskExit {
            code: None,
            signal: Some(-returncode),
        }
    } else {
        TaskExit {
            code: Some(returncode),
            signal: None,
        }
    };
    let mismatch = |surface: String| OutcomeView::MalformedReply {
        exit_code: exit.code,
        reason: format!("the task replied for {surface:?}, not {surface_id:?}"),
    };
    Ok(match classify_task_exit(exit, stdout) {
        TaskOutcome::Succeeded { surface, .. }
        | TaskOutcome::Refused { surface, .. }
        | TaskOutcome::Failed { surface, .. }
        | TaskOutcome::Interrupted { surface, .. }
            if surface != surface_id =>
        {
            mismatch(surface)
        }
        TaskOutcome::Succeeded { value, result, .. } => {
            let result = serde_json::to_string(&result)
                .map_err(|error| format!("serialize the {surface_id} result: {error}"))?;
            OutcomeView::Succeeded {
                value,
                completion: completion(surface_id, &result, workspace, values)?,
            }
        }
        TaskOutcome::Refused {
            message, parameter, ..
        } => OutcomeView::Refused { message, parameter },
        TaskOutcome::Failed {
            message, parameter, ..
        } => OutcomeView::Failed { message, parameter },
        TaskOutcome::Interrupted { message, .. } => OutcomeView::Interrupted { message },
        TaskOutcome::Signalled { signal } => OutcomeView::Signalled { signal },
        TaskOutcome::MalformedReply { exit_code, reason } => {
            OutcomeView::MalformedReply { exit_code, reason }
        }
        TaskOutcome::LaunchFailed { message } => OutcomeView::MalformedReply {
            exit_code: exit.code,
            reason: message,
        },
    })
}

/// The application catalog's launch entries.
#[pyfunction]
pub(crate) fn application_catalog(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let applications = applications().map_err(|reason| reason_error(py, reason))?;
    to_python(py, &applications)
}

/// The executable that runs `surface_id`, found in `environ` (Python's
/// `os.environ`) and the suite of Python environment `prefix`
/// (`sys.prefix`). Raises `FileNotFoundError` naming what is missing.
#[pyfunction]
pub(crate) fn task_executable(
    py: Python<'_>,
    surface_id: &str,
    environ: HashMap<String, String>,
    prefix: PathBuf,
) -> PyResult<PathBuf> {
    py.detach(|| executable(surface_id, &environ, &prefix))
        .map_err(PyFileNotFoundError::new_err)
}

/// How a task process ended, with a success's products. Releases the GIL
/// while it checks them.
#[pyfunction]
pub(crate) fn task_outcome<'py>(
    py: Python<'py>,
    surface_id: &str,
    returncode: i32,
    stdout: &str,
    workspace: PathBuf,
    values: Vec<(String, String)>,
) -> PyResult<Bound<'py, PyAny>> {
    let outcome = py
        .detach(|| outcome(surface_id, returncode, stdout, &workspace, values))
        .map_err(|reason| reason_error(py, reason))?;
    let view = to_python(py, &outcome)?;
    if let OutcomeView::Succeeded { value, .. } = &outcome {
        view.set_item("value", reply_value(py, value)?)?;
    }
    Ok(view)
}

#[cfg(test)]
#[path = "tasks_tests.rs"]
mod tests;
