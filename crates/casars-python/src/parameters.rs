// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task parameters for `casars.parameters`: the catalog's dictionary view,
//! resolution of one surface's values, sparse TOML rendering, explicit and
//! managed (Last) profiles, and the provider invocation a task runs with.
//!
//! The Rust parameter runtime (`casa-task-runtime`) owns defaults,
//! validation, normalization and rendering; these functions convert Python
//! values in and dictionaries out. A failure of the runtime raises
//! `ValueError` with the message `reason='...'`, the reason being the Python
//! `repr` of the runtime's text. Calls that read or write a profile release
//! the GIL.

mod contract;
mod values;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_provider_contracts::{
    ParameterValue, SurfaceContractBundle, builtin_surface_bundle, builtin_surface_catalog,
};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ManagedProfileKind, ManagedStateStore, OpenSessionRequest,
    ParameterRuntime, ParameterSession, ResolutionPatch, parse_profile,
    project_provider_invocation, render_documented_template, write_parameter_profile_atomic,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
use serde::Serialize;

use self::contract::{BundleView, CatalogEnvelopeView, snake_case_debug};
use self::values::{parameter_values, serialize_native};

/// The `ValueError` a failed runtime call raises: `reason='...'`.
pub(crate) fn reason_error(py: Python<'_>, reason: impl AsRef<str>) -> PyErr {
    match PyString::new(py, reason.as_ref()).repr() {
        Ok(repr) => PyValueError::new_err(format!("reason={repr}")),
        Err(error) => error,
    }
}

/// Convert a Rust value to its Python dictionary view.
pub(crate) fn to_python<'py>(
    py: Python<'py>,
    value: &impl Serialize,
) -> PyResult<Bound<'py, PyAny>> {
    pythonize::pythonize(py, value).map_err(PyErr::from)
}

type ServiceResult<T> = Result<T, String>;

fn parameter_error(context: &str, error: impl std::fmt::Display) -> String {
    format!("{context}: {error}")
}

fn parameter_bundle(surface_id: &str) -> ServiceResult<SurfaceContractBundle> {
    builtin_surface_bundle(surface_id)
        .map_err(|error| parameter_error("load parameter surface", error))
}

/// The base a parameter session resolves over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProfileBase {
    Defaults,
    Last,
    LastSuccessful,
    File(PathBuf),
}

impl ProfileBase {
    fn parse(source: &str, profile_path: Option<String>) -> ServiceResult<Self> {
        Ok(match source {
            "defaults" => Self::Defaults,
            "file" => Self::File(PathBuf::from(
                profile_path.unwrap_or_else(|| "<memory>".to_string()),
            )),
            "last" => Self::Last,
            "last_successful" => Self::LastSuccessful,
            other => {
                return Err(parameter_error(
                    "resolve parameter source",
                    format!(
                        "unknown parameter source {other:?}; expected defaults, file, last, or last_successful"
                    ),
                ));
            }
        })
    }

    fn source(self) -> BaseSource {
        match self {
            Self::Defaults => BaseSource::Defaults,
            Self::Last => BaseSource::Last,
            Self::LastSuccessful => BaseSource::LastSuccessful,
            Self::File(path) => BaseSource::File(path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct StateView {
    #[serde(serialize_with = "serialize_native")]
    value: Option<ParameterValue>,
    origin: String,
    active: bool,
    required: bool,
    explicit: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct LocationView {
    line: u64,
    column: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct DiagnosticView {
    level: String,
    code: String,
    message: String,
    parameter: Option<String>,
    location: Option<LocationView>,
    suggestions: Vec<String>,
}

/// One resolved session: every parameter's state, the diagnostics, and the
/// sparse profile, absent while a required value is missing.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct SnapshotView {
    surface_id: String,
    dirty: bool,
    states: BTreeMap<String, StateView>,
    diagnostics: Vec<DiagnosticView>,
    profile_toml: Option<String>,
}

impl SnapshotView {
    fn new(session: &ParameterSession) -> ServiceResult<Self> {
        let profile_toml = match session.render_sparse() {
            Ok(profile_toml) => Some(profile_toml),
            Err(_)
                if session
                    .diagnostics()
                    .iter()
                    .any(|diagnostic| diagnostic.code == DiagnosticCode::MissingRequired) =>
            {
                None
            }
            Err(error) => return Err(parameter_error("render sparse parameter profile", error)),
        };
        Ok(Self {
            surface_id: session.bundle().surface.id().to_string(),
            dirty: session.is_dirty(),
            states: session
                .states()
                .iter()
                .map(|(name, state)| {
                    let view = StateView {
                        value: state.value.clone(),
                        origin: snake_case_debug(state.origin.clone()),
                        active: state.active,
                        required: state.required,
                        explicit: state.explicit,
                    };
                    (name.clone(), view)
                })
                .collect(),
            diagnostics: session
                .diagnostics()
                .iter()
                .map(|diagnostic| DiagnosticView {
                    level: snake_case_debug(diagnostic.level),
                    code: snake_case_debug(diagnostic.code),
                    message: diagnostic.message.clone(),
                    parameter: diagnostic.parameter.clone(),
                    location: diagnostic.location.map(|location| LocationView {
                        line: location.line as u64,
                        column: location.column as u64,
                    }),
                    suggestions: diagnostic.suggestions.clone(),
                })
                .collect(),
            profile_toml,
        })
    }
}

/// Resolve `overrides` and `unset` names over one base.
pub(crate) fn resolve(
    surface_id: &str,
    base: ProfileBase,
    profile_toml: Option<String>,
    overrides: BTreeMap<String, ParameterValue>,
    unset: BTreeSet<String>,
) -> ServiceResult<SnapshotView> {
    let session = ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle: parameter_bundle(surface_id)?,
            workspace: PathBuf::from("."),
            source: base.source(),
            profile_text: profile_toml,
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch {
                values: overrides,
                unset,
            },
            managed_save: false,
        })
        .map_err(|error| parameter_error("resolve parameter session", error))?;
    SnapshotView::new(&session)
}

fn session_from_values(
    surface_id: &str,
    values: BTreeMap<String, ParameterValue>,
) -> ServiceResult<ParameterSession> {
    ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle: parameter_bundle(surface_id)?,
            workspace: PathBuf::from("."),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch {
                values,
                unset: BTreeSet::new(),
            },
            managed_save: false,
        })
        .map_err(|error| parameter_error("resolve parameter values", error))
}

/// Render resolved values as a sparse current-contract TOML profile.
pub(crate) fn render_toml(
    surface_id: &str,
    values: BTreeMap<String, ParameterValue>,
) -> ServiceResult<String> {
    session_from_values(surface_id, values)?
        .render_sparse()
        .map_err(|error| parameter_error("render sparse parameter profile", error))
}

/// Load Last or Last Successful from a workspace's managed store, if present.
pub(crate) fn last(
    surface_id: &str,
    workspace: &Path,
    successful: bool,
) -> ServiceResult<Option<SnapshotView>> {
    // Reject an unknown surface before looking for its managed state.
    let bundle = parameter_bundle(surface_id)?;
    let (source, kind) = if successful {
        (
            BaseSource::LastSuccessful,
            ManagedProfileKind::LastSuccessful,
        )
    } else {
        (BaseSource::Last, ManagedProfileKind::Last)
    };
    let Some(profile) = ManagedStateStore::for_workspace(workspace)
        .read(surface_id, kind)
        .map_err(|error| parameter_error("read managed parameter profile", error))?
    else {
        return Ok(None);
    };
    let session = ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle,
            workspace: workspace.to_path_buf(),
            source,
            profile_text: Some(profile),
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            managed_save: true,
        })
        .map_err(|error| parameter_error("open parameter session", error))?;
    SnapshotView::new(&session).map(Some)
}

/// A profile written to disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WriteView {
    pub(crate) path: String,
    bytes_written: u64,
    managed_kind: Option<String>,
}

/// Atomically save resolved values to an explicit sparse TOML profile.
pub(crate) fn save(
    surface_id: &str,
    values: BTreeMap<String, ParameterValue>,
    destination: &Path,
) -> ServiceResult<WriteView> {
    let profile = render_toml(surface_id, values)?;
    let outcome = write_parameter_profile_atomic(destination, &profile)
        .map_err(|error| parameter_error("save parameter profile", error))?;
    Ok(WriteView {
        path: destination.to_string_lossy().into_owned(),
        bytes_written: outcome.bytes_written as u64,
        managed_kind: None,
    })
}

/// Write Last or Last Successful for one workspace.
pub(crate) fn write_managed(
    surface_id: &str,
    workspace: &Path,
    values: BTreeMap<String, ParameterValue>,
    successful: bool,
) -> ServiceResult<WriteView> {
    let profile = render_toml(surface_id, values)?;
    let (kind, name) = if successful {
        (ManagedProfileKind::LastSuccessful, "last_successful")
    } else {
        (ManagedProfileKind::Last, "last")
    };
    let outcome = ManagedStateStore::for_workspace(workspace)
        .write(surface_id, kind, &profile)
        .map_err(|error| parameter_error("write managed parameter profile", error))?;
    Ok(WriteView {
        path: outcome.path.to_string_lossy().into_owned(),
        bytes_written: profile.len() as u64,
        managed_kind: Some(name.to_string()),
    })
}

/// The arguments and standard input a task provider runs with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct InvocationView {
    pub(crate) protocol_name: Option<String>,
    pub(crate) protocol_version: Option<u32>,
    pub(crate) args: Vec<String>,
    pub(crate) stdin: Option<String>,
}

/// Project one complete task invocation: the task binary's `--json-run -`
/// with the values set.
pub(crate) fn provider_invocation(
    surface_id: &str,
    values: BTreeMap<String, ParameterValue>,
) -> ServiceResult<InvocationView> {
    let session = session_from_values(surface_id, values)?;
    session
        .render_sparse()
        .map_err(|error| parameter_error("validate provider invocation parameters", error))?;
    let invocation = project_provider_invocation(&session)
        .map_err(|error| parameter_error("project provider invocation", error))?;
    let descriptor = match session.bundle().surface.provider_family() {
        "imager" => Some(casars_imager::imager_protocol_descriptor()),
        "simobserve" => Some(casars_simulation::simobserve_protocol_descriptor()),
        _ => None,
    };
    Ok(InvocationView {
        protocol_name: descriptor
            .as_ref()
            .map(|descriptor| descriptor.protocol_name.clone()),
        protocol_version: descriptor.map(|descriptor| descriptor.protocol_version),
        args: invocation.args,
        stdin: invocation.stdin,
    })
}

/// The complete parameter catalog.
#[pyfunction]
pub(crate) fn parameter_catalog(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let catalog = builtin_surface_catalog()
        .map_err(|error| reason_error(py, parameter_error("load parameter catalog", error)))?;
    to_python(py, &CatalogEnvelopeView::new(catalog))
}

/// One self-contained surface definition and the concepts it references.
#[pyfunction]
pub(crate) fn parameter_surface_bundle<'py>(
    py: Python<'py>,
    surface_id: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let bundle = parameter_bundle(surface_id).map_err(|reason| reason_error(py, reason))?;
    to_python(py, &BundleView::from(&bundle))
}

/// The surface a sparse profile names in its header.
#[pyfunction]
pub(crate) fn parameter_profile_surface(py: Python<'_>, profile_toml: &str) -> PyResult<String> {
    parse_profile(profile_toml)
        .map(|profile| profile.header.surface)
        .map_err(|error| reason_error(py, parameter_error("parse parameter profile", error)))
}

/// The catalog's commented reference template for one surface.
#[pyfunction]
pub(crate) fn parameter_template_toml(py: Python<'_>, surface_id: &str) -> PyResult<String> {
    let bundle = parameter_bundle(surface_id).map_err(|reason| reason_error(py, reason))?;
    render_documented_template(&bundle)
        .map_err(|error| reason_error(py, parameter_error("render parameter template", error)))
}

/// Last or Last Successful of one workspace, or `None`.
#[pyfunction]
pub(crate) fn parameter_last<'py>(
    py: Python<'py>,
    surface_id: &str,
    workspace: PathBuf,
    successful: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let snapshot = py
        .detach(|| last(surface_id, &workspace, successful))
        .map_err(|reason| reason_error(py, reason))?;
    to_python(py, &snapshot)
}

/// Resolve explicit values and unset names over one base source.
#[pyfunction]
#[pyo3(signature = (surface_id, base_source, profile_toml, profile_path, overrides, unset))]
pub(crate) fn parameter_resolve<'py>(
    py: Python<'py>,
    surface_id: &str,
    base_source: &str,
    profile_toml: Option<String>,
    profile_path: Option<String>,
    overrides: &Bound<'py, PyDict>,
    unset: Vec<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let overrides = parameter_values(overrides)?;
    let snapshot = ProfileBase::parse(base_source, profile_path)
        .and_then(|base| {
            resolve(
                surface_id,
                base,
                profile_toml,
                overrides,
                unset.into_iter().collect(),
            )
        })
        .map_err(|reason| reason_error(py, reason))?;
    to_python(py, &snapshot)
}

/// Resolved values as a sparse current-contract TOML profile.
#[pyfunction]
pub(crate) fn parameter_render_toml(
    py: Python<'_>,
    surface_id: &str,
    values: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let values = parameter_values(values)?;
    render_toml(surface_id, values).map_err(|reason| reason_error(py, reason))
}

/// The provider invocation of resolved values.
#[pyfunction]
pub(crate) fn parameter_provider_invocation<'py>(
    py: Python<'py>,
    surface_id: &str,
    values: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyAny>> {
    let values = parameter_values(values)?;
    let invocation =
        provider_invocation(surface_id, values).map_err(|reason| reason_error(py, reason))?;
    to_python(py, &invocation)
}

/// Atomically save resolved values to a sparse TOML profile.
#[pyfunction]
pub(crate) fn parameter_save<'py>(
    py: Python<'py>,
    surface_id: &str,
    values: &Bound<'py, PyDict>,
    destination_path: PathBuf,
) -> PyResult<Bound<'py, PyAny>> {
    let values = parameter_values(values)?;
    let written = py
        .detach(|| save(surface_id, values, &destination_path))
        .map_err(|reason| reason_error(py, reason))?;
    to_python(py, &written)
}

/// Write Last or Last Successful for one workspace.
#[pyfunction]
pub(crate) fn parameter_write_managed<'py>(
    py: Python<'py>,
    surface_id: &str,
    workspace: PathBuf,
    values: &Bound<'py, PyDict>,
    successful: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let values = parameter_values(values)?;
    let written = py
        .detach(|| write_managed(surface_id, &workspace, values, successful))
        .map_err(|reason| reason_error(py, reason))?;
    to_python(py, &written)
}

#[cfg(test)]
#[path = "parameters_tests.rs"]
mod tests;
