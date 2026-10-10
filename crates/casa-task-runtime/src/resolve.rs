// SPDX-License-Identifier: LGPL-3.0-or-later

//! One-shot resolution of a surface's parameters over its defaults, as a
//! provider resolves a request on every route: command-line flags or the
//! sparse JSON of `--json-run` become the same active values.

use std::collections::BTreeMap;
use std::path::PathBuf;

use casa_provider_contracts::{ParameterValue, SurfaceContractBundle};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{
    BaseSource, DiagnosticLevel, OpenSessionRequest, ParameterRuntime, ParameterRuntimeError,
    ResolutionPatch,
};

/// Why a set of values does not resolve.
#[derive(Debug, Error)]
pub enum ResolveError {
    /// A value was `null`, which no parameter domain admits.
    #[error("parameter {0:?} is null")]
    Null(String),
    /// The session could not be opened.
    #[error(transparent)]
    Runtime(#[from] ParameterRuntimeError),
    /// The resolved values break the surface's contract.
    #[error("{message}")]
    Invalid {
        /// The parameter the diagnostic names, if one.
        parameter: Option<String>,
        /// The diagnostic.
        message: String,
    },
}

/// Resolve `overrides` over `bundle`'s defaults and return every active
/// parameter's value; the first error-level diagnostic refuses them.
pub fn resolve_active_values(
    bundle: SurfaceContractBundle,
    workspace: PathBuf,
    overrides: ResolutionPatch,
) -> Result<BTreeMap<String, ParameterValue>, ResolveError> {
    let session = ParameterRuntime::default().open_session(OpenSessionRequest {
        bundle,
        workspace,
        source: BaseSource::Defaults,
        profile_text: None,
        context_patch: ResolutionPatch::default(),
        override_patch: overrides,
        managed_save: false,
    })?;
    if let Some(diagnostic) = session
        .diagnostics()
        .iter()
        .find(|diagnostic| diagnostic.level == DiagnosticLevel::Error)
    {
        return Err(ResolveError::Invalid {
            parameter: diagnostic.parameter.clone(),
            message: diagnostic.message.clone(),
        });
    }
    Ok(session
        .states()
        .iter()
        .filter(|(_, state)| state.active)
        .filter_map(|(name, state)| Some((name.clone(), state.value.clone()?)))
        .collect())
}

/// Resolve sparse plain-JSON `values`, keyed by parameter name, over
/// `bundle`'s defaults and return every active parameter as plain JSON.
pub fn resolve_plain_values(
    bundle: SurfaceContractBundle,
    workspace: PathBuf,
    values: &Map<String, Value>,
) -> Result<Map<String, Value>, ResolveError> {
    let overrides = ResolutionPatch {
        values: values
            .iter()
            .map(|(name, value)| {
                ParameterValue::from_plain_json(value)
                    .map(|value| (name.clone(), value))
                    .ok_or_else(|| ResolveError::Null(name.clone()))
            })
            .collect::<Result<_, _>>()?,
        ..ResolutionPatch::default()
    };
    Ok(resolve_active_values(bundle, workspace, overrides)?
        .into_iter()
        .map(|(name, value)| (name, value.to_plain_json()))
        .collect())
}
