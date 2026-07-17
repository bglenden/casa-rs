// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use casa_provider_contracts::{ParameterType, ParameterValue, SurfaceContractBundle, SurfaceKind};
use thiserror::Error;

use crate::{
    BaseSource, ManagedProfileKind, ManagedStateError, ManagedStateStore, ParameterSession,
    ParameterSessionError, ProfileError, ResolutionPatch, parse_profile,
};

/// Complete request for opening one canonical parameter session.
#[derive(Debug, Clone)]
pub struct OpenSessionRequest {
    pub bundle: SurfaceContractBundle,
    pub workspace: PathBuf,
    pub source: BaseSource,
    /// Optional already-read profile text. This is useful at generated/FFI
    /// boundaries; when absent, file and managed sources are read here.
    pub profile_text: Option<String>,
    pub context_patch: ResolutionPatch,
    pub override_patch: ResolutionPatch,
    pub managed_save: bool,
}

impl OpenSessionRequest {
    pub fn defaults(bundle: SurfaceContractBundle, workspace: impl Into<PathBuf>) -> Self {
        Self {
            bundle,
            workspace: workspace.into(),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            managed_save: true,
        }
    }
}

/// Sole high-level owner of parameter source resolution and lifecycle policy.
#[derive(Debug, Clone)]
pub struct ParameterRuntime {
    session_debounce: Duration,
}

impl Default for ParameterRuntime {
    fn default() -> Self {
        Self::new(Duration::from_millis(350))
    }
}

impl ParameterRuntime {
    pub fn new(session_debounce: Duration) -> Self {
        Self { session_debounce }
    }

    pub fn session_debounce(&self) -> Duration {
        self.session_debounce
    }

    /// Open and fully resolve a session from exactly the requested source.
    /// Missing, unreadable, and corrupt Last state are errors and never switch
    /// the request to Defaults.
    pub fn open_session(
        &self,
        request: OpenSessionRequest,
    ) -> Result<ParameterSession, ParameterRuntimeError> {
        let surface_id = request.bundle.surface.id().to_string();
        let mut session = match &request.source {
            BaseSource::Defaults => {
                if request.profile_text.is_some() {
                    return Err(ParameterRuntimeError::InvalidRequest(
                        "Defaults cannot carry profile text".to_string(),
                    ));
                }
                ParameterSession::defaults(request.bundle)?
            }
            BaseSource::Last => {
                let text =
                    self.profile_text(&request, &surface_id, ManagedProfileKind::Last, "Last")?;
                let profile = parse_profile(&text)?;
                ParameterSession::from_profile(request.bundle, BaseSource::Last, &profile)?
            }
            BaseSource::LastSuccessful => {
                if request.bundle.surface.kind() != SurfaceKind::Task {
                    return Err(ParameterRuntimeError::InvalidRequest(
                        "Last Successful exists only for task surfaces".to_string(),
                    ));
                }
                let text = self.profile_text(
                    &request,
                    &surface_id,
                    ManagedProfileKind::LastSuccessful,
                    "Last Successful",
                )?;
                let profile = parse_profile(&text)?;
                ParameterSession::from_profile(
                    request.bundle,
                    BaseSource::LastSuccessful,
                    &profile,
                )?
            }
            BaseSource::File(path) => {
                let text = match request.profile_text {
                    Some(text) => text,
                    None => fs::read_to_string(path).map_err(|source| {
                        ParameterRuntimeError::ReadProfile {
                            path: path.clone(),
                            source,
                        }
                    })?,
                };
                let profile = parse_profile(&text)?;
                ParameterSession::from_profile(
                    request.bundle,
                    BaseSource::File(path.clone()),
                    &profile,
                )?
            }
        };
        if request.context_patch != ResolutionPatch::default() {
            session.apply_context_patch(request.context_patch)?;
        }
        if request.override_patch != ResolutionPatch::default() {
            session.apply_override_patch(request.override_patch)?;
        }
        Ok(session)
    }

    fn profile_text(
        &self,
        request: &OpenSessionRequest,
        surface_id: &str,
        kind: ManagedProfileKind,
        label: &'static str,
    ) -> Result<String, ParameterRuntimeError> {
        if let Some(text) = &request.profile_text {
            return Ok(text.clone());
        }
        let store = ManagedStateStore::for_workspace(&request.workspace);
        let path = store.profile_path(surface_id, kind)?;
        store
            .read(surface_id, kind)?
            .ok_or(ParameterRuntimeError::MissingManagedProfile {
                surface_id: surface_id.to_string(),
                kind: label,
                path,
            })
    }
}

/// Parse one presentation string through the canonical parameter value domain.
pub fn parse_parameter_text(
    value: &str,
    domain: &ParameterType,
) -> Result<ParameterValue, ParameterTextError> {
    parse_parameter_text_inner(value, domain).map_err(ParameterTextError)
}

fn parse_parameter_text_inner(
    value: &str,
    domain: &ParameterType,
) -> Result<ParameterValue, String> {
    match domain {
        ParameterType::Bool => match value {
            "true" => Ok(ParameterValue::Bool(true)),
            "false" => Ok(ParameterValue::Bool(false)),
            _ => Err(format!("expected true or false, got {value:?}")),
        },
        ParameterType::Integer => value
            .parse::<i64>()
            .map(ParameterValue::Integer)
            .map_err(|error| format!("parse integer {value:?}: {error}")),
        ParameterType::Float => value
            .parse::<f64>()
            .map(ParameterValue::Float)
            .map_err(|error| format!("parse number {value:?}: {error}")),
        ParameterType::String
        | ParameterType::Path { .. }
        | ParameterType::Choice { .. }
        | ParameterType::Quantity { .. } => Ok(ParameterValue::String(value.to_string())),
        ParameterType::Array { element, .. } => {
            if value.starts_with('[') {
                let parsed = format!("value = {value}")
                    .parse::<toml::Value>()
                    .map_err(|error| format!("parse array {value:?}: {error}"))?;
                let values = parsed
                    .get("value")
                    .and_then(toml::Value::as_array)
                    .ok_or_else(|| format!("expected TOML array, got {value:?}"))?;
                return values
                    .iter()
                    .map(|value| parse_toml_value(value, element))
                    .collect::<Result<Vec<_>, _>>()
                    .map(ParameterValue::Array);
            }
            if value.contains(',') {
                return value
                    .split(',')
                    .map(|value| parse_parameter_text_inner(value.trim(), element))
                    .collect::<Result<Vec<_>, _>>()
                    .map(ParameterValue::Array);
            }
            parse_parameter_text_inner(value, element)
        }
        ParameterType::Table { .. } => {
            let parsed = format!("value = {value}")
                .parse::<toml::Value>()
                .map_err(|error| format!("parse table {value:?}: {error}"))?;
            let value = parsed
                .get("value")
                .ok_or_else(|| "missing parsed table value".to_string())?;
            parse_toml_value(value, domain)
        }
        ParameterType::Optional {
            value: inner,
            states,
        } => {
            if states.iter().any(|state| state == value) {
                Ok(ParameterValue::String(value.to_string()))
            } else {
                parse_parameter_text_inner(value, inner)
            }
        }
    }
}

fn parse_toml_value(value: &toml::Value, domain: &ParameterType) -> Result<ParameterValue, String> {
    match value {
        toml::Value::String(value) => parse_parameter_text_inner(value, domain),
        toml::Value::Integer(value) => Ok(ParameterValue::Integer(*value)),
        toml::Value::Float(value) if value.is_finite() => Ok(ParameterValue::Float(*value)),
        toml::Value::Boolean(value) => Ok(ParameterValue::Bool(*value)),
        toml::Value::Array(values) => values
            .iter()
            .map(|value| parse_toml_value(value, domain))
            .collect::<Result<Vec<_>, _>>()
            .map(ParameterValue::Array),
        toml::Value::Table(values) => values
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    parse_toml_value(value, &ParameterType::String)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()
            .map(ParameterValue::Table),
        _ => Err("TOML datetime and non-finite values are not parameters".to_string()),
    }
}

#[derive(Debug, Error)]
pub enum ParameterRuntimeError {
    #[error("invalid parameter session request: {0}")]
    InvalidRequest(String),
    #[error("no managed {kind} profile exists for {surface_id} at {path}")]
    MissingManagedProfile {
        surface_id: String,
        kind: &'static str,
        path: PathBuf,
    },
    #[error("read parameter profile {path}: {source}")]
    ReadProfile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    ManagedState(#[from] ManagedStateError),
    #[error(transparent)]
    Profile(#[from] ProfileError),
    #[error(transparent)]
    Session(#[from] ParameterSessionError),
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ParameterTextError(String);

#[cfg(test)]
mod tests {
    use super::*;
    use casa_provider_contracts::builtin_surface_bundle;

    #[test]
    fn missing_last_is_not_replaced_with_defaults() {
        let workspace = tempfile::tempdir().unwrap();
        let request = OpenSessionRequest {
            bundle: builtin_surface_bundle("imager").unwrap(),
            workspace: workspace.path().to_path_buf(),
            source: BaseSource::Last,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            managed_save: true,
        };
        assert!(matches!(
            ParameterRuntime::default().open_session(request),
            Err(ParameterRuntimeError::MissingManagedProfile { .. })
        ));
    }

    #[test]
    fn text_parser_handles_nested_domains() {
        assert_eq!(
            parse_parameter_text(
                "1,2,3",
                &ParameterType::Array {
                    element: Box::new(ParameterType::Integer),
                    min_items: 0,
                    max_items: None,
                    allow_scalar: true,
                }
            )
            .unwrap(),
            ParameterValue::Array(vec![
                ParameterValue::Integer(1),
                ParameterValue::Integer(2),
                ParameterValue::Integer(3),
            ])
        );
    }
}
