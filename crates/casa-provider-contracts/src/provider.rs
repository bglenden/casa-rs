// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;

use schemars::{JsonSchema, schema::RootSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    ProviderComponentSchemas, ProviderProjectionMetadata, ProviderSurfaceKind,
    SessionSemanticContract, SurfaceContractBundle, SurfaceDefinition, TaskSemanticContract,
};

/// Shared version descriptor for every task and session provider protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderProtocolDescriptor {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version used for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl ProviderProtocolDescriptor {
    /// Construct a descriptor owned by the shared provider-contract boundary.
    pub fn new(
        protocol_name: impl Into<String>,
        protocol_version: u32,
        surface_kind: ProviderSurfaceKind,
        binary_version: impl Into<String>,
    ) -> Self {
        Self {
            protocol_name: protocol_name.into(),
            protocol_version,
            surface_kind,
            binary_version: binary_version.into(),
        }
    }
}

/// Generic top-level provider bundle shared by task and session protocols.
///
/// `S` retains the task- or session-specific semantic contract. `D` is a
/// strongly typed schema payload flattened into the historical wire shape.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderContractEnvelope<S, D> {
    /// Compatibility descriptor for the provider protocol.
    pub protocol: ProviderProtocolDescriptor,
    /// Typed semantic contract for the provider family.
    pub semantic: S,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived metadata for supported consumer projections.
    pub projections: ProviderProjectionMetadata,
    /// Canonical parameter contracts embedded for self-contained consumers.
    pub parameter_surfaces: Vec<SurfaceContractBundle>,
    /// Provider-specific root schemas, flattened to preserve the public JSON.
    #[serde(flatten)]
    pub domain_schemas: D,
}

/// Typed request/result schemas for a one-shot task provider.
#[derive(Debug, Clone, Serialize)]
pub struct TaskProviderSchemas<E = NoAdditionalProviderSchemas> {
    pub request_schema: RootSchema,
    pub result_schema: RootSchema,
    #[serde(flatten)]
    pub additional: E,
}

/// Typed request/response schemas for a stateful session provider.
#[derive(Debug, Clone, Serialize)]
pub struct SessionProviderSchemas<E = NoAdditionalProviderSchemas> {
    pub request_schema: RootSchema,
    pub response_schema: RootSchema,
    #[serde(flatten)]
    pub additional: E,
}

/// Empty typed extension for providers without additional root schemas.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct NoAdditionalProviderSchemas {}

/// Shared task-provider envelope.
pub type TaskProviderContract<E = NoAdditionalProviderSchemas> =
    ProviderContractEnvelope<TaskSemanticContract, TaskProviderSchemas<E>>;

/// Shared session-provider envelope.
pub type SessionProviderContract<E = NoAdditionalProviderSchemas> =
    ProviderContractEnvelope<SessionSemanticContract, SessionProviderSchemas<E>>;

/// Stable validation failure for a provider envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContractValidationError {
    pub code: &'static str,
    pub message: String,
}

impl<E> ProviderContractEnvelope<TaskSemanticContract, TaskProviderSchemas<E>> {
    /// Validate shared task-envelope invariants.
    pub fn validate(&self) -> Result<(), Vec<ProviderContractValidationError>> {
        let mut errors = self.validate_common(ProviderSurfaceKind::Task);
        if self.semantic.operations.is_empty() {
            errors.push(validation_error(
                "missing_operations",
                "task provider declares no operations",
            ));
        }
        if self.semantic.request_schema != self.domain_schemas.request_schema {
            errors.push(validation_error(
                "request_schema_mismatch",
                "semantic and flattened task request schemas differ",
            ));
        }
        if self.semantic.result_schema != self.domain_schemas.result_schema {
            errors.push(validation_error(
                "result_schema_mismatch",
                "semantic and flattened task result schemas differ",
            ));
        }
        let mut names = BTreeSet::new();
        let mut request_kinds = BTreeSet::new();
        for operation in &self.semantic.operations {
            if operation.name.is_empty() || !names.insert(operation.name.as_str()) {
                errors.push(validation_error(
                    "duplicate_operation",
                    format!(
                        "task operation name {:?} is empty or duplicated",
                        operation.name
                    ),
                ));
            }
            if operation.request_kind.is_empty()
                || !request_kinds.insert(operation.request_kind.as_str())
            {
                errors.push(validation_error(
                    "duplicate_request_kind",
                    format!(
                        "task request kind {:?} is empty or duplicated",
                        operation.request_kind
                    ),
                ));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl<E> ProviderContractEnvelope<SessionSemanticContract, SessionProviderSchemas<E>> {
    /// Validate shared session-envelope invariants.
    pub fn validate(&self) -> Result<(), Vec<ProviderContractValidationError>> {
        let mut errors = self.validate_common(ProviderSurfaceKind::Session);
        if self.semantic.transport.trim().is_empty() {
            errors.push(validation_error(
                "missing_transport",
                "session provider declares an empty transport",
            ));
        }
        if self.semantic.request_schema != self.domain_schemas.request_schema {
            errors.push(validation_error(
                "request_schema_mismatch",
                "semantic and flattened session request schemas differ",
            ));
        }
        if self.semantic.response_schema != self.domain_schemas.response_schema {
            errors.push(validation_error(
                "response_schema_mismatch",
                "semantic and flattened session response schemas differ",
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl<S, D> ProviderContractEnvelope<S, D> {
    /// Render the canonical provider bundle deterministically.
    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error>
    where
        S: Serialize,
        D: Serialize,
    {
        serde_json::to_string_pretty(self)
    }

    fn validate_common(
        &self,
        expected_kind: ProviderSurfaceKind,
    ) -> Vec<ProviderContractValidationError> {
        let mut errors = Vec::new();
        if self.protocol.protocol_name.trim().is_empty() {
            errors.push(validation_error(
                "missing_protocol_name",
                "provider protocol name is empty",
            ));
        }
        if self.protocol.protocol_version == 0 {
            errors.push(validation_error(
                "invalid_protocol_version",
                "provider protocol version must be positive",
            ));
        }
        if self.protocol.binary_version.trim().is_empty() {
            errors.push(validation_error(
                "missing_binary_version",
                "provider binary version is empty",
            ));
        }
        if self.protocol.surface_kind != expected_kind {
            errors.push(validation_error(
                "surface_kind",
                format!(
                    "provider descriptor is {:?}, expected {expected_kind:?}",
                    self.protocol.surface_kind
                ),
            ));
        }
        if self.parameter_surfaces.is_empty() {
            errors.push(validation_error(
                "missing_parameter_surface",
                "provider bundle embeds no parameter surfaces",
            ));
        }

        let mut surface_ids = BTreeSet::new();
        for bundle in &self.parameter_surfaces {
            if !surface_ids.insert(bundle.surface.id()) {
                errors.push(validation_error(
                    "duplicate_parameter_surface",
                    format!("duplicate parameter surface {:?}", bundle.surface.id()),
                ));
            }
            let surface_kind = match &bundle.surface {
                SurfaceDefinition::Task(_) => ProviderSurfaceKind::Task,
                SurfaceDefinition::Session(_) => ProviderSurfaceKind::Session,
            };
            if surface_kind != expected_kind {
                errors.push(validation_error(
                    "parameter_surface_kind",
                    format!(
                        "parameter surface {:?} is {surface_kind:?}, expected {expected_kind:?}",
                        bundle.surface.id()
                    ),
                ));
            }
            if let Err(surface_errors) = bundle.validate() {
                errors.extend(surface_errors.into_iter().map(|error| {
                    validation_error(
                        "parameter_surface",
                        format!(
                            "parameter surface {:?} failed validation: {}: {}",
                            bundle.surface.id(),
                            error.code,
                            error.message
                        ),
                    )
                }));
            }
        }

        match self.projections.cli.as_ref() {
            Some(cli) => {
                let actions = &cli.machine_actions;
                if actions.protocol_info.as_deref() != Some("--protocol-info")
                    || actions.json_schema.as_deref() != Some("--json-schema")
                {
                    errors.push(validation_error(
                        "missing_discovery_action",
                        "provider CLI must declare protocol-info and schema actions",
                    ));
                }
                match expected_kind {
                    ProviderSurfaceKind::Task
                        if actions.json_run.as_deref() != Some("--json-run <SOURCE>")
                            || actions.session.is_some() =>
                    {
                        errors.push(validation_error(
                            "task_actions",
                            "task provider must declare json-run and no session action",
                        ));
                    }
                    ProviderSurfaceKind::Session
                        if actions.session.as_deref() != Some("--session")
                            || actions.json_run.is_some() =>
                    {
                        errors.push(validation_error(
                            "session_actions",
                            "session provider must declare session and no json-run action",
                        ));
                    }
                    _ => {}
                }
            }
            None => errors.push(validation_error(
                "missing_cli_projection",
                "task and session providers must declare CLI discovery actions",
            )),
        }
        errors
    }
}

fn validation_error(
    code: &'static str,
    message: impl Into<String>,
) -> ProviderContractValidationError {
    ProviderContractValidationError {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
        TaskOperationDescriptor, builtin_surface_bundle, merged_components,
    };
    use schemars::schema_for;

    #[test]
    fn task_envelope_preserves_flat_schema_fields_and_validates() {
        let request_schema = schema_for!(String);
        let result_schema = schema_for!(usize);
        let mut bundle = TaskProviderContract {
            protocol: ProviderProtocolDescriptor::new(
                "example_task",
                1,
                ProviderSurfaceKind::Task,
                "0.1.0",
            ),
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
            parameter_surfaces: vec![builtin_surface_bundle("importvla").unwrap()],
            domain_schemas: TaskProviderSchemas {
                request_schema,
                result_schema,
                additional: NoAdditionalProviderSchemas {},
            },
        };

        bundle.validate().unwrap();
        let value = serde_json::to_value(&bundle).unwrap();
        assert!(value.get("request_schema").is_some());
        assert!(value.get("result_schema").is_some());
        assert!(value.get("domain_schemas").is_none());

        bundle.protocol.protocol_version = 0;
        assert!(validation_codes(&bundle).contains("invalid_protocol_version"));
        bundle.protocol.protocol_version = 1;

        bundle
            .projections
            .cli
            .as_mut()
            .unwrap()
            .machine_actions
            .json_run = Some("--different-run".to_string());
        assert!(validation_codes(&bundle).contains("task_actions"));
        bundle
            .projections
            .cli
            .as_mut()
            .unwrap()
            .machine_actions
            .json_run = Some("--json-run <SOURCE>".to_string());

        let original_surface = bundle.parameter_surfaces[0].clone();
        match &mut bundle.parameter_surfaces[0].surface {
            SurfaceDefinition::Task(definition) => definition.id.clear(),
            SurfaceDefinition::Session(_) => unreachable!(),
        }
        assert!(validation_codes(&bundle).contains("parameter_surface"));
        bundle.parameter_surfaces[0] = original_surface;

        bundle.domain_schemas.request_schema = schema_for!(u64);
        assert!(validation_codes(&bundle).contains("request_schema_mismatch"));
    }

    #[test]
    fn task_envelope_rejects_session_surfaces_and_missing_actions() {
        let request_schema = schema_for!(String);
        let result_schema = schema_for!(usize);
        let bundle = TaskProviderContract {
            protocol: ProviderProtocolDescriptor::new(
                "broken_task",
                1,
                ProviderSurfaceKind::Task,
                "0.1.0",
            ),
            semantic: TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: vec![],
            },
            components: merged_components([&request_schema, &result_schema]),
            annotations: JsonValue::Null,
            projections: ProviderProjectionMetadata {
                cli: None,
                python: None,
            },
            parameter_surfaces: vec![builtin_surface_bundle("imexplore").unwrap()],
            domain_schemas: TaskProviderSchemas {
                request_schema,
                result_schema,
                additional: NoAdditionalProviderSchemas {},
            },
        };

        let codes = bundle
            .validate()
            .unwrap_err()
            .into_iter()
            .map(|error| error.code)
            .collect::<BTreeSet<_>>();
        assert!(codes.contains("missing_operations"));
        assert!(codes.contains("missing_cli_projection"));
        assert!(codes.contains("parameter_surface_kind"));
    }

    fn validation_codes<E: Serialize>(
        bundle: &ProviderContractEnvelope<TaskSemanticContract, TaskProviderSchemas<E>>,
    ) -> BTreeSet<&'static str> {
        bundle
            .validate()
            .unwrap_err()
            .into_iter()
            .map(|error| error.code)
            .collect()
    }
}
