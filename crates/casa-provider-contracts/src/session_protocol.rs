// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared JSONL session-protocol scaffolding.

use schemars::{JsonSchema, schema_for};
use serde::Serialize;

use crate::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    SessionProviderContract, SessionProviderSchemas, SessionSemanticContract,
    SurfaceContractBundle, merged_components,
};

/// Transport used by every subprocess session protocol in this workspace.
pub const JSONL_STDIO_TRANSPORT: &str = "jsonl_stdio";

/// Build the common provider bundle for a typed JSONL session protocol.
pub fn jsonl_session_contract<Request, Response>(
    protocol_name: &str,
    protocol_version: u32,
    binary_version: &str,
    parameter_surface: SurfaceContractBundle,
) -> SessionProviderContract
where
    Request: JsonSchema,
    Response: JsonSchema,
{
    let request_schema = schema_for!(Request);
    let response_schema = schema_for!(Response);
    SessionProviderContract {
        protocol: ProviderProtocolDescriptor::new(
            protocol_name,
            protocol_version,
            ProviderSurfaceKind::Session,
            binary_version,
        ),
        semantic: SessionSemanticContract {
            transport: JSONL_STDIO_TRANSPORT.to_string(),
            request_schema: request_schema.clone(),
            response_schema: response_schema.clone(),
        },
        components: merged_components([&request_schema, &response_schema]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: None,
                    session: Some("--session".to_string()),
                },
            }),
            python: None,
        },
        parameter_surfaces: vec![parameter_surface],
        domain_schemas: SessionProviderSchemas {
            request_schema,
            response_schema,
            additional: NoAdditionalProviderSchemas::default(),
        },
    }
}

/// Render the deterministic JSON schema for a session request or response.
pub fn session_schema_json<T>() -> Result<String, serde_json::Error>
where
    T: JsonSchema,
{
    serde_json::to_string_pretty(&schema_for!(T))
}

/// Render a provider contract as deterministic pretty JSON.
pub fn session_contract_json(
    contract: &SessionProviderContract,
) -> Result<String, serde_json::Error> {
    contract.to_pretty_json()
}

/// Common version accessor used by generic JSONL clients.
pub trait VersionedSessionEnvelope {
    /// Protocol version carried on the wire.
    fn protocol_version(&self) -> u32;
}

/// Define concrete, schema-stable request/response/error types for a JSONL
/// session protocol while keeping their implementation in this shared owner.
#[macro_export]
macro_rules! define_jsonl_session_envelopes {
    (
        request $request:ident for $command:ty;
        response $response:ident for $payload:ty;
        error $error:ident;
    ) => {
        /// JSON Lines request envelope sent from `casars` to the browser backend.
        #[derive(
            Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema, PartialEq, Eq,
        )]
        pub struct $request {
            /// Protocol version expected by the client.
            pub version: u32,
            /// Requested command.
            pub command: $command,
        }

        impl $request {
            /// Wrap a command using the current protocol version.
            pub fn new(command: $command) -> Self {
                Self {
                    version: PROTOCOL_VERSION,
                    command,
                }
            }
        }

        impl $crate::VersionedSessionEnvelope for $request {
            fn protocol_version(&self) -> u32 {
                self.version
            }
        }

        /// JSON Lines response envelope sent from the backend to `casars`.
        #[derive(
            Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema, PartialEq,
        )]
        pub struct $response {
            /// Protocol version returned by the backend.
            pub version: u32,
            /// Response payload.
            pub response: $payload,
        }

        impl $crate::VersionedSessionEnvelope for $response {
            fn protocol_version(&self) -> u32 {
                self.version
            }
        }

        /// Structured error payload returned by the session backend.
        #[derive(
            Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema, PartialEq, Eq,
        )]
        pub struct $error {
            /// Stable machine-readable error code.
            pub code: String,
            /// Human-readable explanation.
            pub message: String,
        }

        impl $error {
            /// Construct a provider-owned error payload.
            pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
                Self {
                    code: code.into(),
                    message: message.into(),
                }
            }
        }
    };
}

/// Define deterministic stale checks (and an explicit regeneration path) for
/// checked-in request and response schemas.
#[macro_export]
macro_rules! committed_session_schema_tests {
    (
        request $request:ty => $request_file:literal;
        response $response:ty => $response_file:literal;
    ) => {
        #[cfg(test)]
        mod committed_session_schemas {
            use std::path::Path;

            fn regenerate_if_requested(path: &Path, schema: &str) {
                if std::env::var_os("CASA_RS_REGENERATE_SESSION_SCHEMAS").is_none() {
                    return;
                }
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).expect("create protocol schema directory");
                }
                std::fs::write(path, format!("{}\n", schema.trim_end()))
                    .expect("write regenerated protocol schema");
            }

            #[test]
            fn request_schema_matches_committed_artifact() {
                let generated =
                    $crate::session_schema_json::<$request>().expect("generate request schema");
                let path = Path::new(env!("CARGO_MANIFEST_DIR")).join($request_file);
                regenerate_if_requested(&path, &generated);
                let committed = std::fs::read_to_string(&path).expect("read request schema");
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&generated)
                        .expect("parse generated request schema"),
                    serde_json::from_str::<serde_json::Value>(&committed)
                        .expect("parse committed request schema")
                );
            }

            #[test]
            fn response_schema_matches_committed_artifact() {
                let generated =
                    $crate::session_schema_json::<$response>().expect("generate response schema");
                let path = Path::new(env!("CARGO_MANIFEST_DIR")).join($response_file);
                regenerate_if_requested(&path, &generated);
                let committed = std::fs::read_to_string(&path).expect("read response schema");
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&generated)
                        .expect("parse generated response schema"),
                    serde_json::from_str::<serde_json::Value>(&committed)
                        .expect("parse committed response schema")
                );
            }
        }
    };
}

/// Serialize one JSONL request without adding transport framing.
pub fn encode_session_message<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}
