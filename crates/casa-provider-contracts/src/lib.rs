// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared canonical provider-contract bundle types.
//!
//! These structs intentionally stay small and transport-oriented so task and
//! session providers can expose one machine-readable schema bundle while
//! retaining compatibility projections such as the legacy `--ui-schema` view.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use schemars::schema::{RootSchema, Schema};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

/// Stable provider surface kind advertised in canonical schema bundles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSurfaceKind {
    /// One-shot request/result operation.
    Task,
    /// Stateful command/response protocol.
    Session,
    /// Handle-oriented object API.
    Object,
}

/// Merged reusable component schemas extracted from one or more root schemas.
pub type ProviderComponentSchemas = BTreeMap<String, Schema>;

/// CLI machine-action projections derived from the canonical contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCliMachineActions {
    /// Legacy compatibility view used by the current launcher/TUI integration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_schema: Option<String>,
    /// Canonical schema bundle action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<String>,
    /// Protocol descriptor action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_info: Option<String>,
    /// One-shot task execution action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_run: Option<String>,
    /// Stateful session action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
}

/// CLI projection metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCliProjection {
    /// Known machine-facing flags derived from the canonical contract.
    pub machine_actions: ProviderCliMachineActions,
}

/// Projection metadata for derived consumer views.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProviderProjectionMetadata {
    /// CLI projection metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cli: Option<ProviderCliProjection>,
    /// Legacy `--ui-schema` compatibility view when the surface still exposes it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_schema: Option<JsonValue>,
    /// Python binding projection metadata for direct in-process object surfaces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub python: Option<JsonValue>,
}

/// One task operation exposed by a task-surface request/result bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskOperationDescriptor {
    /// Stable operation identifier used on the wire.
    pub name: String,
    /// Tagged request discriminator for the operation.
    pub request_kind: String,
    /// Tagged result discriminator when the result envelope is variant-based.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_kind: Option<String>,
}

/// Semantic task contract exposed by the canonical bundle.
#[derive(Debug, Clone, Serialize)]
pub struct TaskSemanticContract {
    /// Canonical tagged request envelope schema.
    pub request_schema: RootSchema,
    /// Canonical tagged result envelope schema.
    pub result_schema: RootSchema,
    /// Stable task operations carried by the envelope schemas.
    pub operations: Vec<TaskOperationDescriptor>,
}

/// Semantic session contract exposed by the canonical bundle.
#[derive(Debug, Clone, Serialize)]
pub struct SessionSemanticContract {
    /// Session transport identifier.
    pub transport: String,
    /// Canonical tagged request envelope schema.
    pub request_schema: RootSchema,
    /// Canonical tagged response envelope schema.
    pub response_schema: RootSchema,
}

/// One constructor exposed by an object surface.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectConstructorDescriptor {
    /// Stable constructor identifier.
    pub name: String,
    /// Canonical constructor parameter schema.
    pub parameters_schema: RootSchema,
}

/// One property exposed by an object surface.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectPropertyDescriptor {
    /// Stable property identifier.
    pub name: String,
    /// Canonical property value schema.
    pub value_schema: RootSchema,
    /// Whether the property can be read.
    pub readable: bool,
    /// Whether the property can be written directly.
    pub writable: bool,
}

/// One method exposed by an object surface.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectMethodDescriptor {
    /// Stable method identifier.
    pub name: String,
    /// Canonical method parameter schema.
    pub parameters_schema: RootSchema,
    /// Canonical method result schema when the method returns a value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_schema: Option<RootSchema>,
    /// Whether the method mutates persistent state.
    pub mutating: bool,
}

/// One object type carried by an object-surface schema bundle.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectTypeContract {
    /// Stable object type name.
    pub name: String,
    /// Constructors exposed by the object.
    pub constructors: Vec<ObjectConstructorDescriptor>,
    /// Properties exposed by the object.
    pub properties: Vec<ObjectPropertyDescriptor>,
    /// Methods exposed by the object.
    pub methods: Vec<ObjectMethodDescriptor>,
    /// Explicit lifecycle operations when the surface exposes them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lifecycle_operations: Vec<String>,
}

/// Semantic object contract exposed by the canonical bundle.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectSemanticContract {
    /// Object types exposed by the bundle.
    pub objects: Vec<ObjectTypeContract>,
}

/// Merge reusable component definitions from one or more root schemas.
pub fn merged_components<'a>(
    schemas: impl IntoIterator<Item = &'a RootSchema>,
) -> ProviderComponentSchemas {
    let mut merged = ProviderComponentSchemas::new();
    for root in schemas {
        for (name, schema) in &root.definitions {
            merged.insert(name.clone(), schema.clone());
        }
    }
    merged
}

/// Annotations noting that the legacy UI schema is a derived compatibility view.
pub fn derived_ui_schema_annotations() -> JsonValue {
    json!({
        "ui_schema": {
            "status": "derived_compatibility_view"
        }
    })
}
