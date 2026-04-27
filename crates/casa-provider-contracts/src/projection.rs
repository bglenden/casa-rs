// SPDX-License-Identifier: LGPL-3.0-or-later

use serde::Serialize;
use serde_json::{Value as JsonValue, json};

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

/// Annotations noting that the legacy UI schema is a derived compatibility view.
pub fn derived_ui_schema_annotations() -> JsonValue {
    json!({
        "ui_schema": {
            "status": "derived_compatibility_view"
        }
    })
}
