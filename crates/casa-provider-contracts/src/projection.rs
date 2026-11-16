// SPDX-License-Identifier: LGPL-3.0-or-later

use serde::Serialize;
use serde_json::Value as JsonValue;

/// CLI machine-action projections derived from the canonical contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCliMachineActions {
    /// Canonical schema bundle action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<String>,
    /// Protocol descriptor action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_info: Option<String>,
    /// One-shot task execution action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_run: Option<String>,
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
    /// Python binding projection metadata for direct in-process object surfaces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub python: Option<JsonValue>,
}
