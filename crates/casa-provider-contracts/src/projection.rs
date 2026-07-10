// SPDX-License-Identifier: LGPL-3.0-or-later

use serde::Serialize;
use serde_json::{Value as JsonValue, json};

use crate::{
    DefaultSpec, ParameterType, ParameterValue, SurfaceContractBundle, SurfaceParameterBinding,
};

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

/// Project a canonical surface definition into the legacy launcher form shape.
///
/// This is a one-way compatibility projection. Parameter names, types,
/// defaults, concepts, and private provider mappings remain authoritative in
/// the embedded [`SurfaceContractBundle`]. Provider crates must not carry a
/// separately authored UI parameter schema.
pub fn project_ui_schema(bundle: &SurfaceContractBundle) -> JsonValue {
    let surface = &bundle.surface;
    let mut bindings = surface.bindings().iter().collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut arguments = bindings
        .into_iter()
        .map(|binding| project_binding(bundle, binding))
        .collect::<Vec<_>>();
    arguments.push(json!({
        "id": "help",
        "label": "Help",
        "order": 1_000_000,
        "parser": {"kind": "action", "flags": ["-h", "--help"], "action": "help"},
        "value_kind": "none",
        "required": false,
        "default": null,
        "help": "Show help generated from the canonical surface definition.",
        "group": "Meta",
        "advanced": true,
        "hidden_in_tui": false
    }));
    arguments.push(json!({
        "id": "ui_schema",
        "label": "UI Schema",
        "order": 1_000_001,
        "parser": {"kind": "action", "flags": ["--ui-schema"], "action": "ui_schema"},
        "value_kind": "none",
        "required": false,
        "default": null,
        "help": "Show the derived UI projection of the canonical surface definition.",
        "group": "Meta",
        "advanced": true,
        "hidden_in_tui": false
    }));
    json!({
        "schema_version": 2,
        "command_id": surface.id(),
        "invocation_name": surface.execution().invocation_name,
        "display_name": surface.display_name(),
        "category": surface.category(),
        "summary": surface.summary(),
        "usage": format!("{} [parameters]", surface.execution().invocation_name),
        "arguments": arguments,
        "managed_output": surface.execution().managed_output
    })
}

fn project_binding(bundle: &SurfaceContractBundle, binding: &SurfaceParameterBinding) -> JsonValue {
    let concept = bundle
        .catalog
        .concept(&binding.concept)
        .expect("validated surface bundle contains referenced concept");
    let cli = binding.projections.cli.as_ref();
    let parser = if cli.and_then(|projection| projection.positional).is_some() {
        json!({
            "kind": "positional",
            "metavar": cli.and_then(|projection| projection.metavar.clone()).unwrap_or_else(|| binding.name.to_ascii_uppercase())
        })
    } else if is_bool(&concept.value_domain) {
        json!({
            "kind": "toggle",
            "true_flags": cli.map(|projection| projection.flags.clone()).unwrap_or_default(),
            "false_flags": cli.map(|projection| projection.false_flags.clone()).unwrap_or_default()
        })
    } else {
        json!({
            "kind": "option",
            "flags": cli.map(|projection| projection.flags.clone()).unwrap_or_else(|| vec![format!("--{}", binding.name.replace('_', "-"))]),
            "metavar": cli.and_then(|projection| projection.metavar.clone()).unwrap_or_else(|| binding.name.to_ascii_uppercase()),
            "choices": choices(&concept.value_domain)
        })
    };
    json!({
        "id": binding.name,
        "label": binding.projections.presentation.label,
        "order": binding.order,
        "parser": parser,
        "value_kind": value_kind(&concept.value_domain),
        "parameter_type": parameter_type_name(&concept.value_domain),
        "concept_id": concept.id,
        "concept_revision": concept.semantic_revision.0,
        "unit_dimension": concept.unit_dimension,
        "context_role": binding.context_role,
        "required": matches!(binding.default, DefaultSpec::Required),
        "default": projected_default(&binding.default),
        "help": binding.surface_note.as_ref().map_or_else(|| concept.documentation.summary.clone(), |note| format!("{} {note}", concept.documentation.summary)),
        "group": binding.projections.presentation.group,
        "advanced": binding.projections.presentation.advanced,
        "hidden_in_tui": binding.projections.presentation.hidden
    })
}

fn projected_default(default: &DefaultSpec) -> JsonValue {
    let value = match default {
        DefaultSpec::Required => return JsonValue::Null,
        DefaultSpec::Literal { value } => value,
        DefaultSpec::Conditional { fallback, .. } => fallback,
    };
    JsonValue::String(projected_value(value))
}

fn projected_value(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Bool(value) => value.to_string(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => value.to_string(),
        ParameterValue::String(value) => value.clone(),
        ParameterValue::Array(values) => values
            .iter()
            .map(projected_value)
            .collect::<Vec<_>>()
            .join(","),
        ParameterValue::Table(values) => serde_json::to_string(values).unwrap_or_default(),
    }
}

fn is_bool(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Bool => true,
        ParameterType::Optional { value, .. } => is_bool(value),
        _ => false,
    }
}

fn choices(domain: &ParameterType) -> Vec<String> {
    match domain {
        ParameterType::Choice { values } => values.clone(),
        ParameterType::Optional { value, states }
            if matches!(value.as_ref(), ParameterType::Choice { .. }) =>
        {
            let mut values = choices(value);
            values.extend(states.iter().cloned());
            values.sort();
            values.dedup();
            values
        }
        ParameterType::Optional { .. } => Vec::new(),
        _ => Vec::new(),
    }
}

fn value_kind(domain: &ParameterType) -> &'static str {
    match domain {
        ParameterType::Bool => "bool",
        ParameterType::Integer | ParameterType::Float => "float",
        ParameterType::Path { .. } => "path",
        ParameterType::Choice { .. } => "choice",
        ParameterType::Array { element, .. }
            if matches!(element.as_ref(), ParameterType::Path { .. }) =>
        {
            "path"
        }
        ParameterType::Optional { value, .. } => value_kind(value),
        _ => "string",
    }
}

fn parameter_type_name(domain: &ParameterType) -> &'static str {
    match domain {
        ParameterType::Bool => "boolean",
        ParameterType::Integer => "integer",
        ParameterType::Float => "number",
        ParameterType::String => "string",
        ParameterType::Path { .. } => "path",
        ParameterType::Choice { .. } => "choice",
        ParameterType::Quantity { .. } => "quantity",
        ParameterType::Array { .. } => "array",
        ParameterType::Table { .. } => "table",
        ParameterType::Optional { .. } => "optional",
    }
}
