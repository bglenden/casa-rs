// SPDX-License-Identifier: LGPL-3.0-or-later

use schemars::schema::{RootSchema, Schema};
use serde_json::json;

use crate::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
    ProviderSurfaceKind, merged_components,
};

#[test]
fn provider_surface_kind_uses_snake_case_wire_names() {
    let cases = [
        (ProviderSurfaceKind::Task, "task"),
        (ProviderSurfaceKind::Session, "session"),
        (ProviderSurfaceKind::Object, "object"),
    ];

    for (kind, wire_name) in cases {
        assert_eq!(serde_json::to_value(kind).unwrap(), json!(wire_name));
        assert_eq!(
            serde_json::from_value::<ProviderSurfaceKind>(json!(wire_name)).unwrap(),
            kind
        );
    }
}

#[test]
fn cli_machine_actions_omit_absent_projection_flags() {
    let actions = ProviderCliMachineActions {
        json_schema: Some("--json-schema".to_string()),
        protocol_info: None,
        json_run: None,
        session: Some("--session".to_string()),
    };

    assert_eq!(
        serde_json::to_value(actions).unwrap(),
        json!({
            "json_schema": "--json-schema",
            "session": "--session"
        })
    );
}

#[test]
fn projection_metadata_omits_absent_projection_sections() {
    let projection = ProviderProjectionMetadata {
        cli: Some(ProviderCliProjection {
            machine_actions: ProviderCliMachineActions {
                json_schema: Some("--json-schema".to_string()),
                protocol_info: None,
                json_run: None,
                session: None,
            },
        }),
        python: None,
    };

    assert_eq!(
        serde_json::to_value(projection).unwrap(),
        json!({
            "cli": {
                "machine_actions": {
                    "json_schema": "--json-schema"
                }
            }
        })
    );
}

#[test]
fn merged_components_combines_definitions_and_later_schemas_win() {
    let mut first = RootSchema::default();
    first
        .definitions
        .insert("Shared".to_string(), Schema::Bool(false));
    first
        .definitions
        .insert("FirstOnly".to_string(), Schema::Bool(true));

    let mut second = RootSchema::default();
    second
        .definitions
        .insert("Shared".to_string(), Schema::Bool(true));
    second
        .definitions
        .insert("SecondOnly".to_string(), Schema::Bool(false));

    let merged = merged_components([&first, &second]);

    assert_eq!(merged.len(), 3);
    assert_eq!(merged.get("FirstOnly"), Some(&Schema::Bool(true)));
    assert_eq!(merged.get("SecondOnly"), Some(&Schema::Bool(false)));
    assert_eq!(merged.get("Shared"), Some(&Schema::Bool(true)));
}
