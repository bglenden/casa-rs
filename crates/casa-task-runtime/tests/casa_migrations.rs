// SPDX-License-Identifier: LGPL-3.0-or-later

//! Profiles saved before the MeasurementSet surfaces took CASA 6.7.6's
//! names and values (#931 PR 4c) load as the CASA runs they meant: the
//! old values migrate, and an omitted value adopts CASA's default with a
//! warning (ADR-0006).

use std::collections::BTreeMap;

use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{DiagnosticCode, ResolvedProfile, parse_profile, resolve_profile};

/// `parameters`, a contract-4 profile of `surface`, as it loads today.
fn load(surface: &str, parameters: &str) -> ResolvedProfile {
    let bundle = builtin_surface_bundle(surface).unwrap();
    let profile = parse_profile(&format!(
        "[casars]\nformat = 1\nsurface = \"{surface}\"\nkind = \"task\"\ncontract = 4\n\n\
         [parameters]\n{parameters}"
    ))
    .unwrap();
    resolve_profile(&profile, &bundle).unwrap()
}

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_string())
}

/// The values `resolved` holds for `names`.
fn values(resolved: &ResolvedProfile, names: &[&str]) -> BTreeMap<String, ParameterValue> {
    names
        .iter()
        .map(|name| (name.to_string(), resolved.values[*name].clone()))
        .collect()
}

/// The parameters whose defaults changed under `resolved`, as it warns.
fn changed_defaults(resolved: &ResolvedProfile) -> Vec<String> {
    resolved
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == DiagnosticCode::DefaultChanged)
        .filter_map(|diagnostic| diagnostic.parameter.clone())
        .collect()
}

#[test]
fn flagdata_unflag_action_becomes_casas_unflag_mode() {
    let resolved = load(
        "flagdata",
        "vis = \"in.ms\"\nmode = \"manual\"\naction = \"unflag\"\n",
    );
    assert_eq!(
        values(&resolved, &["mode", "action"]),
        BTreeMap::from([
            ("mode".into(), text("unflag")),
            ("action".into(), text("apply"))
        ])
    );
    assert!(
        resolved
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiagnosticCode::Migrated)
    );
}

#[test]
fn flagdata_old_values_take_casas_spellings() {
    let resolved = load(
        "flagdata",
        "vis = \"in.ms\"\nmode = \"quack\"\naction = \"flag\"\nquackmode = \"end\"\n",
    );
    assert_eq!(
        values(&resolved, &["mode", "action", "quackmode"]),
        BTreeMap::from([
            ("mode".into(), text("quack")),
            ("action".into(), text("apply")),
            // casa-rs's old `end` flagged each scan's last interval: CASA's `endb`.
            ("quackmode".into(), text("endb")),
        ])
    );
}

#[test]
fn flagdata_omitted_mode_adopts_casas_manual_with_a_warning() {
    // casa-rs's old default was summary; CASA's flags the whole selection.
    let resolved = load("flagdata", "vis = \"in.ms\"\n");
    assert_eq!(resolved.values["mode"], text("manual"));
    assert!(
        changed_defaults(&resolved).contains(&"mode".to_string()),
        "{:?}",
        resolved.diagnostics
    );
}

#[test]
fn mstransform_width_becomes_chanaverage_and_chanbin() {
    let resolved = load(
        "mstransform",
        "vis = \"in.ms\"\noutputvis = \"out.ms\"\nwidth = 4\nmsselect = \"ANTENNA1 == 0\"\n",
    );
    assert_eq!(
        values(&resolved, &["chanaverage", "chanbin", "taql", "width"]),
        BTreeMap::from([
            ("chanaverage".into(), ParameterValue::Bool(true)),
            ("chanbin".into(), ParameterValue::Integer(4)),
            ("taql".into(), text("ANTENNA1 == 0")),
            // CASA's regridding width, unimplemented, at its default.
            ("width".into(), ParameterValue::Integer(1)),
        ])
    );

    let unaveraged = load("mstransform", "vis = \"in.ms\"\noutputvis = \"out.ms\"\n");
    assert_eq!(
        unaveraged.values["chanaverage"],
        ParameterValue::Bool(false)
    );
    assert!(changed_defaults(&unaveraged).contains(&"datacolumn".to_string()));
}

#[test]
fn split_keeps_its_width_and_adopts_casas_corrected_column() {
    let resolved = load(
        "split",
        "vis = \"in.ms\"\noutputvis = \"out.ms\"\nwidth = 4\n",
    );
    assert_eq!(resolved.values["width"], ParameterValue::Integer(4));
    assert_eq!(resolved.values["datacolumn"], text("corrected"));
    assert!(changed_defaults(&resolved).contains(&"datacolumn".to_string()));
}
