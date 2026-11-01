// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numeric selector text reaches the real imager boundary despite legacy normalization.

use casa_provider_contracts::{
    ParameterValue, SurfaceContractBundle, SurfaceDefinition, builtin_surface_bundle,
};
use casa_task_runtime::{
    BaseSource, DiagnosticLevel, OpenSessionRequest, ParameterOrigin, ParameterRuntime,
    ParameterSession, ResolutionPatch, TaskInvocation, parse_profile, resolve_profile,
};
use casars_imager::{ImagingParameters, resolve_request, task_request};
use serde_json::{Value, json};

fn preview_bundle() -> SurfaceContractBundle {
    let mut bundle = builtin_surface_bundle("tclean").unwrap();
    let SurfaceDefinition::Task(definition) = &mut bundle.surface;
    let mut cleared = 0;
    for binding in &mut definition.bindings {
        if matches!(binding.name.as_str(), "scan" | "observation") {
            assert_eq!(binding.unimplemented.take().unwrap().issue, 980);
            cleared += 1;
        }
    }
    assert_eq!(cleared, 2);
    bundle
}

#[test]
fn task_request_restores_original_numeric_selector_text_and_exact_empty_defaults() {
    let bundle = preview_bundle();
    let workspace = std::env::current_dir().unwrap();
    for name in ["scan", "observation"] {
        for text in [
            None,
            Some(""),
            Some("  "),
            Some("none"),
            Some("None"),
            Some(" 1 "),
            Some(",1"),
            Some("1,"),
            Some("1,,2"),
            Some("+1"),
        ] {
            let mut values =
                json!({ "vis": "unused.ms", "imagename": "unused", "cell": "1arcsec" });
            if let Some(text) = text {
                values[name] = json!(text);
            }
            let mut open = OpenSessionRequest::defaults(bundle.clone(), &workspace);
            open.managed_save = false;
            open.override_patch.values = values
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        ParameterValue::from_plain_json(value).unwrap(),
                    )
                })
                .collect();
            let session = ParameterRuntime::default().open_session(open).unwrap();
            assert!(
                session
                    .diagnostics()
                    .iter()
                    .all(|diagnostic| diagnostic.level != DiagnosticLevel::Error)
            );
            assert_eq!(
                session.given(name).map(ParameterValue::to_plain_json),
                text.map(|text| json!(text))
            );
            if text.is_none() || matches!(text, Some("" | "  " | "none")) {
                assert_eq!(
                    session.states()[name].value,
                    Some(ParameterValue::String("none".into())),
                    "legacy normalization remains unchanged"
                );
            }
            let (parameters, request) =
                task_request(&TaskInvocation::new(&session, &workspace)).unwrap();
            assert_eq!(parameters.0[name], json!(text.unwrap_or("")));
            let actual = if name == "scan" {
                request.scan
            } else {
                request.observation
            };
            assert_eq!(actual.as_deref(), text.filter(|text| !text.is_empty()));
        }
    }
}

#[test]
fn sparse_resolve_request_preserves_values_the_production_catalog_admits() {
    for text in [None, Some(""), Some("  "), Some("none")] {
        let mut values = json!({ "vis": "unused.ms", "imagename": "unused", "cell": "1arcsec" });
        if let Some(text) = text {
            values["scan"] = json!(text);
            values["observation"] = json!(text);
        }
        let (parameters, request) =
            resolve_request(&ImagingParameters(values.as_object().unwrap().clone())).unwrap();
        let expected = text.filter(|text| !text.is_empty());
        assert_eq!(request.scan.as_deref(), expected);
        assert_eq!(request.observation.as_deref(), expected);
        assert_eq!(
            parameters.0["scan"],
            Value::String(text.unwrap_or("").to_string())
        );
    }
}

fn profile_session(
    bundle: SurfaceContractBundle,
    name: &str,
    text: Option<&str>,
) -> ParameterSession {
    let selector = text.map_or_else(String::new, |text| format!("{name} = {}\n", json!(text)));
    let source = format!(
        "[casars]\nformat = 1\nsurface = \"tclean\"\nkind = \"task\"\ncontract = {}\n\n[parameters]\nvis = \"unused.ms\"\nimagename = \"unused\"\ncell = \"1arcsec\"\n{selector}",
        bundle.surface.contract_version(),
    );
    let profile = parse_profile(&source).unwrap();
    let resolved = resolve_profile(&profile, &bundle).unwrap();
    assert_eq!(
        resolved.explicit_overrides.get(name),
        text.map(|text| ParameterValue::String(text.into()))
            .as_ref()
    );
    let mut open = OpenSessionRequest::defaults(bundle, std::env::current_dir().unwrap());
    open.source = BaseSource::File("input.toml".into());
    open.profile_text = Some(source);
    open.managed_save = false;
    let session = ParameterRuntime::default().open_session(open).unwrap();
    assert_eq!(session.values(), resolved.values);
    session
}

fn assert_task_selector(session: &ParameterSession, name: &str, text: Option<&str>) {
    assert_eq!(
        session.given(name).map(ParameterValue::to_plain_json),
        text.map(|text| json!(text))
    );
    let workspace = std::env::current_dir().unwrap();
    let (parameters, request) = task_request(&TaskInvocation::new(session, &workspace)).unwrap();
    assert_eq!(parameters.0[name], json!(text.unwrap_or("")));
    let actual = if name == "scan" {
        request.scan
    } else {
        request.observation
    };
    assert_eq!(actual.as_deref(), text.filter(|text| !text.is_empty()));
}

#[test]
fn base_profile_given_text_survives_normalization_into_the_task_request() {
    let bundle = preview_bundle();
    for name in ["scan", "observation"] {
        for text in [
            None,
            Some(""),
            Some("  "),
            Some("none"),
            Some("None"),
            Some(" 1 "),
            Some(",1"),
            Some("1,"),
            Some("1,,2"),
            Some("+1"),
        ] {
            let session = profile_session(bundle.clone(), name, text);
            assert_task_selector(&session, name, text);
            if text.is_none() || matches!(text, Some("" | "  " | "none")) {
                assert_eq!(
                    session.states()[name].value,
                    Some(ParameterValue::String("none".into()))
                );
            }
        }
    }
}

#[test]
fn original_profile_context_override_and_reset_follow_the_winning_origin() {
    for name in ["scan", "observation"] {
        let mut session = profile_session(preview_bundle(), name, Some(""));
        assert_eq!(session.states()[name].origin, ParameterOrigin::BaseProfile);
        assert_task_selector(&session, name, Some(""));
        let mut context = ResolutionPatch::default();
        context
            .values
            .insert(name.into(), ParameterValue::String(" 1 ".into()));
        session.apply_context_patch(context).unwrap();
        assert_eq!(session.states()[name].origin, ParameterOrigin::Context);
        assert_task_selector(&session, name, Some(" 1 "));
        session
            .set(name, ParameterValue::String("  ".into()))
            .unwrap();
        assert_eq!(session.states()[name].origin, ParameterOrigin::Override);
        assert_task_selector(&session, name, Some("  "));
        session.reset(name).unwrap();
        assert_eq!(session.states()[name].origin, ParameterOrigin::Default);
        assert_task_selector(&session, name, None);
        session.revert().unwrap();
        assert_task_selector(&session, name, Some(" 1 "));
        session
            .apply_context_patch(ResolutionPatch::default())
            .unwrap();
        assert_task_selector(&session, name, Some(""));
    }
}

#[test]
fn sparse_export_keeps_canonical_defaults_and_padded_valid_selector_meaning() {
    for name in ["scan", "observation"] {
        for text in [None, Some(""), Some(" 1 ")] {
            let bundle = preview_bundle();
            let session = profile_session(bundle.clone(), name, text);
            let exported = session.render_sparse().unwrap();
            let parsed = parse_profile(&exported).unwrap();
            let restored = ParameterSession::from_profile(
                bundle,
                BaseSource::File("roundtrip.toml".into()),
                &parsed,
            )
            .unwrap();
            assert_eq!(session.values(), restored.values());
            let expected = text.filter(|text| !text.is_empty()).map(str::trim);
            assert_task_selector(&restored, name, expected);
            assert_eq!(parsed.parameters.contains_key(name), expected.is_some());
        }
    }
}
