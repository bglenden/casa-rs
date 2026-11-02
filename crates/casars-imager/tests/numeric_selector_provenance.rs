// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exact-text selector values (scan, observation, antenna, timerange; one
//! per MeasurementSet since contract 37) survive each normalized transport
//! boundary.

use casa_imaging_application::ImagingRequest;
use casa_provider_contracts::{ParameterValue, TaskRunRequest, builtin_surface_bundle};
use casa_task_runtime::{
    BaseSource, ParameterSession, TaskInvocation, parse_profile, project_provider_invocation,
    resolve_plain_values, resolve_profile,
};
use casars_imager::{ImagingParameters, resolve_request, task_request};
use serde_json::{Map, Value, json};

/// The selectors whose concepts keep exact text (revision 2 and later).
const SELECTORS: [&str; 4] = ["scan", "observation", "antenna", "timerange"];

const TEXTS: &[&str] = &[
    "", "  ", "none", "None", " none ", " 1 ", "1,2", "1~2", "1~1", "2~1", ",1", "1,", "1,,2",
    "+1", ">1",
];

fn profile_session(
    version: u32,
    surface: &str,
    name: &str,
    text: Option<&str>,
) -> ParameterSession {
    let selector = text.map_or_else(String::new, |text| format!("{name} = {}\n", json!(text)));
    let source = format!(
        "[casars]\nformat = 1\nsurface = {surface:?}\nkind = \"task\"\ncontract = {version}\n\n[parameters]\nvis = \"unused.ms\"\nimagename = \"unused\"\ncell = \"1arcsec\"\n{selector}",
    );
    let profile = parse_profile(&source).unwrap();
    let bundle = builtin_surface_bundle("tclean").unwrap();
    let resolved = resolve_profile(&profile, &bundle).unwrap();
    let session =
        ParameterSession::from_profile(bundle, BaseSource::File("input.toml".into()), &profile)
            .unwrap();
    assert_eq!(session.values(), resolved.values);
    session
}

fn assert_request(request: &ImagingRequest, name: &str, expected: &str) {
    let actual = match name {
        "scan" => &request.vis[0].scan,
        "observation" => &request.vis[0].observation,
        "antenna" => &request.vis[0].antenna,
        "timerange" => &request.vis[0].timerange,
        other => panic!("no exact-text selector {other}"),
    };
    assert_eq!(
        actual.as_deref(),
        (!expected.is_empty()).then_some(expected)
    );
}

fn assert_plain(values: &Map<String, Value>, name: &str, expected: &str) {
    assert_eq!(values[name], json!([expected]));
    // Deliberately use the plain resolved values: no given() restoration helper.
    assert_request(
        &ImagingRequest::from_tclean(values).unwrap(),
        name,
        expected,
    );
}

fn assert_transports(session: &ParameterSession, name: &str, expected: &str) {
    assert_eq!(
        session.values()[name],
        ParameterValue::Array(vec![ParameterValue::String(expected.into())]),
        "{name}"
    );
    let workspace = std::env::current_dir().unwrap();
    let task = TaskInvocation::new(session, &workspace);
    assert_plain(&task.plain_values(), name, expected);
    let (parameters, request) = task_request(&task).unwrap();
    assert_plain(&parameters.0, name, expected);
    assert_request(&request, name, expected);
    let invocation = project_provider_invocation(session).unwrap();
    let projected: TaskRunRequest =
        serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
    let resolved = resolve_plain_values(
        builtin_surface_bundle("tclean").unwrap(),
        workspace.clone(),
        &projected.values,
    )
    .unwrap();
    assert_plain(&resolved, name, expected);
    let (parameters, request) = resolve_request(&ImagingParameters(projected.values)).unwrap();
    assert_plain(&parameters.0, name, expected);
    assert_request(&request, name, expected);
    let exported = session.render_sparse().unwrap();
    let profile = parse_profile(&exported).unwrap();
    let restored = ParameterSession::from_profile(
        builtin_surface_bundle("tclean").unwrap(),
        BaseSource::File("roundtrip.toml".into()),
        &profile,
    )
    .unwrap();
    assert_eq!(restored.values(), session.values());
}

#[test]
fn current_profiles_and_provider_projection_preserve_selector_text() {
    for name in SELECTORS {
        assert_transports(&profile_session(37, "tclean", name, None), name, "");
        for &text in TEXTS {
            assert_transports(&profile_session(37, "tclean", name, Some(text)), name, text);
        }
    }
}

#[test]
fn direct_current_values_preserve_selector_text_without_profile_migration() {
    for name in SELECTORS {
        for &text in TEXTS {
            let mut values = json!({"vis":"unused.ms", "imagename":"unused", "cell":"1arcsec"});
            values[name] = json!(text);
            let (parameters, request) =
                resolve_request(&ImagingParameters(values.as_object().unwrap().clone())).unwrap();
            assert_plain(&parameters.0, name, text);
            assert_request(&request, name, text);
        }
    }
}

#[test]
fn supported_old_profiles_migrate_only_exact_typed_none_before_exact_text() {
    for version in 28..=36 {
        for surface in ["tclean", "imager"] {
            for name in SELECTORS {
                assert_transports(&profile_session(version, surface, name, None), name, "");
                for &text in TEXTS {
                    // Each selector's "none" was the stored absence until the
                    // contract that kept its text exact: 33 for scan and
                    // observation, 34 for antenna and timerange.
                    let exact_from = if matches!(name, "scan" | "observation") {
                        33
                    } else {
                        34
                    };
                    let expected = if text == "none" && version < exact_from {
                        ""
                    } else {
                        text
                    };
                    assert_transports(
                        &profile_session(version, surface, name, Some(text)),
                        name,
                        expected,
                    );
                }
            }
        }
    }
}
