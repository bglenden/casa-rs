// SPDX-License-Identifier: LGPL-3.0-or-later
//! Revision-2 selector values survive each normalized transport boundary.

use casa_imaging_application::ImagingRequest;
use casa_provider_contracts::{
    ParameterValue, SurfaceCatalogBundle, SurfaceContractBundle, TaskRunRequest,
};
use casa_task_runtime::{
    BaseSource, ParameterSession, TaskInvocation, parse_profile, project_provider_invocation,
    resolve_plain_values, resolve_profile,
};
use casars_imager::{ImagingParameters, task_request};
use serde_json::{Map, Value, json};

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
    let actual = if name == "scan" {
        &request.scan
    } else {
        &request.observation
    };
    assert_eq!(
        actual.as_deref(),
        (!expected.is_empty()).then_some(expected)
    );
}

fn assert_plain(values: &Map<String, Value>, name: &str, expected: &str) {
    assert_eq!(values[name], json!(expected));
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
        ParameterValue::String(expected.into())
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

fn current_profiles_and_provider_projection_preserve_selector_text() {
    for name in ["scan", "observation"] {
        assert_transports(&profile_session(33, "tclean", name, None), name, "");
        for &text in TEXTS {
            assert_transports(&profile_session(33, "tclean", name, Some(text)), name, text);
        }
    }
}

fn direct_current_values_preserve_selector_text_without_profile_migration() {
    for name in ["scan", "observation"] {
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

fn supported_old_profiles_migrate_only_exact_typed_none() {
    for version in 28..=32 {
        for surface in ["tclean", "imager"] {
            for name in ["scan", "observation"] {
                assert_transports(&profile_session(version, surface, name, None), name, "");
                for &text in TEXTS {
                    let expected = if text == "none" { "" } else { text };
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

// Evidence-only catalog injection: public builtins remain provider32.
fn builtin_surface_bundle(id: &str) -> Result<SurfaceContractBundle, String> {
    static CANDIDATE: std::sync::OnceLock<SurfaceCatalogBundle> = std::sync::OnceLock::new();
    let catalog = CANDIDATE.get_or_init(|| {
        let root = std::path::Path::new(
            "/private/tmp/tclean-selector-semantic-candidate-20261101/proposed",
        );
        let parameters = std::fs::read_to_string(
            root.join("crates/casa-provider-contracts/resources/parameter-catalog.json"),
        )
        .unwrap();
        let surfaces: Value = serde_json::from_str(
            &std::fs::read_to_string(
                root.join("crates/casa-provider-contracts/resources/parameter-surfaces.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let candidate = SurfaceCatalogBundle {
            schema_version: u32::try_from(surfaces["schema_version"].as_u64().unwrap()).unwrap(),
            catalog: serde_json::from_str(&parameters).unwrap(),
            surfaces: serde_json::from_value(surfaces["surfaces"].clone()).unwrap(),
        };
        candidate.validate().unwrap();
        candidate
    });
    catalog
        .embedded_surface(id)
        .ok_or_else(|| format!("missing {id}"))
}

fn resolve_request(
    parameters: &ImagingParameters,
) -> Result<(ImagingParameters, ImagingRequest), String> {
    let values = resolve_plain_values(
        builtin_surface_bundle("tclean")?,
        std::env::current_dir().unwrap(),
        &parameters.0,
    )
    .map_err(|e| e.to_string())?;
    let request = ImagingRequest::from_tclean(&values).map_err(|e| e.message)?;
    Ok((ImagingParameters(values), request))
}

fn main() {
    current_profiles_and_provider_projection_preserve_selector_text();
    direct_current_values_preserve_selector_text_without_profile_migration();
    supported_old_profiles_migrate_only_exact_typed_none();
    println!(
        "candidate33 exact-text catalog/profile/projectedJSON/re-resolution/plain-reader/sparse-roundtrip corpus PASS; public provider32 unchanged; public Python not exercised"
    );
}
