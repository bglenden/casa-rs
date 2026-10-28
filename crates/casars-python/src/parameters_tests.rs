// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;

use casa_provider_contracts::builtin_surface_bundle;

use super::*;

fn values(entries: &[(&str, ParameterValue)]) -> BTreeMap<String, ParameterValue> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect()
}

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_string())
}

fn defaults(surface_id: &str) -> SnapshotView {
    resolve(
        surface_id,
        ProfileBase::Defaults,
        None,
        BTreeMap::new(),
        BTreeSet::new(),
    )
    .expect("defaults")
}

#[test]
fn the_catalog_view_projects_every_task_surface() {
    let catalog = CatalogEnvelopeView::new(builtin_surface_catalog().expect("catalog"));
    let json = serde_json::to_value(&catalog).expect("serialize");
    assert_eq!(json["surfaces"].as_array().expect("surfaces").len(), 26);
    let imstat = BundleView::from(&parameter_bundle("imstat").expect("imstat bundle"));
    let imstat = serde_json::to_value(&imstat).expect("serialize");
    assert_eq!(imstat["surface"]["kind"], "task");
    assert!(
        imstat["surface"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .any(|binding| {
                binding["name"] == "imagename"
                    && binding["concept"]["id"] == "image.input.imagename"
            })
    );
    let image_path = imstat["catalog"]["concepts"]
        .as_array()
        .expect("concepts")
        .iter()
        .find(|concept| concept["id"] == "image.input.imagename")
        .expect("imagename concept");
    assert_eq!(
        image_path["value_domain"],
        serde_json::json!({
            "kind": "array",
            "elements": [{"kind": "path", "resource_kind": "image"}],
            "min_items": 0,
            "max_items": null,
            "allow_scalar": true,
        })
    );
    assert!(parameter_bundle("imexplore").is_err());
}

#[test]
fn resolution_keeps_override_origins_and_renders_them() {
    let defaults = defaults("imager");
    assert!(!defaults.dirty);
    assert_eq!(defaults.states["niter"].origin, "default");

    let resolved = resolve(
        "imager",
        ProfileBase::Defaults,
        None,
        values(&[
            ("vis", text("input.ms")),
            ("imagename", text("products/image")),
            ("niter", ParameterValue::Integer(7)),
        ]),
        BTreeSet::new(),
    )
    .expect("resolved parameters");
    assert!(resolved.dirty);
    assert_eq!(resolved.states["niter"].origin, "override");
    assert_eq!(
        resolved.states["niter"].value,
        Some(ParameterValue::Integer(7))
    );
    assert!(
        resolved
            .profile_toml
            .as_deref()
            .expect("profile TOML")
            .contains("niter = 7")
    );
    let unset = resolve(
        "imager",
        ProfileBase::Defaults,
        None,
        BTreeMap::new(),
        BTreeSet::from(["niter".to_string()]),
    )
    .expect("unset");
    assert_eq!(unset.states["niter"].origin, "default");
}

#[test]
fn an_unknown_base_source_names_the_choices() {
    let error = ProfileBase::parse("yesterday", None).expect_err("unknown source");
    assert!(error.contains("expected defaults, file, last, or last_successful"));
}

#[test]
fn simobserve_invocation_carries_casas_parameters() {
    let invocation =
        provider_invocation("simobserve", values(&[("antennalist", text("vla.a.cfg"))]))
            .expect("simobserve invocation");
    assert_eq!(invocation.args, ["--json-run", "-"]);
    let request: serde_json::Value =
        serde_json::from_str(invocation.stdin.as_deref().expect("stdin JSON")).unwrap();
    // The values set, which the binary resolves over CASA's defaults.
    assert_eq!(
        request,
        serde_json::json!({"surface": "simobserve", "values": {"antennalist": "vla.a.cfg"}})
    );
}

#[test]
fn imager_invocation_carries_the_imager_parameters() {
    let invocation = provider_invocation(
        "imager",
        values(&[
            ("vis", text("input.ms")),
            ("imagename", text("products/image")),
            ("parallel", ParameterValue::Bool(false)),
        ]),
    )
    .expect("imager invocation");
    assert_eq!(invocation.args, ["--json-run", "-"]);
    assert_eq!(
        invocation.protocol_name.as_deref(),
        Some("casa_imager_task")
    );
    assert_eq!(
        invocation.protocol_version,
        Some(casars_imager::IMAGER_TASK_PROTOCOL_VERSION)
    );
    let request: serde_json::Value =
        serde_json::from_str(invocation.stdin.as_deref().expect("stdin JSON")).unwrap();
    assert_eq!(request["surface"], "imager");
    assert_eq!(request["values"]["vis"], serde_json::json!(["input.ms"]));
    assert_eq!(request["values"]["imagename"], "products/image");
    assert_eq!(request["values"]["parallel"], false);
}

#[test]
fn one_profile_resolves_alike_through_the_runtime_and_the_binding() {
    let source_path = PathBuf::from("profiles/imager.toml");
    let profile_toml = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/test-profiles/imager-cross-surface.toml"
    ));
    let direct = ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle: builtin_surface_bundle("imager").expect("builtin imager surface"),
            workspace: PathBuf::from("."),
            source: BaseSource::File(source_path.clone()),
            profile_text: Some(profile_toml.to_string()),
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            managed_save: false,
        })
        .expect("direct runtime resolution");
    assert_eq!(
        direct.states()["imsize"].value,
        Some(ParameterValue::Array(vec![
            ParameterValue::Integer(12150),
            ParameterValue::Integer(12150),
        ]))
    );

    let snapshot = resolve(
        "imager",
        ProfileBase::File(source_path),
        Some(profile_toml.to_string()),
        BTreeMap::new(),
        BTreeSet::new(),
    )
    .expect("profile resolution");
    assert_eq!(
        snapshot.states["imsize"].value,
        direct.states()["imsize"].value
    );
    assert_eq!(snapshot.diagnostics.len(), direct.diagnostics().len());
    let invocation = provider_invocation(
        "imager",
        snapshot
            .states
            .iter()
            .filter_map(|(name, state)| state.value.clone().map(|value| (name.clone(), value)))
            .collect(),
    )
    .expect("Python-facing canonical provider invocation");
    let expected: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/test-profiles/imager-cross-surface.expected.json"
    )))
    .expect("shared imager request baseline");
    let request: serde_json::Value = serde_json::from_str(
        invocation
            .stdin
            .as_deref()
            .expect("canonical imager request stdin"),
    )
    .expect("canonical imager request JSON");
    assert_eq!(request, expected["request"]);
    let canonical_profile = snapshot
        .profile_toml
        .as_deref()
        .expect("canonical profile TOML");
    assert!(canonical_profile.contains("imsize = [12150, 12150]"));
    assert!(canonical_profile.contains("cell = [\"0.6arcsec\", \"0.6arcsec\"]"));
}

#[test]
fn explicit_and_managed_profiles_round_trip() {
    let temp = tempfile::tempdir().expect("tempdir");
    let draft = values(&[
        ("vis", text("input.ms")),
        ("imagename", text("products/image")),
        ("niter", ParameterValue::Integer(42)),
    ]);
    let profile_path = temp.path().join("profiles/imager.toml");
    let saved = save("imager", draft.clone(), &profile_path).expect("save profile");
    assert_eq!(saved.path, profile_path.to_string_lossy());
    assert!(saved.bytes_written > 0);
    assert_eq!(saved.managed_kind, None);

    let profile_toml = fs::read_to_string(&profile_path).expect("read profile");
    assert!(profile_toml.contains("surface = \"imager\""));
    assert!(profile_toml.contains("niter = 42"));
    assert!(!profile_toml.contains("cell ="));
    let loaded = resolve(
        "imager",
        ProfileBase::File(profile_path),
        Some(profile_toml),
        BTreeMap::new(),
        BTreeSet::new(),
    )
    .expect("load profile");
    assert_eq!(loaded.states["niter"].origin, "base_profile");
    assert_eq!(
        loaded.states["niter"].value,
        Some(ParameterValue::Integer(42))
    );

    let workspace = temp.path().join("workspace");
    let written = write_managed("imager", &workspace, draft.clone(), false).expect("write Last");
    assert!(written.path.ends_with("last.toml"), "{}", written.path);
    assert_eq!(written.managed_kind.as_deref(), Some("last"));
    let last_view = last("imager", &workspace, false)
        .expect("load Last")
        .expect("Last exists");
    assert_eq!(
        last_view.states["niter"].value,
        Some(ParameterValue::Integer(42))
    );

    assert!(
        last("imager", &workspace, true)
            .expect("look up Last Successful")
            .is_none()
    );
    write_managed("imager", &workspace, draft, true).expect("write Last Successful");
    assert!(
        last("imager", &workspace, true)
            .expect("load Last Successful")
            .is_some()
    );
    assert!(
        last("imexplore", &workspace, false).is_err(),
        "removed surfaces are unknown"
    );
}

#[test]
fn managed_lookup_distinguishes_missing_from_corrupt() {
    let workspace = tempfile::tempdir().unwrap();
    assert!(last("imager", workspace.path(), false).unwrap().is_none());
    ManagedStateStore::for_workspace(workspace.path())
        .write("imager", ManagedProfileKind::Last, "not valid profile TOML")
        .unwrap();
    let error = last("imager", workspace.path(), false)
        .expect_err("corrupt Last must not be treated as missing");
    assert!(error.contains("open parameter session"), "{error}");
}

#[test]
fn contract_words_are_snake_case_debug_names() {
    assert_eq!(
        snake_case_debug(DiagnosticCode::MissingRequired),
        "missing_required"
    );
    assert_eq!(
        snake_case_debug(ManagedProfileKind::LastSuccessful),
        "last_successful"
    );
}
