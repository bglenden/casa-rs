// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::RefCell;
use std::ffi::OsString;
use std::path::Path;

use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskReply,
    TaskSemanticContract, builtin_surface_bundle, merged_components,
};
use serde_json::json;

use super::*;
use crate::{ManagedProfileKind, ManagedStateStore};

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct Request {
    imagename: Vec<String>,
}

/// A provider contract hosting `imstat` (the default) and `imhead`.
fn contract() -> TaskProviderContract {
    let request_schema = schemars::schema_for!(Request);
    let result_schema = schemars::schema_for!(Request);
    TaskProviderContract {
        protocol: ProviderProtocolDescriptor::new("test", 1, ProviderSurfaceKind::Task, "1"),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: vec![TaskOperationDescriptor {
                name: "run".into(),
                request_kind: "run".into(),
                result_kind: None,
            }],
            front_end: None,
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".into()),
                    protocol_info: Some("--protocol-info".into()),
                    json_run: Some("--json-run <SOURCE>".into()),
                },
            }),
            python: None,
        },
        parameter_surfaces: vec![
            builtin_surface_bundle("imstat").unwrap(),
            builtin_surface_bundle("imhead").unwrap(),
        ],
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

/// Every call the fake task received, as `surface:image`.
type Calls = RefCell<Vec<String>>;

/// A front end whose task fails for `fail.image`, refuses `refuse.image` and
/// returns the image name otherwise.
fn front_end(calls: &Calls) -> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> + '_> {
    TaskFrontEnd::new("imexplore", contract(), &["imstat", "imhead"], move |run| {
        let image = run.plain_values()["imagename"][0]
            .as_str()
            .unwrap()
            .to_string();
        calls.borrow_mut().push(format!("{}:{image}", run.surface()));
        match image.as_str() {
            "fail.image" => Err(TaskFailure::execution("the image is unreadable")),
            "refuse.image" => Err(TaskFailure::validation("no such image").at("imagename")),
            _ => TaskSuccess::new(&json!({"image": image}), &json!({"kind": "statistics"})),
        }
    })
    .unwrap()
}

fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn in_workspace(workspace: &Path, values: &[&str]) -> Vec<OsString> {
    let mut all = args(values);
    all.extend(args(&["--workspace", workspace.to_str().unwrap()]));
    all
}

fn managed(workspace: &Path, surface: &str, kind: ManagedProfileKind) -> Option<String> {
    let store = ManagedStateStore::for_workspace(workspace);
    std::fs::read_to_string(store.profile_path(surface, kind).unwrap()).ok()
}

fn last_files(workspace: &Path) -> (Option<String>, Option<String>) {
    (
        managed(workspace, "imstat", ManagedProfileKind::Last),
        managed(workspace, "imstat", ManagedProfileKind::LastSuccessful),
    )
}

#[test]
fn a_command_line_records_last_and_promotes_it_only_after_success() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();

    let ok = front_end.dispatch(&in_workspace(workspace.path(), &["first.image"]));
    assert_eq!(ok.code, 0, "{ok:?}");
    assert_eq!(ok.stdout, "{\n  \"image\": \"first.image\"\n}\n");
    let (last, successful) = last_files(workspace.path());
    assert!(last.unwrap().contains("first.image"));
    assert!(successful.unwrap().contains("first.image"));

    let failed = front_end.dispatch(&in_workspace(workspace.path(), &["fail.image"]));
    assert_eq!(failed.code, 1);
    assert_eq!(failed.stderr, ["Error: the image is unreadable"]);
    let (last, successful) = last_files(workspace.path());
    assert!(last.unwrap().contains("fail.image"));
    assert!(successful.unwrap().contains("first.image"));

    let refused = front_end.dispatch(&in_workspace(workspace.path(), &["refuse.image"]));
    assert_eq!(refused.code, 2);
    assert!(last_files(workspace.path()).1.unwrap().contains("first.image"));
}

#[test]
fn no_save_last_leaves_existing_last_files_untouched() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    front_end.dispatch(&in_workspace(workspace.path(), &["first.image"]));
    let before = last_files(workspace.path());

    for image in ["second.image", "fail.image"] {
        let output = front_end.dispatch(&in_workspace(
            workspace.path(),
            &[image, "--no-save-last"],
        ));
        assert_ne!(output.code, 2, "{output:?}");
        assert_eq!(last_files(workspace.path()), before, "{image}");
    }
    assert_eq!(calls.borrow().len(), 3);
}

#[test]
fn json_run_writes_one_typed_reply_and_no_last() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    front_end.dispatch(&in_workspace(workspace.path(), &["first.image"]));
    let before = last_files(workspace.path());

    let request = |values: serde_json::Value| {
        let path = workspace.path().join("request.json");
        std::fs::write(&path, values.to_string()).unwrap();
        front_end.dispatch(&args(&["--json-run", path.to_str().unwrap()]))
    };
    let reply = |output: &FrontEndOutput| {
        assert!(output.stderr.is_empty(), "{output:?}");
        serde_json::from_str::<TaskReply>(&output.stdout).unwrap()
    };

    let ok = request(json!({"surface": "imhead", "values": {"imagename": ["x.image"]}}));
    assert_eq!(ok.code, 0);
    assert_eq!(
        reply(&ok),
        TaskReply::Succeeded {
            surface: "imhead".into(),
            value: json!({"image": "x.image"}),
            result: json!({"kind": "statistics"}),
        }
    );
    let failed = request(json!({"surface": "imstat", "values": {"imagename": ["fail.image"]}}));
    assert_eq!(failed.code, 1);
    assert!(matches!(
        reply(&failed),
        TaskReply::Failed { kind: TaskFailureKind::Execution, .. }
    ));
    let refused = request(json!({"surface": "imstat", "values": {"imagename": ["refuse.image"]}}));
    assert_eq!(refused.code, 2);
    assert!(matches!(
        reply(&refused),
        TaskReply::Failed { kind: TaskFailureKind::Validation, parameter: Some(ref name), .. }
            if name == "imagename"
    ));
    assert_eq!(last_files(workspace.path()), before);
    assert_eq!(classify_task_exit(TaskExit { code: Some(ok.code), signal: None }, &ok.stdout),
        TaskOutcome::Succeeded {
            surface: "imhead".into(),
            value: json!({"image": "x.image"}),
            result: json!({"kind": "statistics"}),
        });
}

#[test]
fn json_run_refuses_what_it_cannot_resolve_before_the_task_runs() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("request.json");
    for (request, fault) in [
        ("not json".to_string(), None),
        (json!({"surface": "split", "values": {}}).to_string(), None),
        (
            json!({"surface": "imstat", "values": {"imagename": ["x.image"], "widht": 2}})
                .to_string(),
            None,
        ),
        (
            json!({"surface": "imstat", "values": {"imagename": null}}).to_string(),
            Some("imagename"),
        ),
        (json!({"surface": "imstat", "values": {}}).to_string(), Some("imagename")),
    ] {
        std::fs::write(&path, &request).unwrap();
        let output = front_end.dispatch(&args(&["--json-run", path.to_str().unwrap()]));
        assert_eq!(output.code, 2, "{request}: {output:?}");
        let TaskReply::Failed { kind, parameter, .. } = serde_json::from_str(&output.stdout).unwrap()
        else {
            panic!("{request}: expected a refusal");
        };
        assert_eq!(kind, TaskFailureKind::Validation, "{request}");
        if let Some(fault) = fault {
            assert_eq!(parameter.as_deref(), Some(fault), "{request}");
        }
    }
    let wrong = front_end.dispatch(&args(&["--json-run", path.to_str().unwrap(), "x.image"]));
    assert_eq!(wrong.code, 2);
    assert!(calls.borrow().is_empty());
}

#[test]
fn the_first_word_selects_a_hosted_surface() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    for (line, call) in [
        (vec!["x.image"], "imstat:x.image"),
        (vec!["imstat", "x.image"], "imstat:x.image"),
        (vec!["imhead", "x.image"], "imhead:x.image"),
    ] {
        let mut line = line;
        line.push("--no-save-last");
        let output = front_end.dispatch(&in_workspace(workspace.path(), &line));
        assert_eq!(output.code, 0, "{output:?}");
        assert_eq!(calls.borrow().last().unwrap(), call);
    }
}

#[test]
fn rejected_arguments_never_reach_the_task_or_last() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    let corrupt = workspace.path().join("corrupt.toml");
    std::fs::write(&corrupt, "[casars\nsurface = ").unwrap();
    let missing = workspace.path().join("missing.toml");
    let cases: [(&[&str], &str); 6] = [
        (&["x.image", "--confirm-overwrite"], "--confirm-overwrite"),
        (&["x.image", "--defaults", "--last"], "mutually exclusive"),
        (&["--last"], "no managed Last profile"),
        (&["--params", missing.to_str().unwrap()], "missing.toml"),
        (&["--params", corrupt.to_str().unwrap()], ""),
        (&[], "imagename"),
    ];
    for (line, message) in cases {
        let output = front_end.dispatch(&in_workspace(workspace.path(), line));
        assert_eq!(output.code, 2, "{line:?}: {output:?}");
        assert!(output.stderr.join("\n").contains(message), "{line:?}: {output:?}");
    }
    assert!(calls.borrow().is_empty());
    assert_eq!(last_files(workspace.path()), (None, None));

    // A corrupt Last is reported, never replaced by Defaults or rewritten.
    let store = ManagedStateStore::for_workspace(workspace.path());
    let last = store.profile_path("imstat", ManagedProfileKind::Last).unwrap();
    std::fs::create_dir_all(last.parent().unwrap()).unwrap();
    std::fs::write(&last, "not a profile").unwrap();
    let output = front_end.dispatch(&in_workspace(workspace.path(), &["--last", "x.image"]));
    assert_eq!(output.code, 2, "{output:?}");
    assert_eq!(std::fs::read_to_string(&last).unwrap(), "not a profile");
    assert!(calls.borrow().is_empty());
}

#[test]
fn non_executing_views_run_nothing_and_write_no_last() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    let saved = workspace.path().join("saved.toml");

    let show = front_end.dispatch(&in_workspace(
        workspace.path(),
        &["--show", "x.image", "--save-params", saved.to_str().unwrap()],
    ));
    assert_eq!(show.code, 0, "{show:?}");
    assert!(show.stdout.contains("imagename"), "{}", show.stdout);
    assert!(show.stdout.contains("Override"), "{}", show.stdout);
    assert!(std::fs::read_to_string(&saved).unwrap().contains("x.image"));

    let validate = front_end.dispatch(&args(&["--validate", saved.to_str().unwrap()]));
    assert_eq!(validate.code, 0, "{validate:?}");
    assert!(validate.stdout.starts_with("valid task imstat profile"));
    let other = front_end.dispatch(&args(&["imhead", "--validate", saved.to_str().unwrap()]));
    assert_eq!(other.code, 2);

    let template = front_end.dispatch(&args(&["imhead", "--template"]));
    assert!(template.stdout.contains("surface = \"imhead\""), "{}", template.stdout);
    let surface = front_end.dispatch(&args(&["--describe"]));
    assert!(surface.stdout.starts_with("task imstat"), "{}", surface.stdout);
    let parameter = front_end.dispatch(&args(&["--describe", "imagename"]));
    assert!(parameter.stdout.contains("CASA name: imagename"), "{}", parameter.stdout);
    let combined = front_end.dispatch(&args(&["--show", "--template"]));
    assert_eq!(combined.code, 2);
    let extra = front_end.dispatch(&args(&["--template", "x.image"]));
    assert_eq!(extra.code, 2);

    assert!(calls.borrow().is_empty());
    assert_eq!(last_files(workspace.path()), (None, None));
}

#[test]
fn help_and_machine_actions_describe_the_hosted_surfaces() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let help = front_end.dispatch(&args(&["imhead", "--help"]));
    assert_eq!(help.code, 0);
    assert!(help.stdout.contains("SURFACE is one of imstat, imhead"), "{}", help.stdout);
    assert!(help.stdout.contains("Parameters of imhead:"), "{}", help.stdout);
    assert!(help.stdout.contains("--last-successful"), "{}", help.stdout);

    let schema: serde_json::Value =
        serde_json::from_str(&front_end.dispatch(&args(&["--json-schema"])).stdout).unwrap();
    assert!(schema["semantic"]["front_end"]["reply_schema"].is_object());
    assert!(schema["semantic"]["front_end"]["run_request_schema"].is_object());
    let protocol: serde_json::Value =
        serde_json::from_str(&front_end.dispatch(&args(&["--protocol-info"])).stdout).unwrap();
    assert_eq!(protocol["protocol_name"], "test");
    assert!(calls.borrow().is_empty());
}

#[test]
fn a_front_end_hosts_only_surfaces_its_contract_carries() {
    let error = TaskFrontEnd::new("x", contract(), &["split"], |_: &TaskInvocation<'_>| {
        TaskSuccess::new(&(), &())
    })
    .err()
    .unwrap();
    assert!(error.contains("split"), "{error}");
}
