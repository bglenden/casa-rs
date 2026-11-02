// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::RefCell;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

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
    contract_hosting(&["imstat", "imhead"])
}

/// A provider contract carrying the builtin `surfaces`.
fn contract_hosting(surfaces: &[&str]) -> TaskProviderContract {
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
        parameter_surfaces: surfaces
            .iter()
            .map(|id| builtin_surface_bundle(id).unwrap())
            .collect(),
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
fn front_end(
    calls: &Calls,
) -> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> + '_> {
    TaskFrontEnd::new("imexplore", contract(), &["imstat", "imhead"], move |run| {
        let image = run.plain_values()["imagename"][0]
            .as_str()
            .unwrap()
            .to_string();
        calls
            .borrow_mut()
            .push(format!("{}:{image}", run.surface()));
        match image.as_str() {
            "fail.image" => Err(TaskFailure::execution("the image is unreadable")),
            "refuse.image" => Err(TaskFailure::validation("no such image").at("imagename")),
            _ => TaskSuccess::new(
                CasaValue::dict([("image", CasaValue::string(image))]),
                &json!({"kind": "statistics"}),
            ),
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
    // Astra's #962 F5: a command line names the parameter at fault, as the
    // reply's `parameter` does, when the message does not.
    assert_eq!(
        refused.stderr,
        ["Error: parameter \"imagename\": no such image"]
    );
    assert!(
        last_files(workspace.path())
            .1
            .unwrap()
            .contains("first.image")
    );
}

#[test]
fn no_save_last_leaves_existing_last_files_untouched() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    front_end.dispatch(&in_workspace(workspace.path(), &["first.image"]));
    let before = last_files(workspace.path());

    for image in ["second.image", "fail.image"] {
        let output =
            front_end.dispatch(&in_workspace(workspace.path(), &[image, "--no-save-last"]));
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
            value: CasaValue::dict([("image", CasaValue::string("x.image"))]),
            result: json!({"kind": "statistics"}),
        }
    );
    let failed = request(json!({"surface": "imstat", "values": {"imagename": ["fail.image"]}}));
    assert_eq!(failed.code, 1);
    assert!(matches!(
        reply(&failed),
        TaskReply::Failed {
            kind: TaskFailureKind::Execution,
            ..
        }
    ));
    let refused = request(json!({"surface": "imstat", "values": {"imagename": ["refuse.image"]}}));
    assert_eq!(refused.code, 2);
    assert!(matches!(
        reply(&refused),
        TaskReply::Failed { kind: TaskFailureKind::Validation, parameter: Some(ref name), .. }
            if name == "imagename"
    ));
    assert_eq!(last_files(workspace.path()), before);
    assert_eq!(
        classify_task_exit(
            TaskExit {
                code: Some(ok.code),
                signal: None
            },
            &ok.stdout
        ),
        TaskOutcome::Succeeded {
            surface: "imhead".into(),
            value: CasaValue::dict([("image", CasaValue::string("x.image"))]),
            result: json!({"kind": "statistics"}),
        }
    );
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
            Some("widht"),
        ),
        // A value of the wrong type is named, though the session would
        // have refused it unnamed.
        (
            json!({"surface": "imstat", "values": {"imagename": ["x.image"], "box": 5}})
                .to_string(),
            Some("box"),
        ),
        (
            json!({"surface": "imstat", "values": {"imagename": 7}}).to_string(),
            Some("imagename"),
        ),
        (
            json!({"surface": "imstat", "values": {"imagename": null}}).to_string(),
            Some("imagename"),
        ),
        (
            json!({"surface": "imstat", "values": {}}).to_string(),
            Some("imagename"),
        ),
    ] {
        std::fs::write(&path, &request).unwrap();
        let output = front_end.dispatch(&args(&["--json-run", path.to_str().unwrap()]));
        assert_eq!(output.code, 2, "{request}: {output:?}");
        let TaskReply::Failed {
            kind, parameter, ..
        } = serde_json::from_str(&output.stdout).unwrap()
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
        assert!(
            output.stderr.join("\n").contains(message),
            "{line:?}: {output:?}"
        );
    }
    assert!(calls.borrow().is_empty());
    assert_eq!(last_files(workspace.path()), (None, None));

    // A corrupt Last is reported, never replaced by Defaults or rewritten.
    let store = ManagedStateStore::for_workspace(workspace.path());
    let last = store
        .profile_path("imstat", ManagedProfileKind::Last)
        .unwrap();
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
        &[
            "--show",
            "x.image",
            "--save-params",
            saved.to_str().unwrap(),
        ],
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
    assert!(
        template.stdout.contains("surface = \"imhead\""),
        "{}",
        template.stdout
    );
    let surface = front_end.dispatch(&args(&["--describe"]));
    assert!(
        surface.stdout.starts_with("task imstat"),
        "{}",
        surface.stdout
    );
    let parameter = front_end.dispatch(&args(&["--describe", "imagename"]));
    assert!(
        parameter.stdout.contains("CASA name: imagename"),
        "{}",
        parameter.stdout
    );
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
    assert!(
        help.stdout.contains("SURFACE is one of imstat, imhead"),
        "{}",
        help.stdout
    );
    assert!(
        help.stdout.contains("Parameters of imhead:"),
        "{}",
        help.stdout
    );
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
fn typed_reads_follow_the_surface_and_name_the_parameter_at_fault() {
    let mut session =
        crate::ParameterSession::defaults(builtin_surface_bundle("immath").unwrap()).unwrap();
    for (name, value) in [
        (
            "imagename",
            casa_provider_contracts::ParameterValue::Array(vec![
                casa_provider_contracts::ParameterValue::String("a.image".into()),
                casa_provider_contracts::ParameterValue::String("b.image".into()),
            ]),
        ),
        (
            "expr",
            casa_provider_contracts::ParameterValue::String("IM0+IM1".into()),
        ),
        (
            "outfile",
            casa_provider_contracts::ParameterValue::String("sum.image".into()),
        ),
    ] {
        session.set(name, value).unwrap();
    }
    let task = TaskInvocation {
        session: &session,
        workspace: Path::new("."),
    };
    assert_eq!(task.paths("imagename").unwrap().len(), 2);
    let many = task.path("imagename").unwrap_err();
    assert_eq!(many.parameter.as_deref(), Some("imagename"));
    assert_eq!(task.path("outfile").unwrap(), Path::new("sum.image"));
    assert_eq!(task.required_text("expr").unwrap(), "IM0+IM1");
    assert!(!task.flag("overwrite").unwrap());
    let wrong = task.integer("overwrite").unwrap_err();
    assert_eq!(wrong.kind, TaskFailureKind::Validation);
    assert!(task.value("no_such_parameter").is_none());

    // An adapter's explicit absence ("none") reads as no value.
    let imstat =
        crate::ParameterSession::defaults(builtin_surface_bundle("imstat").unwrap()).unwrap();
    let task = TaskInvocation {
        session: &imstat,
        workspace: Path::new("."),
    };
    assert_eq!(task.text("box").unwrap(), None);
    assert!(task.required_text("box").is_err());

    // Astra's #962 P2: an optional number (flagdata's thresholds) was read
    // as text, refusing every explicit value.
    let mut flagdata =
        crate::ParameterSession::defaults(builtin_surface_bundle("flagdata").unwrap()).unwrap();
    {
        let task = TaskInvocation {
            session: &flagdata,
            workspace: Path::new("."),
        };
        assert_eq!(task.optional_number("timedev").unwrap(), None);
    }
    flagdata
        .set(
            "timedev",
            casa_provider_contracts::ParameterValue::Float(1.5),
        )
        .unwrap();
    flagdata
        .set(
            "freqdev",
            casa_provider_contracts::ParameterValue::String("none".into()),
        )
        .unwrap();
    let task = TaskInvocation {
        session: &flagdata,
        workspace: Path::new("."),
    };
    assert_eq!(task.optional_number("timedev").unwrap(), Some(1.5));
    assert_eq!(task.optional_number("freqdev").unwrap(), None);
    let wrong = task.optional_number("mode").unwrap_err();
    assert_eq!(wrong.parameter.as_deref(), Some("mode"));
}

/// A stderr a test hands `main` and reads back.
#[derive(Clone, Default)]
struct SharedBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn quiet_logging() -> Result<CasaLoggingConfig, CasaLoggingError> {
    Ok(CasaLoggingConfig {
        table_path: None,
        table_priority: casa_logging::CasaPriority::Info,
        stderr_priority: Some(casa_logging::CasaPriority::Info),
    })
}

fn bad_logging() -> Result<CasaLoggingConfig, CasaLoggingError> {
    Err(CasaLoggingError::InvalidEnvPriority {
        name: "CASA_RS_LOG_TABLE_PRIORITY",
        value: "INVALID".into(),
    })
}

/// Enters no workspace: a test shares its process directory.
fn stay(_: PathBuf) -> std::io::Result<()> {
    Ok(())
}

thread_local! {
    static ENTERED: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

/// Records the workspace a run enters.
fn record_entry(workspace: PathBuf) -> std::io::Result<()> {
    ENTERED.with_borrow_mut(|entered| entered.push(workspace));
    Ok(())
}

fn refuse_entry(_: PathBuf) -> std::io::Result<()> {
    Err(std::io::Error::other("permission denied"))
}

#[test]
fn a_command_line_run_enters_its_absolute_workspace_before_the_task() {
    // Astra's #956 P1: a binary started directly resolved a run's relative
    // paths against the process directory, not its --workspace.
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    let entered = || ENTERED.with_borrow_mut(std::mem::take);

    let ran = front_end.dispatch_entering(
        &in_workspace(workspace.path(), &["x.image"]),
        Some(record_entry),
    );
    assert_eq!(ran.code, 0, "{ran:?}");
    assert_eq!(entered(), [std::path::absolute(workspace.path()).unwrap()]);
    assert!(last_files(workspace.path()).1.is_some());

    // Nothing but a command-line run enters a workspace.
    let request = workspace.path().join("request.json");
    std::fs::write(
        &request,
        r#"{"surface":"imstat","values":{"imagename":"x.image"}}"#,
    )
    .unwrap();
    for line in [
        args(&["--json-run", request.to_str().unwrap()]),
        in_workspace(workspace.path(), &["--show", "x.image"]),
        args(&["--template"]),
    ] {
        let output = front_end.dispatch_entering(&line, Some(record_entry));
        assert_eq!(output.code, 0, "{line:?}: {output:?}");
    }
    assert!(entered().is_empty());

    // A workspace it cannot enter refuses the run before the task or Last;
    // the default workspace, the current directory, is named absolute.
    let before = last_files(workspace.path());
    for (line, named) in [
        (
            in_workspace(workspace.path(), &["other.image"]),
            workspace.path().to_path_buf(),
        ),
        (args(&["other.image"]), std::env::current_dir().unwrap()),
    ] {
        let refused = front_end.dispatch_entering(&line, Some(refuse_entry));
        assert_eq!(refused.code, 2, "{line:?}");
        assert_eq!(
            refused.stderr,
            [format!(
                "Error: cannot enter workspace {}: permission denied",
                named.display()
            )]
        );
    }
    assert_eq!(last_files(workspace.path()), before);
    assert_eq!(*calls.borrow(), ["imstat:x.image", "imstat:x.image"]);
}

#[test]
fn main_writes_only_to_the_streams_its_binary_passes() {
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let (mut stdout, stderr) = (Vec::new(), SharedBuffer::default());
    let code = front_end.main(
        args(&["--template", "x.image"]),
        quiet_logging(),
        stay,
        &mut stdout,
        stderr.clone(),
    );
    assert_eq!(code, std::process::ExitCode::from(2));
    assert!(stdout.is_empty());
    assert!(
        stderr
            .text()
            .contains("--template takes no other arguments")
    );
}

#[test]
fn a_bad_logging_configuration_refuses_only_a_run() {
    // Astra's #947 S1 probe: CASA_RS_LOG_TABLE_PRIORITY=INVALID failed a
    // non-executing --template before dispatch.
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    let main = |line: Vec<OsString>| {
        let (mut stdout, stderr) = (Vec::new(), SharedBuffer::default());
        let code = front_end.main(line, bad_logging(), stay, &mut stdout, stderr.clone());
        (code, String::from_utf8(stdout).unwrap(), stderr.text())
    };

    for line in [
        args(&["--template"]),
        args(&["--describe"]),
        args(&["--json-schema"]),
        args(&["--log-stderr-priority", "INFO", "--show", "x.image"]),
    ] {
        let (code, stdout, stderr) = main(line.clone());
        assert_eq!(code, std::process::ExitCode::SUCCESS, "{line:?}: {stderr}");
        assert!(!stdout.is_empty(), "{line:?}");
    }

    let (code, stdout, stderr) = main(in_workspace(workspace.path(), &["x.image"]));
    assert_eq!(code, std::process::ExitCode::from(2));
    assert!(stdout.is_empty());
    assert!(
        stderr.contains("failed to initialize logging: invalid CASA_RS_LOG_TABLE_PRIORITY"),
        "{stderr}"
    );

    let request = workspace.path().join("request.json");
    std::fs::write(
        &request,
        r#"{"surface":"imstat","values":{"imagename":"x.image"}}"#,
    )
    .unwrap();
    let (code, stdout, _) = main(args(&["--json-run", request.to_str().unwrap()]));
    assert_eq!(code, std::process::ExitCode::from(2));
    let reply: TaskReply = serde_json::from_str(&stdout).unwrap();
    assert!(
        matches!(&reply, TaskReply::Failed { kind: TaskFailureKind::Validation, message, .. }
            if message.contains("failed to initialize logging")),
        "{reply:?}"
    );

    // A malformed logging flag is refused on every action.
    let (code, _, stderr) = main(args(&["--template", "--log-table"]));
    assert_eq!(code, std::process::ExitCode::from(2));
    assert!(stderr.contains("missing value for --log-table"), "{stderr}");

    assert!(calls.borrow().is_empty());
    assert_eq!(last_files(workspace.path()), (None, None));
}

const LOGGING_CHILD: &str = "CASA_RS_FRONT_END_LOGGING_CHILD";

/// Runs only as the child of the test below, which owns its process: a run
/// installs the process's global subscriber.
#[test]
fn a_failed_run_logged_in_a_child_process() {
    let Some(report) = std::env::var_os(LOGGING_CHILD) else {
        return;
    };
    let calls = Calls::default();
    let front_end = front_end(&calls);
    let workspace = tempfile::tempdir().unwrap();
    let (mut stdout, stderr) = (Vec::new(), SharedBuffer::default());
    let code = front_end.main(
        in_workspace(workspace.path(), &["fail.image", "--no-save-last"]),
        quiet_logging(),
        stay,
        &mut stdout,
        stderr.clone(),
    );
    assert_eq!(code, std::process::ExitCode::from(1));
    std::fs::write(report, stderr.text()).unwrap();
}

#[test]
fn a_runs_log_reaches_only_the_stderr_its_binary_passes() {
    // Astra's #947 S1 probe: the front end's SEVERE event reached the
    // process's stderr, not the buffer `main` was handed.
    if std::env::var_os(LOGGING_CHILD).is_some() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let report = directory.path().join("stderr.txt");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "front_end::tests::a_failed_run_logged_in_a_child_process",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env(LOGGING_CHILD, &report)
        .output()
        .unwrap();
    let process_stderr = String::from_utf8_lossy(&child.stderr);
    assert!(child.status.success(), "{process_stderr}");
    let supplied = std::fs::read_to_string(&report).expect("the child ran");
    assert!(supplied.contains("SEVERE"), "{supplied}");
    assert!(supplied.contains("imstat failed"), "{supplied}");
    assert!(supplied.contains("INFO"), "{supplied}");
    assert!(
        supplied.contains("Error: the image is unreadable"),
        "{supplied}"
    );
    assert!(!process_stderr.contains("SEVERE"), "{process_stderr}");
    assert!(!process_stderr.contains("imstat"), "{process_stderr}");
}

/// A front end hosting imregrid, recording each run's template.
fn imregrid_front_end(
    templates: &Calls,
) -> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> + '_> {
    TaskFrontEnd::new(
        "imregrid",
        contract_hosting(&["imregrid"]),
        &["imregrid"],
        move |run| {
            let template = run.plain_values()["template"].as_str().unwrap().to_string();
            templates.borrow_mut().push(template);
            TaskSuccess::new(CasaValue::None, &())
        },
    )
    .unwrap()
}

#[test]
fn imregrids_template_reaches_its_parameter_through_full_dispatch() {
    // Astra's #947 F1 probe: dispatch took --template for the profile.
    let templates = Calls::default();
    let front_end = imregrid_front_end(&templates);
    let workspace = tempfile::tempdir().unwrap();
    for spelling in [
        &["--template", "target.image"][..],
        &["--template=target.image"][..],
    ] {
        let mut line = args(&["--imagename", "input.image"]);
        line.extend(args(spelling));
        line.extend(args(&["--outfile", "out.image", "--no-save-last"]));
        line.extend(args(&["--workspace", workspace.path().to_str().unwrap()]));
        let ran = front_end.dispatch(&line);
        assert_eq!(ran.code, 0, "{spelling:?}: {ran:?}");
    }
    assert_eq!(*templates.borrow(), ["target.image", "target.image"]);

    let shown = front_end.dispatch(&args(&["--show", "--template", "target.image"]));
    assert_eq!(shown.code, 0, "{shown:?}");
    assert!(shown.stdout.contains("target.image"), "{}", shown.stdout);
    let profile = front_end.dispatch(&args(&["--profile-template"]));
    assert_eq!(profile.code, 0, "{profile:?}");
    assert!(profile.stdout.contains("surface = \"imregrid\""));
    let help = front_end.dispatch(&args(&["--help"]));
    assert!(help.stdout.contains("imregrid --profile-template |"));
    assert!(!help.stdout.contains("--template |"), "{}", help.stdout);
    assert_eq!(templates.borrow().len(), 2);
}

/// Every spelling the front end owns, with a value where it takes one.
const FRONT_END_SPELLINGS: &[&str] = &[
    "--help",
    "-h",
    "--json-schema",
    "--protocol-info",
    "--json-run",
    "--template",
    "--profile-template",
    "--describe",
    "--validate",
    "--show",
    "--defaults",
    "--last",
    "--last-successful",
    "--params",
    "--workspace",
    "--save-params",
    "--no-save-last",
    "--unset",
];

#[test]
fn every_front_end_spelling_does_its_action_through_full_dispatch_on_every_surface() {
    // Only imregrid declares one of the front end's spellings; a surface that
    // starts to must make the same visible choice here.
    let collisions = [("imregrid", "--template")];
    let catalog = casa_provider_contracts::builtin_surface_catalog().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let directory = workspace.path().to_str().unwrap();
    let missing = workspace.path().join("missing.toml");
    let missing = missing.to_str().unwrap();
    let saved = workspace.path().join("saved.toml");
    let saved = saved.to_str().unwrap();
    for surface in &catalog.surfaces {
        let id = surface.id();
        let ran = std::cell::Cell::new(false);
        let front_end = TaskFrontEnd::new(
            id,
            contract_hosting(&[id]),
            &[id],
            |_: &TaskInvocation<'_>| {
                ran.set(true);
                TaskSuccess::new(CasaValue::None, &())
            },
        )
        .unwrap();
        let bundle = builtin_surface_bundle(id).unwrap();
        let first = &bundle.surface.bindings()[0].name;
        for spelling in FRONT_END_SPELLINGS {
            let declared = crate::cli::surface_declares_flag(&bundle, spelling);
            assert_eq!(
                declared,
                collisions.contains(&(id, *spelling)),
                "{id} declares {spelling}: record the choice in this test"
            );
            if declared {
                let shown = front_end.dispatch(&args(&["--show", spelling, "chosen.value"]));
                assert_eq!(shown.code, 0, "{id} {spelling}: {shown:?}");
                assert!(shown.stdout.contains("chosen.value"), "{id} {spelling}");
                continue;
            }
            let (line, code, expect): (Vec<&str>, i32, &str) = match *spelling {
                "--help" | "-h" => (vec![spelling], 0, "Usage:"),
                "--json-schema" => (vec![spelling], 0, "\"front_end\""),
                "--protocol-info" => (vec![spelling], 0, "\"protocol_name\""),
                "--json-run" => (vec![spelling, missing], 2, "failed to read JSON request"),
                "--template" | "--profile-template" => (vec![spelling], 0, "[casars]"),
                "--describe" => (vec![spelling], 0, "task "),
                "--validate" => (vec![spelling, missing], 2, ""),
                "--show" => (vec![spelling], 0, "source Defaults"),
                "--defaults" | "--no-save-last" => (vec!["--show", spelling], 0, "source Defaults"),
                "--last" | "--last-successful" => {
                    (vec!["--show", spelling, "--workspace", directory], 2, "")
                }
                "--params" => (vec!["--show", spelling, missing], 2, ""),
                "--workspace" => (vec!["--show", spelling, directory], 0, "source Defaults"),
                "--unset" => (vec!["--show", spelling, first], 0, "source Defaults"),
                "--save-params" => {
                    // A surface with a required parameter has no sparse
                    // profile until it is set.
                    let output = front_end.dispatch(&args(&["--show", spelling, saved]));
                    let text = output.stdout + &output.stderr.join("\n");
                    let expect = if output.code == 0 {
                        "saved "
                    } else {
                        "required parameter"
                    };
                    assert!(text.contains(expect), "{id} {spelling}: {text}");
                    continue;
                }
                other => panic!("no expectation for {other}"),
            };
            let output = front_end.dispatch(&args(&line));
            assert_eq!(output.code, code, "{id} {line:?}: {output:?}");
            let text = output.stdout + &output.stderr.join("\n");
            assert!(text.contains(expect), "{id} {line:?}: {text}");
        }
        assert!(!ran.get(), "{id}: a front-end spelling ran the task");
    }
}

#[test]
fn a_front_end_hosts_only_surfaces_its_contract_carries() {
    let error = TaskFrontEnd::new("x", contract(), &["split"], |_: &TaskInvocation<'_>| {
        TaskSuccess::new(CasaValue::None, &())
    })
    .err()
    .unwrap();
    assert!(error.contains("split"), "{error}");
}
