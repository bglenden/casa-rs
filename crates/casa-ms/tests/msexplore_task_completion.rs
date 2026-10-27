// SPDX-License-Identifier: LGPL-3.0-or-later

//! msexplore and plotms task completions from the real provider.
//!
//! A request goes as `casars run` and the Python tasks send it: the runner's
//! projection of a typed parameter session, one `--json-run` request on
//! stdin, whose typed reply goes through the completion decoder (#835, #931).
//! The runner injects no output format, so the user's `format` decides the
//! summary in `output` and on the binary's own command line (#836).

mod common;

use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_provider_contracts::{ParameterValue, RunProductKind, TaskReply, builtin_surface_bundle};
use casa_task_runtime::{
    ParameterSession, TaskCompletion, TaskOutputValue, decode_task_completion,
    project_provider_invocation,
};
use common::create_msexplore_fixture_ms;
use tempfile::tempdir;

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_string())
}

/// Run `surface` through the real msexplore provider with `parameters`,
/// returning its reply and decoded successful completion.
fn run(
    surface: &str,
    workspace: &Path,
    parameters: &[(&str, ParameterValue)],
) -> (TaskReply, TaskCompletion) {
    let bundle = builtin_surface_bundle(surface).expect("surface bundle");
    let mut session = ParameterSession::defaults(bundle).expect("default session");
    for (name, value) in parameters {
        session.set(*name, value.clone()).expect("set parameter");
    }
    let invocation = project_provider_invocation(&session).expect("provider invocation");
    assert_eq!(invocation.args, ["--json-run", "-"]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(&invocation.args)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run msexplore");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(invocation.stdin.expect("request").as_bytes())
        .expect("write request");
    let output = child.wait_with_output().expect("msexplore output");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let reply: TaskReply = serde_json::from_str(&stdout).expect("typed reply");
    let values = parameters
        .iter()
        .map(|(name, value)| TaskOutputValue {
            name: (*name).to_string(),
            value: match value {
                ParameterValue::String(value) => value.clone(),
                ParameterValue::Bool(value) => value.to_string(),
                other => panic!("unexpected test parameter value {other:?}"),
            },
        })
        .collect::<Vec<_>>();
    let completion = decode_task_completion(session.bundle(), &stdout, workspace, &values)
        .unwrap_or_else(|error| panic!("{surface} completion: {error}\nstdout: {stdout}"));
    (reply, completion)
}

/// The binary's own command line for `surface`, returning its stdout.
fn command_line(surface: &str, workspace: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .arg(surface)
        .args(args)
        .arg("--no-save-last")
        .current_dir(workspace)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).expect("utf-8 stdout")
}

fn is_json(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok()
}

fn is_fixture_summary(summary: &serde_json::Value) -> bool {
    summary["measurement_set"]["path"]
        .as_str()
        .is_some_and(|path| path.ends_with("msexplore_fixture.ms"))
}

#[test]
fn user_format_decides_the_summary_on_stdout_and_in_output() {
    for surface in ["msexplore", "plotms"] {
        let workspace = tempdir().expect("tempdir");
        let vis_path = create_msexplore_fixture_ms(workspace.path());
        let vis_arg = vis_path.to_str().unwrap();
        let vis = text(vis_arg);

        let stdout = command_line(surface, workspace.path(), &[vis_arg]);
        assert!(
            !stdout.trim().is_empty() && !is_json(&stdout),
            "{surface} default format should print the text summary: {stdout}"
        );
        let stdout = command_line(surface, workspace.path(), &[vis_arg, "--format", "json"]);
        let summary: serde_json::Value = serde_json::from_str(&stdout).expect("JSON summary");
        assert!(is_fixture_summary(&summary), "{surface}: {summary}");

        // msexplore returns its summary, as CASA's listobs returns its
        // record; plotms returns None. The result carries it either way.
        let (reply, completion) = run(surface, workspace.path(), &[("vis", vis.clone())]);
        let TaskReply::Succeeded { value, result, .. } = reply else {
            panic!("{surface}: {reply:?}");
        };
        assert_eq!(
            surface == "msexplore",
            is_fixture_summary(&value),
            "{surface}"
        );
        assert_eq!(surface == "plotms", value.is_null(), "{surface}");
        assert!(
            is_fixture_summary(&result["summary"]),
            "{surface}: {result}"
        );
        assert_eq!(completion.summary, format!("{surface} completed"));
        assert!(completion.products.is_empty(), "{surface}");

        for (format, file, json) in [
            ("text", "summary.txt", false),
            ("json", "summary.json", true),
        ] {
            let (_, completion) = run(
                surface,
                workspace.path(),
                &[
                    ("vis", vis.clone()),
                    ("format", text(format)),
                    ("output", text(file)),
                ],
            );
            let written = fs::read_to_string(workspace.path().join(file)).expect("summary file");
            assert_eq!(is_json(&written), json, "{surface} {format}: {written}");
            assert_eq!(completion.products.len(), 1, "{surface} {format}");
            let product = &completion.products[0];
            assert_eq!(product.id, "output", "{surface}");
            assert_eq!(product.resource_kind, RunProductKind::File, "{surface}");
            assert!(product.exists, "{surface}: {product:?}");
        }
    }
}

#[test]
fn plot_and_flag_preview_products_are_reported_as_files() {
    for surface in ["msexplore", "plotms"] {
        let workspace = tempdir().expect("tempdir");
        let vis = create_msexplore_fixture_ms(workspace.path());
        let (_, completion) = run(
            surface,
            workspace.path(),
            &[
                ("vis", text(vis.to_str().unwrap())),
                ("preset", text("amplitude_vs_time")),
                ("plot_output", text("amp.txt")),
                ("flag_action", text("flag")),
                ("flag_selected", ParameterValue::Bool(true)),
                ("flag_output", text("preview.json")),
            ],
        );

        let ids = completion
            .products
            .iter()
            .map(|product| product.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["plot_output", "flag_output"], "{surface}");
        for product in &completion.products {
            assert_eq!(product.resource_kind, RunProductKind::File, "{surface}");
            assert!(product.exists, "{surface}: {product:?}");
        }
        let preview: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(workspace.path().join("preview.json")).expect("preview file"),
        )
        .expect("flag preview JSON");
        assert!(
            preview.get("matched_points").is_some(),
            "{surface}: {preview}"
        );
    }
}
