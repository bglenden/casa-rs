// SPDX-License-Identifier: LGPL-3.0-or-later

//! msexplore and plotms task completions from the real provider.
//!
//! The provider arguments come from the runner's projection of a typed
//! parameter session, the path `casars run` and the Python tasks take, and
//! the provider's stdout goes through the completion decoder (#835). The
//! runner injects no output format, so the user's `format` decides what the
//! summary looks like on stdout and in `output` (#836).

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

use casa_provider_contracts::{
    ParameterValue, ProviderInvocationAdaptation, RunProductKind, builtin_surface_bundle,
};
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
/// returning its stdout and decoded successful completion.
fn run(
    surface: &str,
    workspace: &Path,
    parameters: &[(&str, ParameterValue)],
) -> (String, TaskCompletion) {
    let bundle = builtin_surface_bundle(surface).expect("surface bundle");
    let mut session = ParameterSession::defaults(bundle).expect("default session");
    for (name, value) in parameters {
        session.set(*name, value.clone()).expect("set parameter");
    }
    let invocation = project_provider_invocation(&session, |_family, _values, direct| {
        Ok(ProviderInvocationAdaptation::direct(direct))
    })
    .expect("provider invocation");
    assert!(invocation.stdin.is_none(), "{invocation:?}");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(&invocation.args)
        .current_dir(workspace)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
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
    (stdout, completion)
}

fn is_json(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok()
}

#[test]
fn user_format_decides_the_summary_on_stdout_and_in_output() {
    for surface in ["msexplore", "plotms"] {
        let workspace = tempdir().expect("tempdir");
        let vis = create_msexplore_fixture_ms(workspace.path());
        let vis = text(vis.to_str().unwrap());

        let (stdout, completion) = run(surface, workspace.path(), &[("vis", vis.clone())]);
        assert!(
            !stdout.trim().is_empty() && !is_json(&stdout),
            "{surface} default format should print the text summary: {stdout}"
        );
        assert_eq!(completion.summary, format!("{surface} completed"));
        assert!(completion.products.is_empty(), "{surface}");

        let (stdout, _) = run(
            surface,
            workspace.path(),
            &[("vis", vis.clone()), ("format", text("json"))],
        );
        let summary: serde_json::Value = serde_json::from_str(&stdout).expect("JSON summary");
        assert!(
            summary["measurement_set"]["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("msexplore_fixture.ms")),
            "{surface}: {summary}"
        );

        for (format, file, json) in [
            ("text", "summary.txt", false),
            ("json", "summary.json", true),
        ] {
            let (stdout, completion) = run(
                surface,
                workspace.path(),
                &[
                    ("vis", vis.clone()),
                    ("format", text(format)),
                    ("output", text(file)),
                ],
            );
            assert!(stdout.trim().is_empty(), "{surface} {format}: {stdout}");
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
