// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image task binaries take CASA's values for CASA's parameters, on the
//! command line and in a `--json-run` request, and return CASA's value
//! (#931). A value casa-rs does not implement yet is refused before the
//! task starts, naming its parameter and its issue (#977).

mod support;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_provider_contracts::{CasaValue, TaskReply};
use serde_json::{Value, json};
use support::write_image;

fn run(binary: &str, args: &[&str], workspace: &Path) -> Output {
    Command::new(binary)
        .args(args)
        .arg("--no-save-last")
        .current_dir(workspace)
        .output()
        .expect("run task binary")
}

fn json_run(binary: &str, surface: &str, values: Value, workspace: &Path) -> TaskReply {
    let mut child = Command::new(binary)
        .args(["--json-run", "-"])
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start task binary");
    let request = json!({"surface": surface, "values": values}).to_string();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.as_bytes())
        .unwrap();
    let output = child.wait_with_output().expect("run task binary");
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{surface}: {error}\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// `output` is a refusal (exit 2) that names `issue`.
fn assert_refused(output: &Output, issue: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains(issue), "{stderr}");
}

/// `reply` refuses `parameter`, naming `issue`.
fn assert_refuses(reply: TaskReply, parameter: &str, issue: &str) {
    let TaskReply::Failed {
        message,
        parameter: named,
        ..
    } = reply
    else {
        panic!("{reply:?}");
    };
    assert_eq!(named.as_deref(), Some(parameter), "{message}");
    assert!(message.contains(issue), "{message}");
}

#[test]
fn immoments_takes_casas_moments_and_includepix() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 1.0);
    let immoments = env!("CARGO_BIN_EXE_immoments");
    // The tutorial's command line: one moment, an inclusive pixel range.
    let output = run(
        immoments,
        &[
            "in.image",
            "--outfile",
            "cli.mom0",
            "--moments",
            "0",
            "--includepix",
            "0.03,100.0",
        ],
        root,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.join("cli.mom0").exists());
    // CASA's Python values: a scalar moment, a [min, max] list.
    let reply = json_run(
        immoments,
        "immoments",
        json!({"imagename": "in.image", "outfile": "py.mom0", "moments": 0, "includepix": [5, 40]}),
        root,
    );
    assert!(
        matches!(
            reply,
            TaskReply::Succeeded {
                value: CasaValue::None,
                ..
            }
        ),
        "{reply:?}"
    );
    assert!(root.join("py.mom0").exists());
    // Several moments in one run, and a lone threshold, are not yet.
    assert_refused(
        &run(
            immoments,
            &["in.image", "--outfile", "two.mom", "--moments", "0,1"],
            root,
        ),
        "#977",
    );
    assert_refuses(
        json_run(
            immoments,
            "immoments",
            json!({"imagename": "in.image", "outfile": "thr.mom", "includepix": [5]}),
            root,
        ),
        "includepix",
        "#977",
    );
    assert!(!root.join("two.mom").exists() && !root.join("thr.mom").exists());
}

#[test]
fn immath_returns_true() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 1.0);
    let reply = json_run(
        env!("CARGO_BIN_EXE_immath"),
        "immath",
        json!({"imagename": ["in.image"], "expr": "IM0*2", "outfile": "twice.im"}),
        root,
    );
    assert!(
        matches!(
            reply,
            TaskReply::Succeeded {
                value: CasaValue::Bool { value: true },
                ..
            }
        ),
        "{reply:?}"
    );
    assert!(root.join("twice.im").exists());
}

#[test]
fn imregrid_refuses_casas_default_template_get() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 1.0);
    let reply = json_run(
        env!("CARGO_BIN_EXE_imregrid"),
        "imregrid",
        json!({"imagename": "in.image", "output": "out.im"}),
        root,
    );
    assert_refuses(reply, "template", "#977");
    assert!(!root.join("out.im").exists());
}
