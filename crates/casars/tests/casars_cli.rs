// SPDX-License-Identifier: LGPL-3.0-or-later

//! The `casars` binary end to end: help, the removed interactive commands,
//! parameter templates, and a real `casars run imstat` through the
//! `imexplore` provider binary, including the Last state it records.

use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_coordinates::{CoordinateSystem, LinearCoordinate};
use casa_images::Image;
use ndarray::{ArrayD, IxDyn};

fn casars(args: &[&str], workspace: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_casars"))
        .args(args)
        .current_dir(workspace)
        .env("CASARS_IMEXPLORE_BIN", env!("CARGO_BIN_EXE_imexplore"))
        .env_remove("CASA_RS_STATE_DIR")
        .stdin(Stdio::null())
        .output()
        .expect("run casars")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn write_image(path: &Path) {
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["pix".into(), "pix".into()],
    ));
    let mut image = Image::create(vec![4, 3], coords, path).expect("create image");
    image.set_units("Jy/beam").expect("set units");
    let pixels =
        ArrayD::from_shape_vec(IxDyn(&[4, 3]), (1..=12).map(|value| value as f32).collect())
            .expect("pixel array");
    image.put_slice(&pixels, &[0, 0]).expect("write pixels");
    image.save().expect("save image");
}

#[test]
fn help_is_printed_without_arguments_and_for_help_flags() {
    let workspace = tempfile::tempdir().unwrap();
    for args in [&[][..], &["--help"][..], &["-h"][..]] {
        let output = casars(args, workspace.path());
        assert!(output.status.success(), "{args:?}: {}", stderr(&output));
        let text = stdout(&output);
        assert!(text.contains("casars run TASK"), "{args:?}: {text}");
        assert!(text.contains("--last-successful"), "{args:?}: {text}");
    }
}

#[test]
fn removed_and_unknown_commands_fail_with_a_message() {
    let workspace = tempfile::tempdir().unwrap();
    let open = casars(&["open", "imexplore"], workspace.path());
    assert_eq!(open.status.code(), Some(1));
    assert!(stderr(&open).contains("removed"), "{}", stderr(&open));

    let launcher = casars(&["msexplore"], workspace.path());
    assert_eq!(launcher.status.code(), Some(1));
    assert!(
        stderr(&launcher).contains("unknown casars command"),
        "{}",
        stderr(&launcher)
    );

    let session = casars(&["run", "tablebrowser"], workspace.path());
    assert_eq!(session.status.code(), Some(1));
    assert!(
        stderr(&session).contains("unknown casars task"),
        "{}",
        stderr(&session)
    );
}

#[test]
fn params_template_documents_the_task_surface() {
    let workspace = tempfile::tempdir().unwrap();
    let output = casars(&["params", "template", "imstat"], workspace.path());
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stdout(&output).contains("imagename"), "{}", stdout(&output));
}

#[test]
fn run_imstat_executes_the_provider_and_records_last_state() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.image");
    write_image(&image);

    let output = casars(
        &[
            "run",
            "imstat",
            "--defaults",
            image.to_str().unwrap(),
            "--workspace",
            workspace.path().to_str().unwrap(),
        ],
        workspace.path(),
    );
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        stdout(&output),
        stderr(&output)
    );
    let text = stdout(&output);
    assert!(text.contains("npts"), "{text}");
    assert!(text.contains("12"), "{text}");

    let state = workspace.path().join(".casa-rs/parameters/imstat");
    assert!(
        state.join("last.toml").is_file(),
        "missing Last in {state:?}"
    );
    assert!(
        state.join("last-successful.toml").is_file(),
        "missing Last Successful in {state:?}"
    );

    let rerun = casars(
        &[
            "run",
            "imstat",
            "--last-successful",
            "--workspace",
            workspace.path().to_str().unwrap(),
        ],
        workspace.path(),
    );
    assert!(rerun.status.success(), "{}", stderr(&rerun));
    assert!(stdout(&rerun).contains("npts"), "{}", stdout(&rerun));
}
