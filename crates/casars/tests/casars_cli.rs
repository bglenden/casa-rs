// SPDX-License-Identifier: LGPL-3.0-or-later

//! The `casars` binary end to end: help, the removed interactive commands,
//! parameter templates, and a real `casars run imstat` through the
//! `imexplore` provider binary, including the Last state it records; and
//! `imexplore` started directly, in its `--workspace`.

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

/// `casars run imstat IMAGE --workspace WORKSPACE EXTRA...`.
fn run_imstat(workspace: &Path, image: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "run",
        "imstat",
        image.to_str().unwrap(),
        "--workspace",
        workspace.to_str().unwrap(),
    ];
    args.extend(extra);
    casars(&args, workspace)
}

fn last_files(workspace: &Path) -> (Option<String>, Option<String>) {
    let state = workspace.join(".casa-rs/parameters/imstat");
    (
        std::fs::read_to_string(state.join("last.toml")).ok(),
        std::fs::read_to_string(state.join("last-successful.toml")).ok(),
    )
}

#[test]
fn imexplore_reads_a_region_file_in_its_workspace() {
    // Astra's #956 P1: started directly, `imstat --region box.crtf` read the
    // region file from the directory the command was run in.
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let elsewhere = root.path().join("elsewhere");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&elsewhere).unwrap();
    write_image(&workspace.join("ramp.image"));
    std::fs::write(
        workspace.join("box.crtf"),
        "#CRTFv0\nbox[[0pix,0pix],[1pix,1pix]]\n",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args(["imstat", "ramp.image", "--region", "box.crtf"])
        .args(["--workspace", "../workspace"])
        .current_dir(&elsewhere)
        .output()
        .expect("run imexplore");
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stdout(&output).contains("npts: 4"), "{}", stdout(&output));
    assert!(last_files(&workspace).1.unwrap().contains("box.crtf"));
    assert_eq!(std::fs::read_dir(&elsewhere).unwrap().count(), 0);
}

#[test]
fn a_failed_run_records_last_but_keeps_last_successful() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.image");
    write_image(&image);
    assert!(run_imstat(workspace.path(), &image, &[]).status.success());

    let missing = workspace.path().join("missing.image");
    let failed = run_imstat(workspace.path(), &missing, &[]);
    assert!(!failed.status.success(), "{}", stdout(&failed));
    let (last, successful) = last_files(workspace.path());
    assert!(last.unwrap().contains("missing.image"));
    assert!(successful.unwrap().contains("ramp.image"));
}

#[test]
fn no_save_last_leaves_existing_last_files_untouched() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.image");
    write_image(&image);
    assert!(run_imstat(workspace.path(), &image, &[]).status.success());
    let before = last_files(workspace.path());

    let other = workspace.path().join("other.image");
    write_image(&other);
    let succeeded = run_imstat(workspace.path(), &other, &["--no-save-last"]);
    assert!(succeeded.status.success(), "{}", stderr(&succeeded));
    let missing = workspace.path().join("missing.image");
    let failed = run_imstat(workspace.path(), &missing, &["--no-save-last"]);
    assert!(!failed.status.success());
    assert_eq!(last_files(workspace.path()), before);
}

#[test]
fn missing_or_corrupt_profiles_are_reported_and_never_replaced() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().to_str().unwrap();
    let missing_last = casars(
        &["run", "imstat", "--last", "--workspace", path],
        workspace.path(),
    );
    assert_eq!(missing_last.status.code(), Some(1));
    assert!(
        stderr(&missing_last).contains("no managed Last profile"),
        "{}",
        stderr(&missing_last)
    );

    let state = workspace.path().join(".casa-rs/parameters/imstat");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(state.join("last.toml"), "not a profile").unwrap();
    let corrupt_last = casars(
        &["run", "imstat", "--last", "--workspace", path],
        workspace.path(),
    );
    assert_eq!(corrupt_last.status.code(), Some(1));
    assert_eq!(
        std::fs::read_to_string(state.join("last.toml")).unwrap(),
        "not a profile"
    );
    assert!(!state.join("last-successful.toml").exists());

    let missing_file = workspace.path().join("missing.toml");
    let corrupt_file = workspace.path().join("corrupt.toml");
    std::fs::write(&corrupt_file, "[casars\nsurface = ").unwrap();
    for profile in [&missing_file, &corrupt_file] {
        let output = casars(
            &[
                "run",
                "imstat",
                "--params",
                profile.to_str().unwrap(),
                "--workspace",
                path,
            ],
            workspace.path(),
        );
        assert_eq!(output.status.code(), Some(1), "{profile:?}");
        assert!(!stderr(&output).is_empty(), "{profile:?}");
    }
    assert_eq!(
        std::fs::read_to_string(state.join("last.toml")).unwrap(),
        "not a profile"
    );
}
