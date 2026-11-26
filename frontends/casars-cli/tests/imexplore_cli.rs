// SPDX-License-Identifier: LGPL-3.0-or-later

//! `imexplore` end to end on its own command line: help and templates for
//! the surface its first word names, a real `imstat`, the Last state a run
//! records in its `--workspace`, and the profiles it refuses.

use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_coordinates::{CoordinateSystem, LinearCoordinate};
use casa_images::Image;
use ndarray::{ArrayD, IxDyn};

fn imexplore(args: &[&str], cwd: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args(args)
        .current_dir(cwd)
        .env_remove("CASA_RS_STATE_DIR")
        .stdin(Stdio::null())
        .output()
        .expect("run imexplore")
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
fn help_and_templates_document_the_surface_named_first() {
    let workspace = tempfile::tempdir().unwrap();
    let help = imexplore(&["imstat", "--help"], workspace.path());
    assert!(help.status.success(), "{}", stderr(&help));
    let text = stdout(&help);
    assert!(text.contains("SURFACE is one of imhead, imstat"), "{text}");
    assert!(text.contains("--last-successful"), "{text}");
    assert!(text.contains("Parameters of imstat"), "{text}");

    let template = imexplore(&["imstat", "--template"], workspace.path());
    assert!(template.status.success(), "{}", stderr(&template));
    assert!(
        stdout(&template).contains("imagename"),
        "{}",
        stdout(&template)
    );
}

#[test]
fn imstat_runs_and_records_last_state() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.image");
    write_image(&image);
    let path = workspace.path().to_str().unwrap();

    let output = imstat(workspace.path(), &image, &["--defaults"]);
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        stdout(&output),
        stderr(&output)
    );
    let text = stdout(&output);
    assert!(text.contains("npts: 12"), "{text}");

    let (last, successful) = last_files(workspace.path());
    assert!(last.is_some(), "missing Last");
    assert!(successful.is_some(), "missing Last Successful");

    let rerun = imexplore(
        &["imstat", "--last-successful", "--workspace", path],
        workspace.path(),
    );
    assert!(rerun.status.success(), "{}", stderr(&rerun));
    assert!(stdout(&rerun).contains("npts: 12"), "{}", stdout(&rerun));
}

/// `imexplore imstat IMAGE --workspace WORKSPACE EXTRA...`.
fn imstat(workspace: &Path, image: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "imstat",
        image.to_str().unwrap(),
        "--workspace",
        workspace.to_str().unwrap(),
    ];
    args.extend(extra);
    imexplore(&args, workspace)
}

fn last_files(workspace: &Path) -> (Option<String>, Option<String>) {
    let state = workspace.join(".casa-rs/parameters/imstat");
    (
        std::fs::read_to_string(state.join("last.toml")).ok(),
        std::fs::read_to_string(state.join("last-successful.toml")).ok(),
    )
}

#[test]
fn imstat_reads_a_region_file_in_its_workspace() {
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

    let output = imexplore(
        &[
            "imstat",
            "ramp.image",
            "--region",
            "box.crtf",
            "--workspace",
            "../workspace",
        ],
        &elsewhere,
    );
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
    assert!(imstat(workspace.path(), &image, &[]).status.success());

    // A missing image is refused, naming imagename (#931).
    let missing = workspace.path().join("missing.image");
    let failed = imstat(workspace.path(), &missing, &[]);
    assert_eq!(failed.status.code(), Some(2), "{}", stderr(&failed));
    let (last, successful) = last_files(workspace.path());
    assert!(last.unwrap().contains("missing.image"));
    assert!(successful.unwrap().contains("ramp.image"));
}

#[test]
fn no_save_last_leaves_existing_last_files_untouched() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.image");
    write_image(&image);
    assert!(imstat(workspace.path(), &image, &[]).status.success());
    let before = last_files(workspace.path());

    let other = workspace.path().join("other.image");
    write_image(&other);
    let succeeded = imstat(workspace.path(), &other, &["--no-save-last"]);
    assert!(succeeded.status.success(), "{}", stderr(&succeeded));
    let missing = workspace.path().join("missing.image");
    let failed = imstat(workspace.path(), &missing, &["--no-save-last"]);
    assert!(!failed.status.success());
    assert_eq!(last_files(workspace.path()), before);
}

#[test]
fn missing_or_corrupt_profiles_are_refused_and_never_replaced() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().to_str().unwrap();
    let missing_last = imexplore(&["imstat", "--last", "--workspace", path], workspace.path());
    assert_eq!(missing_last.status.code(), Some(2));
    assert!(
        stderr(&missing_last).contains("no managed Last profile"),
        "{}",
        stderr(&missing_last)
    );

    let state = workspace.path().join(".casa-rs/parameters/imstat");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(state.join("last.toml"), "not a profile").unwrap();
    let corrupt_last = imexplore(&["imstat", "--last", "--workspace", path], workspace.path());
    assert_eq!(corrupt_last.status.code(), Some(2));
    assert_eq!(
        std::fs::read_to_string(state.join("last.toml")).unwrap(),
        "not a profile"
    );
    assert!(!state.join("last-successful.toml").exists());

    let missing_file = workspace.path().join("missing.toml");
    let corrupt_file = workspace.path().join("corrupt.toml");
    std::fs::write(&corrupt_file, "[casars\nsurface = ").unwrap();
    for profile in [&missing_file, &corrupt_file] {
        let output = imexplore(
            &[
                "imstat",
                "--params",
                profile.to_str().unwrap(),
                "--workspace",
                path,
            ],
            workspace.path(),
        );
        assert_eq!(output.status.code(), Some(2), "{profile:?}");
        assert!(!stderr(&output).is_empty(), "{profile:?}");
    }
    assert_eq!(
        std::fs::read_to_string(state.join("last.toml")).unwrap(),
        "not a profile"
    );
}
