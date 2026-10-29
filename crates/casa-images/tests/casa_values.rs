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
    // Several moments in one run are not yet.
    assert_refused(
        &run(
            immoments,
            &["in.image", "--outfile", "two.mom", "--moments", "0,1"],
            root,
        ),
        "#977",
    );
    assert!(!root.join("two.mom").exists());
}

/// A 4x4 direction image with one channel per value of `spectrum`, the
/// same spectrum at every direction pixel, in Jy/beam.
fn write_spectrum_cube(path: &Path, spectrum: &[f32]) {
    use casa_coordinates::{
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    };
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    use ndarray::{ArrayD, IxDyn};
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [1.5, 1.5],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.415e9,
        1.0e3,
        0.0,
        1.415e9,
    ));
    let shape = vec![4, 4, spectrum.len()];
    let mut image =
        casa_images::PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(&shape), |index| spectrum[index[2]]);
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.set_units("Jy/beam").unwrap();
    image.save().unwrap();
}

/// The first pixel of the moment image at `path` and whether it is valid.
fn moment_pixel(path: &Path) -> (f32, bool) {
    let image = casa_images::PagedImage::<f32>::open(path).unwrap();
    let pixels = image.get().unwrap();
    let valid = image
        .get_mask()
        .unwrap()
        .is_none_or(|mask| *mask.iter().next().unwrap());
    (*pixels.iter().next().unwrap(), valid)
}

/// `includepix` reads as CASA 6.7.6.14 reads it, on a cube whose spectrum
/// is [-10, -2, 3, 11] at every pixel, where the mean moment of every
/// channel is 0.5 (CASA's values, from its own immoments).
#[test]
fn immoments_includepix_is_casas_inclusive_range() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_spectrum_cube(&root.join("cube.image"), &[-10.0, -2.0, 3.0, 11.0]);
    let immoments = env!("CARGO_BIN_EXE_immoments");
    for (index, (includepix, expected)) in [
        // Only -1 (or no value) includes every pixel.
        (json!(-1), Some(0.5)),
        (json!([]), Some(0.5)),
        // One value b is [-|b|, |b|], whatever its sign.
        (json!(-2), Some(-2.0)),
        (json!([2]), Some(-2.0)),
        // Nothing in [0, 0]: CASA's masked pixel.
        (json!([0]), None),
        // Two ends in either order.
        (json!([5, 2]), Some(3.0)),
    ]
    .into_iter()
    .enumerate()
    {
        let outfile = format!("mean{index}.im");
        let reply = json_run(
            immoments,
            "immoments",
            json!({"imagename": "cube.image", "outfile": outfile, "moments": -1,
                   "includepix": includepix}),
            root,
        );
        assert!(
            matches!(reply, TaskReply::Succeeded { .. }),
            "{includepix}: {reply:?}"
        );
        let (value, valid) = moment_pixel(&root.join(&outfile));
        match expected {
            Some(expected) => {
                assert!(valid, "{includepix}: masked");
                assert!((value - expected).abs() < 1e-6, "{includepix}: {value}");
            }
            None => assert!(!valid, "{includepix}: {value} is not masked"),
        }
    }
    // The command line reads the same ranges.
    let output = run(
        immoments,
        &[
            "cube.image",
            "--outfile",
            "cli.im",
            "--moments=-1",
            "--includepix=5,2",
        ],
        root,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (value, _) = moment_pixel(&root.join("cli.im"));
    assert!((value - 3.0).abs() < 1e-6, "{value}");
    // CASA takes one or two values; three are refused, writing nothing.
    let reply = json_run(
        immoments,
        "immoments",
        json!({"imagename": "cube.image", "outfile": "three.im", "includepix": [1, 2, 3]}),
        root,
    );
    assert!(
        matches!(&reply, TaskReply::Failed { parameter: Some(name), .. } if name == "includepix"),
        "{reply:?}"
    );
    assert!(!root.join("three.im").exists());
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
