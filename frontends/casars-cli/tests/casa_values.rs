// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image task binaries take CASA's values for CASA's parameters, on the
//! command line and in a `--json-run` request, and return CASA's value
//! (#931). A value casa-rs does not implement yet is refused before the
//! task starts, naming its parameter and its issue (#977).

mod support;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_contracts::{CasaValue, TaskReply};
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
    // CASA reads excludepix=[] as its -1: nothing excluded.
    let reply = json_run(
        immoments,
        "immoments",
        json!({"imagename": "in.image", "outfile": "ex.mom0", "excludepix": []}),
        root,
    );
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    // CASA appends the moment suffixes when the request names several.
    let output = run(
        immoments,
        &["in.image", "--outfile", "two.mom", "--moments", "0,1"],
        root,
    );
    let summary = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bundle = casa_contracts::builtin_surface_bundle("immoments").unwrap();
    let products = casa_task_runtime::decode_task_completion(&bundle, &summary, root, &[])
        .unwrap()
        .products;
    assert_eq!(products.len(), 2);
    for (product, suffix) in products.iter().zip(["integrated", "weighted_coord"]) {
        assert!(product.exists);
        assert_eq!(
            product.path.file_name().unwrap().to_str().unwrap(),
            format!("two.mom.{suffix}")
        );
    }
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

/// immath takes CASA's spellings of its defaults and ignores what CASA
/// ignores outside its polarization modes; CASA 6.7.6.14 runs each of
/// these as the plain expression.
#[test]
fn immath_takes_casas_spellings_of_its_defaults() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 1.0);
    let immath = env!("CARGO_BIN_EXE_immath");
    for (index, values) in [
        json!({"varnames": []}),
        json!({"varnames": ""}),
        json!({"sigma": "0Jy/beam"}),
        json!({"sigma": "5mJy/beam", "polithresh": "1Jy/beam", "stretch": true}),
    ]
    .into_iter()
    .enumerate()
    {
        let mut request = values.clone();
        request["imagename"] = json!(["in.image"]);
        request["expr"] = json!("IM0*2");
        request["outfile"] = json!(format!("ok{index}.im"));
        let reply = json_run(immath, "immath", request, root);
        assert!(
            matches!(
                reply,
                TaskReply::Succeeded {
                    value: CasaValue::Bool { value: true },
                    ..
                }
            ),
            "{values}: {reply:?}"
        );
    }
    let output = run(
        immath,
        &[
            "--imagename",
            "in.image",
            "--expr",
            "IM0*2",
            "--outfile",
            "cli.im",
            "--sigma",
            "0Jy/beam",
            "--polithresh",
            "1Jy/beam",
        ],
        root,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Varname binding succeeds on the JSON surface.
    let named = json_run(
        immath,
        "immath",
        json!({"imagename": ["in.image"], "expr": "A*2", "varnames": ["A"], "outfile": "named.im"}),
        root,
    );
    assert!(
        matches!(
            named,
            TaskReply::Succeeded {
                value: CasaValue::Bool { value: true },
                ..
            }
        ),
        "{named:?}"
    );
    assert!(root.join("named.im").exists());
    let input_pixels = casa_images::PagedImage::<f32>::open(root.join("in.image"))
        .unwrap()
        .get()
        .unwrap();
    let named_img = casa_images::PagedImage::<f32>::open(root.join("named.im")).unwrap();
    let named_pixels = named_img.get().unwrap();
    assert!(!named_pixels.is_empty());
    for (&got, &orig) in named_pixels.iter().zip(input_pixels.iter()) {
        assert!(
            (got - orig * 2.0).abs() < 1e-6,
            "expected named.im pixel {} == 2.0 * {orig}, got {got}",
            orig * 2.0
        );
    }

    // Invalid mode, prec, or insufficient inputs fail cleanly without creating output.
    for (request, needle) in [
        (
            json!({"imagename": ["in.image"], "mode": "bogus", "outfile": "badmode.im"}),
            "invalid choice",
        ),
        (
            json!({"imagename": ["in.image"], "prec": "int", "outfile": "badprec.im"}),
            "Unsupported value for type",
        ),
        (
            json!({"imagename": ["in.image"], "mode": "spix", "outfile": "badspix.im"}),
            "Requires two images",
        ),
    ] {
        let reply = json_run(immath, "immath", request, root);
        let TaskReply::Failed { message, .. } = reply else {
            panic!("expected failure for {needle}: {reply:?}");
        };
        assert!(message.contains(needle), "got: {message}");
    }
    assert!(!root.join("badmode.im").exists());
    assert!(!root.join("badprec.im").exists());
    assert!(!root.join("badspix.im").exists());
}

#[test]
fn imregrid_returns_a_reusable_template_or_writes_the_default_product() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 1.0);
    let imregrid = env!("CARGO_BIN_EXE_imregrid");
    let reply = json_run(
        imregrid,
        "imregrid",
        json!({"imagename": "in.image", "output": "unused.image"}),
        root,
    );
    let TaskReply::Succeeded { value, .. } = reply else {
        panic!("{reply:?}");
    };
    let template = value.to_plain_json();
    assert_eq!(template["shap"], json!([4, 4, 3]));
    assert!(template["csys"]["direction0"].is_object());
    assert!(!root.join("unused.image").exists());
    let reply = json_run(
        imregrid,
        "imregrid",
        json!({"imagename": "in.image", "template": template, "output": "dictionary.image"}),
        root,
    );
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    assert!(root.join("dictionary.image").exists());
    let cli = run(
        imregrid,
        &["--imagename", "in.image", "--template", "dictionary.image"],
        root,
    );
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    assert!(root.join("in.image.regridded").exists());
}

#[test]
fn impbcor_accepts_casa_selection_and_numeric_beams() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 2.0);
    write_image(&root.join("pb.image"), 1.0);
    let impbcor = env!("CARGO_BIN_EXE_impbcor");
    let reply = json_run(
        impbcor,
        "impbcor",
        json!({
            "imagename": "in.image", "pbimage": vec![vec![2.0; 4]; 4], "outfile": "pixels.im",
            "box": "0,0,1,1", "chans": "0", "mask": "in.image > 0",
            "stretch": true,
        }),
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
    let selected = casa_images::PagedImage::<f32>::open(root.join("pixels.im")).unwrap();
    assert_eq!(selected.shape(), &[2, 2, 1]);
    let values = selected.get().unwrap();
    assert_eq!(values[[0, 0, 0]], 1.0);
    assert_eq!(values[[1, 1, 0]], 6.0);
    let box_line = [
        "--imagename",
        "in.image",
        "--pbimage",
        "pb.image",
        "--outfile",
        "o.im",
        "--box",
        "0,0,1,1",
    ];
    let cli = run(impbcor, &box_line, root);
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let selected = casa_images::PagedImage::<f32>::open(root.join("o.im")).unwrap();
    assert_eq!(selected.shape(), &[2, 2, 3]);
    assert!(selected.get().unwrap().iter().all(|&value| value == 2.0));
}

/// An old casa-rs profile is refused before the task writes an image.
#[test]
fn stale_impbcor_profile_is_refused_before_writing() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 2.0);
    write_image(&root.join("pb.image"), 1.0);
    let current = casa_contracts::builtin_surface_bundle("impbcor")
        .unwrap()
        .surface
        .contract_version();
    let profile = root.join("old.toml");
    let source = format!(
        "[casars]\nformat = 1\nsurface = \"impbcor\"\nkind = \"task\"\ncontract = {}\n\n\
         [parameters]\nimagename = \"in.image\"\npbimage = \"pb.image\"\n\
         outfile = \"out.im\"\nstretch = false\n",
        current - 1
    );
    std::fs::write(&profile, &source).unwrap();
    let output = run(
        env!("CARGO_BIN_EXE_impbcor"),
        &["--params", profile.to_str().unwrap()],
        root,
    );
    assert_refused(&output, "too old");
    assert!(!root.join("out.im").exists());
    assert_eq!(std::fs::read_to_string(profile).unwrap(), source);
}

/// A missing input is a refusal naming its parameter (exit 2), before the
/// task starts, as CASA's `imstat('does-not-exist.im')` is; an error the
/// task meets once it runs stays an execution failure.
#[test]
fn a_missing_input_image_is_refused_naming_its_parameter() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    write_image(&root.join("in.image"), 1.0);
    for (binary, surface, values, parameter) in [
        (
            env!("CARGO_BIN_EXE_immoments"),
            "immoments",
            json!({"imagename": "missing.im", "outfile": "m.im"}),
            "imagename",
        ),
        (
            env!("CARGO_BIN_EXE_immath"),
            "immath",
            json!({"imagename": ["in.image", "missing.im"], "expr": "IM0+IM1", "outfile": "m.im"}),
            "imagename",
        ),
        (
            env!("CARGO_BIN_EXE_impbcor"),
            "impbcor",
            json!({"imagename": "in.image", "pbimage": "missing.im", "outfile": "m.im"}),
            "pbimage",
        ),
        (
            env!("CARGO_BIN_EXE_exportfits"),
            "exportfits",
            json!({"imagename": "missing.im", "fitsimage": "m.fits"}),
            "imagename",
        ),
        (
            env!("CARGO_BIN_EXE_importfits"),
            "importfits",
            json!({"fitsimage": "missing.fits", "imagename": "m.im"}),
            "fitsimage",
        ),
    ] {
        let reply = json_run(binary, surface, values, root);
        let TaskReply::Failed {
            message,
            parameter: named,
            ..
        } = &reply
        else {
            panic!("{surface}: {reply:?}");
        };
        assert_eq!(named.as_deref(), Some(parameter), "{surface}: {message}");
        assert!(message.contains("does not exist"), "{surface}: {message}");
    }
    assert!(!root.join("m.im").exists());
    let output = run(
        env!("CARGO_BIN_EXE_immoments"),
        &["missing.im", "--outfile", "m.im"],
        root,
    );
    assert_refused(&output, "missing.im");
    // An image that exists but cannot be read fails as the task runs.
    std::fs::create_dir(root.join("empty.im")).unwrap();
    let reply = json_run(
        env!("CARGO_BIN_EXE_immoments"),
        "immoments",
        json!({"imagename": "empty.im", "outfile": "e.im"}),
        root,
    );
    let TaskReply::Failed { parameter, .. } = &reply else {
        panic!("{reply:?}");
    };
    assert_eq!(parameter, &None, "{reply:?}");
}
