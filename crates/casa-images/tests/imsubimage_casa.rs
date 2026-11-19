// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 snapshots from tools/casa-compat/imsubimage_oracle.py.

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateType;
use casa_images::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImagePixel, ImsubimageRequest, PagedImage, imhead,
    imsubimage,
};
use casa_types::{Complex32, Complex64};
use serde_json::Value as Json;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imsubimage")
}

fn input_path(name: &str) -> PathBuf {
    if let Some(axis_fixture) = name.strip_prefix("axis_maps/") {
        fixtures()
            .join("../casa-6.7.6/axis_maps")
            .join(axis_fixture)
    } else {
        fixtures().join(name)
    }
}

fn close(actual: f64, expected: f64, label: &str) {
    assert!(
        (actual - expected).abs() <= 1e-10 * expected.abs().max(1.0),
        "{label}: calculated {actual}, CASA {expected}"
    );
}

fn numbers(value: &Json) -> Vec<f64> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_f64().unwrap())
        .collect()
}

fn check_beam(actual: &GaussianBeam, expected: &Json) {
    let value_in = |key: &str| {
        let angle = &expected[key];
        let value = angle["value"].as_f64().unwrap();
        match angle["unit"].as_str().unwrap() {
            "arcsec" => (value / 3600.0).to_radians(),
            "deg" => value.to_radians(),
            "rad" => value,
            unit => panic!("unexpected beam unit {unit}"),
        }
    };
    close(actual.major, value_in("major"), "major axis");
    close(actual.minor, value_in("minor"), "minor axis");
    close(
        actual.position_angle,
        value_in("positionangle"),
        "position angle",
    );
}

fn check_beams(beams: &ImageBeamSet, expected: &Json) {
    if expected["major"].is_object() {
        check_beam(beams.get_beam().unwrap(), expected);
    } else if let (Some(nchan), Some(nstokes)) =
        (expected["nChannels"].as_u64(), expected["nStokes"].as_u64())
    {
        assert_eq!(beams.shape(), (nchan as usize, nstokes as usize));
        for channel in 0..nchan as usize {
            for pol in 0..nstokes as usize {
                check_beam(
                    beams.beam(channel, pol),
                    &expected["beams"][format!("*{channel}")][format!("*{pol}")],
                );
            }
        }
    } else {
        assert!(beams.is_empty());
    }
}

fn check_wcs<T: ImagePixel>(image: &PagedImage<T>, expected: &Json, case: &str) {
    assert_eq!(
        serde_json::json!(image.axis_names()),
        expected["names"],
        "{case}"
    );
    let first = vec![0.0; image.shape().len()];
    let last = image
        .shape()
        .iter()
        .map(|&n| (n - 1) as f64)
        .collect::<Vec<_>>();
    for (i, pixel) in [first, last].iter().enumerate() {
        let actual = image.coordinates().to_world(pixel).unwrap();
        let expected_world = numbers(&expected["world"][i]);
        assert_eq!(actual.len(), expected_world.len(), "{case}");
        for (axis, ((ours, theirs), kind)) in actual
            .iter()
            .zip(expected_world)
            .zip(image.axis_types())
            .enumerate()
        {
            if kind == CoordinateType::Direction {
                let angle = match expected["units"][axis].as_str().unwrap() {
                    "'" => (theirs / 60.0).to_radians(),
                    "deg" => theirs.to_radians(),
                    "rad" => theirs,
                    unit => panic!("unexpected direction unit {unit}"),
                };
                let delta = ours - angle;
                close(delta.sin().atan2(delta.cos()), 0.0, case);
            } else {
                close(*ours, theirs, &format!("{case}: world axis {axis}"));
            }
        }
    }
}

fn check_image<T: ImagePixel + Copy>(
    image: PagedImage<T>,
    oracle: &Json,
    case: &str,
    real_imag: impl Fn(T) -> (f64, f64),
) {
    assert_eq!(serde_json::json!(image.shape()), oracle["shape"], "{case}");
    assert_eq!(image.units(), oracle["unit"].as_str().unwrap(), "{case}");
    assert_eq!(
        image.has_pixel_mask(),
        oracle["masks"] == serde_json::json!(["mask0"]),
        "{case}"
    );
    let mask = image.get_mask().unwrap();
    let values = image.get().unwrap();
    let pixels = oracle["pixels"].as_array().unwrap();
    assert_eq!(pixels.len(), values.len(), "{case}");
    for (i, ((idx, &value), expected)) in values.indexed_iter().zip(pixels).enumerate() {
        let valid = mask.as_ref().is_none_or(|m| m[idx]);
        assert_eq!(valid, !expected.is_null(), "{case}: pixel {i} validity");
        if valid {
            let (real, imag) = real_imag(value);
            let (expect_real, expect_imag) = if expected.is_array() {
                (expected[0].as_f64().unwrap(), expected[1].as_f64().unwrap())
            } else {
                (expected.as_f64().unwrap(), 0.0)
            };
            assert_eq!(real, expect_real, "{case}: pixel {i} real");
            assert_eq!(imag, expect_imag, "{case}: pixel {i} imaginary");
        }
    }
    check_wcs(&image, oracle, case);
    check_beams(&image.image_info().unwrap().beam_set, &oracle["beams"]);
    let history = image.history().unwrap();
    assert_eq!(
        history
            .iter()
            .any(|line| line.contains("imsubimage oracle input history")),
        oracle["copied_history"].as_bool().unwrap(),
        "{case}"
    );
    assert!(
        history.iter().any(|line| line.contains("imsubimage(")),
        "{case}"
    );
}

#[test]
fn subimage_matches_casa_6_7_6_14() {
    let document: Json =
        serde_json::from_str(include_str!("data/imsubimage/expected.json")).unwrap();
    assert_eq!(document["casa_version"], "6.7.6.14");
    let scratch = tempfile::tempdir().unwrap();
    for (name, case) in document["cases"].as_object().unwrap() {
        let params = &case["params"];
        let text = |key: &str| params[key].as_str().map(String::from);
        let flag = |key: &str| params[key].as_bool().unwrap_or_default();
        let region = text("region").map(|region| {
            let file = fixtures().join(&region);
            if file.is_file() {
                file.to_str().unwrap().to_string()
            } else {
                region
            }
        });
        let mask = text("mask").map(|mask| {
            if name == "bare_lel" {
                return mask.replace("cube.im", "tests/data/imsubimage/cube.im");
            }
            if name == "mask_image" {
                return input_path(&mask).to_str().unwrap().to_string();
            }
            mask.replace(
                "\"cube.im",
                &format!("\"{}", input_path("cube.im").display()),
            )
            .replace(
                "\"plane.im",
                &format!("\"{}", input_path("plane.im").display()),
            )
            .replace(
                "\"stretch.im",
                &format!("\"{}", input_path("stretch.im").display()),
            )
        });
        let keepaxes = match &params["keepaxes"] {
            Json::Number(axis) => vec![axis.as_i64().unwrap()],
            Json::Array(axes) => axes.iter().map(|axis| axis.as_i64().unwrap()).collect(),
            Json::Null => Vec::new(),
            other => panic!("unexpected keepaxes {other}"),
        };
        let request = ImsubimageRequest {
            imagename: input_path(case["imagename"].as_str().unwrap()),
            outfile: if name == "empty" {
                PathBuf::new()
            } else {
                scratch.path().join(name)
            },
            box_pixels: text("box"),
            region,
            chans: text("chans"),
            stokes: text("stokes"),
            mask,
            keepaxes,
            dropdeg: flag("dropdeg"),
            stretch: flag("stretch"),
            ..ImsubimageRequest::default()
        };
        let result = imsubimage(&request);
        if case["error"].is_string() {
            let message = result.expect_err(name).to_string();
            let expected = match name.as_str() {
                "no_stretch" => "mask should not be extended",
                "drop_all" => "cannot drop all axes",
                "empty" => "outfile must be specified",
                unexpected => panic!("no assertion for error case {unexpected}"),
            };
            assert!(message.contains(expected), "{name}: {message}");
            assert!(!request.outfile.exists(), "{name}");
            continue;
        }
        result.unwrap_or_else(|error| panic!("{name}: {error}"));
        let path = &request.outfile;
        match (
            case["pixel_type"].as_str().unwrap(),
            AnyPagedImage::open(path).unwrap(),
        ) {
            ("float", AnyPagedImage::Float32(im)) => {
                check_image(im, case, name, |v| (f64::from(v), 0.0))
            }
            ("double", AnyPagedImage::Float64(im)) => check_image(im, case, name, |v| (v, 0.0)),
            ("complex", AnyPagedImage::Complex32(im)) => {
                check_image(im, case, name, |v: Complex32| {
                    (f64::from(v.re), f64::from(v.im))
                })
            }
            ("dcomplex", AnyPagedImage::Complex64(im)) => {
                check_image(im, case, name, |v: Complex64| (v.re, v.im))
            }
            (expected, other) => panic!("{name}: expected {expected}, got {other:?}"),
        }
    }
}

#[test]
fn failed_overwrite_preserves_image() {
    let scratch = tempfile::tempdir().unwrap();
    let mut request = ImsubimageRequest {
        imagename: input_path("plane.im"),
        outfile: scratch.path().join("out.im"),
        ..ImsubimageRequest::default()
    };
    imsubimage(&request).unwrap();
    let original = PagedImage::<f32>::open(&request.outfile)
        .unwrap()
        .get()
        .unwrap();
    request.box_pixels = Some("1,1,3,4".into());
    assert!(
        imsubimage(&request)
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );

    request.overwrite = true;
    request.dropdeg = true;
    request.keepaxes = vec![2];
    assert!(
        imsubimage(&request)
            .unwrap_err()
            .to_string()
            .contains("out of range")
    );
    let retained = PagedImage::<f32>::open(&request.outfile)
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(original, retained);

    request.keepaxes.clear();
    imsubimage(&request).unwrap();
    assert_eq!(
        PagedImage::<f32>::open(&request.outfile).unwrap().shape(),
        &[3, 4]
    );
}

#[test]
fn dropping_a_direction_axis_preserves_header_and_allows_copying() {
    let scratch = tempfile::tempdir().unwrap();
    let mut request = ImsubimageRequest {
        imagename: input_path("cube.im"),
        outfile: scratch.path().join("slice.im"),
        box_pixels: Some("2,0,2,5".into()),
        chans: Some("1".into()),
        stokes: Some("Q".into()),
        dropdeg: true,
        ..ImsubimageRequest::default()
    };
    imsubimage(&request).unwrap();
    let header = imhead(&request.outfile).unwrap();
    assert_eq!(header.axes.len(), 1);
    assert_eq!(header.axes[0].name, "Declination");
    assert_eq!(header.axes[0].unit, "'");

    request.imagename = request.outfile;
    request.outfile = scratch.path().join("copy.im");
    request.box_pixels = None;
    request.chans = None;
    request.stokes = None;
    imsubimage(&request).unwrap();
    assert_eq!(
        imhead(&request.outfile).unwrap().axes[0].name,
        "Declination"
    );
}

#[test]
fn a_transposed_crop_keeps_coordinates_on_their_data_axes() {
    let scratch = tempfile::tempdir().unwrap();
    let request = ImsubimageRequest {
        imagename: input_path("transposed.im"),
        outfile: scratch.path().join("slice.im"),
        box_pixels: Some("1,2,4,5".into()),
        chans: Some("2".into()),
        stokes: Some("Q".into()),
        dropdeg: true,
        keepaxes: vec![0],
        ..ImsubimageRequest::default()
    };
    imsubimage(&request).unwrap();
    let input = PagedImage::<f32>::open(&request.imagename).unwrap();
    let output = PagedImage::<f32>::open(&request.outfile).unwrap();
    assert_eq!(output.shape(), &[1, 4, 4]);
    assert_eq!(
        output.axis_names(),
        ["Frequency", "Right Ascension", "Declination"]
    );
    let world = input.coordinates().to_world(&[2.0, 4.0, 5.0, 1.0]).unwrap();
    let measured = output.coordinates().to_world(&[0.0, 3.0, 3.0]).unwrap();
    for (actual, expected) in measured.into_iter().zip(world) {
        close(
            actual,
            expected,
            "world coordinate of the retained last pixel",
        );
    }
    assert_eq!(
        output.get_at(&[0, 3, 3]).unwrap(),
        input.get_at(&[2, 4, 5, 1]).unwrap()
    );
}
