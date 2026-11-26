// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 snapshots from tools/casa-compat/imsubimage_oracle.py.

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateType;
use casa_core::{ArrayValue, Complex32, Complex64, ScalarValue, Value};
use casa_imageanalysis::analysis::{SubimageOptions, SubimagePlan, SubimageRegion};
use casa_imageanalysis::{
    ImpbcorRequest, ImpvRequest, ImsubimageRequest, PbcorRegion, PvPosition, imhead, impbcor, impv,
    imsubimage,
};
use casa_images::image_history_entries;
use casa_images::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImagePixel, PagedImage, TempImage, TempStoragePolicy,
};
use serde_json::Value as Json;

#[test]
fn the_image_editor_uses_the_same_strided_section_geometry() {
    let input = PagedImage::<f32>::open(input_path("cube.im")).unwrap();
    let region = casa_tables::read_table_record_file(&input_path("stride.rgn")).unwrap();
    let (plan, stride) =
        SubimagePlan::for_edit(&input, Some(SubimageRegion::Record(&region))).unwrap();
    assert_eq!(plan.shape, [3, 3, 4, 2]);
    assert_eq!(stride, [2, 2, 1, 2]);
    let (start, end) = plan.bounds();
    assert_eq!(start, [0, 1, 0, 0]);
    assert_eq!(end, [4, 5, 3, 2]);
    let from = input.coordinates().to_world(&[4.0, 5.0, 3.0, 2.0]).unwrap();
    let actual = plan.coordinates.to_world(&[2.0, 2.0, 3.0, 1.0]).unwrap();
    for (actual, expected) in actual.into_iter().zip(from) {
        close(actual, expected, "editor strided WCS");
    }
}

#[test]
fn a_strided_array_section_can_reach_the_edge_without_enabling_dense_complex_copy() {
    let cube = PagedImage::<f32>::open(input_path("cube.im")).unwrap();
    let mut input = TempImage::new(
        cube.shape().to_vec(),
        cube.coordinates().clone(),
        TempStoragePolicy::Memory,
    )
    .unwrap();
    let pixels = cube.get_slice(&[0, 0, 0, 0], cube.shape()).unwrap();
    input.put_slice(&pixels, &[0, 0, 0, 0]).unwrap();
    let region = casa_tables::read_table_record_file(&input_path("stride.rgn")).unwrap();
    let (plan, _) = SubimagePlan::for_edit(&input, Some(SubimageRegion::Record(&region))).unwrap();
    let mut output = TempImage::<f32>::new(
        plan.shape.clone(),
        plan.coordinates.clone(),
        TempStoragePolicy::Memory,
    )
    .unwrap();
    plan.copy_to(&input, &mut output, None, false).unwrap();
    let values = output.get_slice(&[0, 0, 0, 0], &plan.shape).unwrap();
    assert_eq!(values[[2, 2, 3, 1]], pixels[[4, 5, 3, 2]]);

    let mut dense_region = region.clone();
    dense_region.upsert("inc", Value::Array(ArrayValue::from_f64_vec(vec![1.0; 4])));
    let mut complex = TempImage::<Complex32>::new(
        plan.shape.clone(),
        plan.coordinates.clone(),
        TempStoragePolicy::Memory,
    )
    .unwrap();
    complex.set_units("untouched").unwrap();
    let error = plan
        .make_complex_to(
            &input,
            &input,
            &mut complex,
            Some(SubimageRegion::Record(&dense_region)),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires a unit-stride image section"),
        "{error}"
    );
    assert_eq!(complex.units(), "untouched");
}

#[test]
fn compound_regions_still_require_multi_region_task_support() {
    let scratch = tempfile::tempdir().unwrap();
    let input = input_path("cube.im");
    let region = input_path("saved_compound_complement.rgn");
    let pv = scratch.path().join("pv.im");
    let corrected = scratch.path().join("corrected.im");
    let pv_request = ImpvRequest {
        imagename: input.clone(),
        outfile: pv.clone(),
        region: Some(PbcorRegion::Text(region.to_str().unwrap().to_string())),
        start: Some(PvPosition::Integral([2, 2])),
        end: Some(PvPosition::Integral([3, 2])),
        ..ImpvRequest::default()
    };
    let correction_request = ImpbcorRequest {
        imagename: input.clone(),
        pbimage: input.into(),
        outfile: corrected.clone(),
        mode: "divide".into(),
        cutoff: -1.0,
        box_selection: None,
        region: Some(PbcorRegion::Text(region.to_str().unwrap().to_string())),
        chans: None,
        stokes: None,
        mask: None,
        stretch: false,
        overwrite: false,
    };
    for result in [
        impv(&pv_request).unwrap_err(),
        impbcor(&correction_request).unwrap_err(),
    ] {
        let message = result.to_string();
        assert!(
            message.contains("Only a single n-dimensional rectangular region is supported"),
            "{message}"
        );
    }
    assert!(!pv.exists());
    assert!(!corrected.exists());
}

#[test]
fn crtf_pair_reader_consumes_one_adjacent_parenthesis() {
    let input = PagedImage::<f32>::open(input_path("cube.im")).unwrap();
    for (text, accepted) in [
        ("box[[1pix,1pix],[3pix,3pix]])", true),
        ("box[[1pix,1pix],[3pix,3pix]]))", false),
        ("circle[[2pix,2pix],1pix])", false),
    ] {
        let selection = SubimagePlan::new(
            &input,
            None,
            SubimageOptions {
                region: Some(SubimageRegion::Text(text)),
                dropdeg: false,
                keepaxes: &[],
            },
        );
        assert_eq!(selection.is_ok(), accepted, "{text}");
    }
}

#[test]
fn wrong_region_kind_is_rejected_for_compounds_and_their_children() {
    let input = PagedImage::<f32>::open(input_path("cube.im")).unwrap();
    let region =
        casa_tables::read_table_record_file(&input_path("saved_compound_union.rgn")).unwrap();
    for change_child in [false, true] {
        let mut invalid_region = region.clone();
        let bad = if change_child {
            let Some(Value::Record(children)) = invalid_region.get_mut("regions") else {
                panic!("compound regions");
            };
            let Some(Value::Record(first)) = children.get_mut("*1") else {
                panic!("first compound member");
            };
            first
        } else {
            &mut invalid_region
        };
        bad.upsert("isRegion", Value::Scalar(ScalarValue::Int32(1)));
        let error = SubimagePlan::new(
            &input,
            None,
            SubimageOptions {
                region: Some(SubimageRegion::Record(&invalid_region)),
                dropdeg: false,
                keepaxes: &[],
            },
        )
        .err()
        .expect("world region tag required");
        assert!(
            error
                .to_string()
                .contains("LC/pixel regions are not supported"),
            "{error}"
        );
    }
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/imsubimage")
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
    let history = image_history_entries(image.name().unwrap()).unwrap();
    let copied = history
        .iter()
        .find(|entry| entry.message == "imsubimage oracle input history")
        .map(|entry| {
            serde_json::json!([
                entry.time_mjd_seconds,
                entry.priority,
                entry.location,
                entry.message
            ])
        });
    assert_eq!(
        copied.unwrap_or(Json::Null),
        oracle["copied_history"],
        "{case}"
    );
    assert!(
        history
            .iter()
            .any(|entry| entry.message.contains("imsubimage(")),
        "{case}"
    );
}

#[test]
fn subimage_matches_casa_6_7_6_14() {
    let document: Json = serde_json::from_str(include_str!(
        "../../../../libs/casacore/casa-images/tests/data/imsubimage/expected.json"
    ))
    .unwrap();
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
            [
                "cube.im",
                "plane.im",
                "stretch.im",
                "double.im",
                "complex.im",
            ]
            .into_iter()
            .fold(mask, |expr, name| {
                expr.replace(
                    &format!("\"{name}"),
                    &format!("\"{}", input_path(name).display()),
                )
            })
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
                "named_double_mask" => "unsupported data type",
                "saved_box_conflict" | "saved_chans_conflict" | "saved_stokes_conflict" => {
                    "saved region cannot be combined"
                }
                "saved_relative_disjoint" => "does not intersect the image",
                "saved_relative_wide" => "SIN deproject: point outside unit circle",
                "saved_relative_ellipse_far_side_invalid" => {
                    "SIN project: point outside visible hemisphere"
                }
                "saved_relative_polygon_default_invalid" => "default units are not allowed",
                "saved_compound_nonoverlap" => "regions do not overlap",
                "saved_axis_stretch_invalid" => "a stretch axis does not have length 1",
                "saved_axis_concat_invalid" => "number of regions has to match the range",
                "saved_axis_concat_frame_invalid" => "region axis is unknown or inconsistent",
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
    request.region = Some(input_path("saved.rgn").to_str().unwrap().to_string());
    assert!(
        imsubimage(&request)
            .unwrap_err()
            .to_string()
            .contains("saved region cannot be combined")
    );
    assert_eq!(
        PagedImage::<f32>::open(&request.outfile)
            .unwrap()
            .get()
            .unwrap(),
        original
    );
    request.region = None;
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
    let mut output = PagedImage::<f32>::open(&request.outfile).unwrap();
    let original = output.set_reference_value(0, 3.0).unwrap();
    close(original, 0.0, "original declination reference");
    let circular = GaussianBeam::new(0.001, 0.001, 0.0);
    assert!(
        casa_imageanalysis::convolve2d(&output, &circular)
            .unwrap_err()
            .to_string()
            .contains("both direction axes")
    );
    assert!(
        casa_coordinates::fits::try_to_fits_header(output.coordinates(), output.shape())
            .unwrap_err()
            .to_string()
            .contains("both celestial pixel axes")
    );

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

#[test]
fn overwrite_cannot_replace_a_directory_containing_the_input() {
    let scratch = tempfile::tempdir().unwrap();
    let work = scratch.path().join("work");
    std::fs::create_dir(&work).unwrap();
    let sentinel = work.join("sentinel");
    std::fs::write(&sentinel, "keep").unwrap();
    let mut request = ImsubimageRequest {
        imagename: input_path("plane.im"),
        outfile: work.join("input.im"),
        ..ImsubimageRequest::default()
    };
    imsubimage(&request).unwrap();
    request.imagename = request.outfile;
    request.outfile = work;
    request.overwrite = true;
    assert!(
        imsubimage(&request)
            .unwrap_err()
            .to_string()
            .contains("contains input")
    );
    assert_eq!(std::fs::read_to_string(sentinel).unwrap(), "keep");
    assert_eq!(
        PagedImage::<f32>::open(&request.imagename).unwrap().shape(),
        &[6, 6]
    );
}
