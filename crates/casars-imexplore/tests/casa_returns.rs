// SPDX-License-Identifier: LGPL-3.0-or-later

//! `imhead` and `imstat` return what CASA 6.7.6.14's return (#931): each
//! key casa-rs computes, in CASA's order, with CASA's type, dtype and shape,
//! as `tools/casa-compat/record_casa_returns.py` recorded them in
//! `resources/casa-compat/casa-6.7.6.14/returns.json`. The keys CASA
//! returns that casa-rs does not compute yet are listed here; the lists
//! only shrink (#977). A CASA parameter casa-rs takes only at its default
//! is refused at any other value, naming its issue.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_coordinates::{CoordinateSystem, LinearCoordinate};
use casa_images::{GaussianBeam, Image, ImageBeamSet, ImageInfo, ImageType};
use casa_provider_contracts::{CasaKey, CasaValue, FloatDtype, IntDtype, TaskReply};
use ndarray::{ArrayD, IxDyn};
use serde_json::{Value, json};

/// The 16 x 16 x 1 x 8 ramp the CASA recording made, with a beam or not.
fn write_ramp(path: &Path, beam: bool) {
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(LinearCoordinate::new(
        4,
        ["X", "Y", "S", "F"].map(String::from).to_vec(),
        ["pix"; 4].map(String::from).to_vec(),
    ));
    let shape = [16, 16, 1, 8];
    let mut image = Image::create(shape.to_vec(), coords, path).expect("create image");
    image.set_units("Jy/beam").expect("set units");
    let mut info = ImageInfo {
        image_type: ImageType::Intensity,
        ..ImageInfo::default()
    };
    if beam {
        let arcsec = std::f64::consts::PI / 180.0 / 3600.0;
        info.beam_set = ImageBeamSet::new(GaussianBeam::new(
            3.0 * arcsec,
            2.0 * arcsec,
            10f64.to_radians(),
        ));
    }
    image.set_image_info(&info).expect("set image info");
    let count = shape.iter().product::<usize>();
    let pixels = ArrayD::from_shape_vec(
        IxDyn(&shape),
        (0..count).map(|value| value as f32 / 100.0).collect(),
    )
    .expect("pixel array");
    image
        .put_slice(&pixels, &[0, 0, 0, 0])
        .expect("write pixels");
    image.save().expect("save image");
}

/// CASA's `ia.fromshape(path, [4, 4, 1, 4])` image: its default direction
/// (J2000 SIN at the origin, 1-arcminute pixels), Stokes I and spectral
/// (LSRK 1.415 GHz, 1 kHz channels, rest 1.420405752 GHz) coordinates,
/// every pixel 1, in `units`, with a 3' x 2' beam; `masked` gives it a
/// default mask that excludes every pixel.
fn write_fromshape(path: &Path, units: &str, masked: bool) {
    use casa_coordinates::{
        DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate, StokesCoordinate,
        StokesType,
    };
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    let arcmin = std::f64::consts::PI / 180.0 / 60.0;
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(
        DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-arcmin, arcmin],
            [2.0, 2.0],
        )
        .with_world_units(["'", "'"])
        .expect("arcminutes"),
    );
    coords.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coords.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.415e9,
        1.0e3,
        2.0,
        1.420_405_752e9,
    ));
    coords.obs_info_mut().observer = "Karl Jansky".into();
    coords.obs_info_mut().telescope = "ALMA".into();
    let shape = [4, 4, 1, 4];
    let mut image = Image::create(shape.to_vec(), coords, path).expect("create image");
    image.set_units(units).expect("set units");
    image
        .set_image_info(&ImageInfo {
            image_type: ImageType::Intensity,
            beam_set: ImageBeamSet::new(GaussianBeam::new(
                3.0 * arcmin,
                2.0 * arcmin,
                10f64.to_radians(),
            )),
            ..ImageInfo::default()
        })
        .expect("set image info");
    image
        .put_slice(&ArrayD::from_elem(IxDyn(&shape), 1.0f32), &[0, 0, 0, 0])
        .expect("write pixels");
    if masked {
        image.make_mask("none", true, false).expect("make mask");
    }
    image.save().expect("save image");
}

/// The entry `key` of `record`.
fn entry<'a>(record: &'a CasaValue, key: &str) -> &'a CasaValue {
    let CasaValue::Dict { entries } = record else {
        panic!("not a record: {record:?}");
    };
    &entries
        .iter()
        .find(|entry| entry.key == CasaKey::String(key.to_string()))
        .unwrap_or_else(|| panic!("no {key}"))
        .value
}

/// The one value of the one-element `float64` array `key` of `record`.
fn statistic(record: &CasaValue, key: &str) -> f64 {
    match entry(record, key) {
        CasaValue::FloatArray { data, shape, .. } if shape == &[1] => data[0].0,
        other => panic!("{key}: {other:?}"),
    }
}

/// The `float64` array `key` of `record`.
fn floats(record: &CasaValue, key: &str) -> Vec<f64> {
    match entry(record, key) {
        CasaValue::FloatArray { data, .. } => data.iter().map(|value| value.0).collect(),
        other => panic!("{key}: {other:?}"),
    }
}

/// The `str` array `key` of `record`.
fn texts<'a>(record: &'a CasaValue, key: &str) -> &'a [String] {
    match entry(record, key) {
        CasaValue::StrArray { data, .. } => data,
        other => panic!("{key}: {other:?}"),
    }
}

/// `imexplore SURFACE --json-run -` with `values`, in `workspace`.
fn json_run(workspace: &Path, surface: &str, values: Value) -> (Output, Option<TaskReply>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([surface, "--json-run", "-"])
        .current_dir(workspace)
        .env_remove("CASA_RS_STATE_DIR")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start imexplore");
    let request = json!({"surface": surface, "values": values}).to_string();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.as_bytes())
        .unwrap();
    let output = child.wait_with_output().expect("run imexplore");
    let reply = serde_json::from_slice(&output.stdout).ok();
    (output, reply)
}

fn returned(workspace: &Path, surface: &str, values: Value) -> CasaValue {
    let (output, reply) = json_run(workspace, surface, values);
    match reply {
        Some(TaskReply::Succeeded { value, .. }) => value,
        other => panic!(
            "{surface}: {other:?}\n{}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// `value`'s structure, as the CASA recording describes Python values.
fn describe(value: &CasaValue) -> Value {
    let array =
        |dtype: &str, shape: &[usize]| json!({"type": "ndarray", "dtype": dtype, "shape": shape});
    match value {
        CasaValue::None => json!({"type": "None"}),
        CasaValue::Bool { .. } => json!({"type": "bool"}),
        CasaValue::Int { .. } => json!({"type": "int"}),
        CasaValue::Float { .. } => json!({"type": "float"}),
        CasaValue::Complex { .. } => json!({"type": "complex"}),
        CasaValue::String { .. } => json!({"type": "str"}),
        CasaValue::List { items } => {
            json!({"type": "list", "items": items.iter().map(describe).collect::<Vec<_>>()})
        }
        CasaValue::Dict { entries } => json!({
            "type": "dict",
            "items": entries
                .iter()
                .map(|entry| match &entry.key {
                    CasaKey::String(key) => json!([key, "str", describe(&entry.value)]),
                    CasaKey::Int(key) => json!([key, "int", describe(&entry.value)]),
                })
                .collect::<Vec<_>>(),
        }),
        CasaValue::BoolArray { shape, .. } => array("bool", shape),
        CasaValue::IntArray { dtype, shape, .. } => array(
            match dtype {
                IntDtype::Int32 => "int32",
                IntDtype::Int64 => "int64",
            },
            shape,
        ),
        CasaValue::FloatArray { dtype, shape, .. } => array(
            match dtype {
                FloatDtype::Float32 => "float32",
                FloatDtype::Float64 => "float64",
            },
            shape,
        ),
        CasaValue::ComplexArray { shape, .. } => array("complex", shape),
        CasaValue::UInt64Array { shape, .. } => array("uint64", shape),
        // The width Python decodes it with, casatools' (`<U15`).
        CasaValue::StrArray { shape, data } => array(&CasaValue::str_dtype(data), shape),
    }
}

/// CASA's recorded return value for `case`.
fn casa(case: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../resources/casa-compat/casa-6.7.6.14/returns.json");
    let document: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(document["casa_version"], "6.7.6.14");
    document["cases"][case]["returns"].clone()
}

/// `ours` holds CASA's dictionary less exactly the keys `not_yet` names:
/// the same keys in the same order, each with CASA's structure.
fn assert_casa_dict(ours: &CasaValue, case: &str, not_yet: &[&str]) {
    let theirs = casa(case);
    let ours = describe(ours);
    assert_eq!(ours["type"], "dict", "{case}: {ours}");
    let expected = theirs["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| !not_yet.contains(&item[0].as_str().unwrap()))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(ours["items"].as_array().unwrap(), &expected, "{case}");
    for key in not_yet {
        assert!(
            theirs["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item[0] == *key),
            "{case}: CASA returns no {key:?}; drop it from the list"
        );
    }
}

#[test]
fn imstat_returns_casas_statistics_record() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("unbeamed.im");
    write_ramp(&image, false);
    let value = returned(
        workspace.path(),
        "imstat",
        json!({"imagename": image, "box": "0,0,3,3"}),
    );
    assert_casa_dict(
        &value,
        "imstat no beam",
        &[
            "blcf",
            "maxposf",
            "medabsdevmed",
            "minposf",
            "q1",
            "q3",
            "quartile",
            "trcf",
        ],
    );
}

/// `flux` is CASA's: integrated over the selected channels when there are
/// several, whether or not `chans` names them. CASA 6.7.6.14's `imstat` on
/// its own `fromshape` image gives these values.
#[test]
fn imstat_flux_is_casas_over_any_channel_selection() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ones.im");
    write_fromshape(&image, "Jy/beam", false);
    for (chans, expected) in [
        ("", 1.986_884_327_454_274_5),
        ("0~3", 1.986_884_327_454_274_5),
        ("0~1", 0.993_442_163_727_137_2),
        ("1", 2.353_446_4),
    ] {
        let record = returned(
            workspace.path(),
            "imstat",
            json!({"imagename": image, "chans": chans}),
        );
        let flux = statistic(&record, "flux");
        assert!(
            (flux - expected).abs() <= 1e-7 * expected,
            "chans {chans:?}: {flux} against CASA's {expected}"
        );
    }
    // A brightness in kelvin times the pixel area in square arcseconds.
    let kelvin = workspace.path().join("kelvin.im");
    write_fromshape(&kelvin, "K", false);
    let flux = statistic(
        &returned(workspace.path(), "imstat", json!({"imagename": kelvin})),
        "flux",
    );
    assert!((flux - 48_628.486_793_74).abs() <= 1e-7 * flux, "{flux}");
}

/// With no valid pixel, every statistic is an empty `float64` array, and
/// there are no extremum positions, as CASA 6.7.6.14 returns them.
#[test]
fn imstat_of_no_pixels_returns_empty_statistics() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("masked.im");
    write_fromshape(&image, "Jy/beam", true);
    let (output, reply) = json_run(workspace.path(), "imstat", json!({"imagename": image}));
    let Some(TaskReply::Succeeded { value, .. }) = reply else {
        panic!("{reply:?}\n{}", String::from_utf8_lossy(&output.stderr));
    };
    let CasaValue::Dict { entries } = &value else {
        panic!("{value:?}");
    };
    let keys = entries
        .iter()
        .map(|entry| match &entry.key {
            CasaKey::String(key) => key.as_str(),
            CasaKey::Int(_) => panic!("an integer key"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "blc", "flux", "max", "mean", "median", "min", "npts", "rms", "sigma", "sum", "sumsq",
            "trc"
        ]
    );
    for entry in entries {
        match &entry.value {
            CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape,
                data,
            } => assert!(shape == &[0] && data.is_empty(), "{:?}", entry.key),
            CasaValue::IntArray { .. } => {}
            other => panic!("{:?}: {other:?}", entry.key),
        }
    }
}

/// `imhead` presents each coordinate in the units the image stores it in,
/// and `hasmask` says whether a mask applies, as CASA 6.7.6.14's does for
/// its own `fromshape` image (values from CASA's `imhead`).
#[test]
fn imhead_presents_stored_units_and_the_active_mask_as_casa_does() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("head.im");
    write_fromshape(&image, "Jy/beam", false);
    let summary = returned(workspace.path(), "imhead", json!({"imagename": image}));
    assert_eq!(texts(&summary, "axisunits"), ["'", "'", "", "Hz"]);
    assert_eq!(floats(&summary, "incr"), [-1.0, 1.0, 1.0, 1000.0]);
    assert_eq!(floats(&summary, "refval"), [0.0, 0.0, 1.0, 1.415e9]);
    assert_eq!(floats(&summary, "refpix"), [2.0, 2.0, 0.0, 2.0]);
    assert_eq!(
        entry(&summary, "hasmask"),
        &CasaValue::Bool { value: false }
    );
    let list = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "list"}),
    );
    for (key, expected) in [
        ("cunit1", CasaValue::string("'")),
        ("cdelt1", CasaValue::float(-1.0)),
        ("cdelt2", CasaValue::float(1.0)),
        ("crval4", CasaValue::float(1.415e9)),
        ("cunit4", CasaValue::string("Hz")),
    ] {
        assert_eq!(entry(&list, key), &expected, "{key}");
    }
    // A stored mask that is not the default does not apply.
    let mut stored = Image::open(&image).expect("open image");
    stored
        .make_mask("allmasked", false, true)
        .expect("make mask");
    stored.save().expect("save image");
    drop(stored);
    let summary = returned(workspace.path(), "imhead", json!({"imagename": image}));
    assert_eq!(
        entry(&summary, "hasmask"),
        &CasaValue::Bool { value: false }
    );
    assert_eq!(entry(&summary, "defaultmask"), &CasaValue::string(""));
    assert_eq!(texts(&summary, "masks"), ["allmasked"]);
    let mut stored = Image::open(&image).expect("open image");
    stored.set_default_mask("allmasked").expect("set mask");
    stored.save().expect("save image");
    drop(stored);
    let summary = returned(workspace.path(), "imhead", json!({"imagename": image}));
    assert_eq!(entry(&summary, "hasmask"), &CasaValue::Bool { value: true });
    assert_eq!(
        entry(&summary, "defaultmask"),
        &CasaValue::string("allmasked")
    );
}

#[test]
fn imhead_returns_casas_summary_and_list_records_and_none_for_put() {
    let workspace = tempfile::tempdir().unwrap();
    // CASA's own coordinates, whose names and units set the string widths.
    let image = workspace.path().join("fromshape.im");
    write_fromshape(&image, "Jy/beam", false);
    let summary = returned(workspace.path(), "imhead", json!({"imagename": image}));
    assert_casa_dict(&summary, "imhead summary", &[]);
    let fixture: Value = serde_json::from_str(include_str!("data/imhead_casa.json")).unwrap();
    assert_eq!(texts(&summary, "messages"), [] as [&str; 0]);
    assert_eq!(
        entry(&summary, "messages").to_plain_json(),
        fixture["summary"]["messages"]
    );
    assert_eq!(
        entry(&summary, "tileshape"),
        &CasaValue::int64_vector([4, 4, 1, 4])
    );
    assert_eq!(
        entry(&summary, "tileshape").to_plain_json(),
        fixture["summary"]["tileshape"]
    );
    let list = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "list"}),
    );
    assert_casa_dict(&list, "imhead list", &[]);
    let put = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "put", "hdkey": "bunit", "hdvalue": "Jy"}),
    );
    assert_eq!(describe(&put), casa("imhead put"));
}

/// The returned tile shape describes storage, not the image dimensions.
#[test]
fn imhead_summary_reports_the_stored_tile_shape() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("tiled.im");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["pix".into(), "pix".into()],
    ));
    let mut image = casa_images::PagedImage::<f32>::create_with_tile_shape(
        vec![12, 10],
        vec![4, 5],
        coordinates,
        &path,
    )
    .unwrap();
    image.save().unwrap();
    drop(image);
    let summary = returned(workspace.path(), "imhead", json!({"imagename": path}));
    assert_eq!(entry(&summary, "shape"), &CasaValue::int64_vector([12, 10]));
    assert_eq!(
        entry(&summary, "tileshape"),
        &CasaValue::int64_vector([4, 5])
    );
    assert_eq!(texts(&summary, "messages"), [] as [&str; 0]);
}

/// Values recorded from CASA 6.7.6.14 on its `fromshape` image.
#[test]
fn imhead_get_returns_casa_header_values() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("head.im");
    write_fromshape(&image, "Jy/beam", false);
    let fixture: Value = serde_json::from_str(include_str!("data/imhead_casa.json")).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6-14");
    let quantity = |value, unit| {
        CasaValue::dict([
            ("unit", CasaValue::string(unit)),
            ("value", CasaValue::float(value)),
        ])
    };
    for (key, expected) in [
        ("bunit", CasaValue::string("Jy/beam")),
        ("BUNIT", CasaValue::string("Jy/beam")),
        ("object", CasaValue::string("")),
        ("imtype", CasaValue::string("Intensity")),
        ("observer", CasaValue::string("Karl Jansky")),
        ("telescope", CasaValue::string("ALMA")),
        ("equinox", CasaValue::string("J2000")),
        ("masks", CasaValue::str_vector(Vec::<String>::new())),
        ("shape", CasaValue::int64_vector([4, 4, 1, 4])),
        ("cdelt1", quantity(-1.0, "'")),
        ("cdelt3", quantity(1.0, "")),
        ("crval1", quantity(0.0, "'")),
        ("crval3", CasaValue::str_vector(["I"])),
        ("crpix1", CasaValue::float(2.0)),
        ("ctype1", CasaValue::string("Right Ascension")),
        ("cunit1", CasaValue::string("'")),
    ] {
        let actual = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": image, "mode": "get", "hdkey": key}),
        );
        assert_eq!(actual, expected, "{key}");
        assert_eq!(actual.to_plain_json(), fixture["get"][key], "{key}");
    }
    // CASA reports an absent user-defined key as an unknown keyword.
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "get", "hdkey": "unimplemented-user-key"}),
    );
    let Some(TaskReply::Failed { message, .. }) = reply else {
        panic!("expected a refusal: {reply:?}");
    };
    assert!(message.contains("Unknown keyword"), "{message}");
}

#[test]
fn imhead_get_reads_metadata_on_complex_images() {
    let workspace = tempfile::tempdir().unwrap();
    let fixture: Value = serde_json::from_str(include_str!("data/imhead_casa.json")).unwrap();
    let path = workspace.path().join("complex.im");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["pix".into(), "pix".into()],
    ));
    let mut image =
        casa_images::PagedImage::<casa_types::Complex32>::create(vec![4, 4], coordinates, &path)
            .unwrap();
    image
        .set_image_info(&ImageInfo {
            object_name: "Galaxy".into(),
            image_type: ImageType::Intensity,
            ..ImageInfo::default()
        })
        .unwrap();
    image.save().unwrap();
    drop(image);
    for (key, expected) in [("object", "Galaxy"), ("imtype", "Intensity")] {
        let value = returned(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "get", "hdkey": key
            }),
        );
        assert_eq!(value, CasaValue::string(expected), "{key}");
        assert_eq!(value.to_plain_json(), fixture["complex_get"][key], "{key}");
    }
}

#[test]
fn imhead_get_stokes_returns_all_plane_labels() {
    use casa_coordinates::{StokesCoordinate, StokesType};
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("stokes.im");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(LinearCoordinate::new(
        2,
        vec!["X".into(), "Y".into()],
        vec!["pix".into(), "pix".into()],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![
        StokesType::I,
        StokesType::Q,
        StokesType::U,
        StokesType::V,
    ]));
    let mut image =
        casa_images::PagedImage::<f32>::create(vec![4, 4, 4], coordinates, &path).unwrap();
    image.save().unwrap();
    drop(image);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({
            "imagename": path, "mode": "get", "hdkey": "crval3"
        }),
    );
    let fixture: Value = serde_json::from_str(include_str!("data/imhead_casa.json")).unwrap();
    assert_eq!(value, CasaValue::str_vector(["I", "Q", "U", "V"]));
    assert_eq!(value.to_plain_json(), fixture["stokes_get"]);
}

/// CASA-created images retain each beam quantity's own unit, including an
/// unwrapped position angle in `summary` and a wrapped angle in `get`/`list`.
#[test]
fn imhead_beam_records_preserve_casas_units_and_angle_convention() {
    fn compare_quantity(actual: &Value, expected: &Value, label: &str) {
        assert_eq!(actual["unit"], expected["unit"], "{label}");
        let a = actual["value"].as_f64().unwrap();
        let b = expected["value"].as_f64().unwrap();
        assert!((a - b).abs() < 1e-11, "{label}: {a} != {b}");
    }
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_beams");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("expected.json")).unwrap())
            .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for name in ["degrees", "radians", "radian_wrap", "negative"] {
        let filename = format!("{name}.im");
        let path = workspace.path().join(&filename);
        copy_tree(&fixture.join(filename), &path);
        let wanted = &expected["cases"][name];
        for key in ["beammajor", "beamminor", "beampa", "bmaj", "bmin", "bpa"] {
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": key}),
            );
            compare_quantity(&value.to_plain_json(), &wanted["get"][key], key);
        }
        let list = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "list"}),
        )
        .to_plain_json();
        for key in ["beammajor", "beamminor", "beampa"] {
            compare_quantity(&list[key], &wanted["list"][key], key);
        }
        let summary =
            returned(workspace.path(), "imhead", json!({"imagename": path})).to_plain_json();
        for key in ["major", "minor", "positionangle"] {
            compare_quantity(&summary["restoringbeam"][key], &wanted["summary"][key], key);
        }
    }
    for name in ["none", "multiple"] {
        let filename = format!("{name}.im");
        let path = workspace.path().join(&filename);
        copy_tree(&fixture.join(filename), &path);
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": "get", "hdkey": "beammajor"
            }),
        );
        let Some(TaskReply::Failed { message, .. }) = reply else {
            panic!("{name}: expected a CASA error, got {reply:?}");
        };
        assert!(
            message.contains(expected["errors"][name].as_str().unwrap()),
            "{name}: {message}"
        );
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for item in std::fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        let dest = to.join(item.file_name());
        if item.file_type().unwrap().is_dir() {
            copy_tree(&item.path(), &dest);
        } else {
            std::fs::copy(item.path(), dest).unwrap();
        }
    }
}

#[test]
fn imhead_reports_casas_per_plane_beams_and_median() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_beams");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("multi_expected.json")).unwrap())
            .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for name in ["multiple", "multiple_mixed", "spaced_units", "nearly_equal"] {
        let path = workspace.path().join(format!("{name}.im"));
        copy_tree(&fixture.join(format!("{name}.im")), &path);
        for mode in ["summary", "list"] {
            let actual = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": mode}),
            )
            .to_plain_json();
            assert_eq!(
                actual["perplanebeams"], expected["cases"][name][mode],
                "{name} {mode}"
            );
        }
    }
    let dual = workspace.path().join("single_dual.im");
    copy_tree(&fixture.join("single_dual.im"), &dual);
    let summary = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": dual, "mode": "summary"}),
    )
    .to_plain_json();
    let listed = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": dual, "mode": "list"}),
    )
    .to_plain_json();
    assert_eq!(summary["restoringbeam"], expected["single_dual"]["summary"]);
    for key in ["beammajor", "beamminor", "beampa"] {
        assert_eq!(listed[key], expected["single_dual"]["list"][key]);
    }
    for name in expected["invalid"].as_object().unwrap().keys() {
        let path = workspace.path().join(format!("{name}.im"));
        copy_tree(&fixture.join(format!("{name}.im")), &path);
        for mode in ["summary", "list", "get"] {
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": mode, "hdkey": "bunit"}),
            );
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{name} {mode}: {reply:?}"
            );
        }
        let table = std::fs::read(path.join("table.dat")).unwrap();
        for key in ["object", "custom"] {
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "put", "hdkey": key, "hdvalue": "must not be written"}),
            );
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{name} put {key}: {reply:?}"
            );
            assert_eq!(
                std::fs::read(path.join("table.dat")).unwrap(),
                table,
                "{name} put {key}"
            );
        }
    }
}

#[test]
fn imhead_spectral_metadata_uses_casas_stored_world_units() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_spectral");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("expected.json")).unwrap())
            .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for unit in ["Hz", "GHz", "MHz"] {
        let filename = format!("{unit}.im");
        let path = workspace.path().join(&filename);
        copy_tree(&fixture.join(filename), &path);
        let wanted = &expected["cases"][unit];
        let get = |key| {
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": key}),
            )
            .to_plain_json()
        };
        let reference = get("restfreq");
        assert_eq!(reference["unit"], wanted["get_restfreq"]["unit"]);
        let actual = reference["value"].as_f64().unwrap();
        let target = wanted["get_restfreq"]["value"].as_f64().unwrap();
        assert!(
            (actual - target).abs() <= target.abs() * 1e-12,
            "{unit}: {actual} != {target}"
        );
        assert_eq!(get("reffreqtype"), wanted["get_reffreqtype"]);
        let list = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "list"}),
        )
        .to_plain_json();
        assert_eq!(list["reffreqtype"], wanted["list_reffreqtype"]);
        let listed = list["restfreq"][0].as_f64().unwrap();
        let listed_target = wanted["list_restfreq"][0].as_f64().unwrap();
        assert!((listed - listed_target).abs() <= listed_target.abs() * 1e-12);
    }
}

#[test]
fn imhead_projection_matches_casa_for_standard_slant_and_ncp() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_projection");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for name in ["sin", "tan", "slant", "ncp"] {
        let filename = format!("{name}.im");
        let path = workspace.path().join(&filename);
        copy_tree(&fixture.join(filename), &path);
        let result = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "projection"}),
        );
        assert_eq!(result.to_plain_json(), expected["cases"][name], "{name}");
        let list = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "list"}),
        );
        assert_eq!(
            entry(&list, "projection").to_plain_json(),
            expected["cases"][name],
            "{name}"
        );
    }
}

#[test]
fn imhead_observation_date_matches_casas_epoch_format() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_dates");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for name in ["missing", "j2000", "noon", "fraction"] {
        let filename = format!("{name}.im");
        let path = workspace.path().join(&filename);
        copy_tree(&fixture.join(filename), &path);
        for key in ["date-obs", "epoch"] {
            let result = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "get", "hdkey": key}),
            );
            assert_eq!(
                result.to_plain_json(),
                expected["cases"][name][key],
                "{name} {key}"
            );
        }
        let list = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "list"}),
        );
        assert_eq!(
            entry(&list, "date-obs").to_plain_json(),
            expected["cases"][name]["list"],
            "{name}"
        );
    }
}

#[test]
fn imhead_extrema_match_casa_with_masks_ties_and_complex_pixels() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_extrema");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for name in [
        "ordinary",
        "partial",
        "masked",
        "complex",
        "ties",
        "nanfirst",
        "nanlater",
        "infinities",
        "four",
        "linear",
        "galactic",
        "tiled",
        "hadec180",
        "ecliptic180",
    ] {
        let filename = format!("{name}.im");
        let path = workspace.path().join(&filename);
        copy_tree(&fixture.join(filename), &path);
        let listed = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "list"}),
        )
        .to_plain_json();
        for key in [
            "datamin",
            "datamax",
            "minpixpos",
            "maxpixpos",
            "minpos",
            "maxpos",
        ] {
            let wanted = &expected["cases"][name][key];
            if let Some(error) = wanted.get("error") {
                let (_, reply) = json_run(
                    workspace.path(),
                    "imhead",
                    json!({"imagename": path, "mode": "get", "hdkey": key}),
                );
                let Some(TaskReply::Failed { message, .. }) = reply else {
                    panic!("{name} {key}: {reply:?}")
                };
                assert!(
                    message.contains(error.as_str().unwrap()),
                    "{name} {key}: {message}"
                );
                assert!(
                    listed.get(key).is_none(),
                    "{name}: unexpectedly listed {key}"
                );
            } else {
                let value = returned(
                    workspace.path(),
                    "imhead",
                    json!({"imagename": path, "mode": "get", "hdkey": key}),
                );
                if key.ends_with("pixpos") {
                    assert!(
                        matches!(
                            value,
                            CasaValue::IntArray {
                                dtype: IntDtype::Int64,
                                ..
                            }
                        ),
                        "{name} {key}"
                    );
                }
                assert_eq!(value.to_plain_json(), *wanted, "{name} {key}");
                assert_eq!(listed[key], *wanted, "{name} list {key}");
            }
        }
    }
}

#[path = "casa_returns/imhead_date_edits.rs"]
mod imhead_date_edits;

#[path = "casa_returns/imhead_restfreq_add.rs"]
mod imhead_restfreq_add;

#[path = "casa_returns/imhead_projection_edits.rs"]
mod imhead_projection_edits;

#[path = "casa_returns/imhead_custom_arrays.rs"]
mod imhead_custom_arrays;

#[path = "casa_returns/imhead_custom_records.rs"]
mod imhead_custom_records;

#[path = "casa_returns/imhead_custom_matrices.rs"]
mod imhead_custom_matrices;

#[path = "casa_returns/imhead_stokes_edits.rs"]
mod imhead_stokes_edits;

#[path = "casa_returns/imhead_custom_mixed.rs"]
mod imhead_custom_mixed;

#[path = "casa_returns/imhead_direction_edits.rs"]
mod imhead_direction_edits;

#[path = "casa_returns/imhead_custom_cubes.rs"]
mod imhead_custom_cubes;

#[path = "casa_returns/imhead_mutations.rs"]
mod imhead_mutations;

#[path = "casa_returns/imhead_list_log.rs"]
mod imhead_list_log;

/// Verbose changes the human-readable beam listing, not the returned record.
#[test]
fn imhead_verbose_summary_returns_casas_record() {
    let workspace = tempfile::tempdir().unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let image = workspace.path().join("verbose_beams.im");
    copy_tree(&fixture.join("verbose_beams.im"), &image);
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("verbose_expected.json")).unwrap())
            .unwrap();
    let table = std::fs::read(image.join("table.dat")).unwrap();
    let mut returns = Vec::new();
    for verbose in [false, true] {
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename": image, "mode": "summary", "verbose": verbose}),
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let Some(TaskReply::Succeeded { value, .. }) = reply else {
            panic!("{reply:?}")
        };
        assert_eq!(
            value.to_plain_json(),
            oracle["summary"][verbose.to_string()],
            "verbose={verbose}"
        );
        let text = String::from_utf8_lossy(&output.stderr);
        assert!(text.contains("Restoring beams: 4 channels, 4 polarizations"));
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("channel ") && line.contains("polarization "))
                .count(),
            if verbose { 16 } else { 12 }
        );
        returns.push(value);
    }
    assert_eq!(returns[0], returns[1]);
    assert_eq!(std::fs::read(image.join("table.dat")).unwrap(), table);
}

/// CASA's `imhead(image, mode='put', hdkey='bunit', hdvalue='')` clears the
/// brightness unit, whether the empty value is given or left at its
/// default, as the Python wrapper leaves it, and on the command line.
#[test]
fn imhead_put_of_an_empty_bunit_clears_it() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.im");
    let units = || {
        casa_images::PagedImage::<f32>::open(&image)
            .unwrap()
            .units()
            .to_string()
    };
    for values in [
        json!({"mode": "put", "hdkey": "bunit", "hdvalue": ""}),
        json!({"mode": "put", "hdkey": "BUNIT"}),
    ] {
        write_ramp(&image, true);
        assert_eq!(units(), "Jy/beam");
        let mut request = values.clone();
        request["imagename"] = json!(image);
        let put = returned(workspace.path(), "imhead", request);
        assert_eq!(put, CasaValue::None, "{values}");
        assert_eq!(units(), "", "{values}");
    }
    write_ramp(&image, true);
    let command_line = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([
            "imhead",
            image.to_str().unwrap(),
            "--mode",
            "put",
            "--hdkey",
            "bunit",
        ])
        .arg("--no-save-last")
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(
        command_line.status.success(),
        "{}",
        String::from_utf8_lossy(&command_line.stderr)
    );
    assert_eq!(units(), "");
    // Another key is refused at hdkey, before the image is touched.
    write_ramp(&image, true);
    let (output, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "put", "hdkey": "shape", "hdvalue": "M31"}),
    );
    assert_eq!(output.status.code(), Some(2));
    let Some(TaskReply::Failed {
        message, parameter, ..
    }) = reply
    else {
        panic!("{reply:?}");
    };
    assert_eq!(parameter.as_deref(), Some("hdkey"));
    assert!(message.contains("#977"), "{message}");
    assert_eq!(units(), "Jy/beam");
}

#[test]
fn a_parameter_casa_rs_takes_only_at_its_default_is_refused_otherwise() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.im");
    write_ramp(&image, false);
    let (output, reply) = json_run(
        workspace.path(),
        "imstat",
        json!({"imagename": image, "axes": [0, 1]}),
    );
    assert_eq!(output.status.code(), Some(2));
    let Some(TaskReply::Failed {
        message, parameter, ..
    }) = reply
    else {
        panic!("{reply:?}");
    };
    assert_eq!(parameter.as_deref(), Some("axes"));
    assert!(message.contains("#977"), "{message}");
    // CASA's default is accepted, on the command line as from Python.
    let (output, _) = json_run(
        workspace.path(),
        "imstat",
        json!({"imagename": image, "axes": []}),
    );
    assert!(output.status.success());
    let command_line = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([
            "imstat",
            image.to_str().unwrap(),
            "--stokes",
            "I",
            "--no-save-last",
        ])
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert_eq!(command_line.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&command_line.stderr).contains("#977"),
        "{}",
        String::from_utf8_lossy(&command_line.stderr)
    );
}

/// imstat takes CASA's own spellings of its defaults and ignores what CASA
/// ignores under the classic algorithm; CASA 6.7.6.14 runs each of these
/// with classic's statistics.
#[test]
fn imstat_takes_casas_spellings_of_its_defaults() {
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("ramp.im");
    write_ramp(&image, false);
    let expected = statistic(
        &returned(workspace.path(), "imstat", json!({"imagename": image})),
        "sum",
    );
    for values in [
        // CASA's algorithm and clmethod are case-blind and minimum match;
        // its axes=-1 is its [].
        json!({"algorithm": "CLASSIC", "clmethod": "AU", "axes": -1}),
        json!({"algorithm": "cl", "axes": [-1]}),
        // The robust algorithms' controls, and stretch without a mask.
        json!({"fence": 5.0, "center": "median", "lside": false, "zscore": 3.0,
               "maxiter": 2, "niter": 7, "stretch": true, "append": false}),
    ] {
        let mut request = values.clone();
        request["imagename"] = json!(image);
        let record = returned(workspace.path(), "imstat", request);
        assert_eq!(statistic(&record, "sum"), expected, "{values}");
    }
    // The command line reads them the same way.
    let command_line = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([
            "imstat",
            image.to_str().unwrap(),
            "--algorithm",
            "Classic",
            "--axes=-1",
        ])
        .args(["--fence", "5", "--stretch", "--no-save-last"])
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(
        command_line.status.success(),
        "{}",
        String::from_utf8_lossy(&command_line.stderr)
    );
    // Another algorithm is not yet, and with it the control it reads.
    let (output, reply) = json_run(
        workspace.path(),
        "imstat",
        json!({"imagename": image, "algorithm": "chauvenet", "zscore": 3.0}),
    );
    assert_eq!(output.status.code(), Some(2));
    let Some(TaskReply::Failed {
        message, parameter, ..
    }) = reply
    else {
        panic!("{reply:?}");
    };
    assert_eq!(parameter.as_deref(), Some("algorithm"));
    assert!(
        message.contains("#977") && message.contains("zscore"),
        "{message}"
    );
}

/// An absent or missing `imagename` is a refusal naming it (exit 2), as
/// CASA's `imstat('does-not-exist.im')` is a parameter error.
#[test]
fn imagename_is_checked_when_the_task_runs() {
    let workspace = tempfile::tempdir().unwrap();
    for (surface, values) in [
        ("imstat", json!({})),
        ("imstat", json!({"imagename": "does-not-exist.im"})),
        ("imhead", json!({"imagename": "does-not-exist.im"})),
    ] {
        let (output, reply) = json_run(workspace.path(), surface, values.clone());
        assert_eq!(output.status.code(), Some(2), "{surface} {values}");
        let Some(TaskReply::Failed { parameter, .. }) = reply else {
            panic!("{reply:?}");
        };
        assert_eq!(
            parameter.as_deref(),
            Some("imagename"),
            "{surface} {values}"
        );
    }
}
