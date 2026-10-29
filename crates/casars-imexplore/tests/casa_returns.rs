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
        CasaValue::StrArray { shape, .. } => array("str", shape),
    }
}

/// A recorded CASA structure with NumPy's sized string dtypes (`<U15`)
/// read as `str`.
fn plain_strings(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, item)| match item.as_str() {
                    Some(dtype) if key == "dtype" && dtype.starts_with("<U") => {
                        (key.clone(), json!("str"))
                    }
                    _ => (key.clone(), plain_strings(item)),
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(plain_strings).collect()),
        other => other.clone(),
    }
}

/// CASA's recorded return value for `case`.
fn casa(case: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../resources/casa-compat/casa-6.7.6.14/returns.json");
    let document: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(document["casa_version"], "6.7.6.14");
    plain_strings(&document["cases"][case]["returns"])
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
    let image = workspace.path().join("ramp.im");
    write_ramp(&image, true);
    let summary = returned(workspace.path(), "imhead", json!({"imagename": image}));
    assert_casa_dict(&summary, "imhead summary", &["messages", "tileshape"]);
    let list = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "list"}),
    );
    assert_casa_dict(
        &list,
        "imhead list",
        &[
            "datamax",
            "datamin",
            "date-obs",
            "equinox",
            "maxpixpos",
            "maxpos",
            "minpixpos",
            "minpos",
            "observer",
            "projection",
            "reffreqtype",
            "restfreq",
            "telescope",
        ],
    );
    let put = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": image, "mode": "put", "hdkey": "bunit", "hdvalue": "Jy"}),
    );
    assert_eq!(describe(&put), casa("imhead put"));
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

#[test]
fn imagename_is_checked_when_the_task_runs() {
    let workspace = tempfile::tempdir().unwrap();
    let (output, reply) = json_run(workspace.path(), "imstat", json!({}));
    assert_eq!(output.status.code(), Some(2));
    let Some(TaskReply::Failed { parameter, .. }) = reply else {
        panic!("{reply:?}");
    };
    assert_eq!(parameter.as_deref(), Some("imagename"));
}
