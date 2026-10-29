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
