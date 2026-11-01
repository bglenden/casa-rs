// SPDX-License-Identifier: LGPL-3.0-or-later

//! `tclean` returns what CASA 6.7.6.14's does (#931): the summary record,
//! each key in CASA's order with CASA's type, as
//! `tools/casa-compat/record_casa_returns.py` recorded it in
//! `resources/casa-compat/casa-6.7.6.14/returns.json`. Lists and arrays
//! follow the run's cycles, so their lengths are not compared, and NumPy's
//! `float64` is Python's `float`. A value casa-rs cannot honour is refused
//! at its parameter.

use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_provider_contracts::{CasaKey, CasaValue, IntDtype, TaskReply};
use serde_json::{Value, json};

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// `casars-imager --json-run -` with tclean `values`, in `directory`.
pub(super) fn json_run(directory: &Path, values: Value) -> TaskReply {
    let mut child = Command::new(env!("CARGO_BIN_EXE_casars-imager"))
        .args(["--json-run", "-"])
        .current_dir(directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start casars-imager");
    let request = json!({"surface": "tclean", "values": values}).to_string();
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(request.as_bytes())
        .expect("write the request");
    let output = child.wait_with_output().expect("casars-imager output");
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "reply: {error}\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// `value`'s structure as the CASA recording describes Python values, with
/// a list's items as its first item's.
fn describe(value: &CasaValue) -> Value {
    match value {
        CasaValue::None => json!({"type": "None"}),
        CasaValue::Bool { .. } => json!({"type": "bool"}),
        CasaValue::Int { .. } => json!({"type": "int"}),
        CasaValue::Float { .. } => json!({"type": "float"}),
        CasaValue::String { .. } => json!({"type": "str"}),
        CasaValue::List { items } => {
            json!({"type": "list", "items": items.first().map(describe).into_iter().collect::<Vec<_>>()})
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
        CasaValue::IntArray { dtype, shape, .. } => json!({
            "type": "ndarray",
            "dtype": match dtype {
                IntDtype::Int32 => "int32",
                IntDtype::Int64 => "int64",
            },
            "ndim": shape.len(),
        }),
        other => panic!("tclean returns no {other:?}"),
    }
}

/// A recorded CASA structure as [`describe`] gives it.
fn comparable(value: &Value) -> Value {
    match value {
        Value::Object(fields) if fields.get("type") == Some(&json!("ndarray")) => json!({
            "type": "ndarray",
            "dtype": fields["dtype"],
            "ndim": fields["shape"].as_array().expect("shape").len(),
        }),
        Value::Object(fields) if fields.get("type") == Some(&json!("numpy.float64")) => {
            json!({"type": "float"})
        }
        Value::Object(fields) if fields.get("type") == Some(&json!("list")) => json!({
            "type": "list",
            "items": fields["items"]
                .as_array()
                .expect("items")
                .first()
                .map(comparable)
                .into_iter()
                .collect::<Vec<_>>(),
        }),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), comparable(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(comparable).collect()),
        other => other.clone(),
    }
}

/// CASA's recorded return for `case`, comparable with [`describe`].
fn casa(case: &str) -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/casa-compat/casa-6.7.6.14/returns.json"
    );
    let document: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("returns")).expect("JSON");
    let recorded = &document["cases"][case]["returns"];
    assert!(!recorded.is_null(), "no recorded return for {case}");
    comparable(recorded)
}

fn entry<'a>(value: &'a CasaValue, key: &str) -> &'a CasaValue {
    let CasaValue::Dict { entries } = value else {
        panic!("not a dictionary: {value:?}");
    };
    &entries
        .iter()
        .find(|entry| entry.key == CasaKey::String(key.to_string()))
        .unwrap_or_else(|| panic!("no {key}"))
        .value
}

fn scalar(value: &CasaValue, key: &str) -> f64 {
    let CasaValue::Float { value } = entry(value, key) else {
        panic!("{key} is a float");
    };
    value.0
}

fn last_minor_threshold(value: &CasaValue) -> f64 {
    let mut plane = entry(value, "summaryminor");
    for _ in 0..3 {
        let CasaValue::Dict { entries } = plane else {
            panic!("minor summary dictionary")
        };
        plane = &entries[0].value;
    }
    let CasaValue::List { items } = entry(plane, "cycleThresh") else {
        panic!("threshold list")
    };
    let CasaValue::Float { value } = items.last().unwrap() else {
        panic!("threshold float")
    };
    value.0
}

#[test]
fn tclean_returns_casas_summary_record() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("casa-return");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let base = json!({
        "vis": observation.measurement_set(),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu",
    });
    let run = |name: &str, controls: Value| {
        let mut values = base.clone();
        values["imagename"] = json!(directory.join(name));
        for (key, value) in controls.as_object().expect("controls") {
            values[key] = value.clone();
        }
        match json_run(&directory, values) {
            TaskReply::Succeeded { value, .. } => value,
            other => panic!("{name}: {other:?}"),
        }
    };

    let dirty = run("dirty", json!({}));
    assert_eq!(describe(&dirty), casa("tclean niter0"));
    assert_eq!(entry(&dirty, "stopcode"), &CasaValue::Int { value: 3 });
    assert_eq!(entry(&dirty, "nmajordone"), &CasaValue::Int { value: 1 });
    // Sub-parameters CASA's tclean reads only under other values (its task
    // XML), set where it does not read them: natural weighting, a user mask,
    // the standard gridder's AW controls.
    let ignored = run(
        "ignored",
        json!({ "npixels": 3, "noise": "2Jy", "smoothfactor": 2.0,
                "computepastep": 10.0, "psfphasecenter": "J2000 0deg 0deg" }),
    );
    assert_eq!(describe(&ignored), casa("tclean niter0"));

    let clean = run("clean", json!({ "niter": 40, "gain": 0.2 }));
    assert_eq!(describe(&clean), casa("tclean niter100"));
    assert_eq!(entry(&clean, "niter"), &CasaValue::Int { value: 40 });
    assert_eq!(entry(&clean, "cycleniter"), &CasaValue::Int { value: 40 });
    let CasaValue::IntArray { data: major, .. } = entry(&clean, "summarymajor") else {
        panic!("summarymajor is an int64 array");
    };
    assert_eq!(major.first(), Some(&0));
    let CasaValue::Int { value: iterations } = entry(&clean, "iterdone") else {
        panic!("iterdone is an int");
    };
    assert_eq!(major.last(), Some(iterations));
    let CasaValue::Int { value: majors } = entry(&clean, "nmajordone") else {
        panic!("nmajordone is an int");
    };
    assert_eq!(major.len(), usize::try_from(*majors).expect("count"));
    // CASA's cyclethreshold comes from the final major-cycle residual,
    // not the preceding minor cycle's controls; dirty remains integer zero.
    let CasaValue::Float { value: cycle } = entry(&clean, "cyclethreshold") else {
        panic!("cyclethreshold is a float");
    };
    assert!(cycle.0.is_finite() && cycle.0 > 0.0, "{cycle:?}");
    assert!(cycle.0 < last_minor_threshold(&clean));
    assert!(scalar(&clean, "maxpsfsidelobe").is_finite());
    assert_eq!(
        entry(&dirty, "cyclethreshold"),
        &CasaValue::Int { value: 0 }
    );

    let full = run("full", json!({ "niter": 40, "fullsummary": true }));
    assert_eq!(describe(&full), casa("tclean fullsummary"));

    // A cube has an entry for each channel, as CASA's has (Astra's #1015
    // F3); each holds nan where the report sums the planes.
    let cube = run(
        "cube",
        json!({ "niter": 30, "specmode": "cube", "nchan": 3 }),
    );
    assert_eq!(describe(&cube), casa("tclean cube"));

    // As CASA 6.7.6.14 returns them (probed on `make_cal_ms`'s MS):
    // - a dirty run reports nsigma 0 and threshold as it was given, a
    //   number as a float and text as its text;
    let given = run("given", json!({ "threshold": "1mJy", "nsigma": 2.0 }));
    assert_eq!(entry(&given, "nsigma"), &CasaValue::float(0.0));
    assert_eq!(entry(&given, "threshold"), &CasaValue::string("1mJy"));
    for (name, threshold, echoed) in [
        ("number", json!(0.001), CasaValue::float(0.001)),
        ("zero", json!("0Jy"), CasaValue::string("0Jy")),
    ] {
        let value = run(name, json!({ "threshold": threshold }));
        assert_eq!(entry(&value, "threshold"), &echoed, "{name}");
    }
    assert_eq!(entry(&dirty, "threshold"), &CasaValue::float(0.0));
    // - a run that iterates but cleans nothing, its threshold above the
    //   peak, keeps its controls, cycleniter at most niter, and has an
    //   empty summaryminor, {0: {}};
    let above = run(
        "above",
        json!({ "niter": 10, "cycleniter": 50, "threshold": "100Jy" }),
    );
    assert_eq!(entry(&above, "stopcode"), &CasaValue::Int { value: 2 });
    assert_eq!(entry(&above, "cycleniter"), &CasaValue::Int { value: 10 });
    assert_eq!(
        entry(&above, "loopgain"),
        &CasaValue::float(f64::from(0.1_f32))
    );
    assert_eq!(
        entry(&above, "summaryminor"),
        &CasaValue::Dict {
            entries: vec![casa_provider_contracts::CasaEntry {
                key: CasaKey::Int(0),
                value: CasaValue::Dict {
                    entries: Vec::new()
                },
            }]
        }
    );
    assert_eq!(entry(&above, "threshold"), &CasaValue::float(100.0));
    assert!(scalar(&above, "cyclethreshold") > 0.0);
    assert!(scalar(&above, "cyclethreshold") < 100.0);
    assert!(scalar(&above, "maxpsfsidelobe").is_finite());
}

#[test]
fn values_casa_rs_cannot_honour_are_refused_at_their_parameter() {
    let directory = tempfile::tempdir().expect("run directory");
    for (values, parameter, issue) in [
        (json!({ "deconvolver": "mem" }), "deconvolver", "#823"),
        // tclean reads interactive whether it iterates or not: it checks for
        // its viewer first (Astra's #1015 F8).
        (
            json!({ "interactive": true, "niter": 10 }),
            "interactive",
            "#980",
        ),
        (json!({ "interactive": true }), "interactive", "#980"),
    ] {
        let mut request = json!({ "vis": "input.ms", "imagename": "out", "cell": "1arcsec" });
        for (key, value) in values.as_object().expect("values") {
            request[key] = value.clone();
        }
        match json_run(directory.path(), request) {
            TaskReply::Failed {
                parameter: Some(name),
                message,
                ..
            } => {
                assert_eq!(name, parameter, "{message}");
                assert!(message.contains(issue), "{message}");
            }
            other => panic!("{parameter}: {other:?}"),
        }
    }
}

#[test]
fn restart_false_is_resolved_and_coordinated_outliers_remain_refused() {
    use casars_imager::{ImagingParameters, resolve_request, run};
    let directory = tempfile::tempdir().unwrap();
    let values = json!({
        "vis": "absent.ms", "imagename": directory.path().join("image"),
        "cell": "1arcsec", "restart": false,
    });
    let (parameters, request) =
        resolve_request(&ImagingParameters(values.as_object().unwrap().clone())).unwrap();
    assert_eq!(parameters.0["restart"], json!(false));
    let mut outlier = request;
    outlier.outlierfile = Some(directory.path().join("outliers.txt"));
    let error = run(parameters, &outlier).unwrap_err();
    assert!(
        error.contains("coordinated outlier") && error.contains("#944"),
        "{error}"
    );
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
