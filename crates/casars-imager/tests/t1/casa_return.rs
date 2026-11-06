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
use casa_types::{ScalarValue, Value as CellValue};
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
    json_run_with(directory, values, false)
}

/// [`json_run`], asking the binary for its run summary when `summary` is
/// set ([`casars_imager::RUN_SUMMARY_VARIABLE`]); otherwise it writes none,
/// whatever the environment the tests run in says.
pub(super) fn json_run_with(directory: &Path, values: Value, summary: bool) -> TaskReply {
    let mut command = Command::new(env!("CARGO_BIN_EXE_casars-imager"));
    if summary {
        command.env(casars_imager::RUN_SUMMARY_VARIABLE, "1");
    } else {
        command.env_remove(casars_imager::RUN_SUMMARY_VARIABLE);
    }
    let mut child = command
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
    // CASA's tclean writes its image products and nothing else beside them
    // (#980, owner decision of 2026-11-02).
    assert!(directory.join("dirty.psf").is_dir());
    assert!(!directory.join("dirty.summary.json").exists());
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
    // F3).
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
        (
            json!({ "deconvolver": "mem", "stokes": "IQUV" }),
            "deconvolver",
            "#1060",
        ),
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

#[test]
fn zero_major_limit_stops_before_minor_work_with_casa_stop_precedence() {
    use casa_images::PagedImage;
    use casars_imager::ImagerRunTaskResult;

    let observation = Observation::synthesise(&[("point", POINT)]);
    observation.image("nmajor-seed", json!({"niter": 0}));
    let seed_path = observation.scratch("nmajor-seed.model");
    let geometry = observation.geometry();
    let seed = {
        let mut image = PagedImage::<f32>::open(&seed_path).unwrap();
        let mut pixels = image.get().unwrap();
        pixels[[geometry.image_size[0] / 2, geometry.image_size[1] / 2, 0, 0]] = 0.25;
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.save().unwrap();
        pixels
    };
    let directory = observation.scratch("nmajor-zero");
    std::fs::create_dir_all(&directory).unwrap();
    let empty = directory.join("empty.crtf");
    std::fs::write(&empty, "#CRTF\n").unwrap();
    let base = json!({
        "vis": observation.measurement_set(), "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 10, "nmajor": 0, "threshold": "0Jy", "gain": 0.1,
    });
    for (name, extra, iterations, majors, stop) in [
        ("initial", json!({}), 0, 1, 9),
        ("threshold", json!({"threshold": "100Jy"}), 0, 1, 2),
        ("empty", json!({"mask": empty}), 0, 1, 7),
        ("seeded", json!({"startmodel": seed_path}), 0, 1, 9),
        ("dirty", json!({"niter": 0}), 0, 1, 3),
        // Fix the minor budget so this synthetic sky is a deterministic control.
        ("one-major", json!({"nmajor": 1, "cycleniter": 3}), 3, 2, 9),
    ] {
        let mut values = base.clone();
        values["imagename"] = json!(directory.join(name));
        values
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (value, result) = match json_run(&directory, values) {
            TaskReply::Succeeded { value, result, .. } => (value, result),
            other => panic!("{name}: expected initial products, got {other:?}"),
        };
        for (key, expected) in [
            ("iterdone", iterations),
            ("nmajordone", majors),
            ("stopcode", stop),
        ] {
            assert_eq!(
                entry(&value, key),
                &CasaValue::Int { value: expected },
                "{name}:{key}"
            );
        }
        let result: ImagerRunTaskResult = serde_json::from_value(result).unwrap();
        let model = PagedImage::<f32>::open(directory.join(format!("{name}.model")))
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(i64::try_from(result.run.major_cycles).unwrap(), majors);
        if iterations == 0 {
            assert!(result.run.minor_cycles.is_empty(), "{name}: no minor work");
            assert_eq!(result.run.actual_minor_iterations, 0);
            if name == "seeded" {
                assert_eq!(model, seed);
            } else {
                assert!(model.iter().all(|&pixel| pixel == 0.0));
            }
        } else {
            assert!(model.sum() > 0.0);
        }
    }
    assert_eq!(
        PagedImage::<f32>::open(seed_path).unwrap().get().unwrap(),
        seed
    );
}

/// A finished tclean leaves CASA's history and metadata in each image
/// (#1068): the task, the version and every parameter, again for each call on
/// the same name, and the field's name and telescope (CASA 6.7.6.14's
/// `write_tclean_history`, `SynthesisImager::createIMStore`).
#[test]
fn each_image_holds_tcleans_history_and_its_source() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("history");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let values = json!({
        "vis": observation.measurement_set(),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu",
        "imagename": directory.join("run"),
        "niter": 5,
        "spw": "0",
    });
    let run = || match json_run(&directory, values.clone()) {
        TaskReply::Succeeded { .. } => {}
        other => panic!("{other:?}"),
    };
    run();
    let path = |suffix: &str| directory.join(format!("run{suffix}"));
    let products = [".image", ".psf", ".residual", ".model", ".sumwt", ".pb"];
    for suffix in products {
        let image = casa_images::PagedImage::<f32>::open(path(suffix)).expect("open");
        let history = image.history().expect("history");
        assert_eq!(history[0], "taskname=tclean", "{suffix}");
        assert!(history[1].starts_with("version: casa-rs "), "{suffix}");
        assert!(history.contains(&"niter       = 5".to_owned()), "{suffix}");
        assert!(
            history.contains(&"spw         = \"0\"".to_owned()),
            "{suffix}"
        );
        let info = image.image_info().expect("image info");
        assert_eq!(info.object_name, "t1", "{suffix}");
        assert_eq!(
            info.image_type,
            casa_images::ImageType::Intensity,
            "{suffix}"
        );
        let misc = image.misc_info();
        for (name, expected) in [
            ("INSTRUME", ScalarValue::String("VLA".to_owned())),
            ("distance", ScalarValue::Float64(0.0)),
            ("mpiprocs", ScalarValue::Int32(1)),
            ("chnchnks", ScalarValue::Int32(1)),
        ] {
            let held = misc.fields().iter().find(|field| field.name == name);
            assert_eq!(
                held.map(|field| &field.value),
                Some(&CellValue::Scalar(expected)),
                "{suffix}: {name}"
            );
        }
    }
    // A second call replaces each image and keeps what its history held, in
    // order, each row where it was.
    let rows_of = |suffix: &str| {
        let table = casa_tables::Table::open(casa_tables::TableOptions::new(
            path(suffix).join("logtable"),
        ))
        .expect("logtable");
        (0..table.row_count())
            .map(|row| {
                let cell = |name: &str| {
                    table
                        .cell_accessor(row, name)
                        .and_then(|cell| cell.value().map(|value| value.cloned()))
                        .expect("a cell")
                };
                (cell("TIME"), cell("MESSAGE"), cell("LOCATION"))
            })
            .collect::<Vec<_>>()
    };
    let first = products.map(rows_of);
    for (suffix, rows) in products.iter().zip(&first) {
        assert_eq!(
            rows[0].2,
            Some(CellValue::Scalar(ScalarValue::String(
                "::tclean".to_owned()
            ))),
            "{suffix}"
        );
        // MJD seconds of this century, as casacore's log sink writes them.
        let Some(CellValue::Scalar(ScalarValue::Float64(time))) = rows[0].0 else {
            panic!("{suffix}: TIME");
        };
        assert!((4.5e9..6.0e9).contains(&time), "{suffix}: TIME {time}");
    }
    run();
    for (suffix, first) in products.iter().zip(first) {
        let rows = rows_of(suffix);
        assert_eq!(rows.len(), 2 * first.len(), "{suffix}");
        assert_eq!(&rows[..first.len()], &first[..], "{suffix}");
        assert_eq!(rows[first.len()].1, rows[0].1, "{suffix}");
        assert_ne!(rows[first.len()].0, rows[0].0, "{suffix}");
    }
}

/// `name`'s row of the plane at `[field, channel, stokes]`, as numbers.
fn plane_row(value: &CasaValue, key: [i64; 3], name: &str) -> Vec<f64> {
    let mut plane = entry(value, "summaryminor");
    for index in key {
        let CasaValue::Dict { entries } = plane else {
            panic!("a dictionary at {key:?}");
        };
        plane = &entries
            .iter()
            .find(|entry| entry.key == CasaKey::Int(index))
            .unwrap_or_else(|| panic!("no plane {key:?}"))
            .value;
    }
    let CasaValue::List { items } = entry(plane, name) else {
        panic!("{name} is a list");
    };
    items
        .iter()
        .map(|item| match item {
            CasaValue::Float { value } => value.0,
            CasaValue::Int { value } => *value as f64,
            other => panic!("{name}: {other:?}"),
        })
        .collect()
}

/// One channel of a four-axis image.
fn channel_pixels(path: &Path, channel: usize) -> Vec<f32> {
    let image = casa_images::PagedImage::<f32>::open(path).expect("open the image");
    let shape = image.shape().to_vec();
    image
        .get_slice(&[0, 0, 0, channel], &[shape[0], shape[1], shape[2], 1])
        .expect("read the channel")
        .iter()
        .copied()
        .collect()
}

/// `summaryminor` holds each plane's own rows (#1068): the iterations of the
/// planes run in sequence and sum to the run's, each plane's flux is that of
/// its own `.model`, a dirty cube's peaks are those of its own `.residual`
/// planes, and a start model's flux is each plane's starting point.
#[test]
fn a_cubes_summary_holds_each_planes_own_rows() {
    const CHANNELS: usize = 4;
    let steep = Component {
        spectral_index: -8.0,
        ..POINT
    };
    let observation = Observation::synthesise_band(CHANNELS, &[("point", steep)]);
    let directory = observation.scratch("plane-rows");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let run = |name: &str, controls: Value| {
        let mut values = json!({
            "vis": observation.measurement_set(),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu",
            "specmode": "cube",
            "fullsummary": true,
            "imagename": directory.join(name),
        });
        for (key, value) in controls.as_object().expect("controls") {
            values[key] = value.clone();
        }
        match json_run(&directory, values) {
            TaskReply::Succeeded { value, .. } => value,
            other => panic!("{name}: {other:?}"),
        }
    };
    let flux_of = |path: &Path, channel: usize| -> f64 {
        channel_pixels(path, channel)
            .iter()
            .map(|value| f64::from(*value))
            .sum()
    };

    let cleaned = run("cleaned", json!({"niter": 30, "gain": 0.2}));
    let CasaValue::Int { value: iterations } = entry(&cleaned, "iterdone") else {
        panic!("iterdone is an integer");
    };
    let mut done = 0.0;
    let mut last_fluxes = Vec::new();
    for channel in 0..CHANNELS {
        let key = [0, channel as i64, 0];
        let iterated = plane_row(&cleaned, key, "iterDone");
        // Before this plane's first cycle only the earlier planes have run.
        assert_eq!(plane_row(&cleaned, key, "startIterDone")[0], done);
        assert_eq!(plane_row(&cleaned, key, "cycleStartIters")[0], 0.0);
        done += iterated[0];
        let flux = *plane_row(&cleaned, key, "modelFlux")
            .last()
            .expect("a cycle");
        let image = flux_of(&directory.join("cleaned.model"), channel);
        assert!(
            (flux - image).abs() <= 1e-5 * image.abs().max(1e-3),
            "channel {channel}: {flux} against its model's {image}"
        );
        last_fluxes.push(flux);
        // With no mask, the peak over the plane's pixels is the peak inside
        // the mask.
        for (inside, over) in [("startPeakRes", "startPeakResNM"), ("peakRes", "peakResNM")] {
            let peaks = plane_row(&cleaned, key, over);
            assert!(peaks.iter().all(|peak| *peak > 0.0));
            for (a, b) in plane_row(&cleaned, key, inside).iter().zip(&peaks) {
                assert!((a - b).abs() <= 1e-3 * b, "{inside}: {a} against {b}");
            }
        }
    }
    // The steep spectrum makes the planes differ; none holds the others'.
    let spread = (last_fluxes[0] - last_fluxes[CHANNELS - 1]).abs();
    assert!(spread > 1e-3 * last_fluxes[0].abs(), "{last_fluxes:?}");
    let total: f64 = (0..CHANNELS)
        .flat_map(|channel| plane_row(&cleaned, [0, channel as i64, 0], "iterDone"))
        .sum();
    assert_eq!(total as i64, *iterations);

    let dirty = run("dirty", json!({"niter": 0}));
    for channel in 0..CHANNELS {
        let key = [0, channel as i64, 0];
        let peak = channel_pixels(&directory.join("dirty.residual"), channel)
            .into_iter()
            .fold(f32::NEG_INFINITY, f32::max);
        assert_eq!(plane_row(&dirty, key, "peakRes"), [f64::from(peak)]);
        assert_eq!(plane_row(&dirty, key, "peakResNM"), [f64::from(peak)]);
    }

    let seeded = run(
        "seeded",
        json!({"niter": 5, "gain": 0.2, "startmodel": directory.join("cleaned.model")}),
    );
    for (channel, flux) in last_fluxes.iter().enumerate() {
        let start = plane_row(&seeded, [0, channel as i64, 0], "startModelFlux")[0];
        assert!(
            (start - flux).abs() <= 1e-5 * flux.abs().max(1e-3),
            "channel {channel}: {start} against {flux}"
        );
    }
}

/// An outlier file's relative `imagename` reads from the working directory, as
/// `imagename` itself does, wherever the file is (CASA 6.7.6.14, #1125).
#[test]
fn an_outlier_files_relative_name_reads_from_the_working_directory() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("outlier-names");
    let settings = directory.join("settings");
    std::fs::create_dir_all(&settings).expect("settings directory");
    let outliers = settings.join("outliers.txt");
    std::fs::write(
        &outliers,
        "imagename=fields/outlier\nimsize=[32,32]\nphasecenter=J2000 19:59:28.5 +40.44.01.5\n",
    )
    .expect("the outlier file");
    let geometry = observation.geometry();
    let reply = json_run(
        &directory,
        json!({
            "vis": observation.measurement_set(),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu",
            "imagename": "products/main",
            "outlierfile": outliers,
        }),
    );
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    assert!(directory.join("products/main.psf").is_dir());
    assert!(directory.join("fields/outlier.psf").is_dir());
    assert!(!settings.join("fields").exists());
    assert!(!directory.join("products/fields").exists());
}

/// CASA `clarkstokes` sends Stokes I and V to separate minor cycles,
/// while `clark` sends them together (#1126).
#[test]
fn clarkstokes_cleans_polarization_planes_separately() {
    use super::fixture::Setup;
    use casa_ms::{SyntheticPolarizationBasis, SyntheticPolarizationSetup};

    let observation = Observation::synthesise_polarized(
        Setup::vla_q_band(1),
        SyntheticPolarizationSetup {
            basis: SyntheticPolarizationBasis::Linear,
            correlation_count: 4,
            source_stokes_fractional_quv: [0.0, 0.0, 0.4],
        },
        &[("point", POINT)],
    );
    let run = |name: &str, deconvolver: &str| {
        observation
            .image(
                name,
                json!({
                    "stokes": "IV", "deconvolver": deconvolver, "niter": 1,
                    "cycleniter": 1, "gain": 0.2, "weighting": "natural",
                }),
            )
            .0
    };
    let joint = run("joint-clark", "clark");
    let separate = run("separate-clark", "clarkstokes");
    assert_eq!(joint.actual_minor_iterations, 1);
    assert_eq!(separate.actual_minor_iterations, 2);
}

/// A requested main-lobe cutoff reaches the minor cycle's PSF sidelobe,
/// not just the beam written in the final image metadata (#1140).
#[test]
fn tclean_uses_psfcutoff_in_its_minor_cycle() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("psfcutoff");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let run = |name: &str, cutoff: f64| {
        let values = json!({
            "vis": observation.measurement_set(),
            "imagename": directory.join(name),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu",
            "deconvolver": "clark",
            "niter": 1,
            "psfcutoff": cutoff,
            "restoration": false,
        });
        match json_run(&directory, values) {
            TaskReply::Succeeded { value, .. } => scalar(&value, "maxpsfsidelobe"),
            other => panic!("{name}: {other:?}"),
        }
    };
    let (low, high) = (run("low", 0.15), run("high", 0.65));
    assert!((low - high).abs() > 1e-5, "low {low}, high {high}");
}

/// A `calcpsf=False` restart keeps the PSF beam fitted by the first run,
/// including in its minor-cycle sidelobe and republished PSF metadata.
#[test]
fn tclean_restart_reuses_the_saved_psf_beam() {
    use casa_images::PagedImage;
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("restart-psfcutoff");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let run = |name: &str, cutoff: f64, niter: usize, restart: bool| {
        let values = json!({
            "vis": observation.measurement_set(),
            "imagename": directory.join(name),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu",
            "deconvolver": "clark",
            "niter": niter,
            "psfcutoff": cutoff,
            "calcpsf": !restart,
            "calcres": !restart,
            "restoration": false,
        });
        match json_run(&directory, values) {
            TaskReply::Succeeded { value, .. } => value,
            other => panic!("{name}: {other:?}"),
        }
    };
    let beam = |name: &str| {
        PagedImage::<f32>::open(directory.join(format!("{name}.psf")))
            .unwrap()
            .image_info()
            .unwrap()
            .beam_set
            .single_beam()
            .expect("one PSF beam")
    };
    for (name, original, changed) in [("low", 0.15, 0.65), ("high", 0.65, 0.15)] {
        let reference = run(&format!("{name}-fresh"), original, 20, false);
        run(name, original, 0, false);
        let before = beam(name);
        let continued = run(name, changed, 20, true);
        let after = beam(name);
        assert!(
            (after.major / before.major - 1.0).abs() < 1e-7,
            "{name}: {before:?} -> {after:?}"
        );
        assert!(
            (after.minor / before.minor - 1.0).abs() < 1e-7,
            "{name}: {before:?} -> {after:?}"
        );
        assert!(
            (after.position_angle - before.position_angle).abs() < 1e-7,
            "{name}: {before:?} -> {after:?}"
        );
        let (expected, actual) = (
            scalar(&reference, "maxpsfsidelobe"),
            scalar(&continued, "maxpsfsidelobe"),
        );
        assert!(
            (expected - actual).abs() < 1e-5,
            "{name}: fresh {expected}, restart {actual}"
        );
    }
}

/// CASA 6.7.6.14 reports 0.4375 for this saved beam on 1:2 cells: the
/// Gaussian is four pixels across x and two across y.
#[test]
fn tclean_saved_psf_sidelobe_uses_both_cell_sizes() {
    use casa_images::{GaussianBeam, ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("rectangular-saved-psf");
    std::fs::create_dir_all(&directory).unwrap();
    let cell = 0.012_f64.to_radians() / 3600.0;
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": 64, "cell": ["0.012arcsec", "0.024arcsec"], "backend": "cpu",
        "specmode": "cube", "nchan": 1, "start": 0, "width": 1,
        "deconvolver": "hogbom", "niter": 0, "restoration": false,
    });
    let seed = json_run(&directory, values.clone());
    assert!(matches!(seed, TaskReply::Succeeded { .. }), "{seed:?}");
    let mut psf = PagedImage::<f32>::open(directory.join("image.psf")).unwrap();
    let mut info = psf.image_info().unwrap();
    info.beam_set = ImageBeamSet::new(GaussianBeam {
        major: 4.0 * cell,
        minor: 4.0 * cell,
        position_angle: 0.0,
    });
    psf.set_image_info(&info).unwrap();
    let mut data = psf.get().unwrap();
    for x in 0..64 {
        for y in 0..64 {
            let d2 = (x as f64 - 32.0).powi(2) + (y as f64 - 32.0).powi(2);
            data[[x, y, 0, 0]] = (-4.0 * std::f64::consts::LN_2 * d2 / 16.0).exp() as f32;
        }
    }
    psf.put_slice(&data, &[0; 4]).unwrap();
    psf.save().unwrap();
    drop(psf);
    let mut residual = PagedImage::<f32>::open(directory.join("image.residual")).unwrap();
    let mut data = residual.get().unwrap();
    data.fill(0.0);
    data[[32, 32, 0, 0]] = 1.0;
    residual.put_slice(&data, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);
    let mut sumwt = PagedImage::<f32>::open(directory.join("image.sumwt")).unwrap();
    let mut weights = sumwt.get().unwrap();
    weights.fill(1.0);
    sumwt.put_slice(&weights, &[0; 4]).unwrap();
    sumwt.save().unwrap();
    drop(sumwt);
    values["niter"] = json!(1);
    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    let result = match json_run(&directory, values) {
        TaskReply::Succeeded { value, .. } => value,
        other => panic!("{other:?}"),
    };
    let sidelobe = scalar(&result, "maxpsfsidelobe");
    assert!((sidelobe - 0.4375).abs() < 1e-6, "{sidelobe}");
}

/// CASA subtracts a saved off-centre PSF beam at the image centre.
#[test]
fn tclean_saved_off_centre_psf_subtracts_the_beam_at_image_centre() {
    use casa_images::{GaussianBeam, ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("offcentre-saved-psf");
    std::fs::create_dir_all(&directory).unwrap();
    let cell = 0.012_f64.to_radians() / 3600.0;
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": 64, "cell": ["0.012arcsec", "0.012arcsec"], "backend": "cpu",
        "specmode": "cube", "nchan": 1, "start": 0, "width": 1,
        "deconvolver": "hogbom", "niter": 0, "restoration": false,
    });
    let seed = json_run(&directory, values.clone());
    assert!(matches!(seed, TaskReply::Succeeded { .. }), "{seed:?}");
    let mut psf = PagedImage::<f32>::open(directory.join("image.psf")).unwrap();
    let mut info = psf.image_info().unwrap();
    info.beam_set = ImageBeamSet::new(GaussianBeam {
        major: 40.0 * cell,
        minor: 40.0 * cell,
        position_angle: 0.0,
    });
    psf.set_image_info(&info).unwrap();
    let mut data = psf.get().unwrap();
    data.fill(0.0);
    data[[15, 32, 0, 0]] = 1.0;
    psf.put_slice(&data, &[0; 4]).unwrap();
    psf.save().unwrap();
    drop(psf);
    let mut residual = PagedImage::<f32>::open(directory.join("image.residual")).unwrap();
    let mut data = residual.get().unwrap();
    data.fill(0.0);
    data[[32, 32, 0, 0]] = 1.0;
    residual.put_slice(&data, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);
    let mut sumwt = PagedImage::<f32>::open(directory.join("image.sumwt")).unwrap();
    let mut weights = sumwt.get().unwrap();
    weights.fill(1.0);
    sumwt.put_slice(&weights, &[0; 4]).unwrap();
    sumwt.save().unwrap();
    drop(sumwt);
    values["niter"] = json!(1);
    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    let result = match json_run(&directory, values) {
        TaskReply::Succeeded { value, .. } => value,
        other => panic!("{other:?}"),
    };
    let sidelobe = scalar(&result, "maxpsfsidelobe");
    assert!((sidelobe - 0.39395368).abs() < 1e-6, "{sidelobe}");
}

/// A fresh two-channel beam set gives the off-centre channel the largest
/// beam before CASA measures the PSF sidelobe (CASA 6.7.6.14: 0.39395368).
#[test]
fn tclean_fresh_off_centre_psf_uses_its_domain_beam_for_sidelobes() {
    use casa_images::{ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("offcentre-fresh-psf");
    std::fs::create_dir_all(&directory).unwrap();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": 64, "cell": ["0.012arcsec", "0.012arcsec"], "backend": "cpu",
        "specmode": "cube", "nchan": 2, "start": 0, "width": 1,
        "deconvolver": "hogbom", "niter": 0, "restoration": false,
    });
    let seed = json_run(&directory, values.clone());
    assert!(matches!(seed, TaskReply::Succeeded { .. }), "{seed:?}");
    let mut psf = PagedImage::<f32>::open(directory.join("image.psf")).unwrap();
    let mut info = psf.image_info().unwrap();
    info.beam_set = ImageBeamSet::empty();
    psf.set_image_info(&info).unwrap();
    let mut data = psf.get().unwrap();
    data.fill(0.0);
    for x in 0..64 {
        for y in 0..64 {
            let dx = x as f64 - 32.0;
            let dy = y as f64 - 32.0;
            data[[x, y, 0, 0]] =
                (-4.0 * std::f64::consts::LN_2 * (dx * dx + dy * dy) / 1600.0).exp() as f32;
        }
    }
    data[[15, 32, 0, 1]] = 1.0;
    psf.put_slice(&data, &[0; 4]).unwrap();
    psf.save().unwrap();
    drop(psf);
    let mut residual = PagedImage::<f32>::open(directory.join("image.residual")).unwrap();
    let mut data = residual.get().unwrap();
    data.fill(0.0);
    data[[32, 32, 0, 0]] = 1.0;
    data[[32, 32, 0, 1]] = 1.0;
    residual.put_slice(&data, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);
    let mut sumwt = PagedImage::<f32>::open(directory.join("image.sumwt")).unwrap();
    let mut weights = sumwt.get().unwrap();
    weights.fill(1.0);
    sumwt.put_slice(&weights, &[0; 4]).unwrap();
    sumwt.save().unwrap();
    drop(sumwt);
    values["niter"] = json!(1);
    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    let result = match json_run(&directory, values) {
        TaskReply::Succeeded { value, .. } => value,
        other => panic!("{other:?}"),
    };
    let sidelobe = scalar(&result, "maxpsfsidelobe");
    assert!((sidelobe - 0.39395368).abs() < 2e-5, "{sidelobe}");
}

/// A fresh cube whose PSF peaks off-centre in every channel uses CASA's
/// tiny beam in automatic masking (#1140). The adjacent subthreshold pixel
/// must not be pulled into the mask by a one-pixel fallback beam.
#[test]
fn tclean_all_refused_psfs_use_the_tiny_beam_in_automatic_masks() {
    use casa_images::{ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("all-refused-psf-automask");
    std::fs::create_dir_all(&directory).unwrap();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": 64, "cell": ["0.012arcsec", "0.012arcsec"], "backend": "cpu",
        "specmode": "cube", "nchan": 2, "start": 0, "width": 1,
        "deconvolver": "clark", "niter": 0, "restoration": false,
    });
    let seed = json_run(&directory, values.clone());
    assert!(matches!(seed, TaskReply::Succeeded { .. }), "{seed:?}");
    for product in ["psf", "residual"] {
        let mut image =
            PagedImage::<f32>::open(directory.join(format!("image.{product}"))).unwrap();
        let mut info = image.image_info().unwrap();
        info.beam_set = ImageBeamSet::empty();
        image.set_image_info(&info).unwrap();
        let mut data = image.get().unwrap();
        data.fill(0.0);
        for channel in 0..2 {
            if product == "psf" {
                data[[15, 32, 0, channel]] = 1.0;
            } else {
                for x in 0..64 {
                    for y in 0..64 {
                        data[[x, y, 0, channel]] =
                            (((x * 73 + y * 29 + channel * 11) % 101) as f32 - 50.0) / 10_000.0;
                    }
                }
                data[[32, 32, 0, channel]] = 1.0;
                data[[31, 32, 0, channel]] = 0.3;
            }
        }
        image.put_slice(&data, &[0; 4]).unwrap();
        image.save().unwrap();
    }
    let mut sumwt = PagedImage::<f32>::open(directory.join("image.sumwt")).unwrap();
    let mut weights = sumwt.get().unwrap();
    weights.fill(1.0);
    sumwt.put_slice(&weights, &[0; 4]).unwrap();
    sumwt.save().unwrap();
    drop(sumwt);
    values["niter"] = json!(2);
    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    values["usemask"] = json!("auto-multithresh");
    values["sidelobethreshold"] = json!(0.8);
    values["noisethreshold"] = json!(3.0);
    values["lownoisethreshold"] = json!(1.5);
    values["minbeamfrac"] = json!(0.3);
    values["negativethreshold"] = json!(0.0);
    let result = json_run(&directory, values);
    assert!(matches!(result, TaskReply::Succeeded { .. }), "{result:?}");
    let mask = PagedImage::<f32>::open(directory.join("image.mask"))
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(
        mask.iter().filter(|&&value| value != 0.0).count(),
        2,
        "{result:?}"
    );
    assert_eq!(mask[[32, 32, 0, 0]], 1.0);
    assert_eq!(mask[[32, 32, 0, 1]], 1.0);
}

/// MFS retains an existing PSF beam through recomputation; cube gridding
/// forms a temporary image without that metadata and fits a fresh beam.
#[test]
fn tclean_recomputed_psf_uses_the_mode_specific_beam() {
    use casa_images::{GaussianBeam, ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("recompute-saved-psf");
    std::fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let cell = geometry.cell_rad()[0];
    for (algorithm, niter, cube) in [
        ("clark", 0, false),
        ("clark", 10, false),
        ("mtmfs", 0, false),
        ("mtmfs", 10, false),
        ("clark", 0, true),
        ("clark", 10, true),
    ] {
        let prefix = format!("{algorithm}-{niter}-{cube}");
        let mut values = json!({
            "vis": observation.measurement_set(), "imagename": directory.join(&prefix),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu", "deconvolver": algorithm, "nterms": 2,
            "niter": 0, "psfcutoff": 0.15, "restoration": false,
        });
        if cube {
            values["specmode"] = json!("cube");
            values["nchan"] = json!(1);
            values["start"] = json!(0);
            values["width"] = json!(1);
        }
        let reference_path = directory.join(format!("{prefix}-fresh.psf"));
        if cube {
            let mut fresh = values.clone();
            fresh["imagename"] = json!(directory.join(format!("{prefix}-fresh")));
            fresh["psfcutoff"] = json!(0.65);
            let result = json_run(&directory, fresh);
            assert!(matches!(result, TaskReply::Succeeded { .. }), "{result:?}");
        }
        let seed = json_run(&directory, values.clone());
        assert!(matches!(seed, TaskReply::Succeeded { .. }), "{seed:?}");
        let suffix = if algorithm == "mtmfs" {
            "psf.tt0"
        } else {
            "psf"
        };
        let path = directory.join(format!("{prefix}.{suffix}"));
        let mut psf = PagedImage::<f32>::open(&path).unwrap();
        let mut info = psf.image_info().unwrap();
        info.beam_set = ImageBeamSet::new(GaussianBeam {
            major: 10.0 * cell,
            minor: 8.0 * cell,
            position_angle: 0.25,
        });
        psf.set_image_info(&info).unwrap();
        psf.save().unwrap();
        drop(psf);
        values["psfcutoff"] = json!(0.65);
        values["niter"] = json!(niter);
        let result = json_run(&directory, values);
        assert!(
            matches!(result, TaskReply::Succeeded { .. }),
            "{prefix}: {result:?}"
        );
        let saved = PagedImage::<f32>::open(path)
            .unwrap()
            .image_info()
            .unwrap()
            .beam_set
            .single_beam()
            .unwrap();
        if cube {
            let fresh = PagedImage::<f32>::open(reference_path)
                .unwrap()
                .image_info()
                .unwrap()
                .beam_set
                .single_beam()
                .unwrap();
            assert!(
                (saved.major / fresh.major - 1.0).abs() < 1e-6,
                "{prefix}: {fresh:?} {saved:?}"
            );
            assert!(
                (saved.minor / fresh.minor - 1.0).abs() < 1e-6,
                "{prefix}: {fresh:?} {saved:?}"
            );
            assert!(
                (saved.position_angle - fresh.position_angle).abs() < 1e-6,
                "{prefix}: {fresh:?} {saved:?}"
            );
        } else {
            assert!(
                (saved.major / cell - 10.0).abs() < 1e-5,
                "{prefix}: {saved:?}"
            );
            assert!(
                (saved.minor / cell - 8.0).abs() < 1e-5,
                "{prefix}: {saved:?}"
            );
            assert!(
                (saved.position_angle - 0.25).abs() < 1e-6,
                "{prefix}: {saved:?}"
            );
        }
    }
}

/// CASA fits a headerless saved PSF at its default cutoff before publishing
/// its beam, even if the continuing request asks for a different cutoff.
#[test]
fn tclean_restart_without_a_saved_beam_publishes_the_default_fit() {
    use casa_images::{ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("restart-headerless-psf");
    std::fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    for (name, algorithm) in [("clark", "clark"), ("mtmfs", "mtmfs")] {
        let run = |prefix: &str, cutoff: f64, niter: usize, restart: bool| {
            let values = json!({
                "vis": observation.measurement_set(), "imagename": directory.join(prefix),
                "imsize": geometry.image_size,
                "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
                "backend": "cpu", "deconvolver": algorithm, "nterms": 2,
                "niter": niter, "psfcutoff": cutoff,
                "calcpsf": !restart, "calcres": !restart, "restoration": false,
            });
            let reply = json_run(&directory, values);
            assert!(
                matches!(reply, TaskReply::Succeeded { .. }),
                "{prefix}: {reply:?}"
            );
        };
        let suffix = if algorithm == "mtmfs" {
            "psf.tt0"
        } else {
            "psf"
        };
        let beam = |prefix: &str| {
            PagedImage::<f32>::open(directory.join(format!("{prefix}.{suffix}")))
                .unwrap()
                .image_info()
                .unwrap()
                .beam_set
                .single_beam()
                .unwrap()
        };
        run(&format!("{name}-reference"), 0.35, 20, false);
        run(name, 0.15, 0, false);
        let path = directory.join(format!("{name}.{suffix}"));
        let mut psf = PagedImage::<f32>::open(&path).unwrap();
        let mut info = psf.image_info().unwrap();
        info.beam_set = ImageBeamSet::empty();
        psf.set_image_info(&info).unwrap();
        psf.save().unwrap();
        drop(psf);
        run(name, 0.65, 20, true);
        let (expected, actual) = (beam(&format!("{name}-reference")), beam(name));
        assert!(
            (actual.major / expected.major - 1.0).abs() < 1e-6,
            "{name}: {expected:?} {actual:?}"
        );
        assert!(
            (actual.minor / expected.minor - 1.0).abs() < 1e-6,
            "{name}: {expected:?} {actual:?}"
        );
        assert!(
            (actual.position_angle - expected.position_angle).abs() < 1e-6,
            "{name}: {expected:?} {actual:?}"
        );
    }
}

/// On restart CASA automask uses the residual's beam, or the PSF beam when
/// the residual has none, including when image pixels are recomputed.
/// The two headers deliberately disagree.
#[test]
fn tclean_automatic_restart_prefers_the_residual_beam() {
    use casa_images::{GaussianBeam, ImageBeamSet, PagedImage};
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("restart-mask-beam");
    std::fs::create_dir_all(&directory).unwrap();
    let cell = 0.012_f64.to_radians() / 3600.0;
    for (name, psf_size, residual_size, recompute, recompute_psf, rectangular, expected_mask) in [
        (
            "residual-broad",
            2.5,
            Some(10.0),
            false,
            false,
            false,
            false,
        ),
        (
            "residual-narrow",
            10.0,
            Some(2.5),
            false,
            false,
            false,
            true,
        ),
        ("residual-absent", 10.0, None, false, false, false, false),
        (
            "recomputed-broad",
            2.5,
            Some(50.0),
            true,
            false,
            false,
            false,
        ),
        (
            "recomputed-narrow",
            2.5,
            Some(2.5),
            true,
            false,
            false,
            true,
        ),
        (
            "both-recomputed-broad",
            2.5,
            Some(50.0),
            true,
            true,
            false,
            false,
        ),
        (
            "both-recomputed-narrow",
            2.5,
            Some(2.5),
            true,
            true,
            false,
            true,
        ),
        ("rectangular", 2.5, Some(9.0), false, false, true, true),
    ] {
        let mut values = json!({
            "vis": observation.measurement_set(), "imagename": directory.join(name),
            "imsize": 64, "cell": ["0.012arcsec", if rectangular { "0.024arcsec" } else { "0.012arcsec" }], "backend": "cpu",
            "specmode": "cube", "nchan": 1, "start": 0, "width": 1,
            "deconvolver": "hogbom", "niter": 0, "restoration": false,
        });
        let seed = json_run(&directory, values.clone());
        assert!(matches!(seed, TaskReply::Succeeded { .. }), "{seed:?}");
        for (product, size) in [("psf", Some(psf_size)), ("residual", residual_size)] {
            let mut image =
                PagedImage::<f32>::open(directory.join(format!("{name}.{product}"))).unwrap();
            let mut info = image.image_info().unwrap();
            info.beam_set = size.map_or_else(ImageBeamSet::empty, |width| {
                ImageBeamSet::new(GaussianBeam {
                    major: width * cell,
                    minor: width * cell,
                    position_angle: 0.0,
                })
            });
            image.set_image_info(&info).unwrap();
            let mut data = image.get().unwrap();
            for x in 0..64 {
                for y in 0..64 {
                    let d2 = ((x as f64 - 32.0).powi(2) + (y as f64 - 32.0).powi(2)) / 8.0;
                    data[[x, y, 0, 0]] = if product == "psf" {
                        (-d2).exp() as f32
                    } else if (30..35).contains(&x) && (30..35).contains(&y) {
                        10.0
                    } else {
                        0.0
                    };
                }
            }
            image.put_slice(&data, &[0; 4]).unwrap();
            image.save().unwrap();
        }
        let mut sumwt = PagedImage::<f32>::open(directory.join(format!("{name}.sumwt"))).unwrap();
        let mut weights = sumwt.get().unwrap();
        weights.fill(1.0);
        sumwt.put_slice(&weights, &[0; 4]).unwrap();
        sumwt.save().unwrap();
        drop(sumwt);
        values["niter"] = json!(1);
        values["calcpsf"] = json!(recompute_psf);
        values["calcres"] = json!(recompute);
        values["usemask"] = json!("auto-multithresh");
        values["sidelobethreshold"] = json!(if recompute || rectangular { 0.01 } else { 2.0 });
        values["noisethreshold"] = json!(4.25);
        values["lownoisethreshold"] = json!(1.5);
        values["minbeamfrac"] = json!(0.5);
        values["smoothfactor"] = json!(1.0);
        values["cutthreshold"] = json!(0.01);
        values["growiterations"] = json!(0);
        let result = json_run(&directory, values);
        assert!(
            matches!(result, TaskReply::Succeeded { .. }),
            "{name}: {result:?}"
        );
        let mask = PagedImage::<f32>::open(directory.join(format!("{name}.mask")))
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(
            mask.iter().any(|value| *value != 0.0),
            expected_mask,
            "{name}"
        );
    }
}

/// A Taylor-family restart also keeps the fitted beam of its saved tt0 PSF.
#[test]
fn mtmfs_restart_reuses_the_saved_psf_beam() {
    use casa_images::PagedImage;
    let observation = Observation::synthesise_band(16, &[("point", POINT)]);
    let directory = observation.scratch("restart-mtmfs-psfcutoff");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let run = |cutoff: f64, niter: usize, restart: bool| {
        let values = json!({
            "vis": observation.measurement_set(),
            "imagename": directory.join("image"),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": niter,
            "psfcutoff": cutoff,
            "calcpsf": !restart,
            "calcres": !restart,
            "restoration": false,
        });
        match json_run(&directory, values) {
            TaskReply::Succeeded { .. } => (),
            other => panic!("MT-MFS restart: {other:?}"),
        }
    };
    let beam = || {
        PagedImage::<f32>::open(directory.join("image.psf.tt0"))
            .unwrap()
            .image_info()
            .unwrap()
            .beam_set
            .single_beam()
            .expect("one PSF beam")
    };
    run(0.65, 0, false);
    let before = beam();
    run(0.15, 3, true);
    let after = beam();
    assert!(
        (after.major / before.major - 1.0).abs() < 1e-7,
        "{before:?} -> {after:?}"
    );
    assert!(
        (after.minor / before.minor - 1.0).abs() < 1e-7,
        "{before:?} -> {after:?}"
    );
    assert!(
        (after.position_angle - before.position_angle).abs() < 1e-7,
        "{before:?} -> {after:?}"
    );
}

#[test]
fn mem_writes_its_last_entropy_multipliers_only_on_a_solved_model() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("mem-metadata");
    std::fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    for (name, niter) in [("dirty", 0), ("clean", 2)] {
        let values = json!({
            "vis": observation.measurement_set(), "imagename": directory.join(name),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend":"cpu", "niter":niter, "deconvolver":"mem",
            "cyclefactor":0.0001, "minpsffraction":0.0
        });
        let reply = json_run(&directory, values);
        assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
        for suffix in [".model", ".psf", ".residual"] {
            let image =
                casa_images::PagedImage::<f32>::open(directory.join(format!("{name}{suffix}")))
                    .unwrap();
            let misc = image.misc_info();
            if niter > 0 && suffix == ".model" {
                let Some(CellValue::Scalar(ScalarValue::Float32(alpha))) = misc.get("ALPHA") else {
                    panic!("MEM model has a float ALPHA")
                };
                assert!(
                    alpha.is_finite() && *alpha > 0.0,
                    "a solved MEM model retains its positive entropy multiplier: {alpha}"
                );
                assert_eq!(
                    misc.get("BETA"),
                    Some(&CellValue::Scalar(ScalarValue::Float32(0.0)))
                );
            } else {
                assert!(misc.get("ALPHA").is_none(), "{name}{suffix}");
                assert!(misc.get("BETA").is_none(), "{name}{suffix}");
            }
        }
    }
}
