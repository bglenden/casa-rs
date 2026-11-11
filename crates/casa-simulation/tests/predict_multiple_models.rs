// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multi-image prediction against independent CASA 6.7.6.14 DATA (#1053).
//! The oracle preserves mosaic DATA before CASA's known cleanup abort.

mod predict_support;

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_imaging_runtime::reserved_memory;
use casa_simulation::{PredictSpec, PredictionOptions, Simulator, VoltagePatternSpec};
use flate2::read::GzDecoder;
use predict_support::{
    CENTRE_RAD, N, Row, read_main, resources, scan, session, write_model, write_model_of,
    write_point_list,
};
use serde_json::Value;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_multiple_models_casa.json.gz")[..])
        .read_to_string(&mut text)
        .expect("decompress CASA fixture");
    let fixture: Value = serde_json::from_str(&text).expect("CASA fixture");
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    fixture
}

fn models(directory: &Path) -> BTreeMap<&'static str, PathBuf> {
    let a = write_model(
        directory,
        "a.image",
        &[vec![(N / 2, N / 2, 1.25), (N / 2 + 9, N / 2 - 4, 0.75)]],
    );
    let planes = (0..4)
        .map(|channel| vec![(48, 48, 0.5 + 0.25 * channel as f32), (60, 49, -0.1)])
        .collect::<Vec<_>>();
    let b = write_model_of(directory, "b.image", 96, &planes);
    let zero = write_model(directory, "zero.image", &[vec![]]);
    BTreeMap::from([("a", a), ("b", b), ("zero", zero)])
}

fn observed(path: &Path, workers: usize, budget: u64) -> Simulator {
    let mut sm = session(path, "ALMA", "XX YY", resources(workers, budget));
    sm.observe(&scan("target", 0.0, 20.0)).expect("observe");
    sm.observe(&scan("offset", 20.0, 40.0)).expect("observe");
    sm
}

fn run_case(case: &Value, workers: usize) -> Vec<Row> {
    let directory = tempfile::tempdir().expect("temporary directory");
    let models = models(directory.path());
    let path = directory.path().join("multi.ms");
    let mut sm = observed(&path, workers, 2 << 30);
    let machine = case["machine"].as_str().unwrap_or("ft");
    sm.set_options(&PredictionOptions {
        ftmachine: machine.to_string(),
        grid_function: if machine == "sd" { "BOX" } else { "SF" }.to_string(),
        w_planes: 16,
        ..PredictionOptions::default()
    })
    .expect("setoptions");
    if case["beam"].as_bool().unwrap_or(false) {
        sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    }
    if case["initial"].as_bool().unwrap_or(false) {
        sm.predict(&PredictSpec {
            images: vec![models["a"].clone()],
            ..PredictSpec::default()
        })
        .expect("initial predict");
    }
    let names: Vec<&str> = case["images"].as_array().map_or_else(
        || vec!["a", "b"],
        |names| {
            names
                .iter()
                .map(|name| name.as_str().expect("image name"))
                .collect()
        },
    );
    let component_lists = if case["components"].as_bool().unwrap_or(false) {
        let path = directory.path().join("point.cl");
        write_point_list(&path, CENTRE_RAD, 0.25);
        vec![path]
    } else {
        Vec::new()
    };
    sm.predict(&PredictSpec {
        images: names.iter().map(|name| models[name].clone()).collect(),
        component_lists,
        incremental: case["incremental"].as_bool().unwrap_or(false),
    })
    .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
    sm.close().expect("close, including after mosaic predict");
    read_main(&path).0
}

fn assert_casa(case: &Value, actual: &[Row]) {
    let expected = case["rows"].as_array().expect("CASA rows");
    let name = case["name"].as_str().expect("case name");
    assert_eq!(actual.len(), expected.len(), "{name}: row count");
    let (mut error, mut power) = (0.0, 0.0);
    for (row, expected) in actual.iter().zip(expected) {
        assert_eq!(
            row.field as u64,
            expected["field"].as_u64().expect("FIELD_ID")
        );
        for (antenna, expected) in row
            .antennas
            .iter()
            .zip(expected["antennas"].as_array().expect("antennas"))
        {
            assert_eq!(u64::from(*antenna), expected.as_u64().expect("antenna id"));
        }
        assert_eq!(
            row.flag_row,
            expected["flag_row"].as_bool().expect("FLAG_ROW")
        );
        let samples = expected["data"].as_array().expect("DATA");
        assert_eq!(row.data.len(), samples.len(), "{name}: cell shape");
        for (value, sample) in row.data.iter().zip(samples) {
            let re = sample[0].as_f64().expect("real");
            let im = sample[1].as_f64().expect("imaginary");
            assert!(
                value.re.is_finite() && value.im.is_finite() && re.is_finite() && im.is_finite()
            );
            if !row.flag_row {
                error += (f64::from(value.re) - re).powi(2) + (f64::from(value.im) - im).powi(2);
                power += re.powi(2) + im.powi(2);
            }
        }
    }
    assert!(power > 0.0, "{name}: nonzero reference power");
    let nrms = (error / power).sqrt();
    println!("{name}: DATA NRMS {nrms:.9e}");
    assert!(nrms <= 1e-3, "{name}: DATA NRMS {nrms} exceeds 1e-3");
}

/// Independent CASA references for the two documented corrections. The
/// raw multi-image CASA DATA remains in the fixture so its defect is also
/// checked: mosaic must add the independently predicted images, and SDGrid
/// must leave cross-correlations zero.
fn corrected_reference(case: &Value, cases: &[Value]) -> Value {
    let mut reference = case.clone();
    if matches!(
        case["name"].as_str(),
        Some("mosaic_models" | "mosaic_reverse")
    ) {
        let a = cases
            .iter()
            .find(|c| c["name"] == "mosaic_a")
            .expect("CASA a");
        let b = cases
            .iter()
            .find(|c| c["name"] == "mosaic_b")
            .expect("CASA b");
        for (index, row) in reference["rows"]
            .as_array_mut()
            .expect("rows")
            .iter_mut()
            .enumerate()
        {
            for (sample, value) in row["data"]
                .as_array_mut()
                .expect("DATA")
                .iter_mut()
                .enumerate()
            {
                for part in 0..2 {
                    let x = a["rows"][index]["data"][sample][part]
                        .as_f64()
                        .expect("CASA sample") as f32;
                    let y = b["rows"][index]["data"][sample][part]
                        .as_f64()
                        .expect("CASA sample") as f32;
                    value[part] = Value::from(f64::from(x + y));
                }
            }
        }
    }
    if case["machine"] == "sd" {
        for row in reference["rows"].as_array_mut().expect("rows") {
            if row["antennas"][0] != row["antennas"][1] {
                for sample in row["data"].as_array_mut().expect("DATA") {
                    sample[0] = Value::from(0.0);
                    sample[1] = Value::from(0.0);
                }
            }
        }
    }
    reference
}

fn reference_nrms(a: &Value, b: &Value) -> f64 {
    let mut error = 0.0;
    let mut power = 0.0;
    for (a, b) in a["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .zip(b["rows"].as_array().expect("rows"))
    {
        for (x, y) in a["data"]
            .as_array()
            .expect("DATA")
            .iter()
            .zip(b["data"].as_array().expect("DATA"))
        {
            for part in 0..2 {
                let x = x[part].as_f64().expect("real or imaginary");
                let y = y[part].as_f64().expect("real or imaginary");
                error += (x - y).powi(2);
                power += y * y;
            }
        }
    }
    assert!(power > 0.0);
    (error / power).sqrt()
}

fn the_raw_fixture_demonstrates_the_documented_casa_defects(cases: &[Value]) {
    for name in ["mosaic_models", "mosaic_reverse"] {
        let raw = cases
            .iter()
            .find(|c| c["name"] == name)
            .expect("mosaic case");
        let nrms = reference_nrms(raw, &corrected_reference(raw, cases));
        println!("{name}: raw CASA versus separate-model sum NRMS {nrms:.9e}");
        assert!(nrms > 1e-3, "fixture must retain the actual CASA defect");
    }
    let sd = cases
        .iter()
        .find(|c| c["name"] == "sd_models")
        .expect("SD case");
    assert!(
        sd["rows"].as_array().expect("rows").iter().any(|row| {
            row["antennas"][0] != row["antennas"][1]
                && row["data"]
                    .as_array()
                    .expect("DATA")
                    .iter()
                    .any(|sample| sample[0].as_f64() != Some(0.0))
        }),
        "fixture must retain CASA's total power in cross-correlations"
    );
}

fn a_failed_later_model_keeps_the_plan_and_releases_its_admission(reference: &Value) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let models = models(directory.path());
    let huge = write_model_of(
        directory.path(),
        "huge.image",
        4096,
        &[vec![(2048, 2048, 1.0)]],
    );
    let path = directory.path().join("atomic.ms");
    let mut sm = observed(&path, 1, 256 << 20);
    sm.predict(&PredictSpec {
        images: vec![models["a"].clone(), models["a"].clone()],
        ..PredictSpec::default()
    })
    .expect("two small images fit");
    let held = reserved_memory();
    for incremental in [false, true] {
        let error = sm
            .predict(&PredictSpec {
                images: vec![models["a"].clone(), huge.clone()],
                incremental,
                ..PredictSpec::default()
            })
            .expect_err("the later image exceeds the budget");
        assert!(error.admission_refused().is_some(), "{error}");
        assert_eq!(
            reserved_memory(),
            held,
            "a refused multi-image call releases its preparations"
        );
    }
    sm.close().expect("the previous prediction still closes");
    assert_casa(reference, &read_main(&path).0);
}

#[test]
fn multiple_model_predictions_match_casa_6_7_6_14() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().expect("cases");
    assert_eq!(cases.len(), 14);
    the_raw_fixture_demonstrates_the_documented_casa_defects(cases);
    let mut failed = Vec::new();
    for workers in [1, 3] {
        for case in cases {
            let reference = corrected_reference(case, cases);
            if std::panic::catch_unwind(|| {
                let actual = run_case(case, workers);
                if case["machine"] == "sd" {
                    for row in actual
                        .iter()
                        .filter(|row| row.antennas[0] != row.antennas[1])
                    {
                        assert!(
                            row.data
                                .iter()
                                .all(|value| value.re == 0.0 && value.im == 0.0)
                        );
                    }
                }
                assert_casa(&reference, &actual);
            })
            .is_err()
            {
                failed.push(format!("{} with {workers} workers", case["name"]));
            }
        }
    }
    let duplicate = cases
        .iter()
        .find(|case| case["name"] == "ft_duplicate")
        .expect("duplicate reference");
    a_failed_later_model_keeps_the_plan_and_releases_its_admission(duplicate);
    assert!(failed.is_empty(), "CASA comparisons failed: {failed:?}");
}
