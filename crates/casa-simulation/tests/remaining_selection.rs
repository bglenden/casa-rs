// SPDX-License-Identifier: LGPL-3.0-or-later
//! Remaining simulator selection forms against actual CASA calculations.
mod existing_ms_calculations;
use casa_simulation::{CasaOutcome, PredictSpec, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn files(path: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "table.lock" {
            continue;
        }
        if entry.path().is_dir() {
            for (name, bytes) in files(&entry.path()) {
                result.push((
                    std::path::PathBuf::from(entry.file_name()).join(name),
                    bytes,
                ));
            }
        } else {
            result.push((
                entry.file_name().into(),
                std::fs::read(entry.path()).unwrap(),
            ));
        }
    }
    result.sort_by(|left, right| left.0.cmp(&right.0));
    result
}

#[test]
fn simulator_selection_forms_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_remaining_selection_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_remaining_selection");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 62);
    if let Ok(name) = std::env::var("CASA_RS_SELECTION_CASE") {
        assert!(
            cases.iter().any(|case| case["name"] == name),
            "unknown selection case {name}"
        );
    }
    let mut failures = Vec::new();
    for workers in [1, 3] {
        for case in cases {
            if std::env::var("CASA_RS_SELECTION_CASE").is_ok_and(|name| case["name"] != name) {
                continue;
            }
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("selected.ms");
                copy(&inputs.join("base.ms"), &path);
                let mut sm = Simulator::open_from_ms(
                    &path,
                    SimulatorOptions {
                        measures: casa_test_support::deterministic_measures_provider(),
                        resources: predict_support::resources(workers, 2 << 30),
                        now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                    },
                )
                .unwrap();
                if case["predict_before_selection"] == true {
                    sm.set_data(&[], &[0], "").unwrap();
                    sm.predict(&PredictSpec {
                        images: Vec::new(),
                        component_lists: vec![inputs.join("spectrum.cl")],
                        incremental: false,
                    })
                    .unwrap();
                }
                if case["append_before_selection"] == true {
                    sm.set_times(
                        10.,
                        false,
                        MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                    )
                    .unwrap();
                    let mut scan = predict_support::scan("target", 40., 60.);
                    scan.project = "continued".into();
                    scan.observer = "CASA simulator".into();
                    sm.observe(&scan).unwrap();
                }
                let before_selection = files(&path);
                let result = sm.set_data(&[], &[0], case["predicate"].as_str().unwrap());
                assert_eq!(
                    files(&path),
                    before_selection,
                    "selection preserves stored files"
                );
                if case["returned"] == true {
                    result.unwrap();
                } else {
                    assert_eq!(
                        result.unwrap_err().outcome(),
                        CasaOutcome::ReturnsFalse,
                        "{}",
                        case["name"]
                    );
                }
                sm.predict(&PredictSpec {
                    images: Vec::new(),
                    component_lists: vec![inputs.join("spectrum.cl")],
                    incremental: case["incremental"] == true,
                })
                .unwrap();
                sm.close().unwrap();
                let nrms = existing_ms_calculations::compare(&path, case);
                assert!(nrms < 1e-3, "{} W{workers}: NRMS {nrms}", case["name"]);
                println!("selection {} W{workers}: NRMS {nrms}", case["name"]);
                if let Some(export) = std::env::var_os("CASA_RS_REMAINING_SELECTION_OUT") {
                    copy(
                        &path,
                        &Path::new(&export)
                            .join(format!("w{workers}-{}.ms", case["name"].as_str().unwrap())),
                    );
                }
            }));
            if outcome.is_err() {
                failures.push(format!("{} W{workers}", case["name"]));
            }
        }
    }
    assert!(failures.is_empty(), "CASA selection failures: {failures:?}");
}
