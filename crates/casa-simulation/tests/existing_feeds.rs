// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reopened multi-feed simulations preserve stored rows and extend every feed.
mod existing_ms_calculations;
use casa_simulation::{ApplySpec, PredictSpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::{
    ArrayValue,
    measures::{EpochRef, MEpoch},
};
use existing_ms_calculations::predict_support;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn copy(source: &Path, output: &Path) {
    std::fs::create_dir_all(output).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "table.lock" {
            continue;
        }
        let destination = output.join(entry.file_name());
        if entry.path().is_dir() {
            copy(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn check_preservation(path: &Path, input: &Path, case: &Value) {
    // FEED is copied intact, including receptor offsets and polarization.
    for entry in std::fs::read_dir(input.join("FEED")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "table.lock" {
            assert_eq!(
                std::fs::read(entry.path()).unwrap(),
                std::fs::read(path.join("FEED").join(entry.file_name())).unwrap()
            );
        }
    }
    let table = Table::open(TableOptions::new(path)).unwrap();
    let threshold = case["threshold"].as_f64().unwrap();
    for (row, before) in case["before"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        if case["operation"] != "append" && before["time"].as_f64().unwrap() >= threshold {
            continue;
        }
        for column in ["DATA", "MODEL_DATA", "CORRECTED_DATA"] {
            let ArrayValue::Complex32(values) =
                table.cell_accessor(row, column).unwrap().array().unwrap()
            else {
                panic!("{column}")
            };
            for (index, expected) in before[column.to_lowercase()]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
            {
                let actual = values[[index % 4, index / 4]];
                assert_eq!(
                    [f64::from(actual.re), f64::from(actual.im)],
                    [expected[0].as_f64().unwrap(), expected[1].as_f64().unwrap()],
                    "untouched {column} row {row}"
                );
            }
        }
    }
}

#[test]
fn resumed_multiple_feeds_match_casa_calculations_and_append() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_existing_feeds_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_existing_feeds");
    for workers in [1, 3] {
        for case in fixture["cases"].as_array().unwrap() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("feeds.ms");
            let input = inputs.join(format!("{}.ms", case["layout"].as_str().unwrap()));
            copy(&input, &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources: predict_support::resources(workers, 2 << 30),
                    now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            match case["operation"].as_str().unwrap() {
                "append" => {
                    sm.set_times(
                        10.,
                        false,
                        MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                    )
                    .unwrap();
                    let mut scan = predict_support::scan("target", 40., 50.);
                    scan.add_observation = true;
                    scan.project = "continued".into();
                    scan.observer = "CASA simulator".into();
                    sm.observe(&scan).unwrap();
                }
                operation => {
                    sm.set_data(&[], &[0], case["predicate"].as_str().unwrap())
                        .unwrap();
                    if operation == "components" {
                        sm.predict(&PredictSpec {
                            images: Vec::new(),
                            component_lists: vec![inputs.join("spectrum.cl")],
                            incremental: false,
                        })
                        .unwrap();
                    } else {
                        sm.set_apply(&ApplySpec {
                            table: inputs
                                .join(format!("{}.cal", case["layout"].as_str().unwrap()))
                                .display()
                                .to_string(),
                            kind: String::new(),
                            interval_s: 0.,
                            interp: "nearest".into(),
                            calwt: false,
                            spwmap: vec![-1],
                            opacity: 0.,
                        })
                        .unwrap();
                        sm.corrupt().unwrap();
                    }
                }
            }
            sm.close().unwrap();
            let error = existing_ms_calculations::compare(&path, case);
            assert!(error < 1e-3, "{} W{workers}: {error}", case["name"]);
            check_preservation(&path, &input, case);
            if let Some(export) = std::env::var_os("CASA_RS_EXISTING_FEEDS_OUT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("w{workers}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
            println!("existing feeds {} W{workers}: NRMS {error}", case["name"]);
        }
    }
}
