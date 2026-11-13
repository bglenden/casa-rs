// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA calculation comparisons shared by existing-MS spectral-layout cases.
#[path = "../predict_support/mod.rs"]
mod predict_support;

use casa_simulation::{ApplySpec, BackendRequest, PredictSpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, ScalarValue};
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
        let target = output.join(entry.file_name());
        if entry.path().is_dir() {
            copy(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn compare(path: &Path, case: &Value) -> f64 {
    let table = Table::open(TableOptions::new(path)).unwrap();
    let expected = case["after"]["rows"].as_array().unwrap();
    assert_eq!(table.row_count(), expected.len());
    let mut error = 0.;
    let mut norm = 0.;
    for (row, expected) in expected.iter().enumerate() {
        for column in ["DATA", "MODEL_DATA", "CORRECTED_DATA"] {
            let ArrayValue::Complex32(values) =
                table.cell_accessor(row, column).unwrap().array().unwrap()
            else {
                panic!("{column}");
            };
            let reference = expected[column.to_lowercase()].as_array().unwrap();
            assert_eq!(values.len(), reference.len());
            for (index, reference) in reference.iter().enumerate() {
                let actual = values[[index % 4, index / 4]];
                let reference = [
                    reference[0].as_f64().unwrap(),
                    reference[1].as_f64().unwrap(),
                ];
                if expected["field"] == 1 || column == "MODEL_DATA" {
                    assert_eq!(
                        [f64::from(actual.re), f64::from(actual.im)],
                        reference,
                        "untouched {column} row{row}"
                    );
                } else {
                    error += (f64::from(actual.re) - reference[0]).powi(2)
                        + (f64::from(actual.im) - reference[1]).powi(2);
                    norm += reference[0].powi(2) + reference[1].powi(2);
                }
            }
        }
        for column in ["WEIGHT", "SIGMA"] {
            let ArrayValue::Float32(values) =
                table.cell_accessor(row, column).unwrap().array().unwrap()
            else {
                panic!("{column}")
            };
            let reference = expected[column.to_lowercase()].as_array().unwrap();
            for (actual, reference) in values.iter().zip(reference) {
                assert_eq!(f64::from(*actual), reference.as_f64().unwrap());
            }
        }
        let ArrayValue::Bool(flags) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("FLAG")
        };
        for (index, flag) in expected["flags"].as_array().unwrap().iter().enumerate() {
            assert_eq!(flags[[index % 4, index / 4]], flag.as_bool().unwrap());
        }
        let ScalarValue::Bool(flag) = table
            .cell_accessor(row, "FLAG_ROW")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("FLAG_ROW")
        };
        assert_eq!(*flag, expected["flag_row"].as_bool().unwrap());
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("TIME")
        };
        assert_eq!(*time, expected["time"].as_f64().unwrap());
    }
    let spectral = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW"))).unwrap();
    let ArrayValue::Float64(frequencies) = spectral
        .cell_accessor(0, "CHAN_FREQ")
        .unwrap()
        .array()
        .unwrap()
    else {
        panic!("CHAN_FREQ")
    };
    for (actual, expected) in frequencies
        .iter()
        .zip(case["frequencies_hz"].as_array().unwrap())
    {
        assert_eq!(*actual, expected.as_f64().unwrap());
    }
    (error / norm.max(1.)).sqrt()
}

pub fn run(fixture_data: &[u8], input_name: &str, export_name: &str) {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(fixture_data)).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(input_name);
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    let mut policies = vec![
        ("w1", predict_support::resources(1, 2 << 30)),
        ("w3", predict_support::resources(3, 2 << 30)),
    ];
    if host.metal {
        policies.push((
            "metal3",
            casa_simulation::SessionResources::pinned(
                host,
                casa_imaging_runtime::ResourcePolicy::Explicit {
                    workers: 3,
                    memory: 2 << 30,
                },
                BackendRequest::Metal,
            )
            .unwrap(),
        ));
    }
    for (prefix, resources) in policies {
        for case in fixture["cases"].as_array().unwrap() {
            let operation = case["operation"].as_str().unwrap();
            if prefix == "metal3" && operation != "image" {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("irregular.ms");
            copy(
                &inputs.join(format!("{}.ms", case["axis"].as_str().unwrap())),
                &path,
            );
            let original_reference = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW")))
                .unwrap()
                .column_keywords("REF_FREQUENCY")
                .cloned();
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_data(&[], &[0], "").unwrap();
            if operation == "bandpass" || operation == "gspline" {
                sm.set_apply(&ApplySpec {
                    table: inputs
                        .join(format!("{operation}.cal"))
                        .display()
                        .to_string(),
                    kind: String::new(),
                    interval_s: 0.,
                    interp: if operation == "bandpass" {
                        "nearest,linear"
                    } else {
                        "linear"
                    }
                    .into(),
                    calwt: false,
                    spwmap: vec![-1],
                    opacity: 0.,
                })
                .unwrap();
                sm.corrupt().unwrap();
            } else {
                if operation == "beam_components" {
                    sm.set_vp(&casa_simulation::VoltagePatternSpec {
                        do_squint: false,
                        ..casa_simulation::VoltagePatternSpec::default()
                    })
                    .unwrap();
                }
                sm.predict(&PredictSpec {
                    images: if operation == "image" {
                        vec![inputs.join("spectrum.image")]
                    } else {
                        Vec::new()
                    },
                    component_lists: if operation.ends_with("components") {
                        vec![inputs.join("spectrum.cl")]
                    } else {
                        Vec::new()
                    },
                    incremental: false,
                })
                .unwrap();
            }
            sm.close().unwrap();
            let reference = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW")))
                .unwrap()
                .column_keywords("REF_FREQUENCY")
                .cloned();
            assert_eq!(
                reference, original_reference,
                "reference-frequency frame metadata persists"
            );
            let error = compare(&path, case);
            assert!(error < 1e-3, "{} {prefix}: DATA NRMS {error}", case["name"]);
            println!("irregular {} {prefix}: NRMS {error}", case["name"]);
            if let Some(export) = std::env::var_os(export_name) {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("{prefix}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
        }
    }
}
