// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulation selections/calculations/appends resolve actual DD-to-SPW mappings.
mod existing_ms_calculations;
use casa_simulation::{ApplySpec, PredictSpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::{
    ArrayValue, ScalarValue,
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
        if entry.path().is_dir() {
            copy(&entry.path(), &output.join(entry.file_name()));
        } else {
            std::fs::copy(entry.path(), output.join(entry.file_name())).unwrap();
        }
    }
}

fn compare_descriptions(path: &Path, case: &Value) {
    for name in ["DATA_DESCRIPTION", "POLARIZATION"] {
        let table = Table::open(TableOptions::new(path.join(name))).unwrap();
        let rows = case["description_metadata"][name].as_array().unwrap();
        assert_eq!(table.row_count(), rows.len());
        for (row, expected) in rows.iter().enumerate() {
            for (column, expected) in expected.as_object().unwrap() {
                let cell = table.cell_accessor(row, column).unwrap();
                if expected.is_array() {
                    let ArrayValue::Int32(actual) = cell.array().unwrap() else {
                        panic!("{column}")
                    };
                    let expected: Vec<i64> = if column == "CORR_PRODUCT" {
                        expected
                            .as_array()
                            .unwrap()
                            .iter()
                            .flat_map(|v| v.as_array().unwrap())
                            .map(|v| v.as_i64().unwrap())
                            .collect()
                    } else {
                        expected
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| v.as_i64().unwrap())
                            .collect()
                    };
                    assert_eq!(
                        actual.iter().map(|v| i64::from(*v)).collect::<Vec<_>>(),
                        expected
                    );
                } else {
                    match cell.scalar().unwrap() {
                        ScalarValue::Int32(actual) => {
                            assert_eq!(Some(i64::from(*actual)), expected.as_i64())
                        }
                        ScalarValue::Bool(actual) => assert_eq!(Some(*actual), expected.as_bool()),
                        value => panic!("{column}: {value:?}"),
                    }
                }
            }
        }
    }
}

fn bytes(path: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "table.lock" {
            continue;
        }
        if entry.path().is_dir() {
            let parent = std::path::PathBuf::from(entry.file_name());
            result.extend(
                bytes(&entry.path())
                    .into_iter()
                    .map(|(p, v)| (parent.join(p), v)),
            );
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
fn invalid_description_references_leave_the_input_unchanged() {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_data_descriptions/alias.ms");
    for column in ["SPECTRAL_WINDOW_ID", "POLARIZATION_ID"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("invalid.ms");
        copy(&source, &path);
        let mut table = Table::open(TableOptions::new(path.join("DATA_DESCRIPTION"))).unwrap();
        table
            .cell_accessor_mut(2, column)
            .unwrap()
            .set(casa_types::Value::Scalar(ScalarValue::Int32(-1)))
            .unwrap();
        table
            .save(TableOptions::new(path.join("DATA_DESCRIPTION")))
            .unwrap();
        drop(table);
        let before = bytes(&path);
        let error = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: casa_test_support::deterministic_measures_provider(),
                resources: predict_support::resources(1, 2 << 30),
                now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .err()
        .expect("invalid description is rejected");
        assert!(error.to_string().contains(column), "{error}");
        assert_eq!(bytes(&path), before);
    }
}

#[test]
fn mapped_descriptions_match_casa_and_append_labels_remain_valid() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_data_descriptions_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_data_descriptions");
    for workers in [1, 3] {
        for case in fixture["cases"].as_array().unwrap() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("mapped.ms");
            copy(
                &inputs.join(format!("{}.ms", case["layout"].as_str().unwrap())),
                &path,
            );
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources: predict_support::resources(workers, 2 << 30),
                    now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            let operation = case["operation"].as_str().unwrap();
            if operation == "append" {
                sm.set_times(
                    10.,
                    false,
                    MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                )
                .unwrap();
                let mut scan = predict_support::scan("target", 40., 60.);
                if case["requested_spw"] == 1 {
                    scan.spectral_window = "band6b".into();
                }
                scan.add_observation = true;
                scan.project = "continued".into();
                scan.observer = "CASA simulator".into();
                sm.observe(&scan).unwrap();
            } else {
                let windows = match operation {
                    "components0" => vec![0],
                    "components1" => vec![1],
                    _ => Vec::new(),
                };
                sm.set_data(
                    &windows,
                    &[0],
                    if operation == "query" {
                        "DATA_DESC_ID == 2"
                    } else {
                        ""
                    },
                )
                .unwrap();
                if operation.starts_with("components") {
                    sm.predict(&PredictSpec {
                        images: Vec::new(),
                        component_lists: vec![inputs.join("spectrum.cl")],
                        incremental: false,
                    })
                    .unwrap();
                } else {
                    sm.set_apply(&ApplySpec {
                        table: inputs.join("gain.cal").display().to_string(),
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
            sm.close().unwrap();
            compare_descriptions(&path, case);
            let error = existing_ms_calculations::compare(&path, case);
            assert!(error < 1e-3, "{} W{workers}: NRMS {error}", case["name"]);
            let table = Table::open(TableOptions::new(&path)).unwrap();
            let before = case["before"]["rows"].as_array().unwrap().len();
            for (row, expected) in case["after"]["rows"].as_array().unwrap().iter().enumerate() {
                let expected = if operation == "append" && row >= before {
                    // CASA's observe writes the physical SPW ID here. Resolve
                    // the requested window's actual DD row to avoid its
                    // documented wrong/out-of-range label bug.
                    case["native_append_description"].as_i64().unwrap()
                } else {
                    expected["data_desc_id"].as_i64().unwrap()
                };
                let ScalarValue::Int32(actual) = table
                    .cell_accessor(row, "DATA_DESC_ID")
                    .unwrap()
                    .scalar()
                    .unwrap()
                else {
                    panic!("DATA_DESC_ID")
                };
                assert_eq!(i64::from(*actual), expected);
            }
            if let Some(export) = std::env::var_os("CASA_RS_SIMULATION_DESCRIPTIONS_EXPORT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("w{workers}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
            println!(
                "description {} W{workers}: DATA NRMS {error}, labels valid",
                case["name"]
            );
        }
    }
}
