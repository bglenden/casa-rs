// SPDX-License-Identifier: LGPL-3.0-or-later
//! Existing polynomial FIELD directions use CASA's iterator epoch.
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, CasaOutcome, PredictSpec, SessionResources, Simulator, SimulatorOptions,
};
use casa_types::measures::{EpochRef, MEpoch};
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
        let target = output.join(entry.file_name());
        if entry.path().is_dir() {
            copy(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn file_bytes(path: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut values = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "table.lock" {
            continue;
        }
        let name = std::path::PathBuf::from(entry.file_name());
        if entry.path().is_dir() {
            values.extend(
                file_bytes(&entry.path())
                    .into_iter()
                    .map(|(p, bytes)| (name.join(p), bytes)),
            );
        } else {
            values.push((name, std::fs::read(entry.path()).unwrap()));
        }
    }
    values.sort_by(|a, b| a.0.cmp(&b.0));
    values
}

#[test]
fn polynomial_fields_match_casa_prediction_and_append_refusal() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_polynomial_fields_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_polynomial_fields");
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    let mut policies = vec![
        ("w1", predict_support::resources(1, 2 << 30)),
        ("w3", predict_support::resources(3, 2 << 30)),
    ];
    if host.metal {
        policies.push((
            "metal3",
            SessionResources::pinned(
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
            if prefix == "metal3" && !case["operation"].as_str().unwrap().starts_with("image") {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("polynomial.ms");
            let input = inputs.join(format!("order{}.ms", case["order"].as_u64().unwrap()));
            copy(&input, &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
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
                scan.add_observation = true;
                scan.project = "continued".into();
                scan.observer = "CASA simulator".into();
                let error = sm
                    .observe(&scan)
                    .expect_err("CASA refuses polynomial-field append");
                assert_eq!(error.outcome(), CasaOutcome::ReturnsFalse);
                assert_eq!(case["casa_observe_return"], false);
            } else {
                sm.set_data(&[], &[0], case["predicate"].as_str().unwrap())
                    .unwrap();
                sm.predict(&PredictSpec {
                    images: if operation.starts_with("image") {
                        vec![inputs.join("spectrum.image")]
                    } else {
                        Vec::new()
                    },
                    component_lists: if operation.starts_with("components") {
                        vec![inputs.join("spectrum.cl")]
                    } else {
                        Vec::new()
                    },
                    incremental: false,
                })
                .unwrap();
                if operation == "image_then_early_image" {
                    sm.set_data(
                        &[],
                        &[0],
                        &format!("TIME < {}", predict_support::REFERENCE_MJD * 86400. + 10.),
                    )
                    .unwrap();
                    sm.predict(&PredictSpec {
                        images: vec![inputs.join("spectrum.image")],
                        component_lists: Vec::new(),
                        incremental: false,
                    })
                    .unwrap();
                }
            }
            sm.close().unwrap();
            if operation == "append" {
                assert_eq!(
                    case["before"], case["after"],
                    "CASA refusal changes no rows or metadata"
                );
                assert_eq!(
                    file_bytes(&input),
                    file_bytes(&path),
                    "native refusal changes no input files"
                );
            }
            for entry in std::fs::read_dir(input.join("FIELD")).unwrap() {
                let entry = entry.unwrap();
                if entry.file_name() != "table.lock" {
                    assert_eq!(
                        std::fs::read(entry.path()).unwrap(),
                        std::fs::read(path.join("FIELD").join(entry.file_name())).unwrap()
                    );
                }
            }
            let error = existing_ms_calculations::compare(&path, case);
            assert!(error < 1e-3, "{} {prefix}: NRMS {error}", case["name"]);
            if let Some(export) = std::env::var_os("CASA_RS_POLYNOMIAL_FIELDS_OUT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("{prefix}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
            println!("polynomial {} {prefix}: NRMS {error}", case["name"]);
        }
    }
}
