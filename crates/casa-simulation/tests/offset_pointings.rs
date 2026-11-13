// SPDX-License-Identifier: LGPL-3.0-or-later
//! Consume existing simulation POINTING in beam, mosaic and total power.
mod existing_ms_calculations;
use casa_simulation::{
    PredictSpec, PredictionOptions, Simulator, SimulatorOptions, VoltagePatternSpec,
};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn fixed_offset_pointings_match_casa_calculations() {
    run("fixed");
}

#[test]
fn changing_offset_pointings_follow_the_antennas() {
    run("changing");
}

#[test]
fn incomplete_antenna_pointings_are_refused_before_prediction() {
    use casa_tables::{Table, TableOptions};
    use casa_types::{ScalarValue, Value};
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_offset_pointings");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("incomplete.ms");
    copy(&inputs.join("fixed.ms"), &path);
    let mut pointing = Table::open(TableOptions::new(path.join("POINTING"))).unwrap();
    for row in 0..pointing.row_count() {
        pointing
            .cell_accessor_mut(row, "ANTENNA_ID")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Int32(0)))
            .unwrap();
    }
    pointing
        .save(TableOptions::new(path.join("POINTING")))
        .unwrap();
    drop(pointing);
    let before = std::fs::read(path.join("table.dat")).unwrap();
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(1, 2 << 30),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    sm.set_options(&PredictionOptions {
        ftmachine: "sd".into(),
        grid_function: "BOX".into(),
        ..PredictionOptions::default()
    })
    .unwrap();
    let error = sm
        .predict(&PredictSpec {
            images: vec![inputs.join("sd.image")],
            ..PredictSpec::default()
        })
        .expect_err("one antenna's offset must not become every antenna's pointing");
    assert_eq!(
        error.outcome(),
        casa_simulation::CasaOutcome::NotYetImplemented
    );
    assert!(error.message().contains("coverage for every antenna"));
    sm.close().unwrap();
    assert_eq!(std::fs::read(path.join("table.dat")).unwrap(), before);
}

fn run(profile: &str) {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_offset_pointings_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_offset_pointings");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(
        cases
            .iter()
            .filter(|case| case["profile"] == profile)
            .count(),
        8
    );
    let mut policies = vec![
        ("w1", predict_support::resources(1, 2 << 30)),
        ("w3", predict_support::resources(3, 2 << 30)),
    ];
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    if host.metal {
        policies.push((
            "metal3",
            casa_simulation::SessionResources::pinned(
                host,
                casa_imaging_runtime::ResourcePolicy::Explicit {
                    workers: 3,
                    memory: 2 << 30,
                },
                casa_simulation::BackendRequest::Metal,
            )
            .unwrap(),
        ));
    }
    for (prefix, resources) in policies {
        for case in cases.iter().filter(|case| case["profile"] == profile) {
            let operation = case["operation"].as_str().unwrap();
            if prefix == "metal3" && !operation.starts_with("image") {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("pointing.ms");
            copy(&inputs.join(format!("{profile}.ms")), &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            let machine = if operation == "mosaic" {
                "mosaic"
            } else if operation.starts_with("sd_") {
                "sd"
            } else {
                "ft"
            };
            sm.set_options(&PredictionOptions {
                ftmachine: machine.into(),
                grid_function: match operation {
                    "sd_pb" => "PB",
                    _ if machine == "sd" => "BOX",
                    _ => "SF",
                }
                .into(),
                ..PredictionOptions::default()
            })
            .unwrap();
            if operation.contains("beam") {
                sm.set_vp(&VoltagePatternSpec {
                    use_default_vp: false,
                    ..VoltagePatternSpec::default()
                })
                .unwrap();
            }
            sm.set_data(&[], &[0], case["predicate"].as_str().unwrap())
                .unwrap();
            let components = operation.starts_with("components");
            sm.predict(&PredictSpec {
                images: if components {
                    Vec::new()
                } else {
                    vec![inputs.join(if machine == "sd" {
                        "sd.image"
                    } else {
                        "spectrum.image"
                    })]
                },
                component_lists: if components {
                    vec![inputs.join("spectrum.cl")]
                } else {
                    Vec::new()
                },
                incremental: false,
            })
            .unwrap();
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare(&path, case);
            assert!(nrms < 1e-3, "{} {prefix}: NRMS {nrms}", case["name"]);
            println!("offset {} {prefix}: NRMS {nrms}", case["name"]);
            if let Some(export) = std::env::var_os("CASA_RS_OFFSET_POINTINGS_OUT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("{prefix}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
        }
    }
}
