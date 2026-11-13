// SPDX-License-Identifier: LGPL-3.0-or-later
//! Synthetic sky-component signals against actual CASA 6.7.6.14 DATA.
mod existing_ms_calculations;
use casa_simulation::{BackendRequest, PredictSpec, SessionResources, Simulator, SimulatorOptions};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn limb_darkened_component_signals_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_component_signals_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    let mut policies = vec![
        ("w1", support::resources(1, 2 << 30)),
        ("w3", support::resources(3, 2 << 30)),
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
        let mut failures = Vec::new();
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("components.ms");
            let mut sm = if case["spec"]["distance"].is_number() {
                support::session(&path, "ALMA", "XX XY YX YY", resources)
            } else {
                copy(&data.join("sm_image_layouts/base.ms"), &path);
                Simulator::open_from_ms(
                    &path,
                    SimulatorOptions {
                        measures: casa_test_support::deterministic_measures_provider(),
                        resources,
                        now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                    },
                )
                .unwrap()
            };
            if let Some(distance) = case["spec"]["distance"].as_f64() {
                let direction = MDirection::from_angles(
                    support::CENTRE_RAD[0],
                    support::CENTRE_RAD[1],
                    DirectionRef::J2000,
                );
                sm.set_field("last", &direction, "", distance).unwrap();
                sm.observe(&support::scan("target", 0., 20.)).unwrap();
                sm.observe(&support::scan("offset", 20., 40.)).unwrap();
                sm.set_data(&[], &[], "FIELD_ID == 0").unwrap();
            } else {
                sm.set_data(&[], &[0], "").unwrap();
            }
            let lists = vec![data.join("sm_component_signals").join(format!("{name}.cl"))];
            let list = casa_components::ComponentList::read_table(&lists[0]).unwrap();
            if let Some(exponent) = case["spec"]["exponent"].as_f64() {
                assert_eq!(
                    list.components()[0].shape().shape_type(),
                    casa_components::ShapeType::LimbDarkenedDisk
                );
                let actual = list.components()[0].limb_darkening_exponent().unwrap();
                assert!(
                    (actual - exponent).abs() < 1e-7,
                    "{name}: exponent {actual}"
                );
            }
            sm.predict(&PredictSpec {
                component_lists: lists.clone(),
                ..PredictSpec::default()
            })
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            if case["spec"]["incremental"] == true {
                sm.predict(&PredictSpec {
                    component_lists: lists,
                    incremental: true,
                    ..PredictSpec::default()
                })
                .unwrap();
            }
            sm.close().unwrap();
            let mut reference = case.clone();
            if let Some(rows) = case.get("corrected_rows") {
                reference["after"]["rows"] = rows.clone();
            }
            let nrms = if case["spec"]["distance"].is_number() {
                existing_ms_calculations::compare_created(&path, &reference)
            } else {
                existing_ms_calculations::compare(&path, &reference)
            };
            println!("component {name} {prefix}: NRMS {nrms}");
            if nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: NRMS {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_COMPONENT_SIGNALS_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
