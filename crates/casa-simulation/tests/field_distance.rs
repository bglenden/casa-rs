// SPDX-License-Identifier: LGPL-3.0-or-later
//! Finite-distance simulator image predictions against actual CASA DATA.
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, PredictSpec, PredictionOptions, SessionResources, Simulator, SimulatorOptions,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn field_and_image_distance_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_field_distance_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
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
        for case in fixture["cases"].as_array().unwrap() {
            let spec = &case["spec"];
            if prefix == "metal3" && spec["machine"] == "wproject" {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("distance.ms");
            let mut sm = support::session(&path, "ALMA", "XX YY", resources);
            let direction = MDirection::from_angles(
                support::CENTRE_RAD[0],
                support::CENTRE_RAD[1],
                DirectionRef::J2000,
            );
            if let Some(distance) = spec["earlier_distance"].as_f64() {
                sm.set_field("earlier", &direction, "", distance).unwrap();
            }
            sm.set_field(
                "last",
                &direction,
                "",
                spec["distance"].as_f64().unwrap_or(0.),
            )
            .unwrap();
            sm.observe(&support::scan("target", 0., 20.)).unwrap();
            sm.observe(&support::scan("offset", 20., 40.)).unwrap();
            sm.set_options(&PredictionOptions {
                ftmachine: spec["machine"].as_str().unwrap_or("ft").into(),
                w_planes: 16,
                ..PredictionOptions::default()
            })
            .unwrap();
            if spec["reopen"] == true {
                sm.close().unwrap();
                sm = Simulator::open_from_ms(
                    &path,
                    SimulatorOptions {
                        measures: casa_test_support::deterministic_measures_provider(),
                        resources,
                        now: casa_types::measures::MEpoch::from_mjd(
                            support::REFERENCE_MJD,
                            casa_types::measures::EpochRef::UTC,
                        ),
                    },
                )
                .unwrap();
            }
            if spec["selected"] == true {
                sm.set_data(&[], &[0], "ANTENNA1 == 0 && ANTENNA2 > 2")
                    .unwrap();
            }
            let a = support::write_model(
                directory.path(),
                "a.image",
                &[vec![(32, 32, 1.25), (41, 28, 0.75)]],
            );
            if let Some(distance) = spec["metadata_distance"].as_f64() {
                use casa_images::PagedImage;
                use casa_types::{RecordField, RecordValue, ScalarValue, Value as Cell};
                let mut image = PagedImage::<f32>::open(&a).unwrap();
                image
                    .set_misc_info(RecordValue::new(vec![RecordField::new(
                        "distance",
                        Cell::Scalar(ScalarValue::Float64(distance)),
                    )]))
                    .unwrap();
                image.save().unwrap();
            }
            let mut images = vec![a];
            if spec["multiple"] == true {
                images.push(support::write_model_of(
                    directory.path(),
                    "b.image",
                    96,
                    &(0..4)
                        .map(|channel| vec![(48, 48, 0.5 + 0.25 * channel as f32), (60, 49, -0.1)])
                        .collect::<Vec<_>>(),
                ));
            }
            let mut component_lists = Vec::new();
            if spec["components"] == true {
                let components = directory.path().join("point.cl");
                support::write_point_list(&components, support::CENTRE_RAD, 0.25);
                component_lists.push(components);
            }
            let arguments = PredictSpec {
                images,
                component_lists,
                incremental: false,
            };
            sm.predict(&arguments)
                .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
            if let Some(distance) = spec["second_distance"].as_f64() {
                sm.set_field("second", &direction, "", distance).unwrap();
                sm.predict(&PredictSpec {
                    incremental: true,
                    ..arguments
                })
                .unwrap();
            }
            if spec["selected"] == true {
                // A later setter cannot change the earlier operation or
                // the baselines used to calculate its focus positions.
                sm.set_data(&[], &[], "").unwrap();
            }
            sm.close().unwrap();
            if spec["selected"] == true {
                for row in support::read_main(&path).0 {
                    if !(row.field == 0 && row.antennas[0] == 0 && row.antennas[1] > 2) {
                        assert!(
                            row.data
                                .iter()
                                .all(|value| value.re == 0. && value.im == 0.)
                        );
                    }
                }
            }
            let nrms = existing_ms_calculations::compare_created(&path, case);
            println!("distance {} {prefix}: NRMS {nrms}", case["name"]);
            if let Some(export) = std::env::var_os("CASA_RS_FIELD_DISTANCE_OUT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("{prefix}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
            assert!(nrms < 1e-3, "{} {prefix}: {nrms}", case["name"]);
        }
    }
}
