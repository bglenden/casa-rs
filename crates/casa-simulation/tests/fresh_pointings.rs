// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fresh observeMany keeps visibility phase separate from the beam pointing.
#[path = "image_frame_combinations/eop.rs"]
mod eop;
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, ObserveManySpec, Pointing, PredictSpec, PredictionOptions, SessionResources,
    VoltagePatternSpec,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn fresh_displaced_beam_and_mosaic_pointings_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_fresh_pointings_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_fresh_pointings");
    let mut policies = vec![
        ("w1", support::resources(1, 2 << 30)),
        ("w3", support::resources(3, 2 << 30)),
    ];
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
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
    let mut failures = Vec::new();
    for (prefix, resources) in policies {
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let operation = case["operation"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("pointings.ms");
            let mut sm = support::session_with_measures(
                &path,
                "ALMA",
                "XX XY YX YY",
                4,
                &[12.],
                resources,
                eop::provider(&case["eop"]),
            );
            let offsets = if case["profile"] == "changing" {
                [[2e-5, -1e-5], [-3e-5, 2e-5]]
            } else {
                [[2e-5, -1e-5]; 2]
            };
            let base = support::scan("target", 0., 20.);
            sm.observe_many(&ObserveManySpec {
                pointings: offsets
                    .iter()
                    .enumerate()
                    .map(|(index, offset)| Pointing {
                        source: "target".into(),
                        start_s: index as f64 * 20.,
                        stop_s: (index + 1) as f64 * 20.,
                        direction: Some(MDirection::from_angles(
                            support::CENTRE_RAD[0] + offset[0],
                            support::CENTRE_RAD[1] + offset[1],
                            DirectionRef::J2000,
                        )),
                    })
                    .collect(),
                spectral_window: "band6".into(),
                add_observation: false,
                state: base.state,
                observer: "CASA simulator".into(),
                project: "CASA simulation".into(),
            })
            .unwrap();
            sm.set_options(&PredictionOptions {
                ftmachine: if operation == "mosaic" {
                    "mosaic"
                } else {
                    "ft"
                }
                .into(),
                ..PredictionOptions::default()
            })
            .unwrap();
            sm.set_vp(&VoltagePatternSpec {
                dovp: operation.ends_with("beam"),
                do_squint: false,
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            let mut prediction = PredictSpec::default();
            if operation.starts_with("components") {
                prediction.component_lists.push(inputs.join("sky.cl"));
            } else {
                prediction.images.push(inputs.join("sky.image"));
            }
            sm.predict(&prediction)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare_created(&path, case);
            println!("fresh pointing {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_FRESH_POINTINGS_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
