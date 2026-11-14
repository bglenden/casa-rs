// SPDX-License-Identifier: LGPL-3.0-or-later
//! Default telescope beams and heterogeneous simulation dish classes.
#[path = "image_frame_combinations/eop.rs"]
mod eop;
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, PredictSpec, PredictionOptions, SessionResources, VoltagePatternSpec,
};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn telescope_beams_and_heterogeneous_mosaics_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_telescope_beams_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_telescope_beams");
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
            let telescope = case["telescope"].as_str().unwrap();
            let dishes: Vec<f64> = case["dishes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect();
            let mut sm = support::session_with_band(
                &path,
                telescope,
                "XX XY YX YY",
                (
                    4,
                    case["frequency_hz"]
                        .as_f64()
                        .unwrap_or(support::FREQUENCY_HZ),
                    case["channel_hz"].as_f64().unwrap_or(support::CHANNEL_HZ),
                ),
                &dishes,
                resources,
                eop::provider(&case["eop"]),
            );
            for (field, start) in [("target", 0.), ("offset", 20.)] {
                let mut scan = support::scan(field, start, start + 20.);
                scan.observer = "CASA simulator".into();
                scan.project = "CASA simulation".into();
                sm.observe(&scan).unwrap();
            }
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
                dovp: operation != "mosaic",
                do_squint: false,
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            let mut prediction = PredictSpec::default();
            if operation == "components" {
                prediction.component_lists.push(inputs.join("sky.cl"));
            } else {
                prediction
                    .images
                    .push(inputs.join(case["image"].as_str().unwrap_or("sky.image")));
            }
            if let Err(error) = sm.predict(&prediction) {
                failures.push(format!("{name} {prefix}: {error}"));
                continue;
            }
            sm.close().unwrap();
            let mut reference = case.clone();
            reference["all_fields_selected"] = true.into();
            let nrms = existing_ms_calculations::compare_created(&path, &reference);
            println!("telescope beam {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_TELESCOPE_BEAMS_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
