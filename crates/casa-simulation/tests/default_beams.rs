// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulator-accepted telescope defaults, including rejected factory aliases.
#[path = "image_frame_combinations/eop.rs"]
#[allow(
    dead_code,
    reason = "this fixture uses the captured observatory variant"
)]
mod eop;
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, CasaOutcome, PredictSpec, SessionResources, VoltagePatternSpec,
};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn default_telescope_beams_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_default_beams_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_default_beams");
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
            let telescope = case["telescope"].as_str().unwrap();
            let profile = case["profile"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("default.ms");
            let mut sm = support::session_with_band(
                &path,
                telescope,
                "XX YY",
                (
                    4,
                    case["frequency_hz"].as_f64().unwrap(),
                    case["channel_hz"].as_f64().unwrap(),
                ),
                &[12.],
                resources,
                eop::with_observatory(&case["eop"], telescope, &case["observatory_itrf_m"]),
            );
            for (field, start) in [("target", 0.), ("offset", 20.)] {
                let mut scan = support::scan(field, start, start + 20.);
                scan.observer = "CASA simulator".into();
                scan.project = "CASA simulation".into();
                sm.observe(&scan).unwrap();
            }
            sm.set_vp(&VoltagePatternSpec {
                do_squint: false,
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            let mut prediction = PredictSpec::default();
            if case["operation"] == "components" {
                prediction
                    .component_lists
                    .push(inputs.join(format!("{profile}.cl")));
            } else {
                prediction
                    .images
                    .push(inputs.join(format!("{profile}.image")));
            }
            let result = sm.predict(&prediction);
            if case["accepted"] == false {
                let error = result.expect_err("CASA refuses this unregistered default");
                assert_eq!(
                    error.outcome(),
                    CasaOutcome::ReturnsFalse,
                    "{name}: {error}"
                );
            } else if let Err(error) = result {
                failures.push(format!("{name} {prefix}: {error}"));
                continue;
            }
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare_created(&path, case);
            println!("default beam {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_DEFAULT_BEAMS_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
