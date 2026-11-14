// SPDX-License-Identifier: LGPL-3.0-or-later
//! Intrinsic mosaic field coverage and accepted telescope refusals.
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, CasaOutcome, PredictSpec, PredictionOptions, SessionResources, Simulator,
    SimulatorOptions, VoltagePatternSpec,
};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn mosaic_coverage_and_unknown_telescope_refusals_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_mosaic_coverage_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let inputs = data.join("sm_mosaic_coverage");
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
            let profile = case["profile"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("signals.ms");
            copy(&inputs.join(format!("{profile}.ms")), &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_options(&PredictionOptions {
                ftmachine: "mosaic".into(),
                ..PredictionOptions::default()
            })
            .unwrap();
            sm.set_vp(&VoltagePatternSpec {
                dovp: false,
                do_squint: false,
                use_default_vp: profile != "common_outside",
                vp_table: if profile == "common_outside" {
                    data.join("sm_voltage_patterns/gaussian_pb.vp")
                        .display()
                        .to_string()
                } else {
                    Default::default()
                },
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            sm.set_data(
                &[],
                if case["all_fields_selected"] == true {
                    &[]
                } else {
                    &[0]
                },
                "",
            )
            .unwrap();
            let mut prediction = PredictSpec::default();
            if profile == "outside" || profile == "fused" {
                prediction.images.push(inputs.join("outside.image"));
            }
            if profile == "common_outside" {
                prediction.images.push(inputs.join("common_outside.image"));
            } else if profile != "outside" {
                prediction
                    .images
                    .push(data.join("sm_telescope_beams/sky.image"));
            }
            let result = sm.predict(&prediction);
            if case["returned"] == true {
                result.unwrap_or_else(|error| panic!("{name}: {error}"));
            } else {
                assert_eq!(
                    result.unwrap_err().outcome(),
                    CasaOutcome::ReturnsFalse,
                    "{name}"
                );
            }
            sm.close().unwrap();
            let mut reference = case.clone();
            if let Some(rows) = case.get("corrected_rows") {
                reference["after"]["rows"] = rows.clone();
            }
            let nrms = existing_ms_calculations::compare(&path, &reference);
            println!("mosaic coverage {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_MOSAIC_COVERAGE_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
