// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA simulation Fourier/grid modes and their refusal stages.
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
fn simulation_ft_modes_and_grid_functions_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_ft_modes_casa.json.gz")[..],
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
            let machine = case["machine"].as_str().unwrap();
            let grid = case["grid"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("signals.ms");
            copy(&inputs.join("outside.ms"), &path);
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
                ftmachine: machine.into(),
                grid_function: grid.into(),
                w_planes: 16,
                ..PredictionOptions::default()
            })
            .unwrap();
            sm.set_vp(&VoltagePatternSpec {
                dovp: false,
                do_squint: false,
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
            let prediction = PredictSpec {
                images: vec![data.join(if case["shifted"] == true {
                    "sm_ft_modes/kernels.image"
                } else if case["large"] == true {
                    "sm_ft_modes/large.image"
                } else {
                    "sm_telescope_beams/sky.image"
                })],
                ..PredictSpec::default()
            };
            let result = sm.predict(&prediction);
            if case["returned"] == true {
                result.unwrap_or_else(|error| panic!("{name}: {error}"));
            } else {
                if result.is_ok() {
                    failures.push(format!("{name}: unexpectedly succeeded"));
                    sm.close().unwrap();
                    continue;
                }
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
            println!("FT mode {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_FT_MODES_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
