// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA legacy tables imposed on simulated DATA (SIM-16).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
#[allow(dead_code, reason = "reuse the complete MS comparison")]
mod support;

use casa_simulation::{ApplySpec, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;
use std::path::Path;

fn run(name: &str) {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_polynomial_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap();
    let kind = case["kind"].as_str().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = if let Some(root) = std::env::var_os("CASA_RS_POLYNOMIAL_OUT") {
        std::fs::create_dir_all(&root).unwrap();
        std::path::PathBuf::from(root).join(format!("{name}.ms"))
    } else {
        directory.path().join("applied.ms")
    };
    let mut sm = predict_support::session(
        &path,
        "ALMA",
        "XX XY YX YY",
        predict_support::resources(1, 2 << 30),
    );
    for (source, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
        sm.observe(&predict_support::scan(source, start, stop))
            .unwrap();
    }
    if case["extended"] == true {
        sm.set_spectral_window(&casa_simulation::SpectralWindowSpec {
            name: "band7".into(),
            start_frequency_hz: 300e9,
            channel_width_hz: predict_support::CHANNEL_HZ,
            resolution_hz: predict_support::CHANNEL_HZ,
            frame: casa_types::measures::frequency::FrequencyRef::TOPO,
            channel_count: 4,
            stokes: "XX XY YX YY".into(),
        })
        .unwrap();
        for (window, start, stop) in [
            ("band6", -120., -80.),
            ("band7", 40., 80.),
            ("band6", 400., 440.),
        ] {
            let mut scan = predict_support::scan("target", start, stop);
            // The shared native fixture calls the original window "band6".
            scan.spectral_window = window.into();
            sm.observe(&scan).unwrap();
        }
    }
    sm.close().unwrap();
    input::prepare(&path);
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(1, 2 << 30),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    let table = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/sm_polynomial")
        .join(kind);
    sm.set_apply(&ApplySpec {
        table: table.display().to_string(),
        kind: String::new(),
        interval_s: 0.,
        interp: case["interp"].as_str().unwrap_or("linear").into(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.,
    })
    .unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    support::compare(&path, case);
}

#[test]
fn supplied_bpoly_matches_casa() {
    run("bpoly");
}
#[test]
fn supplied_bpoly_context_modifiers_match_casa() {
    run("bpoly_perobs");
    run("bpoly_perscan");
}

#[test]
fn supplied_gspline_matches_casa() {
    run("gspline");
}

#[test]
fn supplied_gspline_other_frequency_and_time_range_match_casa() {
    run("gspline_extended");
}
