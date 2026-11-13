// SPDX-License-Identifier: LGPL-3.0-or-later
//! Remaining supplied physical-error operators in simulated DATA (SIM-16).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
#[allow(dead_code, reason = "reuse independent inputs and full MS comparison")]
mod support;

use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

fn run(name: &str) {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_remaining_priors_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap();
    if case["outcome"] == "returned" {
        assert_eq!(support::run(case, 1), support::run(case, 3));
    } else {
        let directory = tempfile::tempdir().unwrap();
        let (path, mut sm, spec) = support::prepared(directory.path(), case, 1);
        let error = sm.set_apply(&spec).unwrap_err();
        assert_eq!(error.outcome(), casa_simulation::CasaOutcome::Raises);
        sm.close().unwrap();
        support::compare(&path, case);
    }
}

#[test]
fn switched_power_missing_context_matches_casa() {
    run("switched_power");
}
#[test]
fn switched_power_matches_casa() {
    run("switched_power_present");
}
#[test]
fn fringe_matches_casa() {
    run("fringe");
}
#[test]
fn fringe_rate_aware_interpolation_matches_casa() {
    run("fringe_linear");
}
#[test]
fn ionosphere_matches_casa() {
    run("ionosphere");
}
#[test]
fn ionosphere_circular_matches_casa() {
    run("ionosphere_circular");
}

#[test]
fn unused_linear_window_does_not_change_circular_ionosphere() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_apply_remaining_priors_casa.json.gz")[..],
    ))
    .unwrap();
    let mut case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "ionosphere_circular")
        .unwrap()
        .clone();
    case["unused_stokes"] = "XX XY YX YY".into();
    case["data_window"] = 1.into();
    assert_eq!(support::run(&case, 1), support::run(&case, 3));
}

#[test]
fn supplied_noise_gates_leakage_validation_on_an_absent_window() {
    let directory = tempfile::tempdir().unwrap();
    let case = serde_json::json!({
        "kind": "A Noise", "input_fixture": "sm_baseline/a_noise",
        "interp": "linear,nearest", "stokes": "XX YY",
        "unused_stokes": "XX XY YX YY", "data_window": 1,
    });
    let (path, mut sm, spec) = support::prepared(directory.path(), &case, 1);
    let before = predict_support::read_main(&path).0;
    sm.set_apply(&spec).unwrap();
    sm.set_leakage(&casa_simulation::LeakageSpec {
        table: String::new(),
        amplitude: vec![0.01],
        offset: vec![0.],
    })
    .unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    let after = predict_support::read_main(&path).0;
    assert_eq!(
        before.iter().map(|r| &r.data).collect::<Vec<_>>(),
        after.iter().map(|r| &r.data).collect::<Vec<_>>()
    );
}
#[test]
fn accor_matches_casa() {
    run("accor");
}
#[test]
fn single_dish_gain_matches_casa() {
    run("single_dish_gain");
}
