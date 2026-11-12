// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flagged and missing G/D solutions against independent CASA (#1053).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
mod support;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

#[test]
fn flagged_and_missing_solutions_follow_casa_with_neutral_leakage() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_flags_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 24);
    for case in fixture["cases"].as_array().unwrap() {
        assert_eq!(
            support::run(case, 1),
            support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}
