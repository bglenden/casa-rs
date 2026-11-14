// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent CASA controls at exact frequencies and short physical axes.
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
mod support;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

fn cases() -> Vec<Value> {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_frequency_boundaries_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 30);
    cases
}

#[test]
fn exact_frequency_flags_preserve_the_original_channel_flag() {
    for case in cases()
        .iter()
        .filter(|case| !case["probe"].as_str().unwrap().starts_with("short"))
    {
        assert_eq!(support::run(case, 1), support::run(case, 3));
    }
}

#[test]
fn physically_short_axes_use_casas_linear_fallback_before_flag_compression() {
    for case in cases()
        .iter()
        .filter(|case| case["probe"].as_str().unwrap().starts_with("short"))
    {
        assert_eq!(support::run(case, 1), support::run(case, 3));
    }
}
