// SPDX-License-Identifier: LGPL-3.0-or-later
//! Non-default G/D application arguments against independent CASA (#1053).

#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;

#[path = "apply_parameters/support.rs"]
mod support;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;
use support::run;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_parameters_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 28);
    // An implementation that ignores interp must fail the scientific gate.
    for kind in ["G", "D"] {
        let cases = fixture["cases"].as_array().unwrap();
        let control = |mode: &str| {
            cases
                .iter()
                .find(|case| case["name"] == format!("{kind}_{mode}_control"))
                .unwrap()
        };
        let (nearest, linear) = (control("nearest"), control("linear"));
        let (mut error, mut power) = (0.0_f64, 0.0_f64);
        for (a, b) in nearest["rows"]
            .as_array()
            .unwrap()
            .iter()
            .zip(linear["rows"].as_array().unwrap())
        {
            for (a, b) in a["data"]
                .as_array()
                .unwrap()
                .iter()
                .zip(b["data"].as_array().unwrap())
            {
                let value = |sample: &Value| {
                    num_complex::Complex64::new(
                        sample[0].as_f64().unwrap(),
                        sample[1].as_f64().unwrap(),
                    )
                };
                error += (value(a) - value(b)).norm_sqr();
                power += value(b).norm_sqr();
            }
        }
        assert!(
            (error / power).sqrt() > 2e-3,
            "{kind}: discriminating interpolation inputs"
        );
    }
    fixture
}

#[test]
fn table_application_arguments_match_casa_and_preserve_weights() {
    for case in fixture()["cases"].as_array().unwrap() {
        assert_eq!(run(case, 1), run(case, 3), "workers for {}", case["name"]);
    }
}
