// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar T tables against CASA, across correlations and invalid solutions (#1053).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
mod support;

use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

#[test]
fn scalar_tables_match_casa_for_each_correlation_layout() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_scalar_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 38);
    let control = |interp: &str| {
        cases
            .iter()
            .find(|case| case["name"] == format!("T_{interp}_full_control"))
            .unwrap()
    };
    let (mut error, mut power) = (0.0, 0.0);
    for (nearest, linear) in control("nearest")["rows"]
        .as_array()
        .unwrap()
        .iter()
        .zip(control("linear")["rows"].as_array().unwrap())
    {
        for (a, b) in nearest["data"]
            .as_array()
            .unwrap()
            .iter()
            .zip(linear["data"].as_array().unwrap())
        {
            for coordinate in 0..2 {
                let (a, b) = (
                    a[coordinate].as_f64().unwrap(),
                    b[coordinate].as_f64().unwrap(),
                );
                error += (a - b).powi(2);
                power += b.powi(2);
            }
        }
    }
    assert!(
        (error / power).sqrt() > 2e-3,
        "interpolation must discriminate"
    );
    for case in cases {
        if case["probe"] == "flag_all_x" || case["probe"] == "missing_antenna" {
            let antenna = if case["probe"] == "missing_antenna" {
                7
            } else {
                0
            };
            let mut autocorrelations = 0;
            for row in case["rows"].as_array().unwrap() {
                if row["antennas"] == serde_json::json!([antenna, antenna]) {
                    autocorrelations += 1;
                    for sample in row["data"].as_array().unwrap() {
                        assert_eq!(sample, &serde_json::json!([3.0, 4.0]));
                    }
                }
            }
            assert!(autocorrelations > 0);
        }
        assert_eq!(
            support::run(case, 1),
            support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}
