// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency-dependent B/Tf/Df application against independent CASA (#1053).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
mod support;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

#[test]
fn frequency_tables_match_casa_with_phase_wraps_and_gaps() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_frequency_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 66);
    for kind in ["B", "Tf", "Df"] {
        let case = |mode: &str| {
            cases
                .iter()
                .find(|case| case["name"] == format!("{kind}_nearest_{mode}_shifted"))
                .unwrap()
        };
        let (mut error, mut power) = (0.0, 0.0);
        for (a, b) in case("nearest")["rows"]
            .as_array()
            .unwrap()
            .iter()
            .zip(case("linear")["rows"].as_array().unwrap())
        {
            for (a, b) in a["data"]
                .as_array()
                .unwrap()
                .iter()
                .zip(b["data"].as_array().unwrap())
            {
                for i in 0..2 {
                    let (a, b) = (a[i].as_f64().unwrap(), b[i].as_f64().unwrap());
                    error += (a - b).powi(2);
                    power += b.powi(2);
                }
            }
        }
        assert!(
            (error / power).sqrt() > 2e-3,
            "{kind} frequency discriminator"
        );
    }
    for case in cases {
        assert_eq!(
            support::run(case, 1),
            support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}
