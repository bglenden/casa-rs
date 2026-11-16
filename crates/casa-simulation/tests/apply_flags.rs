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
        if case["kind"] == "D" {
            assert_ne!(case["rows"], case["casa_unmodified_rows"]);
            // Analytic discriminator: zero D cannot add parallel hands to
            // this antenna's autocorrelation. CASA's two unit D values
            // add both parallel hands, tripling the 3+4j cross hands.
            let antenna = &case["calibration"]["bad_antenna"];
            let mut corrected_autocorrelations = 0;
            for (row, raw) in case["rows"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["casa_unmodified_rows"].as_array().unwrap())
            {
                if &row["antennas"][0] == antenna
                    && &row["antennas"][1] == antenna
                    && row["data"] != raw["data"]
                {
                    corrected_autocorrelations += 1;
                    for channel in 0..4 {
                        for correlation in [1, 2] {
                            assert_eq!(
                                row["data"][channel * 4 + correlation],
                                serde_json::json!([3.0, 4.0])
                            );
                            assert_eq!(
                                raw["data"][channel * 4 + correlation],
                                serde_json::json!([9.0, 12.0])
                            );
                        }
                    }
                }
            }
            assert!(corrected_autocorrelations > 0, "{}", case["name"]);
        }
        assert_eq!(
            support::run(case, 1),
            support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}
