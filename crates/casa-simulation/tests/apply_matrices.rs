// SPDX-License-Identifier: LGPL-3.0-or-later
//! Full polarized table errors imposed on simulated visibilities, against CASA.
#[path = "apply_parameters/input.rs"]
mod input;
#[path = "apply_matrices/support.rs"]
mod matrices;
mod predict_support;
#[path = "apply_parameters/support.rs"]
#[allow(dead_code, reason = "reuse the complete MS comparator")]
mod support;

use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

#[test]
fn full_polarized_table_errors_match_casa() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_matrices_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 5);
    for case in cases {
        assert_eq!(case["outcome"], "returned");
        assert_eq!(
            matrices::run(case, 1),
            matrices::run(case, 3),
            "{}",
            case["name"]
        );
    }
}
