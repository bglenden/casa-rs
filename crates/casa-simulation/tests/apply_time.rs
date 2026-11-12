// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time interpolation against independent CASA and dense-system spline controls.
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
mod support;
#[path = "apply_time/support.rs"]
mod time_support;

use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_time_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

#[test]
fn higher_time_interpolation_matches_independent_references() {
    let fixture = fixture();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 41);
    for case in cases.iter().filter(|case| case["refusal"] != true) {
        assert_eq!(case["outcome"], "returned");
        assert_eq!(
            time_support::run(case, 1),
            time_support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}

#[test]
fn short_time_axes_and_unknown_modes_raise_without_changing_data() {
    use casa_simulation::CasaOutcome;
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let refusals = cases
        .iter()
        .filter(|case| case["refusal"] == true)
        .collect::<Vec<_>>();
    assert_eq!(refusals.len(), 7);
    for case in refusals {
        assert_eq!(case["outcome"], "raises");
        let directory = tempfile::tempdir().unwrap();
        let (path, mut sm, spec) = time_support::prepared(directory.path(), case, 1);
        let error = if case["stage"] == "setapply" {
            sm.set_apply(&spec).expect_err("CASA refuses the mode")
        } else {
            sm.set_apply(&spec).unwrap();
            sm.corrupt().expect_err("CASA refuses a short cubic axis")
        };
        assert_eq!(
            error.outcome(),
            CasaOutcome::Raises,
            "{}: {error}",
            case["name"]
        );
        sm.close().unwrap();
        assert!(predict_support::read_main(&path).0.iter().all(|row| {
            row.data
                .iter()
                .all(|value| *value == num_complex::Complex32::new(3.0, 4.0))
        }));
    }
}
