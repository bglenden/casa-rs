// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected noise has CASA's support and analytic moments; random sequences
//! differ between the implementations, so do not compare draws pointwise.

use casa_simulation::BackendRequest;
use casa_types::Complex32;
use serde_json::{Value, json};

use super::{fixture, opened_case};

fn complex(value: &Value) -> Complex32 {
    Complex32::new(
        value[0].as_f64().unwrap() as f32,
        value[1].as_f64().unwrap() as f32,
    )
}

fn moments(values: &[f64], label: &str) {
    assert!(values.len() >= 64, "enough independent samples: {label}");
    assert!(values.iter().all(|value| value.is_finite()));
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / n;
    println!(
        "{label}: n={} mean={mean:e} variance={variance:e}",
        values.len()
    );
    assert!(mean.abs() <= 5.0 * 0.1 / n.sqrt(), "{label}: mean {mean}");
    assert!(
        (variance - 0.01).abs() <= 5.0 * 0.01 * (2.0 / n).sqrt(),
        "{label}: variance {variance}"
    );
}

fn baseline(case: &Value, all_replace: &Value) -> Value {
    let mut baseline = case.clone();
    baseline["name"] = json!(format!("{}_without_noise", case["name"].as_str().unwrap()));
    baseline["noise"] = json!(true);
    baseline["steps"]
        .as_array_mut()
        .unwrap()
        .retain(|step| step["op"] != "corrupt");
    if case["name"] == "selected_prediction_noise" {
        baseline["rows"] = all_replace["rows"].clone();
    } else {
        for row in baseline["rows"].as_array_mut().unwrap() {
            row["data"] = json!(vec![[3.0, 4.0]; 8]);
            row["model_data"] = json!(vec![[7.0, -2.0]; 8]);
            row["corrected_data"] = json!(vec![[-5.0, 6.0]; 8]);
        }
    }
    baseline
}

#[test]
fn selected_noise_preserves_other_rows_and_matches_casa_moments() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let all_replace = cases
        .iter()
        .find(|case| case["name"] == "all_replace")
        .unwrap();
    for case in cases.iter().filter(|case| case["noise"] == true) {
        let baseline = baseline(case, all_replace);
        let clean = opened_case(&baseline, 1, BackendRequest::Cpu);
        let actual = opened_case(case, 1, BackendRequest::Cpu);
        assert_eq!(actual, opened_case(case, 3, BackendRequest::Cpu), "workers");
        let mut native = [Vec::new(), Vec::new(), Vec::new()];
        let mut oracle = [Vec::new(), Vec::new(), Vec::new()];
        let mut untouched = 0;
        for (row, (data, clean)) in actual.iter().zip(&clean).enumerate() {
            let reference = &case["rows"][row];
            let before = &baseline["rows"][row];
            let selected = reference["data"] != before["data"];
            if !selected {
                assert_eq!(data, clean, "unselected noise row {row}");
                untouched += 1;
                continue;
            }
            let auto = reference["antennas"][0] == reference["antennas"][1];
            for sample in 0..data.len() {
                let residual = data[sample] - clean[sample];
                let expected =
                    complex(&reference["data"][sample]) - complex(&before["data"][sample]);
                if auto {
                    assert_eq!(residual.im, 0.0, "native auto imaginary");
                    assert_eq!(expected.im, 0.0, "CASA auto imaginary");
                    native[2].push(f64::from(residual.re));
                    oracle[2].push(f64::from(expected.re));
                } else {
                    native[0].push(f64::from(residual.re));
                    native[1].push(f64::from(residual.im));
                    oracle[0].push(f64::from(expected.re));
                    oracle[1].push(f64::from(expected.im));
                }
            }
        }
        assert!(untouched > 0, "every case tests an actual subset");
        for (index, kind) in ["cross real", "cross imaginary", "auto real"]
            .iter()
            .enumerate()
        {
            if native[index].is_empty() {
                assert_eq!(case["name"], "query_noise");
                assert_eq!(index, 2, "the query omits only autocorrelations");
                continue;
            }
            moments(&native[index], &format!("{} native {kind}", case["name"]));
            moments(&oracle[index], &format!("{} CASA {kind}", case["name"]));
        }
    }
}
