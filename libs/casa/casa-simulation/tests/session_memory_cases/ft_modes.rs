// SPDX-License-Identifier: LGPL-3.0-or-later
//! BOX kernel ownership and combined interferometer/total-power preparation.
use super::*;

#[test]
fn new_ft_modes_hold_their_preparation_and_retained_charges() {
    if !alone("ft_modes::new_ft_modes_hold_their_preparation_and_retained_charges") {
        return;
    }
    let policy = resources(BUDGET_320);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for n in [96, 768] {
        let model = directory.path().join(format!("{n}.image"));
        write_model_of(&model, 32, n);
        for (machine, grid) in [("ft", "box"), ("both", "sf"), ("both", "gjinc")] {
            let options = PredictionOptions {
                ftmachine: machine.into(),
                grid_function: grid.into(),
                ..PredictionOptions::default()
            };
            let ms = directory.path().join(format!("{machine}-{grid}-{n}.ms"));
            predict_through(&options, (&ms, &model, &measures), BUDGET_320, &[12.]);
            for _ in 0..MEASURED_RUNS {
                let result =
                    predict_through(&options, (&ms, &model, &measures), BUDGET_320, &[12.]);
                println!("{machine}/{grid}, {n} squared: {result:?}");
                assert!(result.beyond <= ALLOWANCE_BYTES as i64);
                assert!(result.during.heap <= result.during.admitted + ALLOWANCE_BYTES);
                assert!(result.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            }
        }
    }
}
