// SPDX-License-Identifier: LGPL-3.0-or-later
//! Supplied baseline/error tables applied to synthetic DATA (SIM-16).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
#[allow(
    dead_code,
    reason = "reuse independent inputs and complete MS comparison"
)]
mod support;

use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

#[test]
fn supplied_baseline_tables_match_casa() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_baseline_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    for case in fixture["cases"].as_array().unwrap() {
        assert_eq!(
            support::run(case, 1),
            support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}

#[test]
fn baseline_inverse_scales_generated_additive_noise_only() {
    use casa_tables::{Table, TableOptions};
    use casa_types::{ArrayValue, Complex32, ScalarValue};
    let directory = tempfile::tempdir().unwrap();
    let case = serde_json::json!({"kind": "M Mueller", "interp": "linear",
        "input_fixture": "sm_baseline/m_mueller"});
    let table = Table::open(TableOptions::new(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_baseline/m_mueller"),
    ))
    .unwrap();
    for on_the_fly in [true, false] {
        let output = |correct: bool| {
            let root = directory.path().join(format!("{on_the_fly}-{correct}"));
            std::fs::create_dir(&root).unwrap();
            let (path, mut sm, spec) = support::prepared(&root, &case, 1);
            sm.set_noise(&casa_simulation::NoiseSpec {
                simple_noise_jy: 0.25,
                on_the_fly,
                ..Default::default()
            })
            .unwrap();
            if correct {
                sm.set_apply(&spec).unwrap();
            }
            sm.corrupt().unwrap();
            sm.close().unwrap();
            predict_support::read_main(&path).0
        };
        let plain = output(false);
        let corrected = output(true);
        for (plain, corrected) in plain.iter().zip(&corrected) {
            let row = (0..table.row_count()).find(|&row| {
                ["ANTENNA1", "ANTENNA2"]
                    .into_iter()
                    .zip(plain.antennas)
                    .all(|(name, antenna)| {
                        matches!(table.cell_accessor(row, name).unwrap().scalar().unwrap(),
                        ScalarValue::Int32(value) if *value == antenna as i32)
                    })
            });
            let mut gain = [Complex32::new(1., 0.); 2];
            if let Some(row) = row {
                let ArrayValue::Complex32(values) =
                    table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
                else {
                    panic!("CPARAM")
                };
                let ArrayValue::Bool(flags) =
                    table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
                else {
                    panic!("FLAG")
                };
                for receptor in 0..2 {
                    if !flags[[receptor, 0]] && values[[receptor, 0]].norm_sqr() > 0. {
                        gain[receptor] = values[[receptor, 0]];
                    }
                }
            }
            for (sample, (&plain, &corrected)) in plain.data.iter().zip(&corrected.data).enumerate()
            {
                let model = Complex32::new(3., 4.);
                let noise = plain - model;
                let expected = model
                    + match sample % 4 {
                        0 => noise / gain[0],
                        3 => noise / gain[1],
                        _ => Complex32::new(0., 0.),
                    };
                assert!(
                    (corrected - expected).norm() < 2e-6,
                    "only additive noise takes M inverse"
                );
            }
        }
    }
}
