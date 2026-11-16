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

#[test]
fn malformed_scalar_g_tables_raise_as_casa_does() {
    use casa_calibration::{
        SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
    };
    use casa_ms::MeasurementSet;
    use casa_simulation::{ApplySpec, CasaOutcome, Simulator, SimulatorOptions};
    use casa_types::Complex32;
    use casa_types::measures::{EpochRef, MEpoch};
    use predict_support::{ANTENNAS, REFERENCE_MJD, resources, scan, session};
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_scalar_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    let refusals = fixture["refusals"].as_array().unwrap();
    assert_eq!(refusals.len(), 2);
    for case in refusals {
        assert_eq!(case["outcome"], "raises");
        assert!(case["error"].as_str().unwrap().contains("shape"));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("malformed.ms");
        let mut sm = session(&path, "ALMA", "RR RL LR LL", resources(1, 2 << 30));
        sm.observe(&scan("target", 0.0, 20.0)).unwrap();
        sm.observe(&scan("offset", 20.0, 40.0)).unwrap();
        sm.close().unwrap();
        input::prepare(&path);
        let table = directory.path().join("malformed.cal");
        let first = case["calibration"]["first_time_s"].as_f64().unwrap();
        let last = case["calibration"]["last_time_s"].as_f64().unwrap();
        let rows = [first, last]
            .into_iter()
            .enumerate()
            .flat_map(|(field, time_s)| {
                (0..ANTENNAS).map(move |antenna| {
                    let elapsed = time_s - first;
                    SimulatedSolution {
                        time_s,
                        field_id: field as i32,
                        spectral_window_id: 0,
                        antenna: antenna as i32,
                        values: SolutionValues::Channels(vec![Complex32::new(
                            (1.1 + 0.05 * antenna as f64 + 0.002 * elapsed) as f32,
                            (0.2 + 0.003 * elapsed) as f32,
                        )]),
                    }
                })
            });
        write_simulated_jones_table(
            &MeasurementSet::open(&path).unwrap(),
            &table,
            SimulatedJones::Gain,
            rows,
            None,
        )
        .unwrap();
        let mut sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: casa_test_support::deterministic_measures_provider(),
                resources: resources(1, 2 << 30),
                now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        sm.set_apply(&ApplySpec {
            table: table.to_string_lossy().into_owned(),
            kind: String::new(),
            interval_s: 0.0,
            interp: case["interp"].as_str().unwrap().to_string(),
            calwt: false,
            spwmap: vec![-1],
            opacity: 0.0,
        })
        .unwrap();
        let error = sm.corrupt().expect_err("malformed G table");
        assert_eq!(error.outcome(), CasaOutcome::Raises, "{error}");
        sm.close().unwrap();
        for row in predict_support::read_main(&path).0 {
            assert!(
                row.data
                    .iter()
                    .all(|value| *value == Complex32::new(3.0, 4.0))
            );
        }
    }
}
