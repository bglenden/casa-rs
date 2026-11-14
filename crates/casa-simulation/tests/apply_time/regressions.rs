// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent controls for context collisions, wrapped phase and invalid shapes.
use super::{fixture, predict_support, time_support};
use casa_calibration::{GencalRequest, GencalType, gencal};
use casa_simulation::{Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
use std::path::Path;

fn reopen(path: &Path, workers: usize) -> Simulator {
    Simulator::open_from_ms(
        path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(workers, 2 << 30),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap()
}

fn number(table: &Table, row: usize, column: &str) -> f64 {
    match table.cell_accessor(row, column).unwrap().scalar().unwrap() {
        ScalarValue::Float64(value) => *value,
        ScalarValue::Int32(value) => f64::from(*value),
        other => panic!("unexpected {column}: {other:?}"),
    }
}

fn set(table: &mut Table, row: usize, column: &str, value: impl Into<Value>) {
    table
        .cell_accessor_mut(row, column)
        .unwrap()
        .set(value.into())
        .unwrap();
}

fn analytic_case() -> serde_json::Value {
    fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "G_spline_analytic")
        .unwrap()
        .clone()
}

#[test]
fn simultaneous_scans_and_observations_keep_their_own_gains() {
    for mode in ["nearestperscan", "nearestperobs"] {
        for workers in [1, 3] {
            let root = tempfile::tempdir().unwrap();
            let mut case = analytic_case();
            case["segment"] = "split".into();
            case["mode"] = mode.into();
            case["interp"] = format!("{mode},nearest").into();
            let (path, sm, spec) = time_support::prepared(root.path(), &case, workers);
            drop(sm);
            let first = case["calibration"]["times_s"][0].as_f64().unwrap();
            for (file, calibration) in [(path.as_path(), false), (Path::new(&spec.table), true)] {
                let mut table = Table::open(TableOptions::new(file)).unwrap();
                for row in 0..table.row_count() {
                    let time = number(&table, row, "TIME");
                    let later = time >= first + 20.;
                    if later {
                        set(
                            &mut table,
                            row,
                            "TIME",
                            Value::Scalar(ScalarValue::Float64(time - 20.)),
                        );
                    }
                    if calibration {
                        let gain = Complex32::new(if later { 3. } else { 2. }, 0.);
                        set(
                            &mut table,
                            row,
                            "CPARAM",
                            Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                                IxDyn(&[2, 1]),
                                gain,
                            ))),
                        );
                    }
                }
                table.save(TableOptions::new(file)).unwrap();
            }
            let mut sm = reopen(&path, workers);
            sm.set_apply(&spec).unwrap();
            sm.corrupt().unwrap();
            sm.close().unwrap();
            for row in predict_support::read_main(&path).0 {
                let factor = if row.field == 0 { 4. } else { 9. };
                for value in row.data {
                    assert!(
                        (value - Complex32::new(3., 4.) * factor).norm() < 1e-5,
                        "{mode}, field {}: {value}",
                        row.field
                    );
                }
            }
        }
    }
}

#[test]
fn spline_phase_delay_counts_a_wrap_once() {
    let root = tempfile::tempdir().unwrap();
    let case = analytic_case();
    let (path, sm, mut spec) = time_support::prepared(root.path(), &case, 1);
    drop(sm);
    let first = case["calibration"]["times_s"][0].as_f64().unwrap();
    let calibration = Path::new(&spec.table);
    let mut table = Table::open(TableOptions::new(calibration)).unwrap();
    for row in 0..table.row_count() {
        // A continuous straight phase [0, 2, 4, 6] crosses the principal cut.
        let phase = if number(&table, row, "ANTENNA1") == 0. {
            ((number(&table, row, "TIME") - first) / 5.) as f32
        } else {
            0.
        };
        set(
            &mut table,
            row,
            "CPARAM",
            Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                IxDyn(&[2, 1]),
                Complex32::from_polar(1., phase),
            ))),
        );
    }
    table.save(TableOptions::new(calibration)).unwrap();
    let spectral = calibration.join("SPECTRAL_WINDOW");
    let mut table = Table::open(TableOptions::new(&spectral)).unwrap();
    let frequency = (predict_support::FREQUENCY_HZ + 1.5 * predict_support::CHANNEL_HZ) / 1.5;
    set(
        &mut table,
        0,
        "CHAN_FREQ",
        Value::Array(ArrayValue::Float64(ArrayD::from_elem(
            IxDyn(&[1]),
            frequency,
        ))),
    );
    table.save(TableOptions::new(&spectral)).unwrap();
    let table = Table::open(TableOptions::new(&path)).unwrap();
    let times: Vec<_> = (0..table.row_count())
        .map(|row| number(&table, row, "TIME"))
        .collect();
    drop(table);
    spec.interp = "splinePD,nearest".into();
    let mut sm = reopen(&path, 1);
    sm.set_apply(&spec).unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    for (row, time) in predict_support::read_main(&path).0.into_iter().zip(times) {
        let phase = ((time - first).clamp(0., 30.) / 5. * 1.5) as f32;
        let difference = i32::from(row.antennas[0] == 0) - i32::from(row.antennas[1] == 0);
        let expected =
            Complex32::new(3., 4.) * Complex32::from_polar(1., phase * difference as f32);
        assert!(
            row.data
                .iter()
                .all(|value| (*value - expected).norm() < 1e-5),
            "time {time}, antennas {:?}, expected {expected}, got {:?}",
            row.antennas,
            row.data
        );
    }
}

#[test]
fn malformed_real_parameter_shapes_raise_without_changing_data() {
    for (values, flags) in [(vec![2, 1], vec![1, 1]), (vec![0, 1], vec![0, 1])] {
        let root = tempfile::tempdir().unwrap();
        let (path, sm, mut spec) = time_support::prepared(root.path(), &analytic_case(), 1);
        drop(sm);
        let calibration = root.path().join("malformed.cal");
        gencal(&GencalRequest {
            measurement_set: path.clone(),
            output_table: calibration.clone(),
            caltype: GencalType::Antpos,
            antenna: "0".into(),
            spw: String::new(),
            parameter: vec![0.; 3],
            gaincurve_table: None,
        })
        .unwrap();
        let mut table = Table::open(TableOptions::new(&calibration)).unwrap();
        for row in 0..table.row_count() {
            set(
                &mut table,
                row,
                "FPARAM",
                Value::Array(ArrayValue::Float32(ArrayD::zeros(IxDyn(&values)))),
            );
            set(
                &mut table,
                row,
                "FLAG",
                Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&flags), false))),
            );
        }
        table.save(TableOptions::new(&calibration)).unwrap();
        spec.table = calibration.to_string_lossy().into_owned();
        spec.interp = "spline".into();
        let mut sm = reopen(&path, 1);
        let error = sm
            .set_apply(&spec)
            .expect_err("invalid shape must be refused before indexing");
        assert_eq!(error.outcome(), casa_simulation::CasaOutcome::Raises);
        sm.close().unwrap();
        assert!(predict_support::read_main(&path).0.iter().all(|row| {
            row.data
                .iter()
                .all(|value| *value == Complex32::new(3., 4.))
        }));
    }
}
