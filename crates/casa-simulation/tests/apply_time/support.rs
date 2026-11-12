// SPDX-License-Identifier: LGPL-3.0-or-later
//! Explicit synthetic CPARAM inputs, independent of interpolation implementation.
use super::{input, predict_support, support};
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_ms::MeasurementSet;
use casa_simulation::{ApplySpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex64;
use serde_json::Value as Json;
use std::path::{Path, PathBuf};

fn parameters(
    kind: &str,
    antenna: usize,
    index: usize,
    channels: usize,
    analytic: bool,
) -> Vec<Complex32> {
    let leakage = matches!(kind, "D" | "Df");
    let base = support::receptor_values_f64(if leakage { "D" } else { "G" }, antenna, 0.0);
    let factors = [
        Complex64::new(1., 0.),
        Complex64::new(1.1, 0.7),
        Complex64::new(-0.6, 1.5),
        Complex64::new(-1.4, -0.3),
    ];
    let offsets = [
        Complex64::new(0., 0.),
        Complex64::new(0.06, 0.04),
        Complex64::new(-0.16, 0.10),
        Complex64::new(0.24, -0.08),
    ];
    (0..channels)
        .flat_map(|channel| {
            (0..if matches!(kind, "T" | "Tf") { 1 } else { 2 }).map(move |receptor| {
                let base = base[receptor];
                let value = if analytic {
                    Complex64::new([1., 2., 0.5, 1.25][index], 0.)
                } else if leakage {
                    base + offsets[index] + 0.01 * channel as f64
                } else {
                    base * factors[index]
                        * (1. + 0.05 * channel as f64)
                        * Complex64::from_polar(1., 0.03 * channel as f64)
                };
                Complex32::new(value.re as f32, value.im as f32)
            })
        })
        .collect()
}

fn table(path: &Path, output: &Path, case: &Json) {
    let ms = MeasurementSet::open(path).unwrap();
    let kind = case["kind"].as_str().unwrap();
    let times = case["calibration"]["times_s"].as_array().unwrap();
    let original = case["calibration"]["original_times_s"].as_array().unwrap();
    let shifted = case["probe"] != "short2_exact";
    let rows = times.iter().flat_map(|time| {
        (0..predict_support::ANTENNAS).map(move |antenna| SimulatedSolution {
            time_s: time.as_f64().unwrap(),
            field_id: 0,
            spectral_window_id: 0,
            antenna: antenna as i32,
            values: if matches!(kind, "B" | "Tf" | "Df") {
                SolutionValues::Channels(vec![Complex32::new(1., 0.); 4])
            } else {
                SolutionValues::Receptors([Complex32::new(1., 0.); 2])
            },
        })
    });
    let writer_kind = if matches!(kind, "B" | "Tf" | "Df") {
        SimulatedJones::Troposphere
    } else {
        SimulatedJones::Gain
    };
    write_simulated_jones_table(&ms, output, writer_kind, rows).unwrap();
    let mut tb = Table::open(TableOptions::new(output)).unwrap();
    let receptors = case["calibration"]["parameters"].as_u64().unwrap() as usize;
    let channels = case["calibration"]["channels"].as_u64().unwrap() as usize;
    for row in 0..tb.row_count() {
        let ScalarValue::Int32(antenna) =
            tb.cell_accessor(row, "ANTENNA1").unwrap().scalar().unwrap()
        else {
            panic!("antenna")
        };
        let ScalarValue::Float64(time) = tb.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("time")
        };
        let (antenna, time) = (*antenna as usize, *time);
        let index = original
            .iter()
            .position(|v| v.as_f64().unwrap() == time + if shifted { 5. } else { 0. })
            .unwrap();
        let values = parameters(kind, antenna, index, channels, case["probe"] == "analytic");
        let shape = IxDyn(&[receptors, channels]);
        let mut flags = vec![false; receptors * channels];
        if case["probe"] == "flag" && antenna == 0 && index == 0 {
            flags[if channels > 1 { receptors } else { 0 }] = true;
        }
        for (column, value) in [
            (
                "CPARAM",
                ArrayValue::Complex32(ArrayD::from_shape_vec(shape.clone().f(), values).unwrap()),
            ),
            (
                "FLAG",
                ArrayValue::Bool(ArrayD::from_shape_vec(shape.clone().f(), flags).unwrap()),
            ),
            (
                "PARAMERR",
                ArrayValue::Float32(ArrayD::zeros(shape.clone().f())),
            ),
            ("SNR", ArrayValue::Float32(ArrayD::ones(shape.f()))),
        ] {
            tb.cell_accessor_mut(row, column)
                .unwrap()
                .set(Value::Array(value))
                .unwrap();
        }
        tb.cell_accessor_mut(row, "INTERVAL")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Float64(0.)))
            .unwrap();
    }
    let mut info = tb.info().clone();
    info.sub_type = format!("{kind} Jones");
    tb.set_info(info);
    tb.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String(format!("{kind} Jones"))),
    );
    tb.save(TableOptions::new(output)).unwrap();
}

pub(super) fn prepared(
    directory: &Path,
    case: &Json,
    workers: usize,
) -> (PathBuf, Simulator, ApplySpec) {
    let path = directory.join("applied.ms");
    let mut sm = predict_support::session(
        &path,
        "ALMA",
        "XX XY YX YY",
        predict_support::resources(workers, 2 << 30),
    );
    for (source, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
        let mut scan = predict_support::scan(source, start, stop);
        scan.add_observation = source == "offset"
            && case["segment"] == "split"
            && case["mode"].as_str().unwrap().contains("perobs");
        sm.observe(&scan).unwrap();
    }
    sm.close().unwrap();
    input::prepare(&path);
    let calibration = directory.join("terms.cal");
    table(&path, &calibration, case);
    context(&path, &calibration, case);
    let sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(workers, 2 << 30),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    let spec = ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.,
        interp: case["interp"].as_str().unwrap().to_string(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.,
    };
    (path, sm, spec)
}

fn context(path: &Path, calibration: &Path, case: &Json) {
    let mode = case["mode"].as_str().unwrap();
    let segment = case["segment"].as_str().unwrap_or("");
    let split = segment == "split" && mode.contains("perobs");
    if split {
        for root in [path, calibration] {
            let observation = root.join("OBSERVATION");
            assert_eq!(Table::rows_on_disk(&observation).unwrap(), 2);
        }
        let mut table = Table::open(TableOptions::new(path)).unwrap();
        for row in 0..table.row_count() {
            let value = table
                .cell_accessor(row, "FIELD_ID")
                .unwrap()
                .scalar()
                .unwrap()
                .clone();
            table
                .cell_accessor_mut(row, "OBSERVATION_ID")
                .unwrap()
                .set(Value::Scalar(value))
                .unwrap();
        }
        table.save(TableOptions::new(path)).unwrap();
    }
    if case["segment"].is_string() {
        let first = case["calibration"]["times_s"][0].as_f64().unwrap();
        let mut table = Table::open(TableOptions::new(calibration)).unwrap();
        for row in 0..table.row_count() {
            let ScalarValue::Float64(time) =
                table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
            else {
                panic!("time")
            };
            let later = *time > first + 15.;
            if mode.contains("perscan") && segment != "unset" {
                let scan = if later {
                    if segment == "missing" { 3 } else { 2 }
                } else {
                    1
                };
                table
                    .cell_accessor_mut(row, "SCAN_NUMBER")
                    .unwrap()
                    .set(Value::Scalar(ScalarValue::Int32(scan)))
                    .unwrap();
            }
            if mode.contains("perobs") && matches!(segment, "split" | "mismatch") {
                let mut observation = i32::from(later);
                if mode.contains("perscan") {
                    observation = 1 - observation;
                }
                table
                    .cell_accessor_mut(row, "OBSERVATION_ID")
                    .unwrap()
                    .set(Value::Scalar(ScalarValue::Int32(observation)))
                    .unwrap();
            }
        }
        table.save(TableOptions::new(calibration)).unwrap();
    }
    if mode.contains("PD") && matches!(case["kind"].as_str().unwrap(), "G" | "D" | "T") {
        let spectral = calibration.join("SPECTRAL_WINDOW");
        let mut table = Table::open(TableOptions::new(&spectral)).unwrap();
        table
            .cell_accessor_mut(0, "CHAN_FREQ")
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(ArrayD::from_elem(
                IxDyn(&[1]),
                100e9,
            ))))
            .unwrap();
        table
            .cell_accessor_mut(0, "REF_FREQUENCY")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Float64(100e9)))
            .unwrap();
        table.save(TableOptions::new(&spectral)).unwrap();
    }
}

pub(super) fn run(case: &Json, workers: usize) -> Vec<Vec<Complex32>> {
    let export = std::env::var_os("CASA_RS_APPLY_TIME_OUT");
    let directory = if let Some(root) = &export {
        std::fs::create_dir_all(root).unwrap();
        tempfile::tempdir_in(root).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let (path, mut sm, mut spec) = prepared(directory.path(), case, workers);
    if let Some(root) = std::env::var_os("CASA_RS_APPLY_CASA_TABLES") {
        assert!(export.is_none());
        // Spline's oracle directory contains sampled controls; its _nearest
        // sibling retains the original knots for native interpolation.
        let name = format!(
            "{}{}",
            case["name"].as_str().unwrap(),
            if case["mode"] == "spline" {
                "_nearest"
            } else {
                ""
            }
        );
        spec.table = PathBuf::from(root)
            .join(name)
            .join("terms.cal")
            .to_string_lossy()
            .into_owned();
    }
    sm.set_apply(&spec).unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    let rows = support::compare(&path, case);
    if export.is_some() {
        std::fs::write(
            directory.path().join("case.json"),
            serde_json::json!({"name":case["name"],"workers":workers}).to_string(),
        )
        .unwrap();
        let _kept = directory.keep();
    }
    rows
}
