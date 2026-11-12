// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared independent G/D table input and complete MS comparisons.

use super::input;
use super::predict_support::{ANTENNAS, REFERENCE_MJD, read_main, resources, scan, session};
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_ms::MeasurementSet;
use casa_simulation::{ApplySpec, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use serde_json::Value;
use std::path::Path;

fn receptor_values(kind: &str, antenna: usize, elapsed: f64) -> [Complex32; 2] {
    let antenna = antenna as f64;
    let values = if kind == "G" {
        [
            (
                1.1 + 0.05 * antenna + 0.002 * elapsed,
                0.2 + 0.003 * elapsed,
            ),
            (
                0.8 + 0.03 * antenna + 0.001 * elapsed,
                -0.1 - 0.002 * elapsed,
            ),
        ]
    } else {
        [
            (0.005 * (antenna + 1.0), 0.001 * elapsed),
            (-0.004 * (antenna + 1.0), 0.0005 * elapsed),
        ]
    };
    values.map(|(real, imaginary)| Complex32::new(real as f32, imaginary as f32))
}

fn table(path: &Path, output: &Path, case: &Value) {
    let ms = MeasurementSet::open(path).unwrap();
    let time = |row| {
        let ScalarValue::Float64(time) = ms
            .main_table()
            .cell_accessor(row, "TIME")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("TIME type")
        };
        *time
    };
    let times = [time(0), time(ms.row_count() - 1)];
    assert_eq!(
        times[0],
        case["calibration"]["first_time_s"].as_f64().unwrap()
    );
    assert_eq!(
        times[1],
        case["calibration"]["last_time_s"].as_f64().unwrap()
    );
    assert_eq!(
        case["calibration"]["rows"].as_u64().unwrap() as usize,
        2 * if case["probe"] == "missing_antenna" {
            ANTENNAS - 1
        } else {
            ANTENNAS
        }
    );
    let kind = case["kind"].as_str().unwrap();
    let rows = times.into_iter().enumerate().flat_map(|(field, time_s)| {
        (0..ANTENNAS)
            .filter(|&antenna| case["probe"] != "missing_antenna" || antenna != 7)
            .map(move |antenna| SimulatedSolution {
                time_s,
                field_id: field as i32,
                spectral_window_id: 0,
                antenna: antenna as i32,
                values: SolutionValues::Receptors(receptor_values(
                    kind,
                    antenna,
                    time_s - times[0],
                )),
            })
    });
    let kind = if kind == "G" {
        SimulatedJones::Gain
    } else {
        SimulatedJones::Leakage
    };
    write_simulated_jones_table(&ms, output, kind, rows).unwrap();
    if let Some(probe) = case["probe"]
        .as_str()
        .filter(|probe| probe.starts_with("flag"))
    {
        flag_solutions(output, probe, times);
    }
}

pub(super) fn run(case: &Value, workers: usize) -> Vec<Vec<Complex32>> {
    let export = std::env::var_os(if case["probe"].is_string() {
        "CASA_RS_APPLY_FLAGS_OUT"
    } else {
        "CASA_RS_APPLY_PARAMETERS_OUT"
    });
    let directory = if let Some(root) = &export {
        std::fs::create_dir_all(root).unwrap();
        tempfile::tempdir_in(root).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let path = directory.path().join("applied.ms");
    let mut sm = session(&path, "ALMA", "XX XY YX YY", resources(workers, 2 << 30));
    sm.observe(&scan("target", 0.0, 20.0)).unwrap();
    sm.observe(&scan("offset", 20.0, 40.0)).unwrap();
    sm.close().unwrap();
    input::prepare(&path);
    let calibration = directory.path().join("terms.cal");
    table(&path, &calibration, case);
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: resources(workers, 2 << 30),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    sm.set_apply(&ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: case["options"]["t"].as_f64().unwrap_or(0.0),
        interp: case["interp"].as_str().unwrap().to_string(),
        calwt: case["options"]["calwt"].as_bool().unwrap_or(false),
        spwmap: vec![-1],
        opacity: case["options"]["opacity"].as_f64().unwrap_or(0.0),
    })
    .unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    let rows = compare(&path, case);
    if export.is_some() {
        std::fs::write(
            directory.path().join("case.json"),
            serde_json::json!({"name": case["name"], "workers": workers}).to_string(),
        )
        .unwrap();
        let _kept = directory.keep();
    }
    rows
}

fn compare(path: &Path, case: &Value) -> Vec<Vec<Complex32>> {
    let (actual, correlations) = read_main(path);
    let expected = case["rows"].as_array().unwrap();
    assert_eq!(correlations, 4);
    assert_eq!(actual.len(), expected.len());
    let ms = MeasurementSet::open(path).unwrap();
    let table = ms.main_table();
    let (mut error, mut power) = (0.0_f64, 0.0_f64);
    for (row, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(actual.field, expected["field"].as_u64().unwrap() as usize);
        assert_eq!(actual.flag_row, expected["flag_row"].as_bool().unwrap());
        assert_eq!(
            actual.antennas,
            [
                expected["antennas"][0].as_u64().unwrap() as u32,
                expected["antennas"][1].as_u64().unwrap() as u32
            ]
        );
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("TIME type")
        };
        assert_eq!(*time, expected["time_s"].as_f64().unwrap());
        for (column, key) in [("WEIGHT", "weight"), ("SIGMA", "sigma")] {
            let ArrayValue::Float32(values) =
                table.cell_accessor(row, column).unwrap().array().unwrap()
            else {
                panic!("float array type")
            };
            assert_eq!(values.len(), 4);
            for (value, reference) in values.iter().zip(expected[key].as_array().unwrap()) {
                assert_eq!(*value, reference.as_f64().unwrap() as f32, "{column}");
            }
        }
        let ArrayValue::Bool(flags) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("FLAG type")
        };
        for channel in 0..4 {
            for correlation in 0..4 {
                assert_eq!(
                    flags[[correlation, channel]],
                    expected["flags"][channel * 4 + correlation]
                        .as_bool()
                        .unwrap()
                );
            }
        }
        for (sample, (value, reference)) in actual
            .data
            .iter()
            .zip(expected["data"].as_array().unwrap())
            .enumerate()
        {
            assert!(value.is_finite());
            let reference = Complex32::new(
                reference[0].as_f64().unwrap() as f32,
                reference[1].as_f64().unwrap() as f32,
            );
            error += f64::from((*value - reference).norm_sqr());
            power += f64::from(reference.norm_sqr());
            for column in ["MODEL_DATA", "CORRECTED_DATA"] {
                let ArrayValue::Complex32(values) =
                    table.cell_accessor(row, column).unwrap().array().unwrap()
                else {
                    panic!("complex column type")
                };
                let expected = if column == "MODEL_DATA" {
                    let reference = &expected["model_data"][sample];
                    Complex32::new(
                        reference[0].as_f64().unwrap() as f32,
                        reference[1].as_f64().unwrap() as f32,
                    )
                } else {
                    assert_eq!(expected["corrected_data"][sample], expected["data"][sample]);
                    *value
                };
                assert_eq!(values[[sample % 4, sample / 4]], expected, "{column}");
            }
        }
    }
    let nrms = (error / power).sqrt();
    println!("{}: DATA NRMS {nrms:e}", case["name"]);
    assert!(power > 0.0 && nrms <= 1e-3);
    actual.into_iter().map(|row| row.data).collect()
}

fn flag_solutions(path: &Path, probe: &str, times: [f64; 2]) {
    use casa_tables::{Table, TableOptions};
    use casa_types::Value;
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    for row in 0..table.row_count() {
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("antenna type")
        };
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("time type")
        };
        if *antenna != 0
            || (probe.contains("first") && *time != times[0])
            || (probe.contains("last") && *time != times[1])
        {
            continue;
        }
        let flags = if probe == "flag_all_both" {
            vec![true, true]
        } else if probe.ends_with('y') {
            vec![false, true]
        } else {
            vec![true, false]
        };
        table
            .column_accessor_mut("FLAG")
            .unwrap()
            .set(
                row,
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), flags).unwrap(),
                )),
            )
            .unwrap();
    }
    table.save(TableOptions::new(path)).unwrap();
}
