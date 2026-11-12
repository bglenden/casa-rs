// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared independent G/D/T table input and complete MS comparisons.

#[path = "frequency.rs"]
mod frequency;

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
    receptor_values_f64(kind, antenna, elapsed)
        .map(|value| Complex32::new(value.re as f32, value.im as f32))
}

pub(super) fn receptor_values_f64(
    kind: &str,
    antenna: usize,
    elapsed: f64,
) -> [num_complex::Complex64; 2] {
    let antenna = antenna as f64;
    let values = if kind == "G" || kind == "T" {
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
    values.map(|(real, imaginary)| num_complex::Complex64::new(real, imaginary))
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
                values: if kind == "T" {
                    SolutionValues::Channels(vec![
                        receptor_values(kind, antenna, time_s - times[0])[0],
                    ])
                } else {
                    SolutionValues::Receptors(receptor_values(kind, antenna, time_s - times[0]))
                },
            })
    });
    let writer_kind = if frequency::is_frequency(kind) {
        SimulatedJones::Troposphere
    } else if kind != "D" {
        SimulatedJones::Gain
    } else {
        SimulatedJones::Leakage
    };
    write_simulated_jones_table(&ms, output, writer_kind, rows).unwrap();
    if frequency::is_frequency(kind) {
        frequency::prepare(output, case);
        return;
    }
    if kind == "T" {
        use casa_tables::{Table, TableOptions};
        use casa_types::Value;
        let mut table = Table::open(TableOptions::new(output)).unwrap();
        let mut info = table.info().clone();
        info.sub_type = "T Jones".to_string();
        table.set_info(info);
        table.keywords_mut().upsert(
            "VisCal",
            Value::Scalar(ScalarValue::String("T Jones".to_string())),
        );
        table.save(TableOptions::new(output)).unwrap();
    }
    if let Some(probe) = case["probe"]
        .as_str()
        .filter(|probe| probe.starts_with("flag"))
    {
        flag_solutions(output, probe, times);
    }
}

pub(super) fn leakage_fixture(path: &Path, output: &Path) {
    let ms = MeasurementSet::open(path).unwrap();
    let time = |row| match ms
        .main_table()
        .cell_accessor(row, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
    {
        ScalarValue::Float64(value) => *value,
        _ => panic!("TIME"),
    };
    table(
        path,
        output,
        &serde_json::json!({
            "kind": "D", "calibration": {
                "first_time_s": time(0), "last_time_s": time(ms.row_count()-1), "rows": 2*ANTENNAS,
            },
        }),
    );
}

pub(super) fn run(case: &Value, workers: usize) -> Vec<Vec<Complex32>> {
    let export = std::env::var_os(if frequency::is_frequency(case["kind"].as_str().unwrap()) {
        "CASA_RS_APPLY_FREQUENCY_OUT"
    } else if case["kind"] == "T" {
        "CASA_RS_APPLY_SCALAR_OUT"
    } else if case["probe"].is_string() {
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
    let (path, mut sm, mut spec) = prepared(directory.path(), case, workers);
    if let Some(root) = std::env::var_os("CASA_RS_APPLY_CASA_TABLES")
        .filter(|_| frequency::is_frequency(case["kind"].as_str().unwrap()))
    {
        assert!(
            export.is_none(),
            "C++ input-table comparison is separate from native-output export"
        );
        spec.table = std::path::PathBuf::from(root)
            .join(case["name"].as_str().unwrap())
            .join("terms.cal")
            .to_string_lossy()
            .into_owned();
    }
    sm.set_apply(&spec).unwrap();
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

pub(super) fn compare(path: &Path, case: &Value) -> Vec<Vec<Complex32>> {
    let (actual, correlations) = read_main(path);
    let expected = case["rows"].as_array().unwrap();
    assert_eq!(
        correlations,
        case["stokes"]
            .as_str()
            .map(|stokes| stokes.split_whitespace().count())
            .unwrap_or(4)
    );
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
        for (column, key) in [("OBSERVATION_ID", "observation"), ("SCAN_NUMBER", "scan")] {
            if let Some(reference) = expected[key].as_i64() {
                let ScalarValue::Int32(value) =
                    table.cell_accessor(row, column).unwrap().scalar().unwrap()
                else {
                    panic!("context type")
                };
                assert_eq!(i64::from(*value), reference, "{column}");
            }
        }
        for (column, key) in [("WEIGHT", "weight"), ("SIGMA", "sigma")] {
            let ArrayValue::Float32(values) =
                table.cell_accessor(row, column).unwrap().array().unwrap()
            else {
                panic!("float array type")
            };
            assert_eq!(values.len(), correlations);
            for (value, reference) in values.iter().zip(expected[key].as_array().unwrap()) {
                assert_eq!(*value, reference.as_f64().unwrap() as f32, "{column}");
            }
        }
        let ArrayValue::Bool(flags) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("FLAG type")
        };
        for channel in 0..4 {
            for correlation in 0..correlations {
                assert_eq!(
                    flags[[correlation, channel]],
                    expected["flags"][channel * correlations + correlation]
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
                assert_eq!(
                    values[[sample % correlations, sample / correlations]],
                    expected,
                    "{column}"
                );
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
        let ArrayValue::Bool(existing) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("flags")
        };
        let receptors = existing.len();
        let flags = if receptors == 1 {
            vec![true]
        } else if probe == "flag_all_both" {
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
                    ArrayD::from_shape_vec(IxDyn(&[receptors, 1]).f(), flags).unwrap(),
                )),
            )
            .unwrap();
    }
    table.save(TableOptions::new(path)).unwrap();
}

/// Shared independent input for successful applications and refusal atomicity.
pub(super) fn prepared(
    directory: &Path,
    case: &Value,
    workers: usize,
) -> (std::path::PathBuf, Simulator, ApplySpec) {
    let path = directory.join("applied.ms");
    let mut sm = session(
        &path,
        "ALMA",
        case["stokes"].as_str().unwrap_or("XX XY YX YY"),
        resources(workers, 2 << 30),
    );
    let second_window = case["data_window"] == 1;
    if second_window {
        use super::predict_support::{CHANNEL_HZ, FREQUENCY_HZ};
        use casa_types::measures::frequency::FrequencyRef;
        sm.set_spectral_window(&casa_simulation::SpectralWindowSpec {
            name: "second".to_string(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: CHANNEL_HZ,
            resolution_hz: CHANNEL_HZ,
            frame: FrequencyRef::TOPO,
            channel_count: 4,
            stokes: "XX XY YX YY".to_string(),
        })
        .unwrap();
    }
    for (source, start, stop) in [("target", 0.0, 20.0), ("offset", 20.0, 40.0)] {
        let mut spec = scan(source, start, stop);
        if second_window {
            spec.spectral_window = "second".to_string();
        }
        sm.observe(&spec).unwrap();
    }
    sm.close().unwrap();
    input::prepare(&path);
    let calibration = directory.join("terms.cal");
    table(&path, &calibration, case);
    let sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: resources(workers, 2 << 30),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    let spec = ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: case["options"]["type"].as_str().unwrap_or("").to_string(),
        interval_s: case["options"]["t"].as_f64().unwrap_or(0.0),
        interp: case["interp"].as_str().unwrap().to_string(),
        calwt: case["options"]["calwt"].as_bool().unwrap_or(false),
        spwmap: case["options"]["spwmap"]
            .as_array()
            .map(|values| values.iter().map(|value| value.as_i64().unwrap()).collect())
            .unwrap_or_else(|| vec![-1]),
        opacity: case["options"]["opacity"].as_f64().unwrap_or(0.0),
    };
    (path, sm, spec)
}
