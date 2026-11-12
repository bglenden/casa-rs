// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection and ordered DATA against independent CASA 6.7.6.14 (#1053).

#[path = "selection/input.rs"]
mod input;
#[path = "selection/noise.rs"]
mod noise;
mod pass_summary;
mod predict_support;

use std::io::Read;
use std::path::{Path, PathBuf};

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_ms::MeasurementSet;
use casa_simulation::{
    ApplySpec, BackendRequest, CasaOutcome, NoiseSpec, PredictSpec, SessionResources, Simulator,
    SimulatorOptions, SpectralWindowSpec,
};
use casa_types::Complex32;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, ScalarValue};
use flate2::read::GzDecoder;
use predict_support::{
    CHANNEL_HZ, FREQUENCY_HZ, N, read_main, scan, session, write_model, write_model_of,
};
use serde_json::Value;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_selection_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 51);
    fixture
}

fn resources(workers: usize, backend: BackendRequest) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: HostResources::detect().unwrap().metal,
        },
        ResourcePolicy::Explicit {
            workers,
            memory: 2 << 30,
        },
        backend,
    )
    .unwrap()
}

fn observed(path: &Path, workers: usize, backend: BackendRequest) -> Simulator {
    let mut sm = session(path, "ALMA", "XX YY", resources(workers, backend));
    sm.observe(&scan("target", 0.0, 20.0)).unwrap();
    sm.observe(&scan("offset", 20.0, 40.0)).unwrap();
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "band6b".to_string(),
        start_frequency_hz: FREQUENCY_HZ,
        channel_width_hz: CHANNEL_HZ,
        resolution_hz: CHANNEL_HZ,
        frame: FrequencyRef::TOPO,
        channel_count: 4,
        stokes: "XX YY".to_string(),
    })
    .unwrap();
    for (field, start, stop) in [("target", 40.0, 60.0), ("offset", 60.0, 80.0)] {
        let mut spec = scan(field, start, stop);
        spec.spectral_window = "band6b".to_string();
        sm.observe(&spec).unwrap();
    }
    sm
}

fn models(directory: &Path) -> [PathBuf; 2] {
    [
        write_model(
            directory,
            "a.image",
            &[vec![(N / 2, N / 2, 1.25), (N / 2 + 9, N / 2 - 4, 0.75)]],
        ),
        write_model_of(
            directory,
            "b.image",
            96,
            &(0..4)
                .map(|channel| vec![(48, 48, 0.5 + 0.25 * channel as f32), (60, 49, -0.1)])
                .collect::<Vec<_>>(),
        ),
    ]
}

fn ids(step: &Value, name: &str) -> Vec<i64> {
    step[name]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_i64().unwrap())
        .collect()
}

fn apply_steps(sm: &mut Simulator, case: &Value, images: &[PathBuf; 2], path: &Path) {
    for step in case["steps"].as_array().unwrap() {
        match step["op"].as_str().unwrap() {
            "setdata" => {
                let mut query = step["msselect"].as_str().unwrap().to_string();
                if query.contains("MID_TIME") {
                    let ms = MeasurementSet::open(path).unwrap();
                    let ScalarValue::Float64(time) = ms
                        .main_table()
                        .cell_accessor(0, "TIME")
                        .unwrap()
                        .scalar()
                        .unwrap()
                    else {
                        panic!("TIME type")
                    };
                    query = query.replace("MID_TIME", &(time + 10.0).to_string());
                }
                let result = sm.set_data(&ids(step, "spwid"), &ids(step, "fieldid"), &query);
                if step["expected_return"] == false {
                    assert_eq!(result.unwrap_err().outcome(), CasaOutcome::ReturnsFalse);
                } else {
                    result.unwrap();
                }
            }
            "predict" => {
                let point = images[0].parent().unwrap().join("point.cl");
                let components = step["components"].as_bool().unwrap();
                if components && !point.exists() {
                    predict_support::write_point_list(&point, predict_support::CENTRE_RAD, 0.25);
                }
                sm.predict(&PredictSpec {
                    images: step["images"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|name| match name.as_str().unwrap() {
                            "a" => images[0].clone(),
                            "b" => images[1].clone(),
                            name => panic!("model {name}"),
                        })
                        .collect(),
                    component_lists: if components { vec![point] } else { vec![] },
                    incremental: step["incremental"].as_bool().unwrap(),
                })
                .unwrap();
            }
            "observe" => {
                let seconds = |key: &str| {
                    step[key]
                        .as_str()
                        .unwrap()
                        .trim_end_matches('s')
                        .parse::<f64>()
                        .unwrap()
                        + 37.0
                };
                let mut spec = scan(
                    step["sourcename"].as_str().unwrap(),
                    seconds("starttime"),
                    seconds("stoptime"),
                );
                spec.spectral_window = step["spwname"].as_str().unwrap().to_string();
                sm.observe(&spec).unwrap();
            }
            "setseed" => sm.set_seed(step["seed"].as_i64().unwrap() as i32),
            "setnoise" => {
                assert_eq!(step["mode"], "simplenoise");
                assert_eq!(step["simplenoise"], "0.1Jy");
                sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
            }
            "fixedgain" => {
                let calibration = images[0].parent().unwrap().join("fixed.gcal");
                input::fixed_gains(
                    path,
                    &calibration,
                    step["receptor_gain"].as_f64().unwrap() as f32,
                );
                sm.reset();
                sm.set_apply(&ApplySpec {
                    table: calibration.to_string_lossy().into_owned(),
                    kind: String::new(),
                    interval_s: 0.0,
                    interp: "nearest".to_string(),
                    calwt: false,
                    spwmap: vec![-1],
                    opacity: 0.0,
                })
                .unwrap();
            }
            "corrupt" => sm.corrupt().unwrap(),
            operation => panic!("unexpected selection operation {operation}"),
        }
    }
}

fn fresh_case(case: &Value, workers: usize, backend: BackendRequest) -> Vec<Vec<Complex32>> {
    let directory = case_directory();
    let images = models(directory.path());
    let path = directory.path().join("selection.ms");
    let mut sm = observed(&path, workers, backend);
    let calculation_started = std::time::Instant::now();
    apply_steps(&mut sm, case, &images, &path);
    close(sm, case, backend);
    println!(
        "CALCULATION_TIME {} W{workers} {backend:?} {}",
        case["name"].as_str().unwrap(),
        calculation_started.elapsed().as_secs_f64()
    );
    let (actual, correlations) = read_main(&path);
    assert_eq!(correlations, 2);
    let expected = case["rows"].as_array().unwrap();
    assert_eq!(actual.len(), expected.len());
    let (mut error, mut power) = (0.0_f64, 0.0_f64);
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(
            actual.data.len(),
            expected["data"].as_array().unwrap().len()
        );
        assert_eq!(
            actual.antennas,
            [
                expected["antennas"][0].as_u64().unwrap() as u32,
                expected["antennas"][1].as_u64().unwrap() as u32
            ]
        );
        assert_eq!(actual.field, expected["field"].as_u64().unwrap() as usize);
        assert_eq!(actual.flag_row, expected["flag_row"].as_bool().unwrap());
        for (&value, reference) in actual.data.iter().zip(expected["data"].as_array().unwrap()) {
            let reference = Complex32::new(
                reference[0].as_f64().unwrap() as f32,
                reference[1].as_f64().unwrap() as f32,
            );
            assert!(value.re.is_finite() && value.im.is_finite());
            if reference == Complex32::new(0.0, 0.0) {
                assert_eq!(value, reference, "unselected fresh DATA must stay zero");
            }
            error += f64::from((value - reference).norm_sqr());
            power += f64::from(reference.norm_sqr());
        }
    }
    assert!(power > 0.0);
    let nrms = (error / power).sqrt();
    println!("{} W{workers}: DATA NRMS {nrms:e}", case["name"]);
    assert!(nrms <= 1e-3);
    export(directory, case, workers, backend);
    actual.into_iter().map(|row| row.data).collect()
}

#[test]
fn fresh_selection_and_observe_reset_match_casa() {
    for case in fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["fresh"] == true)
    {
        assert_eq!(
            fresh_case(case, 1, BackendRequest::Cpu),
            fresh_case(case, 3, BackendRequest::Cpu),
            "CPU workers"
        );
    }
}

fn opened_case(case: &Value, workers: usize, backend: BackendRequest) -> Vec<Vec<Complex32>> {
    let directory = case_directory();
    let images = models(directory.path());
    let path = directory.path().join("existing.ms");
    observed(&path, workers, backend).close().unwrap();
    input::prepare(&path);
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: resources(workers, backend),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    let calculation_started = std::time::Instant::now();
    apply_steps(&mut sm, case, &images, &path);
    close(sm, case, backend);
    println!(
        "CALCULATION_TIME {} W{workers} {backend:?} {}",
        case["name"].as_str().unwrap(),
        calculation_started.elapsed().as_secs_f64()
    );
    let (rows, correlations) = read_main(&path);
    assert_eq!(correlations, 2);
    assert_eq!(rows.len(), case["rows"].as_array().unwrap().len());
    let ms = MeasurementSet::open(&path).unwrap();
    let table = ms.main_table();
    let (mut error, mut power) = (0.0_f64, 0.0_f64);
    for (index, (actual, expected)) in rows
        .iter()
        .zip(case["rows"].as_array().unwrap())
        .enumerate()
    {
        assert_eq!(actual.field, expected["field"].as_u64().unwrap() as usize);
        assert_eq!(
            actual.antennas,
            [
                expected["antennas"][0].as_u64().unwrap() as u32,
                expected["antennas"][1].as_u64().unwrap() as u32
            ]
        );
        assert_eq!(actual.flag_row, expected["flag_row"].as_bool().unwrap());
        for (column, key) in [("WEIGHT", "weight"), ("SIGMA", "sigma")] {
            let ArrayValue::Float32(values) =
                table.cell_accessor(index, column).unwrap().array().unwrap()
            else {
                panic!("weight type")
            };
            assert_eq!(values.len(), expected[key].as_array().unwrap().len());
            for (&value, expected) in values.iter().zip(expected[key].as_array().unwrap()) {
                assert_eq!(value, expected.as_f64().unwrap() as f32);
            }
        }
        let ArrayValue::Bool(flags) = table.cell_accessor(index, "FLAG").unwrap().array().unwrap()
        else {
            panic!("FLAG type")
        };
        for channel in 0..4 {
            for correlation in 0..2 {
                assert_eq!(
                    flags[[correlation, channel]],
                    expected["flags"][channel * 2 + correlation]
                        .as_bool()
                        .unwrap()
                );
            }
        }
        assert_eq!(
            actual.data.len(),
            expected["data"].as_array().unwrap().len()
        );
        for (column, key) in [
            ("MODEL_DATA", "model_data"),
            ("CORRECTED_DATA", "corrected_data"),
        ] {
            let ArrayValue::Complex32(values) =
                table.cell_accessor(index, column).unwrap().array().unwrap()
            else {
                panic!("complex column")
            };
            for channel in 0..4 {
                for correlation in 0..2 {
                    let sample = channel * 2 + correlation;
                    let reference = &expected[key][sample];
                    let reference = Complex32::new(
                        reference[0].as_f64().unwrap() as f32,
                        reference[1].as_f64().unwrap() as f32,
                    );
                    if column == "MODEL_DATA" || reference == Complex32::new(-5.0, 6.0) {
                        assert_eq!(
                            values[[correlation, channel]],
                            reference,
                            "{column} row {index}"
                        );
                    } else {
                        assert_eq!(values[[correlation, channel]], actual.data[sample]);
                    }
                }
            }
        }
        for (&value, reference) in actual.data.iter().zip(expected["data"].as_array().unwrap()) {
            assert!(value.re.is_finite() && value.im.is_finite());
            let reference = Complex32::new(
                reference[0].as_f64().unwrap() as f32,
                reference[1].as_f64().unwrap() as f32,
            );
            if reference == Complex32::new(3.0, 4.0) {
                assert_eq!(value, reference, "unselected DATA row {index}");
            } else {
                error += f64::from((value - reference).norm_sqr());
                power += f64::from(reference.norm_sqr());
            }
        }
    }
    if case["noise"] != true {
        assert!(power > 0.0);
        let nrms = (error / power).sqrt();
        println!("{} W{workers}: selected DATA NRMS {nrms:e}", case["name"]);
        assert!(nrms <= 1e-3);
    }
    export(directory, case, workers, backend);
    rows.into_iter().map(|row| row.data).collect()
}

#[test]
fn selected_existing_rows_preserve_data_and_scratch_columns() {
    let fixture = fixture();
    for case in fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["fresh"] != true && case["noise"] != true)
    {
        assert_eq!(
            opened_case(case, 1, BackendRequest::Cpu),
            opened_case(case, 3, BackendRequest::Cpu),
            "CPU workers for {}",
            case["name"]
        );
    }
}

fn close(sm: Simulator, case: &Value, backend: BackendRequest) {
    let ((), summary) = pass_summary::capture(|| sm.close().unwrap());
    let images = case["steps"]
        .as_array()
        .unwrap()
        .iter()
        .any(|step| step["op"] == "predict" && !step["images"].as_array().unwrap().is_empty());
    if images {
        let expected = if backend == BackendRequest::Metal {
            "Metal"
        } else {
            "Cpu"
        };
        let expected = if case["fresh"] == true {
            expected.to_string()
        } else {
            format!("Some({expected})")
        };
        assert_eq!(summary.unwrap().backend, expected);
    }
}

#[test]
fn selected_prediction_uses_metal_and_matches_cpu() {
    if !HostResources::detect().unwrap().metal {
        eprintln!("Metal comparisons skipped: no Metal device");
        return;
    }
    for case in fixture()["cases"].as_array().unwrap() {
        let run = if case["fresh"] == true {
            fresh_case
        } else {
            opened_case
        };
        let cpu = run(case, 3, BackendRequest::Cpu);
        let metal = run(case, 3, BackendRequest::Metal);
        let (mut difference, mut peak) = (0.0_f32, 0.0_f32);
        for (cpu, metal) in cpu.iter().flatten().zip(metal.iter().flatten()) {
            assert!(cpu.is_finite() && metal.is_finite());
            difference = difference.max((*metal - *cpu).norm());
            peak = peak.max(cpu.norm());
        }
        println!(
            "{}: Metal/CPU max relative error {}",
            case["name"],
            difference / peak
        );
        assert!(peak > 0.0 && difference <= 1e-4 * peak);
    }
}

fn export(directory: tempfile::TempDir, case: &Value, workers: usize, backend: BackendRequest) {
    let Some(root) = std::env::var_os("CASA_RS_SELECTION_OUT") else {
        return;
    };
    let name = case["name"].as_str().unwrap();
    if name.ends_with("_without_noise") {
        return;
    }
    let destination = PathBuf::from(root).join(format!("{name}-{backend:?}-w{workers}"));
    if destination.exists() {
        return;
    }
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    let record =
        serde_json::json!({ "name": name, "workers": workers, "backend": format!("{backend:?}") });
    std::fs::write(directory.path().join("case.json"), record.to_string()).unwrap();
    std::fs::rename(directory.path(), destination).unwrap();
}

fn case_directory() -> tempfile::TempDir {
    if let Some(root) = std::env::var_os("CASA_RS_SELECTION_OUT") {
        std::fs::create_dir_all(&root).unwrap();
        tempfile::tempdir_in(root).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
}
