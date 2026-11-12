// SPDX-License-Identifier: LGPL-3.0-or-later
//! Entire corruption steps skip uncovered windows, as installed CASA does.
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_ms::MeasurementSet;
use casa_simulation::{
    ApplySpec, NoiseSpec, PredictSpec, Simulator, SimulatorOptions, SpectralWindowSpec,
};
use casa_types::measures::{EpochRef, MEpoch, frequency::FrequencyRef};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use flate2::read::GzDecoder;
use predict_support::{
    ANTENNAS, CENTRE_RAD, CHANNEL_HZ, FREQUENCY_HZ, REFERENCE_MJD, read_main, resources, scan,
    session, write_point_list,
};
use serde_json::Value;
use std::{io::Read, path::Path};

fn fixture(noise: bool) -> Value {
    let bytes: &[u8] = if noise {
        include_bytes!("data/sm_apply_later_window_noise_casa.json.gz")
    } else {
        include_bytes!("data/sm_apply_existing_window_casa.json.gz")
    };
    let mut text = String::new();
    GzDecoder::new(bytes).read_to_string(&mut text).unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["casa_version"], "6.7.6.14");
    assert_eq!(value["rows"].as_array().unwrap().len(), 180);
    value
}

fn original(path: &Path, workers: usize) -> Simulator {
    let mut sm = session(path, "ALMA", "XX XY YX YY", resources(workers, 256 << 20));
    sm.observe(&scan("target", 0.0, 20.0)).unwrap();
    sm.observe(&scan("offset", 20.0, 40.0)).unwrap();
    sm
}

fn later(sm: &mut Simulator) {
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "later".into(),
        start_frequency_hz: FREQUENCY_HZ,
        channel_width_hz: CHANNEL_HZ,
        resolution_hz: CHANNEL_HZ,
        frame: FrequencyRef::TOPO,
        channel_count: 4,
        stokes: "XX XY YX YY".into(),
    })
    .unwrap();
    let mut spec = scan("target", 40.0, 50.0);
    spec.spectral_window = "later".into();
    sm.observe(&spec).unwrap();
}

fn gains(reference: &Path, path: &Path) {
    let ms = MeasurementSet::open(reference).unwrap();
    let times = [5223916805.0, 5223916835.0];
    let rows = times.into_iter().enumerate().flat_map(|(field, time_s)| {
        (0..ANTENNAS).map(move |antenna| SimulatedSolution {
            time_s,
            field_id: field as i32,
            spectral_window_id: 0,
            antenna: antenna as i32,
            values: SolutionValues::Receptors([Complex32::new(2.0, 0.0); 2]),
        })
    });
    write_simulated_jones_table(&ms, path, SimulatedJones::Gain, rows).unwrap();
}

fn apply(path: &Path, broadcast: bool) -> ApplySpec {
    ApplySpec {
        table: path.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: "nearest".into(),
        calwt: false,
        spwmap: if broadcast { vec![0] } else { vec![-1] },
        opacity: 0.0,
    }
}

fn options(workers: usize) -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(workers, 256 << 20),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

#[test]
fn existing_ms_whole_step_preserves_uncovered_data_noise_and_scratch() {
    for noise in [false, true] {
        for workers in [1, 3] {
            let root = tempfile::tempdir().unwrap();
            let reference = root.path().join("reference.ms");
            original(&reference, workers).close().unwrap();
            let table = root.path().join("g.cal");
            gains(&reference, &table);
            let path = root.path().join("applied.ms");
            let mut sm = original(&path, workers);
            later(&mut sm);
            sm.close().unwrap();
            input::prepare(&path);
            let mut sm = Simulator::open_from_ms(&path, options(workers)).unwrap();
            sm.set_apply(&apply(&table, false)).unwrap();
            if noise {
                sm.set_seed(5);
                sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
            }
            sm.corrupt().unwrap();
            sm.close().unwrap();
            compare(&path, &fixture(noise), noise);
        }
    }
}

fn compare(path: &Path, reference: &Value, noise: bool) {
    let ms = MeasurementSet::open(path).unwrap();
    let table = ms.main_table();
    let (rows, correlations) = read_main(path);
    assert_eq!(correlations, 4);
    let mut native_variance = 0.0;
    let mut casa_variance = 0.0;
    for (row, (actual, expected)) in rows
        .iter()
        .zip(reference["rows"].as_array().unwrap())
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
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("time")
        };
        assert_eq!(*time, expected["time_s"].as_f64().unwrap());
        for (column, key) in [("WEIGHT", "weight"), ("SIGMA", "sigma")] {
            let ArrayValue::Float32(values) =
                table.cell_accessor(row, column).unwrap().array().unwrap()
            else {
                panic!("float")
            };
            assert_eq!(
                values.iter().copied().collect::<Vec<_>>(),
                expected[key]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_f64().unwrap() as f32)
                    .collect::<Vec<_>>()
            );
        }
        let ArrayValue::Bool(flags) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("flags")
        };
        let covered = expected["window"] == 0;
        for (sample, &value) in actual.data.iter().enumerate() {
            let complex = |key: &str| {
                Complex32::new(
                    expected[key][sample][0].as_f64().unwrap() as f32,
                    expected[key][sample][1].as_f64().unwrap() as f32,
                )
            };
            assert_eq!(
                flags[[sample % 4, sample / 4]],
                expected["flags"][sample].as_bool().unwrap()
            );
            if noise && covered {
                native_variance += f64::from((value - Complex32::new(12.0, 16.0)).norm_sqr());
                casa_variance +=
                    f64::from((complex("data") - Complex32::new(12.0, 16.0)).norm_sqr());
            } else {
                assert_eq!(value, complex("data"));
            }
            for (column, key) in [
                ("MODEL_DATA", "model_data"),
                ("CORRECTED_DATA", "corrected_data"),
            ] {
                let ArrayValue::Complex32(values) =
                    table.cell_accessor(row, column).unwrap().array().unwrap()
                else {
                    panic!("complex")
                };
                let expected = if noise && covered && column == "CORRECTED_DATA" {
                    value
                } else {
                    complex(key)
                };
                assert_eq!(
                    values[[sample % 4, sample / 4]],
                    expected,
                    "{column} row{row}"
                );
            }
        }
    }
    assert_eq!(rows.len(), 180);
    if noise {
        let ratio = native_variance / casa_variance;
        assert!(
            (0.85..1.15).contains(&ratio),
            "independent noise variance ratio {ratio}"
        );
    }
}

#[test]
fn new_window_after_setapply_skips_the_whole_step_with_nonzero_predictions() {
    let root = tempfile::tempdir().unwrap();
    let reference = root.path().join("reference.ms");
    original(&reference, 1).close().unwrap();
    let table = root.path().join("g.cal");
    gains(&reference, &table);
    let point = root.path().join("point.cl");
    write_point_list(&point, CENTRE_RAD, 1.0);
    let output = |name: &str, corrupt: bool| {
        let path = root.path().join(name);
        let mut sm = original(&path, 1);
        if corrupt {
            sm.set_apply(&apply(&table, true)).unwrap();
            sm.set_seed(5);
            sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        }
        later(&mut sm);
        sm.predict(&PredictSpec {
            images: Vec::new(),
            component_lists: vec![point.clone()],
            incremental: false,
        })
        .unwrap();
        if corrupt {
            sm.corrupt().unwrap();
        }
        sm.close().unwrap();
        read_main(&path).0
    };
    let clean = output("clean.ms", false);
    let corrupted = output("corrupt.ms", true);
    for (row, (a, b)) in clean.iter().zip(&corrupted).enumerate() {
        if row >= 144 {
            assert!(a.data.iter().any(|value| value.norm() > 0.5));
            assert_eq!(a.data, b.data, "entire new-window step including noise");
        }
    }
    assert!(
        clean[..144]
            .iter()
            .zip(&corrupted[..144])
            .any(|(a, b)| a.data != b.data)
    );
}
