// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection and ordered DATA against independent CASA 6.7.6.14 (#1053).

mod predict_support;

use std::io::Read;
use std::path::{Path, PathBuf};

use casa_simulation::{PredictSpec, Simulator, SpectralWindowSpec};
use casa_types::Complex32;
use casa_types::measures::frequency::FrequencyRef;
use flate2::read::GzDecoder;
use predict_support::{
    CHANNEL_HZ, FREQUENCY_HZ, N, read_main, resources, scan, session, write_model, write_model_of,
};
use serde_json::Value;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_selection_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 26);
    fixture
}

fn observed(path: &Path, workers: usize) -> Simulator {
    let mut sm = session(path, "ALMA", "XX YY", resources(workers, 2 << 30));
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

fn fresh_case(case: &Value, workers: usize) {
    let directory = tempfile::tempdir().unwrap();
    let images = models(directory.path());
    let path = directory.path().join("selection.ms");
    let mut sm = observed(&path, workers);
    for step in case["steps"].as_array().unwrap() {
        match step["op"].as_str().unwrap() {
            "setdata" => sm
                .set_data(
                    &ids(step, "spwid"),
                    &ids(step, "fieldid"),
                    step["msselect"].as_str().unwrap(),
                )
                .unwrap(),
            "predict" => sm
                .predict(&PredictSpec {
                    images: step["images"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|name| images[usize::from(name == "b")].clone())
                        .collect(),
                    incremental: step["incremental"].as_bool().unwrap(),
                    ..PredictSpec::default()
                })
                .unwrap(),
            "observe" => {
                let mut spec = scan("target", 80.0, 100.0);
                spec.spectral_window = "band6b".to_string();
                sm.observe(&spec).unwrap();
            }
            operation => panic!("unexpected fresh-case operation {operation}"),
        }
    }
    sm.close().unwrap();
    let actual = read_main(&path).0;
    let expected = case["rows"].as_array().unwrap();
    assert_eq!(actual.len(), expected.len());
    let (mut error, mut power) = (0.0_f64, 0.0_f64);
    for (actual, expected) in actual.iter().zip(expected) {
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
}

#[test]
fn fresh_selection_and_observe_reset_match_casa() {
    for case in fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["fresh"] == true)
    {
        for workers in [1, 3] {
            fresh_case(case, workers);
        }
    }
}
