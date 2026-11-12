// SPDX-License-Identifier: LGPL-3.0-or-later
//! Supplied physical error tables imposed on simulated DATA, against CASA.
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
#[allow(dead_code, reason = "reuse the complete MS comparison")]
mod support;

use casa_calibration::{GencalRequest, GencalType, gencal};
use casa_simulation::{ApplySpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, ScalarValue, Value};
use flate2::read::GzDecoder;
use ndarray::{ArrayD, IxDyn};
use serde_json::Value as Json;
use std::io::Read;
use std::path::Path;

fn delay_table(path: &Path) {
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    let mut info = table.info().clone();
    info.sub_type = "K Jones".into();
    table.set_info(info);
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("K Jones".into())),
    );
    for row in 0..table.row_count() {
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("antenna");
        };
        let parameters = if *antenna == 0 {
            vec![1.2, -0.8]
        } else {
            vec![0., 0.]
        };
        for (column, value) in [
            (
                "FPARAM",
                ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[2, 1]), parameters).unwrap()),
            ),
            (
                "FLAG",
                ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[2, 1]), false)),
            ),
            (
                "PARAMERR",
                ArrayValue::Float32(ArrayD::zeros(IxDyn(&[2, 1]))),
            ),
            ("SNR", ArrayValue::Float32(ArrayD::ones(IxDyn(&[2, 1])))),
        ] {
            table
                .cell_accessor_mut(row, column)
                .unwrap()
                .set(Value::Array(value))
                .unwrap();
        }
    }
    table.save(TableOptions::new(path)).unwrap();
    // gencal's single-band delay reference is the native band's centroid.
    let spectral = path.join("SPECTRAL_WINDOW");
    let mut table = Table::open(TableOptions::new(&spectral)).unwrap();
    let centroid = predict_support::FREQUENCY_HZ + 1.5 * predict_support::CHANNEL_HZ;
    table
        .cell_accessor_mut(0, "CHAN_FREQ")
        .unwrap()
        .set(Value::Array(ArrayValue::Float64(ArrayD::from_elem(
            IxDyn(&[1]),
            centroid,
        ))))
        .unwrap();
    table.save(TableOptions::new(&spectral)).unwrap();
}

fn run(case: &Json, workers: usize) -> Vec<Vec<casa_types::Complex32>> {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data.ms");
    let calibration = root.path().join("terms.cal");
    let resources = predict_support::resources(workers, 2 << 30);
    let mut sm = predict_support::session(&path, "ALMA", "XX XY YX YY", resources.clone());
    for (field, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
        sm.observe(&predict_support::scan(field, start, stop))
            .unwrap();
    }
    sm.close().unwrap();
    input::prepare(&path);
    let delay = case["caltype"] == "sbd";
    let opacity = case["caltype"] == "opac";
    gencal(&GencalRequest {
        measurement_set: path.clone(),
        output_table: calibration.clone(),
        caltype: if opacity {
            GencalType::Opac
        } else {
            GencalType::Antpos
        },
        antenna: if opacity { String::new() } else { "0".into() },
        spw: String::new(),
        parameter: if delay {
            vec![0., 0., 0.]
        } else {
            case["parameter"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect()
        },
        gaincurve_table: None,
    })
    .unwrap();
    if delay {
        delay_table(&calibration);
    }
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    sm.set_apply(&ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.,
        interp: "linear".into(),
        calwt: false,
        spwmap: vec![-1],
        opacity: case["opacity"].as_f64().unwrap_or(0.),
    })
    .unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    support::compare(&path, case)
}

#[test]
fn supplied_delay_position_and_opacity_errors_match_casa() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_priors_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Json = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 4);
    for case in cases {
        assert_eq!(case["outcome"], "returned");
        assert_eq!(run(case, 1), run(case, 3), "{}", case["name"]);
    }
}
