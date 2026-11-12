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

fn patch_table(path: &Path, case: &Json) {
    let delay = case["caltype"] == "sbd";
    let kind = case["subtype"].as_str().unwrap_or("K Jones");
    let recipe: Vec<f32> = case["values"]
        .as_array()
        .unwrap_or_else(|| case["parameter"].as_array().unwrap())
        .iter()
        .map(|v| v.as_f64().unwrap() as f32)
        .collect();
    let channels = case["channels"].as_u64().unwrap_or(1) as usize;
    let shape = IxDyn(&[recipe.len() / channels, channels]);
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    let mut info = table.info().clone();
    info.sub_type = kind.into();
    table.set_info(info);
    table
        .keywords_mut()
        .upsert("VisCal", Value::Scalar(ScalarValue::String(kind.into())));
    for row in 0..table.row_count() {
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("antenna");
        };
        let parameters = if delay && *antenna != 0 {
            vec![0.; recipe.len()]
        } else {
            recipe.clone()
        };
        for (column, value) in [
            (
                "FPARAM",
                ArrayValue::Float32(ArrayD::from_shape_vec(shape.clone(), parameters).unwrap()),
            ),
            (
                "FLAG",
                ArrayValue::Bool(ArrayD::from_elem(shape.clone(), false)),
            ),
            (
                "PARAMERR",
                ArrayValue::Float32(ArrayD::zeros(shape.clone())),
            ),
            ("SNR", ArrayValue::Float32(ArrayD::ones(shape.clone()))),
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
            IxDyn(&[channels]),
            centroid,
        ))))
        .unwrap();
    table
        .cell_accessor_mut(0, "NUM_CHAN")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(channels as i32)))
        .unwrap();
    for column in ["CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"] {
        table
            .cell_accessor_mut(0, column)
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(ArrayD::from_elem(
                IxDyn(&[channels]),
                predict_support::CHANNEL_HZ,
            ))))
            .unwrap();
    }
    if channels > 1 {
        let axis: Vec<_> = (0..channels)
            .map(|i| {
                predict_support::FREQUENCY_HZ
                    + 3. * predict_support::CHANNEL_HZ * i as f64 / (channels - 1) as f64
            })
            .collect();
        table
            .cell_accessor_mut(0, "CHAN_FREQ")
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[channels]), axis).unwrap(),
            )))
            .unwrap();
        table
            .cell_accessor_mut(0, "NUM_CHAN")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Int32(channels as i32)))
            .unwrap();
        for column in ["CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"] {
            table
                .cell_accessor_mut(0, column)
                .unwrap()
                .set(Value::Array(ArrayValue::Float64(ArrayD::from_elem(
                    IxDyn(&[channels]),
                    3. * predict_support::CHANNEL_HZ / (channels - 1) as f64,
                ))))
                .unwrap();
        }
    }
    table.save(TableOptions::new(&spectral)).unwrap();
}

fn time_axis(path: &Path, case: &Json) {
    let Some(parameters) = case["time_values"].as_array() else {
        return;
    };
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    let count = table.row_count();
    let rows: Vec<_> = (0..count)
        .map(|row| table.row_accessor().row(row).unwrap().clone())
        .collect();
    for row in rows {
        table.add_row(row).unwrap();
    }
    let times = [
        case["rows"][0]["time_s"].as_f64().unwrap(),
        case["rows"].as_array().unwrap().last().unwrap()["time_s"]
            .as_f64()
            .unwrap(),
    ];
    for row in 0..table.row_count() {
        let endpoint = row / count;
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("antenna");
        };
        let values: Vec<f32> = if *antenna == 0 {
            parameters[endpoint]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap() as f32)
                .collect()
        } else {
            vec![0.; 2]
        };
        for (column, value) in [
            ("TIME", Value::Scalar(ScalarValue::Float64(times[endpoint]))),
            (
                "FIELD_ID",
                Value::Scalar(ScalarValue::Int32(endpoint as i32)),
            ),
            (
                "FPARAM",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[2, 1]), values).unwrap(),
                )),
            ),
        ] {
            table
                .cell_accessor_mut(row, column)
                .unwrap()
                .set(value)
                .unwrap();
        }
    }
    table.save(TableOptions::new(path)).unwrap();
}

fn run(case: &Json, workers: usize) -> Vec<Vec<casa_types::Complex32>> {
    let export = std::env::var_os("CASA_RS_APPLY_PRIORS_OUT");
    let root = if let Some(out) = &export {
        std::fs::create_dir_all(out).unwrap();
        tempfile::tempdir_in(out).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let path = root.path().join("data.ms");
    let calibration = root.path().join("terms.cal");
    let resources = predict_support::resources(workers, 2 << 30);
    let mut sm = predict_support::session(&path, "ALMA", "XX XY YX YY", resources);
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
    // CASA gencal collapses a uniform parameter table's spectral axis.
    // Its four 15.625 MHz channels become one at 230.0234375 GHz.
    let spectral = Table::open(TableOptions::new(calibration.join("SPECTRAL_WINDOW"))).unwrap();
    assert_eq!(
        spectral
            .cell_accessor(0, "NUM_CHAN")
            .unwrap()
            .scalar()
            .unwrap(),
        &ScalarValue::Int32(1)
    );
    for (column, expected) in [("CHAN_FREQ", 230_023_437_500.), ("CHAN_WIDTH", 62_500_000.)] {
        let ArrayValue::Float64(values) =
            spectral.cell_accessor(0, column).unwrap().array().unwrap()
        else {
            panic!("spectral axis");
        };
        assert_eq!(values.as_slice().unwrap(), &[expected]);
    }
    if delay || case["subtype"].is_string() {
        patch_table(&calibration, case);
    }
    time_axis(&calibration, case);
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    let calibration = if let Some(casa) = std::env::var_os("CASA_RS_APPLY_PRIORS_CASA_TABLES") {
        assert!(export.is_none());
        std::path::PathBuf::from(casa)
            .join(case["name"].as_str().unwrap())
            .join("terms.cal")
    } else {
        calibration
    };
    let spec = ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.,
        interp: "linear".into(),
        calwt: false,
        spwmap: vec![-1],
        opacity: case["opacity"].as_f64().unwrap_or(0.),
    };
    if case["leakage"] == true {
        let leakage = root.path().join("leakage.cal");
        support::leakage_fixture(&path, &leakage);
        sm.set_apply(&ApplySpec {
            table: leakage.to_string_lossy().into_owned(),
            ..spec.clone()
        })
        .unwrap();
    }
    sm.set_apply(&spec).unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    let output = support::compare(&path, case);
    if export.is_some() {
        std::fs::write(
            root.path().join("case.json"),
            serde_json::to_vec(&serde_json::json!({"name":case["name"],"workers":workers}))
                .unwrap(),
        )
        .unwrap();
        let _kept = root.keep();
    }
    output
}

#[test]
fn supplied_physical_error_tables_match_casa() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_priors_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Json = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 14);
    for case in cases {
        assert_eq!(case["outcome"], "returned");
        assert_eq!(run(case, 1), run(case, 3), "{}", case["name"]);
    }
}
