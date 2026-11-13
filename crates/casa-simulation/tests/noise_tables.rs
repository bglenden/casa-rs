// SPDX-License-Identifier: LGPL-3.0-or-later
//! Optional simulation T/Tf noise-scale tables, against independent CASA output.
mod predict_support;
use std::{io::Read, path::Path};

use casa_simulation::{NoiseSpec, PredictSpec, Simulator};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use flate2::read::GzDecoder;
use serde_json::Value;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_noise_tables_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    fixture
}

fn observed(path: &Path, workers: usize) -> Simulator {
    let mut sm = predict_support::session(
        path,
        "ALMA",
        "XX XY YX YY",
        predict_support::resources(workers, 512 << 20),
    );
    for (field, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
        sm.observe(&predict_support::scan(field, start, stop))
            .unwrap();
    }
    sm
}

fn spec(case: &Value, table: &Path) -> NoiseSpec {
    let mut spec = NoiseSpec {
        mode: case["options"]["mode"].as_str().unwrap().into(),
        table: table.to_string_lossy().into_owned(),
        water_vapour_mm: 1.,
        ..NoiseSpec::default()
    };
    if let Some(tau) = case["options"]["tau"].as_f64() {
        spec.tsys.tau = tau;
    }
    spec
}

fn check_table(path: &Path, case: &Value) {
    let table = Table::open(TableOptions::new(path)).unwrap();
    assert_eq!(
        table.info().sub_type,
        case["info"]["subType"].as_str().unwrap()
    );
    let rows = case["rows"].as_array().unwrap();
    assert_eq!(table.row_count(), rows.len());
    let (mut error, mut power) = (0., 0.);
    for (row, expected) in rows.iter().enumerate() {
        for (column, key) in [
            ("ANTENNA1", "antenna"),
            ("FIELD_ID", "field"),
            ("SPECTRAL_WINDOW_ID", "window"),
        ] {
            assert_eq!(
                table.cell_accessor(row, column).unwrap().scalar().unwrap(),
                &ScalarValue::Int32(expected[key].as_i64().unwrap() as i32)
            );
        }
        assert_eq!(
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap(),
            &ScalarValue::Float64(expected["time_s"].as_f64().unwrap())
        );
        let ArrayValue::Complex32(gains) =
            table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
        else {
            panic!("CPARAM")
        };
        let shape: Vec<_> = expected["shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap() as usize)
            .collect();
        assert_eq!(gains.shape(), shape);
        let ArrayValue::Bool(flags) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("FLAG")
        };
        assert_eq!(
            flags.iter().copied().collect::<Vec<_>>(),
            expected["flags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_bool().unwrap())
                .collect::<Vec<_>>()
        );
        for (gain, expected) in gains.iter().zip(expected["gains"].as_array().unwrap()) {
            let reference = Complex32::new(
                expected[0].as_f64().unwrap() as f32,
                expected[1].as_f64().unwrap() as f32,
            );
            error += f64::from((*gain - reference).norm_sqr());
            power += f64::from(reference.norm_sqr());
        }
    }
    let nrms = (error / power).sqrt();
    println!("{} table NRMS {nrms:.3e}", case["options"]["name"]);
    assert!(
        nrms < 1e-3,
        "{} table NRMS {}",
        case["options"]["name"],
        nrms
    );
}

fn run(case: &Value, workers: usize, output: bool) -> Vec<Vec<Complex32>> {
    let export = if output && workers == 1 {
        std::env::var_os("CASA_RS_NOISE_TABLE_OUT")
    } else {
        None
    };
    let root = if let Some(path) = &export {
        std::fs::create_dir_all(path).unwrap();
        tempfile::tempdir_in(path).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let path = root.path().join("data.ms");
    let mut sm = observed(&path, workers);
    let point = root.path().join("point.cl");
    predict_support::write_point_list(&point, predict_support::CENTRE_RAD, 1.);
    sm.predict(&PredictSpec {
        component_lists: vec![point],
        ..PredictSpec::default()
    })
    .unwrap();
    sm.set_seed(31);
    let table = root.path().join("noise.cal");
    let mut parameters = spec(case, &table);
    if !output {
        parameters.table.clear();
    }
    sm.set_noise(&parameters).unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    if output {
        check_table(&root.path().join("noise.T.cal"), case);
    }
    let data = predict_support::read_main(&path)
        .0
        .into_iter()
        .map(|row| row.data)
        .collect();
    if export.is_some() {
        std::fs::write(
            root.path().join("case.json"),
            serde_json::to_vec(case).unwrap(),
        )
        .unwrap();
        let _kept = root.keep();
    }
    data
}

#[test]
fn tsys_tables_match_casa_without_changing_predicted_noisy_data() {
    let fixture = fixture();
    for case in fixture["cases"].as_array().unwrap() {
        let reference = run(case, 1, false);
        assert_eq!(run(case, 1, true), reference);
        assert_eq!(run(case, 3, true), reference);
    }
}

#[test]
fn reset_and_later_observations_preserve_the_table_snapshot() {
    let fixture = fixture();
    for case in fixture["cases"].as_array().unwrap() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data.ms");
        let mut sm = observed(&path, 1);
        sm.set_noise(&spec(case, &root.path().join("noise_.cal")))
            .unwrap();
        sm.reset();
        sm.observe(&predict_support::scan("target", 40., 60.))
            .unwrap();
        sm.close().unwrap();
        check_table(&root.path().join("noise.T.cal"), case);
    }
}
