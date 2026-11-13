// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA iterator times and source-defined ramp through independent CASA ATM.
mod predict_support;
use casa_simulation::{PredictSpec, Simulator, SimulatorOptions, TroposphereSpec};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use flate2::read::GzDecoder;
use ndarray::IxDyn;
use predict_support::{REFERENCE_MJD, Row, resources};
use serde_json::Value;
use std::{io::Read, path::Path};

fn reference() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_trop_test_interval_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["casa_version"], "6.7.6.14");
    assert_eq!(value["original"], false);
    value
}

fn observed(path: &Path, point: &Path, workers: usize) -> Simulator {
    let mut sm =
        predict_support::session(path, "ALMA", "XX XY YX YY", resources(workers, 512 << 20));
    for (field, start, stop) in [
        ("target", 0., 20.),
        ("offset", 20., 40.),
        ("target", 40., 140.),
    ] {
        sm.observe(&predict_support::scan(field, start, stop))
            .unwrap();
    }
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_lists: vec![point.into()],
        incremental: false,
    })
    .unwrap();
    sm
}

fn gain(solution: &Value, antenna: usize, channel: usize) -> Complex32 {
    let v = &solution["gains"][antenna][channel];
    Complex32::new(v[0].as_f64().unwrap() as f32, v[1].as_f64().unwrap() as f32)
}

fn scalar(table: &Table, row: usize, name: &str) -> f64 {
    let value = table.cell_accessor(row, name).unwrap();
    match value.scalar().unwrap() {
        ScalarValue::Float64(v) => *v,
        ScalarValue::Int32(v) => f64::from(*v),
        other => panic!("unexpected scalar {other:?}"),
    }
}

fn check(path: &Path, table_path: &Path, rows: &[Row], sky: &[Row], solutions: &[Value]) {
    let table = Table::open(TableOptions::new(table_path)).unwrap();
    assert_eq!(table.info().sub_type, "Tf Jones");
    assert_eq!(table.row_count(), solutions.len() * 8);
    let mut error = 0.;
    for row in 0..table.row_count() {
        let solution = &solutions[row / 8];
        assert_eq!(
            scalar(&table, row, "TIME"),
            solution["time_s"].as_f64().unwrap()
        );
        assert_eq!(
            scalar(&table, row, "FIELD_ID"),
            solution["field"].as_f64().unwrap()
        );
        assert_eq!(scalar(&table, row, "ANTENNA1"), (row % 8) as f64);
        let ArrayValue::Complex32(values) =
            table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
        else {
            panic!("CPARAM");
        };
        for ch in 0..4 {
            error += (values[IxDyn(&[0, ch])] - gain(solution, row % 8, ch)).norm_sqr();
        }
    }
    let table_nrms = (error / (solutions.len() * 8 * 4) as f32).sqrt();
    assert!(table_nrms < 1e-3, "table NRMS {table_nrms}");
    let main = Table::open(TableOptions::new(path)).unwrap();
    let mut error = 0.;
    let mut scale = 0.;
    for (index, (row, sky)) in rows.iter().zip(sky).enumerate() {
        let time = scalar(&main, index, "TIME");
        let solution = solutions
            .iter()
            .min_by(|a, b| {
                (a["time_s"].as_f64().unwrap() - time)
                    .abs()
                    .total_cmp(&(b["time_s"].as_f64().unwrap() - time).abs())
            })
            .unwrap();
        for (sample, (actual, sky)) in row.data.iter().zip(&sky.data).enumerate() {
            let factor = gain(solution, row.antennas[0] as usize, sample / 4)
                * gain(solution, row.antennas[1] as usize, sample / 4).conj();
            let expected = sky * factor * factor;
            error += (*actual - expected).norm_sqr();
            scale += expected.norm_sqr();
        }
    }
    let nrms = (error / scale).sqrt();
    println!(
        "custom test ramp: {} solutions, table NRMS {table_nrms}, DATA NRMS {nrms}",
        solutions.len()
    );
    assert!(nrms < 1e-3);
}

#[test]
fn custom_intervals_follow_casas_iterator_and_source_defined_ramp() {
    let root = tempfile::tempdir().unwrap();
    let point = root.path().join("point.cl");
    predict_support::write_point_list(&point, predict_support::CENTRE_RAD, 1.);
    let baseline = root.path().join("sky.ms");
    observed(&baseline, &point, 1).close().unwrap();
    let sky = predict_support::read_main(&baseline).0;
    for case in reference()["cases"].as_array().unwrap() {
        let simint_s = case["simint_s"].as_f64().unwrap();
        let mut first: Option<Vec<Row>> = None;
        for workers in [1, 3] {
            let export = (workers == 1)
                .then(|| std::env::var_os("CASA_RS_TROP_INTERVAL_OUT"))
                .flatten();
            let dir = if let Some(base) = &export {
                std::fs::create_dir_all(base).unwrap();
                tempfile::tempdir_in(base).unwrap()
            } else {
                tempfile::tempdir().unwrap()
            };
            let path = dir.path().join("data.ms");
            let table = dir.path().join("trop.cal");
            let mut sm = observed(&path, &point, workers);
            if workers == 3 {
                sm.close().unwrap();
                sm = Simulator::open_from_ms(
                    &path,
                    SimulatorOptions {
                        resources: resources(workers, 512 << 20),
                        measures: casa_test_support::deterministic_measures_provider(),
                        now: casa_types::measures::MEpoch::from_mjd(
                            REFERENCE_MJD,
                            casa_types::measures::EpochRef::UTC,
                        ),
                    },
                )
                .unwrap();
            }
            sm.set_trop(&TroposphereSpec {
                mode: "test".into(),
                simint_s,
                water_vapour_mm: 0.01,
                table: table.to_string_lossy().into_owned(),
                ..TroposphereSpec::default()
            })
            .unwrap();
            sm.corrupt().unwrap();
            sm.corrupt().unwrap();
            sm.reset();
            sm.close().unwrap();
            let rows = predict_support::read_main(&path).0;
            assert_eq!(rows.len(), 504);
            check(
                &path,
                &table,
                &rows,
                &sky,
                case["solutions"].as_array().unwrap(),
            );
            if let Some(first) = &first {
                assert!(rows.iter().zip(first).all(|(a, b)| a.data == b.data));
            } else {
                first = Some(rows);
            }
            if export.is_some() {
                std::fs::write(
                    dir.path().join("case.json"),
                    format!("{{\"simint_s\":{simint_s}}}"),
                )
                .unwrap();
                let _kept = dir.keep();
            }
        }
    }
}
