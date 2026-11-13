// SPDX-License-Identifier: LGPL-3.0-or-later
//! Source-defined CASA test ramp, independently evaluated through CASA ATM.
mod predict_support;
use casa_simulation::{PredictSpec, Simulator, SimulatorOptions, TroposphereSpec};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use flate2::read::GzDecoder;
use ndarray::IxDyn;
use predict_support::{REFERENCE_MJD, Row, resources};
use std::{io::Read, path::Path};

fn reference() -> serde_json::Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_trop_test_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let reference: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(reference["casa_version"], "6.7.6.14");
    assert_eq!(reference["original"]["returned"], false);
    reference
}

fn gain(
    reference: &serde_json::Value,
    integration: usize,
    antenna: usize,
    channel: usize,
) -> Complex32 {
    let pair = &reference["gains"][integration][antenna][channel];
    Complex32::new(
        pair[0].as_f64().unwrap() as f32,
        pair[1].as_f64().unwrap() as f32,
    )
}

fn observed(path: &Path, point: &Path, workers: usize) -> Simulator {
    let mut sm =
        predict_support::session(path, "ALMA", "XX XY YX YY", resources(workers, 512 << 20));
    for (field, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
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

fn check_table(path: &Path, reference: &serde_json::Value) {
    let table = Table::open(TableOptions::new(path)).unwrap();
    assert_eq!(table.info().sub_type, "Tf Jones");
    assert_eq!(table.row_count(), 4 * 8);
    let mut error = 0.;
    for row in 0..table.row_count() {
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("antenna")
        };
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("TIME")
        };
        assert_eq!(*time, REFERENCE_MJD * 86400. + 5. + 10. * (row / 8) as f64);
        let ArrayValue::Complex32(values) =
            table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
        else {
            panic!("CPARAM")
        };
        assert_eq!(values.shape(), [1, 4]);
        for channel in 0..4 {
            error += (values[IxDyn(&[0, channel])]
                - gain(reference, row / 8, *antenna as usize, channel))
            .norm_sqr();
        }
    }
    let nrms = (error / (4. * 8. * 4.)).sqrt();
    println!("source-defined CASA ATM ramp table NRMS {nrms}");
    assert!(nrms < 1e-3);
}

fn check_data(actual: &[Row], baseline: &[Row], reference: &serde_json::Value, repeats: usize) {
    let mut error = 0.;
    let mut scale = 0.;
    for (row_index, (row, baseline)) in actual.iter().zip(baseline).enumerate() {
        let integration = row_index / 36;
        for (sample, (&value, &sky)) in row.data.iter().zip(&baseline.data).enumerate() {
            let channel = sample / 4;
            let factor = gain(reference, integration, row.antennas[0] as usize, channel)
                * gain(reference, integration, row.antennas[1] as usize, channel).conj();
            let expected = sky * factor.powu(repeats as u32);
            error += (value - expected).norm_sqr();
            scale += expected.norm_sqr();
        }
    }
    let nrms = (error / scale).sqrt();
    println!("source-defined CASA ATM ramp DATA NRMS {nrms}");
    assert!(nrms < 1e-3);
}

#[test]
fn the_test_ramp_matches_the_source_formula_and_independent_casa_atm() {
    let root = tempfile::tempdir().unwrap();
    let point = root.path().join("point.cl");
    predict_support::write_point_list(&point, predict_support::CENTRE_RAD, 1.);
    let baseline_path = root.path().join("baseline.ms");
    observed(&baseline_path, &point, 1).close().unwrap();
    let baseline = predict_support::read_main(&baseline_path).0;
    let reference = reference();
    let mut first: Option<Vec<Row>> = None;
    for (workers, reopened) in [(1, false), (3, false), (3, true)] {
        let export = if workers == 1 {
            std::env::var_os("CASA_RS_TROP_TEST_OUT")
        } else {
            None
        };
        let directory = match &export {
            Some(path) => {
                std::fs::create_dir_all(path).unwrap();
                tempfile::tempdir_in(path).unwrap()
            }
            None => tempfile::tempdir().unwrap(),
        };
        let path = directory.path().join("data.ms");
        let table = directory.path().join("trop.cal");
        let mut sm = observed(&path, &point, workers);
        if reopened {
            let options = SimulatorOptions {
                resources: resources(workers, 512 << 20),
                measures: casa_test_support::deterministic_measures_provider(),
                now: casa_types::measures::MEpoch::from_mjd(
                    REFERENCE_MJD,
                    casa_types::measures::EpochRef::UTC,
                ),
            };
            sm.close().unwrap();
            sm = Simulator::open_from_ms(&path, options).unwrap();
        }
        sm.set_seed(workers as i32);
        sm.set_trop(&TroposphereSpec {
            mode: "test".into(),
            table: table.to_string_lossy().into_owned(),
            water_vapour_mm: 0.01,
            delta_pwv: workers as f64 / 10.,
            beta: workers as f64,
            ..TroposphereSpec::default()
        })
        .unwrap();
        sm.corrupt().unwrap();
        sm.corrupt().unwrap();
        sm.reset();
        sm.close().unwrap();
        check_table(&table, &reference);
        let rows = predict_support::read_main(&path).0;
        check_data(&rows, &baseline, &reference, 2);
        if let Some(first) = &first {
            assert!(rows.iter().zip(first).all(|(a, b)| a.data == b.data));
        } else {
            first = Some(rows);
        }
        if export.is_some() {
            std::fs::write(directory.path().join("case.json"), "{}").unwrap();
            let _kept = directory.keep();
        }
    }
}
