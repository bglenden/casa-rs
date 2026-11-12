// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 OTF=False controls: a full A Noise field, reused on corrupt.
mod predict_support;
use casa_simulation::{NoiseSpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue};
use ndarray::IxDyn;
use std::path::Path;

fn observed(path: &Path, workers: usize, stokes: &str) -> Simulator {
    let mut sm = predict_support::session(
        path,
        "ALMA",
        stokes,
        predict_support::resources(workers, 512 << 20),
    );
    sm.observe(&predict_support::scan("target", 0., 20.))
        .unwrap();
    sm.observe(&predict_support::scan("offset", 20., 40.))
        .unwrap();
    sm
}

fn check_table(path: &Path, table: &Path, stokes: &str, repeats: usize) {
    let main = Table::open(TableOptions::new(path)).unwrap();
    let table = Table::open(TableOptions::new(table)).unwrap();
    assert_eq!(table.info().sub_type, "A Noise");
    assert_eq!(
        table.row_count(),
        144,
        "four integrations of all 36 baselines"
    );
    let parameters: &[usize] = match stokes {
        "XX" => &[0],
        "XX YY" => &[0, 3],
        _ => &[0, 1, 2, 3],
    };
    for row in 0..table.row_count() {
        for column in ["TIME", "FIELD_ID", "ANTENNA1", "ANTENNA2"] {
            assert_eq!(
                main.cell_accessor(row, column).unwrap().scalar().unwrap(),
                table.cell_accessor(row, column).unwrap().scalar().unwrap()
            );
        }
        let ArrayValue::Complex32(noise) =
            table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
        else {
            panic!("CPARAM")
        };
        let ArrayValue::Complex32(data) = main.cell_accessor(row, "DATA").unwrap().array().unwrap()
        else {
            panic!("DATA")
        };
        assert_eq!(
            noise.shape(),
            [4, 4],
            "CASA stores all four correlations for every input layout"
        );
        let ArrayValue::Bool(flags) = table.cell_accessor(row, "FLAG").unwrap().array().unwrap()
        else {
            panic!("FLAG")
        };
        assert!(flags.iter().all(|flag| !*flag));
        for channel in 0..4 {
            for (correlation, &parameter) in parameters.iter().enumerate() {
                // CASA corrects zero DATA by A, then adds the model: DATA -= A.
                let expected = -noise[IxDyn(&[parameter, channel])] * repeats as f32;
                assert!((data[IxDyn(&[correlation, channel])] - expected).norm() < 1e-6);
            }
        }
    }
}

fn run(stokes: &str, workers: usize, repeats: usize, named: bool) -> Vec<predict_support::Row> {
    let export = if named && workers == 1 && repeats == 1 {
        std::env::var_os("CASA_RS_OFFLINE_NOISE_OUT")
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
    let mut sm = observed(&path, workers, stokes);
    sm.set_seed(19);
    sm.set_noise(&NoiseSpec {
        simple_noise_jy: 0.25,
        on_the_fly: false,
        table: if named {
            root.path()
                .join("noise_.cal")
                .to_string_lossy()
                .into_owned()
        } else {
            String::new()
        },
        ..NoiseSpec::default()
    })
    .unwrap();
    for _ in 0..repeats {
        sm.corrupt().unwrap();
    }
    sm.close().unwrap();
    if named {
        check_table(&path, &root.path().join("noise.A.cal"), stokes, repeats);
    }
    let rows = predict_support::read_main(&path).0;
    if export.is_some() {
        std::fs::write(
            root.path().join("case.json"),
            serde_json::to_vec(&serde_json::json!({"stokes":stokes})).unwrap(),
        )
        .unwrap();
        let _kept = root.keep();
    }
    rows
}

#[test]
fn saved_and_unsaved_fields_repeat_casas_fixed_draws() {
    for stokes in ["XX", "XX YY", "XX XY YX YY"] {
        let once = run(stokes, 1, 1, true);
        let twice = run(stokes, 3, 2, true);
        let unsaved = run(stokes, 1, 1, false);
        for ((once, twice), unsaved) in once.iter().zip(&twice).zip(&unsaved) {
            assert_eq!(
                once.data, unsaved.data,
                "saving the field does not change its draws"
            );
            for (&one, &two) in once.data.iter().zip(&twice.data) {
                assert_eq!(two, one * 2.);
            }
        }
        let cross: Vec<_> = once
            .iter()
            .filter(|row| row.antennas[0] != row.antennas[1])
            .flat_map(|row| row.data.iter())
            .collect();
        for imaginary in [false, true] {
            let values: Vec<f64> = cross
                .iter()
                .map(|v| f64::from(if imaginary { v.im } else { v.re }))
                .collect();
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            assert!(mean.abs() < 6. * 0.25 / (values.len() as f64).sqrt());
            assert!((variance - 0.0625).abs() < 6. * 0.0625 * (2. / values.len() as f64).sqrt());
        }
        assert!(
            once.iter()
                .filter(|r| r.antennas[0] == r.antennas[1])
                .all(|r| r.data.iter().all(|v| v.im == 0.))
        );
    }
}

#[test]
fn later_observation_refusal_is_atomic_and_reset_preserves_output() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data.ms");
    let mut sm = observed(&path, 1, "XX YY");
    sm.set_noise(&NoiseSpec {
        simple_noise_jy: 0.25,
        on_the_fly: false,
        table: root.path().join("noise.cal").to_string_lossy().into_owned(),
        ..NoiseSpec::default()
    })
    .unwrap();
    sm.observe(&predict_support::scan("target", 40., 50.))
        .unwrap();
    let error = sm
        .corrupt()
        .expect_err("later observations remain explicitly unfinished");
    assert_eq!(
        error.outcome(),
        casa_simulation::CasaOutcome::NotYetImplemented
    );
    assert!(error.message().contains("#1292"));
    sm.reset();
    sm.close().unwrap();
    assert!(
        predict_support::read_main(&path)
            .0
            .iter()
            .all(|r| r.data.iter().all(|v| *v == Complex32::new(0., 0.)))
    );
    let table = Table::open(TableOptions::new(root.path().join("noise.A.cal"))).unwrap();
    assert_eq!(table.row_count(), 144);
    assert_eq!(
        table
            .cell_accessor(0, "ANTENNA2")
            .unwrap()
            .scalar()
            .unwrap(),
        &ScalarValue::Int32(0)
    );
}

#[test]
fn an_opened_measurement_set_uses_each_integrations_saved_field() {
    let reference = run("XX YY", 1, 1, false);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data.ms");
    observed(&path, 1, "XX YY").close().unwrap();
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(3, 512 << 20),
            now: casa_types::measures::MEpoch::from_mjd(
                predict_support::REFERENCE_MJD,
                casa_types::measures::EpochRef::UTC,
            ),
        },
    )
    .unwrap();
    let spec = NoiseSpec {
        simple_noise_jy: 0.25,
        on_the_fly: false,
        table: root.path().join("noise.cal").to_string_lossy().into_owned(),
        ..NoiseSpec::default()
    };
    sm.set_noise(&spec).unwrap();
    sm.reset();
    sm.set_noise(&NoiseSpec {
        simple_noise_jy: 0.5,
        ..spec
    })
    .unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    check_table(&path, &root.path().join("noise.A.cal"), "XX YY", 1);
    for (actual, reference) in predict_support::read_main(&path).0.iter().zip(&reference) {
        for (actual, reference) in actual.data.iter().zip(&reference.data) {
            assert_eq!(
                *actual,
                *reference * 2.,
                "the newest snapshot uses sigma 0.5 Jy"
            );
        }
    }
}
