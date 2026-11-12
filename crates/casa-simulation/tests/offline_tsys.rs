// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA OTF=False Tsys: fixed unit A parameters, scaled by T/Tf noise gains.
mod predict_support;
use casa_simulation::{NoiseSpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32};
use ndarray::IxDyn;
use predict_support::{REFERENCE_MJD, Row, resources};
use std::path::Path;

fn observed(path: &Path, stokes: &str, workers: usize) -> Simulator {
    let mut sm = predict_support::session(path, "ALMA", stokes, resources(workers, 512 << 20));
    for (field, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
        sm.observe(&predict_support::scan(field, start, stop))
            .unwrap();
    }
    sm
}

fn parameter(table: &Table, row: usize, parameter: usize, channel: usize) -> Complex32 {
    let ArrayValue::Complex32(values) =
        table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
    else {
        panic!("CPARAM")
    };
    values[IxDyn(&[parameter, channel])]
}

fn check_tables(rows: &[Row], root: &Path, mode: &str, repeats: usize) {
    let a = Table::open(TableOptions::new(root.join("noise.A.cal"))).unwrap();
    let t = Table::open(TableOptions::new(root.join("noise.T.cal"))).unwrap();
    assert_eq!(a.info().sub_type, "A Noise");
    assert_eq!(a.row_count(), rows.len());
    assert_eq!(
        t.info().sub_type,
        if mode == "tsys-manual" {
            "T Jones"
        } else {
            "Tf Jones"
        }
    );
    assert_eq!(t.row_count(), 32);
    let mut error = 0.;
    let mut scale = 0.;
    let mut unit_power = 0.;
    for (index, row) in rows.iter().enumerate() {
        let correlations = row.data.len() / 4;
        let parameters: &[usize] = match correlations {
            1 => &[0],
            2 => &[0, 3],
            _ => &[0, 1, 2, 3],
        };
        for channel in 0..4 {
            for p in 0..4 {
                unit_power += parameter(&a, index, p, channel).re.powi(2);
            }
            let gain = |antenna| {
                parameter(
                    &t,
                    index / 36 * 8 + antenna,
                    0,
                    if mode == "tsys-manual" { 0 } else { channel },
                )
            };
            let denominator =
                gain(row.antennas[0] as usize) * gain(row.antennas[1] as usize).conj();
            for (correlation, &p) in parameters.iter().enumerate() {
                let expected = -parameter(&a, index, p, channel) / denominator * repeats as f32;
                let actual = row.data[channel * correlations + correlation];
                error += (actual - expected).norm_sqr();
                scale += expected.norm_sqr();
            }
        }
    }
    let unit_power = unit_power / (rows.len() * 16) as f32;
    assert!((unit_power - 1.).abs() < 6. * (2. / (rows.len() * 16) as f32).sqrt());
    let nrms = (error / scale).sqrt();
    println!(
        "offline {mode}, {repeats} applications: A/T DATA NRMS {nrms}, unit A power {unit_power}"
    );
    assert!(nrms < 1e-3);
}

fn run(
    mode: &str,
    stokes: &str,
    workers: usize,
    repeats: usize,
    named: bool,
    reopened: bool,
) -> Vec<Row> {
    let export = if workers == 1 && repeats == 1 && named && stokes == "XX XY YX YY" {
        std::env::var_os("CASA_RS_OFFLINE_TSYS_OUT")
    } else {
        None
    };
    let root = match &export {
        Some(path) => {
            std::fs::create_dir_all(path).unwrap();
            tempfile::tempdir_in(path).unwrap()
        }
        None => tempfile::tempdir().unwrap(),
    };
    let path = root.path().join("data.ms");
    let mut sm = observed(&path, stokes, workers);
    if reopened {
        sm.close().unwrap();
        sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: casa_test_support::deterministic_measures_provider(),
                resources: resources(workers, 512 << 20),
                now: casa_types::measures::MEpoch::from_mjd(
                    REFERENCE_MJD,
                    casa_types::measures::EpochRef::UTC,
                ),
            },
        )
        .unwrap();
    }
    sm.set_seed(19);
    sm.set_noise(&NoiseSpec {
        mode: mode.into(),
        on_the_fly: false,
        simple_noise_jy: 99.,
        water_vapour_mm: 1.,
        table: if named {
            root.path().join("noise.cal").to_string_lossy().into_owned()
        } else {
            String::new()
        },
        tsys: casa_simulation::TsysParameters {
            tau: 0.05,
            ..casa_simulation::TsysParameters::default()
        },
        ..NoiseSpec::default()
    })
    .unwrap();
    for _ in 0..repeats {
        sm.corrupt().unwrap();
    }
    sm.reset();
    sm.close().unwrap();
    let rows = predict_support::read_main(&path).0;
    if named {
        check_tables(&rows, root.path(), mode, repeats);
    }
    if export.is_some() {
        std::fs::write(
            root.path().join("case.json"),
            serde_json::to_vec(&serde_json::json!({"mode":mode})).unwrap(),
        )
        .unwrap();
        let _kept = root.keep();
    }
    rows
}

#[test]
fn offline_tsys_reuses_unit_noise_with_its_noise_scale() {
    for mode in ["tsys-manual", "tsys-atm"] {
        for stokes in ["XX", "XX YY", "XX XY YX YY"] {
            let once = run(mode, stokes, 1, 1, true, false);
            let twice = run(mode, stokes, 3, 2, true, false);
            let unsaved = run(mode, stokes, 1, 1, false, false);
            let opened = run(mode, stokes, 2, 1, true, true);
            for (((a, b), c), d) in once.iter().zip(&twice).zip(&unsaved).zip(&opened) {
                assert_eq!(a.data, c.data);
                assert_eq!(a.data, d.data);
                for (a, b) in a.data.iter().zip(&b.data) {
                    assert_eq!(*b, *a * 2.);
                }
            }
        }
    }
}

#[test]
fn offline_tsys_adds_noise_to_the_predicted_model() {
    let root = tempfile::tempdir().unwrap();
    let point = root.path().join("point.cl");
    predict_support::write_point_list(&point, predict_support::CENTRE_RAD, 0.01);
    for mode in ["tsys-manual", "tsys-atm"] {
        let noise = run(mode, "XX XY YX YY", 1, 1, false, false);
        let mut outputs = Vec::new();
        for noisy in [false, true] {
            let path = root.path().join(format!("{mode}-{noisy}.ms"));
            let mut sm = observed(&path, "XX XY YX YY", 1);
            sm.predict(&casa_simulation::PredictSpec {
                images: Vec::new(),
                component_lists: vec![point.clone()],
                incremental: false,
            })
            .unwrap();
            if noisy {
                sm.set_seed(19);
                sm.set_noise(&NoiseSpec {
                    mode: mode.into(),
                    on_the_fly: false,
                    water_vapour_mm: 1.,
                    tsys: casa_simulation::TsysParameters {
                        tau: 0.05,
                        ..casa_simulation::TsysParameters::default()
                    },
                    ..NoiseSpec::default()
                })
                .unwrap();
                sm.corrupt().unwrap();
            }
            sm.close().unwrap();
            outputs.push(predict_support::read_main(&path).0);
        }
        for ((sky, actual), noise) in outputs[0].iter().zip(&outputs[1]).zip(&noise) {
            for ((sky, actual), noise) in sky.data.iter().zip(&actual.data).zip(&noise.data) {
                assert!(
                    (*actual - (*sky + *noise)).norm() < 2e-8,
                    "scale only the additive contribution"
                );
            }
        }
    }
}

#[test]
fn offline_tsys_refuses_later_observations_before_data_changes() {
    for mode in ["tsys-manual", "tsys-atm"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data.ms");
        let mut sm = observed(&path, "XX YY", 1);
        sm.set_noise(&NoiseSpec {
            mode: mode.into(),
            on_the_fly: false,
            water_vapour_mm: 1.,
            table: root.path().join("noise.cal").to_string_lossy().into_owned(),
            ..NoiseSpec::default()
        })
        .unwrap();
        sm.observe(&predict_support::scan("target", 40., 50.))
            .unwrap();
        let error = sm.corrupt().unwrap_err();
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
                .all(|r| r.data.iter().all(|v| v.norm_sqr() == 0.))
        );
        assert_eq!(
            Table::open(TableOptions::new(root.path().join("noise.A.cal")))
                .unwrap()
                .row_count(),
            144
        );
        assert_eq!(
            Table::open(TableOptions::new(root.path().join("noise.T.cal")))
                .unwrap()
                .row_count(),
            32
        );
    }
}
