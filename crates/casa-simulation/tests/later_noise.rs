// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA later-scan controls: nearest original A/T solutions and window gates.
mod predict_support;
use casa_simulation::{NoiseSpec, Simulator, SpectralWindowSpec, TsysParameters};
use casa_tables::{Table, TableOptions};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, Complex32};
use ndarray::IxDyn;
use predict_support::Row;
use std::path::Path;

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

fn extend(sm: &mut Simulator, new_window: bool) {
    if new_window {
        sm.set_spectral_window(&SpectralWindowSpec {
            name: "later".into(),
            start_frequency_hz: 100e9,
            channel_width_hz: 15.625e6,
            resolution_hz: 15.625e6,
            channel_count: 4,
            stokes: "XX XY YX YY".into(),
            frame: FrequencyRef::TOPO,
        })
        .unwrap();
    }
    sm.observe(&casa_simulation::ObserveSpec {
        spectral_window: if new_window { "later" } else { "band6" }.into(),
        ..predict_support::scan("target", 3600., 3620.)
    })
    .unwrap();
}

fn run(
    mode: &str,
    otf: bool,
    new_window: bool,
    workers: usize,
    named: bool,
) -> (tempfile::TempDir, Vec<Row>) {
    let export = std::env::var_os("CASA_RS_LATER_NOISE_EXPORT");
    let root = if named
        && workers == 1
        && let Some(base) = export
    {
        std::fs::create_dir_all(&base).unwrap();
        tempfile::tempdir_in(base).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let path = root.path().join("data.ms");
    let mut sm = observed(&path, workers);
    sm.set_seed(19);
    sm.set_noise(&NoiseSpec {
        mode: mode.into(),
        on_the_fly: otf,
        simple_noise_jy: 1.,
        water_vapour_mm: 1.,
        tsys: TsysParameters {
            tau: 0.05,
            ..TsysParameters::default()
        },
        table: if named {
            root.path().join("noise.cal").to_string_lossy().into_owned()
        } else {
            String::new()
        },
        ..NoiseSpec::default()
    })
    .unwrap();
    extend(&mut sm, new_window);
    sm.corrupt()
        .expect("later observations use their original noise terms");
    sm.close().unwrap();
    let rows = predict_support::read_main(&path).0;
    (root, rows)
}

fn value(table: &Table, row: usize, parameter: usize, channel: usize) -> Complex32 {
    let ArrayValue::Complex32(v) = table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
    else {
        panic!("CPARAM")
    };
    v[IxDyn(&[parameter, channel])]
}

fn check(mode: &str, otf: bool, new_window: bool, root: &Path, rows: &[Row], unit: &[Row]) {
    assert_eq!(rows.len(), 216);
    let a = (!otf).then(|| Table::open(TableOptions::new(root.join("noise.A.cal"))).unwrap());
    let t = (mode != "simplenoise")
        .then(|| Table::open(TableOptions::new(root.join("noise.T.cal"))).unwrap());
    if let Some(t) = &t {
        assert_eq!(t.row_count(), 32, "the original T/Tf snapshot is unchanged");
    }
    if let Some(a) = &a {
        assert_eq!(a.row_count(), 144);
    }
    for name in ["noise.A.cal", "noise.T.cal"] {
        let calibration = root.join(name);
        if calibration.exists() {
            assert_eq!(
                Table::open(TableOptions::new(calibration.join("SPECTRAL_WINDOW")))
                    .unwrap()
                    .row_count(),
                1,
                "saved metadata predates the added window"
            );
        }
    }
    let mut error = 0.;
    let mut scale = 0.;
    for (index, row) in rows.iter().enumerate() {
        let original = index.min(143) / 36;
        for channel in 0..4 {
            for correlation in 0..4 {
                let expected = if new_window && index >= 144 {
                    Complex32::new(0., 0.)
                } else {
                    let noise = if let Some(a) = &a {
                        -value(a, original * 36 + index % 36, correlation, channel)
                    } else {
                        unit[index].data[channel * 4 + correlation]
                    };
                    if let Some(t) = &t {
                        let ch = if mode == "tsys-manual" { 0 } else { channel };
                        noise
                            / (value(t, original * 8 + row.antennas[0] as usize, 0, ch)
                                * value(t, original * 8 + row.antennas[1] as usize, 0, ch).conj())
                    } else {
                        noise
                    }
                };
                error += (row.data[channel * 4 + correlation] - expected).norm_sqr();
                scale += expected.norm_sqr();
            }
        }
        if new_window && index >= 144 {
            assert!(row.data.iter().all(|v| v.norm_sqr() == 0.));
        }
    }
    let nrms = (error / scale).sqrt();
    println!("later {mode} OTF={otf} new_window={new_window}: original A/T NRMS {nrms}");
    assert!(nrms < 1e-3);
}

#[test]
fn later_observations_follow_casas_original_solutions_and_window_coverage() {
    let (_unit_root, unit) = run("simplenoise", true, false, 1, false);
    for mode in ["simplenoise", "tsys-manual", "tsys-atm"] {
        for otf in [false, true] {
            for new_window in [false, true] {
                let (root, once) = run(mode, otf, new_window, 1, true);
                check(mode, otf, new_window, root.path(), &once, &unit);
                if std::env::var_os("CASA_RS_LATER_NOISE_EXPORT").is_some() {
                    std::fs::write(
                        root.path().join("case.json"),
                        serde_json::to_vec(
                            &serde_json::json!({"mode":mode,"otf":otf,"new_window":new_window}),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                    let _kept = root.keep();
                }
                let (_root, workers) = run(mode, otf, new_window, 3, true);
                let (_root, unnamed) = run(mode, otf, new_window, 1, false);
                let mut error = 0.;
                let mut scale = 0.;
                for ((a, b), c) in once.iter().zip(&workers).zip(&unnamed) {
                    assert_eq!(a.data, b.data);
                    for (a, c) in a.data.iter().zip(&c.data) {
                        error += (*a - *c).norm_sqr();
                        scale += a.norm_sqr();
                    }
                }
                assert!(
                    (error / scale).sqrt() < 1e-3,
                    "named and unnamed original scales agree"
                );
            }
        }
    }
}

#[test]
fn original_noise_coverage_gates_incompatible_later_leakage_before_evaluation() {
    for noise in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut sm = observed(&root.path().join("data.ms"), 1);
        if noise {
            sm.set_noise(&NoiseSpec {
                on_the_fly: false,
                ..NoiseSpec::default()
            })
            .unwrap();
        }
        sm.set_spectral_window(&SpectralWindowSpec {
            name: "later".into(),
            start_frequency_hz: 100e9,
            channel_width_hz: 15.625e6,
            resolution_hz: 15.625e6,
            channel_count: 4,
            stokes: "XX YY".into(),
            frame: FrequencyRef::TOPO,
        })
        .unwrap();
        sm.observe(&casa_simulation::ObserveSpec {
            spectral_window: "later".into(),
            ..predict_support::scan("target", 3600., 3620.)
        })
        .unwrap();
        sm.set_leakage(&casa_simulation::LeakageSpec {
            table: String::new(),
            amplitude: vec![0.01],
            offset: vec![0.],
        })
        .unwrap();
        let result = sm.corrupt();
        if noise {
            result.expect("CASA skips the entire uncovered noise/D step");
        } else {
            assert!(
                result
                    .unwrap_err()
                    .message()
                    .contains("incompatible with VisVector")
            );
        }
        // Mixed-shape MS writing is owned by #1294; this tests the corruption preflight.
    }
}
