// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setgain` and `sm.setleakage` then `sm.corrupt` (#841 Phase 3): the
//! antenna-based terms as a session applies them to the rows observed so
//! far. The terms' simulation and its agreement with CASA are the crate's
//! `corrupt::jones` module's and its unit tests'; these laws hold what
//! `corrupt` and `close` make of them.

mod predict_support;

use std::path::Path;

use casa_simulation::{
    CasaOutcome, GainSpec, LeakageSpec, NoiseSpec, PredictSpec, Simulator, SimulatorResult,
    TsysParameters,
};
use num_complex::Complex32;
use predict_support::{
    ANTENNAS, CENTRE_RAD, Row, bits, data, read_main, resources, scan, session, write_point_list,
    write_stokes_point_list,
};

fn gain(amplitude: f64) -> GainSpec {
    GainSpec {
        mode: "fbm".to_string(),
        table: String::new(),
        amplitude: vec![amplitude],
    }
}

fn leakage() -> LeakageSpec {
    LeakageSpec {
        table: String::new(),
        amplitude: vec![0.05],
        offset: vec![0.02],
    }
}

fn simple_noise() -> NoiseSpec {
    NoiseSpec::simple(0.1)
}

/// A session of `stokes` observing the field `target` for a minute, with a
/// 1 Jy point at its centre predicted when `point` is given, then `steps`;
/// MAIN read back.
fn simulate(
    path: &Path,
    stokes: &str,
    point: Option<&Path>,
    steps: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
) -> Vec<Row> {
    let mut sm = session(path, "ALMA", stokes, resources(2, 2 << 30));
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    if let Some(point) = point {
        sm.predict(&PredictSpec {
            images: Vec::new(),
            component_list: Some(point.to_path_buf()),
            incremental: false,
        })
        .expect("predict");
    }
    steps(&mut sm).expect("the steps");
    sm.close().expect("close");
    read_main(path).0
}

fn point(directory: &Path) -> std::path::PathBuf {
    let path = directory.join("point.cl");
    write_point_list(&path, CENTRE_RAD, 1.0);
    path
}

/// Rows of one integration, autocorrelations included.
const INTEGRATION_ROWS: usize = ANTENNAS * (ANTENNAS + 1) / 2;

/// Gains are antenna-based, in both modes: on a 1 Jy point at the phase
/// centre each integration's first correlation is `g_p·conj(g_q)`, so
/// phases close around triangles and amplitudes around quadrilaterals,
/// and an autocorrelation is `|g_p|²`. Every antenna has its gains, the
/// last one included (CASA's autocorrelation row leaves it unit gain).
#[test]
fn gains_are_antenna_based() {
    for mode in ["fbm", "random"] {
        gains_are_antenna_based_in(mode);
    }
}

fn gains_are_antenna_based_in(mode: &str) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let rows = simulate(
        &directory.path().join("gains.ms"),
        "XX YY",
        Some(&point),
        &|sm| {
            sm.set_gain(&GainSpec {
                mode: mode.to_string(),
                ..gain(0.05)
            })?;
            sm.corrupt()
        },
    );
    assert_eq!(rows.len() % INTEGRATION_ROWS, 0);
    let mut moved = 0;
    for integration in rows.as_chunks::<INTEGRATION_ROWS>().0 {
        let v = |p: usize, q: usize| {
            let row = integration
                .iter()
                .find(|row| row.antennas == [p as u32, q as u32])
                .expect("a baseline");
            row.data[0]
        };
        for p in 0..ANTENNAS {
            let auto = v(p, p);
            assert!(auto.im.abs() < 1e-6 && auto.re > 0.0, "{auto}");
            moved += usize::from((auto.re - 1.0).abs() > 1e-3);
            for q in p + 1..ANTENNAS {
                assert!(
                    ((v(p, q).norm_sqr() / (auto.re * v(q, q).re)) - 1.0).abs() < 1e-4,
                    "|V_pq|² = V_pp·V_qq"
                );
                for r in q + 1..ANTENNAS {
                    let closure = v(p, q) * v(q, r) * v(p, r).conj();
                    assert!(closure.arg().abs() < 1e-4, "phase closure {closure}");
                }
            }
        }
    }
    assert!(
        moved > rows.len() / INTEGRATION_ROWS * (ANTENNAS - 1),
        "{mode}: the gains move"
    );
}

/// The noise of a `corrupt` follows its gains (`collapseForSim`), whether
/// a fixed σ or a system temperature's (which scales the noise alone): DATA
/// is the gained model plus the noise a session of the noise alone writes,
/// bit for bit.
#[test]
fn the_noise_of_a_corrupt_follows_its_gains() {
    for noise in [
        simple_noise(),
        NoiseSpec::tsys_manual(TsysParameters::default()),
    ] {
        the_noise_follows_the_gains(&noise);
    }
}

fn the_noise_follows_the_gains(noise_spec: &NoiseSpec) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let both = simulate(&path("both.ms"), "XX YY", Some(&point), &|sm| {
        sm.set_gain(&gain(0.05))?;
        sm.set_noise(noise_spec)?;
        sm.corrupt()
    });
    let gained = simulate(&path("gained.ms"), "XX YY", Some(&point), &|sm| {
        sm.set_gain(&gain(0.05))?;
        sm.corrupt()
    });
    let noise = simulate(&path("noise.ms"), "XX YY", None, &|sm| {
        sm.set_noise(noise_spec)?;
        sm.corrupt()
    });
    let expected: Vec<Vec<Complex32>> = gained
        .iter()
        .zip(&noise)
        .map(|(gained, noise)| {
            gained
                .data
                .iter()
                .zip(&noise.data)
                .map(|(g, n)| g + n)
                .collect()
        })
        .collect();
    assert_eq!(bits(&data(&both)), bits(&expected));
}

/// Leakage moves only the cross hands of four correlations (CASA's
/// linearised `D`), and CASA's `JonesGenLin` raises on two.
#[test]
fn leakage_moves_the_cross_hands_and_needs_them() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let rows = simulate(
        &directory.path().join("leak.ms"),
        "XX XY YX YY",
        Some(&point),
        &|sm| {
            sm.set_leakage(&leakage())?;
            sm.corrupt()
        },
    );
    let one = Complex32::new(1.0, 0.0);
    let mut cross = 0.0_f32;
    for row in &rows {
        for sample in row.data.as_chunks::<4>().0 {
            assert_eq!([sample[0], sample[3]], [one, one], "the parallel hands");
            cross = cross.max(sample[1].norm()).max(sample[2].norm());
        }
    }
    assert!(cross > 0.01, "the cross hands leak: {cross}");

    let mut sm = session(
        &directory.path().join("two.ms"),
        "ALMA",
        "XX YY",
        resources(2, 2 << 30),
    );
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.set_leakage(&leakage()).expect("setleakage");
    let refused = sm.corrupt().expect_err("leakage without the cross hands");
    assert_eq!(refused.outcome(), CasaOutcome::Raises);
    assert!(refused.message().contains("JonesGenLin"), "{refused}");
}

/// A leakage is drawn once per spectral window for the whole run (CASA's
/// infinite solution interval spans the scans): a later scan's cross hands
/// repeat the first's.
#[test]
fn leakage_holds_through_the_scans() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = directory.path().join("scans.ms");
    let mut sm = session(&path, "ALMA", "XX XY YX YY", resources(2, 2 << 30));
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.observe(&scan("target", 120.0, 180.0))
        .expect("a second scan");
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_list: Some(point),
        incremental: false,
    })
    .expect("predict");
    sm.set_leakage(&leakage()).expect("setleakage");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
    let rows = read_main(&path).0;
    let (first, later) = rows.split_at(rows.len() / 2);
    assert_eq!(first.len(), later.len());
    for (first, later) in first.iter().zip(later) {
        assert_eq!(first.antennas, later.antennas);
        assert_eq!(bits([&first.data]), bits([&later.data]));
    }
}

/// One `corrupt` applies leakage before gains, from the sky side in, as
/// CASA's `collapseForSim` does: on a polarised point (`U` makes XY
/// nonzero), `XY_both = (XY_G / XY_model) · XY_D` within float rounding,
/// which gains applied first would not give. Draws are keyed by their
/// purpose, so the three sessions share their gains and leakages.
#[test]
fn leakage_comes_before_gains() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = directory.path().join("polarised.cl");
    write_stokes_point_list(&point, CENTRE_RAD, [1.0, 0.1, 0.3, 0.0]);
    let path = |name: &str| directory.path().join(name);
    let run = |name: &str, gains: bool, leaks: bool| {
        simulate(&path(name), "XX XY YX YY", Some(&point), &|sm| {
            if leaks {
                sm.set_leakage(&leakage())?;
            }
            if gains {
                sm.set_gain(&gain(0.05))?;
            }
            sm.corrupt()
        })
    };
    let model = run("model.ms", false, false);
    let gained = run("gained.ms", true, false);
    let leaked = run("leaked.ms", false, true);
    let both = run("both.ms", true, true);
    let mut worst = 0.0_f32;
    for (((model, gained), leaked), both) in model.iter().zip(&gained).zip(&leaked).zip(&both) {
        for sample in 0..model.data.len() / 4 {
            for hand in [1, 2] {
                let index = 4 * sample + hand;
                let expected = gained.data[index] / model.data[index] * leaked.data[index];
                worst = worst.max((both.data[index] - expected).norm() / expected.norm());
            }
        }
    }
    assert!(worst < 1e-5, "D then G: worst relative error {worst}");
}

/// `reset` removes the gains and leakage: a later `corrupt` leaves DATA as
/// predicted.
#[test]
fn reset_removes_the_gains() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let reset = simulate(&path("reset.ms"), "XX XY YX YY", Some(&point), &|sm| {
        sm.set_gain(&gain(0.05))?;
        sm.set_leakage(&leakage())?;
        sm.reset();
        sm.corrupt()
    });
    let model = simulate(&path("model.ms"), "XX XY YX YY", Some(&point), &|_| Ok(()));
    assert_eq!(bits(&data(&reset)), bits(&data(&model)));
}

/// What `setgain`, `setleakage` and `corrupt` refuse, as CASA does or as
/// not yet implemented.
#[test]
fn gains_and_leakage_refuse_what_casa_or_casa_rs_cannot_do() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let mut sm = session(
        &directory.path().join("refused.ms"),
        "ALMA",
        "XX YY",
        resources(2, 2 << 30),
    );
    let refusal = |result: SimulatorResult<()>| {
        let error = result.expect_err("refused");
        (error.outcome(), error.message().to_string())
    };
    let (outcome, message) = refusal(sm.set_gain(&gain(0.05)));
    assert_eq!(
        outcome,
        CasaOutcome::ReturnsFalse,
        "before observe: {message}"
    );
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    for (mode, expected) in [
        ("table", CasaOutcome::ReturnsFalse),
        ("drift", CasaOutcome::ReturnsFalse),
    ] {
        let (outcome, message) = refusal(sm.set_gain(&GainSpec {
            mode: mode.to_string(),
            ..gain(0.05)
        }));
        assert_eq!(outcome, expected, "mode {mode}: {message}");
    }
    let (outcome, _) = refusal(sm.set_gain(&GainSpec {
        table: "g.cal".to_string(),
        ..gain(0.05)
    }));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented, "a table to write");
    let (outcome, _) = refusal(sm.set_gain(&GainSpec {
        amplitude: Vec::new(),
        ..gain(0.05)
    }));
    assert_eq!(outcome, CasaOutcome::ReturnsFalse, "no amplitude");
    let (outcome, _) = refusal(sm.set_leakage(&LeakageSpec {
        table: "d.cal".to_string(),
        ..leakage()
    }));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented, "a table to write");

    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_list: Some(point.clone()),
        incremental: false,
    })
    .expect("predict");
    sm.set_gain(&gain(0.05)).expect("setgain");
    sm.corrupt().expect("corrupt");
    let (outcome, message) = refusal(sm.predict(&PredictSpec {
        images: Vec::new(),
        component_list: Some(point),
        incremental: true,
    }));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{message}");
    assert!(message.contains("incremental"), "{message}");
    sm.close().expect("close");
}
