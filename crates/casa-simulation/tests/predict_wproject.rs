// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict` through `WProjectFT` (`setoptions(ftmachine='wproject')`,
//! simulation Phase 2, #841).
//!
//! - The DATA `close` writes is the per-row reference's prediction through
//!   [`FtMachine::WProject`], bit for bit, with and without the beam. The
//!   reference pads by 1, so a session given `padding=1.5` matching it shows
//!   `WProjectFT` ignores `setoptions(padding=…)`, as CASA's simulator does.
//! - Autocorrelations predict zero (`WProjectFT` is built with `usezero`
//!   off).
//! - A later `setoptions(ftmachine='ft')` predicts through `GridFT` again.
//! - `close` predicts W on the CPU when the backend is `auto` on a host with
//!   a Metal device; an explicit `metal` backend is refused at `predict`.
//! - Fewer than two planes are refused: one plane as CASA's
//!   `WPConvFunc` refuses it, none or fewer raised where CASA's simulator
//!   crashes. More than 65,536, the planes casa-rs's kernel key indexes,
//!   are raised.
//!
//! Agreement with CASA's own `sm.predict` through `WProjectFT` is measured
//! by `tools/simulation/compare_sm_ms.py` on `sm_predict_scenarios.json`.

mod predict_support;

use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_simulation::predict::{FtMachine, ModelPredictor, PredictOptions, PrimaryBeam, SkyModel};
use casa_simulation::{
    BackendRequest, CasaOutcome, PredictionOptions, SessionResources, Simulator, SimulatorResult,
    VoltagePatternSpec,
};
use casa_types::Correlation;
use num_complex::Complex32;
use predict_support::{
    CENTRE_RAD, bits, data, extended_model, predict_spec, read_main, reference_through, resources,
    scan, session,
};

const PLANES: u32 = 16;

fn wproject(planes: i64, padding: f64) -> PredictionOptions {
    PredictionOptions {
        ftmachine: "wproject".to_string(),
        padding,
        w_planes: planes,
        ..PredictionOptions::default()
    }
}

/// Predict `model` into two scans of `session` after `options`, and close.
fn predicted(
    mut sm: Simulator,
    model: &Path,
    options: &[PredictionOptions],
    beam: bool,
) -> SimulatorResult<()> {
    sm.observe(&scan("target", 0.0, 60.0))?;
    sm.observe(&scan("offset", 60.0, 120.0))?;
    if beam {
        sm.set_vp(&VoltagePatternSpec::default())?;
    }
    for option in options {
        sm.set_options(option)?;
    }
    sm.predict(&predict_spec(model, false))?;
    sm.close()
}

fn refusal(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.message().to_string())
}

#[test]
fn the_session_projects_w_as_the_row_predictor_does() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    for (stokes, correlations, beam) in [
        ("XX YY", vec![Correlation::XX, Correlation::YY], false),
        ("RR LL", vec![Correlation::RR, Correlation::LL], true),
    ] {
        let path = directory.path().join(format!("{stokes}-{beam}.ms"));
        let sm = session(&path, "ALMA", stokes, resources(3, 2 << 30));
        predicted(sm, &model, &[wproject(i64::from(PLANES), 1.5)], beam).expect("predicted");
        let (rows, _) = read_main(&path);
        let expected = reference_through(
            &rows,
            &model,
            &correlations,
            beam.then_some(PrimaryBeam::ALMA),
            FtMachine::WProject(PLANES),
        );
        assert_eq!(
            bits(&data(&rows)),
            bits(&expected),
            "{stokes}: the session's DATA is the W reference's"
        );
        let autocorrelations = rows
            .iter()
            .filter(|row| row.antennas[0] == row.antennas[1])
            .collect::<Vec<_>>();
        assert!(!autocorrelations.is_empty(), "{stokes}: autocorrelations");
        for row in autocorrelations {
            assert!(
                row.data
                    .iter()
                    .all(|value| *value == Complex32::new(0.0, 0.0)),
                "{stokes}: an autocorrelation predicts zero: {row:?}"
            );
        }
        assert!(
            rows.iter()
                .filter(|row| row.antennas[0] != row.antennas[1])
                .all(|row| row.data.iter().any(|value| value.norm() > 0.0)),
            "{stokes}: every cross-correlation predicts the model"
        );
    }
}

#[test]
fn setoptions_ft_after_wproject_predicts_through_gridft() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let path = directory.path().join("ft.ms");
    let sm = session(&path, "ALMA", "XX YY", resources(2, 2 << 30));
    predicted(
        sm,
        &model,
        &[wproject(8, 1.3), PredictionOptions::default()],
        false,
    )
    .expect("predicted");
    let (rows, _) = read_main(&path);
    let correlations = [Correlation::XX, Correlation::YY];
    let gridft = reference_through(
        &rows,
        &model,
        &correlations,
        None,
        FtMachine::Grid(casa_imaging_operator::PaddingFactor::SIMULATOR),
    );
    assert_eq!(bits(&data(&rows)), bits(&gridft), "GridFT again");
    let w = reference_through(&rows, &model, &correlations, None, FtMachine::WProject(8));
    assert_ne!(bits(&gridft), bits(&w), "the two machines differ");
}

/// A pinned host claiming a Metal device, with `backend`.
fn metal_host(backend: BackendRequest) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: true,
        },
        ResourcePolicy::Explicit {
            workers: 2,
            memory: 2 << 30,
        },
        backend,
    )
    .expect("resources")
}

#[test]
fn w_projection_predicts_on_the_cpu_and_refuses_an_explicit_metal_backend() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    // `auto` on a Metal host: `close` predicts W on the CPU, bit for bit as
    // the CPU reference (Metal's sums differ in the last bits, and a host
    // without the device it claims fails on it).
    let path = directory.path().join("auto.ms");
    let sm = session(&path, "ALMA", "XX YY", metal_host(BackendRequest::Auto));
    predicted(sm, &model, &[wproject(8, 1.3)], false).expect("predicted");
    let (rows, _) = read_main(&path);
    let expected = reference_through(
        &rows,
        &model,
        &[Correlation::XX, Correlation::YY],
        None,
        FtMachine::WProject(8),
    );
    assert_eq!(
        bits(&data(&rows)),
        bits(&expected),
        "auto predicts W on the CPU"
    );

    let sm = session(
        &directory.path().join("metal.ms"),
        "ALMA",
        "XX YY",
        metal_host(BackendRequest::Metal),
    );
    let (outcome, message) = refusal(predicted(sm, &model, &[wproject(8, 1.3)], false));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    assert!(message.contains("Metal"), "{message}");
}

/// More planes than casa-rs's kernel key indexes (65,536) are refused
/// before anything is prepared, by the session and by the row predictor;
/// the row predictor panicked past a 16-bit plane index (#865 review).
#[test]
fn more_planes_than_the_kernel_key_indexes_are_refused() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let mut sm = session(
        &directory.path().join("planes.ms"),
        "ALMA",
        "XX YY",
        resources(2, 2 << 30),
    );
    sm.observe(&scan("target", 0.0, 20.0)).expect("observe");
    sm.set_options(&wproject(65_537, 1.3))
        .expect("setoptions records, as CASA's does");
    let (outcome, message) = refusal(sm.predict(&predict_spec(&model, false)));
    assert_eq!(outcome, CasaOutcome::Raises, "{message}");
    assert!(message.contains("65,536"), "{message}");
    let sky = SkyModel::open(&model).expect("model");
    let options = |planes| PredictOptions {
        ft_machine: FtMachine::WProject(planes),
        ..PredictOptions::default()
    };
    let correlations = [Correlation::XX, Correlation::YY];
    let refused = ModelPredictor::new(&sky, &correlations, CENTRE_RAD, &options(65_537))
        .err()
        .expect("refused");
    assert!(refused.to_string().contains("65,536"), "{refused}");
    ModelPredictor::new(&sky, &correlations, CENTRE_RAD, &options(16)).expect("16 planes");
}

#[test]
fn fewer_than_two_planes_are_refused() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    for (planes, expected) in [
        (1, CasaOutcome::ReturnsFalse),
        (0, CasaOutcome::Raises),
        (-1, CasaOutcome::Raises),
    ] {
        let path = directory.path().join(format!("planes{planes}.ms"));
        let mut sm = session(&path, "ALMA", "XX YY", resources(2, 2 << 30));
        sm.observe(&scan("target", 0.0, 20.0)).expect("observe");
        sm.set_options(&wproject(planes, 1.3))
            .expect("setoptions records, as CASA's does");
        let (outcome, message) = refusal(sm.predict(&predict_spec(&model, false)));
        assert_eq!(outcome, expected, "{planes}: {message}");
        if planes == 1 {
            assert!(
                message.contains("Convolution function integral is not positive"),
                "{message}"
            );
        }
        // The refused predict leaves DATA zero.
        sm.close().expect("close");
        let (rows, _) = read_main(&path);
        assert!(
            rows.iter().all(|row| row
                .data
                .iter()
                .all(|value| *value == Complex32::new(0.0, 0.0))),
            "{planes}: nothing predicted"
        );
    }
}
