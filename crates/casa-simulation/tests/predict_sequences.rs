// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict` in sequences and its refusals (simulation Phase 1d, #841).
//!
//! In CASA every `predict` and `corrupt` rewrites the DATA of the rows
//! observed so far: a `predict` replaces it (or adds to it when
//! `incremental`), a `corrupt` adds noise. The session folds the steps at
//! `close`, so each scan's DATA is that of CASA's sequence:
//! - a replacing `predict` overwrites every earlier step, noise included;
//! - an incremental one adds to what is there;
//! - a scan observed after a step is not touched by it.
//!
//! Each case is checked against sessions that predict or corrupt alone, bit
//! for bit (adding single-precision values is exact in either order).
//!
//! The refusals follow CASA's outcomes (`False`, raised, or not yet
//! implemented), and a refused `predict` leaves the plan as it was.

mod predict_support;

use std::path::{Path, PathBuf};

use casa_simulation::{
    CasaOutcome, NoiseSpec, PredictSpec, PredictionOptions, Simulator, SimulatorResult,
    VoltagePatternSpec,
};
use num_complex::Complex32;
use predict_support::{
    bits, cube_model, data, extended_model, predict_spec, read_main, resources, scan, session,
};

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: sigma_jy,
        on_the_fly: true,
    }
}

/// Write a session of two scans: `first` runs after the first scan is
/// observed, `second` after the second.
fn write(
    path: &Path,
    first: impl FnOnce(&mut Simulator) -> SimulatorResult<()>,
    second: impl FnOnce(&mut Simulator) -> SimulatorResult<()>,
) -> Vec<Vec<Complex32>> {
    let mut sm = session(path, "ALMA", "XX YY", resources(3, 2 << 30));
    sm.observe(&scan("target", 0.0, 50.0)).expect("observe");
    first(&mut sm).expect("first steps");
    sm.observe(&scan("offset", 50.0, 80.0)).expect("observe");
    second(&mut sm).expect("second steps");
    sm.close().expect("close");
    data(&read_main(path).0)
}

fn nothing(_: &mut Simulator) -> SimulatorResult<()> {
    Ok(())
}

fn corrupt(session: &mut Simulator) -> SimulatorResult<()> {
    session.set_noise(&simple_noise(0.2))?;
    session.corrupt()
}

/// Rows of the first scan: 5 integrations with autocorrelations.
const FIRST_SCAN_ROWS: usize = 5 * predict_support::ANTENNAS * (predict_support::ANTENNAS + 1) / 2;

/// The element-wise sum of `a` and `b`.
fn sum(a: &[Vec<Complex32>], b: &[Vec<Complex32>]) -> Vec<Vec<Complex32>> {
    a.iter()
        .zip(b)
        .map(|(a, b)| a.iter().zip(b).map(|(a, b)| a + b).collect())
        .collect()
}

/// `cells` with the rows from `from` on zeroed.
fn first_scan_only(cells: &[Vec<Complex32>]) -> Vec<Vec<Complex32>> {
    cells
        .iter()
        .enumerate()
        .map(|(row, cell)| {
            if row < FIRST_SCAN_ROWS {
                cell.clone()
            } else {
                vec![Complex32::new(0.0, 0.0); cell.len()]
            }
        })
        .collect()
}

#[test]
fn predict_and_corrupt_fold_as_casa_rewrites_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let a = extended_model(directory.path(), "a.image");
    let b = cube_model(directory.path(), "b.image");
    let predict = |model: &PathBuf, incremental: bool| {
        let spec = predict_spec(model, incremental);
        move |session: &mut Simulator| session.predict(&spec)
    };
    // The parts: each model predicted into both scans, and the noise of one
    // corrupt of the first scan.
    let all_a = write(&path("all-a.ms"), nothing, predict(&a, false));
    let all_b = write(&path("all-b.ms"), nothing, predict(&b, false));
    let noise = write(&path("noise.ms"), corrupt, nothing);
    assert!(
        noise[FIRST_SCAN_ROWS..]
            .iter()
            .flatten()
            .all(|v| v.norm() == 0.0)
    );
    let a_first = first_scan_only(&all_a);

    // A replacing predict overwrites an earlier predict on every scan.
    let replaced = write(&path("replaced.ms"), predict(&a, false), predict(&b, false));
    assert_eq!(bits(&replaced), bits(&all_b), "replace");
    // An incremental predict adds to the scans an earlier predict covered.
    let added = write(&path("added.ms"), predict(&a, false), predict(&b, true));
    assert_same(&added, &sum(&a_first, &all_b), "incremental");
    // Noise adds to the model it corrupts...
    let corrupted = write(
        &path("corrupted.ms"),
        |session| {
            session.predict(&predict_spec(&a, false))?;
            corrupt(session)
        },
        nothing,
    );
    assert_same(&corrupted, &sum(&a_first, &noise), "predict, corrupt");
    // ...a replacing predict wipes it...
    let wiped = write(&path("wiped.ms"), corrupt, predict(&a, false));
    assert_eq!(bits(&wiped), bits(&all_a), "corrupt, predict");
    // ...and an incremental predict keeps it.
    let kept = write(&path("kept.ms"), corrupt, predict(&a, true));
    assert_same(&kept, &sum(&noise, &all_a), "corrupt, incremental predict");
}

/// Every value of `actual` equals `expected`'s exactly; a zero equals a zero
/// of either sign, which adding a zero may change.
fn assert_same(actual: &[Vec<Complex32>], expected: &[Vec<Complex32>], what: &str) {
    assert_eq!(actual.len(), expected.len(), "{what}: rows");
    if let Some((row, (actual, expected))) = actual
        .iter()
        .zip(expected)
        .enumerate()
        .find(|(_, (actual, expected))| actual != expected)
    {
        panic!("{what}: row {row} is {actual:?}, expected {expected:?}");
    }
}

/// The outcome and message of a refused call.
fn refusal(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.message().to_string())
}

fn spec(images: &[&Path]) -> PredictSpec {
    PredictSpec {
        images: images.iter().map(|path| path.to_path_buf()).collect(),
        component_list: None,
        incremental: false,
    }
}

#[test]
fn predict_refuses_as_casa_does() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let mut sm = session(
        &directory.path().join("x.ms"),
        "ALMA",
        "XX YY",
        resources(2, 2 << 30),
    );
    // Before observe CASA has no rows to predict and returns False.
    let (outcome, message) = refusal(sm.predict(&spec(&[&model])));
    assert_eq!(outcome, CasaOutcome::ReturnsFalse);
    assert!(message.contains("zero selected rows"), "{message}");
    sm.observe(&scan("target", 0.0, 30.0)).expect("observe");
    // casatools' binding opens every named image first, and raises.
    let missing = directory.path().join("missing.image");
    assert_eq!(
        refusal(sm.predict(&spec(&[&missing]))),
        (
            CasaOutcome::Raises,
            format!("File {} does not exist.", missing.display())
        )
    );
    let not_an_image = directory.path().join("not-an-image");
    std::fs::write(&not_an_image, "x").expect("write");
    assert_eq!(
        refusal(sm.predict(&spec(&[&not_an_image]))),
        (
            CasaOutcome::Raises,
            format!("Unable to open image {}.", not_an_image.display())
        )
    );
    // No image: False.
    for images in [vec![], vec![PathBuf::new()]] {
        let (outcome, message) = refusal(sm.predict(&PredictSpec {
            images,
            ..PredictSpec::default()
        }));
        assert_eq!(outcome, CasaOutcome::ReturnsFalse);
        assert!(message.contains("No model images found"), "{message}");
    }
    // What CASA does and casa-rs does not yet.
    let (outcome, message) = refusal(sm.predict(&spec(&[&model, &model])));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    assert!(message.contains("Phase 2"), "{message}");
    // A component list that is not a readable table is CASA's False.
    let (outcome, message) = refusal(sm.predict(&PredictSpec {
        component_list: Some(directory.path().join("model.cl")),
        ..spec(&[&model])
    }));
    assert_eq!(outcome, CasaOutcome::ReturnsFalse);
    assert!(message.contains("not readable"), "{message}");
    sm.close().expect("close");
    let rows = read_main(&directory.path().join("x.ms")).0;
    assert!(
        rows.iter()
            .all(|row| row.data.iter().all(|v| v.norm() == 0.0))
    );
}

#[test]
fn setvp_setoptions_and_the_session_refuse_what_casa_rs_lacks() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    // VLA's default voltage pattern is not an Airy pattern casa-rs has.
    let mut vla = session(
        &directory.path().join("vla.ms"),
        "VLA",
        "RR LL",
        resources(2, 2 << 30),
    );
    vla.observe(&scan("target", 0.0, 20.0)).expect("observe");
    vla.set_vp(&VoltagePatternSpec::default())
        .expect("setvp records");
    let (outcome, message) = refusal(vla.predict(&spec(&[&model])));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    assert!(
        message.contains("VLA") && message.contains("Phase 2"),
        "{message}"
    );
    vla.set_vp(&VoltagePatternSpec {
        dovp: false,
        ..VoltagePatternSpec::default()
    })
    .expect("setvp");
    vla.predict(&spec(&[&model])).expect("no beam: predicted");
    // A voltage-pattern table, other FTMachines and grid functions.
    let (outcome, _) = refusal(vla.set_vp(&VoltagePatternSpec {
        use_default_vp: false,
        vp_table: "vp.tab".to_string(),
        ..VoltagePatternSpec::default()
    }));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    for ftmachine in ["mosaic", "WPROJECT", "sd"] {
        let (outcome, message) = refusal(vla.set_options(&PredictionOptions {
            ftmachine: ftmachine.to_string(),
            ..PredictionOptions::default()
        }));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{ftmachine}");
        assert!(message.contains("Phase 2"), "{message}");
    }
    let (outcome, _) = refusal(vla.set_options(&PredictionOptions {
        grid_function: "BOX".to_string(),
        ..PredictionOptions::default()
    }));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    vla.close().expect("close");

    // Three correlations raise, as CASA's visibility vectors do.
    let mut three = session(
        &directory.path().join("3.ms"),
        "ALMA",
        "XX XY YY",
        resources(2, 2 << 30),
    );
    three.observe(&scan("target", 0.0, 20.0)).expect("observe");
    let (outcome, message) = refusal(three.predict(&spec(&[&model])));
    assert_eq!(outcome, CasaOutcome::Raises);
    assert!(message.starts_with("No such VisVector type"), "{message}");

    // A near-field distance on the last field CASA defines.
    let mut near = session(
        &directory.path().join("near.ms"),
        "ALMA",
        "XX YY",
        resources(2, 2 << 30),
    );
    near.observe(&scan("target", 0.0, 20.0)).expect("observe");
    near.set_field(
        "comet",
        &casa_types::measures::direction::MDirection::from_angles(
            predict_support::CENTRE_RAD[0],
            predict_support::CENTRE_RAD[1],
            casa_types::measures::direction::DirectionRef::J2000,
        ),
        "",
        3.0e11,
    )
    .expect("setfield");
    assert_eq!(
        refusal(near.predict(&spec(&[&model]))).0,
        CasaOutcome::NotYetImplemented
    );
}

#[test]
fn a_refused_predict_leaves_the_plan_as_it_was() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let expected = write(&directory.path().join("plain.ms"), nothing, |session| {
        session.predict(&predict_spec(&model, false))
    });
    let refused = write(&directory.path().join("refused.ms"), nothing, |session| {
        session.predict(&predict_spec(&model, false))?;
        let missing = directory.path().join("missing.image");
        assert!(session.predict(&spec(&[&missing])).is_err());
        assert!(session.predict(&spec(&[&model, &model])).is_err());
        assert!(session.predict(&PredictSpec::default()).is_err());
        Ok(())
    });
    assert_eq!(bits(&refused), bits(&expected));
    // A padding below 1 is ignored, as CASA ignores it.
    let padded = write(&directory.path().join("padding.ms"), nothing, |session| {
        session.set_options(&PredictionOptions {
            padding: 0.5,
            ..PredictionOptions::default()
        })?;
        session.predict(&predict_spec(&model, false))
    });
    assert_eq!(bits(&padded), bits(&expected));
}

#[test]
fn model_grids_beyond_the_budget_are_a_typed_refusal() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = cube_model(directory.path(), "cube.image");
    let path = directory.path().join("small.ms");
    // 256 MiB: the process caches' 80 MiB fit, the grids do not.
    let mut sm = session(&path, "ALMA", "XX YY", resources(2, 256 << 20));
    sm.observe(&scan("target", 0.0, 20.0)).expect("observe");
    // A padding of 60 makes four planes of a 3840² f32 grid: 470 MB.
    sm.set_options(&PredictionOptions {
        padding: 60.0,
        ..PredictionOptions::default()
    })
    .expect("setoptions");
    let error = sm
        .predict(&predict_spec(&model, false))
        .expect_err("over budget");
    assert_eq!(error.outcome(), CasaOutcome::Raises);
    let refusal = error
        .admission_refused()
        .expect("a typed admission refusal");
    assert_eq!(refusal.phase, "the simulator's model grids");
    assert!(refusal.required > refusal.available, "{refusal:?}");
    sm.close().expect("close");
    assert!(path.exists(), "the session writes on without the step");
}
