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

use casa_components::{
    ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
};
use casa_imaging_model::CorrelationType;
use casa_simulation::{
    CasaOutcome, NoiseSpec, ObserveManySpec, Pointing, PredictSpec, PredictionOptions, Simulator,
    SimulatorResult, VoltagePatternSpec,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use num_complex::Complex32;
use predict_support::{
    CENTRE_RAD, FREQUENCY_HZ, Row, bits, cube_model, data, extended_model, predict_spec, read_main,
    reference, resources, scan, session, write_point_list,
};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];

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

/// CASA's primary beam on a pointing `observemany` moved off its field is
/// centred where the first row of the field it visits points, and kept
/// there for the field's other rows (`BeamSkyJones`, #884): measured on
/// CASA 6.7.6, a field-first sequence keeps the beam on the field and a
/// moved-first one on the moved direction. casa-rs does not follow that
/// yet, so a beam over a model image, a component list or total power, and
/// the mosaic machine, refuse a moved pointing; each predicts it without.
#[test]
fn a_beam_on_a_moved_pointing_is_refused() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let points = directory.path().join("points.cl");
    write_points(&points);
    let list = PredictSpec {
        images: Vec::new(),
        component_list: Some(points),
        incremental: false,
    };
    let mosaic = PredictionOptions {
        ftmachine: "mosaic".to_string(),
        ..PredictionOptions::default()
    };
    let single_dish = PredictionOptions {
        ftmachine: "sd".to_string(),
        grid_function: "PB".to_string(),
        ..PredictionOptions::default()
    };
    let no_beam = VoltagePatternSpec {
        dovp: false,
        ..VoltagePatternSpec::default()
    };
    let cases = [
        ("an image under the beam", None, spec(&[&model])),
        ("a list under the beam", None, list),
        (
            "total power under the beam",
            Some(&single_dish),
            spec(&[&model]),
        ),
    ];
    for (index, (what, options, predict)) in cases.into_iter().enumerate() {
        let mut sm = moved_pointing(&directory.path().join(format!("beam-{index}.ms")));
        if let Some(options) = options {
            sm.set_options(options).expect("setoptions");
        }
        sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        let (outcome, message) = refusal(sm.predict(&predict));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{what}");
        assert!(
            message.contains("pointed away") && message.contains("#884"),
            "{what}: {message}"
        );
        sm.set_vp(&no_beam).expect("setvp");
        sm.predict(&predict)
            .unwrap_or_else(|error| panic!("{what} without the beam: {error}"));
    }
    let mut sm = moved_pointing(&directory.path().join("mosaic.ms"));
    sm.set_options(&mosaic).expect("setoptions");
    let (outcome, message) = refusal(sm.predict(&spec(&[&model])));
    assert_eq!(
        outcome,
        CasaOutcome::NotYetImplemented,
        "the mosaic machine"
    );
    assert!(message.contains("pointed away"), "{message}");
    sm.set_options(&PredictionOptions::default())
        .expect("setoptions");
    sm.predict(&spec(&[&model]))
        .expect("GridFT predicts the moved pointing");
}

/// An ALMA session observing `target` and then a pointing of it moved
/// 1.7″ east, to `path`.
fn moved_pointing(path: &Path) -> Simulator {
    let mut sm = session(path, "ALMA", "XX YY", resources(2, 2 << 30));
    let base = scan("target", 0.0, 20.0);
    let pointing = |start_s: f64, direction: Option<MDirection>| Pointing {
        source: "target".to_string(),
        start_s,
        stop_s: start_s + 20.0,
        direction,
    };
    let moved = MDirection::from_angles(CENTRE_RAD[0] + 1.0e-5, CENTRE_RAD[1], DirectionRef::J2000);
    sm.observe_many(&ObserveManySpec {
        pointings: vec![pointing(0.0, None), pointing(20.0, Some(moved))],
        spectral_window: base.spectral_window,
        add_observation: false,
        state: base.state,
        observer: base.observer,
        project: base.project,
    })
    .expect("observemany");
    sm
}

/// A pointing `observemany` moved off its field is phased on its field, as
/// CASA's `VisBuffer` phases it from FIELD's PHASE_DIR (#884): its DATA is
/// the per-row reference's at the field, with its UVW, which are a field's
/// at the moved direction, as `NewMSSimulator` computes them. A field there
/// is phased there, and predicts other DATA.
#[test]
fn a_moved_pointing_is_phased_on_its_field() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let (pointed, field) = moved_and_field_there(directory.path(), &model, None);
    let expected = reference(&pointed, &model, &XX_YY, None);
    assert_eq!(
        bits(&data(&pointed)),
        bits(&expected),
        "the moved pointing is phased on its field"
    );
    assert_ne!(
        bits(&data(&pointed)),
        bits(&data(&field)),
        "a field at the moved direction is phased there"
    );
}

/// Total power samples a moved pointing at its own direction, not its
/// field's (#874, after #861's moved pointings): its DATA is that of a
/// field at the moved direction.
#[test]
fn total_power_samples_a_moved_pointing_at_its_own_direction() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let (pointed, field) = moved_and_field_there(
        directory.path(),
        &model,
        Some(&PredictionOptions {
            ftmachine: "sd".to_string(),
            grid_function: "PB".to_string(),
            ..PredictionOptions::default()
        }),
    );
    assert!(
        pointed
            .iter()
            .flat_map(|row| &row.data)
            .any(|v| v.norm() > 0.0),
        "the model is predicted"
    );
    assert_eq!(
        bits(pointed.iter().map(|row| &row.data)),
        bits(field.iter().map(|row| &row.data)),
        "the moved pointing's DATA"
    );
}

/// Sessions of two pointings predicting `model` after `options`: the second
/// pointing moved off its field, and the second at a field at the moved
/// direction. Their UVW agree, as `NewMSSimulator` computes a moved
/// pointing's at the moved direction.
fn moved_and_field_there(
    directory: &Path,
    model: &Path,
    options: Option<&PredictionOptions>,
) -> (Vec<Row>, Vec<Row>) {
    let moved = MDirection::from_angles(CENTRE_RAD[0] + 1.0e-5, CENTRE_RAD[1], DirectionRef::J2000);
    let observed = |name: &str, second: Pointing| {
        let path = directory.join(name);
        let mut sm = session(&path, "ALMA", "XX YY", resources(2, 2 << 30));
        sm.set_field("moved", &moved, "", 0.0).expect("setfield");
        let base = scan("target", 0.0, 20.0);
        sm.observe_many(&ObserveManySpec {
            pointings: vec![
                Pointing {
                    source: "target".to_string(),
                    start_s: 0.0,
                    stop_s: 20.0,
                    direction: None,
                },
                second,
            ],
            spectral_window: base.spectral_window,
            add_observation: false,
            state: base.state,
            observer: base.observer,
            project: base.project,
        })
        .expect("observemany");
        if let Some(options) = options {
            sm.set_options(options).expect("setoptions");
        }
        sm.predict(&spec(&[model])).expect("predict");
        sm.close().expect("close");
        read_main(&path).0
    };
    let pointed = observed(
        "pointed.ms",
        Pointing {
            source: "target".to_string(),
            start_s: 20.0,
            stop_s: 40.0,
            direction: Some(moved.clone()),
        },
    );
    let field = observed(
        "field.ms",
        Pointing {
            source: "moved".to_string(),
            start_s: 20.0,
            stop_s: 40.0,
            direction: None,
        },
    );
    assert_eq!(pointed.len(), field.len(), "rows");
    for (row, (pointed, field)) in pointed.iter().zip(&field).enumerate() {
        assert_eq!(pointed.uvw_m, field.uvw_m, "row {row}: the same UVW");
    }
    (pointed, field)
}

/// A list of a point at the centre and one 3″ north of it, written to
/// `path`.
fn write_points(path: &Path) {
    let mut list = ComponentList::new();
    for (north_rad, flux) in [(0.0, 1.0), (1.5e-5, 0.5)] {
        list.push(SkyComponent::new(
            Flux::stokes_jy(flux, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(
                CENTRE_RAD[0],
                CENTRE_RAD[1] + north_rad,
                DirectionRef::J2000,
            )),
            SpectralModel::new(
                MFrequency::new(FREQUENCY_HZ, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        ));
    }
    list.write_table(path).expect("write the list");
}

/// A component list on a pointing `observemany` moved off its field is
/// phased on the field, as CASA predicts it (#884, the
/// `components-moved-pointing` scenario): a point at the field's direction
/// predicts its flux at zero phase in both pointings, though the moved
/// pointing's UVW are the moved direction's.
#[test]
fn components_are_predicted_at_a_moved_pointing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let points = directory.path().join("points.cl");
    write_point_list(&points, CENTRE_RAD, 1.0);
    let path = directory.path().join("moved.ms");
    let mut sm = moved_pointing(&path);
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_list: Some(points),
        incremental: false,
    })
    .expect("predict");
    sm.close().expect("close");
    let rows = read_main(&path).0;
    assert!(!rows.is_empty());
    for (row, cells) in rows.iter().enumerate() {
        for value in &cells.data {
            assert!(
                (value.re - 1.0).abs() < 1e-6 && value.im.abs() < 1e-6,
                "row {row}: the point at the field: {value}"
            );
        }
    }
}

/// CASA squints the VLA's and the EVLA's beams for a component list, moving
/// the RR and LL beams apart; casa-rs's patterns are unsquinted, so it
/// refuses the list under them with squint, and predicts it without.
#[test]
fn components_under_a_squinting_beam_are_refused_unless_squint_is_off() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let points = directory.path().join("points.cl");
    write_points(&points);
    for telescope in ["VLA", "EVLA"] {
        let mut sm = session(
            &directory.path().join(format!("{telescope}.ms")),
            telescope,
            "RR LL",
            resources(2, 2 << 30),
        );
        sm.observe(&scan("target", 0.0, 20.0)).expect("observe");
        let list = PredictSpec {
            images: Vec::new(),
            component_list: Some(points.clone()),
            incremental: false,
        };
        sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        let (outcome, message) = refusal(sm.predict(&list));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{telescope}");
        assert!(message.contains("squint"), "{telescope}: {message}");
        sm.set_vp(&VoltagePatternSpec {
            do_squint: false,
            ..VoltagePatternSpec::default()
        })
        .expect("setvp");
        sm.predict(&list).expect("unsquinted: predicted");
        sm.close().expect("close");
    }
}

#[test]
fn setvp_setoptions_and_the_session_refuse_what_casa_rs_lacks() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    // The SMA's default voltage pattern is not one casa-rs has.
    let mut vla = session(
        &directory.path().join("sma.ms"),
        "SMA",
        "RR LL",
        resources(2, 2 << 30),
    );
    vla.observe(&scan("target", 0.0, 20.0)).expect("observe");
    vla.set_vp(&VoltagePatternSpec::default())
        .expect("setvp records");
    let (outcome, message) = refusal(vla.predict(&spec(&[&model])));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    assert!(
        message.contains("SMA") && message.contains("#841"),
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
    let (outcome, message) = refusal(vla.set_options(&PredictionOptions {
        ftmachine: "both".to_string(),
        ..PredictionOptions::default()
    }));
    assert_eq!(outcome, CasaOutcome::NotYetImplemented);
    assert!(message.contains("#841"), "{message}");
    // SDGrid's Gaussian kernels.
    for grid_function in ["GAUSS", "GJINC"] {
        let (outcome, message) = refusal(vla.set_options(&PredictionOptions {
            ftmachine: "SD".to_string(),
            grid_function: grid_function.to_string(),
            ..PredictionOptions::default()
        }));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{grid_function}");
        assert!(message.contains("'PB', 'BOX' and 'SF' are"), "{message}");
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
