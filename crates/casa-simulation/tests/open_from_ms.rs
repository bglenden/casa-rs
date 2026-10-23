// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.openfromms` (#841 Phase 2): a session on a MeasurementSet that
//! exists, whose `predict` and `corrupt` rewrite its rows at `close`.
//!
//! The laws are two-stage: one session observes, takes DATA steps and
//! closes; another observes and closes, then `openfromms` takes the same
//! steps and closes. On the CPU both hold the same DATA, bit for bit: what
//! `openfromms` reads back (antennas, telescope, channels, correlations,
//! fields, phase centres, rows) is all the steps take. Agreement with CASA
//! is the `openfromms` scenarios' (`tools/simulation`).

mod predict_support;

use std::path::Path;

use casa_simulation::{
    CasaOutcome, NoiseSpec, PredictionOptions, Simulator, SimulatorOptions, SimulatorResult,
};
use casa_types::measures::{EpochRef, MEpoch};
use predict_support::{
    CHANNELS, REFERENCE_MJD, bits, data, extended_model, predict_spec, read_main, resources, scan,
    session, session_with_dishes,
};

/// The options [`session`] opens with.
fn options() -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(2, 2 << 30),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// An eight-antenna ALMA session observing the model's centre and then the
/// `offset` field.
fn observed(path: &Path) -> Simulator {
    let mut sm = session(path, "ALMA", "XX YY", resources(2, 2 << 30));
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.observe(&scan("offset", 60.0, 120.0)).expect("observe");
    sm
}

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: sigma_jy,
        on_the_fly: true,
    }
}

/// `sm.setnoise(simplenoise=…)` then `sm.corrupt()`.
fn corrupt(sm: &mut Simulator) -> SimulatorResult<()> {
    sm.set_noise(&simple_noise(0.25))?;
    sm.corrupt()
}

/// The MeasurementSet `make` observes in `directory/one.ms`, closed with
/// `first` and `then` taken in one session, and in `directory/two.ms`,
/// closed with `first` and then reopened with `openfromms` to take `then`.
fn two_stage(
    directory: &Path,
    make: &dyn Fn(&Path) -> Simulator,
    first: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
    then: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
) -> (Vec<predict_support::Row>, Vec<predict_support::Row>) {
    let one = directory.join("one.ms");
    let mut sm = make(&one);
    first(&mut sm).expect("the first steps");
    then(&mut sm).expect("the later steps");
    sm.close().expect("close");
    let two = directory.join("two.ms");
    let mut sm = make(&two);
    first(&mut sm).expect("the first steps");
    sm.close().expect("close");
    let mut sm = Simulator::open_from_ms(&two, options()).expect("openfromms");
    then(&mut sm).expect("the later steps after openfromms");
    sm.close().expect("close the opened MeasurementSet");
    (read_main(&one).0, read_main(&two).0)
}

fn nothing(_: &mut Simulator) -> SimulatorResult<()> {
    Ok(())
}

/// A `predict` and a `corrupt` after `openfromms` write the DATA they write
/// in the session that observed.
#[test]
fn predict_and_corrupt_after_openfromms_write_the_same_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let steps = |sm: &mut Simulator| {
        sm.predict(&predict_spec(&model, false))?;
        corrupt(sm)
    };
    let (one, two) = two_stage(directory.path(), &observed, &nothing, &steps);
    assert!(
        one.iter()
            .any(|row| row.data.iter().any(|value| value.norm() > 0.0))
    );
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// A `corrupt` after `openfromms` adds its noise to the DATA on disk: a
/// prediction closed first, or the zeros `observe` wrote.
#[test]
fn a_corrupt_after_openfromms_adds_to_the_data_on_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let predict = |sm: &mut Simulator| sm.predict(&predict_spec(&model, false));
    let (one, two) = two_stage(directory.path(), &observed, &predict, &corrupt);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    let noise_only = tempfile::tempdir().expect("temporary directory");
    let (one, two) = two_stage(noise_only.path(), &observed, &nothing, &corrupt);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// A replacing `predict` after `openfromms` replaces the DATA on disk, and
/// an incremental one adds to it, to rounding: one session sums the
/// predictions within its predictor.
#[test]
fn a_predict_after_openfromms_replaces_or_adds_to_the_data_on_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let predict =
        |incremental| move |sm: &mut Simulator| sm.predict(&predict_spec(model, incremental));
    let (one, two) = two_stage(directory.path(), &observed, &corrupt, &predict(false));
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    let incremental = tempfile::tempdir().expect("temporary directory");
    let (one, two) = two_stage(
        incremental.path(),
        &observed,
        &predict(false),
        &predict(true),
    );
    let (mut difference, mut norm) = (0.0_f64, 0.0_f64);
    for (a, b) in one.iter().zip(&two) {
        for (x, y) in a.data.iter().zip(&b.data) {
            difference += f64::from((x - y).norm_sqr());
            norm += f64::from(x.norm_sqr());
        }
    }
    assert!(norm > 0.0);
    assert!(
        (difference / norm).sqrt() < 1e-6,
        "NRMS {}",
        (difference / norm).sqrt()
    );
}

/// The `wproject` machine, and the heterogeneous mosaic machine, predict
/// after `openfromms` as in the session that observed.
#[test]
fn wproject_and_the_mosaic_predict_after_openfromms_as_before() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let with = |ftmachine: &'static str| {
        move |sm: &mut Simulator| {
            sm.set_options(&PredictionOptions {
                ftmachine: ftmachine.to_string(),
                w_planes: 4,
                ..PredictionOptions::default()
            })?;
            sm.predict(&predict_spec(model, false))
        }
    };
    let wproject = directory.path().join("wproject");
    std::fs::create_dir(&wproject).expect("directory");
    let (one, two) = two_stage(&wproject, &observed, &nothing, &with("wproject"));
    assert_eq!(bits(&data(&one)), bits(&data(&two)));

    let heterogeneous = |path: &Path| {
        let mut sm = session_with_dishes(
            path,
            "ALMA",
            "XX YY",
            CHANNELS,
            &[12.0, 7.0, 12.0, 7.0, 12.0, 7.0, 12.0, 7.0],
            resources(2, 2 << 30),
        );
        sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
        sm.observe(&scan("offset", 60.0, 120.0)).expect("observe");
        sm
    };
    let mosaic = directory.path().join("mosaic");
    std::fs::create_dir(&mosaic).expect("directory");
    let (one, two) = two_stage(&mosaic, &heterogeneous, &nothing, &with("mosaic"));
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// What adds to the MeasurementSet is not yet implemented after
/// `openfromms`, and a session with no DATA step leaves it as it was.
#[test]
fn openfromms_refuses_additions_and_writes_nothing_without_a_step() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("observed.ms");
    observed(&path).close().expect("close");
    let before = read_main(&path).0;
    let mut sm = Simulator::open_from_ms(&path, options()).expect("openfromms");
    assert_eq!(sm.scan_count(), 2, "one scan per field");
    let refused = sm
        .observe(&scan("target", 120.0, 180.0))
        .expect_err("observe");
    assert_eq!(refused.outcome(), CasaOutcome::NotYetImplemented);
    assert!(
        refused.message().contains("observe after openfromms"),
        "{refused}"
    );
    sm.close().expect("close");
    assert_eq!(bits(&data(&read_main(&path).0)), bits(&data(&before)));
    let missing = Simulator::open_from_ms(directory.path().join("missing.ms"), options())
        .err()
        .expect("no MeasurementSet");
    assert_eq!(missing.outcome(), CasaOutcome::Raises);
}

fn write_points(path: &Path) {
    use casa_components::{
        ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
    };
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::frequency::{FrequencyRef, MFrequency};
    let mut list = ComponentList::new();
    for (north_rad, flux) in [(0.0, 1.0), (1.5e-5, 0.5)] {
        list.push(SkyComponent::new(
            Flux::stokes_jy(flux, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(
                predict_support::CENTRE_RAD[0],
                predict_support::CENTRE_RAD[1] + north_rad,
                DirectionRef::J2000,
            )),
            SpectralModel::new(
                MFrequency::new(predict_support::FREQUENCY_HZ, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        ));
    }
    list.write_table(path).expect("write the list");
}

/// A model image and a component list under the primary beam, and total
/// power, predict after `openfromms` as in the session that observed.
#[test]
fn components_under_the_beam_and_total_power_predict_after_openfromms_as_before() {
    use casa_simulation::{PredictSpec, VoltagePatternSpec};
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let points = &directory.path().join("points.cl");
    write_points(points);
    let both = move |sm: &mut Simulator| {
        sm.set_vp(&VoltagePatternSpec::default())?;
        sm.predict(&PredictSpec {
            images: vec![model.clone()],
            component_list: Some(points.clone()),
            incremental: false,
        })?;
        corrupt(sm)
    };
    let components = directory.path().join("components");
    std::fs::create_dir(&components).expect("directory");
    let (one, two) = two_stage(&components, &observed, &nothing, &both);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));

    let total_power = move |sm: &mut Simulator| {
        sm.set_options(&PredictionOptions {
            ftmachine: "sd".to_string(),
            grid_function: "PB".to_string(),
            ..PredictionOptions::default()
        })?;
        sm.predict(&predict_spec(model, false))
    };
    let single_dish = directory.path().join("sd");
    std::fs::create_dir(&single_dish).expect("directory");
    let (one, two) = two_stage(&single_dish, &observed, &nothing, &total_power);
    assert!(
        one.iter()
            .any(|row| row.data.iter().any(|value| value.norm() > 0.0))
    );
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// A point source at `direction`, 1 Jy.
fn write_point(path: &Path, direction: [f64; 2]) {
    use casa_components::{
        ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
    };
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::frequency::{FrequencyRef, MFrequency};
    let mut list = ComponentList::new();
    list.push(SkyComponent::new(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        ComponentShape::point(MDirection::from_angles(
            direction[0],
            direction[1],
            DirectionRef::J2000,
        )),
        SpectralModel::new(
            MFrequency::new(predict_support::FREQUENCY_HZ, FrequencyRef::LSRK),
            SpectralKind::Constant,
        ),
    ));
    list.write_table(path).expect("write the list");
}

/// A pointing `observemany` moved off its field keeps its field's FIELD
/// row, and after `openfromms` it is phased there, as CASA's `VisBuffer`
/// phases it: a source at the field's direction predicts zero phase in
/// both pointings, as it does in CASA 6.7.6 (measured), although the moved
/// pointing's UVW were computed at the moved direction. The session that
/// observed phases the moved pointing at the moved direction
/// (`docs/CASA (C++) bugs.md`). The pointing not moved predicts the same
/// either way, and a primary beam, centred on POINTING's direction, is
/// refused.
#[test]
fn a_moved_pointing_is_phased_on_its_field_after_openfromms() {
    use casa_simulation::{ObserveManySpec, Pointing, PredictSpec, VoltagePatternSpec};
    use casa_types::measures::direction::{DirectionRef, MDirection};
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = &directory.path().join("point.cl");
    write_point(point, predict_support::CENTRE_RAD);
    let moved = |path: &Path| {
        let mut sm = session(path, "ALMA", "XX YY", resources(2, 2 << 30));
        let base = scan("target", 0.0, 20.0);
        let direction = MDirection::from_angles(
            predict_support::CENTRE_RAD[0] + 1.0e-5,
            predict_support::CENTRE_RAD[1],
            DirectionRef::J2000,
        );
        sm.observe_many(&ObserveManySpec {
            pointings: vec![
                Pointing {
                    source: "target".to_string(),
                    start_s: 0.0,
                    stop_s: 20.0,
                    direction: None,
                },
                Pointing {
                    source: "target".to_string(),
                    start_s: 20.0,
                    stop_s: 40.0,
                    direction: Some(direction),
                },
            ],
            spectral_window: base.spectral_window,
            add_observation: false,
            state: base.state,
            observer: base.observer,
            project: base.project,
        })
        .expect("observemany");
        sm
    };
    let predict = move |sm: &mut Simulator| {
        sm.predict(&PredictSpec {
            images: Vec::new(),
            component_list: Some(point.clone()),
            incremental: false,
        })
    };
    let (one, two) = two_stage(directory.path(), &moved, &nothing, &predict);
    // The two pointings have as many rows each.
    let half = one.len() / 2;
    assert_eq!(bits(&data(&one[..half])), bits(&data(&two[..half])));
    for row in &two {
        for value in &row.data {
            assert!((value.re - 1.0).abs() < 1e-6, "{value}");
            assert!(value.im.abs() < 1e-6, "{value}");
        }
    }
    assert!(
        one[half..]
            .iter()
            .any(|row| row.data.iter().any(|value| value.im.abs() > 1e-3)),
        "the session that observed phases the moved pointing elsewhere"
    );

    let mut sm =
        Simulator::open_from_ms(directory.path().join("two.ms"), options()).expect("openfromms");
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    let refused = predict(&mut sm).expect_err("the beam needs POINTING at the phase centres");
    assert_eq!(refused.outcome(), CasaOutcome::NotYetImplemented);
    assert!(
        refused.message().contains("POINTING points scan"),
        "{refused}"
    );
}
