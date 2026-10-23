// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session's mosaic machine (`setoptions(ftmachine='mosaic')`, #841
//! Phase 2), as CASA's simulator builds `MosaicFTNew`: the beams are in the
//! kernels whatever `setvp` says, autocorrelations are predicted, and what
//! casa-rs does not build yet is refused. Its agreement with CASA is the
//! `predict-mosaic-*` scenarios' (`tools/simulation/sm_predict_scenarios.json`).

mod predict_support;

use std::path::Path;

use casa_simulation::{
    CasaOutcome, ObserveManySpec, Pointing, PredictionOptions, Simulator, SimulatorResult,
    VoltagePatternSpec,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use predict_support::{
    CENTRE_RAD, CHANNELS, bits, data, extended_model, predict_spec, read_main, resources, scan,
    session_with_dishes,
};

/// Eight antennas, ALMA's 12 m and ACA's 7 m in turn.
const DISHES: [f64; 8] = [12.0, 7.0, 12.0, 7.0, 12.0, 7.0, 12.0, 7.0];

fn mosaic() -> PredictionOptions {
    PredictionOptions {
        ftmachine: "mosaic".to_string(),
        ..PredictionOptions::default()
    }
}

/// A session of [`DISHES`] of `telescope`, before `observe`.
fn session(path: &Path, telescope: &str) -> Simulator {
    session_with_dishes(
        path,
        telescope,
        "XX YY",
        CHANNELS,
        &DISHES,
        resources(2, 2 << 30),
    )
}

/// [`session`] observing the model's centre and then the `offset` field,
/// through the mosaic machine.
fn observed(path: &Path, telescope: &str) -> Simulator {
    let mut sm = session(path, telescope);
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.observe(&scan("offset", 60.0, 120.0)).expect("observe");
    sm.set_options(&mosaic()).expect("setoptions");
    sm
}

fn refusal(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.message().to_string())
}

#[test]
fn setoptions_takes_the_mosaic_machine_but_not_both_or_pbwproject() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut sm = session(&directory.path().join("options.ms"), "ALMA");
    sm.set_options(&mosaic()).expect("mosaic");
    for machine in ["both", "pbwproject"] {
        let (outcome, message) = refusal(sm.set_options(&PredictionOptions {
            ftmachine: machine.to_string(),
            ..PredictionOptions::default()
        }));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{machine}");
        assert!(
            message.contains(machine) && message.contains("#841"),
            "{message}"
        );
    }
    sm.close().expect("close");
}

/// `MosaicFTNew`'s kernels carry the beams, and `SkyEquation::
/// applySkyJones` skips machines named `MosaicFT…`: `setvp(dovp=True)` and
/// no `setvp` predict the same DATA, bit for bit, and every autocorrelation
/// is predicted (`usezero`).
#[test]
fn the_mosaic_beams_are_in_its_kernels_whatever_setvp_says() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let predicted = |name: &str, voltage_pattern: Option<VoltagePatternSpec>| {
        let path = directory.path().join(name);
        let mut sm = observed(&path, "ALMA");
        if let Some(spec) = voltage_pattern {
            sm.set_vp(&spec).expect("setvp");
        }
        sm.predict(&predict_spec(&model, false)).expect("predict");
        sm.close().expect("close");
        read_main(&path).0
    };
    let beam = predicted("vp.ms", Some(VoltagePatternSpec::default()));
    let none = predicted("no-vp.ms", None);
    assert_eq!(bits(&data(&beam)), bits(&data(&none)));
    let autocorrelations = beam
        .iter()
        .filter(|row| row.antennas[0] == row.antennas[1])
        .collect::<Vec<_>>();
    assert_eq!(autocorrelations.len(), 12 * DISHES.len());
    assert!(
        autocorrelations
            .iter()
            .all(|row| row.data.iter().all(|value| value.norm() > 0.1)),
        "every autocorrelation is predicted"
    );
}

/// What the mosaic machine refuses: a telescope other than ALMA and ACA
/// (CASA's `MosaicFTNew` keeps `SimplePBConvFunc` of the VLA's beam), a
/// field pointed outside the model (for which CASA predicts nothing), and a
/// pointing `observemany` moved off its field. The session closes after.
#[test]
fn the_mosaic_refuses_what_casa_rs_does_not_build_yet() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let refused = |mut sm: Simulator, needle: &str| {
        let (outcome, message) = refusal(sm.predict(&predict_spec(&model, false)));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented, "{message}");
        assert!(
            message.contains(needle) && message.contains("ftmachine='mosaic'"),
            "{message}"
        );
        sm.close().expect("close");
    };
    refused(
        observed(&directory.path().join("vla.ms"), "VLA"),
        "telescope VLA",
    );

    let mut far = session(&directory.path().join("far.ms"), "ALMA");
    far.set_field(
        "far",
        &MDirection::from_angles(CENTRE_RAD[0] + 1.0e-3, CENTRE_RAD[1], DirectionRef::J2000),
        "",
        0.0,
    )
    .expect("setfield");
    far.observe(&scan("far", 0.0, 60.0)).expect("observe");
    far.set_options(&mosaic()).expect("setoptions");
    refused(far, "outside the model image");

    let mut moved = session(&directory.path().join("moved.ms"), "ALMA");
    let base = scan("target", 0.0, 20.0);
    let pointing = |start_s: f64, direction: Option<MDirection>| Pointing {
        source: "target".to_string(),
        start_s,
        stop_s: start_s + 20.0,
        direction,
    };
    let off = MDirection::from_angles(CENTRE_RAD[0] + 1.0e-5, CENTRE_RAD[1], DirectionRef::J2000);
    moved
        .observe_many(&ObserveManySpec {
            pointings: vec![pointing(0.0, None), pointing(20.0, Some(off))],
            spectral_window: base.spectral_window.clone(),
            add_observation: false,
            state: base.state.clone(),
            observer: base.observer.clone(),
            project: base.project.clone(),
        })
        .expect("observemany");
    moved.set_options(&mosaic()).expect("setoptions");
    refused(moved, "pointed away");
}
