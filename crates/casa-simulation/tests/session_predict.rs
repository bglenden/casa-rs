// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict` through the session (simulation Phase 1d, #841): the DATA
//! `close` writes is the per-row reference's prediction, bit for bit.
//!
//! - Each row is what [`ModelPredictor`] predicts for it on the CPU, with
//!   the row's UVW rotated to the model's tangent point as `GridFT` rotates
//!   it, the beam centred on the row's field, flagged rows zero.
//! - Autocorrelations predict the model's zero spacing: its flux, as CASA's
//!   `GridFT` (`usezero`) predicts them.
//! - A Stokes I model reaches the parallel hands of 1, 2 and 4 circular or
//!   linear correlations as the 2-correlation prediction does, and leaves
//!   the cross-hands exactly zero (#840).
//! - The written DATA does not depend on the workers.
//!
//! Agreement with CASA's own `sm.predict` is measured by
//! `tools/simulation/compare_sm_ms.py` on `sm_predict_scenarios.json`.

mod predict_support;

use casa_imaging_model::CorrelationType;
use casa_simulation::VoltagePatternSpec;
use casa_simulation::predict::PrimaryBeam;
use num_complex::Complex32;
use predict_support::{
    N, Row, bits, cube_model, data, extended_model, predict_spec, read_main, reference, resources,
    scan, session, write_model,
};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];

/// Whether every value of `cell` is exactly zero.
fn zero(cell: &[Complex32]) -> bool {
    cell.iter().all(|value| *value == Complex32::new(0.0, 0.0))
}

#[test]
fn the_session_predicts_each_row_as_the_row_predictor_does() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    for (stokes, correlations, beam, workers) in [
        ("XX YY", XX_YY.to_vec(), false, 1),
        (
            "RR LL",
            vec![CorrelationType::CircularRr, CorrelationType::CircularLl],
            true,
            5,
        ),
    ] {
        let path = directory.path().join(format!("{stokes}-{beam}.ms"));
        let mut sm = session(&path, "ALMA", stokes, resources(workers, 2 << 30));
        sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
        sm.observe(&scan("offset", 60.0, 120.0)).expect("observe");
        // A scan below the elevation limit: every row flagged.
        sm.set_limits(1.0e-6, 91.0_f64.to_radians());
        sm.observe(&scan("target", 120.0, 140.0)).expect("observe");
        if beam {
            sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        }
        sm.predict(&predict_spec(&model, false)).expect("predict");
        sm.close().expect("close");
        let (rows, _) = read_main(&path);
        let expected = reference(
            &rows,
            &model,
            &correlations,
            beam.then_some(PrimaryBeam::ALMA),
        );
        assert_eq!(
            bits(&data(&rows)),
            bits(&expected),
            "{stokes}: the session's DATA is the reference's"
        );
        let flagged = rows.iter().filter(|row| row.flag_row).count();
        assert_eq!(
            flagged,
            2 * ANTENNA_ROWS,
            "{stokes}: the low scan is flagged"
        );
        for row in &rows {
            assert_eq!(zero(&row.data), row.flag_row, "{stokes}: {row:?}");
        }
    }
}

/// Rows of one integration of [`predict_support::session`], autocorrelations
/// included.
const ANTENNA_ROWS: usize = predict_support::ANTENNAS * (predict_support::ANTENNAS + 1) / 2;

#[test]
fn autocorrelations_predict_the_models_zero_spacing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    // A compact model: its zero spacing is its flux to the gridder's
    // correction near the centre.
    let flux = [1.25_f32, 0.5, 2.0];
    let model = write_model(
        directory.path(),
        "compact.image",
        &[vec![
            (N / 2, N / 2, flux[0]),
            (N / 2 + 1, N / 2 - 2, flux[1]),
            (N / 2 - 3, N / 2 + 1, flux[2]),
        ]],
    );
    let path = directory.path().join("autos.ms");
    let mut sm = session(&path, "ALMA", "XX YY", resources(3, 2 << 30));
    sm.observe(&scan("offset", 0.0, 50.0)).expect("observe");
    sm.predict(&predict_spec(&model, false)).expect("predict");
    sm.close().expect("close");
    let (rows, _) = read_main(&path);
    let total: f32 = flux.iter().sum();
    let autos: Vec<&Row> = rows
        .iter()
        .filter(|row| row.antennas[0] == row.antennas[1])
        .collect();
    assert_eq!(autos.len(), 5 * predict_support::ANTENNAS);
    for row in &autos {
        for value in &row.data {
            assert!(
                (value.re - total).abs() < 1.0e-3 * total && value.im.abs() < 1.0e-5,
                "an autocorrelation predicts the flux {total}: {value}"
            );
        }
    }
    let first = &autos[0].data;
    assert!(
        autos.iter().all(|row| bits([&row.data]) == bits([first])),
        "every autocorrelation predicts the same zero spacing"
    );
    let cross = rows
        .iter()
        .find(|row| row.antennas[0] != row.antennas[1])
        .expect("a cross-correlation");
    assert_ne!(bits([&cross.data]), bits([first]));
}

/// The DATA a session of `stokes` writes for the cube model, beamed.
fn stokes_session(directory: &std::path::Path, stokes: &str) -> Vec<Row> {
    let model = cube_model(
        directory,
        &format!("cube-{}.image", stokes.replace(' ', "")),
    );
    let path = directory.join(format!("{}.ms", stokes.replace(' ', "")));
    let mut sm = session(&path, "ALMA", stokes, resources(2, 2 << 30));
    sm.observe(&scan("offset", 0.0, 40.0)).expect("observe");
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    sm.predict(&predict_spec(&model, false)).expect("predict");
    sm.close().expect("close");
    read_main(&path).0
}

/// One correlation of every sample, as bits.
fn correlation(rows: &[Row], correlations: usize, index: usize) -> Vec<(u32, u32)> {
    rows.iter()
        .flat_map(|row| row.data.iter().skip(index).step_by(correlations))
        .map(|value| (value.re.to_bits(), value.im.to_bits()))
        .collect()
}

#[test]
fn stokes_i_reaches_the_parallel_hands_and_leaves_the_cross_hands_zero() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let linear = stokes_session(directory.path(), "XX YY");
    let parallel = correlation(&linear, 2, 0);
    assert_eq!(correlation(&linear, 2, 1), parallel, "XX = YY = I");
    assert!(
        parallel.iter().any(|&(re, _)| f32::from_bits(re) > 0.1),
        "the model reaches the rows"
    );
    // Each layout's parallel hands are the 2-correlation prediction, and its
    // cross-hands are zero: 1 correlation, 4 linear and 4 circular (#840),
    // and a 2-correlation layout with one cross-hand.
    for (stokes, parallel_hands, cross_hands) in [
        ("XX", vec![0], vec![]),
        ("RR LL", vec![0, 1], vec![]),
        ("XX XY YX YY", vec![0, 3], vec![1, 2]),
        ("RR RL LR LL", vec![0, 3], vec![1, 2]),
        ("RR RL", vec![0], vec![1]),
    ] {
        let rows = stokes_session(directory.path(), stokes);
        let count = stokes.split(' ').count();
        for hand in parallel_hands {
            assert_eq!(
                correlation(&rows, count, hand),
                parallel,
                "{stokes}: correlation {hand} is Stokes I"
            );
        }
        for hand in cross_hands {
            // Exactly zero, of either sign.
            assert!(
                correlation(&rows, count, hand)
                    .iter()
                    .all(|&(re, im)| f32::from_bits(re) == 0.0 && f32::from_bits(im) == 0.0),
                "{stokes}: correlation {hand} is exactly zero"
            );
        }
    }
}

#[test]
fn the_written_prediction_does_not_depend_on_the_workers() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = cube_model(directory.path(), "cube.image");
    let write = |name: &str, workers: usize| {
        let path = directory.path().join(name);
        let mut sm = session(&path, "ALMA", "XX XY YX YY", resources(workers, 2 << 30));
        sm.observe(&scan("target", 0.0, 100.0)).expect("observe");
        sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        sm.predict(&predict_spec(&model, false)).expect("predict");
        sm.observe(&scan("offset", 100.0, 150.0)).expect("observe");
        sm.predict(&predict_spec(&model, true)).expect("predict");
        sm.close().expect("close");
        bits(&data(&read_main(&path).0))
    };
    let one = write("one.ms", 1);
    assert_eq!(write("five.ms", 5), one);
    assert_eq!(write("three.ms", 3), one);
}
