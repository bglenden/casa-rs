// SPDX-License-Identifier: LGPL-3.0-or-later
//! `CalibrationTableLookup`: a complex table's solution for an antenna,
//! window, field and time, as the apply executor finds and interpolates it.

mod common;

use casa_calibration::{ApplyInterpolationMode, CalibrationTableLookup};
use casa_types::Complex32;
use common::{SyntheticDelaySolutionRow, SyntheticGainSolutionRow};
use tempfile::TempDir;

fn row(
    time_seconds: f64,
    field_id: i32,
    antenna_id: i32,
    gains: [Complex32; 2],
) -> SyntheticGainSolutionRow {
    SyntheticGainSolutionRow {
        time_seconds,
        field_id,
        spectral_window_id: 0,
        antenna_id,
        gains: gains.to_vec(),
        flags: vec![false, false],
    }
}

/// Antenna 0 at 10 s and 30 s, antenna 1 at 10 s; one field.
fn table(dir: &TempDir) -> std::path::PathBuf {
    common::create_apply_gain_caltable(
        dir.path(),
        &["target"],
        &[
            row(
                10.0,
                0,
                0,
                [Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
            ),
            row(
                30.0,
                0,
                0,
                [Complex32::new(0.0, 3.0), Complex32::new(4.0, 0.0)],
            ),
            row(
                10.0,
                0,
                1,
                [Complex32::new(0.5, 0.5), Complex32::new(1.0, 1.0)],
            ),
        ],
    )
}

fn close(found: Complex32, expected: Complex32) -> bool {
    (found - expected).norm() < 1e-6
}

#[test]
fn nearest_takes_the_nearest_solution_in_time() {
    let dir = TempDir::new().expect("tempdir");
    let lookup =
        CalibrationTableLookup::open(table(&dir), ApplyInterpolationMode::Nearest).expect("open");
    assert_eq!(lookup.subtype(), "G Jones");
    let at = |time: f64| {
        lookup
            .solution(0, 0, 0, time)
            .expect("lookup")
            .expect("a solution")
    };
    assert_eq!(at(18.0).value(0, 0), Complex32::new(1.0, 0.0));
    assert_eq!(at(22.0).value(0, 0), Complex32::new(0.0, 3.0));
    assert_eq!(at(22.0).value(1, 0), Complex32::new(4.0, 0.0));
    assert_eq!((at(0.0).receptors, at(0.0).channels), (2, 1));
}

/// Linear interpolation is in amplitude and phase apart, as CASA's
/// `setapply` interpolates (measured), clamped at the ends.
#[test]
fn linear_interpolates_amplitude_and_phase() {
    let dir = TempDir::new().expect("tempdir");
    let lookup =
        CalibrationTableLookup::open(table(&dir), ApplyInterpolationMode::Linear).expect("open");
    let at = |time: f64| {
        lookup
            .solution(0, 0, 0, time)
            .expect("lookup")
            .expect("a solution")
    };
    // Halfway from 1 to 3i: amplitude 2, phase π/4.
    let half = std::f32::consts::FRAC_PI_4;
    assert!(close(
        at(20.0).value(0, 0),
        Complex32::from_polar(2.0, half)
    ));
    assert!(close(at(20.0).value(1, 0), Complex32::new(3.0, 0.0)));
    assert!(close(at(0.0).value(0, 0), Complex32::new(1.0, 0.0)));
    assert!(close(at(40.0).value(0, 0), Complex32::new(0.0, 3.0)));
}

/// A table of one field serves a row of any field; an antenna or window
/// the table lacks has no solution.
#[test]
fn a_sole_field_serves_every_field_and_absent_keys_have_none() {
    let dir = TempDir::new().expect("tempdir");
    let lookup =
        CalibrationTableLookup::open(table(&dir), ApplyInterpolationMode::Nearest).expect("open");
    let other_field = lookup
        .solution(3, 0, 1, 10.0)
        .expect("lookup")
        .expect("a solution");
    assert_eq!(other_field.value(0, 0), Complex32::new(0.5, 0.5));
    assert!(lookup.solution(0, 0, 7, 10.0).expect("lookup").is_none());
    assert!(lookup.solution(0, 5, 0, 10.0).expect("lookup").is_none());
}

/// A table of other than complex solutions is refused.
#[test]
fn a_delay_table_is_refused() {
    let dir = TempDir::new().expect("tempdir");
    let delays = common::create_apply_delay_caltable(
        dir.path(),
        &["target"],
        &[SyntheticDelaySolutionRow {
            time_seconds: 10.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            delays_ns: vec![1.0, 2.0],
            flags: vec![false, false],
        }],
    );
    let refused = CalibrationTableLookup::open(delays, ApplyInterpolationMode::Nearest)
        .err()
        .expect("refused");
    assert!(refused.to_string().contains("not complex"), "{refused}");
}
