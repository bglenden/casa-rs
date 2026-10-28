// SPDX-License-Identifier: LGPL-3.0-or-later
//! Raster sampling on constructed rasters. Its agreement with CASA's
//! `im.pointingsampling` is in `tests/tsdimaging_casa.rs`.

use super::*;

const ARCSEC: f64 = std::f64::consts::PI / 180.0 / 3600.0;

/// A raster of `rows` rows of `per_row` pointings `step` arcsec apart
/// along `along` (unit vector in true angle: east, north), the rows
/// `spacing` arcsec apart perpendicular to it, centred on (1, −0.5) rad,
/// one second apart, with 30 s between rows.
fn raster(
    rows: usize,
    per_row: usize,
    (step, spacing): (f64, f64),
    along: [f64; 2],
) -> Vec<Pointing> {
    let centre = [1.0, -0.5_f64];
    let across = [-along[1], along[0]];
    let mut time = 1.0e9;
    let mut pointings = Vec::new();
    for row in 0..rows {
        for i in 0..per_row {
            let a = (i as f64 - per_row as f64 / 2.0) * step * ARCSEC;
            let b = (row as f64 - rows as f64 / 2.0) * spacing * ARCSEC;
            let east = a * along[0] + b * across[0];
            let north = a * along[1] + b * across[1];
            pointings.push(Pointing {
                time_s: time,
                direction_rad: [centre[0] + east / centre[1].cos(), centre[1] + north],
            });
            time += 1.0;
        }
        time += 30.0;
    }
    pointings
}

fn assert_close(actual: f64, expected: f64, tolerance: f64) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{actual} differs from {expected} by more than {tolerance}"
    );
}

#[test]
fn a_raster_in_longitude_samples_along_its_step_and_across_its_rows() {
    let sampling = raster_sampling(&raster(9, 21, (10.0, 12.0), [1.0, 0.0])).expect("sampling");
    // The latitude's span puts the central cosine a little off each row's.
    assert_close(sampling.interval_rad[0] / ARCSEC, 10.0, 2.0e-3);
    assert_close(sampling.interval_rad[1] / ARCSEC, 12.0, 1.0e-6);
    assert_close(sampling.position_angle_rad, 0.0, 1.0e-6);
}

#[test]
fn a_raster_in_latitude_scales_its_row_spacing_by_the_latitudes_cosine() {
    let sampling = raster_sampling(&raster(9, 21, (10.0, 12.0), [0.0, 1.0])).expect("sampling");
    assert_close(sampling.interval_rad[0] / ARCSEC, 10.0, 1.0e-6);
    // CASA's unscaled projection would give 12″ / cos(−0.5) = 13.7″.
    assert_close(sampling.interval_rad[1] / ARCSEC, 12.0, 2.0e-3);
    // The longitude steps are floored at 10⁻⁸ rad, so the slope is finite.
    assert_close(
        sampling.position_angle_rad,
        0.5 * std::f64::consts::PI,
        1.0e-3,
    );
}

#[test]
fn repeated_times_count_once_and_antennas_start_new_rows() {
    let one = raster(5, 11, (10.0, 12.0), [1.0, 0.0]);
    // Each pointing twice (two spectral windows), then a second antenna.
    let mut both: Vec<Pointing> = one.iter().flat_map(|p| [*p, *p]).collect();
    both.extend(one.iter().copied());
    let single = raster_sampling(&one).expect("one");
    let doubled = raster_sampling(&both).expect("both");
    assert_close(
        doubled.interval_rad[0],
        single.interval_rad[0],
        1.0e-12 * single.interval_rad[0],
    );
    assert_close(doubled.interval_rad[1] / ARCSEC, 12.0, 1.0e-6);
}

#[test]
fn a_single_time_or_no_row_gap_has_no_interval_to_measure() {
    let at = |time_s: f64| Pointing {
        time_s,
        direction_rad: [1.0, -0.5],
    };
    let single = raster_sampling(&[at(5.0e9), at(5.0e9)]).expect("single");
    assert_eq!(single.interval_rad, [0.0, 0.0]);
    let row = raster_sampling(&raster(1, 30, (10.0, 12.0), [1.0, 0.0])).expect("row");
    assert!(row.interval_rad[0] > 0.0);
    assert_eq!(row.interval_rad[1], 0.0);
    assert_eq!(raster_sampling(&[]), None);
}

#[test]
fn longitudes_across_the_boundary_are_made_continuous() {
    let pi = std::f64::consts::PI;
    // In [0, 2π) around 0: the mean is near π, and those below π move up.
    let mut around_zero = vec![0.1, 2.0 * pi - 0.1, 0.05, 2.0 * pi - 0.05];
    unwrap_longitudes(&mut around_zero);
    assert!(
        around_zero.iter().all(|&l| (l - 2.0 * pi).abs() < 0.2),
        "{around_zero:?}"
    );
    // In (−π, π] around π: the mean is near 0, and the negative ones move up.
    let mut around_pi = vec![pi - 0.1, -pi + 0.1, pi - 0.05, -pi + 0.05];
    unwrap_longitudes(&mut around_pi);
    assert!(
        around_pi.iter().all(|&l| (l - pi).abs() < 0.2),
        "{around_pi:?}"
    );
}

#[test]
fn medians_are_casacores() {
    assert_eq!(median(vec![3.0, 1.0, 2.0]), 2.0);
    assert_eq!(median(vec![4.0, 1.0, 3.0, 2.0]), 2.5);
    // Above 100 values, an even count takes the lower middle.
    let many: Vec<f64> = (0..102).map(f64::from).rev().collect();
    assert_eq!(median(many), 50.0);
    let hundred: Vec<f64> = (0..100).map(f64::from).collect();
    assert_eq!(median(hundred), 49.5);
}

#[test]
fn the_beam_takes_the_valid_intervals() {
    assert_eq!(beam_sampling([10.0, 12.0]), Some(vec![10.0, 12.0]));
    assert_eq!(beam_sampling([10.0, 10.05]), Some(vec![10.0]));
    assert_eq!(beam_sampling([10.0, 0.0]), Some(vec![10.0]));
    assert_eq!(beam_sampling([10.0, f64::NAN]), Some(vec![10.0]));
    // CASA sets no beam here, though it logs that it uses the other.
    assert_eq!(beam_sampling([1.0e-3, 12.0]), Some(vec![12.0]));
    assert_eq!(beam_sampling([0.0, 0.0]), None);
}
