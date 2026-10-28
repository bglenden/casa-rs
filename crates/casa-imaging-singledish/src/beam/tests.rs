// SPDX-License-Identifier: LGPL-3.0-or-later
//! The beam's parts against their definitions. The beam's agreement with
//! CASA's `sdbeamutil` is in `tests/tsdimaging_casa.rs`.

use super::*;

#[test]
fn the_obstruction_spline_matches_its_five_points() {
    for (epsilon, width) in SCHROEDER {
        let factor = central_obstruction_factor(epsilon);
        assert!(
            (factor * 1.22 - width).abs() < 1.0e-13,
            "{epsilon}: {factor}"
        );
    }
    // Between the points, smooth and decreasing.
    let values: Vec<f64> = (0..=40)
        .map(|i| central_obstruction_factor(f64::from(i) * 0.01))
        .collect();
    assert!(values.windows(2).all(|pair| pair[1] < pair[0]));
}

#[test]
fn a_ten_db_taper_gives_baars_coefficient() {
    let tau = 10_f64.powf(-0.5);
    let expected = 1.269 - 0.566 * tau + 0.534 * tau * tau - 0.208 * tau.powi(3);
    assert!((baars_taper_factor(10.0) - expected).abs() < 1.0e-15);
    // An unobstructed 12 m dish at 300 GHz: b λ/D.
    let fwhm = primary_beam_fwhm_arcsec(299_792_458.0e3, 12.0, 0.0, 10.0);
    let lambda_over_d = 1.0e-3 / 12.0 * 180.0 * 3600.0 / std::f64::consts::PI;
    assert!((fwhm - expected * lambda_over_d).abs() < 1.0e-12 * fwhm);
}

#[test]
fn convolution_is_numpys_same_mode() {
    // numpy.convolve([0, 0, 0, 1, 0, 0], [1, 2, 3, 4, 5, 6], 'same')
    // is [0, 1, 2, 3, 4, 5]; for length 5, [1, 2, 3, 4, 5].
    let even = convolve_same(
        &[0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    );
    assert_eq!(even, vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);
    let odd = convolve_same(&[0.0, 0.0, 1.0, 0.0, 0.0], &[1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_eq!(odd, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
}

#[test]
fn the_fit_recovers_a_gaussians_width() {
    let axis = arange(-30.0, 30.0 + 0.01, 0.02);
    let width = 7.3;
    let sigma = width / 2.354_820_1;
    let profile: Vec<f64> = axis
        .iter()
        .map(|x| (-(x * x) / (2.0 * sigma * sigma)).exp())
        .collect();
    assert!((gaussian_fit(&axis, &profile) - width).abs() < 1.0e-10);
    assert!((half_power_width(&axis, &profile) - width).abs() < 1.0e-3);
}

#[test]
fn the_beam_widens_with_the_kernel_and_the_sampling() {
    let (frequency_hz, diameter_m) = (230.0e9, 12.0);
    let fwhm = primary_beam_fwhm_arcsec(frequency_hz, diameter_m, 0.75, 10.0);
    let narrow = KernelTable::new(GridFunction::BOX).expect("box");
    let spheroid = GridFunction::Spheroidal { support: 6 };
    let wide = KernelTable::new(spheroid).expect("sf");
    let beam = |kernel: (GridFunction, &KernelTable), cell: f64, sampling: &[f64]| {
        theoretical_beam_arcsec(&BeamSetup {
            diameter_m,
            obscuration_m: 0.75,
            frequency_hz,
            cell_arcsec: [cell, cell],
            kernel,
            sampling_arcsec: sampling,
        })
    };
    let fine = beam((GridFunction::BOX, &narrow), 0.05, &[0.05]);
    assert!(fine > fwhm && fine < fwhm * 1.001, "{fine} against {fwhm}");
    let gridded = beam((spheroid, &wide), fwhm / 3.0, &[0.05]);
    let sampled = beam((spheroid, &wide), fwhm / 3.0, &[fwhm / 3.0]);
    assert!(gridded > fine * 1.1, "{gridded}");
    assert!(sampled > gridded, "{sampled}");
    // Two intervals: the geometric mean of the two axes' widths.
    let across = beam((spheroid, &wide), fwhm / 3.0, &[fwhm / 3.0, fwhm / 2.0]);
    let wider = beam((spheroid, &wide), fwhm / 3.0, &[fwhm / 2.0]);
    assert!((across - (sampled * wider).sqrt()).abs() < 1.0e-9 * across);
}
