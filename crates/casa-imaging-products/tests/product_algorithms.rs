// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product algorithms on explicit planes: normalization, mosaic sensitivity
//! and restoring-beam fitting, independent of any major-cycle completion.

use casa_imaging_model::{
    PrimaryBeamValidityPolicy, ProductBlankingPolicy, ProductNormalization,
    ProductSupportComparison,
};
use casa_imaging_products::{
    MosaicSensitivity, fit_restoring_beam, gaussian_beam_image, normalize_plane,
};

#[test]
fn flat_noise_normalization_divides_by_the_exact_sensitivity() {
    let values = [2.0_f32, -4.0, 6.0];
    assert_eq!(
        normalize_plane(&values, ProductNormalization::UnitResponse, 8.0).expect("unit response"),
        [0.25, -0.5, 0.75]
    );
    assert_eq!(
        normalize_plane(&values, ProductNormalization::FlatNoise, 8.0).expect("flat noise"),
        [0.25, -0.5, 0.75]
    );
    // No usable sensitivity blanks every pixel instead of dividing by zero.
    let blanked = normalize_plane(&values, ProductNormalization::FlatNoise, 0.0).expect("blanked");
    assert!(blanked.iter().all(|value| value.is_nan()));
}

#[test]
fn mosaic_sensitivity_owns_normalization_primary_beam_and_valid_support() {
    let sensitivity =
        MosaicSensitivity::new(&[16.0, 4.0, 1.0, 0.0]).expect("finite positive mosaic sensitivity");
    assert_eq!(sensitivity.primary_beam(), [1.0, 0.5, 0.25, 0.0]);
    assert_eq!(
        sensitivity
            .normalize(&[32.0, 16.0, 8.0, 4.0], ProductNormalization::FlatNoise)
            .expect("flat-noise normalization"),
        [2.0, 2.0, 2.0, 0.0]
    );
    assert_eq!(
        sensitivity
            .normalize(&[32.0, 16.0, 8.0, 4.0], ProductNormalization::FlatSky)
            .expect("flat-sky normalization"),
        [2.0, 4.0, 8.0, 0.0]
    );

    let policy = PrimaryBeamValidityPolicy::new(
        0.25,
        ProductSupportComparison::StrictlyGreater,
        ProductBlankingPolicy::Zero,
    )
    .expect("valid PB policy");
    assert_eq!(sensitivity.validity(policy), [true, true, false, false]);
    assert_eq!(
        sensitivity
            .correct_primary_beam(&[2.0, 2.0, 2.0, 2.0], policy)
            .expect("PB correction"),
        [2.0, 4.0, 0.0, 0.0]
    );
}

#[test]
fn beam_fit_recovers_a_synthetic_elliptical_gaussian() {
    let shape = [32_usize, 32];
    let cell = [1.0e-4_f64, 1.0e-4];
    // Elliptical Gaussian with major FWHM 4 pixels, minor FWHM 2 pixels,
    // rotated 45 degrees east from north.
    let major_px = 4.0_f64;
    let ratio = 0.5;
    let pa = std::f64::consts::FRAC_PI_4;
    const FWHM_TO_SIGMA: f64 = 1.0 / 2.354_820_045_030_949_3;
    // Real PSF planes peak exactly on a pixel.
    let centre = 16.0_f64;
    let sigma_major = major_px * cell[0] * FWHM_TO_SIGMA;
    let sigma_minor = major_px * ratio * cell[0] * FWHM_TO_SIGMA;
    let mut psf = Vec::with_capacity(shape[0] * shape[1]);
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = (x as f64 - centre) * cell[0];
            let dy = (y as f64 - centre) * cell[1];
            let cos_pa = pa.cos();
            let sin_pa = pa.sin();
            let u = dx * cos_pa + dy * sin_pa;
            let v = -dx * sin_pa + dy * cos_pa;
            let value = (-0.5 * ((v / sigma_major).powi(2) + (u / sigma_minor).powi(2))).exp();
            psf.push(value as f32);
        }
    }
    let beam = fit_restoring_beam(&psf, shape, cell, 0.35).expect("fitted beam");
    let expected_major = major_px * cell[0];
    let expected_minor = major_px * ratio * cell[0];
    assert!(
        (beam.major_fwhm_rad() - expected_major).abs() <= 0.05 * expected_major,
        "major {} vs {expected_major}",
        beam.major_fwhm_rad()
    );
    assert!(
        (beam.minor_fwhm_rad() - expected_minor).abs() <= 0.10 * expected_minor,
        "minor {} vs {expected_minor}",
        beam.minor_fwhm_rad()
    );
}

#[test]
fn restoring_kernel_units_follow_the_image_cell_scale() {
    // A beam whose FWHM spans a known pixel count at a non-unit cell scale
    // must fit to the same physical width, and the generated kernel must be
    // multi-pixel: fitted radians and cell radians share one unit system.
    let cell = [2.0e-3_f64, 2.0e-3];
    let major_pixels = 6.0_f64;
    let minor_pixels = 2.5_f64;
    let shape = [32_usize, 32];
    let fwhm_to_sigma = 1.0 / 2.354_820_045_030_949_3;
    let sigma_major = major_pixels * fwhm_to_sigma;
    let sigma_minor = minor_pixels * fwhm_to_sigma;
    let mut psf = vec![0.0_f32; shape[0] * shape[1]];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = x as f64 - shape[0] as f64 / 2.0;
            let dy = y as f64 - shape[1] as f64 / 2.0;
            psf[x * shape[1] + y] =
                (-0.5 * ((dx / sigma_minor).powi(2) + (dy / sigma_major).powi(2))).exp() as f32;
        }
    }
    let beam = fit_restoring_beam(&psf, shape, cell, 0.35).expect("multi-pixel synthetic beam fit");
    assert!(
        (beam.major_fwhm_rad() - major_pixels * cell[1]).abs() < 0.15 * cell[1],
        "fitted major {} should match {} px",
        beam.major_fwhm_rad(),
        major_pixels
    );
    assert!(
        (beam.minor_fwhm_rad() - minor_pixels * cell[0]).abs() < 0.15 * cell[0],
        "fitted minor {} should match {} px",
        beam.minor_fwhm_rad(),
        minor_pixels
    );

    // The kernel evaluated with the same cells keeps that width in pixels:
    // walk along y (position angle zero) and find the half-maximum crossings.
    let kernel = gaussian_beam_image(shape, &beam, cell);
    let centre_y = shape[1] / 2;
    let row = shape[0] / 2 * shape[1];
    let half = kernel[(shape[0] / 2, centre_y)];
    let above: Vec<usize> = (0..shape[1])
        .filter(|y| kernel[(shape[0] / 2, *y)] >= half * 0.5)
        .collect();
    let measured_pixels = (above.len() as f64).max(1.0);
    assert!(
        ((major_pixels - measured_pixels).abs() < 1.5),
        "kernel FWHM {measured_pixels} px must stay near {major_pixels} px at cell {:?}",
        cell
    );
    assert!(half > 0.0 && half <= 1.0);
    let _ = row;
}

#[test]
fn psf_cutoff_is_a_fraction_of_the_actual_peak() {
    // Identical PSF shapes with different amplitudes must fit identical
    // beams: the cutoff walks a fraction of whatever peak exists.
    let cell = [1.0e-3_f64, 1.0e-3];
    let shape = [32_usize, 32];
    let fwhm_to_sigma = 1.0 / 2.354_820_045_030_949_3;
    let sigma_major = 5.0 * fwhm_to_sigma;
    let sigma_minor = 3.0 * fwhm_to_sigma;
    let mut psf = vec![0.0_f32; shape[0] * shape[1]];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = x as f64 - shape[0] as f64 / 2.0;
            let dy = y as f64 - shape[1] as f64 / 2.0;
            psf[x * shape[1] + y] =
                (-0.5 * ((dx / sigma_minor).powi(2) + (dy / sigma_major).powi(2))).exp() as f32;
        }
    }
    let unit_peak = fit_restoring_beam(&psf, shape, cell, 0.35).expect("unit-peak beam fit");
    let scaled: Vec<f32> = psf.iter().map(|value| value * 1000.0).collect();
    let large_peak = fit_restoring_beam(&scaled, shape, cell, 0.35).expect("scaled-peak beam fit");
    assert!(
        (unit_peak.major_fwhm_rad() - large_peak.major_fwhm_rad()).abs()
            < 1.0e-9 + 1.0e-4 * unit_peak.major_fwhm_rad(),
        "amplitude must not change the fitted major axis"
    );
    assert!(
        (unit_peak.minor_fwhm_rad() - large_peak.minor_fwhm_rad()).abs()
            < 1.0e-9 + 1.0e-4 * unit_peak.minor_fwhm_rad(),
        "amplitude must not change the fitted minor axis"
    );
}
