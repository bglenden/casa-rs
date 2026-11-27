// SPDX-License-Identifier: LGPL-3.0-or-later
//! A transfer field's spectral fit across its windows, as CASA's
//! `fluxscale` reports it (`SolvableVisJones::fluxscale`, #931).
//!
//! CASA fits a polynomial in `log10(f / f0)` to `log10 S`, where `f0`
//! is the windows' geometric mean frequency. Retain the default order-one
//! arithmetic, and use the shared weighted solver for other degrees.

/// One window's flux density for the fit.
#[derive(Debug, Clone, Copy)]
pub(super) struct WindowFlux {
    /// The window's solution frequency, CASA's `freq`.
    pub(super) frequency_hz: f64,
    pub(super) flux_density_jy: f64,
    pub(super) flux_density_error_jy: f64,
}

/// A field's fit, as CASA's return reports it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SpectralFit {
    /// `fitRefFreq`, `f0`.
    pub(super) ref_frequency_hz: f64,
    /// `fitFluxd`, `10^a0`.
    pub(super) fluxd: f64,
    /// `fitFluxdErr`.
    pub(super) fluxd_err: f64,
    /// `spidx`, `[a0, a1]`.
    pub(super) spidx: Vec<f64>,
    /// `spidxerr`.
    pub(super) spidx_err: Vec<f64>,
    /// `covarMat`, empty when nothing was fitted.
    pub(super) covariance: Vec<Vec<f64>>,
}

/// The fit of `windows`' flux densities, as CASA makes it:
///
/// - No window leaves CASA's zeros, and one is its own flux density at its
///   frequency, with no fit.
/// - More are fitted by least squares in `log10 S` against `log10 f` less
///   its mean, each window weighted by `(S / σ)²`; a window with no error
///   carries no weight, as casacore's `GenericL2Fit` skips a zero sigma.
/// - `covarMat` is the inverse of the normal matrix, and the coefficients'
///   errors are the roots of its diagonal scaled by `sqrt(χ² / (n - 2))`,
///   which is zero when the fit has no degree of freedom; the flux
///   density's error is `ln 10 · 10^a0 · σ(a0)`.
/// - Windows that cannot determine both coefficients (fewer than two with
///   an error, or one frequency) give NaN coefficients rather than a fit.
///
/// Probed against CASA 6.7.6.14 with one, two and three windows.
pub(super) fn spectral_fit(windows: &[WindowFlux]) -> SpectralFit {
    let unfitted = |ref_frequency_hz, fluxd| SpectralFit {
        ref_frequency_hz,
        fluxd,
        fluxd_err: 0.0,
        spidx: vec![0.0; 2],
        spidx_err: vec![0.0; 2],
        covariance: Vec::new(),
    };
    match windows {
        [] => unfitted(0.0, 0.0),
        [window] => unfitted(window.frequency_hz, window.flux_density_jy),
        _ => fit(windows),
    }
}

pub(super) fn effective_order(requested: i32, windows: usize) -> usize {
    let requested = if requested < 0 { 1 } else { requested as usize };
    requested.min(windows.saturating_sub(1))
}

pub(super) fn spectral_fit_order(windows: &[WindowFlux], requested: i32) -> SpectralFit {
    let requested = if requested < 0 {
        tracing::warn!(
            requested,
            "negative fitorder: using fitorder=1, as CASA advertises"
        );
        1
    } else {
        requested as usize
    };
    if requested == 1 {
        return spectral_fit(windows);
    }
    let order = effective_order(requested as i32, windows.len());
    if windows.len() > 1 && order < requested {
        tracing::warn!(
            requested,
            order,
            "reducing underdetermined spectral fit degree"
        );
    }
    let mut result = match windows {
        [] => spectral_fit(windows),
        [window] => SpectralFit {
            ref_frequency_hz: window.frequency_hz,
            fluxd: window.flux_density_jy,
            fluxd_err: 0.,
            spidx: Vec::new(),
            spidx_err: Vec::new(),
            covariance: Vec::new(),
        },
        _ => polynomial_fit(windows, order),
    };
    result.spidx.resize(requested + 1, 0.);
    result.spidx_err.resize(requested + 1, 0.);
    result
}

fn polynomial_fit(windows: &[WindowFlux], order: usize) -> SpectralFit {
    let mean_log_frequency = windows
        .iter()
        .map(|window| window.frequency_hz.log10())
        .sum::<f64>()
        / windows.len() as f64;
    let size = order + 1;
    let points = windows
        .iter()
        .filter_map(|window| {
            let sigma = window.flux_density_error_jy / window.flux_density_jy;
            let x = window.frequency_hz.log10() - mean_log_frequency;
            (sigma != 0.).then(|| {
                let mut basis = vec![1.; size];
                for index in 1..size {
                    basis[index] = basis[index - 1] * x;
                }
                (basis, window.flux_density_jy.log10(), 1. / (sigma * sigma))
            })
        })
        .collect::<Vec<_>>();
    let mut normal = vec![0.; size * size];
    for (basis, _, weight) in &points {
        for i in 0..size {
            for j in 0..size {
                normal[i * size + j] += weight * basis[i] * basis[j];
            }
        }
    }
    let coefficients = casa_numerics::solve_weighted_least_squares(&points, size)
        .unwrap_or_else(|| vec![f64::NAN; size]);
    let covariance = casa_numerics::invert_symmetric_positive_definite(&normal, size)
        .unwrap_or_else(|| vec![f64::NAN; size * size]);
    let chi_square = points
        .iter()
        .map(|(basis, y, weight)| {
            let prediction = basis
                .iter()
                .zip(&coefficients)
                .map(|(x, a)| x * a)
                .sum::<f64>();
            weight * (y - prediction).powi(2)
        })
        .sum::<f64>();
    let free = points.len().saturating_sub(size);
    let unit_error = if free > 0 {
        (chi_square / free as f64).sqrt()
    } else {
        0.
    };
    let errors = (0..size)
        .map(|index| covariance[index * size + index].sqrt() * unit_error)
        .collect::<Vec<_>>();
    let fluxd = 10_f64.powf(coefficients[0]);
    SpectralFit {
        ref_frequency_hz: 10_f64.powf(mean_log_frequency),
        fluxd,
        fluxd_err: if errors[0] > 0. {
            std::f64::consts::LN_10 * fluxd * errors[0]
        } else {
            0.
        },
        spidx: coefficients,
        spidx_err: errors,
        covariance: covariance.chunks(size).map(<[f64]>::to_vec).collect(),
    }
}

fn fit(windows: &[WindowFlux]) -> SpectralFit {
    let mean_log_frequency = windows
        .iter()
        .map(|window| window.frequency_hz.log10())
        .sum::<f64>()
        / windows.len() as f64;
    // `(x, y, weight)` of each window with an error.
    let points = windows
        .iter()
        .filter_map(|window| {
            let sigma = window.flux_density_error_jy / window.flux_density_jy;
            (sigma != 0.0).then(|| {
                (
                    window.frequency_hz.log10() - mean_log_frequency,
                    window.flux_density_jy.log10(),
                    1.0 / (sigma * sigma),
                )
            })
        })
        .collect::<Vec<_>>();
    let (mut n00, mut n01, mut n11, mut r0, mut r1) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for &(x, y, weight) in &points {
        n00 += weight;
        n01 += weight * x;
        n11 += weight * x * x;
        r0 += weight * y;
        r1 += weight * x * y;
    }
    let determinant = n00 * n11 - n01 * n01;
    let covariance = if determinant > 0.0 {
        [
            [n11 / determinant, -n01 / determinant],
            [-n01 / determinant, n00 / determinant],
        ]
    } else {
        [[f64::NAN; 2]; 2]
    };
    let a0 = covariance[0][0] * r0 + covariance[0][1] * r1;
    let a1 = covariance[1][0] * r0 + covariance[1][1] * r1;
    let chi_square = points
        .iter()
        .map(|&(x, y, weight)| weight * (y - a0 - a1 * x).powi(2))
        .sum::<f64>();
    let free = points.len().saturating_sub(2);
    let unit_error = if free > 0 {
        (chi_square / free as f64).sqrt()
    } else {
        0.0
    };
    let spidx_err = vec![
        covariance[0][0].sqrt() * unit_error,
        covariance[1][1].sqrt() * unit_error,
    ];
    let fluxd = 10.0_f64.powf(a0);
    SpectralFit {
        ref_frequency_hz: 10.0_f64.powf(mean_log_frequency),
        fluxd,
        fluxd_err: if spidx_err[0] > 0.0 {
            std::f64::consts::LN_10 * fluxd * spidx_err[0]
        } else {
            0.0
        },
        spidx: vec![a0, a1],
        spidx_err,
        covariance: covariance.map(Vec::from).to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_degrees_match_casa_polynomial_and_covariance() {
        let windows = [
            WindowFlux {
                frequency_hz: 100_003_500_000.,
                flux_density_jy: 1.014_581_621_096_799,
                flux_density_error_jy: 0.037_674_965_469_373_33,
            },
            WindowFlux {
                frequency_hz: 110_003_500_000.,
                flux_density_jy: 1.625_712_369_547_564,
                flux_density_error_jy: 0.060_974_668_343_850_31,
            },
            WindowFlux {
                frequency_hz: 125_003_500_000.,
                flux_density_jy: 3.518_518_820_391_956_4,
                flux_density_error_jy: 0.114_365_520_945_259_77,
            },
        ];
        let constant = spectral_fit_order(&windows, 0);
        close(constant.fluxd, 1.913_940_282_294_492_4);
        close(constant.fluxd_err, 0.713_361_317_227_738_9);
        close_all(&constant.spidx, &[0.281_928_383_036_135_4]);
        close_all(&constant.spidx_err, &[0.161_869_670_930_293_28]);
        close(constant.covariance[0][0], 0.000_419_708_825_242_374_8);
        let quadratic = spectral_fit_order(&windows, 2);
        close(quadratic.fluxd, 1.724_973_681_147_680_6);
        close_all(
            &quadratic.spidx,
            &[
                0.236_782_473_195_072_76,
                5.519_995_877_479_235,
                11.280_651_701_679_295,
            ],
        );
        close_all(
            &quadratic.covariance.concat(),
            &[
                0.001_468_159_866_913_015_5,
                0.004_285_502_858_496_359,
                -0.636_580_329_174_632_6,
                0.004_285_502_858_496_359,
                0.282_270_217_556_632_2,
                -3.350_620_330_868_181_5,
                -0.636_580_329_174_632_6,
                -3.350_620_330_868_181_5,
                388.624_633_701_836_73,
            ],
        );
        // CASA's normal-equation residual rounds to a tiny nonzero error here.
        assert!((quadratic.fluxd_err - 3.628_481_652_284_325_4e-8).abs() < 1e-7);
        assert_eq!(spectral_fit_order(&windows[..1], 0).spidx, [0.]);
        assert_eq!(spectral_fit_order(&windows, -1), spectral_fit(&windows));
    }

    /// The transfer field's windows of a probe of CASA 6.7.6.14's
    /// `fluxscale`: three windows near 100, 110 and 125 GHz, with gains
    /// scaled and scattered per window.
    const PROBED: [WindowFlux; 3] = [
        WindowFlux {
            frequency_hz: 100_001_500_000.0,
            flux_density_jy: 1.045_320_397_120_710_6,
            flux_density_error_jy: 0.054_365_109_761_127_99,
        },
        WindowFlux {
            frequency_hz: 110_001_500_000.0,
            flux_density_jy: 1.122_375_623_567_635,
            flux_density_error_jy: 0.081_670_004_800_788_65,
        },
        WindowFlux {
            frequency_hz: 125_001_499_999.999_98,
            flux_density_jy: 1.613_920_162_074_12,
            flux_density_error_jy: 0.085_489_668_157_345_38,
        },
    ];

    fn close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= 1e-9 * expected.abs().max(1e-12),
            "{actual} != {expected}"
        );
    }

    fn close_all(actual: &[f64], expected: &[f64]) {
        assert_eq!(actual.len(), expected.len(), "{actual:?} {expected:?}");
        for (actual, expected) in actual.iter().zip(expected) {
            close(*actual, *expected);
        }
    }

    #[test]
    fn three_windows_fit_as_casa_fits_them() {
        let fit = spectral_fit(&PROBED);
        close(fit.ref_frequency_hz, 111_200_510_769.443_45);
        close(fit.fluxd, 1.255_230_360_151_499_6);
        close(fit.fluxd_err, 0.058_019_457_193_453_13);
        close_all(
            &fit.spidx,
            &[0.098_723_434_950_143_38, 1.973_384_878_322_450_4],
        );
        close_all(
            &fit.spidx_err,
            &[0.020_074_028_562_452_405, 0.464_351_874_576_059_9],
        );
        close_all(
            &fit.covariance.concat(),
            &[
                0.001_092_951_377_683_117_8,
                -0.000_112_026_048_727_069_51,
                -0.000_112_026_048_727_069_51,
                0.584_825_327_350_395_4,
            ],
        );
    }

    #[test]
    fn two_windows_fit_exactly_with_no_error() {
        let fit = spectral_fit(&PROBED[..2]);
        close(fit.ref_frequency_hz, 104_882_386_520.568_91);
        close(fit.fluxd, 1.083_163_022_146_862_9);
        close_all(
            &fit.spidx,
            &[0.034_693_825_314_768_37, 0.746_248_833_448_791_8],
        );
        assert_eq!((fit.fluxd_err, fit.spidx_err.clone()), (0.0, vec![0.0; 2]));
        close_all(
            &fit.covariance.concat(),
            &[
                0.001_999_907_579_316_794,
                0.031_285_552_152_303_58,
                0.031_285_552_152_303_58,
                4.669_121_168_297_186_5,
            ],
        );
    }

    #[test]
    fn one_window_is_its_own_flux_density_and_none_stays_zero() {
        let fit = spectral_fit(&PROBED[..1]);
        assert_eq!(
            (fit.ref_frequency_hz, fit.fluxd, fit.fluxd_err),
            (100_001_500_000.0, 1.045_320_397_120_710_6, 0.0)
        );
        assert_eq!((fit.spidx, fit.spidx_err), (vec![0.0; 2], vec![0.0; 2]));
        assert!(fit.covariance.is_empty());
        assert_eq!(spectral_fit(&[]).fluxd, 0.0);
    }

    #[test]
    fn windows_that_cannot_determine_a_fit_give_nan() {
        // Two windows at one frequency fix no spectral index.
        let same = [PROBED[0], PROBED[0]];
        let fit = spectral_fit(&same);
        assert!(fit.spidx.iter().all(|value| value.is_nan()), "{fit:?}");
        assert!(fit.fluxd.is_nan(), "{fit:?}");
    }
}
