// SPDX-License-Identifier: LGPL-3.0-or-later
//! A transfer field's spectral fit across its windows, as CASA's
//! `fluxscale` reports it (`SolvableVisJones::fluxscale`, #931).
//!
//! CASA fits `log10 S = a0 + a1 log10(f / f0)`, its default `fitorder=1`
//! and the only order casa-rs takes, where `f0` is the windows' geometric
//! mean frequency.

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
