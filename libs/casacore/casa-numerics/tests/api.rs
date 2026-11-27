// SPDX-License-Identifier: LGPL-3.0-or-later

use std::num::NonZeroUsize;

use casa_numerics::continuum::{RowWeights, fit_polynomial_row};

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1.0e-12,
        "{actual} != {expected}"
    );
}

#[test]
fn fits_the_frozen_casa_continuum_row_components() {
    // CASA 6.7 UVContSubTVI anchor: fit channels 0~2 and 5~7 of one SPW.
    for (constant, slope) in [(3.0, 1.5), (-2.0, 0.5)] {
        let samples = [0, 1, 2, 5, 6, 7].map(|channel| {
            let x = 1.0 - 2.0 * f64::from(channel) / 7.0;
            (x, constant + slope * x, 2.0 + f64::from(channel))
        });
        let fit = fit_polynomial_row(samples.into_iter(), 2, RowWeights::Supplied).unwrap();
        close(fit[0], constant);
        close(fit[1], slope);
    }
}

#[test]
fn normalized_and_residual_reweighted_fits_keep_their_distinct_results() {
    let samples = [
        (-1.0, 1.0, 1.0e-30),
        (0.0, 1.0, 1.0e-30),
        (1.0, 7.0, 1.0e-30),
    ];
    let mode = |passes| RowWeights::Reweighted(NonZeroUsize::new(passes).unwrap());
    close(
        fit_polynomial_row(samples.into_iter(), 1, mode(1)).unwrap()[0],
        3.0,
    );
    close(
        fit_polynomial_row(samples.into_iter(), 1, mode(2)).unwrap()[0],
        1.0,
    );
}

#[test]
fn rank_deficient_fit_returns_the_minimum_norm_coefficients() {
    let samples = [(0.0, 2.0, 1.0), (0.0, 4.0, 3.0)];
    let fit = fit_polynomial_row(samples.into_iter(), 2, RowWeights::Supplied).unwrap();
    close(fit[0], 3.5);
    close(fit[1], 0.0);
}

#[test]
fn reweighting_preserves_exact_fit_zero_residual_samples() {
    // Pass 1 constant mean is 2.0, so residuals are [0.0, 2.0, 1.0, 3.0].
    // The active sample with 0.0 residual must receive weight 1.0 (not MIN_POSITIVE).
    let samples = [
        (-1.0, 2.0, 1.0),
        (0.0, 0.0, 1.0),
        (1.0, 1.0, 1.0),
        (2.0, 5.0, 1.0),
    ];
    let fit = fit_polynomial_row(
        samples.into_iter(),
        1,
        RowWeights::Reweighted(NonZeroUsize::new(2).unwrap()),
    )
    .unwrap();
    close(fit[0], 4.0 / 3.0);
}
