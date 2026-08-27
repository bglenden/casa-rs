// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::ContinuumChannelUse;
use casa_imaging_reconstruction::{
    ContinuumFitStatus, ContinuumRowInput, ContinuumSample, fit_and_subtract_continuum,
};
use num_complex::Complex64;

fn assert_complex_close(actual: Complex64, expected: Complex64) {
    assert!(
        (actual - expected).norm() < 1.0e-10,
        "actual={actual:?}, expected={expected:?}"
    );
}

#[test]
fn continuum_transform_matches_casa_minmax_basis() {
    // CASA normalizes the complete row-frequency domain with
    // x = (frequency - midpoint) / (minimum - midpoint).  These irregular,
    // descending frequencies therefore map to [-1.0, 0.4, 1.0].
    let frequencies_hz = [200.0, 130.0, 100.0];
    let continuum = [
        Complex64::new(-1.0, -1.5),
        Complex64::new(3.2, -0.8),
        Complex64::new(5.0, -0.5),
    ];
    let samples = [
        ContinuumSample::new(continuum[0], false, 1.0, ContinuumChannelUse::FitOnly),
        ContinuumSample::new(
            continuum[1] + Complex64::new(7.0, -2.0),
            false,
            1.0,
            ContinuumChannelUse::ApplyOnly,
        ),
        ContinuumSample::new(continuum[2], false, 1.0, ContinuumChannelUse::FitAndApply),
    ];

    let result = fit_and_subtract_continuum(ContinuumRowInput::new(&frequencies_hz, &samples, 1))
        .expect("CASA-equivalent linear fit");

    assert_eq!(
        result.status(),
        ContinuumFitStatus::Fitted { effective_order: 1 }
    );
    assert_eq!(result.normalized_frequencies(), &[-1.0, 0.4, 1.0]);
    assert_eq!(result.fit_sample_indices(), &[0, 2]);
    assert_complex_close(result.coefficients()[0], Complex64::new(2.0, -1.0));
    assert_complex_close(result.coefficients()[1], Complex64::new(3.0, 0.5));
    for (actual, expected) in result.prediction().iter().zip(continuum) {
        assert_complex_close(*actual, expected);
    }
    assert_complex_close(result.residual()[1], Complex64::new(7.0, -2.0));
    assert_eq!(result.samples()[0].use_role(), ContinuumChannelUse::FitOnly);
    assert_eq!(
        result.samples()[1].use_role(),
        ContinuumChannelUse::ApplyOnly
    );
}

#[test]
fn continuum_transform_reduces_order_like_casa() {
    let frequencies_hz = [100.0, 150.0, 200.0];
    let samples = [
        ContinuumSample::new(
            Complex64::new(2.0, 4.0),
            false,
            1.0,
            ContinuumChannelUse::FitAndApply,
        ),
        ContinuumSample::new(
            Complex64::new(3.0, 5.0),
            true,
            1.0,
            ContinuumChannelUse::FitAndApply,
        ),
        ContinuumSample::new(
            Complex64::new(4.0, 6.0),
            false,
            1.0,
            ContinuumChannelUse::FitAndApply,
        ),
    ];
    let reduced = fit_and_subtract_continuum(ContinuumRowInput::new(&frequencies_hz, &samples, 2))
        .expect("two valid samples reduce an order-two fit to order one");
    assert_eq!(
        reduced.status(),
        ContinuumFitStatus::Fitted { effective_order: 1 }
    );
    assert_eq!(reduced.coefficients().len(), 2);

    let all_flagged = samples.map(|sample| sample.with_flag(true));
    let pass_through =
        fit_and_subtract_continuum(ContinuumRowInput::new(&frequencies_hz, &all_flagged, 2))
            .expect("CASA passes through a row/correlation with no valid fit samples");
    assert_eq!(pass_through.status(), ContinuumFitStatus::NoValidFitSamples);
    assert!(pass_through.coefficients().is_empty());
    assert_eq!(pass_through.prediction(), &[Complex64::new(0.0, 0.0); 3]);
    assert_eq!(
        pass_through.residual(),
        all_flagged.map(ContinuumSample::visibility)
    );
}

#[test]
fn continuum_transform_preserves_flags_weights_and_application_roles() {
    let frequencies_hz = [100.0, 150.0, 200.0];
    let samples = [
        ContinuumSample::new(
            Complex64::new(1.0, 0.0),
            false,
            2.0,
            ContinuumChannelUse::FitOnly,
        ),
        ContinuumSample::new(
            Complex64::new(5.0, 0.0),
            true,
            3.0,
            ContinuumChannelUse::ApplyOnly,
        ),
        ContinuumSample::new(
            Complex64::new(1.0, 0.0),
            false,
            4.0,
            ContinuumChannelUse::FitAndApply,
        ),
    ];
    let result = fit_and_subtract_continuum(ContinuumRowInput::new(&frequencies_hz, &samples, 0))
        .expect("constant continuum fit");

    assert!(result.samples()[1].flag());
    assert_eq!(result.samples()[1].weight(), 3.0);
    assert_eq!(
        result.samples()[1].use_role(),
        ContinuumChannelUse::ApplyOnly
    );
    assert_complex_close(result.residual()[1], Complex64::new(4.0, 0.0));
    assert!(!result.samples()[0].use_role().contributes_to_output());
    assert!(result.samples()[1].use_role().contributes_to_output());
}
