// SPDX-License-Identifier: LGPL-3.0-or-later
//! `SyntheticCorruptionState::apply` against gains written out by hand.

use num_complex::Complex32;

use super::{
    ObservedWindow, SyntheticCorruptionState, SyntheticPolarizationBasis,
    SyntheticPolarizationSetup, SyntheticSpectralSetup,
};

/// One channel of ones on baseline (0, 1) with `correlations` linear
/// correlations, corrupted by antenna 0's receptor gains `g0` and antenna
/// 1's `g1` alone.
fn corrupt_ones(correlations: usize, g0: [Complex32; 2], g1: [Complex32; 2]) -> Vec<Complex32> {
    let setup = SyntheticPolarizationSetup::new(SyntheticPolarizationBasis::Linear, correlations)
        .expect("supported correlation count");
    let one = Complex32::new(1.0, 0.0);
    let state = SyntheticCorruptionState {
        seed: 0,
        simplenoise_jy: 0.0,
        gains_by_sample: vec![vec![g0, g1]],
        bandpass_gains: vec![vec![one]; 2],
        total_channel_count: 1,
        leakage_terms: vec![[Complex32::new(0.0, 0.0); 2]; 2],
        receptors: setup.correlation_receptors(),
    };
    let spectral = SyntheticSpectralSetup {
        name: "test".to_string(),
        start_frequency_hz: 1.0e9,
        channel_width_hz: 1.0e6,
        channel_count: 1,
    };
    let window = ObservedWindow {
        data_description_id: 0,
        setup: &spectral,
        first_channel: 0,
    };
    let mut values = vec![one; correlations];
    state.apply(&mut values, 0, 1, window, 0);
    values
}

#[test]
fn each_correlation_takes_antenna_1_gain_times_conjugate_antenna_2_gain() {
    // Distinct, non-real gains: a swapped receptor or the conjugate on the
    // other antenna changes every cross product.
    let g0 = [Complex32::new(1.1, 0.2), Complex32::new(0.9, -0.3)];
    let g1 = [Complex32::new(0.8, 0.5), Complex32::new(1.2, -0.1)];
    let product = |p: usize, q: usize| g0[p] * g1[q].conj();

    // XX, XY, YX, YY; XX, YY; XX (CASA's `JonesDiag::applyRight` then
    // `applyLeft` for four, two and one correlations).
    assert_eq!(
        corrupt_ones(4, g0, g1),
        vec![product(0, 0), product(0, 1), product(1, 0), product(1, 1)]
    );
    assert_eq!(corrupt_ones(2, g0, g1), vec![product(0, 0), product(1, 1)]);
    assert_eq!(corrupt_ones(1, g0, g1), vec![product(0, 0)]);
}
