// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's Float channel frequencies affect cancelling Taylor moments (#980).

use casa_imaging_operator::Basis;

#[test]
fn high_frequency_taylor_moments_use_casas_channel_rounding() {
    // The pinned CASA6.7.6.14 refim_alma_mosaic.ms first-divergence probe:
    // MultiTermFTNew::{modifyVisWeights,modifyModelVis} narrows frequency
    // before subtraction, while the image reference stays Double.
    let basis = Basis::Taylor {
        terms: 2,
        reference_hz: 341_400_000_000.0,
    };
    let frequencies = [340_000_000_000.0, 341_400_000_000.0, 342_800_000_000.0];
    let expected = [-0.004_100_719_5_f32, -1.649_677_7e-8, 0.004_100_783];
    let factors = frequencies.map(|frequency| basis.spectral(frequency));
    for (actual, expected) in factors.into_iter().zip(expected) {
        assert!(
            (actual - expected).abs() <= 2.0 * f32::EPSILON * expected.abs(),
            "Taylor variable {actual} versus CASA {expected}"
        );
    }
    // This asymmetric residual is measured in CASA's published sumwt.tt1.
    // Rounding only the final ratio instead yields zero and misses weight.tt1.
    let published_sumwt = (factors.into_iter().map(f64::from).sum::<f64>() * 173_376.0) as f32;
    assert!((published_sumwt - 0.008_119_745).abs() <= 1.0e-9);
    assert_eq!(Basis::Constant.spectral(frequencies[0]), 0.0);
    assert_eq!(
        Basis::ChannelLocal { planes: 3 }.spectral(frequencies[0]),
        0.0
    );
}
