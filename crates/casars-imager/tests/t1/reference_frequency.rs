// SPDX-License-Identifier: LGPL-3.0-or-later
//! `reffreq` (tclean contract 35), as `SynthesisParamsImage::getImFreq`
//! reads it for a continuum image: the header's spectral reference at pixel
//! 0, with the band widened by twice its offset from the band's centre
//! (CAS-9518), and an `mtmfs` Taylor expansion's reference frequency.

use serde_json::json;

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: -0.7,
};

#[test]
fn a_continuum_reffreq_is_the_headers_reference_with_casas_widened_band() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let (_, plain) = observation.image("plain", json!({"niter": 0}));
    let psf = plain.get(".psf");
    let (centre_hz, width_hz) = (psf.channel_frequency_hz(0), psf.channel_width_hz());
    let reference_hz = centre_hz + 0.3 * width_hz;
    let (_, given) = observation.image(
        "given",
        json!({"niter": 0, "reffreq": format!("{reference_hz}Hz")}),
    );
    let psf_given = given.get(".psf");
    assert_eq!(psf_given.channel_frequency_hz(0), reference_hz);
    let widened = width_hz + 2.0 * (reference_hz - centre_hz).abs();
    assert!(
        (psf_given.channel_width_hz() - widened).abs() <= 1e-9 * widened,
        "{} vs {widened}",
        psf_given.channel_width_hz()
    );
    // A single plane grids each sample at its own frequency, whatever the
    // header's reference.
    assert_eq!(psf_given.channel_values(), psf.channel_values());
}

#[test]
fn an_mtmfs_expansion_takes_reffreq_as_its_reference() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let controls = |reffreq: Option<f64>| {
        let mut controls = json!({"niter": 0, "deconvolver": "mtmfs", "nterms": 2});
        if let Some(hz) = reffreq {
            controls["reffreq"] = json!(format!("{hz}Hz"));
        }
        controls
    };
    let (_, plain) = observation.image("mtmfs-plain", controls(None));
    let centre_hz = plain.get(".psf.tt0").channel_frequency_hz(0);
    let reference_hz = centre_hz * 1.01;
    let (_, given) = observation.image("mtmfs-given", controls(Some(reference_hz)));
    let (tt0, tt1) = (
        plain.get(".psf.tt0").channel_values(),
        plain.get(".psf.tt1").channel_values(),
    );
    assert_eq!(given.get(".psf.tt0").channel_values(), tt0);
    // Σw(ν/ν₁ − 1) = (ν₀/ν₁)·Σw(ν/ν₀ − 1 + 1) − Σw, normalized alike.
    let ratio = centre_hz / reference_hz;
    let given_tt1 = given.get(".psf.tt1").channel_values();
    let peak = tt0.iter().copied().fold(0.0_f32, f32::max);
    for ((&old, &zero), &new) in tt1.iter().zip(&tt0).zip(&given_tt1) {
        let expected = ratio * f64::from(old + zero) - f64::from(zero);
        assert!(
            (f64::from(new) - expected).abs() <= 1e-5 * f64::from(peak),
            "{new} vs {expected}"
        );
    }
}
