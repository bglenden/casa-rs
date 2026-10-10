// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, 16-channel cube with linear interpolation onto the
//! LSRK axis, Hogbom per channel, natural and per-channel Briggs weighting.

use serde_json::json;

use super::fixture::{
    Component, Observation, Products, component_pixel, fit_psf_main_lobe, off_source_rms, peak,
};

const CHANNELS: usize = 16;
/// A steep spectrum: the flux falls by 9% across the 2 GHz band.
const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: -2.0,
};
/// Five times the 2 mJy thermal noise of one channel.
const THRESHOLD: &str = "0.01Jy";

/// The cube under `weighting`, the weighting parameters.
fn image(observation: &Observation, name: &str, weighting: serde_json::Value) -> Products {
    let mut controls = json!({
        "specmode": "cube",
        "channel_count": CHANNELS,
        "perchanweightdensity": true,
        "deconvolver": "hogbom",
        "niter": 2000,
        "threshold": THRESHOLD,
    });
    controls
        .as_object_mut()
        .expect("cube controls")
        .extend(weighting.as_object().expect("weighting").clone());
    observation.image(name, controls).1
}

/// Channels whose every row brackets the image frequency with two native
/// channels: all but the two edge channels, which the LSRK shift may leave
/// partly covered.
fn interior() -> std::ops::Range<usize> {
    1..CHANNELS - 1
}

/// The restored peak of every channel with data follows `S(ν)` at the
/// channel's own frequency; only an edge channel may be blank.
fn assert_follows_the_spectrum(observation: &Observation, products: &Products, label: &str) {
    let image = products.get(".image");
    let sumwt = products.get(".sumwt").channel_values();
    assert_eq!(image.channels(), CHANNELS);
    assert_eq!(sumwt.len(), CHANNELS);
    for (channel, sumwt) in sumwt.iter().enumerate() {
        if *sumwt == 0.0 {
            assert!(
                !interior().contains(&channel),
                "{label}: interior channel {channel} is blank"
            );
            continue;
        }
        let frequency_hz = image.channel_frequency_hz(channel);
        let expected_jy = POINT.flux_at(frequency_hz);
        let (peak_jy, peak_pixel, _) = peak(&image.plane(channel));
        assert_eq!(
            peak_pixel,
            component_pixel(POINT),
            "{label} channel {channel}"
        );
        // Thermal noise of one channel is 2 mJy and the clean threshold
        // 10 mJy; 1.5% of the ~1 Jy flux covers both.
        assert!(
            (f64::from(peak_jy) - expected_jy).abs() < 0.015 * expected_jy,
            "{label} channel {channel} at {frequency_hz} Hz: peak {peak_jy} Jy, \
             S(ν) {expected_jy} Jy (native noise {} Jy)",
            observation.channel_noise_jy()
        );
    }
}

#[test]
fn sixteen_channel_cube_follows_the_injected_spectrum_under_natural_and_briggs() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let natural = image(
        &observation,
        "cube-natural",
        json!({ "weighting": "natural" }),
    );
    let briggs = image(
        &observation,
        "cube-briggs",
        json!({ "weighting": "briggs", "robust": 0.5 }),
    );

    // The LSRK axis keeps the native width to the Doppler factor and lies
    // within half a channel of the native centres.
    let image = natural.get(".image");
    observation.assert_direction_and_stokes(image);
    let native = observation.channel_frequencies_hz();
    let width_hz = native[1] - native[0];
    assert!(
        (image.channel_width_hz() / width_hz - 1.0).abs() < 1.0e-4,
        "image channel width {} Hz, native {width_hz} Hz",
        image.channel_width_hz()
    );
    for (channel, native_hz) in native.iter().enumerate() {
        assert!(
            (image.channel_frequency_hz(channel) - native_hz).abs() < 0.5 * width_hz,
            "channel {channel}"
        );
    }

    assert_follows_the_spectrum(&observation, &natural, "natural");
    assert_follows_the_spectrum(&observation, &briggs, "briggs");

    // Natural weighting of unit weights: an interior channel's sumwt counts
    // the Stokes I samples of one native channel (linear interpolation of
    // two unit weights is one).
    let natural_sumwt = natural.get(".sumwt").channel_values();
    let briggs_sumwt = briggs.get(".sumwt").channel_values();
    for channel in interior() {
        assert_eq!(
            f64::from(natural_sumwt[channel]),
            observation.stokes_i_samples_per_channel() as f64,
            "natural sumwt of channel {channel}"
        );
        // Briggs divides every weight by 1 + f²·density.
        assert!(
            briggs_sumwt[channel] < natural_sumwt[channel],
            "channel {channel}: Briggs sumwt {} not below natural {}",
            briggs_sumwt[channel],
            natural_sumwt[channel]
        );
    }

    // The PSF main lobe shrinks as 1/ν across the band.
    let psf = natural.get(".psf");
    let [first, last] = [interior().start, interior().end - 1];
    let lobe = |products: &Products, channel: usize| {
        let [major, minor] = fit_psf_main_lobe(&products.get(".psf").plane(channel), 0.35);
        (major * minor).sqrt()
    };
    let scaled = |channel: usize| lobe(&natural, channel) * psf.channel_frequency_hz(channel);
    assert!(
        (scaled(last) / scaled(first) - 1.0).abs() < 0.02,
        "PSF width × ν: channel {first} {}, channel {last} {}",
        scaled(first),
        scaled(last)
    );

    // Briggs trades sensitivity for resolution: a narrower main lobe, and a
    // residual no quieter than natural weighting's minimum-variance one.
    let middle = CHANNELS / 2;
    assert!(
        lobe(&briggs, middle) < lobe(&natural, middle),
        "Briggs PSF lobe {} not narrower than natural {}",
        lobe(&briggs, middle),
        lobe(&natural, middle)
    );
    let rms = |products: &Products| {
        off_source_rms(
            &products.get(".residual").plane(middle),
            &[component_pixel(POINT)],
            24.0,
        )
    };
    assert!(
        rms(&briggs) >= 0.99 * rms(&natural),
        "Briggs residual RMS {} below natural {}",
        rms(&briggs),
        rms(&natural)
    );
}
