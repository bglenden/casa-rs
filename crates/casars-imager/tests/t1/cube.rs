// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, 16-channel cube with linear interpolation onto the
//! LSRK axis, Hogbom per channel, natural and per-channel Briggs weighting.

use serde_json::json;

use super::fixture::{
    Component, Observation, ProductContract, Products, SpectralAxis, component_pixel,
    fit_psf_main_lobe, off_source_rms, peak,
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
        json!({ "weighting": "natural", "write_pb": true }),
    );
    let briggs = image(
        &observation,
        "cube-briggs",
        json!({ "weighting": "briggs", "robust": 0.5, "write_pb": true }),
    );

    // Every product's LSRK axis lies within half a channel of the native
    // centres, and each channel's `.pb` is the Q-band beam at its frequency.
    for products in [&natural, &briggs] {
        observation.assert_product_contract(
            products,
            &ProductContract {
                inventory: &[
                    ".image",
                    ".mask",
                    ".model",
                    ".pb",
                    ".psf",
                    ".residual",
                    ".sumwt",
                ],
                spectral: SpectralAxis::NativeChannels,
                natural: false,
                pblimit: 0.2,
                uncorrected_masks: true,
            },
        );
    }
    // The LSRK axis keeps the native width to the Doppler factor.
    let image = natural.get(".image");
    let native = observation.channel_frequencies_hz();
    let width_hz = native[1] - native[0];
    assert!(
        (image.channel_width_hz() / width_hz - 1.0).abs() < 1.0e-4,
        "image channel width {} Hz, native {width_hz} Hz",
        image.channel_width_hz()
    );

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

/// CASA zeroes a plane whose `.sumwt` is positive but at most 1e-7
/// (`SIImageStore::divideImageByWeightVal`): its PSF, residual, model and
/// image are zero and nothing is cleaned there. Native channels 6 to 8 carry
/// weights small enough that image channel 7, which linear interpolation
/// takes only from them, sums to about 1e-8; every other channel keeps the
/// row's `WEIGHT` and images as before (#980).
#[test]
fn a_plane_at_casas_empty_sum_weight_bound_is_empty() {
    use casa_ms::{MeasurementSet, OptionalMainColumn};
    use casa_types::{ArrayValue, Value};
    const TINY: std::ops::Range<usize> = 6..9;
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    if !ms
        .main_table()
        .schema()
        .unwrap()
        .contains_column("WEIGHT_SPECTRUM")
    {
        let schema = casa_ms::column_def::build_table_schema(&[
            *OptionalMainColumn::WeightSpectrum.column_def(),
        ])
        .unwrap();
        ms.main_table_mut()
            .add_column(schema.column("WEIGHT_SPECTRUM").unwrap().clone(), None)
            .unwrap();
    }
    let rows = ms.row_count();
    for row in 0..rows {
        let ArrayValue::Float32(weight) = ms.weight_column().get(row).unwrap().clone() else {
            panic!("Float WEIGHT");
        };
        let ArrayValue::Bool(flags) = ms.flag_column().get(row).unwrap().clone() else {
            panic!("boolean FLAG");
        };
        let npol = weight.len();
        let tiny = 1.0e-7 / (8.0 * (npol * rows) as f32);
        let spectrum = ndarray::ArrayD::from_shape_fn(flags.raw_dim(), |index| {
            if TINY.contains(&index[1]) {
                tiny
            } else {
                weight[[index[0]]]
            }
        });
        ms.main_table_mut()
            .cell_accessor_mut(row, "WEIGHT_SPECTRUM")
            .unwrap()
            .set(Value::Array(ArrayValue::Float32(spectrum)))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);

    let products = image(&observation, "empty-bound", json!({"weighting": "natural"}));
    let sumwt = products.get(".sumwt").channel_values();
    assert!(
        sumwt[7] > 0.0 && f64::from(sumwt[7]) <= 1.0e-7,
        "channel 7 sumwt {} is not a positive empty-bound weight",
        sumwt[7]
    );
    for suffix in [".psf", ".residual", ".model", ".image"] {
        let plane = products.get(suffix).plane(7);
        assert!(
            plane.iter().all(|value| *value == 0.0),
            "{suffix} channel 7 is not CASA's zero plane"
        );
    }
    let (peak_jy, peak_pixel, _) = peak(&products.get(".image").plane(3));
    assert_eq!(peak_pixel, component_pixel(POINT));
    assert!(
        peak_jy > 0.5,
        "an ordinary channel images: peak {peak_jy} Jy"
    );
}
