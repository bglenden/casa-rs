// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, 16-channel cube with linear interpolation onto the
//! LSRK axis, Hogbom per channel, natural and per-channel Briggs weighting;
//! a TOPO cube gridded through LSRK as CASA grids it.

use casa_provider_contracts::{CasaKey, CasaValue, TaskReply};
use serde_json::json;

use super::casa_return::json_run;
use super::fixture::{
    Component, Observation, Product, ProductContract, Products, SpectralAxis, component_pixel,
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

/// CASA writes a `cubedata` cube's axis in no frame (`MFrequency::Undefined`,
/// `SynthesisParamsImage::buildCoordinateSystemCore`): the data's own
/// frequencies, with no Doppler tracking. casacore reads that axis back as
/// observed frequencies; casa-rs writes it the same way (#980).
#[test]
fn a_data_frame_cube_declares_no_frame() {
    use casa_types::{ScalarValue, Value};
    const DATA_CHANNELS: usize = 4;
    let observation = Observation::synthesise_band(DATA_CHANNELS, &[("point", POINT)]);
    let (_, products) = observation.image(
        "cubedata",
        json!({ "specmode": "cubedata", "channel_count": DATA_CHANNELS, "niter": 0 }),
    );
    let image = products.get(".image");
    let record = image.image.coordinates().to_record();
    let Some(Value::Record(spectral)) = record.get("spectral2") else {
        panic!("no spectral2 coordinate in {record:?}");
    };
    assert!(
        matches!(
            spectral.get("system"),
            Some(Value::Scalar(ScalarValue::String(system))) if system == "Undefined"
        ),
        "spectral system {:?}",
        spectral.get("system")
    );
    for (channel, native_hz) in observation.channel_frequencies_hz().iter().enumerate() {
        let image_hz = image.channel_frequency_hz(channel);
        assert!(
            (image_hz - native_hz).abs() <= 1.0e-9 * native_hz,
            "channel {channel}: image {image_hz} Hz, data {native_hz} Hz"
        );
    }
}

/// CASA grids a cube in LSRK whatever its `outframe` (#980): the channels of
/// a TOPO cube of TOPO rows are fixed in LSRK at the first time and each row
/// is Doppler tracked from it, as for an LSRK cube. Starting on the band's
/// edge channel, the TOPO cube's edge keeps at most the first integration,
/// whose rows sit on it, as CASA's `test_cube_1` keeps 552 of 321786 in
/// its; every other integration has drifted off the band there. Its
/// channels so differ in weight and PSF, and the returned `maxpsfsidelobe`
/// is the largest fitted sidelobe of them all. An interior channel takes the
/// LSRK cube's rows at the LSRK frequencies CASA grids them at, so its PSF
/// is the LSRK cube's. Gridding the rows at their own TOPO frequencies kept
/// every row in the edge channels and scaled uv by the TOPO frequency
/// (`test_cube_1`'s sidelobe 0.128 against CASA's 0.138).
#[test]
fn a_topo_cube_of_topo_rows_is_doppler_tracked_as_casa_grids_it() {
    use casa_imaging_deconvolution::{PlaneShape, PsfSummary};
    use casa_ms::MeasurementSet;
    use casa_types::measures::frequency::FrequencyRef;
    const CHANNELS: usize = 6;
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .unwrap();
    ms.save().unwrap();
    drop(ms);

    let directory = observation.scratch("topo-cube");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    let run = |frame: &str| {
        let values = json!({
            "vis": observation.measurement_set(),
            "imagename": directory.join(frame),
            "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
            "backend": "cpu",
            "specmode": "cube",
            "outframe": frame,
            "interpolation": "linear",
            "deconvolver": "hogbom",
            "niter": 10,
        });
        let TaskReply::Succeeded { value, .. } = json_run(&directory, values) else {
            panic!("{frame} cube refused");
        };
        let open = |suffix: &str| Product::open(&directory.join(format!("{frame}{suffix}")));
        (
            sidelobe(&value),
            open(".sumwt"),
            open(".psf"),
            open(".image"),
        )
    };
    let (_, lsrk_sumwt, lsrk_psf, _) = run("LSRK");
    let (topo_sidelobe, topo_sumwt, topo_psf, topo_image) = run("TOPO");

    // The TOPO image keeps the native channels as its axis.
    let native = observation.channel_frequencies_hz();
    for (channel, native_hz) in native.iter().enumerate() {
        let frequency_hz = topo_image.channel_frequency_hz(channel);
        assert!(
            (frequency_hz - native_hz).abs() < 1.0e-3,
            "TOPO channel {channel} at {frequency_hz} Hz, native {native_hz} Hz"
        );
    }
    // The rows of the first integration sit on the band's edge channels to
    // rounding, in either cube; which side of them they fall is a tie.
    let [lsrk_sumwt, topo_sumwt] = [lsrk_sumwt, topo_sumwt].map(|sumwt| sumwt.channel_values());
    let samples = observation.stokes_i_samples_per_channel() as f32;
    let integration = samples / observation.integrations() as f32;
    for channel in 0..CHANNELS {
        assert!(
            (topo_sumwt[channel] - lsrk_sumwt[channel]).abs() <= integration,
            "sumwt of channel {channel}: TOPO {topo_sumwt:?}, LSRK {lsrk_sumwt:?}"
        );
    }
    assert!(
        topo_sumwt[0].min(topo_sumwt[CHANNELS - 1]) <= integration,
        "an edge channel of {topo_sumwt:?} keeps rows Doppler tracking moved off the band"
    );
    // An interior channel grids the LSRK cube's rows at its LSRK frequencies.
    for (channel, sumwt) in topo_sumwt.iter().enumerate().take(CHANNELS - 1).skip(1) {
        assert_eq!(*sumwt, samples, "interior channel {channel}");
        let difference = (&topo_psf.plane(channel) - &lsrk_psf.plane(channel))
            .iter()
            .fold(0.0_f32, |largest, value| largest.max(value.abs()));
        assert!(
            difference < 1.0e-6,
            "PSF of channel {channel} differs from the LSRK cube's by {difference}"
        );
    }
    // The returned sidelobe is the largest of the channels' own.
    let [nx, ny] = geometry.image_size;
    let sidelobes = (0..CHANNELS)
        .filter(|channel| topo_sumwt[*channel] > 0.0)
        .map(|channel| {
            let psf = topo_psf
                .plane(channel)
                .iter()
                .map(|value| f64::from(*value))
                .collect::<Vec<_>>();
            PsfSummary::new(&psf, PlaneShape::new(nx, ny))
                .expect("a weighted PSF plane")
                .sidelobe()
        })
        .collect::<Vec<_>>();
    let largest = sidelobes.iter().copied().fold(0.0_f64, f64::max);
    assert!(
        (topo_sidelobe - largest).abs() <= 1.0e-6 * largest,
        "maxpsfsidelobe {topo_sidelobe}, the channels' sidelobes {sidelobes:?}"
    );
}

/// The `maxpsfsidelobe` of a tclean return.
fn sidelobe(value: &CasaValue) -> f64 {
    let CasaValue::Dict { entries } = value else {
        panic!("not a dictionary: {value:?}");
    };
    let found = entries
        .iter()
        .find(|entry| entry.key == CasaKey::String("maxpsfsidelobe".to_string()))
        .expect("maxpsfsidelobe");
    let CasaValue::Float { value } = &found.value else {
        panic!("maxpsfsidelobe is a float");
    };
    value.0
}
