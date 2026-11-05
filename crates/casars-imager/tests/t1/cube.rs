// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, 16-channel cube with linear interpolation onto the
//! LSRK axis, Hogbom per channel, natural and per-channel Briggs weighting;
//! a TOPO cube gridded through LSRK as CASA grids it.

use casa_provider_contracts::{CasaKey, CasaValue, TaskReply};
use casa_types::Complex32;
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

/// CASA images a cube over several windows on one axis laid over their
/// channels merged into one (`MSTransformRegridder::combineSpwsCore`) and
/// grids every window's rows onto it (#980). Two adjacent ten-channel
/// windows make a twenty-channel cube, and away from the windows' edges
/// each plane is the plane of the cube of the window holding its channel:
/// a row interpolates only between its own window's channels, and a
/// per-channel Briggs density sees only the samples on its plane. At an
/// edge whether the plane lies a rounding inside the window's span decides
/// whether its rows reach it, as in CASA's gridder.
#[test]
fn a_cube_over_two_windows_is_the_cubes_of_its_windows_side_by_side() {
    const WINDOW_CHANNELS: usize = 10;
    let (observation, low) = adjacent_windows(WINDOW_CHANNELS);
    let [both, windows @ ..] = window_cubes(&observation, "lsrk");
    let image = both.get(".image");
    let sumwt = both.get(".sumwt").channel_values();
    assert_eq!(sumwt.len(), 2 * WINDOW_CHANNELS);
    for (channel, sumwt) in sumwt.iter().enumerate() {
        let native_hz = low.start_frequency_hz + channel as f64 * low.channel_width_hz;
        let frequency_hz = image.channel_frequency_hz(channel);
        assert!(
            (frequency_hz - native_hz).abs() <= 1.0e-9 * native_hz,
            "channel {channel} at {frequency_hz} Hz, native {native_hz} Hz"
        );
        assert!(*sumwt > 0.0, "channel {channel} holds no samples");
    }
    assert_interior_planes_match(&observation, &both, &windows, WINDOW_CHANNELS);
}

/// The same law for TOPO windows, Doppler tracked onto an LSRK axis: the
/// per-channel Briggs density is then padded by CASA's swing of the
/// windows' channels (`BriggsCubeWeightor::estimateSwingChanPad`), which
/// differs between the cubes but not for an interior plane.
#[test]
fn a_cube_over_two_topo_windows_is_the_cubes_of_its_windows_side_by_side() {
    use casa_ms::MeasurementSet;
    use casa_types::measures::frequency::FrequencyRef;
    const WINDOW_CHANNELS: usize = 10;
    let (observation, _) = adjacent_windows(WINDOW_CHANNELS);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    for window in 0..2 {
        ms.spectral_window_mut()
            .unwrap()
            .set_i32(window, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    let [both, windows @ ..] = window_cubes(&observation, "topo");
    assert_interior_planes_match(&observation, &both, &windows, WINDOW_CHANNELS);
}

/// Per-channel Briggs cubes of every window (`spw='*'`) and of each of the
/// two windows alone.
fn window_cubes(observation: &Observation, label: &str) -> [Products; 3] {
    ["*", "0", "1"].map(|spw| {
        observation
            .image(
                &format!("{label}-window-{}", spw.replace('*', "all")),
                json!({
                    "specmode": "cube", "spw": spw, "niter": 0,
                    "weighting": "briggs", "robust": 0.5, "perchanweightdensity": true,
                }),
            )
            .1
    })
}

/// Each plane of the cube over `windows.len()` windows of `window_channels`
/// channels, but for the first and last of each window, is the plane of
/// its window's own cube: its sum of weights, PSF and residual.
fn assert_interior_planes_match(
    observation: &Observation,
    both: &Products,
    windows: &[Products],
    window_channels: usize,
) {
    assert_eq!(
        both.get(".image").channels(),
        windows.len() * window_channels
    );
    let sumwt = both.get(".sumwt").channel_values();
    let samples = observation.stokes_i_samples_per_channel() as f32;
    for (window, alone) in windows.iter().enumerate() {
        let alone_sumwt = alone.get(".sumwt").channel_values();
        let interior = alone_sumwt
            .iter()
            .enumerate()
            .take(window_channels - 1)
            .skip(1);
        for (plane, alone_sumwt) in interior {
            let channel = window * window_channels + plane;
            assert!(
                (sumwt[channel] - alone_sumwt).abs() <= 1.0e-6 * samples,
                "sumwt of channel {channel}: {} against window {window}'s {alone_sumwt}",
                sumwt[channel],
            );
            for suffix in [".psf", ".residual"] {
                let ours = both.get(suffix).plane(channel);
                let theirs = alone.get(suffix).plane(plane);
                let scale = theirs
                    .iter()
                    .fold(0.0_f32, |peak, value| peak.max(value.abs()));
                let difference = (&ours - &theirs)
                    .iter()
                    .fold(0.0_f32, |largest, value| largest.max(value.abs()));
                assert!(
                    difference <= 1.0e-5 * scale,
                    "{suffix} channel {channel} differs from window {window}'s plane {plane} \
                     by {difference} of {scale}"
                );
            }
        }
    }
}

/// `BriggsCubeWeightor` pads the per-channel density of every cube, one
/// channel included (`estimateSwingChanPad` counts the swing in the image's
/// own channel increment), so a one-channel Briggs cube of TOPO data images
/// (#980; `test_onefield_pcwdT_and_pcwdF` was refused): its weights are
/// Briggs weights, below natural weighting's.
#[test]
fn a_one_channel_cube_has_a_per_channel_briggs_density() {
    use casa_ms::MeasurementSet;
    use casa_types::measures::frequency::FrequencyRef;
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let sumwt = |name: &str, weighting: serde_json::Value| {
        let mut controls = json!({ "specmode": "cube", "nchan": 1, "start": 1, "niter": 0 });
        controls
            .as_object_mut()
            .expect("controls")
            .extend(weighting.as_object().expect("weighting").clone());
        observation
            .image(name, controls)
            .1
            .get(".sumwt")
            .channel_values()[0]
    };
    let natural = sumwt("one-natural", json!({ "weighting": "natural" }));
    let briggs = sumwt(
        "one-briggs",
        json!({ "weighting": "briggs", "robust": 0.5, "perchanweightdensity": true }),
    );
    assert!(
        briggs > 0.0 && briggs < natural,
        "Briggs sumwt {briggs}, natural {natural}"
    );
}

/// A cube reads a window its axis does not reach as `spw` selects it,
/// tuned or not, and its `VisImagingWeight` density counts that window's
/// samples, as CASA's does (#980): CASA forms the density over the
/// untuned selection of the imager defining the cube, and the tuned
/// selection of the imager gridding it keeps a window none of whose bands
/// the image's band overlaps. CASA 6.7.6.14, `refim_point.ms` split into
/// two ten-channel windows, a five-channel uniform cube on the first:
/// `spw='*'`'s sum of weights and PSF differ from `spw='0'`'s (PSF by 14%),
/// and alike whether the selection is tuned (`savemodel='modelcolumn'`) or
/// not. Over two adjacent 30-channel windows here, the first window's
/// buffer tunes the selection; the cube of both windows grids the samples
/// of the first's alone, and weighs them otherwise: the second window's
/// samples add to the uniform density, so every plane's sum of weights
/// falls, and with them flagged the cube of both windows weighs as the
/// first's alone.
#[test]
fn a_cube_density_counts_a_window_its_axis_misses() {
    use casa_ms::MeasurementSet;
    use casa_types::{ScalarValue, Value};
    let (observation, _) = adjacent_windows(30);
    let cube = |name: &str, spw: &str| {
        observation.image(
            name,
            json!({
                "specmode": "cube", "spw": spw, "nchan": 10, "niter": 0,
                "weighting": "uniform", "perchanweightdensity": false,
            }),
        )
    };
    let (both_report, both) = cube("tuned-both", "*");
    let (alone_report, alone) = cube("tuned-alone", "0");
    assert!(alone_report.gridded_samples > 0);
    assert_eq!(both_report.gridded_samples, alone_report.gridded_samples);
    let [ours, theirs] = [&both, &alone].map(|products| products.get(".sumwt").channel_values());
    for (channel, (ours, theirs)) in ours.iter().zip(&theirs).enumerate() {
        assert!(
            *ours < theirs * (1.0 - 1.0e-3),
            "sumwt of channel {channel}: {ours} with the missed window, {theirs} without"
        );
    }
    // The missed window's rows flagged, its samples leave the density.
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    for row in 0..ms.row_count() {
        let description = ms
            .main_table()
            .cell_accessor(row, "DATA_DESC_ID")
            .unwrap()
            .scalar()
            .unwrap()
            .clone();
        if description == ScalarValue::Int32(1) {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, "FLAG_ROW", Value::Scalar(ScalarValue::Bool(true)))
                .unwrap();
        }
    }
    ms.save().unwrap();
    drop(ms);
    let flagged = cube("tuned-both-flagged", "*")
        .1
        .get(".sumwt")
        .channel_values();
    for (channel, (flagged, theirs)) in flagged.iter().zip(&theirs).enumerate() {
        assert!(
            (flagged - theirs).abs() <= 1.0e-6 * theirs,
            "sumwt of channel {channel}: {flagged} with the missed window flagged, {theirs} \
             without it"
        );
    }
}

/// A cube with per-channel Briggs density pads its density cube whatever
/// its channel count and interpolation, as CASA's `BriggsCubeWeightor`
/// always does (`estimateSwingChanPad`, whose edge step is the image's
/// increment for one channel), so a one-channel cube
/// (`test_onefield_pcwdT_and_pcwdF.1`: `refim_twochan.ms`, `spw='0:1'`) and
/// a nearest-interpolation cube image, with a sum of weights on every plane
/// and a PSF peaking at 1 (#980). Padding only linearly interpolated cubes
/// of several channels left both without the density padding their
/// per-channel weighting needs, and the run refused them.
#[test]
fn a_one_channel_or_nearest_cube_pads_its_per_channel_density() {
    let observation = Observation::synthesise_band(2, &[("point", POINT)]);
    for (name, controls) in [
        (
            "one-channel-briggs",
            json!({
                "specmode": "cube", "nchan": 1, "spw": "0:1", "perchanweightdensity": true,
                "weighting": "briggs", "niter": 0,
            }),
        ),
        (
            "nearest-briggs",
            json!({
                "specmode": "cube", "interpolation": "nearest", "perchanweightdensity": true,
                "weighting": "briggs", "niter": 0,
            }),
        ),
    ] {
        let (_, products) = observation.image(name, controls);
        let sumwt = products.get(".sumwt").channel_values();
        assert!(
            !sumwt.is_empty() && sumwt.iter().all(|sumwt| sumwt.is_finite() && *sumwt > 0.0),
            "{name}: sumwt {sumwt:?}"
        );
        let psf = products.get(".psf");
        assert_eq!(psf.channels(), sumwt.len(), "{name}");
        for channel in 0..psf.channels() {
            let peak = psf
                .plane(channel)
                .iter()
                .fold(f32::NEG_INFINITY, |peak, value| peak.max(*value));
            assert!(
                peak.is_finite() && (peak - 1.0).abs() < 1.0e-3,
                "{name}: PSF peak {peak} on plane {channel}"
            );
        }
    }
}

/// tclean selects a `cubedata` cube's channels in the data's frame
/// (`ignoreframe`: its selection frame is `Undefined`), by a band from the
/// lowest to the highest channel centre of each range `spw` names, so a
/// range with a stride is read whole, as a `cube`'s is (#980): the cube of
/// `spw='0:0~10^2'` is the cube of `spw='0:0~10'`. Reading the named
/// channels alone filled each odd plane by interpolating its neighbours'
/// noisy samples, where it holds its own channel's.
#[test]
fn a_cubedata_range_with_a_stride_is_read_whole() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let cube = |name: &str, spw: &str| {
        observation
            .image(
                name,
                json!({
                    "specmode": "cubedata", "spw": spw, "start": 0, "width": 1, "nchan": 11,
                    "niter": 0,
                }),
            )
            .1
    };
    let strided = cube("cubedata-strided", "0:0~10^2");
    let whole = cube("cubedata-whole", "0:0~10");
    assert_eq!(
        strided.get(".sumwt").channel_values(),
        whole.get(".sumwt").channel_values()
    );
    let [strided, whole] = [&strided, &whole].map(|products| products.get(".image").values());
    let peak = whole
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    let worst = strided
        .iter()
        .zip(&whole)
        .fold(0.0_f32, |worst, (a, b)| worst.max((a - b).abs()));
    assert!(
        worst <= 1.0e-6 * peak,
        "the strided cube differs from the whole range's by {worst} of a peak {peak}"
    );
}

/// `interpolation='cubic'` (#1066) grids each output sample from the cubic
/// through the four native channels about it and predicts the model at
/// native channels by the cubic through four output points
/// (`FTMachine::interpolateFrequencyTogrid`, `interpolateFrequencyFromgrid`):
/// the cleaned cube follows the injected spectrum as the linear one does,
/// its residual formed at native channels through the four-point sources.
#[test]
fn a_cubic_cube_follows_the_injected_spectrum() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let cubic = image(
        &observation,
        "cube-cubic",
        json!({ "weighting": "natural", "interpolation": "cubic" }),
    );
    assert_follows_the_spectrum(&observation, &cubic, "cubic");
    let residual = cubic.get(".residual").values();
    let image = cubic.get(".image").values();
    let peak = image
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    let worst = residual
        .iter()
        .fold(0.0_f32, |worst, value| worst.max(value.abs()));
    assert!(
        worst < 0.05 * peak,
        "the cubic residual {worst} is not cleaned below the peak {peak}"
    );
}

/// tclean's `nchan=-1` takes CASA's count (`MSTransformRegridder::
/// regridChanBounds` with `nchan < 0`, #1066): from the first selected
/// channel when `start` is unset, as many channels of the output width as
/// fit to the band's far edge. CASA 6.7.6.14 gives `spw='0:2~4'` three
/// channels from the third, `width=2` the centres of the pairs, and
/// `start=5` every channel from the sixth (probed on `refim_point.ms`).
#[test]
fn a_cube_without_nchan_counts_the_channels_from_its_start() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let native = observation.channel_frequencies_hz();
    let planes = |name: &str, extra: serde_json::Value| {
        let mut controls = json!({ "specmode": "cubedata", "nchan": -1, "niter": 0 });
        controls
            .as_object_mut()
            .expect("cube controls")
            .extend(extra.as_object().expect("controls").clone());
        let image = observation.image(name, controls).1;
        let image = image.get(".image");
        (0..image.channels())
            .map(|channel| image.channel_frequency_hz(channel))
            .collect::<Vec<_>>()
    };
    let close = |image: &[f64], expected: &[f64]| {
        image.len() == expected.len()
            && image
                .iter()
                .zip(expected)
                .all(|(image, expected)| (image - expected).abs() <= 1.0e-9 * expected)
    };
    let selected = planes("derived-selection", json!({ "spw": "0:2~4" }));
    assert!(close(&selected, &native[2..=4]), "{selected:?}");
    let paired = planes("derived-width", json!({ "width": 2 }));
    let pairs = native
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[low, high]| (low + high) / 2.0)
        .collect::<Vec<_>>();
    assert!(close(&paired, &pairs), "{paired:?}");
    let started = planes("derived-start", json!({ "start": 5 }));
    assert!(close(&started, &native[5..]), "{started:?}");
    // A frequency grid (`regridChanBounds`' "freq" branch with nchan < 0):
    // from `start`'s lower edge to the band's upper edge in whole widths,
    // with CASA's 1% tolerance.
    let spacing_hz = native[1] - native[0];
    let (start_hz, width_hz) = (native[2] + 0.3 * spacing_hz, 2.0 * spacing_hz);
    let upper_edge_hz = native[native.len() - 1] + spacing_hz / 2.0;
    let count =
        ((upper_edge_hz - (start_hz - width_hz / 2.0) + 0.01 * width_hz) / width_hz).floor();
    let expected = (0..count as usize)
        .map(|plane| start_hz + plane as f64 * width_hz)
        .collect::<Vec<_>>();
    let frequency = planes(
        "derived-frequency",
        json!({ "start": format!("{start_hz}Hz"), "width": format!("{width_hz}Hz") }),
    );
    assert!(
        close(&frequency, &expected),
        "{frequency:?} against {expected:?}"
    );
}

/// CASA's visibility iterator selects a cube's channels at each time by
/// frequency (`vi::FrequencySelectionUsingFrame`, #980): by the LSRK band
/// from the lowest to the highest frequency the channels `spw` names take
/// over the track, narrowed to the image's band, a time reads the channels
/// whose extent meets the band converted to that time. With TOPO channels
/// narrower than the LSRK drift across the track, the channels a time
/// reads move with it, so the edge plane on the band's end is bracketed by
/// two read channels at about half the times, as the plane's frequency
/// falls in the upper or lower half of the last channel read: CASA's
/// `test_cube_D2` keeps 2460.56 of 5020.51 there. Every other plane keeps
/// every sample. Reading every channel at every time kept the edge plane
/// whole.
#[test]
fn a_cube_reads_at_each_time_the_channels_its_band_selects_then() {
    use casa_ms::MeasurementSet;
    use casa_types::measures::frequency::FrequencyRef;
    const NAMED: usize = 30;
    // 2 kHz channels at 44 GHz: an hour's LSRK drift is several channels.
    let mut setup = super::fixture::Setup::vla_q_band(NAMED + 10);
    setup.spectral_windows[0].channel_width_hz = 2.0e3;
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let (_, products) = observation.image(
        "drift",
        json!({
            "specmode": "cube", "spw": format!("0:5~{}", 4 + NAMED), "start": 5, "width": 1,
            "nchan": NAMED, "niter": 0,
        }),
    );
    let sumwt = products.get(".sumwt").channel_values();
    let samples = observation.stokes_i_samples_per_channel() as f32;
    assert_eq!(sumwt.len(), NAMED);
    for (channel, sumwt) in sumwt.iter().enumerate().take(NAMED - 1).skip(1) {
        assert_eq!(*sumwt, samples, "interior channel {channel}");
    }
    let [low, high] = [sumwt[0], sumwt[NAMED - 1]];
    let edge = low.min(high);
    assert_eq!(
        low.max(high),
        samples,
        "edges {low} and {high} of {samples}"
    );
    assert!(
        (0.3 * samples..=0.7 * samples).contains(&edge),
        "the edge plane on the band's end keeps {edge} of {samples}"
    );
}

/// A cube's density without `perchanweightdensity` counts every channel `spw`
/// selects, whatever the axis reaches: `SynthesisImagerVi2::weight` forms
/// `VisImagingWeight` on the parent imager's iterator (#1137). CASA 6.7.6.14
/// on `refim_point.ms` gives a uniform cube of 5 of its 20 channels the same
/// sums of weights as those planes of a cube of all 20, where counting only
/// the channels reached gave 61.3 against 26.4. So do these, of a window
/// read whole and of what `spw` names, and a range with a stride counts
/// whole, as CASA gives `0:0~10^2` the sums of `0:0~10`.
#[test]
fn a_whole_selection_density_counts_channels_the_axis_misses() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let sumwt = |name: &str, controls: serde_json::Value| {
        let mut values = json!({
            "specmode": "cube", "weighting": "uniform", "perchanweightdensity": false,
            "interpolation": "linear", "niter": 0,
        });
        values
            .as_object_mut()
            .unwrap()
            .extend(controls.as_object().unwrap().clone());
        observation
            .image(name, values)
            .1
            .get(".sumwt")
            .channel_values()
    };
    let assert_close = |part: &[f32], whole: &[f32], label: &str| {
        for (plane, (part, whole)) in part.iter().zip(whole).enumerate() {
            assert!(
                (part - whole).abs() <= 1.0e-3 * whole.abs(),
                "{label}, plane {plane}: {part} against {whole}"
            );
        }
    };
    let whole = sumwt(
        "whole",
        json!({ "start": 0, "width": 1, "nchan": CHANNELS }),
    );
    let part = sumwt("part", json!({ "start": 4, "width": 1, "nchan": 4 }));
    assert_close(&part, &whole[4..8], "a part of the window");
    let one = sumwt("one", json!({ "start": 6, "width": 1, "nchan": 1 }));
    assert_close(&one, &whole[6..7], "one channel of the window");
    let named = sumwt(
        "named",
        json!({ "spw": "0:2~13", "start": 2, "width": 1, "nchan": 12 }),
    );
    let named_part = sumwt(
        "named-part",
        json!({ "spw": "0:2~13", "start": 4, "width": 1, "nchan": 4 }),
    );
    assert_close(&named_part, &named[2..6], "a part of what spw names");
    let strided = sumwt(
        "strided",
        json!({ "spw": "0:0~10^2", "start": 0, "width": 1, "nchan": 11 }),
    );
    let contiguous = sumwt(
        "contiguous",
        json!({ "spw": "0:0~10", "start": 0, "width": 1, "nchan": 11 }),
    );
    assert_close(&strided, &contiguous, "a range with a stride");
}

/// A track of 40 TOPO channels of 2 kHz at 44 GHz, where the LSRK drift is
/// several channels wide.
fn drifting_track() -> Observation {
    drifting_track_with_width(2.0e3)
}

/// The same track with a chosen native channel width.
fn drifting_track_with_width(width_hz: f64) -> Observation {
    use casa_ms::MeasurementSet;
    use casa_types::measures::frequency::FrequencyRef;
    let mut setup = super::fixture::Setup::vla_q_band(40);
    setup.spectral_windows[0].channel_width_hz = width_hz;
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    observation
}

/// A whole-selection density selects each time's channels by the untuned
/// bands of `spw`, so they move with the time's Doppler shift, and it counts
/// channels `spw` doesn't name at the times they come into its band. These
/// sums of weights are CASA 6.7.6.14's on this track, saved
/// (`tclean-density-channels-cases.json`). Counting the named channels at
/// every time gave 2.3 times these on the short range. The first plane sits on
/// the band's end, where the first integration's rows fall on either side by
/// rounding (64.2 here, 66.5 on Linux), so it isn't compared.
#[test]
fn a_whole_selection_density_follows_each_times_bands() {
    let observation = drifting_track();
    for (label, weighting, spw, start, nchan, casa) in [
        (
            "short",
            "uniform",
            "0:18~21",
            18,
            4,
            &[64.2052, 143.746, 143.744, 143.744][..],
        ),
        (
            "superuniform",
            "superuniform",
            "0:18~21",
            18,
            4,
            &[1.99163, 4.21554, 4.21554, 4.21554][..],
        ),
        (
            "part",
            "uniform",
            "0:5~34",
            5,
            4,
            &[17.3309, 37.4347, 37.4347, 37.4347][..],
        ),
    ] {
        let (_, products) = observation.image(
            label,
            json!({
                "specmode": "cube", "weighting": weighting, "perchanweightdensity": false,
                "spw": spw, "start": start, "width": 1, "nchan": nchan,
                "interpolation": "linear", "niter": 0, "imsize": 128, "cell": "0.012arcsec",
            }),
        );
        let sumwt = products.get(".sumwt").channel_values();
        assert_eq!(sumwt.len(), casa.len(), "{label}");
        for (plane, (ours, theirs)) in sumwt.iter().zip(casa).enumerate().skip(1) {
            let (ours, theirs) = (f64::from(*ours), *theirs);
            assert!(
                (ours - theirs).abs() <= 1.0e-3 * theirs,
                "{label}, plane {plane}: {ours} against CASA's {theirs}"
            );
        }
    }
}

/// Over `drifting_track`, the rows in which each of its channels holds a
/// written model, in row order, which is time order: a cube on channels 5 to
/// 34 with `controls` added.
fn model_written_over_a_drift(label: &str, controls: serde_json::Value) -> Vec<Vec<bool>> {
    const NAMED: usize = 30;
    let observation = drifting_track();
    let mut values = json!({
        "specmode": "cube", "spw": format!("0:5~{}", 4 + NAMED), "start": 5, "width": 1,
        "nchan": NAMED, "niter": 5, "savemodel": "modelcolumn",
    });
    values
        .as_object_mut()
        .unwrap()
        .extend(controls.as_object().unwrap().clone());
    observation.image(label, values);
    let initial = Complex32::new(1.0, 0.0);
    let rows = super::vis_list::model_rows(observation.measurement_set());
    (0..NAMED + 10)
        .map(|channel| {
            rows.iter()
                .map(|row| {
                    let cells = row.column(channel);
                    let untouched = cells.iter().filter(|cell| **cell == initial).count();
                    assert!(
                        untouched == 0 || untouched == cells.len(),
                        "channel {channel} is part written in a row"
                    );
                    untouched == 0
                })
                .collect()
        })
        .collect()
}

/// The last major cycle writes a time's model only into the channels its
/// band selects: CASA's iterator leaves the others out of the buffer it
/// writes back, so their `MODEL_DATA` keeps its initial value of one. CASA
/// 6.7.6.14 on `test_cube_1`'s cube leaves 336258 cells of the channel past
/// the band's end, of 336960; a run that wrote every time's channels filled
/// them all (#1081). Here a channel the band drifts across is written while
/// the band covers it, so from one end of the track on one edge of the band
/// and to the other end on the other; a channel inside the drift's reach is
/// written at every time, and one beyond it at none.
#[test]
fn a_times_model_column_holds_only_the_channels_its_band_selects() {
    let written = model_written_over_a_drift("drift", json!({}));
    let count = |channel: &Vec<bool>| channel.iter().filter(|row| **row).count();
    let all = written[0].len();
    for channel in &written[5..=30] {
        assert_eq!(count(channel), all);
    }
    for channel in &written[36..] {
        assert_eq!(count(channel), 0);
    }
    // Written rows lie at one end of the track, whole integrations of them.
    let starts_written = |edge: &[Vec<bool>]| {
        edge.iter()
            .map(|channel| {
                assert!((1..all).contains(&count(channel)));
                let changes = channel.windows(2).filter(|pair| pair[0] != pair[1]);
                assert_eq!(changes.count(), 1, "more than one run of written rows");
                channel[0]
            })
            .collect::<Vec<_>>()
    };
    let (low, high) = (
        starts_written(&written[0..=4]),
        starts_written(&written[31..=35]),
    );
    assert!(low.iter().all(|first| *first == low[0]));
    assert!(
        high.iter().all(|first| *first != low[0]),
        "{low:?} {high:?}"
    );
}

/// CASA's final `predictModel` uses the parent imager's untuned bands. On
/// this drifting named range, the interior channels are written at every
/// time and each outer channel is written at one end of the track. CASA
/// 6.7.6.14 gives the same selection for `nsigma` and automatic masking.
#[test]
fn predicting_the_final_model_uses_the_parent_imagers_time_dependent_bands() {
    for (label, controls) in [
        ("nsigma", json!({ "nsigma": 0.01 })),
        ("auto", json!({ "usemask": "auto-multithresh" })),
    ] {
        let written = model_written_over_a_drift(label, controls);
        let all = written[0].len();
        for (channel, rows) in written.iter().enumerate().take(35).skip(5) {
            assert!(
                rows.iter().all(|written| *written),
                "{label}: channel {channel}"
            );
        }
        for (channel, rows) in written
            .iter()
            .enumerate()
            .take(5)
            .chain(written.iter().enumerate().skip(35))
        {
            let count = rows.iter().filter(|written| **written).count();
            assert!(
                (1..all).contains(&count),
                "{label}: channel {channel}, {count} rows"
            );
            assert_eq!(
                rows.windows(2).filter(|pair| pair[0] != pair[1]).count(),
                1,
                "{label}: channel {channel}"
            );
        }
        let counts = written
            .iter()
            .map(|rows| rows.iter().filter(|written| **written).count())
            .collect::<Vec<_>>();
        assert!(
            counts[..5].windows(2).all(|pair| pair[0] < pair[1]),
            "{label}: {counts:?}"
        );
        assert!(
            counts[35..].windows(2).all(|pair| pair[0] > pair[1]),
            "{label}: {counts:?}"
        );
    }
}

/// CASA 6.7.6.14's parent iterator physically selects these channel sets
/// over the drift for `spw=0:20`, rather than flagging a union row.
#[test]
fn parent_prediction_rows_match_casas_drifting_channel_layouts() {
    let observation = drifting_track();
    let request = observation.request(
        "compact-prediction-rows",
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 1,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn",
        }),
    );
    let layouts = casa_imaging_application::source_law::prediction_layouts(
        &request,
        &super::fixture::run_context(&request),
    )
    .expect("the parent prediction traversal");
    let mut actual = layouts
        .iter()
        .map(|layout| layout.channels.clone())
        .collect::<Vec<_>>();
    actual.sort();
    let mut casa = (15..=19)
        .flat_map(|first| {
            [
                (first..first + 6).collect::<Vec<_>>(),
                (first..first + 7).collect::<Vec<_>>(),
            ]
        })
        .chain(std::iter::once((20..26).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    casa.sort();
    assert_eq!(
        actual, casa,
        "CASA's physical parent selections over the drift"
    );
    assert!(
        layouts
            .iter()
            .all(|layout| layout.flags.iter().all(|flag| !flag)),
        "the selected unflagged cells remain unflagged: {layouts:?}"
    );
}

/// With 20 kHz native channels, CASA reads channel 20 alone for most of
/// the track and one adjacent channel at either end. A physically singleton
/// row takes CASA's nearest prediction path even under linear interpolation.
#[test]
fn parent_prediction_rows_include_casas_singleton_channel() {
    let observation = drifting_track_with_width(20_000.0);
    let request = observation.request(
        "compact-singleton-rows",
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn",
        }),
    );
    let layouts = casa_imaging_application::source_law::prediction_layouts(
        &request,
        &super::fixture::run_context(&request),
    )
    .expect("the parent prediction traversal");
    let mut actual = layouts
        .iter()
        .map(|layout| layout.channels.clone())
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(
        actual,
        [vec![19, 20], vec![20], vec![20, 21]],
        "CASA's parent selections"
    );
    assert!(
        layouts
            .iter()
            .all(|layout| layout.flags.iter().all(|flag| !flag))
    );
}

/// CASA grids the physically selected one-channel rows as nearest rather
/// than interpolating a wider, mostly flagged union. On this two-plane
/// 20 kHz track CASA 6.7.6.14 reports these sums of weights.
#[test]
fn tuned_singleton_rows_match_casas_sum_of_weights() {
    let observation = drifting_track_with_width(20_000.0);
    let (_, products) = observation.image(
        "tuned-singleton-sumwt",
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn",
        }),
    );
    let actual = products.get(".sumwt").channel_values();
    assert_eq!(
        actual,
        [101_790.0, 3_510.0],
        "CASA's singleton gridding weights"
    );
}

/// The native `fitspw` extension reads its fit-only channels but keeps
/// the channels named by `spw` as outputs. Fit channels 0 and 39 must not
/// become interpolation neighbors of the selected output channel 20.
#[test]
fn continuum_fit_only_channels_do_not_enter_tuned_gridding_rows() {
    let observation = drifting_track_with_width(20_000.0);
    let request = observation.request(
        "fitted-grid-layout",
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
            "niter": 0, "fitspw": "0:0;39",
        }),
    );
    let layouts = casa_imaging_application::source_law::gridding_layouts(
        &request,
        &super::fixture::run_context(&request),
    )
    .expect("the tuned gridding traversal");
    assert_eq!(
        layouts,
        [vec![20]],
        "fit-only channels cannot enter a gridding row"
    );
}

/// A continuum fit may read a wider union than its output. Both the tuned
/// traversal and the final predictor must physically use the output roles.
#[test]
fn final_prediction_excludes_continuum_fit_only_channels() {
    let observation = drifting_track_with_width(20_000.0);
    let request = observation.request(
        "fitted-prediction-layout",
        json!({
            "specmode": "cube", "spw": "0", "start": 20, "width": 1, "nchan": 2,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn", "fitspw": "0:0;39",
        }),
    );
    let context = super::fixture::run_context(&request);
    let mut tuned = casa_imaging_application::source_law::gridding_layouts(&request, &context)
        .expect("the tuned gridding traversal");
    let mut predicted =
        casa_imaging_application::source_law::prediction_layouts(&request, &context)
            .expect("the final prediction traversal")
            .into_iter()
            .map(|layout| layout.channels)
            .collect::<Vec<_>>();
    tuned.sort();
    predicted.sort();
    assert_eq!(
        predicted, tuned,
        "prediction must not interpolate through fit-only or unrelated channels"
    );
    assert!(
        predicted
            .iter()
            .all(|channels| !channels.contains(&0) && !channels.contains(&39))
    );
}

/// A corrected-only final major cycle uses the same tuned rows as a run
/// without a visibility write, even when nsigma is enabled.
#[test]
fn corrected_only_nsigma_uses_the_tuned_continuum_rows() {
    use casa_ms::MeasurementSet;
    use casa_tables::ColumnSchema;
    use casa_types::{ArrayValue, PrimitiveType, Value};

    let observation = drifting_track_with_width(20_000.0);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let Some(Value::Array(ArrayValue::Complex32(data))) = ms
        .main_table()
        .column_accessor("DATA")
        .unwrap()
        .get(0)
        .unwrap()
        .cloned()
    else {
        panic!("DATA is a complex array");
    };
    let shape = data.shape().to_vec();
    ms.main_table_mut()
        .add_column(
            ColumnSchema::array_fixed("CORRECTED_DATA", PrimitiveType::Complex32, shape.clone()),
            Some(Value::Array(ArrayValue::Complex32(ndarray::ArrayD::zeros(
                shape,
            )))),
        )
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let controls = json!({
        "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
        "niter": 0, "nsigma": 0.01, "fitspw": "0:0;39",
    });
    let ordinary = observation
        .image("continuum-without-write", controls.clone())
        .1;
    let mut corrected_controls = controls;
    corrected_controls["save_continuum_residual"] = json!(true);
    let request = observation.request("corrected-only-source", corrected_controls.clone());
    assert!(
        !casa_imaging_application::source_law::ordinary_write_uses_parent_selection(
            &request,
            &super::fixture::run_context(&request),
        )
        .expect("corrected-only writer"),
        "a corrected-only writer must request tuned rows",
    );
    let corrected = observation
        .image("continuum-corrected-only", corrected_controls)
        .1;
    assert_eq!(
        corrected.get(".sumwt").channel_values(),
        ordinary.get(".sumwt").channel_values(),
        "the visibility writer must not select the parent prediction source",
    );
    assert_eq!(
        corrected.get(".residual").values(),
        ordinary.get(".residual").values()
    );
}

/// `predictModel` visits the whole parent selection and writes zero where
/// the cube has no spectral support, rather than leaving the initial model.
#[test]
fn predicting_the_final_model_zeroes_channels_beyond_the_cube() {
    let observation = drifting_track();
    observation.image(
        "predict-outside",
        json!({
            "specmode": "cube", "spw": "0", "start": 10, "width": 1, "nchan": 10,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn",
        }),
    );
    let rows = super::vis_list::model_rows(observation.measurement_set());
    assert!(!rows.is_empty());
    for row in &rows {
        for channel in [0, 39] {
            assert!(
                row.column(channel)
                    .iter()
                    .all(|value| *value == Complex32::new(0.0, 0.0)),
                "channel {channel}"
            );
        }
    }
    // CASA 6.7.6.14 extrapolates channel 21 on 10,179 of the 52,650 rows.
    let extrapolated = rows
        .iter()
        .filter(|row| {
            row.column(21)
                .iter()
                .any(|value| *value != Complex32::new(0.0, 0.0))
        })
        .count();
    assert!(
        (5_000..15_000).contains(&extrapolated),
        "channel 21: {extrapolated} extrapolated rows"
    );
}

/// Two adjacent Q-band windows of `channels` 128 MHz channels each, the
/// second starting where the first ends, and the first window's setup.
fn adjacent_windows(channels: usize) -> (Observation, casa_ms::SyntheticSpectralSetup) {
    let mut setup = super::fixture::Setup::vla_q_band(channels);
    let low = setup.spectral_windows[0].clone();
    setup
        .spectral_windows
        .push(casa_ms::SyntheticSpectralSetup {
            name: "t1-qband-next".to_string(),
            start_frequency_hz: low.start_frequency_hz + channels as f64 * low.channel_width_hz,
            ..low.clone()
        });
    (
        Observation::synthesise_setup(setup, &[("point", POINT)]),
        low,
    )
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
