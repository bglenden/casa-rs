// SPDX-License-Identifier: LGPL-3.0-or-later

//! A list of MeasurementSets images as one observation (#980), as CASA's
//! tclean images its `vis` list: one track's band split by channel into two
//! MeasurementSets images as the whole band does in one, sample for sample,
//! weighting density included, whichever order the list names them in; a
//! cube on the first one's channels reads the second where it reaches the
//! cube; and the final model is written into each one's `MODEL_DATA`.

use std::path::Path;

use casa_ms::{MeasurementSet, SyntheticSpectralSetup, VisibilityDataColumn};
use casa_types::{ArrayValue, Complex32};
use ndarray::Array2;
use serde_json::json;

use super::fixture::{Component, Observation, Products, Setup};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: -0.7,
};

const CHANNEL_WIDTH_HZ: f64 = 128.0e6;

/// The track of the T1 fixture without noise, in one window of `channels`
/// channels from `first` 128 MHz above 44 GHz: every MeasurementSet holds
/// the same samples of the channels it holds, bit for bit.
fn track(first: usize, channels: usize) -> Observation {
    let mut setup = Setup::vla_q_band(channels);
    setup.noise_jy = 0.0;
    setup.spectral_windows = vec![SyntheticSpectralSetup {
        name: "t1-qband".to_string(),
        start_frequency_hz: 44.0e9 + first as f64 * CHANNEL_WIDTH_HZ,
        channel_width_hz: CHANNEL_WIDTH_HZ,
        channel_count: channels,
    }];
    Observation::synthesise_setup(setup, &[("point", POINT)])
}

/// The band's two halves and the whole band.
struct Split {
    low: Observation,
    high: Observation,
    whole: Observation,
}

impl Split {
    fn new() -> Self {
        Self {
            low: track(0, 2),
            high: track(2, 2),
            whole: track(0, 4),
        }
    }

    fn halves(&self) -> [&Path; 2] {
        [self.low.measurement_set(), self.high.measurement_set()]
    }
}

/// The largest difference between every plane of `suffix` in `actual` and
/// `expected`, relative to `expected`'s largest magnitude.
fn relative_difference(actual: &Products, expected: &Products, suffix: &str) -> f64 {
    let (actual, expected) = (actual.get(suffix), expected.get(suffix));
    assert_eq!(actual.image.shape(), expected.image.shape(), "{suffix}");
    let (actual, expected) = (actual.values(), expected.values());
    let scale = expected
        .iter()
        .fold(0.0_f64, |most, value| most.max(f64::from(value.abs())));
    let largest = actual
        .iter()
        .zip(expected.iter())
        .fold(0.0_f64, |most, (a, e)| most.max(f64::from((a - e).abs())));
    largest / scale.max(f64::MIN_POSITIVE)
}

/// Assert that `actual`'s products match `expected`'s to rounding.
fn assert_products(actual: &Products, expected: &Products, suffixes: &[&str], label: &str) {
    assert_eq!(actual.suffixes(), expected.suffixes(), "{label}");
    for suffix in suffixes {
        let difference = relative_difference(actual, expected, suffix);
        eprintln!("T1 vis list {label} {suffix}: relative difference {difference:.3e}");
        assert!(difference < 1.0e-5, "{label} {suffix}: {difference}");
    }
}

/// Briggs weighting grids one density over every MeasurementSet's samples,
/// as CASA's `VisImagingWeight` iterates over all of them, so the halves of
/// a band image exactly as the whole band: the same samples gridded, the
/// same sums of weights, the same PSF, residual and model; and so does the
/// list in the other order, which images the same samples.
#[test]
fn two_measurement_sets_image_as_their_concatenation() {
    let split = Split::new();
    let controls = json!({
        "weighting": "briggs",
        "robust": 0.5,
        "deconvolver": "hogbom",
        "niter": 30,
    });
    let (whole_run, whole) = split.whole.image("whole", controls.clone());
    let [low, high] = split.halves();
    for (label, stores) in [("in order", [low, high]), ("reversed", [high, low])] {
        let (run, products) = split.whole.image_stores(&stores, label, controls.clone());
        assert_eq!(run.gridded_samples, whole_run.gridded_samples, "{label}");
        assert_eq!(run.minor_iterations, whole_run.minor_iterations, "{label}");
        assert_products(
            &products,
            &whole,
            &[".sumwt", ".psf", ".residual", ".model", ".image"],
            label,
        );
        // One continuum plane over the whole band, at its centre.
        let frequency = products.get(".image").channel_frequency_hz(0);
        assert!(
            (frequency - whole.get(".image").channel_frequency_hz(0)).abs() < 1.0,
            "{label}: {frequency} Hz"
        );
    }
}

/// A cube is laid on the channels of the MeasurementSet holding its start
/// (CASA's `buildCoordinateSystem`, CAS-12877), whichever place the list
/// gives it, and reads the other where its channels reach the cube: the
/// halves' cube is the whole band's.
#[test]
fn a_cube_over_two_measurement_sets_reads_each_where_it_reaches_the_cube() {
    let split = Split::new();
    let controls = json!({
        "specmode": "cube",
        "start": "44.128GHz",
        "nchan": 3,
        "interpolation": "nearest",
        "deconvolver": "hogbom",
        "niter": 20,
    });
    let (whole_run, whole) = split.whole.image("whole-cube", controls.clone());
    let [low, high] = split.halves();
    for (label, stores) in [
        ("cube in order", [low, high]),
        ("cube reversed", [high, low]),
    ] {
        let (run, products) = split.whole.image_stores(&stores, label, controls.clone());
        assert_eq!(run.gridded_samples, whole_run.gridded_samples, "{label}");
        assert_eq!(products.get(".image").channels(), 3, "{label}");
        for channel in 0..3 {
            let (actual, expected) = (
                products.get(".image").channel_frequency_hz(channel),
                whole.get(".image").channel_frequency_hz(channel),
            );
            assert!((actual - expected).abs() < 1.0, "{label} {channel}");
        }
        assert_products(&products, &whole, &[".sumwt", ".psf", ".residual"], label);
    }
}

/// `MODEL_DATA` of each MeasurementSet of the list (CASA's
/// `predictModel` over every one): each half's is the whole band's at its
/// channels.
#[test]
fn the_model_is_written_into_each_measurement_set() {
    let split = Split::new();
    let controls = json!({
        "deconvolver": "hogbom",
        "niter": 30,
        "savemodel": "modelcolumn",
    });
    let (whole_run, _) = split.whole.image("whole-model", controls.clone());
    let (run, _) = split
        .whole
        .image_stores(&split.halves(), "halves-model", controls);
    let predicted = |run: &casars_imager::ImagerRunReport| {
        run.visibility_products
            .as_ref()
            .expect("a run that writes MODEL_DATA reports it")
            .sample_count
    };
    assert_eq!(predicted(&run), predicted(&whole_run));
    let whole = model_rows(split.whole.measurement_set());
    let low = model_rows(split.low.measurement_set());
    let high = model_rows(split.high.measurement_set());
    assert_eq!(whole.len(), low.len());
    assert_eq!(whole.len(), high.len());
    let mut largest = 0.0_f32;
    let mut scale = 0.0_f32;
    for ((whole, low), high) in whole.iter().zip(&low).zip(&high) {
        for correlation in 0..whole.nrows() {
            for channel in 0..4 {
                let half = if channel < 2 {
                    low[[correlation, channel]]
                } else {
                    high[[correlation, channel - 2]]
                };
                let expected = whole[[correlation, channel]];
                largest = largest.max((half - expected).norm());
                scale = scale.max(expected.norm());
            }
        }
    }
    eprintln!("T1 vis list MODEL_DATA: largest difference {largest:.3e} of {scale:.3e}");
    assert!(scale > 0.5, "the model predicts the point source: {scale}");
    assert!(largest < 1.0e-5 * scale, "{largest} of {scale}");
}

/// Every MAIN row's `MODEL_DATA`, `[correlation, channel]`.
fn model_rows(path: &Path) -> Vec<Array2<Complex32>> {
    let ms = MeasurementSet::open(path).expect("reopen the MeasurementSet");
    let model = ms
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA was written");
    (0..ms.row_count())
        .map(|row| {
            let ArrayValue::Complex32(values) = model.get(row).expect("MODEL_DATA row") else {
                panic!("MODEL_DATA is complex");
            };
            values
                .clone()
                .into_dimensionality()
                .expect("a [correlation, channel] cell")
        })
        .collect()
}

/// The imager refuses a selector list whose length is neither one nor the
/// list's, with CASA's message.
#[test]
fn a_selector_list_must_match_the_measurement_sets() {
    let split = Split::new();
    let Err(refusal) = split.whole.try_image_stores(
        &split.halves(),
        "mismatched",
        json!({ "spw": ["0", "0", "0"] }),
    ) else {
        panic!("three spw selectors for two MeasurementSets were accepted");
    };
    assert!(
        refusal.contains("spw must have a single entry, or 2 entries to match vis list"),
        "{refusal}"
    );
    // One selector, or a list of one, is every MeasurementSet's.
    for spw in [json!("0"), json!(["0"]), json!(["0", "0:0~1"])] {
        let (run, _) =
            split
                .whole
                .image_stores(&split.halves(), "selected", json!({ "spw": spw.clone() }));
        assert!(run.gridded_samples > 0, "{spw}");
    }
}
