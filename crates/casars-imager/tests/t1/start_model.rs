// SPDX-License-Identifier: LGPL-3.0-or-later

//! `startmodel` (#841 Phase 6b): CASA's start model is the model's first
//! generation, so the first major cycle subtracts its prediction and the
//! restored image includes it. A run started from another run's final model
//! with no minor cycle is that run's last major cycle again: the same model,
//! residual and restored image (CASA 6.7.6 on `refim_point` gives the deep
//! clean's residual exactly). Its initial pass also forms the PSF, the
//! weight image and the sums as a run without a model does, so those are an
//! unseeded run's. The law holds for every structure the initial pass with a
//! model reaches (#970 review, finding 3): MFS, a cube interpolated onto
//! its output channels, a mosaic with its weight image, MFS and cube, whose
//! flat-noise start model is made physical with the weights first (CASA
//! 6.7.6 reproduces a 14-pointing mosaic's deep clean the same way), and
//! facets; and a start model's `MODEL_DATA` is the deep run's. Agreement
//! with CASA's own runs is the T1.5 rows'.

use serde_json::{Value, json};

use super::fixture::{Component, Observation, Products};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const GAUSSIAN: Component = Component {
    offset_px: [-30, -26],
    flux_jy: 0.5,
    fwhm_px: Some(8.0),
    spectral_index: 0.0,
};

/// The largest difference of every plane of `product` between two runs,
/// relative to the first's largest magnitude.
fn difference(left: &Products, right: &Products, product: &str) -> f64 {
    let left = left.get(product).values();
    let right = right.get(product).values();
    assert_eq!(left.shape(), right.shape(), "{product}");
    let peak = left.iter().fold(0.0_f64, |m, v| m.max(f64::from(v.abs())));
    let worst = left
        .iter()
        .zip(&right)
        .fold(0.0_f64, |m, (a, b)| m.max(f64::from((a - b).abs())));
    worst / peak
}

/// Image `observation` three ways under `controls`: `deep` as given, then
/// with no minor cycle `started` from `deep`'s model and `dirty` without
/// one. `started` is `deep`'s last major cycle again; its PSF, weight image
/// and sums are `dirty`'s. The three runs' products.
fn assert_first_generation(
    observation: &Observation,
    label: &str,
    controls: &Value,
) -> [Products; 3] {
    let (_, deep) = observation.image(&format!("{label}-deep"), controls.clone());
    let start = observation.scratch(&format!("{label}-deep.model"));
    let without_cycles = |extra: Value| {
        let mut controls = controls.clone();
        controls["niter"] = json!(0);
        controls
            .as_object_mut()
            .expect("controls")
            .extend(extra.as_object().expect("extra").clone());
        controls
    };
    let (_, started) = observation.image(
        &format!("{label}-started"),
        without_cycles(json!({ "startmodel": start })),
    );
    let (_, dirty) = observation.image(&format!("{label}-dirty"), without_cycles(json!({})));
    for product in [".model", ".residual", ".image", ".psf"] {
        let worst = difference(&deep, &started, product);
        eprintln!("T1 startmodel {label}: {product} differs by {worst:.2e} of its peak");
        assert!(worst < 1.0e-5, "{label} {product}: {worst:e}");
    }
    for product in [".psf", ".weight", ".pb"] {
        if dirty.suffixes().contains(product) {
            let worst = difference(&dirty, &started, product);
            assert!(
                worst < 1.0e-6,
                "{label} {product} of the seeded pass: {worst:e}"
            );
        }
    }
    assert_eq!(
        dirty.get(".sumwt").values(),
        started.get(".sumwt").values(),
        "{label}: the seeded initial pass sums the weights a run without a model does"
    );
    [deep, started, dirty]
}

fn residual_rms(products: &Products) -> f64 {
    let residual = products.get(".residual").values();
    (residual.iter().map(|v| f64::from(*v).powi(2)).sum::<f64>() / residual.len() as f64).sqrt()
}

#[test]
fn a_start_model_is_the_first_generation_of_the_model() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": "0.005Jy",
    });
    let [_, started, _] = assert_first_generation(&observation, "mfs", &controls);
    // Cleaning on from the start model goes deeper than it.
    let (_, cleaned) = observation.image(
        "mfs-cleaned-on",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 100,
            "threshold": "0.001Jy",
            "startmodel": observation.scratch("mfs-deep.model"),
        }),
    );
    assert!(
        residual_rms(&cleaned) < residual_rms(&started),
        "cleaning on: residual rms {} against {}",
        residual_rms(&cleaned),
        residual_rms(&started)
    );
}

/// A sixteen-channel cube interpolated linearly onto its LSRK channels, of
/// a source whose flux falls by 9% across the band: a start model that
/// differs in every channel.
#[test]
fn a_cube_start_model_is_the_first_generation_of_each_channels_model() {
    let steep = Component {
        spectral_index: -2.0,
        ..POINT
    };
    let observation = Observation::synthesise_band(16, &[("point", steep)]);
    let controls = json!({
        "specmode": "cube",
        "channel_count": 16,
        "perchanweightdensity": true,
        "weighting": "briggs",
        "robust": 0.5,
        "deconvolver": "hogbom",
        "niter": 2000,
        "threshold": "0.01Jy",
    });
    let [deep, ..] = assert_first_generation(&observation, "cube", &controls);
    let model = deep.get(".model");
    let flux = |channel: usize| {
        model
            .plane(channel)
            .iter()
            .map(|v| f64::from(*v))
            .sum::<f64>()
    };
    assert!(
        flux(1) > flux(14) * 1.05,
        "the start model differs by channel: {} against {}",
        flux(1),
        flux(14)
    );
}

/// The T1 mosaic: three pointings, the summed-beam weight image and the
/// primary-beam-corrected image of a model that starts the run. Its
/// `.model` is in CASA's flat-noise units, the physical model times the
/// PB: the start model is made physical with the weights of an initial
/// pass without it, as CASA divides by them after `makePSF`, and the run
/// is the deep one's last major cycle again.
#[test]
fn a_mosaic_start_model_is_the_first_generation_of_the_model() {
    let (observation, controls) = super::mosaic::mosaic_run(false);
    let [_, started, _] = assert_first_generation(&observation, "mosaic", &controls);
    assert!(
        started.suffixes().contains(".weight"),
        "the mosaic's weight image"
    );
}

/// The T1 mosaic cube, at 50 and 75 GHz: each channel's start model is
/// made physical with its own channel's weights.
#[test]
fn a_mosaic_cube_start_model_is_the_first_generation_of_each_channels_model() {
    let (observation, controls) = super::mosaic::mosaic_run(true);
    assert_first_generation(&observation, "mosaic-cube", &controls);
}

/// Two by two facets, each gridded about its own centre, seeded from the
/// faceted run's model.
#[test]
fn a_faceted_start_model_is_the_first_generation_of_the_model() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": "0.005Jy",
        "facets": 2,
    });
    assert_first_generation(&observation, "facets", &controls);
}

/// `savemodel` with no minor cycle predicts the start model into
/// `MODEL_DATA`: the values the deep run wrote of the same model.
#[test]
fn a_start_models_model_data_is_the_deep_runs() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    observation.image(
        "column-deep",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold": "0.005Jy",
            "savemodel": "modelcolumn",
        }),
    );
    let deep: Vec<_> = observation
        .data_and_model()
        .into_iter()
        .map(|(_, model)| model)
        .collect();
    assert!(
        deep.iter().any(|model| model.norm() > 0.1),
        "a model to predict"
    );
    observation.image(
        "column-started",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 0,
            "savemodel": "modelcolumn",
            "startmodel": observation.scratch("column-deep.model"),
        }),
    );
    let started: Vec<_> = observation
        .data_and_model()
        .into_iter()
        .map(|(_, model)| model)
        .collect();
    let peak = deep.iter().fold(0.0_f32, |m, v| m.max(v.norm()));
    let worst = deep
        .iter()
        .zip(&started)
        .fold(0.0_f32, |m, (a, b)| m.max((a - b).norm()));
    eprintln!(
        "T1 startmodel MODEL_DATA differs by {:.2e} of its peak",
        worst / peak
    );
    assert!(worst <= 1.0e-5 * peak, "MODEL_DATA: {worst} of {peak}");
}

/// `controls` extended by `extra`.
fn with(controls: &Value, extra: Value) -> Value {
    let mut controls = controls.clone();
    controls
        .as_object_mut()
        .expect("controls")
        .extend(extra.as_object().expect("extra").clone());
    controls
}

/// An MT-MFS start model is one image per Taylor term (#1067,
/// `SIImageStoreMultiTerm::setModelImage`): started from a deep run's
/// `.model.tt0` and `.model.tt1` with no minor cycle, the run is the deep
/// run's last major cycle again, every term's residual included; an empty
/// entry leaves its term zero (CASA's `test_startmodel_mtmfs_restart`
/// passes `['', 'tst1.model.tt1']`).
#[test]
fn an_mtmfs_start_model_is_one_image_per_taylor_term() {
    let steep = Component {
        spectral_index: -1.0,
        ..POINT
    };
    let observation = Observation::synthesise_band(8, &[("point", steep)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "mtmfs",
        "nterms": 2,
        "niter": 500,
        "threshold": "0.005Jy",
    });
    let (_, deep) = observation.image("mtmfs-deep", controls.clone());
    let terms = [
        observation.scratch("mtmfs-deep.model.tt0"),
        observation.scratch("mtmfs-deep.model.tt1"),
    ];
    let (_, started) = observation.image(
        "mtmfs-started",
        with(&controls, json!({ "niter": 0, "startmodel": terms })),
    );
    for product in [".model.tt0", ".model.tt1", ".residual.tt0", ".residual.tt1"] {
        let worst = difference(&deep, &started, product);
        eprintln!("T1 mtmfs startmodel: {product} differs by {worst:.2e} of its peak");
        assert!(worst < 1.0e-5, "{product}: {worst:e}");
    }
    let (_, second) = observation.image(
        "mtmfs-second-term",
        with(
            &controls,
            json!({ "niter": 0, "startmodel": ["", terms[1]] }),
        ),
    );
    assert!(
        second
            .get(".model.tt0")
            .values()
            .iter()
            .all(|value| *value == 0.0)
    );
    assert!(difference(&deep, &second, ".model.tt1") < 1.0e-5);
}

/// A start model on another spectral grid is regridded along frequency
/// (#1067, `SIImageStore::regridToModelImage`'s `ImageRegrid` over the
/// direction and spectral axes, linear): an eight-channel model of
/// channels 4 to 11 seeds a sixteen-channel cube with those channels'
/// planes, and the channels beyond it with its end planes, as casacore's
/// linear interpolation writes them (CASA's `test_modelvis_7` regrids such
/// a model onto its whole window).
#[test]
fn a_start_model_on_another_spectral_grid_is_regridded_along_frequency() {
    let steep = Component {
        spectral_index: -2.0,
        ..POINT
    };
    let observation = Observation::synthesise_band(16, &[("point", steep)]);
    let controls = json!({
        "specmode": "cube",
        "nchan": -1,
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 300,
        "threshold": "0.01Jy",
    });
    let (_, part) = observation.image("regrid-part", with(&controls, json!({ "spw": "0:4~11" })));
    let (_, whole) = observation.image(
        "regrid-whole",
        with(
            &controls,
            json!({ "niter": 0, "startmodel": observation.scratch("regrid-part.model") }),
        ),
    );
    let (part, whole) = (part.get(".model"), whole.get(".model"));
    assert_eq!((part.channels(), whole.channels()), (8, 16));
    for channel in 0..16 {
        let source = part.plane(channel.clamp(4, 11) - 4);
        let seeded = whole.plane(channel);
        let peak = source.iter().fold(0.0_f32, |m, v| m.max(v.abs()));
        let worst = source
            .iter()
            .zip(&seeded)
            .fold(0.0_f32, |m, (a, b)| m.max((a - b).abs()));
        assert!(
            worst <= 1.0e-5 * peak,
            "channel {channel}: {worst} of the model plane's peak {peak}"
        );
    }
}

/// A plane no sample reaches restores its start model: CASA's
/// `SIImageStore::restore` convolves every plane's model with its beam and
/// adds the residual, whatever the plane's weights, and `makeImageBeamSet`
/// gives the plane the largest beam of the domain. Its residual is zero, so
/// its restored image is the model's point convolved with that beam: a peak
/// of the model's flux, and the beam's area in the ratio of its sum to its
/// peak.
#[test]
fn a_plane_without_weights_restores_its_start_model() {
    let observation = Observation::synthesise_band(16, &[("point", POINT)]);
    let controls = json!({
        "specmode": "cube",
        "start": 0,
        "nchan": 16,
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 300,
        "threshold": "0.01Jy",
    });
    let (_, deep) = observation.image("unweighted-deep", controls.clone());
    let (_, started) = observation.image(
        "unweighted-started",
        with(
            &controls,
            json!({
                "niter": 0,
                "spw": "0:2~15",
                "startmodel": observation.scratch("unweighted-deep.model"),
            }),
        ),
    );
    assert_eq!(started.get(".sumwt").channel_values()[0], 0.0);
    assert!(started.get(".residual").plane(0).iter().all(|v| *v == 0.0));
    let flux: f32 = deep.get(".model").plane(0).iter().sum();
    let restored = started.get(".image").plane(0);
    let peak = restored.iter().fold(0.0_f32, |m, v| m.max(*v));
    assert!(
        flux > 0.5 && (peak - flux).abs() < 0.02 * flux,
        "the unweighted plane restores a peak of {peak} for a model of {flux} Jy"
    );
    // The fitted beams of the channels with data, read off the run's PSF.
    let image = started.get(".image");
    let largest = (2..16)
        .map(|channel| image.channel_beam(channel))
        .map(|beam| beam.major_in("arcsec").unwrap() * beam.minor_in("arcsec").unwrap())
        .fold(0.0_f64, f64::max);
    let smallest = (2..16)
        .map(|channel| image.channel_beam(channel))
        .map(|beam| beam.major_in("arcsec").unwrap() * beam.minor_in("arcsec").unwrap())
        .fold(f64::INFINITY, f64::min);
    assert!(
        largest > 1.05 * smallest,
        "the channels' beams must differ for this law: {smallest} and {largest} arcsec²"
    );
    let cell_arcsec = observation.geometry().cell_arcsec[0];
    let gaussian = std::f64::consts::PI / (4.0 * std::f64::consts::LN_2);
    let expected = gaussian * largest / (cell_arcsec * cell_arcsec);
    let sum: f64 = restored.iter().map(|value| f64::from(*value)).sum();
    let area = sum / f64::from(peak);
    assert!(
        (area - expected).abs() < 0.02 * expected,
        "the unweighted plane's beam covers {area} pixels, the largest beam {expected}"
    );
}
