// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's weighting modes beyond natural, uniform and Briggs (#980):
//! `superuniform`, `radial`, `briggsabs`, and `npixels`'s density grids,
//! end to end through the production route, dirty. Each reshapes the PSF
//! as its formula says (`SynthesisImagerVi2::weight`, `VisImagingWeight`,
//! `BriggsCubeWeightor`). Two laws are exact: `briggsabs` at robust 0 is
//! natural weighting scaled by `1/(2·noise²)` and at noise 0 uniform
//! weighting scaled by `1/robust²`; and uniform weights sum to the number
//! of occupied density cells, so `npixels = n` caps the sum at `n²`.
//! Agreement with CASA's own images is the T1.5 rows'.

use ndarray::Array2;
use serde_json::{Value, json};

use super::fixture::{Component, IMAGE_SIZE, Observation, Products, fit_psf_main_lobe_cells};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// The dirty products of `controls` with `weighting` (`npixels`, `noise`).
fn dirty(observation: &Observation, name: &str, controls: Value, weighting: Value) -> Products {
    let mut parameters = json!({ "niter": 0 });
    let fields = parameters.as_object_mut().expect("parameters");
    fields.extend(controls.as_object().expect("controls").clone());
    fields.extend(weighting.as_object().expect("weighting").clone());
    observation.image(name, parameters).1
}

/// The PSF's main-lobe area in cells², `major · minor` of its fitted FWHM.
fn lobe(psf: &Array2<f32>) -> f64 {
    let [major, minor] = fit_psf_main_lobe_cells(psf, 0.35);
    major * minor
}

/// The largest absolute difference between two planes.
fn largest_difference(left: &Array2<f32>, right: &Array2<f32>) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| (left - right).abs())
        .fold(0.0, f32::max)
}

fn sumwt(products: &Products) -> Vec<f64> {
    products
        .get(".sumwt")
        .channel_values()
        .into_iter()
        .map(f64::from)
        .collect()
}

/// Measured on this fixture (main-lobe area in cells², sumwt): natural
/// 30.3 and 421,200; uniform 15.3 and 3,364; radial 16.8; superuniform
/// 12.4 (npixels 3) and 11.9 (npixels 8); uniform over 16 × 16 cells 9.5
/// and 44.
#[test]
fn continuum_weightings_reshape_the_psf_as_their_formulae_say() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let none = json!({});
    let image = |name: &str, weighting: Value, resolved: Value| {
        dirty(&observation, name, weighting, resolved)
    };
    let psf = |products: &Products| products.get(".psf").plane(0);
    let natural = image("natural", json!({ "weighting": "natural" }), none.clone());
    let uniform = image("uniform", json!({ "weighting": "uniform" }), none.clone());
    let [natural_sum, uniform_sum] = [&natural, &uniform].map(|products| sumwt(products)[0]);

    // briggsabs, w/(robust²·D + 2·noise²): at robust 0 natural weighting
    // halved by 2·(1 Jy)², at noise 0 uniform weighting quartered by 2².
    let flat = image(
        "abs-robust0",
        json!({ "weighting": "briggsabs", "robust": 0.0 }),
        json!({ "noise": "1Jy" }),
    );
    assert!(largest_difference(&psf(&flat), &psf(&natural)) < 1e-5);
    assert!((sumwt(&flat)[0] / natural_sum - 0.5).abs() < 1e-6);
    let steep = image(
        "abs-noise0",
        json!({ "weighting": "briggsabs", "robust": 2.0 }),
        json!({ "noise": "0Jy" }),
    );
    assert!(largest_difference(&psf(&steep), &psf(&uniform)) < 1e-5);
    assert!((sumwt(&steep)[0] / uniform_sum - 0.25).abs() < 1e-6);
    // Between, the noise trades the two as Briggs's robust does: a cell of
    // the mean density, about 250 unit weights, balances 2·(10 Jy)².
    let between = image(
        "abs-between",
        json!({ "weighting": "briggsabs", "robust": 1.0 }),
        json!({ "noise": "10Jy" }),
    );
    let [natural_lobe, uniform_lobe] = [&natural, &uniform].map(|products| lobe(&psf(products)));
    let between_lobe = lobe(&psf(&between));
    eprintln!(
        "T1 weighting: briggsabs lobe {between_lobe:.2} cells² between {uniform_lobe:.2} and {natural_lobe:.2}"
    );
    assert!(uniform_lobe < between_lobe && between_lobe < natural_lobe);

    // Radial weights grow with uv distance, favouring long baselines.
    let radial = image("radial", json!({ "weighting": "radial" }), none.clone());
    assert!(lobe(&psf(&radial)) < 0.6 * natural_lobe);

    // Superuniform sums the density over a box of ±npixels cells, 3 when
    // npixels is 0: the wider the box, the more the sparse long baselines
    // weigh against the dense short ones.
    let superuniform = image(
        "superuniform",
        json!({ "weighting": "superuniform" }),
        none.clone(),
    );
    let three = image(
        "superuniform-3",
        json!({ "weighting": "superuniform" }),
        json!({ "npixels": 3 }),
    );
    assert_eq!(psf(&three), psf(&superuniform), "CASA's default box is ±3");
    let eight = image(
        "superuniform-8",
        json!({ "weighting": "superuniform" }),
        json!({ "npixels": 8 }),
    );
    let [box3_lobe, box8_lobe] = [&superuniform, &eight].map(|products| lobe(&psf(products)));
    assert!(box8_lobe < box3_lobe && box3_lobe < uniform_lobe);

    // Uniform weights over a cell's samples sum to one per occupied cell
    // and its conjugate, whole: npixels = n puts them on n × n cells across
    // the image's uv extent, and npixels = imsize is the image's own grid.
    let own = image(
        "uniform-256",
        json!({ "weighting": "uniform" }),
        json!({ "npixels": IMAGE_SIZE }),
    );
    assert_eq!(
        psf(&own),
        psf(&uniform),
        "npixels = imsize is the image's grid"
    );
    assert_eq!(uniform_sum.round(), uniform_sum, "occupied image cells");
    for cells in [16, 4] {
        let coarse = image(
            &format!("uniform-{cells}"),
            json!({ "weighting": "uniform" }),
            json!({ "npixels": cells }),
        );
        let occupied = sumwt(&coarse)[0];
        eprintln!("T1 weighting: uniform over {cells}² cells occupies {occupied}");
        assert!(
            occupied.round() == occupied && occupied <= (cells * cells) as f64,
            "{cells} cells: sumwt {occupied}"
        );
        if cells == 16 {
            // Uniform over larger cells suppresses sidelobes over a smaller
            // field, weighing the sparse outer baselines up further.
            assert!(lobe(&psf(&coarse)) < uniform_lobe);
        }
    }
}

const CUBE_CHANNELS: usize = 8;

/// The dirty cube of `weighting` with per-channel density
/// (`perchanweightdensity`), `resolved` set after the catalog.
fn cube(observation: &Observation, name: &str, weighting: Value, resolved: Value) -> Products {
    let mut controls = json!({
        "specmode": "cube",
        "channel_count": CUBE_CHANNELS,
        "perchanweightdensity": true,
    });
    controls
        .as_object_mut()
        .expect("controls")
        .extend(weighting.as_object().expect("weighting").clone());
    dirty(observation, name, controls, resolved)
}

/// Per-channel density (`BriggsCubeWeightor`) with `npixels` spreads each
/// sample over a box; superuniform and radial weights are not per-channel
/// densities (`VisImagingWeight`). Measured in the middle channel
/// (main-lobe area in cells²): natural 29.9, uniform 15.6, uniform with a
/// ±1 box 14.5, superuniform 12.2, radial 16.6.
#[test]
fn cube_weightings_reshape_every_channels_psf() {
    let observation = Observation::synthesise_band(CUBE_CHANNELS, &[("point", POINT)]);
    let none = json!({});
    let image = |name: &str, weighting: Value, resolved: Value| {
        cube(&observation, name, weighting, resolved)
    };
    let natural = image("natural", json!({ "weighting": "natural" }), none.clone());
    let uniform = image("uniform", json!({ "weighting": "uniform" }), none.clone());
    let boxed = image(
        "uniform-box",
        json!({ "weighting": "uniform" }),
        json!({ "npixels": 1 }),
    );
    let flat = image(
        "abs-robust0",
        json!({ "weighting": "briggsabs", "robust": 0.0 }),
        json!({ "noise": "1Jy" }),
    );
    let radial = image("radial", json!({ "weighting": "radial" }), none.clone());
    let superuniform = image(
        "superuniform",
        json!({ "weighting": "superuniform" }),
        none.clone(),
    );
    let natural_sumwt = sumwt(&natural);
    let radial_sumwt = sumwt(&radial);
    let spectral = radial.get(".psf");
    for channel in 0..CUBE_CHANNELS {
        let psf = |products: &Products| products.get(".psf").plane(channel);
        let natural_lobe = lobe(&psf(&natural));
        let uniform_lobe = lobe(&psf(&uniform));
        // The cube weightor's absolute Briggs at robust 0 is natural
        // weighting halved.
        assert!(
            largest_difference(&psf(&flat), &psf(&natural)) < 1e-5,
            "{channel}"
        );
        assert!((sumwt(&flat)[channel] / natural_sumwt[channel] - 0.5).abs() < 1e-6);
        assert!(lobe(&psf(&boxed)) < uniform_lobe, "{channel}");
        assert!(lobe(&psf(&superuniform)) < uniform_lobe, "{channel}");
        assert!(lobe(&psf(&radial)) < 0.6 * natural_lobe, "{channel}");
        // Radial weights scale with frequency: f·|uv|.
        let ratio = radial_sumwt[channel] / radial_sumwt[0];
        let frequencies = spectral.channel_frequency_hz(channel) / spectral.channel_frequency_hz(0);
        assert!(
            (ratio - frequencies).abs() < 1e-3 * frequencies,
            "{channel}: sumwt ratio {ratio}, frequency ratio {frequencies}"
        );
    }
    assert_ne!(
        sumwt(&boxed),
        sumwt(&uniform),
        "the box changes the density"
    );
}
