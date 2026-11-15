// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard-gridder MFS with point, multiscale and maximum-entropy solvers.

use casars_imager::ImagerRunReport;
use serde_json::{Value, json};

use super::fixture::{
    CELL_ARCSEC, Component, Observation, Product, ProductContract, Products, SpectralAxis, box_sum,
    component_pixel, fit_psf_main_lobe, off_source_rms, peak,
};

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
/// Five times the 1.0 mJy thermal image noise.
const THRESHOLD: &str = "0.005Jy";

fn controls(deconvolver: &str) -> Value {
    json!({
        "weighting": "natural",
        "deconvolver": deconvolver,
        "niter": 500,
        "threshold": THRESHOLD,
    })
}

/// Image the point-and-Gaussian sky and check the laws every deconvolver
/// must meet; returns the run for algorithm-specific checks.
pub(super) fn assert_recovers_the_point(
    name: &str,
    controls: Value,
) -> (Observation, ImagerRunReport, Products) {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (summary, products) = observation.image(name, controls);
    let image = products.get(".image");
    let noise_jy = observation.image_noise_jy();
    let point_pixel = component_pixel(POINT);
    let (peak_jy, peak_pixel, peak_offset_px) = peak(&image.pixels);
    let point_model_jy = box_sum(&products.get(".model").pixels, point_pixel, 2);
    let beam = image.restoring_beam();
    let psf_fit = fit_psf_main_lobe(&products.get(".psf").pixels, 0.35);
    let residual_rms_jy = off_source_rms(
        &products.get(".residual").pixels,
        &[point_pixel, component_pixel(GAUSSIAN)],
        24.0,
    );
    eprintln!(
        "T1 {name}: row-channel samples {}, thermal {noise_jy:.3e} Jy; \
         peak {peak_jy:.5} Jy at {peak_pixel:?} offset {peak_offset_px:.3?} px; \
         point model {point_model_jy:.5} Jy; beam [{:.4e}, {:.4e}] rad, PSF fit \
         [{:.4e}, {:.4e}] rad; residual RMS {residual_rms_jy:.3e} Jy; \
         {} minor iterations, {} major cycles, stop {:?}",
        observation.row_channel_samples(),
        beam.major,
        beam.minor,
        psf_fit[0],
        psf_fit[1],
        summary.actual_minor_iterations,
        summary.major_cycles,
        summary.clean_stop_reason,
    );

    // No PB is asked for, so none is written and nothing is masked. Natural
    // weighting of unit weights: sumwt counts every parallel-hand sample
    // that enters Stokes I.
    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &[".image", ".mask", ".model", ".psf", ".residual", ".sumwt"],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: false,
        },
    );
    observation.assert_image_wcs(image);

    // The point sits on a pixel centre, 68 pixels (9-17 beams) from the
    // Gaussian and 0.35 arcsec from the phase centre, where the Q-band
    // primary beam attenuates by about 1e-4. Restored peak = clean flux +
    // residual, so its error is the 1 mJy thermal noise plus sidelobe
    // leakage from the Gaussian's uncleaned remainder (PSF sidelobes <= 0.18
    // times ~40 mJy, so <= 7 mJy). 1% covers both; the clean components at
    // the point differ from the injected flux by at most the 5 mJy threshold
    // plus noise.
    assert_eq!(peak_pixel, point_pixel);
    assert!(
        peak_offset_px.iter().all(|offset| offset.abs() < 0.1),
        "point-source sub-pixel offset {peak_offset_px:?} px"
    );
    for (product, flux_jy) in [
        ("restored peak", f64::from(peak_jy)),
        ("model", point_model_jy),
    ] {
        assert!(
            (flux_jy - POINT.flux_jy).abs() < 0.01 * POINT.flux_jy,
            "point {product} {flux_jy} Jy, injected {} Jy",
            POINT.flux_jy
        );
    }

    for (axis, (restored, fitted)) in [beam.major, beam.minor]
        .into_iter()
        .zip(psf_fit)
        .enumerate()
    {
        assert!(
            (restored / fitted - 1.0).abs() < 0.1,
            "restoring-beam axis {axis}: {restored} rad, PSF main-lobe fit {fitted} rad"
        );
    }

    // Away from the sources the residual is thermal noise plus sidelobes of
    // the emission left below the 5-sigma threshold; a noise-only dirty
    // image of this fixture measures 0.90-0.97 sigma. The lower bound keeps
    // the residual normalisation honest.
    assert!(
        (0.8 * noise_jy..1.5 * noise_jy).contains(&residual_rms_jy),
        "off-source residual RMS {residual_rms_jy} Jy, thermal {noise_jy} Jy"
    );
    (observation, summary, products)
}

#[test]
fn standard_mfs_hogbom_recovers_the_analytic_sky() {
    assert_recovers_the_point("standard-mfs-hogbom", controls("hogbom"));
}

#[test]
fn standard_mfs_clark_recovers_the_analytic_sky() {
    assert_recovers_the_point("standard-mfs-clark", controls("clark"));
}

/// Faceted imaging (#664): the same sky as two-by-two facets, each gridded
/// about its own centre. The point and the Gaussian, each in its own facet
/// away from the seams, are recovered as in the unfaceted run; `.sumwt` is
/// CASA's `[2, 2]`, each facet carrying facet 0's sum; the PSF is facet 0's,
/// peaking at one at the image centre (`setPsfFromOneFacet`).
#[test]
fn standard_mfs_facets_recover_the_analytic_sky() {
    let mut faceted = controls("hogbom");
    faceted["facets"] = json!(2);
    let (observation, _, products) = assert_recovers_the_point("standard-mfs-facets", faceted);
    let sumwt = products.get(".sumwt");
    assert_eq!(&sumwt.image.shape()[..2], &[2, 2]);
    assert!(
        sumwt
            .channel_values()
            .iter()
            .all(|value| f64::from(*value) == observation.stokes_i_samples() as f64),
        "every facet's .sumwt counts the Stokes I samples: {:?}",
        sumwt.channel_values()
    );
    let centre = component_pixel(Component {
        offset_px: [0, 0],
        ..POINT
    });
    let psf = &products.get(".psf").pixels;
    assert!(
        (psf[centre] - 1.0).abs() < 1.0e-6,
        "PSF at the image centre {}",
        psf[centre]
    );
}

#[test]
fn standard_mfs_multiscale_recovers_the_extended_flux() {
    let mut multiscale = controls("multiscale");
    multiscale["scales"] = json!([0, 6, 12]);
    let (_, _, products) = assert_recovers_the_point("standard-mfs-multiscale", multiscale);
    // The 41-pixel box holds the Gaussian's 0.5 Jy to 1e-7. Cleaning stops
    // at 5 mJy/beam, so up to ~30 mJy (six beams of source at the threshold)
    // stays in the residual: the model holds less than the injected flux and
    // the restored image, which adds the residual back in clean-beam units,
    // more. Both stay within 10%, and they bracket it.
    let gaussian_model_jy = box_sum(
        &products.get(".model").pixels,
        component_pixel(GAUSSIAN),
        20,
    );
    let image = products.get(".image");
    let beam = image.restoring_beam();
    let cell_rad = CELL_ARCSEC.to_radians() / 3_600.0;
    let beam_area_px = std::f64::consts::PI * beam.major * beam.minor
        / (4.0 * std::f64::consts::LN_2 * cell_rad * cell_rad);
    let restored_jy = box_sum(&image.pixels, component_pixel(GAUSSIAN), 20) / beam_area_px;
    let residual_jy = box_sum(
        &products.get(".residual").pixels,
        component_pixel(GAUSSIAN),
        20,
    ) / beam_area_px;
    eprintln!(
        "T1 standard-mfs-multiscale: Gaussian model flux {gaussian_model_jy:.5} Jy, restored \
         {restored_jy:.5} Jy, residual {residual_jy:.5} Jy, beam area {beam_area_px:.2} px"
    );
    for (product, flux_jy) in [("model", gaussian_model_jy), ("restored", restored_jy)] {
        assert!(
            (flux_jy - GAUSSIAN.flux_jy).abs() < 0.1 * GAUSSIAN.flux_jy,
            "Gaussian {product} flux {flux_jy} Jy, injected {} Jy",
            GAUSSIAN.flux_jy
        );
    }
    assert!(
        gaussian_model_jy < GAUSSIAN.flux_jy && GAUSSIAN.flux_jy < restored_jy,
        "model {gaussian_model_jy} Jy and restored {restored_jy} Jy do not bracket {} Jy",
        GAUSSIAN.flux_jy
    );
}

#[test]
fn mem_uses_briggs_weighted_normal_images() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let briggs = json!({"weighting":"briggs", "robust":0.5, "niter":0});
    let (_, dirty) = observation.image("briggs-dirty", briggs.clone());
    let (_, natural) = observation.image("natural-dirty", json!({"niter":0}));
    let mut mem = briggs;
    mem["deconvolver"] = json!("mem");
    mem["niter"] = json!(2);
    mem["cyclefactor"] = json!(0.0001);
    mem["minpsffraction"] = json!(0.0);
    let (_, cleaned) = observation.image("briggs-mem", mem);
    let psf = &cleaned.get(".psf").pixels;
    let difference = |reference: &Products| {
        psf.iter()
            .zip(reference.get(".psf").pixels.iter())
            .map(|(actual, expected)| (actual - expected).abs())
            .fold(0.0_f32, f32::max)
    };
    assert!(difference(&dirty) < 1e-6, "MEM uses the Briggs PSF");
    assert!(
        difference(&natural) > 1e-5,
        "the fixture distinguishes the weights"
    );
    let weighted = cleaned.get(".sumwt").channel_values()[0];
    let expected = dirty.get(".sumwt").channel_values()[0];
    assert!((weighted / expected - 1.0).abs() < 1e-6);
    assert!(weighted < natural.get(".sumwt").channel_values()[0]);
    assert!(
        cleaned
            .get(".model")
            .pixels
            .iter()
            .any(|&value| value > 0.0)
    );
}

#[test]
fn mem_updates_a_saved_model_only_when_it_solves() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let name = "mem-continuation";
    let mut controls = json!({
        "deconvolver":"mem", "niter":2,
        "cyclefactor":0.0001, "minpsffraction":0.0
    });
    let (_, seed) = observation.image(name, controls.clone());
    let model = seed.get(".model");
    let old_metadata = model.image.misc_info();
    assert!(old_metadata.get("ALPHA").is_some());
    assert!(old_metadata.get("BETA").is_some());
    let model_path = observation.scratch(&format!("{name}.model"));
    let psf_path = observation.scratch(&format!("{name}.psf"));
    let mut psf = casa_images::PagedImage::<f32>::open(&psf_path).unwrap();
    // A small sidelobe marker distinguishes reading the saved PSF from
    // forming the same PSF again.
    let marker = 0.0125;
    psf.put_slice(
        &ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1, 1, 1, 1]), marker),
        &[0, 0, 0, 0],
    )
    .unwrap();
    psf.save().unwrap();
    drop(psf);

    controls["calcpsf"] = json!(false);
    controls["niter"] = json!(0);
    let (_, refresh) = observation.image(name, controls.clone());
    assert!(
        !refresh.suffixes().contains(".model"),
        "keep the saved model"
    );
    assert_eq!(
        Product::open(&psf_path).pixels[[0, 0]],
        marker,
        "reuse the saved PSF"
    );
    let residual_error = refresh
        .get(".residual")
        .pixels
        .iter()
        .zip(seed.get(".residual").pixels.iter())
        .map(|(actual, expected)| (actual - expected).abs())
        .fold(0.0_f32, f32::max);
    assert!(
        residual_error < 1e-5,
        "predict the saved model before refreshing"
    );
    let saved_model = Product::open(&model_path);
    assert_eq!(saved_model.pixels, model.pixels);
    assert_eq!(saved_model.image.misc_info(), old_metadata);

    controls["niter"] = json!(2);
    let (_, resumed) = observation.image(name, controls);
    assert!(
        resumed.suffixes().contains(".model"),
        "publish an updated model"
    );
    let new_model = resumed.get(".model");
    assert_ne!(new_model.pixels, model.pixels);
    assert!(new_model.image.misc_info().get("ALPHA").is_some());
    assert_eq!(Product::open(&psf_path).pixels[[0, 0]], marker);
}
