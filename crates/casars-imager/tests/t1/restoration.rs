// SPDX-License-Identifier: LGPL-3.0-or-later

//! Restoration as tclean's `restoringbeam` and `restoration` ask, and an
//! MFS image's header in tclean's `outframe` with its `restfreq`.
//!
//! A beam the request gives restores every plane (CASA's
//! `SIImageStore::restore`): the model convolved with it, plus the residual
//! brought from the PSF's fitted beam to it by `rescaleResolution` (a
//! unit-volume smoothing, times the ratio of the beam areas), or left as it
//! is when the fitted beam does not deconvolve from the given one. Over the
//! whole image the restored flux is therefore analytic:
//! `Σ image = Σ model · A_given / cell² + (A_given / A_fitted) · Σ residual`,
//! the rescaling's factor being one when it is skipped.

use std::f64::consts::{LN_2, PI};

use casa_coordinates::CoordinateModel;
use casa_images::GaussianBeam;
use casa_types::measures::frequency::FrequencyRef;
use serde_json::{Value, json};

use super::fixture::{
    CELL_ARCSEC, Component, Observation, ProductContract, Products, SpectralAxis, component_pixel,
    off_source_rms, peak,
};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// Five times the 1.0 mJy thermal image noise.
const THRESHOLD: &str = "0.005Jy";
const ARCSEC: f64 = PI / (180.0 * 3600.0);

fn controls(restoringbeam: Value) -> Value {
    json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": THRESHOLD,
        "restoringbeam": restoringbeam,
    })
}

fn area(beam: &GaussianBeam) -> f64 {
    PI / (4.0 * LN_2) * beam.major * beam.minor
}

fn sum(products: &Products, suffix: &str) -> f64 {
    products
        .get(suffix)
        .values()
        .iter()
        .map(|value| f64::from(*value))
        .sum()
}

fn assert_beam(actual: GaussianBeam, expected: GaussianBeam, context: &str) {
    for (actual, expected) in [
        (actual.major, expected.major),
        (actual.minor, expected.minor),
        (actual.position_angle, expected.position_angle),
    ] {
        assert!(
            (actual - expected).abs() <= 1.0e-9 * expected.abs().max(1.0e-9),
            "{context}: {actual:?} != {expected:?}"
        );
    }
}

/// Image the point with `restoringbeam` and hold the run to the analytic
/// restoration: the image records the given beam and the PSF its fitted
/// one; the point keeps its flux at its pixel; and the restored flux
/// sums as the module's law states, with the residual scaled by
/// `rescale` (the area ratio) or not.
fn assert_restores_with(name: &str, restoringbeam: Value, given: GaussianBeam, rescaled: bool) {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, products) = observation.image(name, controls(restoringbeam));
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
    let image = products.get(".image");
    assert_beam(image.restoring_beam(), given, ".image beam");
    // The PSF records the fitted beam; CASA's residual records none.
    let fitted = products.get(".psf").restoring_beam();
    let narrower = given.minor < fitted.minor;
    assert_eq!(!narrower, rescaled, "{name}: fitted {fitted:?}");

    let (peak_jy, peak_pixel, _) = peak(&image.pixels);
    assert_eq!(peak_pixel, component_pixel(POINT));
    assert!(
        (f64::from(peak_jy) - POINT.flux_jy).abs() < 0.01 * POINT.flux_jy,
        "{name}: restored peak {peak_jy} Jy/beam, {} Jy injected",
        POINT.flux_jy
    );

    let cell = CELL_ARCSEC * ARCSEC;
    let ratio = if rescaled {
        area(&given) / area(&fitted)
    } else {
        1.0
    };
    let model = sum(&products, ".model");
    let residual = sum(&products, ".residual");
    let expected = model * area(&given) / (cell * cell) + ratio * residual;
    let restored = sum(&products, ".image");
    eprintln!(
        "T1 {name}: fitted [{:.4e}, {:.4e}] rad, given [{:.4e}, {:.4e}] rad, area ratio \
         {ratio:.4}; Σ model {model:.6} Jy, Σ residual {residual:.4} Jy/beam, Σ image \
         {restored:.4}, expected {expected:.4}",
        fitted.major, fitted.minor, given.major, given.minor,
    );
    assert!(
        (restored - expected).abs() < 1.0e-4 * expected.abs(),
        "{name}: Σ image {restored}, Σ model ⊗ given + rescaled residual {expected}"
    );
    // Off the source the image is the residual's noise: smoothed from the
    // fitted beam's area to the given one's and scaled by their ratio, its
    // RMS grows by about sqrt(A_given / A_fitted), 4 here; added as it is,
    // it stays the residual's.
    let pixel = [component_pixel(POINT)];
    let growth = off_source_rms(&image.pixels, &pixel, 40.0)
        / off_source_rms(&products.get(".residual").pixels, &pixel, 40.0);
    eprintln!("T1 {name}: off-source RMS grows by {growth:.3}");
    if rescaled {
        assert!(growth > 2.0, "{name}: off-source RMS grows by {growth}");
    } else {
        assert!(
            (growth - 1.0).abs() < 0.2,
            "{name}: off-source RMS grows by {growth}"
        );
    }
}

/// A beam wider than the PSF's on both axes: CASA restores with it and
/// rescales the residual to it.
#[test]
fn a_given_wider_beam_restores_with_it_and_rescales_the_residual() {
    assert_restores_with(
        "given-wider-beam",
        json!(["0.3arcsec", "0.25arcsec", "20deg"]),
        GaussianBeam::new(0.3 * ARCSEC, 0.25 * ARCSEC, 20.0_f64.to_radians()),
        true,
    );
}

/// A beam narrower than the PSF's: the fitted beam does not deconvolve from
/// it, so CASA warns ("Cannot convolve to new beam") and adds the residual
/// as it is; the image still records the given beam. A circular beam given
/// by one quantity, as CASA's `restoringbeam='0.035arcsec'`.
#[test]
fn a_given_narrower_beam_adds_the_residual_as_it_is() {
    assert_restores_with(
        "given-narrower-beam",
        json!("0.035arcsec"),
        GaussianBeam::new(0.035 * ARCSEC, 0.035 * ARCSEC, 0.0),
        false,
    );
}

/// MT-MFS with a given beam: every Taylor image and the spectral index
/// record it, the PSF keeps the fitted beam, and the point's flux and
/// spectral index are recovered as with the fitted beam.
#[test]
fn mtmfs_restores_every_term_with_a_given_beam() {
    const CHANNELS: usize = 16;
    const POINT: Component = Component {
        offset_px: [24, 16],
        flux_jy: 5.0,
        fwhm_px: None,
        spectral_index: -1.0,
    };
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let (_, products) = observation.image(
        "mtmfs-given-beam",
        json!({
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 1000,
            "threshold": THRESHOLD,
            "restoringbeam": ["0.3arcsec", "0.25arcsec", "-30deg"],
        }),
    );
    let given = GaussianBeam::new(0.3 * ARCSEC, 0.25 * ARCSEC, (-30.0_f64).to_radians());
    for suffix in [".image.tt0", ".image.tt1", ".alpha", ".alpha.error"] {
        assert_beam(products.get(suffix).restoring_beam(), given, suffix);
    }
    let fitted = products.get(".psf.tt0").restoring_beam();
    assert!(fitted.major < given.minor, "fitted {fitted:?}");
    let tt0 = products.get(".image.tt0");
    let pixel = component_pixel(POINT);
    let (peak_jy, peak_pixel, _) = peak(&tt0.pixels);
    assert_eq!(peak_pixel, pixel);
    let expected_jy = POINT.flux_at(tt0.channel_frequency_hz(0));
    assert!(
        (f64::from(peak_jy) - expected_jy).abs() < 0.01 * expected_jy,
        "tt0 peak {peak_jy} Jy/beam, S(ν_ref) {expected_jy} Jy"
    );
    let alpha = products.get(".alpha").pixels[pixel];
    assert!(
        (f64::from(alpha) - POINT.spectral_index).abs() < 0.05,
        "alpha {alpha}, injected {}",
        POINT.spectral_index
    );
}

/// CASA's `restoration=False` neither restores nor PB-corrects: no `.image`
/// and, with `pbcor`, no `.image.pbcor`; the PB `pbcor` asks for is made.
/// The catalog holds `restoration` at its default until its contract
/// activates it, so the resolved request is given it.
#[test]
fn without_restoration_no_image_is_written() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let mut clean = controls(json!([]));
    clean["pbcor"] = json!(true);
    let (_, products) = observation.image_edited("without-restoration", clean, |request| {
        assert!(request.restoration);
        request.restoration = false;
    });
    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &[".mask", ".model", ".pb", ".psf", ".residual", ".sumwt"],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: true,
        },
    );
    let model = sum(&products, ".model");
    assert!(
        (model - POINT.flux_jy).abs() < 0.01 * POINT.flux_jy,
        "the minor cycle still cleans: Σ model {model} Jy"
    );
}

/// An MFS image's header takes its frame from `outframe` and its rest
/// frequencies from `restfreq` (`buildCoordinateSystemCore`); CASA grids
/// the samples at their LSRK frequencies whatever the frame
/// (`FTMachine::matchChannel`), so the pixels are the LSRK image's. The
/// band's BARY envelope is its LSRK one scaled by the frames' one Doppler
/// factor towards the field, its midpoint and width alike.
#[test]
fn an_mfs_header_records_outframe_and_restfreq() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let dirty = json!({ "weighting": "natural" });
    let (_, lsrk) = observation.image("mfs-lsrk", dirty.clone());
    let mut bary_controls = dirty;
    bary_controls["outframe"] = json!("BARY");
    bary_controls["restfreq"] = json!(["44.5GHz", "45.5GHz"]);
    let (_, bary) = observation.image("mfs-bary", bary_controls);
    for suffix in [".image", ".psf", ".residual"] {
        assert_eq!(
            bary.get(suffix).values(),
            lsrk.get(suffix).values(),
            "{suffix}: the frame labels the header alone"
        );
    }
    let spectral = |products: &Products| {
        let image = products.get(".image");
        let CoordinateModel::Spectral(spectral) = image.image.coordinates().coordinate(2).clone()
        else {
            panic!("coordinate 2 is not spectral");
        };
        (
            spectral,
            image.channel_frequency_hz(0),
            image.channel_width_hz(),
        )
    };
    let (lsrk_axis, lsrk_hz, lsrk_width) = spectral(&lsrk);
    let (bary_axis, bary_hz, bary_width) = spectral(&bary);
    assert_eq!(lsrk_axis.frequency_ref(), FrequencyRef::LSRK);
    assert_eq!(bary_axis.frequency_ref(), FrequencyRef::BARY);
    assert_eq!(bary_axis.rest_frequency(), 44.5e9);
    assert_eq!(bary_axis.rest_frequencies(), [44.5e9, 45.5e9]);
    let factor = bary_hz / lsrk_hz;
    eprintln!(
        "T1 mfs outframe: LSRK {lsrk_hz} Hz wide {lsrk_width}, BARY {bary_hz} Hz wide \
         {bary_width}, factor {factor}, LSRK rest {}",
        lsrk_axis.rest_frequency()
    );
    // The LSR moves at 20 km/s relative to the barycentre: 6.7e-5 at most.
    assert!(
        (1.0e-6..7.0e-5).contains(&(factor - 1.0).abs()),
        "BARY/LSRK {factor}"
    );
    assert!(
        (bary_width / lsrk_width - factor).abs() < 1.0e-9,
        "the width scales by {}, the midpoint by {factor}",
        bary_width / lsrk_width
    );
}
