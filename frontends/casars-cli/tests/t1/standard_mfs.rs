// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard-gridder MFS with point, multiscale and maximum-entropy solvers.

use casa_coordinates::CoordinateModel;
use casa_tasks::imaging::{ImagerCleanStopReason, ImagerRunReport};
use casa_types::measures::direction::DirectionRef;
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

/// CASA 6.7.6.14's ICRS conversion of the fixture's J2000 centre
/// anchors the same sky while preserving the literal frame in the image.
#[test]
fn tclean_preserves_an_explicit_icrs_phase_centre() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let icrs = [-1.570_794_108_997_614_3, -0.401_423_821_764_203];
    let phasecenter = format!("ICRS {}rad {}rad", icrs[0], icrs[1]);
    let (_, products) = observation.image(
        "fixed-icrs",
        json!({"phasecenter": phasecenter, "niter": 0}),
    );
    let product = products.get(".image");
    let CoordinateModel::Direction(direction) = product.image.coordinates().coordinate(0) else {
        panic!("image coordinate 0 is not a direction coordinate");
    };
    assert_eq!(direction.direction_ref(), DirectionRef::ICRS);
    for (actual, expected) in product
        .image
        .coordinates()
        .coordinate(0)
        .reference_value()
        .iter()
        .zip(icrs)
    {
        let difference = (actual - expected + std::f64::consts::PI)
            .rem_euclid(std::f64::consts::TAU)
            - std::f64::consts::PI;
        assert!(
            difference.abs() < 1e-12,
            "ICRS reference {actual}, expected {expected}"
        );
    }
    let (_, source_pixel, offset) = peak(&product.pixels);
    assert_eq!(source_pixel, component_pixel(POINT));
    assert!(
        offset.iter().all(|value| value.abs() < 0.1),
        "source offset {offset:?} px"
    );
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
    // stays in the residual. Both model and restored flux stay within 10%.
    // A noisy finite CLEAN need not bracket the injected flux: CASA 6.7.6.14
    // also restores 0.497665 Jy on this input. Compare directly with its
    // independently computed products below.
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
    // tools/simulation/t1_fixture_imaging_oracle.py --case multiscale runs CASA
    // tclean on the same native input MS; no native images supply references.
    for (product, actual, casa) in [
        ("model", gaussian_model_jy, 0.483_791_939_904_325_1),
        ("restored", restored_jy, 0.497_664_580_447_596),
        ("residual", residual_jy, 0.013_874_676_717_472_766),
    ] {
        assert!(
            (actual - casa).abs() / GAUSSIAN.flux_jy < 1.0e-3,
            "Gaussian {product} flux {actual} Jy, CASA {casa} Jy"
        );
    }
}

#[test]
fn mem_uses_the_selected_continuum_weights() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (_, natural) = observation.image("natural-dirty", json!({"niter":0}));
    for weighting in ["briggs", "uniform", "superuniform", "radial", "briggsabs"] {
        let mut controls = json!({"weighting":weighting, "niter":0});
        if weighting == "superuniform" {
            controls["npixels"] = json!(2);
        }
        if weighting == "briggsabs" {
            controls["noise"] = json!("1Jy");
        }
        let (_, dirty) = observation.image(&format!("{weighting}-dirty"), controls.clone());
        controls["deconvolver"] = json!("mem");
        controls["niter"] = json!(2);
        controls["cyclefactor"] = json!(0.0001);
        controls["minpsffraction"] = json!(0.0);
        let (_, cleaned) = observation.image(&format!("{weighting}-mem"), controls);
        let psf = &cleaned.get(".psf").pixels;
        let difference = |reference: &Products| {
            psf.iter()
                .zip(reference.get(".psf").pixels.iter())
                .map(|(actual, expected)| (actual - expected).abs())
                .fold(0.0_f32, f32::max)
        };
        assert!(difference(&dirty) < 1e-6, "MEM uses the {weighting} PSF");
        assert!(
            difference(&natural) > 1e-5,
            "the fixture distinguishes {weighting} from natural weights"
        );
        let weighted = cleaned.get(".sumwt").channel_values()[0];
        let expected = dirty.get(".sumwt").channel_values()[0];
        assert!(
            (weighted / expected - 1.0).abs() < 1e-6,
            "MEM uses the {weighting} sum of weights"
        );
        assert!(
            cleaned
                .get(".model")
                .pixels
                .iter()
                .any(|&value| value > 0.0),
            "MEM solves with {weighting}"
        );
    }
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
    // A small sidelobe marker distinguishes reading the saved PSF from
    // forming the same PSF again.
    let marker = 0.0125;
    write_saved_corner(&psf_path, marker);

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

fn write_saved_corner(path: &std::path::Path, value: f32) {
    let mut image = casa_images::PagedImage::<f32>::open(path).unwrap();
    image
        .put_slice(
            &ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1, 1, 1, 1]), value),
            &[0, 0, 0, 0],
        )
        .unwrap();
    image.save().unwrap();
}

#[test]
fn mem_restart_reads_or_reforms_the_requested_normal_images() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let name = "mem-restart-normal-images";
    let mut controls = json!({
        "deconvolver":"mem", "niter":2,
        "cyclefactor":0.0001, "minpsffraction":0.0
    });
    let (_, seed) = observation.image(name, controls.clone());
    let psf = observation.scratch(&format!("{name}.psf"));
    let residual = observation.scratch(&format!("{name}.residual"));
    let psf_marker = 0.0125;
    let residual_marker = 0.1234;
    write_saved_corner(&psf, psf_marker);
    write_saved_corner(&residual, residual_marker);
    controls["niter"] = json!(0);
    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    observation.image(name, controls.clone());
    assert_eq!(
        Product::open(&psf).pixels[[0, 0]],
        psf_marker,
        "read the saved PSF"
    );
    assert_eq!(
        Product::open(&residual).pixels[[0, 0]],
        residual_marker,
        "read the saved residual"
    );

    controls["calcpsf"] = json!(true);
    controls["calcres"] = json!(true);
    let (_, reformed) = observation.image(name, controls);
    for suffix in [".psf", ".residual"] {
        let actual = reformed.get(suffix).pixels[[0, 0]];
        let expect = seed.get(suffix).pixels[[0, 0]];
        assert!(
            (actual - expect).abs() < 1e-6,
            "reform {suffix} from the observations"
        );
    }
}

#[test]
fn mem_stops_at_the_noise_threshold_before_solving() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let mut controls = json!({
        "deconvolver":"mem", "nsigma":0.01, "niter":2,
        "cyclefactor":0.0001, "minpsffraction":0.0
    });
    let (run, cleaned) = observation.image("mem-low-nsigma", controls.clone());
    assert_eq!(run.actual_minor_iterations, 2);
    assert!(
        cleaned
            .get(".model")
            .pixels
            .iter()
            .any(|&value| value > 0.0)
    );

    controls["nsigma"] = json!(1e6);
    let (run, stopped) = observation.image("mem-high-nsigma", controls);
    assert_eq!(
        run.clean_stop_reason,
        Some(ImagerCleanStopReason::NsigmaThresholdReached),
        "MEM observes the noise stop"
    );
    assert_eq!((run.actual_minor_iterations, run.major_cycles), (0, 1));
    assert!(
        stopped
            .get(".model")
            .pixels
            .iter()
            .all(|&value| value == 0.0)
    );
}

#[test]
fn a_saved_telescope_beam_changes_standard_and_w_primary_beam_correction() {
    let table = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/casa-simulation/tests/data/sm_squint/custom_gaussian.vp");
    let observation = Observation::synthesise(&[("point", POINT)]);
    let pixel = [16, 16];
    for (gridder, planes) in [("standard", 1), ("wproject", 8)] {
        let controls = json!({
            "gridder": gridder, "wprojplanes": planes, "niter": 0,
            "write_pb": true, "pbcor": true, "pblimit": -0.2
        });
        let (_, default) = observation.image(&format!("{gridder}-default-beam"), controls.clone());
        let mut saved = controls;
        saved["vptable"] = json!(table);
        let (_, supplied) = observation.image(&format!("{gridder}-supplied-beam"), saved);
        let pb = supplied.get(".pb");
        let extent = pb.pixels.dim().0;
        let dx = (extent / 2 - pixel[0]) as f64 * CELL_ARCSEC;
        let dy = (extent / 2 - pixel[1]) as f64 * CELL_ARCSEC;
        let radius = dx.hypot(dy) / 60.0 * (pb.channel_frequency_hz(0) / 1e9);
        // The CASA table stores a 10 arcmin power half-width and a 100
        // arcmin outer radius at 1.4 GHz. PBMath1D takes the lower of the
        // two enclosing Float samples.
        let spacing = 140.0 / 9999.0;
        let sampled_radius = (radius / spacing).floor() * spacing;
        let expected = (-(sampled_radius / 14.0).powi(2)).exp2();
        let actual = pb.pixels[pixel];
        assert!(
            (f64::from(actual) - expected).abs() <= 1e-6,
            "{gridder}: {actual} vs {expected}"
        );
        assert!((actual - default.get(".pb").pixels[pixel]).abs() > 1e-3);
        let image = supplied.get(".image").pixels[pixel];
        assert!((supplied.get(".image.pbcor").pixels[pixel] - image / actual).abs() < 1e-6);
        for name in [".psf", ".residual", ".sumwt"] {
            for (a, b) in supplied
                .get(name)
                .pixels
                .iter()
                .zip(default.get(name).pixels.iter())
            {
                assert!((a - b).abs() < 1e-6, "{gridder} {name}: {a} vs {b}");
            }
        }
    }
}
