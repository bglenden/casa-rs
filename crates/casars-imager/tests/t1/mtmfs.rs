// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, and Taylor spectral products.

use casa_ms::{SyntheticPolarizationBasis, SyntheticPolarizationSetup};
use serde_json::json;

use super::fixture::{
    CELL_ARCSEC, Component, Geometry, Observation, ProductContract, Products, Setup, SpectralAxis,
    component_pixel, peak,
};

const CHANNELS: usize = 16;
const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 5.0,
    fwhm_px: None,
    spectral_index: -1.0,
};
/// Ten times the 0.5 mJy thermal noise of the 16-channel band.
const THRESHOLD: &str = "0.005Jy";
/// Above the Q-band PB at the field's corners (0.99974), below it at the
/// point.
const PBLIMIT: f64 = 0.9998;

#[test]
fn mtmfs_two_terms_recover_flux_and_spectral_index() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let (summary, products) = observation.image(
        "mtmfs",
        json!({
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 1000,
            "threshold": THRESHOLD,
            "write_pb": true,
            "pblimit": PBLIMIT,
        }),
    );
    // CASA's MT-MFS set, with `.pb.tt0` alone: no pbcor touches pb.tt1.
    // The pblimit cuts the 3-arcsec field's edge, so the PB is zero and
    // masked there, as `SIImageStore::makePBImage` leaves it.
    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &[
                ".alpha",
                ".alpha.error",
                ".image.tt0",
                ".image.tt1",
                ".mask",
                ".model.tt0",
                ".model.tt1",
                ".pb.tt0",
                ".psf.tt0",
                ".psf.tt1",
                ".psf.tt2",
                ".residual.tt0",
                ".residual.tt1",
                ".sumwt.tt0",
                ".sumwt.tt1",
                ".sumwt.tt2",
            ],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: PBLIMIT as f32,
            uncorrected_masks: true,
        },
    );
    let pb = products.get(".pb.tt0").values();
    let zeros = pb.iter().filter(|value| **value == 0.0).count();
    let lowest = pb
        .iter()
        .filter(|value| **value > 0.0)
        .fold(f32::MAX, |low, value| low.min(*value));
    assert!(
        zeros > 0 && zeros < pb.len(),
        "the pblimit cuts the field: {zeros} of {} PB pixels zero, lowest kept {lowest}",
        pb.len()
    );
    let tt0 = products.get(".image.tt0");
    observation.assert_image_wcs(tt0);
    let reference_hz = tt0.channel_frequency_hz(0);
    let pixel = component_pixel(POINT);
    let (peak_jy, peak_pixel, _) = peak(&tt0.pixels);
    let alpha = products.get(".alpha").pixels[pixel];
    let alpha_error = products.get(".alpha.error").pixels[pixel];
    eprintln!(
        "T1 mtmfs: tt0 peak {peak_jy:.5} Jy at {peak_pixel:?}, alpha {alpha:.4} ± \
         {alpha_error:.4}; {} minor iterations, {} major cycles",
        summary.actual_minor_iterations, summary.major_cycles
    );

    // Natural weighting of unit weights: the zeroth moment counts every
    // Stokes I sample.
    assert_eq!(
        f64::from(products.get(".sumwt.tt0").pixels[[0, 0]]),
        observation.stokes_i_samples() as f64
    );
    // tt0 is the flux at the reference frequency, to the clean threshold
    // and the 0.5 mJy noise.
    assert_eq!(peak_pixel, pixel);
    let expected_jy = POINT.flux_at(reference_hz);
    assert!(
        (f64::from(peak_jy) - expected_jy).abs() < 0.01 * expected_jy,
        "tt0 peak {peak_jy} Jy, S(ν_ref) {expected_jy} Jy"
    );
    // The band spans ±2.2% about the reference, so the tt1 noise is about
    // 40 mJy and the spectral-index error about 0.008 for 5 Jy.
    assert!(
        (f64::from(alpha) - POINT.spectral_index).abs() < 0.05,
        "alpha {alpha}, injected {}",
        POINT.spectral_index
    );
    assert!(
        alpha_error.is_finite() && alpha_error < 0.05,
        "alpha error {alpha_error}"
    );
}

#[test]
fn mtmfs_three_terms_publish_spectral_curvature_on_alpha_support() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let (_, products) = observation.image(
        "mtmfs-beta",
        json!({
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 3,
            "niter": 20,
            "pbcor": true,
        }),
    );
    let beta = products.get(".beta");
    let alpha = products.get(".alpha");
    let mask = beta.mask().expect("spectral curvature validity mask");
    assert_eq!(mask, alpha.mask().expect("spectral index validity mask"));
    assert_eq!(beta.restoring_beam(), alpha.restoring_beam());
    assert!(products.suffixes().contains(".image.tt2.pbcor"));
    assert!(!products.suffixes().contains(".beta.pbcor"));

    let taylor0 = &products.get(".image.tt0").pixels;
    let taylor2 = &products.get(".image.tt2").pixels;
    let mut nonzero = false;
    for ((((&value, &valid), &index), &image0), &image2) in beta
        .pixels
        .iter()
        .zip(&mask)
        .zip(&alpha.pixels)
        .zip(taylor0)
        .zip(taylor2)
    {
        if valid {
            let expected = f64::from(image2) / f64::from(image0)
                - 0.5 * f64::from(index) * (f64::from(index) - 1.0);
            assert!(
                (f64::from(value) - expected).abs() <= 2.0e-6 * expected.abs().max(1.0),
                "spectral curvature {value}, expected {expected}"
            );
            nonzero |= value.abs() > 1.0e-3;
        } else {
            assert_eq!(value, 0.0);
        }
    }
    assert!(nonzero);
    assert!(mask.iter().any(|valid| !valid));
}

/// `‖a − b‖ / ‖b‖` of product `a` of one run and product `b` of another.
fn nrms(actual: &Products, a: &str, expected: &Products, b: &str) -> f64 {
    let (a, b) = (actual.get(a).values(), expected.get(b).values());
    assert_eq!(a.shape(), b.shape());
    let error: f64 = a
        .iter()
        .zip(&b)
        .map(|(&a, &b)| f64::from(a - b).powi(2))
        .sum();
    let power: f64 = b.iter().map(|&b| f64::from(b).powi(2)).sum();
    (error / power).sqrt()
}

/// CASA's `mtmfs` with `nterms=1` (`test_onefield_mtmfs_nterms1`) is a
/// Taylor family of one term: `.tt0` products and no spectral index
/// (`calculateAlphaBeta` needs two terms). On the point scale alone its
/// minor cycle is Högbom's: one term's Hessian is the PSF's unit peak, so
/// each step takes the residual's peak and subtracts the gain-scaled PSF
/// there, within the cleaner's patch (`findBeamPatch`, 80 pixels), which
/// covers an 80-pixel image. Its major cycle grids what a constant basis
/// grids. So its PSF, sums of weights, PB, model, residual and restored
/// image are Högbom's, and its iterations and major cycles too.
#[test]
fn one_taylor_term_is_hogbom_where_its_patch_covers_the_image() {
    const FLAT: Component = Component {
        offset_px: [9, -6],
        flux_jy: 5.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(
        Setup {
            geometry: Geometry::square(80, CELL_ARCSEC),
            ..Setup::vla_q_band(CHANNELS)
        },
        &[("point", FLAT)],
    );
    let controls = |deconvolver: &str| {
        json!({
            "weighting": "natural",
            "deconvolver": deconvolver,
            "nterms": 1,
            "niter": 1000,
            "threshold": THRESHOLD,
            "write_pb": true,
            "pblimit": 0.2,
        })
    };
    let (taylor_run, taylor) = observation.image("one-term", controls("mtmfs"));
    let (hogbom_run, hogbom) = observation.image("hogbom", controls("hogbom"));
    observation.assert_product_contract(
        &taylor,
        &ProductContract {
            inventory: &[
                ".image.tt0",
                ".mask",
                ".model.tt0",
                ".pb.tt0",
                ".psf.tt0",
                ".residual.tt0",
                ".sumwt.tt0",
            ],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: true,
        },
    );
    eprintln!(
        "T1 one-term mtmfs: {} minor iterations, {} major cycles; Högbom {} and {}",
        taylor_run.actual_minor_iterations,
        taylor_run.major_cycles,
        hogbom_run.actual_minor_iterations,
        hogbom_run.major_cycles
    );
    // Both stop on the threshold, before the iteration budget where
    // Högbom's inclusive loop would take one component more.
    assert!(taylor_run.actual_minor_iterations > 0);
    assert!(hogbom_run.actual_minor_iterations < 1000);
    assert_eq!(
        (taylor_run.actual_minor_iterations, taylor_run.major_cycles),
        (hogbom_run.actual_minor_iterations, hogbom_run.major_cycles)
    );
    for (term, single) in [
        (".psf.tt0", ".psf"),
        (".sumwt.tt0", ".sumwt"),
        (".pb.tt0", ".pb"),
        (".model.tt0", ".model"),
        (".residual.tt0", ".residual"),
        (".image.tt0", ".image"),
    ] {
        let error = nrms(&taylor, term, &hogbom, single);
        assert!(error < 1e-5, "{term} against {single}: {error}");
    }
    assert_eq!(
        taylor.get(".mask").values(),
        hogbom.get(".mask").values(),
        "one CLEAN mask"
    );
    // And the flat spectrum's flux, to the threshold and the noise.
    let (peak_jy, peak_pixel, _) = peak(&taylor.get(".image.tt0").pixels);
    assert_eq!(peak_pixel, observation.geometry().pixel(FLAT));
    assert!(
        (f64::from(peak_jy) - FLAT.flux_jy).abs() < 0.01 * FLAT.flux_jy,
        "tt0 peak {peak_jy} Jy, S {} Jy",
        FLAT.flux_jy
    );
}

/// The `[x, y]` plane of Stokes plane `stokes` of `values`.
fn stokes_plane(values: &ndarray::ArrayD<f32>, stokes: usize) -> ndarray::ArrayD<f32> {
    values.index_axis(ndarray::Axis(2), stokes).to_owned()
}

fn abs_peak(values: &ndarray::ArrayD<f32>) -> f32 {
    values
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()))
}

/// The normalized RMS difference of plane `actual` from plane `expected`.
fn plane_nrms(actual: &ndarray::ArrayD<f32>, expected: &ndarray::ArrayD<f32>) -> f64 {
    let (error, power) =
        actual
            .iter()
            .zip(expected)
            .fold((0.0_f64, 0.0_f64), |(error, power), (a, b)| {
                let (a, b) = (f64::from(*a), f64::from(*b));
                (error + (a - b).powi(2), power + b * b)
            });
    (error / power).sqrt()
}

/// CASA's `mtmfs` cleans and restores each Stokes plane as a Taylor family
/// of its own (`SDAlgorithmMSMFS` loops over polarization planes;
/// `test_stokes_mtmfs_Q`, `test_stokes_weighting`). Of parallel-hand data,
/// `stokes='IQUV'` images Stokes I's family as an I-only run does, to
/// rounding, in every term; Q and U have no data, and V is a family of its
/// own. Every Stokes plane of a Taylor term has the first correlation's
/// sum of weights (`MultiTermFTNew` calls `ToStokesSumWt` without
/// coordinates), so Q and U have Stokes I's where a single term's have
/// none. Every plane is restored with its own PSF's beam (all Stokes I's
/// here, as CASA's test checks), and no spectral index is written, as for
/// anything but Stokes I alone (`calculateAlphaBeta` needs
/// `isOnlyStokesI`).
#[test]
fn full_stokes_taylor_families_image_each_stokes_plane_on_its_own() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let controls = |stokes: &str| {
        json!({
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 1000,
            "threshold": THRESHOLD,
            "stokes": stokes,
        })
    };
    let (i_run, i_only) = observation.image("i", controls("I"));
    let (full_run, full) = observation.image("iquv", controls("IQUV"));
    assert!(i_only.suffixes().contains(".alpha"));
    let inventory = full.suffixes();
    assert!(
        !inventory.iter().any(|suffix| suffix.starts_with(".alpha")),
        "{inventory:?}"
    );
    for term in [".sumwt.tt0", ".sumwt.tt1", ".sumwt.tt2"] {
        let sumwt = full.get(term).channel_values();
        assert_eq!(sumwt.len(), 4, "{term}");
        assert_ne!(sumwt[0], 0.0, "{term}");
        assert_eq!(sumwt, [sumwt[0]; 4], "{term}");
        // An I-only run grids both hands on its one plane; the fixture
        // weighs RR and LL alike, so RR's sum is half of theirs.
        assert_eq!(
            2.0 * sumwt[0],
            i_only.get(term).channel_values()[0],
            "{term}"
        );
    }
    for suffix in [
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".residual.tt0",
        ".residual.tt1",
        ".model.tt0",
        ".model.tt1",
        ".image.tt0",
        ".image.tt1",
    ] {
        let values = full.get(suffix).values();
        let i = stokes_plane(&i_only.get(suffix).values(), 0);
        let (error, power) = stokes_plane(&values, 0).iter().zip(&i).fold(
            (0.0_f64, 0.0_f64),
            |(error, power), (a, b)| {
                let (a, b) = (f64::from(*a), f64::from(*b));
                (error + (a - b).powi(2), power + b * b)
            },
        );
        let nrms = (error / power).sqrt();
        assert!(nrms < 1e-6, "{suffix} Stokes I NRMS {nrms:e}");
        if !suffix.starts_with(".psf") {
            for stokes in [1, 2] {
                assert!(
                    stokes_plane(&values, stokes)
                        .iter()
                        .all(|value| *value == 0.0),
                    "{suffix} Stokes plane {stokes} is empty"
                );
            }
        }
    }
    // Stokes V's family holds the two hands' noise alone, below the
    // threshold: it is formed (its residual is not zero) and cleans nothing.
    assert_eq!(
        full_run.actual_minor_iterations,
        i_run.actual_minor_iterations
    );
    for suffix in [".model.tt0", ".model.tt1"] {
        assert!(
            stokes_plane(&full.get(suffix).values(), 3)
                .iter()
                .all(|value| *value == 0.0),
            "{suffix} Stokes V"
        );
    }
    let v_peak = abs_peak(&stokes_plane(&full.get(".residual.tt0").values(), 3));
    assert!(
        v_peak > 0.0 && v_peak < 0.005,
        "Stokes V residual peak {v_peak}"
    );
    let beams = &full
        .get(".image.tt0")
        .image
        .image_info()
        .expect("image info")
        .beam_set;
    assert_eq!(beams.n_stokes(), 4);
    // Q and U fit the PSF CASA's conversion gives them, Stokes I's: the
    // same beam to rounding.
    let i_beam = i_only.get(".image.tt0").restoring_beam();
    let close = |a: f64, b: f64| (a - b).abs() <= 1e-9 * b.abs().max(1e-12);
    for stokes in 0..4 {
        let beam = beams.beam(0, stokes);
        assert!(
            close(beam.major, i_beam.major)
                && close(beam.minor, i_beam.minor)
                && close(beam.position_angle, i_beam.position_angle),
            "Stokes plane {stokes}'s beam {beam:?}, Stokes I's {i_beam:?}"
        );
    }
}

/// A polarized Taylor family is imaged on its own Stokes plane. Linear
/// feeds observe the point 40 % circularly polarized, so Stokes V holds
/// 0.4 of Stokes I's flux and spectrum. `stokes='IV'` recovers it on V's
/// plane (index 1) of every term, with Stokes I's PSF the I-only run's and
/// its image the I-only run's to clean's resolution; `stokes='IQUV'` images
/// the same V family on index 3, with Q and U at the noise. A family written to another polarization's plane fails
/// both. With `restoringbeam='common'` the image records one beam for all
/// its planes, as `SDAlgorithmMSMFS::restore` sets one restoring beam.
#[test]
fn a_polarized_taylor_family_is_imaged_on_its_own_stokes_plane() {
    const V_FRACTION: f32 = 0.4;
    let observation = Observation::synthesise_polarized(
        Setup::vla_q_band(CHANNELS),
        SyntheticPolarizationSetup {
            basis: SyntheticPolarizationBasis::Linear,
            correlation_count: 4,
            source_stokes_fractional_quv: [0.0, 0.0, f64::from(V_FRACTION)],
        },
        &[("point", POINT)],
    );
    let controls = |stokes: &str, beam: &str| {
        json!({
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 1000,
            "threshold": THRESHOLD,
            "stokes": stokes,
            "restoringbeam": beam,
        })
    };
    let (i_run, i_only) = observation.image("i", controls("I", ""));
    let (iv_run, iv) = observation.image("iv", controls("IV", ""));
    let (_, full) = observation.image("iquv", controls("IQUV", "common"));
    // V's family is cleaned too, in the same major cycles. The families
    // share each cycle's iteration control, so Stokes I's agrees with an
    // I-only run's to clean's resolution, not to rounding.
    assert!(iv_run.actual_minor_iterations > i_run.actual_minor_iterations);
    assert_eq!(iv_run.major_cycles, i_run.major_cycles);
    for suffix in [".psf.tt0", ".psf.tt1", ".psf.tt2"] {
        assert_eq!(
            stokes_plane(&iv.get(suffix).values(), 0),
            stokes_plane(&i_only.get(suffix).values(), 0),
            "{suffix}"
        );
    }
    for suffix in [".image.tt0", ".model.tt0", ".model.tt1"] {
        let i = stokes_plane(&i_only.get(suffix).values(), 0);
        let error = plane_nrms(&stokes_plane(&iv.get(suffix).values(), 0), &i);
        assert!(error < 1e-3, "{suffix} Stokes I NRMS {error:e}");
    }
    let [x, y] = component_pixel(POINT);
    for (suffix, tolerance) in [(".image.tt0", 0.01), (".image.tt1", 0.03)] {
        let i = i_only.get(suffix).values()[&[x, y, 0, 0][..]];
        let v = iv.get(suffix).values()[&[x, y, 1, 0][..]];
        assert!(
            (v / i - V_FRACTION).abs() <= tolerance * V_FRACTION,
            "{suffix}: V {v}, I {i}"
        );
    }
    let values = full.get(".image.tt0").values();
    let v_peak = abs_peak(&stokes_plane(&values, 3));
    for stokes in [1, 2] {
        let peak = abs_peak(&stokes_plane(&values, stokes));
        assert!(
            peak < 0.01 * v_peak,
            "Stokes plane {stokes}: {peak}, V {v_peak}"
        );
    }
    // Q and U clean nothing, so V's family is the IV run's to rounding,
    // restored with the same beam under 'common'.
    for suffix in [
        ".model.tt0",
        ".model.tt1",
        ".residual.tt0",
        ".residual.tt1",
        ".image.tt0",
    ] {
        let error = plane_nrms(
            &stokes_plane(&full.get(suffix).values(), 3),
            &stokes_plane(&iv.get(suffix).values(), 1),
        );
        assert!(error < 1e-6, "{suffix} Stokes V NRMS {error:e}");
    }
    let beams = |products: &Products| {
        products
            .get(".image.tt0")
            .image
            .image_info()
            .expect("image info")
            .beam_set
    };
    assert_eq!(beams(&iv).n_stokes(), 2);
    assert!(beams(&iv).single_beam().is_none());
    assert!(beams(&full).single_beam().is_some());
}
