// SPDX-License-Identifier: LGPL-3.0-or-later

//! Separately centred PSFs for mosaic continuum and cube imaging (#1062).

use casa_coordinates::{Coordinate, CoordinateModel};
use casa_images::PagedImage;
use casa_types::measures::direction::DirectionRef;
use serde_json::json;

use super::fixture::{Component, Observation, Product, Setup};
use super::mosaic::mosaic_run;

fn setup(cube: bool) -> (Observation, serde_json::Value, String) {
    let (observation, mut controls) = mosaic_run(cube);
    let spacing = 80.0 * observation.geometry().cell_rad()[0];
    let [longitude, latitude] = Setup::pointing([-spacing, 0.0]);
    let alternate = format!("J2000 {longitude}rad {latitude}rad");
    controls["imsize"] = json!(256);
    controls["niter"] = json!(0);
    controls["threshold"] = json!("0Jy");
    (observation, controls, alternate)
}

fn nrms(actual: &Product, reference: &Product) -> f64 {
    assert_eq!(actual.pixels.shape(), reference.pixels.shape());
    let error: f64 = actual
        .pixels
        .iter()
        .zip(&reference.pixels)
        .map(|(&a, &b)| (f64::from(a) - f64::from(b)).powi(2))
        .sum();
    let power: f64 = reference
        .pixels
        .iter()
        .map(|&value| f64::from(value).powi(2))
        .sum();
    if error == 0.0 {
        0.0
    } else {
        (error / power).sqrt()
    }
}

fn assert_same(actual: &Product, reference: &Product, suffix: &str) {
    let error = nrms(actual, reference);
    assert!(error < 1e-5, "{suffix} normalized RMS {error:.3e}");
    assert_eq!(actual.mask(), reference.mask(), "{suffix} mask");
    assert_eq!(
        actual.image.coordinates().coordinate(0).reference_value(),
        reference
            .image
            .coordinates()
            .coordinate(0)
            .reference_value(),
        "{suffix} direction",
    );
}

#[test]
fn alternate_mosaic_psf_changes_only_the_psf_and_its_beam() {
    check_alternate_psf(false);
}

#[test]
fn alternate_mosaic_cube_psf_replaces_each_channel_and_its_beam() {
    check_alternate_psf(true);
}

fn check_alternate_psf(cube: bool) {
    let (observation, mut controls, alternate) = setup(cube);
    let (_, ordinary) = observation.image("ordinary-mosaic", controls.clone());
    let mut shifted_controls = controls.clone();
    shifted_controls["phasecenter"] = json!(alternate);
    let (_, shifted) = observation.image("shifted-mosaic", shifted_controls);
    controls["psfphasecenter"] = json!(alternate);
    let (_, separate) = observation.image("separate-mosaic-psf", controls);

    // The shifted chart excludes the last pointing. Its PSF determines
    // the fitted beam, while the original mosaic supplies all other normals.
    let sums = ordinary.get(".sumwt").channel_values();
    assert_eq!(sums.len(), if cube { 2 } else { 1 });
    for (shifted_sum, sum) in shifted.get(".sumwt").channel_values().iter().zip(&sums) {
        assert!(shifted_sum < sum);
    }
    assert_eq!(separate.get(".sumwt").channel_values(), sums);
    assert_same(separate.get(".psf"), shifted.get(".psf"), ".psf");
    assert!(nrms(separate.get(".psf"), ordinary.get(".psf")) > 1e-3);
    for suffix in [".residual", ".pb", ".weight"] {
        assert_same(separate.get(suffix), ordinary.get(suffix), suffix);
    }
    for channel in 0..sums.len() {
        let psf = separate.get(".psf").channel_beam(channel);
        assert_eq!(separate.get(".image").channel_beam(channel), psf);
        let expected = shifted.get(".psf").channel_beam(channel);
        assert!((psf.major / expected.major - 1.0).abs() < 1e-5);
        assert!((psf.minor / expected.minor - 1.0).abs() < 1e-5);
    }
}

fn tracked_evla() -> Observation {
    let mut setup = Setup::vla_q_band(4);
    setup.telescope = "EVLA".to_owned();
    super::ephemeris::following_body(Observation::synthesise_setup(
        setup,
        &[(
            "point",
            Component {
                offset_px: [10, -6],
                flux_jy: 1.0,
                fwhm_px: None,
                spectral_index: 0.0,
            },
        )],
    ))
}

#[test]
fn a_tracked_mosaic_forms_and_reuses_a_fixed_alternate_psf() {
    let observation = tracked_evla();
    let [longitude, latitude] = Setup::pointing([12.0_f64.to_radians() / 3600.0, 0.0]);
    let alternate = format!("J2000 {longitude}rad {latitude}rad");
    let mut controls = json!({
        "gridder": "mosaic", "weighting": "natural", "phasecenter": "TRACKFIELD",
        "imsize": 192, "cell": "0.5arcsec", "pblimit": -0.2, "write_pb": true, "niter": 0
    });
    let (_, ordinary) = observation.image("tracked", controls.clone());
    let mut fixed = controls.clone();
    fixed["phasecenter"] = json!(alternate);
    let (_, shifted) = observation.image("fixed", fixed);
    controls["psfphasecenter"] = json!(alternate);
    let (_, separate) = observation.image("alternate", controls.clone());

    assert_same(separate.get(".psf"), shifted.get(".psf"), ".psf");
    assert!(nrms(separate.get(".psf"), ordinary.get(".psf")) > 1e-3);
    for suffix in [".residual", ".pb", ".weight", ".sumwt"] {
        assert_same(separate.get(suffix), ordinary.get(suffix), suffix);
    }
    assert_eq!(
        separate.get(".image").channel_beam(0),
        separate.get(".psf").channel_beam(0)
    );

    controls.as_object_mut().unwrap().remove("psfphasecenter");
    controls["calcpsf"] = json!(false);
    let (_, resumed) = observation.image("alternate", controls);
    assert_same(resumed.get(".image"), separate.get(".image"), ".image");
    assert!(!resumed.suffixes().contains(".psf"));
    let saved = Product::open(&observation.scratch("alternate.psf"));
    assert_same(&saved, separate.get(".psf"), ".psf");

    let Err(reason) = observation.try_image(
        "source-frame-with-fixed-psf",
        json!({
            "gridder": "mosaic", "weighting": "natural", "phasecenter": "TRACKFIELD",
            "specmode": "cubesource", "psfphasecenter": alternate, "niter": 0
        }),
    ) else {
        panic!("a fixed alternate PSF was admitted for a source-frame cube");
    };
    assert!(
        reason.contains("a fixed alternate PSF centre cannot track"),
        "{reason}"
    );

    let Err(reason) = observation.try_image(
        "single-channel-tracked-taylor-with-fixed-psf",
        json!({
            "gridder": "mosaic", "weighting": "natural", "phasecenter": "TRACKFIELD",
            "deconvolver": "mtmfs", "nterms": 2, "spw": "0:0", "psfphasecenter": alternate,
            "niter": 0
        }),
    ) else {
        panic!("an alternate Taylor PSF was admitted on a single tracked channel");
    };
    assert!(
        reason.contains("more than one selected native channel"),
        "{reason}"
    );
}

#[test]
fn a_tracked_cube_reuses_a_saved_psf_with_equal_coordinates_in_another_frame() {
    let observation = tracked_evla();
    let mut controls = json!({
        "gridder": "mosaic", "weighting": "natural", "phasecenter": "TRACKFIELD",
        "specmode": "cube", "outframe": "LSRK", "nchan": 2, "start": 0, "width": 1,
        "imsize": 192, "cell": "0.5arcsec", "pblimit": -0.2, "write_pb": true, "niter": 0
    });
    let (_, ordinary) = observation.image("tracked-cube", controls.clone());
    let CoordinateModel::Direction(main) =
        ordinary.get(".residual").image.coordinates().coordinate(0)
    else {
        panic!("the main image has a direction coordinate");
    };
    assert_eq!(main.direction_ref(), DirectionRef::ICRS);
    let [longitude, latitude] = [main.reference_value()[0], main.reference_value()[1]];
    controls["psfphasecenter"] = json!(format!("J2000 {longitude}rad {latitude}rad"));
    let (_, seeded) = observation.image("alternate-cube", controls.clone());
    let CoordinateModel::Direction(psf) = seeded.get(".psf").image.coordinates().coordinate(0)
    else {
        panic!("the PSF has a direction coordinate");
    };
    assert_eq!(psf.direction_ref(), DirectionRef::J2000);
    assert_eq!(psf.reference_value(), main.reference_value());

    controls.as_object_mut().unwrap().remove("psfphasecenter");
    controls["calcpsf"] = json!(false);
    let (_, resumed) = observation.image("alternate-cube", controls);
    assert_same(resumed.get(".image"), seeded.get(".image"), ".image");
    assert!(!resumed.suffixes().contains(".psf"));
}

fn taylor_setup() -> (Observation, serde_json::Value, String) {
    let (observation, mut controls, alternate) = setup(true);
    controls["specmode"] = json!("mfs");
    controls["deconvolver"] = json!("mtmfs");
    controls["nterms"] = json!(2);
    controls["pbcor"] = json!(false);
    (observation, controls, alternate)
}

#[test]
fn alternate_mosaic_taylor_psf_uses_all_signed_moments() {
    let (observation, mut controls, alternate) = taylor_setup();
    let (_, ordinary) = observation.image("ordinary-taylor-mosaic", controls.clone());
    let mut shifted = controls.clone();
    shifted["phasecenter"] = json!(alternate);
    let (_, shifted) = observation.image("shifted-taylor-mosaic", shifted);
    controls["psfphasecenter"] = json!(alternate);
    let (_, separate) = observation.image("separate-taylor-mosaic", controls);
    for term in 0..3 {
        let psf = format!(".psf.tt{term}");
        assert_same(separate.get(&psf), shifted.get(&psf), &psf);
        assert!(nrms(separate.get(&psf), ordinary.get(&psf)) > 1e-3);
        for family in [".weight", ".sumwt"] {
            let suffix = format!("{family}.tt{term}");
            assert_same(separate.get(&suffix), ordinary.get(&suffix), &suffix);
        }
    }
    for suffix in [".pb.tt0", ".residual.tt0", ".residual.tt1"] {
        assert_same(separate.get(suffix), ordinary.get(suffix), suffix);
    }
    let alternate_beam = shifted.get(".psf.tt0").restoring_beam();
    let matches_alternate = |beam: casa_images::GaussianBeam| {
        (beam.major / alternate_beam.major - 1.0).abs() < 1e-5
            && (beam.minor / alternate_beam.minor - 1.0).abs() < 1e-5
            && (beam.position_angle - alternate_beam.position_angle).abs() < 1e-5
    };
    assert!(!matches_alternate(
        ordinary.get(".psf.tt0").restoring_beam()
    ));
    let beam = separate.get(".psf.tt0").restoring_beam();
    assert!(matches_alternate(beam));
    assert_eq!(separate.get(".image.tt0").restoring_beam(), beam);
}

#[test]
fn alternate_mosaic_psf_can_be_reused_and_checks_the_saved_pixel_grid() {
    check_saved_psf(false);
}

#[test]
fn alternate_mosaic_cube_psf_can_be_reused_and_checks_the_saved_pixel_grid() {
    check_saved_psf(true);
}

fn check_saved_psf(cube: bool) {
    let (observation, mut controls, alternate) = setup(cube);
    controls["psfphasecenter"] = json!(alternate);
    let (_, seeded) = observation.image("saved-mosaic-psf", controls.clone());
    controls["niter"] = json!(60);
    let (_, fresh) = observation.image("fresh-mosaic-psf", controls.clone());
    controls.as_object_mut().unwrap().remove("psfphasecenter");
    controls["calcpsf"] = json!(false);
    let (_, continued) = observation.image("saved-mosaic-psf", controls.clone());
    for suffix in [".psf", ".weight", ".pb", ".sumwt"] {
        assert!(!continued.suffixes().contains(suffix), "rewrote {suffix}");
        let after = Product::open(&observation.scratch(&format!("saved-mosaic-psf{suffix}")));
        assert_eq!(after.pixels, seeded.get(suffix).pixels, "{suffix}");
    }
    for suffix in [".model", ".residual", ".image"] {
        assert_same(continued.get(suffix), fresh.get(suffix), suffix);
    }

    let path = observation.scratch("saved-mosaic-psf.psf");
    let mut psf = PagedImage::<f32>::open(&path).unwrap();
    let mut coords = psf.coordinates().clone();
    let CoordinateModel::Direction(direction) = coords.coordinate(0) else {
        panic!("the PSF has a direction coordinate");
    };
    let reference_pixel = direction.reference_pixel();
    let shifted = direction
        .clone()
        .with_reference_pixel([reference_pixel[0] + 1.0, reference_pixel[1]]);
    coords.replace_coordinate(0, shifted);
    psf.set_coordinates(coords).unwrap();
    psf.save().unwrap();
    drop(psf);
    let Err(reason) = observation.try_image("saved-mosaic-psf", controls) else {
        panic!("an offset saved PSF still requires the original reference pixel");
    };
    assert!(reason.contains("coordinate system mismatch"), "{reason}");
}

#[test]
fn reforming_an_existing_mosaic_psf_keeps_its_header_until_a_new_centre_is_requested() {
    let (observation, mut controls, alternate) = setup(false);
    let (_, ordinary) = observation.image("main-centre-mosaic", controls.clone());
    controls["psfphasecenter"] = json!(alternate);
    let (_, saved) = observation.image("reformed-mosaic", controls.clone());
    let previous = saved.get(".psf");
    controls.as_object_mut().unwrap().remove("psfphasecenter");
    let (_, reformed) = observation.image("reformed-mosaic", controls.clone());
    let psf = reformed.get(".psf");
    assert!(nrms(psf, ordinary.get(".psf")) < 1e-5);
    assert_eq!(
        psf.image.coordinates().coordinate(0).reference_value(),
        previous.image.coordinates().coordinate(0).reference_value(),
    );
    assert_eq!(psf.restoring_beam(), previous.restoring_beam());

    let reference = ordinary
        .get(".psf")
        .image
        .coordinates()
        .coordinate(0)
        .reference_value();
    controls["psfphasecenter"] = json!(format!("J2000 {}rad {}rad", reference[0], reference[1]));
    let (_, replaced) = observation.image("reformed-mosaic", controls);
    let replaced = replaced.get(".psf");
    assert_same(replaced, ordinary.get(".psf"), ".psf");
    let beam = replaced.restoring_beam();
    assert!((beam.major / ordinary.get(".psf").restoring_beam().major - 1.0).abs() < 1e-5);
    assert_ne!(beam, previous.restoring_beam());
}

#[test]
fn reforming_a_mosaic_cube_psf_uses_the_requested_geometry() {
    let (observation, mut controls, alternate) = setup(true);
    let (_, ordinary) = observation.image("main-centre-cube", controls.clone());
    controls["psfphasecenter"] = json!(alternate);
    let (_, seeded) = observation.image("reformed-cube", controls.clone());
    assert!(nrms(seeded.get(".psf"), ordinary.get(".psf")) > 1e-3);
    controls.as_object_mut().unwrap().remove("psfphasecenter");
    let (_, reformed) = observation.image("reformed-cube", controls);
    for suffix in [".psf", ".residual", ".pb", ".weight"] {
        assert_same(reformed.get(suffix), ordinary.get(suffix), suffix);
    }
    for channel in 0..2 {
        assert_eq!(
            reformed.get(".psf").channel_beam(channel),
            ordinary.get(".psf").channel_beam(channel),
        );
    }
    assert_eq!(
        reformed.get(".sumwt").channel_values(),
        ordinary.get(".sumwt").channel_values(),
    );
}

#[test]
fn a_saved_mosaic_taylor_psf_family_reuses_and_reforms_one_grid() {
    let (observation, mut controls, alternate) = taylor_setup();
    let (_, ordinary) = observation.image("ordinary-taylor", controls.clone());
    controls["psfphasecenter"] = json!(alternate);
    let (_, seeded) = observation.image("reuse-taylor", controls.clone());
    observation.image("reform-taylor", controls.clone());
    controls.as_object_mut().unwrap().remove("psfphasecenter");
    controls["calcpsf"] = json!(false);
    let (_, reused) = observation.image("reuse-taylor", controls.clone());
    for suffix in [
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".pb.tt0",
        ".weight.tt0",
        ".weight.tt1",
        ".weight.tt2",
        ".sumwt.tt0",
        ".sumwt.tt1",
        ".sumwt.tt2",
    ] {
        assert!(!reused.suffixes().contains(suffix), "rewrote {suffix}");
        let after = Product::open(&observation.scratch(&format!("reuse-taylor{suffix}")));
        assert_eq!(after.pixels, seeded.get(suffix).pixels, "{suffix}");
    }
    for suffix in [".image.tt0", ".image.tt1", ".residual.tt0", ".residual.tt1"] {
        assert_same(reused.get(suffix), seeded.get(suffix), suffix);
    }

    let path = observation.scratch("reuse-taylor.psf.tt1");
    let mut altered = PagedImage::<f32>::open(&path).unwrap();
    altered
        .set_coordinates(ordinary.get(".psf.tt1").image.coordinates().clone())
        .unwrap();
    altered.save().unwrap();
    drop(altered);
    let Err(reason) = observation.try_image("reuse-taylor", controls.clone()) else {
        panic!("the saved PSF moments must share one direction");
    };
    assert!(reason.contains("coordinate system mismatch"), "{reason}");

    controls["calcpsf"] = json!(true);
    let (_, reformed) = observation.image("reform-taylor", controls.clone());
    for term in 0..3 {
        let suffix = format!(".psf.tt{term}");
        let psf = reformed.get(&suffix);
        assert!(nrms(psf, ordinary.get(&suffix)) < 1e-5, "{suffix}");
        assert_eq!(
            psf.image.coordinates().coordinate(0).reference_value(),
            seeded
                .get(&suffix)
                .image
                .coordinates()
                .coordinate(0)
                .reference_value(),
        );
    }
    assert_eq!(
        reformed.get(".psf.tt0").restoring_beam(),
        seeded.get(".psf.tt0").restoring_beam(),
    );

    let reference = ordinary
        .get(".psf.tt0")
        .image
        .coordinates()
        .coordinate(0)
        .reference_value();
    controls["psfphasecenter"] = json!(format!("J2000 {}rad {}rad", reference[0], reference[1]));
    let (_, replaced) = observation.image("reform-taylor", controls);
    for term in 0..3 {
        let suffix = format!(".psf.tt{term}");
        assert_same(replaced.get(&suffix), ordinary.get(&suffix), &suffix);
    }
}
