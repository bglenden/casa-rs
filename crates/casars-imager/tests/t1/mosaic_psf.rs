// SPDX-License-Identifier: LGPL-3.0-or-later

//! The separately centred PSF of a single-term MFS mosaic (#1062).

use casa_coordinates::{Coordinate, CoordinateModel};
use casa_images::PagedImage;
use serde_json::json;

use super::fixture::{Observation, Product, Setup};
use super::mosaic::mosaic_run;

fn setup() -> (Observation, serde_json::Value, String) {
    let (observation, mut controls) = mosaic_run(false);
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
    (error / power).sqrt()
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
    let (observation, mut controls, alternate) = setup();
    let (_, ordinary) = observation.image("ordinary-mosaic", controls.clone());
    let mut shifted_controls = controls.clone();
    shifted_controls["phasecenter"] = json!(alternate);
    let (_, shifted) = observation.image("shifted-mosaic", shifted_controls);
    controls["psfphasecenter"] = json!(alternate);
    let (_, separate) = observation.image("separate-mosaic-psf", controls);

    // The shifted chart excludes the last pointing. Its PSF determines
    // the fitted beam, while the original mosaic supplies all other normals.
    let sums = ordinary.get(".sumwt").channel_values();
    assert!(shifted.get(".sumwt").channel_values()[0] < sums[0]);
    assert_eq!(separate.get(".sumwt").channel_values(), sums);
    assert_same(separate.get(".psf"), shifted.get(".psf"), ".psf");
    assert!(nrms(separate.get(".psf"), ordinary.get(".psf")) > 1e-3);
    for suffix in [".residual", ".pb", ".weight"] {
        assert_same(separate.get(suffix), ordinary.get(suffix), suffix);
    }
    let psf = separate.get(".psf").restoring_beam();
    assert_eq!(separate.get(".image").restoring_beam(), psf);
    let expected = shifted.get(".psf").restoring_beam();
    assert!((psf.major / expected.major - 1.0).abs() < 1e-5);
    assert!((psf.minor / expected.minor - 1.0).abs() < 1e-5);
}

#[test]
fn alternate_mosaic_psf_can_be_reused_and_checks_the_saved_pixel_grid() {
    let (observation, mut controls, alternate) = setup();
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
    let (observation, mut controls, alternate) = setup();
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
