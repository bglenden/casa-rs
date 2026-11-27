// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateSystem, DirectionCoordinate, Projection, ProjectionType};
use casa_core::measures::direction::DirectionRef;
use casa_imageanalysis::linearmosaic;
use casa_images::Image;
use ndarray::{IxDyn, ShapeBuilder};

fn write(path: &Path, nx: usize, centre: f64, value: impl Fn(usize) -> f32) {
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.4, 0.5],
        [-1e-3, 1e-3],
        [centre, 2.0],
    ));
    let shape = [nx, 5];
    let pixels = ndarray::ArrayD::from_shape_fn(IxDyn(&shape).f(), |i| value(i[0]));
    let mut image = Image::create(shape.to_vec(), coords, path).unwrap();
    image.put_slice(&pixels, &[0, 0]).unwrap();
    image.save().unwrap();
}

#[test]
fn shifted_pointings_use_beam_squared_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let path = |name| dir.path().join(name);
    let pairs = [
        (path("first.image"), path("first.pb")),
        (path("second.image"), path("second.pb")),
    ];
    write(&pairs[0].0, 8, 5.0, |_| 1.5);
    write(&pairs[0].1, 8, 5.0, |_| 0.5);
    write(&pairs[1].0, 8, 2.0, |x| if x < 6 { 1.75 } else { 7e-4 });
    write(&pairs[1].1, 8, 2.0, |x| if x < 6 { 0.25 } else { 1e-4 });
    let template = path("template");
    let output = path("mosaic.image");
    let weights = path("mosaic.weight");
    write(&template, 15, 6.0, |_| 999.0);
    linearmosaic(&pairs, &template, &output, &weights).unwrap();
    let output = Image::open(output).unwrap().get().unwrap();
    let weights = Image::open(weights).unwrap().get().unwrap();

    // For one pointing the apparent sky is divided by its beam. In the overlap
    // the two corrected estimates (3 and 7) receive weights 0.25 and 0.0625.
    for (x, flux, weight) in [
        (2, 3.0, 0.25),
        (6, 3.8, 0.3125),
        (9, 7.0, 0.0625),
        (10, 0.0, 1e-8),
        (13, 0.0, 0.0),
    ] {
        assert!((output[[x, 2]] - flux).abs() < 1e-6, "flux at {x}");
        assert!((weights[[x, 2]] - weight).abs() < 1e-9, "weight at {x}");
    }
}

#[test]
fn interpolate_the_weighted_pixels_and_squared_beam() {
    let dir = tempfile::tempdir().unwrap();
    let path = |name| dir.path().join(name);
    let pairs = [(path("pointing"), path("pb"))];
    let template = path("template");
    let output = path("mosaic.image");
    let weight = path("mosaic.weight");
    write(&pairs[0].0, 6, 2.5, |x| if x < 3 { 1.0 } else { 3.0 });
    write(&pairs[0].1, 6, 2.5, |x| if x < 3 { 0.5 } else { 1.0 });
    write(&template, 6, 2.0, |_| 0.0);
    linearmosaic(&pairs, &template, &output, &weight).unwrap();
    let actual = Image::open(output).unwrap().get().unwrap();
    let coverage = Image::open(weight).unwrap().get().unwrap();

    // The midpoint interpolates (I*PB) and (PB²) before they are divided.
    assert!((actual[[2, 2]] - 2.8).abs() < 1e-6);
    assert!((coverage[[2, 2]] - 0.625).abs() < 1e-6);
}

#[test]
fn footprint_excludes_pixels_beyond_the_rounded_input_box() {
    let dir = tempfile::tempdir().unwrap();
    let path = |name| dir.path().join(name);
    let pairs = [(path("pointing"), path("pb"))];
    let template = path("template");
    let output = path("mosaic.image");
    let weight = path("mosaic.weight");
    write(&pairs[0].0, 8, 2.25, |_| 1.5);
    write(&pairs[0].1, 8, 2.25, |_| 0.5);
    write(&template, 12, 4.0, |_| 0.0);
    linearmosaic(&pairs, &template, &output, &weight).unwrap();
    let coverage = Image::open(weight).unwrap().get().unwrap();

    // The source's left edge lands at output x=1.75. Ordinary linear regrid
    // extrapolates to x=1, but CASA's linear mosaic clips to the rounded box.
    assert_eq!(coverage[[1, 2]], 0.0);
    assert_eq!(coverage[[2, 2]], 0.25);
}

#[test]
fn a_masked_image_or_beam_pixel_contributes_neither_flux_nor_weight() {
    let dir = tempfile::tempdir().unwrap();
    let path = |name| dir.path().join(name);
    let pairs = [(path("pointing"), path("pb"))];
    let template = path("template");
    let output = path("mosaic.image");
    let weight = path("mosaic.weight");
    write(&pairs[0].0, 6, 3.0, |_| 1.5);
    write(&pairs[0].1, 6, 3.0, |_| 0.5);
    write(&template, 10, 5.0, |_| 0.0);
    for (name, x) in [(&pairs[0].0, 1), (&pairs[0].1, 2)] {
        let mut input = Image::open(name).unwrap();
        let mut mask = ndarray::ArrayD::from_elem(IxDyn(input.shape()), true);
        mask[[x, 2]] = false;
        input.make_mask("valid", true, true).unwrap();
        input.put_mask("valid", &mask).unwrap();
        input.save().unwrap();
    }
    linearmosaic(&pairs, &template, &output, &weight).unwrap();
    let image = Image::open(output).unwrap().get().unwrap();
    let coverage = Image::open(weight).unwrap().get().unwrap();
    for x in [3, 4] {
        assert_eq!(image[[x, 2]], 0.0);
        assert_eq!(coverage[[x, 2]], 0.0);
    }
    assert_eq!(image[[2, 2]], 3.0);
    assert_eq!(coverage[[2, 2]], 0.25);
    assert_eq!(image[[5, 2]], 3.0);
    assert_eq!(coverage[[5, 2]], 0.25);
}

#[test]
fn outputs_must_be_new_and_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let path = |name| dir.path().join(name);
    let template = path("template");
    write(&template, 5, 2.0, |_| 1.0);
    let pairs = [(path("pointing"), path("pb"))];
    let unused = path("unused");
    let error = linearmosaic(&pairs, &template, &template, &unused).unwrap_err();
    assert!(error.to_string().contains("output already exists"));
    assert!(!unused.exists());
    let alias = dir.path().join("./unused");
    let error = linearmosaic(&pairs, &template, &unused, &alias).unwrap_err();
    assert!(error.to_string().contains("must be distinct"));
    assert!(!unused.exists());
}
