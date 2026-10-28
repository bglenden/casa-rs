// SPDX-License-Identifier: LGPL-3.0-or-later
//! A start model's Stokes and spectral axes against its image's (#841
//! Phase 6b, #970 review finding 1). The model keeps only the regridded
//! pixels, so an axis `regrid` would let through by keeping the input's
//! coordinate (one pixel in both images) is checked by the start model
//! itself:
//! - its Stokes are the image's, plane for plane, a single plane included;
//! - an MFS image's start model has one spectral plane, at any frequency,
//!   as CASA's regrid replicates a degenerate spectral axis;
//! - a cube's start model has the cube's spectral axis.

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::{PagedImage, RegridTemplate};
use casa_imaging_application::start_model_law::{main_samples, seeding_bytes};
use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const ARCSEC: f64 = std::f64::consts::PI / (180.0 * 3600.0);
const SIDE: usize = 16;

/// A SIN image of `SIDE`² cells of 2″ with `stokes` and `channels` planes
/// from `frequency_hz` in 1 MHz steps.
fn coordinates(stokes: &[StokesType], frequency_hz: f64) -> CoordinateSystem {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-2.0 * ARCSEC, 2.0 * ARCSEC],
        [(SIDE / 2) as f64; 2],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(stokes.to_vec()));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        frequency_hz,
        1.0e6,
        0.0,
        frequency_hz,
    ));
    coordinates
}

/// A Jy/pixel start model at `dir` whose pixel at (x, y, stokes, channel)
/// holds a value distinct to each plane.
fn start_model(
    dir: &std::path::Path,
    stokes: &[StokesType],
    channels: usize,
    frequency_hz: f64,
) -> std::path::PathBuf {
    let path = dir.join(format!(
        "start-{}-{channels}-{frequency_hz}.model",
        stokes.len()
    ));
    let shape = [SIDE, SIDE, stokes.len(), channels];
    let mut image =
        PagedImage::<f32>::create(shape.to_vec(), coordinates(stokes, frequency_hz), &path)
            .expect("create the start model");
    image.set_units("Jy/pixel").expect("units");
    let pixels = ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| {
        (index[0] + 3 * index[1]) as f32 + 100.0 * index[2] as f32 + 1000.0 * index[3] as f32
    });
    image.put_slice(&pixels, &[0; 4]).expect("pixels");
    image.save().expect("save the start model");
    path
}

fn template(stokes: &[StokesType], channels: usize, frequency_hz: f64) -> RegridTemplate {
    RegridTemplate {
        coordinates: coordinates(stokes, frequency_hz),
        shape: vec![SIDE, SIDE, stokes.len(), channels],
    }
}

#[test]
fn a_single_stokes_plane_must_be_the_images_stokes() {
    let dir = tempfile::tempdir().expect("dir");
    let q = start_model(dir.path(), &[StokesType::Q], 1, 1.0e9);
    let refused = seeding_bytes(&q, template(&[StokesType::I], 1, 1.0e9))
        .expect_err("a Stokes Q start model of a Stokes I image");
    assert!(refused.contains("Stokes"), "{refused}");
    seeding_bytes(&q, template(&[StokesType::Q], 1, 1.0e9)).expect("Q onto Q");
}

#[test]
fn a_full_stokes_start_model_seeds_each_plane_of_its_own() {
    let dir = tempfile::tempdir().expect("dir");
    let iquv = [StokesType::I, StokesType::Q, StokesType::U, StokesType::V];
    let path = start_model(dir.path(), &iquv, 1, 1.0e9);
    let samples = main_samples(&path, template(&iquv, 1, 1.0e9)).expect("IQUV onto IQUV");
    // The canonical order: polarization, y, x, x fastest.
    for (plane, offset) in [0.0_f32, 100.0, 200.0, 300.0].into_iter().enumerate() {
        let first = samples[plane * SIDE * SIDE + SIDE + 2];
        assert_eq!(first, 2.0 + 3.0 + offset, "plane {plane}");
    }
    let swapped = [StokesType::I, StokesType::U, StokesType::Q, StokesType::V];
    seeding_bytes(&path, template(&swapped, 1, 1.0e9)).expect_err("IUQV is not IQUV");
}

#[test]
fn an_mfs_start_model_has_one_plane_at_any_frequency() {
    let dir = tempfile::tempdir().expect("dir");
    let path = start_model(dir.path(), &[StokesType::I], 1, 1.0e9);
    let samples =
        main_samples(&path, template(&[StokesType::I], 1, 1.2e9)).expect("another frequency");
    assert_eq!(samples[SIDE + 2], 5.0, "the plane is seeded as it is");
    let cube = start_model(dir.path(), &[StokesType::I], 4, 1.0e9);
    seeding_bytes(&cube, template(&[StokesType::I], 1, 1.0e9))
        .expect_err("four planes are not an MFS start model");
}

#[test]
fn a_cube_start_model_has_the_cubes_spectral_axis() {
    let dir = tempfile::tempdir().expect("dir");
    let cube = start_model(dir.path(), &[StokesType::I], 4, 1.0e9);
    let samples = main_samples(&cube, template(&[StokesType::I], 4, 1.0e9)).expect("the cube's");
    for channel in 0..4 {
        assert_eq!(
            samples[channel * SIDE * SIDE + SIDE + 2],
            5.0 + 1000.0 * channel as f32,
            "channel {channel}"
        );
    }
    seeding_bytes(&cube, template(&[StokesType::I], 4, 1.1e9)).expect_err("another spectral axis");
    let plane = start_model(dir.path(), &[StokesType::I], 1, 1.0e9);
    seeding_bytes(&plane, template(&[StokesType::I], 4, 1.0e9))
        .expect_err("one plane is not a cube's start model");
}
