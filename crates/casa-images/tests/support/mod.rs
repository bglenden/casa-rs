// SPDX-License-Identifier: LGPL-3.0-or-later

//! What the image task binaries' process tests share.

use std::path::Path;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

/// A 4x4 direction image with one channel per value of `spectrum`, the
/// same spectrum at every direction pixel, in Jy/beam.
pub fn write_spectrum_cube(path: &Path, spectrum: &[f32]) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [1.5, 1.5],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.415e9,
        1.0e3,
        0.0,
        1.415e9,
    ));
    let shape = vec![4, 4, spectrum.len()];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(&shape), |index| spectrum[index[2]]);
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.set_units("Jy/beam").unwrap();
    image.save().unwrap();
}

/// A 4x4x3 direction-and-frequency image with a beam, whose pixels are
/// `1..=48` times `scale`.
pub fn write_image(path: &Path, scale: f32) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [1.5, 1.5],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        372.0e9,
        2.0e6,
        0.0,
        372.0e9,
    ));
    let shape = vec![4, 4, 3];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (1..=shape.iter().product::<usize>())
            .map(|value| value as f32 * scale)
            .collect(),
    )
    .unwrap();
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.set_units("Jy/beam").unwrap();
    image
        .set_image_info(&ImageInfo {
            beam_set: ImageBeamSet::new(GaussianBeam::new(
                1.7e-4_f64.to_radians(),
                1.2e-4_f64.to_radians(),
                (-34.5_f64).to_radians(),
            )),
            image_type: ImageType::Intensity,
            object_name: "inventory".to_string(),
        })
        .unwrap();
    image.save().unwrap();
}
