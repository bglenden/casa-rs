// SPDX-License-Identifier: LGPL-3.0-or-later
//! A valid PB image for the total-power allocation law. Wider cells keep
//! convolution work small while retaining the large cube and sample arrays.
use super::*;

pub(super) fn write_model(path: &Path, planes: usize) {
    let pixels_across = 256;
    let shift = (pixels_across - PIXELS) / 2;
    let mut coordinates = ImageCoordinates::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE_RAD,
        [-8e-6, 8e-6],
        [(pixels_across / 2) as f64; 2],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        if planes == 1 {
            FREQUENCY_HZ + (SINGLE_DISH_CHANNELS - 1) as f64 / 2. * CHANNEL_HZ
        } else {
            FREQUENCY_HZ
        },
        if planes == 1 {
            SINGLE_DISH_CHANNELS as f64 * CHANNEL_HZ
        } else {
            CHANNEL_HZ
        },
        0.0,
        FREQUENCY_HZ,
    ));
    let shape = vec![pixels_across, pixels_across, 1, planes];
    let mut image =
        PagedImage::<f32>::create(shape.clone(), coordinates, path).expect("create model");
    let mut pixels = ArrayD::zeros(IxDyn(&shape));
    for plane in 0..planes {
        for (x, y, flux) in [(32, 32, 1.0_f32), (41, 28, 0.5), (12, 50, 0.25)] {
            pixels[IxDyn(&[x + shift, y + shift, 0, plane])] = flux;
        }
    }
    image.put_slice(&pixels, &[0, 0, 0, 0]).expect("pixels");
    image.set_units("Jy/pixel").expect("units");
    image.save().expect("save model");
}
