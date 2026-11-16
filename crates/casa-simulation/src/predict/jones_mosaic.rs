// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample a saved Jones image on SimplePBConvFunc's common-beam screen.
use super::jones::{ImagePlane, JonesImage};
use casa_coordinates::Coordinate;
use num_complex::Complex32;

impl JonesImage {
    pub(crate) fn fill_mosaic_screen(
        &self,
        values: &mut [Complex32],
        side: usize,
        increments_deg: [f64; 2],
        frequency_hz: f64,
        angle: f64,
        weight: bool,
    ) {
        let pa = (-angle) as f32;
        let (sin, cos) = (f64::from(pa.sin()), f64::from(pa.cos()));
        let increments = increments_deg.map(f64::to_radians);
        // The screen and the synthetic Jones coordinate share their
        // projection, pointing and poles. Their pixel mapping is exactly
        // affine: undo the intermediate-world rotation, then increments.
        let mapping = |image: &ImagePlane| {
            let scale = image.frequency_hz / frequency_hz;
            let step = image.direction.increment();
            let pixel = image.direction.reference_pixel();
            let (dx, dy) = (-step[0] * scale, step[1] * scale);
            (
                [pixel[0], pixel[1]],
                [cos * increments[0] / dx, -sin * increments[0] / dy],
                [sin * increments[1] / dx, cos * increments[1] / dy],
            )
        };
        let real = mapping(&self.real.value);
        let imaginary = self.imaginary.as_ref().map(|image| mapping(&image.value));
        let centre = side as f64 / 2.;
        for (index, value) in values.iter_mut().enumerate() {
            let offset = [
                (index % side) as f64 - centre,
                (index / side) as f64 - centre,
            ];
            let sample = |image: &ImagePlane, (origin, x, y): ([f64; 2], [f64; 2], [f64; 2])| {
                image.linear(std::array::from_fn(|axis| {
                    origin[axis] + offset[0] * x[axis] + offset[1] * y[axis]
                }))
            };
            let mut voltage = sample(&self.real.value, real);
            if let (Some(image), Some(mapping)) = (&self.imaginary, imaginary) {
                for (value, imag) in voltage.iter_mut().zip(sample(&image.value, mapping)) {
                    value.im = imag.re;
                }
            }
            // CASA deliberately constructs a one-correlation common-beam
            // convolution. Its complex path keeps the reversed first
            // correlation; its Float path transforms the noise covariance.
            let power = if weight {
                0.5 * voltage.iter().map(|v| v.norm_sqr().powi(2)).sum::<f32>()
            } else {
                voltage[3].norm_sqr()
            };
            *value = Complex32::new(power, 0.);
        }
    }
}
