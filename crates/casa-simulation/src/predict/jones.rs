// SPDX-License-Identifier: LGPL-3.0-or-later
//! PBMath2DImage's sampled voltage matrices for simulation.
use casa_components::{Polarisation, stokes_in};
use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_imaging_runtime::Admitted;
use num_complex::{Complex32, Complex64};

pub(crate) struct ImagePlane {
    pub(crate) shape: [usize; 2],
    pub(crate) pixels: Vec<[Complex32; 4]>,
    pub(crate) direction: DirectionCoordinate,
    pub(crate) frequency_hz: f64,
    pub(crate) basis: Polarisation,
}

pub(crate) struct JonesImage {
    pub(crate) real: Admitted<ImagePlane>,
    pub(crate) imaginary: Option<Admitted<ImagePlane>>,
}

impl std::fmt::Debug for JonesImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JonesImage")
            .field("shape", &self.real.value.shape)
            .field("frequency_hz", &self.real.value.frequency_hz)
            .finish_non_exhaustive()
    }
}

impl ImagePlane {
    fn linear(&self, pixel: [f64; 2]) -> [Complex32; 4] {
        let [x, y] = pixel;
        if !x.is_finite()
            || !y.is_finite()
            || x < 0.
            || y < 0.
            || x > (self.shape[0] - 1) as f64
            || y > (self.shape[1] - 1) as f64
        {
            return [Complex32::new(0., 0.); 4];
        }
        let (ix, iy) = (x.floor() as usize, y.floor() as usize);
        let (dx, dy) = ((x - ix as f64) as f32, (y - iy as f64) as f32);
        std::array::from_fn(|pol| {
            let at = |x: usize, y: usize| {
                self.pixels[y.min(self.shape[1] - 1) * self.shape[0] + x.min(self.shape[0] - 1)]
                    [pol]
            };
            (at(ix, iy) * (1. - dx) + at(ix + 1, iy) * dx) * (1. - dy)
                + (at(ix, iy + 1) * (1. - dx) + at(ix + 1, iy + 1) * dx) * dy
        })
    }

    fn image_coordinate(
        &self,
        model: &super::SkyModel,
        pointing: [f64; 2],
        angle: f64,
        frequency: f64,
    ) -> Result<DirectionCoordinate, super::PredictError> {
        let source = model.direction_coordinate();
        let increment = self.direction.increment();
        let pixel = self.direction.reference_pixel();
        let scale = self.frequency_hz / frequency;
        let pa = (-angle) as f32;
        let (sin, cos) = (f64::from(pa.sin()), f64::from(pa.cos()));
        let increments = [-increment[0] * scale, increment[1] * scale];
        Ok(DirectionCoordinate::new(
            source.direction_ref(),
            source.projection().clone(),
            model.native_pointing(pointing)?,
            increments,
            [pixel[0], pixel[1]],
        )
        // CASA's setLinearTransform rotates the intermediate world axes.
        // with_pc_matrix precedes pixel increments, so use D^-1 R D.
        .with_pc_matrix(ndarray::arr2(&[
            [cos, -sin * increments[1] / increments[0]],
            [sin * increments[0] / increments[1], cos],
        ])))
    }

    fn nearest(&self, pixel: [f64; 2]) -> [Complex32; 4] {
        // C++ Int truncation, including coordinates slightly below zero.
        let [x, y] = pixel.map(|v| (v + 0.5) as isize);
        if x < 0 || y < 0 || x >= self.shape[0] as isize || y >= self.shape[1] as isize {
            return [Complex32::new(0., 0.); 4];
        }
        self.pixels[y as usize * self.shape[0] + x as usize]
    }
}

impl JonesImage {
    /// Regrid voltage samples linearly, then apply the Jones product to all
    /// Stokes planes before gridding, as PBMath2DImage does.
    pub(crate) fn apply_plane(
        &self,
        model: &super::SkyModel,
        plane: usize,
        images: Vec<ndarray::Array2<f32>>,
        pointing: [f64; 2],
        angle: f64,
    ) -> Result<Vec<ndarray::Array2<f32>>, super::PredictError> {
        let frequency = model.plane_frequency_hz(plane);
        let real = self
            .real
            .value
            .image_coordinate(model, pointing, angle, frequency)?;
        let imaginary = self
            .imaginary
            .as_ref()
            .map(|v| v.value.image_coordinate(model, pointing, angle, frequency))
            .transpose()?;
        let [nx, ny] = model.shape();
        let mut output: Vec<_> = (0..4).map(|_| ndarray::Array2::zeros((ny, nx))).collect();
        let slots = model
            .polarizations()
            .iter()
            .map(|pol| match pol {
                casa_imaging_model::PolarizationCoordinate::StokesI => 0,
                casa_imaging_model::PolarizationCoordinate::StokesQ => 1,
                casa_imaging_model::PolarizationCoordinate::StokesU => 2,
                casa_imaging_model::PolarizationCoordinate::StokesV => 3,
                _ => unreachable!("sky models have Stokes planes"),
            })
            .collect::<Vec<_>>();
        for y in 0..ny {
            for x in 0..nx {
                let world = model
                    .direction_coordinate()
                    .to_world(&[x as f64, y as f64])?;
                let sample = |part: &ImagePlane, coordinate: &DirectionCoordinate| {
                    coordinate
                        .to_pixel(&world)
                        .ok()
                        .map(|v| part.linear([v[0], v[1]]))
                        .unwrap_or([Complex32::new(0., 0.); 4])
                };
                let mut voltage = sample(&self.real.value, &real);
                if let (Some(part), Some(coordinate)) = (&self.imaginary, &imaginary) {
                    for (value, imag) in voltage.iter_mut().zip(sample(&part.value, coordinate)) {
                        value.im = imag.re;
                    }
                }
                let mut stokes = [0.; 4];
                for (image, &slot) in images.iter().zip(&slots) {
                    stokes[slot] = f64::from(image[[y, x]]);
                }
                for (image, value) in output.iter_mut().zip(self.apply_stokes(voltage, stokes)) {
                    image[[y, x]] = value as f32;
                }
            }
        }
        Ok(output)
    }

    /// PBMath2DImage::apply(SkyComponent): rotate the angular offset into
    /// the feed frame, scale to the beam image's frequency and pick a pixel.
    pub(crate) fn component_flux(
        &self,
        circular: [Complex64; 4],
        direction: [f64; 2],
        pointing: [f64; 2],
        angle: f64,
        frequency_hz: f64,
        output: Polarisation,
    ) -> [Complex64; 4] {
        let [ra, dec] = direction;
        let [pra, pdec] = pointing;
        let delta = ra - pra;
        let dot = pdec.sin() * dec.sin() + pdec.cos() * dec.cos() * delta.cos();
        let separation = dot.clamp(-1., 1.).acos() * frequency_hz / self.real.value.frequency_hz;
        let position_angle = (dec.cos() * delta.sin())
            .atan2(pdec.cos() * dec.sin() - pdec.sin() * dec.cos() * delta.cos())
            - angle;
        let world = [
            -separation * position_angle.sin(),
            separation * position_angle.cos(),
        ];
        let Some(pixel) = self
            .real
            .value
            .direction
            .to_pixel(&world)
            .ok()
            .map(|v| [v[0], v[1]])
        else {
            return [Complex64::new(0., 0.); 4];
        };
        let mut voltage = self.real.value.nearest(pixel);
        if let Some(imaginary) = &self.imaginary {
            // CASA computes imPix but samples both parts at rePix.
            let imaginary = imaginary.value.nearest(pixel);
            for (value, imag) in voltage.iter_mut().zip(imaginary) {
                value.im = imag.re;
            }
        }
        let [rr, rl, lr, ll] = circular;
        let stokes = [
            (rr.re + ll.re) / 2.,
            (rl.re + lr.re) / 2.,
            (rl.im - lr.im) / 2.,
            (rr.re - ll.re) / 2.,
        ];
        stokes_in(output, self.apply_stokes(voltage, stokes))
    }

    pub(crate) fn apply_stokes(&self, voltage: [Complex32; 4], stokes: [f64; 4]) -> [f64; 4] {
        let flux = stokes_in(self.real.value.basis, stokes);
        // CStokesVector reverses the correlations; the image Jones entries
        // therefore act in this order in the ordinary coherency convention.
        let e = [voltage[0], voltage[2], voltage[1], voltage[3]]
            .map(|v| Complex64::new(f64::from(v.re), f64::from(v.im)));
        let out: [Complex64; 4] = std::array::from_fn(|cell| {
            let (row, column) = (cell / 2, cell % 2);
            let mut value = Complex64::new(0., 0.);
            for left in 0..2 {
                for right in 0..2 {
                    value +=
                        e[row * 2 + left] * flux[left * 2 + right] * e[column * 2 + right].conj();
                }
            }
            value
        });
        let [a, b, c, d] = out;
        match self.real.value.basis {
            Polarisation::Circular => [
                (a.re + d.re) / 2.,
                (b.re + c.re) / 2.,
                (b.im - c.im) / 2.,
                (a.re - d.re) / 2.,
            ],
            Polarisation::Linear => [
                (a.re + d.re) / 2.,
                (a.re - d.re) / 2.,
                (b.re + c.re) / 2.,
                (b.im - c.im) / 2.,
            ],
            Polarisation::Stokes => unreachable!("Jones images contain correlation coordinates"),
        }
    }
}
