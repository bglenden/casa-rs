// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image-plane beams that mix the sky's Stokes planes before gridding.
use super::{BeamPattern, PredictError, SkyModel, jones::JonesImage};
use casa_imaging_runtime::Admitted;
use ndarray::Array2;
use std::sync::Arc;

pub(crate) enum ImageBeam {
    Jones(Arc<JonesImage>),
    Squinted {
        pattern: Arc<Admitted<BeamPattern>>,
        offset: [f64; 2],
        reference_hz: f64,
    },
}

impl ImageBeam {
    pub(crate) fn apply_plane(
        &self,
        model: &SkyModel,
        plane: usize,
        images: Vec<Array2<f32>>,
        pointing: [f64; 2],
        angle: f64,
    ) -> Result<Vec<Array2<f32>>, PredictError> {
        match self {
            Self::Jones(image) => image.apply_plane(model, plane, images, pointing, angle),
            Self::Squinted {
                pattern,
                offset,
                reference_hz,
            } => {
                let frequency = model.plane_frequency_hz(plane);
                let [right, left] = [1., -1.].map(|sign| {
                    // BeamSquint::getPointingDirection quantizes the
                    // frequency scale before rotating the saved AZEL offset.
                    let scale = f64::from((-reference_hz / frequency) as f32) * sign;
                    let [sx, sy] = offset.map(|v| v * scale);
                    let (sin, cos) = (-angle).sin_cos();
                    let (dx, dy) = (-(sx * cos + sy * sin), -sx * sin + sy * cos);
                    let [ra, dec] = pointing;
                    let direction = [
                        ra + dx.sin().atan2(dx.cos() * (dec + dy).cos()),
                        (dx.cos() * (dec + dy).sin()).asin(),
                    ];
                    model.pixel_of(direction)
                });
                let centres = [right?, left?, model.pixel_of(pointing)?];
                Ok(radial_plane(model, plane, images, &pattern.value, centres))
            }
        }
    }
}

fn radial_plane(
    model: &SkyModel,
    plane: usize,
    images: Vec<Array2<f32>>,
    pattern: &BeamPattern,
    centres: [[f64; 2]; 3],
) -> Vec<Array2<f32>> {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
    let frequency = model.plane_frequency_hz(plane);
    let at = pattern.at_frequency(frequency);
    let increments = model.increment_rad.map(f64::to_degrees);
    let [nx, ny] = model.shape();
    let mut out: Vec<_> = (0..4).map(|_| Array2::zeros((ny, nx))).collect();
    for y in 0..ny {
        for x in 0..nx {
            let powers = centres.map(|centre| {
                let rx = ((x as f64 - centre[0]) * increments[0]).powi(2) as f32;
                let ry = ((y as f64 - centre[1]) * increments[1]).powi(2) as f32;
                let radius = (f64::from((rx + ry).sqrt()) * 60. * frequency / 1e9) as f32;
                at.power(f64::from(radius))
            });
            let mut stokes = [0.; 4];
            for (pol, image) in model.polarizations().iter().zip(&images) {
                let slot = match pol {
                    StokesI => 0,
                    StokesQ => 1,
                    StokesU => 2,
                    StokesV => 3,
                    _ => unreachable!("sky models contain Stokes planes"),
                };
                stokes[slot] = image[[y, x]];
            }
            let [i, q, u, v] = stokes;
            let rr = (i + v) * powers[0];
            let ll = (i - v) * powers[1];
            // PBMath1D applies the central beam to RL/LR rather than
            // the product of displaced voltage patterns.
            for (output, value) in
                out.iter_mut()
                    .zip([(rr + ll) / 2., q * powers[2], u * powers[2], (rr - ll) / 2.])
            {
                output[[y, x]] = value;
            }
        }
    }
    out
}
