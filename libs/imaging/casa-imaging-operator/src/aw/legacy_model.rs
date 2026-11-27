// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the circular Fourier model consumed by the legacy AW imager.

use ndarray::ArrayD;
use num_complex::Complex32;

use super::AwCatalog;
use crate::error::OperatorError;
use crate::fft::PlaneFft;

impl AwCatalog {
    /// Prepare the four single-channel circular Fourier planes from a legacy
    /// `AWProjectFT` IQUV image and its optional cached sensitivity image.
    ///
    /// # Errors
    /// The image, sensitivity image or PB cutoff is incompatible.
    pub fn prepare_legacy_model(
        &self,
        model: &ArrayD<f32>,
        average_beam: Option<&ArrayD<f32>>,
        cutoff: Option<f32>,
    ) -> Result<[Vec<Complex32>; 4], OperatorError> {
        let [nx, ny, 4, 1] = *model.shape() else {
            return Err(invalid(
                "legacy AW prediction needs a single-channel IQUV image",
            ));
        };
        if nx != ny || !nx.is_multiple_of(2) || nx == 0 {
            return Err(invalid(
                "legacy AW needs a square image with even pixel lengths",
            ));
        }
        if cutoff.is_some_and(|limit| !limit.is_finite() || limit < 0.)
            || (cutoff.is_some() && average_beam.is_none())
            || average_beam.is_some_and(|beam| {
                beam.shape() != model.shape()
                    || beam.iter().any(|pixel| !pixel.is_finite() || *pixel < 0.)
            })
        {
            return Err(invalid("invalid legacy AW average beam or cutoff"));
        }
        let sinc = (0..nx)
            .map(|pixel| {
                let x = std::f32::consts::PI * (pixel as isize - (nx / 2) as isize) as f32
                    / (nx as f32 * f32::from(self.oversampling));
                if x == 0. { 1. } else { x.sin() / x }
            })
            .collect::<Vec<_>>();
        let mut grids = std::array::from_fn(|_| Vec::with_capacity(nx * ny));
        for (y, &sy) in sinc.iter().enumerate() {
            for (x, &sx) in sinc.iter().enumerate() {
                let mut stokes = std::array::from_fn(|pol| model[[x, y, pol, 0]]);
                if let (Some(beam), Some(limit)) = (average_beam, cutoff) {
                    for (pol, value) in stokes.iter_mut().enumerate() {
                        let power = beam[[x, y, pol, 0]];
                        if power > limit {
                            *value /= power.sqrt();
                        }
                    }
                }
                let [i, q, u, v] = stokes;
                let circular = [
                    Complex32::from(i + v),
                    Complex32::new(q, u),
                    Complex32::new(q, -u),
                    Complex32::from(i - v),
                ];
                for (grid, value) in grids.iter_mut().zip(circular) {
                    let taper = sx * sy;
                    let value = match (average_beam, cutoff) {
                        (Some(_), Some(_)) => value / taper,
                        (Some(_), None) => value * taper,
                        (None, _) => value,
                    };
                    grid.push(value);
                }
            }
        }
        let mut fft = PlaneFft::new([nx, ny], false)?;
        for grid in &mut grids {
            fft.transform(grid, false)?;
        }
        Ok(grids)
    }
}

fn invalid(reason: &'static str) -> OperatorError {
    OperatorError::ConvolutionFunction { reason }
}
