// SPDX-License-Identifier: LGPL-3.0-or-later
//! Single W-plane convolution in the legacy EP solver's nPBWProjectFT.

use num_complex::Complex32;

use super::{EvlaApertureGrid, EvlaApertureModel};
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::weighting::SPEED_OF_LIGHT_M_PER_S;

const SCREEN: usize = 2048;
const SAMPLING: usize = 20;

/// The legacy one-plane EP convolution and its average primary beam.
pub struct LegacyEpConvolution {
    /// Fourier-cell oversampling.
    pub sampling: usize,
    /// The support radius in visibility-grid pixels.
    pub support: usize,
    /// The side of the stored convolution after cropping to support.
    pub kernel_side: usize,
    /// Imaging and weight planes in x-contiguous order. The legacy EP
    /// request uses the same Mueller element for each correlation.
    pub kernels: [Vec<Complex32>; 2],
    /// Unsquinted beam on the input image geometry, x-contiguous.
    pub average_beam: Vec<f32>,
}

impl LegacyEpConvolution {
    /// Form the nPBWProjectFT cells for the original VLA L-band feed.
    ///
    /// `frequency_hz` is the highest channel in the first visibility buffer.
    /// `angle_rad` is that buffer's feed angle for antenna zero.
    ///
    /// # Errors
    /// The input geometry is invalid, the aperture cannot be evaluated or
    /// the convolution has no finite positive integral.
    pub fn vla_l_band(
        aperture: &EvlaApertureModel,
        image_side: usize,
        cell_rad: f64,
        frequency_hz: f64,
        angle_rad: f64,
    ) -> Result<Self, OperatorError> {
        let frequency_hz = f64::from(frequency_hz as f32);
        let beam =
            |side, sky_cell| aperture_beam(aperture, side, sky_cell, frequency_hz, angle_rad);
        let mut average = beam(image_side, cell_rad)?;
        let average_beam = average
            .power(false)?
            .into_iter()
            .map(|value| value.re)
            .collect();
        drop(average);
        let mut far = beam(
            SCREEN,
            cell_rad * (SAMPLING * image_side) as f64 / SCREEN as f64,
        )?;
        let mut kernels = [far.power(true)?, far.power(false)?];
        drop(far);
        let mut fft = PlaneFft::<f32>::new([SCREEN, SCREEN], false)?;
        let origin = SCREEN / 2;
        for kernel in &mut kernels {
            fft.transform(kernel, false)?;
            let peak = kernel[origin * SCREEN + origin];
            if !peak.re.is_finite() || peak.re <= 0. {
                return Err(invalid("legacy EP convolution has no positive peak"));
            }
            for value in kernel.iter_mut() {
                *value /= peak;
            }
        }
        let support = kernel_support(&kernels[0])?;
        let radius = (support + 1) * SAMPLING;
        let kernel_side = radius * 2 + 1;
        let integral = sampled_integral(&kernels[0], support);
        if !integral.is_finite() || integral <= 0. {
            return Err(invalid("legacy EP convolution integral is not positive"));
        }
        let kernels = kernels.map(|kernel| crop(&kernel, radius));
        let [mut imaging, mut weights] = kernels;
        for value in &mut imaging {
            *value /= integral;
        }
        for value in &mut weights {
            *value /= integral * integral;
        }
        Ok(Self {
            sampling: SAMPLING,
            support,
            kernel_side,
            kernels: [imaging, weights],
            average_beam,
        })
    }

    /// Prepare the beam-corrected RR, RL, LR and LL Fourier planes from an
    /// IQUV sky image as the legacy EP solver's prediction grid.
    /// # Errors
    /// The image and average beam must have matching square IQUV planes.
    pub fn prepare_model(
        image: &ndarray::ArrayD<f32>,
        beam: &ndarray::ArrayD<f32>,
        sampling: usize,
    ) -> Result<[Vec<Complex32>; 4], OperatorError> {
        let [nx, ny, 4, 1] = *image.shape() else {
            return Err(invalid("legacy EP needs a single-channel IQUV image"));
        };
        if nx != ny || !nx.is_multiple_of(2) || sampling == 0 || image.shape() != beam.shape() {
            return Err(invalid("invalid model or beam shape in legacy EP"));
        }
        let mut fft = PlaneFft::new([nx, ny], false)?;
        let mut result = std::array::from_fn(|_| Vec::with_capacity(nx * ny));
        let sinc = (0..nx)
            .map(|pixel| {
                if pixel == nx / 2 {
                    1.
                } else {
                    let value = std::f32::consts::PI * (pixel as isize - (nx / 2) as isize) as f32
                        / (nx * sampling) as f32;
                    value.sin() / value
                }
            })
            .collect::<Vec<_>>();
        let peak = beam.iter().copied().fold(0_f32, f32::max);
        if !peak.is_finite() || peak < 1e-4 {
            return Err(invalid("invalid average beam in legacy EP"));
        }
        let normalize = (peak - 1.).abs() > 1e-3;
        let primary_beam = |x, y, pol| {
            if pol < 2 {
                0.5 * (beam[[x, y, 0, 0]] + beam[[x, y, 1, 0]])
            } else {
                beam[[x, y, pol, 0]]
            }
        };
        for (pol, grid) in result.iter_mut().enumerate() {
            let peak = if normalize {
                (0..ny)
                    .flat_map(|y| (0..nx).map(move |x| primary_beam(x, y, pol)))
                    .fold(0_f32, f32::max)
            } else {
                1.
            };
            for y in 0..ny {
                for x in 0..nx {
                    let [i, q, u, v] = std::array::from_fn(|index| image[[x, y, index, 0]]);
                    let circular = [
                        Complex32::from(i + v),
                        Complex32::new(q, u),
                        Complex32::new(q, -u),
                        Complex32::from(i - v),
                    ][pol];
                    let taper = sinc[x] * sinc[y];
                    let correction = primary_beam(x, y, pol) / peak * taper;
                    grid.push(if correction.abs() >= 0.05 {
                        circular / correction
                    } else {
                        circular * taper
                    });
                }
            }
            fft.transform(grid, false)?;
        }
        Ok(result)
    }
}

struct ApertureBeam {
    size: usize,
    jones: Vec<Complex32>,
    fft: PlaneFft<f32>,
}

fn aperture_beam(
    aperture: &EvlaApertureModel,
    size: usize,
    sky_cell: f64,
    frequency_hz: f64,
    angle_rad: f64,
) -> Result<ApertureBeam, OperatorError> {
    let cell_m = SPEED_OF_LIGHT_M_PER_S / (frequency_hz * size as f64 * sky_cell.abs());
    let grid = EvlaApertureGrid::new(size, cell_m, 3, frequency_hz, angle_rad)
        .map_err(|_| invalid("invalid legacy EP aperture geometry"))?;
    let mut jones = vec![Complex32::default(); grid.workspace_values()];
    aperture
        .fill_vla_l_aperture(grid, &mut jones)
        .map_err(|_| invalid("cannot evaluate the legacy VLA aperture"))?;
    let fft = PlaneFft::new([size, size], false)?;
    Ok(ApertureBeam { size, jones, fft })
}

impl ApertureBeam {
    fn power(&mut self, squint: bool) -> Result<Vec<Complex32>, OperatorError> {
        let plane_size = self.size * self.size;
        let center = self.size / 2;
        let index = center * self.size + center;
        let mut normalization = 0.;
        let mut rr = Vec::new();
        for (pol, source) in self.jones.chunks_exact(plane_size).enumerate() {
            let mut plane = source.to_vec();
            if !squint && pol == 0 {
                // The legacy loop removes the R phase in place starting at
                // R itself. Later planes see R with its phase already removed.
                for value in &mut plane {
                    *value = Complex32::new(value.norm(), 0.);
                }
            }
            self.fft.transform(&mut plane, false)?;
            normalization += 0.5 * plane[index].norm_sqr();
            if pol == 0 {
                rr = plane;
            }
        }
        if !normalization.is_finite() || normalization <= 0. {
            return Err(invalid("legacy EP beam normalization is zero"));
        }
        for value in &mut rr {
            *value = Complex32::new(value.norm_sqr() / normalization, 0.);
        }
        Ok(rr)
    }
}

fn kernel_support(kernel: &[Complex32]) -> Result<usize, OperatorError> {
    let origin = SCREEN as f64 * 0.5;
    for radius in (2..=SCREEN / 4).rev() {
        // The original repeatedly sweeps the same circle at angles 2πi/R.
        for step in 0..radius {
            let (sin, cos) = (std::f64::consts::TAU * step as f64 / radius as f64).sin_cos();
            let x = (origin + radius as f64 * sin) as usize;
            let y = (origin + radius as f64 * cos) as usize;
            if kernel[y * SCREEN + x].norm() > 1e-4 {
                return Ok((radius + SAMPLING / 2) / SAMPLING + 1);
            }
        }
    }
    Err(invalid("legacy EP convolution has no support"))
}

fn crop(kernel: &[Complex32], radius: usize) -> Vec<Complex32> {
    let start = SCREEN / 2 - radius;
    let end = SCREEN / 2 + radius;
    let mut cropped = Vec::with_capacity((2 * radius + 1).pow(2));
    for y in start..=end {
        cropped.extend_from_slice(&kernel[y * SCREEN + start..=y * SCREEN + end]);
    }
    cropped
}

fn sampled_integral(kernel: &[Complex32], support: usize) -> f32 {
    let origin = SCREEN / 2;
    let start = origin - support * SAMPLING;
    let end = origin + support * SAMPLING;
    let mut sum = 0.;
    for y in (start..end).step_by(SAMPLING) {
        for x in (start..end).step_by(SAMPLING) {
            sum += kernel[y * SCREEN + x].re;
        }
    }
    sum
}

fn invalid(reason: &'static str) -> OperatorError {
    OperatorError::ConvolutionFunction { reason }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use casa_images::PagedImage;
    use casa_imaging_model::EvlaDishSurface;
    use ndarray::Axis;

    use super::*;

    #[test]
    #[ignore = "requires the installed CASA VLA EP reference cache and VLA.surface"]
    fn legacy_ep_convolution_matches_casa_cache() {
        let root = PathBuf::from(std::env::var_os("CASARS_EP_REFERENCE_CACHE").unwrap());
        let surface =
            std::fs::read_to_string(std::env::var_os("CASARS_EP_VLA_SURFACE").unwrap()).unwrap();
        let surface = EvlaDishSurface::from_surface_text(&surface).unwrap();
        let aperture = EvlaApertureModel::new(surface);
        let cold = LegacyEpConvolution::vla_l_band(
            &aperture,
            256,
            15_f64.to_radians() / 3600.,
            1_414_178_745.643_775_5,
            74.460_9_f64.to_radians(),
        )
        .unwrap();
        assert_eq!(
            (cold.sampling, cold.support, cold.kernel_side),
            (20, 3, 161)
        );
        for (name, obtained) in ["CF0_0", "CFWT0_0"].into_iter().zip(cold.kernels) {
            let image = PagedImage::<Complex32>::open(root.join(name)).unwrap();
            let pixels = image.get().unwrap();
            let pixels = pixels.index_axis(Axis(2), 0);
            let expected = pixels.as_slice_memory_order().unwrap();
            let difference = nrms(&obtained, expected);
            eprintln!("{name} normalized RMS {difference:.9e}");
            assert!(difference < 1e-3, "{name}: {difference}");
        }
        let beam = PagedImage::<f32>::open(root.join("avgPB")).unwrap();
        let beam = beam.get().unwrap();
        let beam = beam.index_axis(Axis(2), 0);
        let result = cold
            .average_beam
            .into_iter()
            .map(Complex32::from)
            .collect::<Vec<_>>();
        let expected = beam
            .as_slice_memory_order()
            .unwrap()
            .iter()
            .copied()
            .map(Complex32::from)
            .collect::<Vec<_>>();
        let difference = nrms(&result, &expected);
        eprintln!("avgPB normalized RMS {difference:.9e}");
        assert!(difference < 1e-3, "avgPB: {difference}");
    }

    fn nrms(actual: &[Complex32], expected: &[Complex32]) -> f64 {
        assert_eq!(actual.len(), expected.len());
        let squared = actual
            .iter()
            .zip(expected)
            .map(|(&left, &right)| f64::from((left - right).norm_sqr()))
            .sum::<f64>();
        (squared
            / expected
                .iter()
                .map(|value| f64::from(value.norm_sqr()))
                .sum::<f64>())
        .sqrt()
    }
}
