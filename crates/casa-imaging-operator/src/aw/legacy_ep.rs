// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generate the original nPBWProjectFT aperture and W convolution cells.

use num_complex::Complex32;

use super::{EvlaApertureGrid, EvlaApertureModel};
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::weighting::SPEED_OF_LIGHT_M_PER_S;

const SCREEN: usize = 2048;
const SAMPLING: usize = 20;

/// The legacy EP convolution planes and their common average primary beam.
pub struct LegacyEpConvolution {
    /// Fourier-cell oversampling.
    pub sampling: usize,
    /// Support radius in visibility-grid pixels, in W-plane order.
    pub support: Vec<usize>,
    /// The side of the stored convolution after cropping to support.
    pub kernel_side: usize,
    /// Imaging and weight cells for each W plane in x-contiguous order.
    pub kernels: Vec<[Vec<Complex32>; 2]>,
    /// Unsquinted beam on the input image geometry, x-contiguous.
    pub average_beam: Vec<f32>,
}

/// The W-plane support and reusable aperture screens before output allocation.
pub struct LegacyEpWPlanePlan {
    average_beam: Vec<f32>,
    screens: [Vec<Complex32>; 2],
    sky_peaks: [usize; 2],
    first_imaging: Vec<Complex32>,
    support: Vec<usize>,
    fft: PlaneFft<f32>,
    sky_cell_rad: [f64; 2],
    w_scale: f64,
}

impl LegacyEpConvolution {
    /// Working memory for the Jones aperture and convolution FFT screens.
    pub const SCREEN_WORKSPACE_BYTES: u64 = 256 << 20;

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
        let support = kernel_support(&kernels[0], 1e-4)?;
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
            support: vec![support],
            kernel_side,
            kernels: vec![[imaging, weights]],
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
        Self::prepare_pbw_model(image, beam, sampling, Some(0.05))
    }

    /// Prepare the four circular Fourier planes, optionally dividing by
    /// the average beam above the requested legacy PB cutoff.
    ///
    /// # Errors
    /// The image, average beam, sampling or cutoff is invalid.
    pub fn prepare_pbw_model(
        image: &ndarray::ArrayD<f32>,
        beam: &ndarray::ArrayD<f32>,
        sampling: usize,
        cutoff: Option<f32>,
    ) -> Result<[Vec<Complex32>; 4], OperatorError> {
        if cutoff.is_some_and(|limit| !limit.is_finite() || limit < 0.) {
            return Err(invalid(
                "the PB correction cutoff must be finite and nonnegative",
            ));
        }
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
                    grid.push(
                        if cutoff.is_some_and(|limit| correction != 0. && correction.abs() >= limit)
                        {
                            circular / correction
                        } else {
                            circular * taper
                        },
                    );
                }
            }
            fft.transform(grid, false)?;
        }
        Ok(result)
    }
}

impl LegacyEpWPlanePlan {
    /// Measure the support of the cold legacy PB/W cells. The caller admits
    /// the aperture, FFT scratch and the materialized output independently.
    /// # Errors
    /// The aperture or image geometry is invalid, or a cell has no support.
    pub fn new(
        aperture: &EvlaApertureModel,
        image_side: usize,
        cell_rad: [f64; 2],
        frequency_hz: f64,
        angle_rad: f64,
        plane_count: usize,
    ) -> Result<Self, OperatorError> {
        if !(2..=usize::from(u16::MAX)).contains(&plane_count) {
            return Err(invalid("legacy cold PB/W requires at least two W planes"));
        }
        let frequency_hz = f64::from(frequency_hz as f32);
        let average_beam =
            aperture_beam(aperture, image_side, cell_rad[0], frequency_hz, angle_rad)?
                .power(false)?
                .into_iter()
                .map(|value| value.re)
                .collect();
        let sky_cell_rad =
            cell_rad.map(|cell| cell * (SAMPLING * image_side) as f64 / SCREEN as f64);
        let mut aperture =
            aperture_beam(aperture, SCREEN, sky_cell_rad[0], frequency_hz, angle_rad)?;
        let screens = [aperture.power(true)?, aperture.power(false)?];
        drop(aperture);
        let sky_peaks = screens.each_ref().map(|screen| peak_index(screen));
        let mut fft = PlaneFft::new([SCREEN, SCREEN], false)?;
        let mut first_imaging = screens[0].clone();
        fft.transform(&mut first_imaging, false)?;
        let width = (plane_count - 1) as f64;
        let w_scale = f64::from((width * width) as f32) / (0.25 / cell_rad[0].abs());
        if !w_scale.is_finite() || w_scale <= 0. {
            return Err(invalid("invalid legacy EP cold W scale"));
        }
        let mut support = Vec::with_capacity(plane_count);
        let first_peak = centre(&first_imaging);
        for plane in 0..plane_count {
            let peak = if plane == 0 {
                first_peak
            } else {
                // The FFT centre is the sum of the projected screen. Only
                // the inner window differs from the W=0 screen.
                first_peak
                    + w_phases(sky_cell_rad, w_scale, plane)
                        .map(|(index, phase)| screens[0][index] * (phase - 1.))
                        .sum::<Complex32>()
            };
            // CASA searches plane zero even for the threshold from plane W.
            support.push(kernel_support(&first_imaging, peak.norm() * 1e-4)?);
        }
        Ok(Self {
            average_beam,
            screens,
            sky_peaks,
            first_imaging,
            support,
            fft,
            sky_cell_rad,
            w_scale,
        })
    }

    /// Additional storage required for all cropped imaging and weight cells.
    pub fn retained_kernel_bytes(&self) -> u64 {
        let side = (2 * self.crop_radius() + 1) as u64;
        side.saturating_pow(2)
            .saturating_mul(self.support.len() as u64)
            .saturating_mul((2 * size_of::<Complex32>()) as u64)
            .saturating_add((self.support.len() * 128) as u64)
    }

    /// The W coordinate used during the tool call that generated the cache.
    pub fn w_scale(&self) -> f64 {
        self.w_scale
    }

    fn crop_radius(&self) -> usize {
        (self.support.iter().copied().max().unwrap_or(0) + 1) * SAMPLING
    }

    /// Transform and crop the cells using their common canvas.
    /// # Errors
    /// A Fourier transform failed, or the plane's normalization is invalid.
    pub fn build(mut self) -> Result<LegacyEpConvolution, OperatorError> {
        let radius = self.crop_radius();
        let mut first_weights = self.screens[1].clone();
        self.fft.transform(&mut first_weights, false)?;
        let peaks = [centre(&self.first_imaging), centre(&first_weights)];
        if peaks
            .iter()
            .any(|peak| !peak.re.is_finite() || peak.re <= 0.)
        {
            return Err(invalid("legacy EP convolution has no positive peak"));
        }
        let mut kernels = Vec::with_capacity(self.support.len());
        let mut imaging = std::mem::take(&mut self.first_imaging);
        let mut weight = first_weights;
        for (plane, &support) in self.support.iter().enumerate() {
            let mut normalization = peaks;
            if plane > 0 {
                imaging.copy_from_slice(&self.screens[0]);
                weight.copy_from_slice(&self.screens[1]);
                for (index, phase) in w_phases(self.sky_cell_rad, self.w_scale, plane) {
                    imaging[index] *= phase;
                    weight[index] *= phase;
                }
                // Dividing by the projected complex sky peak before FFT
                // is equivalent to removing its phase from the result.
                for (cell, grid) in [&imaging, &weight].into_iter().enumerate() {
                    let peak = self.sky_peaks[cell];
                    normalization[cell] *= grid[peak] / self.screens[cell][peak];
                }
                self.fft.transform(&mut imaging, false)?;
                self.fft.transform(&mut weight, false)?;
            }
            let [mut imaging, mut weights] = [&imaging, &weight].map(|grid| crop(grid, radius));
            for value in &mut imaging {
                *value /= normalization[0];
            }
            let integral = sample_cropped(&imaging, 2 * radius + 1, support)?;
            if !integral.is_finite() || integral <= 0. {
                return Err(invalid("legacy EP convolution integral is not positive"));
            }
            for value in &mut imaging {
                *value /= integral;
            }
            for value in &mut weights {
                *value /= normalization[1] * integral * integral;
            }
            kernels.push([imaging, weights]);
        }
        Ok(LegacyEpConvolution {
            sampling: SAMPLING,
            support: self.support,
            kernel_side: 2 * radius + 1,
            kernels,
            average_beam: self.average_beam,
        })
    }
}

fn centre(screen: &[Complex32]) -> Complex32 {
    screen[(SCREEN / 2) * SCREEN + SCREEN / 2]
}

fn w_phases(
    [dx, dy]: [f64; 2],
    scale: f64,
    plane: usize,
) -> impl Iterator<Item = (usize, Complex32)> {
    let half_window = (SCREEN / SAMPLING / 2) as isize;
    let origin = (SCREEN / 2) as isize;
    let index = plane as f64;
    let phase_factor = std::f64::consts::TAU * index * index / scale;
    (-half_window..half_window).flat_map(move |y| {
        let m_squared = (y as f64 * dy).powi(2);
        (-half_window..half_window).filter_map(move |x| {
            let radius_squared = (x as f64 * dx).powi(2) + m_squared;
            (radius_squared < 1.).then(|| {
                let phase = phase_factor * ((1. - radius_squared).sqrt() - 1.);
                let (sin, cos) = phase.sin_cos();
                let index = (origin + y) as usize * SCREEN + (origin + x) as usize;
                (index, Complex32::new(cos as f32, sin as f32))
            })
        })
    })
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

fn kernel_support(kernel: &[Complex32], threshold: f32) -> Result<usize, OperatorError> {
    let origin = SCREEN as f64 * 0.5;
    for radius in (2..=SCREEN / 4).rev() {
        // The original repeatedly sweeps the same circle at angles 2πi/R.
        for step in 0..radius {
            let (sin, cos) = (std::f64::consts::TAU * step as f64 / radius as f64).sin_cos();
            let x = (origin + radius as f64 * sin) as usize;
            let y = (origin + radius as f64 * cos) as usize;
            if kernel[y * SCREEN + x].norm() > threshold {
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

fn sample_cropped(kernel: &[Complex32], side: usize, support: usize) -> Result<f32, OperatorError> {
    // CASA uses the peak's X coordinate for both sampling axes.
    let origin = peak_index(kernel) % side;
    let radius = support * SAMPLING;
    debug_assert!(radius >= SAMPLING);
    if origin < radius || origin + radius - SAMPLING >= side {
        return Err(invalid(
            "legacy EP convolution peak is outside its sampling area",
        ));
    }
    let bounds = origin - radius..origin + radius;
    let mut integral = 0.;
    for y in bounds.clone().step_by(SAMPLING) {
        for x in bounds.clone().step_by(SAMPLING) {
            integral += kernel[y * side + x].re;
        }
    }
    Ok(integral)
}

fn peak_index(samples: &[Complex32]) -> usize {
    let mut peak = 0.;
    let mut best = 0;
    for (index, sample) in samples.iter().enumerate() {
        let magnitude = sample.norm_sqr();
        if magnitude > peak {
            peak = magnitude;
            best = index;
        }
    }
    best
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
    fn strided_integral_uses_the_last_sample_for_its_edge_check() {
        let side = 161;
        let mut kernel = vec![Complex32::from(1.); side * side];
        let interior = 110 * side + 110;
        kernel[interior] = Complex32::from(2.);
        assert_eq!(sample_cropped(&kernel, side, 3).unwrap(), 37.);
        kernel[interior] = Complex32::from(1.);
        kernel[121 * side + 121] = Complex32::from(2.);
        assert!(sample_cropped(&kernel, side, 3).is_err());
    }

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
            (cold.sampling, cold.support[0], cold.kernel_side),
            (20, 3, 161)
        );
        let [imaging, weights] = cold.kernels.into_iter().next().unwrap();
        for (name, obtained) in [("CF0_0", imaging), ("CFWT0_0", weights)] {
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
