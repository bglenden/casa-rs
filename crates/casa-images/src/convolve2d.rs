// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convolving an image's direction planes with a Gaussian, as the image
//! tool's `convolve2d` does (CASA's `Image2DConvolver`).
//!
//! `simutil.convimage` convolves a Jy/pixel sky model with the clean beam to
//! compare it with the clean image. For an image in Jy/pixel, or any flux
//! per pixel, CASA's rules (`Image2DConvolver::_dealWithRestoringBeam`) are:
//! - the kernel is the Gaussian at unit peak, unscaled, so a point of 1
//!   Jy/pixel becomes a peak of 1 Jy/beam;
//! - the output is in Jy/beam, with the Gaussian as its restoring beam.
//!
//! The kernel is sampled on a square of an odd number of pixels: ±5σ of
//! the wider axis, rounded as `Image2DConvolver::_sizeOfGaussian` rounds
//! it. It is centred on the middle pixel, with the beam's position angle
//! (north through east) turned into the pixel frame at the reference pixel.
//! The convolution is linear (casacore's `LatticeConvolver` with
//! `ConvEnums::LINEAR`): the plane is padded with zeros, so flux does not
//! wrap around the edges, and the output has the input's shape. Masked
//! pixels count as zero, and the output keeps the input's mask.
//!
//! As in CASA, the pixels must be square. Images in other units (Jy/beam,
//! whose beam CASA convolves numerically, or none, whose kernel it scales to
//! unit volume) are refused: the simulation tasks convolve Jy/pixel images
//! only.

use std::f64::consts::LN_2;

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType};
use casa_fft::Fft2;
use casa_types::ArrayD;
use num_complex::Complex64;

use crate::{GaussianBeam, ImageError, ImageInterface};

/// An image convolved with a Gaussian.
#[derive(Clone, Debug)]
pub struct Convolved {
    /// The pixels, in the input's shape.
    pub data: ArrayD<f32>,
    /// The input's mask, when it has one.
    pub mask: Option<ArrayD<bool>>,
    /// The output's brightness unit, `Jy/beam`.
    pub units: String,
    /// The output's restoring beam: the Gaussian, its position angle in
    /// [−90°, 90°] as casacore's `GaussianBeam` keeps it.
    pub beam: GaussianBeam,
}

/// The direction coordinate's first pixel axis and its increments, rad.
fn direction_axes(coordinates: &CoordinateSystem) -> Result<(usize, [f64; 2]), ImageError> {
    let index = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| {
            ImageError::InvalidMetadata("convolve2d needs a direction coordinate".into())
        })?;
    let first = (0..index)
        .map(|k| coordinates.coordinate(k).n_pixel_axes())
        .sum();
    let CoordinateModel::Direction(direction) = coordinates.coordinate(index) else {
        unreachable!("find_coordinate found a direction coordinate");
    };
    let increment = direction.increment();
    Ok((first, [increment[0], increment[1]]))
}

/// `Image2DConvolver::_sizeOfGaussian`: twice (5σ rounded, plus one).
fn size_of_gaussian(fwhm_pixels: f64) -> usize {
    let sigma = fwhm_pixels / (8.0 * LN_2).sqrt();
    ((5.0 * sigma + 0.5) as usize + 1) * 2
}

/// The Gaussian kernel for `beam` on pixels of `increment` rad: its side
/// (odd) and values, first axis fastest, peak 1 at the centre.
fn kernel(beam: &GaussianBeam, increment: [f64; 2]) -> (usize, Vec<f64>) {
    let scale = increment[0].abs();
    let (major, minor) = (beam.major / scale, beam.minor / scale);
    // The major axis points (east, north) = (sin θ, cos θ); a pixel step
    // moves increment[0] in longitude and increment[1] in latitude.
    let (sin_pa, cos_pa) = beam.position_angle.sin_cos();
    let angle = (cos_pa / increment[1]).atan2(sin_pa / increment[0]);
    let (sin_a, cos_a) = angle.sin_cos();
    let mut side = size_of_gaussian(major).max(size_of_gaussian(minor));
    if side.is_multiple_of(2) {
        side += 1;
    }
    let centre = (side - 1) as f64 / 2.0;
    let mut values = Vec::with_capacity(side * side);
    for j in 0..side {
        for i in 0..side {
            let (dx, dy) = (i as f64 - centre, j as f64 - centre);
            let along = dx * cos_a + dy * sin_a;
            let across = -dx * sin_a + dy * cos_a;
            values.push((-4.0 * LN_2 * ((along / major).powi(2) + (across / minor).powi(2))).exp());
        }
    }
    (side, values)
}

/// The smallest length at least `n` with no prime factor above 7, which
/// FFTW transforms fastest.
fn fast_length(n: usize) -> usize {
    (n..)
        .find(|&m| {
            let mut rest = m;
            for p in [2, 3, 5, 7] {
                while rest % p == 0 {
                    rest /= p;
                }
            }
            rest == 1
        })
        .expect("a length with small prime factors exists")
}

/// The linear convolution of one plane (first axis fastest) with the
/// kernel, cut to the plane's shape about the kernel's centre.
struct PlaneConvolver {
    plane: [usize; 2],
    padded: [usize; 2],
    kernel_side: usize,
    kernel_transform: Vec<Complex64>,
    fft: Fft2<f64>,
}

impl PlaneConvolver {
    fn new(plane: [usize; 2], kernel_side: usize, kernel: &[f64]) -> Result<Self, ImageError> {
        let padded = [
            fast_length(plane[0] + kernel_side - 1),
            fast_length(plane[1] + kernel_side - 1),
        ];
        // Row-major [rows = second axis][columns = first axis].
        let mut fft = Fft2::<f64>::with_threads([padded[1], padded[0]], 1)
            .map_err(|error| ImageError::InvalidMetadata(format!("convolve2d FFT: {error}")))?
            .with_estimated_plan();
        let mut kernel_transform = vec![Complex64::new(0.0, 0.0); padded[0] * padded[1]];
        for j in 0..kernel_side {
            for i in 0..kernel_side {
                kernel_transform[i + j * padded[0]] =
                    Complex64::new(kernel[i + j * kernel_side], 0.0);
            }
        }
        fft.transform(&mut kernel_transform, false)
            .map_err(|error| ImageError::InvalidMetadata(format!("convolve2d FFT: {error}")))?;
        Ok(Self {
            plane,
            padded,
            kernel_side,
            kernel_transform,
            fft,
        })
    }

    fn convolve(&mut self, values: &[f64]) -> Result<Vec<f64>, ImageError> {
        let [nx, ny] = self.plane;
        let [px, py] = self.padded;
        let mut grid = vec![Complex64::new(0.0, 0.0); px * py];
        for j in 0..ny {
            for i in 0..nx {
                grid[i + j * px] = Complex64::new(values[i + j * nx], 0.0);
            }
        }
        let fft_error = |error| ImageError::InvalidMetadata(format!("convolve2d FFT: {error}"));
        self.fft.transform(&mut grid, false).map_err(fft_error)?;
        for (value, kernel) in grid.iter_mut().zip(&self.kernel_transform) {
            *value *= kernel;
        }
        self.fft.transform(&mut grid, true).map_err(fft_error)?;
        let norm = (px * py) as f64;
        let centre = (self.kernel_side - 1) / 2;
        let mut out = vec![0.0; nx * ny];
        for j in 0..ny {
            for i in 0..nx {
                out[i + j * nx] = grid[(i + centre) + (j + centre) * px].re / norm;
            }
        }
        Ok(out)
    }
}

/// `ia.convolve2d(major, minor, pa)` with a Gaussian on the direction plane
/// of `input`, an image in a flux per pixel.
///
/// # Errors
///
/// An image without a direction coordinate, with pixels that are not square,
/// or in a unit that is not a flux per pixel; a failure to read the image.
pub fn convolve2d(
    input: &impl ImageInterface<f32>,
    beam: &GaussianBeam,
) -> Result<Convolved, ImageError> {
    let (axis, increment) = direction_axes(input.coordinates())?;
    let (x, y) = (increment[0].abs(), increment[1].abs());
    if (x - y).abs() > 1e-13 * x.max(y) {
        return Err(ImageError::InvalidMetadata(
            "Pixels must be square, please regrid your image so that they are".into(),
        ));
    }
    if !input.units().to_ascii_uppercase().contains("/PIXEL") {
        return Err(ImageError::InvalidMetadata(format!(
            "convolve2d: casa-rs convolves images in Jy/pixel; this one is in {:?}",
            input.units()
        )));
    }
    let shape = input.shape().to_vec();
    let plane = [shape[axis], shape[axis + 1]];
    let (side, values) = kernel(beam, increment);
    let mut convolver = PlaneConvolver::new(plane, side, &values)?;
    let mut data = input.get()?;
    let mask = input.default_mask()?;
    let others: Vec<usize> = (0..shape.len())
        .filter(|a| *a != axis && *a != axis + 1)
        .collect();
    let planes: usize = others.iter().map(|a| shape[*a]).product();
    let mut index = vec![0usize; shape.len()];
    let mut values = vec![0.0f64; plane[0] * plane[1]];
    for plane_number in 0..planes {
        let mut rest = plane_number;
        for a in &others {
            index[*a] = rest % shape[*a];
            rest /= shape[*a];
        }
        for j in 0..plane[1] {
            for i in 0..plane[0] {
                index[axis] = i;
                index[axis + 1] = j;
                let good = mask
                    .as_ref()
                    .is_none_or(|mask| mask[ndarray::IxDyn(&index)]);
                values[i + j * plane[0]] = if good {
                    f64::from(data[ndarray::IxDyn(&index)])
                } else {
                    0.0
                };
            }
        }
        let convolved = convolver.convolve(&values)?;
        for j in 0..plane[1] {
            for i in 0..plane[0] {
                index[axis] = i;
                index[axis + 1] = j;
                data[ndarray::IxDyn(&index)] = convolved[i + j * plane[0]] as f32;
            }
        }
    }
    Ok(Convolved {
        data,
        mask,
        units: "Jy/beam".into(),
        beam: GaussianBeam::new(
            beam.major,
            beam.minor,
            unwrap_position_angle(beam.position_angle),
        ),
    })
}

/// A position angle in [−90°, 90°], as casacore's `GaussianBeam` keeps it.
fn unwrap_position_angle(angle: f64) -> f64 {
    use std::f64::consts::{FRAC_PI_2, PI};
    let mut pa = angle % PI;
    if pa > FRAC_PI_2 {
        pa -= PI;
    } else if pa < -FRAC_PI_2 {
        pa += PI;
    }
    pa
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_sizes_round_as_image2dconvolver_rounds_them() {
        // σ = 5 / √(8 ln 2) ≈ 2.123: 5σ = 10.6 → (11 + 1) × 2 = 24, made odd.
        assert_eq!(size_of_gaussian(5.0), 24);
        let beam = GaussianBeam::new(5.0e-6, 2.0e-6, 0.0);
        assert_eq!(kernel(&beam, [-1.0e-6, 1.0e-6]).0, 25);
    }

    #[test]
    fn the_major_axis_lies_north_through_east_on_the_sky() {
        // RA increases to the left (negative increment): a position angle of
        // 90° (east) puts the major axis along −x, the same line as +x.
        let beam = GaussianBeam::new(6.0e-6, 2.0e-6, std::f64::consts::FRAC_PI_2);
        let (side, values) = kernel(&beam, [-1.0e-6, 1.0e-6]);
        let c = (side - 1) / 2;
        let along_x = values[(c + 3) + c * side];
        let along_y = values[c + (c + 3) * side];
        assert!(along_x > along_y, "{along_x} {along_y}");
        // Peak 1 at the centre.
        assert!((values[c + c * side] - 1.0).abs() < 1e-15);
    }

    #[test]
    fn position_angles_are_kept_within_a_quarter_turn() {
        let deg = std::f64::consts::PI / 180.0;
        assert!((unwrap_position_angle(110.0 * deg) / deg + 70.0).abs() < 1e-9);
        assert!((unwrap_position_angle(-62.0 * deg) / deg + 62.0).abs() < 1e-9);
        assert!((unwrap_position_angle(-100.0 * deg) / deg - 80.0).abs() < 1e-9);
    }

    #[test]
    fn fast_lengths_have_small_prime_factors() {
        assert_eq!(fast_length(11), 12);
        assert_eq!(fast_length(97), 98);
        assert_eq!(fast_length(64), 64);
    }
}
