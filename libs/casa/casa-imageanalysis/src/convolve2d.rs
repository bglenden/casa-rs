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

mod beam_fit;
pub(crate) use beam_fit::sampled_beam_convolution;

use std::f64::consts::{LN_2, PI};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
};
use casa_core::ArrayD;
use casa_fft::Fft2;
use num_complex::Complex64;

use casa_images::{GaussianBeam, ImageError, ImageInterface};

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

/// The direction coordinate and axes in longitude-latitude order.
fn direction_axes(
    coordinates: &CoordinateSystem,
) -> Result<([usize; 2], &DirectionCoordinate), ImageError> {
    let index = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| {
            ImageError::InvalidMetadata("convolve2d needs a direction coordinate".into())
        })?;
    let CoordinateModel::Direction(direction) = coordinates.coordinate(index) else {
        unreachable!("find_coordinate found a direction coordinate");
    };
    let axes = coordinates
        .coordinate_pixel_axes(index)
        .expect("existing coordinate");
    let [Some(longitude), Some(latitude)] = axes.as_slice() else {
        return Err(ImageError::InvalidMetadata(
            "convolve2d needs both direction axes".into(),
        ));
    };
    Ok(([*longitude, *latitude], direction))
}

fn require_square_pixels(direction: &DirectionCoordinate) -> Result<(), ImageError> {
    let increment = direction.increment();
    let (x, y) = (increment[0].abs(), increment[1].abs());
    if (x - y).abs() > 1e-13 * x.max(y) {
        return Err(ImageError::InvalidMetadata(
            "Pixels must be square, please regrid your image so that they are".into(),
        ));
    }
    Ok(())
}

/// The sky offsets (east, north), rad, of a pixel step along each axis at
/// the reference pixel: `[[east per x, east per y], [north per x, north per
/// y]]`, by central differences of the coordinate's own conversion. It
/// carries the increments, the PC matrix and the projection, as casacore's
/// `GaussianShape::toPixel` does when `worldWidthsToPixel` turns a beam into
/// pixels.
pub(crate) fn sky_per_pixel(direction: &DirectionCoordinate) -> Result<[[f64; 2]; 2], ImageError> {
    const STEP: f64 = 0.5;
    let crpix = direction.reference_pixel();
    let world = |dx: f64, dy: f64| direction.to_world(&[crpix[0] + dx, crpix[1] + dy]);
    let reference = world(0.0, 0.0)?;
    let (longitude, latitude) = (reference[0], reference[1]);
    let (sin_lon, cos_lon) = longitude.sin_cos();
    let (sin_lat, cos_lat) = latitude.sin_cos();
    let east = [-sin_lon, cos_lon, 0.];
    let north = [-sin_lat * cos_lon, -sin_lat * sin_lon, cos_lat];
    let line_of_sight = |world: &[f64]| {
        let (sin_lon, cos_lon) = world[0].sin_cos();
        let (sin_lat, cos_lat) = world[1].sin_cos();
        [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat]
    };
    let mut jacobian = [[0.0; 2]; 2];
    for (axis, (dx, dy)) in [(STEP, 0.0), (0.0, STEP)].into_iter().enumerate() {
        let plus = line_of_sight(&world(dx, dy)?);
        let minus = line_of_sight(&world(-dx, -dy)?);
        let project = |basis: [f64; 3]| {
            (0..3).map(|i| basis[i] * (plus[i] - minus[i])).sum::<f64>() / (2.0 * STEP)
        };
        jacobian[0][axis] = project(east);
        jacobian[1][axis] = project(north);
    }
    Ok(jacobian)
}

/// `Image2DConvolver::_sizeOfGaussian`: twice (5σ rounded, plus one).
fn size_of_gaussian(fwhm_pixels: f64) -> usize {
    let sigma = fwhm_pixels / (8.0 * LN_2).sqrt();
    ((5.0 * sigma + 0.5) as usize + 1) * 2
}

/// A sampled Gaussian defined by its two projected, normalized pixel axes.
/// The footprint is odd and includes all pixels out to CASA's five-sigma
/// cutoff. Its values can also be generated a tile at a time.
#[derive(Clone, Copy)]
pub(crate) struct GaussianSamples {
    pub(crate) side: usize,
    along: [f64; 2],
    across: [f64; 2],
    determinant_root: f64,
    minor_pixels: f64,
}

impl GaussianSamples {
    pub(crate) fn new(beam: GaussianBeam, sky: [[f64; 2]; 2]) -> Result<Self, ImageError> {
        if !beam.major.is_finite()
            || beam.major <= 0.0
            || !beam.minor.is_finite()
            || beam.minor < 0.0
            || beam.minor > beam.major
            || !beam.position_angle.is_finite()
        {
            return Err(ImageError::InvalidMetadata(
                "invalid Gaussian kernel dimensions".into(),
            ));
        }
        let (sin_pa, cos_pa) = beam.position_angle.sin_cos();
        // A target-resolution difference may have rank one. Sample its
        // limiting Gaussian without dividing by zero or rounding positive
        // values onto the axis.
        let minor = if beam.minor == 0.0 {
            beam.major * 1e-12
        } else {
            beam.minor
        };
        let along = [
            (sky[0][0] * sin_pa + sky[1][0] * cos_pa) / beam.major,
            (sky[0][1] * sin_pa + sky[1][1] * cos_pa) / beam.major,
        ];
        let across = [
            (sky[0][0] * cos_pa - sky[1][0] * sin_pa) / minor,
            (sky[0][1] * cos_pa - sky[1][1] * sin_pa) / minor,
        ];
        let a = along[0].powi(2) + across[0].powi(2);
        let b = along[1].powi(2) + across[1].powi(2);
        let c = along[0] * along[1] + across[0] * across[1];
        let determinant_root =
            (sky[0][0] * sky[1][1] - sky[0][1] * sky[1][0]).abs() / beam.major / minor;
        let largest_root = ((a + b + (a - b).hypot(2.0 * c)) / 2.0).sqrt();
        let major_pixels = largest_root / determinant_root;
        if !major_pixels.is_finite()
            || major_pixels <= 0.0
            || major_pixels >= (isize::MAX / 16) as f64
        {
            return Err(ImageError::InvalidMetadata(
                "invalid Gaussian kernel footprint".into(),
            ));
        }
        let side = size_of_gaussian(major_pixels) + 1;
        Ok(Self {
            side,
            along,
            across,
            determinant_root,
            minor_pixels: largest_root.recip(),
        })
    }

    pub(crate) fn sample(&self, x: usize, y: usize) -> f64 {
        let center = self.side / 2;
        let dx = (x as isize - center as isize) as f64;
        let dy = (y as isize - center as isize) as f64;
        let major = self.along[0] * dx + self.along[1] * dy;
        let minor = self.across[0] * dx + self.across[1] * dy;
        (-4.0 * LN_2 * (major.powi(2) + minor.powi(2))).exp()
    }

    pub(crate) fn precision(&self) -> [[f64; 2]; 2] {
        let entry =
            |i: usize, j: usize| self.along[i] * self.along[j] + self.across[i] * self.across[j];
        [[entry(0, 0), entry(0, 1)], [entry(0, 1), entry(1, 1)]]
    }

    pub(crate) fn well_sampled(&self) -> bool {
        self.minor_pixels >= 8.
    }

    pub(crate) fn volume(&self) -> f64 {
        if self.side > 129 {
            if self.well_sampled() {
                // At this sampling the discrete and continuous volumes
                // differ only by the far Gaussian tails outside the stamp.
                return PI / (4. * LN_2 * self.determinant_root);
            }
            let mid = (self.side / 2) as isize;
            return lattice_gaussian_volume(
                self.precision(),
                self.determinant_root.powi(2),
                [0., 0.],
                [[-mid, mid]; 2],
            );
        }
        let mut sum = 0.;
        for y in 0..self.side {
            for x in 0..self.side {
                sum += self.sample(x, y);
            }
        }
        sum
    }

    fn collect(self) -> Result<Vec<f64>, ImageError> {
        let too_large =
            |_| ImageError::InvalidMetadata("Gaussian kernel exceeds available storage".into());
        let count = self
            .side
            .checked_mul(self.side)
            .ok_or_else(|| too_large(()))?;
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|_| too_large(()))?;
        for y in 0..self.side {
            for x in 0..self.side {
                values.push(self.sample(x, y));
            }
        }
        Ok(values)
    }
}

// Sum a possibly thin Gaussian by completing the square along the narrower
// lattice direction. Expand each strip to a tail well below Float precision.
fn lattice_gaussian_volume(
    precision: [[f64; 2]; 2],
    determinant: f64,
    mean: [f64; 2],
    bounds: [[isize; 2]; 2],
) -> f64 {
    const LIMIT: f64 = 14.;
    let first = usize::from(precision[1][1] > precision[0][0]);
    let second = 1 - first;
    let a = precision[first][first];
    let tilt = precision[first][second] / a;
    let reduced = determinant / a;
    let row_radius = (LIMIT / reduced).sqrt();
    let row_first = ((mean[second] - row_radius).ceil() as isize).max(bounds[second][0]);
    let row_last = ((mean[second] + row_radius).floor() as isize).min(bounds[second][1]);
    let mut volume = 0.;
    for row in row_first..=row_last {
        let row_delta = row as f64 - mean[second];
        let row_cost = reduced * row_delta * row_delta;
        let center = mean[first] - tilt * row_delta;
        let radius = ((LIMIT - row_cost).max(0.) / a).sqrt();
        let first_pixel = ((center - radius).ceil() as isize).max(bounds[first][0]);
        let last_pixel = ((center + radius).floor() as isize).min(bounds[first][1]);
        for pixel in first_pixel..=last_pixel {
            let form = a * (pixel as f64 - center).powi(2) + row_cost;
            volume += (-4. * LN_2 * form).exp();
        }
    }
    volume
}

fn kernel(beam: &GaussianBeam, sky: [[f64; 2]; 2]) -> Result<(usize, Vec<f64>), ImageError> {
    let sampled = GaussianSamples::new(*beam, sky)?;
    Ok((sampled.side, sampled.collect()?))
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
    let (axes, direction) = direction_axes(input.coordinates())?;
    require_square_pixels(direction)?;
    if !input.units().to_ascii_uppercase().contains("/PIXEL") {
        return Err(ImageError::InvalidMetadata(format!(
            "convolve2d: casa-rs convolves images in Jy/pixel; this one is in {:?}",
            input.units()
        )));
    }
    let shape = input.shape().to_vec();
    let plane = axes.map(|axis| shape[axis]);
    let (side, values) = kernel(beam, sky_per_pixel(direction)?)?;
    let mut convolver = PlaneConvolver::new(plane, side, &values)?;
    let mut data = input.get()?;
    let mask = input.default_mask()?;
    let others: Vec<usize> = (0..shape.len()).filter(|a| !axes.contains(a)).collect();
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
                index[axes[0]] = i;
                index[axes[1]] = j;
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
                index[axes[0]] = i;
                index[axes[1]] = j;
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

/// Homogenize a spectral moment's selected planes to their common beam.
/// The pixel mask remains on the same positions as the input.
pub(crate) fn convolve_to_common_beam<T>(
    data: &mut ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    coordinates: &CoordinateSystem,
    units: &str,
    beams: &casa_images::ImageBeamSet,
) -> Result<(GaussianBeam, String), ImageError>
where
    T: Into<f64> + From<f32> + Copy,
{
    let target = beams.common_beam()?;
    let (axes, direction) = direction_axes(coordinates)?;
    require_square_pixels(direction)?;
    let sky = sky_per_pixel(direction)?;
    let shape = data.shape().to_vec();
    let plane = axes.map(|axis| shape[axis]);
    let beam_axis = |kind| {
        coordinates
            .find_coordinate(kind)
            .and_then(|i| coordinates.coordinate_pixel_axes(i))
            .and_then(|axes| axes.first().copied().flatten())
    };
    let spectral = beam_axis(CoordinateType::Spectral);
    let stokes = beam_axis(CoordinateType::Stokes);
    let others: Vec<_> = (0..shape.len()).filter(|i| !axes.contains(i)).collect();
    let count = others.iter().map(|&i| shape[i]).product();
    let upper_units = units.to_ascii_uppercase();
    let per_pixel = upper_units.contains("/PIXEL");
    let per_beam = upper_units.contains("BEAM");
    let mut units = units.to_owned();
    let mut index = vec![0; shape.len()];
    let mut values = vec![0.0; plane[0] * plane[1]];
    for mut number in 0..count {
        for &i in &others {
            index[i] = number % shape[i];
            number /= shape[i];
        }
        let channel = spectral.map_or(0, |i| index[i]);
        let polarization = stokes.map_or(0, |i| index[i]);
        let input = *beams.beam(channel, polarization);
        let Some(smoothing) = target.deconvolving_beam(input)? else {
            continue;
        };
        if smoothing.minor <= 0.0 {
            return Err(ImageError::InvalidMetadata(
                "the common-beam convolution requires a positive axial ratio".into(),
            ));
        }
        let (side, mut weights) = kernel(&smoothing, sky)?;
        let scale = if per_pixel {
            // CASA loops over channels within each polarization. The last
            // plane sets the output unit and keeps the input unit if copied.
            if channel + 1 == beams.n_channels() && polarization + 1 == beams.n_stokes() {
                units = "Jy/beam".into();
            }
            1.0
        } else {
            let (input_side, input_weights) = kernel(&input, sky)?;
            let peak = central_gaussian_response(side, &weights, input_side, &input_weights);
            if per_beam {
                peak.recip()
            } else {
                input.area() / (target.area() * peak)
            }
        };
        for weight in &mut weights {
            *weight = f64::from((*weight * scale) as f32);
        }
        let mut convolver = PlaneConvolver::new(plane, side, &weights)?;
        for j in 0..plane[1] {
            index[axes[1]] = j;
            for i in 0..plane[0] {
                index[axes[0]] = i;
                let at = ndarray::IxDyn(&index);
                values[i + j * plane[0]] = if mask.is_none_or(|mask| mask[at.clone()]) {
                    data[at].into()
                } else {
                    0.0
                };
            }
        }
        let convolved = convolver.convolve(&values)?;
        for j in 0..plane[1] {
            index[axes[1]] = j;
            for i in 0..plane[0] {
                index[axes[0]] = i;
                data[ndarray::IxDyn(&index)] = T::from(convolved[i + j * plane[0]] as f32);
            }
        }
    }
    Ok((target, units))
}

fn central_gaussian_response(
    left_side: usize,
    left: &[f64],
    right_side: usize,
    right: &[f64],
) -> f64 {
    // Both odd-sized kernels are centred and even. The peak of their
    // discrete convolution is their dot product over the central overlap.
    let width = left_side.min(right_side);
    let a = (left_side - width) / 2;
    let b = (right_side - width) / 2;
    let mut peak = 0.0;
    for row in 0..width {
        for col in 0..width {
            peak += left[(row + a) * left_side + col + a] * right[(row + b) * right_side + col + b];
        }
    }
    peak
}

/// A position angle in [−90°, 90°], as casacore's `GaussianBeam` keeps it.
pub(crate) fn unwrap_position_angle(angle: f64) -> f64 {
    use std::f64::consts::FRAC_PI_2;
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

    use casa_coordinates::{Projection, ProjectionType};
    use casa_core::measures::direction::DirectionRef;

    /// Sky offsets of RA increasing to the left and Dec up, 1e-6 rad pixels.
    const UNROTATED: [[f64; 2]; 2] = [[-1.0e-6, 0.0], [0.0, 1.0e-6]];

    #[test]
    fn convolution_stays_within_the_mapped_direction_plane() {
        let step = 1.0e-6;
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-step, step],
            [3.0, 3.0],
        ));
        coordinates.add_coordinate(casa_coordinates::LinearCoordinate::new(1, vec![], vec![]));
        coordinates.transpose_axes(&[0, 1, 2], &[1, 2, 0]).unwrap();
        let mut input = casa_images::TempImage::new(
            vec![7, 2, 7],
            coordinates,
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        input.set_units("Jy/pixel").unwrap();
        input.put_at(1.0, &[3, 1, 3]).unwrap();
        let output = convolve2d(&input, &GaussianBeam::new(4.0 * step, 2.0 * step, 0.0)).unwrap();
        for (index, expected) in [
            ([3, 1, 3], 1.0),
            ([5, 1, 3], 0.5),
            ([3, 1, 4], 0.5),
            ([3, 1, 5], 0.0625),
            ([3, 0, 3], 0.0),
        ] {
            let value = output.data[index];
            assert!((value - expected).abs() < 1e-5, "{index:?}: {value}");
        }
    }

    #[test]
    fn kernel_sizes_round_as_image2dconvolver_rounds_them() {
        // σ = 5 / √(8 ln 2) ≈ 2.123: 5σ = 10.6 → (11 + 1) × 2 = 24, made odd.
        assert_eq!(size_of_gaussian(5.0), 24);
        let beam = GaussianBeam::new(5.0e-6, 2.0e-6, 0.0);
        assert_eq!(kernel(&beam, UNROTATED).unwrap().0, 25);
    }

    #[test]
    fn the_major_axis_lies_north_through_east_on_the_sky() {
        // RA increases to the left (negative increment): a position angle of
        // 90° (east) puts the major axis along −x, the same line as +x.
        let beam = GaussianBeam::new(6.0e-6, 2.0e-6, std::f64::consts::FRAC_PI_2);
        let (side, values) = kernel(&beam, UNROTATED).unwrap();
        let c = (side - 1) / 2;
        let along_x = values[(c + 3) + c * side];
        let along_y = values[c + (c + 3) * side];
        assert!(along_x > along_y, "{along_x} {along_y}");
        // Peak 1 at the centre.
        assert!((values[c + c * side] - 1.0).abs() < 1e-15);
    }

    #[test]
    fn a_rotated_grid_turns_the_kernel_with_it() {
        // The #941 review's case: PC [[0, −1], [1, 0]], 1″ pixels, a 6″ × 2″
        // beam at PA 0°. Pixel x then runs north, so the major axis lies
        // along x: half maximum 3 pixels out, as CASA 6.7.6 gives 0.5 there
        // and 0.00195 three pixels along y.
        let arcsec = PI / 648_000.0;
        let rotated = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.3],
            [-arcsec, arcsec],
            [10.0, 10.0],
        )
        .with_pc_matrix(ndarray::array![[0.0, -1.0], [1.0, 0.0]]);
        let beam = GaussianBeam::new(6.0 * arcsec, 2.0 * arcsec, 0.0);
        let (side, values) = kernel(&beam, sky_per_pixel(&rotated).unwrap()).unwrap();
        let c = (side - 1) / 2;
        assert!((values[(c + 3) + c * side] - 0.5).abs() < 1e-9);
        assert!((values[c + (c + 3) * side] - 0.001_953_125).abs() < 1e-9);
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
