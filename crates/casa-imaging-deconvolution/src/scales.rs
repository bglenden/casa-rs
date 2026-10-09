// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's multiscale scale functions and the plane transforms that convolve
//! with them (`MatrixCleaner::makeScale`, `spheroidal`, `makePsfScales`).

use casa_fft::RealFft2;
use num_complex::Complex64;

use crate::Error;
use crate::plane::{PlaneShape, Support};

/// Real two-dimensional transforms of one plane shape, in double precision
/// on one thread. Plans are FFTW's estimated ones: a measured plan is chosen
/// by timing, so its rounding, and with it a solve, could differ from run
/// to run.
pub(crate) struct PlaneFft {
    shape: PlaneShape,
    fft: RealFft2<f64>,
}

impl PlaneFft {
    pub(crate) fn new(shape: PlaneShape) -> Result<Self, Error> {
        Ok(Self {
            shape,
            fft: RealFft2::with_threads([shape.nx, shape.ny], 1)?.with_estimated_plan(),
        })
    }

    /// The half spectrum of `plane`.
    pub(crate) fn forward(&mut self, plane: &[f64]) -> Result<Vec<Complex64>, Error> {
        let mut storage = vec![Complex64::default(); self.fft.storage_len()];
        let stride = self.fft.real_row_stride();
        let real: &mut [f64] = bytemuck::cast_slice_mut(&mut storage);
        for x in 0..self.shape.nx {
            real[x * stride..][..self.shape.ny]
                .copy_from_slice(&plane[x * self.shape.ny..][..self.shape.ny]);
        }
        self.fft.forward(&mut storage)?;
        Ok(storage)
    }

    /// The plane of a half spectrum, normalised by the pixel count.
    pub(crate) fn inverse(&mut self, mut spectrum: Vec<Complex64>) -> Result<Vec<f64>, Error> {
        self.fft.inverse(&mut spectrum)?;
        let stride = self.fft.real_row_stride();
        let scale = 1.0 / self.shape.len() as f64;
        let real: &[f64] = bytemuck::cast_slice(&spectrum);
        let mut plane = vec![0.0; self.shape.len()];
        for x in 0..self.shape.nx {
            for (value, &sample) in plane[x * self.shape.ny..][..self.shape.ny]
                .iter_mut()
                .zip(&real[x * stride..][..self.shape.ny])
            {
                *value = sample * scale;
            }
        }
        Ok(plane)
    }
}

/// `MatrixCleaner::spheroidal`: the rational approximation to the
/// prolate spheroidal function the scales are tapered by.
#[must_use]
pub fn spheroidal(nu: f64) -> f64 {
    if nu <= 0.0 {
        return 1.0;
    }
    if nu >= 1.0 {
        return 0.0;
    }
    let (p, q, end) = if nu < 0.75 {
        (
            [
                8.203343e-2,
                -3.644705e-1,
                6.278660e-1,
                -5.335581e-1,
                2.312756e-1,
            ],
            [1.0, 8.212018e-1, 2.078043e-1],
            0.75,
        )
    } else {
        (
            [
                4.028559e-3,
                -3.697768e-2,
                1.021332e-1,
                -1.201436e-1,
                6.412774e-2,
            ],
            [1.0, 9.599102e-1, 2.918724e-1],
            1.0,
        )
    };
    let delta = nu * nu - end * end;
    let top = p
        .iter()
        .rev()
        .fold(0.0, |sum, coefficient| sum * delta + coefficient);
    let bottom = q
        .iter()
        .rev()
        .fold(0.0, |sum, coefficient| sum * delta + coefficient);
    if bottom == 0.0 { 0.0 } else { top / bottom }
}

/// One scale function: `(1 − r²)·spheroidal(r)` inside radius `size`
/// pixels, normalised to unit sum, as its nonzero offsets from the centre.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScaleFunction {
    /// The scale size in pixels.
    pub(crate) size: f64,
    /// `(dx, dy, value)` of every nonzero sample.
    pub(crate) samples: Vec<(isize, isize, f64)>,
}

impl ScaleFunction {
    /// `MatrixCleaner::makeScale` centred at `centre` of `shape`: a delta for
    /// size 0, otherwise the tapered disc sampled within the plane.
    pub(crate) fn new(size: f64, shape: PlaneShape) -> Self {
        let [cx, cy] = shape.centre();
        if size == 0.0 {
            return Self {
                size,
                samples: vec![(0, 0, 1.0)],
            };
        }
        let low = |centre: usize| ((centre as f64 - size) as isize).max(0);
        let high = |centre: usize, extent: usize| {
            ((centre as f64 + size) as isize).min(extent as isize - 1)
        };
        let mut samples = Vec::new();
        let mut volume = 0.0;
        for y in low(cy)..=high(cy, shape.ny) {
            let y_part = ((cy as f64 - y as f64) / size).powi(2);
            for x in low(cx)..=high(cx, shape.nx) {
                let r2 = y_part + ((cx as f64 - x as f64) / size).powi(2);
                if r2 < 1.0 {
                    let value = (1.0 - r2) * spheroidal(r2.max(0.0).sqrt());
                    volume += value;
                    samples.push((x - cx as isize, y - cy as isize, value));
                }
            }
        }
        for sample in &mut samples {
            sample.2 /= volume;
        }
        samples.retain(|sample| sample.2 != 0.0);
        Self { size, samples }
    }

    /// The function as a plane centred at the origin, wrapping negative
    /// offsets, so that a product of transforms is a centred convolution.
    fn wrapped(&self, shape: PlaneShape) -> Vec<f64> {
        let mut plane = vec![0.0; shape.len()];
        for &(dx, dy, value) in &self.samples {
            let x = dx.rem_euclid(shape.nx as isize) as usize;
            let y = dy.rem_euclid(shape.ny as isize) as usize;
            plane[shape.index(x, y)] += value;
        }
        plane
    }
}

/// The scales of one solve with their transforms and small-scale biases.
pub(crate) struct ScaleBank {
    shape: PlaneShape,
    pub(crate) functions: Vec<ScaleFunction>,
    pub(crate) bias: Vec<f64>,
    transforms: Vec<Vec<Complex64>>,
    pub(crate) fft: PlaneFft,
}

impl ScaleBank {
    /// The scales `sizes` (ascending) with CASA's bias
    /// `1 − bias·size/largest` (1 for a single scale); `small_scale_bias`
    /// is clamped to [−1, 1] as `SDAlgorithmMSClean` does.
    pub(crate) fn new(
        sizes: &[f64],
        small_scale_bias: f64,
        shape: PlaneShape,
    ) -> Result<Self, Error> {
        let small_scale_bias = small_scale_bias.clamp(-1.0, 1.0);
        let largest = sizes.last().copied().unwrap_or(0.0);
        let bias = sizes
            .iter()
            .map(|size| {
                if sizes.len() > 1 {
                    1.0 - small_scale_bias * size / largest
                } else {
                    1.0
                }
            })
            .collect();
        let functions = sizes
            .iter()
            .map(|&size| ScaleFunction::new(size, shape))
            .collect::<Vec<_>>();
        let mut fft = PlaneFft::new(shape)?;
        let transforms = functions
            .iter()
            .map(|function| fft.forward(&function.wrapped(shape)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            shape,
            functions,
            bias,
            transforms,
            fft,
        })
    }

    /// Number of scales.
    pub(crate) fn len(&self) -> usize {
        self.functions.len()
    }

    /// `spectrum` times the transforms of the listed scales, back in the
    /// image domain: the centred circular convolution with each.
    pub(crate) fn convolve(
        &mut self,
        spectrum: &[Complex64],
        scales: &[usize],
    ) -> Result<Vec<f64>, Error> {
        let mut product = spectrum.to_vec();
        for &scale in scales {
            for (value, kernel) in product.iter_mut().zip(&self.transforms[scale]) {
                *value *= kernel;
            }
        }
        self.fft.inverse(product)
    }

    /// The support convolved with each scale, above `threshold`, with the
    /// `border(size)` pixels at every edge removed (`makeScaleMasks`,
    /// `setupUserMask`). `skip_point_border` keeps the edge of the point
    /// scale, as the multi-term cleaner does.
    pub(crate) fn masks(
        &mut self,
        support: &Support,
        threshold: f64,
        skip_point_border: bool,
    ) -> Result<Vec<Support>, Error> {
        let shape = self.shape;
        let plane = support
            .as_slice()
            .iter()
            .map(|supported| f64::from(u8::from(*supported)))
            .collect::<Vec<_>>();
        let spectrum = self.fft.forward(&plane)?;
        (0..self.len())
            .map(|scale| {
                let smoothed = self.convolve(&spectrum, &[scale])?;
                let border = if skip_point_border && scale == 0 {
                    None
                } else {
                    Some((self.functions[scale].size * 1.5) as usize)
                };
                let pixels = smoothed
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        let [x, y] = shape.pixel(index);
                        let inside = border.is_none_or(|border| {
                            x > border
                                && y > border
                                && x + border + 1 < shape.nx
                                && y + border + 1 < shape.ny
                        });
                        inside && *value > threshold
                    })
                    .collect();
                Ok(Support::new(shape, pixels))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_functions_have_unit_sum_and_the_casa_radius() {
        let shape = PlaneShape::new(64, 48);
        for size in [0.0, 1.5, 6.0, 10.0] {
            let function = ScaleFunction::new(size, shape);
            let sum = function.samples.iter().map(|sample| sample.2).sum::<f64>();
            assert!((sum - 1.0).abs() < 1e-12, "size {size}: sum {sum}");
            assert!(
                function
                    .samples
                    .iter()
                    .all(|&(dx, dy, _)| ((dx * dx + dy * dy) as f64) < size * size || size == 0.0)
            );
        }
        assert_eq!(spheroidal(0.0), 1.0);
        assert_eq!(spheroidal(1.0), 0.0);
        // Continuous across the 0.75 break.
        assert!((spheroidal(0.749_999_9) - spheroidal(0.75)).abs() < 1e-5);
    }

    /// Transform products are centred convolutions: a delta convolved with a
    /// scale is the scale centred on the delta, wrapping at the edges.
    #[test]
    fn convolution_is_centred_and_circular() {
        let shape = PlaneShape::new(15, 12);
        let mut bank = ScaleBank::new(&[0.0, 3.0], 0.6, shape).unwrap();
        assert_eq!(bank.bias, vec![1.0, 0.4]);
        let mut delta = vec![0.0; shape.len()];
        delta[shape.index(1, 10)] = 2.0;
        let spectrum = bank.fft.forward(&delta).unwrap();
        let convolved = bank.convolve(&spectrum, &[1]).unwrap();
        let mut expected = vec![0.0; shape.len()];
        for &(dx, dy, value) in &bank.functions[1].samples {
            let x = (1 + dx).rem_euclid(15) as usize;
            let y = (10 + dy).rem_euclid(12) as usize;
            expected[shape.index(x, y)] += 2.0 * value;
        }
        for (a, e) in convolved.iter().zip(&expected) {
            assert!((a - e).abs() < 1e-12);
        }
    }

    #[test]
    fn scale_masks_drop_the_border_and_partial_overlaps() {
        let shape = PlaneShape::new(32, 32);
        let mut bank = ScaleBank::new(&[0.0, 4.0], 0.0, shape).unwrap();
        let support = Support::new(
            shape,
            (0..shape.len())
                .map(|index| {
                    let [x, y] = shape.pixel(index);
                    (8..24).contains(&x) && (8..24).contains(&y)
                })
                .collect(),
        );
        let masks = bank.masks(&support, 0.9, false).unwrap();
        // The point scale keeps the box; a full-image support loses its ring.
        assert_eq!(masks[0], support);
        let full = bank.masks(&Support::full(shape), 0.9, false).unwrap();
        assert!(!full[0].contains(shape.index(0, 5)) && full[0].contains(shape.index(1, 1)));
        assert!(bank.masks(&Support::full(shape), 0.1, true).unwrap()[0].contains(0));
        // The 4-pixel scale needs 90% of its weight inside the box.
        assert!(masks[1].contains(shape.index(16, 16)));
        assert!(!masks[1].contains(shape.index(8, 16)));
        assert!(masks[1].count() < support.count());
    }
}
