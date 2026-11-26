// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::Complex64;
use casa_fft::Fft2;
use ndarray::{Array2, ArrayView2, Zip};

use crate::ImageError;

use super::{StructurePixel, invalid};

const MAX_PAIRWISE_SAMPLES: usize = 64;

pub(super) enum StructureFunction {
    Pairwise,
    Fourier(Box<FourierPlanes>),
}

impl StructureFunction {
    pub(super) fn new(input: [usize; 2], output: [usize; 2]) -> Result<Self, ImageError> {
        if input[0].saturating_mul(input[1]) <= MAX_PAIRWISE_SAMPLES {
            Ok(Self::Pairwise)
        } else {
            FourierPlanes::new(output).map(|plan| Self::Fourier(Box::new(plan)))
        }
    }

    pub(super) fn measure<T: StructurePixel>(
        &mut self,
        data: ArrayView2<'_, T>,
        mask: Option<ArrayView2<'_, bool>>,
    ) -> Result<(Array2<T>, Array2<bool>), ImageError> {
        let valid = |pixel| mask.as_ref().is_none_or(|bits| bits[pixel]);
        match self {
            Self::Fourier(plan) => plan.measure(data, valid),
            Self::Pairwise => Ok(pairwise(data, valid)),
        }
    }
}

fn pairwise<T: StructurePixel>(
    data: ArrayView2<'_, T>,
    valid: impl Fn((usize, usize)) -> bool,
) -> (Array2<T>, Array2<bool>) {
    let (nx, ny) = data.dim();
    let shape = (nx * 2 - 1, ny * 2 - 1);
    let mut totals = Array2::<Complex64>::zeros(shape);
    let mut counts = Array2::<usize>::zeros(shape);
    let points = data
        .indexed_iter()
        .filter(|(pixel, _)| valid(*pixel))
        .map(|(pixel, &value)| (pixel, value.as_complex64()))
        .collect::<Vec<_>>();
    for (index, &((x, y), a)) in points.iter().enumerate() {
        for &((p, q), b) in &points[index..] {
            let lag = (nx - 1 + p - x, ny - 1 + q - y);
            let difference = a - b;
            let square = difference * difference;
            totals[lag] += square;
            counts[lag] += 1;
            if (p, q) != (x, y) {
                let opposite = (nx - 1 + x - p, ny - 1 + y - q);
                totals[opposite] += square;
                counts[opposite] += 1;
            }
        }
    }
    let values = Zip::from(&totals).and(&counts).map_collect(|&sum, &count| {
        T::from_complex64(if count == 0 {
            Complex64::default()
        } else {
            sum / count as f64
        })
    });
    (values, counts.mapv(|count| count != 0))
}

pub(super) struct FourierPlanes {
    signal: Vec<Complex64>,
    squares: Vec<Complex64>,
    mask: Vec<Complex64>,
    shape: [usize; 2],
    output: [usize; 2],
    transform: Fft2<f64>,
}

impl FourierPlanes {
    fn new(output: [usize; 2]) -> Result<Self, ImageError> {
        let [Some(x), Some(y)] = output.map(usize::checked_next_power_of_two) else {
            return Err(invalid("Two-point transform shape overflows"));
        };
        let size = x
            .checked_mul(y)
            .ok_or_else(|| invalid("Two-point transform size overflows"))?;
        let transform = Fft2::with_threads([x, y], 1)
            .map_err(fft_error)?
            .with_estimated_plan();
        Ok(Self {
            signal: vec![Complex64::default(); size],
            squares: vec![Complex64::default(); size],
            mask: vec![Complex64::default(); size],
            shape: [x, y],
            output,
            transform,
        })
    }

    fn measure<T: StructurePixel>(
        &mut self,
        data: ArrayView2<'_, T>,
        valid: impl Fn((usize, usize)) -> bool,
    ) -> Result<(Array2<T>, Array2<bool>), ImageError> {
        for values in [&mut self.signal, &mut self.squares, &mut self.mask] {
            values.fill(Complex64::default());
        }
        let (center, typical) = typical_scale(data, &valid);
        let [width, height] = self.shape;
        let mut nonfinite = Vec::new();
        let mut energy = 0.0;
        let mut nonzero = 0;
        for ((x, y), value) in data.indexed_iter() {
            if valid((x, y)) {
                let value = value.as_complex64() - center;
                if !value.is_finite() {
                    nonfinite.push((x, y));
                    continue;
                }
                let index = x * height + y;
                self.signal[index] = value;
                self.squares[index] = value * value;
                self.mask[index].re = 1.0;
                energy += value.norm_sqr();
                nonzero += usize::from(value != Complex64::default());
            }
        }
        self.correlate()?;
        let [nx, ny] = self.output;
        let origin = [(nx - 1) / 2, (ny - 1) / 2];
        let mut result = Array2::from_elem((nx, ny), T::from_complex64(Complex64::default()));
        let mut result_mask = Array2::from_elem((nx, ny), false);
        let fft_area = (width * height) as f64;
        // Compare transform roundoff with ordinary sample contrast so a
        // bright feature cannot hide a quiet lag it does not contribute to.
        let error = energy * f64::EPSILON * (fft_area.log2() + 1.0) * 8.0;
        let contrast = if typical > 0.0 {
            typical
        } else {
            energy / nonzero.max(1) as f64
        };
        let repair = error > f64::from(f32::EPSILON) * contrast;
        for ((x, y), item) in result.indexed_iter_mut() {
            let x_index = (width + x - origin[0]) % width;
            let y_index = (height + y - origin[1]) % height;
            let index = x_index * height + y_index;
            let count = (self.mask[index].re / fft_area).round();
            if count > 0.0 {
                result_mask[(x, y)] = true;
                if [x, y] != origin {
                    let total = self.squares[index] / fft_area;
                    *item = if repair && total.norm() <= error * 1000.0 {
                        let lag = [
                            x as isize - origin[0] as isize,
                            y as isize - origin[1] as isize,
                        ];
                        T::from_complex64(exact_lag(data, &valid, lag) / count)
                    } else {
                        T::from_fft_estimate(total / count)
                    };
                }
            }
        }
        include_nonfinite(data, valid, &nonfinite, &mut result, &mut result_mask);
        Ok((result, result_mask))
    }

    fn correlate(&mut self) -> Result<(), ImageError> {
        for values in [&mut self.signal, &mut self.squares, &mut self.mask] {
            self.transform.transform(values, false).map_err(fft_error)?;
        }
        let [width, height] = self.shape;
        for x in 0..width {
            for y in 0..height {
                let index = x * height + y;
                let reflected = ((width - x) % width) * height + (height - y) % height;
                if reflected < index {
                    continue;
                }
                // Expand (a-b)^2 and correlate each term. Reflection is
                // frequency negation, without conjugating a complex signal.
                let sum = self.squares[index] * self.mask[reflected]
                    + self.squares[reflected] * self.mask[index]
                    - 2.0 * self.signal[index] * self.signal[reflected];
                self.squares[index] = sum;
                self.squares[reflected] = sum;
                let count = Complex64::new(self.mask[index].norm_sqr(), 0.0);
                self.mask[index] = count;
                self.mask[reflected] = count;
            }
        }
        for values in [&mut self.squares, &mut self.mask] {
            self.transform.transform(values, true).map_err(fft_error)?;
        }
        Ok(())
    }
}

fn typical_scale<T: StructurePixel>(
    data: ArrayView2<'_, T>,
    valid: &impl Fn((usize, usize)) -> bool,
) -> (Complex64, f64) {
    let stride = data.len().div_ceil(31).max(1);
    let mut sample = data
        .indexed_iter()
        .filter_map(|(index, value)| {
            let value = value.as_complex64();
            (valid(index) && value.is_finite()).then_some(value)
        })
        .step_by(stride)
        .collect::<Vec<_>>();
    if sample.is_empty() {
        return (Complex64::default(), 0.0);
    }
    let median = sample.len() / 2;
    sample.sort_unstable_by(|a, b| a.norm_sqr().total_cmp(&b.norm_sqr()));
    let center = sample[median];
    sample.sort_unstable_by(|a, b| {
        (*a - center)
            .norm_sqr()
            .total_cmp(&(*b - center).norm_sqr())
    });
    (center, (sample[median] - center).norm_sqr())
}

fn exact_lag<T: StructurePixel>(
    data: ArrayView2<'_, T>,
    valid: &impl Fn((usize, usize)) -> bool,
    [dx, dy]: [isize; 2],
) -> Complex64 {
    let (nx, ny) = data.dim();
    let x = (-dx).max(0) as usize..nx - dx.max(0) as usize;
    let y = (-dy).max(0) as usize..ny - dy.max(0) as usize;
    let mut sum = Complex64::default();
    for i in x {
        for j in y.clone() {
            let first = (i, j);
            let second = (i.wrapping_add_signed(dx), j.wrapping_add_signed(dy));
            if valid(first) && valid(second) {
                let a = data[first].as_complex64();
                let b = data[second].as_complex64();
                if a.is_finite() && b.is_finite() {
                    let difference = a - b;
                    sum += difference * difference;
                }
            }
        }
    }
    sum
}

fn include_nonfinite<T: StructurePixel>(
    data: ArrayView2<'_, T>,
    valid: impl Fn((usize, usize)) -> bool,
    nonfinite: &[(usize, usize)],
    output: &mut Array2<T>,
    mask: &mut Array2<bool>,
) {
    let (nx, ny) = data.dim();
    for &(x, y) in nonfinite {
        let first = (x, y);
        let a = data[first].as_complex64();
        for (second @ (p, q), value) in data.indexed_iter() {
            let b = value.as_complex64();
            if !valid(second) || (!b.is_finite() && second < first) {
                continue;
            }
            let difference = a - b;
            let power = difference * difference;
            // An exceptional pair makes each reported component nonfinite.
            // A positive pair count does not alter that value or its sign.
            let lag = (nx - 1 + p - x, ny - 1 + q - y);
            output[lag] = T::from_complex64(output[lag].as_complex64() + power);
            mask[lag] = true;
            if first != second {
                let opposite = (nx - 1 + x - p, ny - 1 + y - q);
                output[opposite] = T::from_complex64(output[opposite].as_complex64() + power);
                mask[opposite] = true;
            }
        }
    }
}

fn fft_error(error: casa_fft::FftError) -> ImageError {
    invalid(format!("Cannot compute the two-point transform: {error}"))
}
