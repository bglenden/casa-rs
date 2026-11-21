// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_fft::Fft2;
use casa_types::Complex64;
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
            Self::Fourier(plan)
                if data
                    .indexed_iter()
                    .all(|(pixel, value)| !valid(pixel) || value.as_complex64().is_finite()) =>
            {
                plan.measure(data, valid)
            }
            _ => Ok(pairwise(data, valid)),
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
        let center = data
            .indexed_iter()
            .find(|(pixel, _)| valid(*pixel))
            .map_or_else(Complex64::default, |(_, value)| value.as_complex64());
        let [width, height] = self.shape;
        for ((x, y), value) in data.indexed_iter() {
            if valid((x, y)) {
                let index = x * height + y;
                let value = value.as_complex64() - center;
                self.signal[index] = value;
                self.squares[index] = value * value;
                self.mask[index].re = 1.0;
            }
        }
        self.correlate()?;
        let [nx, ny] = self.output;
        let origin = [(nx - 1) / 2, (ny - 1) / 2];
        let mut result = Array2::from_elem((nx, ny), T::from_complex64(Complex64::default()));
        let mut valid = Array2::from_elem((nx, ny), false);
        let fft_area = (width * height) as f64;
        for ((x, y), item) in result.indexed_iter_mut() {
            let x_index = (width + x - origin[0]) % width;
            let y_index = (height + y - origin[1]) % height;
            let index = x_index * height + y_index;
            let count = (self.mask[index].re / fft_area).round();
            if count > 0.0 {
                valid[(x, y)] = true;
                if [x, y] != origin {
                    *item = T::from_fft_estimate(self.squares[index] / (count * fft_area));
                }
            }
        }
        Ok((result, valid))
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

fn fft_error(error: casa_fft::FftError) -> ImageError {
    invalid(format!("Cannot compute the two-point transform: {error}"))
}
