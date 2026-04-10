// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal centered 2-D FFT helpers.

use ndarray::{Array2, Axis};
use num_complex::Complex32;
use rustfft::FftPlanner;

pub(crate) fn centered_fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    transform_axis(&mut shifted, Axis(0), false);
    transform_axis(&mut shifted, Axis(1), false);
    fftshift2(&shifted)
}

pub(crate) fn centered_ifft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    transform_axis(&mut shifted, Axis(0), true);
    transform_axis(&mut shifted, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f32;
    shifted.mapv_inplace(|value| value * scale);
    fftshift2(&shifted)
}

fn transform_axis(data: &mut Array2<Complex32>, axis: Axis, inverse: bool) {
    let len = data.len_of(axis);
    let mut planner = FftPlanner::<f32>::new();
    let fft = if inverse {
        planner.plan_fft_inverse(len)
    } else {
        planner.plan_fft_forward(len)
    };

    if axis.index() == 0 {
        for row_index in 0..data.shape()[0] {
            let mut lane = data.row(row_index).to_vec();
            fft.process(&mut lane);
            for (column_index, value) in lane.into_iter().enumerate() {
                data[(row_index, column_index)] = value;
            }
        }
    } else {
        for column_index in 0..data.shape()[1] {
            let mut lane = data.column(column_index).to_vec();
            fft.process(&mut lane);
            for (row_index, value) in lane.into_iter().enumerate() {
                data[(row_index, column_index)] = value;
            }
        }
    }
}

fn fftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, false)
}

fn ifftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, true)
}

fn shift2(input: &Array2<Complex32>, inverse: bool) -> Array2<Complex32> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let mut output = Array2::<Complex32>::zeros((nx, ny));
    let x_shift = if inverse { nx.div_ceil(2) } else { nx / 2 };
    let y_shift = if inverse { ny.div_ceil(2) } else { ny / 2 };

    for x in 0..nx {
        for y in 0..ny {
            let new_x = (x + x_shift) % nx;
            let new_y = (y + y_shift) % ny;
            output[(x, y)] = input[(new_x, new_y)];
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use ndarray::Array2;
    use num_complex::Complex32;

    use super::{centered_fft2, centered_ifft2};

    #[test]
    fn fft_round_trip_preserves_image() {
        let mut image = Array2::<Complex32>::zeros((8, 8));
        image[(4, 4)] = Complex32::new(1.0, 0.0);
        image[(5, 3)] = Complex32::new(-0.25, 0.5);

        let transformed = centered_fft2(&image);
        let restored = centered_ifft2(&transformed);

        for (expected, actual) in image.iter().zip(restored.iter()) {
            assert!((expected.re - actual.re).abs() < 1.0e-5);
            assert!((expected.im - actual.im).abs() < 1.0e-5);
        }
    }
}
