// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal centered 2-D FFT helpers.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use ndarray::{Array2, Axis};
use num_complex::{Complex32, Complex64};
use rustfft::{Fft, FftPlanner};

type FftKey = (usize, bool);

static FFT32_CACHE: LazyLock<Mutex<HashMap<FftKey, Arc<dyn Fft<f32>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static FFT64_CACHE: LazyLock<Mutex<HashMap<FftKey, Arc<dyn Fft<f64>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut transformed = input.clone();
    transform_axis(&mut transformed, Axis(0), false);
    transform_axis(&mut transformed, Axis(1), false);
    transformed
}

pub(crate) fn centered_fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    transform_axis(&mut shifted, Axis(0), false);
    transform_axis(&mut shifted, Axis(1), false);
    fftshift2(&shifted)
}

pub(crate) fn centered_fft2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    let mut shifted = ifftshift2_f64(input);
    transform_axis_f64(&mut shifted, Axis(0), false);
    transform_axis_f64(&mut shifted, Axis(1), false);
    fftshift2_f64(&shifted)
}

pub(crate) fn centered_ifft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    transform_axis(&mut shifted, Axis(0), true);
    transform_axis(&mut shifted, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f32;
    shifted.mapv_inplace(|value| value * scale);
    fftshift2(&shifted)
}

pub(crate) fn centered_ifft2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    let mut shifted = ifftshift2_f64(input);
    transform_axis_f64(&mut shifted, Axis(0), true);
    transform_axis_f64(&mut shifted, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f64;
    shifted.mapv_inplace(|value| value * scale);
    fftshift2_f64(&shifted)
}

fn transform_axis(data: &mut Array2<Complex32>, axis: Axis, inverse: bool) {
    if axis.index() == 0 {
        transform_rows(data, inverse);
    } else {
        transform_columns(data, inverse);
    }
}

fn transform_axis_f64(data: &mut Array2<Complex64>, axis: Axis, inverse: bool) {
    if axis.index() == 0 {
        transform_rows_f64(data, inverse);
    } else {
        transform_columns_f64(data, inverse);
    }
}

fn fft32(len: usize, inverse: bool) -> Arc<dyn Fft<f32>> {
    let mut cache = FFT32_CACHE.lock().expect("f32 FFT cache lock poisoned");
    if let Some(fft) = cache.get(&(len, inverse)) {
        return Arc::clone(fft);
    }
    let mut planner = FftPlanner::<f32>::new();
    let fft = if inverse {
        planner.plan_fft_inverse(len)
    } else {
        planner.plan_fft_forward(len)
    };
    cache.insert((len, inverse), Arc::clone(&fft));
    fft
}

fn fft64(len: usize, inverse: bool) -> Arc<dyn Fft<f64>> {
    let mut cache = FFT64_CACHE.lock().expect("f64 FFT cache lock poisoned");
    if let Some(fft) = cache.get(&(len, inverse)) {
        return Arc::clone(fft);
    }
    let mut planner = FftPlanner::<f64>::new();
    let fft = if inverse {
        planner.plan_fft_inverse(len)
    } else {
        planner.plan_fft_forward(len)
    };
    cache.insert((len, inverse), Arc::clone(&fft));
    fft
}

fn transform_rows(data: &mut Array2<Complex32>, inverse: bool) {
    let row_len = data.shape()[1];
    let fft = fft32(row_len, inverse);
    for mut row in data.rows_mut() {
        if let Some(row) = row.as_slice_mut() {
            fft.process(row);
        } else {
            let mut lane = row.to_vec();
            fft.process(&mut lane);
            for (column_index, value) in lane.into_iter().enumerate() {
                row[column_index] = value;
            }
        }
    }
}

fn transform_columns(data: &mut Array2<Complex32>, inverse: bool) {
    let [row_count, column_count]: [usize; 2] = data
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let fft = fft32(row_count, inverse);
    let mut lane = vec![Complex32::default(); row_count];
    for column_index in 0..column_count {
        for row_index in 0..row_count {
            lane[row_index] = data[(row_index, column_index)];
        }
        fft.process(&mut lane);
        for row_index in 0..row_count {
            data[(row_index, column_index)] = lane[row_index];
        }
    }
}

fn transform_rows_f64(data: &mut Array2<Complex64>, inverse: bool) {
    let row_len = data.shape()[1];
    let fft = fft64(row_len, inverse);
    for mut row in data.rows_mut() {
        if let Some(row) = row.as_slice_mut() {
            fft.process(row);
        } else {
            let mut lane = row.to_vec();
            fft.process(&mut lane);
            for (column_index, value) in lane.into_iter().enumerate() {
                row[column_index] = value;
            }
        }
    }
}

fn transform_columns_f64(data: &mut Array2<Complex64>, inverse: bool) {
    let [row_count, column_count]: [usize; 2] = data
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let fft = fft64(row_count, inverse);
    let mut lane = vec![Complex64::default(); row_count];
    for column_index in 0..column_count {
        for row_index in 0..row_count {
            lane[row_index] = data[(row_index, column_index)];
        }
        fft.process(&mut lane);
        for row_index in 0..row_count {
            data[(row_index, column_index)] = lane[row_index];
        }
    }
}

fn fftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, false)
}

fn ifftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, true)
}

fn fftshift2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    shift2_f64(input, false)
}

fn ifftshift2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    shift2_f64(input, true)
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

fn shift2_f64(input: &Array2<Complex64>, inverse: bool) -> Array2<Complex64> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let mut output = Array2::<Complex64>::zeros((nx, ny));
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
