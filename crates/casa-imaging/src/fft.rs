// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal centered 2-D FFT helpers.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use ndarray::{Array2, Axis};
use num_complex::{Complex32, Complex64};
use rustfft::{Fft, FftPlanner};

type FftKey = (usize, bool);
type FftPlan32 = Arc<dyn Fft<f32>>;
type FftPlan64 = Arc<dyn Fft<f64>>;
type FftCache<T> = LazyLock<Mutex<HashMap<FftKey, T>>>;

static FFT32_CACHE: FftCache<FftPlan32> = LazyLock::new(|| Mutex::new(HashMap::new()));
static FFT64_CACHE: FftCache<FftPlan64> = LazyLock::new(|| Mutex::new(HashMap::new()));

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

pub(crate) fn centered_ifft2_f64_owned(mut input: Array2<Complex64>) -> Array2<Complex64> {
    if !shift2_in_place_even_f64(&mut input) {
        return centered_ifft2_f64(&input);
    }
    inverse_fft2_scale_f64(&mut input);
    let shifted = shift2_in_place_even_f64(&mut input);
    debug_assert!(shifted);
    input
}

pub(crate) fn centered_ifft2_f64_owned_unshifted_even(
    mut input: Array2<Complex64>,
) -> Result<Array2<Complex64>, Array2<Complex64>> {
    if !is_even_contiguous_f64(&input) {
        return Err(input);
    }
    inverse_fft2_scale_centered_frequency_f64(&mut input);
    Ok(input)
}

fn inverse_fft2_scale_f64(input: &mut Array2<Complex64>) {
    transform_axis_f64(input, Axis(0), true);
    transform_axis_f64(input, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f64;
    input.mapv_inplace(|value| value * scale);
}

fn inverse_fft2_scale_centered_frequency_f64(input: &mut Array2<Complex64>) {
    transform_axis_f64(input, Axis(0), true);
    transform_axis_f64(input, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f64;
    let [nx, ny]: [usize; 2] = input
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let storage = input
        .as_slice_memory_order_mut()
        .expect("even centered-frequency FFT input should be contiguous");
    for x in 0..nx {
        let row_base = x * ny;
        let row_sign = if x % 2 == 0 { scale } else { -scale };
        for y in 0..ny {
            let sign = if y % 2 == 0 { row_sign } else { -row_sign };
            storage[row_base + y] *= sign;
        }
    }
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
    let mut scratch = vec![Complex32::default(); fft.get_inplace_scratch_len()];
    for mut row in data.rows_mut() {
        if let Some(row) = row.as_slice_mut() {
            fft.process_with_scratch(row, &mut scratch);
        } else {
            let mut lane = row.to_vec();
            fft.process_with_scratch(&mut lane, &mut scratch);
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
    let mut scratch = vec![Complex32::default(); fft.get_inplace_scratch_len()];
    for column_index in 0..column_count {
        for row_index in 0..row_count {
            lane[row_index] = data[(row_index, column_index)];
        }
        fft.process_with_scratch(&mut lane, &mut scratch);
        for row_index in 0..row_count {
            data[(row_index, column_index)] = lane[row_index];
        }
    }
}

fn transform_rows_f64(data: &mut Array2<Complex64>, inverse: bool) {
    let row_len = data.shape()[1];
    let fft = fft64(row_len, inverse);
    let mut scratch = vec![Complex64::default(); fft.get_inplace_scratch_len()];
    for mut row in data.rows_mut() {
        if let Some(row) = row.as_slice_mut() {
            fft.process_with_scratch(row, &mut scratch);
        } else {
            let mut lane = row.to_vec();
            fft.process_with_scratch(&mut lane, &mut scratch);
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
    let mut scratch = vec![Complex64::default(); fft.get_inplace_scratch_len()];
    for column_index in 0..column_count {
        for row_index in 0..row_count {
            lane[row_index] = data[(row_index, column_index)];
        }
        fft.process_with_scratch(&mut lane, &mut scratch);
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

fn shift2_in_place_even_f64(input: &mut Array2<Complex64>) -> bool {
    if !is_even_contiguous_f64(input) {
        return false;
    }
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let storage = input
        .as_slice_memory_order_mut()
        .expect("even contiguous f64 grid should have memory-order slice");
    let hx = nx / 2;
    let hy = ny / 2;
    for x in 0..hx {
        for y in 0..hy {
            let q00 = x * ny + y;
            let q11 = (x + hx) * ny + y + hy;
            storage.swap(q00, q11);

            let q10 = (x + hx) * ny + y;
            let q01 = x * ny + y + hy;
            storage.swap(q10, q01);
        }
    }
    true
}

fn is_even_contiguous_f64(input: &Array2<Complex64>) -> bool {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    nx % 2 == 0 && ny % 2 == 0 && input.as_slice_memory_order().is_some()
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
    use num_complex::{Complex32, Complex64};

    use super::centered_ifft2_f64_owned_unshifted_even;
    use super::{centered_fft2, centered_ifft2, centered_ifft2_f64, centered_ifft2_f64_owned};

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

    #[test]
    fn owned_f64_ifft_matches_borrowed_for_even_shape() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex64::new((x * 13 + y * 7) as f64, (x as isize - y as isize) as f64)
        });
        let borrowed = centered_ifft2_f64(&image);
        let owned = centered_ifft2_f64_owned(image);
        for (expected, actual) in borrowed.iter().zip(owned.iter()) {
            assert!((expected.re - actual.re).abs() < 1.0e-10);
            assert!((expected.im - actual.im).abs() < 1.0e-10);
        }
    }

    #[test]
    fn unshifted_owned_f64_ifft_can_reconstruct_centered_output() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex64::new((x * 17 + y * 3) as f64, (y as isize - x as isize) as f64)
        });
        let borrowed = centered_ifft2_f64(&image);
        let unshifted =
            centered_ifft2_f64_owned_unshifted_even(image).expect("even contiguous shape");
        let nx = unshifted.shape()[0];
        let ny = unshifted.shape()[1];
        for x in 0..nx {
            for y in 0..ny {
                let actual = unshifted[((x + nx / 2) % nx, (y + ny / 2) % ny)];
                let expected = borrowed[(x, y)];
                assert!((expected.re - actual.re).abs() < 1.0e-10);
                assert!((expected.im - actual.im).abs() < 1.0e-10);
            }
        }
    }

    #[test]
    fn owned_f64_ifft_matches_borrowed_for_odd_shape() {
        let image = Array2::from_shape_fn((7, 5), |(x, y)| {
            Complex64::new((x * 11 + y * 5) as f64, (y as isize - x as isize) as f64)
        });
        let borrowed = centered_ifft2_f64(&image);
        let owned = centered_ifft2_f64_owned(image);
        for (expected, actual) in borrowed.iter().zip(owned.iter()) {
            assert!((expected.re - actual.re).abs() < 1.0e-10);
            assert!((expected.im - actual.im).abs() < 1.0e-10);
        }
    }
}
