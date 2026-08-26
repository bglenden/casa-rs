// SPDX-License-Identifier: LGPL-3.0-or-later

//! Restoration and normalization algorithms for continuum image products.
//!
//! The restoring kernel is evaluated in physical radians: fitted beam widths
//! and the image cell scale share one unit system, so a multi-pixel beam
//! stays a multi-pixel kernel at any cell size.

use ndarray::{Array2, Axis};
use num_complex::Complex64;
use rustfft::FftPlanner;

use casa_imaging_model::ProductNormalization;

use crate::beam::RestoringBeam;
use crate::error::ProductsError;

const FWHM_TO_SIGMA: f64 = 1.0 / 2.354_820_045_030_949_3;

/// Normalize one unnormalized plane to its compiled product normalization.
///
/// `UnitResponse` and the currently supported scalar-response `FlatNoise`
/// path divide by the scalar sensitivity (sum weight). `UnitResponse` does
/// not apply direction-dependent sensitivity division. When no usable scalar
/// sensitivity exists every pixel blanks to NaN. `FlatSky` is not produced by
/// this catalog version.
///
/// # Errors
///
/// Fails when the normalization is not representable by this catalog.
pub fn normalize_plane(
    values: &[f32],
    normalization: ProductNormalization,
    sensitivity: f64,
) -> Result<Vec<f32>, ProductsError> {
    match normalization {
        ProductNormalization::UnitResponse | ProductNormalization::FlatNoise => {
            if !(sensitivity.is_finite() && sensitivity > 0.0) {
                return Ok(vec![f32::NAN; values.len()]);
            }
            Ok(values
                .iter()
                .map(|value| value / sensitivity as f32)
                .collect())
        }
        ProductNormalization::FlatSky => Err(ProductsError::UnsupportedProductRole {
            role: casa_imaging_model::ProductRole::Sensitivity,
            catalog: crate::CONTINUUM_ALGORITHM_CATALOG_VERSION,
        }),
    }
}

/// Build the unit-peak elliptical-Gaussian restoring kernel image.
///
/// `cell_size_rad` is the image cell scale in radians per pixel on each
/// axis: the fitted beam widths are radians, so pixel offsets must be
/// converted to radians before they are divided by the Gaussian sigmas.
#[must_use]
pub fn gaussian_beam_image(
    shape: [usize; 2],
    beam: &RestoringBeam,
    cell_size_rad: [f64; 2],
) -> Array2<f32> {
    let [width, height] = shape;
    let sigma_major = (beam.major_fwhm_rad() * FWHM_TO_SIGMA).abs();
    let sigma_minor = (beam.minor_fwhm_rad() * FWHM_TO_SIGMA).abs();
    let cos_pa = beam.position_angle_rad().cos();
    let sin_pa = beam.position_angle_rad().sin();
    let centre_x = width as f64 / 2.0;
    let centre_y = height as f64 / 2.0;
    let mut kernel = Array2::<f32>::zeros((width, height));
    for x in 0..width {
        for y in 0..height {
            let dx = (x as f64 - centre_x) * cell_size_rad[0];
            let dy = (y as f64 - centre_y) * cell_size_rad[1];
            let u = dx * cos_pa + dy * sin_pa;
            let v = -dx * sin_pa + dy * cos_pa;
            kernel[(x, y)] =
                (-0.5 * ((u / sigma_minor).powi(2) + (v / sigma_major).powi(2))).exp() as f32;
        }
    }
    let peak = kernel.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    if peak > 0.0 {
        kernel.mapv_inplace(|value| value / peak);
    }
    kernel
}

/// Convolve one real plane with an equal-shape kernel through even-shifted
/// FFTs, matching the reconstruction owner's transform conventions.
#[must_use]
pub fn fft_convolve(plane: &[f32], kernel: &[f32], shape: [usize; 2]) -> Vec<f32> {
    let cells = shape[0] * shape[1];
    let mut planner = FftPlanner::<f64>::new();
    let mut signal = Array2::<Complex64>::from_shape_vec(
        (shape[0], shape[1]),
        plane
            .iter()
            .map(|v| Complex64::new(f64::from(*v), 0.0))
            .collect(),
    )
    .expect("shape matches payload");
    let mut response = Array2::<Complex64>::from_shape_vec(
        (shape[0], shape[1]),
        kernel
            .iter()
            .map(|v| Complex64::new(f64::from(*v), 0.0))
            .collect(),
    )
    .expect("shape matches kernel");

    for data in [&mut signal, &mut response] {
        shift_even(data);
        for axis in 0..2 {
            let plan = planner.plan_fft_forward(data.len_of(Axis(axis)));
            let scratch_len = plan.get_inplace_scratch_len();
            let length = data.len_of(Axis(axis));
            let mut lane = vec![Complex64::default(); length];
            let mut scratch = vec![Complex64::default(); scratch_len];
            for mut view in data.lanes_mut(Axis(axis)) {
                lane.iter_mut()
                    .zip(view.iter())
                    .for_each(|(target, source)| {
                        *target = *source;
                    });
                plan.process_with_scratch(&mut lane, &mut scratch);
                lane.iter()
                    .zip(view.iter_mut())
                    .for_each(|(source, target)| *target = *source);
            }
        }
        shift_even(data);
    }
    for (signal, response) in signal.iter_mut().zip(response.iter()) {
        *signal *= *response;
    }
    shift_even(&mut signal);
    for axis in 0..2 {
        let plan = planner.plan_fft_inverse(signal.len_of(Axis(axis)));
        let scratch_len = plan.get_inplace_scratch_len();
        let length = signal.len_of(Axis(axis));
        let mut lane = vec![Complex64::default(); length];
        let mut scratch = vec![Complex64::default(); scratch_len];
        for mut view in signal.lanes_mut(Axis(axis)) {
            lane.iter_mut()
                .zip(view.iter())
                .for_each(|(target, source)| *target = *source);
            plan.process_with_scratch(&mut lane, &mut scratch);
            lane.iter()
                .zip(view.iter_mut())
                .for_each(|(source, target)| *target = *source);
        }
    }
    shift_even(&mut signal);

    let scale = 1.0 / cells as f64;
    signal
        .iter()
        .map(|value| (value.re * scale) as f32)
        .collect()
}

fn shift_even(data: &mut Array2<Complex64>) {
    let [width, height] = [data.shape()[0], data.shape()[1]];
    debug_assert_eq!(width % 2, 0);
    debug_assert_eq!(height % 2, 0);
    for x in 0..width / 2 {
        for y in 0..height / 2 {
            data.swap((x, y), (x + width / 2, y + height / 2));
            data.swap((x + width / 2, y), (x, y + height / 2));
        }
    }
}
