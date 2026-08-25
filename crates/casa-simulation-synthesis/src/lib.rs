// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! CASA-compatible synthesis kernels used while simulating MeasurementSets.
//!
//! This crate owns only the numerical forward-model pieces needed by the
//! simulator: standard-MFS model prediction and the cached Airy voltage
//! pattern. MeasurementSet persistence remains in `casa-ms`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use libm::j1;
use ndarray::{Array2, Axis};
use num_complex::Complex32;
use rustfft::{Fft, FftPlanner};
use thiserror::Error;

const GRIDDER_SUPPORT: usize = 3;
const GRIDDER_TAP_COUNT: usize = GRIDDER_SUPPORT * 2 + 1;
const GRIDDER_PRODUCT_TAP_COUNT: usize = GRIDDER_TAP_COUNT * GRIDDER_TAP_COUNT;

type FftKey = (usize, bool);
type FftPlan = Arc<dyn Fft<f32>>;
static FFT_CACHE: LazyLock<Mutex<HashMap<FftKey, FftPlan>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Failure to construct a simulation synthesis operator.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SynthesisError {
    /// The requested image geometry is not usable by the standard gridder.
    #[error("invalid simulation synthesis request: {0}")]
    InvalidRequest(String),
}

/// Two-dimensional image geometry for the simulation model plane.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImageGeometry {
    /// `[nx, ny]` image shape in pixels.
    pub image_shape: [usize; 2],
    /// `[dx, dy]` positive pixel size in radians.
    pub cell_size_rad: [f64; 2],
}

impl ImageGeometry {
    fn nx(self) -> usize {
        self.image_shape[0]
    }

    fn ny(self) -> usize {
        self.image_shape[1]
    }

    fn validate(self) -> Result<(), SynthesisError> {
        if self.nx() < 8 || self.ny() < 8 {
            return Err(SynthesisError::InvalidRequest(
                "image shape must be at least 8x8".to_string(),
            ));
        }
        if !(self.cell_size_rad[0].is_finite()
            && self.cell_size_rad[0] > 0.0
            && self.cell_size_rad[1].is_finite()
            && self.cell_size_rad[1] > 0.0)
        {
            return Err(SynthesisError::InvalidRequest(
                "cell sizes must be finite positive radians".to_string(),
            ));
        }
        Ok(())
    }
}

/// Circular primary-beam voltage model used by the simulator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AiryPrimaryBeam {
    /// Dish diameter in meters.
    pub dish_diameter_m: f64,
    /// Central blockage diameter in meters.
    pub blockage_diameter_m: f64,
}

/// Cached CASA-compatible Airy voltage-pattern evaluator.
#[derive(Clone)]
pub struct AiryVoltagePattern {
    maximum_radius_arcmin_ghz: f64,
    values: Vec<f32>,
}

impl AiryVoltagePattern {
    /// Build a cached evaluator for one dish and blockage geometry.
    pub fn new(model: AiryPrimaryBeam) -> Self {
        let maximum_radius_arcmin_ghz =
            casa_airy_max_radius_arcmin_ghz(model.dish_diameter_m, model.blockage_diameter_m);
        let sample_count_minus_one = 9_999.0;
        let values = (0..=sample_count_minus_one as usize)
            .map(|index| {
                airy_voltage_pattern_from_table_index(
                    index as f64,
                    maximum_radius_arcmin_ghz,
                    model.dish_diameter_m,
                    model.blockage_diameter_m,
                )
            })
            .collect();
        Self {
            maximum_radius_arcmin_ghz,
            values,
        }
    }

    /// Evaluate the voltage pattern at direction-cosine offsets in radians.
    pub fn evaluate_offsets(&self, l_rad: f64, m_rad: f64, frequency_hz: f64) -> f32 {
        if !(l_rad.is_finite()
            && m_rad.is_finite()
            && frequency_hz.is_finite()
            && frequency_hz > 0.0)
        {
            return 0.0;
        }
        // Preserve CASA PBMath1D's Float rounding at the radius boundaries.
        let l_deg = l_rad.to_degrees() as f32;
        let m_deg = m_rad.to_degrees() as f32;
        let radius_deg = (l_deg * l_deg + m_deg * m_deg).sqrt();
        let radius_arcmin_ghz =
            (f64::from(radius_deg) * 60.0 * (frequency_hz / 1.0e9)) as f32 as f64;
        if radius_arcmin_ghz > self.maximum_radius_arcmin_ghz {
            return 0.0;
        }
        let sample_count_minus_one = (self.values.len() - 1) as f64;
        let index = (radius_arcmin_ghz * sample_count_minus_one / self.maximum_radius_arcmin_ghz)
            .floor()
            .clamp(0.0, sample_count_minus_one) as usize;
        self.values[index]
    }
}

/// Serial standard-MFS predictor for one simulation model plane.
pub struct StandardMfsModelPredictor {
    gridder: StandardGridder,
    model_grid: Option<Array2<Complex32>>,
}

impl StandardMfsModelPredictor {
    /// Build a predictor for one image geometry and model plane.
    pub fn new(geometry: ImageGeometry, model: &Array2<f32>) -> Result<Self, SynthesisError> {
        let gridder = StandardGridder::new_with_casa_composite_padding(geometry)?;
        let model_has_components = model.iter().any(|value| value.abs() > 0.0);
        let model_grid = model_has_components.then(|| centered_fft2(&gridder.apodize_model(model)));
        Ok(Self {
            gridder,
            model_grid,
        })
    }

    /// Predict the model visibility at one `(u, v)` coordinate in wavelengths.
    pub fn predict(&self, u_lambda: f64, v_lambda: f64) -> Complex32 {
        let Some(model_grid) = self.model_grid.as_ref() else {
            return Complex32::new(0.0, 0.0);
        };
        if let Some(predicted) = self
            .gridder
            .degrid_sample_product_planned_sectdgrid(model_grid, u_lambda, v_lambda)
        {
            return predicted;
        }
        let Some(plan) = self.gridder.plan_sample(u_lambda, v_lambda) else {
            return Complex32::new(0.0, 0.0);
        };
        self.gridder
            .degrid_sample_product_planned(model_grid, &plan)
    }
}

#[derive(Clone, Copy)]
struct TapSet {
    indices: [usize; GRIDDER_TAP_COUNT],
    weights: [f32; GRIDDER_TAP_COUNT],
}

#[derive(Clone, Copy)]
struct ProductTapSet {
    flat_indices: [usize; GRIDDER_PRODUCT_TAP_COUNT],
    weights: [f32; GRIDDER_PRODUCT_TAP_COUNT],
}

struct StandardGridder {
    geometry: ImageGeometry,
    grid_shape: [usize; 2],
    image_blc: [usize; 2],
    oversampling: usize,
    kernel_table: Vec<f32>,
    normalized_tap_weights: Vec<[f32; GRIDDER_TAP_COUNT]>,
    correction_x: Vec<f32>,
    correction_y: Vec<f32>,
    du_lambda: f64,
    dv_lambda: f64,
}

impl StandardGridder {
    fn new_with_casa_composite_padding(geometry: ImageGeometry) -> Result<Self, SynthesisError> {
        geometry.validate()?;
        let grid_shape = [
            casa_composite_padded_len(geometry.nx(), 1.3),
            casa_composite_padded_len(geometry.ny(), 1.3),
        ];
        let image_blc = [
            (grid_shape[0] - geometry.nx() + usize::from(grid_shape[0] % 2 == 0)) / 2,
            (grid_shape[1] - geometry.ny() + usize::from(grid_shape[1] % 2 == 0)) / 2,
        ];
        let oversampling = 100usize;
        let mut kernel_table = vec![0.0f32; oversampling * (GRIDDER_SUPPORT + 1)];
        for (index, kernel) in kernel_table
            .iter_mut()
            .enumerate()
            .take(oversampling * GRIDDER_SUPPORT)
        {
            let distance = index as f64 / (GRIDDER_SUPPORT as f64 * oversampling as f64);
            *kernel = spheroidal_kernel(distance * GRIDDER_SUPPORT as f64, GRIDDER_SUPPORT as f64);
        }
        let normalized_tap_weights = build_normalized_tap_weights(&kernel_table, oversampling);
        let correction_x = build_correction_axis(grid_shape[0]);
        let correction_y = build_correction_axis(grid_shape[1]);
        Ok(Self {
            geometry,
            grid_shape,
            image_blc,
            oversampling,
            kernel_table,
            normalized_tap_weights,
            correction_x,
            correction_y,
            du_lambda: 1.0 / (grid_shape[0] as f64 * geometry.cell_size_rad[0]),
            dv_lambda: 1.0 / (grid_shape[1] as f64 * geometry.cell_size_rad[1]),
        })
    }

    fn plan_sample(&self, u_lambda: f64, v_lambda: f64) -> Option<ProductTapSet> {
        let x = self.sample_taps(self.grid_coordinate_x(u_lambda), self.grid_shape[0])?;
        let y = self.sample_taps(self.grid_coordinate_y(v_lambda), self.grid_shape[1])?;
        Some(flatten_tap_products(&x, &y, self.grid_shape[1]))
    }

    fn degrid_sample_product_planned(
        &self,
        grid: &Array2<Complex32>,
        taps: &ProductTapSet,
    ) -> Complex32 {
        if let Some(storage) = grid.as_slice_memory_order() {
            let mut value = Complex32::new(0.0, 0.0);
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                value += storage[taps.flat_indices[tap]] * taps.weights[tap];
            }
            return value;
        }
        let mut value = Complex32::new(0.0, 0.0);
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            let flat_index = taps.flat_indices[tap];
            value += grid[(
                flat_index / self.grid_shape[1],
                flat_index % self.grid_shape[1],
            )] * taps.weights[tap];
        }
        value
    }

    fn degrid_sample_product_planned_sectdgrid(
        &self,
        grid: &Array2<Complex32>,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<Complex32> {
        let x_taps =
            self.sample_taps_unnormalized(self.grid_coordinate_x(u_lambda), self.grid_shape[0])?;
        let y_taps =
            self.sample_taps_unnormalized(self.grid_coordinate_y(v_lambda), self.grid_shape[1])?;
        let mut value = Complex32::new(0.0, 0.0);
        let mut norm = 0.0f32;
        if let Some(storage) = grid.as_slice_memory_order() {
            let grid_stride = self.grid_shape[1];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = x_taps.indices[x_tap];
                let x_weight = x_taps.weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = x_weight * y_taps.weights[y_tap];
                    value += storage[x_index * grid_stride + y_taps.indices[y_tap]] * weight;
                    norm += weight;
                }
            }
        } else {
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = x_taps.indices[x_tap];
                let x_weight = x_taps.weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = x_weight * y_taps.weights[y_tap];
                    value += grid[(x_index, y_taps.indices[y_tap])] * weight;
                    norm += weight;
                }
            }
        }
        (norm > 0.0 && norm.is_finite()).then(|| value / norm)
    }

    fn apodize_model(&self, model: &Array2<f32>) -> Array2<Complex32> {
        let mut apodized = Array2::<Complex32>::zeros((self.grid_shape[0], self.grid_shape[1]));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let correction = self.correction_x[grid_x] * self.correction_y[grid_y];
                apodized[(grid_x, grid_y)] = Complex32::new(model[(x, y)] * correction, 0.0);
            }
        }
        apodized
    }

    fn grid_coordinate_x(&self, u_lambda: f64) -> f64 {
        u_lambda / self.du_lambda + self.grid_shape[0] as f64 / 2.0
    }

    fn grid_coordinate_y(&self, v_lambda: f64) -> f64 {
        -v_lambda / self.dv_lambda + self.grid_shape[1] as f64 / 2.0
    }

    fn sample_taps(&self, coordinate: f64, size: usize) -> Option<TapSet> {
        if !coordinate.is_finite() {
            return None;
        }
        let anchor = coordinate.round() as isize;
        let offset = ((anchor as f64 - coordinate) * self.oversampling as f64).round() as isize;
        let start = anchor - GRIDDER_SUPPORT as isize;
        let end = anchor + GRIDDER_SUPPORT as isize;
        if start < 0 || end >= size as isize {
            return None;
        }
        let half = self.oversampling as isize / 2;
        let weight_index = offset + half;
        if weight_index < 0 || weight_index >= self.normalized_tap_weights.len() as isize {
            return None;
        }
        let mut indices = [0usize; GRIDDER_TAP_COUNT];
        for (tap, index) in (start..=end).enumerate() {
            indices[tap] = index as usize;
        }
        Some(TapSet {
            indices,
            weights: self.normalized_tap_weights[weight_index as usize],
        })
    }

    fn sample_taps_unnormalized(&self, coordinate: f64, size: usize) -> Option<TapSet> {
        if !coordinate.is_finite() {
            return None;
        }
        let anchor = coordinate.round() as isize;
        let offset = ((anchor as f64 - coordinate) * self.oversampling as f64).round() as isize;
        let mut indices = [0usize; GRIDDER_TAP_COUNT];
        let mut weights = [0.0f32; GRIDDER_TAP_COUNT];
        let mut norm = 0.0f32;
        for (tap, index) in
            ((anchor - GRIDDER_SUPPORT as isize)..=(anchor + GRIDDER_SUPPORT as isize)).enumerate()
        {
            if index < 0 || index >= size as isize {
                return None;
            }
            let delta = index - anchor;
            let lookup = (delta * self.oversampling as isize + offset).unsigned_abs();
            let weight = self.kernel_table.get(lookup).copied().unwrap_or(0.0);
            indices[tap] = index as usize;
            weights[tap] = weight;
            norm += weight;
        }
        (norm > 0.0).then_some(TapSet { indices, weights })
    }
}

fn centered_fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut transformed = shift2(input, true);
    transform_axis(&mut transformed, Axis(0));
    transform_axis(&mut transformed, Axis(1));
    shift2(&transformed, false)
}

fn transform_axis(data: &mut Array2<Complex32>, axis: Axis) {
    if axis.index() == 0 {
        transform_rows(data);
    } else {
        transform_columns(data);
    }
}

fn fft(len: usize) -> FftPlan {
    let mut cache = FFT_CACHE.lock().expect("FFT cache lock poisoned");
    if let Some(fft) = cache.get(&(len, false)) {
        return Arc::clone(fft);
    }
    let fft = FftPlanner::<f32>::new().plan_fft_forward(len);
    cache.insert((len, false), Arc::clone(&fft));
    fft
}

fn transform_rows(data: &mut Array2<Complex32>) {
    let fft = fft(data.shape()[1]);
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

fn transform_columns(data: &mut Array2<Complex32>) {
    let [row_count, column_count]: [usize; 2] = data
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let fft = fft(row_count);
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

fn shift2(input: &Array2<Complex32>, inverse: bool) -> Array2<Complex32> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let mut output = Array2::<Complex32>::zeros((nx, ny));
    let x_shift = if inverse { nx.div_ceil(2) } else { nx / 2 };
    let y_shift = if inverse { ny.div_ceil(2) } else { ny / 2 };
    for x in 0..nx {
        for y in 0..ny {
            output[(x, y)] = input[((x + x_shift) % nx, (y + y_shift) % ny)];
        }
    }
    output
}

fn build_normalized_tap_weights(
    kernel_table: &[f32],
    oversampling: usize,
) -> Vec<[f32; GRIDDER_TAP_COUNT]> {
    let half = oversampling as isize / 2;
    (-half..=half)
        .map(|offset| {
            let mut weights = [0.0f32; GRIDDER_TAP_COUNT];
            let mut norm = 0.0f32;
            for (tap, delta) in
                (-(GRIDDER_SUPPORT as isize)..=(GRIDDER_SUPPORT as isize)).enumerate()
            {
                let lookup = (delta * oversampling as isize + offset).unsigned_abs();
                let weight = kernel_table.get(lookup).copied().unwrap_or(0.0);
                weights[tap] = weight;
                norm += weight;
            }
            if norm > 0.0 {
                for weight in &mut weights {
                    *weight /= norm;
                }
            }
            weights
        })
        .collect()
}

fn flatten_tap_products(x_taps: &TapSet, y_taps: &TapSet, stride: usize) -> ProductTapSet {
    let mut flat_indices = [0usize; GRIDDER_PRODUCT_TAP_COUNT];
    let mut weights = [0.0f32; GRIDDER_PRODUCT_TAP_COUNT];
    let mut slot = 0usize;
    for x_tap in 0..GRIDDER_TAP_COUNT {
        for y_tap in 0..GRIDDER_TAP_COUNT {
            flat_indices[slot] = x_taps.indices[x_tap] * stride + y_taps.indices[y_tap];
            weights[slot] = x_taps.weights[x_tap] * y_taps.weights[y_tap];
            slot += 1;
        }
    }
    ProductTapSet {
        flat_indices,
        weights,
    }
}

fn padded_len(image_len: usize, padding_factor: f64) -> usize {
    let padded = (padding_factor * image_len as f64 - 0.5)
        .floor()
        .max(image_len as f64) as usize;
    if padded % 2 == 0 { padded } else { padded + 1 }
}

fn casa_composite_padded_len(image_len: usize, padding_factor: f64) -> usize {
    let mut padded = padded_len(image_len, padding_factor);
    while !is_casa_composite_len(padded) {
        padded += 2;
    }
    padded
}

fn is_casa_composite_len(mut value: usize) -> bool {
    for factor in [2, 3, 5] {
        while value > 1 && value % factor == 0 {
            value /= factor;
        }
    }
    value == 1
}

fn build_correction_axis(size: usize) -> Vec<f32> {
    let center = size as f64 / 2.0;
    (0..size)
        .map(|index| {
            let nu = ((index as f64 - center).abs() / center).clamp(0.0, 1.0);
            let value = grdsf(nu);
            if value > 1.0e-6 {
                (1.0 / value) as f32
            } else {
                0.0
            }
        })
        .collect()
}

fn spheroidal_kernel(distance: f64, support: f64) -> f32 {
    if !(distance.is_finite() && distance <= support) {
        return 0.0;
    }
    let nu = distance / support;
    if nu > 1.0 {
        return 0.0;
    }
    ((1.0 - nu * nu) * grdsf(nu)) as f32
}

fn grdsf(nu: f64) -> f64 {
    const P0: [f64; 5] = [
        8.203_343e-2,
        -3.644_705e-1,
        6.278_660e-1,
        -5.335_581e-1,
        2.312_756e-1,
    ];
    const P1: [f64; 5] = [
        4.028_559e-3,
        -3.697_768e-2,
        1.021_332e-1,
        -1.201_436e-1,
        6.412_774e-2,
    ];
    const Q0: [f64; 3] = [1.0, 8.212_018e-1, 2.078_043e-1];
    const Q1: [f64; 3] = [1.0, 9.599_102e-1, 2.918_724e-1];
    if !(0.0..=1.0).contains(&nu) {
        return 0.0;
    }
    let (p, q, nu_end) = if nu < 0.75 {
        (&P0, &Q0, 0.75)
    } else {
        (&P1, &Q1, 1.0)
    };
    let delta_nu_sq = nu * nu - nu_end * nu_end;
    let numerator = p.iter().enumerate().fold(0.0, |sum, (order, coefficient)| {
        sum + coefficient * delta_nu_sq.powi(order as i32)
    });
    let denominator = q.iter().enumerate().fold(0.0, |sum, (order, coefficient)| {
        sum + coefficient * delta_nu_sq.powi(order as i32)
    });
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn airy_voltage_pattern_from_table_index(
    index: f64,
    maximum_radius_arcmin_ghz: f64,
    dish_diameter_m: f64,
    blockage_diameter_m: f64,
) -> f32 {
    let sample_count_minus_one = 9_999.0;
    let dimensionless_max_radius =
        maximum_radius_arcmin_ghz * 7.016 / (1.566 * 60.0) * dish_diameter_m / 24.5;
    let x = index * dimensionless_max_radius / sample_count_minus_one;
    if x.abs() <= f64::EPSILON {
        return 1.0;
    }
    if blockage_diameter_m <= 0.0 {
        return (2.0 * j1(x) / x) as f32;
    }
    let area_ratio = (dish_diameter_m / blockage_diameter_m).powi(2);
    let area_norm = area_ratio - 1.0;
    let length_ratio = dish_diameter_m / blockage_diameter_m;
    ((area_ratio * 2.0 * j1(x) / x - 2.0 * j1(x * length_ratio) / (x * length_ratio)) / area_norm)
        as f32
}

fn casa_airy_max_radius_arcmin_ghz(dish_diameter_m: f64, blockage_diameter_m: f64) -> f64 {
    if approx_eq(dish_diameter_m, 10.7, 1.0e-6) && approx_eq(blockage_diameter_m, 0.75, 1.0e-6) {
        250.0
    } else if approx_eq(dish_diameter_m, 6.25, 1.0e-6)
        && approx_eq(blockage_diameter_m, 0.75, 1.0e-6)
    {
        500.0
    } else {
        1.784 * 60.0
    }
}

fn approx_eq(left: f64, right: f64, tolerance: f64) -> bool {
    (left - right).abs() <= tolerance
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_model_predicts_zero() {
        let geometry = ImageGeometry {
            image_shape: [16, 16],
            cell_size_rad: [1.0e-5; 2],
        };
        let predictor =
            StandardMfsModelPredictor::new(geometry, &Array2::zeros((16, 16))).expect("predictor");
        assert_eq!(predictor.predict(10.0, -20.0), Complex32::new(0.0, 0.0));
    }

    #[test]
    fn airy_pattern_is_unity_at_boresight_and_zero_outside_support() {
        let pattern = AiryVoltagePattern::new(AiryPrimaryBeam {
            dish_diameter_m: 12.0,
            blockage_diameter_m: 0.75,
        });
        assert_eq!(pattern.evaluate_offsets(0.0, 0.0, 100.0e9), 1.0);
        assert_eq!(pattern.evaluate_offsets(1.0, 0.0, 100.0e9), 0.0);
    }
}
