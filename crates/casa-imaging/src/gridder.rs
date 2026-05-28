// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concrete prolate-spheroidal gridding and degridding helpers.

use ndarray::Array2;
use num_complex::{Complex32, Complex64};
use std::collections::HashMap;

use crate::{
    ImageGeometry, ImagingError,
    fft::{centered_fft2_f64, fft2},
};

pub(crate) const STANDARD_GRIDDER_SUPPORT: usize = 3;
pub(crate) const STANDARD_GRIDDER_TAP_COUNT: usize = STANDARD_GRIDDER_SUPPORT * 2 + 1;
const GRIDDER_SUPPORT: usize = STANDARD_GRIDDER_SUPPORT;
const GRIDDER_TAP_COUNT: usize = STANDARD_GRIDDER_TAP_COUNT;
const GRIDDER_PRODUCT_TAP_COUNT: usize = GRIDDER_TAP_COUNT * GRIDDER_TAP_COUNT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DensityCellConvention {
    VisImagingWeight,
    CubeBriggsWeightorDensity,
    CubeBriggsWeightorLookup,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(crate) struct DensityGridCoordinateParams {
    pub(crate) center_x: f64,
    pub(crate) center_y: f64,
    pub(crate) u_scale: f64,
    pub(crate) v_scale: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TapSet {
    pub(crate) indices: [usize; GRIDDER_TAP_COUNT],
    pub(crate) weights: [f32; GRIDDER_TAP_COUNT],
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProductTapSet {
    pub(crate) flat_indices: [usize; GRIDDER_PRODUCT_TAP_COUNT],
    pub(crate) weights: [f32; GRIDDER_PRODUCT_TAP_COUNT],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TapAxisSpan {
    pub(crate) start: usize,
    pub(crate) weight_index: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PositiveTapSet {
    pub(crate) x: TapAxisSpan,
    pub(crate) y: TapAxisSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PositiveTapKey {
    x_start: usize,
    y_start: usize,
    x_weight_index: usize,
    y_weight_index: usize,
}

impl PositiveTapSet {
    pub(crate) fn census_key(&self) -> PositiveTapKey {
        PositiveTapKey {
            x_start: self.x.start,
            y_start: self.y.start,
            x_weight_index: self.x.weight_index,
            y_weight_index: self.y.weight_index,
        }
    }

    pub(crate) fn center(&self) -> [usize; 2] {
        [self.x.center(), self.y.center()]
    }
}

impl TapAxisSpan {
    pub(crate) fn center(&self) -> usize {
        self.start + GRIDDER_SUPPORT
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StandardMfsTapSkipReason {
    NotGridable,
    InvalidWeight,
    InvalidSumwt,
    OutOfGrid,
    NonfiniteVisibility,
}

#[derive(Debug)]
pub(crate) struct StandardMfsTapCensus {
    label: &'static str,
    max_unique_keys: Option<usize>,
    accepted_samples: usize,
    skipped_not_gridable: usize,
    skipped_invalid_weight: usize,
    skipped_invalid_sumwt: usize,
    skipped_out_of_grid: usize,
    skipped_nonfinite_visibility: usize,
    unique_counts: HashMap<PositiveTapKey, usize>,
    unique_truncated: bool,
    adjacent_runs: Vec<usize>,
    current_run_key: Option<PositiveTapKey>,
    current_run_len: usize,
}

impl StandardMfsTapCensus {
    pub(crate) fn new(label: &'static str) -> Option<Self> {
        std::env::var_os("CASA_RS_STANDARD_MFS_TAP_CENSUS").map(|mode| {
            let exact = mode
                .to_str()
                .is_some_and(|value| value.eq_ignore_ascii_case("exact"));
            let max_unique_keys = if exact {
                None
            } else {
                Some(
                    std::env::var("CASA_RS_STANDARD_MFS_TAP_CENSUS_MAX_KEYS")
                        .ok()
                        .and_then(|value| value.parse::<usize>().ok())
                        .filter(|value| *value > 0)
                        .unwrap_or(1_000_000),
                )
            };
            Self {
                label,
                max_unique_keys,
                accepted_samples: 0,
                skipped_not_gridable: 0,
                skipped_invalid_weight: 0,
                skipped_invalid_sumwt: 0,
                skipped_out_of_grid: 0,
                skipped_nonfinite_visibility: 0,
                unique_counts: HashMap::new(),
                unique_truncated: false,
                adjacent_runs: Vec::new(),
                current_run_key: None,
                current_run_len: 0,
            }
        })
    }

    pub(crate) fn observe_accepted(&mut self, taps: &PositiveTapSet) {
        let key = taps.census_key();
        self.accepted_samples += 1;
        if let Some(count) = self.unique_counts.get_mut(&key) {
            *count += 1;
        } else if self
            .max_unique_keys
            .is_none_or(|max_unique_keys| self.unique_counts.len() < max_unique_keys)
        {
            self.unique_counts.insert(key, 1);
        } else {
            self.unique_truncated = true;
        }
        if self.current_run_key == Some(key) {
            self.current_run_len += 1;
            return;
        }
        self.flush_current_run();
        self.current_run_key = Some(key);
        self.current_run_len = 1;
    }

    pub(crate) fn observe_skip(&mut self, reason: StandardMfsTapSkipReason) {
        match reason {
            StandardMfsTapSkipReason::NotGridable => self.skipped_not_gridable += 1,
            StandardMfsTapSkipReason::InvalidWeight => self.skipped_invalid_weight += 1,
            StandardMfsTapSkipReason::InvalidSumwt => self.skipped_invalid_sumwt += 1,
            StandardMfsTapSkipReason::OutOfGrid => self.skipped_out_of_grid += 1,
            StandardMfsTapSkipReason::NonfiniteVisibility => {
                self.skipped_nonfinite_visibility += 1;
            }
        }
    }

    pub(crate) fn log(mut self, current_planned_sample_bytes: usize) {
        self.flush_current_run();
        let unique_taps = self.unique_counts.len();
        let duplicate_samples = self.accepted_samples.saturating_sub(unique_taps);
        let compact_plan_bytes = self
            .accepted_samples
            .saturating_mul(std::mem::size_of::<usize>() * 4 + std::mem::size_of::<f32>() * 2);
        let current_plan_bytes = self
            .accepted_samples
            .saturating_mul(current_planned_sample_bytes);
        let (p50, p90, p99, max_run) = percentile_summary(&mut self.adjacent_runs);
        eprintln!(
            "standard_mfs_tap_census label={} accepted={} unique_taps={} unique_taps_truncated={} duplicate_samples={} adjacent_runs={} adjacent_run_p50={} adjacent_run_p90={} adjacent_run_p99={} adjacent_run_max={} skipped_not_gridable={} skipped_invalid_weight={} skipped_invalid_sumwt={} skipped_out_of_grid={} skipped_nonfinite_visibility={} current_plan_bytes={} compact_plan_bytes_estimate={}",
            self.label,
            self.accepted_samples,
            unique_taps,
            self.unique_truncated,
            duplicate_samples,
            self.adjacent_runs.len(),
            p50,
            p90,
            p99,
            max_run,
            self.skipped_not_gridable,
            self.skipped_invalid_weight,
            self.skipped_invalid_sumwt,
            self.skipped_out_of_grid,
            self.skipped_nonfinite_visibility,
            current_plan_bytes,
            compact_plan_bytes,
        );
    }

    fn flush_current_run(&mut self) {
        if self.current_run_len > 0 {
            self.adjacent_runs.push(self.current_run_len);
            self.current_run_len = 0;
            self.current_run_key = None;
        }
    }
}

fn percentile_summary(values: &mut [usize]) -> (usize, usize, usize, usize) {
    if values.is_empty() {
        return (0, 0, 0, 0);
    }
    values.sort_unstable();
    let p50 = percentile_sorted(values, 50);
    let p90 = percentile_sorted(values, 90);
    let p99 = percentile_sorted(values, 99);
    let max = values.last().copied().unwrap_or(0);
    (p50, p90, p99, max)
}

fn percentile_sorted(values: &[usize], percentile: usize) -> usize {
    debug_assert!(!values.is_empty());
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index]
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PlannedSample {
    pub(crate) positive_x: TapSet,
    pub(crate) positive_y: TapSet,
    pub(crate) negative_x: TapSet,
    pub(crate) negative_y: TapSet,
    pub(crate) positive: ProductTapSet,
    pub(crate) negative: ProductTapSet,
}

pub(crate) struct StandardGridder {
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
    density_center_x: f64,
    density_center_y: f64,
    density_u_scale: f64,
    density_v_scale: f64,
    density_limit_x: isize,
    density_limit_y: isize,
}

impl StandardGridder {
    pub(crate) fn new(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Self::new_with_padding(geometry, casa_composite_padded_len)
    }

    pub(crate) fn new_unpadded(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Self::new_with_padding_factor(geometry, |len, _| len, 1.0)
    }

    pub(crate) fn new_with_casa_composite_padding(
        geometry: ImageGeometry,
    ) -> Result<Self, ImagingError> {
        Self::new_with_padding_factor(geometry, casa_composite_padded_len, 1.3)
    }

    fn new_with_padding(
        geometry: ImageGeometry,
        padded_len_for_axis: fn(usize, f64) -> usize,
    ) -> Result<Self, ImagingError> {
        Self::new_with_padding_factor(geometry, padded_len_for_axis, 1.2)
    }

    fn new_with_padding_factor(
        geometry: ImageGeometry,
        padded_len_for_axis: fn(usize, f64) -> usize,
        padding: f64,
    ) -> Result<Self, ImagingError> {
        geometry.validate()?;

        let grid_shape = [
            padded_len_for_axis(geometry.nx(), padding),
            padded_len_for_axis(geometry.ny(), padding),
        ];
        let image_blc = [
            (grid_shape[0] - geometry.nx() + (grid_shape[0] % 2 == 0) as usize) / 2,
            (grid_shape[1] - geometry.ny() + (grid_shape[1] % 2 == 0) as usize) / 2,
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

        let density_center_x = geometry.nx() as f64 / 2.0;
        let density_center_y = geometry.ny() as f64 / 2.0;
        let density_u_scale = geometry.nx() as f64 * geometry.cell_size_rad[0];
        let density_v_scale = geometry.ny() as f64 * geometry.cell_size_rad[1];
        Ok(Self {
            du_lambda: 1.0 / (grid_shape[0] as f64 * geometry.cell_size_rad[0]),
            dv_lambda: 1.0 / (grid_shape[1] as f64 * geometry.cell_size_rad[1]),
            geometry,
            grid_shape,
            image_blc,
            oversampling,
            kernel_table,
            normalized_tap_weights,
            correction_x,
            correction_y,
            density_center_x,
            density_center_y,
            density_u_scale,
            density_v_scale,
            density_limit_x: geometry.nx() as isize,
            density_limit_y: geometry.ny() as isize,
        })
    }

    pub(crate) fn grid_shape(&self) -> [usize; 2] {
        self.grid_shape
    }

    pub(crate) fn geometry(&self) -> ImageGeometry {
        self.geometry
    }

    pub(crate) fn positive_tap_grid_center(&self) -> [usize; 2] {
        self.locate_positive_tap_center(0.0, 0.0)
            .unwrap_or([self.grid_shape[0] / 2, self.grid_shape[1] / 2])
    }

    pub(crate) fn grid_spacing_lambda(&self) -> [f64; 2] {
        [self.du_lambda, self.dv_lambda]
    }

    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub(crate) fn oversampling(&self) -> usize {
        self.oversampling
    }

    pub(crate) fn positive_tap_halo(&self) -> usize {
        GRIDDER_SUPPORT
    }

    pub(crate) fn density_grid_shape(&self) -> [usize; 2] {
        [self.geometry.nx(), self.geometry.ny()]
    }

    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub(crate) fn density_grid_coordinate_params(&self) -> DensityGridCoordinateParams {
        DensityGridCoordinateParams {
            center_x: self.density_center_x,
            center_y: self.density_center_y,
            u_scale: self.density_u_scale,
            v_scale: self.density_v_scale,
        }
    }

    #[cfg(test)]
    pub(crate) fn grid_sample(
        &self,
        grid: &mut Array2<Complex32>,
        u_lambda: f64,
        v_lambda: f64,
        value: Complex32,
    ) -> bool {
        let Some(plan) = self.plan_sample(u_lambda, v_lambda) else {
            return false;
        };
        self.grid_sample_planned(grid, &plan.positive_x, &plan.positive_y, value);
        true
    }

    #[cfg(test)]
    pub(crate) fn degrid_sample(
        &self,
        grid: &Array2<Complex32>,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<Complex32> {
        let plan = self.plan_sample(u_lambda, v_lambda)?;
        Some(self.degrid_sample_planned(grid, &plan.positive_x, &plan.positive_y))
    }

    pub(crate) fn plan_sample(&self, u_lambda: f64, v_lambda: f64) -> Option<PlannedSample> {
        let positive_x = self.sample_taps(self.grid_coordinate_x(u_lambda), self.grid_shape[0])?;
        let positive_y = self.sample_taps(self.grid_coordinate_y(v_lambda), self.grid_shape[1])?;
        let negative_x = self.sample_taps(self.grid_coordinate_x(-u_lambda), self.grid_shape[0])?;
        let negative_y = self.sample_taps(self.grid_coordinate_y(-v_lambda), self.grid_shape[1])?;
        let grid_stride = self.grid_shape[1];
        Some(PlannedSample {
            positive_x,
            positive_y,
            negative_x,
            negative_y,
            positive: flatten_tap_products(&positive_x, &positive_y, grid_stride),
            negative: flatten_tap_products(&negative_x, &negative_y, grid_stride),
        })
    }

    #[inline]
    pub(crate) fn plan_positive_taps(
        &self,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<PositiveTapSet> {
        Some(PositiveTapSet {
            x: self.sample_tap_span(self.grid_coordinate_x(u_lambda), self.grid_shape[0])?,
            y: self.sample_tap_span(self.grid_coordinate_y(v_lambda), self.grid_shape[1])?,
        })
    }

    #[inline]
    pub(crate) fn locate_positive_tap_center(
        &self,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<[usize; 2]> {
        Some([
            self.sample_tap_center(self.grid_coordinate_x(u_lambda), self.grid_shape[0])?,
            self.sample_tap_center(self.grid_coordinate_y(v_lambda), self.grid_shape[1])?,
        ])
    }

    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub(crate) fn positive_tap_grid_coordinates(
        &self,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<[f32; 2]> {
        let x = self.grid_coordinate_x(u_lambda);
        let y = self.grid_coordinate_y(v_lambda);
        (x.is_finite() && y.is_finite()).then_some([x as f32, y as f32])
    }

    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub(crate) fn positive_tap_axis_weights(
        &self,
        taps: &PositiveTapSet,
    ) -> (
        [f32; STANDARD_GRIDDER_TAP_COUNT],
        [f32; STANDARD_GRIDDER_TAP_COUNT],
    ) {
        (
            self.normalized_tap_weights[taps.x.weight_index],
            self.normalized_tap_weights[taps.y.weight_index],
        )
    }

    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub(crate) fn normalized_tap_weights(&self) -> &[[f32; STANDARD_GRIDDER_TAP_COUNT]] {
        &self.normalized_tap_weights
    }

    #[allow(dead_code)]
    pub(crate) fn grid_sample_planned(
        &self,
        grid: &mut Array2<Complex32>,
        x_taps: &TapSet,
        y_taps: &TapSet,
        value: Complex32,
    ) {
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = x_taps.indices[x_tap];
            let x_weight = x_taps.weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let y_index = y_taps.indices[y_tap];
                let y_weight = y_taps.weights[y_tap];
                grid[(x_index, y_index)] += value * (x_weight * y_weight);
            }
        }
    }

    pub(crate) fn grid_sample_product_planned(
        &self,
        grid: &mut Array2<Complex32>,
        taps: &ProductTapSet,
        value: Complex32,
    ) {
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                let weight = taps.weights[tap];
                let cell = &mut storage[taps.flat_indices[tap]];
                cell.re += value.re * weight;
                cell.im += value.im * weight;
            }
            return;
        }
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            let flat_index = taps.flat_indices[tap];
            let weight = taps.weights[tap];
            let cell = &mut grid[(
                flat_index / self.grid_shape[1],
                flat_index % self.grid_shape[1],
            )];
            cell.re += value.re * weight;
            cell.im += value.im * weight;
        }
    }

    pub(crate) fn grid_sample_product_planned_f64(
        &self,
        grid: &mut Array2<Complex64>,
        taps: &ProductTapSet,
        value: Complex64,
    ) {
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                let weight = f64::from(taps.weights[tap]);
                let cell = &mut storage[taps.flat_indices[tap]];
                cell.re += value.re * weight;
                cell.im += value.im * weight;
            }
            return;
        }
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            let flat_index = taps.flat_indices[tap];
            let weight = f64::from(taps.weights[tap]);
            let cell = &mut grid[(
                flat_index / self.grid_shape[1],
                flat_index % self.grid_shape[1],
            )];
            cell.re += value.re * weight;
            cell.im += value.im * weight;
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_planned_f64(
        &self,
        grid: &mut Array2<Complex64>,
        taps: &PositiveTapSet,
        value: Complex64,
    ) {
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            let grid_stride = self.grid_shape[1];
            let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
            let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = f64::from(x_weight * y_weights[y_tap]);
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < storage.len());
                    // `sample_taps` only produces in-bounds grid coordinates.
                    let cell = unsafe { storage.get_unchecked_mut(index) };
                    cell.re += value.re * weight;
                    cell.im += value.im * weight;
                }
            }
            return;
        }
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let cell = &mut grid[(x_index, taps.y.start + y_tap)];
                cell.re += value.re * weight;
                cell.im += value.im * weight;
            }
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_planned_f64_storage(
        &self,
        storage: &mut [Complex64],
        taps: &PositiveTapSet,
        value: Complex64,
    ) {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (taps.x.start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let index = x_index + taps.y.start + y_tap;
                debug_assert!(index < storage.len());
                let cell = unsafe { storage.get_unchecked_mut(index) };
                cell.re += value.re * weight;
                cell.im += value.im * weight;
            }
        }
    }

    #[allow(clippy::needless_range_loop, clippy::too_many_arguments)]
    pub(crate) fn grid_compact_taps_planned_f64_storage(
        &self,
        storage: &mut [Complex64],
        value: Complex64,
        x_start: usize,
        y_start: usize,
        x_weight_index: usize,
        y_weight_index: usize,
    ) {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[x_weight_index];
        let y_weights = &self.normalized_tap_weights[y_weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (x_start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let index = x_index + y_start + y_tap;
                debug_assert!(index < storage.len());
                let cell = unsafe { storage.get_unchecked_mut(index) };
                cell.re += value.re * weight;
                cell.im += value.im * weight;
            }
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_planned_f64_with_offset(
        &self,
        grid: &mut Array2<Complex64>,
        taps: &PositiveTapSet,
        value: Complex64,
        offset: [usize; 2],
    ) {
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        let local_stride = grid.shape()[1];
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = taps.x.start + x_tap - offset[0];
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let y_index = taps.y.start + y_tap - offset[1];
                    let weight = f64::from(x_weight * y_weights[y_tap]);
                    let index = x_index * local_stride + y_index;
                    debug_assert!(index < storage.len());
                    let cell = unsafe { storage.get_unchecked_mut(index) };
                    cell.re += value.re * weight;
                    cell.im += value.im * weight;
                }
            }
            return;
        }
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap - offset[0];
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let y_index = taps.y.start + y_tap - offset[1];
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let cell = &mut grid[(x_index, y_index)];
                cell.re += value.re * weight;
                cell.im += value.im * weight;
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn grid_sample_product_real_planned_f64(
        &self,
        grid: &mut Array2<Complex64>,
        taps: &ProductTapSet,
        value: f64,
    ) {
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                storage[taps.flat_indices[tap]].re += value * f64::from(taps.weights[tap]);
            }
            return;
        }
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            let flat_index = taps.flat_indices[tap];
            grid[(
                flat_index / self.grid_shape[1],
                flat_index % self.grid_shape[1],
            )]
                .re += value * f64::from(taps.weights[tap]);
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_real_planned_f64(
        &self,
        grid: &mut Array2<Complex64>,
        taps: &PositiveTapSet,
        value: f64,
    ) {
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            let grid_stride = self.grid_shape[1];
            let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
            let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < storage.len());
                    // `sample_taps` only produces in-bounds grid coordinates.
                    unsafe {
                        storage.get_unchecked_mut(index).re +=
                            value * f64::from(x_weight * y_weights[y_tap]);
                    }
                }
            }
            return;
        }
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                grid[(x_index, taps.y.start + y_tap)].re +=
                    value * f64::from(x_weight * y_weights[y_tap]);
            }
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_real_planned_f64_storage(
        &self,
        storage: &mut [Complex64],
        taps: &PositiveTapSet,
        value: f64,
    ) {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (taps.x.start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let index = x_index + taps.y.start + y_tap;
                debug_assert!(index < storage.len());
                unsafe {
                    storage.get_unchecked_mut(index).re +=
                        value * f64::from(x_weight * y_weights[y_tap]);
                }
            }
        }
    }

    #[allow(clippy::needless_range_loop, clippy::too_many_arguments)]
    pub(crate) fn grid_compact_taps_real_planned_f64_storage(
        &self,
        storage: &mut [Complex64],
        value: f64,
        x_start: usize,
        y_start: usize,
        x_weight_index: usize,
        y_weight_index: usize,
    ) {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[x_weight_index];
        let y_weights = &self.normalized_tap_weights[y_weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (x_start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let index = x_index + y_start + y_tap;
                debug_assert!(index < storage.len());
                unsafe {
                    storage.get_unchecked_mut(index).re +=
                        value * f64::from(x_weight * y_weights[y_tap]);
                }
            }
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_real_planned_f64_with_offset(
        &self,
        grid: &mut Array2<Complex64>,
        taps: &PositiveTapSet,
        value: f64,
        offset: [usize; 2],
    ) {
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        let local_stride = grid.shape()[1];
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = taps.x.start + x_tap - offset[0];
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let y_index = taps.y.start + y_tap - offset[1];
                    let index = x_index * local_stride + y_index;
                    debug_assert!(index < storage.len());
                    unsafe {
                        storage.get_unchecked_mut(index).re +=
                            value * f64::from(x_weight * y_weights[y_tap]);
                    }
                }
            }
            return;
        }
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap - offset[0];
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let y_index = taps.y.start + y_tap - offset[1];
                grid[(x_index, y_index)].re += value * f64::from(x_weight * y_weights[y_tap]);
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn grid_sample_product_pair_planned_f64(
        &self,
        first_grid: &mut Array2<Complex64>,
        first_value: Complex64,
        second_grid: &mut Array2<Complex64>,
        second_value: Complex64,
        taps: &ProductTapSet,
    ) {
        if let (Some(first_storage), Some(second_storage)) = (
            first_grid.as_slice_memory_order_mut(),
            second_grid.as_slice_memory_order_mut(),
        ) {
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                let weight = f64::from(taps.weights[tap]);
                let index = taps.flat_indices[tap];
                let first_cell = &mut first_storage[index];
                first_cell.re += first_value.re * weight;
                first_cell.im += first_value.im * weight;
                let second_cell = &mut second_storage[index];
                second_cell.re += second_value.re * weight;
                second_cell.im += second_value.im * weight;
            }
            return;
        }
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            let weight = f64::from(taps.weights[tap]);
            let flat_index = taps.flat_indices[tap];
            let index = (
                flat_index / self.grid_shape[1],
                flat_index % self.grid_shape[1],
            );
            let first_cell = &mut first_grid[index];
            first_cell.re += first_value.re * weight;
            first_cell.im += first_value.im * weight;
            let second_cell = &mut second_grid[index];
            second_cell.re += second_value.re * weight;
            second_cell.im += second_value.im * weight;
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_real_complex_pair_planned_f64(
        &self,
        real_grid: &mut Array2<Complex64>,
        real_value: f64,
        complex_grid: &mut Array2<Complex64>,
        complex_value: Complex64,
        taps: &PositiveTapSet,
    ) {
        if let (Some(real_storage), Some(complex_storage)) = (
            real_grid.as_slice_memory_order_mut(),
            complex_grid.as_slice_memory_order_mut(),
        ) {
            let grid_stride = self.grid_shape[1];
            let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
            let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = f64::from(x_weight * y_weights[y_tap]);
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < real_storage.len());
                    debug_assert!(index < complex_storage.len());
                    // `sample_taps` only produces in-bounds grid coordinates.
                    unsafe {
                        real_storage.get_unchecked_mut(index).re += real_value * weight;
                        let complex_cell = complex_storage.get_unchecked_mut(index);
                        complex_cell.re += complex_value.re * weight;
                        complex_cell.im += complex_value.im * weight;
                    }
                }
            }
            return;
        }
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let index = (x_index, taps.y.start + y_tap);
                real_grid[index].re += real_value * weight;
                let complex_cell = &mut complex_grid[index];
                complex_cell.re += complex_value.re * weight;
                complex_cell.im += complex_value.im * weight;
            }
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_real_complex_pair_planned_f64_storage(
        &self,
        real_storage: &mut [Complex64],
        real_value: f64,
        complex_storage: &mut [Complex64],
        complex_value: Complex64,
        taps: &PositiveTapSet,
    ) {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (taps.x.start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let index = x_index + taps.y.start + y_tap;
                debug_assert!(index < real_storage.len());
                debug_assert!(index < complex_storage.len());
                unsafe {
                    real_storage.get_unchecked_mut(index).re += real_value * weight;
                    let complex_cell = complex_storage.get_unchecked_mut(index);
                    complex_cell.re += complex_value.re * weight;
                    complex_cell.im += complex_value.im * weight;
                }
            }
        }
    }

    #[allow(clippy::needless_range_loop, clippy::too_many_arguments)]
    pub(crate) fn grid_compact_taps_real_complex_pair_planned_f64_storage(
        &self,
        real_storage: &mut [Complex64],
        real_value: f64,
        complex_storage: &mut [Complex64],
        complex_value: Complex64,
        x_start: usize,
        y_start: usize,
        x_weight_index: usize,
        y_weight_index: usize,
    ) {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[x_weight_index];
        let y_weights = &self.normalized_tap_weights[y_weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (x_start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let index = x_index + y_start + y_tap;
                debug_assert!(index < real_storage.len());
                debug_assert!(index < complex_storage.len());
                unsafe {
                    real_storage.get_unchecked_mut(index).re += real_value * weight;
                    let complex_cell = complex_storage.get_unchecked_mut(index);
                    complex_cell.re += complex_value.re * weight;
                    complex_cell.im += complex_value.im * weight;
                }
            }
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn grid_sample_taps_real_complex_pair_planned_f64_with_offset(
        &self,
        real_grid: &mut Array2<Complex64>,
        real_value: f64,
        complex_grid: &mut Array2<Complex64>,
        complex_value: Complex64,
        taps: &PositiveTapSet,
        offset: [usize; 2],
    ) {
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        let local_stride = real_grid.shape()[1];
        if let (Some(real_storage), Some(complex_storage)) = (
            real_grid.as_slice_memory_order_mut(),
            complex_grid.as_slice_memory_order_mut(),
        ) {
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = taps.x.start + x_tap - offset[0];
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let y_index = taps.y.start + y_tap - offset[1];
                    let weight = f64::from(x_weight * y_weights[y_tap]);
                    let index = x_index * local_stride + y_index;
                    debug_assert!(index < real_storage.len());
                    debug_assert!(index < complex_storage.len());
                    unsafe {
                        real_storage.get_unchecked_mut(index).re += real_value * weight;
                        let complex_cell = complex_storage.get_unchecked_mut(index);
                        complex_cell.re += complex_value.re * weight;
                        complex_cell.im += complex_value.im * weight;
                    }
                }
            }
            return;
        }
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap - offset[0];
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let y_index = taps.y.start + y_tap - offset[1];
                let weight = f64::from(x_weight * y_weights[y_tap]);
                real_grid[(x_index, y_index)].re += real_value * weight;
                let complex_cell = &mut complex_grid[(x_index, y_index)];
                complex_cell.re += complex_value.re * weight;
                complex_cell.im += complex_value.im * weight;
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn degrid_sample_planned(
        &self,
        grid: &Array2<Complex32>,
        x_taps: &TapSet,
        y_taps: &TapSet,
    ) -> Complex32 {
        let mut value = Complex32::new(0.0, 0.0);
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = x_taps.indices[x_tap];
            let x_weight = x_taps.weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let y_index = y_taps.indices[y_tap];
                let y_weight = y_taps.weights[y_tap];
                value += grid[(x_index, y_index)] * (x_weight * y_weight);
            }
        }
        value
    }

    pub(crate) fn degrid_sample_product_planned(
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

    pub(crate) fn degrid_sample_product_planned_sectdgrid(
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
        if norm > 0.0 && norm.is_finite() {
            Some(value / norm)
        } else {
            None
        }
    }

    pub(crate) fn degrid_sample_product_planned_normalized(
        &self,
        grid: &Array2<Complex32>,
        taps: &ProductTapSet,
    ) -> Complex32 {
        debug_assert!((taps.weights.iter().sum::<f32>() - 1.0).abs() <= 1.0e-5);
        self.degrid_sample_product_planned(grid, taps)
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn degrid_sample_taps_planned_normalized(
        &self,
        grid: &Array2<Complex32>,
        taps: &PositiveTapSet,
    ) -> Complex32 {
        if let Some(storage) = grid.as_slice_memory_order() {
            let grid_stride = self.grid_shape[1];
            let mut value = Complex32::new(0.0, 0.0);
            let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
            let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < storage.len());
                    // `sample_taps` only produces in-bounds grid coordinates.
                    value +=
                        unsafe { *storage.get_unchecked(index) } * (x_weight * y_weights[y_tap]);
                }
            }
            return value;
        }
        let mut value = Complex32::new(0.0, 0.0);
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                value += grid[(x_index, taps.y.start + y_tap)] * (x_weight * y_weights[y_tap]);
            }
        }
        value
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn degrid_model_and_grid_residual_taps_planned_f64(
        &self,
        model_grid: &Array2<Complex32>,
        residual_grid: &mut Array2<Complex64>,
        taps: &PositiveTapSet,
        observed_visibility: Complex32,
        residual_weight: f64,
    ) -> Complex32 {
        debug_assert_eq!(model_grid.shape(), self.grid_shape.as_slice());
        debug_assert_eq!(residual_grid.shape(), self.grid_shape.as_slice());

        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        let mut predicted_re = 0.0f32;
        let mut predicted_im = 0.0f32;

        if let Some(model_storage) = model_grid.as_slice_memory_order() {
            let grid_stride = self.grid_shape[1];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < model_storage.len());
                    // `sample_taps` only produces in-bounds grid coordinates.
                    let weight = x_weight * y_weights[y_tap];
                    let cell = unsafe { *model_storage.get_unchecked(index) };
                    predicted_re += cell.re * weight;
                    predicted_im += cell.im * weight;
                }
            }
        } else {
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = taps.x.start + x_tap;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = x_weight * y_weights[y_tap];
                    let cell = model_grid[(x_index, taps.y.start + y_tap)];
                    predicted_re += cell.re * weight;
                    predicted_im += cell.im * weight;
                }
            }
        }
        let predicted_visibility = Complex32::new(predicted_re, predicted_im);

        let residual_visibility = observed_visibility - predicted_visibility;
        let residual = Complex64::new(
            f64::from(residual_visibility.re) * residual_weight,
            f64::from(residual_visibility.im) * residual_weight,
        );

        if let Some(residual_storage) = residual_grid.as_slice_memory_order_mut() {
            let grid_stride = self.grid_shape[1];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = f64::from(x_weight * y_weights[y_tap]);
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < residual_storage.len());
                    // `sample_taps` only produces in-bounds grid coordinates.
                    let cell = unsafe { residual_storage.get_unchecked_mut(index) };
                    cell.re += residual.re * weight;
                    cell.im += residual.im * weight;
                }
            }
            return predicted_visibility;
        }

        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = taps.x.start + x_tap;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let weight = f64::from(x_weight * y_weights[y_tap]);
                let cell = &mut residual_grid[(x_index, taps.y.start + y_tap)];
                cell.re += residual.re * weight;
                cell.im += residual.im * weight;
            }
        }

        predicted_visibility
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn degrid_model_and_grid_residual_taps_planned_f64_storage(
        &self,
        model_storage: &[Complex32],
        residual_storage: &mut [Complex64],
        taps: &PositiveTapSet,
        observed_visibility: Complex32,
        residual_weight: f64,
    ) -> Complex32 {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        let mut predicted_re = 0.0f32;
        let mut predicted_im = 0.0f32;

        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (taps.x.start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let index = x_index + taps.y.start + y_tap;
                debug_assert!(index < model_storage.len());
                let weight = x_weight * y_weights[y_tap];
                let cell = unsafe { *model_storage.get_unchecked(index) };
                predicted_re += cell.re * weight;
                predicted_im += cell.im * weight;
            }
        }
        let predicted_visibility = Complex32::new(predicted_re, predicted_im);

        let residual_visibility = observed_visibility - predicted_visibility;
        let residual = Complex64::new(
            f64::from(residual_visibility.re) * residual_weight,
            f64::from(residual_visibility.im) * residual_weight,
        );
        self.grid_sample_taps_planned_f64_storage(residual_storage, taps, residual);
        predicted_visibility
    }

    #[allow(clippy::needless_range_loop, clippy::too_many_arguments)]
    pub(crate) fn degrid_model_and_grid_residual_compact_taps_planned_f64_storage(
        &self,
        model_storage: &[Complex32],
        residual_storage: &mut [Complex64],
        observed_visibility: Complex32,
        residual_weight: f64,
        x_start: usize,
        y_start: usize,
        x_weight_index: usize,
        y_weight_index: usize,
    ) -> Complex32 {
        let grid_stride = self.grid_shape[1];
        let x_weights = &self.normalized_tap_weights[x_weight_index];
        let y_weights = &self.normalized_tap_weights[y_weight_index];
        let mut predicted_re = 0.0f32;
        let mut predicted_im = 0.0f32;

        for x_tap in 0..GRIDDER_TAP_COUNT {
            let x_index = (x_start + x_tap) * grid_stride;
            let x_weight = x_weights[x_tap];
            for y_tap in 0..GRIDDER_TAP_COUNT {
                let index = x_index + y_start + y_tap;
                debug_assert!(index < model_storage.len());
                let weight = x_weight * y_weights[y_tap];
                let cell = unsafe { *model_storage.get_unchecked(index) };
                predicted_re += cell.re * weight;
                predicted_im += cell.im * weight;
            }
        }
        let predicted_visibility = Complex32::new(predicted_re, predicted_im);

        let residual_visibility = observed_visibility - predicted_visibility;
        let residual = Complex64::new(
            f64::from(residual_visibility.re) * residual_weight,
            f64::from(residual_visibility.im) * residual_weight,
        );
        self.grid_compact_taps_planned_f64_storage(
            residual_storage,
            residual,
            x_start,
            y_start,
            x_weight_index,
            y_weight_index,
        );
        predicted_visibility
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn degrid_model_and_grid_residual_taps_planned_f64_with_residual_offset(
        &self,
        model_grid: &Array2<Complex32>,
        residual_grid: &mut Array2<Complex64>,
        taps: &PositiveTapSet,
        observed_visibility: Complex32,
        residual_weight: f64,
        residual_offset: [usize; 2],
    ) -> Complex32 {
        debug_assert_eq!(model_grid.shape(), self.grid_shape.as_slice());

        let x_weights = &self.normalized_tap_weights[taps.x.weight_index];
        let y_weights = &self.normalized_tap_weights[taps.y.weight_index];
        let mut predicted_re = 0.0f32;
        let mut predicted_im = 0.0f32;

        if let Some(model_storage) = model_grid.as_slice_memory_order() {
            let grid_stride = self.grid_shape[1];
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = (taps.x.start + x_tap) * grid_stride;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let index = x_index + taps.y.start + y_tap;
                    debug_assert!(index < model_storage.len());
                    let weight = x_weight * y_weights[y_tap];
                    let cell = unsafe { *model_storage.get_unchecked(index) };
                    predicted_re += cell.re * weight;
                    predicted_im += cell.im * weight;
                }
            }
        } else {
            for x_tap in 0..GRIDDER_TAP_COUNT {
                let x_index = taps.x.start + x_tap;
                let x_weight = x_weights[x_tap];
                for y_tap in 0..GRIDDER_TAP_COUNT {
                    let weight = x_weight * y_weights[y_tap];
                    let cell = model_grid[(x_index, taps.y.start + y_tap)];
                    predicted_re += cell.re * weight;
                    predicted_im += cell.im * weight;
                }
            }
        }
        let predicted_visibility = Complex32::new(predicted_re, predicted_im);

        let residual_visibility = observed_visibility - predicted_visibility;
        let residual = Complex64::new(
            f64::from(residual_visibility.re) * residual_weight,
            f64::from(residual_visibility.im) * residual_weight,
        );
        self.grid_sample_taps_planned_f64_with_offset(
            residual_grid,
            taps,
            residual,
            residual_offset,
        );
        predicted_visibility
    }

    #[cfg(test)]
    pub(crate) fn correction_image(&self) -> Array2<f32> {
        let mut correction = Array2::<f32>::zeros((self.grid_shape[0], self.grid_shape[1]));
        for x in 0..self.grid_shape[0] {
            for y in 0..self.grid_shape[1] {
                correction[(x, y)] = self.correction_x[x] * self.correction_y[y];
            }
        }
        correction
    }

    pub(crate) fn apodize_model(&self, model: &Array2<f32>) -> Array2<Complex32> {
        let complex = model.mapv(|value| Complex32::new(value, 0.0));
        self.apodize_complex_model(&complex)
    }

    pub(crate) fn apodize_complex_model(&self, model: &Array2<Complex32>) -> Array2<Complex32> {
        let mut apodized = Array2::<Complex32>::zeros((self.grid_shape[0], self.grid_shape[1]));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let correction = self.correction_x[grid_x] * self.correction_y[grid_y];
                apodized[(grid_x, grid_y)] = model[(x, y)] * correction;
            }
        }
        apodized
    }

    pub(crate) fn apodize_w_project_model(
        &self,
        model: &Array2<f32>,
        conv_sampling: usize,
    ) -> Array2<Complex32> {
        let sinc_x = build_sinc_axis(self.grid_shape[0], conv_sampling);
        let sinc_y = build_sinc_axis(self.grid_shape[1], conv_sampling);
        let mut apodized = Array2::<Complex32>::zeros((self.grid_shape[0], self.grid_shape[1]));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let factor = self.correction_x[grid_x]
                    * self.correction_y[grid_y]
                    * sinc_x[grid_x]
                    * sinc_y[grid_y];
                apodized[(grid_x, grid_y)] = Complex32::new(model[(x, y)] * factor, 0.0);
            }
        }
        apodized
    }

    pub(crate) fn apodize_mosaic_model(
        &self,
        model: &Array2<f32>,
        conv_sampling: usize,
    ) -> Array2<Complex32> {
        let sinc = build_sinc_axis(self.grid_shape[0].max(self.grid_shape[1]), conv_sampling);
        let mut apodized = Array2::<Complex32>::zeros((self.grid_shape[0], self.grid_shape[1]));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let factor = sinc[grid_x] * sinc[grid_y];
                apodized[(grid_x, grid_y)] = Complex32::new(model[(x, y)] * factor, 0.0);
            }
        }
        apodized
    }

    pub(crate) fn corrected_image_from_grid(&self, raw: &Array2<Complex32>) -> Array2<f32> {
        self.corrected_complex_image_from_grid(raw)
            .mapv(|value| value.re)
    }

    pub(crate) fn corrected_image_from_grid_f64(&self, raw: &Array2<Complex64>) -> Array2<f32> {
        self.corrected_image_from_grid(
            &raw.mapv(|value| Complex32::new(value.re as f32, value.im as f32)),
        )
    }

    pub(crate) fn corrected_complex_image_from_grid(
        &self,
        raw: &Array2<Complex32>,
    ) -> Array2<Complex32> {
        let mut image = Array2::<Complex32>::zeros((self.geometry.nx(), self.geometry.ny()));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                image[(x, y)] =
                    raw[(grid_x, grid_y)] * (self.correction_x[grid_x] * self.correction_y[grid_y]);
            }
        }
        image
    }

    pub(crate) fn corrected_w_project_complex_image_from_grid(
        &self,
        raw: &Array2<Complex32>,
        conv_sampling: usize,
    ) -> Array2<Complex32> {
        let sinc = build_sinc_axis(self.grid_shape[0].max(self.grid_shape[1]), conv_sampling);
        let mut image = Array2::<Complex32>::zeros((self.geometry.nx(), self.geometry.ny()));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let factor = self.correction_x[grid_x] * self.correction_y[grid_y];
                let sinc_factor = sinc[grid_x] * sinc[grid_y];
                image[(x, y)] = if sinc_factor.abs() > 1.0e-6 {
                    raw[(grid_x, grid_y)] * (factor / sinc_factor)
                } else {
                    Complex32::new(0.0, 0.0)
                };
            }
        }
        image
    }

    pub(crate) fn corrected_w_project_image_from_grid(
        &self,
        raw: &Array2<Complex32>,
        conv_sampling: usize,
    ) -> Array2<f32> {
        self.corrected_w_project_complex_image_from_grid(raw, conv_sampling)
            .mapv(|value| value.re)
    }

    pub(crate) fn corrected_mosaic_image_from_grid_f64(
        &self,
        raw: &Array2<Complex64>,
        conv_sampling: usize,
    ) -> Array2<f32> {
        let sinc = build_sinc_axis(self.grid_shape[0].max(self.grid_shape[1]), conv_sampling);
        let mut image = Array2::<f32>::zeros((self.geometry.nx(), self.geometry.ny()));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let sinc_factor = f64::from(sinc[grid_x] * sinc[grid_y]);
                image[(x, y)] = if sinc_factor.abs() > 1.0e-6 {
                    (raw[(grid_x, grid_y)] / sinc_factor).re as f32
                } else {
                    0.0
                };
            }
        }
        image
    }

    #[cfg(test)]
    pub(crate) fn density_cell_index(
        &self,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<(usize, usize)> {
        self.weight_density_cell_anchor(u_lambda, v_lambda, DensityCellConvention::VisImagingWeight)
    }

    pub(crate) fn density_at_with_convention(
        &self,
        density: &Array2<f32>,
        u_lambda: f64,
        v_lambda: f64,
        convention: DensityCellConvention,
    ) -> Option<f32> {
        let (anchor_x, anchor_y) =
            self.weight_density_cell_anchor(u_lambda, v_lambda, convention)?;
        Some(density[(anchor_x, anchor_y)])
    }

    pub(crate) fn density_cell_index_with_convention(
        &self,
        u_lambda: f64,
        v_lambda: f64,
        convention: DensityCellConvention,
    ) -> Option<(usize, usize)> {
        self.weight_density_cell_anchor(u_lambda, v_lambda, convention)
    }

    pub(crate) fn cube_briggs_uv_cell_radius(&self, u_lambda: f64, v_lambda: f64) -> f64 {
        let u_cells = u_lambda * self.density_u_scale;
        let v_cells = v_lambda * self.density_v_scale;
        (u_cells * u_cells + v_cells * v_cells).sqrt()
    }

    fn grid_coordinate_x(&self, u_lambda: f64) -> f64 {
        u_lambda / self.du_lambda + self.grid_shape[0] as f64 / 2.0
    }

    fn grid_coordinate_y(&self, v_lambda: f64) -> f64 {
        // CASA GridFT negates the first two UVW axes before calling the
        // low-level gridder. With a positive Dec increment this makes the
        // image-plane y coordinate depend on -v rather than +v.
        -v_lambda / self.dv_lambda + self.grid_shape[1] as f64 / 2.0
    }

    fn weight_density_cell_anchor(
        &self,
        u_lambda: f64,
        v_lambda: f64,
        convention: DensityCellConvention,
    ) -> Option<(usize, usize)> {
        let (x, y) = match convention {
            DensityCellConvention::VisImagingWeight => {
                let u = f64::from(u_lambda as f32);
                let v = f64::from(v_lambda as f32);
                (
                    -u * self.density_u_scale + self.density_center_x,
                    v * self.density_v_scale + self.density_center_y,
                )
            }
            DensityCellConvention::CubeBriggsWeightorDensity => (
                u_lambda * self.density_u_scale + self.density_center_x,
                -v_lambda * self.density_v_scale + self.density_center_y,
            ),
            DensityCellConvention::CubeBriggsWeightorLookup => (
                u_lambda * self.density_u_scale + self.density_center_x,
                -v_lambda * self.density_v_scale + self.density_center_y,
            ),
        };
        if !(x.is_finite() && y.is_finite()) {
            return None;
        }
        let (anchor_x, anchor_y) = match convention {
            DensityCellConvention::VisImagingWeight => (x as isize, y as isize),
            DensityCellConvention::CubeBriggsWeightorDensity
            | DensityCellConvention::CubeBriggsWeightorLookup => {
                (x.round() as isize, y.round() as isize)
            }
        };
        if anchor_x <= 0
            || anchor_y <= 0
            || anchor_x >= self.density_limit_x
            || anchor_y >= self.density_limit_y
        {
            return None;
        }
        Some((anchor_x as usize, anchor_y as usize))
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
        let weight_index = self.normalized_tap_weight_index(offset)?;
        let mut indices = [0usize; GRIDDER_TAP_COUNT];
        for (tap, index) in (start..=end).enumerate() {
            indices[tap] = index as usize;
        }
        Some(TapSet {
            indices,
            weights: self.normalized_tap_weights[weight_index],
        })
    }

    fn sample_tap_span(&self, coordinate: f64, size: usize) -> Option<TapAxisSpan> {
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
        let weight_index = self.normalized_tap_weight_index(offset)?;
        Some(TapAxisSpan {
            start: start as usize,
            weight_index,
        })
    }

    fn sample_tap_center(&self, coordinate: f64, size: usize) -> Option<usize> {
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
        self.normalized_tap_weight_index(offset)?;
        Some(anchor as usize)
    }

    fn normalized_tap_weight_index(&self, offset: isize) -> Option<usize> {
        let half = self.oversampling as isize / 2;
        let index = offset + half;
        (index >= 0 && index < self.normalized_tap_weights.len() as isize).then_some(index as usize)
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
        if norm <= 0.0 {
            return None;
        }
        Some(TapSet { indices, weights })
    }
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

fn flatten_tap_products(x_taps: &TapSet, y_taps: &TapSet, grid_stride: usize) -> ProductTapSet {
    let mut flat_indices = [0usize; GRIDDER_PRODUCT_TAP_COUNT];
    let mut weights = [0.0f32; GRIDDER_PRODUCT_TAP_COUNT];
    let mut slot = 0usize;
    for x_tap in 0..GRIDDER_TAP_COUNT {
        let x_index = x_taps.indices[x_tap];
        let x_weight = x_taps.weights[x_tap];
        for y_tap in 0..GRIDDER_TAP_COUNT {
            flat_indices[slot] = x_index * grid_stride + y_taps.indices[y_tap];
            weights[slot] = x_weight * y_taps.weights[y_tap];
            slot += 1;
        }
    }
    ProductTapSet {
        flat_indices,
        weights,
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WProjectSamplePlan {
    pub(crate) loc_x: isize,
    pub(crate) loc_y: isize,
    pub(crate) off_x: isize,
    pub(crate) off_y: isize,
    pub(crate) plane_index: usize,
    pub(crate) conjugate_kernel: bool,
    pub(crate) normalization: f32,
}

#[derive(Clone, Debug)]
struct WProjectKernel {
    support: usize,
    weights: Array2<Complex32>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScreenProjectSamplePlan {
    pub(crate) loc_x: isize,
    pub(crate) loc_y: isize,
    pub(crate) off_x: isize,
    pub(crate) off_y: isize,
    pub(crate) min_ix: isize,
    pub(crate) max_ix: isize,
    pub(crate) min_iy: isize,
    pub(crate) max_iy: isize,
    pub(crate) center_in_bounds: bool,
    pub(crate) normalization: f32,
}

#[derive(Clone)]
pub(crate) struct ScreenProjector {
    grid_shape: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    sampling: usize,
    support: usize,
    kernel_center: usize,
    kernel_weights: Array2<Complex32>,
    phased_kernel_weights: Array2<Complex32>,
    normalization_sum: Complex32,
    phase_gradient_rad_per_sample: [f64; 2],
}

pub(crate) struct ScreenProjectorCompactKernel {
    pub(crate) values: Vec<Complex32>,
    pub(crate) tap_width: usize,
    pub(crate) offset_count: usize,
    pub(crate) offset_bias: usize,
}

impl ScreenProjector {
    pub(crate) fn support(&self) -> usize {
        self.support
    }

    pub(crate) fn sampling(&self) -> usize {
        self.sampling
    }

    pub(crate) fn normalization_sum(&self) -> Complex32 {
        self.normalization_sum
    }

    pub(crate) fn kernel_weight_width(&self) -> usize {
        self.phased_kernel_weights.dim().0
    }

    pub(crate) fn compact_phased_kernel_weights(&self) -> ScreenProjectorCompactKernel {
        let support = self.support as isize;
        let tap_width = self.support * 2 + 1;
        let offset_bias = self.sampling;
        let offset_count = offset_bias * 2 + 1;
        let mut values =
            Vec::<Complex32>::with_capacity(offset_count * offset_count * tap_width * tap_width);
        for off_y in -(offset_bias as isize)..=(offset_bias as isize) {
            for off_x in -(offset_bias as isize)..=(offset_bias as isize) {
                for iy in -support..=support {
                    let kernel_y = (self.kernel_center as isize
                        + iy * self.sampling as isize
                        + off_y) as usize;
                    for ix in -support..=support {
                        let kernel_x = (self.kernel_center as isize
                            + ix * self.sampling as isize
                            + off_x) as usize;
                        values.push(self.phased_kernel_weights[(kernel_x, kernel_y)]);
                    }
                }
            }
        }
        ScreenProjectorCompactKernel {
            values,
            tap_width,
            offset_count,
            offset_bias,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn from_screen<F>(
        geometry: ImageGeometry,
        gridder: &StandardGridder,
        sampling: usize,
        mut evaluator: F,
    ) -> Result<Self, ImagingError>
    where
        F: FnMut(f64, f64) -> Complex32,
    {
        if sampling == 0 {
            return Err(ImagingError::InvalidRequest(
                "screen projector sampling must be >= 1".to_string(),
            ));
        }
        let grid_shape = gridder.grid_shape();
        let conv_size =
            screen_projector_conv_size(geometry, sampling).max(grid_shape[0].max(grid_shape[1]));
        let inner = (conv_size / sampling).max(2);
        let correction_x = build_gridder_correction_axis(inner);
        let correction_y = build_gridder_correction_axis(inner);
        let screen_sampling = 2.0 * sampling as f64;
        let s0 = geometry.cell_size_rad[0].abs() * screen_sampling * grid_shape[0] as f64
            / conv_size as f64;
        let s1 = geometry.cell_size_rad[1].abs() * screen_sampling * grid_shape[1] as f64
            / conv_size as f64;

        let mut screen = Array2::<Complex32>::zeros((conv_size, conv_size));
        for iy in -(inner as isize / 2)..(inner as isize / 2) {
            let m = s1 * iy as f64;
            let msq = m * m;
            for ix in -(inner as isize / 2)..(inner as isize / 2) {
                let l = s0 * ix as f64;
                let rsq = l * l + msq;
                if rsq >= 1.0 {
                    continue;
                }
                let x = if ix >= 0 {
                    ix as usize
                } else {
                    (ix + conv_size as isize) as usize
                };
                let y = if iy >= 0 {
                    iy as usize
                } else {
                    (iy + conv_size as isize) as usize
                };
                let correction = correction_x[(ix + inner as isize / 2) as usize]
                    * correction_y[(iy + inner as isize / 2) as usize];
                screen[(x, y)] = evaluator(l, m) * correction;
            }
        }

        let transformed = fft2(&screen);
        let peak = transformed[(0, 0)].norm();
        if !(peak.is_finite() && peak > 0.0) {
            return Err(ImagingError::Normalization(
                "screen projector kernel peak is non-finite or zero".to_string(),
            ));
        }
        let quarter_len = conv_size / 2 - 1;
        let mut weights = Array2::<Complex32>::zeros((quarter_len, quarter_len));
        for y in 0..quarter_len {
            for x in 0..quarter_len {
                weights[(x, y)] = transformed[(x, y)] / peak;
            }
        }
        let support = find_screen_projector_support(&weights, sampling).max(7);
        // Mirror CASA SimplePBConvFunc's cropped kernel extent:
        // newConvSize = 2 * (support + 2) * convSampling.
        let kernel_size = 2 * (support + 2) * sampling;
        let kernel_center = kernel_size / 2;
        let mut kernel_weights = Array2::<Complex32>::zeros((kernel_size, kernel_size));
        for kernel_y in 0..kernel_size {
            let signed_y = kernel_y as isize - kernel_center as isize;
            let source_y = if signed_y >= 0 {
                signed_y as usize
            } else {
                (conv_size as isize + signed_y) as usize
            };
            for kernel_x in 0..kernel_size {
                let signed_x = kernel_x as isize - kernel_center as isize;
                let source_x = if signed_x >= 0 {
                    signed_x as usize
                } else {
                    (conv_size as isize + signed_x) as usize
                };
                kernel_weights[(kernel_x, kernel_y)] = transformed[(source_x, source_y)] / peak;
            }
        }
        let kernel_sum =
            screen_projector_plane_sum(&kernel_weights, kernel_center, support, sampling);
        if !(kernel_sum.re.is_finite() && kernel_sum.im.is_finite() && kernel_sum.norm() > 1.0e-6) {
            return Err(ImagingError::Normalization(
                "screen projector kernel normalization is non-finite or zero".to_string(),
            ));
        }
        kernel_weights.mapv_inplace(|value| value / kernel_sum);

        Ok(Self {
            grid_shape,
            du_lambda: gridder.grid_spacing_lambda()[0],
            dv_lambda: gridder.grid_spacing_lambda()[1],
            sampling,
            support,
            kernel_center,
            phased_kernel_weights: kernel_weights.clone(),
            kernel_weights,
            normalization_sum: kernel_sum,
            phase_gradient_rad_per_sample: [0.0, 0.0],
        })
    }

    pub(crate) fn from_hetarray_screens<I, W>(
        geometry: ImageGeometry,
        gridder: &StandardGridder,
        sampling: usize,
        conv_size: usize,
        mut imaging_evaluator: I,
        mut weight_evaluator: W,
    ) -> Result<Self, ImagingError>
    where
        I: FnMut(f64, f64) -> Complex32,
        W: FnMut(f64, f64) -> Complex32,
    {
        if sampling == 0 {
            return Err(ImagingError::InvalidRequest(
                "screen projector sampling must be >= 1".to_string(),
            ));
        }
        let imaging_temp = hetarray_screen_fft_temp(geometry, conv_size, &mut imaging_evaluator)?;
        let weight_temp = hetarray_screen_fft_temp(geometry, conv_size, &mut weight_evaluator)?;
        let support = find_hetarray_screen_support(&weight_temp, 1);
        if support == 0 {
            return Err(ImagingError::Normalization(
                "mosaic screen projector support is zero".to_string(),
            ));
        }

        let mut normalized_imaging = imaging_temp;
        let imaging_sum = screen_projector_plane_sum(
            &normalized_imaging,
            normalized_imaging.dim().0 / 2,
            support,
            1,
        );
        let imaging_norm = imaging_sum.re;
        if !(imaging_norm.is_finite() && imaging_norm > 1.0e-6) {
            return Err(ImagingError::Normalization(
                "mosaic screen projector kernel normalization is non-finite or zero".to_string(),
            ));
        }
        normalized_imaging.mapv_inplace(|value| value / imaging_norm);

        let cropped_size = 2 * (support + 2);
        let cropped_center = cropped_size / 2;
        let temp_center = normalized_imaging.dim().0 / 2;
        let mut cropped = Array2::<Complex32>::zeros((cropped_size, cropped_size));
        for y in 0..cropped_size {
            let source_y = temp_center + y - cropped_center;
            for x in 0..cropped_size {
                let source_x = temp_center + x - cropped_center;
                cropped[(x, y)] = normalized_imaging[(source_x, source_y)];
            }
        }

        let kernel_weights = lanczos_resample_complex(&cropped, sampling);
        let kernel_center = kernel_weights.dim().0 / 2;
        Ok(Self {
            grid_shape: gridder.grid_shape(),
            du_lambda: gridder.grid_spacing_lambda()[0],
            dv_lambda: gridder.grid_spacing_lambda()[1],
            sampling,
            support,
            kernel_center,
            normalization_sum: Complex32::new(imaging_norm, 0.0),
            phased_kernel_weights: kernel_weights.clone(),
            kernel_weights,
            phase_gradient_rad_per_sample: [0.0, 0.0],
        })
    }

    pub(crate) fn with_phase_gradient(mut self, phase_gradient_rad_per_sample: [f64; 2]) -> Self {
        self.phase_gradient_rad_per_sample = phase_gradient_rad_per_sample;
        self.phased_kernel_weights = self.kernel_weights.clone();
        let center = self.kernel_center as isize;
        for ((kernel_x, kernel_y), weight) in self.phased_kernel_weights.indexed_iter_mut() {
            let signed_x = kernel_x as isize - center;
            let signed_y = kernel_y as isize - center;
            let phase = signed_x as f64 * phase_gradient_rad_per_sample[0]
                + signed_y as f64 * phase_gradient_rad_per_sample[1];
            *weight *= Complex32::new(phase.cos() as f32, phase.sin() as f32);
        }
        self
    }

    pub(crate) fn plan_sample(
        &self,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<ScreenProjectSamplePlan> {
        self.plan_sample_internal(u_lambda, v_lambda, true)
    }

    pub(crate) fn plan_sample_for_grid(
        &self,
        u_lambda: f64,
        v_lambda: f64,
    ) -> Option<ScreenProjectSamplePlan> {
        self.plan_sample_internal(u_lambda, v_lambda, false)
    }

    fn plan_sample_internal(
        &self,
        u_lambda: f64,
        v_lambda: f64,
        compute_normalization: bool,
    ) -> Option<ScreenProjectSamplePlan> {
        let pos_x = u_lambda / self.du_lambda + self.grid_shape[0] as f64 / 2.0;
        let pos_y = -v_lambda / self.dv_lambda + self.grid_shape[1] as f64 / 2.0;
        if !(pos_x.is_finite() && pos_y.is_finite()) {
            return None;
        }
        let loc_x = pos_x.round() as isize;
        let loc_y = pos_y.round() as isize;
        let off_x = ((loc_x as f64 - pos_x) * self.sampling as f64).round() as isize;
        let off_y = ((loc_y as f64 - pos_y) * self.sampling as f64).round() as isize;
        let support = self.support as isize;
        if support <= 0 {
            return None;
        }
        let center_in_bounds = loc_x >= 0
            && loc_x < self.grid_shape[0] as isize
            && loc_y >= 0
            && loc_y < self.grid_shape[1] as isize;
        let min_ix = (-support).max(-loc_x);
        let max_ix = support.min(self.grid_shape[0] as isize - 1 - loc_x);
        let min_iy = (-support).max(-loc_y);
        let max_iy = support.min(self.grid_shape[1] as isize - 1 - loc_y);
        if min_ix > max_ix || min_iy > max_iy {
            return None;
        }
        let normalization = if compute_normalization {
            self.sample_normalization(off_x, off_y, min_ix, max_ix, min_iy, max_iy)?
        } else {
            0.0
        };
        Some(ScreenProjectSamplePlan {
            loc_x,
            loc_y,
            off_x,
            off_y,
            min_ix,
            max_ix,
            min_iy,
            max_iy,
            center_in_bounds,
            normalization,
        })
    }

    #[cfg(test)]
    pub(crate) fn grid_sample_planned(
        &self,
        grid: &mut Array2<Complex32>,
        plan: &ScreenProjectSamplePlan,
        value: Complex32,
    ) {
        for iy in plan.min_iy..=plan.max_iy {
            let kernel_y =
                (self.kernel_center as isize + iy * self.sampling as isize + plan.off_y) as usize;
            for ix in plan.min_ix..=plan.max_ix {
                let signed_x = ix * self.sampling as isize + plan.off_x;
                let kernel_x = (self.kernel_center as isize + signed_x) as usize;
                let cwt = self.phased_kernel_weights[(kernel_x, kernel_y)];
                grid[((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize)] += value * cwt;
            }
        }
    }

    pub(crate) fn grid_sample_planned_f64(
        &self,
        grid: &mut Array2<Complex64>,
        plan: &ScreenProjectSamplePlan,
        value: Complex64,
    ) {
        if let Some(storage) = grid.as_slice_memory_order_mut() {
            let grid_stride = self.grid_shape[1];
            for iy in plan.min_iy..=plan.max_iy {
                let kernel_y = (self.kernel_center as isize
                    + iy * self.sampling as isize
                    + plan.off_y) as usize;
                let grid_y = (plan.loc_y + iy) as usize;
                for ix in plan.min_ix..=plan.max_ix {
                    let signed_x = ix * self.sampling as isize + plan.off_x;
                    let kernel_x = (self.kernel_center as isize + signed_x) as usize;
                    let kernel = self.phased_kernel_weights[(kernel_x, kernel_y)];
                    let cwt = Complex64::new(kernel.re as f64, kernel.im as f64);
                    let grid_x = (plan.loc_x + ix) as usize;
                    storage[grid_x * grid_stride + grid_y] += value * cwt;
                }
            }
            return;
        }
        for iy in plan.min_iy..=plan.max_iy {
            let kernel_y =
                (self.kernel_center as isize + iy * self.sampling as isize + plan.off_y) as usize;
            for ix in plan.min_ix..=plan.max_ix {
                let signed_x = ix * self.sampling as isize + plan.off_x;
                let kernel_x = (self.kernel_center as isize + signed_x) as usize;
                let kernel = self.phased_kernel_weights[(kernel_x, kernel_y)];
                let cwt = Complex64::new(kernel.re as f64, kernel.im as f64);
                grid[((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize)] += value * cwt;
            }
        }
    }

    pub(crate) fn degrid_sample_planned(
        &self,
        grid: &Array2<Complex32>,
        plan: &ScreenProjectSamplePlan,
    ) -> Complex32 {
        if let Some(storage) = grid.as_slice_memory_order() {
            let grid_stride = self.grid_shape[1];
            let mut value = Complex32::new(0.0, 0.0);
            for iy in plan.min_iy..=plan.max_iy {
                let kernel_y = (self.kernel_center as isize
                    + iy * self.sampling as isize
                    + plan.off_y) as usize;
                let grid_y = (plan.loc_y + iy) as usize;
                for ix in plan.min_ix..=plan.max_ix {
                    let signed_x = ix * self.sampling as isize + plan.off_x;
                    let kernel_x = (self.kernel_center as isize + signed_x) as usize;
                    let cwt = self.phased_kernel_weights[(kernel_x, kernel_y)];
                    let grid_x = (plan.loc_x + ix) as usize;
                    value += cwt.conj() * storage[grid_x * grid_stride + grid_y];
                }
            }
            return value;
        }
        let mut value = Complex32::new(0.0, 0.0);
        for iy in plan.min_iy..=plan.max_iy {
            let kernel_y =
                (self.kernel_center as isize + iy * self.sampling as isize + plan.off_y) as usize;
            for ix in plan.min_ix..=plan.max_ix {
                let signed_x = ix * self.sampling as isize + plan.off_x;
                let kernel_x = (self.kernel_center as isize + signed_x) as usize;
                let cwt = self.phased_kernel_weights[(kernel_x, kernel_y)];
                value +=
                    cwt.conj() * grid[((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize)];
            }
        }
        value
    }

    pub(crate) fn trace_sample_taps(
        &self,
        plan: &ScreenProjectSamplePlan,
    ) -> Option<(Complex32, Complex32, Complex32)> {
        let mut sum = Complex32::new(0.0, 0.0);
        let mut center = Complex32::new(0.0, 0.0);
        let mut first = Complex32::new(0.0, 0.0);
        let mut first_set = false;
        for iy in plan.min_iy..=plan.max_iy {
            let kernel_y = usize::try_from(
                self.kernel_center as isize + iy * self.sampling as isize + plan.off_y,
            )
            .ok()?;
            for ix in plan.min_ix..=plan.max_ix {
                let signed_x = ix * self.sampling as isize + plan.off_x;
                let kernel_x = usize::try_from(self.kernel_center as isize + signed_x).ok()?;
                let tap = *self.phased_kernel_weights.get((kernel_x, kernel_y))?;
                sum += tap;
                if ix == 0 && iy == 0 {
                    center = tap;
                }
                if !first_set {
                    first = tap;
                    first_set = true;
                }
            }
        }
        Some((sum, center, first))
    }

    pub(crate) fn trace_sample_tap_matrix(
        &self,
        plan: &ScreenProjectSamplePlan,
    ) -> Option<Vec<(isize, isize, Complex32)>> {
        let mut taps = Vec::new();
        for iy in plan.min_iy..=plan.max_iy {
            let kernel_y = usize::try_from(
                self.kernel_center as isize + iy * self.sampling as isize + plan.off_y,
            )
            .ok()?;
            for ix in plan.min_ix..=plan.max_ix {
                let signed_x = ix * self.sampling as isize + plan.off_x;
                let kernel_x = usize::try_from(self.kernel_center as isize + signed_x).ok()?;
                taps.push((
                    ix,
                    iy,
                    *self.phased_kernel_weights.get((kernel_x, kernel_y))?,
                ));
            }
        }
        Some(taps)
    }

    pub(crate) fn trace_sample_tap_at(
        &self,
        plan: &ScreenProjectSamplePlan,
        ix: isize,
        iy: isize,
    ) -> Option<Complex32> {
        if ix < plan.min_ix || ix > plan.max_ix || iy < plan.min_iy || iy > plan.max_iy {
            return None;
        }
        let kernel_y =
            usize::try_from(self.kernel_center as isize + iy * self.sampling as isize + plan.off_y)
                .ok()?;
        let signed_x = ix * self.sampling as isize + plan.off_x;
        let kernel_x = usize::try_from(self.kernel_center as isize + signed_x).ok()?;
        self.phased_kernel_weights
            .get((kernel_x, kernel_y))
            .copied()
    }

    fn sample_normalization(
        &self,
        off_x: isize,
        off_y: isize,
        min_ix: isize,
        max_ix: isize,
        min_iy: isize,
        max_iy: isize,
    ) -> Option<f32> {
        let mut normalization = Complex32::new(0.0, 0.0);
        for iy in min_iy..=max_iy {
            let kernel_y =
                usize::try_from(self.kernel_center as isize + iy * self.sampling as isize + off_y)
                    .ok()?;
            for ix in min_ix..=max_ix {
                let signed_x = ix * self.sampling as isize + off_x;
                let kernel_x = usize::try_from(self.kernel_center as isize + signed_x).ok()?;
                let value = *self.phased_kernel_weights.get((kernel_x, kernel_y))?;
                normalization += value;
            }
        }
        Some(normalization.norm())
    }
}

pub(crate) struct WProjector {
    grid_shape: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    sampling: usize,
    w_scale: f64,
    kernels: Vec<WProjectKernel>,
    normalization_offset_radius: isize,
    normalization_by_plane_offset: Vec<f32>,
}

impl WProjector {
    pub(crate) fn new(
        geometry: ImageGeometry,
        gridder: &StandardGridder,
        max_abs_w_lambda: f64,
        explicit_plane_count: Option<usize>,
    ) -> Result<Self, ImagingError> {
        let raw_auto_plane_count = suggested_w_project_plane_count(geometry, max_abs_w_lambda);
        let plane_count = explicit_plane_count
            .unwrap_or_else(|| choose_w_project_plane_count(geometry, max_abs_w_lambda));
        let sampling = if plane_count > 1 { 4usize } else { 1usize };
        let grid_shape = gridder.grid_shape();
        let conv_size = grid_shape[0].max(grid_shape[1]);
        let inner = (conv_size / sampling).max(2);
        let correction_x = build_gridder_correction_axis(inner);
        let correction_y = build_gridder_correction_axis(inner);
        let s0 = geometry.cell_size_rad[0].abs() * sampling as f64 * grid_shape[0] as f64
            / conv_size as f64;
        let s1 = geometry.cell_size_rad[1].abs() * sampling as f64 * grid_shape[1] as f64
            / conv_size as f64;
        let max_increment = geometry.cell_size_rad[0]
            .abs()
            .max(geometry.cell_size_rad[1].abs());
        let effective_max_w_lambda =
            if explicit_plane_count.is_some() || raw_auto_plane_count > MAX_AUTO_WPROJECT_PLANES {
                0.25 / max_increment
            } else {
                1.05 * max_abs_w_lambda
            };
        let w_scale = if plane_count > 1
            && effective_max_w_lambda.is_finite()
            && effective_max_w_lambda > 0.0
        {
            ((plane_count - 1) * (plane_count - 1)) as f64 / effective_max_w_lambda
        } else {
            1.0
        };

        let mut kernels = Vec::with_capacity(plane_count);
        let mut plane_zero_peak = None::<f32>;
        for plane_index in 0..plane_count {
            let mut screen = Array2::<Complex32>::zeros((conv_size, conv_size));
            let w_lambda = if plane_count > 1 {
                (plane_index * plane_index) as f64 / w_scale
            } else {
                0.0
            };
            for iy in -(inner as isize / 2)..(inner as isize / 2) {
                let m = s1 * iy as f64;
                let msq = m * m;
                for ix in -(inner as isize / 2)..(inner as isize / 2) {
                    let l = s0 * ix as f64;
                    let rsq = l * l + msq;
                    if rsq >= 1.0 {
                        continue;
                    }
                    let phase = std::f64::consts::TAU * w_lambda * ((1.0 - rsq).sqrt() - 1.0);
                    let correction = correction_x[(ix + inner as isize / 2) as usize]
                        * correction_y[(iy + inner as isize / 2) as usize];
                    let x = if ix >= 0 {
                        ix as usize
                    } else {
                        (ix + conv_size as isize) as usize
                    };
                    let y = if iy >= 0 {
                        iy as usize
                    } else {
                        (iy + conv_size as isize) as usize
                    };
                    screen[(x, y)] = Complex32::new(
                        correction * phase.cos() as f32,
                        correction * phase.sin() as f32,
                    );
                }
            }

            let transformed = fft2(&screen);
            let plane_peak = transformed[(0, 0)].norm();
            if plane_zero_peak.is_none() {
                plane_zero_peak = Some(plane_peak);
            }
            let peak = plane_zero_peak.unwrap_or(0.0);
            if peak <= 0.0 {
                return Err(ImagingError::Normalization(
                    "wproject plane-zero kernel peak is zero".to_string(),
                ));
            }
            let quarter_len = conv_size / 2 - 1;
            let mut weights = Array2::<Complex32>::zeros((quarter_len, quarter_len));
            for y in 0..quarter_len {
                for x in 0..quarter_len {
                    weights[(x, y)] = transformed[(x, y)] / peak;
                }
            }
            let support = find_w_project_support(&weights, sampling);
            kernels.push(WProjectKernel { support, weights });
        }

        let pb_sum = w_project_plane_sum(&kernels[0], sampling);
        if !(pb_sum.is_finite() && pb_sum > 0.0) {
            return Err(ImagingError::Normalization(
                "wproject plane-zero kernel integral is non-positive".to_string(),
            ));
        }
        for kernel in &mut kernels {
            kernel.weights.mapv_inplace(|value| value / pb_sum);
        }
        let normalization_offset_radius = (sampling as isize + 1) / 2;
        let normalization_axis_len = (2 * normalization_offset_radius + 1) as usize;
        let mut normalization_by_plane_offset = Vec::with_capacity(
            kernels
                .len()
                .saturating_mul(normalization_axis_len)
                .saturating_mul(normalization_axis_len),
        );
        for kernel in &kernels {
            for off_x in -normalization_offset_radius..=normalization_offset_radius {
                for off_y in -normalization_offset_radius..=normalization_offset_radius {
                    normalization_by_plane_offset.push(
                        compute_w_project_sample_normalization(kernel, sampling, off_x, off_y)
                            .unwrap_or(0.0),
                    );
                }
            }
        }

        Ok(Self {
            grid_shape,
            du_lambda: gridder.grid_spacing_lambda()[0],
            dv_lambda: gridder.grid_spacing_lambda()[1],
            sampling,
            w_scale,
            kernels,
            normalization_offset_radius,
            normalization_by_plane_offset,
        })
    }

    pub(crate) fn plan_sample(
        &self,
        u_lambda: f64,
        v_lambda: f64,
        w_lambda: f64,
    ) -> Option<WProjectSamplePlan> {
        let pos_x = u_lambda / self.du_lambda + self.grid_shape[0] as f64 / 2.0;
        let pos_y = -v_lambda / self.dv_lambda + self.grid_shape[1] as f64 / 2.0;
        if !(pos_x.is_finite() && pos_y.is_finite() && w_lambda.is_finite()) {
            return None;
        }
        let loc_x = pos_x.round() as isize;
        let loc_y = pos_y.round() as isize;
        let off_x = ((loc_x as f64 - pos_x) * self.sampling as f64).round() as isize;
        let off_y = ((loc_y as f64 - pos_y) * self.sampling as f64).round() as isize;
        let plane_index = self.w_plane_index(w_lambda);
        let kernel = &self.kernels[plane_index];
        let support = kernel.support as isize;
        if support <= 0
            || loc_x - support < 0
            || loc_y - support < 0
            || loc_x + support >= self.grid_shape[0] as isize
            || loc_y + support >= self.grid_shape[1] as isize
        {
            return None;
        }
        let normalization = self.sample_normalization(plane_index, off_x, off_y)?;
        Some(WProjectSamplePlan {
            loc_x,
            loc_y,
            off_x,
            off_y,
            plane_index,
            conjugate_kernel: w_lambda > 0.0,
            normalization,
        })
    }

    pub(crate) fn sampling(&self) -> usize {
        self.sampling
    }

    pub(crate) fn grid_shape(&self) -> [usize; 2] {
        self.grid_shape
    }

    pub(crate) fn plane_count(&self) -> usize {
        self.kernels.len()
    }

    pub(crate) fn w_scale(&self) -> f64 {
        self.w_scale
    }

    pub(crate) fn kernel_support(&self, plane_index: usize) -> usize {
        self.kernels[plane_index].support
    }

    pub(crate) fn kernel_weight_width(&self) -> usize {
        self.kernels
            .first()
            .map(|kernel| kernel.weights.dim().0)
            .unwrap_or(0)
    }

    pub(crate) fn flattened_kernel_weights(&self) -> Vec<Complex32> {
        let width = self.kernel_weight_width();
        let mut weights = Vec::with_capacity(
            self.kernels
                .len()
                .saturating_mul(width)
                .saturating_mul(width),
        );
        for kernel in &self.kernels {
            weights.extend(kernel.weights.iter().copied());
        }
        weights
    }

    pub(crate) fn kernel_integral(&self, plane_index: usize) -> f32 {
        w_project_plane_sum(&self.kernels[plane_index], self.sampling)
    }

    pub(crate) fn kernel_w_lambda(&self, plane_index: usize) -> f64 {
        if self.kernels.len() > 1 {
            (plane_index * plane_index) as f64 / self.w_scale
        } else {
            0.0
        }
    }

    pub(crate) fn grid_sample_planned(
        &self,
        grid: &mut Array2<Complex32>,
        plan: &WProjectSamplePlan,
        value: Complex32,
    ) {
        let kernel = &self.kernels[plan.plane_index];
        let support = kernel.support as isize;
        for iy in -support..=support {
            let kernel_y = (iy * self.sampling as isize + plan.off_y).unsigned_abs();
            for ix in -support..=support {
                let kernel_x = (ix * self.sampling as isize + plan.off_x).unsigned_abs();
                let mut cwt = kernel.weights[(kernel_x, kernel_y)];
                if plan.conjugate_kernel {
                    cwt = cwt.conj();
                }
                grid[((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize)] += value * cwt;
            }
        }
    }

    pub(crate) fn grid_sample_planned_pair(
        &self,
        first_grid: &mut Array2<Complex32>,
        first_value: Complex32,
        second_grid: &mut Array2<Complex32>,
        second_value: Complex32,
        plan: &WProjectSamplePlan,
    ) {
        let kernel = &self.kernels[plan.plane_index];
        let support = kernel.support as isize;
        for iy in -support..=support {
            let kernel_y = (iy * self.sampling as isize + plan.off_y).unsigned_abs();
            for ix in -support..=support {
                let kernel_x = (ix * self.sampling as isize + plan.off_x).unsigned_abs();
                let mut cwt = kernel.weights[(kernel_x, kernel_y)];
                if plan.conjugate_kernel {
                    cwt = cwt.conj();
                }
                let cell = ((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize);
                first_grid[cell] += first_value * cwt;
                second_grid[cell] += second_value * cwt;
            }
        }
    }

    pub(crate) fn degrid_sample_planned(
        &self,
        grid: &Array2<Complex32>,
        plan: &WProjectSamplePlan,
    ) -> Complex32 {
        let kernel = &self.kernels[plan.plane_index];
        let support = kernel.support as isize;
        let mut value = Complex32::new(0.0, 0.0);
        for iy in -support..=support {
            let kernel_y = (iy * self.sampling as isize + plan.off_y).unsigned_abs();
            for ix in -support..=support {
                let kernel_x = (ix * self.sampling as isize + plan.off_x).unsigned_abs();
                let mut cwt = kernel.weights[(kernel_x, kernel_y)];
                if plan.conjugate_kernel {
                    cwt = cwt.conj();
                }
                value +=
                    cwt.conj() * grid[((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize)];
            }
        }
        value
    }

    fn w_plane_index(&self, w_lambda: f64) -> usize {
        if self.kernels.len() <= 1 {
            return 0;
        }
        let coordinate = (self.w_scale * w_lambda.abs()).max(0.0).sqrt();
        coordinate
            .round()
            .clamp(0.0, (self.kernels.len() - 1) as f64) as usize
    }

    fn sample_normalization(&self, plane_index: usize, off_x: isize, off_y: isize) -> Option<f32> {
        if off_x.abs() <= self.normalization_offset_radius
            && off_y.abs() <= self.normalization_offset_radius
        {
            let axis_len = (2 * self.normalization_offset_radius + 1) as usize;
            let offset_x = (off_x + self.normalization_offset_radius) as usize;
            let offset_y = (off_y + self.normalization_offset_radius) as usize;
            let index = plane_index
                .checked_mul(axis_len)?
                .checked_mul(axis_len)?
                .checked_add(offset_x.checked_mul(axis_len)?)?
                .checked_add(offset_y)?;
            return self.normalization_by_plane_offset.get(index).copied();
        }
        compute_w_project_sample_normalization(
            &self.kernels[plane_index],
            self.sampling,
            off_x,
            off_y,
        )
    }
}

fn compute_w_project_sample_normalization(
    kernel: &WProjectKernel,
    sampling: usize,
    off_x: isize,
    off_y: isize,
) -> Option<f32> {
    let support = kernel.support as isize;
    let mut normalization = 0.0f32;
    for iy in -support..=support {
        let kernel_y = (iy * sampling as isize + off_y).unsigned_abs();
        for ix in -support..=support {
            let kernel_x = (ix * sampling as isize + off_x).unsigned_abs();
            let value = *kernel.weights.get((kernel_x, kernel_y))?;
            normalization += value.re;
        }
    }
    Some(normalization)
}

pub(crate) fn choose_w_project_plane_count(
    geometry: ImageGeometry,
    max_abs_w_lambda: f64,
) -> usize {
    let suggested = suggested_w_project_plane_count(geometry, max_abs_w_lambda);
    // Keep the default auto estimate in the current tested regime rather than
    // letting it explode into very large supports that discard edge samples.
    // Callers that need a specific CASA-style `wprojplanes` budget can now
    // override this through the public request surface.
    suggested
        .max(1)
        .next_power_of_two()
        .clamp(1, MAX_AUTO_WPROJECT_PLANES)
}

const MAX_AUTO_WPROJECT_PLANES: usize = 16;

fn suggested_w_project_plane_count(geometry: ImageGeometry, max_abs_w_lambda: f64) -> usize {
    if !(max_abs_w_lambda.is_finite() && max_abs_w_lambda > 0.0) {
        return 1;
    }
    let max_axis = geometry.nx().max(geometry.ny()) as f64;
    let max_increment = geometry.cell_size_rad[0]
        .abs()
        .max(geometry.cell_size_rad[1].abs());
    (1.05 * max_abs_w_lambda * (max_increment * max_axis / 2.0).sin().abs()).ceil() as usize
}

fn find_w_project_support(weights: &Array2<Complex32>, sampling: usize) -> usize {
    let quarter_len = weights.dim().0;
    let mut trial = 0usize;
    let mut found = false;
    for candidate in (1..quarter_len).rev() {
        if weights[(candidate, 0)].norm() > 1.0e-3 || weights[(0, candidate)].norm() > 1.0e-3 {
            trial = candidate;
            found = true;
            break;
        }
    }
    let max_support = quarter_len / sampling - 1;
    if !found {
        return max_support.max(1);
    }
    (((trial as f32 / sampling as f32) + 0.5).floor() as usize + 1).min(max_support.max(1))
}

#[allow(dead_code)]
fn find_screen_projector_support(weights: &Array2<Complex32>, sampling: usize) -> usize {
    let quarter_len = weights.dim().0;
    let max_abs = weights
        .iter()
        .map(|value| value.norm())
        .fold(0.0f32, f32::max);
    let max_support = (quarter_len / sampling).saturating_sub(2).max(1);
    if !(max_abs.is_finite() && max_abs > 0.0) {
        return max_support;
    }

    let mut found = false;
    let mut trial = 0usize;
    for candidate in (1..quarter_len.saturating_sub(2)).rev() {
        if weights[(candidate, candidate)].norm() > 1.0e-3 * max_abs {
            found = true;
            trial = ((2.0 * (candidate * candidate) as f64).sqrt()) as usize;
            break;
        }
    }
    if !found {
        let dynamic_floor = if quarter_len > 10 * sampling {
            5 * sampling
        } else {
            quarter_len.saturating_sub(4 * sampling)
        };
        trial = dynamic_floor.max(sampling);
    }
    if trial < 5 * sampling {
        trial = if quarter_len > 10 * sampling {
            5 * sampling
        } else {
            quarter_len.saturating_sub(4 * sampling)
        };
    }
    ((((trial as f32) / (sampling as f32)) + 0.5).floor() as usize + 1).min(max_support)
}

fn find_hetarray_screen_support(weights: &Array2<Complex32>, sampling: usize) -> usize {
    let conv_size = weights.dim().0;
    let mut max_abs = 0.0f32;
    let mut max_pos = (conv_size / 2, conv_size / 2);
    for ((x, y), value) in weights.indexed_iter() {
        let amplitude = value.norm();
        if amplitude > max_abs {
            max_abs = amplitude;
            max_pos = (x, y);
        }
    }
    if !(max_abs.is_finite() && max_abs > 0.0) {
        return conv_size / 2 / sampling - 1;
    }

    let cut_level = 2.5e-2f32;
    let mut found = false;
    let mut trial = 0usize;
    let max_axis = max_pos.0.max(max_pos.1);
    for candidate in 0..conv_size.saturating_sub(max_axis + 2) {
        let x_probe = max_pos.0.saturating_sub(candidate);
        let y_probe = max_pos.1.saturating_sub(candidate);
        if weights[(x_probe, max_pos.1)].norm() < cut_level * max_abs
            && weights[(max_pos.0, y_probe)].norm() < cut_level * max_abs
        {
            found = true;
            trial = candidate;
            break;
        }
    }
    if !found {
        trial = conv_size / 2 - 4 * sampling;
    }
    if trial < 5 * sampling {
        trial = if 10 * sampling < conv_size {
            5 * sampling
        } else {
            conv_size / 2 - 4 * sampling
        };
    }
    let mut support = (((trial as f32) / (sampling as f32)) + 0.5).floor() as usize + 1;
    if support * sampling >= conv_size / 2 {
        support = conv_size / 2 / sampling - 1;
    }
    support
}

fn w_project_plane_sum(kernel: &WProjectKernel, sampling: usize) -> f32 {
    let support = kernel.support as isize;
    let mut sum = 0.0f32;
    for iy in -support..=support {
        let kernel_y = (iy * sampling as isize).unsigned_abs();
        for ix in -support..=support {
            let kernel_x = (ix * sampling as isize).unsigned_abs();
            sum += kernel.weights[(kernel_x, kernel_y)].re;
        }
    }
    sum
}

fn screen_projector_plane_sum(
    kernel_weights: &Array2<Complex32>,
    kernel_center: usize,
    support: usize,
    sampling: usize,
) -> Complex32 {
    let mut sum = Complex32::new(0.0, 0.0);
    for iy in -(support as isize)..=(support as isize) {
        let kernel_y = (kernel_center as isize + iy * sampling as isize) as usize;
        for ix in -(support as isize)..=(support as isize) {
            let kernel_x = (kernel_center as isize + ix * sampling as isize) as usize;
            sum += kernel_weights[(kernel_x, kernel_y)];
        }
    }
    sum
}

#[allow(dead_code)]
fn screen_projector_conv_size(geometry: ImageGeometry, sampling: usize) -> usize {
    let image_max = geometry.nx().max(geometry.ny());
    let scaled = (image_max * sampling * 2).max(4);
    if scaled % 2 == 0 { scaled } else { scaled + 1 }
}

#[allow(dead_code)]
pub(crate) fn hetarray_screen_conv_size(geometry: ImageGeometry) -> usize {
    hetarray_screen_conv_size_for_support(geometry, geometry.nx().max(geometry.ny()) / 10)
}

pub(crate) fn hetarray_screen_conv_size_for_support(
    geometry: ImageGeometry,
    pb_support_pixels: usize,
) -> usize {
    let image_max = geometry.nx().max(geometry.ny());
    let support = image_max.max(pb_support_pixels);
    let mut conv_size = support.max(64);
    while conv_size % 2 != 0 || !is_casa_composite_len(conv_size) {
        conv_size += 1;
    }
    (conv_size / 16).max(1) * 16
}

fn hetarray_screen_fft_temp<F>(
    geometry: ImageGeometry,
    conv_size: usize,
    evaluator: &mut F,
) -> Result<Array2<Complex32>, ImagingError>
where
    F: FnMut(f64, f64) -> Complex32,
{
    let s0 = geometry.cell_size_rad[0].abs() * geometry.nx() as f64 / conv_size as f64;
    let s1 = geometry.cell_size_rad[1].abs() * geometry.ny() as f64 / conv_size as f64;
    let center = conv_size as isize / 2;
    let mut screen = Array2::<Complex64>::zeros((conv_size, conv_size));
    for y in 0..conv_size {
        let m = (y as isize - center) as f64 * s1;
        for x in 0..conv_size {
            let l = (x as isize - center) as f64 * s0;
            let value = evaluator(l, m);
            screen[(x, y)] = Complex64::new(value.re as f64, value.im as f64);
        }
    }
    let transformed = centered_fft2_f64(&screen);
    let peak = transformed[(conv_size / 2, conv_size / 2)].norm();
    if !(peak.is_finite() && peak > 0.0) {
        return Err(ImagingError::Normalization(
            "mosaic screen projector FFT peak is non-finite or zero".to_string(),
        ));
    }
    let temp_size = conv_size / 4;
    let start = conv_size * 3 / 8;
    let mut temp = Array2::<Complex32>::zeros((temp_size, temp_size));
    for y in 0..temp_size {
        for x in 0..temp_size {
            let value = transformed[(start + x, start + y)];
            temp[(x, y)] = Complex32::new(value.re as f32, value.im as f32);
        }
    }
    Ok(temp)
}

fn lanczos_resample_complex(input: &Array2<Complex32>, factor: usize) -> Array2<Complex32> {
    let nx = input.dim().0;
    let ny = input.dim().1;
    let out_nx = (nx * factor / 2) * 2;
    let out_ny = (ny * factor / 2) * 2;
    let mut output = Array2::<Complex32>::zeros((out_nx, out_ny));
    for y in 0..out_ny {
        let source_y = y as f64 / out_ny as f64 * ny as f64;
        for x in 0..out_nx {
            let source_x = x as f64 / out_nx as f64 * nx as f64;
            output[(x, y)] = lanczos_sample_complex(input, source_x, source_y, 3.0);
        }
    }
    output
}

fn lanczos_sample_complex(input: &Array2<Complex32>, x: f64, y: f64, a: f64) -> Complex32 {
    let nx = input.dim().0 as f64;
    let ny = input.dim().1 as f64;
    let floor_x = x.floor();
    let floor_y = y.floor();
    if nx > 2.0 * a
        && ny > 2.0 * a
        && (floor_x < a || floor_x >= nx - a || floor_y < a || floor_y >= ny - a)
    {
        return Complex32::new(0.0, 0.0);
    }
    let mut result = Complex32::new(0.0, 0.0);
    let mut xx = floor_x - a + 1.0;
    while xx <= floor_x + a {
        if xx < 0.0 || xx >= nx {
            xx += 1.0;
            continue;
        }
        let wx = lanczos_sinc(x - xx) * lanczos_sinc((x - xx) / a);
        let mut yy = floor_y - a + 1.0;
        while yy <= floor_y + a {
            if yy >= 0.0 && yy < ny {
                let wy = lanczos_sinc(y - yy) * lanczos_sinc((y - yy) / a);
                result += input[(xx as usize, yy as usize)] * (wx * wy) as f32;
            }
            yy += 1.0;
        }
        xx += 1.0;
    }
    result
}

fn lanczos_sinc(x: f64) -> f64 {
    if x == 0.0 {
        1.0
    } else {
        (std::f64::consts::PI * x).sin() / (std::f64::consts::PI * x)
    }
}

fn padded_len(image_len: usize, padding_factor: f64) -> usize {
    let padded = (padding_factor * image_len as f64 - 0.5).floor() as usize;
    let padded = padded.max(image_len);
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

fn build_gridder_correction_axis(size: usize) -> Vec<f32> {
    let center = size as f64 / 2.0;
    (0..size)
        .map(|index| {
            let nu = ((index as f64 - center).abs() / center).clamp(0.0, 1.0);
            grdsf(nu) as f32
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

fn build_sinc_axis(size: usize, conv_sampling: usize) -> Vec<f32> {
    (0..size)
        .map(|index| {
            let argument = std::f64::consts::PI * (index as f64 - size as f64 / 2.0)
                / (size as f64 * conv_sampling as f64);
            if index == size / 2 || argument.abs() <= f64::EPSILON {
                1.0
            } else {
                (argument.sin() / argument) as f32
            }
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use casa_test_support::gridder_interop::{
        cpp_convolve_gridder_correction_row_2d, cpp_convolve_gridder_grid_unit_sample_2d,
        cpp_convolve_gridder_predict_visibility_2d,
    };
    use ndarray::Array2;
    use num_complex::{Complex32, Complex64};
    use serial_test::serial;

    use super::{
        DensityCellConvention, GRIDDER_TAP_COUNT, ScreenProjector, StandardGridder, WProjector,
    };
    use crate::{
        ImageGeometry,
        fft::{centered_fft2, centered_ifft2},
    };

    fn peak_location(image: &Array2<f32>) -> (usize, usize) {
        let mut best = (0usize, 0usize);
        let mut best_value = f32::NEG_INFINITY;
        for ((x, y), value) in image.indexed_iter() {
            if *value > best_value {
                best_value = *value;
                best = (x, y);
            }
        }
        best
    }

    fn assert_peak_within_tolerance(
        peak: (usize, usize),
        expected: (usize, usize),
        tolerance: isize,
    ) {
        assert!(
            (peak.0 as isize - expected.0 as isize).abs() <= tolerance
                && (peak.1 as isize - expected.1 as isize).abs() <= tolerance,
            "peak {peak:?} not within {tolerance} px of expected {expected:?}"
        );
    }

    #[test]
    fn positive_tap_span_reconstructs_legacy_positive_taps() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0 / 206_264.806_247, 1.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let samples = [(12.25, -18.75), (-9.5, 7.125), (1.0, 2.0)];

        for (u, v) in samples {
            let span = gridder
                .plan_positive_taps(u, v)
                .expect("compact positive tap span");
            let legacy_x = gridder
                .sample_taps(gridder.grid_coordinate_x(u), gridder.grid_shape()[0])
                .expect("legacy x taps");
            let legacy_y = gridder
                .sample_taps(gridder.grid_coordinate_y(v), gridder.grid_shape()[1])
                .expect("legacy y taps");

            assert_eq!(span.x.start, legacy_x.indices[0]);
            assert_eq!(span.y.start, legacy_y.indices[0]);
            assert_eq!(
                gridder.normalized_tap_weights[span.x.weight_index],
                legacy_x.weights
            );
            assert_eq!(
                gridder.normalized_tap_weights[span.y.weight_index],
                legacy_y.weights
            );
            for tap in 0..GRIDDER_TAP_COUNT {
                assert_eq!(span.x.start + tap, legacy_x.indices[tap]);
                assert_eq!(span.y.start + tap, legacy_y.indices[tap]);
            }
        }
    }

    #[test]
    fn positive_tap_center_locator_matches_positive_tap_plan() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0 / 206_264.806_247, 1.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let samples = [(0.0, 0.0), (12.25, -18.75), (-9.5, 7.125), (1.0, 2.0)];

        for (u, v) in samples {
            let center = gridder
                .locate_positive_tap_center(u, v)
                .expect("center should locate");
            let taps = gridder
                .plan_positive_taps(u, v)
                .expect("full tap plan should locate");
            assert_eq!(center, taps.center());
        }

        assert_eq!(gridder.locate_positive_tap_center(f64::NAN, 0.0), None);
        assert_eq!(gridder.locate_positive_tap_center(1.0e12, 0.0), None);
    }

    #[test]
    fn compact_positive_tap_grid_and_degrid_match_product_taps() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0 / 206_264.806_247, 1.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let plan = gridder
            .plan_sample(10.75, -5.5)
            .expect("legacy product plan");
        let positive = gridder
            .plan_positive_taps(10.75, -5.5)
            .expect("compact positive plan");
        let mut product_grid = Array2::<Complex64>::zeros(gridder.grid_shape());
        let mut compact_grid = Array2::<Complex64>::zeros(gridder.grid_shape());
        let value = Complex64::new(3.25, -1.75);

        gridder.grid_sample_product_planned_f64(&mut product_grid, &plan.positive, value);
        gridder.grid_sample_taps_planned_f64(&mut compact_grid, &positive, value);
        assert_eq!(product_grid, compact_grid);

        let model = product_grid.mapv(|value| Complex32::new(value.re as f32, value.im as f32));
        let product_degrid = gridder.degrid_sample_product_planned(&model, &plan.positive);
        let compact_degrid = gridder.degrid_sample_taps_planned_normalized(&model, &positive);
        assert_eq!(product_degrid, compact_degrid);
    }

    #[test]
    fn fused_residual_refresh_matches_separate_degrid_grid() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0 / 206_264.806_247, 1.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let positive = gridder
            .plan_positive_taps(10.75, -5.5)
            .expect("compact positive plan");
        let mut model = Array2::<Complex32>::zeros(gridder.grid_shape());
        for ((x, y), cell) in model.indexed_iter_mut() {
            *cell = Complex32::new(
                (x as f32 * 0.0125) - (y as f32 * 0.003),
                (y as f32 * 0.0075) + (x as f32 * 0.002),
            );
        }

        let observed = Complex32::new(3.25, -1.75);
        let residual_weight = 2.5;
        let predicted = gridder.degrid_sample_taps_planned_normalized(&model, &positive);
        let residual_visibility = observed - predicted;
        let residual = Complex64::new(
            f64::from(residual_visibility.re) * residual_weight,
            f64::from(residual_visibility.im) * residual_weight,
        );
        let mut separate_grid = Array2::<Complex64>::zeros(gridder.grid_shape());
        let mut fused_grid = Array2::<Complex64>::zeros(gridder.grid_shape());

        gridder.grid_sample_taps_planned_f64(&mut separate_grid, &positive, residual);
        let fused_predicted = gridder.degrid_model_and_grid_residual_taps_planned_f64(
            &model,
            &mut fused_grid,
            &positive,
            observed,
            residual_weight,
        );

        assert_eq!(fused_predicted, predicted);
        assert_eq!(fused_grid, separate_grid);
    }

    #[test]
    fn kernel_is_even_and_compact() {
        assert!(
            (super::spheroidal_kernel(0.25, 3.0) - super::spheroidal_kernel(0.25, 3.0)).abs()
                < 1.0e-6
        );
        assert_eq!(super::spheroidal_kernel(3.5, 3.0), 0.0);
    }

    #[test]
    fn correction_is_finite_interior() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let correction = gridder.correction_image();
        assert!(correction[(16, 16)].is_finite());
        assert!(correction[(16, 16)] > 0.0);
    }

    #[test]
    fn hetarray_screen_size_keeps_at_least_image_sampling() {
        let geometry = ImageGeometry {
            image_shape: [800, 800],
            cell_size_rad: [1.0e-6, 1.0e-6],
        };
        assert_eq!(super::hetarray_screen_conv_size(geometry), 800);
        assert_eq!(
            super::hetarray_screen_conv_size_for_support(geometry, 224),
            800
        );
        assert_eq!(
            super::hetarray_screen_conv_size_for_support(geometry, 524),
            800
        );
        assert_eq!(
            super::hetarray_screen_conv_size_for_support(geometry, 1040),
            1072
        );
        assert_eq!(
            super::hetarray_screen_conv_size(ImageGeometry {
                image_shape: [64, 64],
                cell_size_rad: [1.0e-6, 1.0e-6],
            }),
            64
        );
    }

    #[test]
    fn degridding_is_adjoint_to_gridding_for_single_sample() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let mut a_grid = Array2::<Complex32>::zeros((32, 32));
        let sample = Complex32::new(0.5, -0.25);
        assert!(gridder.grid_sample(&mut a_grid, 120.0, -80.0, sample));

        let mut b_grid = Array2::<Complex32>::zeros((32, 32));
        for ((x, y), value) in b_grid.indexed_iter_mut() {
            *value = Complex32::new((x as f32 - 10.0) * 0.01, (y as f32 - 6.0) * -0.02);
        }

        let lhs: Complex32 = a_grid
            .iter()
            .zip(b_grid.iter())
            .map(|(a, b)| a.conj() * *b)
            .sum();
        let rhs = sample.conj() * gridder.degrid_sample(&b_grid, 120.0, -80.0).unwrap();

        assert!((lhs.re - rhs.re).abs() < 1.0e-4);
        assert!((lhs.im - rhs.im).abs() < 1.0e-4);
    }

    #[test]
    fn shifted_screen_projector_reconstructs_ra_peak_at_expected_offset() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [
                (1.0f64 / 3600.0).to_radians(),
                (1.0f64 / 3600.0).to_radians(),
            ],
        };
        let cell = geometry.cell_size_rad[0];
        let delta_l = 6.0 * cell;
        let sigma = 3.0 * cell;
        let gridder = StandardGridder::new(geometry).expect("gridder");
        let projector = ScreenProjector::from_screen(geometry, &gridder, 4, |l, m| {
            let radius_sq = l * l + m * m;
            let value = (-0.5 * radius_sq / (sigma * sigma)).exp() as f32;
            Complex32::new(value, 0.0)
        })
        .expect("screen projector")
        .with_phase_gradient([
            -(delta_l / cell) * std::f64::consts::TAU / (geometry.image_shape[0] as f64 * 4.0),
            0.0,
        ]);
        let plan = projector.plan_sample(0.0, 0.0).expect("origin sample plan");
        let [grid_nx, grid_ny] = gridder.grid_shape();
        let mut grid = Array2::<Complex32>::zeros((grid_nx, grid_ny));
        projector.grid_sample_planned(&mut grid, &plan, Complex32::new(1.0, 0.0));
        let raw = centered_ifft2(&grid);
        let image = gridder.corrected_w_project_image_from_grid(&raw, 4);
        let peak = peak_location(&image);
        let expected_x = geometry.image_shape[0] / 2 + 6;
        let expected_y = geometry.image_shape[1] / 2;
        assert_peak_within_tolerance(peak, (expected_x, expected_y), 2);
    }

    #[test]
    fn screen_projector_grid_plan_skips_trace_only_normalization() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [
                (1.0f64 / 3600.0).to_radians(),
                (1.0f64 / 3600.0).to_radians(),
            ],
        };
        let gridder = StandardGridder::new(geometry).expect("gridder");
        let projector = ScreenProjector::from_screen(geometry, &gridder, 4, |l, m| {
            let radius_sq = l * l + m * m;
            Complex32::new((-0.5 * radius_sq / 1.0e-10).exp() as f32, 0.0)
        })
        .expect("screen projector");

        let traced = projector
            .plan_sample(12.25, -7.5)
            .expect("normalized sample plan");
        let gridding = projector
            .plan_sample_for_grid(12.25, -7.5)
            .expect("gridding sample plan");

        assert_eq!(gridding.loc_x, traced.loc_x);
        assert_eq!(gridding.loc_y, traced.loc_y);
        assert_eq!(gridding.off_x, traced.off_x);
        assert_eq!(gridding.off_y, traced.off_y);
        assert_eq!(gridding.min_ix, traced.min_ix);
        assert_eq!(gridding.max_ix, traced.max_ix);
        assert_eq!(gridding.min_iy, traced.min_iy);
        assert_eq!(gridding.max_iy, traced.max_iy);
        assert_eq!(gridding.center_in_bounds, traced.center_in_bounds);
        assert!(traced.normalization > 0.0);
        assert_eq!(gridding.normalization, 0.0);
    }

    #[test]
    fn w_project_sample_normalization_uses_cached_plane_offset_value() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [
                (0.25f64 / 3600.0).to_radians(),
                (0.25f64 / 3600.0).to_radians(),
            ],
        };
        let gridder = StandardGridder::new_with_casa_composite_padding(geometry).unwrap();
        let projector = WProjector::new(geometry, &gridder, 20_000.0, Some(8)).unwrap();
        let plan = projector
            .plan_sample(12_000.25, -9_000.75, -4_000.0)
            .expect("wproject sample should plan inside the grid");

        let cached = projector
            .sample_normalization(plan.plane_index, plan.off_x, plan.off_y)
            .expect("cached normalization");
        let direct = super::compute_w_project_sample_normalization(
            &projector.kernels[plan.plane_index],
            projector.sampling,
            plan.off_x,
            plan.off_y,
        )
        .expect("direct normalization");

        assert!((cached - direct).abs() <= f32::EPSILON);
        assert_eq!(cached, plan.normalization);
    }

    #[test]
    fn shifted_screen_projector_reconstructs_dec_peak_at_expected_offset() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [
                (1.0f64 / 3600.0).to_radians(),
                (1.0f64 / 3600.0).to_radians(),
            ],
        };
        let cell = geometry.cell_size_rad[0];
        let delta_m = 9.0 * cell;
        let sigma = 3.0 * cell;
        let gridder = StandardGridder::new(geometry).expect("gridder");
        let projector = ScreenProjector::from_screen(geometry, &gridder, 4, |l, m| {
            let radius_sq = l * l + m * m;
            let value = (-0.5 * radius_sq / (sigma * sigma)).exp() as f32;
            Complex32::new(value, 0.0)
        })
        .expect("screen projector")
        .with_phase_gradient([
            0.0,
            (delta_m / cell) * std::f64::consts::TAU / (geometry.image_shape[1] as f64 * 4.0),
        ]);
        let plan = projector.plan_sample(0.0, 0.0).expect("origin sample plan");
        let [grid_nx, grid_ny] = gridder.grid_shape();
        let mut grid = Array2::<Complex32>::zeros((grid_nx, grid_ny));
        projector.grid_sample_planned(&mut grid, &plan, Complex32::new(1.0, 0.0));
        let raw = centered_ifft2(&grid);
        let image = gridder.corrected_w_project_image_from_grid(&raw, 4);
        let peak = peak_location(&image);
        let expected_x = geometry.image_shape[0] / 2;
        let expected_y = geometry.image_shape[1] / 2 - 9;
        assert_peak_within_tolerance(peak, (expected_x, expected_y), 2);
    }

    #[test]
    fn constant_uv_grid_degrids_to_constant_value() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let grid = Array2::<Complex32>::from_elem((32, 32), Complex32::new(2.5, -1.0));
        let value = gridder.degrid_sample(&grid, 55.0, -40.0).unwrap();
        assert!((value.re - 2.5).abs() < 1.0e-4);
        assert!((value.im + 1.0).abs() < 1.0e-4);
    }

    #[test]
    fn density_cells_truncate_to_image_weight_grid() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let center = (16usize, 16usize);
        assert_eq!(gridder.density_cell_index(0.0, 0.0), Some(center));

        let du = 1.0 / (32.0 * 1.0e-4);
        let dv = 1.0 / (32.0 * 1.0e-4);
        assert_eq!(
            gridder.density_cell_index(0.99 * du, -0.99 * dv),
            Some((center.0 - 1, center.1 - 1))
        );
        assert_eq!(
            gridder.density_cell_index(1.01 * du, -1.01 * dv),
            Some((center.0 - 2, center.1 - 2))
        );
        assert_eq!(
            gridder.density_cell_index(-1.01 * du, 1.01 * dv),
            Some((center.0 + 1, center.1 + 1))
        );
    }

    #[test]
    fn cube_density_cells_round_to_nearest_weight_grid_location() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let center = (16usize, 16usize);
        let du = 1.0 / (32.0 * 1.0e-4);
        let dv = 1.0 / (32.0 * 1.0e-4);
        assert_eq!(
            gridder.density_cell_index_with_convention(
                0.49 * du,
                -0.49 * dv,
                DensityCellConvention::CubeBriggsWeightorLookup,
            ),
            Some(center)
        );
        assert_eq!(
            gridder.density_cell_index_with_convention(
                0.51 * du,
                -0.51 * dv,
                DensityCellConvention::CubeBriggsWeightorLookup,
            ),
            Some((center.0 + 1, center.1 + 1))
        );
    }

    #[test]
    fn padded_grid_accepts_samples_outside_image_extent() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let mut grid =
            Array2::<Complex32>::zeros((gridder.grid_shape()[0], gridder.grid_shape()[1]));
        let x_target = gridder.geometry.nx() as f64 + 4.0;
        let y_target = gridder.geometry.ny() as f64 + 3.0;
        let u_lambda = (x_target - gridder.grid_shape()[0] as f64 / 2.0) * gridder.du_lambda;
        let v_lambda = -(y_target - gridder.grid_shape()[1] as f64 / 2.0) * gridder.dv_lambda;

        assert!(gridder.grid_sample(&mut grid, u_lambda, v_lambda, Complex32::new(1.0, 0.0)));
        assert!(grid.iter().any(|value| value.norm() > 0.0));
        assert!(gridder.degrid_sample(&grid, u_lambda, v_lambda).is_some());
    }

    #[test]
    fn paired_f64_product_grid_matches_separate_updates() {
        let gridder = StandardGridder::new(ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        })
        .unwrap();
        let plan = gridder
            .plan_sample(42.25, -17.5)
            .expect("sample should lie on grid");
        let shape = (gridder.grid_shape()[0], gridder.grid_shape()[1]);
        let first_value = Complex64::new(1.5, -0.25);
        let second_value = Complex64::new(-2.0, 0.75);

        let mut separate_first = Array2::<Complex64>::zeros(shape);
        let mut separate_second = Array2::<Complex64>::zeros(shape);
        gridder.grid_sample_product_planned_f64(&mut separate_first, &plan.positive, first_value);
        gridder.grid_sample_product_planned_f64(&mut separate_second, &plan.positive, second_value);
        let mut separate_real = Array2::<Complex64>::zeros(shape);
        gridder.grid_sample_product_planned_f64(
            &mut separate_real,
            &plan.positive,
            Complex64::new(first_value.re, 0.0),
        );

        let mut paired_first = Array2::<Complex64>::zeros(shape);
        let mut paired_second = Array2::<Complex64>::zeros(shape);
        gridder.grid_sample_product_pair_planned_f64(
            &mut paired_first,
            first_value,
            &mut paired_second,
            second_value,
            &plan.positive,
        );

        assert_eq!(paired_first, separate_first);
        assert_eq!(paired_second, separate_second);

        let mut real = Array2::<Complex64>::zeros(shape);
        gridder.grid_sample_product_real_planned_f64(&mut real, &plan.positive, first_value.re);
        assert_eq!(real, separate_real);
    }

    #[test]
    #[serial(casa_cpp)]
    fn convolve_gridder_patch_matches_casacore_for_fractional_sample() {
        let geometry = ImageGeometry {
            image_shape: [100, 100],
            cell_size_rad: [8.0 / 206_264.806_247, 8.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let sample = [123.456_f64, -78.9_f64];
        let mut rust_grid =
            Array2::<Complex32>::zeros((gridder.grid_shape()[0], gridder.grid_shape()[1]));
        let plan = gridder
            .plan_sample(sample[0], sample[1])
            .expect("sample should lie on grid");
        gridder.grid_sample_planned(
            &mut rust_grid,
            &plan.positive_x,
            &plan.positive_y,
            Complex32::new(1.0, 0.0),
        );

        let mut rust_cells = Vec::new();
        for ((x, y), value) in rust_grid.indexed_iter() {
            if *value != Complex32::new(0.0, 0.0) {
                rust_cells.push((x, y, value.re, value.im));
            }
        }
        rust_cells.sort_by_key(|cell| (cell.1, cell.0));

        let Ok(cpp_patch) = cpp_convolve_gridder_grid_unit_sample_2d(
            gridder.grid_shape(),
            [
                gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
            ],
            [
                gridder.grid_shape()[0] as f64 / 2.0,
                gridder.grid_shape()[1] as f64 / 2.0,
            ],
            [sample[0], -sample[1]],
        ) else {
            return;
        };
        let mut cpp_cells = cpp_patch
            .cells
            .into_iter()
            .map(|cell| (cell.x, cell.y, cell.re, cell.im))
            .collect::<Vec<_>>();
        cpp_cells.sort_by_key(|cell| (cell.1, cell.0));

        assert_eq!(rust_cells.len(), cpp_cells.len(), "nonzero patch size");
        for (rust, cpp) in rust_cells.iter().zip(&cpp_cells) {
            assert_eq!((rust.0, rust.1), (cpp.0, cpp.1), "grid cell index");
            assert!(
                (rust.2 - cpp.2).abs() < 1.0e-6,
                "cell real mismatch at ({}, {}): rust={} cpp={}",
                rust.0,
                rust.1,
                rust.2,
                cpp.2
            );
            assert!(
                (rust.3 - cpp.3).abs() < 1.0e-6,
                "cell imag mismatch at ({}, {}): rust={} cpp={}",
                rust.0,
                rust.1,
                rust.3,
                cpp.3
            );
        }
    }

    #[test]
    #[serial(casa_cpp)]
    fn convolve_gridder_correction_row_matches_casacore() {
        let geometry = ImageGeometry {
            image_shape: [100, 100],
            cell_size_rad: [8.0 / 206_264.806_247, 8.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let correction = gridder.correction_image();
        let locy = gridder.grid_shape()[1] / 2 + 7;
        let Ok(cpp_row) = cpp_convolve_gridder_correction_row_2d(
            gridder.grid_shape(),
            [
                gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
            ],
            [
                gridder.grid_shape()[0] as f64 / 2.0,
                gridder.grid_shape()[1] as f64 / 2.0,
            ],
            locy,
        ) else {
            return;
        };
        for (x, expected) in cpp_row.iter().enumerate() {
            let actual = correction[(x, locy)];
            let expected_inverse = if *expected > 1.0e-6 {
                1.0 / *expected
            } else {
                0.0
            };
            assert!(
                (actual - expected_inverse).abs() < 1.0e-4,
                "correction row mismatch at ({x}, {locy}): rust={actual} cpp_inverse={expected_inverse}"
            );
        }
    }

    #[test]
    fn apodize_model_applies_casa_degrid_correction() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [8.0 / 206_264.806_247, 8.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut model = Array2::<f32>::zeros((32, 32));
        model[(16, 16)] = 1.0;
        model[(11, 20)] = -0.25;
        model[(25, 7)] = 0.5;

        let apodized = gridder.apodize_model(&model);
        for x in 0..geometry.nx() {
            for y in 0..geometry.ny() {
                let grid_x = gridder.image_blc[0] + x;
                let grid_y = gridder.image_blc[1] + y;
                let correction = gridder.correction_x[grid_x] * gridder.correction_y[grid_y];
                let expected = model[(x, y)] * correction;
                let actual = apodized[(grid_x, grid_y)].re;
                assert!(
                    (expected - actual).abs() < 1.0e-6,
                    "model degrid correction mismatch at ({x}, {y}): expected={expected} actual={actual}"
                );
            }
        }
    }

    #[test]
    #[serial(casa_cpp)]
    fn convolve_gridder_accumulates_multiple_fractional_samples_like_casacore() {
        let geometry = ImageGeometry {
            image_shape: [100, 100],
            cell_size_rad: [8.0 / 206_264.806_247, 8.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut rust_grid =
            Array2::<Complex32>::zeros((gridder.grid_shape()[0], gridder.grid_shape()[1]));
        let mut cpp_grid =
            Array2::<Complex32>::zeros((gridder.grid_shape()[0], gridder.grid_shape()[1]));
        let samples = [
            (123.456_f64, -78.9_f64, Complex32::new(1.0, 0.0)),
            (-210.25_f64, 98.125_f64, Complex32::new(0.5, -0.25)),
            (15.875_f64, 144.625_f64, Complex32::new(-0.75, 0.125)),
            (301.4_f64, -12.2_f64, Complex32::new(0.33, 0.44)),
        ];

        for &(u, v, value) in &samples {
            assert!(gridder.grid_sample(&mut rust_grid, u, v, value));
            let Ok(cpp_patch) = cpp_convolve_gridder_grid_unit_sample_2d(
                gridder.grid_shape(),
                [
                    gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                    gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
                ],
                [
                    gridder.grid_shape()[0] as f64 / 2.0,
                    gridder.grid_shape()[1] as f64 / 2.0,
                ],
                [u, -v],
            ) else {
                return;
            };
            for cell in cpp_patch.cells {
                cpp_grid[(cell.x, cell.y)] += value * Complex32::new(cell.re, cell.im);
            }
        }

        for ((x, y), rust_value) in rust_grid.indexed_iter() {
            let cpp_value = cpp_grid[(x, y)];
            assert!(
                (rust_value.re - cpp_value.re).abs() < 1.0e-5,
                "grid real mismatch at ({x}, {y}): rust={} cpp={}",
                rust_value.re,
                cpp_value.re
            );
            assert!(
                (rust_value.im - cpp_value.im).abs() < 1.0e-5,
                "grid imag mismatch at ({x}, {y}): rust={} cpp={}",
                rust_value.im,
                cpp_value.im
            );
        }
    }

    #[test]
    fn standard_gridder_uses_casa_composite_grid_padding() {
        let geometry = ImageGeometry {
            image_shape: [512, 512],
            cell_size_rad: [0.08 / 206_264.806_247, 0.08 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        assert_eq!(gridder.grid_shape(), [640, 640]);
    }

    #[test]
    #[serial(casa_cpp)]
    fn convolve_gridder_degrids_structured_model_like_casacore() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [8.0 / 206_264.806_247, 8.0 / 206_264.806_247],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut model = Array2::<f32>::zeros((64, 64));
        model[(32, 32)] = 1.0;
        model[(21, 45)] = -0.35;
        model[(47, 18)] = 0.2;
        let model_grid = centered_fft2(&gridder.apodize_model(&model));
        let samples = [
            (123.456_f64, -78.9_f64),
            (-210.25_f64, 98.125_f64),
            (15.875_f64, 144.625_f64),
            (301.4_f64, -12.2_f64),
        ];

        for &(u, v) in &samples {
            let plan = gridder
                .plan_sample(u, v)
                .expect("sample should lie on grid");
            let rust = gridder.degrid_sample_product_planned(&model_grid, &plan.positive);
            let Ok(cpp) = cpp_convolve_gridder_predict_visibility_2d(
                gridder.grid_shape(),
                geometry.image_shape,
                [
                    gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                    gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
                ],
                [
                    gridder.grid_shape()[0] as f64 / 2.0,
                    gridder.grid_shape()[1] as f64 / 2.0,
                ],
                [u, -v],
                model.as_slice().unwrap(),
            ) else {
                return;
            };
            assert!(
                (rust.re - cpp.re).abs() < 1.0e-6,
                "predicted visibility real mismatch at ({u}, {v}): rust={} cpp={}",
                rust.re,
                cpp.re
            );
            assert!(
                (rust.im - cpp.im).abs() < 1.0e-6,
                "predicted visibility imag mismatch at ({u}, {v}): rust={} cpp={}",
                rust.im,
                cpp.im
            );
        }
    }

    #[test]
    #[serial(casa_cpp)]
    fn convolve_gridder_degrids_odd_composite_padded_model_like_casacore() {
        let geometry = ImageGeometry {
            image_shape: [257, 257],
            cell_size_rad: [8.638_889_530_690e-7_f64.to_radians(); 2],
        };
        let gridder = StandardGridder::new_with_casa_composite_padding(geometry).unwrap();
        assert_eq!(gridder.grid_shape(), [360, 360]);

        let mut model = Array2::<f32>::zeros((257, 257));
        for x in 0..257 {
            for y in 0..257 {
                let dx = (x as f32 - 129.25) / 27.0;
                let dy = (y as f32 - 126.5) / 19.0;
                let ring = (-(dx * dx + dy * dy)).exp();
                let shoulder = (-(((x as f32 - 87.0) / 13.0).powi(2)
                    + ((y as f32 - 169.0) / 21.0).powi(2)))
                .exp();
                model[(x, y)] = 0.0025 * ring - 0.0007 * shoulder;
            }
        }
        let model_grid = centered_fft2(&gridder.apodize_model(&model));
        let samples = [
            (4_806.297_926_382_51_f64, 41_290.840_313_424_32_f64),
            (-38_890.191_177_123_3_f64, -12_300.584_882_047_77_f64),
            (24_915.177_739_689_71_f64, -34_020.365_105_376_14_f64),
            (-9_024.365_419_946_97_f64, 7_115.436_092_750_48_f64),
        ];

        for &(u, v) in &samples {
            let plan = gridder
                .plan_sample(u, v)
                .expect("sample should lie on grid");
            let rust = gridder.degrid_sample_product_planned(&model_grid, &plan.positive);
            let Ok(cpp) = cpp_convolve_gridder_predict_visibility_2d(
                gridder.grid_shape(),
                geometry.image_shape,
                [
                    gridder.grid_shape()[0] as f64 * geometry.cell_size_rad[0],
                    gridder.grid_shape()[1] as f64 * geometry.cell_size_rad[1],
                ],
                [
                    gridder.grid_shape()[0] as f64 / 2.0,
                    gridder.grid_shape()[1] as f64 / 2.0,
                ],
                [u, -v],
                model.as_slice().unwrap(),
            ) else {
                return;
            };
            assert!(
                (rust.re - cpp.re).abs() < 1.0e-6,
                "predicted visibility real mismatch at ({u}, {v}): rust={} cpp={}",
                rust.re,
                cpp.re
            );
            assert!(
                (rust.im - cpp.im).abs() < 1.0e-6,
                "predicted visibility imag mismatch at ({u}, {v}): rust={} cpp={}",
                rust.im,
                cpp.im
            );
        }
    }
}
