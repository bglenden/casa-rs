// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concrete prolate-spheroidal gridding and degridding helpers.

use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{ImageGeometry, ImagingError, fft::fft2};

const GRIDDER_SUPPORT: usize = 3;
const GRIDDER_TAP_COUNT: usize = GRIDDER_SUPPORT * 2 + 1;
const GRIDDER_PRODUCT_TAP_COUNT: usize = GRIDDER_TAP_COUNT * GRIDDER_TAP_COUNT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DensityCellConvention {
    VisImagingWeight,
    CubeBriggsWeightor,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TapSet {
    pub(crate) indices: [usize; GRIDDER_TAP_COUNT],
    pub(crate) weights: [f32; GRIDDER_TAP_COUNT],
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProductTapSet {
    pub(crate) x_indices: [usize; GRIDDER_PRODUCT_TAP_COUNT],
    pub(crate) y_indices: [usize; GRIDDER_PRODUCT_TAP_COUNT],
    pub(crate) flat_indices: [usize; GRIDDER_PRODUCT_TAP_COUNT],
    pub(crate) weights: [f32; GRIDDER_PRODUCT_TAP_COUNT],
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
    correction_x: Vec<f32>,
    correction_y: Vec<f32>,
    du_lambda: f64,
    dv_lambda: f64,
}

impl StandardGridder {
    pub(crate) fn new(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Self::new_with_padding(geometry, padded_len)
    }

    pub(crate) fn new_with_casa_composite_padding(
        geometry: ImageGeometry,
    ) -> Result<Self, ImagingError> {
        Self::new_with_padding(geometry, casa_composite_padded_len)
    }

    fn new_with_padding(
        geometry: ImageGeometry,
        padded_len_for_axis: fn(usize, f64) -> usize,
    ) -> Result<Self, ImagingError> {
        geometry.validate()?;

        let grid_shape = [
            padded_len_for_axis(geometry.nx(), 1.2),
            padded_len_for_axis(geometry.ny(), 1.2),
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
        let correction_x = build_correction_axis(grid_shape[0]);
        let correction_y = build_correction_axis(grid_shape[1]);

        Ok(Self {
            du_lambda: 1.0 / (grid_shape[0] as f64 * geometry.cell_size_rad[0]),
            dv_lambda: 1.0 / (grid_shape[1] as f64 * geometry.cell_size_rad[1]),
            geometry,
            grid_shape,
            image_blc,
            oversampling,
            kernel_table,
            correction_x,
            correction_y,
        })
    }

    pub(crate) fn grid_shape(&self) -> [usize; 2] {
        self.grid_shape
    }

    pub(crate) fn grid_spacing_lambda(&self) -> [f64; 2] {
        [self.du_lambda, self.dv_lambda]
    }

    pub(crate) fn density_grid_shape(&self) -> [usize; 2] {
        [self.geometry.nx(), self.geometry.ny()]
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
                storage[taps.flat_indices[tap]] += value * taps.weights[tap];
            }
            return;
        }
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            grid[(taps.x_indices[tap], taps.y_indices[tap])] += value * taps.weights[tap];
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
                storage[taps.flat_indices[tap]] += value * f64::from(taps.weights[tap]);
            }
            return;
        }
        for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
            grid[(taps.x_indices[tap], taps.y_indices[tap])] +=
                value * f64::from(taps.weights[tap]);
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
            value += grid[(taps.x_indices[tap], taps.y_indices[tap])] * taps.weights[tap];
        }
        value
    }

    pub(crate) fn degrid_sample_product_planned_normalized(
        &self,
        grid: &Array2<Complex32>,
        taps: &ProductTapSet,
    ) -> Complex32 {
        let mut value = Complex32::new(0.0, 0.0);
        let mut norm = 0.0f32;
        if let Some(storage) = grid.as_slice_memory_order() {
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                let weight = taps.weights[tap];
                value += storage[taps.flat_indices[tap]] * weight;
                norm += weight;
            }
        } else {
            for tap in 0..GRIDDER_PRODUCT_TAP_COUNT {
                let weight = taps.weights[tap];
                value += grid[(taps.x_indices[tap], taps.y_indices[tap])] * weight;
                norm += weight;
            }
        }
        if norm > 0.0 && norm.is_finite() {
            value / norm
        } else {
            Complex32::new(0.0, 0.0)
        }
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
        let nx = self.geometry.nx() as f64;
        let ny = self.geometry.ny() as f64;
        let u_cells = u_lambda * nx * self.geometry.cell_size_rad[0];
        let v_cells = v_lambda * ny * self.geometry.cell_size_rad[1];
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
        let nx = self.geometry.nx() as f64;
        let ny = self.geometry.ny() as f64;
        let (x, y) = match convention {
            DensityCellConvention::VisImagingWeight => {
                let u = f64::from(u_lambda as f32);
                let v = f64::from(v_lambda as f32);
                (
                    -u * nx * self.geometry.cell_size_rad[0] + nx / 2.0,
                    v * ny * self.geometry.cell_size_rad[1] + ny / 2.0,
                )
            }
            DensityCellConvention::CubeBriggsWeightor => (
                u_lambda * nx * self.geometry.cell_size_rad[0] + nx / 2.0,
                -v_lambda * ny * self.geometry.cell_size_rad[1] + ny / 2.0,
            ),
        };
        if !(x.is_finite() && y.is_finite()) {
            return None;
        }
        let (anchor_x, anchor_y) = match convention {
            DensityCellConvention::VisImagingWeight => (x as isize, y as isize),
            DensityCellConvention::CubeBriggsWeightor => (x.round() as isize, y.round() as isize),
        };
        if anchor_x <= 0
            || anchor_y <= 0
            || anchor_x >= self.geometry.nx() as isize
            || anchor_y >= self.geometry.ny() as isize
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
        for weight in &mut weights {
            *weight /= norm;
        }
        Some(TapSet { indices, weights })
    }
}

fn flatten_tap_products(x_taps: &TapSet, y_taps: &TapSet, grid_stride: usize) -> ProductTapSet {
    let mut x_indices = [0usize; GRIDDER_PRODUCT_TAP_COUNT];
    let mut y_indices = [0usize; GRIDDER_PRODUCT_TAP_COUNT];
    let mut flat_indices = [0usize; GRIDDER_PRODUCT_TAP_COUNT];
    let mut weights = [0.0f32; GRIDDER_PRODUCT_TAP_COUNT];
    let mut slot = 0usize;
    for x_tap in 0..GRIDDER_TAP_COUNT {
        let x_index = x_taps.indices[x_tap];
        let x_weight = x_taps.weights[x_tap];
        for y_tap in 0..GRIDDER_TAP_COUNT {
            x_indices[slot] = x_index;
            y_indices[slot] = y_taps.indices[y_tap];
            flat_indices[slot] = x_index * grid_stride + y_indices[slot];
            weights[slot] = x_weight * y_taps.weights[y_tap];
            slot += 1;
        }
    }
    ProductTapSet {
        x_indices,
        y_indices,
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

pub(crate) struct ScreenProjector {
    grid_shape: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    sampling: usize,
    support: usize,
    kernel_center: usize,
    kernel_weights: Array2<Complex32>,
    phase_gradient_rad_per_sample: [f64; 2],
}

impl ScreenProjector {
    pub(crate) fn support(&self) -> usize {
        self.support
    }

    pub(crate) fn sampling(&self) -> usize {
        self.sampling
    }

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
        let s0 = geometry.cell_size_rad[0].abs() * sampling as f64 * grid_shape[0] as f64
            / conv_size as f64;
        let s1 = geometry.cell_size_rad[1].abs() * sampling as f64 * grid_shape[1] as f64
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
        let support = find_screen_projector_support(&weights, sampling);
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
            kernel_weights,
            phase_gradient_rad_per_sample: [0.0, 0.0],
        })
    }

    pub(crate) fn with_phase_gradient(mut self, phase_gradient_rad_per_sample: [f64; 2]) -> Self {
        self.phase_gradient_rad_per_sample = phase_gradient_rad_per_sample;
        self
    }

    pub(crate) fn plan_sample(
        &self,
        u_lambda: f64,
        v_lambda: f64,
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
        let normalization =
            self.sample_normalization(off_x, off_y, min_ix, max_ix, min_iy, max_iy)?;
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
                let signed_y = iy * self.sampling as isize + plan.off_y;
                let kernel_x = (self.kernel_center as isize + signed_x) as usize;
                let phase = signed_x as f64 * self.phase_gradient_rad_per_sample[0]
                    + signed_y as f64 * self.phase_gradient_rad_per_sample[1];
                let phasor = Complex32::new(phase.cos() as f32, phase.sin() as f32);
                let cwt = self.kernel_weights[(kernel_x, kernel_y)] * phasor;
                grid[((plan.loc_x + ix) as usize, (plan.loc_y + iy) as usize)] += value * cwt;
            }
        }
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
                let signed_y = iy * self.sampling as isize + off_y;
                let kernel_x = usize::try_from(self.kernel_center as isize + signed_x).ok()?;
                let phase = signed_x as f64 * self.phase_gradient_rad_per_sample[0]
                    + signed_y as f64 * self.phase_gradient_rad_per_sample[1];
                let phasor = Complex32::new(phase.cos() as f32, phase.sin() as f32);
                let value = *self.kernel_weights.get((kernel_x, kernel_y))? * phasor;
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

        Ok(Self {
            grid_shape,
            du_lambda: gridder.grid_spacing_lambda()[0],
            dv_lambda: gridder.grid_spacing_lambda()[1],
            sampling,
            w_scale,
            kernels,
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
        let normalization = self.sample_normalization(kernel, off_x, off_y)?;
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

    pub(crate) fn plane_count(&self) -> usize {
        self.kernels.len()
    }

    pub(crate) fn w_scale(&self) -> f64 {
        self.w_scale
    }

    pub(crate) fn kernel_support(&self, plane_index: usize) -> usize {
        self.kernels[plane_index].support
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

    fn sample_normalization(
        &self,
        kernel: &WProjectKernel,
        off_x: isize,
        off_y: isize,
    ) -> Option<f32> {
        let support = kernel.support as isize;
        let mut normalization = 0.0f32;
        for iy in -support..=support {
            let kernel_y = (iy * self.sampling as isize + off_y).unsigned_abs();
            for ix in -support..=support {
                let kernel_x = (ix * self.sampling as isize + off_x).unsigned_abs();
                let value = *kernel.weights.get((kernel_x, kernel_y))?;
                normalization += value.re;
            }
        }
        Some(normalization)
    }
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

fn screen_projector_conv_size(geometry: ImageGeometry, sampling: usize) -> usize {
    let image_max = geometry.nx().max(geometry.ny());
    let scaled = ((image_max * sampling).max(4)) / 4;
    if scaled % 2 == 0 { scaled } else { scaled + 1 }
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
    use num_complex::Complex32;
    use serial_test::serial;

    use super::{DensityCellConvention, ScreenProjector, StandardGridder};
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
                DensityCellConvention::CubeBriggsWeightor,
            ),
            Some(center)
        );
        assert_eq!(
            gridder.density_cell_index_with_convention(
                0.51 * du,
                -0.51 * dv,
                DensityCellConvention::CubeBriggsWeightor,
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
}
