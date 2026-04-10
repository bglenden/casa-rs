// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concrete prolate-spheroidal gridding and degridding helpers.

use ndarray::Array2;
use num_complex::Complex32;

use crate::{ImageGeometry, ImagingError};

const GRIDDER_SUPPORT: usize = 3;
const GRIDDER_TAP_COUNT: usize = GRIDDER_SUPPORT * 2 + 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DensityCellConvention {
    VisImagingWeight,
    CubeBriggsWeightor,
}

#[derive(Clone, Copy)]
pub(crate) struct TapSet {
    pub(crate) indices: [usize; GRIDDER_TAP_COUNT],
    pub(crate) weights: [f32; GRIDDER_TAP_COUNT],
}

#[derive(Clone, Copy)]
pub(crate) struct PlannedSample {
    pub(crate) positive_x: TapSet,
    pub(crate) positive_y: TapSet,
    pub(crate) negative_x: TapSet,
    pub(crate) negative_y: TapSet,
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
        geometry.validate()?;

        let grid_shape = [
            padded_len(geometry.nx(), 1.2),
            padded_len(geometry.ny(), 1.2),
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
        Some(PlannedSample {
            positive_x,
            positive_y,
            negative_x,
            negative_y,
        })
    }

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
        let mut apodized = Array2::<Complex32>::zeros((self.grid_shape[0], self.grid_shape[1]));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                let correction = self.correction_x[grid_x] * self.correction_y[grid_y];
                if correction > 1.0e-6 {
                    apodized[(grid_x, grid_y)] = Complex32::new(model[(x, y)] * correction, 0.0);
                }
            }
        }
        apodized
    }

    pub(crate) fn corrected_image_from_grid(&self, raw: &Array2<Complex32>) -> Array2<f32> {
        let mut image = Array2::<f32>::zeros((self.geometry.nx(), self.geometry.ny()));
        for x in 0..self.geometry.nx() {
            for y in 0..self.geometry.ny() {
                let grid_x = self.image_blc[0] + x;
                let grid_y = self.image_blc[1] + y;
                image[(x, y)] = raw[(grid_x, grid_y)].re
                    * self.correction_x[grid_x]
                    * self.correction_y[grid_y];
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
        let x = u_lambda * self.geometry.nx() as f64 * self.geometry.cell_size_rad[0]
            + self.geometry.nx() as f64 / 2.0;
        let y = -v_lambda * self.geometry.ny() as f64 * self.geometry.cell_size_rad[1]
            + self.geometry.ny() as f64 / 2.0;
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

fn padded_len(image_len: usize, padding_factor: f64) -> usize {
    let padded = (padding_factor * image_len as f64 - 0.5).floor() as usize;
    let padded = padded.max(image_len);
    if padded % 2 == 0 { padded } else { padded + 1 }
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

#[cfg(test)]
mod tests {
    use casa_test_support::gridder_interop::{
        cpp_convolve_gridder_correction_row_2d, cpp_convolve_gridder_grid_unit_sample_2d,
    };
    use ndarray::Array2;
    use num_complex::Complex32;

    use super::{DensityCellConvention, StandardGridder};
    use crate::ImageGeometry;

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
            Some((center.0, center.1))
        );
        assert_eq!(
            gridder.density_cell_index(1.01 * du, -1.01 * dv),
            Some((center.0 + 1, center.1 + 1))
        );
        assert_eq!(
            gridder.density_cell_index(-1.01 * du, 1.01 * dv),
            Some((center.0 - 2, center.1 - 2))
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
}
