// SPDX-License-Identifier: LGPL-3.0-or-later

//! Clark's compact active pixels and linear residual refreshes.

use casa_fft::RealFft2;
use num_complex::Complex32;

use super::{ClarkApproximation, MinorCycleError};
use crate::spectral_operator::SpectralOperatorError;

pub(super) struct ClarkActivePixel {
    index: usize,
    value: f64,
}

struct LinearRefresh {
    shape: [usize; 2],
    padded: [usize; 2],
    psf_spectrum: Vec<Complex32>,
    components: Vec<Complex32>,
    fft: RealFft2<f32>,
}

impl LinearRefresh {
    fn new(
        psf: &[f32],
        shape: [usize; 2],
        center: [usize; 2],
        threads: usize,
    ) -> Result<Self, MinorCycleError> {
        let padded_axis = |axis: usize| {
            let extent = shape[axis];
            let origin = center[axis];
            if origin >= extent {
                return Err(MinorCycleError::ModelShapeMismatch);
            }
            // For the requested output interval [0, extent), neither the
            // negative PSF tail nor the positive convolution tail may wrap
            // into it. This is the smallest alias-free circular extent for
            // the PSF origin, including off-centre peaks.
            let positive_tail = extent
                .checked_mul(2)
                .and_then(|twice| twice.checked_sub(1 + origin))
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            let negative_tail = extent
                .checked_add(origin)
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            Ok::<_, MinorCycleError>(positive_tail.max(negative_tail))
        };
        let padded = [padded_axis(0)?, padded_axis(1)?];
        Self::with_padded(psf, shape, center, padded, threads)
    }

    fn with_padded(
        psf: &[f32],
        shape: [usize; 2],
        center: [usize; 2],
        padded: [usize; 2],
        threads: usize,
    ) -> Result<Self, MinorCycleError> {
        let mut fft = RealFft2::with_threads(padded, threads)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        if threads > 1 {
            fft = fft.with_estimated_plan();
        }
        let mut psf_spectrum = vec![Complex32::default(); fft.storage_len()];
        let real_psf: &mut [f32] = bytemuck::cast_slice_mut(&mut psf_spectrum);
        let row_stride = fft.real_row_stride();
        for x in 0..shape[0] {
            for y in 0..shape[1] {
                let offset = [
                    (x + padded[0] - center[0]) % padded[0],
                    (y + padded[1] - center[1]) % padded[1],
                ];
                real_psf[offset[0] * row_stride + offset[1]] = psf[x * shape[1] + y];
            }
        }
        fft.forward(&mut psf_spectrum)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        let components = vec![Complex32::default(); fft.storage_len()];
        Ok(Self {
            shape,
            padded,
            psf_spectrum,
            components,
            fft,
        })
    }

    fn add(&mut self, index: usize, flux: f64) {
        let pixel = [index / self.shape[1], index % self.shape[1]];
        let real: &mut [f32] = bytemuck::cast_slice_mut(&mut self.components);
        real[pixel[0] * self.fft.real_row_stride() + pixel[1]] += flux as f32;
    }

    fn refresh(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        self.fft
            .forward(&mut self.components)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        for (value, kernel) in self.components.iter_mut().zip(self.psf_spectrum.iter()) {
            *value *= kernel;
        }
        self.fft
            .inverse(&mut self.components)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        let normalization = (self.padded[0] * self.padded[1]) as f64;
        let real: &[f32] = bytemuck::cast_slice(&self.components);
        let row_stride = self.fft.real_row_stride();
        for x in 0..self.shape[0] {
            for y in 0..self.shape[1] {
                let index = x * self.shape[1] + y;
                residual[index] -= f64::from(real[x * row_stride + y]) / normalization;
                if !residual[index].is_finite() {
                    return Err(MinorCycleError::GeneratedNonfinite);
                }
            }
        }
        self.components.fill(Complex32::default());
        Ok(())
    }
}

pub(super) struct ClarkWorkState {
    shape: [usize; 2],
    psf_peak: [usize; 2],
    approximation: ClarkApproximation,
    normalization: f64,
    threshold: f64,
    factor: f64,
    max_residual: f64,
    flux_limit: f64,
    iteration_flux_limit: f64,
    fac: f64,
    fmn: f64,
    subcycle_iterations: usize,
    maximum_subcycle_iterations: usize,
    subcycles: usize,
    refreshes: usize,
    active: Vec<ClarkActivePixel>,
    convolution: LinearRefresh,
}

impl ClarkWorkState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        residual: &[f64],
        psf: &[f32],
        shape: [usize; 2],
        psf_peak: [usize; 2],
        normalization: f64,
        approximation: ClarkApproximation,
        threshold: f64,
        fft_threads: usize,
        accept: impl Fn([usize; 2]) -> bool,
    ) -> Result<Self, MinorCycleError> {
        let mut max_residual = 0.0_f64;
        for (x, row) in residual.chunks_exact(shape[1]).enumerate() {
            for (y, value) in row.iter().enumerate() {
                let magnitude = value.abs();
                if magnitude > max_residual && accept([x, y]) {
                    max_residual = magnitude;
                }
            }
        }
        let exterior = approximation.maximum_exterior_sidelobe / normalization;
        let maximum_subcycle_iterations = if exterior > 0.5 {
            5
        } else if exterior > 0.35 {
            50
        } else {
            usize::MAX
        };
        let convolution = LinearRefresh::new(psf, shape, psf_peak, fft_threads)?;
        let mut state = Self {
            shape,
            psf_peak,
            approximation,
            normalization,
            threshold: threshold * normalization,
            factor: 1.0 / 3.0,
            max_residual,
            flux_limit: 0.0,
            iteration_flux_limit: 0.0,
            fac: 0.0,
            fmn: 0.0,
            subcycle_iterations: 0,
            maximum_subcycle_iterations,
            subcycles: 0,
            refreshes: 0,
            active: Vec::new(),
            convolution,
        };
        state.begin(residual, accept);
        Ok(state)
    }

    fn begin(&mut self, residual: &[f64], accept: impl Fn([usize; 2]) -> bool) {
        self.flux_limit = self.max_residual * self.approximation.maximum_exterior_sidelobe
            / self.normalization
            * self.factor;
        if self.factor > 1.0 {
            self.flux_limit = self.flux_limit.min(0.95 * self.max_residual);
        }
        let cutoff = self.flux_limit.max(self.threshold);
        self.active.clear();
        for (x, row) in residual.chunks_exact(self.shape[1]).enumerate() {
            for (y, &value) in row.iter().enumerate() {
                let pixel = [x, y];
                let index = x * self.shape[1] + y;
                if value.abs() > cutoff && accept(pixel) {
                    self.active.push(ClarkActivePixel { index, value });
                }
            }
        }
        // Sort only compact active pixels to retain casacore's x-fastest tie order.
        self.active.sort_unstable_by_key(|pixel| {
            (pixel.index % self.shape[1], pixel.index / self.shape[1])
        });
        let peak = self
            .active
            .iter()
            .fold(0.0_f64, |best, pixel| best.max(pixel.value.abs()));
        self.fac = if self.flux_limit > 0.0 {
            peak / self.flux_limit
        } else {
            0.0
        };
        self.fmn = 0.0;
        self.iteration_flux_limit = cutoff;
        self.subcycle_iterations = 0;
    }

    pub(super) fn candidate(
        &mut self,
        residual: &mut [f64],
        accept: impl Fn([usize; 2]) -> bool,
    ) -> Result<Option<(usize, f64)>, MinorCycleError> {
        loop {
            let peak = self
                .active
                .iter()
                .fold(None::<&ClarkActivePixel>, |best, pixel| {
                    if best.is_none_or(|current| pixel.value.abs() > current.value.abs()) {
                        Some(pixel)
                    } else {
                        best
                    }
                });
            if self.subcycle_iterations < self.maximum_subcycle_iterations
                && let Some(pixel) = peak
                && pixel.value.abs() > self.iteration_flux_limit
            {
                return Ok(Some((pixel.index, pixel.value)));
            }
            if self.subcycle_iterations == 0 {
                return Ok(None);
            }
            self.refresh_pending(residual)?;
            if self.max_residual <= self.threshold || self.subcycles >= 10 {
                return Ok(None);
            }
            self.begin(residual, &accept);
        }
    }

    pub(super) fn accept(
        &mut self,
        index: usize,
        flux: f64,
        global_iterations: usize,
        psf_real_at: impl Fn(usize) -> f64,
    ) -> Result<(), MinorCycleError> {
        self.convolution.add(index, flux);
        let peak = [index / self.shape[1], index % self.shape[1]];
        for pixel in &mut self.active {
            let target = [pixel.index / self.shape[1], pixel.index % self.shape[1]];
            let relative = [
                target[0] as isize - peak[0] as isize,
                target[1] as isize - peak[1] as isize,
            ];
            if relative[0] < -(self.approximation.radius[0] as isize)
                || relative[0]
                    >= (self.approximation.patch_size[0] - self.approximation.radius[0]) as isize
                || relative[1] < -(self.approximation.radius[1] as isize)
                || relative[1]
                    >= (self.approximation.patch_size[1] - self.approximation.radius[1]) as isize
            {
                continue;
            }
            let source = [
                self.psf_peak[0] as isize + relative[0],
                self.psf_peak[1] as isize + relative[1],
            ];
            if source[0] < 0
                || source[1] < 0
                || source[0] >= self.shape[0] as isize
                || source[1] >= self.shape[1] as isize
            {
                continue;
            }
            pixel.value -=
                flux * psf_real_at(source[0] as usize * self.shape[1] + source[1] as usize);
            if !pixel.value.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
        }
        self.subcycle_iterations += 1;
        self.fmn += self.fac / global_iterations as f64;
        self.iteration_flux_limit = (self.flux_limit * self.fmn).max(self.threshold);
        Ok(())
    }

    fn refresh_pending(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        let previous = self.max_residual;
        self.max_residual = self
            .active
            .iter()
            .fold(0.0_f64, |peak, pixel| peak.max(pixel.value.abs()));
        self.convolution.refresh(residual)?;
        self.refreshes += 1;
        self.subcycles += 1;
        self.subcycle_iterations = 0;
        if self.max_residual > previous {
            self.factor *= 3.0;
            self.maximum_subcycle_iterations = 10;
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        if self.subcycle_iterations > 0 {
            self.refresh_pending(residual)?;
        }
        Ok(())
    }

    pub(super) fn refreshes(&self) -> usize {
        self.refreshes
    }

    pub(super) fn stopped_at_subcycle_bound(&self) -> bool {
        self.subcycles >= 10 && self.max_residual > self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn contiguous_active_scan_preserves_x_fastest_ties_and_support() {
        let shape = [4, 6];
        let center = [2, 3];
        let mut psf = vec![0.0; shape[0] * shape[1]];
        psf[center[0] * shape[1] + center[1]] = 1.0;
        let mut residual = vec![0.0; psf.len()];
        residual[3 * shape[1]] = 1.0;
        residual[3] = -1.0;
        residual[2 * shape[1] + 1] = 2.0;
        let accept = |pixel| pixel != [2, 1];
        let mut state = ClarkWorkState::new(
            &residual,
            &psf,
            shape,
            center,
            1.0,
            ClarkApproximation {
                radius: [0, 0],
                patch_size: [1, 1],
                maximum_exterior_sidelobe: 0.0,
            },
            0.1,
            1,
            accept,
        )
        .unwrap();
        assert_eq!(state.max_residual, 1.0);
        assert_eq!(
            state
                .active
                .iter()
                .map(|pixel| pixel.index)
                .collect::<Vec<_>>(),
            [18, 3]
        );
        assert_eq!(
            state.candidate(&mut residual, accept).unwrap(),
            Some((18, 1.0))
        );
    }

    #[test]
    fn compact_refresh_matches_linear_asymmetric_psf_at_edges_and_off_center() {
        for (shape, center, threads) in [
            ([4, 6], [2, 3]),
            ([5, 7], [2, 3]),
            ([6, 5], [1, 3]),
            ([5, 4], [0, 1]),
        ]
        .into_iter()
        .flat_map(|(shape, center)| [1, 4].map(|threads| (shape, center, threads)))
        {
            let psf = (0..shape[0] * shape[1])
                .map(|index| {
                    let x = index / shape[1];
                    let y = index % shape[1];
                    (x as f64 * 0.17 + y as f64 * 0.11).sin() as f32
                })
                .collect::<Vec<_>>();
            let mut refresh = LinearRefresh::new(&psf, shape, center, threads).unwrap();
            let half_cells = refresh.padded[0] * (refresh.padded[1] / 2 + 1);
            assert_eq!(refresh.psf_spectrum.len(), half_cells);
            assert_eq!(refresh.components.len(), half_cells);
            for axis in 0..2 {
                assert_eq!(
                    refresh.padded[axis],
                    (2 * shape[axis] - 1 - center[axis]).max(shape[axis] + center[axis])
                );
            }
            let components = [
                (0, 0, 0.75),
                (shape[0] - 1, shape[1] - 1, -0.375),
                (shape[0] / 2, 1, 0.125),
            ];
            let mut expected = vec![0.5; psf.len()];
            for &(source_x, source_y, flux) in &components {
                refresh.add(source_x * shape[1] + source_y, flux);
                for x in 0..shape[0] {
                    for y in 0..shape[1] {
                        let psf_x = center[0] as isize + x as isize - source_x as isize;
                        let psf_y = center[1] as isize + y as isize - source_y as isize;
                        if psf_x >= 0
                            && psf_x < shape[0] as isize
                            && psf_y >= 0
                            && psf_y < shape[1] as isize
                        {
                            expected[x * shape[1] + y] -=
                                flux * f64::from(psf[psf_x as usize * shape[1] + psf_y as usize]);
                        }
                    }
                }
            }
            let mut actual = vec![0.5; psf.len()];
            refresh.refresh(&mut actual).unwrap();
            for (&actual, &expected) in actual.iter().zip(&expected) {
                assert!(
                    (actual - expected).abs() < 1e-5,
                    "shape={shape:?}, center={center:?}, actual={actual}, expected={expected}"
                );
            }
            let completed = actual.clone();
            refresh.refresh(&mut actual).unwrap();
            assert_eq!(
                actual, completed,
                "completed component batch must be cleared"
            );
        }
    }

    #[test]
    fn cropped_linear_refresh_has_no_edge_alias_for_every_peak_origin() {
        for shape in [[3, 4], [4, 3], [5, 7], [6, 6]] {
            let psf = (0..shape[0] * shape[1])
                .map(|index| (index as f64 * 0.37).cos() as f32)
                .collect::<Vec<_>>();
            for center_x in 0..shape[0] {
                for center_y in 0..shape[1] {
                    let center = [center_x, center_y];
                    for source_x in [0, shape[0] - 1] {
                        for source_y in [0, shape[1] - 1] {
                            let mut refresh = LinearRefresh::new(&psf, shape, center, 1).unwrap();
                            refresh.add(source_x * shape[1] + source_y, 0.625);
                            let mut actual = vec![0.0; psf.len()];
                            refresh.refresh(&mut actual).unwrap();
                            for x in 0..shape[0] {
                                for y in 0..shape[1] {
                                    let kernel_x =
                                        center_x as isize + x as isize - source_x as isize;
                                    let kernel_y =
                                        center_y as isize + y as isize - source_y as isize;
                                    let expected = if (0..shape[0] as isize).contains(&kernel_x)
                                        && (0..shape[1] as isize).contains(&kernel_y)
                                    {
                                        -0.625
                                            * f64::from(
                                                psf[kernel_x as usize * shape[1]
                                                    + kernel_y as usize],
                                            )
                                    } else {
                                        0.0
                                    };
                                    assert!(
                                        (actual[x * shape[1] + y] - expected).abs() < 1e-5,
                                        "shape={shape:?}, center={center:?}, source=({source_x},{source_y}), output=({x},{y})"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    #[ignore = "bounded manual cost comparison; run with --release --ignored --nocapture"]
    fn compare_cropped_and_full_clark_transform_cost() {
        let shape = [512, 512];
        let center = [256, 256];
        let psf = (0..shape[0] * shape[1])
            .map(|index| {
                let x = index / shape[1];
                let y = index % shape[1];
                (-(x as f64 - 256.0).hypot(y as f64 - 256.0) / 19.0).exp() as f32
            })
            .collect::<Vec<_>>();
        let run = |padded| {
            let start = Instant::now();
            let mut refresh = LinearRefresh::with_padded(&psf, shape, center, padded, 1).unwrap();
            let setup = start.elapsed();
            for index in [0, shape[1] - 1, psf.len() / 2, psf.len() - 1] {
                refresh.add(index, 0.25);
            }
            let mut residual = vec![1.0; psf.len()];
            let start = Instant::now();
            refresh.refresh(&mut residual).unwrap();
            let execution = start.elapsed();
            (setup, execution, residual)
        };
        let (full_setup, full_execution, full) = run([1024, 1024]);
        let (crop_setup, crop_execution, crop) = run([768, 768]);
        let max_error = full
            .iter()
            .zip(crop)
            .map(|(left, right)| (left - right).abs())
            .fold(0.0_f64, f64::max);
        println!(
            "full setup={full_setup:?} refresh={full_execution:?}; crop setup={crop_setup:?} refresh={crop_execution:?}; max_error={max_error}"
        );
        assert!(max_error < 1e-5);
    }
}
