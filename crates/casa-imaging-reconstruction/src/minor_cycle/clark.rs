// SPDX-License-Identifier: LGPL-3.0-or-later

//! Clark's compact active pixels and linear residual refreshes.

use casa_fft::RealFft2;
use num_complex::Complex32;
use smallvec::SmallVec;
use std::time::Instant;

use super::{ClarkApproximation, MinorCycleError};
use crate::spectral_operator::SpectralOperatorError;

pub(super) struct ClarkActivePixel {
    index: usize,
    value: f64,
}

// The inline tracking limit bounds sparse bookkeeping; the work estimate below
// chooses the numerical strategy. Larger batches use the existing FFT buffer.
const SPARSE_COMPONENT_CAPACITY: usize = 64;

struct LinearRefresh<'psf> {
    shape: [usize; 2],
    padded: [usize; 2],
    psf: &'psf [f32],
    center: [usize; 2],
    threads: usize,
    sparse_indices: SmallVec<[usize; SPARSE_COMPONENT_CAPACITY]>,
    dense_batch: bool,
    sparse_refreshes: usize,
    fft_refreshes: usize,
    psf_spectrum: Vec<Complex32>,
    components: Vec<Complex32>,
    fft: RealFft2<f32>,
}

impl<'psf> LinearRefresh<'psf> {
    fn new(
        psf: &'psf [f32],
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
        psf: &'psf [f32],
        shape: [usize; 2],
        center: [usize; 2],
        padded: [usize; 2],
        threads: usize,
    ) -> Result<Self, MinorCycleError> {
        let mut fft = RealFft2::with_threads(padded, threads)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
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
            psf,
            center,
            threads,
            sparse_indices: SmallVec::new(),
            dense_batch: false,
            sparse_refreshes: 0,
            fft_refreshes: 0,
            psf_spectrum,
            components,
            fft,
        })
    }

    fn add(&mut self, index: usize, flux: f64) {
        if !self.dense_batch && !self.sparse_indices.contains(&index) {
            if self.sparse_indices.len() == SPARSE_COMPONENT_CAPACITY {
                self.dense_batch = true;
            } else {
                self.sparse_indices.push(index);
            }
        }
        let pixel = [index / self.shape[1], index % self.shape[1]];
        let real: &mut [f32] = bytemuck::cast_slice_mut(&mut self.components);
        real[pixel[0] * self.fft.real_row_stride() + pixel[1]] += flux as f32;
    }

    fn refresh(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        if !self.dense_batch {
            let row_stride = self.fft.real_row_stride();
            let real: &[f32] = bytemuck::cast_slice(&self.components);
            let components = self
                .sparse_indices
                .iter()
                .map(|&index| {
                    (
                        index,
                        real[index / self.shape[1] * row_stride + index % self.shape[1]],
                    )
                })
                .filter(|(_, flux)| *flux != 0.0)
                .collect::<SmallVec<[(usize, f32); SPARSE_COMPONENT_CAPACITY]>>();
            if components.is_empty() {
                self.sparse_indices.clear();
                return Ok(());
            }
            // Count clipped direct multiply-adds against the butterfly work of
            // two real transforms. This scales with shape and batch support,
            // not a workload-specific component-count crossover.
            let direct_work = components.iter().fold(0_u128, |work, &(index, _)| {
                let source = [index / self.shape[1], index % self.shape[1]];
                let overlap =
                    |axis: usize| self.shape[axis] - source[axis].abs_diff(self.center[axis]);
                work + overlap(0) as u128 * overlap(1) as u128
            });
            let fft_work = self.padded[0] as u128
                * self.padded[1] as u128
                * u128::from(self.padded[0].ilog2() + self.padded[1].ilog2());
            if direct_work <= fft_work {
                sparse_refresh(
                    self.psf,
                    self.shape,
                    self.center,
                    &components,
                    self.threads,
                    residual,
                )?;
                let real: &mut [f32] = bytemuck::cast_slice_mut(&mut self.components);
                for &index in &self.sparse_indices {
                    real[index / self.shape[1] * row_stride + index % self.shape[1]] = 0.0;
                }
                self.sparse_indices.clear();
                self.sparse_refreshes += 1;
                return Ok(());
            }
        }
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
        self.sparse_indices.clear();
        self.dense_batch = false;
        self.fft_refreshes += 1;
        Ok(())
    }
}

fn sparse_refresh(
    psf: &[f32],
    shape: [usize; 2],
    center: [usize; 2],
    components: &[(usize, f32)],
    threads: usize,
    residual: &mut [f64],
) -> Result<(), MinorCycleError> {
    let threads = threads.min(shape[0]).max(1);
    if threads == 1 {
        return sparse_refresh_rows(psf, shape, center, components, 0, residual);
    }
    let rows = shape[0].div_ceil(threads);
    std::thread::scope(|scope| {
        let (caller, remaining) = residual.split_at_mut(rows * shape[1]);
        let mut handles = Vec::with_capacity(threads - 1);
        for (ordinal, tile) in remaining.chunks_mut(rows * shape[1]).enumerate() {
            handles.push(
                std::thread::Builder::new()
                    .stack_size(super::CLARK_ROW_STACK_BYTES)
                    .spawn_scoped(scope, move || {
                        sparse_refresh_rows(
                            psf,
                            shape,
                            center,
                            components,
                            (ordinal + 1) * rows,
                            tile,
                        )
                    })
                    .map_err(|error| SpectralOperatorError::SpatialExecution(error.to_string()))?,
            );
        }
        let mut result = sparse_refresh_rows(psf, shape, center, components, 0, caller);
        for handle in handles {
            let completed = match handle.join() {
                Ok(completed) => completed,
                Err(_) => Err(SpectralOperatorError::SpatialExecution(
                    "Clark row worker panicked".into(),
                )
                .into()),
            };
            result = result.and(completed);
        }
        result
    })
}

fn sparse_refresh_rows(
    psf: &[f32],
    shape: [usize; 2],
    center: [usize; 2],
    components: &[(usize, f32)],
    first_row: usize,
    residual: &mut [f64],
) -> Result<(), MinorCycleError> {
    let width = shape[1];
    for (local_x, row) in residual.chunks_exact_mut(width).enumerate() {
        let x = first_row + local_x;
        for &(index, flux) in components {
            let source = [index / width, index % width];
            let psf_x = center[0] as isize + x as isize - source[0] as isize;
            if !(0..shape[0] as isize).contains(&psf_x) {
                continue;
            }
            let begin = source[1].saturating_sub(center[1]);
            let end = (source[1] + width - center[1]).min(width);
            let kernel_begin = psf_x as usize * width + center[1] + begin - source[1];
            let kernel = &psf[kernel_begin..kernel_begin + end - begin];
            for (value, &coefficient) in row[begin..end].iter_mut().zip(kernel) {
                *value -= f64::from(flux) * f64::from(coefficient);
            }
        }
        if row.iter().any(|value| !value.is_finite()) {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
    }
    Ok(())
}

pub(super) struct ClarkWorkState<'psf> {
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
    convolution: LinearRefresh<'psf>,
    measurements: Option<ClarkMeasurements>,
}

#[derive(Default)]
struct ClarkMeasurements {
    setup_nanos: u128,
    build_nanos: u128,
    peak_nanos: u128,
    update_nanos: u128,
    refresh_nanos: u128,
    peak_visits: u64,
    update_visits: u64,
    maximum_active: usize,
}

impl<'psf> ClarkWorkState<'psf> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        residual: &[f64],
        psf: &'psf [f32],
        shape: [usize; 2],
        psf_peak: [usize; 2],
        normalization: f64,
        approximation: ClarkApproximation,
        threshold: f64,
        fft_threads: usize,
        accept: impl Fn([usize; 2]) -> bool,
    ) -> Result<Self, MinorCycleError> {
        let started = std::env::var_os("CASA_RS_TRACE_CLARK_TIMING")
            .is_some()
            .then(Instant::now);
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
            measurements: started.map(|started| ClarkMeasurements {
                setup_nanos: started.elapsed().as_nanos(),
                ..ClarkMeasurements::default()
            }),
        };
        state.begin(residual, accept);
        Ok(state)
    }

    fn begin(&mut self, residual: &[f64], accept: impl Fn([usize; 2]) -> bool) {
        let started = self.measurements.as_ref().map(|_| Instant::now());
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
        if let (Some(measurements), Some(started)) = (&mut self.measurements, started) {
            measurements.build_nanos += started.elapsed().as_nanos();
            measurements.maximum_active = measurements.maximum_active.max(self.active.len());
        }
    }

    pub(super) fn candidate(
        &mut self,
        residual: &mut [f64],
        accept: impl Fn([usize; 2]) -> bool,
    ) -> Result<Option<(usize, f64)>, MinorCycleError> {
        loop {
            let started = self.measurements.as_ref().map(|_| Instant::now());
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
            if let (Some(measurements), Some(started)) = (&mut self.measurements, started) {
                measurements.peak_nanos += started.elapsed().as_nanos();
                measurements.peak_visits += self.active.len() as u64;
            }
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
        let started = self.measurements.as_ref().map(|_| Instant::now());
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
        if let (Some(measurements), Some(started)) = (&mut self.measurements, started) {
            measurements.update_nanos += started.elapsed().as_nanos();
            measurements.update_visits += self.active.len() as u64;
        }
        Ok(())
    }

    fn refresh_pending(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        let started = self.measurements.as_ref().map(|_| Instant::now());
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
        if let (Some(measurements), Some(started)) = (&mut self.measurements, started) {
            measurements.refresh_nanos += started.elapsed().as_nanos();
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        if self.subcycle_iterations > 0 {
            self.refresh_pending(residual)?;
        }
        if let Some(measurements) = &self.measurements {
            eprintln!(
                "imaging_clark_cost setup_nanos={} build_nanos={} peak_nanos={} update_nanos={} refresh_nanos={} peak_visits={} update_visits={} maximum_active={} refreshes={} sparse_refreshes={} fft_refreshes={}",
                measurements.setup_nanos,
                measurements.build_nanos,
                measurements.peak_nanos,
                measurements.update_nanos,
                measurements.refresh_nanos,
                measurements.peak_visits,
                measurements.update_visits,
                measurements.maximum_active,
                self.refreshes,
                self.convolution.sparse_refreshes,
                self.convolution.fft_refreshes,
            );
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
    #[ignore = "actual persisted PSF/model sparse-convolution crossover; guarded diagnostic"]
    fn compare_actual_sparse_clark_refresh() {
        use casa_images::PagedImage;
        let image = |name| {
            let path = std::env::var_os(name).expect("durable actual application product");
            let image = PagedImage::<f32>::open(std::path::PathBuf::from(path)).unwrap();
            let shape = image.shape().to_vec();
            assert_eq!(shape.len(), 4);
            assert_eq!(&shape[2..], &[1, 1]);
            let pixels = image
                .get_slice(&[0; 4], &shape)
                .unwrap()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            ([shape[0], shape[1]], pixels)
        };
        let (shape, psf) = image("CASA_RS_CLARK_PROBE_PSF");
        let (model_shape, model) = image("CASA_RS_CLARK_PROBE_MODEL");
        assert_eq!(shape, model_shape);
        let origin = psf
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
            .unwrap()
            .0;
        let center = [origin / shape[1], origin % shape[1]];
        let mut components = model
            .into_iter()
            .enumerate()
            .filter(|(_, flux)| *flux != 0.0)
            .collect::<Vec<_>>();
        assert!(components.iter().all(|(_, flux)| flux.is_finite()));
        components.sort_unstable_by(|(_, a), (_, b)| b.abs().total_cmp(&a.abs()));
        assert!(components.len() >= 64);
        let mut fft = LinearRefresh::new(&psf, shape, center, 4).unwrap();
        println!(
            "clark_sparse_probe shape={shape:?} padded={:?} actual_components={}",
            fft.padded,
            components.len()
        );
        for count in [1, 4, 8, 16, 64] {
            let selected = &components[..count];
            let mut expected = vec![0.0; psf.len()];
            for &(index, flux) in selected {
                fft.add(index, f64::from(flux));
            }
            fft.dense_batch = true;
            let start = Instant::now();
            fft.refresh(&mut expected).unwrap();
            let fft_seconds = start.elapsed().as_secs_f64();
            for workers in [1, 4] {
                let psf = psf.as_slice();
                let mut actual = vec![0.0; psf.len()];
                let rows = shape[0].div_ceil(workers);
                let start = Instant::now();
                std::thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for (ordinal, tile) in actual.chunks_mut(rows * shape[1]).enumerate() {
                        handles.push(scope.spawn(move || {
                            sparse_refresh_rows(psf, shape, center, selected, ordinal * rows, tile)
                        }));
                    }
                    for handle in handles {
                        handle.join().unwrap().unwrap();
                    }
                });
                let direct_seconds = start.elapsed().as_secs_f64();
                let (mut error, mut norm, mut maximum, mut peak) =
                    (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
                for (&actual, &expected) in actual.iter().zip(&expected) {
                    error += (actual - expected).powi(2);
                    norm += expected.powi(2);
                    maximum = maximum.max((actual - expected).abs());
                    peak = peak.max(expected.abs());
                }
                let relative_l2 = (error / norm).sqrt();
                let peak_normalized_error = maximum / peak;
                assert!(relative_l2 <= 1e-3 && peak_normalized_error <= 1e-3);
                println!(
                    "clark_sparse_probe components={count} workers={workers} fft_seconds={fft_seconds:.9} direct_seconds={direct_seconds:.9} relative_l2={relative_l2:.9e} peak_normalized_error={peak_normalized_error:.9e}"
                );
            }
        }
    }

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
            assert_eq!(refresh.sparse_refreshes, 1);
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
                            refresh.dense_batch = true;
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
    fn sparse_batches_share_f32_accumulation_and_reset_after_dense_overflow() {
        let shape = [16, 16];
        let center = [8, 8];
        let psf = (0..shape[0] * shape[1])
            .map(|index| (index as f32 * 0.37).sin())
            .collect::<Vec<_>>();
        for threads in [1, 4, 8] {
            let mut actual = LinearRefresh::new(&psf, shape, center, threads).unwrap();
            let mut reference = LinearRefresh::new(&psf, shape, center, threads).unwrap();
            let batches = [
                vec![(0, 0.1), (0, 0.2), (17, 0.625), (17, -0.625), (255, -0.375)],
                (0..SPARSE_COMPONENT_CAPACITY + 1)
                    .map(|index| (index, (index as f64 * 0.7).cos()))
                    .collect(),
                vec![(17, 0.5), (17, -0.5)],
                vec![(7, 0.25)],
            ];
            let mut residual = vec![0.5; psf.len()];
            let mut expected = residual.clone();
            for (ordinal, batch) in batches.into_iter().enumerate() {
                for (index, flux) in batch {
                    actual.add(index, flux);
                    reference.add(index, flux);
                }
                assert_eq!(actual.dense_batch, ordinal == 1);
                assert!(!actual.sparse_indices.spilled());
                reference.dense_batch = true;
                actual.refresh(&mut residual).unwrap();
                reference.refresh(&mut expected).unwrap();
                for (&value, &reference) in residual.iter().zip(&expected) {
                    assert!((value - reference).abs() < 1e-5);
                }
                assert!(actual.sparse_indices.is_empty());
                assert!(!actual.dense_batch);
                let completed = residual.clone();
                actual.refresh(&mut residual).unwrap();
                assert_eq!(residual, completed);
            }
            assert_eq!(actual.sparse_refreshes, 2);
            assert_eq!(actual.fft_refreshes, 1);
        }
    }

    #[test]
    fn sparse_tracking_uses_fft_when_clipped_work_exceeds_transform_work() {
        let shape = [32, 32];
        let psf = vec![1.0; shape[0] * shape[1]];
        let mut refresh = LinearRefresh::new(&psf, shape, [16, 16], 1).unwrap();
        for index in 0..40 {
            refresh.add((14 + index / 8) * shape[1] + 12 + index % 8, 0.25);
        }
        assert!(!refresh.dense_batch);
        refresh.refresh(&mut vec![0.0; psf.len()]).unwrap();
        assert_eq!(refresh.sparse_refreshes, 0);
        assert_eq!(refresh.fft_refreshes, 1);
    }

    #[test]
    fn sparse_refresh_propagates_nonfinite_worker_output() {
        let shape = [16, 16];
        let center = [8, 8];
        let psf = vec![1.0; shape[0] * shape[1]];
        for threads in [1, 4, 8] {
            let mut refresh = LinearRefresh::new(&psf, shape, center, threads).unwrap();
            refresh.add(8 * shape[1] + 8, f64::NAN);
            assert_eq!(
                refresh.refresh(&mut vec![0.0; psf.len()]),
                Err(MinorCycleError::GeneratedNonfinite)
            );
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
            refresh.dense_batch = true;
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
