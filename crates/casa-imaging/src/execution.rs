// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal imaging execution plans and CPU workspaces.

use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{
    ImageGeometry, ImagingError, VisibilityBatch,
    gridder::{PositiveTapSet, StandardGridder, StandardMfsTapCensus, StandardMfsTapSkipReason},
    profile,
};

/// Internal backend selection for standard MFS execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StandardMfsBackend {
    /// Synchronous CPU execution using the native Rust gridder.
    Cpu,
    /// Reserved marker for future backends that must fail before execution.
    #[allow(dead_code)]
    Reserved(&'static str),
}

/// CPU-only executor for standard MFS imaging.
pub(crate) struct StandardMfsCpuExecutor<'a> {
    gridder: &'a StandardGridder,
    plan: StandardMfsVisibilityPlan,
    workspace: StandardMfsWorkspace,
}

/// CPU executor for streaming standard MFS dirty accumulation.
///
/// This owns the standard gridder and reusable grids while each call to
/// `accumulate_batches` builds only a borrowed plan for the current frontend
/// row block.
pub(crate) struct StandardMfsDirtyCpuExecutor {
    gridder: StandardGridder,
    workspace: StandardMfsWorkspace,
    normalization_sumwt: f64,
    reported_sumwt: f64,
    gridded_samples: usize,
    skipped_samples: usize,
    max_abs_w_lambda: f64,
}

/// Immutable summary of accumulated streaming standard MFS dirty samples.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StandardMfsDirtyAccumulation {
    /// PSF and residual normalization sum from accepted samples.
    pub(crate) normalization_sumwt: f64,
    /// CASA-style reported sumwt from accepted samples.
    pub(crate) reported_sumwt: f64,
    /// Number of samples accepted by the standard gridder.
    pub(crate) gridded_samples: usize,
    /// Number of samples rejected by flags, weights, or gridder bounds.
    pub(crate) skipped_samples: usize,
    /// Maximum absolute `w` coordinate seen in wavelengths.
    pub(crate) max_abs_w_lambda: f64,
}

impl<'a> StandardMfsCpuExecutor<'a> {
    /// Build a CPU executor over weighted standard-gridder visibility batches.
    pub(crate) fn new(
        gridder: &'a StandardGridder,
        batches: &'a [VisibilityBatch],
    ) -> Result<Self, ImagingError> {
        Self::for_backend(StandardMfsBackend::Cpu, gridder, batches)
    }

    /// Build an executor for a requested internal backend.
    pub(crate) fn for_backend(
        backend: StandardMfsBackend,
        gridder: &'a StandardGridder,
        batches: &'a [VisibilityBatch],
    ) -> Result<Self, ImagingError> {
        match backend {
            StandardMfsBackend::Cpu => Ok(Self {
                gridder,
                plan: StandardMfsVisibilityPlan::new(gridder, batches),
                workspace: StandardMfsWorkspace::new(gridder),
            }),
            StandardMfsBackend::Reserved(name) => Err(ImagingError::Unsupported(format!(
                "standard MFS backend '{name}' is not implemented"
            ))),
        }
    }

    /// Borrow the executor's gridder, visibility plan, and mutable workspace.
    pub(crate) fn parts_mut(
        &mut self,
    ) -> (
        &'a StandardGridder,
        &StandardMfsVisibilityPlan,
        &mut StandardMfsWorkspace,
    ) {
        (self.gridder, &self.plan, &mut self.workspace)
    }

    /// Return the prepared visibility plan.
    #[cfg(test)]
    pub(crate) fn plan(&self) -> &StandardMfsVisibilityPlan {
        &self.plan
    }
}

impl StandardMfsDirtyCpuExecutor {
    /// Build a dirty accumulator executor for the requested internal backend.
    pub(crate) fn for_backend(
        backend: StandardMfsBackend,
        geometry: ImageGeometry,
    ) -> Result<Self, ImagingError> {
        match backend {
            StandardMfsBackend::Cpu => {
                let gridder = StandardGridder::new(geometry)?;
                let workspace = StandardMfsWorkspace::new(&gridder);
                Ok(Self {
                    gridder,
                    workspace,
                    normalization_sumwt: 0.0,
                    reported_sumwt: 0.0,
                    gridded_samples: 0,
                    skipped_samples: 0,
                    max_abs_w_lambda: 0.0,
                })
            }
            StandardMfsBackend::Reserved(name) => Err(ImagingError::Unsupported(format!(
                "standard MFS backend '{name}' is not implemented"
            ))),
        }
    }

    /// Build the current CPU dirty executor.
    pub(crate) fn new(geometry: ImageGeometry) -> Result<Self, ImagingError> {
        Self::for_backend(StandardMfsBackend::Cpu, geometry)
    }

    /// Accumulate a borrowed row-block plan into the reusable dirty grids.
    pub(crate) fn accumulate_batches(
        &mut self,
        batches: &[VisibilityBatch],
    ) -> Result<(), ImagingError> {
        for batch in batches {
            batch.validate()?;
            self.max_abs_w_lambda = batch
                .w_lambda
                .iter()
                .fold(self.max_abs_w_lambda, |max_abs_w_lambda, &w_lambda| {
                    max_abs_w_lambda.max(w_lambda.abs())
                });
        }

        let gridder = &self.gridder;
        let (psf_grid, residual_grid) = self.workspace.dirty_grids_mut();
        let mut census = StandardMfsTapCensus::new("streaming_dirty_accumulate");
        for batch in batches {
            for sample_index in 0..batch.len() {
                if !batch.gridable[sample_index] {
                    self.skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::NotGridable);
                    }
                    continue;
                }
                let weight = batch.weight[sample_index];
                let sumwt_factor = batch.sumwt_factor[sample_index];
                if !(weight.is_finite()
                    && weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0)
                {
                    self.skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        if !(weight.is_finite() && weight > 0.0) {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidWeight);
                        } else {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidSumwt);
                        }
                    }
                    continue;
                }
                let Some(plan) = gridder
                    .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                else {
                    self.skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::OutOfGrid);
                    }
                    continue;
                };
                if let Some(census) = census.as_mut() {
                    census.observe_accepted(&plan);
                }
                let grid_weight = weight * sumwt_factor;
                let sumwt = f64::from(grid_weight);
                self.normalization_sumwt += sumwt;
                self.reported_sumwt += sumwt;
                self.gridded_samples += 1;

                let observed_visibility = batch.visibility[sample_index];
                if finite_visibility(observed_visibility) {
                    let residual = Complex64::new(
                        f64::from(observed_visibility.re) * sumwt,
                        f64::from(observed_visibility.im) * sumwt,
                    );
                    gridder.grid_sample_taps_real_complex_pair_planned_f64(
                        psf_grid,
                        sumwt,
                        residual_grid,
                        residual,
                        &plan,
                    );
                } else {
                    gridder.grid_sample_taps_real_planned_f64(psf_grid, &plan, sumwt);
                }
            }
        }
        if let Some(census) = census {
            census.log(std::mem::size_of::<StandardMfsPlannedSample>());
        }
        Ok(())
    }

    /// Return the reusable standard gridder.
    pub(crate) fn gridder(&self) -> &StandardGridder {
        &self.gridder
    }

    /// Return the accumulated dirty PSF and residual grids.
    pub(crate) fn dirty_grids(&self) -> (&Array2<Complex64>, &Array2<Complex64>) {
        self.workspace.dirty_grids()
    }

    /// Return the accumulated sample summary.
    pub(crate) fn accumulation(&self) -> StandardMfsDirtyAccumulation {
        StandardMfsDirtyAccumulation {
            normalization_sumwt: self.normalization_sumwt,
            reported_sumwt: self.reported_sumwt,
            gridded_samples: self.gridded_samples,
            skipped_samples: self.skipped_samples,
            max_abs_w_lambda: self.max_abs_w_lambda,
        }
    }
}

/// Compact planned view of weighted standard-gridder visibility samples.
pub(crate) struct StandardMfsVisibilityPlan {
    samples: Vec<StandardMfsPlannedSample>,
    normalization_sumwt: f64,
    reported_sumwt: f64,
    skipped_samples: usize,
}

impl StandardMfsVisibilityPlan {
    fn new(gridder: &StandardGridder, batches: &[VisibilityBatch]) -> Self {
        let plan_started = profile::maybe_profile_now();
        let sample_count = batches.iter().map(VisibilityBatch::len).sum();
        let mut samples = Vec::with_capacity(sample_count);
        let mut normalization_sumwt = 0.0f64;
        let mut reported_sumwt = 0.0f64;
        let mut skipped_samples = 0usize;
        let mut census = StandardMfsTapCensus::new("visibility_plan");

        for batch in batches {
            for sample_index in 0..batch.len() {
                if !batch.gridable[sample_index] {
                    skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::NotGridable);
                    }
                    continue;
                }
                let weight = batch.weight[sample_index];
                let sumwt_factor = batch.sumwt_factor[sample_index];
                if !(weight.is_finite()
                    && weight > 0.0
                    && sumwt_factor.is_finite()
                    && sumwt_factor > 0.0)
                {
                    skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        if !(weight.is_finite() && weight > 0.0) {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidWeight);
                        } else {
                            census.observe_skip(StandardMfsTapSkipReason::InvalidSumwt);
                        }
                    }
                    continue;
                }
                let Some(positive_taps) = gridder
                    .plan_positive_taps(batch.u_lambda[sample_index], batch.v_lambda[sample_index])
                else {
                    skipped_samples += 1;
                    if let Some(census) = census.as_mut() {
                        census.observe_skip(StandardMfsTapSkipReason::OutOfGrid);
                    }
                    continue;
                };
                if let Some(census) = census.as_mut() {
                    census.observe_accepted(&positive_taps);
                }
                let grid_weight = weight * sumwt_factor;
                let sumwt = f64::from(grid_weight);
                normalization_sumwt += sumwt;
                reported_sumwt += sumwt;
                samples.push(StandardMfsPlannedSample {
                    visibility: batch.visibility[sample_index],
                    grid_weight,
                    positive_taps,
                });
            }
        }
        if let Some(census) = census {
            census.log(std::mem::size_of::<StandardMfsPlannedSample>());
        }
        if profile::standard_mfs_profile_detail_enabled() {
            eprintln!(
                "standard_mfs_executor_plan stage=build input_samples={} accepted_samples={} skipped_samples={} sample_plan_bytes={} build_ms={:.3}",
                sample_count,
                samples.len(),
                skipped_samples,
                samples
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StandardMfsPlannedSample>()),
                profile::millis(profile::elapsed_since(plan_started)),
            );
        }

        Self {
            samples,
            normalization_sumwt,
            reported_sumwt,
            skipped_samples,
        }
    }

    /// Return the planned samples accepted by the standard gridder.
    pub(crate) fn samples(&self) -> &[StandardMfsPlannedSample] {
        &self.samples
    }

    /// Return the PSF and residual normalization sum of accepted samples.
    pub(crate) fn normalization_sumwt(&self) -> f64 {
        self.normalization_sumwt
    }

    /// Return the CASA-style reported sumwt of accepted samples.
    pub(crate) fn reported_sumwt(&self) -> f64 {
        self.reported_sumwt
    }

    /// Return the number of samples accepted by the gridder plan.
    pub(crate) fn gridded_samples(&self) -> usize {
        self.samples.len()
    }

    /// Return the number of samples rejected while building the plan.
    pub(crate) fn skipped_samples(&self) -> usize {
        self.skipped_samples
    }

    /// Return approximate heap bytes owned by the compact sample plan.
    pub(crate) fn estimated_bytes(&self) -> usize {
        self.samples
            .capacity()
            .saturating_mul(std::mem::size_of::<StandardMfsPlannedSample>())
    }
}

/// One planned standard MFS visibility sample.
pub(crate) struct StandardMfsPlannedSample {
    /// Weighted source visibility for this planned grid sample.
    pub(crate) visibility: Complex32,
    /// Product of imaging weight and sumwt factor used by standard MFS grids.
    pub(crate) grid_weight: f32,
    /// Precomputed positive-UV gridder taps for this `(u, v)` coordinate.
    pub(crate) positive_taps: PositiveTapSet,
}

impl StandardMfsPlannedSample {
    /// Return the grid weight in double precision for f64 grid accumulation.
    pub(crate) fn grid_weight_f64(&self) -> f64 {
        f64::from(self.grid_weight)
    }
}

/// Reusable f64 grid workspace for standard MFS CPU execution.
pub(crate) struct StandardMfsWorkspace {
    psf_grid: Array2<Complex64>,
    residual_grid: Array2<Complex64>,
}

impl StandardMfsWorkspace {
    fn new(gridder: &StandardGridder) -> Self {
        let [nx, ny] = gridder.grid_shape();
        Self {
            psf_grid: Array2::zeros((nx, ny)),
            residual_grid: Array2::zeros((nx, ny)),
        }
    }

    /// Clear and borrow the PSF grid.
    pub(crate) fn clear_psf_grid(&mut self) -> &mut Array2<Complex64> {
        self.psf_grid.fill(Complex64::new(0.0, 0.0));
        &mut self.psf_grid
    }

    /// Clear and borrow the residual grid.
    pub(crate) fn clear_residual_grid(&mut self) -> &mut Array2<Complex64> {
        self.residual_grid.fill(Complex64::new(0.0, 0.0));
        &mut self.residual_grid
    }

    /// Clear and borrow the PSF and residual grids for a combined dirty pass.
    pub(crate) fn clear_dirty_grids(&mut self) -> (&mut Array2<Complex64>, &mut Array2<Complex64>) {
        self.psf_grid.fill(Complex64::new(0.0, 0.0));
        self.residual_grid.fill(Complex64::new(0.0, 0.0));
        (&mut self.psf_grid, &mut self.residual_grid)
    }

    /// Borrow the PSF and residual grids without clearing them.
    pub(crate) fn dirty_grids_mut(&mut self) -> (&mut Array2<Complex64>, &mut Array2<Complex64>) {
        (&mut self.psf_grid, &mut self.residual_grid)
    }

    /// Borrow the PSF and residual grids without clearing them.
    pub(crate) fn dirty_grids(&self) -> (&Array2<Complex64>, &Array2<Complex64>) {
        (&self.psf_grid, &self.residual_grid)
    }
}

/// Return true when a scalar visibility can contribute to a residual grid.
pub(crate) fn finite_visibility(visibility: Complex32) -> bool {
    visibility.re.is_finite() && visibility.im.is_finite()
}

#[cfg(test)]
mod tests {
    use super::{StandardMfsBackend, StandardMfsCpuExecutor, StandardMfsDirtyCpuExecutor};
    use crate::{ImageGeometry, VisibilityBatch, gridder::StandardGridder};
    use num_complex::Complex32;

    #[test]
    fn standard_mfs_plan_buckets_gridder_accepted_samples() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let batches = vec![VisibilityBatch {
            u_lambda: vec![0.0, 4.0, 8.0, 12.0],
            v_lambda: vec![0.0, 1.0, 2.0, 3.0],
            w_lambda: vec![0.0; 4],
            weight: vec![1.0, 2.0, 0.0, 3.0],
            sumwt_factor: vec![1.0, 2.0, 1.0, f32::NAN],
            gridable: vec![true, true, true, true],
            visibility: vec![Complex32::new(1.0, 0.0); 4],
        }];

        let executor = StandardMfsCpuExecutor::new(&gridder, &batches).unwrap();
        let plan = executor.plan();

        assert_eq!(plan.gridded_samples(), 2);
        assert_eq!(plan.skipped_samples(), 2);
        assert!((plan.normalization_sumwt() - 5.0).abs() < 1.0e-6);
        assert!((plan.reported_sumwt() - 5.0).abs() < 1.0e-6);
        assert_eq!(plan.samples()[0].visibility, Complex32::new(1.0, 0.0));
        assert_eq!(plan.samples()[1].visibility, Complex32::new(1.0, 0.0));
    }

    #[test]
    fn reserved_standard_mfs_backend_fails_before_execution() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let batches = Vec::new();
        let error = match StandardMfsCpuExecutor::for_backend(
            StandardMfsBackend::Reserved("gpu"),
            &gridder,
            &batches,
        ) {
            Ok(_) => panic!("reserved backend unexpectedly built an executor"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("standard MFS backend 'gpu' is not implemented"),
            "{error}"
        );
    }

    #[test]
    fn streaming_dirty_executor_accumulates_borrowed_row_blocks() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let left = VisibilityBatch {
            u_lambda: vec![0.0, 4.0],
            v_lambda: vec![0.0, 1.0],
            w_lambda: vec![3.0, -5.0],
            weight: vec![1.0, 2.0],
            sumwt_factor: vec![1.0, 2.0],
            gridable: vec![true, true],
            visibility: vec![Complex32::new(1.0, 0.0); 2],
        };
        let right = VisibilityBatch {
            u_lambda: vec![8.0, 12.0],
            v_lambda: vec![2.0, 3.0],
            w_lambda: vec![7.0, 11.0],
            weight: vec![0.0, 3.0],
            sumwt_factor: vec![1.0, f32::NAN],
            gridable: vec![true, true],
            visibility: vec![Complex32::new(1.0, 0.0); 2],
        };
        let mut executor = StandardMfsDirtyCpuExecutor::new(geometry).unwrap();

        executor.accumulate_batches(&[left]).unwrap();
        executor.accumulate_batches(&[right]).unwrap();
        let accumulation = executor.accumulation();

        assert_eq!(accumulation.gridded_samples, 2);
        assert_eq!(accumulation.skipped_samples, 2);
        assert!((accumulation.normalization_sumwt - 5.0).abs() < 1.0e-6);
        assert!((accumulation.reported_sumwt - 5.0).abs() < 1.0e-6);
        assert!((accumulation.max_abs_w_lambda - 11.0).abs() < 1.0e-6);
    }

    #[test]
    fn reserved_streaming_dirty_backend_fails_before_workspace_creation() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let error = match StandardMfsDirtyCpuExecutor::for_backend(
            StandardMfsBackend::Reserved("gpu"),
            geometry,
        ) {
            Ok(_) => panic!("reserved backend unexpectedly built a dirty executor"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("standard MFS backend 'gpu' is not implemented"),
            "{error}"
        );
    }
}
