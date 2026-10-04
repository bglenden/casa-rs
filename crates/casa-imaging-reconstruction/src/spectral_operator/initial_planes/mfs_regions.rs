// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded routing into disjoint strips of one initial MFS grid pair.

use super::*;
use ndarray::{ArrayViewMut2, Axis};

pub(super) const STRIP_ROWS: usize = 64;
const _: () = assert!(TAP_COUNT <= STRIP_ROWS);

#[derive(Debug)]
struct Record {
    sample: InitialSample,
    taps: Option<SampleTaps>,
}

#[derive(Debug)]
struct Route {
    record: usize,
    next: usize,
}

#[derive(Debug)]
pub(super) struct MfsRegions {
    records: Vec<Record>,
    routes: Vec<Route>,
    buckets: Box<[Bucket]>,
    capacity: usize,
}

impl MfsRegions {
    pub(super) fn supports(
        spec: &SpectralOperatorSpecification,
        pass: SpectralOperatorPass,
    ) -> bool {
        spec.is_initial_certified_zero(pass)
            && spec.supports_bulk_mfs()
            && spec.polarization_coordinates.as_ref() == [PolarizationCoordinate::StokesI]
    }

    pub(super) fn workspace_bytes(
        shape: [usize; 2],
        capacity: usize,
    ) -> Result<usize, SpectralOperatorError> {
        if capacity == 0 || shape.contains(&0) {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        capacity
            .checked_mul(size_of::<Record>() + 2 * size_of::<Route>())
            .and_then(|bytes| {
                shape[0]
                    .div_ceil(STRIP_ROWS)
                    .checked_mul(size_of::<Bucket>() + size_of::<InitialPlaneWork<'static>>())
                    .and_then(|jobs| bytes.checked_add(jobs))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    pub(super) fn new(shape: [usize; 2], capacity: usize) -> Result<Self, SpectralOperatorError> {
        Self::workspace_bytes(shape, capacity)?;
        Ok(Self {
            records: Vec::with_capacity(capacity),
            routes: Vec::with_capacity(capacity * 2),
            buckets: vec![Bucket::EMPTY; shape[0].div_ceil(STRIP_ROWS)].into_boxed_slice(),
            capacity,
        })
    }

    pub(super) fn ready(&self) -> bool {
        self.records.len() == self.capacity
    }

    pub(super) fn push(
        &mut self,
        operator: &SpectralSlabOperator,
        sample: SpectralOperatorSample,
    ) -> Result<(), SpectralOperatorError> {
        if self.ready() {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let sample = InitialSample::new(sample);
        let taps = if sample.active {
            operator.gridder.taps(sample.uvw_lambda)
        } else {
            None
        };
        let record = self.records.len();
        self.records.push(Record { sample, taps });
        if let Some(taps) = taps {
            operator.gridder.validate_taps(taps)?;
            // A standard seven-row stencil intersects at most two strips.
            for strip in taps.x.start / STRIP_ROWS..=(taps.x.start + TAP_COUNT - 1) / STRIP_ROWS {
                let bucket = &mut self.buckets[strip];
                let index = self.routes.len();
                if bucket.last == END {
                    bucket.first = index;
                } else {
                    self.routes[bucket.last].next = index;
                }
                bucket.last = index;
                self.routes.push(Route { record, next: END });
            }
        }
        Ok(())
    }

    pub(super) fn dispatch(
        &mut self,
        operator: &mut SpectralSlabOperator,
        dispatch: &mut impl FnMut(&mut [InitialPlaneWork<'_>]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        if self.records.is_empty() {
            return Ok(());
        }
        let ConvolutionOperator::Standard(gridder) = &operator.gridder else {
            return Err(SpectralOperatorError::UnsupportedProblem);
        };
        let dirty = operator
            .dirty_grids
            .as_mut()
            .ok_or(SpectralOperatorError::ProblemMismatch)?;
        let psf = operator
            .psf_grids
            .as_mut()
            .ok_or(SpectralOperatorError::ProblemMismatch)?;
        let ([dirty], [psf]) = (dirty.as_mut_slice(), psf.as_mut_slice()) else {
            return Err(SpectralOperatorError::ProblemMismatch);
        };
        let mut jobs = Vec::with_capacity(self.buckets.len());
        for (strip, (dirty, psf)) in dirty
            .axis_chunks_iter_mut(Axis(0), STRIP_ROWS)
            .zip(psf.axis_chunks_iter_mut(Axis(0), STRIP_ROWS))
            .enumerate()
        {
            let first = self.buckets[strip].first;
            if first != END {
                jobs.push(InitialPlaneWork(InitialWork::Mfs(MfsWork {
                    gridder,
                    dirty,
                    psf,
                    first_row: strip * STRIP_ROWS,
                    records: &self.records,
                    routes: &self.routes,
                    first,
                    executed: false,
                    completed: false,
                })));
            }
        }
        dispatch(&mut jobs)?;
        if jobs
            .iter()
            .any(|job| !matches!(&job.0, InitialWork::Mfs(work) if work.completed))
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        drop(jobs);
        for record in &self.records {
            operator.mapped_samples[0] = operator.mapped_samples[0]
                .checked_add(1)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            if record.taps.is_some() {
                for (value, sum, compensation) in [
                    (
                        record.sample.normal_weight,
                        &mut operator.sum_weights[0],
                        &mut operator.sum_weight_compensations[0],
                    ),
                    (
                        record.sample.published_weight,
                        &mut operator.published_sum_weights[0],
                        &mut operator.published_sum_weight_compensations[0],
                    ),
                ] {
                    let corrected = value - *compensation;
                    let updated = *sum + corrected;
                    *compensation = (updated - *sum) - corrected;
                    *sum = updated;
                }
                #[cfg(test)]
                {
                    record_measurement(
                        &mut operator.measurements.dirty_grid_tap_visits,
                        TAP_VISITS_PER_SAMPLE,
                    );
                    record_measurement(
                        &mut operator.measurements.psf_grid_tap_visits,
                        TAP_VISITS_PER_SAMPLE,
                    );
                }
            }
        }
        self.records.clear();
        self.routes.clear();
        self.buckets.fill(Bucket::EMPTY);
        Ok(())
    }
}

pub(super) struct MfsWork<'a> {
    gridder: &'a StandardConvolution,
    dirty: ArrayViewMut2<'a, Complex64>,
    psf: ArrayViewMut2<'a, Complex64>,
    first_row: usize,
    records: &'a [Record],
    routes: &'a [Route],
    first: usize,
    executed: bool,
    completed: bool,
}

impl MfsWork<'_> {
    pub(super) fn execute(&mut self) -> Result<(), SpectralOperatorError> {
        if self.executed {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.executed = true;
        let mut next = self.first;
        while next != END {
            let route = &self.routes[next];
            let record = &self.records[route.record];
            let taps = record.taps.ok_or(SpectralOperatorError::InvalidSample)?;
            self.gridder.grid_rows(
                &mut self.dirty,
                taps,
                record.sample.visibility,
                self.first_row,
            );
            self.gridder.grid_rows(
                &mut self.psf,
                taps,
                Complex64::new(record.sample.normal_weight, 0.0),
                self.first_row,
            );
            next = route.next;
        }
        self.completed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operator() -> SpectralSlabOperator {
        let slab = SpectralSlabPlan::compile(1, 0, 1, SpectralKernel::Nearest).unwrap();
        let mut operator = super::super::super::tests::cube_operator(slab);
        operator.basis = SpectralBasisPlan::Polynomial(BlockNormalPlan::constant(1e9).unwrap());
        operator.geometry.grid_shape = [512, 64];
        operator.gridder = ConvolutionOperator::new(&operator.geometry, None).unwrap();
        for grids in [&mut operator.dirty_grids, &mut operator.psf_grids] {
            *grids = Some(vec![Array2::zeros((512, 64))]);
        }
        operator
    }

    #[test]
    fn mfs_regions_match_scalar_across_boundaries_and_worker_counts() {
        for workers in [1, 4, 8] {
            let mut scalar = operator();
            let mut candidate = operator();
            let mut batch = MfsRegions::new([512, 64], 19).unwrap();
            let records = batch.records.as_ptr();
            let routes = batch.routes.as_ptr();
            let mut largest_dispatch = 0;
            for first in (0..152).step_by(19) {
                for index in first..first + 19 {
                    // Cross strip boundaries, cover every strip, include flags
                    // and samples outside the supported grid.
                    let row = if index % 17 == 0 {
                        -20.0
                    } else {
                        (index % 8 * 64) as f64 + 0.25
                    };
                    let uv = [
                        (row - 256.0) * scalar.gridder.standard().du_lambda,
                        0.25 * scalar.gridder.standard().dv_lambda,
                        0.0,
                    ];
                    let sample = SpectralOperatorSample::new(
                        0,
                        uv,
                        299_792_458.0,
                        0.03,
                        [1.0 + index as f64 * 0.01, -0.2],
                        if index % 13 == 0 { 0.0 } else { 0.7 },
                        1.0,
                    )
                    .unwrap()
                    .with_published_weight(0.4)
                    .unwrap();
                    scalar.push_polarization(sample, 0).unwrap();
                    batch.push(&candidate, sample).unwrap();
                }
                assert!(batch.routes.len() <= 2 * batch.capacity);
                batch
                    .dispatch(&mut candidate, &mut |jobs| {
                        largest_dispatch = largest_dispatch.max(jobs.len());
                        let chunk = jobs.len().div_ceil(workers).max(1);
                        std::thread::scope(|scope| {
                            let handles = jobs
                                .chunks_mut(chunk)
                                .map(|jobs| {
                                    scope.spawn(move || {
                                        jobs.iter_mut().try_for_each(InitialPlaneWork::execute)
                                    })
                                })
                                .collect::<Vec<_>>();
                            for handle in handles {
                                handle.join().unwrap()?;
                            }
                            Ok(())
                        })
                    })
                    .unwrap();
                assert_eq!(batch.records.as_ptr(), records);
                assert_eq!(batch.routes.as_ptr(), routes);
            }
            assert_eq!(largest_dispatch, 8);
            for (left, right) in scalar.dirty_grids.as_ref().unwrap()[0]
                .iter()
                .zip(candidate.dirty_grids.as_ref().unwrap()[0].iter())
                .chain(
                    scalar.psf_grids.as_ref().unwrap()[0]
                        .iter()
                        .zip(candidate.psf_grids.as_ref().unwrap()[0].iter()),
                )
            {
                assert!((*left - *right).norm() < 1e-12);
            }
            assert_eq!(scalar.mapped_samples, candidate.mapped_samples);
            assert!((scalar.sum_weights[0] - candidate.sum_weights[0]).abs() < 1e-12);
            assert!(
                (scalar.published_sum_weights[0] - candidate.published_sum_weights[0]).abs()
                    < 1e-12
            );
        }
    }

    #[test]
    fn mfs_regions_reject_missing_and_duplicate_execution() {
        let mut operator = operator();
        let sample =
            SpectralOperatorSample::new(0, [0.0; 3], 1e9, 0.0, [1.0, 0.0], 1.0, 1.0).unwrap();
        let mut batch = MfsRegions::new([512, 64], 1).unwrap();
        batch.push(&operator, sample).unwrap();
        assert!(batch.dispatch(&mut operator, &mut |_| Ok(())).is_err());
        batch
            .dispatch(&mut operator, &mut |jobs| {
                for job in jobs {
                    job.execute()?;
                    assert!(job.execute().is_err());
                }
                Ok(())
            })
            .unwrap();
    }
}
