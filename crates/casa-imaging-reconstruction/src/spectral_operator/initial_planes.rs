// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded initial-pass scheduling over the owner's existing disjoint planes.

use super::*;

const END: usize = usize::MAX;

#[derive(Debug, Clone, Copy)]
struct InitialSample {
    uvw_lambda: [f64; 3],
    visibility: Complex64,
    normal_weight: f64,
    published_weight: f64,
    active: bool,
    next: usize,
}

impl InitialSample {
    fn new(sample: SpectralOperatorSample) -> Self {
        Self {
            uvw_lambda: sample.uvw_lambda(),
            visibility: sample.visibility
                * sample.phase()
                * (sample.imaging_weight * sample.spectral_factor),
            normal_weight: sample.imaging_weight * sample.spectral_factor * sample.spectral_factor,
            published_weight: sample.published_weight
                * sample.spectral_factor
                * sample.spectral_factor,
            active: sample.imaging_weight != 0.0,
            next: END,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    first: usize,
    last: usize,
    #[cfg(test)]
    visits: u64,
}

impl Bucket {
    const EMPTY: Self = Self {
        first: END,
        last: END,
        #[cfg(test)]
        visits: 0,
    };
}

#[derive(Debug)]
pub(super) struct InitialPlaneBatch {
    samples: Vec<InitialSample>,
    buckets: Box<[Bucket]>,
    planes_per_chart: usize,
    flush_samples: usize,
    sample_capacity: usize,
}

impl InitialPlaneBatch {
    fn plane_count(
        specification: &SpectralOperatorSpecification,
        pass: SpectralOperatorPass,
    ) -> Result<usize, SpectralOperatorError> {
        if !specification.is_initial_certified_zero(pass)
            || !matches!(specification.basis, SpectralBasisPlan::ChannelLocal)
            || specification.aw_projection.is_some()
            || specification.mosaic
        {
            return Ok(0);
        }
        specification
            .slab
            .core_depth()
            .checked_mul(specification.polarization_count())
            .and_then(|planes| planes.checked_mul(specification.chart_count()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    pub(super) fn workspace_bytes(
        specification: &SpectralOperatorSpecification,
        pass: SpectralOperatorPass,
        max_replay_block_samples: usize,
    ) -> Result<usize, SpectralOperatorError> {
        let planes = Self::plane_count(specification, pass)?;
        if planes == 0 {
            return Ok(0);
        }
        if max_replay_block_samples == 0 {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        // A canonical input group can fan out to each slab plane once. Flush
        // after that group, without disturbing the stateful CASA resampler.
        max_replay_block_samples
            .checked_add(planes)
            .and_then(|samples| samples.checked_mul(std::mem::size_of::<InitialSample>()))
            .and_then(|bytes| {
                planes
                    .checked_mul(
                        std::mem::size_of::<Bucket>() + std::mem::size_of::<InitialPlaneWork<'_>>(),
                    )
                    .and_then(|descriptors| bytes.checked_add(descriptors))
            })
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    pub(super) fn new(
        specification: &SpectralOperatorSpecification,
        pass: SpectralOperatorPass,
        max_replay_block_samples: usize,
    ) -> Result<Option<Self>, SpectralOperatorError> {
        let planes = Self::plane_count(specification, pass)?;
        if planes == 0 {
            return Ok(None);
        }
        Self::workspace_bytes(specification, pass, max_replay_block_samples)?;
        let sample_capacity = max_replay_block_samples + planes;
        Ok(Some(Self {
            samples: Vec::with_capacity(sample_capacity),
            buckets: vec![Bucket::EMPTY; planes].into_boxed_slice(),
            planes_per_chart: specification.slab.core_depth() * specification.polarization_count(),
            flush_samples: max_replay_block_samples,
            sample_capacity,
        }))
    }

    pub(super) fn ready(&self) -> bool {
        self.samples.len() >= self.flush_samples
    }

    pub(super) fn push(
        &mut self,
        chart: usize,
        operator: &SpectralSlabOperator,
        sample: SpectralOperatorSample,
        polarization: usize,
    ) -> Result<(), SpectralOperatorError> {
        if polarization >= operator.polarization_count {
            return Err(SpectralOperatorError::InvalidSample);
        }
        let Some(channel) = operator.slab.core_index(sample.output_channel) else {
            return Ok(());
        };
        let plane = chart
            .checked_mul(self.planes_per_chart)
            .and_then(|base| base.checked_add(operator.polarization_plane(channel, polarization)))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let bucket = self
            .buckets
            .get_mut(plane)
            .ok_or(SpectralOperatorError::InvalidSample)?;
        if self.samples.len() == self.sample_capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let index = self.samples.len();
        if bucket.last != END {
            self.samples[bucket.last].next = index;
        } else {
            bucket.first = index;
        }
        bucket.last = index;
        self.samples.push(InitialSample::new(sample));
        Ok(())
    }
}

struct InitialPlaneKernel<'a> {
    gridder: &'a ConvolutionOperator,
    dirty: &'a mut Array2<Complex64>,
    dirty_compensation: &'a mut Array2<Complex64>,
    psf: &'a mut Array2<Complex64>,
    psf_compensation: &'a mut Array2<Complex64>,
    sum_weight: &'a mut f64,
    sum_weight_compensation: &'a mut f64,
    published_sum_weight: &'a mut f64,
    published_sum_weight_compensation: &'a mut f64,
    mapped: &'a mut u64,
}

impl InitialPlaneKernel<'_> {
    fn accumulate(&mut self, sample: InitialSample) -> Result<bool, SpectralOperatorError> {
        *self.mapped = self
            .mapped
            .checked_add(1)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        if !sample.active {
            return Ok(false);
        }
        let Some(taps) = self.gridder.taps(sample.uvw_lambda) else {
            return Ok(false);
        };
        let normalization = self.gridder.normalization(taps)?;
        self.gridder.grid_compensated(
            self.dirty,
            self.dirty_compensation,
            taps,
            sample.visibility,
        )?;
        self.gridder.grid_compensated(
            self.psf,
            self.psf_compensation,
            taps,
            Complex64::new(sample.normal_weight, 0.0),
        )?;
        if normalization > 0.0 {
            let corrected = sample.normal_weight * normalization - *self.sum_weight_compensation;
            let updated = *self.sum_weight + corrected;
            *self.sum_weight_compensation = (updated - *self.sum_weight) - corrected;
            *self.sum_weight = updated;
        }
        if normalization != 0.0 {
            let corrected =
                sample.published_weight * normalization - *self.published_sum_weight_compensation;
            let updated = *self.published_sum_weight + corrected;
            *self.published_sum_weight_compensation =
                (updated - *self.published_sum_weight) - corrected;
            *self.published_sum_weight = updated;
        }
        Ok(true)
    }
}

/// One disjoint initial channel/polarization plane, borrowed from its owner.
/// The runtime may reorder jobs but must execute each once before returning.
#[doc(hidden)]
pub struct InitialPlaneWork<'a> {
    kernel: InitialPlaneKernel<'a>,
    samples: &'a [InitialSample],
    first: usize,
    #[cfg(test)]
    bucket: usize,
    executed: bool,
    completed: bool,
    #[cfg(test)]
    visits: u64,
}

impl InitialPlaneWork<'_> {
    /// Accumulate this plane's samples in their original canonical order.
    pub fn execute(&mut self) -> Result<(), SpectralOperatorError> {
        if self.executed {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.executed = true;
        let mut next = self.first;
        while next != END {
            let sample = *self
                .samples
                .get(next)
                .ok_or(SpectralOperatorError::InvalidSample)?;
            let _gridded = self.kernel.accumulate(sample)?;
            #[cfg(test)]
            if _gridded {
                self.visits += TAP_VISITS_PER_SAMPLE;
            }
            next = sample.next;
        }
        self.completed = true;
        Ok(())
    }
}

impl CompleteDataOwnerState {
    pub(super) fn flush_initial_planes(
        &mut self,
        dispatch: &mut impl FnMut(&mut [InitialPlaneWork<'_>]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.initial_planes
            .as_mut()
            .ok_or(SpectralOperatorError::ProblemMismatch)?
            .dispatch(&mut self.operators, dispatch)
    }
}

impl InitialPlaneBatch {
    fn dispatch(
        &mut self,
        operators: &mut [SpectralSlabOperator],
        dispatch: &mut impl FnMut(&mut [InitialPlaneWork<'_>]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        let batch = self;
        if batch.samples.is_empty() {
            return Ok(());
        }
        let mut jobs = Vec::with_capacity(batch.buckets.len());
        for (chart, operator) in operators.iter_mut().enumerate() {
            let dirty = operator
                .dirty_grids
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?;
            let dirty_compensation = operator
                .dirty_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?;
            let psf = operator
                .psf_grids
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?;
            let psf_compensation = operator
                .psf_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?;
            let planes = batch.planes_per_chart;
            if [
                dirty.len(),
                dirty_compensation.len(),
                psf.len(),
                psf_compensation.len(),
                operator.sum_weights.len(),
                operator.sum_weight_compensations.len(),
                operator.published_sum_weights.len(),
                operator.published_sum_weight_compensations.len(),
                operator.mapped_samples.len(),
            ]
            .into_iter()
            .any(|len| len != planes)
            {
                return Err(SpectralOperatorError::ProblemMismatch);
            }
            let mut dirty_compensation = dirty_compensation.iter_mut();
            let mut psf = psf.iter_mut();
            let mut psf_compensation = psf_compensation.iter_mut();
            let mut sum_weights = operator.sum_weights.iter_mut();
            let mut sum_weight_compensations = operator.sum_weight_compensations.iter_mut();
            let mut published_sum_weights = operator.published_sum_weights.iter_mut();
            let mut published_sum_weight_compensations =
                operator.published_sum_weight_compensations.iter_mut();
            let mut mapped = operator.mapped_samples.iter_mut();
            for (plane, dirty) in dirty.iter_mut().enumerate() {
                let kernel = InitialPlaneKernel {
                    gridder: &operator.gridder,
                    dirty,
                    dirty_compensation: dirty_compensation.next().expect("checked plane length"),
                    psf: psf.next().expect("checked plane length"),
                    psf_compensation: psf_compensation.next().expect("checked plane length"),
                    sum_weight: sum_weights.next().expect("checked plane length"),
                    sum_weight_compensation: sum_weight_compensations
                        .next()
                        .expect("checked plane length"),
                    published_sum_weight: published_sum_weights
                        .next()
                        .expect("checked plane length"),
                    published_sum_weight_compensation: published_sum_weight_compensations
                        .next()
                        .expect("checked plane length"),
                    mapped: mapped.next().expect("checked plane length"),
                };
                let bucket = chart * planes + plane;
                let first = batch.buckets[bucket].first;
                if first != END {
                    if jobs.len() == batch.buckets.len() {
                        return Err(SpectralOperatorError::ResidencyOverflow);
                    }
                    jobs.push(InitialPlaneWork {
                        kernel,
                        samples: &batch.samples,
                        first,
                        executed: false,
                        completed: false,
                        #[cfg(test)]
                        bucket,
                        #[cfg(test)]
                        visits: 0,
                    });
                }
            }
        }
        dispatch(&mut jobs)?;
        if jobs.iter().any(|job| !job.completed) {
            return Err(SpectralOperatorError::BlockSequence);
        }
        #[cfg(test)]
        for job in &jobs {
            batch.buckets[job.bucket].visits = job.visits;
        }
        drop(jobs);
        #[cfg(test)]
        for (chart, operator) in operators.iter_mut().enumerate() {
            let visits = batch.buckets
                [chart * batch.planes_per_chart..(chart + 1) * batch.planes_per_chart]
                .iter()
                .map(|bucket| bucket.visits)
                .sum();
            record_measurement(&mut operator.measurements.dirty_grid_tap_visits, visits);
            record_measurement(&mut operator.measurements.psf_grid_tap_visits, visits);
        }
        batch.samples.clear();
        batch.buckets.fill(Bucket::EMPTY);
        Ok(())
    }
}

impl SpectralSlabOperator {
    pub(super) fn push_initial_plane(
        &mut self,
        sample: SpectralOperatorSample,
        polarization: usize,
    ) -> Result<(), SpectralOperatorError> {
        if polarization >= self.polarization_count {
            return Err(SpectralOperatorError::InvalidSample);
        }
        let Some(channel) = self.slab.core_index(sample.output_channel) else {
            return Ok(());
        };
        let plane = self.polarization_plane(channel, polarization);
        let mut kernel = InitialPlaneKernel {
            gridder: &self.gridder,
            dirty: &mut self
                .dirty_grids
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            dirty_compensation: &mut self
                .dirty_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            psf: &mut self
                .psf_grids
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            psf_compensation: &mut self
                .psf_compensations
                .as_mut()
                .ok_or(SpectralOperatorError::ProblemMismatch)?[plane],
            sum_weight: &mut self.sum_weights[plane],
            sum_weight_compensation: &mut self.sum_weight_compensations[plane],
            published_sum_weight: &mut self.published_sum_weights[plane],
            published_sum_weight_compensation: &mut self.published_sum_weight_compensations[plane],
            mapped: &mut self.mapped_samples[plane],
        };
        let _gridded = kernel.accumulate(InitialSample::new(sample))?;
        #[cfg(test)]
        if _gridded {
            record_measurement(
                &mut self.measurements.dirty_grid_tap_visits,
                TAP_VISITS_PER_SAMPLE,
            );
            record_measurement(
                &mut self.measurements.psf_grid_tap_visits,
                TAP_VISITS_PER_SAMPLE,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operator(w_projection: bool) -> SpectralSlabOperator {
        let slab = SpectralSlabPlan::compile(4, 1, 2, SpectralKernel::Linear).unwrap();
        let mut operator = super::super::tests::cube_operator(slab);
        operator.polarization_count = 2;
        if w_projection {
            operator.geometry.grid_shape = [64, 64];
            operator.geometry.image_shape = [48, 48];
            operator.geometry.image_blc = [8, 8];
            operator.gridder = ConvolutionOperator::new(
                &operator.geometry,
                Some(
                    casa_imaging_model::WProjectionContract::new(
                        10_000.0,
                        std::num::NonZeroUsize::new(9),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        }
        let shape = (
            operator.geometry.grid_shape[0],
            operator.geometry.grid_shape[1],
        );
        for grids in [
            &mut operator.dirty_grids,
            &mut operator.dirty_compensations,
            &mut operator.psf_grids,
            &mut operator.psf_compensations,
        ] {
            *grids = Some(vec![Array2::zeros(shape); 4]);
        }
        for values in [
            &mut operator.sum_weights,
            &mut operator.sum_weight_compensations,
            &mut operator.published_sum_weights,
            &mut operator.published_sum_weight_compensations,
        ] {
            *values = vec![0.0; 4];
        }
        operator.mapped_samples = vec![0; 4];
        operator
    }

    fn batch(charts: usize, threshold: usize) -> InitialPlaneBatch {
        let planes = charts * 4;
        InitialPlaneBatch {
            samples: Vec::with_capacity(threshold + planes),
            buckets: vec![Bucket::EMPTY; planes].into_boxed_slice(),
            planes_per_chart: 4,
            flush_samples: threshold,
            sample_capacity: threshold + planes,
        }
    }

    fn sample(row: usize, channel: usize) -> SpectralOperatorSample {
        let visibility = [1.0e16, 1.0, -1.0e16, 3.0][row % 4];
        SpectralOperatorSample::new(
            channel,
            if row == 9 {
                [1.0e15, 0.0, 0.0]
            } else {
                [1.0, -2.0, 600.0]
            },
            1.0e9,
            0.017,
            [visibility, -0.3],
            if row == 7 { 0.0 } else { 0.75 },
            if row == 5 { f64::MIN_POSITIVE } else { -0.25 },
        )
        .unwrap()
        .with_published_weight(1.25)
        .unwrap()
    }

    // Independent expression of the pre-dispatch nonmosaic channel-local path.
    fn reference(
        operator: &mut SpectralSlabOperator,
        sample: SpectralOperatorSample,
        polarization: usize,
    ) {
        let Some(channel) = operator.slab.core_index(sample.output_channel) else {
            return;
        };
        let plane = operator.polarization_plane(channel, polarization);
        operator.mapped_samples[plane] += 1;
        if sample.imaging_weight == 0.0 {
            return;
        }
        let Some(taps) = operator.operator_taps(sample, false).unwrap() else {
            return;
        };
        let taps = operator.prepare_grid_taps(taps, true).unwrap();
        let normalization = taps.normal_normalization(&operator.gridder).unwrap();
        let published_normalization = taps.imaging_normalization(&operator.gridder).unwrap();
        operator
            .grid_dirty_term(
                plane,
                &taps,
                sample.visibility
                    * sample.phase()
                    * (sample.imaging_weight * sample.spectral_factor),
            )
            .unwrap();
        operator
            .grid_normal_moment(
                plane,
                &taps,
                sample.imaging_weight * sample.spectral_factor * sample.spectral_factor,
                normalization,
            )
            .unwrap();
        operator
            .accumulate_published_sum_weight(
                plane,
                sample.published_weight * sample.spectral_factor * sample.spectral_factor,
                published_normalization,
                None,
            )
            .unwrap();
    }

    fn assert_same(expected: &SpectralSlabOperator, actual: &SpectralSlabOperator) {
        assert_eq!(actual.dirty_grids, expected.dirty_grids);
        assert_eq!(actual.dirty_compensations, expected.dirty_compensations);
        assert_eq!(actual.psf_grids, expected.psf_grids);
        assert_eq!(actual.psf_compensations, expected.psf_compensations);
        assert_eq!(actual.sum_weights, expected.sum_weights);
        assert_eq!(
            actual.sum_weight_compensations,
            expected.sum_weight_compensations
        );
        assert_eq!(actual.published_sum_weights, expected.published_sum_weights);
        assert_eq!(
            actual.published_sum_weight_compensations,
            expected.published_sum_weight_compensations
        );
        assert_eq!(actual.mapped_samples, expected.mapped_samples);
        assert_eq!(
            actual.measurements.dirty_grid_tap_visits,
            expected.measurements.dirty_grid_tap_visits
        );
        assert_eq!(
            actual.measurements.psf_grid_tap_visits,
            expected.measurements.psf_grid_tap_visits
        );
    }

    #[test]
    fn initial_planes_preserve_exact_order_weights_and_compensation_with_reverse_dispatch() {
        for w_projection in [false, true] {
            let mut expected = [operator(w_projection), operator(w_projection)];
            let mut direct = [operator(w_projection), operator(w_projection)];
            let mut dispatched = [operator(w_projection), operator(w_projection)];
            let mut batch = batch(2, 5);
            let initial_capacity = batch.samples.capacity();
            let mut dispatches = 0;
            let mut reverse = |planes: &mut [InitialPlaneWork<'_>]| {
                dispatches += 1;
                for plane in planes.iter_mut().rev() {
                    plane.execute()?;
                }
                Ok(())
            };
            for row in 0..16 {
                for chart in 0..2 {
                    for channel in 0..4 {
                        for polarization in 0..2 {
                            let sample = sample(row, channel);
                            reference(&mut expected[chart], sample, polarization);
                            direct[chart]
                                .push_polarization(sample, polarization)
                                .unwrap();
                            batch
                                .push(chart, &dispatched[chart], sample, polarization)
                                .unwrap();
                        }
                    }
                }
                if batch.ready() {
                    batch.dispatch(&mut dispatched, &mut reverse).unwrap();
                }
            }
            batch.dispatch(&mut dispatched, &mut reverse).unwrap();
            assert!(dispatches > 1);
            assert_eq!(batch.samples.capacity(), initial_capacity);
            assert!(batch.samples.is_empty());
            for chart in 0..2 {
                assert_same(&expected[chart], &direct[chart]);
                assert_same(&expected[chart], &dispatched[chart]);
                assert!(
                    dispatched[chart]
                        .sum_weights
                        .iter()
                        .all(|weight| *weight > 0.0)
                );
            }
        }
    }

    #[test]
    fn initial_planes_enforce_fixed_capacity_selection_and_execution() {
        assert!(matches!(
            SpectralOperatorSample::new(1, [1.0, -2.0, 0.0], 1.0e9, 0.0, [1.0, 0.0], 1.0, 0.0),
            Err(SpectralOperatorError::InvalidSample)
        ));
        let mut operators = [operator(false)];
        let mut batch = batch(1, 1);
        batch
            .dispatch(&mut operators, &mut |_| {
                panic!("empty batch must not dispatch")
            })
            .unwrap();
        assert!(matches!(
            batch.push(0, &operators[0], sample(0, 1), 2),
            Err(SpectralOperatorError::InvalidSample)
        ));
        batch.push(0, &operators[0], sample(0, 0), 0).unwrap();
        assert!(batch.samples.is_empty(), "out-of-slab sample is ignored");
        for _ in 0..batch.sample_capacity {
            batch.push(0, &operators[0], sample(0, 1), 0).unwrap();
        }
        let capacity = batch.samples.capacity();
        assert!(matches!(
            batch.push(0, &operators[0], sample(0, 1), 0),
            Err(SpectralOperatorError::ResidencyOverflow)
        ));
        assert_eq!(batch.samples.capacity(), capacity);
        assert!(matches!(
            batch.dispatch(&mut operators, &mut |_| Ok(())),
            Err(SpectralOperatorError::BlockSequence)
        ));
        batch
            .dispatch(&mut operators, &mut |planes| {
                for plane in planes {
                    plane.execute()?;
                    assert!(matches!(
                        plane.execute(),
                        Err(SpectralOperatorError::BlockSequence)
                    ));
                }
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn initial_planes_do_not_accept_swallowed_worker_failure() {
        let mut operators = [operator(false)];
        operators[0].mapped_samples[0] = u64::MAX;
        let mut batch = batch(1, 1);
        batch.push(0, &operators[0], sample(0, 1), 0).unwrap();
        assert!(matches!(
            batch.dispatch(&mut operators, &mut |planes| {
                assert!(matches!(
                    planes[0].execute(),
                    Err(SpectralOperatorError::CoverageOverflow)
                ));
                Ok(())
            }),
            Err(SpectralOperatorError::BlockSequence)
        ));
    }
}
