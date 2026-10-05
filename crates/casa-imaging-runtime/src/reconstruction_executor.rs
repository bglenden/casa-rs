// SPDX-License-Identifier: LGPL-3.0-or-later

use std::io;

use casa_imaging_reconstruction::{
    ReconstructionCycleError, ReconstructionCycleResult,
    runtime_adapter::{
        ReconstructionPlaneInput, ReconstructionPlanePartial, ReconstructionPlaneStatistics,
        ReconstructionPlaneWork, ReconstructionPlaneWorkspace,
    },
};

use crate::bounded_stream::{
    BOUNDED_WORKER_STACK_BYTES, BlockIdentity, BoundedKernelPlan, BoundedStreamMeasurements,
    KernelPartition, PartitionedKernel, WorkIdentity, execute_bounded_resident,
};
use crate::{LeaseResource, RuntimeOverheadKind, WorkExecutionContext};

pub(crate) const ALLOCATION: &str = "spectral-cycle-minor-cycle";

/// Run-lifetime reservation for one immutable PSF's Clark refresh buffers.
/// Retain this owner until all major cycles and their normal states are dropped.
#[doc(hidden)]
pub struct ClarkWorkspaceReservation {
    _lease: crate::ResourceLease,
    bytes: u64,
}

impl ClarkWorkspaceReservation {
    /// Reserve only the single-plane constant-basis Clark case; other modes
    /// continue to use their bounded per-solve workspace.
    pub fn acquire(
        problem: &casa_imaging_model::CompiledProblem,
        authority: &crate::ResourceAuthority,
        policy: crate::ResourcePolicy,
    ) -> Result<Option<Self>, crate::ResourceError> {
        let bytes = ReconstructionPlaneWorkspace::clark_reuse_bytes(problem);
        if bytes == 0 {
            return Ok(None);
        }
        let lease = authority.reserve_host_memory(policy, "cross-plan-clark-workspace", bytes)?;
        Ok(Some(Self {
            _lease: lease,
            bytes,
        }))
    }

    /// Resident ceiling priced before the first major plan is admitted.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// One owner-derived envelope, shared by admission and execution validation.
pub(crate) struct PlaneExecutionPlan {
    kernel: BoundedKernelPlan,
    pub(crate) heap_bytes: u64,
    /// Bounded outer plane and inner direct-convolution worker stacks; native FFT stacks are reserved as
    /// process-lifetime external-library overhead by the cycle planner.
    pub(crate) stack_bytes: u64,
    pub(crate) workers: usize,
    fft_threads: usize,
}

/// FFTW's pthread pool can survive between phases, so its default stack bound
/// is also reserved as process-lifetime external-library overhead by planning.
pub(crate) fn native_fft_stack_bytes(threads: usize) -> io::Result<u64> {
    if threads <= 1 {
        return Ok(0);
    }
    #[cfg(unix)]
    {
        let mut attributes = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
        let mut stack_bytes = 0;
        // A null pthread_create attribute uses the same platform defaults.
        let status = unsafe { libc::pthread_attr_init(attributes.as_mut_ptr()) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        let mut attributes = unsafe { attributes.assume_init() };
        let status = unsafe { libc::pthread_attr_getstacksize(&attributes, &mut stack_bytes) };
        let destroyed = unsafe { libc::pthread_attr_destroy(&mut attributes) };
        if status != 0 || destroyed != 0 {
            return Err(io::Error::from_raw_os_error(if status != 0 {
                status
            } else {
                destroyed
            }));
        }
        ((threads - 1) as u64)
            .checked_mul(stack_bytes as u64)
            .ok_or_else(|| io::Error::other("native FFT stack overflow"))
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::other(
            "native FFT stack admission is unavailable",
        ))
    }
}

impl PlaneExecutionPlan {
    pub(crate) fn new(workspace: ReconstructionPlaneWorkspace, workers: usize) -> io::Result<Self> {
        let fft_threads = if workspace.parallel_fft() { workers } else { 1 };
        let plane_workers = workers.min(workspace.plane_count());
        let dynamic_bytes = workspace
            .worker_bytes()
            .checked_mul(plane_workers as u64)
            .ok_or_else(|| io::Error::other("plane workspace overflow"))?;
        let partitions = workspace
            .plane_count()
            .checked_mul(if workspace.plane_count() == 1 { 1 } else { 2 })
            .ok_or_else(|| io::Error::other("plane partition count overflow"))?;
        let kernel = BoundedKernelPlan::new::<PlanePartition<'_>, PlanePartial<'_>>(
            plane_workers,
            partitions,
            dynamic_bytes,
        )
        .map_err(|error| io::Error::other(format!("invalid plane kernel plan: {error:?}")))?;
        let plane_stack_bytes = if plane_workers == 1 {
            0
        } else {
            (plane_workers as u64)
                .checked_mul(BOUNDED_WORKER_STACK_BYTES as u64)
                .ok_or_else(|| io::Error::other("plane worker stack overflow"))?
        };
        let (convolution_heap_bytes, convolution_stack_bytes) =
            workspace.parallel_convolution_overhead(fft_threads);
        let heap_bytes = kernel
            .capacity_bytes()
            .checked_sub(plane_stack_bytes)
            .and_then(|bytes| bytes.checked_add(workspace.retained_bytes()))
            .and_then(|bytes| bytes.checked_add(convolution_heap_bytes))
            .ok_or_else(|| io::Error::other("plane collection workspace overflow"))?;
        Ok(Self {
            kernel,
            heap_bytes,
            stack_bytes: plane_stack_bytes
                .checked_add(convolution_stack_bytes)
                .ok_or_else(|| io::Error::other("convolution worker stack overflow"))?,
            workers: if workspace.parallel_fft() {
                workers
            } else {
                plane_workers
            },
            fft_threads,
        })
    }
}

pub(crate) fn execute(
    work: ReconstructionPlaneWork<'_>,
    context: WorkExecutionContext<'_>,
    pass: u32,
    measurements: &mut Option<BoundedStreamMeasurements>,
) -> io::Result<ReconstructionCycleResult> {
    let amount = |resource: &LeaseResource| {
        context
            .resources()
            .iter()
            .find(|capability| capability.resource() == resource)
            .map_or(0, |capability| capability.amount())
    };
    let workers = amount(&LeaseResource::Workers);
    if workers == 0 || workers > context.knobs().workers {
        return Err(io::Error::other(
            "plane worker capability does not match the admitted plan",
        ));
    }
    let workspace = work.workspace();
    let plan = PlaneExecutionPlan::new(
        workspace,
        usize::try_from(workers).map_err(|_| io::Error::other("plane worker count overflow"))?,
    )?;
    let allocation_bytes = context
        .allocations()
        .iter()
        .find(|capability| capability.allocation().as_str() == ALLOCATION)
        .map_or(0, |capability| capability.capacity_bytes());
    if plan.heap_bytes > allocation_bytes
        || plan.stack_bytes
            > amount(&LeaseResource::RuntimeOverhead(
                RuntimeOverheadKind::ThreadStack,
            ))
    {
        return Err(io::Error::other(
            "plane solve exceeds its admitted memory capabilities",
        ));
    }
    if std::env::var_os("CASA_RS_TRACE_IMAGING_STAGE_TIMING").is_some() {
        eprintln!(
            "imaging_minor_cycle_execution_budget admitted_workers={} plane_workers={} fft_threads={} native_stack_bytes={}",
            workers,
            workspace.plane_count().min(workers as usize),
            plan.fft_threads,
            native_fft_stack_bytes(plan.fft_threads)?
        );
    }
    match execute_bounded_resident(
        plan.kernel,
        pass,
        &(),
        PlaneKernel {
            work,
            worker_bytes: workspace.worker_bytes(),
            fft_threads: plan.fft_threads,
        },
    ) {
        Ok(outcome) => {
            *measurements = Some(outcome.measurements);
            Ok(outcome.kernel_completion)
        }
        Err(failure) => {
            *measurements = Some(*failure.measurements);
            Err(io::Error::other(format!(
                "bounded plane reconstruction failed: {:?}",
                failure.cause
            )))
        }
    }
}

struct PlaneKernel<'a> {
    work: ReconstructionPlaneWork<'a>,
    worker_bytes: u64,
    fft_threads: usize,
}

enum PlanePartition<'a> {
    Statistics(usize),
    Solve(ReconstructionPlaneInput<'a>),
}

// The bounded kernel folds these owned partials directly; boxing would add an
// allocation to each solve completion just to shrink the statistics variant.
#[allow(clippy::large_enum_variant)]
enum PlanePartial<'a> {
    Statistics(ReconstructionPlaneStatistics<'a>),
    Solve(ReconstructionPlanePartial<'a>),
}

impl<'a> PartitionedKernel<()> for PlaneKernel<'a> {
    type Partition = PlanePartition<'a>;
    type Partial = PlanePartial<'a>;
    type Completion = ReconstructionCycleResult;
    type Error = ReconstructionCycleError;

    fn partition_count(&self, _: BlockIdentity, _: &()) -> Result<usize, Self::Error> {
        Ok(self.work.threshold_plane_count() + self.work.plane_count())
    }

    fn partition(
        &self,
        _: BlockIdentity,
        _: &(),
        ordinal: usize,
    ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
        let statistics = self.work.threshold_plane_count();
        let (phase, partition) = if ordinal < statistics {
            (0, PlanePartition::Statistics(ordinal))
        } else {
            (
                1,
                PlanePartition::Solve(self.work.prepare_plane(ordinal - statistics)?),
            )
        };
        Ok(KernelPartition::ordered_in_phase(
            phase,
            ordinal as u64,
            0,
            ordinal as u64,
            partition,
        ))
    }

    fn execution_dynamic_capacity_bytes(&self, _: &Self::Partition) -> u64 {
        self.worker_bytes
    }

    fn execute(
        &self,
        _: WorkIdentity,
        _: &(),
        input: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error> {
        match input {
            PlanePartition::Statistics(ordinal) => self
                .work
                .plane_statistics(*ordinal)
                .map(PlanePartial::Statistics),
            PlanePartition::Solve(input) => self
                .work
                .execute_plane(input, self.fft_threads)
                .map(PlanePartial::Solve),
        }
    }

    fn partial_dynamic_capacity_bytes(&self, partial: &Self::Partial) -> u64 {
        match partial {
            PlanePartial::Statistics(_) => 0,
            PlanePartial::Solve(partial) => partial.owned_bytes(),
        }
    }

    fn commit(
        &mut self,
        _: WorkIdentity,
        _: &(),
        partial: Self::Partial,
        _execution: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<(), Self::Error> {
        match partial {
            PlanePartial::Statistics(statistics) => self.work.commit_statistics(statistics),
            PlanePartial::Solve(partial) => self.work.commit_plane(partial),
        }
    }

    fn complete(
        self,
        _execution: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<Self::Completion, Self::Error> {
        self.work.finish()
    }
}

#[cfg(test)]
#[path = "../../casa-imaging-model/tests/common/mod.rs"]
#[allow(dead_code, clippy::duplicate_mod)]
mod model_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::*;
    use casa_imaging_reconstruction::runtime_adapter::ReconstructionPlaneWorkspace;

    use super::model_fixture;
    use crate::complete_data_parallel_mfs_tests::geometry_with_facets;

    #[test]
    fn persistent_clark_bound_is_one_constant_plane_not_a_cube_cache() {
        let clark = compiled_problem(1, ReconstructionAlgorithm::Clark);
        assert!(ReconstructionPlaneWorkspace::clark_reuse_bytes(&clark) > 0);
        for problem in [
            compiled_problem(1, ReconstructionAlgorithm::Hogbom),
            compiled_problem(8, ReconstructionAlgorithm::Clark),
        ] {
            assert_eq!(ReconstructionPlaneWorkspace::clark_reuse_bytes(&problem), 0);
        }
    }

    fn compiled_problem(channels: usize, algorithm: ReconstructionAlgorithm) -> CompiledProblem {
        let geometry =
            geometry_with_facets(FacetLayout::Single).with_spectral(SpectralCoordinateSpec::new(
                FrequencyFrame::Topocentric,
                FrequencyFrame::Topocentric,
                SpectralFrameAnchor::NotApplicable,
                SpectralWcs::Linear {
                    channels,
                    reference_pixel: 0.0,
                    reference_frequency_hz: 1.4e9,
                    increment_hz: 1.0e6,
                },
                RestFrequency::NotApplicable,
                DopplerConvention::NotApplicable,
            ));
        let validity = ProductValidityPolicies::new(
            PrimaryBeamValidityPolicy::new(
                0.2,
                ProductSupportComparison::StrictlyGreater,
                ProductBlankingPolicy::Zero,
            )
            .unwrap(),
            TaylorValidityPolicy::new(
                TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                0.1,
                ProductSupportComparison::StrictlyGreater,
                ProductBlankingPolicy::Zero,
            )
            .unwrap(),
        );
        let basis = if channels == 1 {
            ReconstructionBasis::Constant
        } else {
            ReconstructionBasis::ChannelLocal { channels }
        };
        let specification = ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
                MeasurementEquationContract::new(
                    InstrumentResponse::Scalar,
                    DeclaredInnerProducts::new(
                        ModelInnerProduct::HermitianEuclidean,
                        VisibilityInnerProduct::HermitianEuclidean,
                    ),
                ),
            ),
            ReconstructionContract::new(
                basis,
                algorithm,
                ReconstructionControls::new(8, 0.1, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                validity,
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::UnorderedWithinBudget,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                    .collect(),
            ),
        );
        compile(ImagingRequest::new(
            specification,
            geometry,
            model_fixture::problem_inputs(1, Vec::new(), ModelStateIdentity::Empty),
            ModelLifecycleRequirements::new(
                ModelBounds::new(
                    10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
                )
                .unwrap(),
                NumericPrecision::F64,
                ModelInputCommitment::Empty,
            ),
        ))
        .unwrap()
    }

    fn plane_workspace(
        channels: usize,
        algorithm: ReconstructionAlgorithm,
    ) -> ReconstructionPlaneWorkspace {
        ReconstructionPlaneWorkspace::for_problem(&compiled_problem(channels, algorithm))
            .unwrap()
            .expect("independent reconstruction planes")
    }

    #[cfg(unix)]
    fn default_pthread_stack_bytes() -> u64 {
        let mut attributes = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
        let initialized = unsafe { libc::pthread_attr_init(attributes.as_mut_ptr()) };
        assert_eq!(initialized, 0);
        let mut attributes = unsafe { attributes.assume_init() };
        let mut stack_bytes = 0;
        let queried = unsafe { libc::pthread_attr_getstacksize(&attributes, &mut stack_bytes) };
        let destroyed = unsafe { libc::pthread_attr_destroy(&mut attributes) };
        assert_eq!(queried, 0);
        assert_eq!(destroyed, 0);
        stack_bytes as u64
    }

    #[cfg(unix)]
    #[test]
    fn one_plane_clark_separates_persistent_fft_stacks_from_outer_worker_stack_claim() {
        let workspace = plane_workspace(1, ReconstructionAlgorithm::Clark);
        let serial = PlaneExecutionPlan::new(workspace, 1).unwrap();
        let pthread_stack_bytes = default_pthread_stack_bytes();

        for workers in [1, 4, 8] {
            let plan = PlaneExecutionPlan::new(workspace, workers).unwrap();
            let expected_native_stack = (workers as u64 - 1) * pthread_stack_bytes;

            assert_eq!(plan.workers, workers);
            assert_eq!(plan.fft_threads, workers);
            let (row_heap, row_stacks) = workspace.parallel_convolution_overhead(workers);
            assert_eq!(row_stacks, (workers as u64 - 1) * 128 * 1024);
            assert_eq!(
                row_heap,
                (workers as u64 - 1)
                    * std::mem::size_of::<
                        std::thread::ScopedJoinHandle<
                            'static,
                            Result<(), casa_imaging_reconstruction::MinorCycleError>,
                        >,
                    >() as u64
            );
            assert_eq!(plan.heap_bytes, serial.heap_bytes + row_heap);
            assert_eq!(
                native_fft_stack_bytes(workers).unwrap(),
                expected_native_stack
            );
            // The cycle alternative reserves these native stacks under
            // ExternalLibrary. Do not duplicate them in this node's ThreadStack
            // claim, which covers outer plane workers and transient row workers.
            assert_eq!(plan.stack_bytes, row_stacks);
        }
    }

    #[test]
    fn multi_plane_clark_keeps_native_ffts_single_threaded_under_outer_parallelism() {
        let workspace = plane_workspace(4, ReconstructionAlgorithm::Clark);
        let plan = PlaneExecutionPlan::new(workspace, 8).unwrap();

        assert_eq!(plan.workers, 4);
        assert_eq!(plan.fft_threads, 1);
        assert_eq!(native_fft_stack_bytes(plan.fft_threads).unwrap(), 0);
        assert_eq!(plan.stack_bytes, 4 * BOUNDED_WORKER_STACK_BYTES as u64,);
    }

    #[test]
    fn one_plane_hogbom_does_not_claim_admitted_workers_for_native_fft() {
        let workspace = plane_workspace(1, ReconstructionAlgorithm::Hogbom);
        let serial = PlaneExecutionPlan::new(workspace, 1).unwrap();
        let plan = PlaneExecutionPlan::new(workspace, 4).unwrap();

        assert_eq!(plan.workers, 1);
        assert_eq!(plan.fft_threads, 1);
        assert_eq!(plan.heap_bytes, serial.heap_bytes);
        assert_eq!(plan.stack_bytes, 0);
    }
}
