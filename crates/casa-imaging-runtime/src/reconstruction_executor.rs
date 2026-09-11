// SPDX-License-Identifier: LGPL-3.0-or-later

use std::io;

use casa_imaging_reconstruction::{
    ReconstructionCycleError, ReconstructionCycleResult,
    runtime_adapter::{
        ReconstructionPlaneInput, ReconstructionPlanePartial, ReconstructionPlaneWork,
        ReconstructionPlaneWorkspace,
    },
};

use crate::bounded_stream::{
    BOUNDED_WORKER_STACK_BYTES, BlockIdentity, BoundedKernelPlan, BoundedStreamMeasurements,
    KernelPartition, PartitionedKernel, WorkIdentity, execute_bounded_resident,
};
use crate::{LeaseResource, RuntimeOverheadKind, WorkExecutionContext};

pub(crate) const ALLOCATION: &str = "spectral-cycle-minor-cycle";

/// One owner-derived envelope, shared by admission and execution validation.
pub(crate) struct PlaneExecutionPlan {
    kernel: BoundedKernelPlan,
    pub(crate) heap_bytes: u64,
    pub(crate) stack_bytes: u64,
}

impl PlaneExecutionPlan {
    pub(crate) fn new(workspace: ReconstructionPlaneWorkspace, workers: usize) -> io::Result<Self> {
        let dynamic_bytes = workspace
            .worker_bytes()
            .checked_mul(workers as u64)
            .ok_or_else(|| io::Error::other("plane workspace overflow"))?;
        let kernel = BoundedKernelPlan::new::<
            ReconstructionPlaneInput<'_>,
            ReconstructionPlanePartial<'_>,
        >(workers, workspace.plane_count(), dynamic_bytes)
        .map_err(|error| io::Error::other(format!("invalid plane kernel plan: {error:?}")))?;
        let stack_bytes = if workers == 1 {
            0
        } else {
            (workers as u64)
                .checked_mul(BOUNDED_WORKER_STACK_BYTES as u64)
                .ok_or_else(|| io::Error::other("plane worker stack overflow"))?
        };
        let heap_bytes = kernel
            .capacity_bytes()
            .checked_sub(stack_bytes)
            .and_then(|bytes| bytes.checked_add(workspace.retained_bytes()))
            .ok_or_else(|| io::Error::other("plane collection workspace overflow"))?;
        Ok(Self {
            kernel,
            heap_bytes,
            stack_bytes,
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
    match execute_bounded_resident(
        plan.kernel,
        pass,
        &(),
        PlaneKernel {
            work,
            worker_bytes: workspace.worker_bytes(),
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
}

impl<'a> PartitionedKernel<()> for PlaneKernel<'a> {
    type Partition = ReconstructionPlaneInput<'a>;
    type Partial = ReconstructionPlanePartial<'a>;
    type Completion = ReconstructionCycleResult;
    type Error = ReconstructionCycleError;

    fn partition_count(&self, _: BlockIdentity, _: &()) -> Result<usize, Self::Error> {
        Ok(self.work.plane_count())
    }

    fn partition(
        &self,
        _: BlockIdentity,
        _: &(),
        ordinal: usize,
    ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
        Ok(KernelPartition::ordered(
            ordinal as u64,
            0,
            ordinal as u64,
            self.work.prepare_plane(ordinal)?,
        ))
    }

    fn partition_dynamic_capacity_bytes(&self, input: &Self::Partition) -> u64 {
        input.owned_bytes()
    }

    fn execution_dynamic_capacity_bytes(&self, input: &Self::Partition) -> u64 {
        self.worker_bytes.saturating_sub(input.owned_bytes())
    }

    fn execute(
        &self,
        _: WorkIdentity,
        _: &(),
        input: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error> {
        self.work.execute_plane(input)
    }

    fn partial_dynamic_capacity_bytes(&self, partial: &Self::Partial) -> u64 {
        partial.owned_bytes()
    }

    fn commit(
        &mut self,
        _: WorkIdentity,
        _: &(),
        partial: Self::Partial,
    ) -> Result<(), Self::Error> {
        self.work.commit_plane(partial)
    }

    fn complete(self) -> Result<Self::Completion, Self::Error> {
        self.work.finish()
    }
}
