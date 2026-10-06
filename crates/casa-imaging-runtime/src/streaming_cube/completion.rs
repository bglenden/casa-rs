// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native-band transfer into the existing runtime-bound normal-state fold.

use super::*;
use casa_imaging_reconstruction::SpectralOperatorPrimitives;
use casa_imaging_reconstruction::runtime_adapter::{CubeNormalRefresh, CubeResidual};
use casa_imaging_reconstruction::{FinalNormalState, ModelGenerationId};

pub(crate) struct PendingCubeRefresh {
    evidence: CubeNormalRefresh,
    binding: CompleteDataExecutionBinding,
}

impl PendingCubeRefresh {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        context: WorkExecutionContext<'_>,
        reconciliation_node: &WorkNodeId,
        imported_node: &WorkNodeId,
        specification: &SpectralOperatorSpecification,
        previous: &FinalNormalState,
        model: ModelGenerationId,
        original: &WeightingReplayCompletion,
        storage: &NormalStoragePlan,
    ) -> Result<Self, CompleteDataOperatorError> {
        if context.node().id != *reconciliation_node
            || context.node().kind != WorkKind::Compute
            || context.compiled().problem_id() != original.problem_id()
            || !context
                .node()
                .dependencies
                .contains(&WorkDependency::Fence(FenceId::new(
                    imported_node.clone(),
                    FenceKind::Io,
                )))
        {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        let evidence = previous.begin_streaming_cube_refresh(
            specification,
            original.reconstruction_summary(),
            original.selected_generation(),
            original
                .continuum_transform()
                .map(|value| value.generation_id()),
            model,
            storage,
        )?;
        Ok(Self {
            evidence,
            binding: CompleteDataExecutionBinding {
                problem: original.problem_id(),
                attempt: context.attempt_id(),
                replay_node: imported_node.clone(),
                reconciliation_node: reconciliation_node.clone(),
                lease_epoch: context.lease_epoch(),
                observation_predecessor_required: false,
            },
        })
    }

    pub(crate) fn append(
        &mut self,
        residual: CubeResidual,
    ) -> Result<(), CompleteDataOperatorError> {
        self.evidence.append(residual)?;
        Ok(())
    }

    pub(crate) fn complete(self) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        Ok(CompleteDataOperatorResult {
            evidence: self.evidence.finish()?,
            attempt: self.binding.attempt,
            replay_node: self.binding.replay_node,
            reconciliation_node: self.binding.reconciliation_node,
            lease_epoch: self.binding.lease_epoch,
            observation_predecessor_required: false,
        })
    }
}

impl CompleteDataSlabResult {
    /// Adopt an initial band only after the selected-source I/O fence settled.
    /// The reconciliation node comes from the composed phase plan, not the band.
    pub(crate) fn from_streaming_cube(
        context: WorkExecutionContext<'_>,
        reconciliation_node: &WorkNodeId,
        specification: &SpectralOperatorSpecification,
        primitives: SpectralOperatorPrimitives,
        replay: &WeightingReplayCompletion,
    ) -> Result<Self, CompleteDataOperatorError> {
        let predecessor = context
            .predecessor_observation_completion(replay.owner_node())
            .ok_or(CompleteDataOperatorError::ExecutionBinding)?;
        if context.node().id != *reconciliation_node
            || context.node().kind != WorkKind::Compute
            || !context
                .node()
                .dependencies
                .contains(&WorkDependency::Fence(FenceId::new(
                    replay.owner_node().clone(),
                    FenceKind::Io,
                )))
            || context.compiled().problem_id() != replay.problem_id()
            || context.attempt_id() != replay.attempt_id()
            || context.lease_epoch() != replay.lease_epoch()
            || predecessor.attempt_id() != replay.attempt_id()
            || predecessor.lease_epoch() != replay.lease_epoch()
            || predecessor.owner_node() != replay.owner_node()
            || !predecessor.settled_fences().contains(&FenceKind::Io)
            || predecessor.owner_completion().generation_id() != replay.selected_generation()
            || predecessor.owner_completion().sample_count() != replay.sample_count()
        {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        let evidence = CompleteDataOwnerResult::from_streaming_cube(
            specification,
            primitives,
            replay.reconstruction_summary(),
            replay.selected_generation(),
            replay
                .continuum_transform()
                .map(|value| value.generation_id()),
        )?;
        if evidence.completion().problem_id() != replay.problem_id() {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(Self {
            evidence,
            binding: CompleteDataExecutionBinding {
                problem: replay.problem_id(),
                attempt: replay.attempt_id(),
                replay_node: replay.owner_node().clone(),
                reconciliation_node: reconciliation_node.clone(),
                lease_epoch: replay.lease_epoch(),
                observation_predecessor_required: true,
            },
        })
    }
}
