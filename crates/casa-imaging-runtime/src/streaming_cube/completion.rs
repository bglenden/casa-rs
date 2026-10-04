// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native-band transfer into the existing runtime-bound normal-state fold.

use super::*;
use casa_imaging_reconstruction::runtime_adapter::{CubeNormalRefresh, CubeResidual};
use casa_imaging_reconstruction::{FinalNormalState, ModelGenerationId};
use casa_imaging_reconstruction::{SpectralOperatorPrimitives, WeightingReplaySummary};

pub(crate) struct PendingCubeRefresh {
    evidence: CubeNormalRefresh,
    binding: CompleteDataExecutionBinding,
}

impl PendingCubeRefresh {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn during_read(
        context: WorkExecutionContext<'_>,
        reconciliation_node: &WorkNodeId,
        specification: &SpectralOperatorSpecification,
        previous: &FinalNormalState,
        model: ModelGenerationId,
        replay: &WeightingReplaySummary,
        storage: &NormalStoragePlan,
    ) -> Result<Self, CompleteDataOperatorError> {
        if context.node().kind != WorkKind::ObservationRead {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(Self {
            evidence: previous.begin_streaming_cube_refresh(
                specification,
                replay,
                model,
                storage,
            )?,
            binding: CompleteDataExecutionBinding {
                problem: context.compiled().problem_id(),
                attempt: context.attempt_id(),
                replay_node: context.node().id.clone(),
                reconciliation_node: reconciliation_node.clone(),
                lease_epoch: context.lease_epoch(),
                observation_predecessor_required: true,
            },
        })
    }

    pub(crate) fn complete_rebound(
        self,
        replay: &WeightingReplayCompletion,
    ) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        if self.binding.problem != replay.problem_id()
            || self.binding.attempt != replay.attempt_id()
            || self.binding.replay_node != *replay.owner_node()
            || self.binding.lease_epoch != replay.lease_epoch()
        {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        let evidence = self.evidence.finish()?;
        if evidence.completion().replay_id() != replay.reconstruction_summary().replay_id() {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(CompleteDataOperatorResult {
            evidence,
            attempt: self.binding.attempt,
            replay_node: self.binding.replay_node,
            reconciliation_node: self.binding.reconciliation_node,
            lease_epoch: self.binding.lease_epoch,
            observation_predecessor_required: true,
            delivered_source_sample_count: Some(replay.delivered_source_sample_count()),
        })
    }
    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn complete(self) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        Ok(CompleteDataOperatorResult {
            evidence: self.evidence.finish()?,
            attempt: self.binding.attempt,
            replay_node: self.binding.replay_node,
            reconciliation_node: self.binding.reconciliation_node,
            lease_epoch: self.binding.lease_epoch,
            observation_predecessor_required: false,
            delivered_source_sample_count: None,
        })
    }
}

pub(crate) struct PendingStreamingCubeFold {
    binding: CompleteDataExecutionBinding,
    replay: WeightingReplaySummary,

    storage: NormalStoragePlan,
    fold: Option<PendingCompleteDataSlabFold>,
}

impl PendingStreamingCubeFold {
    /// Numerical output can be folded while the locked source traversal is
    /// pending. Its attempt-bound result cannot be reconciled until the real
    /// source I/O fence provides the matching replay completion.
    pub(crate) fn during_read(
        context: WorkExecutionContext<'_>,
        reconciliation_node: &WorkNodeId,
        replay: &WeightingReplaySummary,

        storage: NormalStoragePlan,
    ) -> Result<Self, CompleteDataOperatorError> {
        if context.node().kind != WorkKind::ObservationRead {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(Self {
            binding: CompleteDataExecutionBinding {
                problem: context.compiled().problem_id(),
                attempt: context.attempt_id(),
                replay_node: context.node().id.clone(),
                reconciliation_node: reconciliation_node.clone(),
                lease_epoch: context.lease_epoch(),
                observation_predecessor_required: true,
            },
            replay: replay.clone(),

            storage,
            fold: None,
        })
    }
    /// Validate the caller-bound source and execution once, before any worker
    /// enters the pool. Completed bands carry only owned, context-free data.
    #[cfg(test)]
    pub(crate) fn new(
        context: WorkExecutionContext<'_>,
        reconciliation_node: &WorkNodeId,
        replay: &WeightingReplayCompletion,
        storage: NormalStoragePlan,
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
            || predecessor.delivered_sample_count() != replay.delivered_source_sample_count()
        {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(Self {
            binding: CompleteDataExecutionBinding {
                problem: replay.problem_id(),
                attempt: replay.attempt_id(),
                replay_node: replay.owner_node().clone(),
                reconciliation_node: reconciliation_node.clone(),
                lease_epoch: replay.lease_epoch(),
                observation_predecessor_required: true,
            },
            replay: replay.reconstruction_summary().clone(),

            storage,
            fold: None,
        })
    }

    pub(crate) fn append(
        &mut self,
        specification: &SpectralOperatorSpecification,
        primitives: SpectralOperatorPrimitives,
    ) -> Result<(), CompleteDataOperatorError> {
        let evidence =
            CompleteDataOwnerResult::from_streaming_cube(specification, primitives, &self.replay)?;
        if evidence.completion().problem_id() != self.binding.problem {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        let next = CompleteDataSlabResult {
            evidence,
            binding: self.binding.clone(),
        };
        self.fold = Some(match self.fold.take() {
            None => next.begin_fold(&self.storage)?,
            Some(prefix) => prefix.fold(next)?,
        });
        Ok(())
    }

    pub(crate) fn complete(
        self,
        replay: &WeightingReplayCompletion,
    ) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        self.fold
            .ok_or(CompleteDataOperatorError::ExecutionBinding)?
            .complete(replay)
    }
}
