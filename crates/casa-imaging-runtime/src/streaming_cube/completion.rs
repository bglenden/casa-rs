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

    pub(crate) fn append(
        &mut self,
        residual: CubeResidual,
    ) -> Result<(), CompleteDataOperatorError> {
        self.evidence.append(residual)?;
        Ok(())
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
