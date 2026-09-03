// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime transport for the Major-Cycle reconciliation envelope.
//!
//! The runtime owns plan placement, capabilities, resource accounting, and
//! attempt/node/lease binding for the reconciliation, and transports the
//! closed completion envelope. It never reinterprets scientific roles or
//! scans generic digests: the join itself is minted by the reconstruction
//! owner from T19 evidence and the T28 model lifecycle.

use std::{error::Error, fmt};

use casa_imaging_model::{LogicalIdentity, ModelExecutionAttemptId};
use casa_imaging_reconstruction::{
    MajorCycleError, MajorCycleOwner, MajorCyclePreparation, ModelLifecycle,
};

use crate::{
    CompleteDataOperatorResult, ExecutionAttemptId, FenceId, FenceKind, WorkDependency,
    WorkExecutionContext, WorkKind, WorkNodeId,
};

/// Runtime-bound owner of one attempt's pending Major-Cycle reconciliation.
///
/// Created only from owner-minted T19 complete-data output; consumed exactly
/// once by [`MajorCycleOperatorState::reconcile`] at the plan-authoritative
/// final-reconciliation node bound when the T19 plan fragment was composed.
#[derive(Debug)]
pub struct MajorCycleOperatorState {
    owner: MajorCycleOwner,
    attempt: ExecutionAttemptId,
    replay_node: WorkNodeId,
    reconciliation_node: WorkNodeId,
    lease_epoch: u64,
    observation_predecessor_required: bool,
}

impl MajorCycleOperatorState {
    /// Retain the T19 operator output as the sole input to reconciliation.
    ///
    /// The reconstruction evidence stays inseparably paired inside the owner,
    /// and the plan-authoritative reconciliation node is retained beside it;
    /// [`Self::reconcile`] accepts no substitute node, evidence, or context.
    pub fn begin(
        result: CompleteDataOperatorResult,
        preparation: MajorCyclePreparation,
    ) -> Result<Self, MajorCycleOperatorError> {
        let state = Self {
            attempt: result.attempt_id(),
            replay_node: result.replay_node().clone(),
            reconciliation_node: result.reconciliation_node().clone(),
            lease_epoch: result.lease_epoch(),
            observation_predecessor_required: result.observation_predecessor_required(),
            owner: MajorCycleOwner::from_complete_data(result.into_evidence(), preparation)?,
        };
        Ok(state)
    }

    /// Perform the one atomic Major-Cycle reconciliation.
    ///
    /// The context must be the exact plan-authoritative final-reconciliation
    /// Compute node behind the sealed plan, and the exact execution attempt and
    /// lease epoch that ran T19. The settled replay-predecessor evidence must
    /// carry the same attempt, node, lease epoch, exhaustive sample count, and
    /// authoritative T17 observation generation as the retained T19 evidence,
    /// and the model lifecycle must be bound to that same canonical attempt
    /// identity and lease epoch. Any mismatch fails atomically before the
    /// reconstruction owner mints any typed completion record.
    ///
    /// Stale weighting evidence cannot reach this seam: the frozen T18
    /// generation, replay identity, and coverage were bound into the T19
    /// completion when it was minted from the terminal replay proof, and the
    /// predecessor check below rebinds that same settled completion to this
    /// node. A cancelled run never dispatches its reconciliation work or drains
    /// past the commit gate, so an envelope minted here is discarded with its
    /// attempt and nothing becomes visible through publication.
    pub fn reconcile(
        self,
        context: WorkExecutionContext<'_>,
        lifecycle: &mut ModelLifecycle,
    ) -> Result<MajorCycleOperatorResult, MajorCycleOperatorError> {
        if context.node().id != self.reconciliation_node || context.node().kind != WorkKind::Compute
        {
            return Err(MajorCycleOperatorError::WrongExecutionNode);
        }
        if context.attempt_id() != self.attempt || context.lease_epoch() != self.lease_epoch {
            return Err(MajorCycleOperatorError::ExecutionBinding);
        }
        if self.observation_predecessor_required {
            let predecessor = context
                .predecessor_observation_completion(&self.replay_node)
                .ok_or(MajorCycleOperatorError::MissingReplayPredecessor)?;
            if predecessor.attempt_id() != self.attempt
                || predecessor.owner_node() != &self.replay_node
                || predecessor.lease_epoch() != self.lease_epoch
                || predecessor.owner_completion().generation_id()
                    != self.owner.selected_generation()
                || predecessor.owner_completion().sample_count() != self.owner.sample_count()
            {
                return Err(MajorCycleOperatorError::ExecutionBinding);
            }
        } else if !context
            .node()
            .dependencies
            .contains(&WorkDependency::Fence(FenceId::new(
                self.replay_node.clone(),
                FenceKind::Io,
            )))
        {
            return Err(MajorCycleOperatorError::MissingReplayPredecessor);
        }
        if lifecycle.problem() != self.owner.problem_id()
            || context.compiled().problem_id() != self.owner.problem_id()
        {
            return Err(MajorCycleOperatorError::StaleProblemEvidence);
        }
        let expected_attempt = ModelExecutionAttemptId::new(LogicalIdentity::from_sha256(
            context.attempt_id().as_bytes(),
        ));
        if lifecycle.attempt() != expected_attempt || lifecycle.epoch() != context.lease_epoch() {
            return Err(MajorCycleOperatorError::ModelAttemptBinding);
        }
        let MajorCycleOperatorState {
            owner,
            attempt,
            reconciliation_node,
            lease_epoch,
            ..
        } = self;
        let completion = owner.reconcile(lifecycle)?;
        Ok(MajorCycleOperatorResult {
            completion,
            attempt,
            node: reconciliation_node,
            lease_epoch,
        })
    }
}

/// Inseparable runtime envelope around one completed Major-Cycle reconciliation.
///
/// The envelope is minted only by [`MajorCycleOperatorState::reconcile`] at
/// its plan-authoritative node; a caller cannot construct or forge it:
///
/// ```compile_fail
/// use casa_imaging_runtime::MajorCycleOperatorResult;
///
/// let _ = MajorCycleOperatorResult {};
/// ```
#[derive(Debug)]
pub struct MajorCycleOperatorResult {
    completion: casa_imaging_reconstruction::MajorCycleCompletion,
    attempt: ExecutionAttemptId,
    node: WorkNodeId,
    lease_epoch: u64,
}

impl MajorCycleOperatorResult {
    /// Borrow the inseparable pair of distinct typed completions.
    #[must_use]
    pub const fn completion(&self) -> &casa_imaging_reconstruction::MajorCycleCompletion {
        &self.completion
    }

    /// Return the execution attempt whose settled fences authorized the join.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.attempt
    }

    /// Return the final-reconciliation node that executed the join.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Return the Resource Authority lease epoch held through the join.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Consume the runtime envelope into the reconstruction-owned completion.
    #[must_use]
    pub fn into_completion(self) -> casa_imaging_reconstruction::MajorCycleCompletion {
        self.completion
    }
}

/// Exact reason the runtime rejected a Major-Cycle reconciliation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MajorCycleOperatorError {
    /// Reconciliation was attempted outside its plan-authoritative Compute node.
    WrongExecutionNode,
    /// The calling context does not match the attempt or lease that ran T19.
    ExecutionBinding,
    /// The planned node does not follow the settled T19 replay completion.
    MissingReplayPredecessor,
    /// The compiled problem changed after T19 completed.
    StaleProblemEvidence,
    /// The model lifecycle is not bound to this attempt's canonical identity and epoch.
    ModelAttemptBinding,
    /// The reconstruction owner rejected the scientific join.
    Owner(MajorCycleError),
}

impl fmt::Display for MajorCycleOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongExecutionNode => formatter.write_str(
                "T20 can reconcile only at its plan-authoritative Compute reconciliation node",
            ),
            Self::ExecutionBinding => {
                formatter.write_str("reconciliation context changed the T19 execution authority")
            }
            Self::MissingReplayPredecessor => {
                formatter.write_str("reconciliation does not follow its T19 replay completion")
            }
            Self::StaleProblemEvidence => {
                formatter.write_str("compiled problem differs from the reconciled T19 evidence")
            }
            Self::ModelAttemptBinding => formatter
                .write_str("model lifecycle is not bound to the executing attempt and lease epoch"),
            Self::Owner(error) => error.fmt(formatter),
        }
    }
}

impl Error for MajorCycleOperatorError {}

impl From<MajorCycleError> for MajorCycleOperatorError {
    fn from(error: MajorCycleError) -> Self {
        Self::Owner(error)
    }
}
