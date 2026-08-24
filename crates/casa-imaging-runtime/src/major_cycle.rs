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
    MajorCycleError, MajorCycleOwner, ModelDelta, ModelGeneration, ModelLifecycle,
};

use crate::{
    CompleteDataOperatorResult, ExecutionAttemptId, WorkExecutionContext, WorkKind, WorkNodeId,
};

/// Runtime-bound owner of one attempt's pending Major-Cycle reconciliation.
///
/// Created only from owner-minted T19 complete-data output; consumed exactly
/// once by [`MajorCycleOperatorState::reconcile`] at the planned
/// final-reconciliation node.
#[derive(Debug)]
pub struct MajorCycleOperatorState {
    owner: MajorCycleOwner,
    result: CompleteDataOperatorResult,
}

impl MajorCycleOperatorState {
    /// Retain the T19 operator output as the sole input to reconciliation.
    pub fn begin(result: CompleteDataOperatorResult) -> Result<Self, MajorCycleOperatorError> {
        if result.completion().sample_count() == 0 || result.completion().block_count() == 0 {
            return Err(MajorCycleOperatorError::IncompleteCoverage);
        }
        let owner = MajorCycleOwner::from_complete_data(result.completion().owner_completion())?;
        Ok(Self { owner, result })
    }

    /// Perform the one atomic Major-Cycle reconciliation at the planned node.
    ///
    /// The context must be the exact execution attempt, lease epoch, compiled
    /// problem, and final-reconciliation work node behind the retained T19
    /// evidence, and the model lifecycle must be bound to that same canonical
    /// attempt identity and lease epoch. Any mismatch fails atomically before
    /// the reconstruction owner mints either typed completion record.
    pub fn reconcile(
        self,
        context: WorkExecutionContext<'_>,
        reconciliation_node: &WorkNodeId,
        lifecycle: &mut ModelLifecycle,
        named: ModelGeneration,
        delta: Option<ModelDelta>,
    ) -> Result<MajorCycleOperatorResult, MajorCycleOperatorError> {
        let completion = self.result.completion();
        if context.node().id != *reconciliation_node
            || context.node().kind != WorkKind::Compute
        {
            return Err(MajorCycleOperatorError::WrongExecutionNode);
        }
        if context.attempt_id() != completion.attempt_id()
            || context.lease_epoch() != completion.lease_epoch()
        {
            return Err(MajorCycleOperatorError::ExecutionBinding);
        }
        let predecessor = context
            .predecessor_observation_completion(completion.replay_node())
            .ok_or(MajorCycleOperatorError::MissingReplayPredecessor)?;
        if predecessor.attempt_id() != completion.attempt_id()
            || predecessor.owner_node() != completion.replay_node()
            || predecessor.lease_epoch() != completion.lease_epoch()
            || predecessor.owner_completion().sample_count() != completion.sample_count()
        {
            return Err(MajorCycleOperatorError::ExecutionBinding);
        }
        if lifecycle.problem() != completion.problem_id()
            || context.compiled().problem_id() != completion.problem_id()
        {
            return Err(MajorCycleOperatorError::StaleProblemEvidence);
        }
        let expected_attempt = ModelExecutionAttemptId::new(LogicalIdentity::from_sha256(
            context.attempt_id().as_bytes(),
        ));
        if lifecycle.attempt() != expected_attempt || lifecycle.epoch() != context.lease_epoch() {
            return Err(MajorCycleOperatorError::ModelAttemptBinding);
        }
        let Self { owner, result } = self;
        let completion =
            owner.reconcile(lifecycle, named, delta, result.primitives())?;
        Ok(MajorCycleOperatorResult {
            completion,
            attempt: context.attempt_id(),
            node: context.node().id.clone(),
            lease_epoch: context.lease_epoch(),
        })
    }
}

/// Inseparable runtime envelope around one completed Major-Cycle reconciliation.
///
/// The envelope is minted only by [`MajorCycleOperatorState::reconcile`] at
/// its planned node; a caller cannot construct or forge it:
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
}

/// Exact reason the runtime rejected a Major-Cycle reconciliation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MajorCycleOperatorError {
    /// Reconciliation was attempted outside its planned Compute reconciliation node.
    WrongExecutionNode,
    /// The calling context does not match the attempt or lease that ran T19.
    ExecutionBinding,
    /// The planned node does not follow the settled T19 replay completion.
    MissingReplayPredecessor,
    /// The compiled problem changed after T19 completed.
    StaleProblemEvidence,
    /// The model lifecycle is not bound to this attempt's canonical identity and epoch.
    ModelAttemptBinding,
    /// The retained T19 evidence lacks exhaustive weighted coverage.
    IncompleteCoverage,
    /// The reconstruction owner rejected the scientific join.
    Owner(MajorCycleError),
}

impl fmt::Display for MajorCycleOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongExecutionNode => formatter.write_str(
                "T20 can reconcile only at its planned Compute reconciliation node",
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
            Self::ModelAttemptBinding => formatter.write_str(
                "model lifecycle is not bound to the executing attempt and lease epoch",
            ),
            Self::IncompleteCoverage => formatter.write_str("T19 evidence lacks exhaustive coverage"),
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
