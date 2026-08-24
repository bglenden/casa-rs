// SPDX-License-Identifier: LGPL-3.0-or-later

//! Major-Cycle reconciliation joining complete-data evidence with the model lifecycle.
//!
//! The Major Cycle is the sole operation that creates authoritative
//! residual/normal state. One reconciliation consumes exhaustive T19
//! complete-data operator evidence, one frozen T18 weighting generation, and
//! the exact named T28 input model generation, and returns one inseparable
//! result containing two distinct opaque typed records: the Final Normal State
//! completion and the Final Model completion. A single untyped record never
//! stands in for both roles, and neither record is a Product Generation seal
//! or publication authority.

use std::fmt;

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblemId, LogicalIdentity, NumericsContractId,
    WeightingCommitmentId,
};

use crate::{
    AuthoritySeal, ContinuumPrimitiveCatalog, Encoder, FinalModelCompletion,
    FinalModelCompletionId, FinalNormalStateCompletionId, MajorCycleCompletionId, ModelDelta,
    ModelGeneration, ModelGenerationId, ModelLifecycle, ModelLifecycleError, SerialMfsPrimitives,
    WeightingGenerationId, WeightingReplayCoverageId, WeightingReplayId,
    runtime_adapter::CompleteDataOwnerCompletion, FINAL_NORMAL_STATE_DOMAIN,
    FINAL_NORMAL_STATE_VERSION, MAJOR_CYCLE_DOMAIN, MAJOR_CYCLE_VERSION,
};

/// Versioned Normal State Generation catalog minted by a Major Cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalStateCatalog {
    /// Unnormalized single-field Stokes-I constant-basis MFS normal state.
    UnnormalizedNterms1V1,
}

/// Reconstruction-owned proof that the final Normal State generation exists.
///
/// The record names exact lineage and content identities without promising
/// that the state is fully resident, dense, or shift-invariant; it may be
/// tiled, streamed, sparse, manifest-based, or operator-backed. It is not a
/// Product Graph artifact and mints no publication authority.
///
/// ```compile_fail
/// use casa_imaging_reconstruction::FinalNormalState;
///
/// let _ = FinalNormalState {};
/// ```
#[doc(hidden)]
#[derive(Debug)]
pub struct FinalNormalState {
    completion_id: FinalNormalStateCompletionId,
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
    replay: WeightingReplayId,
    coverage: WeightingReplayCoverageId,
    catalog: NormalStateCatalog,
    content: LogicalIdentity,
    sample_count: u64,
    block_count: u64,
    input_model_generation: ModelGenerationId,
    final_model_generation: ModelGenerationId,
}

impl FinalNormalState {
    /// Return the completion identity.
    #[must_use]
    pub const fn completion_id(&self) -> FinalNormalStateCompletionId {
        self.completion_id
    }

    /// Return the exact Compiled Problem reconciled by this Major Cycle.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the compiled geometry/operator coordinate commitment.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return the exact numerical contract governing the state arithmetic.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the compiler-owned weighting commitment bound to the frozen W.
    #[must_use]
    pub const fn weighting_commitment_id(&self) -> WeightingCommitmentId {
        self.weighting_commitment
    }

    /// Return the frozen T18 weighting generation behind the state.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Return the terminal replay whose exhaustive coverage produced the state.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.replay
    }

    /// Return the exact T18 weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.coverage
    }

    /// Return the versioned Normal State Generation catalog.
    #[must_use]
    pub const fn catalog(&self) -> NormalStateCatalog {
        self.catalog
    }

    /// Return the owner-derived content identity of the exact normal-state evidence.
    #[must_use]
    pub const fn content_identity(&self) -> LogicalIdentity {
        self.content
    }

    /// Return the exhaustive selected-sample count behind the state.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return the exhaustive replay block count behind the state.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Return the exact T28 input model generation this reconciliation named.
    #[must_use]
    pub const fn input_model_generation(&self) -> ModelGenerationId {
        self.input_model_generation
    }

    /// Return the authoritative final model generation of this reconciliation.
    #[must_use]
    pub const fn final_model_generation(&self) -> ModelGenerationId {
        self.final_model_generation
    }
}

/// Inseparable result of the one atomic Major-Cycle reconciliation.
///
/// The pair cannot be constructed, cloned, or split outside this operation:
/// every successful reconciliation carries exactly one Final Normal State
/// completion and exactly one Final Model completion, and neither member is a
/// Product Generation seal.
///
/// A caller cannot forge the join from its parts:
///
/// ```compile_fail
/// use casa_imaging_reconstruction::MajorCycleCompletion;
///
/// let _ = MajorCycleCompletion {};
/// ```
#[derive(Debug)]
pub struct MajorCycleCompletion {
    completion_id: MajorCycleCompletionId,
    normal_state: FinalNormalState,
    model_completion: FinalModelCompletion,
}

impl MajorCycleCompletion {
    /// Return the reconciliation identity binding both typed completions.
    #[must_use]
    pub const fn completion_id(&self) -> MajorCycleCompletionId {
        self.completion_id
    }

    /// Borrow the distinct Final Normal State completion.
    #[must_use]
    pub const fn normal_state(&self) -> &FinalNormalState {
        &self.normal_state
    }

    /// Borrow the distinct Final Model completion.
    #[must_use]
    pub const fn model_completion(&self) -> &FinalModelCompletion {
        &self.model_completion
    }
}

/// Reconstruction owner of one attempt's Major-Cycle reconciliation.
///
/// The owner is affine and derived only from owner-minted T19 evidence; it
/// cannot be forged from raw fields or caller digests and is consumed by its
/// single `reconcile` call.
#[doc(hidden)]
#[derive(Debug)]
pub struct MajorCycleOwner {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
    replay: WeightingReplayId,
    coverage: WeightingReplayCoverageId,
    catalog: ContinuumPrimitiveCatalog,
    content: LogicalIdentity,
    sample_count: u64,
    block_count: u64,
}

impl MajorCycleOwner {
    /// Derive the reconciliation owner from one owner-minted T19 result.
    ///
    /// The exact normal-state content is bound at construction so a later
    /// reconciliation rejects substituted or mutated primitives.
    pub fn from_complete_data(result: &crate::runtime_adapter::CompleteDataOwnerResult) -> Result<Self, MajorCycleError> {
        Self::from_owner_evidence(result.completion(), result.primitives())
    }

    /// Bind one T19 owner completion to its exact primitives.
    pub fn from_owner_evidence(
        completion: &CompleteDataOwnerCompletion,
        primitives: &SerialMfsPrimitives,
    ) -> Result<Self, MajorCycleError> {
        if completion.sample_count() == 0 || completion.block_count() == 0 {
            return Err(MajorCycleError::IncompleteCoverage);
        }
        Ok(Self {
            problem: completion.problem_id(),
            geometry: completion.geometry_id(),
            numerics: completion.numerics_id(),
            weighting_commitment: completion.weighting_commitment_id(),
            weighting_generation: completion.weighting_generation(),
            replay: completion.replay_id(),
            coverage: completion.coverage(),
            catalog: completion.primitive_catalog(),
            content: primitives.normal_state_content_identity(),
            sample_count: completion.sample_count(),
            block_count: completion.block_count(),
        })
    }

    /// Return the frozen T18 weighting generation this owner reconciles against.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Perform the one atomic Major-Cycle reconciliation.
    ///
    /// The named T28 input model generation is validated through its lifecycle
    /// owner, any pending Model Delta is applied only through that same owner,
    /// and the final Normal State completion is minted from the exhaustive T19
    /// primitives. Every check runs before anything is minted, so generation
    /// mismatch, stale problem or weighting evidence, mutation, cancellation,
    /// and incomplete coverage fail atomically with no partial record.
    pub fn reconcile(
        self,
        lifecycle: &mut ModelLifecycle,
        named: ModelGeneration,
        delta: Option<ModelDelta>,
        primitives: &SerialMfsPrimitives,
    ) -> Result<MajorCycleCompletion, MajorCycleError> {
        if lifecycle.problem() != self.problem {
            return Err(MajorCycleError::StaleModelEvidence);
        }
        if primitives.normal_state_content_identity() != self.content {
            return Err(MajorCycleError::MutatedNormalStateEvidence);
        }
        let update = match delta {
            Some(delta) => lifecycle.apply_final_delta(named, delta)?,
            None => lifecycle.confirm_final_model(named)?,
        };
        let input_model_generation = update.completion().base();
        let final_model_generation = update.completion().generation();
        let content = primitives.normal_state_content_identity();
        let seal = lifecycle.seal;
        let normal_state = FinalNormalState {
            completion_id: final_normal_state_id(
                seal,
                self.problem,
                lifecycle.attempt(),
                lifecycle.epoch(),
                self.weighting_generation,
                self.replay,
                self.coverage,
                content,
                input_model_generation,
                final_model_generation,
            ),
            problem: self.problem,
            geometry: self.geometry,
            numerics: self.numerics,
            weighting_commitment: self.weighting_commitment,
            weighting_generation: self.weighting_generation,
            replay: self.replay,
            coverage: self.coverage,
            catalog: match self.catalog {
                ContinuumPrimitiveCatalog::UnnormalizedNterms1V1 => {
                    NormalStateCatalog::UnnormalizedNterms1V1
                }
            },
            content,
            sample_count: self.sample_count,
            block_count: self.block_count,
            input_model_generation,
            final_model_generation,
        };
        let completion_id = major_cycle_completion_id(
            seal,
            self.problem,
            lifecycle.attempt(),
            lifecycle.epoch(),
            normal_state.completion_id(),
            update.completion().completion_id(),
        );
        debug_assert_ne!(
            normal_state.completion_id().as_bytes(),
            update.completion().completion_id().as_bytes()
        );
        Ok(MajorCycleCompletion {
            completion_id,
            normal_state,
            model_completion: update.into_parts().1,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn final_normal_state_id(
    seal: AuthoritySeal,
    problem: CompiledProblemId,
    attempt: casa_imaging_model::ModelExecutionAttemptId,
    epoch: u64,
    weighting_generation: WeightingGenerationId,
    replay: WeightingReplayId,
    coverage: WeightingReplayCoverageId,
    content: LogicalIdentity,
    input_model_generation: ModelGenerationId,
    final_model_generation: ModelGenerationId,
) -> FinalNormalStateCompletionId {
    let mut encoder = Encoder::new(FINAL_NORMAL_STATE_DOMAIN, FINAL_NORMAL_STATE_VERSION);
    encoder.u64(seal.0);
    encoder.identity(problem.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(weighting_generation.as_bytes());
    encoder.identity(replay.as_bytes());
    encoder.identity(coverage.as_bytes());
    encoder.identity(content.as_bytes());
    encoder.identity(input_model_generation.as_bytes());
    encoder.identity(final_model_generation.as_bytes());
    FinalNormalStateCompletionId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn major_cycle_completion_id(
    seal: AuthoritySeal,
    problem: CompiledProblemId,
    attempt: casa_imaging_model::ModelExecutionAttemptId,
    epoch: u64,
    normal_state: FinalNormalStateCompletionId,
    model: FinalModelCompletionId,
) -> MajorCycleCompletionId {
    let mut encoder = Encoder::new(MAJOR_CYCLE_DOMAIN, MAJOR_CYCLE_VERSION);
    encoder.u64(seal.0);
    encoder.identity(problem.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(normal_state.as_bytes());
    encoder.identity(model.as_bytes());
    MajorCycleCompletionId(LogicalIdentity::from_sha256(encoder.finish()))
}

/// Exact reason a Major-Cycle reconciliation failed closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MajorCycleError {
    /// The model lifecycle belongs to another compiled problem than the T19 evidence.
    StaleModelEvidence,
    /// The T19 evidence did not prove exhaustive weighted coverage.
    IncompleteCoverage,
    /// The model owner rejected the named generation or pending delta.
    Model(ModelLifecycleError),
    /// The primitives presented at reconciliation differ from the exact
    /// complete-data output bound when this owner was constructed.
    MutatedNormalStateEvidence,
}

impl fmt::Display for MajorCycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleModelEvidence => {
                formatter.write_str("model lifecycle does not bind the reconciled problem")
            }
            Self::IncompleteCoverage => {
                formatter.write_str("complete-data evidence lacks exhaustive coverage")
            }
            Self::Model(error) => error.fmt(formatter),
            Self::MutatedNormalStateEvidence => formatter.write_str(
                "normal-state primitives differ from their bound complete-data evidence",
            ),
        }
    }
}

impl std::error::Error for MajorCycleError {}

impl From<ModelLifecycleError> for MajorCycleError {
    fn from(error: ModelLifecycleError) -> Self {
        Self::Model(error)
    }
}
