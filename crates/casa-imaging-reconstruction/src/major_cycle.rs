// SPDX-License-Identifier: LGPL-3.0-or-later

//! Major-Cycle reconciliation joining complete-data evidence with the model lifecycle.
//!
//! The Major Cycle is the sole operation that creates authoritative
//! residual/normal state. One reconciliation consumes exhaustive T19
//! complete-data operator evidence, one frozen T18 weighting generation, and
//! the exact named T28 input model generation, reconciles the exact final
//! model through the declared normal-operator composition, and returns one
//! inseparable result containing the authoritative Final Normal State
//! completion (with its exact model-dependent residual content), the Final
//! Model completion, and the authoritative final model generation itself. The
//! three members are never separable outside this operation, and none is a
//! Product Generation seal or publication authority.

use std::fmt;

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblemId, LogicalIdentity, NumericsContractId,
    SelectedObservationGenerationId, WeightingCommitmentId,
};

use crate::{
    ContinuumPrimitiveCatalog, Encoder, FINAL_NORMAL_STATE_DOMAIN, FINAL_NORMAL_STATE_VERSION,
    FinalModelCompletion, FinalModelCompletionId, FinalModelContinuation,
    FinalNormalStateCompletionId, MAJOR_CYCLE_DOMAIN, MAJOR_CYCLE_VERSION, MajorCycleCompletionId,
    ModelDelta, ModelGeneration, ModelGenerationId, ModelLifecycle, ModelLifecycleError,
    PreparedFinalModel, SerialMfsError, SerialMfsPrimitives, WeightingGenerationId,
    WeightingReplayCoverageId, WeightingReplayId, runtime_adapter::CompleteDataOwnerResult,
};

/// Versioned Normal State Generation catalog minted by a Major Cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalStateCatalog {
    /// Unnormalized single-field Stokes-I constant-basis MFS normal state
    /// whose residual follows the exact paired `A* W (d - A x)` composition.
    UnnormalizedNterms1V1,
}

/// Reconstruction-owned proof that the final Normal State generation exists.
///
/// The record names exact lineage, the authoritative observation generation,
/// and the content identity of the model-dependent unnormalized residual; it
/// makes no promise that the state is fully resident, dense, or
/// shift-invariant. It is not a Product Graph artifact and mints no
/// publication authority.
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
    selected_generation: SelectedObservationGenerationId,
    primitives: SerialMfsPrimitives,
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

    /// Return the owner-derived content identity of the exact residual evidence.
    ///
    /// The identity covers the model-dependent unnormalized normal state, so a
    /// nonzero final model never shares content with an empty-model state.
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

    /// Return the exact T17 observation generation behind every weighted sample.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.selected_generation
    }

    /// Return the authoritative model-dependent residual plane.
    #[must_use]
    pub const fn residual(&self) -> &[num_complex::Complex64] {
        self.primitives.dirty()
    }

    /// Return the exact unnormalized plane shape of every primitive.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.primitives.shape()
    }

    /// Return the T19 normal approximation paired with the residual.
    #[must_use]
    pub const fn normal_approximation(&self) -> &[num_complex::Complex64] {
        self.primitives.psf()
    }

    /// Return sensitivity state in unnormalized normal-state units.
    #[must_use]
    pub const fn sensitivity(&self) -> &[f64] {
        self.primitives.sensitivity()
    }

    /// Return the exact accumulated sum weight.
    #[must_use]
    pub const fn sum_weight(&self) -> f64 {
        self.primitives.sum_weight()
    }
}

/// Inseparable result of the one atomic Major-Cycle reconciliation.
///
/// The triple cannot be constructed, cloned, or paired by assembly outside
/// this operation: every successful reconciliation carries exactly one Final
/// Normal State completion, exactly one Final Model completion, and the exact
/// authoritative final model generation those completions name. None of the
/// members is a Product Generation seal. The only way to obtain members is to
/// consume a whole minted join via [`MajorCycleCompletion::into_parts`].
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
    final_model: ModelGeneration,
}

/// Validated final-model candidate retained across the exhaustive T18/T19
/// replay without consuming final-model completion authority.
#[doc(hidden)]
#[derive(Debug)]
pub struct MajorCyclePreparation {
    model: PreparedFinalModel,
}

impl MajorCyclePreparation {
    /// Validate and prepare the exact final model before complete-data replay.
    pub fn prepare(
        lifecycle: &ModelLifecycle,
        named: ModelGeneration,
        delta: Option<ModelDelta>,
    ) -> Result<Self, MajorCycleError> {
        Ok(Self {
            model: lifecycle.prepare_final_model(named, delta)?,
        })
    }

    /// Borrow the exact candidate generation for paired-operator prediction.
    #[must_use]
    pub const fn final_model(&self) -> &ModelGeneration {
        self.model.generation()
    }

    /// Return the exact candidate generation identity.
    #[must_use]
    pub const fn final_model_generation(&self) -> ModelGenerationId {
        self.model.generation_id()
    }
}

impl MajorCycleCompletion {
    /// Return the reconciliation identity binding all typed members.
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

    /// Borrow the authoritative final model generation.
    ///
    /// Downstream product authority consumes this exact generation rather than
    /// reconstructing a model from identifiers.
    #[must_use]
    pub const fn final_model(&self) -> &ModelGeneration {
        &self.final_model
    }

    /// Release the three typed members by consuming the whole join.
    ///
    /// Members can never be assembled or paired from parts; releasing them is
    /// reserved for downstream owners (the next Major-Cycle round and the
    /// product authority) that consume the entire minted result at once.
    #[must_use]
    pub fn into_parts(self) -> (FinalNormalState, FinalModelCompletion, ModelGeneration) {
        (self.normal_state, self.model_completion, self.final_model)
    }

    /// Consume the whole reconciliation into its normal state and the affine
    /// model continuation required by a following Minor/Major Cycle pair.
    #[doc(hidden)]
    #[must_use]
    pub fn into_continuation(self) -> (FinalNormalState, FinalModelContinuation) {
        (
            self.normal_state,
            FinalModelContinuation {
                completion: self.model_completion,
                generation: self.final_model,
            },
        )
    }
}

/// Reconstruction owner of one attempt's Major-Cycle reconciliation.
///
/// The owner is affine and derived only by consuming one owner-minted T19
/// result, which stays inseparably paired inside it; it cannot be forged from
/// raw fields or caller digests and is consumed by its single `reconcile`
/// call.
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
    selected_generation: SelectedObservationGenerationId,
    sample_count: u64,
    block_count: u64,
    primitives: SerialMfsPrimitives,
    preparation: MajorCyclePreparation,
}

impl MajorCycleOwner {
    /// Derive the reconciliation owner from one owner-minted T19 result.
    ///
    /// The result is consumed whole, so its completion metadata and primitives
    /// can never be paired with foreign partners afterwards.
    ///
    /// # Errors
    ///
    /// Rejects evidence whose replay did not prove exhaustive coverage.
    pub fn from_complete_data(
        result: CompleteDataOwnerResult,
        preparation: MajorCyclePreparation,
    ) -> Result<Self, MajorCycleError> {
        if result.completion().sample_count() == 0 || result.completion().block_count() == 0 {
            return Err(MajorCycleError::IncompleteCoverage);
        }
        let (primitives, completion) = result.into_parts();
        let primitives = primitives
            .promote_major_cycle_residual(preparation.final_model_generation())
            .map_err(MajorCycleError::Residual)?;
        Ok(Self {
            problem: completion.problem_id(),
            geometry: completion.geometry_id(),
            numerics: completion.numerics_id(),
            weighting_commitment: completion.weighting_commitment_id(),
            weighting_generation: completion.weighting_generation(),
            replay: completion.replay_id(),
            coverage: completion.coverage(),
            catalog: completion.primitive_catalog(),
            selected_generation: completion.selected_generation(),
            sample_count: completion.sample_count(),
            block_count: completion.block_count(),
            primitives,
            preparation,
        })
    }

    /// Return the frozen T18 weighting generation this owner reconciles against.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Return the exact compiled problem behind the retained T19 evidence.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the authoritative T17 observation generation of the retained evidence.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.selected_generation
    }

    /// Return the exhaustive selected-sample count of the retained evidence.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Perform the one atomic Major-Cycle reconciliation.
    ///
    /// The named T28 input model generation is validated through its lifecycle
    /// owner, any pending Model Delta is applied only through that same owner,
    /// and the exact final model is then reconciled through the declared v1
    /// normal-operator composition before either typed record is minted. The
    /// lifecycle's final-completion authority is consumed only after every
    /// pre-check succeeds; a rejected update fails atomically leaving both
    /// authorities intact.
    ///
    /// # Errors
    ///
    /// Stale problem evidence, foreign generations or deltas, non-exhaustive
    /// coverage, and generated-nonfinite residuals all fail closed with no
    /// partial record.
    pub fn reconcile(
        self,
        lifecycle: &mut ModelLifecycle,
    ) -> Result<MajorCycleCompletion, MajorCycleError> {
        if lifecycle.problem() != self.problem {
            return Err(MajorCycleError::StaleModelEvidence);
        }
        let update = lifecycle.commit_final_model(self.preparation.model)?;
        let (final_model, model_completion) = update.into_parts();
        let input_model_generation = model_completion.base();
        let final_model_generation = model_completion.generation();
        let content = self.primitives.normal_state_content_identity();
        let authority = lifecycle.authority();
        let attempt = lifecycle.attempt();
        let epoch = lifecycle.epoch();
        let normal_state = FinalNormalState {
            completion_id: final_normal_state_id(
                authority,
                attempt,
                epoch,
                self.weighting_generation,
                self.replay,
                self.coverage,
                content,
                input_model_generation,
                final_model_generation,
                self.selected_generation,
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
            selected_generation: self.selected_generation,
            primitives: self.primitives,
        };
        let completion_id = major_cycle_completion_id(
            authority,
            attempt,
            epoch,
            normal_state.completion_id(),
            model_completion.completion_id(),
        );
        debug_assert_ne!(
            normal_state.completion_id().as_bytes(),
            model_completion.completion_id().as_bytes()
        );
        Ok(MajorCycleCompletion {
            completion_id,
            normal_state,
            model_completion,
            final_model,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn final_normal_state_id(
    authority: LogicalIdentity,
    attempt: casa_imaging_model::ModelExecutionAttemptId,
    epoch: u64,
    weighting_generation: WeightingGenerationId,
    replay: WeightingReplayId,
    coverage: WeightingReplayCoverageId,
    content: LogicalIdentity,
    input_model_generation: ModelGenerationId,
    final_model_generation: ModelGenerationId,
    selected_generation: SelectedObservationGenerationId,
) -> FinalNormalStateCompletionId {
    let mut encoder = Encoder::new(FINAL_NORMAL_STATE_DOMAIN, FINAL_NORMAL_STATE_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(weighting_generation.as_bytes());
    encoder.identity(replay.as_bytes());
    encoder.identity(coverage.as_bytes());
    encoder.identity(content.as_bytes());
    encoder.identity(input_model_generation.as_bytes());
    encoder.identity(final_model_generation.as_bytes());
    encoder.identity(selected_generation.as_bytes());
    FinalNormalStateCompletionId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn major_cycle_completion_id(
    authority: LogicalIdentity,
    attempt: casa_imaging_model::ModelExecutionAttemptId,
    epoch: u64,
    normal_state: FinalNormalStateCompletionId,
    model: FinalModelCompletionId,
) -> MajorCycleCompletionId {
    let mut encoder = Encoder::new(MAJOR_CYCLE_DOMAIN, MAJOR_CYCLE_VERSION);
    encoder.identity(authority.as_bytes());
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
    /// Reconciling the final model produced or consumed invalid numbers.
    Residual(SerialMfsError),
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
            Self::Residual(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for MajorCycleError {}

impl From<ModelLifecycleError> for MajorCycleError {
    fn from(error: ModelLifecycleError) -> Self {
        Self::Model(error)
    }
}
