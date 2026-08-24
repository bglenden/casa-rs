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
    CompiledGeometryId, CompiledProblemId, LogicalIdentity, ModelSupport, NumericsContractId,
    SelectedObservationGenerationId, WeightingCommitmentId,
};
use num_complex::Complex64;

use crate::{
    ContinuumPrimitiveCatalog, Encoder, FINAL_NORMAL_STATE_DOMAIN, FINAL_NORMAL_STATE_VERSION,
    FinalModelCompletion, FinalModelCompletionId, FinalNormalStateCompletionId, MAJOR_CYCLE_DOMAIN,
    MAJOR_CYCLE_VERSION, MajorCycleCompletionId, ModelDelta, ModelGeneration, ModelGenerationId,
    ModelLifecycle, ModelLifecycleError, SerialMfsError, SerialMfsPrimitives,
    WeightingGenerationId, WeightingReplayCoverageId, WeightingReplayId,
    runtime_adapter::CompleteDataOwnerResult,
};

/// Versioned Normal State Generation catalog minted by a Major Cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalStateCatalog {
    /// Unnormalized single-field Stokes-I constant-basis MFS normal state
    /// whose residual follows the v1 `g(x) = b - psf (*) x` composition.
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
}

/// Inseparable result of the one atomic Major-Cycle reconciliation.
///
/// The triple cannot be constructed, cloned, or split outside this operation:
/// every successful reconciliation carries exactly one Final Normal State
/// completion, exactly one Final Model completion, and the exact authoritative
/// final model generation those completions name. None of the members is a
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
    final_model: ModelGeneration,
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
    pub fn from_complete_data(result: CompleteDataOwnerResult) -> Result<Self, MajorCycleError> {
        if result.completion().sample_count() == 0 || result.completion().block_count() == 0 {
            return Err(MajorCycleError::IncompleteCoverage);
        }
        let (primitives, completion) = result.into_parts();
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
    /// coverage, unsupported model spaces, and generated-nonfinite residuals
    /// all fail closed with no partial record.
    pub fn reconcile(
        mut self,
        lifecycle: &mut ModelLifecycle,
        named: ModelGeneration,
        delta: Option<ModelDelta>,
    ) -> Result<MajorCycleCompletion, MajorCycleError> {
        if lifecycle.problem() != self.problem {
            return Err(MajorCycleError::StaleModelEvidence);
        }
        let update = match delta {
            Some(delta) => lifecycle.apply_final_delta(named, delta)?,
            None => lifecycle.confirm_final_model(named)?,
        };
        let (final_model, model_completion) = update.into_parts();
        let input_model_generation = model_completion.base();
        let final_model_generation = model_completion.generation();
        let model_plane = major_cycle_model_plane(&final_model, self.primitives.shape())?;
        self.primitives
            .apply_major_cycle_residual(&model_plane)
            .map_err(MajorCycleError::Residual)?;
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

/// Flatten one final generation into the operator-plane order of the
/// unnormalized primitives.
///
/// Lifecycle samples live in canonical target order (`y * width + x` per
/// domain plane); the primitive planes index `[x * height + y]`. Only valid
/// support contributes; invalid-support cells contribute zero.
fn major_cycle_model_plane(
    generation: &ModelGeneration,
    primitive_shape: [usize; 2],
) -> Result<Vec<Complex64>, MajorCycleError> {
    let shape = generation.shape();
    if shape.domains().len() != 1
        || shape.coefficients() != 1
        || shape.polarizations() != 1
        || shape.domains()[0].pixels() != primitive_shape
        || generation.samples().len() != primitive_shape[0] * primitive_shape[1]
    {
        return Err(MajorCycleError::UnsupportedModelSpace);
    }
    let height = primitive_shape[1];
    let mut plane = vec![Complex64::default(); generation.samples().len()];
    for (flat, sample) in generation.samples().iter().enumerate() {
        let value = match sample.support() {
            ModelSupport::Valid => sample.value().value(),
            ModelSupport::Invalid => continue,
        };
        let y = flat / primitive_shape[0];
        let x = flat % primitive_shape[0];
        plane[x * height + y] = Complex64::new(value, 0.0);
    }
    Ok(plane)
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
    /// The final generation does not describe the reconciled primitive plane.
    UnsupportedModelSpace,
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
            Self::UnsupportedModelSpace => formatter
                .write_str("final model space does not describe the reconciled primitive plane"),
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
