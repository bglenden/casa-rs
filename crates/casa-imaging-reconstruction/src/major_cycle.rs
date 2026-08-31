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
    CompiledGeometryId, CompiledProblemId, ContinuumTransformGenerationId, LogicalIdentity,
    NumericsContractId, SelectedObservationGenerationId, WeightingCommitmentId,
};

use crate::{
    Encoder, FINAL_NORMAL_STATE_DOMAIN, FINAL_NORMAL_STATE_VERSION, FinalModelCompletion,
    FinalModelCompletionId, FinalModelContinuation, FinalNormalStateCompletionId,
    MAJOR_CYCLE_DOMAIN, MAJOR_CYCLE_VERSION, MajorCycleCompletionId, ModelDelta, ModelGeneration,
    ModelGenerationId, ModelLifecycle, ModelLifecycleError, PreparedFinalModel,
    SpectralOperatorError, SpectralOperatorPrimitives, SpectralPrimitiveCatalog,
    WeightingGenerationId, WeightingReplayCoverageId, WeightingReplayId,
    runtime_adapter::CompleteDataOwnerResult, spectral_operator::ReusableNormalState,
};

/// Versioned Normal State Generation catalog minted by a Major Cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalStateCatalog {
    /// Unnormalized single-field Stokes-I constant-basis MFS normal state
    /// whose residual follows the exact paired `A* W (d - A x)` composition.
    UnnormalizedPlaneV1,
    /// Unnormalized channel-major normal-state slab whose planes follow the
    /// exact output-channel interval named by the paired spectral operator.
    UnnormalizedChannelSlabV1,
    /// Unnormalized Taylor residual terms and `2T-1` signed block-normal moments.
    UnnormalizedTaylorBlockV1,
    /// Unnormalized joint continuum-line residual terms and full dense block normal state.
    UnnormalizedJointBlockV1,
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
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    coupled_mask_generation: Option<crate::ReconstructionMaskGenerationId>,
    primitives: SpectralOperatorPrimitives,
}

impl FinalNormalState {
    pub(crate) fn into_reusable(self) -> ReusableNormalState {
        ReusableNormalState::new(
            self.problem,
            self.geometry,
            self.numerics,
            self.weighting_commitment,
            self.weighting_generation,
            self.selected_generation,
            self.continuum_transform_generation,
            self.primitives,
        )
    }

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

    /// Return the sequential continuum-transform generation, when present.
    #[must_use]
    pub const fn continuum_transform_generation(&self) -> Option<ContinuumTransformGenerationId> {
        self.continuum_transform_generation
    }

    /// Return the immutable coupled spatial-support generation bound to this state.
    #[must_use]
    pub const fn coupled_mask_generation(&self) -> Option<crate::ReconstructionMaskGenerationId> {
        self.coupled_mask_generation
    }

    /// Return the authoritative model-dependent residual plane.
    #[must_use]
    pub const fn residual(&self) -> &[num_complex::Complex64] {
        self.primitives.dirty()
    }

    /// Borrow one channel plane of the common joint-model residual.
    #[must_use]
    pub fn joint_common_residual(
        &self,
        output_channel: usize,
    ) -> Option<&[num_complex::Complex64]> {
        if self.catalog != NormalStateCatalog::UnnormalizedJointBlockV1 {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let start = output_channel.checked_mul(cells)?;
        self.primitives
            .common_residual()?
            .get(start..start.checked_add(cells)?)
    }

    /// Return the exact unnormalized plane shape of every primitive.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.primitives.shape()
    }

    /// Return the exact output-channel slab represented by this state.
    #[must_use]
    pub const fn slab(&self) -> crate::SpectralSlabPlan {
        self.primitives.slab()
    }

    /// Return the number of channel planes resident in this state.
    #[must_use]
    pub const fn channel_count(&self) -> usize {
        self.primitives.slab().core_depth()
    }

    /// Return the number of reconstruction-coefficient residual terms.
    #[must_use]
    pub const fn coefficient_term_count(&self) -> usize {
        self.primitives.coefficient_term_count()
    }

    /// Return the number of retained normal moments.
    #[must_use]
    pub const fn normal_moment_count(&self) -> usize {
        self.primitives.normal_moment_count()
    }

    /// Return the polynomial reference frequency when this is continuum state.
    #[must_use]
    pub const fn reference_frequency_hz(&self) -> Option<f64> {
        self.primitives.reference_frequency_hz()
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
    pub fn sum_weight(&self) -> f64 {
        self.primitives.sum_weight()
    }

    /// Return all channel sum weights in output-channel order.
    #[must_use]
    pub const fn sum_weights(&self) -> &[f64] {
        self.primitives.sum_weights()
    }

    /// Return exact channel-local response weights for a joint common residual.
    #[must_use]
    pub const fn channel_sum_weights(&self) -> &[f64] {
        self.primitives.channel_sum_weights()
    }

    /// Return all channel validity states in output-channel order.
    #[must_use]
    pub const fn channel_validity(&self) -> &[crate::SpectralChannelValidity] {
        self.primitives.channel_validity()
    }

    /// Return the principal support state for a polynomial normal family.
    #[must_use]
    pub fn support_validity(&self) -> Option<crate::SpectralChannelValidity> {
        if !matches!(
            self.catalog,
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            return None;
        }
        self.primitives.channel_validity().first().copied()
    }

    /// Borrow one reconstruction-coefficient residual term.
    #[must_use]
    pub fn coefficient_term(
        &self,
        coefficient: usize,
    ) -> Option<FinalNormalStateCoefficientTerm<'_>> {
        if !matches!(
            self.catalog,
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let start = coefficient.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        Some(FinalNormalStateCoefficientTerm {
            owner: self,
            coefficient,
            residual: self.primitives.dirty().get(start..end)?,
        })
    }

    /// Borrow one retained normal moment.
    #[must_use]
    pub fn normal_moment(&self, moment: usize) -> Option<FinalNormalStateNormalMoment<'_>> {
        if !matches!(
            self.catalog,
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let start = moment.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        Some(FinalNormalStateNormalMoment {
            owner: self,
            moment,
            normal_approximation: self.primitives.psf().get(start..end)?,
            sensitivity: self.primitives.sensitivity().get(start..end)?,
            sum_weight: *self.primitives.sum_weights().get(moment)?,
        })
    }

    /// Borrow the Hankel normal block `H[row,column] = P[row+column]`.
    #[must_use]
    pub fn normal_block(
        &self,
        row: usize,
        column: usize,
    ) -> Option<FinalNormalStateNormalMoment<'_>> {
        let moment = self.primitives.normal_moment_index(row, column)?;
        self.normal_moment(moment)
    }

    /// Return the smooth-coefficient prefix length for a joint normal block.
    #[must_use]
    pub const fn joint_continuum_term_count(&self) -> Option<usize> {
        self.primitives.joint_continuum_term_count()
    }

    /// Borrow one channel plane from this bounded Normal State slab.
    #[must_use]
    pub fn plane(&self, local_channel: usize) -> Option<FinalNormalStatePlane<'_>> {
        if matches!(
            self.catalog,
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            return None;
        }
        let cells = self.shape()[0].checked_mul(self.shape()[1])?;
        let start = local_channel.checked_mul(cells)?;
        let end = start.checked_add(cells)?;
        if local_channel >= self.channel_count() {
            return None;
        }
        Some(FinalNormalStatePlane {
            owner: self,
            local_channel,
            residual: self.primitives.dirty().get(start..end)?,
            psf: self.primitives.psf().get(start..end)?,
            sensitivity: self.primitives.sensitivity().get(start..end)?,
        })
    }
}

/// Borrowed model-dependent residual for one reconstruction coefficient.
#[derive(Debug, Clone, Copy)]
pub struct FinalNormalStateCoefficientTerm<'a> {
    owner: &'a FinalNormalState,
    coefficient: usize,
    residual: &'a [num_complex::Complex64],
}

impl<'a> FinalNormalStateCoefficientTerm<'a> {
    /// Return the state owner.
    #[must_use]
    pub const fn owner(self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the zero-based Taylor coefficient ordinal.
    #[must_use]
    pub const fn coefficient(self) -> usize {
        self.coefficient
    }

    /// Return this coefficient's unnormalized model-dependent residual.
    #[must_use]
    pub const fn residual(self) -> &'a [num_complex::Complex64] {
        self.residual
    }
}

/// Borrowed `P[k]` member of a polynomial block-normal family.
#[derive(Debug, Clone, Copy)]
pub struct FinalNormalStateNormalMoment<'a> {
    owner: &'a FinalNormalState,
    moment: usize,
    normal_approximation: &'a [num_complex::Complex64],
    sensitivity: &'a [f64],
    sum_weight: f64,
}

impl<'a> FinalNormalStateNormalMoment<'a> {
    /// Return the state owner.
    #[must_use]
    pub const fn owner(self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the zero-based polynomial moment ordinal.
    #[must_use]
    pub const fn moment(self) -> usize {
        self.moment
    }

    /// Return this moment's unnormalized PSF approximation.
    #[must_use]
    pub const fn normal_approximation(self) -> &'a [num_complex::Complex64] {
        self.normal_approximation
    }

    /// Return this moment's unnormalized scalar sensitivity.
    #[must_use]
    pub const fn sensitivity(self) -> &'a [f64] {
        self.sensitivity
    }

    /// Return this moment's signed accumulated sum weight.
    #[must_use]
    pub const fn sum_weight(self) -> f64 {
        self.sum_weight
    }
}

/// Borrowed two-dimensional plane of one authoritative Normal State slab.
#[derive(Debug, Clone, Copy)]
pub struct FinalNormalStatePlane<'a> {
    owner: &'a FinalNormalState,
    local_channel: usize,
    residual: &'a [num_complex::Complex64],
    psf: &'a [num_complex::Complex64],
    sensitivity: &'a [f64],
}

impl<'a> FinalNormalStatePlane<'a> {
    /// Return the slab owner this view borrows.
    #[must_use]
    pub const fn owner(self) -> &'a FinalNormalState {
        self.owner
    }

    /// Return the absolute output-channel ordinal.
    #[must_use]
    pub const fn output_channel(self) -> usize {
        self.owner.slab().core_range().start + self.local_channel
    }

    /// Return this plane's model-dependent unnormalized residual.
    #[must_use]
    pub const fn residual(self) -> &'a [num_complex::Complex64] {
        self.residual
    }

    /// Return this plane's unnormalized PSF approximation.
    #[must_use]
    pub const fn normal_approximation(self) -> &'a [num_complex::Complex64] {
        self.psf
    }

    /// Return this plane's per-pixel sensitivity values.
    #[must_use]
    pub const fn sensitivity(self) -> &'a [f64] {
        self.sensitivity
    }

    /// Return this plane's accumulated sum weight.
    #[must_use]
    pub const fn sum_weight(self) -> f64 {
        self.owner.sum_weights()[self.local_channel]
    }

    /// Return the common direction-plane shape.
    #[must_use]
    pub const fn shape(self) -> [usize; 2] {
        self.owner.shape()
    }

    /// Return mapped, blank, or unmapped channel validity.
    #[must_use]
    pub const fn validity(self) -> crate::SpectralChannelValidity {
        self.owner.channel_validity()[self.local_channel]
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
    catalog: SpectralPrimitiveCatalog,
    selected_generation: SelectedObservationGenerationId,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    coupled_mask_generation: Option<crate::ReconstructionMaskGenerationId>,
    sample_count: u64,
    block_count: u64,
    primitives: SpectralOperatorPrimitives,
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
            continuum_transform_generation: completion.continuum_transform_generation(),
            coupled_mask_generation: None,
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

    /// Bind the exact reconstruction supports consumed before this final reconciliation.
    pub fn bind_reconstruction_masks(
        mut self,
        masks: &crate::ReconstructionMaskSet,
    ) -> Result<Self, MajorCycleError> {
        match (self.catalog, masks) {
            (
                SpectralPrimitiveCatalog::UnnormalizedJointBlockV1,
                crate::ReconstructionMaskSet::Coupled(masks),
            ) => self.coupled_mask_generation = Some(masks.generation_id()),
            (SpectralPrimitiveCatalog::UnnormalizedJointBlockV1, _)
            | (_, crate::ReconstructionMaskSet::Coupled(_)) => {
                return Err(MajorCycleError::InvalidReconstructionMaskLineage);
            }
            (_, crate::ReconstructionMaskSet::Shared(_)) => {}
        }
        Ok(self)
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
                self.continuum_transform_generation,
                self.coupled_mask_generation,
            ),
            problem: self.problem,
            geometry: self.geometry,
            numerics: self.numerics,
            weighting_commitment: self.weighting_commitment,
            weighting_generation: self.weighting_generation,
            replay: self.replay,
            coverage: self.coverage,
            catalog: match self.catalog {
                SpectralPrimitiveCatalog::UnnormalizedPlaneV1 => {
                    NormalStateCatalog::UnnormalizedPlaneV1
                }
                SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1 => {
                    NormalStateCatalog::UnnormalizedChannelSlabV1
                }
                SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1 => {
                    NormalStateCatalog::UnnormalizedTaylorBlockV1
                }
                SpectralPrimitiveCatalog::UnnormalizedJointBlockV1 => {
                    NormalStateCatalog::UnnormalizedJointBlockV1
                }
            },
            content,
            sample_count: self.sample_count,
            block_count: self.block_count,
            input_model_generation,
            final_model_generation,
            selected_generation: self.selected_generation,
            continuum_transform_generation: self.continuum_transform_generation,
            coupled_mask_generation: self.coupled_mask_generation,
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
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    coupled_mask_generation: Option<crate::ReconstructionMaskGenerationId>,
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
    match continuum_transform_generation {
        Some(generation) => {
            encoder.u8(1);
            encoder.identity(generation.as_bytes());
        }
        None => encoder.u8(0),
    }
    match coupled_mask_generation {
        Some(generation) => {
            encoder.u8(1);
            encoder.identity(generation.as_bytes());
        }
        None => encoder.u8(0),
    }
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
    Residual(SpectralOperatorError),
    /// The final joint Normal State was not bound to one coupled mask generation.
    InvalidReconstructionMaskLineage,
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
            Self::InvalidReconstructionMaskLineage => {
                formatter.write_str("final joint normal state requires coupled mask lineage")
            }
        }
    }
}

impl std::error::Error for MajorCycleError {}

impl From<ModelLifecycleError> for MajorCycleError {
    fn from(error: ModelLifecycleError) -> Self {
        Self::Model(error)
    }
}
