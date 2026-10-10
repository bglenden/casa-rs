// SPDX-License-Identifier: LGPL-3.0-or-later

//! Owner of model generations, Model Deltas and affine final-model completion.

use std::sync::atomic::{AtomicU64, Ordering};

use casa_imaging_model::{
    CompiledProblem, LogicalIdentity, ModelCell, ModelContractError, ModelDeltaTerm,
    ModelExecutionAttemptId, ModelLifecycleContract, ModelSample, ModelSourceShape, ModelSupport,
    ModelValue,
};
use thiserror::Error;

use crate::{
    identity::{
        AUTHORITY_DOMAIN, AUTHORITY_VERSION, Encoder, FINAL_COMPLETION_DOMAIN,
        FINAL_COMPLETION_VERSION, FinalModelCompletionId, GENERATION_DOMAIN, GENERATION_VERSION,
        ModelDeltaId, ModelGenerationId, NEXT_MODEL_DELTA,
    },
    model_storage::{ModelSampleUpdate, ModelSamples, ModelStoragePlan},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AuthoritySeal(u64);

#[derive(Debug)]
struct FinalAuthority(AuthoritySeal);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContinuationAuthority {
    seal: AuthoritySeal,
    generation: ModelGenerationId,
}

static NEXT_AUTHORITY_SEAL: AtomicU64 = AtomicU64::new(1);

/// Owner-recorded origin of one named model generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelGenerationOrigin {
    /// The lifecycle's empty initial model.
    Empty,
    /// A base-bound Model Delta produced this generation.
    Delta {
        /// Parent generation.
        base: ModelGenerationId,
        /// Applied delta.
        delta: ModelDeltaId,
    },
}

/// Logically immutable authoritative model generation.
///
/// It has no public constructor and is deliberately not `Clone`: ownership and
/// the private authority seal remain coupled to the generation's values.
/// Sparse updates are materialized once per admitted window on first access,
/// allowing plane workers to reuse the owned backing without a cube-wide copy.
#[derive(Debug)]
pub struct ModelGeneration {
    generation_id: ModelGenerationId,
    authority: LogicalIdentity,
    seal: AuthoritySeal,
    shape: ModelSourceShape,
    samples: ModelSamples,
    origin: ModelGenerationOrigin,
}

impl ModelGeneration {
    /// Return the canonical generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> ModelGenerationId {
        self.generation_id
    }

    /// Return the exact model-space shape.
    #[must_use]
    pub const fn shape(&self) -> &ModelSourceShape {
        &self.shape
    }

    /// Return the logical sample count, independently of resident storage.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Read an explicitly bounded canonical sample range.
    pub fn read_samples(
        &self,
        range: std::ops::Range<usize>,
    ) -> Result<Box<[ModelSample]>, ModelLifecycleError> {
        self.samples.read(range)
    }

    /// Read one complete spatial model plane in canonical y, x order.
    pub fn read_plane(
        &self,
        domain: usize,
        coefficient: usize,
        polarization: usize,
    ) -> Result<Box<[ModelSample]>, ModelLifecycleError> {
        let start = self
            .shape
            .flat_index(ModelCell::new(domain, coefficient, polarization, [0, 0]))
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        let [width, height] = self.shape.domains()[domain].pixels();
        self.samples.read(start..start + width * height)
    }

    /// Return the owner-recorded origin.
    #[must_use]
    pub const fn origin(&self) -> ModelGenerationOrigin {
        self.origin
    }
}

/// Immutable sparse update validated and minted by one lifecycle owner.
#[derive(Debug)]
pub struct ModelDelta {
    delta_id: ModelDeltaId,
    authority: LogicalIdentity,
    seal: AuthoritySeal,
    base: ModelGenerationId,
    terms: Box<[ModelDeltaTerm]>,
}

impl ModelDelta {
    /// Return the run-local event associated with this update.
    #[must_use]
    pub const fn delta_id(&self) -> ModelDeltaId {
        self.delta_id
    }

    /// Return the exact generation this delta may update.
    #[must_use]
    pub const fn base(&self) -> ModelGenerationId {
        self.base
    }

    /// Return canonical, unique delta terms.
    #[must_use]
    pub const fn terms(&self) -> &[ModelDeltaTerm] {
        &self.terms
    }
}

/// Opaque proof that one affine final-model update completed through the owner.
///
/// `delta` is `None` exactly when the named input generation's samples and
/// support were confirmed unchanged as final. A generation carried from an
/// earlier attempt is still rebound to the completing lifecycle, so its final
/// identity may differ from `base` without a Model Delta. This is
/// reconstruction evidence, not a Product Generation seal.
#[derive(Debug)]
pub struct FinalModelCompletion {
    completion_id: FinalModelCompletionId,
    seal: AuthoritySeal,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    base: ModelGenerationId,
    delta: Option<ModelDeltaId>,
    generation: ModelGenerationId,
}

impl FinalModelCompletion {
    /// Return the completion identity.
    #[must_use]
    pub const fn completion_id(&self) -> FinalModelCompletionId {
        self.completion_id
    }

    /// Return the execution attempt that owns the completion.
    #[must_use]
    pub const fn attempt(&self) -> ModelExecutionAttemptId {
        self.attempt
    }

    /// Return the generation epoch within the attempt.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return the affine update's base generation.
    #[must_use]
    pub const fn base(&self) -> ModelGenerationId {
        self.base
    }

    /// Return the applied Model Delta, or `None` when the named input was confirmed unchanged.
    #[must_use]
    pub const fn delta(&self) -> Option<ModelDeltaId> {
        self.delta
    }

    /// Return the completed final generation.
    #[must_use]
    pub const fn generation(&self) -> ModelGenerationId {
        self.generation
    }
}

/// Affine handoff of one completed model generation to the next Major Cycle.
///
/// The token can be minted only by consuming a whole [`MajorCycleCompletion`]
/// and is itself consumed when the next execution attempt binds its model
/// lifecycle. Keeping the completion and generation inseparable prevents a
/// caller from pairing a model buffer with foreign finalization evidence.
#[doc(hidden)]
#[derive(Debug)]
pub struct FinalModelContinuation {
    pub(crate) completion: FinalModelCompletion,
    pub(crate) generation: ModelGeneration,
}

impl FinalModelContinuation {
    /// Borrow the completed generation used as the next Major Cycle's base.
    #[must_use]
    pub const fn generation(&self) -> &ModelGeneration {
        &self.generation
    }

    /// Borrow the finalization evidence paired with the generation.
    #[must_use]
    pub const fn completion(&self) -> &FinalModelCompletion {
        &self.completion
    }

    fn into_parts(self) -> (FinalModelCompletion, ModelGeneration) {
        (self.completion, self.generation)
    }
}

/// Result of the sole affine final-model operation intended for T20 composition.
#[derive(Debug)]
pub struct FinalModelUpdate {
    generation: ModelGeneration,
    completion: FinalModelCompletion,
}

/// Owner-validated final-model candidate whose one-shot completion authority has not
/// yet been consumed.
///
/// A Major Cycle prepares this value before its exhaustive operator replay and
/// commits it only after every fallible scientific and resource-bound step has
/// succeeded. The value has no public constructor and remains bound to one
/// lifecycle owner.
/// Pending sparse arithmetic is checked on window access and must succeed
/// before completion; a storage or scientific error fails the candidate.
#[doc(hidden)]
#[derive(Debug)]
pub struct PreparedFinalModel {
    generation: ModelGeneration,
    authority: LogicalIdentity,
    seal: AuthoritySeal,
    base: ModelGenerationId,
    delta: Option<ModelDeltaId>,
}

impl PreparedFinalModel {
    /// Borrow the candidate generation for fallible paired-operator work.
    #[must_use]
    pub const fn generation(&self) -> &ModelGeneration {
        &self.generation
    }

    /// Return the named input generation.
    #[must_use]
    pub const fn base(&self) -> ModelGenerationId {
        self.base
    }

    /// Return the candidate final generation.
    #[must_use]
    pub const fn generation_id(&self) -> ModelGenerationId {
        self.generation.generation_id
    }
}

impl FinalModelUpdate {
    /// Borrow the next authoritative generation.
    #[must_use]
    pub const fn generation(&self) -> &ModelGeneration {
        &self.generation
    }

    /// Borrow the distinct final-model completion.
    #[must_use]
    pub const fn completion(&self) -> &FinalModelCompletion {
        &self.completion
    }

    /// Consume the result into its generation and evidence.
    #[must_use]
    pub fn into_parts(self) -> (ModelGeneration, FinalModelCompletion) {
        (self.generation, self.completion)
    }
}

/// Solver-independent owner of one compiled model lifecycle.
///
/// This authority is deliberately not `Clone`. Generation IDs bind the
/// attempt, epoch, and unique owner instance, without hashing content.
/// The private instance binding rejects values minted by another owner. Its final
/// completion authority is consumed by the first finalization attempt.
#[derive(Debug)]
pub struct ModelLifecycle {
    contract: ModelLifecycleContract,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    authority: LogicalIdentity,
    seal: AuthoritySeal,
    final_authority: Option<FinalAuthority>,
    continuation: Option<ContinuationAuthority>,
    storage: ModelStoragePlan,
    next_generation: AtomicU64,
}

impl ModelLifecycle {
    /// Bind the Compiled Problem to one non-zero execution attempt and epoch.
    pub fn bind(
        problem: &CompiledProblem,
        attempt: ModelExecutionAttemptId,
        epoch: u64,
        storage: ModelStoragePlan,
    ) -> Result<Self, ModelLifecycleError> {
        if attempt.identity().as_bytes() == [0; 32] || epoch == 0 {
            return Err(ModelLifecycleError::InvalidExecutionBinding);
        }
        let seal = next_authority_seal();
        Ok(Self {
            contract: problem.model_lifecycle().clone(),
            attempt,
            epoch,
            authority: lifecycle_authority(attempt, epoch),
            seal,
            final_authority: Some(FinalAuthority(seal)),
            continuation: None,
            storage,
            next_generation: AtomicU64::new(1),
        })
    }

    /// Bind the next execution attempt by consuming one completed generation.
    ///
    /// This is the sole cross-attempt model handoff. The previous completion
    /// and generation stay inseparable until this method validates their
    /// generation and private owner binding. The returned generation is
    /// accepted only by this newly bound lifecycle and remains affine.
    pub fn continue_from(
        problem: &CompiledProblem,
        attempt: ModelExecutionAttemptId,
        epoch: u64,
        continuation: FinalModelContinuation,
        storage: ModelStoragePlan,
    ) -> Result<(Self, ModelGeneration), ModelLifecycleError> {
        let mut lifecycle = Self::bind(problem, attempt, epoch, storage)?;
        let (completion, mut generation) = continuation.into_parts();
        lifecycle.validate_generation_shape_and_bounds(&generation)?;
        let completion_identity = final_completion_id(
            generation.authority,
            completion.attempt,
            completion.epoch,
            completion.base,
            completion.delta,
            completion.generation,
        );
        if completion.generation != generation.generation_id
            || completion.seal != generation.seal
            || completion_identity != completion.completion_id
            || generation.shape != *lifecycle.contract.target()
        {
            return Err(ModelLifecycleError::ForeignModelLifecycle);
        }
        lifecycle.continuation = Some(ContinuationAuthority {
            seal: generation.seal,
            generation: generation.generation_id,
        });
        generation
            .samples
            .record_validated_bound(lifecycle.contract.bounds().max_absolute_model_value());
        Ok((lifecycle, generation))
    }

    /// Return the exact compiled lifecycle commitment.
    #[must_use]
    pub const fn contract(&self) -> &ModelLifecycleContract {
        &self.contract
    }

    /// Return the bound execution attempt.
    #[must_use]
    pub const fn attempt(&self) -> ModelExecutionAttemptId {
        self.attempt
    }

    /// Return the bound generation epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return the stable lifecycle authority behind every owner-minted ID.
    ///
    /// Unlike the per-instance process-local seal, this identity binds the
    /// attempt and epoch. Generation identities additionally bind the unique
    /// process-local owner instance.
    #[must_use]
    pub(crate) const fn authority(&self) -> LogicalIdentity {
        self.authority
    }

    /// Establish the empty initial generation every lifecycle begins from.
    pub fn initial_empty(&self) -> Result<ModelGeneration, ModelLifecycleError> {
        self.ensure_open()?;
        let zero = ModelValue::new(0.0)?;
        let mut samples = self.storage.create(self.contract.target().sample_count())?;
        let window = vec![ModelSample::valid(zero); samples.window_samples()];
        for start in (0..samples.len()).step_by(window.len()) {
            samples.write(start, &window[..window.len().min(samples.len() - start)])?;
        }
        self.mint_stored_generation(samples, ModelGenerationOrigin::Empty)
    }

    /// Validate and name one canonical sparse Model Delta.
    ///
    /// Terms must arrive in strictly increasing canonical cell order, which
    /// removes the former full sorting/canonicalization allocation.
    /// Delta derivation is independent of the lifecycle's one-shot final-model
    /// completion authority: a completed Major Cycle may derive the bounded
    /// T21 update that will be consumed by the next lifecycle attempt.
    pub fn compile_delta(
        &self,
        base: &ModelGeneration,
        terms: impl IntoIterator<Item = ModelDeltaTerm>,
    ) -> Result<ModelDelta, ModelLifecycleError> {
        self.validate_base(base)?;
        self.compile_delta_with_support(base.generation_id(), terms, |index| {
            Ok(base.samples.read(index..index + 1)?[0].support())
        })
    }

    fn compile_delta_with_support(
        &self,
        base: ModelGenerationId,
        terms: impl IntoIterator<Item = ModelDeltaTerm>,
        mut support: impl FnMut(usize) -> Result<ModelSupport, ModelLifecycleError>,
    ) -> Result<ModelDelta, ModelLifecycleError> {
        let terms = terms.into_iter();
        let capacity = terms.size_hint().0.min(
            self.contract
                .bounds()
                .max_delta_terms()
                .min(self.contract.target().sample_count()),
        );
        let mut canonical = Vec::with_capacity(capacity);
        let mut prior = None;
        for term in terms {
            if canonical.len() == self.contract.bounds().max_delta_terms() {
                return Err(ModelLifecycleError::DeltaTermBoundExceeded {
                    terms: canonical.len() + 1,
                    bound: self.contract.bounds().max_delta_terms(),
                });
            }
            let index = self
                .contract
                .target()
                .flat_index(term.cell())
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            if prior.is_some_and(|previous| previous >= index) {
                return Err(ModelLifecycleError::NonCanonicalDelta);
            }
            prior = Some(index);
            if term.increment().value() == 0.0 {
                return Err(ModelLifecycleError::ZeroDeltaTerm);
            }
            validate_model_value(
                term.increment(),
                self.contract.bounds().max_absolute_delta_value(),
            )
            .map_err(|_| ModelLifecycleError::DeltaValueBoundExceeded)?;
            if support(index)? != ModelSupport::Valid {
                return Err(ModelLifecycleError::DeltaOutsideValidSupport);
            }
            canonical.push(term);
        }
        if canonical.is_empty() {
            return Err(ModelLifecycleError::EmptyDelta);
        }
        let delta_id = ModelDeltaId(
            NEXT_MODEL_DELTA
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |ordinal| {
                    ordinal.checked_add(1)
                })
                .expect("model update event space exhausted"),
        );
        Ok(ModelDelta {
            delta_id,
            authority: self.authority,
            seal: self.seal,
            base,
            terms: canonical.into_boxed_slice(),
        })
    }

    /// Consume the sole owner and queue sparse updates on its existing storage.
    /// Each bounded window is updated once on first scientific access; disjoint
    /// windows can be prepared concurrently. Read and completion errors fail the
    /// candidate rather than returning partially updated model contents.
    pub fn apply_delta(
        &self,
        base: ModelGeneration,
        delta: ModelDelta,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        self.ensure_open()?;
        self.apply_delta_inner(base, delta)
    }

    /// Validate one authoritative candidate generation against this lifecycle
    /// without consuming it.
    ///
    /// A Major Cycle names its exact input generation through this owner check
    /// before any mutation; foreign or stale generations fail closed without
    /// rereading trusted model contents.
    pub fn validate_named_generation(
        &self,
        generation: &ModelGeneration,
    ) -> Result<(), ModelLifecycleError> {
        self.validate_base(generation)
    }

    /// Prepare one final-model candidate without consuming final-completion
    /// authority.
    ///
    /// This is the first phase of the Major-Cycle transaction. All model and
    /// delta association checks happen here. Sparse arithmetic is performed by
    /// the first consumer of each model window; completion also resolves any
    /// unvisited updates before consuming the lifecycle authority. A named generation
    /// carried from an earlier attempt is rebound in place to this lifecycle
    /// even when no Model Delta changes its samples.
    pub fn prepare_final_model(
        &self,
        named: ModelGeneration,
        delta: Option<ModelDelta>,
    ) -> Result<PreparedFinalModel, ModelLifecycleError> {
        self.ensure_open()?;
        let base = named.generation_id;
        let (mut generation, delta) = match delta {
            Some(delta) => {
                let delta_id = delta.delta_id;
                (self.apply_delta_inner(named, delta)?, Some(delta_id))
            }
            None => {
                let generation = self.adopt_generation(named)?;
                (generation, None)
            }
        };
        self.restore_later_domain_overlap(&mut generation)?;
        Ok(PreparedFinalModel {
            generation,
            authority: self.authority,
            seal: self.seal,
            base,
            delta,
        })
    }

    /// Restore CASA's canonical multi-domain model ownership after an update.
    ///
    /// Later compiled domains own shared sky pixels. CASA omits those pixels from
    /// earlier-domain prediction and restores the later values into earlier models
    /// before publication and continuation.
    fn restore_later_domain_overlap(
        &self,
        generation: &mut ModelGeneration,
    ) -> Result<(), ModelLifecycleError> {
        let shape = self.contract.target();
        if shape.domains().len() < 2 {
            return Ok(());
        }
        for earlier in (0..shape.domains().len() - 1).rev() {
            let [width, height] = shape.domains()[earlier].pixels();
            let target_coordinate = shape
                .direction(earlier)
                .ok_or(ModelLifecycleError::UnsupportedDirectionConversion)?;
            for y in 0..height {
                for x in 0..width {
                    let mut owner = None;
                    for later in earlier + 1..shape.domains().len() {
                        let source_coordinate = shape
                            .direction(later)
                            .ok_or(ModelLifecycleError::UnsupportedDirectionConversion)?;
                        if let Some(pixel) = crate::mask::reprojected_pixel(
                            source_coordinate,
                            shape.domains()[later].pixels(),
                            target_coordinate,
                            [x, y],
                        )
                        .map_err(|_| ModelLifecycleError::UnsupportedDirectionConversion)?
                        {
                            owner = Some((later, pixel));
                        }
                    }
                    let Some((owner_domain, owner_pixel)) = owner else {
                        continue;
                    };
                    for coefficient in 0..shape.coefficients() {
                        for polarization in 0..shape.polarizations() {
                            let target = shape
                                .flat_index(ModelCell::new(
                                    earlier,
                                    coefficient,
                                    polarization,
                                    [x, y],
                                ))
                                .ok_or(ModelLifecycleError::CellOutsideShape)?;
                            let source = shape
                                .flat_index(ModelCell::new(
                                    owner_domain,
                                    coefficient,
                                    polarization,
                                    owner_pixel,
                                ))
                                .ok_or(ModelLifecycleError::CellOutsideShape)?;
                            let sample =
                                match generation.samples.read(target..target + 1)?[0].support() {
                                    ModelSupport::Valid => ModelSample::valid(
                                        generation.samples.read(source..source + 1)?[0].value(),
                                    ),
                                    ModelSupport::Invalid => ModelSample::invalid(),
                                };
                            generation.samples.write(target, &[sample])?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Commit a successfully reconciled final-model candidate and mint its
    /// distinct completion evidence.
    pub fn commit_final_model(
        &mut self,
        mut prepared: PreparedFinalModel,
    ) -> Result<FinalModelUpdate, ModelLifecycleError> {
        self.ensure_open()?;
        if prepared.authority != self.authority || prepared.seal != self.seal {
            return Err(ModelLifecycleError::ForeignModelLifecycle);
        }
        self.validate_named_generation(&prepared.generation)?;
        prepared.generation.samples.complete_updates()?;
        let generation_id = prepared.generation.generation_id;
        let completion_id = final_completion_id(
            self.authority,
            self.attempt,
            self.epoch,
            prepared.base,
            prepared.delta,
            generation_id,
        );
        let final_authority = self
            .final_authority
            .take()
            .expect("open lifecycle retains final authority");
        let completion = FinalModelCompletion {
            completion_id,
            seal: final_authority.0,
            attempt: self.attempt,
            epoch: self.epoch,
            base: prepared.base,
            delta: prepared.delta,
            generation: generation_id,
        };
        debug_assert_eq!(completion.seal, self.seal);
        Ok(FinalModelUpdate {
            generation: prepared.generation,
            completion,
        })
    }

    /// Confirm one validated named generation's unchanged values as the final
    /// model without a pending Model Delta and mint distinct opaque completion
    /// evidence under this lifecycle.
    pub fn confirm_final_model(
        &mut self,
        named: ModelGeneration,
    ) -> Result<FinalModelUpdate, ModelLifecycleError> {
        let prepared = self.prepare_final_model(named, None)?;
        self.commit_final_model(prepared)
    }

    /// Perform the affine final-model update and mint distinct opaque completion evidence.
    ///
    /// Every validation runs before the one-shot final-completion authority is
    /// consumed, so a rejected update leaves the lifecycle exactly as it was.
    pub fn apply_final_delta(
        &mut self,
        base: ModelGeneration,
        delta: ModelDelta,
    ) -> Result<FinalModelUpdate, ModelLifecycleError> {
        let prepared = self.prepare_final_model(base, Some(delta))?;
        self.commit_final_model(prepared)
    }

    fn validate_delta_update(
        &self,
        base: &ModelGeneration,
        delta: &ModelDelta,
    ) -> Result<(), ModelLifecycleError> {
        self.validate_base(base)?;
        if delta.seal != self.seal
            || delta.authority != self.authority
            || delta.base != base.generation_id
        {
            return Err(ModelLifecycleError::DeltaBaseMismatch);
        }
        Ok(())
    }

    fn apply_delta_inner(
        &self,
        mut base: ModelGeneration,
        delta: ModelDelta,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        self.validate_delta_update(&base, &delta)?;
        let terms = delta.terms.iter().map(|term| ModelSampleUpdate {
            index: self
                .contract
                .target()
                .flat_index(term.cell())
                .expect("validated delta cell remains in range"),
            increment: term.increment().value(),
        });
        base.samples.queue_updates(
            terms,
            self.contract.arithmetic_precision(),
            self.contract.bounds().max_absolute_model_value(),
        )?;
        self.mint_stored_generation(
            base.samples,
            ModelGenerationOrigin::Delta {
                base: base.generation_id,
                delta: delta.delta_id,
            },
        )
    }

    fn adopt_generation(
        &self,
        mut generation: ModelGeneration,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        self.validate_base(&generation)?;
        if generation.authority != self.authority || generation.seal != self.seal {
            generation.authority = self.authority;
            generation.seal = self.seal;
            generation.generation_id = self.next_generation_id();
        }
        Ok(generation)
    }

    fn validate_base(&self, generation: &ModelGeneration) -> Result<(), ModelLifecycleError> {
        self.validate_generation_shape_and_bounds(generation)?;
        if generation.shape != *self.contract.target() {
            return Err(ModelLifecycleError::ForeignModelSpace);
        }
        if generation.seal == self.seal {
            return Ok(());
        }
        if self.continuation.is_some_and(|continuation| {
            continuation.seal == generation.seal
                && continuation.generation == generation.generation_id
        }) {
            Ok(())
        } else {
            Err(ModelLifecycleError::ForeignModelLifecycle)
        }
    }

    fn validate_generation_shape_and_bounds(
        &self,
        generation: &ModelGeneration,
    ) -> Result<(), ModelLifecycleError> {
        generation.samples.finish_updates()?;
        if generation.samples.len() != generation.shape.sample_count() {
            return Err(ModelLifecycleError::GenerationIdentityMismatch);
        }
        // A tighter scientific bound is a new constraint, unlike a routine
        // transfer of an already validated model. Overlap restoration can lower
        // the recorded maximum, so validate rather than reject this transition.
        if generation.samples.maximum_magnitude()
            <= self.contract.bounds().max_absolute_model_value()
        {
            return Ok(());
        }
        generation.samples.for_each_window(|_, samples| {
            for sample in samples {
                if sample.support() == ModelSupport::Valid {
                    validate_model_value(
                        sample.value(),
                        self.contract.bounds().max_absolute_model_value(),
                    )?;
                } else if sample.value().value() != 0.0 {
                    return Err(ModelLifecycleError::InvalidSupportPayload);
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    fn mint_stored_generation(
        &self,
        samples: ModelSamples,
        origin: ModelGenerationOrigin,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        let generation_id = self.next_generation_id();
        Ok(ModelGeneration {
            generation_id,
            authority: self.authority,
            seal: self.seal,
            shape: self.contract.target().clone(),
            samples,
            origin,
        })
    }

    fn next_generation_id(&self) -> ModelGenerationId {
        let ordinal = self
            .next_generation
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |ordinal| {
                ordinal.checked_add(1)
            })
            .expect("model generation ordinal exhausted");
        let mut encoder = Encoder::new(GENERATION_DOMAIN, GENERATION_VERSION);
        encoder.identity(self.authority.as_bytes());
        encoder.u64(self.seal.0);
        encoder.u64(ordinal);
        ModelGenerationId(LogicalIdentity::from_bytes(encoder.finish()))
    }

    fn ensure_open(&self) -> Result<(), ModelLifecycleError> {
        if self.final_authority.is_some() {
            Ok(())
        } else {
            Err(ModelLifecycleError::FinalModelAlreadyCompleted)
        }
    }
}

/// Exact reason model lifecycle validation failed closed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelLifecycleError {
    /// An admitted model backing or window could not be accessed.
    #[error("model storage: {0}")]
    Storage(String),
    /// A model schema value was invalid.
    #[error(transparent)]
    Contract(#[from] ModelContractError),
    /// The execution attempt identity or generation epoch was zero.
    #[error("model lifecycle requires a non-zero execution attempt and epoch")]
    InvalidExecutionBinding,
    /// A sample stream length differed from its shape.
    #[error("model stream requires {expected} samples but received {actual}")]
    SampleCountMismatch {
        /// Shape-derived sample count.
        expected: usize,
        /// Supplied sample count.
        actual: usize,
    },
    /// A cell lay outside its typed shape.
    #[error("model cell lies outside its declared shape")]
    CellOutsideShape,
    /// Two image domains' direction laws require an unsupported frame or tangent-point conversion.
    #[error("model domains overlap only through an exact affine mapping within one tangent plane")]
    UnsupportedDirectionConversion,
    /// A model value exceeded the lifecycle ceiling.
    #[error("model value exceeds the compiled lifecycle bound")]
    ModelValueBoundExceeded,
    /// A Model Delta contained no terms.
    #[error("a Model Delta requires at least one term")]
    EmptyDelta,
    /// A Model Delta exceeded its compiled term ceiling.
    #[error("Model Delta has {terms} terms, exceeding bound {bound}")]
    DeltaTermBoundExceeded {
        /// Exact term count observed before failure.
        terms: usize,
        /// Compiled term ceiling.
        bound: usize,
    },
    /// Delta terms were not unique and strictly canonical.
    #[error("Model Delta terms must be unique and in canonical cell order")]
    NonCanonicalDelta,
    /// A Model Delta term exceeded its value ceiling.
    #[error("Model Delta term exceeds the compiled lifecycle bound")]
    DeltaValueBoundExceeded,
    /// A Model Delta term had no numerical update.
    #[error("Model Delta terms must be non-zero")]
    ZeroDeltaTerm,
    /// A Model Delta attempted to create a value outside valid support.
    #[error("Model Delta terms may update only valid model support")]
    DeltaOutsideValidSupport,
    /// A generation belonged to another model space.
    #[error("model generation belongs to a different model space")]
    ForeignModelSpace,
    /// A generation belonged to a separately constructed lifecycle owner.
    #[error("model generation belongs to a different lifecycle owner")]
    ForeignModelLifecycle,
    /// A generation did not match the named input or its declared shape.
    #[error("model generation does not match its named input or shape")]
    GenerationIdentityMismatch,
    /// Invalid support carried a numeric payload.
    #[error("invalid model support may not carry a numeric value")]
    InvalidSupportPayload,
    /// A Model Delta did not name this exact base and owner.
    #[error("Model Delta does not name this base generation and owner")]
    DeltaBaseMismatch,
    /// The lifecycle's affine final-completion authority was already consumed.
    #[error("final-model completion authority has already been consumed")]
    FinalModelAlreadyCompleted,
}

fn next_authority_seal() -> AuthoritySeal {
    let seal = NEXT_AUTHORITY_SEAL
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |seal| {
            seal.checked_add(1)
        })
        .expect("model lifecycle authority seal space exhausted");
    AuthoritySeal(seal)
}

fn lifecycle_authority(attempt: ModelExecutionAttemptId, epoch: u64) -> LogicalIdentity {
    let mut encoder = Encoder::new(AUTHORITY_DOMAIN, AUTHORITY_VERSION);
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    LogicalIdentity::from_bytes(encoder.finish())
}

fn final_completion_id(
    authority: LogicalIdentity,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    base: ModelGenerationId,
    delta: Option<ModelDeltaId>,
    generation: ModelGenerationId,
) -> FinalModelCompletionId {
    let mut encoder = Encoder::new(FINAL_COMPLETION_DOMAIN, FINAL_COMPLETION_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(base.as_bytes());
    match delta {
        None => encoder.u8(0),
        Some(delta) => {
            encoder.u8(1);
            encoder.u64(delta.ordinal());
        }
    }
    encoder.identity(generation.as_bytes());
    FinalModelCompletionId(LogicalIdentity::from_bytes(encoder.finish()))
}

pub(crate) fn validate_model_value(
    value: ModelValue,
    bound: f64,
) -> Result<(), ModelLifecycleError> {
    if value.value().abs() > bound {
        Err(ModelLifecycleError::ModelValueBoundExceeded)
    } else {
        Ok(())
    }
}
