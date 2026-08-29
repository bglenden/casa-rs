// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Authoritative solver-independent model-state lifecycle.
//!
//! The model crate supplies only closed commitments and value schemas. This
//! crate owns ingest, reprojection, delta application, and opaque completion
//! evidence without importing storage, execution, product, or solver APIs.

use std::{
    error::Error,
    fmt,
    ops::Deref,
    sync::atomic::{AtomicU64, Ordering},
};

pub use casa_imaging_model::model_support_identity;
use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, DirectionCoordinateSpec, LogicalIdentity,
    ModelBasisConversionRegistry, ModelBounds, ModelCell, ModelContractError, ModelDeltaTerm,
    ModelDirectionConversionRegistry, ModelExecutionAttemptId, ModelInputCommitment,
    ModelInvalidContributorPolicy, ModelLifecycleContract, ModelLifecycleRequirements,
    ModelPolarizationConversionRegistry, ModelReprojectedSeedProjection, ModelReprojectionPolicy,
    ModelSample, ModelSourceShape, ModelStateIdentity, ModelSupport, ModelUncoveredTargetPolicy,
    ModelValue, NumericPrecision, PolarizationCoordinate, ReconstructionBasis,
    model_reprojected_seed_mapping_identity,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod continuum_transform;
mod gridded_normal_operator;
mod major_cycle;
mod mask;
mod minor_cycle;
mod psf_beam;
mod reconstruction_cycle;
mod spectral_operator;
mod spectral_sampling;
mod weighting;

pub use spectral_operator::{
    SpectralChannelValidity, SpectralOperatorError, SpectralOperatorPrimitives,
    SpectralOperatorSpecification, SpectralPrimitiveCatalog, SpectralSlabPlan,
};

/// Internal composition surface used by `casa-imaging-runtime`.
///
/// These opaque operations are public only because Rust crates have no friend
/// visibility. Application code should use the runtime's plan-bound T19 API.
#[doc(hidden)]
pub mod runtime_adapter {
    pub use crate::gridded_normal_operator::{
        GRIDDED_NORMAL_OPERATOR_RECORD_BYTES, GriddedNormalOperatorApply,
        GriddedNormalOperatorBlock, GriddedNormalOperatorCompiler, GriddedNormalOperatorProgram,
    };
    pub use crate::spectral_operator::{
        CompleteDataOwnerCompletion, CompleteDataOwnerResult, CompleteDataOwnerState,
        FinalVisibilitySample, PreparedSpectralOperator, SpectralOperatorPass,
        SpectralOperatorWorkload, SpectralSlabPlan, prepare_spectral_operator,
        spectral_operator_workload,
    };
    pub use crate::weighting::{
        FusedWeightingPhase, WeightingReplayPhase, begin_natural_weighting_stream,
    };
}

pub use continuum_transform::{
    ContinuumFitError, ContinuumFitStatus, ContinuumRowInput, ContinuumRowResult, ContinuumSample,
    fit_and_subtract_continuum,
};
pub use major_cycle::{
    FinalNormalState, FinalNormalStatePlane, MajorCycleCompletion, MajorCycleError,
    MajorCycleOwner, MajorCyclePreparation, NormalStateCatalog,
};
pub use mask::{
    AutoMultithreshControls, AutoMultithreshEvidence, MaskBox, MaskError, ReconstructionMask,
    ReconstructionMaskGenerationId, ReconstructionMaskPlan, auto_multithresh,
    reproject_mask_support,
};
pub use minor_cycle::{
    ClarkApproximation, ComponentDivergence, MinorCycleComponent, MinorCycleError,
    MinorCycleEvidence, MinorCycleEvidenceId, MinorCycleModelPlane, MinorCycleProgram,
    MinorCycleResult, MinorCycleStopReason, MinorCycleValidity, run_minor_cycle,
};
pub use psf_beam::{
    DEFAULT_PSF_FIT_CUTOFF, PsfBeamFitError, RestoringBeam, fit_restoring_beam,
    fitted_psf_sidelobe_fraction,
};
pub use reconstruction_cycle::{
    ChannelComponentDivergence, ChannelCycleEvidence, ChannelCyclePolicy, ReconstructionCycle,
    ReconstructionCycleError, ReconstructionCycleEvidence, ReconstructionCycleEvidenceId,
    ReconstructionCycleResult,
};
pub use spectral_sampling::{
    SpectralStencilError, SpectralStencilReceipt, SpectralStencilValidity, compile_spectral_stencil,
};
pub use weighting::{
    FrozenWeightingCoverageProof, FusedWeightingPhase, WeightingAlgorithmState,
    WeightingDensityPhase, WeightingError, WeightingExecutionLimits, WeightingGenerationId,
    WeightingPlan, WeightingReplayChunk, WeightingReplayCoverageId, WeightingReplayId,
    WeightingReplaySummary, WeightingResidency, WeightingSampleValue, WeightingSelectedSample,
    WeightingSpectralValue, begin_natural_weighting_stream, begin_weighting_generation,
    plan_weighting,
};

const AUTHORITY_DOMAIN: &[u8] = b"casa-rs-model-lifecycle-authority";
const AUTHORITY_VERSION: u32 = 2;
const GENERATION_DOMAIN: &[u8] = b"casa-rs-model-generation";
const GENERATION_VERSION: u32 = 2;
const DELTA_DOMAIN: &[u8] = b"casa-rs-model-delta";
const DELTA_VERSION: u32 = 2;
const REPROJECTION_VERSION: u32 = 3;
const REPROJECTED_SAMPLES_DOMAIN: &[u8] = b"casa-rs-reprojected-model-samples";
const REPROJECTED_SAMPLES_VERSION: u32 = 1;
const REPROJECTED_STENCIL_DOMAIN: &[u8] = b"casa-rs-reprojected-model-stencil";
const REPROJECTED_STENCIL_VERSION: u32 = 1;
const REPROJECTED_PROOF_DOMAIN: &[u8] = b"casa-rs-reprojected-model-proof";
const REPROJECTED_PROOF_VERSION: u32 = 1;
const FINAL_COMPLETION_DOMAIN: &[u8] = b"casa-rs-final-model-completion";
const FINAL_COMPLETION_VERSION: u32 = 2;
const FINAL_NORMAL_STATE_DOMAIN: &[u8] = b"casa-rs-final-normal-state";
const FINAL_NORMAL_STATE_VERSION: u32 = 2;
const MAJOR_CYCLE_DOMAIN: &[u8] = b"casa-rs-major-cycle-completion";
const MAJOR_CYCLE_VERSION: u32 = 2;

macro_rules! lifecycle_identity {
    ($name:ident, $version:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(LogicalIdentity);

        impl $name {
            /// Identity schema version used by the canonical encoder.
            pub const SCHEMA_VERSION: u32 = $version;

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0.as_bytes()
            }

            /// Return this typed identity as a compiler input commitment.
            #[must_use]
            pub const fn identity(self) -> LogicalIdentity {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.as_bytes())?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.as_bytes())
            }
        }
    };
}

lifecycle_identity!(
    ModelGenerationId,
    GENERATION_VERSION,
    "Stable owner-minted identity of one complete model generation."
);
lifecycle_identity!(
    ModelDeltaId,
    DELTA_VERSION,
    "Stable owner-minted identity of one base-bound Model Delta."
);
lifecycle_identity!(
    ModelReprojectionId,
    REPROJECTION_VERSION,
    "Stable identity of one validated canonical reprojection."
);
lifecycle_identity!(
    FinalModelCompletionId,
    FINAL_COMPLETION_VERSION,
    "Stable identity of one affine final-model completion."
);
lifecycle_identity!(
    FinalNormalStateCompletionId,
    FINAL_NORMAL_STATE_VERSION,
    "Stable identity of one authoritative final Normal State completion."
);
lifecycle_identity!(
    MajorCycleCompletionId,
    MAJOR_CYCLE_VERSION,
    "Stable identity of one atomic Major-Cycle reconciliation."
);

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

/// Fallible random-access source used by reconstruction-owned reprojection.
///
/// The reader supplies only source identity, typed geometry, and samples as
/// data. It cannot supply interpolation rules, support evidence, or a
/// reprojection identity; the reconstruction owner derives those values.
pub trait ModelSourceReader {
    /// Storage/provider error preserved by the preparation pass.
    type Error;

    /// Return the immutable source artifact identity.
    fn source_identity(&self) -> LogicalIdentity;

    /// Return the source artifact's exact typed model space.
    fn source_shape(&self) -> &ModelSourceShape;

    /// Read one source sample named by its typed cell.
    fn read_sample(&mut self, cell: ModelCell) -> Result<ModelSample, Self::Error>;
}

/// Failure while deriving one reprojected model seed from a source reader.
#[derive(Debug, PartialEq, Eq)]
pub enum ModelReprojectionError<E> {
    /// The source reader failed at the requested cell.
    Source(E),
    /// Reconstruction rejected geometry, support, numerics, or bounds.
    Lifecycle(ModelLifecycleError),
}

impl<E: fmt::Display> fmt::Display for ModelReprojectionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(formatter, "model source read failed: {error}"),
            Self::Lifecycle(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for ModelReprojectionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Lifecycle(error) => Some(error),
        }
    }
}

impl<E> From<ModelLifecycleError> for ModelReprojectionError<E> {
    fn from(error: ModelLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Opaque owner-derived reprojection ready to bind into a Compiled Problem.
///
/// Preparation is target-ordered and retains only the target generation plus
/// the current interpolation stencil. The value is deliberately not `Clone`;
/// lifecycle ingestion consumes it after checking every compiled claim.
#[derive(Debug)]
pub struct PreparedReprojectedSeed {
    projection: ModelReprojectedSeedProjection,
    target_shape: ModelSourceShape,
    bounds: ModelBounds,
    precision: NumericPrecision,
    samples: Box<[ModelSample]>,
}

impl PreparedReprojectedSeed {
    /// Return requirements containing the exact owner-derived evidence.
    #[must_use]
    pub fn lifecycle_requirements(&self) -> ModelLifecycleRequirements {
        ModelLifecycleRequirements::new(
            self.bounds,
            self.precision,
            ModelInputCommitment::ReprojectedSeed(self.projection.clone()),
        )
    }

    /// Return the canonical owner-derived reprojection identity.
    #[must_use]
    pub fn reprojection_id(&self) -> ModelReprojectionId {
        ModelReprojectionId(self.projection.reprojection())
    }

    /// Return the owner-derived validity identity of the projected target.
    #[must_use]
    pub const fn support_identity(&self) -> LogicalIdentity {
        self.projection.support()
    }

    /// Return the proof binding exact projected samples and ordered stencils.
    #[must_use]
    pub const fn proof_identity(&self) -> LogicalIdentity {
        self.projection.proof()
    }

    /// Bind this owner preparation to its final compiler projection.
    ///
    /// The compiler projection alone is descriptive. Consuming this opaque
    /// preparation is the only way to construct an executable reprojected
    /// problem, and every projected identity is checked before branding it.
    pub fn bind_compiled_problem(
        self,
        problem: CompiledProblem,
    ) -> Result<ExecutableModelProblem, ModelLifecycleError> {
        let ModelInputCommitment::ReprojectedSeed(projection) = problem.model_lifecycle().input()
        else {
            return Err(ModelLifecycleError::InitialModelKindMismatch);
        };
        if projection.source() != self.projection.source()
            || projection.source_shape() != self.projection.source_shape()
            || projection.preparation_contract() != self.projection.preparation_contract()
            || self.target_shape != *problem.model_lifecycle().target()
            || self.bounds != problem.model_lifecycle().bounds()
            || self.precision != problem.model_lifecycle().arithmetic_precision()
            || problem.inputs().model() != ModelStateIdentity::Seed(self.projection.source())
        {
            return Err(ModelLifecycleError::SourceProvenanceMismatch);
        }
        if projection.support() != self.projection.support() {
            return Err(ModelLifecycleError::SupportIdentityMismatch);
        }
        if projection.reprojection() != self.projection.reprojection()
            || projection.samples() != self.projection.samples()
            || projection.stencil() != self.projection.stencil()
            || projection.proof() != self.projection.proof()
        {
            return Err(ModelLifecycleError::ReprojectionIdentityMismatch);
        }
        Ok(ExecutableModelProblem {
            problem,
            prepared: Some(self),
        })
    }
}

/// Reconstruction-branded Compiled Problem accepted by execution and receipts.
///
/// Direct inputs are admitted only when no reprojected preparation claim is
/// present. Reprojected inputs can be constructed only by consuming
/// [`PreparedReprojectedSeed::bind_compiled_problem`].
///
/// The brand cannot be minted by downstream callers:
///
/// ```compile_fail
/// use casa_imaging_model::CompiledProblem;
/// use casa_imaging_reconstruction::ExecutableModelProblem;
///
/// fn forge(problem: CompiledProblem) -> ExecutableModelProblem {
///     ExecutableModelProblem {
///         problem,
///         prepared: None,
///     }
/// }
/// ```
#[derive(Debug)]
pub struct ExecutableModelProblem {
    problem: CompiledProblem,
    prepared: Option<PreparedReprojectedSeed>,
}

impl ExecutableModelProblem {
    /// Admit a Compiled Problem whose initial model needs no prior reprojection.
    pub fn from_compiled(problem: CompiledProblem) -> Result<Self, ModelLifecycleError> {
        if matches!(
            problem.model_lifecycle().input(),
            ModelInputCommitment::ReprojectedSeed(_)
        ) {
            return Err(ModelLifecycleError::OwnerPreparationRequired);
        }
        Ok(Self {
            problem,
            prepared: None,
        })
    }

    /// Borrow the immutable dependency-free compiler projection.
    #[must_use]
    pub const fn compiled_problem(&self) -> &CompiledProblem {
        &self.problem
    }
}

impl Deref for ExecutableModelProblem {
    type Target = CompiledProblem;

    fn deref(&self) -> &Self::Target {
        &self.problem
    }
}

#[derive(Debug, Clone, Copy)]
struct WeightedSourceCell {
    cell: ModelCell,
    weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct WeightedCoefficient {
    coefficient: usize,
    weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct WeightedPolarization {
    polarization: usize,
    weight: f64,
}

/// Owner-recorded origin of one named model generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelGenerationOrigin {
    /// The compiled problem explicitly began from an empty model.
    Empty,
    /// An identified source artifact was ingested, with optional reprojection.
    Ingested {
        /// External source artifact identity.
        source: LogicalIdentity,
        /// Exact reprojection identity, or `None` for aligned ingest.
        reprojection: Option<ModelReprojectionId>,
    },
    /// A base-bound Model Delta produced this generation.
    Delta {
        /// Parent generation.
        base: ModelGenerationId,
        /// Applied delta.
        delta: ModelDeltaId,
    },
}

/// Immutable authoritative model generation.
///
/// It has no public constructor and is deliberately not `Clone`: ownership and
/// the private authority seal remain coupled to the generation's values.
#[derive(Debug)]
pub struct ModelGeneration {
    generation_id: ModelGenerationId,
    authority: LogicalIdentity,
    seal: AuthoritySeal,
    shape: ModelSourceShape,
    samples: Box<[ModelSample]>,
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

    /// Return semantic values and independent validity state in canonical order.
    #[must_use]
    pub const fn samples(&self) -> &[ModelSample] {
        &self.samples
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
    /// Return the canonical delta identity.
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
/// `delta` is `None` exactly when the named input generation was confirmed
/// unchanged as final. This is reconstruction evidence, not a Product
/// Generation seal.
#[derive(Debug)]
pub struct FinalModelCompletion {
    completion_id: FinalModelCompletionId,
    seal: AuthoritySeal,
    problem: CompiledProblemId,
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

    /// Return the exact compiled problem.
    #[must_use]
    pub const fn problem(&self) -> CompiledProblemId {
        self.problem
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
    completion: FinalModelCompletion,
    generation: ModelGeneration,
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

/// Validated final-model candidate whose one-shot completion authority has not
/// yet been consumed.
///
/// A Major Cycle prepares this value before its exhaustive operator replay and
/// commits it only after every fallible scientific and resource-bound step has
/// succeeded. The value has no public constructor and remains bound to one
/// lifecycle owner.
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
    /// Borrow the validated candidate generation for paired-operator work.
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
/// This authority is deliberately not `Clone`. Stable IDs bind the complete
/// problem, attempt, and epoch, while a private per-instance seal prevents
/// values minted by a separately constructed owner from crossing into this
/// owner even when their stable content happens to be equal. Its final
/// completion authority is consumed by the first finalization attempt.
#[derive(Debug)]
pub struct ModelLifecycle {
    problem: CompiledProblemId,
    contract: ModelLifecycleContract,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    authority: LogicalIdentity,
    seal: AuthoritySeal,
    final_authority: Option<FinalAuthority>,
    continuation: Option<ContinuationAuthority>,
    prepared: Option<PreparedReprojectedSeed>,
}

impl ModelLifecycle {
    /// Bind the Compiled Problem to one non-zero execution attempt and epoch.
    pub fn bind(
        problem: ExecutableModelProblem,
        attempt: ModelExecutionAttemptId,
        epoch: u64,
    ) -> Result<Self, ModelLifecycleError> {
        if attempt.identity().as_bytes() == [0; 32] || epoch == 0 {
            return Err(ModelLifecycleError::InvalidExecutionBinding);
        }
        let authority = lifecycle_authority(
            problem.problem_id(),
            problem.model_lifecycle(),
            attempt,
            epoch,
        );
        let seal = next_authority_seal();
        let ExecutableModelProblem { problem, prepared } = problem;
        Ok(Self {
            problem: problem.problem_id(),
            contract: problem.model_lifecycle().clone(),
            attempt,
            epoch,
            authority,
            seal,
            final_authority: Some(FinalAuthority(seal)),
            continuation: None,
            prepared,
        })
    }

    /// Bind the next execution attempt by consuming one completed generation.
    ///
    /// This is the sole cross-attempt model handoff. The previous completion
    /// and generation stay inseparable until this method validates their
    /// problem, content, and private owner seal. The returned generation is
    /// accepted only by this newly bound lifecycle and remains affine.
    pub fn continue_from(
        problem: ExecutableModelProblem,
        attempt: ModelExecutionAttemptId,
        epoch: u64,
        continuation: FinalModelContinuation,
    ) -> Result<(Self, ModelGeneration), ModelLifecycleError> {
        let mut lifecycle = Self::bind(problem, attempt, epoch)?;
        let (completion, generation) = continuation.into_parts();
        lifecycle.validate_generation_integrity(&generation)?;
        let completion_identity = final_completion_id(
            generation.authority,
            completion.problem,
            completion.attempt,
            completion.epoch,
            completion.base,
            completion.delta,
            completion.generation,
        );
        if completion.problem != lifecycle.problem
            || completion.generation != generation.generation_id
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
        Ok((lifecycle, generation))
    }

    /// Return the exact compiled lifecycle commitment.
    #[must_use]
    pub const fn contract(&self) -> &ModelLifecycleContract {
        &self.contract
    }

    /// Return the exact compiled problem this lifecycle is bound to.
    #[must_use]
    pub const fn problem(&self) -> CompiledProblemId {
        self.problem
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
    /// compiled problem, lifecycle commitment, attempt, and epoch, so IDs
    /// derived from it remain stable across separate owner allocations.
    #[must_use]
    pub(crate) const fn authority(&self) -> LogicalIdentity {
        self.authority
    }

    /// Establish the compiled empty initial generation.
    pub fn initial_empty(&self) -> Result<ModelGeneration, ModelLifecycleError> {
        self.ensure_open()?;
        if !matches!(self.contract.input(), ModelInputCommitment::Empty) {
            return Err(ModelLifecycleError::InitialModelKindMismatch);
        }
        let zero = ModelValue::new(0.0)?;
        let samples = vec![ModelSample::valid(zero); self.contract.target().sample_count()];
        self.mint_generation(samples, ModelGenerationOrigin::Empty)
    }

    /// Consume one fallible aligned seed stream in exact canonical target order.
    ///
    /// The outer result preserves a source-reader failure. The inner result
    /// reports the lifecycle contract failure after the source stream has been
    /// consumed. Only the resulting generation buffer is retained; no complete
    /// source array, coverage bitmap, or canonicalization copy is allocated.
    pub fn ingest_aligned<E>(
        &self,
        source: LogicalIdentity,
        source_shape: &ModelSourceShape,
        samples: impl IntoIterator<Item = Result<ModelSample, E>>,
    ) -> Result<Result<ModelGeneration, ModelLifecycleError>, E> {
        if let Err(error) = self.ensure_open() {
            return Ok(Err(error));
        }
        let ModelInputCommitment::AlignedSeed {
            source: expected_source,
            support,
        } = self.contract.input()
        else {
            return Ok(Err(ModelLifecycleError::InitialModelKindMismatch));
        };
        if source != *expected_source || source_shape != self.contract.target() {
            return Ok(Err(ModelLifecycleError::SourceProvenanceMismatch));
        }
        let samples = match collect_exact_samples(
            samples,
            self.contract.target().sample_count(),
            self.contract.bounds().max_absolute_model_value(),
        )? {
            Ok(samples) => samples,
            Err(error) => return Ok(Err(error)),
        };
        if model_support_identity(samples.iter().map(|sample| sample.support())) != *support {
            return Ok(Err(ModelLifecycleError::SupportIdentityMismatch));
        }
        Ok(self.mint_generation(
            samples,
            ModelGenerationOrigin::Ingested {
                source,
                reprojection: None,
            },
        ))
    }

    /// Consume one reconstruction-derived reprojection after checking its compiled evidence.
    pub fn initial_reprojected(&mut self) -> Result<ModelGeneration, ModelLifecycleError> {
        self.ensure_open()?;
        let prepared = self
            .prepared
            .take()
            .ok_or(ModelLifecycleError::OwnerPreparationRequired)?;
        let ModelInputCommitment::ReprojectedSeed(commitment) = self.contract.input() else {
            return Err(ModelLifecycleError::InitialModelKindMismatch);
        };
        if prepared.projection.source() != commitment.source()
            || prepared.projection.source_shape() != commitment.source_shape()
            || prepared.target_shape != *self.contract.target()
            || prepared.projection.preparation_contract() != commitment.preparation_contract()
            || prepared.bounds != self.contract.bounds()
            || prepared.precision != self.contract.arithmetic_precision()
        {
            return Err(ModelLifecycleError::SourceProvenanceMismatch);
        }
        if model_support_identity(prepared.samples.iter().map(|sample| sample.support()))
            != commitment.support()
        {
            return Err(ModelLifecycleError::SupportIdentityMismatch);
        }
        if prepared.projection != *commitment {
            return Err(ModelLifecycleError::ReprojectionIdentityMismatch);
        }
        let source = prepared.projection.source();
        let reprojection = ModelReprojectionId(prepared.projection.reprojection());
        self.mint_generation(
            prepared.samples.into_vec(),
            ModelGenerationOrigin::Ingested {
                source,
                reprojection: Some(reprojection),
            },
        )
    }

    /// Adopt the exact generation named by the compiled input.
    pub fn resume(
        &self,
        generation: ModelGeneration,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        self.ensure_open()?;
        let ModelInputCommitment::Generation(expected) = self.contract.input() else {
            return Err(ModelLifecycleError::InitialModelKindMismatch);
        };
        self.validate_generation_integrity(&generation)?;
        if generation.shape != *self.contract.target()
            || generation.generation_id.identity() != *expected
        {
            return Err(ModelLifecycleError::GenerationIdentityMismatch);
        }
        Ok(generation)
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
        let capacity = self
            .contract
            .bounds()
            .max_delta_terms()
            .min(self.contract.target().sample_count());
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
            if base.samples[index].support() != ModelSupport::Valid {
                return Err(ModelLifecycleError::DeltaOutsideValidSupport);
            }
            canonical.push(term);
        }
        if canonical.is_empty() {
            return Err(ModelLifecycleError::EmptyDelta);
        }
        let delta_id = delta_id(
            self.authority,
            base.generation_id,
            self.contract.target(),
            &canonical,
        );
        Ok(ModelDelta {
            delta_id,
            authority: self.authority,
            seal: self.seal,
            base: base.generation_id,
            terms: canonical.into_boxed_slice(),
        })
    }

    /// Consume a generation and delta, reusing the generation buffer for the affine update.
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
    /// before any mutation; foreign, stale, or tampered evidence fails closed.
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
    /// delta validation and arithmetic happen here, while the lifecycle remains
    /// open if later complete-data reconciliation fails.
    pub fn prepare_final_model(
        &self,
        named: ModelGeneration,
        delta: Option<ModelDelta>,
    ) -> Result<PreparedFinalModel, ModelLifecycleError> {
        self.ensure_open()?;
        let base = named.generation_id;
        let (generation, delta) = match delta {
            Some(delta) => {
                let delta_id = delta.delta_id;
                (self.apply_delta_inner(named, delta)?, Some(delta_id))
            }
            None => {
                self.validate_named_generation(&named)?;
                (named, None)
            }
        };
        Ok(PreparedFinalModel {
            generation,
            authority: self.authority,
            seal: self.seal,
            base,
            delta,
        })
    }

    /// Commit a successfully reconciled final-model candidate and mint its
    /// distinct completion evidence.
    pub fn commit_final_model(
        &mut self,
        prepared: PreparedFinalModel,
    ) -> Result<FinalModelUpdate, ModelLifecycleError> {
        self.ensure_open()?;
        if prepared.authority != self.authority || prepared.seal != self.seal {
            return Err(ModelLifecycleError::ForeignModelLifecycle);
        }
        self.validate_named_generation(&prepared.generation)?;
        let generation_id = prepared.generation.generation_id;
        let completion_id = final_completion_id(
            self.authority,
            self.problem,
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
            problem: self.problem,
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

    /// Confirm one validated named generation as the final model without a
    /// pending Model Delta and mint distinct opaque completion evidence.
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
        let expected = delta_id(
            self.authority,
            base.generation_id,
            self.contract.target(),
            &delta.terms,
        );
        if expected != delta.delta_id {
            return Err(ModelLifecycleError::DeltaIdentityMismatch);
        }
        for term in &delta.terms {
            let index = self
                .contract
                .target()
                .flat_index(term.cell())
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            let updated = add_with_precision(
                self.contract.arithmetic_precision(),
                base.samples[index].value().value(),
                term.increment().value(),
            );
            let updated = ModelValue::new(updated)?;
            validate_model_value(updated, self.contract.bounds().max_absolute_model_value())?;
        }
        Ok(())
    }

    fn apply_delta_inner(
        &self,
        mut base: ModelGeneration,
        delta: ModelDelta,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        self.validate_delta_update(&base, &delta)?;
        for term in &delta.terms {
            let index = self
                .contract
                .target()
                .flat_index(term.cell())
                .expect("validated delta cell remains in range");
            let updated = add_with_precision(
                self.contract.arithmetic_precision(),
                base.samples[index].value().value(),
                term.increment().value(),
            );
            base.samples[index] = ModelSample::valid(ModelValue::new(updated)?);
        }
        let parent = base.generation_id;
        base.authority = self.authority;
        base.seal = self.seal;
        base.origin = ModelGenerationOrigin::Delta {
            base: parent,
            delta: delta.delta_id,
        };
        base.generation_id = generation_id(self.authority, &base.samples, base.origin);
        Ok(base)
    }

    fn validate_base(&self, generation: &ModelGeneration) -> Result<(), ModelLifecycleError> {
        self.validate_generation_integrity(generation)?;
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
            return Ok(());
        }
        if matches!(
            self.contract.input(),
            ModelInputCommitment::Generation(expected)
                if generation.generation_id.identity() == *expected
        ) {
            Ok(())
        } else {
            Err(ModelLifecycleError::ForeignModelLifecycle)
        }
    }

    fn validate_generation_integrity(
        &self,
        generation: &ModelGeneration,
    ) -> Result<(), ModelLifecycleError> {
        if generation.samples.len() != generation.shape.sample_count() {
            return Err(ModelLifecycleError::GenerationIdentityMismatch);
        }
        for sample in &generation.samples {
            if sample.support() == ModelSupport::Valid {
                validate_model_value(
                    sample.value(),
                    self.contract.bounds().max_absolute_model_value(),
                )?;
            } else if sample.value().value() != 0.0 {
                return Err(ModelLifecycleError::InvalidSupportPayload);
            }
        }
        if generation_id(generation.authority, &generation.samples, generation.origin)
            != generation.generation_id
        {
            return Err(ModelLifecycleError::GenerationIdentityMismatch);
        }
        Ok(())
    }

    fn mint_generation(
        &self,
        samples: Vec<ModelSample>,
        origin: ModelGenerationOrigin,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        if samples.len() != self.contract.target().sample_count() {
            return Err(ModelLifecycleError::SampleCountMismatch {
                expected: self.contract.target().sample_count(),
                actual: samples.len(),
            });
        }
        let generation_id = generation_id(self.authority, &samples, origin);
        Ok(ModelGeneration {
            generation_id,
            authority: self.authority,
            seal: self.seal,
            shape: self.contract.target().clone(),
            samples: samples.into_boxed_slice(),
            origin,
        })
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
    /// A raw reprojected compiler projection was presented without owner preparation.
    #[error("reprojected model execution requires reconstruction-owned preparation")]
    OwnerPreparationRequired,
    /// A model schema value was invalid.
    #[error(transparent)]
    Contract(#[from] ModelContractError),
    /// The execution attempt identity or generation epoch was zero.
    #[error("model lifecycle requires a non-zero execution attempt and epoch")]
    InvalidExecutionBinding,
    /// The requested initialization path differed from the compiled input commitment.
    #[error("model initialization does not match the compiled input commitment")]
    InitialModelKindMismatch,
    /// A source stream length differed from its shape.
    #[error("model stream requires {expected} samples but received {actual}")]
    SampleCountMismatch {
        /// Shape-derived sample count.
        expected: usize,
        /// Supplied sample count.
        actual: usize,
    },
    /// Source validity evidence differed from the Compiled Problem.
    #[error("model source validity evidence differs from the Compiled Problem")]
    SupportIdentityMismatch,
    /// Source artifact or typed coordinate/coefficient-space provenance differed.
    #[error("model source provenance differs from the Compiled Problem")]
    SourceProvenanceMismatch,
    /// A cell lay outside its typed source or target shape.
    #[error("model cell lies outside its declared shape")]
    CellOutsideShape,
    /// Source and target image-domain inventories cannot be paired exactly.
    #[error("model reprojection requires matching canonical image-domain inventories")]
    UnsupportedDomainMapping,
    /// Source and target direction laws require an unsupported frame or tangent-point conversion.
    #[error("model reprojection supports exact affine mapping within one direction tangent plane")]
    UnsupportedDirectionConversion,
    /// Source and target spectral coefficient bases cannot be mapped exactly.
    #[error("model reprojection cannot convert between the declared coefficient bases")]
    UnsupportedBasisConversion,
    /// A requested target polarization coordinate is absent from the source.
    #[error("model reprojection cannot derive the requested polarization coordinate")]
    UnsupportedPolarizationConversion,
    /// The mapping exceeded its compiled term ceiling.
    #[error("model reprojection has {terms} terms, exceeding bound {bound}")]
    ReprojectionTermBoundExceeded {
        /// Exact term count.
        terms: usize,
        /// Compiled term ceiling.
        bound: usize,
    },
    /// Reprojection weights did not sum to one in the committed arithmetic precision.
    #[error("reprojection stencil weights must sum to one")]
    UnnormalizedReprojectionStencil,
    /// Applied mapping evidence differed from the Compiled Problem.
    #[error("model reprojection identity differs from the Compiled Problem")]
    ReprojectionIdentityMismatch,
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
    /// A generation did not have its claimed content identity.
    #[error("model generation identity does not match its content")]
    GenerationIdentityMismatch,
    /// Invalid support carried a numeric payload.
    #[error("invalid model support may not carry a numeric value")]
    InvalidSupportPayload,
    /// A Model Delta did not name this exact base and owner.
    #[error("Model Delta does not name this base generation and owner")]
    DeltaBaseMismatch,
    /// A Model Delta did not have its claimed identity.
    #[error("Model Delta identity does not match its terms")]
    DeltaIdentityMismatch,
    /// The lifecycle's affine final-completion authority was already consumed.
    #[error("final-model completion authority has already been consumed")]
    FinalModelAlreadyCompleted,
}

/// Derive one exact target-ordered reprojection and its compiled input evidence.
///
/// The caller provides only a fallible source reader and a compiler-owned
/// empty target problem. Reconstruction derives the target shape, bounds,
/// precision, coefficient and polarization correspondence, affine
/// direction-coordinate stencils, target support, and the reprojection
/// identity in the same bounded pass.
pub fn prepare_reprojected_seed<R: ModelSourceReader>(
    reader: &mut R,
    target_problem: &CompiledProblem,
) -> Result<PreparedReprojectedSeed, ModelReprojectionError<R::Error>> {
    let target_contract = target_problem.model_lifecycle();
    if !matches!(target_contract.input(), ModelInputCommitment::Empty) {
        return Err(ModelLifecycleError::InitialModelKindMismatch.into());
    }
    let target_shape = target_contract.target();
    let bounds = target_contract.bounds();
    let precision = target_contract.arithmetic_precision();
    let reprojection_policy = target_contract.reprojection_policy();
    let source = reader.source_identity();
    let source_shape = reader.source_shape().clone();
    if source.as_bytes() == [0; 32] {
        return Err(
            ModelLifecycleError::Contract(ModelContractError::UnidentifiedInputEvidence).into(),
        );
    }
    if source_shape == *target_shape {
        return Err(ModelLifecycleError::SourceProvenanceMismatch.into());
    }
    if source_shape.sample_count() > bounds.max_source_samples() {
        return Err(
            ModelLifecycleError::Contract(ModelContractError::SourceSampleBoundExceeded {
                samples: source_shape.sample_count(),
                bound: bounds.max_source_samples(),
            })
            .into(),
        );
    }
    if target_shape.sample_count() > bounds.max_model_samples() {
        return Err(
            ModelLifecycleError::Contract(ModelContractError::ModelSampleBoundExceeded {
                samples: target_shape.sample_count(),
                bound: bounds.max_model_samples(),
            })
            .into(),
        );
    }
    if source_shape.domain_roles() != target_shape.domain_roles() {
        return Err(ModelLifecycleError::UnsupportedDomainMapping.into());
    }

    let preparation_contract =
        LogicalIdentity::from_sha256(target_contract.contract_id().as_bytes());
    let mapping = model_reprojected_seed_mapping_identity(
        preparation_contract,
        source_shape.identity(),
        target_shape.identity(),
    );
    let mut stencil_encoder = Encoder::new(REPROJECTED_STENCIL_DOMAIN, REPROJECTED_STENCIL_VERSION);
    stencil_encoder.identity(source_shape.identity().as_bytes());
    stencil_encoder.identity(target_shape.identity().as_bytes());
    stencil_encoder.u8(match precision {
        NumericPrecision::F32 => 0,
        NumericPrecision::F64 => 1,
    });
    let mut samples = Vec::with_capacity(target_shape.sample_count());
    let mut term_count = 0usize;
    for target_index in 0..target_shape.sample_count() {
        let target = target_shape
            .cell_at(target_index)
            .expect("target index is derived from the target shape");
        let stencil = derive_reprojection_stencil(
            &source_shape,
            target_shape,
            target,
            precision,
            reprojection_policy,
            term_count,
            bounds.max_reprojection_terms(),
        )?;
        stencil_encoder.usize(target_index);
        match &stencil {
            None => stencil_encoder.u8(0),
            Some(terms) => {
                stencil_encoder.u8(1);
                stencil_encoder.usize(terms.len());
                for weighted in terms {
                    stencil_encoder.usize(
                        source_shape
                            .flat_index(weighted.cell)
                            .expect("derived source stencil cell belongs to source shape"),
                    );
                    stencil_encoder.u64(canonical_f64_bits(weighted.weight));
                }
            }
        }
        match stencil {
            None => match reprojection_policy.uncovered_target() {
                ModelUncoveredTargetPolicy::Invalid => {
                    samples.push(ModelSample::invalid());
                }
            },
            Some(stencil) => {
                term_count = term_count.checked_add(stencil.len()).ok_or(
                    ModelLifecycleError::ReprojectionTermBoundExceeded {
                        terms: usize::MAX,
                        bound: bounds.max_reprojection_terms(),
                    },
                )?;
                if term_count > bounds.max_reprojection_terms() {
                    return Err(ModelLifecycleError::ReprojectionTermBoundExceeded {
                        terms: term_count,
                        bound: bounds.max_reprojection_terms(),
                    }
                    .into());
                }
                let mut valid = true;
                let mut value = 0.0_f64;
                for weighted in stencil {
                    let sample = reader
                        .read_sample(weighted.cell)
                        .map_err(ModelReprojectionError::Source)?;
                    if sample.support() == ModelSupport::Invalid {
                        match reprojection_policy.invalid_contributor() {
                            ModelInvalidContributorPolicy::InvalidateTarget => valid = false,
                        }
                    } else {
                        validate_model_value(sample.value(), bounds.max_absolute_model_value())?;
                        let product = multiply_with_precision(
                            precision,
                            sample.value().value(),
                            weighted.weight,
                        );
                        value = add_with_precision(precision, value, product);
                    }
                }
                if valid {
                    let value = ModelValue::new(value).map_err(ModelLifecycleError::from)?;
                    validate_model_value(value, bounds.max_absolute_model_value())?;
                    samples.push(ModelSample::valid(value));
                } else {
                    samples.push(ModelSample::invalid());
                }
            }
        }
    }
    let support = model_support_identity(samples.iter().map(|sample| sample.support()));
    let sample_identity = reprojected_samples_identity(&samples);
    let stencil_identity = LogicalIdentity::from_sha256(stencil_encoder.finish());
    let proof = reprojected_seed_proof_identity(
        source,
        source_shape.identity(),
        preparation_contract,
        mapping,
        support,
        sample_identity,
        stencil_identity,
    );
    let projection = ModelReprojectedSeedProjection::from_identities(
        source,
        source_shape.clone(),
        preparation_contract,
        mapping,
        support,
        sample_identity,
        stencil_identity,
        proof,
    )
    .map_err(ModelLifecycleError::from)?;
    Ok(PreparedReprojectedSeed {
        projection,
        target_shape: target_shape.clone(),
        bounds,
        precision,
        samples: samples.into_boxed_slice(),
    })
}

fn derive_reprojection_stencil(
    source: &ModelSourceShape,
    target_shape: &ModelSourceShape,
    target: ModelCell,
    precision: NumericPrecision,
    reprojection_policy: ModelReprojectionPolicy,
    prior_terms: usize,
    term_bound: usize,
) -> Result<Option<Vec<WeightedSourceCell>>, ModelLifecycleError> {
    if source.coefficient_space().inner_product()
        != target_shape.coefficient_space().inner_product()
    {
        return Err(ModelLifecycleError::UnsupportedBasisConversion);
    }
    let coefficient_terms = match reprojection_policy.basis_registry() {
        ModelBasisConversionRegistry::ExactSpectralV1 => {
            basis_conversion_terms(source, target_shape, target.coefficient(), precision)
        }
    }?;
    let Some(coefficient_terms) = coefficient_terms else {
        return Ok(None);
    };
    let polarization_terms = match reprojection_policy.polarization_registry() {
        ModelPolarizationConversionRegistry::RealParallelHandsV1 => {
            polarization_conversion_terms(source, target_shape, target.polarization())
        }
    }?;

    let source_direction = source
        .direction(target.domain())
        .ok_or(ModelLifecycleError::UnsupportedDomainMapping)?;
    let target_direction = target_shape
        .direction(target.domain())
        .ok_or(ModelLifecycleError::UnsupportedDomainMapping)?;
    let source_pixel = match reprojection_policy.direction_registry() {
        ModelDirectionConversionRegistry::SameTangentPlaneAffineBilinearV1 => {
            affine_source_pixel(source_direction, target_direction, target.pixel())
        }
    }?;
    let [width, height] = source
        .domains()
        .get(target.domain())
        .ok_or(ModelLifecycleError::UnsupportedDomainMapping)?
        .pixels();
    let tolerance = coordinate_tolerance(precision);
    let Some(x_terms) = axis_stencil(source_pixel[0], width, tolerance) else {
        return Ok(None);
    };
    let Some(y_terms) = axis_stencil(source_pixel[1], height, tolerance) else {
        return Ok(None);
    };
    if !normalized_weight_sum(
        precision,
        x_terms.iter().fold(0.0, |sum, (_, weight)| {
            add_with_precision(precision, sum, *weight)
        }),
    ) || !normalized_weight_sum(
        precision,
        y_terms.iter().fold(0.0, |sum, (_, weight)| {
            add_with_precision(precision, sum, *weight)
        }),
    ) {
        return Err(ModelLifecycleError::UnnormalizedReprojectionStencil);
    }
    let mut stencil = Vec::new();
    for (y, y_weight) in y_terms {
        for &(x, x_weight) in &x_terms {
            for coefficient in &coefficient_terms {
                for polarization in &polarization_terms {
                    let spatial_weight = multiply_with_precision(precision, x_weight, y_weight);
                    let basis_weight =
                        multiply_with_precision(precision, coefficient.weight, polarization.weight);
                    let weight = canonical_f64(multiply_with_precision(
                        precision,
                        spatial_weight,
                        basis_weight,
                    ));
                    if !weight.is_finite() {
                        return Err(ModelContractError::NonFiniteReprojectionWeight.into());
                    }
                    if weight != 0.0 {
                        if prior_terms
                            .checked_add(stencil.len())
                            .and_then(|terms| terms.checked_add(1))
                            .is_none_or(|terms| terms > term_bound)
                        {
                            return Err(ModelLifecycleError::ReprojectionTermBoundExceeded {
                                terms: prior_terms.saturating_add(stencil.len()).saturating_add(1),
                                bound: term_bound,
                            });
                        }
                        stencil.push(WeightedSourceCell {
                            cell: ModelCell::new(
                                target.domain(),
                                coefficient.coefficient,
                                polarization.polarization,
                                [x, y],
                            ),
                            weight,
                        });
                    }
                }
            }
        }
    }
    stencil.sort_unstable_by_key(|weighted| {
        source
            .flat_index(weighted.cell)
            .expect("owner-derived source stencil remains in range")
    });
    Ok(Some(stencil))
}

fn basis_conversion_terms(
    source: &ModelSourceShape,
    target: &ModelSourceShape,
    target_coefficient: usize,
    precision: NumericPrecision,
) -> Result<Option<Vec<WeightedCoefficient>>, ModelLifecycleError> {
    let source_basis = source.coefficient_space().basis();
    let target_basis = target.coefficient_space().basis();
    if target_coefficient >= target.coefficients() {
        return Err(ModelLifecycleError::CellOutsideShape);
    }
    if source.spectral().output_frame() != target.spectral().output_frame() {
        return Err(ModelLifecycleError::UnsupportedBasisConversion);
    }

    match (
        polynomial_terms(source_basis),
        polynomial_terms(target_basis),
    ) {
        (Some(source_terms), Some(target_terms)) => {
            if target_terms < source_terms {
                return Err(ModelLifecycleError::UnsupportedBasisConversion);
            }
            if target_coefficient >= source_terms {
                return Ok(Some(Vec::new()));
            }
            let source_reference = spectral_reference_frequency(source)?;
            let target_reference = spectral_reference_frequency(target)?;
            let offset = (target_reference - source_reference) / source_reference;
            let scale = target_reference / source_reference;
            let mut terms = Vec::with_capacity(source_terms - target_coefficient);
            for source_coefficient in target_coefficient..source_terms {
                let weight = binomial(source_coefficient, target_coefficient)
                    * offset.powi((source_coefficient - target_coefficient) as i32)
                    * scale.powi(target_coefficient as i32);
                let weight = round_to_precision(precision, weight);
                if weight != 0.0 {
                    terms.push(WeightedCoefficient {
                        coefficient: source_coefficient,
                        weight,
                    });
                }
            }
            Ok(Some(terms))
        }
        (Some(source_terms), None) => {
            let frequency = target
                .spectral()
                .channel_centre_hz(target_coefficient)
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            let reference = spectral_reference_frequency(source)?;
            let x = round_to_precision(precision, (frequency - reference) / reference);
            Ok(Some(
                (0..source_terms)
                    .filter_map(|coefficient| {
                        let weight = round_to_precision(precision, x.powi(coefficient as i32));
                        (weight != 0.0).then_some(WeightedCoefficient {
                            coefficient,
                            weight,
                        })
                    })
                    .collect(),
            ))
        }
        (None, Some(target_terms)) => {
            if target_terms < source.coefficients() {
                return Err(ModelLifecycleError::UnsupportedBasisConversion);
            }
            if target_coefficient >= source.coefficients() {
                return Ok(Some(Vec::new()));
            }
            channel_to_polynomial_terms(source, target, target_coefficient, precision).map(Some)
        }
        (None, None) => {
            let frequency = target
                .spectral()
                .channel_centre_hz(target_coefficient)
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            channel_interpolation_terms(source, frequency, precision)
        }
    }
}

const fn polynomial_terms(basis: ReconstructionBasis) -> Option<usize> {
    match basis {
        ReconstructionBasis::Constant => Some(1),
        ReconstructionBasis::Taylor { terms } => Some(terms),
        ReconstructionBasis::ChannelLocal { .. } => None,
    }
}

fn spectral_reference_frequency(shape: &ModelSourceShape) -> Result<f64, ModelLifecycleError> {
    let first = shape
        .spectral()
        .channel_centre_hz(0)
        .ok_or(ModelLifecycleError::UnsupportedBasisConversion)?;
    let last = shape
        .spectral()
        .channel_centre_hz(shape.spectral().output_channels() - 1)
        .ok_or(ModelLifecycleError::UnsupportedBasisConversion)?;
    let reference = 0.5 * (first + last);
    if reference.is_finite() && reference > 0.0 {
        Ok(reference)
    } else {
        Err(ModelLifecycleError::UnsupportedBasisConversion)
    }
}

fn channel_to_polynomial_terms(
    source: &ModelSourceShape,
    target: &ModelSourceShape,
    target_coefficient: usize,
    precision: NumericPrecision,
) -> Result<Vec<WeightedCoefficient>, ModelLifecycleError> {
    let reference = spectral_reference_frequency(target)?;
    let mut abscissae = Vec::with_capacity(source.coefficients());
    for channel in 0..source.coefficients() {
        let frequency = source
            .spectral()
            .channel_centre_hz(channel)
            .ok_or(ModelLifecycleError::UnsupportedBasisConversion)?;
        abscissae.push((frequency - reference) / reference);
    }
    let mut result = Vec::with_capacity(source.coefficients());
    for source_coefficient in 0..source.coefficients() {
        let mut polynomial = vec![1.0];
        let mut denominator = 1.0;
        for (other, x) in abscissae.iter().copied().enumerate() {
            if other == source_coefficient {
                continue;
            }
            let mut next = vec![0.0; polynomial.len() + 1];
            for (degree, coefficient) in polynomial.iter().copied().enumerate() {
                next[degree] -= coefficient * x;
                next[degree + 1] += coefficient;
            }
            polynomial = next;
            denominator *= abscissae[source_coefficient] - x;
        }
        if !denominator.is_finite() || denominator == 0.0 {
            return Err(ModelLifecycleError::UnsupportedBasisConversion);
        }
        let weight = round_to_precision(precision, polynomial[target_coefficient] / denominator);
        if weight != 0.0 {
            result.push(WeightedCoefficient {
                coefficient: source_coefficient,
                weight,
            });
        }
    }
    Ok(result)
}

fn channel_interpolation_terms(
    source: &ModelSourceShape,
    frequency: f64,
    precision: NumericPrecision,
) -> Result<Option<Vec<WeightedCoefficient>>, ModelLifecycleError> {
    let frequencies = (0..source.coefficients())
        .map(|channel| {
            source
                .spectral()
                .channel_centre_hz(channel)
                .ok_or(ModelLifecycleError::UnsupportedBasisConversion)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let tolerance = frequency_tolerance(precision, frequency);
    if let Some(channel) = frequencies
        .iter()
        .position(|candidate| (frequency - candidate).abs() <= tolerance)
    {
        return Ok(Some(vec![WeightedCoefficient {
            coefficient: channel,
            weight: 1.0,
        }]));
    }
    for channel in 0..frequencies.len().saturating_sub(1) {
        let lower = frequencies[channel];
        let upper = frequencies[channel + 1];
        if (frequency - lower) * (frequency - upper) < 0.0 {
            let upper_weight = round_to_precision(precision, (frequency - lower) / (upper - lower));
            return Ok(Some(vec![
                WeightedCoefficient {
                    coefficient: channel,
                    weight: round_to_precision(precision, 1.0 - upper_weight),
                },
                WeightedCoefficient {
                    coefficient: channel + 1,
                    weight: upper_weight,
                },
            ]));
        }
    }
    Ok(None)
}

fn polarization_conversion_terms(
    source: &ModelSourceShape,
    target: &ModelSourceShape,
    target_polarization: usize,
) -> Result<Vec<WeightedPolarization>, ModelLifecycleError> {
    let source_coordinates = source.coefficient_space().polarization().coordinates();
    let target_coordinate = *target
        .coefficient_space()
        .polarization()
        .coordinates()
        .get(target_polarization)
        .ok_or(ModelLifecycleError::CellOutsideShape)?;
    if let Some(polarization) = source_coordinates
        .iter()
        .position(|coordinate| *coordinate == target_coordinate)
    {
        return Ok(vec![WeightedPolarization {
            polarization,
            weight: 1.0,
        }]);
    }
    let pair = |first, second, first_weight, second_weight| {
        let first = source_coordinates
            .iter()
            .position(|value| *value == first)?;
        let second = source_coordinates
            .iter()
            .position(|value| *value == second)?;
        Some(vec![
            WeightedPolarization {
                polarization: first,
                weight: first_weight,
            },
            WeightedPolarization {
                polarization: second,
                weight: second_weight,
            },
        ])
    };
    let terms = match target_coordinate {
        PolarizationCoordinate::StokesI => pair(
            PolarizationCoordinate::LinearXx,
            PolarizationCoordinate::LinearYy,
            0.5,
            0.5,
        )
        .or_else(|| {
            pair(
                PolarizationCoordinate::CircularRr,
                PolarizationCoordinate::CircularLl,
                0.5,
                0.5,
            )
        }),
        PolarizationCoordinate::StokesQ => pair(
            PolarizationCoordinate::LinearXx,
            PolarizationCoordinate::LinearYy,
            0.5,
            -0.5,
        ),
        PolarizationCoordinate::StokesV => pair(
            PolarizationCoordinate::CircularRr,
            PolarizationCoordinate::CircularLl,
            0.5,
            -0.5,
        ),
        PolarizationCoordinate::LinearXx => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            1.0,
            1.0,
        ),
        PolarizationCoordinate::LinearYy => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            1.0,
            -1.0,
        ),
        PolarizationCoordinate::CircularRr => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesV,
            1.0,
            1.0,
        ),
        PolarizationCoordinate::CircularLl => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesV,
            1.0,
            -1.0,
        ),
        PolarizationCoordinate::StokesU
        | PolarizationCoordinate::LinearXy
        | PolarizationCoordinate::LinearYx
        | PolarizationCoordinate::CircularRl
        | PolarizationCoordinate::CircularLr => None,
    };
    terms.ok_or(ModelLifecycleError::UnsupportedPolarizationConversion)
}

fn binomial(n: usize, k: usize) -> f64 {
    let k = k.min(n - k);
    (0..k).fold(1.0, |value, index| {
        value * (n - index) as f64 / (index + 1) as f64
    })
}

fn frequency_tolerance(precision: NumericPrecision, frequency: f64) -> f64 {
    coordinate_tolerance(precision) * frequency.abs().max(1.0)
}

fn round_to_precision(precision: NumericPrecision, value: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from(value as f32),
        NumericPrecision::F64 => value,
    }
}

fn affine_source_pixel(
    source: DirectionCoordinateSpec,
    target: DirectionCoordinateSpec,
    target_pixel: [usize; 2],
) -> Result<[f64; 2], ModelLifecycleError> {
    if source.projection() != target.projection()
        || source.reference_direction() != target.reference_direction()
        || source.pole_deg() != target.pole_deg()
    {
        return Err(ModelLifecycleError::UnsupportedDirectionConversion);
    }
    let target_offset = [
        target_pixel[0] as f64 - target.reference_pixel()[0],
        target_pixel[1] as f64 - target.reference_pixel()[1],
    ];
    let target_pc = target.pc();
    let target_increment = target.increment_rad();
    let intermediate = [
        target_increment[0]
            * (target_pc[0][0] * target_offset[0] + target_pc[0][1] * target_offset[1]),
        target_increment[1]
            * (target_pc[1][0] * target_offset[0] + target_pc[1][1] * target_offset[1]),
    ];
    let source_increment = source.increment_rad();
    let source_intermediate = [
        intermediate[0] / source_increment[0],
        intermediate[1] / source_increment[1],
    ];
    let source_pc = source.pc();
    let determinant = source_pc[0][0] * source_pc[1][1] - source_pc[0][1] * source_pc[1][0];
    let source_offset = [
        (source_pc[1][1] * source_intermediate[0] - source_pc[0][1] * source_intermediate[1])
            / determinant,
        (-source_pc[1][0] * source_intermediate[0] + source_pc[0][0] * source_intermediate[1])
            / determinant,
    ];
    let reference = source.reference_pixel();
    let pixel = [
        canonical_f64(reference[0] + source_offset[0]),
        canonical_f64(reference[1] + source_offset[1]),
    ];
    if pixel.iter().all(|coordinate| coordinate.is_finite()) {
        Ok(pixel)
    } else {
        Err(ModelLifecycleError::UnsupportedDirectionConversion)
    }
}

fn axis_stencil(coordinate: f64, length: usize, tolerance: f64) -> Option<Vec<(usize, f64)>> {
    let maximum = (length - 1) as f64;
    if coordinate < -tolerance || coordinate > maximum + tolerance {
        return None;
    }
    let coordinate = coordinate.clamp(0.0, maximum);
    let nearest = coordinate.round();
    if (coordinate - nearest).abs() <= tolerance {
        return Some(vec![(nearest as usize, 1.0)]);
    }
    let lower = coordinate.floor() as usize;
    let upper = lower + 1;
    if upper >= length {
        return None;
    }
    let upper_weight = coordinate - lower as f64;
    Some(vec![(lower, 1.0 - upper_weight), (upper, upper_weight)])
}

const fn coordinate_tolerance(precision: NumericPrecision) -> f64 {
    match precision {
        NumericPrecision::F32 => 64.0 * f32::EPSILON as f64,
        NumericPrecision::F64 => 64.0 * f64::EPSILON,
    }
}

fn next_authority_seal() -> AuthoritySeal {
    let seal = NEXT_AUTHORITY_SEAL.fetch_add(1, Ordering::Relaxed);
    assert_ne!(seal, 0, "model lifecycle authority seal space exhausted");
    AuthoritySeal(seal)
}

fn lifecycle_authority(
    problem: CompiledProblemId,
    contract: &ModelLifecycleContract,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
) -> LogicalIdentity {
    let mut encoder = Encoder::new(AUTHORITY_DOMAIN, AUTHORITY_VERSION);
    encoder.identity(problem.as_bytes());
    encoder.identity(contract.contract_id().as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    LogicalIdentity::from_sha256(encoder.finish())
}

fn reprojected_samples_identity(samples: &[ModelSample]) -> LogicalIdentity {
    let mut encoder = Encoder::new(REPROJECTED_SAMPLES_DOMAIN, REPROJECTED_SAMPLES_VERSION);
    encoder.usize(samples.len());
    for sample in samples {
        encoder.u64(canonical_f64_bits(sample.value().value()));
        encoder.u8(match sample.support() {
            ModelSupport::Valid => 1,
            ModelSupport::Invalid => 0,
        });
    }
    LogicalIdentity::from_sha256(encoder.finish())
}

#[allow(clippy::too_many_arguments)]
fn reprojected_seed_proof_identity(
    source: LogicalIdentity,
    source_shape: LogicalIdentity,
    preparation_contract: LogicalIdentity,
    reprojection: LogicalIdentity,
    support: LogicalIdentity,
    samples: LogicalIdentity,
    stencil: LogicalIdentity,
) -> LogicalIdentity {
    let mut encoder = Encoder::new(REPROJECTED_PROOF_DOMAIN, REPROJECTED_PROOF_VERSION);
    encoder.identity(source.as_bytes());
    encoder.identity(source_shape.as_bytes());
    encoder.identity(preparation_contract.as_bytes());
    encoder.identity(reprojection.as_bytes());
    encoder.identity(support.as_bytes());
    encoder.identity(samples.as_bytes());
    encoder.identity(stencil.as_bytes());
    LogicalIdentity::from_sha256(encoder.finish())
}

/// Revalidate a durable reconstruction proof from its digest-only projection.
///
/// This verifies receipt integrity only; it cannot create the private prepared
/// values or the [`ExecutableModelProblem`] brand required for execution.
#[allow(clippy::too_many_arguments)]
pub fn validate_reprojected_seed_proof_identity(
    claimed: LogicalIdentity,
    source: LogicalIdentity,
    source_shape: LogicalIdentity,
    preparation_contract: LogicalIdentity,
    reprojection: LogicalIdentity,
    support: LogicalIdentity,
    samples: LogicalIdentity,
    stencil: LogicalIdentity,
) -> Result<(), ModelLifecycleError> {
    let identities = [
        claimed,
        source,
        source_shape,
        preparation_contract,
        reprojection,
        support,
        samples,
        stencil,
    ];
    if identities
        .iter()
        .any(|identity| identity.as_bytes() == [0; 32])
        || claimed
            != reprojected_seed_proof_identity(
                source,
                source_shape,
                preparation_contract,
                reprojection,
                support,
                samples,
                stencil,
            )
    {
        Err(ModelLifecycleError::ReprojectionIdentityMismatch)
    } else {
        Ok(())
    }
}

fn generation_id(
    authority: LogicalIdentity,
    samples: &[ModelSample],
    origin: ModelGenerationOrigin,
) -> ModelGenerationId {
    let mut encoder = Encoder::new(GENERATION_DOMAIN, GENERATION_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.usize(samples.len());
    for sample in samples {
        encoder.u64(canonical_f64_bits(sample.value().value()));
        encoder.u8(match sample.support() {
            ModelSupport::Valid => 1,
            ModelSupport::Invalid => 0,
        });
    }
    encode_origin(&mut encoder, origin);
    ModelGenerationId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn delta_id(
    authority: LogicalIdentity,
    base: ModelGenerationId,
    shape: &ModelSourceShape,
    terms: &[ModelDeltaTerm],
) -> ModelDeltaId {
    let mut encoder = Encoder::new(DELTA_DOMAIN, DELTA_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.identity(base.as_bytes());
    encoder.usize(terms.len());
    for term in terms {
        encoder.usize(
            shape
                .flat_index(term.cell())
                .expect("owner validates delta cells before hashing"),
        );
        encoder.u64(canonical_f64_bits(term.increment().value()));
    }
    ModelDeltaId(LogicalIdentity::from_sha256(encoder.finish()))
}

#[allow(clippy::too_many_arguments)]
fn final_completion_id(
    authority: LogicalIdentity,
    problem: CompiledProblemId,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    base: ModelGenerationId,
    delta: Option<ModelDeltaId>,
    generation: ModelGenerationId,
) -> FinalModelCompletionId {
    let mut encoder = Encoder::new(FINAL_COMPLETION_DOMAIN, FINAL_COMPLETION_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.identity(problem.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(base.as_bytes());
    match delta {
        None => encoder.u8(0),
        Some(delta) => {
            encoder.u8(1);
            encoder.identity(delta.as_bytes());
        }
    }
    encoder.identity(generation.as_bytes());
    FinalModelCompletionId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn encode_origin(encoder: &mut Encoder, origin: ModelGenerationOrigin) {
    match origin {
        ModelGenerationOrigin::Empty => encoder.u8(0),
        ModelGenerationOrigin::Ingested {
            source,
            reprojection,
        } => {
            encoder.u8(1);
            encoder.identity(source.as_bytes());
            match reprojection {
                Some(reprojection) => {
                    encoder.u8(1);
                    encoder.identity(reprojection.as_bytes());
                }
                None => encoder.u8(0),
            }
        }
        ModelGenerationOrigin::Delta { base, delta } => {
            encoder.u8(2);
            encoder.identity(base.as_bytes());
            encoder.identity(delta.as_bytes());
        }
    }
}

fn collect_exact_samples<E>(
    samples: impl IntoIterator<Item = Result<ModelSample, E>>,
    expected: usize,
    bound: f64,
) -> Result<Result<Vec<ModelSample>, ModelLifecycleError>, E> {
    let mut values = Vec::with_capacity(expected);
    let mut iterator = samples.into_iter();
    for index in 0..expected {
        let sample = match iterator.next() {
            None => {
                return Ok(Err(ModelLifecycleError::SampleCountMismatch {
                    expected,
                    actual: index,
                }));
            }
            Some(Err(error)) => return Err(error),
            Some(Ok(sample)) => sample,
        };
        if sample.support() == ModelSupport::Valid {
            if let Err(error) = validate_model_value(sample.value(), bound) {
                return Ok(Err(error));
            }
        } else if sample.value().value() != 0.0 {
            return Ok(Err(ModelLifecycleError::InvalidSupportPayload));
        }
        values.push(sample);
    }
    match iterator.next() {
        None => {}
        Some(Err(error)) => return Err(error),
        Some(Ok(_)) => {
            return Ok(Err(ModelLifecycleError::SampleCountMismatch {
                expected,
                actual: expected + 1,
            }));
        }
    }
    Ok(Ok(values))
}

fn validate_model_value(value: ModelValue, bound: f64) -> Result<(), ModelLifecycleError> {
    if value.value().abs() > bound {
        Err(ModelLifecycleError::ModelValueBoundExceeded)
    } else {
        Ok(())
    }
}

fn normalized_weight_sum(precision: NumericPrecision, sum: f64) -> bool {
    let tolerance = match precision {
        NumericPrecision::F32 => 64.0 * f64::from(f32::EPSILON),
        NumericPrecision::F64 => 64.0 * f64::EPSILON,
    };
    sum.is_finite() && (sum - 1.0).abs() <= tolerance
}

fn add_with_precision(precision: NumericPrecision, left: f64, right: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from((left as f32) + (right as f32)),
        NumericPrecision::F64 => left + right,
    }
}

fn multiply_with_precision(precision: NumericPrecision, left: f64, right: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from((left as f32) * (right as f32)),
        NumericPrecision::F64 => left * right,
    }
}

fn canonical_f64(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

pub(crate) fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

pub(crate) fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

pub(crate) struct Encoder(Sha256);

impl Encoder {
    pub(crate) fn new(domain: &[u8], version: u32) -> Self {
        let mut encoder = Self(Sha256::new());
        encoder.bytes(domain);
        encoder.u32(version);
        encoder
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    pub(crate) fn identity(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn usize(&mut self, value: usize) {
        self.u64(u64::try_from(value).expect("usize fits in u64 on supported targets"));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ModelSample, ModelValue, reprojected_samples_identity, reprojected_seed_proof_identity,
    };
    use casa_imaging_model::LogicalIdentity;

    fn identity(byte: u8) -> LogicalIdentity {
        LogicalIdentity::from_sha256([byte; 32])
    }

    #[test]
    fn reprojected_proof_binds_exact_values_and_ordered_stencil_identity() {
        let original_samples = reprojected_samples_identity(&[
            ModelSample::valid(ModelValue::new(2.0).expect("finite value")),
            ModelSample::valid(ModelValue::new(6.0).expect("finite value")),
        ]);
        let changed_samples = reprojected_samples_identity(&[
            ModelSample::valid(ModelValue::new(3.0).expect("finite value")),
            ModelSample::valid(ModelValue::new(6.0).expect("finite value")),
        ]);
        assert_ne!(original_samples, changed_samples);

        let proof = |samples, stencil| {
            reprojected_seed_proof_identity(
                identity(1),
                identity(2),
                identity(3),
                identity(4),
                identity(5),
                samples,
                stencil,
            )
        };
        assert_ne!(
            proof(original_samples, identity(6)),
            proof(changed_samples, identity(6)),
        );
        assert_ne!(
            proof(original_samples, identity(6)),
            proof(original_samples, identity(7)),
        );
    }
}
