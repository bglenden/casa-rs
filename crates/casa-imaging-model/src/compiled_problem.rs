// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeSet, fmt};

use sha2::{Digest, Sha256};
use thiserror::Error;

const COMPILED_PROBLEM_IDENTITY_DOMAIN: &[u8] = b"casa-rs-compiled-problem";
const COMPILED_PROBLEM_IDENTITY_VERSION: u32 = 1;

/// A content identity supplied by an owner outside the problem compiler.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalIdentity([u8; 32]);

impl LogicalIdentity {
    /// Construct an identity from an already computed SHA-256 digest.
    #[must_use]
    pub const fn from_sha256(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for LogicalIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LogicalIdentity(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for LogicalIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Stable identity of an immutable observation snapshot manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationSnapshotId(LogicalIdentity);

impl ObservationSnapshotId {
    /// Wrap an observation manifest content identity.
    #[must_use]
    pub const fn new(identity: LogicalIdentity) -> Self {
        Self(identity)
    }

    /// Return the wrapped logical identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.0
    }
}

/// Stable identity of compiled coordinate and image-domain geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompiledGeometryId(LogicalIdentity);

impl CompiledGeometryId {
    /// Wrap a compiled-geometry content identity.
    #[must_use]
    pub const fn new(identity: LogicalIdentity) -> Self {
        Self(identity)
    }

    /// Return the wrapped logical identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.0
    }
}

/// Authoritative reference-data family bound into an imaging problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReferenceDataKind {
    /// Measures tables and frame-conversion data.
    Measures,
    /// Ephemeris data.
    Ephemeris,
    /// Observatory and position data.
    Observatory,
    /// Spectral-line catalog data.
    SpectralLines,
    /// Instrument-response reference data.
    Instrument,
}

/// Logical identity of the initial model state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelStateIdentity {
    /// Begin from an empty model.
    Empty,
    /// Seed from an immutable model artifact.
    Seed(LogicalIdentity),
    /// Continue from an identified authoritative model generation.
    Generation(LogicalIdentity),
}

/// Immutable identities required to compile one logical problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProblemInputIdentities {
    observation: ObservationSnapshotId,
    geometry: CompiledGeometryId,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
}

impl ProblemInputIdentities {
    /// Construct input identities. Compilation canonicalizes reference ordering.
    #[must_use]
    pub fn new(
        observation: ObservationSnapshotId,
        geometry: CompiledGeometryId,
        reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
        model: ModelStateIdentity,
    ) -> Self {
        Self {
            observation,
            geometry,
            reference_data,
            model,
        }
    }

    /// Return the observation snapshot identity.
    #[must_use]
    pub const fn observation(&self) -> ObservationSnapshotId {
        self.observation
    }

    /// Return the compiled geometry identity.
    #[must_use]
    pub const fn geometry(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return reference identities in canonical family order after compilation.
    #[must_use]
    pub fn reference_data(&self) -> &[(ReferenceDataKind, LogicalIdentity)] {
        &self.reference_data
    }

    /// Return the initial model-state identity.
    #[must_use]
    pub const fn model(&self) -> ModelStateIdentity {
        self.model
    }

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.reference_data.sort_unstable_by_key(|(kind, _)| *kind);
        if let Some(kind) = self
            .reference_data
            .windows(2)
            .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
        {
            return Err(CompileProblemError::DuplicateReferenceData(kind));
        }
        Ok(self)
    }
}

/// Frequency-domain model coefficient basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconstructionBasis {
    /// One coefficient shared across the selected frequency domain.
    Constant,
    /// Taylor polynomial coefficients across frequency.
    Taylor {
        /// Number of Taylor coefficients.
        terms: usize,
    },
    /// Independent coefficient state for each output channel.
    ChannelLocal {
        /// Number of output channels.
        channels: usize,
    },
}

/// Logical reconstruction algorithm, independent of its implementation backend.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconstructionAlgorithm {
    /// Produce normal-state and dirty products without a minor cycle.
    Dirty,
    /// Högbom point-component minor cycle.
    Hogbom,
    /// Clark point-component minor cycle.
    Clark,
    /// Multiscale minor cycle with explicit scale sizes in pixels.
    Multiscale {
        /// Canonical requested scale sizes.
        scales_px: Vec<f64>,
    },
    /// Multi-term multi-frequency synthesis minor cycle.
    Mtmfs,
}

/// Scientific stopping and update controls for reconstruction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReconstructionControls {
    max_minor_iterations: usize,
    gain: f64,
    threshold_jy_per_beam: f64,
}

impl ReconstructionControls {
    /// Construct reconstruction controls.
    #[must_use]
    pub const fn new(max_minor_iterations: usize, gain: f64, threshold_jy_per_beam: f64) -> Self {
        Self {
            max_minor_iterations,
            gain,
            threshold_jy_per_beam,
        }
    }

    /// Return the maximum number of minor-cycle updates.
    #[must_use]
    pub const fn max_minor_iterations(self) -> usize {
        self.max_minor_iterations
    }

    /// Return the loop gain.
    #[must_use]
    pub const fn gain(self) -> f64 {
        self.gain
    }

    /// Return the absolute stopping threshold in Jy/beam.
    #[must_use]
    pub const fn threshold_jy_per_beam(self) -> f64 {
        self.threshold_jy_per_beam
    }
}

/// Logical reconstruction requirements for one imaging problem.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconstructionContract {
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
}

impl ReconstructionContract {
    /// Construct reconstruction requirements.
    #[must_use]
    pub const fn new(
        basis: ReconstructionBasis,
        algorithm: ReconstructionAlgorithm,
        controls: ReconstructionControls,
    ) -> Self {
        Self {
            basis,
            algorithm,
            controls,
        }
    }

    /// Return the reconstruction basis.
    #[must_use]
    pub const fn basis(&self) -> ReconstructionBasis {
        self.basis
    }

    /// Return the requested algorithm.
    #[must_use]
    pub const fn algorithm(&self) -> &ReconstructionAlgorithm {
        &self.algorithm
    }

    /// Return reconstruction controls.
    #[must_use]
    pub const fn controls(&self) -> ReconstructionControls {
        self.controls
    }

    fn canonicalize(mut self) -> Self {
        if let ReconstructionAlgorithm::Multiscale { scales_px } = &mut self.algorithm {
            for scale in scales_px.iter_mut() {
                if *scale == 0.0 {
                    *scale = 0.0;
                }
            }
            scales_px.sort_unstable_by(|left, right| left.total_cmp(right));
            scales_px.dedup();
        }
        self
    }
}

/// Visibility-weighting formula.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WeightingScheme {
    /// Natural weighting.
    Natural,
    /// Uniform density weighting.
    Uniform,
    /// Briggs robust weighting.
    Briggs {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
    },
    /// Briggs bandwidth-taper weighting.
    BriggsBandwidthTaper {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
    },
}

/// Domain over which visibility-density weights are derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightDensityScope {
    /// The weighting formula does not use a density generation.
    NotApplicable,
    /// All selected data contributing to the logical product.
    GlobalSelection,
    /// An explicit global density generation per output channel.
    PerOutputChannel,
}

/// Complete logical visibility-weighting requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightingContract {
    scheme: WeightingScheme,
    density_scope: WeightDensityScope,
}

impl WeightingContract {
    /// Construct weighting requirements.
    #[must_use]
    pub const fn new(scheme: WeightingScheme, density_scope: WeightDensityScope) -> Self {
        Self {
            scheme,
            density_scope,
        }
    }

    /// Return the weighting formula.
    #[must_use]
    pub const fn scheme(self) -> WeightingScheme {
        self.scheme
    }

    /// Return the density-generation scope.
    #[must_use]
    pub const fn density_scope(self) -> WeightDensityScope {
        self.density_scope
    }
}

/// Logical image product requested from the problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductKind {
    /// Point-spread function.
    Psf,
    /// Authoritative final residual.
    Residual,
    /// Reconstructed coefficient model.
    Model,
    /// Restored image.
    RestoredImage,
    /// Sum-of-weights state.
    SumWeights,
    /// Reconstruction mask.
    Mask,
    /// Imaging weight image.
    Weight,
    /// Primary-beam response.
    PrimaryBeam,
    /// Sensitivity response.
    Sensitivity,
    /// Primary-beam-corrected restored image.
    PbCorrectedImage,
    /// Taylor coefficient products.
    TaylorTerms,
    /// Spectral-index product.
    SpectralIndex,
    /// Spectral-index uncertainty.
    SpectralIndexError,
    /// Primary-beam-corrected spectral index.
    PbCorrectedSpectralIndex,
    /// Restoring and fitted beam metadata.
    Beam,
}

/// Published image normalization semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductNormalization {
    /// Unit-response normalization without direction-dependent sensitivity division.
    UnitResponse,
    /// Flat-noise normalization using sensitivity state.
    FlatNoise,
    /// Flat-sky normalization using sensitivity state.
    FlatSky,
}

/// Restoring-beam requirement for published products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoringBeamPolicy {
    /// Do not create a restored product.
    None,
    /// Fit and use an independent beam for each plane.
    PerPlane,
    /// Use one common enclosing beam across all spectral planes.
    Common,
}

/// Requested product set and publication semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductRequirements {
    products: Vec<ProductKind>,
    normalization: ProductNormalization,
    restoring_beam: RestoringBeamPolicy,
}

impl ProductRequirements {
    /// Construct product requirements. Compilation canonicalizes product ordering.
    #[must_use]
    pub fn new(
        products: Vec<ProductKind>,
        normalization: ProductNormalization,
        restoring_beam: RestoringBeamPolicy,
    ) -> Self {
        Self {
            products,
            normalization,
            restoring_beam,
        }
    }

    /// Return requested products in canonical order after compilation.
    #[must_use]
    pub fn products(&self) -> &[ProductKind] {
        &self.products
    }

    /// Return product normalization semantics.
    #[must_use]
    pub const fn normalization(&self) -> ProductNormalization {
        self.normalization
    }

    /// Return restoring-beam semantics.
    #[must_use]
    pub const fn restoring_beam(&self) -> RestoringBeamPolicy {
        self.restoring_beam
    }

    fn canonicalize(mut self) -> Self {
        self.products.sort_unstable();
        self.products.dedup();
        self
    }

    fn contains(&self, product: ProductKind) -> bool {
        self.products.binary_search(&product).is_ok()
    }
}

/// Arithmetic precision permitted by a problem's numerical contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NumericPrecision {
    /// IEEE-754 binary32 arithmetic.
    F32,
    /// IEEE-754 binary64 arithmetic.
    F64,
}

/// Reduction semantics permitted by a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReductionPolicy {
    /// A fixed pairwise reduction tree.
    DeterministicPairwise,
    /// Compensated accumulation with implementation-independent error bounds.
    Compensated,
    /// An unordered reduction accepted only within declared stage budgets.
    UnorderedWithinBudget,
}

/// Treatment of non-finite input and generated values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiniteValuePolicy {
    /// Reject every non-finite value.
    RejectAll,
    /// Treat declared non-finite inputs as flagged and reject generated non-finite values.
    FlagInputRejectGenerated,
}

/// Logical numerical stage requiring an explicit error budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NumericalStage {
    /// Coordinate transformations.
    CoordinateTransforms,
    /// Spectral transformations and sampling.
    SpectralTransforms,
    /// Visibility weighting.
    Weighting,
    /// Forward measurement operation.
    ForwardOperator,
    /// Adjoint measurement operation.
    AdjointOperator,
    /// Global reductions.
    Reductions,
    /// Reconstruction and minor-cycle updates.
    Reconstruction,
    /// Restoration.
    Restoration,
    /// Product formation and normalization.
    ProductFormation,
}

impl NumericalStage {
    /// Every stage that must have an explicit budget.
    pub const ALL: [Self; 9] = [
        Self::CoordinateTransforms,
        Self::SpectralTransforms,
        Self::Weighting,
        Self::ForwardOperator,
        Self::AdjointOperator,
        Self::Reductions,
        Self::Reconstruction,
        Self::Restoration,
        Self::ProductFormation,
    ];
}

/// Absolute and relative error allowance for one numerical stage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StageErrorBudget {
    absolute: f64,
    relative: f64,
}

impl StageErrorBudget {
    /// Construct a stage error budget.
    #[must_use]
    pub const fn new(absolute: f64, relative: f64) -> Self {
        Self { absolute, relative }
    }

    /// Return the absolute error allowance.
    #[must_use]
    pub const fn absolute(self) -> f64 {
        self.absolute
    }

    /// Return the relative error allowance.
    #[must_use]
    pub const fn relative(self) -> f64 {
        self.relative
    }
}

/// Complete numerical behavior permitted for one problem.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericsContract {
    permitted_precisions: Vec<NumericPrecision>,
    reduction: ReductionPolicy,
    finite_values: FiniteValuePolicy,
    stage_error_budgets: Vec<(NumericalStage, StageErrorBudget)>,
}

impl NumericsContract {
    /// Construct numerical requirements. Compilation canonicalizes ordering.
    #[must_use]
    pub fn new(
        permitted_precisions: Vec<NumericPrecision>,
        reduction: ReductionPolicy,
        finite_values: FiniteValuePolicy,
        stage_error_budgets: Vec<(NumericalStage, StageErrorBudget)>,
    ) -> Self {
        Self {
            permitted_precisions,
            reduction,
            finite_values,
            stage_error_budgets,
        }
    }

    /// Return permitted arithmetic precisions in canonical order after compilation.
    #[must_use]
    pub fn permitted_precisions(&self) -> &[NumericPrecision] {
        &self.permitted_precisions
    }

    /// Return permitted reduction semantics.
    #[must_use]
    pub const fn reduction(&self) -> ReductionPolicy {
        self.reduction
    }

    /// Return finite-value behavior.
    #[must_use]
    pub const fn finite_values(&self) -> FiniteValuePolicy {
        self.finite_values
    }

    /// Return complete stage budgets in canonical stage order after compilation.
    #[must_use]
    pub fn stage_error_budgets(&self) -> &[(NumericalStage, StageErrorBudget)] {
        &self.stage_error_budgets
    }

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.permitted_precisions.sort_unstable();
        self.permitted_precisions.dedup();
        if self.permitted_precisions.is_empty() {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "at least one arithmetic precision must be permitted",
            });
        }
        self.stage_error_budgets
            .sort_unstable_by_key(|(stage, _)| *stage);
        if self
            .stage_error_budgets
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "a numerical stage has more than one error budget",
            });
        }
        if self.stage_error_budgets.len() != NumericalStage::ALL.len()
            || NumericalStage::ALL
                .iter()
                .zip(&self.stage_error_budgets)
                .any(|(required, (actual, _))| required != actual)
        {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "every numerical stage must have exactly one error budget",
            });
        }
        if self.stage_error_budgets.iter().any(|(_, budget)| {
            !(budget.absolute.is_finite()
                && budget.absolute >= 0.0
                && budget.relative.is_finite()
                && budget.relative >= 0.0)
        }) {
            return Err(CompileProblemError::InvalidNumerics {
                reason: "stage error budgets must be finite and non-negative",
            });
        }
        Ok(self)
    }
}

/// Complete uncompiled logical problem specification.
#[derive(Debug, Clone, PartialEq)]
pub struct ProblemSpecification {
    reconstruction: ReconstructionContract,
    weighting: WeightingContract,
    products: ProductRequirements,
    numerics: NumericsContract,
}

impl ProblemSpecification {
    /// Construct a logical problem specification.
    #[must_use]
    pub const fn new(
        reconstruction: ReconstructionContract,
        weighting: WeightingContract,
        products: ProductRequirements,
        numerics: NumericsContract,
    ) -> Self {
        Self {
            reconstruction,
            weighting,
            products,
            numerics,
        }
    }
}

/// Backend-independent capability required to plan and execute a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequiredCapability {
    /// Constant reconstruction basis.
    ConstantBasis,
    /// Taylor reconstruction basis.
    TaylorBasis,
    /// Channel-local reconstruction basis.
    ChannelLocalBasis,
    /// Dirty-only reconstruction.
    DirtyReconstruction,
    /// Högbom reconstruction.
    HogbomReconstruction,
    /// Clark reconstruction.
    ClarkReconstruction,
    /// Multiscale reconstruction.
    MultiscaleReconstruction,
    /// MT-MFS reconstruction.
    MtmfsReconstruction,
    /// Natural weighting.
    NaturalWeighting,
    /// Uniform density weighting.
    UniformWeighting,
    /// Briggs density weighting.
    BriggsWeighting,
    /// Briggs bandwidth-taper weighting.
    BriggsBandwidthTaperWeighting,
    /// Unit-response product normalization.
    UnitResponseNormalization,
    /// Flat-noise product normalization.
    FlatNoiseNormalization,
    /// Flat-sky product normalization.
    FlatSkyNormalization,
    /// Formation of a particular logical product.
    Product(ProductKind),
}

/// Stable comparable identity of one compiled problem.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompiledProblemId(LogicalIdentity);

impl CompiledProblemId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = COMPILED_PROBLEM_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for CompiledProblemId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompiledProblemId(")?;
        write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

impl fmt::Display for CompiledProblemId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.as_bytes())
    }
}

/// Immutable logical problem accepted by downstream planning.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledProblem {
    problem_id: CompiledProblemId,
    inputs: ProblemInputIdentities,
    reconstruction: ReconstructionContract,
    weighting: WeightingContract,
    products: ProductRequirements,
    numerics: NumericsContract,
    required_capabilities: BTreeSet<RequiredCapability>,
}

impl CompiledProblem {
    /// Return the canonical comparable identity.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return immutable input identities.
    #[must_use]
    pub const fn inputs(&self) -> &ProblemInputIdentities {
        &self.inputs
    }

    /// Return reconstruction requirements.
    #[must_use]
    pub const fn reconstruction(&self) -> &ReconstructionContract {
        &self.reconstruction
    }

    /// Return visibility-weighting requirements.
    #[must_use]
    pub const fn weighting(&self) -> &WeightingContract {
        &self.weighting
    }

    /// Return product requirements.
    #[must_use]
    pub const fn products(&self) -> &ProductRequirements {
        &self.products
    }

    /// Return numerical requirements.
    #[must_use]
    pub const fn numerics(&self) -> &NumericsContract {
        &self.numerics
    }

    /// Return the complete sorted capability set.
    #[must_use]
    pub const fn required_capabilities(&self) -> &BTreeSet<RequiredCapability> {
        &self.required_capabilities
    }
}

/// Failure to compile a logical imaging problem.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompileProblemError {
    /// More than one identity was supplied for one reference-data family.
    #[error("duplicate reference-data identity for {0:?}")]
    DuplicateReferenceData(ReferenceDataKind),
    /// Reconstruction and capability requirements contradict each other.
    #[error("invalid capability combination: {reason}")]
    InvalidCapabilityCombination {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Weighting requirements are outside the logical domain.
    #[error("invalid weighting contract: {reason}")]
    InvalidWeighting {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Product normalization requirements contradict the product set.
    #[error("invalid normalization combination: {reason}")]
    InvalidNormalizationCombination {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Numerical requirements are incomplete or invalid.
    #[error("invalid numerics contract: {reason}")]
    InvalidNumerics {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Requested products contradict each other or reconstruction semantics.
    #[error("invalid product combination: {reason}")]
    InvalidProductCombination {
        /// Stable human-readable reason.
        reason: &'static str,
    },
}

/// Compile and validate one immutable backend-independent imaging problem.
pub fn compile_problem(
    specification: ProblemSpecification,
    inputs: ProblemInputIdentities,
) -> Result<CompiledProblem, CompileProblemError> {
    let inputs = inputs.canonicalize()?;
    let products = specification.products.canonicalize();
    let numerics = specification.numerics.canonicalize()?;
    validate_reconstruction(&specification.reconstruction)?;
    let reconstruction = specification.reconstruction.canonicalize();
    validate_weighting(specification.weighting)?;
    validate_products(&reconstruction, &products)?;
    let required_capabilities =
        derive_capabilities(&reconstruction, specification.weighting, &products);
    let problem_id = canonical_problem_id(
        &inputs,
        &reconstruction,
        specification.weighting,
        &products,
        &numerics,
    );
    Ok(CompiledProblem {
        problem_id,
        inputs,
        reconstruction,
        weighting: specification.weighting,
        products,
        numerics,
        required_capabilities,
    })
}

fn validate_reconstruction(contract: &ReconstructionContract) -> Result<(), CompileProblemError> {
    match contract.basis {
        ReconstructionBasis::Taylor { terms: 0 } => {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "a Taylor basis requires at least one term",
            });
        }
        ReconstructionBasis::ChannelLocal { channels: 0 } => {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "a channel-local basis requires at least one channel",
            });
        }
        ReconstructionBasis::Constant
        | ReconstructionBasis::Taylor { .. }
        | ReconstructionBasis::ChannelLocal { .. } => {}
    }
    if matches!(contract.algorithm, ReconstructionAlgorithm::Mtmfs)
        != matches!(contract.basis, ReconstructionBasis::Taylor { .. })
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "MT-MFS and Taylor-basis reconstruction must be requested together",
        });
    }
    if matches!(contract.algorithm, ReconstructionAlgorithm::Dirty)
        && contract.controls.max_minor_iterations != 0
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "dirty reconstruction cannot request minor-cycle iterations",
        });
    }
    if !matches!(contract.algorithm, ReconstructionAlgorithm::Dirty)
        && contract.controls.max_minor_iterations == 0
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "a minor-cycle algorithm requires a positive iteration budget",
        });
    }
    if !(contract.controls.gain.is_finite()
        && contract.controls.gain > 0.0
        && contract.controls.gain <= 1.0
        && contract.controls.threshold_jy_per_beam.is_finite()
        && contract.controls.threshold_jy_per_beam >= 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "reconstruction gain and threshold must be finite and in their valid domains",
        });
    }
    if let ReconstructionAlgorithm::Multiscale { scales_px } = &contract.algorithm {
        if scales_px.is_empty()
            || scales_px
                .iter()
                .any(|scale| !(scale.is_finite() && *scale >= 0.0))
        {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "multiscale reconstruction requires finite non-negative explicit scales",
            });
        }
    }
    Ok(())
}

fn validate_weighting(contract: WeightingContract) -> Result<(), CompileProblemError> {
    match contract.scheme {
        WeightingScheme::Natural if contract.density_scope != WeightDensityScope::NotApplicable => {
            return Err(CompileProblemError::InvalidWeighting {
                reason: "natural weighting has no density generation",
            });
        }
        WeightingScheme::Uniform
        | WeightingScheme::Briggs { .. }
        | WeightingScheme::BriggsBandwidthTaper { .. }
            if contract.density_scope == WeightDensityScope::NotApplicable =>
        {
            return Err(CompileProblemError::InvalidWeighting {
                reason: "density weighting requires an explicit global density scope",
            });
        }
        WeightingScheme::Natural
        | WeightingScheme::Uniform
        | WeightingScheme::Briggs { .. }
        | WeightingScheme::BriggsBandwidthTaper { .. } => {}
    }
    let robust = match contract.scheme {
        WeightingScheme::Briggs { robust } | WeightingScheme::BriggsBandwidthTaper { robust } => {
            Some(robust)
        }
        WeightingScheme::Natural | WeightingScheme::Uniform => None,
    };
    if robust.is_some_and(|value| !(value.is_finite() && (-2.0..=2.0).contains(&value))) {
        return Err(CompileProblemError::InvalidWeighting {
            reason: "Briggs robustness must be finite and in [-2, 2]",
        });
    }
    Ok(())
}

fn validate_products(
    reconstruction: &ReconstructionContract,
    products: &ProductRequirements,
) -> Result<(), CompileProblemError> {
    if products.products.is_empty() {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "at least one product must be requested",
        });
    }
    if matches!(
        products.normalization,
        ProductNormalization::FlatNoise | ProductNormalization::FlatSky
    ) && !products.contains(ProductKind::Sensitivity)
    {
        return Err(CompileProblemError::InvalidNormalizationCombination {
            reason: "flat-noise and flat-sky normalization require sensitivity state",
        });
    }
    let restored_image_requested = products.contains(ProductKind::RestoredImage);
    let restoring_beam_requested = !matches!(products.restoring_beam, RestoringBeamPolicy::None);
    if restored_image_requested != restoring_beam_requested {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "restored-image and restoring-beam requirements must be requested together",
        });
    }
    if products.contains(ProductKind::PbCorrectedImage)
        && !(products.contains(ProductKind::RestoredImage)
            && products.contains(ProductKind::PrimaryBeam))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a PB-corrected image requires restored-image and primary-beam products",
        });
    }
    let taylor_terms = match reconstruction.basis {
        ReconstructionBasis::Taylor { terms } => terms,
        ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => 0,
    };
    if products.contains(ProductKind::TaylorTerms) && taylor_terms == 0 {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "Taylor products require a Taylor reconstruction basis",
        });
    }
    if products.contains(ProductKind::SpectralIndex)
        && !(taylor_terms >= 2 && products.contains(ProductKind::TaylorTerms))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral index requires at least two Taylor terms and Taylor products",
        });
    }
    if products.contains(ProductKind::SpectralIndexError)
        && !products.contains(ProductKind::SpectralIndex)
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral-index uncertainty requires a spectral-index product",
        });
    }
    if products.contains(ProductKind::PbCorrectedSpectralIndex)
        && !(products.contains(ProductKind::SpectralIndex)
            && products.contains(ProductKind::PrimaryBeam))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "PB-corrected spectral index requires spectral-index and primary-beam products",
        });
    }
    Ok(())
}

fn derive_capabilities(
    reconstruction: &ReconstructionContract,
    weighting: WeightingContract,
    products: &ProductRequirements,
) -> BTreeSet<RequiredCapability> {
    let mut capabilities = BTreeSet::new();
    capabilities.insert(match reconstruction.basis {
        ReconstructionBasis::Constant => RequiredCapability::ConstantBasis,
        ReconstructionBasis::Taylor { .. } => RequiredCapability::TaylorBasis,
        ReconstructionBasis::ChannelLocal { .. } => RequiredCapability::ChannelLocalBasis,
    });
    capabilities.insert(match reconstruction.algorithm {
        ReconstructionAlgorithm::Dirty => RequiredCapability::DirtyReconstruction,
        ReconstructionAlgorithm::Hogbom => RequiredCapability::HogbomReconstruction,
        ReconstructionAlgorithm::Clark => RequiredCapability::ClarkReconstruction,
        ReconstructionAlgorithm::Multiscale { .. } => RequiredCapability::MultiscaleReconstruction,
        ReconstructionAlgorithm::Mtmfs => RequiredCapability::MtmfsReconstruction,
    });
    capabilities.insert(match weighting.scheme {
        WeightingScheme::Natural => RequiredCapability::NaturalWeighting,
        WeightingScheme::Uniform => RequiredCapability::UniformWeighting,
        WeightingScheme::Briggs { .. } => RequiredCapability::BriggsWeighting,
        WeightingScheme::BriggsBandwidthTaper { .. } => {
            RequiredCapability::BriggsBandwidthTaperWeighting
        }
    });
    capabilities.insert(match products.normalization {
        ProductNormalization::UnitResponse => RequiredCapability::UnitResponseNormalization,
        ProductNormalization::FlatNoise => RequiredCapability::FlatNoiseNormalization,
        ProductNormalization::FlatSky => RequiredCapability::FlatSkyNormalization,
    });
    capabilities.extend(
        products
            .products
            .iter()
            .copied()
            .map(RequiredCapability::Product),
    );
    capabilities
}

fn canonical_problem_id(
    inputs: &ProblemInputIdentities,
    reconstruction: &ReconstructionContract,
    weighting: WeightingContract,
    products: &ProductRequirements,
    numerics: &NumericsContract,
) -> CompiledProblemId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(COMPILED_PROBLEM_IDENTITY_DOMAIN);
    encoder.u32(COMPILED_PROBLEM_IDENTITY_VERSION);
    encoder.identity(inputs.observation.0);
    encoder.identity(inputs.geometry.0);
    encoder.usize(inputs.reference_data.len());
    for (kind, identity) in &inputs.reference_data {
        encoder.u8(reference_data_tag(*kind));
        encoder.identity(*identity);
    }
    match inputs.model {
        ModelStateIdentity::Empty => encoder.u8(0),
        ModelStateIdentity::Seed(identity) => {
            encoder.u8(1);
            encoder.identity(identity);
        }
        ModelStateIdentity::Generation(identity) => {
            encoder.u8(2);
            encoder.identity(identity);
        }
    }
    match reconstruction.basis {
        ReconstructionBasis::Constant => encoder.u8(0),
        ReconstructionBasis::Taylor { terms } => {
            encoder.u8(1);
            encoder.usize(terms);
        }
        ReconstructionBasis::ChannelLocal { channels } => {
            encoder.u8(2);
            encoder.usize(channels);
        }
    }
    match &reconstruction.algorithm {
        ReconstructionAlgorithm::Dirty => encoder.u8(0),
        ReconstructionAlgorithm::Hogbom => encoder.u8(1),
        ReconstructionAlgorithm::Clark => encoder.u8(2),
        ReconstructionAlgorithm::Multiscale { scales_px } => {
            encoder.u8(3);
            encoder.usize(scales_px.len());
            for scale in scales_px {
                encoder.f64(*scale);
            }
        }
        ReconstructionAlgorithm::Mtmfs => encoder.u8(4),
    }
    encoder.usize(reconstruction.controls.max_minor_iterations);
    encoder.f64(reconstruction.controls.gain);
    encoder.f64(reconstruction.controls.threshold_jy_per_beam);
    match weighting.scheme {
        WeightingScheme::Natural => encoder.u8(0),
        WeightingScheme::Uniform => encoder.u8(1),
        WeightingScheme::Briggs { robust } => {
            encoder.u8(2);
            encoder.f64(robust);
        }
        WeightingScheme::BriggsBandwidthTaper { robust } => {
            encoder.u8(3);
            encoder.f64(robust);
        }
    }
    encoder.u8(match weighting.density_scope {
        WeightDensityScope::NotApplicable => 0,
        WeightDensityScope::GlobalSelection => 1,
        WeightDensityScope::PerOutputChannel => 2,
    });
    encoder.usize(products.products.len());
    for product in &products.products {
        encoder.u8(product_tag(*product));
    }
    encoder.u8(match products.normalization {
        ProductNormalization::UnitResponse => 0,
        ProductNormalization::FlatNoise => 1,
        ProductNormalization::FlatSky => 2,
    });
    encoder.u8(match products.restoring_beam {
        RestoringBeamPolicy::None => 0,
        RestoringBeamPolicy::PerPlane => 1,
        RestoringBeamPolicy::Common => 2,
    });
    encoder.usize(numerics.permitted_precisions.len());
    for precision in &numerics.permitted_precisions {
        encoder.u8(match precision {
            NumericPrecision::F32 => 0,
            NumericPrecision::F64 => 1,
        });
    }
    encoder.u8(match numerics.reduction {
        ReductionPolicy::DeterministicPairwise => 0,
        ReductionPolicy::Compensated => 1,
        ReductionPolicy::UnorderedWithinBudget => 2,
    });
    encoder.u8(match numerics.finite_values {
        FiniteValuePolicy::RejectAll => 0,
        FiniteValuePolicy::FlagInputRejectGenerated => 1,
    });
    for (stage, budget) in &numerics.stage_error_budgets {
        encoder.u8(numerical_stage_tag(*stage));
        encoder.f64(budget.absolute);
        encoder.f64(budget.relative);
    }
    CompiledProblemId(LogicalIdentity::from_sha256(encoder.finish()))
}

struct CanonicalEncoder(Sha256);

impl CanonicalEncoder {
    fn new() -> Self {
        Self(Sha256::new())
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    fn usize(&mut self, value: usize) {
        self.0.update((value as u128).to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    fn identity(&mut self, identity: LogicalIdentity) {
        self.0.update(identity.0);
    }

    fn f64(&mut self, value: f64) {
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.0.update(bits.to_le_bytes());
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn reference_data_tag(kind: ReferenceDataKind) -> u8 {
    match kind {
        ReferenceDataKind::Measures => 0,
        ReferenceDataKind::Ephemeris => 1,
        ReferenceDataKind::Observatory => 2,
        ReferenceDataKind::SpectralLines => 3,
        ReferenceDataKind::Instrument => 4,
    }
}

fn product_tag(product: ProductKind) -> u8 {
    match product {
        ProductKind::Psf => 0,
        ProductKind::Residual => 1,
        ProductKind::Model => 2,
        ProductKind::RestoredImage => 3,
        ProductKind::SumWeights => 4,
        ProductKind::Mask => 5,
        ProductKind::Weight => 6,
        ProductKind::PrimaryBeam => 7,
        ProductKind::Sensitivity => 8,
        ProductKind::PbCorrectedImage => 9,
        ProductKind::TaylorTerms => 10,
        ProductKind::SpectralIndex => 11,
        ProductKind::SpectralIndexError => 12,
        ProductKind::PbCorrectedSpectralIndex => 13,
        ProductKind::Beam => 14,
    }
}

fn numerical_stage_tag(stage: NumericalStage) -> u8 {
    match stage {
        NumericalStage::CoordinateTransforms => 0,
        NumericalStage::SpectralTransforms => 1,
        NumericalStage::Weighting => 2,
        NumericalStage::ForwardOperator => 3,
        NumericalStage::AdjointOperator => 4,
        NumericalStage::Reductions => 5,
        NumericalStage::Reconstruction => 6,
        NumericalStage::Restoration => 7,
        NumericalStage::ProductFormation => 8,
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
