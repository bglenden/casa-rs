// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;

use thiserror::Error;

use crate::geometry::{CompileGeometryError, CompiledGeometry, GeometryInput, compile_geometry};
use crate::measurement_equation::{
    DeclaredInnerProducts, NormalEquationContract, ProductNormalizationBoundary,
    WeightingOperatorContract, compile_normal_equation, compile_product_boundary,
};
use crate::model_state::{
    ModelContractError, ModelLifecycleContract, ModelLifecycleRequirements,
    compile_model_lifecycle_contract,
};
use crate::observation::ObservationSnapshot;
use crate::product_list::{ProductList, compile_product_list};
pub use crate::restoring_beam::RestoringBeamPolicy;
use crate::transaction::{
    ObservationTransactionCompileError, ObservationTransactionContract,
    ObservationTransactionRequirements, compile_observation_transaction,
};
pub use crate::weighting::{
    DensityFieldKeys, UvTaper, WeightDensityScope, WeightingContract, WeightingScheme,
};

mod measurement;
mod products;
mod reconstruction;

pub use measurement::AwProjectionContract;
pub use measurement::AwProjectionContractError;
pub use measurement::InstrumentModel;
pub use measurement::InstrumentResponse;
pub use measurement::MeasurementEquationContract;
pub use measurement::PolarizationContract;
pub use measurement::PolarizationCoordinate;
pub use measurement::ScientificContract;
pub use measurement::SpectralContract;
pub use measurement::SpectralCoupling;
pub use measurement::SpectralCovariance;
pub use measurement::SpectralEdgePolicy;
pub use measurement::SpectralKernel;
pub use measurement::SpectralSamplingLaw;
pub use measurement::WProjectionContract;
pub use measurement::WProjectionContractError;
pub use measurement::WStatistics;
pub use products::PrimaryBeamValidityPolicy;
pub use products::ProductBlankingPolicy;
pub use products::ProductKind;
pub use products::ProductNormalization;
pub use products::ProductRequirements;
pub use products::ProductSupportComparison;
pub use products::ProductValidityPolicies;
pub use products::ProductValidityPolicyError;
pub use products::TaylorSupportReference;
pub use products::TaylorValidityPolicy;
pub use products::UncorrectedImageMaskPolicy;
pub use reconstruction::CycleThresholdRule;
pub use reconstruction::HogbomIterationAccounting;
pub use reconstruction::NoiseEstimate;
pub use reconstruction::ReconstructionAlgorithm;
pub use reconstruction::ReconstructionBasis;
pub use reconstruction::ReconstructionContract;
pub use reconstruction::ReconstructionControls;

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
    /// Reject every non-finite value, flagged or not.
    RejectAll,
    /// Reject a non-finite input in an unflagged sample, and every
    /// generated non-finite value; ignore one in a flagged sample.
    ///
    /// A flagged sample is never imaged, so its values do not matter, as in
    /// CASA. An unflagged non-finite visibility is a different matter: CASA
    /// grids it, and one such sample turns the whole residual and image
    /// into NaN without a message. Failing the run says so at the first
    /// pass that reads it, so the data can be flagged first.
    RejectUnflaggedInput,
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
    science: ScientificContract,
    reconstruction: ReconstructionContract,
    weighting: WeightingContract,
    products: ProductRequirements,
    observation_transaction: ObservationTransactionRequirements,
    numerics: NumericsContract,
    visibility_transform: Option<crate::SequentialContinuumTransform>,
}

/// Everything [`compile`] needs: the logical specification, the geometry,
/// the selected observation and the model lifecycle.
#[derive(Debug, Clone, PartialEq)]
pub struct ProblemInput {
    specification: ProblemSpecification,
    geometry: GeometryInput,
    observation: ObservationSnapshot,
    model_lifecycle: ModelLifecycleRequirements,
}

impl ProblemInput {
    /// Gather the inputs of one compile.
    #[must_use]
    pub const fn new(
        specification: ProblemSpecification,
        geometry: GeometryInput,
        observation: ObservationSnapshot,
        model_lifecycle: ModelLifecycleRequirements,
    ) -> Self {
        Self {
            specification,
            geometry,
            observation,
            model_lifecycle,
        }
    }
}

impl ProblemSpecification {
    /// Construct a logical problem specification.
    #[must_use]
    pub const fn new(
        science: ScientificContract,
        reconstruction: ReconstructionContract,
        weighting: WeightingContract,
        products: ProductRequirements,
        observation_transaction: ObservationTransactionRequirements,
        numerics: NumericsContract,
    ) -> Self {
        Self {
            science,
            reconstruction,
            weighting,
            products,
            observation_transaction,
            numerics,
            visibility_transform: None,
        }
    }

    /// Compose one sequential visibility transform into the logical problem.
    #[must_use]
    pub fn with_visibility_transform(
        mut self,
        transform: crate::SequentialContinuumTransform,
    ) -> Self {
        self.visibility_transform = Some(transform);
        self
    }
}

/// Backend-independent capability required to plan and execute a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequiredCapability {
    /// More than one user-visible image-domain chart.
    MultiDomainGeometry,
    /// Multiple image-domain facets.
    FacetedGeometry,
    /// Paired W-projection convolution.
    WProjection,
    /// Paired prepared-convolution-function A/W projection.
    AwProjection,
    /// Spectral reference-frame transformation.
    SpectralFrameTransform,
    /// Non-identity paired spectral sampling.
    SpectralResampling,
    /// Sequential visibility-domain continuum subtraction.
    SequentialContinuumTransform,
    /// Common restoring-beam coupling across spectral planes.
    CommonBeamSpectralCoupling,
    /// Reconstruction of one polarization coordinate.
    Polarization(PolarizationCoordinate),
    /// Scalar primary-beam response.
    PrimaryBeamResponse,
    /// Full Mueller instrument response.
    FullMuellerResponse,
    /// Gaussian UV tapering in the data metric.
    UvTaper,
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
    /// Image-plane maximum-entropy reconstruction.
    MemReconstruction,
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
    /// CASA radial weighting.
    RadialWeighting,
    /// CASA absolute Briggs weighting.
    BriggsAbsoluteWeighting,
    /// Unit-response product normalization.
    UnitResponseNormalization,
    /// Flat-noise product normalization.
    FlatNoiseNormalization,
    /// Flat-sky product normalization.
    FlatSkyNormalization,
    /// CASA's `pbsquare` product normalization.
    PbSquareNormalization,
    /// Formation of a particular logical product.
    Product(ProductKind),
}

impl RequiredCapability {
    /// Return every stable compiler-derived capability in the current request
    /// catalog, including every polarization coordinate and logical product.
    #[must_use]
    pub fn catalog() -> Vec<Self> {
        let mut capabilities = vec![
            Self::MultiDomainGeometry,
            Self::FacetedGeometry,
            Self::WProjection,
            Self::AwProjection,
            Self::SpectralFrameTransform,
            Self::SpectralResampling,
            Self::SequentialContinuumTransform,
            Self::CommonBeamSpectralCoupling,
            Self::PrimaryBeamResponse,
            Self::FullMuellerResponse,
            Self::UvTaper,
            Self::ConstantBasis,
            Self::TaylorBasis,
            Self::ChannelLocalBasis,
            Self::DirtyReconstruction,
            Self::HogbomReconstruction,
            Self::ClarkReconstruction,
            Self::MemReconstruction,
            Self::MultiscaleReconstruction,
            Self::MtmfsReconstruction,
            Self::NaturalWeighting,
            Self::UniformWeighting,
            Self::BriggsWeighting,
            Self::BriggsBandwidthTaperWeighting,
            Self::RadialWeighting,
            Self::BriggsAbsoluteWeighting,
            Self::UnitResponseNormalization,
            Self::FlatNoiseNormalization,
            Self::FlatSkyNormalization,
            Self::PbSquareNormalization,
        ];
        capabilities.extend(Self::polarization_catalog());
        capabilities.extend(ProductKind::ALL.into_iter().map(Self::Product));
        capabilities
    }

    fn polarization_catalog() -> impl Iterator<Item = Self> {
        PolarizationCoordinate::ALL
            .into_iter()
            .map(Self::Polarization)
    }

    /// Return the stable request-catalog identity used by boundary projections.
    #[must_use]
    pub fn catalog_id(self) -> String {
        match self {
            Self::MultiDomainGeometry => "multi_domain_geometry".to_string(),
            Self::FacetedGeometry => "faceted_geometry".to_string(),
            Self::WProjection => "w_projection".to_string(),
            Self::AwProjection => "aw_projection".to_string(),
            Self::SpectralFrameTransform => "spectral_frame_transform".to_string(),
            Self::SpectralResampling => "spectral_resampling".to_string(),
            Self::SequentialContinuumTransform => "sequential_continuum_transform".to_string(),
            Self::CommonBeamSpectralCoupling => "common_beam_spectral_coupling".to_string(),
            Self::Polarization(coordinate) => {
                format!("polarization.{}", coordinate.catalog_id())
            }
            Self::PrimaryBeamResponse => "primary_beam_response".to_string(),
            Self::FullMuellerResponse => "full_mueller_response".to_string(),
            Self::UvTaper => "uv_taper".to_string(),
            Self::ConstantBasis => "constant_basis".to_string(),
            Self::TaylorBasis => "taylor_basis".to_string(),
            Self::ChannelLocalBasis => "channel_local_basis".to_string(),
            Self::DirtyReconstruction => "dirty_reconstruction".to_string(),
            Self::HogbomReconstruction => "hogbom_reconstruction".to_string(),
            Self::ClarkReconstruction => "clark_reconstruction".to_string(),
            Self::MemReconstruction => "mem_reconstruction".to_string(),
            Self::MultiscaleReconstruction => "multiscale_reconstruction".to_string(),
            Self::MtmfsReconstruction => "mtmfs_reconstruction".to_string(),
            Self::NaturalWeighting => "natural_weighting".to_string(),
            Self::UniformWeighting => "uniform_weighting".to_string(),
            Self::BriggsWeighting => "briggs_weighting".to_string(),
            Self::BriggsBandwidthTaperWeighting => "briggs_bandwidth_taper_weighting".to_string(),
            Self::RadialWeighting => "radial_weighting".to_string(),
            Self::BriggsAbsoluteWeighting => "briggs_absolute_weighting".to_string(),
            Self::UnitResponseNormalization => "unit_response_normalization".to_string(),
            Self::FlatNoiseNormalization => "flat_noise_normalization".to_string(),
            Self::FlatSkyNormalization => "flat_sky_normalization".to_string(),
            Self::PbSquareNormalization => "pb_square_normalization".to_string(),
            Self::Product(product) => format!("product.{}", product.catalog_id()),
        }
    }
}

/// Immutable logical problem accepted by downstream planning.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledProblem {
    model_lifecycle: ModelLifecycleContract,
    observation: ObservationSnapshot,
    geometry: CompiledGeometry,
    science: ScientificContract,
    reconstruction: ReconstructionContract,
    normal_equation: NormalEquationContract,
    products: ProductRequirements,
    product_list: ProductList,
    observation_transaction: ObservationTransactionContract,
    numerics: NumericsContract,
    required_capabilities: BTreeSet<RequiredCapability>,
    visibility_transform: Option<crate::SequentialContinuumTransform>,
}

impl CompiledProblem {
    /// Return the compiler-owned model-lifecycle commitment.
    #[must_use]
    pub const fn model_lifecycle(&self) -> &ModelLifecycleContract {
        &self.model_lifecycle
    }

    /// Return the selected observation the problem was compiled from.
    #[must_use]
    pub const fn observation(&self) -> &ObservationSnapshot {
        &self.observation
    }

    /// Return immutable compiler-owned geometry.
    #[must_use]
    pub const fn geometry(&self) -> &CompiledGeometry {
        &self.geometry
    }

    /// Return the complete science-owned logical contract.
    #[must_use]
    pub const fn science(&self) -> &ScientificContract {
        &self.science
    }

    /// Return reconstruction requirements.
    #[must_use]
    pub const fn reconstruction(&self) -> &ReconstructionContract {
        &self.reconstruction
    }

    /// Whether selected rows must evaluate physical feed-rotation coordinates.
    ///
    /// The ordinary Stokes-I operator has no parallactic-angle input. Polarized
    /// reconstruction and AW responses retain their physical angles;
    /// phase-centre and spectral-frame conversions have independent requirements.
    #[must_use]
    pub fn requires_parallactic_angles(&self) -> bool {
        self.reconstruction.polarization().coordinates() != [PolarizationCoordinate::StokesI]
            || self.science.instrument_model() == Some(InstrumentModel::CasaVlaAwplpgSquint)
            || self
                .science
                .measurement_equation()
                .aw_projection()
                .is_some()
    }

    /// Return the compiled positive-semidefinite data metric W.
    #[must_use]
    pub const fn weighting(&self) -> &WeightingOperatorContract {
        self.normal_equation.weighting()
    }

    /// Return the typed A/A*, W, b, g(x), and H contract.
    #[must_use]
    pub const fn normal_equation(&self) -> &NormalEquationContract {
        &self.normal_equation
    }

    /// Return product requirements.
    #[must_use]
    pub const fn products(&self) -> &ProductRequirements {
        &self.products
    }

    /// Return the product list: every product, how it is stored, and which
    /// are published.
    #[must_use]
    pub const fn product_list(&self) -> &ProductList {
        &self.product_list
    }

    /// Return exact snapshot-bound MeasurementSet read and write sets.
    #[must_use]
    pub const fn observation_transaction(&self) -> &ObservationTransactionContract {
        &self.observation_transaction
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

    /// Return the compiled sequential visibility transform, when present.
    #[must_use]
    pub const fn visibility_transform(&self) -> Option<&crate::SequentialContinuumTransform> {
        self.visibility_transform.as_ref()
    }
}

/// Failure to compile a logical imaging problem.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompileProblemError {
    /// Coordinate or image-domain geometry is invalid or incomplete.
    #[error(transparent)]
    Geometry(#[from] CompileGeometryError),
    /// The requested model lifecycle is incomplete or conflicts with the problem.
    #[error(transparent)]
    ModelLifecycle(#[from] ModelContractError),
    /// The requested MeasurementSet write contract is invalid for this snapshot.
    #[error(transparent)]
    ObservationTransaction(#[from] ObservationTransactionCompileError),
    /// Reconstruction and capability requirements contradict each other.
    #[error("invalid capability combination: {reason}")]
    InvalidCapabilityCombination {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Reconstruction-owned output coordinates are invalid.
    #[error("invalid reconstruction contract: {reason}")]
    InvalidReconstructionContract {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Channel-local reconstruction disagrees with the compiled output axis.
    #[error(
        "channel-local reconstruction requested {reconstruction_channels} channels but geometry compiles {geometry_channels}"
    )]
    SpectralChannelCountMismatch {
        /// Exact channel count compiled from spectral geometry.
        geometry_channels: usize,
        /// Exact channel count requested by reconstruction.
        reconstruction_channels: usize,
    },
    /// Spectral-sampling or measurement-equation requirements conflict.
    #[error("invalid scientific contract: {reason}")]
    InvalidScientificContract {
        /// Stable human-readable reason.
        reason: &'static str,
    },
    /// Weighting requirements are outside the logical domain.
    #[error("invalid weighting contract: {reason}")]
    InvalidWeighting {
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

/// Compile and validate one immutable backend-independent problem.
pub fn compile(input: ProblemInput) -> Result<CompiledProblem, CompileProblemError> {
    let ProblemInput {
        specification,
        geometry,
        observation,
        model_lifecycle,
    } = input;
    let geometry = compile_geometry(geometry)?;
    let visibility_transform = specification.visibility_transform;
    let science = specification.science;
    let products = specification.products.canonicalize();
    let observation_transaction = compile_observation_transaction(
        &observation,
        specification.observation_transaction,
        visibility_transform.as_ref(),
    )?;
    let numerics = specification.numerics.canonicalize()?;
    validate_science(&science)?;
    validate_reconstruction(&specification.reconstruction, &geometry)?;
    if visibility_transform.is_some()
        && !matches!(
            specification.reconstruction.basis(),
            ReconstructionBasis::ChannelLocal { .. }
        )
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "sequential visibility transforms require channel-local reconstruction",
        });
    }
    let reconstruction = specification.reconstruction.canonicalize()?;
    specification
        .weighting
        .validate()
        .map_err(|reason| CompileProblemError::InvalidWeighting { reason })?;
    validate_products(&science, &reconstruction, &products)?;
    let normal_equation = compile_normal_equation(
        &geometry,
        &observation,
        &science,
        &reconstruction,
        specification.weighting,
    );
    let mut required_capabilities = derive_capabilities(
        &geometry,
        &science,
        &reconstruction,
        normal_equation.weighting(),
        &products,
    );
    if visibility_transform.is_some() {
        required_capabilities.insert(RequiredCapability::SequentialContinuumTransform);
    }
    let product_list = compile_product_list(&geometry, &reconstruction, &products);
    let model_lifecycle = compile_model_lifecycle_contract(
        &geometry,
        normal_equation.measurement_operator().domain(),
        &numerics,
        model_lifecycle,
    )?;
    Ok(CompiledProblem {
        model_lifecycle,
        observation,
        geometry,
        science,
        reconstruction,
        normal_equation,
        products,
        product_list,
        observation_transaction,
        numerics,
        required_capabilities,
        visibility_transform,
    })
}

fn validate_science(science: &ScientificContract) -> Result<(), CompileProblemError> {
    if let SpectralKernel::ChannelIntegration { maximum_terms: 0 } =
        science.spectral.sampling.kernel()
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "spectral channel averaging requires a positive bin width",
        });
    }
    if !matches!(
        (
            science.measurement_equation.instrument_response,
            science.instrument_model,
        ),
        (InstrumentResponse::Scalar, None)
            | (
                InstrumentResponse::PrimaryBeam,
                Some(
                    InstrumentModel::CasaAca7mInterferometricDirectPbV1
                        | InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1
                        | InstrumentModel::CasaEvlaCommonInterferometricResponseV1
                        | InstrumentModel::CasaAiryHeterogeneousInterferometricResponseV1
                        | InstrumentModel::CasaEvlaWidebandAwV1
                        | InstrumentModel::CasaVlaAwplpg
                        | InstrumentModel::CasaVlaAwplpgSquint
                )
            )
    ) {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "instrument response and instrument model must form one supported exact pair",
        });
    }
    if science.measurement_equation.w_projection.is_some()
        && science.measurement_equation.aw_projection.is_some()
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "W projection and AW projection are distinct paired operators",
        });
    }
    if science.measurement_equation.aw_projection.is_some()
        && science.instrument_model != Some(InstrumentModel::CasaEvlaWidebandAwV1)
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "the paired AW contract requires the exact EVLA wideband instrument model",
        });
    }
    if science.measurement_equation.mosaic_conjugate_beams
        && !matches!(
            science.instrument_model,
            Some(
                InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1
                    | InstrumentModel::CasaEvlaCommonInterferometricResponseV1
                    | InstrumentModel::CasaAiryHeterogeneousInterferometricResponseV1
            )
        )
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "mosaic conjugate beams require a mosaic instrument model",
        });
    }
    Ok(())
}

fn validate_reconstruction(
    contract: &ReconstructionContract,
    geometry: &CompiledGeometry,
) -> Result<(), CompileProblemError> {
    match contract.basis {
        // CASA's `mtmfs` runs one Taylor term as a multi-term family
        // (`SIImageStoreMultiTerm` with `itsNTerms = 1`); every other
        // single-term MFS deconvolver uses the constant basis, which the
        // MT-MFS pairing check below keeps apart.
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
    if let ReconstructionBasis::ChannelLocal { channels } = contract.basis
        && channels != geometry.spectral().output_channels()
    {
        return Err(CompileProblemError::SpectralChannelCountMismatch {
            geometry_channels: geometry.spectral().output_channels(),
            reconstruction_channels: channels,
        });
    }
    if matches!(contract.algorithm, ReconstructionAlgorithm::Mtmfs { .. })
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
    if matches!(contract.algorithm, ReconstructionAlgorithm::Dirty)
        && (contract.controls.gain != 1.0 || contract.controls.threshold_jy_per_beam != 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "dirty reconstruction requires canonical inactive controls: gain 1 and threshold 0",
        });
    }
    if !matches!(contract.algorithm, ReconstructionAlgorithm::Hogbom)
        && contract.controls.hogbom_iteration_accounting != HogbomIterationAccounting::Strict
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "CASA-inclusive iteration accounting is specific to Högbom reconstruction",
        });
    }
    // CASA bounds neither the loop gain nor its sign: a negative gain makes
    // the residual grow until the divergence stop (`test_iterbot_divergence`).
    if !(contract.controls.gain.is_finite()
        && contract.controls.threshold_jy_per_beam.is_finite()
        && contract.controls.threshold_jy_per_beam >= 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "reconstruction gain and threshold must be finite and in their valid domains",
        });
    }
    if contract.controls.cycle_iteration_limit == Some(0)
        || contract
            .controls
            .noise_sigma
            .is_some_and(|sigma| !sigma.is_finite() || sigma < 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "minor-cycle limit must be positive and nsigma must be finite and non-negative",
        });
    }
    if contract.controls.cycle_threshold.is_some_and(|rule| {
        !(rule.factor.is_finite()
            && rule.factor > 0.0
            && rule.minimum_psf_fraction.is_finite()
            && rule.minimum_psf_fraction >= 0.0
            && rule.maximum_psf_fraction.is_finite()
            && rule.maximum_psf_fraction > 0.0)
    }) {
        // The fractions need not be ordered: CASA raises the fraction to
        // the minimum and then caps it at the maximum
        // (`grpcInteractiveCleanManager::updateCycleThreshold`), so the
        // maximum wins when they cross.
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "cycle threshold requires a positive factor and finite PSF fractions",
        });
    }
    if let ReconstructionAlgorithm::Multiscale {
        scales_px,
        small_scale_bias,
    }
    | ReconstructionAlgorithm::Mtmfs {
        scales_px,
        small_scale_bias,
    } = &contract.algorithm
    {
        if scales_px.is_empty()
            || scales_px
                .iter()
                .any(|scale| !(scale.is_finite() && *scale >= 0.0))
        {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "scale-aware reconstruction requires finite non-negative explicit scales",
            });
        }
        if !small_scale_bias.is_finite() || !(0.0..=1.0).contains(small_scale_bias) {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "scale-aware small-scale bias must be finite and in [0, 1]",
            });
        }
    }
    Ok(())
}

fn validate_products(
    science: &ScientificContract,
    reconstruction: &ReconstructionContract,
    products: &ProductRequirements,
) -> Result<(), CompileProblemError> {
    if products.products.is_empty() {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "at least one product must be requested",
        });
    }
    let restored_image_requested = products.contains(ProductKind::RestoredImage);
    let restoring_beam_requested = !matches!(products.restoring_beam, RestoringBeamPolicy::None);
    if restored_image_requested != restoring_beam_requested {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "restored-image and restoring-beam requirements must be requested together",
        });
    }
    if restored_image_requested
        && !(products.contains(ProductKind::Residual) && products.contains(ProductKind::Model))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a restored image requires residual and model products",
        });
    }
    let common_spectral_beam = science.spectral.coupling == SpectralCoupling::CommonRestoringBeam;
    let common_product_beam = products.restoring_beam == RestoringBeamPolicy::Common;
    if common_spectral_beam != common_product_beam {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "common spectral coupling and common restoring-beam publication must be requested together",
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
    if products.contains(ProductKind::TaylorTerms)
        && ![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::Weight,
            ProductKind::PrimaryBeam,
            ProductKind::PbCorrectedImage,
        ]
        .into_iter()
        .any(|product| products.contains(product))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a Taylor coefficient set requires at least one Taylor image product",
        });
    }
    // CASA forms a spectral index only for a Stokes I image
    // (`SIImageStoreMultiTerm::calculateAlphaBeta`'s `isOnlyStokesI`).
    if products.contains(ProductKind::SpectralIndex)
        && !(taylor_terms >= 2
            && reconstruction.polarization.coordinates() == [PolarizationCoordinate::StokesI]
            && products.contains(ProductKind::TaylorTerms)
            && products.contains(ProductKind::Residual)
            && products.contains(ProductKind::RestoredImage))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral index requires at least two Taylor terms of Stokes I alone plus Taylor, residual, and restored-image products",
        });
    }
    if products.contains(ProductKind::SpectralIndexError)
        && !products.contains(ProductKind::SpectralIndex)
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral-index uncertainty requires a spectral-index product",
        });
    }
    if products.contains(ProductKind::SpectralCurvature)
        && !(taylor_terms >= 3 && products.contains(ProductKind::SpectralIndex))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral curvature requires at least three Taylor terms and a spectral-index product",
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
    if products.contains(ProductKind::Beam)
        && ![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::RestoredImage,
        ]
        .into_iter()
        .any(|product| products.contains(product))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "beam metadata requires a PSF, residual, or restored-image product",
        });
    }
    Ok(())
}

fn derive_capabilities(
    geometry: &CompiledGeometry,
    science: &ScientificContract,
    reconstruction: &ReconstructionContract,
    weighting: &WeightingOperatorContract,
    products: &ProductRequirements,
) -> BTreeSet<RequiredCapability> {
    let mut capabilities = BTreeSet::new();
    if geometry.domains().len() > 1 {
        capabilities.insert(RequiredCapability::MultiDomainGeometry);
    }
    if geometry
        .domains()
        .iter()
        .any(|domain| domain.facets().len() > 1)
    {
        capabilities.insert(RequiredCapability::FacetedGeometry);
    }
    if geometry.spectral().source_frame() != geometry.spectral().output_frame() {
        capabilities.insert(RequiredCapability::SpectralFrameTransform);
    }
    if science.spectral.sampling != SpectralSamplingLaw::IDENTITY {
        capabilities.insert(RequiredCapability::SpectralResampling);
    }
    if science.measurement_equation.w_projection.is_some() {
        capabilities.insert(RequiredCapability::WProjection);
    }
    if science.measurement_equation.aw_projection.is_some() {
        capabilities.insert(RequiredCapability::AwProjection);
    }
    if science.spectral.coupling == SpectralCoupling::CommonRestoringBeam {
        capabilities.insert(RequiredCapability::CommonBeamSpectralCoupling);
    }
    capabilities.extend(
        reconstruction
            .polarization
            .coordinates
            .iter()
            .copied()
            .map(RequiredCapability::Polarization),
    );
    match science.measurement_equation.instrument_response {
        InstrumentResponse::Scalar => {}
        InstrumentResponse::PrimaryBeam => {
            capabilities.insert(RequiredCapability::PrimaryBeamResponse);
        }
        InstrumentResponse::FullMueller => {
            capabilities.insert(RequiredCapability::FullMuellerResponse);
        }
    }
    if weighting.uv_taper().is_some() {
        capabilities.insert(RequiredCapability::UvTaper);
    }
    capabilities.insert(match reconstruction.basis {
        ReconstructionBasis::Constant => RequiredCapability::ConstantBasis,
        ReconstructionBasis::Taylor { .. } => RequiredCapability::TaylorBasis,
        ReconstructionBasis::ChannelLocal { .. } => RequiredCapability::ChannelLocalBasis,
    });
    capabilities.insert(match reconstruction.algorithm {
        ReconstructionAlgorithm::Dirty => RequiredCapability::DirtyReconstruction,
        ReconstructionAlgorithm::Hogbom => RequiredCapability::HogbomReconstruction,
        ReconstructionAlgorithm::Clark | ReconstructionAlgorithm::ClarkStokes => {
            RequiredCapability::ClarkReconstruction
        }
        ReconstructionAlgorithm::Mem => RequiredCapability::MemReconstruction,
        ReconstructionAlgorithm::Multiscale { .. } => RequiredCapability::MultiscaleReconstruction,
        ReconstructionAlgorithm::Mtmfs { .. } => RequiredCapability::MtmfsReconstruction,
    });
    capabilities.insert(match weighting.scheme() {
        WeightingScheme::Natural => RequiredCapability::NaturalWeighting,
        WeightingScheme::Uniform => RequiredCapability::UniformWeighting,
        WeightingScheme::Briggs { .. } => RequiredCapability::BriggsWeighting,
        WeightingScheme::BriggsBandwidthTaper { .. } => {
            RequiredCapability::BriggsBandwidthTaperWeighting
        }
        WeightingScheme::Radial => RequiredCapability::RadialWeighting,
        WeightingScheme::BriggsAbsolute { .. } => RequiredCapability::BriggsAbsoluteWeighting,
    });
    capabilities.insert(match products.normalization {
        ProductNormalization::UnitResponse => RequiredCapability::UnitResponseNormalization,
        ProductNormalization::FlatNoise => RequiredCapability::FlatNoiseNormalization,
        ProductNormalization::FlatSky => RequiredCapability::FlatSkyNormalization,
        ProductNormalization::PbSquare => RequiredCapability::PbSquareNormalization,
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
