// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeSet, fmt};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::geometry::{CompileGeometryError, CompiledGeometry, GeometryInput, compile_geometry};
use crate::measurement_equation::{
    DeclaredInnerProducts, ModelInnerProduct, NormalEquationContract, NormalStateNormalization,
    PairedMeasurementTransform, ProductBoundaryOperation, ProductNormalizationBoundary,
    VisibilityInnerProduct, WeightingOperatorContract, compile_normal_equation,
    compile_product_boundary,
};
use crate::model_state::{
    ModelContractError, ModelLifecycleContract, ModelLifecycleRequirements,
    compile_model_lifecycle_contract,
};
use crate::observation::{FlagPolicy, ObservationSnapshot, ObservationSnapshotId, WeightColumn};
use crate::product_graph::{ProductGraph, compile_product_graph};
use crate::selected_observation::{
    SelectedObservationCommitment, SelectedObservationPassError,
    compile_selected_observation_commitment, inspect_selected_observation,
};
use crate::selected_observation_sample::{
    SelectedObservationGenerationId, SelectedObservationSample,
};
use crate::transaction::{
    ObservationTransactionContract, ObservationTransactionRequirements,
    compile_observation_transaction,
};

const COMPILED_PROBLEM_IDENTITY_DOMAIN: &[u8] = b"casa-rs-compiled-problem";
const COMPILED_PROBLEM_IDENTITY_VERSION: u32 = 9;
const COMPILED_PROBLEM_BASIS_DOMAIN: &[u8] = b"casa-rs-compiled-problem-basis";
const COMPILED_PROBLEM_BASIS_VERSION: u32 = 1;
const NUMERICS_CONTRACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-numerics-contract";
const NUMERICS_CONTRACT_IDENTITY_VERSION: u32 = 1;

/// Version of the sole native imaging request contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImagingRequestVersion {
    /// Contract with compiler-owned immutable geometry and exact product validity.
    V3,
}

impl ImagingRequestVersion {
    /// Current request version accepted by [`compile`].
    pub const CURRENT: Self = Self::V3;

    /// Return the stable integer representation used at transport boundaries.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::V3 => 3,
        }
    }
}

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
    observation: ObservationSnapshot,
}

impl ProblemInputIdentities {
    /// Bind a compiler-derived observation snapshot as the sole input authority.
    #[must_use]
    pub const fn new(observation: ObservationSnapshot) -> Self {
        Self { observation }
    }

    /// Return the observation snapshot identity.
    #[must_use]
    pub const fn observation(&self) -> ObservationSnapshotId {
        self.observation.snapshot_id()
    }

    /// Return the complete immutable observation snapshot.
    #[must_use]
    pub const fn observation_snapshot(&self) -> &ObservationSnapshot {
        &self.observation
    }

    /// Return reference identities in canonical family order after compilation.
    #[must_use]
    pub fn reference_data(&self) -> &[(ReferenceDataKind, LogicalIdentity)] {
        self.observation.reference_data()
    }

    /// Return the initial model-state identity.
    #[must_use]
    pub const fn model(&self) -> ModelStateIdentity {
        self.observation.model()
    }
}

/// Paired spectral sampling used in prediction and adjoint imaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralSampling {
    /// Preserve native channel samples exactly.
    Identity,
    /// Nearest covered source channel.
    Nearest,
    /// Linear paired interpolation.
    Linear,
    /// Integrate a fixed number of adjacent channels into each output bin.
    ChannelAverage {
        /// Number of source channels in each bin.
        channels_per_bin: usize,
    },
}

/// Scientific coupling between reconstructed spectral planes or coefficients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralCoupling {
    /// Planes or coefficients have no shared product constraint.
    Independent,
    /// Published planes share one common restoring beam.
    CommonRestoringBeam,
}

/// Spectral coordinate, sampling, and cross-plane requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpectralContract {
    sampling: SpectralSampling,
    coupling: SpectralCoupling,
}

impl SpectralContract {
    /// Construct spectral requirements.
    #[must_use]
    pub const fn new(sampling: SpectralSampling, coupling: SpectralCoupling) -> Self {
        Self { sampling, coupling }
    }

    /// Return paired spectral sampling semantics.
    #[must_use]
    pub const fn sampling(self) -> SpectralSampling {
        self.sampling
    }

    /// Return spectral coupling semantics.
    #[must_use]
    pub const fn coupling(self) -> SpectralCoupling {
        self.coupling
    }
}

/// Requested reconstruction coordinate in Stokes or correlation space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolarizationCoordinate {
    /// Stokes I.
    StokesI,
    /// Stokes Q.
    StokesQ,
    /// Stokes U.
    StokesU,
    /// Stokes V.
    StokesV,
    /// Linear-feed XX correlation.
    LinearXx,
    /// Linear-feed XY correlation.
    LinearXy,
    /// Linear-feed YX correlation.
    LinearYx,
    /// Linear-feed YY correlation.
    LinearYy,
    /// Circular-feed RR correlation.
    CircularRr,
    /// Circular-feed RL correlation.
    CircularRl,
    /// Circular-feed LR correlation.
    CircularLr,
    /// Circular-feed LL correlation.
    CircularLl,
}

/// Requested polarization reconstruction coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolarizationContract {
    coordinates: Vec<PolarizationCoordinate>,
}

impl PolarizationContract {
    /// Construct requested coordinates. Compilation canonicalizes ordering.
    #[must_use]
    pub const fn new(coordinates: Vec<PolarizationCoordinate>) -> Self {
        Self { coordinates }
    }

    /// Return canonical requested coordinates after compilation.
    #[must_use]
    pub fn coordinates(&self) -> &[PolarizationCoordinate] {
        &self.coordinates
    }

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.coordinates.sort_unstable();
        self.coordinates.dedup();
        if self.coordinates.is_empty() {
            return Err(CompileProblemError::InvalidReconstructionContract {
                reason: "at least one polarization coordinate must be requested",
            });
        }
        let categories = self
            .coordinates
            .iter()
            .fold([false; 3], |mut present, coordinate| {
                match coordinate {
                    PolarizationCoordinate::StokesI
                    | PolarizationCoordinate::StokesQ
                    | PolarizationCoordinate::StokesU
                    | PolarizationCoordinate::StokesV => present[0] = true,
                    PolarizationCoordinate::LinearXx
                    | PolarizationCoordinate::LinearXy
                    | PolarizationCoordinate::LinearYx
                    | PolarizationCoordinate::LinearYy => present[1] = true,
                    PolarizationCoordinate::CircularRr
                    | PolarizationCoordinate::CircularRl
                    | PolarizationCoordinate::CircularLr
                    | PolarizationCoordinate::CircularLl => present[2] = true,
                }
                present
            });
        if categories.into_iter().filter(|present| *present).count() > 1 {
            return Err(CompileProblemError::InvalidReconstructionContract {
                reason: "one reconstruction cannot mix Stokes, linear, and circular coordinates",
            });
        }
        Ok(self)
    }
}

/// Direction-dependent instrument response included in the measurement equation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentResponse {
    /// Direction-independent scalar response.
    Scalar,
    /// Scalar primary-beam response.
    PrimaryBeam,
    /// Full polarization Mueller response.
    FullMueller,
}

/// Logical measurement-equation terms independent of an implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurementEquationContract {
    instrument_response: InstrumentResponse,
    inner_products: DeclaredInnerProducts,
}

impl MeasurementEquationContract {
    /// Construct measurement-equation requirements.
    #[must_use]
    pub const fn new(
        instrument_response: InstrumentResponse,
        inner_products: DeclaredInnerProducts,
    ) -> Self {
        Self {
            instrument_response,
            inner_products,
        }
    }

    /// Return the required instrument response.
    #[must_use]
    pub const fn instrument_response(self) -> InstrumentResponse {
        self.instrument_response
    }

    /// Return the model and visibility inner products defining the adjoint.
    #[must_use]
    pub const fn inner_products(self) -> DeclaredInnerProducts {
        self.inner_products
    }
}

/// Complete science-owned contract outside reconstruction, weighting, and products.
#[derive(Debug, Clone, PartialEq)]
pub struct ScientificContract {
    spectral: SpectralContract,
    measurement_equation: MeasurementEquationContract,
}

impl ScientificContract {
    /// Construct a complete logical scientific contract.
    #[must_use]
    pub const fn new(
        spectral: SpectralContract,
        measurement_equation: MeasurementEquationContract,
    ) -> Self {
        Self {
            spectral,
            measurement_equation,
        }
    }

    /// Return spectral requirements.
    #[must_use]
    pub const fn spectral(&self) -> SpectralContract {
        self.spectral
    }

    /// Return measurement-equation requirements.
    #[must_use]
    pub const fn measurement_equation(&self) -> MeasurementEquationContract {
        self.measurement_equation
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
    polarization: PolarizationContract,
}

impl ReconstructionContract {
    /// Construct reconstruction requirements.
    #[must_use]
    pub const fn new(
        basis: ReconstructionBasis,
        algorithm: ReconstructionAlgorithm,
        controls: ReconstructionControls,
        polarization: PolarizationContract,
    ) -> Self {
        Self {
            basis,
            algorithm,
            controls,
            polarization,
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

    /// Return reconstruction-owned polarization coordinates.
    #[must_use]
    pub const fn polarization(&self) -> &PolarizationContract {
        &self.polarization
    }

    fn canonicalize(mut self) -> Result<Self, CompileProblemError> {
        self.polarization = self.polarization.canonicalize()?;
        if let ReconstructionAlgorithm::Multiscale { scales_px } = &mut self.algorithm {
            for scale in scales_px.iter_mut() {
                if *scale == 0.0 {
                    *scale = 0.0;
                }
            }
            scales_px.sort_unstable_by(|left, right| left.total_cmp(right));
            scales_px.dedup();
        }
        Ok(self)
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

/// Gaussian taper in the UV plane, expressed in wavelengths and radians.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvTaper {
    major_lambda: f64,
    minor_lambda: f64,
    position_angle_rad: f64,
}

impl UvTaper {
    /// Construct a Gaussian UV taper.
    #[must_use]
    pub const fn new(major_lambda: f64, minor_lambda: f64, position_angle_rad: f64) -> Self {
        Self {
            major_lambda,
            minor_lambda,
            position_angle_rad,
        }
    }

    /// Return the major-axis scale in wavelengths.
    #[must_use]
    pub const fn major_lambda(self) -> f64 {
        self.major_lambda
    }

    /// Return the minor-axis scale in wavelengths.
    #[must_use]
    pub const fn minor_lambda(self) -> f64 {
        self.minor_lambda
    }

    /// Return the position angle in radians.
    #[must_use]
    pub const fn position_angle_rad(self) -> f64 {
        self.position_angle_rad
    }
}

/// Complete logical visibility-weighting requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightingContract {
    scheme: WeightingScheme,
    density_scope: WeightDensityScope,
    uv_taper: Option<UvTaper>,
}

impl WeightingContract {
    /// Construct weighting requirements.
    #[must_use]
    pub const fn new(scheme: WeightingScheme, density_scope: WeightDensityScope) -> Self {
        Self {
            scheme,
            density_scope,
            uv_taper: None,
        }
    }

    /// Add a Gaussian UV taper to the weighting metric.
    #[must_use]
    pub const fn with_uv_taper(mut self, uv_taper: UvTaper) -> Self {
        self.uv_taper = Some(uv_taper);
        self
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

    /// Return the optional Gaussian UV taper.
    #[must_use]
    pub const fn uv_taper(self) -> Option<UvTaper> {
        self.uv_taper
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

/// Comparison used to decide whether a product pixel has valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductSupportComparison {
    /// The measured support must be strictly greater than the configured threshold.
    StrictlyGreater,
}

/// Persisted treatment of pixels outside a product's valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductBlankingPolicy {
    /// Store numeric zero and mark the corresponding validity mask false.
    ZeroAndFalseMask,
}

/// Reference statistic used by the MT-MFS Taylor-support threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaylorSupportReference {
    /// Positive maximum of the temporary principal-solution Taylor-zero residual.
    PrincipalResidualTaylor0PositiveMaximum,
}

/// Failure to construct a finite product-validity policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProductValidityPolicyError {
    /// Primary-beam support requires a finite positive cutoff.
    #[error("primary-beam support cutoff must be finite and positive")]
    InvalidPrimaryBeamCutoff,
    /// Taylor support requires a finite fraction in `(0, 1]`.
    #[error("Taylor support peak fraction must be finite, positive, and at most one")]
    InvalidTaylorPeakFraction,
}

/// Exact primary-beam support and blanking policy carried by a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryBeamValidityPolicy {
    cutoff_bits: u32,
    comparison: ProductSupportComparison,
    blanking: ProductBlankingPolicy,
}

impl PrimaryBeamValidityPolicy {
    /// Construct an exact finite primary-beam support policy.
    pub fn new(
        cutoff: f32,
        comparison: ProductSupportComparison,
        blanking: ProductBlankingPolicy,
    ) -> Result<Self, ProductValidityPolicyError> {
        if !(cutoff.is_finite() && cutoff > 0.0) {
            return Err(ProductValidityPolicyError::InvalidPrimaryBeamCutoff);
        }
        Ok(Self {
            cutoff_bits: cutoff.to_bits(),
            comparison,
            blanking,
        })
    }

    /// Return the exact primary-beam cutoff.
    #[must_use]
    pub fn cutoff(self) -> f32 {
        f32::from_bits(self.cutoff_bits)
    }

    /// Return the exact support comparison.
    #[must_use]
    pub const fn comparison(self) -> ProductSupportComparison {
        self.comparison
    }

    /// Return the exact persisted treatment outside support.
    #[must_use]
    pub const fn blanking(self) -> ProductBlankingPolicy {
        self.blanking
    }
}

/// Exact Taylor-coefficient support and blanking policy carried by a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaylorValidityPolicy {
    reference: TaylorSupportReference,
    peak_fraction_bits: u32,
    comparison: ProductSupportComparison,
    blanking: ProductBlankingPolicy,
}

impl TaylorValidityPolicy {
    /// Construct an exact finite Taylor-support policy.
    pub fn new(
        reference: TaylorSupportReference,
        peak_fraction: f32,
        comparison: ProductSupportComparison,
        blanking: ProductBlankingPolicy,
    ) -> Result<Self, ProductValidityPolicyError> {
        if !(peak_fraction.is_finite() && peak_fraction > 0.0 && peak_fraction <= 1.0) {
            return Err(ProductValidityPolicyError::InvalidTaylorPeakFraction);
        }
        Ok(Self {
            reference,
            peak_fraction_bits: peak_fraction.to_bits(),
            comparison,
            blanking,
        })
    }

    /// Return the reference statistic for the Taylor threshold.
    #[must_use]
    pub const fn reference(self) -> TaylorSupportReference {
        self.reference
    }

    /// Return the fraction applied to the reference statistic.
    #[must_use]
    pub fn peak_fraction(self) -> f32 {
        f32::from_bits(self.peak_fraction_bits)
    }

    /// Return the exact support comparison.
    #[must_use]
    pub const fn comparison(self) -> ProductSupportComparison {
        self.comparison
    }

    /// Return the exact persisted treatment outside support.
    #[must_use]
    pub const fn blanking(self) -> ProductBlankingPolicy {
        self.blanking
    }
}

/// Exact compiler-owned validity policies for all requested products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductValidityPolicies {
    primary_beam: PrimaryBeamValidityPolicy,
    taylor: TaylorValidityPolicy,
}

impl ProductValidityPolicies {
    /// Bind the exact primary-beam and Taylor validity policies into a request.
    #[must_use]
    pub const fn new(
        primary_beam: PrimaryBeamValidityPolicy,
        taylor: TaylorValidityPolicy,
    ) -> Self {
        Self {
            primary_beam,
            taylor,
        }
    }

    /// Return the primary-beam support policy.
    #[must_use]
    pub const fn primary_beam(self) -> PrimaryBeamValidityPolicy {
        self.primary_beam
    }

    /// Return the Taylor-coefficient support policy.
    #[must_use]
    pub const fn taylor(self) -> TaylorValidityPolicy {
        self.taylor
    }
}

/// Requested product set and publication semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductRequirements {
    products: Vec<ProductKind>,
    normalization: ProductNormalization,
    restoring_beam: RestoringBeamPolicy,
    validity: ProductValidityPolicies,
    normalization_boundary: ProductNormalizationBoundary,
}

impl ProductRequirements {
    /// Construct product requirements. Compilation canonicalizes product ordering.
    #[must_use]
    pub fn new(
        products: Vec<ProductKind>,
        normalization: ProductNormalization,
        restoring_beam: RestoringBeamPolicy,
        validity: ProductValidityPolicies,
    ) -> Self {
        let normalization_boundary =
            compile_product_boundary(&products, normalization, restoring_beam);
        Self {
            products,
            normalization,
            restoring_beam,
            validity,
            normalization_boundary,
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

    /// Return the exact product-validity policies supplied by the request.
    #[must_use]
    pub const fn validity(&self) -> ProductValidityPolicies {
        self.validity
    }

    /// Return the downstream handoff that keeps product operations outside A*.
    #[must_use]
    pub const fn normalization_boundary(&self) -> &ProductNormalizationBoundary {
        &self.normalization_boundary
    }

    fn canonicalize(mut self) -> Self {
        self.products.sort_unstable();
        self.products.dedup();
        self.normalization_boundary =
            compile_product_boundary(&self.products, self.normalization, self.restoring_beam);
        self
    }

    pub(crate) fn contains(&self, product: ProductKind) -> bool {
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
    science: ScientificContract,
    reconstruction: ReconstructionContract,
    weighting: WeightingContract,
    products: ProductRequirements,
    observation_transaction: ObservationTransactionRequirements,
    numerics: NumericsContract,
}

/// One versioned, backend-independent native imaging request.
#[derive(Debug, Clone, PartialEq)]
pub struct ImagingRequest {
    version: ImagingRequestVersion,
    specification: ProblemSpecification,
    geometry: GeometryInput,
    inputs: ProblemInputIdentities,
    model_lifecycle: ModelLifecycleRequirements,
}

impl ImagingRequest {
    /// Construct a request in the current native contract version.
    #[must_use]
    pub const fn new(
        specification: ProblemSpecification,
        geometry: GeometryInput,
        inputs: ProblemInputIdentities,
        model_lifecycle: ModelLifecycleRequirements,
    ) -> Self {
        Self {
            version: ImagingRequestVersion::CURRENT,
            specification,
            geometry,
            inputs,
            model_lifecycle,
        }
    }

    /// Return the exact request contract version.
    #[must_use]
    pub const fn version(&self) -> ImagingRequestVersion {
        self.version
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
        }
    }
}

/// Backend-independent capability required to plan and execute a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequiredCapability {
    /// Multiple image-domain facets.
    FacetedGeometry,
    /// Spectral reference-frame transformation.
    SpectralFrameTransform,
    /// Non-identity paired spectral sampling.
    SpectralResampling,
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

/// Stable comparable identity of a complete numerical contract.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NumericsContractId(LogicalIdentity);

impl NumericsContractId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = NUMERICS_CONTRACT_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for NumericsContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NumericsContractId(")?;
        write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

impl fmt::Display for NumericsContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.as_bytes())
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
    problem_identity_basis: LogicalIdentity,
    numerics_id: NumericsContractId,
    model_lifecycle: ModelLifecycleContract,
    inputs: ProblemInputIdentities,
    geometry: CompiledGeometry,
    science: ScientificContract,
    reconstruction: ReconstructionContract,
    normal_equation: NormalEquationContract,
    products: ProductRequirements,
    product_graph: ProductGraph,
    observation_transaction: ObservationTransactionContract,
    selected_observation: SelectedObservationCommitment,
    numerics: NumericsContract,
    required_capabilities: BTreeSet<RequiredCapability>,
}

impl CompiledProblem {
    /// Return the canonical comparable identity.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the compiler-owned identity beneath the explicit model/lifecycle layer.
    ///
    /// Receipt readers combine this basis with the typed initial model and
    /// lifecycle commitment to revalidate the parent Compiled Problem identity.
    #[must_use]
    pub const fn problem_identity_basis(&self) -> LogicalIdentity {
        self.problem_identity_basis
    }

    /// Return the exact numerical-contract identity.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics_id
    }

    /// Return the compiler-owned model-lifecycle commitment.
    #[must_use]
    pub const fn model_lifecycle(&self) -> &ModelLifecycleContract {
        &self.model_lifecycle
    }

    /// Return immutable input identities.
    #[must_use]
    pub const fn inputs(&self) -> &ProblemInputIdentities {
        &self.inputs
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

    /// Return the mandatory compiler-owned product topology and publication contract.
    #[must_use]
    pub const fn product_graph(&self) -> &ProductGraph {
        &self.product_graph
    }

    /// Return exact snapshot-bound MeasurementSet read and write sets.
    #[must_use]
    pub const fn observation_transaction(&self) -> &ObservationTransactionContract {
        &self.observation_transaction
    }

    /// Return the sole compiler-owned selected-observation commitment.
    #[must_use]
    pub const fn selected_observation(&self) -> &SelectedObservationCommitment {
        &self.selected_observation
    }

    /// Validate, consume, and identify one canonical selected-observation sample pass.
    ///
    /// Each sample is scientifically validated before reaching `consume`. The
    /// inspection state and content-identity encoder never escape this closed
    /// call. This does not prove retained source access or mint completion.
    pub fn inspect_selected_observation<E>(
        &self,
        samples: impl IntoIterator<Item = Result<SelectedObservationSample, E>>,
        consume: impl FnMut(SelectedObservationSample) -> Result<(), E>,
    ) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationPassError<E>> {
        inspect_selected_observation(
            &self.selected_observation,
            self.observation_transaction.write_set(),
            samples,
            consume,
        )
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
    /// Coordinate or image-domain geometry is invalid or incomplete.
    #[error(transparent)]
    Geometry(#[from] CompileGeometryError),
    /// The requested model lifecycle is incomplete or conflicts with the problem.
    #[error(transparent)]
    ModelLifecycle(#[from] ModelContractError),
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

/// Compile and validate one immutable backend-independent imaging request.
pub fn compile(request: ImagingRequest) -> Result<CompiledProblem, CompileProblemError> {
    let ImagingRequest {
        version: ImagingRequestVersion::V3,
        specification,
        geometry,
        inputs,
        model_lifecycle,
    } = request;
    let geometry = compile_geometry(geometry, &inputs)?;
    let science = specification.science;
    let products = specification.products.canonicalize();
    let observation_transaction = compile_observation_transaction(
        inputs.observation_snapshot(),
        specification.observation_transaction,
    );
    let numerics = specification.numerics.canonicalize()?;
    validate_science(&science, &inputs)?;
    validate_reconstruction(&specification.reconstruction, &geometry)?;
    let reconstruction = specification.reconstruction.canonicalize()?;
    validate_weighting(specification.weighting)?;
    validate_products(&science, &reconstruction, &products)?;
    let normal_equation = compile_normal_equation(
        &geometry,
        &inputs,
        &science,
        &reconstruction,
        specification.weighting,
    );
    let selected_observation = compile_selected_observation_commitment(
        &observation_transaction,
        geometry.geometry_id(),
        normal_equation
            .measurement_operator()
            .codomain()
            .inner_product(),
        science.spectral().sampling(),
    );
    let required_capabilities = derive_capabilities(
        &geometry,
        &science,
        &reconstruction,
        normal_equation.weighting(),
        &products,
    );
    let numerics_id = canonical_numerics_id(&numerics);
    let product_graph = compile_product_graph(&geometry, &reconstruction, &products);
    let model_lifecycle = compile_model_lifecycle_contract(
        &geometry,
        normal_equation.measurement_operator().domain(),
        &inputs,
        &numerics,
        numerics_id,
        product_graph.graph_id(),
        model_lifecycle,
    )?;
    let problem_identity_basis = canonical_problem_identity_basis(ProblemIdentityInput {
        inputs: &inputs,
        geometry: &geometry,
        science: &science,
        reconstruction: &reconstruction,
        normal_equation: &normal_equation,
        products: &products,
        observation_transaction: &observation_transaction,
        numerics: &numerics,
    });
    let problem_id = canonical_problem_id(
        problem_identity_basis,
        inputs.model(),
        LogicalIdentity::from_sha256(model_lifecycle.contract_id().as_bytes()),
    );
    Ok(CompiledProblem {
        problem_id,
        problem_identity_basis,
        numerics_id,
        model_lifecycle,
        inputs,
        geometry,
        science,
        reconstruction,
        normal_equation,
        products,
        product_graph,
        observation_transaction,
        selected_observation,
        numerics,
        required_capabilities,
    })
}

fn validate_science(
    science: &ScientificContract,
    inputs: &ProblemInputIdentities,
) -> Result<(), CompileProblemError> {
    if let SpectralSampling::ChannelAverage {
        channels_per_bin: 0,
    } = science.spectral.sampling
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "spectral channel averaging requires a positive bin width",
        });
    }
    if science.measurement_equation.instrument_response != InstrumentResponse::Scalar
        && !inputs
            .reference_data()
            .iter()
            .any(|(kind, _)| *kind == ReferenceDataKind::Instrument)
    {
        return Err(CompileProblemError::InvalidScientificContract {
            reason: "direction-dependent response requires bound instrument reference data",
        });
    }
    Ok(())
}

fn validate_reconstruction(
    contract: &ReconstructionContract,
    geometry: &CompiledGeometry,
) -> Result<(), CompileProblemError> {
    match contract.basis {
        ReconstructionBasis::Taylor { terms: 0 | 1 } => {
            return Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "a Taylor basis requires at least two terms; single-term MFS uses the constant basis",
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
    if matches!(contract.algorithm, ReconstructionAlgorithm::Dirty)
        && (contract.controls.gain != 1.0 || contract.controls.threshold_jy_per_beam != 0.0)
    {
        return Err(CompileProblemError::InvalidCapabilityCombination {
            reason: "dirty reconstruction requires canonical inactive controls: gain 1 and threshold 0",
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
    if contract.uv_taper.is_some_and(|taper| {
        !(taper.major_lambda.is_finite()
            && taper.major_lambda > 0.0
            && taper.minor_lambda.is_finite()
            && taper.minor_lambda > 0.0
            && taper.position_angle_rad.is_finite())
    }) {
        return Err(CompileProblemError::InvalidWeighting {
            reason: "UV taper axes must be finite and positive and its angle must be finite",
        });
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
    if products.contains(ProductKind::PrimaryBeam)
        && science.measurement_equation.instrument_response == InstrumentResponse::Scalar
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "a primary-beam product requires a primary-beam or full-Mueller response",
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
    if products.contains(ProductKind::SpectralIndex)
        && !(taylor_terms >= 2
            && products.contains(ProductKind::TaylorTerms)
            && products.contains(ProductKind::Residual)
            && products.contains(ProductKind::RestoredImage))
    {
        return Err(CompileProblemError::InvalidProductCombination {
            reason: "spectral index requires at least two Taylor terms plus Taylor, residual, and restored-image products",
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
    if science.spectral.sampling != SpectralSampling::Identity {
        capabilities.insert(RequiredCapability::SpectralResampling);
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
        ReconstructionAlgorithm::Clark => RequiredCapability::ClarkReconstruction,
        ReconstructionAlgorithm::Multiscale { .. } => RequiredCapability::MultiscaleReconstruction,
        ReconstructionAlgorithm::Mtmfs => RequiredCapability::MtmfsReconstruction,
    });
    capabilities.insert(match weighting.scheme() {
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

struct ProblemIdentityInput<'a> {
    inputs: &'a ProblemInputIdentities,
    geometry: &'a CompiledGeometry,
    science: &'a ScientificContract,
    reconstruction: &'a ReconstructionContract,
    normal_equation: &'a NormalEquationContract,
    products: &'a ProductRequirements,
    observation_transaction: &'a ObservationTransactionContract,
    numerics: &'a NumericsContract,
}

fn canonical_problem_identity_basis(input: ProblemIdentityInput<'_>) -> LogicalIdentity {
    let ProblemIdentityInput {
        inputs,
        geometry,
        science,
        reconstruction,
        normal_equation,
        products,
        observation_transaction,
        numerics,
    } = input;
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(COMPILED_PROBLEM_BASIS_DOMAIN);
    encoder.u32(COMPILED_PROBLEM_BASIS_VERSION);
    encoder.identity(inputs.observation().identity());
    encoder.digest(observation_transaction.transaction_id().as_bytes());
    encoder.digest(geometry.geometry_id().as_bytes());
    encoder.usize(inputs.reference_data().len());
    for (kind, identity) in inputs.reference_data() {
        encoder.u8(reference_data_tag(*kind));
        encoder.identity(*identity);
    }
    match science.spectral.sampling {
        SpectralSampling::Identity => encoder.u8(0),
        SpectralSampling::Nearest => encoder.u8(1),
        SpectralSampling::Linear => encoder.u8(2),
        SpectralSampling::ChannelAverage { channels_per_bin } => {
            encoder.u8(3);
            encoder.usize(channels_per_bin);
        }
    }
    encoder.u8(match science.spectral.coupling {
        SpectralCoupling::Independent => 0,
        SpectralCoupling::CommonRestoringBeam => 1,
    });
    encoder.u8(match science.measurement_equation.instrument_response {
        InstrumentResponse::Scalar => 0,
        InstrumentResponse::PrimaryBeam => 1,
        InstrumentResponse::FullMueller => 2,
    });
    let inner_products = science.measurement_equation.inner_products;
    encoder.u8(match inner_products.model() {
        ModelInnerProduct::HermitianEuclidean => 0,
    });
    encoder.u8(match inner_products.visibility() {
        VisibilityInnerProduct::HermitianEuclidean => 0,
    });
    let operator = normal_equation.measurement_operator();
    encoder.digest(operator.domain().geometry().as_bytes());
    encoder.u8(match operator.domain().basis() {
        ReconstructionBasis::Constant => 0,
        ReconstructionBasis::Taylor { .. } => 1,
        ReconstructionBasis::ChannelLocal { .. } => 2,
    });
    encoder.usize(operator.domain().polarization().coordinates().len());
    for coordinate in operator.domain().polarization().coordinates() {
        encoder.u8(polarization_tag(*coordinate));
    }
    encoder.identity(operator.codomain().observation().identity());
    encoder.usize(operator.transforms().len());
    for transform in operator.transforms() {
        match transform {
            PairedMeasurementTransform::SpectralBasis { basis } => {
                encoder.u8(0);
                encode_reconstruction_basis(&mut encoder, *basis);
            }
            PairedMeasurementTransform::PolarizationMapping => encoder.u8(1),
            PairedMeasurementTransform::DirectionDependentResponse { response } => {
                encoder.u8(2);
                encoder.u8(match response {
                    InstrumentResponse::Scalar => 0,
                    InstrumentResponse::PrimaryBeam => 1,
                    InstrumentResponse::FullMueller => 2,
                });
            }
            PairedMeasurementTransform::PhaseRotation { convention } => {
                encoder.u8(3);
                encoder.u8(match convention {
                    crate::geometry::VisibilityPhaseConvention::NegativeTwoPiFrequencyDelay => 0,
                });
            }
            PairedMeasurementTransform::SpectralResampling { sampling } => {
                encoder.u8(4);
                encoder.u8(match sampling {
                    SpectralSampling::Nearest => 0,
                    SpectralSampling::Linear => 1,
                    SpectralSampling::Identity | SpectralSampling::ChannelAverage { .. } => {
                        unreachable!("compiled spectral resampling is nearest or linear")
                    }
                });
            }
            PairedMeasurementTransform::ChannelIntegration { channels_per_bin } => {
                encoder.u8(5);
                encoder.usize(*channels_per_bin);
            }
        }
    }
    let compiled_weighting = normal_equation.weighting();
    encoder.digest(compiled_weighting.generation_id().as_bytes());
    encoder.identity(compiled_weighting.snapshot().identity());
    encoder.usize(compiled_weighting.sources().len());
    for source in compiled_weighting.sources() {
        encoder.identity(source.source().identity());
        encoder.u8(match source.flags() {
            FlagPolicy::FlagOrFlagRow => 0,
        });
        encoder.u8(match source.input_weights() {
            WeightColumn::Weight => 0,
            WeightColumn::WeightSpectrum => 1,
        });
        encoder.identity(source.flag_generation());
        encoder.identity(source.flag_row_generation());
        encoder.identity(source.input_weight_generation());
    }
    encoder.u8(match normal_equation.output().normalization() {
        NormalStateNormalization::Unnormalized => 0,
    });
    encode_reconstruction_basis(&mut encoder, reconstruction.basis);
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
    encoder.usize(reconstruction.polarization.coordinates.len());
    for coordinate in &reconstruction.polarization.coordinates {
        encoder.u8(polarization_tag(*coordinate));
    }
    match compiled_weighting.scheme() {
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
    encoder.u8(match compiled_weighting.density_scope() {
        WeightDensityScope::NotApplicable => 0,
        WeightDensityScope::GlobalSelection => 1,
        WeightDensityScope::PerOutputChannel => 2,
    });
    match compiled_weighting.uv_taper() {
        None => encoder.u8(0),
        Some(taper) => {
            encoder.u8(1);
            encoder.f64(taper.major_lambda());
            encoder.f64(taper.minor_lambda());
            encoder.f64(taper.position_angle_rad());
        }
    }
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
    let primary_beam_validity = products.validity.primary_beam();
    encoder.u32(primary_beam_validity.cutoff().to_bits());
    encoder.u8(match primary_beam_validity.comparison() {
        ProductSupportComparison::StrictlyGreater => 0,
    });
    encoder.u8(match primary_beam_validity.blanking() {
        ProductBlankingPolicy::ZeroAndFalseMask => 0,
    });
    let taylor_validity = products.validity.taylor();
    encoder.u8(match taylor_validity.reference() {
        TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum => 0,
    });
    encoder.u32(taylor_validity.peak_fraction().to_bits());
    encoder.u8(match taylor_validity.comparison() {
        ProductSupportComparison::StrictlyGreater => 0,
    });
    encoder.u8(match taylor_validity.blanking() {
        ProductBlankingPolicy::ZeroAndFalseMask => 0,
    });
    encoder.usize(products.normalization_boundary.operations().len());
    for operation in products.normalization_boundary.operations() {
        match operation {
            ProductBoundaryOperation::Normalize(normalization) => {
                encoder.u8(0);
                encoder.u8(match normalization {
                    ProductNormalization::UnitResponse => 0,
                    ProductNormalization::FlatNoise => 1,
                    ProductNormalization::FlatSky => 2,
                });
            }
            ProductBoundaryOperation::ScaleResidual => encoder.u8(1),
            ProductBoundaryOperation::Restore(policy) => {
                encoder.u8(2);
                encoder.u8(match policy {
                    RestoringBeamPolicy::None => 0,
                    RestoringBeamPolicy::PerPlane => 1,
                    RestoringBeamPolicy::Common => 2,
                });
            }
            ProductBoundaryOperation::CorrectPrimaryBeam => encoder.u8(3),
            ProductBoundaryOperation::BlankInvalid => encoder.u8(4),
            ProductBoundaryOperation::ConvertUnits => encoder.u8(5),
        }
    }
    encode_numerics(&mut encoder, numerics);
    LogicalIdentity::from_sha256(encoder.finish())
}

fn canonical_problem_id(
    basis: LogicalIdentity,
    model: ModelStateIdentity,
    model_lifecycle: LogicalIdentity,
) -> CompiledProblemId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(COMPILED_PROBLEM_IDENTITY_DOMAIN);
    encoder.u32(COMPILED_PROBLEM_IDENTITY_VERSION);
    encoder.identity(basis);
    match model {
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
    encoder.identity(model_lifecycle);
    CompiledProblemId(LogicalIdentity::from_sha256(encoder.finish()))
}

/// Revalidate the parent Compiled Problem identity from its canonical layers.
///
/// This digest-only seam is intended for durable receipt validation. It does
/// not construct a [`CompiledProblem`] or confer execution authority.
#[must_use]
pub fn validate_compiled_problem_identity(
    claimed: [u8; 32],
    basis: LogicalIdentity,
    model: ModelStateIdentity,
    model_lifecycle: LogicalIdentity,
) -> bool {
    if claimed == [0; 32]
        || basis.as_bytes() == [0; 32]
        || model_lifecycle.as_bytes() == [0; 32]
        || matches!(
            model,
            ModelStateIdentity::Seed(identity) | ModelStateIdentity::Generation(identity)
                if identity.as_bytes() == [0; 32]
        )
    {
        return false;
    }
    canonical_problem_id(basis, model, model_lifecycle).as_bytes() == claimed
}

fn canonical_numerics_id(numerics: &NumericsContract) -> NumericsContractId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(NUMERICS_CONTRACT_IDENTITY_DOMAIN);
    encoder.u32(NUMERICS_CONTRACT_IDENTITY_VERSION);
    encode_numerics(&mut encoder, numerics);
    NumericsContractId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn encode_numerics(encoder: &mut CanonicalEncoder, numerics: &NumericsContract) {
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
}

pub(crate) struct CanonicalEncoder(Sha256);

impl CanonicalEncoder {
    pub(crate) fn new() -> Self {
        Self(Sha256::new())
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn i32(&mut self, value: i32) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn usize(&mut self, value: usize) {
        self.0.update((value as u128).to_le_bytes());
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    pub(crate) fn identity(&mut self, identity: LogicalIdentity) {
        self.0.update(identity.0);
    }

    pub(crate) fn digest(&mut self, digest: [u8; 32]) {
        self.0.update(digest);
    }

    pub(crate) fn f64(&mut self, value: f64) {
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.0.update(bits.to_le_bytes());
    }

    pub(crate) fn f32(&mut self, value: f32) {
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.0.update(bits.to_le_bytes());
    }

    pub(crate) fn finish(self) -> [u8; 32] {
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

pub(crate) fn encode_reconstruction_basis(
    encoder: &mut CanonicalEncoder,
    basis: ReconstructionBasis,
) {
    match basis {
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
}

pub(crate) const fn polarization_tag(coordinate: PolarizationCoordinate) -> u8 {
    match coordinate {
        PolarizationCoordinate::StokesI => 0,
        PolarizationCoordinate::StokesQ => 1,
        PolarizationCoordinate::StokesU => 2,
        PolarizationCoordinate::StokesV => 3,
        PolarizationCoordinate::LinearXx => 4,
        PolarizationCoordinate::LinearXy => 5,
        PolarizationCoordinate::LinearYx => 6,
        PolarizationCoordinate::LinearYy => 7,
        PolarizationCoordinate::CircularRr => 8,
        PolarizationCoordinate::CircularRl => 9,
        PolarizationCoordinate::CircularLr => 10,
        PolarizationCoordinate::CircularLl => 11,
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
