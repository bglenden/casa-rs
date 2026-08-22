// SPDX-License-Identifier: LGPL-3.0-or-later

//! Closed commitments and value schemas for authoritative model state.

use std::fmt;

use thiserror::Error;

use crate::{
    CompiledGeometry, DirectionCoordinateSpec, ImageDomainRole, ImageShape, LogicalIdentity,
    ModelCoefficientSpace, ModelInnerProduct, ModelStateIdentity, NumericPrecision,
    NumericsContract, NumericsContractId, ProblemInputIdentities, ProductGraphId,
    ReconstructionBasis, SpectralCoordinateSpec,
    compiled_problem::{CanonicalEncoder, encode_reconstruction_basis, polarization_tag},
};

const MODEL_SPACE_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-space";
const MODEL_SPACE_IDENTITY_VERSION: u32 = 1;
const MODEL_SOURCE_SHAPE_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-source-shape";
const MODEL_SOURCE_SHAPE_IDENTITY_VERSION: u32 = 1;
const MODEL_LIFECYCLE_CONTRACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-lifecycle-contract";
const MODEL_LIFECYCLE_CONTRACT_IDENTITY_VERSION: u32 = 4;
const MODEL_REPROJECTION_CONTRACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-reprojection-contract";
const MODEL_REPROJECTION_CONTRACT_IDENTITY_VERSION: u32 = 1;
const MODEL_REPROJECTED_SEED_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-reprojected-seed";
const MODEL_REPROJECTED_SEED_IDENTITY_VERSION: u32 = 1;
const MODEL_SUPPORT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-support";
const MODEL_SUPPORT_IDENTITY_VERSION: u32 = 1;

/// One finite, canonical semantic `f64` model coefficient or increment.
///
/// The representation is precision-independent. Arithmetic that creates a new
/// value remains governed by the lifecycle precision selected from the
/// Compiled Problem's Numerics Contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelValue(u64);

impl ModelValue {
    /// Construct a finite value, canonicalizing negative zero.
    pub fn new(value: f64) -> Result<Self, ModelContractError> {
        if !value.is_finite() {
            return Err(ModelContractError::NonFiniteValue);
        }
        Ok(Self(canonical_f64_bits(value)))
    }

    /// Return the represented value.
    #[must_use]
    pub fn value(self) -> f64 {
        f64::from_bits(self.0)
    }

    pub(crate) const fn bits(self) -> u64 {
        self.0
    }
}

/// Whether one model coefficient belongs to the declared valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelSupport {
    /// The coefficient is scientifically defined.
    Valid,
    /// The coefficient is outside valid support; its numeric payload is not a value.
    Invalid,
}

/// One semantic model coefficient and its independent support state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelSample {
    value: ModelValue,
    support: ModelSupport,
}

impl ModelSample {
    /// Construct one valid model coefficient.
    #[must_use]
    pub const fn valid(value: ModelValue) -> Self {
        Self {
            value,
            support: ModelSupport::Valid,
        }
    }

    /// Construct an invalid-support sample with a canonical non-value payload.
    pub fn invalid() -> Self {
        Self {
            value: ModelValue(0),
            support: ModelSupport::Invalid,
        }
    }

    /// Return the semantic numeric payload.
    ///
    /// Callers must inspect [`Self::support`] before treating it as a value.
    #[must_use]
    pub const fn value(self) -> ModelValue {
        self.value
    }

    /// Return the independent validity state.
    #[must_use]
    pub const fn support(self) -> ModelSupport {
        self.support
    }
}

/// Compute the canonical identity of one target-ordered validity mask.
///
/// The identity commits support independently from numeric payloads so an
/// invalid coefficient can never alias a valid numeric zero.
#[must_use]
pub fn model_support_identity(support: impl IntoIterator<Item = ModelSupport>) -> LogicalIdentity {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_SUPPORT_IDENTITY_DOMAIN);
    encoder.u32(MODEL_SUPPORT_IDENTITY_VERSION);
    let mut count = 0usize;
    for value in support {
        encoder.u8(match value {
            ModelSupport::Valid => 1,
            ModelSupport::Invalid => 0,
        });
        count += 1;
    }
    encoder.usize(count);
    LogicalIdentity::from_sha256(encoder.finish())
}

/// Explicit residency and numeric ceilings for one model lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelBounds {
    max_model_samples: usize,
    max_source_samples: usize,
    max_reprojection_terms: usize,
    max_delta_terms: usize,
    max_absolute_model_value: ModelValue,
    max_absolute_delta_value: ModelValue,
}

impl ModelBounds {
    /// Construct positive finite numeric bounds and exact count ceilings.
    pub fn new(
        max_model_samples: usize,
        max_source_samples: usize,
        max_reprojection_terms: usize,
        max_delta_terms: usize,
        max_absolute_model_value: f64,
        max_absolute_delta_value: f64,
    ) -> Result<Self, ModelContractError> {
        if max_model_samples == 0
            || max_source_samples == 0
            || max_reprojection_terms == 0
            || max_delta_terms == 0
            || !max_absolute_model_value.is_finite()
            || max_absolute_model_value <= 0.0
            || !max_absolute_delta_value.is_finite()
            || max_absolute_delta_value <= 0.0
        {
            return Err(ModelContractError::InvalidBounds);
        }
        Ok(Self {
            max_model_samples,
            max_source_samples,
            max_reprojection_terms,
            max_delta_terms,
            max_absolute_model_value: ModelValue::new(max_absolute_model_value)?,
            max_absolute_delta_value: ModelValue::new(max_absolute_delta_value)?,
        })
    }

    /// Return the maximum resident target-model sample count.
    #[must_use]
    pub const fn max_model_samples(self) -> usize {
        self.max_model_samples
    }

    /// Return the maximum source-model sample count.
    #[must_use]
    pub const fn max_source_samples(self) -> usize {
        self.max_source_samples
    }

    /// Return the maximum total interpolation terms in one reprojection.
    #[must_use]
    pub const fn max_reprojection_terms(self) -> usize {
        self.max_reprojection_terms
    }

    /// Return the maximum number of terms in one Model Delta.
    #[must_use]
    pub const fn max_delta_terms(self) -> usize {
        self.max_delta_terms
    }

    /// Return the absolute model-value ceiling.
    #[must_use]
    pub fn max_absolute_model_value(self) -> f64 {
        self.max_absolute_model_value.value()
    }

    /// Return the absolute per-term delta ceiling.
    #[must_use]
    pub fn max_absolute_delta_value(self) -> f64 {
        self.max_absolute_delta_value.value()
    }

    fn encode(self, encoder: &mut CanonicalEncoder) {
        encoder.usize(self.max_model_samples);
        encoder.usize(self.max_source_samples);
        encoder.usize(self.max_reprojection_terms);
        encoder.usize(self.max_delta_terms);
        encoder.u64(self.max_absolute_model_value.bits());
        encoder.u64(self.max_absolute_delta_value.bits());
    }
}

/// One coefficient cell flattened in domain, coefficient, polarization, y, x order.
///
/// The two-element pixel coordinate itself is represented as `[x, y]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelCell {
    domain: usize,
    coefficient: usize,
    polarization: usize,
    pixel: [usize; 2],
}

impl ModelCell {
    /// Construct one typed cell coordinate.
    #[must_use]
    pub const fn new(
        domain: usize,
        coefficient: usize,
        polarization: usize,
        pixel: [usize; 2],
    ) -> Self {
        Self {
            domain,
            coefficient,
            polarization,
            pixel,
        }
    }

    /// Return the image-domain ordinal.
    #[must_use]
    pub const fn domain(self) -> usize {
        self.domain
    }

    /// Return the spectral-basis coefficient ordinal.
    #[must_use]
    pub const fn coefficient(self) -> usize {
        self.coefficient
    }

    /// Return the polarization-coordinate ordinal.
    #[must_use]
    pub const fn polarization(self) -> usize {
        self.polarization
    }

    /// Return `[x, y]` pixel coordinates.
    #[must_use]
    pub const fn pixel(self) -> [usize; 2] {
        self.pixel
    }
}

/// Rectangular model-array shape with typed WCS and coefficient-space provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelSourceShape {
    coefficient_space: ModelCoefficientSpace,
    domains: Box<[ImageShape]>,
    domain_roles: Box<[ImageDomainRole]>,
    directions: Box<[DirectionCoordinateSpec]>,
    spectral: SpectralCoordinateSpec,
    coefficients: usize,
    polarizations: usize,
    samples: usize,
}

// `ModelSourceShape` is constructed only from canonical compiled geometry, so
// its finite floating-point coordinate laws have reflexive equality.
impl Eq for ModelSourceShape {}

impl ModelSourceShape {
    /// Derive one checked shape from matching compiler-owned geometry and coefficient space.
    pub fn from_compiled(
        geometry: &CompiledGeometry,
        coefficient_space: &ModelCoefficientSpace,
    ) -> Result<Self, ModelContractError> {
        if coefficient_space.geometry() != geometry.geometry_id() {
            return Err(ModelContractError::CoefficientSpaceGeometryMismatch);
        }
        let domains = geometry
            .domains()
            .iter()
            .map(|domain| domain.shape())
            .collect::<Vec<_>>();
        let directions = geometry
            .domains()
            .iter()
            .map(|domain| domain.direction())
            .collect::<Vec<_>>();
        let domain_roles = geometry
            .domains()
            .iter()
            .map(|domain| domain.role().clone())
            .collect::<Vec<_>>();
        let coefficients = coefficient_count(coefficient_space.basis());
        let polarizations = coefficient_space.polarization().coordinates().len();
        if domains.is_empty() || coefficients == 0 || polarizations == 0 {
            return Err(ModelContractError::InvalidShape);
        }
        let mut samples = 0usize;
        for domain in &domains {
            let [width, height] = domain.pixels();
            if width == 0 || height == 0 {
                return Err(ModelContractError::InvalidShape);
            }
            let domain_samples = width
                .checked_mul(height)
                .and_then(|pixels| pixels.checked_mul(coefficients))
                .and_then(|values| values.checked_mul(polarizations))
                .ok_or(ModelContractError::ShapeTooLarge)?;
            samples = samples
                .checked_add(domain_samples)
                .ok_or(ModelContractError::ShapeTooLarge)?;
        }
        Ok(Self {
            coefficient_space: coefficient_space.clone(),
            domains: domains.into_boxed_slice(),
            domain_roles: domain_roles.into_boxed_slice(),
            directions: directions.into_boxed_slice(),
            spectral: geometry.spectral().clone(),
            coefficients,
            polarizations,
            samples,
        })
    }

    /// Return the exact compiler-owned coordinate and coefficient space.
    #[must_use]
    pub const fn coefficient_space(&self) -> &ModelCoefficientSpace {
        &self.coefficient_space
    }

    /// Return domain shapes in canonical domain order.
    #[must_use]
    pub const fn domains(&self) -> &[ImageShape] {
        &self.domains
    }

    /// Return domain roles in the same canonical order as [`Self::domains`].
    #[must_use]
    pub const fn domain_roles(&self) -> &[ImageDomainRole] {
        &self.domain_roles
    }

    /// Return the exact direction-coordinate law for one canonical domain.
    #[must_use]
    pub fn direction(&self, domain: usize) -> Option<DirectionCoordinateSpec> {
        self.directions.get(domain).copied()
    }

    /// Return the exact compiler-owned spectral coordinate law.
    #[must_use]
    pub const fn spectral(&self) -> &SpectralCoordinateSpec {
        &self.spectral
    }

    /// Return the number of spectral-basis coefficient planes.
    #[must_use]
    pub const fn coefficients(&self) -> usize {
        self.coefficients
    }

    /// Return the number of polarization planes.
    #[must_use]
    pub const fn polarizations(&self) -> usize {
        self.polarizations
    }

    /// Return the exact flattened sample count.
    #[must_use]
    pub const fn sample_count(&self) -> usize {
        self.samples
    }

    /// Return the closed commitment identity of this exact typed shape.
    #[must_use]
    pub fn identity(&self) -> LogicalIdentity {
        let mut encoder = CanonicalEncoder::new();
        encoder.bytes(MODEL_SOURCE_SHAPE_IDENTITY_DOMAIN);
        encoder.u32(MODEL_SOURCE_SHAPE_IDENTITY_VERSION);
        self.encode(&mut encoder);
        LogicalIdentity::from_sha256(encoder.finish())
    }

    /// Return a cell's canonical flattened ordinal, when it belongs to this shape.
    #[must_use]
    pub fn flat_index(&self, cell: ModelCell) -> Option<usize> {
        if cell.coefficient >= self.coefficients || cell.polarization >= self.polarizations {
            return None;
        }
        let mut offset = 0usize;
        for (domain_index, shape) in self.domains.iter().enumerate() {
            let [width, height] = shape.pixels();
            if domain_index == cell.domain {
                let [x, y] = cell.pixel;
                if x >= width || y >= height {
                    return None;
                }
                let plane = cell
                    .coefficient
                    .checked_mul(self.polarizations)?
                    .checked_add(cell.polarization)?;
                return offset
                    .checked_add(plane.checked_mul(width.checked_mul(height)?)?)?
                    .checked_add(y.checked_mul(width)?)?
                    .checked_add(x);
            }
            offset = offset.checked_add(
                width
                    .checked_mul(height)?
                    .checked_mul(self.coefficients)?
                    .checked_mul(self.polarizations)?,
            )?;
        }
        None
    }

    /// Return the typed cell at one canonical flattened ordinal.
    #[must_use]
    pub fn cell_at(&self, mut index: usize) -> Option<ModelCell> {
        if index >= self.samples {
            return None;
        }
        for (domain, shape) in self.domains.iter().enumerate() {
            let [width, height] = shape.pixels();
            let pixels = width.checked_mul(height)?;
            let domain_samples = pixels
                .checked_mul(self.coefficients)?
                .checked_mul(self.polarizations)?;
            if index < domain_samples {
                let plane = index / pixels;
                index %= pixels;
                return Some(ModelCell::new(
                    domain,
                    plane / self.polarizations,
                    plane % self.polarizations,
                    [index % width, index / width],
                ));
            }
            index -= domain_samples;
        }
        None
    }

    pub(crate) fn encode(&self, encoder: &mut CanonicalEncoder) {
        encoder.identity(model_coefficient_space_identity(&self.coefficient_space));
        encoder.usize(self.domains.len());
        for domain in &self.domains {
            let [width, height] = domain.pixels();
            encoder.usize(width);
            encoder.usize(height);
        }
        encoder.usize(self.coefficients);
        encoder.usize(self.polarizations);
    }
}

/// Digest projection of one reconstruction-owned, target-ordered preparation.
///
/// This projection is compiler and receipt data, not executable preparation
/// authority. Only `casa-imaging-reconstruction` can bind it to the opaque
/// prepared values and ordered interpolation stencils required for execution.
///
/// Reprojected preparation evidence is not a caller construction surface:
///
/// ```compile_fail
/// use casa_imaging_model::{
///     LogicalIdentity, ModelLifecycleContract, ModelReprojectedSeedProjection, ModelSample,
///     ModelSourceShape,
/// };
///
/// fn forge(
///     target: &ModelLifecycleContract,
///     source: LogicalIdentity,
///     source_shape: ModelSourceShape,
///     samples: &[ModelSample],
/// ) {
///     let _ = ModelReprojectedSeedProjection::from_prepared_samples(
///         target,
///         source,
///         source_shape,
///         samples,
///     );
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelReprojectedSeedProjection {
    source: LogicalIdentity,
    source_shape: Box<ModelSourceShape>,
    preparation_contract: LogicalIdentity,
    reprojection: LogicalIdentity,
    support: LogicalIdentity,
    samples: LogicalIdentity,
    stencil: LogicalIdentity,
    proof: LogicalIdentity,
}

impl ModelReprojectedSeedProjection {
    /// Carry reconstruction-owned identities into the dependency-free compiler.
    ///
    /// Constructing this digest projection does not create executable evidence;
    /// reconstruction's opaque executable-problem brand performs that binding.
    #[allow(clippy::too_many_arguments)]
    pub fn from_identities(
        source: LogicalIdentity,
        source_shape: ModelSourceShape,
        preparation_contract: LogicalIdentity,
        reprojection: LogicalIdentity,
        support: LogicalIdentity,
        samples: LogicalIdentity,
        stencil: LogicalIdentity,
        proof: LogicalIdentity,
    ) -> Result<Self, ModelContractError> {
        if !identities_are_set(&[
            source,
            preparation_contract,
            reprojection,
            support,
            samples,
            stencil,
            proof,
        ]) {
            return Err(ModelContractError::InvalidReprojectedSeedProof);
        }
        Ok(Self {
            source,
            source_shape: Box::new(source_shape),
            preparation_contract,
            reprojection,
            support,
            samples,
            stencil,
            proof,
        })
    }

    /// Return the immutable source artifact identity.
    #[must_use]
    pub const fn source(&self) -> LogicalIdentity {
        self.source
    }

    /// Return the exact typed source model space.
    #[must_use]
    pub const fn source_shape(&self) -> &ModelSourceShape {
        &self.source_shape
    }

    /// Return the empty-target preparation contract identity.
    #[must_use]
    pub const fn preparation_contract(&self) -> LogicalIdentity {
        self.preparation_contract
    }

    /// Return the canonical mapping identity derived from both typed spaces.
    #[must_use]
    pub const fn reprojection(&self) -> LogicalIdentity {
        self.reprojection
    }

    /// Return the canonical target-support identity.
    #[must_use]
    pub const fn support(&self) -> LogicalIdentity {
        self.support
    }

    /// Return the canonical target-ordered value-and-support identity.
    #[must_use]
    pub const fn samples(&self) -> LogicalIdentity {
        self.samples
    }

    /// Return the canonical ordered interpolation-stencil identity.
    #[must_use]
    pub const fn stencil(&self) -> LogicalIdentity {
        self.stencil
    }

    /// Return the complete preparation-proof identity.
    #[must_use]
    pub const fn proof(&self) -> LogicalIdentity {
        self.proof
    }

    fn validate(
        &self,
        target: &ModelSourceShape,
        bounds: ModelBounds,
        preparation_contract: LogicalIdentity,
    ) -> Result<(), ModelContractError> {
        if self.source_shape.sample_count() > bounds.max_source_samples {
            return Err(ModelContractError::SourceSampleBoundExceeded {
                samples: self.source_shape.sample_count(),
                bound: bounds.max_source_samples,
            });
        }
        if self.preparation_contract != preparation_contract {
            return Err(ModelContractError::ReprojectionContractMismatch);
        }
        let expected_reprojection = model_reprojected_seed_mapping_identity(
            preparation_contract,
            self.source_shape.identity(),
            target.identity(),
        );
        if self.source.as_bytes() == [0; 32]
            || self.support.as_bytes() == [0; 32]
            || self.samples.as_bytes() == [0; 32]
            || self.stencil.as_bytes() == [0; 32]
            || self.proof.as_bytes() == [0; 32]
            || self.source_shape.as_ref() == target
            || self.reprojection != expected_reprojection
        {
            Err(ModelContractError::InvalidReprojectedSeedProof)
        } else {
            Ok(())
        }
    }
}

/// Compiler input commitment for the initial authoritative model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelInputCommitment {
    /// Begin from a zero-valued model with valid support everywhere.
    Empty,
    /// Ingest a seed already aligned to the compiler-derived target model space.
    AlignedSeed {
        /// Immutable source artifact identity.
        source: LogicalIdentity,
        /// Identity of the source artifact's exact validity mask.
        support: LogicalIdentity,
    },
    /// Ingest a seed through one exact typed reprojection.
    ///
    /// Raw evidence is not a caller construction surface. Reprojected input is
    /// accepted only through the model-validating commitment carried by
    /// reconstruction's opaque preparation product.
    ///
    /// ```compile_fail
    /// use casa_imaging_model::ModelInputCommitment;
    ///
    /// let _ = ModelInputCommitment::ReprojectedSeed {
    ///     source: todo!(),
    ///     source_shape: todo!(),
    ///     contract: todo!(),
    ///     reprojection: todo!(),
    ///     support: todo!(),
    /// };
    /// ```
    ReprojectedSeed(ModelReprojectedSeedProjection),
    /// Resume the exact authoritative generation named by the observation input.
    Generation(LogicalIdentity),
}

/// Digest-only input projection used to revalidate durable audit evidence.
///
/// This type cannot initialize or resume a model lifecycle. It exists solely
/// so receipt readers can ask the model-contract owner to recompute a compiled
/// lifecycle identity without reconstructing compiler-owned shapes or exposing
/// raw executable commitments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelInputCommitmentIdentity {
    /// A zero-valued initial model.
    Empty,
    /// An already aligned seed and its exact support mask.
    AlignedSeed {
        /// Immutable source artifact identity.
        source: LogicalIdentity,
        /// Source validity-mask identity.
        support: LogicalIdentity,
    },
    /// One opaque reconstruction-prepared reprojection proof.
    ReprojectedSeed {
        /// Immutable source artifact identity.
        source: LogicalIdentity,
        /// Exact typed source-shape identity.
        source_shape: LogicalIdentity,
        /// Empty-target preparation-contract identity.
        preparation_contract: LogicalIdentity,
        /// Canonical source-to-target mapping identity.
        reprojection: LogicalIdentity,
        /// Projected target-support identity.
        support: LogicalIdentity,
        /// Canonical target-ordered projected sample identity.
        samples: LogicalIdentity,
        /// Canonical ordered interpolation-stencil identity.
        stencil: LogicalIdentity,
        /// Complete opaque preparation-proof identity.
        proof: LogicalIdentity,
    },
    /// One exact previously minted generation.
    Generation(LogicalIdentity),
}

impl ModelInputCommitmentIdentity {
    fn from_commitment(input: &ModelInputCommitment) -> Self {
        match input {
            ModelInputCommitment::Empty => Self::Empty,
            ModelInputCommitment::AlignedSeed { source, support } => Self::AlignedSeed {
                source: *source,
                support: *support,
            },
            ModelInputCommitment::ReprojectedSeed(commitment) => Self::ReprojectedSeed {
                source: commitment.source(),
                source_shape: commitment.source_shape().identity(),
                preparation_contract: commitment.preparation_contract(),
                reprojection: commitment.reprojection(),
                support: commitment.support(),
                samples: commitment.samples(),
                stencil: commitment.stencil(),
                proof: commitment.proof(),
            },
            ModelInputCommitment::Generation(generation) => Self::Generation(*generation),
        }
    }
}

/// Uncompiled lifecycle requirements carried by the sole imaging request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLifecycleRequirements {
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
    input: ModelInputCommitment,
}

impl ModelLifecycleRequirements {
    /// Bind explicit resource bounds, arithmetic precision, and input evidence.
    #[must_use]
    pub const fn new(
        bounds: ModelBounds,
        arithmetic_precision: NumericPrecision,
        input: ModelInputCommitment,
    ) -> Self {
        Self {
            bounds,
            arithmetic_precision,
            input,
        }
    }
}

/// Stable identity of one complete compiled model-lifecycle commitment.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelLifecycleContractId(LogicalIdentity);

impl ModelLifecycleContractId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = MODEL_LIFECYCLE_CONTRACT_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }

    const fn identity(self) -> LogicalIdentity {
        self.0
    }
}

impl fmt::Debug for ModelLifecycleContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelLifecycleContractId(")?;
        write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ModelLifecycleContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.as_bytes())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ModelReprojectionContractId(LogicalIdentity);

impl ModelReprojectionContractId {
    const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ModelReprojectionContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelReprojectionContractId(")?;
        write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ModelReprojectionContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.as_bytes())
    }
}

/// Closed direction-conversion registry selected by the model compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelDirectionConversionRegistry {
    /// Same-tangent-plane affine bilinear direction conversion, version 1.
    SameTangentPlaneAffineBilinearV1,
}

impl ModelDirectionConversionRegistry {
    /// Return the canonical persisted policy tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SameTangentPlaneAffineBilinearV1 => "same_tangent_plane_affine_bilinear_v1",
        }
    }
}

/// Closed spectral-basis conversion registry selected by the model compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelBasisConversionRegistry {
    /// Exact spectral basis conversion, version 1.
    ExactSpectralV1,
}

impl ModelBasisConversionRegistry {
    /// Return the canonical persisted policy tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactSpectralV1 => "exact_spectral_v1",
        }
    }
}

/// Closed polarization-conversion registry selected by the model compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelPolarizationConversionRegistry {
    /// Real parallel-hands polarization conversion, version 1.
    RealParallelHandsV1,
}

impl ModelPolarizationConversionRegistry {
    /// Return the canonical persisted policy tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RealParallelHandsV1 => "real_parallel_hands_v1",
        }
    }
}

/// Closed invalid-contributor policy selected by the model compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInvalidContributorPolicy {
    /// Invalidate a target when an invalid non-zero contributor participates.
    InvalidateTarget,
}

impl ModelInvalidContributorPolicy {
    /// Return the canonical persisted policy tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidateTarget => "invalidate_target",
        }
    }
}

/// Closed uncovered-target policy selected by the model compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelUncoveredTargetPolicy {
    /// Mark an uncovered target as invalid.
    Invalid,
}

impl ModelUncoveredTargetPolicy {
    /// Return the canonical persisted policy tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
        }
    }
}

/// The complete compiler-owned model reprojection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelReprojectionPolicy {
    direction_registry: ModelDirectionConversionRegistry,
    basis_registry: ModelBasisConversionRegistry,
    polarization_registry: ModelPolarizationConversionRegistry,
    invalid_contributor: ModelInvalidContributorPolicy,
    uncovered_target: ModelUncoveredTargetPolicy,
}

impl ModelReprojectionPolicy {
    /// Return the canonical policy used by the model compiler.
    #[must_use]
    pub const fn canonical() -> Self {
        Self {
            direction_registry: ModelDirectionConversionRegistry::SameTangentPlaneAffineBilinearV1,
            basis_registry: ModelBasisConversionRegistry::ExactSpectralV1,
            polarization_registry: ModelPolarizationConversionRegistry::RealParallelHandsV1,
            invalid_contributor: ModelInvalidContributorPolicy::InvalidateTarget,
            uncovered_target: ModelUncoveredTargetPolicy::Invalid,
        }
    }

    /// Return the compiler-owned direction conversion registry.
    #[must_use]
    pub const fn direction_registry(self) -> ModelDirectionConversionRegistry {
        self.direction_registry
    }

    /// Return the compiler-owned spectral basis conversion registry.
    #[must_use]
    pub const fn basis_registry(self) -> ModelBasisConversionRegistry {
        self.basis_registry
    }

    /// Return the compiler-owned polarization conversion registry.
    #[must_use]
    pub const fn polarization_registry(self) -> ModelPolarizationConversionRegistry {
        self.polarization_registry
    }

    /// Return the compiler-owned invalid-contributor policy.
    #[must_use]
    pub const fn invalid_contributor(self) -> ModelInvalidContributorPolicy {
        self.invalid_contributor
    }

    /// Return the compiler-owned uncovered-target policy.
    #[must_use]
    pub const fn uncovered_target(self) -> ModelUncoveredTargetPolicy {
        self.uncovered_target
    }
}

/// Precision-independent in-memory encoding of authoritative model values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelStateEncoding {
    /// Finite canonical binary64 values, with arithmetic rounded as separately committed.
    CanonicalF64,
}

/// Invalid-support behavior retained independently from numeric values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelSupportSemantics {
    /// Every sample carries explicit validity; uncovered targets and targets
    /// with any invalid non-zero reprojection contributor remain invalid.
    ExplicitValidity,
}

/// Compiler-owned closed commitment consumed by the reconstruction owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLifecycleContract {
    contract_id: ModelLifecycleContractId,
    reprojection_contract: LogicalIdentity,
    reprojection_policy: ModelReprojectionPolicy,
    numerics: NumericsContractId,
    target: ModelSourceShape,
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
    state_encoding: ModelStateEncoding,
    support_semantics: ModelSupportSemantics,
    input: ModelInputCommitment,
}

impl ModelLifecycleContract {
    /// Return the exact commitment identity.
    #[must_use]
    pub const fn contract_id(&self) -> ModelLifecycleContractId {
        self.contract_id
    }

    /// Return the compiler-owned Product/Numerics/reprojection policy commitment.
    #[must_use]
    pub const fn reprojection_contract_identity(&self) -> LogicalIdentity {
        self.reprojection_contract
    }

    /// Return the compiler-owned reprojection policy used for the commitment.
    #[must_use]
    pub const fn reprojection_policy(&self) -> ModelReprojectionPolicy {
        self.reprojection_policy
    }

    /// Return the bound Numerics Contract identity.
    #[must_use]
    pub const fn numerics(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the sole compiler-derived target model space.
    #[must_use]
    pub const fn target(&self) -> &ModelSourceShape {
        &self.target
    }

    /// Return all explicit owner bounds.
    #[must_use]
    pub const fn bounds(&self) -> ModelBounds {
        self.bounds
    }

    /// Return the arithmetic precision selected from the Numerics Contract.
    #[must_use]
    pub const fn arithmetic_precision(&self) -> NumericPrecision {
        self.arithmetic_precision
    }

    /// Return the precision-independent state encoding.
    #[must_use]
    pub const fn state_encoding(&self) -> ModelStateEncoding {
        self.state_encoding
    }

    /// Return the explicit invalid-support semantics.
    #[must_use]
    pub const fn support_semantics(&self) -> ModelSupportSemantics {
        self.support_semantics
    }

    /// Return the complete compiler-bound input commitment.
    #[must_use]
    pub const fn input(&self) -> &ModelInputCommitment {
        &self.input
    }
}

/// One sparse solver update term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelDeltaTerm {
    cell: ModelCell,
    increment: ModelValue,
}

impl ModelDeltaTerm {
    /// Construct one typed cell increment.
    #[must_use]
    pub const fn new(cell: ModelCell, increment: ModelValue) -> Self {
        Self { cell, increment }
    }

    /// Return the changed cell.
    #[must_use]
    pub const fn cell(self) -> ModelCell {
        self.cell
    }

    /// Return the additive increment.
    #[must_use]
    pub const fn increment(self) -> ModelValue {
        self.increment
    }
}

/// Typed identity of one execution attempt bound to reconstruction evidence.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelExecutionAttemptId(LogicalIdentity);

impl ModelExecutionAttemptId {
    /// Bind an already established execution-attempt identity.
    #[must_use]
    pub const fn new(identity: LogicalIdentity) -> Self {
        Self(identity)
    }

    /// Return the exact logical identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.0
    }
}

impl fmt::Debug for ModelExecutionAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelExecutionAttemptId(")?;
        write_hex(formatter, &self.0.as_bytes())?;
        formatter.write_str(")")
    }
}

/// Exact reason a model schema or compiler commitment was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelContractError {
    /// A model coefficient or increment was non-finite.
    #[error("model values must be finite")]
    NonFiniteValue,
    /// A reprojection weight was non-finite.
    #[error("reprojection weights must be finite")]
    NonFiniteReprojectionWeight,
    /// Numeric or count bounds were empty, non-finite, or non-positive.
    #[error("model lifecycle bounds must be explicit, finite, and positive")]
    InvalidBounds,
    /// A source or target shape was empty.
    #[error("model shapes require domains, coefficient planes, and polarization planes")]
    InvalidShape,
    /// A coefficient space did not belong to the supplied compiled geometry.
    #[error("model coefficient space does not belong to the supplied compiled geometry")]
    CoefficientSpaceGeometryMismatch,
    /// A shape sample count overflowed `usize`.
    #[error("model shape is too large to represent")]
    ShapeTooLarge,
    /// The target model exceeded its compiled residency ceiling.
    #[error("target model has {samples} samples, exceeding bound {bound}")]
    ModelSampleBoundExceeded {
        /// Exact target sample count.
        samples: usize,
        /// Declared target ceiling.
        bound: usize,
    },
    /// The source model exceeded its compiled residency ceiling.
    #[error("source model has {samples} samples, exceeding bound {bound}")]
    SourceSampleBoundExceeded {
        /// Exact source sample count.
        samples: usize,
        /// Declared source ceiling.
        bound: usize,
    },
    /// The lifecycle selected arithmetic not permitted by the Numerics Contract.
    #[error("model lifecycle arithmetic precision is not permitted by the Numerics Contract")]
    PrecisionNotPermitted,
    /// The lifecycle input commitment disagreed with the observation's initial model.
    #[error("model lifecycle input commitment differs from the observation model input")]
    InputCommitmentMismatch,
    /// Reprojection evidence disagreed with its Product, Numerics, precision, or policy inputs.
    #[error("model reprojection contract differs from its compiled inputs")]
    ReprojectionContractMismatch,
    /// A seed commitment used an all-zero evidence identity.
    #[error("model seed, support, and reprojection identities must be established")]
    UnidentifiedInputEvidence,
    /// Reprojected input was not one canonical opaque preparation proof.
    #[error("reprojected model input is not a canonical preparation proof")]
    InvalidReprojectedSeedProof,
    /// A persisted lifecycle identity disagreed with its complete typed projection.
    #[error("model lifecycle identity differs from its canonical typed projection")]
    LifecycleContractMismatch,
}

pub(crate) fn compile_model_lifecycle_contract(
    geometry: &CompiledGeometry,
    coefficient_space: &ModelCoefficientSpace,
    inputs: &ProblemInputIdentities,
    numerics: &NumericsContract,
    numerics_id: NumericsContractId,
    product_graph: ProductGraphId,
    requirements: ModelLifecycleRequirements,
) -> Result<ModelLifecycleContract, ModelContractError> {
    let target = ModelSourceShape::from_compiled(geometry, coefficient_space)?;
    let ModelLifecycleRequirements {
        bounds,
        arithmetic_precision,
        input,
    } = requirements;
    if target.sample_count() > bounds.max_model_samples {
        return Err(ModelContractError::ModelSampleBoundExceeded {
            samples: target.sample_count(),
            bound: bounds.max_model_samples,
        });
    }
    if !numerics
        .permitted_precisions()
        .contains(&arithmetic_precision)
    {
        return Err(ModelContractError::PrecisionNotPermitted);
    }
    let reprojection_policy = ModelReprojectionPolicy::canonical();
    let reprojection = compile_model_reprojection_contract(
        product_graph,
        numerics_id,
        arithmetic_precision,
        reprojection_policy,
    );
    let state_encoding = ModelStateEncoding::CanonicalF64;
    let support_semantics = ModelSupportSemantics::ExplicitValidity;
    let preparation_contract = model_lifecycle_contract_id(
        numerics_id,
        reprojection,
        &target,
        bounds,
        arithmetic_precision,
        state_encoding,
        support_semantics,
        &ModelInputCommitment::Empty,
    );
    validate_input_commitment(
        inputs.model(),
        &target,
        bounds,
        preparation_contract.identity(),
        &input,
    )?;
    let contract_id = model_lifecycle_contract_id(
        numerics_id,
        reprojection,
        &target,
        bounds,
        arithmetic_precision,
        state_encoding,
        support_semantics,
        &input,
    );
    Ok(ModelLifecycleContract {
        contract_id,
        reprojection_contract: LogicalIdentity::from_sha256(reprojection.as_bytes()),
        reprojection_policy,
        numerics: numerics_id,
        target,
        bounds,
        arithmetic_precision,
        state_encoding,
        support_semantics,
        input,
    })
}

fn validate_input_commitment(
    model: ModelStateIdentity,
    target: &ModelSourceShape,
    bounds: ModelBounds,
    preparation_contract: LogicalIdentity,
    input: &ModelInputCommitment,
) -> Result<(), ModelContractError> {
    let matches = match (model, input) {
        (ModelStateIdentity::Empty, ModelInputCommitment::Empty) => true,
        (
            ModelStateIdentity::Seed(expected),
            ModelInputCommitment::AlignedSeed { source, support },
        ) => expected == *source && identities_are_set(&[*source, *support]),
        (ModelStateIdentity::Seed(expected), ModelInputCommitment::ReprojectedSeed(commitment)) => {
            commitment.validate(target, bounds, preparation_contract)?;
            expected == commitment.source()
        }
        (ModelStateIdentity::Generation(expected), ModelInputCommitment::Generation(actual)) => {
            expected == *actual && identities_are_set(&[*actual])
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else if input_identities_are_unset(input) {
        Err(ModelContractError::UnidentifiedInputEvidence)
    } else {
        Err(ModelContractError::InputCommitmentMismatch)
    }
}

fn input_identities_are_unset(input: &ModelInputCommitment) -> bool {
    match input {
        ModelInputCommitment::Empty => false,
        ModelInputCommitment::AlignedSeed { source, support } => {
            !identities_are_set(&[*source, *support])
        }
        ModelInputCommitment::ReprojectedSeed(commitment) => !identities_are_set(&[
            commitment.source(),
            commitment.preparation_contract(),
            commitment.reprojection(),
            commitment.support(),
            commitment.samples(),
            commitment.stencil(),
            commitment.proof(),
        ]),
        ModelInputCommitment::Generation(identity) => !identities_are_set(&[*identity]),
    }
}

fn identities_are_set(identities: &[LogicalIdentity]) -> bool {
    identities
        .iter()
        .all(|identity| identity.as_bytes() != [0; 32])
}

fn compile_model_reprojection_contract(
    product_graph: ProductGraphId,
    numerics: NumericsContractId,
    conversion_precision: NumericPrecision,
    policy: ModelReprojectionPolicy,
) -> ModelReprojectionContractId {
    model_reprojection_contract_id(product_graph, numerics, conversion_precision, policy)
}

fn model_reprojection_contract_id(
    product_graph: ProductGraphId,
    numerics: NumericsContractId,
    conversion_precision: NumericPrecision,
    policy: ModelReprojectionPolicy,
) -> ModelReprojectionContractId {
    ModelReprojectionContractId(model_reprojection_contract_identity(
        LogicalIdentity::from_sha256(product_graph.as_bytes()),
        LogicalIdentity::from_sha256(numerics.as_bytes()),
        conversion_precision,
        policy,
    ))
}

/// Validate one persisted reprojection-contract identity against its exact inputs.
///
/// Product Graph and Numerics identities are accepted as logical identities so
/// receipt validation can rebind their persisted digests without exposing
/// constructors for compiler-derived identity types. The model owner retains
/// the canonical encoding and the typed policy interpretation.
pub fn validate_model_reprojection_contract_identity(
    claimed: LogicalIdentity,
    product_graph: LogicalIdentity,
    numerics: LogicalIdentity,
    conversion_precision: NumericPrecision,
    policy: ModelReprojectionPolicy,
) -> Result<(), ModelContractError> {
    if claimed
        == model_reprojection_contract_identity(
            product_graph,
            numerics,
            conversion_precision,
            policy,
        )
    {
        Ok(())
    } else {
        Err(ModelContractError::ReprojectionContractMismatch)
    }
}

fn model_reprojection_contract_identity(
    product_graph: LogicalIdentity,
    numerics: LogicalIdentity,
    conversion_precision: NumericPrecision,
    policy: ModelReprojectionPolicy,
) -> LogicalIdentity {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_REPROJECTION_CONTRACT_IDENTITY_DOMAIN);
    encoder.u32(MODEL_REPROJECTION_CONTRACT_IDENTITY_VERSION);
    encoder.digest(product_graph.as_bytes());
    encoder.digest(numerics.as_bytes());
    encoder.u8(precision_tag(conversion_precision));
    encoder.u8(match policy.direction_registry() {
        ModelDirectionConversionRegistry::SameTangentPlaneAffineBilinearV1 => 0,
    });
    encoder.u8(match policy.basis_registry() {
        ModelBasisConversionRegistry::ExactSpectralV1 => 0,
    });
    encoder.u8(match policy.polarization_registry() {
        ModelPolarizationConversionRegistry::RealParallelHandsV1 => 0,
    });
    encoder.u8(match policy.invalid_contributor() {
        ModelInvalidContributorPolicy::InvalidateTarget => 0,
    });
    encoder.u8(match policy.uncovered_target() {
        ModelUncoveredTargetPolicy::Invalid => 0,
    });
    LogicalIdentity::from_sha256(encoder.finish())
}

/// Derive the dependency-free identity of one exact source-to-target mapping.
///
/// This identifies the typed mapping contract only. It is not proof that
/// reconstruction evaluated its ordered stencils or projected samples.
#[must_use]
pub fn model_reprojected_seed_mapping_identity(
    preparation_contract: LogicalIdentity,
    source_shape: LogicalIdentity,
    target_shape: LogicalIdentity,
) -> LogicalIdentity {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_REPROJECTED_SEED_IDENTITY_DOMAIN);
    encoder.u32(MODEL_REPROJECTED_SEED_IDENTITY_VERSION);
    encoder.identity(preparation_contract);
    encoder.identity(source_shape);
    encoder.identity(target_shape);
    LogicalIdentity::from_sha256(encoder.finish())
}

#[allow(clippy::too_many_arguments)]
fn model_lifecycle_contract_id(
    numerics: NumericsContractId,
    reprojection: ModelReprojectionContractId,
    target: &ModelSourceShape,
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
    state_encoding: ModelStateEncoding,
    support_semantics: ModelSupportSemantics,
    input: &ModelInputCommitment,
) -> ModelLifecycleContractId {
    ModelLifecycleContractId(model_lifecycle_contract_identity(
        LogicalIdentity::from_sha256(numerics.as_bytes()),
        LogicalIdentity::from_sha256(reprojection.as_bytes()),
        target.identity(),
        bounds,
        arithmetic_precision,
        state_encoding,
        support_semantics,
        &ModelInputCommitmentIdentity::from_commitment(input),
    ))
}

/// Recompute and validate one persisted model-lifecycle identity.
///
/// Receipt validation supplies typed, digest-only projections. The model
/// contract owner revalidates the complete lifecycle identity, the canonical
/// empty-target preparation identity, and the opaque reprojected-input proof;
/// duplicate audit fields are never treated as authority.
#[allow(clippy::too_many_arguments)]
pub fn validate_model_lifecycle_contract_identity(
    claimed: LogicalIdentity,
    numerics: LogicalIdentity,
    reprojection: LogicalIdentity,
    target_shape: LogicalIdentity,
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
    state_encoding: ModelStateEncoding,
    support_semantics: ModelSupportSemantics,
    input: &ModelInputCommitmentIdentity,
) -> Result<(), ModelContractError> {
    if !identities_are_set(&[claimed, numerics, reprojection, target_shape]) {
        return Err(ModelContractError::UnidentifiedInputEvidence);
    }
    validate_input_commitment_identity(
        numerics,
        reprojection,
        target_shape,
        bounds,
        arithmetic_precision,
        state_encoding,
        support_semantics,
        input,
    )?;
    let expected = model_lifecycle_contract_identity(
        numerics,
        reprojection,
        target_shape,
        bounds,
        arithmetic_precision,
        state_encoding,
        support_semantics,
        input,
    );
    if claimed == expected {
        Ok(())
    } else {
        Err(ModelContractError::LifecycleContractMismatch)
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_input_commitment_identity(
    numerics: LogicalIdentity,
    reprojection_contract: LogicalIdentity,
    target_shape: LogicalIdentity,
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
    state_encoding: ModelStateEncoding,
    support_semantics: ModelSupportSemantics,
    input: &ModelInputCommitmentIdentity,
) -> Result<(), ModelContractError> {
    match input {
        ModelInputCommitmentIdentity::Empty => Ok(()),
        ModelInputCommitmentIdentity::AlignedSeed { source, support } => {
            require_input_identities(&[*source, *support])
        }
        ModelInputCommitmentIdentity::ReprojectedSeed {
            source,
            source_shape,
            preparation_contract,
            reprojection,
            support,
            samples,
            stencil,
            proof,
        } => {
            require_input_identities(&[
                *source,
                *source_shape,
                *preparation_contract,
                *reprojection,
                *support,
                *samples,
                *stencil,
                *proof,
            ])?;
            let expected_preparation = model_lifecycle_contract_identity(
                numerics,
                reprojection_contract,
                target_shape,
                bounds,
                arithmetic_precision,
                state_encoding,
                support_semantics,
                &ModelInputCommitmentIdentity::Empty,
            );
            let expected_reprojection = model_reprojected_seed_mapping_identity(
                expected_preparation,
                *source_shape,
                target_shape,
            );
            if *preparation_contract != expected_preparation
                || *reprojection != expected_reprojection
            {
                return Err(ModelContractError::InvalidReprojectedSeedProof);
            }
            Ok(())
        }
        ModelInputCommitmentIdentity::Generation(generation) => {
            require_input_identities(&[*generation])
        }
    }
}

fn require_input_identities(identities: &[LogicalIdentity]) -> Result<(), ModelContractError> {
    if identities_are_set(identities) {
        Ok(())
    } else {
        Err(ModelContractError::UnidentifiedInputEvidence)
    }
}

#[allow(clippy::too_many_arguments)]
fn model_lifecycle_contract_identity(
    numerics: LogicalIdentity,
    reprojection: LogicalIdentity,
    target_shape: LogicalIdentity,
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
    state_encoding: ModelStateEncoding,
    support_semantics: ModelSupportSemantics,
    input: &ModelInputCommitmentIdentity,
) -> LogicalIdentity {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_LIFECYCLE_CONTRACT_IDENTITY_DOMAIN);
    encoder.u32(MODEL_LIFECYCLE_CONTRACT_IDENTITY_VERSION);
    encoder.identity(numerics);
    encoder.identity(reprojection);
    encoder.identity(target_shape);
    bounds.encode(&mut encoder);
    encoder.u8(precision_tag(arithmetic_precision));
    encoder.u8(match state_encoding {
        ModelStateEncoding::CanonicalF64 => 0,
    });
    encoder.u8(match support_semantics {
        ModelSupportSemantics::ExplicitValidity => 0,
    });
    encode_input_commitment_identity(&mut encoder, input);
    LogicalIdentity::from_sha256(encoder.finish())
}

fn encode_input_commitment_identity(
    encoder: &mut CanonicalEncoder,
    input: &ModelInputCommitmentIdentity,
) {
    match input {
        ModelInputCommitmentIdentity::Empty => encoder.u8(0),
        ModelInputCommitmentIdentity::AlignedSeed { source, support } => {
            encoder.u8(1);
            encoder.identity(*source);
            encoder.identity(*support);
        }
        ModelInputCommitmentIdentity::ReprojectedSeed {
            source,
            source_shape,
            preparation_contract,
            reprojection,
            support,
            samples,
            stencil,
            proof,
        } => {
            encoder.u8(2);
            encoder.identity(*source);
            encoder.identity(*source_shape);
            encoder.identity(*preparation_contract);
            encoder.identity(*reprojection);
            encoder.identity(*support);
            encoder.identity(*samples);
            encoder.identity(*stencil);
            encoder.identity(*proof);
        }
        ModelInputCommitmentIdentity::Generation(generation) => {
            encoder.u8(3);
            encoder.identity(*generation);
        }
    }
}

fn model_coefficient_space_identity(space: &ModelCoefficientSpace) -> LogicalIdentity {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_SPACE_IDENTITY_DOMAIN);
    encoder.u32(MODEL_SPACE_IDENTITY_VERSION);
    encoder.digest(space.geometry().as_bytes());
    encode_reconstruction_basis(&mut encoder, space.basis());
    encoder.usize(space.polarization().coordinates().len());
    for coordinate in space.polarization().coordinates() {
        encoder.u8(polarization_tag(*coordinate));
    }
    encoder.u8(match space.inner_product() {
        ModelInnerProduct::HermitianEuclidean => 0,
    });
    LogicalIdentity::from_sha256(encoder.finish())
}

const fn coefficient_count(basis: ReconstructionBasis) -> usize {
    match basis {
        ReconstructionBasis::Constant => 1,
        ReconstructionBasis::Taylor { terms } => terms,
        ReconstructionBasis::ChannelLocal { channels } => channels,
    }
}

const fn precision_tag(precision: NumericPrecision) -> u8 {
    match precision {
        NumericPrecision::F32 => 0,
        NumericPrecision::F64 => 1,
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
