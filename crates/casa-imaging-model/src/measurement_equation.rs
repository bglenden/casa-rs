// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed measurement-equation, weighting, normal-state, and product-boundary contracts.
//!
//! The measurement operator is always represented as one composition of paired
//! forward/adjoint transforms. Visibility flags, input weights, UV tapering,
//! and complete-selection density weighting belong only to [`WeightingOperatorContract`].
//! Its output is an explicitly unnormalized normal-state space; publication
//! normalization and restoration remain beyond [`ProductNormalizationBoundary`].

use std::fmt;

use sha2::{Digest, Sha256};

use crate::{
    ProblemInputIdentities,
    compiled_problem::{
        InstrumentResponse, LogicalIdentity, PolarizationContract, ProductKind,
        ProductNormalization, ReconstructionBasis, ReconstructionContract, RestoringBeamPolicy,
        ScientificContract, SpectralSampling, UvTaper, WeightDensityScope, WeightingContract,
        WeightingScheme,
    },
    geometry::{CompiledGeometry, CompiledGeometryId, VisibilityPhaseConvention},
    observation::{
        FlagPolicy, MeasurementSetIdentity, MsColumnKind, ObservationSnapshotId, WeightColumn,
    },
};

const WEIGHTING_GENERATION_IDENTITY_DOMAIN: &[u8] = b"casa-rs-weighting-generation";
const WEIGHTING_GENERATION_IDENTITY_VERSION: u32 = 1;

/// Inner product on the model-coefficient space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInnerProduct {
    /// Hermitian Euclidean product, conjugate-linear in its first argument.
    HermitianEuclidean,
}

/// Inner product on the unweighted selected-visibility space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityInnerProduct {
    /// Hermitian Euclidean product, conjugate-linear in its first argument.
    HermitianEuclidean,
}

/// Explicit inner products under which the measurement operator has its adjoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredInnerProducts {
    model: ModelInnerProduct,
    visibility: VisibilityInnerProduct,
}

impl DeclaredInnerProducts {
    /// Declare the model and selected-visibility inner products.
    #[must_use]
    pub const fn new(model: ModelInnerProduct, visibility: VisibilityInnerProduct) -> Self {
        Self { model, visibility }
    }

    /// Return the model-coefficient inner product.
    #[must_use]
    pub const fn model(self) -> ModelInnerProduct {
        self.model
    }

    /// Return the selected-visibility inner product.
    #[must_use]
    pub const fn visibility(self) -> VisibilityInnerProduct {
        self.visibility
    }
}

/// Typed domain of the complete logical measurement operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCoefficientSpace {
    geometry: CompiledGeometryId,
    basis: ReconstructionBasis,
    polarization: PolarizationContract,
    inner_product: ModelInnerProduct,
}

impl ModelCoefficientSpace {
    /// Return the compiled geometry defining coefficient coordinates.
    #[must_use]
    pub const fn geometry(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return the reconstruction basis spanning this space.
    #[must_use]
    pub const fn basis(&self) -> ReconstructionBasis {
        self.basis
    }

    /// Return the requested model polarization coordinates.
    #[must_use]
    pub const fn polarization(&self) -> &PolarizationContract {
        &self.polarization
    }

    /// Return the declared model-space inner product.
    #[must_use]
    pub const fn inner_product(&self) -> ModelInnerProduct {
        self.inner_product
    }
}

/// Typed codomain of the complete logical measurement operator before W.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilitySampleSpace {
    observation: ObservationSnapshotId,
    inner_product: VisibilityInnerProduct,
}

impl VisibilitySampleSpace {
    /// Return the immutable selected-observation identity defining the samples.
    #[must_use]
    pub const fn observation(self) -> ObservationSnapshotId {
        self.observation
    }

    /// Return the declared unweighted visibility-space inner product.
    #[must_use]
    pub const fn inner_product(self) -> VisibilityInnerProduct {
        self.inner_product
    }
}

/// Stable category of one intrinsically paired measurement transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PairedTransformKind {
    /// Evaluation of the reconstruction basis at visibility frequencies.
    SpectralBasis,
    /// Polarization synthesis/analysis mapping.
    Polarization,
    /// Direction-dependent or scalar instrument response.
    DirectionDependentResponse,
    /// Visibility phase rotation and its conjugate adjoint.
    Phase,
    /// Paired spectral interpolation.
    SpectralResampling,
    /// Paired integration over source channels.
    ChannelIntegration,
}

/// One logical transform whose forward and adjoint directions cannot be separated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairedMeasurementTransform {
    /// Evaluate/reduce one declared reconstruction basis.
    SpectralBasis {
        /// Basis evaluated by prediction and reduced by the adjoint.
        basis: ReconstructionBasis,
    },
    /// Map model polarization coordinates to/from selected correlations.
    PolarizationMapping,
    /// Apply the declared instrument response and its adjoint.
    DirectionDependentResponse {
        /// Exact response included in both directions.
        response: InstrumentResponse,
    },
    /// Apply the compiled prediction phase and its conjugate adjoint.
    PhaseRotation {
        /// Prediction-side phase convention; imaging uses its adjoint.
        convention: VisibilityPhaseConvention,
    },
    /// Interpolate spectra with one explicitly paired rule.
    SpectralResampling {
        /// Nearest or linear paired sampling rule.
        sampling: SpectralSampling,
    },
    /// Integrate source channels and distribute through the paired adjoint.
    ChannelIntegration {
        /// Fixed source-channel count contributing to each output bin.
        channels_per_bin: usize,
    },
}

impl PairedMeasurementTransform {
    /// Return the stable transform category.
    #[must_use]
    pub const fn kind(self) -> PairedTransformKind {
        match self {
            Self::SpectralBasis { .. } => PairedTransformKind::SpectralBasis,
            Self::PolarizationMapping => PairedTransformKind::Polarization,
            Self::DirectionDependentResponse { .. } => {
                PairedTransformKind::DirectionDependentResponse
            }
            Self::PhaseRotation { .. } => PairedTransformKind::Phase,
            Self::SpectralResampling { .. } => PairedTransformKind::SpectralResampling,
            Self::ChannelIntegration { .. } => PairedTransformKind::ChannelIntegration,
        }
    }
}

/// Complete logical A: X -> D together with the declaration defining A*.
///
/// Callers cannot supply a partial, duplicated, or reordered transform list;
/// the problem compiler is the only constructor.
///
/// ```compile_fail
/// use casa_imaging_model::MeasurementOperatorContract;
///
/// let _ = MeasurementOperatorContract::new(Vec::new());
/// ```
///
/// Product operations are a distinct type and cannot enter A or A*.
///
/// ```compile_fail
/// use casa_imaging_model::{PairedMeasurementTransform, ProductBoundaryOperation};
///
/// let _: PairedMeasurementTransform = ProductBoundaryOperation::CorrectPrimaryBeam;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementOperatorContract {
    domain: ModelCoefficientSpace,
    codomain: VisibilitySampleSpace,
    transforms: Box<[PairedMeasurementTransform]>,
}

impl MeasurementOperatorContract {
    /// Return the typed model-coefficient domain X.
    #[must_use]
    pub const fn domain(&self) -> &ModelCoefficientSpace {
        &self.domain
    }

    /// Return the typed unweighted visibility-sample codomain D.
    #[must_use]
    pub const fn codomain(&self) -> VisibilitySampleSpace {
        self.codomain
    }

    /// Return transforms in forward application order; A* uses reverse adjoint order.
    #[must_use]
    pub const fn transforms(&self) -> &[PairedMeasurementTransform] {
        &self.transforms
    }
}

/// Stable identity of one immutable complete-selection weighting generation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WeightingGenerationId([u8; 32]);

impl WeightingGenerationId {
    /// Identity schema version used by the weighting-generation encoder.
    pub const SCHEMA_VERSION: u32 = WEIGHTING_GENERATION_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for WeightingGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WeightingGenerationId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for WeightingGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Snapshot-derived flag and input-weight provenance consumed exclusively by W.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightingSource {
    source: MeasurementSetIdentity,
    flags: FlagPolicy,
    input_weights: WeightColumn,
    flag_generation: LogicalIdentity,
    flag_row_generation: LogicalIdentity,
    input_weight_generation: LogicalIdentity,
}

impl WeightingSource {
    /// Return the immutable source identity.
    #[must_use]
    pub const fn source(self) -> MeasurementSetIdentity {
        self.source
    }

    /// Return the exact exclusion rule owned by W.
    #[must_use]
    pub const fn flags(self) -> FlagPolicy {
        self.flags
    }

    /// Return the exact input-weight column owned by W.
    #[must_use]
    pub const fn input_weights(self) -> WeightColumn {
        self.input_weights
    }

    /// Return the captured `FLAG` generation.
    #[must_use]
    pub const fn flag_generation(self) -> LogicalIdentity {
        self.flag_generation
    }

    /// Return the captured `FLAG_ROW` generation.
    #[must_use]
    pub const fn flag_row_generation(self) -> LogicalIdentity {
        self.flag_row_generation
    }

    /// Return the captured input-weight generation.
    #[must_use]
    pub const fn input_weight_generation(self) -> LogicalIdentity {
        self.input_weight_generation
    }
}

/// Positive-semidefinite data metric W and its frozen global generation.
///
/// Callers cannot construct a weighting operator with a snapshot or source
/// generation different from the compiled problem inputs.
///
/// ```compile_fail
/// use casa_imaging_model::WeightingOperatorContract;
///
/// let _ = WeightingOperatorContract::new();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingOperatorContract {
    generation_id: WeightingGenerationId,
    snapshot: ObservationSnapshotId,
    scheme: WeightingScheme,
    density_scope: WeightDensityScope,
    uv_taper: Option<UvTaper>,
    sources: Box<[WeightingSource]>,
}

impl WeightingOperatorContract {
    /// Return the immutable weighting generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> WeightingGenerationId {
        self.generation_id
    }

    /// Return the selected observation bound into this generation.
    #[must_use]
    pub const fn snapshot(&self) -> ObservationSnapshotId {
        self.snapshot
    }

    /// Return the weighting formula owned by W.
    #[must_use]
    pub const fn scheme(&self) -> WeightingScheme {
        self.scheme
    }

    /// Return the complete-selection density scope. No chunk-local scope exists.
    #[must_use]
    pub const fn density_scope(&self) -> WeightDensityScope {
        self.density_scope
    }

    /// Return the optional UV taper owned by W.
    #[must_use]
    pub const fn uv_taper(&self) -> Option<UvTaper> {
        self.uv_taper
    }

    /// Return exact snapshot-derived source provenance consumed by W.
    #[must_use]
    pub const fn sources(&self) -> &[WeightingSource] {
        &self.sources
    }
}

/// Normalization state of a normal-equation output before product processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalStateNormalization {
    /// A* output has no publication normalization, restoration, or unit conversion.
    Unnormalized,
}

/// Typed codomain of b, g(x), and H before product normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalStateSpace {
    model: ModelCoefficientSpace,
    normalization: NormalStateNormalization,
}

impl NormalStateSpace {
    /// Return the model-coordinate space receiving normal-state values.
    #[must_use]
    pub const fn model(&self) -> &ModelCoefficientSpace {
        &self.model
    }

    /// Return the fixed pre-product normalization state.
    #[must_use]
    pub const fn normalization(&self) -> NormalStateNormalization {
        self.normalization
    }
}

/// Authoritative forms derived from one A/A* pair and one W generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NormalEquationForm {
    /// Right-hand side b = A* W d.
    RightHandSide,
    /// Model residual g(x) = A* W (d - A x).
    Residual,
    /// Logical normal operator H = A* W A.
    NormalOperator,
}

impl NormalEquationForm {
    /// Complete canonical set of normal-equation forms.
    pub const ALL: [Self; 3] = [Self::RightHandSide, Self::Residual, Self::NormalOperator];
}

/// Typed normal-equation contract sharing one paired A/A* and one frozen W.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalEquationContract {
    measurement_operator: MeasurementOperatorContract,
    weighting: WeightingOperatorContract,
    output: NormalStateSpace,
}

impl NormalEquationContract {
    /// Return the complete paired measurement operator A/A*.
    #[must_use]
    pub const fn measurement_operator(&self) -> &MeasurementOperatorContract {
        &self.measurement_operator
    }

    /// Return the sole data metric W used by every normal-equation form.
    #[must_use]
    pub const fn weighting(&self) -> &WeightingOperatorContract {
        &self.weighting
    }

    /// Return the explicitly unnormalized normal-state codomain.
    #[must_use]
    pub const fn output(&self) -> &NormalStateSpace {
        &self.output
    }

    /// Return b, g(x), and H in canonical order.
    #[must_use]
    pub const fn forms(&self) -> [NormalEquationForm; 3] {
        NormalEquationForm::ALL
    }
}

/// Product-only operation forbidden from measurement-operator implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductBoundaryOperation {
    /// Normalize the unnormalized normal state for publication.
    Normalize(ProductNormalization),
    /// Reconcile residual units with the restoring beam.
    ScaleResidual,
    /// Restore a model and residual with the declared beam policy.
    Restore(RestoringBeamPolicy),
    /// Divide a restored image by the product-owned primary-beam response.
    CorrectPrimaryBeam,
    /// Blank product samples outside their declared valid support.
    BlankInvalid,
    /// Convert or attach final published image units.
    ConvertUnits,
}

/// Downstream handoff from unnormalized normal state to the Product Contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductNormalizationBoundary {
    input: NormalStateNormalization,
    operations: Box<[ProductBoundaryOperation]>,
}

impl ProductNormalizationBoundary {
    /// Return the only normal-state input accepted by product normalization.
    #[must_use]
    pub const fn input(&self) -> NormalStateNormalization {
        self.input
    }

    /// Return product-owned operations in canonical dependency order.
    #[must_use]
    pub const fn operations(&self) -> &[ProductBoundaryOperation] {
        &self.operations
    }
}

pub(crate) fn compile_normal_equation(
    geometry: &CompiledGeometry,
    inputs: &ProblemInputIdentities,
    science: &ScientificContract,
    reconstruction: &ReconstructionContract,
    weighting: WeightingContract,
) -> NormalEquationContract {
    let inner_products = science.measurement_equation().inner_products();
    let domain = ModelCoefficientSpace {
        geometry: geometry.geometry_id(),
        basis: reconstruction.basis(),
        polarization: reconstruction.polarization().clone(),
        inner_product: inner_products.model(),
    };
    let codomain = VisibilitySampleSpace {
        observation: inputs.observation(),
        inner_product: inner_products.visibility(),
    };
    let mut transforms = vec![
        PairedMeasurementTransform::SpectralBasis {
            basis: reconstruction.basis(),
        },
        PairedMeasurementTransform::PolarizationMapping,
        PairedMeasurementTransform::DirectionDependentResponse {
            response: science.measurement_equation().instrument_response(),
        },
        PairedMeasurementTransform::PhaseRotation {
            convention: geometry.uvw().prediction_phase(),
        },
    ];
    match science.spectral().sampling() {
        SpectralSampling::Identity => {}
        sampling @ (SpectralSampling::Nearest | SpectralSampling::Linear) => {
            transforms.push(PairedMeasurementTransform::SpectralResampling { sampling });
        }
        SpectralSampling::ChannelAverage { channels_per_bin } => {
            transforms.push(PairedMeasurementTransform::ChannelIntegration { channels_per_bin });
        }
    }
    let measurement_operator = MeasurementOperatorContract {
        domain: domain.clone(),
        codomain,
        transforms: transforms.into_boxed_slice(),
    };
    let weighting = compile_weighting_operator(geometry, inputs, weighting);
    NormalEquationContract {
        measurement_operator,
        weighting,
        output: NormalStateSpace {
            model: domain,
            normalization: NormalStateNormalization::Unnormalized,
        },
    }
}

pub(crate) fn compile_product_boundary(
    requested: &[ProductKind],
    normalization: ProductNormalization,
    restoring_beam: RestoringBeamPolicy,
) -> ProductNormalizationBoundary {
    let restored = requested.contains(&ProductKind::RestoredImage);
    let pb_corrected = requested.contains(&ProductKind::PbCorrectedImage)
        || requested.contains(&ProductKind::PbCorrectedSpectralIndex);
    let mut operations = vec![ProductBoundaryOperation::Normalize(normalization)];
    if restored {
        operations.push(ProductBoundaryOperation::ScaleResidual);
        operations.push(ProductBoundaryOperation::Restore(restoring_beam));
    }
    if pb_corrected {
        operations.push(ProductBoundaryOperation::CorrectPrimaryBeam);
    }
    if pb_corrected
        || matches!(
            normalization,
            ProductNormalization::FlatNoise | ProductNormalization::FlatSky
        )
    {
        operations.push(ProductBoundaryOperation::BlankInvalid);
    }
    operations.push(ProductBoundaryOperation::ConvertUnits);
    ProductNormalizationBoundary {
        input: NormalStateNormalization::Unnormalized,
        operations: operations.into_boxed_slice(),
    }
}

fn compile_weighting_operator(
    geometry: &CompiledGeometry,
    inputs: &ProblemInputIdentities,
    weighting: WeightingContract,
) -> WeightingOperatorContract {
    let snapshot = inputs.observation_snapshot();
    let sources = snapshot
        .sources()
        .iter()
        .map(|source| {
            let columns = source.generations().columns();
            let input_weights = columns.weights();
            let weight_kind = match input_weights {
                WeightColumn::Weight => MsColumnKind::Weight,
                WeightColumn::WeightSpectrum => MsColumnKind::WeightSpectrum,
            };
            WeightingSource {
                source: source.identity(),
                flags: columns.flags(),
                input_weights,
                flag_generation: columns
                    .generation(MsColumnKind::Flag)
                    .expect("compiled observation binds FLAG"),
                flag_row_generation: columns
                    .generation(MsColumnKind::FlagRow)
                    .expect("compiled observation binds FLAG_ROW"),
                input_weight_generation: columns
                    .generation(weight_kind)
                    .expect("compiled observation binds its selected weight column"),
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    WeightingOperatorContract {
        generation_id: weighting_generation_id(
            snapshot.snapshot_id(),
            geometry.geometry_id(),
            weighting,
        ),
        snapshot: snapshot.snapshot_id(),
        scheme: weighting.scheme(),
        density_scope: weighting.density_scope(),
        uv_taper: weighting.uv_taper(),
        sources,
    }
}

fn weighting_generation_id(
    snapshot: ObservationSnapshotId,
    geometry: CompiledGeometryId,
    weighting: WeightingContract,
) -> WeightingGenerationId {
    let mut hasher = Sha256::new();
    hasher.update(WEIGHTING_GENERATION_IDENTITY_DOMAIN);
    hasher.update(WEIGHTING_GENERATION_IDENTITY_VERSION.to_be_bytes());
    hasher.update(snapshot.as_bytes());
    hasher.update(geometry.as_bytes());
    match weighting.scheme() {
        WeightingScheme::Natural => hasher.update([0]),
        WeightingScheme::Uniform => hasher.update([1]),
        WeightingScheme::Briggs { robust } => {
            hasher.update([2]);
            hash_f64(&mut hasher, robust);
        }
        WeightingScheme::BriggsBandwidthTaper { robust } => {
            hasher.update([3]);
            hash_f64(&mut hasher, robust);
        }
    }
    hasher.update([match weighting.density_scope() {
        WeightDensityScope::NotApplicable => 0,
        WeightDensityScope::GlobalSelection => 1,
        WeightDensityScope::PerOutputChannel => 2,
    }]);
    match weighting.uv_taper() {
        None => hasher.update([0]),
        Some(taper) => {
            hasher.update([1]);
            hash_f64(&mut hasher, taper.major_lambda());
            hash_f64(&mut hasher, taper.minor_lambda());
            hash_f64(&mut hasher, taper.position_angle_rad());
        }
    }
    WeightingGenerationId(hasher.finalize().into())
}

fn hash_f64(hasher: &mut Sha256, value: f64) {
    let bits = if value == 0.0 { 0 } else { value.to_bits() };
    hasher.update(bits.to_be_bytes());
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
