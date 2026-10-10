// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed measurement-equation, weighting, normal-state, and product-boundary contracts.
//!
//! The measurement operator is always represented as one composition of paired
//! forward/adjoint transforms. Visibility flags, input weights, UV tapering,
//! and complete-selection density weighting belong only to [`WeightingOperatorContract`].
//! Its output is an explicitly unnormalized normal-state space; publication
//! normalization and restoration remain beyond [`ProductNormalizationBoundary`].

use crate::{
    compiled_problem::{
        AwProjectionContract, InstrumentModel, InstrumentResponse, PolarizationContract,
        ProductKind, ProductNormalization, ReconstructionBasis, ReconstructionContract,
        RestoringBeamPolicy, ScientificContract, SpectralKernel, SpectralSamplingLaw, UvTaper,
        WProjectionContract, WeightDensityScope, WeightingContract, WeightingScheme,
    },
    geometry::{CompiledGeometry, VisibilityPhaseConvention},
    observation::{FlagPolicy, ObservationSnapshot, WeightColumn},
};

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
    basis: ReconstructionBasis,
    polarization: PolarizationContract,
    inner_product: ModelInnerProduct,
}

impl ModelCoefficientSpace {
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
    inner_product: VisibilityInnerProduct,
}

impl VisibilitySampleSpace {
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
    /// Source-backed feed-basis and parallactic-angle response.
    FeedResponse,
    /// Direction-dependent or scalar instrument response.
    DirectionDependentResponse,
    /// Visibility phase rotation and its conjugate adjoint.
    Phase,
    /// Paired spectral interpolation.
    SpectralResampling,
    /// Paired integration over source channels.
    ChannelIntegration,
    /// W-dependent convolution and its conjugate adjoint.
    WProjection,
    /// Prepared convolution-function A/W response and its conjugate adjoint.
    AwProjection,
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
    /// Rotate the sky coherency into/out of the source-backed feed basis.
    FeedResponse,
    /// Apply the declared instrument response and its adjoint.
    DirectionDependentResponse {
        /// Exact response included in both directions.
        response: InstrumentResponse,
        /// Exact versioned power-response law included in both directions.
        instrument_model: Option<InstrumentModel>,
    },
    /// Apply the compiled prediction phase and its conjugate adjoint.
    PhaseRotation {
        /// Prediction-side phase convention; imaging uses its adjoint.
        convention: VisibilityPhaseConvention,
    },
    /// Interpolate spectra with one explicitly paired rule.
    SpectralResampling {
        /// Nearest or linear paired sampling rule.
        sampling: SpectralSamplingLaw,
    },
    /// Integrate source channels and distribute through the paired adjoint.
    ChannelIntegration {
        /// Fixed source-channel count contributing to each output bin.
        channels_per_bin: usize,
    },
    /// Apply one W-dependent convolution family in both directions.
    WProjection {
        /// Exact W envelope and plane identity compiled for this problem.
        contract: WProjectionContract,
    },
    /// Apply one validated prepared-CF A/W family in both directions.
    AwProjection {
        /// Exact A/W terms and coordinate envelope compiled for this problem.
        contract: AwProjectionContract,
    },
}

impl PairedMeasurementTransform {
    /// Return the stable transform category.
    #[must_use]
    pub const fn kind(self) -> PairedTransformKind {
        match self {
            Self::SpectralBasis { .. } => PairedTransformKind::SpectralBasis,
            Self::PolarizationMapping => PairedTransformKind::Polarization,
            Self::FeedResponse => PairedTransformKind::FeedResponse,
            Self::DirectionDependentResponse { .. } => {
                PairedTransformKind::DirectionDependentResponse
            }
            Self::PhaseRotation { .. } => PairedTransformKind::Phase,
            Self::SpectralResampling { .. } => PairedTransformKind::SpectralResampling,
            Self::ChannelIntegration { .. } => PairedTransformKind::ChannelIntegration,
            Self::WProjection { .. } => PairedTransformKind::WProjection,
            Self::AwProjection { .. } => PairedTransformKind::AwProjection,
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

/// Snapshot-derived flag and input-weight columns consumed exclusively by W.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightingSource {
    source: usize,
    flags: FlagPolicy,
    input_weights: WeightColumn,
}

impl WeightingSource {
    /// Return the source's position in the snapshot.
    #[must_use]
    pub const fn source(self) -> usize {
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
}

/// Positive-semidefinite data metric W, compiled from the selected observation.
///
/// ```compile_fail
/// use casa_imaging_model::WeightingOperatorContract;
///
/// let _ = WeightingOperatorContract::new();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct WeightingOperatorContract {
    visibility_inner_product: VisibilityInnerProduct,
    scheme: WeightingScheme,
    density_scope: WeightDensityScope,
    casa_cube_density_padding: Option<usize>,
    uv_taper: Option<UvTaper>,
    sources: Box<[WeightingSource]>,
}

impl WeightingOperatorContract {
    /// Return the visibility-space inner-product law under which W is defined.
    #[must_use]
    pub const fn visibility_inner_product(&self) -> VisibilityInnerProduct {
        self.visibility_inner_product
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

    /// Return the bound CASA cube law's metadata-derived padding per side.
    #[must_use]
    pub const fn casa_cube_density_padding(&self) -> Option<usize> {
        self.casa_cube_density_padding
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
    observation: &ObservationSnapshot,
    science: &ScientificContract,
    reconstruction: &ReconstructionContract,
    weighting: WeightingContract,
) -> NormalEquationContract {
    let inner_products = science.measurement_equation().inner_products();
    let domain = ModelCoefficientSpace {
        basis: reconstruction.basis(),
        polarization: reconstruction.polarization().clone(),
        inner_product: inner_products.model(),
    };
    let codomain = VisibilitySampleSpace {
        inner_product: inner_products.visibility(),
    };
    let mut transforms = vec![
        PairedMeasurementTransform::SpectralBasis {
            basis: reconstruction.basis(),
        },
        PairedMeasurementTransform::PolarizationMapping,
        PairedMeasurementTransform::FeedResponse,
        PairedMeasurementTransform::DirectionDependentResponse {
            response: science.measurement_equation().instrument_response(),
            instrument_model: science.instrument_model(),
        },
        PairedMeasurementTransform::PhaseRotation {
            convention: geometry.uvw().prediction_phase(),
        },
    ];
    match science.spectral().sampling().kernel() {
        SpectralKernel::Identity => {}
        SpectralKernel::Nearest | SpectralKernel::Linear | SpectralKernel::Cubic => {
            let sampling = science.spectral().sampling();
            transforms.push(PairedMeasurementTransform::SpectralResampling { sampling });
        }
        SpectralKernel::ChannelIntegration { maximum_terms } => {
            transforms.push(PairedMeasurementTransform::ChannelIntegration {
                channels_per_bin: maximum_terms,
            });
        }
    }
    if let Some(contract) = science.measurement_equation().w_projection() {
        transforms.push(PairedMeasurementTransform::WProjection { contract });
    }
    if let Some(contract) = science.measurement_equation().aw_projection() {
        transforms.push(PairedMeasurementTransform::AwProjection { contract });
    }
    let measurement_operator = MeasurementOperatorContract {
        domain: domain.clone(),
        codomain,
        transforms: transforms.into_boxed_slice(),
    };
    let weighting = compile_weighting_operator(observation, weighting, inner_products.visibility());
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
    observation: &ObservationSnapshot,
    weighting: WeightingContract,
    visibility_inner_product: VisibilityInnerProduct,
) -> WeightingOperatorContract {
    let sources = observation
        .sources()
        .iter()
        .map(|source| WeightingSource {
            source: source.input_ordinal(),
            flags: source.columns().flags(),
            input_weights: source.columns().weights(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    WeightingOperatorContract {
        visibility_inner_product,
        scheme: weighting.scheme(),
        density_scope: weighting.density_scope(),
        casa_cube_density_padding: weighting.casa_cube_density_padding(),
        uv_taper: weighting.uv_taper(),
        sources,
    }
}
