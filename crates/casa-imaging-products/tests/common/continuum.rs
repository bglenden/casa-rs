// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiled continuum and cube problems for the product-generation tests:
//! an 8 × 8 Stokes-I main domain at J2000 (1.0, −0.5) with 1 µrad cells,
//! natural weighting and unit-response normalization.

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompiledProblem, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    InstrumentResponse, MeasurementEquationContract, ModelBounds, ModelColumnWrite,
    ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    NumericPrecision, NumericalStage, NumericsContract, ObservationSnapshotInput,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PolarizationContract,
    PolarizationCoordinate, ProblemInput, ProblemInputIdentities, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};

use super::observation::{source, validity};

/// `[width, height]` of the main image domain.
pub const SHAPE: [usize; 2] = [8, 8];

/// The image axis order of every fixture domain.
pub fn axes() -> AxisOrder {
    AxisOrder::new([
        ImageAxis::DirectionLongitude,
        ImageAxis::DirectionLatitude,
        ImageAxis::Polarization,
        ImageAxis::Spectral,
    ])
}

/// A one-plane dirty continuum problem with per-plane restoring beams.
pub fn continuum_problem(observation: u8, products: &[ProductKind]) -> CompiledProblem {
    continuum_problem_with_policy(observation, products, RestoringBeamPolicy::PerPlane)
}

/// A one-plane dirty continuum problem with restoring-beam policy
/// `restoring_beam`.
pub fn continuum_problem_with_policy(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
) -> CompiledProblem {
    continuum_problem_with_reconstruction(
        observation,
        products,
        restoring_beam,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
    )
}

/// A one-domain problem of `channels` output channels from 1.4 GHz in 1 MHz
/// steps with the given response, basis and algorithm.
pub fn continuum_problem_with_reconstruction(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
    response: InstrumentResponse,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    channels: usize,
) -> CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [(SHAPE[0] / 2) as f64, (SHAPE[1] / 2) as f64],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    continuum_problem_with_domains_and_reconstruction(
        observation,
        products,
        restoring_beam,
        response,
        basis,
        algorithm,
        channels,
        [1.4e9, 1.0e6],
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            direction,
            FacetLayout::Single,
            axes(),
        )],
    )
}

/// A problem over explicit image `domains` and a linear spectral axis of
/// `channels` channels starting at `frequency[0]` in steps of `frequency[1]`.
#[allow(clippy::too_many_arguments)]
pub fn continuum_problem_with_domains_and_reconstruction(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
    response: InstrumentResponse,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    channels: usize,
    frequency: [f64; 2],
    domains: Vec<ImageDomainSpec>,
) -> CompiledProblem {
    let controls = if matches!(algorithm, ReconstructionAlgorithm::Mtmfs { .. }) {
        ReconstructionControls::new(1, 1.0, 0.0)
    } else {
        ReconstructionControls::new(0, 1.0, 0.0)
    };
    let phase_centre = domains
        .iter()
        .find(|domain| domain.role() == &ImageDomainRole::Main)
        .expect("one main domain")
        .direction()
        .reference_direction();
    let geometry = GeometryInput::new(
        domains,
        CentreLaws::new(
            PhaseCentreLaw::Fixed(phase_centre),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels,
                reference_pixel: 0.0,
                reference_frequency_hz: frequency[0],
                increment_hz: frequency[1],
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(observation, "products")],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation snapshot");
    compile(ProblemInput::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(
                    SpectralSamplingLaw::IDENTITY,
                    if restoring_beam == RestoringBeamPolicy::Common {
                        SpectralCoupling::CommonRestoringBeam
                    } else {
                        SpectralCoupling::Independent
                    },
                ),
                MeasurementEquationContract::new(
                    response,
                    DeclaredInnerProducts::new(
                        ModelInnerProduct::HermitianEuclidean,
                        VisibilityInnerProduct::HermitianEuclidean,
                    ),
                ),
            ),
            ReconstructionContract::new(
                basis,
                algorithm,
                controls,
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                products.to_vec(),
                ProductNormalization::UnitResponse,
                restoring_beam,
                validity(0.1),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::UnorderedWithinBudget,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                    .collect(),
            ),
        ),
        geometry,
        ProblemInputIdentities::new(snapshot),
        ModelLifecycleRequirements::new(
            ModelBounds::new(4_096, 4_096, 4_096, 4_096, 1.0e30, 1.0e30).expect("valid bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile T22 continuum problem")
}

/// The main domain of [`two_domain_problem`].
pub fn two_domain_main_direction() -> DirectionCoordinateSpec {
    DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [4.0, 4.0],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    )
}

/// The 6 × 4 "east" outlier domain of [`two_domain_problem`].
pub fn two_domain_outlier_direction() -> DirectionCoordinateSpec {
    DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.02, -0.48),
        [3.0, 2.0],
        [-1.5e-6, 1.5e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    )
}

/// `[width, height]` of the outlier domain of [`two_domain_problem`].
pub const OUTLIER_SHAPE: [usize; 2] = [6, 4];

/// The products every two-domain fixture requests.
pub const TWO_DOMAIN_PRODUCTS: [ProductKind; 6] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::SumWeights,
    ProductKind::Weight,
    ProductKind::Mask,
];

/// A dirty one-plane problem over the 8 × 8 main domain and the 6 × 4 "east"
/// outlier, with no restoring beam.
pub fn two_domain_problem(observation: u8) -> CompiledProblem {
    let domains = vec![
        ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            two_domain_main_direction(),
            FacetLayout::Single,
            axes(),
        ),
        ImageDomainSpec::new(
            ImageDomainRole::Outlier("east".into()),
            ImageShape::new(OUTLIER_SHAPE[0], OUTLIER_SHAPE[1]),
            two_domain_outlier_direction(),
            FacetLayout::Single,
            axes(),
        ),
    ];
    continuum_problem_with_domains_and_reconstruction(
        observation,
        &TWO_DOMAIN_PRODUCTS,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
        [1.4e9, 1.0e6],
        domains,
    )
}
