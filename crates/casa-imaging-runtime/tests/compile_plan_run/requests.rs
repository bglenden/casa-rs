// SPDX-License-Identifier: LGPL-3.0-or-later

//! Imaging requests, problem specifications and identities shared by the
//! compile/plan/run suite.

use super::*;

pub(crate) fn geometry(reference_pixel: f64) -> GeometryInput {
    geometry_with_shape([reference_pixel, 255.0], ImageShape::new(512, 512))
}

pub(crate) fn geometry_with_shape(
    reference_pixel: [f64; 2],
    image_shape: ImageShape,
) -> GeometryInput {
    geometry_with_shape_and_increment(
        reference_pixel,
        image_shape,
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
    )
}

pub(crate) fn geometry_with_shape_and_increment(
    reference_pixel: [f64; 2],
    image_shape: ImageShape,
    increment_rad: [f64; 2],
) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        reference_pixel,
        increment_rad,
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            image_shape,
            direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
        CentreLaws::new(
            PhaseCentreLaw::Fixed(direction.reference_direction()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

pub(crate) fn request(observation: u8) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry(255.0), default_references())
}

pub(crate) fn request_with_geometry(observation: u8, geometry: GeometryInput) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry, default_references())
}

pub(crate) fn default_references() -> Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>
{
    vec![(ReferenceDataKind::Measures, identity(90))]
}

pub(crate) fn request_with_geometry_and_references(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
) -> ImagingRequest {
    request_with_geometry_references_and_weighting(
        observation,
        geometry,
        references,
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
    )
}

pub(crate) fn request_with_geometry_references_and_weighting(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        references,
        weighting,
        vec![ProductKind::Psf],
        ModelColumnWrite::Disabled,
    )
}

pub(crate) fn request_with_products(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        ModelColumnWrite::Disabled,
    )
}

pub(crate) fn request_with_products_and_initial_model(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
    model: ModelStateIdentity,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_and_input(
        observation,
        geometry,
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        ModelColumnWrite::Disabled,
        model,
    )
}

pub(crate) fn request_with_products_and_model(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        model_column_write,
    )
}

pub(crate) fn request_with_geometry_references_weighting_products_and_model(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_and_input(
        observation,
        geometry,
        references,
        weighting,
        products,
        model_column_write,
        ModelStateIdentity::Empty,
    )
}

pub(crate) fn request_with_geometry_references_weighting_products_model_write_and_input(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
    model: ModelStateIdentity,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_input_and_source_count(
        observation,
        geometry,
        references,
        weighting,
        products,
        model_column_write,
        model,
        1,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn request_with_geometry_references_weighting_products_model_write_input_and_source_count(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
    model: ModelStateIdentity,
    source_count: usize,
) -> ImagingRequest {
    let specification = standard_problem_specification(weighting, products, model_column_write);
    ImagingRequest::new(
        specification,
        geometry,
        problem_inputs_with_source_count(observation, references, model, source_count),
        model_lifecycle(model),
    )
}

pub(crate) fn standard_problem_specification(
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ProblemSpecification {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::UnorderedWithinBudget,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting,
        ProductRequirements::new(
            products,
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(model_column_write),
        numerics,
    )
}

pub(crate) fn mtmfs_problem_specification(small_scale_bias: f64) -> ProblemSpecification {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::UnorderedWithinBudget,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0, 5.0],
                small_scale_bias,
            },
            ReconstructionControls::new(8, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf, ProductKind::Residual, ProductKind::Model],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        numerics,
    )
}

pub(crate) fn registry(byte: u8) -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([byte; 32])
}

pub(crate) fn cost_model(byte: u8) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([byte; 32])
}

pub(crate) fn implementation(byte: u8) -> WorkImplementationId {
    WorkImplementationId::new(format!("test-cpu-{byte}"))
}
