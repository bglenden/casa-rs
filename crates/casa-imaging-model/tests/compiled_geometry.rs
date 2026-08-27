// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileGeometryError, CompileProblemError, DeclaredInnerProducts,
    DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame, DopplerConvention, Epoch, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, ImagingRequest, InstrumentResponse, ItrfPosition, LogicalIdentity,
    MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, ProblemSpecification, ProductKind,
    ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, StageErrorBudget, TimeScale, UvwAxes, UvwCoordinateLaw,
    UvwUnit, VisibilityInnerProduct, VisibilityPhaseConvention, WeightDensityScope,
    WeightingContract, WeightingScheme, compile,
};

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use common::{identity, problem_inputs};
use model_lifecycle_fixture::model_lifecycle;

fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
    casa_imaging_model::ProductValidityPolicies::new(
        casa_imaging_model::PrimaryBeamValidityPolicy::new(
            0.2,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    )
}

fn observation_pointing() -> ObservationPointingLaw {
    ObservationPointingLaw::new(
        PointingDirectionColumn::Direction,
        PointingDirectionSemantic::AntennaBoresight,
        PointingTimeSampling::VisibilityTimeCentroid,
        PointingInterpolation::GreatCircleShortestArc,
        PointingExtrapolation::Reject,
        MissingPointingPolicy::Reject,
    )
}

fn geometry() -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(
            DirectionFrame::J2000,
            std::f64::consts::FRAC_PI_2,
            -0.523_598_775_598_298_8,
        ),
        [255.0, 255.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(512, 512),
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
            PointingCentreLaw::Observation(observation_pointing()),
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.420_405_751_77e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn conversion_anchor(mjd_days: f64) -> SpectralFrameAnchor {
    SpectralFrameAnchor::Conversion {
        epoch: Epoch::new(mjd_days, TimeScale::Utc),
        direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        observatory_position: ItrfPosition::new(-1_601_188.0, -5_041_977.0, 3_554_875.0),
    }
}

fn request(
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, LogicalIdentity)>,
) -> ImagingRequest {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    let inputs = problem_inputs(1, references, ModelStateIdentity::Empty);
    ImagingRequest::new(
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
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                product_validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry,
        inputs,
        model_lifecycle(ModelStateIdentity::Empty),
    )
}

#[test]
fn compiles_exact_axis_centre_uvw_and_continuum_spectral_laws() {
    let problem = compile(request(geometry(), Vec::new())).expect("compile geometry");
    let compiled = problem.geometry();

    assert_ne!(compiled.geometry_id().as_bytes(), [0; 32]);
    assert_eq!(compiled.domains().len(), 1);
    assert_eq!(compiled.domains()[0].facets().len(), 1);
    assert_eq!(
        compiled.domains()[0].axes().positions(),
        &[
            ImageAxis::DirectionLongitude,
            ImageAxis::DirectionLatitude,
            ImageAxis::Polarization,
            ImageAxis::Spectral
        ]
    );
    assert_eq!(compiled.uvw(), UvwCoordinateLaw::PhaseTrackingCentre);
    assert_eq!(
        compiled.centres().pointing(),
        &PointingCentreLaw::Observation(observation_pointing())
    );
    // Synthesis Imaging II, p. 45 defines u east, v north, and w toward the
    // phase centre. The law is metadata only; no evaluated row arrays enter it.
    assert_eq!(compiled.uvw().unit(), UvwUnit::Metres);
    assert_eq!(compiled.uvw().axes(), UvwAxes::EastNorthPhaseTrackingCentre);
    assert_eq!(
        compiled.uvw().prediction_phase(),
        VisibilityPhaseConvention::NegativeTwoPiFrequencyDelay
    );
    assert_eq!(
        compiled.spectral().rest_frequency(),
        RestFrequency::NotApplicable
    );
    assert_eq!(
        compiled.spectral().doppler_convention(),
        DopplerConvention::NotApplicable
    );
}

#[test]
fn exact_wcs_metadata_round_trips_through_compilation() {
    let first = compile(request(geometry(), Vec::new())).expect("compile source geometry");
    let compiled = first.geometry();
    let domain = &compiled.domains()[0];
    let direction = domain.direction();

    // Greisen & Calabretta (2002), A&A 395, 1061-1075 treats CRPIX, CRVAL,
    // CDELT, PC, and pole metadata as distinct WCS terms. Keep each exact;
    // neither infer a matrix nor absorb signed increments into another field.
    assert_eq!(direction.reference_pixel(), [255.0, 255.0]);
    assert_eq!(
        direction.increment_rad(),
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6]
    );
    assert_eq!(direction.pc(), [[1.0, 0.0], [0.0, 1.0]]);
    assert_eq!(direction.pole_deg(), [180.0, 0.0]);
    assert_eq!(
        direction.reference_direction().frame(),
        DirectionFrame::J2000
    );

    let reconstructed = GeometryInput::new(
        vec![ImageDomainSpec::new(
            domain.role().clone(),
            domain.shape(),
            direction,
            FacetLayout::Single,
            domain.axes().clone(),
        )],
        compiled.centres().clone(),
        compiled.uvw(),
        compiled.spectral().clone(),
    );
    let second = compile(request(reconstructed, Vec::new())).expect("compile round trip");
    assert_eq!(compiled.geometry_id(), second.geometry().geometry_id());
}

#[test]
fn every_observation_pointing_semantic_is_identity_bearing() {
    let variants = [
        observation_pointing(),
        observation_pointing().with_direction(
            PointingDirectionColumn::Target,
            PointingDirectionSemantic::TrackingTarget,
        ),
        observation_pointing().with_time_sampling(PointingTimeSampling::VisibilityTime),
        observation_pointing().with_interpolation(PointingInterpolation::Nearest),
        observation_pointing().with_extrapolation(PointingExtrapolation::HoldNearest),
        observation_pointing().with_missing(MissingPointingPolicy::UsePhaseTrackingCentre),
    ];
    let compiled = variants.map(|law| {
        let input = geometry().with_centres(CentreLaws::new(
            geometry().centres().phase_tracking().clone(),
            geometry().centres().delay().clone(),
            PointingCentreLaw::Observation(law),
        ));
        compile(request(input, Vec::new())).expect("compile pointing law")
    });

    for first in 0..compiled.len() {
        for second in first + 1..compiled.len() {
            assert_ne!(
                compiled[first].geometry().geometry_id(),
                compiled[second].geometry().geometry_id()
            );
            assert_ne!(compiled[first].problem_id(), compiled[second].problem_id());
        }
    }
}

#[test]
fn pointing_column_and_semantic_must_match() {
    let inconsistent = geometry().with_centres(CentreLaws::new(
        geometry().centres().phase_tracking().clone(),
        geometry().centres().delay().clone(),
        PointingCentreLaw::Observation(observation_pointing().with_direction(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::TrackingTarget,
        )),
    ));

    assert!(matches!(
        compile(request(inconsistent, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InconsistentPointingDirection
        ))
    ));
}

#[test]
fn spectral_axes_define_exact_channel_boundaries_and_identity() {
    let linear = compile(request(geometry(), Vec::new())).expect("compile linear axis");
    assert_eq!(linear.geometry().spectral().output_channels(), 1);
    assert_eq!(
        linear.geometry().spectral().channel_boundary_hz(0),
        Some(1.419_905_751_77e9)
    );
    assert_eq!(
        linear.geometry().spectral().channel_boundary_hz(1),
        Some(1.420_905_751_77e9)
    );
    assert_eq!(linear.geometry().spectral().channel_boundary_hz(2), None);

    let tabular = geometry().with_spectral(geometry().spectral().clone().with_wcs(
        SpectralWcs::Tabular {
            channel_centres_hz: vec![1.05e9, 1.2e9],
            channel_boundaries_hz: vec![1.0e9, 1.1e9, 1.3e9],
        },
    ));
    let first = compile(request(tabular.clone(), Vec::new())).expect("compile tabular axis");
    assert_eq!(first.geometry().spectral().output_channels(), 2);
    assert_eq!(
        first.geometry().spectral().channel_centre_hz(0),
        Some(1.05e9)
    );
    assert_eq!(
        first.geometry().spectral().channel_centre_hz(1),
        Some(1.2e9)
    );
    assert_eq!(
        first.geometry().spectral().channel_boundary_hz(0),
        Some(1.0e9)
    );
    assert_eq!(
        first.geometry().spectral().channel_boundary_hz(1),
        Some(1.1e9)
    );
    assert_eq!(
        first.geometry().spectral().channel_boundary_hz(2),
        Some(1.3e9)
    );
    assert_eq!(first.geometry().spectral().channel_boundary_hz(3), None);

    let changed_boundary = tabular
        .clone()
        .with_spectral(tabular.spectral().clone().with_wcs(SpectralWcs::Tabular {
            channel_centres_hz: vec![1.05e9, 1.2e9],
            channel_boundaries_hz: vec![1.0e9, 1.1e9, 1.31e9],
        }));
    let second = compile(request(changed_boundary, Vec::new())).expect("compile changed boundary");
    let changed_centre = tabular
        .clone()
        .with_spectral(tabular.spectral().clone().with_wcs(SpectralWcs::Tabular {
            channel_centres_hz: vec![1.04e9, 1.2e9],
            channel_boundaries_hz: vec![1.0e9, 1.1e9, 1.3e9],
        }));
    let third = compile(request(changed_centre, Vec::new())).expect("compile changed centre");
    assert_ne!(
        first.geometry().geometry_id(),
        second.geometry().geometry_id()
    );
    assert_ne!(first.problem_id(), second.problem_id());
    assert_ne!(
        first.geometry().geometry_id(),
        third.geometry().geometry_id()
    );
    assert_ne!(first.problem_id(), third.problem_id());
}

#[test]
fn canonical_geometry_identity_normalizes_signed_zero_and_outlier_order() {
    let mut first = geometry();
    let main = first.domains()[0].clone();
    let mut east = main
        .clone()
        .with_role(ImageDomainRole::Outlier("east".into()));
    let east_direction = east.direction().with_reference_pixel([-0.0, 255.0]);
    east = east.with_direction(east_direction);
    let west = main
        .clone()
        .with_role(ImageDomainRole::Outlier("west".into()));
    first = first.with_domains(vec![main.clone(), west.clone(), east.clone()]);
    let main_direction = *main.direction();
    let equivalent_pole = DirectionCoordinateSpec::new(
        main_direction.projection(),
        main_direction.reference_direction(),
        main_direction.reference_pixel(),
        main_direction.increment_rad(),
        main_direction.pc(),
        [540.0, 0.0],
    );
    let second = geometry().with_domains(vec![east, main.with_direction(equivalent_pole), west]);

    let first = compile(request(first, Vec::new())).expect("compile first");
    let second = compile(request(second, Vec::new())).expect("compile second");
    assert_eq!(
        first.geometry().geometry_id(),
        second.geometry().geometry_id()
    );
    assert_eq!(first.problem_id(), second.problem_id());
}

#[test]
fn frame_transform_requires_bound_measures_and_changes_identity_with_anchor() {
    let unanchored = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_output_frame(FrequencyFrame::Lsrk),
    );
    assert!(matches!(
        compile(request(unanchored, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InconsistentSpectralAnchor
        ))
    ));

    let invalid_anchor = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_output_frame(FrequencyFrame::Lsrk)
            .with_anchor(SpectralFrameAnchor::Conversion {
                epoch: Epoch::new(59_000.25, TimeScale::Utc),
                direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                observatory_position: ItrfPosition::new(0.0, 0.0, 0.0),
            }),
    );
    assert!(matches!(
        compile(request(
            invalid_anchor,
            vec![(ReferenceDataKind::Measures, identity(7))],
        )),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InvalidSpectralAnchor
        ))
    ));

    let transform = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_output_frame(FrequencyFrame::Lsrk)
            .with_anchor(conversion_anchor(59_000.25)),
    );
    assert!(matches!(
        compile(request(transform.clone(), Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::MissingMeasuresReference
        ))
    ));

    let references = vec![(ReferenceDataKind::Measures, identity(7))];
    let first = compile(request(transform.clone(), references.clone())).expect("compile transform");
    let shifted_spectral = transform
        .spectral()
        .clone()
        .with_anchor(conversion_anchor(59_001.25));
    let shifted = transform.with_spectral(shifted_spectral);
    let second = compile(request(shifted, references)).expect("compile shifted anchor");
    assert_ne!(
        first.geometry().geometry_id(),
        second.geometry().geometry_id()
    );
}

#[test]
fn topo_barycentric_and_lsrk_frames_and_reference_snapshots_are_identity_bearing() {
    // Pihlstrom, Essential Radio Astronomy for Interferometry (2024), slides
    // 40-42 distinguishes TOPO, BARY, and LSR frames and the frame context
    // needed to convert them. T06 records that context; T36 will evaluate it.
    for output in [FrequencyFrame::Barycentric, FrequencyFrame::Lsrk] {
        let transformed = geometry().with_spectral(
            geometry()
                .spectral()
                .clone()
                .with_output_frame(output)
                .with_anchor(conversion_anchor(59_000.25)),
        );
        let first = compile(request(
            transformed.clone(),
            vec![(ReferenceDataKind::Measures, identity(7))],
        ))
        .expect("compile first Measures snapshot");
        let second = compile(request(
            transformed,
            vec![(ReferenceDataKind::Measures, identity(8))],
        ))
        .expect("compile second Measures snapshot");

        assert_eq!(
            first.geometry().spectral().source_frame(),
            FrequencyFrame::Topocentric
        );
        assert_eq!(first.geometry().spectral().output_frame(), output);
        assert_eq!(first.geometry().measures_reference(), Some(identity(7)));
        assert_ne!(
            first.geometry().geometry_id(),
            second.geometry().geometry_id()
        );
    }
}

#[test]
fn facet_windows_cover_the_domain_exactly_and_non_divisible_layouts_fail_closed() {
    let faceted = geometry().with_domains(vec![geometry().domains()[0].clone().with_facets(
        FacetLayout::Regular {
            columns: 2,
            rows: 4,
        },
    )]);
    let compiled = compile(request(faceted, Vec::new())).expect("compile facets");
    let facets = compiled.geometry().domains()[0].facets();
    assert_eq!(facets.len(), 8);
    assert_eq!(facets.first().expect("first").origin(), [0, 0]);
    assert_eq!(facets.last().expect("last").end_exclusive(), [512, 512]);

    let invalid = geometry().with_domains(vec![geometry().domains()[0].clone().with_facets(
        FacetLayout::Regular {
            columns: 3,
            rows: 1,
        },
    )]);
    assert!(matches!(
        compile(request(invalid, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::NonDivisibleFacetLayout { .. }
        ))
    ));
}

#[test]
fn line_velocity_metadata_is_explicit_and_fail_closed() {
    let line = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_rest_frequency(RestFrequency::Line {
                hertz: 1.420_405_751_77e9,
            })
            .with_doppler_convention(DopplerConvention::Radio),
    );
    compile(request(line, Vec::new())).expect("compile line law");

    let inconsistent = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_rest_frequency(RestFrequency::NotApplicable)
            .with_doppler_convention(DopplerConvention::Radio),
    );
    assert!(matches!(
        compile(request(inconsistent, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InconsistentVelocityMetadata
        ))
    ));
}

#[test]
fn invalid_shapes_axes_direction_matrices_and_spectral_tables_fail_closed() {
    let domain = geometry().domains()[0].clone();
    let empty = geometry().with_domains(vec![domain.clone().with_shape(ImageShape::new(0, 512))]);
    assert!(matches!(
        compile(request(empty, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::EmptyImageDomain
        ))
    ));

    let duplicate_axes = geometry().with_domains(vec![domain.clone().with_axes(AxisOrder::new([
        ImageAxis::DirectionLongitude,
        ImageAxis::DirectionLatitude,
        ImageAxis::Spectral,
        ImageAxis::Spectral,
    ]))]);
    assert!(matches!(
        compile(request(duplicate_axes, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InvalidAxisOrder
        ))
    ));

    let valid_direction = *domain.direction();
    let singular_direction = DirectionCoordinateSpec::new(
        valid_direction.projection(),
        valid_direction.reference_direction(),
        valid_direction.reference_pixel(),
        valid_direction.increment_rad(),
        [[1.0, 2.0], [2.0, 4.0]],
        valid_direction.pole_deg(),
    );
    let singular = geometry().with_domains(vec![domain.with_direction(singular_direction)]);
    assert!(matches!(
        compile(request(singular, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::SingularDirectionMatrix
        ))
    ));

    let non_monotonic = geometry().with_spectral(geometry().spectral().clone().with_wcs(
        SpectralWcs::Tabular {
            channel_centres_hz: vec![1.05e9, 1.075e9],
            channel_boundaries_hz: vec![1.0e9, 1.1e9, 1.05e9],
        },
    ));
    assert!(matches!(
        compile(request(non_monotonic, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InvalidSpectralWcs
        ))
    ));

    let missing_endpoint_width = geometry().with_spectral(geometry().spectral().clone().with_wcs(
        SpectralWcs::Tabular {
            channel_centres_hz: vec![1.05e9, 1.15e9],
            channel_boundaries_hz: vec![1.0e9, 1.1e9],
        },
    ));
    assert!(matches!(
        compile(request(missing_endpoint_width, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InvalidSpectralWcs
        ))
    ));

    let collapsed_linear_axis =
        geometry().with_spectral(geometry().spectral().clone().with_wcs(SpectralWcs::Linear {
            channels: 2,
            reference_pixel: 0.0,
            reference_frequency_hz: 1.0e300,
            increment_hz: 1.0,
        }));
    assert!(matches!(
        compile(request(collapsed_linear_axis, Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InvalidSpectralWcs
        ))
    ));
}

#[test]
fn ephemeris_centre_laws_require_and_identify_one_bound_snapshot() {
    let moving = geometry().with_centres(CentreLaws::new(
        PhaseCentreLaw::Ephemeris("Mars".into()),
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::PhaseTrackingCentre,
    ));
    assert!(matches!(
        compile(request(moving.clone(), Vec::new())),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::MissingEphemerisReference
        ))
    ));

    let first = compile(request(
        moving.clone(),
        vec![(ReferenceDataKind::Ephemeris, identity(9))],
    ))
    .expect("compile moving centre");
    let second = compile(request(
        moving,
        vec![(ReferenceDataKind::Ephemeris, identity(10))],
    ))
    .expect("compile changed ephemeris");
    assert_eq!(first.geometry().ephemeris_reference(), Some(identity(9)));
    assert_ne!(
        first.geometry().geometry_id(),
        second.geometry().geometry_id()
    );
}
