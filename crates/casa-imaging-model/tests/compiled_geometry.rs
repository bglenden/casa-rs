// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileGeometryError, CompileProblemError, DeclaredInnerProducts,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, Epoch, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, InstrumentResponse, ItrfPosition, MeasurementEquationContract,
    MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct, NumericPrecision, NumericalStage,
    NumericsContract, ObservationPointingLaw, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemInput, ProblemSpecification, ProductKind, ProductNormalization, ProductRequirements,
    Projection, PsfPhaseCentreLaw, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RestFrequency,
    RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, StageErrorBudget, TimeScale, UvwAxes, UvwCoordinateLaw, UvwUnit,
    VisibilityInnerProduct, VisibilityPhaseConvention, WeightDensityScope, WeightingContract,
    WeightingScheme, compile,
};

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use common::observation_snapshot;
use model_lifecycle_fixture::model_lifecycle;

fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
    casa_imaging_model::ProductValidityPolicies::new(
        casa_imaging_model::PrimaryBeamValidityPolicy::new(
            0.2,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
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

fn request(geometry: GeometryInput) -> ProblemInput {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    ProblemInput::new(
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
        observation_snapshot(1),
        model_lifecycle(),
    )
}

#[test]
fn compiles_exact_axis_centre_uvw_and_continuum_spectral_laws() {
    let problem = compile(request(geometry())).expect("compile geometry");
    let compiled = problem.geometry();

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
    let first = compile(request(geometry())).expect("compile source geometry");
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
    let second = compile(request(reconstructed)).expect("compile round trip");
    assert_eq!(compiled, second.geometry());
}

#[test]
fn every_observation_pointing_semantic_is_retained() {
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
            PointingCentreLaw::Observation(law),
        ));
        compile(request(input)).expect("compile pointing law")
    });

    for first in 0..compiled.len() {
        for second in first + 1..compiled.len() {
            assert_ne!(compiled[first].geometry(), compiled[second].geometry());
        }
    }
}

#[test]
fn pointing_column_and_semantic_must_match() {
    let inconsistent = geometry().with_centres(CentreLaws::new(
        geometry().centres().phase_tracking().clone(),
        PointingCentreLaw::Observation(observation_pointing().with_direction(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::TrackingTarget,
        )),
    ));

    assert!(matches!(
        compile(request(inconsistent)),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InconsistentPointingDirection
        ))
    ));
}

#[test]
fn spectral_axes_define_exact_channel_boundaries() {
    let linear = compile(request(geometry())).expect("compile linear axis");
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
    let first = compile(request(tabular)).expect("compile tabular axis");
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
}

#[test]
fn canonical_geometry_normalizes_signed_zero_and_outlier_order() {
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

    let first = compile(request(first)).expect("compile first");
    let second = compile(request(second)).expect("compile second");
    assert_eq!(first.geometry(), second.geometry());
    assert_eq!(first, second);
}

#[test]
fn multi_domain_centres_are_canonical_and_explicit() {
    let main = geometry().domains()[0].clone();
    let outlier_direction = main
        .direction()
        .with_reference_pixel([39.0, 39.0])
        .with_reference_direction(SkyDirection::new(DirectionFrame::J2000, 1.6, -0.4));
    let outlier_spec = main
        .clone()
        .with_role(ImageDomainRole::Outlier("outlier".into()))
        .with_shape(ImageShape::new(80, 80))
        .with_direction(outlier_direction)
        .with_psf_phase_centre(PsfPhaseCentreLaw::Fixed(SkyDirection::new(
            DirectionFrame::J2000,
            1.7,
            -0.4,
        )));
    let input = geometry().with_domains(vec![outlier_spec, main]);
    let compiled = compile(request(input)).expect("compile multi-domain geometry");

    assert_eq!(compiled.geometry().domains().len(), 2);
    assert_eq!(
        compiled.geometry().domains()[0].role(),
        &ImageDomainRole::Main
    );
    let outlier = &compiled.geometry().domains()[1];
    assert_eq!(outlier.role(), &ImageDomainRole::Outlier("outlier".into()));
    assert_eq!(outlier.shape().pixels(), [80, 80]);
    assert_eq!(
        outlier.model_phase_centre(),
        outlier.direction().reference_direction()
    );
    assert_eq!(outlier.psf_phase_centre().longitude_rad(), 1.7);
    assert!(
        compiled
            .required_capabilities()
            .contains(&casa_imaging_model::RequiredCapability::MultiDomainGeometry)
    );
}

#[test]
fn frame_transform_requires_a_valid_spectral_anchor() {
    let unanchored = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_output_frame(FrequencyFrame::Lsrk),
    );
    assert!(matches!(
        compile(request(unanchored)),
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
        compile(request(invalid_anchor)),
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
    compile(request(transform)).expect("compile transform");
}

#[test]
fn topo_barycentric_and_lsrk_frames_are_recorded_exactly() {
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
        let compiled = compile(request(transformed)).expect("compile frame transform");

        assert_eq!(
            compiled.geometry().spectral().source_frame(),
            FrequencyFrame::Topocentric
        );
        assert_eq!(compiled.geometry().spectral().output_frame(), output);
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
    let compiled = compile(request(faceted)).expect("compile facets");
    let domain = &compiled.geometry().domains()[0];
    let facets = domain.facets();
    assert_eq!(facets.len(), 8);
    let expected_windows = [
        ([0, 0], [256, 128]),
        ([256, 0], [512, 128]),
        ([0, 128], [256, 256]),
        ([256, 128], [512, 256]),
        ([0, 256], [256, 384]),
        ([256, 256], [512, 384]),
        ([0, 384], [256, 512]),
        ([256, 384], [512, 512]),
    ];
    assert_eq!(
        facets
            .iter()
            .map(|facet| (facet.origin(), facet.end_exclusive()))
            .collect::<Vec<_>>(),
        expected_windows
    );
    assert_eq!(
        facets
            .iter()
            .map(|facet| {
                let origin = facet.origin();
                let end = facet.end_exclusive();
                (end[0] - origin[0]) * (end[1] - origin[1])
            })
            .sum::<usize>(),
        domain.shape().pixels()[0] * domain.shape().pixels()[1]
    );
    assert!(
        facets
            .iter()
            .all(|facet| facet.local_centre_pixel() == [128, 64])
    );
    assert_eq!(facets[0].direction().reference_pixel(), [255.0, 255.0]);
    assert_eq!(facets[1].direction().reference_pixel(), [-1.0, 255.0]);
    assert_eq!(facets[7].direction().reference_pixel(), [-1.0, -129.0]);
    assert_eq!(
        facets[0].direction().reference_direction(),
        domain.direction().reference_direction(),
        "facet subwindows retain the common tangent"
    );
    assert_ne!(facets[0].phase_centre(), facets[1].phase_centre());
    assert_ne!(
        facets[0].phase_centre(),
        domain.direction().reference_direction()
    );

    let invalid = geometry().with_domains(vec![geometry().domains()[0].clone().with_facets(
        FacetLayout::Regular {
            columns: 3,
            rows: 1,
        },
    )]);
    assert!(matches!(
        compile(request(invalid)),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::NonDivisibleFacetLayout { .. }
        ))
    ));
}

#[test]
fn one_facet_preserves_the_domain_chart_and_phase_centre() {
    let compiled = compile(request(geometry())).expect("compile one facet");
    let domain = &compiled.geometry().domains()[0];
    let facet = domain.facets()[0];

    assert_eq!(facet.origin(), [0, 0]);
    assert_eq!(facet.end_exclusive(), [512, 512]);
    assert_eq!(facet.direction(), domain.direction());
    assert_eq!(facet.phase_centre(), domain.model_phase_centre());
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
    compile(request(line)).expect("compile line law");

    let inconsistent = geometry().with_spectral(
        geometry()
            .spectral()
            .clone()
            .with_rest_frequency(RestFrequency::NotApplicable)
            .with_doppler_convention(DopplerConvention::Radio),
    );
    assert!(matches!(
        compile(request(inconsistent)),
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
        compile(request(empty)),
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
        compile(request(duplicate_axes)),
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
        compile(request(singular)),
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
        compile(request(non_monotonic)),
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
        compile(request(missing_endpoint_width)),
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
        compile(request(collapsed_linear_axis)),
        Err(CompileProblemError::Geometry(
            CompileGeometryError::InvalidSpectralWcs
        ))
    ));
}
