// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-validated synthetic lineage for externally captured native minor planes.

use casa_imaging_model::*;
use num_complex::Complex64;

use super::*;
use crate::spectral_operator::{SpectralDomainPrimitives, SpectralOperatorPrimitives};

const REFERENCE_FREQUENCY_HZ: f64 = 1.0e9;
const IMAGE_WIDTH: usize = 512;

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
}

fn source() -> ObservationSourceInput {
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| ColumnGeneration::new(kind, identity(42, 20 + index as u8)))
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| MetadataGeneration::new(kind, identity(42, 60 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(42, 1)),
        ObservationSourceProvenance::new(
            "fixture://native-minor/synthetic-lineage".to_owned(),
            identity(42, 2),
        ),
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(
                2,
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(1, 1)],
            )
            .expect("two selected rows"),
            RowSelection::new(
                IdSelection::All,
                TimeSelection::All,
                UvSelection::All,
                AntennaSelection::All,
                IdSelection::All,
                IdSelection::All,
                IntentSelection::All,
                IdSelection::All,
            ),
            vec![
                DataDescriptionSelection::new(0, 0, 0),
                DataDescriptionSelection::new(1, 1, 0),
            ],
            vec![
                SpectralWindowSelection::new(0, vec![0]),
                SpectralWindowSelection::new(1, vec![0]),
            ],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
            )],
        ),
        SourceGenerations::new(
            ConsistencyToken::new(identity(42, 3)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            ModelColumnState::Absent,
        ),
    )
}

fn validity() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid primary-beam policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    )
}

fn problem_with_scales(scales_px: Vec<f64>) -> casa_imaging_model::CompiledProblem {
    let centre = IMAGE_WIDTH as f64 / 2.0;
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [centre, centre],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(IMAGE_WIDTH, IMAGE_WIDTH),
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
                reference_frequency_hz: REFERENCE_FREQUENCY_HZ,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source()],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile native minor fixture observation");
    compile(ImagingRequest::new(
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
                    scales_px,
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(30, f64::from(0.1_f32), 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(
                WeightingScheme::Briggs { robust: 0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::Model,
                    ProductKind::SumWeights,
                    ProductKind::Sensitivity,
                    ProductKind::TaylorTerms,
                ],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::Compensated,
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
            ModelBounds::new(1_048_576, 1_048_576, 1_048_576, 1_048_576, 1.0e30, 1.0e30)
                .expect("native minor fixture model bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile native minor fixture MT-MFS problem")
}

fn samples(problem: &casa_imaging_model::CompiledProblem) -> [SelectedObservationSample; 2] {
    let measurement_set = problem.selected_observation().read_set().sources()[0].measurement_set();
    [
        sample(measurement_set, 0, 0, 0.8e9, 2.0, [1.0, 0.25]),
        sample(measurement_set, 1, 1, 1.2e9, 1.0, [0.5, -0.75]),
    ]
}

fn sample(
    measurement_set: MeasurementSetIdentity,
    physical_row: u64,
    data_description_id: i32,
    frequency_hz: f64,
    input_weight: f32,
    visibility: [f32; 2],
) -> SelectedObservationSample {
    SelectedObservationSample {
        address: SelectedSampleAddress {
            measurement_set,
            physical_row,
            data_description_id,
            spectral_window_id: u32::try_from(data_description_id)
                .expect("non-negative fixture DDID"),
            channel_index: 0,
            frequency_centre_hz: frequency_hz,
            frequency_lower_hz: frequency_hz - 1.0e6,
            frequency_upper_hz: frequency_hz + 1.0e6,
            channel_width_hz: 2.0e6,
            frequency_frame: FrequencyFrame::Topocentric,
            polarization_id: 0,
            correlation_index: 0,
            correlation_type: CorrelationType::StokesI,
        },
        visibility: SelectedVisibilitySample::Complex32(visibility),
        prediction_target: SelectedPredictionTarget::NotRequested,
        channel_flag: false,
        parallel_hand_group_flag: false,
        row_flag: false,
        input_weight,
        coordinates: SelectedSampleCoordinates {
            raw_uvw_m: [1.0, 1.0, 0.0],
            density_uvw_m: [1.0, 1.0, 0.0],
            transformed_uvw_m: [1.0, 1.0, 0.0],
            phase_shift_m: 0.0,
            uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
            time: Epoch::new(59_000.0 + physical_row as f64, TimeScale::Utc),
            time_centroid: Epoch::new(59_000.0 + physical_row as f64, TimeScale::Utc),
            interval_seconds: 1.0,
            exposure_seconds: 1.0,
            parallactic_angles_rad: [0.0, 0.0],
            phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            pointing_directions: casa_imaging_model::SelectedPointingDirections {
                antenna1: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                antenna2: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            },
        },
        domain_projections: SelectedImageDomainProjections::one_domain_with_shared_psf(
            SelectedPhaseCentreProjection::new([1.0, 1.0, 0.0], 0.0)
                .expect("finite one-domain projection"),
        ),
        metadata: SelectedSampleMetadata {
            field_id: 0,
            antenna1: 0,
            antenna2: 1,
            antenna_responses: None,
            feed1: 0,
            feed2: 0,
            scan_number: 1,
            state_id: 0,
            observation_id: 0,
            array_id: 0,
        },
    }
}

/// Bind externally captured native planes to synthetic, compiler-validated
/// model lineage. No MeasurementSet is opened or scientific input regenerated.
pub(crate) fn build(
    residuals: Box<[Complex64]>,
    psfs: Box<[Complex64]>,
    response: Option<(Vec<f64>, f64, f64)>,
) -> (
    casa_imaging_model::CompiledProblem,
    ModelLifecycle,
    ModelGeneration,
    FinalNormalState,
) {
    let problem = problem_with_scales(vec![0.0, 5.0, 12.0]);
    let lifecycle = ModelLifecycle::bind(
        crate::ExecutableModelProblem::from_compiled(problem.clone()).expect("executable fixture"),
        ModelExecutionAttemptId::new(identity(51, 100)),
        1,
    )
    .expect("fixture lifecycle");
    let base = lifecycle.initial_empty().expect("zero fixture model");
    let (selected_generation, sample_count) = problem
        .inspect_selected_observation(
            samples(&problem)
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
            |_| Ok(()),
        )
        .expect("synthetic fixture lineage");
    let (weighting_generation, replay, coverage) =
        crate::weighting::native_normal_fixture_weighting_ids();
    let primitives = SpectralPrimitiveDomains::new(
        vec![SpectralDomainPrimitives::new(
            0,
            ImageDomainRole::Main,
            SpectralOperatorPrimitives::native_taylor_fixture(
                &problem,
                base.generation_id(),
                residuals,
                psfs,
                response,
            ),
        )]
        .into_boxed_slice(),
    )
    .expect("native fixture domain");
    let normal = FinalNormalState {
        completion_id: FinalNormalStateCompletionId(identity(51, 101)),
        problem: problem.problem_id(),
        geometry: problem.geometry().geometry_id(),
        numerics: problem.numerics_id(),
        weighting_commitment: problem.weighting().commitment_id(),
        weighting_generation,
        replay,
        coverage,
        catalog: NormalStateCatalog::UnnormalizedTaylorBlockV1,
        content: primitives.normal_state_content_identity(),
        sample_count,
        block_count: 1,
        input_model_generation: base.generation_id(),
        final_model_generation: base.generation_id(),
        selected_generation,
        continuum_transform_generation: None,
        coupled_mask_generation: None,
        image_domain_mask_generation: None,
        primitives,
    };
    (problem, lifecycle, base, normal)
}
