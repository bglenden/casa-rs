// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared small cube contract for numerical and runtime band tests.

use casa_imaging_model::*;

#[path = "../../../casa-imaging-model/tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

pub fn problem(sampling: SpectralSamplingLaw) -> CompiledProblem {
    problem_with_inputs(sampling, inputs())
}

pub fn problem_with_inputs(
    sampling: SpectralSamplingLaw,
    inputs: ProblemInputIdentities,
) -> CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [4.0; 2],
        [-0.002, 0.002],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(8, 8),
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
                channels: 4,
                reference_pixel: 0.0,
                reference_frequency_hz: 1e9,
                increment_hz: 1e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let validity = ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .unwrap(),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .unwrap(),
    );
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(sampling, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::ChannelLocal { channels: 4 },
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            validity,
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1e-7, 1e-3)))
                .collect(),
        ),
    );
    compile(ImagingRequest::new(
        specification,
        geometry,
        inputs,
        ModelLifecycleRequirements::new(
            ModelBounds::new(256, 256, 256, 256, 1e30, 1e30).unwrap(),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .unwrap()
}

fn inputs() -> ProblemInputIdentities {
    let template = common::observation_snapshot(1, Vec::new(), ModelStateIdentity::Empty);
    let source = &template.sources()[0];
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            5,
            (0..5_u32).map(|row| SelectedMainRow::new(u64::from(row), 0)),
        )
        .unwrap(),
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
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, (0..6).collect())],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    );
    ProblemInputIdentities::new(
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                source.identity(),
                source.provenance().clone(),
                selection,
                source.generations().clone(),
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
        .unwrap(),
    )
}

/// Full declared selected source, in canonical row/channel/correlation order.
pub fn selected_samples(problem: &CompiledProblem) -> Vec<SelectedObservationSample> {
    let source = problem.selected_observation().read_set().sources()[0].measurement_set();
    let direction = SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5);
    let mut samples = Vec::new();
    for row in 0..5 {
        for channel in 0..6 {
            for correlation in 0..2 {
                let frequency = 0.999e9 + channel as f64 * 1e6;
                let uvw = [2.0 + row as f64, -3.0, 0.0];
                samples.push(SelectedObservationSample {
                    address: SelectedSampleAddress {
                        measurement_set: source,
                        physical_row: row,
                        data_description_id: 0,
                        spectral_window_id: 0,
                        channel_index: channel,
                        frequency_centre_hz: frequency,
                        frequency_lower_hz: frequency - 0.5e6,
                        frequency_upper_hz: frequency + 0.5e6,
                        channel_width_hz: 1e6,
                        frequency_frame: FrequencyFrame::Topocentric,
                        polarization_id: 0,
                        correlation_index: correlation,
                        correlation_type: if correlation == 0 {
                            CorrelationType::CircularRr
                        } else {
                            CorrelationType::CircularLl
                        },
                    },
                    visibility: SelectedVisibilitySample::Complex32([
                        0.25 + channel as f32 * 0.12,
                        -0.8 + correlation as f32 * 0.2,
                    ]),
                    prediction_target: SelectedPredictionTarget::NotRequested,
                    channel_flag: channel == 3 && correlation == 1,
                    parallel_hand_group_flag: false,
                    row_flag: false,
                    input_weight: 0.7 + row as f32 * 0.1,
                    coordinates: SelectedSampleCoordinates {
                        raw_uvw_m: uvw,
                        density_uvw_m: uvw,
                        transformed_uvw_m: uvw,
                        phase_shift_m: 0.017,
                        uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
                        time: Epoch::new(59_000.0 + row as f64, TimeScale::Utc),
                        time_centroid: Epoch::new(59_000.0 + row as f64, TimeScale::Utc),
                        interval_seconds: 1.0,
                        exposure_seconds: 1.0,
                        parallactic_angles_rad: [0.0; 2],
                        phase_direction: direction,
                        delay_direction: direction,
                        pointing_directions: SelectedPointingDirections {
                            antenna1: direction,
                            antenna2: direction,
                        },
                    },
                    domain_projections: SelectedImageDomainProjections::one_domain_with_shared_psf(
                        SelectedPhaseCentreProjection::new(uvw, 0.017).unwrap(),
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
                });
            }
        }
    }
    samples
}
