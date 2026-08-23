// SPDX-License-Identifier: LGPL-3.0-or-later

use std::convert::Infallible;

use casa_imaging_model::{
    AxisOrder, CentreLaws, ColumnGeneration, CorrelationProduct, CorrelationSelection,
    CorrelationType, DataDescriptionSelection, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, MeasurementEquationContract, ModelColumnWrite,
    ModelInnerProduct, ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage,
    NumericsContract, ObservationPointingLaw, ObservationSelection, ObservationSnapshotInput,
    ObservationSourceInput, ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy,
    ProductKind, ProductNormalization, ProductRequirements, ProductSupportComparison,
    ProductValidityPolicies, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RestFrequency, RestoringBeamPolicy, ScientificContract, SelectedColumns, SelectedMainRow,
    SelectedObservationGenerationId, SelectedObservationInspectionError,
    SelectedObservationPassError, SelectedObservationSample, SelectedPredictionTarget,
    SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates, SelectedSampleMetadata,
    SelectedVisibilitySample, SkyDirection, SourceGenerations, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSampling, SpectralWcs,
    SpectralWindowSelection, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    TimeScale, UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn,
    WeightDensityScope, WeightingContract, WeightingScheme, compile, compile_observation,
};

mod common;

use common::{identity, observation_snapshot};

#[test]
fn selected_observation_inspection_rejects_any_departure_from_compiled_coverage() {
    let problem = compiled_problem();
    let samples = exact_samples(&problem);

    let contiguous = inspect(&problem, samples.iter().copied()).expect("inspect exact stream");
    let differently_chunked = inspect(
        &problem,
        samples.chunks(2).flat_map(|block| block.iter().copied()),
    )
    .expect("inspect the same logical stream through different borrowed chunks");
    assert_eq!(contiguous.1, samples.len() as u64);
    assert_eq!(contiguous.0, differently_chunked.0);

    let first_source = problem.selected_observation().read_set().sources()[0].measurement_set();

    let mut wrong_rows = samples.clone();
    for sample in &mut wrong_rows {
        if sample.address.measurement_set == first_source && sample.address.physical_row == 2 {
            sample.address.physical_row = 1;
        }
    }
    assert!(matches!(
        inspect(&problem, wrong_rows.iter().copied()),
        Err(SelectedObservationInspectionError::SelectedRowSequenceMismatch { .. })
    ));

    let mut substituted_data_description = samples.clone();
    let alternate = DataDescriptionSelection::new(0, 0, 0);
    let correlation = CorrelationProduct::new(0, CorrelationType::StokesI);
    let row_two_start = substituted_data_description
        .iter()
        .position(|sample| {
            sample.address.measurement_set == first_source && sample.address.physical_row == 2
        })
        .expect("second selected MAIN row");
    substituted_data_description.splice(
        row_two_start..row_two_start + 2,
        [
            sample(first_source, 2, alternate, 0, correlation),
            sample(first_source, 2, alternate, 1, correlation),
        ],
    );
    assert!(matches!(
        inspect(&problem, substituted_data_description.iter().copied()),
        Err(SelectedObservationInspectionError::SelectedRowSequenceMismatch { .. })
    ));

    let mut wrong_data_description = samples.clone();
    wrong_data_description[0].address.polarization_id = 1;
    assert!(matches!(
        inspect(&problem, wrong_data_description.iter().copied()),
        Err(SelectedObservationInspectionError::DataDescriptionCoordinateMismatch { .. })
    ));

    let mut missing_channel = samples.clone();
    let missing_index = missing_channel
        .iter()
        .position(|sample| {
            sample.address.measurement_set == first_source
                && sample.address.physical_row == 0
                && sample.address.channel_index == 1
        })
        .expect("selected channel fixture");
    missing_channel.remove(missing_index);
    assert!(matches!(
        inspect(&problem, missing_channel.iter().copied()),
        Err(SelectedObservationInspectionError::MissingSample { .. })
    ));

    let mut replaced_channel = samples.clone();
    replaced_channel[missing_index].address.channel_index = 7;
    assert!(matches!(
        inspect(&problem, replaced_channel.iter().copied()),
        Err(SelectedObservationInspectionError::UnexpectedSample { .. })
    ));

    let mut duplicate = samples.clone();
    duplicate.insert(1, duplicate[0]);
    assert!(matches!(
        inspect(&problem, duplicate.iter().copied()),
        Err(SelectedObservationInspectionError::DuplicateSample { .. })
    ));

    let mut reversed = samples.clone();
    reversed.reverse();
    assert!(matches!(
        inspect(&problem, reversed.iter().copied()),
        Err(SelectedObservationInspectionError::NonCanonicalSampleOrder { .. })
    ));

    let mut wrong_prediction_target = samples.clone();
    wrong_prediction_target[0].prediction_target = SelectedPredictionTarget::NotRequested;
    assert!(matches!(
        inspect(&problem, wrong_prediction_target.iter().copied()),
        Err(SelectedObservationInspectionError::PredictionTargetMismatch { .. })
    ));

    let mut wrong_visibility_storage = samples.clone();
    wrong_visibility_storage[0].visibility = SelectedVisibilitySample::Float32(1.0);
    assert!(matches!(
        inspect(&problem, wrong_visibility_storage.iter().copied()),
        Err(SelectedObservationInspectionError::VisibilityStorageMismatch { .. })
    ));

    let weight_problem = compiled_problem_with_weight_broadcast_shape();
    let weight_samples = exact_samples(&weight_problem);
    let weight_source =
        weight_problem.selected_observation().read_set().sources()[0].measurement_set();
    let mut per_correlation_broadcast_weight = weight_samples;
    for sample in &mut per_correlation_broadcast_weight {
        if sample.address.measurement_set == weight_source && sample.address.physical_row == 0 {
            sample.input_weight = if sample.address.correlation_index == 0 {
                2.0
            } else {
                5.0
            };
        }
    }
    inspect(
        &weight_problem,
        per_correlation_broadcast_weight.iter().copied(),
    )
    .expect("WEIGHT may differ between correlations while repeating across channels");

    let mut varying_broadcast_weight = per_correlation_broadcast_weight;
    let second_channel_second_correlation = varying_broadcast_weight
        .iter_mut()
        .find(|sample| {
            sample.address.measurement_set == weight_source
                && sample.address.physical_row == 0
                && sample.address.channel_index == 1
                && sample.address.correlation_index == 1
        })
        .expect("second selected channel and correlation");
    second_channel_second_correlation.input_weight = 6.0;
    assert!(matches!(
        inspect(&weight_problem, varying_broadcast_weight.iter().copied()),
        Err(SelectedObservationInspectionError::WeightBroadcastMismatch { .. })
    ));

    let spectrum_problem =
        compiled_problem_with_columns(VisibilityColumn::Data, WeightColumn::WeightSpectrum);
    let mut channel_varying_spectrum = exact_samples(&spectrum_problem);
    channel_varying_spectrum[1].input_weight = 3.0;
    inspect(&spectrum_problem, channel_varying_spectrum)
        .expect("WEIGHT_SPECTRUM may vary across selected channels");

    let float_problem =
        compiled_problem_with_columns(VisibilityColumn::FloatData, WeightColumn::Weight);
    let float_samples = exact_samples(&float_problem);
    inspect(&float_problem, float_samples.iter().copied())
        .expect("FLOAT_DATA reports single-precision real samples");
    let mut wrong_float_storage = float_samples;
    wrong_float_storage[0].visibility = SelectedVisibilitySample::Complex32([1.0, 0.0]);
    assert!(matches!(
        inspect(&float_problem, wrong_float_storage),
        Err(SelectedObservationInspectionError::VisibilityStorageMismatch { .. })
    ));

    // Inspection is deterministic validation plus content identity only. It is
    // not access evidence, attempt freshness, traversal completion, or weighting authority.
}

#[test]
fn closed_inspection_pass_validates_before_exposing_a_sample() {
    let problem = compiled_problem();
    let mut samples = exact_samples(&problem);
    samples[0].address.correlation_index = u32::MAX;
    let mut consumed = Vec::new();

    let result = problem.inspect_selected_observation(
        samples.into_iter().map(Ok::<_, Infallible>),
        |sample| {
            consumed.push(sample);
            Ok::<_, Infallible>(())
        },
    );

    assert!(matches!(
        result,
        Err(SelectedObservationPassError::Inspection(
            SelectedObservationInspectionError::UnexpectedSample { .. }
        ))
    ));
    assert!(
        consumed.is_empty(),
        "an invalid sample must not cross the closed validation boundary"
    );
}

fn inspect(
    problem: &casa_imaging_model::CompiledProblem,
    samples: impl IntoIterator<Item = SelectedObservationSample>,
) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationInspectionError> {
    match problem.inspect_selected_observation(samples.into_iter().map(Ok::<_, Infallible>), |_| {
        Ok::<_, Infallible>(())
    }) {
        Ok(inspection) => Ok(inspection),
        Err(SelectedObservationPassError::Inspection(error)) => Err(error),
        Err(SelectedObservationPassError::External(error)) => match error {},
    }
}

fn exact_samples(problem: &casa_imaging_model::CompiledProblem) -> Vec<SelectedObservationSample> {
    let mut samples = Vec::new();
    for source in problem.selected_observation().read_set().sources() {
        let rows: &[u64] = if source.selection().rows().selected_row_count() == 2 {
            &[0, 2]
        } else {
            &[1]
        };
        for &row in rows {
            let data_description_id = u32::from(row == 2);
            let data_description = source
                .selection()
                .data_descriptions()
                .iter()
                .copied()
                .find(|selection| selection.data_description_id() == data_description_id)
                .expect("DATA_DESCRIPTION fixture");
            let channels = source
                .selection()
                .spectral_windows()
                .iter()
                .find(|selection| {
                    selection.spectral_window_id() == data_description.spectral_window_id()
                })
                .expect("spectral-window fixture")
                .channel_indices();
            let correlations = source
                .selection()
                .correlations()
                .iter()
                .find(|selection| selection.polarization_id() == data_description.polarization_id())
                .expect("polarization fixture")
                .products();
            for &channel in channels {
                for &correlation in correlations {
                    let mut selected = sample(
                        source.measurement_set(),
                        row,
                        data_description,
                        channel,
                        correlation,
                    );
                    if source.selected_columns().visibility() == VisibilityColumn::FloatData {
                        selected.visibility = SelectedVisibilitySample::Float32(1.0 + row as f32);
                    }
                    samples.push(selected);
                }
            }
        }
    }
    samples
}

fn sample(
    measurement_set: casa_imaging_model::MeasurementSetIdentity,
    physical_row: u64,
    data_description: DataDescriptionSelection,
    channel_index: u32,
    correlation: CorrelationProduct,
) -> SelectedObservationSample {
    let centre_hz = 1_400_000_000.0
        + f64::from(data_description.spectral_window_id()) * 10_000_000.0
        + f64::from(channel_index) * 1_000_000.0;
    SelectedObservationSample {
        address: SelectedSampleAddress {
            measurement_set,
            physical_row,
            data_description_id: data_description.data_description_id() as i32,
            spectral_window_id: data_description.spectral_window_id(),
            channel_index,
            frequency_centre_hz: centre_hz,
            frequency_lower_hz: centre_hz - 500_000.0,
            frequency_upper_hz: centre_hz + 500_000.0,
            channel_width_hz: 1_000_000.0,
            frequency_frame: FrequencyFrame::Topocentric,
            polarization_id: data_description.polarization_id(),
            correlation_index: correlation.correlation_index(),
            correlation_type: correlation.correlation_type(),
        },
        visibility: SelectedVisibilitySample::Complex32([
            1.0 + physical_row as f32,
            -(channel_index as f32),
        ]),
        prediction_target: SelectedPredictionTarget::ModelData,
        channel_flag: false,
        row_flag: false,
        input_weight: 2.0,
        coordinates: SelectedSampleCoordinates {
            raw_uvw_m: [12.0 + physical_row as f64, -4.0, 2.0],
            density_uvw_m: [12.5 + physical_row as f64, -4.25, 2.25],
            transformed_uvw_m: [11.75 + physical_row as f64, -3.75, 1.5],
            phase_shift_m: 0.125,
            uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
            time: Epoch::new(59_000.0 + physical_row as f64 * 1.0e-6, TimeScale::Utc),
            time_centroid: Epoch::new(
                59_000.000_001 + physical_row as f64 * 1.0e-6,
                TimeScale::Utc,
            ),
            interval_seconds: 1.0,
            exposure_seconds: 0.8,
            phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.000_5, -0.500_5),
            pointing_directions: casa_imaging_model::SelectedPointingDirections {
                antenna1: SkyDirection::new(DirectionFrame::J2000, 1.001, -0.499),
                antenna2: SkyDirection::new(DirectionFrame::J2000, 1.002, -0.498),
            },
        },
        metadata: SelectedSampleMetadata {
            field_id: 0,
            antenna1: 0,
            antenna2: 1,
            feed1: 0,
            feed2: 0,
            scan_number: 7,
            state_id: 0,
            observation_id: 0,
            array_id: 0,
        },
    }
}

fn compiled_problem() -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_columns(VisibilityColumn::Data, WeightColumn::Weight)
}

fn compiled_problem_with_columns(
    visibility: VisibilityColumn,
    weights: WeightColumn,
) -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_columns_and_weight_shape(visibility, weights, false)
}

fn compiled_problem_with_weight_broadcast_shape() -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_columns_and_weight_shape(
        VisibilityColumn::Data,
        WeightColumn::Weight,
        true,
    )
}

fn compiled_problem_with_columns_and_weight_shape(
    visibility: VisibilityColumn,
    weights: WeightColumn,
    two_correlations: bool,
) -> casa_imaging_model::CompiledProblem {
    let references = vec![
        (ReferenceDataKind::Measures, identity(3)),
        (ReferenceDataKind::Ephemeris, identity(4)),
    ];
    let model_source = identity(5);
    let model = ModelStateIdentity::Seed(model_source);
    let sources = vec![
        source(
            1,
            &[0, 2],
            &references,
            model,
            visibility,
            weights,
            two_correlations,
        ),
        source(
            2,
            &[1],
            &references,
            model,
            visibility,
            weights,
            two_correlations,
        ),
    ];
    let snapshot = compile_observation(ObservationSnapshotInput::new(sources, references, model))
        .expect("compile multi-source inspection observation");
    compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot),
        casa_imaging_model::ModelLifecycleRequirements::new(
            casa_imaging_model::ModelBounds::new(
                10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
            )
            .expect("valid model lifecycle fixture bounds"),
            NumericPrecision::F32,
            casa_imaging_model::ModelInputCommitment::AlignedSeed {
                source: model_source,
                support: identity(0xa5),
            },
        ),
    ))
    .expect("compile inspection problem")
}

fn source(
    observation: u8,
    rows: &[u64],
    references: &[(ReferenceDataKind, casa_imaging_model::LogicalIdentity)],
    model: ModelStateIdentity,
    visibility: VisibilityColumn,
    weights: WeightColumn,
    two_correlations: bool,
) -> ObservationSourceInput {
    let baseline = observation_snapshot(observation, references.to_vec(), model);
    let source = &baseline.sources()[0];
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            3,
            rows.iter()
                .copied()
                .map(|row| SelectedMainRow::new(row, u32::from(row == 2))),
        )
        .expect("selected physical-row fixture"),
        source.selection().rows_filter().clone(),
        vec![
            DataDescriptionSelection::new(0, 0, 0),
            DataDescriptionSelection::new(1, 1, 1),
        ],
        vec![
            SpectralWindowSelection::new(0, vec![0, 1]),
            SpectralWindowSelection::new(1, vec![2]),
        ],
        vec![
            CorrelationSelection::new(
                0,
                if two_correlations {
                    vec![
                        CorrelationProduct::new(0, CorrelationType::StokesI),
                        CorrelationProduct::new(1, CorrelationType::StokesQ),
                    ]
                } else {
                    vec![CorrelationProduct::new(0, CorrelationType::StokesI)]
                },
            ),
            CorrelationSelection::new(
                1,
                vec![
                    CorrelationProduct::new(0, CorrelationType::StokesQ),
                    CorrelationProduct::new(1, CorrelationType::StokesU),
                ],
            ),
        ],
    );
    let mut column_generations = source.generations().columns().generations().to_vec();
    let required_column = match visibility {
        VisibilityColumn::Data => MsColumnKind::Data,
        VisibilityColumn::CorrectedData => MsColumnKind::CorrectedData,
        VisibilityColumn::FloatData => MsColumnKind::FloatData,
    };
    if column_generations
        .iter()
        .all(|generation| generation.kind() != required_column)
    {
        column_generations.push(ColumnGeneration::new(required_column, identity(111)));
    }
    let required_weight = match weights {
        WeightColumn::Weight => MsColumnKind::Weight,
        WeightColumn::WeightSpectrum => MsColumnKind::WeightSpectrum,
    };
    if column_generations
        .iter()
        .all(|generation| generation.kind() != required_weight)
    {
        column_generations.push(ColumnGeneration::new(required_weight, identity(112)));
    }
    let generations = SourceGenerations::new(
        source.generations().consistency_token(),
        SelectedColumns::new(
            visibility,
            FlagPolicy::FlagOrFlagRow,
            weights,
            column_generations,
        ),
        source.generations().metadata_generations().to_vec(),
        source.generations().model_column(),
    );
    ObservationSourceInput::new(
        source.identity(),
        source.provenance().clone(),
        selection,
        generations,
    )
}

fn specification() -> ProblemSpecification {
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
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
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(10, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    0.2,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )
                .expect("valid PB policy"),
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )
                .expect("valid Taylor policy"),
            ),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
        NumericsContract::new(
            vec![NumericPrecision::F32],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    )
}

fn geometry() -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [15.0, 15.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(32, 32),
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
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTimeCentroid,
                PointingInterpolation::GreatCircleShortestArc,
                PointingExtrapolation::Reject,
                casa_imaging_model::MissingPointingPolicy::Reject,
            )),
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 3,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            casa_imaging_model::DopplerConvention::NotApplicable,
        ),
    )
}
