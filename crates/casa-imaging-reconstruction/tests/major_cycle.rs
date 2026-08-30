// SPDX-License-Identifier: LGPL-3.0-or-later

//! T20 Major-Cycle reconciliation join over T19 complete-data evidence and
//! the T28 model lifecycle, driven entirely through owner seams.

use std::convert::Infallible;

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    ContinuumTransformGenerationId, CorrelationProduct, CorrelationSelection, CorrelationType,
    DataDescriptionSelection, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy,
    FrequencyFrame, GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelCell, ModelColumnState, ModelColumnWrite, ModelDeltaTerm,
    ModelExecutionAttemptId, ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements,
    ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage, NumericsContract,
    ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedObservationGenerationId,
    SelectedObservationSample, SelectedPredictionTarget, SelectedRows, SelectedSampleAddress,
    SelectedSampleCoordinates, SelectedSampleMetadata, SelectedSpectralContribution,
    SelectedSpectralContributions, SelectedVisibilitySample, SkyDirection, SourceGenerations,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection,
    UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ChannelCyclePolicy, ExecutableModelProblem, FinalModelCompletionId, MajorCycleError,
    MajorCycleOwner, MajorCyclePreparation, MinorCycleProgram, ModelDelta, ModelGeneration,
    ModelLifecycle, ModelLifecycleError, ReconstructionCycle, ReconstructionMask,
    SpectralChannelValidity, SpectralOperatorError, SpectralOperatorSpecification,
    WeightingAlgorithmState, WeightingError, WeightingExecutionLimits, WeightingPlan,
    WeightingReplayChunk, WeightingReplaySummary, begin_weighting_generation, plan_weighting,
    runtime_adapter::{
        CompleteDataOwnerResult, GriddedNormalOperatorCompiler, SourceCardinalityObservation,
        SpectralOperatorPass, prepare_spectral_operator, spectral_operator_workload,
    },
};

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
}

fn attempt(byte: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(identity(byte, 0))
}

fn source(seed: u8) -> ObservationSourceInput {
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
    .map(|(index, kind)| ColumnGeneration::new(kind, identity(seed, 20 + index as u8)))
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
    .map(|(index, kind)| MetadataGeneration::new(kind, identity(seed, 60 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(seed, 1)),
        ObservationSourceProvenance::new(
            format!("fixture://major-cycle/{seed}"),
            identity(seed, 2),
        ),
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(
                3,
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(2, 1)],
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
                SpectralWindowSelection::new(1, vec![1]),
            ],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
            )],
        ),
        SourceGenerations::new(
            ConsistencyToken::new(identity(seed, 3)),
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

fn t19_compatible_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    t19_compatible_problem_with_width(observation, 8)
}

fn t19_compatible_problem_with_width(
    observation: u8,
    width: usize,
) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem(
        observation,
        width,
        1,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
    )
}

fn t38_cube_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    t38_cube_problem_with_channels(observation, 2)
}

fn t38_cube_problem_with_channels(
    observation: u8,
    channels: usize,
) -> casa_imaging_model::CompiledProblem {
    t38_cube_problem_with_controls(
        observation,
        channels,
        ReconstructionControls::new(8, 0.5, 0.0).with_noise_sigma(0.0),
    )
}

fn t38_cube_problem_with_controls(
    observation: u8,
    channels: usize,
    controls: ReconstructionControls,
) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem(
        observation,
        8,
        channels,
        ReconstructionBasis::ChannelLocal { channels },
        ReconstructionAlgorithm::Hogbom,
        controls,
    )
}

fn reconstruction_problem(
    observation: u8,
    width: usize,
    channels: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem_with_sampling(
        observation,
        width,
        channels,
        basis,
        algorithm,
        controls,
        SpectralSamplingLaw::IDENTITY,
    )
}

fn reconstruction_problem_with_sampling(
    observation: u8,
    width: usize,
    channels: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
    sampling: SpectralSamplingLaw,
) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem_with_sampling_and_model(
        observation,
        width,
        channels,
        basis,
        algorithm,
        controls,
        (
            sampling,
            ModelStateIdentity::Empty,
            ModelInputCommitment::Empty,
        ),
    )
}

fn reconstruction_problem_with_sampling_and_model(
    observation: u8,
    width: usize,
    channels: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
    model: (
        SpectralSamplingLaw,
        ModelStateIdentity,
        ModelInputCommitment,
    ),
) -> casa_imaging_model::CompiledProblem {
    let (sampling, model_state, model_input) = model;
    let centre = width as f64 / 2.0;
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
            ImageShape::new(width, width),
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
                channels,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.05e9,
                increment_hz: 1.0e8,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(observation)],
        Vec::new(),
        model_state,
    ))
    .expect("compile observation snapshot");
    compile(ImagingRequest::new(
        ProblemSpecification::new(
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
                basis,
                algorithm,
                controls,
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
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
            ModelBounds::new(4_096, 4_096, 4_096, 4_096, 1.0e30, 1.0e30).expect("valid bounds"),
            NumericPrecision::F64,
            model_input,
        ),
    ))
    .expect("compile T20 reconciliation problem")
}

#[test]
fn t38_two_channel_hogbom_cycle_is_ordered_and_model_plane_complete() {
    let problem = t38_cube_problem(238);
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable cube problem"),
        attempt(239),
        1,
    )
    .expect("cube lifecycle");
    let initial = lifecycle.initial_empty().expect("empty cube model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare cube model");
    let complete = run_t19_complete_data(&problem, Some(&preparation));
    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("channel-local major-cycle owner")
        .reconcile(&mut lifecycle)
        .expect("channel-local normal state");
    let (normal, continuation) = joined.into_continuation();
    let coordinate = problem.geometry().domains()[0].direction();
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        coordinate,
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program")
    .record_component_sequence(16)
    .expect("bounded component evidence");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("two-channel reconstruction cycle");

    let channels = result.evidence().channels();
    assert_eq!(channels.len(), 2);
    assert_eq!(
        channels
            .iter()
            .map(|channel| channel.output_channel())
            .collect::<Vec<_>>(),
        vec![0, 1],
        "cycle evidence is always in output-channel order"
    );
    assert!(channels.iter().all(|channel| {
        channel
            .minor_cycle()
            .is_some_and(|evidence| evidence.iterations() > 0)
    }));
    let delta = result
        .delta()
        .expect("both channel planes update the model");
    assert_eq!(
        delta
            .terms()
            .iter()
            .map(|term| term.cell().coefficient())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([0, 1]),
        "one owner-minted delta contains every cleaned channel plane"
    );
}

#[cfg(feature = "cpp-interop-tests")]
// Reconstruct the finite-support residual implied by Rust's recorded
// components without treating casacore's working residual as a Major-Cycle
// oracle.
fn t38_finite_hogbom_residual(
    initial: &[f32],
    psf: &[f32],
    shape: [usize; 2],
    psf_peak: [usize; 2],
    components: &[([usize; 2], f32)],
) -> Vec<f32> {
    let mut residual = initial.to_vec();
    for (component, flux) in components {
        for x in 0..shape[0] {
            let Some(source_x) = (x + psf_peak[0]).checked_sub(component[0]) else {
                continue;
            };
            if source_x >= shape[0] {
                continue;
            }
            for y in 0..shape[1] {
                let Some(source_y) = (y + psf_peak[1]).checked_sub(component[1]) else {
                    continue;
                };
                if source_y < shape[1] {
                    residual[x * shape[1] + y] -= flux * psf[source_x * shape[1] + source_y];
                }
            }
        }
    }
    residual
}

#[cfg(feature = "cpp-interop-tests")]
#[test]
fn t38_casacore_minor_cycle_and_paired_final_residual_are_split_oracles() {
    use casa_imaging_reconstruction::MaskBox;
    use casa_test_support::hogbom_interop::HogbomOracle;

    let controls = ReconstructionControls::new(4, 0.5, 0.0).with_noise_sigma(0.0);
    let problem = t38_cube_problem_with_controls(246, 2, controls);
    let mut initial_lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable cube problem"),
        attempt(247),
        1,
    )
    .expect("initial cube lifecycle");
    let initial = initial_lifecycle.initial_empty().expect("empty cube model");
    let preparation = MajorCyclePreparation::prepare(&initial_lifecycle, initial, None)
        .expect("prepare empty cube model");
    let complete = run_t19_complete_data(&problem, Some(&preparation));
    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("initial complete-data owner")
        .reconcile(&mut initial_lifecycle)
        .expect("initial paired normal state");
    let (normal, continuation) = joined.into_continuation();
    let (mut lifecycle, carried) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("continued cube problem"),
        attempt(248),
        2,
        continuation,
    )
    .expect("continue the initial final model");

    // The full synthetic plane has exact symmetry ties whose winner depends
    // only on row-major versus column-major scan order. Keep the mask finite
    // and interior while selecting an unambiguous two-component trajectory.
    let mask = ReconstructionMask::from_boxes(
        problem.problem_id(),
        carried.generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
        [MaskBox::new([2, 4], [4, 4]).expect("interior CLEAN strip")],
    )
    .expect("identical interior CASA/Rust mask");
    let mut mask_pixels = Vec::with_capacity(normal.shape()[0] * normal.shape()[1]);
    for x in 0..normal.shape()[0] {
        for y in 0..normal.shape()[1] {
            mask_pixels.push(mask.contains([x, y]));
        }
    }
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("identical Högbom controls")
    .record_component_sequence(4)
    .expect("bounded component sequence");
    let rust = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, &carried, &normal, &mask)
        .expect("Rust channel-local minor cycle");
    assert_eq!(rust.evidence().iterations(), 4);
    assert!(rust.evidence().requests_reconciliation());

    for (channel_index, channel) in rust.evidence().channels().iter().enumerate() {
        let plane = normal.plane(channel_index).expect("channel normal plane");
        let psf = plane
            .normal_approximation()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        let residual = plane
            .residual()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        let casa_steps = [1, 2].map(|iterations| {
            HogbomOracle::clean_minor_cycle_2d_masked(
                &psf,
                &residual,
                plane.shape(),
                &mask_pixels,
                0.5,
                0.0,
                iterations,
            )
            .expect("CASA/casacore masked Högbom oracle")
        });
        let evidence = channel.minor_cycle().expect("valid channel evidence");
        assert_eq!(evidence.iterations(), 2);
        let components = evidence
            .recorded_component_sequence()
            .expect("recorded T38 component trajectory");
        assert_eq!(components.len(), 2);

        let mut cumulative_model = vec![0.0_f32; psf.len()];
        for (step, (component, casa)) in components.iter().zip(&casa_steps).enumerate() {
            let pixel = component.cell().pixel();
            cumulative_model[pixel[0] * plane.shape()[1] + pixel[1]] += component.flux() as f32;
            assert_eq!(casa.iterdone, step + 1);
            for (rust_value, casa_value) in cumulative_model.iter().zip(&casa.model) {
                assert!((rust_value - casa_value).abs() < 1.0e-5);
            }
        }

        let casa = &casa_steps[1];
        let psf_peak = psf
            .iter()
            .map(|value| f64::from(value.abs()))
            .fold(0.0_f64, f64::max);
        let casa_terminal_peak = f64::from(casa.peak_residual_jy_per_beam) / psf_peak;
        assert!((evidence.final_peak_flux() - casa_terminal_peak).abs() < 1.0e-5);
        let mut psf_peak_index = 0;
        for index in 1..psf.len() {
            if psf[index].abs() > psf[psf_peak_index].abs() {
                psf_peak_index = index;
            }
        }
        let component_values = components
            .iter()
            .map(|component| (component.cell().pixel(), component.flux() as f32))
            .collect::<Vec<_>>();
        let rust_working_residual = t38_finite_hogbom_residual(
            &residual,
            &psf,
            plane.shape(),
            [
                psf_peak_index / plane.shape()[1],
                psf_peak_index % plane.shape()[1],
            ],
            &component_values,
        );
        for (index, ((rust_value, casa_value), selected)) in rust_working_residual
            .iter()
            .zip(&casa.residual)
            .zip(&mask_pixels)
            .enumerate()
        {
            if *selected {
                assert!(
                    (rust_value - casa_value).abs() < 1.0e-4,
                    "channel {channel_index} finite minor residual diverged at {:?}: rust={rust_value} casa={casa_value}",
                    [index / plane.shape()[1], index % plane.shape()[1]],
                );
            }
        }
    }

    let (delta, _) = rust.into_parts();
    let (complete, preparation) =
        prepare_reconciliation_reusing(&problem, &lifecycle, carried, delta, normal);
    let final_join = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("final complete-data owner")
        .reconcile(&mut lifecycle)
        .expect("final paired A/A* reconciliation");
    assert!(final_join.model_completion().delta().is_some());
    let (paired_normal, continuation) = final_join.into_continuation();
    let paired_final_residual = paired_normal.residual().to_vec();
    let paired_psf = paired_normal.normal_approximation().to_vec();
    let paired_sensitivity = paired_normal.sensitivity().to_vec();
    let paired_sum_weights = paired_normal.sum_weights().to_vec();
    let paired_validity = paired_normal.channel_validity().to_vec();

    let (mut replay_lifecycle, replay_carried) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("replay cube problem"),
        attempt(249),
        3,
        continuation,
    )
    .expect("continue the exact final model for a fresh paired replay");
    let (replayed_complete, replayed_preparation) =
        prepare_reconciliation(&problem, &replay_lifecycle, replay_carried, None);
    let replayed_join =
        MajorCycleOwner::from_complete_data(replayed_complete, replayed_preparation)
            .expect("fresh paired complete-data owner")
            .reconcile(&mut replay_lifecycle)
            .expect("fresh paired A/A* replay");
    assert_eq!(
        replayed_join.normal_state().residual(),
        paired_final_residual,
        "invariant reuse is bit-exact under a full paired A/A* replay of the same model"
    );
    assert_eq!(
        replayed_join.normal_state().normal_approximation(),
        paired_psf
    );
    assert_eq!(
        replayed_join.normal_state().sensitivity(),
        paired_sensitivity
    );
    assert_eq!(
        replayed_join.normal_state().sum_weights(),
        paired_sum_weights
    );
    assert_eq!(
        replayed_join.normal_state().channel_validity(),
        paired_validity
    );
    assert_eq!(replayed_join.model_completion().delta(), None);
}

#[test]
fn residual_refresh_rejects_prior_invariants_from_another_selected_generation() {
    let problem = t19_compatible_problem(250);
    let mut initial_lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(251),
        1,
    )
    .expect("initial lifecycle");
    let initial_model = initial_lifecycle.initial_empty().expect("empty model");
    let initial_preparation =
        MajorCyclePreparation::prepare(&initial_lifecycle, initial_model, None)
            .expect("initial preparation");
    let initial_complete = run_t19_complete_data(&problem, Some(&initial_preparation));
    let initial_join = MajorCycleOwner::from_complete_data(initial_complete, initial_preparation)
        .expect("initial major owner")
        .reconcile(&mut initial_lifecycle)
        .expect("initial normal state");
    let (normal_state, continuation) = initial_join.into_continuation();
    let (continued_lifecycle, carried_model) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("continued problem"),
        attempt(252),
        2,
        continuation,
    )
    .expect("continued lifecycle");
    let preparation = MajorCyclePreparation::prepare(&continued_lifecycle, carried_model, None)
        .expect("residual refresh preparation");
    let mut changed_samples = fixture_samples(&problem);
    changed_samples[0].visibility = SelectedVisibilitySample::Complex32([91.0, -17.0]);

    let error = run_t19_complete_data_for_pass_result(
        &problem,
        Some(&preparation),
        &changed_samples,
        None,
        SpectralOperatorPass::ResidualRefresh,
        Some(normal_state),
    )
    .expect_err("changed selected content must invalidate reused normal state");
    assert_eq!(error, SpectralOperatorError::ReusableNormalStateMismatch);
}

#[test]
fn sealed_gridded_program_is_reused_across_distinct_model_generations() {
    let problem = t19_compatible_problem(253);
    let mut initial_lifecycle = bind_lifecycle(&problem, attempt(254));
    let initial_model = initial_lifecycle.initial_empty().expect("empty model");
    let initial_preparation =
        MajorCyclePreparation::prepare(&initial_lifecycle, initial_model, None)
            .expect("initial preparation");

    let (program, gridded_blocks, initial_complete, max_replay_block_samples) = {
        let samples = fixture_samples(&problem);
        let plan = plan_weighting(
            &problem,
            WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
        )
        .expect("weighting plan");
        let selected_generation = replay_selected_generation(&problem, &samples);
        let generation = freeze_weighting_generation_with(
            &problem,
            &plan,
            &samples,
            constant_basis_contributions,
        )
        .expect("freeze constant-basis weighting generation");
        let (weighted_blocks, summary) = replay_with(
            &generation,
            &problem,
            &plan,
            &samples,
            constant_basis_contributions,
        );

        let mut compiler =
            GriddedNormalOperatorCompiler::new(&problem, SourceCardinalityObservation::Disabled)
                .expect("gridded compiler");
        let gridded_blocks = weighted_blocks
            .iter()
            .map(|block| {
                compiler
                    .compile_block(block)
                    .expect("compile gridded block")
            })
            .collect::<Vec<_>>();
        let program = compiler
            .complete(&summary, selected_generation, None)
            .expect("seal gridded program");

        let specification =
            SpectralOperatorSpecification::new(&problem).expect("initial spectral specification");
        let max_replay_block_samples = plan.limits().max_block_samples();
        let workload = spectral_operator_workload(
            &specification,
            max_replay_block_samples,
            SpectralOperatorPass::InitialMajor,
        )
        .expect("initial workload");
        let prepared =
            prepare_spectral_operator(specification, workload).expect("initial operator");
        let mut owner = prepared
            .begin(&problem, &generation)
            .expect("initial complete-data owner");
        owner
            .bind_major_cycle_model(initial_preparation.final_model(), None)
            .expect("bind initial model");
        for block in &weighted_blocks {
            owner.consume_block(block).expect("consume selected block");
        }
        let initial_complete = owner
            .complete(&summary, selected_generation, None)
            .expect("complete initial normal state");

        (
            program,
            gridded_blocks,
            initial_complete,
            max_replay_block_samples,
        )
    };
    let program_alias = program.clone();
    assert_eq!(program_alias.identity(), program.identity());

    let initial_join = MajorCycleOwner::from_complete_data(initial_complete, initial_preparation)
        .expect("initial major owner")
        .reconcile(&mut initial_lifecycle)
        .expect("initial normal state");
    let (initial_normal, initial_continuation) = initial_join.into_continuation();

    let (mut first_lifecycle, first_named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("first continued problem"),
        attempt(255),
        2,
        initial_continuation,
    )
    .expect("first continued lifecycle");
    let first_delta = first_lifecycle
        .compile_delta(
            &first_named,
            [ModelDeltaTerm::new(cell(1), delta_value(0.5))],
        )
        .expect("first model delta");
    let first_preparation =
        MajorCyclePreparation::prepare(&first_lifecycle, first_named, Some(first_delta))
            .expect("first preparation");
    let first_model_generation = first_preparation.final_model_generation();
    let specification =
        SpectralOperatorSpecification::new(&problem).expect("first spectral specification");
    let workload = spectral_operator_workload(
        &specification,
        max_replay_block_samples,
        SpectralOperatorPass::ResidualRefresh,
    )
    .expect("first residual workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("first operator");
    let mut first_apply = program
        .begin_apply(
            &problem,
            first_preparation.final_model(),
            initial_normal,
            prepared,
        )
        .expect("first gridded apply");
    for block in &gridded_blocks {
        first_apply
            .apply_encoded_block(block.sequence(), block.encoded_bytes())
            .expect("apply first borrowed gridded block");
    }
    let first_complete = first_apply.finish().expect("finish first gridded apply");
    let first_join = MajorCycleOwner::from_complete_data(first_complete, first_preparation)
        .expect("first major owner")
        .reconcile(&mut first_lifecycle)
        .expect("first independent completion");
    assert_eq!(
        first_join.normal_state().final_model_generation(),
        first_model_generation
    );
    let first_completion = first_join.completion_id();
    let first_content = first_join.normal_state().content_identity();
    let (first_normal, first_continuation) = first_join.into_continuation();

    let (mut second_lifecycle, second_named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("second continued problem"),
        attempt(1),
        3,
        first_continuation,
    )
    .expect("second continued lifecycle");
    let second_delta = second_lifecycle
        .compile_delta(
            &second_named,
            [ModelDeltaTerm::new(cell(2), delta_value(0.75))],
        )
        .expect("second model delta");
    let second_preparation =
        MajorCyclePreparation::prepare(&second_lifecycle, second_named, Some(second_delta))
            .expect("second preparation");
    let second_model_generation = second_preparation.final_model_generation();
    let specification =
        SpectralOperatorSpecification::new(&problem).expect("second spectral specification");
    let workload = spectral_operator_workload(
        &specification,
        max_replay_block_samples,
        SpectralOperatorPass::ResidualRefresh,
    )
    .expect("second residual workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("second operator");
    let mut second_apply = program
        .begin_apply(
            &problem,
            second_preparation.final_model(),
            first_normal,
            prepared,
        )
        .expect("second gridded apply from the same program");
    for block in &gridded_blocks {
        second_apply
            .apply_encoded_block(block.sequence(), block.encoded_bytes())
            .expect("apply second borrowed gridded block");
    }
    let second_complete = second_apply.finish().expect("finish second gridded apply");
    let second_join = MajorCycleOwner::from_complete_data(second_complete, second_preparation)
        .expect("second major owner")
        .reconcile(&mut second_lifecycle)
        .expect("second independent completion");

    assert_ne!(first_model_generation, second_model_generation);
    assert_ne!(first_completion, second_join.completion_id());
    assert_ne!(first_content, second_join.normal_state().content_identity());
    assert_eq!(program.identity(), program_alias.identity());
}

#[test]
fn sealed_gridded_program_replays_channel_local_cross_channel_groups() {
    let problem = reconstruction_problem_with_sampling(
        247,
        8,
        2,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
        SpectralSamplingLaw::LINEAR,
    );
    let samples = fixture_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let selected_generation = replay_selected_generation(&problem, &samples);
    let generation =
        freeze_weighting_generation_with(&problem, &plan, &samples, split_channel_contributions)
            .expect("freeze split-channel weighting generation");
    let (weighted_blocks, summary) = replay_with(
        &generation,
        &problem,
        &plan,
        &samples,
        split_channel_contributions,
    );
    let mut compiler =
        GriddedNormalOperatorCompiler::new(&problem, SourceCardinalityObservation::Disabled)
            .expect("gridded compiler");
    let gridded_blocks = weighted_blocks
        .iter()
        .map(|block| {
            compiler
                .compile_block(block)
                .expect("compile split-channel gridded block")
        })
        .collect::<Vec<_>>();
    let program = compiler
        .complete(&summary, selected_generation, None)
        .expect("seal split-channel gridded program");
    assert_eq!(
        program.record_count(),
        u64::try_from(samples.len() * 2).expect("record count"),
        "one grouped record is retained for each accepted spectral contribution"
    );

    let mut initial_lifecycle = bind_lifecycle(&problem, attempt(248));
    let initial_model = initial_lifecycle.initial_empty().expect("empty model");
    let initial_preparation =
        MajorCyclePreparation::prepare(&initial_lifecycle, initial_model, None)
            .expect("initial preparation");
    let specification =
        SpectralOperatorSpecification::new(&problem).expect("initial spectral specification");
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::InitialMajor,
    )
    .expect("initial workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("initial operator");
    let mut owner = prepared
        .begin(&problem, &generation)
        .expect("initial complete-data owner");
    owner
        .bind_major_cycle_model(initial_preparation.final_model(), None)
        .expect("bind initial model");
    for block in &weighted_blocks {
        owner.consume_block(block).expect("consume selected block");
    }
    let initial_complete = owner
        .complete(&summary, selected_generation, None)
        .expect("complete initial normal state");
    let initial_join = MajorCycleOwner::from_complete_data(initial_complete, initial_preparation)
        .expect("initial major owner")
        .reconcile(&mut initial_lifecycle)
        .expect("initial channel slab");
    let initial_sum_weights = initial_join.normal_state().sum_weights().to_vec();
    let (initial_normal, continuation) = initial_join.into_continuation();

    let (mut lifecycle, named_model) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("continued problem"),
        attempt(249),
        2,
        continuation,
    )
    .expect("continued lifecycle");
    let delta = lifecycle
        .compile_delta(
            &named_model,
            [
                ModelDeltaTerm::new(ModelCell::new(0, 0, 0, [1, 0]), delta_value(0.5)),
                ModelDeltaTerm::new(ModelCell::new(0, 1, 0, [2, 0]), delta_value(0.75)),
            ],
        )
        .expect("two-channel model delta");
    let preparation = MajorCyclePreparation::prepare(&lifecycle, named_model, Some(delta))
        .expect("residual preparation");
    let final_model_generation = preparation.final_model_generation();
    let specification =
        SpectralOperatorSpecification::new(&problem).expect("residual spectral specification");
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::ResidualRefresh,
    )
    .expect("residual workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("residual operator");
    let mut apply = program
        .begin_apply(
            &problem,
            preparation.final_model(),
            initial_normal,
            prepared,
        )
        .expect("begin channel-local gridded apply");
    for block in &gridded_blocks {
        apply
            .apply_encoded_block(block.sequence(), block.encoded_bytes())
            .expect("apply borrowed split-channel block");
    }
    let complete = apply.finish().expect("finish channel-local gridded apply");
    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("channel-local major owner")
        .reconcile(&mut lifecycle)
        .expect("reconciled channel-local replay");

    assert_eq!(joined.normal_state().channel_count(), 2);
    assert_eq!(joined.normal_state().sum_weights(), initial_sum_weights);
    assert_eq!(
        joined.normal_state().final_model_generation(),
        final_model_generation
    );
    assert!(
        joined
            .normal_state()
            .residual()
            .iter()
            .all(|value| value.re.is_finite() && value.im.is_finite())
    );
}

#[test]
fn t38_independent_channels_share_one_ordered_iteration_budget() {
    let problem = t38_cube_problem_with_controls(244, 2, ReconstructionControls::new(3, 0.5, 0.0));
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable cube problem"),
        attempt(245),
        1,
    )
    .expect("cube lifecycle");
    let initial = lifecycle.initial_empty().expect("empty cube model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare cube model");
    let complete = run_t19_complete_data(&problem, Some(&preparation));
    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("channel-local major-cycle owner")
        .reconcile(&mut lifecycle)
        .expect("channel-local normal state");
    let (normal, continuation) = joined.into_continuation();
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("budgeted cube cycle");
    let iterations = result
        .evidence()
        .channels()
        .iter()
        .map(|channel| {
            channel.minor_cycle().map_or(
                0,
                casa_imaging_reconstruction::MinorCycleEvidence::iterations,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(iterations, vec![2, 1]);
    assert_eq!(result.evidence().iterations(), 3);
    assert!(
        result
            .evidence()
            .channels()
            .iter()
            .all(|channel| !channel.budget_exhausted())
    );
}

#[test]
fn t38_blank_and_unmapped_channels_are_ordered_and_never_cleaned() {
    let problem = t38_cube_problem_with_channels(240, 3);
    let mut samples = fixture_samples(&problem);
    samples[1].input_weight = 0.0;
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable cube problem"),
        attempt(241),
        1,
    )
    .expect("cube lifecycle");
    let initial = lifecycle.initial_empty().expect("empty cube model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare cube model");
    let complete = run_t19_complete_data_with_samples(&problem, Some(&preparation), &samples);
    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("channel-local major-cycle owner")
        .reconcile(&mut lifecycle)
        .expect("channel-local normal state");
    let (normal, continuation) = joined.into_continuation();
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("three-channel reconstruction cycle");
    let channels = result.evidence().channels();
    assert_eq!(
        channels
            .iter()
            .map(|channel| channel.validity())
            .collect::<Vec<_>>(),
        vec![
            SpectralChannelValidity::Valid,
            SpectralChannelValidity::Blank,
            SpectralChannelValidity::Unmapped,
        ]
    );
    assert!(channels[0].minor_cycle().is_some());
    assert!(channels[1].minor_cycle().is_none());
    assert!(channels[2].minor_cycle().is_none());
    assert!(result.delta().is_some_and(|delta| {
        delta
            .terms()
            .iter()
            .all(|term| term.cell().coefficient() == 0)
    }));
}

#[test]
fn t38_late_nsigma_floor_and_first_component_divergence_remain_per_channel() {
    let problem = t38_cube_problem(242);
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable cube problem"),
        attempt(243),
        1,
    )
    .expect("cube lifecycle");
    let initial = lifecycle.initial_empty().expect("empty cube model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare cube model");
    let complete = run_t19_complete_data(&problem, Some(&preparation));
    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("channel-local major-cycle owner")
        .reconcile(&mut lifecycle)
        .expect("channel-local normal state");
    let (normal, continuation) = joined.into_continuation();
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let baseline_program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("compiled nsigma floor")
    .record_component_sequence(16)
    .expect("bounded sequence");
    let candidate_program = MinorCycleProgram::new(0.25, 0.5, 8)
        .expect("candidate controls")
        .record_component_sequence(16)
        .expect("bounded sequence");
    let baseline = ReconstructionCycle::new(ChannelCyclePolicy::Independent, baseline_program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("baseline cube cycle");
    let candidate = ReconstructionCycle::new(ChannelCyclePolicy::Independent, candidate_program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("candidate cube cycle");

    assert!(baseline.evidence().channels().iter().all(|channel| {
        channel
            .minor_cycle()
            .is_some_and(|evidence| evidence.noise_rms().is_some())
    }));
    let divergence = candidate
        .evidence()
        .first_divergence(baseline.evidence())
        .expect("gain change diverges at the first accepted component");
    assert_eq!(divergence.output_channel(), 0);
    assert_eq!(divergence.component().index(), 0);
}

/// The fixture sample stream for one compiled problem.
///
/// Distinct fixture problems describe distinct observation streams: the
/// sample content carries one byte of the compiled problem identity so the
/// minted T17 generation differs whenever the source content differs.
fn fixture_samples(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<SelectedObservationSample> {
    let mut samples = exact_samples(problem);
    let flavour = f32::from(
        problem.selected_observation().read_set().sources()[0]
            .measurement_set()
            .identity()
            .as_bytes()[1],
    );
    for sample in &mut samples {
        sample.input_weight *= 1.0 + flavour / 256.0;
    }
    samples
}

fn exact_samples(problem: &casa_imaging_model::CompiledProblem) -> Vec<SelectedObservationSample> {
    let mut samples = Vec::new();
    for (source_index, source) in problem
        .selected_observation()
        .read_set()
        .sources()
        .iter()
        .enumerate()
    {
        for (row_index, physical_row) in [0_u64, 2].into_iter().enumerate() {
            let data_description_id = row_index as i32;
            let spectral_window_id = row_index as u32;
            let channel_index = row_index as u32;
            let frequency = if row_index == 0 { 1.05e9 } else { 1.15e9 };
            samples.push(SelectedObservationSample {
                address: SelectedSampleAddress {
                    measurement_set: source.measurement_set(),
                    physical_row,
                    data_description_id,
                    spectral_window_id,
                    channel_index,
                    frequency_centre_hz: frequency,
                    frequency_lower_hz: frequency - 5.0e6,
                    frequency_upper_hz: frequency + 5.0e6,
                    channel_width_hz: 1.0e7,
                    frequency_frame: FrequencyFrame::Topocentric,
                    polarization_id: 0,
                    correlation_index: 0,
                    correlation_type: CorrelationType::StokesI,
                },
                visibility: SelectedVisibilitySample::Complex32([
                    1.0 + source_index as f32,
                    row_index as f32,
                ]),
                prediction_target: SelectedPredictionTarget::NotRequested,
                channel_flag: false,
                parallel_hand_group_flag: false,
                row_flag: false,
                input_weight: 1.0 + (source_index * 2 + row_index) as f32,
                coordinates: SelectedSampleCoordinates {
                    raw_uvw_m: [1.0 + row_index as f64, source_index as f64, 0.0],
                    density_uvw_m: [1.0 + row_index as f64, source_index as f64, 0.0],
                    transformed_uvw_m: [1.0 + row_index as f64, source_index as f64, 0.0],
                    phase_shift_m: 0.0,
                    uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
                    time: Epoch::new(59_000.0 + physical_row as f64, TimeScale::Utc),
                    time_centroid: Epoch::new(59_000.0 + physical_row as f64, TimeScale::Utc),
                    interval_seconds: 1.0,
                    exposure_seconds: 1.0,
                    phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    pointing_directions: casa_imaging_model::SelectedPointingDirections {
                        antenna1: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                        antenna2: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    },
                },
                metadata: SelectedSampleMetadata {
                    field_id: 0,
                    antenna1: 0,
                    antenna2: 1,
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
    samples
}

fn exact_contributions(sample: &SelectedObservationSample) -> SelectedSpectralContributions {
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(
            sample.address.channel_index,
            1.0,
            sample.address.frequency_centre_hz,
        ),
        None,
    ])
    .expect("one exact output contribution")
}

fn constant_basis_contributions(
    sample: &SelectedObservationSample,
) -> SelectedSpectralContributions {
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(0, 1.0, sample.address.frequency_centre_hz),
        None,
    ])
    .expect("one constant-basis MFS contribution")
}

fn split_channel_contributions(
    sample: &SelectedObservationSample,
) -> SelectedSpectralContributions {
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(0, 0.25, sample.address.frequency_centre_hz),
        SelectedSpectralContribution::new(1, 0.75, sample.address.frequency_centre_hz),
    ])
    .expect("two linear output contributions")
}

/// Mint the authoritative T17 observation generation of the fixture stream.
///
/// Production binds this identity through the casa-ms traversal seam; the
/// reconstruction fixtures use the same compiler-owned inspection pass, so no
/// second construction path exists.
fn replay_selected_generation(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
) -> SelectedObservationGenerationId {
    let (generation, count) = problem
        .inspect_selected_observation(samples.iter().copied().map(Ok::<_, Infallible>), |_| {
            Ok::<_, Infallible>(())
        })
        .expect("inspect fixture sample stream");
    assert_eq!(
        usize::try_from(count).expect("fixture sample count"),
        samples.len()
    );
    generation
}

fn freeze_weighting_generation(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
) -> Result<WeightingAlgorithmState, WeightingError> {
    freeze_weighting_generation_with(problem, plan, samples, exact_contributions)
}

fn freeze_weighting_generation_with(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
    contributions: impl Fn(&SelectedObservationSample) -> SelectedSpectralContributions + Copy,
) -> Result<WeightingAlgorithmState, WeightingError> {
    let mut density = begin_weighting_generation(problem, plan)?;
    for sample in samples {
        density.consume(problem, sample, contributions(sample))?;
    }
    let mut sum_weight = density.finish(problem)?;
    for sample in samples {
        sum_weight.consume(problem, sample, contributions(sample))?;
    }
    sum_weight.finish()
}

fn replay(
    generation: &WeightingAlgorithmState,
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
) -> (Vec<WeightingReplayChunk>, WeightingReplaySummary) {
    replay_with(generation, problem, plan, samples, exact_contributions)
}

fn replay_with(
    generation: &WeightingAlgorithmState,
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
    contributions: impl Fn(&SelectedObservationSample) -> SelectedSpectralContributions,
) -> (Vec<WeightingReplayChunk>, WeightingReplaySummary) {
    let mut blocks = Vec::new();
    let mut phase = generation
        .begin_replay(problem, plan)
        .expect("begin replay");
    for sample in samples {
        if let Some(block) = phase
            .consume(problem, sample, contributions(sample))
            .expect("weight sample")
        {
            blocks.push(block);
        }
    }
    let (final_block, completion) = phase.finish().expect("finish replay state");
    if let Some(block) = final_block {
        blocks.push(block);
    }
    (blocks, completion)
}

/// Drive one exhaustive T19 complete-data replay and mint its paired evidence.
///
/// The returned value keeps primitives and completion inseparably paired; the
/// only way to split them is to hand the whole result to the Major-Cycle owner.
fn run_t19_complete_data(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
) -> CompleteDataOwnerResult {
    run_t19_complete_data_with_samples(problem, preparation, &fixture_samples(problem))
}

fn run_t19_complete_data_with_samples(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
    samples: &[SelectedObservationSample],
) -> CompleteDataOwnerResult {
    run_t19_complete_data_with_transform(problem, preparation, samples, None)
}

fn run_t19_complete_data_with_transform(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
    samples: &[SelectedObservationSample],
    transform_generation: Option<ContinuumTransformGenerationId>,
) -> CompleteDataOwnerResult {
    run_t19_complete_data_for_pass(
        problem,
        preparation,
        samples,
        transform_generation,
        SpectralOperatorPass::InitialMajor,
        None,
    )
}

fn run_t19_complete_data_for_pass(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
    samples: &[SelectedObservationSample],
    transform_generation: Option<ContinuumTransformGenerationId>,
    pass: SpectralOperatorPass,
    prior_normal_state: Option<casa_imaging_reconstruction::FinalNormalState>,
) -> CompleteDataOwnerResult {
    run_t19_complete_data_for_pass_result(
        problem,
        preparation,
        samples,
        transform_generation,
        pass,
        prior_normal_state,
    )
    .expect("complete T19 evidence")
}

fn run_t19_complete_data_for_pass_result(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
    samples: &[SelectedObservationSample],
    transform_generation: Option<ContinuumTransformGenerationId>,
    pass: SpectralOperatorPass,
    prior_normal_state: Option<casa_imaging_reconstruction::FinalNormalState>,
) -> Result<CompleteDataOwnerResult, SpectralOperatorError> {
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let selected_generation = replay_selected_generation(problem, samples);
    let generation = freeze_weighting_generation(problem, &plan, samples)
        .expect("freeze global weighting generation");
    let (blocks, summary) = replay(&generation, problem, &plan, samples);
    assert!(!blocks.is_empty(), "replay must emit bounded blocks");

    let specification =
        SpectralOperatorSpecification::new(problem).expect("spectral operator specification");
    let workload =
        spectral_operator_workload(&specification, plan.limits().max_block_samples(), pass)
            .expect("workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(problem, &generation)
        .expect("begin complete-data owner");
    if let Some(preparation) = preparation {
        state
            .bind_major_cycle_model(preparation.final_model(), prior_normal_state)
            .expect("bind exact final model before replay");
    }
    for block in &blocks {
        assert!(
            state
                .consume_block(block)
                .expect("consume weighted block")
                .is_empty(),
            "a complete-data replay without a final-visibility sink must not emit samples"
        );
    }
    state.complete(&summary, selected_generation, transform_generation)
}

fn prepare_reconciliation(
    problem: &casa_imaging_model::CompiledProblem,
    lifecycle: &ModelLifecycle,
    named: ModelGeneration,
    delta: Option<ModelDelta>,
) -> (CompleteDataOwnerResult, MajorCyclePreparation) {
    let preparation =
        MajorCyclePreparation::prepare(lifecycle, named, delta).expect("prepare final model");
    let evidence = run_t19_complete_data(problem, Some(&preparation));
    (evidence, preparation)
}

#[cfg(feature = "cpp-interop-tests")]
fn prepare_reconciliation_reusing(
    problem: &casa_imaging_model::CompiledProblem,
    lifecycle: &ModelLifecycle,
    named: ModelGeneration,
    delta: Option<ModelDelta>,
    prior_normal_state: casa_imaging_reconstruction::FinalNormalState,
) -> (CompleteDataOwnerResult, MajorCyclePreparation) {
    let preparation =
        MajorCyclePreparation::prepare(lifecycle, named, delta).expect("prepare final model");
    let evidence = run_t19_complete_data_for_pass(
        problem,
        Some(&preparation),
        &fixture_samples(problem),
        None,
        SpectralOperatorPass::ResidualRefresh,
        Some(prior_normal_state),
    );
    (evidence, preparation)
}

#[test]
fn bound_major_cycle_model_cannot_be_replaced_by_diagnostic_prediction() {
    let problem = t19_compatible_problem(37);
    let lifecycle = bind_lifecycle(&problem, attempt(38));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare final model");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let samples = fixture_samples(&problem);
    let generation = freeze_weighting_generation(&problem, &plan, &samples)
        .expect("freeze weighting generation");
    let (blocks, _) = replay(&generation, &problem, &plan, &samples);
    let specification =
        SpectralOperatorSpecification::new(&problem).expect("spectral operator specification");
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::InitialMajor,
    )
    .expect("spectral operator workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(&problem, &generation)
        .expect("begin complete-data owner");
    state
        .bind_major_cycle_model(preparation.final_model(), None)
        .expect("bind final model");
    let arbitrary_model = vec![num_complex::Complex64::new(1.0, 0.0); 8 * 8];

    assert_eq!(
        state
            .predict_block(&arbitrary_model, &blocks[0])
            .expect_err("prediction must not overwrite the final-model grid"),
        SpectralOperatorError::PredictionAfterMajorCycleBinding
    );
}

fn bind_lifecycle(
    problem: &casa_imaging_model::CompiledProblem,
    attempt: ModelExecutionAttemptId,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem"),
        attempt,
        7,
    )
    .expect("bind model lifecycle")
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn delta_value(value: f64) -> casa_imaging_model::ModelValue {
    casa_imaging_model::ModelValue::new(value).expect("finite model value")
}

#[test]
fn schema_versions_record_the_t20_completion_records() {
    assert_eq!(FinalModelCompletionId::SCHEMA_VERSION, 2);
    assert_eq!(
        casa_imaging_reconstruction::FinalNormalStateCompletionId::SCHEMA_VERSION,
        2
    );
    assert_eq!(
        casa_imaging_reconstruction::MajorCycleCompletionId::SCHEMA_VERSION,
        2
    );
}

#[test]
fn reconciliation_applies_one_pending_delta_through_the_model_owner() {
    let problem = t19_compatible_problem(11);
    let mut lifecycle = bind_lifecycle(&problem, attempt(21));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(1), delta_value(-2.5))])
        .expect("pending Högbom-style delta");
    let delta_id = delta.delta_id();
    let (evidence, preparation) = prepare_reconciliation(&problem, &lifecycle, named, Some(delta));
    // The data-side dirty plane remains T19 evidence beside the exact residual.
    let data_side_content = evidence.primitives().normal_state_content_identity();
    let sample_count = evidence.completion().sample_count();
    let block_count = evidence.completion().block_count();
    let weighting_generation = evidence.completion().weighting_generation();

    let owner =
        MajorCycleOwner::from_complete_data(evidence, preparation).expect("T20 owner from T19");
    assert_eq!(owner.weighting_generation(), weighting_generation);
    let joined = owner
        .reconcile(&mut lifecycle)
        .expect("atomic Major-Cycle reconciliation");

    // One inseparable result carrying two distinct opaque typed records plus
    // the authoritative final model generation.
    let normal_state = joined.normal_state();
    let model_completion = joined.model_completion();
    assert_ne!(
        normal_state.completion_id().as_bytes(),
        model_completion.completion_id().as_bytes()
    );
    assert_ne!(
        joined.completion_id().as_bytes(),
        normal_state.completion_id().as_bytes()
    );

    // The Normal State record names the full T17/T18/T19 lineage and both models.
    assert_eq!(normal_state.problem_id(), problem.problem_id());
    assert_eq!(normal_state.geometry_id(), problem.geometry().geometry_id());
    assert_eq!(normal_state.numerics_id(), problem.numerics_id());
    assert_eq!(normal_state.sample_count(), sample_count);
    assert_eq!(normal_state.block_count(), block_count);
    assert_eq!(
        normal_state.catalog(),
        casa_imaging_reconstruction::NormalStateCatalog::UnnormalizedPlaneV1
    );
    // The residual content is model-dependent: a nonzero final model never
    // relabels the data-side dirty plane.
    assert_ne!(normal_state.content_identity(), data_side_content);
    assert_eq!(
        normal_state.input_model_generation(),
        model_completion.base()
    );
    assert_eq!(
        normal_state.final_model_generation(),
        model_completion.generation()
    );
    assert_eq!(
        joined.final_model().generation_id(),
        model_completion.generation()
    );
    assert!(joined.final_model().samples()[1].value().value() == -2.5);

    // The pending delta was applied only through the model owner.
    assert_eq!(model_completion.delta(), Some(delta_id));
    assert_eq!(model_completion.attempt(), attempt(21));
    assert_eq!(model_completion.epoch(), 7);
}

#[test]
fn reconciliation_without_a_pending_delta_confirms_the_named_generation_final() {
    let problem = t19_compatible_problem(12);
    let mut lifecycle = bind_lifecycle(&problem, attempt(22));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let input_id = named.generation_id();
    let (evidence, preparation) = prepare_reconciliation(&problem, &lifecycle, named, None);
    let data_side_content = evidence.primitives().normal_state_content_identity();
    let data_side_dirty = evidence.primitives().dirty().to_vec();

    let owner =
        MajorCycleOwner::from_complete_data(evidence, preparation).expect("T20 owner from T19");
    let joined = owner
        .reconcile(&mut lifecycle)
        .expect("confirm-only reconciliation");

    assert_eq!(joined.model_completion().delta(), None);
    assert_eq!(joined.model_completion().base(), input_id);
    assert_eq!(joined.model_completion().generation(), input_id);
    assert_eq!(joined.normal_state().input_model_generation(), input_id);
    assert_eq!(joined.normal_state().final_model_generation(), input_id);
    // An empty final model reconciles to the exact T19 dirty plane bit-for-bit.
    assert_eq!(joined.normal_state().content_identity(), data_side_content);
    assert_eq!(joined.normal_state().residual(), data_side_dirty);
    assert_eq!(joined.normal_state().normal_approximation().len(), 8 * 8);
    assert_eq!(joined.normal_state().sensitivity().len(), 8 * 8);
    assert!(joined.normal_state().sum_weight() > 0.0);
}

#[test]
fn empty_initial_model_emits_zero_predictions_only_for_an_explicit_sink() {
    let problem = t19_compatible_problem(66);
    let lifecycle = bind_lifecycle(&problem, attempt(67));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("empty preparation");
    let samples = fixture_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let selected_generation = replay_selected_generation(&problem, &samples);
    let weighting =
        freeze_weighting_generation(&problem, &plan, &samples).expect("weighting generation");
    let (blocks, summary) = replay(&weighting, &problem, &plan, &samples);
    let specification =
        SpectralOperatorSpecification::new(&problem).expect("spectral specification");
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::InitialMajor,
    )
    .expect("initial workload");
    let mut state = prepare_spectral_operator(specification, workload)
        .expect("prepare operator")
        .begin(&problem, &weighting)
        .expect("begin owner");
    state
        .bind_major_cycle_model(preparation.final_model(), None)
        .expect("bind certified empty model");
    state.enable_final_visibility_samples();

    let mut emitted = 0_usize;
    for block in &blocks {
        let output = state.consume_block(block).expect("consume weighted block");
        emitted += output.len();
        assert!(output.iter().all(|sample| {
            sample.predicted() == num_complex::Complex64::default()
                && sample.residual() == sample.observed()
        }));
    }
    assert_eq!(emitted, samples.len());
    state
        .complete(&summary, selected_generation, None)
        .expect("complete empty sink-enabled replay");
}

#[test]
fn all_zero_ingested_model_uses_the_general_operator_and_matches_empty_science() {
    let seed = identity(68, 1);
    let support = casa_imaging_reconstruction::model_support_identity(std::iter::repeat_n(
        casa_imaging_model::ModelSupport::Valid,
        8 * 8,
    ));
    let problem = reconstruction_problem_with_sampling_and_model(
        69,
        8,
        1,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
        (
            SpectralSamplingLaw::IDENTITY,
            ModelStateIdentity::Seed(seed),
            ModelInputCommitment::AlignedSeed {
                source: seed,
                support,
            },
        ),
    );
    let lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("ingested problem"),
        attempt(70),
        1,
    )
    .expect("ingested lifecycle");
    let target = lifecycle.contract().target().clone();
    let zeros = (0..target.sample_count()).map(|_| {
        Ok::<_, Infallible>(casa_imaging_model::ModelSample::valid(
            casa_imaging_model::ModelValue::new(0.0).expect("zero model value"),
        ))
    });
    let ingested = lifecycle
        .ingest_aligned(seed, &target, zeros)
        .expect("read aligned zero model")
        .expect("ingest aligned zero model");
    assert!(matches!(
        ingested.origin(),
        casa_imaging_reconstruction::ModelGenerationOrigin::Ingested { .. }
    ));
    let preparation = MajorCyclePreparation::prepare(&lifecycle, ingested, None)
        .expect("prepare ingested zero model");
    let ingested_evidence = run_t19_complete_data(&problem, Some(&preparation));

    let empty_problem = t19_compatible_problem(69);
    let empty_lifecycle = bind_lifecycle(&empty_problem, attempt(71));
    let empty = empty_lifecycle
        .initial_empty()
        .expect("empty reference model");
    let empty_preparation = MajorCyclePreparation::prepare(&empty_lifecycle, empty, None)
        .expect("prepare empty reference");
    let empty_evidence = run_t19_complete_data(&empty_problem, Some(&empty_preparation));

    assert_eq!(
        ingested_evidence.primitives().dirty(),
        empty_evidence.primitives().dirty()
    );
    assert_eq!(
        ingested_evidence.primitives().psf(),
        empty_evidence.primitives().psf()
    );
    assert_eq!(
        ingested_evidence.primitives().sensitivity(),
        empty_evidence.primitives().sensitivity()
    );
    assert_eq!(
        ingested_evidence.primitives().sum_weights(),
        empty_evidence.primitives().sum_weights()
    );
    assert_eq!(
        ingested_evidence.primitives().channel_validity(),
        empty_evidence.primitives().channel_validity()
    );
    assert_eq!(
        ingested_evidence
            .primitives()
            .normal_state_content_identity(),
        empty_evidence.primitives().normal_state_content_identity()
    );
}

#[test]
fn empty_origin_residual_refresh_uses_the_general_operator() {
    let problem = t19_compatible_problem(72);
    let mut initial_lifecycle = bind_lifecycle(&problem, attempt(73));
    let initial_model = initial_lifecycle.initial_empty().expect("empty model");
    let initial_preparation =
        MajorCyclePreparation::prepare(&initial_lifecycle, initial_model, None)
            .expect("initial preparation");
    let initial_complete = run_t19_complete_data(&problem, Some(&initial_preparation));
    let initial_join = MajorCycleOwner::from_complete_data(initial_complete, initial_preparation)
        .expect("initial major owner")
        .reconcile(&mut initial_lifecycle)
        .expect("initial normal state");
    let initial_content = initial_join.normal_state().content_identity();
    let (initial_normal, continuation) = initial_join.into_continuation();
    let (mut continued_lifecycle, carried_model) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("continued problem"),
        attempt(74),
        2,
        continuation,
    )
    .expect("continue empty model");
    assert_eq!(
        carried_model.origin(),
        casa_imaging_reconstruction::ModelGenerationOrigin::Empty
    );
    let refresh_preparation =
        MajorCyclePreparation::prepare(&continued_lifecycle, carried_model, None)
            .expect("refresh preparation");
    let refresh_complete = run_t19_complete_data_for_pass(
        &problem,
        Some(&refresh_preparation),
        &fixture_samples(&problem),
        None,
        SpectralOperatorPass::ResidualRefresh,
        Some(initial_normal),
    );
    let refreshed = MajorCycleOwner::from_complete_data(refresh_complete, refresh_preparation)
        .expect("refresh major owner")
        .reconcile(&mut continued_lifecycle)
        .expect("refresh normal state");
    assert_eq!(refreshed.normal_state().content_identity(), initial_content);
}

#[test]
fn residual_content_depends_on_the_exact_final_model() {
    let problem = t19_compatible_problem(27);
    // The same T19 evidence reconciled against two different final models.
    let mut empty_lifecycle = bind_lifecycle(&problem, attempt(28));
    let empty_named = empty_lifecycle
        .initial_empty()
        .expect("empty named generation");
    let (empty_evidence, empty_preparation) =
        prepare_reconciliation(&problem, &empty_lifecycle, empty_named, None);
    let empty_join = MajorCycleOwner::from_complete_data(empty_evidence, empty_preparation)
        .expect("owner from intact T19 pairing")
        .reconcile(&mut empty_lifecycle)
        .expect("empty-model reconciliation");

    let mut delta_lifecycle = bind_lifecycle(&problem, attempt(29));
    let delta_named = delta_lifecycle
        .initial_empty()
        .expect("empty named generation");
    let delta = delta_lifecycle
        .compile_delta(
            &delta_named,
            [ModelDeltaTerm::new(cell(3), delta_value(1.75))],
        )
        .expect("pending delta");
    let (delta_evidence, delta_preparation) =
        prepare_reconciliation(&problem, &delta_lifecycle, delta_named, Some(delta));
    let delta_join = MajorCycleOwner::from_complete_data(delta_evidence, delta_preparation)
        .expect("owner from intact T19 pairing")
        .reconcile(&mut delta_lifecycle)
        .expect("delta reconciliation");

    assert_ne!(
        empty_join.normal_state().content_identity(),
        delta_join.normal_state().content_identity(),
        "a nonzero final model must change the authoritative residual content"
    );
    assert_ne!(
        empty_join.normal_state().residual(),
        delta_join.normal_state().residual(),
        "the retained authoritative residual must depend on the final model"
    );
    assert_ne!(empty_join.completion_id(), delta_join.completion_id());
    assert_ne!(
        empty_join.normal_state().final_model_generation(),
        delta_join.normal_state().final_model_generation()
    );
}

#[test]
fn completion_ids_stay_stable_across_owner_allocations() {
    let problem = t19_compatible_problem(30);
    // Two independent reconciliation passes over identical evidence, each with
    // its own process-local lifecycle allocation but the same stable
    // problem/attempt/epoch binding.
    let mut first = bind_lifecycle(&problem, attempt(31));
    let mut second = bind_lifecycle(&problem, attempt(31));
    let first_named = first.initial_empty().expect("first named generation");
    let second_named = second.initial_empty().expect("second named generation");
    let (first_evidence, first_preparation) =
        prepare_reconciliation(&problem, &first, first_named, None);
    let (second_evidence, second_preparation) =
        prepare_reconciliation(&problem, &second, second_named, None);

    let first_join = MajorCycleOwner::from_complete_data(first_evidence, first_preparation)
        .expect("first owner")
        .reconcile(&mut first)
        .expect("first reconciliation");
    let second_join = MajorCycleOwner::from_complete_data(second_evidence, second_preparation)
        .expect("second owner")
        .reconcile(&mut second)
        .expect("second reconciliation");

    assert_eq!(
        first_join.completion_id(),
        second_join.completion_id(),
        "completion IDs hash the stable lifecycle authority, not a process-local seal"
    );
    assert_eq!(
        first_join.normal_state().completion_id(),
        second_join.normal_state().completion_id()
    );
}

#[test]
fn transformed_visibility_generation_cannot_be_substituted_under_raw_lineage() {
    let problem = t19_compatible_problem(41);
    let samples = fixture_samples(&problem);
    let selected_generation = replay_selected_generation(&problem, &samples);
    let first_transform = ContinuumTransformGenerationId::from_owner_digest([1; 32]);
    let second_transform = ContinuumTransformGenerationId::from_owner_digest([2; 32]);

    let reconcile = |transform_generation, attempt_byte| {
        let mut lifecycle = bind_lifecycle(&problem, attempt(attempt_byte));
        let named = lifecycle.initial_empty().expect("empty named generation");
        let preparation =
            MajorCyclePreparation::prepare(&lifecycle, named, None).expect("preparation");
        let evidence = run_t19_complete_data_with_transform(
            &problem,
            Some(&preparation),
            &samples,
            Some(transform_generation),
        );
        MajorCycleOwner::from_complete_data(evidence, preparation)
            .expect("owner")
            .reconcile(&mut lifecycle)
            .expect("reconciliation")
    };

    let first = reconcile(first_transform, 42);
    let second = reconcile(second_transform, 42);
    assert_eq!(
        first.normal_state().selected_generation(),
        selected_generation
    );
    assert_eq!(
        second.normal_state().selected_generation(),
        selected_generation
    );
    assert_eq!(
        first.normal_state().continuum_transform_generation(),
        Some(first_transform)
    );
    assert_eq!(
        second.normal_state().continuum_transform_generation(),
        Some(second_transform)
    );
    assert_ne!(
        first.normal_state().completion_id(),
        second.normal_state().completion_id()
    );
    assert_ne!(first.completion_id(), second.completion_id());
}

#[test]
fn observation_generation_lineage_is_bound_into_the_normal_state() {
    // Two problems whose fixture streams carry different observation content.
    let first_problem = t19_compatible_problem(33);
    let second_problem = t19_compatible_problem_with_width(34, 12);

    for (problem, attempt_byte) in [(&first_problem, 35_u8), (&second_problem, 36)] {
        let expected = replay_selected_generation(problem, &fixture_samples(problem));
        let mut lifecycle = bind_lifecycle(problem, attempt(attempt_byte));
        let named = lifecycle.initial_empty().expect("named generation");
        let (evidence, preparation) = prepare_reconciliation(problem, &lifecycle, named, None);
        let join = MajorCycleOwner::from_complete_data(evidence, preparation)
            .expect("owner from intact T19 pairing")
            .reconcile(&mut lifecycle)
            .expect("reconciliation");
        assert_eq!(
            join.normal_state().selected_generation(),
            expected,
            "Final Normal State carries the exact authoritative observation generation"
        );
    }

    // Distinct observation streams never share one lineage identity.
    let first_generation =
        replay_selected_generation(&first_problem, &fixture_samples(&first_problem));
    let second_generation =
        replay_selected_generation(&second_problem, &fixture_samples(&second_problem));
    assert_ne!(first_generation, second_generation);
}

#[test]
fn reconciliation_fails_atomically_and_leaves_both_authorities_intact() {
    let problem = t19_compatible_problem(13);
    let other_problem = t19_compatible_problem(14);

    // A lifecycle bound to another compiled problem is stale model evidence.
    let mut foreign_problem_lifecycle = bind_lifecycle(&other_problem, attempt(23));
    let foreign_named = foreign_problem_lifecycle
        .initial_empty()
        .expect("foreign empty generation");
    let foreign_preparation =
        MajorCyclePreparation::prepare(&foreign_problem_lifecycle, foreign_named, None)
            .expect("prepare foreign problem model");
    let foreign_evidence = run_t19_complete_data(&problem, Some(&foreign_preparation));
    let stale_problem = MajorCycleOwner::from_complete_data(foreign_evidence, foreign_preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut foreign_problem_lifecycle)
        .expect_err("stale model evidence must fail closed");
    assert!(matches!(stale_problem, MajorCycleError::StaleModelEvidence));

    let mut lifecycle = bind_lifecycle(&problem, attempt(23));

    // A foreign generation cannot be named through this lifecycle owner.
    let other_owner_same_problem = bind_lifecycle(&problem, attempt(24));
    let foreign_generation = other_owner_same_problem
        .initial_empty()
        .expect("same-problem foreign generation");
    let foreign = MajorCyclePreparation::prepare(&lifecycle, foreign_generation, None)
        .expect_err("foreign generation must fail before replay");
    assert!(matches!(
        foreign,
        MajorCycleError::Model(ModelLifecycleError::ForeignModelLifecycle)
    ));

    // A delta bound to another base fails before anything is minted, and the
    // lifecycle remains open for a correct reconciliation afterwards.
    let scratch_base = lifecycle.initial_empty().expect("scratch base generation");
    let bump = lifecycle
        .compile_delta(
            &scratch_base,
            [ModelDeltaTerm::new(cell(3), delta_value(0.5))],
        )
        .expect("non-final scratch delta");
    let alternative_base = lifecycle
        .apply_delta(scratch_base, bump)
        .expect("advanced scratch base");
    let misbound_delta = lifecycle
        .compile_delta(
            &alternative_base,
            [ModelDeltaTerm::new(cell(2), delta_value(1.0))],
        )
        .expect("delta against the alternative base");
    let named = lifecycle.initial_empty().expect("fresh named generation");
    assert_ne!(alternative_base.generation_id(), named.generation_id());
    let misbound = MajorCyclePreparation::prepare(&lifecycle, named, Some(misbound_delta))
        .expect_err("misbound delta must fail before replay");
    assert!(matches!(
        misbound,
        MajorCycleError::Model(ModelLifecycleError::DeltaBaseMismatch)
    ));

    // The same authorities then complete exactly once.
    let named = lifecycle.initial_empty().expect("named after repairs");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(2), delta_value(1.0))])
        .expect("correctly bound delta");
    let (evidence, preparation) = prepare_reconciliation(&problem, &lifecycle, named, Some(delta));
    let joined = MajorCycleOwner::from_complete_data(evidence, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("reconciliation succeeds after atomic failures");
    assert_eq!(
        joined.model_completion().attempt(),
        casa_imaging_model::ModelExecutionAttemptId::new(identity(23, 0))
    );

    // Mutation and replay are impossible: the final authority is consumed.
    let late_base = lifecycle.initial_empty();
    assert!(matches!(
        late_base,
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
}

#[test]
fn incomplete_or_foreign_operator_evidence_cannot_become_a_major_cycle_owner() {
    // A specification mismatch keeps raw problems from forging operator plans.
    let problem = t19_compatible_problem(15);
    let other = t19_compatible_problem(16);
    let specification = SpectralOperatorSpecification::new(&other).expect("other specification");
    let workload =
        spectral_operator_workload(&specification, 1, SpectralOperatorPass::InitialMajor)
            .expect("other workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepared");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("limits"),
    )
    .expect("plan");
    let samples = exact_samples(&problem);
    let generation = freeze_weighting_generation(&problem, &plan, &samples).expect("frozen W");
    let foreign = prepared
        .begin(&problem, &generation)
        .expect_err("foreign prepared operator cannot adopt this problem");
    assert!(matches!(foreign, SpectralOperatorError::ProblemMismatch));

    // Exhaustive coverage is required before any owner can exist.
    let evidence = run_t19_complete_data(&problem, None);
    let completion = evidence.completion();
    assert!(completion.sample_count() > 0 && completion.block_count() > 0);
    assert!(completion.coverage_proof_bytes() > 0);
    assert!(completion.coverage_proof_hash_calls() > 0);
    assert_eq!(completion.problem_id(), problem.problem_id());
}
