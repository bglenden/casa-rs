// SPDX-License-Identifier: LGPL-3.0-or-later

//! T43 focused acceptance for coupled MT-MFS point and multiscale Minor Cycles.
//! Normal states come from synthetic passes over a known two-term Taylor sky
//! (`support/synthetic_pass.rs`).

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame, GeometryInput,
    IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, IntentSelection, LogicalIdentity, MeasurementEquationContract,
    MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, ModelBounds, ModelColumnState,
    ModelColumnWrite, ModelExecutionAttemptId, ModelInnerProduct, ModelInputCommitment,
    ModelLifecycleRequirements, ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage,
    NumericsContract, ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedRows, SkyDirection,
    SourceGenerations, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection,
    StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, TimeSelection, UvSelection,
    UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ChannelCyclePolicy, ExecutableModelProblem, FinalNormalState, MajorCyclePreparation,
    MinorCycleProgram, MinorCycleStopReason, MinorCycleValidity, ModelGeneration, ModelLifecycle,
    ReconstructionCycle, ReconstructionMask, run_minor_cycle,
};

#[path = "support/synthetic_pass.rs"]
mod synthetic_pass;
use synthetic_pass::Scene;

const REFERENCE_FREQUENCY_HZ: f64 = 1.0e9;
const IMAGE_WIDTH: usize = 16;

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
        ObservationSourceProvenance::new("fixture://t42/multi-spw".to_owned(), identity(42, 2)),
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
            ProductBlankingPolicy::Zero,
        )
        .expect("valid primary-beam policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid Taylor policy"),
    )
}

fn problem() -> casa_imaging_model::CompiledProblem {
    problem_with_scales(vec![0.0])
}

fn problem_with_scales(scales_px: Vec<f64>) -> casa_imaging_model::CompiledProblem {
    problem_with_controls(scales_px, ReconstructionControls::new(8, 1.0, 0.0))
}

fn problem_with_controls(
    scales_px: Vec<f64>,
    controls: ReconstructionControls,
) -> casa_imaging_model::CompiledProblem {
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
    .expect("compile T42 observation");
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
                controls,
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
            ModelBounds::new(1_024, 1_024, 1_024, 1_024, 1.0e30, 1.0e30).expect("T42 model bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile T42 MT-MFS problem")
}

/// The fixed sky: an extended source with a falling spectrum beside a
/// compact one with a rising spectrum, plus a low residual field, so scale
/// selection, the coupled Taylor solve and the noise estimate all have
/// structure to act on.
fn scene(problem: &casa_imaging_model::CompiledProblem) -> Scene {
    Scene::new(problem)
        .with_gaussian([10.0, 8.0], 2.5, &[1.1, -0.35])
        .with_point([5, 11], &[0.7, 0.2])
        .with_noise(0.01)
}

fn run_final_normal_state(
    problem: &casa_imaging_model::CompiledProblem,
) -> (ModelLifecycle, FinalNormalState, ModelGeneration) {
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable Taylor problem");
    let mut lifecycle = ModelLifecycle::bind(
        executable,
        ModelExecutionAttemptId::new(identity(42, 120)),
        1,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("bind Taylor model lifecycle");
    let initial = lifecycle.initial_empty().expect("empty Taylor model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare Taylor model");
    let completion = scene(problem).reconcile(problem, &mut lifecycle, preparation);
    let (normal, model_completion, final_model) = completion.into_parts();
    assert_eq!(
        normal.input_model_generation(),
        model_completion.base(),
        "normal and model completions bind the same input generation"
    );
    assert_eq!(
        normal.final_model_generation(),
        final_model.generation_id(),
        "normal state binds the exact authoritative final model"
    );
    (lifecycle, normal, final_model)
}

fn full_mask(normal: &FinalNormalState, model: &ModelGeneration) -> ReconstructionMask {
    let centre = normal.shape()[0] as f64 / 2.0;
    ReconstructionMask::full_plane(
        normal.problem_id(),
        model.generation_id(),
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [centre, centre],
            [-1.0e-6, 1.0e-6],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        ),
        normal.shape(),
    )
    .expect("full Taylor reconstruction mask")
}

fn one_pixel_mask(
    normal: &FinalNormalState,
    model: &ModelGeneration,
    pixel: [usize; 2],
) -> ReconstructionMask {
    let centre = normal.shape()[0] as f64 / 2.0;
    let coordinate = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [centre, centre],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let mut support = vec![false; normal.shape()[0] * normal.shape()[1]];
    support[pixel[0] * normal.shape()[1] + pixel[1]] = true;
    ReconstructionMask::from_reprojected_support(
        normal.problem_id(),
        model.generation_id(),
        coordinate,
        normal.shape(),
        &support,
        coordinate,
        normal.shape(),
    )
    .expect("one-pixel Taylor mask")
}

fn point_program(
    problem: &casa_imaging_model::CompiledProblem,
    iterations: usize,
) -> MinorCycleProgram {
    MinorCycleProgram::for_problem(problem)
        .expect("coupled point MT-MFS controls")
        .limit_iterations(iterations)
        .expect("positive point iteration limit")
}

fn run_point(
    problem: casa_imaging_model::CompiledProblem,
    program: MinorCycleProgram,
    mask_pixel: Option<[usize; 2]>,
) -> casa_imaging_reconstruction::MinorCycleResult {
    let (lifecycle, normal, model) = run_final_normal_state(&problem);
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    assert_eq!(model.shape().coefficients(), 2);
    assert_eq!(model.shape().domains()[0].pixels(), normal.shape());
    assert_eq!(
        model
            .read_samples(0..model.sample_count())
            .expect("read fixture model")
            .len(),
        model.shape().sample_count()
    );
    let mask = mask_pixel.map_or_else(
        || full_mask(&normal, &model),
        |pixel| one_pixel_mask(&normal, &model, pixel),
    );
    run_minor_cycle(&lifecycle, &model, &normal, &mask, program)
        .expect("coupled point MT-MFS solve")
}

fn assert_close(actual: f64, expected: f64, context: &str) {
    let scale = actual.abs().max(expected.abs()).max(1.0);
    assert!(
        (actual - expected).abs() <= 1.0e-10 * scale,
        "{context}: actual={actual:e}, expected={expected:e}"
    );
}

#[test]
fn t51_mtmfs_noise_population_is_independent_of_the_clean_mask() {
    let problem = problem_with_controls(
        vec![0.0, 2.0],
        ReconstructionControls::new(8, 0.1, 0.0).with_noise_sigma(5.0),
    );
    let (lifecycle, normal, model) = run_final_normal_state(&problem);
    let full = run_minor_cycle(
        &lifecycle,
        &model,
        &normal,
        &full_mask(&normal, &model),
        point_program(&problem, 1),
    )
    .expect("full-mask MT-MFS noise estimate");
    let source = run_minor_cycle(
        &lifecycle,
        &model,
        &normal,
        &one_pixel_mask(&normal, &model, [10, 8]),
        point_program(&problem, 1),
    )
    .expect("source-mask MT-MFS noise estimate");
    let full_rms = full.evidence().noise_rms().expect("full-image noise RMS");
    let source_rms = source
        .evidence()
        .noise_rms()
        .expect("source-mask noise RMS");
    assert!(
        full_rms > 0.0,
        "fixture must contain spatial residual variation"
    );
    assert_eq!(
        source_rms, full_rms,
        "CLEAN support chooses components, not the fast-noise population (CASA SIImageStore::calcRobustRMS)"
    );
}

#[test]
fn t51_global_convergence_tolerance_does_not_relax_the_minor_cycle_threshold() {
    let baseline = problem_with_scales(vec![0.0, 2.0]);
    let reference = run_point(baseline.clone(), point_program(&baseline, 1), None);
    let peak = reference.evidence().initial_peak_flux();

    for (ratio, expected_iterations) in [(1.005, 0), (1.011, 1)] {
        for use_noise in [false, true] {
            let threshold = peak / ratio;
            let controls = if use_noise {
                let probe = problem_with_controls(
                    vec![0.0, 2.0],
                    ReconstructionControls::new(8, 0.1, 0.0).with_noise_sigma(1.0),
                );
                let noise = run_point(probe.clone(), point_program(&probe, 1), None)
                    .evidence()
                    .noise_rms()
                    .expect("nonzero fixture noise");
                assert!(noise > 0.0);
                ReconstructionControls::new(8, 0.1, 0.0).with_noise_sigma(threshold / noise)
            } else {
                ReconstructionControls::new(8, 0.1, threshold)
            };
            let problem = problem_with_controls(vec![0.0, 2.0], controls);
            let (lifecycle, normal, model) = run_final_normal_state(&problem);
            let mask = full_mask(&normal, &model);
            let program = point_program(&problem, 1);
            let minor = run_minor_cycle(&lifecycle, &model, &normal, &mask, program.clone())
                .expect("standalone minor solve");
            assert_eq!(
                minor.evidence().iterations(),
                1,
                "minor threshold stays exact"
            );
            let cycle = ReconstructionCycle::new(ChannelCyclePolicy::Coupled, program)
                .run(&lifecycle, &model, &normal, &mask)
                .expect("fresh-state coupled reconstruction cycle");
            assert_eq!(
                cycle.evidence().iterations(),
                expected_iterations,
                "CASA's 1% global entry tolerance: ratio={ratio}, nsigma={use_noise}"
            );
            if expected_iterations == 0 {
                assert!(cycle.delta().is_none());
                assert!(!cycle.evidence().requests_reconciliation());
                assert!(cycle.evidence().recorded_components().next().is_none());
            }
        }
    }
}

#[test]
fn t43_point_selection_solves_the_declared_cross_term_block_in_coefficient_order() {
    let problem = problem();
    let (lifecycle, normal, model) = run_final_normal_state(&problem);
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    assert_eq!(model.shape().coefficients(), 2);
    let result = run_minor_cycle(
        &lifecycle,
        &model,
        &normal,
        &full_mask(&normal, &model),
        point_program(&problem, 1),
    )
    .expect("one coupled point selection");

    assert_eq!(result.evidence().iterations(), 1);
    let terms = result.delta().expect("one coupled delta").terms();
    assert_eq!(
        terms.len(),
        2,
        "one spatial selection updates both Taylor coefficients"
    );
    assert_eq!(terms[0].cell().coefficient(), 0);
    assert_eq!(terms[1].cell().coefficient(), 1);
    assert_eq!(terms[0].cell().pixel(), terms[1].cell().pixel());

    let pixel = terms[0].cell().pixel();
    let residual_index = pixel[0] * normal.shape()[1] + pixel[1];
    let normal = normal
        .read_window(normal.slab().core_range())
        .expect("coupled Taylor window");
    let psf_peak_index = normal
        .normal_block(0, 0)
        .expect("principal normal block")
        .normal_approximation()
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.re.total_cmp(&right.re))
        .expect("nonempty normal block")
        .0;
    let h00 = normal
        .normal_block(0, 0)
        .expect("H00")
        .normal_approximation()[psf_peak_index]
        .re;
    let h01 = normal
        .normal_block(0, 1)
        .expect("H01 cross term")
        .normal_approximation()[psf_peak_index]
        .re;
    let h11 = normal
        .normal_block(1, 1)
        .expect("H11")
        .normal_approximation()[psf_peak_index]
        .re;
    assert_ne!(
        h01.to_bits(),
        0.0_f64.to_bits(),
        "fixture must exercise coupling"
    );
    let r0 = normal
        .coefficient_term(0)
        .expect("Taylor zero residual")
        .residual()[residual_index]
        .re;
    let r1 = normal
        .coefficient_term(1)
        .expect("Taylor one residual")
        .residual()[residual_index]
        .re;
    let determinant = h00 * h11 - h01 * h01;
    assert!(
        determinant.abs() > f64::EPSILON,
        "fixture Hessian must be invertible"
    );
    let expected0 = (h11 * r0 - h01 * r1) / determinant;
    let expected1 = (h00 * r1 - h01 * r0) / determinant;
    assert_close(
        terms[0].increment().value(),
        expected0,
        "Taylor coefficient zero",
    );
    assert_close(
        terms[1].increment().value(),
        expected1,
        "Taylor coefficient one",
    );
}

#[test]
fn t43_validity_charges_all_coefficients_and_rejects_the_expiring_selection_atomically() {
    let exact_problem = problem();
    let exact_program = point_program(&exact_problem, 1);
    let exact = run_point(exact_problem, exact_program, None);
    let charge = exact
        .delta()
        .expect("exact coupled delta")
        .terms()
        .iter()
        .map(|term| term.increment().value().abs())
        .sum::<f64>();
    assert_close(
        exact.evidence().total_flux(),
        charge,
        "coupled validity charge",
    );

    let bounded_problem = problem();
    let bounded_program = point_program(&bounded_problem, 8)
        .with_validity(MinorCycleValidity::Bounded {
            maximum_absolute_update: charge * 0.99,
        })
        .expect("positive Taylor validity envelope");
    let bounded = run_point(bounded_problem, bounded_program, None);
    assert_eq!(bounded.evidence().iterations(), 0);
    assert_eq!(bounded.evidence().total_flux().to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        bounded.evidence().stop_reason(),
        MinorCycleStopReason::StalenessBound
    );
    assert!(bounded.evidence().requests_reconciliation());
    assert!(
        bounded.delta().is_none(),
        "an expiring coupled selection is all-or-nothing"
    );
}

#[test]
fn t43_multiscale_reuses_canonical_scales_and_counts_one_coupled_selection() {
    let problem = problem_with_scales(vec![2.0, 0.0, 2.0]);
    let program = MinorCycleProgram::for_problem(&problem)
        .expect("coupled multiscale MT-MFS controls")
        .limit_iterations(1)
        .expect("one coupled iteration")
        .record_component_sequence(1)
        .expect("bounded component diagnostics");
    match program.algorithm() {
        ReconstructionAlgorithm::Mtmfs { scales_px, .. } => {
            assert_eq!(
                scales_px,
                &[0.0, 2.0],
                "MT-MFS uses the canonical scale set"
            )
        }
        algorithm => panic!("unexpected algorithm {algorithm:?}"),
    }

    let result = run_point(problem, program, Some([10, 8]));
    assert_eq!(result.evidence().iterations(), 1);
    let terms = result
        .delta()
        .expect("one coupled multiscale delta")
        .terms();
    assert!(terms.iter().any(|term| term.cell().coefficient() == 0));
    assert!(terms.iter().any(|term| term.cell().coefficient() == 1));
    let component = result
        .evidence()
        .recorded_component_sequence()
        .expect("recorded coupled selection")
        .first()
        .expect("one coupled selection");
    assert_eq!(
        component.scale_px(),
        2.0,
        "the fixture must exercise a nonzero MT-MFS scale"
    );
    let coefficient_zero_cells = terms
        .iter()
        .filter(|term| term.cell().coefficient() == 0)
        .count();
    assert!(
        coefficient_zero_cells > 1,
        "a nonzero scale spreads each Taylor coefficient spatially"
    );
}

#[test]
fn t43_point_scale_can_select_an_image_edge() {
    let problem = problem();
    let program = point_program(&problem, 1);
    let result = run_point(problem, program, Some([0, 0]));
    let delta = result.delta().expect("edge point component");
    assert!(
        delta
            .terms()
            .iter()
            .all(|term| term.cell().pixel() == [0, 0]),
        "the zero scale must not inherit the nonzero-scale border"
    );
}

#[test]
fn t43_oversized_scales_are_pruned_before_bias_and_hessian_setup() {
    let problem = problem_with_scales(vec![20.0, 0.0]);
    let program = MinorCycleProgram::for_problem(&problem)
        .expect("identity-bound MT-MFS program")
        .limit_iterations(1)
        .expect("one iteration")
        .record_component_sequence(2)
        .expect("bounded diagnostics");
    let result = run_point(problem, program, None);
    assert_eq!(
        result
            .evidence()
            .recorded_component_sequence()
            .expect("recorded point coefficient")[0]
            .scale_px(),
        0.0
    );
}
