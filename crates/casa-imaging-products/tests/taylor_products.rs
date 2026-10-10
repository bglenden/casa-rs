// SPDX-License-Identifier: LGPL-3.0-or-later

//! T44 acceptance contract for Taylor-family product construction.
//!
//! Products are generated from major-cycle completions assembled from
//! explicit synthetic pass planes (`common::synthetic_pass`), not gridded.

mod common;
use common::observation::source;
use common::synthetic_pass::{Scene, two_cycle_round};
use common::{GeneratedMember, GeneratedProducts, MemoryProductOutput, full_window};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, InstrumentResponse, MeasurementEquationContract,
    ModelBounds, ModelCell, ModelColumnWrite, ModelDeltaTerm, ModelInnerProduct,
    ModelLifecycleRequirements, ModelValue, NumericPrecision, NumericalStage, NumericsContract,
    ObservationSnapshotInput, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, ProblemInput,
    ProblemSpecification, ProductBeamRule, ProductKind, ProductNormalization, ProductRequirements,
    ProductRole, ProductTerm, ProductUnit, ProductValidityPolicies, ProductValidityRule,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy,
    ScientificContract, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, StageErrorBudget, UvwCoordinateLaw,
    VisibilityInnerProduct, WeightDensityScope, WeightingContract, WeightingScheme, compile,
    compile_observation,
};
use casa_imaging_products::{
    AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs,
    PlannedContinuumGeneration, ProductsError, fft_convolve, gaussian_beam_image,
    produce_continuum_members,
};
use casa_imaging_reconstruction::MajorCycleCompletion;

const SHAPE: [usize; 2] = [8, 8];
const TERMS: usize = 2;
const TAYLOR_PRODUCTS: [ProductKind; 9] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::TaylorTerms,
    ProductKind::SpectralIndex,
    ProductKind::SpectralIndexError,
    ProductKind::Beam,
];

const PB_PRODUCTS: [ProductKind; 9] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::PrimaryBeam,
    ProductKind::PbCorrectedImage,
    ProductKind::TaylorTerms,
    ProductKind::Beam,
];

fn validity() -> ProductValidityPolicies {
    validity_with_taylor_fraction(0.1)
}

fn validity_with_taylor_fraction(taylor_fraction: f32) -> ProductValidityPolicies {
    common::observation::validity(taylor_fraction)
}

fn taylor_problem(
    seed: u8,
    products: &[ProductKind],
    response: InstrumentResponse,
) -> casa_imaging_model::CompiledProblem {
    taylor_problem_with_fraction(seed, products, response, 0.1)
}

fn taylor_problem_with_fraction(
    seed: u8,
    products: &[ProductKind],
    response: InstrumentResponse,
    taylor_fraction: f32,
) -> casa_imaging_model::CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [(SHAPE[0] / 2) as f64, (SHAPE[1] / 2) as f64],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
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
                reference_frequency_hz: 1.1e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source(seed, "t44")]))
        .expect("observation snapshot");
    compile(ProblemInput::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(
                    SpectralSamplingLaw::IDENTITY,
                    SpectralCoupling::CommonRestoringBeam,
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
                ReconstructionBasis::Taylor { terms: TERMS },
                ReconstructionAlgorithm::Mtmfs {
                    scales_px: vec![0.0],
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(1, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                products.to_vec(),
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::Common,
                validity_with_taylor_fraction(taylor_fraction),
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
        snapshot,
        ModelLifecycleRequirements::new(
            ModelBounds::new(4_096, 4_096, 1.0e30, 1.0e30).expect("model bounds"),
            NumericPrecision::F64,
        ),
    ))
    .expect("compile Taylor problem")
}

/// The pixel of the Taylor fixture's source and model: the image centre,
/// where the PSF moments peak.
const CENTRE: [usize; 2] = [SHAPE[0] / 2, SHAPE[1] / 2];

/// A point source of unit Taylor-zero amplitude and spectral index −0.5 at
/// the image centre, imaged through the moments of the two spectral samples
/// (principal `sumwt = 3`).
fn taylor_scene(problem: &casa_imaging_model::CompiledProblem) -> Scene {
    Scene::new(problem).with_point(0, CENTRE, &[1.0, -0.5])
}

fn run_round(problem: &casa_imaging_model::CompiledProblem) -> MajorCycleCompletion {
    run_round_with_model(problem, Some(0.75))
}

fn run_round_with_model(
    problem: &casa_imaging_model::CompiledProblem,
    model_value: Option<f64>,
) -> MajorCycleCompletion {
    let terms = model_value
        .into_iter()
        .map(|value| (0, value))
        .collect::<Vec<_>>();
    run_round_with_terms(problem, &terms)
}

/// The initial major cycle and, when `model_terms` names any
/// `(coefficient, value)` at the centre, a residual refresh after them.
fn run_round_with_terms(
    problem: &casa_imaging_model::CompiledProblem,
    model_terms: &[(usize, f64)],
) -> MajorCycleCompletion {
    two_cycle_round(
        problem,
        &taylor_scene(problem),
        model_terms
            .iter()
            .map(|(coefficient, value)| {
                ModelDeltaTerm::new(
                    ModelCell::new(0, *coefficient, 0, CENTRE),
                    ModelValue::new(*value).expect("model value"),
                )
            })
            .collect(),
    )
}

fn generate(
    problem: &casa_imaging_model::CompiledProblem,
    join: &MajorCycleCompletion,
) -> GeneratedProducts {
    generate_with_controls(problem, join, ContinuumProductControls::default())
}

fn generate_with_controls(
    problem: &casa_imaging_model::CompiledProblem,
    join: &MajorCycleCompletion,
    controls: ContinuumProductControls,
) -> GeneratedProducts {
    let inputs = ContinuumProductInputs::from_major_cycle(problem, join);
    let planned = PlannedContinuumGeneration::new(&inputs, &controls).expect("T44 Taylor plan");
    let output = MemoryProductOutput::default();
    let produced =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("T44 Taylor product family");
    GeneratedProducts::from_output(&planned, &produced, &output)
}

fn member<'a>(generated: &'a GeneratedProducts, name: &str) -> &'a GeneratedMember {
    generated
        .members()
        .iter()
        .find(|member| member.name() == name)
        .unwrap_or_else(|| panic!("missing {name}"))
}

fn assert_close(actual: f32, expected: f32, context: &str) {
    let tolerance = 2.0e-5_f32.max(expected.abs() * 2.0e-5);
    assert!(
        (actual - expected).abs() <= tolerance,
        "{context}: {actual} != {expected}"
    );
}

fn principal_residuals(join: &MajorCycleCompletion) -> [Vec<f32>; TERMS] {
    let normal = join.normal_state();
    let window = normal
        .read_window(normal.slab().core_range())
        .expect("coupled Taylor fixture window");
    let cells = SHAPE[0] * SHAPE[1];
    let moments = (0..3)
        .map(|term| window.normal_moment(term).expect("Taylor normal moment"))
        .collect::<Vec<_>>();
    let peak = moments[0]
        .normal_approximation()
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.re.abs().total_cmp(&right.re.abs()))
        .expect("PSF peak")
        .0;
    let h00 = moments[0].normal_approximation()[peak].re;
    let h01 = moments[1].normal_approximation()[peak].re;
    let h11 = moments[2].normal_approximation()[peak].re;
    let determinant = h00 * h11 - h01 * h01;
    assert!(determinant.is_finite() && determinant.abs() > f64::EPSILON);
    let residual0 = window.coefficient_term(0).expect("residual tt0").residual();
    let residual1 = window.coefficient_term(1).expect("residual tt1").residual();
    let mut principal = [vec![0.0; cells], vec![0.0; cells]];
    for index in 0..cells {
        principal[0][index] =
            ((h11 * residual0[index].re - h01 * residual1[index].re) / determinant) as f32;
        principal[1][index] =
            ((h00 * residual1[index].re - h01 * residual0[index].re) / determinant) as f32;
    }
    principal
}

#[test]
fn t44_taylor_families_preserve_raw_state_and_share_one_restoring_beam() {
    let problem = taylor_problem(201, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round(&problem);
    let generated = generate(&problem, &join);
    let names = generated
        .members()
        .iter()
        .map(GeneratedMember::name)
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual.tt0",
            ".residual.tt1",
            ".model.tt0",
            ".model.tt1",
            ".image.tt0",
            ".image.tt1",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
            ".alpha",
            ".alpha.error",
        ]
    );

    let normal = join.normal_state();
    let window = normal
        .read_window(normal.slab().core_range())
        .expect("coupled Taylor fixture window");
    let principal_weight = window.normal_moment(0).expect("moment zero").sum_weight();
    let principal_peak = window
        .normal_moment(0)
        .unwrap()
        .normal_approximation()
        .iter()
        .map(|value| value.re as f32 / principal_weight as f32)
        .fold(0.0_f32, f32::max);
    for term in 0..3 {
        let psf = member(&generated, &format!(".psf.tt{term}"));
        let sumwt = member(&generated, &format!(".sumwt.tt{term}"));
        assert_eq!(psf.contract().unit(), ProductUnit::JyPerBeam);
        assert_eq!(sumwt.contract().unit(), ProductUnit::VisibilityWeight);
        let moment = window.normal_moment(term).expect("normal moment");
        assert_eq!(sumwt.payload(), &[moment.sum_weight() as f32]);
        for (actual, raw) in psf.payload().iter().zip(moment.normal_approximation()) {
            assert_eq!(
                *actual,
                (raw.re as f32 / principal_weight as f32) / principal_peak,
                "shared principal Taylor PSF normalization"
            );
        }
    }
    assert_eq!(
        member(&generated, ".psf.tt0")
            .payload()
            .iter()
            .copied()
            .fold(0.0_f32, f32::max),
        1.0
    );
    for term in 0..TERMS {
        let raw = window
            .coefficient_term(term)
            .expect("raw Taylor residual")
            .residual();
        let residual = member(&generated, &format!(".residual.tt{term}"));
        let model = member(&generated, &format!(".model.tt{term}"));
        let restored = member(&generated, &format!(".image.tt{term}"));
        assert_eq!(residual.contract().unit(), ProductUnit::JyPerBeam);
        assert_eq!(model.contract().unit(), ProductUnit::JyPerPixel);
        assert_eq!(restored.contract().unit(), ProductUnit::JyPerBeam);
        for (actual, raw) in residual.payload().iter().zip(raw) {
            assert_close(
                *actual,
                (raw.re / principal_weight) as f32,
                "published raw Taylor residual",
            );
        }
        assert_eq!(
            restored.resolved_beam(),
            member(&generated, ".image.tt0").resolved_beam(),
            "every Taylor image must use the same common beam"
        );
    }

    let principal = principal_residuals(&join);
    let beam = member(&generated, ".image.tt0")
        .resolved_beam()
        .expect("common restoring beam");
    let kernel = gaussian_beam_image(SHAPE, beam, [1.0e-6, 1.0e-6]);
    for (term, principal_term) in principal.iter().enumerate().take(TERMS) {
        let model = member(&generated, &format!(".model.tt{term}"));
        let restored = member(&generated, &format!(".image.tt{term}"));
        let convolved = fft_convolve(
            model.payload(),
            kernel.as_slice().expect("contiguous kernel"),
            SHAPE,
        );
        for index in 0..convolved.len() {
            assert_close(
                restored.payload()[index],
                convolved[index] + principal_term[index],
                "principal-solution restoration",
            );
        }
    }
}

#[test]
fn t44_alpha_and_error_use_strict_principal_support_and_zero_false_blanking() {
    let problem = taylor_problem(203, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round_with_model(&problem, None);
    let principal = principal_residuals(&join);
    let generated = generate(&problem, &join);
    let image0 = member(&generated, ".image.tt0");
    let image1 = member(&generated, ".image.tt1");
    let alpha = member(&generated, ".alpha");
    let error = member(&generated, ".alpha.error");
    assert_eq!(alpha.contract().unit(), ProductUnit::Dimensionless);
    assert_eq!(error.contract().unit(), ProductUnit::Dimensionless);
    let positive_max = principal[0]
        .iter()
        .copied()
        .filter(|value| *value > 0.0 && value.is_finite())
        .fold(f32::NEG_INFINITY, f32::max);
    assert!(positive_max.is_finite());
    let floor = 0.1 * positive_max;
    let mut supported = 0;
    for (index, image0_value) in image0.payload().iter().copied().enumerate() {
        let valid = image0_value > floor;
        assert_eq!(alpha.validity()[index], valid);
        assert_eq!(error.validity()[index], valid);
        if valid {
            supported += 1;
            let i0 = image0.payload()[index];
            let i1 = image1.payload()[index];
            let r0 = principal[0][index];
            let r1 = principal[1][index];
            assert_close(alpha.payload()[index], i1 / i0, "spectral index");
            let expected = ((i1 * r0 / i0.powi(2)).powi(2) + (r1 / i0).powi(2)).sqrt();
            assert_close(error.payload()[index], expected, "spectral-index error");
            assert!(error.payload()[index].is_finite());
        } else {
            assert_eq!(alpha.payload()[index], 0.0);
            assert_eq!(error.payload()[index], 0.0);
        }
    }
    assert!(supported > 0, "fixture must exercise Taylor support");

    let strict_problem =
        taylor_problem_with_fraction(207, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar, 1.0);
    let strict_join = run_round_with_model(&strict_problem, None);
    let strict = generate(&strict_problem, &strict_join);
    for name in [".alpha", ".alpha.error"] {
        let product = member(&strict, name);
        assert!(product.validity().iter().all(|valid| !valid));
        assert!(product.payload().iter().all(|value| *value == 0.0));
    }
}

#[test]
fn t44_standard_pb_family_uses_pb_tt0_and_does_not_invent_weight_or_alpha_pbcor() {
    let problem = taylor_problem(205, &PB_PRODUCTS, InstrumentResponse::Scalar);
    let graph = problem.product_graph();
    let names = graph
        .nodes()
        .iter()
        .filter_map(|node| node.name())
        .collect::<Vec<_>>();
    assert!(names.contains(&".pb.tt0"));
    assert!(names.contains(&".pb.tt1"));
    assert!(names.contains(&".image.tt0.pbcor"));
    assert!(names.contains(&".image.tt1.pbcor"));
    assert!(!names.iter().any(|name| name.starts_with(".weight")));
    assert!(!names.contains(&".alpha.pbcor"));

    let pb0 = graph
        .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
        .expect("Taylor-zero PB");
    assert_eq!(pb0.unit(), ProductUnit::Dimensionless);
    assert_eq!(
        pb0.validity(),
        ProductValidityRule::PrimaryBeam(validity().primary_beam())
    );
    assert_eq!(
        graph
            .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(1)))
            .expect("Taylor-one PB")
            .validity(),
        ProductValidityRule::All,
        "CASA masks only the principal primary-beam Taylor term",
    );
    for term in 0..TERMS {
        let restored = graph
            .node(ProductRole::RestoredImage(ProductTerm::Taylor(term)))
            .expect("restored Taylor term");
        let corrected = graph
            .node(ProductRole::PbCorrectedImage(ProductTerm::Taylor(term)))
            .expect("PB-corrected Taylor term");
        assert_eq!(corrected.unit(), ProductUnit::JyPerBeam);
        assert_eq!(
            corrected.validity(),
            ProductValidityRule::PrimaryBeam(validity().primary_beam())
        );
        assert_eq!(
            corrected.beam(),
            ProductBeamRule::Inherit(restored.node_id()),
            "every Taylor image correction keeps its restored term's beam"
        );
    }

    assert!(
        graph
            .nodes()
            .iter()
            .all(|node| !matches!(node.role(), ProductRole::Weight(ProductTerm::Taylor(_)))),
        "the frozen standard CASA row emits no standalone weight family"
    );

    let join = run_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join);
    assert_eq!(
        PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
            .expect_err("requested PB needs a bound model at planning"),
        ProductsError::UnsupportedProblem
    );
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let planned = PlannedContinuumGeneration::new(&inputs, &controls).expect("analytic PB plan");
    let alternate = PlannedContinuumGeneration::new(
        &inputs,
        &ContinuumProductControls::default()
            .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand),
    )
    .expect("alternate analytic PB plan");
    assert_ne!(planned.primary_beam_model(), alternate.primary_beam_model());
    assert_eq!(
        planned.primary_beam_model(),
        Some(AnalyticPrimaryBeamModel::CasaEvlaCommon)
    );
    let generated = generate_with_controls(&problem, &join, controls);
    let pb0 = member(&generated, ".pb.tt0");
    let pb1 = member(&generated, ".pb.tt1");
    assert_eq!(pb0.payload()[4 * SHAPE[1] + 4], 1.0);
    assert!(pb1.payload().iter().all(|value| *value == 0.0));
    assert!(pb1.validity().iter().all(|valid| *valid));
    for term in 0..TERMS {
        let restored = member(&generated, &format!(".image.tt{term}"));
        let corrected = member(&generated, &format!(".image.tt{term}.pbcor"));
        for index in 0..pb0.payload().len() {
            let valid = pb0.payload()[index] > 0.2;
            assert_eq!(corrected.validity()[index], valid);
            let expected = if valid {
                restored.payload()[index] / pb0.payload()[index]
            } else {
                0.0
            };
            assert_close(corrected.payload()[index], expected, "PB correction");
        }
    }
}

#[test]
fn t47_mosaic_taylor_products_publish_weight_and_pb_corrected_alpha() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::Weight,
        ProductKind::Sensitivity,
        ProductKind::PrimaryBeam,
        ProductKind::PbCorrectedImage,
        ProductKind::TaylorTerms,
        ProductKind::SpectralIndex,
        ProductKind::SpectralIndexError,
        ProductKind::PbCorrectedSpectralIndex,
        ProductKind::Beam,
    ];
    let problem = taylor_problem(211, &products, InstrumentResponse::Scalar);
    let join = run_round(&problem);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::MosaicSensitivity);
    let generated = generate_with_controls(&problem, &join, controls);
    let weight0 = member(&generated, ".weight.tt0");
    let weight1 = member(&generated, ".weight.tt1");
    let sensitivity = member(&generated, ".sensitivity");
    let alpha = member(&generated, ".alpha");
    let alpha_pbcor = member(&generated, ".alpha.pbcor");

    assert!(weight0.payload().iter().any(|value| *value > 0.0));
    let normal = join.normal_state();
    let window = normal
        .read_window(normal.slab().core_range())
        .expect("coupled Taylor fixture window");
    let principal_sum_weight = window
        .normal_moment(0)
        .expect("principal normal moment")
        .sum_weight() as f32;
    let raw_sensitivity = window
        .normal_moment(0)
        .expect("principal normal moment")
        .sensitivity();
    for (index, raw) in raw_sensitivity.iter().copied().enumerate() {
        assert_eq!(sensitivity.payload()[index], raw as f32);
        assert_close(
            weight0.payload()[index],
            sensitivity.payload()[index] / principal_sum_weight,
            "normalized principal mosaic weight",
        );
    }
    assert_ne!(
        weight0.payload(),
        sensitivity.payload(),
        "normalized Weight must not alias raw Sensitivity"
    );
    let raw_weight1 = window
        .normal_moment(1)
        .expect("first signed normal moment")
        .sensitivity();
    assert_eq!(
        weight1.payload(),
        raw_weight1
            .iter()
            .map(|value| *value as f32)
            .collect::<Vec<_>>(),
        "higher Taylor weights retain CASA's raw signed sensitivity moments"
    );
    for index in 0..alpha_pbcor.payload().len() {
        let expected = if alpha_pbcor.validity()[index] {
            // This scalar-response fixture has no PB spectral slope. The
            // non-zero spectral-slope law is covered by the product-owner unit
            // test rather than by cancelling two tt0-PB-corrected images.
            alpha.payload()[index]
        } else {
            0.0
        };
        assert_close(
            alpha_pbcor.payload()[index],
            expected,
            "PB-corrected spectral index",
        );
    }
}

#[test]
fn t51_weight_derived_mtmfs_plan_matches_casa_eighteen_member_inventory() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::Weight,
        ProductKind::PrimaryBeam,
        ProductKind::TaylorTerms,
        ProductKind::SpectralIndex,
        ProductKind::SpectralIndexError,
        ProductKind::Beam,
    ];
    let problem = taylor_problem(213, &products, InstrumentResponse::Scalar);
    let names = problem
        .product_graph()
        .nodes()
        .iter()
        .filter_map(|node| node.name())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        names,
        std::collections::BTreeSet::from([
            ".alpha",
            ".alpha.error",
            ".image.tt0",
            ".image.tt1",
            ".model.tt0",
            ".model.tt1",
            ".pb.tt0",
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual.tt0",
            ".residual.tt1",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
            ".weight.tt0",
            ".weight.tt1",
            ".weight.tt2",
        ])
    );
}

#[test]
fn taylor_generation_demand_charges_retained_families_and_algorithm_scratch() {
    let problem = taylor_problem(209, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("Taylor plan");
    let demand = planned
        .demand(&inputs, full_window(&planned))
        .expect("Taylor demand");
    let maximum = planned
        .members()
        .iter()
        .map(|member| member.payload_values() as u64)
        .max()
        .expect("Taylor members");
    assert_eq!(demand.maximum_member_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_member_validity_bytes(), maximum);
    assert_eq!(demand.maximum_window_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_window_validity_bytes(), maximum);
    // Two-term 8x8 fixture: retained families, residual/PB planes, normal solve.
    let taylor_workspace = 5_516 + 768 + 128;
    let restoration_workspace = maximum * (4 + 2 * 16 + 4)
        + (maximum + 64) * 16
        + maximum * std::mem::size_of::<usize>() as u64;
    assert_eq!(
        demand.algorithm_scratch_bytes(),
        taylor_workspace + restoration_workspace + maximum * 10,
        "coupled Taylor scratch additionally overlaps its emitted member and bounded backing-write window"
    );
    assert_eq!(
        demand.peak_residency_bytes(),
        demand.algorithm_scratch_bytes()
            + demand.retained_metadata_bytes()
            + demand.beam_scratch_bytes()
    );
    let generated = produce_continuum_members(
        &planned,
        &inputs,
        full_window(&planned),
        &(),
        &MemoryProductOutput::default(),
    )
    .unwrap();
    assert_eq!(
        demand.retained_metadata_bytes(),
        common::retained_metadata_bytes(&generated)
    );
}
