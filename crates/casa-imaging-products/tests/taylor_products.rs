// SPDX-License-Identifier: LGPL-3.0-or-later

//! T44 acceptance contract for Taylor-family product construction.
//!
//! Products are generated from major-cycle completions assembled from
//! explicit synthetic pass planes (`common::synthetic_pass`), not gridded.

#[path = "taylor_products/blank.rs"]
mod blank;
mod common;
use common::observation::source;
use common::synthetic_pass::{Scene, two_cycle_round};
use common::{GeneratedMember, GeneratedProducts, ImageOutput, full_window};

use casa_imaging_deconvolution::MajorCycleCompletion;
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
    PlannedContinuumGeneration, ProductsError, RestoringBeam, fft_convolve, gaussian_beam_image,
    produce_continuum_members, rescale_residual_to_beam,
};

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
    taylor_problem_with_beam(
        seed,
        products,
        response,
        taylor_fraction,
        RestoringBeamPolicy::Common,
    )
}

fn taylor_problem_with_beam(
    seed: u8,
    products: &[ProductKind],
    response: InstrumentResponse,
    taylor_fraction: f32,
    restoring_beam: RestoringBeamPolicy,
) -> casa_imaging_model::CompiledProblem {
    taylor_problem_with_beam_terms(
        seed,
        products,
        response,
        taylor_fraction,
        restoring_beam,
        TERMS,
    )
}

fn taylor_problem_with_beam_terms(
    seed: u8,
    products: &[ProductKind],
    response: InstrumentResponse,
    taylor_fraction: f32,
    restoring_beam: RestoringBeamPolicy,
    terms: usize,
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
                ReconstructionBasis::Taylor { terms },
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
                restoring_beam,
                validity_with_taylor_fraction(taylor_fraction),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::UnorderedWithinBudget,
                FiniteValuePolicy::RejectUnflaggedInput,
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
    let output = ImageOutput::for_problem(problem);
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

/// CASA's principal solution (`SDAlgorithmMSMFS::restore`): the residuals
/// over the principal sum weight, solved with the PSF moments at the peak
/// divided by the zeroth moment's peak (`SIImageStore::normPSF`).
fn principal_residuals(join: &MajorCycleCompletion) -> [Vec<f32>; TERMS] {
    let normal = join.normal_state();
    let window = normal
        .read_window(normal.slab().core_range())
        .expect("coupled Taylor fixture window");
    let cells = SHAPE[0] * SHAPE[1];
    let moments = (0..3)
        .map(|term| window.normal_moment(term, 0).expect("Taylor normal moment"))
        .collect::<Vec<_>>();
    let peak = moments[0]
        .normal_approximation()
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.re.abs().total_cmp(&right.re.abs()))
        .expect("PSF peak")
        .0;
    let psf_peak = moments[0].normal_approximation()[peak].re;
    let sum_weight = moments[0].sum_weight();
    let h00 = moments[0].normal_approximation()[peak].re / psf_peak;
    let h01 = moments[1].normal_approximation()[peak].re / psf_peak;
    let h11 = moments[2].normal_approximation()[peak].re / psf_peak;
    let determinant = h00 * h11 - h01 * h01;
    assert!(determinant.is_finite() && determinant.abs() > f64::EPSILON);
    let residual0 = window
        .coefficient_term(0, 0)
        .expect("residual tt0")
        .residual();
    let residual1 = window
        .coefficient_term(1, 0)
        .expect("residual tt1")
        .residual();
    let mut principal = [vec![0.0; cells], vec![0.0; cells]];
    for index in 0..cells {
        let [r0, r1] = [residual0[index].re, residual1[index].re].map(|r| r / sum_weight);
        principal[0][index] = ((h11 * r0 - h01 * r1) / determinant) as f32;
        principal[1][index] = ((h00 * r1 - h01 * r0) / determinant) as f32;
    }
    principal
}

/// The restored images solve with the PSF moments over the zeroth
/// moment's peak, not its sum of weights (#704). A mosaic's PSF peaks below
/// its sum of weights; scaling the PSF moments alone, with the residuals
/// and sums unchanged, leaves `.image.tt*` CASA's principal solution, the
/// same as for a PSF that peaks at its sum.
#[test]
fn t44_the_principal_solution_divides_the_moments_by_the_psf_peak() {
    let problem = taylor_problem(205, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let images = [1.0, 0.5].map(|scale| {
        let scene = taylor_scene(&problem).with_psf_scale(0, scale);
        let join = two_cycle_round(&problem, &scene, Vec::new());
        let principal = principal_residuals(&join);
        let generated = generate(&problem, &join);
        for (term, principal) in principal.iter().enumerate() {
            let restored = member(&generated, &format!(".image.tt{term}")).payload();
            for (actual, expected) in restored.iter().zip(principal) {
                assert_close(*actual, *expected, "principal solution, empty model");
            }
        }
        (0..TERMS)
            .map(|term| {
                member(&generated, &format!(".image.tt{term}"))
                    .payload()
                    .to_vec()
            })
            .collect::<Vec<_>>()
    });
    for (term, (unit, half)) in images[0].iter().zip(&images[1]).enumerate() {
        for (unit, half) in unit.iter().zip(half) {
            assert_close(*half, *unit, &format!(".image.tt{term} at half the PSF"));
        }
    }
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
    let principal_weight = window
        .normal_moment(0, 0)
        .expect("moment zero")
        .sum_weight();
    let principal_peak = window
        .normal_moment(0, 0)
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
        let moment = window.normal_moment(term, 0).expect("normal moment");
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
            .coefficient_term(term, 0)
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
    let list = problem.product_list();
    let names = list
        .entries()
        .iter()
        .filter_map(|entry| entry.name())
        .collect::<Vec<_>>();
    assert!(names.contains(&".pb.tt0"));
    assert!(names.contains(&".pb.tt1"));
    assert!(names.contains(&".image.tt0.pbcor"));
    assert!(names.contains(&".image.tt1.pbcor"));
    assert!(!names.iter().any(|name| name.starts_with(".weight")));
    assert!(!names.contains(&".alpha.pbcor"));

    let pb0 = list
        .entry(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
        .expect("Taylor-zero PB");
    assert_eq!(pb0.unit(), ProductUnit::Dimensionless);
    assert_eq!(
        pb0.validity(),
        ProductValidityRule::PrimaryBeam(validity().primary_beam())
    );
    assert_eq!(
        list.entry(ProductRole::PrimaryBeam(ProductTerm::Taylor(1)))
            .expect("Taylor-one PB")
            .validity(),
        ProductValidityRule::All,
        "CASA masks only the principal primary-beam Taylor term",
    );
    for term in 0..TERMS {
        let restored = list
            .entry(ProductRole::RestoredImage(ProductTerm::Taylor(term)))
            .expect("restored Taylor term");
        let corrected = list
            .entry(ProductRole::PbCorrectedImage(ProductTerm::Taylor(term)))
            .expect("PB-corrected Taylor term");
        assert_eq!(corrected.unit(), ProductUnit::JyPerBeam);
        assert_eq!(
            corrected.validity(),
            ProductValidityRule::PrimaryBeam(validity().primary_beam())
        );
        assert_eq!(
            corrected.beam(),
            ProductBeamRule::Inherit(restored.id()),
            "every Taylor image correction keeps its restored term's beam"
        );
    }

    assert!(
        list.entries()
            .iter()
            .all(|node| !matches!(node.role(), ProductRole::Weight(ProductTerm::Taylor(_)))),
        "the frozen standard CASA row emits no standalone weight family"
    );

    let join = run_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join);
    assert!(matches!(
        PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
            .expect_err("requested PB needs a bound model at planning"),
        ProductsError::MissingPrimaryBeamModel
    ));
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
        .normal_moment(0, 0)
        .expect("principal normal moment")
        .sum_weight() as f32;
    let raw_sensitivity = window
        .normal_moment(0, 0)
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
        .normal_moment(1, 0)
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
        .product_list()
        .entries()
        .iter()
        .filter_map(|entry| entry.name())
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
        .demand(
            &inputs,
            full_window(&planned),
            &ImageOutput::for_problem(&problem),
        )
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
    // The family is built whole, then published member by member, so the
    // phase peaks at the larger stage (#738). Building peaks at the largest
    // of: the built family beside its principal residuals, PB0 source and
    // solve; the read family (3 PSF and 3 weight moments, 2 residual and 2
    // model terms, the sensitivity, 3 sum weights) and the 2 principal
    // residuals beside the beam fit, or beside restoration and the term
    // restored before the last. Publication holds the family, one member,
    // its window and the writer.
    let plane = maximum * 4;
    let solved = 11 * plane + 3 * 4 + 2 * plane;
    let building = taylor_workspace
        .max(solved + demand.beam_scratch_bytes())
        .max(solved + restoration_workspace + plane);
    let publishing = 5_516 + 2 * (maximum * 4 + maximum) + demand.writer_bytes();
    assert_eq!(
        demand.peak_residency_bytes(),
        demand.retained_metadata_bytes() + building.max(publishing)
    );
    assert!(
        demand.peak_residency_bytes()
            < demand.algorithm_scratch_bytes()
                + demand.retained_metadata_bytes()
                + demand.beam_scratch_bytes()
                + demand.writer_bytes(),
        "the stages do not all coexist"
    );
    let generated = produce_continuum_members(
        &planned,
        &inputs,
        full_window(&planned),
        &(),
        &ImageOutput::for_problem(&problem),
    )
    .unwrap();
    assert_eq!(
        demand.retained_metadata_bytes(),
        common::retained_metadata_bytes(&generated)
    );
}

/// The algorithm scratch a generation of `problem` is charged.
fn taylor_algorithm_scratch(
    problem: &casa_imaging_model::CompiledProblem,
    join: &MajorCycleCompletion,
) -> u64 {
    let inputs = ContinuumProductInputs::from_major_cycle(problem, join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("Taylor plan");
    planned
        .demand(
            &inputs,
            full_window(&planned),
            &ImageOutput::for_problem(problem),
        )
        .expect("Taylor demand")
        .algorithm_scratch_bytes()
}

/// A beam the request gives restores every Taylor term as
/// `SIImageStore::restore` restores each: the term's model convolved with
/// the given beam plus its principal residual rescaled from the PSF's
/// fitted beam (`rescaleResolution`). The images and the spectral index
/// record the given beam, the residuals and PSF the fitted one; the
/// rescaled residual costs one plane more than a common beam's restoration.
#[test]
fn t44_a_given_beam_restores_every_term_with_rescaled_principal_residuals() {
    let common = taylor_problem(213, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let common_join = run_round(&common);
    let fitted = *member(&generate(&common, &common_join), ".image.tt0")
        .resolved_beam()
        .expect("the fitted beam");
    let given = RestoringBeam::new(
        2.5 * fitted.major_fwhm_rad(),
        2.0 * fitted.major_fwhm_rad(),
        0.4,
    )
    .expect("a beam");
    let problem = taylor_problem_with_beam(
        213,
        &TAYLOR_PRODUCTS,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::Fixed(
            casa_imaging_model::FixedRestoringBeam::new(
                given.major_fwhm_rad(),
                given.minor_fwhm_rad(),
                given.position_angle_rad(),
            )
            .expect("a beam"),
        ),
    );
    let join = run_round(&problem);
    let generated = generate(&problem, &join);
    for name in [".image.tt0", ".image.tt1", ".alpha", ".alpha.error"] {
        assert_eq!(
            member(&generated, name).resolved_beam(),
            Some(&given),
            "{name}"
        );
    }
    for name in [".psf.tt0", ".residual.tt0", ".residual.tt1"] {
        assert_eq!(
            member(&generated, name).resolved_beam(),
            Some(&fitted),
            "{name}"
        );
    }
    let principal = principal_residuals(&join);
    let kernel = gaussian_beam_image(SHAPE, &given, [1.0e-6, 1.0e-6]);
    for (term, principal) in principal.iter().enumerate() {
        let scaling = rescale_residual_to_beam(principal, SHAPE, [1.0e-6; 2], fitted, given)
            .expect("a rescaled residual");
        assert!(scaling.applied(), "tt{term}");
        let convolved = fft_convolve(
            member(&generated, &format!(".model.tt{term}")).payload(),
            kernel.as_slice().expect("contiguous kernel"),
            SHAPE,
        );
        let restored = member(&generated, &format!(".image.tt{term}")).payload();
        for index in 0..convolved.len() {
            assert_close(
                restored[index],
                convolved[index] + scaling.values()[index],
                &format!(".image.tt{term}: conv(model, given) + rescaled principal residual"),
            );
        }
    }
    let plane = (SHAPE[0] * SHAPE[1] * std::mem::size_of::<f32>()) as u64;
    assert_eq!(
        taylor_algorithm_scratch(&problem, &join),
        taylor_algorithm_scratch(&common, &common_join) + plane
    );
}

/// An MVC Taylor family uses the weighted cube PB supplied by its run,
/// including its own cutoff, instead of recomputing one at the Taylor WCS.
#[test]
fn a_supplied_taylor_beam_controls_pb_and_pbcor_products() {
    let problem = taylor_problem(205, &PB_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round(&problem);
    let beam: Vec<f32> = (0..SHAPE[0] * SHAPE[1])
        .map(|index| [0.0, 0.15, 0.5, 0.9][index % 4])
        .collect();
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join)
        .with_taylor_primary_beam(&beam)
        .expect("one finite Taylor PB plane");
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let baseline = generate_with_controls(&problem, &join, controls);
    let planned = PlannedContinuumGeneration::new(&inputs, &controls).expect("Taylor plan");
    let output = ImageOutput::for_problem(&problem);
    let produced =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("Taylor products");
    let generated = GeneratedProducts::from_output(&planned, &produced, &output);
    for name in [".psf.tt0", ".residual.tt0", ".model.tt0", ".image.tt0"] {
        assert_eq!(
            member(&generated, name).payload(),
            member(&baseline, name).payload(),
            "{name}"
        );
    }
    let pb0 = member(&generated, ".pb.tt0");
    for (index, &value) in beam.iter().enumerate() {
        let valid = value > 0.2;
        assert_eq!(pb0.validity()[index], valid, "PB validity {index}");
        assert_eq!(
            pb0.payload()[index],
            value,
            "PB payload retains the supplied value below the mask cutoff at {index}"
        );
        for term in 0..TERMS {
            let restored = member(&generated, &format!(".image.tt{term}"));
            let corrected = member(&generated, &format!(".image.tt{term}.pbcor"));
            assert_eq!(
                corrected.validity()[index],
                valid,
                "PB correction validity {index}"
            );
            let expected = if valid {
                restored.payload()[index] / value
            } else {
                0.0
            };
            assert_close(corrected.payload()[index], expected, "PB correction");
        }
    }
    assert!(
        ContinuumProductInputs::from_major_cycle(&problem, &join)
            .with_taylor_primary_beam(&beam[..beam.len() - 1])
            .is_err()
    );
    let mut invalid = beam;
    invalid[0] = f32::NAN;
    assert!(
        ContinuumProductInputs::from_major_cycle(&problem, &join)
            .with_taylor_primary_beam(&invalid)
            .is_err()
    );
    invalid[0] = -0.1;
    assert!(
        ContinuumProductInputs::from_major_cycle(&problem, &join)
            .with_taylor_primary_beam(&invalid)
            .is_err()
    );
    let continuum = common::continuum::continuum_problem(
        206,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::Beam,
        ],
    );
    let scene = Scene::new(&continuum).with_point(0, CENTRE, &[1.0]);
    let continuum_join = two_cycle_round(&continuum, &scene, vec![]);
    assert!(
        ContinuumProductInputs::from_major_cycle(&continuum, &continuum_join)
            .with_taylor_primary_beam(&[1.0; SHAPE[0] * SHAPE[1]])
            .is_err()
    );
}

/// CASA's MVC PB is the sumweight-weighted mean of the published cube PB
/// planes. The final pixel has only one channel above its own PB cutoff.
#[test]
fn cube_primary_beams_reduce_to_casas_taylor_beam_without_a_cube_cache() {
    use casa_imaging_products::TaylorPrimaryBeamAccumulator;
    let planes = [
        [1.0, 0.0, 0.420_582_12, 0.101_429_46],
        [1.0, 0.0, 0.418_301_34, 0.0],
        [1.0, 0.0, 0.415_848_9, 0.0],
    ];
    let weights = [10_529.967, 7_880.975, 4_194.388];
    let mut output = [9.0; 4];
    let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut output).expect("four pixels");
    for (plane, weight) in planes.iter().zip(weights) {
        reduction.add(plane, weight).expect("finite channel");
    }
    // A channel with no imaging weight does not enter the mean.
    reduction.add(&[9.0; 4], 0.0).expect("zero-weight channel");
    let actual = reduction.finish().expect("positive total weight");
    for (index, (actual, expected)) in actual
        .iter()
        .zip([1.0_f32, 0.0, 0.418_908_75, 0.047_247_656])
        .enumerate()
    {
        assert!(
            (actual - expected).abs() < 2e-7,
            "pixel {index}: {actual} != {expected}"
        );
    }
}

/// Reject malformed input rather than publishing a partial or non-finite PB.
#[test]
fn cube_primary_beam_reduction_rejects_invalid_inputs() {
    use casa_imaging_products::TaylorPrimaryBeamAccumulator;
    assert!(TaylorPrimaryBeamAccumulator::new(&mut []).is_err());
    let rejects = |plane: &[f32], weight| {
        let mut pixels = [0.0; 2];
        let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut pixels).unwrap();
        reduction.add(plane, weight).is_err()
    };
    assert!(rejects(&[1.0], 1.0));
    assert!(rejects(&[1.0, f32::NAN], 1.0));
    assert!(rejects(&[1.0, -0.1], 1.0));
    assert!(rejects(&[1.0, 1.0], -1.0));
    assert!(rejects(&[1.0, 1.0], f32::NAN));
    assert!(rejects(&[f32::MAX, 1.0], 2.0));
    let mut empty = [0.0; 2];
    let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut empty).unwrap();
    reduction.add(&[1.0, 1.0], 0.0).unwrap();
    assert!(reduction.finish().is_err());
    let mut poisoned = [0.0; 2];
    let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut poisoned).unwrap();
    reduction.add(&[1.0, 1.0], 1.0).unwrap();
    assert!(reduction.add(&[1.0, f32::NAN], 1.0).is_err());
    assert!(reduction.add(&[1.0, 1.0], 1.0).is_err());
    assert!(reduction.finish().is_err());
    let mut bad_negative = [0.0; 2];
    let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut bad_negative).unwrap();
    reduction.add(&[1.0, 1.0], 1.0).unwrap();
    assert!(reduction.add(&[1.0, -0.1], 1.0).is_err());
    assert!(reduction.finish().is_err());
    let mut bad_shape = [0.0; 2];
    let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut bad_shape).unwrap();
    reduction.add(&[1.0, 1.0], 1.0).unwrap();
    assert!(reduction.add(&[1.0], 1.0).is_err());
    assert!(reduction.add(&[1.0, 1.0], 1.0).is_err());
    assert!(reduction.finish().is_err());
    let mut overflow = [0.0; 2];
    let mut reduction = TaylorPrimaryBeamAccumulator::new(&mut overflow).unwrap();
    reduction.add(&[0.0, 0.0], f32::MAX).unwrap();
    assert!(reduction.add(&[0.0, 0.0], f32::MAX).is_err());
    assert!(reduction.finish().is_err());
}

/// One run may stage a cube and a Taylor family with distinct spectral WCS,
/// then publish the complete staged set through either bound writer.
#[test]
fn two_product_families_share_staging_with_distinct_coordinates() {
    use casa_coordinates::{CoordinateType, SpectralCoordinate, defaults};
    use casa_imaging_products::CasaImageWriter;
    use casa_types::measures::frequency::FrequencyRef;

    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
    ];
    let cube =
        common::continuum::continuum_problem_with_policy(207, &products, RestoringBeamPolicy::None);
    let cube_scene = Scene::new(&cube).with_point(0, CENTRE, &[1.0]);
    let cube_join = two_cycle_round(&cube, &cube_scene, vec![]);
    let cube_inputs = ContinuumProductInputs::from_major_cycle(&cube, &cube_join);
    let controls = ContinuumProductControls::default();
    let cube_plan = PlannedContinuumGeneration::new(&cube_inputs, &controls).unwrap();
    let taylor = taylor_problem(208, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let taylor_join = run_round(&taylor);
    let taylor_inputs = ContinuumProductInputs::from_major_cycle(&taylor, &taylor_join);
    let taylor_plan = PlannedContinuumGeneration::new(&taylor_inputs, &controls).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("image");
    let coordinates = |reference| {
        let mut coordinates = defaults::coordinate_system_for_shape(&[8, 8, 1, 1], false);
        let axis = coordinates
            .find_coordinate(CoordinateType::Spectral)
            .unwrap();
        coordinates.replace_coordinate(
            axis,
            SpectralCoordinate::new(FrequencyRef::LSRK, reference, 1.0e6, 0.0, reference),
        );
        coordinates
    };
    let cube_writer = CasaImageWriter::new(base.clone(), coordinates(1.0e9));
    let taylor_writer =
        cube_writer.with_coordinates(vec![(ImageDomainRole::Main, coordinates(2.0e9))]);
    produce_continuum_members(
        &cube_plan,
        &cube_inputs,
        full_window(&cube_plan),
        &(),
        &cube_writer,
    )
    .unwrap();
    produce_continuum_members(
        &taylor_plan,
        &taylor_inputs,
        full_window(&taylor_plan),
        &(),
        &taylor_writer,
    )
    .unwrap();
    assert!(!base.with_extension("psf").exists());
    assert!(!directory.path().join("image.psf.tt0").exists());
    drop(cube_writer);
    let mut published = Vec::new();
    taylor_writer
        .publish(|path| published.push(path.to_path_buf()))
        .unwrap();
    assert_eq!(
        published.len(),
        cube_plan.members().len() + taylor_plan.members().len()
    );
    for (suffix, expected) in [(".psf", 1.0e9), (".psf.tt0", 2.0e9)] {
        let image =
            casa_images::PagedImage::<f32>::open(directory.path().join(format!("image{suffix}")))
                .unwrap();
        let coordinates = image.coordinates();
        let axis = coordinates
            .find_coordinate(CoordinateType::Spectral)
            .unwrap();
        assert_eq!(
            coordinates.coordinate(axis).reference_value(),
            vec![expected]
        );
    }

    // Cancelling before publication drops both bindings and discards all of
    // their staging, even though dropping just one must retain it.
    let abandoned = tempfile::tempdir().unwrap();
    let first = CasaImageWriter::new(abandoned.path().join("image"), coordinates(1.0e9));
    let second = first.with_coordinates(vec![(ImageDomainRole::Main, coordinates(2.0e9))]);
    produce_continuum_members(
        &cube_plan,
        &cube_inputs,
        full_window(&cube_plan),
        &(),
        &first,
    )
    .unwrap();
    drop(first);
    assert!(
        std::fs::read_dir(abandoned.path())
            .unwrap()
            .next()
            .is_some()
    );
    drop(second);
    assert!(
        std::fs::read_dir(abandoned.path())
            .unwrap()
            .next()
            .is_none()
    );
}

/// A reduced MVC Taylor family is encoded as one normal state and published
/// through the existing Taylor product path without changing its values.
#[test]
fn reduced_mvc_planes_round_trip_through_the_taylor_normal_state() {
    use casa_imaging_deconvolution::{
        MajorCycle, ModelLifecycle, ModelStoragePlan, NormalStoragePlan,
    };
    use casa_imaging_products::encode_mvc_taylor_planes;
    let kinds = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::TaylorTerms,
    ];
    let problem = taylor_problem_with_beam(
        216,
        &kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
    );
    let original = run_round_with_model(&problem, None);
    let controls = ContinuumProductControls::default();
    let baseline = generate_with_controls(&problem, &original, controls);
    let psf = (0..3)
        .flat_map(|term| {
            member(&baseline, &format!(".psf.tt{term}"))
                .payload()
                .to_vec()
        })
        .collect();
    let mut residual: Vec<f32> = (0..2)
        .flat_map(|term| {
            member(&baseline, &format!(".residual.tt{term}"))
                .payload()
                .to_vec()
        })
        .collect();
    // A subnormal CASA Float must survive the handoff, even where a
    // multiply/divide normalization round trip would erase it.
    let tiny = f32::from_bits(1);
    residual[0] = tiny;
    let sumwt =
        std::array::from_fn(|term| member(&baseline, &format!(".sumwt.tt{term}")).payload()[0]);
    let images = encode_mvc_taylor_planes(SHAPE, psf, residual, sumwt).unwrap();
    let lifecycle = ModelLifecycle::new(&problem, ModelStoragePlan::resident(usize::MAX).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let mut cycle =
        MajorCycle::initial(&problem, model, NormalStoragePlan::resident(1).unwrap()).unwrap();
    cycle.parts().1.append(images).unwrap();
    let completion = cycle.finish_read().unwrap();
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &completion)
        .with_taylor_residual_sum_weight(1.0)
        .unwrap();
    let plan = PlannedContinuumGeneration::new(&inputs, &controls).unwrap();
    let output = ImageOutput::for_problem(&problem);
    let produced =
        produce_continuum_members(&plan, &inputs, full_window(&plan), &(), &output).unwrap();
    let rebuilt = GeneratedProducts::from_output(&plan, &produced, &output);
    for name in [
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".residual.tt0",
        ".residual.tt1",
        ".sumwt.tt0",
        ".sumwt.tt1",
        ".sumwt.tt2",
    ] {
        for (index, (actual, expected)) in member(&rebuilt, name)
            .payload()
            .iter()
            .zip(member(&baseline, name).payload())
            .enumerate()
        {
            if name == ".residual.tt0" && index == 0 {
                assert_eq!(actual.to_bits(), tiny.to_bits(), "subnormal residual");
            } else {
                assert!(
                    (actual - expected).abs() < 1e-6,
                    "{name}: {actual} != {expected}"
                );
            }
        }
    }
}

/// Drive cube normal state → bounded MVC reduction → Taylor normal state →
/// actual Taylor image publication with the weighted common PB.
#[test]
fn cube_normals_reduce_and_publish_as_taylor_products() {
    use casa_imaging_deconvolution::{
        MajorCycle, ModelLifecycle, ModelStoragePlan, NormalStoragePlan,
    };
    use casa_imaging_products::{encode_mvc_taylor_planes, reduce_mvc_cube};
    let cube_kinds = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let cube = common::continuum::continuum_problem_with_reconstruction(
        217,
        &cube_kinds,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Dirty,
        3,
    );
    let scene = Scene::new(&cube).with_point(0, CENTRE, &[1.0, 1.0, 1.0]);
    let cube_completion = two_cycle_round(&cube, &scene, vec![]);
    let cube_inputs = ContinuumProductInputs::from_major_cycle(&cube, &cube_completion);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let cube_plan = PlannedContinuumGeneration::new(&cube_inputs, &controls).unwrap();
    let cells = SHAPE[0] * SHAPE[1];
    let mut mean = vec![0.0; cells];
    let mut psf = vec![0.0; 3 * cells];
    let mut residual = vec![0.0; 2 * cells];
    let mut sumwt = [0.0; 3];
    reduce_mvc_cube(
        &cube_plan,
        &cube_inputs,
        1.4e9,
        &mut mean,
        &mut psf,
        &mut residual,
        &mut sumwt,
    )
    .unwrap();
    let expected_psf = psf.clone();
    let expected_residual = residual.clone();
    let images = encode_mvc_taylor_planes(SHAPE, psf, residual, sumwt).unwrap();
    let kinds = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
        ProductKind::TaylorTerms,
    ];
    let taylor = taylor_problem_with_beam(
        218,
        &kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
    );
    let lifecycle = ModelLifecycle::new(&taylor, ModelStoragePlan::resident(usize::MAX).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let mut cycle =
        MajorCycle::initial(&taylor, model, NormalStoragePlan::resident(1).unwrap()).unwrap();
    cycle.parts().1.append(images).unwrap();
    let completion = cycle.finish_read().unwrap();
    let inputs = ContinuumProductInputs::from_major_cycle(&taylor, &completion)
        .with_taylor_primary_beam(&mean)
        .unwrap()
        .with_taylor_residual_sum_weight(1.0)
        .unwrap();
    let planned = PlannedContinuumGeneration::new(&inputs, &controls).unwrap();
    let output = ImageOutput::for_problem(&taylor);
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output).unwrap();
    let actual = GeneratedProducts::from_output(&planned, &generated, &output);
    for term in 0..3 {
        for (pixel, &value) in member(&actual, &format!(".psf.tt{term}"))
            .payload()
            .iter()
            .enumerate()
        {
            assert!(
                (value - expected_psf[term * cells + pixel]).abs() < 1e-6,
                "PSF {term}/{pixel}"
            );
        }
        assert!(
            (member(&actual, &format!(".sumwt.tt{term}")).payload()[0] - sumwt[term]).abs() < 1e-6
        );
        if term < 2 {
            for (pixel, &value) in member(&actual, &format!(".residual.tt{term}"))
                .payload()
                .iter()
                .enumerate()
            {
                assert!(
                    (value - expected_residual[term * cells + pixel]).abs() < 1e-6,
                    "residual {term}/{pixel}"
                );
            }
        }
    }
    assert_eq!(member(&actual, ".pb.tt0").payload(), mean);
}

#[test]
fn supplied_taylor_residual_divisor_changes_only_residual_normalization() {
    let kinds = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::TaylorTerms,
    ];
    let problem = taylor_problem_with_beam(
        219,
        &kinds,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::None,
    );
    let completion = run_round_with_model(&problem, None);
    let controls = ContinuumProductControls::default();
    let baseline = generate_with_controls(&problem, &completion, controls);
    let weight = f64::from(member(&baseline, ".sumwt.tt0").payload()[0]);
    for divisor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(
            ContinuumProductInputs::from_major_cycle(&problem, &completion)
                .with_taylor_residual_sum_weight(divisor)
                .is_err()
        );
    }
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &completion)
        .with_taylor_residual_sum_weight(2.0 * weight)
        .unwrap();
    let plan = PlannedContinuumGeneration::new(&inputs, &controls).unwrap();
    let output = ImageOutput::for_problem(&problem);
    let produced =
        produce_continuum_members(&plan, &inputs, full_window(&plan), &(), &output).unwrap();
    let actual = GeneratedProducts::from_output(&plan, &produced, &output);
    for term in 0..3 {
        for name in [format!(".psf.tt{term}"), format!(".sumwt.tt{term}")] {
            assert_eq!(
                member(&actual, &name).payload(),
                member(&baseline, &name).payload(),
                "{name}"
            );
        }
        if term < 2 {
            let name = format!(".residual.tt{term}");
            for (actual, expected) in member(&actual, &name)
                .payload()
                .iter()
                .zip(member(&baseline, &name).payload())
            {
                assert!((actual - expected * 0.5).abs() < 1e-6, "{name}");
            }
        }
    }
    let continuum = common::continuum::continuum_problem_with_policy(
        220,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::SumWeights,
        ],
        RestoringBeamPolicy::None,
    );
    let scene = Scene::new(&continuum).with_point(0, CENTRE, &[1.0]);
    let completion = two_cycle_round(&continuum, &scene, vec![]);
    assert!(
        ContinuumProductInputs::from_major_cycle(&continuum, &completion)
            .with_taylor_residual_sum_weight(1.0)
            .is_err()
    );
}
