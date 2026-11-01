// SPDX-License-Identifier: LGPL-3.0-or-later

//! T22 continuum product algorithms, written as CASA images and read back.
//!
//! Products are generated from major-cycle completions assembled from
//! explicit synthetic pass planes (`common::synthetic_pass`), not gridded.

use std::mem::size_of;

mod common;
use common::continuum::{
    SHAPE, TWO_DOMAIN_PRODUCTS, axes, continuum_problem,
    continuum_problem_with_domains_and_reconstruction, continuum_problem_with_policy,
    continuum_problem_with_reconstruction, two_domain_problem,
};
use common::synthetic_pass::{Scene, continue_round, two_cycle_round};
use common::{GeneratedProducts, ImageOutput, full_window};

use casa_images::PagedImage;
use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, MaskBox, ReconstructionMask,
    ReconstructionMaskPlan,
};
use casa_imaging_model::{
    CompiledProblem, FacetLayout, ImageDomainRole, ImageDomainSpec, ImageShape, InstrumentResponse,
    ModelCell, ModelDeltaTerm, ModelValue, ProductKind, ProductRole, ReconstructionAlgorithm,
    ReconstructionBasis, RestoringBeamPolicy,
};
use casa_imaging_products::{
    AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs,
    PlannedContinuumGeneration, ProductStoragePlan, ProductsError, gaussian_beam_image,
    produce_continuum_members,
};
use casa_types::Value;

/// One fixture round: two major cycles, the initial one over the empty model
/// forming the PSF and a refresh after a nonzero model delta.
struct ContinuumRound {
    join: MajorCycleCompletion,
}

/// The main-domain centre, where the PSF peaks and the fixture source lies.
const CENTRE: [usize; 2] = [SHAPE[0] / 2, SHAPE[1] / 2];

/// A unit point source on the main-domain centre in every model plane;
/// the round's model delta subtracts 0.75 of it from the first plane.
fn continuum_scene(problem: &CompiledProblem) -> Scene {
    let scene = Scene::new(problem);
    let amplitudes = vec![1.0; scene.planes()];
    scene.with_point(0, CENTRE, &amplitudes)
}

fn model_term(domain: usize, pixel: [usize; 2], value: f64) -> ModelDeltaTerm {
    ModelDeltaTerm::new(
        ModelCell::new(domain, 0, 0, pixel),
        ModelValue::new(value).expect("finite model value"),
    )
}

fn run_continuum_round(problem: &CompiledProblem) -> ContinuumRound {
    ContinuumRound {
        join: two_cycle_round(
            problem,
            &continuum_scene(problem),
            vec![model_term(0, CENTRE, 0.75)],
        ),
    }
}

/// Where the two-domain round puts 0.25 of model on the outlier.
const OUTLIER_SOURCE: [usize; 2] = [2, 1];

/// The continuum scene on both domains; the 6×4 outlier's PSF fit window is
/// wider than its shorter axis, which CASA's beam fit clips per axis.
fn two_domain_scene(problem: &CompiledProblem) -> Scene {
    continuum_scene(problem)
}

fn run_two_domain_round(problem: &CompiledProblem) -> ContinuumRound {
    ContinuumRound {
        join: two_cycle_round(
            problem,
            &two_domain_scene(problem),
            vec![
                model_term(0, CENTRE, 0.75),
                model_term(1, OUTLIER_SOURCE, 0.25),
            ],
        ),
    }
}

/// One more major cycle of `prior`'s model.
fn rerun_two_domain(problem: &CompiledProblem, prior: ContinuumRound) -> ContinuumRound {
    ContinuumRound {
        join: continue_round(problem, &two_domain_scene(problem), prior.join),
    }
}

const CONTINUUM_PRODUCTS: [ProductKind; 6] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::Mask,
];

fn planned_for<'a>(
    inputs: &ContinuumProductInputs<'a>,
    controls: &ContinuumProductControls,
) -> PlannedContinuumGeneration {
    PlannedContinuumGeneration::new(inputs, controls).expect("planned generation")
}

fn generate_for(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> GeneratedProducts {
    let output = ImageOutput::for_problem(inputs.problem());
    let generated = produce_continuum_members(planned, inputs, full_window(planned), &(), &output)
        .expect("direct product generation");
    GeneratedProducts::from_output(planned, &generated, &output)
}

/// The demand for writing `planned` into a fresh image output.
fn demand(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
) -> casa_imaging_products::ContinuumGenerationDemand {
    planned
        .demand(
            inputs,
            storage_plan,
            &ImageOutput::for_problem(inputs.problem()),
        )
        .expect("product demand")
}

#[test]
fn planned_generation_lists_the_published_members() {
    let problem = continuum_problem(81, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned generation");

    let published = problem.product_list().publication().members();
    assert_eq!(planned.members().len(), published.len());
    for (member, id) in planned.members().iter().zip(published.iter()) {
        assert_eq!(member.id(), *id);
        assert!(!member.name().is_empty());
        assert_eq!(member.shape()[2..], [1, 1]);
        assert_eq!(
            member.payload_values(),
            member.shape().iter().product::<usize>()
        );
    }
    let names = planned
        .members()
        .iter()
        .map(|member| member.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [".psf", ".residual", ".model", ".image", ".sumwt", ".mask"]
    );
    assert_eq!(
        planned.members()[0].role(),
        ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
    );

    let replanned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("replanned");
    assert_eq!(planned.members().len(), replanned.members().len());
    assert_eq!(
        planned
            .members()
            .iter()
            .map(|member| member.id())
            .collect::<Vec<_>>(),
        replanned
            .members()
            .iter()
            .map(|member| member.id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn direct_generation_publishes_exactly_the_member_set() {
    let problem = continuum_problem(85, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned");
    let output = ImageOutput::for_problem(&problem);
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("generated members");

    assert_eq!(generated.members().len(), planned.members().len());
    for (generated_member, planned_member) in generated.members().iter().zip(planned.members()) {
        assert_eq!(generated_member.id(), planned_member.id());
        assert_eq!(generated_member.name(), planned_member.name());
    }
    let beam = generated
        .restoring_beams()
        .iter()
        .copied()
        .flatten()
        .next()
        .expect("fitted restoring beam");
    assert!(beam.major_fwhm_rad() >= beam.minor_fwhm_rad());
    assert!(beam.major_fwhm_rad() > 0.0);

    let collected = GeneratedProducts::from_output(&planned, &generated, &output);
    assert_eq!(
        output.entries(),
        [
            "domain0.image",
            "domain0.mask",
            "domain0.model",
            "domain0.psf",
            "domain0.residual",
            "domain0.sumwt"
        ],
        "publishing leaves the members and no staging"
    );
    let psf_payload = collected
        .members()
        .iter()
        .find(|member| member.name() == ".psf")
        .expect("psf member")
        .payload();
    let sensitivity = round.join.normal_state().sum_weight();
    let raw_psf = round
        .join
        .normal_state()
        .read_window(0..1)
        .expect("single-plane continuum fixture window")
        .normal_approximation()
        .iter()
        .map(|value| value.re as f32)
        .collect::<Vec<_>>();
    let peak = raw_psf.iter().copied().fold(0.0_f32, f32::max);
    let expected_psf = raw_psf.iter().map(|value| value / peak).collect::<Vec<_>>();
    assert_eq!(psf_payload, expected_psf);
    assert_eq!(psf_payload.iter().copied().fold(0.0_f32, f32::max), 1.0);

    let sumwt_index = collected
        .members()
        .iter()
        .position(|member| member.name() == ".sumwt")
        .expect("sumwt member");
    assert_eq!(
        collected.members()[sumwt_index].payload(),
        &[sensitivity as f32][..]
    );
}

#[test]
fn two_domain_members_consume_their_matching_normal_and_model_chart() {
    let products = TWO_DOMAIN_PRODUCTS;
    let problem = two_domain_problem(141);
    let first_round = run_two_domain_round(&problem);
    assert_eq!(first_round.join.normal_state().domain_count(), 2);
    let mask_plans =
        ImageDomainReconstructionMaskPlans::new(problem.geometry().domains().iter().map(
            |domain| ReconstructionMaskPlan::FullPlane {
                coordinate: domain.direction(),
            },
        ))
        .expect("domain mask plans");
    let (masks, _) = mask_plans
        .materialize(
            first_round.join.final_model(),
            first_round.join.normal_state(),
            None,
        )
        .expect("domain masks")
        .into_parts();
    let round = rerun_two_domain(&problem, first_round);
    let normal = round.join.normal_state();
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_domain_reconstruction_masks(&masks);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    demand(&planned, &inputs, full_window(&planned));
    let generated = generate_for(&planned, &inputs);
    assert_eq!(generated.members().len(), products.len() * 2);

    let window = normal
        .read_window(normal.slab().core_range())
        .expect("continuum fixture window");

    for (ordinal, role) in [
        ImageDomainRole::Main,
        ImageDomainRole::Outlier("east".into()),
    ]
    .iter()
    .enumerate()
    {
        let domain = window.domain_by_role(role).expect("domain normal state");
        let expected_shape = problem.geometry().domains()[ordinal].shape().pixels();
        let model_member = generated
            .members()
            .iter()
            .find(|member| {
                member.contract().axes().domain() == role
                    && member.contract().role()
                        == ProductRole::Model(casa_imaging_model::ProductTerm::Single)
            })
            .expect("domain model member");
        assert_eq!(
            model_member.payload().len(),
            expected_shape[0] * expected_shape[1]
        );
        let expected_model_peak = if ordinal == 0 { 0.75 } else { 0.25 };
        assert_eq!(
            model_member
                .payload()
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max),
            expected_model_peak
        );

        let psf_member = generated
            .members()
            .iter()
            .find(|member| {
                member.contract().axes().domain() == role
                    && member.contract().role()
                        == ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
            })
            .expect("domain PSF member");
        let sum_weight = domain.sum_weights()[0] as f32;
        let peak = domain
            .normal_approximation()
            .iter()
            .map(|value| value.re as f32)
            .fold(0.0_f32, f32::max);
        let expected_psf = if sum_weight.is_finite() && sum_weight > 0.0 {
            domain
                .normal_approximation()
                .iter()
                .map(|value| value.re as f32 / peak)
                .collect::<Vec<_>>()
        } else {
            vec![0.0; expected_shape[0] * expected_shape[1]]
        };
        assert_eq!(psf_member.payload(), expected_psf);

        let mask_member = generated
            .members()
            .iter()
            .find(|member| {
                member.contract().axes().domain() == role
                    && member.contract().role() == ProductRole::CleanMask
            })
            .expect("domain mask member");
        let expected_mask = vec![1.0; expected_shape[0] * expected_shape[1]];
        assert_eq!(mask_member.payload(), expected_mask);
    }
}

#[test]
fn direct_generation_publishes_metadata_without_payload_residency() {
    let problem = continuum_problem(92, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned");
    let output = ImageOutput::for_problem(&problem);
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("generated");
    assert_eq!(generated.members().len(), planned.members().len());
    let collected = GeneratedProducts::from_output(&planned, &generated, &output);
    for (member, planned_member) in collected.members().iter().zip(planned.members()) {
        assert_eq!(member.id(), planned_member.id());
        assert_eq!(member.name(), planned_member.name());
        assert_eq!(member.payload().len(), planned_member.payload_values());
    }
}

#[test]
fn bounded_windows_publish_the_members_one_window_does() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Mask,
    ];
    let problem = continuum_problem_with_reconstruction(
        96,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let output = ImageOutput::for_problem(&problem);
    let storage_plan = ProductStoragePlan::new(1, 1).expect("one-channel output bound");
    let generated = produce_continuum_members(&planned, &inputs, storage_plan, &(), &output)
        .expect("bounded generation");
    assert_eq!(generated.members().len(), planned.members().len());
    let bounded = GeneratedProducts::from_output(&planned, &generated, &output);
    let full = generate_for(&planned, &inputs);
    for (bounded, full) in bounded.members().iter().zip(full.members()) {
        assert_eq!(bounded.name(), full.name());
        assert_eq!(bounded.payload(), full.payload());
        assert_eq!(bounded.validity(), full.validity());
    }

    let parallel_plan = demand(&planned, &inputs, ProductStoragePlan::new(1, 8).unwrap());
    assert_eq!(parallel_plan.storage_plan().maximum_workers(), 2);
    let serial_demand = demand(&planned, &inputs, storage_plan);
    assert_eq!(
        parallel_plan.algorithm_scratch_bytes(),
        serial_demand.algorithm_scratch_bytes() * 2
    );
    assert_eq!(
        parallel_plan.beam_scratch_bytes(),
        serial_demand.beam_scratch_bytes() * 2
    );
    assert_eq!(
        parallel_plan.retained_metadata_bytes(),
        serial_demand.retained_metadata_bytes()
    );
    let execution = ReversedWindowCompletion { fail: false };
    let parallel_output = ImageOutput::for_problem(&problem);
    let parallel = produce_continuum_members(
        &planned,
        &inputs,
        parallel_plan.storage_plan(),
        &execution,
        &parallel_output,
    )
    .expect("out-of-order preparation drains in exact channel order");
    let parallel = GeneratedProducts::from_output(&planned, &parallel, &parallel_output);
    for (parallel, serial) in parallel.members().iter().zip(full.members()) {
        assert_eq!(parallel.name(), serial.name());
        assert_eq!(parallel.payload(), serial.payload());
        assert_eq!(parallel.validity(), serial.validity());
    }
    let failed_output = ImageOutput::for_problem(&problem);
    assert!(matches!(
        produce_continuum_members(
            &planned,
            &inputs,
            parallel_plan.storage_plan(),
            &ReversedWindowCompletion { fail: true },
            &failed_output,
        ),
        Err(ProductsError::GeneratedNonfinite)
    ));
    failed_output
        .publish(|image| panic!("{} published", image.display()))
        .expect("nothing staged to publish");
    assert!(
        failed_output.entries().is_empty(),
        "a failed generation publishes no image and leaves no staging: {:?}",
        failed_output.entries()
    );
}

/// A generation that fails after members have finished discards their
/// staging before the writer drops, so publishing it publishes nothing.
#[test]
fn a_late_failure_discards_the_members_already_staged() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
    ];
    let problem = continuum_problem_with_reconstruction(
        97,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let storage_plan = ProductStoragePlan::new(1, 1).expect("one-channel windows");
    let counting = FailingCall::new(usize::MAX);
    produce_continuum_members(
        &planned,
        &inputs,
        storage_plan,
        &counting,
        &ImageOutput::for_problem(&problem),
    )
    .expect("an unfailing generation");
    let calls = counting.calls.into_inner();
    // Fail every preparation in turn, the later ones after members finished.
    for failing_call in 1..=calls {
        let output = ImageOutput::for_problem(&problem);
        let error = produce_continuum_members(
            &planned,
            &inputs,
            storage_plan,
            &FailingCall::new(failing_call),
            &output,
        )
        .expect_err("the executor fails");
        assert!(
            matches!(error, ProductsError::GeneratedNonfinite),
            "{error}"
        );
        assert!(
            output.entries().is_empty(),
            "call {failing_call}: the finished members' staging is gone: {:?}",
            output.entries()
        );
        output
            .publish(|image| panic!("{} published", image.display()))
            .expect("nothing staged to publish");
        assert!(output.entries().is_empty(), "nothing published");
    }
}

/// Runs preparations in order and fails the `failing`th, counting from one.
struct FailingCall {
    calls: std::sync::atomic::AtomicUsize,
    failing: usize,
}

impl FailingCall {
    fn new(failing: usize) -> Self {
        Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
            failing,
        }
    }
}

impl casa_imaging_products::ProductWindowExecutor for FailingCall {
    fn prepare<T: Send>(
        &self,
        slots: &mut [Option<T>],
        operation: &(dyn Fn(usize) -> Result<T, ProductsError> + Sync),
    ) -> Result<(), ProductsError> {
        for (index, slot) in slots.iter_mut().enumerate() {
            let call = self
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if call == self.failing {
                return Err(ProductsError::GeneratedNonfinite);
            }
            *slot = Some(operation(index)?);
        }
        Ok(())
    }
}

struct ReversedWindowCompletion {
    fail: bool,
}

impl casa_imaging_products::ProductWindowExecutor for ReversedWindowCompletion {
    fn prepare<T: Send>(
        &self,
        slots: &mut [Option<T>],
        operation: &(dyn Fn(usize) -> Result<T, ProductsError> + Sync),
    ) -> Result<(), ProductsError> {
        use std::sync::{Condvar, Mutex};
        let next = Mutex::new(slots.len() - 1);
        let ready = Condvar::new();
        std::thread::scope(|scope| {
            let handles = slots
                .iter_mut()
                .enumerate()
                .map(|(index, slot)| {
                    let next = &next;
                    let ready = &ready;
                    scope.spawn(move || {
                        let prepared = operation(index);
                        let mut turn = next.lock().unwrap();
                        while *turn != index {
                            turn = ready.wait(turn).unwrap();
                        }
                        let result = if self.fail && index == 0 {
                            Err(ProductsError::GeneratedNonfinite)
                        } else {
                            prepared.map(|window| *slot = Some(window))
                        };
                        *turn = turn.saturating_sub(1);
                        ready.notify_all();
                        result
                    })
                })
                .collect::<Vec<_>>();
            let results = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>();
            results.into_iter().collect()
        })
    }
}

#[test]
fn an_unwritable_output_fails_generation_and_publishes_nothing() {
    let problem = continuum_problem(98, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());

    let root = tempfile::tempdir().expect("output root");
    let blocked = root.path().join("blocked");
    std::fs::write(&blocked, b"a file where the output directory goes").expect("blocker");
    let writer = casa_imaging_products::CasaImageWriter::new(
        blocked.join("image"),
        casa_coordinates::CoordinateSystem::new(),
    );
    let error = produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &writer)
        .expect_err("the output directory cannot be created");
    assert!(
        matches!(&error, ProductsError::Io { path, .. } if path == &blocked),
        "{error}"
    );
    writer
        .publish(|image| panic!("{} published", image.display()))
        .expect("nothing staged to publish");
    let entries = std::fs::read_dir(root.path())
        .expect("output root")
        .map(|entry| entry.expect("entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, ["blocked"], "nothing was published or staged");
}

#[test]
fn single_window_restoration_admits_inner_fft_workers_without_replica_buffers() {
    let problem = continuum_problem(119, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let serial = demand(&planned, &inputs, ProductStoragePlan::new(1, 1).unwrap());
    let serial_output = ImageOutput::for_problem(&problem);
    let serial_generation = produce_continuum_members(
        &planned,
        &inputs,
        serial.storage_plan(),
        &(),
        &serial_output,
    )
    .unwrap();
    let serial_values =
        GeneratedProducts::from_output(&planned, &serial_generation, &serial_output);
    for workers in [4, 8] {
        let parallel = demand(
            &planned,
            &inputs,
            ProductStoragePlan::new(1, workers).unwrap(),
        );
        assert_eq!(
            parallel.storage_plan().maximum_workers(),
            if cfg!(unix) { workers } else { 1 }
        );
        assert_eq!(
            parallel.algorithm_scratch_bytes(),
            serial.algorithm_scratch_bytes()
        );
        assert_eq!(parallel.beam_scratch_bytes(), serial.beam_scratch_bytes());
        assert_eq!(
            parallel.peak_residency_bytes(),
            serial.peak_residency_bytes()
        );
        let output = ImageOutput::for_problem(&problem);
        let generated =
            produce_continuum_members(&planned, &inputs, parallel.storage_plan(), &(), &output)
                .unwrap();
        let values = GeneratedProducts::from_output(&planned, &generated, &output);
        for (actual, expected) in values.members().iter().zip(serial_values.members()) {
            assert_eq!(actual.id(), expected.id());
            assert_eq!(actual.validity(), expected.validity());
            for (actual, expected) in actual.payload().iter().zip(expected.payload()) {
                assert!((actual - expected).abs() <= 1.0e-6 * expected.abs().max(1.0));
            }
        }
    }
}

#[test]
fn generic_generation_demand_charges_exact_owned_arrays() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Mask,
    ];
    let problem = continuum_problem_with_policy(94, &products, RestoringBeamPolicy::None);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let demand = demand(&planned, &inputs, full_window(&planned));
    let values = planned
        .members()
        .iter()
        .map(|member| member.payload_values() as u64)
        .sum::<u64>();
    let maximum = planned
        .members()
        .iter()
        .map(|member| member.payload_values() as u64)
        .max()
        .expect("members");
    assert_eq!(
        values,
        planned
            .members()
            .iter()
            .map(|member| member.payload_values() as u64)
            .sum::<u64>()
    );
    assert_eq!(demand.maximum_member_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_member_validity_bytes(), maximum);
    assert_eq!(demand.maximum_window_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_window_validity_bytes(), maximum);
    assert_eq!(
        demand.algorithm_scratch_bytes(),
        (SHAPE[0]
            * SHAPE[1]
            * (2 * size_of::<f32>() + size_of::<casa_imaging_model::ModelSample>())) as u64
            + casa_imaging_deconvolution::normal_state_window_residency_bytes(SHAPE, 1, 1).unwrap()
            + maximum * 5
            + size_of::<Option<casa_imaging_products::ProductWindow>>() as u64,
        "generic normalization overlaps input windows, its converted plane/result, and one output window"
    );
    assert_eq!(
        demand.peak_residency_bytes(),
        demand.retained_metadata_bytes()
            + (demand.algorithm_scratch_bytes()
                + size_of::<Option<casa_imaging_products::RestoringBeam>>() as u64)
                .max(demand.beam_scratch_bytes())
            + demand.writer_bytes()
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
    assert!(demand.retained_metadata_bytes() > 0);
}

#[test]
fn cube_generation_demand_retains_channel_beams_and_charges_common_fit_scratch() {
    use casa_imaging_products::RestoringBeam;
    for (offset, policy) in [
        RestoringBeamPolicy::None,
        RestoringBeamPolicy::PerPlane,
        RestoringBeamPolicy::Common,
    ]
    .into_iter()
    .enumerate()
    {
        let mut products = vec![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::SumWeights,
        ];
        if policy != RestoringBeamPolicy::None {
            products.push(ProductKind::RestoredImage);
        }
        let problem = continuum_problem_with_reconstruction(
            180 + offset as u8,
            &products,
            policy,
            InstrumentResponse::Scalar,
            ReconstructionBasis::ChannelLocal { channels: 2 },
            ReconstructionAlgorithm::Dirty,
            2,
        );
        let round = run_continuum_round(&problem);
        let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
        let planned = planned_for(&inputs, &ContinuumProductControls::default());
        let demand = demand(&planned, &inputs, ProductStoragePlan::new(1, 1).unwrap());
        let generated = produce_continuum_members(
            &planned,
            &inputs,
            ProductStoragePlan::new(1, 1).unwrap(),
            &(),
            &ImageOutput::for_problem(&problem),
        )
        .unwrap();
        assert_eq!(generated.fitted_beams().len(), 2);
        assert_eq!(
            demand.retained_metadata_bytes(),
            common::retained_metadata_bytes(&generated)
        );
        assert!(
            demand.beam_scratch_bytes()
                >= casa_imaging_deconvolution::psf_fit_workspace_bytes(SHAPE)
        );
        if policy == RestoringBeamPolicy::Common {
            assert!(
                demand.beam_scratch_bytes()
                    >= (2
                        * (size_of::<RestoringBeam>()
                            + 2 * size_of::<casa_numerics::EllipticalGaussian>()))
                        as u64
            );
        }
        assert_eq!(
            demand.peak_residency_bytes(),
            demand.retained_metadata_bytes()
                + (demand.algorithm_scratch_bytes()
                    + (2 * size_of::<Option<RestoringBeam>>()) as u64)
                    .max(demand.beam_scratch_bytes())
                + demand.writer_bytes()
        );

        let parallel_demand =
            self::demand(&planned, &inputs, ProductStoragePlan::new(1, 8).unwrap());
        assert_eq!(parallel_demand.storage_plan().maximum_workers(), 2);
        assert_eq!(
            parallel_demand.beam_scratch_bytes(),
            2 * demand.beam_scratch_bytes()
        );
        assert_eq!(
            parallel_demand.retained_metadata_bytes(),
            demand.retained_metadata_bytes()
        );
        assert_eq!(
            parallel_demand.peak_residency_bytes(),
            parallel_demand.retained_metadata_bytes()
                + (parallel_demand.algorithm_scratch_bytes()
                    + (2 * size_of::<Option<RestoringBeam>>()) as u64)
                    .max(parallel_demand.beam_scratch_bytes())
                + parallel_demand.writer_bytes()
        );
        let parallel_output = ImageOutput::for_problem(&problem);
        let parallel = produce_continuum_members(
            &planned,
            &inputs,
            parallel_demand.storage_plan(),
            &ReversedWindowCompletion { fail: false },
            &parallel_output,
        )
        .expect("reversed beam completion preserves ordered beam policy");
        assert_eq!(parallel.fitted_beams(), generated.fitted_beams());
        assert_eq!(parallel.restoring_beams(), generated.restoring_beams());
        let parallel = GeneratedProducts::from_output(&planned, &parallel, &parallel_output);
        let serial = generate_for(&planned, &inputs);
        for (parallel, serial) in parallel.members().iter().zip(serial.members()) {
            assert_eq!(parallel.payload(), serial.payload());
            assert_eq!(parallel.validity(), serial.validity());
        }
        let failed_output = ImageOutput::for_problem(&problem);
        assert!(matches!(
            produce_continuum_members(
                &planned,
                &inputs,
                parallel_demand.storage_plan(),
                &ReversedWindowCompletion { fail: true },
                &failed_output,
            ),
            Err(ProductsError::GeneratedNonfinite)
        ));
        assert!(
            failed_output.entries().is_empty(),
            "beam fitting precedes every image"
        );
    }
}

#[test]
fn weight_products_plan_and_produce_the_exact_normal_state_sensitivity_plane() {
    // Weight members are required products: they plan like every other
    // member and carry the normal state's exact per-pixel sensitivity.
    let problem = continuum_problem(
        107,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::Mask,
            ProductKind::Weight,
        ],
    );
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);
    let weight = generated
        .members()
        .iter()
        .find(|member| member.name().starts_with(".weight"))
        .expect("weight member");
    let expected: Vec<f32> = round
        .join
        .normal_state()
        .read_window(0..1)
        .expect("single-plane continuum fixture window")
        .sensitivity()
        .iter()
        .map(|value| value as f32)
        .collect();
    assert_eq!(weight.payload(), expected);
}

#[test]
fn standard_products_publish_the_selected_analytic_primary_beam() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
        ProductKind::PbCorrectedImage,
        ProductKind::Beam,
    ];
    let problem = continuum_problem_with_reconstruction(
        109,
        &products,
        RestoringBeamPolicy::PerPlane,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
    );
    let round = run_continuum_round(&problem);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &controls);
    let generated = generate_for(&planned, &inputs);
    let pb = generated
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam");
    let centre = pb.payload()[4 * SHAPE[1] + 4];
    let corner = pb.payload()[0];
    assert_eq!(centre, 1.0);
    assert!(
        corner < centre,
        "analytic PB must fall away from phase centre"
    );
}

/// #715: the writer stages each image and then promotes it, and the published
/// image's mask table reference must be relative to the image itself, so the
/// mask survives a later move of the published image.
#[test]
fn a_published_masked_product_keeps_its_mask_after_the_image_is_moved() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        109,
        &products,
        RestoringBeamPolicy::PerPlane,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
    );
    let round = run_continuum_round(&problem);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &controls);
    let output = ImageOutput::for_problem(inputs.problem());
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("direct product generation");
    let published = GeneratedProducts::from_output(&planned, &generated, &output);
    let validity = published
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam")
        .validity()
        .to_vec();
    let member = planned
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("planned primary beam");
    let image = PagedImage::<f32>::open(output.path(member)).expect("published primary beam");
    let Some(Value::Record(masks)) = image.table().keywords().get("masks") else {
        panic!("the published primary beam has no masks keyword");
    };
    let Some(Value::Record(region)) = masks.get("mask0") else {
        panic!("the published primary beam has no mask0 region");
    };
    assert_eq!(region.get("mask"), Some(&Value::table_ref("././mask0")));
    drop(image);

    let moved_root = tempfile::tempdir().expect("moved directory");
    let moved = moved_root.path().join("deeper").join("moved.pb");
    std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
    std::fs::rename(output.path(member), &moved).expect("move the published image");
    let image = PagedImage::<f32>::open(&moved).expect("moved primary beam");
    assert_eq!(image.default_mask_name().as_deref(), Some("mask0"));
    let shape = image.shape().to_vec();
    let mask = image
        .get_mask_slice(&vec![0; shape.len()], &shape, &vec![1; shape.len()])
        .expect("moved mask")
        .expect("default mask");
    assert_eq!(mask.iter().copied().collect::<Vec<_>>(), validity);
}

#[test]
fn primary_beam_plan_rejects_a_cube_crossing_the_evla_c_band_boundary() {
    let products = [ProductKind::Psf, ProductKind::PrimaryBeam];
    let template = continuum_problem_with_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let problem = continuum_problem_with_domains_and_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
        [7.880e9, 128.0e6],
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            template.geometry().domains()[0].direction(),
            FacetLayout::Single,
            axes(),
        )],
    );
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let unsupported = |error| {
        matches!(
            error,
            ProductsError::UnsupportedPrimaryBeamFrequency {
                model: AnalyticPrimaryBeamModel::CasaEvlaCommon,
                output_channel: 1,
                frequency_hz,
            } if frequency_hz == 8.008e9
        )
    };
    assert!(unsupported(
        PlannedContinuumGeneration::new(&inputs, &controls)
            .expect_err("unsupported frequency must fail during planning, not production")
    ));
    assert!(unsupported(
        controls
            .validate_for_problem(&problem)
            .expect_err("unsupported frequency")
    ));
}

#[test]
fn primary_beam_cube_crosses_from_vla_q_to_nvss() {
    let products = [ProductKind::Psf, ProductKind::PrimaryBeam];
    let template = continuum_problem_with_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let problem = continuum_problem_with_domains_and_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
        [54.880e9, 128.0e6],
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            template.geometry().domains()[0].direction(),
            FacetLayout::Single,
            axes(),
        )],
    );
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand);
    controls
        .validate_for_problem(&problem)
        .expect("VLA Q and NVSS coverage");
    let planned = planned_for(&inputs, &controls);
    let generated = generate_for(&planned, &inputs);
    let pb = generated
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam");
    assert_eq!(pb.payload().len(), SHAPE[0] * SHAPE[1] * 2);
    for channel in 0..2 {
        let plane = pb.payload().iter().skip(channel).step_by(2);
        assert!(plane.clone().all(|value| value.is_finite()));
        assert_eq!(plane.clone().copied().reduce(f32::max), Some(1.0));
        assert!(plane.copied().any(|value| value < 1.0));
    }
    assert!(
        pb.payload()
            .as_chunks::<2>()
            .0
            .iter()
            .any(|pixel| pixel[0] != pixel[1])
    );
}

#[test]
fn standard_cube_products_publish_analytic_primary_beams_per_output_channel() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let round = run_continuum_round(&problem);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &controls);
    let generated = generate_for(&planned, &inputs);
    let pb = generated
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam");
    assert_eq!(pb.payload().len(), SHAPE[0] * SHAPE[1] * 2);
    assert_eq!(pb.payload().iter().copied().reduce(f32::max), Some(1.0),);
    assert!(pb.payload().iter().any(|value| *value < 1.0));
    let payload = pb.payload();
    for channel in 0..2 {
        let plane = payload.iter().skip(channel).step_by(2);
        assert_eq!(plane.clone().copied().reduce(f32::max), Some(1.0));
        assert!(plane.copied().any(|value| value < 1.0));
    }
}

#[test]
fn clean_mask_product_is_the_committed_reconstruction_support() {
    let problem = continuum_problem(117, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let direction = problem.geometry().domains()[0].direction();
    let mask = ReconstructionMask::from_boxes(
        direction,
        SHAPE,
        [MaskBox::new([2, 3], [4, 5]).expect("mask box")],
    )
    .expect("reconstruction mask");
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_reconstruction_mask(&mask);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);
    let published_mask = generated
        .members()
        .iter()
        .find(|member| member.name().starts_with(".mask"))
        .expect("mask member");
    assert!(
        published_mask.validity().iter().all(|valid| *valid),
        "the numeric CLEAN-mask support is not the product-validity mask"
    );
    let expected = mask
        .support()
        .iter()
        .map(|selected| if *selected { 1.0 } else { 0.0 })
        .collect::<Vec<_>>();
    assert_eq!(published_mask.payload(), expected);
}

#[test]
fn restoration_adds_the_published_residual_without_scaling_the_convolved_model() {
    // CASA equation: restored = conv(model, beam) + residual-as-published.
    // With FlatNoise members the residual part is divided by the sum weight
    // while the convolved sky model is never divided by it.
    let problem = continuum_problem(105, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);

    // A nonzero final model: apply the round's delta through a fresh owner.
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);

    let sensitivity = round.join.normal_state().sum_weight();
    assert!(
        sensitivity.is_finite() && sensitivity > 0.0 && (sensitivity - 1.0).abs() > 1.0e-6,
        "fixture must carry a non-unit sum weight, got {sensitivity}"
    );
    let model_member = generated
        .members()
        .iter()
        .find(|member| member.name() == ".model")
        .expect("model member");
    assert!(
        model_member.payload().iter().any(|value| *value != 0.0),
        "fixture must carry a nonzero sky model"
    );
    let residual_member = generated
        .members()
        .iter()
        .find(|member| member.name() == ".residual")
        .expect("residual member");
    assert!(
        residual_member.payload().iter().any(|value| *value != 0.0),
        "fixture must carry a nonzero residual"
    );

    // Recompute the expected restoration independently from the generated parts.
    let beam = generated.restoring_beam().copied().expect("fitted beam");
    let cells = round.join.normal_state().shape();
    let cell = inputs.cell_size_rad();
    let kernel = gaussian_beam_image(cells, &beam, cell);
    let convolved = casa_imaging_products::fft_convolve(
        model_member.payload(),
        kernel.as_slice().expect("contiguous"),
        cells,
    );
    let restored = generated
        .members()
        .iter()
        .find(|member| member.name() == ".image")
        .expect("restored member")
        .payload();
    let mut max_error = 0.0_f64;
    for (index, restored_value) in restored.iter().enumerate() {
        let expected = convolved[index] + residual_member.payload()[index];
        max_error = max_error.max((f64::from(*restored_value) - f64::from(expected)).abs());
    }
    assert!(
        max_error < 1.0e-5,
        "restored plane diverged from conv(model) + published residual by {max_error}"
    );
    // The old wrong behavior normalized the whole combined plane by the
    // sensitivity; with a non-unit sum weight the two planes must differ.
    let wrongly_scaled = convolved
        .iter()
        .zip(residual_member.payload())
        .map(|(convolved, residual)| (convolved + residual) / sensitivity as f32)
        .collect::<Vec<_>>();
    assert_ne!(
        restored.to_vec(),
        wrongly_scaled,
        "restored payload must not be the sensitivity-scaled combined plane"
    );
}

#[test]
fn generated_members_carry_the_complete_product_contract() {
    // Every generated member is planned from its product-list entry: role,
    // unit, WCS/axes law, beam rule with resolved fitted beam, and validity
    // rule - not just name and payload.
    let problem = continuum_problem(111, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);

    let list = problem.product_list();
    for member in generated.members() {
        let entry = list
            .entries()
            .iter()
            .find(|entry| entry.id() == member.id())
            .expect("generated member names a product-list entry");
        let contract = member.contract();
        assert_eq!(contract.role(), entry.role());
        assert_eq!(contract.unit(), entry.unit());
        assert_eq!(contract.axes(), entry.axes());
        assert_eq!(contract.beam_rule(), entry.beam());
        assert_eq!(contract.validity(), entry.validity());
    }

    // Beam-bearing members resolve the generation's fitted beam; beam-free
    // members resolve none.
    let fitted = generated.restoring_beam().copied().expect("fitted beam");
    let image = generated
        .members()
        .iter()
        .find(|member| member.name() == ".image")
        .expect("restored member");
    assert_eq!(image.resolved_beam(), Some(&fitted));
    let mask = generated
        .members()
        .iter()
        .find(|member| member.name() == ".mask")
        .expect("mask member");
    assert_eq!(mask.resolved_beam(), None);

    let expected_mask = vec![1.0; SHAPE[0] * SHAPE[1]];
    assert_eq!(mask.payload(), expected_mask);
}

/// Generate and reopen channel one of a two-channel VLA PB cube. The wide
/// pixels make the aperture shape measurable without a large image fixture.
fn vla_cube_target_beam(first_hz: f64, target_hz: f64) -> Vec<f32> {
    use casa_imaging_model::{DirectionCoordinateSpec, DirectionFrame, Projection, SkyDirection};

    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [CENTRE[0] as f64, CENTRE[1] as f64],
        [-1.0e-3, 1.0e-3],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let problem = continuum_problem_with_domains_and_reconstruction(
        111,
        &[ProductKind::Psf, ProductKind::PrimaryBeam],
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
        [first_hz, target_hz - first_hz],
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            direction,
            FacetLayout::Single,
            axes(),
        )],
    );
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand);
    let generated = generate_for(&planned_for(&inputs, &controls), &inputs);
    generated
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("published primary beam")
        .payload()
        .iter()
        .skip(1)
        .step_by(2)
        .copied()
        .collect()
}

#[test]
fn vla_cube_primary_beam_keeps_the_first_channel_aperture() {
    // Hold the measured plane's frequency fixed while changing only the
    // first channel. CASA selects its VLA model once at channel zero:
    // 0.8/0.9 GHz select NVSS, and 1.1/1.2 GHz select the named L band.
    // The two target frequencies cover NVSS-to-L ascending cubes and
    // L-to-NVSS descending cubes without reproducing an aperture formula.
    for target_hz in [1.5e9, 0.7e9] {
        let nvss = vla_cube_target_beam(0.8e9, target_hz);
        let same_nvss = vla_cube_target_beam(0.9e9, target_hz);
        let named = vla_cube_target_beam(1.1e9, target_hz);
        let same_named = vla_cube_target_beam(1.2e9, target_hz);
        assert_eq!(nvss, same_nvss, "same NVSS aperture at {target_hz} Hz");
        assert_eq!(named, same_named, "same named aperture at {target_hz} Hz");
        assert!(nvss.iter().chain(&named).all(|value| value.is_finite()));
        assert_eq!(nvss.iter().copied().reduce(f32::max), Some(1.0));
        assert_eq!(named.iter().copied().reduce(f32::max), Some(1.0));
        let difference = nvss
            .iter()
            .zip(&named)
            .map(|(left, right)| (left - right).abs())
            .fold(0.0_f32, f32::max);
        assert!(
            difference > 1.0e-4,
            "channel-zero aperture choice must affect the fixed {target_hz} Hz plane: {difference}"
        );
    }
    // Keeping the aperture fixed must still let the actual plane frequency
    // change its width; channel zero must not replace the scaling frequency.
    assert_ne!(
        vla_cube_target_beam(0.8e9, 0.7e9),
        vla_cube_target_beam(0.8e9, 1.5e9)
    );
}
