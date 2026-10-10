// SPDX-License-Identifier: LGPL-3.0-or-later

//! T22 continuum product algorithms driven through the direct generation and
//! write-only output seams.
//!
//! Products are generated from major-cycle completions assembled from
//! explicit synthetic pass planes (`common::synthetic_pass`), not gridded.

use std::mem::size_of;

mod common;
use common::continuum::{
    SHAPE, TWO_DOMAIN_PRODUCTS, axes, continuum_problem,
    continuum_problem_with_domains_and_reconstruction, continuum_problem_with_policy,
    continuum_problem_with_reconstruction, two_domain_main_direction, two_domain_outlier_direction,
    two_domain_problem,
};
use common::observation::attempt;
use common::synthetic_pass::{Scene, continue_round, two_cycle_round};
use common::{GeneratedProducts, MemoryProductOutput, full_window};

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
use casa_imaging_reconstruction::{
    ImageDomainReconstructionMaskPlans, ImageDomainReconstructionMasks, MajorCycleCompletion,
    MaskBox, ReconstructionMask, ReconstructionMaskPlan, ReconstructionMaskSet,
};

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

fn run_continuum_round(problem: &CompiledProblem, attempt_byte: u8) -> ContinuumRound {
    ContinuumRound {
        join: two_cycle_round(
            problem,
            &continuum_scene(problem),
            attempt(attempt_byte),
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

fn run_two_domain_round(problem: &CompiledProblem, attempt_byte: u8) -> ContinuumRound {
    ContinuumRound {
        join: two_cycle_round(
            problem,
            &two_domain_scene(problem),
            attempt(attempt_byte),
            vec![
                model_term(0, CENTRE, 0.75),
                model_term(1, OUTLIER_SOURCE, 0.25),
            ],
        ),
    }
}

/// One more major cycle of `prior`'s model under attempt `attempt_byte` at
/// `epoch`, reconciled with the exact domain `masks`.
fn rerun_two_domain_with_masks(
    problem: &CompiledProblem,
    attempt_byte: u8,
    epoch: u64,
    prior: ContinuumRound,
    masks: &ImageDomainReconstructionMasks,
) -> ContinuumRound {
    ContinuumRound {
        join: continue_round(
            problem,
            &two_domain_scene(problem),
            attempt(attempt_byte),
            epoch,
            prior.join,
            Some(&ReconstructionMaskSet::Domains(masks.clone())),
        ),
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
    let output = MemoryProductOutput::default();
    let generated = produce_continuum_members(planned, inputs, full_window(planned), &(), &output)
        .expect("direct product generation");
    GeneratedProducts::from_output(&generated, &output)
}

#[test]
fn planned_generation_binds_the_exact_graph_and_run_associations() {
    let problem = continuum_problem(81, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 82);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned generation");
    assert_eq!(
        planned.major_cycle_completion(),
        inputs.major_cycle_completion()
    );
    assert_eq!(
        planned.normal_state_completion(),
        inputs.normal_state_completion()
    );
    assert_eq!(
        planned.final_model_generation(),
        round.join.normal_state().final_model_generation()
    );

    let graph_members = problem.product_graph().publication().members();
    assert_eq!(planned.members().len(), graph_members.len());
    for (member, node) in planned.members().iter().zip(graph_members.iter()) {
        assert_eq!(member.node(), *node);
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
            .map(|member| member.node())
            .collect::<Vec<_>>(),
        replanned
            .members()
            .iter()
            .map(|member| member.node())
            .collect::<Vec<_>>()
    );

    let other = continuum_problem(83, &CONTINUUM_PRODUCTS);
    let other_round = run_continuum_round(&other, 84);
    let other_inputs = ContinuumProductInputs::from_major_cycle(&other, &other_round.join);
    let other_planned =
        PlannedContinuumGeneration::new(&other_inputs, &ContinuumProductControls::default())
            .expect("other planned");
    assert_ne!(
        planned.major_cycle_completion(),
        other_planned.major_cycle_completion()
    );
}

#[test]
fn direct_generation_writes_the_exact_member_set_once() {
    let problem = continuum_problem(85, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 86);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned");
    let output = MemoryProductOutput::default();
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("generated members");

    assert_eq!(generated.members().len(), planned.members().len());
    for (generated_member, planned_member) in generated.members().iter().zip(planned.members()) {
        assert_eq!(generated_member.node(), planned_member.node());
        assert_eq!(generated_member.contract().role(), planned_member.role());
        assert_eq!(
            output.write_count(planned_member.node()),
            1,
            "full-window generation writes each member once"
        );
        assert!(output.finished(planned_member.node()));
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

    let collected = GeneratedProducts::from_output(&generated, &output);
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
    let main_direction = two_domain_main_direction();
    let outlier_direction = two_domain_outlier_direction();
    let products = TWO_DOMAIN_PRODUCTS;
    let problem = two_domain_problem(141);
    let first_round = run_two_domain_round(&problem, 142);
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
    let alternate_plans = ImageDomainReconstructionMaskPlans::new([
        ReconstructionMaskPlan::FullPlane {
            coordinate: main_direction,
        },
        ReconstructionMaskPlan::Boxes {
            coordinate: outlier_direction,
            boxes: vec![MaskBox::new([0, 0], [1, 1]).expect("alternate outlier box")],
        },
    ])
    .expect("alternate mask plans");
    let (alternate_masks, _) = alternate_plans
        .materialize(
            first_round.join.final_model(),
            first_round.join.normal_state(),
            None,
        )
        .expect("alternate domain masks")
        .into_parts();
    assert_ne!(masks.generation_id(), alternate_masks.generation_id());
    let round = rerun_two_domain_with_masks(&problem, 143, 8, first_round, &masks);
    let normal = round.join.normal_state();
    assert_eq!(
        normal.image_domain_mask_generation(),
        Some(masks.generation_id())
    );
    assert!(matches!(
        ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .with_domain_reconstruction_masks(&alternate_masks),
        Err(ProductsError::SourceLineageMismatch)
    ));
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_domain_reconstruction_masks(&masks)
        .expect("domain-mask inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    planned
        .demand(&inputs, full_window(&planned))
        .expect("two-domain product residency demand");
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
fn direct_generation_rejects_same_problem_with_foreign_completions() {
    let problem = two_domain_problem(145);
    let first_round = run_two_domain_round(&problem, 146);
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

    let second_round = rerun_two_domain_with_masks(&problem, 147, 8, first_round, &masks);
    let planned = {
        let second_inputs = ContinuumProductInputs::from_major_cycle(&problem, &second_round.join)
            .with_domain_reconstruction_masks(&masks)
            .expect("second mask-bound inputs");
        planned_for(&second_inputs, &ContinuumProductControls::default())
    };

    let third_round = rerun_two_domain_with_masks(&problem, 148, 9, second_round, &masks);
    let third_inputs = ContinuumProductInputs::from_major_cycle(&problem, &third_round.join)
        .with_domain_reconstruction_masks(&masks)
        .expect("third mask-bound inputs");
    assert_ne!(
        planned.final_model_generation(),
        third_inputs.final_model().generation_id(),
        "a distinct execution attempt owns a distinct adopted model generation"
    );
    assert_ne!(
        planned.major_cycle_completion(),
        third_inputs.major_cycle_completion(),
        "each reconciliation has its own run association"
    );
    assert_ne!(
        planned.normal_state_completion(),
        third_inputs.normal_state_completion(),
        "each reconciliation has its own normal-state completion"
    );

    assert!(matches!(
        planned.demand(&third_inputs, full_window(&planned)),
        Err(ProductsError::SourceLineageMismatch)
    ));
    let output = MemoryProductOutput::default();
    assert!(matches!(
        produce_continuum_members(&planned, &third_inputs, full_window(&planned), &(), &output),
        Err(ProductsError::SourceLineageMismatch)
    ));
}

#[test]
fn direct_generation_publishes_metadata_without_payload_residency() {
    let problem = continuum_problem(92, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 93);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned");
    let output = MemoryProductOutput::default();
    let generated =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("generated");
    assert_eq!(generated.members().len(), planned.members().len());
    let collected = GeneratedProducts::from_output(&generated, &output);
    for (member, planned_member) in collected.members().iter().zip(planned.members()) {
        assert_eq!(member.node(), planned_member.node());
        assert_eq!(member.name(), planned_member.name());
        assert_eq!(member.contract().role(), planned_member.role());
        assert_eq!(member.contract().unit(), planned_member.unit());
        assert_eq!(member.contract().schema(), planned_member.schema());
        assert_eq!(member.contract().validity(), planned_member.validity());
        assert_eq!(member.payload().len(), planned_member.payload_values());
        assert!(output.finished(planned_member.node()));
    }
}

#[test]
fn direct_generation_counts_bounded_windows_and_finishes_each_member() {
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
    let round = run_continuum_round(&problem, 97);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let output = MemoryProductOutput::default();
    let storage_plan = ProductStoragePlan::new(1, 1).expect("one-channel output bound");
    let generated = produce_continuum_members(&planned, &inputs, storage_plan, &(), &output)
        .expect("bounded generation");
    assert_eq!(generated.members().len(), planned.members().len());
    for member in planned.members() {
        let channels = member.axes().spectral().output_channels();
        assert_eq!(output.write_count(member.node()), channels);
        assert!(output.finished(member.node()));
    }
    let bounded = GeneratedProducts::from_output(&generated, &output);
    let full = generate_for(&planned, &inputs);
    for (bounded, full) in bounded.members().iter().zip(full.members()) {
        assert_eq!(bounded.name(), full.name());
        assert_eq!(bounded.payload(), full.payload());
        assert_eq!(bounded.validity(), full.validity());
    }

    let parallel_plan = planned
        .demand(&inputs, ProductStoragePlan::new(1, 8).unwrap())
        .unwrap();
    assert_eq!(parallel_plan.storage_plan().maximum_workers(), 2);
    let serial_demand = planned.demand(&inputs, storage_plan).unwrap();
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
    let parallel_output = MemoryProductOutput::default();
    let parallel = produce_continuum_members(
        &planned,
        &inputs,
        parallel_plan.storage_plan(),
        &execution,
        &parallel_output,
    )
    .expect("out-of-order preparation drains in exact channel order");
    let parallel = GeneratedProducts::from_output(&parallel, &parallel_output);
    for (parallel, serial) in parallel.members().iter().zip(full.members()) {
        assert_eq!(parallel.name(), serial.name());
        assert_eq!(parallel.payload(), serial.payload());
        assert_eq!(parallel.validity(), serial.validity());
    }
    let failed_output = MemoryProductOutput::default();
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
    assert_eq!(failed_output.begun_members(), 0);
    assert_eq!(failed_output.write_count(planned.members()[0].node()), 0);
    assert!(!failed_output.finished(planned.members()[0].node()));
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
fn output_errors_fail_generation_without_a_completion_receipt() {
    let problem = continuum_problem(98, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 99);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());

    let write_failure = MemoryProductOutput::failing_write();
    assert!(matches!(
        produce_continuum_members(
            &planned,
            &inputs,
            full_window(&planned),
            &(),
            &write_failure
        ),
        Err(ProductsError::Storage(_))
    ));
    assert_eq!(write_failure.begun_members(), 1);
    assert!(!write_failure.finished(planned.members()[0].node()));

    let finish_failure = MemoryProductOutput::failing_finish();
    assert!(matches!(
        produce_continuum_members(
            &planned,
            &inputs,
            full_window(&planned),
            &(),
            &finish_failure
        ),
        Err(ProductsError::Storage(_))
    ));
    assert_eq!(finish_failure.begun_members(), 1);
    assert!(!finish_failure.finished(planned.members()[0].node()));
}

#[test]
fn single_window_restoration_admits_inner_fft_workers_without_replica_buffers() {
    let problem = continuum_problem(119, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 120);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let serial = planned
        .demand(&inputs, ProductStoragePlan::new(1, 1).unwrap())
        .unwrap();
    let serial_output = MemoryProductOutput::default();
    let serial_generation = produce_continuum_members(
        &planned,
        &inputs,
        serial.storage_plan(),
        &(),
        &serial_output,
    )
    .unwrap();
    let serial_values = GeneratedProducts::from_output(&serial_generation, &serial_output);
    for workers in [4, 8] {
        let parallel = planned
            .demand(&inputs, ProductStoragePlan::new(1, workers).unwrap())
            .unwrap();
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
        let output = MemoryProductOutput::default();
        let generated =
            produce_continuum_members(&planned, &inputs, parallel.storage_plan(), &(), &output)
                .unwrap();
        let values = GeneratedProducts::from_output(&generated, &output);
        for (actual, expected) in values.members().iter().zip(serial_values.members()) {
            assert_eq!(actual.node(), expected.node());
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
    let round = run_continuum_round(&problem, 95);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let demand = planned
        .demand(&inputs, full_window(&planned))
        .expect("generic demand");
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
            + casa_imaging_reconstruction::normal_state_window_residency_bytes(SHAPE, 1, 1)
                .unwrap()
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
        let round = run_continuum_round(&problem, 190 + offset as u8);
        let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
        let planned = planned_for(&inputs, &ContinuumProductControls::default());
        let demand = planned
            .demand(&inputs, ProductStoragePlan::new(1, 1).unwrap())
            .unwrap();
        let generated = produce_continuum_members(
            &planned,
            &inputs,
            ProductStoragePlan::new(1, 1).unwrap(),
            &(),
            &MemoryProductOutput::default(),
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
        );

        let parallel_demand = planned
            .demand(&inputs, ProductStoragePlan::new(1, 8).unwrap())
            .unwrap();
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
        );
        let parallel_output = MemoryProductOutput::default();
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
        let parallel = GeneratedProducts::from_output(&parallel, &parallel_output);
        let serial = generate_for(&planned, &inputs);
        for (parallel, serial) in parallel.members().iter().zip(serial.members()) {
            assert_eq!(parallel.payload(), serial.payload());
            assert_eq!(parallel.validity(), serial.validity());
        }
        let failed_output = MemoryProductOutput::default();
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
        assert_eq!(
            failed_output.begun_members(),
            0,
            "beam barrier precedes every writer"
        );
    }
}

#[test]
fn weight_products_plan_and_produce_the_exact_normal_state_sensitivity_plane() {
    // Weight members are required graph products: they plan like every other
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
    let round = run_continuum_round(&problem, 108);
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
    let round = run_continuum_round(&problem, 110);
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

#[test]
fn primary_beam_plan_rejects_a_cube_crossing_the_vla_band_boundary() {
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
    let round = run_continuum_round(&problem, 112);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand);
    let error = PlannedContinuumGeneration::new(&inputs, &controls)
        .expect_err("unsupported frequency must fail during planning, not production");
    assert_eq!(
        error,
        ProductsError::UnsupportedPrimaryBeamFrequency {
            model: AnalyticPrimaryBeamModel::CasaVlaBand,
            output_channel: 1,
            frequency_hz: 55.008e9,
        }
    );
    assert_eq!(controls.validate_for_problem(&problem), Err(error));
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
    let round = run_continuum_round(&problem, 112);
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
    let round = run_continuum_round(&problem, 118);
    let normal = round.join.normal_state();
    let direction = problem.geometry().domains()[0].direction();
    let mask = ReconstructionMask::from_boxes(
        normal.input_model_generation(),
        direction,
        SHAPE,
        [MaskBox::new([2, 3], [4, 5]).expect("mask box")],
    )
    .expect("reconstruction mask");
    let unbound_inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .with_reconstruction_mask(&mask)
        .expect("mask-bound inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let unbound_output = MemoryProductOutput::default();
    assert!(matches!(
        produce_continuum_members(
            &planned,
            &unbound_inputs,
            full_window(&planned),
            &(),
            &unbound_output
        ),
        Err(ProductsError::SourceLineageMismatch)
    ));
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
    let round = run_continuum_round(&problem, 106);

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
fn generated_members_carry_the_complete_graph_contract() {
    // Every generated member must carry its full compiled contract: schema,
    // unit, WCS/axes law, beam rule with resolved fitted beam, validity
    // rule, and dependencies - not just name and payload.
    let problem = continuum_problem(111, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 112);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);

    let graph = problem.product_graph();
    for member in generated.members() {
        let node = graph
            .nodes()
            .iter()
            .find(|node| node.node_id() == member.node())
            .expect("generated member names a graph node");
        let contract = member.contract();
        assert_eq!(contract.role(), node.role());
        assert_eq!(contract.unit(), node.unit());
        assert_eq!(contract.schema(), node.schema());
        assert_eq!(contract.axes(), node.axes());
        assert_eq!(contract.beam_rule(), node.beam());
        assert_eq!(contract.validity(), node.validity());
        assert_eq!(contract.dependencies(), node.dependencies());
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
