// SPDX-License-Identifier: LGPL-3.0-or-later

//! Minor-cycle lineage and component placement: masks bound to generations,
//! deltas composed through the next Major Cycle, carried non-empty models,
//! scale kernels, window and valid-support constraints, and fail-closed
//! lineage checks.

use super::*;

#[test]
fn static_and_auto_masks_share_explicit_geometry_and_generation_lineage() {
    let round = first_confirm_round(185, 186);
    let problem = problem_with_model(
        187,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let coordinate = problem.geometry().domains()[0].direction();
    let static_mask = ReconstructionMask::from_boxes(
        problem.problem_id(),
        round.final_model.generation_id(),
        coordinate,
        SHAPE,
        [MaskBox::new([2, 2], [4, 4]).expect("box")],
    )
    .expect("static mask");
    assert!(static_mask.contains([3, 3]));
    assert!(!static_mask.contains([1, 1]));

    let valid = vec![true; SHAPE[0] * SHAPE[1]];
    let (automatic, evidence) = auto_multithresh(
        problem.problem_id(),
        round.final_model.generation_id(),
        coordinate,
        &round.normal_state,
        Some(&static_mask),
        &valid,
        1,
        false,
        false,
        AutoMultithreshControls {
            sidelobe_factor: 3.0,
            noise_factor: 5.0,
            low_noise_factor: 1.5,
            negative_factor: 0.0,
            minimum_beam_fraction: 0.0,
            smooth_factor: 0.0,
            cut_threshold: 0.01,
            grow_iterations: 1,
            minimum_percent_change: 0.0,
        },
    )
    .expect("generation-bound auto mask");
    assert_eq!(
        automatic.normal_state_completion(),
        Some(round.normal_state.completion_id())
    );
    assert!(
        automatic.contains([3, 3]),
        "auto masks retain prior support"
    );
    assert!(evidence.positive_threshold.is_finite());
}

#[test]
fn minor_cycle_delta_composes_with_the_next_major_cycle_reconciliation() {
    let round = first_confirm_round(41, 42);
    let final_generation = round.final_model.generation_id();
    assert_eq!(
        round.model_completion.generation(),
        final_generation,
        "the confirm round commits the named generation"
    );

    // The next round continues from the released generation through a
    // continuation commitment naming it; only the model owner can accept the
    // resulting delta against that base.
    let continuation = problem_with_model(
        43,
        ModelStateIdentity::Generation(final_generation.identity()),
    );
    let mut lifecycle = bind_lifecycle(&continuation, 44, 8);

    let residual_before = round
        .normal_state
        .read_window(0..1)
        .expect("single-plane fixture window")
        .residual()
        .complex()
        .unwrap()
        .to_vec();
    let model_before = round
        .final_model
        .read_samples(0..round.final_model.sample_count())
        .expect("read fixture model")
        .iter()
        .map(|sample| (sample.value().value(), sample.support()))
        .collect::<Vec<_>>();

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        controls(),
    )
    .expect("bounded Högbom solve");

    // Lineage binds the exact approximation and the exact named generation.
    let evidence = outcome.evidence();
    assert_eq!(evidence.problem_id(), continuation.problem_id());
    assert_eq!(evidence.attempt(), attempt(44));
    assert_eq!(evidence.epoch(), 8);
    assert_eq!(evidence.input_generation(), final_generation);
    assert_eq!(
        evidence.normal_state_completion(),
        round.normal_state.completion_id()
    );
    assert!(
        evidence.iterations() > 1,
        "the fixture must clean repeatedly"
    );
    assert!(
        matches!(
            evidence.stop_reason(),
            MinorCycleStopReason::IterationBound
                | MinorCycleStopReason::StalenessBound
                | MinorCycleStopReason::MultiscaleDivergence
                | MinorCycleStopReason::ThresholdReached
        ),
        "the solve stops explicitly"
    );

    // Authoritative state is untouched by the solve.
    assert_eq!(
        round
            .normal_state
            .read_window(0..1)
            .expect("single-plane fixture window")
            .residual()
            .complex()
            .unwrap(),
        residual_before
    );
    let model_after = round
        .final_model
        .read_samples(0..round.final_model.sample_count())
        .expect("read fixture model")
        .iter()
        .map(|sample| (sample.value().value(), sample.support()))
        .collect::<Vec<_>>();
    assert_eq!(model_before, model_after);

    // The delta is minted by the lifecycle owner against the exact base, in
    // canonical order, and accumulates the recorded components per cell.
    let (delta, evidence) = outcome.into_parts();
    let delta = delta.expect("an active solve mints a delta");
    let delta_id = delta.delta_id();
    assert_eq!(delta.base(), final_generation);
    let recorded = evidence
        .recorded_component_sequence()
        .expect("recording was requested");
    let mut expected = BTreeMap::<usize, f64>::new();
    for component in recorded {
        let flat = round
            .final_model
            .shape()
            .flat_index(component.cell())
            .expect("component cell inside shape");
        *expected.entry(flat).or_default() += component.flux();
    }
    let terms = delta
        .terms()
        .iter()
        .map(|term| {
            (
                round
                    .final_model
                    .shape()
                    .flat_index(term.cell())
                    .expect("inside"),
                term.increment().value(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(terms.len(), expected.len());
    for ((flat, sum), (expected_flat, expected_sum)) in terms.iter().zip(expected.iter()) {
        assert_eq!(flat, expected_flat, "canonical ascending cell order");
        assert!((sum - expected_sum).abs() <= 1.0e-12 * sum.abs().max(1.0));
    }

    // Recorded components carry the CASA normalization: gain times the
    // PSF-normalized residual peak, and the first lands on the global peak.
    let window = round
        .normal_state
        .read_window(0..1)
        .expect("single-plane fixture window");
    let psf_peak = window
        .normal_approximation()
        .complex()
        .unwrap()
        .iter()
        .map(|value| value.re.abs())
        .fold(0.0_f64, f64::max);
    let residual_peak_pixel = maximal_pixel(window.residual().complex().unwrap());
    assert_eq!(
        recorded[0].cell().pixel(),
        residual_peak_pixel,
        "the first component sits on the residual peak inside the window"
    );
    let expected_first_flux = controls().gain()
        * window.residual().complex().unwrap()[plane_index(residual_peak_pixel)].re
        / psf_peak;
    assert!((recorded[0].flux() - expected_first_flux).abs() <= 1.0e-12);
    assert!(
        (evidence.total_flux() - recorded.iter().map(|c| c.flux().abs()).sum::<f64>()).abs()
            <= 1.0e-12
    );

    // Applying the delta happens only through the owner, which mints the next
    // generation and reconciles a strictly reduced residual.
    let preparation = MajorCyclePreparation::prepare(&lifecycle, round.final_model, Some(delta))
        .expect("owner accepts its own delta");
    // Round two images the same sky through the continuation problem, now
    // with the updated model subtracted.
    let joined2 = scene(&continuation).reconcile(&continuation, &mut lifecycle, preparation);
    let model_completion2 = joined2.model_completion();
    assert_eq!(model_completion2.delta(), Some(delta_id));
    assert_eq!(model_completion2.base(), final_generation);
    let final2 = joined2.final_model();
    assert_ne!(final2.generation_id(), final_generation);
    assert_eq!(
        final2.origin(),
        casa_imaging_reconstruction::ModelGenerationOrigin::Delta {
            base: final_generation,
            delta: delta_id,
        }
    );
    for (flat, increment) in &terms {
        let updated = final2
            .read_samples(0..final2.sample_count())
            .expect("read fixture model")[*flat]
            .value()
            .value();
        let base_value = model_before[*flat].0;
        assert!((updated - (base_value + increment)).abs() <= 1.0e-12);
    }
    let peak2 = residual_peak(
        joined2
            .normal_state()
            .read_window(0..1)
            .expect("single-plane fixture window")
            .residual()
            .complex()
            .unwrap(),
    );
    assert!(
        peak2 < round.residual_peak,
        "one clean-and-reconcile round must reduce the residual peak: {peak2} !< {}",
        round.residual_peak
    );
}

#[test]
fn completed_nonempty_model_is_carried_affinely_into_the_next_major_cycle() {
    let problem = problem_with_model(45, ModelStateIdentity::Empty);
    let scene = scene(&problem);
    let mut initial = bind_lifecycle(&problem, 46, 9);
    let empty = initial.initial_empty().expect("initial empty model");
    let cell = empty.shape().cell_at(9).expect("fixture model cell");
    let seed_delta = initial
        .compile_delta(
            &empty,
            [casa_imaging_model::ModelDeltaTerm::new(
                cell,
                casa_imaging_model::ModelValue::new(2.5).expect("seed value"),
            )],
        )
        .expect("initial non-empty model delta");
    let preparation = MajorCyclePreparation::prepare(&initial, empty, Some(seed_delta))
        .expect("prepare non-empty initial model");
    let initial_completion = scene.reconcile(&problem, &mut initial, preparation);
    let carried_id = initial_completion.final_model().generation_id();
    assert_eq!(
        initial_completion
            .final_model()
            .read_samples(0..initial_completion.final_model().sample_count())
            .expect("read fixture model")[9]
            .value()
            .value(),
        2.5
    );

    let (initial_normal, continuation) = initial_completion.into_continuation();
    assert_eq!(initial_normal.final_model_generation(), carried_id);
    let (mut continued, carried) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable continuation"),
        attempt(47),
        10,
        continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("consume completed model continuation exactly once");
    assert_eq!(carried.generation_id(), carried_id);
    assert_eq!(
        carried
            .read_samples(0..carried.sample_count())
            .expect("read fixture model")[9]
            .value()
            .value(),
        2.5
    );

    let next_delta = continued
        .compile_delta(
            &carried,
            [casa_imaging_model::ModelDeltaTerm::new(
                cell,
                casa_imaging_model::ModelValue::new(-0.5).expect("continued value"),
            )],
        )
        .expect("continued lifecycle accepts only its carried base");
    let preparation = MajorCyclePreparation::prepare(&continued, carried, Some(next_delta))
        .expect("prepare continued final model");
    // The continued pass refreshes only the residual of the carried state.
    let final_completion =
        scene.reconcile_refresh(&problem, &mut continued, initial_normal, preparation);
    assert_eq!(final_completion.model_completion().base(), carried_id);
    assert_eq!(
        final_completion
            .final_model()
            .read_samples(0..final_completion.final_model().sample_count())
            .expect("read fixture model")[9]
            .value()
            .value(),
        2.0
    );
    assert!(matches!(
        continued.initial_empty(),
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
}

#[test]
fn multiscale_zero_scale_matches_the_point_component_and_extended_scale_spreads_support() {
    let round = first_confirm_round(175, 176);
    let continuation = problem_with_model(
        177,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let point_lifecycle = bind_lifecycle(&continuation, 178, 14);
    let scale_lifecycle = bind_lifecycle(&continuation, 178, 14);
    let extended_lifecycle = bind_lifecycle(&continuation, 178, 14);
    let compiled = ReconstructionControls::new(1, 0.5, 0.0);
    let point = hogbom_minor_cycle(
        &point_lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        HogbomControls::for_algorithm(ReconstructionAlgorithm::Clark, compiled)
            .expect("point program"),
    )
    .expect("point solve");
    let zero_scale = hogbom_minor_cycle(
        &scale_lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        HogbomControls::for_algorithm(
            ReconstructionAlgorithm::Multiscale {
                scales_px: vec![0.0],
                small_scale_bias: 0.6,
            },
            compiled,
        )
        .expect("zero-scale program"),
    )
    .expect("zero-scale solve");
    assert_eq!(
        point.delta().expect("point delta").terms(),
        zero_scale.delta().expect("zero-scale delta").terms()
    );

    let extended = hogbom_minor_cycle(
        &extended_lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        HogbomControls::for_algorithm(
            ReconstructionAlgorithm::Multiscale {
                // The 8x8 fixture can contain a 1.5-pixel scale inside CASA's
                // mandatory floor(1.5*scale) search border.
                scales_px: vec![1.5],
                small_scale_bias: 0.6,
            },
            compiled,
        )
        .expect("extended-scale program"),
    )
    .expect("extended-scale solve");
    assert!(
        extended.delta().expect("extended delta").terms().len() > 1,
        "a nonzero scale must distribute model flux over its compact kernel"
    );
}

#[test]
fn window_and_valid_support_constrain_component_placement() {
    // An aligned seed marks one pixel invalid; the solver must skip it.
    let seed = identity(57, 90);
    let invalid_flat = 3 * SHAPE[0] + 3;
    let mut supports = Vec::new();
    let mut seed_values: Vec<Result<casa_imaging_model::ModelSample, ()>> = Vec::new();
    for index in 0..SHAPE[0] * SHAPE[1] {
        let support = if index == invalid_flat {
            ModelSupport::Invalid
        } else {
            ModelSupport::Valid
        };
        supports.push(support);
        let value = casa_imaging_model::ModelValue::new(f64::from(index as i16) * 0.125 - 4.0)
            .expect("finite seed value");
        seed_values.push(if support == ModelSupport::Valid {
            Ok(casa_imaging_model::ModelSample::valid(value))
        } else {
            Ok(casa_imaging_model::ModelSample::invalid())
        });
    }
    let input = ModelInputCommitment::AlignedSeed {
        source: seed,
        support: model_support_identity(supports.iter().copied()),
    };
    let problem = problem_with_model_requirements(58, ModelStateIdentity::Seed(seed), input);
    let mut lifecycle = bind_lifecycle(&problem, 59, 12);
    let seeded = lifecycle
        .ingest_aligned(seed, lifecycle.contract().target(), seed_values)
        .expect("aligned stream")
        .expect("aligned seed ingest");
    assert_eq!(
        seeded
            .read_samples(0..seeded.sample_count())
            .expect("read fixture model")[invalid_flat]
            .support(),
        ModelSupport::Invalid
    );

    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, seeded, None).expect("prepare seeded model");
    let joined = scene(&problem).reconcile(&problem, &mut lifecycle, preparation);
    let (normal_state, _, final_model) = joined.into_parts();

    // The solve needs an open owner: continue through a fresh lifecycle whose
    // commitment names the seeded final generation.
    let continuation = problem_with_model(
        60,
        ModelStateIdentity::Generation(final_model.generation_id().identity()),
    );
    let open = bind_lifecycle(&continuation, 61, 13);

    let solving_controls = HogbomControls::new(0.5, 1.0e-30, 64)
        .expect("valid controls")
        .record_component_sequence(64)
        .expect("recording limit");
    let outcome = hogbom_minor_cycle(
        &open,
        &final_model,
        &normal_state,
        &full_mask(&normal_state, &final_model),
        solving_controls.clone(),
    )
    .expect("bounded solve over seeded support");
    for component in outcome
        .evidence()
        .recorded_component_sequence()
        .expect("recording requested")
    {
        assert_ne!(
            final_model.shape().flat_index(component.cell()),
            Some(invalid_flat),
            "no component may land outside valid support"
        );
    }

    // A mask restricted to the invalid pixel has no valid support at all.
    let invalid_only = box_mask(&normal_state, &final_model, [3, 3], [3, 3]);
    let empty_support = hogbom_minor_cycle(
        &open,
        &final_model,
        &normal_state,
        &invalid_only,
        HogbomControls::new(0.5, 0.0, 8).expect("valid controls"),
    )
    .expect("an empty effective mask is a converged channel, not a solver failure");
    assert_eq!(empty_support.evidence().iterations(), 0);
    assert_eq!(
        empty_support.evidence().stop_reason(),
        MinorCycleStopReason::ThresholdReached
    );

    // Components respect the declared mask support.
    let quarter_mask = box_mask(&normal_state, &final_model, [2, 2], [5, 5]);
    let quarter_outcome = hogbom_minor_cycle(
        &open,
        &final_model,
        &normal_state,
        &quarter_mask,
        solving_controls,
    )
    .expect("quarter-window solve");
    for component in quarter_outcome
        .evidence()
        .recorded_component_sequence()
        .expect("recording requested")
    {
        let pixel = component.cell().pixel();
        assert!(quarter_mask.contains(pixel));
    }
}

#[test]
fn mismatched_lineage_geometry_and_masks_fail_closed() {
    let round = first_confirm_round(62, 63);

    // A generation minted by another lifecycle authority is foreign even with
    // identical content shape.
    let other = problem_with_model(64, ModelStateIdentity::Empty);
    let other_lifecycle = bind_lifecycle(&other, 65, 14);
    let foreign_base = other_lifecycle.initial_empty().expect("foreign generation");
    assert!(matches!(
        hogbom_minor_cycle(
            &other_lifecycle,
            &foreign_base,
            &round.normal_state,
            &full_mask(&round.normal_state, &round.final_model),
            controls(),
        ),
        Err(MinorCycleError::ForeignNormalState)
    ));

    // A base from another model space cannot address this plane. The shape
    // guard fires before the lineage check, so a 12x12 base fails on shape
    // even though the window would still fit.
    let wide_problem = problem_with_model_and_width(66, ModelStateIdentity::Empty, 12);
    let wide_lifecycle = bind_lifecycle(&wide_problem, 67, 15);
    let wide_base = wide_lifecycle.initial_empty().expect("wide generation");
    assert!(matches!(
        hogbom_minor_cycle(
            &wide_lifecycle,
            &wide_base,
            &round.normal_state,
            &full_mask(&round.normal_state, &round.final_model),
            controls(),
        ),
        Err(MinorCycleError::ModelShapeMismatch)
    ));

    // The remaining guards use an open lifecycle bound to this chain.
    let continuation = problem_with_model(
        69,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 67, 15);

    // Static masks must lie inside the plane before solver admission.
    assert!(matches!(
        ReconstructionMask::from_boxes(
            round.normal_state.problem_id(),
            round.final_model.generation_id(),
            mask_coordinate(SHAPE),
            SHAPE,
            [MaskBox::new([6, 6], [10, 10]).expect("ordered box")],
        ),
        Err(casa_imaging_reconstruction::MaskError::OutsideTarget)
    ));
    let _ = lifecycle;
}
