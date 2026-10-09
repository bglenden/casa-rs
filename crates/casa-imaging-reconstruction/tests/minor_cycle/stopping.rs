// SPDX-License-Identifier: LGPL-3.0-or-later

//! Minor-cycle controls and stop rules: control validation, iteration
//! accounting, threshold, nsigma, iteration and staleness bounds, the Clark
//! patch, and informational component-sequence comparison.

use super::*;

/// `SIImageStore::divideResidualByWeight` divides the residual by the
/// published `.sumwt` and the weight image by the PSF gridding's sum (the
/// same number unless the pass published another gridding's, #667); the
/// flat-noise peak the minor cycle cleans follows from both.
#[test]
fn the_minor_cycle_divides_the_residual_by_the_published_sum_and_the_weight_by_the_psf_sum() {
    use casa_imaging_reconstruction::{
        MajorCycleOwner, MinorCycleImageResponse, PassNormalState, WeightingGenerationId,
        runtime_adapter::NormalStoragePlan,
    };
    let problem = problem_with_model(211, ModelStateIdentity::Empty);
    let mut lifecycle = bind_lifecycle(&problem, 212, 1);
    let empty = lifecycle.initial_empty().expect("empty model");
    let preparation = MajorCyclePreparation::prepare(&lifecycle, empty, None).expect("prepare");
    let mut images = scene(&problem).pass_images(preparation.final_model(), true);
    let cells = SHAPE[0] * SHAPE[1];
    images.sum_weights = vec![8.0];
    images.published_sum_weights = vec![3.0];
    images.weight = Some(vec![16.0; cells]);
    images.residual.fill(0.0);
    images.residual[4 * SHAPE[1] + 4] = 8.0;
    images.psf = Some(vec![0.0; cells]);
    images.psf.as_mut().unwrap()[4 * SHAPE[1] + 4] = 8.0;
    let mut pass = PassNormalState::initial(
        &problem,
        WeightingGenerationId::next(),
        preparation.final_model_generation(),
        NormalStoragePlan::resident(1).expect("storage"),
    )
    .expect("pass");
    pass.append(images).expect("images");
    let joined =
        MajorCycleOwner::from_complete_data(pass.finish(2, 1).expect("complete"), preparation)
            .expect("owner")
            .reconcile(&mut lifecycle)
            .expect("reconcile");
    let (normal, _, model) = joined.into_parts();
    let next = problem_with_model(
        213,
        ModelStateIdentity::Generation(model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&next, 214, 2);
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let result = hogbom_minor_cycle(
        &lifecycle,
        &model,
        &normal,
        &full_mask(&normal, &model),
        HogbomControls::new(0.25, 0.0, 1)
            .expect("controls")
            .with_image_response(response),
    )
    .expect("minor cycle");
    // (8 / 3) / (√(16 / 8) · √(16 / 8)) = 4/3 Jy; one iteration at gain
    // 0.25 removes a third of a Jansky.
    assert!((result.evidence().initial_peak_flux() - 4.0 / 3.0).abs() < 1e-6);
    assert!((result.evidence().total_flux() - 1.0 / 3.0).abs() < 1e-6);
}

#[test]
fn controls_are_validated_explicitly() {
    let compiled = HogbomControls::from_compiled(ReconstructionControls::new(8, 0.5, 0.0))
        .expect("exact compiled Högbom view");
    assert_eq!(compiled.validity(), MinorCycleValidity::Exact);
    assert_eq!(
        compiled.hogbom_iteration_accounting(),
        HogbomIterationAccounting::Strict
    );
    let compiled = HogbomControls::from_compiled(
        ReconstructionControls::new(100, 0.5, 0.0)
            .with_cycle_limits(7, Some(3))
            .with_noise_sigma(4.5),
    )
    .expect("cycle and nsigma controls");
    assert_eq!(compiled.max_iterations(), 7);
    assert_eq!(compiled.noise_sigma(), Some(4.5));
    assert!(matches!(
        HogbomControls::new(0.0, 1.0, 8),
        Err(MinorCycleError::InvalidGain)
    ));
    assert!(matches!(
        HogbomControls::new(1.25, 1.0, 8),
        Err(MinorCycleError::InvalidGain)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, -1.0, 8),
        Err(MinorCycleError::InvalidThreshold)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, f64::NAN, 8),
        Err(MinorCycleError::InvalidThreshold)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, 0.0, 0),
        Err(MinorCycleError::InvalidIterationBound)
    ));
    assert!(matches!(
        HogbomControls::new_bounded(0.5, 0.0, 8, 0.0),
        Err(MinorCycleError::InvalidValidityBound)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, 0.0, 8)
            .expect("controls")
            .record_component_sequence(0),
        Err(MinorCycleError::InvalidRecordingLimit)
    ));
    let valid = HogbomControls::new_bounded(0.1, 0.5, 7, 3.25).expect("valid controls");
    assert_eq!(
        (
            valid.gain(),
            valid.threshold(),
            valid.max_iterations(),
            valid.validity(),
            valid.component_sequence_limit()
        ),
        (
            0.1,
            0.5,
            7,
            MinorCycleValidity::Bounded {
                maximum_absolute_update: 3.25
            },
            None
        )
    );
    let reused_plane = valid
        .on_model_plane(MinorCycleModelPlane::new(2, 3, 1))
        .model_plane();
    assert_eq!(
        (
            reused_plane.domain(),
            reused_plane.coefficient(),
            reused_plane.polarization(),
        ),
        (2, 3, 1),
        "the shared loop carries typed domain/coefficient/polarization coordinates"
    );
}

#[test]
fn exact_hogbom_view_is_not_stopped_by_cumulative_component_flux() {
    let round = first_confirm_round_scaled(198, 199, 1_000.0);
    let continuation = problem_with_model(
        200,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 201, 20);
    let program = HogbomControls::from_compiled(
        ReconstructionControls::new(50, 0.1, 0.0).with_cycle_limits(50, None),
    )
    .expect("exact Högbom program");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        program,
    )
    .expect("exact Högbom solve");

    assert_eq!(outcome.evidence().iterations(), 50);
    assert_eq!(outcome.evidence().controller_iterations(), 50);
    assert_eq!(
        outcome.evidence().stop_reason(),
        MinorCycleStopReason::IterationBound
    );
    assert!(
        outcome.evidence().total_flux() > 100.0,
        "fixture must cross the displaced arbitrary 100 Jy cap"
    );
}

#[test]
fn casa_inclusive_hogbom_executes_51_but_charges_50_per_bound_stopped_cycle() {
    let round = first_confirm_round_scaled(202, 203, 1_000.0);
    let continuation = problem_with_model(
        204,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 205, 21);
    let program = HogbomControls::from_compiled(
        ReconstructionControls::new(500, 0.1, 0.0)
            .with_cycle_limits(50, None)
            .with_hogbom_iteration_accounting(HogbomIterationAccounting::CasaInclusive),
    )
    .expect("CASA-inclusive Högbom program")
    .record_component_sequence(64)
    .expect("bounded component diagnostics");

    let mut actual_total = 0_usize;
    let mut controller_total = 0_usize;
    for _ in 0..10 {
        let outcome = hogbom_minor_cycle(
            &lifecycle,
            &round.final_model,
            &round.normal_state,
            &full_mask(&round.normal_state, &round.final_model),
            program.clone(),
        )
        .expect("CASA-inclusive Högbom solve");
        let evidence = outcome.evidence();
        assert_eq!(evidence.iterations(), 51);
        assert_eq!(evidence.controller_iterations(), 50);
        assert_eq!(evidence.stop_reason(), MinorCycleStopReason::IterationBound);
        assert_eq!(
            evidence
                .recorded_component_sequence()
                .expect("all inclusive components are retained")
                .len(),
            51
        );
        actual_total += evidence.iterations();
        controller_total += evidence.controller_iterations();
    }
    assert_eq!(actual_total, 510);
    assert_eq!(controller_total, 500);
}

#[test]
fn threshold_stop_converges_without_a_delta_or_a_reconciliation_request() {
    let round = first_confirm_round(45, 46);
    let continuation = problem_with_model(
        47,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 48, 9);
    let converging_controls = HogbomControls::new(0.5, 1.0e12, 32)
        .expect("valid controls")
        .record_component_sequence(8)
        .expect("recording limit");
    let mask = box_mask(&round.normal_state, &round.final_model, [2, 2], [5, 5]);

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &mask,
        converging_controls.clone(),
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    assert_eq!(
        evidence.stop_reason(),
        MinorCycleStopReason::ThresholdReached
    );
    assert_eq!(evidence.iterations(), 0);
    assert!(!evidence.requests_reconciliation());
    assert!(outcome.delta().is_none());
    assert!(evidence.recorded_component_sequence().is_none());

    // Reusing the same input owners gives the same solve evidence.
    let other_lifecycle = bind_lifecycle(&continuation, 48, 9);
    let repeat = hogbom_minor_cycle(
        &other_lifecycle,
        &round.final_model,
        &round.normal_state,
        &mask,
        converging_controls,
    )
    .expect("repeat solve");
    assert_eq!(
        repeat.evidence().evidence_id(),
        evidence.evidence_id(),
        "the repeated solve uses the same mask owner"
    );
    assert!(repeat.evidence().first_divergence(evidence).is_none());
}

#[test]
fn nsigma_and_cycle_iteration_limits_are_solver_evidence_not_frontend_policy() {
    let round = first_confirm_round(170, 171);
    let continuation = problem_with_model(
        172,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 173, 17);
    let program = HogbomControls::from_compiled(
        ReconstructionControls::new(64, 0.5, 0.0)
            .with_cycle_limits(2, Some(4))
            .with_noise_sigma(1.0)
            .with_cycle_threshold(1.0, 0.05, 0.8),
    )
    .expect("compiled cycle controls");
    let result = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        program,
    )
    .expect("nsigma solve");
    assert!(result.evidence().iterations() <= 2);
    let rms = result.evidence().noise_rms().expect("nsigma records RMS");
    assert!(rms.is_finite() && rms >= 0.0);
    let cycle_threshold = result
        .evidence()
        .cycle_threshold()
        .expect("cycle threshold is recorded");
    assert!(cycle_threshold.is_finite() && cycle_threshold >= 0.0);
    assert_eq!(
        result.evidence().effective_threshold(),
        rms.max(cycle_threshold)
    );
}

#[test]
fn iteration_bound_stops_with_an_explicit_reconciliation_request() {
    let round = first_confirm_round(49, 50);
    let continuation = problem_with_model(
        51,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 52, 10);
    let bounded = HogbomControls::new(0.5, 0.0, 1)
        .expect("valid controls")
        .record_component_sequence(8)
        .expect("recording limit");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        bounded.clone(),
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    assert_eq!(evidence.stop_reason(), MinorCycleStopReason::IterationBound);
    assert_eq!(evidence.iterations(), 1);
    assert!(evidence.requests_reconciliation());
    assert!(outcome.delta().is_some());
}

#[test]
fn staleness_bound_rejects_the_candidate_before_any_state_advances() {
    let round = first_confirm_round(53, 54);
    let continuation = problem_with_model(
        55,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 56, 11);
    // The very first candidate already exceeds the cumulative update
    // ceiling, so nothing may be applied at all.
    let tight = HogbomControls::new_bounded(0.5, 0.0, 64, 1.0e-300)
        .expect("valid controls")
        .record_component_sequence(8)
        .expect("recording limit");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        tight,
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    assert_eq!(evidence.stop_reason(), MinorCycleStopReason::StalenessBound);
    assert_eq!(evidence.iterations(), 0);
    assert_eq!(evidence.total_flux(), 0.0);
    assert!(outcome.delta().is_none());
    assert!(
        evidence
            .recorded_component_sequence()
            .unwrap_or(&[])
            .is_empty()
    );
    assert!(evidence.requests_reconciliation());
}

#[test]
fn returned_deltas_never_exceed_the_accepted_view_envelope() {
    let round = first_confirm_round(61, 62);
    let continuation = problem_with_model(
        63,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 64, 12);
    let envelope = residual_peak(
        round
            .normal_state
            .read_window(0..1)
            .expect("single-plane fixture window")
            .residual()
            .complex()
            .unwrap(),
    ) * 0.75;
    let bounded = HogbomControls::new_bounded(0.5, 0.0, 64, envelope)
        .expect("valid controls")
        .record_component_sequence(64)
        .expect("recording limit");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        bounded.clone(),
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    let MinorCycleValidity::Bounded {
        maximum_absolute_update,
    } = bounded.validity()
    else {
        panic!("test program must be bounded");
    };
    assert!(
        evidence.total_flux() <= maximum_absolute_update,
        "cumulative flux {} exceeded the accepted envelope {}",
        evidence.total_flux(),
        maximum_absolute_update
    );
    if let Some(delta) = outcome.delta() {
        let delta_flux: f64 = delta
            .terms()
            .iter()
            .map(|term| term.increment().value().abs())
            .sum();
        assert!(
            delta_flux <= maximum_absolute_update + f64::EPSILON * delta_flux.abs(),
            "delta terms sum to {delta_flux}, beyond the accepted envelope"
        );
    }
    assert_eq!(evidence.stop_reason(), MinorCycleStopReason::StalenessBound);
}

#[test]
fn threshold_boundary_follows_the_casa_hogbom_convention() {
    // casacore LatticeCleaner stops only for abs(strength) strictly below
    // the threshold; a peak exactly at the threshold cleans one component.
    let round = first_confirm_round(65, 66);
    let continuation = problem_with_model(
        67,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 68, 13);

    // Recompute the solver's first normalized peak exactly: same scan over
    // the same private copy, so the equality case is bit-exact.
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
    let strength = residual_peak(window.residual().complex().unwrap()) / psf_peak;

    let solve = |threshold: f64| {
        hogbom_minor_cycle(
            &lifecycle,
            &round.final_model,
            &round.normal_state,
            &full_mask(&round.normal_state, &round.final_model),
            HogbomControls::new(0.5, threshold, 64).expect("valid controls"),
        )
        .expect("boundary solve")
    };

    // Above the peak: converges before cleaning anything.
    let above = solve(strength * 1.01);
    assert_eq!(
        above.evidence().stop_reason(),
        MinorCycleStopReason::ThresholdReached
    );
    assert_eq!(above.evidence().iterations(), 0);
    assert!(above.delta().is_none());

    // Exactly at the peak: not a stop; one component is cleaned and the
    // reduced peak then falls below the threshold.
    let equal = solve(strength);
    assert_eq!(
        equal.evidence().stop_reason(),
        MinorCycleStopReason::ThresholdReached
    );
    assert_eq!(equal.evidence().iterations(), 1);
    assert!(equal.delta().is_some());
    assert!(equal.evidence().requests_reconciliation());

    let inclusive_equal = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        HogbomControls::from_compiled(
            ReconstructionControls::new(64, 0.5, strength)
                .with_hogbom_iteration_accounting(HogbomIterationAccounting::CasaInclusive),
        )
        .expect("CASA-inclusive threshold-boundary controls"),
    )
    .expect("CASA-inclusive threshold-boundary solve");
    assert_eq!(inclusive_equal.evidence().iterations(), 1);
    assert_eq!(inclusive_equal.evidence().controller_iterations(), 1);
    assert_eq!(
        inclusive_equal.evidence().stop_reason(),
        MinorCycleStopReason::ThresholdReached,
        "an early scientific stop reports the actual count without clamping"
    );

    // Strictly below the peak: the first component is cleaned too.
    let below = solve(strength * 0.99);
    assert_eq!(
        below.evidence().stop_reason(),
        MinorCycleStopReason::ThresholdReached
    );
    assert!(below.evidence().iterations() >= 1);
    assert!(below.evidence().requests_reconciliation());
}

#[test]
fn clark_uses_a_derived_bounded_patch_and_stops_at_or_below_threshold() {
    let round = first_confirm_round(165, 166);
    let continuation = problem_with_model(
        167,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 168, 13);
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
    let strength = residual_peak(window.residual().complex().unwrap()) / psf_peak;
    for threshold in [strength, strength * 2.0] {
        let program = casa_imaging_reconstruction::MinorCycleProgram::for_algorithm(
            ReconstructionAlgorithm::Clark,
            ReconstructionControls::new(8, 0.5, threshold),
        )
        .expect("Clark program");

        let result = hogbom_minor_cycle(
            &lifecycle,
            &round.final_model,
            &round.normal_state,
            &full_mask(&round.normal_state, &round.final_model),
            program,
        )
        .expect("Clark threshold-boundary solve");

        assert_eq!(result.evidence().iterations(), 0);
        assert_eq!(
            result.evidence().stop_reason(),
            MinorCycleStopReason::ThresholdReached
        );
        let approximation = result
            .evidence()
            .clark_approximation()
            .expect("Clark records its approximation");
        assert!(approximation.radius().into_iter().all(|radius| radius > 0));
        assert!(approximation.maximum_exterior_sidelobe().is_finite());
        assert!(result.delta().is_none());
    }
}

#[test]
fn component_sequence_divergence_is_informational_only() {
    let round = first_confirm_round(70, 71);
    let continuation = problem_with_model(
        72,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let first = bind_lifecycle(&continuation, 73, 16);
    let second = bind_lifecycle(&continuation, 73, 16);

    let baseline = hogbom_minor_cycle(
        &first,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        controls(),
    )
    .expect("baseline solve");
    let halved_gain = HogbomControls::new(0.25, 1.0e-30, 64)
        .expect("valid controls")
        .record_component_sequence(64)
        .expect("recording limit");
    let candidate = hogbom_minor_cycle(
        &second,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        halved_gain,
    )
    .expect("candidate solve");

    // Different gains diverge immediately, but the comparison is purely
    // informational: it reports facts and never fails either evidence.
    let divergence = candidate
        .evidence()
        .first_divergence(baseline.evidence())
        .expect("different gains diverge");
    let index = divergence.index();
    let b = divergence.baseline().expect("baseline entry present");
    let c = divergence.candidate().expect("candidate entry present");
    assert_eq!(index, 0);
    assert_eq!(b.cell(), c.cell(), "the peak pixel is gain-independent");
    assert_ne!(b.flux().to_bits(), c.flux().to_bits());
    assert!((c.flux() - b.flux() / 2.0).abs() <= 1.0e-12 * b.flux().abs());

    // Sequence-length differences report the terminal divergence.
    let shorter_controls = HogbomControls::new(0.5, 1.0e-30, 64)
        .expect("valid controls")
        .record_component_sequence(1)
        .expect("recording limit");
    let third = bind_lifecycle(&continuation, 73, 16);
    let shorter = hogbom_minor_cycle(
        &third,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        shorter_controls,
    )
    .expect("short-recording solve");
    let terminal = shorter
        .evidence()
        .first_divergence(baseline.evidence())
        .expect("lengths differ");
    assert_eq!(terminal.index(), 1);
    assert!(terminal.baseline().is_some());
    assert!(terminal.candidate().is_none());

    // Unrecorded sequences simply cannot be compared.
    let unrecorded_controls = HogbomControls::new(0.5, 1.0e-30, 64).expect("no recording");
    let fourth = bind_lifecycle(&continuation, 73, 16);
    let unrecorded = hogbom_minor_cycle(
        &fourth,
        &round.final_model,
        &round.normal_state,
        &full_mask(&round.normal_state, &round.final_model),
        unrecorded_controls,
    )
    .expect("unrecorded solve");
    assert!(
        unrecorded
            .evidence()
            .first_divergence(baseline.evidence())
            .is_none()
    );
}
