// SPDX-License-Identifier: LGPL-3.0-or-later

//! T20 reconciliation of complete-data normal states with the model
//! lifecycle: owned storage hand-off, completion records and lineage,
//! model-dependent residual content, residual refreshes, and the passes that
//! must never become a Major-Cycle owner.

use super::*;

#[test]
fn normal_reconciliation_transfers_owned_storage_without_reading_arrays() {
    use casa_imaging_reconstruction::runtime_adapter::{NormalArrayStorage, NormalStorageFactory};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Debug)]
    struct Factory(Arc<AtomicUsize>);
    #[derive(Debug)]
    struct Storage {
        values: Vec<f64>,
        reads: Arc<AtomicUsize>,
    }
    impl NormalStorageFactory for Factory {
        fn create(
            &self,
            _domain: usize,
            scalars: usize,
        ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError> {
            Ok(Box::new(Storage {
                values: vec![0.0; scalars],
                reads: self.0.clone(),
            }))
        }
    }
    impl NormalArrayStorage for Storage {
        fn len(&self) -> usize {
            self.values.len()
        }
        fn read(
            &self,
            start: usize,
            len: usize,
        ) -> Result<std::borrow::Cow<'_, [f64]>, SpectralOperatorError> {
            self.reads.fetch_add(len, Ordering::Relaxed);
            Ok(std::borrow::Cow::Borrowed(&self.values[start..start + len]))
        }
        fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
            self.values[start..start + values.len()].copy_from_slice(values);
            Ok(())
        }
    }
    let problem = t38_cube_problem(238);
    let mut lifecycle = bind_lifecycle(&problem, attempt(239));
    let initial = lifecycle.initial_empty().unwrap();
    let preparation = MajorCyclePreparation::prepare(&lifecycle, initial, None).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let storage = NormalStoragePlan::new(Arc::new(Factory(reads.clone())), 2).unwrap();
    let stored = scene(&problem).initial_with(
        &problem,
        &preparation,
        storage,
        WeightingGenerationId::next(),
    );
    reads.store(0, Ordering::Relaxed);
    let joined = MajorCycleOwner::from_complete_data(stored, preparation)
        .unwrap()
        .reconcile(&mut lifecycle)
        .unwrap();
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "completion must not authorize trusted ownership by rereading normal arrays"
    );
    joined.normal_state().diagnostic_content_identity().unwrap();
    assert!(
        reads.load(Ordering::Relaxed) > 0,
        "an explicitly requested diagnostic still reads and fingerprints arrays"
    );
}

#[test]
fn schema_versions_record_the_t20_completion_records() {
    assert_eq!(FinalModelCompletionId::SCHEMA_VERSION, 2);
    assert_eq!(
        casa_imaging_reconstruction::FinalNormalStateCompletionId::SCHEMA_VERSION,
        4
    );
    assert_eq!(
        casa_imaging_reconstruction::MajorCycleCompletionId::SCHEMA_VERSION,
        2
    );
}

#[test]
fn reconciliation_applies_one_pending_delta_through_the_model_owner() {
    let problem = t19_compatible_problem(11);
    let scene = scene(&problem);
    let mut lifecycle = bind_lifecycle(&problem, attempt(21));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(1), delta_value(-2.5))])
        .expect("pending Högbom-style delta");
    let delta_id = delta.delta_id();
    let preparation = MajorCyclePreparation::prepare(&lifecycle, named, Some(delta))
        .expect("prepare final model");
    let complete = scene.initial(&problem, &preparation);
    let sample_count = complete.completion().sample_count();
    let block_count = complete.completion().block_count();
    let weighting_generation = complete.completion().weighting_generation();

    let owner =
        MajorCycleOwner::from_complete_data(complete, preparation).expect("T20 owner from T19");
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
    // relabels the data side of the same sky.
    assert_ne!(
        normal_state.diagnostic_content_identity().unwrap(),
        confirm_content(&problem, &scene)
    );
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
    assert!(
        joined
            .final_model()
            .read_samples(0..joined.final_model().sample_count())
            .expect("read fixture model")[1]
            .value()
            .value()
            == -2.5
    );

    // The pending delta was applied only through the model owner.
    assert_eq!(model_completion.delta(), Some(delta_id));
    assert_eq!(model_completion.attempt(), attempt(21));
    assert_eq!(model_completion.epoch(), 7);
}

#[test]
fn reconciliation_without_a_pending_delta_confirms_the_named_generation_final() {
    let problem = t19_compatible_problem(12);
    let scene = scene(&problem);
    let mut lifecycle = bind_lifecycle(&problem, attempt(22));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let input_id = named.generation_id();
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare final model");
    let complete = scene.initial(&problem, &preparation);
    let pass_residual = complete
        .read_window(0..1)
        .expect("single-plane pass window")
        .primitives()
        .dirty()
        .complex()
        .unwrap()
        .to_vec();

    let joined = MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("confirm-only reconciliation");

    assert_eq!(joined.model_completion().delta(), None);
    assert_eq!(joined.model_completion().base(), input_id);
    assert_eq!(joined.model_completion().generation(), input_id);
    assert_eq!(joined.normal_state().input_model_generation(), input_id);
    assert_eq!(joined.normal_state().final_model_generation(), input_id);
    // An empty final model reconciles to the exact pass residual bit-for-bit,
    // and its content identity matches an independent confirm pass over the
    // same sky.
    assert_eq!(
        joined.normal_state().diagnostic_content_identity().unwrap(),
        confirm_content(&problem, &scene)
    );
    let window = joined
        .normal_state()
        .read_window(0..1)
        .expect("single-plane fixture window");
    assert_eq!(window.residual().complex().unwrap(), pass_residual);
    assert_eq!(
        window.normal_approximation().complex().unwrap().len(),
        8 * 8
    );
    assert_eq!(window.sensitivity().dense().unwrap().len(), 8 * 8);
    assert!(joined.normal_state().sum_weight() > 0.0);
}

#[test]
fn empty_origin_residual_refresh_reproduces_the_initial_content() {
    let problem = t19_compatible_problem(72);
    let scene = scene(&problem);
    let mut initial_lifecycle = bind_lifecycle(&problem, attempt(73));
    let initial_model = initial_lifecycle.initial_empty().expect("empty model");
    let initial_preparation =
        MajorCyclePreparation::prepare(&initial_lifecycle, initial_model, None)
            .expect("initial preparation");
    let initial_join = scene.reconcile(&problem, &mut initial_lifecycle, initial_preparation);
    let initial_content = initial_join
        .normal_state()
        .diagnostic_content_identity()
        .unwrap();
    let (initial_normal, continuation) = initial_join.into_continuation();
    let (mut continued_lifecycle, carried_model) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("continued problem"),
        attempt(74),
        2,
        continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue empty model");
    assert_eq!(
        carried_model.origin(),
        casa_imaging_reconstruction::ModelGenerationOrigin::Empty
    );
    let refresh_preparation =
        MajorCyclePreparation::prepare(&continued_lifecycle, carried_model, None)
            .expect("refresh preparation");
    let refreshed = scene.reconcile_refresh(
        &problem,
        &mut continued_lifecycle,
        initial_normal,
        refresh_preparation,
    );
    assert_eq!(
        refreshed
            .normal_state()
            .diagnostic_content_identity()
            .unwrap(),
        initial_content
    );
}

#[test]
fn chained_no_delta_final_major_cycles_reauthorize_the_carried_generation() {
    let problem = t19_compatible_problem(13);
    let scene = scene(&problem);
    let mut initial_lifecycle = bind_lifecycle(&problem, attempt(23));
    let initial_named = initial_lifecycle
        .initial_empty()
        .expect("initial empty generation");
    let initial_samples = initial_named
        .read_samples(0..initial_named.sample_count())
        .expect("read fixture model");
    let initial_id = initial_named.generation_id();
    let initial_preparation =
        MajorCyclePreparation::prepare(&initial_lifecycle, initial_named, None).unwrap();
    let initial_join = scene.reconcile(&problem, &mut initial_lifecycle, initial_preparation);
    let (initial_normal, initial_continuation) = initial_join.into_continuation();

    let (mut first_lifecycle, first_named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone())
            .expect("first continued executable problem"),
        attempt(24),
        8,
        initial_continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue the initial no-delta generation");
    assert_eq!(first_named.generation_id(), initial_id);
    let first_preparation = MajorCyclePreparation::prepare(&first_lifecycle, first_named, None)
        .expect("prepare the first no-delta final major cycle");
    let first_join = scene.reconcile_refresh(
        &problem,
        &mut first_lifecycle,
        initial_normal,
        first_preparation,
    );
    assert_eq!(first_join.model_completion().base(), initial_id);
    assert_eq!(first_join.model_completion().delta(), None);
    assert_eq!(
        first_join
            .final_model()
            .read_samples(0..first_join.final_model().sample_count())
            .expect("read fixture model"),
        initial_samples
    );
    let first_id = first_join.final_model().generation_id();
    assert_ne!(
        first_id, initial_id,
        "the first continued attempt must own its confirmed generation"
    );
    let (first_normal, first_continuation) = first_join.into_continuation();

    let (mut second_lifecycle, second_named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone())
            .expect("second continued executable problem"),
        attempt(25),
        9,
        first_continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue the first no-delta final-major generation");
    assert_eq!(second_named.generation_id(), first_id);
    let second_preparation = MajorCyclePreparation::prepare(&second_lifecycle, second_named, None)
        .expect("prepare the second no-delta final major cycle");
    let second_join = scene.reconcile_refresh(
        &problem,
        &mut second_lifecycle,
        first_normal,
        second_preparation,
    );
    assert_eq!(second_join.model_completion().base(), first_id);
    assert_eq!(second_join.model_completion().delta(), None);
    assert_eq!(
        second_join
            .final_model()
            .read_samples(0..second_join.final_model().sample_count())
            .expect("read fixture model"),
        initial_samples
    );
    let second_id = second_join.final_model().generation_id();
    assert_ne!(
        second_id, first_id,
        "each continued attempt must own its confirmed generation"
    );
    let (_, second_continuation) = second_join.into_continuation();

    let (_, third_named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem).expect("third continued executable problem"),
        attempt(26),
        10,
        second_continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue the second no-delta final-major generation");
    assert_eq!(third_named.generation_id(), second_id);
}

#[test]
fn residual_content_depends_on_the_exact_final_model() {
    let problem = t19_compatible_problem(27);
    let scene = scene(&problem);
    // The same sky reconciled against two different final models.
    let mut empty_lifecycle = bind_lifecycle(&problem, attempt(28));
    let empty_named = empty_lifecycle
        .initial_empty()
        .expect("empty named generation");
    let empty_preparation = MajorCyclePreparation::prepare(&empty_lifecycle, empty_named, None)
        .expect("prepare empty final model");
    let empty_join = scene.reconcile(&problem, &mut empty_lifecycle, empty_preparation);

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
    let delta_preparation =
        MajorCyclePreparation::prepare(&delta_lifecycle, delta_named, Some(delta))
            .expect("prepare delta final model");
    let delta_join = scene.reconcile(&problem, &mut delta_lifecycle, delta_preparation);

    assert_ne!(
        empty_join
            .normal_state()
            .diagnostic_content_identity()
            .unwrap(),
        delta_join
            .normal_state()
            .diagnostic_content_identity()
            .unwrap(),
        "a nonzero final model must change the authoritative residual content"
    );
    assert_ne!(
        empty_join
            .normal_state()
            .read_window(0..1)
            .expect("single-plane fixture window")
            .residual()
            .complex()
            .unwrap(),
        delta_join
            .normal_state()
            .read_window(0..1)
            .expect("single-plane fixture window")
            .residual()
            .complex()
            .unwrap(),
        "the retained authoritative residual must depend on the final model"
    );
    assert_ne!(empty_join.completion_id(), delta_join.completion_id());
    assert_ne!(
        empty_join.normal_state().final_model_generation(),
        delta_join.normal_state().final_model_generation()
    );
}

#[test]
fn completion_ids_distinguish_owners_while_science_and_replay_remain_stable() {
    let problem = t19_compatible_problem(30);
    let scene = scene(&problem);
    // Two independent reconciliation passes over the same sky, each with its
    // own process-local lifecycle allocation and weighting generation but
    // the same stable problem/attempt/epoch binding.
    let mut first = bind_lifecycle(&problem, attempt(31));
    let mut second = bind_lifecycle(&problem, attempt(31));
    let first_named = first.initial_empty().expect("first named generation");
    let second_named = second.initial_empty().expect("second named generation");
    let first_preparation =
        MajorCyclePreparation::prepare(&first, first_named, None).expect("first preparation");
    let second_preparation =
        MajorCyclePreparation::prepare(&second, second_named, None).expect("second preparation");

    let first_join = scene.reconcile(&problem, &mut first, first_preparation);
    let second_join = scene.reconcile(&problem, &mut second, second_preparation);

    assert_ne!(
        first_join.completion_id(),
        second_join.completion_id(),
        "completion IDs retain the unique model owner association"
    );
    assert_ne!(
        first_join.normal_state().completion_id(),
        second_join.normal_state().completion_id()
    );
    let first_normal = first_join.normal_state();
    let second_normal = second_join.normal_state();
    assert_eq!(first_normal.problem_id(), second_normal.problem_id());
    assert_ne!(
        first_normal.weighting_generation(),
        second_normal.weighting_generation()
    );
    assert_ne!(first_normal.replay_id(), second_normal.replay_id());
    assert_eq!(
        first_normal.diagnostic_content_identity().unwrap(),
        second_normal.diagnostic_content_identity().unwrap()
    );
    assert_eq!(
        first_join
            .final_model()
            .read_samples(0..first_join.final_model().sample_count())
            .unwrap(),
        second_join
            .final_model()
            .read_samples(0..second_join.final_model().sample_count())
            .unwrap()
    );
    for joined in [&first_join, &second_join] {
        assert_eq!(joined.model_completion().attempt(), attempt(31));
        assert_eq!(joined.model_completion().epoch(), 7);
        assert_eq!(
            joined.model_completion().generation(),
            joined.final_model().generation_id()
        );
        assert_eq!(
            joined.normal_state().final_model_generation(),
            joined.final_model().generation_id()
        );
    }
}

#[test]
fn reconciliation_fails_atomically_and_leaves_both_authorities_intact() {
    let problem = t19_compatible_problem(13);
    let other_problem = t19_compatible_problem(14);
    let scene = scene(&problem);

    // A lifecycle bound to another compiled problem is stale model evidence.
    let mut foreign_problem_lifecycle = bind_lifecycle(&other_problem, attempt(23));
    let foreign_named = foreign_problem_lifecycle
        .initial_empty()
        .expect("foreign empty generation");
    let foreign_preparation =
        MajorCyclePreparation::prepare(&foreign_problem_lifecycle, foreign_named, None)
            .expect("prepare foreign problem model");
    let foreign_complete = scene.initial(&problem, &foreign_preparation);
    let stale_problem = MajorCycleOwner::from_complete_data(foreign_complete, foreign_preparation)
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
    let preparation = MajorCyclePreparation::prepare(&lifecycle, named, Some(delta))
        .expect("prepare final model");
    let joined = scene.reconcile(&problem, &mut lifecycle, preparation);
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
fn incomplete_or_inconsistent_passes_cannot_become_a_major_cycle_owner() {
    let problem = t19_compatible_problem(15);
    let scene = scene(&problem);
    let lifecycle = bind_lifecycle(&problem, attempt(16));
    let prepare = || {
        MajorCyclePreparation::prepare(
            &lifecycle,
            lifecycle.initial_empty().expect("empty named generation"),
            None,
        )
        .expect("prepare empty final model")
    };
    let preparation = prepare();
    let state = || {
        PassNormalState::initial(
            &problem,
            WeightingGenerationId::next(),
            preparation.final_model_generation(),
            NormalStoragePlan::resident(1).expect("resident normal storage"),
        )
        .expect("initial pass state")
    };
    let images = || scene.pass_images(preparation.final_model(), true);

    // Every image domain must be appended before the state completes.
    assert!(matches!(
        state().finish(4, 1),
        Err(SpectralOperatorError::IncompleteCoverage)
    ));

    // A non-finite generated value or sumwt is rejected, and a rejected
    // append leaves the state as it was.
    let mut nonfinite = state();
    let mut nan_residual = images();
    nan_residual.residual[0] = f32::NAN;
    assert!(matches!(
        nonfinite.append(nan_residual),
        Err(SpectralOperatorError::GeneratedNonfinite)
    ));
    let mut infinite_sumwt = images();
    infinite_sumwt.sum_weights[0] = f64::INFINITY;
    assert!(matches!(
        nonfinite.append(infinite_sumwt),
        Err(SpectralOperatorError::GeneratedNonfinite)
    ));
    nonfinite.append(images()).expect("finite images");
    nonfinite
        .finish(4, 1)
        .expect("the state survives rejected appends");

    // An initial pass carries PSF moments, names a compiled domain and covers
    // the whole spectral axis with consistently sized planes.
    let mut without_psf = images();
    without_psf.psf = None;
    assert!(matches!(
        state().append(without_psf),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
    let mut foreign_domain = images();
    foreign_domain.domain = 1;
    assert!(matches!(
        state().append(foreign_domain),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
    let mut short = images();
    short.residual.pop();
    assert!(matches!(
        state().append(short),
        Err(SpectralOperatorError::InvalidSlab)
    ));
    let mut beyond_axis = images();
    beyond_axis.channels = 0..2;
    assert!(matches!(
        state().append(beyond_axis),
        Err(SpectralOperatorError::InvalidSlab)
    ));
    // Planes must have the compiled domain's shape and polarization count,
    // even when their lengths agree with each other.
    let mut transposed = images();
    let [width, height] = transposed.shape;
    transposed.shape = [width * height, 1];
    assert!(matches!(
        state().append(transposed),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
    let mut extra_polarization = images();
    extra_polarization.polarizations += 1;
    assert!(matches!(
        state().append(extra_polarization),
        Err(SpectralOperatorError::ProblemMismatch)
    ));

    // A pass that proves no traversal cannot become an owner.
    let mut untraversed = state();
    untraversed.append(images()).expect("complete images");
    let untraversed = untraversed.finish(0, 0).expect("assembled normal state");
    assert!(matches!(
        MajorCycleOwner::from_complete_data(untraversed, prepare()),
        Err(MajorCycleError::IncompleteCoverage)
    ));

    // The residual must belong to the prepared final model.
    let complete = scene.initial(&problem, &preparation);
    assert_eq!(complete.completion().problem_id(), problem.problem_id());
    assert!(complete.completion().sample_count() > 0 && complete.completion().block_count() > 0);
    assert!(matches!(
        MajorCycleOwner::from_complete_data(complete, prepare()),
        Err(MajorCycleError::Residual(
            SpectralOperatorError::ModelMismatch
        ))
    ));

    // A residual refresh takes residual planes only.
    let mut owner_lifecycle = bind_lifecycle(&problem, attempt(17));
    let named = owner_lifecycle.initial_empty().expect("owner generation");
    let owner_preparation =
        MajorCyclePreparation::prepare(&owner_lifecycle, named, None).expect("owner preparation");
    let (previous, _) = scene
        .reconcile(&problem, &mut owner_lifecycle, owner_preparation)
        .into_continuation();
    let mut refresh = PassNormalState::refresh(
        &problem,
        previous,
        preparation.final_model_generation(),
        NormalStoragePlan::resident(1).expect("resident normal storage"),
    )
    .expect("refresh pass state");
    assert!(matches!(
        refresh.append(images()),
        Err(SpectralOperatorError::ProblemMismatch)
    ));

    // A refresh keeps the previous PSF and sumwt, so it carries the
    // weighting generation they were formed with, must place the samples the
    // previous state placed, and refreshes only the previous state's own
    // problem.
    let previous_state = || {
        let mut lifecycle = bind_lifecycle(&problem, attempt(18));
        let named = lifecycle.initial_empty().expect("previous generation");
        let preparation =
            MajorCyclePreparation::prepare(&lifecycle, named, None).expect("previous preparation");
        scene
            .reconcile(&problem, &mut lifecycle, preparation)
            .into_continuation()
            .0
    };
    let refreshed = || {
        let previous = previous_state();
        let weighting = previous.weighting_generation();
        let samples = previous.sample_count();
        let mut refresh = PassNormalState::refresh(
            &problem,
            previous,
            preparation.final_model_generation(),
            NormalStoragePlan::resident(1).expect("resident normal storage"),
        )
        .expect("refresh pass state");
        let mut residual_only = images();
        residual_only.psf = None;
        residual_only.sum_weights.clear();
        refresh.append(residual_only).expect("residual planes");
        (refresh, weighting, samples)
    };
    let (refresh, _, samples) = refreshed();
    assert!(matches!(
        refresh.finish(samples + 1, 1),
        Err(SpectralOperatorError::ReusableNormalStateMismatch)
    ));
    let (refresh, weighting, samples) = refreshed();
    let complete = refresh.finish(samples, 2).expect("a complete refresh");
    assert_eq!(complete.completion().weighting_generation(), weighting);
    assert_eq!(complete.completion().problem_id(), problem.problem_id());
    let other_problem = t19_compatible_problem(16);
    assert_ne!(other_problem.problem_id(), problem.problem_id());
    assert!(matches!(
        PassNormalState::refresh(
            &other_problem,
            previous_state(),
            preparation.final_model_generation(),
            NormalStoragePlan::resident(1).expect("resident normal storage"),
        ),
        Err(SpectralOperatorError::ReusableNormalStateMismatch)
    ));
}
