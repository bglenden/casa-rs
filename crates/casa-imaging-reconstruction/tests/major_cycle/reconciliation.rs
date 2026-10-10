// SPDX-License-Identifier: LGPL-3.0-or-later

//! Major cycles pairing complete-data normal states with their final
//! models: owned storage hand-off, model-dependent residual content,
//! residual refreshes, and the passes that can never finish a major cycle.

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
    let lifecycle = model_lifecycle(&problem);
    let reads = Arc::new(AtomicUsize::new(0));
    let storage = NormalStoragePlan::new(Arc::new(Factory(reads.clone())), 2).unwrap();
    let cycle = scene(&problem).initial_cycle(&problem, empty_final_model(&lifecycle), storage);
    reads.store(0, Ordering::Relaxed);
    cycle
        .finish(SAMPLES, BLOCKS)
        .expect("complete the stored pass");
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "completion must not authorize trusted ownership by rereading normal arrays"
    );
}

#[test]
fn reconciliation_applies_one_pending_delta_through_the_model_owner() {
    let problem = t19_compatible_problem(11);
    let scene = scene(&problem);
    let lifecycle = model_lifecycle(&problem);
    let named = lifecycle.initial_empty().expect("empty named generation");
    let model = lifecycle
        .prepare_final_model(named, [ModelDeltaTerm::new(cell(1), delta_value(-2.5))])
        .expect("final model with a pending Högbom-style update");
    let joined = scene.initial(&problem, model);

    // The normal state records the pass's traversal and catalog.
    let normal_state = joined.normal_state();
    assert_eq!(normal_state.sample_count(), SAMPLES);
    assert_eq!(normal_state.block_count(), BLOCKS);
    assert_eq!(
        normal_state.catalog(),
        casa_imaging_reconstruction::NormalStateCatalog::UnnormalizedPlaneV1
    );
    // The residual content is model-dependent: a nonzero final model never
    // relabels the data side of the same sky.
    assert_ne!(
        normal_content(normal_state),
        confirm_content(&problem, &scene)
    );
    // The pending update was applied to the final model.
    assert!(
        joined
            .final_model()
            .read_samples(0..joined.final_model().sample_count())
            .expect("read fixture model")[1]
            .value()
            .value()
            == -2.5
    );
}

#[test]
fn reconciliation_without_a_pending_delta_confirms_the_named_generation_final() {
    let problem = t19_compatible_problem(12);
    let scene = scene(&problem);
    let lifecycle = model_lifecycle(&problem);
    let named = lifecycle.initial_empty().expect("empty named generation");
    let named_samples = named
        .read_samples(0..named.sample_count())
        .expect("read the named model");
    let model = lifecycle
        .prepare_final_model(named, [])
        .expect("prepare final model");
    let joined = scene.initial(&problem, model);

    // No update terms leave the named model unchanged.
    assert_eq!(
        joined
            .final_model()
            .read_samples(0..joined.final_model().sample_count())
            .expect("read the final model"),
        named_samples
    );
    // An empty final model reconciles to the exact pass residual bit-for-bit,
    // and its content matches an independent confirm pass over the same sky.
    let pass_residual = scene
        .pass_images(joined.final_model(), true)
        .residual
        .iter()
        .map(|&value| Complex64::new(f64::from(value), 0.0))
        .collect::<Vec<_>>();
    assert_eq!(
        normal_content(joined.normal_state()),
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
    let lifecycle = model_lifecycle(&problem);
    let initial = scene.initial(&problem, empty_final_model(&lifecycle));
    let initial_content = normal_content(initial.normal_state());
    let (initial_normal, carried_model) = initial.into_parts();
    let empty = lifecycle.initial_empty().expect("empty model");
    assert_eq!(
        carried_model
            .read_samples(0..carried_model.sample_count())
            .expect("read the carried model"),
        empty
            .read_samples(0..empty.sample_count())
            .expect("read the empty model"),
        "the carried model is the empty model"
    );
    let refresh_model = lifecycle
        .prepare_final_model(carried_model, [])
        .expect("refresh preparation");
    let refreshed = scene.refresh(&problem, initial_normal, refresh_model);
    assert_eq!(normal_content(refreshed.normal_state()), initial_content);
}

#[test]
fn chained_no_delta_major_cycles_carry_the_model_unchanged() {
    let problem = t19_compatible_problem(13);
    let scene = scene(&problem);
    let lifecycle = model_lifecycle(&problem);
    let initial_named = lifecycle.initial_empty().expect("initial empty generation");
    let initial_samples = initial_named
        .read_samples(0..initial_named.sample_count())
        .expect("read fixture model");
    let initial_model = lifecycle
        .prepare_final_model(initial_named, [])
        .expect("prepare the initial no-delta final model");
    let (mut normal, mut model) = scene.initial(&problem, initial_model).into_parts();
    for cycle in ["first", "second"] {
        let prepared = lifecycle
            .prepare_final_model(model, [])
            .expect("prepare the no-delta final major cycle");
        let joined = scene.refresh(&problem, normal, prepared);
        assert_eq!(
            joined
                .final_model()
                .read_samples(0..joined.final_model().sample_count())
                .expect("read fixture model"),
            initial_samples,
            "the {cycle} no-delta major cycle carries the model unchanged"
        );
        (normal, model) = joined.into_parts();
    }
}

#[test]
fn residual_content_depends_on_the_exact_final_model() {
    let problem = t19_compatible_problem(27);
    let scene = scene(&problem);
    let lifecycle = model_lifecycle(&problem);
    // The same sky reconciled against two different final models.
    let empty_join = scene.initial(&problem, empty_final_model(&lifecycle));
    let delta_model = lifecycle
        .prepare_final_model(
            lifecycle.initial_empty().expect("empty named generation"),
            [ModelDeltaTerm::new(cell(3), delta_value(1.75))],
        )
        .expect("prepare delta final model");
    let delta_join = scene.initial(&problem, delta_model);

    assert_ne!(
        normal_content(empty_join.normal_state()),
        normal_content(delta_join.normal_state()),
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
    assert_ne!(
        empty_join
            .final_model()
            .read_samples(0..empty_join.final_model().sample_count())
            .unwrap(),
        delta_join
            .final_model()
            .read_samples(0..delta_join.final_model().sample_count())
            .unwrap()
    );
}

#[test]
fn independent_major_cycles_over_one_sky_agree() {
    let problem = t19_compatible_problem(30);
    let scene = scene(&problem);
    // Two independent major cycles over the same sky, each with its own
    // model lifecycle.
    let first = scene.initial(&problem, empty_final_model(&model_lifecycle(&problem)));
    let second = scene.initial(&problem, empty_final_model(&model_lifecycle(&problem)));
    assert_eq!(
        normal_content(first.normal_state()),
        normal_content(second.normal_state())
    );
    assert_eq!(
        first
            .final_model()
            .read_samples(0..first.final_model().sample_count())
            .unwrap(),
        second
            .final_model()
            .read_samples(0..second.final_model().sample_count())
            .unwrap()
    );
}

#[test]
fn incomplete_or_inconsistent_passes_cannot_finish_a_major_cycle() {
    let problem = t19_compatible_problem(15);
    let scene = scene(&problem);
    let lifecycle = model_lifecycle(&problem);
    let storage = || NormalStoragePlan::resident(1).expect("resident normal storage");
    let cycle = || {
        MajorCycle::initial(&problem, empty_final_model(&lifecycle), storage())
            .expect("initial major cycle")
    };
    let empty = lifecycle.initial_empty().expect("empty model generation");
    let images = || scene.pass_images(&empty, true);

    // Every image domain must be appended before the cycle finishes.
    assert!(matches!(
        cycle().finish(4, 1),
        Err(MajorCycleError::Residual(
            SpectralOperatorError::IncompleteCoverage
        ))
    ));

    // A non-finite generated value or sumwt is rejected, and a rejected
    // append leaves the state as it was.
    let mut nonfinite = cycle();
    let mut nan_residual = images();
    nan_residual.residual[0] = f32::NAN;
    assert!(matches!(
        nonfinite.parts().1.append(nan_residual),
        Err(SpectralOperatorError::GeneratedNonfinite)
    ));
    let mut infinite_sumwt = images();
    infinite_sumwt.sum_weights[0] = f64::INFINITY;
    assert!(matches!(
        nonfinite.parts().1.append(infinite_sumwt),
        Err(SpectralOperatorError::GeneratedNonfinite)
    ));
    nonfinite.parts().1.append(images()).expect("finite images");
    nonfinite
        .finish(4, 1)
        .expect("the state survives rejected appends");

    // An initial pass carries PSF moments, names a compiled domain and covers
    // the whole spectral axis with consistently sized planes.
    let mut without_psf = images();
    without_psf.psf = None;
    assert!(matches!(
        cycle().parts().1.append(without_psf),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
    let mut foreign_domain = images();
    foreign_domain.domain = 1;
    assert!(matches!(
        cycle().parts().1.append(foreign_domain),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
    let mut short = images();
    short.residual.pop();
    assert!(matches!(
        cycle().parts().1.append(short),
        Err(SpectralOperatorError::InvalidSlab)
    ));
    let mut beyond_axis = images();
    beyond_axis.channels = 0..2;
    assert!(matches!(
        cycle().parts().1.append(beyond_axis),
        Err(SpectralOperatorError::InvalidSlab)
    ));
    // Planes must have the compiled domain's shape and polarization count,
    // even when their lengths agree with each other.
    let mut transposed = images();
    let [width, height] = transposed.shape;
    transposed.shape = [width * height, 1];
    assert!(matches!(
        cycle().parts().1.append(transposed),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
    let mut extra_polarization = images();
    extra_polarization.polarizations += 1;
    assert!(matches!(
        cycle().parts().1.append(extra_polarization),
        Err(SpectralOperatorError::ProblemMismatch)
    ));

    // A pass that proves no traversal cannot finish.
    let mut untraversed = cycle();
    untraversed
        .parts()
        .1
        .append(images())
        .expect("complete images");
    assert!(matches!(
        untraversed.finish(0, 0),
        Err(MajorCycleError::IncompleteCoverage)
    ));

    // A residual refresh takes residual planes only.
    let previous = || {
        scene
            .initial(&problem, empty_final_model(&lifecycle))
            .into_parts()
            .0
    };
    let refresh_cycle = |previous: FinalNormalState| {
        MajorCycle::refresh(&problem, previous, empty_final_model(&lifecycle), storage())
            .expect("refresh major cycle")
    };
    let mut refresh = refresh_cycle(previous());
    assert!(matches!(
        refresh.parts().1.append(images()),
        Err(SpectralOperatorError::ProblemMismatch)
    ));

    // A refresh keeps the previous PSF and sumwt, so it carries the
    // weighting generation they were formed with and must place the samples
    // the previous state placed.
    let refreshed = || {
        let previous = previous();
        let samples = previous.sample_count();
        let mut refresh = refresh_cycle(previous);
        let mut residual_only = images();
        residual_only.psf = None;
        residual_only.sum_weights.clear();
        refresh
            .parts()
            .1
            .append(residual_only)
            .expect("residual planes");
        (refresh, samples)
    };
    let (refresh, samples) = refreshed();
    assert!(matches!(
        refresh.finish(samples + 1, 1),
        Err(MajorCycleError::Residual(
            SpectralOperatorError::ReusableNormalStateMismatch
        ))
    ));
    let (refresh, samples) = refreshed();
    refresh.finish(samples, 2).expect("a complete refresh");
}

/// A coupled (constant-basis) refresh carries every domain's previous
/// residual until its pass forms that residual again, so a refresh that
/// leaves any image domain's residual unformed cannot finish.
#[test]
fn a_coupled_refresh_must_form_the_residual_of_every_domain_again() {
    let problem = two_domain_problem(40);
    assert_eq!(problem.geometry().domains().len(), 2);
    let scene = scene(&problem);
    let lifecycle = model_lifecycle(&problem);
    let storage = || NormalStoragePlan::resident(1).expect("resident normal storage");
    let refresh = |formed: std::ops::Range<usize>| {
        let mut initial = MajorCycle::initial(&problem, empty_final_model(&lifecycle), storage())
            .expect("initial major cycle");
        let (model, state) = initial.parts();
        for domain in 0..2 {
            let mut images = scene.pass_images(model, true);
            images.domain = domain;
            state
                .append(images)
                .expect("initial images of every domain");
        }
        let (previous, model) = initial
            .finish(SAMPLES, BLOCKS)
            .expect("complete the initial major cycle")
            .into_parts();
        let model = lifecycle
            .prepare_final_model(model, [])
            .expect("prepare the refresh's final model");
        let mut refresh =
            MajorCycle::refresh(&problem, previous, model, storage()).expect("refresh major cycle");
        let (model, state) = refresh.parts();
        for domain in formed {
            let mut images = scene.pass_images(model, false);
            images.domain = domain;
            state.append(images).expect("residual images");
        }
        refresh.finish(SAMPLES, BLOCKS)
    };

    let complete = refresh(0..2).expect("a refresh that forms every domain's residual");
    assert_eq!(complete.normal_state().domain_count(), 2);
    for formed in [0..0, 0..1, 1..2] {
        assert!(
            matches!(
                refresh(formed.clone()),
                Err(MajorCycleError::Residual(
                    SpectralOperatorError::IncompleteCoverage
                ))
            ),
            "a refresh forming the residuals of domains {formed:?} only"
        );
    }
}
