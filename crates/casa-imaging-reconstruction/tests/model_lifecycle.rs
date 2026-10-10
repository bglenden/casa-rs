// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompiledProblem, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    InstrumentResponse, LogicalIdentity, MeasurementEquationContract, ModelBounds, ModelCell,
    ModelColumnWrite, ModelDeltaTerm, ModelExecutionAttemptId, ModelInnerProduct,
    ModelLifecycleRequirements, ModelSample, ModelValue, NumericPrecision, NumericalStage,
    NumericsContract, ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInput,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope,
    WeightingContract, WeightingScheme, compile,
};
use casa_imaging_reconstruction::{
    FinalModelCompletionId, ModelGenerationId, ModelLifecycle, ModelLifecycleError,
};

#[path = "../../casa-imaging-model/tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_bytes([byte; 32])
}

fn bounds() -> ModelBounds {
    ModelBounds::new(16, 8, 1.0e30, 1.0e30).expect("valid bounds")
}

fn empty_requirements(precision: NumericPrecision) -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(bounds(), precision)
}

fn product_validity() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid PB policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid Taylor policy"),
    )
}

fn geometry(width: usize) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [0.0, 0.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(width, 1),
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
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn overlapping_geometry(width: usize, domains: usize) -> GeometryInput {
    assert!(domains >= 2);
    let direction = |reference_x| {
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [reference_x, 0.0],
            [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        )
    };
    let image_domains = (0..domains)
        .map(|ordinal| {
            ImageDomainSpec::new(
                if ordinal == 0 {
                    ImageDomainRole::Main
                } else {
                    ImageDomainRole::Outlier(format!("outlier-{ordinal}"))
                },
                ImageShape::new(width, 1),
                direction(ordinal as f64),
                FacetLayout::Single,
                AxisOrder::new([
                    ImageAxis::DirectionLongitude,
                    ImageAxis::DirectionLatitude,
                    ImageAxis::Polarization,
                    ImageAxis::Spectral,
                ]),
            )
        })
        .collect();
    GeometryInput::new(
        image_domains,
        CentreLaws::new(
            PhaseCentreLaw::Fixed(direction(0.0).reference_direction()),
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
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn problem(
    observation: u8,
    width: usize,
    lifecycle: ModelLifecycleRequirements,
    precision: NumericPrecision,
) -> CompiledProblem {
    problem_with_geometry(observation, geometry(width), lifecycle, precision)
}

fn problem_with_geometry(
    observation: u8,
    geometry: GeometryInput,
    lifecycle: ModelLifecycleRequirements,
    precision: NumericPrecision,
) -> CompiledProblem {
    let specification = ProblemSpecification::new(
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
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![precision],
            ReductionPolicy::UnorderedWithinBudget,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    );
    compile(ProblemInput::new(
        specification,
        geometry,
        common::observation_snapshot(observation),
        lifecycle,
    ))
    .expect("compile model lifecycle problem")
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn value(value: f64) -> ModelValue {
    ModelValue::new(value).expect("finite model value")
}

fn attempt(byte: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(identity(byte))
}

fn bind_direct(
    problem: &CompiledProblem,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        problem,
        attempt,
        epoch,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("bind model lifecycle")
}

#[test]
fn compiled_commitment_binds_problem_input_numerics_and_bounds() {
    let first = problem(
        1,
        2,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let other_observation = problem(
        2,
        2,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let f32_problem = problem(
        1,
        2,
        empty_requirements(NumericPrecision::F32),
        NumericPrecision::F32,
    );

    assert_ne!(first.observation(), other_observation.observation());
    assert_ne!(
        first.model_lifecycle().arithmetic_precision(),
        f32_problem.model_lifecycle().arithmetic_precision()
    );
    assert_eq!(
        first.model_lifecycle().bounds().max_delta_terms(),
        bounds().max_delta_terms()
    );
    assert_eq!(
        first.model_lifecycle().arithmetic_precision(),
        NumericPrecision::F64
    );
}

#[test]
fn non_power_of_two_delta_bound_uses_the_explicit_canonical_capacity() {
    const TERMS: usize = 65;
    let bounded = ModelBounds::new(TERMS, TERMS, 1.0e30, 1.0e30).expect("non-power-of-two bounds");
    let compiled = problem(
        3,
        TERMS,
        ModelLifecycleRequirements::new(bounded, NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let owner = bind_direct(&compiled, attempt(89), 1);
    let base = owner.initial_empty().expect("bounded empty generation");
    let delta = owner
        .compile_delta(
            &base,
            (0..TERMS).map(|x| ModelDeltaTerm::new(cell(x), value(1.0))),
        )
        .expect("compile the full non-power-of-two delta bound");

    assert_eq!(delta.terms().len(), TERMS);
}

#[test]
fn final_model_restores_highest_ordinal_domain_across_overlaps() {
    let compiled = problem_with_geometry(
        3,
        overlapping_geometry(3, 3),
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let mut owner = bind_direct(&compiled, attempt(88), 1);
    let base = owner.initial_empty().expect("empty multi-domain model");
    let terms = [
        ModelDeltaTerm::new(ModelCell::new(0, 0, 0, [0, 0]), value(1.0)),
        ModelDeltaTerm::new(ModelCell::new(0, 0, 0, [2, 0]), value(2.0)),
        ModelDeltaTerm::new(ModelCell::new(1, 0, 0, [1, 0]), value(10.0)),
        ModelDeltaTerm::new(ModelCell::new(2, 0, 0, [2, 0]), value(30.0)),
    ];
    let delta = owner
        .compile_delta(&base, terms)
        .expect("multi-domain delta");
    let update = owner
        .apply_final_delta(base, delta)
        .expect("restore canonical overlap ownership");
    let shape = update.generation().shape();
    let sample = |domain, x| {
        update
            .generation()
            .read_samples(0..update.generation().sample_count())
            .expect("read fixture model")[shape
            .flat_index(ModelCell::new(domain, 0, 0, [x, 0]))
            .expect("model cell")]
        .value()
        .value()
    };

    assert_eq!(sample(0, 0), 30.0, "highest domain ordinal wins");
    assert_eq!(sample(1, 1), 30.0, "later owner is restored transitively");
    assert_eq!(sample(0, 2), 2.0, "non-overlapping model remains unchanged");
    assert_eq!(sample(2, 2), 30.0, "owner model remains unchanged");
}

#[test]
fn t55_model_windows_preserve_values_and_support_with_owner_scoped_identities() {
    let compiled = problem_with_geometry(
        1,
        geometry(5),
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let mut expected = None;
    let mut identities = std::collections::BTreeSet::new();
    for window in [5, 1, 2, 3] {
        let mut owner = ModelLifecycle::bind(
            &compiled,
            attempt(90),
            1,
            casa_imaging_reconstruction::ModelStoragePlan::resident(window).unwrap(),
        )
        .unwrap();
        let base = owner.initial_empty().unwrap();
        if window < 5 {
            assert!(base.read_samples(0..5).is_err());
        }
        let initial = base.generation_id();
        let delta = owner
            .compile_delta(
                &base,
                [
                    ModelDeltaTerm::new(cell(0), value(-0.25)),
                    ModelDeltaTerm::new(cell(4), value(1.5)),
                ],
            )
            .unwrap();
        let delta_id = delta.delta_id();
        let update = owner.apply_final_delta(base, delta).unwrap();
        let samples = (0..5)
            .map(|index| update.generation().read_samples(index..index + 1).unwrap()[0])
            .collect::<Vec<_>>();
        assert!(identities.insert((
            initial,
            delta_id,
            update.generation().generation_id(),
            update.completion().completion_id(),
        )));
        if let Some(expected) = &expected {
            assert_eq!(&samples, expected);
        } else {
            expected = Some(samples);
        }
    }
}

#[derive(Debug, Default)]
struct ModelIoCounts {
    reads: std::sync::atomic::AtomicUsize,
    fail_reads: std::sync::atomic::AtomicBool,
    creations: std::sync::atomic::AtomicUsize,
    updated: std::sync::atomic::AtomicUsize,
}

#[derive(Debug)]
struct CountedModelStorage {
    counts: std::sync::Arc<ModelIoCounts>,
    samples: std::sync::RwLock<Box<[ModelSample]>>,
}

#[derive(Debug)]
struct CountedModelFactory(std::sync::Arc<ModelIoCounts>);

impl casa_imaging_reconstruction::ModelStorageFactory for CountedModelFactory {
    fn create(
        &self,
        count: usize,
    ) -> Result<Box<dyn casa_imaging_reconstruction::ModelSampleStorage>, ModelLifecycleError> {
        self.0
            .creations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Box::new(CountedModelStorage {
            counts: self.0.clone(),
            samples: std::sync::RwLock::new(vec![ModelSample::invalid(); count].into()),
        }))
    }
}

impl casa_imaging_reconstruction::ModelSampleStorage for CountedModelStorage {
    fn sample_count(&self) -> usize {
        self.samples.read().unwrap().len()
    }

    fn read(
        &self,
        start: usize,
        destination: &mut [ModelSample],
    ) -> Result<(), ModelLifecycleError> {
        use std::sync::atomic::Ordering::Relaxed;
        self.counts.reads.fetch_add(destination.len(), Relaxed);
        if self.counts.fail_reads.load(Relaxed) {
            return Err(ModelLifecycleError::Storage("injected read failure".into()));
        }
        destination
            .copy_from_slice(&self.samples.read().unwrap()[start..start + destination.len()]);
        Ok(())
    }

    fn write(&mut self, start: usize, samples: &[ModelSample]) -> Result<(), ModelLifecycleError> {
        self.samples.get_mut().unwrap()[start..start + samples.len()].copy_from_slice(samples);
        Ok(())
    }

    fn apply_updates(
        &self,
        updates: &[casa_imaging_reconstruction::ModelSampleUpdate],
        precision: NumericPrecision,
        bound: f64,
    ) -> Result<f64, ModelLifecycleError> {
        use std::sync::atomic::Ordering::Relaxed;
        self.counts.reads.fetch_add(updates.len(), Relaxed);
        if self.counts.fail_reads.load(Relaxed) {
            return Err(ModelLifecycleError::Storage("injected read failure".into()));
        }
        self.counts.updated.fetch_add(updates.len(), Relaxed);
        casa_imaging_reconstruction::ModelSampleStorage::apply_updates(
            &self.samples,
            updates,
            precision,
            bound,
        )
    }
}

#[test]
fn sparse_delta_reuses_owned_storage_and_does_not_touch_unchanged_windows() {
    use std::sync::{Arc, atomic::Ordering::Relaxed};
    let compiled = problem(
        1,
        8,
        empty_requirements(NumericPrecision::F32),
        NumericPrecision::F32,
    );
    let counts = Arc::new(ModelIoCounts::default());
    let mut owner = ModelLifecycle::bind(
        &compiled,
        attempt(90),
        1,
        casa_imaging_reconstruction::ModelStoragePlan::new(
            Arc::new(CountedModelFactory(counts.clone())),
            2,
        )
        .unwrap(),
    )
    .unwrap();
    let base = owner.initial_empty().unwrap();
    let delta = owner
        .compile_delta(&base, [ModelDeltaTerm::new(cell(3), value(2.0))])
        .unwrap();
    counts.reads.store(0, Relaxed);
    let prepared = owner.prepare_final_model(base, Some(delta)).unwrap();
    assert_eq!(
        counts.creations.load(Relaxed),
        1,
        "no replacement cube allocation"
    );
    assert_eq!(
        counts.reads.load(Relaxed),
        0,
        "preparation must not scan the cube"
    );
    assert_eq!(
        counts.updated.load(Relaxed),
        0,
        "worker applies its own updates"
    );
    prepared.generation().read_samples(0..2).unwrap();
    assert_eq!(counts.updated.load(Relaxed), 0, "unaffected plane/window");
    let samples = prepared.generation().read_samples(2..4).unwrap();
    assert_eq!(samples[1].value().value(), 2.0);
    assert_eq!(counts.updated.load(Relaxed), 1, "only the changed cell");
    let reads = counts.reads.load(Relaxed);
    owner.commit_final_model(prepared).unwrap();
    assert_eq!(counts.reads.load(Relaxed), reads, "no completion reread");
    assert_eq!(counts.creations.load(Relaxed), 1);
}

#[test]
fn owned_model_handoffs_do_not_read_contents_and_scientific_reads_remain_fallible() {
    use std::sync::{Arc, atomic::Ordering::Relaxed};
    let compiled = problem(
        1,
        8,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let counts = Arc::new(ModelIoCounts::default());
    let mut owner = ModelLifecycle::bind(
        &compiled,
        attempt(90),
        1,
        casa_imaging_reconstruction::ModelStoragePlan::new(
            Arc::new(CountedModelFactory(counts.clone())),
            2,
        )
        .unwrap(),
    )
    .unwrap();
    let base = owner.initial_empty().unwrap();
    owner.validate_named_generation(&base).unwrap();
    owner.validate_named_generation(&base).unwrap();
    let prepared = owner.prepare_final_model(base, None).unwrap();
    let update = owner.commit_final_model(prepared).unwrap();
    assert_eq!(
        counts.reads.load(Relaxed),
        0,
        "mint, handoff and completion must not inspect owned contents"
    );
    let (base, _) = update.into_parts();
    let _delta = owner
        .compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .unwrap();
    assert_eq!(
        counts.reads.load(Relaxed),
        1,
        "sparse delta checks only its affected support"
    );
    counts.fail_reads.store(true, Relaxed);
    owner.validate_named_generation(&base).unwrap();
    assert!(matches!(
        base.read_samples(0..1),
        Err(ModelLifecycleError::Storage(_))
    ));
    assert!(matches!(
        owner.compile_delta(&base, [ModelDeltaTerm::new(cell(1), value(1.0))]),
        Err(ModelLifecycleError::Storage(_))
    ));
}

#[test]
fn generations_are_distinct_and_finalization_is_affine() {
    let compiled = problem(
        1,
        2,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let mut owner = bind_direct(&compiled, attempt(90), 1);
    let base = owner.initial_empty().expect("empty generation");
    let replay_base = owner.initial_empty().expect("second pre-final base");
    assert_eq!(ModelGenerationId::SCHEMA_VERSION, 4);
    assert_eq!(FinalModelCompletionId::SCHEMA_VERSION, 2);
    assert_ne!(base.generation_id(), replay_base.generation_id());

    let delta = owner
        .compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(1.5))])
        .expect("compile delta");
    let replay_delta = owner
        .compile_delta(&replay_base, [ModelDeltaTerm::new(cell(0), value(1.5))])
        .expect("compile replay delta");
    assert_ne!(delta.delta_id(), replay_delta.delta_id());
    let update = owner
        .apply_final_delta(base, delta)
        .expect("apply final affine update");
    assert_eq!(
        update
            .generation()
            .read_samples(0..update.generation().sample_count())
            .expect("read fixture model")[0]
            .value()
            .value(),
        1.5
    );
    assert_eq!(
        update.completion().generation(),
        update.generation().generation_id()
    );
    assert_ne!(
        update.completion().completion_id().as_bytes(),
        update.generation().generation_id().as_bytes()
    );
    assert!(matches!(
        owner.apply_final_delta(replay_base, replay_delta),
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
    assert!(matches!(
        owner.initial_empty(),
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
}

#[test]
fn owner_rejects_zero_noncanonical_and_foreign_deltas() {
    assert!(ModelBounds::new(1, 0, 1.0, 1.0).is_err());
    let compiled = problem(
        5,
        2,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let first = bind_direct(&compiled, attempt(93), 1);
    let duplicate = bind_direct(&compiled, attempt(93), 1);
    let base = first.initial_empty().expect("base");
    assert!(matches!(
        first.compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(0.0))]),
        Err(ModelLifecycleError::ZeroDeltaTerm)
    ));
    assert!(matches!(
        first.compile_delta(
            &base,
            [
                ModelDeltaTerm::new(cell(1), value(1.0)),
                ModelDeltaTerm::new(cell(0), value(1.0)),
            ],
        ),
        Err(ModelLifecycleError::NonCanonicalDelta)
    ));
    assert!(matches!(
        duplicate.compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(1.0))]),
        Err(ModelLifecycleError::ForeignModelLifecycle)
    ));
}

#[test]
fn compiled_precision_governs_delta_arithmetic() {
    let f32_problem = problem(
        6,
        1,
        empty_requirements(NumericPrecision::F32),
        NumericPrecision::F32,
    );
    let f64_problem = problem(
        6,
        1,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let f32_owner = bind_direct(&f32_problem, attempt(95), 1);
    let f64_owner = bind_direct(&f64_problem, attempt(95), 1);
    let f32_base = f32_owner.initial_empty().expect("f32 base");
    let f64_base = f64_owner.initial_empty().expect("f64 base");
    let f32_seed_delta = f32_owner
        .compile_delta(
            &f32_base,
            [ModelDeltaTerm::new(cell(0), value(16_777_216.0))],
        )
        .expect("f32 seed delta");
    let f64_seed_delta = f64_owner
        .compile_delta(
            &f64_base,
            [ModelDeltaTerm::new(cell(0), value(16_777_216.0))],
        )
        .expect("f64 seed delta");
    let f32_base = f32_owner
        .apply_delta(f32_base, f32_seed_delta)
        .expect("f32 seed apply");
    let f64_base = f64_owner
        .apply_delta(f64_base, f64_seed_delta)
        .expect("f64 seed apply");
    let f32_delta = f32_owner
        .compile_delta(&f32_base, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .expect("f32 delta");
    let f64_delta = f64_owner
        .compile_delta(&f64_base, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .expect("f64 delta");
    let f32_next = f32_owner
        .apply_delta(f32_base, f32_delta)
        .expect("f32 apply");
    let f64_next = f64_owner
        .apply_delta(f64_base, f64_delta)
        .expect("f64 apply");
    assert_eq!(
        f32_next
            .read_samples(0..f32_next.sample_count())
            .expect("read fixture model")[0]
            .value()
            .value(),
        16_777_216.0
    );
    assert_eq!(
        f64_next
            .read_samples(0..f64_next.sample_count())
            .expect("read fixture model")[0]
            .value()
            .value(),
        16_777_217.0
    );
}

#[test]
fn identical_lifecycle_bindings_cannot_substitute_a_different_owners_generation() {
    let initial_problem = problem(
        7,
        2,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let first_owner = bind_direct(&initial_problem, attempt(96), 1);
    let other_owner = bind_direct(&initial_problem, attempt(96), 1);
    let first_base = first_owner.initial_empty().unwrap();
    let other_base = other_owner.initial_empty().unwrap();
    assert_ne!(first_base.generation_id(), other_base.generation_id());
    let first_delta = first_owner
        .compile_delta(&first_base, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .unwrap();
    let other_delta = other_owner
        .compile_delta(&other_base, [ModelDeltaTerm::new(cell(0), value(2.0))])
        .unwrap();
    let first = first_owner.apply_delta(first_base, first_delta).unwrap();
    let other = other_owner.apply_delta(other_base, other_delta).unwrap();
    assert_ne!(first.generation_id(), other.generation_id());
    assert_eq!(first.read_samples(0..1).unwrap()[0].value().value(), 1.0);
    assert_eq!(other.read_samples(0..1).unwrap()[0].value().value(), 2.0);
    assert!(matches!(
        first_owner.validate_named_generation(&other),
        Err(ModelLifecycleError::ForeignModelLifecycle)
    ));
    first_owner.validate_named_generation(&first).unwrap();
}
