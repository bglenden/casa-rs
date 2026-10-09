// SPDX-License-Identifier: LGPL-3.0-or-later

//! Sealed Product Generation publication through the plan/run executor.
//!
//! The scientific state a publication seals is one reconciled major cycle.
//! The fixture forms it from a synthetic initial pass assembled through
//! `PassNormalState`, as the major-cycle pass would: publication reads only
//! that normal state, so no visibilities are traversed. The publication
//! path, layouts, fences, receipts, sole visibility and terminal promotion
//! are the runtime's own.

use super::*;

const SEALED_PRODUCTS_SHAPE: [usize; 2] = [8, 8];

/// Full width at half maximum of the synthetic pass's PSF, in pixels.
const SYNTHETIC_PSF_FWHM_PX: f64 = 2.5;

/// Normalised frequency offsets `(ν − ν₀)/ν₀` and weights of the two
/// spectral samples whose sums form a Taylor basis's normal moments.
const SYNTHETIC_TAYLOR_SAMPLES: [(f64, f64); 2] = [(-0.2, 2.0 / 3.0), (0.2, 1.0 / 3.0)];

pub(crate) fn sealed_products_request(observation: u8) -> ImagingRequest {
    let references = default_references();
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::UnorderedWithinBudget,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
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
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::Mask,
            ],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::PerPlane,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        numerics,
    );
    ImagingRequest::new(
        specification,
        geometry_with_shape_and_increment(
            [4.0, 4.0],
            ImageShape::new(SEALED_PRODUCTS_SHAPE[0], SEALED_PRODUCTS_SHAPE[1]),
            [-1.0e-6, 1.0e-6],
        ),
        problem_inputs_with_source_count(observation, references, ModelStateIdentity::Empty, 1),
        model_lifecycle(ModelStateIdentity::Empty),
    )
}

/// The x-major Gaussian PSF of unit peak at the image centre.
fn synthetic_psf(shape: [usize; 2]) -> Vec<f64> {
    let sigma = SYNTHETIC_PSF_FWHM_PX / (8.0 * std::f64::consts::LN_2).sqrt();
    let centre = [shape[0] / 2, shape[1] / 2];
    let mut psf = Vec::with_capacity(shape[0] * shape[1]);
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = x as f64 - centre[0] as f64;
            let dy = y as f64 - centre[1] as f64;
            psf.push((-(dx * dx + dy * dy) / (2.0 * sigma * sigma)).exp());
        }
    }
    psf
}

/// The images an initial pass forms of a unit point source at the centre of
/// one Stokes-I domain: every PSF moment plane is `m_k P` with sum weight
/// `m_k`, and residual term `t` is `m_t P`. A constant basis has the single
/// moment one; a channel-local basis one unit-weight plane per channel.
fn synthetic_pass_images(
    problem: &casa_imaging_model::CompiledProblem,
    domain: usize,
    shape: [usize; 2],
) -> casa_imaging_reconstruction::PassImages {
    let unit = synthetic_psf(shape);
    let planes = |weights: &[f64]| -> Vec<f32> {
        weights
            .iter()
            .flat_map(|weight| unit.iter().map(move |value| (weight * value) as f32))
            .collect()
    };
    let (channels, terms, moments) = match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { channels } => (channels, channels, vec![1.0; channels]),
        ReconstructionBasis::Taylor { terms }
        | ReconstructionBasis::TaylorViaChannelMajor { terms, .. } => (
            problem.geometry().spectral().output_channels(),
            terms,
            (0..2 * terms - 1)
                .map(|order| {
                    SYNTHETIC_TAYLOR_SAMPLES
                        .iter()
                        .map(|(offset, weight)| weight * offset.powi(order as i32))
                        .sum()
                })
                .collect(),
        ),
        ReconstructionBasis::Constant => (
            problem.geometry().spectral().output_channels(),
            1,
            vec![1.0],
        ),
    };
    casa_imaging_reconstruction::PassImages {
        domain,
        shape,
        channels: 0..channels,
        polarizations: 1,
        residual: planes(&moments[..terms]),
        psf: Some(planes(&moments)),
        published_sum_weights: moments.clone(),
        sum_weights: moments,
        weight: None,
    }
}

/// One reconciled major cycle of `problem`: an initial pass over its empty
/// start model, reconciled through the model lifecycle owner.
fn sealed_products_round(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
) -> MajorCycleCompletion {
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        ModelExecutionAttemptId::new(identity(attempt_byte)),
        7,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("bind model lifecycle");
    let named = lifecycle.initial_empty().expect("empty named generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare model");
    let storage = casa_imaging_reconstruction::runtime_adapter::NormalStoragePlan::resident(
        problem.geometry().spectral().output_channels(),
    )
    .expect("fixture normal window");
    let mut state = casa_imaging_reconstruction::PassNormalState::initial(
        problem,
        casa_imaging_reconstruction::WeightingGenerationId::next(),
        preparation.final_model_generation(),
        storage,
    )
    .expect("initial synthetic pass state");
    for (domain, specification) in problem.geometry().domains().iter().enumerate() {
        state
            .append(synthetic_pass_images(
                problem,
                domain,
                specification.shape().pixels(),
            ))
            .expect("append synthetic pass images");
    }
    let normal = state.finish(1, 1).expect("complete synthetic pass");
    MajorCycleOwner::from_complete_data(normal, preparation)
        .expect("major-cycle owner")
        .reconcile(&mut lifecycle)
        .expect("atomic reconciliation")
}

pub(crate) fn pending_generation_for_problem(
    problem: &casa_imaging_model::CompiledProblem,
) -> (
    casa_imaging_products::PlannedContinuumGeneration,
    MajorCycleCompletion,
    ContinuumGenerationDemand,
) {
    let join = sealed_products_round(problem, 202);
    let inputs = ContinuumProductInputs::from_major_cycle(problem, &join).expect("inputs");
    let planned = casa_imaging_products::PlannedContinuumGeneration::new(
        &inputs,
        &ContinuumProductControls::default(),
    )
    .expect("planned generation");
    let demand = planned
        .demand(
            &inputs,
            casa_imaging_products::ProductStoragePlan::new(1, 1).unwrap(),
        )
        .expect("product generation demand");
    (planned, join, demand)
}

pub(crate) fn publication_plan_for_problem(
    problem: &casa_imaging_model::CompiledProblem,
) -> ProductPublicationPlan {
    let (planned, _, _) = pending_generation_for_problem(problem);
    ProductPublicationPlan::bind(problem, &planned).expect("planned inventory")
}

#[test]
fn planned_publication_rejects_another_problem_with_the_same_product_graph() {
    let source = compile(sealed_products_request(236)).expect("source continuum compilation");
    let foreign = compile(sealed_products_request(237)).expect("foreign continuum compilation");
    assert_eq!(
        source.product_graph().graph_id(),
        foreign.product_graph().graph_id(),
        "the probe isolates problem lineage from identical product topology"
    );
    assert_ne!(source.problem_id(), foreign.problem_id());

    let (planned, _, _) = pending_generation_for_problem(&source);
    let publication = ProductPublicationPlan::bind(&source, &planned).unwrap();
    let shared = publication.clone();
    assert!(
        std::ptr::eq(publication.entries(), shared.entries()),
        "planning and execution share one routing inventory"
    );
    let error = ProductPublicationPlan::bind(&foreign, &planned)
        .expect_err("a plan from another problem must not enter publication planning");
    assert_eq!(
        error,
        casa_imaging_runtime::ProductPublicationError::ForeignGeneration {
            expected_problem: foreign.problem_id(),
            expected_graph: foreign.product_graph().graph_id(),
        }
    );
}

#[derive(Default)]
pub(crate) struct InMemoryProductSink {
    pub(crate) staged: Mutex<Vec<casa_imaging_model::ProductNodeId>>,
    pub(crate) visible: Mutex<Vec<casa_imaging_model::ProductNodeId>>,
    pub(crate) publish_calls: AtomicUsize,
    pub(crate) fail: bool,
}
struct CountingProductWriter<'a> {
    sink: &'a InMemoryProductSink,
    node: casa_imaging_model::ProductNodeId,
    layout: casa_imaging_products::ProductWindowLayout,
    next_channel: usize,
}
impl casa_imaging_products::ProductOutput for InMemoryProductSink {
    fn begin_member<'a>(
        &'a self,
        member: &casa_imaging_products::PlannedMember,
        layout: casa_imaging_products::ProductWindowLayout,
        _: &[Option<casa_imaging_products::RestoringBeam>],
    ) -> Result<
        Box<dyn casa_imaging_products::ProductWriter + 'a>,
        casa_imaging_products::ProductsError,
    > {
        assert!(self.visible.lock().unwrap().is_empty());
        Ok(Box::new(CountingProductWriter {
            sink: self,
            node: member.node(),
            layout,
            next_channel: 0,
        }))
    }
}
impl casa_imaging_products::ProductWriter for CountingProductWriter<'_> {
    fn write(
        &mut self,
        window: casa_imaging_products::ProductWindow,
    ) -> Result<(), casa_imaging_products::ProductsError> {
        let axis = self.layout.spectral_axis();
        assert_eq!(window.start()[axis], self.next_channel);
        assert!(window.payload().len() <= self.layout.maximum_values());
        assert!(window.payload().iter().all(|value| !value.is_infinite()));
        self.next_channel += window.shape()[axis];
        Ok(())
    }
    fn finish(self: Box<Self>) -> Result<(), casa_imaging_products::ProductsError> {
        assert_eq!(
            self.next_channel,
            self.layout.shape()[self.layout.spectral_axis()]
        );
        self.sink.staged.lock().unwrap().push(self.node);
        Ok(())
    }
}
impl SerialProductPublicationSink for InMemoryProductSink {
    type Error = io::Error;
    fn residency(
        &self,
        _: &casa_imaging_products::PlannedContinuumGeneration,
        demand: &ContinuumGenerationDemand,
    ) -> Result<casa_imaging_runtime::ProductSinkResidency, Self::Error> {
        Ok(casa_imaging_runtime::ProductSinkResidency {
            writer_bytes: demand.maximum_window_payload_bytes()
                + demand.maximum_window_validity_bytes(),
            retained_bytes: 0,
        })
    }
    fn publish(&self) -> Result<(), Self::Error> {
        self.publish_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(io::Error::other(
                "injected publication failure; rerun required",
            ));
        }
        *self.visible.lock().unwrap() = self.staged.lock().unwrap().clone();
        Ok(())
    }
}

#[test]
fn direct_product_publication_has_bounded_write_only_generation_and_one_terminal_publish() {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for fail in [false, true] {
        let problem = compile(sealed_products_request(242)).unwrap();
        let (planned, scientific, demand) = pending_generation_for_problem(&problem);
        let sink = InMemoryProductSink {
            fail,
            ..InMemoryProductSink::default()
        };
        let writer_bytes = sink.residency(&planned, &demand).unwrap();
        let planning_registry = ContractOnlyRegistry::new(
            registry(77),
            implementation_metadata(&problem),
            [implementation(77)],
        );
        let planned_runtime = SerialProductPublicationPlan::new(
            &problem,
            &planned,
            &demand,
            writer_bytes,
            &planning_registry,
            SerialProductPublicationPolicy::new(
                implementation(77),
                serial_storage_io(),
                1_000,
                900_000,
                512 << 10,
            ),
        )
        .unwrap();
        let dag = planned_runtime.physical_work().execution_dag();
        assert_eq!(dag.nodes().len(), 2);
        assert!(
            planned_runtime
                .physical_work()
                .observation_transaction()
                .initial_consistency_check()
                .is_none()
        );
        let metadata =
            &dag.logical_allocations()[&AllocationId::new("product-generation-metadata")];
        assert_eq!(metadata.bytes, demand.retained_metadata_bytes());
        assert_eq!(
            metadata.lifetime.acquire_at,
            WorkNodeId::new("product-generation-write")
        );
        assert_eq!(
            metadata.lifetime.release_after,
            BTreeSet::from([
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("product-publication-commit"),
                    FenceKind::Io
                )),
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("product-publication-commit"),
                    FenceKind::Publication
                )),
            ])
        );
        assert_eq!(dag.resource_alternative().demand.file_descriptors.hard(), 1);
        assert_eq!(
            dag.resource_alternative()
                .demand
                .io_buffers
                .serialization_bytes,
            writer_bytes.writer_bytes
        );
        assert!(
            dag.resource_alternative()
                .demand
                .storage
                .iter()
                .all(|storage| storage.temporary_bytes == 0)
        );
        assert!(
            dag.nodes()
                .values()
                .all(|node| !node.kind.reads_observation())
        );
        let expected_members = planned.members().len();
        let (physical, publication, window) = planned_runtime.into_parts();
        let executor = SerialProductPublicationExecutor::new(
            implementation(77),
            problem.clone(),
            publication,
            planned,
            scientific,
            None,
            sink,
            window,
        )
        .unwrap();
        let registry = SerialProductPublicationRegistry::new(
            registry(77),
            implementation(77),
            &problem,
            executor,
        );
        let directory = tempfile::tempdir().unwrap();
        let receipts = ExecutionReceiptStore::new(
            directory.path(),
            ReceiptRetention::new(4, 1_048_576).unwrap(),
        )
        .unwrap();
        let plan = runtime_plan(
            &problem,
            PlanningBindings::new(
                registry.registry_id(),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            authority(),
            &registry,
            &receipts,
            move |_, _| Ok::<_, io::Error>(vec![physical]),
        )
        .unwrap();
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([78; 32]);
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let result = runtime_run(
            &ExecutableModelProblem::from_compiled(problem.clone()).unwrap(),
            &plan,
            &current,
            &registry,
            authority(),
            &mut RunToCompletion,
            receipts.bind(execution_provenance(
                attempt,
                BuildIdentity::from_sha256([79; 32]),
            )),
        );
        assert_eq!(result.is_err(), fail);
        let sink = registry.implementation().sink();
        assert_eq!(sink.staged.lock().unwrap().len(), expected_members);
        assert_eq!(sink.publish_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            sink.visible.lock().unwrap().len(),
            if fail { 0 } else { expected_members }
        );
        assert_eq!(registry.implementation().take_completion().is_none(), fail);
        assert!(
            registry.implementation().take_completion().is_none(),
            "completion is consumed once"
        );
        assert_eq!(
            receipts.open(attempt).unwrap().status(),
            if fail {
                ReceiptStatus::Failed
            } else {
                ReceiptStatus::Completed
            }
        );
    }
}

#[test]
fn production_storage_profile_admits_the_serial_publication_plan() {
    let problem = compile(sealed_products_request(245)).expect("continuum compilation");
    let (planned, _, generation_demand) = pending_generation_for_problem(&problem);
    let staging_residency_bytes = InMemoryProductSink::default()
        .residency(&planned, &generation_demand)
        .expect("in-memory staging demand");
    let planning_registry = ContractOnlyRegistry::new(
        registry(81),
        implementation_metadata(&problem),
        [implementation(81)],
    );
    let storage_root = tempfile::tempdir().expect("storage root");
    let storage = ProductionStorageProfile::new(
        storage_root.path(),
        1_073_741_824,
        1_073_741_824,
        1_000_000,
        1_000_000,
        64,
        8,
    )
    .expect("valid production storage profile");
    let authority = ResourceAuthority::detected_with_storage_profile(&storage)
        .expect("detected production authority");
    let planned_runtime = SerialProductPublicationPlan::new(
        &problem,
        &planned,
        &generation_demand,
        staging_residency_bytes,
        &planning_registry,
        SerialProductPublicationPolicy::new(
            implementation(81),
            storage.io_resources(),
            1_000,
            900_000,
            512 << 10,
        ),
    )
    .expect("production publication plan");
    let (physical, _, _) = planned_runtime.into_parts();
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("receipt store");

    runtime_plan(
        &problem,
        PlanningBindings::new(registry(81), ResourcePolicy::Balanced, cost_model(4)),
        &authority,
        &planning_registry,
        &receipts,
        move |_, _| Ok::<_, io::Error>(vec![physical]),
    )
    .expect("profiled production resources admit the publication plan");
}

#[test]
fn profiled_serial_plans_bind_only_their_used_storage_identities() {
    let problem = compile(sealed_products_request(246)).expect("continuum compilation");
    let (planned, _, generation_demand) = pending_generation_for_problem(&problem);
    let staging_residency_bytes = InMemoryProductSink::default()
        .residency(&planned, &generation_demand)
        .expect("in-memory staging demand");
    let planning_registry = ContractOnlyRegistry::new(
        registry(82),
        implementation_metadata(&problem),
        [implementation(82)],
    );
    let storage_root = tempfile::tempdir().expect("storage root");
    let storage = ProductionStorageProfile::new(
        storage_root.path(),
        1_073_741_824,
        1_073_741_824,
        1_000_000,
        1_000_000,
        64,
        8,
    )
    .expect("valid production storage profile");
    let authority = ResourceAuthority::detected_with_storage_profile(&storage)
        .expect("detected production authority");
    let exact = storage.io_resources();
    let substitutions = [
        StorageIoResourceBinding::new(
            StorageDomainId::new("foreign-storage-domain"),
            exact.read_rate().clone(),
            exact.write_rate().clone(),
            exact.queue().clone(),
        ),
        StorageIoResourceBinding::new(
            exact.domain().clone(),
            RateResourceId::new("foreign-read-rate"),
            exact.write_rate().clone(),
            exact.queue().clone(),
        ),
        StorageIoResourceBinding::new(
            exact.domain().clone(),
            exact.read_rate().clone(),
            RateResourceId::new("foreign-write-rate"),
            exact.queue().clone(),
        ),
        StorageIoResourceBinding::new(
            exact.domain().clone(),
            exact.read_rate().clone(),
            exact.write_rate().clone(),
            QueueResourceId::new("foreign-storage-queue"),
        ),
    ];
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let reject = |physical| {
        let result = runtime_plan(
            &problem,
            PlanningBindings::new(registry(82), ResourcePolicy::Balanced, cost_model(4)),
            &authority,
            &planning_registry,
            &receipts,
            move |_, _| Ok::<_, io::Error>(vec![physical]),
        );
        match result {
            Err(PlanError::Resource(ResourceError::Invalid(message))) => {
                assert!(
                    message.contains("unknown"),
                    "substitution failed for an unrelated reason: {message}"
                );
            }
            Err(other) => panic!("unexpected substituted-resource failure: {other}"),
            Ok(_) => panic!("substituted storage identity was admitted"),
        }
    };

    for (index, substitution) in substitutions.into_iter().enumerate() {
        let publication = SerialProductPublicationPlan::new(
            &problem,
            &planned,
            &generation_demand,
            staging_residency_bytes,
            &planning_registry,
            SerialProductPublicationPolicy::new(
                implementation(82),
                substitution,
                1_000,
                900_000,
                512 << 10,
            ),
        )
        .expect("publication plan construction");
        let publication = publication.into_parts().0;
        if index == 1 {
            runtime_plan(
                &problem,
                PlanningBindings::new(registry(82), ResourcePolicy::Balanced, cost_model(4)),
                &authority,
                &planning_registry,
                &receipts,
                move |_, _| Ok::<_, io::Error>(vec![publication]),
            )
            .expect("unused observation-read rate does not constrain sealed publication");
        } else {
            reject(publication);
        }
    }
}

pub(crate) fn problem_bound_sealed_work(
    problem: &casa_imaging_model::CompiledProblem,
    sealed: &ProductPublicationPlan,
) -> PhysicalWorkBinding {
    let graph_id = problem.product_graph().graph_id();
    let participants = problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .copied()
        .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
        .collect();
    let base = physical_work_with_optional_seal(problem, 6, participants, false, true, sealed)
        .expect("problem-bound native product publication");
    let measurement_sets = problem
        .observation_transaction()
        .read_set()
        .sources()
        .iter()
        .map(|source| source.measurement_set())
        .collect::<Vec<_>>();
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for node in &mut nodes {
        let mut claims = Vec::with_capacity(node.claims.len());
        for claim in std::mem::take(&mut node.claims) {
            if matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }) {
                claims.extend(measurement_sets.iter().copied().map(|measurement_set| {
                    ResourceClaim {
                        resource: LeaseResource::MeasurementSetLock { measurement_set },
                        amount: claim.amount,
                        lifetime: claim.lifetime.clone(),
                    }
                }));
            } else {
                claims.push(claim);
            }
        }
        node.claims = claims;
    }
    let mut alternative = base.execution_dag().resource_alternative().clone();
    let lock_count = u64::try_from(measurement_sets.len()).expect("test lock count fits u64");
    alternative.demand.locks = CountDemand::new(lock_count, lock_count);
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: base
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("problem-bound transaction DAG");
    PhysicalWorkBinding::new_with_product_publication(
        implementation_catalog(problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
        sealed,
    )
    .expect("problem-bound sealed transaction work")
}
