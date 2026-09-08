// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_imaging_model::{
    EvlaAwCellRequest, EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequest,
    NativeAwRequestInput, NativeAwTerms,
};
use casa_imaging_runtime::{
    PreparedArtifactGenerator, PreparedArtifactNativeCatalogOutcome,
    PreparedArtifactNativeGenerator, PreparedArtifactNativeLayout, PreparedArtifactNativeOperation,
    PreparedArtifactNativePlanFragment, PreparedArtifactNativePlaneLayout,
    PreparedArtifactNativeRequest,
};

#[derive(Clone)]
struct GeneratorSettings {
    shape: u64,
    value: f32,
    fail_at: Option<usize>,
    zero_workspace: bool,
}

impl Default for GeneratorSettings {
    fn default() -> Self {
        Self {
            shape: 6,
            value: 1.0,
            fail_at: None,
            zero_workspace: false,
        }
    }
}

struct Generator {
    settings: GeneratorSettings,
    calls: Arc<AtomicUsize>,
    workspace: Vec<u8>,
}

impl PreparedArtifactNativeGenerator for Generator {
    fn generate_cell(
        &mut self,
        index: usize,
        cell: EvlaAwCellRequest,
        workspace_limit: u64,
    ) -> Result<PreparedArtifactNativeLayout, PreparedArtifactError> {
        assert!(workspace_limit >= cell.generation_workspace_bytes().unwrap() as u64);
        if self.settings.fail_at == Some(index) {
            return Err(PreparedArtifactError::Interrupted);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.workspace.resize(128, 0);
        Ok(PreparedArtifactNativeLayout {
            imaging: PreparedArtifactNativePlaneLayout {
                shape: [self.settings.shape; 2],
                support: [1; 2],
            },
            weight: PreparedArtifactNativePlaneLayout {
                shape: [8; 2],
                support: [1; 2],
            },
        })
    }
    fn workspace_peak_bytes(&self) -> u64 {
        if self.settings.zero_workspace {
            0
        } else {
            self.workspace.capacity() as u64
        }
    }
}

impl PreparedArtifactGenerator for Generator {
    fn fill_segment(
        &mut self,
        _: &PreparedArtifactSegmentDescriptor,
        _: u64,
        output: &mut [u8],
    ) -> Result<(), PreparedArtifactError> {
        for scalar in output.chunks_exact_mut(4) {
            scalar.copy_from_slice(&self.settings.value.to_le_bytes());
        }
        Ok(())
    }
}

pub(super) struct NativeAdapter {
    id: WorkImplementationId,
    store: PreparedArtifactStore,
    request: PreparedArtifactNativeRequest,
    operation: PreparedArtifactNativeOperation,
    settings: GeneratorSettings,
    calls: Arc<AtomicUsize>,
    outcome: Mutex<Option<PreparedArtifactNativeCatalogOutcome>>,
}

impl WorkImplementation for NativeAdapter {
    type Error = io::Error;
    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }
    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let mut generator = Generator {
            settings: self.settings.clone(),
            calls: Arc::clone(&self.calls),
            workspace: vec![],
        };
        let selected = (self.operation != PreparedArtifactNativeOperation::Reuse)
            .then_some(&mut generator as &mut dyn PreparedArtifactNativeGenerator);
        let (outcome, measurements) = self
            .store
            .prepare_native_catalog(&context, &self.request, self.operation, selected)
            .map_err(prepared_io_error)?;
        *self.outcome.lock().unwrap() = Some(outcome);
        Ok(measurements)
    }
    fn failure_measurements<'e>(&'e self, error: &'e io::Error) -> Option<&'e WorkMeasurements> {
        error
            .get_ref()?
            .downcast_ref::<PreparedArtifactError>()?
            .work_measurements()
    }
    fn wait_for_fence(
        &self,
        _: WorkExecutionContext<'_>,
        _: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        Ok(WorkMeasurements::default())
    }
    fn complete_observation_read(
        &self,
        _: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(io::Error::other(
            "native preparation owns no observation traversal",
        ))
    }
    fn publish(&self, _: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn input(count: usize) -> NativeAwRequestInput {
    NativeAwRequestInput {
        surface: EvlaDishSurface::new(vec![[0.0, 0.0, 0.0], [6.25, 0.1, 0.1], [12.5, 0.2, 0.1]])
            .unwrap(),
        antenna_diameter_m: 25.0,
        frequencies: vec![NativeAwFrequencyGroup {
            spectral_window: 0,
            channel_frequencies_hz: vec![1.5e9],
            cf_frequency_hz: 1.5e9,
        }],
        w_values: (0..count).map(|n| n as f64).collect(),
        w_increment: 1.0,
        pa_values: vec![0.0],
        mueller_elements: vec![0],
        reference_frequency_hz: 1.5e9,
        grid: NativeAwGrid {
            size: 8,
            sky_increment_rad: [-0.01, 0.01],
            oversampling: 1,
        },
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: true,
            wideband: false,
            conjugate_beams: false,
        },
        maximum_cells: count,
    }
}

struct Fixture {
    suite: PreparedSuiteRegistry,
    physical: PhysicalWorkBinding,
    request: PreparedArtifactNativeRequest,
    id: WorkImplementationId,
    calls: Arc<AtomicUsize>,
}

fn fixture(
    problem: &casa_imaging_model::CompiledProblem,
    path: &Path,
    count: usize,
    operation: PreparedArtifactNativeOperation,
    settings: GeneratorSettings,
) -> Fixture {
    let budget = PreparedArtifactBudget::new(200_000, 8, 128).unwrap();
    let store = PreparedArtifactStore::open(path, prepared_storage_domain(), budget).unwrap();
    let (mut suite, owner) = prepared_catalog(prepared_registration());
    suite.metadata = Some(ImplementationContractMetadata::new(
        problem.problem_id(),
        problem.numerics_id(),
        problem.required_capabilities().clone(),
    ));
    let native = NativeAwRequest::new(problem.geometry().geometry_id(), input(count)).unwrap();
    let request =
        PreparedArtifactNativeRequest::new(&store, &suite, &owner, problem, native).unwrap();
    let fragment = PreparedArtifactNativePlanFragment::new(
        &request,
        &store,
        operation,
        WorkNodeId::new("prepared-phase-producer"),
        WorkNodeId::new("prepared-phase-commit"),
        implementation(6),
    )
    .unwrap();
    let id = fragment.work_implementation_id();
    let calls = Arc::new(AtomicUsize::new(0));
    suite.implementations.insert(
        implementation(6),
        PreparedSuiteImplementation::Base(Box::new(recording_executor(6, None, None))),
    );
    suite.implementations.insert(
        id.clone(),
        PreparedSuiteImplementation::Native(Box::new(NativeAdapter {
            id: id.clone(),
            store: PreparedArtifactStore::open(path, prepared_storage_domain(), budget).unwrap(),
            request: request.clone(),
            operation,
            settings,
            calls: Arc::clone(&calls),
            outcome: Mutex::new(None),
        })),
    );
    let base = PreparedArtifactNativePlanFragment::standalone_base(
        &suite,
        implementation(6),
        &request,
        &store,
        1_000,
        900_000,
    )
    .unwrap();
    let physical = fragment.compose(&base).unwrap();
    Fixture {
        suite,
        physical,
        request,
        id,
        calls,
    }
}

fn execute(
    problem: &casa_imaging_model::CompiledProblem,
    fixture: &Fixture,
    attempt: u8,
) -> (
    Result<ExecutionOutcome, RunError<io::Error>>,
    ExecutionReceipt,
) {
    let execution_plan = plan(
        problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(fixture.physical.clone()),
    )
    .unwrap();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt; 32]);
    let result = run_prepared(
        problem,
        &execution_plan,
        &fixture.suite,
        execution_plan.bind_receipt(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([210; 32]),
        )),
    );
    let receipt = execution_plan.receipt_store().open(attempt).unwrap();
    (result, receipt)
}

fn outcome(fixture: &Fixture) -> PreparedArtifactNativeCatalogOutcome {
    let PreparedSuiteImplementation::Native(adapter) = &fixture.suite.implementations[&fixture.id]
    else {
        unreachable!()
    };
    adapter.outcome.lock().unwrap().take().unwrap()
}

#[test]
fn native_catalog_cold_warm_exact_realization_and_workspace_admission() {
    let problem = compile(request(1)).unwrap();
    let cache = prepared_tempdir();
    let cold = fixture(
        &problem,
        cache.path(),
        2,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings::default(),
    );
    let allocations = cold.physical.execution_dag().logical_allocations();
    assert!(allocations.values().any(|allocation| {
        allocation
            .id
            .as_str()
            .starts_with("native-generation-workspace-")
            && allocation.bytes
                == cold.request.request().generation_workspace_bytes().unwrap() as u64
            && allocation.purpose == AllocationPurpose::Data
    }));
    let (result, receipt) = execute(&problem, &cold, 211);
    assert_eq!(result.unwrap(), ExecutionOutcome::Succeeded);
    assert_eq!(cold.calls.load(Ordering::SeqCst), 2);
    let generated = outcome(&cold);
    assert_eq!(generated.generation_workspace_peak_bytes(), 128);
    let exact = generated.into_complete().unwrap();
    for (index, (descriptor, artifact)) in exact.iter().enumerate() {
        assert_ne!(
            cold.request.cell_identity(index).unwrap(),
            descriptor.identity()
        );
        assert_eq!(
            receipt.artifact_disposition(cold.request.cell_identity(index).unwrap()),
            Some(ArtifactDisposition::Built)
        );
        assert_eq!(descriptor.identity(), artifact.identity());
        let imaging = descriptor.imaging_plane().unwrap();
        assert_eq!(
            imaging.order(),
            PreparedArtifactOrder::LastAxisContiguousLittleEndian
        );
        assert_eq!(imaging.precision(), PreparedArtifactPrecision::ComplexF32);
        assert_eq!(
            imaging.uv_affine().unwrap().increment_bits(),
            [-1.0 / (6.0 * 0.01_f64), 1.0 / (6.0 * 0.01)].map(f64::to_bits)
        );
    }
    let warm = fixture(
        &problem,
        cache.path(),
        2,
        PreparedArtifactNativeOperation::Reuse,
        GeneratorSettings::default(),
    );
    assert!(
        !warm
            .physical
            .execution_dag()
            .logical_allocations()
            .keys()
            .any(|id| id.as_str().starts_with("native-generation-workspace-"))
    );
    let (result, _) = execute(&problem, &warm, 212);
    assert_eq!(result.unwrap(), ExecutionOutcome::Succeeded);
    assert_eq!(warm.calls.load(Ordering::SeqCst), 0);
    let warmed = outcome(&warm);
    assert_eq!(warmed.generation_workspace_peak_bytes(), 0);
    let warm_exact = warmed.into_complete().unwrap();
    assert_eq!(
        exact
            .iter()
            .map(|(d, a)| (d.identity(), a.integrity_identity()))
            .collect::<Vec<_>>(),
        warm_exact
            .iter()
            .map(|(d, a)| (d.identity(), a.integrity_identity()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn native_catalog_failure_preserves_only_complete_prefix_and_explicit_restart() {
    let problem = compile(request(1)).unwrap();
    let cache = prepared_tempdir();
    let interrupted = fixture(
        &problem,
        cache.path(),
        2,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings {
            fail_at: Some(1),
            ..GeneratorSettings::default()
        },
    );
    let (result, receipt) = execute(&problem, &interrupted, 213);
    assert!(result.is_err());
    assert_eq!(
        receipt.artifact_disposition(interrupted.request.cell_identity(0).unwrap()),
        Some(ArtifactDisposition::Built)
    );
    assert_eq!(
        receipt.artifact_disposition(interrupted.request.cell_identity(1).unwrap()),
        None
    );
    assert_eq!(
        fs::read_dir(cache.path().join("objects-v3"))
            .unwrap()
            .count(),
        1
    );
    let warm = fixture(
        &problem,
        cache.path(),
        2,
        PreparedArtifactNativeOperation::Reuse,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &warm, 214).0.is_err());
    assert!(outcome(&warm).into_complete().is_err());
    assert_eq!(warm.calls.load(Ordering::SeqCst), 0);
    let restart = fixture(
        &problem,
        cache.path(),
        2,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings::default(),
    );
    assert_eq!(
        execute(&problem, &restart, 215).0.unwrap(),
        ExecutionOutcome::Succeeded
    );
    assert_eq!(restart.calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome(&restart).into_complete().unwrap().len(), 2);
}

#[test]
fn native_catalog_regeneration_rejects_valid_layout_and_content_conflicts() {
    let problem = compile(request(1)).unwrap();
    let cache = prepared_tempdir();
    let cold = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &cold, 216).0.is_ok());
    for (attempt, settings) in [
        (
            217,
            GeneratorSettings {
                shape: 8,
                ..GeneratorSettings::default()
            },
        ),
        (
            218,
            GeneratorSettings {
                value: 2.0,
                ..GeneratorSettings::default()
            },
        ),
    ] {
        let regenerated = fixture(
            &problem,
            cache.path(),
            1,
            PreparedArtifactNativeOperation::Regenerate,
            settings,
        );
        let (result, _) = execute(&problem, &regenerated, attempt);
        assert!(format!("{:?}", result.unwrap_err()).contains("PublicationConflict"));
        assert_eq!(
            fs::read_dir(cache.path().join("objects-v3"))
                .unwrap()
                .count(),
            1
        );
    }
    let same = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Regenerate,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &same, 219).0.is_ok());
}

#[test]
fn native_catalog_explicit_regeneration_replaces_corrupt_payload() {
    let problem = compile(request(1)).unwrap();
    let cache = prepared_tempdir();
    let cold = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &cold, 220).0.is_ok());
    let exact = outcome(&cold).into_complete().unwrap();
    let payload = cache
        .path()
        .join("objects-v3")
        .join(exact[0].0.identity().to_string())
        .join("payload.bin");
    let mut bytes = fs::read(&payload).unwrap();
    bytes[0] ^= 1;
    fs::write(&payload, bytes).unwrap();
    let warm = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Reuse,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &warm, 221).0.is_err());
    assert_eq!(warm.calls.load(Ordering::SeqCst), 0);
    let regenerated = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Regenerate,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &regenerated, 222).0.is_ok());
    assert_eq!(regenerated.calls.load(Ordering::SeqCst), 1);
    let complete = outcome(&regenerated).into_complete().unwrap();
    assert_eq!(
        exact[0].1.integrity_identity(),
        complete[0].1.integrity_identity()
    );
}

#[test]
fn native_catalog_zero_workspace_and_invalid_layout_fail_before_publication() {
    let problem = compile(request(1)).unwrap();
    for (attempt, settings) in [
        (
            223,
            GeneratorSettings {
                zero_workspace: true,
                ..GeneratorSettings::default()
            },
        ),
        (
            224,
            GeneratorSettings {
                shape: 10,
                ..GeneratorSettings::default()
            },
        ),
    ] {
        let cache = prepared_tempdir();
        let invalid = fixture(
            &problem,
            cache.path(),
            1,
            PreparedArtifactNativeOperation::Generate,
            settings,
        );
        assert!(execute(&problem, &invalid, attempt).0.is_err());
        assert_eq!(
            fs::read_dir(cache.path().join("objects-v3"))
                .unwrap()
                .count(),
            0
        );
    }
}

fn without_generation_workspace(fixture: &mut Fixture) {
    let base = &fixture.physical;
    let original = base.execution_dag();
    let selected = |id: &str| id.starts_with("native-generation-workspace-");
    let mut alternative = original.resource_alternative().clone();
    alternative
        .demand
        .memory
        .retain(|memory| !selected(&memory.allocation_id));
    let nodes = original
        .nodes()
        .values()
        .cloned()
        .map(|mut node| {
            node.allocations
                .retain(|allocation| !selected(allocation.allocation.as_str()));
            node
        })
        .collect();
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: original.required_resource_capabilities().clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: original
            .logical_allocations()
            .values()
            .filter(|allocation| !selected(allocation.id.as_str()))
            .cloned()
            .collect(),
        physical_slots: original
            .physical_slots()
            .values()
            .filter(|slot| !selected(slot.id.as_str()))
            .cloned()
            .collect(),
        initial_knobs: original.initial_knobs().clone(),
        adaptations: original.adaptations().values().cloned().collect(),
    })
    .unwrap();
    let catalog = ImplementationContractCatalog::from_registry(
        &fixture.suite,
        dag.nodes().values().map(|node| node.implementation.clone()),
    )
    .unwrap();
    fixture.physical = PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .unwrap();
}

#[test]
fn native_catalog_missing_workspace_and_foreign_request_fail_before_generation() {
    let problem = compile(request(1)).unwrap();
    let foreign = compile(request(2)).unwrap();
    for (attempt, mutate_request) in [(225, false), (226, true)] {
        let cache = prepared_tempdir();
        let mut invalid = fixture(
            &problem,
            cache.path(),
            1,
            PreparedArtifactNativeOperation::Generate,
            GeneratorSettings::default(),
        );
        if mutate_request {
            let foreign_fixture = fixture(
                &foreign,
                cache.path(),
                1,
                PreparedArtifactNativeOperation::Generate,
                GeneratorSettings::default(),
            );
            let PreparedSuiteImplementation::Native(adapter) =
                invalid.suite.implementations.get_mut(&invalid.id).unwrap()
            else {
                unreachable!()
            };
            adapter.request = foreign_fixture.request;
        } else {
            without_generation_workspace(&mut invalid);
        }
        let (result, _) = execute(&problem, &invalid, attempt);
        let error = format!("{:?}", result.unwrap_err());
        assert!(
            error.contains(if mutate_request {
                "ScientificBindingMismatch"
            } else {
                "MissingReservation"
            }),
            "{error}"
        );
        assert_eq!(invalid.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            fs::read_dir(cache.path().join("objects-v3"))
                .unwrap()
                .count(),
            0
        );
    }
}

fn execute_exact(
    problem: &casa_imaging_model::CompiledProblem,
    path: &Path,
    descriptor: PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    attempt_byte: u8,
) {
    let store = PreparedArtifactStore::open(
        path,
        prepared_storage_domain(),
        PreparedArtifactBudget::new(200_000, 8, 128).unwrap(),
    )
    .unwrap();
    let (mut suite, _) = prepared_registry(
        problem,
        PreparedOperationAdapter::new(operation, store, descriptor.clone()),
    );
    suite.implementations.insert(
        implementation(6),
        PreparedSuiteImplementation::Base(Box::new(recording_executor(6, None, None))),
    );
    let store = PreparedArtifactStore::open(
        path,
        prepared_storage_domain(),
        PreparedArtifactBudget::new(200_000, 8, 128).unwrap(),
    )
    .unwrap();
    let base = PreparedArtifactPlanFragment::standalone_base(
        problem,
        &suite,
        implementation(6),
        &descriptor,
        &store,
        1000,
        900_000,
    )
    .unwrap();
    let physical = PreparedArtifactPlanFragment::new(
        &descriptor,
        &store,
        operation,
        WorkNodeId::new("prepared-phase-producer"),
        WorkNodeId::new("prepared-phase-commit"),
        implementation(6),
    )
    .compose(&base)
    .unwrap();
    let execution_plan = plan(
        problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .unwrap();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt_byte; 32]);
    run_prepared(
        problem,
        &execution_plan,
        &suite,
        execution_plan.bind_receipt(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([210; 32]),
        )),
    )
    .unwrap();
}

fn alternate_descriptor(
    problem: &casa_imaging_model::CompiledProblem,
    path: &Path,
    request: &PreparedArtifactNativeRequest,
    order: PreparedArtifactOrder,
) -> PreparedArtifactDescriptor {
    let store = PreparedArtifactStore::open(
        path,
        prepared_storage_domain(),
        PreparedArtifactBudget::new(200_000, 8, 128).unwrap(),
    )
    .unwrap();
    let (catalog, implementation) = prepared_catalog(prepared_registration());
    let plane = || {
        PreparedArtifactPlaneDescriptor::new(
            [8; 2],
            [1; 2],
            1,
            PreparedArtifactUvAffine::new(
                [0.0; 2],
                [4.0; 2],
                [-12.5, 12.5],
                [[1.0, 0.0], [0.0, 1.0]],
            )
            .unwrap(),
            PreparedArtifactPrecision::ComplexF32,
            order,
        )
        .unwrap()
    };
    PreparedArtifactDescriptor::convolution_function(
        &store,
        &catalog,
        &implementation,
        problem,
        request.request().cell(0).unwrap().1,
        plane(),
        plane(),
    )
    .unwrap()
}

#[test]
fn native_catalog_warm_lookup_rejects_ambiguous_valid_layouts() {
    let problem = compile(request(1)).unwrap();
    let cache = prepared_tempdir();
    let cold = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &cold, 227).0.is_ok());
    let different = alternate_descriptor(
        &problem,
        cache.path(),
        &cold.request,
        PreparedArtifactOrder::LastAxisContiguousLittleEndian,
    );
    execute_exact(
        &problem,
        cache.path(),
        different,
        PreparedArtifactOperation::Generate,
        228,
    );
    let warm = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Reuse,
        GeneratorSettings::default(),
    );
    let (result, _) = execute(&problem, &warm, 229);
    assert!(format!("{:?}", result.unwrap_err()).contains("PublicationConflict"));
    assert_eq!(warm.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn native_catalog_warm_lookup_rejects_manifest_and_fixed_output_contract_tampering() {
    let problem = compile(request(1)).unwrap();
    for (attempt, wrong_order) in [(230, false), (233, true)] {
        let cache = prepared_tempdir();
        let cold = fixture(
            &problem,
            cache.path(),
            1,
            PreparedArtifactNativeOperation::Generate,
            GeneratorSettings::default(),
        );
        if wrong_order {
            let wrong = alternate_descriptor(
                &problem,
                cache.path(),
                &cold.request,
                PreparedArtifactOrder::Axis0ContiguousLittleEndian,
            );
            execute_exact(
                &problem,
                cache.path(),
                wrong,
                PreparedArtifactOperation::Generate,
                attempt,
            );
        } else {
            assert!(execute(&problem, &cold, attempt).0.is_ok());
            let exact = outcome(&cold).into_complete().unwrap();
            let manifest = cache
                .path()
                .join("objects-v3")
                .join(exact[0].0.identity().to_string())
                .join("manifest.json");
            let mut json: serde_json::Value =
                serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
            json["schema_version"] = 999.into();
            fs::write(manifest, serde_json::to_vec(&json).unwrap()).unwrap();
        }
        let warm = fixture(
            &problem,
            cache.path(),
            1,
            PreparedArtifactNativeOperation::Reuse,
            GeneratorSettings::default(),
        );
        let error = format!("{:?}", execute(&problem, &warm, attempt + 1).0.unwrap_err());
        assert!(
            error.contains(if wrong_order {
                "StaleArtifact"
            } else {
                "UnknownSchema"
            }),
            "{error}"
        );
        assert_eq!(warm.calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn native_catalog_rebinds_reused_exact_descriptor_to_current_execution() {
    let problem = compile(request(1)).unwrap();
    let current = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::SumWeights],
    ))
    .unwrap();
    assert_ne!(problem.problem_id(), current.problem_id());
    let cache = prepared_tempdir();
    let cold = fixture(
        &problem,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Generate,
        GeneratorSettings::default(),
    );
    assert!(execute(&problem, &cold, 235).0.is_ok());
    let old = outcome(&cold).into_complete().unwrap();
    let warm = fixture(
        &current,
        cache.path(),
        1,
        PreparedArtifactNativeOperation::Reuse,
        GeneratorSettings::default(),
    );
    assert_eq!(cold.request.cell_identity(0), warm.request.cell_identity(0));
    assert!(execute(&current, &warm, 236).0.is_ok());
    let rebound = outcome(&warm).into_complete().unwrap();
    assert_eq!(old[0].0.identity(), rebound[0].0.identity());
    execute_exact(
        &current,
        cache.path(),
        rebound.into_iter().next().unwrap().0,
        PreparedArtifactOperation::Reuse,
        237,
    );
}
