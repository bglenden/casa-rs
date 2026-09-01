// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

const PREPARED_PAYLOAD_BYTES: u64 = (3 * 3 + 5 * 5) * 8;

fn prepared_storage_domain() -> &'static StorageDomain {
    authority()
        .topology()
        .storage_domains
        .iter()
        .find(|domain| domain.id == StorageDomainId::new("atomic-output"))
        .expect("prepared storage domain")
}

fn secondary_source_domain() -> &'static StorageDomain {
    authority()
        .topology()
        .storage_domains
        .iter()
        .find(|domain| domain.id == StorageDomainId::new("prepared-source-secondary"))
        .expect("secondary prepared source domain")
}

fn prepared_tempdir() -> tempfile::TempDir {
    fs::create_dir_all(&prepared_storage_domain().root).expect("prepared storage root");
    tempfile::tempdir_in(&prepared_storage_domain().root).expect("prepared temporary directory")
}

fn directory_entry_names(path: &std::path::Path) -> BTreeSet<std::ffi::OsString> {
    fs::read_dir(path)
        .expect("directory inventory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect()
}

fn forged_rejection_identity(
    planned: ArtifactIdentity,
    rejection: PreparedArtifactRejection,
) -> ArtifactIdentity {
    let tag = match rejection {
        PreparedArtifactRejection::Missing => 0,
        PreparedArtifactRejection::Incomplete => 1,
        PreparedArtifactRejection::Incompatible => 2,
        PreparedArtifactRejection::Corrupt => 3,
        PreparedArtifactRejection::NonFinite => 4,
    };
    let mut hasher = Sha256::new();
    hasher.update(b"casa-rs/private-prepared-artifact/rejection\0");
    hasher.update(6_u32.to_le_bytes());
    hasher.update(planned.as_bytes());
    hasher.update([tag]);
    ArtifactIdentity::from_sha256(hasher.finalize().into())
}

fn expected_orphan_staging_identity(name: &str, bytes: u64) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(b"casa-rs/private-prepared-artifact/orphan-staging-evidence\0");
    hasher.update(6_u32.to_le_bytes());
    hasher.update((name.len() as u64).to_le_bytes());
    hasher.update(name.as_bytes());
    hasher.update(bytes.to_le_bytes());
    ArtifactIdentity::from_sha256(hasher.finalize().into())
}

fn expected_eviction_observed_identity(
    ledger: ArtifactIdentity,
    evictions: &[(ArtifactIdentity, u64)],
) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(b"casa-rs/private-prepared-artifact/eviction-observed\0");
    hasher.update(6_u32.to_le_bytes());
    hasher.update(ledger.as_bytes());
    hasher.update((evictions.len() as u64).to_le_bytes());
    for (identity, bytes) in evictions {
        hasher.update(identity.as_bytes());
        hasher.update(bytes.to_le_bytes());
    }
    ArtifactIdentity::from_sha256(hasher.finalize().into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PreparedObserved {
    Materialized {
        identity: ArtifactIdentity,
        integrity: ArtifactIdentity,
    },
    Rejected(PreparedArtifactRejection),
}

struct PreparedOperationAdapter {
    id: WorkImplementationId,
    operation: PreparedArtifactOperation,
    store: PreparedArtifactStore,
    descriptor: PreparedArtifactDescriptor,
    sources: Option<PreparedSourceFiles>,
    bound_source: Option<PreparedArtifactLoadSource>,
    observed: Mutex<Option<PreparedObserved>>,
}

struct PreparedSourceFiles {
    _directory: tempfile::TempDir,
    _weight_directory: Option<tempfile::TempDir>,
    imaging: PathBuf,
    weight: PathBuf,
    imaging_domain: StorageDomain,
    weight_domain: StorageDomain,
    producer: WorkNodeId,
}

impl PreparedSourceFiles {
    fn new() -> Self {
        let directory = prepared_tempdir();
        let imaging_path = directory.path().join("imaging.bin");
        let weight_path = directory.path().join("weight.bin");
        let (imaging, weight) = prepared_payloads();
        fs::write(&imaging_path, imaging).expect("prepared imaging source");
        fs::write(&weight_path, weight).expect("prepared weight source");
        Self {
            _directory: directory,
            _weight_directory: None,
            imaging: imaging_path,
            weight: weight_path,
            imaging_domain: prepared_storage_domain().clone(),
            weight_domain: prepared_storage_domain().clone(),
            producer: WorkNodeId::new("execute"),
        }
    }

    fn across_domains() -> Self {
        let mut files = Self::new();
        fs::create_dir_all(&secondary_source_domain().root)
            .expect("secondary prepared source root");
        let weight_directory = tempfile::tempdir_in(&secondary_source_domain().root)
            .expect("secondary prepared source directory");
        let weight_path = weight_directory.path().join("weight.bin");
        let (_, weight) = prepared_payloads();
        fs::write(&weight_path, weight).expect("secondary prepared weight source");
        files.weight = weight_path;
        files.weight_domain = secondary_source_domain().clone();
        files._weight_directory = Some(weight_directory);
        files
    }

    fn load_source(
        &self,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<PreparedArtifactLoadSource, PreparedArtifactError> {
        let (imaging, weight) = prepared_payloads();
        PreparedArtifactLoadSource::new(
            descriptor,
            self.producer.clone(),
            vec![
                PreparedArtifactSourceSegment::new(
                    "imaging",
                    self.imaging.clone(),
                    Sha256::digest(imaging).into(),
                    &self.imaging_domain,
                )?,
                PreparedArtifactSourceSegment::new(
                    "weight",
                    self.weight.clone(),
                    Sha256::digest(weight).into(),
                    &self.weight_domain,
                )?,
            ],
        )
    }
}

impl PreparedOperationAdapter {
    fn new(
        operation: PreparedArtifactOperation,
        store: PreparedArtifactStore,
        descriptor: PreparedArtifactDescriptor,
    ) -> Self {
        let id = descriptor.work_implementation_id(operation);
        Self::with_id(id, operation, store, descriptor)
    }

    fn with_id(
        id: WorkImplementationId,
        operation: PreparedArtifactOperation,
        store: PreparedArtifactStore,
        descriptor: PreparedArtifactDescriptor,
    ) -> Self {
        Self {
            id,
            operation,
            store,
            descriptor,
            sources: (operation == PreparedArtifactOperation::Load).then(PreparedSourceFiles::new),
            bound_source: None,
            observed: Mutex::new(None),
        }
    }
}

struct PreparedFailureAdapter {
    id: WorkImplementationId,
    evidence: WorkMeasurements,
    succeed: bool,
}

impl WorkImplementation for PreparedFailureAdapter {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, _context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        if self.succeed {
            Ok(self.evidence.clone())
        } else {
            Err(io::Error::other(
                "injected prepared-artifact mutation failure",
            ))
        }
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        Some(&self.evidence)
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        Ok(WorkMeasurements::default())
    }

    fn complete_observation_read(
        &self,
        _completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(io::Error::other(
            "prepared-failure adapter cannot own observation traversal",
        ))
    }

    fn publish(&self, _context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl WorkImplementation for PreparedOperationAdapter {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let source = if let Some(source) = &self.bound_source {
            Some(source.clone())
        } else {
            self.sources
                .as_ref()
                .map(|sources| sources.load_source(&self.descriptor))
                .transpose()
                .map_err(prepared_io_error)?
        };
        let (observed, measurements) = match self.operation {
            PreparedArtifactOperation::Generate => {
                let mut generator = fill_prepared_segment;
                let (artifact, measurements) = self
                    .store
                    .generate(&context, &self.descriptor, &mut generator)
                    .map_err(prepared_io_error)?;
                (
                    PreparedObserved::Materialized {
                        identity: artifact.identity(),
                        integrity: artifact.integrity_identity(),
                    },
                    measurements,
                )
            }
            PreparedArtifactOperation::Load => {
                let (artifact, measurements) = self
                    .store
                    .load(
                        &context,
                        &self.descriptor,
                        source.as_ref().expect("plan-bound load source"),
                    )
                    .map_err(prepared_io_error)?;
                (
                    PreparedObserved::Materialized {
                        identity: artifact.identity(),
                        integrity: artifact.integrity_identity(),
                    },
                    measurements,
                )
            }
            PreparedArtifactOperation::Reuse => {
                let (outcome, measurements) = self
                    .store
                    .reuse(&context, &self.descriptor)
                    .map_err(prepared_io_error)?;
                let observed = match outcome {
                    PreparedArtifactReuseOutcome::Reused(artifact) => {
                        PreparedObserved::Materialized {
                            identity: artifact.identity(),
                            integrity: artifact.integrity_identity(),
                        }
                    }
                    PreparedArtifactReuseOutcome::Rejected(rejection) => {
                        PreparedObserved::Rejected(rejection)
                    }
                };
                (observed, measurements)
            }
        };
        *self.observed.lock().expect("prepared outcome lock") = Some(observed);
        Ok(measurements)
    }

    fn failure_measurements<'error>(
        &'error self,
        error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        error
            .get_ref()?
            .downcast_ref::<casa_imaging_runtime::PreparedArtifactError>()?
            .work_measurements()
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        Ok(WorkMeasurements::default())
    }

    fn complete_observation_read(
        &self,
        _completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(io::Error::other(
            "prepared-artifact adapter cannot own observation traversal",
        ))
    }

    fn publish(&self, _context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

enum PreparedSuiteImplementation {
    Base(Box<RecordingExecutor>),
    Prepared(Box<PreparedOperationAdapter>),
    Failure(Box<PreparedFailureAdapter>),
}

impl WorkImplementation for PreparedSuiteImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        match self {
            Self::Base(adapter) => adapter.implementation_id(),
            Self::Prepared(adapter) => adapter.implementation_id(),
            Self::Failure(adapter) => adapter.implementation_id(),
        }
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        match self {
            Self::Base(adapter) => adapter.execute(context),
            Self::Prepared(adapter) => adapter.execute(context),
            Self::Failure(adapter) => adapter.execute(context),
        }
    }

    fn failure_measurements<'error>(
        &'error self,
        error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        match self {
            Self::Base(adapter) => adapter.failure_measurements(error),
            Self::Prepared(adapter) => adapter.failure_measurements(error),
            Self::Failure(adapter) => adapter.failure_measurements(error),
        }
    }

    fn wait_for_fence(
        &self,
        context: WorkExecutionContext<'_>,
        fence: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        match self {
            Self::Base(adapter) => adapter.wait_for_fence(context, fence),
            Self::Prepared(adapter) => adapter.wait_for_fence(context, fence),
            Self::Failure(adapter) => adapter.wait_for_fence(context, fence),
        }
    }

    fn complete_observation_read(
        &self,
        completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        match self {
            Self::Base(adapter) => adapter.complete_observation_read(completion),
            Self::Prepared(adapter) => adapter.complete_observation_read(completion),
            Self::Failure(adapter) => adapter.complete_observation_read(completion),
        }
    }

    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        match self {
            Self::Base(adapter) => adapter.publish(context),
            Self::Prepared(adapter) => adapter.publish(context),
            Self::Failure(adapter) => adapter.publish(context),
        }
    }
}

struct PreparedSuiteRegistry {
    id: ImplementationRegistryId,
    metadata: Option<ImplementationContractMetadata>,
    implementations: BTreeMap<WorkImplementationId, PreparedSuiteImplementation>,
    prepared: BTreeMap<WorkImplementationId, PreparedArtifactRegistration>,
}

impl ImplementationRegistry for PreparedSuiteRegistry {
    type Implementation = PreparedSuiteImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.implementations.get(id)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.implementations
            .contains_key(id)
            .then(|| self.metadata.clone())
            .flatten()
    }

    fn prepared_artifact_registration(
        &self,
        implementation: &WorkImplementationId,
    ) -> Option<&PreparedArtifactRegistration> {
        self.prepared.get(implementation)
    }
}

fn prepared_budget() -> PreparedArtifactBudget {
    PreparedArtifactBudget::new(32_768, 4, 64).expect("prepared cache budget")
}

fn prepared_registration() -> PreparedArtifactRegistration {
    PreparedArtifactRegistration::new(
        "casa-rs-imaging-v1",
        "native-awproject",
        "3.1.0",
        WorkImplementationId::new("native-awproject-cpu"),
    )
    .expect("prepared registration")
}

fn prepared_catalog(
    registration: PreparedArtifactRegistration,
) -> (PreparedSuiteRegistry, WorkImplementationId) {
    let implementation = registration.implementation().clone();
    (
        PreparedSuiteRegistry {
            id: registry(3),
            metadata: None,
            implementations: BTreeMap::new(),
            prepared: BTreeMap::from([(implementation.clone(), registration)]),
        },
        implementation,
    )
}

fn prepared_uv(shape: u64) -> PreparedArtifactUvAffine {
    PreparedArtifactUvAffine::new(
        [0.0, 0.0],
        [shape as f64 / 2.0; 2],
        [8.0 / shape as f64; 2],
        [[1.0, 0.0], [0.0, 1.0]],
    )
    .expect("prepared UV affine")
}

fn prepared_descriptor(
    store: &PreparedArtifactStore,
    problem: &casa_imaging_model::CompiledProblem,
) -> PreparedArtifactDescriptor {
    prepared_descriptor_with_registration(store, problem, prepared_registration())
}

fn prepared_descriptor_with_registration(
    store: &PreparedArtifactStore,
    problem: &casa_imaging_model::CompiledProblem,
    registration: PreparedArtifactRegistration,
) -> PreparedArtifactDescriptor {
    prepared_descriptor_with_registration_and_cell(
        store,
        problem,
        registration,
        prepared_cell(1.0e9),
    )
}

fn prepared_descriptor_with_registration_and_cell(
    store: &PreparedArtifactStore,
    problem: &casa_imaging_model::CompiledProblem,
    registration: PreparedArtifactRegistration,
    cell: PreparedArtifactScientificIdentity,
) -> PreparedArtifactDescriptor {
    prepared_descriptor_with_registry_registration_and_cell(
        store,
        problem,
        registry(3),
        registration,
        cell,
    )
}

fn prepared_descriptor_with_registry_registration_and_cell(
    store: &PreparedArtifactStore,
    problem: &casa_imaging_model::CompiledProblem,
    registry_id: ImplementationRegistryId,
    registration: PreparedArtifactRegistration,
    cell: PreparedArtifactScientificIdentity,
) -> PreparedArtifactDescriptor {
    let (mut catalog, implementation) = prepared_catalog(registration);
    catalog.id = registry_id;
    PreparedArtifactDescriptor::convolution_function(
        store,
        &catalog,
        &implementation,
        problem,
        cell,
        PreparedArtifactPlaneDescriptor::new(
            [3, 3],
            [1, 1],
            1,
            prepared_uv(3),
            PreparedArtifactPrecision::ComplexF32,
            PreparedArtifactOrder::Axis0ContiguousLittleEndian,
        )
        .expect("imaging plane"),
        PreparedArtifactPlaneDescriptor::new(
            [5, 5],
            [2, 2],
            1,
            prepared_uv(5),
            PreparedArtifactPrecision::ComplexF32,
            PreparedArtifactOrder::LastAxisContiguousLittleEndian,
        )
        .expect("weight plane"),
    )
    .expect("compiled-problem-derived descriptor")
}

fn prepared_cell(frequency_hz: f64) -> PreparedArtifactScientificIdentity {
    PreparedArtifactScientificIdentity::convolution_function(
        PreparedArtifactCellSemantics::new(
            frequency_hz,
            3.0,
            1,
            1,
            0.125,
            frequency_hz * 1.1,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
            "flatnoise",
        )
        .expect("prepared cell semantics"),
    )
    .expect("prepared cell")
}

fn prepared_payloads() -> (Vec<u8>, Vec<u8>) {
    let complex = |elements: usize, value: f32| {
        (0..elements)
            .flat_map(|_| {
                [value.to_le_bytes(), (-value).to_le_bytes()]
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>()
    };
    (complex(9, 1.25), complex(25, -2.5))
}

fn fill_prepared_segment(
    segment: &PreparedArtifactSegmentDescriptor,
    byte_offset: u64,
    output: &mut [u8],
) -> Result<(), PreparedArtifactError> {
    let value = match segment.name() {
        "imaging" => 1.25_f32,
        "weight" => -2.5_f32,
        _ => return Err(PreparedArtifactError::SegmentMismatch),
    };
    let mut pattern = [0_u8; 8];
    pattern[..4].copy_from_slice(&value.to_le_bytes());
    pattern[4..].copy_from_slice(&(-value).to_le_bytes());
    let offset =
        usize::try_from(byte_offset).map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = pattern[offset.wrapping_add(index) % pattern.len()];
    }
    Ok(())
}

fn operation_name(operation: PreparedArtifactOperation) -> &'static str {
    match operation {
        PreparedArtifactOperation::Generate => "generate",
        PreparedArtifactOperation::Load => "load",
        PreparedArtifactOperation::Reuse => "reuse",
    }
}

fn prepared_release_node_id(
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
) -> WorkNodeId {
    WorkNodeId::new(format!(
        "prepared-release-{}-{}",
        operation_name(operation),
        descriptor.identity()
    ))
}

fn prepared_base_executor(
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    bound_source: Option<&PreparedArtifactLoadSource>,
) -> RecordingExecutor {
    let mut base = recording_executor(6, None, None);
    if operation == PreparedArtifactOperation::Load {
        let sources = bound_source.is_none().then(PreparedSourceFiles::new);
        let source = if let Some(source) = bound_source {
            source.clone()
        } else {
            sources
                .as_ref()
                .expect("default load source")
                .load_source(descriptor)
                .expect("canonical plan-bound load source")
        };
        base.measurements
            .entry(WorkNodeId::new("execute"))
            .or_insert_with(|| (Vec::new(), Vec::new()))
            .1
            .push(artifact_measurement(
                source.identity(),
                Some(source.identity()),
                ArtifactDisposition::Loaded,
                PREPARED_PAYLOAD_BYTES,
                sources
                    .as_ref()
                    .map(|sources| RedactedPath::from_path(sources._directory.path())),
            ));
    }
    base.measurements.insert(
        prepared_release_node_id(descriptor, operation),
        (
            vec![IoMeasurement::new(IoBufferKind::StorageManager, 0, 0)],
            Vec::new(),
        ),
    );
    base
}

fn prepared_storage_resource(
    descriptor: &PreparedArtifactDescriptor,
    use_kind: StorageUseKind,
) -> LeaseResource {
    LeaseResource::Storage {
        demand_id: format!("private-prepared-cache-{}", descriptor.cache_identity()),
        use_kind,
    }
}

fn prepared_physical_work(
    problem: &casa_imaging_model::CompiledProblem,
    descriptor: &PreparedArtifactDescriptor,
    store: &PreparedArtifactStore,
    operation: PreparedArtifactOperation,
) -> PhysicalWorkBinding {
    let base = physical_work_for_problem(problem, 6);
    let sources = (operation == PreparedArtifactOperation::Load).then(PreparedSourceFiles::new);
    let load_source = sources.as_ref().map(|sources| {
        sources
            .load_source(descriptor)
            .expect("canonical plan-bound load source")
    });
    let fragment = PreparedArtifactPlanFragment::new(
        descriptor,
        store,
        operation,
        WorkNodeId::new("execute"),
        WorkNodeId::new("transaction-commit"),
        implementation(6),
    );
    let fragment = if let Some(source) = load_source.as_ref() {
        fragment.with_load_source(source)
    } else {
        fragment
    };
    fragment.compose(&base).expect("prepared physical work")
}
fn prepared_registry(
    problem: &casa_imaging_model::CompiledProblem,
    adapter: PreparedOperationAdapter,
) -> (PreparedSuiteRegistry, WorkImplementationId) {
    let prepared_id = adapter.id.clone();
    let planned_operation = [
        PreparedArtifactOperation::Generate,
        PreparedArtifactOperation::Load,
        PreparedArtifactOperation::Reuse,
    ]
    .into_iter()
    .find(|operation| adapter.descriptor.work_implementation_id(*operation) == prepared_id)
    .expect("adapter identity names one canonical prepared operation");
    let base = prepared_base_executor(
        &adapter.descriptor,
        planned_operation,
        adapter.bound_source.as_ref(),
    );
    (
        PreparedSuiteRegistry {
            id: registry(3),
            metadata: Some(super::implementation_metadata(problem)),
            implementations: BTreeMap::from([
                (
                    implementation(6),
                    PreparedSuiteImplementation::Base(Box::new(base)),
                ),
                (
                    prepared_id.clone(),
                    PreparedSuiteImplementation::Prepared(Box::new(adapter)),
                ),
            ]),
            prepared: BTreeMap::from([(
                prepared_registration().implementation().clone(),
                prepared_registration(),
            )]),
        },
        prepared_id,
    )
}

fn run_prepared(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    registry: &PreparedSuiteRegistry,
    receipt: ExecutionReceiptBinding<'_>,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");
    let provenance = receipt.provenance().clone();
    let canonical_receipt = plan.bind_receipt(provenance);
    runtime_run(
        &executable,
        plan,
        &current,
        registry,
        authority(),
        &mut RunToCompletion,
        canonical_receipt,
    )
}

struct PreparedRunExpectation {
    attempt_byte: u8,
    build_byte: u8,
    expect_rejection: bool,
}

fn execute_prepared_operation(
    problem: &casa_imaging_model::CompiledProblem,
    receipts: &ExecutionReceiptStore,
    store: PreparedArtifactStore,
    descriptor: PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    expectation: PreparedRunExpectation,
) -> PreparedObserved {
    let plan = plan_with_receipts(
        problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        receipts,
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &descriptor,
                &store,
                operation,
            ))
        },
    )
    .expect("prepared operation plan");
    let adapter = PreparedOperationAdapter::new(operation, store, descriptor);
    let (registry, id) = prepared_registry(problem, adapter);
    let result = run_prepared(
        problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([expectation.attempt_byte; 32]),
            BuildIdentity::from_sha256([expectation.build_byte; 32]),
        )),
    );
    if expectation.expect_rejection {
        assert!(matches!(
            result,
            Err(RunError::Evidence(
                ExecutionEvidenceError::RejectedArtifact { .. }
            ))
        ));
    } else {
        result.expect("prepared operation execution");
    }
    prepared_adapter_observed(&registry, &id)
}

fn prepared_io_error(error: casa_imaging_runtime::PreparedArtifactError) -> io::Error {
    io::Error::other(error)
}

fn assert_rejection_evidence(
    receipt: &ExecutionReceipt,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    rejection: PreparedArtifactRejection,
) {
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert_eq!(
        receipt.artifact_disposition(descriptor.identity()),
        Some(ArtifactDisposition::RejectedStale)
    );
    let observed = ArtifactIdentity::from_sha256(
        receipt
            .artifact_observed_identity(descriptor.identity())
            .expect("durable rejection evidence"),
    );
    assert_eq!(
        observed,
        forged_rejection_identity(descriptor.identity(), rejection),
        "the durable schema retains the exact domain-bound rejection identity"
    );
    assert_eq!(
        PreparedArtifactRejection::from_evidence_identity(descriptor.identity(), observed),
        Some(rejection)
    );
    assert!(
        receipt
            .artifact_actual_bytes(descriptor.identity())
            .is_some()
    );
    assert!(
        receipt
            .stage_actual_io(
                &descriptor.work_node_id(operation),
                IoBufferKind::StorageManager,
            )
            .is_some_and(|(_, operations)| operations > 0)
    );
    assert!(
        receipt
            .stage_actual_elapsed_nanos(&descriptor.work_node_id(operation))
            .is_some(),
        "failing a rejected cache node must preserve its measured elapsed time"
    );
}

fn prepared_adapter_observed(
    registry: &PreparedSuiteRegistry,
    id: &WorkImplementationId,
) -> PreparedObserved {
    let Some(PreparedSuiteImplementation::Prepared(adapter)) = registry.implementations.get(id)
    else {
        panic!("prepared adapter missing")
    };
    adapter
        .observed
        .lock()
        .expect("prepared outcome lock")
        .expect("prepared adapter outcome")
}

#[test]
fn prepared_streaming_residency_is_admitted_once_without_overlapping_slots() {
    let problem = compile(request(1)).expect("single-charge prepared problem");
    let cache_directory = prepared_tempdir();
    let budget =
        PreparedArtifactBudget::new(32_768, 800, 65_536).expect("single-charge constrained budget");
    let store =
        PreparedArtifactStore::open(cache_directory.path(), prepared_storage_domain(), budget)
            .expect("single-charge prepared store");
    let descriptor = prepared_descriptor(&store, &problem);
    let reservation = store
        .reservation(&descriptor, PreparedArtifactOperation::Generate)
        .expect("single-charge prepared reservation");
    assert_eq!(
        reservation.source_read_bytes(),
        0,
        "the declared generator has no path-backed source read"
    );
    assert_eq!(reservation.source_descriptor_bytes(), 0);
    assert_eq!(reservation.file_descriptors(), 2);
    let base = physical_work_for_problem(&problem, 6);
    let base_alternative = base.execution_dag().resource_alternative();
    let base_locks = base_alternative.demand.locks;
    let base_file_descriptors = base_alternative.demand.file_descriptors;
    let base_storage_manager = base_alternative.demand.io_buffers.storage_manager_bytes;
    let work = PreparedArtifactPlanFragment::new(
        &descriptor,
        &store,
        PreparedArtifactOperation::Generate,
        WorkNodeId::new("execute"),
        WorkNodeId::new("transaction-commit"),
        implementation(6),
    )
    .compose(&base)
    .expect("single-charge prepared composition");
    let composed = work.execution_dag().resource_alternative();
    assert_eq!(composed.demand.locks.hard(), base_locks.hard().max(2));
    assert_eq!(
        composed.demand.file_descriptors.hard(),
        base_file_descriptors
            .hard()
            .max(reservation.file_descriptors())
    );
    assert_eq!(
        composed.demand.io_buffers.storage_manager_bytes,
        base_storage_manager.max(reservation.resident_buffer_bytes())
    );

    let prepared_demands = work
        .execution_dag()
        .resource_alternative()
        .demand
        .memory
        .iter()
        .filter(|demand| demand.allocation_id.starts_with("prepared-"))
        .collect::<Vec<_>>();
    assert_eq!(prepared_demands.len(), 1);
    assert_eq!(
        prepared_demands[0].hard_bytes,
        reservation.resident_buffer_bytes()
    );

    let prepared_slots = work
        .execution_dag()
        .physical_slots()
        .values()
        .filter(|slot| slot.id.as_str().starts_with("prepared-"))
        .collect::<Vec<_>>();
    assert_eq!(prepared_slots.len(), 1);
    assert_eq!(
        prepared_slots[0].capacity_bytes,
        reservation.resident_buffer_bytes()
    );

    let node = &work.execution_dag().nodes()
        [&descriptor.work_node_id(PreparedArtifactOperation::Generate)];
    assert_eq!(node.allocations.len(), 1);
    assert_eq!(
        node.allocations[0].allocation,
        AllocationId::new(format!(
            "prepared-resident-buffer-generate-{}",
            descriptor.identity()
        ))
    );

    let balanced_host_limit = 1_048_576 * 3 / 4;
    let admitted_host_bytes = work
        .execution_dag()
        .resource_alternative()
        .demand
        .memory
        .iter()
        .map(|demand| demand.hard_bytes)
        .sum::<u64>();
    assert!(
        admitted_host_bytes <= balanced_host_limit,
        "single-buffer plan needs {admitted_host_bytes} host bytes within {balanced_host_limit}"
    );
    assert!(
        admitted_host_bytes + 2 * reservation.streaming_buffer_bytes() > balanced_host_limit,
        "the former duplicate source/writeback slots would make this feasible plan look infeasible"
    );

    plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(work),
    )
    .expect("a feasible single-buffer prepared plan must be admitted");
}

#[test]
fn distinct_prepared_cells_compose_without_resource_identity_collisions() {
    let problem = compile(request(1)).expect("multi-cell prepared problem");
    let cache = prepared_tempdir();
    let shared_budget =
        PreparedArtifactBudget::new(600_000, 8, 128).expect("shared domain-capacity budget");
    let store = PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), shared_budget)
        .expect("multi-cell prepared store");
    let first = prepared_descriptor_with_registration_and_cell(
        &store,
        &problem,
        prepared_registration(),
        prepared_cell(1.0e9),
    );
    let second = prepared_descriptor_with_registration_and_cell(
        &store,
        &problem,
        prepared_registration(),
        prepared_cell(1.1e9),
    );
    assert_ne!(first.identity(), second.identity());

    let base = physical_work_for_problem(&problem, 6);
    let first_work = PreparedArtifactPlanFragment::new(
        &first,
        &store,
        PreparedArtifactOperation::Generate,
        WorkNodeId::new("execute"),
        WorkNodeId::new("transaction-commit"),
        implementation(6),
    )
    .compose(&base)
    .expect("first prepared cell");
    let composed = PreparedArtifactPlanFragment::new(
        &second,
        &store,
        PreparedArtifactOperation::Generate,
        WorkNodeId::new("execute"),
        WorkNodeId::new("transaction-commit"),
        implementation(6),
    )
    .compose(&first_work)
    .expect("second prepared cell");

    let prepared_allocations = composed
        .execution_dag()
        .logical_allocations()
        .keys()
        .filter(|id| {
            id.as_str()
                .starts_with("prepared-resident-buffer-generate-")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(prepared_allocations.len(), 2);
    let prepared_demands = composed
        .execution_dag()
        .resource_alternative()
        .demand
        .storage
        .iter()
        .filter(|demand| demand.demand_id.starts_with("private-prepared-cache-"))
        .collect::<Vec<_>>();
    assert_eq!(prepared_demands.len(), 1);
    assert_eq!(
        prepared_demands[0].persistent_cache_bytes,
        shared_budget.cache_bytes(),
        "one store budget is charged once even when multiple cells share it"
    );
    plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(composed),
    )
    .expect("shared cache demand remains feasible below domain capacity");
}

#[test]
fn prepared_sources_are_bounded_accounted_files_and_never_casa_tables() {
    assert!(matches!(
        PreparedArtifactSourceSegment::new(
            "imaging",
            PathBuf::from("relative.bin"),
            [0; 32],
            prepared_storage_domain(),
        ),
        Err(PreparedArtifactError::InvalidSource)
    ));
    assert!(matches!(
        PreparedArtifactSourceSegment::new(
            "imaging",
            PathBuf::from(format!(
                "/{}",
                "x".repeat(PreparedArtifactSourceSegment::MAX_PATH_BYTES + 1)
            )),
            [0; 32],
            prepared_storage_domain(),
        ),
        Err(PreparedArtifactError::InvalidSource)
    ));

    let source_root = prepared_tempdir();
    let other_root = prepared_tempdir();
    let source_path = source_root.path().join("imaging.bin");
    fs::write(&source_path, []).expect("domain-mismatch source");
    let mut wrong_source_domain = prepared_storage_domain().clone();
    wrong_source_domain.root = other_root.path().to_path_buf();
    assert!(matches!(
        PreparedArtifactSourceSegment::new(
            "imaging",
            source_path,
            Sha256::digest([]).into(),
            &wrong_source_domain,
        ),
        Err(PreparedArtifactError::InvalidSource)
    ));
    assert!(matches!(
        PreparedArtifactStore::open(source_root.path(), &wrong_source_domain, prepared_budget(),),
        Err(PreparedArtifactError::InvalidDescriptor)
    ));

    let problem = compile(request(1)).expect("CASA-source problem");
    let cache_directory = prepared_tempdir();
    let store = PreparedArtifactStore::open(
        cache_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("CASA-source store");
    let descriptor = prepared_descriptor(&store, &problem);
    let operation = PreparedArtifactOperation::Load;
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &descriptor,
                &store,
                operation,
            ))
        },
    )
    .expect("CASA-source plan");
    let casa_table = prepared_tempdir();
    fs::write(casa_table.path().join("table.dat"), b"CASA table marker")
        .expect("CASA table marker");
    let (imaging, _) = prepared_payloads();
    let casa_imaging = casa_table.path().join("imaging.bin");
    fs::write(&casa_imaging, imaging).expect("CASA-contained source bytes");
    let mut adapter = PreparedOperationAdapter::new(operation, store, descriptor.clone());
    adapter.sources.as_mut().expect("load sources").imaging = casa_imaging;
    let (registry, _) = prepared_registry(&problem, adapter);
    let receipts = plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([164; 32]);
    let error = run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([165; 32]),
        )),
    )
    .expect_err("CASA table bytes cannot enter through the private source contract");
    assert!(matches!(
        error,
        RunError::Execution { source, .. } if source.to_string().contains("CASA-visible")
    ));
    let receipt = receipts.open(attempt).expect("CASA-source receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.failure_kind(), Some(ReceiptFailureKind::Adapter));
    assert_eq!(
        receipt.actual_resource_peak(
            &descriptor.work_node_id(operation),
            &LeaseResource::FileDescriptors,
            &ClaimLifetime::Work,
        ),
        Some(3),
        "the failed source validation retains its lock, staging, and source descriptor peak"
    );
    assert_eq!(
        fs::read_dir(cache_directory.path().join("objects-v3"))
            .expect("rolled-back source cache")
            .count(),
        0
    );
}

#[test]
fn cold_load_rejects_missing_mismatched_unlisted_and_wrong_producer_sources() {
    let problem = compile(request(1)).expect("source-binding problem");
    let cases = [
        ("missing", PreparedArtifactError::IncompleteArtifact),
        ("mismatched", PreparedArtifactError::SourceIdentityMismatch),
        ("unlisted", PreparedArtifactError::UnplannedSource),
        (
            "wrong-producer",
            PreparedArtifactError::SourceProducerMismatch,
        ),
    ];

    for (index, (case, expected)) in cases.into_iter().enumerate() {
        let cache = prepared_tempdir();
        let store =
            PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), prepared_budget())
                .expect("source-binding store");
        let descriptor = prepared_descriptor(&store, &problem);
        let planned_operation = if case == "unlisted" {
            PreparedArtifactOperation::Generate
        } else {
            PreparedArtifactOperation::Load
        };
        let plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |problem, _| {
                Ok::<_, ()>(prepared_physical_work(
                    problem,
                    &descriptor,
                    &store,
                    planned_operation,
                ))
            },
        )
        .expect("source-binding plan");
        let mut adapter = PreparedOperationAdapter::with_id(
            descriptor.work_implementation_id(planned_operation),
            PreparedArtifactOperation::Load,
            store,
            descriptor.clone(),
        );
        let sources = adapter.sources.as_mut().expect("load source fixture");
        match case {
            "missing" => sources.imaging = sources._directory.path().join("missing-imaging.bin"),
            "mismatched" => fs::write(&sources.imaging, vec![0_u8; 3 * 3 * 8])
                .expect("same-size mismatched source"),
            "unlisted" => {}
            "wrong-producer" => sources.producer = WorkNodeId::new("prepare"),
            _ => unreachable!("complete source-binding case table"),
        }
        let (registry, _) = prepared_registry(&problem, adapter);
        let receipts = plan.receipt_store();
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256(
            [170 + u8::try_from(index).expect("bounded case index"); 32],
        );
        let error = run_prepared(
            &problem,
            &plan,
            &registry,
            receipts.bind(execution_provenance(
                attempt,
                BuildIdentity::from_sha256(
                    [180 + u8::try_from(index).expect("bounded case index"); 32],
                ),
            )),
        )
        .expect_err("invalid source binding must fail closed");
        assert!(matches!(
            error,
            RunError::Execution { source, .. } if source.to_string() == expected.to_string()
        ));
        assert_eq!(
            receipts
                .open(attempt)
                .expect("failed source receipt")
                .status(),
            ReceiptStatus::Failed
        );
        assert_eq!(
            fs::read_dir(cache.path().join("objects-v3"))
                .expect("failed source cache inventory")
                .count(),
            0,
            "{case} source cannot publish a prepared entry"
        );
    }
}

#[test]
fn cold_load_source_identity_is_owned_and_accounted_by_its_predecessor_receipt() {
    let problem = compile(request(1)).expect("source-receipt problem");
    let cache = prepared_tempdir();
    let store =
        PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), prepared_budget())
            .expect("source-receipt store");
    let descriptor = prepared_descriptor(&store, &problem);
    let work = prepared_physical_work(
        &problem,
        &descriptor,
        &store,
        PreparedArtifactOperation::Load,
    );
    let cache_demand_id = format!("private-prepared-cache-{}", descriptor.cache_identity());
    let source_read_resource = work.execution_dag().nodes()
        [&descriptor.work_node_id(PreparedArtifactOperation::Load)]
        .claims
        .iter()
        .find_map(|claim| match &claim.resource {
            LeaseResource::StorageReadRate { demand_id } if demand_id != &cache_demand_id => {
                Some(claim.resource.clone())
            }
            _ => None,
        })
        .expect("separate cold-source read claim");
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(work),
    )
    .expect("source-receipt plan");
    let adapter =
        PreparedOperationAdapter::new(PreparedArtifactOperation::Load, store, descriptor.clone());
    let source_identity = adapter
        .sources
        .as_ref()
        .expect("load source fixture")
        .load_source(&descriptor)
        .expect("plan-bound source")
        .identity();
    let (registry, _) = prepared_registry(&problem, adapter);
    let receipts = plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([193; 32]);
    run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([194; 32]),
        )),
    )
    .expect("plan-bound cold load");

    let receipt = receipts.open(attempt).expect("source receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert_eq!(
        receipt.artifact_disposition(source_identity),
        Some(ArtifactDisposition::Loaded)
    );
    assert_eq!(
        receipt.artifact_role(source_identity),
        Some(ArtifactRole::Input)
    );
    assert_eq!(
        receipt.artifact_node(source_identity),
        Some(WorkNodeId::new("execute"))
    );
    assert_eq!(
        receipt.artifact_actual_bytes(source_identity),
        Some(PREPARED_PAYLOAD_BYTES)
    );
    assert_eq!(
        receipt.actual_resource_peak(
            &descriptor.work_node_id(PreparedArtifactOperation::Load),
            &source_read_resource,
            &ClaimLifetime::Work,
        ),
        Some(1),
        "cold source reads are attributed to their source-domain demand"
    );
}

#[cfg(unix)]
#[test]
fn cold_load_rejects_post_plan_symlink_escape_and_keeps_domains_exact() {
    use std::os::unix::fs::symlink;

    let problem = compile(request(1)).expect("multi-domain source problem");
    let cache = prepared_tempdir();
    let store =
        PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), prepared_budget())
            .expect("multi-domain source store");
    let descriptor = prepared_descriptor(&store, &problem);
    let sources = PreparedSourceFiles::across_domains();
    fs::remove_file(&sources.weight).expect("reserve nonexistent in-domain source path");
    let bound_source = sources
        .load_source(&descriptor)
        .expect("plan nonexistent in-domain source path");
    let work = PreparedArtifactPlanFragment::new(
        &descriptor,
        &store,
        PreparedArtifactOperation::Load,
        WorkNodeId::new("execute"),
        WorkNodeId::new("transaction-commit"),
        implementation(6),
    )
    .with_load_source(&bound_source)
    .compose(&physical_work_for_problem(&problem, 6))
    .expect("multi-domain source composition");
    let source_demand = |domain: &StorageDomainId| {
        work.execution_dag()
            .resource_alternative()
            .demand
            .storage
            .iter()
            .find(|demand| {
                &demand.domain == domain
                    && demand.persistent_cache_bytes == 0
                    && demand.read_rate.hard() > 0
            })
            .map(|demand| LeaseResource::StorageReadRate {
                demand_id: demand.demand_id.clone(),
            })
            .expect("exact source-domain demand")
    };
    let first_source_read = source_demand(&prepared_storage_domain().id);
    let untouched_source_read = source_demand(&secondary_source_domain().id);
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(work),
    )
    .expect("multi-domain source plan");

    let outside = prepared_tempdir();
    let outside_weight = outside.path().join("escaped-weight.bin");
    let (_, weight) = prepared_payloads();
    fs::write(&outside_weight, weight).expect("outside source target");
    symlink(&outside_weight, &sources.weight).expect("post-plan source replacement");
    let mut adapter =
        PreparedOperationAdapter::new(PreparedArtifactOperation::Load, store, descriptor.clone());
    adapter.sources = Some(sources);
    adapter.bound_source = Some(bound_source);
    let (registry, _) = prepared_registry(&problem, adapter);
    let receipts = plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([195; 32]);
    let error = run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([196; 32]),
        )),
    )
    .expect_err("post-plan source escape must fail closed");
    assert!(matches!(
        error,
        RunError::Execution { source, .. }
            if source.to_string() == PreparedArtifactError::InvalidSource.to_string()
    ));
    let receipt = receipts
        .open(attempt)
        .expect("multi-domain failure receipt");
    let load_node = descriptor.work_node_id(PreparedArtifactOperation::Load);
    assert_eq!(
        receipt.actual_resource_peak(&load_node, &first_source_read, &ClaimLifetime::Work),
        Some(1),
        "the first domain retains its completed source activity"
    );
    assert_eq!(
        receipt.actual_resource_peak(&load_node, &untouched_source_read, &ClaimLifetime::Work),
        Some(0),
        "the escaped second domain receives no activity attribution"
    );
}

#[test]
fn cold_load_cannot_run_without_its_predecessor_source_receipt() {
    let problem = compile(request(1)).expect("missing-source-receipt problem");
    let cache = prepared_tempdir();
    let store =
        PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), prepared_budget())
            .expect("missing-source-receipt store");
    let descriptor = prepared_descriptor(&store, &problem);
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &descriptor,
                &store,
                PreparedArtifactOperation::Load,
            ))
        },
    )
    .expect("missing-source-receipt plan");
    let adapter =
        PreparedOperationAdapter::new(PreparedArtifactOperation::Load, store, descriptor.clone());
    let source_identity = adapter
        .sources
        .as_ref()
        .expect("load source fixture")
        .load_source(&descriptor)
        .expect("plan-bound source")
        .identity();
    let (mut registry, _) = prepared_registry(&problem, adapter);
    let PreparedSuiteImplementation::Base(base) = registry
        .implementations
        .get_mut(&implementation(6))
        .expect("source producer implementation")
    else {
        panic!("execute implementation must be the source producer");
    };
    base.measurements
        .get_mut(&WorkNodeId::new("execute"))
        .expect("source producer measurements")
        .1
        .retain(|measurement| measurement.planned_identity() != source_identity);
    let receipts = plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([195; 32]);
    let error = run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([196; 32]),
        )),
    )
    .expect_err("unreceipted source must stop before cold load");

    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::MissingArtifact {
            artifact,
            ..
        }) if artifact == source_identity
    ));
    let receipt = receipts.open(attempt).expect("missing-source receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert_eq!(
        fs::read_dir(cache.path().join("objects-v3"))
            .expect("unreceipted source cache inventory")
            .count(),
        0
    );
}

#[test]
fn public_prepared_generate_reuse_and_load_are_plan_and_receipt_bound() {
    let problem = compile(request(1)).expect("prepared compiled problem");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(16, 2_000_000).expect("prepared receipt retention"),
    )
    .expect("prepared receipt store");

    let generated_directory = prepared_tempdir();
    let generated_store = PreparedArtifactStore::open(
        generated_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("generated store");
    let generated_descriptor = prepared_descriptor(&generated_store, &problem);
    let generated_reservation = generated_store
        .reservation(&generated_descriptor, PreparedArtifactOperation::Generate)
        .expect("generated reservation");
    assert_eq!(
        generated_reservation.source_read_bytes(),
        0,
        "generation has no caller-supplied source path"
    );
    assert_eq!(generated_reservation.file_descriptors(), 2);
    assert!(
        generated_reservation.resident_buffer_bytes()
            > generated_reservation.streaming_buffer_bytes() + 64 * 1024,
        "resident reservation must also charge bounded cache-inventory storage"
    );
    let generate_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &generated_descriptor,
                &generated_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("generate plan");
    let generate_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Generate,
        generated_store,
        generated_descriptor.clone(),
    );
    let (generate_registry, generate_id) = prepared_registry(&problem, generate_adapter);
    let generate_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([91; 32]);
    run_prepared(
        &problem,
        &generate_plan,
        &generate_registry,
        receipts.bind(execution_provenance(
            generate_attempt,
            BuildIdentity::from_sha256([92; 32]),
        )),
    )
    .expect("generate execution");
    let PreparedObserved::Materialized {
        integrity: generated_integrity,
        ..
    } = prepared_adapter_observed(&generate_registry, &generate_id)
    else {
        panic!("generation did not materialize")
    };
    let generate_receipt = receipts.open(generate_attempt).expect("generate receipt");
    assert_eq!(generate_receipt.status(), ReceiptStatus::Completed);
    assert!(
        generate_receipt
            .selected_implementation_identities()
            .contains(
                &generated_descriptor.work_implementation_id(PreparedArtifactOperation::Generate,)
            )
    );
    assert_eq!(
        generate_receipt.artifact_disposition(generated_descriptor.identity()),
        Some(ArtifactDisposition::Built)
    );
    assert_eq!(
        generate_receipt.artifact_role(generated_descriptor.identity()),
        Some(ArtifactRole::Prepared)
    );
    assert_eq!(
        generate_receipt.artifact_node(generated_descriptor.identity()),
        Some(generated_descriptor.work_node_id(PreparedArtifactOperation::Generate))
    );
    assert_eq!(
        generate_receipt.artifact_cache_identity(generated_descriptor.identity()),
        Some(generated_descriptor.cache_identity().as_bytes())
    );
    assert_eq!(
        generate_receipt.artifact_observed_identity(generated_descriptor.identity()),
        Some(generated_integrity.as_bytes())
    );
    assert_eq!(
        generate_receipt.artifact_actual_bytes(generated_descriptor.identity()),
        Some(PREPARED_PAYLOAD_BYTES)
    );
    assert_eq!(
        generate_receipt.actual_resource_peak(
            &generated_descriptor.work_node_id(PreparedArtifactOperation::Generate),
            &LeaseResource::FileDescriptors,
            &ClaimLifetime::Work,
        ),
        Some(generated_reservation.file_descriptors())
    );
    assert_eq!(
        generate_receipt.actual_resource_peak(
            &generated_descriptor.work_node_id(PreparedArtifactOperation::Generate),
            &LeaseResource::Locks,
            &ClaimLifetime::Work,
        ),
        Some(1)
    );
    let generated_node = generated_descriptor.work_node_id(PreparedArtifactOperation::Generate);
    let persistent_cache =
        prepared_storage_resource(&generated_descriptor, StorageUseKind::PersistentCache);
    assert_eq!(
        generate_receipt.planned_resource_amount(
            &generated_node,
            &persistent_cache,
            &ClaimLifetime::Work,
        ),
        Some(prepared_budget().cache_bytes())
    );
    let actual_cache_bytes = generate_receipt
        .actual_resource_peak(&generated_node, &persistent_cache, &ClaimLifetime::Work)
        .expect("actual persistent cache bytes");
    assert!(actual_cache_bytes > PREPARED_PAYLOAD_BYTES);
    assert!(actual_cache_bytes <= prepared_budget().cache_bytes());
    let temporary = prepared_storage_resource(&generated_descriptor, StorageUseKind::Temporary);
    assert_eq!(
        generate_receipt
            .planned_resource_amount(&generated_node, &temporary, &ClaimLifetime::Work,),
        Some(generated_reservation.temporary_staging_bytes())
    );
    assert_eq!(
        generate_receipt.actual_resource_peak(&generated_node, &temporary, &ClaimLifetime::Work,),
        Some(actual_cache_bytes)
    );
    let observed_resident = generate_receipt
        .actual_resource_peak(
            &generated_node,
            &LeaseResource::IoBuffer(IoBufferKind::StorageManager),
            &ClaimLifetime::Work,
        )
        .expect("observed private-store residency");
    assert_eq!(
        generate_receipt.planned_resource_amount(
            &generated_node,
            &LeaseResource::IoBuffer(IoBufferKind::StorageManager),
            &ClaimLifetime::Work,
        ),
        Some(generated_reservation.resident_buffer_bytes())
    );
    assert!(
        observed_resident >= generated_reservation.streaming_buffer_bytes(),
        "observed residency covers the actually allocated streaming buffer"
    );
    assert!(
        observed_resident < generated_reservation.resident_buffer_bytes(),
        "the conservative reservation is distinct from measured peak residency"
    );
    let generate_io = generate_receipt
        .stage_actual_io(&generated_node, IoBufferKind::StorageManager)
        .expect("measured generation I/O");
    assert!(generate_io.0 > PREPARED_PAYLOAD_BYTES * 2);
    assert!(generate_io.1 > 3);
    assert_eq!(
        generate_receipt.stage_actual_io(&generated_node, IoBufferKind::MappedPageCache),
        None,
        "private-cache reads and controls must not claim mapped-page-cache authority"
    );
    assert_eq!(
        generate_receipt.stage_actual_io(&generated_node, IoBufferKind::SourceReadAhead),
        None,
        "the shared streaming Vec is not a second physical source buffer"
    );
    assert_eq!(
        generate_receipt.stage_actual_io(&generated_node, IoBufferKind::Writeback),
        None,
        "the shared streaming Vec is not a second physical writeback buffer"
    );
    assert_eq!(
        generate_receipt.stage_actual_io(&generated_node, IoBufferKind::Publication),
        None,
        "private cache mutation must never claim scientific publication authority"
    );
    assert_eq!(
        generate_receipt.stage_actual_io(
            &prepared_release_node_id(&generated_descriptor, PreparedArtifactOperation::Generate,),
            IoBufferKind::StorageManager,
        ),
        Some((0, 0)),
        "the terminal storage-manager release performs no hidden payload I/O"
    );

    let narrower_store = PreparedArtifactStore::open(
        generated_directory.path(),
        prepared_storage_domain(),
        PreparedArtifactBudget::new(32_768, 3, 64).expect("narrower cache policy"),
    )
    .expect("same-root store with narrower policy");
    let narrower_descriptor = prepared_descriptor(&narrower_store, &problem);
    assert_ne!(
        narrower_descriptor.cache_identity(),
        generated_descriptor.cache_identity(),
        "entry-count policy is committed into CacheIdentity"
    );
    assert_eq!(
        narrower_descriptor.identity(),
        generated_descriptor.identity(),
        "cache policy is not immutable prepared-content identity"
    );
    assert_eq!(
        fs::read_dir(generated_directory.path().join("objects-v3"))
            .expect("published cache inventory")
            .count(),
        1,
        "opening a store neither inspects nor mutates prepared entries"
    );
    drop((narrower_descriptor, narrower_store));

    let reuse_store = PreparedArtifactStore::open(
        generated_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("reuse store");
    let reuse_descriptor = prepared_descriptor(&reuse_store, &problem);
    assert_eq!(reuse_descriptor, generated_descriptor);
    let reuse_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &reuse_descriptor,
                &reuse_store,
                PreparedArtifactOperation::Reuse,
            ))
        },
    )
    .expect("reuse plan");
    let reuse_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Reuse,
        reuse_store,
        reuse_descriptor.clone(),
    );
    let (reuse_registry, reuse_id) = prepared_registry(&problem, reuse_adapter);
    let reuse_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([93; 32]);
    run_prepared(
        &problem,
        &reuse_plan,
        &reuse_registry,
        receipts.bind(execution_provenance(
            reuse_attempt,
            BuildIdentity::from_sha256([94; 32]),
        )),
    )
    .expect("reuse execution");
    assert_eq!(
        prepared_adapter_observed(&reuse_registry, &reuse_id),
        PreparedObserved::Materialized {
            identity: reuse_descriptor.identity(),
            integrity: generated_integrity,
        }
    );
    let reuse_receipt = receipts.open(reuse_attempt).expect("reuse receipt");
    assert_eq!(
        reuse_receipt.artifact_disposition(reuse_descriptor.identity()),
        Some(ArtifactDisposition::Reused)
    );
    assert_eq!(
        reuse_receipt.artifact_role(reuse_descriptor.identity()),
        Some(ArtifactRole::Cache)
    );
    let reuse_io = reuse_receipt
        .stage_actual_io(
            &reuse_descriptor.work_node_id(PreparedArtifactOperation::Reuse),
            IoBufferKind::StorageManager,
        )
        .expect("measured reuse I/O");
    assert!(reuse_io.0 > PREPARED_PAYLOAD_BYTES);
    assert!(reuse_io.1 > 3);
    assert_eq!(
        reuse_receipt.stage_actual_io(
            &reuse_descriptor.work_node_id(PreparedArtifactOperation::Reuse),
            IoBufferKind::SourceReadAhead,
        ),
        None
    );
    assert_eq!(
        reuse_receipt.stage_actual_io(
            &reuse_descriptor.work_node_id(PreparedArtifactOperation::Reuse),
            IoBufferKind::Writeback,
        ),
        None
    );

    let loaded_directory = prepared_tempdir();
    let loaded_store = PreparedArtifactStore::open(
        loaded_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("loaded store");
    let loaded_descriptor = prepared_descriptor(&loaded_store, &problem);
    assert_ne!(
        loaded_descriptor.cache_identity(),
        generated_descriptor.cache_identity(),
        "the canonical private root is committed into CacheIdentity"
    );
    assert_eq!(
        loaded_descriptor.identity(),
        generated_descriptor.identity(),
        "relocating identical prepared content does not change ArtifactIdentity"
    );
    let load_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &loaded_descriptor,
                &loaded_store,
                PreparedArtifactOperation::Load,
            ))
        },
    )
    .expect("load plan");
    let load_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Load,
        loaded_store,
        loaded_descriptor.clone(),
    );
    let load_source_identity = PreparedSourceFiles::new()
        .load_source(&loaded_descriptor)
        .expect("canonical load source identity")
        .identity();
    let (load_registry, load_id) = prepared_registry(&problem, load_adapter);
    let load_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([95; 32]);
    run_prepared(
        &problem,
        &load_plan,
        &load_registry,
        receipts.bind(execution_provenance(
            load_attempt,
            BuildIdentity::from_sha256([96; 32]),
        )),
    )
    .expect("load execution");
    assert!(matches!(
        prepared_adapter_observed(&load_registry, &load_id),
        PreparedObserved::Materialized { .. }
    ));
    let load_receipt = receipts.open(load_attempt).expect("load receipt");
    assert_eq!(
        load_receipt.artifact_disposition(loaded_descriptor.identity()),
        Some(ArtifactDisposition::Loaded)
    );
    assert_eq!(
        load_receipt.artifact_role(loaded_descriptor.identity()),
        Some(ArtifactRole::Prepared)
    );
    assert_eq!(
        load_receipt.artifact_disposition(load_source_identity),
        Some(ArtifactDisposition::Loaded),
        "the predecessor import accounts for the exact source identity"
    );
    assert_eq!(
        load_receipt.artifact_node(load_source_identity),
        Some(WorkNodeId::new("execute"))
    );
    assert_eq!(
        load_receipt.artifact_actual_bytes(load_source_identity),
        Some(PREPARED_PAYLOAD_BYTES)
    );
    let load_io = load_receipt
        .stage_actual_io(
            &loaded_descriptor.work_node_id(PreparedArtifactOperation::Load),
            IoBufferKind::StorageManager,
        )
        .expect("measured load I/O");
    assert!(load_io.0 > PREPARED_PAYLOAD_BYTES);
    assert!(load_io.1 > 3);
    assert_eq!(
        load_receipt.stage_actual_io(
            &loaded_descriptor.work_node_id(PreparedArtifactOperation::Load),
            IoBufferKind::SourceReadAhead,
        ),
        None
    );
    assert_eq!(
        load_receipt.stage_actual_io(
            &loaded_descriptor.work_node_id(PreparedArtifactOperation::Load),
            IoBufferKind::Writeback,
        ),
        None
    );
    assert_ne!(
        generated_descriptor.work_node_id(PreparedArtifactOperation::Generate),
        loaded_descriptor.work_node_id(PreparedArtifactOperation::Load)
    );
}

#[test]
fn prepared_operation_identity_cannot_authorize_a_different_operation() {
    let problem = compile(request(1)).expect("operation mismatch problem");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(8, 1_000_000).expect("operation mismatch retention"),
    )
    .expect("operation mismatch receipt store");
    let mismatch_directory = prepared_tempdir();
    let mismatch_store = PreparedArtifactStore::open(
        mismatch_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("operation mismatch store");
    let mismatch_descriptor = prepared_descriptor(&mismatch_store, &problem);
    let mismatch_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &mismatch_descriptor,
                &mismatch_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("operation mismatch plan");
    let mismatch_adapter = PreparedOperationAdapter::with_id(
        mismatch_descriptor.work_implementation_id(PreparedArtifactOperation::Generate),
        PreparedArtifactOperation::Load,
        mismatch_store,
        mismatch_descriptor.clone(),
    );
    let (mismatch_registry, _) = prepared_registry(&problem, mismatch_adapter);
    let mismatch_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([103; 32]);
    let mismatch_error = run_prepared(
        &problem,
        &mismatch_plan,
        &mismatch_registry,
        receipts.bind(execution_provenance(
            mismatch_attempt,
            BuildIdentity::from_sha256([104; 32]),
        )),
    )
    .expect_err("a generation plan cannot authorize cold load");
    match mismatch_error {
        RunError::Execution { node, source } => {
            assert_eq!(
                node,
                mismatch_descriptor.work_node_id(PreparedArtifactOperation::Generate)
            );
            assert_eq!(
                source.to_string(),
                PreparedArtifactError::UnplannedSource.to_string()
            );
        }
        other => panic!("unexpected operation-mismatch failure: {other}"),
    }
    let mismatch_receipt = receipts
        .open(mismatch_attempt)
        .expect("operation mismatch receipt");
    assert_eq!(mismatch_receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        mismatch_receipt.failure_kind(),
        Some(ReceiptFailureKind::Adapter)
    );
    assert_eq!(
        fs::read_dir(mismatch_directory.path().join("objects-v3"))
            .expect("operation mismatch inventory")
            .count(),
        0,
        "the rejected operation cannot publish"
    );
}

#[test]
fn prepared_owner_and_cell_identities_fail_closed() {
    let problem = compile(request(1)).expect("owner identity problem");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(8, 1_000_000).expect("owner identity retention"),
    )
    .expect("owner identity receipt store");
    let owner_mismatch_directory = prepared_tempdir();
    let owner_mismatch_store = PreparedArtifactStore::open(
        owner_mismatch_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("owner mismatch store");
    let spoofed_registration = PreparedArtifactRegistration::new(
        "untrusted-catalog",
        "untrusted-provider",
        "9.9.9",
        WorkImplementationId::new("untrusted-awproject-cpu"),
    )
    .expect("spoofed registration");
    let spoofed_owner_descriptor = prepared_descriptor_with_registry_registration_and_cell(
        &owner_mismatch_store,
        &problem,
        registry(4),
        spoofed_registration,
        prepared_cell(1.0e9),
    );
    let cell_one_descriptor = prepared_descriptor_with_registration_and_cell(
        &owner_mismatch_store,
        &problem,
        prepared_registration(),
        prepared_cell(1.0e9),
    );
    let cell_two_descriptor = prepared_descriptor_with_registration_and_cell(
        &owner_mismatch_store,
        &problem,
        prepared_registration(),
        PreparedArtifactScientificIdentity::convolution_function(
            PreparedArtifactCellSemantics::new(
                1.000001e9,
                4.0,
                2,
                2,
                0.25,
                1.100001e9,
                3,
                "vla",
                "l-band",
                25.0,
                1.0,
                PreparedArtifactAwInterpretation::Wavelength,
                false,
                "flatnoise",
            )
            .expect("second prepared cell semantics"),
        )
        .expect("second prepared cell"),
    );
    assert_ne!(
        cell_one_descriptor.identity(),
        cell_two_descriptor.identity(),
        "distinct AW scientific cells cannot share an artifact identity"
    );
    let delimiter_left = prepared_descriptor_with_registration(
        &owner_mismatch_store,
        &problem,
        PreparedArtifactRegistration::new(
            "catalog",
            "provider-a",
            "version-b",
            WorkImplementationId::new("implementation-c"),
        )
        .expect("left delimiter registration"),
    );
    let delimiter_right = prepared_descriptor_with_registration(
        &owner_mismatch_store,
        &problem,
        PreparedArtifactRegistration::new(
            "catalog",
            "provider",
            "a-version-b",
            WorkImplementationId::new("implementation-c"),
        )
        .expect("right delimiter registration"),
    );
    assert_ne!(
        delimiter_left.work_implementation_id(PreparedArtifactOperation::Generate),
        delimiter_right.work_implementation_id(PreparedArtifactOperation::Generate),
        "owner fields use length-prefixed implementation identity hashing"
    );
    let empty_catalog = PreparedSuiteRegistry {
        id: registry(3),
        metadata: None,
        implementations: BTreeMap::new(),
        prepared: BTreeMap::new(),
    };
    assert!(matches!(
        PreparedArtifactDescriptor::convolution_function(
            &owner_mismatch_store,
            &empty_catalog,
            prepared_registration().implementation(),
            &problem,
            prepared_cell(1.0e9),
            PreparedArtifactPlaneDescriptor::new(
                [3, 3],
                [1, 1],
                1,
                prepared_uv(3),
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::Axis0ContiguousLittleEndian,
            )
            .expect("unlisted imaging plane"),
            PreparedArtifactPlaneDescriptor::new(
                [5, 5],
                [2, 2],
                1,
                prepared_uv(5),
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::LastAxisContiguousLittleEndian,
            )
            .expect("unlisted weight plane"),
        ),
        Err(PreparedArtifactError::InvalidOwner)
    ));
    let owner_mismatch_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &spoofed_owner_descriptor,
                &owner_mismatch_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("owner mismatch plan");
    let owner_mismatch_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Generate,
        owner_mismatch_store,
        spoofed_owner_descriptor,
    );
    let (owner_mismatch_registry, _) = prepared_registry(&problem, owner_mismatch_adapter);
    let owner_mismatch_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([105; 32]);
    let owner_mismatch_error = run_prepared(
        &problem,
        &owner_mismatch_plan,
        &owner_mismatch_registry,
        receipts.bind(execution_provenance(
            owner_mismatch_attempt,
            BuildIdentity::from_sha256([106; 32]),
        )),
    )
    .expect_err("registry-selected owner must bind during plan execution");
    assert!(matches!(
        owner_mismatch_error,
        RunError::Execution { source, .. }
            if source.to_string() == PreparedArtifactError::ImplementationRegistryMismatch.to_string()
    ));
    let owner_mismatch_receipt = receipts
        .open(owner_mismatch_attempt)
        .expect("owner mismatch receipt");
    assert_eq!(owner_mismatch_receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        owner_mismatch_receipt.failure_kind(),
        Some(ReceiptFailureKind::Adapter)
    );
    assert_eq!(
        fs::read_dir(owner_mismatch_directory.path().join("objects-v3"))
            .expect("owner mismatch inventory")
            .count(),
        0
    );
}

#[test]
fn prepared_content_identity_is_independent_of_cache_location_and_policy_for_every_kind() {
    let problem = compile(request(1)).expect("relocatable prepared problem");
    let first_root = prepared_tempdir();
    let second_root = prepared_tempdir();
    let first = PreparedArtifactStore::open(
        first_root.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("first prepared store");
    let second = PreparedArtifactStore::open(
        second_root.path(),
        prepared_storage_domain(),
        PreparedArtifactBudget::new(65_536, 8, 128).expect("different cache policy"),
    )
    .expect("second prepared store");

    let first_cf = prepared_descriptor(&first, &problem);
    let second_cf = prepared_descriptor(&second, &problem);
    assert_eq!(first_cf.identity(), second_cf.identity());
    assert_ne!(first_cf.cache_identity(), second_cf.cache_identity());

    let registration = prepared_registration();
    let (catalog, implementation) = prepared_catalog(registration);
    let segment = PreparedArtifactSegmentDescriptor::new(
        "routing",
        vec![4],
        vec![0],
        vec![1],
        None,
        PreparedArtifactPrecision::U8,
        PreparedArtifactOrder::Axis0ContiguousLittleEndian,
    )
    .expect("relocatable segment");
    let spectral = |store: &PreparedArtifactStore| {
        PreparedArtifactDescriptor::from_owner_identity(
            store,
            &catalog,
            &implementation,
            &problem,
            PreparedArtifactScientificIdentity::spectral_map(
                PreparedArtifactSpectralMapSemantics::new(1.0e9, 4, 1.0e6, "lsrk")
                    .expect("relocatable spectral semantics"),
            )
            .expect("relocatable spectral identity"),
            vec![segment.clone()],
        )
        .expect("relocatable spectral descriptor")
    };
    let first_spectral = spectral(&first);
    let second_spectral = spectral(&second);
    assert_eq!(first_spectral.identity(), second_spectral.identity());
    assert_ne!(
        first_spectral.cache_identity(),
        second_spectral.cache_identity()
    );

    let kernel = |store: &PreparedArtifactStore| {
        PreparedArtifactDescriptor::from_owner_identity(
            store,
            &catalog,
            &implementation,
            &problem,
            PreparedArtifactScientificIdentity::kernel(
                PreparedArtifactKernelSemantics::new(
                    PreparedArtifactKernelAlgorithm::Gridding,
                    vec![4],
                    vec![4],
                )
                .expect("relocatable kernel semantics"),
            )
            .expect("relocatable kernel identity"),
            vec![segment.clone()],
        )
        .expect("relocatable kernel descriptor")
    };
    let first_kernel = kernel(&first);
    let second_kernel = kernel(&second);
    assert_eq!(first_kernel.identity(), second_kernel.identity());
    assert_ne!(
        first_kernel.cache_identity(),
        second_kernel.cache_identity()
    );
}

#[test]
fn public_prepared_scientific_keys_are_exact_typed_and_bounded() {
    let problem = compile(request(1)).expect("prepared scientific-key problem");
    let directory = prepared_tempdir();
    let store = PreparedArtifactStore::open(
        directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("prepared scientific-key store");
    let registration = prepared_registration();
    let (catalog, owner_implementation) = prepared_catalog(registration.clone());
    let cell = |w_coordinate,
                mueller_element,
                polarization,
                conjugate_frequency_hz,
                conjugate_polarization,
                telescope: &str,
                band: &str,
                diameter_m,
                w_increment,
                interpretation,
                rotationally_symmetric| {
        PreparedArtifactScientificIdentity::convolution_function(
            PreparedArtifactCellSemantics::new(
                1.0e9,
                w_coordinate,
                mueller_element,
                polarization,
                0.125,
                conjugate_frequency_hz,
                conjugate_polarization,
                telescope,
                band,
                diameter_m,
                w_increment,
                interpretation,
                rotationally_symmetric,
                "flatnoise",
            )
            .expect("exact AW semantics"),
        )
        .expect("exact AW cell")
    };
    let aw_cells = [
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            f64::from_bits(3.0_f64.to_bits() + 1),
            1,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            2,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            2,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            f64::from_bits(1.1e9_f64.to_bits() + 1),
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            2,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla-b",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla",
            "s-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            27.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            2.0,
            PreparedArtifactAwInterpretation::Wavelength,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::BaselineMeters,
            true,
        ),
        cell(
            3.0,
            1,
            1,
            1.1e9,
            1,
            "vla",
            "l-band",
            25.0,
            1.0,
            PreparedArtifactAwInterpretation::Wavelength,
            false,
        ),
    ];
    let aw_identities = aw_cells
        .into_iter()
        .map(|cell| {
            prepared_descriptor_with_registration_and_cell(
                &store,
                &problem,
                registration.clone(),
                cell,
            )
            .identity()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        aw_identities.len(),
        12,
        "bit-exact AW and paired-kernel semantics must not collide"
    );

    let segment = PreparedArtifactSegmentDescriptor::new(
        "routing",
        vec![4],
        vec![0],
        vec![1],
        None,
        PreparedArtifactPrecision::U8,
        PreparedArtifactOrder::Axis0ContiguousLittleEndian,
    )
    .expect("generic prepared segment");
    let spectral = |channel_index| {
        PreparedArtifactDescriptor::from_owner_identity(
            &store,
            &catalog,
            &owner_implementation,
            &problem,
            PreparedArtifactScientificIdentity::spectral_map(
                PreparedArtifactSpectralMapSemantics::new(1.0e9, channel_index, 1.0e6, "lsrk")
                    .expect("spectral-map semantics"),
            )
            .expect("spectral-map identity"),
            vec![segment.clone()],
        )
        .expect("spectral-map descriptor")
    };
    assert_ne!(
        spectral(3).identity(),
        spectral(4).identity(),
        "distinct owner-minted spectral semantics require identity separation"
    );

    let kernel = |algorithm| {
        PreparedArtifactDescriptor::from_owner_identity(
            &store,
            &catalog,
            &owner_implementation,
            &problem,
            PreparedArtifactScientificIdentity::kernel(
                PreparedArtifactKernelSemantics::new(algorithm, vec![4], vec![4])
                    .expect("kernel semantics"),
            )
            .expect("kernel identity"),
            vec![segment.clone()],
        )
        .expect("kernel descriptor")
    };
    assert_ne!(
        kernel(PreparedArtifactKernelAlgorithm::Gridding).identity(),
        kernel(PreparedArtifactKernelAlgorithm::Degridding).identity(),
        "distinct owner-minted kernel semantics require identity separation"
    );

    let too_many_segments = (0..65)
        .map(|index| {
            PreparedArtifactSegmentDescriptor::new(
                format!("segment-{index}"),
                vec![1],
                vec![0],
                vec![1],
                None,
                PreparedArtifactPrecision::U8,
                PreparedArtifactOrder::Axis0ContiguousLittleEndian,
            )
            .expect("bounded manifest segment")
        })
        .collect();
    assert!(matches!(
        PreparedArtifactDescriptor::from_owner_identity(
            &store,
            &catalog,
            &owner_implementation,
            &problem,
            PreparedArtifactScientificIdentity::kernel(
                PreparedArtifactKernelSemantics::new(
                    PreparedArtifactKernelAlgorithm::Gridding,
                    vec![1],
                    vec![1],
                )
                .expect("bounded kernel semantics"),
            )
            .expect("bounded kernel identity"),
            too_many_segments,
        ),
        Err(PreparedArtifactError::InvalidDescriptor)
    ));
}

#[test]
fn convolution_function_pair_requires_shared_sampling() {
    let problem = compile(request(1)).expect("paired-sampling problem");
    let directory = prepared_tempdir();
    let store = PreparedArtifactStore::open(
        directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("paired-sampling store");
    let (catalog, implementation) = prepared_catalog(prepared_registration());
    let imaging = PreparedArtifactPlaneDescriptor::new(
        [3, 3],
        [1, 1],
        1,
        prepared_uv(3),
        PreparedArtifactPrecision::ComplexF32,
        PreparedArtifactOrder::Axis0ContiguousLittleEndian,
    )
    .expect("imaging plane");
    let weight = PreparedArtifactPlaneDescriptor::new(
        [9, 9],
        [2, 2],
        2,
        prepared_uv(9),
        PreparedArtifactPrecision::ComplexF32,
        PreparedArtifactOrder::LastAxisContiguousLittleEndian,
    )
    .expect("weight plane");

    assert!(matches!(
        PreparedArtifactDescriptor::convolution_function(
            &store,
            &catalog,
            &implementation,
            &problem,
            prepared_cell(1.0e9),
            imaging,
            weight,
        ),
        Err(PreparedArtifactError::InvalidLayout)
    ));
}

#[test]
fn public_prepared_reuse_receipts_fail_closed_rejections() {
    let problem = compile(request(1)).expect("prepared rejection problem");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(16, 2_000_000).expect("rejection receipt retention"),
    )
    .expect("rejection receipt store");

    let missing_directory = prepared_tempdir();
    let missing_store = PreparedArtifactStore::open(
        missing_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("missing store");
    let missing_descriptor = prepared_descriptor(&missing_store, &problem);
    let missing_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &missing_descriptor,
                &missing_store,
                PreparedArtifactOperation::Reuse,
            ))
        },
    )
    .expect("missing reuse plan");
    let missing_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Reuse,
        missing_store,
        missing_descriptor.clone(),
    );
    let (missing_registry, missing_id) = prepared_registry(&problem, missing_adapter);
    let missing_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([97; 32]);
    let missing_result = run_prepared(
        &problem,
        &missing_plan,
        &missing_registry,
        receipts.bind(execution_provenance(
            missing_attempt,
            BuildIdentity::from_sha256([98; 32]),
        )),
    );
    assert!(matches!(
        missing_result,
        Err(RunError::Evidence(
            ExecutionEvidenceError::RejectedArtifact { .. }
        ))
    ));
    assert_eq!(
        prepared_adapter_observed(&missing_registry, &missing_id),
        PreparedObserved::Rejected(PreparedArtifactRejection::Missing)
    );
    let missing_receipt = receipts.open(missing_attempt).expect("missing receipt");
    assert_eq!(
        missing_receipt.artifact_disposition(missing_descriptor.identity()),
        Some(ArtifactDisposition::RejectedStale)
    );
    assert_eq!(
        missing_receipt.artifact_observed_identity(missing_descriptor.identity()),
        Some(
            forged_rejection_identity(
                missing_descriptor.identity(),
                PreparedArtifactRejection::Missing,
            )
            .as_bytes(),
        )
    );
    assert_eq!(
        PreparedArtifactRejection::from_evidence_identity(
            missing_descriptor.identity(),
            ArtifactIdentity::from_sha256(
                missing_receipt
                    .artifact_observed_identity(missing_descriptor.identity())
                    .expect("missing rejection evidence"),
            ),
        ),
        Some(PreparedArtifactRejection::Missing)
    );
    assert_eq!(
        missing_receipt.artifact_actual_bytes(missing_descriptor.identity()),
        Some(0)
    );
    assert_rejection_evidence(
        &missing_receipt,
        &missing_descriptor,
        PreparedArtifactOperation::Reuse,
        PreparedArtifactRejection::Missing,
    );

    let corrupt_directory = prepared_tempdir();
    let generate_store = PreparedArtifactStore::open(
        corrupt_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("corrupt seed store");
    let generate_descriptor = prepared_descriptor(&generate_store, &problem);
    let generate_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &generate_descriptor,
                &generate_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("corrupt seed plan");
    let generate_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Generate,
        generate_store,
        generate_descriptor.clone(),
    );
    let (generate_registry, _) = prepared_registry(&problem, generate_adapter);
    let seed_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([99; 32]);
    run_prepared(
        &problem,
        &generate_plan,
        &generate_registry,
        receipts.bind(execution_provenance(
            seed_attempt,
            BuildIdentity::from_sha256([100; 32]),
        )),
    )
    .expect("corrupt seed execution");
    let object = fs::read_dir(corrupt_directory.path().join("objects-v3"))
        .expect("cache objects")
        .next()
        .expect("published object")
        .expect("object entry")
        .path();
    let payload = object.join("payload.bin");
    let mut bytes = fs::read(&payload).expect("published payload");
    bytes[0] ^= 1;
    fs::write(&payload, bytes).expect("corrupt payload");

    let corrupt_store = PreparedArtifactStore::open(
        corrupt_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("corrupt reuse store");
    let corrupt_descriptor = prepared_descriptor(&corrupt_store, &problem);
    let corrupt_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &corrupt_descriptor,
                &corrupt_store,
                PreparedArtifactOperation::Reuse,
            ))
        },
    )
    .expect("corrupt reuse plan");
    let corrupt_adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Reuse,
        corrupt_store,
        corrupt_descriptor.clone(),
    );
    let (corrupt_registry, corrupt_id) = prepared_registry(&problem, corrupt_adapter);
    let corrupt_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([101; 32]);
    let corrupt_result = run_prepared(
        &problem,
        &corrupt_plan,
        &corrupt_registry,
        receipts.bind(execution_provenance(
            corrupt_attempt,
            BuildIdentity::from_sha256([102; 32]),
        )),
    );
    assert!(matches!(
        corrupt_result,
        Err(RunError::Evidence(
            ExecutionEvidenceError::RejectedArtifact { .. }
        ))
    ));
    assert_eq!(
        prepared_adapter_observed(&corrupt_registry, &corrupt_id),
        PreparedObserved::Rejected(PreparedArtifactRejection::Corrupt)
    );
    let corrupt_receipt = receipts.open(corrupt_attempt).expect("corrupt receipt");
    assert_eq!(
        corrupt_receipt.artifact_disposition(corrupt_descriptor.identity()),
        Some(ArtifactDisposition::RejectedStale)
    );
    assert!(
        corrupt_receipt
            .artifact_path_identity(corrupt_descriptor.identity())
            .is_some()
    );
    let corrupt_evidence = ArtifactIdentity::from_sha256(
        corrupt_receipt
            .artifact_observed_identity(corrupt_descriptor.identity())
            .expect("corrupt rejection evidence"),
    );
    assert_eq!(
        PreparedArtifactRejection::from_evidence_identity(
            corrupt_descriptor.identity(),
            corrupt_evidence,
        ),
        Some(PreparedArtifactRejection::Corrupt)
    );
    assert!(
        corrupt_receipt
            .artifact_actual_bytes(corrupt_descriptor.identity())
            .expect("corrupt inspected bytes")
            > 0
    );
    assert!(
        corrupt_receipt
            .stage_actual_io(
                &corrupt_descriptor.work_node_id(PreparedArtifactOperation::Reuse),
                IoBufferKind::StorageManager,
            )
            .expect("corrupt I/O evidence")
            .0
            > 0
    );
    assert_rejection_evidence(
        &corrupt_receipt,
        &corrupt_descriptor,
        PreparedArtifactOperation::Reuse,
        PreparedArtifactRejection::Corrupt,
    );

    let incomplete_directory = prepared_tempdir();
    let incomplete_store = PreparedArtifactStore::open(
        incomplete_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("incomplete seed store");
    let incomplete_descriptor = prepared_descriptor(&incomplete_store, &problem);
    assert!(matches!(
        execute_prepared_operation(
            &problem,
            &receipts,
            incomplete_store,
            incomplete_descriptor.clone(),
            PreparedArtifactOperation::Generate,
            PreparedRunExpectation {
                attempt_byte: 103,
                build_byte: 104,
                expect_rejection: false,
            },
        ),
        PreparedObserved::Materialized { .. }
    ));
    let incomplete_object = incomplete_directory
        .path()
        .join("objects-v3")
        .join(incomplete_descriptor.identity().to_string());
    fs::remove_file(incomplete_object.join("payload.bin")).expect("remove prepared payload");
    let incomplete_store = PreparedArtifactStore::open(
        incomplete_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("incomplete reuse store");
    assert_eq!(
        execute_prepared_operation(
            &problem,
            &receipts,
            incomplete_store,
            incomplete_descriptor.clone(),
            PreparedArtifactOperation::Reuse,
            PreparedRunExpectation {
                attempt_byte: 105,
                build_byte: 106,
                expect_rejection: true,
            },
        ),
        PreparedObserved::Rejected(PreparedArtifactRejection::Incomplete)
    );
    let incomplete_receipt = receipts
        .open(casa_imaging_runtime::ExecutionAttemptId::from_sha256(
            [105; 32],
        ))
        .expect("incomplete receipt");
    assert_rejection_evidence(
        &incomplete_receipt,
        &incomplete_descriptor,
        PreparedArtifactOperation::Reuse,
        PreparedArtifactRejection::Incomplete,
    );

    let incompatible_directory = prepared_tempdir();
    let incompatible_store = PreparedArtifactStore::open(
        incompatible_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("incompatible seed store");
    let incompatible_descriptor = prepared_descriptor(&incompatible_store, &problem);
    assert!(matches!(
        execute_prepared_operation(
            &problem,
            &receipts,
            incompatible_store,
            incompatible_descriptor.clone(),
            PreparedArtifactOperation::Generate,
            PreparedRunExpectation {
                attempt_byte: 107,
                build_byte: 108,
                expect_rejection: false,
            },
        ),
        PreparedObserved::Materialized { .. }
    ));
    let incompatible_manifest = incompatible_directory
        .path()
        .join("objects-v3")
        .join(incompatible_descriptor.identity().to_string())
        .join("manifest.json");
    let mut manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&incompatible_manifest).expect("incompatible manifest bytes"),
    )
    .expect("incompatible manifest JSON");
    manifest["descriptor"]["provider_version"] = "9.9.9".into();
    fs::write(
        &incompatible_manifest,
        serde_json::to_vec_pretty(&manifest).expect("mismatched manifest JSON"),
    )
    .expect("write mismatched manifest");
    let incompatible_store = PreparedArtifactStore::open(
        incompatible_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("incompatible reuse store");
    assert_eq!(
        execute_prepared_operation(
            &problem,
            &receipts,
            incompatible_store,
            incompatible_descriptor.clone(),
            PreparedArtifactOperation::Reuse,
            PreparedRunExpectation {
                attempt_byte: 109,
                build_byte: 110,
                expect_rejection: true,
            },
        ),
        PreparedObserved::Rejected(PreparedArtifactRejection::Incompatible)
    );
    let incompatible_receipt = receipts
        .open(casa_imaging_runtime::ExecutionAttemptId::from_sha256(
            [109; 32],
        ))
        .expect("incompatible receipt");
    assert_rejection_evidence(
        &incompatible_receipt,
        &incompatible_descriptor,
        PreparedArtifactOperation::Reuse,
        PreparedArtifactRejection::Incompatible,
    );

    let oversized_manifest_directory = prepared_tempdir();
    let oversized_manifest_store = PreparedArtifactStore::open(
        oversized_manifest_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("oversized manifest seed store");
    let oversized_manifest_descriptor = prepared_descriptor(&oversized_manifest_store, &problem);
    assert!(matches!(
        execute_prepared_operation(
            &problem,
            &receipts,
            oversized_manifest_store,
            oversized_manifest_descriptor.clone(),
            PreparedArtifactOperation::Generate,
            PreparedRunExpectation {
                attempt_byte: 115,
                build_byte: 116,
                expect_rejection: false,
            },
        ),
        PreparedObserved::Materialized { .. }
    ));
    let oversized_manifest_path = oversized_manifest_directory
        .path()
        .join("objects-v3")
        .join(oversized_manifest_descriptor.identity().to_string())
        .join("manifest.json");
    fs::write(&oversized_manifest_path, vec![b'{'; 16 * 1024 + 1])
        .expect("write oversized manifest");
    let oversized_manifest_store = PreparedArtifactStore::open(
        oversized_manifest_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("oversized manifest reuse store");
    assert_eq!(
        execute_prepared_operation(
            &problem,
            &receipts,
            oversized_manifest_store,
            oversized_manifest_descriptor.clone(),
            PreparedArtifactOperation::Reuse,
            PreparedRunExpectation {
                attempt_byte: 117,
                build_byte: 118,
                expect_rejection: true,
            },
        ),
        PreparedObserved::Rejected(PreparedArtifactRejection::Incompatible)
    );
    let oversized_manifest_receipt = receipts
        .open(casa_imaging_runtime::ExecutionAttemptId::from_sha256(
            [117; 32],
        ))
        .expect("oversized manifest receipt");
    assert_rejection_evidence(
        &oversized_manifest_receipt,
        &oversized_manifest_descriptor,
        PreparedArtifactOperation::Reuse,
        PreparedArtifactRejection::Incompatible,
    );

    let nonfinite_directory = prepared_tempdir();
    let nonfinite_store = PreparedArtifactStore::open(
        nonfinite_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("nonfinite seed store");
    let nonfinite_descriptor = prepared_descriptor(&nonfinite_store, &problem);
    assert!(matches!(
        execute_prepared_operation(
            &problem,
            &receipts,
            nonfinite_store,
            nonfinite_descriptor.clone(),
            PreparedArtifactOperation::Generate,
            PreparedRunExpectation {
                attempt_byte: 111,
                build_byte: 112,
                expect_rejection: false,
            },
        ),
        PreparedObserved::Materialized { .. }
    ));
    let nonfinite_object = nonfinite_directory
        .path()
        .join("objects-v3")
        .join(nonfinite_descriptor.identity().to_string());
    let nonfinite_payload = nonfinite_object.join("payload.bin");
    let mut payload = fs::read(&nonfinite_payload).expect("nonfinite payload bytes");
    payload[..4].copy_from_slice(&f32::NAN.to_le_bytes());
    fs::write(&nonfinite_payload, &payload).expect("write nonfinite payload");
    let nonfinite_manifest = nonfinite_object.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&nonfinite_manifest).expect("nonfinite manifest bytes"))
            .expect("nonfinite manifest JSON");
    let imaging_bytes = usize::try_from(
        manifest["segments"][0]["bytes"]
            .as_u64()
            .expect("imaging byte count"),
    )
    .expect("imaging byte count fits usize");
    manifest["segments"][0]["sha256"] =
        format!("{:x}", Sha256::digest(&payload[..imaging_bytes])).into();
    manifest["payload_sha256"] = format!("{:x}", Sha256::digest(&payload)).into();
    fs::write(
        &nonfinite_manifest,
        serde_json::to_vec_pretty(&manifest).expect("nonfinite manifest JSON"),
    )
    .expect("write nonfinite manifest");
    let nonfinite_store = PreparedArtifactStore::open(
        nonfinite_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("nonfinite reuse store");
    assert_eq!(
        execute_prepared_operation(
            &problem,
            &receipts,
            nonfinite_store,
            nonfinite_descriptor.clone(),
            PreparedArtifactOperation::Reuse,
            PreparedRunExpectation {
                attempt_byte: 113,
                build_byte: 114,
                expect_rejection: true,
            },
        ),
        PreparedObserved::Rejected(PreparedArtifactRejection::NonFinite)
    );
    let nonfinite_receipt = receipts
        .open(casa_imaging_runtime::ExecutionAttemptId::from_sha256(
            [113; 32],
        ))
        .expect("nonfinite receipt");
    assert_rejection_evidence(
        &nonfinite_receipt,
        &nonfinite_descriptor,
        PreparedArtifactOperation::Reuse,
        PreparedArtifactRejection::NonFinite,
    );
}

#[test]
fn failed_prepared_receipt_retains_materialization_eviction_and_io_evidence() {
    let problem = compile(request(1)).expect("prepared failure problem");
    let cache_directory = prepared_tempdir();
    let store = PreparedArtifactStore::open(
        cache_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("prepared failure store");
    let descriptor = prepared_descriptor(&store, &problem);
    let operation = PreparedArtifactOperation::Generate;
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &descriptor,
                &store,
                operation,
            ))
        },
    )
    .expect("prepared failure plan");
    let node_id = descriptor.work_node_id(operation);
    let node = &plan.execution_dag().nodes()[&node_id];
    let resources = node
        .claims
        .iter()
        .map(|claim| ResourceMeasurement::new(claim.resource.clone(), claim.lifetime.clone(), 0))
        .collect();
    let io = plan.prediction().stages()[&node_id]
        .io()
        .iter()
        .map(|prediction| IoMeasurement::new(prediction.kind(), 338, 9))
        .collect();
    let ledger = descriptor.eviction_artifact(operation);
    let observed_evictions = ArtifactIdentity::from_sha256([211; 32]);
    let observed_materialized = ArtifactIdentity::from_sha256([210; 32]);
    let evidence = WorkMeasurements::new(
        resources,
        io,
        vec![
            artifact_measurement(
                descriptor.identity(),
                Some(observed_materialized),
                ArtifactDisposition::Built,
                PREPARED_PAYLOAD_BYTES,
                Some(RedactedPath::from_path(cache_directory.path())),
            ),
            artifact_measurement(
                ledger.identity(),
                Some(observed_evictions),
                ArtifactDisposition::Loaded,
                128,
                None,
            ),
        ],
    );
    let prepared_id = descriptor.work_implementation_id(operation);
    let registry = PreparedSuiteRegistry {
        id: registry(3),
        metadata: Some(super::implementation_metadata(&problem)),
        implementations: BTreeMap::from([
            (
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(prepared_base_executor(
                    &descriptor,
                    operation,
                    None,
                ))),
            ),
            (
                prepared_id.clone(),
                PreparedSuiteImplementation::Failure(Box::new(PreparedFailureAdapter {
                    id: prepared_id,
                    evidence,
                    succeed: false,
                })),
            ),
        ]),
        prepared: BTreeMap::from([(
            prepared_registration().implementation().clone(),
            prepared_registration(),
        )]),
    };
    let receipts = plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([151; 32]);

    let error = run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([152; 32]),
        )),
    )
    .expect_err("the prepared cache node fails after completed mutation");
    assert!(matches!(error, RunError::Execution { node, .. } if node == node_id));

    let receipt = receipts.open(attempt).expect("failed prepared receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.failure_kind(), Some(ReceiptFailureKind::Adapter));
    assert_eq!(receipt.failure_node(), Some(node_id.clone()));
    assert_eq!(
        receipt.stage_actual_io(&node_id, IoBufferKind::StorageManager),
        Some((338, 9))
    );
    assert_eq!(receipt.artifact_actual_bytes(ledger.identity()), Some(128));
    assert_eq!(
        receipt.artifact_observed_identity(ledger.identity()),
        Some(observed_evictions.as_bytes())
    );
    assert_eq!(
        receipt.artifact_actual_bytes(descriptor.identity()),
        Some(PREPARED_PAYLOAD_BYTES)
    );
    assert_eq!(
        receipt.artifact_observed_identity(descriptor.identity()),
        Some(observed_materialized.as_bytes())
    );
    assert!(
        receipt
            .artifact_path_identity(descriptor.identity())
            .is_some()
    );
}

#[test]
fn orphan_staging_is_included_in_reuse_budget_and_receipt_evidence() {
    let problem = compile(request(1)).expect("orphan-staging problem");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(4, 1_000_000).expect("orphan-staging retention"),
    )
    .expect("orphan-staging receipt store");

    let cache_directory = prepared_tempdir();
    let store = PreparedArtifactStore::open(
        cache_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("bounded orphan-staging store");
    let staging = cache_directory.path().join("objects-v3/.staging-orphan");
    fs::create_dir(&staging).expect("bounded orphan-staging directory");
    fs::write(staging.join("manifest.json"), [0_u8; 91]).expect("bounded orphan manifest");
    fs::write(staging.join("payload.bin"), [0_u8; 173]).expect("bounded orphan payload");
    let orphan_bytes = 264;
    let descriptor = prepared_descriptor(&store, &problem);
    let operation = PreparedArtifactOperation::Reuse;
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([160; 32]);
    assert_eq!(
        execute_prepared_operation(
            &problem,
            &receipts,
            store,
            descriptor.clone(),
            operation,
            PreparedRunExpectation {
                attempt_byte: 160,
                build_byte: 161,
                expect_rejection: true,
            },
        ),
        PreparedObserved::Rejected(PreparedArtifactRejection::Missing)
    );
    let receipt = receipts
        .open(attempt)
        .expect("bounded orphan-staging receipt");
    assert_rejection_evidence(
        &receipt,
        &descriptor,
        operation,
        PreparedArtifactRejection::Missing,
    );
    assert_eq!(
        receipt.actual_resource_peak(
            &descriptor.work_node_id(operation),
            &prepared_storage_resource(&descriptor, StorageUseKind::PersistentCache),
            &ClaimLifetime::Work,
        ),
        Some(orphan_bytes),
        "orphan staging bytes must remain visible in rejection evidence"
    );

    let over_budget_directory = prepared_tempdir();
    let over_budget =
        PreparedArtifactBudget::new(20_000, 4, 64).expect("over-budget orphan-staging policy");
    let over_budget_store = PreparedArtifactStore::open(
        over_budget_directory.path(),
        prepared_storage_domain(),
        over_budget,
    )
    .expect("over-budget orphan-staging store");
    let oversized_staging = over_budget_directory
        .path()
        .join("objects-v3/.staging-oversized");
    fs::create_dir(&oversized_staging).expect("oversized staging directory");
    fs::write(
        oversized_staging.join("payload.bin"),
        vec![0_u8; usize::try_from(over_budget.cache_bytes() + 1).expect("small test budget")],
    )
    .expect("oversized staging payload");
    let over_budget_descriptor = prepared_descriptor(&over_budget_store, &problem);
    let over_budget_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &over_budget_descriptor,
                &over_budget_store,
                operation,
            ))
        },
    )
    .expect("over-budget orphan-staging plan");
    let over_budget_adapter =
        PreparedOperationAdapter::new(operation, over_budget_store, over_budget_descriptor.clone());
    let (over_budget_registry, _) = prepared_registry(&problem, over_budget_adapter);
    let over_budget_attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([162; 32]);
    let error = run_prepared(
        &problem,
        &over_budget_plan,
        &over_budget_registry,
        receipts.bind(execution_provenance(
            over_budget_attempt,
            BuildIdentity::from_sha256([163; 32]),
        )),
    )
    .expect_err("orphan staging above the hard cache ceiling must fail closed");
    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::ResourcePeakExceeded {
            resource: LeaseResource::Storage {
                use_kind: StorageUseKind::PersistentCache,
                ..
            },
            planned,
            actual,
            ..
        }) if planned == over_budget.cache_bytes() && actual == over_budget.cache_bytes() + 1
    ));
    let receipt = receipts
        .open(over_budget_attempt)
        .expect("over-budget orphan-staging receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert_eq!(
        receipt.actual_resource_peak(
            &over_budget_descriptor.work_node_id(operation),
            &prepared_storage_resource(&over_budget_descriptor, StorageUseKind::PersistentCache,),
            &ClaimLifetime::Work,
        ),
        Some(over_budget.cache_bytes() + 1),
        "failure_measurements must retain the uncensored private-store overrun"
    );
    assert!(
        receipt
            .stage_actual_io(
                &over_budget_descriptor.work_node_id(operation),
                IoBufferKind::StorageManager,
            )
            .is_some_and(|(_, operations)| operations > 0),
        "completed private-store inspection I/O must remain in the failure receipt"
    );
}

#[test]
fn cold_operations_account_and_deterministically_remove_orphan_staging() {
    let problem = compile(request(1)).expect("cold orphan-staging problem");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(8, 2_000_000).expect("cold orphan-staging retention"),
    )
    .expect("cold orphan-staging receipt store");

    for (index, operation) in [
        PreparedArtifactOperation::Generate,
        PreparedArtifactOperation::Load,
    ]
    .into_iter()
    .enumerate()
    {
        let cache_directory = prepared_tempdir();
        let store = PreparedArtifactStore::open(
            cache_directory.path(),
            prepared_storage_domain(),
            prepared_budget(),
        )
        .expect("cold orphan-staging store");
        let objects = cache_directory.path().join("objects-v3");
        for (suffix, manifest_bytes, payload_bytes) in
            [("zeta", 31_usize, 47_usize), ("alpha", 19, 23)]
        {
            let orphan = objects.join(format!(".staging-{suffix}"));
            fs::create_dir(&orphan).expect("cold orphan directory");
            fs::write(orphan.join("manifest.json"), vec![0_u8; manifest_bytes])
                .expect("cold orphan manifest");
            fs::write(orphan.join("payload.bin"), vec![0_u8; payload_bytes])
                .expect("cold orphan payload");
        }
        let orphan_bytes = 120;
        let descriptor = prepared_descriptor(&store, &problem);
        let ledger = descriptor.eviction_artifact(operation);
        let expected_ledger_observation = expected_eviction_observed_identity(
            ledger.identity(),
            &[
                (expected_orphan_staging_identity(".staging-alpha", 42), 42),
                (expected_orphan_staging_identity(".staging-zeta", 78), 78),
            ],
        );
        let attempt_byte = 170 + u8::try_from(index).expect("small operation index");
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt_byte; 32]);

        assert!(matches!(
            execute_prepared_operation(
                &problem,
                &receipts,
                store,
                descriptor.clone(),
                operation,
                PreparedRunExpectation {
                    attempt_byte,
                    build_byte: attempt_byte + 2,
                    expect_rejection: false,
                },
            ),
            PreparedObserved::Materialized { .. }
        ));
        let receipt = receipts.open(attempt).expect("cold orphan-staging receipt");
        assert_eq!(receipt.status(), ReceiptStatus::Completed);
        assert_eq!(
            receipt.artifact_actual_bytes(ledger.identity()),
            Some(orphan_bytes)
        );
        assert_eq!(
            receipt.artifact_observed_identity(ledger.identity()),
            Some(expected_ledger_observation.as_bytes()),
            "the ledger must commit the sorted orphan entry identities, count, and bytes"
        );
        assert!(
            receipt
                .actual_resource_peak(
                    &descriptor.work_node_id(operation),
                    &prepared_storage_resource(&descriptor, StorageUseKind::PersistentCache),
                    &ClaimLifetime::Work,
                )
                .is_some_and(|bytes| bytes >= orphan_bytes),
            "the pre-cleanup private-store peak must include orphan staging"
        );
        assert!(
            fs::read_dir(&objects)
                .expect("post-cleanup object inventory")
                .all(|entry| !entry
                    .expect("post-cleanup entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".staging-")),
            "cold cleanup must remove every inspected orphan staging entry"
        );
    }
}

#[test]
fn cold_operation_fails_before_deleting_over_budget_orphan_staging() {
    let problem = compile(request(1)).expect("over-budget cold orphan problem");
    let budget =
        PreparedArtifactBudget::new(20_000, 4, 64).expect("over-budget cold orphan policy");
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(4, 1_000_000).expect("over-budget cold orphan retention"),
    )
    .expect("over-budget cold orphan receipt store");

    for (index, operation) in [
        PreparedArtifactOperation::Generate,
        PreparedArtifactOperation::Load,
    ]
    .into_iter()
    .enumerate()
    {
        let cache_directory = prepared_tempdir();
        let store =
            PreparedArtifactStore::open(cache_directory.path(), prepared_storage_domain(), budget)
                .expect("over-budget cold orphan store");
        let orphan = cache_directory
            .path()
            .join("objects-v3/.staging-over-budget");
        fs::create_dir(&orphan).expect("over-budget cold orphan directory");
        fs::write(
            orphan.join("payload.bin"),
            vec![0_u8; usize::try_from(budget.cache_bytes() + 1).expect("small cache budget")],
        )
        .expect("over-budget cold orphan payload");
        let descriptor = prepared_descriptor(&store, &problem);
        let plan = plan_with_receipts(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            &receipts,
            |problem, _| {
                Ok::<_, ()>(prepared_physical_work(
                    problem,
                    &descriptor,
                    &store,
                    operation,
                ))
            },
        )
        .expect("over-budget cold orphan plan");
        let adapter = PreparedOperationAdapter::new(operation, store, descriptor);
        let (registry, _) = prepared_registry(&problem, adapter);
        let attempt_byte = 174 + u8::try_from(index).expect("small cold operation index");
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt_byte; 32]);

        let error = run_prepared(
            &problem,
            &plan,
            &registry,
            receipts.bind(execution_provenance(
                attempt,
                BuildIdentity::from_sha256([attempt_byte + 2; 32]),
            )),
        )
        .expect_err("cold cleanup must fail closed before deleting over-budget staging");
        assert!(matches!(
            error,
            RunError::Evidence(ExecutionEvidenceError::ResourcePeakExceeded {
                resource: LeaseResource::Storage {
                    use_kind: StorageUseKind::PersistentCache,
                    ..
                },
                planned,
                actual,
                ..
            }) if planned == budget.cache_bytes() && actual == budget.cache_bytes() + 1
        ));
        assert!(
            orphan.exists(),
            "the rejected orphan must remain recoverable"
        );
        let receipt = receipts
            .open(attempt)
            .expect("over-budget cold orphan receipt");
        assert_eq!(receipt.status(), ReceiptStatus::Failed);
        assert_eq!(
            receipt.failure_kind(),
            Some(ReceiptFailureKind::EvidenceContract)
        );
    }
}

#[test]
fn external_adapter_cannot_mint_rejected_stale_evidence() {
    let planned = ArtifactIdentity::from_sha256([207; 32]);
    let forged = forged_rejection_identity(planned, PreparedArtifactRejection::Missing);
    let forged_for_another_artifact = forged_rejection_identity(
        ArtifactIdentity::from_sha256([208; 32]),
        PreparedArtifactRejection::Missing,
    );

    for observed in [
        None,
        Some(ArtifactIdentity::from_sha256([209; 32])),
        Some(forged_for_another_artifact),
        Some(forged),
    ] {
        assert!(
            ArtifactMeasurement::new(
                planned,
                observed,
                ArtifactDisposition::RejectedStale,
                0,
                None,
            )
            .is_err(),
            "the public constructor must reject every stale-evidence identity, including a correctly bound forgery"
        );
    }
    assert_eq!(
        PreparedArtifactRejection::from_evidence_identity(planned, forged),
        Some(PreparedArtifactRejection::Missing),
        "durable receipt decoding remains public"
    );
}

#[test]
fn prepared_resource_overrun_fails_closed_without_censoring_the_peak() {
    let problem = compile(request(1)).expect("prepared overrun problem");
    let cache_directory = prepared_tempdir();
    let store = PreparedArtifactStore::open(
        cache_directory.path(),
        prepared_storage_domain(),
        prepared_budget(),
    )
    .expect("prepared overrun store");
    let descriptor = prepared_descriptor(&store, &problem);
    let operation = PreparedArtifactOperation::Generate;
    let reservation = store
        .reservation(&descriptor, operation)
        .expect("prepared overrun reservation");
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| {
            Ok::<_, ()>(prepared_physical_work(
                problem,
                &descriptor,
                &store,
                operation,
            ))
        },
    )
    .expect("prepared overrun plan");
    let node_id = descriptor.work_node_id(operation);
    let resources = plan.execution_dag().nodes()[&node_id]
        .claims
        .iter()
        .map(|claim| {
            let peak = if matches!(
                claim.resource,
                LeaseResource::IoBuffer(IoBufferKind::StorageManager)
            ) {
                reservation.resident_buffer_bytes().saturating_add(1)
            } else {
                0
            };
            ResourceMeasurement::new(claim.resource.clone(), claim.lifetime.clone(), peak)
        })
        .collect();
    let actual_io = (321, 7);
    let io = vec![IoMeasurement::new(
        IoBufferKind::StorageManager,
        actual_io.0,
        actual_io.1,
    )];
    let ledger = descriptor.eviction_artifact(operation);
    let artifacts = vec![
        ArtifactMeasurement::new(
            descriptor.identity(),
            Some(ArtifactIdentity::from_sha256([157; 32])),
            ArtifactDisposition::Built,
            PREPARED_PAYLOAD_BYTES,
            None,
        )
        .expect("materialized prepared-artifact evidence"),
        ArtifactMeasurement::new(
            ledger.identity(),
            Some(ArtifactIdentity::from_sha256([158; 32])),
            ArtifactDisposition::Loaded,
            37,
            None,
        )
        .expect("completed cache-mutation evidence"),
    ];
    let evidence = WorkMeasurements::new(resources, io, artifacts);
    let prepared_id = descriptor.work_implementation_id(operation);
    let registry = PreparedSuiteRegistry {
        id: registry(3),
        metadata: Some(super::implementation_metadata(&problem)),
        implementations: BTreeMap::from([
            (
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(prepared_base_executor(
                    &descriptor,
                    operation,
                    None,
                ))),
            ),
            (
                prepared_id.clone(),
                PreparedSuiteImplementation::Failure(Box::new(PreparedFailureAdapter {
                    id: prepared_id,
                    evidence,
                    succeed: true,
                })),
            ),
        ]),
        prepared: BTreeMap::from([(
            prepared_registration().implementation().clone(),
            prepared_registration(),
        )]),
    };
    let receipts = plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([155; 32]);

    let error = run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([156; 32]),
        )),
    )
    .expect_err("an observed prepared residency overrun must fail closed");
    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::ResourcePeakExceeded {
            resource: LeaseResource::IoBuffer(IoBufferKind::StorageManager),
            planned,
            actual,
            ..
        }) if planned == reservation.resident_buffer_bytes()
            && actual == reservation.resident_buffer_bytes() + 1
    ));

    let receipt = receipts.open(attempt).expect("prepared overrun receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert_eq!(
        receipt.actual_resource_peak(
            &node_id,
            &LeaseResource::IoBuffer(IoBufferKind::StorageManager),
            &ClaimLifetime::Work,
        ),
        Some(reservation.resident_buffer_bytes() + 1),
        "the immutable failure receipt must retain the uncensored observed overrun"
    );
    assert_eq!(
        receipt.stage_actual_io(&node_id, IoBufferKind::StorageManager),
        Some(actual_io),
        "completed I/O must survive the evidence-contract failure"
    );
    assert_eq!(
        receipt.artifact_disposition(descriptor.identity()),
        Some(ArtifactDisposition::Built)
    );
    assert_eq!(
        receipt.artifact_actual_bytes(ledger.identity()),
        Some(37),
        "completed cache mutations must survive the evidence-contract failure"
    );
}

#[test]
fn public_prepared_cache_evicts_deterministically_and_rejects_casa_boundaries() {
    for marker in ["table.dat", "table.info", "table.f0", "table.lock"] {
        let table_root = prepared_tempdir();
        fs::write(table_root.path().join(marker), b"casacore table marker")
            .expect("casacore table marker");
        let before = directory_entry_names(table_root.path());
        assert!(matches!(
            PreparedArtifactStore::open(
                table_root.path(),
                prepared_storage_domain(),
                prepared_budget()
            ),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
        assert_eq!(directory_entry_names(table_root.path()), before);
    }

    let table_root = prepared_tempdir();
    fs::write(
        table_root.path().join("table.info"),
        b"casacore table marker",
    )
    .expect("nested casacore table marker");
    let before = directory_entry_names(table_root.path());
    let nested_private_root = table_root.path().join("private-cache");
    assert!(matches!(
        PreparedArtifactStore::open(
            &nested_private_root,
            prepared_storage_domain(),
            prepared_budget()
        ),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert_eq!(directory_entry_names(table_root.path()), before);

    #[cfg(unix)]
    {
        let symlink_parent = prepared_tempdir();
        let symlink_root = symlink_parent.path().join("private-cache");
        std::os::unix::fs::symlink(table_root.path(), &symlink_root)
            .expect("casacore table symlink");
        assert!(matches!(
            PreparedArtifactStore::open(
                &symlink_root,
                prepared_storage_domain(),
                prepared_budget()
            ),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
        assert_eq!(directory_entry_names(table_root.path()), before);
    }

    let casa_root = prepared_tempdir();
    fs::create_dir(casa_root.path().join("foo.im")).expect("generic CASA image directory");
    assert!(matches!(
        PreparedArtifactStore::open(
            casa_root.path(),
            prepared_storage_domain(),
            prepared_budget()
        ),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert!(!casa_root.path().join("objects-v3").exists());
    let private_parent = prepared_tempdir();
    let casa_named_root = private_parent.path().join("foo.im");
    assert!(matches!(
        PreparedArtifactStore::open(
            &casa_named_root,
            prepared_storage_domain(),
            prepared_budget()
        ),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert!(!casa_named_root.exists());

    let ancestor_parent = prepared_tempdir();
    let casa_ancestor = ancestor_parent.path().join("ancestor.im");
    fs::create_dir(&casa_ancestor).expect("existing CASA image ancestor");
    let nested_private_root = casa_ancestor.join("private-cache");
    assert!(matches!(
        PreparedArtifactStore::open(
            &nested_private_root,
            prepared_storage_domain(),
            prepared_budget()
        ),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert!(!nested_private_root.exists());

    for casa_entry in ["CFS_0_0_CF_0_0_0.im", "WTCFS_0_0_CF_0_0_0.im", "generic.im"] {
        let casa_cache_parent = prepared_tempdir();
        let casa_cache = casa_cache_parent.path().join("CASA-cache");
        fs::create_dir(&casa_cache).expect("ordinary-named CASA cache root");
        fs::create_dir(casa_cache.join(casa_entry)).expect("existing CASA image");
        let nested_private_root = casa_cache.join("private");
        let nested = PreparedArtifactStore::open(
            &nested_private_root,
            prepared_storage_domain(),
            prepared_budget(),
        )
        .expect("an unrelated CASA-like sibling does not make the parent a CASA table");
        assert!(
            nested.root().ends_with("private"),
            "the candidate private cache is still independently validated for {casa_entry}"
        );
    }

    #[cfg(unix)]
    {
        let symlink_parent = prepared_tempdir();
        let casa_target = symlink_parent.path().join("foo.im");
        fs::create_dir(&casa_target).expect("CASA symlink target");
        let symlink_root = symlink_parent.path().join("private-cache");
        std::os::unix::fs::symlink(&casa_target, &symlink_root).expect("CASA cache symlink");
        assert!(matches!(
            PreparedArtifactStore::open(
                &symlink_root,
                prepared_storage_domain(),
                prepared_budget()
            ),
            Err(PreparedArtifactError::CasaVisiblePath(_))
                | Err(PreparedArtifactError::UnknownCacheEntry(_))
        ));
        assert!(!symlink_root.join("objects-v3").exists());
        assert!(
            fs::read_dir(&casa_target)
                .expect("CASA target inventory")
                .next()
                .is_none()
        );

        let ordinary_target = symlink_parent.path().join("ordinary-target");
        fs::create_dir(&ordinary_target).expect("ordinary symlink target");
        let casa_named_symlink = symlink_parent.path().join("linked.im");
        std::os::unix::fs::symlink(&ordinary_target, &casa_named_symlink)
            .expect("generic CASA-named symlink");
        assert!(matches!(
            PreparedArtifactStore::open(
                &casa_named_symlink,
                prepared_storage_domain(),
                prepared_budget()
            ),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
        assert!(
            fs::read_dir(&ordinary_target)
                .expect("ordinary target inventory")
                .next()
                .is_none()
        );
    }

    let cache_directory = prepared_tempdir();
    let receipts_directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(8, 1_000_000).expect("eviction receipt retention"),
    )
    .expect("eviction receipt store");
    let budget = PreparedArtifactBudget::new(32_768, 2, 64).expect("two-entry cache policy");
    let problems = [
        compile(request_with_geometry(1, geometry(255.0))).expect("first eviction problem"),
        compile(request_with_geometry(1, geometry(254.0))).expect("second eviction problem"),
        compile(request_with_geometry(1, geometry(253.0))).expect("third eviction problem"),
    ];
    let mut identities = Vec::new();
    let mut eviction_ledgers = Vec::new();
    let mut attempt_ids = Vec::new();
    for (index, problem) in problems.iter().enumerate() {
        let store =
            PreparedArtifactStore::open(cache_directory.path(), prepared_storage_domain(), budget)
                .expect("eviction operation store");
        let descriptor = prepared_descriptor(&store, problem);
        identities.push(descriptor.identity());
        eviction_ledgers.push(
            descriptor
                .eviction_artifact(PreparedArtifactOperation::Generate)
                .identity(),
        );
        let attempt_byte = 121 + u8::try_from(index * 2).expect("small attempt index");
        attempt_ids.push(casa_imaging_runtime::ExecutionAttemptId::from_sha256(
            [attempt_byte; 32],
        ));
        assert!(matches!(
            execute_prepared_operation(
                problem,
                &receipts,
                store,
                descriptor,
                PreparedArtifactOperation::Generate,
                PreparedRunExpectation {
                    attempt_byte,
                    build_byte: 122 + u8::try_from(index * 2).expect("small build index"),
                    expect_rejection: false,
                },
            ),
            PreparedObserved::Materialized { .. }
        ));
    }
    let retained = identities[0].max(identities[1]);
    let expected = BTreeSet::from([retained.to_string(), identities[2].to_string()]);
    let actual = fs::read_dir(cache_directory.path().join("objects-v3"))
        .expect("evicted cache inventory")
        .map(|entry| {
            entry
                .expect("evicted cache entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);

    let eviction_receipt = receipts.open(attempt_ids[2]).expect("eviction receipt");
    let eviction_ledger = eviction_ledgers[2];
    assert_eq!(
        eviction_receipt.artifact_role(eviction_ledger),
        Some(ArtifactRole::Input)
    );
    assert_eq!(
        eviction_receipt.artifact_disposition(eviction_ledger),
        Some(ArtifactDisposition::Loaded)
    );
    let evicted_bytes = eviction_receipt
        .artifact_actual_bytes(eviction_ledger)
        .expect("evicted-byte evidence");
    assert!(evicted_bytes > PREPARED_PAYLOAD_BYTES);
    let observed_evictions = eviction_receipt
        .artifact_observed_identity(eviction_ledger)
        .expect("deterministic eviction identity")
        .to_owned();

    fs::remove_dir_all(cache_directory.path().join("objects-v3"))
        .expect("reset temporary eviction cache");
    let mut replay_ledger = None;
    let mut replay_attempt = None;
    for (index, problem) in problems.iter().enumerate() {
        let store =
            PreparedArtifactStore::open(cache_directory.path(), prepared_storage_domain(), budget)
                .expect("replayed eviction operation store");
        let descriptor = prepared_descriptor(&store, problem);
        if index == 2 {
            replay_ledger = Some(
                descriptor
                    .eviction_artifact(PreparedArtifactOperation::Generate)
                    .identity(),
            );
        }
        let attempt_byte = 131 + u8::try_from(index * 2).expect("small replay index");
        replay_attempt = (index == 2)
            .then_some(casa_imaging_runtime::ExecutionAttemptId::from_sha256(
                [attempt_byte; 32],
            ))
            .or(replay_attempt);
        assert!(matches!(
            execute_prepared_operation(
                problem,
                &receipts,
                store,
                descriptor,
                PreparedArtifactOperation::Generate,
                PreparedRunExpectation {
                    attempt_byte,
                    build_byte: 132 + u8::try_from(index * 2).expect("small replay build index"),
                    expect_rejection: false,
                },
            ),
            PreparedObserved::Materialized { .. }
        ));
    }
    let replay_ledger = replay_ledger.expect("replayed eviction ledger");
    assert_eq!(replay_ledger, eviction_ledger);
    let replay_receipt = receipts
        .open(replay_attempt.expect("replayed eviction attempt"))
        .expect("replayed eviction receipt");
    assert_eq!(
        replay_receipt.artifact_actual_bytes(replay_ledger),
        Some(evicted_bytes)
    );
    assert_eq!(
        replay_receipt.artifact_observed_identity(replay_ledger),
        Some(observed_evictions)
    );
}

#[test]
fn private_cache_ancestor_inventory_ignores_unrelated_casa_like_siblings() {
    let parent = prepared_tempdir();
    fs::create_dir(parent.path().join("unrelated.ms")).expect("unrelated CASA-looking sibling");
    fs::create_dir(parent.path().join("CFS_0_0_CF_0_0_0.im"))
        .expect("unrelated CF-cache-looking sibling");

    let root = parent.path().join("private-cache");
    let store = PreparedArtifactStore::open(&root, prepared_storage_domain(), prepared_budget())
        .expect("siblings do not make their parent a CASA/casacore store");
    assert_eq!(
        store.root(),
        fs::canonicalize(root).expect("canonical private root")
    );
}
