// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

const PREPARED_PAYLOAD_BYTES: u64 = (3 * 3 + 5 * 5) * 8;

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
    observed: Mutex<Option<PreparedObserved>>,
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
    ) -> Result<(), Self::Error> {
        Ok(())
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
        let (mut imaging, mut weight) = prepared_payloads();
        let mut imaging = Cursor::new(&mut imaging);
        let mut weight = Cursor::new(&mut weight);
        let mut inputs = [
            PreparedArtifactSegmentInput::new("imaging", &mut imaging),
            PreparedArtifactSegmentInput::new("weight", &mut weight),
        ];
        let (observed, measurements) = match self.operation {
            PreparedArtifactOperation::Generate => {
                let (artifact, measurements) = self
                    .store
                    .generate(&context, &self.descriptor, &mut inputs)
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
                    .load(&context, &self.descriptor, &mut inputs)
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
    ) -> Result<(), Self::Error> {
        Ok(())
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
    ) -> Result<(), Self::Error> {
        match self {
            Self::Base(adapter) => adapter.wait_for_fence(context, fence),
            Self::Prepared(adapter) => adapter.wait_for_fence(context, fence),
            Self::Failure(adapter) => adapter.wait_for_fence(context, fence),
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
    implementations: BTreeMap<WorkImplementationId, PreparedSuiteImplementation>,
}

impl ImplementationRegistry for PreparedSuiteRegistry {
    type Implementation = PreparedSuiteImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.implementations.get(id)
    }
}

fn prepared_budget() -> PreparedArtifactBudget {
    PreparedArtifactBudget::new(32_768, 4, 64).expect("prepared cache budget")
}

fn prepared_owner() -> PreparedArtifactOwner {
    PreparedArtifactOwner::new(
        registry(3),
        "native-awproject",
        "3.1.0",
        WorkImplementationId::new("native-awproject-cpu"),
    )
    .expect("prepared owner")
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
    prepared_descriptor_with_owner(store, problem, prepared_owner())
}

fn prepared_descriptor_with_owner(
    store: &PreparedArtifactStore,
    problem: &casa_imaging_model::CompiledProblem,
    owner: PreparedArtifactOwner,
) -> PreparedArtifactDescriptor {
    prepared_descriptor_with_owner_and_cell(store, problem, owner, prepared_cell(1.0e9))
}

fn prepared_descriptor_with_owner_and_cell(
    store: &PreparedArtifactStore,
    problem: &casa_imaging_model::CompiledProblem,
    owner: PreparedArtifactOwner,
    cell: PreparedArtifactCellKey,
) -> PreparedArtifactDescriptor {
    PreparedArtifactDescriptor::convolution_function(
        store,
        owner,
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

fn prepared_cell(frequency_hz: f64) -> PreparedArtifactCellKey {
    PreparedArtifactCellKey::new(
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
        casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
        true,
        "flatnoise",
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
) -> RecordingExecutor {
    let mut base = recording_executor(6, None, None);
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
    descriptor: &PreparedArtifactDescriptor,
    store: &PreparedArtifactStore,
    operation: PreparedArtifactOperation,
) -> PhysicalWorkBinding {
    let base = physical_work(6);
    let reservation = store
        .reservation(descriptor, operation)
        .expect("prepared reservation");
    let store_allocation = AllocationId::new(format!(
        "prepared-store-buffer-{}",
        operation_name(operation)
    ));
    let store_slot =
        PhysicalSlotId::new(format!("prepared-store-slot-{}", operation_name(operation)));
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 8,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("prepared-artifact-streaming-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let writeback_bytes = if operation == PreparedArtifactOperation::Reuse {
        1
    } else {
        reservation.streaming_buffer_bytes()
    };
    let mut io_allocations = vec![
        (
            IoBufferKind::StorageManager,
            reservation.resident_buffer_bytes(),
            store_allocation.clone(),
            store_slot.clone(),
        ),
        (
            IoBufferKind::Writeback,
            writeback_bytes,
            AllocationId::new(format!(
                "prepared-writeback-buffer-{}",
                operation_name(operation)
            )),
            PhysicalSlotId::new(format!(
                "prepared-writeback-slot-{}",
                operation_name(operation)
            )),
        ),
    ];
    if operation != PreparedArtifactOperation::Reuse {
        io_allocations.push((
            IoBufferKind::SourceReadAhead,
            reservation.streaming_buffer_bytes(),
            AllocationId::new(format!(
                "prepared-source-buffer-{}",
                operation_name(operation)
            )),
            PhysicalSlotId::new(format!(
                "prepared-source-slot-{}",
                operation_name(operation)
            )),
        ));
    }
    let demand_id = format!("private-prepared-cache-{}", descriptor.cache_identity());
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.id = AlternativeId::new(format!("prepared-{}", operation_name(operation)));
    alternative
        .demand
        .memory
        .extend(
            io_allocations
                .iter()
                .map(|(_, bytes, allocation, _)| MemoryDemand {
                    allocation_id: allocation.as_str().to_string(),
                    hard_bytes: *bytes,
                    preferred_bytes: *bytes,
                    views: vec![CapacityViewId::new("host-memory")],
                }),
        );
    alternative.demand.locks = CountDemand::new(2, 2);
    alternative.demand.file_descriptors = CountDemand::new(2, 2);
    if operation != PreparedArtifactOperation::Reuse {
        alternative.demand.io_buffers.source_read_ahead_bytes =
            reservation.streaming_buffer_bytes();
    }
    alternative.demand.io_buffers.storage_manager_bytes = reservation.resident_buffer_bytes();
    alternative.demand.io_buffers.writeback_bytes = writeback_bytes;
    alternative
        .demand
        .storage
        .push(casa_imaging_runtime::StorageDemand {
            demand_id: demand_id.clone(),
            domain: StorageDomainId::new("atomic-output"),
            temporary_bytes: reservation.temporary_staging_bytes(),
            staged_output_bytes: 0,
            final_output_bytes: 0,
            persistent_cache_bytes: reservation.persistent_cache_bytes(),
            read_rate: CountDemand::zero(),
            write_rate: CountDemand::zero(),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        });

    let mut claims = vec![
        ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::Locks,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::FileDescriptors,
            amount: 2,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id: demand_id.clone(),
                use_kind: StorageUseKind::PersistentCache,
            },
            amount: reservation.persistent_cache_bytes(),
            lifetime: ClaimLifetime::Work,
        },
    ];
    claims.extend(
        io_allocations
            .iter()
            .map(|(kind, bytes, _, _)| ResourceClaim {
                resource: LeaseResource::IoBuffer(*kind),
                amount: *bytes,
                lifetime: ClaimLifetime::Work,
            }),
    );
    if reservation.temporary_staging_bytes() > 0 {
        claims.push(ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id,
                use_kind: StorageUseKind::Temporary,
            },
            amount: reservation.temporary_staging_bytes(),
            lifetime: ClaimLifetime::Work,
        });
    }
    let prepared_id = descriptor.work_node_id(operation);
    let prepared_node = WorkNode {
        id: prepared_id.clone(),
        kind: WorkKind::Cache,
        domain: WorkDomain::Cpu,
        implementation: descriptor.work_implementation_id(operation),
        dependencies: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("execute"))]),
        claims,
        allocations: io_allocations
            .iter()
            .map(|(_, _, allocation, _)| AllocationUse {
                allocation: allocation.clone(),
                lifetime: ClaimLifetime::Work,
            })
            .collect(),
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    };
    let release_id = prepared_release_node_id(descriptor, operation);
    let release_node = WorkNode {
        id: release_id.clone(),
        kind: WorkKind::Release,
        domain: WorkDomain::Cpu,
        implementation: implementation(6),
        dependencies: BTreeSet::from([WorkDependency::Work(prepared_node.id.clone())]),
        claims: vec![
            ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: LeaseResource::IoBuffer(IoBufferKind::StorageManager),
                amount: reservation.resident_buffer_bytes(),
                lifetime: ClaimLifetime::Work,
            },
        ],
        allocations: vec![AllocationUse {
            allocation: store_allocation.clone(),
            lifetime: ClaimLifetime::Work,
        }],
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    };
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("transaction-commit"))
        .expect("transaction commit node")
        .dependencies
        .insert(WorkDependency::Work(release_id.clone()));
    nodes.extend([prepared_node.clone(), release_node]);
    let prepared_logical_allocations = io_allocations
        .iter()
        .map(|(kind, bytes, allocation, slot)| LogicalAllocation {
            id: allocation.clone(),
            bytes: *bytes,
            purpose: AllocationPurpose::IoBuffer(*kind),
            compatibility: compatibility.clone(),
            physical_slot: slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: prepared_node.id.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(
                    if *kind == IoBufferKind::StorageManager {
                        release_id.clone()
                    } else {
                        prepared_node.id.clone()
                    },
                )]),
            },
        })
        .collect::<Vec<_>>();
    let prepared_slots = io_allocations
        .iter()
        .map(|(_, bytes, allocation, slot)| PhysicalSlot {
            id: slot.clone(),
            lease_resource: LeaseResource::Memory {
                allocation_id: allocation.as_str().to_string(),
            },
            capacity_bytes: *bytes,
            compatibility: compatibility.clone(),
        })
        .collect::<Vec<_>>();
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
            .chain(prepared_logical_allocations)
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .chain(prepared_slots)
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("prepared execution DAG");
    let mut prepared_io = vec![
        IoPrediction::new(
            IoBufferKind::StorageManager,
            reservation.persistent_cache_bytes(),
            10_000,
        ),
        IoPrediction::new(IoBufferKind::Writeback, reservation.entry_bytes(), 10_000),
    ];
    if operation != PreparedArtifactOperation::Reuse {
        prepared_io.extend([IoPrediction::new(
            IoBufferKind::SourceReadAhead,
            PREPARED_PAYLOAD_BYTES,
            1_000,
        )]);
    }
    let prepared_stage = StagePrediction::new(prepared_node.id.clone(), 1_000).with_io(prepared_io);
    let release_stage = StagePrediction::new(release_id, 100).with_io(vec![IoPrediction::new(
        IoBufferKind::StorageManager,
        reservation.resident_buffer_bytes(),
        1,
    )]);
    let prediction = PlanPrediction::new(
        base.prediction().elapsed_nanos()
            + prepared_stage.elapsed_nanos()
            + release_stage.elapsed_nanos(),
        base.prediction().confidence(),
        base.prediction().uncertainty().to_vec(),
        base.prediction()
            .stages()
            .values()
            .cloned()
            .chain([prepared_stage, release_stage])
            .collect(),
    )
    .expect("prepared prediction");
    PhysicalWorkBinding::new(
        dag,
        prediction,
        base.artifacts()
            .iter()
            .cloned()
            .chain([
                descriptor.planned_artifact(operation),
                descriptor.eviction_artifact(operation),
            ])
            .collect(),
        base.observation_transaction().clone(),
    )
    .expect("prepared physical work")
}

fn prepared_registry(
    adapter: PreparedOperationAdapter,
) -> (PreparedSuiteRegistry, WorkImplementationId) {
    let prepared_id = adapter.id.clone();
    let base = prepared_base_executor(&adapter.descriptor, adapter.operation);
    (
        PreparedSuiteRegistry {
            id: registry(3),
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
    authority_run(
        problem,
        plan,
        &current,
        registry,
        authority(),
        &mut RunToCompletion,
        receipt,
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
    let plan = plan(
        problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(prepared_physical_work(&descriptor, &store, operation)),
    )
    .expect("prepared operation plan");
    let adapter = PreparedOperationAdapter {
        id: descriptor.work_implementation_id(operation),
        operation,
        store,
        descriptor,
        observed: Mutex::new(None),
    };
    let (registry, id) = prepared_registry(adapter);
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
    assert_eq!(observed, rejection.evidence_identity(descriptor.identity()));
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
fn public_prepared_generate_load_and_reuse_are_plan_and_receipt_bound() {
    let problem = compile(request(1)).expect("prepared compiled problem");
    let receipts_directory = tempfile::tempdir().expect("prepared receipts");
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(16, 2_000_000).expect("prepared receipt retention"),
    )
    .expect("prepared receipt store");

    let generated_directory = tempfile::tempdir().expect("generated cache");
    let generated_store =
        PreparedArtifactStore::open(generated_directory.path(), prepared_budget())
            .expect("generated store");
    let generated_descriptor = prepared_descriptor(&generated_store, &problem);
    let generated_reservation = generated_store
        .reservation(&generated_descriptor, PreparedArtifactOperation::Generate)
        .expect("generated reservation");
    assert!(
        generated_reservation.resident_buffer_bytes()
            > generated_reservation.streaming_buffer_bytes() + 64 * 1024,
        "resident reservation must also charge bounded cache-inventory storage"
    );
    let generate_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &generated_descriptor,
                &generated_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("generate plan");
    let generate_adapter = PreparedOperationAdapter {
        id: generated_descriptor.work_implementation_id(PreparedArtifactOperation::Generate),
        operation: PreparedArtifactOperation::Generate,
        store: generated_store,
        descriptor: generated_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (generate_registry, generate_id) = prepared_registry(generate_adapter);
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
        Some(2)
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
    assert!(generate_io.0 > PREPARED_PAYLOAD_BYTES);
    assert!(generate_io.1 > 3);
    assert_eq!(
        generate_receipt.stage_actual_io(&generated_node, IoBufferKind::MappedPageCache),
        None,
        "private-cache reads and controls must not claim mapped-page-cache authority"
    );
    let generate_source_io = generate_receipt
        .stage_actual_io(&generated_node, IoBufferKind::SourceReadAhead)
        .expect("measured caller-source I/O");
    assert_eq!(generate_source_io.0, PREPARED_PAYLOAD_BYTES);
    assert!(generate_source_io.1 > 2);
    let generate_store_write_io = generate_receipt
        .stage_actual_io(&generated_node, IoBufferKind::Writeback)
        .expect("measured private-store write I/O");
    assert!(generate_store_write_io.0 > PREPARED_PAYLOAD_BYTES);
    assert!(generate_store_write_io.1 > 3);
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
        PreparedArtifactBudget::new(32_768, 3, 64).expect("narrower cache policy"),
    )
    .expect("same-root store with narrower policy");
    let narrower_descriptor = prepared_descriptor(&narrower_store, &problem);
    assert_ne!(
        narrower_descriptor.cache_identity(),
        generated_descriptor.cache_identity(),
        "entry-count policy is committed into CacheIdentity"
    );
    assert_ne!(
        narrower_descriptor.identity(),
        generated_descriptor.identity(),
        "cache policy participates in ArtifactIdentity"
    );
    assert_eq!(
        fs::read_dir(generated_directory.path().join("objects-v1"))
            .expect("published cache inventory")
            .count(),
        1,
        "opening a store neither inspects nor mutates prepared entries"
    );
    drop((narrower_descriptor, narrower_store));

    let reuse_store = PreparedArtifactStore::open(generated_directory.path(), prepared_budget())
        .expect("reuse store");
    let reuse_descriptor = prepared_descriptor(&reuse_store, &problem);
    assert_eq!(reuse_descriptor, generated_descriptor);
    let reuse_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &reuse_descriptor,
                &reuse_store,
                PreparedArtifactOperation::Reuse,
            ))
        },
    )
    .expect("reuse plan");
    let reuse_adapter = PreparedOperationAdapter {
        id: reuse_descriptor.work_implementation_id(PreparedArtifactOperation::Reuse),
        operation: PreparedArtifactOperation::Reuse,
        store: reuse_store,
        descriptor: reuse_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (reuse_registry, reuse_id) = prepared_registry(reuse_adapter);
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
        Some((0, 0))
    );
    let reuse_node = reuse_descriptor.work_node_id(PreparedArtifactOperation::Reuse);
    let reuse_writeback = LeaseResource::IoBuffer(IoBufferKind::Writeback);
    assert_eq!(
        reuse_receipt.planned_resource_amount(&reuse_node, &reuse_writeback, &ClaimLifetime::Work,),
        Some(1),
        "reuse reserves for possible private lock-file creation"
    );
    assert_eq!(
        reuse_receipt.actual_resource_peak(&reuse_node, &reuse_writeback, &ClaimLifetime::Work,),
        Some(0),
        "an existing private lock file performs no writeback work"
    );

    let loaded_directory = tempfile::tempdir().expect("loaded cache");
    let loaded_store = PreparedArtifactStore::open(loaded_directory.path(), prepared_budget())
        .expect("loaded store");
    let loaded_descriptor = prepared_descriptor(&loaded_store, &problem);
    assert_ne!(
        loaded_descriptor.cache_identity(),
        generated_descriptor.cache_identity(),
        "the canonical private root is committed into CacheIdentity"
    );
    let load_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &loaded_descriptor,
                &loaded_store,
                PreparedArtifactOperation::Load,
            ))
        },
    )
    .expect("load plan");
    let load_adapter = PreparedOperationAdapter {
        id: loaded_descriptor.work_implementation_id(PreparedArtifactOperation::Load),
        operation: PreparedArtifactOperation::Load,
        store: loaded_store,
        descriptor: loaded_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (load_registry, load_id) = prepared_registry(load_adapter);
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
        Some(ArtifactRole::Input)
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
        load_receipt
            .stage_actual_io(
                &loaded_descriptor.work_node_id(PreparedArtifactOperation::Load),
                IoBufferKind::SourceReadAhead,
            )
            .expect("measured load source I/O")
            .0,
        PREPARED_PAYLOAD_BYTES
    );
    assert!(
        load_receipt
            .stage_actual_io(
                &loaded_descriptor.work_node_id(PreparedArtifactOperation::Load),
                IoBufferKind::Writeback,
            )
            .expect("measured load store-write I/O")
            .0
            > PREPARED_PAYLOAD_BYTES
    );
    assert_ne!(
        generated_descriptor.work_node_id(PreparedArtifactOperation::Generate),
        loaded_descriptor.work_node_id(PreparedArtifactOperation::Load)
    );

    let mismatch_directory = tempfile::tempdir().expect("operation mismatch cache");
    let mismatch_store = PreparedArtifactStore::open(mismatch_directory.path(), prepared_budget())
        .expect("operation mismatch store");
    let mismatch_descriptor = prepared_descriptor(&mismatch_store, &problem);
    let mismatch_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &mismatch_descriptor,
                &mismatch_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("operation mismatch plan");
    let mismatch_adapter = PreparedOperationAdapter {
        id: mismatch_descriptor.work_implementation_id(PreparedArtifactOperation::Generate),
        operation: PreparedArtifactOperation::Load,
        store: mismatch_store,
        descriptor: mismatch_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (mismatch_registry, _) = prepared_registry(mismatch_adapter);
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
                PreparedArtifactError::UnplannedOperation.to_string()
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
        fs::read_dir(mismatch_directory.path().join("objects-v1"))
            .expect("operation mismatch inventory")
            .count(),
        0,
        "the rejected operation cannot publish"
    );

    let owner_mismatch_directory = tempfile::tempdir().expect("owner mismatch cache");
    let owner_mismatch_store =
        PreparedArtifactStore::open(owner_mismatch_directory.path(), prepared_budget())
            .expect("owner mismatch store");
    let spoofed_owner = PreparedArtifactOwner::new(
        registry(4),
        "untrusted-provider",
        "9.9.9",
        WorkImplementationId::new("untrusted-awproject-cpu"),
    )
    .expect("spoofed owner");
    let spoofed_owner_descriptor =
        prepared_descriptor_with_owner(&owner_mismatch_store, &problem, spoofed_owner);
    let cell_one_descriptor = prepared_descriptor_with_owner_and_cell(
        &owner_mismatch_store,
        &problem,
        prepared_owner(),
        prepared_cell(1.0e9),
    );
    let cell_two_descriptor = prepared_descriptor_with_owner_and_cell(
        &owner_mismatch_store,
        &problem,
        prepared_owner(),
        PreparedArtifactCellKey::new(
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
            false,
            "flatnoise",
        )
        .expect("second prepared cell"),
    );
    assert_ne!(
        cell_one_descriptor.identity(),
        cell_two_descriptor.identity(),
        "distinct AW scientific cells cannot share an artifact identity"
    );
    let delimiter_left = prepared_descriptor_with_owner(
        &owner_mismatch_store,
        &problem,
        PreparedArtifactOwner::new(
            registry(3),
            "provider-a",
            "version-b",
            WorkImplementationId::new("implementation-c"),
        )
        .expect("left delimiter owner"),
    );
    let delimiter_right = prepared_descriptor_with_owner(
        &owner_mismatch_store,
        &problem,
        PreparedArtifactOwner::new(
            registry(3),
            "provider",
            "a-version-b",
            WorkImplementationId::new("implementation-c"),
        )
        .expect("right delimiter owner"),
    );
    assert_ne!(
        delimiter_left.work_implementation_id(PreparedArtifactOperation::Generate),
        delimiter_right.work_implementation_id(PreparedArtifactOperation::Generate),
        "owner fields use length-prefixed implementation identity hashing"
    );
    let owner_mismatch_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &spoofed_owner_descriptor,
                &owner_mismatch_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("owner mismatch plan");
    let owner_mismatch_adapter = PreparedOperationAdapter {
        id: spoofed_owner_descriptor.work_implementation_id(PreparedArtifactOperation::Generate),
        operation: PreparedArtifactOperation::Generate,
        store: owner_mismatch_store,
        descriptor: spoofed_owner_descriptor,
        observed: Mutex::new(None),
    };
    let (owner_mismatch_registry, _) = prepared_registry(owner_mismatch_adapter);
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
        fs::read_dir(owner_mismatch_directory.path().join("objects-v1"))
            .expect("owner mismatch inventory")
            .count(),
        0
    );
}

#[test]
fn public_prepared_scientific_keys_are_exact_typed_and_bounded() {
    let problem = compile(request(1)).expect("prepared scientific-key problem");
    let directory = tempfile::tempdir().expect("prepared scientific-key cache");
    let store = PreparedArtifactStore::open(directory.path(), prepared_budget())
        .expect("prepared scientific-key store");
    let owner = prepared_owner();
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
        PreparedArtifactCellKey::new(
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::BaselineMeters,
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
            casa_imaging_runtime::PreparedArtifactAwInterpretation::Wavelength,
            false,
        ),
    ];
    let aw_identities = aw_cells
        .into_iter()
        .map(|cell| {
            prepared_descriptor_with_owner_and_cell(&store, &problem, owner.clone(), cell)
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
    let spectral = |owner_artifact_key| {
        PreparedArtifactDescriptor::new(
            &store,
            owner.clone(),
            casa_imaging_runtime::PreparedArtifactKind::SpectralMap,
            &problem,
            PreparedArtifactScientificKey::SpectralMap(
                PreparedArtifactSpectralMapKey::new(owner_artifact_key, 1.0e9, 3, 1.0e6, "lsrk")
                    .expect("spectral-map key"),
            ),
            vec![segment.clone()],
        )
        .expect("spectral-map descriptor")
    };
    assert_ne!(
        spectral("owner-map-a").identity(),
        spectral("owner-map-b").identity(),
        "same-kind/same-layout spectral maps require owner-derived identity separation"
    );

    let kernel = |owner_artifact_key| {
        PreparedArtifactDescriptor::new(
            &store,
            owner.clone(),
            casa_imaging_runtime::PreparedArtifactKind::Kernel,
            &problem,
            PreparedArtifactScientificKey::Kernel(
                PreparedArtifactKernelKey::new(
                    owner_artifact_key,
                    PreparedArtifactKernelAlgorithm::Gridding,
                    vec![4],
                    vec![4],
                    vec![0],
                    vec![1],
                    PreparedArtifactPrecision::U8,
                )
                .expect("kernel key"),
            ),
            vec![segment.clone()],
        )
        .expect("kernel descriptor")
    };
    assert_ne!(
        kernel("owner-kernel-a").identity(),
        kernel("owner-kernel-b").identity(),
        "same-kind/same-layout kernels require owner-derived identity separation"
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
        PreparedArtifactDescriptor::new(
            &store,
            owner,
            casa_imaging_runtime::PreparedArtifactKind::Kernel,
            &problem,
            PreparedArtifactScientificKey::Kernel(
                PreparedArtifactKernelKey::new(
                    "owner-too-many-segments",
                    PreparedArtifactKernelAlgorithm::Gridding,
                    vec![1],
                    vec![1],
                    vec![0],
                    vec![1],
                    PreparedArtifactPrecision::U8,
                )
                .expect("bounded kernel key"),
            ),
            too_many_segments,
        ),
        Err(PreparedArtifactError::InvalidDescriptor)
    ));
}

#[test]
fn public_prepared_reuse_receipts_fail_closed_rejections() {
    let problem = compile(request(1)).expect("prepared rejection problem");
    let receipts_directory = tempfile::tempdir().expect("rejection receipts");
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(16, 2_000_000).expect("rejection receipt retention"),
    )
    .expect("rejection receipt store");

    let missing_directory = tempfile::tempdir().expect("missing cache");
    let missing_store = PreparedArtifactStore::open(missing_directory.path(), prepared_budget())
        .expect("missing store");
    let missing_descriptor = prepared_descriptor(&missing_store, &problem);
    let missing_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &missing_descriptor,
                &missing_store,
                PreparedArtifactOperation::Reuse,
            ))
        },
    )
    .expect("missing reuse plan");
    let missing_adapter = PreparedOperationAdapter {
        id: missing_descriptor.work_implementation_id(PreparedArtifactOperation::Reuse),
        operation: PreparedArtifactOperation::Reuse,
        store: missing_store,
        descriptor: missing_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (missing_registry, missing_id) = prepared_registry(missing_adapter);
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
            PreparedArtifactRejection::Missing
                .evidence_identity(missing_descriptor.identity())
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

    let corrupt_directory = tempfile::tempdir().expect("corrupt cache");
    let generate_store = PreparedArtifactStore::open(corrupt_directory.path(), prepared_budget())
        .expect("corrupt seed store");
    let generate_descriptor = prepared_descriptor(&generate_store, &problem);
    let generate_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &generate_descriptor,
                &generate_store,
                PreparedArtifactOperation::Generate,
            ))
        },
    )
    .expect("corrupt seed plan");
    let generate_adapter = PreparedOperationAdapter {
        id: generate_descriptor.work_implementation_id(PreparedArtifactOperation::Generate),
        operation: PreparedArtifactOperation::Generate,
        store: generate_store,
        descriptor: generate_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (generate_registry, _) = prepared_registry(generate_adapter);
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
    let object = fs::read_dir(corrupt_directory.path().join("objects-v1"))
        .expect("cache objects")
        .next()
        .expect("published object")
        .expect("object entry")
        .path();
    let payload = object.join("payload.bin");
    let mut bytes = fs::read(&payload).expect("published payload");
    bytes[0] ^= 1;
    fs::write(&payload, bytes).expect("corrupt payload");

    let corrupt_store = PreparedArtifactStore::open(corrupt_directory.path(), prepared_budget())
        .expect("corrupt reuse store");
    let corrupt_descriptor = prepared_descriptor(&corrupt_store, &problem);
    let corrupt_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(prepared_physical_work(
                &corrupt_descriptor,
                &corrupt_store,
                PreparedArtifactOperation::Reuse,
            ))
        },
    )
    .expect("corrupt reuse plan");
    let corrupt_adapter = PreparedOperationAdapter {
        id: corrupt_descriptor.work_implementation_id(PreparedArtifactOperation::Reuse),
        operation: PreparedArtifactOperation::Reuse,
        store: corrupt_store,
        descriptor: corrupt_descriptor.clone(),
        observed: Mutex::new(None),
    };
    let (corrupt_registry, corrupt_id) = prepared_registry(corrupt_adapter);
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

    let incomplete_directory = tempfile::tempdir().expect("incomplete cache");
    let incomplete_store =
        PreparedArtifactStore::open(incomplete_directory.path(), prepared_budget())
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
        .join("objects-v1")
        .join(incomplete_descriptor.identity().to_string());
    fs::remove_file(incomplete_object.join("payload.bin")).expect("remove prepared payload");
    let incomplete_store =
        PreparedArtifactStore::open(incomplete_directory.path(), prepared_budget())
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

    let incompatible_directory = tempfile::tempdir().expect("incompatible cache");
    let incompatible_store =
        PreparedArtifactStore::open(incompatible_directory.path(), prepared_budget())
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
        .join("objects-v1")
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
    let incompatible_store =
        PreparedArtifactStore::open(incompatible_directory.path(), prepared_budget())
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

    let oversized_manifest_directory = tempfile::tempdir().expect("oversized manifest cache");
    let oversized_manifest_store =
        PreparedArtifactStore::open(oversized_manifest_directory.path(), prepared_budget())
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
        .join("objects-v1")
        .join(oversized_manifest_descriptor.identity().to_string())
        .join("manifest.json");
    fs::write(&oversized_manifest_path, vec![b'{'; 16 * 1024 + 1])
        .expect("write oversized manifest");
    let oversized_manifest_store =
        PreparedArtifactStore::open(oversized_manifest_directory.path(), prepared_budget())
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

    let nonfinite_directory = tempfile::tempdir().expect("nonfinite cache");
    let nonfinite_store =
        PreparedArtifactStore::open(nonfinite_directory.path(), prepared_budget())
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
        .join("objects-v1")
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
    let nonfinite_store =
        PreparedArtifactStore::open(nonfinite_directory.path(), prepared_budget())
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
    let cache_directory = tempfile::tempdir().expect("prepared failure cache");
    let store = PreparedArtifactStore::open(cache_directory.path(), prepared_budget())
        .expect("prepared failure store");
    let descriptor = prepared_descriptor(&store, &problem);
    let operation = PreparedArtifactOperation::Generate;
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(prepared_physical_work(&descriptor, &store, operation)),
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
        .map(|prediction| match prediction.kind() {
            IoBufferKind::StorageManager => IoMeasurement::new(prediction.kind(), 321, 7),
            IoBufferKind::Writeback => IoMeasurement::new(prediction.kind(), 17, 2),
            kind => IoMeasurement::new(kind, 0, 0),
        })
        .collect();
    let ledger = descriptor.eviction_artifact(operation);
    let observed_evictions = ArtifactIdentity::from_sha256([211; 32]);
    let observed_materialized = ArtifactIdentity::from_sha256([210; 32]);
    let evidence = WorkMeasurements::new(
        resources,
        io,
        vec![
            ArtifactMeasurement::new(
                descriptor.identity(),
                Some(observed_materialized),
                ArtifactDisposition::Built,
                PREPARED_PAYLOAD_BYTES,
                Some(RedactedPath::from_path(cache_directory.path())),
            ),
            ArtifactMeasurement::new(
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
        implementations: BTreeMap::from([
            (
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(prepared_base_executor(
                    &descriptor,
                    operation,
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
    };
    let receipts_directory = tempfile::tempdir().expect("prepared failure receipts");
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(4, 1_000_000).expect("prepared failure retention"),
    )
    .expect("prepared failure receipt store");
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
        Some((321, 7))
    );
    assert_eq!(
        receipt.stage_actual_io(&node_id, IoBufferKind::Writeback),
        Some((17, 2))
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
fn malformed_rejection_evidence_is_rejected_without_partial_receipt_mutation() {
    let problem = compile(request(1)).expect("malformed rejection problem");
    let cache_directory = tempfile::tempdir().expect("malformed rejection cache");
    let store = PreparedArtifactStore::open(cache_directory.path(), prepared_budget())
        .expect("malformed rejection store");
    let descriptor = prepared_descriptor(&store, &problem);
    let operation = PreparedArtifactOperation::Reuse;
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(prepared_physical_work(&descriptor, &store, operation)),
    )
    .expect("malformed rejection plan");
    let node_id = descriptor.work_node_id(operation);
    let resources = plan.execution_dag().nodes()[&node_id]
        .claims
        .iter()
        .map(|claim| ResourceMeasurement::new(claim.resource.clone(), claim.lifetime.clone(), 0))
        .collect();
    let io = plan.prediction().stages()[&node_id]
        .io()
        .iter()
        .map(|prediction| IoMeasurement::new(prediction.kind(), 0, 1))
        .collect();
    let ledger = descriptor.eviction_artifact(operation);
    let evidence = WorkMeasurements::new(
        resources,
        io,
        vec![
            ArtifactMeasurement::new(
                descriptor.identity(),
                Some(PreparedArtifactRejection::Missing.evidence_identity(descriptor.identity())),
                ArtifactDisposition::RejectedStale,
                0,
                None,
            ),
            ArtifactMeasurement::new(
                ledger.identity(),
                Some(ArtifactIdentity::from_sha256([212; 32])),
                ArtifactDisposition::Staged,
                0,
                None,
            ),
        ],
    );
    let prepared_id = descriptor.work_implementation_id(operation);
    let registry = PreparedSuiteRegistry {
        id: registry(3),
        implementations: BTreeMap::from([
            (
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(prepared_base_executor(
                    &descriptor,
                    operation,
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
    };
    let receipts_directory = tempfile::tempdir().expect("malformed rejection receipts");
    let receipts = ExecutionReceiptStore::new(
        receipts_directory.path(),
        ReceiptRetention::new(4, 1_000_000).expect("malformed rejection retention"),
    )
    .expect("malformed rejection receipt store");
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([153; 32]);

    let error = run_prepared(
        &problem,
        &plan,
        &registry,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([154; 32]),
        )),
    )
    .expect_err("malformed rejection evidence must fail closed");
    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::ArtifactDispositionMismatch {
            artifact,
            ..
        }) if artifact == ledger.identity()
    ));

    let receipt = receipts.open(attempt).expect("malformed rejection receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert!(receipt.stage_actual_elapsed_nanos(&node_id).is_some());
    assert_eq!(receipt.artifact_actual_bytes(descriptor.identity()), None);
    assert_eq!(receipt.artifact_actual_bytes(ledger.identity()), None);
    assert_eq!(
        receipt.actual_resource_peak(&node_id, &LeaseResource::Workers, &ClaimLifetime::Work,),
        None,
        "invalid evidence must not be checkpointed partially"
    );
}

#[test]
fn public_prepared_cache_evicts_deterministically_and_rejects_casa_boundaries() {
    let casa_root = tempfile::tempdir().expect("CASA-looking root contents");
    fs::create_dir(casa_root.path().join("foo.im")).expect("generic CASA image directory");
    assert!(matches!(
        PreparedArtifactStore::open(casa_root.path(), prepared_budget()),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert!(!casa_root.path().join("objects-v1").exists());
    let private_parent = tempfile::tempdir().expect("private cache parent");
    let casa_named_root = private_parent.path().join("foo.im");
    assert!(matches!(
        PreparedArtifactStore::open(&casa_named_root, prepared_budget()),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert!(!casa_named_root.exists());

    let ancestor_parent = tempfile::tempdir().expect("CASA ancestor parent");
    let casa_ancestor = ancestor_parent.path().join("ancestor.im");
    fs::create_dir(&casa_ancestor).expect("existing CASA image ancestor");
    let nested_private_root = casa_ancestor.join("private-cache");
    assert!(matches!(
        PreparedArtifactStore::open(&nested_private_root, prepared_budget()),
        Err(PreparedArtifactError::CasaVisiblePath(_))
    ));
    assert!(!nested_private_root.exists());

    for casa_entry in ["CFS_0_0_CF_0_0_0.im", "WTCFS_0_0_CF_0_0_0.im", "generic.im"] {
        let casa_cache_parent = tempfile::tempdir().expect("CASA cache parent");
        let casa_cache = casa_cache_parent.path().join("CASA-cache");
        fs::create_dir(&casa_cache).expect("ordinary-named CASA cache root");
        fs::create_dir(casa_cache.join(casa_entry)).expect("existing CASA image");
        let nested_private_root = casa_cache.join("private");
        assert!(matches!(
            PreparedArtifactStore::open(&nested_private_root, prepared_budget()),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
        assert!(
            !nested_private_root.exists(),
            "CASA ancestry containing {casa_entry} must be rejected before creating the private root"
        );
    }

    #[cfg(unix)]
    {
        let symlink_parent = tempfile::tempdir().expect("symlink cache parent");
        let casa_target = symlink_parent.path().join("foo.im");
        fs::create_dir(&casa_target).expect("CASA symlink target");
        let symlink_root = symlink_parent.path().join("private-cache");
        std::os::unix::fs::symlink(&casa_target, &symlink_root).expect("CASA cache symlink");
        assert!(matches!(
            PreparedArtifactStore::open(&symlink_root, prepared_budget()),
            Err(PreparedArtifactError::CasaVisiblePath(_))
                | Err(PreparedArtifactError::UnknownCacheEntry(_))
        ));
        assert!(!symlink_root.join("objects-v1").exists());
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
            PreparedArtifactStore::open(&casa_named_symlink, prepared_budget()),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
        assert!(
            fs::read_dir(&ordinary_target)
                .expect("ordinary target inventory")
                .next()
                .is_none()
        );
    }

    let cache_directory = tempfile::tempdir().expect("eviction cache");
    let receipts_directory = tempfile::tempdir().expect("eviction receipts");
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
        let store = PreparedArtifactStore::open(cache_directory.path(), budget)
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
    let actual = fs::read_dir(cache_directory.path().join("objects-v1"))
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

    fs::remove_dir_all(cache_directory.path().join("objects-v1"))
        .expect("reset temporary eviction cache");
    let mut replay_ledger = None;
    let mut replay_attempt = None;
    for (index, problem) in problems.iter().enumerate() {
        let store = PreparedArtifactStore::open(cache_directory.path(), budget)
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
