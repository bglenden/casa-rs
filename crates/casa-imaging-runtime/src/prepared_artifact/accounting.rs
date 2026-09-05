// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn validate_plan_binding(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    reservation: PreparedArtifactReservation,
    source: Option<PreparedArtifactSourceBinding<'_>>,
    cache_demand_id: &str,
) -> Result<(), PreparedArtifactError> {
    if descriptor.compatibility.owner.implementation_registry
        != context.implementation_registry_id()
    {
        return Err(PreparedArtifactError::ImplementationRegistryMismatch);
    }
    if !descriptor.matches_context(context) {
        return Err(PreparedArtifactError::ScientificBindingMismatch);
    }
    match (operation, source) {
        (PreparedArtifactOperation::Load, Some(source)) => {
            validate_source_binding(context, descriptor, source)?;
        }
        (PreparedArtifactOperation::Load, None)
        | (
            PreparedArtifactOperation::Generate
            | PreparedArtifactOperation::Reuse
            | PreparedArtifactOperation::Consume,
            Some(_),
        ) => {
            return Err(PreparedArtifactError::UnplannedSource);
        }
        (
            PreparedArtifactOperation::Generate
            | PreparedArtifactOperation::Reuse
            | PreparedArtifactOperation::Consume,
            None,
        ) => {}
    }
    validate_plan_declaration(
        context,
        descriptor,
        operation,
        reservation,
        source,
        cache_demand_id,
    )
}

pub(super) fn validate_catalog_plan_binding(
    context: WorkExecutionContext<'_>,
    store: &PreparedArtifactStore,
    descriptors: &[PreparedArtifactDescriptor],
    reservation: PreparedArtifactReservation,
) -> Result<(), PreparedArtifactError> {
    validate_catalog_descriptors(store, descriptors)?;
    for descriptor in descriptors {
        if descriptor.compatibility.owner.implementation_registry
            != context.implementation_registry_id()
        {
            return Err(PreparedArtifactError::ImplementationRegistryMismatch);
        }
        if !descriptor.matches_context(context) {
            return Err(PreparedArtifactError::ScientificBindingMismatch);
        }
    }
    let node = context.node();
    if node.kind != WorkKind::Cache
        || node.id != catalog_work_node_id(descriptors)?
        || node.implementation != catalog_work_implementation_id(descriptors)?
    {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let planned = context.planned_artifacts().cloned().collect::<Vec<_>>();
    let expected = catalog_planned_artifacts(descriptors)?;
    if planned.len() != expected.len()
        || expected.iter().any(|expected| {
            !planned.iter().any(|actual| {
                actual.identity() == expected.identity()
                    && actual.node() == expected.node()
                    && actual.role() == expected.role()
                    && actual.cache_identity() == expected.cache_identity()
            })
        })
        || planned
            .iter()
            .any(|artifact| artifact.role() == ArtifactRole::Output)
    {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let first = descriptors
        .first()
        .ok_or(PreparedArtifactError::InvalidDescriptor)?;
    let demand_id = store.storage_demand_id(first);
    let matching = context
        .resource_alternative()
        .demand
        .storage
        .iter()
        .filter(|demand| demand.demand_id == demand_id && demand.domain == *store.storage_domain())
        .collect::<Vec<_>>();
    if matching.len() != 1
        || matching[0].persistent_cache_bytes < reservation.persistent_cache_bytes
        || matching[0].temporary_bytes != 0
        || matching[0].read_rate.hard() == 0
        || matching[0].write_rate.hard() == 0
        || matching[0].operations_rate.hard() == 0
        || matching[0].queue_slots.hard() == 0
    {
        return Err(PreparedArtifactError::MissingReservation(
            "catalog private-cache storage demand",
        ));
    }
    for (resource, amount, label) in [
        (LeaseResource::Workers, 1, "worker"),
        (LeaseResource::Locks, 1, "private-cache lock"),
        (
            LeaseResource::FileDescriptors,
            reservation.file_descriptors,
            "file descriptors",
        ),
        (
            LeaseResource::IoBuffer(IoBufferKind::StorageManager),
            reservation.resident_buffer_bytes,
            "private-store buffer",
        ),
        (
            LeaseResource::Storage {
                demand_id: demand_id.clone(),
                use_kind: StorageUseKind::PersistentCache,
            },
            reservation.persistent_cache_bytes,
            "persistent-cache storage",
        ),
        (
            LeaseResource::StorageReadRate {
                demand_id: demand_id.clone(),
            },
            1,
            "private-cache read rate",
        ),
        (
            LeaseResource::StorageWriteRate {
                demand_id: demand_id.clone(),
            },
            1,
            "private-cache write rate",
        ),
        (
            LeaseResource::StorageOperationsRate {
                demand_id: demand_id.clone(),
            },
            1,
            "private-cache operations rate",
        ),
        (
            LeaseResource::StorageQueue { demand_id },
            1,
            "private-cache queue",
        ),
    ] {
        require_claim(node, |claim| claim == &resource, amount, label)?;
    }
    let stage = context.stage_prediction();
    if stage.io().len() != 1
        || stage.io()[0].kind() != IoBufferKind::StorageManager
        || stage.io()[0].bytes() < reservation.persistent_cache_bytes
        || stage.io()[0].operations() == 0
    {
        return Err(PreparedArtifactError::MissingReservation(
            "catalog cache I/O prediction",
        ));
    }
    Ok(())
}

fn validate_source_binding(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    source: PreparedArtifactSourceBinding<'_>,
) -> Result<(), PreparedArtifactError> {
    match source {
        PreparedArtifactSourceBinding::Files(source) => {
            validate_source_segments(descriptor, &source.segments)?;
            if derive_load_source_identity(descriptor, &source.segments)? != source.identity {
                return Err(PreparedArtifactError::SourceIdentityMismatch);
            }
        }
        PreparedArtifactSourceBinding::Import(source) => {
            validate_import_segments(descriptor, &source.segments)?;
            if derive_import_source_identity(descriptor, &source.segments)? != source.identity {
                return Err(PreparedArtifactError::SourceIdentityMismatch);
            }
            for segment in &source.segments {
                let canonical = segment
                    .source
                    .canonicalize()
                    .map_err(|_| PreparedArtifactError::InvalidSource)?;
                let metadata = canonical
                    .metadata()
                    .map_err(|_| PreparedArtifactError::InvalidSource)?;
                if canonical.as_path() != segment.source.as_ref()
                    || !canonical.starts_with(segment.storage_root.as_ref())
                    || !metadata.file_type().is_dir()
                    || metadata.dev() != segment.source_device
                    || metadata.ino() != segment.source_inode
                {
                    return Err(PreparedArtifactError::InvalidSource);
                }
            }
        }
    }
    validate_source_producer(context, source.identity(), source.producer())
}

fn validate_source_producer(
    context: WorkExecutionContext<'_>,
    identity: ArtifactIdentity,
    producer: &WorkNodeId,
) -> Result<(), PreparedArtifactError> {
    let planned = context
        .plan_artifact(identity)
        .ok_or(PreparedArtifactError::UnplannedSource)?;
    if planned.node() != producer
        || planned.role() != ArtifactRole::Input
        || planned.cache_identity().is_some()
        || producer == &context.node().id
        || !context
            .node()
            .dependencies
            .iter()
            .any(|dependency| match dependency {
                WorkDependency::Work(node) => node == producer,
                WorkDependency::Fence(fence) => fence.node() == producer,
            })
    {
        return Err(PreparedArtifactError::SourceProducerMismatch);
    }
    Ok(())
}

pub(super) fn validate_plan_declaration(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    reservation: PreparedArtifactReservation,
    source: Option<PreparedArtifactSourceBinding<'_>>,
    cache_demand_id: &str,
) -> Result<(), PreparedArtifactError> {
    let node = context.node();
    let planned = context.planned_artifacts().cloned().collect::<Vec<_>>();
    let stage = context.stage_prediction();
    let alternative = context.resource_alternative();
    if node.kind != WorkKind::Cache {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    if node.id != descriptor.work_node_id(operation)
        || node.implementation != descriptor.work_implementation_id(operation)
    {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    if planned
        .iter()
        .any(|artifact| artifact.role() == ArtifactRole::Output)
    {
        return Err(PreparedArtifactError::ProductAuthorityViolation);
    }
    if planned.len() != 2 {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let expected = descriptor.planned_artifact(operation);
    let expected_ledger = descriptor.eviction_artifact(operation);
    let selected = planned
        .iter()
        .find(|artifact| artifact.identity() == expected.identity());
    let ledger = planned
        .iter()
        .find(|artifact| artifact.identity() == expected_ledger.identity());
    if selected.is_none()
        || ledger.is_none()
        || selected.is_some_and(|artifact| {
            artifact.cache_identity() != Some(descriptor.cache_identity())
                || artifact.role() != expected.role()
                || artifact.node() != expected.node()
        })
        || ledger.is_some_and(|artifact| {
            artifact.cache_identity().is_some()
                || artifact.role() != ArtifactRole::Input
                || artifact.node() != expected_ledger.node()
        })
    {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let demand_id = cache_demand_id.to_string();
    let storage_domain = descriptor.storage_domain_id();
    let matching_demands = alternative
        .demand
        .storage
        .iter()
        .filter(|demand| demand.demand_id == demand_id && demand.domain == storage_domain)
        .collect::<Vec<_>>();
    if matching_demands.len() != 1
        || alternative
            .demand
            .storage
            .iter()
            .any(|demand| demand.demand_id == demand_id && demand.domain != storage_domain)
    {
        return Err(PreparedArtifactError::MissingReservation(
            "private-cache storage domain",
        ));
    }
    let storage = matching_demands[0];
    if storage.persistent_cache_bytes < reservation.persistent_cache_bytes
        || storage.temporary_bytes < reservation.temporary_staging_bytes
        || storage.read_rate.hard() == 0
        || storage.write_rate.hard() == 0
        || storage.operations_rate.hard() == 0
        || storage.queue_slots.hard() == 0
    {
        return Err(PreparedArtifactError::MissingReservation(
            "private-cache storage demand",
        ));
    }
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::Workers),
        1,
        "worker",
    )?;
    require_claim(
        node,
        |resource| {
            matches!(
                resource,
                LeaseResource::IoBuffer(IoBufferKind::StorageManager)
            )
        },
        reservation.resident_buffer_bytes,
        "private-store buffer",
    )?;
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::Locks),
        1,
        "private-cache lock",
    )?;
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::FileDescriptors),
        reservation.file_descriptors,
        "file descriptors",
    )?;
    require_claim(
        node,
        |resource| {
            matches!(
                resource,
                LeaseResource::Storage {
                    demand_id: claim_id,
                    use_kind: StorageUseKind::PersistentCache,
                } if claim_id == &demand_id
            )
        },
        reservation.persistent_cache_bytes,
        "persistent-cache storage",
    )?;
    if reservation.temporary_staging_bytes > 0 {
        require_claim(
            node,
            |resource| {
                matches!(
                    resource,
                    LeaseResource::Storage {
                        demand_id: claim_id,
                        use_kind: StorageUseKind::Temporary,
                    } if claim_id == &demand_id
                )
            },
            reservation.temporary_staging_bytes,
            "temporary staging storage",
        )?;
    }
    for (resource, label) in [
        (
            LeaseResource::StorageReadRate {
                demand_id: demand_id.clone(),
            },
            "private-cache read rate",
        ),
        (
            LeaseResource::StorageWriteRate {
                demand_id: demand_id.clone(),
            },
            "private-cache write rate",
        ),
        (
            LeaseResource::StorageOperationsRate {
                demand_id: demand_id.clone(),
            },
            "private-cache operations rate",
        ),
        (
            LeaseResource::StorageQueue { demand_id },
            "private-cache queue",
        ),
    ] {
        require_claim(node, |claim| claim == &resource, 1, label)?;
    }
    let source_demands = source
        .map(PreparedArtifactSourceBinding::storage_demands)
        .unwrap_or_default();
    for (source_demand_id, source_domain) in source_demands {
        let matching = alternative
            .demand
            .storage
            .iter()
            .filter(|demand| demand.demand_id == source_demand_id && demand.domain == source_domain)
            .collect::<Vec<_>>();
        if matching.len() != 1
            || alternative.demand.storage.iter().any(|demand| {
                demand.demand_id == source_demand_id && demand.domain != source_domain
            })
            || matching[0].read_rate.hard() == 0
            || matching[0].write_rate.hard() != 0
            || matching[0].operations_rate.hard() == 0
            || matching[0].queue_slots.hard() == 0
            || matching[0].temporary_bytes != 0
            || matching[0].staged_output_bytes != 0
            || matching[0].final_output_bytes != 0
            || matching[0].persistent_cache_bytes != 0
        {
            return Err(PreparedArtifactError::MissingReservation(
                "cold-load source storage demand",
            ));
        }
        for (resource, label) in [
            (
                LeaseResource::StorageReadRate {
                    demand_id: source_demand_id.clone(),
                },
                "cold-load source read rate",
            ),
            (
                LeaseResource::StorageOperationsRate {
                    demand_id: source_demand_id.clone(),
                },
                "cold-load source operations rate",
            ),
            (
                LeaseResource::StorageQueue {
                    demand_id: source_demand_id,
                },
                "cold-load source queue",
            ),
        ] {
            require_claim(node, |claim| claim == &resource, 1, label)?;
        }
    }
    // Generation and load use one Vec as both the file-source read buffer
    // and the private-store write buffer. The complete resident envelope,
    // including source descriptors and that Vec, therefore has one
    // StorageManager claim and one MemoryDemand-backed slot. Source and store
    // traffic remain separately counted inside ValidationEvidence before being
    // folded into the complete private-store I/O measurement. A second
    // SourceReadAhead claim would describe a physical allocation that does not
    // exist and would charge the same unified memory twice.
    let required_io = [IoBufferKind::StorageManager];
    let predicted = stage
        .io()
        .iter()
        .map(|prediction| prediction.kind())
        .collect::<BTreeSet<_>>();
    let minimum_storage_io_bytes = reservation
        .entry_bytes
        .checked_add(reservation.source_read_bytes)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    if predicted.len() != required_io.len()
        || required_io.iter().any(|kind| !predicted.contains(kind))
        || stage.io().iter().any(|prediction| {
            let minimum_bytes = match prediction.kind() {
                IoBufferKind::StorageManager => minimum_storage_io_bytes,
                _ => u64::MAX,
            };
            prediction.bytes() < minimum_bytes || prediction.operations() == 0
        })
    {
        return Err(PreparedArtifactError::MissingReservation(
            "cache I/O prediction",
        ));
    }
    Ok(())
}

pub(super) fn require_claim(
    node: &crate::WorkNode,
    predicate: impl Fn(&LeaseResource) -> bool,
    required: u64,
    label: &'static str,
) -> Result<(), PreparedArtifactError> {
    let amount = node
        .claims
        .iter()
        .filter(|claim| predicate(&claim.resource))
        .try_fold(0_u64, |total, claim| total.checked_add(claim.amount))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    if amount < required {
        Err(PreparedArtifactError::MissingReservation(label))
    } else {
        Ok(())
    }
}

pub(super) fn measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    disposition: ArtifactDisposition,
    validated: &ValidatedArtifact,
    input: MeasurementInput,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        descriptor,
        input.operation,
        input.cache_bytes,
        validated.disk_bytes,
        &input.evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| input.evidence.measurement(prediction.kind()))
        .collect();
    let ledger = descriptor.eviction_artifact(input.operation);
    let evicted_bytes = input
        .evidence
        .evictions
        .iter()
        .map(|(_, bytes)| *bytes)
        .sum();
    let artifacts = vec![
        ArtifactMeasurement::new_store_owned(
            descriptor.compatibility.identity,
            Some(derive_content_identity(
                descriptor,
                validated.payload_sha256,
            )),
            disposition,
            validated.payload_bytes,
            Some(RedactedPath::from_path(&validated.path)),
        ),
        ArtifactMeasurement::new_store_owned(
            ledger.identity(),
            Some(derive_eviction_observed_identity(
                ledger.identity(),
                &input.evidence.evictions,
            )),
            ArtifactDisposition::Loaded,
            evicted_bytes,
            None,
        ),
    ];
    WorkMeasurements::new(resources, io, artifacts)
}

pub(super) fn catalog_measurements(
    context: WorkExecutionContext<'_>,
    _store: &PreparedArtifactStore,
    descriptors: &[PreparedArtifactDescriptor],
    outcomes: &[ReuseEvaluation],
    cache_bytes: u64,
    evidence: &ValidationEvidence,
) -> WorkMeasurements {
    let first = descriptors
        .first()
        .expect("validated catalog has one descriptor");
    let resources = resource_measurements(
        context,
        first,
        PreparedArtifactOperation::Reuse,
        cache_bytes,
        0,
        evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    let evicted_bytes = evidence.evictions.iter().map(|(_, bytes)| *bytes).sum();
    let mut artifacts = Vec::with_capacity(descriptors.len().saturating_mul(2));
    for (descriptor, outcome) in descriptors.iter().zip(outcomes) {
        match outcome {
            ReuseEvaluation::Reused { validated, .. } => {
                artifacts.push(ArtifactMeasurement::new_store_owned(
                    descriptor.compatibility.identity,
                    Some(derive_content_identity(
                        descriptor,
                        validated.payload_sha256,
                    )),
                    ArtifactDisposition::Reused,
                    validated.payload_bytes,
                    Some(RedactedPath::from_path(&validated.path)),
                ));
            }
            ReuseEvaluation::Rejected {
                rejection, path, ..
            } => {
                artifacts.push(ArtifactMeasurement::new_store_owned(
                    descriptor.compatibility.identity,
                    Some(rejection.evidence_identity(descriptor.compatibility.identity)),
                    ArtifactDisposition::RejectedStale,
                    0,
                    Some(RedactedPath::from_path(path)),
                ));
            }
        }
        let ledger = descriptor.eviction_artifact(PreparedArtifactOperation::Reuse);
        artifacts.push(ArtifactMeasurement::new_store_owned(
            ledger.identity(),
            Some(derive_eviction_observed_identity(
                ledger.identity(),
                &evidence.evictions,
            )),
            ArtifactDisposition::Loaded,
            evicted_bytes,
            None,
        ));
    }
    WorkMeasurements::new(resources, io, artifacts)
}

pub(super) fn failed_catalog_measurements(
    context: WorkExecutionContext<'_>,
    _store: &PreparedArtifactStore,
    descriptors: &[PreparedArtifactDescriptor],
    evidence: &ValidationEvidence,
) -> WorkMeasurements {
    let Some(first) = descriptors.first() else {
        return WorkMeasurements::default();
    };
    let resources = resource_measurements(
        context,
        first,
        PreparedArtifactOperation::Reuse,
        evidence.cache_bytes_peak,
        0,
        evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    WorkMeasurements::new(resources, io, Vec::new())
}

pub(super) fn rejected_measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    rejection: PreparedArtifactRejection,
    path: &Path,
    cache_bytes: u64,
    evidence: ValidationEvidence,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        descriptor,
        PreparedArtifactOperation::Reuse,
        cache_bytes,
        0,
        &evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    let ledger = descriptor.eviction_artifact(PreparedArtifactOperation::Reuse);
    let evicted_bytes = evidence.evictions.iter().map(|(_, bytes)| *bytes).sum();
    let artifacts = vec![
        ArtifactMeasurement::new_store_owned(
            descriptor.compatibility.identity,
            Some(rejection.evidence_identity(descriptor.compatibility.identity)),
            ArtifactDisposition::RejectedStale,
            evidence.inspected_bytes(),
            Some(RedactedPath::from_path(path)),
        ),
        ArtifactMeasurement::new_store_owned(
            ledger.identity(),
            Some(derive_eviction_observed_identity(
                ledger.identity(),
                &evidence.evictions,
            )),
            ArtifactDisposition::Loaded,
            evicted_bytes,
            None,
        ),
    ];
    WorkMeasurements::new(resources, io, artifacts)
}

pub(super) fn failed_measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    evidence: &ValidationEvidence,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        descriptor,
        operation,
        evidence.cache_bytes_peak,
        evidence.cache_write.bytes,
        evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    let ledger = descriptor.eviction_artifact(operation);
    let evicted_bytes = evidence.evictions.iter().map(|(_, bytes)| *bytes).sum();
    let mut artifacts = Vec::with_capacity(2);
    if let Some(materialized) = &evidence.materialized {
        artifacts.push(ArtifactMeasurement::new_store_owned(
            descriptor.compatibility.identity,
            Some(derive_content_identity(
                descriptor,
                materialized.payload_sha256,
            )),
            materialized.disposition,
            materialized.payload_bytes,
            Some(RedactedPath::from_path(&materialized.path)),
        ));
    }
    artifacts.push(ArtifactMeasurement::new_store_owned(
        ledger.identity(),
        Some(derive_eviction_observed_identity(
            ledger.identity(),
            &evidence.evictions,
        )),
        ArtifactDisposition::Loaded,
        evicted_bytes,
        None,
    ));
    WorkMeasurements::new(resources, io, artifacts)
}

pub(super) fn resource_measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
    entry_bytes: u64,
    evidence: &ValidationEvidence,
) -> Vec<ResourceMeasurement> {
    let cache_demand_prefix = format!("private-prepared-cache-{}", descriptor.cache_identity());
    let cache_demand_id = context
        .resources()
        .iter()
        .find_map(|capability| match capability.resource() {
            LeaseResource::Storage {
                demand_id,
                use_kind: StorageUseKind::PersistentCache,
            } if demand_id.starts_with(&cache_demand_prefix) => Some(demand_id.as_str()),
            _ => None,
        })
        .expect("validated prepared operation has one private-cache demand");
    context
        .resources()
        .iter()
        .map(|capability| {
            ResourceMeasurement::new(
                capability.resource().clone(),
                capability.lifetime().clone(),
                observed_resource_peak(
                    capability.resource(),
                    cache_demand_id,
                    operation,
                    cache_bytes,
                    entry_bytes,
                    evidence,
                ),
            )
        })
        .collect()
}

pub(super) fn observed_resource_peak(
    resource: &LeaseResource,
    cache_demand_id: &str,
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
    entry_bytes: u64,
    evidence: &ValidationEvidence,
) -> u64 {
    match resource {
        LeaseResource::Workers => 1,
        LeaseResource::Locks => evidence.locks_peak,
        LeaseResource::FileDescriptors => evidence.file_descriptors_peak,
        LeaseResource::IoBuffer(IoBufferKind::StorageManager) => evidence.resident_buffer_bytes,
        LeaseResource::Storage {
            demand_id,
            use_kind: StorageUseKind::PersistentCache,
        } if demand_id == cache_demand_id => cache_bytes,
        LeaseResource::Storage {
            demand_id,
            use_kind: StorageUseKind::Temporary,
        } if demand_id == cache_demand_id
            && !matches!(
                operation,
                PreparedArtifactOperation::Reuse | PreparedArtifactOperation::Consume
            ) =>
        {
            evidence.temporary_storage_peak.max(entry_bytes)
        }
        LeaseResource::StorageReadRate { demand_id }
            if demand_id == cache_demand_id && evidence.cache_read.bytes > 0 =>
        {
            1
        }
        LeaseResource::StorageReadRate { demand_id }
            if demand_id != cache_demand_id && evidence.source_counter(demand_id).bytes > 0 =>
        {
            1
        }
        LeaseResource::StorageWriteRate { demand_id }
            if demand_id == cache_demand_id && evidence.cache_write.bytes > 0 =>
        {
            1
        }
        LeaseResource::StorageOperationsRate { demand_id }
            if demand_id == cache_demand_id
                && evidence.cache_read.operations
                    + evidence.cache_write.operations
                    + evidence.cache_control.operations
                    > 0 =>
        {
            1
        }
        LeaseResource::StorageOperationsRate { demand_id }
            if demand_id != cache_demand_id
                && evidence.source_counter(demand_id).operations > 0 =>
        {
            1
        }
        LeaseResource::StorageQueue { demand_id }
            if demand_id == cache_demand_id
                && evidence.cache_read.operations
                    + evidence.cache_write.operations
                    + evidence.cache_control.operations
                    > 0 =>
        {
            1
        }
        LeaseResource::StorageQueue { demand_id }
            if demand_id != cache_demand_id
                && evidence.source_counter(demand_id).operations > 0 =>
        {
            1
        }
        _ => 0,
    }
}

pub(super) fn rejection_for(error: &PreparedArtifactError) -> Option<PreparedArtifactRejection> {
    match error {
        PreparedArtifactError::IncompleteArtifact | PreparedArtifactError::UnknownCacheEntry(_) => {
            Some(PreparedArtifactRejection::Incomplete)
        }
        PreparedArtifactError::InvalidOwner
        | PreparedArtifactError::InvalidCellKey
        | PreparedArtifactError::InvalidScientificKey
        | PreparedArtifactError::InvalidDescriptor
        | PreparedArtifactError::InvalidLayout
        | PreparedArtifactError::InvalidUvAffine
        | PreparedArtifactError::ArtifactTooLarge
        | PreparedArtifactError::ImplementationRegistryMismatch
        | PreparedArtifactError::UnplannedSource
        | PreparedArtifactError::SourceProducerMismatch
        | PreparedArtifactError::SourceIdentityMismatch
        | PreparedArtifactError::InvalidSourceMeasurement
        | PreparedArtifactError::UnknownSchema { .. }
        | PreparedArtifactError::InvalidManifest
        | PreparedArtifactError::IdentityMismatch
        | PreparedArtifactError::StaleArtifact
        | PreparedArtifactError::SegmentLayoutMismatch => {
            Some(PreparedArtifactRejection::Incompatible)
        }
        PreparedArtifactError::Json(error) if !error.is_io() => {
            Some(PreparedArtifactRejection::Incompatible)
        }
        PreparedArtifactError::CorruptArtifact | PreparedArtifactError::OversizedArtifact => {
            Some(PreparedArtifactRejection::Corrupt)
        }
        PreparedArtifactError::NonFiniteValue { .. } => Some(PreparedArtifactRejection::NonFinite),
        _ => None,
    }
}
