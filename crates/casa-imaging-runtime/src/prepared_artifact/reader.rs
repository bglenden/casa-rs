// SPDX-License-Identifier: LGPL-3.0-or-later

//! Transaction-bound lazy readers for immutable prepared-artifact catalogs.

use super::*;
use crate::{
    ArtifactRole, ClaimLifetime, FenceKind, IoBufferKind, IoMeasurement, LeaseResource,
    PlannedArtifact, ResourceMeasurement, WorkDomain, WorkExecutionContext, WorkImplementationId,
    WorkKind, WorkMeasurements, WorkNodeId,
};

const READER_CATALOG_DOMAIN: &[u8] = b"casa-rs:prepared-artifact-reader-catalog:v1";

#[derive(Clone)]
struct ReaderEntry {
    descriptor: PreparedArtifactDescriptor,
    integrity_identity: ArtifactIdentity,
    payload_bytes: u64,
}

/// Cloneable, payload-free plan declaration for one lazy prepared catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactReaderPlan {
    catalog_identity: ArtifactIdentity,
    cache_identity: CacheIdentity,
    node: WorkNodeId,
    release_node: WorkNodeId,
    implementation: WorkImplementationId,
    storage_domain: StorageDomainId,
    storage_demand_id: String,
    persistent_cache_bytes: u64,
    decoded_resident_bytes: u64,
    decoder_workspace_bytes: u64,
    store_resident_bytes: u64,
    total_resident_bytes: u64,
    logical_bytes: u64,
}

impl PreparedArtifactReaderPlan {
    /// Identity of the immutable prepared catalog owned by this reader.
    #[must_use]
    pub const fn catalog_identity(&self) -> ArtifactIdentity {
        self.catalog_identity
    }

    /// Cache activation node that owns all payload reads for this plan.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Terminal node that releases the reader's StorageManager allocation.
    #[must_use]
    pub const fn release_node(&self) -> &WorkNodeId {
        &self.release_node
    }

    /// Exact decoded-cell byte ceiling enforced by the application owner.
    #[must_use]
    pub const fn decoded_resident_bytes(&self) -> u64 {
        self.decoded_resident_bytes
    }

    /// Maximum concurrent encoded-decoder workspace admitted beside the LRU.
    #[must_use]
    pub const fn decoder_workspace_bytes(&self) -> u64 {
        self.decoder_workspace_bytes
    }

    /// Complete decoded-pool plus T50 streaming/validation residency claim.
    #[must_use]
    pub const fn total_resident_bytes(&self) -> u64 {
        self.total_resident_bytes
    }

    /// Sum of immutable logical payload bytes represented by the catalog.
    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    pub(crate) const fn implementation(&self) -> &WorkImplementationId {
        &self.implementation
    }

    pub(crate) const fn storage_domain(&self) -> &StorageDomainId {
        &self.storage_domain
    }

    pub(crate) fn storage_demand_id(&self) -> &str {
        &self.storage_demand_id
    }

    pub(crate) const fn persistent_cache_bytes(&self) -> u64 {
        self.persistent_cache_bytes
    }

    pub(crate) fn planned_artifact(&self) -> PlannedArtifact {
        PlannedArtifact::new(
            self.catalog_identity,
            self.node.clone(),
            ArtifactRole::Cache,
            Some(self.cache_identity),
        )
    }
}

/// Non-cloneable authority which can mint one fresh inactive reader per plan.
pub struct PreparedArtifactReaderFactory {
    store: Arc<PreparedArtifactStore>,
    entries: Arc<[ReaderEntry]>,
    plan: PreparedArtifactReaderPlan,
}

impl fmt::Debug for PreparedArtifactReaderFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedArtifactReaderFactory")
            .field("plan", &self.plan)
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl PreparedArtifactReaderFactory {
    /// Seal opaque T50 handles into one canonical lazy-reader catalog.
    pub fn new(
        store: Arc<PreparedArtifactStore>,
        mut artifacts: Vec<(PreparedArtifactDescriptor, PreparedArtifact)>,
        implementation: WorkImplementationId,
        decoded_resident_bytes: u64,
        decoder_workspace_bytes: u64,
    ) -> Result<Self, PreparedArtifactError> {
        if artifacts.is_empty() || decoded_resident_bytes == 0 || decoder_workspace_bytes == 0 {
            return Err(PreparedArtifactError::InvalidDescriptor);
        }
        artifacts.sort_unstable_by_key(|(descriptor, _)| descriptor.identity());
        let mut entries = Vec::with_capacity(artifacts.len());
        let mut logical_bytes = 0_u64;
        let mut store_resident_bytes = 0_u64;
        let mut cache_identity = None;
        let mut storage_demand_id = None;
        let mut hasher = Sha256::new();
        hasher.update(READER_CATALOG_DOMAIN);
        hasher.update(IDENTITY_VERSION.to_le_bytes());
        for (descriptor, artifact) in artifacts {
            if descriptor.cache_scope != store.scope
                || descriptor.owner.registration.implementation() != &implementation
                || artifact.identity != descriptor.identity()
                || artifact.cache_identity != descriptor.cache_identity()
            {
                return Err(PreparedArtifactError::IdentityMismatch);
            }
            if entries.last().is_some_and(|entry: &ReaderEntry| {
                entry.descriptor.identity() == descriptor.identity()
            }) {
                return Err(PreparedArtifactError::InvalidDescriptor);
            }
            match cache_identity {
                Some(identity) if identity != descriptor.cache_identity() => {
                    return Err(PreparedArtifactError::CachePolicyMismatch);
                }
                None => cache_identity = Some(descriptor.cache_identity()),
                Some(_) => {}
            }
            let descriptor_storage_demand_id = store.storage_demand_id(&descriptor);
            match &storage_demand_id {
                Some(id) if id != &descriptor_storage_demand_id => {
                    return Err(PreparedArtifactError::CachePolicyMismatch);
                }
                None => storage_demand_id = Some(descriptor_storage_demand_id),
                Some(_) => {}
            }
            let payload_bytes = descriptor.payload_bytes()?;
            logical_bytes = logical_bytes
                .checked_add(payload_bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            store_resident_bytes = store_resident_bytes.max(
                store
                    .reservation(&descriptor, PreparedArtifactOperation::Consume)?
                    .resident_buffer_bytes(),
            );
            hasher.update(descriptor.identity().as_bytes());
            hasher.update(artifact.integrity_identity.as_bytes());
            hasher.update(payload_bytes.to_le_bytes());
            entries.push(ReaderEntry {
                descriptor,
                integrity_identity: artifact.integrity_identity,
                payload_bytes,
            });
        }
        let cache_identity = cache_identity.ok_or(PreparedArtifactError::InvalidDescriptor)?;
        let storage_demand_id =
            storage_demand_id.ok_or(PreparedArtifactError::InvalidDescriptor)?;
        let persistent_cache_bytes = store.budget().cache_bytes();
        hasher.update(cache_identity.as_bytes());
        hasher.update(decoded_resident_bytes.to_le_bytes());
        hasher.update(decoder_workspace_bytes.to_le_bytes());
        let catalog_identity = ArtifactIdentity::from_owner_digest(hasher.finalize().into());
        let total_resident_bytes = decoded_resident_bytes
            .checked_add(decoder_workspace_bytes)
            .and_then(|bytes| bytes.checked_add(store_resident_bytes))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        let suffix = catalog_identity.to_string();
        let plan = PreparedArtifactReaderPlan {
            catalog_identity,
            cache_identity,
            node: WorkNodeId::new(format!("prepared-artifact-reader-{suffix}")),
            release_node: WorkNodeId::new(format!("prepared-artifact-reader-release-{suffix}")),
            implementation,
            storage_domain: store.storage_domain.clone(),
            storage_demand_id,
            persistent_cache_bytes,
            decoded_resident_bytes,
            decoder_workspace_bytes,
            store_resident_bytes,
            total_resident_bytes,
            logical_bytes,
        };
        Ok(Self {
            store,
            entries: entries.into(),
            plan,
        })
    }

    /// Return the payload-free declaration cloned into physical plans.
    #[must_use]
    pub const fn plan(&self) -> &PreparedArtifactReaderPlan {
        &self.plan
    }

    /// Mint a fresh inactive transaction reader for one execution attempt.
    #[must_use]
    pub fn session(&self) -> Arc<PreparedArtifactReader> {
        Arc::new(PreparedArtifactReader {
            store: Arc::clone(&self.store),
            entries: Arc::clone(&self.entries),
            plan: self.plan.clone(),
            state: Mutex::new(ReaderState::default()),
            settled: std::sync::Condvar::new(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReaderBinding {
    attempt: crate::ExecutionAttemptId,
    registry: crate::ImplementationRegistryId,
    lease_epoch: u64,
}

#[derive(Default)]
struct ReaderState {
    binding: Option<ReaderBinding>,
    active_reads: usize,
    read_bytes: u64,
    read_operations: u64,
    reader_resident_peak: u64,
    cache_bytes_peak: u64,
    locks_peak: u64,
    file_descriptors_peak: u64,
    read_rate_peak: u64,
    write_rate_peak: u64,
    operations_rate_peak: u64,
    queue_slots_peak: u64,
    read_count: u64,
    closed: bool,
    aborted: bool,
    released: bool,
    fence_emitted: bool,
}

/// One attempt-local lazy reader. Payload access is unavailable before its
/// exact Cache node activates and after its I/O fence closes.
pub struct PreparedArtifactReader {
    store: Arc<PreparedArtifactStore>,
    entries: Arc<[ReaderEntry]>,
    plan: PreparedArtifactReaderPlan,
    state: Mutex<ReaderState>,
    settled: std::sync::Condvar,
}

impl fmt::Debug for PreparedArtifactReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedArtifactReader")
            .field("plan", &self.plan)
            .finish_non_exhaustive()
    }
}

impl PreparedArtifactReader {
    /// Borrow the immutable physical-plan declaration for this session.
    #[must_use]
    pub const fn plan(&self) -> &PreparedArtifactReaderPlan {
        &self.plan
    }

    pub(crate) fn activate(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.validate_cache_context(context)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        if state.binding.is_some() || state.closed || state.released {
            return Err(PreparedArtifactError::ReaderAlreadyActivated);
        }
        state.binding = Some(ReaderBinding {
            attempt: context.attempt_id(),
            registry: context.implementation_registry_id(),
            lease_epoch: context.lease_epoch(),
        });
        let worker = context
            .resources()
            .iter()
            .find(|capability| capability.resource() == &LeaseResource::Workers)
            .ok_or(PreparedArtifactError::MissingReservation(
                "reader activation worker",
            ))?;
        Ok(WorkMeasurements::new(
            vec![ResourceMeasurement::new(
                worker.resource().clone(),
                worker.lifetime().clone(),
                1,
            )],
            vec![],
            vec![],
        ))
    }

    /// Stream one exact artifact through T50 validation into a caller-owned decoder.
    pub fn read(
        &self,
        identity: ArtifactIdentity,
        consumer: &mut dyn PreparedArtifactConsumer,
    ) -> Result<(), PreparedArtifactError> {
        let entry = self
            .entries
            .binary_search_by_key(&identity, |entry| entry.descriptor.identity())
            .ok()
            .map(|index| self.entries[index].clone())
            .ok_or(PreparedArtifactError::ReaderArtifactMissing)?;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| PreparedArtifactError::PoisonedStore)?;
            if state.binding.is_none() {
                return Err(PreparedArtifactError::ReaderInactive);
            }
            if state.closed || state.aborted || state.released {
                return Err(PreparedArtifactError::ReaderClosed);
            }
            state.active_reads = state
                .active_reads
                .checked_add(1)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        let artifact = PreparedArtifact {
            identity: entry.descriptor.identity(),
            integrity_identity: entry.integrity_identity,
            cache_identity: entry.descriptor.cache_identity(),
        };
        let read = self
            .store
            .consume_for_reader(&entry.descriptor, &artifact, consumer);
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        state.active_reads = state
            .active_reads
            .checked_sub(1)
            .ok_or(PreparedArtifactError::InvalidDescriptor)?;
        if let Ok(read) = &read {
            if read.content_identity != entry.integrity_identity
                || read.payload_bytes != entry.payload_bytes
            {
                state.aborted = true;
                self.settled.notify_all();
                return Err(PreparedArtifactError::IdentityMismatch);
            }
            let Some(read_bytes) = state.read_bytes.checked_add(read.read_bytes) else {
                state.aborted = true;
                self.settled.notify_all();
                return Err(PreparedArtifactError::ArtifactTooLarge);
            };
            let Some(read_operations) = state.read_operations.checked_add(read.read_operations)
            else {
                state.aborted = true;
                self.settled.notify_all();
                return Err(PreparedArtifactError::ArtifactTooLarge);
            };
            let Some(read_count) = state.read_count.checked_add(1) else {
                state.aborted = true;
                self.settled.notify_all();
                return Err(PreparedArtifactError::ArtifactTooLarge);
            };
            state.read_bytes = read_bytes;
            state.read_operations = read_operations;
            state.reader_resident_peak = state.reader_resident_peak.max(read.resident_buffer_bytes);
            state.cache_bytes_peak = state.cache_bytes_peak.max(read.cache_bytes);
            state.locks_peak = state.locks_peak.max(read.locks);
            state.file_descriptors_peak = state.file_descriptors_peak.max(read.file_descriptors);
            state.read_rate_peak = state.read_rate_peak.max(read.read_rate);
            state.write_rate_peak = state.write_rate_peak.max(read.write_rate);
            state.operations_rate_peak = state.operations_rate_peak.max(read.operations_rate);
            state.queue_slots_peak = state.queue_slots_peak.max(read.queue_slots);
            state.read_count = read_count;
        }
        self.settled.notify_all();
        read.map(|_| ())
    }

    fn close(
        &self,
        context: WorkExecutionContext<'_>,
        residency: PreparedArtifactResidencyMeasurements,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.validate_close_context(context)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        let binding = state.binding.ok_or(PreparedArtifactError::ReaderInactive)?;
        if binding.attempt != context.attempt_id()
            || binding.registry != context.implementation_registry_id()
            || binding.lease_epoch != context.lease_epoch()
        {
            return Err(PreparedArtifactError::ReaderBindingMismatch);
        }
        while state.active_reads != 0 {
            state = self
                .settled
                .wait(state)
                .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        }
        if state.fence_emitted {
            return Err(PreparedArtifactError::ReaderClosed);
        }
        state.closed = true;
        state.fence_emitted = true;
        if residency.peak_resident_bytes > self.plan.decoded_resident_bytes {
            return Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: residency.peak_resident_bytes,
                budget: self.plan.decoded_resident_bytes,
            });
        }
        if residency.peak_decoder_workspace_bytes > self.plan.decoder_workspace_bytes {
            return Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: residency.peak_decoder_workspace_bytes,
                budget: self.plan.decoder_workspace_bytes,
            });
        }
        let combined_resident = residency
            .peak_resident_bytes
            .checked_add(residency.peak_decoder_workspace_bytes)
            .and_then(|bytes| bytes.checked_add(state.reader_resident_peak))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        if combined_resident > self.plan.total_resident_bytes {
            return Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: combined_resident,
                budget: self.plan.total_resident_bytes,
            });
        }
        if residency.peak_pinned_bytes > self.plan.decoded_resident_bytes {
            return Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: residency.peak_pinned_bytes,
                budget: self.plan.decoded_resident_bytes,
            });
        }
        let resources = context
            .resources()
            .iter()
            .map(|capability| {
                let peak = match capability.resource() {
                    LeaseResource::IoBuffer(IoBufferKind::StorageManager) => combined_resident,
                    LeaseResource::Locks => state.locks_peak,
                    LeaseResource::FileDescriptors => state.file_descriptors_peak,
                    LeaseResource::Storage {
                        demand_id,
                        use_kind: StorageUseKind::PersistentCache,
                    } if demand_id == &self.plan.storage_demand_id => state.cache_bytes_peak,
                    LeaseResource::StorageReadRate { demand_id }
                        if demand_id == &self.plan.storage_demand_id =>
                    {
                        state.read_rate_peak
                    }
                    LeaseResource::StorageWriteRate { demand_id }
                        if demand_id == &self.plan.storage_demand_id =>
                    {
                        state.write_rate_peak
                    }
                    LeaseResource::StorageOperationsRate { demand_id }
                        if demand_id == &self.plan.storage_demand_id =>
                    {
                        state.operations_rate_peak
                    }
                    LeaseResource::StorageQueue { demand_id }
                        if demand_id == &self.plan.storage_demand_id =>
                    {
                        state.queue_slots_peak
                    }
                    _ => capability.amount(),
                };
                ResourceMeasurement::new(
                    capability.resource().clone(),
                    capability.lifetime().clone(),
                    peak,
                )
            })
            .collect();
        let artifact = ArtifactMeasurement::new_store_owned(
            self.plan.catalog_identity,
            Some(self.plan.catalog_identity),
            ArtifactDisposition::Reused,
            self.plan.logical_bytes,
            None,
        );
        eprintln!(
            "imaging_prepared_artifact_reader_summary catalog={} logical_bytes={} decoded_ceiling_bytes={} decoder_workspace_ceiling_bytes={} total_ceiling_bytes={} reads={} read_bytes={} read_operations={} resident_peak_bytes={} decoder_workspace_peak_bytes={} total_peak_resident_bytes={} pinned_peak_bytes={} hits={} loads={} evicted_bytes={} copied_bytes={} aborted={}",
            self.plan.catalog_identity,
            self.plan.logical_bytes,
            self.plan.decoded_resident_bytes,
            self.plan.decoder_workspace_bytes,
            self.plan.total_resident_bytes,
            state.read_count,
            state.read_bytes,
            state.read_operations,
            residency.peak_resident_bytes,
            residency.peak_decoder_workspace_bytes,
            combined_resident,
            residency.peak_pinned_bytes,
            residency.hits,
            residency.loads,
            residency.evicted_bytes,
            residency.copied_bytes,
            state.aborted,
        );
        Ok(WorkMeasurements::new(
            resources,
            vec![IoMeasurement::new(
                IoBufferKind::StorageManager,
                state.read_bytes,
                state.read_operations,
            )],
            vec![artifact],
        ))
    }

    fn validate_close_context(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), PreparedArtifactError> {
        if context.node().id != self.plan.node || context.node().kind != WorkKind::Cache {
            return Err(PreparedArtifactError::ReaderBindingMismatch);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        let binding = state.binding.ok_or(PreparedArtifactError::ReaderInactive)?;
        if binding.attempt != context.attempt_id()
            || binding.registry != context.implementation_registry_id()
            || binding.lease_epoch != context.lease_epoch()
        {
            return Err(PreparedArtifactError::ReaderBindingMismatch);
        }
        if state.fence_emitted {
            return Err(PreparedArtifactError::ReaderClosed);
        }
        Ok(())
    }

    fn release(
        &self,
        context: WorkExecutionContext<'_>,
        released_bytes: u64,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.validate_release_context(context)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        state.closed = true;
        state.released = true;
        let resources = context
            .resources()
            .iter()
            .map(|capability| {
                let peak = if capability.resource()
                    == &LeaseResource::IoBuffer(IoBufferKind::StorageManager)
                {
                    released_bytes
                } else {
                    capability.amount()
                };
                ResourceMeasurement::new(
                    capability.resource().clone(),
                    capability.lifetime().clone(),
                    peak,
                )
            })
            .collect();
        Ok(WorkMeasurements::new(
            resources,
            vec![IoMeasurement::new(IoBufferKind::StorageManager, 0, 0)],
            vec![],
        ))
    }

    fn validate_release_context(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), PreparedArtifactError> {
        if context.node().id != self.plan.release_node || context.node().kind != WorkKind::Release {
            return Err(PreparedArtifactError::ReaderBindingMismatch);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        if state.released {
            return Err(PreparedArtifactError::ReaderClosed);
        }
        if state.binding.is_some_and(|binding| {
            binding.attempt != context.attempt_id()
                || binding.registry != context.implementation_registry_id()
                || binding.lease_epoch != context.lease_epoch()
        }) {
            return Err(PreparedArtifactError::ReaderBindingMismatch);
        }
        if !state.fence_emitted && !context.is_cleanup() {
            return Err(PreparedArtifactError::ReaderInactive);
        }
        Ok(())
    }

    fn abort(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.aborted = true;
            self.settled.notify_all();
        }
    }

    fn validate_cache_context(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), PreparedArtifactError> {
        let node = context.node();
        if node.id != self.plan.node
            || node.kind != WorkKind::Cache
            || node.domain != WorkDomain::Io
            || node.implementation != self.plan.implementation
            || node.fences != BTreeSet::from([FenceKind::Io])
            || context.implementation_registry_id()
                != self.entries[0].descriptor.owner.implementation_registry
            || self
                .entries
                .iter()
                .any(|entry| !entry.descriptor.scientific.matches_context(context))
        {
            return Err(PreparedArtifactError::ReaderBindingMismatch);
        }
        let planned = context.planned_artifacts().collect::<Vec<_>>();
        if planned.len() != 1 || planned[0] != &self.plan.planned_artifact() {
            return Err(PreparedArtifactError::UnplannedOperation);
        }
        let work = node
            .claims
            .iter()
            .filter(|claim| {
                claim.resource == LeaseResource::Workers
                    && claim.lifetime == ClaimLifetime::Work
                    && claim.amount == 1
            })
            .count();
        let retained = ClaimLifetime::through_fence(FenceKind::Io);
        let required = [
            LeaseResource::Locks,
            LeaseResource::Storage {
                demand_id: self.plan.storage_demand_id.clone(),
                use_kind: StorageUseKind::PersistentCache,
            },
            LeaseResource::StorageReadRate {
                demand_id: self.plan.storage_demand_id.clone(),
            },
            LeaseResource::StorageWriteRate {
                demand_id: self.plan.storage_demand_id.clone(),
            },
            LeaseResource::StorageOperationsRate {
                demand_id: self.plan.storage_demand_id.clone(),
            },
            LeaseResource::StorageQueue {
                demand_id: self.plan.storage_demand_id.clone(),
            },
            LeaseResource::IoBuffer(IoBufferKind::StorageManager),
            LeaseResource::FileDescriptors,
        ];
        if work != 1
            || node.claims.len() != 9
            || required.iter().any(|resource| {
                node.claims
                    .iter()
                    .filter(|claim| {
                        &claim.resource == resource
                            && claim.lifetime == retained
                            && claim.amount
                                == match resource {
                                    LeaseResource::IoBuffer(_) => self.plan.total_resident_bytes,
                                    LeaseResource::FileDescriptors => 2,
                                    LeaseResource::Storage {
                                        use_kind: StorageUseKind::PersistentCache,
                                        ..
                                    } => self.plan.persistent_cache_bytes,
                                    _ => 1,
                                }
                    })
                    .count()
                    != 1
            })
        {
            return Err(PreparedArtifactError::MissingReservation("reader claims"));
        }
        let matching_storage = context
            .resource_alternative()
            .demand
            .storage
            .iter()
            .filter(|demand| demand.demand_id == self.plan.storage_demand_id)
            .collect::<Vec<_>>();
        if matching_storage.len() != 1
            || matching_storage[0].domain != self.plan.storage_domain
            || matching_storage[0].persistent_cache_bytes < self.plan.persistent_cache_bytes
            || matching_storage[0].read_rate.hard() == 0
            || matching_storage[0].write_rate.hard() == 0
            || matching_storage[0].operations_rate.hard() == 0
            || matching_storage[0].queue_slots.hard() == 0
            || context.resource_alternative().demand.locks.hard() == 0
            || context
                .resource_alternative()
                .demand
                .file_descriptors
                .hard()
                < 2
            || context
                .resource_alternative()
                .demand
                .io_buffers
                .storage_manager_bytes
                < self.plan.total_resident_bytes
        {
            return Err(PreparedArtifactError::MissingReservation(
                "reader resource demand",
            ));
        }
        if context.allocations().len() != 1
            || context.allocations()[0].capacity_bytes() != self.plan.total_resident_bytes
        {
            return Err(PreparedArtifactError::MissingReservation(
                "reader StorageManager allocation",
            ));
        }
        let io = context.stage_prediction().io();
        if io.len() != 1 || io[0].kind() != IoBufferKind::StorageManager {
            return Err(PreparedArtifactError::MissingReservation(
                "reader I/O prediction",
            ));
        }
        Ok(())
    }
}

/// Exact decoded-pool evidence supplied by the application-owned LRU.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreparedArtifactResidencyMeasurements {
    /// Peak decoded bytes resident in the LRU, including pinned cells.
    pub peak_resident_bytes: u64,
    /// Peak concurrent encoded-decoder workspace beside retained cells.
    pub peak_decoder_workspace_bytes: u64,
    /// Peak decoded bytes protected from eviction by live operator leases.
    pub peak_pinned_bytes: u64,
    /// Exact resident-hit count.
    pub hits: u64,
    /// Exact decode/load count.
    pub loads: u64,
    /// Exact decoded bytes evicted.
    pub evicted_bytes: u64,
    /// Exact decoded bytes copied from validated payloads.
    pub copied_bytes: u64,
    /// Bytes resident immediately before terminal release.
    pub released_bytes: u64,
}

/// Application-owned decoded residency coupled to one reader session.
pub trait PreparedArtifactReaderResidency: Send + Sync + 'static {
    /// Close new loads and prove all in-flight loads and pins have settled.
    fn close(&self) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError>;

    /// Drop every decoded cell at the scheduler-owned Release node.
    fn release(&self) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError>;

    /// Cancel loads, discard decoded state, and wake all waiters.
    fn abort(&self);
}

/// Non-cloneable execution capability shared only by one plan's executor and
/// its application-owned decoded-cell provider.
pub struct PreparedArtifactExecutionBinding {
    reader: Arc<PreparedArtifactReader>,
    residency: Arc<dyn PreparedArtifactReaderResidency>,
}

impl fmt::Debug for PreparedArtifactExecutionBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedArtifactExecutionBinding")
            .field("plan", self.reader.plan())
            .finish_non_exhaustive()
    }
}

impl PreparedArtifactExecutionBinding {
    /// Couple one fresh reader to the sole decoded-residency owner for its plan.
    #[must_use]
    pub fn new<R>(reader: Arc<PreparedArtifactReader>, residency: R) -> Self
    where
        R: PreparedArtifactReaderResidency,
    {
        Self {
            reader,
            residency: Arc::new(residency),
        }
    }

    /// Borrow the exact payload-free plan declaration.
    #[must_use]
    pub fn plan(&self) -> &PreparedArtifactReaderPlan {
        self.reader.plan()
    }

    pub(crate) fn activate(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.reader.activate(context)
    }

    pub(crate) fn close(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.reader.validate_close_context(context)?;
        let residency = self.residency.close()?;
        self.reader.close(context, residency)
    }

    pub(crate) fn release(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.reader.validate_release_context(context)?;
        let residency = self.residency.release()?;
        self.reader.release(context, residency.released_bytes)
    }

    pub(crate) fn abort(&self) {
        self.residency.abort();
        self.reader.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_bindings::WorkExecutionTestBindings;
    use crate::{
        AllocationId, AllocationUse, AlternativeId, CacheDemand, CapabilityPredicate,
        CapacityViewId, CountDemand, DemandAlternative, DemandEnvelope, ExecutionAttemptId,
        ExecutionKnobs, IoBufferDemand, IoPrediction, PhysicalSlotId, ResourceClaim,
        ResourceHeadroom, RuntimeOverheadDemand, ScalingMetadata, StagePrediction, StorageDemand,
        StorageDomain, WorkNode,
    };
    use casa_imaging_model::{
        PreparedArtifactKernelAlgorithm, PreparedArtifactKernelSemantics,
        PreparedArtifactScientificIdentity,
    };
    use std::sync::Arc;

    const PAYLOAD: &[u8] = b"prepared-reader-dummy-science";
    const DECODED_CEILING: u64 = 64;
    const DECODER_WORKSPACE_CEILING: u64 = 32;

    struct ReaderFixture {
        _directory: tempfile::TempDir,
        problem: CompiledProblem,
        registry: crate::ImplementationRegistryId,
        factory: PreparedArtifactReaderFactory,
        artifact_identity: ArtifactIdentity,
    }

    impl ReaderFixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().expect("reader private cache root");
            let storage = StorageDomain {
                id: StorageDomainId::new("reader-test-storage"),
                root: directory.path().to_path_buf(),
                capacity_bytes: 1 << 20,
                read_rate: crate::RateResourceId::new("reader-test-read-rate"),
                write_rate: crate::RateResourceId::new("reader-test-write-rate"),
                operations_rate: None,
                queue: crate::QueueResourceId::new("reader-test-queue"),
            };
            let budget =
                PreparedArtifactBudget::new(1 << 20, 4, 8).expect("bounded reader cache budget");
            let store = Arc::new(
                PreparedArtifactStore::open(directory.path().join("private"), &storage, budget)
                    .expect("reader private store"),
            );
            let problem = crate::execution::tests::compiled_problem();
            let registry = crate::ImplementationRegistryId::from_sha256([0x51; 32]);
            let implementation = WorkImplementationId::new("reader-test-implementation");
            let registration = PreparedArtifactRegistration::new(
                "reader-test-catalog",
                "reader-test-provider",
                "v1",
                implementation.clone(),
            )
            .expect("reader test registration");
            let owner = PreparedArtifactOwner::from_manifest(registry, registration);
            let scientific_identity = PreparedArtifactScientificIdentity::kernel(
                PreparedArtifactKernelSemantics::new(
                    PreparedArtifactKernelAlgorithm::Gridding,
                    vec![PAYLOAD.len() as u64],
                    vec![PAYLOAD.len() as u64],
                )
                .expect("reader test kernel semantics"),
            )
            .expect("reader test scientific identity");
            let scientific = ScientificCommitments::from_problem(&problem, scientific_identity);
            let segment = PreparedArtifactSegmentDescriptor::new(
                "science",
                vec![PAYLOAD.len() as u64],
                vec![0],
                vec![1],
                None,
                PreparedArtifactPrecision::U8,
                PreparedArtifactOrder::Axis0ContiguousLittleEndian,
            )
            .expect("reader test segment");
            let descriptor = PreparedArtifactDescriptor::from_commitments(
                owner,
                PreparedArtifactKind::Kernel,
                scientific,
                store.scope.clone(),
                vec![segment.clone()],
            )
            .expect("reader test descriptor");
            let payload_sha256: [u8; 32] = Sha256::digest(PAYLOAD).into();
            let manifest = ArtifactManifest {
                schema: CACHE_SCHEMA.to_string(),
                schema_version: CACHE_SCHEMA_VERSION,
                identity: descriptor.identity().to_string(),
                cache_identity: descriptor.cache_identity().to_string(),
                descriptor: ManifestDescriptor::from_descriptor(&descriptor),
                payload_sha256: encode_hex(&payload_sha256),
                payload_bytes: PAYLOAD.len() as u64,
                segments: vec![ManifestSegment {
                    descriptor: segment,
                    offset: 0,
                    bytes: PAYLOAD.len() as u64,
                    sha256: encode_hex(&payload_sha256),
                }],
            };
            let entry = store.entry_path(descriptor.identity());
            fs::create_dir(&entry).expect("reader test cache entry");
            fs::write(entry.join(PAYLOAD_FILE), PAYLOAD).expect("reader test payload");
            let mut encoded = serde_json::to_vec(&manifest).expect("reader test manifest");
            encoded.push(b'\n');
            fs::write(entry.join(MANIFEST_FILE), encoded).expect("reader test manifest file");
            let artifact = PreparedArtifact {
                identity: descriptor.identity(),
                integrity_identity: derive_content_identity(&descriptor, payload_sha256),
                cache_identity: descriptor.cache_identity(),
            };
            let artifact_identity = descriptor.identity();
            let factory = PreparedArtifactReaderFactory::new(
                Arc::clone(&store),
                vec![(descriptor, artifact)],
                implementation,
                DECODED_CEILING,
                DECODER_WORKSPACE_CEILING,
            )
            .expect("reader test factory");
            Self {
                _directory: directory,
                problem,
                registry,
                factory,
                artifact_identity,
            }
        }
    }

    #[test]
    fn reader_reserves_the_full_store_ceiling_for_differently_sized_catalog_entries() {
        let fixture = ReaderFixture::new();
        let first = &fixture.factory.entries[0];
        let first_descriptor = first.descriptor.clone();
        let first_artifact = PreparedArtifact {
            identity: first_descriptor.identity(),
            integrity_identity: first.integrity_identity,
            cache_identity: first_descriptor.cache_identity(),
        };
        let larger_bytes = PAYLOAD.len() as u64 + 7;
        let larger_segment = PreparedArtifactSegmentDescriptor::new(
            "science",
            vec![larger_bytes],
            vec![0],
            vec![1],
            None,
            PreparedArtifactPrecision::U8,
            PreparedArtifactOrder::Axis0ContiguousLittleEndian,
        )
        .expect("larger reader test segment");
        let larger_scientific_identity = PreparedArtifactScientificIdentity::kernel(
            PreparedArtifactKernelSemantics::new(
                PreparedArtifactKernelAlgorithm::Gridding,
                vec![larger_bytes],
                vec![larger_bytes],
            )
            .expect("larger reader kernel semantics"),
        )
        .expect("larger reader scientific identity");
        let larger_descriptor = PreparedArtifactDescriptor::from_commitments(
            first_descriptor.owner.clone(),
            first_descriptor.kind,
            ScientificCommitments::from_problem(&fixture.problem, larger_scientific_identity),
            first_descriptor.cache_scope.clone(),
            vec![larger_segment],
        )
        .expect("larger reader test descriptor");
        let larger_artifact = PreparedArtifact {
            identity: larger_descriptor.identity(),
            integrity_identity: derive_content_identity(&larger_descriptor, [0x52; 32]),
            cache_identity: larger_descriptor.cache_identity(),
        };
        assert_ne!(
            fixture
                .factory
                .store
                .reservation(&first_descriptor, PreparedArtifactOperation::Consume)
                .expect("first reservation")
                .entry_bytes(),
            fixture
                .factory
                .store
                .reservation(&larger_descriptor, PreparedArtifactOperation::Consume)
                .expect("larger reservation")
                .entry_bytes()
        );

        let factory = PreparedArtifactReaderFactory::new(
            Arc::clone(&fixture.factory.store),
            vec![
                (first_descriptor, first_artifact),
                (larger_descriptor, larger_artifact),
            ],
            fixture.factory.plan.implementation.clone(),
            DECODED_CEILING,
            DECODER_WORKSPACE_CEILING,
        )
        .expect("variable-entry reader factory");
        let plan = factory.plan();
        assert_eq!(plan.logical_bytes(), PAYLOAD.len() as u64 + larger_bytes);
        assert_eq!(
            plan.persistent_cache_bytes(),
            fixture.factory.store.budget().cache_bytes(),
            "persistent storage is the full private-store ceiling, not one entry"
        );
        assert_eq!(
            cache_node(plan)
                .claims
                .iter()
                .find_map(|claim| match &claim.resource {
                    LeaseResource::Storage {
                        use_kind: StorageUseKind::PersistentCache,
                        ..
                    } => Some(claim.amount),
                    _ => None,
                }),
            Some(fixture.factory.store.budget().cache_bytes())
        );
    }

    fn allocation_id() -> AllocationId {
        AllocationId::new("reader-test-allocation")
    }

    fn cache_node(plan: &PreparedArtifactReaderPlan) -> WorkNode {
        let retained = ClaimLifetime::through_fence(FenceKind::Io);
        WorkNode {
            id: plan.node().clone(),
            kind: WorkKind::Cache,
            domain: WorkDomain::Io,
            implementation: plan.implementation().clone(),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: LeaseResource::Locks,
                    amount: 1,
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::FileDescriptors,
                    amount: 2,
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: plan.storage_demand_id().to_string(),
                        use_kind: StorageUseKind::PersistentCache,
                    },
                    amount: plan.persistent_cache_bytes(),
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::StorageManager),
                    amount: plan.total_resident_bytes(),
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::StorageReadRate {
                        demand_id: plan.storage_demand_id().to_string(),
                    },
                    amount: 1,
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::StorageWriteRate {
                        demand_id: plan.storage_demand_id().to_string(),
                    },
                    amount: 1,
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::StorageOperationsRate {
                        demand_id: plan.storage_demand_id().to_string(),
                    },
                    amount: 1,
                    lifetime: retained.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::StorageQueue {
                        demand_id: plan.storage_demand_id().to_string(),
                    },
                    amount: 1,
                    lifetime: retained.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: allocation_id(),
                lifetime: retained,
            }],
            fences: BTreeSet::from([FenceKind::Io]),
            quiescence_after: BTreeSet::new(),
        }
    }

    fn release_node(plan: &PreparedArtifactReaderPlan) -> WorkNode {
        WorkNode {
            id: plan.release_node().clone(),
            kind: WorkKind::Release,
            domain: WorkDomain::Cpu,
            implementation: plan.implementation().clone(),
            dependencies: BTreeSet::from([WorkDependency::Fence(crate::FenceId::new(
                plan.node().clone(),
                FenceKind::Io,
            ))]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::StorageManager),
                    amount: plan.total_resident_bytes(),
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: vec![AllocationUse {
                allocation: allocation_id(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        }
    }

    fn scheduled(
        node: WorkNode,
        plan: &PreparedArtifactReaderPlan,
        lease_epoch: u64,
        cleanup: bool,
    ) -> crate::execution::WorkExecutionContext {
        let lifetime = node.allocations[0].lifetime.clone();
        let allocation = crate::execution::WorkExecutionContext::test_allocation_capability(
            allocation_id(),
            PhysicalSlotId::new("reader-test-slot"),
            plan.total_resident_bytes(),
            lifetime,
        );
        crate::execution::WorkExecutionContext::for_test(
            node,
            ExecutionKnobs::serial(),
            lease_epoch,
            cleanup,
            vec![allocation],
        )
    }

    fn alternative(plan: &PreparedArtifactReaderPlan) -> DemandAlternative {
        DemandAlternative {
            id: AlternativeId::new("reader-test-alternative"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("reader-test-host"),
                memory: Vec::new(),
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: vec![StorageDemand {
                    demand_id: plan.storage_demand_id().to_string(),
                    domain: plan.storage_domain().clone(),
                    temporary_bytes: 0,
                    staged_output_bytes: 0,
                    final_output_bytes: 0,
                    persistent_cache_bytes: plan.persistent_cache_bytes(),
                    read_rate: CountDemand::new(1, 1),
                    write_rate: CountDemand::new(1, 1),
                    operations_rate: CountDemand::new(1, 1),
                    queue_slots: CountDemand::new(1, 1),
                }],
                rates: Vec::new(),
                caches: CacheDemand::zero(),
                locks: CountDemand::new(1, 1),
                file_descriptors: CountDemand::new(2, 2),
                queues: Vec::new(),
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand {
                    storage_manager_bytes: plan.total_resident_bytes(),
                    ..IoBufferDemand::zero()
                },
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::new(),
        }
    }

    fn context<'a>(
        fixture: &'a ReaderFixture,
        scheduled: &'a crate::execution::WorkExecutionContext,
        planned: &'a [PlannedArtifact],
        prediction: &'a StagePrediction,
        alternative: &'a DemandAlternative,
        completed: &'a BTreeMap<WorkNodeId, crate::AttemptBoundObservationCompletion>,
        attempt: u8,
    ) -> WorkExecutionContext<'a> {
        WorkExecutionContext::for_test(
            ExecutionAttemptId::from_sha256([attempt; 32]),
            WorkExecutionTestBindings::new(&fixture.problem, fixture.registry, completed),
            scheduled,
            planned,
            prediction,
            alternative,
        )
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ResidencyEvent {
        Close,
        Release,
        Abort,
    }

    #[derive(Clone)]
    struct RecordingResidency {
        events: Arc<Mutex<Vec<ResidencyEvent>>>,
        close: PreparedArtifactResidencyMeasurements,
        release: PreparedArtifactResidencyMeasurements,
    }

    impl RecordingResidency {
        fn bounded() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                close: PreparedArtifactResidencyMeasurements {
                    peak_resident_bytes: 48,
                    peak_decoder_workspace_bytes: 24,
                    peak_pinned_bytes: 32,
                    hits: 1,
                    loads: 1,
                    evicted_bytes: 8,
                    copied_bytes: PAYLOAD.len() as u64,
                    released_bytes: 48,
                },
                release: PreparedArtifactResidencyMeasurements {
                    released_bytes: 48,
                    ..PreparedArtifactResidencyMeasurements::default()
                },
            }
        }
    }

    impl PreparedArtifactReaderResidency for RecordingResidency {
        fn close(&self) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError> {
            self.events
                .lock()
                .expect("residency event lock")
                .push(ResidencyEvent::Close);
            Ok(self.close)
        }

        fn release(&self) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError> {
            self.events
                .lock()
                .expect("residency event lock")
                .push(ResidencyEvent::Release);
            Ok(self.release)
        }

        fn abort(&self) {
            self.events
                .lock()
                .expect("residency event lock")
                .push(ResidencyEvent::Abort);
        }
    }

    #[derive(Default)]
    struct CollectingConsumer {
        bytes: Vec<u8>,
        require_science_segment: bool,
    }

    impl PreparedArtifactConsumer for CollectingConsumer {
        fn consume_segment(
            &mut self,
            segment: &PreparedArtifactSegmentDescriptor,
            byte_offset: u64,
            input: &[u8],
        ) -> Result<(), PreparedArtifactError> {
            if self.require_science_segment && segment.name() != "science" {
                return Err(PreparedArtifactError::SegmentMismatch);
            }
            if byte_offset as usize != self.bytes.len() {
                return Err(PreparedArtifactError::SegmentMismatch);
            }
            self.bytes.extend_from_slice(input);
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingConsumer(u64);

    impl PreparedArtifactConsumer for CountingConsumer {
        fn consume_segment(
            &mut self,
            _segment: &PreparedArtifactSegmentDescriptor,
            _byte_offset: u64,
            input: &[u8],
        ) -> Result<(), PreparedArtifactError> {
            self.0 = self
                .0
                .checked_add(input.len() as u64)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            Ok(())
        }
    }

    struct DiscardingConsumer;

    impl PreparedArtifactConsumer for DiscardingConsumer {
        fn consume_segment(
            &mut self,
            _segment: &PreparedArtifactSegmentDescriptor,
            _byte_offset: u64,
            _input: &[u8],
        ) -> Result<(), PreparedArtifactError> {
            Ok(())
        }
    }

    #[test]
    fn private_reader_streams_dummy_science_only_inside_its_bound_cache_lifecycle() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        let residency = RecordingResidency::bounded();
        let events = Arc::clone(&residency.events);
        let binding = PreparedArtifactExecutionBinding::new(Arc::clone(&reader), residency);
        let plan = binding.plan();
        assert_eq!(plan.logical_bytes(), PAYLOAD.len() as u64);
        assert_eq!(plan.decoder_workspace_bytes(), DECODER_WORKSPACE_CEILING);
        let catalog = plan.catalog_identity();
        assert_eq!(catalog, fixture.factory.plan().catalog_identity());

        let mut before_activation = CollectingConsumer::default();
        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut before_activation),
            Err(PreparedArtifactError::ReaderInactive)
        ));
        assert!(before_activation.bytes.is_empty());

        let mut foreign_node = cache_node(plan);
        foreign_node.id = WorkNodeId::new("foreign-reader-plan");
        let foreign_cache = scheduled(foreign_node, plan, 7, false);
        let cache = scheduled(cache_node(plan), plan, 7, false);
        let fence = cache.for_fence(FenceKind::Io);
        let release = scheduled(release_node(plan), plan, 7, false);
        let planned = [plan.planned_artifact()];
        let cache_prediction =
            StagePrediction::new(plan.node().clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                u64::MAX,
                u64::MAX,
            )]);
        let release_prediction = StagePrediction::new(plan.release_node().clone(), 1)
            .with_io(vec![IoPrediction::new(IoBufferKind::StorageManager, 0, 0)]);
        let alternative = alternative(plan);
        let completed = BTreeMap::new();
        assert!(matches!(
            binding.activate(context(
                &fixture,
                &foreign_cache,
                &planned,
                &cache_prediction,
                &alternative,
                &completed,
                1,
            )),
            Err(PreparedArtifactError::ReaderBindingMismatch)
        ));
        let cache_context = context(
            &fixture,
            &cache,
            &planned,
            &cache_prediction,
            &alternative,
            &completed,
            1,
        );
        let activation = binding
            .activate(cache_context)
            .expect("activate bound reader");
        assert_eq!(activation.resources().len(), 1);
        assert!(matches!(
            binding.activate(cache_context),
            Err(PreparedArtifactError::ReaderAlreadyActivated)
        ));

        let mut consumed = CollectingConsumer {
            require_science_segment: true,
            ..CollectingConsumer::default()
        };
        reader
            .read(fixture.artifact_identity, &mut consumed)
            .expect("stream real private bytes to dummy science");
        assert_eq!(consumed.bytes, PAYLOAD);

        let wrong_fence_context = context(
            &fixture,
            &fence,
            &planned,
            &cache_prediction,
            &alternative,
            &completed,
            2,
        );
        assert!(matches!(
            binding.close(wrong_fence_context),
            Err(PreparedArtifactError::ReaderBindingMismatch)
        ));
        assert!(events.lock().expect("residency events").is_empty());

        let wrong_release_context = context(
            &fixture,
            &release,
            &planned,
            &release_prediction,
            &alternative,
            &completed,
            2,
        );
        assert!(matches!(
            binding.release(wrong_release_context),
            Err(PreparedArtifactError::ReaderBindingMismatch)
        ));
        assert!(events.lock().expect("residency events").is_empty());

        let release_context = context(
            &fixture,
            &release,
            &planned,
            &release_prediction,
            &alternative,
            &completed,
            1,
        );
        assert!(matches!(
            binding.release(release_context),
            Err(PreparedArtifactError::ReaderInactive)
        ));
        assert!(events.lock().expect("residency events").is_empty());

        let fence_context = context(
            &fixture,
            &fence,
            &planned,
            &cache_prediction,
            &alternative,
            &completed,
            1,
        );
        let closed = binding
            .close(fence_context)
            .expect("close at exact I/O fence");
        assert_eq!(
            events.lock().expect("residency events").as_slice(),
            &[ResidencyEvent::Close]
        );
        let io = closed
            .io()
            .iter()
            .find(|measurement| measurement.kind() == IoBufferKind::StorageManager)
            .expect("reader storage evidence");
        assert!(io.bytes() >= PAYLOAD.len() as u64);
        assert!(io.operations() > 0);
        for resource in [
            LeaseResource::Locks,
            LeaseResource::FileDescriptors,
            LeaseResource::Storage {
                demand_id: plan.storage_demand_id().to_string(),
                use_kind: StorageUseKind::PersistentCache,
            },
            LeaseResource::StorageReadRate {
                demand_id: plan.storage_demand_id().to_string(),
            },
            LeaseResource::StorageWriteRate {
                demand_id: plan.storage_demand_id().to_string(),
            },
            LeaseResource::StorageOperationsRate {
                demand_id: plan.storage_demand_id().to_string(),
            },
            LeaseResource::StorageQueue {
                demand_id: plan.storage_demand_id().to_string(),
            },
        ] {
            assert!(
                closed
                    .resources()
                    .iter()
                    .find(|measurement| measurement.resource() == &resource)
                    .is_some_and(|measurement| measurement.peak() > 0),
                "missing exact reader measurement for {resource:?}"
            );
        }
        assert_eq!(closed.artifacts().len(), 1);
        assert_eq!(closed.artifacts()[0].planned_identity(), catalog);
        assert_eq!(closed.artifacts()[0].observed_identity(), Some(catalog));
        assert_eq!(closed.artifacts()[0].bytes(), PAYLOAD.len() as u64);

        let mut after_close = CollectingConsumer::default();
        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut after_close),
            Err(PreparedArtifactError::ReaderClosed)
        ));
        assert!(after_close.bytes.is_empty());
        assert!(matches!(
            binding.close(fence_context),
            Err(PreparedArtifactError::ReaderClosed)
        ));
        assert_eq!(
            events.lock().expect("residency events").as_slice(),
            &[ResidencyEvent::Close]
        );

        let released = binding
            .release(release_context)
            .expect("release after exact I/O fence");
        assert_eq!(
            released.io()[0].actual(),
            Some((0, 0)),
            "release owns no reader transfer attribution"
        );
        assert_eq!(
            events.lock().expect("residency events").as_slice(),
            &[ResidencyEvent::Close, ResidencyEvent::Release]
        );
        assert!(matches!(
            binding.release(release_context),
            Err(PreparedArtifactError::ReaderClosed)
        ));
    }

    #[test]
    fn downstream_failure_abort_drains_the_same_reader_through_fence_then_cleanup_release() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        let residency = RecordingResidency::bounded();
        let events = Arc::clone(&residency.events);
        let binding = PreparedArtifactExecutionBinding::new(Arc::clone(&reader), residency);
        let plan = binding.plan();
        let cache = scheduled(cache_node(plan), plan, 11, false);
        let fence = cache.for_fence(FenceKind::Io);
        let cleanup_release = scheduled(release_node(plan), plan, 11, true);
        let planned = [plan.planned_artifact()];
        let cache_prediction =
            StagePrediction::new(plan.node().clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                u64::MAX,
                u64::MAX,
            )]);
        let release_prediction = StagePrediction::new(plan.release_node().clone(), 1)
            .with_io(vec![IoPrediction::new(IoBufferKind::StorageManager, 0, 0)]);
        let alternative = alternative(plan);
        let completed = BTreeMap::new();
        binding
            .activate(context(
                &fixture,
                &cache,
                &planned,
                &cache_prediction,
                &alternative,
                &completed,
                3,
            ))
            .expect("activate before downstream science");
        let mut science = CountingConsumer::default();
        reader
            .read(fixture.artifact_identity, &mut science)
            .expect("dummy downstream science consumes prepared bytes");
        assert_eq!(science.0, PAYLOAD.len() as u64);

        binding.abort();
        binding.abort();
        let fence_measurements = binding
            .close(context(
                &fixture,
                &fence,
                &planned,
                &cache_prediction,
                &alternative,
                &completed,
                3,
            ))
            .expect("failure drain settles the already-launched Cache fence");
        assert!(fence_measurements.io()[0].bytes() > 0);
        assert!(reader.state.lock().expect("reader state").aborted);
        binding
            .release(context(
                &fixture,
                &cleanup_release,
                &planned,
                &release_prediction,
                &alternative,
                &completed,
                3,
            ))
            .expect("cleanup Release drains the aborted session");
        assert_eq!(
            events.lock().expect("residency events").as_slice(),
            &[
                ResidencyEvent::Abort,
                ResidencyEvent::Abort,
                ResidencyEvent::Close,
                ResidencyEvent::Release,
            ]
        );
    }

    #[test]
    fn reader_fails_closed_on_pinned_residency_and_exact_counter_overflow() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        let mut residency = RecordingResidency::bounded();
        residency.close.peak_resident_bytes = DECODED_CEILING + 1;
        residency.close.peak_pinned_bytes = DECODED_CEILING + 1;
        let binding = PreparedArtifactExecutionBinding::new(Arc::clone(&reader), residency);
        let plan = binding.plan();
        let cache = scheduled(cache_node(plan), plan, 17, false);
        let fence = cache.for_fence(FenceKind::Io);
        let planned = [plan.planned_artifact()];
        let prediction =
            StagePrediction::new(plan.node().clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                u64::MAX,
                u64::MAX,
            )]);
        let alternative = alternative(plan);
        let completed = BTreeMap::new();
        binding
            .activate(context(
                &fixture,
                &cache,
                &planned,
                &prediction,
                &alternative,
                &completed,
                4,
            ))
            .expect("activate pinned-overrun reader");
        assert!(matches!(
            binding.close(context(
                &fixture,
                &fence,
                &planned,
                &prediction,
                &alternative,
                &completed,
                4,
            )),
            Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: 65,
                budget: DECODED_CEILING,
            })
        ));

        let mut evidence = ValidationEvidence::default();
        evidence.cache_read.bytes = u64::MAX;
        evidence.record(CacheIoClass::Read, 1);
        assert!(evidence.accounting_overflowed);
        assert!(matches!(
            evidence.exact_counter(IoBufferKind::StorageManager),
            Err(PreparedArtifactError::ArtifactTooLarge)
        ));

        let overflow_reader = fixture.factory.session();
        let overflow_residency = RecordingResidency::bounded();
        let overflow_binding =
            PreparedArtifactExecutionBinding::new(Arc::clone(&overflow_reader), overflow_residency);
        let overflow_plan = overflow_binding.plan();
        let overflow_cache = scheduled(cache_node(overflow_plan), overflow_plan, 19, false);
        let overflow_planned = [overflow_plan.planned_artifact()];
        let overflow_prediction =
            StagePrediction::new(overflow_plan.node().clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                u64::MAX,
                u64::MAX,
            )]);
        overflow_binding
            .activate(context(
                &fixture,
                &overflow_cache,
                &overflow_planned,
                &overflow_prediction,
                &alternative,
                &completed,
                5,
            ))
            .expect("activate counter-overrun reader");
        overflow_reader
            .state
            .lock()
            .expect("reader state")
            .read_bytes = u64::MAX;
        assert!(matches!(
            overflow_reader.read(fixture.artifact_identity, &mut DiscardingConsumer),
            Err(PreparedArtifactError::ArtifactTooLarge)
        ));
        assert!(overflow_reader.state.lock().expect("reader state").aborted);
    }

    #[test]
    fn reader_fails_closed_when_decoder_workspace_exceeds_its_separate_ceiling() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        let mut residency = RecordingResidency::bounded();
        residency.close.peak_decoder_workspace_bytes = DECODER_WORKSPACE_CEILING + 1;
        let binding = PreparedArtifactExecutionBinding::new(Arc::clone(&reader), residency);
        let plan = binding.plan();
        let cache = scheduled(cache_node(plan), plan, 23, false);
        let fence = cache.for_fence(FenceKind::Io);
        let planned = [plan.planned_artifact()];
        let prediction =
            StagePrediction::new(plan.node().clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                u64::MAX,
                u64::MAX,
            )]);
        let alternative = alternative(plan);
        let completed = BTreeMap::new();
        binding
            .activate(context(
                &fixture,
                &cache,
                &planned,
                &prediction,
                &alternative,
                &completed,
                6,
            ))
            .expect("activate workspace-overrun reader");
        assert!(matches!(
            binding.close(context(
                &fixture,
                &fence,
                &planned,
                &prediction,
                &alternative,
                &completed,
                6,
            )),
            Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: 33,
                budget: DECODER_WORKSPACE_CEILING,
            })
        ));
    }
}
