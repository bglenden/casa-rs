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
pub(super) struct ReaderEntry {
    pub(super) descriptor: PreparedArtifactDescriptor,
    pub(super) integrity_identity: ArtifactIdentity,
    pub(super) payload_bytes: u64,
}

pub(super) struct ReaderManifestSnapshot {
    segment_digests: Box<[Box<[[u8; 32]]>]>,
    resident_bytes: u64,
}

impl ReaderManifestSnapshot {
    pub(super) fn new(
        entries: &[ReaderEntry],
        segment_digests: Vec<Box<[[u8; 32]]>>,
        resident_bytes: u64,
    ) -> Result<Self, PreparedArtifactError> {
        if entries.len() != segment_digests.len()
            || entries
                .iter()
                .zip(&segment_digests)
                .any(|(entry, digests)| entry.descriptor.segments.len() != digests.len())
        {
            return Err(PreparedArtifactError::IdentityMismatch);
        }
        Ok(Self {
            segment_digests: segment_digests.into_boxed_slice(),
            resident_bytes,
        })
    }

    pub(super) fn segment_digests(&self, entry: usize) -> &[[u8; 32]] {
        &self.segment_digests[entry]
    }

    pub(super) const fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }
}

pub(super) fn reader_manifest_snapshot_resident_bytes(
    entries: &[ReaderEntry],
) -> Result<u64, PreparedArtifactError> {
    let fixed = size_of::<ReaderManifestSnapshot>()
        .checked_add(size_of::<Arc<ReaderManifestSnapshot>>())
        .and_then(|bytes| {
            bytes.checked_add(entries.len().checked_mul(size_of::<Box<[[u8; 32]]>>())?)
        })
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    entries
        .iter()
        .try_fold(fixed, |bytes, entry| {
            bytes
                .checked_add(
                    entry
                        .descriptor
                        .segments
                        .len()
                        .checked_mul(size_of::<[u8; 32]>())
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
                )
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
        .and_then(|bytes| u64::try_from(bytes).map_err(|_| PreparedArtifactError::ArtifactTooLarge))
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
        store_resident_bytes = store_resident_bytes
            .checked_add(reader_manifest_snapshot_resident_bytes(&entries)?)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
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
    store_lock: Option<ReaderStoreLock>,
    manifest_snapshot: Option<Arc<ReaderManifestSnapshot>>,
    session_validation_count: u64,
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
    observations: ReaderSessionObservations,
    session_started: Option<std::time::Instant>,
    read_count: u64,
    closed: bool,
    aborted: bool,
    reader_failed: bool,
    released: bool,
    fence_emitted: bool,
    observer_emitted: bool,
}

impl ReaderState {
    fn observe(
        &mut self,
        measurements: &super::transaction::PreparedArtifactSessionMeasurements,
    ) -> Result<(), PreparedArtifactError> {
        self.read_bytes = self
            .read_bytes
            .checked_add(measurements.read_bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        self.read_operations = self
            .read_operations
            .checked_add(measurements.read_operations)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        self.reader_resident_peak = self
            .reader_resident_peak
            .max(measurements.resident_buffer_bytes);
        self.cache_bytes_peak = self.cache_bytes_peak.max(measurements.cache_bytes);
        self.locks_peak = self.locks_peak.max(measurements.locks);
        self.file_descriptors_peak = self
            .file_descriptors_peak
            .max(measurements.file_descriptors);
        self.read_rate_peak = self.read_rate_peak.max(measurements.read_rate);
        self.write_rate_peak = self.write_rate_peak.max(measurements.write_rate);
        self.operations_rate_peak = self.operations_rate_peak.max(measurements.operations_rate);
        self.queue_slots_peak = self.queue_slots_peak.max(measurements.queue_slots);
        self.observations.peak_buffer_bytes = self
            .observations
            .peak_buffer_bytes
            .max(measurements.resident_buffer_bytes);
        self.observations.merge(&measurements.observations);
        Ok(())
    }

    fn record_cell_requested(&mut self) {
        self.observations.cells_requested = self.observations.cells_requested.saturating_add(1);
    }

    fn record_cell_committed(&mut self) {
        self.observations.cells_verified = self.observations.cells_verified.saturating_add(1);
        self.observations.cells_committed = self.observations.cells_committed.saturating_add(1);
    }

    fn record_cell_rejected(&mut self, identity: ArtifactIdentity) {
        self.observations.cells_rejected = self.observations.cells_rejected.saturating_add(1);
        if self.observations.first_failure_identity.is_none() {
            self.observations.first_failure_identity = Some(identity);
        }
    }

    fn observe_residency(&mut self, residency: PreparedArtifactResidencyMeasurements) {
        self.observations.peak_decoded_bytes = self
            .observations
            .peak_decoded_bytes
            .max(residency.peak_resident_bytes);
    }
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
        state.session_started = Some(std::time::Instant::now());
        drop(state);
        let session = self.store.begin_reader_session(&self.entries);
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        let (store_lock, manifest_snapshot, measurements) = match session {
            Ok(session) => session,
            Err(failure) => {
                if let Some(measurements) = &failure.measurements {
                    if let Err(error) = state.observe(measurements) {
                        state.aborted = true;
                        state.reader_failed = true;
                        return Err(error);
                    }
                }
                state.aborted = true;
                state.reader_failed = true;
                return Err(failure.source);
            }
        };
        if state.closed || state.released || state.aborted {
            return Err(PreparedArtifactError::ReaderClosed);
        }
        if let Err(error) = state.observe(&measurements) {
            state.aborted = true;
            state.reader_failed = true;
            return Err(error);
        }
        state.store_lock = Some(store_lock);
        state.manifest_snapshot = Some(manifest_snapshot);
        state.session_validation_count = 1;
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
        let (entry_index, entry) = self
            .entries
            .binary_search_by_key(&identity, |entry| entry.descriptor.identity())
            .ok()
            .map(|index| (index, self.entries[index].clone()))
            .ok_or(PreparedArtifactError::ReaderArtifactMissing)?;
        let manifest_snapshot;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| PreparedArtifactError::PoisonedStore)?;
            if state.binding.is_none() {
                return Err(PreparedArtifactError::ReaderInactive);
            }
            if state.store_lock.is_none()
                || state.manifest_snapshot.is_none()
                || state.session_validation_count != 1
                || state.closed
                || state.aborted
                || state.released
            {
                return Err(PreparedArtifactError::ReaderClosed);
            }
            manifest_snapshot = Arc::clone(
                state
                    .manifest_snapshot
                    .as_ref()
                    .expect("validated reader snapshot was checked"),
            );
            state.record_cell_requested();
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
        let read = self.store.read_for_reader(
            &entry.descriptor,
            &artifact,
            manifest_snapshot.segment_digests(entry_index),
            manifest_snapshot.resident_bytes(),
            consumer,
        );
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        state.active_reads = state
            .active_reads
            .checked_sub(1)
            .ok_or(PreparedArtifactError::InvalidDescriptor)?;
        let outcome = match read {
            Ok(read) => {
                if let Err(error) = state.observe(&read.measurements) {
                    state.aborted = true;
                    state.reader_failed = true;
                    state.record_cell_rejected(identity);
                    Err(error)
                } else if state.aborted {
                    let error = PreparedArtifactError::ReaderClosed;
                    state.record_cell_rejected(identity);
                    Err(error)
                } else if read.content_identity != entry.integrity_identity
                    || read.payload_bytes != entry.payload_bytes
                {
                    let error = PreparedArtifactError::IdentityMismatch;
                    state.aborted = true;
                    state.reader_failed = true;
                    state.observations.record_failure(identity, &error);
                    state.record_cell_rejected(identity);
                    Err(error)
                } else {
                    match state.read_count.checked_add(1) {
                        Some(read_count) => {
                            state.record_cell_committed();
                            state.read_count = read_count;
                            Ok(())
                        }
                        None => {
                            state.aborted = true;
                            state.reader_failed = true;
                            state.record_cell_rejected(identity);
                            Err(PreparedArtifactError::ArtifactTooLarge)
                        }
                    }
                }
            }
            Err(failure) => {
                if let Some(measurements) = &failure.measurements {
                    if let Err(error) = state.observe(measurements) {
                        state.aborted = true;
                        state.reader_failed = true;
                        state.record_cell_rejected(identity);
                        self.settled.notify_all();
                        return Err(error);
                    }
                }
                state.aborted = true;
                state.reader_failed = true;
                if failure.measurements.is_none() {
                    state.observations.record_failure(identity, &failure.source);
                }
                state.record_cell_rejected(identity);
                Err(failure.source)
            }
        };
        self.settled.notify_all();
        outcome
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
        state.closed = true;
        while state.active_reads != 0 {
            state = self
                .settled
                .wait(state)
                .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        }
        if state.fence_emitted {
            return Err(PreparedArtifactError::ReaderClosed);
        }
        state.fence_emitted = true;
        if let Some(mut store_lock) = state.store_lock.take() {
            let mut evidence = ValidationEvidence::default();
            store_lock.release(&mut evidence)?;
            let measurements = super::transaction::session_measurements(&evidence, 0)?;
            state.observe(&measurements)?;
        }
        state.manifest_snapshot.take();
        state.observe_residency(residency);
        if state.reader_failed {
            return Err(PreparedArtifactError::ReaderClosed);
        }
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
        self.emit_observer(&mut state, residency, combined_resident);
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
        residency: PreparedArtifactResidencyMeasurements,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        self.validate_release_context(context)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        state.closed = true;
        while state.active_reads != 0 {
            state = self
                .settled
                .wait(state)
                .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        }
        state.released = true;
        state.store_lock.take();
        state.manifest_snapshot.take();
        state.observe_residency(residency);
        let combined_resident = residency
            .peak_resident_bytes
            .checked_add(residency.peak_decoder_workspace_bytes)
            .and_then(|bytes| bytes.checked_add(state.reader_resident_peak))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        self.emit_observer(&mut state, residency, combined_resident);
        let resources = context
            .resources()
            .iter()
            .map(|capability| {
                let peak = if capability.resource()
                    == &LeaseResource::IoBuffer(IoBufferKind::StorageManager)
                {
                    residency.released_bytes
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

    fn emit_observer(
        &self,
        state: &mut ReaderState,
        residency: PreparedArtifactResidencyMeasurements,
        combined_resident: u64,
    ) {
        if state.observer_emitted {
            return;
        }
        state.observer_emitted = true;
        state.observations.session_wall = state
            .session_started
            .map_or(std::time::Duration::ZERO, |started| started.elapsed());
        eprintln!(
            "imaging_prepared_artifact_reader_summary catalog={} logical_bytes={} decoded_ceiling_bytes={} decoder_workspace_ceiling_bytes={} total_ceiling_bytes={} session_validations={} reads={} read_bytes={} read_operations={} resident_peak_bytes={} decoder_workspace_peak_bytes={} total_peak_resident_bytes={} pinned_peak_bytes={} hits={} loads={} evicted_bytes={} copied_bytes={} aborted={} {}",
            self.plan.catalog_identity,
            self.plan.logical_bytes,
            self.plan.decoded_resident_bytes,
            self.plan.decoder_workspace_bytes,
            self.plan.total_resident_bytes,
            state.session_validation_count,
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
            state.observations,
        );
    }

    fn abort(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.aborted = true;
            while state.active_reads != 0 {
                let Ok(next) = self.settled.wait(state) else {
                    return;
                };
                state = next;
            }
            state.store_lock.take();
            state.manifest_snapshot.take();
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
        self.reader.release(context, residency)
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
    const STREAMING_BUFFER_BYTES: u64 = 8;

    struct ReaderFixture {
        _directory: tempfile::TempDir,
        problem: CompiledProblem,
        registry: crate::ImplementationRegistryId,
        factory: PreparedArtifactReaderFactory,
        artifact_identity: ArtifactIdentity,
    }

    impl ReaderFixture {
        fn new() -> Self {
            Self::with_entries(1)
        }

        fn with_entries(entry_count: usize) -> Self {
            assert!(entry_count > 0);
            let directory = tempfile::tempdir().expect("reader private cache root");
            let storage = StorageDomain {
                id: StorageDomainId::new("reader-test-storage"),
                root: directory.path().to_path_buf(),
                capacity_bytes: 4 << 20,
                read_rate: crate::RateResourceId::new("reader-test-read-rate"),
                write_rate: crate::RateResourceId::new("reader-test-write-rate"),
                operations_rate: None,
                queue: crate::QueueResourceId::new("reader-test-queue"),
            };
            let budget = PreparedArtifactBudget::new(4 << 20, entry_count, STREAMING_BUFFER_BYTES)
                .expect("bounded reader cache budget");
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
            let payload_sha256: [u8; 32] = Sha256::digest(PAYLOAD).into();
            let mut artifacts = Vec::with_capacity(entry_count);
            for ordinal in 0..entry_count {
                let scientific_identity = PreparedArtifactScientificIdentity::kernel(
                    PreparedArtifactKernelSemantics::new(
                        PreparedArtifactKernelAlgorithm::Gridding,
                        vec![PAYLOAD.len() as u64, ordinal as u64 + 1],
                        vec![PAYLOAD.len() as u64, ordinal as u64 + 1],
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
                    owner.clone(),
                    PreparedArtifactKind::Kernel,
                    scientific,
                    store.scope.clone(),
                    vec![segment.clone()],
                )
                .expect("reader test descriptor");
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
                artifacts.push((descriptor, artifact));
            }
            let artifact_identity = artifacts[0].0.identity();
            let factory = PreparedArtifactReaderFactory::new(
                Arc::clone(&store),
                artifacts,
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

    fn with_reader_context<T>(
        fixture: &ReaderFixture,
        reader: &PreparedArtifactReader,
        release: bool,
        use_context: impl FnOnce(WorkExecutionContext<'_>) -> T,
    ) -> T {
        let plan = reader.plan();
        let node = if release {
            scheduled(release_node(plan), plan, 7, true)
        } else {
            scheduled(cache_node(plan), plan, 7, false)
        };
        let planned = [plan.planned_artifact()];
        let prediction =
            StagePrediction::new(node.node().id.clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                if release { 0 } else { u64::MAX },
                if release { 0 } else { u64::MAX },
            )]);
        let alternative = alternative(plan);
        let completed = BTreeMap::new();
        use_context(context(
            fixture,
            &node,
            &planned,
            &prediction,
            &alternative,
            &completed,
            1,
        ))
    }

    fn activate_reader(
        fixture: &ReaderFixture,
        reader: &PreparedArtifactReader,
    ) -> Result<WorkMeasurements, PreparedArtifactError> {
        with_reader_context(fixture, reader, false, |context| reader.activate(context))
    }

    fn rewrite_manifest(fixture: &ReaderFixture, rewrite: impl FnOnce(&mut ArtifactManifest)) {
        let path = fixture
            .factory
            .store
            .entry_path(fixture.artifact_identity)
            .join(MANIFEST_FILE);
        let mut manifest: ArtifactManifest =
            serde_json::from_slice(&fs::read(&path).expect("read test manifest"))
                .expect("decode test manifest");
        rewrite(&mut manifest);
        let mut encoded = serde_json::to_vec(&manifest).expect("encode test manifest");
        encoded.push(b'\n');
        fs::write(path, encoded).expect("rewrite test manifest");
    }

    fn assert_metadata_only_activation(reader: &PreparedArtifactReader) {
        let state = reader.state.lock().expect("reader state");
        assert_eq!(state.observations.activation_payload.opens, 0);
        assert_eq!(state.observations.activation_payload.read_bytes, 0);
        assert_eq!(state.observations.activation_payload.hashed_bytes, 0);
    }

    fn assert_failed_reader_unpublished(
        fixture: &ReaderFixture,
        reader: &PreparedArtifactReader,
        read_bytes: u64,
        hashed_bytes: u64,
        digest_failures: u64,
        eof_failures: u64,
    ) {
        let state = reader.state.lock().expect("reader state");
        assert!(state.aborted);
        assert!(state.reader_failed);
        assert_eq!(state.read_count, 0);
        assert_eq!(state.observations.consume_payload.opens, 1);
        assert_eq!(state.observations.consume_payload.read_bytes, read_bytes);
        assert_eq!(
            state.observations.consume_payload.hashed_bytes,
            hashed_bytes
        );
        assert_eq!(state.observations.cells_requested, 1);
        assert_eq!(state.observations.cells_verified, 0);
        assert_eq!(state.observations.cells_committed, 0);
        assert_eq!(state.observations.cells_rejected, 1);
        assert_eq!(state.observations.digest_failures, digest_failures);
        assert_eq!(state.observations.eof_failures, eof_failures);
        assert_eq!(
            state.observations.first_failure_identity,
            Some(fixture.artifact_identity)
        );
        drop(state);

        assert!(matches!(
            with_reader_context(fixture, reader, false, |context| reader
                .close(context, PreparedArtifactResidencyMeasurements::default())),
            Err(PreparedArtifactError::ReaderClosed)
        ));
        let released = with_reader_context(fixture, reader, true, |context| {
            reader.release(context, PreparedArtifactResidencyMeasurements::default())
        })
        .expect("release failed reader");
        assert!(released.artifacts().is_empty());
        assert!(reader.state.lock().expect("reader state").observer_emitted);
    }

    #[test]
    fn reader_session_does_not_repeat_store_inventory_or_payload_validation_per_load() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        let plan = reader.plan();
        let cache = scheduled(cache_node(plan), plan, 7, false);
        let planned = [plan.planned_artifact()];
        let prediction =
            StagePrediction::new(plan.node().clone(), 1).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                u64::MAX,
                u64::MAX,
            )]);
        let alternative = alternative(plan);
        let completed = BTreeMap::new();
        reader
            .activate(context(
                &fixture,
                &cache,
                &planned,
                &prediction,
                &alternative,
                &completed,
                1,
            ))
            .expect("activate reader session");
        assert_eq!(
            reader
                .state
                .lock()
                .expect("reader state")
                .session_validation_count,
            1,
            "the immutable catalog is validated exactly once per reader session"
        );
        assert_eq!(
            fixture
                .factory
                .store
                .state
                .access
                .lock()
                .expect("store access")
                .active_readers,
            1,
            "the validated catalog must exclude store mutation for the session"
        );

        let mut prior_bytes = reader.state.lock().expect("reader state").read_bytes;
        let mut prior_operations = reader.state.lock().expect("reader state").read_operations;
        let expected_operations = (PAYLOAD.len() as u64).div_ceil(STREAMING_BUFFER_BYTES) * 2
            + fixture.factory.entries[0].descriptor.segments.len() as u64
            + 3;
        for read in 1..=2 {
            reader
                .read(fixture.artifact_identity, &mut DiscardingConsumer)
                .expect("stream validated cell");
            let state = reader.state.lock().expect("reader state");
            let read_bytes = state.read_bytes - prior_bytes;
            let read_operations = state.read_operations - prior_operations;
            assert_eq!(
                read_bytes,
                PAYLOAD.len() as u64,
                "cell load {read} must stream the target payload exactly once"
            );
            assert_eq!(
                read_operations, expected_operations,
                "cell load {read} must do only bounded payload reads and checksum work"
            );
            prior_bytes = state.read_bytes;
            prior_operations = state.read_operations;
        }
        assert!(
            reader
                .state
                .lock()
                .expect("reader state")
                .reader_resident_peak
                <= reader.plan.store_resident_bytes,
            "session validation and target-cell reads must stay within the planned store residency"
        );

        let payload = fixture
            .factory
            .store
            .entry_path(fixture.artifact_identity)
            .join(PAYLOAD_FILE);
        let mut corrupted = PAYLOAD.to_vec();
        corrupted[0] ^= 1;
        fs::write(payload, corrupted).expect("corrupt one cell behind the cooperative store lock");
        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut DiscardingConsumer),
            Err(PreparedArtifactError::CorruptArtifact)
        ));
        assert_eq!(
            reader
                .state
                .lock()
                .expect("reader state")
                .session_validation_count,
            1,
            "per-cell integrity checks must not repeat catalog validation"
        );
        reader.abort();
        assert_eq!(
            fixture
                .factory
                .store
                .state
                .access
                .lock()
                .expect("store access")
                .active_readers,
            0,
            "aborting the reader must release store mutation exclusion"
        );
    }

    #[test]
    fn reader_activation_of_256_entries_reads_manifests_but_defers_payload_integrity() {
        let fixture = ReaderFixture::with_entries(256);
        let reader = fixture.factory.session();
        activate_reader(&fixture, &reader).expect("activate 256-entry reader session");

        let state = reader.state.lock().expect("reader state");
        assert_eq!(state.session_validation_count, 1);
        assert_eq!(state.observations.expected_entries, 256);
        assert_eq!(state.observations.discovered_entries, 256);
        assert_eq!(state.observations.accepted_entries, 256);
        assert_eq!(state.observations.activation_payload.opens, 0);
        assert_eq!(state.observations.activation_payload.read_bytes, 0);
        assert_eq!(state.observations.activation_payload.hashed_bytes, 0);
        assert!(state.observations.directory_enumerations > 0);
        assert_eq!(state.read_bytes, state.observations.manifest_bytes);
        assert!(state.reader_resident_peak <= reader.plan.store_resident_bytes);
        drop(state);

        reader.abort();
    }

    #[test]
    fn valid_cell_is_opened_read_hashed_verified_and_committed_once() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        activate_reader(&fixture, &reader).expect("metadata-only activation");

        let mut consumer = CollectingConsumer::default();
        reader
            .read(fixture.artifact_identity, &mut consumer)
            .expect("validated first consume");

        assert_eq!(consumer.bytes, PAYLOAD);
        let state = reader.state.lock().expect("reader state");
        let observed = &state.observations;
        assert_eq!(observed.activation_payload.metadata_checks, 3);
        assert_eq!(observed.activation_payload.opens, 0);
        assert_eq!(
            observed.activation_payload.declared_bytes,
            PAYLOAD.len() as u64
        );
        assert_eq!(observed.activation_payload.read_bytes, 0);
        assert_eq!(observed.activation_payload.hashed_bytes, 0);
        assert_eq!(observed.consume_payload.metadata_checks, 0);
        assert_eq!(observed.consume_payload.opens, 1);
        assert_eq!(
            observed.consume_payload.declared_bytes,
            PAYLOAD.len() as u64
        );
        assert_eq!(observed.consume_payload.read_bytes, PAYLOAD.len() as u64);
        assert_eq!(observed.consume_payload.hashed_bytes, PAYLOAD.len() as u64);
        assert_eq!(observed.cells_requested, 1);
        assert_eq!(observed.cells_verified, 1);
        assert_eq!(observed.cells_committed, 1);
        assert_eq!(observed.cells_rejected, 0);
        assert_eq!(observed.digest_failures, 0);
        assert_eq!(observed.eof_failures, 0);
        assert_eq!(observed.finite_failures, 0);
        drop(state);
        reader.abort();
    }

    #[test]
    fn reader_activation_rejects_staging_and_duplicate_catalog_entries() {
        let fixture = ReaderFixture::new();
        let staging = fixture
            .factory
            .store
            .cache
            .join(format!("{STAGING_PREFIX}reader-test"));
        fs::create_dir(&staging).expect("staging directory");
        let reader = fixture.factory.session();
        assert!(matches!(
            activate_reader(&fixture, &reader),
            Err(PreparedArtifactError::UnknownCacheEntry(path)) if path == staging
        ));
        assert_eq!(
            reader
                .state
                .lock()
                .expect("reader state")
                .observations
                .activation_payload
                .read_bytes,
            0,
            "failed metadata activation must not open a payload"
        );

        fs::remove_dir(staging).expect("remove test staging directory");
        let unknown = fixture.factory.store.cache.join("unknown-entry");
        fs::write(&unknown, b"unknown").expect("unknown cache entry");
        let reader = fixture.factory.session();
        assert!(matches!(
            activate_reader(&fixture, &reader),
            Err(PreparedArtifactError::UnknownCacheEntry(path)) if path == unknown
        ));
        fs::remove_file(unknown).expect("remove unknown cache entry");

        let duplicate = fixture.factory.entries[0].clone();
        let failure = fixture
            .factory
            .store
            .begin_reader_session(&[duplicate.clone(), duplicate])
            .err()
            .expect("duplicate catalog must fail activation");
        assert!(matches!(
            failure.source,
            PreparedArtifactError::InvalidDescriptor
        ));
    }

    #[test]
    fn late_payload_corruption_aborts_reader_without_catalog_publication() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        activate_reader(&fixture, &reader).expect("metadata-only activation");
        let payload = fixture
            .factory
            .store
            .entry_path(fixture.artifact_identity)
            .join(PAYLOAD_FILE);
        let mut corrupted = PAYLOAD.to_vec();
        *corrupted.last_mut().expect("non-empty payload") ^= 1;
        fs::write(payload, corrupted).expect("late same-length corruption");

        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut DiscardingConsumer),
            Err(PreparedArtifactError::CorruptArtifact)
        ));
        assert_failed_reader_unpublished(
            &fixture,
            &reader,
            PAYLOAD.len() as u64,
            PAYLOAD.len() as u64,
            1,
            0,
        );
    }

    #[test]
    fn valid_hex_segment_digest_corruption_fails_on_first_consume_without_publication() {
        let fixture = ReaderFixture::new();
        rewrite_manifest(&fixture, |manifest| {
            manifest.segments[0].sha256 = "00".repeat(32);
        });
        let reader = fixture.factory.session();
        activate_reader(&fixture, &reader).expect("metadata-only activation");
        assert_metadata_only_activation(&reader);

        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut DiscardingConsumer),
            Err(PreparedArtifactError::CorruptArtifact)
        ));
        assert_failed_reader_unpublished(
            &fixture,
            &reader,
            PAYLOAD.len() as u64,
            PAYLOAD.len() as u64,
            1,
            0,
        );
    }

    #[test]
    fn post_activation_short_payload_fails_once_without_publication() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        activate_reader(&fixture, &reader).expect("metadata-only activation");
        assert_metadata_only_activation(&reader);
        let payload = fixture
            .factory
            .store
            .entry_path(fixture.artifact_identity)
            .join(PAYLOAD_FILE);
        fs::write(payload, &PAYLOAD[..PAYLOAD.len() - 1])
            .expect("truncate payload after activation");

        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut DiscardingConsumer),
            Err(PreparedArtifactError::IncompleteArtifact)
        ));
        let completely_hashed_chunks =
            (PAYLOAD.len() / STREAMING_BUFFER_BYTES as usize) * STREAMING_BUFFER_BYTES as usize;
        assert_failed_reader_unpublished(
            &fixture,
            &reader,
            (PAYLOAD.len() - 1) as u64,
            completely_hashed_chunks as u64,
            0,
            1,
        );
    }

    #[test]
    fn post_activation_extra_payload_byte_fails_once_without_publication() {
        let fixture = ReaderFixture::new();
        let reader = fixture.factory.session();
        activate_reader(&fixture, &reader).expect("metadata-only activation");
        assert_metadata_only_activation(&reader);
        let payload = fixture
            .factory
            .store
            .entry_path(fixture.artifact_identity)
            .join(PAYLOAD_FILE);
        let mut oversized = PAYLOAD.to_vec();
        oversized.push(0);
        fs::write(payload, oversized).expect("append payload byte after activation");

        assert!(matches!(
            reader.read(fixture.artifact_identity, &mut DiscardingConsumer),
            Err(PreparedArtifactError::OversizedArtifact)
        ));
        assert_failed_reader_unpublished(
            &fixture,
            &reader,
            (PAYLOAD.len() + 1) as u64,
            PAYLOAD.len() as u64,
            0,
            1,
        );
    }

    #[test]
    fn reader_observations_are_flat_parser_compatible_key_values() {
        let observed = ReaderSessionObservations {
            lock_wait: std::time::Duration::from_nanos(2),
            metadata_validation: std::time::Duration::from_nanos(3),
            payload_consumption: std::time::Duration::from_nanos(5),
            session_wall: std::time::Duration::from_nanos(7),
            ..ReaderSessionObservations::default()
        };

        let rendered = observed.to_string();
        let keys = rendered
            .split_whitespace()
            .map(|field| field.split_once('=').expect("flat key=value field").0)
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            [
                "lock_wait_nanos",
                "metadata_validation_nanos",
                "payload_consumption_nanos",
                "session_wall_nanos",
                "expected_entries",
                "discovered_entries",
                "accepted_entries",
                "directory_enumerations",
                "directory_entries",
                "manifest_opens",
                "manifest_bytes",
                "activation_payload_metadata_checks",
                "activation_payload_opens",
                "activation_payload_declared_bytes",
                "activation_payload_read_bytes",
                "activation_payload_hashed_bytes",
                "consume_payload_metadata_checks",
                "consume_payload_opens",
                "consume_payload_declared_bytes",
                "consume_payload_read_bytes",
                "consume_payload_hashed_bytes",
                "cells_requested",
                "cells_verified",
                "cells_committed",
                "cells_rejected",
                "duplicates",
                "digest_failures",
                "eof_failures",
                "finite_failures",
                "peak_decoded_bytes",
                "peak_buffer_bytes",
                "first_failure_identity",
            ]
        );
        assert!(rendered.contains("lock_wait_nanos=2"));
        assert!(rendered.contains("metadata_validation_nanos=3"));
        assert!(rendered.contains("payload_consumption_nanos=5"));
        assert!(rendered.contains("session_wall_nanos=7"));
        assert!(rendered.ends_with("first_failure_identity=none"));
    }

    #[test]
    fn activation_rejects_manifest_corruption_and_wrong_payload_length_without_payload_reads() {
        let fixture = ReaderFixture::new();
        let entry = fixture.factory.store.entry_path(fixture.artifact_identity);
        fs::write(entry.join(MANIFEST_FILE), b"not-json\n").expect("corrupt test manifest");
        let reader = fixture.factory.session();
        assert!(matches!(
            activate_reader(&fixture, &reader),
            Err(PreparedArtifactError::Json(_))
        ));
        assert_eq!(
            reader
                .state
                .lock()
                .expect("reader state")
                .observations
                .activation_payload
                .read_bytes,
            0
        );

        let fixture = ReaderFixture::new();
        let payload = fixture
            .factory
            .store
            .entry_path(fixture.artifact_identity)
            .join(PAYLOAD_FILE);
        let mut oversized = PAYLOAD.to_vec();
        oversized.push(0);
        fs::write(payload, oversized).expect("append one test payload byte");
        let reader = fixture.factory.session();
        assert!(matches!(
            activate_reader(&fixture, &reader),
            Err(PreparedArtifactError::IncompleteArtifact)
        ));
        assert_eq!(
            reader
                .state
                .lock()
                .expect("reader state")
                .observations
                .activation_payload
                .read_bytes,
            0
        );
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
