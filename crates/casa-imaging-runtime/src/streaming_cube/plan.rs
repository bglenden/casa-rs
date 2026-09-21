// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native preparation and wave residency within the existing physical plan.
//! Source/weighting and paged model/normal reservations are composed first;
//! this owner can consume only the authority's remaining native allowance.

use std::{collections::BTreeSet, io, mem::size_of};

use casa_imaging_model::CompiledProblem;
use casa_imaging_reconstruction::runtime_adapter::BandPlan;

use super::{
    execute::WavePlan,
    input::{NativeSource, NativeStore, StorePlan},
    prepare::NativePreparation,
};
use crate::*;

pub(super) struct NativePhasePlan {
    pub(super) store: StorePlan,
    pub(super) shared_bytes: u64,
    pub(super) workspace_bytes: u64,
    pub(super) workers: usize,
    pub(super) source_slots: usize,
    pub(super) minimum_workspace_bytes: u64,
    read: WorkNodeId,
    reconcile: WorkNodeId,
    storage_id: String,
    imported: bool,
    pub(super) prior_window_bytes: u64,
}

fn overflow() -> io::Error {
    io::Error::other("native phase residency overflow")
}

impl NativePhasePlan {
    /// Store each native channel independently so complete band jobs read only
    /// their exact discovered support, regardless of the number of resident
    /// output bands. Row blocking still derives from the source buffer budget.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn for_initial_source(
        base: &PhysicalWorkBinding,
        authority: &ResourceAuthority,
        policy: &ResourcePolicy,
        storage: &ManagedSpillStorage,
        problem: &CompiledProblem,
        bands: &[BandPlan],
        shared_owner_bytes: u64,
        workers: usize,
        source_slots: usize,
    ) -> io::Result<Self> {
        let [source] = problem.selected_observation().read_set().sources() else {
            return Err(io::Error::other("native cube requires one selected source"));
        };
        let selection = source.selection();
        let ([dd], [spw], [pol]) = (
            selection.data_descriptions(),
            selection.spectral_windows(),
            selection.correlations(),
        ) else {
            return Err(io::Error::other(
                "native cube requires one homogeneous selection",
            ));
        };
        if dd.spectral_window_id() != spw.spectral_window_id()
            || dd.polarization_id() != pol.polarization_id()
            || spw.channel_indices().len() < 2
        {
            return Err(io::Error::other("invalid native cube source axes"));
        }
        let source_buffer_bytes = usize::try_from(
            base.execution_dag()
                .resource_alternative()
                .demand
                .io_buffers
                .source_read_ahead_bytes,
        )
        .map_err(|_| overflow())?;
        let store = StorePlan::for_source_buffer(
            selection.rows().selected_row_count(),
            spw.channel_indices().len(),
            pol.products().len(),
            1,
            source_buffer_bytes,
        )?;
        Self::new(
            base,
            authority,
            policy,
            storage,
            store,
            bands,
            shared_owner_bytes,
            workers,
            source_slots,
        )
    }

    /// `shared_owner_bytes` covers native layout/frequency descriptors and
    /// enclosing executor state not already charged by the composed base.
    /// Support grows during the source pass, so its shape bound is reserved
    /// here. Actual post-preparation support selects each wave, without rereads.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        base: &PhysicalWorkBinding,
        authority: &ResourceAuthority,
        policy: &ResourcePolicy,
        storage: &ManagedSpillStorage,
        store: StorePlan,
        bands: &[BandPlan],
        shared_owner_bytes: u64,
        workers: usize,
        source_slots: usize,
    ) -> io::Result<Self> {
        Self::new_for_pass(
            base,
            authority,
            policy,
            storage,
            store,
            bands,
            shared_owner_bytes,
            workers,
            source_slots,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_for_pass(
        base: &PhysicalWorkBinding,
        authority: &ResourceAuthority,
        policy: &ResourcePolicy,
        storage: &ManagedSpillStorage,
        store: StorePlan,
        bands: &[BandPlan],
        shared_owner_bytes: u64,
        workers: usize,
        source_slots: usize,
        prior_window_bytes: Option<u64>,
    ) -> io::Result<Self> {
        if prior_window_bytes.is_none()
            && bands.iter().any(|band| !band.is_certified_empty_initial())
        {
            return Err(io::Error::other(
                "native initial phase requires a certified empty model",
            ));
        }
        let read = base
            .execution_dag()
            .nodes()
            .values()
            .find(|node| node.kind == WorkKind::ObservationRead)
            .ok_or_else(|| io::Error::other("native preparation lacks source read"))?
            .id
            .clone();
        let reconcile = base
            .observation_transaction()
            .post_replay_reconciliation()
            .ok_or_else(|| io::Error::other("native phase lacks reconciliation"))?
            .clone();
        let storage_id = format!("native-cube-{}", read.as_str());
        let resources = [
            LeaseResource::Storage {
                demand_id: storage_id.clone(),
                use_kind: StorageUseKind::Temporary,
            },
            LeaseResource::FileDescriptors,
        ];
        let permits = RetainedArtifactPermit::heap_bytes_for_resources(
            &resources,
            0,
            "host-memory",
            storage.resources().domain().as_str(),
        )
        .ok_or_else(overflow)?;
        let mut shared_bytes = shared_owner_bytes
            .checked_add(size_of::<Self>() as u64)
            .and_then(|bytes| bytes.checked_add(size_of::<NativePreparation<'_>>() as u64))
            .and_then(|bytes| bytes.checked_add(storage.retained_path_bytes() as u64))
            .and_then(|bytes| bytes.checked_add(permits))
            .ok_or_else(overflow)?;
        let metadata = bands.iter().try_fold(0_u64, |sum, band| {
            sum.checked_add(
                band.preparation_metadata_bytes()
                    .map_err(io::Error::other)? as u64,
            )
            .ok_or_else(overflow)
        })?;
        shared_bytes = shared_bytes.checked_add(metadata).ok_or_else(overflow)?;
        let preparation = shared_bytes
            .checked_add(store.preparation_residency()?)
            .ok_or_else(overflow)?;
        // Full-channel decoded slots conservatively bound any row-dependent
        // window. No band support needs to be guessed before source traversal.
        let (source, slot) = NativeSource::memory(store, 0..store.channels)?;
        let wave_shared = (source_slots as u64)
            .checked_mul(slot)
            .and_then(|bytes| bytes.checked_add(source))
            .and_then(|bytes| bytes.checked_add(shared_bytes))
            // WavePlan also charges the selected plans' owned support. Include
            // its eventual growth before the source pass discovers that support.
            .and_then(|bytes| bytes.checked_add(metadata))
            .ok_or_else(overflow)?;
        let full: Vec<_> = if prior_window_bytes.is_some() {
            bands.iter().map(BandPlan::full_refresh).collect()
        } else {
            bands.to_vec()
        };
        let all = WavePlan::project(
            store,
            full.iter().map(|band| (band, None)),
            workers,
            source_slots,
            wave_shared,
        )?
        .peak_bytes
        .checked_add(
            prior_window_bytes
                .unwrap_or(0)
                .checked_mul(bands.len() as u64)
                .ok_or_else(overflow)?,
        )
        .ok_or_else(overflow)?;
        let mut minimum = preparation;
        for band in &full {
            minimum = minimum.max(
                WavePlan::project(
                    store,
                    std::iter::once((band, None)),
                    workers,
                    source_slots,
                    wave_shared,
                )?
                .peak_bytes
                .checked_add(prior_window_bytes.unwrap_or(0))
                .ok_or_else(overflow)?,
            );
        }
        let available = authority
            .remaining_planning_memory_bytes(policy, base.execution_dag().resource_alternative())
            .map_err(io::Error::other)?;
        let workspace_bytes = preparation.max(all).min(available);
        if std::env::var_os("CASA_RS_TRACE_IMAGING_STAGE_TIMING").is_some() {
            eprintln!(
                "streaming_cube_phase_plan imported={} workers={workers} available_bytes={available} workspace_bytes={workspace_bytes} minimum_bytes={minimum} shared_bytes={shared_bytes} preparation_bytes={preparation} all_bands_bytes={all}",
                prior_window_bytes.is_some(),
            );
        }
        if minimum > workspace_bytes {
            return Err(io::Error::other(
                "native phase cannot fit preparation and one band",
            ));
        }
        Ok(Self {
            store,
            shared_bytes,
            workspace_bytes,
            workers,
            source_slots,
            minimum_workspace_bytes: minimum,
            read,
            reconcile,
            storage_id,
            imported: prior_window_bytes.is_some(),
            prior_window_bytes: prior_window_bytes.unwrap_or(0),
        })
    }

    fn storage_resource(&self) -> LeaseResource {
        LeaseResource::Storage {
            demand_id: self.storage_id.clone(),
            use_kind: StorageUseKind::Temporary,
        }
    }

    pub(super) fn retains_at(&self, node: &WorkNodeId) -> bool {
        !self.imported && node == &self.read
    }

    pub(super) fn retain(
        &self,
        store: &mut NativeStore,
        permit: RetainedArtifactPermit,
    ) -> io::Result<()> {
        let heap = self.metadata_allocation();
        if permit.covers_exact_immutable_allocation(&self.read, &heap) {
            if store.metadata_retention.is_some() {
                return Err(io::Error::other("native metadata retained twice"));
            }
            store.metadata_retention = Some(permit);
            return Ok(());
        }
        if store.plan != self.store
            || !permit.covers_exact_resources(&[
                (self.storage_resource(), self.store.artifact_bytes),
                (LeaseResource::FileDescriptors, 1),
            ])
        {
            return Err(io::Error::other(
                "native store capacity differs from its admitted plan",
            ));
        }
        store.retain(permit)
    }

    fn metadata_allocation(&self) -> LogicalAllocation {
        let id = format!("{}-metadata", self.storage_id);
        LogicalAllocation {
            id: AllocationId::new(&id),
            physical_slot: PhysicalSlotId::new(&id),
            bytes: self.shared_bytes,
            purpose: AllocationPurpose::Data,
            compatibility: SlotCompatibility {
                memory_domain: CapacityDomainId::new("host-memory"),
                views: BTreeSet::from([CapacityViewId::new("host-memory")]),
                alignment_bytes: 64,
                storage_mode: StorageMode::Host,
                layout: AllocationLayout::new("native-cube-metadata"),
                initialization: InitializationPolicy::OverwriteBeforeRead,
                access: AllocationAccess::ReadOnly,
            },
            lifetime: AllocationLifetime {
                acquire_at: self.read.clone(),
                release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    self.read.clone(),
                    FenceKind::Io,
                ))]),
                disposition: AllocationDisposition::ExportImmutableArtifact {
                    owner_node: self.read.clone(),
                },
            },
        }
    }

    pub(super) fn compose(
        &self,
        base: PhysicalWorkBinding,
        storage: &ManagedSpillStorage,
    ) -> io::Result<PhysicalWorkBinding> {
        let id = AllocationId::new(format!("{}-workspace", self.storage_id));
        let compatibility = SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 64,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new("native-cube-phase"),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: AllocationAccess::ReadWrite,
        };
        let workspace = self.workspace_bytes - if self.imported { 0 } else { self.shared_bytes };
        let allocation = LogicalAllocation {
            id: id.clone(),
            purpose: AllocationPurpose::Data,
            bytes: workspace,
            physical_slot: PhysicalSlotId::new(id.as_str()),
            compatibility: compatibility.clone(),
            lifetime: AllocationLifetime {
                disposition: AllocationDisposition::Release,
                acquire_at: self.read.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(self.reconcile.clone())]),
            },
        };
        let slot = PhysicalSlot {
            id: allocation.physical_slot.clone(),
            capacity_bytes: workspace,
            lease_resource: LeaseResource::Memory {
                allocation_id: id.as_str().into(),
            },
            compatibility,
        };
        let mut alternative = base.execution_dag().resource_alternative().clone();
        let queue = alternative
            .demand
            .queues
            .iter()
            .find(|queue| &queue.resource == storage.resources().queue())
            .ok_or_else(|| io::Error::other("native phase lacks composed storage queue"))?
            .demand_id
            .clone();
        let mut nodes: Vec<_> = base.execution_dag().nodes().values().cloned().collect();
        let metadata = self.metadata_allocation();
        for node in &mut nodes {
            if self.imported {
                node.claims.retain(|claim| {
                    !matches!(claim.resource, LeaseResource::MeasurementSetLock { .. })
                });
            }
            if node.id != self.read && node.id != self.reconcile {
                continue;
            }
            let lifetime = if node.fences.is_empty() {
                ClaimLifetime::Work
            } else {
                ClaimLifetime::through_fences(node.fences.iter().copied())
            };
            node.allocations.push(AllocationUse {
                allocation: id.clone(),
                lifetime: lifetime.clone(),
            });
            node.claims.push(ResourceClaim {
                resource: if node.id == self.read && !self.imported {
                    LeaseResource::StorageWriteRate {
                        demand_id: self.storage_id.clone(),
                    }
                } else {
                    LeaseResource::StorageReadRate {
                        demand_id: self.storage_id.clone(),
                    }
                },
                amount: 1,
                lifetime: lifetime.clone(),
            });
            let queue_resource = LeaseResource::Queue {
                demand_id: queue.clone(),
            };
            if !node
                .claims
                .iter()
                .any(|claim| claim.resource == queue_resource)
            {
                node.claims.push(ResourceClaim {
                    resource: queue_resource,
                    amount: 1,
                    lifetime,
                });
            }
            if node.id == self.read {
                if self.imported {
                    node.kind = WorkKind::Compute;
                    node.allocations.retain(|use_| {
                        use_.allocation.as_str() != "spectral-cycle-selected-source"
                    });
                    node.claims.retain(|claim| {
                        !matches!(
                            claim.resource,
                            LeaseResource::MeasurementSetLock { .. }
                                | LeaseResource::FileDescriptors
                                | LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
                        )
                    });
                } else {
                    node.allocations.push(AllocationUse {
                        allocation: metadata.id.clone(),
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    });
                    node.claims.extend([
                        ResourceClaim {
                            resource: self.storage_resource(),
                            amount: self.store.artifact_bytes,
                            lifetime: ClaimLifetime::Artifact,
                        },
                        ResourceClaim {
                            resource: LeaseResource::FileDescriptors,
                            amount: 1,
                            lifetime: ClaimLifetime::Artifact,
                        },
                    ]);
                }
            } else {
                node.claims
                    .retain(|claim| claim.resource != LeaseResource::Workers);
                node.claims.push(ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: self.workers as u64,
                    lifetime: ClaimLifetime::Work,
                });
            }
        }
        alternative.demand.memory.push(MemoryDemand {
            allocation_id: id.as_str().into(),
            hard_bytes: workspace,
            preferred_bytes: workspace,
            views: vec![CapacityViewId::new("host-memory")],
        });
        alternative.demand.storage.push(StorageDemand {
            demand_id: self.storage_id.clone(),
            domain: storage.resources().domain().clone(),
            temporary_bytes: if self.imported {
                0
            } else {
                self.store.artifact_bytes
            },
            staged_output_bytes: 0,
            final_output_bytes: 0,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::new(1, 1),
            write_rate: CountDemand::new(1, 1),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        });
        alternative.demand.file_descriptors = CountDemand::new(
            alternative
                .demand
                .file_descriptors
                .hard()
                .checked_add(u64::from(!self.imported))
                .ok_or_else(overflow)?,
            alternative
                .demand
                .file_descriptors
                .preferred()
                .checked_add(u64::from(!self.imported))
                .ok_or_else(overflow)?,
        );
        alternative.demand.workers = CountDemand::new(
            alternative.demand.workers.hard().max(self.workers as u64),
            alternative
                .demand
                .workers
                .preferred()
                .max(self.workers as u64),
        );
        alternative.scaling.minimum_workers = alternative.demand.workers.hard();
        alternative.scaling.maximum_workers = alternative.demand.workers.preferred();
        let mut knobs = base.execution_dag().initial_knobs().clone();
        knobs.workers = alternative.demand.workers.hard();
        let mut allocations: Vec<_> = base
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .chain([allocation])
            .collect();
        let mut slots: Vec<_> = base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .chain([slot])
            .collect();
        if !self.imported {
            alternative.demand.memory.push(MemoryDemand {
                allocation_id: metadata.id.as_str().into(),
                hard_bytes: metadata.bytes,
                preferred_bytes: metadata.bytes,
                views: vec![CapacityViewId::new("host-memory")],
            });
            slots.push(PhysicalSlot {
                id: metadata.physical_slot.clone(),
                capacity_bytes: metadata.bytes,
                lease_resource: LeaseResource::Memory {
                    allocation_id: metadata.id.as_str().into(),
                },
                compatibility: metadata.compatibility.clone(),
            });
            allocations.push(metadata);
        } else {
            allocations.retain(|a| a.id.as_str() != "spectral-cycle-selected-source");
            slots.retain(|s| s.id.as_str() != "spectral-cycle-selected-source-slot");
            alternative
                .demand
                .memory
                .retain(|d| d.allocation_id != "spectral-cycle-selected-source");
            alternative.demand.io_buffers.source_read_ahead_bytes = 0;
            alternative.demand.locks = CountDemand::zero();
        }
        // Worker stacks already belong to WavePlan's workspace projection.
        let dag = ExecutionDag::new(ExecutionDagSpecification {
            required_resource_capabilities: base
                .execution_dag()
                .required_resource_capabilities()
                .clone(),
            resource_alternative: alternative,
            nodes,
            logical_allocations: allocations,
            physical_slots: slots,
            initial_knobs: knobs,
            adaptations: base
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })
        .map_err(io::Error::other)?;
        let prediction = PlanPrediction::new(
            base.prediction().elapsed_nanos(),
            base.prediction().confidence(),
            base.prediction().uncertainty().to_vec(),
            base.prediction()
                .stages()
                .values()
                .map(|stage| {
                    if self.imported && stage.node() == &self.read {
                        stage.clone().with_io(Vec::new())
                    } else {
                        stage.clone()
                    }
                })
                .collect(),
        )
        .map_err(io::Error::other)?;
        let transaction = if self.imported {
            let transaction = base.observation_transaction();
            ObservationTransactionWork::new_source_free_reconstruction(
                transaction
                    .initial_consistency_check()
                    .ok_or_else(|| io::Error::other("native refresh lacks consistency node"))?
                    .clone(),
                self.reconcile.clone(),
                transaction.commit().clone(),
            )
            .with_final_model_preparation(
                transaction
                    .final_model_preparation()
                    .ok_or_else(|| io::Error::other("native refresh lacks model node"))?
                    .clone(),
            )
        } else {
            base.observation_transaction().clone()
        };
        PhysicalWorkBinding::with_implementation_contract(
            base.implementation_contract()
                .for_execution_dag(&dag)
                .map_err(io::Error::other)?,
            dag,
            prediction,
            base.artifacts().to_vec(),
            transaction,
            base.publication_layouts().clone(),
            base.product_publication_plan(),
        )
        .map_err(io::Error::other)
    }
}
