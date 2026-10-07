// SPDX-License-Identifier: LGPL-3.0-or-later

//! Physical ownership of channel-local model and normal generations.

use std::{
    collections::BTreeSet,
    io,
    sync::{Arc, OnceLock},
};

use crate::{
    managed_cube_blocks::{CubeResidency, ManagedPlaneArray},
    managed_model::ManagedModelFactory,
    managed_normal::ManagedNormalFactory,
    paged_cube_state::{
        CubeArrayLayout, CubeBackingMetrics, PagedModelStorageFactory, PagedNormalStorageFactory,
    },
    *,
};
use casa_imaging_model::{CompiledProblem, ModelSample};
use casa_imaging_reconstruction::{
    ModelStorageFactory, ModelStoragePlan, SpectralOperatorSpecification,
    runtime_adapter::{ChannelNormalStorageRequirement, NormalStorageFactory, NormalStoragePlan},
};

/// Reservations are attached at the scheduler's existing export boundaries.
/// Every physical backing retains this owner after its arrays and directory.
#[derive(Debug, Default)]
struct CubeStateRetention {
    capacity: OnceLock<RetainedArtifactPermit>,
    heap: OnceLock<RetainedArtifactPermit>,
}

/// One independently releasable memory allocation; shared capacity is small for
/// resident normals and remains conservative for paged file/storage resources.
#[derive(Debug)]
struct CubeBackingRetention {
    heap: OnceLock<std::sync::Mutex<RetainedArtifactPermit>>,
    _shared: Arc<CubeStateRetention>,
}

/// One retained lease and one physical cache across every cube major cycle.
pub(crate) struct ManagedCubeRun {
    pub(crate) residency: Arc<CubeResidency>,
    retention: Arc<CubeBackingRetention>,
}

impl ManagedCubeRun {
    /// Physical eviction must precede returning any part of the retained lease.
    pub(crate) fn shrink_cache_to(&self, target: usize) -> io::Result<()> {
        self.residency.shrink_to(target)?;
        self.retention
            .heap
            .get()
            .ok_or_else(|| io::Error::other("managed cache permit is not retained"))?
            .lock()
            .map_err(|_| io::Error::other("managed cache permit lock poisoned"))?
            .narrow_memory_to(target as u64)
            .map_err(io::Error::other)
    }
}

impl std::fmt::Debug for ManagedCubeRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedCubeRun")
            .field("used_bytes", &self.residency.used_bytes())
            .finish_non_exhaustive()
    }
}

/// A candidate owns a model and separate epoch/invariant normal backings.
/// Prior generations keep their own reservations while this generation is built.
#[derive(Debug)]
pub(crate) struct CubeStatePlan {
    model: Arc<dyn ModelStorageFactory>,
    normal: Arc<dyn NormalStorageFactory>,
    managed: Option<Arc<ManagedCubeRun>>,
    managed_initial: bool,
    retention: Arc<CubeStateRetention>,
    metrics: Arc<CubeBackingMetrics>,
    model_window_samples: usize,
    normal_window_channels: usize,
    acquire: WorkNodeId,
    terminal: WorkNodeId,
    heap: LogicalAllocation,
    backings: Box<[(LogicalAllocation, Arc<CubeBackingRetention>)]>,
    retained_bytes: u64,
    scratch: LogicalAllocation,
    storage_id: String,
    storage_bytes: u64,
    file_handles: u64,
}

impl CubeStatePlan {
    /// Admission floor and full-residency ceiling for two overlapping model
    /// generations, one invariant PSF and two residual epochs. All owner,
    /// registry, staging and active-operation terms use the same typed array
    /// formulas as physical creation.
    pub(crate) fn managed_cache_limits(
        problem: &CompiledProblem,
        storage: &ManagedSpillStorage,
        workers: usize,
    ) -> io::Result<(usize, usize)> {
        let shape = problem.model_lifecycle().target();
        let [domain] = shape.domains() else {
            return Err(io::Error::other("managed cube requires one image domain"));
        };
        let [width, height] = domain.pixels();
        let cells = width.checked_mul(height).ok_or_else(overflow)?;
        if cells == 0 || workers == 0 || !shape.sample_count().is_multiple_of(cells) {
            return Err(overflow());
        }
        let planes = shape.sample_count() / cells;
        Self::managed_cache_limits_for_shape(storage.directory(), height, width, planes, workers)
    }

    fn managed_cache_limits_for_shape(
        directory: &std::path::Path,
        height: usize,
        width: usize,
        planes: usize,
        workers: usize,
    ) -> io::Result<(usize, usize)> {
        let value = ManagedPlaneArray::<f32>::footprint(directory, height, width, planes)?;
        let support = ManagedPlaneArray::<bool>::footprint(directory, height, width, planes)?;
        let owner = add(
            value.owner_bytes.checked_mul(5).ok_or_else(overflow)?,
            support.owner_bytes.checked_mul(2).ok_or_else(overflow)?,
        )?;
        let registry = add(
            value.registry_bytes.checked_mul(5).ok_or_else(overflow)?,
            support.registry_bytes.checked_mul(2).ok_or_else(overflow)?,
        )?;
        let staging = add(
            value.staging_bytes.checked_mul(5).ok_or_else(overflow)?,
            support.staging_bytes.checked_mul(2).ok_or_else(overflow)?,
        )?;
        let worker_blocks = add(
            value.block_bytes.checked_mul(3).ok_or_else(overflow)?,
            support.block_bytes.checked_mul(2).ok_or_else(overflow)?,
        )?;
        let active = worker_blocks.checked_mul(workers).ok_or_else(overflow)?;
        let fixed = CubeResidency::fixed_owner_bytes();
        let operation =
            CubeResidency::operation_overhead(workers.checked_mul(5).ok_or_else(overflow)?)?;
        let normal_floor = [fixed, owner, registry, staging, active, operation]
            .into_iter()
            .try_fold(0usize, add)?;
        let creation_overlap = add(
            add(fixed, add(owner, registry)?)?,
            add(staging, value.creation_bytes.max(support.creation_bytes))?,
        )?;
        let minimum = normal_floor.max(creation_overlap);
        let payload = add(
            value
                .block_bytes
                .checked_mul(5)
                .and_then(|n| n.checked_mul(planes))
                .ok_or_else(overflow)?,
            support
                .block_bytes
                .checked_mul(2)
                .and_then(|n| n.checked_mul(planes))
                .ok_or_else(overflow)?,
        )?;
        let full = add(
            add(add(fixed, owner)?, registry)?,
            add(add(staging, payload)?, operation)?,
        )?
        .max(minimum);
        Ok((minimum, full))
    }

    /// Bind channel-local Float owners to the one lease-backed run cache.
    /// The first major reserves its storage and memory capacity; later majors
    /// reuse that retained capacity while admitting only their own workspaces.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn managed_streaming_cube(
        problem: &CompiledProblem,
        storage: &ManagedSpillStorage,
        window_channels: usize,
        acquire: WorkNodeId,
        terminal: WorkNodeId,
        run: Option<Arc<ManagedCubeRun>>,
        cache_bytes: usize,
    ) -> io::Result<Self> {
        let shape = problem.model_lifecycle().target();
        let [domain] = shape.domains() else {
            return Err(io::Error::other("managed cube requires one image domain"));
        };
        let [width, height] = domain.pixels();
        let cells = width.checked_mul(height).ok_or_else(overflow)?;
        let planes = shape.sample_count() / cells;
        if cells == 0 || planes == 0 || cells.checked_mul(planes) != Some(shape.sample_count()) {
            return Err(overflow());
        }
        let initial = run.is_none();
        let retention = run.as_ref().map_or_else(
            || Arc::new(CubeStateRetention::default()),
            |run| run.retention._shared.clone(),
        );
        let run = match run {
            Some(run) => run,
            None => Arc::new(ManagedCubeRun {
                residency: CubeResidency::new(cache_bytes)?,
                retention: Arc::new(CubeBackingRetention {
                    heap: OnceLock::new(),
                    _shared: retention.clone(),
                }),
            }),
        };
        let permit_owner: Arc<dyn std::fmt::Debug + Send + Sync> = run.clone();
        let model: Arc<dyn ModelStorageFactory> = Arc::new(ManagedModelFactory::new(
            run.residency.clone(),
            permit_owner.clone(),
            storage.directory(),
            [height, width],
            planes,
        )?);
        let normal: Arc<dyn NormalStorageFactory> = Arc::new(ManagedNormalFactory::new(
            run.residency.clone(),
            permit_owner,
            storage.directory(),
            [height, width],
        )?);
        let storage_id = format!("cube-state-storage-{}", acquire.as_str());
        let value_disk =
            ManagedPlaneArray::<f32>::footprint(storage.directory(), height, width, planes)?
                .storage_bytes;
        let support_disk =
            ManagedPlaneArray::<bool>::footprint(storage.directory(), height, width, planes)?
                .storage_bytes;
        let storage_bytes = if initial {
            let model_pair = add(value_disk, support_disk)?;
            as_u64(add(
                model_pair.checked_mul(2).ok_or_else(overflow)?,
                value_disk.checked_mul(3).ok_or_else(overflow)?,
            )?)?
        } else {
            0
        };
        let file_handles = if initial { 7 } else { 0 };
        let heap_id = format!("cube-state-retained-{}", acquire.as_str());
        let heap_bytes = size_of::<Self>()
            .checked_add(size_of::<ManagedModelFactory>())
            .and_then(|n| n.checked_add(size_of::<ManagedNormalFactory>()))
            .and_then(|n| n.checked_add(size_of::<ManagedCubeRun>()))
            .and_then(|n| n.checked_add(size_of::<CubeStateRetention>()))
            .and_then(|n| n.checked_add(size_of::<CubeBackingRetention>()))
            .and_then(|n| n.checked_add(storage.directory().as_os_str().len() * 2))
            .and_then(|n| n.checked_add(12 * size_of::<usize>()))
            .ok_or_else(overflow)?;
        let permit_bytes = if initial {
            RetainedArtifactPermit::heap_bytes_for_resources(
                &[
                    LeaseResource::Storage {
                        demand_id: storage_id.clone(),
                        use_kind: StorageUseKind::Temporary,
                    },
                    LeaseResource::FileDescriptors,
                ],
                0,
                "host-memory",
                storage.resources().domain().as_str(),
            )
            .and_then(|n| {
                RetainedArtifactPermit::heap_bytes_for_resources(
                    &[LeaseResource::Memory {
                        allocation_id: format!("cube-state-manager-{}", acquire.as_str()),
                    }],
                    1,
                    "host-memory",
                    storage.resources().domain().as_str(),
                )
                .and_then(|extra| n.checked_add(extra))
            })
            .ok_or_else(overflow)? as usize
        } else {
            0
        };
        let heap = allocation(
            heap_id,
            as_u64(add(heap_bytes, permit_bytes)?)?,
            &acquire,
            &terminal,
            initial,
        );
        let backings = if initial {
            vec![(
                allocation(
                    format!("cube-state-manager-{}", acquire.as_str()),
                    as_u64(cache_bytes)?,
                    &acquire,
                    &terminal,
                    true,
                ),
                run.retention.clone(),
            )]
            .into_boxed_slice()
        } else {
            Box::new([])
        };
        let plane_complex = cells
            .checked_mul(size_of::<num_complex::Complex64>())
            .ok_or_else(overflow)?;
        let scratch_bytes = add(
            plane_complex.checked_mul(2).ok_or_else(overflow)?,
            cells
                .checked_mul(size_of::<ModelSample>())
                .ok_or_else(overflow)?,
        )?;
        let scratch = allocation(
            format!("cube-state-access-{}", acquire.as_str()),
            as_u64(scratch_bytes)?,
            &acquire,
            &terminal,
            false,
        );
        let retained_bytes = if initial {
            heap.bytes
                .checked_add(as_u64(cache_bytes)?)
                .ok_or_else(overflow)?
        } else {
            0
        };
        Ok(Self {
            model,
            normal,
            managed: Some(run),
            managed_initial: initial,
            retention,
            metrics: Arc::new(CubeBackingMetrics::default()),
            model_window_samples: cells,
            normal_window_channels: window_channels,
            acquire,
            terminal,
            heap,
            backings,
            retained_bytes,
            scratch,
            storage_id,
            storage_bytes,
            file_handles,
        })
    }

    pub(crate) fn managed_run(&self) -> Option<Arc<ManagedCubeRun>> {
        self.managed.clone()
    }

    pub(crate) fn new(
        problem: &CompiledProblem,
        storage: &ManagedSpillStorage,
        window_channels: usize,
        acquire: WorkNodeId,
        terminal: WorkNodeId,
    ) -> io::Result<Self> {
        let specification =
            SpectralOperatorSpecification::new(problem).map_err(io::Error::other)?;
        let requirements =
            ChannelNormalStorageRequirement::for_specification(&specification, window_channels)
                .map_err(io::Error::other)?;
        Self::with_normal(
            problem,
            storage,
            window_channels,
            acquire,
            terminal,
            requirements,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_normal(
        problem: &CompiledProblem,
        storage: &ManagedSpillStorage,
        window_channels: usize,
        acquire: WorkNodeId,
        terminal: WorkNodeId,
        requirements: Box<[ChannelNormalStorageRequirement]>,
    ) -> io::Result<Self> {
        let shape = problem.model_lifecycle().target();
        let model_window_samples = shape.domains().iter().try_fold(0usize, |largest, domain| {
            let [width, height] = domain.pixels();
            width
                .checked_mul(height)
                .and_then(|n| n.checked_mul(shape.polarizations()))
                .map(|n| largest.max(n))
                .ok_or_else(overflow)
        })?;
        let retention = Arc::new(CubeStateRetention::default());
        let model_retention = Arc::new(CubeBackingRetention {
            heap: OnceLock::new(),
            _shared: retention.clone(),
        });
        let metrics = Arc::new(CubeBackingMetrics::default());
        let model_layout = if let [domain] = shape.domains() {
            let [width, height] = domain.pixels();
            CubeArrayLayout::new_spatial(
                shape.sample_count(),
                height,
                width
                    .checked_mul(shape.polarizations())
                    .ok_or_else(overflow)?,
                model_window_samples,
                1,
            )
        } else {
            CubeArrayLayout::new(
                shape.sample_count(),
                model_window_samples,
                model_window_samples,
                1,
            )
        }
        .map_err(io::Error::other)?;
        let model = Arc::new(
            PagedModelStorageFactory::new(
                storage.directory(),
                model_layout,
                model_retention.clone(),
                metrics.clone(),
            )
            .map_err(io::Error::other)?,
        );
        let model_ledger = model.ledger().map_err(io::Error::other)?;
        let layouts = requirements
            .iter()
            .map(|requirement| {
                CubeArrayLayout::new_spatial(
                    requirement.scalar_capacity(),
                    requirement.image_axes()[0],
                    requirement.image_axes()[1],
                    requirement.maximum_window_scalars(),
                    1,
                )
                .map(|layout| (requirement.allocation_ordinal(), layout))
                .map_err(io::Error::other)
            })
            .collect::<io::Result<Box<[_]>>>()?;
        let mut retained_bytes = 0usize;
        let mut backings = vec![(
            allocation(
                format!("cube-state-model-{}", acquire.as_str()),
                as_u64(model_ledger.retained_bytes)?,
                &acquire,
                &terminal,
                true,
            ),
            model_retention,
        )];
        let mut normal_retentions: Vec<(usize, Arc<dyn std::fmt::Debug + Send + Sync>)> =
            Vec::new();
        let mut storage_bytes = model_ledger.storage_bytes;
        let mut file_handles = model_ledger.file_handles;
        let mut scratch_bytes = model_ledger
            .read_scratch_bytes
            .max(model_ledger.write_scratch_bytes)
            .max(model_ledger.flush_scratch_bytes)
            .checked_add(
                model_window_samples
                    .checked_mul(size_of::<ModelSample>())
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        for ((_, layout), requirement) in layouts.iter().zip(&requirements) {
            let ledger = layout
                .normal_ledger(storage.directory())
                .map_err(io::Error::other)?;
            let normal_retention = Arc::new(CubeBackingRetention {
                heap: OnceLock::new(),
                _shared: retention.clone(),
            });
            normal_retentions.push((requirement.allocation_ordinal(), normal_retention.clone()));
            backings.push((
                allocation(
                    format!(
                        "cube-state-normal-{}-{}",
                        acquire.as_str(),
                        requirement.allocation_ordinal()
                    ),
                    as_u64(add(
                        ledger.retained_bytes,
                        requirement.retained_metadata_bytes(),
                    )?)?,
                    &acquire,
                    &terminal,
                    true,
                ),
                normal_retention,
            ));
            storage_bytes = add(storage_bytes, ledger.storage_bytes)?;
            file_handles = add(file_handles, ledger.file_handles)?;
            // The normal owner converts a complex source to f64 before the
            // backing's ndarray copy. Both coexist with the source primitive.
            let conversion = requirement
                .maximum_window_scalars()
                .checked_mul(size_of::<f64>())
                .ok_or_else(overflow)?;
            scratch_bytes = scratch_bytes.max(
                add(conversion, ledger.write_scratch_bytes)?
                    .max(ledger.read_scratch_bytes)
                    .max(ledger.flush_scratch_bytes),
            );
        }
        let factory = PagedNormalStorageFactory::new(
            storage.directory(),
            layouts,
            normal_retentions.into_boxed_slice(),
            metrics.clone(),
            false,
        );
        let normal_metadata = factory.owned_metadata_bytes().map_err(io::Error::other)?;
        let normal: Arc<dyn NormalStorageFactory> = Arc::new(factory);
        retained_bytes = add(
            retained_bytes,
            model.owned_metadata_bytes().map_err(io::Error::other)?,
        )?;
        retained_bytes = add(retained_bytes, normal_metadata)?;
        retained_bytes = add(
            retained_bytes,
            size_of::<Self>()
                + size_of::<CubeStateRetention>()
                + size_of::<CubeBackingMetrics>()
                + backings.len()
                    * (size_of::<(LogicalAllocation, Arc<CubeBackingRetention>)>()
                        + size_of::<CubeBackingRetention>()
                        + 2 * size_of::<usize>())
                + 10 * size_of::<usize>(),
        )?;
        let storage_id = format!("cube-state-storage-{}", acquire.as_str());
        let heap_id = format!("cube-state-retained-{}", acquire.as_str());
        let capacity_resources = [
            LeaseResource::Storage {
                demand_id: storage_id.clone(),
                use_kind: StorageUseKind::Temporary,
            },
            LeaseResource::FileDescriptors,
        ];
        let heap_resources = [LeaseResource::Memory {
            allocation_id: heap_id.clone(),
        }];
        let permit_bytes = RetainedArtifactPermit::heap_bytes_for_resources(
            &capacity_resources,
            0,
            "host-memory",
            storage.resources().domain().as_str(),
        )
        .and_then(|bytes| {
            RetainedArtifactPermit::heap_bytes_for_resources(
                &heap_resources,
                1,
                "host-memory",
                storage.resources().domain().as_str(),
            )
            .and_then(|other| bytes.checked_add(other))
        })
        .ok_or_else(overflow)?;
        let permit_bytes = backings
            .iter()
            .try_fold(permit_bytes, |bytes, (allocation, _)| {
                RetainedArtifactPermit::heap_bytes_for_resources(
                    &[LeaseResource::Memory {
                        allocation_id: allocation.id.as_str().to_owned(),
                    }],
                    1,
                    "host-memory",
                    storage.resources().domain().as_str(),
                )
                .and_then(|other| bytes.checked_add(other))
                .ok_or_else(overflow)
            })?;
        let retained_bytes = as_u64(retained_bytes)?
            .checked_add(permit_bytes)
            .ok_or_else(overflow)?;
        let heap = allocation(heap_id, retained_bytes, &acquire, &terminal, true);
        let retained_bytes = backings
            .iter()
            .try_fold(heap.bytes, |bytes, (allocation, _)| {
                bytes.checked_add(allocation.bytes).ok_or_else(overflow)
            })?;
        let scratch = allocation(
            format!("cube-state-access-{}", acquire.as_str()),
            as_u64(scratch_bytes)?,
            &acquire,
            &terminal,
            false,
        );
        Ok(Self {
            model,
            normal,
            managed: None,
            managed_initial: false,
            retention,
            metrics,
            model_window_samples,
            normal_window_channels: window_channels,
            acquire,
            terminal,
            heap,
            backings: backings.into_boxed_slice(),
            retained_bytes,
            scratch,
            storage_id,
            storage_bytes: as_u64(storage_bytes)?,
            file_handles: as_u64(file_handles)?,
        })
    }

    pub(crate) fn retained_memory_bytes(&self) -> u64 {
        self.retained_bytes
    }

    /// Allocation capabilities must be live before any mutable backing is made.
    pub(crate) fn model_storage(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> io::Result<ModelStoragePlan> {
        let retained_capacity = self.managed.is_some()
            && !self.managed_initial
            && self.retention.capacity.get().is_some()
            && self
                .managed
                .as_ref()
                .is_some_and(|run| run.retention.heap.get().is_some());
        if context.node().id != self.acquire
            || !context
                .node()
                .allocations
                .iter()
                .any(|use_| use_.allocation == self.heap.id)
            || self.backings.iter().any(|(allocation, _)| {
                !context
                    .node()
                    .allocations
                    .iter()
                    .any(|use_| use_.allocation == allocation.id)
            })
            || !(retained_capacity
                || context.node().claims.iter().any(|claim| {
                    claim.resource == self.storage_resource()
                        && claim.amount == self.storage_bytes
                        && claim.lifetime == ClaimLifetime::Artifact
                }))
            || !(retained_capacity
                || context.node().claims.iter().any(|claim| {
                    claim.resource == LeaseResource::FileDescriptors
                        && claim.amount == self.file_handles
                        && claim.lifetime == ClaimLifetime::Artifact
                }))
        {
            return Err(io::Error::other(
                "cube storage lacks its plan-issued allocation and capacity claims",
            ));
        }
        ModelStoragePlan::new(self.model.clone(), self.model_window_samples)
            .map_err(io::Error::other)
    }

    pub(crate) fn normal_storage(&self) -> io::Result<NormalStoragePlan> {
        if self.retention.capacity.get().is_none() {
            return Err(io::Error::other(
                "cube normal creation precedes backing-capacity retention",
            ));
        }
        NormalStoragePlan::new(self.normal.clone(), self.normal_window_channels)
            .map_err(io::Error::other)
    }

    pub(crate) fn log_measurements(&self, node: &WorkNodeId) {
        if std::env::var_os("CASA_RS_TRACE_IMAGING_STAGE_TIMING").is_some() {
            if let Some(run) = &self.managed {
                let metrics = run.residency.metrics();
                eprintln!(
                    "imaging_cube_managed node={} used_bytes={} live_payload_bytes={} limit_bytes={} peak_used_bytes={} dirty_write_operations={} dirty_write_bytes={} reload_read_operations={} reload_read_bytes={}",
                    node.as_str(),
                    run.residency.used_bytes(),
                    run.residency.live_payload_bytes(),
                    run.residency.limit_bytes(),
                    metrics.peak_used_bytes,
                    metrics.dirty_write_operations,
                    metrics.dirty_write_bytes,
                    metrics.reload_read_operations,
                    metrics.reload_read_bytes,
                );
            }
            eprintln!(
                "imaging_cube_backing_measurements node={} planned_retained_bytes={} planned_access_scratch_bytes={} planned_storage_bytes={} planned_file_handles={} observed={:?}",
                node.as_str(),
                self.retained_memory_bytes(),
                self.scratch.bytes,
                self.storage_bytes,
                self.file_handles,
                self.metrics.snapshot()
            );
        }
    }

    pub(crate) fn retains_at(&self, node: &WorkNodeId) -> bool {
        (self.managed.is_none() || self.managed_initial)
            && (node == &self.acquire || node == &self.terminal)
    }

    pub(crate) fn retain(
        &self,
        node: &WorkNodeId,
        permit: RetainedArtifactPermit,
    ) -> io::Result<()> {
        if node == &self.acquire {
            if !permit.covers_exact_resources(&[
                (self.storage_resource(), self.storage_bytes),
                (LeaseResource::FileDescriptors, self.file_handles),
            ]) {
                return Err(io::Error::other(
                    "cube backing reservation differs from its admitted capacity",
                ));
            }
            self.retention
                .capacity
                .set(permit)
                .map_err(|_| io::Error::other("cube backing capacity was retained twice"))
        } else if node == &self.terminal {
            let allocations = std::iter::once(&self.heap)
                .chain(self.backings.iter().map(|(allocation, _)| allocation))
                .collect::<Vec<_>>();
            let mut partitions = permit
                .partition_immutable_allocations(node, &allocations)
                .map_err(io::Error::other)?
                .into_iter();
            self.retention
                .heap
                .set(partitions.next().expect("shared allocation is present"))
                .map_err(|_| io::Error::other("cube backing cache was retained twice"))?;
            for ((_, retention), permit) in self.backings.iter().zip(partitions) {
                retention
                    .heap
                    .set(std::sync::Mutex::new(permit))
                    .map_err(|_| io::Error::other("cube backing allocation was retained twice"))?;
            }
            Ok(())
        } else {
            Err(io::Error::other(
                "cube backing reservation was exported by an unrelated node",
            ))
        }
    }

    fn storage_resource(&self) -> LeaseResource {
        LeaseResource::Storage {
            demand_id: self.storage_id.clone(),
            use_kind: StorageUseKind::Temporary,
        }
    }

    pub(crate) fn compose<R: ImplementationRegistry>(
        &self,
        registry: &R,
        implementation: WorkImplementationId,
        storage: &ManagedSpillStorage,
        base: PhysicalWorkBinding,
        replay: &WorkNodeId,
        reconcile: &WorkNodeId,
    ) -> Result<PhysicalWorkBinding, SpectralCyclePlanError> {
        // Cube I/O runs synchronously on the coordinator, using the existing
        // transaction queue. It does not add a concurrent reader or writer.
        let existing_queue = base
            .execution_dag()
            .resource_alternative()
            .demand
            .queues
            .iter()
            .find(|queue| &queue.resource == storage.resources().queue())
            .map(|queue| queue.demand_id.clone());
        let new_queue = existing_queue.is_none().then(|| QueueDemand {
            demand_id: format!("{}-queue", self.storage_id),
            resource: storage.resources().queue().clone(),
            slots: CountDemand::new(1, 1),
        });
        let queue = LeaseResource::Queue {
            demand_id: existing_queue.unwrap_or_else(|| {
                new_queue
                    .as_ref()
                    .expect("new queue needed")
                    .demand_id
                    .clone()
            }),
        };
        let mut nodes = base
            .execution_dag()
            .nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let users = BTreeSet::from([
            self.acquire.clone(),
            replay.clone(),
            reconcile.clone(),
            self.terminal.clone(),
        ]);
        for node in &mut nodes {
            if !users.contains(&node.id) {
                continue;
            }
            let lifetime = if node.fences.contains(&FenceKind::Io) {
                ClaimLifetime::through_fence(FenceKind::Io)
            } else {
                ClaimLifetime::Work
            };
            node.allocations.extend([
                AllocationUse {
                    allocation: self.heap.id.clone(),
                    lifetime: lifetime.clone(),
                },
                AllocationUse {
                    allocation: self.scratch.id.clone(),
                    lifetime: lifetime.clone(),
                },
            ]);
            node.allocations
                .extend(self.backings.iter().map(|(allocation, _)| AllocationUse {
                    allocation: allocation.id.clone(),
                    lifetime: lifetime.clone(),
                }));
            node.claims.extend([
                ResourceClaim {
                    resource: LeaseResource::StorageReadRate {
                        demand_id: self.storage_id.clone(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::StorageWriteRate {
                        demand_id: self.storage_id.clone(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
            ]);
            if !node.claims.iter().any(|claim| claim.resource == queue) {
                node.claims.push(ResourceClaim {
                    resource: queue.clone(),
                    amount: 1,
                    lifetime,
                });
            }
            if node.id == self.acquire && self.storage_bytes != 0 {
                node.claims.extend([
                    ResourceClaim {
                        resource: self.storage_resource(),
                        amount: self.storage_bytes,
                        lifetime: ClaimLifetime::Artifact,
                    },
                    ResourceClaim {
                        resource: LeaseResource::FileDescriptors,
                        amount: self.file_handles,
                        lifetime: ClaimLifetime::Artifact,
                    },
                ]);
            }
        }
        let mut alternative = base.execution_dag().resource_alternative().clone();
        alternative.demand.queues.extend(new_queue);
        for allocation in [&self.heap, &self.scratch]
            .into_iter()
            .chain(self.backings.iter().map(|(allocation, _)| allocation))
        {
            alternative.demand.memory.push(MemoryDemand {
                allocation_id: allocation.id.as_str().to_owned(),
                hard_bytes: allocation.bytes,
                preferred_bytes: allocation.bytes,
                views: vec![CapacityViewId::new("host-memory")],
            });
        }
        alternative.demand.file_descriptors = CountDemand::new(
            alternative
                .demand
                .file_descriptors
                .hard()
                .checked_add(self.file_handles)
                .ok_or(SpectralCyclePlanError::Overflow)?,
            alternative
                .demand
                .file_descriptors
                .preferred()
                .checked_add(self.file_handles)
                .ok_or(SpectralCyclePlanError::Overflow)?,
        );
        alternative.demand.storage.push(StorageDemand {
            demand_id: self.storage_id.clone(),
            domain: storage.resources().domain().clone(),
            temporary_bytes: self.storage_bytes,
            staged_output_bytes: 0,
            final_output_bytes: 0,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::new(1, 1),
            write_rate: CountDemand::new(1, 1),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        });
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
                .chain([self.heap.clone(), self.scratch.clone()])
                .chain(
                    self.backings
                        .iter()
                        .map(|(allocation, _)| allocation.clone()),
                )
                .collect(),
            physical_slots: base
                .execution_dag()
                .physical_slots()
                .values()
                .cloned()
                .chain(
                    [&self.heap, &self.scratch]
                        .into_iter()
                        .chain(self.backings.iter().map(|(allocation, _)| allocation))
                        .map(|allocation| PhysicalSlot {
                            id: allocation.physical_slot.clone(),
                            lease_resource: LeaseResource::Memory {
                                allocation_id: allocation.id.as_str().to_owned(),
                            },
                            capacity_bytes: allocation.bytes,
                            compatibility: allocation.compatibility.clone(),
                        }),
                )
                .collect(),
            initial_knobs: base.execution_dag().initial_knobs().clone(),
            adaptations: base
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })?;
        let catalog = ImplementationContractCatalog::from_registry(registry, [implementation])?;
        Ok(PhysicalWorkBinding::new_reconstruction(
            catalog,
            dag,
            base.prediction().clone(),
            base.artifacts().to_vec(),
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
        )?)
    }
}

fn allocation(
    id: String,
    bytes: u64,
    acquire: &WorkNodeId,
    terminal: &WorkNodeId,
    retained: bool,
) -> LogicalAllocation {
    LogicalAllocation {
        physical_slot: PhysicalSlotId::new(format!("{id}-slot")),
        id: AllocationId::new(id),
        bytes,
        purpose: AllocationPurpose::Data,
        compatibility: SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 64,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new(if retained {
                "cube-state-cache-and-metadata"
            } else {
                "cube-state-access"
            }),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: if retained {
                AllocationAccess::ReadOnly
            } else {
                AllocationAccess::ReadWrite
            },
        },
        lifetime: AllocationLifetime {
            acquire_at: acquire.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(terminal.clone())]),
            disposition: if retained {
                AllocationDisposition::ExportImmutableArtifact {
                    owner_node: terminal.clone(),
                }
            } else {
                AllocationDisposition::Release
            },
        },
    }
}

fn overflow() -> io::Error {
    io::Error::other("cube state storage layout overflow")
}
fn add(left: usize, right: usize) -> io::Result<usize> {
    left.checked_add(right).ok_or_else(overflow)
}
fn as_u64(value: usize) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| overflow())
}

#[cfg(test)]
mod managed_cache_tests {
    use super::*;

    #[test]
    fn full_cube_cache_ceiling_exceeds_active_four_worker_floor() {
        let (minimum, full) = CubeStatePlan::managed_cache_limits_for_shape(
            std::path::Path::new("cube"),
            512,
            512,
            512,
            4,
        )
        .unwrap();
        eprintln!("review2_cache_formula minimum_bytes={minimum} full_bytes={full}");
        assert!(minimum < full);
        assert!(minimum < 16 << 30);
    }

    #[test]
    fn large_spatial_working_plane_does_not_require_channel_count_payload_residency() {
        let directory = std::path::Path::new("cube");
        let (short_floor, short_full) =
            CubeStatePlan::managed_cache_limits_for_shape(directory, 2048, 2048, 64, 4).unwrap();
        let (long_floor, long_full) =
            CubeStatePlan::managed_cache_limits_for_shape(directory, 2048, 2048, 2048, 4).unwrap();
        let plane_bytes = 2048 * 2048 * size_of::<f32>();
        assert!(short_floor >= 4 * plane_bytes);
        assert!(long_floor < short_floor + 32 * plane_bytes);
        assert!(long_full > short_full * 16);
        assert!(long_floor < 16 << 30);
    }
}
