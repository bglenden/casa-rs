// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pure, deterministic imaging execution planning.
//!
//! This module owns formulas only. Application code supplies workload facts,
//! an explicitly assigned resource slice, and user policy; imaging algorithms
//! consume the resulting immutable plan without consulting process state.

use std::fmt;

/// Exact workload facts needed to plan an imaging run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImagingWorkloadShape {
    /// Selected visibility rows.
    pub selected_rows: usize,
    /// Correlation products per visibility sample.
    pub correlations: usize,
    /// Selected spectral channels.
    pub channels: usize,
    /// Logical output image width.
    pub image_width: usize,
    /// Logical output image height.
    pub image_height: usize,
    /// Simultaneously resident output image planes.
    pub image_planes: usize,
    /// Padded Fourier-grid width.
    pub grid_width: usize,
    /// Padded Fourier-grid height.
    pub grid_height: usize,
    /// Simultaneously resident Fourier-grid planes.
    pub grid_planes: usize,
    /// Number of Taylor terms for MT-MFS work.
    pub taylor_terms: usize,
    /// Number of multiscale components.
    pub scales: usize,
    /// Number of mosaic facets or pointings handled together.
    pub facets: usize,
    /// Maximum gridding-kernel halo in cells.
    pub kernel_halo: usize,
    /// Bytes read from the source for one row.
    pub source_bytes_per_row: usize,
    /// Bytes retained after preparing one row.
    pub prepared_bytes_per_row: usize,
    /// Per-worker scratch requirement in bytes.
    pub worker_scratch_bytes: usize,
    /// Bytes per output image element.
    pub image_element_bytes: usize,
    /// Bytes per Fourier-grid element.
    pub grid_element_bytes: usize,
    /// Scratch bytes needed to transform one FFT plane.
    pub fft_bytes_per_plane: usize,
    /// Persistent spectral state retained per output plane.
    pub spectral_state_bytes_per_plane: usize,
    /// Total routed samples in the run.
    pub sample_count: usize,
    /// Metal staging bytes per routed sample.
    pub metal_bytes_per_sample: usize,
    /// Exact always-live application allocations not derivable from the
    /// generic image/grid shape (for example weighting density or product
    /// writer state). These are charged before any optional execution cache.
    pub fixed_allocations: Vec<ImagingMemoryAllocation>,
    /// Bytes required by a replay cache when the workload can reuse it.
    pub routed_replay_cache_candidate_bytes: usize,
    /// Bytes required by a grouped Metal input cache when eligible.
    pub metal_grouped_input_cache_candidate_bytes: usize,
    /// Bytes required by a materialized sample plan when useful.
    pub materialized_sample_plan_candidate_bytes: usize,
    /// Maximum useful host scratch for direct Metal gridding.
    pub direct_metal_scratch_candidate_bytes: usize,
    /// Bytes retained by one bounded tile-queue entry.
    pub tile_queue_entry_bytes: usize,
}

impl ImagingWorkloadShape {
    /// Returns checked row-channel work units.
    pub fn work_units(&self) -> Result<usize, ImagingPlanError> {
        self.selected_rows
            .checked_mul(self.channels.max(1))
            .ok_or(ImagingPlanError::Overflow("work units"))
    }
}

/// Machine resources assigned to this run by the application runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingResources {
    /// Process memory slice assigned to this operation.
    pub usable_memory_bytes: usize,
    /// CPU workers assigned to this operation.
    pub cpu_capacity: usize,
    /// Whether an eligible Metal device is available.
    pub metal_available: bool,
    /// Device-memory slice assigned to this operation.
    pub metal_device_budget_bytes: usize,
}

/// Explicit user limits and preferences. `None` means the pure planner may
/// derive the value from workload and assigned resources.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImagingExecutionPolicy {
    /// Optional user cap within the assigned process-memory slice.
    pub memory_limit_bytes: Option<usize>,
    /// Optional user worker cap.
    pub worker_limit: Option<usize>,
    /// Optional ingest batch-row cap.
    pub ingest_batch_rows_limit: Option<usize>,
    /// Optional source row-block cap.
    pub source_row_block_rows_limit: Option<usize>,
    /// Optional cap on concurrently live source blocks.
    pub max_live_row_blocks: Option<usize>,
    /// Optional FFT plane-chunk cap.
    pub fft_chunk_count_limit: Option<usize>,
    /// Optional fixed tile edge.
    pub tile_edge: Option<usize>,
    /// Optional cap on resident tiles.
    pub tile_resident_count_limit: Option<usize>,
    /// Fixed-tile partition anchor.
    pub tile_anchor: ImagingTileAnchor,
    /// Whether the user explicitly prefers Metal execution.
    pub prefer_metal: bool,
    /// Optional Metal command sample cap.
    pub metal_command_samples_limit: Option<usize>,
    /// Allow a replay cache when the workload supplies a non-zero candidate.
    pub allow_routed_replay_cache: bool,
    /// Allow a grouped Metal input cache when the workload supplies one.
    pub allow_metal_grouped_input_cache: bool,
    /// Allow a materialized CPU sample plan when the workload supplies one.
    pub allow_materialized_sample_plan: bool,
    /// Optional cap on direct-Metal host scratch.
    pub direct_metal_scratch_limit_bytes: Option<usize>,
}

/// Origin of a resolved choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagingPlanOrigin {
    /// Derived from workload dimensions.
    Workload,
    /// Derived from the assigned resource slice.
    Resources,
    /// Set by explicit user policy.
    UserPolicy,
}

/// Human-readable explanation for one resolved choice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingPlanDecision {
    /// Stable decision name.
    pub name: &'static str,
    /// Resolved value suitable for diagnostics.
    pub value: String,
    /// Authority that determined the value.
    pub origin: ImagingPlanOrigin,
    /// Formula or override explanation.
    pub reason: String,
}

/// One item in the run memory ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingMemoryAllocation {
    /// Buffer or state component.
    pub component: &'static str,
    /// Execution stage in which the allocation is live.
    pub stage: &'static str,
    /// Planned resident bytes.
    pub bytes: usize,
}

/// Resolved ingest batching and source-residency decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingIngestPlan {
    /// Rows prepared in one ingest batch.
    pub batch_rows: usize,
    /// Rows read in one source block.
    pub source_row_block_rows: usize,
    /// Maximum concurrently live source blocks.
    pub max_live_row_blocks: usize,
    /// Bytes in one source row block.
    pub source_row_block_bytes: usize,
}

/// Resolved FFT chunk geometry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingFftChunkPlan {
    /// Planes transformed in one chunk.
    pub chunk_planes: usize,
    /// Resident scratch bytes for the chunk.
    pub chunk_bytes: usize,
}

/// Resolved fixed-tile geometry and residency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingTilePlan {
    /// Tile-grid anchor selected for partitioning.
    pub anchor: ImagingTileAnchor,
    /// Interior tile edge in cells.
    pub edge: usize,
    /// Kernel halo around each tile in cells.
    pub halo: usize,
    /// Bytes in one padded tile.
    pub padded_tile_bytes: usize,
    /// Maximum concurrently resident tiles.
    pub resident_tiles: usize,
    /// Total resident tile bytes.
    pub resident_bytes: usize,
    /// Maximum queued samples retained between source-block flushes.
    pub queue_capacity: usize,
    /// Per-tile sample count that makes queued work schedulable.
    pub ready_sample_threshold: usize,
    /// Whether the scheduler must drain queued work after every source block.
    pub flush_after_source_block: bool,
}

/// Fixed-tile partition anchor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ImagingTileAnchor {
    /// Anchor tiles at grid coordinate zero.
    Zero,
    /// Put a tile boundary through the Fourier-grid center.
    #[default]
    CenterBoundary,
    /// Use four center-boundary quadrants.
    CenterQuadrants,
}

/// Resolved scheduling granularity for spectral products.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImagingSpectralSchedule {
    /// Execute one spectral plane at a time.
    SinglePlane,
    /// Read the source once and retain bounded output-plane state.
    SourceFirst {
        /// Planes resident while consuming the source.
        planes: usize,
    },
    /// Retain a bounded source cache while executing plane slabs.
    Hybrid {
        /// Planes in one execution slab.
        planes: usize,
    },
    /// Execute a bounded slab of spectral planes.
    Slab {
        /// Planes in one slab.
        planes: usize,
    },
}

impl ImagingSpectralSchedule {
    /// Number of simultaneously active spectral planes.
    pub const fn active_planes(&self) -> usize {
        match self {
            Self::SinglePlane => 1,
            Self::SourceFirst { planes } | Self::Hybrid { planes } | Self::Slab { planes } => {
                *planes
            }
        }
    }
}

/// Resolved Metal eligibility and staging limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingMetalPlan {
    /// Whether Metal is both requested and resource-feasible.
    pub eligible: bool,
    /// Routed samples submitted in one command.
    pub command_samples: usize,
    /// Maximum device-resident staging cache bytes.
    pub device_cache_bytes: usize,
    /// Explanation when Metal is not eligible.
    pub rejection_reason: Option<String>,
}

/// Optional execution caches and scratch selected from the same run ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingCachePlan {
    /// General application storage/cache budget admitted by the run ledger.
    pub storage_cache_bytes: usize,
    /// Whether replayable routed samples remain resident between passes.
    pub routed_replay_enabled: bool,
    /// Charged routed replay bytes.
    pub routed_replay_bytes: usize,
    /// Whether grouped Metal inputs remain resident between passes.
    pub metal_grouped_input_enabled: bool,
    /// Charged grouped Metal input bytes.
    pub metal_grouped_input_bytes: usize,
    /// Charged materialized sample-plan bytes.
    pub materialized_sample_plan_bytes: usize,
    /// Charged direct-Metal host scratch bytes.
    pub direct_metal_scratch_bytes: usize,
}

/// Fully resolved, immutable execution decisions for one imaging run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingResolvedPlan {
    /// Exact workload facts from which the plan was resolved.
    pub workload: ImagingWorkloadShape,
    /// Effective process-memory budget for this run.
    pub usable_memory_bytes: usize,
    /// CPU workers used by parallel stages.
    pub workers: usize,
    /// Rows assigned to one worker partition.
    pub worker_partition_rows: usize,
    /// Ingest decisions.
    pub ingest: ImagingIngestPlan,
    /// FFT decisions.
    pub fft: ImagingFftChunkPlan,
    /// Fixed-tile decisions.
    pub tile: ImagingTilePlan,
    /// Spectral scheduling decisions.
    pub spectral: ImagingSpectralSchedule,
    /// Metal decisions.
    pub metal: ImagingMetalPlan,
    /// Optional caches and backend scratch selected from the run budget.
    pub caches: ImagingCachePlan,
    /// Itemized stage memory ledger.
    pub memory_allocations: Vec<ImagingMemoryAllocation>,
    /// Maximum planned resident bytes at any stage.
    pub maximum_planned_resident_bytes: usize,
    /// Human-readable provenance for resolved choices.
    pub decisions: Vec<ImagingPlanDecision>,
}

/// Exact schedule selected by an application-level I/O model and submitted to
/// the shared planner for resource admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingPlanAdmission {
    /// Workload facts represented by this schedule.
    pub workload: ImagingWorkloadShape,
    /// Process-memory slice assigned to the operation.
    pub usable_memory_bytes: usize,
    /// Concurrent workers in the admitted schedule.
    pub workers: usize,
    /// Rows assigned to one worker partition.
    pub worker_partition_rows: usize,
    /// Ingest schedule.
    pub ingest: ImagingIngestPlan,
    /// FFT schedule.
    pub fft: ImagingFftChunkPlan,
    /// Tile schedule.
    pub tile: ImagingTilePlan,
    /// Spectral schedule.
    pub spectral: ImagingSpectralSchedule,
    /// Metal schedule.
    pub metal: ImagingMetalPlan,
    /// Optional caches charged to the operation.
    pub caches: ImagingCachePlan,
    /// Itemized resident allocations.
    pub memory_allocations: Vec<ImagingMemoryAllocation>,
    /// Exact maximum simultaneous residency computed by the schedule model.
    pub maximum_planned_resident_bytes: usize,
    /// Provenance for schedule choices.
    pub decisions: Vec<ImagingPlanDecision>,
}

/// Admit an exact application-selected schedule into the canonical immutable
/// execution-plan contract.
///
/// Applications may model storage I/O and product-writing costs that the pure
/// shape planner cannot observe. They still submit the resulting schedule here
/// so one contract validates resource bounds and drives every core consumer.
pub fn admit_imaging_execution(
    admission: ImagingPlanAdmission,
) -> Result<ImagingResolvedPlan, ImagingPlanError> {
    if admission.usable_memory_bytes == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "usable memory budget must be positive",
        ));
    }
    if admission.workers == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "worker count must be positive",
        ));
    }
    if admission.workload.selected_rows > 0
        && (admission.ingest.batch_rows == 0
            || admission.ingest.source_row_block_rows == 0
            || admission.ingest.max_live_row_blocks == 0)
    {
        return Err(ImagingPlanError::InvalidInput(
            "non-empty workloads require a non-empty ingest schedule",
        ));
    }
    if admission.workload.image_planes > 0 && admission.fft.chunk_planes == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "image workloads require a non-empty FFT schedule",
        ));
    }
    require_fits(
        "admitted schedule",
        admission.maximum_planned_resident_bytes,
        admission.usable_memory_bytes,
    )?;
    Ok(ImagingResolvedPlan {
        workload: admission.workload,
        usable_memory_bytes: admission.usable_memory_bytes,
        workers: admission.workers,
        worker_partition_rows: admission.worker_partition_rows,
        ingest: admission.ingest,
        fft: admission.fft,
        tile: admission.tile,
        spectral: admission.spectral,
        metal: admission.metal,
        caches: admission.caches,
        memory_allocations: admission.memory_allocations,
        maximum_planned_resident_bytes: admission.maximum_planned_resident_bytes,
        decisions: admission.decisions,
    })
}

impl ImagingResolvedPlan {
    #[cfg(test)]
    pub(crate) fn idle() -> Self {
        Self {
            workload: ImagingWorkloadShape::default(),
            usable_memory_bytes: 0,
            workers: 1,
            worker_partition_rows: 0,
            ingest: ImagingIngestPlan {
                batch_rows: 0,
                source_row_block_rows: 0,
                max_live_row_blocks: 0,
                source_row_block_bytes: 0,
            },
            fft: ImagingFftChunkPlan {
                chunk_planes: 0,
                chunk_bytes: 0,
            },
            tile: ImagingTilePlan {
                anchor: ImagingTileAnchor::CenterBoundary,
                edge: 0,
                halo: 0,
                padded_tile_bytes: 0,
                resident_tiles: 0,
                resident_bytes: 0,
                queue_capacity: 0,
                ready_sample_threshold: 1,
                flush_after_source_block: false,
            },
            spectral: ImagingSpectralSchedule::SinglePlane,
            metal: ImagingMetalPlan {
                eligible: false,
                command_samples: 0,
                device_cache_bytes: 0,
                rejection_reason: Some("no workload was assigned".to_string()),
            },
            caches: ImagingCachePlan {
                storage_cache_bytes: 0,
                routed_replay_enabled: false,
                routed_replay_bytes: 0,
                metal_grouped_input_enabled: false,
                metal_grouped_input_bytes: 0,
                materialized_sample_plan_bytes: 0,
                direct_metal_scratch_bytes: 0,
            },
            memory_allocations: Vec::new(),
            maximum_planned_resident_bytes: 0,
            decisions: Vec::new(),
        }
    }

    /// Return the charged bytes for a named component.
    pub fn allocation_bytes(&self, component: &str) -> usize {
        self.memory_allocations
            .iter()
            .filter(|allocation| allocation.component == component)
            .map(|allocation| allocation.bytes)
            .sum()
    }
}

#[cfg(test)]
impl Default for ImagingResolvedPlan {
    fn default() -> Self {
        Self::idle()
    }
}

/// Failures returned by deterministic imaging planning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImagingPlanError {
    /// A required workload or resource value was invalid.
    InvalidInput(&'static str),
    /// Checked size arithmetic overflowed.
    Overflow(&'static str),
    /// A stage cannot fit within its assigned memory slice.
    InsufficientMemory {
        /// Stage whose required resident set does not fit.
        stage: &'static str,
        /// Minimum required bytes.
        required_bytes: usize,
        /// Assigned bytes.
        budget_bytes: usize,
    },
}

impl fmt::Display for ImagingPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => {
                write!(formatter, "invalid imaging plan input: {message}")
            }
            Self::Overflow(component) => {
                write!(
                    formatter,
                    "imaging planning overflow while computing {component}"
                )
            }
            Self::InsufficientMemory {
                stage,
                required_bytes,
                budget_bytes,
            } => write!(
                formatter,
                "imaging stage {stage} needs {required_bytes} bytes but only {budget_bytes} bytes were assigned"
            ),
        }
    }
}

impl std::error::Error for ImagingPlanError {}

fn checked_product(
    values: impl IntoIterator<Item = usize>,
    name: &'static str,
) -> Result<usize, ImagingPlanError> {
    values
        .into_iter()
        .try_fold(1usize, |product, value| product.checked_mul(value))
        .ok_or(ImagingPlanError::Overflow(name))
}

fn checked_sum(
    values: impl IntoIterator<Item = usize>,
    name: &'static str,
) -> Result<usize, ImagingPlanError> {
    values
        .into_iter()
        .try_fold(0usize, |sum, value| sum.checked_add(value))
        .ok_or(ImagingPlanError::Overflow(name))
}

fn require_fits(
    stage: &'static str,
    required_bytes: usize,
    budget_bytes: usize,
) -> Result<(), ImagingPlanError> {
    if required_bytes <= budget_bytes {
        Ok(())
    } else {
        Err(ImagingPlanError::InsufficientMemory {
            stage,
            required_bytes,
            budget_bytes,
        })
    }
}

fn integer_sqrt(value: usize) -> usize {
    if value < 2 {
        return value;
    }
    let mut low = 1usize;
    let mut high = value.min(usize::MAX / 2 + 1);
    while low + 1 < high {
        let middle = low + (high - low) / 2;
        if middle <= value / middle {
            low = middle;
        } else {
            high = middle;
        }
    }
    low
}

fn tile_axis_count(length: usize, edge: usize, anchor: ImagingTileAnchor) -> usize {
    let edge = edge.max(1);
    match anchor {
        ImagingTileAnchor::Zero => length.max(1).div_ceil(edge),
        ImagingTileAnchor::CenterBoundary | ImagingTileAnchor::CenterQuadrants => {
            let origin = (length / 2) % edge;
            if origin == 0 {
                length.max(1).div_ceil(edge)
            } else if length <= origin {
                1
            } else {
                1 + (length - origin).div_ceil(edge)
            }
        }
    }
}

/// Builds one deterministic plan from explicit workload, resource, and policy
/// inputs. No environment, host, or device query occurs here.
pub fn plan_imaging_execution(
    workload: &ImagingWorkloadShape,
    resources: &ImagingResources,
    policy: &ImagingExecutionPolicy,
) -> Result<ImagingResolvedPlan, ImagingPlanError> {
    if resources.cpu_capacity == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "cpu capacity must be positive",
        ));
    }
    if resources.usable_memory_bytes == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "usable memory budget must be positive",
        ));
    }
    let usable_memory_bytes = policy
        .memory_limit_bytes
        .unwrap_or(resources.usable_memory_bytes)
        .min(resources.usable_memory_bytes);
    if usable_memory_bytes == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "effective memory budget must be positive",
        ));
    }

    let image_cells =
        checked_product([workload.image_width, workload.image_height], "image cells")?;
    let grid_cells = checked_product([workload.grid_width, workload.grid_height], "grid cells")?;
    let image_bytes = checked_product(
        [
            image_cells,
            workload.image_planes,
            workload.image_element_bytes,
        ],
        "image resident bytes",
    )?;
    let grid_bytes = checked_product(
        [
            grid_cells,
            workload.grid_planes,
            workload.grid_element_bytes,
        ],
        "grid resident bytes",
    )?;
    let application_fixed_bytes =
        workload
            .fixed_allocations
            .iter()
            .try_fold(0usize, |sum, allocation| {
                sum.checked_add(allocation.bytes)
                    .ok_or(ImagingPlanError::Overflow("application fixed allocations"))
            })?;
    let fixed_bytes = checked_sum(
        [image_bytes, grid_bytes, application_fixed_bytes],
        "fixed resident bytes",
    )?;
    require_fits("fixed", fixed_bytes, usable_memory_bytes)?;

    let work_units = workload.work_units()?;
    let topology_worker_cap = match (policy.tile_anchor, policy.tile_edge) {
        (ImagingTileAnchor::CenterQuadrants, _) => {
            let edge = workload
                .grid_width
                .div_ceil(2)
                .max(workload.grid_height.div_ceil(2));
            tile_axis_count(workload.grid_width, edge, policy.tile_anchor)
                .checked_mul(tile_axis_count(
                    workload.grid_height,
                    edge,
                    policy.tile_anchor,
                ))
                .ok_or(ImagingPlanError::Overflow("topology worker cap"))?
        }
        (_, Some(edge)) => tile_axis_count(workload.grid_width, edge, policy.tile_anchor)
            .checked_mul(tile_axis_count(
                workload.grid_height,
                edge,
                policy.tile_anchor,
            ))
            .ok_or(ImagingPlanError::Overflow("topology worker cap"))?,
        _ => work_units.max(1),
    };
    let requested_workers = policy
        .worker_limit
        .unwrap_or(resources.cpu_capacity)
        .min(resources.cpu_capacity)
        .min(work_units.max(1))
        .min(topology_worker_cap.max(1));
    if requested_workers == 0 {
        return Err(ImagingPlanError::InvalidInput(
            "worker limit must be positive",
        ));
    }
    let state_charge = workload.worker_scratch_bytes.max(1);
    let memory_workers = usable_memory_bytes.saturating_sub(fixed_bytes) / state_charge;
    let workers = requested_workers.min(memory_workers.max(1));
    let worker_bytes = workers
        .checked_mul(workload.worker_scratch_bytes)
        .ok_or(ImagingPlanError::Overflow("worker scratch bytes"))?;
    let fixed_with_workers = fixed_bytes
        .checked_add(worker_bytes)
        .ok_or(ImagingPlanError::Overflow("fixed and worker bytes"))?;
    require_fits("workers", fixed_with_workers, usable_memory_bytes)?;

    let row_bytes = workload
        .source_bytes_per_row
        .checked_add(workload.prepared_bytes_per_row)
        .ok_or(ImagingPlanError::Overflow("row bytes"))?
        .max(1);
    let fft_plane_bytes = workload.fft_bytes_per_plane.max(1);
    let tile_plane_bytes = workload
        .grid_planes
        .checked_mul(workload.grid_element_bytes)
        .ok_or(ImagingPlanError::Overflow("tile cell bytes"))?
        .max(1);
    let minimum_dynamic_bytes = row_bytes.max(fft_plane_bytes).max(tile_plane_bytes);
    let mut optional_capacity = usable_memory_bytes
        .saturating_sub(fixed_with_workers)
        .saturating_sub(minimum_dynamic_bytes);

    let materialized_sample_plan_bytes = if policy.allow_materialized_sample_plan
        && workload.materialized_sample_plan_candidate_bytes <= optional_capacity
    {
        let bytes = workload.materialized_sample_plan_candidate_bytes;
        optional_capacity = optional_capacity.saturating_sub(bytes);
        bytes
    } else {
        0
    };
    let metal_grouped_input_bytes = if policy.allow_metal_grouped_input_cache
        && policy.prefer_metal
        && resources.metal_available
        && workload.metal_grouped_input_cache_candidate_bytes <= optional_capacity
    {
        let bytes = workload.metal_grouped_input_cache_candidate_bytes;
        optional_capacity = optional_capacity.saturating_sub(bytes);
        bytes
    } else {
        0
    };
    let routed_replay_bytes = if policy.allow_routed_replay_cache
        && workload.routed_replay_cache_candidate_bytes <= optional_capacity
    {
        let bytes = workload.routed_replay_cache_candidate_bytes;
        optional_capacity = optional_capacity.saturating_sub(bytes);
        bytes
    } else {
        0
    };
    let direct_metal_scratch_bytes = if policy.prefer_metal && resources.metal_available {
        policy
            .direct_metal_scratch_limit_bytes
            .unwrap_or(workload.direct_metal_scratch_candidate_bytes)
            .min(workload.direct_metal_scratch_candidate_bytes)
            .min(optional_capacity)
    } else {
        0
    };
    let optional_fixed_bytes = checked_sum(
        [
            materialized_sample_plan_bytes,
            metal_grouped_input_bytes,
            routed_replay_bytes,
            direct_metal_scratch_bytes,
        ],
        "optional execution allocations",
    )?;
    let fixed_with_execution = fixed_with_workers
        .checked_add(optional_fixed_bytes)
        .ok_or(ImagingPlanError::Overflow("fixed execution allocations"))?;
    let available_for_rows = usable_memory_bytes.saturating_sub(fixed_with_execution);
    let requested_live_row_blocks = policy.max_live_row_blocks.unwrap_or(workers).max(1);
    let max_live_row_blocks = requested_live_row_blocks
        .min((available_for_rows / row_bytes).max(1))
        .min(workload.selected_rows.max(1));
    let row_block_capacity = available_for_rows / max_live_row_blocks / row_bytes;
    let mut source_row_block_rows = policy
        .source_row_block_rows_limit
        .unwrap_or(workload.selected_rows.max(1))
        .min(workload.selected_rows.max(1))
        .min(row_block_capacity);
    if workload.selected_rows > 0 && source_row_block_rows == 0 {
        return Err(ImagingPlanError::InsufficientMemory {
            stage: "ingest",
            required_bytes: fixed_with_execution
                .checked_add(row_bytes)
                .ok_or(ImagingPlanError::Overflow("minimum ingest bytes"))?,
            budget_bytes: usable_memory_bytes,
        });
    }
    let fft_capacity = usable_memory_bytes.saturating_sub(fixed_with_execution) / fft_plane_bytes;
    let fft_chunk_planes = policy
        .fft_chunk_count_limit
        .unwrap_or(workload.image_planes.max(1))
        .min(workload.image_planes.max(1))
        .min(fft_capacity);
    if workload.image_planes > 0 && fft_chunk_planes == 0 {
        return Err(ImagingPlanError::InsufficientMemory {
            stage: "fft",
            required_bytes: fixed_with_execution
                .checked_add(fft_plane_bytes)
                .ok_or(ImagingPlanError::Overflow("minimum fft bytes"))?,
            budget_bytes: usable_memory_bytes,
        });
    }
    let fft_chunk_bytes = fft_chunk_planes
        .checked_mul(fft_plane_bytes)
        .ok_or(ImagingPlanError::Overflow("fft chunk bytes"))?;
    let fft_peak = fixed_with_execution
        .checked_add(fft_chunk_bytes)
        .ok_or(ImagingPlanError::Overflow("fft stage peak"))?;
    require_fits("fft", fft_peak, usable_memory_bytes)?;

    let tile_enabled = workload.tile_queue_entry_bytes > 0;
    let tile_budget = usable_memory_bytes.saturating_sub(fixed_with_execution);
    let cells_per_worker = tile_budget / workers.max(1) / tile_plane_bytes;
    let derived_padded_edge = integer_sqrt(cells_per_worker);
    let halo_width = workload
        .kernel_halo
        .checked_mul(2)
        .ok_or(ImagingPlanError::Overflow("tile halo width"))?;
    let derived_edge = derived_padded_edge
        .saturating_sub(halo_width)
        .min(workload.grid_width.max(1))
        .min(workload.grid_height.max(1));
    let derived_edge = if policy.tile_anchor == ImagingTileAnchor::CenterQuadrants {
        workload
            .grid_width
            .div_ceil(2)
            .max(workload.grid_height.div_ceil(2))
    } else {
        derived_edge
    };
    let tile_edge = policy
        .tile_edge
        .unwrap_or(derived_edge)
        .min(workload.grid_width.max(workload.grid_height).max(1));
    if grid_cells > 0 && tile_edge == 0 {
        return Err(ImagingPlanError::InsufficientMemory {
            stage: "tile",
            required_bytes: fixed_with_execution
                .checked_add(tile_plane_bytes)
                .ok_or(ImagingPlanError::Overflow("minimum tile bytes"))?,
            budget_bytes: usable_memory_bytes,
        });
    }
    let padded_edge = tile_edge
        .checked_add(halo_width)
        .ok_or(ImagingPlanError::Overflow("padded tile edge"))?;
    let padded_tile_bytes = checked_product(
        [padded_edge, padded_edge, tile_plane_bytes],
        "padded tile bytes",
    )?;
    let tiles_x = tile_axis_count(workload.grid_width, tile_edge, policy.tile_anchor);
    let tiles_y = tile_axis_count(workload.grid_height, tile_edge, policy.tile_anchor);
    let tile_count = tiles_x
        .checked_mul(tiles_y)
        .ok_or(ImagingPlanError::Overflow("tile count"))?;
    let resident_capacity = tile_budget / padded_tile_bytes.max(1);
    let resident_tiles = if tile_enabled {
        policy
            .tile_resident_count_limit
            .unwrap_or(tile_count)
            .min(tile_count)
            .min(resident_capacity)
    } else {
        0
    };
    if tile_enabled && tile_count > 0 && resident_tiles == 0 {
        return Err(ImagingPlanError::InsufficientMemory {
            stage: "tile",
            required_bytes: fixed_with_execution
                .checked_add(padded_tile_bytes)
                .ok_or(ImagingPlanError::Overflow("minimum resident tile bytes"))?,
            budget_bytes: usable_memory_bytes,
        });
    }
    let resident_tile_bytes = resident_tiles
        .checked_mul(padded_tile_bytes)
        .ok_or(ImagingPlanError::Overflow("resident tile bytes"))?;
    let queue_entry_bytes = workload.tile_queue_entry_bytes;
    let samples_per_row = if workload.selected_rows == 0 {
        0
    } else {
        workload.sample_count.div_ceil(workload.selected_rows)
    };
    let queued_bytes_per_row = samples_per_row
        .checked_mul(queue_entry_bytes)
        .ok_or(ImagingPlanError::Overflow("queued bytes per source row"))?;
    if queued_bytes_per_row > 0 {
        source_row_block_rows = source_row_block_rows.min(
            tile_budget
                .saturating_sub(resident_tile_bytes)
                .checked_div(queued_bytes_per_row)
                .unwrap_or(0),
        );
        if workload.selected_rows > 0 && source_row_block_rows == 0 {
            return Err(ImagingPlanError::InsufficientMemory {
                stage: "tile queue",
                required_bytes: fixed_with_execution
                    .checked_add(resident_tile_bytes)
                    .and_then(|bytes| bytes.checked_add(queued_bytes_per_row))
                    .ok_or(ImagingPlanError::Overflow("minimum tile queue bytes"))?,
                budget_bytes: usable_memory_bytes,
            });
        }
    }
    let source_row_block_bytes = source_row_block_rows
        .checked_mul(row_bytes)
        .ok_or(ImagingPlanError::Overflow("source row block bytes"))?;
    let live_row_bytes = source_row_block_bytes
        .checked_mul(max_live_row_blocks)
        .ok_or(ImagingPlanError::Overflow("live row block bytes"))?;
    let batch_rows = policy
        .ingest_batch_rows_limit
        .unwrap_or(source_row_block_rows)
        .min(source_row_block_rows);
    let ingest_peak = checked_sum([fixed_with_execution, live_row_bytes], "ingest stage peak")?;
    require_fits("ingest", ingest_peak, usable_memory_bytes)?;

    let queue_capacity = if queue_entry_bytes == 0 {
        0
    } else {
        source_row_block_rows
            .checked_mul(samples_per_row)
            .ok_or(ImagingPlanError::Overflow("tile queue sample capacity"))?
            .min(work_units)
    };
    let queue_bytes = queue_capacity
        .checked_mul(queue_entry_bytes)
        .ok_or(ImagingPlanError::Overflow("tile queue bytes"))?;
    let ready_sample_threshold = if queue_capacity == 0 {
        0
    } else {
        workload
            .channels
            .max(1)
            .checked_mul(resident_tiles.max(1))
            .and_then(|value| value.checked_mul(workers.max(1)))
            .ok_or(ImagingPlanError::Overflow("tile ready sample threshold"))?
            .min(queue_capacity)
    };
    let tile_peak = checked_sum(
        [fixed_with_execution, resident_tile_bytes, queue_bytes],
        "tile stage peak",
    )?;
    require_fits("tile", tile_peak, usable_memory_bytes)?;

    let spectral_capacity = usable_memory_bytes
        .saturating_sub(fixed_with_execution)
        .checked_div(workload.spectral_state_bytes_per_plane.max(1))
        .unwrap_or(0)
        .min(workload.image_planes.max(1));
    let spectral = if spectral_capacity >= workload.image_planes.max(1) {
        ImagingSpectralSchedule::SinglePlane
    } else {
        ImagingSpectralSchedule::Slab {
            planes: spectral_capacity.max(1),
        }
    };

    let metal = if !policy.prefer_metal {
        ImagingMetalPlan {
            eligible: false,
            command_samples: 0,
            device_cache_bytes: 0,
            rejection_reason: Some("CPU execution was requested".to_string()),
        }
    } else if !resources.metal_available {
        ImagingMetalPlan {
            eligible: false,
            command_samples: 0,
            device_cache_bytes: 0,
            rejection_reason: Some("no Metal device was assigned".to_string()),
        }
    } else {
        let per_sample = workload.metal_bytes_per_sample.max(1);
        let command_capacity = resources
            .metal_device_budget_bytes
            .saturating_sub(grid_bytes)
            / per_sample;
        let command_samples = policy
            .metal_command_samples_limit
            .unwrap_or(workload.sample_count)
            .min(workload.sample_count)
            .min(command_capacity);
        if workload.sample_count > 0 && command_samples == 0 {
            ImagingMetalPlan {
                eligible: false,
                command_samples: 0,
                device_cache_bytes: 0,
                rejection_reason: Some(format!(
                    "device budget {} cannot hold the grid and one sample lane",
                    resources.metal_device_budget_bytes
                )),
            }
        } else {
            ImagingMetalPlan {
                eligible: true,
                command_samples,
                device_cache_bytes: command_samples
                    .checked_mul(per_sample)
                    .ok_or(ImagingPlanError::Overflow("Metal device cache bytes"))?,
                rejection_reason: None,
            }
        }
    };

    let maximum_planned_resident_bytes = ingest_peak.max(fft_peak).max(tile_peak);
    require_fits(
        "run peak",
        maximum_planned_resident_bytes,
        usable_memory_bytes,
    )?;
    let worker_partition_rows = workload.selected_rows.div_ceil(workers.max(1));
    let resolved_tile_edge = if tile_enabled { tile_edge } else { 0 };

    let mut memory_allocations = vec![
        ImagingMemoryAllocation {
            component: "image planes",
            stage: "run",
            bytes: image_bytes,
        },
        ImagingMemoryAllocation {
            component: "grids",
            stage: "run",
            bytes: grid_bytes,
        },
        ImagingMemoryAllocation {
            component: "worker scratch",
            stage: "grid",
            bytes: worker_bytes,
        },
        ImagingMemoryAllocation {
            component: "source row blocks",
            stage: "ingest",
            bytes: live_row_bytes,
        },
        ImagingMemoryAllocation {
            component: "FFT chunks",
            stage: "fft",
            bytes: fft_chunk_bytes,
        },
        ImagingMemoryAllocation {
            component: "resident tiles",
            stage: "grid",
            bytes: resident_tile_bytes,
        },
        ImagingMemoryAllocation {
            component: "tile queue",
            stage: "grid",
            bytes: queue_bytes,
        },
    ];
    memory_allocations.extend(workload.fixed_allocations.iter().cloned());
    memory_allocations.extend([
        ImagingMemoryAllocation {
            component: "materialized sample plan",
            stage: "run",
            bytes: materialized_sample_plan_bytes,
        },
        ImagingMemoryAllocation {
            component: "Metal grouped input cache",
            stage: "run",
            bytes: metal_grouped_input_bytes,
        },
        ImagingMemoryAllocation {
            component: "routed replay cache",
            stage: "run",
            bytes: routed_replay_bytes,
        },
        ImagingMemoryAllocation {
            component: "direct Metal host scratch",
            stage: "grid",
            bytes: direct_metal_scratch_bytes,
        },
    ]);
    let decisions = vec![
        ImagingPlanDecision {
            name: "usable_memory_bytes",
            value: usable_memory_bytes.to_string(),
            origin: if policy.memory_limit_bytes.is_some() {
                ImagingPlanOrigin::UserPolicy
            } else {
                ImagingPlanOrigin::Resources
            },
            reason: "bounded by the process resource slice assigned to this run".to_string(),
        },
        ImagingPlanDecision {
            name: "workers",
            value: workers.to_string(),
            origin: if policy.worker_limit == Some(workers) {
                ImagingPlanOrigin::UserPolicy
            } else {
                ImagingPlanOrigin::Resources
            },
            reason: format!(
                "bounded by {} work units, {} CPUs, and {} bytes per worker",
                work_units, resources.cpu_capacity, state_charge
            ),
        },
        ImagingPlanDecision {
            name: "source_row_block_rows",
            value: source_row_block_rows.to_string(),
            origin: if policy.source_row_block_rows_limit.is_some() {
                ImagingPlanOrigin::UserPolicy
            } else {
                ImagingPlanOrigin::Workload
            },
            reason: format!(
                "derived from {} bytes per row and {} simultaneously live blocks",
                row_bytes, max_live_row_blocks
            ),
        },
        ImagingPlanDecision {
            name: "tile_edge",
            value: resolved_tile_edge.to_string(),
            origin: if policy.tile_edge.is_some() {
                ImagingPlanOrigin::UserPolicy
            } else {
                ImagingPlanOrigin::Workload
            },
            reason: if tile_enabled {
                format!(
                    "derived from grid {}x{}, halo {}, workers {}, and the remaining tile budget",
                    workload.grid_width, workload.grid_height, workload.kernel_halo, workers
                )
            } else {
                "unused because this workload has no bounded tile queue".to_string()
            },
        },
        ImagingPlanDecision {
            name: "tile_anchor",
            value: format!("{:?}", policy.tile_anchor),
            origin: ImagingPlanOrigin::UserPolicy,
            reason: "resolved once at the application configuration boundary".to_string(),
        },
        ImagingPlanDecision {
            name: "tile_queue_capacity",
            value: queue_capacity.to_string(),
            origin: ImagingPlanOrigin::Workload,
            reason: if tile_enabled {
                format!(
                    "holds at most one admitted source block at {} bytes per queued sample",
                    queue_entry_bytes
                )
            } else {
                "zero because the full-grid topology has no tile queue".to_string()
            },
        },
        ImagingPlanDecision {
            name: "tile_ready_sample_threshold",
            value: ready_sample_threshold.to_string(),
            origin: ImagingPlanOrigin::Workload,
            reason: if tile_enabled {
                format!(
                    "derived from {} channels, {} resident tiles, and {} workers",
                    workload.channels.max(1),
                    resident_tiles,
                    workers
                )
            } else {
                "zero because the full-grid topology has no tile scheduler".to_string()
            },
        },
        ImagingPlanDecision {
            name: "flush_after_source_block",
            value: (queue_capacity > 0).to_string(),
            origin: ImagingPlanOrigin::Workload,
            reason: "owned by the admitted tile-queue topology".to_string(),
        },
        ImagingPlanDecision {
            name: "metal",
            value: if metal.eligible {
                "eligible"
            } else {
                "rejected"
            }
            .to_string(),
            origin: if policy.prefer_metal {
                ImagingPlanOrigin::Resources
            } else {
                ImagingPlanOrigin::UserPolicy
            },
            reason: metal.rejection_reason.clone().unwrap_or_else(|| {
                format!(
                    "{} samples fit the assigned device budget",
                    metal.command_samples
                )
            }),
        },
    ];

    Ok(ImagingResolvedPlan {
        workload: workload.clone(),
        usable_memory_bytes,
        workers,
        worker_partition_rows,
        ingest: ImagingIngestPlan {
            batch_rows,
            source_row_block_rows,
            max_live_row_blocks,
            source_row_block_bytes,
        },
        fft: ImagingFftChunkPlan {
            chunk_planes: fft_chunk_planes,
            chunk_bytes: fft_chunk_bytes,
        },
        tile: ImagingTilePlan {
            anchor: policy.tile_anchor,
            edge: resolved_tile_edge,
            halo: workload.kernel_halo,
            padded_tile_bytes,
            resident_tiles,
            resident_bytes: resident_tile_bytes,
            queue_capacity,
            ready_sample_threshold,
            flush_after_source_block: queue_capacity > 0,
        },
        spectral,
        metal,
        caches: ImagingCachePlan {
            storage_cache_bytes: 0,
            routed_replay_enabled: routed_replay_bytes > 0,
            routed_replay_bytes,
            metal_grouped_input_enabled: metal_grouped_input_bytes > 0,
            metal_grouped_input_bytes,
            materialized_sample_plan_bytes,
            direct_metal_scratch_bytes,
        },
        memory_allocations,
        maximum_planned_resident_bytes,
        decisions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload() -> ImagingWorkloadShape {
        ImagingWorkloadShape {
            selected_rows: 4096,
            correlations: 2,
            channels: 64,
            image_width: 1024,
            image_height: 1024,
            image_planes: 4,
            grid_width: 1280,
            grid_height: 1280,
            grid_planes: 2,
            taylor_terms: 1,
            scales: 1,
            facets: 1,
            kernel_halo: 3,
            source_bytes_per_row: 4096,
            prepared_bytes_per_row: 2048,
            worker_scratch_bytes: 8 * 1024 * 1024,
            image_element_bytes: 4,
            grid_element_bytes: 16,
            fft_bytes_per_plane: 32 * 1024 * 1024,
            spectral_state_bytes_per_plane: 24 * 1024 * 1024,
            sample_count: 4096 * 64,
            metal_bytes_per_sample: 64,
            fixed_allocations: Vec::new(),
            routed_replay_cache_candidate_bytes: 0,
            metal_grouped_input_cache_candidate_bytes: 0,
            materialized_sample_plan_candidate_bytes: 0,
            direct_metal_scratch_candidate_bytes: 0,
            tile_queue_entry_bytes: 64,
        }
    }

    fn resources(memory: usize) -> ImagingResources {
        ImagingResources {
            usable_memory_bytes: memory,
            cpu_capacity: 8,
            metal_available: true,
            metal_device_budget_bytes: memory / 2,
        }
    }

    #[test]
    fn deterministic_matrix_covers_cpu_cube_mosaic_mtmfs_and_metal_shapes() {
        let base = workload();
        for mut case in [
            base.clone(),
            ImagingWorkloadShape {
                image_planes: 1,
                ..base.clone()
            },
            ImagingWorkloadShape {
                facets: 4,
                ..base.clone()
            },
            ImagingWorkloadShape {
                taylor_terms: 3,
                ..base.clone()
            },
            ImagingWorkloadShape {
                channels: 1,
                ..base.clone()
            },
        ] {
            case.grid_planes = case.taylor_terms.max(1) * case.facets.max(1);
            let plan = plan_imaging_execution(
                &case,
                &resources(2 * 1024 * 1024 * 1024),
                &ImagingExecutionPolicy {
                    prefer_metal: true,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(plan.workers <= 8);
            assert!(plan.maximum_planned_resident_bytes <= plan.usable_memory_bytes);
            assert!(!plan.memory_allocations.is_empty());
            assert!(!plan.decisions.is_empty());
        }
    }

    #[test]
    fn explicit_limits_win_and_remain_within_budget() {
        let plan = plan_imaging_execution(
            &workload(),
            &resources(2 * 1024 * 1024 * 1024),
            &ImagingExecutionPolicy {
                memory_limit_bytes: Some(1024 * 1024 * 1024),
                worker_limit: Some(3),
                ingest_batch_rows_limit: Some(17),
                source_row_block_rows_limit: Some(31),
                max_live_row_blocks: Some(2),
                fft_chunk_count_limit: Some(2),
                tile_edge: Some(48),
                tile_resident_count_limit: Some(5),
                tile_anchor: ImagingTileAnchor::Zero,
                prefer_metal: false,
                metal_command_samples_limit: None,
                allow_routed_replay_cache: false,
                allow_metal_grouped_input_cache: false,
                allow_materialized_sample_plan: false,
                direct_metal_scratch_limit_bytes: None,
            },
        )
        .unwrap();
        assert_eq!(plan.usable_memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(plan.workers, 3);
        assert_eq!(plan.ingest.batch_rows, 17);
        assert_eq!(plan.ingest.source_row_block_rows, 31);
        assert_eq!(plan.tile.edge, 48);
        assert!(!plan.metal.eligible);
    }

    #[test]
    fn quadrant_tiles_bound_workers_and_flush_one_source_block() {
        let plan = plan_imaging_execution(
            &workload(),
            &resources(2 * 1024 * 1024 * 1024),
            &ImagingExecutionPolicy {
                worker_limit: Some(8),
                tile_anchor: ImagingTileAnchor::CenterQuadrants,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(plan.workers, 4);
        assert_eq!(plan.tile.resident_tiles, 4);
        assert_eq!(plan.tile.ready_sample_threshold, 64 * 4 * 4);
        assert!(plan.tile.flush_after_source_block);
        assert_eq!(
            plan.tile.queue_capacity,
            plan.ingest.source_row_block_rows * 64
        );
        assert_eq!(
            plan.allocation_bytes("tile queue"),
            plan.tile.queue_capacity * workload().tile_queue_entry_bytes
        );
        assert!(plan.maximum_planned_resident_bytes <= plan.usable_memory_bytes);
    }

    #[test]
    fn full_grid_workload_does_not_charge_or_flush_an_unused_tile_queue() {
        let mut full_grid = workload();
        full_grid.tile_queue_entry_bytes = 0;
        let plan = plan_imaging_execution(
            &full_grid,
            &resources(2 * 1024 * 1024 * 1024),
            &ImagingExecutionPolicy::default(),
        )
        .unwrap();

        assert_eq!(plan.tile.edge, 0);
        assert_eq!(plan.tile.resident_tiles, 0);
        assert_eq!(plan.tile.resident_bytes, 0);
        assert_eq!(plan.tile.queue_capacity, 0);
        assert_eq!(plan.tile.ready_sample_threshold, 0);
        assert!(!plan.tile.flush_after_source_block);
        assert_eq!(plan.allocation_bytes("resident tiles"), 0);
        assert_eq!(plan.allocation_bytes("tile queue"), 0);
        assert!(plan.maximum_planned_resident_bytes <= plan.usable_memory_bytes);
    }

    #[test]
    fn insufficient_and_overflow_inputs_are_errors() {
        let error = plan_imaging_execution(
            &workload(),
            &resources(1024),
            &ImagingExecutionPolicy::default(),
        )
        .unwrap_err();
        assert!(matches!(error, ImagingPlanError::InsufficientMemory { .. }));

        let mut overflow = workload();
        overflow.image_width = usize::MAX;
        overflow.image_height = 2;
        assert!(matches!(
            plan_imaging_execution(
                &overflow,
                &resources(usize::MAX),
                &ImagingExecutionPolicy::default(),
            ),
            Err(ImagingPlanError::Overflow(_))
        ));
    }
}
