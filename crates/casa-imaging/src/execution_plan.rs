// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pure, deterministic imaging execution planning.
//!
//! This module owns formulas only. Application code supplies workload facts,
//! an explicitly assigned resource slice, and user policy; imaging algorithms
//! consume the resulting immutable plan without consulting process state.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

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

/// Memory-pressure behavior selected for one imaging run.
///
/// The policy describes how an application assigned the resource slice. The
/// pure planner never creates swap headroom by itself; in particular,
/// [`Self::AutoSafe`] preserves the existing bounded, no-intentional-swap
/// behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ImagingMemoryPressurePolicy {
    /// Use an automatically assigned conservative resource slice.
    #[default]
    AutoSafe,
    /// Require the assigned slice to fit current no-swap headroom.
    ConservativeNoSwap,
    /// Use nearly all physical memory and permit compression or modest swap.
    AggressiveMemoryUse,
    /// Deliberately exceed physical-memory headroom for a bounded experiment.
    IntentionalOversubscription,
    /// Release, demote, spill, and prefetch allocations using stage lifetimes.
    StageAwareRelease,
    /// Combine high utilization with explicit next-use-aware eviction.
    Hybrid,
}

impl ImagingMemoryPressurePolicy {
    /// Stable diagnostic label used by task and benchmark receipts.
    pub const fn label(self) -> &'static str {
        match self {
            Self::AutoSafe => "auto",
            Self::ConservativeNoSwap => "conservative-no-swap",
            Self::AggressiveMemoryUse => "aggressive",
            Self::IntentionalOversubscription => "oversubscribe",
            Self::StageAwareRelease => "stage-aware",
            Self::Hybrid => "hybrid",
        }
    }

    /// Whether selecting this policy explicitly authorizes planned swap use.
    pub const fn permits_intentional_swap(self) -> bool {
        matches!(
            self,
            Self::AggressiveMemoryUse | Self::IntentionalOversubscription | Self::Hybrid
        )
    }
}

/// Resource-admission action selected by one memory-pressure policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagingMemoryAdmissionAction {
    /// Automatically use current no-swap headroom.
    AutomaticNoSwapHeadroom,
    /// Explicitly require current no-swap headroom.
    NoSwapHeadroom,
    /// Use the process share of installed physical memory.
    PhysicalProcessCeiling,
    /// Require an explicit target that may exceed physical-memory headroom.
    ExplicitOversubscriptionTarget,
}

impl ImagingMemoryAdmissionAction {
    /// Stable diagnostic label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::AutomaticNoSwapHeadroom => "automatic-no-swap-headroom",
            Self::NoSwapHeadroom => "no-swap-headroom",
            Self::PhysicalProcessCeiling => "physical-process-ceiling",
            Self::ExplicitOversubscriptionTarget => "explicit-oversubscription-target",
        }
    }
}

/// Swap-pressure action selected by one memory-pressure policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagingMemorySwapAction {
    /// Avoid intentional compression or swap dependence.
    AvoidIntentionalSwap,
    /// Use physical memory aggressively and allow compression or incidental swap.
    AllowCompressionOrIncidentalSwap,
    /// Deliberately exceed current physical-memory headroom.
    IntentionalOversubscription,
}

impl ImagingMemorySwapAction {
    /// Stable diagnostic label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::AvoidIntentionalSwap => "avoid-intentional-swap",
            Self::AllowCompressionOrIncidentalSwap => "allow-compression-or-incidental-swap",
            Self::IntentionalOversubscription => "intentional-oversubscription",
        }
    }
}

/// Host and device facts detected before planning.
///
/// Every field is optional because some platforms cannot report all facts.
/// These values are evidence used to assign [`ImagingResources`]; they do not
/// silently enlarge the explicitly assigned resource slice.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImagingDetectedResources {
    /// Installed physical memory.
    pub physical_memory_bytes: Option<usize>,
    /// Current memory headroom before expected compression or swapping.
    pub current_memory_headroom_bytes: Option<usize>,
    /// Physical footprint of the process before this run.
    pub process_physical_footprint_bytes: Option<usize>,
    /// Logical CPU threads visible to the process.
    pub logical_cpu_threads: Option<usize>,
    /// Performance-oriented CPU cores available to the process.
    pub performance_cpu_cores: Option<usize>,
    /// Metal device recommended maximum working-set size.
    pub metal_recommended_working_set_bytes: Option<usize>,
    /// Metal device allocation observed before this run.
    pub metal_current_allocated_bytes: Option<usize>,
    /// Unified-memory bytes reserved for concurrent CPU/GPU use.
    pub unified_memory_requirement_bytes: Option<usize>,
    /// Measured sequential read bandwidth of the selected spill volume.
    pub storage_read_bytes_per_second: Option<u64>,
    /// Measured sequential write bandwidth of the selected spill volume.
    pub storage_write_bytes_per_second: Option<u64>,
}

/// Resource evidence and memory-pressure intent supplied to the pure planner.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImagingPlanningContext {
    /// Selected memory-pressure behavior.
    pub memory_pressure_policy: ImagingMemoryPressurePolicy,
    /// Detected host, device, and storage facts.
    pub detected_resources: ImagingDetectedResources,
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

/// Ordered execution stages used by the imaging memory lifetime ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImagingMemoryStage {
    /// Construct indexes, convolution functions, and execution inputs.
    Prepare,
    /// Read and prepare bounded source visibility blocks.
    SourceIngest,
    /// Derive natural, uniform, or Briggs visibility weights.
    Weighting,
    /// Grid the initial dirty image and PSF.
    InitialGrid,
    /// Transform and normalize the initial dirty image and PSF.
    DirtyTransform,
    /// Select and subtract components in a minor cycle.
    MinorCycle,
    /// Transform model terms for prediction.
    ModelTransform,
    /// Grid a major-cycle residual refresh.
    ResidualGrid,
    /// Transform and normalize a major-cycle residual refresh.
    ResidualTransform,
    /// Restore the model and derive Taylor-term products.
    Finish,
    /// Materialize the output product set.
    ProductMaterialization,
    /// Persist output products.
    ProductWrite,
}

impl ImagingMemoryStage {
    /// Stages in execution and lifetime order.
    pub const ORDERED: [Self; 12] = [
        Self::Prepare,
        Self::SourceIngest,
        Self::Weighting,
        Self::InitialGrid,
        Self::DirtyTransform,
        Self::MinorCycle,
        Self::ModelTransform,
        Self::ResidualGrid,
        Self::ResidualTransform,
        Self::Finish,
        Self::ProductMaterialization,
        Self::ProductWrite,
    ];

    /// Stable diagnostic label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::SourceIngest => "source-ingest",
            Self::Weighting => "weighting",
            Self::InitialGrid => "initial-grid",
            Self::DirtyTransform => "dirty-transform",
            Self::MinorCycle => "minor-cycle",
            Self::ModelTransform => "model-transform",
            Self::ResidualGrid => "residual-grid",
            Self::ResidualTransform => "residual-transform",
            Self::Finish => "finish",
            Self::ProductMaterialization => "product-materialization",
            Self::ProductWrite => "product-write",
        }
    }
}

/// Actual replay-retention action currently implemented by the runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagingReplayRetentionAction {
    /// Retain a deterministic source-order subset without eviction.
    PinnedNoEvictionSourceOrder,
}

impl ImagingReplayRetentionAction {
    /// Stable diagnostic label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::PinnedNoEvictionSourceOrder => "pinned-no-eviction-source-order",
        }
    }
}

/// Immutable runtime-action receipt resolved once from a memory-pressure policy.
///
/// Requested-but-not-yet-active actions are recorded separately from actual
/// runtime behavior so planner evidence cannot imply that replay spill,
/// product streaming, or storage demotion already exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImagingMemoryRuntimeActionReceipt {
    /// Policy from which this receipt was resolved.
    pub pressure_policy: ImagingMemoryPressurePolicy,
    /// Resource slice used for admission.
    pub admission_action: ImagingMemoryAdmissionAction,
    /// Permitted swap-pressure behavior.
    pub swap_action: ImagingMemorySwapAction,
    /// Whether the selected policy requests stage-lifetime-directed release.
    pub stage_lifetime_release_requested: bool,
    /// Whether the selected policy requests next-use-aware replay selection.
    pub next_use_aware_replay_requested: bool,
    /// First production stage at which replay programs may become resident.
    pub replay_prime_stage: ImagingMemoryStage,
    /// Replay-retention behavior actually implemented by the runtime.
    pub replay_retention_action: ImagingReplayRetentionAction,
    /// Whether known last-use drops already present in the runtime are active.
    pub known_last_use_release_active: bool,
    /// Whether owned product streaming is active.
    pub product_streaming_active: bool,
    /// Whether replay programs are spilled to external storage.
    pub replay_spill_active: bool,
    /// Whether any allocation is demoted to external storage by this plan.
    pub storage_demotion_active: bool,
}

impl ImagingMemoryRuntimeActionReceipt {
    /// Resolve the current truthful runtime actions for `pressure_policy`.
    pub const fn resolve(pressure_policy: ImagingMemoryPressurePolicy) -> Self {
        let (admission_action, swap_action) = match pressure_policy {
            ImagingMemoryPressurePolicy::AutoSafe => (
                ImagingMemoryAdmissionAction::AutomaticNoSwapHeadroom,
                ImagingMemorySwapAction::AvoidIntentionalSwap,
            ),
            ImagingMemoryPressurePolicy::ConservativeNoSwap
            | ImagingMemoryPressurePolicy::StageAwareRelease => (
                ImagingMemoryAdmissionAction::NoSwapHeadroom,
                ImagingMemorySwapAction::AvoidIntentionalSwap,
            ),
            ImagingMemoryPressurePolicy::AggressiveMemoryUse
            | ImagingMemoryPressurePolicy::Hybrid => (
                ImagingMemoryAdmissionAction::PhysicalProcessCeiling,
                ImagingMemorySwapAction::AllowCompressionOrIncidentalSwap,
            ),
            ImagingMemoryPressurePolicy::IntentionalOversubscription => (
                ImagingMemoryAdmissionAction::ExplicitOversubscriptionTarget,
                ImagingMemorySwapAction::IntentionalOversubscription,
            ),
        };
        Self {
            pressure_policy,
            admission_action,
            swap_action,
            stage_lifetime_release_requested: matches!(
                pressure_policy,
                ImagingMemoryPressurePolicy::StageAwareRelease
                    | ImagingMemoryPressurePolicy::Hybrid
            ),
            next_use_aware_replay_requested: matches!(
                pressure_policy,
                ImagingMemoryPressurePolicy::Hybrid
            ),
            replay_prime_stage: ImagingMemoryStage::ResidualGrid,
            replay_retention_action: ImagingReplayRetentionAction::PinnedNoEvictionSourceOrder,
            known_last_use_release_active: true,
            product_streaming_active: false,
            replay_spill_active: false,
            storage_demotion_active: false,
        }
    }
}

/// Storage backing for one resident interval in an allocation lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImagingMemoryBacking {
    /// Ordinary CPU heap or anonymous virtual memory.
    HostHeap,
    /// CPU/GPU shared memory charged to the unified physical-memory pool.
    UnifiedMemory,
    /// Metal-private device allocation.
    MetalPrivate,
    /// Memory-mapped product or replay data.
    MemoryMapped,
    /// Temporary external-storage spill.
    TemporarySpill,
}

impl ImagingMemoryBacking {
    /// Backings in deterministic receipt order.
    pub const ORDERED: [Self; 5] = [
        Self::HostHeap,
        Self::UnifiedMemory,
        Self::MetalPrivate,
        Self::MemoryMapped,
        Self::TemporarySpill,
    ];
}

/// Next-use fact used to choose release, demotion, or prefetch behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagingMemoryNextUse {
    /// The allocation is dead after this resident interval.
    NoFurtherUse,
    /// The next use occurs at a known later execution stage.
    AtStage(ImagingMemoryStage),
    /// The allocation participates in a repeated cyclic access sequence.
    Cyclic {
        /// Stage containing the next access.
        next_stage: ImagingMemoryStage,
        /// Other logical allocations visited before this one is used again.
        intervening_uses: usize,
    },
}

/// One contiguous resident interval for a logical allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingMemoryResidency {
    /// Storage backing used during this interval.
    pub backing: ImagingMemoryBacking,
    /// Bytes expected to be physically resident during this interval.
    pub resident_bytes: usize,
    /// Logical bytes stored by this backing without counting them as resident.
    pub stored_bytes: usize,
    /// First stage in which the allocation is resident.
    pub live_from: ImagingMemoryStage,
    /// Last stage in which the allocation is resident.
    pub live_through: ImagingMemoryStage,
    /// Known use after this interval ends.
    pub next_use: ImagingMemoryNextUse,
}

impl ImagingMemoryResidency {
    fn includes(&self, stage: ImagingMemoryStage) -> bool {
        self.live_from <= stage && stage <= self.live_through
    }
}

/// Stable logical allocation and its one or more residency intervals.
///
/// Multiple non-overlapping intervals model stage-aware demotion and reload
/// without changing the allocation identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingMemoryAllocationLifecycle {
    /// Stable allocation identity used by planner, telemetry, and receipts.
    pub allocation_id: String,
    /// Human-readable component name.
    pub component: String,
    /// Logical allocation size, including bytes that may be spilled.
    pub logical_bytes: usize,
    /// Ordered resident intervals for this logical allocation.
    pub residencies: Vec<ImagingMemoryResidency>,
}

/// Resident and stored bytes for one backing at one stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingMemoryBackingBytes {
    /// Storage backing.
    pub backing: ImagingMemoryBacking,
    /// Simultaneously resident bytes.
    pub bytes: usize,
    /// Simultaneously stored bytes that are not charged as process residency.
    pub stored_bytes: usize,
}

/// Exact overlap receipt for one execution stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagingMemoryStagePeak {
    /// Execution stage.
    pub stage: ImagingMemoryStage,
    /// Total simultaneous resident bytes across all backings.
    pub resident_bytes: usize,
    /// Total simultaneously stored bytes across all backings.
    pub stored_bytes: usize,
    /// Resident and stored bytes itemized by backing.
    pub backing_bytes: Vec<ImagingMemoryBackingBytes>,
}

impl ImagingMemoryStagePeak {
    /// Return resident bytes charged to one backing.
    pub fn bytes_for_backing(&self, backing: ImagingMemoryBacking) -> usize {
        self.backing_bytes
            .iter()
            .find(|entry| entry.backing == backing)
            .map_or(0, |entry| entry.bytes)
    }

    /// Return stored, non-resident bytes charged to one backing.
    pub fn stored_bytes_for_backing(&self, backing: ImagingMemoryBacking) -> usize {
        self.backing_bytes
            .iter()
            .find(|entry| entry.backing == backing)
            .map_or(0, |entry| entry.stored_bytes)
    }
}

/// Explicit allocation lifetimes and their computed stage-overlap peaks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImagingMemoryLifetimeLedger {
    /// Stable logical allocations.
    pub allocations: Vec<ImagingMemoryAllocationLifecycle>,
    /// Per-stage simultaneous residency.
    pub stage_peaks: Vec<ImagingMemoryStagePeak>,
    /// Maximum simultaneous residency across all stages.
    pub maximum_resident_bytes: usize,
    /// Maximum simultaneous external-storage footprint across all stages.
    pub maximum_stored_bytes: usize,
    /// First stage at which the maximum is reached.
    pub peak_stage: Option<ImagingMemoryStage>,
    /// First stage at which the maximum stored footprint is reached.
    pub peak_stored_stage: Option<ImagingMemoryStage>,
    /// Sum of distinct logical allocation sizes.
    pub total_logical_bytes: usize,
}

impl ImagingMemoryLifetimeLedger {
    /// Validate lifetimes and compute deterministic stage-overlap peaks.
    pub fn build(
        allocations: Vec<ImagingMemoryAllocationLifecycle>,
    ) -> Result<Self, ImagingPlanError> {
        let mut allocation_ids = BTreeSet::new();
        let mut total_logical_bytes = 0usize;
        for allocation in &allocations {
            if allocation.allocation_id.is_empty() {
                return Err(ImagingPlanError::InvalidInput(
                    "memory allocation id must not be empty",
                ));
            }
            if !allocation_ids.insert(allocation.allocation_id.as_str()) {
                return Err(ImagingPlanError::InvalidInput(
                    "memory allocation ids must be unique",
                ));
            }
            if allocation.residencies.is_empty() {
                return Err(ImagingPlanError::InvalidInput(
                    "memory allocation must have a residency interval",
                ));
            }
            total_logical_bytes = total_logical_bytes
                .checked_add(allocation.logical_bytes)
                .ok_or(ImagingPlanError::Overflow(
                    "logical memory allocation bytes",
                ))?;
            for residency in &allocation.residencies {
                if residency.live_from > residency.live_through {
                    return Err(ImagingPlanError::InvalidInput(
                        "memory residency stages must be ordered",
                    ));
                }
                if residency.resident_bytes > allocation.logical_bytes {
                    return Err(ImagingPlanError::InvalidInput(
                        "resident bytes cannot exceed logical allocation bytes",
                    ));
                }
                if residency.stored_bytes > allocation.logical_bytes {
                    return Err(ImagingPlanError::InvalidInput(
                        "stored bytes cannot exceed logical allocation bytes",
                    ));
                }
                let is_external_storage = matches!(
                    residency.backing,
                    ImagingMemoryBacking::MemoryMapped | ImagingMemoryBacking::TemporarySpill
                );
                if is_external_storage != (residency.stored_bytes > 0) {
                    return Err(ImagingPlanError::InvalidInput(
                        "stored-byte accounting must match a mapped or temporary-spill backing",
                    ));
                }
            }
            for (index, left) in allocation.residencies.iter().enumerate() {
                for right in allocation.residencies.iter().skip(index + 1) {
                    if ((left.resident_bytes > 0 && right.resident_bytes > 0)
                        || (left.stored_bytes > 0 && right.stored_bytes > 0))
                        && left.live_from <= right.live_through
                        && right.live_from <= left.live_through
                    {
                        return Err(ImagingPlanError::InvalidInput(
                            "one allocation cannot have overlapping resident or stored intervals",
                        ));
                    }
                }
            }
        }

        let mut stage_peaks = Vec::with_capacity(ImagingMemoryStage::ORDERED.len());
        let mut maximum_resident_bytes = 0usize;
        let mut maximum_stored_bytes = 0usize;
        let mut peak_stage = None;
        let mut peak_stored_stage = None;
        for stage in ImagingMemoryStage::ORDERED {
            let mut resident_by_backing = BTreeMap::<ImagingMemoryBacking, usize>::new();
            let mut stored_by_backing = BTreeMap::<ImagingMemoryBacking, usize>::new();
            for allocation in &allocations {
                for residency in allocation
                    .residencies
                    .iter()
                    .filter(|residency| residency.includes(stage))
                {
                    let resident_bytes = resident_by_backing.entry(residency.backing).or_default();
                    *resident_bytes = resident_bytes
                        .checked_add(residency.resident_bytes)
                        .ok_or(ImagingPlanError::Overflow("stage memory backing bytes"))?;
                    let stored_bytes = stored_by_backing.entry(residency.backing).or_default();
                    *stored_bytes = stored_bytes
                        .checked_add(residency.stored_bytes)
                        .ok_or(ImagingPlanError::Overflow("stage storage backing bytes"))?;
                }
            }
            let resident_bytes = checked_sum(
                ImagingMemoryBacking::ORDERED
                    .iter()
                    .map(|backing| resident_by_backing.get(backing).copied().unwrap_or(0)),
                "stage overlap resident bytes",
            )?;
            let stored_bytes = checked_sum(
                ImagingMemoryBacking::ORDERED
                    .iter()
                    .map(|backing| stored_by_backing.get(backing).copied().unwrap_or(0)),
                "stage overlap stored bytes",
            )?;
            if resident_bytes > maximum_resident_bytes {
                maximum_resident_bytes = resident_bytes;
                peak_stage = Some(stage);
            }
            if stored_bytes > maximum_stored_bytes {
                maximum_stored_bytes = stored_bytes;
                peak_stored_stage = Some(stage);
            }
            stage_peaks.push(ImagingMemoryStagePeak {
                stage,
                resident_bytes,
                stored_bytes,
                backing_bytes: ImagingMemoryBacking::ORDERED
                    .iter()
                    .map(|backing| ImagingMemoryBackingBytes {
                        backing: *backing,
                        bytes: resident_by_backing.get(backing).copied().unwrap_or(0),
                        stored_bytes: stored_by_backing.get(backing).copied().unwrap_or(0),
                    })
                    .collect(),
            });
        }

        Ok(Self {
            allocations,
            stage_peaks,
            maximum_resident_bytes,
            maximum_stored_bytes,
            peak_stage,
            peak_stored_stage,
            total_logical_bytes,
        })
    }

    /// Return the overlap receipt for one stage.
    pub fn stage_peak(&self, stage: ImagingMemoryStage) -> Option<&ImagingMemoryStagePeak> {
        self.stage_peaks.iter().find(|peak| peak.stage == stage)
    }
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
    /// Resource evidence and memory-pressure intent used for this plan.
    pub planning_context: ImagingPlanningContext,
    /// Runtime memory actions resolved once from the selected pressure policy.
    memory_runtime_actions: ImagingMemoryRuntimeActionReceipt,
    /// Explicit allocation lifetimes and computed stage overlap.
    pub memory_lifetime_ledger: ImagingMemoryLifetimeLedger,
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
    admit_imaging_execution_internal(admission, ImagingPlanningContext::default(), None, false)
}

/// Admit an application-selected schedule with detected resources, a typed
/// memory-pressure policy, and optional exact allocation lifetimes.
///
/// When exact lifetimes are supplied, their computed overlap participates in
/// admission. This makes the context-aware entry point suitable for
/// stage-aware release and spill plans while the legacy entry point preserves
/// its existing admission behavior.
pub fn admit_imaging_execution_with_context(
    admission: ImagingPlanAdmission,
    planning_context: ImagingPlanningContext,
    memory_lifetimes: Option<Vec<ImagingMemoryAllocationLifecycle>>,
) -> Result<ImagingResolvedPlan, ImagingPlanError> {
    admit_imaging_execution_internal(admission, planning_context, memory_lifetimes, true)
}

fn admit_imaging_execution_internal(
    admission: ImagingPlanAdmission,
    planning_context: ImagingPlanningContext,
    memory_lifetimes: Option<Vec<ImagingMemoryAllocationLifecycle>>,
    enforce_lifetime_peak: bool,
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
    let exact_lifetimes_supplied = memory_lifetimes.is_some();
    let memory_lifetime_ledger = match memory_lifetimes {
        Some(lifetimes) => ImagingMemoryLifetimeLedger::build(lifetimes)?,
        None => legacy_memory_lifetime_ledger(&admission.memory_allocations)?,
    };
    let maximum_planned_resident_bytes = if enforce_lifetime_peak && exact_lifetimes_supplied {
        // The caller supplied the complete allocation schedule, so its
        // computed overlap is authoritative. Retaining an older always-live
        // estimate here would make explicit release and demotion incapable of
        // creating any planner headroom.
        memory_lifetime_ledger.maximum_resident_bytes
    } else if enforce_lifetime_peak {
        admission
            .maximum_planned_resident_bytes
            .max(memory_lifetime_ledger.maximum_resident_bytes)
    } else {
        admission.maximum_planned_resident_bytes
    };
    require_fits(
        "admitted schedule",
        maximum_planned_resident_bytes,
        admission.usable_memory_bytes,
    )?;
    let memory_runtime_actions =
        ImagingMemoryRuntimeActionReceipt::resolve(planning_context.memory_pressure_policy);
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
        planning_context,
        memory_runtime_actions,
        memory_lifetime_ledger,
        maximum_planned_resident_bytes,
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
            planning_context: ImagingPlanningContext::default(),
            memory_runtime_actions: ImagingMemoryRuntimeActionReceipt::resolve(
                ImagingMemoryPressurePolicy::AutoSafe,
            ),
            memory_lifetime_ledger: ImagingMemoryLifetimeLedger::default(),
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

    /// Stable structured fields for the resolved memory runtime-action receipt.
    pub fn memory_runtime_action_log_fields(&self) -> String {
        let receipt = self.memory_runtime_actions;
        format!(
            "policy={} admission_action={} swap_action={} stage_lifetime_release_requested={} next_use_aware_replay_requested={} replay_prime_stage={} replay_retention_action={} known_last_use_release_active={} product_streaming_active={} replay_spill_active={} storage_demotion_active={}",
            receipt.pressure_policy.label(),
            receipt.admission_action.label(),
            receipt.swap_action.label(),
            receipt.stage_lifetime_release_requested,
            receipt.next_use_aware_replay_requested,
            receipt.replay_prime_stage.label(),
            receipt.replay_retention_action.label(),
            receipt.known_last_use_release_active,
            receipt.product_streaming_active,
            receipt.replay_spill_active,
            receipt.storage_demotion_active,
        )
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

fn legacy_allocation_id(index: usize, component: &str) -> String {
    let mut normalized = String::with_capacity(component.len());
    let mut separator_pending = false;
    for character in component.chars() {
        if character.is_ascii_alphanumeric() {
            if separator_pending && !normalized.is_empty() {
                normalized.push('.');
            }
            normalized.push(character.to_ascii_lowercase());
            separator_pending = false;
        } else {
            separator_pending = true;
        }
    }
    if normalized.is_empty() {
        normalized.push_str("unnamed");
    }
    format!("legacy.{index:04}.{normalized}")
}

fn memory_residency(
    bytes: usize,
    live_from: ImagingMemoryStage,
    live_through: ImagingMemoryStage,
    next_use: ImagingMemoryNextUse,
) -> ImagingMemoryResidency {
    ImagingMemoryResidency {
        backing: ImagingMemoryBacking::HostHeap,
        resident_bytes: bytes,
        stored_bytes: 0,
        live_from,
        live_through,
        next_use,
    }
}

fn legacy_residencies(allocation: &ImagingMemoryAllocation) -> Vec<ImagingMemoryResidency> {
    use ImagingMemoryNextUse::{AtStage, NoFurtherUse};
    use ImagingMemoryStage::{
        DirtyTransform, Finish, InitialGrid, MinorCycle, ModelTransform, Prepare,
        ProductMaterialization, ProductWrite, ResidualGrid, ResidualTransform, SourceIngest,
        Weighting,
    };

    match allocation.stage {
        "run" => vec![memory_residency(
            allocation.bytes,
            Prepare,
            ProductWrite,
            NoFurtherUse,
        )],
        "ingest" => vec![memory_residency(
            allocation.bytes,
            SourceIngest,
            SourceIngest,
            NoFurtherUse,
        )],
        "weighting" => vec![memory_residency(
            allocation.bytes,
            Weighting,
            Weighting,
            NoFurtherUse,
        )],
        "grid" => vec![
            memory_residency(
                allocation.bytes,
                InitialGrid,
                InitialGrid,
                AtStage(ResidualGrid),
            ),
            memory_residency(allocation.bytes, ResidualGrid, ResidualGrid, NoFurtherUse),
        ],
        "fft" => vec![
            memory_residency(
                allocation.bytes,
                DirtyTransform,
                DirtyTransform,
                AtStage(ResidualTransform),
            ),
            memory_residency(
                allocation.bytes,
                ResidualTransform,
                ResidualTransform,
                NoFurtherUse,
            ),
        ],
        "major-cycle" | "major_cycle" => vec![memory_residency(
            allocation.bytes,
            ModelTransform,
            ModelTransform,
            NoFurtherUse,
        )],
        "minor-cycle" | "minor_cycle" => vec![memory_residency(
            allocation.bytes,
            MinorCycle,
            MinorCycle,
            NoFurtherUse,
        )],
        "finish" => vec![memory_residency(
            allocation.bytes,
            Finish,
            Finish,
            NoFurtherUse,
        )],
        "products" => vec![memory_residency(
            allocation.bytes,
            ProductMaterialization,
            ProductWrite,
            NoFurtherUse,
        )],
        "product-write" | "product_write" => vec![memory_residency(
            allocation.bytes,
            ProductWrite,
            ProductWrite,
            NoFurtherUse,
        )],
        "initial-grid" | "initial_grid" => vec![memory_residency(
            allocation.bytes,
            InitialGrid,
            InitialGrid,
            NoFurtherUse,
        )],
        "residual-grid" | "residual_grid" => vec![memory_residency(
            allocation.bytes,
            ResidualGrid,
            ResidualGrid,
            NoFurtherUse,
        )],
        "dirty-transform" | "dirty_transform" => vec![memory_residency(
            allocation.bytes,
            DirtyTransform,
            DirtyTransform,
            NoFurtherUse,
        )],
        "residual-transform" | "residual_transform" => vec![memory_residency(
            allocation.bytes,
            ResidualTransform,
            ResidualTransform,
            NoFurtherUse,
        )],
        _ => vec![memory_residency(
            allocation.bytes,
            Prepare,
            ProductWrite,
            NoFurtherUse,
        )],
    }
}

/// Convert the original component/stage ledger into explicit conservative
/// lifetimes with deterministic compatibility allocation identifiers.
///
/// New application plans should supply semantic allocation identifiers and
/// exact lifetimes through [`admit_imaging_execution_with_context`]. This
/// adapter preserves existing plans while they migrate.
pub fn legacy_memory_lifetime_ledger(
    allocations: &[ImagingMemoryAllocation],
) -> Result<ImagingMemoryLifetimeLedger, ImagingPlanError> {
    ImagingMemoryLifetimeLedger::build(
        allocations
            .iter()
            .enumerate()
            .map(|(index, allocation)| ImagingMemoryAllocationLifecycle {
                allocation_id: legacy_allocation_id(index, allocation.component),
                component: allocation.component.to_string(),
                logical_bytes: allocation.bytes,
                residencies: legacy_residencies(allocation),
            })
            .collect(),
    )
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
    plan_imaging_execution_with_context(
        workload,
        resources,
        policy,
        &ImagingPlanningContext::default(),
    )
}

/// Build one deterministic plan with detected resource evidence and a typed
/// memory-pressure policy.
///
/// Detected facts are retained in the resolved receipt. The explicitly
/// assigned [`ImagingResources`] remain the hard resource authority, so this
/// entry point cannot silently introduce swap dependence or enlarge a budget.
pub fn plan_imaging_execution_with_context(
    workload: &ImagingWorkloadShape,
    resources: &ImagingResources,
    policy: &ImagingExecutionPolicy,
    planning_context: &ImagingPlanningContext,
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
        // The persistent grid and direct Metal scratch share one unified-memory
        // working set. Process admission alone is insufficient on Apple GPUs:
        // Metal can reject a command even when the combined allocation still
        // fits physical RAM. Bound scratch by the device slice remaining after
        // the grid so plane segmentation is chosen before buffer allocation.
        let device_scratch_capacity = resources
            .metal_device_budget_bytes
            .saturating_sub(grid_bytes);
        policy
            .direct_metal_scratch_limit_bytes
            .unwrap_or(workload.direct_metal_scratch_candidate_bytes)
            .min(workload.direct_metal_scratch_candidate_bytes)
            .min(optional_capacity)
            .min(device_scratch_capacity)
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
    if tile_enabled {
        // The consumer retains its admitted source/read-ahead blocks while
        // routing the current block into resident tiles and the bounded tile
        // queue. Size all three from one simultaneous grid-stage budget. The
        // older independent row and tile peaks could each fit while their
        // real overlap exceeded the assigned process-memory slice.
        let live_source_and_queue_bytes_per_row = row_bytes
            .checked_mul(max_live_row_blocks)
            .and_then(|bytes| bytes.checked_add(queued_bytes_per_row))
            .ok_or(ImagingPlanError::Overflow(
                "live source and tile queue bytes per row",
            ))?;
        source_row_block_rows = source_row_block_rows.min(
            tile_budget
                .saturating_sub(resident_tile_bytes)
                .checked_div(live_source_and_queue_bytes_per_row)
                .unwrap_or(0),
        );
        if workload.selected_rows > 0 && source_row_block_rows == 0 {
            return Err(ImagingPlanError::InsufficientMemory {
                stage: "grid stream",
                required_bytes: fixed_with_execution
                    .checked_add(resident_tile_bytes)
                    .and_then(|bytes| bytes.checked_add(live_source_and_queue_bytes_per_row))
                    .ok_or(ImagingPlanError::Overflow("minimum grid stream bytes"))?,
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
        [
            fixed_with_execution,
            live_row_bytes,
            resident_tile_bytes,
            queue_bytes,
        ],
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
    let memory_lifetime_ledger = legacy_memory_lifetime_ledger(&memory_allocations)?;
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
        ImagingPlanDecision {
            name: "memory_pressure_policy",
            value: planning_context.memory_pressure_policy.label().to_string(),
            origin: if planning_context.memory_pressure_policy
                == ImagingMemoryPressurePolicy::AutoSafe
            {
                ImagingPlanOrigin::Resources
            } else {
                ImagingPlanOrigin::UserPolicy
            },
            reason: "describes how the application assigned the bounded resource slice; the pure planner does not enlarge it"
                .to_string(),
        },
    ];

    let memory_runtime_actions =
        ImagingMemoryRuntimeActionReceipt::resolve(planning_context.memory_pressure_policy);
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
        planning_context: planning_context.clone(),
        memory_runtime_actions,
        memory_lifetime_ledger,
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
    fn tile_plan_admits_source_blocks_tiles_and_queue_as_one_grid_peak() {
        let plan = plan_imaging_execution(
            &workload(),
            &resources(1024 * 1024 * 1024),
            &ImagingExecutionPolicy::default(),
        )
        .unwrap();
        let grid_stream_peak = [
            "image planes",
            "grids",
            "worker scratch",
            "source row blocks",
            "resident tiles",
            "tile queue",
        ]
        .into_iter()
        .try_fold(0usize, |sum, component| {
            sum.checked_add(plan.allocation_bytes(component))
        })
        .expect("grid stream peak must not overflow");

        assert!(plan.allocation_bytes("source row blocks") > 0);
        assert!(plan.allocation_bytes("resident tiles") > 0);
        assert!(plan.allocation_bytes("tile queue") > 0);
        assert!(grid_stream_peak <= plan.usable_memory_bytes);
        assert!(plan.maximum_planned_resident_bytes >= grid_stream_peak);
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
    fn direct_metal_grid_and_scratch_share_one_device_budget() {
        const MIB: usize = 1024 * 1024;
        let mut full_grid = workload();
        full_grid.tile_queue_entry_bytes = 0;
        full_grid.direct_metal_scratch_candidate_bytes = 256 * MIB;
        let assigned = ImagingResources {
            usable_memory_bytes: 2 * 1024 * MIB,
            cpu_capacity: 8,
            metal_available: true,
            metal_device_budget_bytes: 96 * MIB,
        };
        let plan = plan_imaging_execution(
            &full_grid,
            &assigned,
            &ImagingExecutionPolicy {
                prefer_metal: true,
                ..Default::default()
            },
        )
        .unwrap();

        let grid_bytes = plan.allocation_bytes("grids");
        let scratch_bytes = plan.allocation_bytes("direct Metal host scratch");
        assert_eq!(
            scratch_bytes,
            assigned.metal_device_budget_bytes - grid_bytes
        );
        assert_eq!(
            grid_bytes + scratch_bytes,
            assigned.metal_device_budget_bytes
        );
        assert!(plan.metal.eligible);
    }

    #[test]
    fn auto_memory_pressure_is_safe_and_detected_resources_do_not_enlarge_the_slice() {
        assert_eq!(
            ImagingMemoryPressurePolicy::default(),
            ImagingMemoryPressurePolicy::AutoSafe
        );
        assert!(!ImagingMemoryPressurePolicy::AutoSafe.permits_intentional_swap());
        assert!(
            ImagingMemoryPressurePolicy::IntentionalOversubscription.permits_intentional_swap()
        );
        assert!(ImagingMemoryPressurePolicy::Hybrid.permits_intentional_swap());

        let assigned = resources(2 * 1024 * 1024 * 1024);
        let baseline =
            plan_imaging_execution(&workload(), &assigned, &ImagingExecutionPolicy::default())
                .unwrap();
        let planning_context = ImagingPlanningContext {
            memory_pressure_policy: ImagingMemoryPressurePolicy::AutoSafe,
            detected_resources: ImagingDetectedResources {
                physical_memory_bytes: Some(32 * 1024 * 1024 * 1024),
                current_memory_headroom_bytes: Some(20 * 1024 * 1024 * 1024),
                process_physical_footprint_bytes: Some(512 * 1024 * 1024),
                logical_cpu_threads: Some(8),
                performance_cpu_cores: Some(4),
                metal_recommended_working_set_bytes: Some(24 * 1024 * 1024 * 1024),
                metal_current_allocated_bytes: Some(128 * 1024 * 1024),
                unified_memory_requirement_bytes: Some(256 * 1024 * 1024),
                storage_read_bytes_per_second: Some(1_500_000_000),
                storage_write_bytes_per_second: Some(1_000_000_000),
            },
        };
        let contextual = plan_imaging_execution_with_context(
            &workload(),
            &assigned,
            &ImagingExecutionPolicy::default(),
            &planning_context,
        )
        .unwrap();

        assert_eq!(contextual.usable_memory_bytes, baseline.usable_memory_bytes);
        assert_eq!(contextual.workers, baseline.workers);
        assert_eq!(
            contextual.maximum_planned_resident_bytes,
            baseline.maximum_planned_resident_bytes
        );
        assert_eq!(contextual.planning_context, planning_context);
    }

    #[test]
    fn memory_runtime_action_receipts_are_policy_exact_and_storage_truthful() {
        let cases = [
            (
                ImagingMemoryPressurePolicy::AutoSafe,
                ImagingMemoryAdmissionAction::AutomaticNoSwapHeadroom,
                ImagingMemorySwapAction::AvoidIntentionalSwap,
                false,
                false,
            ),
            (
                ImagingMemoryPressurePolicy::ConservativeNoSwap,
                ImagingMemoryAdmissionAction::NoSwapHeadroom,
                ImagingMemorySwapAction::AvoidIntentionalSwap,
                false,
                false,
            ),
            (
                ImagingMemoryPressurePolicy::AggressiveMemoryUse,
                ImagingMemoryAdmissionAction::PhysicalProcessCeiling,
                ImagingMemorySwapAction::AllowCompressionOrIncidentalSwap,
                false,
                false,
            ),
            (
                ImagingMemoryPressurePolicy::IntentionalOversubscription,
                ImagingMemoryAdmissionAction::ExplicitOversubscriptionTarget,
                ImagingMemorySwapAction::IntentionalOversubscription,
                false,
                false,
            ),
            (
                ImagingMemoryPressurePolicy::StageAwareRelease,
                ImagingMemoryAdmissionAction::NoSwapHeadroom,
                ImagingMemorySwapAction::AvoidIntentionalSwap,
                true,
                false,
            ),
            (
                ImagingMemoryPressurePolicy::Hybrid,
                ImagingMemoryAdmissionAction::PhysicalProcessCeiling,
                ImagingMemorySwapAction::AllowCompressionOrIncidentalSwap,
                true,
                true,
            ),
        ];
        let mut distinct_receipts = BTreeSet::new();
        for (policy, admission_action, swap_action, release_requested, next_use_requested) in cases
        {
            let receipt = ImagingMemoryRuntimeActionReceipt::resolve(policy);
            assert_eq!(receipt.pressure_policy, policy);
            assert_eq!(receipt.admission_action, admission_action);
            assert_eq!(receipt.swap_action, swap_action);
            assert_eq!(receipt.stage_lifetime_release_requested, release_requested);
            assert_eq!(receipt.next_use_aware_replay_requested, next_use_requested);
            assert_eq!(receipt.replay_prime_stage, ImagingMemoryStage::ResidualGrid);
            assert_eq!(
                receipt.replay_retention_action,
                ImagingReplayRetentionAction::PinnedNoEvictionSourceOrder
            );
            assert!(receipt.known_last_use_release_active);
            assert!(!receipt.product_streaming_active);
            assert!(!receipt.replay_spill_active);
            assert!(!receipt.storage_demotion_active);
            distinct_receipts.insert(format!(
                "{}:{}:{}:{}:{}",
                policy.label(),
                admission_action.label(),
                swap_action.label(),
                release_requested,
                next_use_requested
            ));
        }
        assert_eq!(distinct_receipts.len(), cases.len());
    }

    #[test]
    fn lifetime_ledger_computes_stage_and_backing_overlap() {
        let ledger = ImagingMemoryLifetimeLedger::build(vec![
            ImagingMemoryAllocationLifecycle {
                allocation_id: "initial-grid".to_string(),
                component: "compensated AW grid".to_string(),
                logical_bytes: 100,
                residencies: vec![ImagingMemoryResidency {
                    backing: ImagingMemoryBacking::HostHeap,
                    resident_bytes: 100,
                    stored_bytes: 0,
                    live_from: ImagingMemoryStage::InitialGrid,
                    live_through: ImagingMemoryStage::DirtyTransform,
                    next_use: ImagingMemoryNextUse::NoFurtherUse,
                }],
            },
            ImagingMemoryAllocationLifecycle {
                allocation_id: "fft-scratch".to_string(),
                component: "FFT staging".to_string(),
                logical_bytes: 50,
                residencies: vec![ImagingMemoryResidency {
                    backing: ImagingMemoryBacking::UnifiedMemory,
                    resident_bytes: 50,
                    stored_bytes: 0,
                    live_from: ImagingMemoryStage::DirtyTransform,
                    live_through: ImagingMemoryStage::DirtyTransform,
                    next_use: ImagingMemoryNextUse::NoFurtherUse,
                }],
            },
            ImagingMemoryAllocationLifecycle {
                allocation_id: "replay-programs".to_string(),
                component: "compact replay programs".to_string(),
                logical_bytes: 200,
                residencies: vec![
                    ImagingMemoryResidency {
                        backing: ImagingMemoryBacking::TemporarySpill,
                        resident_bytes: 0,
                        stored_bytes: 200,
                        live_from: ImagingMemoryStage::InitialGrid,
                        live_through: ImagingMemoryStage::DirtyTransform,
                        next_use: ImagingMemoryNextUse::AtStage(ImagingMemoryStage::ResidualGrid),
                    },
                    ImagingMemoryResidency {
                        backing: ImagingMemoryBacking::MemoryMapped,
                        resident_bytes: 25,
                        stored_bytes: 200,
                        live_from: ImagingMemoryStage::ResidualGrid,
                        live_through: ImagingMemoryStage::ResidualTransform,
                        next_use: ImagingMemoryNextUse::Cyclic {
                            next_stage: ImagingMemoryStage::ResidualGrid,
                            intervening_uses: 15,
                        },
                    },
                ],
            },
        ])
        .unwrap();

        let dirty = ledger
            .stage_peak(ImagingMemoryStage::DirtyTransform)
            .unwrap();
        assert_eq!(dirty.resident_bytes, 150);
        assert_eq!(dirty.bytes_for_backing(ImagingMemoryBacking::HostHeap), 100);
        assert_eq!(
            dirty.bytes_for_backing(ImagingMemoryBacking::UnifiedMemory),
            50
        );
        assert_eq!(dirty.stored_bytes, 200);
        assert_eq!(
            dirty.stored_bytes_for_backing(ImagingMemoryBacking::TemporarySpill),
            200
        );
        let residual = ledger.stage_peak(ImagingMemoryStage::ResidualGrid).unwrap();
        assert_eq!(residual.resident_bytes, 25);
        assert_eq!(residual.stored_bytes, 200);
        assert_eq!(
            residual.bytes_for_backing(ImagingMemoryBacking::MemoryMapped),
            25
        );
        assert_eq!(
            residual.stored_bytes_for_backing(ImagingMemoryBacking::MemoryMapped),
            200
        );
        assert_eq!(ledger.maximum_resident_bytes, 150);
        assert_eq!(ledger.maximum_stored_bytes, 200);
        assert_eq!(ledger.peak_stage, Some(ImagingMemoryStage::DirtyTransform));
        assert_eq!(
            ledger.peak_stored_stage,
            Some(ImagingMemoryStage::InitialGrid)
        );
        assert_eq!(ledger.total_logical_bytes, 350);
    }

    #[test]
    fn lifetime_ledger_rejects_duplicate_ids_and_overlapping_residencies() {
        let lifecycle = ImagingMemoryAllocationLifecycle {
            allocation_id: "replay".to_string(),
            component: "replay".to_string(),
            logical_bytes: 64,
            residencies: vec![ImagingMemoryResidency {
                backing: ImagingMemoryBacking::HostHeap,
                resident_bytes: 64,
                stored_bytes: 0,
                live_from: ImagingMemoryStage::InitialGrid,
                live_through: ImagingMemoryStage::DirtyTransform,
                next_use: ImagingMemoryNextUse::NoFurtherUse,
            }],
        };
        assert!(matches!(
            ImagingMemoryLifetimeLedger::build(vec![lifecycle.clone(), lifecycle.clone()]),
            Err(ImagingPlanError::InvalidInput(
                "memory allocation ids must be unique"
            ))
        ));

        let mut overlapping = lifecycle;
        overlapping.residencies.push(ImagingMemoryResidency {
            backing: ImagingMemoryBacking::MemoryMapped,
            resident_bytes: 32,
            stored_bytes: 32,
            live_from: ImagingMemoryStage::DirtyTransform,
            live_through: ImagingMemoryStage::ResidualGrid,
            next_use: ImagingMemoryNextUse::NoFurtherUse,
        });
        assert!(matches!(
            ImagingMemoryLifetimeLedger::build(vec![overlapping]),
            Err(ImagingPlanError::InvalidInput(
                "one allocation cannot have overlapping resident or stored intervals"
            ))
        ));

        let false_spill = ImagingMemoryAllocationLifecycle {
            allocation_id: "false-spill".to_string(),
            component: "false spill".to_string(),
            logical_bytes: 64,
            residencies: vec![ImagingMemoryResidency {
                backing: ImagingMemoryBacking::TemporarySpill,
                resident_bytes: 0,
                stored_bytes: 0,
                live_from: ImagingMemoryStage::InitialGrid,
                live_through: ImagingMemoryStage::DirtyTransform,
                next_use: ImagingMemoryNextUse::NoFurtherUse,
            }],
        };
        assert!(matches!(
            ImagingMemoryLifetimeLedger::build(vec![false_spill]),
            Err(ImagingPlanError::InvalidInput(
                "stored-byte accounting must match a mapped or temporary-spill backing"
            ))
        ));
    }

    #[test]
    fn legacy_stage_allocations_receive_stable_ids_and_conservative_lifetimes() {
        let ledger = legacy_memory_lifetime_ledger(&[
            ImagingMemoryAllocation {
                component: "run state",
                stage: "run",
                bytes: 10,
            },
            ImagingMemoryAllocation {
                component: "grid scratch",
                stage: "grid",
                bytes: 20,
            },
        ])
        .unwrap();

        assert_eq!(ledger.allocations[0].allocation_id, "legacy.0000.run.state");
        assert_eq!(
            ledger
                .stage_peak(ImagingMemoryStage::InitialGrid)
                .unwrap()
                .resident_bytes,
            30
        );
        assert_eq!(
            ledger
                .stage_peak(ImagingMemoryStage::DirtyTransform)
                .unwrap()
                .resident_bytes,
            10
        );
        assert_eq!(
            ledger
                .stage_peak(ImagingMemoryStage::ResidualGrid)
                .unwrap()
                .resident_bytes,
            30
        );
    }

    #[test]
    fn context_admission_charges_exact_lifetime_overlap() {
        let baseline = plan_imaging_execution(
            &workload(),
            &resources(2 * 1024 * 1024 * 1024),
            &ImagingExecutionPolicy::default(),
        )
        .unwrap();
        let budget = baseline.maximum_planned_resident_bytes;
        let admission = ImagingPlanAdmission {
            workload: baseline.workload,
            usable_memory_bytes: budget,
            workers: baseline.workers,
            worker_partition_rows: baseline.worker_partition_rows,
            ingest: baseline.ingest,
            fft: baseline.fft,
            tile: baseline.tile,
            spectral: baseline.spectral,
            metal: baseline.metal,
            caches: baseline.caches,
            memory_allocations: baseline.memory_allocations,
            maximum_planned_resident_bytes: budget,
            decisions: baseline.decisions,
        };
        let oversized_lifetime = ImagingMemoryAllocationLifecycle {
            allocation_id: "unaccounted".to_string(),
            component: "unaccounted".to_string(),
            logical_bytes: budget + 1,
            residencies: vec![ImagingMemoryResidency {
                backing: ImagingMemoryBacking::HostHeap,
                resident_bytes: budget + 1,
                stored_bytes: 0,
                live_from: ImagingMemoryStage::Prepare,
                live_through: ImagingMemoryStage::Prepare,
                next_use: ImagingMemoryNextUse::NoFurtherUse,
            }],
        };

        assert!(matches!(
            admit_imaging_execution_with_context(
                admission,
                ImagingPlanningContext::default(),
                Some(vec![oversized_lifetime]),
            ),
            Err(ImagingPlanError::InsufficientMemory {
                stage: "admitted schedule",
                ..
            })
        ));
    }

    #[test]
    fn context_admission_uses_overlap_instead_of_total_logical_bytes() {
        let admission = ImagingPlanAdmission {
            workload: ImagingWorkloadShape::default(),
            usable_memory_bytes: 100,
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
                ready_sample_threshold: 0,
                flush_after_source_block: false,
            },
            spectral: ImagingSpectralSchedule::SinglePlane,
            metal: ImagingMetalPlan {
                eligible: false,
                command_samples: 0,
                device_cache_bytes: 0,
                rejection_reason: Some("CPU-only test".to_string()),
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
            maximum_planned_resident_bytes: 80,
            decisions: Vec::new(),
        };
        let lifecycle =
            |allocation_id: &str, stage: ImagingMemoryStage| ImagingMemoryAllocationLifecycle {
                allocation_id: allocation_id.to_string(),
                component: allocation_id.to_string(),
                logical_bytes: 80,
                residencies: vec![ImagingMemoryResidency {
                    backing: ImagingMemoryBacking::HostHeap,
                    resident_bytes: 80,
                    stored_bytes: 0,
                    live_from: stage,
                    live_through: stage,
                    next_use: ImagingMemoryNextUse::NoFurtherUse,
                }],
            };

        let plan = admit_imaging_execution_with_context(
            admission,
            ImagingPlanningContext {
                memory_pressure_policy: ImagingMemoryPressurePolicy::StageAwareRelease,
                ..Default::default()
            },
            Some(vec![
                lifecycle("initial-grid", ImagingMemoryStage::InitialGrid),
                lifecycle("products", ImagingMemoryStage::ProductMaterialization),
            ]),
        )
        .unwrap();

        assert_eq!(plan.memory_lifetime_ledger.total_logical_bytes, 160);
        assert_eq!(plan.memory_lifetime_ledger.maximum_resident_bytes, 80);
        assert_eq!(plan.maximum_planned_resident_bytes, 80);
    }

    #[test]
    fn exact_lifetimes_replace_a_conservative_always_live_peak() {
        let admission = ImagingPlanAdmission {
            workload: ImagingWorkloadShape::default(),
            usable_memory_bytes: 100,
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
                ready_sample_threshold: 0,
                flush_after_source_block: false,
            },
            spectral: ImagingSpectralSchedule::SinglePlane,
            metal: ImagingMetalPlan {
                eligible: false,
                command_samples: 0,
                device_cache_bytes: 0,
                rejection_reason: Some("CPU-only test".to_string()),
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
            memory_allocations: vec![
                ImagingMemoryAllocation {
                    component: "initial grid",
                    stage: "always-live legacy estimate",
                    bytes: 80,
                },
                ImagingMemoryAllocation {
                    component: "products",
                    stage: "always-live legacy estimate",
                    bytes: 80,
                },
            ],
            maximum_planned_resident_bytes: 160,
            decisions: Vec::new(),
        };
        let lifecycle =
            |allocation_id: &str, stage: ImagingMemoryStage| ImagingMemoryAllocationLifecycle {
                allocation_id: allocation_id.to_string(),
                component: allocation_id.to_string(),
                logical_bytes: 80,
                residencies: vec![ImagingMemoryResidency {
                    backing: ImagingMemoryBacking::HostHeap,
                    resident_bytes: 80,
                    stored_bytes: 0,
                    live_from: stage,
                    live_through: stage,
                    next_use: ImagingMemoryNextUse::NoFurtherUse,
                }],
            };

        let plan = admit_imaging_execution_with_context(
            admission,
            ImagingPlanningContext {
                memory_pressure_policy: ImagingMemoryPressurePolicy::StageAwareRelease,
                ..Default::default()
            },
            Some(vec![
                lifecycle("initial-grid", ImagingMemoryStage::InitialGrid),
                lifecycle("products", ImagingMemoryStage::ProductMaterialization),
            ]),
        )
        .unwrap();

        assert_eq!(plan.memory_lifetime_ledger.total_logical_bytes, 160);
        assert_eq!(plan.maximum_planned_resident_bytes, 80);
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
