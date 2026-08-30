// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    error::Error,
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::Instant,
};

use rayon::prelude::*;
use sha2::{Digest, Sha256};

pub(crate) const BOUNDED_WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundedStreamPlan {
    source_slots: usize,
    workers: usize,
    source_capacity_bytes: u64,
    maximum_partitions_per_block: usize,
    dynamic_kernel_window_capacity_bytes: u64,
    kernel_window_capacity_bytes: u64,
}

impl BoundedStreamPlan {
    pub(crate) fn new<Partition, Partial>(
        source_slots: usize,
        workers: usize,
        source_capacity_bytes: u64,
        maximum_partitions_per_block: usize,
        dynamic_kernel_window_capacity_bytes: u64,
    ) -> Result<Self, BoundedStreamPlanError> {
        if !(1..=2).contains(&source_slots) {
            return Err(BoundedStreamPlanError::SourceSlots);
        }
        if workers == 0 {
            return Err(BoundedStreamPlanError::Workers);
        }
        if source_capacity_bytes == 0 {
            return Err(BoundedStreamPlanError::SourceCapacity);
        }
        if maximum_partitions_per_block == 0 {
            return Err(BoundedStreamPlanError::Partitions);
        }
        let kernel_window_capacity_bytes =
            fixed_kernel_window_capacity_bytes::<Partition, Partial>(workers)?
                .checked_add(dynamic_kernel_window_capacity_bytes)
                .ok_or(BoundedStreamPlanError::KernelWindowCapacity)?;
        Ok(Self {
            source_slots,
            workers,
            source_capacity_bytes,
            maximum_partitions_per_block,
            dynamic_kernel_window_capacity_bytes,
            kernel_window_capacity_bytes,
        })
    }
}

fn fixed_kernel_window_capacity_bytes<Partition, Partial>(
    workers: usize,
) -> Result<u64, BoundedStreamPlanError> {
    let per_worker = size_of::<(WorkIdentity, KernelPartition<Partition>)>()
        .checked_add(size_of::<WorkerExecution<Partial, BoundedStreamPlanError>>())
        .and_then(|bytes| bytes.checked_add(size_of::<u64>()))
        .and_then(|bytes| bytes.checked_add(size_of::<BoundedWorkerMeasurements>()))
        .and_then(|bytes| {
            bytes.checked_add(size_of::<(
                WorkIdentity,
                std::thread::ScopedJoinHandle<'static, ()>,
            )>())
        })
        .ok_or(BoundedStreamPlanError::KernelWindowCapacity)?;
    let vectors = per_worker
        .checked_mul(workers)
        .ok_or(BoundedStreamPlanError::KernelWindowCapacity)?;
    let worker_stacks = if workers == 1 {
        0
    } else {
        BOUNDED_WORKER_STACK_BYTES
            .checked_mul(workers)
            .ok_or(BoundedStreamPlanError::KernelWindowCapacity)?
    };
    let total = vectors
        .checked_add(size_of::<Option<(WorkIdentity, KernelPartition<Partition>)>>())
        .and_then(|bytes| bytes.checked_add(worker_stacks))
        .ok_or(BoundedStreamPlanError::KernelWindowCapacity)?;
    u64::try_from(total).map_err(|_| BoundedStreamPlanError::KernelWindowCapacity)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedStreamPlanError {
    SourceSlots,
    Workers,
    SourceCapacity,
    Partitions,
    KernelWindowCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourcePoll {
    Ready {
        source_ordinal: u32,
        logical_bytes: u64,
        source_read_operations: u64,
        resident_current_bytes: u64,
        resident_capacity_bytes: u64,
    },
    Exhausted,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceFillCancellation<'a>(&'a AtomicBool);

impl SourceFillCancellation<'_> {
    #[must_use]
    pub(crate) fn is_cancelled(self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub(crate) trait OrderedBlockSource: Send {
    type Storage: Send + Sync;
    type Completion: Send;
    type Error: Error + Send + 'static;

    fn create_storage(&self, slot: usize) -> Self::Storage;
    fn fill(
        &mut self,
        block_ordinal: u64,
        storage: &mut Self::Storage,
        cancellation: SourceFillCancellation<'_>,
    ) -> Result<SourcePoll, Self::Error>;
    fn complete(self) -> Result<Self::Completion, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BlockIdentity {
    pass_ordinal: u32,
    source_ordinal: u32,
    block_ordinal: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WorkIdentity {
    pass_ordinal: u32,
    source_ordinal: u32,
    block_ordinal: u64,
    commit_region: u64,
    commit_key: u64,
    partition_key: u64,
    local_ordinal: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Accumulation {
    Exclusive {
        region: u64,
    },
    #[allow(dead_code)]
    OrderedPartial {
        region: u64,
        commit_key: u64,
    },
}

#[derive(Debug)]
pub(crate) struct KernelPartition<P> {
    partition_key: u64,
    accumulation: Accumulation,
    payload: P,
}

impl<P> KernelPartition<P> {
    pub(crate) const fn exclusive(partition_key: u64, region: u64, payload: P) -> Self {
        Self {
            partition_key,
            accumulation: Accumulation::Exclusive { region },
            payload,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn ordered(
        partition_key: u64,
        region: u64,
        commit_key: u64,
        payload: P,
    ) -> Self {
        Self {
            partition_key,
            accumulation: Accumulation::OrderedPartial { region, commit_key },
            payload,
        }
    }

    fn identity(&self, block: BlockIdentity, local_ordinal: u64) -> WorkIdentity {
        let (commit_region, commit_key) = match self.accumulation {
            Accumulation::Exclusive { region } => (region, self.partition_key),
            Accumulation::OrderedPartial { region, commit_key } => (region, commit_key),
        };
        WorkIdentity {
            pass_ordinal: block.pass_ordinal,
            source_ordinal: block.source_ordinal,
            block_ordinal: block.block_ordinal,
            commit_region,
            commit_key,
            partition_key: self.partition_key,
            local_ordinal,
        }
    }
}

pub(crate) trait PartitionedKernel<S>: Sync {
    type Partition: Send + Sync;
    type Partial: Send;
    type Completion: Send;
    type Error: Error + Send + 'static;

    fn partition_count(&self, block: BlockIdentity, storage: &S) -> Result<usize, Self::Error>;
    fn partition(
        &self,
        block: BlockIdentity,
        storage: &S,
        local_ordinal: usize,
    ) -> Result<KernelPartition<Self::Partition>, Self::Error>;
    fn partition_dynamic_capacity_bytes(&self, _partition: &Self::Partition) -> u64 {
        0
    }
    fn execute(
        &self,
        work: WorkIdentity,
        storage: &S,
        partition: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error>;
    fn partial_dynamic_capacity_bytes(&self, _partial: &Self::Partial) -> u64 {
        0
    }
    fn commit(
        &mut self,
        work: WorkIdentity,
        storage: &S,
        partial: Self::Partial,
    ) -> Result<(), Self::Error>;
    fn complete(self) -> Result<Self::Completion, Self::Error>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BoundedWorkerMeasurements {
    /// Partitions executed by this stable logical worker slot.
    pub(crate) work_units: u64,
    /// Time spent inside the scientific kernel's execute callback.
    pub(crate) active_nanos: u128,
    /// Scheduled-wave time outside that callback, including unassigned waves.
    pub(crate) wait_nanos: u128,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BoundedStreamMeasurements {
    pub(crate) blocks_filled: u64,
    pub(crate) logical_source_bytes: u64,
    pub(crate) source_read_operations: u64,
    pub(crate) source_fill_nanos: u128,
    pub(crate) prepare_nanos: u128,
    pub(crate) execute_nanos: u128,
    pub(crate) commit_nanos: u128,
    pub(crate) partitions_executed: u64,
    pub(crate) commits_completed: u64,
    pub(crate) workers_with_nonzero_partitions: usize,
    pub(crate) minimum_partitions_per_active_worker: u64,
    pub(crate) maximum_partitions_per_active_worker: u64,
    pub(crate) worker_slots: Box<[BoundedWorkerMeasurements]>,
    /// Ordered identities that completed scientific execution, chained by source block.
    pub(crate) executed_work_identity_digest: [u8; 32],
    /// Ordered identities that completed deterministic commit, chained by source block.
    pub(crate) committed_work_identity_digest: [u8; 32],
    pub(crate) producer_wait_nanos: u128,
    pub(crate) consumer_wait_nanos: u128,
    pub(crate) source_starved_nanos: u128,
    pub(crate) terminal_wait_nanos: u128,
    pub(crate) ready_queue_high_water: usize,
    pub(crate) ready_queue_current_bytes_high_water: u64,
    pub(crate) ready_queue_capacity_bytes_high_water: u64,
    pub(crate) peak_live_source_blocks: usize,
    pub(crate) peak_live_source_current_bytes: u64,
    pub(crate) peak_live_source_capacity_bytes: u64,
    pub(crate) source_slots: usize,
    pub(crate) workers: usize,
    pub(crate) worker_threads_started: u64,
    pub(crate) worker_pool_entries: u64,
    pub(crate) dispatch_waves: u64,
    pub(crate) planned_source_capacity_bytes: u64,
    pub(crate) maximum_partitions_per_block: usize,
    pub(crate) planned_kernel_dynamic_capacity_bytes: u64,
    pub(crate) planned_kernel_window_capacity_bytes: u64,
    pub(crate) peak_partial_dynamic_capacity_bytes: u64,
    pub(crate) peak_worker_stack_capacity_bytes: u64,
    pub(crate) peak_kernel_window_capacity_bytes: u64,
    pub(crate) lease_return_nanos: u128,
    pub(crate) overlap_nanos: u128,
    pub(crate) wall_nanos: u128,
}

#[derive(Debug)]
pub(crate) struct BoundedStreamOutcome<S, K> {
    pub(crate) source_completion: S,
    pub(crate) kernel_completion: K,
    pub(crate) measurements: BoundedStreamMeasurements,
}

#[derive(Debug)]
pub(crate) struct BoundedStreamFailure<S, K> {
    pub(crate) cause: Box<BoundedStreamError<S, K>>,
    pub(crate) measurements: Box<BoundedStreamMeasurements>,
}

#[derive(Debug)]
pub(crate) enum BoundedStreamError<S, K> {
    Source(S),
    Kernel(K),
    MeasurementOverflow,
    InvalidKernelPlan,
    ResidencyExceeded,
    ProducerPanicked,
    ProducerDisconnected,
}

type BoundedStreamResult<SourceCompletion, KernelCompletion, SourceError, KernelError> = Result<
    BoundedStreamOutcome<SourceCompletion, KernelCompletion>,
    BoundedStreamFailure<SourceError, KernelError>,
>;

enum WorkerExecution<P, E> {
    Completed {
        identity: WorkIdentity,
        partial: P,
        active_nanos: u128,
    },
    Failed(Box<E>),
}

struct FixedWorkerTeam {
    pool: Option<rayon::ThreadPool>,
    threads_started: Arc<AtomicU64>,
    stack_capacity_bytes: u64,
}

impl FixedWorkerTeam {
    fn new(workers: usize) -> Result<Self, BoundedStreamPlanError> {
        if workers == 0 {
            return Err(BoundedStreamPlanError::Workers);
        }
        let threads_started = Arc::new(AtomicU64::new(0));
        if workers == 1 {
            return Ok(Self {
                pool: None,
                threads_started,
                stack_capacity_bytes: 0,
            });
        }
        let stack_capacity_bytes = u64::try_from(BOUNDED_WORKER_STACK_BYTES)
            .ok()
            .and_then(|bytes| bytes.checked_mul(u64::try_from(workers).ok()?))
            .ok_or(BoundedStreamPlanError::KernelWindowCapacity)?;
        let started = Arc::clone(&threads_started);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .stack_size(BOUNDED_WORKER_STACK_BYTES)
            .start_handler(move |_| {
                started.fetch_add(1, Ordering::Relaxed);
            })
            .build()
            .map_err(|_| BoundedStreamPlanError::Workers)?;
        Ok(Self {
            pool: Some(pool),
            threads_started,
            stack_capacity_bytes,
        })
    }

    fn execute_wave<S, K>(
        &self,
        kernel: &K,
        storage: &S,
        wave: &[(WorkIdentity, KernelPartition<K::Partition>)],
        completed: &mut Vec<WorkerExecution<K::Partial, K::Error>>,
    ) -> Result<(), BoundedStreamError<InfallibleSource, K::Error>>
    where
        S: Sync,
        K: PartitionedKernel<S>,
    {
        completed.clear();
        if wave.len() == 1 {
            let (identity, partition) = &wave[0];
            let active_started = Instant::now();
            let result = kernel.execute(*identity, storage, &partition.payload);
            let active_nanos = active_started.elapsed().as_nanos();
            completed.push(match result {
                Ok(partial) => WorkerExecution::Completed {
                    identity: *identity,
                    partial,
                    active_nanos,
                },
                Err(error) => WorkerExecution::Failed(Box::new(error)),
            });
        } else {
            let pool = self
                .pool
                .as_ref()
                .ok_or(BoundedStreamError::InvalidKernelPlan)?;
            pool.install(|| {
                wave.par_iter()
                    .map(|(identity, partition)| {
                        let active_started = Instant::now();
                        let result = kernel.execute(*identity, storage, &partition.payload);
                        let active_nanos = active_started.elapsed().as_nanos();
                        match result {
                            Ok(partial) => WorkerExecution::Completed {
                                identity: *identity,
                                partial,
                                active_nanos,
                            },
                            Err(error) => WorkerExecution::Failed(Box::new(error)),
                        }
                    })
                    .collect_into_vec(completed);
            });
        }
        if let Some(index) = completed
            .iter()
            .position(|execution| matches!(execution, WorkerExecution::Failed(_)))
        {
            let WorkerExecution::Failed(error) = completed.remove(index) else {
                return Err(BoundedStreamError::InvalidKernelPlan);
            };
            return Err(BoundedStreamError::Kernel(*error));
        }
        Ok(())
    }

    const fn stack_capacity_bytes(&self) -> u64 {
        self.stack_capacity_bytes
    }

    fn install<R: Send>(&self, operation: impl FnOnce() -> R + Send) -> R {
        match &self.pool {
            Some(pool) => pool.install(operation),
            None => operation(),
        }
    }

    fn pool_entries(&self) -> u64 {
        u64::from(self.pool.is_some())
    }

    fn shutdown(self) -> u64 {
        let Self {
            pool,
            threads_started,
            stack_capacity_bytes: _,
        } = self;
        drop(pool);
        threads_started.load(Ordering::Acquire)
    }
}

struct ProcessMeasurements {
    prepare_nanos: u128,
    execute_nanos: u128,
    commit_nanos: u128,
    partitions_executed: u64,
    commits_completed: u64,
    dispatch_waves: u64,
    peak_partial_dynamic_capacity_bytes: u64,
    peak_worker_stack_capacity_bytes: u64,
    peak_kernel_window_capacity_bytes: u64,
    executed_work_identity_digest: [u8; 32],
    committed_work_identity_digest: [u8; 32],
}

impl BoundedStreamMeasurements {
    fn record_process(&mut self, process: ProcessMeasurements) -> Option<()> {
        let prepare_nanos = self.prepare_nanos.checked_add(process.prepare_nanos)?;
        let execute_nanos = self.execute_nanos.checked_add(process.execute_nanos)?;
        let commit_nanos = self.commit_nanos.checked_add(process.commit_nanos)?;
        let partitions_executed = self
            .partitions_executed
            .checked_add(process.partitions_executed)?;
        let commits_completed = self
            .commits_completed
            .checked_add(process.commits_completed)?;
        let dispatch_waves = self.dispatch_waves.checked_add(process.dispatch_waves)?;
        let mut workers_with_nonzero_partitions = 0usize;
        let mut minimum_partitions_per_active_worker = u64::MAX;
        let mut maximum_partitions_per_active_worker = 0u64;
        for executions in self
            .worker_slots
            .iter()
            .map(|worker| worker.work_units)
            .filter(|count| *count > 0)
        {
            workers_with_nonzero_partitions += 1;
            minimum_partitions_per_active_worker =
                minimum_partitions_per_active_worker.min(executions);
            maximum_partitions_per_active_worker =
                maximum_partitions_per_active_worker.max(executions);
        }
        if workers_with_nonzero_partitions == 0 {
            minimum_partitions_per_active_worker = 0;
        }
        self.prepare_nanos = prepare_nanos;
        self.execute_nanos = execute_nanos;
        self.commit_nanos = commit_nanos;
        self.partitions_executed = partitions_executed;
        self.commits_completed = commits_completed;
        self.dispatch_waves = dispatch_waves;
        self.workers_with_nonzero_partitions = workers_with_nonzero_partitions;
        self.minimum_partitions_per_active_worker = minimum_partitions_per_active_worker;
        self.maximum_partitions_per_active_worker = maximum_partitions_per_active_worker;
        self.executed_work_identity_digest = extend_work_identity_digest(
            b"casa-rs-bounded-work-v1",
            self.executed_work_identity_digest,
            process.executed_work_identity_digest,
            process.partitions_executed,
        );
        self.committed_work_identity_digest = extend_work_identity_digest(
            b"casa-rs-bounded-work-v1",
            self.committed_work_identity_digest,
            process.committed_work_identity_digest,
            process.commits_completed,
        );
        self.peak_partial_dynamic_capacity_bytes = self
            .peak_partial_dynamic_capacity_bytes
            .max(process.peak_partial_dynamic_capacity_bytes);
        self.peak_worker_stack_capacity_bytes = self
            .peak_worker_stack_capacity_bytes
            .max(process.peak_worker_stack_capacity_bytes);
        self.peak_kernel_window_capacity_bytes = self
            .peak_kernel_window_capacity_bytes
            .max(process.peak_kernel_window_capacity_bytes);
        Some(())
    }
}

fn worker_measurements(workers: usize) -> Option<Box<[BoundedWorkerMeasurements]>> {
    let mut measurements = Vec::new();
    measurements.try_reserve_exact(workers).ok()?;
    measurements.resize(workers, BoundedWorkerMeasurements::default());
    Some(measurements.into_boxed_slice())
}

fn extend_work_identity_digest(
    domain: &[u8],
    previous: [u8; 32],
    block: [u8; 32],
    work_units: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(previous);
    hasher.update(work_units.to_be_bytes());
    hasher.update(block);
    hasher.finalize().into()
}

fn record_work_identity(hasher: &mut Sha256, identity: WorkIdentity) {
    hasher.update(identity.pass_ordinal.to_be_bytes());
    hasher.update(identity.source_ordinal.to_be_bytes());
    hasher.update(identity.block_ordinal.to_be_bytes());
    hasher.update(identity.commit_region.to_be_bytes());
    hasher.update(identity.commit_key.to_be_bytes());
    hasher.update(identity.partition_key.to_be_bytes());
    hasher.update(identity.local_ordinal.to_be_bytes());
}

fn process_block<S, K>(
    plan: BoundedStreamPlan,
    block: BlockIdentity,
    storage: &S,
    kernel: &mut K,
    worker_team: &FixedWorkerTeam,
    worker_measurements: &mut [BoundedWorkerMeasurements],
    worker_measurement_capacity_bytes: u64,
) -> Result<ProcessMeasurements, BoundedStreamError<InfallibleSource, K::Error>>
where
    S: Sync,
    K: PartitionedKernel<S>,
{
    if worker_measurements.len() != plan.workers {
        return Err(BoundedStreamError::InvalidKernelPlan);
    }
    let count_started = Instant::now();
    let partition_count = kernel
        .partition_count(block, storage)
        .map_err(BoundedStreamError::Kernel)?;
    if partition_count > plan.maximum_partitions_per_block {
        return Err(BoundedStreamError::InvalidKernelPlan);
    }
    let mut prepare_nanos = count_started.elapsed().as_nanos();
    let mut execute_nanos = 0_u128;
    let mut commit_nanos = 0_u128;
    let mut partitions_executed = 0_u64;
    let mut commits_completed = 0_u64;
    let mut dispatch_waves = 0_u64;
    let mut peak_partial_dynamic_capacity_bytes = 0_u64;
    let mut peak_worker_stack_capacity_bytes = 0_u64;
    let mut wave = Vec::<(WorkIdentity, KernelPartition<K::Partition>)>::new();
    let mut completed = Vec::<WorkerExecution<K::Partial, K::Error>>::new();
    let mut exclusive_regions = Vec::<u64>::new();
    let mut executed_work_identity_hasher = Sha256::new();
    let mut committed_work_identity_hasher = Sha256::new();
    wave.try_reserve_exact(plan.workers)
        .map_err(|_| BoundedStreamError::InvalidKernelPlan)?;
    completed
        .try_reserve_exact(plan.workers)
        .map_err(|_| BoundedStreamError::InvalidKernelPlan)?;
    exclusive_regions
        .try_reserve_exact(plan.workers)
        .map_err(|_| BoundedStreamError::InvalidKernelPlan)?;
    let fixed_wave_bytes = vector_capacity_bytes(&wave)
        .and_then(|bytes| bytes.checked_add(vector_capacity_bytes(&completed)?))
        .and_then(|bytes| bytes.checked_add(vector_capacity_bytes(&exclusive_regions)?))
        .and_then(|bytes| bytes.checked_add(worker_measurement_capacity_bytes))
        .and_then(|bytes| {
            bytes.checked_add(
                u64::try_from(size_of::<
                    Option<(WorkIdentity, KernelPartition<K::Partition>)>,
                >())
                .ok()?,
            )
        })
        .ok_or(BoundedStreamError::MeasurementOverflow)?;
    if fixed_wave_bytes > plan.kernel_window_capacity_bytes {
        return Err(BoundedStreamError::ResidencyExceeded);
    }
    let mut peak_kernel_window_capacity_bytes = fixed_wave_bytes;

    let mut next_ordinal = 0usize;
    let mut deferred: Option<(WorkIdentity, KernelPartition<K::Partition>)> = None;
    let mut previous_identity = None;
    while next_ordinal < partition_count || deferred.is_some() {
        wave.clear();
        completed.clear();
        exclusive_regions.clear();
        let mut dynamic_partition_bytes = 0_u64;
        if let Some((identity, partition)) = deferred.take() {
            if let Accumulation::Exclusive { region } = partition.accumulation {
                exclusive_regions.push(region);
            }
            dynamic_partition_bytes = kernel.partition_dynamic_capacity_bytes(&partition.payload);
            wave.push((identity, partition));
        }
        while wave.len() < plan.workers && next_ordinal < partition_count {
            let prepare_started = Instant::now();
            let partition = kernel
                .partition(block, storage, next_ordinal)
                .map_err(BoundedStreamError::Kernel)?;
            let identity = partition.identity(block, next_ordinal as u64);
            prepare_nanos = prepare_nanos
                .checked_add(prepare_started.elapsed().as_nanos())
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
            if previous_identity.is_some_and(|previous| identity <= previous) {
                return Err(BoundedStreamError::InvalidKernelPlan);
            }
            previous_identity = Some(identity);
            next_ordinal += 1;
            let compatible = match partition.accumulation {
                Accumulation::Exclusive { region } => !exclusive_regions.contains(&region),
                Accumulation::OrderedPartial { .. } => true,
            };
            if !compatible {
                deferred = Some((identity, partition));
                break;
            }
            if let Accumulation::Exclusive { region } = partition.accumulation {
                exclusive_regions.push(region);
            }
            dynamic_partition_bytes = dynamic_partition_bytes
                .checked_add(kernel.partition_dynamic_capacity_bytes(&partition.payload))
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
            wave.push((identity, partition));
        }
        let deferred_dynamic_bytes = deferred
            .as_ref()
            .map(|(_, partition)| kernel.partition_dynamic_capacity_bytes(&partition.payload))
            .unwrap_or(0);
        let dynamic_partition_window_bytes = dynamic_partition_bytes
            .checked_add(deferred_dynamic_bytes)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        if dynamic_partition_window_bytes > plan.dynamic_kernel_window_capacity_bytes {
            return Err(BoundedStreamError::ResidencyExceeded);
        }
        let partition_window_bytes = fixed_wave_bytes
            .checked_add(dynamic_partition_window_bytes)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        if partition_window_bytes > plan.kernel_window_capacity_bytes {
            return Err(BoundedStreamError::ResidencyExceeded);
        }
        peak_kernel_window_capacity_bytes =
            peak_kernel_window_capacity_bytes.max(partition_window_bytes);

        dispatch_waves = dispatch_waves
            .checked_add(1)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        let execute_started = Instant::now();
        let worker_stack_capacity_bytes = worker_team.stack_capacity_bytes();
        peak_worker_stack_capacity_bytes =
            peak_worker_stack_capacity_bytes.max(worker_stack_capacity_bytes);
        worker_team.execute_wave(&*kernel, storage, &wave, &mut completed)?;
        let wave_nanos = execute_started.elapsed().as_nanos();
        execute_nanos = execute_nanos
            .checked_add(wave_nanos)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        for (slot, execution) in completed.iter().enumerate() {
            let WorkerExecution::Completed { active_nanos, .. } = execution else {
                return Err(BoundedStreamError::InvalidKernelPlan);
            };
            let worker = &mut worker_measurements[slot];
            worker.work_units = worker
                .work_units
                .checked_add(1)
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
            worker.active_nanos = worker
                .active_nanos
                .checked_add(*active_nanos)
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
            worker.wait_nanos = worker
                .wait_nanos
                .checked_add(wave_nanos.saturating_sub(*active_nanos))
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
        }
        for worker in &mut worker_measurements[completed.len()..] {
            worker.wait_nanos = worker
                .wait_nanos
                .checked_add(wave_nanos)
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
        }
        partitions_executed = partitions_executed
            .checked_add(
                u64::try_from(wave.len()).map_err(|_| BoundedStreamError::MeasurementOverflow)?,
            )
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        for execution in &completed {
            let WorkerExecution::Completed { identity, .. } = execution else {
                return Err(BoundedStreamError::InvalidKernelPlan);
            };
            record_work_identity(&mut executed_work_identity_hasher, *identity);
        }
        let dynamic_partial_bytes = completed.iter().try_fold(0_u64, |total, execution| {
            let WorkerExecution::Completed { partial, .. } = execution else {
                return None;
            };
            total.checked_add(kernel.partial_dynamic_capacity_bytes(partial))
        });
        let dynamic_partial_bytes =
            dynamic_partial_bytes.ok_or(BoundedStreamError::MeasurementOverflow)?;
        peak_partial_dynamic_capacity_bytes =
            peak_partial_dynamic_capacity_bytes.max(dynamic_partial_bytes);
        let dynamic_live_window_bytes = dynamic_partition_window_bytes
            .checked_add(dynamic_partial_bytes)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        if dynamic_live_window_bytes > plan.dynamic_kernel_window_capacity_bytes {
            return Err(BoundedStreamError::ResidencyExceeded);
        }
        let live_window_bytes = partition_window_bytes
            .checked_add(worker_stack_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(dynamic_partial_bytes))
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        if live_window_bytes > plan.kernel_window_capacity_bytes {
            return Err(BoundedStreamError::ResidencyExceeded);
        }
        peak_kernel_window_capacity_bytes =
            peak_kernel_window_capacity_bytes.max(live_window_bytes);

        let commit_started = Instant::now();
        for execution in completed.drain(..) {
            let WorkerExecution::Completed {
                identity, partial, ..
            } = execution
            else {
                return Err(BoundedStreamError::InvalidKernelPlan);
            };
            kernel
                .commit(identity, storage, partial)
                .map_err(BoundedStreamError::Kernel)?;
            record_work_identity(&mut committed_work_identity_hasher, identity);
            commits_completed = commits_completed
                .checked_add(1)
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
        }
        commit_nanos = commit_nanos
            .checked_add(commit_started.elapsed().as_nanos())
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
    }
    Ok(ProcessMeasurements {
        prepare_nanos,
        execute_nanos,
        commit_nanos,
        partitions_executed,
        commits_completed,
        dispatch_waves,
        peak_partial_dynamic_capacity_bytes,
        peak_worker_stack_capacity_bytes,
        peak_kernel_window_capacity_bytes,
        executed_work_identity_digest: executed_work_identity_hasher.finalize().into(),
        committed_work_identity_digest: committed_work_identity_hasher.finalize().into(),
    })
}

fn vector_capacity_bytes<T>(values: &Vec<T>) -> Option<u64> {
    let bytes = values.capacity().checked_mul(size_of::<T>())?;
    u64::try_from(bytes).ok()
}

fn slice_capacity_bytes<T>(values: &[T]) -> Option<u64> {
    let bytes = values.len().checked_mul(size_of::<T>())?;
    u64::try_from(bytes).ok()
}

#[derive(Debug)]
enum InfallibleSource {}

enum ReadyMessage<S, E, C> {
    Block {
        identity: BlockIdentity,
        lease: StorageLease<S>,
    },
    Exhausted,
    SourceError(E),
    MeasurementOverflow,
    ResidencyExceeded,
    Completed(C, ProducerMeasurements),
}

struct StorageLease<S> {
    storage: S,
    resident_current_bytes: u64,
    resident_capacity_bytes: u64,
    returned_at: Option<Instant>,
}

#[derive(Clone, Copy, Default)]
struct ProducerMeasurements {
    blocks_filled: u64,
    logical_source_bytes: u64,
    source_read_operations: u64,
    source_fill_nanos: u128,
    producer_wait_nanos: u128,
    peak_live_source_blocks: usize,
    peak_live_source_current_bytes: u64,
    peak_live_source_capacity_bytes: u64,
    lease_return_nanos: u128,
}

struct ProducerMeasurementRecorder {
    current: ProducerMeasurements,
    published: Arc<Mutex<ProducerMeasurements>>,
}

impl ProducerMeasurementRecorder {
    fn new(published: Arc<Mutex<ProducerMeasurements>>) -> Self {
        Self {
            current: ProducerMeasurements::default(),
            published,
        }
    }
}

impl Deref for ProducerMeasurementRecorder {
    type Target = ProducerMeasurements;

    fn deref(&self) -> &Self::Target {
        &self.current
    }
}

impl DerefMut for ProducerMeasurementRecorder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.current
    }
}

impl Drop for ProducerMeasurementRecorder {
    fn drop(&mut self) {
        *self
            .published
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = self.current;
    }
}

#[derive(Default)]
struct OverlapState {
    producer_active: bool,
    consumer_active: bool,
    overlap_started: Option<Instant>,
    overlap_nanos: u128,
}

impl OverlapState {
    fn set_producer(&mut self, active: bool) {
        self.set_active(active, true);
    }

    fn set_consumer(&mut self, active: bool) {
        self.set_active(active, false);
    }

    fn set_active(&mut self, active: bool, producer: bool) {
        let was_overlapping = self.producer_active && self.consumer_active;
        if producer {
            self.producer_active = active;
        } else {
            self.consumer_active = active;
        }
        let is_overlapping = self.producer_active && self.consumer_active;
        match (was_overlapping, is_overlapping) {
            (false, true) => self.overlap_started = Some(Instant::now()),
            (true, false) => {
                if let Some(started) = self.overlap_started.take() {
                    self.overlap_nanos = self
                        .overlap_nanos
                        .saturating_add(started.elapsed().as_nanos());
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn execute_bounded<S, K>(
    plan: BoundedStreamPlan,
    pass_ordinal: u32,
    source: S,
    kernel: K,
) -> BoundedStreamResult<S::Completion, K::Completion, S::Error, K::Error>
where
    S: OrderedBlockSource,
    K: PartitionedKernel<S::Storage> + Send,
{
    let started = Instant::now();
    let worker_team = match FixedWorkerTeam::new(plan.workers) {
        Ok(worker_team) => worker_team,
        Err(_) => {
            let mut measurements = measurements_for_plan(plan);
            measurements.wall_nanos = started.elapsed().as_nanos();
            return Err(BoundedStreamFailure {
                cause: Box::new(BoundedStreamError::InvalidKernelPlan),
                measurements: Box::new(measurements),
            });
        }
    };
    let worker_pool_entries = worker_team.pool_entries();
    let mut result = worker_team.install(|| {
        if plan.source_slots == 1 {
            execute_inline(plan, pass_ordinal, source, kernel, &worker_team)
        } else {
            execute_overlapped(plan, pass_ordinal, source, kernel, &worker_team)
        }
    });
    let worker_threads_started = worker_team.shutdown();
    match &mut result {
        Ok(outcome) => {
            outcome.measurements.worker_threads_started = worker_threads_started;
            outcome.measurements.worker_pool_entries = worker_pool_entries;
        }
        Err(failure) => {
            failure.measurements.worker_threads_started = worker_threads_started;
            failure.measurements.worker_pool_entries = worker_pool_entries;
        }
    }
    match result {
        Ok(mut outcome) => {
            outcome.measurements.wall_nanos = started.elapsed().as_nanos();
            Ok(outcome)
        }
        Err(mut failure) => {
            failure.measurements.wall_nanos = started.elapsed().as_nanos();
            Err(failure)
        }
    }
}

fn measurements_for_plan(plan: BoundedStreamPlan) -> BoundedStreamMeasurements {
    BoundedStreamMeasurements {
        source_slots: plan.source_slots,
        workers: plan.workers,
        planned_source_capacity_bytes: plan.source_capacity_bytes,
        maximum_partitions_per_block: plan.maximum_partitions_per_block,
        planned_kernel_dynamic_capacity_bytes: plan.dynamic_kernel_window_capacity_bytes,
        planned_kernel_window_capacity_bytes: plan.kernel_window_capacity_bytes,
        ..BoundedStreamMeasurements::default()
    }
}

fn execute_inline<S, K>(
    plan: BoundedStreamPlan,
    pass_ordinal: u32,
    mut source: S,
    mut kernel: K,
    worker_team: &FixedWorkerTeam,
) -> BoundedStreamResult<S::Completion, K::Completion, S::Error, K::Error>
where
    S: OrderedBlockSource,
    K: PartitionedKernel<S::Storage>,
{
    let cancelled = AtomicBool::new(false);
    let mut measurements = measurements_for_plan(plan);
    let source_result = (|| {
        measurements.worker_slots =
            worker_measurements(plan.workers).ok_or(BoundedStreamError::InvalidKernelPlan)?;
        let worker_measurement_capacity_bytes = slice_capacity_bytes(&measurements.worker_slots)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        let mut storage = source.create_storage(0);
        let mut block_ordinal = 0_u64;
        loop {
            let fill_started = Instant::now();
            let poll = source.fill(
                block_ordinal,
                &mut storage,
                SourceFillCancellation(&cancelled),
            );
            measurements.source_fill_nanos = measurements
                .source_fill_nanos
                .checked_add(fill_started.elapsed().as_nanos())
                .ok_or(BoundedStreamError::MeasurementOverflow)?;
            match poll.map_err(BoundedStreamError::Source)? {
                SourcePoll::Ready {
                    source_ordinal,
                    logical_bytes,
                    source_read_operations,
                    resident_current_bytes,
                    resident_capacity_bytes,
                } => {
                    if resident_capacity_bytes > plan.source_capacity_bytes {
                        return Err(BoundedStreamError::ResidencyExceeded);
                    }
                    measurements.blocks_filled = measurements
                        .blocks_filled
                        .checked_add(1)
                        .ok_or(BoundedStreamError::MeasurementOverflow)?;
                    measurements.logical_source_bytes = measurements
                        .logical_source_bytes
                        .checked_add(logical_bytes)
                        .ok_or(BoundedStreamError::MeasurementOverflow)?;
                    measurements.source_read_operations = measurements
                        .source_read_operations
                        .checked_add(source_read_operations)
                        .ok_or(BoundedStreamError::MeasurementOverflow)?;
                    measurements.peak_live_source_blocks = 1;
                    measurements.peak_live_source_current_bytes = measurements
                        .peak_live_source_current_bytes
                        .max(resident_current_bytes);
                    measurements.peak_live_source_capacity_bytes = measurements
                        .peak_live_source_capacity_bytes
                        .max(resident_capacity_bytes);
                    let process = process_block(
                        plan,
                        BlockIdentity {
                            pass_ordinal,
                            source_ordinal,
                            block_ordinal,
                        },
                        &storage,
                        &mut kernel,
                        worker_team,
                        &mut measurements.worker_slots,
                        worker_measurement_capacity_bytes,
                    )
                    .map_err(map_process_error)?;
                    measurements
                        .record_process(process)
                        .ok_or(BoundedStreamError::MeasurementOverflow)?;
                    block_ordinal = block_ordinal
                        .checked_add(1)
                        .ok_or(BoundedStreamError::MeasurementOverflow)?;
                }
                SourcePoll::Exhausted => break,
            }
        }
        source.complete().map_err(BoundedStreamError::Source)
    })();
    let result = source_result.and_then(|source_completion| {
        kernel
            .complete()
            .map(|kernel_completion| (source_completion, kernel_completion))
            .map_err(BoundedStreamError::Kernel)
    });
    match result {
        Ok((source_completion, kernel_completion)) => Ok(BoundedStreamOutcome {
            source_completion,
            kernel_completion,
            measurements,
        }),
        Err(cause) => Err(BoundedStreamFailure {
            cause: Box::new(cause),
            measurements: Box::new(measurements),
        }),
    }
}

fn execute_overlapped<S, K>(
    plan: BoundedStreamPlan,
    pass_ordinal: u32,
    source: S,
    mut kernel: K,
    worker_team: &FixedWorkerTeam,
) -> BoundedStreamResult<S::Completion, K::Completion, S::Error, K::Error>
where
    S: OrderedBlockSource,
    K: PartitionedKernel<S::Storage>,
{
    let cancelled = Arc::new(AtomicBool::new(false));
    let ready_count = Arc::new(AtomicUsize::new(0));
    let ready_high_water = Arc::new(AtomicUsize::new(0));
    let ready_current_bytes = Arc::new(AtomicU64::new(0));
    let ready_current_high_water = Arc::new(AtomicU64::new(0));
    let ready_capacity_bytes = Arc::new(AtomicU64::new(0));
    let ready_capacity_high_water = Arc::new(AtomicU64::new(0));
    let ready_queue_capacity = plan.source_slots - 2;
    let overlap = Arc::new(Mutex::new(OverlapState::default()));
    let producer_measurements = Arc::new(Mutex::new(ProducerMeasurements::default()));
    let (ready_tx, ready_rx) = mpsc::sync_channel(ready_queue_capacity);
    let (returned_sender, returned_rx) =
        mpsc::sync_channel::<StorageLease<S::Storage>>(plan.source_slots);
    let mut returned_tx = Some(returned_sender);
    let mut measurements = measurements_for_plan(plan);
    let source_completion = std::thread::scope(|scope| {
        measurements.worker_slots =
            worker_measurements(plan.workers).ok_or(BoundedStreamError::InvalidKernelPlan)?;
        let worker_measurement_capacity_bytes = slice_capacity_bytes(&measurements.worker_slots)
            .ok_or(BoundedStreamError::MeasurementOverflow)?;
        let producer_cancelled = Arc::clone(&cancelled);
        let producer_ready_count = Arc::clone(&ready_count);
        let producer_high_water = Arc::clone(&ready_high_water);
        let producer_ready_current_bytes = Arc::clone(&ready_current_bytes);
        let producer_ready_current_high_water = Arc::clone(&ready_current_high_water);
        let producer_ready_capacity_bytes = Arc::clone(&ready_capacity_bytes);
        let producer_ready_capacity_high_water = Arc::clone(&ready_capacity_high_water);
        let producer_overlap = Arc::clone(&overlap);
        let published_producer_measurements = Arc::clone(&producer_measurements);
        let producer = scope.spawn(move || {
            let mut source = source;
            let mut empty = (0..plan.source_slots)
                .map(|slot| StorageLease {
                    storage: source.create_storage(slot),
                    resident_current_bytes: 0,
                    resident_capacity_bytes: 0,
                    returned_at: None,
                })
                .collect::<Vec<_>>();
            let mut outstanding = 0usize;
            let mut live_current_bytes = 0_u64;
            let mut live_capacity_bytes = 0_u64;
            let mut block_ordinal = 0_u64;
            let mut producer_measurements =
                ProducerMeasurementRecorder::new(published_producer_measurements);
            loop {
                if producer_cancelled.load(Ordering::Acquire) {
                    return;
                }
                let lease = if let Some(lease) = empty.pop() {
                    lease
                } else {
                    let wait_started = Instant::now();
                    let Ok(mut lease) = returned_rx.recv() else {
                        return;
                    };
                    let Some(wait_nanos) = producer_measurements
                        .producer_wait_nanos
                        .checked_add(wait_started.elapsed().as_nanos())
                    else {
                        let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                        return;
                    };
                    producer_measurements.producer_wait_nanos = wait_nanos;
                    if let Some(returned_at) = lease.returned_at.take() {
                        let Some(return_nanos) = producer_measurements
                            .lease_return_nanos
                            .checked_add(returned_at.elapsed().as_nanos())
                        else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        producer_measurements.lease_return_nanos = return_nanos;
                    }
                    outstanding -= 1;
                    let Some(current_bytes) =
                        live_current_bytes.checked_sub(lease.resident_current_bytes)
                    else {
                        let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                        return;
                    };
                    let Some(capacity_bytes) =
                        live_capacity_bytes.checked_sub(lease.resident_capacity_bytes)
                    else {
                        let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                        return;
                    };
                    live_current_bytes = current_bytes;
                    live_capacity_bytes = capacity_bytes;
                    lease
                };
                let mut lease = lease;
                let fill_started = Instant::now();
                producer_overlap
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .set_producer(true);
                let fill_cancellation = SourceFillCancellation(producer_cancelled.as_ref());
                let poll = source.fill(block_ordinal, &mut lease.storage, fill_cancellation);
                producer_overlap
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .set_producer(false);
                let Some(fill_nanos) = producer_measurements
                    .source_fill_nanos
                    .checked_add(fill_started.elapsed().as_nanos())
                else {
                    let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                    return;
                };
                producer_measurements.source_fill_nanos = fill_nanos;
                match poll {
                    Ok(SourcePoll::Ready {
                        source_ordinal,
                        logical_bytes,
                        source_read_operations,
                        resident_current_bytes,
                        resident_capacity_bytes,
                    }) => {
                        let Some(blocks_filled) =
                            producer_measurements.blocks_filled.checked_add(1)
                        else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        let Some(logical_source_bytes) = producer_measurements
                            .logical_source_bytes
                            .checked_add(logical_bytes)
                        else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        let Some(total_source_read_operations) = producer_measurements
                            .source_read_operations
                            .checked_add(source_read_operations)
                        else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        let Some(current_bytes) =
                            live_current_bytes.checked_add(resident_current_bytes)
                        else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        let Some(capacity_bytes) =
                            live_capacity_bytes.checked_add(resident_capacity_bytes)
                        else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        if capacity_bytes > plan.source_capacity_bytes {
                            let _ = ready_tx.send(ReadyMessage::ResidencyExceeded);
                            return;
                        }
                        producer_measurements.blocks_filled = blocks_filled;
                        producer_measurements.logical_source_bytes = logical_source_bytes;
                        producer_measurements.source_read_operations = total_source_read_operations;
                        live_current_bytes = current_bytes;
                        live_capacity_bytes = capacity_bytes;
                        outstanding += 1;
                        producer_measurements.peak_live_source_blocks = producer_measurements
                            .peak_live_source_blocks
                            .max(outstanding);
                        producer_measurements.peak_live_source_current_bytes =
                            producer_measurements
                                .peak_live_source_current_bytes
                                .max(live_current_bytes);
                        producer_measurements.peak_live_source_capacity_bytes =
                            producer_measurements
                                .peak_live_source_capacity_bytes
                                .max(live_capacity_bytes);
                        lease.resident_current_bytes = resident_current_bytes;
                        lease.resident_capacity_bytes = resident_capacity_bytes;
                        if fill_cancellation.is_cancelled() {
                            return;
                        }
                        if ready_queue_capacity > 0 {
                            let current_queued = match producer_ready_current_bytes.fetch_update(
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                |bytes| bytes.checked_add(resident_current_bytes),
                            ) {
                                Ok(previous) => previous + resident_current_bytes,
                                Err(_) => {
                                    let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                                    return;
                                }
                            };
                            let capacity_queued = match producer_ready_capacity_bytes.fetch_update(
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                |bytes| bytes.checked_add(resident_capacity_bytes),
                            ) {
                                Ok(previous) => previous + resident_capacity_bytes,
                                Err(_) => {
                                    producer_ready_current_bytes
                                        .fetch_sub(resident_current_bytes, Ordering::AcqRel);
                                    let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                                    return;
                                }
                            };
                            producer_ready_current_high_water
                                .fetch_max(current_queued, Ordering::AcqRel);
                            producer_ready_capacity_high_water
                                .fetch_max(capacity_queued, Ordering::AcqRel);
                            let queued = producer_ready_count.fetch_add(1, Ordering::AcqRel) + 1;
                            producer_high_water.fetch_max(queued, Ordering::AcqRel);
                        }
                        if ready_tx
                            .send(ReadyMessage::Block {
                                identity: BlockIdentity {
                                    pass_ordinal,
                                    source_ordinal,
                                    block_ordinal,
                                },
                                lease,
                            })
                            .is_err()
                        {
                            if ready_queue_capacity > 0 {
                                producer_ready_count.fetch_sub(1, Ordering::AcqRel);
                                producer_ready_current_bytes
                                    .fetch_sub(resident_current_bytes, Ordering::AcqRel);
                                producer_ready_capacity_bytes
                                    .fetch_sub(resident_capacity_bytes, Ordering::AcqRel);
                            }
                            return;
                        }
                        let Some(next) = block_ordinal.checked_add(1) else {
                            let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                            return;
                        };
                        block_ordinal = next;
                    }
                    Ok(SourcePoll::Exhausted) => {
                        if ready_tx.send(ReadyMessage::Exhausted).is_err() {
                            return;
                        }
                        while outstanding > 0 {
                            let wait_started = Instant::now();
                            let Ok(lease) = returned_rx.recv() else {
                                return;
                            };
                            let Some(wait_nanos) = producer_measurements
                                .producer_wait_nanos
                                .checked_add(wait_started.elapsed().as_nanos())
                            else {
                                let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                                return;
                            };
                            producer_measurements.producer_wait_nanos = wait_nanos;
                            if let Some(returned_at) = lease.returned_at {
                                let Some(return_nanos) = producer_measurements
                                    .lease_return_nanos
                                    .checked_add(returned_at.elapsed().as_nanos())
                                else {
                                    let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                                    return;
                                };
                                producer_measurements.lease_return_nanos = return_nanos;
                            }
                            outstanding -= 1;
                            if live_current_bytes
                                .checked_sub(lease.resident_current_bytes)
                                .is_none()
                                || live_capacity_bytes
                                    .checked_sub(lease.resident_capacity_bytes)
                                    .is_none()
                            {
                                let _ = ready_tx.send(ReadyMessage::MeasurementOverflow);
                                return;
                            }
                            empty.push(lease);
                        }
                        if producer_cancelled.load(Ordering::Acquire) {
                            return;
                        }
                        match source.complete() {
                            Ok(completion) => {
                                let _ = ready_tx.send(ReadyMessage::Completed(
                                    completion,
                                    *producer_measurements,
                                ));
                            }
                            Err(error) => {
                                let _ = ready_tx.send(ReadyMessage::SourceError(error));
                            }
                        }
                        return;
                    }
                    Err(error) => {
                        let _ = ready_tx.send(ReadyMessage::SourceError(error));
                        return;
                    }
                }
            }
        });

        let mut completion = None;
        loop {
            let wait_started = Instant::now();
            let message = ready_rx.recv();
            let waited_nanos = wait_started.elapsed().as_nanos();
            let Some(consumer_wait_nanos) =
                measurements.consumer_wait_nanos.checked_add(waited_nanos)
            else {
                cancelled.store(true, Ordering::Release);
                returned_tx.take();
                drop(ready_rx);
                return Err(BoundedStreamError::MeasurementOverflow);
            };
            measurements.consumer_wait_nanos = consumer_wait_nanos;
            match &message {
                Ok(ReadyMessage::Block { .. }) => {
                    let Some(source_starved_nanos) =
                        measurements.source_starved_nanos.checked_add(waited_nanos)
                    else {
                        cancelled.store(true, Ordering::Release);
                        returned_tx.take();
                        drop(ready_rx);
                        return Err(BoundedStreamError::MeasurementOverflow);
                    };
                    measurements.source_starved_nanos = source_starved_nanos;
                }
                _ => {
                    let Some(terminal_wait_nanos) =
                        measurements.terminal_wait_nanos.checked_add(waited_nanos)
                    else {
                        cancelled.store(true, Ordering::Release);
                        returned_tx.take();
                        drop(ready_rx);
                        return Err(BoundedStreamError::MeasurementOverflow);
                    };
                    measurements.terminal_wait_nanos = terminal_wait_nanos;
                }
            }
            match message {
                Ok(ReadyMessage::Block {
                    identity,
                    mut lease,
                }) => {
                    if ready_queue_capacity > 0 {
                        ready_count.fetch_sub(1, Ordering::AcqRel);
                        if ready_current_bytes
                            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                                bytes.checked_sub(lease.resident_current_bytes)
                            })
                            .is_err()
                            || ready_capacity_bytes
                                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                                    bytes.checked_sub(lease.resident_capacity_bytes)
                                })
                                .is_err()
                        {
                            cancelled.store(true, Ordering::Release);
                            returned_tx.take();
                            drop(ready_rx);
                            return Err(BoundedStreamError::MeasurementOverflow);
                        }
                    }
                    overlap
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .set_consumer(true);
                    let process = process_block(
                        plan,
                        identity,
                        &lease.storage,
                        &mut kernel,
                        worker_team,
                        &mut measurements.worker_slots,
                        worker_measurement_capacity_bytes,
                    )
                    .map_err(map_process_error);
                    overlap
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .set_consumer(false);
                    match process {
                        Ok(process) => {
                            if measurements.record_process(process).is_none() {
                                cancelled.store(true, Ordering::Release);
                                returned_tx.take();
                                drop(ready_rx);
                                return Err(BoundedStreamError::MeasurementOverflow);
                            }
                            lease.returned_at = Some(Instant::now());
                            if returned_tx
                                .as_ref()
                                .expect("return sender remains live while consuming")
                                .send(lease)
                                .is_err()
                            {
                                // The producer closes the lease-return channel after
                                // publishing a terminal source error. Keep receiving so
                                // that precise error wins over a generic disconnect.
                                continue;
                            }
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Release);
                            returned_tx.take();
                            drop(ready_rx);
                            return Err(error);
                        }
                    }
                }
                Ok(ReadyMessage::Exhausted) => {}
                Ok(ReadyMessage::SourceError(error)) => {
                    cancelled.store(true, Ordering::Release);
                    returned_tx.take();
                    drop(ready_rx);
                    return Err(BoundedStreamError::Source(error));
                }
                Ok(ReadyMessage::MeasurementOverflow) => {
                    cancelled.store(true, Ordering::Release);
                    returned_tx.take();
                    drop(ready_rx);
                    return Err(BoundedStreamError::MeasurementOverflow);
                }
                Ok(ReadyMessage::ResidencyExceeded) => {
                    cancelled.store(true, Ordering::Release);
                    returned_tx.take();
                    drop(ready_rx);
                    return Err(BoundedStreamError::ResidencyExceeded);
                }
                Ok(ReadyMessage::Completed(value, producer_measurements)) => {
                    measurements.blocks_filled = producer_measurements.blocks_filled;
                    measurements.logical_source_bytes = producer_measurements.logical_source_bytes;
                    measurements.source_read_operations =
                        producer_measurements.source_read_operations;
                    measurements.source_fill_nanos = producer_measurements.source_fill_nanos;
                    measurements.producer_wait_nanos = producer_measurements.producer_wait_nanos;
                    measurements.peak_live_source_blocks =
                        producer_measurements.peak_live_source_blocks;
                    measurements.peak_live_source_current_bytes =
                        producer_measurements.peak_live_source_current_bytes;
                    measurements.peak_live_source_capacity_bytes =
                        producer_measurements.peak_live_source_capacity_bytes;
                    measurements.lease_return_nanos = producer_measurements.lease_return_nanos;
                    completion = Some(value);
                    break;
                }
                Err(_) => {
                    cancelled.store(true, Ordering::Release);
                    returned_tx.take();
                    break;
                }
            }
        }
        if producer.join().is_err() {
            return Err(BoundedStreamError::ProducerPanicked);
        }
        completion.ok_or(BoundedStreamError::ProducerDisconnected)
    });
    let producer_measurements = *producer_measurements
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    measurements.blocks_filled = producer_measurements.blocks_filled;
    measurements.logical_source_bytes = producer_measurements.logical_source_bytes;
    measurements.source_read_operations = producer_measurements.source_read_operations;
    measurements.source_fill_nanos = producer_measurements.source_fill_nanos;
    measurements.producer_wait_nanos = producer_measurements.producer_wait_nanos;
    measurements.peak_live_source_blocks = producer_measurements.peak_live_source_blocks;
    measurements.peak_live_source_current_bytes =
        producer_measurements.peak_live_source_current_bytes;
    measurements.peak_live_source_capacity_bytes =
        producer_measurements.peak_live_source_capacity_bytes;
    measurements.lease_return_nanos = producer_measurements.lease_return_nanos;
    measurements.ready_queue_high_water = ready_high_water.load(Ordering::Acquire);
    measurements.ready_queue_current_bytes_high_water =
        ready_current_high_water.load(Ordering::Acquire);
    measurements.ready_queue_capacity_bytes_high_water =
        ready_capacity_high_water.load(Ordering::Acquire);
    measurements.overlap_nanos = overlap
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .overlap_nanos;
    let source_completion = match source_completion {
        Ok(completion) => completion,
        Err(cause) => {
            return Err(BoundedStreamFailure {
                cause: Box::new(cause),
                measurements: Box::new(measurements),
            });
        }
    };
    let kernel_completion = match kernel.complete() {
        Ok(completion) => completion,
        Err(error) => {
            return Err(BoundedStreamFailure {
                cause: Box::new(BoundedStreamError::Kernel(error)),
                measurements: Box::new(measurements),
            });
        }
    };
    Ok(BoundedStreamOutcome {
        source_completion,
        kernel_completion,
        measurements,
    })
}

fn map_process_error<S, K>(
    error: BoundedStreamError<InfallibleSource, K>,
) -> BoundedStreamError<S, K> {
    match error {
        BoundedStreamError::Kernel(error) => BoundedStreamError::Kernel(error),
        BoundedStreamError::MeasurementOverflow => BoundedStreamError::MeasurementOverflow,
        BoundedStreamError::InvalidKernelPlan => BoundedStreamError::InvalidKernelPlan,
        BoundedStreamError::ResidencyExceeded => BoundedStreamError::ResidencyExceeded,
        BoundedStreamError::ProducerPanicked => BoundedStreamError::ProducerPanicked,
        BoundedStreamError::ProducerDisconnected => BoundedStreamError::ProducerDisconnected,
        BoundedStreamError::Source(error) => match error {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_ms::{
        MeasurementSet, VisibilityBuffer, VisibilityBufferRequest, VisibilityComplexSamples,
        VisibilityDataColumn, VisibilityFloatSamples,
    };
    use std::{
        collections::BTreeSet,
        convert::Infallible,
        env, fmt,
        mem::{size_of, size_of_val},
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    struct NumberSource {
        blocks: Vec<Vec<u64>>,
        next: usize,
        pointers: Arc<Mutex<Vec<(usize, usize)>>>,
        completions: Arc<AtomicUsize>,
    }

    impl OrderedBlockSource for NumberSource {
        type Storage = Vec<u64>;
        type Completion = usize;
        type Error = Infallible;

        fn create_storage(&self, _slot: usize) -> Self::Storage {
            Vec::with_capacity(4)
        }

        fn fill(
            &mut self,
            block_ordinal: u64,
            storage: &mut Self::Storage,
            _cancellation: SourceFillCancellation<'_>,
        ) -> Result<SourcePoll, Self::Error> {
            let Some(values) = self.blocks.get(self.next) else {
                return Ok(SourcePoll::Exhausted);
            };
            storage.clear();
            storage.extend_from_slice(values);
            self.pointers
                .lock()
                .unwrap()
                .push((block_ordinal as usize, storage.as_ptr() as usize));
            self.next += 1;
            Ok(SourcePoll::Ready {
                source_ordinal: 0,
                logical_bytes: (storage.len() * size_of::<u64>()) as u64,
                source_read_operations: 1,
                resident_current_bytes: (storage.len() * size_of::<u64>()) as u64,
                resident_capacity_bytes: (storage.capacity() * size_of::<u64>()) as u64,
            })
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            self.completions.fetch_add(1, Ordering::SeqCst);
            Ok(self.next)
        }
    }

    #[derive(Default)]
    struct SumKernel {
        commits: Vec<(WorkIdentity, u64)>,
    }

    impl PartitionedKernel<Vec<u64>> for SumKernel {
        type Partition = usize;
        type Partial = u64;
        type Completion = Vec<(WorkIdentity, u64)>;
        type Error = Infallible;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            values: &Vec<u64>,
        ) -> Result<usize, Self::Error> {
            Ok(values.len())
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _values: &Vec<u64>,
            local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            Ok(KernelPartition::ordered(
                local_ordinal as u64,
                0,
                local_ordinal as u64,
                local_ordinal,
            ))
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            values: &Vec<u64>,
            partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            Ok(values[*partition])
        }

        fn commit(
            &mut self,
            work: WorkIdentity,
            _values: &Vec<u64>,
            partial: Self::Partial,
        ) -> Result<(), Self::Error> {
            self.commits.push((work, partial));
            Ok(())
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok(self.commits)
        }
    }

    fn run_blocks(
        blocks: Vec<Vec<u64>>,
        slots: usize,
        workers: usize,
    ) -> BoundedStreamOutcome<usize, Vec<(WorkIdentity, u64)>> {
        let maximum_partitions_per_block = blocks.iter().map(Vec::len).max().unwrap_or(1);
        let source_capacity_bytes = slots
            .checked_mul(maximum_partitions_per_block.next_power_of_two())
            .and_then(|values| values.checked_mul(size_of::<u64>()))
            .unwrap()
            .max(64) as u64;
        let source = NumberSource {
            blocks,
            next: 0,
            pointers: Arc::new(Mutex::new(Vec::new())),
            completions: Arc::new(AtomicUsize::new(0)),
        };
        execute_bounded(
            BoundedStreamPlan::new::<usize, u64>(
                slots,
                workers,
                source_capacity_bytes,
                maximum_partitions_per_block,
                0,
            )
            .unwrap(),
            3,
            source,
            SumKernel::default(),
        )
        .unwrap()
    }

    fn run(slots: usize, workers: usize) -> BoundedStreamOutcome<usize, Vec<(WorkIdentity, u64)>> {
        run_blocks(
            vec![vec![1, 2, 3], vec![4, 5], vec![6, 7, 8, 9]],
            slots,
            workers,
        )
    }

    #[test]
    fn one_and_two_slots_have_identical_ordered_results() {
        let inline = run(1, 1);
        let overlapped = run(2, 1);
        assert_eq!(inline.source_completion, overlapped.source_completion);
        assert_eq!(inline.kernel_completion, overlapped.kernel_completion);
        assert_eq!(inline.measurements.blocks_filled, 3);
        assert_eq!(overlapped.measurements.blocks_filled, 3);
        assert_eq!(inline.measurements.ready_queue_high_water, 0);
        assert_eq!(inline.measurements.producer_wait_nanos, 0);
        assert_eq!(inline.measurements.consumer_wait_nanos, 0);
        assert_eq!(inline.measurements.source_starved_nanos, 0);
        assert_eq!(inline.measurements.terminal_wait_nanos, 0);
        assert_eq!(overlapped.measurements.ready_queue_high_water, 0);
        assert_eq!(
            overlapped.measurements.consumer_wait_nanos,
            overlapped
                .measurements
                .source_starved_nanos
                .saturating_add(overlapped.measurements.terminal_wait_nanos)
        );
        assert!(
            inline.measurements.peak_kernel_window_capacity_bytes
                <= inline.measurements.planned_kernel_window_capacity_bytes
        );
    }

    #[test]
    fn worker_count_does_not_change_stable_commit_order() {
        let serial = run(2, 1);
        let parallel = run(2, 2);
        assert_eq!(serial.kernel_completion, parallel.kernel_completion);
        assert_eq!(serial.measurements.partitions_executed, 9);
        assert_eq!(serial.measurements.commits_completed, 9);
        assert_eq!(serial.measurements.workers_with_nonzero_partitions, 1);
        assert_eq!(serial.measurements.minimum_partitions_per_active_worker, 9);
        assert_eq!(serial.measurements.maximum_partitions_per_active_worker, 9);
        assert_eq!(serial.measurements.worker_slots.len(), 1);
        assert_eq!(serial.measurements.worker_slots[0].work_units, 9);
        assert!(serial.measurements.worker_slots[0].active_nanos > 0);
        assert_eq!(serial.measurements.peak_worker_stack_capacity_bytes, 0);
        assert_eq!(parallel.measurements.partitions_executed, 9);
        assert_eq!(parallel.measurements.commits_completed, 9);
        assert_eq!(parallel.measurements.workers_with_nonzero_partitions, 2);
        assert_eq!(
            parallel.measurements.minimum_partitions_per_active_worker,
            4
        );
        assert_eq!(
            parallel.measurements.maximum_partitions_per_active_worker,
            5
        );
        assert_eq!(
            parallel.measurements.peak_worker_stack_capacity_bytes,
            2 * BOUNDED_WORKER_STACK_BYTES as u64
        );
        assert_eq!(
            serial.measurements.executed_work_identity_digest,
            parallel.measurements.executed_work_identity_digest
        );
        assert_eq!(
            serial.measurements.committed_work_identity_digest,
            parallel.measurements.committed_work_identity_digest
        );
        assert_eq!(
            parallel.measurements.executed_work_identity_digest,
            parallel.measurements.committed_work_identity_digest
        );
        assert_eq!(
            parallel
                .measurements
                .worker_slots
                .iter()
                .map(|worker| worker.work_units)
                .collect::<Vec<_>>(),
            vec![5, 4]
        );
        assert!(
            parallel
                .measurements
                .worker_slots
                .iter()
                .all(|worker| worker.active_nanos > 0 && worker.wait_nanos > 0)
        );
    }

    #[test]
    fn fixed_worker_team_is_reused_across_blocks_and_waves() {
        let blocks = vec![(1..=6).collect::<Vec<_>>(), (7..=12).collect::<Vec<_>>()];
        let serial = run_blocks(blocks.clone(), 2, 1);
        let parallel = run_blocks(blocks, 2, 3);

        assert_eq!(serial.kernel_completion, parallel.kernel_completion);
        assert_eq!(parallel.measurements.blocks_filled, 2);
        assert_eq!(parallel.measurements.partitions_executed, 12);
        assert_eq!(parallel.measurements.commits_completed, 12);
        assert_eq!(parallel.measurements.dispatch_waves, 4);
        assert_eq!(parallel.measurements.worker_threads_started, 3);
        assert_eq!(serial.measurements.worker_pool_entries, 0);
        assert_eq!(parallel.measurements.worker_pool_entries, 1);
        assert_eq!(
            parallel
                .measurements
                .worker_slots
                .iter()
                .map(|worker| worker.work_units)
                .collect::<Vec<_>>(),
            vec![4, 4, 4]
        );
        assert_eq!(
            serial.measurements.executed_work_identity_digest,
            parallel.measurements.executed_work_identity_digest
        );
        assert_eq!(
            serial.measurements.committed_work_identity_digest,
            parallel.measurements.committed_work_identity_digest
        );
    }

    #[derive(Default)]
    struct DynamicPartialKernel {
        committed_capacities: Vec<u64>,
    }

    impl PartitionedKernel<Vec<u64>> for DynamicPartialKernel {
        type Partition = usize;
        type Partial = Vec<u8>;
        type Completion = Vec<u64>;
        type Error = Infallible;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            _values: &Vec<u64>,
        ) -> Result<usize, Self::Error> {
            Ok(2)
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _values: &Vec<u64>,
            local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            Ok(KernelPartition::ordered(
                local_ordinal as u64,
                0,
                local_ordinal as u64,
                local_ordinal,
            ))
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            _values: &Vec<u64>,
            partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            Ok(Vec::with_capacity((partition + 1) * 64))
        }

        fn partial_dynamic_capacity_bytes(&self, partial: &Self::Partial) -> u64 {
            partial.capacity() as u64
        }

        fn commit(
            &mut self,
            _work: WorkIdentity,
            _values: &Vec<u64>,
            partial: Self::Partial,
        ) -> Result<(), Self::Error> {
            self.committed_capacities.push(partial.capacity() as u64);
            Ok(())
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok(self.committed_capacities)
        }
    }

    #[test]
    fn partial_peak_is_measured_inside_the_planned_kernel_window() {
        let outcome = execute_bounded(
            BoundedStreamPlan::new::<usize, Vec<u8>>(1, 2, 64, 2, 192).unwrap(),
            0,
            NumberSource {
                blocks: vec![vec![1]],
                next: 0,
                pointers: Arc::new(Mutex::new(Vec::new())),
                completions: Arc::new(AtomicUsize::new(0)),
            },
            DynamicPartialKernel::default(),
        )
        .unwrap();
        assert_eq!(
            outcome.measurements.peak_partial_dynamic_capacity_bytes,
            outcome.kernel_completion.iter().sum::<u64>()
        );
        assert!(
            outcome.measurements.peak_kernel_window_capacity_bytes
                <= outcome.measurements.planned_kernel_window_capacity_bytes
        );
    }

    #[test]
    fn kernel_partition_count_must_fit_the_planned_window() {
        let result = execute_bounded(
            BoundedStreamPlan::new::<usize, u64>(1, 1, 64, 2, 0).unwrap(),
            0,
            NumberSource {
                blocks: vec![vec![1, 2, 3]],
                next: 0,
                pointers: Arc::new(Mutex::new(Vec::new())),
                completions: Arc::new(AtomicUsize::new(0)),
            },
            SumKernel::default(),
        );
        let failure = result.unwrap_err();
        assert!(matches!(
            *failure.cause,
            BoundedStreamError::InvalidKernelPlan
        ));
        assert_eq!(failure.measurements.blocks_filled, 1);
        assert!(failure.measurements.wall_nanos > 0);
    }

    #[test]
    fn plan_rejects_unbounded_or_empty_execution() {
        assert_eq!(
            BoundedStreamPlan::new::<usize, u64>(0, 1, 64, 4, 0),
            Err(BoundedStreamPlanError::SourceSlots)
        );
        assert_eq!(
            BoundedStreamPlan::new::<usize, u64>(3, 1, 64, 4, 0),
            Err(BoundedStreamPlanError::SourceSlots)
        );
        assert_eq!(
            BoundedStreamPlan::new::<usize, u64>(1, 0, 64, 4, 0),
            Err(BoundedStreamPlanError::Workers)
        );
        assert_eq!(
            BoundedStreamPlan::new::<usize, u64>(1, 1, 0, 4, 0),
            Err(BoundedStreamPlanError::SourceCapacity)
        );
        assert_eq!(
            BoundedStreamPlan::new::<usize, u64>(1, 1, 64, 0, 0),
            Err(BoundedStreamPlanError::Partitions)
        );
    }

    #[test]
    fn overlapped_source_reuses_only_the_planned_storage_slots() {
        let pointers = Arc::new(Mutex::new(Vec::new()));
        let completions = Arc::new(AtomicUsize::new(0));
        let outcome = execute_bounded(
            BoundedStreamPlan::new::<usize, u64>(2, 1, 64, 4, 0).unwrap(),
            0,
            NumberSource {
                blocks: vec![vec![1], vec![2], vec![3], vec![4]],
                next: 0,
                pointers: Arc::clone(&pointers),
                completions: Arc::clone(&completions),
            },
            SumKernel::default(),
        )
        .unwrap();
        let pointers = pointers.lock().unwrap();
        let unique = pointers
            .iter()
            .map(|(_, pointer)| *pointer)
            .collect::<BTreeSet<_>>();
        assert_eq!(pointers.len(), 4);
        assert_eq!(unique.len(), 2);
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.measurements.peak_live_source_blocks, 2);
        assert!(outcome.measurements.peak_live_source_current_bytes <= 16);
        assert!(outcome.measurements.peak_live_source_capacity_bytes <= 64);
        assert_eq!(outcome.measurements.ready_queue_current_bytes_high_water, 0);
        assert_eq!(
            outcome.measurements.ready_queue_capacity_bytes_high_water,
            0
        );
        assert!(outcome.measurements.lease_return_nanos > 0);
        assert!(outcome.measurements.wall_nanos > 0);
    }

    #[derive(Debug, Clone, Copy)]
    struct TestFailure;

    impl fmt::Display for TestFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("injected failure")
        }
    }

    impl Error for TestFailure {}

    struct FailingKernel;

    impl PartitionedKernel<Vec<u64>> for FailingKernel {
        type Partition = ();
        type Partial = ();
        type Completion = ();
        type Error = TestFailure;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
        ) -> Result<usize, Self::Error> {
            Ok(1)
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
            local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            debug_assert_eq!(local_ordinal, 0);
            Ok(KernelPartition::exclusive(0, 0, ()))
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            _partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            Ok(())
        }

        fn commit(
            &mut self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            (): Self::Partial,
        ) -> Result<(), Self::Error> {
            Err(TestFailure)
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn consumer_failure_cancels_without_source_completion() {
        let completions = Arc::new(AtomicUsize::new(0));
        let result = execute_bounded(
            BoundedStreamPlan::new::<(), ()>(2, 1, 64, 1, 0).unwrap(),
            0,
            NumberSource {
                blocks: (0..100).map(|value| vec![value]).collect(),
                next: 0,
                pointers: Arc::new(Mutex::new(Vec::new())),
                completions: Arc::clone(&completions),
            },
            FailingKernel,
        );
        let failure = result.unwrap_err();
        assert!(matches!(
            *failure.cause,
            BoundedStreamError::Kernel(TestFailure)
        ));
        assert!(failure.measurements.blocks_filled > 0);
        assert_eq!(
            failure.measurements.source_read_operations,
            failure.measurements.blocks_filled
        );
        assert!(failure.measurements.wall_nanos > 0);
        assert_eq!(completions.load(Ordering::SeqCst), 0);
    }

    struct CancellationDelayedSource {
        next: usize,
        second_fill_started: Arc<AtomicBool>,
        completions: Arc<AtomicUsize>,
    }

    impl OrderedBlockSource for CancellationDelayedSource {
        type Storage = Vec<u64>;
        type Completion = ();
        type Error = Infallible;

        fn create_storage(&self, _slot: usize) -> Self::Storage {
            Vec::with_capacity(1)
        }

        fn fill(
            &mut self,
            _block_ordinal: u64,
            storage: &mut Self::Storage,
            cancellation: SourceFillCancellation<'_>,
        ) -> Result<SourcePoll, Self::Error> {
            if self.next == 2 {
                return Ok(SourcePoll::Exhausted);
            }
            if self.next == 1 {
                self.second_fill_started.store(true, Ordering::Release);
                while !cancellation.is_cancelled() {
                    std::thread::yield_now();
                }
            }
            storage.clear();
            storage.push(self.next as u64);
            let source_read_operations = if self.next == 0 { 3 } else { 5 };
            self.next += 1;
            Ok(SourcePoll::Ready {
                source_ordinal: 0,
                logical_bytes: 8,
                source_read_operations,
                resident_current_bytes: 8,
                resident_capacity_bytes: 8,
            })
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            self.completions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailAfterReadAheadStarts {
        second_fill_started: Arc<AtomicBool>,
    }

    impl PartitionedKernel<Vec<u64>> for FailAfterReadAheadStarts {
        type Partition = ();
        type Partial = ();
        type Completion = ();
        type Error = TestFailure;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
        ) -> Result<usize, Self::Error> {
            Ok(1)
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
            _local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            Ok(KernelPartition::exclusive(0, 0, ()))
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            _partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            Ok(())
        }

        fn commit(
            &mut self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            (): Self::Partial,
        ) -> Result<(), Self::Error> {
            while !self.second_fill_started.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            Err(TestFailure)
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn completed_read_ahead_fill_is_measured_after_consumer_cancellation() {
        let second_fill_started = Arc::new(AtomicBool::new(false));
        let completions = Arc::new(AtomicUsize::new(0));
        let result = execute_bounded(
            BoundedStreamPlan::new::<(), ()>(2, 1, 16, 1, 0).unwrap(),
            0,
            CancellationDelayedSource {
                next: 0,
                second_fill_started: Arc::clone(&second_fill_started),
                completions: Arc::clone(&completions),
            },
            FailAfterReadAheadStarts {
                second_fill_started,
            },
        );
        let failure = result.unwrap_err();
        assert!(matches!(
            *failure.cause,
            BoundedStreamError::Kernel(TestFailure)
        ));
        assert_eq!(failure.measurements.blocks_filled, 2);
        assert_eq!(failure.measurements.logical_source_bytes, 16);
        assert_eq!(failure.measurements.source_read_operations, 8);
        assert_eq!(failure.measurements.peak_live_source_blocks, 2);
        assert_eq!(completions.load(Ordering::SeqCst), 0);
    }

    struct FailingSource {
        emitted: bool,
        completions: Arc<AtomicUsize>,
    }

    impl OrderedBlockSource for FailingSource {
        type Storage = Vec<u64>;
        type Completion = ();
        type Error = TestFailure;

        fn create_storage(&self, _slot: usize) -> Self::Storage {
            Vec::with_capacity(1)
        }

        fn fill(
            &mut self,
            _block_ordinal: u64,
            storage: &mut Self::Storage,
            _cancellation: SourceFillCancellation<'_>,
        ) -> Result<SourcePoll, Self::Error> {
            if self.emitted {
                return Err(TestFailure);
            }
            self.emitted = true;
            storage.clear();
            storage.push(1);
            Ok(SourcePoll::Ready {
                source_ordinal: 0,
                logical_bytes: 8,
                source_read_operations: 1,
                resident_current_bytes: 8,
                resident_capacity_bytes: 8,
            })
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            self.completions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn source_failure_is_retained_without_source_completion() {
        let completions = Arc::new(AtomicUsize::new(0));
        let result = execute_bounded(
            BoundedStreamPlan::new::<usize, u64>(2, 1, 64, 4, 0).unwrap(),
            0,
            FailingSource {
                emitted: false,
                completions: Arc::clone(&completions),
            },
            SumKernel::default(),
        );
        let failure = result.unwrap_err();
        assert!(matches!(
            *failure.cause,
            BoundedStreamError::Source(TestFailure)
        ));
        assert_eq!(failure.measurements.blocks_filled, 1);
        assert_eq!(failure.measurements.source_read_operations, 1);
        assert!(failure.measurements.source_fill_nanos > 0);
        assert_eq!(completions.load(Ordering::SeqCst), 0);
    }

    #[derive(Default)]
    struct ExclusiveProbe {
        active: AtomicUsize,
        maximum: AtomicUsize,
    }

    impl PartitionedKernel<Vec<u64>> for ExclusiveProbe {
        type Partition = ();
        type Partial = ();
        type Completion = usize;
        type Error = Infallible;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
        ) -> Result<usize, Self::Error> {
            Ok(8)
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
            local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            Ok(KernelPartition::exclusive(local_ordinal as u64, 7, ()))
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            _partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            for _ in 0..32 {
                std::thread::yield_now();
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }

        fn commit(
            &mut self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            (): Self::Partial,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok(self.maximum.load(Ordering::SeqCst))
        }
    }

    #[test]
    fn exclusive_regions_never_execute_concurrently() {
        let source = NumberSource {
            blocks: vec![vec![1]],
            next: 0,
            pointers: Arc::new(Mutex::new(Vec::new())),
            completions: Arc::new(AtomicUsize::new(0)),
        };
        let outcome = execute_bounded(
            BoundedStreamPlan::new::<(), ()>(1, 8, 32, 8, 0).unwrap(),
            0,
            source,
            ExclusiveProbe::default(),
        )
        .unwrap();
        assert_eq!(outcome.kernel_completion, 1);
    }

    struct DynamicExclusiveKernel;

    impl PartitionedKernel<Vec<u64>> for DynamicExclusiveKernel {
        type Partition = Vec<u8>;
        type Partial = ();
        type Completion = ();
        type Error = Infallible;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
        ) -> Result<usize, Self::Error> {
            Ok(2)
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
            local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            let mut payload = Vec::with_capacity(256);
            payload.push(local_ordinal as u8);
            Ok(KernelPartition::exclusive(local_ordinal as u64, 7, payload))
        }

        fn partition_dynamic_capacity_bytes(&self, partition: &Self::Partition) -> u64 {
            partition.capacity() as u64
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            _partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            Ok(())
        }

        fn commit(
            &mut self,
            _work: WorkIdentity,
            _storage: &Vec<u64>,
            (): Self::Partial,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn deferred_partition_is_charged_to_the_kernel_window() {
        let result = execute_bounded(
            BoundedStreamPlan::new::<Vec<u8>, ()>(1, 2, 32, 2, 256).unwrap(),
            0,
            NumberSource {
                blocks: vec![vec![1]],
                next: 0,
                pointers: Arc::new(Mutex::new(Vec::new())),
                completions: Arc::new(AtomicUsize::new(0)),
            },
            DynamicExclusiveKernel,
        );
        let failure = result.unwrap_err();
        assert!(matches!(
            *failure.cause,
            BoundedStreamError::ResidencyExceeded
        ));
        assert_eq!(failure.measurements.blocks_filled, 1);
    }

    const LARGE_GATE_ROWS: usize = 6_709_290;
    const LARGE_GATE_CHANNELS: usize = 1_024;
    const LARGE_GATE_CORRELATIONS: usize = 2;
    const LARGE_GATE_BLOCK_ROWS: usize = 1_024;
    const LARGE_GATE_SAMPLES: u64 = 13_740_625_920;
    const LARGE_GATE_ACCEPTED: u64 = 12_412_252_160;
    const LARGE_GATE_REAL_BITS: u64 = 0x423b_db13_113a_47c7;
    const LARGE_GATE_IMAG_BITS: u64 = 0xc1c2_e4fd_fd88_9d2d;
    const LARGE_GATE_WEIGHT_BITS: u64 = 0x4207_21a6_8210_e413;
    const LARGE_GATE_SOURCE_CAPACITY_BYTES: u64 = 128 * 1024 * 1024;

    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    struct LargeGateDigest {
        real: f64,
        imag: f64,
        weight: f64,
        accepted: u64,
    }

    struct LargeGateStorage {
        buffer: VisibilityBuffer,
    }

    #[derive(Debug)]
    struct LargeGateSourceCompletion {
        passes: u64,
        rows: u64,
        samples: u64,
        fill_operations: u64,
        modeled_physical_bytes: u64,
        named_row_projection_bytes: u64,
        explicit_adaptation_copy_bytes: u64,
        implicit_copy_bytes: u64,
        warmup_fills: u64,
        reused_fills: u64,
        rss_samples: [u64; 8],
        rss_sample_count: usize,
        terminal: bool,
    }

    struct LargeGateSource {
        measurement_set: MeasurementSet,
        next_row: usize,
        rows: u64,
        samples: u64,
        fills: u64,
        modeled_physical_bytes: u64,
        named_row_projection_bytes: u64,
        explicit_adaptation_copy_bytes: u64,
        implicit_copy_bytes: u64,
        warmup_fills: u64,
        reused_fills: u64,
        rss_samples: [u64; 8],
        rss_sample_count: usize,
        started: Instant,
        last_progress: Instant,
    }

    impl LargeGateSource {
        fn new(path: PathBuf) -> Result<Self, casa_ms::MsError> {
            let measurement_set = MeasurementSet::open(path)?;
            Ok(Self {
                measurement_set,
                next_row: 0,
                rows: 0,
                samples: 0,
                fills: 0,
                modeled_physical_bytes: 0,
                named_row_projection_bytes: 0,
                explicit_adaptation_copy_bytes: 0,
                implicit_copy_bytes: 0,
                warmup_fills: 0,
                reused_fills: 0,
                rss_samples: [0; 8],
                rss_sample_count: 0,
                started: Instant::now(),
                last_progress: Instant::now(),
            })
        }

        fn sample_rss(&mut self) {
            if self.rss_sample_count < self.rss_samples.len() {
                self.rss_samples[self.rss_sample_count] = peak_rss_bytes();
                self.rss_sample_count += 1;
            }
        }
    }

    impl OrderedBlockSource for LargeGateSource {
        type Storage = LargeGateStorage;
        type Completion = LargeGateSourceCompletion;
        type Error = casa_ms::MsError;

        fn create_storage(&self, _slot: usize) -> Self::Storage {
            LargeGateStorage {
                buffer: VisibilityBuffer::default(),
            }
        }

        fn fill(
            &mut self,
            _block_ordinal: u64,
            storage: &mut Self::Storage,
            _cancellation: SourceFillCancellation<'_>,
        ) -> Result<SourcePoll, Self::Error> {
            if self.next_row == LARGE_GATE_ROWS {
                return Ok(SourcePoll::Exhausted);
            }
            let row_end = self
                .next_row
                .saturating_add(LARGE_GATE_BLOCK_ROWS)
                .min(LARGE_GATE_ROWS);
            let request = VisibilityBufferRequest::imaging(
                VisibilityDataColumn::Data,
                (self.next_row..row_end).collect(),
                0,
                LARGE_GATE_CHANNELS,
            );
            let fill = self
                .measurement_set
                .fill_visibility_buffer(&request, &mut storage.buffer)?;
            assert_eq!(storage.buffer.channel_count, LARGE_GATE_CHANNELS);
            assert_eq!(storage.buffer.corr_count, LARGE_GATE_CORRELATIONS);

            if fill.allocation.grown_or_retyped_buffers == 0 {
                self.reused_fills += 1;
            } else {
                self.warmup_fills += 1;
            }
            let rows = row_end - self.next_row;
            let samples = rows
                .saturating_mul(LARGE_GATE_CHANNELS)
                .saturating_mul(LARGE_GATE_CORRELATIONS);
            self.next_row = row_end;
            self.rows += rows as u64;
            self.samples += samples as u64;
            self.fills += 1;
            self.modeled_physical_bytes = self
                .modeled_physical_bytes
                .saturating_add(fill.modeled_physical_read_bytes);
            self.named_row_projection_bytes = self
                .named_row_projection_bytes
                .saturating_add((rows * size_of::<usize>()) as u64);
            self.explicit_adaptation_copy_bytes = self
                .explicit_adaptation_copy_bytes
                .saturating_add(fill.explicit_adaptation_copy_bytes);
            self.implicit_copy_bytes = self
                .implicit_copy_bytes
                .saturating_add(fill.implicit_copy_bytes);

            if self.fills == 2 || self.fills.is_multiple_of(1_024) {
                self.sample_rss();
            }
            if self.last_progress.elapsed() >= Duration::from_secs(30)
                || self.next_row == LARGE_GATE_ROWS
            {
                eprintln!(
                    "issue540_large_gate rows={}/{} fills={} elapsed_s={:.3} rss_bytes={}",
                    self.next_row,
                    LARGE_GATE_ROWS,
                    self.fills,
                    self.started.elapsed().as_secs_f64(),
                    peak_rss_bytes(),
                );
                self.last_progress = Instant::now();
            }

            let (resident_current_bytes, resident_capacity_bytes) =
                large_gate_residency(&storage.buffer);
            Ok(SourcePoll::Ready {
                source_ordinal: 0,
                logical_bytes: fill.logical_output_bytes,
                source_read_operations: u64::try_from(fill.columns.len())
                    .expect("large-gate column count fits u64"),
                resident_current_bytes,
                resident_capacity_bytes,
            })
        }

        fn complete(mut self) -> Result<Self::Completion, Self::Error> {
            self.sample_rss();
            Ok(LargeGateSourceCompletion {
                passes: 1,
                rows: self.rows,
                samples: self.samples,
                fill_operations: self.fills,
                modeled_physical_bytes: self.modeled_physical_bytes,
                named_row_projection_bytes: self.named_row_projection_bytes,
                explicit_adaptation_copy_bytes: self.explicit_adaptation_copy_bytes,
                implicit_copy_bytes: self.implicit_copy_bytes,
                warmup_fills: self.warmup_fills,
                reused_fills: self.reused_fills,
                rss_samples: self.rss_samples,
                rss_sample_count: self.rss_sample_count,
                terminal: true,
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct LargeGateKernelError(&'static str);

    impl fmt::Display for LargeGateKernelError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for LargeGateKernelError {}

    #[derive(Default)]
    struct LargeGateKernel {
        digest: LargeGateDigest,
        samples: u64,
    }

    impl PartitionedKernel<LargeGateStorage> for LargeGateKernel {
        type Partition = ();
        type Partial = ();
        type Completion = (LargeGateDigest, u64);
        type Error = LargeGateKernelError;

        fn partition_count(
            &self,
            _block: BlockIdentity,
            _storage: &LargeGateStorage,
        ) -> Result<usize, Self::Error> {
            Ok(1)
        }

        fn partition(
            &self,
            _block: BlockIdentity,
            _storage: &LargeGateStorage,
            local_ordinal: usize,
        ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
            debug_assert_eq!(local_ordinal, 0);
            Ok(KernelPartition::exclusive(0, 0, ()))
        }

        fn execute(
            &self,
            _work: WorkIdentity,
            _storage: &LargeGateStorage,
            _partition: &Self::Partition,
        ) -> Result<Self::Partial, Self::Error> {
            Ok(())
        }

        fn commit(
            &mut self,
            _work: WorkIdentity,
            storage: &LargeGateStorage,
            (): Self::Partial,
        ) -> Result<(), Self::Error> {
            let samples = consume_large_gate_block(&storage.buffer, &mut self.digest)?;
            self.samples = self
                .samples
                .checked_add(samples)
                .ok_or(LargeGateKernelError("sample count overflow"))?;
            Ok(())
        }

        fn complete(self) -> Result<Self::Completion, Self::Error> {
            Ok((self.digest, self.samples))
        }
    }

    fn large_gate_residency(buffer: &VisibilityBuffer) -> (u64, u64) {
        let mut current = 0usize;
        let mut capacity = 0usize;
        macro_rules! add_vec {
            ($values:expr, $type:ty) => {
                if let Some(values) = $values.as_ref() {
                    current = current.saturating_add(values.len() * size_of::<$type>());
                    capacity = capacity.saturating_add(values.capacity() * size_of::<$type>());
                }
            };
        }
        current = current.saturating_add(buffer.row_indices.len() * size_of::<usize>());
        capacity = capacity.saturating_add(buffer.row_indices.capacity() * size_of::<usize>());
        match buffer.data.as_ref() {
            Some(VisibilityComplexSamples::Complex32(values)) => {
                current = current.saturating_add(values.len() * size_of_val(&values[0]));
                capacity = capacity.saturating_add(values.capacity() * size_of_val(&values[0]));
            }
            Some(VisibilityComplexSamples::Complex64(values)) => {
                current = current.saturating_add(values.len() * size_of_val(&values[0]));
                capacity = capacity.saturating_add(values.capacity() * size_of_val(&values[0]));
            }
            None => {}
        }
        add_vec!(buffer.flags, bool);
        match buffer.weights.as_ref() {
            Some(VisibilityFloatSamples::Float32(values)) => {
                current = current.saturating_add(values.len() * size_of::<f32>());
                capacity = capacity.saturating_add(values.capacity() * size_of::<f32>());
            }
            Some(VisibilityFloatSamples::Float64(values)) => {
                current = current.saturating_add(values.len() * size_of::<f64>());
                capacity = capacity.saturating_add(values.capacity() * size_of::<f64>());
            }
            None => {}
        }
        match buffer.weight_spectrum.as_ref() {
            Some(VisibilityFloatSamples::Float32(values)) => {
                current = current.saturating_add(values.len() * size_of::<f32>());
                capacity = capacity.saturating_add(values.capacity() * size_of::<f32>());
            }
            Some(VisibilityFloatSamples::Float64(values)) => {
                current = current.saturating_add(values.len() * size_of::<f64>());
                capacity = capacity.saturating_add(values.capacity() * size_of::<f64>());
            }
            None => {}
        }
        add_vec!(buffer.uvw, f64);
        add_vec!(buffer.antenna1, i32);
        add_vec!(buffer.antenna2, i32);
        add_vec!(buffer.data_desc_ids, i32);
        add_vec!(buffer.field_ids, i32);
        add_vec!(buffer.flag_row, bool);
        add_vec!(buffer.time, f64);
        add_vec!(buffer.interval, f64);
        add_vec!(buffer.exposure, f64);
        add_vec!(buffer.array_ids, i32);
        add_vec!(buffer.observation_ids, i32);
        add_vec!(buffer.scan_numbers, i32);
        add_vec!(buffer.state_ids, i32);
        (current as u64, capacity as u64)
    }

    fn consume_large_gate_block(
        buffer: &VisibilityBuffer,
        digest: &mut LargeGateDigest,
    ) -> Result<u64, LargeGateKernelError> {
        let VisibilityComplexSamples::Complex32(data) = buffer
            .data
            .as_ref()
            .ok_or(LargeGateKernelError("DATA was not filled"))?
        else {
            return Err(LargeGateKernelError("DATA is not Complex32"));
        };
        let flags = buffer
            .flags
            .as_deref()
            .ok_or(LargeGateKernelError("FLAG was not filled"))?;
        let row_flags = buffer
            .flag_row
            .as_deref()
            .ok_or(LargeGateKernelError("FLAG_ROW was not filled"))?;
        let VisibilityFloatSamples::Float32(weights) = buffer
            .weights
            .as_ref()
            .ok_or(LargeGateKernelError("WEIGHT was not filled"))?
        else {
            return Err(LargeGateKernelError("WEIGHT is not Float32"));
        };
        let uvw = buffer
            .uvw
            .as_deref()
            .ok_or(LargeGateKernelError("UVW was not filled"))?;
        let rows = buffer.row_count();
        let correlations = buffer.corr_count;
        let plane_samples = rows * correlations;
        for channel in 0..buffer.channel_count {
            let plane_start = channel * plane_samples;
            let plane_end = plane_start + plane_samples;
            let data_plane = &data[plane_start..plane_end];
            let flag_plane = &flags[plane_start..plane_end];
            let spectral_factor = 1.0 + channel as f64 * 1.0e-6;
            for (row, row_flag) in row_flags.iter().copied().enumerate().take(rows) {
                let uvw_start = row * 3;
                let phase = (uvw[uvw_start] * 1.0e-9
                    + uvw[uvw_start + 1] * 3.0e-10
                    + uvw[uvw_start + 2] * 7.0e-10)
                    * spectral_factor;
                let (phase_imag, phase_real) = phase.sin_cos();
                let run_start = row * correlations;
                let run_end = run_start + correlations;
                for ((visibility, channel_flag), input_weight) in data_plane[run_start..run_end]
                    .iter()
                    .zip(&flag_plane[run_start..run_end])
                    .zip(&weights[run_start..run_end])
                {
                    let weight = f64::from(*input_weight);
                    let real = f64::from(visibility.re);
                    let imag = f64::from(visibility.im);
                    if *channel_flag
                        || row_flag
                        || !weight.is_finite()
                        || weight <= 0.0
                        || !real.is_finite()
                        || !imag.is_finite()
                    {
                        continue;
                    }
                    let effective_weight = weight * spectral_factor;
                    digest.real += (real * phase_real - imag * phase_imag) * effective_weight;
                    digest.imag += (real * phase_imag + imag * phase_real) * effective_weight;
                    digest.weight += effective_weight;
                    digest.accepted += 1;
                }
            }
        }
        Ok((rows * buffer.channel_count * correlations) as u64)
    }

    fn peak_rss_bytes() -> u64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return 0;
        }
        let maximum = unsafe { usage.assume_init() }.ru_maxrss;
        #[cfg(target_os = "macos")]
        {
            u64::try_from(maximum).unwrap_or(0)
        }
        #[cfg(not(target_os = "macos"))]
        {
            u64::try_from(maximum).unwrap_or(0).saturating_mul(1_024)
        }
    }

    #[test]
    #[ignore = "requires the directly mounted 106.9 GiB issue #540 dataset"]
    fn issue540_complete_large_ms_uses_one_bounded_overlapped_pass() {
        let path = env::var_os("CASA_RS_ISSUE540_LARGE_MS")
            .map(PathBuf::from)
            .expect("set CASA_RS_ISSUE540_LARGE_MS to wave1-alma-mosaic-large.ms");
        let source = LargeGateSource::new(path).expect("open large MeasurementSet");
        assert_eq!(source.measurement_set.row_count(), LARGE_GATE_ROWS);
        let outcome = execute_bounded(
            BoundedStreamPlan::new::<(), ()>(2, 1, LARGE_GATE_SOURCE_CAPACITY_BYTES, 1, 0)
                .expect("valid large gate plan"),
            0,
            source,
            LargeGateKernel::default(),
        )
        .expect("large bounded stream completes");
        let source = outcome.source_completion;
        let (digest, samples) = outcome.kernel_completion;
        let measurements = outcome.measurements;

        assert_eq!(source.passes, 1);
        assert_eq!(source.rows, LARGE_GATE_ROWS as u64);
        assert_eq!(source.samples, LARGE_GATE_SAMPLES);
        assert_eq!(samples, LARGE_GATE_SAMPLES);
        assert_eq!(source.fill_operations, 6_553);
        assert_eq!(digest.accepted, LARGE_GATE_ACCEPTED);
        assert_eq!(digest.real.to_bits(), LARGE_GATE_REAL_BITS);
        assert_eq!(digest.imag.to_bits(), LARGE_GATE_IMAG_BITS);
        assert_eq!(digest.weight.to_bits(), LARGE_GATE_WEIGHT_BITS);
        assert!(
            source.explicit_adaptation_copy_bytes > source.named_row_projection_bytes,
            "the gate must report every measured arrangement copy, not only row projection"
        );
        assert_eq!(source.implicit_copy_bytes, 0);
        assert_eq!(
            source.named_row_projection_bytes,
            (LARGE_GATE_ROWS * size_of::<usize>()) as u64
        );
        assert!(source.warmup_fills <= 2);
        assert_eq!(source.reused_fills + source.warmup_fills, 6_553);
        assert!(source.reused_fills >= 6_551);
        assert!(source.terminal);
        assert_eq!(measurements.blocks_filled, 6_553);
        assert_eq!(measurements.source_slots, 2);
        assert_eq!(measurements.workers, 1);
        assert!(measurements.peak_live_source_blocks <= 2);
        assert!(
            measurements.peak_live_source_capacity_bytes
                <= measurements.planned_source_capacity_bytes
        );
        assert!(
            measurements.ready_queue_capacity_bytes_high_water
                <= measurements.planned_source_capacity_bytes
        );
        assert_eq!(measurements.ready_queue_high_water, 0);
        assert!(measurements.producer_wait_nanos > 0);
        assert!(measurements.consumer_wait_nanos > 0);
        assert!(measurements.overlap_nanos > 0);
        let serial_stage_sum = measurements
            .source_fill_nanos
            .saturating_add(measurements.commit_nanos);
        assert!(measurements.wall_nanos.saturating_mul(100) < serial_stage_sum.saturating_mul(95));
        assert!(source.rss_sample_count >= 4);
        let rss = &source.rss_samples[..source.rss_sample_count];
        let warm_rss = rss[1..].iter().copied().min().unwrap_or(0);
        let peak_rss = rss[1..].iter().copied().max().unwrap_or(0);
        assert!(peak_rss.saturating_sub(warm_rss) <= 64 * 1024 * 1024);

        println!(
            "{}",
            serde_json::json!({
                "gate": "issue540-complete-large-ms",
                "passes": source.passes,
                "rows": source.rows,
                "samples": source.samples,
                "accepted": digest.accepted,
                "digest_real_bits": format!("{:016x}", digest.real.to_bits()),
                "digest_imag_bits": format!("{:016x}", digest.imag.to_bits()),
                "digest_weight_bits": format!("{:016x}", digest.weight.to_bits()),
                "fill_operations": source.fill_operations,
                "logical_source_bytes": measurements.logical_source_bytes,
                "source_read_operations": measurements.source_read_operations,
                "modeled_physical_bytes": source.modeled_physical_bytes,
                "named_row_projection_bytes": source.named_row_projection_bytes,
                "explicit_adaptation_copy_bytes": source.explicit_adaptation_copy_bytes,
                "implicit_copy_bytes": source.implicit_copy_bytes,
                "warmup_fills": source.warmup_fills,
                "reused_fills": source.reused_fills,
                "source_slots": measurements.source_slots,
                "workers": measurements.workers,
                "planned_source_capacity_bytes": measurements.planned_source_capacity_bytes,
                "maximum_partitions_per_block": measurements.maximum_partitions_per_block,
                "planned_kernel_window_capacity_bytes": measurements.planned_kernel_window_capacity_bytes,
                "peak_kernel_window_capacity_bytes": measurements.peak_kernel_window_capacity_bytes,
                "peak_live_source_blocks": measurements.peak_live_source_blocks,
                "peak_live_source_current_bytes": measurements.peak_live_source_current_bytes,
                "peak_live_source_capacity_bytes": measurements.peak_live_source_capacity_bytes,
                "ready_queue_high_water": measurements.ready_queue_high_water,
                "ready_queue_current_bytes_high_water": measurements.ready_queue_current_bytes_high_water,
                "ready_queue_capacity_bytes_high_water": measurements.ready_queue_capacity_bytes_high_water,
                "source_fill_nanos": measurements.source_fill_nanos,
                "commit_nanos": measurements.commit_nanos,
                "producer_wait_nanos": measurements.producer_wait_nanos,
                "consumer_wait_nanos": measurements.consumer_wait_nanos,
                "overlap_nanos": measurements.overlap_nanos,
                "lease_return_nanos": measurements.lease_return_nanos,
                "wall_nanos": measurements.wall_nanos,
                "rss_samples": rss,
                "terminal": source.terminal,
            })
        );
    }
}
