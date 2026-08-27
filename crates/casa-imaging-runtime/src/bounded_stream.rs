// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundedStreamPlan {
    source_slots: usize,
    workers: usize,
}

impl BoundedStreamPlan {
    pub(crate) fn new(source_slots: usize, workers: usize) -> Result<Self, BoundedStreamPlanError> {
        if !(1..=2).contains(&source_slots) {
            return Err(BoundedStreamPlanError::SourceSlots);
        }
        if workers == 0 {
            return Err(BoundedStreamPlanError::Workers);
        }
        Ok(Self {
            source_slots,
            workers,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedStreamPlanError {
    SourceSlots,
    Workers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourcePoll {
    Ready {
        source_ordinal: u32,
        logical_bytes: u64,
        resident_current_bytes: u64,
        resident_capacity_bytes: u64,
    },
    Exhausted,
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
    Exclusive { region: u64 },
    OrderedPartial { region: u64, commit_key: u64 },
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
    type Completion;
    type Error: Error + Send + 'static;

    fn partitions(
        &self,
        block: BlockIdentity,
        storage: &S,
    ) -> Result<Vec<KernelPartition<Self::Partition>>, Self::Error>;
    fn execute(
        &self,
        work: WorkIdentity,
        storage: &S,
        partition: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error>;
    fn commit(
        &mut self,
        work: WorkIdentity,
        storage: &S,
        partial: Self::Partial,
    ) -> Result<(), Self::Error>;
    fn complete(self) -> Result<Self::Completion, Self::Error>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BoundedStreamMeasurements {
    pub(crate) blocks_filled: u64,
    pub(crate) logical_source_bytes: u64,
    pub(crate) source_fill_nanos: u128,
    pub(crate) prepare_nanos: u128,
    pub(crate) execute_nanos: u128,
    pub(crate) commit_nanos: u128,
    pub(crate) producer_wait_nanos: u128,
    pub(crate) consumer_wait_nanos: u128,
    pub(crate) ready_queue_high_water: usize,
    pub(crate) peak_live_source_blocks: usize,
    pub(crate) peak_live_source_current_bytes: u64,
    pub(crate) peak_live_source_capacity_bytes: u64,
    pub(crate) source_slots: usize,
    pub(crate) workers: usize,
}

#[derive(Debug)]
pub(crate) struct BoundedStreamOutcome<S, K> {
    pub(crate) source_completion: S,
    pub(crate) kernel_completion: K,
    pub(crate) measurements: BoundedStreamMeasurements,
}

#[derive(Debug)]
pub(crate) enum BoundedStreamError<S, K> {
    Source(S),
    Kernel(K),
    MeasurementOverflow,
    InvalidKernelPlan,
    ProducerPanicked,
    ProducerDisconnected,
}

struct ProcessMeasurements {
    prepare_nanos: u128,
    execute_nanos: u128,
    commit_nanos: u128,
}

fn process_block<S, K>(
    plan: BoundedStreamPlan,
    block: BlockIdentity,
    storage: &S,
    kernel: &mut K,
) -> Result<ProcessMeasurements, BoundedStreamError<InfallibleSource, K::Error>>
where
    S: Sync,
    K: PartitionedKernel<S>,
{
    let prepare_started = Instant::now();
    let partitions = kernel
        .partitions(block, storage)
        .map_err(BoundedStreamError::Kernel)?;
    let prepare_nanos = prepare_started.elapsed().as_nanos();
    let mut pending = (0..partitions.len()).collect::<Vec<_>>();
    let mut completed = Vec::with_capacity(partitions.len());
    let execute_started = Instant::now();
    while !pending.is_empty() {
        let mut exclusive_regions = Vec::new();
        let mut wave = Vec::new();
        let mut rest = Vec::new();
        for index in pending {
            let compatible = match partitions[index].accumulation {
                Accumulation::Exclusive { region } => !exclusive_regions.contains(&region),
                Accumulation::OrderedPartial { .. } => true,
            };
            if wave.len() < plan.workers && compatible {
                if let Accumulation::Exclusive { region } = partitions[index].accumulation {
                    exclusive_regions.push(region);
                }
                wave.push(index);
            } else {
                rest.push(index);
            }
        }
        pending = rest;
        let kernel_ref = &*kernel;
        if wave.len() == 1 {
            let index = wave[0];
            let identity = partitions[index].identity(block, index as u64);
            let partial = kernel_ref
                .execute(identity, storage, &partitions[index].payload)
                .map_err(BoundedStreamError::Kernel)?;
            completed.push((identity, partial));
        } else {
            let results = std::thread::scope(|scope| {
                wave.into_iter()
                    .map(|index| {
                        let identity = partitions[index].identity(block, index as u64);
                        let partition = &partitions[index];
                        (
                            identity,
                            scope.spawn(move || {
                                kernel_ref.execute(identity, storage, &partition.payload)
                            }),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|(identity, handle)| {
                        handle
                            .join()
                            .map(|partial| (identity, partial))
                            .map_err(|_| BoundedStreamError::InvalidKernelPlan)
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            for (identity, result) in results {
                completed.push((identity, result.map_err(BoundedStreamError::Kernel)?));
            }
        }
    }
    let execute_nanos = execute_started.elapsed().as_nanos();
    completed.sort_by_key(|(identity, _)| *identity);
    if completed.windows(2).any(|items| items[0].0 == items[1].0) {
        return Err(BoundedStreamError::InvalidKernelPlan);
    }
    let commit_started = Instant::now();
    for (identity, partial) in completed {
        kernel
            .commit(identity, storage, partial)
            .map_err(BoundedStreamError::Kernel)?;
    }
    Ok(ProcessMeasurements {
        prepare_nanos,
        execute_nanos,
        commit_nanos: commit_started.elapsed().as_nanos(),
    })
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
    Completed(C, ProducerMeasurements),
}

struct StorageLease<S> {
    storage: S,
    resident_current_bytes: u64,
    resident_capacity_bytes: u64,
}

#[derive(Clone, Copy, Default)]
struct ProducerMeasurements {
    blocks_filled: u64,
    logical_source_bytes: u64,
    source_fill_nanos: u128,
    producer_wait_nanos: u128,
    peak_live_source_blocks: usize,
    peak_live_source_current_bytes: u64,
    peak_live_source_capacity_bytes: u64,
}

pub(crate) fn execute_bounded<S, K>(
    plan: BoundedStreamPlan,
    pass_ordinal: u32,
    source: S,
    kernel: K,
) -> Result<
    BoundedStreamOutcome<S::Completion, K::Completion>,
    BoundedStreamError<S::Error, K::Error>,
>
where
    S: OrderedBlockSource,
    K: PartitionedKernel<S::Storage>,
{
    execute_overlapped(plan, pass_ordinal, source, kernel)
}

fn execute_overlapped<S, K>(
    plan: BoundedStreamPlan,
    pass_ordinal: u32,
    source: S,
    mut kernel: K,
) -> Result<
    BoundedStreamOutcome<S::Completion, K::Completion>,
    BoundedStreamError<S::Error, K::Error>,
>
where
    S: OrderedBlockSource,
    K: PartitionedKernel<S::Storage>,
{
    let cancelled = Arc::new(AtomicBool::new(false));
    let ready_count = Arc::new(AtomicUsize::new(0));
    let ready_high_water = Arc::new(AtomicUsize::new(0));
    let (ready_tx, ready_rx) = mpsc::sync_channel(plan.source_slots);
    let (returned_sender, returned_rx) =
        mpsc::sync_channel::<StorageLease<S::Storage>>(plan.source_slots);
    let mut returned_tx = Some(returned_sender);
    let mut measurements = BoundedStreamMeasurements {
        source_slots: plan.source_slots,
        workers: plan.workers,
        ..BoundedStreamMeasurements::default()
    };
    let source_completion = std::thread::scope(|scope| {
        let producer_cancelled = Arc::clone(&cancelled);
        let producer_ready_count = Arc::clone(&ready_count);
        let producer_high_water = Arc::clone(&ready_high_water);
        let producer = scope.spawn(move || {
            let mut source = source;
            let mut empty = (0..plan.source_slots)
                .map(|slot| StorageLease {
                    storage: source.create_storage(slot),
                    resident_current_bytes: 0,
                    resident_capacity_bytes: 0,
                })
                .collect::<Vec<_>>();
            let mut outstanding = 0usize;
            let mut live_current_bytes = 0_u64;
            let mut live_capacity_bytes = 0_u64;
            let mut block_ordinal = 0_u64;
            let mut producer_measurements = ProducerMeasurements::default();
            loop {
                if producer_cancelled.load(Ordering::Acquire) {
                    return;
                }
                let lease = if let Some(lease) = empty.pop() {
                    lease
                } else {
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
                let poll = source.fill(block_ordinal, &mut lease.storage);
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
                        resident_current_bytes,
                        resident_capacity_bytes,
                    }) => {
                        if producer_cancelled.load(Ordering::Acquire) {
                            return;
                        }
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
                        producer_measurements.blocks_filled = blocks_filled;
                        producer_measurements.logical_source_bytes = logical_source_bytes;
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
                        let queued = producer_ready_count.fetch_add(1, Ordering::AcqRel) + 1;
                        producer_high_water.fetch_max(queued, Ordering::AcqRel);
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
                            producer_ready_count.fetch_sub(1, Ordering::AcqRel);
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
                                    producer_measurements,
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
            let Some(consumer_wait_nanos) = measurements
                .consumer_wait_nanos
                .checked_add(wait_started.elapsed().as_nanos())
            else {
                cancelled.store(true, Ordering::Release);
                returned_tx.take();
                return Err(BoundedStreamError::MeasurementOverflow);
            };
            measurements.consumer_wait_nanos = consumer_wait_nanos;
            match message {
                Ok(ReadyMessage::Block { identity, lease }) => {
                    ready_count.fetch_sub(1, Ordering::AcqRel);
                    let process = process_block(plan, identity, &lease.storage, &mut kernel)
                        .map_err(map_process_error);
                    match process {
                        Ok(process) => {
                            let Some(prepare_nanos) = measurements
                                .prepare_nanos
                                .checked_add(process.prepare_nanos)
                            else {
                                cancelled.store(true, Ordering::Release);
                                returned_tx.take();
                                return Err(BoundedStreamError::MeasurementOverflow);
                            };
                            let Some(execute_nanos) = measurements
                                .execute_nanos
                                .checked_add(process.execute_nanos)
                            else {
                                cancelled.store(true, Ordering::Release);
                                returned_tx.take();
                                return Err(BoundedStreamError::MeasurementOverflow);
                            };
                            let Some(commit_nanos) =
                                measurements.commit_nanos.checked_add(process.commit_nanos)
                            else {
                                cancelled.store(true, Ordering::Release);
                                returned_tx.take();
                                return Err(BoundedStreamError::MeasurementOverflow);
                            };
                            measurements.prepare_nanos = prepare_nanos;
                            measurements.execute_nanos = execute_nanos;
                            measurements.commit_nanos = commit_nanos;
                            if returned_tx
                                .as_ref()
                                .expect("return sender remains live while consuming")
                                .send(lease)
                                .is_err()
                            {
                                cancelled.store(true, Ordering::Release);
                                return Err(BoundedStreamError::ProducerDisconnected);
                            }
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Release);
                            returned_tx.take();
                            return Err(error);
                        }
                    }
                }
                Ok(ReadyMessage::Exhausted) => {}
                Ok(ReadyMessage::SourceError(error)) => {
                    cancelled.store(true, Ordering::Release);
                    returned_tx.take();
                    return Err(BoundedStreamError::Source(error));
                }
                Ok(ReadyMessage::MeasurementOverflow) => {
                    cancelled.store(true, Ordering::Release);
                    returned_tx.take();
                    return Err(BoundedStreamError::MeasurementOverflow);
                }
                Ok(ReadyMessage::Completed(value, producer_measurements)) => {
                    measurements.blocks_filled = producer_measurements.blocks_filled;
                    measurements.logical_source_bytes = producer_measurements.logical_source_bytes;
                    measurements.source_fill_nanos = producer_measurements.source_fill_nanos;
                    measurements.producer_wait_nanos = producer_measurements.producer_wait_nanos;
                    measurements.peak_live_source_blocks =
                        producer_measurements.peak_live_source_blocks;
                    measurements.peak_live_source_current_bytes =
                        producer_measurements.peak_live_source_current_bytes;
                    measurements.peak_live_source_capacity_bytes =
                        producer_measurements.peak_live_source_capacity_bytes;
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
    })?;
    measurements.ready_queue_high_water = ready_high_water.load(Ordering::Acquire);
    let kernel_completion = kernel.complete().map_err(BoundedStreamError::Kernel)?;
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
        BoundedStreamError::ProducerPanicked => BoundedStreamError::ProducerPanicked,
        BoundedStreamError::ProducerDisconnected => BoundedStreamError::ProducerDisconnected,
        BoundedStreamError::Source(error) => match error {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeSet,
        convert::Infallible,
        fmt,
        mem::size_of,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
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

        fn partitions(
            &self,
            _block: BlockIdentity,
            values: &Vec<u64>,
        ) -> Result<Vec<KernelPartition<Self::Partition>>, Self::Error> {
            Ok(values
                .iter()
                .enumerate()
                .map(|(index, _)| KernelPartition::ordered(index as u64, 0, index as u64, index))
                .collect())
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

    fn run(slots: usize, workers: usize) -> BoundedStreamOutcome<usize, Vec<(WorkIdentity, u64)>> {
        let source = NumberSource {
            blocks: vec![vec![1, 2, 3], vec![4, 5], vec![6, 7, 8, 9]],
            next: 0,
            pointers: Arc::new(Mutex::new(Vec::new())),
            completions: Arc::new(AtomicUsize::new(0)),
        };
        execute_bounded(
            BoundedStreamPlan::new(slots, workers).unwrap(),
            3,
            source,
            SumKernel::default(),
        )
        .unwrap()
    }

    #[test]
    fn one_and_two_slots_have_identical_ordered_results() {
        let inline = run(1, 1);
        let overlapped = run(2, 1);
        assert_eq!(inline.source_completion, overlapped.source_completion);
        assert_eq!(inline.kernel_completion, overlapped.kernel_completion);
        assert_eq!(inline.measurements.blocks_filled, 3);
        assert_eq!(overlapped.measurements.blocks_filled, 3);
        assert!(overlapped.measurements.ready_queue_high_water <= 2);
    }

    #[test]
    fn worker_count_does_not_change_stable_commit_order() {
        assert_eq!(run(2, 1).kernel_completion, run(2, 3).kernel_completion);
    }

    #[test]
    fn plan_rejects_unbounded_or_empty_execution() {
        assert_eq!(
            BoundedStreamPlan::new(0, 1),
            Err(BoundedStreamPlanError::SourceSlots)
        );
        assert_eq!(
            BoundedStreamPlan::new(3, 1),
            Err(BoundedStreamPlanError::SourceSlots)
        );
        assert_eq!(
            BoundedStreamPlan::new(1, 0),
            Err(BoundedStreamPlanError::Workers)
        );
    }

    #[test]
    fn overlapped_source_reuses_only_the_planned_storage_slots() {
        let pointers = Arc::new(Mutex::new(Vec::new()));
        let completions = Arc::new(AtomicUsize::new(0));
        let outcome = execute_bounded(
            BoundedStreamPlan::new(2, 1).unwrap(),
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

        fn partitions(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
        ) -> Result<Vec<KernelPartition<Self::Partition>>, Self::Error> {
            Ok(vec![KernelPartition::exclusive(0, 0, ())])
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
            BoundedStreamPlan::new(2, 1).unwrap(),
            0,
            NumberSource {
                blocks: (0..100).map(|value| vec![value]).collect(),
                next: 0,
                pointers: Arc::new(Mutex::new(Vec::new())),
                completions: Arc::clone(&completions),
            },
            FailingKernel,
        );
        assert!(matches!(
            result,
            Err(BoundedStreamError::Kernel(TestFailure))
        ));
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

        fn partitions(
            &self,
            _block: BlockIdentity,
            _storage: &Vec<u64>,
        ) -> Result<Vec<KernelPartition<Self::Partition>>, Self::Error> {
            Ok((0..8)
                .map(|partition| KernelPartition::exclusive(partition, 7, ()))
                .collect())
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
            BoundedStreamPlan::new(1, 8).unwrap(),
            0,
            source,
            ExclusiveProbe::default(),
        )
        .unwrap();
        assert_eq!(outcome.kernel_completion, 1);
    }
}
