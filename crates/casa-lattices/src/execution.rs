// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal chunk execution helpers for read-only lattice traversal.
//!
//! These helpers keep traversal/execution structure out of higher-level
//! algorithms such as statistics while remaining crate-internal for now.

use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;

use crossbeam_channel::TrySendError;

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::Lattice;
use crate::traversal::{
    TraversalChunk, TraversalCursor, TraversalCursorIter, TraversalIter, TraversalSpec,
};

/// Caller-selected execution policy for lattice and image work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExecutionPolicy {
    /// Derive a plan from explicit workload and resource inputs.
    #[default]
    Auto,
    /// Force a plain serial traversal.
    Serial,
    /// Use one worker and a bounded producer/consumer queue.
    Pipelined { prefetch_depth: usize },
    /// Use exactly the requested worker count and queue depth.
    Parallel {
        workers: usize,
        prefetch_depth: usize,
    },
}

/// Whether the source can benefit from producer/consumer overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceResidency {
    Resident,
    Persistent,
}

/// Resource limits assigned to an execution owner.
///
/// Application runtimes can reserve a slice from their process-level memory
/// ledger and pass it here. The planner never samples host state or competes
/// with other consumers on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionResources {
    pub memory_budget_bytes: usize,
    pub worker_limit: usize,
    pub prefetch_cap: usize,
}

impl ExecutionResources {
    /// Resources that preserve the historical unconstrained library default.
    /// Applications should prefer an explicitly reserved budget.
    pub fn library_default() -> Self {
        Self {
            memory_budget_bytes: usize::MAX,
            worker_limit: std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1),
            prefetch_cap: usize::MAX,
        }
    }
}

impl Default for ExecutionResources {
    fn default() -> Self {
        Self::library_default()
    }
}

/// Exact workload and resource facts consumed by [`plan_execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionInputs {
    pub task_count: usize,
    pub chunk_bytes: usize,
    pub per_worker_state_bytes: usize,
    pub memory_budget_bytes: usize,
    pub available_workers: usize,
    pub requested_worker_limit: usize,
    pub source_residency: SourceResidency,
    pub prefetch_capability: bool,
    pub configured_prefetch_cap: usize,
}

/// Resolved execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Serial,
    Pipelined,
    Parallel,
}

/// Deterministic byte-aware execution plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub mode: ExecutionMode,
    pub workers: usize,
    pub prefetch_depth: usize,
    pub worker_state_bytes: usize,
    pub prefetch_bytes: usize,
    pub planned_resident_bytes: usize,
}

/// Invalid workload, policy, or resource inputs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecutionPlanError {
    #[error("execution planning overflow while computing {0}")]
    Overflow(&'static str),
    #[error("invalid execution policy: {0}")]
    InvalidPolicy(String),
    #[error("execution needs {required_bytes} bytes but only {budget_bytes} bytes were assigned")]
    InsufficientMemory {
        required_bytes: usize,
        budget_bytes: usize,
    },
}

/// Resolves one execution plan from checked workload/resource formulas.
pub fn plan_execution(
    policy: ExecutionPolicy,
    inputs: ExecutionInputs,
) -> Result<ExecutionPlan, ExecutionPlanError> {
    if inputs.task_count == 0 {
        return Ok(ExecutionPlan {
            mode: ExecutionMode::Serial,
            workers: 0,
            prefetch_depth: 0,
            worker_state_bytes: 0,
            prefetch_bytes: 0,
            planned_resident_bytes: 0,
        });
    }
    if inputs.available_workers == 0 || inputs.requested_worker_limit == 0 {
        return Err(ExecutionPlanError::InvalidPolicy(
            "worker availability and caller worker limit must be positive".to_string(),
        ));
    }

    let state_charge = inputs.per_worker_state_bytes.max(1);
    let chunk_charge = inputs.chunk_bytes.max(1);
    let worker_cap = inputs
        .available_workers
        .min(inputs.requested_worker_limit)
        .min(inputs.task_count);

    let finish = |mode, workers: usize, prefetch_depth: usize| {
        let worker_state_bytes = workers
            .checked_mul(state_charge)
            .ok_or(ExecutionPlanError::Overflow("worker state bytes"))?;
        let prefetch_bytes = prefetch_depth
            .checked_mul(chunk_charge)
            .ok_or(ExecutionPlanError::Overflow("prefetch bytes"))?;
        let planned_resident_bytes = worker_state_bytes
            .checked_add(prefetch_bytes)
            .ok_or(ExecutionPlanError::Overflow("planned resident bytes"))?;
        if planned_resident_bytes > inputs.memory_budget_bytes {
            return Err(ExecutionPlanError::InsufficientMemory {
                required_bytes: planned_resident_bytes,
                budget_bytes: inputs.memory_budget_bytes,
            });
        }
        Ok(ExecutionPlan {
            mode,
            workers,
            prefetch_depth,
            worker_state_bytes,
            prefetch_bytes,
            planned_resident_bytes,
        })
    };

    match policy {
        ExecutionPolicy::Serial => finish(ExecutionMode::Serial, 1, 0),
        ExecutionPolicy::Pipelined { prefetch_depth } => {
            if !inputs.prefetch_capability || inputs.source_residency != SourceResidency::Persistent
            {
                return Err(ExecutionPlanError::InvalidPolicy(
                    "pipelining requires a persistent source with prefetch support".to_string(),
                ));
            }
            if prefetch_depth == 0 {
                return Err(ExecutionPlanError::InvalidPolicy(
                    "pipelined prefetch depth must be positive".to_string(),
                ));
            }
            if prefetch_depth > inputs.configured_prefetch_cap {
                return Err(ExecutionPlanError::InvalidPolicy(format!(
                    "prefetch depth {prefetch_depth} exceeds the configured cap"
                )));
            }
            if prefetch_depth > inputs.task_count {
                return Err(ExecutionPlanError::InvalidPolicy(format!(
                    "pipelined prefetch depth {prefetch_depth} exceeds {} available tasks",
                    inputs.task_count
                )));
            }
            finish(ExecutionMode::Pipelined, 1, prefetch_depth)
        }
        ExecutionPolicy::Parallel {
            workers,
            prefetch_depth,
        } => {
            if workers < 2 {
                return Err(ExecutionPlanError::InvalidPolicy(
                    "parallel execution requires at least two workers".to_string(),
                ));
            }
            if workers > worker_cap {
                return Err(ExecutionPlanError::InvalidPolicy(format!(
                    "parallel worker count {workers} exceeds the assigned work/resource cap {worker_cap}",
                )));
            }
            if prefetch_depth == 0 {
                return Err(ExecutionPlanError::InvalidPolicy(
                    "parallel prefetch depth must be positive".to_string(),
                ));
            }
            if prefetch_depth > inputs.configured_prefetch_cap {
                return Err(ExecutionPlanError::InvalidPolicy(format!(
                    "prefetch depth {prefetch_depth} exceeds the configured cap"
                )));
            }
            if prefetch_depth > inputs.task_count {
                return Err(ExecutionPlanError::InvalidPolicy(format!(
                    "parallel prefetch depth {prefetch_depth} exceeds {} available tasks",
                    inputs.task_count
                )));
            }
            finish(ExecutionMode::Parallel, workers, prefetch_depth)
        }
        ExecutionPolicy::Auto => {
            let memory_workers = inputs.memory_budget_bytes / state_charge;
            let workers = worker_cap.min(memory_workers);
            if workers == 0 {
                return Err(ExecutionPlanError::InsufficientMemory {
                    required_bytes: state_charge,
                    budget_bytes: inputs.memory_budget_bytes,
                });
            }
            let worker_state_bytes = workers
                .checked_mul(state_charge)
                .ok_or(ExecutionPlanError::Overflow("worker state bytes"))?;
            let remaining_budget = inputs.memory_budget_bytes - worker_state_bytes;
            let remaining_tasks = inputs.task_count.saturating_sub(workers);
            let prefetch_depth = if inputs.prefetch_capability {
                inputs
                    .configured_prefetch_cap
                    .min(remaining_tasks)
                    .min(remaining_budget / chunk_charge)
            } else {
                0
            };
            if workers >= 2 {
                finish(ExecutionMode::Parallel, workers, prefetch_depth)
            } else if inputs.source_residency == SourceResidency::Persistent && prefetch_depth > 0 {
                finish(ExecutionMode::Pipelined, 1, prefetch_depth)
            } else {
                finish(ExecutionMode::Serial, 1, 0)
            }
        }
    }
}

/// Internal strategy selector for owned read-chunk execution.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadChunkExecutionStrategy {
    Serial,
    Pipelined(PipelinedReadChunkConfig),
    Parallel(ParallelReadChunkConfig),
}

/// Configuration for the internal overlap-only read-chunk pipeline.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipelinedReadChunkConfig {
    pub prefetch_depth: usize,
}

/// Configuration for the internal parallel read-chunk pipeline.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelReadChunkConfig {
    pub workers: usize,
    pub prefetch_depth: usize,
}

/// Internal producer/consumer strategy for cursor-driven map/write workloads.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorMapWriteExecutionStrategy {
    Serial,
    Pipelined(CursorMapWriteConfig),
}

/// Configuration for the internal cursor map/write pipeline.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorMapWriteConfig {
    pub prefetch_depth: usize,
}

/// Internal producer/worker/writer strategy for ordered cursor map/write
/// workloads with optional parallel mapping.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderedCursorMapWriteExecutionStrategy {
    Serial,
    Pipelined(CursorMapWriteConfig),
    Parallel(ParallelReadChunkConfig),
}

/// Owned read-only chunk task passed to internal execution helpers.
#[doc(hidden)]
#[derive(Debug)]
pub struct ChunkTask<T> {
    pub cursor: TraversalCursor,
    pub data: ndarray::ArrayD<T>,
}

impl<T> From<TraversalChunk<T>> for ChunkTask<T> {
    fn from(chunk: TraversalChunk<T>) -> Self {
        Self {
            cursor: chunk.cursor,
            data: chunk.data,
        }
    }
}

#[cfg(test)]
/// Fold over owned read-only chunks in traversal order.
pub(crate) fn try_fold_read_chunks<T: LatticeElement, Acc>(
    lattice: &dyn Lattice<T>,
    spec: TraversalSpec,
    mut acc: Acc,
    mut f: impl FnMut(&mut Acc, ChunkTask<T>) -> Result<(), LatticeError>,
) -> Result<Acc, LatticeError> {
    for chunk in TraversalIter::new(lattice, spec) {
        f(&mut acc, chunk?.into())?;
    }
    Ok(acc)
}

/// Fold over traversal cursors in traversal order.
#[doc(hidden)]
pub fn try_fold_traversal_cursors<Acc>(
    full_shape: &[usize],
    cursor_shape: &[usize],
    spec: TraversalSpec,
    mut acc: Acc,
    mut f: impl FnMut(&mut Acc, TraversalCursor) -> Result<(), LatticeError>,
) -> Result<Acc, LatticeError> {
    for cursor in TraversalCursorIter::new(full_shape.to_vec(), cursor_shape.to_vec(), spec) {
        f(&mut acc, cursor?)?;
    }
    Ok(acc)
}

/// Visit traversal cursors in traversal order.
#[doc(hidden)]
pub fn try_for_each_traversal_cursor(
    full_shape: &[usize],
    cursor_shape: &[usize],
    spec: TraversalSpec,
    mut f: impl FnMut(TraversalCursor) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    try_fold_traversal_cursors(full_shape, cursor_shape, spec, (), |(), cursor| f(cursor))
}

/// Produce values from traversal cursors and consume them serially or via a
/// bounded producer/consumer pipeline.
#[doc(hidden)]
pub fn try_map_traversal_cursors_with_strategy<Out, Produce, Consume>(
    full_shape: &[usize],
    cursor_shape: &[usize],
    spec: TraversalSpec,
    strategy: CursorMapWriteExecutionStrategy,
    mut produce: Produce,
    mut consume: Consume,
) -> Result<(), LatticeError>
where
    Out: Send,
    Produce: FnMut(TraversalCursor) -> Result<Out, LatticeError>,
    Consume: FnMut(Out) -> Result<(), LatticeError> + Send,
{
    match strategy {
        CursorMapWriteExecutionStrategy::Serial => {
            try_for_each_traversal_cursor(full_shape, cursor_shape, spec, |cursor| {
                let item = produce(cursor)?;
                consume(item)
            })
        }
        CursorMapWriteExecutionStrategy::Pipelined(config) => {
            let prefetch_depth = config.prefetch_depth.max(1);
            let (task_tx, task_rx) = crossbeam_channel::bounded::<Out>(prefetch_depth);
            let cancelled = Arc::new(AtomicBool::new(false));
            let first_error = Arc::new(Mutex::new(None));

            thread::scope(|scope| {
                let cancelled_consumer = Arc::clone(&cancelled);
                let first_error_consumer = Arc::clone(&first_error);
                let consumer = scope.spawn(move || {
                    while let Ok(item) = task_rx.recv() {
                        if cancelled_consumer.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(err) = consume(item) {
                            store_first_error(first_error_consumer.as_ref(), err);
                            cancelled_consumer.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                });

                let mut send_failed = false;
                for cursor in TraversalCursorIter::new(
                    full_shape.to_vec(),
                    cursor_shape.to_vec(),
                    spec.clone(),
                ) {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut item = match cursor.and_then(&mut produce) {
                        Ok(item) => item,
                        Err(err) => {
                            store_first_error(first_error.as_ref(), err);
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                    loop {
                        if cancelled.load(Ordering::Relaxed) {
                            break;
                        }
                        match task_tx.try_send(item) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                item = returned;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                send_failed = true;
                                break;
                            }
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
                drop(task_tx);
                consumer.join().expect("cursor map/write worker panicked");

                if let Some(err) = first_error.lock().expect("lock not poisoned").take() {
                    Err(err)
                } else {
                    Ok(())
                }
            })
        }
    }
}

/// Produce values from traversal cursors and consume them in traversal order
/// while allowing the mapping stage to run serially, pipelined, or in parallel.
#[doc(hidden)]
pub fn try_map_traversal_cursors_ordered_with_strategy<Out, State, Init, Map, Consume>(
    full_shape: &[usize],
    cursor_shape: &[usize],
    spec: TraversalSpec,
    strategy: OrderedCursorMapWriteExecutionStrategy,
    init_state: Init,
    map_cursor: Map,
    mut consume: Consume,
) -> Result<(), LatticeError>
where
    Out: Send,
    Init: Fn() -> State + Sync + Send,
    Map: Fn(&mut State, TraversalCursor) -> Result<Out, LatticeError> + Sync + Send,
    Consume: FnMut(Out) -> Result<(), LatticeError> + Send,
{
    match strategy {
        OrderedCursorMapWriteExecutionStrategy::Serial => {
            let mut state = init_state();
            for cursor in TraversalCursorIter::new(full_shape.to_vec(), cursor_shape.to_vec(), spec)
            {
                let item = map_cursor(&mut state, cursor?)?;
                consume(item)?;
            }
            Ok(())
        }
        OrderedCursorMapWriteExecutionStrategy::Pipelined(config) => {
            let prefetch_depth = config.prefetch_depth.max(1);
            let (task_tx, task_rx) = crossbeam_channel::bounded::<(usize, Out)>(prefetch_depth);
            let cancelled = Arc::new(AtomicBool::new(false));
            let first_error = Arc::new(Mutex::new(None));

            thread::scope(|scope| {
                let cancelled_consumer = Arc::clone(&cancelled);
                let first_error_consumer = Arc::clone(&first_error);
                let consumer = scope.spawn(move || {
                    let mut next_index = 0usize;
                    let mut pending = BTreeMap::new();
                    while let Ok((index, item)) = task_rx.recv() {
                        if cancelled_consumer.load(Ordering::Relaxed) {
                            break;
                        }
                        pending.insert(index, item);
                        while let Some(item) = pending.remove(&next_index) {
                            if let Err(err) = consume(item) {
                                store_first_error(first_error_consumer.as_ref(), err);
                                cancelled_consumer.store(true, Ordering::Relaxed);
                                return;
                            }
                            next_index += 1;
                        }
                    }
                });

                let mut state = init_state();
                let mut send_failed = false;
                for (index, cursor) in TraversalCursorIter::new(
                    full_shape.to_vec(),
                    cursor_shape.to_vec(),
                    spec.clone(),
                )
                .enumerate()
                {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut item = match cursor.and_then(|cursor| map_cursor(&mut state, cursor)) {
                        Ok(item) => item,
                        Err(err) => {
                            store_first_error(first_error.as_ref(), err);
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                    loop {
                        if cancelled.load(Ordering::Relaxed) {
                            break;
                        }
                        match task_tx.try_send((index, item)) {
                            Ok(()) => break,
                            Err(TrySendError::Full((returned_index, returned_item))) => {
                                debug_assert_eq!(returned_index, index);
                                item = returned_item;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                send_failed = true;
                                break;
                            }
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
                drop(task_tx);
                consumer
                    .join()
                    .expect("ordered cursor map/write worker panicked");

                if let Some(err) = first_error.lock().expect("lock not poisoned").take() {
                    Err(err)
                } else {
                    Ok(())
                }
            })
        }
        OrderedCursorMapWriteExecutionStrategy::Parallel(config) => {
            let workers = config.workers.max(1);
            let prefetch_depth = config.prefetch_depth.max(workers);
            let (cursor_tx, cursor_rx) =
                crossbeam_channel::bounded::<(usize, TraversalCursor)>(prefetch_depth);
            let (result_tx, result_rx) = crossbeam_channel::bounded::<(usize, Out)>(prefetch_depth);
            let cancelled = Arc::new(AtomicBool::new(false));
            let first_error = Arc::new(Mutex::new(None));

            thread::scope(|scope| {
                let cancelled_writer = Arc::clone(&cancelled);
                let first_error_writer = Arc::clone(&first_error);
                let writer = scope.spawn(move || {
                    let mut next_index = 0usize;
                    let mut pending = BTreeMap::new();
                    while let Ok((index, item)) = result_rx.recv() {
                        if cancelled_writer.load(Ordering::Relaxed) {
                            break;
                        }
                        pending.insert(index, item);
                        while let Some(item) = pending.remove(&next_index) {
                            if let Err(err) = consume(item) {
                                store_first_error(first_error_writer.as_ref(), err);
                                cancelled_writer.store(true, Ordering::Relaxed);
                                return;
                            }
                            next_index += 1;
                        }
                    }
                });

                let mut handles = Vec::with_capacity(workers);
                for _ in 0..workers {
                    let rx = cursor_rx.clone();
                    let tx = result_tx.clone();
                    let cancelled = Arc::clone(&cancelled);
                    let first_error = Arc::clone(&first_error);
                    let init_state = &init_state;
                    let map_cursor = &map_cursor;
                    handles.push(scope.spawn(move || {
                        let mut state = init_state();
                        while let Ok((index, cursor)) = rx.recv() {
                            if cancelled.load(Ordering::Relaxed) {
                                break;
                            }
                            let item = match map_cursor(&mut state, cursor) {
                                Ok(item) => item,
                                Err(err) => {
                                    store_first_error(first_error.as_ref(), err);
                                    cancelled.store(true, Ordering::Relaxed);
                                    break;
                                }
                            };
                            let mut item = item;
                            loop {
                                if cancelled.load(Ordering::Relaxed) {
                                    break;
                                }
                                match tx.try_send((index, item)) {
                                    Ok(()) => break,
                                    Err(TrySendError::Full((returned_index, returned_item))) => {
                                        debug_assert_eq!(returned_index, index);
                                        item = returned_item;
                                        thread::yield_now();
                                    }
                                    Err(TrySendError::Disconnected(_)) => return,
                                }
                            }
                        }
                    }));
                }
                drop(cursor_rx);
                drop(result_tx);

                let mut send_failed = false;
                for (index, cursor) in TraversalCursorIter::new(
                    full_shape.to_vec(),
                    cursor_shape.to_vec(),
                    spec.clone(),
                )
                .enumerate()
                {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut task = match cursor {
                        Ok(cursor) => (index, cursor),
                        Err(err) => {
                            store_first_error(first_error.as_ref(), err);
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                    loop {
                        if cancelled.load(Ordering::Relaxed) {
                            break;
                        }
                        match cursor_tx.try_send(task) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                task = returned;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                send_failed = true;
                                break;
                            }
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
                drop(cursor_tx);

                for handle in handles {
                    handle
                        .join()
                        .expect("ordered cursor map/write worker panicked");
                }
                writer
                    .join()
                    .expect("ordered cursor map/write writer panicked");

                if let Some(err) = first_error.lock().expect("lock not poisoned").take() {
                    Err(err)
                } else {
                    Ok(())
                }
            })
        }
    }
}

#[cfg(test)]
/// Visit owned read-only chunks in traversal order.
pub(crate) fn try_for_each_read_chunk<T: LatticeElement>(
    lattice: &dyn Lattice<T>,
    spec: TraversalSpec,
    mut f: impl FnMut(ChunkTask<T>) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    try_fold_read_chunks(lattice, spec, (), |(), chunk| f(chunk))
}

/// Visit owned read-only chunks using either serial or producer/worker execution.
#[doc(hidden)]
pub fn try_for_each_read_chunk_with_strategy<T, Process>(
    lattice: &dyn Lattice<T>,
    spec: TraversalSpec,
    strategy: ReadChunkExecutionStrategy,
    process_chunk: Process,
) -> Result<(), LatticeError>
where
    T: LatticeElement,
    Process: Fn(ChunkTask<T>) -> Result<(), LatticeError> + Sync,
{
    try_reduce_read_chunks(
        lattice,
        spec,
        strategy,
        || (),
        |(), chunk| process_chunk(chunk),
        |(), ()| Ok(()),
    )
}

/// Reduce over owned read-only chunks using either serial or producer/worker execution.
#[doc(hidden)]
pub fn try_reduce_read_chunks<T, Part, Init, Process, Merge>(
    lattice: &dyn Lattice<T>,
    spec: TraversalSpec,
    strategy: ReadChunkExecutionStrategy,
    make_partial: Init,
    process_chunk: Process,
    merge_partials: Merge,
) -> Result<Part, LatticeError>
where
    T: LatticeElement,
    Part: Send,
    Init: Fn() -> Part + Sync + Send,
    Process: Fn(&mut Part, ChunkTask<T>) -> Result<(), LatticeError> + Sync + Send,
    Merge: Fn(&mut Part, Part) -> Result<(), LatticeError> + Sync,
{
    match strategy {
        ReadChunkExecutionStrategy::Serial => {
            let mut partial = make_partial();
            for chunk in TraversalIter::new(lattice, spec) {
                process_chunk(&mut partial, chunk?.into())?;
            }
            Ok(partial)
        }
        ReadChunkExecutionStrategy::Pipelined(config) => {
            let prefetch_depth = config.prefetch_depth.max(1);
            let (task_tx, task_rx) = crossbeam_channel::bounded::<ChunkTask<T>>(prefetch_depth);
            let cancelled = Arc::new(AtomicBool::new(false));
            let first_error = Arc::new(Mutex::new(None));

            thread::scope(|scope| {
                let cancelled_worker = Arc::clone(&cancelled);
                let first_error_worker = Arc::clone(&first_error);
                let worker = scope.spawn(move || {
                    let mut partial = make_partial();
                    while let Ok(chunk) = task_rx.recv() {
                        if cancelled_worker.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(err) = process_chunk(&mut partial, chunk) {
                            store_first_error(first_error_worker.as_ref(), err);
                            cancelled_worker.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                    partial
                });

                let mut send_failed = false;
                for chunk in TraversalIter::new(lattice, spec) {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut task: ChunkTask<T> = match chunk {
                        Ok(chunk) => chunk.into(),
                        Err(err) => {
                            store_first_error(first_error.as_ref(), err);
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                    loop {
                        if cancelled.load(Ordering::Relaxed) {
                            break;
                        }
                        match task_tx.try_send(task) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                task = returned;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                send_failed = true;
                                break;
                            }
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
                drop(task_tx);

                let partial = worker.join().expect("read-chunk worker panicked");
                if let Some(err) = first_error.lock().expect("lock not poisoned").take() {
                    Err(err)
                } else {
                    Ok(partial)
                }
            })
        }
        ReadChunkExecutionStrategy::Parallel(config) => {
            let workers = config.workers.max(1);
            let prefetch_depth = config.prefetch_depth.max(workers);
            let (task_tx, task_rx) = crossbeam_channel::bounded::<ChunkTask<T>>(prefetch_depth);
            let cancelled = Arc::new(AtomicBool::new(false));
            let first_error = Arc::new(Mutex::new(None));

            thread::scope(|scope| {
                let mut handles = Vec::with_capacity(workers);
                for _ in 0..workers {
                    let rx = task_rx.clone();
                    let cancelled = Arc::clone(&cancelled);
                    let first_error = Arc::clone(&first_error);
                    let make_partial = &make_partial;
                    let process_chunk = &process_chunk;
                    handles.push(scope.spawn(move || {
                        let mut partial = make_partial();
                        while let Ok(chunk) = rx.recv() {
                            if cancelled.load(Ordering::Relaxed) {
                                break;
                            }
                            if let Err(err) = process_chunk(&mut partial, chunk) {
                                store_first_error(first_error.as_ref(), err);
                                cancelled.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                        partial
                    }));
                }
                drop(task_rx);

                let mut send_failed = false;
                for chunk in TraversalIter::new(lattice, spec) {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut task: ChunkTask<T> = match chunk {
                        Ok(chunk) => chunk.into(),
                        Err(err) => {
                            store_first_error(first_error.as_ref(), err);
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                    loop {
                        if cancelled.load(Ordering::Relaxed) {
                            break;
                        }
                        match task_tx.try_send(task) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                task = returned;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                send_failed = true;
                                break;
                            }
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
                drop(task_tx);

                let mut reduced = make_partial();
                for handle in handles {
                    let partial = handle.join().expect("read-chunk worker panicked");
                    merge_partials(&mut reduced, partial)?;
                }

                if let Some(err) = first_error.lock().expect("lock not poisoned").take() {
                    Err(err)
                } else {
                    Ok(reduced)
                }
            })
        }
    }
}

fn store_first_error(slot: &Mutex<Option<LatticeError>>, err: LatticeError) {
    let mut guard = slot.lock().expect("lock not poisoned");
    if guard.is_none() {
        *guard = Some(err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArrayLattice;
    use ndarray::{ArrayD, IxDyn};

    #[test]
    fn fold_read_chunks_covers_all_values() {
        let data = ArrayD::from_shape_fn(IxDyn(&[6, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        let lat = ArrayLattice::new(data.clone());
        let total = try_fold_read_chunks(
            &lat,
            crate::TraversalSpec::chunks(vec![3, 2]),
            0.0f64,
            |acc, chunk| {
                *acc += chunk.data.sum();
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(total, data.sum());
    }

    #[test]
    fn for_each_read_chunk_preserves_cursor_metadata() {
        let lat = ArrayLattice::<f64>::zeros(vec![4, 3]);
        let mut cursors = Vec::new();
        try_for_each_read_chunk(&lat, crate::TraversalSpec::lines(0), |chunk| {
            cursors.push(chunk.cursor);
            Ok(())
        })
        .unwrap();
        assert_eq!(cursors.len(), 3);
        assert_eq!(cursors[0].position, vec![0, 0]);
        assert_eq!(cursors[0].shape, vec![4, 1]);
    }

    #[test]
    fn reduce_read_chunks_parallel_matches_serial() {
        let data = ArrayD::from_shape_fn(IxDyn(&[16, 8]), |idx| (idx[0] * 8 + idx[1]) as f64);
        let lat = ArrayLattice::new(data.clone());
        let serial = try_reduce_read_chunks(
            &lat,
            crate::TraversalSpec::chunks(vec![4, 2]),
            ReadChunkExecutionStrategy::Serial,
            || 0.0f64,
            |sum, chunk| {
                *sum += chunk.data.sum();
                Ok(())
            },
            |sum, partial| {
                *sum += partial;
                Ok(())
            },
        )
        .unwrap();
        let parallel = try_reduce_read_chunks(
            &lat,
            crate::TraversalSpec::chunks(vec![4, 2]),
            ReadChunkExecutionStrategy::Parallel(ParallelReadChunkConfig {
                workers: 3,
                prefetch_depth: 4,
            }),
            || 0.0f64,
            |sum, chunk| {
                *sum += chunk.data.sum();
                Ok(())
            },
            |sum, partial| {
                *sum += partial;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(serial, data.sum());
        assert_eq!(parallel, data.sum());
    }

    #[test]
    fn reduce_read_chunks_pipelined_matches_serial() {
        let data = ArrayD::from_shape_fn(IxDyn(&[16, 8]), |idx| (idx[0] * 8 + idx[1]) as f64);
        let lat = ArrayLattice::new(data.clone());
        let pipelined = try_reduce_read_chunks(
            &lat,
            crate::TraversalSpec::chunks(vec![4, 2]),
            ReadChunkExecutionStrategy::Pipelined(PipelinedReadChunkConfig { prefetch_depth: 3 }),
            || 0.0f64,
            |sum, chunk| {
                *sum += chunk.data.sum();
                Ok(())
            },
            |sum, partial| {
                *sum += partial;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(pipelined, data.sum());
    }

    #[test]
    fn reduce_read_chunks_parallel_propagates_errors() {
        let lat = ArrayLattice::<f64>::zeros(vec![8, 8]);
        let err = try_reduce_read_chunks(
            &lat,
            crate::TraversalSpec::chunks(vec![2, 2]),
            ReadChunkExecutionStrategy::Parallel(ParallelReadChunkConfig {
                workers: 2,
                prefetch_depth: 2,
            }),
            || 0usize,
            |_count, _chunk| {
                Err(LatticeError::InvalidTraversal(
                    "synthetic worker failure".into(),
                ))
            },
            |_count, _partial| Ok(()),
        )
        .unwrap_err();
        assert!(matches!(err, LatticeError::InvalidTraversal(_)));
    }

    #[test]
    fn for_each_read_chunk_with_strategy_parallel_visits_all_chunks() {
        let lat = ArrayLattice::new(ArrayD::from_shape_fn(IxDyn(&[8, 6]), |idx| {
            (idx[0] * 6 + idx[1]) as f64
        }));
        let seen = std::sync::Mutex::new(Vec::new());
        try_for_each_read_chunk_with_strategy(
            &lat,
            crate::TraversalSpec::chunks(vec![2, 3]),
            ReadChunkExecutionStrategy::Parallel(ParallelReadChunkConfig {
                workers: 3,
                prefetch_depth: 4,
            }),
            |chunk| {
                seen.lock()
                    .expect("lock not poisoned")
                    .push(chunk.cursor.position);
                Ok(())
            },
        )
        .unwrap();
        let seen = seen.into_inner().expect("lock not poisoned");
        assert_eq!(seen.len(), 8);
        assert!(seen.contains(&vec![0, 0]));
        assert!(seen.contains(&vec![6, 3]));
    }

    #[test]
    fn map_traversal_cursors_with_strategy_pipelined_preserves_ordered_coverage() {
        let seen = std::sync::Mutex::new(Vec::new());
        try_map_traversal_cursors_with_strategy(
            &[5, 4],
            &[2, 3],
            crate::TraversalSpec::chunks(vec![2, 3]),
            CursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig { prefetch_depth: 2 }),
            Ok,
            |cursor| {
                seen.lock()
                    .expect("lock not poisoned")
                    .push((cursor.position, cursor.shape));
                Ok(())
            },
        )
        .unwrap();
        let seen = seen.into_inner().expect("lock not poisoned");
        assert_eq!(seen.len(), 6);
        assert_eq!(seen[0], (vec![0, 0], vec![2, 3]));
        assert_eq!(seen[5], (vec![4, 3], vec![1, 1]));
    }

    #[test]
    fn fold_traversal_cursors_preserves_shape_coverage() {
        let seen = try_fold_traversal_cursors(
            &[5, 4],
            &[2, 3],
            crate::TraversalSpec::chunks(vec![2, 3]),
            Vec::new(),
            |seen, cursor| {
                seen.push((cursor.position, cursor.shape));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen.len(), 6);
        assert_eq!(seen[0], (vec![0, 0], vec![2, 3]));
        assert_eq!(seen[5], (vec![4, 3], vec![1, 1]));
    }

    #[test]
    fn map_traversal_cursors_serial_propagates_consume_error() {
        let err = try_map_traversal_cursors_with_strategy(
            &[4, 4],
            &[2, 2],
            crate::TraversalSpec::chunks(vec![2, 2]),
            CursorMapWriteExecutionStrategy::Serial,
            Ok,
            |_cursor| {
                Err(LatticeError::InvalidTraversal(
                    "synthetic consume failure".into(),
                ))
            },
        )
        .unwrap_err();
        assert!(matches!(err, LatticeError::InvalidTraversal(_)));
    }

    #[test]
    fn map_traversal_cursors_pipelined_propagates_produce_error() {
        let err = try_map_traversal_cursors_with_strategy(
            &[4, 4],
            &[2, 2],
            crate::TraversalSpec::chunks(vec![2, 2]),
            CursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig { prefetch_depth: 2 }),
            |cursor| {
                if cursor.position == vec![2, 2] {
                    Err(LatticeError::InvalidTraversal(
                        "synthetic produce failure".into(),
                    ))
                } else {
                    Ok(cursor)
                }
            },
            |_cursor| Ok(()),
        )
        .unwrap_err();
        assert!(matches!(err, LatticeError::InvalidTraversal(_)));
    }

    #[test]
    fn ordered_map_traversal_cursors_pipelined_preserves_order() {
        let seen = std::sync::Mutex::new(Vec::new());
        try_map_traversal_cursors_ordered_with_strategy(
            &[5, 4],
            &[2, 3],
            crate::TraversalSpec::chunks(vec![2, 3]),
            OrderedCursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig {
                prefetch_depth: 2,
            }),
            || (),
            |_state, cursor| Ok((cursor.position, cursor.shape)),
            |item| {
                seen.lock().expect("lock not poisoned").push(item);
                Ok(())
            },
        )
        .unwrap();
        let seen = seen.into_inner().expect("lock not poisoned");
        assert_eq!(seen.len(), 6);
        assert_eq!(seen[0], (vec![0, 0], vec![2, 3]));
        assert_eq!(seen[5], (vec![4, 3], vec![1, 1]));
    }

    #[test]
    fn ordered_map_traversal_cursors_parallel_preserves_order() {
        let expected = std::sync::Mutex::new(Vec::new());
        try_map_traversal_cursors_ordered_with_strategy(
            &[6, 4],
            &[2, 2],
            crate::TraversalSpec::chunks(vec![2, 2]),
            OrderedCursorMapWriteExecutionStrategy::Serial,
            || (),
            |_state, cursor| Ok((cursor.position, cursor.shape)),
            |item| {
                expected.lock().expect("lock not poisoned").push(item);
                Ok(())
            },
        )
        .unwrap();
        let expected = expected.into_inner().expect("lock not poisoned");

        let seen = std::sync::Mutex::new(Vec::new());
        try_map_traversal_cursors_ordered_with_strategy(
            &[6, 4],
            &[2, 2],
            crate::TraversalSpec::chunks(vec![2, 2]),
            OrderedCursorMapWriteExecutionStrategy::Parallel(ParallelReadChunkConfig {
                workers: 3,
                prefetch_depth: 4,
            }),
            || (),
            |_state, cursor| {
                // Force some out-of-order worker completion so the writer path
                // has to reorder before consuming.
                let delay_ms = (cursor.position[0] % 3) as u64;
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                Ok((cursor.position, cursor.shape))
            },
            |item| {
                seen.lock().expect("lock not poisoned").push(item);
                Ok(())
            },
        )
        .unwrap();
        let seen = seen.into_inner().expect("lock not poisoned");
        assert_eq!(seen, expected);
    }

    #[test]
    fn ordered_map_traversal_cursors_parallel_propagates_map_error() {
        let err = try_map_traversal_cursors_ordered_with_strategy(
            &[6, 4],
            &[2, 2],
            crate::TraversalSpec::chunks(vec![2, 2]),
            OrderedCursorMapWriteExecutionStrategy::Parallel(ParallelReadChunkConfig {
                workers: 2,
                prefetch_depth: 2,
            }),
            || (),
            |_state, cursor| {
                if cursor.position == vec![2, 2] {
                    Err(LatticeError::InvalidTraversal(
                        "synthetic ordered map failure".into(),
                    ))
                } else {
                    Ok(cursor.position)
                }
            },
            |_item| Ok(()),
        )
        .unwrap_err();
        assert!(matches!(err, LatticeError::InvalidTraversal(_)));
    }

    #[test]
    fn ordered_map_traversal_cursors_pipelined_propagates_consume_error() {
        let err = try_map_traversal_cursors_ordered_with_strategy(
            &[4, 4],
            &[2, 2],
            crate::TraversalSpec::chunks(vec![2, 2]),
            OrderedCursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig {
                prefetch_depth: 2,
            }),
            || (),
            |_state, cursor| Ok(cursor.position),
            |position| {
                if position == vec![2, 0] {
                    Err(LatticeError::InvalidTraversal(
                        "synthetic ordered consume failure".into(),
                    ))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(err, LatticeError::InvalidTraversal(_)));
    }

    fn planner_inputs() -> ExecutionInputs {
        ExecutionInputs {
            task_count: 8,
            chunk_bytes: 1_024,
            per_worker_state_bytes: 2_048,
            memory_budget_bytes: 32_768,
            available_workers: 8,
            requested_worker_limit: 4,
            source_residency: SourceResidency::Persistent,
            prefetch_capability: true,
            configured_prefetch_cap: 8,
        }
    }

    #[test]
    fn execution_planner_is_formula_driven_and_byte_bounded() {
        let plan = plan_execution(ExecutionPolicy::Auto, planner_inputs()).unwrap();
        assert_eq!(plan.mode, ExecutionMode::Parallel);
        assert_eq!(plan.workers, 4);
        assert_eq!(plan.prefetch_depth, 4);
        assert_eq!(plan.worker_state_bytes, 8_192);
        assert_eq!(plan.prefetch_bytes, 4_096);
        assert!(plan.planned_resident_bytes <= 32_768);
    }

    #[test]
    fn execution_planner_handles_zero_work_and_one_worker_deliberately() {
        let mut inputs = planner_inputs();
        inputs.task_count = 0;
        assert_eq!(
            plan_execution(ExecutionPolicy::Auto, inputs)
                .unwrap()
                .workers,
            0
        );

        let mut inputs = planner_inputs();
        inputs.available_workers = 1;
        inputs.requested_worker_limit = 1;
        let plan = plan_execution(ExecutionPolicy::Auto, inputs).unwrap();
        assert_eq!(plan.mode, ExecutionMode::Pipelined);
        assert_eq!(plan.workers, 1);
    }

    #[test]
    fn explicit_policies_reject_requests_that_exceed_available_work() {
        let mut inputs = planner_inputs();
        inputs.task_count = 1;
        inputs.requested_worker_limit = 8;

        assert!(matches!(
            plan_execution(ExecutionPolicy::Pipelined { prefetch_depth: 4 }, inputs),
            Err(ExecutionPlanError::InvalidPolicy(_))
        ));

        assert!(matches!(
            plan_execution(
                ExecutionPolicy::Parallel {
                    workers: 8,
                    prefetch_depth: 16,
                },
                ExecutionInputs {
                    configured_prefetch_cap: 16,
                    ..inputs
                },
            ),
            Err(ExecutionPlanError::InvalidPolicy(_))
        ));
    }

    #[test]
    fn execution_planner_rejects_budget_policy_and_overflow_errors() {
        let mut inputs = planner_inputs();
        inputs.memory_budget_bytes = 2_047;
        assert!(matches!(
            plan_execution(ExecutionPolicy::Auto, inputs),
            Err(ExecutionPlanError::InsufficientMemory { .. })
        ));

        let inputs = planner_inputs();
        assert!(matches!(
            plan_execution(
                ExecutionPolicy::Parallel {
                    workers: 7,
                    prefetch_depth: 1,
                },
                inputs
            ),
            Err(ExecutionPlanError::InvalidPolicy(_))
        ));

        let mut inputs = planner_inputs();
        inputs.chunk_bytes = usize::MAX;
        inputs.memory_budget_bytes = usize::MAX;
        assert!(matches!(
            plan_execution(ExecutionPolicy::Pipelined { prefetch_depth: 2 }, inputs),
            Err(ExecutionPlanError::Overflow("prefetch bytes"))
        ));
    }
}
