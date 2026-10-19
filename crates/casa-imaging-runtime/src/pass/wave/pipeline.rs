// SPDX-License-Identifier: LGPL-3.0-or-later
//! A wave whose region owners accumulate each slice as soon as it is placed
//! and their earlier slices are done, rather than all together at every
//! slice (#449).
//!
//! A pass's blocks follow the observation in time, and as the sky turns the
//! samples of one block load the strips unevenly even where the whole pass
//! loads them evenly. With one set of row chunks every owner waits at each
//! slice for the slowest: B3 lost about 2.7 s of a 13 s accumulation stage
//! that way. Here the wave places into several sets of chunks
//! ([`MajorCyclePass::chunk_sets`]); each owner accumulates the sets in
//! placement order, as it would one set, and a set is placed into again once
//! every owner has released it. Each owner's accumulator therefore sees the
//! same samples in the same order as with one set, and its grids are the
//! same bits; only the waiting moves.
//!
//! The owners' tasks run on the team's threads and never wait on each other
//! or on the placer: an owner with nothing queued ends its task, and the
//! placer, on the calling thread, starts it again with the next set. The
//! placer waits, off the team's threads, only for a set to come free.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, PoisonError, RwLock};

use super::{Chunk, Owner, Subtract, Wave};
use crate::pass::{
    BoundedSource, Cancel, MajorCyclePass, PassDomain, PassError, WorkerTeam, chunk_ranges, slices,
    stream_blocks,
};

/// One set of row chunks: the chunks, which the placer takes alone and the
/// owners share; how many of them the last placement filled; and how many
/// owners have yet to accumulate them.
struct ChunkSet {
    chunks: RwLock<Vec<Chunk>>,
    filled: AtomicUsize,
    pending: AtomicUsize,
}

/// One owner's lane: its accumulator, and the sets queued for it in
/// placement order with whether a task is draining them.
struct Lane<'w> {
    owner: Mutex<Owner<'w>>,
    queue: Mutex<(VecDeque<usize>, bool)>,
}

/// What a pipelined wave shares between its placer and its owners' tasks.
struct Pipeline<'w> {
    sets: Vec<ChunkSet>,
    lanes: Vec<Lane<'w>>,
    /// Signalled, under `released`, whenever a set comes free.
    released: Mutex<()>,
    freed: Condvar,
    /// The first error an owner met; the placer stops at the next slice.
    failure: Mutex<Option<PassError>>,
}

impl Pipeline<'_> {
    /// Record `error` unless an earlier one is recorded.
    fn fail(&self, error: PassError) {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(error);
    }

    /// The recorded error, if an owner met one.
    fn failed(&self) -> Option<PassError> {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Wait until no owner holds set `set`.
    fn wait_free(&self, set: usize) {
        let mut guard = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        while self.sets[set].pending.load(Ordering::Acquire) > 0 {
            guard = self
                .freed
                .wait(guard)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// Releases one owner's hold on a set when dropped, so the placer waking on
/// it learns of a panicking owner too.
struct Release<'a, 'w> {
    pipeline: &'a Pipeline<'w>,
    set: usize,
}

impl Drop for Release<'_, '_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.pipeline.fail(PassError::OwnerPanicked);
        }
        if self.pipeline.sets[self.set]
            .pending
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            let _guard = self
                .pipeline
                .released
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.pipeline.freed.notify_all();
        }
    }
}

/// What an owner's task reads besides the pipeline.
#[derive(Clone, Copy)]
struct Accumulation<'a> {
    pass: &'a MajorCyclePass<'a>,
    target: &'a PassDomain<'a>,
    weight_owner: Option<usize>,
    subtract: Subtract<'a>,
}

/// Accumulate lane `lane`'s queued sets in order until none is queued.
fn drain(pipeline: &Pipeline<'_>, lane: usize, work: Accumulation<'_>) {
    loop {
        let set = {
            let mut queue = pipeline.lanes[lane]
                .queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(set) = queue.0.pop_front() else {
                queue.1 = false;
                return;
            };
            set
        };
        let _release = Release { pipeline, set };
        let has_failed = pipeline
            .failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some();
        if has_failed {
            continue;
        }
        let chunks = pipeline.sets[set]
            .chunks
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let filled = pipeline.sets[set].filled.load(Ordering::Acquire);
        let result = match pipeline.lanes[lane].owner.lock() {
            Ok(mut owner) => owner.accumulate(
                work.pass,
                work.target,
                (lane, work.weight_owner),
                work.subtract,
                &chunks[..filled],
            ),
            Err(_) => Err(PassError::OwnerPanicked),
        };
        if let Err(error) = result {
            pipeline.fail(error);
        }
    }
}

impl Wave<'_, '_> {
    /// Whether this wave pipelines its owners over its sets of chunks: a
    /// pass that can ([`crate::pass::pipelines_owners`]) with more than one
    /// set, unless it hands its blocks to a visibility sink, which writes
    /// each block before its placement.
    pub(in crate::pass) fn pipelines(&self, sink: bool) -> bool {
        self.pass.chunk_sets > 1
            && !sink
            && crate::pass::pipelines_owners(
                self.pass.domains,
                self.pass.backend,
                self.pass.model.is_some(),
            )
    }

    /// Stream `source`'s blocks through the wave with its owners pipelined
    /// over `self.pass.chunk_sets` sets of chunks; returns the blocks
    /// consumed. The owners, and the chunks, are the wave's again when this
    /// returns, every owner having accumulated every placed slice unless an
    /// owner or the stream failed.
    pub(in crate::pass) fn stream_pipelined(
        &mut self,
        source: &mut dyn BoundedSource,
        cancel: &Cancel,
        timing: bool,
        team: &WorkerTeam,
    ) -> Result<u64, PassError> {
        let pass = self.pass;
        let target = &pass.domains[0];
        let domain = &mut self.domains[0];
        let weight_owner = domain.router.weight_owner();
        let work = Accumulation {
            pass,
            target,
            weight_owner,
            subtract: Subtract {
                model: domain.model,
                native: None,
            },
        };
        let mut sets = vec![std::mem::take(&mut self.chunks)];
        let count = sets[0].len();
        tracing::info!(
            "wave owners pipelined over {} sets of {count} chunks of {} rows",
            pass.chunk_sets,
            self.shape.rows
        );
        for _ in 1..pass.chunk_sets {
            sets.push(
                (0..count)
                    .map(|_| Chunk::new(self.shape, pass.domains))
                    .collect(),
            );
        }
        let pipeline = Pipeline {
            sets: sets
                .into_iter()
                .map(|chunks| ChunkSet {
                    chunks: RwLock::new(chunks),
                    filled: AtomicUsize::new(0),
                    pending: AtomicUsize::new(0),
                })
                .collect(),
            lanes: std::mem::take(&mut self.domains[0].owners)
                .into_iter()
                .map(|owner| Lane {
                    owner: Mutex::new(owner),
                    queue: Mutex::new((VecDeque::new(), false)),
                })
                .collect(),
            released: Mutex::new(()),
            freed: Condvar::new(),
            failure: Mutex::new(None),
        };
        let mut next = 0;
        let mut waiting = std::time::Duration::ZERO;
        let streamed = team
            .in_place_scope(|scope| {
                let streamed = stream_blocks(source, cancel, timing, |block| {
                    let rows = block.len();
                    for slice in slices(rows, count, self.shape.rows) {
                        let set = next;
                        next = (next + 1) % pipeline.sets.len();
                        let started = std::time::Instant::now();
                        pipeline.wait_free(set);
                        waiting += started.elapsed();
                        if let Some(error) = pipeline.failed() {
                            return Err(error);
                        }
                        let placing = std::time::Instant::now();
                        let mut chunks = pipeline.sets[set]
                            .chunks
                            .write()
                            .unwrap_or_else(PoisonError::into_inner);
                        let mut filled = 0;
                        for (chunk, rows) in chunks.iter_mut().zip(chunk_ranges(slice, count)) {
                            chunk.rows = rows;
                            filled += 1;
                        }
                        self.placer(block, 0, true)
                            .place(team, &mut chunks[..filled])?;
                        let placed = &chunks[..filled];
                        self.domains[0].add_row_work(placed);
                        self.samples += placed.iter().map(|chunk| chunk.placed).sum::<u64>();
                        drop(chunks);
                        let held = &pipeline.sets[set];
                        held.filled.store(filled, Ordering::Release);
                        held.pending.store(pipeline.lanes.len(), Ordering::Release);
                        self.stages.placing += placing.elapsed();
                        for (index, lane) in pipeline.lanes.iter().enumerate() {
                            let mut queue =
                                lane.queue.lock().unwrap_or_else(PoisonError::into_inner);
                            queue.0.push_back(set);
                            if !queue.1 {
                                queue.1 = true;
                                let pipeline = &pipeline;
                                scope.spawn(move |_| drain(pipeline, index, work));
                            }
                        }
                    }
                    self.weight_base += rows * block.channels();
                    Ok(())
                });
                let started = std::time::Instant::now();
                for set in 0..pipeline.sets.len() {
                    pipeline.wait_free(set);
                }
                waiting += started.elapsed();
                streamed
            })
            .expect("a pipelined wave's team has threads");
        let failure = pipeline.failed();
        self.stages.accumulating += waiting;
        let Pipeline { sets, lanes, .. } = pipeline;
        self.domains[0].owners = lanes
            .into_iter()
            .map(|lane| {
                lane.owner
                    .into_inner()
                    .map_err(|_| PassError::OwnerPanicked)
            })
            .collect::<Result<_, _>>()?;
        self.chunks = sets
            .into_iter()
            .next()
            .expect("the first set")
            .chunks
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        let blocks = streamed?;
        failure.map_or(Ok(blocks), Err)
    }
}
