// SPDX-License-Identifier: LGPL-3.0-or-later
//! Example B's reusable consumer: the production gain accumulation and
//! solver fed from `VisibilityBatch` fragments of any [`Observation`].
//!
//! This module sees only the shared interface. It reads each sample's time,
//! time centroid, interval and scan from the batch, and each value's
//! exclusion from the batch's validity rule, so it applies no rule of
//! either store. It imports no MSv2 or MSv4 type; the setup opens the store
//! (`batch_trial.rs`).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use casa_observation::{
    AccessRequest, BatchBuffers, BufferPool, Catalogue, GroupCompletion, GroupKey, Observation,
    OwnedEvent, ReadEvent, VisibilityBatch, WorkToken,
};
use casa_types::measures::MjdHighPrec;
use ndarray::{ArrayView2, s};

use crate::solve::grouping::{SolveAccumulator, SolveGroupOptions, SolveSampleRow, SolveSamples};
use crate::solve::kernel::SolutionRow;
use crate::solve::{GainSolveRequest, apply_flex_refant, nearest_first, solve_closed_group};

/// A fresh accumulator for group `key`.
fn accumulator(key: GroupKey) -> SolveAccumulator {
    let field = key.field.map_or(0, |field| field.0 as i32);
    SolveAccumulator::new(field, key.partition.0 as i32, 0)
}

/// Accumulate `batch`'s samples `samples` into `group`. Visibilities and
/// weights are views of the batch, transposed to the `[correlation,
/// channel]` order the accumulator reads (a weight shared by every channel
/// is the same view with a zero channel stride); the flags are the batch's
/// validity rule, evaluated once over the samples
/// ([`Validity::mask_into`](casa_observation::Validity::mask_into))
/// and viewed the same way.
///
/// # Panics
///
/// If the batch lacks a role the solve reads (the request asks for all),
/// or a scan has no casacore number.
fn observe(
    group: &mut SolveAccumulator,
    batch: &VisibilityBatch<'_>,
    samples: Range<usize>,
    catalogue: &Catalogue,
    request: &GainSolveRequest,
) {
    let values = batch.group.values.expect("values");
    let weights = batch.group.weights.expect("weights");
    let window = batch
        .sample_window(samples.clone())
        .expect("a fragment is whole times");
    let mut mask = Vec::new();
    window.validity().mask_into(&mut mask, window.shape());
    let codes: Vec<i32> = batch
        .correlations
        .iter()
        .map(|c| c.casacore_code())
        .collect();
    let channels: Vec<usize> = (0..batch.channel_count()).collect();
    let cell = channels.len() * codes.len();
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let centroid = batch.timing.centroid.expect("time centroid");
    let per_time = batch.samples_per_time();
    let first = samples.start;
    let options = SolveGroupOptions {
        gain_type: request.gain_type,
        model_source: request.model_source,
        stokes_i: request.smodel[0],
        solve_interval: request.solve_interval,
        combine: request.combine,
        channelized: false,
    };
    for sample in samples {
        let coordinate = batch.sample(sample);
        if coordinate.baseline.is_auto() {
            continue;
        }
        let flags = ArrayView2::from_shape(
            (channels.len(), codes.len()),
            &mask[(sample - first) * cell..(sample - first + 1) * cell],
        )
        .expect("one cell")
        .reversed_axes();
        let scan = batch.scan(sample).expect("scan");
        group
            .observe_samples(
                &SolveSampleRow {
                    field_id: batch.field[sample / per_time].0 as i32,
                    data_desc_id: 0,
                    antenna1: coordinate.baseline.antenna1.0 as i32,
                    antenna2: coordinate.baseline.antenna2.0 as i32,
                    time_seconds: batch.time.seconds_since(sample / per_time, mjd_zero),
                    time_centroid_seconds: centroid.seconds_since(sample, mjd_zero),
                    interval_seconds: batch.interval_s(sample).expect("interval"),
                    scan_number: catalogue.scans[scan.0 as usize]
                        .number
                        .expect("casacore scan numbers"),
                },
                &SolveSamples {
                    data: values.slice(s![sample, .., ..]).reversed_axes(),
                    flags,
                    weights: weights.slice(s![sample, .., ..]).reversed_axes(),
                    model: None,
                },
                &codes,
                &channels,
                &options,
            )
            .expect("observe");
    }
}

/// What a batch-fed solve measured.
#[derive(Debug, Default)]
pub(super) struct SolveTrace {
    pub(super) batches: usize,
    pub(super) fragments: usize,
    /// Groups live at once, and the most accumulator entries they held.
    pub(super) max_live_groups: usize,
    pub(super) max_state_entries: usize,
    /// Groups closed by a fragment marker, by `GroupEnd`, and closed with
    /// fragment work still outstanding.
    pub(super) closed_by_fragment: usize,
    pub(super) closed_by_end: usize,
    pub(super) closed_with_pending: usize,
    pub(super) buffer_bytes: usize,
}

/// The antennas other than `refant`, nearest it first: production's
/// alternates when the reference antenna is flagged, from the catalogue's
/// positions.
fn alternates(catalogue: &Catalogue, refant: i32) -> Vec<i32> {
    let positions: Vec<[f64; 3]> = catalogue
        .antennas
        .iter()
        .map(|antenna| antenna.position.as_itrf())
        .collect();
    nearest_first(&positions, refant)
}

/// The catalogue's antennas, as the solver numbers them.
fn antennas(catalogue: &Catalogue) -> BTreeSet<i32> {
    (0..catalogue.antennas.len() as i32).collect()
}

/// Solve one closed group into the rows.
fn finish(
    mut group: SolveAccumulator,
    available: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant: i32,
    rows: &mut Vec<SolutionRow>,
) {
    group.finalize_for_solve(request.solve_mode);
    rows.extend(
        solve_closed_group(group, available, request, refant)
            .into_iter()
            .flatten(),
    );
}

/// Sequential realization: one batch at a time, fragments accumulated in
/// delivery order, each group solved when its input completes. `refant` is
/// in the observation's antenna numbering.
pub(super) fn solve_sequential(
    observation: &dyn Observation,
    request: &GainSolveRequest,
    access: &AccessRequest,
    refant: i32,
) -> (Vec<SolutionRow>, SolveTrace) {
    let catalogue = observation.catalogue();
    let available = antennas(catalogue);
    let mut traversal = observation.bind_read(access).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut live = BTreeMap::<GroupKey, SolveAccumulator>::new();
    let mut rows = Vec::new();
    let mut trace = SolveTrace::default();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        let closed: Vec<GroupKey> = match event {
            ReadEvent::GroupEnd(key) => {
                trace.closed_by_end += 1;
                vec![key]
            }
            ReadEvent::Batch(batch) => {
                trace.batches += 1;
                let mut closed = Vec::new();
                for fragment in batch.fragments {
                    trace.fragments += 1;
                    let group = live
                        .entry(fragment.key)
                        .or_insert_with(|| accumulator(fragment.key));
                    observe(group, &batch, fragment.samples.clone(), catalogue, request);
                    if fragment.position.closes() {
                        trace.closed_by_fragment += 1;
                        closed.push(fragment.key);
                    }
                }
                trace.max_live_groups = trace.max_live_groups.max(live.len());
                trace.max_state_entries = trace
                    .max_state_entries
                    .max(live.values().map(SolveAccumulator::state_entries).sum());
                closed
            }
        };
        for key in closed {
            let group = live.remove(&key).expect("live group");
            finish(group, &available, request, refant, &mut rows);
        }
    }
    trace.buffer_bytes = buffers.capacity_bytes();
    assert!(live.is_empty(), "every group closed");
    apply_flex_refant(&mut rows, refant, &alternates(catalogue, refant));
    (rows, trace)
}

/// Batch buffers in the concurrent realization's pool.
const CONCURRENT_POOL: usize = 4;

/// Concurrent realization: each fragment is a work unit on its own thread,
/// accumulating a partial group from a shared batch (`Arc<FilledBatch>`).
/// The first is held back, so its group's input completes while its work
/// is pending; [`GroupCompletion`] releases the group for solving only
/// when every fragment's work has handed back its token. A batch's buffer
/// returns to the pool when its last fragment's work lets go of it; the
/// producer waits on the pool while all of its buffers are in use, and
/// takes none at the end.
pub(super) fn solve_concurrent(
    observation: &dyn Observation,
    request: &GainSolveRequest,
    access: &AccessRequest,
    refant: i32,
) -> (Vec<SolutionRow>, SolveTrace) {
    let catalogue = observation.catalogue();
    let available = antennas(catalogue);
    let mut traversal = observation.bind_read(access).expect("bind");
    let mut trace = SolveTrace::default();
    let mut rows = Vec::new();
    let mut partial = BTreeMap::<GroupKey, SolveAccumulator>::new();
    let mut completion = GroupCompletion::default();
    let (done_tx, done_rx) = mpsc::channel::<(WorkToken, SolveAccumulator)>();
    // Dropped once production ends, so waiting for work ends when the
    // last unit has sent or died.
    let mut done_tx = Some(done_tx);
    let mut ready = |key: GroupKey, partial: &mut BTreeMap<GroupKey, SolveAccumulator>| {
        let group = partial.remove(&key).expect("group");
        finish(group, &available, request, refant, &mut rows);
    };
    // Join finished work, solving each group it makes ready.
    let join = |completion: &mut GroupCompletion,
                partial: &mut BTreeMap<GroupKey, SolveAccumulator>,
                ready: &mut dyn FnMut(GroupKey, &mut BTreeMap<GroupKey, SolveAccumulator>),
                wait: bool| {
        let mut next = if wait {
            done_rx.recv().ok()
        } else {
            done_rx.try_recv().ok()
        };
        while let Some((token, group)) = next {
            let key = token.key();
            match partial.get_mut(&key) {
                Some(total) => total.merge(group),
                None => {
                    partial.insert(key, group);
                }
            }
            if completion
                .finish(token)
                .expect("no unit abandoned its work")
            {
                ready(key, partial);
            }
            next = done_rx.try_recv().ok();
        }
    };
    let pool = BufferPool::new(CONCURRENT_POOL, traversal.estimate.peak_buffer_bytes());
    std::thread::scope(|scope| {
        let mut first = true;
        loop {
            let filled = match traversal.next_owned(&pool).expect("read") {
                OwnedEvent::End => break,
                OwnedEvent::GroupEnd(key) => {
                    trace.closed_by_end += 1;
                    if completion.pending(key) > 0 {
                        trace.closed_with_pending += 1;
                    }
                    if completion.close(key).expect("one closure") {
                        ready(key, &mut partial);
                    }
                    join(&mut completion, &mut partial, &mut ready, false);
                    continue;
                }
                OwnedEvent::Batch(filled) => Arc::new(filled),
            };
            trace.batches += 1;
            trace.buffer_bytes = trace.buffer_bytes.max(filled.capacity_bytes());
            for fragment in filled.batch().fragments.to_vec() {
                trace.fragments += 1;
                let token = completion.begin(fragment.key).expect("an open group");
                let filled = filled.clone();
                let done_tx = done_tx.as_ref().expect("producing").clone();
                let delay = std::mem::take(&mut first);
                scope.spawn(move || {
                    if delay {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    let mut group = accumulator(fragment.key);
                    observe(
                        &mut group,
                        &filled.batch(),
                        fragment.samples.clone(),
                        catalogue,
                        request,
                    );
                    let _ = done_tx.send((token, group));
                });
                if fragment.position.closes() {
                    trace.closed_by_fragment += 1;
                    if completion.pending(fragment.key) > 1 {
                        trace.closed_with_pending += 1;
                    }
                    if completion.close(fragment.key).expect("one closure") {
                        ready(fragment.key, &mut partial);
                    }
                }
            }
            trace.max_live_groups = trace.max_live_groups.max(partial.len().max(1));
            drop(filled);
            join(&mut completion, &mut partial, &mut ready, false);
        }
        done_tx = None;
        while !completion.is_idle().expect("no unit abandoned its work") {
            join(&mut completion, &mut partial, &mut ready, true);
        }
    });
    assert_eq!(pool.free(), CONCURRENT_POOL, "every buffer came back");
    drop(ready);
    rows.sort_by(|a, b| {
        (a.time_seconds, a.antenna_id)
            .partial_cmp(&(b.time_seconds, b.antenna_id))
            .expect("finite")
    });
    apply_flex_refant(&mut rows, refant, &alternates(catalogue, refant));
    (rows, trace)
}
