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
    AccessRequest, BatchBuffers, Catalogue, GroupCompletion, GroupKey, Observation, OwnedEvent,
    ReadEvent, VisibilityBatch, WeightView,
};
use casa_types::measures::MjdHighPrec;
use ndarray::{Array2, Axis, s};

use crate::solve::grouping::{SolveAccumulator, SolveSampleRow, SolveSamples, SolveWeights};
use crate::solve::kernel::SolutionRow;
use crate::solve::{GainSolveRequest, apply_flex_refant, solve_closed_group};

/// A fresh accumulator for group `key`.
fn accumulator(key: GroupKey) -> SolveAccumulator {
    let field = key.field.map_or(0, |field| field.0 as i32);
    SolveAccumulator::new(field, key.partition.0 as i32, 0)
}

/// Accumulate `batch`'s samples `samples` into `group`. Visibilities and
/// weights are views of the batch, transposed to the `[correlation,
/// channel]` order the accumulator reads; the flags are the batch's
/// validity rule, written into one small reusable array per call.
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
    let values = batch
        .group
        .values
        .expect("values")
        .as_complex32()
        .expect("c64");
    let weights = batch.group.weights.expect("weights");
    let validity = batch.validity();
    let codes: Vec<i32> = batch
        .correlations
        .iter()
        .map(|c| c.casacore_code())
        .collect();
    let channels: Vec<usize> = (0..batch.channel_count()).collect();
    let mut flags = Array2::from_elem((codes.len(), channels.len()), false);
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let time = batch.topology.time();
    let centroid = batch.timing.centroid.expect("time centroid");
    let per_time = batch.topology.samples_per_time();
    for sample in samples {
        let coordinate = batch.topology.sample(sample);
        if coordinate.baseline.is_auto() {
            continue;
        }
        for ((correlation, channel), flag) in flags.indexed_iter_mut() {
            *flag = validity.excluded(sample, channel, correlation);
        }
        let scan = batch.scan(sample).expect("scan");
        group
            .observe_samples(
                &SolveSamples {
                    row: SolveSampleRow {
                        data_desc_id: 0,
                        antenna1: coordinate.baseline.antenna1.0 as i32,
                        antenna2: coordinate.baseline.antenna2.0 as i32,
                        time_seconds: time.seconds_since(sample / per_time, mjd_zero),
                        time_centroid_seconds: centroid.seconds_since(sample, mjd_zero),
                        interval_seconds: batch.interval_s(sample).expect("interval"),
                        scan_number: catalogue.scans[scan.0 as usize]
                            .number
                            .expect("casacore scan numbers"),
                    },
                    data: values.slice(s![sample, .., ..]).reversed_axes(),
                    flags: flags.view(),
                    weights: match weights {
                        WeightView::PerCorrelation(weights) => {
                            SolveWeights::Row(weights.index_axis_move(Axis(0), sample))
                        }
                        WeightView::Spectral(weights) => SolveWeights::Spectrum(
                            weights.slice_move(s![sample, .., ..]).reversed_axes(),
                        ),
                    },
                    model: None,
                    correlation_types: &codes,
                    selected_channels: &channels,
                },
                request.gain_type,
                request.model_source,
                request.smodel[0],
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
    rows.extend(solve_closed_group(group, available, request, refant).expect("solve"));
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
    let mut cursor = observation.bind_read(access).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut live = BTreeMap::<GroupKey, SolveAccumulator>::new();
    let mut rows = Vec::new();
    let mut trace = SolveTrace::default();
    while let Some(event) = cursor.next(&mut buffers).expect("read") {
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
    apply_flex_refant(&mut rows, refant);
    (rows, trace)
}

/// Batch buffers the concurrent realization circulates.
const CONCURRENT_POOL: usize = 4;

/// Concurrent realization: each fragment is a work unit on its own thread,
/// accumulating a partial group from a shared batch (`Arc<FilledBatch>`).
/// The first is held back, so its group's input completes while its work
/// is pending; [`GroupCompletion`] releases the group for solving only
/// when every fragment's work has joined. A batch's buffers return to the
/// pool when its last fragment's work lets go of them, and at the end the
/// cursor hands the last buffers back.
pub(super) fn solve_concurrent(
    observation: &dyn Observation,
    request: &GainSolveRequest,
    access: &AccessRequest,
    refant: i32,
) -> (Vec<SolutionRow>, SolveTrace) {
    let catalogue = observation.catalogue();
    let available = antennas(catalogue);
    let mut cursor = observation.bind_read(access).expect("bind");
    let mut trace = SolveTrace::default();
    let mut rows = Vec::new();
    let mut partial = BTreeMap::<GroupKey, SolveAccumulator>::new();
    let mut completion = GroupCompletion::default();
    let (done_tx, done_rx) = mpsc::channel::<(GroupKey, SolveAccumulator)>();
    let (free_tx, free_rx) = mpsc::channel::<BatchBuffers>();
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
        while let Some((key, group)) = next {
            match partial.get_mut(&key) {
                Some(total) => total.merge(group),
                None => {
                    partial.insert(key, group);
                }
            }
            if completion.finish(key).expect("pending work") {
                ready(key, partial);
            }
            next = done_rx.try_recv().ok();
        }
    };
    std::thread::scope(|scope| {
        let mut allocated = 0;
        let mut spare: Option<BatchBuffers> = None;
        let mut first = true;
        loop {
            let buffers = match spare.take().or_else(|| free_rx.try_recv().ok()) {
                Some(buffers) => buffers,
                None if allocated < CONCURRENT_POOL => {
                    allocated += 1;
                    BatchBuffers::default()
                }
                None => free_rx.recv().expect("a buffer comes back"),
            };
            let filled = match cursor.next_owned(buffers).expect("read") {
                OwnedEvent::End(_) => break,
                OwnedEvent::GroupEnd(key, returned) => {
                    spare = Some(returned);
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
                completion.begin(fragment.key).expect("an open group");
                let (filled, done_tx, free_tx) = (filled.clone(), done_tx.clone(), free_tx.clone());
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
                    let _ = done_tx.send((fragment.key, group));
                    if let Some(filled) = Arc::into_inner(filled) {
                        let _ = free_tx.send(filled.into_buffers());
                    }
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
            if let Some(filled) = Arc::into_inner(filled) {
                spare = Some(filled.into_buffers());
            }
            join(&mut completion, &mut partial, &mut ready, false);
        }
        while !completion.is_idle() {
            join(&mut completion, &mut partial, &mut ready, true);
        }
    });
    drop(ready);
    rows.sort_by(|a, b| {
        (a.time_seconds, a.antenna_id)
            .partial_cmp(&(b.time_seconds, b.antenna_id))
            .expect("finite")
    });
    apply_flex_refant(&mut rows, refant);
    (rows, trace)
}
