// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time splits (#910): slices of a request planned together, whose
//! traversals share one completion. On both stores, whatever the cuts and
//! however the slices interleave, each scientific group becomes ready once,
//! after every slice has delivered its part and the work on it is done;
//! slices that do not tile the request are refused. The slices write back
//! at once through an updater each, claiming disjoint times.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, PoisonError};

use casa_ms::observation_io::{OpenOptions, UpdateSession, open_update};
use casa_observation::{
    AccessRequest, BatchBuffers, GroupKey, Grouping, Observation, ObservationError, PartitionId,
    PatchValues, ReadEvent, SliceCompletion, TimeBaselineGrid, TimeRange, TimeSplit, UpdateReport,
    UpdateTarget, VisibilityBatch, WritableRole,
};
use ndarray::Array3;

use super::{fixture, open_both, per_times, request, samples};

/// Groups' samples so far, and the groups made ready with their samples
/// at that moment.
#[derive(Default)]
struct Tally {
    samples: Mutex<BTreeMap<GroupKey, usize>>,
    ready: Mutex<Vec<(GroupKey, usize)>>,
}

impl Tally {
    fn add(&self, key: GroupKey, samples: usize) {
        *self
            .samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key)
            .or_default() += samples;
    }

    fn ready(&self, key: GroupKey) {
        let samples = self
            .samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .copied()
            .unwrap_or(0);
        self.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((key, samples));
    }
}

/// Consume one slice's traversal as a consumer of the split does: begin
/// work on each of a batch's fragments, close the parts the batch
/// completes, then finish the batch's work, so a part often closes with
/// its work still pending; end the slice once its traversal is over.
fn consume(
    observation: &dyn Observation,
    slice: &AccessRequest,
    completion: SliceCompletion,
    tally: &Tally,
) {
    let mut traversal = observation.bind_read(slice).expect("bind");
    let mut buffers = BatchBuffers::default();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        match event {
            ReadEvent::Batch(batch) => {
                let mut work = Vec::new();
                for fragment in batch.fragments {
                    work.push(completion.begin(fragment.key).expect("begin"));
                    tally.add(fragment.key, fragment.samples.len());
                }
                for fragment in batch.fragments {
                    if fragment.position.closes() && completion.close(fragment.key).expect("close")
                    {
                        tally.ready(fragment.key);
                    }
                }
                for token in work {
                    let key = token.key();
                    if completion.finish(token).expect("finish") {
                        tally.ready(key);
                    }
                }
            }
            ReadEvent::GroupEnd(key) => {
                if completion.close(key).expect("close") {
                    tally.ready(key);
                }
            }
        }
    }
    completion.end().expect("every part completed");
}

/// Each group's samples in one traversal of `request`.
fn groups(observation: &dyn Observation, request: &AccessRequest) -> BTreeMap<GroupKey, usize> {
    let mut traversal = observation.bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut groups = BTreeMap::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            for fragment in batch.fragments {
                *groups.entry(fragment.key).or_default() += fragment.samples.len();
            }
        }
    }
    groups
}

/// Partition 0's grid times cut at the indices `at`, as consecutive ranges.
fn cuts(observation: &dyn Observation, at: &[usize]) -> Vec<TimeRange> {
    let grid = observation.grid(PartitionId(0), "base").expect("grid");
    at.windows(2)
        .map(|pair| TimeRange::of_grid(&grid, pair[0]..pair[1]))
        .collect()
}

/// Whatever the cuts, a split's traversals finalize each of the whole
/// traversal's groups exactly once, with all its samples: run on a thread
/// per slice, and in reverse order, so a later slice closes its part of a
/// group before an earlier one has begun. The split names as divided the
/// groups more than one slice holds.
#[test]
fn a_time_split_finalizes_each_group_once_after_all_its_parts() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let times = observation
            .grid(PartitionId(0), "base")
            .expect("grid")
            .times_s
            .len();
        for grouping in [
            Grouping::TimeBins {
                width_s: 60.0,
                combine_fields: false,
                combine_scans: false,
            },
            Grouping::PerScan {
                combine_fields: true,
            },
        ] {
            let grouped = AccessRequest {
                grouping,
                ..per_times(observation, request(0..8, 0), 2)
            };
            let whole = groups(observation, &grouped);
            for at in [
                vec![0, times],
                vec![0, 5, times],
                vec![0, 3, 7, times],
                (0..=times).collect::<Vec<_>>(),
            ] {
                let split =
                    TimeSplit::plan(observation, &grouped, &cuts(observation, &at)).expect("plan");
                let held: Vec<_> = split
                    .slices()
                    .iter()
                    .map(|slice| groups(observation, slice))
                    .collect();
                let divided: BTreeMap<GroupKey, BTreeSet<usize>> = whole
                    .keys()
                    .map(|key| {
                        let holders = held.iter().enumerate();
                        let holders = holders.filter(|(_, g)| g.contains_key(key));
                        (*key, holders.map(|(slice, _)| slice).collect())
                    })
                    .filter(|(_, slices): &(_, BTreeSet<usize>)| slices.len() > 1)
                    .collect();
                assert_eq!(*split.divided(), divided, "{store} {grouping:?} {at:?}");
                if at.len() > 2 && matches!(grouping, Grouping::PerScan { .. }) {
                    assert!(!divided.is_empty(), "{store}: the cuts divide the scan");
                }
                for concurrent in [true, false] {
                    let completion = split.completion();
                    let tally = Tally::default();
                    if concurrent {
                        std::thread::scope(|scope| {
                            for (index, slice) in split.slices().iter().enumerate() {
                                let (completion, tally) = (completion.slice(index), &tally);
                                scope.spawn(move || consume(observation, slice, completion, tally));
                            }
                        });
                    } else {
                        for (index, slice) in split.slices().iter().enumerate().rev() {
                            consume(observation, slice, completion.slice(index), &tally);
                        }
                    }
                    let mut ready = tally
                        .ready
                        .into_inner()
                        .unwrap_or_else(PoisonError::into_inner);
                    ready.sort();
                    assert_eq!(
                        ready,
                        whole
                            .iter()
                            .map(|(key, samples)| (*key, *samples))
                            .collect::<Vec<_>>(),
                        "{store} {grouping:?} {at:?} concurrent {concurrent}"
                    );
                    assert!(completion.is_idle().expect("idle"), "{store}");
                }
            }
        }
    }
}

/// Cuts that leave a gap between them or overlap, start after the
/// request's first time or end before its last, or are missing are refused
/// before anything is read.
#[test]
fn a_time_split_must_tile_the_request() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let base = request(0..8, usize::MAX);
        let times = observation
            .grid(PartitionId(0), "base")
            .expect("grid")
            .times_s
            .len();
        let grid = observation.grid(PartitionId(0), "base").expect("grid");
        let gap = vec![
            TimeRange::of_grid(&grid, 0..3),
            TimeRange {
                start_s: grid.times_s[4],
                ..TimeRange::of_grid(&grid, 4..times)
            },
        ];
        let overlapping = vec![
            TimeRange::of_grid(&grid, 0..5),
            TimeRange::of_grid(&grid, 3..times),
        ];
        let late_start = vec![TimeRange::of_grid(&grid, 1..times)];
        let early_end = vec![TimeRange::of_grid(&grid, 0..5)];
        for refused in [gap, overlapping, late_start, early_end, Vec::new()] {
            assert!(
                matches!(
                    TimeSplit::plan(observation, &base, &refused),
                    Err(ObservationError::InvalidRequest { .. })
                ),
                "{store}: {refused:?}"
            );
        }
    }
}

/// Flags to set where a sample's grid time, baseline, channel and product
/// follow a pattern, so a write that lands on another time or baseline
/// shows.
fn flag_pattern(batch: &VisibilityBatch<'_>, grid: &TimeBaselineGrid) -> Array3<bool> {
    let per_time = batch.samples_per_time();
    Array3::from_shape_fn(batch.shape(), |(sample, channel, product)| {
        let seconds = batch
            .time
            .seconds_since(sample / per_time, grid.time_origin);
        let time = grid
            .times_s
            .binary_search_by(|known| known.total_cmp(&seconds))
            .expect("a grid time");
        (time + sample % per_time + channel + product).is_multiple_of(3)
    })
}

/// Write the pattern over `request`'s traversal through one updater of
/// `session`, and report what it wrote.
fn write_pattern(session: &UpdateSession, request: &AccessRequest) -> UpdateReport {
    let grid = session
        .observation()
        .grid(PartitionId(0), "base")
        .expect("grid");
    let mut traversal = session.observation().bind_read(request).expect("bind");
    let mut updater = session.updater(request, &[0]).expect("updater");
    let mut buffers = BatchBuffers::default();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            let set = flag_pattern(&batch, &grid);
            updater
                .write(
                    &batch,
                    &[PatchValues::Flags {
                        set: Some(set.view()),
                        clear: None,
                    }],
                )
                .expect("write");
        }
    }
    updater.finish().expect("finish")
}

/// Everything `path` holds, read after its session closed.
fn stored(path: &std::path::Path) -> BTreeMap<super::Key, super::Sample> {
    let observation = casa_ms::observation_io::open(path, &OpenOptions::default()).expect("reopen");
    samples(
        &mut observation
            .bind_read(&request(0..8, usize::MAX))
            .expect("bind"),
        observation.catalogue(),
    )
    .expect("read")
}

/// Times per slice: on MSv4 its natural time block, a chunk row; on MSv2,
/// whose rows no slice shares, any.
fn slice_times(name: &str, partition: &casa_observation::PartitionEstimate) -> usize {
    let block = if name.ends_with(".ms") {
        4
    } else {
        partition.natural_blocks.times
    };
    assert!(
        block < partition.times,
        "{name}: {block} of {} times",
        partition.times
    );
    block
}

/// A time split's slices write back at once, an updater per slice on its
/// own thread, and leave the store as one updater writing the whole
/// traversal does, on both stores. The cuts follow the store's natural
/// time blocks, so MSv4 updaters replace disjoint chunks; each reports its
/// own reads and writes, which add up to the whole traversal's.
#[test]
fn a_split_writes_back_its_slices_at_once() {
    let target = [UpdateTarget::new("base", WritableRole::Flags)];
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let (whole_root, split_root) = (
            tempfile::tempdir().expect("root"),
            tempfile::tempdir().expect("root"),
        );
        let (whole_path, split_path) = (
            fixture(whole_root.path(), name),
            fixture(split_root.path(), name),
        );
        let whole = {
            let session =
                open_update(&whole_path, &OpenOptions::default(), &target).expect("session");
            write_pattern(
                &session,
                &per_times(session.observation(), request(0..8, 0), 2),
            )
        };
        let reports = {
            let session =
                open_update(&split_path, &OpenOptions::default(), &target).expect("session");
            let observation = session.observation();
            let traversal = per_times(observation, request(0..8, 0), 2);
            let partition = &observation
                .estimate(&traversal)
                .expect("estimate")
                .partitions[0];
            let block = slice_times(name, partition);
            let mut at: Vec<usize> = (0..partition.times).step_by(block).collect();
            at.push(partition.times);
            assert!(at.len() > 2, "{name}: at least two slices of {block} times");
            let split =
                TimeSplit::plan(observation, &traversal, &cuts(observation, &at)).expect("plan");
            std::thread::scope(|scope| {
                let workers: Vec<_> = split
                    .slices()
                    .iter()
                    .map(|slice| {
                        let session = &session;
                        scope.spawn(move || write_pattern(session, slice))
                    })
                    .collect();
                workers
                    .into_iter()
                    .map(|worker| worker.join().expect("slice"))
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(stored(&split_path), stored(&whole_path), "{name}");
        let sum = |count: fn(&UpdateReport) -> usize| reports.iter().map(count).sum::<usize>();
        assert_eq!(
            (
                sum(|r| r.samples),
                sum(|r| r.units_read),
                sum(|r| r.units_written)
            ),
            (whole.samples, whole.units_read, whole.units_written),
            "{name}: {reports:?} against {whole:?}"
        );
    }
}

/// Live updaters of one session claim disjoint times of each variable:
/// slices cut anywhere on MSv2, whose rows no slice shares, and on MSv4
/// slices of whole chunk rows; a slice that shares a chunk row, or the
/// same times, with a live updater is refused until it is gone. A slice
/// of no times claims nothing.
#[test]
fn live_updaters_claim_disjoint_times() {
    let root = tempfile::tempdir().expect("root");
    let target = [UpdateTarget::new("base", WritableRole::Flags)];
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let session = open_update(&path, &OpenOptions::default(), &target).expect("session");
        let observation = session.observation();
        let whole = request(0..8, usize::MAX);
        let partition = &observation.estimate(&whole).expect("estimate").partitions[0];
        let (times, block) = (partition.times, slice_times(name, partition));
        let slice = |at: std::ops::Range<usize>| AccessRequest {
            times: casa_observation::TimeSelection::Within(
                cuts(observation, &[at.start, at.end])[0],
            ),
            ..whole.clone()
        };
        let aligned = [slice(0..block), slice(block..times)];
        let live: Vec<_> = aligned
            .iter()
            .map(|slice| session.updater(slice, &[0]).expect("disjoint chunk rows"))
            .collect();
        assert!(
            matches!(
                session.updater(&aligned[0], &[0]),
                Err(ObservationError::Conflict { .. })
            ),
            "{name}: the same times"
        );
        drop(live);
        let first = session.updater(&slice(0..block / 2), &[0]).expect("first");
        // A slice of no times, inside the first's chunk row, writes
        // nothing, so claims nothing.
        drop(
            session
                .updater(&slice(block / 2 + 1..block / 2 + 1), &[0])
                .expect("an empty slice"),
        );
        let second = session.updater(&slice(block / 2..times), &[0]);
        if name.ends_with(".ms") {
            assert!(second.is_ok(), "{name}: rows no other slice writes");
        } else {
            assert!(
                matches!(second, Err(ObservationError::Conflict { .. })),
                "{name}: a chunk row the first may replace"
            );
        }
        drop(second);
        drop(first);
        drop(
            session
                .updater(&slice(block / 2..times), &[0])
                .expect("free once the first is gone"),
        );
    }
}
