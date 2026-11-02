// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time selections (#910): slices of a partition's grid times, on both
//! stores. Slices that tile the times deliver the whole traversal; groups
//! keep their keys and close within each slice; slices aligned to the
//! store's natural time blocks decode no unit twice; ranges holding no time
//! are empty and malformed ones refused; a sliced traversal writes back.

use std::collections::BTreeMap;

use casa_ms::observation_io::{OpenOptions, open_update};
use casa_observation::{
    AccessRequest, BatchBuffers, FragmentPosition, GroupKey, Grouping, Observation,
    ObservationError, PartitionId, PatchValues, ReadEvent, TimeRange, TimeRuns, TimeSelection,
    UpdateTarget, WritableRole,
};
use casa_types::measures::{EpochRef, MjdHighPrec};
use ndarray::Array3;

use super::{Key, Sample, by_scan, fixture, open_both, per_times, request, samples, three_scans};

/// `request` restricted to grid times `times` of partition 0.
fn sliced(
    observation: &dyn Observation,
    request: &AccessRequest,
    times: std::ops::Range<usize>,
) -> AccessRequest {
    let grid = observation
        .grid(PartitionId(0), &request.data_group)
        .expect("grid");
    AccessRequest {
        times: TimeSelection::Within(TimeRange::of_grid(&grid, times)),
        ..request.clone()
    }
}

/// Every sample `request` delivers.
fn read(observation: &dyn Observation, request: &AccessRequest) -> BTreeMap<Key, Sample> {
    samples(
        &mut observation.bind_read(request).expect("bind"),
        observation.catalogue(),
    )
    .expect("read")
}

/// Slices that tile a partition's times deliver exactly the whole
/// traversal, sample for sample, on both stores; each slice's estimate
/// gives its times and the first of them.
#[test]
fn time_slices_tile_a_partition_sample_for_sample() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let whole_request = per_times(observation, request(0..8, 0), 2);
        let whole = read(observation, &whole_request);
        let times = observation
            .grid(PartitionId(0), "base")
            .expect("grid")
            .times_s
            .len();
        for cuts in [
            vec![0, times],
            vec![0, 5, times],
            vec![0, 3, 7, times],
            (0..=times).collect::<Vec<_>>(),
        ] {
            let mut tiled = BTreeMap::new();
            for pair in cuts.windows(2) {
                let slice = sliced(observation, &whole_request, pair[0]..pair[1]);
                let estimate = observation.estimate(&slice).expect("estimate");
                let partition = &estimate.partitions[0];
                assert_eq!(
                    (&partition.time_runs, partition.times),
                    (&TimeRuns::whole(pair[0]..pair[1]), pair[1] - pair[0]),
                    "{store} {pair:?}"
                );
                assert_eq!(
                    partition.batches,
                    (pair[1] - pair[0]).div_ceil(2),
                    "{store} {pair:?}"
                );
                for (key, sample) in read(observation, &slice) {
                    assert!(tiled.insert(key, sample).is_none(), "{store}: once");
                }
            }
            assert_eq!(tiled, whole, "{store} {cuts:?}");
        }
    }
}

/// Each group's samples, whether a fragment of it was clipped, and its
/// closure count, over one traversal; fragments are checked to be
/// positioned and closed within it.
fn group_parts(
    observation: &dyn Observation,
    request: &AccessRequest,
) -> BTreeMap<GroupKey, (usize, bool)> {
    let mut traversal = observation.bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut parts = BTreeMap::<GroupKey, (usize, bool)>::new();
    let mut closed = Vec::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        for fragment in batch.fragments {
            assert!(!closed.contains(&fragment.key), "no data after closure");
            let first = !parts.contains_key(&fragment.key);
            assert_eq!(
                first,
                matches!(
                    fragment.position,
                    FragmentPosition::First | FragmentPosition::Whole
                ),
                "{fragment:?}"
            );
            let part = parts.entry(fragment.key).or_default();
            part.0 += fragment.samples.len();
            part.1 |= fragment.clipped;
            if fragment.position.closes() {
                closed.push(fragment.key);
            }
        }
    }
    assert_eq!(closed.len(), parts.len(), "every part closed once");
    parts
}

/// Groups keep the keys they have over the whole grid in every slice, and
/// each slice delivers and closes the part of each group it holds; a group
/// a slice cuts is marked clipped there, one whole within it is not, and
/// the parts add up to the whole traversal's groups.
#[test]
fn groups_keep_their_keys_and_close_within_each_slice() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let grouped = AccessRequest {
            grouping: Grouping::TimeBins {
                width_s: 60.0,
                combine_fields: false,
                combine_scans: false,
            },
            ..per_times(observation, request(0..8, 0), 3)
        };
        let whole = group_parts(observation, &grouped);
        assert!(whole.values().all(|(_, clipped)| !clipped), "{store}");
        let times = observation
            .grid(PartitionId(0), "base")
            .expect("grid")
            .times_s
            .len();
        for cut in 1..times {
            let mut added = BTreeMap::<GroupKey, usize>::new();
            let mut slices = Vec::new();
            for times in [0..cut, cut..times] {
                let parts = group_parts(observation, &sliced(observation, &grouped, times));
                for (key, (samples, _)) in &parts {
                    *added.entry(*key).or_default() += samples;
                }
                slices.push(parts);
            }
            assert_eq!(
                added,
                whole
                    .iter()
                    .map(|(key, (samples, _))| (*key, *samples))
                    .collect(),
                "{store} cut at {cut}"
            );
            for (key, (samples, _)) in &whole {
                let cut_here = slices.iter().all(|parts| parts.contains_key(key));
                for parts in &slices {
                    if let Some((part, clipped)) = parts.get(key) {
                        assert_eq!(*clipped, cut_here, "{store} cut at {cut}: {key:?}");
                        assert!(cut_here || part == samples, "{store}");
                    }
                }
            }
        }
    }
}

/// Slices aligned to the store's natural time blocks decode no unit twice:
/// their decode units add up to the whole traversal's, modelled and
/// counted alike. An MSv4 slice that splits a chunk decodes it in both
/// slices; an MSv2 reads rows, which no slice shares.
#[test]
fn aligned_slices_decode_no_unit_twice() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let whole_request = request(0..8, usize::MAX);
        let whole = observation
            .estimate(&whole_request)
            .expect("estimate")
            .partitions[0]
            .clone();
        let block = whole.natural_blocks.times;
        let units = |slices: &[std::ops::Range<usize>]| -> (usize, usize) {
            slices.iter().fold((0, 0), |(modelled, counted), times| {
                let slice = sliced(observation, &whole_request, times.clone());
                let estimate = observation.estimate(&slice).expect("estimate");
                let mut traversal = observation.bind_read(&slice).expect("bind");
                let mut buffers = BatchBuffers::default();
                while traversal.next(&mut buffers).expect("read").is_some() {}
                (
                    modelled + estimate.decode_units(),
                    counted + traversal.counters().decode_units,
                )
            })
        };
        let aligned: Vec<_> = (0..whole.times)
            .step_by(block)
            .map(|start| start..(start + block).min(whole.times))
            .collect();
        assert_eq!(
            units(&aligned),
            (whole.decode_units, whole.decode_units),
            "{store}: aligned to {block} times"
        );
        if store == "v4" {
            let split = units(&[0..block / 2, block / 2..whole.times]);
            assert!(
                split.0 > whole.decode_units && split.1 == split.0,
                "{store}: a split chunk is decoded twice: {split:?}"
            );
        }
    }
}

/// A range holding no grid time (between two, or past the end) is an
/// empty traversal; a range that is not an interval, or is stated in
/// another scale or origin than the partition's, is refused at binding.
#[test]
fn empty_ranges_are_empty_and_malformed_ones_refused() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let base = request(0..8, usize::MAX);
        let grid = observation.grid(PartitionId(0), "base").expect("grid");
        let within = |start_s: f64, end_s: f64| AccessRequest {
            times: TimeSelection::Within(TimeRange {
                scale: grid.time_scale,
                origin: grid.time_origin,
                start_s,
                end_s,
            }),
            ..base.clone()
        };
        let (first, last) = (grid.times_s[0], *grid.times_s.last().expect("times"));
        let between = (grid.times_s[3] + grid.times_s[4]) / 2.0;
        for empty in [
            within(between, grid.times_s[4]),
            within(last + 1.0, f64::INFINITY),
            within(f64::NEG_INFINITY, first),
        ] {
            let estimate = observation.estimate(&empty).expect("estimate");
            assert_eq!(
                (estimate.partitions[0].times, estimate.batches()),
                (0, 0),
                "{store}"
            );
            let mut traversal = observation.bind_read(&empty).expect("bind");
            assert!(
                traversal
                    .next(&mut BatchBuffers::default())
                    .expect("read")
                    .is_none(),
                "{store}"
            );
        }
        let other_scale = AccessRequest {
            times: TimeSelection::Within(TimeRange {
                scale: EpochRef::TAI,
                ..TimeRange::of_grid(&grid, 0..2)
            }),
            ..base.clone()
        };
        let other_origin = AccessRequest {
            times: TimeSelection::Within(TimeRange {
                origin: MjdHighPrec::new(grid.time_origin.day() + 1.0, 0.0),
                ..TimeRange::of_grid(&grid, 0..2)
            }),
            ..base.clone()
        };
        for refused in [
            within(last, first),
            within(f64::NAN, last),
            other_scale,
            other_origin,
        ] {
            assert!(
                matches!(
                    observation.bind_read(&refused),
                    Err(ObservationError::InvalidRequest { .. })
                ),
                "{store}: {:?}",
                refused.times
            );
        }
    }
}

/// A sliced traversal writes back through an updater on both stores: the
/// slice's samples change, the others do not.
#[test]
fn a_sliced_traversal_writes_back_its_times() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let before = {
            let observation =
                casa_ms::observation_io::open(&path, &OpenOptions::default()).expect("open");
            read(&*observation, &request(0..8, usize::MAX))
        };
        let target = UpdateTarget::new("base", WritableRole::Flags);
        let session = open_update(&path, &OpenOptions::default(), &[target]).expect("session");
        let slice = sliced(
            session.observation(),
            &per_times(session.observation(), request(0..8, 0), 2),
            3..8,
        );
        let mut traversal = session.observation().bind_read(&slice).expect("bind");
        let mut updater = session.updater(&slice, &[0]).expect("updater");
        let mut buffers = BatchBuffers::default();
        let mut slice_times = Vec::new();
        while let Some(event) = traversal.next(&mut buffers).expect("read") {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            let mjd_zero = MjdHighPrec::new(0.0, 0.0);
            slice_times.extend(
                (0..batch.times()).map(|time| batch.time.seconds_since(time, mjd_zero).to_bits()),
            );
            let set = Array3::from_elem(batch.shape(), true);
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
        updater.finish().expect("finish");
        drop(traversal);
        drop(session);
        assert_eq!(slice_times.len(), 5, "{name}");
        let observation =
            casa_ms::observation_io::open(&path, &OpenOptions::default()).expect("reopen");
        let after = read(&*observation, &request(0..8, usize::MAX));
        for (key, sample) in &before {
            let written = &after[key];
            if slice_times.contains(&key.0) {
                assert!(written.flags.iter().all(|flag| *flag), "{name}: {key:?}");
            } else {
                assert_eq!(written.flags, sample.flags, "{name}: {key:?}");
            }
        }
    }
}

/// A time range can hold no time of some requested partitions, at the start
/// of the request or between others: the updater passes them, and the
/// slice writes back. The MSv2 fixture, its twelve times relabelled as
/// scans 1, 2 and 3 of four times each and split by scan, has three
/// partitions in time order; a range over the first two leaves the third
/// empty, listed between them, first or last.
#[test]
fn a_sliced_write_back_passes_partitions_the_range_leaves_empty() {
    let options = by_scan();
    for order in [[0, 2, 1], [2, 0, 1], [0, 1, 2]] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        three_scans(&path);
        let target = UpdateTarget::new("base", WritableRole::Flags);
        let session = open_update(&path, &options, &[target]).expect("session");
        let observation = session.observation();
        let mut firsts: Vec<(f64, PartitionId)> = observation
            .catalogue()
            .partition_ids()
            .into_iter()
            .map(|id| (observation.grid(id, "base").expect("grid").times_s[0], id))
            .collect();
        firsts.sort_by(|a, b| a.0.total_cmp(&b.0));
        assert_eq!(firsts.len(), 3, "three scans");
        let in_time = [firsts[0].1, firsts[1].1, firsts[2].1];
        let last = observation.grid(in_time[2], "base").expect("grid");
        let sliced = AccessRequest {
            partitions: order.iter().map(|index| in_time[*index]).collect(),
            times: TimeSelection::Within(TimeRange {
                start_s: firsts[0].0,
                ..TimeRange::of_grid(&last, 0..0)
            }),
            ..request(0..8, usize::MAX)
        };
        let mut traversal = observation.bind_read(&sliced).expect("bind");
        let mut updater = session.updater(&sliced, &[0]).expect("updater");
        let mut buffers = BatchBuffers::default();
        let mut written = Vec::new();
        while let Some(event) = traversal.next(&mut buffers).expect("read") {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            written.push(batch.partition);
            let set = Array3::from_elem(batch.shape(), true);
            updater
                .write(
                    &batch,
                    &[PatchValues::Flags {
                        set: Some(set.view()),
                        clear: None,
                    }],
                )
                .expect("the empty partition is passed");
        }
        updater.finish().expect("finish");
        drop(traversal);
        drop(session);
        written.dedup();
        assert_eq!(written, [in_time[0], in_time[1]], "{order:?}");
        let reopened = casa_ms::observation_io::open(&path, &options).expect("reopen");
        for (index, id) in in_time.iter().enumerate() {
            let samples = read(
                &*reopened,
                &AccessRequest {
                    partitions: vec![*id],
                    ..request(0..8, usize::MAX)
                },
            );
            let flagged = samples
                .values()
                .all(|sample| sample.flags.iter().all(|flag| *flag));
            assert_eq!(flagged, index < 2, "{order:?}: partition {index} in time");
        }
    }
}

/// An updater bound to a slice writes only the slice's times: a batch of
/// the whole traversal from before the slice is refused, on both stores.
#[test]
fn an_updater_of_a_slice_refuses_times_outside_it() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let target = UpdateTarget::new("base", WritableRole::Flags);
        let session = open_update(&path, &OpenOptions::default(), &[target]).expect("session");
        let whole = per_times(session.observation(), request(0..8, 0), 2);
        let slice = sliced(session.observation(), &whole, 6..10);
        let mut updater = session.updater(&slice, &[0]).expect("updater");
        let mut traversal = session.observation().bind_read(&whole).expect("bind");
        let mut buffers = BatchBuffers::default();
        let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
            panic!("a batch");
        };
        let set = Array3::from_elem(batch.shape(), true);
        let written = updater.write(
            &batch,
            &[PatchValues::Flags {
                set: Some(set.view()),
                clear: None,
            }],
        );
        assert!(
            matches!(written, Err(ObservationError::InvalidRequest { .. })),
            "{name}: {written:?}"
        );
    }
}

/// An updater takes its partitions in any order: the MSv2 fixture split
/// into three scans' partitions, written last partition first, holds every
/// flag set.
#[test]
fn an_updater_takes_its_partitions_in_any_order() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    three_scans(&path);
    let target = UpdateTarget::new("base", WritableRole::Flags);
    let session = open_update(&path, &by_scan(), &[target]).expect("session");
    let request = AccessRequest {
        partitions: session.observation().catalogue().partition_ids(),
        ..request(0..8, usize::MAX)
    };
    let mut traversal = session.observation().bind_read(&request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut delivered = Vec::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            delivered.push((batch.address(), Array3::from_elem(batch.shape(), true)));
        }
    }
    drop(traversal);
    assert_eq!(delivered.len(), 3, "one batch a partition");
    let mut updater = session.updater(&request, &[0]).expect("updater");
    for (address, set) in delivered.iter().rev() {
        updater
            .write_at(
                address,
                &[PatchValues::Flags {
                    set: Some(set.view()),
                    clear: None,
                }],
            )
            .expect("a later partition first");
    }
    updater.finish().expect("finish");
    drop(session);
    let reopened = casa_ms::observation_io::open(&path, &by_scan()).expect("reopen");
    let flagged = read(&*reopened, &request)
        .values()
        .all(|sample| sample.flags.iter().all(|flag| *flag));
    assert!(flagged, "every partition written");
}
