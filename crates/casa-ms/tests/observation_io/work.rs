// SPDX-License-Identifier: LGPL-3.0-or-later
//! Group completion when a grouped batch is split among work units: the
//! owner's case of threads (or thread groups) that each take a channel
//! window of every batch and retire independently.

use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{
    AccessRequest, BatchBuffers, BufferPool, GroupCompletion, GroupKey, Grouping, OwnedEvent,
    ReadEvent, VisibilityBatch, WorkToken,
};

use super::{fixture, per_times, request};

/// The sum of the weights `batch`'s validity rule admits.
fn admitted_weight(batch: &VisibilityBatch<'_>) -> f64 {
    let (Some(weights), validity) = (batch.group.weights, batch.validity()) else {
        return 0.0;
    };
    let [samples, channels, correlations] = batch.shape();
    let mut total = 0.0;
    for sample in 0..samples {
        for channel in 0..channels {
            for correlation in 0..correlations {
                if !validity.excluded(sample, channel, correlation) {
                    total += f64::from(weights[[sample, channel, correlation]]);
                }
            }
        }
    }
    total
}

/// Each grouped batch is split into two channel windows, each window of
/// each fragment a unit of work on its own thread, the low-channel units
/// held back, the batches drawn from a pool of two buffers. The windows
/// carry no fragments, so only the delivered batch can complete a group's
/// input, once; [`GroupCompletion`] then finalizes a group only after both
/// windows' work on every one of its fragments has handed its token back.
/// The finalized per-group sums equal a sequential traversal's, and every
/// buffer returns to the pool.
#[test]
fn split_channel_windows_complete_each_group_once() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let observation = open(fixture(root.path(), name), &OpenOptions::default()).expect("open");
        let grouped = AccessRequest {
            grouping: Grouping::TimeBins {
                width_s: 60.0,
                combine_fields: false,
                combine_scans: false,
            },
            ..per_times(&*observation, request(0..8, 0), 3)
        };
        // Sequentially: each group's admitted weight.
        let mut expected = BTreeMap::<GroupKey, f64>::new();
        let mut traversal = observation.bind_read(&grouped).expect("bind");
        let mut buffers = BatchBuffers::default();
        while let Some(event) = traversal.next(&mut buffers).expect("read") {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            for fragment in batch.fragments {
                *expected.entry(fragment.key).or_default() +=
                    admitted_weight(&batch.sample_window(fragment.samples.clone()).expect("w"));
            }
        }
        // Concurrently, in two channel windows, from a pool of two buffers:
        // the producer waits while both batches are still in use.
        let mut traversal = observation.bind_read(&grouped).expect("bind");
        let pool = BufferPool::new(2, traversal.estimate.peak_buffer_bytes());
        let mut completion = GroupCompletion::default();
        let mut partial = BTreeMap::<GroupKey, f64>::new();
        let mut finalized = BTreeMap::<GroupKey, f64>::new();
        let mut closed_with_pending = 0;
        let (done_tx, done_rx) = mpsc::channel::<(WorkToken, f64)>();
        std::thread::scope(|scope| {
            let mut join = |token: WorkToken,
                            sum: f64,
                            completion: &mut GroupCompletion,
                            partial: &mut BTreeMap<GroupKey, f64>| {
                let key = token.key();
                *partial.entry(key).or_default() += sum;
                if completion.finish(token).expect("no work abandoned") {
                    let total = partial.remove(&key).expect("partial");
                    assert!(finalized.insert(key, total).is_none(), "finalized once");
                }
            };
            loop {
                let filled = match traversal.next_owned(&pool).expect("read") {
                    OwnedEvent::Batch(filled) => Arc::new(filled),
                    OwnedEvent::GroupEnd(key) => {
                        let _ = completion.close(key).expect("one closure");
                        continue;
                    }
                    OwnedEvent::End => break,
                };
                for fragment in filled.batch().fragments.to_vec() {
                    for (unit, channels) in [(0, 0..4), (1, 4..8)] {
                        let token = completion.begin(fragment.key).expect("open group");
                        let (filled, done_tx) = (filled.clone(), done_tx.clone());
                        let samples = fragment.samples.clone();
                        scope.spawn(move || {
                            if unit == 0 {
                                std::thread::sleep(Duration::from_millis(20));
                            }
                            let batch = filled.batch();
                            let window = batch.channel_window(channels).expect("window");
                            assert!(window.fragments.is_empty(), "a window closes nothing");
                            let piece = window.sample_window(samples).expect("piece");
                            let _ = done_tx.send((token, admitted_weight(&piece)));
                        });
                    }
                    if fragment.position.closes() {
                        if completion.pending(fragment.key) > 0 {
                            closed_with_pending += 1;
                        }
                        assert!(!completion.close(fragment.key).expect("one closure"));
                    }
                }
                drop(filled);
                while let Ok((token, sum)) = done_rx.try_recv() {
                    join(token, sum, &mut completion, &mut partial);
                }
            }
            drop(done_tx);
            for (token, sum) in done_rx {
                join(token, sum, &mut completion, &mut partial);
            }
        });
        assert_eq!(pool.free(), 2, "{name}: every buffer came back");
        assert!(
            completion.is_idle().expect("no work abandoned"),
            "{name}: every group finalized"
        );
        assert!(
            closed_with_pending >= 1,
            "{name}: a closure waited for work"
        );
        assert_eq!(finalized.len(), expected.len(), "{name}");
        for (key, total) in &expected {
            let concurrent = finalized[key];
            assert!(
                (concurrent - total).abs() <= 1e-9 * total.abs(),
                "{name} {key:?}: {concurrent} vs {total}"
            );
        }
    }
}
