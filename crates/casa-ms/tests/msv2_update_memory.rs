// SPDX-License-Identifier: LGPL-3.0-or-later
//! An MSv2 updater holds at most what its session says it does
//! ([`UpdateSession::write_bytes`]), so a pass that writes back can admit
//! it before it writes: creating and seeding `MODEL_DATA`, writing whole
//! cells, and merging masked writes into cells read back, one target and
//! two.
//!
//! This binary's global allocator counts the live heap; each law compares
//! with the bound the most the heap rises, after the updater is bound,
//! above the least it has held since (the values it is given live before).
//! Taking casacore's write lock drops the read caches of the session's
//! observation, which would otherwise hide what the updater allocates in
//! their place. One test runs the laws one after another: the allocator
//! counts every thread's heap.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::observation_io::{OpenOptions, UpdateSession, open_update};
use casa_ms::{
    SyntheticField, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_observation::{
    AccessRequest, BatchAddress, BatchBuffers, CorrelationRequest, FieldSelection, Grouping,
    PartitionId, PatchValues, ReadEvent, Roles, TimeSelection, UpdateTarget, WeightRequest,
    WritableRole,
};
use ndarray::Array3;
use num_complex::Complex32;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
/// The least live heap since the mark.
static FLOOR: AtomicU64 = AtomicU64::new(0);
/// The most the live heap has risen above [`FLOOR`] since the mark.
static RISE: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    RISE.fetch_max(
        live.saturating_sub(FLOOR.load(Ordering::Relaxed)),
        Ordering::Relaxed,
    );
}

fn shrink(bytes: u64) {
    let live = LIVE.fetch_sub(bytes, Ordering::Relaxed) - bytes;
    FLOOR.fetch_min(live, Ordering::Relaxed);
}

/// Start measuring from the live heap.
fn mark() {
    FLOOR.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    RISE.store(0, Ordering::Relaxed);
}

// SAFETY: forwards to `System` with the caller's arguments and adds atomic
// bookkeeping only.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        shrink(layout.size() as u64);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            grow(new_size as u64);
            shrink(layout.size() as u64);
        }
        moved
    }
}

/// Channels of the fixture's one window.
const CHANNELS: usize = 32;

/// A VLA-A track of `integrations` ten-second integrations of
/// [`CHANNELS`] channels: 351 baseline rows per integration.
fn track(root: &Path, integrations: usize) -> PathBuf {
    std::fs::create_dir_all(root).expect("root");
    let measurement_set = root.join("track.ms");
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 10.0 * integrations as f64;
    request.integration_seconds = 10.0;
    request.predict_model = false;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "Qband".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 2.0e6,
        channel_count: CHANNELS,
    }];
    request.fields = vec![SyntheticField {
        name: request.field_name.clone(),
        phase_center_rad: request.phase_center_rad,
    }];
    generate_synthetic_observation_ms(&request).expect("synthesise the MS");
    measurement_set
}

/// Every role of partition 0's `channels`, `times` whole times a batch.
fn request(
    session: &UpdateSession,
    channels: std::ops::Range<usize>,
    times: usize,
) -> AccessRequest {
    let probe = AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels,
        times: TimeSelection::All,
        fields: FieldSelection::All,
        weights: WeightRequest::Stored,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: usize::MAX,
    };
    let held = session
        .observation()
        .estimate(&probe)
        .expect("estimate")
        .partitions[0]
        .held_bytes_per_time();
    AccessRequest {
        batch_bytes: held * times,
        ..probe
    }
}

/// One batch's address and the values written to it.
struct Patch {
    address: BatchAddress,
    values: Array3<Complex32>,
    written: Option<Array3<bool>>,
}

/// Every batch of `request` read through `session`, with values to write
/// to it and, when `masked`, a mask of every other sample's first half of
/// the channels.
fn patches(session: &UpdateSession, request: &AccessRequest, masked: bool) -> Vec<Patch> {
    let mut traversal = session.observation().bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut patches = Vec::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let values = batch
            .group
            .values
            .expect("values")
            .mapv(|value| value * 2.0);
        let channels = batch.channel_count();
        let written = masked.then(|| {
            Array3::from_shape_fn(batch.shape(), |(s, c, _)| {
                s.is_multiple_of(2) && c < channels / 2
            })
        });
        patches.push(Patch {
            address: batch.address(),
            values,
            written,
        });
    }
    patches
}

/// Bind an updater of `targets` over `request` and write `patches` to each
/// target: the most the heap rose meanwhile, beside the session's bound.
fn measure(
    session: &UpdateSession,
    request: &AccessRequest,
    targets: &[usize],
    patches: &[Patch],
) -> (u64, u64) {
    let bound = session.write_bytes(request, targets).expect("bound") as u64;
    mark();
    let mut updater = session.updater(request, targets).expect("updater");
    for patch in patches {
        let values: Vec<PatchValues<'_>> = targets
            .iter()
            .map(|_| PatchValues::Visibilities {
                values: patch.values.view(),
                written: patch.written.as_ref().map(|mask| mask.view()),
            })
            .collect();
        updater.write_at(&patch.address, &values).expect("write");
    }
    updater.finish().expect("finish");
    (RISE.load(Ordering::Relaxed), bound)
}

/// The laws, one after another.
#[test]
fn an_msv2_updater_holds_at_most_its_bound() {
    let root = tempfile::tempdir().expect("test root");
    let path = track(&root.path().join("track"), 30);
    let model = UpdateTarget {
        create: true,
        ..UpdateTarget::new("model", WritableRole::Visibilities)
    };
    let corrected = UpdateTarget {
        create: true,
        ..UpdateTarget::new("corrected", WritableRole::Visibilities)
    };
    let mut breaches = Vec::new();
    let mut check = |label: &str, (peak, bound): (u64, u64)| {
        eprintln!("{label}: rose {peak} bytes, bound {bound}");
        if peak > bound {
            breaches.push(format!(
                "{label}: {peak} bytes held, {} beyond the bound",
                peak - bound
            ));
        }
    };
    {
        // MODEL_DATA created and seeded, then written whole, four times a
        // batch.
        let session = open_update(&path, &OpenOptions::default(), std::slice::from_ref(&model))
            .expect("session");
        let whole = request(&session, 0..CHANNELS, 4);
        let patches = patches(&session, &whole, false);
        check(
            "created and whole",
            measure(&session, &whole, &[0], &patches),
        );
    }
    {
        // The existing MODEL_DATA, masked: half the samples and channels,
        // each cell read and merged.
        let session = open_update(&path, &OpenOptions::default(), std::slice::from_ref(&model))
            .expect("session");
        let whole = request(&session, 0..CHANNELS, 4);
        let patches = patches(&session, &whole, true);
        check(
            "existing and masked",
            measure(&session, &whole, &[0], &patches),
        );
    }
    {
        // MODEL_DATA and a created CORRECTED_DATA together, a channel window
        // of every batch, ten times a batch.
        let session =
            open_update(&path, &OpenOptions::default(), &[model, corrected]).expect("session");
        let window = request(&session, 4..20, 10);
        let patches = patches(&session, &window, false);
        check(
            "two targets, a window",
            measure(&session, &window, &[0, 1], &patches),
        );
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
