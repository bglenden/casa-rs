// SPDX-License-Identifier: LGPL-3.0-or-later

//! Allocation laws of the numeric selectors imaging resolves at compile:
//! a selector's heap depends on its terms and on the fields that exist,
//! never on the width of a numeric range (`0~2147483647`).
//!
//! The binary holds one test, so no other test allocates while it
//! measures.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::{
    FieldSelectorRow, MeasurementSet, SyntheticSpectralSetup, generate_synthetic_observation_ms,
    parse_numeric_row_id_ranges, resolve_field_selector_rows, tutorial_vla_a_antennas,
};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

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
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new > old {
                grow(new - old);
            } else {
                LIVE.fetch_sub(old - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

/// The peak and retained heap of `work`, over the heap live before it.
fn measure<T>(work: impl FnOnce() -> T) -> (T, u64, u64) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let value = work();
    let peak = PEAK.load(Ordering::Relaxed) - before;
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    (value, peak, held)
}

/// A one-field MeasurementSet.
fn track(root: &Path) -> PathBuf {
    let measurement_set = root.join("selector-width.ms");
    let mut request = casa_ms::SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas().into_iter().take(2).collect(),
    );
    request.duration_seconds = 1.0;
    request.integration_seconds = 1.0;
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "44e9".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 1.0e6,
        channel_count: 1,
    }];
    generate_synthetic_observation_ms(&request).expect("synthesise the selector MS");
    measurement_set
}

#[test]
fn numeric_selector_width_does_not_grow_the_heap() {
    // Scan and observation selectors: intervals, held in space proportional
    // to their terms.
    let _ = parse_numeric_row_id_ranges("0~1", "scan").expect("warm");
    for (narrow, wide) in [
        ("0~1", "0~2147483647"),
        ("1~2,5~6", "1~2,5~2147483647"),
        (
            "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38",
            "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38~2147483647",
        ),
    ] {
        let mut measured = [(0, 0); 2];
        for (index, text) in [narrow, wide].into_iter().enumerate() {
            let (ranges, peak, held) = measure(|| parse_numeric_row_id_ranges(text, "scan"));
            let ranges = ranges.expect("a valid scan selector");
            assert_eq!(
                held,
                (ranges.capacity() * size_of::<(i32, i32)>()) as u64,
                "{text}: only the intervals are retained"
            );
            eprintln!("scan {text}: peak {peak} B, held {held} B");
            measured[index] = (peak, held);
        }
        assert_eq!(measured[0], measured[1], "range width has no heap cost");
    }

    // FIELD ranges: the fields that exist bound enumeration and the
    // retained IDs, over a MeasurementSet and over a catalogue's rows alike.
    let root = tempfile::tempdir().expect("test root");
    let ms = MeasurementSet::open(track(root.path())).expect("open");
    assert_eq!(ms.field().unwrap().row_count(), 1);
    ms.resolve_field_selector("0~1").expect("warm metadata");
    let rows = [FieldSelectorRow {
        name: "only",
        flagged: false,
    }];
    for (narrow, wide) in [
        ("0~1", "0~2147483647"),
        ("0~1,0~1,0", "0~2147483647,0~2147483647,0"),
    ] {
        let mut measured = [(0, 0, 0, 0, 0); 2];
        for (index, selector) in [narrow, wide].into_iter().enumerate() {
            let (ids, ms_peak, ms_held) = measure(|| ms.resolve_field_selector(selector));
            let ids = ids.expect("existing IDs");
            assert_eq!(ids, vec![0]);
            let (catalogue_ids, rows_peak, rows_held) =
                measure(|| resolve_field_selector_rows(&rows, selector));
            assert_eq!(catalogue_ids.expect("existing IDs"), vec![0]);
            eprintln!(
                "field {selector}: MS peak {ms_peak} B held {ms_held} B; rows peak {rows_peak} B \
                 held {rows_held} B"
            );
            measured[index] = (ms_peak, ms_held, ids.capacity(), rows_peak, rows_held);
        }
        assert_eq!(measured[0], measured[1], "range width has no heap cost");
    }
}
