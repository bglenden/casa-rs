// SPDX-License-Identifier: LGPL-3.0-or-later
//! A walk of the selected MAIN rows holds at most what the MeasurementSet
//! says it does ([`MeasurementSet::row_walk_bytes`]), so a caller can admit
//! the walk before it reads.
//!
//! This binary's global allocator counts the live heap; the law compares a
//! walk's peak beyond the heap live before it with the walk's bytes, under
//! budgets that give one block and many. The tiles the walk reads go to the
//! process-wide table-read cache, which is charged on its own, so the cache
//! is filled before the walks are measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::{
    MeasurementSet, MsReadPlan, MsSelectionIoBudget, SelectedObservationRow,
    SyntheticObservationRequest, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
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
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if moved != pointer {
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
            }
            if new >= old {
                grow(new - old);
            } else {
                LIVE.fetch_sub(old - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

/// A VLA-A track of `integrations` two-second integrations in one channel,
/// without model data: 351 baseline rows per integration.
fn many_rows(root: &Path, integrations: usize) -> PathBuf {
    let measurement_set = root.join("rows.ms");
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 2.0 * integrations as f64;
    request.integration_seconds = 2.0;
    request.predict_model = false;
    generate_synthetic_observation_ms(&request).expect("synthesise the walk MS");
    measurement_set
}

#[test]
fn a_row_walk_holds_at_most_its_bytes() {
    let root = tempfile::tempdir().expect("test root");
    let ms = MeasurementSet::open(many_rows(root.path(), 120)).expect("open");
    let selection = ms
        .selected_observation_row_selection(&[0], None, None, None)
        .expect("selection");
    let rows = ms.row_count();
    // The tiles a walk reads go to the process-wide table-read cache, which
    // is charged on its own; one walk first fills it.
    let warm = MsSelectionIoBudget {
        available_bytes: rows * 4096,
        maximum_live_blocks: 2,
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    };
    ms.visit_selected_observation_rows(&selection, warm, |_| {})
        .expect("warm the cache");
    let mut breaches = Vec::new();
    for available in [rows * 4096, 4 << 20, 512 << 10, 64 << 10] {
        let io = MsSelectionIoBudget {
            available_bytes: available,
            maximum_live_blocks: 2,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        };
        let plan = MsReadPlan::new(rows, io).expect("plan");
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let mut visited = 0_usize;
        ms.visit_selected_observation_rows(&selection, io, |_| visited += 1)
            .expect("walk");
        let peak = PEAK.load(Ordering::Relaxed) - baseline;
        let bound = ms.row_walk_bytes(io).expect("walk bytes") as u64;
        eprintln!(
            "{rows} rows, {} per block: peak {peak} bytes ({:.1} per block row), bound {bound}",
            plan.rows_per_block,
            peak as f64 / plan.rows_per_block as f64
        );
        assert_eq!(visited, rows);
        if peak > bound {
            breaches.push(format!(
                "a walk of {} rows per block held {peak} bytes, more than its {bound}",
                plan.rows_per_block
            ));
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
