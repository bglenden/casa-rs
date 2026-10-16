// SPDX-License-Identifier: LGPL-3.0-or-later
//! An in-place `MODEL_DATA` write holds at most what the writer says it does
//! ([`SelectedVisibilityWrite::retained_bytes`]), so the pass that writes can
//! admit it, and it never holds MAIN's rows (ADR-0008).
//!
//! This binary's global allocator counts the live heap; the law compares a
//! write's peak beyond the heap live before it with the writer's bytes. The
//! tiles a write reads go to the process-wide table-read cache, which is
//! charged on its own, so the law gives that cache no budget.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_imaging_model::MsColumnKind;
use casa_ms::{
    MeasurementSet, SelectedVisibilityWrite, SelectedVisibilityWriteTargets,
    SyntheticObservationRequest, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_types::ArrayValue;
use num_complex::Complex32;

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

/// A VLA-A track of 60 two-second integrations in `channels` channels,
/// without model data and without `MODEL_DATA`: 21,060 rows, more than a
/// write batch.
fn many_rows(root: &Path, channels: usize) -> PathBuf {
    let measurement_set = root.join(format!("rows-{channels}.ms"));
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 120.0;
    request.integration_seconds = 2.0;
    request.spectral_windows[0].channel_count = channels;
    request.predict_model = false;
    generate_synthetic_observation_ms(&request).expect("synthesise the write MS");
    measurement_set
}

/// Write `MODEL_DATA` for every channel and correlation of every row, as the
/// final imaging pass does, and complete the write.
fn write_every_cell(path: &Path, rows: usize, cell: [usize; 2]) {
    let [correlations, channels] = cell;
    let mut writer =
        SelectedVisibilityWrite::begin(path, SelectedVisibilityWriteTargets::new(true, false))
            .expect("begin the write");
    for row in 0..rows {
        for channel in 0..channels {
            for correlation in 0..correlations {
                let value =
                    Complex32::new(row as f32, (channel * correlations + correlation) as f32);
                writer
                    .write(
                        MsColumnKind::ModelData,
                        row as u64,
                        channel as u32,
                        correlation as u32,
                        value,
                    )
                    .expect("write a cell");
            }
        }
    }
    writer.complete().expect("complete the write");
}

/// The `[correlations, channels]` shape of row 0's `column` cell.
fn cell_shape(path: &Path, column: &str) -> [usize; 2] {
    let ms = MeasurementSet::open(path).expect("open");
    let cell = ms
        .main_table()
        .column_accessor(column)
        .expect("the column")
        .array_cells_owned_uncached(&[0])
        .expect("read a cell")[0]
        .clone()
        .expect("the cell is defined");
    [cell.shape()[0], cell.shape()[1]]
}

#[test]
fn a_model_data_write_holds_at_most_its_bytes() {
    casa_tables::set_table_cache_budget_bytes(0);
    let root = tempfile::tempdir().expect("test root");
    let mut breaches = Vec::new();
    // A narrow and a wide cell: the queued cells against the per-row cost.
    for channels in [1, 64] {
        let path = many_rows(root.path(), channels);
        let rows = MeasurementSet::open(&path).expect("open").row_count();
        let cell = cell_shape(&path, "DATA");
        let bound = SelectedVisibilityWrite::retained_bytes(
            &path,
            SelectedVisibilityWriteTargets::new(true, false),
        )
        .expect("the writer's bytes");
        // The first write creates MODEL_DATA, zero-filled; the second
        // rewrites it.
        for write in ["creating", "rewriting"] {
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            write_every_cell(&path, rows, cell);
            let peak = PEAK.load(Ordering::Relaxed) - baseline;
            eprintln!("{write} {rows} rows of {cell:?} cells: peak {peak} bytes, bound {bound}");
            if peak > bound {
                breaches.push(format!(
                    "a {write} write of {rows} rows of {cell:?} cells held {peak} bytes, \
                     more than its {bound}"
                ));
            }
        }
        // The write reached the disk: the last row holds what was written.
        let ms = MeasurementSet::open(&path).expect("reopen");
        let written = ms
            .main_table()
            .column_accessor("MODEL_DATA")
            .expect("MODEL_DATA")
            .array_cells_owned_uncached(&[rows - 1])
            .expect("read the last cell")[0]
            .clone();
        let Some(ArrayValue::Complex32(written)) = written else {
            panic!("MODEL_DATA holds Complex32 cells");
        };
        assert_eq!(written.shape(), &cell);
        assert_eq!(written[[0, 0]], Complex32::new((rows - 1) as f32, 0.0));
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
