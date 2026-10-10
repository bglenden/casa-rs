// SPDX-License-Identifier: LGPL-3.0-or-later
//! The heap a typed selected read holds stays within the footprint
//! casa-tables publishes for its column's data manager.
//!
//! A counting global allocator records the peak live heap above the level
//! before one warm read. This binary holds a single test so that no other
//! test allocates concurrently.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, SelectedArray2DCellsMut, SelectedReadFootprint,
    Table, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

struct CountingAllocator;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                grow(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Peak live heap while `read` runs above what it leaves allocated, which is
/// the heap it held only temporarily. Data a read leaves behind (the
/// process-wide tile cache) is budgeted separately.
fn peak_temporary_heap(read: impl FnOnce()) -> usize {
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);
    read();
    let end = LIVE.load(Ordering::Relaxed);
    PEAK.load(Ordering::Relaxed) - start.max(end)
}

const ROWS: usize = 40;
const CORRELATIONS: usize = 2;
const CHANNELS: usize = 16_384;

fn wide_table() -> Table {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "DATA",
        PrimitiveType::Float32,
        vec![CORRELATIONS, CHANNELS],
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row in 0..ROWS {
        let values = (0..CORRELATIONS * CHANNELS)
            .map(|index| (row * 1_000_000 + index) as f32)
            .collect();
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[CORRELATIONS, CHANNELS]).f(), values)
                        .expect("cell"),
                )),
            )]))
            .expect("row");
    }
    table
}

/// Peak temporary heap of one 32-row, one-channel read of the table saved at
/// `path`, and that column's published footprint.
fn measure_narrow_read(path: &std::path::Path) -> (usize, SelectedReadFootprint) {
    let reopened = Table::open(TableOptions::new(path)).expect("open");
    let footprint = reopened.selected_channel_read_footprint("DATA");
    let column = reopened.column_accessor("DATA").expect("DATA");
    let selected_rows = (0..SELECTED_ROWS).collect::<Vec<_>>();
    let mut values = Vec::with_capacity(SELECTED_ROWS * CORRELATIONS);
    // Warm the reader's metadata before measuring.
    column
        .fill_array_cells_2d_channel_range_typed_uncached(
            &[ROWS - 1],
            7,
            1,
            SelectedArray2DCellsMut::RowChannelFloat32(&mut values),
        )
        .expect("warm read");
    let peak = peak_temporary_heap(|| {
        column
            .fill_array_cells_2d_channel_range_typed_uncached(
                &selected_rows,
                7,
                1,
                SelectedArray2DCellsMut::RowChannelFloat32(&mut values),
            )
            .expect("selected read")
            .expect("defined cells");
    });
    assert_eq!(values.len(), SELECTED_ROWS * CORRELATIONS);
    (peak, footprint)
}

const SELECTED_ROWS: usize = 32;

/// A narrow channel selection of a wide column held by a manager read cell
/// by cell holds every selected row's whole cell, and no more than the
/// footprint casa-tables publishes for it. `TiledShapeStMan` streams the
/// selection: with `[2, 16, 1]` tiles it holds far less than one cell.
#[test]
fn narrow_selections_of_wide_cells_stay_within_the_published_footprint() {
    let mut table = wide_table();
    let stored_cell_bytes = CORRELATIONS * CHANNELS * size_of::<f32>();
    for manager in [
        DataManagerKind::StandardStMan,
        DataManagerKind::StManAipsIO,
        DataManagerKind::TiledColumnStMan,
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("wide.tbl");
        table
            .save(TableOptions::new(&path).with_data_manager(manager))
            .expect("save");
        let (peak, footprint) = measure_narrow_read(&path);
        assert_eq!(footprint, SelectedReadFootprint::WholeCells, "{manager:?}");
        let charged = footprint
            .staging_bytes_per_row(stored_cell_bytes)
            .and_then(|per_row| per_row.checked_mul(SELECTED_ROWS))
            .and_then(|rows| rows.checked_add(footprint.staging_fixed_bytes(stored_cell_bytes)?))
            .expect("footprint");
        assert!(
            (SELECTED_ROWS * stored_cell_bytes..=charged).contains(&peak),
            "{manager:?}: a {SELECTED_ROWS}-row read held {peak} bytes; its footprint is {charged}"
        );
    }

    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("wide.tbl");
    table
        .prepare_write()
        .save_with_bindings(
            TableOptions::new(&path),
            &std::collections::HashMap::from([(
                "DATA".to_string(),
                ColumnBinding {
                    data_manager: DataManagerKind::TiledShapeStMan,
                    tile_shape: Some(vec![CORRELATIONS, 16, 1]),
                },
            )]),
        )
        .expect("save tiled-shape table");
    let (peak, footprint) = measure_narrow_read(&path);
    assert_eq!(footprint, SelectedReadFootprint::Streamed);
    assert!(
        peak < stored_cell_bytes,
        "TiledShapeStMan held {peak} bytes for a one-channel selection"
    );
}
