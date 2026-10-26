// SPDX-License-Identifier: LGPL-3.0-or-later
//! An in-place `MODEL_DATA` or `CORRECTED_DATA` write holds at most what the
//! writer says it does ([`SelectedVisibilityWrite::retained_bytes`]), so the
//! pass that writes can admit it, and it never holds MAIN's rows (ADR-0008).
//!
//! The laws cover every destination layout the writer accepts: a
//! `MODEL_DATA` it creates in a clone of DATA's manager, and existing
//! destinations in `TiledShapeStMan` or `TiledColumnStMan`. A write of one
//! cell must not grow with MAIN's row count, neither in the cell's read nor
//! in the save at completion. A destination in any other manager is refused
//! before anything changes.
//!
//! This binary's global allocator counts the live heap; the laws compare a
//! write's peak beyond the heap live before it with the writer's bytes. The
//! tiles a write reads go to the process-wide table-read cache, which is
//! charged on its own, so the laws give that cache no budget. The counters
//! are the process's, so each law runs alone in a process of its own
//! ([`alone`]).

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_model::MsColumnKind;
use casa_ms::{
    MeasurementSet, ObservationOwnerError, SelectedVisibilityWrite, SelectedVisibilityWriteTargets,
    SyntheticObservationRequest, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, LockMode, LockOptions, LockType, Table,
    TableOptions,
};
use casa_types::{ArrayValue, PrimitiveType, Value};
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

/// The environment variable that names the law a child process of
/// [`alone`] runs.
const LAW_VAR: &str = "CASA_RS_COUNTING_LAW";

/// The test a law's process runs before the law ([`alone`]).
const SETTLE: &str = "a_counting_law_process_settles";

/// Whether [`SETTLE`] ran in this process.
static SETTLED: AtomicBool = AtomicBool::new(false);

/// Runs first in a law's process ([`alone`]): libtest's main thread sets up
/// its wait for results while the first test it starts runs, so the law,
/// which runs second, measures none of it. The pause makes sure the main
/// thread is waiting before this test reports.
#[test]
#[ignore = "alone runs it before a law, in the law's process"]
fn a_counting_law_process_settles() {
    std::thread::sleep(std::time::Duration::from_millis(10));
    SETTLED.store(true, Ordering::Relaxed);
}

/// Whether this process runs the law `name` alone; if it does not, run the
/// law alone and assert that it passed.
///
/// The counters see every allocation in the process, and libtest runs a
/// binary's tests in one process: under `cargo test` its main thread starts
/// tests and reports their results, and the binary's other tests run, while
/// a law measures. So each law begins `if !alone("law") { return; }`, with
/// its own name. In the harness's process it runs [`SETTLE`], then the law
/// (libtest runs tests in name order), in a child process of this binary
/// with one test thread, so that the harness only waits while the law runs;
/// in the child it returns true.
fn alone(name: &str) -> bool {
    // libtest names each test's thread after the test.
    assert_eq!(
        std::thread::current().name(),
        Some(name),
        "a law runs alone under its own name"
    );
    let ran = format!("{name} ran alone");
    if std::env::var_os(LAW_VAR).is_some_and(|law| law == name) {
        assert!(
            SETTLED.load(Ordering::Relaxed),
            "{SETTLE} runs before {name}"
        );
        println!("{ran}");
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            SETTLE,
            name,
            "--exact",
            "--include-ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(LAW_VAR, name)
        .output()
        .expect("the law's process runs");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{name} alone: {}\n{stdout}",
        output.status
    );
    assert!(stdout.contains(&ran), "{name} did not run alone:\n{stdout}");
    false
}

/// The peak heap `write` holds beyond what was live before it.
fn peak_of(write: impl FnOnce()) -> u64 {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    write();
    PEAK.load(Ordering::Relaxed) - baseline
}

/// A VLA-A track of two-second integrations for `seconds` in `channels`
/// channels, without model data and without `MODEL_DATA`: 351 rows an
/// integration, so 120 s gives 21,060 rows, more than a write batch.
fn many_rows(root: &Path, label: &str, channels: usize, seconds: f64) -> PathBuf {
    let measurement_set = root.join(format!("{label}-{channels}-{seconds}.ms"));
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = seconds;
    request.integration_seconds = 2.0;
    request.spectral_windows[0].channel_count = channels;
    request.predict_model = false;
    generate_synthetic_observation_ms(&request).expect("synthesise the write MS");
    measurement_set
}

/// Rewrite MAIN with every column in `StandardStMan` except `destinations`,
/// added zero-filled in `manager`: the layouts other tools write.
fn relayout(path: &Path, destinations: &[&str], manager: DataManagerKind) {
    let [correlations, channels] = cell_shape(path, "DATA");
    let mut main = Table::open(TableOptions::new(path)).expect("open MAIN");
    let zeros = Value::Array(ArrayValue::Complex32(ndarray::ArrayD::from_elem(
        ndarray::IxDyn(&[correlations, channels]),
        Complex32::new(0.0, 0.0),
    )));
    let tiled = matches!(
        manager,
        DataManagerKind::TiledShapeStMan | DataManagerKind::TiledColumnStMan
    );
    let mut bindings = HashMap::new();
    for &column in destinations {
        let schema = if manager == DataManagerKind::TiledColumnStMan {
            ColumnSchema::array_fixed(
                column,
                PrimitiveType::Complex32,
                vec![correlations, channels],
            )
        } else {
            ColumnSchema::array_variable(column, PrimitiveType::Complex32, Some(2))
        };
        main.add_column(schema, Some(zeros.clone()))
            .expect("add the destination");
        bindings.insert(
            column.to_string(),
            ColumnBinding {
                data_manager: manager,
                tile_shape: tiled.then(|| vec![correlations, channels, 512]),
            },
        );
    }
    // Every row is in memory before the files it came from are replaced.
    main.rows().expect("load MAIN");
    main.prepare_write()
        .save_with_bindings(
            TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("rewrite MAIN");
}

/// Write the destinations of `targets` for every channel and correlation of
/// `rows` with `[correlations, channels]` cells, as the final imaging pass
/// does, and complete the write.
fn write_cells(
    path: &Path,
    targets: SelectedVisibilityWriteTargets,
    rows: Range<usize>,
    cell: [usize; 2],
) {
    let [correlations, channels] = cell;
    let columns = [
        (MsColumnKind::ModelData, targets.model_data()),
        (MsColumnKind::CorrectedData, targets.corrected_data()),
    ];
    let mut writer = SelectedVisibilityWrite::begin(path, targets).expect("begin the write");
    for row in rows {
        for channel in 0..channels {
            for correlation in 0..correlations {
                let value =
                    Complex32::new(row as f32, (channel * correlations + correlation) as f32);
                for (column, written) in columns {
                    if written {
                        writer
                            .write(
                                column,
                                row as u64,
                                channel as u32,
                                correlation as u32,
                                value,
                            )
                            .expect("write a cell");
                    }
                }
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

/// `column`'s cell at `row`, read from the disk.
fn stored_cell(path: &Path, column: &str, row: usize) -> ndarray::ArrayD<Complex32> {
    let ms = MeasurementSet::open(path).expect("reopen");
    let written = ms
        .main_table()
        .column_accessor(column)
        .expect("the destination")
        .array_cells_owned_uncached(&[row])
        .expect("read a cell")[0]
        .clone();
    let Some(ArrayValue::Complex32(written)) = written else {
        panic!("{column} holds Complex32 cells");
    };
    written
}

/// The storage manager `column` is stored in.
fn manager_of(path: &Path, column: &str) -> String {
    let ms = MeasurementSet::open(path).expect("open");
    ms.main_table()
        .data_manager_info()
        .iter()
        .find(|manager| manager.columns.iter().any(|name| name == column))
        .map(|manager| manager.dm_type.clone())
        .expect("the column's manager")
}

#[test]
fn a_model_data_write_holds_at_most_its_bytes() {
    if !alone("a_model_data_write_holds_at_most_its_bytes") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let root = tempfile::tempdir().expect("test root");
    let targets = SelectedVisibilityWriteTargets::new(true, false);
    let mut breaches = Vec::new();
    // A narrow and a wide cell: the queued cells against the per-row cost.
    for channels in [1, 64] {
        let path = many_rows(root.path(), "created", channels, 120.0);
        let rows = MeasurementSet::open(&path).expect("open").row_count();
        let cell = cell_shape(&path, "DATA");
        let bound =
            SelectedVisibilityWrite::retained_bytes(&path, targets).expect("the writer's bytes");
        // The first write creates MODEL_DATA, seeded with CASA's initial
        // model; the second rewrites it.
        for write in ["creating", "rewriting"] {
            let peak = peak_of(|| write_cells(&path, targets, 0..rows, cell));
            eprintln!("{write} {rows} rows of {cell:?} cells: peak {peak} bytes, bound {bound}");
            if peak > bound {
                breaches.push(format!(
                    "a {write} write of {rows} rows of {cell:?} cells held {peak} bytes, \
                     more than its {bound}"
                ));
            }
        }
        // The write reached the disk: the last row holds what was written.
        let written = stored_cell(&path, "MODEL_DATA", rows - 1);
        assert_eq!(written.shape(), &cell);
        assert_eq!(written[[0, 0]], Complex32::new((rows - 1) as f32, 0.0));
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

#[test]
fn every_accepted_destination_layout_holds_at_most_its_bytes_whatever_mains_rows() {
    if !alone("every_accepted_destination_layout_holds_at_most_its_bytes_whatever_mains_rows") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let root = tempfile::tempdir().expect("test root");
    let mut breaches = Vec::new();
    for manager in [
        DataManagerKind::TiledShapeStMan,
        DataManagerKind::TiledColumnStMan,
    ] {
        let label = format!("{manager:?}");
        // A write of one cell, with MAIN at one and twice the rows.
        let mut one_cell = Vec::new();
        for seconds in [120.0, 240.0] {
            let path = many_rows(root.path(), &label, 64, seconds);
            relayout(&path, &["MODEL_DATA", "CORRECTED_DATA"], manager);
            assert_eq!(manager_of(&path, "MODEL_DATA"), label);
            assert_eq!(manager_of(&path, "CORRECTED_DATA"), label);
            let rows = MeasurementSet::open(&path).expect("open").row_count();
            let cell = cell_shape(&path, "DATA");
            let both = SelectedVisibilityWriteTargets::new(true, true);
            let bound =
                SelectedVisibilityWrite::retained_bytes(&path, both).expect("the writer's bytes");
            let peak = peak_of(|| write_cells(&path, both, 0..1, [1, 1]));
            eprintln!("{label}: one cell of {rows} rows: peak {peak} bytes, bound {bound}");
            if peak > bound {
                breaches.push(format!(
                    "{label}: a one-cell write of {rows} rows held {peak} bytes, more than {bound}"
                ));
            }
            let column_bytes = (rows * cell[0] * cell[1] * size_of::<Complex32>()) as u64;
            one_cell.push((rows, peak, column_bytes));
            let peak = peak_of(|| write_cells(&path, both, 0..rows, cell));
            eprintln!(
                "{label}: all {rows} rows of {cell:?} cells: peak {peak} bytes, bound {bound}"
            );
            if peak > bound {
                breaches.push(format!(
                    "{label}: a write of {rows} rows of {cell:?} cells held {peak} bytes, \
                     more than its {bound}"
                ));
            }
            for column in ["MODEL_DATA", "CORRECTED_DATA"] {
                let written = stored_cell(&path, column, rows - 1);
                assert_eq!(written.shape(), &cell);
                assert_eq!(written[[1, 2]], Complex32::new((rows - 1) as f32, 5.0));
            }
        }
        // Twice MAIN's rows adds a whole destination column's worth of
        // cells; a one-cell write must hold none of them, so this law would
        // see a cell read or a save that loads the column.
        let [(rows, peak, column_bytes), (more_rows, more_peak, _)] = one_cell[..] else {
            unreachable!("two row counts");
        };
        let slack = 256 << 10;
        assert!(
            column_bytes > 16 * slack,
            "the law must be able to see a column"
        );
        if more_peak > peak + slack {
            breaches.push(format!(
                "{label}: a one-cell write held {more_peak} bytes with {more_rows} rows against \
                 {peak} with {rows}: it grows with MAIN"
            ));
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

#[test]
fn a_destination_in_any_other_manager_is_refused_before_anything_changes() {
    let root = tempfile::tempdir().expect("test root");
    let model = SelectedVisibilityWriteTargets::new(true, false);
    let corrected = SelectedVisibilityWriteTargets::new(false, true);
    for (label, destinations, manager, targets, refused) in [
        (
            "standard-model",
            &["MODEL_DATA"][..],
            DataManagerKind::StandardStMan,
            model,
            "MODEL_DATA",
        ),
        (
            "aipsio-model",
            &["MODEL_DATA"][..],
            DataManagerKind::StManAipsIO,
            model,
            "MODEL_DATA",
        ),
        (
            "standard-corrected",
            &["CORRECTED_DATA"][..],
            DataManagerKind::StandardStMan,
            corrected,
            "CORRECTED_DATA",
        ),
        // No MODEL_DATA, and DATA in StandardStMan: its clone would be too.
        (
            "standard-data",
            &[][..],
            DataManagerKind::StandardStMan,
            model,
            "MODEL_DATA",
        ),
    ] {
        let path = many_rows(root.path(), label, 1, 120.0);
        relayout(&path, destinations, manager);
        let description = std::fs::read(path.join("table.dat")).expect("MAIN's description");
        let unbounded = |result: Result<_, ObservationOwnerError>, step: &str| match result {
            Err(ObservationOwnerError::UnboundedWriteDestination { column, reason }) => {
                assert_eq!(
                    column, refused,
                    "{label}: {step} refused {column}: {reason}"
                );
                eprintln!("{label}: {step}: {reason}");
            }
            Err(other) => panic!("{label}: {step} failed otherwise: {other}"),
            Ok(_) => panic!("{label}: {step} accepted a destination it cannot bound"),
        };
        unbounded(
            SelectedVisibilityWrite::retained_bytes(&path, targets).map(|_| ()),
            "planning",
        );
        unbounded(
            SelectedVisibilityWrite::begin(&path, targets).map(|_| ()),
            "begin",
        );
        // Nothing changed and no lock is held.
        assert_eq!(
            std::fs::read(path.join("table.dat")).expect("MAIN's description"),
            description,
            "{label}: MAIN changed"
        );
        let mut main = Table::open_with_lock(
            TableOptions::new(&path),
            LockOptions::new(LockMode::AutoLocking),
        )
        .expect("reopen MAIN");
        assert!(
            main.lock(LockType::Write, 0).expect("lock MAIN"),
            "{label}: the refused write left MAIN locked"
        );
    }
}
