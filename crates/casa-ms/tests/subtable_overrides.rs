// SPDX-License-Identifier: LGPL-3.0-or-later
//! A MeasurementSet save writes a subtable from column overrides
//! ([`MeasurementSet::save_with_column_overrides`]).
//!
//! A POINTING given as generated cells, one row per antenna per
//! integration, is written as the same files as the same cells held as
//! rows, and its save holds no more for ten times the rows: the
//! StandardStMan writer holds one bucket slice and the generators their
//! per-scan state.
//!
//! This binary's global allocator counts the live heap; the counters are
//! the process's, so the law runs alone in a process of its own
//! ([`alone`]).

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_ms::schema::SubtableId;
use casa_ms::{MeasurementSet, MeasurementSetBuilder, MsError, SubtableColumnOverrides};
use casa_tables::{
    ColumnOverrides, GeneratedArrayColumn, GeneratedArrayValueRun, GeneratedScalarColumn,
    GeneratedScalarValueRun, Table, TableOptions,
};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

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
/// law alone and assert that it passed (see `creation_write_memory.rs`,
/// whose helper this is).
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

/// A simulator's POINTING: `scans` scans of `integrations` integrations,
/// one row per antenna per integration, each scan tracking its own
/// direction.
struct Pointing {
    antennas: usize,
    integrations: usize,
    scans: usize,
}

const INTEGRATION_S: f64 = 10.0;
const START_S: f64 = 4.0e9;

fn direction(scan: usize) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(
            vec![2, 1],
            vec![0.1 * scan as f64, -0.5 + 0.01 * scan as f64],
        )
        .expect("2x1"),
    )
}

impl Pointing {
    fn rows(&self) -> usize {
        self.antennas * self.integrations * self.scans
    }

    fn scan_rows(&self) -> usize {
        self.antennas * self.integrations
    }

    fn scan_start_s(&self, scan: usize) -> f64 {
        START_S + (scan * self.integrations) as f64 * INTEGRATION_S
    }

    fn time_s(&self, row: usize) -> f64 {
        let integration = row / self.antennas;
        START_S + (integration as f64 + 0.5) * INTEGRATION_S
    }

    /// Row `row` as a record.
    fn record(&self, row: usize) -> RecordValue {
        let scan = row / self.scan_rows();
        let scalar = |value| Value::Scalar(value);
        RecordValue::new(vec![
            RecordField::new(
                "ANTENNA_ID",
                scalar(ScalarValue::Int32((row % self.antennas) as i32)),
            ),
            RecordField::new("DIRECTION", Value::Array(direction(scan))),
            RecordField::new("INTERVAL", scalar(ScalarValue::Float64(INTEGRATION_S))),
            RecordField::new("NAME", scalar(ScalarValue::String(String::new()))),
            RecordField::new("NUM_POLY", scalar(ScalarValue::Int32(0))),
            RecordField::new("TARGET", Value::Array(direction(scan))),
            RecordField::new("TIME", scalar(ScalarValue::Float64(self.time_s(row)))),
            RecordField::new(
                "TIME_ORIGIN",
                scalar(ScalarValue::Float64(self.scan_start_s(scan))),
            ),
            RecordField::new("TRACKING", scalar(ScalarValue::Bool(true))),
        ])
    }

    /// The same cells as generators: ANTENNA_ID and TIME per row, the rest
    /// as per-scan runs or constants.
    fn overrides(&self) -> ColumnOverrides {
        let rows = self.rows();
        let antennas = self.antennas;
        let starts = (0..self.scans).map(|scan| scan * self.scan_rows());
        let mut overrides = ColumnOverrides::for_row_count(rows);
        overrides.insert_generated_scalar(
            "ANTENNA_ID",
            GeneratedScalarColumn::new(rows, move |row| {
                Some(ScalarValue::Int32((row % antennas) as i32))
            }),
        );
        let time = Pointing {
            antennas,
            integrations: self.integrations,
            scans: self.scans,
        };
        overrides.insert_generated_scalar(
            "TIME",
            GeneratedScalarColumn::new(rows, move |row| {
                Some(ScalarValue::Float64(time.time_s(row)))
            }),
        );
        let directions = || {
            GeneratedArrayColumn::from_array_runs(
                rows,
                starts
                    .clone()
                    .enumerate()
                    .map(|(scan, start)| GeneratedArrayValueRun::new(start, Some(direction(scan))))
                    .collect(),
            )
            .expect("direction runs")
        };
        overrides.insert_generated_array("DIRECTION", directions());
        overrides.insert_generated_array("TARGET", directions());
        let origins = starts
            .clone()
            .enumerate()
            .map(|(scan, start)| {
                GeneratedScalarValueRun::new(
                    start,
                    Some(ScalarValue::Float64(self.scan_start_s(scan))),
                )
            })
            .collect();
        overrides.insert_generated_scalar(
            "TIME_ORIGIN",
            GeneratedScalarColumn::from_scalar_runs(rows, origins).expect("origin runs"),
        );
        for (column, value) in [
            ("INTERVAL", ScalarValue::Float64(INTEGRATION_S)),
            ("NAME", ScalarValue::String(String::new())),
            ("NUM_POLY", ScalarValue::Int32(0)),
            ("TRACKING", ScalarValue::Bool(true)),
        ] {
            overrides.insert_generated_scalar(
                column,
                GeneratedScalarColumn::constant(rows, Some(value)),
            );
        }
        overrides
    }

    fn subtable_overrides(&self) -> SubtableColumnOverrides {
        let mut subtables = SubtableColumnOverrides::new();
        subtables.insert(SubtableId::Pointing, self.overrides());
        subtables
    }
}

/// Every file of a table, by name.
fn files(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = std::fs::read_dir(path)
        .expect("table directory")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.is_file())
        .map(|file| {
            let name = file.file_name().unwrap().to_string_lossy().into_owned();
            (name, std::fs::read(&file).expect("read file"))
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn a_generated_pointing_writes_the_files_of_its_rows() {
    let root = tempfile::tempdir().expect("tempdir");
    let pointing = Pointing {
        antennas: 5,
        integrations: 230,
        scans: 3,
    };
    let from_rows = root.path().join("rows.ms");
    let mut ms = MeasurementSet::create(&from_rows, MeasurementSetBuilder::new()).expect("ms");
    let table = ms.subtable_mut(SubtableId::Pointing).expect("POINTING");
    for row in 0..pointing.rows() {
        table.add_row(pointing.record(row)).expect("row");
    }
    ms.save().expect("save rows");

    let generated = root.path().join("generated.ms");
    let mut ms = MeasurementSet::create(&generated, MeasurementSetBuilder::new()).expect("ms");
    ms.save_with_column_overrides(&ColumnOverrides::new(), &pointing.subtable_overrides())
        .expect("save generated");
    assert_eq!(
        ms.subtable(SubtableId::Pointing)
            .expect("POINTING")
            .row_count(),
        0,
        "the in-memory subtable is unchanged"
    );
    drop(ms);

    let expected = files(&from_rows.join("POINTING"));
    assert!(expected.iter().any(|(name, _)| name == "table.f0i"));
    assert!(
        files(&generated.join("POINTING")) == expected,
        "POINTING files differ"
    );
    let reopened = Table::open(TableOptions::new(generated.join("POINTING"))).expect("reopen");
    assert_eq!(reopened.row_count(), pointing.rows());
    let reopened_ms = MeasurementSet::open(&generated).expect("reopen MS");
    assert_eq!(
        reopened_ms
            .subtable(SubtableId::Pointing)
            .expect("POINTING")
            .row_count(),
        pointing.rows()
    );
}

#[test]
fn overrides_of_a_missing_subtable_are_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut ms =
        MeasurementSet::create(root.path().join("a.ms"), MeasurementSetBuilder::new()).expect("ms");
    let mut subtables = SubtableColumnOverrides::new();
    subtables.insert(SubtableId::Source, ColumnOverrides::for_row_count(0));
    assert!(matches!(
        ms.save_with_column_overrides(&ColumnOverrides::new(), &subtables),
        Err(MsError::MissingSubtable(name)) if name == "SOURCE"
    ));
    assert!(!root.path().join("a.ms").join("SOURCE").exists());
}

/// Unrelated allocations of a save: file handles, table metadata and the
/// writer's bucket slice, string bucket and array-file buffer.
const SLACK_BYTES: u64 = 64 * 1024;

/// The heap a MeasurementSet save of `pointing` as generated cells held
/// beyond what was live before the overrides were built.
fn save_peak(root: &Path, pointing: &Pointing) -> u64 {
    let path = root.join(format!("pointing-{}.ms", pointing.rows()));
    let mut ms = MeasurementSet::create(&path, MeasurementSetBuilder::new()).expect("ms");
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let subtables = pointing.subtable_overrides();
    ms.save_with_column_overrides(&ColumnOverrides::new(), &subtables)
        .expect("save");
    drop(subtables);
    let peak = PEAK.load(Ordering::Relaxed) - baseline;
    drop(ms);
    let reopened = Table::open(TableOptions::new(path.join("POINTING"))).expect("reopen");
    assert_eq!(reopened.row_count(), pointing.rows());
    peak
}

#[test]
fn a_generated_pointing_save_holds_no_more_for_more_rows() {
    if !alone("a_generated_pointing_save_holds_no_more_for_more_rows") {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let small = Pointing {
        antennas: 12,
        integrations: 250,
        scans: 4,
    };
    let large = Pointing {
        integrations: 2_500,
        ..small
    };
    let small_peak = save_peak(root.path(), &small);
    let large_peak = save_peak(root.path(), &large);
    println!(
        "{} rows: save peak {small_peak} bytes; {} rows: save peak {large_peak} bytes",
        small.rows(),
        large.rows()
    );
    assert!(
        large_peak <= small_peak + SLACK_BYTES,
        "the save grew from {small_peak} to {large_peak} bytes"
    );
}
